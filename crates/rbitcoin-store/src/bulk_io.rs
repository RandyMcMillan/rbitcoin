//! Bulk table reads via **io_uring** (Linux) so the kernel can schedule many
//! independent preads at once.
//!
//! Used by archive head-resolve (`probe → idx → body_txid`) and confirm load
//! body batches. Completions are unordered within a submit batch; callers apply
//! a depth-aware / id-aligned state machine after each wave.
//!
//! # Controls
//!
//! - `RBITCOIN_IO_URING=0` — force libc `pread` fallback (serial or parallel).
//! - `RBITCOIN_BULK_IO_WORKERS` — parallel pread workers when uring is off
//!   (default `min(CPUs, 16)`; `1` = serial).
//!
//! Ring entries: fixed **1024**. Large waves keep the ring full: submit up to
//! 1024 outstanding preads, then refill as CQEs complete (pipelined, not
//! stop-and-wait chunks). Each `pread_batch` drains leftover CQEs first so
//! `user_data` never collides across calls on the thread-local ring.

use std::cell::RefCell;
use std::os::fd::RawFd;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicUsize, Ordering};

/// Thread-local io_uring SQ/CQ depth (and max outstanding preads per batch).
const RING_ENTRIES: u32 = 1024;

/// One independent pread. Caller owns `buf` for the full submit/wait.
pub struct ReadOp<'a> {
    pub fd: RawFd,
    pub offset: u64,
    pub buf: &'a mut [u8],
    /// Filled: bytes read (≥0) or negated errno on failure.
    pub result: i32,
}

static URING_MODE: AtomicU8 = AtomicU8::new(0); // 0 unknown, 1 on, 2 off
static WORKERS: AtomicUsize = AtomicUsize::new(0);
static URING_FAIL_LOGGED: AtomicBool = AtomicBool::new(false);

/// Whether io_uring bulk reads are enabled (env + successful ring setup).
pub fn io_uring_enabled() -> bool {
    match URING_MODE.load(Ordering::Relaxed) {
        1 => true,
        2 => false,
        _ => {
            let want = std::env::var("RBITCOIN_IO_URING")
                .map(|s| s != "0" && s != "false" && s != "off")
                .unwrap_or(true);
            if !want {
                URING_MODE.store(2, Ordering::Relaxed);
                return false;
            }
            let ok = probe_uring();
            URING_MODE.store(if ok { 1 } else { 2 }, Ordering::Relaxed);
            if !ok && !URING_FAIL_LOGGED.swap(true, Ordering::Relaxed) {
                rbitcoin_log::warn!(
                    "store: io_uring unavailable — bulk reads use pread fallback \
                     (set RBITCOIN_IO_URING=0 to silence)"
                );
            }
            ok
        }
    }
}

fn probe_uring() -> bool {
    #[cfg(target_os = "linux")]
    {
        io_uring::IoUring::new(32).is_ok()
    }
    #[cfg(not(target_os = "linux"))]
    {
        false
    }
}

/// Worker count for pread fallback (cached).
pub fn bulk_io_workers() -> usize {
    let cached = WORKERS.load(Ordering::Relaxed);
    if cached > 0 {
        return cached;
    }
    let n = std::env::var("RBITCOIN_BULK_IO_WORKERS")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .filter(|&n| n > 0)
        .unwrap_or_else(|| {
            std::thread::available_parallelism()
                .map(|p| p.get())
                .unwrap_or(4)
                .clamp(1, 16)
        })
        .max(1);
    WORKERS.store(n, Ordering::Relaxed);
    n
}

/// Submit all ops; fill [`ReadOp::result`]. Prefers io_uring; else parallel pread.
pub fn pread_batch(ops: &mut [ReadOp<'_>]) {
    if ops.is_empty() {
        return;
    }
    if io_uring_enabled() && pread_batch_uring(ops) {
        return;
    }
    pread_batch_fallback(ops);
}

#[cfg(target_os = "linux")]
fn with_ring<R>(f: impl FnOnce(&mut io_uring::IoUring) -> R) -> Option<R> {
    use io_uring::IoUring;
    thread_local! {
        static RING: RefCell<Option<IoUring>> = const { RefCell::new(None) };
    }
    RING.with(|cell| {
        let mut slot = cell.borrow_mut();
        if slot.is_none() {
            match IoUring::new(RING_ENTRIES) {
                Ok(r) => *slot = Some(r),
                Err(_) => {
                    URING_MODE.store(2, Ordering::Relaxed);
                    return None;
                }
            }
        }
        let ring = slot.as_mut().expect("ring installed");
        Some(f(ring))
    })
}

/// Pipelined bulk pread: keep up to [`RING_ENTRIES`] ops in flight, refill as
/// CQEs free slots. Absolute `user_data = op index` for the whole batch.
#[cfg(target_os = "linux")]
fn pread_batch_uring(ops: &mut [ReadOp<'_>]) -> bool {
    use io_uring::{opcode, types};
    const MAX_IN_FLIGHT: usize = RING_ENTRIES as usize;
    with_ring(|ring| {
        drain_cq_all(ring);
        for op in ops.iter_mut() {
            op.result = if op.buf.is_empty() { 0 } else { i32::MIN };
        }
        let n = ops.len();
        let total_nonempty = ops.iter().filter(|o| !o.buf.is_empty()).count();
        if total_nonempty == 0 {
            return true;
        }

        let mut next = 0usize;
        let mut in_flight = 0usize;
        let mut completed = 0usize;

        while completed < total_nonempty {
            // 1) Fill free SQ slots up to ring depth.
            while next < n && in_flight < MAX_IN_FLIGHT {
                if ops[next].buf.is_empty() {
                    next += 1;
                    continue;
                }
                let sqe = opcode::Read::new(
                    types::Fd(ops[next].fd),
                    ops[next].buf.as_mut_ptr(),
                    ops[next].buf.len() as u32,
                )
                .offset(ops[next].offset)
                .build()
                .user_data(next as u64);
                // SAFETY: caller owns each `buf` until `pread_batch` returns;
                // we do not return while any op is still in flight.
                if unsafe { ring.submission().push(&sqe) }.is_err() {
                    // SQ full of unsubmitted entries — submit/wait below.
                    if in_flight == 0 {
                        drain_cq_all(ring);
                        return false;
                    }
                    break;
                }
                next += 1;
                in_flight += 1;
            }
            ring.submission().sync();

            if in_flight == 0 {
                // Only empty ops left (or nothing pushed).
                break;
            }

            // 2) Harvest ready CQEs without blocking; if none, submit pending
            //    SQEs and wait for ≥1 completion so we can refill.
            if harvest_ready(ring, ops, &mut in_flight, &mut completed) == 0 {
                if ring.submit_and_wait(1).is_err() {
                    drain_cq_all(ring);
                    return false;
                }
                if harvest_ready(ring, ops, &mut in_flight, &mut completed) == 0 {
                    drain_cq_all(ring);
                    return false;
                }
            } else if ring.submit().is_err() {
                // Had ready CQEs; still kick any unsubmitted SQEs before refill.
                drain_cq_all(ring);
                return false;
            }
        }

        for op in ops.iter_mut() {
            if !op.buf.is_empty() && op.result == i32::MIN {
                op.result = -5; // EIO
            }
        }
        drain_cq_all(ring);
        true
    })
    .unwrap_or(false)
}

/// Harvest all currently available CQEs (non-blocking). Returns count taken.
#[cfg(target_os = "linux")]
fn harvest_ready(
    ring: &mut io_uring::IoUring,
    ops: &mut [ReadOp<'_>],
    in_flight: &mut usize,
    completed: &mut usize,
) -> usize {
    ring.completion().sync();
    let mut got = 0usize;
    for cqe in ring.completion() {
        let i = cqe.user_data() as usize;
        if i < ops.len() {
            ops[i].result = cqe.result();
        }
        if *in_flight > 0 {
            *in_flight -= 1;
        }
        *completed += 1;
        got += 1;
    }
    if got > 0 {
        ring.completion().sync();
    }
    got
}

#[cfg(target_os = "linux")]
fn drain_cq_all(ring: &mut io_uring::IoUring) {
    ring.completion().sync();
    for _ in ring.completion() {}
    ring.completion().sync();
}

#[cfg(not(target_os = "linux"))]
fn pread_batch_uring(_ops: &mut [ReadOp<'_>]) -> bool {
    false
}

fn pread_batch_fallback(ops: &mut [ReadOp<'_>]) {
    for op in ops.iter_mut() {
        op.result = i32::MIN;
    }
    let n = ops.len();
    let workers = bulk_io_workers();
    if n == 1 || workers <= 1 || n < 8 {
        for op in ops.iter_mut() {
            pread_one(op);
        }
        return;
    }
    let threads = workers.min(n);
    let chunk = n.div_ceil(threads);
    std::thread::scope(|scope| {
        for piece in ops.chunks_mut(chunk) {
            scope.spawn(|| {
                for op in piece.iter_mut() {
                    pread_one(op);
                }
            });
        }
    });
}

fn pread_one(op: &mut ReadOp<'_>) {
    if op.buf.is_empty() {
        op.result = 0;
        return;
    }
    let mut got = 0usize;
    while got < op.buf.len() {
        let n = unsafe {
            libc::pread(
                op.fd,
                op.buf[got..].as_mut_ptr() as *mut libc::c_void,
                op.buf.len() - got,
                (op.offset + got as u64) as i64,
            )
        };
        if n < 0 {
            op.result = -std::io::Error::last_os_error()
                .raw_os_error()
                .unwrap_or(5) as i32;
            return;
        }
        if n == 0 {
            op.result = got as i32;
            return;
        }
        got += n as usize;
    }
    op.result = got as i32;
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use std::os::fd::AsRawFd;

    #[test]
    fn pread_batch_roundtrip_tmpfile() {
        let dir = std::env::temp_dir().join(format!(
            "rbitcoin-uring-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let _ = std::fs::create_dir_all(&dir);
        let path = dir.join("blob");
        let mut f = std::fs::File::create(&path).unwrap();
        let data: Vec<u8> = (0u8..200).collect();
        f.write_all(&data).unwrap();
        f.flush().unwrap();
        let f = std::fs::File::open(&path).unwrap();
        let fd = f.as_raw_fd();

        let mut b0 = [0u8; 50];
        let mut b1 = [0u8; 50];
        let mut b2 = [0u8; 50];
        let mut ops = [
            ReadOp {
                fd,
                offset: 0,
                buf: &mut b0[..],
                result: i32::MIN,
            },
            ReadOp {
                fd,
                offset: 50,
                buf: &mut b1[..],
                result: i32::MIN,
            },
            ReadOp {
                fd,
                offset: 100,
                buf: &mut b2[..],
                result: i32::MIN,
            },
        ];
        pread_batch(&mut ops);
        for op in &ops {
            assert!(op.result >= 50, "result={}", op.result);
        }
        drop(ops);
        assert_eq!(&b0[..], &data[0..50]);
        assert_eq!(&b1[..], &data[50..100]);
        assert_eq!(&b2[..], &data[100..150]);

        // Many small reads (stress completion mapping).
        let mut bufs: Vec<[u8; 1]> = (0..120).map(|_| [0u8; 1]).collect();
        let mut ops: Vec<ReadOp<'_>> = Vec::new();
        // Build via raw pointers after collecting mut refs carefully.
        let mut slices: Vec<&mut [u8]> = bufs.iter_mut().map(|b| &mut b[..]).collect();
        for (i, sl) in slices.iter_mut().enumerate() {
            ops.push(ReadOp {
                fd,
                offset: i as u64,
                buf: *sl,
                result: i32::MIN,
            });
        }
        pread_batch(&mut ops);
        for (i, op) in ops.iter().enumerate() {
            assert_eq!(op.result, 1, "i={i}");
        }
        drop(ops);
        for (i, b) in bufs.iter().enumerate() {
            assert_eq!(b[0], data[i], "i={i}");
        }

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn pread_batch_fallback_matches() {
        let dir = std::env::temp_dir().join(format!(
            "rbitcoin-pread-fb-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let _ = std::fs::create_dir_all(&dir);
        let path = dir.join("blob");
        let mut f = std::fs::File::create(&path).unwrap();
        f.write_all(b"hello-fallback-path!!").unwrap();
        f.flush().unwrap();
        let f = std::fs::File::open(&path).unwrap();
        let fd = f.as_raw_fd();
        let mut b = [0u8; 5];
        let mut ops = [ReadOp {
            fd,
            offset: 0,
            buf: &mut b[..],
            result: i32::MIN,
        }];
        pread_batch_fallback(&mut ops);
        drop(ops);
        assert_eq!(&b, b"hello");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Forces multi-fill of the 1024-deep ring (N > RING_ENTRIES) and checks
    /// every byte — exercises pipelined refill, not just a single wave.
    #[test]
    fn pread_batch_pipeline_over_ring_depth() {
        const N: usize = 1500;
        assert!(N > RING_ENTRIES as usize);

        let dir = std::env::temp_dir().join(format!(
            "rbitcoin-uring-pipe-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let _ = std::fs::create_dir_all(&dir);
        let path = dir.join("blob");
        let data: Vec<u8> = (0..N).map(|i| (i % 251) as u8).collect();
        {
            let mut f = std::fs::File::create(&path).unwrap();
            f.write_all(&data).unwrap();
            f.flush().unwrap();
        }
        let f = std::fs::File::open(&path).unwrap();
        let fd = f.as_raw_fd();

        let mut bufs: Vec<[u8; 1]> = (0..N).map(|_| [0u8; 1]).collect();
        let mut slices: Vec<&mut [u8]> = bufs.iter_mut().map(|b| &mut b[..]).collect();
        let mut ops: Vec<ReadOp<'_>> = Vec::with_capacity(N);
        for (i, sl) in slices.iter_mut().enumerate() {
            ops.push(ReadOp {
                fd,
                offset: i as u64,
                buf: *sl,
                result: i32::MIN,
            });
        }
        pread_batch(&mut ops);
        for (i, op) in ops.iter().enumerate() {
            assert_eq!(op.result, 1, "i={i} result={}", op.result);
        }
        drop(ops);
        for (i, b) in bufs.iter().enumerate() {
            assert_eq!(b[0], data[i], "i={i}");
        }

        // Empty buffer interleaved — must not be submitted; index mapping intact.
        let mut b0 = [0u8; 1];
        let mut empty: [u8; 0] = [];
        let mut b1 = [0u8; 1];
        let mut ops2 = [
            ReadOp {
                fd,
                offset: 0,
                buf: &mut b0[..],
                result: i32::MIN,
            },
            ReadOp {
                fd,
                offset: 0,
                buf: &mut empty[..],
                result: i32::MIN,
            },
            ReadOp {
                fd,
                offset: 1,
                buf: &mut b1[..],
                result: i32::MIN,
            },
        ];
        pread_batch(&mut ops2);
        assert_eq!(ops2[0].result, 1);
        assert_eq!(ops2[1].result, 0);
        assert_eq!(ops2[2].result, 1);
        drop(ops2);
        assert_eq!(b0[0], data[0]);
        assert_eq!(b1[0], data[1]);

        let _ = std::fs::remove_dir_all(&dir);
    }
}
