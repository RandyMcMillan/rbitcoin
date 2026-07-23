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
//! Ring entries: fixed 1024; large waves are submitted in chunks.

use std::cell::RefCell;
use std::os::fd::RawFd;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicUsize, Ordering};

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
            // Probe: can we build a ring?
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
fn pread_batch_uring(ops: &mut [ReadOp<'_>]) -> bool {
    use io_uring::{opcode, types, IoUring};

    thread_local! {
        static RING: RefCell<Option<IoUring>> = const { RefCell::new(None) };
    }

    const ENTRIES: u32 = 1024;
    const CHUNK: usize = 512;

    RING.with(|cell| {
        let mut slot = cell.borrow_mut();
        if slot.is_none() {
            match IoUring::new(ENTRIES) {
                Ok(r) => *slot = Some(r),
                Err(_) => {
                    URING_MODE.store(2, Ordering::Relaxed);
                    return false;
                }
            }
        }
        let ring = slot.as_mut().expect("ring installed");

        for chunk in ops.chunks_mut(CHUNK) {
            for op in chunk.iter_mut() {
                if op.buf.is_empty() {
                    op.result = 0;
                } else {
                    op.result = i32::MIN;
                }
            }
            // Push SQEs (retry push after submit if full).
            for (i, op) in chunk.iter_mut().enumerate() {
                if op.buf.is_empty() {
                    continue;
                }
                let sqe = opcode::Read::new(
                    types::Fd(op.fd),
                    op.buf.as_mut_ptr(),
                    op.buf.len() as u32,
                )
                .offset(op.offset)
                .build()
                .user_data(i as u64);
                // SAFETY: op.buf lives until we drain completions for this chunk.
                loop {
                    let push = unsafe { ring.submission().push(&sqe) };
                    match push {
                        Ok(()) => break,
                        Err(_) => {
                            if ring.submit().is_err() {
                                return false;
                            }
                        }
                    }
                }
            }
            let want = chunk.iter().filter(|o| !o.buf.is_empty()).count();
            if want == 0 {
                continue;
            }
            if ring.submit_and_wait(want).is_err() {
                return false;
            }
            let mut got = 0usize;
            for cqe in ring.completion() {
                let i = cqe.user_data() as usize;
                if i < chunk.len() {
                    chunk[i].result = cqe.result();
                }
                got += 1;
                if got >= want {
                    break;
                }
            }
            // Non-empty without CQE → treat as EIO.
            for op in chunk.iter_mut() {
                if !op.buf.is_empty() && op.result == i32::MIN {
                    op.result = -5; // EIO
                }
            }
        }
        true
    })
}

#[cfg(not(target_os = "linux"))]
fn pread_batch_uring(_ops: &mut [ReadOp<'_>]) -> bool {
    false
}

fn pread_batch_fallback(ops: &mut [ReadOp<'_>]) {
    // Init results.
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
        // Force both paths if possible.
        pread_batch(&mut ops);
        for op in &ops {
            assert!(op.result >= 50, "result={}", op.result);
        }
        drop(ops);
        assert_eq!(&b0[..], &data[0..50]);
        assert_eq!(&b1[..], &data[50..100]);
        assert_eq!(&b2[..], &data[100..150]);

        // Explicit fallback path.
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
        pread_batch_fallback(&mut ops);
        drop(ops);
        assert_eq!(&b0[..], &data[0..50]);
        assert_eq!(&b1[..], &data[50..100]);
        assert_eq!(&b2[..], &data[100..150]);

        let _ = std::fs::remove_dir_all(&dir);
    }
}
