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
//! Ring entries: fixed 1024; large waves are submitted in chunks. Each
//! `pread_batch` drains leftover CQEs first so user_data never collides across
//! calls on the thread-local ring.

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

/// One independent pwrite. Caller owns `buf` for the full submit/wait.
pub struct WriteOp<'a> {
    pub fd: RawFd,
    pub offset: u64,
    pub buf: &'a [u8],
    /// Filled: bytes written (≥0) or negated errno on failure.
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

/// Submit all ops; fill [`WriteOp::result`]. Prefers io_uring; else serial pwrite.
pub fn pwrite_batch(ops: &mut [WriteOp<'_>]) {
    if ops.is_empty() {
        return;
    }
    if io_uring_enabled() && pwrite_batch_uring(ops) {
        return;
    }
    pwrite_batch_fallback(ops);
}

#[cfg(target_os = "linux")]
fn with_ring<R>(f: impl FnOnce(&mut io_uring::IoUring) -> R) -> Option<R> {
    use io_uring::IoUring;
    thread_local! {
        static RING: RefCell<Option<IoUring>> = const { RefCell::new(None) };
    }
    const ENTRIES: u32 = 1024;
    RING.with(|cell| {
        let mut slot = cell.borrow_mut();
        if slot.is_none() {
            match IoUring::new(ENTRIES) {
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

#[cfg(target_os = "linux")]
fn pread_batch_uring(ops: &mut [ReadOp<'_>]) -> bool {
    use io_uring::{opcode, types};
    const CHUNK: usize = 256;
    with_ring(|ring| {
        drain_cq_all(ring);
        for op in ops.iter_mut() {
            op.result = if op.buf.is_empty() { 0 } else { i32::MIN };
        }
        let n = ops.len();
        let mut base = 0usize;
        while base < n {
            let end = (base + CHUNK).min(n);
            let mut pending = 0usize;
            for i in base..end {
                if !ops[i].buf.is_empty() {
                    pending += 1;
                }
            }
            if pending == 0 {
                base = end;
                continue;
            }
            for i in base..end {
                if ops[i].buf.is_empty() {
                    continue;
                }
                let sqe = opcode::Read::new(
                    types::Fd(ops[i].fd),
                    ops[i].buf.as_mut_ptr(),
                    ops[i].buf.len() as u32,
                )
                .offset(ops[i].offset)
                .build()
                .user_data(i as u64);
                // SAFETY: buf lives until chunk CQEs drained.
                if unsafe { ring.submission().push(&sqe) }.is_err() {
                    drain_cq_all(ring);
                    return false;
                }
            }
            ring.submission().sync();
            if !submit_wait_drain(ring, pending, n, &mut |i, res| {
                if i < n {
                    ops[i].result = res;
                }
            }) {
                return false;
            }
            base = end;
        }
        for op in ops.iter_mut() {
            if !op.buf.is_empty() && op.result == i32::MIN {
                op.result = -5;
            }
        }
        drain_cq_all(ring);
        true
    })
    .unwrap_or(false)
}

#[cfg(target_os = "linux")]
fn pwrite_batch_uring(ops: &mut [WriteOp<'_>]) -> bool {
    use io_uring::{opcode, types};
    const CHUNK: usize = 256;
    with_ring(|ring| {
        drain_cq_all(ring);
        for op in ops.iter_mut() {
            op.result = if op.buf.is_empty() { 0 } else { i32::MIN };
        }
        let n = ops.len();
        let mut base = 0usize;
        while base < n {
            let end = (base + CHUNK).min(n);
            let mut pending = 0usize;
            for i in base..end {
                if !ops[i].buf.is_empty() {
                    pending += 1;
                }
            }
            if pending == 0 {
                base = end;
                continue;
            }
            for i in base..end {
                if ops[i].buf.is_empty() {
                    continue;
                }
                let sqe = opcode::Write::new(
                    types::Fd(ops[i].fd),
                    ops[i].buf.as_ptr(),
                    ops[i].buf.len() as u32,
                )
                .offset(ops[i].offset)
                .build()
                .user_data(i as u64);
                // SAFETY: buf lives until chunk CQEs drained.
                if unsafe { ring.submission().push(&sqe) }.is_err() {
                    drain_cq_all(ring);
                    return false;
                }
            }
            ring.submission().sync();
            if !submit_wait_drain(ring, pending, n, &mut |i, res| {
                if i < n {
                    ops[i].result = res;
                }
            }) {
                return false;
            }
            base = end;
        }
        for op in ops.iter_mut() {
            if !op.buf.is_empty() && op.result == i32::MIN {
                op.result = -5;
            }
        }
        drain_cq_all(ring);
        true
    })
    .unwrap_or(false)
}

#[cfg(target_os = "linux")]
fn submit_wait_drain(
    ring: &mut io_uring::IoUring,
    pending: usize,
    n_ops: usize,
    on_cqe: &mut dyn FnMut(usize, i32),
) -> bool {
    if ring.submit_and_wait(pending).is_err() {
        drain_cq_all(ring);
        return false;
    }
    let mut got = 0usize;
    while got < pending {
        let mut progressed = false;
        for cqe in ring.completion() {
            progressed = true;
            let i = cqe.user_data() as usize;
            if i < n_ops {
                on_cqe(i, cqe.result());
            }
            got += 1;
            if got >= pending {
                break;
            }
        }
        if !progressed && ring.submit_and_wait(1).is_err() {
            drain_cq_all(ring);
            return false;
        }
    }
    ring.completion().sync();
    true
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

#[cfg(not(target_os = "linux"))]
fn pwrite_batch_uring(_ops: &mut [WriteOp<'_>]) -> bool {
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

fn pwrite_batch_fallback(ops: &mut [WriteOp<'_>]) {
    for op in ops.iter_mut() {
        pwrite_one(op);
    }
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

fn pwrite_one(op: &mut WriteOp<'_>) {
    if op.buf.is_empty() {
        op.result = 0;
        return;
    }
    let mut got = 0usize;
    while got < op.buf.len() {
        let n = unsafe {
            libc::pwrite(
                op.fd,
                op.buf[got..].as_ptr() as *const libc::c_void,
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
    fn pwrite_batch_roundtrip() {
        let dir = std::env::temp_dir().join(format!(
            "rbitcoin-pwrite-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let _ = std::fs::create_dir_all(&dir);
        let path = dir.join("blob");
        {
            let f = std::fs::File::create(&path).unwrap();
            f.set_len(64).unwrap();
        }
        let f = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .unwrap();
        let fd = f.as_raw_fd();
        let a = [0x11u8, 0x22, 0x33, 0x44];
        let b = [0x55u8, 0x66, 0x77, 0x88];
        let mut ops = [
            WriteOp {
                fd,
                offset: 0,
                buf: &a,
                result: i32::MIN,
            },
            WriteOp {
                fd,
                offset: 8,
                buf: &b,
                result: i32::MIN,
            },
        ];
        pwrite_batch(&mut ops);
        assert!(ops[0].result >= 4);
        assert!(ops[1].result >= 4);
        drop(ops);
        let mut r0 = [0u8; 4];
        let mut r1 = [0u8; 4];
        let mut rops = [
            ReadOp {
                fd,
                offset: 0,
                buf: &mut r0,
                result: i32::MIN,
            },
            ReadOp {
                fd,
                offset: 8,
                buf: &mut r1,
                result: i32::MIN,
            },
        ];
        pread_batch(&mut rops);
        drop(rops);
        assert_eq!(r0, a);
        assert_eq!(r1, b);
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
}
