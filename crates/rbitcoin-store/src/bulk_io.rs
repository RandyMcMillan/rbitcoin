//! Concurrent bulk table reads so the kernel can schedule many faults/IOs.
//!
//! Hot paths (archive head-resolve, confirm body load) collect independent
//! reads, then run them across a small worker set. Completions are applied on
//! the calling thread in any order within a wave; dependent stages (probe →
//! idx → body) are re-queued for the next wave.
//!
//! Reads still use the mmap publish path ([`crate::file::TableFile::read_at`]);
//! concurrency is what exposes queue depth to the page-cache / block layer.
//!
//! Workers: `RBITCOIN_BULK_IO_WORKERS` (default = min(available_parallelism, 16),
//! floor 1). Set to `1` to force serial (debug / A-B).

use std::sync::atomic::{AtomicUsize, Ordering};

static WORKERS: AtomicUsize = AtomicUsize::new(0);

/// Resolve worker count (cached after first call; `0` in env → auto).
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
        });
    let n = n.max(1);
    WORKERS.store(n, Ordering::Relaxed);
    n
}

/// Run `f` over every item, using up to [`bulk_io_workers`] threads when the
/// batch is large enough to amortize spawn cost.
///
/// `f` must be safe to call concurrently on distinct elements.
pub fn for_each_mut<T, F>(items: &mut [T], f: F)
where
    T: Send,
    F: Fn(&mut T) + Sync,
{
    let n = items.len();
    if n == 0 {
        return;
    }
    let workers = bulk_io_workers();
    // Small waves: serial avoids thread spawn tax on tiny sticky-miss tails.
    if n == 1 || workers <= 1 || n < 8 {
        for item in items.iter_mut() {
            f(item);
        }
        return;
    }
    let threads = workers.min(n);
    let chunk = n.div_ceil(threads);
    std::thread::scope(|scope| {
        for piece in items.chunks_mut(chunk) {
            scope.spawn(|| {
                for item in piece.iter_mut() {
                    f(item);
                }
            });
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicU64;

    #[test]
    fn for_each_mut_covers_all() {
        let mut v: Vec<u64> = (0..100).collect();
        for_each_mut(&mut v, |x| *x += 1);
        assert_eq!(v, (1..101).collect::<Vec<_>>());
    }

    #[test]
    fn parallel_increments_are_racy_free_on_disjoint() {
        static HITS: AtomicU64 = AtomicU64::new(0);
        let mut slots = vec![0u64; 64];
        for_each_mut(&mut slots, |s| {
            *s = 1;
            HITS.fetch_add(1, Ordering::Relaxed);
        });
        assert_eq!(HITS.swap(0, Ordering::Relaxed), 64);
        assert!(slots.iter().all(|&x| x == 1));
    }
}
