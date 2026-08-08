//! Lightweight parallel script-check pool (replaces rayon on the hot path).
//!
//! Production only needs: (1) parallel `try_for_each` over script jobs, and
//! (2) fire-and-forget spawn for async scripts phase. A small work-stealing
//! loop over `std::thread::scope` avoids pulling rayon + crossbeam into the
//! consensus crate graph.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Mutex;
use std::thread;

use crate::error::ConsensusError;

/// Parallel map over `items` until the first error (or all succeed).
///
/// Uses one OS thread per logical CPU (capped by `items.len()`), each claiming
/// the next index with a shared atomic. On first error, workers stop claiming
/// new work; in-flight jobs may still finish.
pub(crate) fn try_for_each_parallel<T, F>(items: &[T], f: F) -> Result<(), ConsensusError>
where
    T: Sync,
    F: Fn(&T) -> Result<(), ConsensusError> + Sync,
{
    if items.is_empty() {
        return Ok(());
    }
    if items.len() == 1 {
        return f(&items[0]);
    }

    let n_workers = thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4)
        .min(items.len())
        .max(1);

    let next = AtomicUsize::new(0);
    let failed = AtomicBool::new(false);
    let first_err: Mutex<Option<ConsensusError>> = Mutex::new(None);

    thread::scope(|scope| {
        for _ in 0..n_workers {
            scope.spawn(|| {
                while !failed.load(Ordering::Relaxed) {
                    let i = next.fetch_add(1, Ordering::Relaxed);
                    if i >= items.len() {
                        return;
                    }
                    if let Err(e) = f(&items[i]) {
                        failed.store(true, Ordering::Relaxed);
                        let mut g = first_err.lock().unwrap_or_else(|p| p.into_inner());
                        if g.is_none() {
                            *g = Some(e);
                        }
                        return;
                    }
                }
            });
        }
    });

    match first_err.into_inner().unwrap_or_else(|p| p.into_inner()) {
        Some(e) => Err(e),
        None => Ok(()),
    }
}

/// Run `work` on a detached OS thread (async scripts phase feed-ahead).
pub(crate) fn spawn_detached<F>(work: F)
where
    F: FnOnce() + Send + 'static,
{
    // Named thread helps IBD logs / `top` during scripts phase.
    let _ = thread::Builder::new()
        .name("rbtc-scripts".into())
        .spawn(work);
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;

    #[test]
    fn parallel_all_ok_and_counts() {
        let items: Vec<u32> = (0..64).collect();
        let hits = AtomicUsize::new(0);
        try_for_each_parallel(&items, |_| {
            hits.fetch_add(1, Ordering::Relaxed);
            Ok(())
        })
        .unwrap();
        assert_eq!(hits.load(Ordering::Relaxed), 64);
    }

    #[test]
    fn parallel_first_error_surfaces() {
        let items: Vec<u32> = (0..32).collect();
        let err = try_for_each_parallel(&items, |&i| {
            if i == 7 {
                Err(ConsensusError::BadBlock("boom"))
            } else {
                Ok(())
            }
        })
        .expect_err("must fail");
        assert!(format!("{err}").contains("boom"));
    }

    #[test]
    fn empty_and_single() {
        let empty: Vec<u32> = vec![];
        try_for_each_parallel(&empty, |_| Ok(())).unwrap();
        try_for_each_parallel(&[1u32], |_| Ok(())).unwrap();
    }
}
