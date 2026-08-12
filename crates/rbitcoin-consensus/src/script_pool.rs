//! Lightweight parallel script-check pool (replaces rayon on the hot path).
//!
//! Production only needs: (1) parallel `try_for_each` over script jobs, and
//! (2) submit/join for mempool accept + async scripts phase. A small
//! work-stealing loop over `std::thread::scope` avoids pulling rayon +
//! crossbeam into the consensus crate graph.
//!
//! Mempool admits hop via [`run_detached_join`] onto a **process-wide** worker
//! set (not one OS thread per tx).

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::mpsc;
use std::sync::{Mutex, OnceLock};
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

type Job = Box<dyn FnOnce() + Send + 'static>;

struct ScriptWorkers {
    tx: Mutex<mpsc::Sender<Job>>,
}

static WORKERS: OnceLock<ScriptWorkers> = OnceLock::new();
static WORKER_SPAWNS: AtomicUsize = AtomicUsize::new(0);

fn workers() -> &'static ScriptWorkers {
    WORKERS.get_or_init(|| {
        let n = thread::available_parallelism()
            .map(|p| p.get())
            .unwrap_or(4)
            .max(1);
        let (tx, rx) = mpsc::channel::<Job>();
        let rx = std::sync::Arc::new(Mutex::new(rx));
        for i in 0..n {
            let rx = std::sync::Arc::clone(&rx);
            let _ = thread::Builder::new()
                .name(format!("rbtc-scripts-{i}"))
                .spawn(move || loop {
                    let job = {
                        let g = rx.lock().unwrap_or_else(|p| p.into_inner());
                        g.recv()
                    };
                    match job {
                        Ok(f) => f(),
                        Err(_) => return,
                    }
                });
            WORKER_SPAWNS.fetch_add(1, Ordering::Relaxed);
        }
        ScriptWorkers { tx: Mutex::new(tx) }
    })
}

/// How many OS worker threads the process pool has started (tests).
#[cfg(test)]
pub(crate) fn worker_spawn_count() -> usize {
    let _ = workers();
    WORKER_SPAWNS.load(Ordering::Relaxed)
}

/// Submit `work` to the process-wide `rbtc-scripts` pool (IBD feed-ahead).
pub(crate) fn spawn_detached<F>(work: F)
where
    F: FnOnce() + Send + 'static,
{
    let pool = workers();
    let tx = pool.tx.lock().unwrap_or_else(|p| p.into_inner());
    let _ = tx.send(Box::new(work));
}

/// Run `work` on the shared `rbtc-scripts` pool and join the result.
///
/// Used by mempool accept so the peer/tokio stack never runs the interpreter
/// (even for a single input). Returns `None` if the pool is gone.
pub(crate) fn run_detached_join<T, F>(work: F) -> Option<T>
where
    T: Send + 'static,
    F: FnOnce() -> T + Send + 'static,
{
    let (tx, rx) = mpsc::sync_channel(1);
    spawn_detached(move || {
        let _ = tx.send(work());
    });
    rx.recv().ok()
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

    #[test]
    fn spawn_detached_runs_work() {
        use std::sync::mpsc;
        let (tx, rx) = mpsc::sync_channel(1);
        spawn_detached(move || {
            let _ = tx.send(42u32);
        });
        assert_eq!(
            rx.recv_timeout(std::time::Duration::from_secs(2)).unwrap(),
            42
        );
    }

    #[test]
    fn join_many_does_not_spawn_per_job() {
        let before = worker_spawn_count();
        assert!(before >= 1);
        for i in 0..32u32 {
            let v = run_detached_join(move || i).expect("join");
            assert_eq!(v, i);
        }
        assert_eq!(
            worker_spawn_count(),
            before,
            "pool must not spawn a thread per mempool-style join"
        );
    }
}
