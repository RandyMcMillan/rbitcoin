//! Lightweight parallel script-check pool (replaces rayon on the hot path).
//!
//! Production only needs: (1) parallel `try_for_each` over script jobs, and
//! (2) submit/join for mempool accept + async scripts phase. A small
//! work-stealing loop over `std::thread::scope` avoids pulling rayon +
//! crossbeam into the consensus crate graph.
//!
//! Mempool admits hop via [`run_detached_join`] onto a **process-wide** worker
//! set (not one OS thread per tx).

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::mpsc;
use std::sync::{Condvar, Mutex, OnceLock};
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
    jobs: Mutex<VecDeque<Job>>,
    cv: Condvar,
}

static WORKERS: OnceLock<ScriptWorkers> = OnceLock::new();
static WORKER_SPAWNS: AtomicUsize = AtomicUsize::new(0);
#[cfg(test)]
static IDLE_WAITERS: AtomicUsize = AtomicUsize::new(0);

fn recv_job(pool: &ScriptWorkers) -> Job {
    let mut g = pool.jobs.lock().unwrap_or_else(|p| p.into_inner());
    loop {
        if let Some(job) = g.pop_front() {
            return job;
        }
        #[cfg(test)]
        IDLE_WAITERS.fetch_add(1, Ordering::SeqCst);
        g = pool.cv.wait(g).unwrap_or_else(|p| p.into_inner());
        #[cfg(test)]
        IDLE_WAITERS.fetch_sub(1, Ordering::SeqCst);
    }
}

fn workers() -> &'static ScriptWorkers {
    static SPAWN: OnceLock<()> = OnceLock::new();
    let pool = WORKERS.get_or_init(|| ScriptWorkers {
        jobs: Mutex::new(VecDeque::new()),
        cv: Condvar::new(),
    });
    SPAWN.get_or_init(|| {
        let n = thread::available_parallelism()
            .map(|p| p.get())
            .unwrap_or(4)
            .max(1);
        for i in 0..n {
            let _ = thread::Builder::new()
                .name(format!("rbtc-scripts-{i}"))
                .spawn(move || loop {
                    let f = recv_job(pool);
                    f();
                });
            WORKER_SPAWNS.fetch_add(1, Ordering::Relaxed);
        }
    });
    pool
}

/// How many OS worker threads the process pool has started (tests).
#[cfg(test)]
pub(crate) fn worker_spawn_count() -> usize {
    let _ = workers();
    WORKER_SPAWNS.load(Ordering::Relaxed)
}

/// Workers currently blocked in the idle wait (recv / condvar), not in a job.
#[cfg(test)]
fn idle_waiter_count() -> usize {
    IDLE_WAITERS.load(Ordering::SeqCst)
}

/// Submit `work` to the process-wide `rbtc-scripts` pool (IBD feed-ahead).
pub(crate) fn spawn_detached<F>(work: F)
where
    F: FnOnce() + Send + 'static,
{
    let pool = workers();
    {
        let mut q = pool.jobs.lock().unwrap_or_else(|p| p.into_inner());
        q.push_back(Box::new(work));
    }
    pool.cv.notify_one();
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

    /// All `rbtc-scripts-*` workers must be able to sit in the idle wait at
    /// once. `Mutex<mpsc::Receiver>` holds the lock across `recv`, so only one
    /// waiter is in recv; the rest block on `lock()` and do not count as idle.
    #[test]
    fn pool_waiters_run_concurrently() {
        use std::sync::{Arc, Condvar, Mutex};
        use std::time::{Duration, Instant};

        let n = worker_spawn_count();
        assert!(n >= 1);
        let start = Instant::now();
        while idle_waiter_count() < n {
            assert!(
                start.elapsed() < Duration::from_secs(1),
                "only {} of {n} workers idle-waiting (recv mutex serializes waiters)",
                idle_waiter_count()
            );
            thread::sleep(Duration::from_millis(1));
        }
        let inside = Arc::new(AtomicUsize::new(0));
        let gate = Arc::new((Mutex::new(false), Condvar::new()));
        let done = Arc::new(AtomicUsize::new(0));
        for _ in 0..n {
            let inside = Arc::clone(&inside);
            let gate = Arc::clone(&gate);
            let done = Arc::clone(&done);
            spawn_detached(move || {
                inside.fetch_add(1, Ordering::SeqCst);
                let (lock, cv) = &*gate;
                let mut g = lock.lock().unwrap_or_else(|p| p.into_inner());
                while !*g {
                    g = cv.wait(g).unwrap_or_else(|p| p.into_inner());
                }
                done.fetch_add(1, Ordering::SeqCst);
            });
        }
        let start = Instant::now();
        while inside.load(Ordering::SeqCst) < n {
            assert!(
                start.elapsed() < Duration::from_secs(1),
                "only {} of {n} workers entered a job (recv mutex serializes waiters)",
                inside.load(Ordering::SeqCst)
            );
            thread::sleep(Duration::from_millis(1));
        }
        {
            let (lock, cv) = &*gate;
            *lock.lock().unwrap_or_else(|p| p.into_inner()) = true;
            cv.notify_all();
        }
        let start = Instant::now();
        while done.load(Ordering::SeqCst) < n {
            assert!(
                start.elapsed() < Duration::from_secs(1),
                "workers did not finish after release"
            );
            thread::sleep(Duration::from_millis(1));
        }
    }
}
