//! Lightweight parallel script-check pool (replaces rayon on the hot path).
//!
//! Production: (1) [`try_for_each_parallel`] steals **chunks** of indices on
//! the process-wide `rbtc-scripts-*` workers; (2) [`spawn_detached`] /
//! [`run_detached_join`] for mempool accept; (3) [`try_for_each_parallel_idle`]
//! publishes a **background** wave claimed only when no foreground wave and no
//! detached job are waiting (sptweak CPU). Confirm scripts phases run on
//! [`spawn_coordinator`], not on steal workers (a worker must not wait on
//! this pool).
//!
//! Idle steal workers [`thread::park`]. A new wave or detached job bumps an
//! epoch and [`Thread::unpark`]s every worker (unpark-before-park leaves a
//! permit, so a worker cannot miss work by parking after the wake). The jobs
//! mutex is only the detached-job queue, never the steal wake path.
//!
//! No rayon / crossbeam.

use arc_swap::ArcSwap;
use std::cell::Cell;
use std::collections::VecDeque;
use std::ops::Range;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Condvar, Mutex, OnceLock};
use std::thread;

/// Jobs claimed per steal. Amortizes `next` / `in_wave` / `Arc<Wave>` traffic
/// without a megachunk on mixed P2WPKH/P2WSH waves.
const STEAL_CHUNK: usize = 32;

use crate::error::ConsensusError;

thread_local! {
    static ON_STEAL_WORKER: Cell<bool> = const { Cell::new(false) };
}

fn on_steal_worker() -> bool {
    ON_STEAL_WORKER.with(|c| c.get())
}

/// Type-erased `f(&items[i])`. `ctx` is valid until the publishing
/// [`try_for_each_parallel`] returns (after [`Wave::wait_done`]).
struct Apply {
    f: unsafe fn(*const (), usize) -> Result<(), ConsensusError>,
    ctx: *const (),
}

// Workers only dereference `ctx` while `in_wave > 0`; the publisher waits
// for `in_wave == 0` before returning, so the stack `ctx` is still live.
unsafe impl Send for Apply {}
unsafe impl Sync for Apply {}

struct Wave {
    n: usize,
    next: AtomicUsize,
    in_wave: AtomicUsize,
    failed: AtomicBool,
    first_err: Mutex<Option<ConsensusError>>,
    apply: Apply,
    done: Mutex<bool>,
    done_cv: Condvar,
}

impl Wave {
    fn claim_chunk(&self) -> Option<Range<usize>> {
        if self.failed.load(Ordering::Relaxed) {
            return None;
        }
        let i = self.next.fetch_add(STEAL_CHUNK, Ordering::Relaxed);
        if i >= self.n {
            return None;
        }
        #[cfg(test)]
        if STEAL_CLAIMS_ON.load(Ordering::Relaxed) {
            STEAL_CLAIMS.fetch_add(1, Ordering::Relaxed);
        }
        self.in_wave.fetch_add(1, Ordering::AcqRel);
        Some(i..self.n.min(i.saturating_add(STEAL_CHUNK)))
    }

    fn is_complete(&self) -> bool {
        let claimed_out =
            self.next.load(Ordering::Relaxed) >= self.n || self.failed.load(Ordering::Relaxed);
        claimed_out && self.in_wave.load(Ordering::Acquire) == 0
    }

    fn run_chunk(&self, range: Range<usize>) {
        // SAFETY: `in_wave` was incremented by `claim_chunk`; publisher does
        // not return until `wait_done` sees `in_wave == 0`.
        for i in range {
            if self.failed.load(Ordering::Relaxed) {
                break;
            }
            let r = unsafe { (self.apply.f)(self.apply.ctx, i) };
            if let Err(e) = r {
                self.failed.store(true, Ordering::Relaxed);
                let mut g = self.first_err.lock().unwrap_or_else(|p| p.into_inner());
                if g.is_none() {
                    *g = Some(e);
                }
                break;
            }
        }
        self.in_wave.fetch_sub(1, Ordering::AcqRel);
        if self.is_complete() {
            *self.done.lock().unwrap_or_else(|p| p.into_inner()) = true;
            self.done_cv.notify_all();
        }
    }

    fn wait_done(&self) {
        let mut g = self.done.lock().unwrap_or_else(|p| p.into_inner());
        while !self.is_complete() {
            g = self.done_cv.wait(g).unwrap_or_else(|p| p.into_inner());
        }
    }
}

static WAVES: Mutex<Vec<Arc<Wave>>> = Mutex::new(Vec::new());
static WAVES_SNAP: OnceLock<ArcSwap<Vec<Arc<Wave>>>> = OnceLock::new();
static WAVES_BG: Mutex<Vec<Arc<Wave>>> = Mutex::new(Vec::new());
static WAVES_BG_SNAP: OnceLock<ArcSwap<Vec<Arc<Wave>>>> = OnceLock::new();

#[cfg(test)]
static STEAL_WAVES_LOCKS: AtomicUsize = AtomicUsize::new(0);
#[cfg(test)]
static STEAL_CLAIMS: AtomicUsize = AtomicUsize::new(0);
/// When true, [`Wave::claim_chunk`] increments [`STEAL_CLAIMS`]. Off by default
/// so parallel `try_for_each_parallel` tests do not inflate the counter.
#[cfg(test)]
static STEAL_CLAIMS_ON: AtomicBool = AtomicBool::new(false);
/// Serialize the two 256-job steal tests so they do not share [`STEAL_CLAIMS`].
#[cfg(test)]
static STEAL_TEST: Mutex<()> = Mutex::new(());

fn waves_snap() -> &'static ArcSwap<Vec<Arc<Wave>>> {
    WAVES_SNAP.get_or_init(|| ArcSwap::from_pointee(Vec::new()))
}

fn waves_bg_snap() -> &'static ArcSwap<Vec<Arc<Wave>>> {
    WAVES_BG_SNAP.get_or_init(|| ArcSwap::from_pointee(Vec::new()))
}

fn publish_waves(waves: &[Arc<Wave>]) {
    waves_snap().store(Arc::new(waves.to_vec()));
}

fn publish_waves_bg(waves: &[Arc<Wave>]) {
    waves_bg_snap().store(Arc::new(waves.to_vec()));
}

/// Lock-free claim: load the published wave list. Must not lock [`WAVES`].
/// [`STEAL_WAVES_LOCKS`] counts steal-path mutex takes only.
fn steal_from(snap: &[Arc<Wave>]) -> Option<(Arc<Wave>, Range<usize>)> {
    for w in snap {
        if let Some(range) = w.claim_chunk() {
            return Some((Arc::clone(w), range));
        }
    }
    None
}

fn steal_chunk() -> Option<(Arc<Wave>, Range<usize>)> {
    steal_from(&waves_snap().load())
}

fn steal_bg_chunk() -> Option<(Arc<Wave>, Range<usize>)> {
    steal_from(&waves_bg_snap().load())
}

/// Parallel map over `items` until the first error (or all succeed).
///
/// Steal workers (`rbtc-scripts-*`) claim indices. Must not be called from a
/// steal worker (hard refuse — same-pool wait would deadlock). On first error,
/// workers stop claiming; in-flight units may still finish.
pub(crate) fn try_for_each_parallel<T, F>(items: &[T], f: F) -> Result<(), ConsensusError>
where
    T: Sync,
    F: Fn(&T) -> Result<(), ConsensusError> + Sync,
{
    run_wave(items, f, false)
}

/// Like [`try_for_each_parallel`], but workers claim only when no foreground
/// wave and no detached job are waiting (mempool / block scripts win).
pub(crate) fn try_for_each_parallel_idle<T, F>(items: &[T], f: F) -> Result<(), ConsensusError>
where
    T: Sync,
    F: Fn(&T) -> Result<(), ConsensusError> + Sync,
{
    run_wave(items, f, true)
}

fn run_wave<T, F>(items: &[T], f: F, background: bool) -> Result<(), ConsensusError>
where
    T: Sync,
    F: Fn(&T) -> Result<(), ConsensusError> + Sync,
{
    if on_steal_worker() {
        return Err(ConsensusError::BadBlock(
            "try_for_each from a script worker",
        ));
    }
    if items.is_empty() {
        return Ok(());
    }
    if items.len() == 1 {
        return f(&items[0]);
    }

    struct Ctx<'a, T, F> {
        items: &'a [T],
        f: &'a F,
    }
    unsafe fn apply<T, F>(ptr: *const (), i: usize) -> Result<(), ConsensusError>
    where
        F: Fn(&T) -> Result<(), ConsensusError>,
    {
        let ctx = unsafe { &*(ptr as *const Ctx<T, F>) };
        (ctx.f)(&ctx.items[i])
    }

    let ctx = Ctx { items, f: &f };
    let wave = Arc::new(Wave {
        n: items.len(),
        next: AtomicUsize::new(0),
        in_wave: AtomicUsize::new(0),
        failed: AtomicBool::new(false),
        first_err: Mutex::new(None),
        apply: Apply {
            f: apply::<T, F>,
            ctx: (&ctx as *const Ctx<T, F>).cast(),
        },
        done: Mutex::new(false),
        done_cv: Condvar::new(),
    });
    if background {
        let mut g = WAVES_BG.lock().unwrap_or_else(|p| p.into_inner());
        g.push(Arc::clone(&wave));
        publish_waves_bg(&g);
    } else {
        let mut g = WAVES.lock().unwrap_or_else(|p| p.into_inner());
        g.push(Arc::clone(&wave));
        publish_waves(&g);
    }
    wake_steal_workers();
    wave.wait_done();

    if background {
        let mut g = WAVES_BG.lock().unwrap_or_else(|p| p.into_inner());
        g.retain(|w| !Arc::ptr_eq(w, &wave));
        publish_waves_bg(&g);
    } else {
        let mut g = WAVES.lock().unwrap_or_else(|p| p.into_inner());
        g.retain(|w| !Arc::ptr_eq(w, &wave));
        publish_waves(&g);
    }

    let err = wave
        .first_err
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .take();
    match err {
        Some(e) => Err(e),
        None => Ok(()),
    }
}

type Job = Box<dyn FnOnce() + Send + 'static>;

struct ScriptWorkers {
    jobs: Mutex<VecDeque<Job>>,
    epoch: AtomicUsize,
}

static WORKERS: OnceLock<ScriptWorkers> = OnceLock::new();
static WORKER_THREADS: OnceLock<Box<[thread::Thread]>> = OnceLock::new();
static WORKER_HANDLES: OnceLock<Vec<thread::JoinHandle<()>>> = OnceLock::new();
static WORKER_SPAWNS: AtomicUsize = AtomicUsize::new(0);
#[cfg(test)]
static IDLE_WAITERS: AtomicUsize = AtomicUsize::new(0);

fn recv_job(jobs: &Mutex<VecDeque<Job>>, cv: &Condvar, count_idle: bool) -> Job {
    let _ = count_idle;
    let mut g = jobs.lock().unwrap_or_else(|p| p.into_inner());
    loop {
        if let Some(job) = g.pop_front() {
            return job;
        }
        #[cfg(test)]
        if count_idle {
            IDLE_WAITERS.fetch_add(1, Ordering::SeqCst);
        }
        g = cv.wait(g).unwrap_or_else(|p| p.into_inner());
        #[cfg(test)]
        if count_idle {
            IDLE_WAITERS.fetch_sub(1, Ordering::SeqCst);
        }
    }
}

fn take_detached_job(pool: &ScriptWorkers) -> Option<Job> {
    pool.jobs
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .pop_front()
}

fn steal_or_job(pool: &ScriptWorkers) -> bool {
    if let Some((w, range)) = steal_chunk() {
        w.run_chunk(range);
        return true;
    }
    if let Some(job) = take_detached_job(pool) {
        job();
        return true;
    }
    if let Some((w, range)) = steal_chunk() {
        w.run_chunk(range);
        return true;
    }
    if let Some((w, range)) = steal_bg_chunk() {
        w.run_chunk(range);
        return true;
    }
    false
}

fn wake_steal_workers() {
    let pool = workers();
    pool.epoch.fetch_add(1, Ordering::Release);
    if let Some(threads) = WORKER_THREADS.get() {
        for t in threads.iter() {
            t.unpark();
        }
    }
}

fn workers() -> &'static ScriptWorkers {
    static SPAWN: OnceLock<()> = OnceLock::new();
    let pool = WORKERS.get_or_init(|| ScriptWorkers {
        jobs: Mutex::new(VecDeque::new()),
        epoch: AtomicUsize::new(0),
    });
    SPAWN.get_or_init(|| {
        let n = thread::available_parallelism()
            .map(|p| p.get())
            .unwrap_or(4)
            .max(1);
        let mut threads = Vec::with_capacity(n);
        let mut handles = Vec::with_capacity(n);
        for i in 0..n {
            let Ok(h) = thread::Builder::new()
                .name(format!("rbtc-scripts-{i}"))
                .spawn(move || {
                    ON_STEAL_WORKER.with(|c| c.set(true));
                    loop {
                        if steal_or_job(pool) {
                            continue;
                        }
                        let epoch = pool.epoch.load(Ordering::Acquire);
                        if steal_or_job(pool) {
                            continue;
                        }
                        if pool.epoch.load(Ordering::Acquire) != epoch {
                            continue;
                        }
                        #[cfg(test)]
                        maybe_delay_park();
                        #[cfg(test)]
                        IDLE_WAITERS.fetch_add(1, Ordering::SeqCst);
                        thread::park();
                        #[cfg(test)]
                        IDLE_WAITERS.fetch_sub(1, Ordering::SeqCst);
                    }
                })
            else {
                continue;
            };
            threads.push(h.thread().clone());
            handles.push(h);
            WORKER_SPAWNS.fetch_add(1, Ordering::Relaxed);
        }
        let _ = WORKER_THREADS.set(threads.into_boxed_slice());
        let _ = WORKER_HANDLES.set(handles);
    });
    pool
}

/// How many OS worker threads the process pool has started (tests).
#[cfg(test)]
pub(crate) fn worker_spawn_count() -> usize {
    let _ = workers();
    WORKER_SPAWNS.load(Ordering::Relaxed)
}

/// Workers currently blocked in [`thread::park`], not in a job or steal.
#[cfg(test)]
fn idle_waiter_count() -> usize {
    IDLE_WAITERS.load(Ordering::SeqCst)
}

#[cfg(test)]
static DELAY_PARK: AtomicBool = AtomicBool::new(false);
#[cfg(test)]
static DELAY_PARK_GO: AtomicBool = AtomicBool::new(true);
#[cfg(test)]
static DELAY_PARK_ENTERED: AtomicUsize = AtomicUsize::new(0);
#[cfg(test)]
static DELAY_PARK_MU: Mutex<()> = Mutex::new(());
#[cfg(test)]
static DELAY_PARK_CV: Condvar = Condvar::new();

#[cfg(test)]
fn maybe_delay_park() {
    if !DELAY_PARK.load(Ordering::SeqCst) {
        return;
    }
    DELAY_PARK_ENTERED.fetch_add(1, Ordering::SeqCst);
    let mut g = DELAY_PARK_MU.lock().unwrap_or_else(|p| p.into_inner());
    while !DELAY_PARK_GO.load(Ordering::SeqCst) {
        g = DELAY_PARK_CV.wait(g).unwrap_or_else(|p| p.into_inner());
    }
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
    wake_steal_workers();
}

/// Two coordinators for IBD feed-ahead phases. Must not share the steal
/// worker set: a phase that waits on `try_for_each_parallel` would deadlock
/// if it occupied an `rbtc-scripts-*` thread.
struct CoordWorkers {
    jobs: Mutex<VecDeque<Job>>,
    cv: Condvar,
}

static COORD: OnceLock<CoordWorkers> = OnceLock::new();

fn coord_workers() -> &'static CoordWorkers {
    static SPAWN: OnceLock<()> = OnceLock::new();
    let pool = COORD.get_or_init(|| CoordWorkers {
        jobs: Mutex::new(VecDeque::new()),
        cv: Condvar::new(),
    });
    SPAWN.get_or_init(|| {
        for i in 0..2 {
            let _ = thread::Builder::new()
                .name(format!("rbtc-script-coord-{i}"))
                .spawn(move || loop {
                    let f = recv_job(&pool.jobs, &pool.cv, false);
                    f();
                });
        }
    });
    pool
}

/// Submit `work` on a scripts-phase coordinator (not a steal worker).
pub(crate) fn spawn_coordinator<F>(work: F)
where
    F: FnOnce() + Send + 'static,
{
    let pool = coord_workers();
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
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::{Arc, Condvar, Mutex, MutexGuard};
    use std::time::{Duration, Instant};

    static OCCUPY: Mutex<()> = Mutex::new(());

    struct OccupyGate {
        gate: Arc<(Mutex<bool>, Condvar)>,
        _occupy: MutexGuard<'static, ()>,
    }

    impl OccupyGate {
        fn occupy_all() -> Self {
            Self::occupy_n(worker_spawn_count())
        }

        fn occupy_n(k: usize) -> Self {
            let occupy = OCCUPY.lock().unwrap_or_else(|p| p.into_inner());
            let n = worker_spawn_count();
            assert!(n >= 1);
            let k = k.min(n);
            let gate = Arc::new((Mutex::new(false), Condvar::new()));
            let me = Self {
                gate: Arc::clone(&gate),
                _occupy: occupy,
            };
            let entered = Arc::new(AtomicUsize::new(0));
            for _ in 0..k {
                let entered = Arc::clone(&entered);
                let gate = Arc::clone(&gate);
                spawn_detached(move || {
                    entered.fetch_add(1, Ordering::SeqCst);
                    let (lock, cv) = &*gate;
                    let mut g = lock.lock().unwrap_or_else(|p| p.into_inner());
                    while !*g {
                        g = cv.wait(g).unwrap_or_else(|p| p.into_inner());
                    }
                });
            }
            let start = Instant::now();
            while entered.load(Ordering::SeqCst) < k {
                assert!(
                    start.elapsed() < Duration::from_secs(2),
                    "failed to occupy steal workers"
                );
                thread::sleep(Duration::from_millis(1));
            }
            me
        }

        fn release(&self) {
            let (lock, cv) = &*self.gate;
            *lock.lock().unwrap_or_else(|p| p.into_inner()) = true;
            cv.notify_all();
        }
    }

    impl Drop for OccupyGate {
        fn drop(&mut self) {
            self.release();
        }
    }

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

    /// All `rbtc-scripts-*` workers must be able to sit in [`thread::park`] at
    /// once (no jobs-mutex held across the idle wait).
    #[test]
    fn pool_waiters_run_concurrently() {
        let n = worker_spawn_count();
        assert!(n >= 1);
        let start = Instant::now();
        while idle_waiter_count() < n {
            assert!(
                start.elapsed() < Duration::from_secs(1),
                "only {} of {n} workers idle-waiting",
                idle_waiter_count()
            );
            thread::sleep(Duration::from_millis(1));
        }
        let occupy = OccupyGate::occupy_all();
        occupy.release();
        let start = Instant::now();
        while idle_waiter_count() < n {
            assert!(
                start.elapsed() < Duration::from_secs(1),
                "workers did not finish after release"
            );
            thread::sleep(Duration::from_millis(1));
        }
    }

    struct DelayParkArm;

    impl DelayParkArm {
        fn arm() -> Self {
            DELAY_PARK_ENTERED.store(0, Ordering::SeqCst);
            DELAY_PARK_GO.store(false, Ordering::SeqCst);
            DELAY_PARK.store(true, Ordering::SeqCst);
            Self
        }

        fn go(&self) {
            let _g = DELAY_PARK_MU.lock().unwrap_or_else(|p| p.into_inner());
            DELAY_PARK_GO.store(true, Ordering::SeqCst);
            DELAY_PARK_CV.notify_all();
        }
    }

    impl Drop for DelayParkArm {
        fn drop(&mut self) {
            DELAY_PARK.store(false, Ordering::SeqCst);
            let _g = DELAY_PARK_MU.lock().unwrap_or_else(|p| p.into_inner());
            DELAY_PARK_GO.store(true, Ordering::SeqCst);
            DELAY_PARK_CV.notify_all();
        }
    }

    /// Publish a wave after the last free worker has missed steal and before
    /// it parks. Condvar notify-without-mutex lost that wake; park permits
    /// must still run the wave.
    #[test]
    fn wave_published_before_park_is_not_missed() {
        let n = worker_spawn_count();
        let occupy = OccupyGate::occupy_n(n.saturating_sub(1));
        let start = Instant::now();
        while idle_waiter_count() == 0 {
            assert!(
                start.elapsed() < Duration::from_secs(2),
                "free worker did not park"
            );
            thread::sleep(Duration::from_millis(1));
        }
        let arm = DelayParkArm::arm();
        wake_steal_workers();
        let start = Instant::now();
        while DELAY_PARK_ENTERED.load(Ordering::SeqCst) == 0 {
            assert!(
                start.elapsed() < Duration::from_secs(2),
                "free worker did not reach park gate"
            );
            thread::sleep(Duration::from_millis(1));
        }
        let hits = Arc::new(AtomicUsize::new(0));
        let hits2 = Arc::clone(&hits);
        let wave = thread::spawn(move || {
            let items: Vec<u32> = (0..64).collect();
            try_for_each_parallel(&items, |_| {
                hits2.fetch_add(1, Ordering::Relaxed);
                Ok(())
            })
        });
        let start = Instant::now();
        while waves_snap().load().is_empty() {
            assert!(
                start.elapsed() < Duration::from_secs(2),
                "wave was not published"
            );
            thread::sleep(Duration::from_millis(1));
        }
        arm.go();
        wave.join().expect("wave thread").expect("wave ok");
        assert_eq!(hits.load(Ordering::Relaxed), 64);
        occupy.release();
    }

    #[test]
    fn try_for_each_runs_on_script_workers() {
        let before = worker_spawn_count();
        let items: Vec<u32> = (0..32).collect();
        let names = Mutex::new(Vec::new());
        try_for_each_parallel(&items, |_| {
            names
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .push(thread::current().name().unwrap_or("").to_string());
            Ok(())
        })
        .unwrap();
        assert_eq!(worker_spawn_count(), before);
        let names = names.lock().unwrap_or_else(|p| p.into_inner());
        assert_eq!(names.len(), 32);
        for n in names.iter() {
            assert!(n.starts_with("rbtc-scripts-"), "item ran on {n:?}");
        }
    }

    #[test]
    fn overlapping_try_for_each_both_complete() {
        let a_hits = Arc::new(AtomicUsize::new(0));
        let b_hits = Arc::new(AtomicUsize::new(0));
        let a = {
            let hits = Arc::clone(&a_hits);
            thread::spawn(move || {
                let items: Vec<u32> = (0..16).collect();
                try_for_each_parallel(&items, |_| {
                    hits.fetch_add(1, Ordering::Relaxed);
                    Ok(())
                })
            })
        };
        let b = {
            let hits = Arc::clone(&b_hits);
            thread::spawn(move || {
                let items: Vec<u32> = (0..16).collect();
                try_for_each_parallel(&items, |_| {
                    hits.fetch_add(1, Ordering::Relaxed);
                    Ok(())
                })
            })
        };
        a.join().expect("a").expect("a ok");
        b.join().expect("b").expect("b ok");
        assert_eq!(a_hits.load(Ordering::Relaxed), 16);
        assert_eq!(b_hits.load(Ordering::Relaxed), 16);
    }

    #[test]
    fn steal_chunk_amortizes_claims() {
        // 256 items → 8 chunks of 32. Not 256 fetch_adds on `next`.
        let _gate = STEAL_TEST.lock().unwrap_or_else(|p| p.into_inner());
        workers();
        STEAL_CLAIMS.store(0, Ordering::Relaxed);
        STEAL_CLAIMS_ON.store(true, Ordering::Relaxed);
        let items: Vec<u32> = (0..256).collect();
        let hits: Vec<AtomicUsize> = (0..256).map(|_| AtomicUsize::new(0)).collect();
        try_for_each_parallel(&items, |&i| {
            hits[i as usize].fetch_add(1, Ordering::Relaxed);
            Ok(())
        })
        .unwrap();
        STEAL_CLAIMS_ON.store(false, Ordering::Relaxed);
        for (i, h) in hits.iter().enumerate() {
            assert_eq!(h.load(Ordering::Relaxed), 1, "index {i} not run once");
        }
        let claims = STEAL_CLAIMS.load(Ordering::Relaxed);
        assert!(
            (8..32).contains(&claims),
            "expected ~8 chunks of 32 for 256 jobs, got {claims}"
        );
    }

    #[test]
    fn steal_index_does_not_lock_waves_per_job() {
        // Claim must not take WAVES: a 256-job wave is tens of thousands of
        // short P2WPKH jobs on IBD. Today's steal_index locks per claim.
        let _gate = STEAL_TEST.lock().unwrap_or_else(|p| p.into_inner());
        workers();
        STEAL_WAVES_LOCKS.store(0, Ordering::Relaxed);
        let items: Vec<u32> = (0..256).collect();
        let hits = AtomicUsize::new(0);
        try_for_each_parallel(&items, |_| {
            hits.fetch_add(1, Ordering::Relaxed);
            Ok(())
        })
        .unwrap();
        assert_eq!(hits.load(Ordering::Relaxed), 256);
        let locks = STEAL_WAVES_LOCKS.load(Ordering::Relaxed);
        assert_eq!(
            locks, 0,
            "steal_index took WAVES {locks} times (must be snapshot load only)"
        );
    }

    #[test]
    fn try_for_each_from_script_worker_is_refused() {
        let got =
            run_detached_join(|| try_for_each_parallel(&[1u32, 2], |_| Ok(()))).expect("join");
        let err = got.expect_err("must refuse nested wait");
        assert!(
            format!("{err}").contains("try_for_each from a script worker"),
            "{err}"
        );
    }

    #[test]
    fn try_for_each_idle_all_ok() {
        let items: Vec<u32> = (0..64).collect();
        let hits = AtomicUsize::new(0);
        try_for_each_parallel_idle(&items, |_| {
            hits.fetch_add(1, Ordering::Relaxed);
            Ok(())
        })
        .unwrap();
        assert_eq!(hits.load(Ordering::Relaxed), 64);
    }

    #[test]
    fn try_for_each_idle_from_script_worker_is_refused() {
        let got =
            run_detached_join(|| try_for_each_parallel_idle(&[1u32, 2], |_| Ok(()))).expect("join");
        let err = got.expect_err("must refuse nested wait");
        assert!(
            format!("{err}").contains("try_for_each from a script worker"),
            "{err}"
        );
    }

    /// Occupy every steal worker in a detached job, publish an idle wave whose
    /// items wait on a later job, queue that job, then release. If idle stole
    /// like a foreground wave, the job never runs (items wait on it).
    #[test]
    fn idle_wave_does_not_starve_detached_job() {
        let occupy = OccupyGate::occupy_all();

        let job2 = Arc::new(AtomicBool::new(false));
        let idle_hits = Arc::new(AtomicUsize::new(0));
        let items: Vec<u32> = (0..64).collect();
        let idle_hits2 = Arc::clone(&idle_hits);
        let job2_wait = Arc::clone(&job2);
        let idle = thread::spawn(move || {
            try_for_each_parallel_idle(&items, |_| {
                let t0 = Instant::now();
                while !job2_wait.load(Ordering::SeqCst) {
                    assert!(
                        t0.elapsed() < Duration::from_secs(2),
                        "idle item waited for a starved detached job"
                    );
                    thread::sleep(Duration::from_millis(1));
                }
                idle_hits2.fetch_add(1, Ordering::Relaxed);
                Ok(())
            })
        });
        thread::sleep(Duration::from_millis(20));
        let job2_flag = Arc::clone(&job2);
        spawn_detached(move || {
            job2_flag.store(true, Ordering::SeqCst);
        });
        occupy.release();
        let start = Instant::now();
        while !job2.load(Ordering::SeqCst) {
            assert!(
                start.elapsed() < Duration::from_secs(2),
                "idle wave starved the detached job"
            );
            thread::sleep(Duration::from_millis(1));
        }
        idle.join().expect("idle thread").expect("idle ok");
        assert_eq!(idle_hits.load(Ordering::Relaxed), 64);
    }

    #[test]
    fn idle_wave_does_not_starve_foreground_wave() {
        let occupy = OccupyGate::occupy_all();

        let fg_done = Arc::new(AtomicBool::new(false));
        let items: Vec<u32> = (0..64).collect();
        let fg_wait = Arc::clone(&fg_done);
        let idle = thread::spawn(move || {
            try_for_each_parallel_idle(&items, |_| {
                let t0 = Instant::now();
                while !fg_wait.load(Ordering::SeqCst) {
                    assert!(
                        t0.elapsed() < Duration::from_secs(2),
                        "idle item waited for a starved foreground wave"
                    );
                    thread::sleep(Duration::from_millis(1));
                }
                Ok(())
            })
        });
        thread::sleep(Duration::from_millis(20));
        let fg_hits = Arc::new(AtomicUsize::new(0));
        let fg_hits2 = Arc::clone(&fg_hits);
        let fg_flag = Arc::clone(&fg_done);
        let fg = thread::spawn(move || {
            let items: Vec<u32> = (0..32).collect();
            let r = try_for_each_parallel(&items, |_| {
                fg_hits2.fetch_add(1, Ordering::Relaxed);
                Ok(())
            });
            fg_flag.store(true, Ordering::SeqCst);
            r
        });
        occupy.release();
        fg.join().expect("fg thread").expect("fg ok");
        assert_eq!(fg_hits.load(Ordering::Relaxed), 32);
        idle.join().expect("idle thread").expect("idle ok");
    }

    #[test]
    fn panic_while_workers_occupied_does_not_deadlock_pool() {
        let prev = std::panic::take_hook();
        std::panic::set_hook(Box::new(|_| {}));
        let panicked = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _gate = OccupyGate::occupy_all();
            panic!("occupy test boom");
        }));
        std::panic::set_hook(prev);
        assert!(panicked.is_err());
        try_for_each_parallel(&[1u32, 2, 3, 4], |_| Ok(())).unwrap();
    }
}
