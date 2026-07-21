//! Shared catch-up sorted-run machinery (tx / point / scripthash).
//!
//! Pipeline: **memtable → spill sorted run → (SH: gradual merge) → paced
//! materialize into open-hash heads** under fetch hysteresis.
//!
//! Point/tx workers flush only. Scripthash uses a custom worker that also
//! merges ~1 pair of oldest runs per second (never the newest spill target).
//!
//! When archive lead is large, IBD **stops peer block fetches** (not the archive
//! writer — that would leave prepared work in limbo). After **in-flight getdata
//! drains to zero**, an idle-IO worker materializes one small run at a time into
//! durable heads (one shard rehash at a time). When lead shrinks, fetches resume
//! and materialize stops until lead rebuilds. At archive≈tip, materialize
//! continues until runs empty (once inflight is clear).

use rbitcoin_log::warn;
use rbitcoin_store::{list_runs, try_set_io_idle, SortedRunPath, StoreError};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;

/// Sleep after a productive flush so confirm keeps the disk.
pub const AFTER_WORK: Duration = Duration::from_millis(40);
/// Idle wait when neither flush nor finalize.
pub const IDLE_POLL: Duration = Duration::from_millis(100);
/// Finalize wait: 10ms × this ≈ 60s budget for large memtables.
pub const FINALIZE_POLL_MAX: u32 = 6000;

/// Family ids (materialize priority: point → tx → sh).
pub const FAMILY_POINT: u8 = 1;
pub const FAMILY_TX: u8 = 2;
pub const FAMILY_SH: u8 = 3;

/// IBD hysteresis: stop **peer fetches** / materialize catch-up runs into heads.
///
/// | Lead (arch − tip) | Behavior |
/// |-------------------|----------|
/// | ≥ start (default 64k) | stop new getdata; after inflight=0 materialize |
/// | < stop (default 32k) | resume fetches; pause materialize |
/// | archive at tip | materialize remaining runs once inflight=0 |
///
/// The Class A **writer is never paused** — only peer downloads stop so the
/// pipeline can drain cleanly instead of leaving prepared work limbo.
pub mod run_materialize_control {
    use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};

    /// 0 = fetch/archive mode, 1 = drain+materialize window.
    static MODE: AtomicU32 = AtomicU32::new(0);
    static ARCH_LEAD: AtomicU32 = AtomicU32::new(0);
    static PEER_INFLIGHT: AtomicU32 = AtomicU32::new(0);
    static ARCHIVE_AT_TIP: AtomicBool = AtomicBool::new(false);
    static RUNS_MATERIALIZED: AtomicU64 = AtomicU64::new(0);
    static KEYS_MATERIALIZED: AtomicU64 = AtomicU64::new(0);
    static LAST_LOG_S: AtomicU64 = AtomicU64::new(0);

    const MODE_FETCH: u32 = 0;
    const MODE_MATERIALIZE: u32 = 1;

    pub fn start_lead_from_env() -> u32 {
        std::env::var("RBITCOIN_RUN_MATERIALIZE_START_LEAD")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(65_536)
    }

    pub fn stop_lead_from_env() -> u32 {
        std::env::var("RBITCOIN_RUN_MATERIALIZE_STOP_LEAD")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(32_768)
    }

    /// `0` disables hysteresis materialize.
    pub fn enabled() -> bool {
        start_lead_from_env() > 0
    }

    /// Update lead, peer getdata inflight, and archive-caught-up; advance hysteresis.
    pub fn publish(arch_lead: u32, archive_at_tip: bool, peer_inflight: u32) {
        ARCH_LEAD.store(arch_lead, Ordering::Relaxed);
        ARCHIVE_AT_TIP.store(archive_at_tip, Ordering::Relaxed);
        PEER_INFLIGHT.store(peer_inflight, Ordering::Relaxed);
        if !enabled() {
            MODE.store(MODE_FETCH, Ordering::Relaxed);
            return;
        }
        if archive_at_tip {
            // Finish remaining runs; fetches can stay off near tip.
            MODE.store(MODE_MATERIALIZE, Ordering::Relaxed);
            return;
        }
        let start = start_lead_from_env();
        let stop = stop_lead_from_env().min(start.saturating_sub(1));
        let mode = MODE.load(Ordering::Relaxed);
        if mode == MODE_FETCH {
            if arch_lead >= start {
                MODE.store(MODE_MATERIALIZE, Ordering::Relaxed);
            }
        } else if arch_lead < stop {
            MODE.store(MODE_FETCH, Ordering::Relaxed);
        }
    }

    pub fn arch_lead() -> u32 {
        ARCH_LEAD.load(Ordering::Relaxed)
    }

    pub fn peer_inflight() -> u32 {
        PEER_INFLIGHT.load(Ordering::Relaxed)
    }

    pub fn archive_at_tip() -> bool {
        ARCHIVE_AT_TIP.load(Ordering::Relaxed)
    }

    pub fn in_materialize_mode() -> bool {
        enabled() && MODE.load(Ordering::Relaxed) == MODE_MATERIALIZE
    }

    /// Stop new peer block getdata (writer keeps draining what it already has).
    pub fn should_pause_peer_fetch() -> bool {
        in_materialize_mode() && !ARCHIVE_AT_TIP.load(Ordering::Relaxed)
    }

    /// @deprecated name — use [`should_pause_peer_fetch`].
    pub fn should_pause_archive() -> bool {
        should_pause_peer_fetch()
    }

    /// Materialize only after peer downloads are quiet (pipeline can finish writing).
    pub fn should_materialize() -> bool {
        in_materialize_mode() && PEER_INFLIGHT.load(Ordering::Relaxed) == 0
    }

    pub fn note_materialized(keys: u64) {
        RUNS_MATERIALIZED.fetch_add(1, Ordering::Relaxed);
        KEYS_MATERIALIZED.fetch_add(keys, Ordering::Relaxed);
    }

    /// `(runs, keys)` since last sample.
    pub fn sample() -> (u64, u64) {
        (
            RUNS_MATERIALIZED.swap(0, Ordering::Relaxed),
            KEYS_MATERIALIZED.swap(0, Ordering::Relaxed),
        )
    }

    pub fn should_log_mode() -> bool {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        let prev = LAST_LOG_S.load(Ordering::Relaxed);
        if now.saturating_sub(prev) >= 30 {
            LAST_LOG_S.store(now, Ordering::Relaxed);
            true
        } else {
            false
        }
    }

    pub fn mode_label() -> &'static str {
        if !in_materialize_mode() {
            "fetch"
        } else if PEER_INFLIGHT.load(Ordering::Relaxed) > 0 {
            "drain"
        } else {
            "materialize"
        }
    }
}

/// Control plane shared by every run builder's `Inner`.
pub struct RunControl {
    pub runs_dir: PathBuf,
    pub next_seq: u64,
    pub stop: bool,
    pub finalize: bool,
    /// Serializes run file list / write / merge / delete.
    pub runs_io: Arc<Mutex<()>>,
}

impl RunControl {
    pub fn open(store_dir: &Path, subdir: &str) -> Self {
        let runs_dir = store_dir.join(subdir);
        let _ = std::fs::create_dir_all(&runs_dir);
        let existing = list_runs(&runs_dir).unwrap_or_default();
        let next_seq = existing
            .iter()
            .filter_map(|r| {
                r.path
                    .file_stem()
                    .and_then(|s| s.to_str())
                    .and_then(|s| s.parse::<u64>().ok())
            })
            .max()
            .map(|n| n + 1)
            .unwrap_or(1);
        Self {
            runs_dir,
            next_seq,
            stop: false,
            finalize: false,
            runs_io: Arc::new(Mutex::new(())),
        }
    }

    pub fn reset_for_enable(&mut self) {
        self.stop = false;
        self.finalize = false;
    }
}

/// Memtable + control for the shared worker / finalize path.
pub trait RunMemtable: Send {
    fn pending_len(&self) -> usize;
    fn control(&self) -> &RunControl;
    fn control_mut(&mut self) -> &mut RunControl;
    fn flush_pending(&mut self) -> Result<u64, StoreError>;
}

pub fn memtable_cap(env_key: &str, default: usize) -> usize {
    std::env::var(env_key)
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
        .max(1_000)
}

pub fn spawn_worker(
    thread_name: &str,
    start_log: impl FnOnce(),
    enabled: &AtomicBool,
    join: &Mutex<Option<JoinHandle<()>>>,
    work: impl FnOnce() + Send + 'static,
) {
    if enabled.swap(true, Ordering::SeqCst) {
        return;
    }
    let mut jg = join.lock().unwrap();
    if jg.is_some() {
        return;
    }
    start_log();
    *jg = Some(
        std::thread::Builder::new()
            .name(thread_name.into())
            .spawn(move || {
                try_set_io_idle();
                work();
            })
            .unwrap_or_else(|e| panic!("spawn {thread_name}: {e}")),
    );
}

/// Background loop: **flush only** (no run↔run merge during IBD).
///
/// Materialize is owned by the IBD hysteresis worker (one run at a time into
/// open-hash heads). Finalize still drains the memtable then stops; tip-mode
/// materialize walks remaining runs without merging.
pub fn worker_loop<T: RunMemtable + 'static>(
    soft_cap: usize,
    log_tag: &'static str,
    _family: u8,
    inner: Arc<Mutex<T>>,
    cv: Arc<Condvar>,
) {
    loop {
        let mut g = inner.lock().unwrap();
        if g.control().stop {
            break;
        }
        let need_flush =
            g.pending_len() >= soft_cap || (g.control().finalize && g.pending_len() > 0);
        if !need_flush {
            if g.control().finalize && g.pending_len() == 0 {
                g.control_mut().stop = true;
                cv.notify_all();
                break;
            }
            let (gg, _) = cv.wait_timeout(g, IDLE_POLL).unwrap();
            g = gg;
            if g.control().stop {
                break;
            }
            continue;
        }
        drop(g);

        let mut g = inner.lock().unwrap();
        if g.pending_len() > 0 {
            if let Err(e) = g.flush_pending() {
                warn!("ibd: {log_tag} run flush failed: {e}");
            }
            cv.notify_all();
        }
        drop(g);
        std::thread::sleep(AFTER_WORK);
    }
}

/// Disable enqueues, signal finalize, wait for worker drain + join, flush leftovers.
pub fn finalize_wait_join<T: RunMemtable>(
    enabled: &AtomicBool,
    inner: &Mutex<T>,
    cv: &Condvar,
    join: &Mutex<Option<JoinHandle<()>>>,
) -> Result<(), StoreError> {
    enabled.store(false, Ordering::SeqCst);
    {
        let mut g = inner.lock().unwrap();
        g.control_mut().finalize = true;
        cv.notify_all();
    }
    for _ in 0..FINALIZE_POLL_MAX {
        {
            let g = inner.lock().unwrap();
            if g.control().stop && g.pending_len() == 0 {
                break;
            }
        }
        cv.notify_all();
        std::thread::sleep(Duration::from_millis(10));
    }
    if let Some(h) = join.lock().unwrap().take() {
        let _ = h.join();
    }
    {
        let mut g = inner.lock().unwrap();
        if g.pending_len() > 0 {
            g.flush_pending()?;
        }
    }
    Ok(())
}

/// Pop the oldest on-disk run (lowest seq) under `runs_io`. Caller materializes
/// then [`rbitcoin_store::remove_run`].
pub fn take_oldest_run(
    runs_dir: &Path,
    runs_io: &Mutex<()>,
) -> Result<Option<SortedRunPath>, StoreError> {
    let _io = runs_io.lock().unwrap();
    let mut runs = list_runs(runs_dir)?;
    if runs.is_empty() {
        return Ok(None);
    }
    runs.sort_by_key(|r| r.seq().unwrap_or(u64::MAX));
    Ok(runs.into_iter().next())
}

pub fn clear_runs_dir(runs_dir: &Path) {
    if let Ok(rd) = std::fs::read_dir(runs_dir) {
        for e in rd.flatten() {
            let _ = std::fs::remove_file(e.path());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::run_materialize_control as ctl;

    #[test]
    fn hysteresis_start_stop_drain_and_tip() {
        std::env::set_var("RBITCOIN_RUN_MATERIALIZE_START_LEAD", "1000");
        std::env::set_var("RBITCOIN_RUN_MATERIALIZE_STOP_LEAD", "500");
        ctl::publish(100, false, 0);
        assert!(!ctl::should_materialize());
        assert!(!ctl::should_pause_peer_fetch());
        // Lead high but downloads still in flight: pause fetch, do not materialize yet.
        ctl::publish(1500, false, 12);
        assert!(ctl::should_pause_peer_fetch());
        assert!(!ctl::should_materialize());
        assert_eq!(ctl::mode_label(), "drain");
        // Inflight clear → materialize.
        ctl::publish(1500, false, 0);
        assert!(ctl::should_materialize());
        assert!(ctl::should_pause_peer_fetch());
        assert_eq!(ctl::mode_label(), "materialize");
        // Stay in window while between stop and start.
        ctl::publish(700, false, 0);
        assert!(ctl::should_materialize());
        ctl::publish(400, false, 0);
        assert!(!ctl::should_materialize());
        assert!(!ctl::should_pause_peer_fetch());
        // Archive at tip: materialize with quiet peers; do not pause fetch for hysteresis.
        ctl::publish(10, true, 0);
        assert!(ctl::should_materialize());
        assert!(!ctl::should_pause_peer_fetch());
        std::env::remove_var("RBITCOIN_RUN_MATERIALIZE_START_LEAD");
        std::env::remove_var("RBITCOIN_RUN_MATERIALIZE_STOP_LEAD");
        ctl::publish(0, false, 0);
    }
}
