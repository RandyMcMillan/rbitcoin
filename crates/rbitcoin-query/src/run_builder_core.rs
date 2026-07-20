//! Shared catch-up sorted-run machinery (tx / point / scripthash).
//!
//! Pipeline: **memtable → spill sorted run → compact merge → materialize**.
//! Each builder owns its record layout and materialize step; this module owns
//! worker timing, merge policy, finalize wait, and runs-dir lifecycle.
//!
//! **Lead compact:** when archive is far ahead of tip and the archive queue is
//! not backed up, idle-IO workers keep merging toward one run per family so
//! tip-enter materialize is a single sequential scan (not a multi-GB multi-pass
//! merge of ~16 peer runs).

use rbitcoin_log::{info, warn};
use rbitcoin_store::{
    list_runs, merge_runs, next_run_path, try_set_io_idle, SortedRunPath, StoreError,
};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;

/// Sleep after a productive flush/compact so confirm keeps the disk.
pub const AFTER_WORK: Duration = Duration::from_millis(40);
/// Longer yield after optional lead-compacts (big sequential merges).
pub const AFTER_WORK_LEAD: Duration = Duration::from_millis(120);
/// Idle wait when neither flush nor merge is needed.
pub const IDLE_POLL: Duration = Duration::from_millis(100);
/// Merge when on-disk run count reaches this (or finalize with >1 run).
pub const MAX_RUNS: usize = 16;
/// When archive lead is high, keep merging until this many runs remain.
pub const LEAD_TARGET_RUNS: usize = 1;
/// Finalize wait: 10ms × this ≈ 60s budget for large memtables.
pub const FINALIZE_POLL_MAX: u32 = 6000;

/// Run-builder family id for merge scheduling (lower = higher priority).
pub const FAMILY_POINT: u8 = 1;
pub const FAMILY_TX: u8 = 2;
pub const FAMILY_SH: u8 = 3;

/// IBD → run-worker pressure: archive lead + pipeline heat.
///
/// Published by the IBD main loop. Workers use it only for **optional**
/// lead-compacts (below [`MAX_RUNS`]); hard cap / finalize merges always run.
pub mod run_compact_pressure {
    use super::{LEAD_TARGET_RUNS, MAX_RUNS};
    use std::sync::atomic::{AtomicU32, AtomicU64, AtomicU8, Ordering};

    static ARCH_LEAD: AtomicU32 = AtomicU32::new(0);
    static ARCH_QUEUE: AtomicU32 = AtomicU32::new(0);
    /// Optional merges hold this (family id); 0 = free. Forced merges ignore it.
    static MERGE_OWNER: AtomicU8 = AtomicU8::new(0);
    static MERGES_LEAD: AtomicU64 = AtomicU64::new(0);
    static MERGES_FORCED: AtomicU64 = AtomicU64::new(0);
    static LAST_LOG_NS: AtomicU64 = AtomicU64::new(0);

    /// Default archive-lead blocks before aggressive compact. `0` disables.
    pub fn lead_threshold_from_env() -> u32 {
        std::env::var("RBITCOIN_RUN_COMPACT_LEAD")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(2_048)
    }

    /// Target run count under lead compact (default 1).
    pub fn lead_target_from_env() -> usize {
        std::env::var("RBITCOIN_RUN_COMPACT_TARGET")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(LEAD_TARGET_RUNS)
            .clamp(1, MAX_RUNS)
    }

    /// Archive queue depth above which optional compact backs off.
    pub fn arch_q_hot_from_env() -> u32 {
        std::env::var("RBITCOIN_RUN_COMPACT_ARCH_Q_HOT")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(256)
    }

    pub fn publish(arch_lead: u32, arch_queue: u32) {
        ARCH_LEAD.store(arch_lead, Ordering::Relaxed);
        ARCH_QUEUE.store(arch_queue, Ordering::Relaxed);
    }

    pub fn arch_lead() -> u32 {
        ARCH_LEAD.load(Ordering::Relaxed)
    }

    pub fn arch_queue() -> u32 {
        ARCH_QUEUE.load(Ordering::Relaxed)
    }

    /// True when archive is far enough ahead and the archive prep queue is not hot.
    pub fn aggressive() -> bool {
        let thr = lead_threshold_from_env();
        if thr == 0 {
            return false;
        }
        if ARCH_LEAD.load(Ordering::Relaxed) < thr {
            return false;
        }
        ARCH_QUEUE.load(Ordering::Relaxed) < arch_q_hot_from_env()
    }

    /// Desired max on-disk runs for optional compact (not the hard cap).
    pub fn target_runs() -> usize {
        if aggressive() {
            lead_target_from_env()
        } else {
            MAX_RUNS
        }
    }

    /// Try to claim the optional-merge slot. Forced merges must not call this.
    pub fn try_begin_optional_merge(family: u8) -> bool {
        MERGE_OWNER
            .compare_exchange(0, family, Ordering::AcqRel, Ordering::Relaxed)
            .is_ok()
    }

    pub fn end_optional_merge(family: u8) {
        let _ = MERGE_OWNER.compare_exchange(family, 0, Ordering::AcqRel, Ordering::Relaxed);
    }

    pub fn note_merge(lead_optional: bool) {
        if lead_optional {
            MERGES_LEAD.fetch_add(1, Ordering::Relaxed);
        } else {
            MERGES_FORCED.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// `(lead_merges, forced_merges)` since last sample.
    pub fn sample_merges() -> (u64, u64) {
        (
            MERGES_LEAD.swap(0, Ordering::Relaxed),
            MERGES_FORCED.swap(0, Ordering::Relaxed),
        )
    }

    /// Rate-limit INFO about lead-compact mode (≈ every 30s).
    pub fn should_log_mode() -> bool {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        let prev = LAST_LOG_NS.load(Ordering::Relaxed);
        if now.saturating_sub(prev) >= 30 {
            LAST_LOG_NS.store(now, Ordering::Relaxed);
            true
        } else {
            false
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
    ///
    /// Lookups and compact used to race: `list_runs` saw a path, merge deleted
    /// it, `open_run` failed and was mis-logged as "corrupt sorted run", and
    /// keys could be missed mid-merge. Hold this for the whole file op set.
    pub runs_io: Arc<Mutex<()>>,
}

impl RunControl {
    /// Create `store_dir/subdir` and resume `next_seq` from existing runs.
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
    /// Spill `pending` to a new sorted run; clear memtable.
    fn flush_pending(&mut self) -> Result<u64, StoreError>;
}

/// Soft memtable cap from env or default (min 1000).
pub fn memtable_cap(env_key: &str, default: usize) -> usize {
    std::env::var(env_key)
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
        .max(1_000)
}

/// Start an idle-IO-priority background worker (no-op if already running).
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

/// Background loop: flush when soft-full or finalizing; compact under pressure.
///
/// - **Forced** merge: `runs ≥ MAX_RUNS` or finalize — always, no global gate.
/// - **Lead** merge: archive far ahead + arch queue not hot → merge while
///   `runs > target` (default 1). Single optional-merge slot so families do not
///   thrash the disk together; [`FAMILY_POINT`] is preferred by shorter idle.
pub fn worker_loop<T: RunMemtable + 'static>(
    soft_cap: usize,
    log_tag: &'static str,
    family: u8,
    inner: Arc<Mutex<T>>,
    cv: Arc<Condvar>,
) {
    let mut logged_lead = false;
    loop {
        let mut g = inner.lock().unwrap();
        if g.control().stop {
            break;
        }
        let need_flush =
            g.pending_len() >= soft_cap || (g.control().finalize && g.pending_len() > 0);
        let runs_dir = g.control().runs_dir.clone();
        let runs_io = Arc::clone(&g.control().runs_io);
        // Count under runs_io so we don't race compact deletes.
        let run_count = {
            drop(g);
            let _io = runs_io.lock().unwrap();
            list_runs(&runs_dir).map(|r| r.len()).unwrap_or(0)
        };
        g = inner.lock().unwrap();
        if g.control().stop {
            break;
        }
        let finalize = g.control().finalize;
        let forced_merge =
            run_count >= MAX_RUNS || (finalize && run_count > 1 && !need_flush);
        let target = run_compact_pressure::target_runs();
        let lead_mode = run_compact_pressure::aggressive();
        let optional_merge = !forced_merge && lead_mode && run_count > target;
        let need_merge = forced_merge || optional_merge;

        if lead_mode && !logged_lead && run_compact_pressure::should_log_mode() {
            logged_lead = true;
            info!(
                "ibd: {log_tag} run lead-compact ON lead={} arch_q={} runs={run_count} target={target} (RBITCOIN_RUN_COMPACT_LEAD)",
                run_compact_pressure::arch_lead(),
                run_compact_pressure::arch_queue(),
            );
        }
        if !lead_mode {
            logged_lead = false;
        }

        if !need_flush && !need_merge {
            if finalize && g.pending_len() == 0 {
                // Leave final multi-run merge to finalize_and_materialize.
                g.control_mut().stop = true;
                cv.notify_all();
                break;
            }
            // Point wakes more often under lead so it claims optional merges first.
            let idle = if lead_mode && family == FAMILY_POINT {
                Duration::from_millis(40)
            } else if lead_mode {
                Duration::from_millis(150)
            } else {
                IDLE_POLL
            };
            let (gg, _) = cv.wait_timeout(g, idle).unwrap();
            g = gg;
            if g.control().stop {
                break;
            }
            continue;
        }
        drop(g);

        if need_flush {
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

        if need_merge {
            let optional = optional_merge && !forced_merge;
            if optional && !run_compact_pressure::try_begin_optional_merge(family) {
                // Another family is lead-compacting; back off.
                std::thread::sleep(if family == FAMILY_POINT {
                    Duration::from_millis(30)
                } else {
                    Duration::from_millis(80)
                });
                continue;
            }
            // Lock order: never take Inner while holding runs_io (flush holds
            // Inner then runs_io). Reserve output seq under Inner first.
            let (runs_dir, out_seq, runs_io) = {
                let mut g = inner.lock().unwrap();
                let runs_dir = g.control().runs_dir.clone();
                let runs_io = Arc::clone(&g.control().runs_io);
                let out_seq = g.control().next_seq;
                g.control_mut().next_seq = out_seq.saturating_add(1);
                (runs_dir, out_seq, runs_io)
            };
            let _io = runs_io.lock().unwrap();
            let mut runs = list_runs(&runs_dir).unwrap_or_default();
            let merged_ok = if runs.len() >= 2 {
                runs.sort_by_key(|r| r.count);
                // Lead: 2-way on huge peers (less write amp). Forced: up to 4.
                let n = if optional {
                    2
                } else {
                    runs.len().min(4).max(2)
                };
                let batch: Vec<SortedRunPath> = runs.drain(..n).collect();
                let out = next_run_path(&runs_dir, out_seq);
                match merge_runs(&batch, &out) {
                    Ok(_) => {
                        run_compact_pressure::note_merge(optional);
                        true
                    }
                    Err(e) => {
                        warn!("ibd: {log_tag} run compact failed: {e}");
                        false
                    }
                }
            } else {
                false
            };
            drop(_io);
            if optional {
                run_compact_pressure::end_optional_merge(family);
            }
            if merged_ok {
                std::thread::sleep(if optional {
                    AFTER_WORK_LEAD
                } else {
                    AFTER_WORK
                });
            }
            // If <2 runs, seq is a harmless hole (race with concurrent finalize).
        }
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

/// Merge all runs in `runs_dir` down to one (or none). Updates `next_seq` via `inner`.
pub fn compact_all_to_one<T: RunMemtable>(
    inner: &Mutex<T>,
) -> Result<Option<SortedRunPath>, StoreError> {
    let (runs_dir, runs_io) = {
        let g = inner.lock().unwrap();
        (g.control().runs_dir.clone(), Arc::clone(&g.control().runs_io))
    };
    loop {
        let mut runs = {
            let _io = runs_io.lock().unwrap();
            list_runs(&runs_dir)?
        };
        if runs.len() <= 1 {
            return Ok(runs.into_iter().next());
        }
        let n = runs.len().min(8);
        let batch: Vec<SortedRunPath> = runs.drain(..n).collect();
        let seq = {
            let mut g = inner.lock().unwrap();
            let s = g.control().next_seq;
            g.control_mut().next_seq = s.saturating_add(1);
            s
        };
        let out = next_run_path(&runs_dir, seq);
        let _io = runs_io.lock().unwrap();
        let merged = merge_runs(&batch, &out)?;
        drop(_io);
        // Loop re-lists under the gate (other runs may still exist).
        let _ = merged;
    }
}

/// Delete all files under the runs directory after materialize.
pub fn clear_runs_dir(runs_dir: &Path) {
    if let Ok(rd) = std::fs::read_dir(runs_dir) {
        for e in rd.flatten() {
            let _ = std::fs::remove_file(e.path());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::run_compact_pressure;

    #[test]
    fn lead_compact_aggressive_when_lead_high_and_queue_cool() {
        // Isolate from process env defaults.
        std::env::set_var("RBITCOIN_RUN_COMPACT_LEAD", "100");
        std::env::set_var("RBITCOIN_RUN_COMPACT_ARCH_Q_HOT", "50");
        std::env::set_var("RBITCOIN_RUN_COMPACT_TARGET", "1");
        run_compact_pressure::publish(5_000, 10);
        assert!(run_compact_pressure::aggressive());
        assert_eq!(run_compact_pressure::target_runs(), 1);
        run_compact_pressure::publish(5_000, 200);
        assert!(!run_compact_pressure::aggressive());
        assert_eq!(run_compact_pressure::target_runs(), super::MAX_RUNS);
        run_compact_pressure::publish(50, 0);
        assert!(!run_compact_pressure::aggressive());
        // Optional merge token.
        assert!(run_compact_pressure::try_begin_optional_merge(super::FAMILY_POINT));
        assert!(!run_compact_pressure::try_begin_optional_merge(super::FAMILY_TX));
        run_compact_pressure::end_optional_merge(super::FAMILY_POINT);
        assert!(run_compact_pressure::try_begin_optional_merge(super::FAMILY_TX));
        run_compact_pressure::end_optional_merge(super::FAMILY_TX);
        std::env::remove_var("RBITCOIN_RUN_COMPACT_LEAD");
        std::env::remove_var("RBITCOIN_RUN_COMPACT_ARCH_Q_HOT");
        std::env::remove_var("RBITCOIN_RUN_COMPACT_TARGET");
        run_compact_pressure::publish(0, 0);
    }
}
