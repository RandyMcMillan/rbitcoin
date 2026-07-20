//! Shared catch-up sorted-run machinery (tx / point / scripthash).
//!
//! Pipeline: **memtable → spill sorted run → compact merge → materialize**.
//! Each builder owns its record layout and materialize step; this module owns
//! worker timing, merge policy, finalize wait, and runs-dir lifecycle.

use rbitcoin_log::warn;
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
/// Idle wait when neither flush nor merge is needed.
pub const IDLE_POLL: Duration = Duration::from_millis(100);
/// Merge when on-disk run count reaches this (or finalize with >1 run).
pub const MAX_RUNS: usize = 16;
/// Finalize wait: 10ms × this ≈ 60s budget for large memtables.
pub const FINALIZE_POLL_MAX: u32 = 6000;

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

/// Background loop: flush when soft-full or finalizing; compact when too many runs.
pub fn worker_loop<T: RunMemtable + 'static>(
    soft_cap: usize,
    log_tag: &'static str,
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
        let need_merge =
            run_count >= MAX_RUNS || (g.control().finalize && run_count > 1 && !need_flush);

        if !need_flush && !need_merge {
            if g.control().finalize && g.pending_len() == 0 {
                // Leave final multi-run merge to finalize_and_materialize.
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
            if runs.len() >= 2 {
                runs.sort_by_key(|r| r.count);
                let n = runs.len().min(4).max(2);
                let batch: Vec<SortedRunPath> = runs.drain(..n).collect();
                let out = next_run_path(&runs_dir, out_seq);
                if let Err(e) = merge_runs(&batch, &out) {
                    warn!("ibd: {log_tag} run compact failed: {e}");
                }
                drop(_io);
                std::thread::sleep(AFTER_WORK);
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
