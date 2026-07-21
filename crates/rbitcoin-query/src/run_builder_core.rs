//! Shared sorted-run machinery for **scripthash** (Direct IBD).
//!
//! Pipeline: **memtable → spill sorted run → gradual merge → bulk load at tip**.
//! No peer-fetch pause / progressive head materialize (removed with Catchup).

use rbitcoin_store::{list_runs, try_set_io_idle, StoreError};
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

/// Family id (SH only; residual constant for worker spawn).
pub const FAMILY_SH: u8 = 3;

/// Control plane shared by every run builder's `Inner`.
///
/// **`runs_io` invariant:** all `list_runs` + write/merge/claim/delete for this
/// family must hold `runs_io` for the full critical section.
pub struct RunControl {
    pub runs_dir: PathBuf,
    pub next_seq: u64,
    pub stop: bool,
    pub finalize: bool,
    /// Serializes run file list / write / merge / delete / orphan cleanup.
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

/// Disable enqueues, signal finalize, wait for worker drain + join, flush leftovers.
///
/// If no worker was ever spawned (`join` is empty), skip the poll wait — otherwise
/// [`FINALIZE_POLL_MAX`] × 10 ms (~60 s) per idle builder stacks to ~2 min at
/// Direct-mode startup when point/tx runs were never enabled.
pub fn finalize_wait_join<T: RunMemtable>(
    enabled: &AtomicBool,
    inner: &Mutex<T>,
    cv: &Condvar,
    join: &Mutex<Option<JoinHandle<()>>>,
) -> Result<(), StoreError> {
    enabled.store(false, Ordering::SeqCst);
    let has_worker = join.lock().unwrap().is_some();
    if !has_worker {
        // Never started (Direct IBD default for point/tx; or already joined).
        let mut g = inner.lock().unwrap();
        if g.pending_len() > 0 {
            g.flush_pending()?;
        }
        g.control_mut().finalize = true;
        g.control_mut().stop = true;
        return Ok(());
    }
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

/// On-disk run count under `runs_io` (safe concurrent with merge/list).
pub fn on_disk_run_count(runs_dir: &Path, runs_io: &Mutex<()>) -> usize {
    let _io = runs_io.lock().unwrap();
    list_runs(runs_dir).map(|r| r.len()).unwrap_or(0)
}

/// Snapshot `(runs_dir, runs_io)` from a locked memtable control.
pub fn runs_dir_io(ctrl: &RunControl) -> (PathBuf, Arc<Mutex<()>>) {
    (ctrl.runs_dir.clone(), Arc::clone(&ctrl.runs_io))
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
    use super::*;
    use std::sync::atomic::AtomicBool;
    use std::time::Instant;

    /// Minimal memtable for finalize path tests.
    struct EmptyMem {
        ctrl: RunControl,
        pending: usize,
    }

    impl RunMemtable for EmptyMem {
        fn pending_len(&self) -> usize {
            self.pending
        }
        fn control(&self) -> &RunControl {
            &self.ctrl
        }
        fn control_mut(&mut self) -> &mut RunControl {
            &mut self.ctrl
        }
        fn flush_pending(&mut self) -> Result<u64, StoreError> {
            self.pending = 0;
            Ok(0)
        }
    }

    #[test]
    fn finalize_without_worker_skips_60s_poll() {
        let dir = std::env::temp_dir().join(format!(
            "rbitcoin-finalize-idle-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let _ = std::fs::create_dir_all(&dir);
        let enabled = AtomicBool::new(false);
        let inner = Mutex::new(EmptyMem {
            ctrl: RunControl::open(&dir, "idle.runs"),
            pending: 0,
        });
        let cv = Condvar::new();
        let join: Mutex<Option<JoinHandle<()>>> = Mutex::new(None);
        let t0 = Instant::now();
        finalize_wait_join(&enabled, &inner, &cv, &join).unwrap();
        let elapsed = t0.elapsed();
        // Regression: used to burn FINALIZE_POLL_MAX × 10ms (~60s) when join was empty.
        assert!(
            elapsed.as_secs() < 2,
            "idle finalize took {elapsed:?} (expected near-instant)"
        );
        assert!(inner.lock().unwrap().control().stop);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
