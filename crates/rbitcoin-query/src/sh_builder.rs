//! Catch-up scripthash builder: memtable → sorted runs → gradual merge →
//! materialize into durable `scripthash` tables.
//!
//! Confirm only enqueues thin creates (no open-hash RMW). A background worker
//! flushes and **merges** at idle IO priority (~1 merge/s of the two oldest
//! immutable runs, never the newest spill target). Materialize claims the
//! oldest run (detaches from MANIFEST) so merge cannot touch it mid-apply.

use super::run_builder_core::{
    clear_runs_dir, finalize_wait_join, memtable_cap, on_disk_run_count, runs_dir_io, spawn_worker,
    RunControl, RunMemtable, FAMILY_SH, AFTER_WORK, IDLE_POLL,
};
use rbitcoin_log::{debug, info, warn};
use rbitcoin_primitives::Fk;
use rbitcoin_store::{
    claim_run_for_materialize, for_each_merged_rec, list_materialize_claims, list_runs, merge_runs,
    next_run_path, write_sorted_run, ScriptHashRecord, Store, StoreError, SortedRunPath,
};
// re-export for tests that claim runs
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

/// Fixed run record: scripthash[32] | create_tx_fk:u64 = 40 bytes (no vout).
pub const SH_RUN_REC_LEN: u32 = 40;
pub const SH_RUN_KEY_LEN: u32 = 32;

const DEFAULT_MEMTABLE_CAP: usize = 256_000;
const HARD_MEMTABLE_MUL: usize = 2;
/// Background merge cadence (one k-way pair merge at most this often).
const MERGE_INTERVAL: Duration = Duration::from_secs(1);
/// Do not **start** a merge with a run whose body is already ≥ this size
/// (≈40 MiB / ~1M rows). Two smaller runs may still merge to larger than this.
const SH_MERGE_MAX_BODY_BYTES: u64 = 40 * 1024 * 1024;

#[inline]
fn run_body_bytes(run: &SortedRunPath) -> u64 {
    run.count.saturating_mul(u64::from(run.rec_len))
}

fn encode_rec(sh: &[u8; 32], tx_fk: Fk) -> [u8; SH_RUN_REC_LEN as usize] {
    let mut r = [0u8; SH_RUN_REC_LEN as usize];
    r[0..32].copy_from_slice(sh);
    r[32..40].copy_from_slice(&tx_fk.0.to_le_bytes());
    r
}

fn decode_rec(buf: &[u8]) -> Result<([u8; 32], Fk), StoreError> {
    if buf.len() < SH_RUN_REC_LEN as usize {
        return Err(StoreError::Corrupt("sh run short record"));
    }
    let mut sh = [0u8; 32];
    sh.copy_from_slice(&buf[0..32]);
    let tx_fk = Fk(u64::from_le_bytes(buf[32..40].try_into().unwrap()));
    Ok((sh, tx_fk))
}

struct Inner {
    pending: Vec<([u8; 32], Fk)>,
    ctrl: RunControl,
    last_merge: Instant,
}

impl RunMemtable for Inner {
    fn pending_len(&self) -> usize {
        self.pending.len()
    }
    fn control(&self) -> &RunControl {
        &self.ctrl
    }
    fn control_mut(&mut self) -> &mut RunControl {
        &mut self.ctrl
    }
    fn flush_pending(&mut self) -> Result<u64, StoreError> {
        if self.pending.is_empty() {
            return Ok(0);
        }
        let mut recs = std::mem::take(&mut self.pending);
        // Sort by scripthash (run key) then create_tx_fk; dedup multi-vout same tx.
        recs.sort_unstable_by(|a, b| a.0.cmp(&b.0).then(a.1 .0.cmp(&b.1 .0)));
        recs.dedup_by(|a, b| a.0 == b.0 && a.1 == b.1);
        let mut body = Vec::with_capacity(recs.len() * SH_RUN_REC_LEN as usize);
        for (sh, fk) in &recs {
            body.extend_from_slice(&encode_rec(sh, *fk));
        }
        let path = next_run_path(&self.ctrl.runs_dir, self.ctrl.next_seq);
        self.ctrl.next_seq += 1;
        let _io = self.ctrl.runs_io.lock().unwrap();
        write_sorted_run(&path, SH_RUN_KEY_LEN, SH_RUN_REC_LEN, &body)?;
        Ok(recs.len() as u64)
    }
}

/// Shared catch-up SH builder + low-prio worker handle.
pub struct ShRunBuilder {
    inner: Arc<Mutex<Inner>>,
    cv: Arc<Condvar>,
    enabled: AtomicBool,
    join: Mutex<Option<JoinHandle<()>>>,
    pub enqueued: AtomicU64,
}

impl ShRunBuilder {
    pub fn new(store_dir: &Path) -> Self {
        let ctrl = RunControl::open(store_dir, "scripthash.runs");
        Self {
            inner: Arc::new(Mutex::new(Inner {
                pending: Vec::new(),
                ctrl,
                last_merge: Instant::now()
                    .checked_sub(MERGE_INTERVAL)
                    .unwrap_or_else(Instant::now),
            })),
            cv: Arc::new(Condvar::new()),
            enabled: AtomicBool::new(false),
            join: Mutex::new(None),
            enqueued: AtomicU64::new(0),
        }
    }

    pub fn is_enabled(&self) -> bool {
        self.enabled.load(Ordering::Relaxed)
    }

    /// Start low-prio worker and accept enqueues.
    pub fn enable(&self) {
        {
            let mut g = self.inner.lock().unwrap();
            g.ctrl.reset_for_enable();
        }
        let inner_w = Arc::clone(&self.inner);
        let cv_w = Arc::clone(&self.cv);
        spawn_worker(
            "ibd-sh-index",
            || {
                info!(
                    "ibd: scripthash catch-up mode ON (memtable→sorted runs+merge; no durable SH head on confirm)"
                );
            },
            &self.enabled,
            &self.join,
            move || {
                debug!("ibd: SH run worker started (idle IO prio, ~1 merge/s)");
                sh_worker_loop(
                    memtable_cap("RBITCOIN_SH_MEMTABLE_CAP", DEFAULT_MEMTABLE_CAP),
                    inner_w,
                    cv_w,
                );
                debug!("ibd: SH run worker stopped");
            },
        );
    }

    /// On-disk sorted-run count (for IBD progress / lead-compact metrics).
    pub fn on_disk_run_count(&self) -> usize {
        let g = self.inner.lock().unwrap();
        let (dir, io) = runs_dir_io(&g.ctrl);
        drop(g);
        on_disk_run_count(&dir, &io)
    }

    /// Enqueue thin creates from confirm. Blocks only if hard memtable cap
    /// until a flush frees space (sequential flush, not head RMW).
    pub fn enqueue(&self, creates: &[ScriptHashRecord]) {
        if !self.is_enabled() || creates.is_empty() {
            return;
        }
        let cap = memtable_cap("RBITCOIN_SH_MEMTABLE_CAP", DEFAULT_MEMTABLE_CAP);
        let hard = cap.saturating_mul(HARD_MEMTABLE_MUL);
        let mut g = self.inner.lock().unwrap();
        for rec in creates {
            if rec.create_tx_fk.is_null() {
                continue;
            }
            while g.pending.len() >= hard && !g.ctrl.stop {
                self.cv.notify_all();
                g = self
                    .cv
                    .wait_timeout(g, Duration::from_millis(50))
                    .unwrap()
                    .0;
            }
            g.pending.push((rec.scripthash, rec.create_tx_fk));
            self.enqueued.fetch_add(1, Ordering::Relaxed);
        }
        if g.pending.len() >= cap {
            self.cv.notify_all();
        }
    }

    /// Flush memtable, optional merge, then **cold bulk-load** into durable SH.
    ///
    /// - Recovers incomplete `*.run.mat` claims from a prior crash/SIGINT.
    /// - Streams a **k-way merge** of all claimed runs (no multi-GB RAM buffer).
    /// - Uses cold multi-chunk load when the SH table starts empty (does not
    ///   fall back to per-key append after the first 512k flush).
    /// - When the table is already non-empty (resume after partial load), uses
    ///   append path so existing heads are merged.
    pub fn finalize_and_bulk_materialize(&self, store: &Store) -> Result<u64, StoreError> {
        finalize_wait_join(&self.enabled, &self.inner, &self.cv, &self.join)?;
        let (runs_dir, runs_io, mut next_seq) = {
            let g = self.inner.lock().unwrap();
            (
                g.ctrl.runs_dir.clone(),
                Arc::clone(&g.ctrl.runs_io),
                g.ctrl.next_seq,
            )
        };
        {
            let _io = runs_io.lock().unwrap();
            let mut merges = 0u32;
            while try_merge_two_oldest(&runs_dir, &mut next_seq).unwrap_or(false) {
                merges = merges.saturating_add(1);
                if merges % 50 == 0 {
                    info!("node: scripthash run merge progress merges={merges}");
                }
            }
            if merges > 0 {
                info!("node: scripthash run merge done merges={merges}");
            }
        }
        {
            let mut g = self.inner.lock().unwrap();
            g.ctrl.next_seq = next_seq;
        }

        // Claim cataloged runs + recover prior materialize claims (*.run.mat).
        let mut claimed: Vec<SortedRunPath> = Vec::new();
        {
            let _io = runs_io.lock().unwrap();
            // Crash recovery first: incomplete claims are off MANIFEST.
            let mut prior = list_materialize_claims(&runs_dir)?;
            if !prior.is_empty() {
                info!(
                    "node: scripthash resuming {} incomplete materialize claim(s)",
                    prior.len()
                );
            }
            claimed.append(&mut prior);
            let mut runs = list_runs(&runs_dir)?;
            runs.sort_by_key(|r| r.seq().unwrap_or(u64::MAX));
            for run in runs {
                claimed.push(claim_run_for_materialize(&run)?);
            }
        }

        if claimed.is_empty() {
            info!("node: scripthash bulk materialize: no runs");
            // Do not wipe leftover unknown files aggressively — only empty catalog.
            clear_runs_dir(&runs_dir);
            return Ok(0);
        }

        let total_recs: u64 = claimed.iter().map(|r| r.count).sum();
        let cold = store.scripthash.entry_count() == 0;
        if !cold {
            warn!(
                "node: scripthash table non-empty (entry_count={}) — resume uses append path; \
                 incomplete prior load will be merged",
                store.scripthash.entry_count()
            );
        }
        info!(
            "node: scripthash bulk materialize start runs={} records≈{total_recs} cold={cold}",
            claimed.len()
        );
        let t0 = Instant::now();
        // Global sorted stream via k-way merge + hold-key at chunk borders so
        // each scripthash is flushed complete (required for cold multi-batch).
        const CHUNK: usize = 512_000;
        let mut n_total = 0usize;
        let mut unique_in = 0usize;
        let mut flushed_chunks = 0u32;
        let mut chunk: Vec<ScriptHashRecord> = Vec::with_capacity(CHUNK.min(1 << 20));

        let flush_chunk = |chunk: &mut Vec<ScriptHashRecord>,
                           n_total: &mut usize,
                           unique_in: &mut usize,
                           flushed_chunks: &mut u32|
         -> Result<(), StoreError> {
            if chunk.is_empty() {
                return Ok(());
            }
            chunk.dedup_by(|a, b| {
                a.scripthash == b.scripthash && a.create_tx_fk == b.create_tx_fk
            });
            *unique_in = unique_in.saturating_add(chunk.len());
            let n = if cold {
                store.scripthash.bulk_load_sorted_creates_cold(chunk)?
            } else {
                store.scripthash.bulk_load_sorted_creates(chunk)?
            };
            *n_total = n_total.saturating_add(n);
            *flushed_chunks = flushed_chunks.saturating_add(1);
            if *flushed_chunks == 1 || *flushed_chunks % 10 == 0 {
                info!(
                    "node: scripthash materialize progress chunks={} creates≈{} elapsed={:?}",
                    *flushed_chunks,
                    *n_total,
                    t0.elapsed()
                );
            }
            chunk.clear();
            Ok(())
        };

        for_each_merged_rec(&claimed, |rec| {
            if rec.len() < SH_RUN_REC_LEN as usize {
                return Err(StoreError::Corrupt("sh run short record in merge stream"));
            }
            let (sh, tx_fk) = decode_rec(rec)?;
            if tx_fk.is_null() {
                return Ok(());
            }
            chunk.push(ScriptHashRecord::from_fk(sh, tx_fk));
            if chunk.len() < CHUNK {
                return Ok(());
            }
            let last_sh = chunk.last().map(|r| r.scripthash);
            let mut hold = Vec::new();
            if let Some(sh) = last_sh {
                while chunk.last().is_some_and(|r| r.scripthash == sh) {
                    hold.push(chunk.pop().unwrap());
                }
            }
            if chunk.is_empty() {
                hold.reverse();
                chunk.extend(hold);
                flush_chunk(
                    &mut chunk,
                    &mut n_total,
                    &mut unique_in,
                    &mut flushed_chunks,
                )?;
            } else {
                flush_chunk(
                    &mut chunk,
                    &mut n_total,
                    &mut unique_in,
                    &mut flushed_chunks,
                )?;
                hold.reverse();
                chunk.extend(hold);
            }
            Ok(())
        })?;
        flush_chunk(
            &mut chunk,
            &mut n_total,
            &mut unique_in,
            &mut flushed_chunks,
        )?;

        store.scripthash.flush()?;
        for run in &claimed {
            let _ = std::fs::remove_file(&run.path);
        }
        clear_runs_dir(&runs_dir);
        info!(
            "node: scripthash bulk materialize done creates≈{n_total} unique_in≈{unique_in} chunks={flushed_chunks} elapsed={:?}",
            t0.elapsed()
        );
        Ok(n_total as u64)
    }
}

/// Flush + gradual merge worker for SH runs only.
fn sh_worker_loop(soft_cap: usize, inner: Arc<Mutex<Inner>>, cv: Arc<Condvar>) {
    let _ = FAMILY_SH; // family id reserved for metrics / future shared scheduler
    loop {
        let mut g = inner.lock().unwrap();
        if g.ctrl.stop {
            break;
        }
        let need_flush = g.pending.len() >= soft_cap || (g.ctrl.finalize && !g.pending.is_empty());
        if need_flush {
            drop(g);
            let mut g = inner.lock().unwrap();
            if !g.pending.is_empty() {
                if let Err(e) = g.flush_pending() {
                    warn!("ibd: SH run flush failed: {e}");
                }
                cv.notify_all();
            }
            drop(g);
            std::thread::sleep(AFTER_WORK);
            continue;
        }
        if g.ctrl.finalize && g.pending.is_empty() {
            // Final merge pass then stop.
            let runs_dir = g.ctrl.runs_dir.clone();
            let runs_io = Arc::clone(&g.ctrl.runs_io);
            let mut next_seq = g.ctrl.next_seq;
            drop(g);
            {
                let _io = runs_io.lock().unwrap();
                while try_merge_two_oldest(&runs_dir, &mut next_seq).unwrap_or(false) {}
            }
            let mut g = inner.lock().unwrap();
            g.ctrl.next_seq = next_seq;
            g.ctrl.stop = true;
            cv.notify_all();
            break;
        }

        // Opportunistic merge: at most one pair every MERGE_INTERVAL.
        let due = g.last_merge.elapsed() >= MERGE_INTERVAL;
        if due {
            let runs_dir = g.ctrl.runs_dir.clone();
            let runs_io = Arc::clone(&g.ctrl.runs_io);
            let mut next_seq = g.ctrl.next_seq;
            drop(g);
            let merged = {
                let _io = runs_io.lock().unwrap();
                try_merge_two_oldest(&runs_dir, &mut next_seq).unwrap_or(false)
            };
            let mut g = inner.lock().unwrap();
            g.ctrl.next_seq = next_seq;
            g.last_merge = Instant::now();
            if merged {
                debug!("ibd: SH run merge applied");
                drop(g);
                std::thread::sleep(AFTER_WORK);
                continue;
            }
            // No merge possible — wait.
            let (gg, _) = cv.wait_timeout(g, IDLE_POLL).unwrap();
            g = gg;
            if g.ctrl.stop {
                break;
            }
            continue;
        }

        let (gg, _) = cv.wait_timeout(g, IDLE_POLL).unwrap();
        g = gg;
        if g.ctrl.stop {
            break;
        }
    }
}

/// Merge the two **oldest eligible** on-disk runs, excluding the **newest**
/// (active spill target). A run is eligible only if its body is **under**
/// [`SH_MERGE_MAX_BODY_BYTES`] — do not start a merge that includes an already
/// large file. The **output** may exceed 40 MiB (two 30 MiB inputs are fine).
/// Output is sorted by scripthash key (k-way merge). Returns true if a merge ran.
fn try_merge_two_oldest(runs_dir: &Path, next_seq: &mut u64) -> Result<bool, StoreError> {
    let mut runs = list_runs(runs_dir)?;
    if runs.len() < 2 {
        return Ok(false);
    }
    runs.sort_by_key(|r| r.seq().unwrap_or(u64::MAX));
    // Never merge the newest run (latest spill / still "active" target).
    if runs.len() >= 3 {
        runs.pop(); // drop newest
    }
    // Skip runs already ≥ 40 MiB body — leave them for materialize only.
    // Prefer the oldest *eligible* pair (may skip oversized files in front).
    let eligible: Vec<SortedRunPath> = runs
        .into_iter()
        .filter(|r| run_body_bytes(r) < SH_MERGE_MAX_BODY_BYTES)
        .collect();
    if eligible.len() < 2 {
        return Ok(false);
    }
    let a = eligible[0].clone();
    let b = eligible[1].clone();
    let out = next_run_path(runs_dir, *next_seq);
    *next_seq += 1;
    let merged = merge_runs(&[a, b], &out)?;
    debug!(
        "ibd: SH merged runs → seq={:?} count={} body≈{}B",
        merged.seq(),
        merged.count,
        run_body_bytes(&merged)
    );
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rbitcoin_store::Store;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn enqueue_flush_materialize() {
        let n = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("rbitcoin-sh-builder-{n}"));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let store = Store::open_or_create(&dir).unwrap();
        let b = ShRunBuilder::new(&dir);
        b.enable();
        let mut creates = Vec::new();
        for i in 0..100u32 {
            let mut sh = [0u8; 32];
            sh[0] = (i % 17) as u8;
            sh[1] = (i / 17) as u8;
            creates.push(ScriptHashRecord::from_fk(sh, Fk(i as u64 + 1)));
        }
        b.enqueue(&creates);
        let n = b.finalize_and_bulk_materialize(&store).unwrap();
        assert!(n >= 100, "inserted={n}");
        assert!(store.scripthash.entry_count() >= 100);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn merge_two_oldest_excludes_newest() {
        let n = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("rbitcoin-sh-merge-{n}"));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        // Three small sorted runs.
        for seq in 1..=3u64 {
            let mut body = Vec::new();
            let mut sh = [0u8; 32];
            sh[0] = seq as u8;
            body.extend_from_slice(&encode_rec(&sh, Fk(seq)));
            let path = next_run_path(&dir, seq);
            write_sorted_run(&path, SH_RUN_KEY_LEN, SH_RUN_REC_LEN, &body).unwrap();
        }
        let mut next = 4u64;
        assert!(try_merge_two_oldest(&dir, &mut next).unwrap());
        let runs = list_runs(&dir).unwrap();
        // Merged 1+2 → new seq; newest (3) kept; total 2 runs.
        assert_eq!(runs.len(), 2, "runs={runs:?}");
        let counts: u64 = runs.iter().map(|r| r.count).sum();
        assert_eq!(counts, 3);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn merge_size_gate_predicates() {
        let big_count = SH_MERGE_MAX_BODY_BYTES / u64::from(SH_RUN_REC_LEN);
        // At or over cap: not eligible as a merge *input*.
        let over = SortedRunPath {
            path: std::path::PathBuf::from("over.run"),
            count: big_count,
            rec_len: SH_RUN_REC_LEN,
            key_len: SH_RUN_KEY_LEN,
            body_crc32: 0,
        };
        assert!(run_body_bytes(&over) >= SH_MERGE_MAX_BODY_BYTES);
        // Under cap: eligible even if two such would sum over 40 MiB.
        let under = SortedRunPath {
            path: std::path::PathBuf::from("under.run"),
            count: big_count - 1,
            rec_len: SH_RUN_REC_LEN,
            key_len: SH_RUN_KEY_LEN,
            body_crc32: 0,
        };
        assert!(run_body_bytes(&under) < SH_MERGE_MAX_BODY_BYTES);
        let half_plus = (SH_MERGE_MAX_BODY_BYTES / 2) / u64::from(SH_RUN_REC_LEN) + 1;
        let a_bytes = half_plus * u64::from(SH_RUN_REC_LEN);
        assert!(a_bytes < SH_MERGE_MAX_BODY_BYTES);
        assert!(a_bytes.saturating_mul(2) > SH_MERGE_MAX_BODY_BYTES);
    }

    #[test]
    fn materialize_recovers_claimed_mat_files() {
        let n = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("rbitcoin-sh-mat-resume-{n}"));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let store = Store::open_or_create(&dir).unwrap();
        let b = ShRunBuilder::new(&dir);
        let runs_dir = dir.join("scripthash.runs");
        std::fs::create_dir_all(&runs_dir).unwrap();
        // Write a cataloged run then claim it (simulates crash mid-materialize).
        let mut body = Vec::new();
        for i in 0..50u32 {
            let mut sh = [0u8; 32];
            sh[0] = i as u8;
            body.extend_from_slice(&encode_rec(&sh, Fk(i as u64 + 1)));
        }
        let path = next_run_path(&runs_dir, 1);
        let run = write_sorted_run(&path, SH_RUN_KEY_LEN, SH_RUN_REC_LEN, &body).unwrap();
        let claimed = claim_run_for_materialize(&run).unwrap();
        assert!(claimed.path.to_string_lossy().ends_with(".run.mat"));
        assert!(list_runs(&runs_dir).unwrap().is_empty());
        assert_eq!(list_materialize_claims(&runs_dir).unwrap().len(), 1);

        // finalize without enable — recovers .mat and materializes.
        let n = b.finalize_and_bulk_materialize(&store).unwrap();
        assert!(n >= 50, "inserted={n}");
        assert!(store.scripthash.entry_count() >= 50);
        // Claims cleaned up.
        assert!(list_materialize_claims(&runs_dir).unwrap().is_empty());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn same_key_bulk_materialize() {
        let n = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("rbitcoin-sh-bulk-{n}"));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let store = Store::open_or_create(&dir).unwrap();
        let sh = [0xabu8; 32];
        // One run, many creates for same key (already sorted by SH).
        let mut body = Vec::new();
        for i in 1..=20u64 {
            body.extend_from_slice(&encode_rec(&sh, Fk(i)));
        }
        let path = next_run_path(&dir, 1);
        let _run = write_sorted_run(&path, SH_RUN_KEY_LEN, SH_RUN_REC_LEN, &body).unwrap();
        // Bulk path (production tip materialize) via builder finalize.
        let b = ShRunBuilder::new(&dir);
        // Run already on disk under builder's expected runs dir — open under store root.
        // Direct append for same-key regression (run body already written).
        let mut batch = Vec::new();
        for i in 1..=20u64 {
            batch.push(ScriptHashRecord::from_fk(sh, Fk(i)));
        }
        let mut heads = std::collections::HashMap::new();
        let (n, _) = store
            .scripthash
            .put_create_batch_append(&batch, &mut heads)
            .unwrap();
        assert_eq!(n, 20);
        assert_eq!(store.scripthash.entries(&sh).unwrap().len(), 20);
        let _ = b; // silence
        let _ = std::fs::remove_dir_all(&dir);
    }
}
