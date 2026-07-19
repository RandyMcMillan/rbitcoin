//! Catch-up scripthash builder: memtable → sorted runs → low-prio compact →
//! materialize into durable `scripthash` tables at tip mode.
//!
//! Confirm only enqueues thin creates (no open-hash RMW). A background worker
//! flushes and merges at idle IO priority **while confirm is live**.

use rbitcoin_log::{debug, info, warn};
use rbitcoin_primitives::Fk;
use rbitcoin_store::{
    list_runs, merge_runs, next_run_path, read_run_body, try_set_io_idle, write_sorted_run,
    ScriptHashRecord, Store, StoreError, SortedRunPath,
};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;

/// Fixed run record: scripthash[32] | create_tx_fk:u64 | vout:u32 = 44 bytes.
pub const SH_RUN_REC_LEN: u32 = 44;
pub const SH_RUN_KEY_LEN: u32 = 32;

/// Default memtable soft cap (creates). Override `RBITCOIN_SH_MEMTABLE_CAP`.
const DEFAULT_MEMTABLE_CAP: usize = 256_000;
/// Force flush at this many creates (hard).
const HARD_MEMTABLE_MUL: usize = 2;
/// Merge when run count exceeds this.
const MAX_RUNS_BEFORE_MERGE: usize = 16;
/// Sleep after productive flush/compact so confirm keeps disk.
const AFTER_WORK: Duration = Duration::from_millis(40);
const IDLE_POLL: Duration = Duration::from_millis(100);

fn memtable_cap() -> usize {
    std::env::var("RBITCOIN_SH_MEMTABLE_CAP")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(DEFAULT_MEMTABLE_CAP)
        .max(1_000)
}

fn encode_rec(sh: &[u8; 32], tx_fk: Fk, vout: u32) -> [u8; SH_RUN_REC_LEN as usize] {
    let mut r = [0u8; SH_RUN_REC_LEN as usize];
    r[0..32].copy_from_slice(sh);
    r[32..40].copy_from_slice(&tx_fk.0.to_le_bytes());
    r[40..44].copy_from_slice(&vout.to_le_bytes());
    r
}

fn decode_rec(buf: &[u8]) -> Result<([u8; 32], Fk, u32), StoreError> {
    if buf.len() < SH_RUN_REC_LEN as usize {
        return Err(StoreError::Corrupt("sh run short record"));
    }
    let mut sh = [0u8; 32];
    sh.copy_from_slice(&buf[0..32]);
    let tx_fk = Fk(u64::from_le_bytes(buf[32..40].try_into().unwrap()));
    let vout = u32::from_le_bytes(buf[40..44].try_into().unwrap());
    Ok((sh, tx_fk, vout))
}

struct Inner {
    pending: Vec<([u8; 32], Fk, u32)>,
    runs_dir: PathBuf,
    next_seq: u64,
    stop: bool,
    finalize: bool,
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
        let runs_dir = store_dir.join("scripthash.runs");
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
            inner: Arc::new(Mutex::new(Inner {
                pending: Vec::new(),
                runs_dir,
                next_seq,
                stop: false,
                finalize: false,
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
        if self.enabled.swap(true, Ordering::SeqCst) {
            return;
        }
        let mut jg = self.join.lock().unwrap();
        if jg.is_some() {
            return;
        }
        // Reset stop/finalize if re-enabled in tests.
        {
            let mut g = self.inner.lock().unwrap();
            g.stop = false;
            g.finalize = false;
        }
        let inner_w = Arc::clone(&self.inner);
        let cv_w = Arc::clone(&self.cv);
        let handle = std::thread::Builder::new()
            .name("ibd-sh-index".into())
            .spawn(move || {
                try_set_io_idle();
                debug!("ibd: SH run worker started (idle IO prio)");
                worker_loop(inner_w, cv_w);
                debug!("ibd: SH run worker stopped");
            })
            .expect("spawn ibd-sh-index");
        *jg = Some(handle);
        info!(
            "ibd: scripthash catch-up mode ON (memtable→sorted runs; no durable SH head on confirm)"
        );
    }

    /// Enqueue thin creates from confirm. Blocks only if hard memtable cap
    /// until a flush frees space (sequential flush, not head RMW).
    pub fn enqueue(&self, creates: &[ScriptHashRecord]) {
        if !self.is_enabled() || creates.is_empty() {
            return;
        }
        let cap = memtable_cap();
        let hard = cap.saturating_mul(HARD_MEMTABLE_MUL);
        let mut g = self.inner.lock().unwrap();
        for rec in creates {
            if rec.create_tx_fk.is_null() {
                continue;
            }
            while g.pending.len() >= hard && !g.stop {
                self.cv.notify_all();
                g = self
                    .cv
                    .wait_timeout(g, Duration::from_millis(50))
                    .unwrap()
                    .0;
            }
            g.pending
                .push((rec.scripthash, rec.create_tx_fk, rec.vout));
            self.enqueued.fetch_add(1, Ordering::Relaxed);
        }
        if g.pending.len() >= cap {
            self.cv.notify_all();
        }
    }

    #[allow(dead_code)] // diagnostics / future perf_log
    pub fn pending_len(&self) -> usize {
        self.inner.lock().unwrap().pending.len()
    }

    fn runs_dir(&self) -> PathBuf {
        self.inner.lock().unwrap().runs_dir.clone()
    }

    /// Stop enqueues, flush + compact remaining, materialize into store, join worker.
    pub fn finalize_and_materialize(&self, store: &Store) -> Result<u64, StoreError> {
        self.enabled.store(false, Ordering::SeqCst);
        {
            let mut g = self.inner.lock().unwrap();
            g.finalize = true;
            self.cv.notify_all();
        }

        // Wait for worker to drain pending and exit.
        for _ in 0..6000 {
            // up to ~60s of 10ms polls; large memtables need flush time
            {
                let g = self.inner.lock().unwrap();
                if g.stop && g.pending.is_empty() {
                    break;
                }
            }
            self.cv.notify_all();
            std::thread::sleep(Duration::from_millis(10));
        }

        if let Some(h) = self.join.lock().unwrap().take() {
            let _ = h.join();
        }

        // Leftover pending (worker race): flush on this thread.
        {
            let mut g = self.inner.lock().unwrap();
            if !g.pending.is_empty() {
                flush_pending(&mut g)?;
            }
        }

        let runs_dir = self.runs_dir();
        let mut runs = list_runs(&runs_dir)?;
        while runs.len() > 1 {
            let n = runs.len().min(8);
            let batch: Vec<SortedRunPath> = runs.drain(..n).collect();
            let seq = {
                let mut g = self.inner.lock().unwrap();
                let s = g.next_seq;
                g.next_seq += 1;
                s
            };
            let out = next_run_path(&runs_dir, seq);
            let merged = merge_runs(&batch, &out)?;
            runs.push(merged);
            runs.sort_by(|a, b| a.path.cmp(&b.path));
        }

        let mut inserted = 0u64;
        if let Some(run) = runs.first() {
            info!(
                "node: materializing scripthash from run {} ({} creates)…",
                run.path.display(),
                run.count
            );
            inserted = materialize_run(store, run)?;
            info!("node: scripthash materialize done inserted≈{inserted}");
        } else {
            info!("node: scripthash run materialize: no runs");
        }

        if let Ok(rd) = std::fs::read_dir(&runs_dir) {
            for e in rd.flatten() {
                let _ = std::fs::remove_file(e.path());
            }
        }
        Ok(inserted)
    }

    /// Disable without materialize (tests / abort).
    #[allow(dead_code)]
    pub fn shutdown(&self) {
        self.enabled.store(false, Ordering::SeqCst);
        {
            let mut g = self.inner.lock().unwrap();
            g.stop = true;
            g.pending.clear();
            self.cv.notify_all();
        }
        if let Some(h) = self.join.lock().unwrap().take() {
            let _ = h.join();
        }
    }
}

fn worker_loop(inner: Arc<Mutex<Inner>>, cv: Arc<Condvar>) {
    let cap = memtable_cap();
    loop {
        let mut g = inner.lock().unwrap();
        if g.stop {
            break;
        }
        let need_flush = g.pending.len() >= cap || (g.finalize && !g.pending.is_empty());
        let runs_dir = g.runs_dir.clone();
        let run_count = list_runs(&runs_dir).map(|r| r.len()).unwrap_or(0);
        let need_merge =
            run_count >= MAX_RUNS_BEFORE_MERGE || (g.finalize && run_count > 1 && !need_flush);

        if !need_flush && !need_merge {
            if g.finalize && g.pending.is_empty() {
                // Leave final multi-run merge to finalize_and_materialize.
                g.stop = true;
                cv.notify_all();
                break;
            }
            let (gg, _) = cv.wait_timeout(g, IDLE_POLL).unwrap();
            g = gg;
            if g.stop {
                break;
            }
            continue;
        }
        drop(g);

        if need_flush {
            let mut g = inner.lock().unwrap();
            if !g.pending.is_empty() {
                match flush_pending(&mut g) {
                    Ok(n) if n > 0 => {
                        debug!("ibd: SH run flush creates={n}");
                    }
                    Ok(_) => {}
                    Err(e) => warn!("ibd: SH run flush failed: {e}"),
                }
                cv.notify_all();
            }
            drop(g);
            std::thread::sleep(AFTER_WORK);
        }

        if need_merge {
            let g = inner.lock().unwrap();
            let runs_dir = g.runs_dir.clone();
            let mut seq = g.next_seq;
            drop(g);
            let runs = list_runs(&runs_dir).unwrap_or_default();
            if runs.len() >= 2 {
                let mut sorted = runs;
                sorted.sort_by_key(|r| r.count);
                let n = sorted.len().min(4).max(2);
                let batch: Vec<SortedRunPath> = sorted.drain(..n).collect();
                let out = next_run_path(&runs_dir, seq);
                seq += 1;
                match merge_runs(&batch, &out) {
                    Ok(m) => {
                        debug!("ibd: SH run compact inputs={n} out_count={}", m.count);
                        let mut g = inner.lock().unwrap();
                        g.next_seq = g.next_seq.max(seq);
                    }
                    Err(e) => warn!("ibd: SH run compact failed: {e}"),
                }
                std::thread::sleep(AFTER_WORK);
            }
        }
    }
}

fn flush_pending(g: &mut Inner) -> Result<u64, StoreError> {
    if g.pending.is_empty() {
        return Ok(0);
    }
    let mut recs = std::mem::take(&mut g.pending);
    recs.sort_unstable_by(|a, b| a.0.cmp(&b.0).then(a.1 .0.cmp(&b.1 .0)).then(a.2.cmp(&b.2)));
    let mut body = Vec::with_capacity(recs.len() * SH_RUN_REC_LEN as usize);
    for (sh, fk, vout) in &recs {
        body.extend_from_slice(&encode_rec(sh, *fk, *vout));
    }
    let path = next_run_path(&g.runs_dir, g.next_seq);
    g.next_seq += 1;
    write_sorted_run(&path, SH_RUN_KEY_LEN, SH_RUN_REC_LEN, &body)?;
    Ok(recs.len() as u64)
}

fn materialize_run(store: &Store, run: &SortedRunPath) -> Result<u64, StoreError> {
    let body = read_run_body(run)?;
    let rec_len = run.rec_len as usize;
    let mut heads: std::collections::HashMap<[u8; 32], Fk> = std::collections::HashMap::new();
    let mut batch: Vec<ScriptHashRecord> = Vec::with_capacity(8192);
    let mut inserted = 0u64;
    let mut offset = 0usize;
    while offset + rec_len <= body.len() {
        let (sh, tx_fk, vout) = decode_rec(&body[offset..offset + rec_len])?;
        offset += rec_len;
        if tx_fk.is_null() {
            continue;
        }
        batch.push(ScriptHashRecord {
            scripthash: sh,
            create_tx_fk: tx_fk,
            vout,
            next: Fk::NULL,
            txid: [0u8; 32],
            value: 0,
            create_height: 0,
        });
        if batch.len() >= 8192 {
            let (fks, _) = store
                .scripthash
                .put_create_batch_append(&batch, &mut heads)?;
            inserted += fks.iter().filter(|f| !f.is_null()).count() as u64;
            batch.clear();
        }
    }
    if !batch.is_empty() {
        let (fks, _) = store
            .scripthash
            .put_create_batch_append(&batch, &mut heads)?;
        inserted += fks.iter().filter(|f| !f.is_null()).count() as u64;
    }
    Ok(inserted)
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
            creates.push(ScriptHashRecord {
                scripthash: sh,
                create_tx_fk: Fk(i as u64 + 1),
                vout: 0,
                next: Fk::NULL,
                txid: [0u8; 32],
                value: 0,
                create_height: 0,
            });
        }
        b.enqueue(&creates);
        let n = b.finalize_and_materialize(&store).unwrap();
        assert!(n >= 100, "inserted={n}");
        assert!(store.scripthash.entry_count() >= 100);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
