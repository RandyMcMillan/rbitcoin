//! Catch-up `tx.head` via sorted runs (txid → fk). Confirm uses process cache +
//! durable head is materialized at tip mode (catch-up parent resolve uses light UTXO).

use rbitcoin_log::{debug, info, warn};
use rbitcoin_primitives::Fk;
use rbitcoin_store::{
    list_runs, lookup_key, merge_runs, next_run_path, read_run_body, try_set_io_idle,
    write_sorted_run, Store, StoreError, SortedRunPath,
};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;

const REC_LEN: u32 = 40; // txid32 + fk8
const KEY_LEN: u32 = 32;
const DEFAULT_CAP: usize = 512_000;
const MAX_RUNS: usize = 16;
const AFTER_WORK: Duration = Duration::from_millis(40);
const IDLE_POLL: Duration = Duration::from_millis(100);

fn cap() -> usize {
    std::env::var("RBITCOIN_TX_MEMTABLE_CAP")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(DEFAULT_CAP)
        .max(1_000)
}

fn encode(txid: &[u8; 32], fk: Fk) -> [u8; REC_LEN as usize] {
    let mut r = [0u8; REC_LEN as usize];
    r[0..32].copy_from_slice(txid);
    r[32..40].copy_from_slice(&fk.0.to_le_bytes());
    r
}

struct Inner {
    pending: Vec<([u8; 32], Fk)>,
    /// Sidecar for O(1) lookup of not-yet-flushed entries.
    pending_map: HashMap<[u8; 32], Fk>,
    runs_dir: PathBuf,
    next_seq: u64,
    stop: bool,
    finalize: bool,
}

pub struct TxRunBuilder {
    inner: Arc<Mutex<Inner>>,
    cv: Arc<Condvar>,
    enabled: AtomicBool,
    join: Mutex<Option<JoinHandle<()>>>,
    pub enqueued: AtomicU64,
}

impl TxRunBuilder {
    pub fn new(store_dir: &Path) -> Self {
        let runs_dir = store_dir.join("tx.runs");
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
                pending_map: HashMap::new(),
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

    pub fn enable(&self) {
        if self.enabled.swap(true, Ordering::SeqCst) {
            return;
        }
        let mut jg = self.join.lock().unwrap();
        if jg.is_some() {
            return;
        }
        {
            let mut g = self.inner.lock().unwrap();
            g.stop = false;
            g.finalize = false;
        }
        let inner_w = Arc::clone(&self.inner);
        let cv_w = Arc::clone(&self.cv);
        *jg = Some(
            std::thread::Builder::new()
                .name("ibd-tx-index".into())
                .spawn(move || {
                    try_set_io_idle();
                    debug!("ibd: tx run worker started");
                    worker_loop(inner_w, cv_w);
                    debug!("ibd: tx run worker stopped");
                })
                .expect("spawn ibd-tx-index"),
        );
        info!("ibd: tx.head catch-up via sorted runs (process cache for confirm)");
    }

    pub fn enqueue(&self, txid: [u8; 32], fk: Fk) {
        if !self.is_enabled() || fk.is_null() {
            return;
        }
        let soft = cap();
        let hard = soft.saturating_mul(2);
        let mut g = self.inner.lock().unwrap();
        while g.pending.len() >= hard && !g.stop {
            self.cv.notify_all();
            g = self
                .cv
                .wait_timeout(g, Duration::from_millis(50))
                .unwrap()
                .0;
        }
        g.pending.push((txid, fk));
        g.pending_map.insert(txid, fk);
        self.enqueued.fetch_add(1, Ordering::Relaxed);
        if g.pending.len() >= soft {
            self.cv.notify_all();
        }
    }

    /// Resolve txid → fk from pending memtable + on-disk sorted runs.
    ///
    /// Newest run wins if duplicates exist. Used when the process txid cache misses.
    pub fn lookup(&self, txid: &[u8; 32]) -> Result<Option<Fk>, StoreError> {
        if !self.is_enabled() {
            return Ok(None);
        }
        let g = self.inner.lock().unwrap();
        if let Some(&fk) = g.pending_map.get(txid) {
            return Ok(Some(fk));
        }
        let runs_dir = g.runs_dir.clone();
        drop(g);
        let mut runs = list_runs(&runs_dir)?;
        // Prefer later runs (higher seq / path sort) for last-wins.
        runs.sort_by(|a, b| b.path.cmp(&a.path));
        for run in &runs {
            if let Some(rec) = lookup_key(run, txid)? {
                if rec.len() >= REC_LEN as usize {
                    let fk = Fk(u64::from_le_bytes(rec[32..40].try_into().unwrap()));
                    if !fk.is_null() {
                        return Ok(Some(fk));
                    }
                }
            }
        }
        Ok(None)
    }

    pub fn finalize_and_materialize(&self, store: &Store) -> Result<u64, StoreError> {
        self.enabled.store(false, Ordering::SeqCst);
        {
            let mut g = self.inner.lock().unwrap();
            g.finalize = true;
            self.cv.notify_all();
        }
        for _ in 0..6000 {
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
        {
            let mut g = self.inner.lock().unwrap();
            if !g.pending.is_empty() {
                flush_pending(&mut g)?;
            }
        }
        let runs_dir = self.inner.lock().unwrap().runs_dir.clone();
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
            runs.push(merge_runs(&batch, &out)?);
            runs.sort_by(|a, b| a.path.cmp(&b.path));
        }
        let mut inserted = 0u64;
        if let Some(run) = runs.first() {
            info!(
                "node: materializing tx.head from run {} ({} entries)…",
                run.path.display(),
                run.count
            );
            inserted = materialize(store, run)?;
            info!("node: tx.head materialize done inserted≈{inserted}");
        }
        if let Ok(rd) = std::fs::read_dir(&runs_dir) {
            for e in rd.flatten() {
                let _ = std::fs::remove_file(e.path());
            }
        }
        Ok(inserted)
    }
}

fn worker_loop(inner: Arc<Mutex<Inner>>, cv: Arc<Condvar>) {
    let soft = cap();
    loop {
        let mut g = inner.lock().unwrap();
        if g.stop {
            break;
        }
        let need_flush = g.pending.len() >= soft || (g.finalize && !g.pending.is_empty());
        let runs_dir = g.runs_dir.clone();
        let run_count = list_runs(&runs_dir).map(|r| r.len()).unwrap_or(0);
        let need_merge =
            run_count >= MAX_RUNS || (g.finalize && run_count > 1 && !need_flush);
        if !need_flush && !need_merge {
            if g.finalize && g.pending.is_empty() {
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
            if let Err(e) = flush_pending(&mut g) {
                warn!("ibd: tx run flush failed: {e}");
            }
            cv.notify_all();
            drop(g);
            std::thread::sleep(AFTER_WORK);
        }
        if need_merge {
            let g = inner.lock().unwrap();
            let runs_dir = g.runs_dir.clone();
            let mut seq = g.next_seq;
            drop(g);
            let mut runs = list_runs(&runs_dir).unwrap_or_default();
            if runs.len() >= 2 {
                runs.sort_by_key(|r| r.count);
                let n = runs.len().min(4).max(2);
                let batch: Vec<SortedRunPath> = runs.drain(..n).collect();
                let out = next_run_path(&runs_dir, seq);
                seq += 1;
                match merge_runs(&batch, &out) {
                    Ok(_) => {
                        let mut g = inner.lock().unwrap();
                        g.next_seq = g.next_seq.max(seq);
                    }
                    Err(e) => warn!("ibd: tx run compact failed: {e}"),
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
    g.pending_map.clear();
    recs.sort_unstable_by(|a, b| a.0.cmp(&b.0));
    let mut body = Vec::with_capacity(recs.len() * REC_LEN as usize);
    for (txid, fk) in &recs {
        body.extend_from_slice(&encode(txid, *fk));
    }
    let path = next_run_path(&g.runs_dir, g.next_seq);
    g.next_seq += 1;
    write_sorted_run(&path, KEY_LEN, REC_LEN, &body)?;
    Ok(recs.len() as u64)
}

fn materialize(store: &Store, run: &SortedRunPath) -> Result<u64, StoreError> {
    let body = read_run_body(run)?;
    let rec_len = run.rec_len as usize;
    let mut batch: Vec<([u8; 32], Fk)> = Vec::with_capacity(8192);
    let mut inserted = 0u64;
    let mut off = 0usize;
    while off + rec_len <= body.len() {
        let mut txid = [0u8; 32];
        txid.copy_from_slice(&body[off..off + 32]);
        let fk = Fk(u64::from_le_bytes(body[off + 32..off + 40].try_into().unwrap()));
        off += rec_len;
        if !fk.is_null() {
            batch.push((txid, fk));
        }
        if batch.len() >= 8192 {
            store.txs.head_insert_many(&batch)?;
            inserted += batch.len() as u64;
            batch.clear();
        }
    }
    if !batch.is_empty() {
        store.txs.head_insert_many(&batch)?;
        inserted += batch.len() as u64;
    }
    Ok(inserted)
}
