//! Catch-up point (spend) edges via sorted runs. Confirm uses complete
//! light UTXO / process-local spentness; durable multimap is materialized at tip mode.

use rbitcoin_log::{debug, info, warn};
use rbitcoin_primitives::Fk;
use rbitcoin_store::{
    list_runs, merge_runs, next_run_path, read_run_body, try_set_io_idle, write_sorted_run,
    PointRecord, Store, StoreError, SortedRunPath,
};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;

/// on-disk: outpoint_key[32] | out_txid[32] | out_index | spend_fk | in_idx | height = 84
const KEY_LEN: u32 = 32;
const DEFAULT_CAP: usize = 512_000;
const MAX_RUNS: usize = 16;
const AFTER_WORK: Duration = Duration::from_millis(40);
const IDLE_POLL: Duration = Duration::from_millis(100);

fn cap() -> usize {
    std::env::var("RBITCOIN_POINT_MEMTABLE_CAP")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(DEFAULT_CAP)
        .max(1_000)
}

/// (out_txid, out_index, spend_tx_fk, in_idx, height)
pub type PointEdge = ([u8; 32], u32, Fk, u32, u32);

fn sort_key(out_txid: &[u8; 32], out_index: u32) -> [u8; 32] {
    PointRecord::outpoint_key(out_txid, out_index)
}

struct Inner {
    pending: Vec<PointEdge>,
    runs_dir: PathBuf,
    next_seq: u64,
    stop: bool,
    finalize: bool,
}

pub struct PointRunBuilder {
    inner: Arc<Mutex<Inner>>,
    cv: Arc<Condvar>,
    enabled: AtomicBool,
    join: Mutex<Option<JoinHandle<()>>>,
    pub enqueued: AtomicU64,
}

impl PointRunBuilder {
    pub fn new(store_dir: &Path) -> Self {
        let runs_dir = store_dir.join("point.runs");
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
                .name("ibd-point-index".into())
                .spawn(move || {
                    try_set_io_idle();
                    debug!("ibd: point run worker started");
                    worker_loop(inner_w, cv_w);
                    debug!("ibd: point run worker stopped");
                })
                .expect("spawn ibd-point-index"),
        );
        info!("ibd: point.head catch-up via sorted runs (mmap UTXO for confirm spentness)");
    }

    pub fn enqueue_batch(&self, edges: &[PointEdge]) {
        if !self.is_enabled() || edges.is_empty() {
            return;
        }
        let soft = cap();
        let hard = soft.saturating_mul(2);
        let mut g = self.inner.lock().unwrap();
        for &e in edges {
            while g.pending.len() >= hard && !g.stop {
                self.cv.notify_all();
                g = self
                    .cv
                    .wait_timeout(g, Duration::from_millis(50))
                    .unwrap()
                    .0;
            }
            g.pending.push(e);
            self.enqueued.fetch_add(1, Ordering::Relaxed);
        }
        if g.pending.len() >= soft {
            self.cv.notify_all();
        }
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
                "node: materializing point spends from run {} ({} edges)…",
                run.path.display(),
                run.count
            );
            inserted = materialize(store, run)?;
            info!("node: point materialize done edges≈{inserted}");
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
                warn!("ibd: point run flush failed: {e}");
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
                    Err(e) => warn!("ibd: point run compact failed: {e}"),
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
    recs.sort_unstable_by(|a, b| {
        sort_key(&a.0, a.1)
            .cmp(&sort_key(&b.0, b.1))
            .then(a.2 .0.cmp(&b.2 .0))
            .then(a.3.cmp(&b.3))
    });
    // Write with key_len=32: first 32 bytes of each record must be sort key.
    // Layout: outpoint_key[32] | out_txid[32] | out_index|spend|in|height → 84 bytes.
    // Simpler: use key_len=0? not allowed. Use REC_LEN=84.
    const FULL: u32 = 84;
    let mut body = Vec::with_capacity(recs.len() * FULL as usize);
    for (out_txid, out_idx, spend_fk, in_idx, height) in &recs {
        let key = sort_key(out_txid, *out_idx);
        body.extend_from_slice(&key);
        body.extend_from_slice(out_txid);
        body.extend_from_slice(&out_idx.to_le_bytes());
        body.extend_from_slice(&spend_fk.0.to_le_bytes());
        body.extend_from_slice(&in_idx.to_le_bytes());
        body.extend_from_slice(&height.to_le_bytes());
    }
    let path = next_run_path(&g.runs_dir, g.next_seq);
    g.next_seq += 1;
    write_sorted_run(&path, KEY_LEN, FULL, &body)?;
    Ok(recs.len() as u64)
}

fn materialize(store: &Store, run: &SortedRunPath) -> Result<u64, StoreError> {
    let body = read_run_body(run)?;
    let rec_len = run.rec_len as usize;
    if rec_len != 84 {
        return Err(StoreError::Corrupt("point run unexpected rec_len"));
    }
    let mut batch: Vec<([u8; 32], u32, Fk, u32)> = Vec::with_capacity(8192);
    let mut inserted = 0u64;
    let mut off = 0usize;
    while off + rec_len <= body.len() {
        // key 0..32, out_txid 32..64, out_index 64..68, spend 68..76, in 76..80, h 80..84
        let mut out_txid = [0u8; 32];
        out_txid.copy_from_slice(&body[off + 32..off + 64]);
        let out_index = u32::from_le_bytes(body[off + 64..off + 68].try_into().unwrap());
        let spend_fk = Fk(u64::from_le_bytes(body[off + 68..off + 76].try_into().unwrap()));
        let in_idx = u32::from_le_bytes(body[off + 76..off + 80].try_into().unwrap());
        off += rec_len;
        if !spend_fk.is_null() {
            batch.push((out_txid, out_index, spend_fk, in_idx));
        }
        if batch.len() >= 8192 {
            store.put_spend_batch(&batch)?;
            inserted += batch.len() as u64;
            batch.clear();
        }
    }
    if !batch.is_empty() {
        store.put_spend_batch(&batch)?;
        inserted += batch.len() as u64;
    }
    Ok(inserted)
}
