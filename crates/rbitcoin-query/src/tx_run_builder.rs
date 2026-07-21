//! Catch-up `tx.head` via sorted runs (txid → fk). Confirm uses process cache +
//! durable head is materialized at tip mode (catch-up parent resolve uses light UTXO).

use super::run_builder_core::{
    claim_oldest_run, finalize_materialize_all, memtable_cap, on_disk_run_count, runs_dir_io,
    spawn_worker, worker_loop, RunControl, RunMemtable, FAMILY_TX,
};
use rbitcoin_log::{debug, info};
use rbitcoin_primitives::Fk;
use rbitcoin_store::{
    list_runs, lookup_key, next_run_path, read_run_body, write_sorted_run, Store, StoreError,
    SortedRunPath,
};
use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;

const REC_LEN: u32 = 40; // txid32 + fk8
const KEY_LEN: u32 = 32;
const DEFAULT_CAP: usize = 512_000;

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
    ctrl: RunControl,
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
        self.pending_map.clear();
        recs.sort_unstable_by(|a, b| a.0.cmp(&b.0));
        let mut body = Vec::with_capacity(recs.len() * REC_LEN as usize);
        for (txid, fk) in &recs {
            body.extend_from_slice(&encode(txid, *fk));
        }
        let path = next_run_path(&self.ctrl.runs_dir, self.ctrl.next_seq);
        self.ctrl.next_seq += 1;
        let _io = self.ctrl.runs_io.lock().unwrap();
        write_sorted_run(&path, KEY_LEN, REC_LEN, &body)?;
        Ok(recs.len() as u64)
    }
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
        let ctrl = RunControl::open(store_dir, "tx.runs");
        Self {
            inner: Arc::new(Mutex::new(Inner {
                pending: Vec::new(),
                pending_map: HashMap::new(),
                ctrl,
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
        {
            let mut g = self.inner.lock().unwrap();
            g.ctrl.reset_for_enable();
        }
        let inner_w = Arc::clone(&self.inner);
        let cv_w = Arc::clone(&self.cv);
        spawn_worker(
            "ibd-tx-index",
            || info!("ibd: tx.head catch-up via sorted runs (process cache for confirm)"),
            &self.enabled,
            &self.join,
            move || {
                debug!("ibd: tx run worker started");
                worker_loop(
                    memtable_cap("RBITCOIN_TX_MEMTABLE_CAP", DEFAULT_CAP),
                    "tx",
                    FAMILY_TX,
                    inner_w,
                    cv_w,
                );
                debug!("ibd: tx run worker stopped");
            },
        );
    }

    pub fn enqueue(&self, txid: [u8; 32], fk: Fk) {
        if !self.is_enabled() || fk.is_null() {
            return;
        }
        let soft = memtable_cap("RBITCOIN_TX_MEMTABLE_CAP", DEFAULT_CAP);
        let hard = soft.saturating_mul(2);
        let mut g = self.inner.lock().unwrap();
        while g.pending.len() >= hard && !g.ctrl.stop {
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
    pub fn lookup(&self, txid: &[u8; 32]) -> Result<Option<Fk>, StoreError> {
        if !self.is_enabled() {
            return Ok(None);
        }
        let (runs_dir, runs_io) = {
            let g = self.inner.lock().unwrap();
            if let Some(&fk) = g.pending_map.get(txid) {
                return Ok(Some(fk));
            }
            (
                g.ctrl.runs_dir.clone(),
                std::sync::Arc::clone(&g.ctrl.runs_io),
            )
        };
        // Hold runs_io for list+probe so compact cannot delete mid-lookup.
        let _io = runs_io.lock().unwrap();
        let mut runs = list_runs(&runs_dir)?;
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

    /// On-disk sorted-run count (for IBD progress / lead-compact metrics).
    pub fn on_disk_run_count(&self) -> usize {
        let g = self.inner.lock().unwrap();
        let (dir, io) = runs_dir_io(&g.ctrl);
        drop(g);
        on_disk_run_count(&dir, &io)
    }

    /// Materialize the oldest on-disk run into `tx.head` (paced). `Ok(None)` if empty.
    pub fn materialize_oldest_run(&self, store: &Store) -> Result<Option<u64>, StoreError> {
        let (runs_dir, runs_io) = {
            let g = self.inner.lock().unwrap();
            runs_dir_io(&g.ctrl)
        };
        let Some(run) = claim_oldest_run(&runs_dir, &runs_io)? else {
            return Ok(None);
        };
        let t0 = std::time::Instant::now();
        let n = materialize(store, &run)?;
        let _ = std::fs::remove_file(&run.path);
        let elapsed = t0.elapsed();
        rbitcoin_log::debug!(
            "ibd: materialize store=tx.head keys≈{n} count={} elapsed={elapsed:?}",
            run.count
        );
        Ok(Some(n))
    }

    pub fn finalize_and_materialize(&self, store: &Store) -> Result<u64, StoreError> {
        finalize_materialize_all(
            &self.enabled,
            &self.inner,
            &self.cv,
            &self.join,
            || self.materialize_oldest_run(store),
            "tx.head",
        )
    }
}

fn materialize(store: &Store, run: &SortedRunPath) -> Result<u64, StoreError> {
    // Whole-run decode → one pre-size → paced insert. Cold empty shards bulk-fill
    // in RAM + one sequential write (see HashHead::bulk_fill_empty); avoids the
    // old 8k-batch rehash cascade while heads are not yet used for reads.
    let body = read_run_body(run)?;
    let rec_len = run.rec_len as usize;
    let n_rec = body.len() / rec_len.max(1);
    let mut batch: Vec<([u8; 32], Fk)> = Vec::with_capacity(n_rec);
    let mut off = 0usize;
    while off + rec_len <= body.len() {
        let mut txid = [0u8; 32];
        txid.copy_from_slice(&body[off..off + 32]);
        let fk = Fk(u64::from_le_bytes(body[off + 32..off + 40].try_into().unwrap()));
        off += rec_len;
        if !fk.is_null() {
            batch.push((txid, fk));
        }
    }
    if batch.is_empty() {
        return Ok(0);
    }
    store.txs.head_reserve_additional(batch.len() as u64)?;
    let n = batch.len() as u64;
    store.txs.head_insert_many(&batch)?;
    Ok(n)
}
