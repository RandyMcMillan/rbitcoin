//! Catch-up point (spend) edges via sorted runs. Confirm uses
//! light UTXO spentness; durable multimap is materialized at tip mode.

use super::run_builder_core::{
    clear_runs_dir, compact_all_to_one, finalize_wait_join, memtable_cap, spawn_worker, worker_loop,
    RunControl, RunMemtable, FAMILY_POINT,
};
use rbitcoin_log::{debug, info};
use rbitcoin_primitives::Fk;
use rbitcoin_store::{
    next_run_path, read_run_body, write_sorted_run, PointRecord, Store, StoreError, SortedRunPath,
};
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;

/// on-disk: outpoint_key[32] | out_txid[32] | out_index | spend_fk | in_idx | height = 84
const KEY_LEN: u32 = 32;
const FULL: u32 = 84;
const DEFAULT_CAP: usize = 512_000;

/// (out_txid, out_index, spend_tx_fk, in_idx, height)
pub type PointEdge = ([u8; 32], u32, Fk, u32, u32);

fn sort_key(out_txid: &[u8; 32], out_index: u32) -> [u8; 32] {
    PointRecord::outpoint_key(out_txid, out_index)
}

struct Inner {
    pending: Vec<PointEdge>,
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
        recs.sort_unstable_by(|a, b| {
            sort_key(&a.0, a.1)
                .cmp(&sort_key(&b.0, b.1))
                .then(a.2 .0.cmp(&b.2 .0))
                .then(a.3.cmp(&b.3))
        });
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
        let path = next_run_path(&self.ctrl.runs_dir, self.ctrl.next_seq);
        self.ctrl.next_seq += 1;
        let _io = self.ctrl.runs_io.lock().unwrap();
        write_sorted_run(&path, KEY_LEN, FULL, &body)?;
        Ok(recs.len() as u64)
    }
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
        let ctrl = RunControl::open(store_dir, "point.runs");
        Self {
            inner: Arc::new(Mutex::new(Inner {
                pending: Vec::new(),
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
            "ibd-point-index",
            || info!("ibd: point.head catch-up via sorted runs (mmap UTXO for confirm spentness)"),
            &self.enabled,
            &self.join,
            move || {
                debug!("ibd: point run worker started");
                worker_loop(
                    memtable_cap("RBITCOIN_POINT_MEMTABLE_CAP", DEFAULT_CAP),
                    "point",
                    FAMILY_POINT,
                    inner_w,
                    cv_w,
                );
                debug!("ibd: point run worker stopped");
            },
        );
    }

    pub fn enqueue_batch(&self, edges: &[PointEdge]) {
        if !self.is_enabled() || edges.is_empty() {
            return;
        }
        let soft = memtable_cap("RBITCOIN_POINT_MEMTABLE_CAP", DEFAULT_CAP);
        let hard = soft.saturating_mul(2);
        let mut g = self.inner.lock().unwrap();
        for &e in edges {
            while g.pending.len() >= hard && !g.ctrl.stop {
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

    /// On-disk sorted-run count (for IBD progress / lead-compact metrics).
    pub fn on_disk_run_count(&self) -> usize {
        let (runs_dir, runs_io) = {
            let g = self.inner.lock().unwrap();
            (g.ctrl.runs_dir.clone(), Arc::clone(&g.ctrl.runs_io))
        };
        let _io = runs_io.lock().unwrap();
        rbitcoin_store::list_runs(&runs_dir)
            .map(|r| r.len())
            .unwrap_or(0)
    }

    pub fn finalize_and_materialize(&self, store: &Store) -> Result<u64, StoreError> {
        finalize_wait_join(&self.enabled, &self.inner, &self.cv, &self.join)?;
        let run = compact_all_to_one(&self.inner)?;
        let mut inserted = 0u64;
        if let Some(ref run) = run {
            info!(
                "node: materializing point spends from run {} ({} edges)…",
                run.path.display(),
                run.count
            );
            inserted = materialize(store, run)?;
            info!("node: point materialize done edges≈{inserted}");
        }
        let runs_dir = self.inner.lock().unwrap().ctrl.runs_dir.clone();
        clear_runs_dir(&runs_dir);
        Ok(inserted)
    }
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
