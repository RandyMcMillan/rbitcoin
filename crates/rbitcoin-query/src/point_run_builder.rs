//! Catch-up spend annotations via sorted runs (confirm enqueue → materialize).
//!
//! Record: create_tx_fk | vout | spending_tx_fk (20 B). Enqueued at **confirm**
//! when light UTXO supplies create_fk; materialize patches create outputs.

use super::run_builder_core::{
    clear_runs_dir, finalize_wait_join, memtable_cap, spawn_worker, take_oldest_run, worker_loop,
    RunControl, RunMemtable, FAMILY_POINT,
};
use rbitcoin_log::{debug, info};
use rbitcoin_primitives::Fk;
use rbitcoin_store::{
    next_run_path, read_run_body, remove_run, write_sorted_run, Store, StoreError, SortedRunPath,
};
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::JoinHandle;

/// create_tx_fk:u64 | vout:u32 | spending_tx_fk:u64 = 20
const KEY_LEN: u32 = 8;
const FULL: u32 = 20;
const DEFAULT_CAP: usize = 512_000;

/// (create_tx_fk, vout, spending_tx_fk)
pub type SpendEdge = (Fk, u32, Fk);

struct Inner {
    pending: Vec<SpendEdge>,
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
        recs.sort_unstable_by(|a, b| a.0 .0.cmp(&b.0 .0).then(a.1.cmp(&b.1)).then(a.2 .0.cmp(&b.2 .0)));
        let mut body = Vec::with_capacity(recs.len() * FULL as usize);
        for (create_fk, vout, spend_fk) in &recs {
            body.extend_from_slice(&create_fk.0.to_le_bytes());
            body.extend_from_slice(&vout.to_le_bytes());
            body.extend_from_slice(&spend_fk.0.to_le_bytes());
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
        let soft = memtable_cap("RBITCOIN_POINT_MEMTABLE_CAP", DEFAULT_CAP);
        let inner = Arc::clone(&self.inner);
        let cv = Arc::clone(&self.cv);
        spawn_worker(
            "ibd-point-runs",
            || debug!("ibd: spend-run worker on (confirm enqueue → output spender_field)"),
            &self.enabled,
            &self.join,
            move || worker_loop(soft, "point", FAMILY_POINT, inner, cv),
        );
    }

    pub fn enqueue_batch(&self, edges: &[SpendEdge]) {
        if edges.is_empty() || !self.is_enabled() {
            return;
        }
        let mut g = self.inner.lock().unwrap();
        g.pending.extend_from_slice(edges);
        self.enqueued
            .fetch_add(edges.len() as u64, Ordering::Relaxed);
        let soft = memtable_cap("RBITCOIN_POINT_MEMTABLE_CAP", DEFAULT_CAP);
        if g.pending.len() >= soft {
            self.cv.notify_one();
        }
    }

    pub fn on_disk_run_count(&self) -> usize {
        let runs_dir = {
            let g = self.inner.lock().unwrap();
            g.ctrl.runs_dir.clone()
        };
        rbitcoin_store::list_runs(&runs_dir)
            .map(|r| r.len())
            .unwrap_or(0)
    }

    pub fn materialize_oldest_run(&self, store: &Store) -> Result<Option<u64>, StoreError> {
        let (runs_dir, runs_io) = {
            let g = self.inner.lock().unwrap();
            (g.ctrl.runs_dir.clone(), Arc::clone(&g.ctrl.runs_io))
        };
        let Some(run) = take_oldest_run(&runs_dir, &runs_io)? else {
            return Ok(None);
        };
        let n = materialize(store, &run)?;
        {
            let _io = runs_io.lock().unwrap();
            remove_run(&run)?;
        }
        Ok(Some(n))
    }

    pub fn finalize_and_materialize(&self, store: &Store) -> Result<u64, StoreError> {
        finalize_wait_join(&self.enabled, &self.inner, &self.cv, &self.join)?;
        let mut inserted = 0u64;
        loop {
            match self.materialize_oldest_run(store)? {
                Some(n) => {
                    inserted = inserted.saturating_add(n);
                    info!("node: spend materialize run edges≈{n} total≈{inserted}");
                }
                None => break,
            }
        }
        let runs_dir = self.inner.lock().unwrap().ctrl.runs_dir.clone();
        clear_runs_dir(&runs_dir);
        Ok(inserted)
    }
}

fn materialize(store: &Store, run: &SortedRunPath) -> Result<u64, StoreError> {
    let body = read_run_body(run)?;
    let rec_len = run.rec_len as usize;
    if rec_len != FULL as usize {
        // Legacy v4 84-byte runs not supported after schema v5.
        return Err(StoreError::Corrupt("spend run unexpected rec_len (need v5 20-byte)"));
    }
    let n_rec = body.len() / rec_len;
    let mut batch: Vec<(Fk, u32, Fk)> = Vec::with_capacity(n_rec);
    let mut off = 0usize;
    while off + rec_len <= body.len() {
        let create_fk = Fk(u64::from_le_bytes(body[off..off + 8].try_into().unwrap()));
        let vout = u32::from_le_bytes(body[off + 8..off + 12].try_into().unwrap());
        let spend_fk = Fk(u64::from_le_bytes(body[off + 12..off + 20].try_into().unwrap()));
        off += rec_len;
        if !create_fk.is_null() && !spend_fk.is_null() {
            batch.push((create_fk, vout, spend_fk));
        }
    }
    if batch.is_empty() {
        return Ok(0);
    }
    let n = batch.len() as u64;
    store.put_spend_batch_by_create(&batch)?;
    Ok(n)
}
