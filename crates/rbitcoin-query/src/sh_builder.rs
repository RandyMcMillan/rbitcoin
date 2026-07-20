//! Catch-up scripthash builder: memtable → sorted runs → low-prio compact →
//! materialize into durable `scripthash` tables at tip mode.
//!
//! Confirm only enqueues thin creates (no open-hash RMW). A background worker
//! flushes and merges at idle IO priority **while confirm is live**.

use super::run_builder_core::{
    clear_runs_dir, finalize_wait_join, memtable_cap, spawn_worker, take_oldest_run, worker_loop,
    RunControl, RunMemtable, FAMILY_SH,
};
use rbitcoin_log::{debug, info};
use rbitcoin_primitives::Fk;
use rbitcoin_store::{
    next_run_path, read_run_body, remove_run, write_sorted_run, ScriptHashRecord, Store, StoreError,
    SortedRunPath,
};
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;

/// Fixed run record: scripthash[32] | create_tx_fk:u64 | vout:u32 = 44 bytes.
pub const SH_RUN_REC_LEN: u32 = 44;
pub const SH_RUN_KEY_LEN: u32 = 32;

const DEFAULT_MEMTABLE_CAP: usize = 256_000;
const HARD_MEMTABLE_MUL: usize = 2;

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
        recs.sort_unstable_by(|a, b| a.0.cmp(&b.0).then(a.1 .0.cmp(&b.1 .0)).then(a.2.cmp(&b.2)));
        let mut body = Vec::with_capacity(recs.len() * SH_RUN_REC_LEN as usize);
        for (sh, fk, vout) in &recs {
            body.extend_from_slice(&encode_rec(sh, *fk, *vout));
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
                    "ibd: scripthash catch-up mode ON (memtable→sorted runs; no durable SH head on confirm)"
                );
            },
            &self.enabled,
            &self.join,
            move || {
                debug!("ibd: SH run worker started (idle IO prio)");
                worker_loop(
                    memtable_cap("RBITCOIN_SH_MEMTABLE_CAP", DEFAULT_MEMTABLE_CAP),
                    "SH",
                    FAMILY_SH,
                    inner_w,
                    cv_w,
                );
                debug!("ibd: SH run worker stopped");
            },
        );
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
            g.pending
                .push((rec.scripthash, rec.create_tx_fk, rec.vout));
            self.enqueued.fetch_add(1, Ordering::Relaxed);
        }
        if g.pending.len() >= cap {
            self.cv.notify_all();
        }
    }

    /// Materialize the oldest on-disk run into scripthash tables. `Ok(None)` if empty.
    pub fn materialize_oldest_run(&self, store: &Store) -> Result<Option<u64>, StoreError> {
        let (runs_dir, runs_io) = {
            let g = self.inner.lock().unwrap();
            (g.ctrl.runs_dir.clone(), Arc::clone(&g.ctrl.runs_io))
        };
        let Some(run) = take_oldest_run(&runs_dir, &runs_io)? else {
            return Ok(None);
        };
        let n = materialize_run(store, &run)?;
        {
            let _io = runs_io.lock().unwrap();
            remove_run(&run)?;
        }
        Ok(Some(n))
    }

    /// Stop enqueues, flush remaining, materialize each run (no merge), join worker.
    pub fn finalize_and_materialize(&self, store: &Store) -> Result<u64, StoreError> {
        finalize_wait_join(&self.enabled, &self.inner, &self.cv, &self.join)?;
        let mut inserted = 0u64;
        loop {
            match self.materialize_oldest_run(store)? {
                Some(n) => {
                    inserted = inserted.saturating_add(n);
                    info!("node: scripthash materialize run keys≈{n} total≈{inserted}");
                }
                None => break,
            }
        }
        if inserted == 0 {
            info!("node: scripthash run materialize: no runs");
        }
        let runs_dir = self.inner.lock().unwrap().ctrl.runs_dir.clone();
        clear_runs_dir(&runs_dir);
        Ok(inserted)
    }
}

fn materialize_run(store: &Store, run: &SortedRunPath) -> Result<u64, StoreError> {
    // Whole-run append: one seed/get pass over unique keys, one body+head apply.
    // Heads are not used for Electrum reads until tip mode.
    let body = read_run_body(run)?;
    let rec_len = run.rec_len as usize;
    let n_rec = body.len() / rec_len.max(1);
    let mut batch: Vec<ScriptHashRecord> = Vec::with_capacity(n_rec);
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
    }
    if batch.is_empty() {
        return Ok(0);
    }
    let mut heads: std::collections::HashMap<[u8; 32], rbitcoin_store::ShHeadValue> =
        std::collections::HashMap::new();
    let (n, _) = store
        .scripthash
        .put_create_batch_append(&batch, &mut heads)?;
    Ok(n as u64)
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
