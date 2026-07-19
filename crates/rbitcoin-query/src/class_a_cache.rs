//! Process-local Class A working set for **reconstruct + input runs**.
//!
//! Holds decoded `TxRecord` + input/output runs.
//!
//! **Fill policy:**
//! - **(A) Confirm-wave prefetch** — before a multi-block confirm, load tip+1…tip+N
//!   bodies (with ins/outs) so reconstruct hits RAM.
//! - **(C) Not dual-filled on confirm creates** — tip_prevout owns prevout locality.
//! - **(D) Archive bulk-fill only when lead is small** (tip-follow); off when archive
//!   races far ahead (avoids FIFO thrash).
//! - Miss path still notes on cold load.
//!
//! Prefer [`crate::tip_prevout_cache`] for confirm **prevouts** (outputs only).
//! Reconstruct / `get_tx_class_a` / `tx_output_run_class_a` stay on this cache.
//! Kill-safe: pure cache; byte-capped FIFO.

use rbitcoin_primitives::Fk;
use rbitcoin_store::{InputRecord, OutputRecord, TxRecord};
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

/// Default RAM budget (~256 MiB). Override with env `RBITCOIN_CLASS_A_CACHE_MB`.
///
/// Was 1 GiB; under IBD with archive lead, thrashing that large a FIFO was worse
/// than a tighter working set (see signet: hundreds of k evicts/5s).
pub const DEFAULT_BUDGET_BYTES: usize = 256 * 1024 * 1024;

pub mod stats {
    use super::*;
    pub static HIT: AtomicU64 = AtomicU64::new(0);
    pub static MISS: AtomicU64 = AtomicU64::new(0);
    pub static EVICT: AtomicU64 = AtomicU64::new(0);

    /// `(hits, misses, evicts)` then reset.
    pub fn sample_and_reset() -> (u64, u64, u64) {
        (
            HIT.swap(0, Ordering::Relaxed),
            MISS.swap(0, Ordering::Relaxed),
            EVICT.swap(0, Ordering::Relaxed),
        )
    }
}

struct Entry {
    tx: TxRecord,
    outputs: Option<Vec<OutputRecord>>,
    inputs: Option<Vec<InputRecord>>,
    bytes: usize,
}

pub struct ClassACache {
    inner: Mutex<Inner>,
    budget: usize,
}

struct Inner {
    map: HashMap<u64, Entry>,
    /// Oldest at front; newest at back.
    order: VecDeque<u64>,
    bytes: usize,
}

impl ClassACache {
    pub fn new(budget: usize) -> Self {
        Self {
            inner: Mutex::new(Inner {
                map: HashMap::new(),
                order: VecDeque::new(),
                bytes: 0,
            }),
            budget: budget.max(1024 * 1024), // at least 1 MiB
        }
    }

    pub fn from_env() -> Self {
        let budget = std::env::var("RBITCOIN_CLASS_A_CACHE_MB")
            .ok()
            .and_then(|s| s.parse::<usize>().ok())
            .map(|mb| mb.saturating_mul(1024 * 1024))
            .unwrap_or(DEFAULT_BUDGET_BYTES);
        Self::new(budget)
    }

    pub fn budget_bytes(&self) -> usize {
        self.budget
    }

    pub fn approx_bytes(&self) -> usize {
        self.inner.lock().unwrap().bytes
    }

    pub fn len(&self) -> usize {
        self.inner.lock().unwrap().map.len()
    }

    /// Insert or refresh a tx after archive (or after a store miss fill).
    pub fn note(
        &self,
        fk: Fk,
        tx: TxRecord,
        outputs: Option<Vec<OutputRecord>>,
        inputs: Option<Vec<InputRecord>>,
    ) {
        let id = match fk.get() {
            Some(i) => i,
            None => return,
        };
        let bytes = approx_entry_bytes(&tx, outputs.as_deref(), inputs.as_deref());
        let mut g = self.inner.lock().unwrap();
        if let Some(old) = g.map.remove(&id) {
            g.bytes = g.bytes.saturating_sub(old.bytes);
            // drop from order (linear; ok — archive batches are bulk)
            if let Some(pos) = g.order.iter().position(|&x| x == id) {
                g.order.remove(pos);
            }
        }
        g.map.insert(
            id,
            Entry {
                tx,
                outputs,
                inputs,
                bytes,
            },
        );
        g.order.push_back(id);
        g.bytes = g.bytes.saturating_add(bytes);
        g.evict_to_budget(self.budget);
    }

    pub fn get_tx(&self, fk: Fk) -> Option<TxRecord> {
        let id = fk.get()?;
        let g = self.inner.lock().unwrap();
        if let Some(e) = g.map.get(&id) {
            stats::HIT.fetch_add(1, Ordering::Relaxed);
            Some(e.tx.clone())
        } else {
            stats::MISS.fetch_add(1, Ordering::Relaxed);
            None
        }
    }

    /// True if `fk` is cached with runs required for reconstruct (no stats bump).
    pub fn has_reconstruct_ready(&self, fk: Fk) -> bool {
        let id = match fk.get() {
            Some(i) => i,
            None => return false,
        };
        let g = self.inner.lock().unwrap();
        let Some(e) = g.map.get(&id) else {
            return false;
        };
        let outs_ok = e.tx.output_count == 0 || e.outputs.is_some();
        let ins_ok = e.tx.input_count == 0 || e.inputs.is_some();
        outs_ok && ins_ok
    }

    pub fn get_outputs(&self, fk: Fk) -> Option<Vec<OutputRecord>> {
        let id = fk.get()?;
        let g = self.inner.lock().unwrap();
        match g.map.get(&id) {
            Some(e) if e.outputs.is_some() => {
                stats::HIT.fetch_add(1, Ordering::Relaxed);
                e.outputs.clone()
            }
            Some(_) => {
                // Entry present but outputs not cached — treat as soft miss.
                stats::MISS.fetch_add(1, Ordering::Relaxed);
                None
            }
            None => {
                stats::MISS.fetch_add(1, Ordering::Relaxed);
                None
            }
        }
    }

    pub fn get_output_at(&self, fk: Fk, vout: u32) -> Option<OutputRecord> {
        let id = fk.get()?;
        let g = self.inner.lock().unwrap();
        match g.map.get(&id).and_then(|e| e.outputs.as_ref()) {
            Some(outs) if (vout as usize) < outs.len() => {
                stats::HIT.fetch_add(1, Ordering::Relaxed);
                Some(outs[vout as usize].clone())
            }
            Some(_) => {
                stats::MISS.fetch_add(1, Ordering::Relaxed);
                None
            }
            None => {
                stats::MISS.fetch_add(1, Ordering::Relaxed);
                None
            }
        }
    }

    pub fn get_inputs(&self, fk: Fk) -> Option<Vec<InputRecord>> {
        let id = fk.get()?;
        let g = self.inner.lock().unwrap();
        match g.map.get(&id) {
            Some(e) if e.inputs.is_some() => {
                stats::HIT.fetch_add(1, Ordering::Relaxed);
                e.inputs.clone()
            }
            Some(_) => {
                stats::MISS.fetch_add(1, Ordering::Relaxed);
                None
            }
            None => {
                stats::MISS.fetch_add(1, Ordering::Relaxed);
                None
            }
        }
    }

    /// Merge outputs into an existing entry (or no-op if absent).
    pub fn fill_outputs(&self, fk: Fk, outputs: Vec<OutputRecord>) {
        let id = match fk.get() {
            Some(i) => i,
            None => return,
        };
        let mut g = self.inner.lock().unwrap();
        let Some(e) = g.map.get_mut(&id) else {
            return;
        };
        if e.outputs.is_some() {
            return;
        }
        let add = outputs.iter().map(output_bytes).sum::<usize>() + 24;
        e.outputs = Some(outputs);
        e.bytes = e.bytes.saturating_add(add);
        g.bytes = g.bytes.saturating_add(add);
        g.evict_to_budget(self.budget);
    }

    pub fn fill_inputs(&self, fk: Fk, inputs: Vec<InputRecord>) {
        let id = match fk.get() {
            Some(i) => i,
            None => return,
        };
        let mut g = self.inner.lock().unwrap();
        let Some(e) = g.map.get_mut(&id) else {
            return;
        };
        if e.inputs.is_some() {
            return;
        }
        let add = inputs.iter().map(input_bytes).sum::<usize>() + 24;
        e.inputs = Some(inputs);
        e.bytes = e.bytes.saturating_add(add);
        g.bytes = g.bytes.saturating_add(add);
        g.evict_to_budget(self.budget);
    }
}

impl Inner {
    fn evict_to_budget(&mut self, budget: usize) {
        while self.bytes > budget {
            let Some(old_id) = self.order.pop_front() else {
                break;
            };
            if let Some(e) = self.map.remove(&old_id) {
                self.bytes = self.bytes.saturating_sub(e.bytes);
                stats::EVICT.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
}

fn approx_entry_bytes(
    _tx: &TxRecord,
    outputs: Option<&[OutputRecord]>,
    inputs: Option<&[InputRecord]>,
) -> usize {
    let mut n = 128 + TxRecord::ENCODED_LEN;
    if let Some(o) = outputs {
        n += 24 + o.iter().map(output_bytes).sum::<usize>();
    }
    if let Some(i) = inputs {
        n += 24 + i.iter().map(input_bytes).sum::<usize>();
    }
    n
}

fn output_bytes(o: &OutputRecord) -> usize {
    32 + o.script.len()
}

fn input_bytes(i: &InputRecord) -> usize {
    64 + i.script_sig.len()
        + i.witness.iter().map(|w| w.len() + 8).sum::<usize>()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tx(id: u8) -> TxRecord {
        let mut txid = [0u8; 32];
        txid[0] = id;
        TxRecord {
            txid,
            version: 1,
            locktime: 0,
            input_start_fk: Fk::NULL,
            input_count: 0,
            output_start_fk: Fk(1),
            output_count: 1,
        }
    }

    fn out(v: i64) -> OutputRecord {
        OutputRecord {
            value: v,
            script: vec![0x51],
        }
    }

    #[test]
    fn note_and_get_roundtrip() {
        let c = ClassACache::new(64 * 1024);
        c.note(Fk(1), tx(1), Some(vec![out(50)]), None);
        assert_eq!(c.get_tx(Fk(1)).unwrap().txid[0], 1);
        assert_eq!(c.get_output_at(Fk(1), 0).unwrap().value, 50);
        assert!(c.get_tx(Fk(2)).is_none());
    }

    #[test]
    fn evicts_when_over_budget() {
        // Floor is 1 MiB; fill well past it with fat scripts.
        let c = ClassACache::new(1024 * 1024);
        for i in 1u64..=8000 {
            let mut t = tx((i & 0xff) as u8);
            t.txid[0..8].copy_from_slice(&i.to_le_bytes());
            let o = OutputRecord {
                value: 1,
                script: vec![0x6a; 512],
            };
            c.note(Fk(i), t, Some(vec![o]), None);
        }
        assert!(c.approx_bytes() <= c.budget_bytes() + 2048);
        assert!(c.get_tx(Fk(8000)).is_some());
        assert!(c.len() < 8000);
    }
}
