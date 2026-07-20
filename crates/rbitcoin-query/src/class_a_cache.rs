//! Process-local Class A working set for **reconstruct + input runs**.
//!
//! Holds decoded `TxRecord` + input/output runs.
//!
//! **Fill policy:**
//! - **(A) Confirm-wave prefetch** — tip+1…tip+N bodies into RAM before connect.
//! - **(B) Parent prewarm** — runway worker loads external parents for tip+1…tip+~1k
//!   and **pins** them (per outpoint) until those outs are spent.
//! - **(C) Not dual-filled on confirm creates** — tip_prevout owns prevout locality.
//! - **(D) Archive bulk-fill only when lead is small** (tip-follow).
//! - Miss path notes without evicting when full.
//!
//! **Eviction:** byte-capped FIFO, but **pinned** parent fks are never dropped.
//! Prefer [`crate::tip_prevout_cache`] for confirm prevout short-circuit.
//! Kill-safe: pure process cache.

use rbitcoin_primitives::Fk;
use rbitcoin_store::{InputRecord, OutputRecord, TxRecord};
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

/// Default RAM budget (~512 MiB). Override with env `RBITCOIN_CLASS_A_CACHE_MB`.
///
/// Sized for confirm runway + pinned parents under deep archive lead.
pub const DEFAULT_BUDGET_BYTES: usize = 512 * 1024 * 1024;

pub mod stats {
    use super::*;
    pub static HIT: AtomicU64 = AtomicU64::new(0);
    pub static MISS: AtomicU64 = AtomicU64::new(0);
    pub static EVICT: AtomicU64 = AtomicU64::new(0);
    pub static PIN: AtomicU64 = AtomicU64::new(0);
    pub static UNPIN: AtomicU64 = AtomicU64::new(0);

    /// `(hits, misses, evicts)` then reset (PIN/UNPIN left for diagnostics).
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
    /// Pin refcount per `(fk_id, vout)`. Parent stays until all pins released.
    out_pins: HashMap<(u64, u32), u32>,
    /// Number of distinct pinned vouts per fk (fast non-evict check).
    fk_pin_outs: HashMap<u64, u32>,
}

impl ClassACache {
    pub fn new(budget: usize) -> Self {
        Self {
            inner: Mutex::new(Inner {
                map: HashMap::new(),
                order: VecDeque::new(),
                bytes: 0,
                out_pins: HashMap::new(),
                fk_pin_outs: HashMap::new(),
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

    /// Insert or refresh a tx after archive / confirm prefetch (may evict FIFO).
    pub fn note(
        &self,
        fk: Fk,
        tx: TxRecord,
        outputs: Option<Vec<OutputRecord>>,
        inputs: Option<Vec<InputRecord>>,
    ) {
        self.note_inner(fk, tx, outputs, inputs, true);
    }

    /// Cold miss-fill: insert only if there is free budget — **never** evict.
    ///
    /// Wave parent loads used to `note` every cold parent and FIFO-evict the
    /// confirm-prefetched bodies (mainnet ~220k: 50–95s wave with archive
    /// 400k ahead). Prefer leaving parents uncached over thrashing the wave set.
    pub fn note_no_evict(
        &self,
        fk: Fk,
        tx: TxRecord,
        outputs: Option<Vec<OutputRecord>>,
        inputs: Option<Vec<InputRecord>>,
    ) -> bool {
        self.note_inner(fk, tx, outputs, inputs, false)
    }

    fn note_inner(
        &self,
        fk: Fk,
        tx: TxRecord,
        outputs: Option<Vec<OutputRecord>>,
        inputs: Option<Vec<InputRecord>>,
        allow_evict: bool,
    ) -> bool {
        let id = match fk.get() {
            Some(i) => i,
            None => return false,
        };
        let bytes = approx_entry_bytes(&tx, outputs.as_deref(), inputs.as_deref());
        let mut g = self.inner.lock().unwrap();
        let replacing = g.map.contains_key(&id);
        if !replacing && !allow_evict && g.bytes.saturating_add(bytes) > self.budget {
            return false;
        }
        if let Some(old) = g.map.remove(&id) {
            g.bytes = g.bytes.saturating_sub(old.bytes);
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
        if allow_evict {
            g.evict_to_budget(self.budget);
        }
        true
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

    /// One lock: clone tx + full I/O runs when reconstruct-ready.
    ///
    /// Prefer this over `get_tx` + `get_outputs` + `get_inputs` (3 mutex round-trips)
    /// on the confirm reconstruct / wave-body hot path.
    pub fn get_reconstruct_parts(
        &self,
        fk: Fk,
    ) -> Option<(TxRecord, Vec<OutputRecord>, Vec<InputRecord>)> {
        let id = fk.get()?;
        let g = self.inner.lock().unwrap();
        let e = g.map.get(&id)?;
        let outs_ok = e.tx.output_count == 0 || e.outputs.is_some();
        let ins_ok = e.tx.input_count == 0 || e.inputs.is_some();
        if !outs_ok || !ins_ok {
            stats::MISS.fetch_add(1, Ordering::Relaxed);
            return None;
        }
        stats::HIT.fetch_add(1, Ordering::Relaxed);
        let outs = if e.tx.output_count == 0 {
            Vec::new()
        } else {
            e.outputs.clone().unwrap_or_default()
        };
        let inputs = if e.tx.input_count == 0 {
            Vec::new()
        } else {
            e.inputs.clone().unwrap_or_default()
        };
        Some((e.tx.clone(), outs, inputs))
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
    ///
    /// Does not FIFO-evict other entries (see [`Self::note_no_evict`]).
    pub fn fill_outputs(&self, fk: Fk, outputs: Vec<OutputRecord>) {
        let id = match fk.get() {
            Some(i) => i,
            None => return,
        };
        let add = outputs.iter().map(output_bytes).sum::<usize>() + 24;
        let mut g = self.inner.lock().unwrap();
        if g.bytes.saturating_add(add) > self.budget && !g.is_pinned(id) {
            return; // keep entry thin rather than thrashing the wave set
        }
        let Some(e) = g.map.get_mut(&id) else {
            return;
        };
        if e.outputs.is_some() {
            return;
        }
        e.outputs = Some(outputs);
        e.bytes = e.bytes.saturating_add(add);
        g.bytes = g.bytes.saturating_add(add);
    }

    pub fn fill_inputs(&self, fk: Fk, inputs: Vec<InputRecord>) {
        let id = match fk.get() {
            Some(i) => i,
            None => return,
        };
        let add = inputs.iter().map(input_bytes).sum::<usize>() + 24;
        let mut g = self.inner.lock().unwrap();
        if g.bytes.saturating_add(add) > self.budget && !g.is_pinned(id) {
            return;
        }
        let Some(e) = g.map.get_mut(&id) else {
            return;
        };
        if e.inputs.is_some() {
            return;
        }
        e.inputs = Some(inputs);
        e.bytes = e.bytes.saturating_add(add);
        g.bytes = g.bytes.saturating_add(add);
    }

    /// Pin parent outpoint so FIFO eviction cannot drop the create tx body.
    ///
    /// Call once per future spend that needs this prevout (prewarm / wave).
    pub fn pin_out(&self, fk: Fk, vout: u32) {
        let Some(id) = fk.get() else {
            return;
        };
        let mut g = self.inner.lock().unwrap();
        g.pin_out(id, vout);
        stats::PIN.fetch_add(1, Ordering::Relaxed);
    }

    /// Release one pin for `(fk, vout)` after that outpoint is spent.
    pub fn unpin_out(&self, fk: Fk, vout: u32) {
        let Some(id) = fk.get() else {
            return;
        };
        let mut g = self.inner.lock().unwrap();
        if g.unpin_out(id, vout) {
            stats::UNPIN.fetch_add(1, Ordering::Relaxed);
            g.evict_to_budget(self.budget);
        }
    }

    /// Number of distinct pinned parent outpoints (diagnostics).
    pub fn pinned_out_count(&self) -> usize {
        self.inner.lock().unwrap().out_pins.len()
    }
}

impl Inner {
    fn is_pinned(&self, id: u64) -> bool {
        self.fk_pin_outs.get(&id).copied().unwrap_or(0) > 0
    }

    fn pin_out(&mut self, id: u64, vout: u32) {
        let c = self.out_pins.entry((id, vout)).or_insert(0);
        *c = c.saturating_add(1);
        if *c == 1 {
            *self.fk_pin_outs.entry(id).or_insert(0) += 1;
        }
    }

    /// Returns true if a pin was released.
    fn unpin_out(&mut self, id: u64, vout: u32) -> bool {
        let Some(c) = self.out_pins.get_mut(&(id, vout)) else {
            return false;
        };
        *c = c.saturating_sub(1);
        if *c > 0 {
            return true;
        }
        self.out_pins.remove(&(id, vout));
        if let Some(n) = self.fk_pin_outs.get_mut(&id) {
            *n = n.saturating_sub(1);
            if *n == 0 {
                self.fk_pin_outs.remove(&id);
            }
        }
        true
    }

    fn evict_to_budget(&mut self, budget: usize) {
        let mut skipped = 0usize;
        let n = self.order.len();
        while self.bytes > budget {
            if skipped >= n || self.order.is_empty() {
                break; // only pinned entries remain (or empty)
            }
            let Some(old_id) = self.order.pop_front() else {
                break;
            };
            if self.is_pinned(old_id) {
                self.order.push_back(old_id);
                skipped += 1;
                continue;
            }
            if let Some(e) = self.map.remove(&old_id) {
                self.bytes = self.bytes.saturating_sub(e.bytes);
                stats::EVICT.fetch_add(1, Ordering::Relaxed);
                skipped = 0; // progress
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

    #[test]
    fn pin_protects_from_eviction() {
        let c = ClassACache::new(1024 * 1024);
        c.note(Fk(1), tx(1), Some(vec![out(1)]), None);
        c.pin_out(Fk(1), 0);
        assert_eq!(c.pinned_out_count(), 1);
        for i in 2u64..=8000 {
            let mut t = tx((i & 0xff) as u8);
            t.txid[0..8].copy_from_slice(&i.to_le_bytes());
            let o = OutputRecord {
                value: 1,
                script: vec![0x6a; 512],
            };
            c.note(Fk(i), t, Some(vec![o]), None);
        }
        // Pinned parent must survive the flood.
        assert!(c.get_tx(Fk(1)).is_some());
        c.unpin_out(Fk(1), 0);
        assert_eq!(c.pinned_out_count(), 0);
    }
}
