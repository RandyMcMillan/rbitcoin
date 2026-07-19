//! Tip-window cache for **confirm connect prevouts**.
//!
//! Filled as blocks are **confirmed** (creates in the block + parents resolved
//! during connect / wave parent prefetch), not as archive races ahead.
//! Byte-capped FIFO: newest tip work stays hot; oldest tip-window entries evict first.
//!
//! **Spend retirement:** after a successful Class C wave, spent vouts are dropped
//! from entries (budget reclaim). Whole parent is removed when no live outputs remain.
//! Do **not** retire mid-connect (script failure must re-resolve cleanly).
//!
//! **Do not probe from reconstruct** — wave bodies are not tip-local; that only
//! generates MISS stats and thrash. Reconstruct uses Class A → store only.
//!
//! Distinct from [`crate::class_a_cache::ClassACache`] (prefetch / reconstruct /
//! input runs). Kill-safe: pure process cache.

use rbitcoin_primitives::Fk;
use rbitcoin_store::{OutputRecord, TxRecord};
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

/// Default ~128 MiB. Override with `RBITCOIN_TIP_PREVOUT_CACHE_MB`.
pub const DEFAULT_BUDGET_BYTES: usize = 128 * 1024 * 1024;

pub mod stats {
    use super::*;
    pub static HIT: AtomicU64 = AtomicU64::new(0);
    pub static MISS: AtomicU64 = AtomicU64::new(0);
    pub static EVICT: AtomicU64 = AtomicU64::new(0);
    pub static NOTE: AtomicU64 = AtomicU64::new(0);
    pub static RETIRE: AtomicU64 = AtomicU64::new(0);

    /// `(hits, misses, evicts, notes, retires)` then reset.
    pub fn sample_and_reset() -> (u64, u64, u64, u64, u64) {
        (
            HIT.swap(0, Ordering::Relaxed),
            MISS.swap(0, Ordering::Relaxed),
            EVICT.swap(0, Ordering::Relaxed),
            NOTE.swap(0, Ordering::Relaxed),
            RETIRE.swap(0, Ordering::Relaxed),
        )
    }
}

struct Entry {
    tx: TxRecord,
    /// Per-vout slots; `None` = spent/retired (reclaimed).
    outputs: Vec<Option<OutputRecord>>,
    bytes: usize,
    live: u32,
}

pub struct TipPrevoutCache {
    inner: Mutex<Inner>,
    budget: usize,
}

struct Inner {
    map: HashMap<u64, Entry>,
    /// txid → fk id for spend retirement / resolve-by-txid.
    by_txid: HashMap<[u8; 32], u64>,
    /// Oldest at front; newest at back.
    order: VecDeque<u64>,
    bytes: usize,
}

impl TipPrevoutCache {
    pub fn new(budget: usize) -> Self {
        Self {
            inner: Mutex::new(Inner {
                map: HashMap::new(),
                by_txid: HashMap::new(),
                order: VecDeque::new(),
                bytes: 0,
            }),
            budget: budget.max(1024 * 1024),
        }
    }

    pub fn from_env() -> Self {
        let budget = std::env::var("RBITCOIN_TIP_PREVOUT_CACHE_MB")
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

    /// True if `fk` is present with every output slot live (no stats bump).
    pub fn has_full_outputs(&self, fk: Fk) -> bool {
        let id = match fk.get() {
            Some(i) => i,
            None => return false,
        };
        let g = self.inner.lock().unwrap();
        let Some(e) = g.map.get(&id) else {
            return false;
        };
        e.live as usize == e.outputs.len() && !e.outputs.is_empty()
    }

    /// Insert or refresh a create tx + full output run (confirm or resolved parent).
    pub fn note(&self, fk: Fk, tx: TxRecord, outputs: Vec<OutputRecord>) {
        let id = match fk.get() {
            Some(i) => i,
            None => return,
        };
        let live = outputs.len() as u32;
        let slots: Vec<Option<OutputRecord>> = outputs.into_iter().map(Some).collect();
        let bytes = entry_bytes(&tx, &slots);
        let mut g = self.inner.lock().unwrap();
        if let Some(old) = g.map.remove(&id) {
            g.bytes = g.bytes.saturating_sub(old.bytes);
            g.by_txid.remove(&old.tx.txid);
            if let Some(pos) = g.order.iter().position(|&x| x == id) {
                g.order.remove(pos);
            }
        }
        g.by_txid.insert(tx.txid, id);
        g.map.insert(
            id,
            Entry {
                tx,
                outputs: slots,
                bytes,
                live,
            },
        );
        g.order.push_back(id);
        g.bytes = g.bytes.saturating_add(bytes);
        stats::NOTE.fetch_add(1, Ordering::Relaxed);
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

    /// Full output run only when **all** slots are live (else `None` → fall through).
    pub fn get_outputs(&self, fk: Fk) -> Option<Vec<OutputRecord>> {
        let id = fk.get()?;
        let g = self.inner.lock().unwrap();
        match g.map.get(&id) {
            Some(e) if e.live as usize == e.outputs.len() && !e.outputs.is_empty() => {
                stats::HIT.fetch_add(1, Ordering::Relaxed);
                Some(
                    e.outputs
                        .iter()
                        .map(|o| o.clone().expect("live count matches"))
                        .collect(),
                )
            }
            Some(_) => {
                // Partial retirement — not a usable full run.
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
        match g.map.get(&id).and_then(|e| e.outputs.get(vout as usize)) {
            Some(Some(o)) => {
                stats::HIT.fetch_add(1, Ordering::Relaxed);
                Some(o.clone())
            }
            Some(None) => {
                // Retired spent slot — not a hit for prevout path.
                stats::MISS.fetch_add(1, Ordering::Relaxed);
                None
            }
            None => {
                stats::MISS.fetch_add(1, Ordering::Relaxed);
                None
            }
        }
    }

    /// True if `(fk, vout)` is cached as **live unspent** (no stats bump).
    ///
    /// Write-through: spent vouts are retired after Class C, so presence means
    /// connect can skip durable `has_confirmed_strong_spender` for this outpoint.
    pub fn has_live_output(&self, fk: Fk, vout: u32) -> bool {
        let Some(id) = fk.get() else {
            return false;
        };
        let g = self.inner.lock().unwrap();
        matches!(
            g.map.get(&id).and_then(|e| e.outputs.get(vout as usize)),
            Some(Some(_))
        )
    }

    /// Live unspent by parent txid (no stats bump).
    pub fn has_live_output_txid(&self, txid: &[u8; 32], vout: u32) -> bool {
        let g = self.inner.lock().unwrap();
        let Some(&id) = g.by_txid.get(txid) else {
            return false;
        };
        matches!(
            g.map.get(&id).and_then(|e| e.outputs.get(vout as usize)),
            Some(Some(_))
        )
    }

    /// Single-lock resolve for connect: `(tx, output)` when the vout is live.
    pub fn get_tx_and_output_at(&self, fk: Fk, vout: u32) -> Option<(TxRecord, OutputRecord)> {
        let id = fk.get()?;
        let g = self.inner.lock().unwrap();
        let e = g.map.get(&id)?;
        match e.outputs.get(vout as usize) {
            Some(Some(o)) => {
                stats::HIT.fetch_add(1, Ordering::Relaxed);
                Some((e.tx.clone(), o.clone()))
            }
            Some(None) | None => {
                stats::MISS.fetch_add(1, Ordering::Relaxed);
                None
            }
        }
    }

    /// Resolve by parent txid (one lock; uses secondary index).
    pub fn get_tx_and_output_by_txid(
        &self,
        txid: &[u8; 32],
        vout: u32,
    ) -> Option<(Fk, TxRecord, OutputRecord)> {
        let g = self.inner.lock().unwrap();
        let id = *g.by_txid.get(txid)?;
        let e = g.map.get(&id)?;
        match e.outputs.get(vout as usize) {
            Some(Some(o)) => {
                stats::HIT.fetch_add(1, Ordering::Relaxed);
                Some((Fk(id), e.tx.clone(), o.clone()))
            }
            Some(None) | None => {
                stats::MISS.fetch_add(1, Ordering::Relaxed);
                None
            }
        }
    }

    /// Drop spent vouts after successful Class C. `spends` are `(prev_txid, vout)`.
    pub fn retire_spends(&self, spends: &[([u8; 32], u32)]) {
        if spends.is_empty() {
            return;
        }
        let mut g = self.inner.lock().unwrap();
        let mut retired = 0u64;
        let mut drop_ids: HashSet<u64> = HashSet::new();
        let mut freed_total = 0usize;
        for &(txid, vout) in spends {
            let Some(&id) = g.by_txid.get(&txid) else {
                continue;
            };
            let Some(e) = g.map.get_mut(&id) else {
                g.by_txid.remove(&txid);
                continue;
            };
            let Some(slot) = e.outputs.get_mut(vout as usize) else {
                continue;
            };
            let Some(old) = slot.take() else {
                continue; // already retired
            };
            let freed = 32 + old.script.len();
            e.bytes = e.bytes.saturating_sub(freed);
            e.live = e.live.saturating_sub(1);
            freed_total = freed_total.saturating_add(freed);
            retired += 1;
            if e.live == 0 {
                drop_ids.insert(id);
            }
        }
        g.bytes = g.bytes.saturating_sub(freed_total);
        for id in drop_ids {
            if let Some(e) = g.map.remove(&id) {
                g.by_txid.remove(&e.tx.txid);
                g.bytes = g.bytes.saturating_sub(e.bytes);
                if let Some(pos) = g.order.iter().position(|&x| x == id) {
                    g.order.remove(pos);
                }
            }
        }
        if retired > 0 {
            stats::RETIRE.fetch_add(retired, Ordering::Relaxed);
        }
    }

    /// Drop everything (e.g. disconnect_tip).
    pub fn clear(&self) {
        let mut g = self.inner.lock().unwrap();
        g.map.clear();
        g.by_txid.clear();
        g.order.clear();
        g.bytes = 0;
    }
}

impl Inner {
    fn evict_to_budget(&mut self, budget: usize) {
        while self.bytes > budget {
            let Some(old_id) = self.order.pop_front() else {
                break;
            };
            if let Some(e) = self.map.remove(&old_id) {
                self.by_txid.remove(&e.tx.txid);
                self.bytes = self.bytes.saturating_sub(e.bytes);
                stats::EVICT.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
}

fn entry_bytes(tx: &TxRecord, outputs: &[Option<OutputRecord>]) -> usize {
    let mut n = 128 + TxRecord::ENCODED_LEN + 24;
    for o in outputs.iter().flatten() {
        n += 32 + o.script.len();
    }
    let _ = tx;
    n
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
            output_count: 2,
        }
    }

    fn out(v: i64) -> OutputRecord {
        OutputRecord {
            value: v,
            script: vec![0x51],
        }
    }

    #[test]
    fn note_get_and_fifo_evict() {
        let c = TipPrevoutCache::new(1024 * 1024);
        c.note(Fk(1), tx(1), vec![out(10), out(20)]);
        assert_eq!(c.get_output_at(Fk(1), 0).unwrap().value, 10);
        assert_eq!(c.get_tx(Fk(1)).unwrap().txid[0], 1);
        let (t, o) = c.get_tx_and_output_at(Fk(1), 1).unwrap();
        assert_eq!(t.txid[0], 1);
        assert_eq!(o.value, 20);

        for i in 2u64..=4000 {
            let mut t = tx((i & 0xff) as u8);
            t.txid[0..8].copy_from_slice(&i.to_le_bytes());
            t.output_count = 1;
            let o = OutputRecord {
                value: i as i64,
                script: vec![0x6a; 256],
            };
            c.note(Fk(i), t, vec![o]);
        }
        assert!(c.approx_bytes() <= c.budget_bytes() + 4096);
        assert!(c.get_tx(Fk(4000)).is_some());
        assert!(c.len() < 4000);
    }

    #[test]
    fn retire_vout_then_drop_empty() {
        let c = TipPrevoutCache::new(1024 * 1024);
        let t = tx(1);
        let txid = t.txid;
        c.note(Fk(1), t, vec![out(10), out(20)]);
        c.retire_spends(&[(txid, 0)]);
        assert!(c.get_output_at(Fk(1), 0).is_none());
        assert_eq!(c.get_output_at(Fk(1), 1).unwrap().value, 20);
        // Full run no longer available.
        assert!(c.get_outputs(Fk(1)).is_none());
        c.retire_spends(&[(txid, 1)]);
        assert!(c.get_tx(Fk(1)).is_none());
        assert_eq!(c.len(), 0);
        let (_h, _m, _e, _n, r) = stats::sample_and_reset();
        assert!(r >= 2);
    }
}
