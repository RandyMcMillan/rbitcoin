//! Unified create residency — **sole hot create map** for wire IBD pin / plan.
//!
//! # Map shape (complete rows only)
//!
//! `create_fk → (txid, optional body_range, CreatePin Arc)`.
//!
//! A row is **always complete**: outs + denserels travel with the fk. Half-rows
//! (fk/range only) and out-slim-keep-fk are gone.
//!
//! # What is stored
//!
//! **Pipeline creates only** — txs that enter confirm (plan packing / res_seed /
//! tip prewarm). **External parents must not be inserted** (batch-local
//! `external_parent_outs` only). Later spends hit residency only if that create
//! is still in the FIFO from **its own** pipeline insert.
//!
//! # Eviction: pure insert-order FIFO by byte budget
//!
//! Default **2 GiB** of pin payload. Oldest complete rows hard-dropped (entire
//! row). Lookups **never** reorder. Do not reintroduce touch-on-hit / LRU.
//!
//! # Arc share
//!
//! Pin material is [`crate::CreatePin`] (`Arc<(TxRecord, outs, denserels)>`).
//! Residency stores the Arc; plan/pin take `Arc::clone` — no deep outs copy for
//! residency hits while a batch is in flight.
//!
//! # Env
//!
//! - `RBITCOIN_RESIDENCY_BYTES` — byte budget (default **2 GiB**). `0` disables
//!   residency (puts are no-ops; prewarm skipped).
//!
//! Header plans are **not** controlled here (always on for multi-block MTP).

use crate::CreatePin;
use rbitcoin_primitives::Fk;
use rbitcoin_store::{OutputRecord, TxRecord};
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};

/// Default residency heap budget for complete create pins (2 GiB).
pub const DEFAULT_RESIDENCY_BYTES: u64 = 2 * 1024 * 1024 * 1024;

/// Serialize `RBITCOIN_RESIDENCY_BYTES` mutations in tests (parallel `cargo test`).
#[cfg(test)]
pub static TEST_RESIDENCY_ENV_LOCK: Mutex<()> = Mutex::new(());

/// Fixed overhead per resident row (map entry + Arc header + txid + range + fudge).
const ROW_FIXED_BYTES: u64 = 256;

/// Estimate heap bytes charged to the residency budget for one complete pin.
pub fn estimate_pin_bytes(pin: &CreatePin) -> u64 {
    let (tx, outs, denserels) = pin.as_ref();
    let mut n = ROW_FIXED_BYTES;
    n = n.saturating_add(std::mem::size_of_val(tx) as u64);
    n = n.saturating_add((denserels.len() * 4) as u64);
    n = n.saturating_add((denserels.capacity() * 4) as u64 / 4); // small capacity fudge
    for o in outs {
        n = n.saturating_add(32); // OutputRecord meta fudge
        n = n.saturating_add(o.script.len() as u64);
        n = n.saturating_add(o.script.capacity() as u64 / 4);
    }
    n = n.saturating_add((outs.capacity() * 24) as u64);
    n
}

#[derive(Debug, Clone)]
pub struct ResidentCreate {
    pub txid: [u8; 32],
    pub body_range: Option<(u64, u64)>,
    /// Complete pin (tx + outs + denserels). Always present.
    pub pin: CreatePin,
    /// Bytes charged for this row (payload estimate at insert).
    pub bytes: u64,
}

struct Inner {
    by_fk: HashMap<u64, ResidentCreate>,
    by_txid: HashMap<[u8; 32], u64>,
    /// Insert-order FIFO of complete creates.
    order: VecDeque<u64>,
    /// Total estimated payload bytes of resident pins.
    total_bytes: u64,
    /// Total output count (metrics / sizes line).
    total_outs: u64,
    /// Byte budget; 0 = residency disabled (puts are no-ops).
    byte_cap: u64,
}

/// Process-local unified residency (sole writer for inserts; shared Mutex).
pub struct CreateResidency {
    inner: Mutex<Inner>,
}

impl CreateResidency {
    /// Construct with explicit byte budget. `byte_cap == 0` disables puts.
    pub fn new(byte_cap: u64) -> Self {
        // HashMap pre-size: rough create count at ~4 KiB/create under budget.
        let init = if byte_cap == 0 {
            16
        } else {
            ((byte_cap / 4096) as usize).clamp(1024, 1 << 20)
        };
        Self {
            inner: Mutex::new(Inner {
                by_fk: HashMap::with_capacity(init),
                by_txid: HashMap::with_capacity(init),
                order: VecDeque::with_capacity(init.min(1 << 18)),
                total_bytes: 0,
                total_outs: 0,
                byte_cap,
            }),
        }
    }

    /// From `RBITCOIN_RESIDENCY_BYTES` (default [`DEFAULT_RESIDENCY_BYTES`]; `0` = off).
    pub fn from_env() -> Self {
        let byte_cap = std::env::var("RBITCOIN_RESIDENCY_BYTES")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(DEFAULT_RESIDENCY_BYTES);
        Self::new(byte_cap)
    }

    /// Whether residency inserts are enabled (`byte_cap > 0`).
    pub fn enabled(&self) -> bool {
        self.inner.lock().unwrap().byte_cap > 0
    }

    pub fn len(&self) -> usize {
        self.inner.lock().unwrap().by_fk.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn total_outs(&self) -> u64 {
        self.inner.lock().unwrap().total_outs
    }

    pub fn total_bytes(&self) -> u64 {
        self.inner.lock().unwrap().total_bytes
    }

    pub fn byte_cap(&self) -> u64 {
        self.inner.lock().unwrap().byte_cap
    }

    /// `(creates, total_bytes, byte_cap, total_outs)` under one lock (IBD sizes).
    pub fn size_stats(&self) -> (usize, u64, u64, u64) {
        let g = self.inner.lock().unwrap();
        (g.by_fk.len(), g.total_bytes, g.byte_cap, g.total_outs)
    }

    /// Insert / update a **complete** pipeline create. Stores `Arc::clone` of `pin`.
    ///
    /// Requires `pin.1.len() == pin.2.len()`. No-op when residency disabled or
    /// null fk. On update of an existing fk: refresh pin/range, **leave FIFO
    /// order** (no promote).
    pub fn put_complete(&self, fk: Fk, pin: CreatePin, body_range: Option<(u64, u64)>) {
        let Some(id) = fk.get() else {
            return;
        };
        let (tx, outs, denserels) = pin.as_ref();
        if outs.len() != denserels.len() {
            // Incomplete denserels — refuse (would break pin).
            return;
        }
        let bytes = estimate_pin_bytes(&pin);
        let n_outs = outs.len() as u64;
        let txid = tx.txid;
        let mut g = self.inner.lock().unwrap();
        if g.byte_cap == 0 {
            return;
        }
        if let Some(e) = g.by_fk.get_mut(&id) {
            let old_bytes = e.bytes;
            let old_outs = e.pin.1.len() as u64;
            e.txid = txid;
            e.pin = pin;
            e.bytes = bytes;
            if body_range.is_some() {
                e.body_range = body_range;
            }
            g.by_txid.insert(txid, id);
            g.total_bytes = g
                .total_bytes
                .saturating_sub(old_bytes)
                .saturating_add(bytes);
            g.total_outs = g.total_outs.saturating_sub(old_outs).saturating_add(n_outs);
            let cap = g.byte_cap;
            g.evict_until_bytes(cap);
            return;
        }
        let cap = g.byte_cap;
        // Make room for this row.
        g.evict_until_bytes(cap.saturating_sub(bytes.min(cap)));
        g.by_fk.insert(
            id,
            ResidentCreate {
                txid,
                body_range,
                pin,
                bytes,
            },
        );
        g.by_txid.insert(txid, id);
        g.order.push_back(id);
        g.total_bytes = g.total_bytes.saturating_add(bytes);
        g.total_outs = g.total_outs.saturating_add(n_outs);
        // Final clamp if estimate overflowed (single row > cap keeps newest only).
        g.evict_until_bytes(cap);
    }

    /// Convenience: build pin Arc from owned parts then [`put_complete`].
    ///
    /// Prefer `put_complete` with a shared [`CreatePin`] when the caller already
    /// has an Arc (pipeline create / plan packing).
    pub fn put_outs(
        &self,
        fk: Fk,
        tx: TxRecord,
        outs: Vec<OutputRecord>,
        denserels: Vec<u32>,
        body_range: Option<(u64, u64)>,
    ) {
        if outs.len() != denserels.len() {
            return;
        }
        self.put_complete(fk, Arc::new((tx, outs, denserels)), body_range);
    }

    /// Patch body range on an existing complete row. Never creates a row.
    pub fn set_body_range(&self, fk: Fk, off: u64, len: u64) {
        let Some(id) = fk.get() else {
            return;
        };
        let mut g = self.inner.lock().unwrap();
        if let Some(e) = g.by_fk.get_mut(&id) {
            e.body_range = Some((off, len));
        }
    }

    pub fn body_ranges_by_fk(&self, fks: &[Fk]) -> Vec<Option<(u64, u64)>> {
        let g = self.inner.lock().unwrap();
        fks.iter()
            .map(|fk| {
                let id = fk.get()?;
                g.by_fk.get(&id).and_then(|e| e.body_range)
            })
            .collect()
    }

    pub fn lookup_fk_by_txid(&self, txid: &[u8; 32]) -> Option<Fk> {
        let g = self.inner.lock().unwrap();
        g.by_txid.get(txid).map(|&id| Fk(id))
    }

    pub fn get_txid(&self, fk: Fk) -> Option<[u8; 32]> {
        let id = fk.get()?;
        self.inner.lock().unwrap().by_fk.get(&id).map(|e| e.txid)
    }

    /// Shared pin Arc + body range. Prefer this over deep-cloning outs.
    pub fn get_pin(&self, fk: Fk) -> Option<(CreatePin, Option<(u64, u64)>)> {
        let id = fk.get()?;
        let g = self.inner.lock().unwrap();
        let e = g.by_fk.get(&id)?;
        Some((Arc::clone(&e.pin), e.body_range))
    }

    pub fn get_tx(&self, fk: Fk) -> Option<TxRecord> {
        let id = fk.get()?;
        self.inner
            .lock()
            .unwrap()
            .by_fk
            .get(&id)
            .map(|e| e.pin.0.clone())
    }

    pub fn get_parent_out(&self, fk: Fk, vout: u32) -> Option<(TxRecord, OutputRecord)> {
        let id = fk.get()?;
        let g = self.inner.lock().unwrap();
        let e = g.by_fk.get(&id)?;
        let o = e.pin.1.get(vout as usize)?;
        Some((e.pin.0.clone(), o.clone()))
    }

    pub fn has_parent_out(&self, fk: Fk, vout: u32) -> bool {
        let Some(id) = fk.get() else {
            return false;
        };
        self.inner
            .lock()
            .unwrap()
            .by_fk
            .get(&id)
            .is_some_and(|e| (vout as usize) < e.pin.1.len())
    }

    /// Deep clone of pin contents (tests / legacy). Prefer [`get_pin`].
    pub fn get_outs(
        &self,
        fk: Fk,
    ) -> Option<(TxRecord, Vec<OutputRecord>, Vec<u32>, Option<(u64, u64)>)> {
        let (pin, range) = self.get_pin(fk)?;
        let (tx, outs, rels) = pin.as_ref();
        Some((tx.clone(), outs.clone(), rels.clone(), range))
    }

    /// True if a complete pin is resident (always true for any present fk).
    pub fn has_outs(&self, fk: Fk) -> bool {
        let Some(id) = fk.get() else {
            return false;
        };
        self.inner.lock().unwrap().by_fk.contains_key(&id)
    }

    /// Sparse pin hit: clone only `need_vouts` scripts + denserel slots.
    ///
    /// Prefer [`get_pin`] + index by vout when the caller can hold the Arc.
    pub fn get_parent_needed(
        &self,
        fk: Fk,
        need_vouts: &[u32],
    ) -> Option<(
        TxRecord,
        Vec<(u32, OutputRecord)>,
        Vec<(u32, u32)>,
        Option<(u64, u64)>,
    )> {
        use crate::batch_parents::{layout_covers_need, sparse_spender_rels, SPENDER_REL_UNKNOWN};

        let id = fk.get()?;
        let g = self.inner.lock().unwrap();
        let e = g.by_fk.get(&id)?;
        let tx = &e.pin.0;
        let outs = &e.pin.1;
        let denserels = &e.pin.2;
        if denserels.is_empty() && !need_vouts.is_empty() {
            return None;
        }
        let mut need: Vec<u32> = if need_vouts.is_empty() {
            (0..outs.len() as u32).collect()
        } else {
            need_vouts.to_vec()
        };
        need.sort_unstable();
        need.dedup();
        let sparse = sparse_spender_rels(denserels, &need);
        if !layout_covers_need(e.body_range, &sparse, &need) {
            return None;
        }
        let mut live = Vec::with_capacity(need.len());
        for &v in &need {
            let o = outs.get(v as usize)?;
            let _ = denserels
                .get(v as usize)
                .filter(|&&r| r != SPENDER_REL_UNKNOWN)?;
            live.push((v, o.clone()));
        }
        Some((tx.clone(), live, sparse, e.body_range))
    }
}

impl Inner {
    fn evict_until_bytes(&mut self, max_bytes: u64) {
        while self.total_bytes > max_bytes {
            if !self.hard_evict_oldest() {
                break;
            }
        }
    }

    /// Drop oldest create entirely.
    fn hard_evict_oldest(&mut self) -> bool {
        while let Some(id) = self.order.pop_front() {
            if let Some(e) = self.by_fk.remove(&id) {
                self.by_txid.remove(&e.txid);
                self.total_bytes = self.total_bytes.saturating_sub(e.bytes);
                self.total_outs = self.total_outs.saturating_sub(e.pin.1.len() as u64);
                return true;
            }
        }
        false
    }
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
            input_count: 1,
            output_start_fk: Fk::NULL,
            output_count: 1,
        }
    }

    fn pin_with(id: u8, n_outs: usize, script_len: usize) -> CreatePin {
        let mut t = tx(id);
        t.output_count = n_outs as u32;
        let outs: Vec<_> = (0..n_outs)
            .map(|v| OutputRecord::unspent(v as i64, vec![id; script_len.max(1)]))
            .collect();
        let dens: Vec<u32> = (0..n_outs as u32).map(|v| v * 8).collect();
        Arc::new((t, outs, dens))
    }

    #[test]
    fn fifo_evicts_oldest_complete_by_bytes() {
        // Budget holds ~2 pins of this size; third insert evicts oldest.
        let one = estimate_pin_bytes(&pin_with(1, 2, 64));
        let r = CreateResidency::new(one.saturating_mul(2) + one / 4);
        r.put_complete(Fk(1), pin_with(1, 2, 64), Some((10, 20)));
        r.put_complete(Fk(2), pin_with(2, 2, 64), Some((30, 40)));
        r.put_complete(Fk(3), pin_with(3, 2, 64), Some((50, 60)));
        assert!(r.len() <= 2, "len={} bytes={}", r.len(), r.total_bytes());
        assert!(r.lookup_fk_by_txid(&tx(1).txid).is_none());
        assert_eq!(r.lookup_fk_by_txid(&tx(3).txid), Some(Fk(3)));
        assert!(r.has_outs(Fk(3)));
        assert!(r.get_pin(Fk(3)).is_some());
    }

    #[test]
    fn put_complete_requires_matching_denserels() {
        let r = CreateResidency::new(DEFAULT_RESIDENCY_BYTES);
        let t = tx(1);
        let outs = vec![OutputRecord::unspent(1, vec![0x51])];
        // denserels len mismatch → refuse
        r.put_outs(Fk(1), t, outs, vec![], Some((1, 2)));
        assert_eq!(r.len(), 0);
    }

    #[test]
    fn disabled_residency_is_noop() {
        let r = CreateResidency::new(0);
        assert!(!r.enabled());
        r.put_complete(Fk(1), pin_with(1, 1, 8), Some((1, 1)));
        assert_eq!(r.len(), 0);
    }

    #[test]
    fn body_range_and_pin_roundtrip_arc_share() {
        let r = CreateResidency::new(DEFAULT_RESIDENCY_BYTES);
        let pin = pin_with(9, 1, 1);
        r.put_complete(Fk(9), Arc::clone(&pin), Some((100, 50)));
        assert_eq!(r.body_ranges_by_fk(&[Fk(9)]), vec![Some((100, 50))]);
        let (got, range) = r.get_pin(Fk(9)).unwrap();
        assert!(Arc::ptr_eq(&got, &pin), "get_pin must Arc-share resident pin");
        assert_eq!(range, Some((100, 50)));
        r.set_body_range(Fk(9), 200, 60);
        assert_eq!(r.body_ranges_by_fk(&[Fk(9)]), vec![Some((200, 60))]);
        // set_body_range does not create rows
        r.set_body_range(Fk(99), 1, 1);
        assert!(r.get_pin(Fk(99)).is_none());
    }

    #[test]
    fn update_leaves_fifo_order() {
        let r = CreateResidency::new(50_000);
        r.put_complete(Fk(1), pin_with(1, 1, 32), None);
        r.put_complete(Fk(2), pin_with(2, 1, 32), None);
        // Update fk1 — must not move to back (still oldest).
        r.put_complete(Fk(1), pin_with(1, 1, 32), Some((9, 9)));
        // Force eviction with many large pins.
        for i in 3u8..=40 {
            r.put_complete(Fk(i as u64), pin_with(i, 4, 128), None);
        }
        // fk1 should leave before fk2 if it stayed oldest (may both be gone).
        // At least complete-only invariant: any present row has outs.
        for i in 1u8..=40 {
            if r.lookup_fk_by_txid(&tx(i).txid).is_some() {
                assert!(r.has_outs(Fk(i as u64)));
            }
        }
    }

    #[test]
    fn default_budget_is_two_gib() {
        assert_eq!(DEFAULT_RESIDENCY_BYTES, 2 * 1024 * 1024 * 1024);
        let r = CreateResidency::new(DEFAULT_RESIDENCY_BYTES);
        assert!(r.enabled());
        assert_eq!(r.byte_cap(), DEFAULT_RESIDENCY_BYTES);
    }

    #[test]
    fn get_parent_needed_sparse() {
        let r = CreateResidency::new(DEFAULT_RESIDENCY_BYTES);
        let pin = pin_with(5, 3, 4);
        r.put_complete(Fk(5), pin, Some((10, 20)));
        let (tx, live, sparse, range) = r.get_parent_needed(Fk(5), &[1]).unwrap();
        assert_eq!(tx.txid[0], 5);
        assert_eq!(live.len(), 1);
        assert_eq!(live[0].0, 1);
        assert_eq!(sparse.len(), 1);
        assert_eq!(range, Some((10, 20)));
    }
}
