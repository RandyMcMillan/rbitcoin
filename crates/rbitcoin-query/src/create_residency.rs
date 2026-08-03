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
//! **Deferred Arc drop (W2b.1):** eviction / pin replace only unlinks map
//! bookkeeping under the write guard; `CreatePin` Arcs are collected and
//! dropped **after** the guard is released so plan/prep are not blocked on
//! allocator free of large outs trees.
//!
//! # Concurrency (W2b.2)
//!
//! [`std::sync::RwLock`]: plan/prep **read** (`lookup_fk_by_txid`, `get_pin`, …);
//! sole Class A commit / prewarm **write** (`put_complete*`). Multiple readers
//! share; write is exclusive only for the bookkeeping window (not Arc drops).
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
use std::ops::{Deref, DerefMut};
use std::sync::atomic::Ordering;
use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::time::Instant;
#[cfg(test)]
use std::sync::Mutex;

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

/// Process-local unified residency.
///
/// **Reads** (plan/prep) take a shared `RwLock` guard; **writes** (Class A
/// res_seed / prewarm) take exclusive. Evicted pins drop after the write guard.
pub struct CreateResidency {
    inner: RwLock<Inner>,
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
            inner: RwLock::new(Inner {
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

    fn read(&self) -> TimedReadGuard<'_> {
        let t0 = Instant::now();
        let guard = self
            .inner
            .read()
            .unwrap_or_else(|e| e.into_inner());
        let wait_ns = t0.elapsed().as_nanos() as u64;
        crate::residency_lock_stats::R_WAIT_NS.fetch_add(wait_ns, Ordering::Relaxed);
        crate::residency_lock_stats::R_N.fetch_add(1, Ordering::Relaxed);
        TimedReadGuard {
            guard,
            acquired: Instant::now(),
        }
    }

    fn write(&self) -> TimedWriteGuard<'_> {
        let t0 = Instant::now();
        let guard = self
            .inner
            .write()
            .unwrap_or_else(|e| e.into_inner());
        let wait_ns = t0.elapsed().as_nanos() as u64;
        crate::residency_lock_stats::W_WAIT_NS.fetch_add(wait_ns, Ordering::Relaxed);
        crate::residency_lock_stats::W_N.fetch_add(1, Ordering::Relaxed);
        TimedWriteGuard {
            guard,
            acquired: Instant::now(),
        }
    }

    /// Whether residency inserts are enabled (`byte_cap > 0`).
    pub fn enabled(&self) -> bool {
        self.read().byte_cap > 0
    }

    pub fn len(&self) -> usize {
        self.read().by_fk.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn total_outs(&self) -> u64 {
        self.read().total_outs
    }

    pub fn total_bytes(&self) -> u64 {
        self.read().total_bytes
    }

    pub fn byte_cap(&self) -> u64 {
        self.read().byte_cap
    }

    /// `(creates, total_bytes, byte_cap, total_outs)` under one read lock (IBD sizes).
    pub fn size_stats(&self) -> (usize, u64, u64, u64) {
        let g = self.read();
        (g.by_fk.len(), g.total_bytes, g.byte_cap, g.total_outs)
    }

    /// Insert / update a **complete** pipeline create. Stores `pin` (caller
    /// `Arc::clone`s when sharing). See [`Self::put_complete_batch`].
    pub fn put_complete(&self, fk: Fk, pin: CreatePin, body_range: Option<(u64, u64)>) {
        self.put_complete_batch(&[(fk, pin, body_range)]);
    }

    /// Bulk insert / update complete pipeline creates under **one write guard**.
    ///
    /// Semantics match N× sequential [`put_complete`] in input order:
    /// incomplete denserels and null fks are skipped; updates refresh pin/range
    /// without FIFO promote; inserts append FIFO and may evict oldest by bytes.
    ///
    /// **Hot path (Class A `res_seed`):** when every entry is a **new** create_fk
    /// (no in-map update, no duplicate id in `items`), performs **one**
    /// pre-evict for the total insert bytes then bulk insert.
    ///
    /// Evicted / replaced pins are dropped **after** the write guard is released
    /// so concurrent readers are not blocked on Arc free.
    pub fn put_complete_batch(&self, items: &[(Fk, CreatePin, Option<(u64, u64)>)]) {
        if items.is_empty() {
            return;
        }
        // Pins unlinked under the write guard; Drop runs after unlock (W2b.1).
        let mut to_drop: Vec<CreatePin> = Vec::new();
        {
            let mut g = self.write();
            if g.byte_cap == 0 {
                return;
            }

            // Fast path: all-new unique fks (res_seed after Class A commit).
            if let Some(rows) = prepare_all_new_inserts(&g, items) {
                let need: u64 = rows.iter().map(|r| r.bytes).sum();
                let cap = g.byte_cap;
                g.evict_until_bytes(cap.saturating_sub(need.min(cap)), &mut to_drop);
                for row in rows {
                    g.by_fk.insert(
                        row.id,
                        ResidentCreate {
                            txid: row.txid,
                            body_range: row.body_range,
                            pin: row.pin,
                            bytes: row.bytes,
                        },
                    );
                    g.by_txid.insert(row.txid, row.id);
                    g.order.push_back(row.id);
                    g.total_bytes = g.total_bytes.saturating_add(row.bytes);
                    g.total_outs = g.total_outs.saturating_add(row.n_outs);
                }
                g.evict_until_bytes(cap, &mut to_drop);
            } else {
                // Mixed updates / duplicates / incomplete rows: ordered single applies.
                for (fk, pin, body_range) in items {
                    g.apply_put(*fk, Arc::clone(pin), *body_range, &mut to_drop);
                }
            }
        } // write guard dropped — readers unblocked
        drop(to_drop);
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
        let mut g = self.write();
        if let Some(e) = g.by_fk.get_mut(&id) {
            e.body_range = Some((off, len));
        }
    }

    pub fn body_ranges_by_fk(&self, fks: &[Fk]) -> Vec<Option<(u64, u64)>> {
        let g = self.read();
        fks.iter()
            .map(|fk| {
                let id = fk.get()?;
                g.by_fk.get(&id).and_then(|e| e.body_range)
            })
            .collect()
    }

    pub fn lookup_fk_by_txid(&self, txid: &[u8; 32]) -> Option<Fk> {
        let g = self.read();
        g.by_txid.get(txid).map(|&id| Fk(id))
    }

    pub fn get_txid(&self, fk: Fk) -> Option<[u8; 32]> {
        let id = fk.get()?;
        self.read().by_fk.get(&id).map(|e| e.txid)
    }

    /// Shared pin Arc + body range. Prefer this over deep-cloning outs.
    pub fn get_pin(&self, fk: Fk) -> Option<(CreatePin, Option<(u64, u64)>)> {
        let id = fk.get()?;
        let g = self.read();
        let e = g.by_fk.get(&id)?;
        Some((Arc::clone(&e.pin), e.body_range))
    }

    pub fn get_tx(&self, fk: Fk) -> Option<TxRecord> {
        let id = fk.get()?;
        self.read().by_fk.get(&id).map(|e| e.pin.0.clone())
    }

    pub fn get_parent_out(&self, fk: Fk, vout: u32) -> Option<(TxRecord, OutputRecord)> {
        let id = fk.get()?;
        let g = self.read();
        let e = g.by_fk.get(&id)?;
        let o = e.pin.1.get(vout as usize)?;
        Some((e.pin.0.clone(), o.clone()))
    }

    pub fn has_parent_out(&self, fk: Fk, vout: u32) -> bool {
        let Some(id) = fk.get() else {
            return false;
        };
        self.read()
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
        self.read().by_fk.contains_key(&id)
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
        let g = self.read();
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

/// Shared-lock guard that accrues hold time into [`crate::residency_lock_stats`].
struct TimedReadGuard<'a> {
    guard: RwLockReadGuard<'a, Inner>,
    acquired: Instant,
}

impl Drop for TimedReadGuard<'_> {
    fn drop(&mut self) {
        let hold = self.acquired.elapsed().as_nanos() as u64;
        if hold > 0 {
            crate::residency_lock_stats::R_HOLD_NS.fetch_add(hold, Ordering::Relaxed);
        }
    }
}

impl Deref for TimedReadGuard<'_> {
    type Target = Inner;
    fn deref(&self) -> &Inner {
        &self.guard
    }
}

/// Exclusive-lock guard that accrues hold time into [`crate::residency_lock_stats`].
struct TimedWriteGuard<'a> {
    guard: RwLockWriteGuard<'a, Inner>,
    acquired: Instant,
}

impl Drop for TimedWriteGuard<'_> {
    fn drop(&mut self) {
        let hold = self.acquired.elapsed().as_nanos() as u64;
        if hold > 0 {
            crate::residency_lock_stats::W_HOLD_NS.fetch_add(hold, Ordering::Relaxed);
        }
    }
}

impl Deref for TimedWriteGuard<'_> {
    type Target = Inner;
    fn deref(&self) -> &Inner {
        &self.guard
    }
}

impl DerefMut for TimedWriteGuard<'_> {
    fn deref_mut(&mut self) -> &mut Inner {
        &mut self.guard
    }
}

/// One validated new-row insert for the all-new batch fast path.
struct NewInsertRow {
    id: u64,
    pin: CreatePin,
    body_range: Option<(u64, u64)>,
    bytes: u64,
    n_outs: u64,
    txid: [u8; 32],
}

/// If every non-skipped item is a unique new create_fk, return prepared rows in
/// input order. `None` → caller must use ordered [`Inner::apply_put`].
fn prepare_all_new_inserts(
    g: &Inner,
    items: &[(Fk, CreatePin, Option<(u64, u64)>)],
) -> Option<Vec<NewInsertRow>> {
    let mut rows = Vec::with_capacity(items.len());
    // Track ids already accepted in this batch (duplicate fk → slow path).
    let mut seen: HashMap<u64, ()> = HashMap::with_capacity(items.len());
    for (fk, pin, body_range) in items {
        let Some(id) = fk.get() else {
            // Null fk is a no-op in apply_put; ignore for fast path.
            continue;
        };
        let (tx, outs, denserels) = pin.as_ref();
        if outs.len() != denserels.len() {
            // Incomplete denserels skipped by apply_put; treat batch as slow path
            // so mixed incomplete + valid stays ordered-equivalent.
            return None;
        }
        if g.by_fk.contains_key(&id) || seen.contains_key(&id) {
            return None;
        }
        seen.insert(id, ());
        let bytes = estimate_pin_bytes(pin);
        rows.push(NewInsertRow {
            id,
            pin: Arc::clone(pin),
            body_range: *body_range,
            bytes,
            n_outs: outs.len() as u64,
            txid: tx.txid,
        });
    }
    // Empty after skipping only nulls: nothing to do (treat as success via empty).
    Some(rows)
}

impl Inner {
    /// Single put/update (caller holds the write guard). Same rules as historical
    /// `put_complete` body. Unlinked pins are pushed to `to_drop` (not Dropped).
    fn apply_put(
        &mut self,
        fk: Fk,
        pin: CreatePin,
        body_range: Option<(u64, u64)>,
        to_drop: &mut Vec<CreatePin>,
    ) {
        let Some(id) = fk.get() else {
            return;
        };
        let (tx, outs, denserels) = pin.as_ref();
        if outs.len() != denserels.len() {
            return;
        }
        let bytes = estimate_pin_bytes(&pin);
        let n_outs = outs.len() as u64;
        let txid = tx.txid;
        if self.byte_cap == 0 {
            return;
        }
        if let Some(e) = self.by_fk.get_mut(&id) {
            let old_bytes = e.bytes;
            let old_outs = e.pin.1.len() as u64;
            e.txid = txid;
            // Defer free of previous pin Arc (may own large outs).
            to_drop.push(std::mem::replace(&mut e.pin, pin));
            e.bytes = bytes;
            if body_range.is_some() {
                e.body_range = body_range;
            }
            self.by_txid.insert(txid, id);
            self.total_bytes = self
                .total_bytes
                .saturating_sub(old_bytes)
                .saturating_add(bytes);
            self.total_outs = self.total_outs.saturating_sub(old_outs).saturating_add(n_outs);
            let cap = self.byte_cap;
            self.evict_until_bytes(cap, to_drop);
            return;
        }
        let cap = self.byte_cap;
        self.evict_until_bytes(cap.saturating_sub(bytes.min(cap)), to_drop);
        self.by_fk.insert(
            id,
            ResidentCreate {
                txid,
                body_range,
                pin,
                bytes,
            },
        );
        self.by_txid.insert(txid, id);
        self.order.push_back(id);
        self.total_bytes = self.total_bytes.saturating_add(bytes);
        self.total_outs = self.total_outs.saturating_add(n_outs);
        self.evict_until_bytes(cap, to_drop);
    }

    fn evict_until_bytes(&mut self, max_bytes: u64, to_drop: &mut Vec<CreatePin>) {
        while self.total_bytes > max_bytes {
            if !self.hard_evict_oldest(to_drop) {
                break;
            }
        }
    }

    /// Unlink oldest create; pin Arc goes to `to_drop` (Drop after write unlock).
    fn hard_evict_oldest(&mut self, to_drop: &mut Vec<CreatePin>) -> bool {
        while let Some(id) = self.order.pop_front() {
            if let Some(e) = self.by_fk.remove(&id) {
                self.by_txid.remove(&e.txid);
                self.total_bytes = self.total_bytes.saturating_sub(e.bytes);
                self.total_outs = self.total_outs.saturating_sub(e.pin.1.len() as u64);
                to_drop.push(e.pin);
                return true;
            }
        }
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering as AtomicOrdering};
    use std::thread;
    use std::time::Duration;

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

    /// Unique txid from u64 id (batch seed stress).
    fn pin_unique(id: u64, n_outs: usize, script_len: usize) -> CreatePin {
        let mut t = tx((id & 0xff) as u8);
        t.txid[..8].copy_from_slice(&id.to_le_bytes());
        t.output_count = n_outs as u32;
        let tag = (id & 0xff) as u8;
        let outs: Vec<_> = (0..n_outs)
            .map(|v| OutputRecord::unspent(v as i64, vec![tag; script_len.max(1)]))
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

    /// Snapshot for equivalence: fk → (txid0, range, pin Arc ptr, bytes, n_outs).
    fn residency_snapshot(
        r: &CreateResidency,
    ) -> std::collections::BTreeMap<u64, (u8, Option<(u64, u64)>, usize, u64, usize)> {
        let g = r.read();
        let mut m = std::collections::BTreeMap::new();
        for (&id, e) in &g.by_fk {
            m.insert(
                id,
                (
                    e.txid[0],
                    e.body_range,
                    Arc::as_ptr(&e.pin) as usize,
                    e.bytes,
                    e.pin.1.len(),
                ),
            );
        }
        // FIFO order must match between sequential and batch.
        let order: Vec<u64> = g.order.iter().copied().collect();
        assert_eq!(
            order.len(),
            m.len(),
            "FIFO length must match map (orphan order slots)"
        );
        for id in &order {
            assert!(m.contains_key(id), "FIFO id missing from map");
        }
        m
    }

    fn fifo_order(r: &CreateResidency) -> Vec<u64> {
        r.read().order.iter().copied().collect()
    }

    #[test]
    fn put_complete_batch_matches_sequential_inserts() {
        let one = estimate_pin_bytes(&pin_with(1, 2, 64));
        // Room for ~3 pins; inserts 5 → both paths evict same oldest.
        let cap = one.saturating_mul(3) + one / 4;
        let items: Vec<_> = (1u8..=5)
            .map(|i| {
                (
                    Fk(i as u64),
                    pin_with(i, 2, 64),
                    Some((i as u64 * 10, 8u64)),
                )
            })
            .collect();

        let seq = CreateResidency::new(cap);
        for (fk, pin, range) in &items {
            seq.put_complete(*fk, Arc::clone(pin), *range);
        }

        let batch = CreateResidency::new(cap);
        let batch_items: Vec<_> = items
            .iter()
            .map(|(fk, pin, range)| (*fk, Arc::clone(pin), *range))
            .collect();
        batch.put_complete_batch(&batch_items);

        assert_eq!(seq.len(), batch.len());
        assert_eq!(seq.total_bytes(), batch.total_bytes());
        assert_eq!(fifo_order(&seq), fifo_order(&batch));
        // Same fks / ranges / txid tags (Arc ptrs differ — separate clones).
        let s = residency_snapshot(&seq);
        let b = residency_snapshot(&batch);
        assert_eq!(s.len(), b.len());
        for (id, (txid, range, _, bytes, n_outs)) in &s {
            let (bt, br, _, bb, bn) = b.get(id).expect("batch missing fk");
            assert_eq!(txid, bt);
            assert_eq!(range, br);
            assert_eq!(bytes, bb);
            assert_eq!(n_outs, bn);
        }
        // Oldest should be gone under tight cap.
        assert!(seq.lookup_fk_by_txid(&tx(1).txid).is_none());
        assert_eq!(seq.lookup_fk_by_txid(&tx(5).txid), Some(Fk(5)));
    }

    #[test]
    fn put_complete_batch_update_leaves_fifo_like_sequential() {
        let r_seq = CreateResidency::new(80_000);
        let r_bat = CreateResidency::new(80_000);
        let p1 = pin_with(1, 1, 16);
        let p2 = pin_with(2, 1, 16);
        r_seq.put_complete(Fk(1), Arc::clone(&p1), None);
        r_seq.put_complete(Fk(2), Arc::clone(&p2), None);
        r_bat.put_complete_batch(&[
            (Fk(1), Arc::clone(&p1), None),
            (Fk(2), Arc::clone(&p2), None),
        ]);
        // Update fk1 via batch (slow path: already resident).
        let p1b = pin_with(1, 1, 16);
        r_seq.put_complete(Fk(1), Arc::clone(&p1b), Some((9, 9)));
        r_bat.put_complete_batch(&[(Fk(1), Arc::clone(&p1b), Some((9, 9)))]);
        assert_eq!(fifo_order(&r_seq), fifo_order(&r_bat));
        assert_eq!(
            r_seq.body_ranges_by_fk(&[Fk(1)]),
            r_bat.body_ranges_by_fk(&[Fk(1)])
        );
        assert_eq!(fifo_order(&r_seq), vec![1, 2], "update must not promote");
    }

    #[test]
    fn put_complete_batch_arc_shares_seed_pins() {
        // res_seed path: Arc::clone of batch_pin into residency.
        let r = CreateResidency::new(DEFAULT_RESIDENCY_BYTES);
        let pins: Vec<CreatePin> = (1u8..=4).map(|i| pin_with(i, 1, 8)).collect();
        let items: Vec<_> = pins
            .iter()
            .enumerate()
            .map(|(i, p)| (Fk(i as u64 + 1), Arc::clone(p), Some((i as u64, 1))))
            .collect();
        r.put_complete_batch(&items);
        for (i, p) in pins.iter().enumerate() {
            let (got, _) = r.get_pin(Fk(i as u64 + 1)).unwrap();
            assert!(
                Arc::ptr_eq(&got, p),
                "batch seed must Arc-share input pin"
            );
        }
    }

    /// W2b.1: last Arc of an evicted pin is released after the put returns
    /// (strong_count observation via Weak).
    #[test]
    fn eviction_releases_evicted_pin_after_put() {
        let one = estimate_pin_bytes(&pin_with(1, 2, 64));
        let r = CreateResidency::new(one.saturating_mul(2) + one / 4);
        let p1 = pin_with(1, 2, 64);
        let weak = Arc::downgrade(&p1);
        r.put_complete(Fk(1), p1, Some((1, 1)));
        // Only residency holds the pin.
        assert_eq!(weak.strong_count(), 1);
        r.put_complete(Fk(2), pin_with(2, 2, 64), Some((2, 1)));
        r.put_complete(Fk(3), pin_with(3, 2, 64), Some((3, 1)));
        // fk1 evicted; after put returns, deferred drop has run.
        assert_eq!(weak.strong_count(), 0, "evicted pin must drop after unlock");
        assert!(weak.upgrade().is_none());
    }

    /// W2b.2: readers make progress while a writer seeds large batches (full FIFO).
    #[test]
    fn concurrent_readers_during_batch_seed() {
        let one = estimate_pin_bytes(&pin_unique(1, 2, 48));
        // ~40 pins capacity → batch seeds force eviction churn.
        let cap = one.saturating_mul(40);
        let r = Arc::new(CreateResidency::new(cap));

        // Warm a stable window of fks 1..16 for readers.
        for i in 1u64..=16 {
            r.put_complete(Fk(i), pin_unique(i, 2, 48), Some((i, 1)));
        }

        let stop = Arc::new(AtomicBool::new(false));
        let ops = Arc::new(AtomicU64::new(0));
        let mut readers = Vec::new();
        for _ in 0..2 {
            let r_r = Arc::clone(&r);
            let stop_r = Arc::clone(&stop);
            let ops_r = Arc::clone(&ops);
            readers.push(thread::spawn(move || {
                while !stop_r.load(AtomicOrdering::Relaxed) {
                    for i in 1u64..=16 {
                        let _ = r_r.get_pin(Fk(i));
                        let mut tid = [0u8; 32];
                        tid[..8].copy_from_slice(&i.to_le_bytes());
                        let _ = r_r.lookup_fk_by_txid(&tid);
                        ops_r.fetch_add(1, AtomicOrdering::Relaxed);
                    }
                    thread::yield_now();
                }
            }));
        }

        // Writer: many large all-new batches (fast path + eviction).
        for round in 0u64..30 {
            let items: Vec<_> = (0u64..25)
                .map(|j| {
                    let id = 1_000 + round * 25 + j;
                    (
                        Fk(id),
                        pin_unique(id, 2, 48),
                        Some((id, 1u64)),
                    )
                })
                .collect();
            r.put_complete_batch(&items);
        }

        // Let readers spin a bit after last write.
        thread::sleep(Duration::from_millis(20));
        stop.store(true, AtomicOrdering::Relaxed);
        for t in readers {
            t.join().expect("reader thread");
        }
        let n = ops.load(AtomicOrdering::Relaxed);
        assert!(
            n > 100,
            "readers must make progress during concurrent seed (ops={n})"
        );
    }
}
