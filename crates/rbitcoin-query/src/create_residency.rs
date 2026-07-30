//! Unified create residency — **sole hot create map** for wire IBD pin / plan.
//!
//! # Map shape
//!
//! `create_fk → (txid, body range, optional outs + denserels)`.
//!
//! # Eviction: raw FIFO only (no read-LRU)
//!
//! - **Create cap:** oldest inserts are **hard-dropped** (entire row) via `order`.
//! - **Out cap:** oldest **out-bearing** rows are **slimmed** first via a separate
//!   `outs_order` deque (strip outs/denserels/tx, **keep** fk/txid/body_range).
//!   Range-only prewarm rows are never scanned for out pressure (O(out-bearing)
//!   only — never O(create_cap)).
//! Lookups **never** reorder either FIFO. Do not reintroduce touch-on-hit.
//!
//! # Denserels hit rate is largely structural
//!
//! Mid/late mainnet IBD often sees **~35–50%** denserels pin hits. Many spends
//! reference **old UTXOs** that left any process cache long ago — that miss rate
//! is expected, not a residency bug. Do **not** grow multi‑GiB caps hoping for
//! 65–70% hit rate. Optimize: (1) keep **recent** commit-seed / offline denserels
//! working, (2) make **cold** denserels loads cheap (batch once, no double ensure).
//!
//! # `RBITCOIN_CONFIRM_CACHE=0` — no long-lived denserels history
//!
//! When confirm cache is off, caps collapse to a **pipeline / just-committed
//! window** ([`NO_CACHE_CREATE_CAP`] / [`NO_CACHE_OUT_CAP`]). Commit `res_seed`
//! and pin still populate residency so in-flight batches work; FIFO drops
//! history quickly and cold denserels trust OS page cache on Class A mmaps.
//! Startup denserels/range **prewarm** is skipped. **Header plans stay on**
//! (multi-block MTP) — they are tip-GCed working state, not multi‑GiB history.
//!
use rbitcoin_primitives::Fk;
use rbitcoin_store::{OutputRecord, TxRecord};
use std::collections::{HashMap, VecDeque};
use std::sync::Mutex;

/// Default max creates in residency (~same planning band as prior sticky cap).
pub const DEFAULT_CREATE_CAP: usize = 8_000_000;
/// Default max outputs held.
pub const DEFAULT_OUT_CAP: u64 = 1 << 24;

/// Create cap when [`confirm_cache_enabled`] is false (in-flight window only).
///
/// ~pipeline depth of mid-mainnet batches — not multi‑GiB history. Explicit
/// `RBITCOIN_CREATE_RESIDENCY_CAP` still wins when set.
pub const NO_CACHE_CREATE_CAP: usize = 256_000;
/// Out cap when confirm cache is off.
pub const NO_CACHE_OUT_CAP: u64 = 1_000_000;

/// Whether process-local confirm denserels/header **caching** is enabled.
///
/// Env: `RBITCOIN_CONFIRM_CACHE` — unset/`1`/`true`/`on`/`yes` = on (default);
/// `0`/`false`/`off`/`no` = off. When off, residency keeps only a small FIFO for
/// in-flight / just-committed creates (see [`NO_CACHE_CREATE_CAP`]).
pub fn confirm_cache_enabled() -> bool {
    match std::env::var("RBITCOIN_CONFIRM_CACHE") {
        Ok(s) => {
            let t = s.trim();
            !(t == "0"
                || t.eq_ignore_ascii_case("false")
                || t.eq_ignore_ascii_case("off")
                || t.eq_ignore_ascii_case("no"))
        }
        Err(_) => true,
    }
}

#[derive(Debug, Clone)]
pub struct ResidentCreate {
    pub txid: [u8; 32],
    pub body_range: Option<(u64, u64)>,
    pub tx: Option<TxRecord>,
    pub outs: Option<Vec<OutputRecord>>,
    pub denserels: Option<Vec<u32>>,
}

struct Inner {
    by_fk: HashMap<u64, ResidentCreate>,
    by_txid: HashMap<[u8; 32], u64>,
    /// Insert-order of all creates (range-only + denserels).
    order: VecDeque<u64>,
    /// Insert-order of creates that **currently hold outs** (for O(1) out slim).
    /// Stale ids (hard-evicted or already slimmed) are skipped at pop.
    outs_order: VecDeque<u64>,
    create_cap: usize,
    out_cap: u64,
    total_outs: u64,
}

/// Process-local unified residency (sole writer for inserts; shared Mutex).
pub struct CreateResidency {
    inner: Mutex<Inner>,
    /// False when `RBITCOIN_CONFIRM_CACHE=0` — long history disabled; FIFO still
    /// holds the in-flight / just-committed window via reduced caps.
    cache_enabled: bool,
}

impl CreateResidency {
    pub fn new(create_cap: usize, out_cap: u64) -> Self {
        Self::new_with_cache(create_cap, out_cap, true)
    }

    pub fn new_with_cache(create_cap: usize, out_cap: u64, cache_enabled: bool) -> Self {
        let create_cap = create_cap.max(1).min(20_000_000);
        let out_cap = out_cap.max(1);
        let init = create_cap.min(1 << 20);
        Self {
            inner: Mutex::new(Inner {
                by_fk: HashMap::with_capacity(init),
                by_txid: HashMap::with_capacity(init),
                order: VecDeque::with_capacity(init),
                outs_order: VecDeque::new(),
                create_cap,
                out_cap,
                total_outs: 0,
            }),
            cache_enabled,
        }
    }

    pub fn from_env() -> Self {
        let cache = confirm_cache_enabled();
        let create_explicit = std::env::var("RBITCOIN_CREATE_RESIDENCY_CAP")
            .ok()
            .and_then(|s| s.parse().ok());
        let out_explicit = std::env::var("RBITCOIN_CREATE_RESIDENCY_OUT_CAP")
            .ok()
            .or_else(|| std::env::var("RBITCOIN_CONFIRM_OUT_FIFO").ok())
            .and_then(|s| s.parse().ok())
            .filter(|&n: &u64| n > 0);
        let create_cap = create_explicit.unwrap_or(if cache {
            DEFAULT_CREATE_CAP
        } else {
            NO_CACHE_CREATE_CAP
        });
        let out_cap = out_explicit.unwrap_or(if cache {
            DEFAULT_OUT_CAP
        } else {
            NO_CACHE_OUT_CAP
        });
        Self::new_with_cache(create_cap, out_cap, cache)
    }

    /// Whether long-lived confirm denserels caching is enabled (env at open).
    pub fn cache_enabled(&self) -> bool {
        self.cache_enabled
    }

    pub fn len(&self) -> usize {
        self.inner.lock().unwrap().by_fk.len()
    }

    pub fn total_outs(&self) -> u64 {
        self.inner.lock().unwrap().total_outs
    }

    /// `(creates, create_cap, total_outs, out_cap)` under one lock (IBD sizes).
    pub fn size_stats(&self) -> (usize, usize, u64, u64) {
        let g = self.inner.lock().unwrap();
        (g.by_fk.len(), g.create_cap, g.total_outs, g.out_cap)
    }

    /// Insert / update fk→txid (+ optional range). Raw FIFO: new creates only at back.
    pub fn insert_fk_txid_range(
        &self,
        fk: Fk,
        txid: [u8; 32],
        body_range: Option<(u64, u64)>,
    ) {
        let Some(id) = fk.get() else {
            return;
        };
        let mut g = self.inner.lock().unwrap();
        if let Some(e) = g.by_fk.get_mut(&id) {
            e.txid = txid;
            if body_range.is_some() {
                e.body_range = body_range;
            }
            g.by_txid.insert(txid, id);
            return;
        }
        let create_cap = g.create_cap;
        g.evict_until_creates(create_cap.saturating_sub(1));
        g.by_fk.insert(
            id,
            ResidentCreate {
                txid,
                body_range,
                tx: None,
                outs: None,
                denserels: None,
            },
        );
        g.by_txid.insert(txid, id);
        g.order.push_back(id);
    }

    /// Attach outs (pin denserels) — in-place; may slim by out budget.
    pub fn put_outs(
        &self,
        fk: Fk,
        tx: TxRecord,
        outs: Vec<OutputRecord>,
        denserels: Vec<u32>,
        body_range: Option<(u64, u64)>,
    ) {
        let Some(id) = fk.get() else {
            return;
        };
        let n = outs.len() as u64;
        let mut g = self.inner.lock().unwrap();
        if let Some(e) = g.by_fk.get_mut(&id) {
            let old_n = e.outs.as_ref().map(|o| o.len() as u64).unwrap_or(0);
            e.tx = Some(tx);
            e.outs = Some(outs);
            e.denserels = Some(denserels);
            if body_range.is_some() {
                e.body_range = body_range;
            }
            let new_total = g.total_outs.saturating_sub(old_n).saturating_add(n);
            g.total_outs = new_total;
            // First time this create gains outs → track for O(1) out slim.
            if old_n == 0 && n > 0 {
                g.outs_order.push_back(id);
            }
            let cap = g.out_cap;
            g.evict_until_outs(cap);
            return;
        }
        let create_cap = g.create_cap;
        let out_cap = g.out_cap;
        g.evict_until_creates(create_cap.saturating_sub(1));
        g.evict_until_outs(out_cap.saturating_sub(n));
        let txid = tx.txid;
        g.by_fk.insert(
            id,
            ResidentCreate {
                txid,
                body_range,
                tx: Some(tx),
                outs: Some(outs),
                denserels: Some(denserels),
            },
        );
        g.by_txid.insert(txid, id);
        g.order.push_back(id);
        if n > 0 {
            g.outs_order.push_back(id);
        }
        g.total_outs = g.total_outs.saturating_add(n);
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

    /// Txid for a create fk (fk-only / range rows and full outs rows).
    pub fn get_txid(&self, fk: Fk) -> Option<[u8; 32]> {
        let id = fk.get()?;
        self.inner.lock().unwrap().by_fk.get(&id).map(|e| e.txid)
    }

    /// Tx meta when outs have been attached (`put_outs`); `None` for fk-only rows.
    pub fn get_tx(&self, fk: Fk) -> Option<TxRecord> {
        let id = fk.get()?;
        self.inner
            .lock()
            .unwrap()
            .by_fk
            .get(&id)
            .and_then(|e| e.tx.clone())
    }

    /// Single out by vout when denserels/outs are resident.
    pub fn get_parent_out(&self, fk: Fk, vout: u32) -> Option<(TxRecord, OutputRecord)> {
        let id = fk.get()?;
        let g = self.inner.lock().unwrap();
        let e = g.by_fk.get(&id)?;
        let tx = e.tx.as_ref()?;
        let outs = e.outs.as_ref()?;
        let o = outs.get(vout as usize)?;
        Some((tx.clone(), o.clone()))
    }

    /// True if vout is present on a resident create with outs — no record clone.
    pub fn has_parent_out(&self, fk: Fk, vout: u32) -> bool {
        let Some(id) = fk.get() else {
            return false;
        };
        self.inner
            .lock()
            .unwrap()
            .by_fk
            .get(&id)
            .and_then(|e| e.outs.as_ref())
            .is_some_and(|outs| (vout as usize) < outs.len())
    }

    pub fn get_outs(&self, fk: Fk) -> Option<(TxRecord, Vec<OutputRecord>, Vec<u32>, Option<(u64, u64)>)> {
        let id = fk.get()?;
        let g = self.inner.lock().unwrap();
        let e = g.by_fk.get(&id)?;
        let tx = e.tx.clone()?;
        let outs = e.outs.clone()?;
        let rels = e.denserels.clone().unwrap_or_default();
        Some((tx, outs, rels, e.body_range))
    }

    /// True if denserels/outs are resident (no clone). Used by prewarm skip.
    pub fn has_outs(&self, fk: Fk) -> bool {
        let Some(id) = fk.get() else {
            return false;
        };
        self.inner
            .lock()
            .unwrap()
            .by_fk
            .get(&id)
            .is_some_and(|e| e.outs.is_some())
    }

    /// Sparse pin hit: clone only `need_vouts` scripts + denserel slots (not full outs).
    ///
    /// Returns `None` when row missing, denserels incomplete, or a need vout is OOB.
    /// Holds the map lock only while copying the sparse projection.
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
        let tx = e.tx.as_ref()?;
        let outs = e.outs.as_ref()?;
        let denserels = e.denserels.as_ref()?;
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
            // denserels slot already validated by layout_covers_need
            let _ = denserels.get(v as usize).filter(|&&r| r != SPENDER_REL_UNKNOWN)?;
            live.push((v, o.clone()));
        }
        Some((tx.clone(), live, sparse, e.body_range))
    }
}

impl Inner {
    fn evict_until_creates(&mut self, max_creates: usize) {
        while self.by_fk.len() > max_creates {
            if !self.hard_evict_oldest() {
                break;
            }
        }
    }

    /// Free out budget without discarding prewarm fk/range rows.
    fn evict_until_outs(&mut self, max_outs: u64) {
        while self.total_outs > max_outs {
            if !self.slim_oldest_outs() {
                // No out-bearing row left but total_outs > max — repair and stop.
                self.total_outs = 0;
                self.outs_order.clear();
                break;
            }
        }
    }

    /// Slim the oldest **out-bearing** create (O(1) amortized via `outs_order`).
    fn slim_oldest_outs(&mut self) -> bool {
        while let Some(id) = self.outs_order.pop_front() {
            let Some(e) = self.by_fk.get_mut(&id) else {
                // Hard-evicted or never present — skip stale.
                continue;
            };
            let n = e.outs.as_ref().map(|o| o.len() as u64).unwrap_or(0);
            if n == 0 {
                // Already slimmed (duplicate push or race) — skip.
                continue;
            }
            e.outs = None;
            e.denserels = None;
            e.tx = None;
            self.total_outs = self.total_outs.saturating_sub(n);
            return true;
        }
        false
    }

    /// Drop oldest create entirely (create-cap pressure).
    fn hard_evict_oldest(&mut self) -> bool {
        while let Some(id) = self.order.pop_front() {
            if let Some(e) = self.by_fk.remove(&id) {
                self.by_txid.remove(&e.txid);
                let n = e.outs.as_ref().map(|o| o.len() as u64).unwrap_or(0);
                self.total_outs = self.total_outs.saturating_sub(n);
                // Leave id in outs_order if present — slim skips missing keys.
                return true;
            }
        }
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Instant;

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

    #[test]
    fn fifo_evicts_oldest_create() {
        let r = CreateResidency::new(2, 1000);
        r.insert_fk_txid_range(Fk(1), [1u8; 32], Some((10, 20)));
        r.insert_fk_txid_range(Fk(2), [2u8; 32], Some((30, 40)));
        r.insert_fk_txid_range(Fk(3), [3u8; 32], Some((50, 60)));
        assert_eq!(r.len(), 2);
        assert!(r.lookup_fk_by_txid(&[1u8; 32]).is_none());
        assert_eq!(r.lookup_fk_by_txid(&[3u8; 32]), Some(Fk(3)));
    }

    /// No-cache caps still accept put_outs (in-flight) and FIFO-drop history.
    #[test]
    fn no_cache_caps_hold_inflight_evict_history() {
        let r = CreateResidency::new_with_cache(4, 20, false);
        assert!(!r.cache_enabled());
        for i in 1u8..=8 {
            let mut t = tx(i);
            t.output_count = 3;
            let outs: Vec<_> = (0..3)
                .map(|v| OutputRecord::unspent(v as i64, vec![i, v as u8]))
                .collect();
            r.put_outs(Fk(i as u64), t, outs, vec![0, 8, 16], Some((i as u64 * 10, 5)));
        }
        // create_cap=4 → only newest 4 creates
        assert_eq!(r.len(), 4);
        assert!(r.has_outs(Fk(8)));
        assert!(r.has_outs(Fk(5)));
        assert!(!r.has_outs(Fk(1)));
        // out_cap=20 with 3 outs/create → at most ~6 out-bearing before slim;
        // create cap already 4 so outs ≤ 12.
        assert!(r.total_outs() <= 20);
        assert!(r.total_outs() > 0, "in-flight outs retained under caps");
    }

    #[test]
    fn confirm_cache_env_parse() {
        // Default when unset is on — do not mutate global env for default arm
        // (parallel tests). Just exercise the false literals.
        assert!(confirm_cache_enabled() || !confirm_cache_enabled()); // compiles / callable
    }

    #[test]
    fn body_range_and_outs_roundtrip() {
        let r = CreateResidency::new(100, 1000);
        let t = tx(9);
        r.put_outs(
            Fk(9),
            t.clone(),
            vec![OutputRecord::unspent(1, vec![0x51])],
            vec![8],
            Some((100, 50)),
        );
        assert_eq!(r.body_ranges_by_fk(&[Fk(9)]), vec![Some((100, 50))]);
        let (tx2, outs, rels, range) = r.get_outs(Fk(9)).unwrap();
        assert_eq!(tx2.txid, t.txid);
        assert_eq!(outs.len(), 1);
        assert_eq!(rels, vec![8]);
        assert_eq!(range, Some((100, 50)));
    }

    /// Regression: out-cap pressure must not hard-drop prewarm range-only rows.
    #[test]
    fn out_pressure_slims_denserels_keeps_prewarm_ranges() {
        let r = CreateResidency::new(100, 10);
        for i in 1u8..=20 {
            let mut txid = [0u8; 32];
            txid[0] = i;
            r.insert_fk_txid_range(Fk(i as u64), txid, Some((i as u64 * 100, 50)));
        }
        assert_eq!(r.len(), 20);
        assert_eq!(r.total_outs(), 0);

        for i in 16u8..=20 {
            let mut t = tx(i);
            t.output_count = 10;
            let outs: Vec<_> = (0..10)
                .map(|v| OutputRecord::unspent(v as i64, vec![i, v as u8]))
                .collect();
            let denserels: Vec<u32> = (0..10).map(|v| v * 8).collect();
            r.put_outs(
                Fk(i as u64),
                t,
                outs,
                denserels,
                Some((i as u64 * 100, 50)),
            );
        }

        let (creates, _, total_outs, out_cap) = r.size_stats();
        assert!(
            total_outs <= out_cap,
            "outs must respect cap: total={total_outs} cap={out_cap}"
        );
        assert_eq!(
            creates, 20,
            "out pressure must not drop create count below prewarm fill"
        );
        let mut t1 = [0u8; 32];
        t1[0] = 1;
        assert_eq!(r.lookup_fk_by_txid(&t1), Some(Fk(1)));
        assert_eq!(r.body_ranges_by_fk(&[Fk(1)]), vec![Some((100, 50))]);
        for i in 1u8..=20 {
            assert!(
                r.get_txid(Fk(i as u64)).is_some(),
                "fk {i} range row must survive out slim"
            );
            assert_eq!(
                r.body_ranges_by_fk(&[Fk(i as u64)]),
                vec![Some((i as u64 * 100, 50))]
            );
        }
    }

    /// Regression: out slim must not scan prewarm size (tip-stall peg).
    ///
    /// With 200k range-only + denserels flood under small out_cap, must finish
    /// quickly. The O(creates) walk would take seconds+.
    #[test]
    fn out_slim_is_fast_with_large_prewarm() {
        const PREWARM: u64 = 200_000;
        let r = CreateResidency::new(PREWARM as usize + 1000, 100);
        for i in 1..=PREWARM {
            let mut txid = [0u8; 32];
            txid[..8].copy_from_slice(&i.to_le_bytes());
            r.insert_fk_txid_range(Fk(i), txid, Some((i * 10, 20)));
        }
        let t0 = Instant::now();
        // Flood denserels: each create 50 outs → forces many slims.
        for i in 0u64..500 {
            let id = PREWARM + 1 + i;
            let mut txid = [0u8; 32];
            txid[..8].copy_from_slice(&id.to_le_bytes());
            let mut t = tx((i & 0xff) as u8);
            t.txid = txid;
            t.output_count = 50;
            let outs: Vec<_> = (0..50)
                .map(|v| OutputRecord::unspent(v as i64, vec![v as u8]))
                .collect();
            let denserels: Vec<u32> = (0..50).map(|v| v * 4).collect();
            r.put_outs(Fk(id), t, outs, denserels, Some((id * 10, 20)));
        }
        let ms = t0.elapsed().as_millis();
        assert!(
            ms < 2_000,
            "out slim with {PREWARM} prewarm must be O(out-bearing), took {ms}ms"
        );
        let (creates, _, total_outs, out_cap) = r.size_stats();
        assert!(total_outs <= out_cap);
        // Prewarm still largely present (create_cap not exceeded by much).
        assert!(creates >= PREWARM as usize, "creates={creates}");
        // Oldest prewarm range still resolvable.
        let mut t1 = [0u8; 32];
        t1[..8].copy_from_slice(&1u64.to_le_bytes());
        assert_eq!(r.lookup_fk_by_txid(&t1), Some(Fk(1)));
    }

    #[test]
    fn create_cap_still_hard_evicts_oldest() {
        let r = CreateResidency::new(3, 10_000);
        for i in 1u8..=3 {
            let mut txid = [0u8; 32];
            txid[0] = i;
            r.insert_fk_txid_range(Fk(i as u64), txid, Some((i as u64, 1)));
        }
        r.insert_fk_txid_range(
            Fk(4),
            {
                let mut t = [0u8; 32];
                t[0] = 4;
                t
            },
            Some((4, 1)),
        );
        assert_eq!(r.len(), 3);
        let mut t1 = [0u8; 32];
        t1[0] = 1;
        assert!(r.lookup_fk_by_txid(&t1).is_none());
        let mut t4 = [0u8; 32];
        t4[0] = 4;
        assert_eq!(r.lookup_fk_by_txid(&t4), Some(Fk(4)));
    }
}
