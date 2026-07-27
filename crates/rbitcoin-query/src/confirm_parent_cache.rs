//! Process-local confirm parent cache (load stage).
//!
//! - **Header plans** (`headers` / `hash_to_height`): tip-GCed header + tx_fks.
//! - **Create outs FIFO** ([`crate::out_fifo::OutFifo`]): pin hits (cap 2²⁴ outs).
//! - **Plans / ready_through**: load-scanned watermark for diagnostics.
//!
//! Thin edges and sparse parent pins are **batch-local**
//! ([`crate::confirm_load::BatchThin`], [`crate::BatchParents`]) — not stored
//! here. Wire / idx→body use store (`tx.idx` / `tx.body`).

use crate::out_fifo::{is_coinbase_inputs, CreateOuts, OutFifo};
use rbitcoin_primitives::Fk;
use rbitcoin_store::{HeaderRecord, InputRecord, OutputRecord, TxRecord};
use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Mutex;

/// Cached header + body fk list for one cache height (avoids header.head/body
/// and header_txs page faults on confirm resolve).
#[derive(Debug, Clone)]
pub struct HeaderPlanCache {
    pub header_fk: Fk,
    pub header_rec: HeaderRecord,
    pub tx_fks: Vec<Fk>,
    /// Previous block hash (zeros at genesis). Filled at cache so wire rebuild
    /// never `store.get_header(prev_fk)`.
    pub prev_hash: [u8; 32],
}

/// Per-height plan: load scan watermark (ready_through) only.
#[derive(Debug, Default)]
struct HeightPlan {
    hash: [u8; 32],
    /// Load finished a body+thin+pin attempt for this height.
    scanned: bool,
}

impl HeightPlan {
    #[inline]
    fn is_ready(&self) -> bool {
        self.scanned
    }
}

struct Inner {
    /// Highest confirmed tip we have pruned to.
    tip: u32,
    /// Contiguous ready watermark: all heights in `(tip, ready_through]` are ready.
    ready_through: u32,
    /// height → plan
    plans: BTreeMap<u32, HeightPlan>,
    /// Create outs FIFO (prevout pin cache).
    outs: OutFifo,
    /// height → header + tx list.
    headers: HashMap<u32, HeaderPlanCache>,
    /// hash → height for O(1) header resolve on confirm.
    hash_to_height: HashMap<[u8; 32], u32>,
}

/// Process-local confirm parent cache.
pub struct ConfirmParentCache {
    inner: Mutex<Inner>,
    /// Mirror of `Inner::ready_through` for lock-free reads.
    ready_through: AtomicU32,
}

impl ConfirmParentCache {
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(Inner {
                tip: 0,
                ready_through: 0,
                plans: BTreeMap::new(),
                outs: OutFifo::with_env_cap(),
                headers: HashMap::new(),
                hash_to_height: HashMap::new(),
            }),
            ready_through: AtomicU32::new(0),
        }
    }

    pub fn from_env() -> Self {
        Self::new()
    }

    /// Override out-FIFO capacity (tests).
    pub fn set_out_fifo_cap(&self, cap_outs: u64) {
        let mut g = self.inner.lock().unwrap();
        g.outs = OutFifo::new(cap_outs.max(1));
    }

    /// Highest height such that every plan in `(tip, ready_through]` is ready.
    pub fn ready_through(&self) -> u32 {
        self.ready_through.load(Ordering::Relaxed)
    }

    /// Advance tip: drop plans/headers at/below tip.
    ///
    /// Create outs live in the FIFO until capacity eviction. Thin edges / sparse
    /// pins are batch-local (not here). Called from write `post_commit`.
    pub fn advance_tip(&self, tip: u32) {
        let mut g = self.inner.lock().unwrap();
        g.tip = tip;
        if g.ready_through < tip {
            g.ready_through = tip;
        }
        let drop_h: Vec<u32> = g.plans.range(..=tip).map(|(h, _)| *h).collect();
        for h in drop_h {
            g.plans.remove(&h);
        }
        let drop_hdr: Vec<u32> = g
            .headers
            .keys()
            .copied()
            .filter(|h| *h <= tip)
            .collect();
        for h in drop_hdr {
            if let Some(plan) = g.headers.remove(&h) {
                g.hash_to_height.remove(&plan.header_rec.hash);
            }
        }
        g.recompute_ready_through();
        self.ready_through
            .store(g.ready_through, Ordering::Relaxed);
    }

    /// Cache header + tx list for a cache height.
    pub fn put_header_plan(
        &self,
        height: u32,
        header_fk: Fk,
        header_rec: HeaderRecord,
        tx_fks: Vec<Fk>,
        prev_hash: [u8; 32],
    ) {
        let mut g = self.inner.lock().unwrap();
        let hash = header_rec.hash;
        g.hash_to_height.insert(hash, height);
        g.headers.insert(
            height,
            HeaderPlanCache {
                header_fk,
                header_rec,
                tx_fks,
                prev_hash,
            },
        );
    }

    pub fn get_header_by_hash(
        &self,
        hash: &[u8; 32],
    ) -> Option<(Fk, HeaderRecord)> {
        let g = self.inner.lock().unwrap();
        let h = *g.hash_to_height.get(hash)?;
        let plan = g.headers.get(&h)?;
        Some((plan.header_fk, plan.header_rec.clone()))
    }

    pub fn get_header_plan(&self, height: u32) -> Option<HeaderPlanCache> {
        self.inner.lock().unwrap().headers.get(&height).cloned()
    }

    pub fn get_tx_fks_for_hash(&self, hash: &[u8; 32]) -> Option<Vec<Fk>> {
        let g = self.inner.lock().unwrap();
        let h = *g.hash_to_height.get(hash)?;
        g.headers.get(&h).map(|p| p.tx_fks.clone())
    }

    /// Insert create outs into the FIFO (meta + outputs only; inputs used only for coinbase flag).
    ///
    /// Clears spender fields on outs. Layout (`body_range` / rels) unknown here.
    pub fn put_body(
        &self,
        fk: Fk,
        height: u32,
        tx: TxRecord,
        mut outputs: Vec<OutputRecord>,
        inputs: Vec<InputRecord>,
    ) {
        let Some(id) = fk.get() else {
            return;
        };
        let coinbase = Some(is_coinbase_inputs(&tx, &inputs));
        rbitcoin_store::clear_output_spender_fields(&mut outputs);
        let mut g = self.inner.lock().unwrap();
        let _ = g.outs.insert(
            id,
            CreateOuts::from_store(height, tx, outputs, coinbase, None, Vec::new()),
        );
    }

    /// Many creates under **one** lock (load phase-1 finish). Moves ownership.
    ///
    /// Inputs are consumed only for the coinbase flag, then dropped.
    pub fn put_bodies_batch(
        &self,
        items: Vec<(Fk, u32, TxRecord, Vec<OutputRecord>, Vec<InputRecord>)>,
    ) {
        if items.is_empty() {
            return;
        }
        let mut g = self.inner.lock().unwrap();
        for (fk, height, tx, mut outputs, inputs) in items {
            let Some(id) = fk.get() else {
                continue;
            };
            let coinbase = Some(is_coinbase_inputs(&tx, &inputs));
            rbitcoin_store::clear_output_spender_fields(&mut outputs);
            let _ = g.outs.insert(
                id,
                CreateOuts::from_store(height, tx, outputs, coinbase, None, Vec::new()),
            );
        }
    }

    /// Outs FIFO put from batch-local full bodies (load → wire keeps full Class A
    /// separately). Clones **all** outs (not need-vouts only) so later batches
    /// that spend other vouts of the same create hit the FIFO.
    ///
    /// Seeds **body_range + denserels** when present so same-batch / later-window
    /// spends hit pin_cache without re-reading `tx.idx` / denserels body.
    pub fn put_bodies_from_batch_full(&self, bodies: &crate::BatchFullBodies) {
        if bodies.is_empty() {
            return;
        }
        let mut g = self.inner.lock().unwrap();
        for (fk, body) in bodies.iter() {
            let Some(id) = fk.get() else {
                continue;
            };
            let coinbase = Some(is_coinbase_inputs(&body.tx, &body.inputs));
            let mut outputs = body.outputs.clone();
            rbitcoin_store::clear_output_spender_fields(&mut outputs);
            let denserels = body.denserels.clone();
            let _ = g.outs.insert(
                id,
                CreateOuts::from_store(
                    body.height,
                    body.tx.clone(),
                    outputs,
                    coinbase,
                    body.body_range,
                    denserels,
                ),
            );
        }
    }

    /// Lookup create fks by txid from the confirm OutFifo (read-only cross-check
    /// for archive prep). Does **not** write sticky or fill from archive.
    pub fn lookup_fk_by_txid_batch(
        &self,
        txids: &[[u8; 32]],
    ) -> HashMap<[u8; 32], Fk> {
        if txids.is_empty() {
            return HashMap::new();
        }
        let g = self.inner.lock().unwrap();
        let mut out = HashMap::with_capacity(txids.len() / 4);
        for t in txids {
            if let Some(id) = g.outs.fk_by_txid(t) {
                out.insert(*t, Fk(id));
            }
        }
        out
    }

    /// Body ranges by create fk from OutFifo (read-only; confirm pin / cross-check).
    pub fn body_ranges_by_fk(&self, fks: &[Fk]) -> Vec<Option<(u64, u64)>> {
        if fks.is_empty() {
            return Vec::new();
        }
        let g = self.inner.lock().unwrap();
        fks.iter()
            .map(|fk| {
                let id = fk.get()?;
                g.outs.get(id).and_then(|e| e.body_range())
            })
            .collect()
    }

    /// Seed FIFO with dense outs from pin_new store loads (no inputs available).
    ///
    /// Multi-in → `coinbase = Some(false)` (cheap). Single-in → `None` (write
    /// may still resolve maturity). Callers pass **all** outs + packed
    /// `body_range` + dense `spender_rels`.
    pub fn put_dense_outs_batch(
        &self,
        items: Vec<(
            Fk,
            u32,
            TxRecord,
            Vec<OutputRecord>,
            Option<(u64, u64)>,
            Vec<u32>,
        )>,
    ) {
        if items.is_empty() {
            return;
        }
        let mut g = self.inner.lock().unwrap();
        for (fk, height, tx, mut outputs, body_range, spender_rels) in items {
            let Some(id) = fk.get() else {
                continue;
            };
            rbitcoin_store::clear_output_spender_fields(&mut outputs);
            // Multi-in is never coinbase; single-in unknown without inputs.
            let coinbase = if tx.input_count != 1 {
                Some(false)
            } else {
                None
            };
            let _ = g.outs.insert(
                id,
                CreateOuts::from_store(height, tx, outputs, coinbase, body_range, spender_rels),
            );
        }
    }

    /// True if create outs are still in the FIFO.
    pub fn has_body(&self, fk: Fk) -> bool {
        let Some(id) = fk.get() else {
            return false;
        };
        self.inner.lock().unwrap().outs.contains(id)
    }

    /// Pin path: clone all outs for one create (SH collect / tests).
    pub fn get_body_for_pin(
        &self,
        fk: Fk,
    ) -> Option<(u32, TxRecord, Vec<OutputRecord>, Vec<InputRecord>)> {
        let id = fk.get()?;
        let g = self.inner.lock().unwrap();
        let e = g.outs.get(id)?;
        Some((
            e.height,
            e.tx_record(),
            e.all_output_records(),
            Vec::new(),
        ))
    }

    /// Slim pin hits under **one** lock: only clone requested outs + tx meta.
    ///
    /// Returns
    /// `id → (create_height, tx, outs, coinbase_hint, body_range, sparse_spender_rels)`.
    ///
    /// `coinbase_hint` mirrors [`CreateOuts::coinbase`] (`None` = unknown).
    pub fn get_bodies_for_pin_batch(
        &self,
        items: &[(u64, &[u32])],
    ) -> HashMap<
        u64,
        (
            u32,
            TxRecord,
            Vec<(u32, OutputRecord)>,
            Option<bool>,
            Option<(u64, u64)>,
            Vec<(u32, u32)>,
        ),
    > {
        if items.is_empty() {
            return HashMap::new();
        }
        let g = self.inner.lock().unwrap();
        let mut out = HashMap::with_capacity(items.len());
        for &(id, vouts) in items {
            let Some(e) = g.outs.get(id) else {
                continue;
            };
            let mut outs = Vec::with_capacity(vouts.len());
            for &v in vouts {
                if let Some(o) = e.get_output(v) {
                    outs.push((v, o));
                }
            }
            let dense = e.dense_spender_rels();
            let sparse = crate::batch_parents::sparse_spender_rels(&dense, vouts);
            out.insert(
                id,
                (
                    e.height,
                    e.tx_record(),
                    outs,
                    e.coinbase(),
                    e.body_range(),
                    sparse,
                ),
            );
        }
        out
    }

    /// `(create_count, total_outs, cap_outs, fifo_order_len)` for perf/tests.
    pub fn body_lru_stats(&self) -> (usize, u64, u64, usize) {
        let g = self.inner.lock().unwrap();
        (
            g.outs.len(),
            g.outs.total_outs(),
            g.outs.cap_outs(),
            g.outs.order_len(),
        )
    }

    pub fn body_count(&self) -> usize {
        self.inner.lock().unwrap().outs.len()
    }

    /// Ensure a height plan exists for `hash`.
    pub fn ensure_plan(&self, height: u32, hash: [u8; 32]) {
        let mut g = self.inner.lock().unwrap();
        if height <= g.tip {
            return;
        }
        g.plans.entry(height).or_insert_with(|| HeightPlan {
            hash,
            scanned: false,
        });
        if let Some(p) = g.plans.get_mut(&height) {
            p.hash = hash;
        }
    }

    /// Seed many plans under one lock (confirm load batch).
    pub fn ensure_plans(&self, items: &[(u32, [u8; 32])]) {
        if items.is_empty() {
            return;
        }
        let mut g = self.inner.lock().unwrap();
        for &(height, hash) in items {
            if height <= g.tip {
                continue;
            }
            g.plans.entry(height).or_insert_with(|| HeightPlan {
                hash,
                scanned: false,
            });
            if let Some(p) = g.plans.get_mut(&height) {
                p.hash = hash;
            }
        }
    }

    /// True if cache finished a scan attempt for this height.
    pub fn is_ready(&self, height: u32) -> bool {
        let g = self.inner.lock().unwrap();
        g.plans.get(&height).is_some_and(|p| p.is_ready())
    }

    /// All heights in `heights` ready (scanned).
    pub fn all_ready(&self, heights: &[u32]) -> bool {
        let g = self.inner.lock().unwrap();
        heights
            .iter()
            .all(|h| g.plans.get(h).is_some_and(|p| p.is_ready()))
    }

    /// Mark body scan complete for `height`.
    pub fn mark_scanned(&self, height: u32) {
        self.mark_scanned_many(&[height]);
    }

    /// Mark many heights scanned and recompute ready watermark once.
    pub fn mark_scanned_many(&self, heights: &[u32]) {
        if heights.is_empty() {
            return;
        }
        let mut g = self.inner.lock().unwrap();
        for &height in heights {
            if let Some(p) = g.plans.get_mut(&height) {
                p.scanned = true;
            }
        }
        g.recompute_ready_through();
        self.ready_through
            .store(g.ready_through, Ordering::Relaxed);
    }

    /// Look up a populated parent out (for wave fill / connect).
    pub fn get_parent_out(
        &self,
        fk: Fk,
        vout: u32,
    ) -> Option<(TxRecord, OutputRecord)> {
        let id = fk.get()?;
        let g = self.inner.lock().unwrap();
        let e = g.outs.get(id)?;
        let o = e.get_output(vout)?;
        Some((e.tx_record(), o))
    }

    /// True if vout is present on a cached body — no record clone.
    pub fn has_parent_out(&self, fk: Fk, vout: u32) -> bool {
        let Some(id) = fk.get() else {
            return false;
        };
        self.inner
            .lock()
            .unwrap()
            .outs
            .get(id)
            .is_some_and(|e| (vout as usize) < e.output_count())
    }

    pub fn get_parent_tx(&self, fk: Fk) -> Option<TxRecord> {
        let id = fk.get()?;
        self.inner.lock().unwrap().outs.get(id).map(|e| e.tx_record())
    }

    /// Txid of a stashed parent create — no clone of outs.
    pub fn get_parent_txid(&self, fk: Fk) -> Option<[u8; 32]> {
        let id = fk.get()?;
        self.inner.lock().unwrap().outs.get(id).map(|e| e.txid())
    }

    pub fn plan_count(&self) -> usize {
        self.inner.lock().unwrap().plans.len()
    }
}

impl Inner {
    /// Contiguous **scanned** watermark from tip+1 upward.
    fn recompute_ready_through(&mut self) {
        let mut h = self.tip.saturating_add(1);
        loop {
            match self.plans.get(&h) {
                Some(p) if p.is_ready() => h = h.saturating_add(1),
                _ => break,
            }
        }
        self.ready_through = h.saturating_sub(1);
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
            input_count: 0,
            output_start_fk: Fk(1),
            output_count: 2,
        }
    }

    fn out(v: i64) -> OutputRecord {
        OutputRecord::unspent(v, vec![0x51])
    }

    fn header_rec(hash: [u8; 32]) -> HeaderRecord {
        HeaderRecord {
            prev_fk: Fk::NULL,
            version: 1,
            timestamp: 1,
            bits: 0x1d00ffff,
            nonce: 0,
            merkle_root: [0u8; 32],
            hash,
        }
    }

    fn seed_header_and_outs(c: &ConfirmParentCache, height: u32, hash: [u8; 32], body_fk: u64) {
        c.ensure_plan(height, hash);
        c.put_header_plan(
            height,
            Fk(height as u64),
            header_rec(hash),
            vec![Fk(body_fk)],
            [0u8; 32],
        );
        let mut t = tx((body_fk & 0xff) as u8);
        t.txid = hash;
        t.input_count = 1;
        t.output_count = 1;
        let inputs = vec![InputRecord {
            prev_txid: [0u8; 32],
            create_fk: Fk::NULL,
            prev_index: u32::MAX,
            sequence: 0xffff_ffff,
            script_sig: vec![],
            witness: vec![],
        }];
        c.put_body(Fk(body_fk), height, t, vec![out(50)], inputs);
        c.mark_scanned(height);
    }

    #[test]
    fn ensure_plans_skips_at_or_below_tip() {
        let c = ConfirmParentCache::new();
        c.advance_tip(360_250);
        c.ensure_plans(&[(360_250, [1u8; 32]), (360_251, [2u8; 32])]);
        assert!(!c.is_ready(360_250));
        assert!(!c.is_ready(360_251));
        c.mark_scanned(360_251);
        assert!(c.is_ready(360_251));
        assert_eq!(c.ready_through(), 360_251);
    }

    #[test]
    fn advance_tip_prunes_plans_and_headers() {
        let c = ConfirmParentCache::new();
        c.advance_tip(0);
        c.ensure_plan(1, [1u8; 32]);
        c.put_header_plan(1, Fk(1), header_rec([1u8; 32]), vec![Fk(10)], [0u8; 32]);
        c.mark_scanned(1);
        assert!(c.get_header_plan(1).is_some());
        c.advance_tip(1);
        assert!(!c.is_ready(1));
        assert!(c.get_header_plan(1).is_none());
        assert_eq!(c.plan_count(), 0);
        assert_eq!(c.ready_through(), 1);
    }

    /// FIFO resolve + tip non-GC + slim pin + dense layout + replace + bounds + eviction.
    /// External pin path: rbitcoin-test three_stage_confirm_and_parent_pin_surface.
    #[test]
    fn out_fifo_pin_and_dense_surface() {
        let c = ConfirmParentCache::new();
        c.advance_tip(0);
        let mut t = tx(7);
        t.input_count = 1; // required for is_coinbase_inputs
        c.put_bodies_batch(vec![(
            Fk(70),
            1,
            t.clone(),
            vec![out(10), out(20), out(30)],
            vec![InputRecord {
                prev_txid: [0u8; 32],
                create_fk: Fk::NULL,
                prev_index: u32::MAX,
                sequence: u32::MAX,
                script_sig: vec![0xde; 8],
                witness: vec![],
            }],
        )]);
        assert_eq!(c.get_parent_txid(Fk(70)), Some(t.txid));
        assert_eq!(c.get_parent_out(Fk(70), 1).unwrap().1.value, 20);
        c.advance_tip(10);
        assert!(c.has_body(Fk(70))); // outs survive tip advance

        // Slim pin: only requested vouts; FIFO still holds the rest for a later need.
        let need = [0u32, 2];
        let mut hits = c.get_bodies_for_pin_batch(&[(70, &need)]);
        let (h, _txr, outs, cb, body_range, _) = hits.remove(&70).expect("hit");
        assert_eq!(h, 1);
        assert_eq!(outs.len(), 2);
        assert_eq!(outs[0].0, 0);
        assert_eq!(outs[1].0, 2);
        assert_eq!(cb, Some(true));
        assert!(body_range.is_none());
        // put_bodies_batch has no layout; txid reverse index still works for archive cross-check.
        assert_eq!(
            c.lookup_fk_by_txid_batch(&[t.txid]).get(&t.txid).copied(),
            Some(Fk(70))
        );
        assert_eq!(c.body_ranges_by_fk(&[Fk(70)]), vec![None]);
        let need1 = [1u32];
        let hits1 = c.get_bodies_for_pin_batch(&[(70, &need1)]);
        assert_eq!(hits1.get(&70).unwrap().2.len(), 1);
        assert_eq!(hits1.get(&70).unwrap().2[0].0, 1);
        assert_eq!(hits1.get(&70).unwrap().2[0].1.value, 20);

        // Batch-full seed with denserels+range ⇒ pin_cache layout_covers_need.
        {
            let mut batch = crate::BatchFullBodies::new();
            let mut t_layout = tx(71);
            t_layout.input_count = 1;
            t_layout.output_count = 2;
            batch.insert(
                Fk(71),
                2,
                t_layout.clone(),
                vec![InputRecord {
                    prev_txid: [0u8; 32],
                    create_fk: Fk::NULL,
                    prev_index: u32::MAX,
                    sequence: u32::MAX,
                    script_sig: vec![],
                    witness: vec![],
                }],
                vec![out(100), out(200)],
                Some((9000, 120)),
                vec![8, 40],
            );
            c.put_bodies_from_batch_full(&batch);
            let hits = c.get_bodies_for_pin_batch(&[(71, &[0u32, 1][..])]);
            let e = hits.get(&71).expect("layout seed");
            assert_eq!(e.4, Some((9000, 120)));
            assert_eq!(e.5, vec![(0, 8), (1, 40)]);
            assert!(crate::batch_parents::layout_covers_need(e.4, &e.5, &[0, 1]));
            assert_eq!(
                c.lookup_fk_by_txid_batch(&[t_layout.txid])
                    .get(&t_layout.txid)
                    .copied(),
                Some(Fk(71))
            );
            assert_eq!(
                c.body_ranges_by_fk(&[Fk(71)]),
                vec![Some((9000, 120))]
            );
        }

        // pin_new dense: layout + multi-in non-cb; all vouts remain addressable after slim.
        let mut t2 = tx(9);
        t2.input_count = 1;
        t2.output_count = 4;
        c.put_dense_outs_batch(vec![(
            Fk(90),
            0,
            t2,
            vec![out(1), out(2), out(3), out(4)],
            Some((5000, 100)),
            vec![10, 30, 50, 70],
        )]);
        let e = c.get_bodies_for_pin_batch(&[(90, &[3u32][..])]).remove(&90).unwrap();
        assert_eq!(e.2[0].1.value, 4);
        assert_eq!(e.3, None); // single-in pin_new: coinbase unknown without inputs
        assert_eq!(e.4, Some((5000, 100)));
        assert_eq!(e.5, vec![(3, 70)]);
        let hits_all = c.get_bodies_for_pin_batch(&[(90, &[0u32, 1, 2, 3][..])]);
        assert_eq!(hits_all.get(&90).unwrap().2.len(), 4);

        let mut t3 = tx(8);
        t3.input_count = 2;
        c.put_dense_outs_batch(vec![(
            Fk(91),
            0,
            t3,
            vec![out(9)],
            Some((6000, 50)),
            vec![12],
        )]);
        assert_eq!(
            c.get_bodies_for_pin_batch(&[(91, &[0u32][..])])
                .get(&91)
                .unwrap()
                .3,
            Some(false)
        );

        // Replace-in-place: same create_fk re-put upgrades outs without extra entry.
        c.put_bodies_batch(vec![(Fk(91), 0, {
            let mut t = tx(8);
            t.input_count = 2;
            t
        }, vec![out(9), out(10)], vec![])]);
        assert_eq!(c.body_count(), 4); // 70, 71, 90, 91 — replace-in-place, not a fifth
        assert_eq!(
            c.get_bodies_for_pin_batch(&[(91, &[0u32, 1][..])])
                .get(&91)
                .unwrap()
                .2
                .len(),
            2
        );

        // Bounds: many small creates must keep total outs ≤ cap (not only hard 2-create eviction).
        let c2 = ConfirmParentCache::new();
        c2.set_out_fifo_cap(100);
        c2.advance_tip(0);
        for i in 0..50u64 {
            let mut ti = tx((i & 0xff) as u8);
            ti.txid[0] = i as u8;
            c2.put_body(Fk(i + 1), 1, ti, vec![out(1); 4], vec![]);
        }
        let (n, total, cap, _) = c2.body_lru_stats();
        assert!(total <= cap, "total_outs={total} cap={cap} creates={n}");
        assert!(n <= 25, "creates={n}");

        // Hard eviction: oldest create dropped when a second create exceeds cap.
        c.set_out_fifo_cap(5);
        c.put_bodies_batch(vec![(Fk(1), 1, tx(1), vec![out(1), out(2), out(3)], vec![])]);
        c.put_bodies_batch(vec![(Fk(2), 2, tx(2), vec![out(4), out(5), out(6)], vec![])]);
        assert!(!c.has_body(Fk(1)));
        assert!(c.has_body(Fk(2)));
    }

    #[test]
    fn recompute_watermark_scanned() {
        let c = ConfirmParentCache::new();
        c.advance_tip(0);
        seed_header_and_outs(&c, 1, [1u8; 32], 1001);
        seed_header_and_outs(&c, 2, [2u8; 32], 1002);
        assert_eq!(c.ready_through(), 2);
        c.ensure_plan(3, [3u8; 32]);
        assert_eq!(c.ready_through(), 2);
        c.mark_scanned(3);
        assert_eq!(c.ready_through(), 3);
    }
}
