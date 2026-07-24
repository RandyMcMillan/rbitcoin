//! Block-structured **confirm parent cache**.
//!
//! Load-stage strategy (no background worker):
//! - **RAM-cache** small lookups: header + `header_txs`, body ranges, thin edges.
//! - **Create outs FIFO** ([`crate::out_fifo::OutFifo`]): tx meta + outputs only
//!   for parent pin / prevout resolve (cap 2²⁴ outs by default). No full-body
//!   inputs/witness; wire rebuild falls back to store (+ cached body range).
//! - Eviction is plain FIFO by create (oldest whole create dropped) — no tip
//!   reaccount / full-map body scans.
//!
//! - Parent pin uses create_fk; no process-local txid→fk map.
//! - A height is **ready** once scanned (load finished for that height).
//! - Prevouts use stamped create_fk only.

use rbitcoin_primitives::Fk;
use crate::out_fifo::{is_coinbase_inputs, CreateOuts, OutFifo};
use rbitcoin_store::{HeaderRecord, InputRecord, OutputRecord, TxRecord};
// ThinInput used via StashedThinInput alias.
use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Condvar, Mutex};
use std::time::{Duration, Instant};

/// Thin create-fk edge (identical to wave [`crate::wave_prevout::ThinInput`]).
pub type StashedThinInput = crate::wave_prevout::ThinInput;

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
    /// Load finished a body+thin+pin attempt for this height (2-stage wait bit).
    scanned: bool,
}

impl HeightPlan {
    /// Runway attempt finished — O(1). Used by wait / ready_through (2-stage).
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
    /// Thin edges without a full body parse.
    thin_edges: HashMap<u64, Vec<StashedThinInput>>,
    /// height → header + tx list.
    headers: HashMap<u32, HeaderPlanCache>,
    /// hash → height for O(1) header resolve on confirm.
    hash_to_height: HashMap<[u8; 32], u32>,
    /// fk id → absolute body (offset, len) from tx.idx.
    body_range: HashMap<u64, (u64, u64)>,
}

/// Process-local confirm parent cache.
pub struct ConfirmParentCache {
    inner: Mutex<Inner>,
    /// Signaled when plans become ready (`mark_scanned*`) or tip GC advances
    /// readiness — confirm waits here instead of spinning / last-mile load.
    ready_cv: Condvar,
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
                thin_edges: HashMap::new(),
                headers: HashMap::new(),
                hash_to_height: HashMap::new(),
                body_range: HashMap::new(),
            }),
            ready_cv: Condvar::new(),
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

    /// Advance tip: drop plans/headers/thin at/below tip.
    ///
    /// Create outs live in the FIFO until evicted by capacity — no tip reaccount.
    /// Called from write `post_commit` after Class C + spend annotate.
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
        // Drop header plans + thin edges for confirmed heights (O(batch txs)).
        let drop_hdr: Vec<u32> = g
            .headers
            .keys()
            .copied()
            .filter(|h| *h <= tip)
            .collect();
        for h in drop_hdr {
            if let Some(plan) = g.headers.remove(&h) {
                g.hash_to_height.remove(&plan.header_rec.hash);
                for fk in &plan.tx_fks {
                    if let Some(id) = fk.get() {
                        g.thin_edges.remove(&id);
                    }
                }
            }
        }
        // Optional: drop outs for creates at/below tip (FIFO still has hard cap).
        // Keep them for pin hits — only capacity evicts. (No O(map) scan required.)
        g.recompute_ready_through();
        self.ready_through
            .store(g.ready_through, Ordering::Relaxed);
        drop(g);
        self.ready_cv.notify_all();
    }


    /// Wake any thread blocked in [`Self::wait_heights_ready`] (cancel / shutdown).
    pub fn notify_ready_waiters(&self) {
        self.ready_cv.notify_all();
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

    /// Cache `tx.idx` body range for `fk`.
    pub fn put_body_range(&self, fk: Fk, offset: u64, len: u64) {
        let Some(id) = fk.get() else {
            return;
        };
        self.inner
            .lock()
            .unwrap()
            .body_range
            .insert(id, (offset, len));
    }

    pub fn get_body_range(&self, fk: Fk) -> Option<(u64, u64)> {
        let id = fk.get()?;
        self.inner.lock().unwrap().body_range.get(&id).copied()
    }

    /// Batch body ranges under one lock.
    pub fn put_body_ranges_batch(&self, items: &[(Fk, u64, u64)]) {
        if items.is_empty() {
            return;
        }
        let mut g = self.inner.lock().unwrap();
        for &(fk, off, len) in items {
            if let Some(id) = fk.get() {
                g.body_range.insert(id, (off, len));
            }
        }
    }


    /// Insert create outs into the FIFO (meta + outputs only; inputs used only for coinbase flag).
    pub fn put_body(
        &self,
        fk: Fk,
        height: u32,
        tx: TxRecord,
        outputs: Vec<OutputRecord>,
        inputs: Vec<InputRecord>,
    ) {
        let Some(id) = fk.get() else {
            return;
        };
        let is_coinbase = is_coinbase_inputs(&tx, &inputs);
        let mut g = self.inner.lock().unwrap();
        let evicted = g.outs.insert(
            id,
            CreateOuts {
                height,
                tx,
                outputs,
                is_coinbase,
            },
        );
        for eid in evicted {
            g.body_range.remove(&eid);
        }
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
        for (fk, height, tx, outputs, inputs) in items {
            let Some(id) = fk.get() else {
                continue;
            };
            let is_coinbase = is_coinbase_inputs(&tx, &inputs);
            let evicted = g.outs.insert(
                id,
                CreateOuts {
                    height,
                    tx,
                    outputs,
                    is_coinbase,
                },
            );
            for eid in evicted {
                g.body_range.remove(&eid);
            }
        }
    }

    /// Alias: same as [`Self::put_body`] (creates = body outs).
    pub fn put_body_and_creates(
        &self,
        fk: Fk,
        height: u32,
        tx: TxRecord,
        outputs: Vec<OutputRecord>,
        inputs: Vec<InputRecord>,
    ) {
        self.put_body(fk, height, tx, outputs, inputs);
    }

    /// Full Class A body is **not** retained (outs-only FIFO). Always `None`.
    /// Wire rebuild uses store (+ [`Self::get_body_range`]).
    pub fn get_body(
        &self,
        _fk: Fk,
    ) -> Option<(TxRecord, Vec<OutputRecord>, Vec<InputRecord>)> {
        None
    }

    /// True if create outs are still in the FIFO.
    pub fn has_body(&self, fk: Fk) -> bool {
        let Some(id) = fk.get() else {
            return false;
        };
        self.inner.lock().unwrap().outs.contains(id)
    }

    /// Full inputs are not retained — always `None` (caller re-decodes for thin).
    pub fn body_prevout_edges(
        &self,
        _fk: Fk,
    ) -> Option<([u8; 32], Vec<(Option<u64>, [u8; 32], u32)>)> {
        None
    }

    /// Compatibility: full body not held.
    pub fn take_body(
        &self,
        _fk: Fk,
    ) -> Option<(TxRecord, Vec<OutputRecord>, Vec<InputRecord>)> {
        None
    }

    pub fn take_bodies_batch(
        &self,
        fks: &[Fk],
    ) -> HashMap<u64, (TxRecord, Vec<OutputRecord>, Vec<InputRecord>)> {
        self.get_bodies_batch(fks)
    }

    /// Full bodies not held — empty map (wire/reconstruct use store).
    pub fn get_bodies_batch(
        &self,
        _fks: &[Fk],
    ) -> HashMap<u64, (TxRecord, Vec<OutputRecord>, Vec<InputRecord>)> {
        HashMap::new()
    }

    /// Pin path (legacy full-body shape) — not used; prefer [`Self::get_bodies_for_pin_batch`].
    pub fn get_body_for_pin(
        &self,
        fk: Fk,
    ) -> Option<(u32, TxRecord, Vec<OutputRecord>, Vec<InputRecord>)> {
        let id = fk.get()?;
        let g = self.inner.lock().unwrap();
        let e = g.outs.get(id)?;
        Some((e.height, e.tx.clone(), e.outputs.clone(), Vec::new()))
    }

    /// Slim pin hits under **one** lock: only clone requested outs + tx meta.
    ///
    /// Returns `id → (create_height, tx, outs, coinbase_hint, body_range)`.
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
                if let Some(o) = e.outputs.get(v as usize) {
                    outs.push((v, o.clone()));
                }
            }
            let cb_hint = if e.tx.input_count != 1 {
                Some(false)
            } else if e.is_coinbase {
                Some(true)
            } else {
                // 1-in non-cb or unknown — let caller resolve if needed
                Some(false)
            };
            let range = g.body_range.get(&id).copied();
            out.insert(id, (e.height, e.tx.clone(), outs, cb_hint, range));
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
            g.outs.len(), // order length ≈ creates (stale skips rare)
        )
    }

    /// Clone thin edges under one lock (keeps stash for retries).
    pub fn get_thin_inputs_batch(&self, fks: &[Fk]) -> HashMap<u64, Vec<StashedThinInput>> {
        if fks.is_empty() {
            return HashMap::new();
        }
        let g = self.inner.lock().unwrap();
        let mut out = HashMap::with_capacity(fks.len());
        for &fk in fks {
            let Some(id) = fk.get() else {
                continue;
            };
            if let Some(edges) = g.thin_edges.get(&id) {
                out.insert(id, edges.clone());
            }
        }
        out
    }

    /// True if every Class A body in `tx_fks` is still fully decoded on the parent cache.
    ///
    /// Used to re-cache heights that were `mark_scanned` but later drained
    /// (e.g. historical `take_bodies_batch` on a failed confirm).
    pub fn bodies_complete(&self, tx_fks: &[Fk]) -> bool {
        if tx_fks.is_empty() {
            return false;
        }
        let g = self.inner.lock().unwrap();
        tx_fks.iter().all(|fk| {
            fk.get()
                .map(|id| g.outs.contains(id))
                .unwrap_or(false)
        })
    }

    /// Attach load-resolved thin edges (assemble reads these; no full body required).
    ///
    /// Stored only in `thin_edges` (not dual-copied onto optional `by_body`).
    pub fn put_thin_inputs(&self, fk: Fk, edges: Vec<StashedThinInput>) {
        let Some(id) = fk.get() else {
            return;
        };
        self.inner.lock().unwrap().thin_edges.insert(id, edges);
    }

    /// Thin edges stashed during cache, if present (clone).
    pub fn get_thin_inputs(&self, fk: Fk) -> Option<Vec<StashedThinInput>> {
        let id = fk.get()?;
        self.inner.lock().unwrap().thin_edges.get(&id).cloned()
    }

    /// Remove thin edges for `fk` (tests / explicit drain).
    pub fn take_thin_inputs(&self, fk: Fk) -> Option<Vec<StashedThinInput>> {
        let id = fk.get()?;
        self.inner.lock().unwrap().thin_edges.remove(&id)
    }

    /// Remove many thin-edge lists under **one** lock (tests).
    pub fn take_thin_inputs_batch(&self, fks: &[Fk]) -> HashMap<u64, Vec<StashedThinInput>> {
        if fks.is_empty() {
            return HashMap::new();
        }
        let mut g = self.inner.lock().unwrap();
        let mut out = HashMap::with_capacity(fks.len());
        for &fk in fks {
            let Some(id) = fk.get() else {
                continue;
            };
            if let Some(edges) = g.thin_edges.remove(&id) {
                out.insert(id, edges);
            }
        }
        out
    }

    pub fn body_count(&self) -> usize {
        self.inner.lock().unwrap().outs.len()
    }

    /// Ensure a height plan exists for `hash`.
    ///
    /// Does not recompute `ready_through` (plan is unscanned). Caller finishes
    /// with [`Self::mark_scanned`] / [`Self::mark_scanned_many`].
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

    /// True if cache finished a scan attempt for this height (2-stage wait).
    pub fn is_ready(&self, height: u32) -> bool {
        let g = self.inner.lock().unwrap();
        g.plans.get(&height).is_some_and(|p| p.is_ready())
    }

    /// Content-complete package (header+bodies+edges+pinned parents). Diagnostic
    /// / optional strict claim; **wait uses [`Self::is_ready`]** like 2-stage.
    pub fn package_ready(&self, height: u32) -> bool {
        self.inner.lock().unwrap().package_ready(height)
    }

    /// All heights in `heights` ready (scanned — 2-stage wait).
    pub fn all_ready(&self, heights: &[u32]) -> bool {
        let g = self.inner.lock().unwrap();
        heights
            .iter()
            .all(|h| g.plans.get(h).is_some_and(|p| p.is_ready()))
    }

    /// Confirm headroom: warmer has fully ready plans through at least
    /// `batch_end + headroom`, or through the furthest **seeded** plan if the
    /// cache is shorter (archive lag / depth edge).
    ///
    /// IBD should [`Self::ensure_plan`] the full published cache so unfinished
    /// heights appear as plans (not "missing" → falsely satisfied). When the
    /// furthest plan is already ready, headroom is satisfied even if
    /// `ready_through < batch_end + headroom` (nothing further to warm).
    pub fn headroom_ready(&self, batch_end: u32, headroom: u32) -> bool {
        let g = self.inner.lock().unwrap();
        if batch_end <= g.tip {
            return true;
        }
        // Batch itself must be under the contiguous watermark.
        if g.ready_through < batch_end {
            return false;
        }
        if headroom == 0 {
            return true;
        }
        let target = batch_end.saturating_add(headroom);
        if g.ready_through >= target {
            return true;
        }
        // Short cache: every seeded plan is ready — archive lag / depth edge.
        let max_plan = g.plans.keys().next_back().copied().unwrap_or(g.tip);
        g.ready_through >= max_plan
    }

    /// Mark body scan complete for `height` (after registering needs/fills).
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
        drop(g);
        self.ready_cv.notify_all();
    }

    /// Recompute contiguous package watermark from tip (no scan flags).
    ///
    /// Call after cache mutations that can invalidate packages without going
    /// through [`Self::mark_scanned_many`] (e.g. parent pin fill mid-flight).
    pub fn recompute_ready_watermark(&self) {
        let mut g = self.inner.lock().unwrap();
        g.recompute_ready_through();
        self.ready_through
            .store(g.ready_through, Ordering::Relaxed);
        drop(g);
        self.ready_cv.notify_all();
    }

    /// Block until every height in `heights` is ready, `cancelled` returns true,
    /// or `timeout` elapses.
    ///
    /// Uses [`Self::ready_cv`] — woken by [`Self::mark_scanned_many`] / tip GC /
    /// [`Self::notify_ready_waiters`]. Does **not** perform cache work.
    ///
    /// Returns `Ok(())` when ready, `Err(true)` if cancelled, `Err(false)` on timeout.
    pub fn wait_heights_ready(
        &self,
        heights: &[u32],
        timeout: Duration,
        mut cancelled: impl FnMut() -> bool,
    ) -> Result<(), bool> {
        if heights.is_empty() {
            return Ok(());
        }
        let start = Instant::now();
        let mut g = self.inner.lock().unwrap();
        loop {
            // 2-stage semantics: scanned only (O(1)). Content completeness is
            // cache's job; wave store-fallbacks residual misses like before.
            let ready = heights
                .iter()
                .all(|h| g.plans.get(h).is_some_and(|p| p.is_ready()));
            if ready {
                return Ok(());
            }
            if cancelled() {
                return Err(true);
            }
            let elapsed = start.elapsed();
            if elapsed >= timeout {
                return Err(false);
            }
            // Cap wait slices so cancel is observed promptly.
            let slice = (timeout - elapsed).min(Duration::from_millis(100));
            let (guard, _) = self
                .ready_cv
                .wait_timeout(g, slice)
                .expect("confirm parent ready_cv");
            g = guard;
        }
    }






    /// Batch thin edges under one lock (moves ownership — no edge clone).
    pub fn put_thin_inputs_batch(&self, items: Vec<(Fk, Vec<StashedThinInput>)>) {
        if items.is_empty() {
            return;
        }
        let mut g = self.inner.lock().unwrap();
        for (fk, edges) in items {
            let Some(id) = fk.get() else {
                continue;
            };
            g.thin_edges.insert(id, edges);
        }
    }


    /// Look up a populated parent out (for wave fill / connect).
    ///
    /// Cache **body** outs only (sparse pins live on per-batch [`crate::BatchParents`]).
    pub fn get_parent_out(
        &self,
        fk: Fk,
        vout: u32,
    ) -> Option<(TxRecord, OutputRecord)> {
        let id = fk.get()?;
        let g = self.inner.lock().unwrap();
        let e = g.outs.get(id)?;
        let o = e.outputs.get(vout as usize)?;
        Some((e.tx.clone(), o.clone()))
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
            .is_some_and(|e| (vout as usize) < e.outputs.len())
    }










    pub fn get_parent_tx(&self, fk: Fk) -> Option<TxRecord> {
        let id = fk.get()?;
        self.inner.lock().unwrap().outs.get(id).map(|e| e.tx.clone())
    }


    /// Txid of a stashed parent create body — no clone of outs.
    pub fn get_parent_txid(&self, fk: Fk) -> Option<[u8; 32]> {
        let id = fk.get()?;
        self.inner.lock().unwrap().outs.get(id).map(|e| e.tx.txid)
    }




    pub fn plan_count(&self) -> usize {
        self.inner.lock().unwrap().plans.len()
    }


    /// Cached body-range entries (idx offsets for spent filter / annotate).
    pub fn body_range_count(&self) -> usize {
        self.inner.lock().unwrap().body_range.len()
    }



}

impl Inner {
    /// Contiguous **scanned** watermark from tip+1 upward (2-stage ready).
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

    /// Content-complete package (header + bodies + edges + external parents).
    ///
    /// Optional strict check (diagnostics / tests). Wait / ready_through use
    /// scanned-only [`HeightPlan::is_ready`]. Content check: header + all wave
    /// bodies present and non-coinbase inputs have stamped create_fk (thin or
    /// body). External spent-filtered pins live on per-batch [`crate::BatchParents`]
    /// and are not required here.
    fn package_ready(&self, height: u32) -> bool {
        let Some(plan) = self.plans.get(&height) else {
            return false;
        };
        if !plan.scanned {
            return false;
        }
        let Some(hdr) = self.headers.get(&height) else {
            return false;
        };
        if hdr.header_rec.hash != plan.hash || hdr.tx_fks.is_empty() {
            return false;
        }
        for fk in &hdr.tx_fks {
            let Some(id) = fk.get() else {
                return false;
            };
            // Outs present (or thin for coinbase-only).
            if !self.outs.contains(id) && !self.thin_edges.contains_key(&id) {
                return false;
            }
        }
        true
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

    /// Minimal coinbase-only package: package_ready without external parents.
    fn seed_coinbase_package(c: &ConfirmParentCache, height: u32, hash: [u8; 32], body_fk: u64) {
        c.ensure_plan(height, hash);
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
        c.put_header_plan(
            height,
            Fk(height as u64),
            header_rec(hash),
            vec![Fk(body_fk)],
            [0u8; 32],
        );
        c.put_body(Fk(body_fk), height, t, vec![out(50)], inputs);
        c.mark_scanned(height);
    }


    #[test]
    fn hollow_mark_scanned_is_not_package_ready() {
        let c = ConfirmParentCache::new();
        c.advance_tip(10);
        c.ensure_plan(11, [9u8; 32]);
        c.mark_scanned(11);
        // Wait uses scanned (2-stage); package_ready stays content-strict.
        assert!(c.is_ready(11));
        assert_eq!(c.ready_through(), 11);
        assert!(
            !c.package_ready(11),
            "scanned without bodies is not content-complete"
        );
    }

    /// Wait / ready_through use scanned (2-stage), not content package_ready.
    #[test]
    fn scanned_unblocks_wait_without_package_ready_content() {
        let c = ConfirmParentCache::new();
        c.advance_tip(10);
        c.ensure_plan(11, [9u8; 32]);
        // No bodies — package_ready false, but scanned is the wait bit.
        c.mark_scanned(11);
        assert!(c.is_ready(11));
        assert_eq!(c.ready_through(), 11);
        assert!(
            !c.package_ready(11),
            "package_ready still content-strict for diagnostics"
        );
    }

    /// Wave clones bodies; package_ready still sees them. Scanned watermark stays.
    #[test]
    fn recompute_watermark_scanned_not_content() {
        let c = ConfirmParentCache::new();
        c.advance_tip(10);
        seed_coinbase_package(&c, 11, [0x11; 32], 1100);
        seed_coinbase_package(&c, 12, [0x12; 32], 1200);
        assert_eq!(c.ready_through(), 12);
        let _ = c.take_bodies_batch(&[Fk(1200)]);
        // Bodies remain for parent LRU — package_ready still true.
        assert!(c.package_ready(12));
        c.recompute_ready_watermark();
        assert_eq!(
            c.ready_through(),
            12,
            "scanned watermark ignores content drain (wave store-fallbacks)"
        );
    }

    /// ensure_plans ignores heights ≤ tip; after advance_tip, batch heights seed.
    #[test]
    fn ensure_plans_skips_at_or_below_tip() {
        let c = ConfirmParentCache::new();
        c.advance_tip(360_250);
        c.ensure_plans(&[
            (360_250, [0u8; 32]), // at tip — skip
            (360_251, [1u8; 32]),
            (360_252, [2u8; 32]),
        ]);
        assert_eq!(c.plan_count(), 2);
        seed_coinbase_package(&c, 360_251, [1u8; 32], 360_251);
        seed_coinbase_package(&c, 360_252, [2u8; 32], 360_252);
        assert!(c.is_ready(360_251));
        assert!(c.is_ready(360_252));
        assert_eq!(c.ready_through(), 360_252);
    }

    #[test]
    fn wait_heights_ready_notified_by_mark_scanned() {
        use std::sync::Arc;
        use std::thread;
        use std::time::Duration;

        let c = Arc::new(ConfirmParentCache::new());
        c.advance_tip(10);
        let hash = [9u8; 32];
        c.ensure_plan(11, hash);

        let waiter = Arc::clone(&c);
        let j = thread::spawn(move || {
            waiter
                .wait_heights_ready(&[11], Duration::from_millis(500), || false)
                .expect("should become ready")
        });
        thread::yield_now();
        thread::sleep(Duration::from_millis(1));
        // Deliver full package then notify via mark_scanned.
        seed_coinbase_package(&c, 11, hash, 1111);
        j.join().unwrap();
        assert!(c.is_ready(11));
    }


    #[test]
    fn pin_batch_hits_out_fifo() {
        let c = ConfirmParentCache::new();
        c.advance_tip(0);
        let mut t = tx(1);
        t.txid = [9u8; 32];
        c.put_body(
            Fk(100),
            1,
            t,
            vec![out(50), out(60)],
            vec![InputRecord {
                prev_txid: [0u8; 32],
                create_fk: Fk::NULL,
                prev_index: u32::MAX,
                sequence: 0xffff_ffff,
                script_sig: vec![],
                witness: vec![],
            }],
        );
        let need = [0u32, 1];
        let hits = c.get_bodies_for_pin_batch(&[(100, &need)]);
        assert_eq!(hits.len(), 1);
        assert_eq!(hits.get(&100).unwrap().2.len(), 2);
    }





    #[test]
    fn out_fifo_survives_past_tip() {
        let c = ConfirmParentCache::new();
        c.advance_tip(0);
        let t = tx(5);
        c.put_body(
            Fk(50),
            1,
            t.clone(),
            vec![out(42)],
            vec![],
        );
        // Full body API is outs-only — get_body always None.
        assert!(c.get_body(Fk(50)).is_none());
        assert!(c.has_body(Fk(50)));
        c.advance_tip(1);
        assert!(c.has_body(Fk(50)));
        assert!(c.get_body_for_pin(Fk(50)).is_some());
    }

    /// Parent pin body_range must not accumulate forever across tip advances.

    #[test]
    fn body_prevout_edges_prefers_create_fk_without_soft_txid() {
        let c = ConfirmParentCache::new();
        c.advance_tip(0);
        let t = tx(9);
        let inputs = vec![
            InputRecord {
                prev_txid: [0u8; 32],
                create_fk: Fk(3),
                prev_index: 1,
                sequence: 0xffff_ffff,
                script_sig: vec![],
                witness: vec![],
            },
            InputRecord {
                prev_txid: [0u8; 32],
                create_fk: Fk::NULL,
                prev_index: u32::MAX,
                sequence: 0xffff_ffff,
                script_sig: vec![],
                witness: vec![],
            },
        ];
        c.put_body(Fk(90), 1, t.clone(), vec![out(1)], inputs);
        // Full inputs not retained — thin rebuild re-decodes from store.
        assert!(c.body_prevout_edges(Fk(90)).is_none());
        assert!(c.has_body(Fk(90)));
        assert!(c.get_parent_out(Fk(90), 0).is_some());
    }


    #[test]
    fn body_create_resolves_from_out_fifo() {
        // Bodies-first: put_body only; get_parent_out/has_parent_out use body outs.
        let c = ConfirmParentCache::new();
        c.advance_tip(0);
        let t = tx(7);
        c.put_bodies_batch(vec![(
            Fk(70),
            1,
            t.clone(),
            vec![out(10), out(20)],
            vec![],
        )]);
        assert!(c.has_body(Fk(70)));
        assert_eq!(c.get_parent_txid(Fk(70)), Some(t.txid));
        assert!(c.has_parent_out(Fk(70), 1));
        assert_eq!(c.get_parent_out(Fk(70), 1).unwrap().1.value, 20);
        // No sparse by_fk — external pins are per-batch BatchParents.
        assert!(c.get_parent_out(Fk(99), 0).is_none());
    }

    #[test]
    fn thin_inputs_stash_and_tip_drops_via_header_plan() {
        let c = ConfirmParentCache::new();
        c.advance_tip(0);
        c.put_header_plan(1, Fk(1), header_rec([1u8; 32]), vec![Fk(10)], [0u8; 32]);
        c.put_thin_inputs(
            Fk(10),
            vec![
                StashedThinInput {
                    create_fk: None,
                    prev_index: 0xffff_ffff,
                },
                StashedThinInput {
                    create_fk: Some(99),
                    prev_index: 1,
                },
            ],
        );
        let edges = c.get_thin_inputs(Fk(10)).unwrap();
        assert_eq!(edges.len(), 2);
        assert_eq!(edges[1].create_fk, Some(99));
        // Thin dropped when header plan for that height is tip-GC'd.
        c.advance_tip(1);
        assert!(c.get_thin_inputs(Fk(10)).is_none());
    }




    #[test]
    fn out_fifo_keeps_outs_across_tip_for_pin() {
        let c = ConfirmParentCache::new();
        c.advance_tip(0);
        let t1 = tx(1);
        c.put_bodies_batch(vec![
            (Fk(1), 1, t1.clone(), vec![out(10)], vec![]),
            (Fk(2), 1, tx(2), vec![out(20)], vec![]),
        ]);
        c.put_body_range(Fk(1), 100, 50);
        assert!(c.has_body(Fk(1)));
        assert!(c.bodies_complete(&[Fk(1), Fk(2)]));
        // Full body API empty (outs-only).
        assert!(c.get_bodies_batch(&[Fk(1)]).is_empty());
        // After tip past create, outs stay until FIFO capacity eviction.
        c.advance_tip(10);
        assert!(c.has_body(Fk(1)));
        let pin = c.get_body_for_pin(Fk(2)).expect("pin");
        assert_eq!(pin.2[0].value, 20);
        assert_eq!(c.get_body_range(Fk(1)), Some((100, 50)));
    }

    #[test]
    fn out_fifo_cap_evicts_oldest_creates() {
        let c = ConfirmParentCache::new();
        c.set_out_fifo_cap(5); // max 5 outs
        c.advance_tip(0);
        c.put_bodies_batch(vec![(Fk(1), 1, tx(1), vec![out(1), out(2), out(3)], vec![])]);
        c.put_bodies_batch(vec![(Fk(2), 2, tx(2), vec![out(4), out(5), out(6)], vec![])]);
        assert!(!c.has_body(Fk(1)), "oldest create evicted when over out cap");
        assert!(c.has_body(Fk(2)));
        let (n, total, cap, _) = c.body_lru_stats();
        assert_eq!(n, 1);
        assert_eq!(total, 3);
        assert_eq!(cap, 5);
    }


    #[test]
    fn take_thin_inputs_batch_moves() {
        let c = ConfirmParentCache::new();
        c.advance_tip(0);
        c.put_thin_inputs(
            Fk(5),
            vec![StashedThinInput {
                create_fk: Some(9),
                prev_index: 2,
            }],
        );
        c.put_thin_inputs(
            Fk(6),
            vec![StashedThinInput {
                create_fk: None,
                prev_index: 0xffff_ffff,
            }],
        );
        let taken = c.take_thin_inputs_batch(&[Fk(5), Fk(6)]);
        assert_eq!(taken.len(), 2);
        assert_eq!(taken.get(&5).unwrap()[0].create_fk, Some(9));
        assert!(c.get_thin_inputs(Fk(5)).is_none());
        assert!(c.take_thin_inputs(Fk(6)).is_none());
    }

    #[test]
    fn headroom_ready_requires_watermark() {
        let c = ConfirmParentCache::new();
        c.advance_tip(0);
        // Ready 1..=3 only (full packages).
        for h in 1..=3u32 {
            let mut hash = [0u8; 32];
            hash[0] = h as u8;
            seed_coinbase_package(&c, h, hash, 1000 + h as u64);
        }
        assert_eq!(c.ready_through(), 3);
        assert!(c.headroom_ready(1, 0));
        assert!(c.headroom_ready(1, 2)); // need through 3
        // Short cache: max plan is 3 and ready → satisfied for any headroom.
        assert!(c.headroom_ready(1, 3));
        assert!(c.headroom_ready(3, 64));
        // Seed unfinished plans further ahead (IBD publishes full cache).
        c.ensure_plan(4, [4u8; 32]);
        c.ensure_plan(5, [5u8; 32]);
        assert!(!c.headroom_ready(3, 2)); // need 5 ready, only through 3
        seed_coinbase_package(&c, 4, [4u8; 32], 1004);
        seed_coinbase_package(&c, 5, [5u8; 32], 1005);
        assert!(c.headroom_ready(3, 2));
    }

    #[test]
    fn advance_tip_prunes() {
        let c = ConfirmParentCache::new();
        c.advance_tip(0);
        c.ensure_plan(1, [1u8; 32]);
        c.mark_scanned(1);
        c.advance_tip(1);
        assert!(!c.is_ready(1)); // pruned
        assert_eq!(c.plan_count(), 0);
        assert_eq!(c.ready_through(), 1);
    }




    #[test]
    fn out_fifo_bounds_total_outs() {
        let c = ConfirmParentCache::new();
        c.set_out_fifo_cap(100);
        c.advance_tip(0);
        for i in 0..50u64 {
            let mut t = tx((i & 0xff) as u8);
            t.txid[0] = i as u8;
            // 4 outs each → 200 outs > 100 cap
            c.put_body(Fk(i + 1), 1, t, vec![out(1); 4], vec![]);
        }
        let (n, total, cap, _) = c.body_lru_stats();
        assert!(total <= cap, "total_outs={total} cap={cap} creates={n}");
        assert!(n <= 25, "creates={n}");
    }



    /// Slim batch pin: only requested outs, one lock, coinbase hint; no full input clone.
    #[test]
    fn get_bodies_for_pin_batch_slims_outs() {
        let c = ConfirmParentCache::new();
        c.advance_tip(0);
        let mut t = tx(7);
        t.input_count = 1;
        let coinbase_in = InputRecord {
            prev_txid: [0u8; 32],
            create_fk: Fk::NULL,
            prev_index: u32::MAX,
            sequence: u32::MAX,
            script_sig: vec![0xde; 200], // would be expensive if cloned every pin
            witness: vec![vec![0xad; 500]],
        };
        c.put_body(
            Fk(77),
            5,
            t.clone(),
            vec![out(10), out(20), out(30)],
            vec![coinbase_in],
        );
        c.put_body_range(Fk(77), 1000, 64);

        let need = [0u32, 2];
        let mut hits = c.get_bodies_for_pin_batch(&[(77, &need)]);
        let (h, txr, outs, cb, range) = hits.remove(&77).expect("hit");
        assert_eq!(h, 5);
        assert_eq!(txr.txid, t.txid);
        assert_eq!(outs.len(), 2);
        assert_eq!(outs[0].0, 0);
        assert_eq!(outs[1].0, 2);
        assert_eq!(cb, Some(true));
        assert_eq!(range, Some((1000, 64)));
        // Spent-filtered pin lives on BatchParents (not shared by_fk).
        let mut bp = crate::BatchParents::new();
        bp.insert_owned(Fk(77), txr, outs, need.to_vec(), Some(Some(5)));
        assert!(bp.pin_covered(Fk(77), &[0, 2]));
        assert!(!bp.pin_covered(Fk(77), &[0, 1]));
    }
}
