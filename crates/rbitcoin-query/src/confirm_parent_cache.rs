//! Block-structured **confirm parent runway** (replaces generic Class A cache).
//!
//! Prewarm **mlocks** Class A body pages for heights in `(tip, tip+depth]` and
//! their external parents; it does **not** fully parse txs into RAM. Confirm
//! wave/connect full-parse from the (now resident) store mappings.
//!
//! - **Runway creates** register `txid → fk` (meta only) so same-batch spends
//!   resolve without head probes.
//! - **Thin input edges** (create_fk + vout) are stashed per spend tx after a
//!   lightweight input prevout walk.
//! - Optional full [`BodyEntry`] / [`ParentEntry`] paths remain for tests and
//!   legacy callers; IBD prewarm no longer fills them.
//! - A height is **ready** once its body has been **scanned** (mlocked + edges).

use rbitcoin_primitives::Fk;
use rbitcoin_store::{InputRecord, OutputRecord, TxRecord};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Mutex;

/// Default runway depth (blocks ahead of tip). Override with env.
pub const DEFAULT_PREWARM_DEPTH: u32 = 256;
pub const MIN_PREWARM_DEPTH: u32 = 32;
pub const MAX_PREWARM_DEPTH: u32 = 4096;
/// Blocks processed per background tick (larger = less overhead / better lead).
pub const DEFAULT_PREWARM_BATCH: u32 = 64;
/// Confirm waits until warmer is this many blocks past `batch_end` (when
/// those heights exist on the runway). Default matches one prewarm batch.
pub const DEFAULT_PREWARM_HEADROOM: u32 = 64;

pub fn prewarm_depth_from_env() -> u32 {
    std::env::var("RBITCOIN_PARENT_PREWARM_DEPTH")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(DEFAULT_PREWARM_DEPTH)
        .clamp(MIN_PREWARM_DEPTH, MAX_PREWARM_DEPTH)
}

pub fn prewarm_batch_from_env() -> u32 {
    std::env::var("RBITCOIN_PARENT_PREWARM_BATCH")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(DEFAULT_PREWARM_BATCH)
        .clamp(8, 512)
}

pub fn prewarm_headroom_from_env() -> u32 {
    std::env::var("RBITCOIN_PARENT_PREWARM_HEADROOM")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(DEFAULT_PREWARM_HEADROOM)
        .clamp(0, MAX_PREWARM_DEPTH)
}

/// One needed prevout under a parent create.
#[derive(Debug, Clone)]
pub struct ParentOut {
    pub output: OutputRecord,
}

/// Parent create row held for the runway.
#[derive(Debug, Clone)]
pub struct ParentEntry {
    pub tx: TxRecord,
    /// Needed / registered vouts → output.
    pub outs: HashMap<u32, ParentOut>,
    /// Height of the runway body that registered this create (`None` = UTXO load).
    pub create_height: Option<u32>,
}

/// Thin create-fk edge for one input (same layout as [`crate::wave_prevout::ThinInput`]).
/// Stored on the runway body so wave_fill can skip re-walking inputs.
#[derive(Debug, Clone, Copy)]
pub struct StashedThinInput {
    pub create_fk: Option<u64>,
    pub prev_index: u32,
}

/// Full Class A body for a runway height (confirm should not re-read store).
#[derive(Debug, Clone)]
pub struct BodyEntry {
    pub height: u32,
    pub tx: TxRecord,
    pub outputs: Vec<OutputRecord>,
    pub inputs: Vec<InputRecord>,
    /// Per-input create-fk edges filled after prewarm phase-2 parent resolve.
    /// `None` = not yet stashed (wave_fill falls back to walking `inputs`).
    pub thin_inputs: Option<Vec<StashedThinInput>>,
}

/// Per-height plan: what prevouts block `height` needs.
#[derive(Debug, Default)]
struct HeightPlan {
    hash: [u8; 32],
    /// Prewarm finished scanning this body (may still have open reserves).
    scanned: bool,
    /// (create_fk, vout) fully populated in cache.
    need_fk: HashSet<(u64, u32)>,
    /// (prev_txid, vout) not in UTXO at prewarm — expect runway / same-wave create.
    /// Does **not** block [`HeightPlan::is_ready`].
    reserved: HashSet<([u8; 32], u32)>,
}

impl HeightPlan {
    /// Ready for confirm once scanned. Open reservations are OK: same-batch
    /// create→spend resolves in the wave; filled cache is best-effort.
    fn is_ready(&self) -> bool {
        self.scanned
    }
}

/// One mlocked Class A body (page-aligned range from `mlock`).
struct MlockRec {
    /// Runway height for block bodies; `None` for external parents.
    body_height: Option<u32>,
    /// Heights whose prewarm still needs this parent mlock.
    need_heights: HashSet<u32>,
    page_start: u64,
    page_len: u64,
    start_page: u64,
    end_page: u64, // exclusive page index
}

struct Inner {
    depth: u32,
    /// Highest confirmed tip we have pruned to.
    tip: u32,
    /// Contiguous ready watermark: all heights in `(tip, ready_through]` are ready.
    /// `ready_through == tip` means nothing ahead is ready.
    ready_through: u32,
    /// height → plan
    plans: BTreeMap<u32, HeightPlan>,
    /// Parent bodies keyed by create fk id (optional; tests / legacy).
    by_fk: HashMap<u64, ParentEntry>,
    /// Full runway block bodies by tx fk (optional; tests / legacy).
    by_body: HashMap<u64, BodyEntry>,
    /// Thin edges without a full body parse (mlock prewarm).
    thin_edges: HashMap<u64, Vec<StashedThinInput>>,
    /// create txid → fk (runway creates + loaded parents).
    by_txid: HashMap<[u8; 32], u64>,
    /// Reserved (txid, vout) → set of heights waiting (legacy; unused by new prewarm).
    reserve_waiters: HashMap<([u8; 32], u32), HashSet<u32>>,
    /// fk → mlocked body page range (prewarm).
    mlocked: HashMap<u64, MlockRec>,
    /// page index → refcount (shared pages between adjacent records).
    page_refs: HashMap<u64, u32>,
}

/// Process-local confirm parent runway.
pub struct ConfirmParentCache {
    inner: Mutex<Inner>,
    depth: AtomicU32,
    /// Mirror of `Inner::ready_through` for lock-free reads.
    ready_through: AtomicU32,
}

impl ConfirmParentCache {
    pub fn new(depth: u32) -> Self {
        let depth = depth.clamp(MIN_PREWARM_DEPTH, MAX_PREWARM_DEPTH);
        Self {
            inner: Mutex::new(Inner {
                depth,
                tip: 0,
                ready_through: 0,
                plans: BTreeMap::new(),
                by_fk: HashMap::new(),
                by_body: HashMap::new(),
                thin_edges: HashMap::new(),
                by_txid: HashMap::new(),
                reserve_waiters: HashMap::new(),
                mlocked: HashMap::new(),
                page_refs: HashMap::new(),
            }),
            depth: AtomicU32::new(depth),
            ready_through: AtomicU32::new(0),
        }
    }

    pub fn from_env() -> Self {
        Self::new(prewarm_depth_from_env())
    }

    pub fn depth(&self) -> u32 {
        self.depth.load(Ordering::Relaxed)
    }

    pub fn set_depth(&self, depth: u32) {
        let d = depth.clamp(MIN_PREWARM_DEPTH, MAX_PREWARM_DEPTH);
        self.depth.store(d, Ordering::Relaxed);
        self.inner.lock().unwrap().depth = d;
    }

    /// Highest height such that every plan in `(tip, ready_through]` is ready.
    pub fn ready_through(&self) -> u32 {
        self.ready_through.load(Ordering::Relaxed)
    }

    /// Advance tip: drop plans at/below tip; drop parents/bodies only needed there.
    ///
    /// Returns page ranges whose refcount hit zero (caller should `munlock`).
    pub fn advance_tip(&self, tip: u32) -> Vec<(u64, u64)> {
        let mut g = self.inner.lock().unwrap();
        g.tip = tip;
        if g.ready_through < tip {
            g.ready_through = tip;
        }
        let drop_h: Vec<u32> = g.plans.range(..=tip).map(|(h, _)| *h).collect();
        for h in drop_h {
            if let Some(plan) = g.plans.remove(&h) {
                for key in plan.reserved {
                    if let Some(waiters) = g.reserve_waiters.get_mut(&key) {
                        waiters.remove(&h);
                        if waiters.is_empty() {
                            g.reserve_waiters.remove(&key);
                        }
                    }
                }
            }
        }
        // Horizon: drop plans beyond tip+depth.
        let max_h = tip.saturating_add(g.depth);
        let far: Vec<u32> = g.plans.range((max_h + 1)..).map(|(h, _)| *h).collect();
        for h in far {
            g.plans.remove(&h);
        }
        // One body retain + one parent GC (was double-scanned every batch).
        let mut drop_body_fks: Vec<u64> = Vec::new();
        g.by_body.retain(|id, b| {
            let keep = b.height > tip && b.height <= max_h;
            if !keep {
                drop_body_fks.push(*id);
            }
            keep
        });
        for id in &drop_body_fks {
            g.thin_edges.remove(id);
        }
        g.gc_orphaned_parents();
        let unlocks = g.gc_mlocks(tip, max_h);
        g.recompute_ready_through();
        self.ready_through
            .store(g.ready_through, Ordering::Relaxed);
        unlocks
    }

    /// Register a runway create after mlock (txid → fk only; no full body).
    pub fn register_mlocked_create(&self, fk: Fk, txid: [u8; 32], height: u32) {
        let Some(id) = fk.get() else {
            return;
        };
        let mut g = self.inner.lock().unwrap();
        g.by_txid.insert(txid, id);
        let _ = height;
    }

    /// Track a successful `mlock` of Class A body pages for a runway tx or parent.
    ///
    /// `body_height = Some(h)` for block bodies at height `h`; `None` for external
    /// parents (then `need_height` is the spending height that needs the parent).
    pub fn note_mlock(
        &self,
        fk: Fk,
        page_start: u64,
        page_len: u64,
        body_height: Option<u32>,
        need_height: u32,
    ) {
        let Some(id) = fk.get() else {
            return;
        };
        if page_len == 0 {
            return;
        }
        let page = 4096u64; // page refs only; actual mlock already page-aligned
        let start_page = page_start / page;
        let end_page = page_start
            .saturating_add(page_len)
            .div_ceil(page)
            .max(start_page);
        let mut g = self.inner.lock().unwrap();
        if let Some(rec) = g.mlocked.get_mut(&id) {
            if body_height.is_some() {
                rec.body_height = body_height;
            }
            rec.need_heights.insert(need_height);
            return;
        }
        for p in start_page..end_page {
            *g.page_refs.entry(p).or_insert(0) = g.page_refs.get(&p).copied().unwrap_or(0).saturating_add(1);
        }
        let mut need = HashSet::new();
        need.insert(need_height);
        g.mlocked.insert(
            id,
            MlockRec {
                body_height,
                need_heights: need,
                page_start,
                page_len,
                start_page,
                end_page,
            },
        );
    }

    /// Number of mlocked Class A bodies currently tracked.
    pub fn mlock_count(&self) -> usize {
        self.inner.lock().unwrap().mlocked.len()
    }

    /// Store a full runway block body (phase-1 prewarm). Confirm/wave should
    /// prefer this over Class A store reads.
    ///
    /// Inserts `txid → fk` so later spends resolve via [`Self::get_by_txid`] +
    /// [`Self::get_parent_out`] (body outs, no dual `by_fk` copy of every script).
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
        let mut g = self.inner.lock().unwrap();
        g.insert_body(id, height, tx, outputs, inputs);
    }

    /// Many bodies under **one** lock (prewarm phase-1 finish). Moves ownership.
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
            g.insert_body(id, height, tx, outputs, inputs);
        }
    }

    /// Phase-1 hot path: body + `by_txid` (creates are the body outs).
    ///
    /// Does **not** clone every output into `by_fk` — that doubled RAM/CPU on
    /// mainnet (~all scripts twice). Wave/prewarm resolve runway creates via
    /// [`Self::get_parent_out`] body fallback. Sparse `by_fk` is only for
    /// external UTXO parents and legacy reserve waiters.
    pub fn put_body_and_creates(
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
        let mut g = self.inner.lock().unwrap();
        let txid = tx.txid;
        // Rare reserve waiters: copy only waited-for outs into by_fk.
        g.fill_reserve_waiters_from_body(id, txid, height, &tx, &outputs);
        g.insert_body(id, height, tx, outputs, inputs);
    }

    pub fn get_body(
        &self,
        fk: Fk,
    ) -> Option<(TxRecord, Vec<OutputRecord>, Vec<InputRecord>)> {
        let id = fk.get()?;
        let g = self.inner.lock().unwrap();
        let e = g.by_body.get(&id)?;
        Some((e.tx.clone(), e.outputs.clone(), e.inputs.clone()))
    }

    /// Attach prewarm-resolved thin edges (wave_fill fast path; no full body required).
    pub fn put_thin_inputs(&self, fk: Fk, edges: Vec<StashedThinInput>) {
        let Some(id) = fk.get() else {
            return;
        };
        let mut g = self.inner.lock().unwrap();
        if let Some(e) = g.by_body.get_mut(&id) {
            e.thin_inputs = Some(edges.clone());
        }
        g.thin_edges.insert(id, edges);
    }

    /// Thin edges stashed during prewarm, if present.
    pub fn get_thin_inputs(&self, fk: Fk) -> Option<Vec<StashedThinInput>> {
        let id = fk.get()?;
        let g = self.inner.lock().unwrap();
        if let Some(e) = g.by_body.get(&id).and_then(|b| b.thin_inputs.clone()) {
            return Some(e);
        }
        g.thin_edges.get(&id).cloned()
    }

    pub fn body_count(&self) -> usize {
        let g = self.inner.lock().unwrap();
        let mlock_bodies = g
            .mlocked
            .values()
            .filter(|m| m.body_height.is_some())
            .count();
        if mlock_bodies > 0 {
            mlock_bodies
        } else {
            g.by_body.len()
        }
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
        if height > g.tip.saturating_add(g.depth) {
            return;
        }
        g.plans.entry(height).or_insert_with(|| HeightPlan {
            hash,
            scanned: false,
            need_fk: HashSet::new(),
            reserved: HashSet::new(),
        });
        if let Some(p) = g.plans.get_mut(&height) {
            p.hash = hash;
        }
    }

    /// Seed many plans under one lock (IBD runway publish).
    pub fn ensure_plans(&self, items: &[(u32, [u8; 32])]) {
        if items.is_empty() {
            return;
        }
        let mut g = self.inner.lock().unwrap();
        let max_h = g.tip.saturating_add(g.depth);
        for &(height, hash) in items {
            if height <= g.tip || height > max_h {
                continue;
            }
            g.plans.entry(height).or_insert_with(|| HeightPlan {
                hash,
                scanned: false,
                need_fk: HashSet::new(),
                reserved: HashSet::new(),
            });
            if let Some(p) = g.plans.get_mut(&height) {
                p.hash = hash;
            }
        }
    }

    /// True if height was scanned (open reservations do not block).
    pub fn is_ready(&self, height: u32) -> bool {
        let g = self.inner.lock().unwrap();
        g.plans.get(&height).map(|p| p.is_ready()).unwrap_or(false)
    }

    /// True if height still has open reserved holes (debug / tests).
    pub fn has_open_reserves(&self, height: u32) -> bool {
        let g = self.inner.lock().unwrap();
        g.plans
            .get(&height)
            .is_some_and(|p| !p.reserved.is_empty())
    }

    /// All heights in `heights` ready (scanned).
    pub fn all_ready(&self, heights: &[u32]) -> bool {
        let g = self.inner.lock().unwrap();
        heights.iter().all(|h| {
            g.plans
                .get(h)
                .map(|p| p.is_ready())
                .unwrap_or(false)
        })
    }

    /// Confirm headroom: warmer has fully ready plans through at least
    /// `batch_end + headroom`, or through the furthest **seeded** plan if the
    /// runway is shorter (archive lag / depth edge).
    ///
    /// IBD should [`Self::ensure_plan`] the full published runway so unfinished
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
        // Short runway: every seeded plan is ready — archive lag / depth edge.
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
    }

    /// Register a UTXO-backed parent out for `height`.
    ///
    /// Does not recompute ready watermark (plan is still unscanned until
    /// [`Self::mark_scanned`]).
    pub fn put_utxo_parent(
        &self,
        height: u32,
        fk: Fk,
        tx: TxRecord,
        vout: u32,
        output: OutputRecord,
    ) {
        let Some(id) = fk.get() else {
            return;
        };
        let mut g = self.inner.lock().unwrap();
        g.put_utxo_parent_inner(height, id, tx, vout, output);
    }

    /// Batch parent outs under one lock (prewarm phase-2 finish).
    pub fn put_utxo_parents_batch(&self, items: &[(u32, Fk, TxRecord, u32, OutputRecord)]) {
        if items.is_empty() {
            return;
        }
        let mut g = self.inner.lock().unwrap();
        for &(height, fk, ref tx, vout, ref output) in items {
            let Some(id) = fk.get() else {
                continue;
            };
            g.put_utxo_parent_inner(height, id, tx.clone(), vout, output.clone());
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
            if let Some(e) = g.by_body.get_mut(&id) {
                e.thin_inputs = Some(edges.clone());
            }
            g.thin_edges.insert(id, edges);
        }
    }

    /// Batch-register runway create txids after mlock prewarm.
    pub fn register_mlocked_creates_batch(&self, items: &[(Fk, [u8; 32], u32)]) {
        if items.is_empty() {
            return;
        }
        let mut g = self.inner.lock().unwrap();
        for &(fk, txid, _height) in items {
            let Some(id) = fk.get() else {
                continue;
            };
            g.by_txid.insert(txid, id);
        }
    }

    /// Note many mlocks under one lock (after successful store.mlock calls).
    pub fn note_mlocks_batch(
        &self,
        items: &[(Fk, u64, u64, Option<u32>, u32)],
    ) {
        // (fk, page_start, page_len, body_height, need_height)
        if items.is_empty() {
            return;
        }
        let page = 4096u64;
        let mut g = self.inner.lock().unwrap();
        for &(fk, page_start, page_len, body_height, need_height) in items {
            let Some(id) = fk.get() else {
                continue;
            };
            if page_len == 0 {
                continue;
            }
            if let Some(rec) = g.mlocked.get_mut(&id) {
                if body_height.is_some() {
                    rec.body_height = body_height;
                }
                rec.need_heights.insert(need_height);
                continue;
            }
            let start_page = page_start / page;
            let end_page = page_start
                .saturating_add(page_len)
                .div_ceil(page)
                .max(start_page);
            for p in start_page..end_page {
                *g.page_refs.entry(p).or_insert(0) += 1;
            }
            let mut need = HashSet::new();
            need.insert(need_height);
            g.mlocked.insert(
                id,
                MlockRec {
                    body_height,
                    need_heights: need,
                    page_start,
                    page_len,
                    start_page,
                    end_page,
                },
            );
        }
    }

    /// Reserve a hole for a prevout not in UTXO (create is still on the runway).
    ///
    /// If the create was already registered with full outs (create-before-reserve),
    /// fills immediately without a store round-trip.
    pub fn reserve(&self, height: u32, prev_txid: [u8; 32], vout: u32) {
        let mut g = self.inner.lock().unwrap();
        // Already filled from runway create or prior UTXO load?
        if let Some(&id) = g.by_txid.get(&prev_txid) {
            if g.by_fk
                .get(&id)
                .is_some_and(|e| e.outs.contains_key(&vout))
            {
                if let Some(plan) = g.plans.get_mut(&height) {
                    plan.need_fk.insert((id, vout));
                }
                g.recompute_ready_through();
                self.ready_through
                    .store(g.ready_through, Ordering::Relaxed);
                return;
            }
        }
        if let Some(plan) = g.plans.get_mut(&height) {
            plan.reserved.insert((prev_txid, vout));
        }
        g.reserve_waiters
            .entry((prev_txid, vout))
            .or_default()
            .insert(height);
    }

    /// Register creates from a runway body with **all** outputs.
    ///
    /// Phase-1 prewarm loads full blocks first so later spends in the same
    /// batch resolve creates without UTXO or reservations.
    /// Prefer [`Self::put_body_and_creates`] on the hot path (one lock).
    pub fn register_runway_creates(
        &self,
        create_fk: Fk,
        tx: &TxRecord,
        outputs: &[OutputRecord],
        create_height: u32,
    ) {
        let Some(id) = create_fk.get() else {
            return;
        };
        let mut g = self.inner.lock().unwrap();
        let txid = tx.txid;
        g.by_txid.insert(txid, id);
        {
            let e = g.by_fk.entry(id).or_insert_with(|| ParentEntry {
                tx: tx.clone(),
                outs: HashMap::new(),
                create_height: Some(create_height),
            });
            e.tx = tx.clone();
            e.create_height = Some(create_height);
            for (v, o) in outputs.iter().enumerate() {
                e.outs.insert(
                    v as u32,
                    ParentOut {
                        output: o.clone(),
                    },
                );
            }
        }
        // Clear any legacy waiters for this create.
        for v in 0..outputs.len() as u32 {
            let key = (txid, v);
            if let Some(waiters) = g.reserve_waiters.remove(&key) {
                for h in waiters {
                    if let Some(plan) = g.plans.get_mut(&h) {
                        plan.reserved.remove(&key);
                        plan.need_fk.insert((id, v));
                    }
                }
            }
        }
        // ready_through unchanged until mark_scanned.
    }

    /// Look up a populated parent out (for wave fill / connect).
    ///
    /// Prefers sparse external-parent `by_fk`, then runway **body** outs
    /// (bodies-first prewarm no longer dual-copies every create into `by_fk`).
    pub fn get_parent_out(
        &self,
        fk: Fk,
        vout: u32,
    ) -> Option<(TxRecord, OutputRecord)> {
        let id = fk.get()?;
        let g = self.inner.lock().unwrap();
        if let Some(e) = g.by_fk.get(&id) {
            if let Some(o) = e.outs.get(&vout) {
                return Some((e.tx.clone(), o.output.clone()));
            }
        }
        if let Some(b) = g.by_body.get(&id) {
            let o = b.outputs.get(vout as usize)?;
            return Some((b.tx.clone(), o.clone()));
        }
        None
    }

    /// True if vout is present (by_fk sparse or body) — no record clone.
    pub fn has_parent_out(&self, fk: Fk, vout: u32) -> bool {
        let Some(id) = fk.get() else {
            return false;
        };
        let g = self.inner.lock().unwrap();
        Self::out_present_locked(&g, id, vout)
    }

    /// One-lock runway hit: `txid` known on runway (mlock or full body).
    ///
    /// With mlock prewarm we only have `by_txid` (no parsed outs); vout validity
    /// is checked when confirm full-parses the parent.
    pub fn get_by_txid_if_out(&self, txid: &[u8; 32], vout: u32) -> Option<Fk> {
        let g = self.inner.lock().unwrap();
        let &id = g.by_txid.get(txid)?;
        if g.mlocked.contains_key(&id) {
            return Some(Fk(id));
        }
        if Self::out_present_locked(&g, id, vout) {
            Some(Fk(id))
        } else {
            None
        }
    }

    #[inline]
    fn out_present_locked(g: &Inner, id: u64, vout: u32) -> bool {
        if g.by_fk
            .get(&id)
            .is_some_and(|e| e.outs.contains_key(&vout))
        {
            return true;
        }
        g.by_body
            .get(&id)
            .is_some_and(|b| (vout as usize) < b.outputs.len())
    }

    pub fn get_parent_tx(&self, fk: Fk) -> Option<TxRecord> {
        let id = fk.get()?;
        let g = self.inner.lock().unwrap();
        if let Some(e) = g.by_fk.get(&id) {
            return Some(e.tx.clone());
        }
        g.by_body.get(&id).map(|b| b.tx.clone())
    }

    pub fn get_by_txid(&self, txid: &[u8; 32]) -> Option<Fk> {
        self.inner
            .lock()
            .unwrap()
            .by_txid
            .get(txid)
            .map(|&id| Fk(id))
    }

    /// Sparse external-parent outs only (`by_fk`). Does **not** expand runway
    /// bodies — callers that need a subset of body outs should use
    /// [`Self::get_body`] (avoids cloning every script of a multi-out create).
    pub fn get_parent_outs(&self, fk: Fk) -> Option<(TxRecord, HashMap<u32, OutputRecord>)> {
        let id = fk.get()?;
        let g = self.inner.lock().unwrap();
        let e = g.by_fk.get(&id)?;
        if e.outs.is_empty() {
            return None;
        }
        let outs: HashMap<u32, OutputRecord> = e
            .outs
            .iter()
            .map(|(v, o)| (*v, o.output.clone()))
            .collect();
        Some((e.tx.clone(), outs))
    }

    pub fn plan_count(&self) -> usize {
        self.inner.lock().unwrap().plans.len()
    }

    pub fn parent_count(&self) -> usize {
        let g = self.inner.lock().unwrap();
        let mlock_parents = g
            .mlocked
            .values()
            .filter(|m| m.body_height.is_none())
            .count();
        if mlock_parents > 0 {
            mlock_parents
        } else {
            g.by_fk.len()
        }
    }

    pub fn reserved_count(&self) -> usize {
        self.inner.lock().unwrap().reserve_waiters.len()
    }

    /// Drop a spent out from cache (after Class C). O(1) under the cache lock.
    ///
    /// Does **not** scan all height plans: stale `need_fk` entries are dropped
    /// when the plan is removed on tip advance. Parent GC uses remaining plans.
    pub fn retire_spend(&self, create_fk: Fk, vout: u32) {
        let Some(id) = create_fk.get() else {
            return;
        };
        let mut g = self.inner.lock().unwrap();
        g.retire_spend_id(id, vout);
    }

    /// Batch retire after Class C (one lock for the whole spend list).
    pub fn retire_spends(&self, spends: &[(Fk, u32)]) {
        if spends.is_empty() {
            return;
        }
        let mut g = self.inner.lock().unwrap();
        for &(fk, vout) in spends {
            let Some(id) = fk.get() else {
                continue;
            };
            g.retire_spend_id(id, vout);
        }
    }
}

impl Inner {
    fn insert_body(
        &mut self,
        id: u64,
        height: u32,
        tx: TxRecord,
        outputs: Vec<OutputRecord>,
        inputs: Vec<InputRecord>,
    ) {
        self.by_txid.insert(tx.txid, id);
        self.by_body.insert(
            id,
            BodyEntry {
                height,
                tx,
                outputs,
                inputs,
                thin_inputs: None,
            },
        );
    }

    /// Legacy reserve path only: copy waited-for outs into sparse `by_fk`.
    fn fill_reserve_waiters_from_body(
        &mut self,
        id: u64,
        txid: [u8; 32],
        height: u32,
        tx: &TxRecord,
        outputs: &[OutputRecord],
    ) {
        if self.reserve_waiters.is_empty() {
            return;
        }
        for v in 0..outputs.len() as u32 {
            let key = (txid, v);
            let Some(waiters) = self.reserve_waiters.remove(&key) else {
                continue;
            };
            let o = &outputs[v as usize];
            {
                let e = self.by_fk.entry(id).or_insert_with(|| ParentEntry {
                    tx: tx.clone(),
                    outs: HashMap::new(),
                    create_height: Some(height),
                });
                e.tx = tx.clone();
                e.create_height = Some(height);
                e.outs.insert(
                    v,
                    ParentOut {
                        output: o.clone(),
                    },
                );
            }
            for h in waiters {
                if let Some(plan) = self.plans.get_mut(&h) {
                    plan.reserved.remove(&key);
                    plan.need_fk.insert((id, v));
                }
            }
        }
    }

    fn put_utxo_parent_inner(
        &mut self,
        height: u32,
        id: u64,
        tx: TxRecord,
        vout: u32,
        output: OutputRecord,
    ) {
        self.by_txid.insert(tx.txid, id);
        let txid = tx.txid;
        {
            let e = self.by_fk.entry(id).or_insert_with(|| ParentEntry {
                tx: tx.clone(),
                outs: HashMap::new(),
                create_height: None,
            });
            e.tx = tx;
            e.outs.insert(vout, ParentOut { output });
        }
        if let Some(plan) = self.plans.get_mut(&height) {
            plan.need_fk.insert((id, vout));
            plan.reserved.remove(&(txid, vout));
        }
        let key = (txid, vout);
        if let Some(waiters) = self.reserve_waiters.remove(&key) {
            for h in waiters {
                if let Some(plan) = self.plans.get_mut(&h) {
                    plan.reserved.remove(&key);
                    plan.need_fk.insert((id, vout));
                }
            }
        }
    }

    fn retire_spend_id(&mut self, id: u64, vout: u32) {
        if let Some(e) = self.by_fk.get_mut(&id) {
            e.outs.remove(&vout);
            if e.outs.is_empty() {
                let txid = e.tx.txid;
                self.by_fk.remove(&id);
                // Keep by_txid if this create is still a live runway body.
                if !self.by_body.contains_key(&id) && self.by_txid.get(&txid) == Some(&id) {
                    self.by_txid.remove(&txid);
                }
            }
        }
    }

    /// Contiguous ready watermark from tip+1 upward.
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

    /// Drop mlocks for bodies past tip / outside horizon and parents no longer needed.
    /// Returns page ranges with refcount 0 for the caller to `munlock`.
    fn gc_mlocks(&mut self, tip: u32, max_h: u32) -> Vec<(u64, u64)> {
        let mut drop_ids: Vec<u64> = Vec::new();
        for (id, rec) in &mut self.mlocked {
            if let Some(h) = rec.body_height {
                if h <= tip || h > max_h {
                    drop_ids.push(*id);
                }
            } else {
                rec.need_heights.retain(|h| *h > tip && *h <= max_h);
                if rec.need_heights.is_empty() {
                    drop_ids.push(*id);
                }
            }
        }
        let mut unlocks: Vec<(u64, u64)> = Vec::new();
        for id in drop_ids {
            let Some(rec) = self.mlocked.remove(&id) else {
                continue;
            };
            self.thin_edges.remove(&id);
            // Drop by_txid if it pointed at this fk and nothing else needs it.
            self.by_txid.retain(|_, fid| *fid != id);
            for p in rec.start_page..rec.end_page {
                let entry = self.page_refs.entry(p).or_insert(0);
                *entry = entry.saturating_sub(1);
                if *entry == 0 {
                    self.page_refs.remove(&p);
                    // Unlock whole record range once when any of its pages hit 0
                    // is wrong if pages shared — unlock only when *all* pages of
                    // this record hit 0. Collect zero-ref pages for this rec.
                    let _ = p;
                }
            }
            // If no page of this record still has a ref from another record, unlock.
            let still = (rec.start_page..rec.end_page).any(|p| self.page_refs.contains_key(&p));
            if !still {
                unlocks.push((rec.page_start, rec.page_len));
            }
        }
        unlocks
    }

    fn gc_orphaned_parents(&mut self) {
        // Parents still referenced by open plans.
        let live: HashSet<u64> = self
            .plans
            .values()
            .flat_map(|p| p.need_fk.iter().map(|(id, _)| *id))
            .collect();
        let tip = self.tip;
        let has_waiters = !self.reserve_waiters.is_empty();
        let mut drop_ids: Vec<u64> = Vec::new();
        for (&id, e) in &self.by_fk {
            if live.contains(&id) {
                continue;
            }
            // Runway creates: keep by_fk identity until tip passes create height
            // so later prewarm get_by_txid still resolves same-batch parents.
            if let Some(ch) = e.create_height {
                if ch > tip {
                    continue;
                }
            }
            if has_waiters {
                let txid = e.tx.txid;
                if self.reserve_waiters.keys().any(|(t, _)| *t == txid) {
                    continue;
                }
            }
            drop_ids.push(id);
        }
        for id in drop_ids {
            if let Some(e) = self.by_fk.remove(&id) {
                if self.by_txid.get(&e.tx.txid) == Some(&id) {
                    self.by_txid.remove(&e.tx.txid);
                }
            }
        }
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

    #[test]
    fn utxo_parent_marks_ready() {
        let c = ConfirmParentCache::new(64);
        c.advance_tip(10);
        let hash = [9u8; 32];
        c.ensure_plan(11, hash);
        let t = tx(1);
        c.put_utxo_parent(11, Fk(7), t, 0, out(100));
        c.mark_scanned(11);
        assert!(c.is_ready(11));
        assert_eq!(c.ready_through(), 11);
        let (tx, o) = c.get_parent_out(Fk(7), 0).unwrap();
        assert_eq!(tx.txid[0], 1);
        assert_eq!(o.value, 100);
    }

    #[test]
    fn reserve_then_register_create_fills() {
        let c = ConfirmParentCache::new(64);
        c.advance_tip(0);
        c.ensure_plan(2, [2u8; 32]);
        let t = tx(5);
        // Spend of 5:0 not in UTXO yet.
        c.reserve(2, t.txid, 0);
        c.mark_scanned(2);
        // Open reserve must NOT block readiness (batch may create+spend).
        assert!(c.is_ready(2));
        assert!(c.has_open_reserves(2));
        // Create appears from height 1 body — fills cache for wave/connect.
        c.register_runway_creates(Fk(50), &t, &[out(42), out(43)], 1);
        assert!(c.is_ready(2));
        assert!(!c.has_open_reserves(2));
        assert_eq!(c.get_parent_out(Fk(50), 0).unwrap().1.value, 42);
    }

    #[test]
    fn open_reserves_do_not_block_ready_or_watermark() {
        // Simulate batch create@1 + spend@2: spend reserves before create is
        // filled; confirm must still see both heights ready after scan.
        let c = ConfirmParentCache::new(64);
        c.advance_tip(0);
        c.ensure_plan(1, [1u8; 32]);
        c.mark_scanned(1);
        c.ensure_plan(2, [2u8; 32]);
        let t = tx(9);
        c.reserve(2, t.txid, 0);
        c.mark_scanned(2);
        assert!(c.is_ready(1));
        assert!(c.is_ready(2));
        assert!(c.has_open_reserves(2));
        assert!(c.all_ready(&[1, 2]));
        assert_eq!(c.ready_through(), 2);
        assert!(c.headroom_ready(2, 0));
        // Create later fills reserve (best-effort); readiness unchanged.
        c.register_runway_creates(Fk(90), &t, &[out(1)], 1);
        assert!(!c.has_open_reserves(2));
        assert_eq!(c.ready_through(), 2);
    }

    #[test]
    fn phase1_register_keeps_all_outs_for_later_spend() {
        // Bodies first: create height registers all outs; spend height hits cache.
        let c = ConfirmParentCache::new(64);
        c.advance_tip(0);
        c.ensure_plan(1, [1u8; 32]);
        let t = tx(5);
        c.register_runway_creates(Fk(50), &t, &[out(42), out(43)], 1);
        c.mark_scanned(1);
        assert!(c.is_ready(1));
        assert_eq!(c.get_by_txid(&t.txid), Some(Fk(50)));
        assert_eq!(c.get_parent_out(Fk(50), 1).unwrap().1.value, 43);

        c.ensure_plan(2, [2u8; 32]);
        c.put_utxo_parent(2, Fk(50), t.clone(), 1, out(43));
        c.mark_scanned(2);
        assert!(c.is_ready(2));
        assert_eq!(c.ready_through(), 2);
    }

    #[test]
    fn body_cache_survives_until_tip_advances() {
        let c = ConfirmParentCache::new(64);
        c.advance_tip(0);
        let t = tx(5);
        c.put_body(
            Fk(50),
            1,
            t.clone(),
            vec![out(42)],
            vec![],
        );
        assert!(c.get_body(Fk(50)).is_some());
        c.advance_tip(0);
        assert!(c.get_body(Fk(50)).is_some());
        c.advance_tip(1);
        assert!(c.get_body(Fk(50)).is_none());
    }

    #[test]
    fn body_create_resolves_without_by_fk_dual_copy() {
        // Bodies-first: put_body only; get_parent_out/has_parent_out use body outs.
        let c = ConfirmParentCache::new(64);
        c.advance_tip(0);
        let t = tx(7);
        c.put_bodies_batch(vec![(
            Fk(70),
            1,
            t.clone(),
            vec![out(10), out(20)],
            vec![],
        )]);
        assert_eq!(c.get_by_txid(&t.txid), Some(Fk(70)));
        assert!(c.has_parent_out(Fk(70), 1));
        assert_eq!(c.get_parent_out(Fk(70), 1).unwrap().1.value, 20);
        // Sparse external path still works alongside body.
        c.put_utxo_parent(2, Fk(99), tx(9), 0, out(5));
        assert_eq!(c.get_parent_out(Fk(99), 0).unwrap().1.value, 5);
        // parent_count is sparse externals only (not every body create).
        assert_eq!(c.parent_count(), 1);
    }

    #[test]
    fn thin_inputs_stash_on_body() {
        let c = ConfirmParentCache::new(64);
        c.advance_tip(0);
        c.put_body(Fk(10), 1, tx(1), vec![out(1)], vec![]);
        assert!(c.get_thin_inputs(Fk(10)).is_none());
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
        assert_eq!(edges[1].prev_index, 1);
        // Dropped with body when tip advances past create height.
        c.advance_tip(1);
        assert!(c.get_thin_inputs(Fk(10)).is_none());
    }

    #[test]
    fn headroom_ready_requires_watermark() {
        let c = ConfirmParentCache::new(128);
        c.advance_tip(0);
        // Ready 1..=3 only.
        for h in 1..=3u32 {
            c.ensure_plan(h, [h as u8; 32]);
            c.mark_scanned(h);
        }
        assert_eq!(c.ready_through(), 3);
        assert!(c.headroom_ready(1, 0));
        assert!(c.headroom_ready(1, 2)); // need through 3
        // Short runway: max plan is 3 and ready → satisfied for any headroom.
        assert!(c.headroom_ready(1, 3));
        assert!(c.headroom_ready(3, 64));
        // Seed unfinished plans further ahead (IBD publishes full runway).
        c.ensure_plan(4, [4u8; 32]);
        c.ensure_plan(5, [5u8; 32]);
        assert!(!c.headroom_ready(3, 2)); // need 5 ready, only through 3
        c.mark_scanned(4);
        c.mark_scanned(5);
        assert!(c.headroom_ready(3, 2));
    }

    #[test]
    fn advance_tip_prunes() {
        let c = ConfirmParentCache::new(64);
        c.advance_tip(0);
        c.ensure_plan(1, [1u8; 32]);
        c.put_utxo_parent(1, Fk(1), tx(1), 0, out(1));
        c.mark_scanned(1);
        c.advance_tip(1);
        assert!(!c.is_ready(1)); // pruned
        assert_eq!(c.plan_count(), 0);
        assert_eq!(c.ready_through(), 1);
    }
}
