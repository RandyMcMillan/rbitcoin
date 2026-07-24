//! Block-structured **confirm parent cache**.
//!
//! Load-stage strategy (no background worker):
//! - **RAM-cache** small lookups: header + `header_txs`, body ranges, thin edges.
//! - **Full-decode** batch Class A into `by_body` once; wave/wire clone from it.
//! - **`mlock`** parent create pages for write annotate when body is not already
//!   in RAM (body LRU hit skips store + mlock on the pin path).
//! - After tip advance, decoded bodies with `height ≤ tip` stay in `by_body`
//!   under a **byte-capped LRU** (default 1 GiB) so near-subsequent spends hit
//!   RAM instead of cold Class A. Runway bodies (`height > tip`) are never
//!   LRU-evicted.
//!
//! - Parent pin uses create_fk; no process-local txid→fk map (use durable head if needed).
//! - A height is **ready** once scanned (load finished for that height).
//! - Prevouts use stamped create_fk only.

use rbitcoin_primitives::Fk;
use rbitcoin_store::{HeaderRecord, InputRecord, MlockRange, OutputRecord, TxRecord};
// ThinInput used via StashedThinInput alias.
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Condvar, Mutex};
use std::time::{Duration, Instant};

/// Default post-confirm body LRU budget (decoded RAM, not mlock).
pub const DEFAULT_BODY_LRU_MB: u64 = 1024;

/// `RBITCOIN_CONFIRM_BODY_LRU_MB` (default 1024). `0` = drop bodies at tip (legacy).
pub fn body_lru_cap_bytes_from_env() -> u64 {
    let mb = std::env::var("RBITCOIN_CONFIRM_BODY_LRU_MB")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(DEFAULT_BODY_LRU_MB);
    mb.saturating_mul(1024 * 1024)
}

/// Rough heap weight of a decoded Class A row (scripts + witness + overhead).
fn estimate_body_bytes(
    _tx: &TxRecord,
    outputs: &[OutputRecord],
    inputs: &[InputRecord],
) -> u64 {
    let mut n = 256u64;
    for o in outputs {
        n = n.saturating_add(32);
        n = n.saturating_add(o.script.len() as u64);
    }
    for i in inputs {
        n = n.saturating_add(64);
        n = n.saturating_add(i.script_sig.len() as u64);
        for w in &i.witness {
            n = n.saturating_add(w.len() as u64);
        }
    }
    n
}

/// Default: mlock **off** (IBD host proof: same tip with/without; less MEMLOCK).
///
/// Opt-in parent create `tx.body` mlock for write annotate:
/// `RBITCOIN_CONFIRM_MLOCK=1` / `true` / `on` (legacy `RBITCOIN_PARENT_PREWARM_MLOCK`).
pub fn confirm_mlock_from_env() -> bool {
    match std::env::var("RBITCOIN_CONFIRM_MLOCK")
        .or_else(|_| std::env::var("RBITCOIN_PARENT_PREWARM_MLOCK"))
    {
        Ok(s) => {
            let t = s.trim();
            t == "1" || t.eq_ignore_ascii_case("true") || t.eq_ignore_ascii_case("on")
        }
        Err(_) => false,
    }
}

/// Thin create-fk edge (identical to wave [`crate::wave_prevout::ThinInput`]).
pub type StashedThinInput = crate::wave_prevout::ThinInput;

/// Full Class A body for a cache height (confirm should not re-read store).
#[derive(Debug, Clone)]
pub struct BodyEntry {
    pub height: u32,
    pub tx: TxRecord,
    pub outputs: Vec<OutputRecord>,
    pub inputs: Vec<InputRecord>,
    /// Per-input create-fk edges filled after load phase-2 parent resolve.
    /// `None` = not yet stashed (assemble falls back via store/thin rebuild).
    pub thin_inputs: Option<Vec<StashedThinInput>>,
    /// Estimated heap bytes (LRU accounting).
    size_bytes: u64,
    /// Lazy LRU stamp (matches [`Inner::body_lru`] records).
    lru_stamp: u64,
}

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

/// Per-height plan: what prevouts block `height` needs.
#[derive(Debug, Default)]
struct HeightPlan {
    hash: [u8; 32],
    /// Runway finished a body+thin+pin attempt for this height.
    ///
    /// This is the **2-stage wait bit**: confirm unblocks when scanned (same work
    /// as pre-pipeline). Full content completeness is best-effort in the pin
    /// phase; wave may store-fallback on residual misses.
    scanned: bool,
    /// (create_fk, vout) fully populated in cache.
    need_fk: HashSet<(u64, u32)>,
    /// (prev_txid, vout) not in UTXO at cache — expect cache / same-wave create.
    reserved: HashSet<([u8; 32], u32)>,
}

impl HeightPlan {
    /// Runway attempt finished — O(1). Used by wait / ready_through (2-stage).
    #[inline]
    fn is_ready(&self) -> bool {
        self.scanned
    }
}

/// One mlocked page range (any store table) held for cache heights.
struct RangeRec {
    range: MlockRange,
    /// Heights that still need this range warm.
    need_heights: HashSet<u32>,
    start_page: u64,
    end_page: u64, // exclusive page index within this table
}

struct Inner {
    /// Highest confirmed tip we have pruned to.
    tip: u32,
    /// Contiguous ready watermark: all heights in `(tip, ready_through]` are ready.
    /// `ready_through == tip` means nothing ahead is ready.
    ready_through: u32,
    /// height → plan
    plans: BTreeMap<u32, HeightPlan>,
    /// Full decoded bodies by create fk (runway + post-confirm LRU).
    by_body: HashMap<u64, BodyEntry>,
    /// Total `BodyEntry::size_bytes` for entries with `height ≤ tip` (LRU budget).
    body_lru_bytes: u64,
    /// Cap for confirmed-body LRU (`height ≤ tip`). Runway (`height > tip`) free.
    body_lru_cap: u64,
    /// Lazy LRU order: `(fk_id, stamp)`; front = oldest. Touch pushes new stamp.
    body_lru: VecDeque<(u64, u64)>,
    next_lru_stamp: u64,
    /// Thin edges without a full body parse (mlock cache).
    thin_edges: HashMap<u64, Vec<StashedThinInput>>,
    /// height → header + tx list (replaces header.head/body + header_txs reads).
    headers: HashMap<u32, HeaderPlanCache>,
    /// hash → height for O(1) header resolve on confirm.
    hash_to_height: HashMap<[u8; 32], u32>,
    /// fk id → absolute body (offset, len) from tx.idx (replaces idx page faults).
    body_range: HashMap<u64, (u64, u64)>,
    /// Reserved (txid, vout) → set of heights waiting (legacy; unused by new cache).
    reserve_waiters: HashMap<([u8; 32], u32), HashSet<u32>>,
    /// (table, page_start) → mlocked range + need_heights.
    mlocked: HashMap<(u8, u64), RangeRec>,
    /// (table, page_index) → refcount (shared pages).
    page_refs: HashMap<(u8, u64), u32>,
    /// How many distinct ranges currently held (perf).
    mlock_n: usize,
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
                by_body: HashMap::new(),
                body_lru_bytes: 0,
                body_lru_cap: body_lru_cap_bytes_from_env(),
                body_lru: VecDeque::new(),
                next_lru_stamp: 1,
                thin_edges: HashMap::new(),
                headers: HashMap::new(),
                hash_to_height: HashMap::new(),
                body_range: HashMap::new(),
                reserve_waiters: HashMap::new(),
                mlocked: HashMap::new(),
                page_refs: HashMap::new(),
                mlock_n: 0,
            }),
            ready_cv: Condvar::new(),
            ready_through: AtomicU32::new(0),
        }
    }

    pub fn from_env() -> Self {
        Self::new()
    }

    /// Override confirmed-body LRU byte cap (tests). `0` = drop bodies at tip.
    pub fn set_body_lru_cap_bytes(&self, cap: u64) {
        self.inner.lock().unwrap().body_lru_cap = cap;
    }

    /// Highest height such that every plan in `(tip, ready_through]` is ready.
    pub fn ready_through(&self) -> u32 {
        self.ready_through.load(Ordering::Relaxed)
    }

    /// Advance tip: drop plans at/below tip; drop parents/bodies only needed there.
    ///
    /// Called from write `post_commit` after Class C + spend annotate for the
    /// committed batch — so mlocks for heights ≤ tip are released only once
    /// write for those heights is done (need_heights drop when h ≤ tip).
    /// No artificial lag behind tip: in-flight later waves keep mlocks via their
    /// own need_heights (h > tip).
    ///
    /// Returns store ranges whose refcount hit zero (caller should `munlock`).
    pub fn advance_tip(&self, tip: u32) -> Vec<MlockRange> {
        let mut g = self.inner.lock().unwrap();
        let old_tip = g.tip;
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
        // Bodies with height ≤ tip enter the confirmed LRU budget (kept for
        // near-subsequent parent pin hits). Thin edges for ≤tip heights drop
        // (wave already consumed them). Cap 0 = drop bodies immediately.
        //
        // Budget update is O(confirmed batch headers' tx_fks), not O(|by_body|):
        // full `reaccount_body_lru_bytes` scanned ~1.8M bodies every tip advance
        // and held the parent-cache lock long enough that load pin stalled
        // (`pin_sub cover` inflated while write tip_gc ran).
        if g.body_lru_cap == 0 {
            let mut drop_ids: Vec<u64> = Vec::new();
            g.by_body.retain(|id, b| {
                let keep = b.height > tip;
                if !keep {
                    drop_ids.push(*id);
                }
                keep
            });
            for id in drop_ids {
                g.thin_edges.remove(&id);
            }
            g.body_lru.clear();
            g.body_lru_bytes = 0;
        } else {
            // Account runway→confirmed using header plans for newly confirmed
            // heights (before those headers are dropped). O(batch txs).
            if tip > old_tip {
                let mut newly: Vec<u64> = Vec::new();
                for h in old_tip.saturating_add(1)..=tip {
                    let Some(plan) = g.headers.get(&h) else {
                        continue;
                    };
                    for fk in &plan.tx_fks {
                        if let Some(id) = fk.get() {
                            newly.push(id);
                        }
                    }
                }
                for id in newly {
                    // Body create height just crossed into confirmed budget.
                    let add = g.by_body.get(&id).and_then(|b| {
                        if b.height > old_tip && b.height <= tip {
                            Some(b.size_bytes)
                        } else {
                            None
                        }
                    });
                    if let Some(sz) = add {
                        g.body_lru_bytes = g.body_lru_bytes.saturating_add(sz);
                    }
                }
            }
            // Thin edges are only useful on runway; drop for confirmed creates.
            // Scan thin_edges (small) not by_body (~millions).
            if !g.thin_edges.is_empty() {
                let thin_ids: Vec<u64> = g.thin_edges.keys().copied().collect();
                for id in thin_ids {
                    let drop = match g.by_body.get(&id) {
                        Some(b) => b.height <= tip,
                        None => true, // orphan thin
                    };
                    if drop {
                        g.thin_edges.remove(&id);
                    }
                }
            }
            g.evict_body_lru_to_cap();
            // Safety net if stamps/budget drifted (rare): one full reaccount.
            if g.body_lru_bytes > g.body_lru_cap {
                g.reaccount_body_lru_bytes();
                g.rebuild_body_lru_from_map();
                g.evict_confirmed_by_stamp_until_cap();
            }
        }
        // Drop header plan cache for heights at/below tip.
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
        // Drop body_range not tied to live cache bodies.
        g.gc_body_ranges();
        // Munlock when no remaining need_height > tip (write done for those heights).
        let unlocks = g.gc_mlocks(tip);
        g.recompute_ready_through();
        self.ready_through
            .store(g.ready_through, Ordering::Relaxed);
        drop(g);
        // Wake confirm waiters (tip advance can satisfy / re-horizon plans).
        self.ready_cv.notify_all();
        unlocks
    }

    /// Wake any thread blocked in [`Self::wait_heights_ready`] (cancel / shutdown).
    pub fn notify_ready_waiters(&self) {
        self.ready_cv.notify_all();
    }

    /// True if every 4 KiB page of `range` is already held (skip re-`mlock`).
    pub fn is_range_pinned(&self, range: &MlockRange) -> bool {
        if range.is_empty() {
            return true;
        }
        const PAGE: u64 = 4096;
        let start_page = range.page_start / PAGE;
        let end_page = range
            .page_start
            .saturating_add(range.page_len)
            .div_ceil(PAGE)
            .max(start_page);
        let g = self.inner.lock().unwrap();
        let t = range.table.as_u8();
        (start_page..end_page).all(|p| g.page_refs.contains_key(&(t, p)))
    }

    /// Track successful `mlock` ranges for cache height `need_height`.
    ///
    /// Keyed by `(table, page_start)`. If a later note shares `page_start` but
    /// covers a **longer** span, page refs and `rec.range` are **extended** so
    /// kernel-locked pages are never tracked short (under-track ⇒ permanent
    /// mlock leak when GC unlocks only the short range).
    ///
    /// Released when every needing height falls ≤ tip / past horizon.
    pub fn note_mlock_ranges(&self, need_height: u32, ranges: &[MlockRange]) {
        self.note_mlock_ranges_for_heights(&[need_height], ranges);
    }

    /// Like [`Self::note_mlock_ranges`] but one lock for many needing heights
    /// (batch unique-range mlock path).
    pub fn note_mlock_ranges_for_heights(&self, heights: &[u32], ranges: &[MlockRange]) {
        if ranges.is_empty() || heights.is_empty() {
            return;
        }
        const PAGE: u64 = 4096;
        let mut g = self.inner.lock().unwrap();
        for &range in ranges {
            if range.is_empty() {
                continue;
            }
            let table = range.table.as_u8();
            let key = (table, range.page_start);
            let start_page = range.page_start / PAGE;
            let end_page = range
                .page_start
                .saturating_add(range.page_len)
                .div_ceil(PAGE)
                .max(start_page);
            if g.mlocked.contains_key(&key) {
                let old_end = g.mlocked.get(&key).map(|r| r.end_page).unwrap_or(0);
                // Extend page_refs before mutably touching the RangeRec.
                if end_page > old_end {
                    for p in old_end..end_page {
                        *g.page_refs.entry((table, p)).or_insert(0) += 1;
                    }
                }
                if let Some(rec) = g.mlocked.get_mut(&key) {
                    for &h in heights {
                        rec.need_heights.insert(h);
                    }
                    if end_page > rec.end_page {
                        rec.end_page = end_page;
                        rec.range.page_len = end_page
                            .saturating_sub(start_page)
                            .saturating_mul(PAGE);
                    }
                }
                continue;
            }
            for p in start_page..end_page {
                *g.page_refs.entry((table, p)).or_insert(0) += 1;
            }
            let mut need = HashSet::with_capacity(heights.len());
            for &h in heights {
                need.insert(h);
            }
            g.mlocked.insert(
                key,
                RangeRec {
                    range,
                    need_heights: need,
                    start_page,
                    end_page,
                },
            );
            g.mlock_n = g.mlock_n.saturating_add(1);
        }
    }



    /// Number of distinct mlocked page ranges currently tracked.
    pub fn mlock_count(&self) -> usize {
        self.inner.lock().unwrap().mlock_n
    }

    /// Bytes of unique 4 KiB pages currently mlocked for the parent cache.
    ///
    /// Counts distinct `(table, page)` entries under refcount (shared ranges
    /// across heights count once). Approximate RSS contribution of parent pins.
    pub fn mlock_bytes(&self) -> u64 {
        const PAGE: u64 = 4096;
        let g = self.inner.lock().unwrap();
        (g.page_refs.len() as u64).saturating_mul(PAGE)
    }

    /// `(range_count, unique_page_bytes)` for parent pin diagnostics.
    pub fn mlock_stats(&self) -> (usize, u64) {
        const PAGE: u64 = 4096;
        let g = self.inner.lock().unwrap();
        (
            g.mlock_n,
            (g.page_refs.len() as u64).saturating_mul(PAGE),
        )
    }

    /// Cache header + tx list for a cache height (small; replaces header mlock).
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

    /// Cache `tx.idx` body range for `fk` (small; body pages are mlocked separately).
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

    /// Store a full cache block body (phase-1 cache). Confirm/wave should
    /// prefer this over Class A store reads.
    ///
    /// Wave/cache resolve cache creates via [`Self::get_parent_out`] body fallback.
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

    /// Many bodies under **one** lock (load phase-1 finish). Moves ownership.
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

    /// Phase-1 hot path: body only (creates are the body outs).
    ///
    /// Sparse spent-filtered parents live on per-batch [`crate::BatchParents`].
    /// Creates resolve via body outs / thin edges.
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
        // Resolve legacy reserve waiters into plan.need_fk (no sparse by_fk map).
        g.fill_reserve_waiters_from_body(id, txid, height, outputs.len() as u32);
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

    /// True if a full cache body is already stashed (skip store re-decode).
    pub fn has_body(&self, fk: Fk) -> bool {
        let Some(id) = fk.get() else {
            return false;
        };
        self.inner.lock().unwrap().by_body.contains_key(&id)
    }

    /// Thin-edge inputs from a cached body without re-reading the store.
    ///
    /// Each edge is `(create_fk_opt, soft_prev_txid, vout)`. Soft prev_txid is
    /// zero when create_fk is set (v10 disk encoding).
    pub fn body_prevout_edges(
        &self,
        fk: Fk,
    ) -> Option<([u8; 32], Vec<(Option<u64>, [u8; 32], u32)>)> {
        let id = fk.get()?;
        let mut g = self.inner.lock().unwrap();
        let e = g.by_body.get(&id)?;
        let prevouts: Vec<(Option<u64>, [u8; 32], u32)> = e
            .inputs
            .iter()
            .map(|i| {
                let soft = if i.create_fk.is_null() {
                    i.prev_txid
                } else {
                    [0u8; 32]
                };
                (i.create_fk.get(), soft, i.prev_index)
            })
            .collect();
        let txid = e.tx.txid;
        g.touch_body_lru(id);
        Some((txid, prevouts))
    }

    /// Clone a full body; keeps entry for post-confirm LRU / retries.
    pub fn take_body(
        &self,
        fk: Fk,
    ) -> Option<(TxRecord, Vec<OutputRecord>, Vec<InputRecord>)> {
        let id = fk.get()?;
        let mut g = self.inner.lock().unwrap();
        let e = g.by_body.get(&id)?;
        let out = (e.tx.clone(), e.outputs.clone(), e.inputs.clone());
        g.touch_body_lru(id);
        Some(out)
    }

    /// Clone many bodies under **one** lock (keeps post-confirm LRU).
    ///
    /// Compatibility alias for [`Self::get_bodies_batch`].
    pub fn take_bodies_batch(
        &self,
        fks: &[Fk],
    ) -> HashMap<u64, (TxRecord, Vec<OutputRecord>, Vec<InputRecord>)> {
        self.get_bodies_batch(fks)
    }

    /// Clone many bodies under **one** lock (keeps cache intact for retries).
    pub fn get_bodies_batch(
        &self,
        fks: &[Fk],
    ) -> HashMap<u64, (TxRecord, Vec<OutputRecord>, Vec<InputRecord>)> {
        if fks.is_empty() {
            return HashMap::new();
        }
        let t0 = std::time::Instant::now();
        let mut g = self.inner.lock().unwrap();
        crate::wave_fill_stats::add(
            &crate::wave_fill_stats::CACHE_LOCK_WAIT_NS,
            t0.elapsed().as_nanos() as u64,
        );
        let mut out = HashMap::with_capacity(fks.len());
        for &fk in fks {
            let Some(id) = fk.get() else {
                continue;
            };
            if let Some(e) = g.by_body.get(&id) {
                out.insert(id, (e.tx.clone(), e.outputs.clone(), e.inputs.clone()));
                g.touch_body_lru(id);
            }
        }
        out
    }

    /// Full body for parent pin (runway or post-confirm LRU).
    ///
    /// Prefer this over a store re-decode when the create is already full-decoded.
    ///
    /// **Hot path:** use [`Self::get_bodies_for_pin_batch`] — this clones **all**
    /// inputs/outputs and takes the cache lock per call (scales poorly with
    /// `|by_body|` under IBD pin volume).
    pub fn get_body_for_pin(
        &self,
        fk: Fk,
    ) -> Option<(u32, TxRecord, Vec<OutputRecord>, Vec<InputRecord>)> {
        let id = fk.get()?;
        let mut g = self.inner.lock().unwrap();
        let e = g.by_body.get(&id)?;
        let out = (
            e.height,
            e.tx.clone(),
            e.outputs.clone(),
            e.inputs.clone(),
        );
        g.touch_body_lru(id);
        Some(out)
    }

    /// Slim pin hits under **one** lock: only clone requested outs + tx meta.
    ///
    /// `items`: `(create_fk_id, need_vouts)`. Missing bodies omitted.
    ///
    /// Returns `id → (create_height, tx, outs, coinbase_hint, body_range)`:
    /// - `outs`: only requested vouts that exist on the body (not spent-filtered)
    /// - `coinbase_hint`: `Some(true)` clearly coinbase, `Some(false)` not,
    ///   `None` ambiguous 1-in (caller may store-resolve)
    /// - `body_range`: cached absolute range if known
    ///
    /// Touches body LRU once per hit. Avoids cloning full witness/input vectors
    /// that made per-parent pin scale with uptime as `|by_body|` grew.
    pub fn get_bodies_for_pin_batch(
        &self,
        items: &[(u64, Vec<u32>)],
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
        let mut g = self.inner.lock().unwrap();
        let mut out = HashMap::with_capacity(items.len());
        for (id, vouts) in items {
            let hit = {
                let Some(e) = g.by_body.get(id) else {
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
                } else if e
                    .inputs
                    .first()
                    .is_some_and(|i| i.is_coinbase() || i.prev_index == u32::MAX)
                {
                    Some(true)
                } else {
                    None
                };
                let range = g.body_range.get(id).copied();
                (e.height, e.tx.clone(), outs, cb_hint, range)
            };
            g.touch_body_lru(*id);
            out.insert(*id, hit);
        }
        out
    }

    /// `(body_count, confirmed_lru_bytes, lru_cap_bytes, lazy_deque_len)` for perf/tests.
    ///
    /// `lazy_deque_len` should stay O(body_count); if it grows to many× body_count,
    /// compaction is lagging and load pin will slow with runtime.
    pub fn body_lru_stats(&self) -> (usize, u64, u64, usize) {
        let g = self.inner.lock().unwrap();
        (
            g.by_body.len(),
            g.body_lru_bytes,
            g.body_lru_cap,
            g.body_lru.len(),
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
                .map(|id| g.by_body.contains_key(&id))
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
        let g = self.inner.lock().unwrap();
        // Prefer full-decoded cache bodies (decode-once cache); else mlock pins.
        if !g.by_body.is_empty() {
            g.by_body.len()
        } else {
            g.mlock_n
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
                need_fk: HashSet::new(),
                reserved: HashSet::new(),
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

    /// True if height still has open reserved holes (debug / tests).
    pub fn has_open_reserves(&self, height: u32) -> bool {
        let g = self.inner.lock().unwrap();
        g.plans
            .get(&height)
            .is_some_and(|p| !p.reserved.is_empty())
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
            if let Some(e) = g.by_body.get_mut(&id) {
                e.thin_inputs = Some(edges.clone());
            }
            g.thin_edges.insert(id, edges);
        }
    }

    /// Reserve a hole for a prevout whose create is still on the runway.
    ///
    /// Filled when the create body lands ([`Self::put_body_and_creates`]) via
    /// body txid match — no sparse by_fk map.
    pub fn reserve(&self, height: u32, prev_txid: [u8; 32], vout: u32) {
        let mut g = self.inner.lock().unwrap();
        // Already have create body with matching txid?
        if let Some((&id, b)) = g.by_body.iter().find(|(_, b)| b.tx.txid == prev_txid) {
            if (vout as usize) < b.outputs.len() {
                if let Some(plan) = g.plans.get_mut(&height) {
                    plan.need_fk.insert((id, vout));
                    plan.reserved.remove(&(prev_txid, vout));
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
        if let Some(b) = g.by_body.get(&id) {
            let o = b.outputs.get(vout as usize)?;
            return Some((b.tx.clone(), o.clone()));
        }
        None
    }

    /// True if vout is present on a cached body — no record clone.
    pub fn has_parent_out(&self, fk: Fk, vout: u32) -> bool {
        let Some(id) = fk.get() else {
            return false;
        };
        let g = self.inner.lock().unwrap();
        g.by_body
            .get(&id)
            .is_some_and(|b| (vout as usize) < b.outputs.len())
    }









    pub fn get_parent_tx(&self, fk: Fk) -> Option<TxRecord> {
        let id = fk.get()?;
        let g = self.inner.lock().unwrap();
        g.by_body.get(&id).map(|b| b.tx.clone())
    }

    /// Txid of a stashed parent create body — no clone of outs.
    pub fn get_parent_txid(&self, fk: Fk) -> Option<[u8; 32]> {
        let id = fk.get()?;
        let g = self.inner.lock().unwrap();
        g.by_body.get(&id).map(|b| b.tx.txid)
    }



    pub fn plan_count(&self) -> usize {
        self.inner.lock().unwrap().plans.len()
    }


    /// Cached body-range entries (idx offsets for mlock/wave).
    pub fn body_range_count(&self) -> usize {
        self.inner.lock().unwrap().body_range.len()
    }

    pub fn reserved_count(&self) -> usize {
        self.inner.lock().unwrap().reserve_waiters.len()
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
        let size_bytes = estimate_body_bytes(&tx, &outputs, &inputs);
        if let Some(old) = self.by_body.remove(&id) {
            if old.height <= self.tip {
                self.body_lru_bytes = self.body_lru_bytes.saturating_sub(old.size_bytes);
            }
        }
        let stamp = self.next_lru_stamp;
        self.next_lru_stamp = self.next_lru_stamp.saturating_add(1);
        self.by_body.insert(
            id,
            BodyEntry {
                height,
                tx,
                outputs,
                inputs,
                thin_inputs: None,
                size_bytes,
                lru_stamp: stamp,
            },
        );
        self.body_lru.push_back((id, stamp));
        if height <= self.tip {
            self.body_lru_bytes = self.body_lru_bytes.saturating_add(size_bytes);
            self.evict_body_lru_to_cap();
        }
        self.maybe_compact_body_lru();
    }

    fn touch_body_lru(&mut self, id: u64) {
        let Some(e) = self.by_body.get_mut(&id) else {
            return;
        };
        let stamp = self.next_lru_stamp;
        self.next_lru_stamp = self.next_lru_stamp.saturating_add(1);
        e.lru_stamp = stamp;
        self.body_lru.push_back((id, stamp));
        self.maybe_compact_body_lru();
    }

    fn reaccount_body_lru_bytes(&mut self) {
        let tip = self.tip;
        self.body_lru_bytes = self
            .by_body
            .values()
            .filter(|b| b.height <= tip)
            .map(|b| b.size_bytes)
            .sum();
    }

    /// Evict oldest **confirmed** (`height ≤ tip`) bodies until under cap.
    ///
    /// Runway bodies (`height > tip`) are never evicted. Work is **one linear
    /// pass** of the lazy deque (plus optional compact): runway stamps are held
    /// aside and restored after — no O(deque) `.all()` per pop.
    fn evict_body_lru_to_cap(&mut self) {
        if self.body_lru_cap == 0 {
            return;
        }
        // Drop stale stamps first so the pass is proportional to live entries.
        self.compact_body_lru_stale();
        if self.body_lru_bytes <= self.body_lru_cap {
            return;
        }
        let tip = self.tip;
        let pass_n = self.body_lru.len();
        let mut hold: VecDeque<(u64, u64)> = VecDeque::with_capacity(pass_n.min(1024));
        for _ in 0..pass_n {
            if self.body_lru_bytes <= self.body_lru_cap {
                break;
            }
            let Some((id, stamp)) = self.body_lru.pop_front() else {
                break;
            };
            let Some(e) = self.by_body.get(&id) else {
                continue; // gone; discard stamp
            };
            if e.lru_stamp != stamp {
                continue; // superseded by a newer touch
            }
            if e.height > tip {
                // Protected runway — hold aside (no full-queue rescan).
                hold.push_back((id, stamp));
                continue;
            }
            let size = e.size_bytes;
            self.by_body.remove(&id);
            self.thin_edges.remove(&id);
            self.body_lru_bytes = self.body_lru_bytes.saturating_sub(size);
        }
        // Restore runway / non-evicted live stamps (confirmed survivors stay
        // ahead in whatever order remained after the pass).
        while let Some(x) = hold.pop_front() {
            self.body_lru.push_back(x);
        }
        // Safety: if budget still high (stamp desync), rebuild from by_body.
        if self.body_lru_bytes > self.body_lru_cap {
            self.reaccount_body_lru_bytes();
            self.rebuild_body_lru_from_map();
            self.evict_confirmed_by_stamp_until_cap();
        }
    }

    /// Rebuild lazy deque as one live stamp per `by_body` entry (oldest first).
    fn rebuild_body_lru_from_map(&mut self) {
        let mut items: Vec<(u64, u64)> = self
            .by_body
            .iter()
            .map(|(&id, e)| (id, e.lru_stamp))
            .collect();
        items.sort_unstable_by_key(|(_, stamp)| *stamp);
        self.body_lru = items.into();
    }

    /// After rebuild: drop oldest confirmed until under cap (no lazy stamps).
    fn evict_confirmed_by_stamp_until_cap(&mut self) {
        if self.body_lru_cap == 0 {
            return;
        }
        let tip = self.tip;
        // Walk oldest→newest; remove confirmed until under budget.
        let mut keep: VecDeque<(u64, u64)> =
            VecDeque::with_capacity(self.by_body.len().saturating_add(8));
        // Drain in stamp order already in body_lru.
        while let Some((id, stamp)) = self.body_lru.pop_front() {
            let Some(e) = self.by_body.get(&id) else {
                continue;
            };
            if e.lru_stamp != stamp {
                continue;
            }
            if e.height > tip {
                keep.push_back((id, stamp));
                continue;
            }
            if self.body_lru_bytes > self.body_lru_cap {
                let size = e.size_bytes;
                self.by_body.remove(&id);
                self.thin_edges.remove(&id);
                self.body_lru_bytes = self.body_lru_bytes.saturating_sub(size);
                continue;
            }
            keep.push_back((id, stamp));
        }
        self.body_lru = keep;
    }

    /// Keep lazy deque from growing unbounded with touch stamps.
    ///
    /// Compact when `body_lru.len() > 2 * by_body.len()` (and at least 64), so
    /// pin/touch paths stay O(bodies) amortized rather than O(touches).
    fn maybe_compact_body_lru(&mut self) {
        let n_body = self.by_body.len();
        let limit = n_body.saturating_mul(2).max(64);
        if self.body_lru.len() <= limit {
            return;
        }
        self.compact_body_lru_stale();
    }

    /// Drop superseded stamps and entries no longer in `by_body`. O(deque).
    fn compact_body_lru_stale(&mut self) {
        if self.body_lru.is_empty() {
            return;
        }
        let mut kept = VecDeque::with_capacity(self.by_body.len().saturating_add(64));
        while let Some((id, stamp)) = self.body_lru.pop_front() {
            if self
                .by_body
                .get(&id)
                .is_some_and(|e| e.lru_stamp == stamp)
            {
                kept.push_back((id, stamp));
            }
        }
        self.body_lru = kept;
    }






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
            let Some(body) = self.by_body.get(&id) else {
                return false;
            };
            let thin = self.thin_edges.get(&id);
            for (i, inp) in body.inputs.iter().enumerate() {
                if inp.is_coinbase()
                    || (inp.prev_txid == [0u8; 32] && inp.prev_index == u32::MAX)
                {
                    continue;
                }
                let pid = thin
                    .and_then(|t| t.get(i))
                    .and_then(|e| e.create_fk)
                    .or_else(|| inp.create_fk.get());
                if pid.is_none() {
                    return false; // unstamped create_fk
                }
            }
        }
        true
    }

    /// Drop body_range entries not referenced by live cache bodies.
    fn gc_body_ranges(&mut self) {
        if self.body_range.is_empty() {
            return;
        }
        self.body_range
            .retain(|id, _| self.by_body.contains_key(id));
    }

    /// Resolve legacy reserve waiters when a create body lands (by txid).
    fn fill_reserve_waiters_from_body(
        &mut self,
        id: u64,
        txid: [u8; 32],
        _height: u32,
        n_outputs: u32,
    ) {
        if self.reserve_waiters.is_empty() {
            return;
        }
        for v in 0..n_outputs {
            let key = (txid, v);
            let Some(waiters) = self.reserve_waiters.remove(&key) else {
                continue;
            };
            for h in waiters {
                if let Some(plan) = self.plans.get_mut(&h) {
                    plan.reserved.remove(&key);
                    plan.need_fk.insert((id, v));
                }
            }
        }
    }

    /// Drop mlocks whose needing heights are all ≤ tip.
    /// Returns ranges with full page-ref zero for the caller to `munlock`.
    fn gc_mlocks(&mut self, tip: u32) -> Vec<MlockRange> {
        let mut drop_keys: Vec<(u8, u64)> = Vec::new();
        for (key, rec) in &mut self.mlocked {
            rec.need_heights.retain(|h| *h > tip);
            if rec.need_heights.is_empty() {
                drop_keys.push(*key);
            }
        }
        let mut unlocks: Vec<MlockRange> = Vec::new();
        for key in drop_keys {
            let Some(rec) = self.mlocked.remove(&key) else {
                continue;
            };
            self.mlock_n = self.mlock_n.saturating_sub(1);
            let table = key.0;
            for p in rec.start_page..rec.end_page {
                let pk = (table, p);
                let entry = self.page_refs.entry(pk).or_insert(0);
                *entry = entry.saturating_sub(1);
                if *entry == 0 {
                    self.page_refs.remove(&pk);
                }
            }
            let still = (rec.start_page..rec.end_page)
                .any(|p| self.page_refs.contains_key(&(table, p)));
            if !still {
                unlocks.push(rec.range);
            }
        }
        unlocks
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
    fn take_bodies_batch_records_lock_wait_counter() {
        use crate::wave_fill_stats;
        let c = ConfirmParentCache::new();
        c.advance_tip(0);
        let mut t = tx(1);
        t.txid = [9u8; 32];
        c.put_body(
            Fk(100),
            1,
            t,
            vec![out(50)],
            vec![InputRecord {
                prev_txid: [0u8; 32],
                create_fk: Fk::NULL,
                prev_index: u32::MAX,
                sequence: 0xffff_ffff,
                script_sig: vec![],
                witness: vec![],
            }],
        );
        let _ = wave_fill_stats::sample_io_and_reset();
        let got = c.take_bodies_batch(&[Fk(100)]);
        assert_eq!(got.len(), 1);
        let (_store_ns, lock_ns) = wave_fill_stats::sample_io_and_reset();
        // Lock wait is usually tiny but the counter path must execute.
        let _ = lock_ns;
    }



    #[test]
    fn open_reserves_do_not_block_ready_or_watermark() {
        // Simulate batch create@1 + spend@2: open reserves must not block package.
        let c = ConfirmParentCache::new();
        c.advance_tip(0);
        seed_coinbase_package(&c, 1, [1u8; 32], 1001);
        seed_coinbase_package(&c, 2, [2u8; 32], 1002);
        let t = tx(9);
        c.reserve(2, t.txid, 0);
        assert!(c.is_ready(1));
        assert!(c.is_ready(2));
        assert!(c.has_open_reserves(2));
        assert!(c.all_ready(&[1, 2]));
        assert_eq!(c.ready_through(), 2);
        assert!(c.headroom_ready(2, 0));
        c.put_body_and_creates(Fk(90), 1, t, vec![out(1)], vec![]);
        assert!(!c.has_open_reserves(2));
        assert_eq!(c.ready_through(), 2);
    }


    #[test]
    fn body_cache_survives_past_tip_in_lru() {
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
        assert!(c.get_body(Fk(50)).is_some());
        assert!(c.has_body(Fk(50)));
        c.advance_tip(0);
        assert!(c.get_body(Fk(50)).is_some());
        // After tip past create height, body stays in confirmed LRU (default 1 GiB).
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
        let (txid, edges) = c.body_prevout_edges(Fk(90)).unwrap();
        assert_eq!(txid, t.txid);
        assert_eq!(edges.len(), 2);
        assert_eq!(edges[0].0, Some(3));
        assert_eq!(edges[0].1, [0u8; 32]); // soft zero when create_fk set
        assert_eq!(edges[0].2, 1);
        assert_eq!(edges[1].0, None);
        assert_eq!(edges[1].2, u32::MAX);
    }


    #[test]
    fn body_create_resolves_from_by_body() {
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
    fn thin_inputs_stash_on_body() {
        let c = ConfirmParentCache::new();
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
    fn take_bodies_batch_keeps_for_parent_lru() {
        // Wave clones bodies; entries stay for post-confirm parent pin hits.
        let c = ConfirmParentCache::new();
        c.advance_tip(0);
        let t1 = tx(1);
        let t2 = tx(2);
        c.put_bodies_batch(vec![
            (Fk(1), 1, t1.clone(), vec![out(10)], vec![]),
            (Fk(2), 1, t2.clone(), vec![out(20)], vec![]),
        ]);
        c.put_body_range(Fk(1), 100, 50);
        c.put_body_range(Fk(2), 200, 60);
        let taken = c.take_bodies_batch(&[Fk(1), Fk(2), Fk(3)]);
        assert_eq!(taken.len(), 2);
        assert_eq!(taken.get(&1).unwrap().0.txid, t1.txid);
        assert_eq!(taken.get(&2).unwrap().1[0].value, 20);
        assert!(c.has_body(Fk(1)));
        assert!(c.has_body(Fk(2)));
        assert_eq!(c.get_body_range(Fk(1)), Some((100, 50)));
        // After tip past create height, bodies remain in LRU (default 1 GiB).
        c.advance_tip(10);
        assert!(c.has_body(Fk(1)));
        assert!(c.get_body_for_pin(Fk(2)).is_some());
    }

    #[test]
    fn body_lru_evicts_oldest_confirmed_under_tiny_cap() {
        // Cap 0: legacy drop-at-tip (setter avoids process-env races with other tests).
        let c = ConfirmParentCache::new();
        c.set_body_lru_cap_bytes(0);
        c.advance_tip(0);
        c.put_bodies_batch(vec![(Fk(1), 1, tx(1), vec![out(10)], vec![])]);
        assert!(c.has_body(Fk(1)));
        c.advance_tip(1);
        assert!(
            !c.has_body(Fk(1)),
            "cap 0 must drop bodies at tip"
        );
    }

    #[test]
    fn get_bodies_batch_keeps_cache_for_retry() {
        // Worker-live confirm clones so a failed package can re-queue.
        let c = ConfirmParentCache::new();
        c.advance_tip(0);
        let t1 = tx(1);
        c.put_bodies_batch(vec![(Fk(1), 1, t1.clone(), vec![out(10)], vec![])]);
        let got = c.get_bodies_batch(&[Fk(1)]);
        assert_eq!(got.len(), 1);
        assert_eq!(got.get(&1).unwrap().0.txid, t1.txid);
        assert!(c.has_body(Fk(1)));
        assert!(c.bodies_complete(&[Fk(1)]));
        assert!(!c.bodies_complete(&[Fk(1), Fk(2)]));
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

    /// Same page_start, longer later note must extend page_refs (mlock leak class).
    #[test]
    fn note_mlock_extends_shorter_prior_range() {
        use rbitcoin_store::{MlockRange, MlockTable};
        let c = ConfirmParentCache::new();
        c.advance_tip(0);
        let short = MlockRange {
            table: MlockTable::TxBody,
            page_start: 0,
            page_len: 4096, // 1 page
        };
        let long = MlockRange {
            table: MlockTable::TxBody,
            page_start: 0,
            page_len: 4096 * 4, // 4 pages
        };
        c.note_mlock_ranges(1, &[short]);
        assert_eq!(c.mlock_bytes(), 4096);
        c.note_mlock_ranges(2, &[long]);
        // Must track all 4 pages, not stay stuck at 1.
        assert_eq!(c.mlock_bytes(), 4096 * 4);
        // need_heights are 1 and 2 — tip past both → unlock after write tip GC.
        let unlocks = c.advance_tip(10);
        assert!(
            unlocks.iter().any(|r| r.page_len >= 4096 * 4),
            "expected unlock of extended range, got {unlocks:?}"
        );
        assert_eq!(c.mlock_bytes(), 0);
    }

    /// Mlocks release at tip once no remaining need_height > tip (write done).
    /// Later cache heights that still need a page keep it locked.
    #[test]
    fn advance_tip_munlocks_when_write_done_keeps_later_needs() {
        use rbitcoin_store::{MlockRange, MlockTable};
        let c = ConfirmParentCache::new();
        c.advance_tip(0);
        let r = MlockRange {
            table: MlockTable::TxBody,
            page_start: 4096,
            page_len: 4096,
        };
        // Same pages needed by height 5 and height 20.
        c.note_mlock_ranges(5, &[r]);
        c.note_mlock_ranges(20, &[r]);
        assert_eq!(c.mlock_stats().0, 1);
        // Write finished through 5 — height 20 still needs the page.
        let unlocks = c.advance_tip(5);
        assert!(unlocks.is_empty(), "later cache need keeps mlock: {unlocks:?}");
        assert_eq!(c.mlock_stats().0, 1);
        // Write finished through 20 — no remaining need → munlock.
        let unlocks = c.advance_tip(20);
        assert_eq!(unlocks.len(), 1, "munlock when write done for all needers");
        assert_eq!(c.mlock_stats().0, 0);
    }

    /// Materialize batches re-note the same parent body page for later heights
    /// (covered pin path). Without re-note, tip GC after the first batch would
    /// munlock while a later in-flight batch still needs the page for annotate.
    #[test]
    fn re_note_need_height_after_first_batch_keeps_mlock_across_pipeline() {
        use rbitcoin_store::{MlockRange, MlockTable};
        let c = ConfirmParentCache::new();
        c.advance_tip(0);
        let r = MlockRange {
            table: MlockTable::TxBody,
            page_start: 8192,
            page_len: 4096,
        };
        // Batch A (height 10) mlocks parent body.
        c.note_mlock_ranges(10, &[r]);
        assert!(c.is_range_pinned(&r));
        // Batch B (height 11) finds pin covered — must still re-note need 11.
        c.note_mlock_ranges(11, &[r]);
        // Writeback A advances tip through 10; B still in flight.
        let unlocks = c.advance_tip(10);
        assert!(
            unlocks.is_empty(),
            "batch B need_height must keep parent body mlocked: {unlocks:?}"
        );
        assert!(c.is_range_pinned(&r));
        // Writeback B done.
        let unlocks = c.advance_tip(11);
        assert_eq!(unlocks.len(), 1);
        assert!(!c.is_range_pinned(&r));
    }

    /// Synthetic pressure: many full bodies must leave RAM when tip catches up.
    #[test]
    fn large_cache_bodies_do_not_accumulate_past_tip() {
        let c = ConfirmParentCache::new();
        c.advance_tip(0);
        // ~2k "txs" per height × 64 heights — enough to trip unbounded hold.
        for h in 1u32..=64 {
            c.ensure_plan(h, [h as u8; 32]);
            for i in 0u32..32 {
                let id = (h as u64) * 1000 + i as u64;
                let mut t = tx((i & 0xff) as u8);
                t.txid[0] = h as u8;
                t.txid[1] = i as u8;
                // Fat-ish scripts (mainnet-like weight on a small scale).
                let outs = vec![out(50); 4];
                let ins = vec![InputRecord {
                    prev_txid: [0u8; 32],
                    create_fk: Fk::NULL,
                    prev_index: u32::MAX,
                    sequence: 0xffff_ffff,
                    script_sig: vec![0x51; 64],
                    witness: vec![vec![0u8; 32]],
                }];
                c.put_body(Fk(id), h, t, outs, ins);
            }
            c.mark_scanned(h);
        }
        assert_eq!(c.body_count(), 64 * 32);
        // Advance tip through all — bodies may remain in the confirmed LRU (≤ cap),
        // not unbounded growth beyond the byte budget.
        c.advance_tip(64);
        let (n, lru_bytes, cap, deque_len) = c.body_lru_stats();
        assert!(
            lru_bytes <= cap || cap == 0,
            "confirmed body LRU over budget: bytes={lru_bytes} cap={cap} n={n}"
        );
        // With default 1 GiB cap, 64×32 small synthetic bodies fit; still bounded.
        assert!(n <= 64 * 32);
        // Lazy deque must not grow unboundedly vs live map (runtime pin tax).
        assert!(
            deque_len <= n.saturating_mul(2).max(64),
            "lazy body_lru deque too long: deque={deque_len} bodies={n}"
        );
    }

    /// Many pin touches must not leave a multi×-bodies stamp queue; eviction with
    /// runway mixed in must not full-scan the deque per pop.
    #[test]
    fn body_lru_touch_compacts_and_evict_skips_runway_without_all_scan() {
        // Tiny cap: force eviction of confirmed while runway bodies stay.
        let c = ConfirmParentCache::new();
        c.set_body_lru_cap_bytes(1024 * 1024); // 1 MiB
        c.advance_tip(0);
        // Fat bodies so a few fill 1 MiB.
        let fat_out = || {
            vec![OutputRecord::unspent(1, vec![0x51; 8 * 1024]); 8]
        };
        // Confirmed-height bodies after tip advance.
        for i in 0..40u64 {
            let mut t = tx((i & 0xff) as u8);
            t.txid[0] = i as u8;
            c.put_body(Fk(i + 1), 1, t, fat_out(), vec![]);
        }
        // Runway bodies (height 10 > tip).
        for i in 0..20u64 {
            let mut t = tx(((i + 40) & 0xff) as u8);
            t.txid[0] = 0x80 | (i as u8);
            c.put_body(Fk(1000 + i), 10, t, fat_out(), vec![]);
        }
        c.advance_tip(1); // confirmed set enters LRU budget; must stay ≤ cap
        let (n, bytes, cap, _deque0) = c.body_lru_stats();
        assert!(bytes <= cap, "over cap bytes={bytes} cap={cap} n={n}");
        // Spam touches (lazy stamps) — must compact.
        for _ in 0..200 {
            for i in 0..20u64 {
                let _ = c.get_body_for_pin(Fk(1000 + i)); // runway pin touches
            }
        }
        let (n2, bytes2, cap2, deque2) = c.body_lru_stats();
        assert!(bytes2 <= cap2);
        assert!(
            deque2 <= n2.saturating_mul(2).max(64),
            "deque grew with touches: deque={deque2} bodies={n2}"
        );
        // Runway still present.
        assert!(c.has_body(Fk(1000)));
    }

    #[test]
    fn note_mlock_ranges_for_heights_unions_needs() {
        let c = ConfirmParentCache::new();
        c.advance_tip(0);
        let r = MlockRange {
            table: rbitcoin_store::MlockTable::TxBody,
            page_start: 0,
            page_len: 4096,
        };
        c.note_mlock_ranges_for_heights(&[5, 6, 7], &[r]);
        assert!(c.is_range_pinned(&r));
        // Tip past 5 but not 6/7 — range still held.
        let unlocks = c.advance_tip(5);
        assert!(unlocks.is_empty(), "heights 6,7 still need pages");
        assert!(c.is_range_pinned(&r));
        let unlocks = c.advance_tip(7);
        assert!(!unlocks.is_empty());
        assert!(!c.is_range_pinned(&r));
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

        let hits = c.get_bodies_for_pin_batch(&[(77, vec![0, 2])]);
        let (h, txr, outs, cb, range) = hits.get(&77).expect("hit");
        assert_eq!(*h, 5);
        assert_eq!(txr.txid, t.txid);
        assert_eq!(outs.len(), 2);
        assert_eq!(outs[0].0, 0);
        assert_eq!(outs[1].0, 2);
        assert_eq!(*cb, Some(true));
        assert_eq!(*range, Some((1000, 64)));
        // Spent-filtered pin lives on BatchParents (not shared by_fk).
        let mut bp = crate::BatchParents::new();
        bp.put_resolved(
            Fk(77),
            txr.clone(),
            &[(0, outs[0].1.clone()), (2, outs[1].1.clone())],
            &[0, 2],
            Some(Some(5)),
            Some(5),
        );
        assert!(bp.pin_covered(Fk(77), &[0, 2]));
        assert!(!bp.pin_covered(Fk(77), &[0, 1]));
    }
}
