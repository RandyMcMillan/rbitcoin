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

/// Default grace heights added past last known need when stamping `keep_until`.
///
/// Cross-batch re-spends of the same parent are common; with empty conf_q load
/// depth, tip GC would otherwise drop `by_fk` before the next load sees them.
/// `256` ≈ 8 × 32-blk batches. `RBITCOIN_CONFIRM_PIN_KEEP_GRACE=0` = strict
/// (need+1 exclusive only).
pub const DEFAULT_PIN_KEEP_GRACE: u32 = 256;

/// `RBITCOIN_CONFIRM_PIN_KEEP_GRACE` (default [`DEFAULT_PIN_KEEP_GRACE`]).
pub fn pin_keep_grace_from_env() -> u32 {
    std::env::var("RBITCOIN_CONFIRM_PIN_KEEP_GRACE")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(DEFAULT_PIN_KEEP_GRACE)
}

/// Exclusive retention end: drop when `tip >= keep_until`.
///
/// `need + 1` keeps the entry through tip == need (write just finished that
/// height); `+ grace` covers unknown future re-spends without load queue depth.
#[inline]
pub fn pin_keep_until_for(need_height: u32, grace: u32) -> u32 {
    need_height
        .saturating_add(1)
        .saturating_add(grace)
}

/// One needed prevout under a parent create.
#[derive(Debug, Clone)]
pub struct ParentOut {
    pub output: OutputRecord,
}

/// Parent create row held for the parent cache.
#[derive(Debug, Clone)]
pub struct ParentEntry {
    pub tx: TxRecord,
    /// Live (unspent) needed vouts → output. Spent vouts are omitted.
    pub outs: HashMap<u32, ParentOut>,
    /// Vouts that cache fully resolved (spent-filtered). When all requested
    /// vouts are in this set, wave can skip store decode + spent re-check.
    pub checked: HashSet<u32>,
    /// Coinbase maturity height resolved at cache (wave skips body re-walk).
    ///
    /// - `None` = not resolved yet
    /// - `Some(None)` = not a coinbase
    /// - `Some(Some(h))` = coinbase created at height `h`
    pub coinbase_height: Option<Option<u32>>,
    /// Height of the parent cache body that registered this create (`None` = UTXO load).
    pub create_height: Option<u32>,
    /// Exclusive end height for tip GC retention: keep while `keep_until > tip`
    /// even if plan.need_fk was dropped. Stamped as `need + 1 + pin_keep_grace`
    /// so empty conf_q / cross-batch re-spends still hit `pin_cached`.
    pub keep_until: u32,
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
    /// Parent bodies keyed by create fk id (optional; tests / legacy).
    by_fk: HashMap<u64, ParentEntry>,
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
    /// Heights past last known need retained in `by_fk` (see `pin_keep_until_for`).
    pin_keep_grace: u32,
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
                by_fk: HashMap::new(),
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
                pin_keep_grace: pin_keep_grace_from_env(),
            }),
            ready_cv: Condvar::new(),
            ready_through: AtomicU32::new(0),
        }
    }

    pub fn from_env() -> Self {
        Self::new()
    }

    /// Override pin keep-alive grace (tests / ops). Default from env at `new`.
    pub fn set_pin_keep_grace(&self, grace: u32) {
        self.inner.lock().unwrap().pin_keep_grace = grace;
    }

    /// Current pin keep-alive grace heights.
    pub fn pin_keep_grace(&self) -> u32 {
        self.inner.lock().unwrap().pin_keep_grace
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
            // Recompute confirmed-byte total; thin edges for ≤tip only.
            g.reaccount_body_lru_bytes();
            let drop_thin: Vec<u64> = g
                .by_body
                .iter()
                .filter(|(_, b)| b.height <= tip)
                .map(|(id, _)| *id)
                .collect();
            for id in drop_thin {
                g.thin_edges.remove(&id);
            }
            g.evict_body_lru_to_cap();
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
        g.gc_orphaned_parents();
        // Drop body_range not tied to live cache bodies or parent pins.
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
    /// Does **not** clone every output into `by_fk` — that doubled RAM/CPU on
    /// mainnet (~all scripts twice). Wave/cache resolve cache creates via
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

    /// Batch parent outs under one lock (load phase-2 finish).
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

    /// Runway parent pin: stash live outs + mark all evaluated vouts checked.
    ///
    /// `checked` includes spent-filtered vouts that are **not** in `live` so
    /// wave can treat the set as complete without re-decoding the body.
    /// `height` is the max cache height needing this parent (plan keep-alive).
    /// `coinbase_height`: `None` = not a coinbase; `Some(h)` = cb create height.
    /// Pass as pre-resolved maturity field (outer `Some` means stashed).
    pub fn put_parent_outs_resolved(
        &self,
        height: u32,
        fk: Fk,
        tx: TxRecord,
        live: &[(u32, OutputRecord)],
        checked: &[u32],
        coinbase_height: Option<Option<u32>>,
    ) {
        let Some(id) = fk.get() else {
            return;
        };
        let mut g = self.inner.lock().unwrap();
        g.put_parent_outs_resolved_inner(
            height,
            id,
            tx,
            live,
            checked,
            coinbase_height,
            None,
        );
    }

    /// Many resolved parents under one lock (parent pin finish).
    ///
    /// Tuple: `(height, fk, tx, live, checked, coinbase_height, create_height)`.
    /// `coinbase_height`: `None` = not stashed; `Some(None)` = not cb; `Some(Some(h))` = height.
    /// `create_height`: body height when known (pin_cache / runway) for GC keep-alive.
    pub fn put_parent_outs_resolved_batch(
        &self,
        items: &[(
            u32,
            Fk,
            TxRecord,
            Vec<(u32, OutputRecord)>,
            Vec<u32>,
            Option<Option<u32>>,
            Option<u32>,
        )],
    ) {
        if items.is_empty() {
            return;
        }
        let mut g = self.inner.lock().unwrap();
        for (height, fk, tx, live, checked, cb, create_h) in items {
            let Some(id) = fk.get() else {
                continue;
            };
            g.put_parent_outs_resolved_inner(
                *height,
                id,
                tx.clone(),
                live,
                checked,
                *cb,
                *create_h,
            );
        }
    }

    /// Load-resolved coinbase maturity for a create fk.
    ///
    /// - `None` = not stashed (wave must compute)
    /// - `Some(None)` = not a coinbase
    /// - `Some(Some(h))` = coinbase at height `h`
    pub fn get_parent_coinbase_height(&self, fk: Fk) -> Option<Option<u32>> {
        let id = fk.get()?;
        // Field is already Option<Option<u32>>; missing entry / unset → None.
        self.inner.lock().unwrap().by_fk.get(&id)?.coinbase_height
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

    /// Reserve a hole for a prevout not in UTXO (create is still on the parent cache).
    ///
    /// If the create was already registered with full outs (create-before-reserve),
    /// fills immediately without a store round-trip.
    pub fn reserve(&self, height: u32, prev_txid: [u8; 32], vout: u32) {
        let mut g = self.inner.lock().unwrap();
        // Already filled from sparse parent pin (legacy reserve path)?
        if let Some((&id, _)) = g
            .by_fk
            .iter()
            .find(|(_, e)| e.tx.txid == prev_txid && e.outs.contains_key(&vout))
        {
            if let Some(plan) = g.plans.get_mut(&height) {
                plan.need_fk.insert((id, vout));
            }
            g.recompute_ready_through();
            self.ready_through
                .store(g.ready_through, Ordering::Relaxed);
            return;
        }
        if let Some(plan) = g.plans.get_mut(&height) {
            plan.reserved.insert((prev_txid, vout));
        }
        g.reserve_waiters
            .entry((prev_txid, vout))
            .or_default()
            .insert(height);
    }

    /// Register creates from a cache body with **all** outputs.
    ///
    /// Phase-1 cache loads full blocks first so later spends in the same
    /// batch resolve creates without UTXO or reservations.
    /// Prefer [`Self::put_body_and_creates`] on the hot path (one lock).
    pub fn register_cache_creates(
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
        {
            let e = g.by_fk.entry(id).or_insert_with(|| ParentEntry {
                tx: tx.clone(),
                outs: HashMap::new(),
                checked: HashSet::new(),
                coinbase_height: None,
                create_height: Some(create_height),
                keep_until: create_height,
            });
            e.tx = tx.clone();
            e.create_height = Some(create_height);
            e.keep_until = e.keep_until.max(create_height);
            for (v, o) in outputs.iter().enumerate() {
                let v = v as u32;
                e.outs.insert(
                    v,
                    ParentOut {
                        output: o.clone(),
                    },
                );
                e.checked.insert(v);
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
    /// Prefers sparse external-parent `by_fk`, then cache **body** outs
    /// (bodies-first cache no longer dual-copies every create into `by_fk`).
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

    /// True when parent pin can skip store decode for `vouts` (wave already
    /// served by `get_parent_outs_needed` / body path).
    ///
    /// Covered if sparse `by_fk` has a full checked set (or all live outs), or
    /// a cache `by_body` holds every requested index.
    pub fn parent_pin_covered(&self, fk: Fk, vouts: &[u32]) -> bool {
        let Some(id) = fk.get() else {
            return false;
        };
        let g = self.inner.lock().unwrap();
        Self::pin_covered_locked(&g, id, vouts)
    }

    /// One-lock cover check for many parents (`(create_fk_id, need_vouts)`).
    pub fn parent_pins_covered(&self, items: &[(u64, Vec<u32>)]) -> Vec<bool> {
        if items.is_empty() {
            return Vec::new();
        }
        let g = self.inner.lock().unwrap();
        items
            .iter()
            .map(|(id, vouts)| Self::pin_covered_locked(&g, *id, vouts))
            .collect()
    }

    /// Cover check with miss classification for diagnostics.
    ///
    /// Returns `(covered[], miss_no_fk, miss_partial)` where misses are counts
    /// among **uncovered** items only.
    pub fn parent_pins_covered_detail(
        &self,
        items: &[(u64, Vec<u32>)],
    ) -> (Vec<bool>, u32, u32) {
        if items.is_empty() {
            return (Vec::new(), 0, 0);
        }
        let g = self.inner.lock().unwrap();
        let mut miss_no_fk = 0u32;
        let mut miss_partial = 0u32;
        let covered: Vec<bool> = items
            .iter()
            .map(|(id, vouts)| {
                if vouts.is_empty() {
                    return true;
                }
                match g.by_fk.get(id) {
                    None => {
                        miss_no_fk = miss_no_fk.saturating_add(1);
                        false
                    }
                    Some(e) => {
                        if !e.checked.is_empty() && vouts.iter().all(|v| e.checked.contains(v)) {
                            true
                        } else {
                            miss_partial = miss_partial.saturating_add(1);
                            false
                        }
                    }
                }
            })
            .collect();
        (covered, miss_no_fk, miss_partial)
    }

    /// Vouts already spent-filtered on `by_fk` (subset of `need` that can skip store).
    pub fn parent_checked_vouts(&self, fk: Fk, need: &[u32]) -> Vec<u32> {
        let Some(id) = fk.get() else {
            return Vec::new();
        };
        let g = self.inner.lock().unwrap();
        let Some(e) = g.by_fk.get(&id) else {
            return Vec::new();
        };
        need.iter()
            .copied()
            .filter(|v| e.checked.contains(v))
            .collect()
    }

    /// Keep-alive already-stashed parents for sliding-window re-pin skip:
    /// attach `need_fk` at `height` so tip GC does not drop them before confirm.
    pub fn touch_parent_needs(&self, height: u32, fk: Fk, vouts: &[u32]) {
        let Some(id) = fk.get() else {
            return;
        };
        if vouts.is_empty() {
            return;
        }
        let mut g = self.inner.lock().unwrap();
        g.touch_parent_needs_inner(height, id, vouts);
    }

    /// Batch keep-alive: `(height, create_fk_id, vouts)`.
    pub fn touch_parent_needs_batch(&self, items: &[(u32, u64, Vec<u32>)]) {
        if items.is_empty() {
            return;
        }
        let mut g = self.inner.lock().unwrap();
        for (height, id, vouts) in items {
            g.touch_parent_needs_inner(*height, *id, vouts);
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

    /// See [`Self::parent_pin_covered`].
    ///
    /// Only **spent-filtered** `by_fk` entries count. Bare cache `by_body` does
    /// **not** cover external parents (wave cache_only requires spent_filtered).
    #[inline]
    fn pin_covered_locked(g: &Inner, id: u64, vouts: &[u32]) -> bool {
        if vouts.is_empty() {
            return true;
        }
        if let Some(e) = g.by_fk.get(&id) {
            if !e.checked.is_empty() && vouts.iter().all(|v| e.checked.contains(v)) {
                return true;
            }
        }
        false
    }

    pub fn get_parent_tx(&self, fk: Fk) -> Option<TxRecord> {
        let id = fk.get()?;
        let g = self.inner.lock().unwrap();
        if let Some(e) = g.by_fk.get(&id) {
            return Some(e.tx.clone());
        }
        g.by_body.get(&id).map(|b| b.tx.clone())
    }

    /// Txid of a stashed parent create (by_fk sparse or cache body) — no clone of outs.
    pub fn get_parent_txid(&self, fk: Fk) -> Option<[u8; 32]> {
        let id = fk.get()?;
        let g = self.inner.lock().unwrap();
        if let Some(e) = g.by_fk.get(&id) {
            return Some(e.tx.txid);
        }
        g.by_body.get(&id).map(|b| b.tx.txid)
    }

    /// Sparse external-parent outs only (`by_fk`). Does **not** expand cache
    /// bodies — callers that need a subset of body outs should use
    /// [`Self::get_parent_outs_needed`] (avoids cloning every script of a multi-out create).
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

    /// Clone only the requested parent vouts (sparse `by_fk`, else cache body).
    ///
    /// Returns `(tx, live_outs, spent_filtered)`:
    /// - `spent_filtered == true`: cache already dropped spent vouts; wave
    ///   must not re-check spentness for these candidates.
    /// - `spent_filtered == false`: candidates need a spent filter (body path).
    ///
    /// Wave_fill path: never materializes a full dense outs list for multi-out
    /// creates when only 1–2 prevouts are needed.
    pub fn get_parent_outs_needed(
        &self,
        fk: Fk,
        vouts: &[u32],
    ) -> Option<(TxRecord, Vec<(u32, OutputRecord)>, bool)> {
        let id = fk.get()?;
        let g = self.inner.lock().unwrap();
        if let Some(e) = g.by_fk.get(&id) {
            // Fully resolved by cache (all requested vouts checked).
            if !e.checked.is_empty() && vouts.iter().all(|v| e.checked.contains(v)) {
                let mut live = Vec::with_capacity(vouts.len());
                for &v in vouts {
                    if let Some(o) = e.outs.get(&v) {
                        live.push((v, o.output.clone()));
                    }
                }
                return Some((e.tx.clone(), live, true));
            }
            // Legacy / partial: all requested present as live outs (no checked).
            if !e.outs.is_empty() && vouts.iter().all(|v| e.outs.contains_key(v)) {
                let mut live = Vec::with_capacity(vouts.len());
                for &v in vouts {
                    if let Some(o) = e.outs.get(&v) {
                        live.push((v, o.output.clone()));
                    }
                }
                return Some((e.tx.clone(), live, false));
            }
        }
        if let Some(b) = g.by_body.get(&id) {
            let mut live = Vec::with_capacity(vouts.len());
            for &v in vouts {
                if let Some(o) = b.outputs.get(v as usize) {
                    live.push((v, o.clone()));
                }
            }
            // Body path still needs spent filter (wave-body creates are live
            // only after wave own spent filter; external body rare).
            return Some((b.tx.clone(), live, false));
        }
        None
    }

    pub fn plan_count(&self) -> usize {
        self.inner.lock().unwrap().plans.len()
    }

    /// Sparse external parents in `by_fk` (not every cache create).
    pub fn parent_count(&self) -> usize {
        self.inner.lock().unwrap().by_fk.len()
    }

    /// Cached body-range entries (idx offsets for mlock/wave).
    pub fn body_range_count(&self) -> usize {
        self.inner.lock().unwrap().body_range.len()
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
    ///
    /// Recomputes [`Self::ready_through`]: removing the last live out of a parent
    /// must not leave a hollow watermark above a now-incomplete package.
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
        g.recompute_ready_through();
        self.ready_through
            .store(g.ready_through, Ordering::Relaxed);
        drop(g);
        self.ready_cv.notify_all();
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
                    checked: HashSet::new(),
                    coinbase_height: None,
                    create_height: Some(height),
                    keep_until: height,
                });
                e.tx = tx.clone();
                e.create_height = Some(height);
                e.keep_until = e.keep_until.max(height);
                e.outs.insert(
                    v,
                    ParentOut {
                        output: o.clone(),
                    },
                );
                e.checked.insert(v);
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
        self.put_parent_outs_resolved_inner(
            height,
            id,
            tx,
            &[(vout, output)],
            &[vout],
            None,
            None,
        );
    }

    /// Attach plan keep-alive for an already-stashed parent (no re-decode).
    fn touch_parent_needs_inner(&mut self, height: u32, id: u64, vouts: &[u32]) {
        // Collect under by_fk/by_body first — cannot hold plan mut borrow too.
        let grace = self.pin_keep_grace;
        let pins: Vec<u32> = if let Some(e) = self.by_fk.get_mut(&id) {
            e.keep_until = e.keep_until.max(pin_keep_until_for(height, grace));
            let mut vs: Vec<u32> = vouts
                .iter()
                .copied()
                .filter(|v| e.outs.contains_key(v) || e.checked.contains(v))
                .collect();
            if vs.is_empty() {
                if let Some(&v) = e.outs.keys().next().or_else(|| e.checked.iter().next()) {
                    vs.push(v);
                }
            }
            vs
        } else if self.by_body.contains_key(&id) {
            vouts.to_vec()
        } else {
            return;
        };
        if let Some(plan) = self.plans.get_mut(&height) {
            for v in pins {
                plan.need_fk.insert((id, v));
            }
        }
    }

    fn put_parent_outs_resolved_inner(
        &mut self,
        height: u32,
        id: u64,
        tx: TxRecord,
        live: &[(u32, OutputRecord)],
        checked: &[u32],
        coinbase_height: Option<Option<u32>>,
        create_height: Option<u32>,
    ) {
        let txid = tx.txid;
        let grace = self.pin_keep_grace;
        let until = pin_keep_until_for(height, grace);
        {
            let e = self.by_fk.entry(id).or_insert_with(|| ParentEntry {
                tx: tx.clone(),
                outs: HashMap::new(),
                checked: HashSet::new(),
                coinbase_height: None,
                create_height: None,
                keep_until: until,
            });
            e.tx = tx;
            e.keep_until = e.keep_until.max(until);
            if coinbase_height.is_some() {
                e.coinbase_height = coinbase_height;
            }
            if create_height.is_some() {
                e.create_height = create_height;
            }
            for &v in checked {
                e.checked.insert(v);
            }
            for (v, output) in live {
                e.outs.insert(*v, ParentOut {
                    output: output.clone(),
                });
                e.checked.insert(*v);
            }
        }
        // Plan bookkeeping for live outs (GC keep-alive via need_fk).
        for (v, _) in live {
            if let Some(plan) = self.plans.get_mut(&height) {
                plan.need_fk.insert((id, *v));
                plan.reserved.remove(&(txid, *v));
            }
            let key = (txid, *v);
            if let Some(waiters) = self.reserve_waiters.remove(&key) {
                for h in waiters {
                    if let Some(plan) = self.plans.get_mut(&h) {
                        plan.reserved.remove(&key);
                        plan.need_fk.insert((id, *v));
                    }
                }
            }
        }
        // Spent-only checked vouts still pin the parent entry for the height.
        if live.is_empty() && !checked.is_empty() {
            if let Some(plan) = self.plans.get_mut(&height) {
                if let Some(&v) = checked.first() {
                    plan.need_fk.insert((id, v));
                }
            }
        }
    }

    fn retire_spend_id(&mut self, id: u64, vout: u32) {
        if let Some(e) = self.by_fk.get_mut(&id) {
            e.outs.remove(&vout);
            // Keep the by_fk row when only live outs are gone: `checked` still
            // covers spent-filtered package_ready for other cache heights.
            // Dropping the whole entry here left tip+N incomplete while
            // ready_through stayed high until the next tip GC (cache cursor
            // jumped past the hole). Full drop is tip GC / gc_orphaned_parents.
            if e.outs.is_empty() && e.checked.is_empty() {
                self.by_fk.remove(&id);
            }
        }
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
    /// scanned-only [`HeightPlan::is_ready`] so the pipeline does not do more
    /// blocking work than the 2-stage path.
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
        let wave_ids: HashSet<u64> = hdr
            .tx_fks
            .iter()
            .filter_map(|f| f.get())
            .collect();
        // Collect external (create_fk, vout) needed from non-wave parents.
        let mut external: HashMap<u64, HashSet<u32>> = HashMap::new();
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
                let Some(pid) = pid else {
                    return false; // unstamped create_fk
                };
                let vout = thin
                    .and_then(|t| t.get(i))
                    .map(|e| e.prev_index)
                    .unwrap_or(inp.prev_index);
                if wave_ids.contains(&pid) {
                    continue; // same-block create; wave resolves
                }
                external.entry(pid).or_default().insert(vout);
            }
        }
        for (pid, vouts) in &external {
            let Some(e) = self.by_fk.get(pid) else {
                return false;
            };
            // Must be spent-filtered for every needed vout.
            if e.checked.is_empty() || !vouts.iter().all(|v| e.checked.contains(v)) {
                return false;
            }
        }
        true
    }

    /// Drop body_range entries not referenced by live cache bodies or parent pins.
    ///
    /// Parent pin used to insert ranges forever (never tied to a height plan),
    /// so body_range grew O(unique parents ever seen) across IBD.
    fn gc_body_ranges(&mut self) {
        if self.body_range.is_empty() {
            return;
        }
        self.body_range.retain(|id, _| {
            self.by_body.contains_key(id) || self.by_fk.contains_key(id)
        });
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
            // Load pipeline lag: keep until last needing height confirms.
            if e.keep_until > tip {
                continue;
            }
            // Keep by_fk until tip passes create height (runway creates).
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
            self.by_fk.remove(&id);
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
    fn utxo_parent_marks_ready() {
        let c = ConfirmParentCache::new();
        c.advance_tip(10);
        let hash = [9u8; 32];
        // Ready = full package (coinbase body), not mark_scanned alone.
        seed_coinbase_package(&c, 11, hash, 1001);
        // External parent pin is orthogonal to package for coinbase-only height.
        let t = tx(1);
        c.put_utxo_parent(11, Fk(7), t, 0, out(100));
        assert!(c.is_ready(11));
        assert_eq!(c.ready_through(), 11);
        let (tx, o) = c.get_parent_out(Fk(7), 0).unwrap();
        assert_eq!(tx.txid[0], 1);
        assert_eq!(o.value, 100);
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

    /// Retiring the last live out must not delete spent-filtered `checked` coverage
    /// or leave ready_through above a now-incomplete height (cache cursor skip).
    #[test]
    fn retire_last_live_out_keeps_checked_and_recomputes_watermark() {
        let c = ConfirmParentCache::new();
        c.advance_tip(10);
        seed_coinbase_package(&c, 11, [0x11; 32], 1100);
        // Height 12 spends parent 42:0 — pin with checked+live.
        let h12 = [0x12u8; 32];
        c.ensure_plan(12, h12);
        let mut t12 = tx(12);
        t12.txid = h12;
        t12.input_count = 1;
        t12.output_count = 1;
        let spend_in = vec![InputRecord {
            prev_txid: [0x42; 32],
            create_fk: Fk(42),
            prev_index: 0,
            sequence: 0xffff_ffff,
            script_sig: vec![],
            witness: vec![],
        }];
        c.put_header_plan(12, Fk(12), header_rec(h12), vec![Fk(1200)], [0x11; 32]);
        c.put_body(Fk(1200), 12, t12, vec![out(49)], spend_in);
        c.put_thin_inputs(
            Fk(1200),
            vec![crate::wave_prevout::ThinInput {
                create_fk: Some(42),
                prev_index: 0,
            }],
        );
        let parent_tx = tx(42);
        c.put_parent_outs_resolved(
            12,
            Fk(42),
            parent_tx.clone(),
            &[(0, out(100))],
            &[0],
            Some(None),
        );
        c.mark_scanned(12);
        assert!(c.package_ready(12));
        assert_eq!(c.ready_through(), 12);

        // Confirm spent 42:0 — old code dropped the whole by_fk row when outs empty.
        c.retire_spends(&[(Fk(42), 0)]);
        // Spent-filtered identity remains (checked kept); package still content-ready
        // for re-queue / headroom (vout is checked, not necessarily live).
        assert!(
            c.parent_pin_covered(Fk(42), &[0]),
            "checked coverage must survive last-live retire"
        );
        // If a later height needed a different vout that was never pinned, watermark
        // recompute still ran (no panic / stale atomic).
        assert_eq!(c.ready_through(), 12);
    }

    /// Regression: same-bite create@11 + spend@12 must pin create into spent-filtered
    /// `by_fk`. Bare `by_body` is not enough for package_ready / cache-only wave —
    /// skipping pin left ready_through at tip+1/+2 after multi-block cache bites.
    #[test]
    fn cross_height_spend_needs_parent_pin_for_package_ready() {
        let c = ConfirmParentCache::new();
        c.advance_tip(10);
        let h11 = [0x11u8; 32];
        let h12 = [0x12u8; 32];
        // Create body at 11 (not coinbase-only seed — height 12 will spend it).
        seed_coinbase_package(&c, 11, h11, 1100);
        assert!(c.package_ready(11));
        assert_eq!(c.ready_through(), 11);

        // Height 12 spends create 1100:0 — external to wave_ids of 12.
        c.ensure_plan(12, h12);
        let mut t12 = tx(12);
        t12.txid = h12;
        t12.input_count = 1;
        t12.output_count = 1;
        let spend_in = vec![InputRecord {
            prev_txid: h11,
            create_fk: Fk(1100),
            prev_index: 0,
            sequence: 0xffff_ffff,
            script_sig: vec![],
            witness: vec![],
        }];
        c.put_header_plan(
            12,
            Fk(12),
            header_rec(h12),
            vec![Fk(1200)],
            h11,
        );
        c.put_body(Fk(1200), 12, t12, vec![out(49)], spend_in);
        c.put_thin_inputs(
            Fk(1200),
            vec![crate::wave_prevout::ThinInput {
                create_fk: Some(1100),
                prev_index: 0,
            }],
        );
        c.mark_scanned(12);
        // Wait unblocks on scanned (2-stage); content package still incomplete.
        assert!(c.is_ready(12));
        assert_eq!(c.ready_through(), 12);
        assert!(
            !c.package_ready(12),
            "spend of same-bite create without by_fk pin must not be package_ready"
        );

        // Pin from cache body (what cache does for batch_create_ids).
        let (create_h, tx, outs, _ins) = c.get_body_for_pin(Fk(1100)).expect("cache body");
        assert_eq!(create_h, 11);
        c.put_parent_outs_resolved(
            12,
            Fk(1100),
            tx,
            &[(0, outs[0].clone())],
            &[0],
            Some(Some(11)),
        );
        assert!(c.package_ready(12));
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
    fn reserve_then_register_create_fills() {
        let c = ConfirmParentCache::new();
        c.advance_tip(0);
        let hash = [2u8; 32];
        // Coinbase package is ready even with open reserves on the plan.
        seed_coinbase_package(&c, 2, hash, 2002);
        let t = tx(5);
        c.reserve(2, t.txid, 0);
        assert!(c.is_ready(2));
        assert!(c.has_open_reserves(2));
        // Create appears from height 1 body — fills cache for wave/connect.
        c.register_cache_creates(Fk(50), &t, &[out(42), out(43)], 1);
        assert!(c.is_ready(2));
        assert!(!c.has_open_reserves(2));
        assert_eq!(c.get_parent_out(Fk(50), 0).unwrap().1.value, 42);
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
        c.register_cache_creates(Fk(90), &t, &[out(1)], 1);
        assert!(!c.has_open_reserves(2));
        assert_eq!(c.ready_through(), 2);
    }

    #[test]
    fn phase1_register_keeps_all_outs_for_later_spend() {
        // Bodies first: create height registers all outs; spend height hits cache.
        let c = ConfirmParentCache::new();
        c.advance_tip(0);
        seed_coinbase_package(&c, 1, [1u8; 32], 1001);
        let t = tx(5);
        c.register_cache_creates(Fk(50), &t, &[out(42), out(43)], 1);
        assert!(c.is_ready(1));
        assert!(c.has_parent_out(Fk(50), 0));
        assert_eq!(c.get_parent_out(Fk(50), 1).unwrap().1.value, 43);

        seed_coinbase_package(&c, 2, [2u8; 32], 1002);
        c.put_utxo_parent(2, Fk(50), t.clone(), 1, out(43));
        assert!(c.is_ready(2));
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
    fn body_range_gc_drops_orphaned_parent_ranges() {
        let c = ConfirmParentCache::new();
        c.set_pin_keep_grace(0); // exclusive need+1 only for this GC test
        c.advance_tip(0);
        c.ensure_plan(1, [1u8; 32]);
        // Parent pin range only (no by_body / by_fk keep-alive after tip).
        c.put_body_range(Fk(999), 1000, 50);
        assert_eq!(c.body_range_count(), 1);
        // Live parent keeps range.
        c.put_parent_outs_resolved(
            1,
            Fk(999),
            tx(9),
            &[(0, out(1))],
            &[0],
            Some(None),
        );
        assert_eq!(c.body_range_count(), 1);
        c.mark_scanned(1);
        // tip == need still retains (keep_until = need+1); tip past exclusive end drops.
        c.advance_tip(1);
        assert_eq!(c.body_range_count(), 1, "grace=0 still holds at tip == need");
        c.advance_tip(2);
        assert_eq!(
            c.body_range_count(),
            0,
            "orphaned parent body_range must not leak across tip"
        );
    }

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
    fn body_create_resolves_without_by_fk_dual_copy() {
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
        // Sparse external path still works alongside body.
        c.put_utxo_parent(2, Fk(99), tx(9), 0, out(5));
        assert_eq!(c.get_parent_out(Fk(99), 0).unwrap().1.value, 5);
        // parent_count is sparse externals only (not every body create).
        assert_eq!(c.parent_count(), 1);
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
    fn parent_outs_resolved_skips_spent_recheck() {
        // Runway stashes live outs + checked set; wave must see spent_filtered.
        let c = ConfirmParentCache::new();
        c.advance_tip(0);
        c.ensure_plan(1, [1u8; 32]);
        let t = tx(9);
        // vout 0 live, vout 1 spent (checked but not live).
        c.put_parent_outs_resolved(
            1,
            Fk(90),
            t.clone(),
            &[(0, out(100))],
            &[0, 1],
            Some(None), // not a coinbase
        );
        let (txr, live, filtered) = c.get_parent_outs_needed(Fk(90), &[0, 1]).unwrap();
        assert!(filtered);
        assert_eq!(txr.txid, t.txid);
        assert_eq!(live.len(), 1);
        assert_eq!(live[0].0, 0);
        assert_eq!(live[0].1.value, 100);
        assert_eq!(c.get_parent_coinbase_height(Fk(90)), Some(None));
        // Partial request still complete.
        let (_, live0, f0) = c.get_parent_outs_needed(Fk(90), &[0]).unwrap();
        assert!(f0);
        assert_eq!(live0.len(), 1);
        // Unknown vout 2 → not complete → None (wave falls back to store).
        assert!(c.get_parent_outs_needed(Fk(90), &[0, 2]).is_none());
    }

    #[test]
    fn parent_pin_covered_and_touch_skip_redecode() {
        // Sliding-window re-pin: already-stashed outs → covered; touch keep-alive.
        let c = ConfirmParentCache::new();
        c.advance_tip(0);
        c.ensure_plan(1, [1u8; 32]);
        c.ensure_plan(2, [2u8; 32]);
        let t = tx(42);
        c.put_parent_outs_resolved(
            1,
            Fk(42),
            t,
            &[(0, out(50)), (1, out(60))],
            &[0, 1],
            Some(None),
        );
        assert!(c.parent_pin_covered(Fk(42), &[0]));
        assert!(c.parent_pin_covered(Fk(42), &[0, 1]));
        assert!(!c.parent_pin_covered(Fk(42), &[0, 2])); // missing checked vout
        assert!(!c.parent_pin_covered(Fk(99), &[0])); // unknown parent

        let batch = c.parent_pins_covered(&[(42, vec![0, 1]), (99, vec![0])]);
        assert_eq!(batch, vec![true, false]);

        // Touch for a later height (sliding window) without re-put.
        c.touch_parent_needs(2, Fk(42), &[0, 1]);
        // Wave still serves spent-filtered outs after touch.
        let (_, live, filtered) = c.get_parent_outs_needed(Fk(42), &[0, 1]).unwrap();
        assert!(filtered);
        assert_eq!(live.len(), 2);

        // Bare by_body does **not** count as pin-covered (need spent-filtered by_fk).
        c.put_body(Fk(70), 1, tx(70), vec![out(1), out(2)], vec![]);
        assert!(
            !c.parent_pin_covered(Fk(70), &[0, 1]),
            "cache body alone must not skip external parent pin"
        );
    }

    #[test]
    fn parent_coinbase_height_stashed_for_wave() {
        let c = ConfirmParentCache::new();
        c.advance_tip(0);
        c.ensure_plan(1, [1u8; 32]);
        let t = tx(11);
        c.put_parent_outs_resolved(
            1,
            Fk(11),
            t,
            &[(0, out(50))],
            &[0],
            Some(Some(100)), // coinbase at height 100
        );
        assert_eq!(c.get_parent_coinbase_height(Fk(11)), Some(Some(100)));
        // Unknown fk.
        assert!(c.get_parent_coinbase_height(Fk(99)).is_none());
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
        std::env::set_var("RBITCOIN_CONFIRM_BODY_LRU_MB", "0");
        // Cap 0: legacy drop-at-tip.
        let c = ConfirmParentCache::new();
        std::env::remove_var("RBITCOIN_CONFIRM_BODY_LRU_MB");
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
        c.put_utxo_parent(1, Fk(1), tx(1), 0, out(1));
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
        std::env::set_var("RBITCOIN_CONFIRM_BODY_LRU_MB", "1"); // 1 MiB
        let c = ConfirmParentCache::from_env();
        std::env::remove_var("RBITCOIN_CONFIRM_BODY_LRU_MB");
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

    /// keep_until must retain by_fk after tip advances past create_height until
    /// the exclusive retention end (need+1 with grace=0).
    #[test]
    fn pin_keep_until_survives_tip_gc_until_spender_confirms() {
        let c = ConfirmParentCache::new();
        c.set_pin_keep_grace(0); // exclusive need+1 only
        c.advance_tip(10);
        // Parent create at height 5 (already confirmed), pinned for spender at 100.
        let t = tx(9);
        c.put_parent_outs_resolved(
            100,
            Fk(99),
            t.clone(),
            &[(0, out(1))],
            &[0],
            Some(None),
        );
        // keep_until = 101 (need+1); create_height None.
        assert!(c.parent_pin_covered(Fk(99), &[0]));
        c.advance_tip(50); // past create, but keep_until still ahead
        assert!(
            c.parent_pin_covered(Fk(99), &[0]),
            "by_fk must survive tip GC while keep_until > tip"
        );
        // tip == need still retains (exclusive end = need+1).
        c.advance_tip(100);
        assert!(
            c.parent_pin_covered(Fk(99), &[0]),
            "retain at tip == need with grace=0"
        );
        // Touch later batch needing height 120 → keep_until = 121.
        c.touch_parent_needs(120, Fk(99), &[0]);
        c.advance_tip(120);
        assert!(
            c.parent_pin_covered(Fk(99), &[0]),
            "retain at tip == need after touch"
        );
        c.advance_tip(121);
        assert!(
            !c.parent_pin_covered(Fk(99), &[0]),
            "drop when tip >= keep_until"
        );
    }

    /// Grace keeps by_fk across tip == need so empty conf_q still gets pin_cached.
    #[test]
    fn pin_keep_grace_survives_cross_batch_without_load_depth() {
        let c = ConfirmParentCache::new();
        c.set_pin_keep_grace(32);
        c.advance_tip(10);
        let t = tx(3);
        c.put_parent_outs_resolved(
            100,
            Fk(55),
            t,
            &[(0, out(1))],
            &[0],
            Some(None),
        );
        // keep_until = 100 + 1 + 32 = 133
        c.advance_tip(100);
        assert!(
            c.parent_pin_covered(Fk(55), &[0]),
            "grace retains after tip reaches last known need"
        );
        c.advance_tip(132);
        assert!(c.parent_pin_covered(Fk(55), &[0]));
        c.advance_tip(133);
        assert!(
            !c.parent_pin_covered(Fk(55), &[0]),
            "drop after tip passes need+1+grace"
        );
    }

    #[test]
    fn pin_keep_until_for_math() {
        assert_eq!(pin_keep_until_for(100, 0), 101);
        assert_eq!(pin_keep_until_for(100, 256), 357);
        assert_eq!(pin_keep_until_for(u32::MAX, 10), u32::MAX);
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
    fn get_bodies_for_pin_batch_slims_outs_and_covers_after_put() {
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

        // After spent-filtered put, same vouts are pin_covered (cheap path).
        c.put_parent_outs_resolved(
            6,
            Fk(77),
            txr.clone(),
            &[(0, outs[0].1.clone())],
            &[0, 2],
            Some(Some(5)),
        );
        // create_height via batch put path
        c.put_parent_outs_resolved_batch(&[(
            6,
            Fk(77),
            txr.clone(),
            vec![(0, outs[0].1.clone())],
            vec![0, 2],
            Some(Some(5)),
            Some(5),
        )]);
        assert!(c.parent_pin_covered(Fk(77), &[0, 2]));
        assert!(!c.parent_pin_covered(Fk(77), &[0, 1])); // vout 1 never checked
    }
}
