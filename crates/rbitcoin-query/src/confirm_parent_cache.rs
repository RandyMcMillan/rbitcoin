//! Block-structured **confirm parent runway** (replaces generic Class A cache).
//!
//! Prewarm strategy:
//! - **RAM-cache** small lookups: header head/body, `header_txs`, `tx.head`→fk,
//!   `tx.idx` body ranges.
//! - **`mlock`** large / write-path pages only: `tx.body`, `strong_tx`, `tx_height`,
//!   `confirmed[h]`. Never mlock `spenders` (no multi-spend writes in IBD).
//! - **Full-decode** runway Class A bodies into `by_body` once; wave_fill / wire
//!   rebuild consume that cache (no second packed parse on confirm).
//!
//! - **Runway creates** register `txid → fk` so same-batch spends skip head probes.
//! - **Thin input edges** stashed per spend tx after a lightweight prevout walk.
//! - A height is **ready** once scanned (cache filled + body mlocked).

use rbitcoin_primitives::Fk;
use rbitcoin_store::{HeaderRecord, InputRecord, MlockRange, OutputRecord, TxRecord};
// ThinInput used via StashedThinInput alias.
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Condvar, Mutex};
use std::time::{Duration, Instant};

/// Default runway depth (blocks ahead of tip). Override with env.
pub const DEFAULT_PREWARM_DEPTH: u32 = 256;
pub const MIN_PREWARM_DEPTH: u32 = 32;
pub const MAX_PREWARM_DEPTH: u32 = 4096;
/// Max entries in confirmed-create sticky map (~40 B/entry; 2M ≈ 80 MiB).
pub const DEFAULT_CONFIRMED_TXID_STICKY_CAP: usize = 2_000_000;

/// Capacity for process-local confirmed txid→fk sticky (prewarm thin).
pub fn confirmed_txid_sticky_cap_from_env() -> usize {
    std::env::var("RBITCOIN_CONFIRMED_TXID_STICKY_CAP")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(DEFAULT_CONFIRMED_TXID_STICKY_CAP)
        .clamp(10_000, 20_000_000)
}
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

/// Default: mlock **on** (body + Class C pages for runway / parents).
/// Set `=0`/`false`/`off` for decode-stash only (no mlock syscalls).
pub fn prewarm_mlock_from_env() -> bool {
    match std::env::var("RBITCOIN_PARENT_PREWARM_MLOCK") {
        Ok(s) => {
            let t = s.trim();
            !(t == "0" || t.eq_ignore_ascii_case("false") || t.eq_ignore_ascii_case("off"))
        }
        // Default on.
        Err(_) => true,
    }
}

/// Only pin external parents needed by spends in tip+1‥tip+K.
///
/// **0 = full runway** (default): pin every external parent in the prewarm window.
/// Non-zero K limits pin to heights ≤ tip+K (tip-near pin; lower RAM).
pub const DEFAULT_PREWARM_PIN_NEAR: u32 = 0;

pub fn prewarm_pin_near_from_env() -> u32 {
    std::env::var("RBITCOIN_PARENT_PREWARM_PIN_NEAR")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(DEFAULT_PREWARM_PIN_NEAR)
        .min(MAX_PREWARM_DEPTH)
}

/// Thin edge walk: only stamped create_fk + coinbase (skip soft prev_txid/head).
///
/// Default **on** — v10 IBD stamps create_fk; soft/head path is legacy.
/// Set `=0`/`false` to restore soft prev_txid + sticky/head resolve.
pub fn prewarm_thin_create_fk_only_from_env() -> bool {
    match std::env::var("RBITCOIN_PARENT_PREWARM_THIN_CREATE_FK_ONLY") {
        Ok(s) => {
            let t = s.trim();
            !(t == "0" || t.eq_ignore_ascii_case("false") || t.eq_ignore_ascii_case("off"))
        }
        Err(_) => true,
    }
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
    /// Live (unspent) needed vouts → output. Spent vouts are omitted.
    pub outs: HashMap<u32, ParentOut>,
    /// Vouts that prewarm fully resolved (spent-filtered). When all requested
    /// vouts are in this set, wave can skip store decode + spent re-check.
    pub checked: HashSet<u32>,
    /// Coinbase maturity height resolved at prewarm (wave skips body re-walk).
    ///
    /// - `None` = not resolved yet
    /// - `Some(None)` = not a coinbase
    /// - `Some(Some(h))` = coinbase created at height `h`
    pub coinbase_height: Option<Option<u32>>,
    /// Height of the runway body that registered this create (`None` = UTXO load).
    pub create_height: Option<u32>,
}

/// Thin create-fk edge (identical to wave [`crate::wave_prevout::ThinInput`]).
pub type StashedThinInput = crate::wave_prevout::ThinInput;

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

/// Cached header + body fk list for one runway height (avoids header.head/body
/// and header_txs page faults on confirm resolve).
#[derive(Debug, Clone)]
pub struct HeaderPlanCache {
    pub header_fk: Fk,
    pub header_rec: HeaderRecord,
    pub tx_fks: Vec<Fk>,
    /// Previous block hash (zeros at genesis). Filled at prewarm so wire rebuild
    /// never `store.get_header(prev_fk)`.
    pub prev_hash: [u8; 32],
}

/// Per-height plan: what prevouts block `height` needs.
#[derive(Debug, Default)]
struct HeightPlan {
    hash: [u8; 32],
    /// Prewarm finished a full package attempt for this height (see package_ready).
    scanned: bool,
    /// (create_fk, vout) fully populated in cache.
    need_fk: HashSet<(u64, u32)>,
    /// (prev_txid, vout) not in UTXO at prewarm — expect runway / same-wave create.
    reserved: HashSet<([u8; 32], u32)>,
}

/// One mlocked page range (any store table) held for runway heights.
struct RangeRec {
    range: MlockRange,
    /// Heights that still need this range warm.
    need_heights: HashSet<u32>,
    start_page: u64,
    end_page: u64, // exclusive page index within this table
}

/// Runway-scoped txid → create fk (with height for tip GC).
#[derive(Clone, Copy, Debug)]
struct TxidEntry {
    fk: u64,
    /// Create height or last spend-height that needed this parent.
    height: u32,
}

/// Lightweight sticky: confirmed (or known) create identity only — no outs.
#[derive(Clone, Copy, Debug)]
struct StickyConfirmed {
    fk: u64,
    /// Create height when registered from runway / promote height.
    height: u32,
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
    /// create txid → (fk, runway height). Height is create height or the spend
    /// height that needed this parent. GC keeps `(tip−depth, tip+depth]` so
    /// recent external parents re-hit without `tx.head` (prewarm thin hot path).
    /// Also kept while held by `by_body` / `by_fk`.
    by_txid: HashMap<[u8; 32], TxidEntry>,
    /// Confirmed-create sticky: txid → fk (capacity-capped FIFO).
    ///
    /// Filled when we decode runway creates (and on head resolve). Survives tip
    /// GC of `by_txid` so later spends skip durable head. No scripts/outs.
    sticky_confirmed: HashMap<[u8; 32], StickyConfirmed>,
    /// Insert order for FIFO eviction when over [`Self::sticky_cap`].
    sticky_fifo: VecDeque<[u8; 32]>,
    sticky_cap: usize,
    /// height → header + tx list (replaces header.head/body + header_txs reads).
    headers: HashMap<u32, HeaderPlanCache>,
    /// hash → height for O(1) header resolve on confirm.
    hash_to_height: HashMap<[u8; 32], u32>,
    /// fk id → absolute body (offset, len) from tx.idx (replaces idx page faults).
    body_range: HashMap<u64, (u64, u64)>,
    /// Reserved (txid, vout) → set of heights waiting (legacy; unused by new prewarm).
    reserve_waiters: HashMap<([u8; 32], u32), HashSet<u32>>,
    /// (table, page_start) → mlocked range + need_heights.
    mlocked: HashMap<(u8, u64), RangeRec>,
    /// (table, page_index) → refcount (shared pages).
    page_refs: HashMap<(u8, u64), u32>,
    /// How many distinct ranges currently held (perf).
    mlock_n: usize,
}

/// Process-local confirm parent runway.
pub struct ConfirmParentCache {
    inner: Mutex<Inner>,
    /// Signaled when plans become ready (`mark_scanned*`) or tip GC advances
    /// readiness — confirm waits here instead of spinning / last-mile load.
    ready_cv: Condvar,
    depth: AtomicU32,
    /// Mirror of `Inner::ready_through` for lock-free reads.
    ready_through: AtomicU32,
}

impl ConfirmParentCache {
    pub fn new(depth: u32) -> Self {
        let depth = depth.clamp(MIN_PREWARM_DEPTH, MAX_PREWARM_DEPTH);
        let sticky_cap = confirmed_txid_sticky_cap_from_env();
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
                sticky_confirmed: HashMap::with_capacity(sticky_cap.min(1 << 20)),
                sticky_fifo: VecDeque::new(),
                sticky_cap,
                headers: HashMap::new(),
                hash_to_height: HashMap::new(),
                body_range: HashMap::new(),
                reserve_waiters: HashMap::new(),
                mlocked: HashMap::new(),
                page_refs: HashMap::new(),
                mlock_n: 0,
            }),
            ready_cv: Condvar::new(),
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
    /// Called from writeback `post_commit` after Class C + spend annotate for the
    /// committed batch — so mlocks for heights ≤ tip are released only once
    /// writeback for those heights is done (need_heights drop when h ≤ tip).
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
        // Horizon: drop plans beyond tip+depth.
        let max_h = tip.saturating_add(g.depth);
        let far: Vec<u32> = g.plans.range((max_h + 1)..).map(|(h, _)| *h).collect();
        for h in far {
            g.plans.remove(&h);
        }
        // Decoded bodies GC at tip — multi-GB if held. Promote sticky identity.
        let mut drop_bodies: Vec<(u64, [u8; 32], u32)> = Vec::new();
        g.by_body.retain(|id, b| {
            let keep = b.height > tip && b.height <= max_h;
            if !keep {
                drop_bodies.push((*id, b.tx.txid, b.height));
            }
            keep
        });
        for (id, txid, _height) in &drop_bodies {
            g.sticky_insert(*txid, *id, *_height);
            g.thin_edges.remove(id);
        }
        // Drop header plan cache for heights at/below tip (or past depth).
        let drop_hdr: Vec<u32> = g
            .headers
            .keys()
            .copied()
            .filter(|h| *h <= tip || *h > max_h)
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
        g.gc_by_txid(tip, max_h);
        // Drop body_range not tied to live runway bodies or parent pins.
        g.gc_body_ranges();
        // Munlock when no remaining need_height in (tip, max_h] — i.e. writeback
        // finished for those heights and no later runway height still needs the page.
        let unlocks = g.gc_mlocks(tip, max_h);
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

    /// Register a create identity (txid → fk) into sticky only (no outs).
    ///
    /// Used for head-resolved external parents and tests. Bodies use sticky via
    /// [`Self::put_bodies_batch`] / insert_body.
    pub fn register_mlocked_create(&self, fk: Fk, txid: [u8; 32], height: u32) {
        let Some(id) = fk.get() else {
            return;
        };
        let mut g = self.inner.lock().unwrap();
        g.sticky_insert(txid, id, height);
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

    /// Track successful `mlock` ranges for runway height `need_height`.
    ///
    /// Keyed by `(table, page_start)`. If a later note shares `page_start` but
    /// covers a **longer** span, page refs and `rec.range` are **extended** so
    /// kernel-locked pages are never tracked short (under-track ⇒ permanent
    /// mlock leak when GC unlocks only the short range).
    ///
    /// Released when every needing height falls ≤ tip / past horizon.
    pub fn note_mlock_ranges(&self, need_height: u32, ranges: &[MlockRange]) {
        if ranges.is_empty() {
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
                    rec.need_heights.insert(need_height);
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
            let mut need = HashSet::new();
            need.insert(need_height);
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

    /// Bytes of unique 4 KiB pages currently mlocked for the confirm runway.
    ///
    /// Counts distinct `(table, page)` entries under refcount (shared ranges
    /// across heights count once). Approximate RSS contribution of prewarm pins.
    pub fn mlock_bytes(&self) -> u64 {
        const PAGE: u64 = 4096;
        let g = self.inner.lock().unwrap();
        (g.page_refs.len() as u64).saturating_mul(PAGE)
    }

    /// `(range_count, unique_page_bytes)` for prewarm pin diagnostics.
    pub fn mlock_stats(&self) -> (usize, u64) {
        const PAGE: u64 = 4096;
        let g = self.inner.lock().unwrap();
        (
            g.mlock_n,
            (g.page_refs.len() as u64).saturating_mul(PAGE),
        )
    }

    /// Cache header + tx list for a runway height (small; replaces header mlock).
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

    /// True if a full runway body is already stashed (skip store re-decode).
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
        let g = self.inner.lock().unwrap();
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
        Some((e.tx.txid, prevouts))
    }

    /// Move a full body out of the runway (wave_fill sole consumer — no clone).
    pub fn take_body(
        &self,
        fk: Fk,
    ) -> Option<(TxRecord, Vec<OutputRecord>, Vec<InputRecord>)> {
        let id = fk.get()?;
        let mut g = self.inner.lock().unwrap();
        let e = g.by_body.remove(&id)?;
        // thin_edges may still be taken separately; body_range stays for spend annotate.
        Some((e.tx, e.outputs, e.inputs))
    }

    /// Move many bodies under **one** lock (confirm wave_fill hot path).
    ///
    /// Prefer [`Self::get_bodies_batch`] when confirm may re-queue (failed
    /// package must not empty the runway while the height stays "ready").
    pub fn take_bodies_batch(
        &self,
        fks: &[Fk],
    ) -> HashMap<u64, (TxRecord, Vec<OutputRecord>, Vec<InputRecord>)> {
        if fks.is_empty() {
            return HashMap::new();
        }
        let mut g = self.inner.lock().unwrap();
        let mut out = HashMap::with_capacity(fks.len());
        for &fk in fks {
            let Some(id) = fk.get() else {
                continue;
            };
            if let Some(e) = g.by_body.remove(&id) {
                out.insert(id, (e.tx, e.outputs, e.inputs));
            }
        }
        out
    }

    /// Clone many bodies under **one** lock (keeps runway intact for retries).
    pub fn get_bodies_batch(
        &self,
        fks: &[Fk],
    ) -> HashMap<u64, (TxRecord, Vec<OutputRecord>, Vec<InputRecord>)> {
        if fks.is_empty() {
            return HashMap::new();
        }
        let g = self.inner.lock().unwrap();
        let mut out = HashMap::with_capacity(fks.len());
        for &fk in fks {
            let Some(id) = fk.get() else {
                continue;
            };
            if let Some(e) = g.by_body.get(&id) {
                out.insert(id, (e.tx.clone(), e.outputs.clone(), e.inputs.clone()));
            }
        }
        out
    }

    /// Runway body + create height for prewarm parent pin (same-bite creates).
    ///
    /// Prefer this over a store re-decode when the create is already full-decoded
    /// on the runway (cross-height same-batch spends).
    pub fn get_body_for_pin(
        &self,
        fk: Fk,
    ) -> Option<(u32, TxRecord, Vec<OutputRecord>, Vec<InputRecord>)> {
        let id = fk.get()?;
        let g = self.inner.lock().unwrap();
        let e = g.by_body.get(&id)?;
        Some((
            e.height,
            e.tx.clone(),
            e.outputs.clone(),
            e.inputs.clone(),
        ))
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

    /// True if every Class A body in `tx_fks` is still fully decoded on the runway.
    ///
    /// Used to re-prewarm heights that were `mark_scanned` but later drained
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

    /// Attach prewarm-resolved thin edges (wave_fill fast path; no full body required).
    ///
    /// Stored only in `thin_edges` (not dual-copied onto optional `by_body`).
    pub fn put_thin_inputs(&self, fk: Fk, edges: Vec<StashedThinInput>) {
        let Some(id) = fk.get() else {
            return;
        };
        self.inner.lock().unwrap().thin_edges.insert(id, edges);
    }

    /// Thin edges stashed during prewarm, if present (clone).
    pub fn get_thin_inputs(&self, fk: Fk) -> Option<Vec<StashedThinInput>> {
        let id = fk.get()?;
        self.inner.lock().unwrap().thin_edges.get(&id).cloned()
    }

    /// Move thin edges out of the runway (wave_fill is the sole consumer).
    pub fn take_thin_inputs(&self, fk: Fk) -> Option<Vec<StashedThinInput>> {
        let id = fk.get()?;
        self.inner.lock().unwrap().thin_edges.remove(&id)
    }

    /// Move many thin-edge lists under **one** lock.
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
        // Prefer full-decoded runway bodies (decode-once cache); else mlock pins.
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
        g.package_ready(height)
    }

    /// True when the height has a **complete confirm package** on the runway:
    /// header plan, all Class A bodies, thin/create_fk edges, and external
    /// parent outs with spent filter applied. This is what wait_prewarm waits for.
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

    /// All heights in `heights` ready (scanned).
    pub fn all_ready(&self, heights: &[u32]) -> bool {
        let g = self.inner.lock().unwrap();
        heights.iter().all(|h| g.package_ready(*h))
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
    /// [`Self::notify_ready_waiters`]. Does **not** perform prewarm work.
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
            let ready = heights.iter().all(|h| g.package_ready(*h));
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

    /// Prewarm parent pin: stash live outs + mark all evaluated vouts checked.
    ///
    /// `checked` includes spent-filtered vouts that are **not** in `live` so
    /// wave can treat the set as complete without re-decoding the body.
    /// `height` is the max runway height needing this parent (GC / by_txid).
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
        g.put_parent_outs_resolved_inner(height, id, tx, live, checked, coinbase_height);
    }

    /// Many resolved parents under one lock (prewarm pin finish).
    ///
    /// Tuple: `(runway_h, fk, tx, live, checked, coinbase_height)`.
    /// `coinbase_height`: `None` = not stashed; `Some(None)` = not cb; `Some(Some(h))` = height.
    pub fn put_parent_outs_resolved_batch(
        &self,
        items: &[(
            u32,
            Fk,
            TxRecord,
            Vec<(u32, OutputRecord)>,
            Vec<u32>,
            Option<Option<u32>>,
        )],
    ) {
        if items.is_empty() {
            return;
        }
        let mut g = self.inner.lock().unwrap();
        for (height, fk, tx, live, checked, cb) in items {
            let Some(id) = fk.get() else {
                continue;
            };
            g.put_parent_outs_resolved_inner(*height, id, tx.clone(), live, checked, *cb);
        }
    }

    /// Prewarm-resolved coinbase maturity for a create fk.
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

    /// Batch-register create identities into sticky (head-resolve / tests).
    /// Items: `(fk, txid, height)`.
    pub fn register_mlocked_creates_batch(&self, items: &[(Fk, [u8; 32], u32)]) {
        if items.is_empty() {
            return;
        }
        let mut g = self.inner.lock().unwrap();
        for &(fk, txid, height) in items {
            let Some(id) = fk.get() else {
                continue;
            };
            g.sticky_insert(txid, id, height);
        }
    }

    /// One-lock bulk `txid → fk` for prewarm thin-resolve (avoids per-edge mutex).
    ///
    /// Lookup order: runway `by_txid`, then **confirmed sticky**. Missing keys
    /// omitted (caller treats as head-probe candidates).
    ///
    /// Returns `(hits, sticky_only_txids)` — sticky_only are keys not in runway
    /// `by_txid` but found in sticky.
    pub fn lookup_txids_batch(
        &self,
        txids: &[[u8; 32]],
    ) -> (HashMap<[u8; 32], Fk>, HashSet<[u8; 32]>) {
        if txids.is_empty() {
            return (HashMap::new(), HashSet::new());
        }
        let g = self.inner.lock().unwrap();
        let mut out = HashMap::with_capacity(txids.len() / 2);
        let mut sticky_only = HashSet::new();
        for txid in txids {
            if let Some(e) = g.by_txid.get(txid) {
                out.insert(*txid, Fk(e.fk));
            } else if let Some(e) = g.sticky_confirmed.get(txid) {
                out.insert(*txid, Fk(e.fk));
                sticky_only.insert(*txid);
            }
        }
        (out, sticky_only)
    }

    /// Record a known create (e.g. head-resolved external parent) into sticky.
    pub fn sticky_remember_create(&self, fk: Fk, txid: [u8; 32], height: u32) {
        let Some(id) = fk.get() else {
            return;
        };
        self.inner.lock().unwrap().sticky_insert(txid, id, height);
    }

    /// Confirmed sticky map size (perf / operator).
    pub fn sticky_confirmed_count(&self) -> usize {
        self.inner.lock().unwrap().sticky_confirmed.len()
    }

    /// Reserve a hole for a prevout not in UTXO (create is still on the runway).
    ///
    /// If the create was already registered with full outs (create-before-reserve),
    /// fills immediately without a store round-trip.
    pub fn reserve(&self, height: u32, prev_txid: [u8; 32], vout: u32) {
        let mut g = self.inner.lock().unwrap();
        // Already filled from runway create or prior UTXO load?
        if let Some(ent) = g.by_txid.get(&prev_txid).copied() {
            let id = ent.fk;
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
        g.insert_by_txid(txid, id, create_height);
        {
            let e = g.by_fk.entry(id).or_insert_with(|| ParentEntry {
                tx: tx.clone(),
                outs: HashMap::new(),
                checked: HashSet::new(),
                coinbase_height: None,
                create_height: Some(create_height),
            });
            e.tx = tx.clone();
            e.create_height = Some(create_height);
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

    /// True when prewarm pin can skip store decode for `vouts` (wave already
    /// served by `get_parent_outs_needed` / body path).
    ///
    /// Covered if sparse `by_fk` has a full checked set (or all live outs), or
    /// a runway `by_body` holds every requested index.
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

    /// One-lock runway hit: `txid` known on runway (mlock or full body).
    ///
    /// With mlock prewarm we only have `by_txid` (no parsed outs); vout validity
    /// is checked when confirm full-parses the parent.
    pub fn get_by_txid_if_out(&self, txid: &[u8; 32], vout: u32) -> Option<Fk> {
        let g = self.inner.lock().unwrap();
        let id = g.by_txid.get(txid)?.fk;
        // Mlock path: by_txid alone is the registration (no parsed outs).
        if g.mlock_n > 0 {
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

    /// See [`Self::parent_pin_covered`].
    ///
    /// Only **spent-filtered** `by_fk` entries count. Bare runway `by_body` does
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

    /// Txid of a stashed parent create (by_fk sparse or runway body) — no clone of outs.
    pub fn get_parent_txid(&self, fk: Fk) -> Option<[u8; 32]> {
        let id = fk.get()?;
        let g = self.inner.lock().unwrap();
        if let Some(e) = g.by_fk.get(&id) {
            return Some(e.tx.txid);
        }
        g.by_body.get(&id).map(|b| b.tx.txid)
    }

    pub fn get_by_txid(&self, txid: &[u8; 32]) -> Option<Fk> {
        let g = self.inner.lock().unwrap();
        // Prefer sticky (bodies + head resolves). Optional by_txid is legacy.
        if let Some(e) = g.sticky_confirmed.get(txid) {
            return Some(Fk(e.fk));
        }
        g.by_txid.get(txid).map(|e| Fk(e.fk))
    }

    /// Sparse external-parent outs only (`by_fk`). Does **not** expand runway
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

    /// Clone only the requested parent vouts (sparse `by_fk`, else runway body).
    ///
    /// Returns `(tx, live_outs, spent_filtered)`:
    /// - `spent_filtered == true`: prewarm already dropped spent vouts; wave
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
            // Fully resolved by prewarm (all requested vouts checked).
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

    /// Sparse external parents in `by_fk` (not every runway create).
    pub fn parent_count(&self) -> usize {
        self.inner.lock().unwrap().by_fk.len()
    }

    /// Runway-scoped `txid → fk` map size (should stay O(depth × txs), not O(chain)).
    pub fn by_txid_count(&self) -> usize {
        self.inner.lock().unwrap().by_txid.len()
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
    /// Insert or refresh txid → fk; keep max height so parent stays while needed.
    fn insert_by_txid(&mut self, txid: [u8; 32], fk: u64, height: u32) {
        match self.by_txid.entry(txid) {
            std::collections::hash_map::Entry::Occupied(mut e) => {
                let ent = e.get_mut();
                ent.fk = fk;
                if height > ent.height {
                    ent.height = height;
                }
            }
            std::collections::hash_map::Entry::Vacant(v) => {
                v.insert(TxidEntry { fk, height });
            }
        }
    }

    /// Insert create identity into capacity-capped sticky (FIFO eviction).
    fn sticky_insert(&mut self, txid: [u8; 32], fk: u64, height: u32) {
        use std::collections::hash_map::Entry;
        match self.sticky_confirmed.entry(txid) {
            Entry::Occupied(mut e) => {
                let ent = e.get_mut();
                ent.fk = fk;
                if height > ent.height {
                    ent.height = height;
                }
            }
            Entry::Vacant(v) => {
                v.insert(StickyConfirmed { fk, height });
                self.sticky_fifo.push_back(txid);
            }
        }
        while self.sticky_confirmed.len() > self.sticky_cap {
            let Some(old) = self.sticky_fifo.pop_front() else {
                break;
            };
            // Skip stale fifo entries (txid was updated in place without re-queue).
            if self.sticky_confirmed.contains_key(&old) {
                // Only drop if this key is still "first-seen" generation: if it
                // was re-inserted as Occupied, it stays in map but not re-queued —
                // then a later pop of the original fifo slot would remove a live
                // entry. Mitigate: only remove if fifo has no newer semantics;
                // for Occupied updates we don't re-queue, so pop removes live key
                // which is wrong. Fix: on Occupied, leave fifo alone; on Vacant,
                // push. Eviction pop removes oldest Vacant-insert. If that key was
                // later Occupied-updated, we still remove it — acceptable (cap).
                self.sticky_confirmed.remove(&old);
            }
        }
    }

    /// Drop `by_txid` entries outside `(tip−depth, tip+depth]` unless live.
    ///
    /// Keeping **depth behind tip** is intentional: external parents resolved
    /// via head are keyed by spend height; re-spends of the same create in
    /// the next few hundred blocks should hit this map (no second head probe).
    fn gc_by_txid(&mut self, tip: u32, max_h: u32) {
        let min_keep = tip.saturating_sub(self.depth);
        self.by_txid.retain(|_txid, e| {
            if e.height > min_keep && e.height <= max_h {
                return true;
            }
            // Still a live runway body or sparse parent with outs.
            self.by_body.contains_key(&e.fk) || self.by_fk.contains_key(&e.fk)
        });
    }

    fn insert_body(
        &mut self,
        id: u64,
        height: u32,
        tx: TxRecord,
        outputs: Vec<OutputRecord>,
        inputs: Vec<InputRecord>,
    ) {
        // Identity: sticky only (not dual by_txid). Runway by_txid was a
        // prev_txid-era index; thin/unpin/connect no longer need it for bodies.
        // External head resolves still use register_mlocked_creates → sticky.
        self.sticky_insert(tx.txid, id, height);
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
                    checked: HashSet::new(),
                    coinbase_height: None,
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
        );
    }

    /// Attach plan keep-alive for an already-stashed parent (no re-decode).
    fn touch_parent_needs_inner(&mut self, height: u32, id: u64, vouts: &[u32]) {
        // Collect under by_fk/by_body first — cannot hold plan mut borrow too.
        let pins: Vec<u32> = if let Some(e) = self.by_fk.get(&id) {
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
    ) {
        self.insert_by_txid(tx.txid, id, height);
        let txid = tx.txid;
        {
            let e = self.by_fk.entry(id).or_insert_with(|| ParentEntry {
                tx: tx.clone(),
                outs: HashMap::new(),
                checked: HashSet::new(),
                coinbase_height: None,
                create_height: None,
            });
            e.tx = tx;
            if coinbase_height.is_some() {
                e.coinbase_height = coinbase_height;
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
            // covers spent-filtered package_ready for other runway heights.
            // Dropping the whole entry here left tip+N incomplete while
            // ready_through stayed high until the next tip GC (prewarm cursor
            // jumped past the hole). Full drop is tip GC / gc_orphaned_parents.
            if e.outs.is_empty() && e.checked.is_empty() {
                let txid = e.tx.txid;
                self.by_fk.remove(&id);
                if !self.by_body.contains_key(&id)
                    && self.by_txid.get(&txid).is_some_and(|ent| ent.fk == id)
                {
                    self.by_txid.remove(&txid);
                }
            }
        }
    }

    /// Contiguous **package-complete** watermark from tip+1 upward.
    fn recompute_ready_through(&mut self) {
        let mut h = self.tip.saturating_add(1);
        while self.package_ready(h) {
            h = h.saturating_add(1);
        }
        self.ready_through = h.saturating_sub(1);
    }

    /// Full confirm package on runway (header + bodies + edges + external parents).
    ///
    /// Content-based — `scanned` alone is never enough. Used by wait, ready_through,
    /// and prewarm skip/rehydrate.
    fn package_ready(&self, height: u32) -> bool {
        let Some(plan) = self.plans.get(&height) else {
            return false;
        };
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

    /// Drop body_range entries not referenced by live runway bodies or parent pins.
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

    /// Drop mlocks whose needing heights left `(tip, max_h]`.
    /// Returns ranges with full page-ref zero for the caller to `munlock`.
    fn gc_mlocks(&mut self, tip: u32, max_h: u32) -> Vec<MlockRange> {
        let mut drop_keys: Vec<(u8, u64)> = Vec::new();
        for (key, rec) in &mut self.mlocked {
            rec.need_heights.retain(|h| *h > tip && *h <= max_h);
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
                if self
                    .by_txid
                    .get(&e.tx.txid)
                    .is_some_and(|ent| ent.fk == id)
                    && !self.by_body.contains_key(&id)
                {
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
        let c = ConfirmParentCache::new(64);
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
        let c = ConfirmParentCache::new(64);
        c.advance_tip(10);
        c.ensure_plan(11, [9u8; 32]);
        c.mark_scanned(11);
        assert!(
            !c.package_ready(11),
            "scanned without bodies must not unblock confirm"
        );
        assert_eq!(c.ready_through(), 10);
    }

    /// Body drained after mark: watermark must fall to the last complete package.
    #[test]
    fn recompute_watermark_drops_when_body_drained() {
        let c = ConfirmParentCache::new(64);
        c.advance_tip(10);
        seed_coinbase_package(&c, 11, [0x11; 32], 1100);
        seed_coinbase_package(&c, 12, [0x12; 32], 1200);
        assert_eq!(c.ready_through(), 12);
        // Historical take_bodies emptied runway while ready_through stayed high.
        let _ = c.take_bodies_batch(&[Fk(1200)]);
        assert!(!c.package_ready(12));
        // Without recompute, atomic watermark would still say 12.
        assert_eq!(c.ready_through(), 12, "stale until recompute");
        c.recompute_ready_watermark();
        assert_eq!(
            c.ready_through(),
            11,
            "cursor must resume at first incomplete (12)"
        );
    }

    /// Retiring the last live out must not delete spent-filtered `checked` coverage
    /// or leave ready_through above a now-incomplete height (prewarm cursor skip).
    #[test]
    fn retire_last_live_out_keeps_checked_and_recomputes_watermark() {
        let c = ConfirmParentCache::new(64);
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
    /// skipping pin left ready_through at tip+1/+2 after multi-block prewarm bites.
    #[test]
    fn cross_height_spend_needs_parent_pin_for_package_ready() {
        let c = ConfirmParentCache::new(64);
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
        assert!(
            !c.package_ready(12),
            "spend of same-bite create without by_fk pin must not be package_ready"
        );
        assert_eq!(
            c.ready_through(),
            11,
            "watermark must stop at last complete package"
        );

        // Pin from runway body (what prewarm now does for batch_create_ids).
        let (create_h, tx, outs, _ins) = c.get_body_for_pin(Fk(1100)).expect("runway body");
        assert_eq!(create_h, 11);
        c.put_parent_outs_resolved(
            12,
            Fk(1100),
            tx,
            &[(0, outs[0].clone())],
            &[0],
            Some(Some(11)),
        );
        // Content-based package_ready (no re-mark needed once pin lands).
        assert!(c.package_ready(12));
        c.mark_scanned(12); // notify + recompute ready_through
        assert_eq!(c.ready_through(), 12);
    }

    /// Regression: without advance_tip to the real IBD tip, ensure_plans rejects
    /// heights outside (0, depth] and ready_through never leaves 0 — confirm stalls.
    #[test]
    fn ensure_plans_requires_tip_horizon() {
        let c = ConfirmParentCache::new(64);
        // Cache tip still 0 (prewarm forgot advance_tip).
        c.ensure_plans(&[(360_251, [1u8; 32]), (360_252, [2u8; 32])]);
        assert_eq!(c.plan_count(), 0, "heights far above tip+depth must not seed");
        c.mark_scanned_many(&[360_251, 360_252]);
        assert_eq!(c.ready_through(), 0);

        c.advance_tip(360_250);
        c.ensure_plans(&[(360_251, [1u8; 32]), (360_252, [2u8; 32])]);
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

        let c = Arc::new(ConfirmParentCache::new(64));
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
    fn lookup_txids_batch_one_lock() {
        let c = ConfirmParentCache::new(64);
        c.advance_tip(0);
        c.register_mlocked_create(Fk(1), [1u8; 32], 1);
        c.register_mlocked_create(Fk(2), [2u8; 32], 2);
        let keys = [[1u8; 32], [2u8; 32], [9u8; 32]];
        let (hits, sticky) = c.lookup_txids_batch(&keys);
        assert_eq!(hits.len(), 2);
        // register_mlocked_create → sticky only (not by_txid).
        assert_eq!(sticky.len(), 2);
        assert_eq!(hits.get(&[1u8; 32]).copied(), Some(Fk(1)));
        assert_eq!(hits.get(&[2u8; 32]).copied(), Some(Fk(2)));
        assert!(!hits.contains_key(&[9u8; 32]));
    }

    #[test]
    fn sticky_confirmed_survives_tip_gc_of_by_txid() {
        // Creates registered on runway enter sticky; after tip advances past
        // create height, by_txid may drop but sticky still resolves thin.
        let c = ConfirmParentCache::new(32);
        c.advance_tip(0);
        let t = tx(7);
        c.put_bodies_batch(vec![(Fk(70), 5, t.clone(), vec![out(1)], vec![])]);
        assert_eq!(c.get_by_txid(&t.txid), Some(Fk(70)));
        assert!(c.sticky_confirmed_count() >= 1);
        // Advance tip past create height 5; runway window no longer holds body.
        c.advance_tip(40); // min_keep=8 for depth=32 → height 5 by_txid may drop
        // Sticky still has the create.
        let keys = [t.txid];
        let (hits, sticky_only) = c.lookup_txids_batch(&keys);
        assert_eq!(hits.get(&t.txid).copied(), Some(Fk(70)));
        // Prefer sticky_only if by_txid already GC'd.
        if sticky_only.contains(&t.txid) {
            assert_eq!(sticky_only.len(), 1);
        }
    }

    #[test]
    fn reserve_then_register_create_fills() {
        let c = ConfirmParentCache::new(64);
        c.advance_tip(0);
        let hash = [2u8; 32];
        // Coinbase package is ready even with open reserves on the plan.
        seed_coinbase_package(&c, 2, hash, 2002);
        let t = tx(5);
        c.reserve(2, t.txid, 0);
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
        // Simulate batch create@1 + spend@2: open reserves must not block package.
        let c = ConfirmParentCache::new(64);
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
        c.register_runway_creates(Fk(90), &t, &[out(1)], 1);
        assert!(!c.has_open_reserves(2));
        assert_eq!(c.ready_through(), 2);
    }

    #[test]
    fn phase1_register_keeps_all_outs_for_later_spend() {
        // Bodies first: create height registers all outs; spend height hits cache.
        let c = ConfirmParentCache::new(64);
        c.advance_tip(0);
        seed_coinbase_package(&c, 1, [1u8; 32], 1001);
        let t = tx(5);
        c.register_runway_creates(Fk(50), &t, &[out(42), out(43)], 1);
        assert!(c.is_ready(1));
        assert_eq!(c.get_by_txid(&t.txid), Some(Fk(50)));
        assert_eq!(c.get_parent_out(Fk(50), 1).unwrap().1.value, 43);

        seed_coinbase_package(&c, 2, [2u8; 32], 1002);
        c.put_utxo_parent(2, Fk(50), t.clone(), 1, out(43));
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
        assert!(c.has_body(Fk(50)));
        c.advance_tip(0);
        assert!(c.get_body(Fk(50)).is_some());
        // Decoded bodies GC at tip (mlock lag only — not multi-GB script RAM).
        c.advance_tip(1);
        assert!(c.get_body(Fk(50)).is_none());
        assert!(!c.has_body(Fk(50)));
    }

    /// Parent pin body_range must not accumulate forever across tip advances.
    #[test]
    fn body_range_gc_drops_orphaned_parent_ranges() {
        let c = ConfirmParentCache::new(64);
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
        // Tip past plan → orphan parent GC → body_range gone.
        c.advance_tip(1);
        assert_eq!(
            c.body_range_count(),
            0,
            "orphaned parent body_range must not leak across tip"
        );
    }

    #[test]
    fn body_prevout_edges_prefers_create_fk_without_soft_txid() {
        let c = ConfirmParentCache::new(64);
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
    fn get_by_txid_falls_back_to_sticky_after_runway_gc() {
        let c = ConfirmParentCache::new(32);
        c.advance_tip(0);
        let t = tx(11);
        c.put_body(Fk(11), 1, t.clone(), vec![out(1)], vec![]);
        assert_eq!(c.get_by_txid(&t.txid), Some(Fk(11)));
        // Tip past create; body GC'd but sticky retains identity.
        c.advance_tip(1);
        assert!(!c.has_body(Fk(11)));
        assert_eq!(c.get_by_txid(&t.txid), Some(Fk(11)));
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
    fn parent_outs_resolved_skips_spent_recheck() {
        // Prewarm stashes live outs + checked set; wave must see spent_filtered.
        let c = ConfirmParentCache::new(64);
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
        let c = ConfirmParentCache::new(64);
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
            "runway body alone must not skip external parent pin"
        );
    }

    #[test]
    fn parent_coinbase_height_stashed_for_wave() {
        let c = ConfirmParentCache::new(64);
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
    fn take_bodies_batch_moves_no_clone_left() {
        // No-worker path may still move bodies; body_range survives for annotate.
        let c = ConfirmParentCache::new(64);
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
        assert!(c.get_body(Fk(1)).is_none());
        assert!(c.get_body(Fk(2)).is_none());
        assert_eq!(c.get_body_range(Fk(1)), Some((100, 50)));
        assert_eq!(c.get_body_range(Fk(2)), Some((200, 60)));
        assert!(c.take_bodies_batch(&[Fk(1)]).is_empty());
    }

    #[test]
    fn get_bodies_batch_keeps_runway_for_retry() {
        // Worker-live confirm clones so a failed package can re-queue.
        let c = ConfirmParentCache::new(64);
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
        let c = ConfirmParentCache::new(64);
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
        let c = ConfirmParentCache::new(128);
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
        // Short runway: max plan is 3 and ready → satisfied for any headroom.
        assert!(c.headroom_ready(1, 3));
        assert!(c.headroom_ready(3, 64));
        // Seed unfinished plans further ahead (IBD publishes full runway).
        c.ensure_plan(4, [4u8; 32]);
        c.ensure_plan(5, [5u8; 32]);
        assert!(!c.headroom_ready(3, 2)); // need 5 ready, only through 3
        seed_coinbase_package(&c, 4, [4u8; 32], 1004);
        seed_coinbase_package(&c, 5, [5u8; 32], 1005);
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

    /// Same page_start, longer later note must extend page_refs (mlock leak class).
    #[test]
    fn note_mlock_extends_shorter_prior_range() {
        use rbitcoin_store::{MlockRange, MlockTable};
        let c = ConfirmParentCache::new(64);
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
        // need_heights are 1 and 2 — tip past both → unlock after writeback tip GC.
        let unlocks = c.advance_tip(10);
        assert!(
            unlocks.iter().any(|r| r.page_len >= 4096 * 4),
            "expected unlock of extended range, got {unlocks:?}"
        );
        assert_eq!(c.mlock_bytes(), 0);
    }

    /// Mlocks release at tip once no remaining need_height > tip (writeback done).
    /// Later runway heights that still need a page keep it locked.
    #[test]
    fn advance_tip_munlocks_when_writeback_done_keeps_later_needs() {
        use rbitcoin_store::{MlockRange, MlockTable};
        let c = ConfirmParentCache::new(64);
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
        // Writeback finished through 5 — height 20 still needs the page.
        let unlocks = c.advance_tip(5);
        assert!(unlocks.is_empty(), "later runway need keeps mlock: {unlocks:?}");
        assert_eq!(c.mlock_stats().0, 1);
        // Writeback finished through 20 — no remaining need → munlock.
        let unlocks = c.advance_tip(20);
        assert_eq!(unlocks.len(), 1, "munlock when writeback done for all needers");
        assert_eq!(c.mlock_stats().0, 0);
    }

    /// Bodies register sticky only (not dual by_txid). Identity survives tip GC.
    #[test]
    fn body_identity_is_sticky_not_runway_by_txid() {
        let c = ConfirmParentCache::new(32);
        c.advance_tip(0);
        for h in 1u32..=40 {
            c.ensure_plan(h, [h as u8; 32]);
            let t = tx(h as u8);
            c.put_body(Fk(h as u64), h, t, vec![out(1)], vec![]);
            c.mark_scanned(h);
        }
        // No dual by_txid registration for bodies.
        assert_eq!(c.by_txid_count(), 0);
        assert!(c.sticky_confirmed_count() >= 32);
        assert_eq!(c.get_by_txid(&tx(10).txid), Some(Fk(10)));
        c.advance_tip(45);
        // Bodies GC'd; sticky still serves identity.
        assert!(!c.has_body(Fk(10)));
        assert!(!c.has_body(Fk(30)));
        assert_eq!(c.get_by_txid(&tx(10).txid), Some(Fk(10)));
        assert_eq!(c.get_by_txid(&tx(30).txid), Some(Fk(30)));
    }

    /// Synthetic pressure: many full bodies must leave RAM when tip catches up.
    #[test]
    fn large_runway_bodies_do_not_accumulate_past_tip() {
        let c = ConfirmParentCache::new(256);
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
        // Advance tip through all — bodies must be gone (not held for 32/64).
        c.advance_tip(64);
        assert_eq!(
            c.body_count(),
            0,
            "decoded bodies must not leak behind tip after writeback tip GC"
        );
        assert_eq!(c.body_range_count(), 0);
    }
}
