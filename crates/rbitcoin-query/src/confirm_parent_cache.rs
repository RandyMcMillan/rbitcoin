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
        // One body retain + one parent GC (was double-scanned every batch).
        // Promote dropped bodies into confirmed sticky (txid→fk) before forget.
        let mut drop_bodies: Vec<(u64, [u8; 32], u32)> = Vec::new();
        g.by_body.retain(|id, b| {
            let keep = b.height > tip && b.height <= max_h;
            if !keep {
                drop_bodies.push((*id, b.tx.txid, b.height));
            }
            keep
        });
        for (id, txid, height) in &drop_bodies {
            g.sticky_insert(*txid, *id, *height);
            g.thin_edges.remove(id);
            g.body_range.remove(id);
        }
        // Drop header plan cache + hash index for heights outside runway.
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
                        g.body_range.remove(&id);
                        g.thin_edges.remove(&id);
                    }
                }
            }
        }
        g.gc_orphaned_parents();
        g.gc_by_txid(tip, max_h);
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

    /// Register a runway create after mlock (txid → fk only; no full body).
    ///
    /// `height` is the create height (or the spend height that needed this
    /// parent). Entries are GC'd on tip advance when outside the runway window.
    pub fn register_mlocked_create(&self, fk: Fk, txid: [u8; 32], height: u32) {
        let Some(id) = fk.get() else {
            return;
        };
        let mut g = self.inner.lock().unwrap();
        g.insert_by_txid(txid, id, height);
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
    /// Same range re-noted by a later batch only adds the height (no double-count
    /// of page refs). Released when every needing height falls ≤ tip / past horizon.
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
            let key = (range.table.as_u8(), range.page_start);
            if let Some(rec) = g.mlocked.get_mut(&key) {
                rec.need_heights.insert(need_height);
                continue;
            }
            let start_page = range.page_start / PAGE;
            let end_page = range
                .page_start
                .saturating_add(range.page_len)
                .div_ceil(PAGE)
                .max(start_page);
            for p in start_page..end_page {
                *g.page_refs.entry((range.table.as_u8(), p)).or_insert(0) += 1;
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
            let ready = heights.iter().all(|h| {
                g.plans
                    .get(h)
                    .map(|p| p.is_ready())
                    .unwrap_or(false)
            });
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

    /// Batch-register runway create txids after mlock prewarm.
    /// Items: `(fk, txid, height)`.
    ///
    /// Also inserts into **confirmed sticky** (create identity) so after tip GC
    /// of the runway window thin still resolves without `tx.head`.
    pub fn register_mlocked_creates_batch(&self, items: &[(Fk, [u8; 32], u32)]) {
        if items.is_empty() {
            return;
        }
        let mut g = self.inner.lock().unwrap();
        for &(fk, txid, height) in items {
            let Some(id) = fk.get() else {
                continue;
            };
            g.insert_by_txid(txid, id, height);
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
    #[inline]
    fn pin_covered_locked(g: &Inner, id: u64, vouts: &[u32]) -> bool {
        if vouts.is_empty() {
            return true;
        }
        if let Some(e) = g.by_fk.get(&id) {
            // Prewarm already spent-filtered these vouts.
            if !e.checked.is_empty() && vouts.iter().all(|v| e.checked.contains(v)) {
                return true;
            }
            // Legacy / partial: every requested vout is a live stashed out.
            if !e.outs.is_empty() && vouts.iter().all(|v| e.outs.contains_key(v)) {
                return true;
            }
        }
        if let Some(b) = g.by_body.get(&id) {
            return vouts
                .iter()
                .all(|v| (*v as usize) < b.outputs.len());
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

    pub fn get_by_txid(&self, txid: &[u8; 32]) -> Option<Fk> {
        self.inner
            .lock()
            .unwrap()
            .by_txid
            .get(txid)
            .map(|e| Fk(e.fk))
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
        self.insert_by_txid(tx.txid, id, height);
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
            if e.outs.is_empty() {
                let txid = e.tx.txid;
                self.by_fk.remove(&id);
                // Keep by_txid if this create is still a live runway body.
                if !self.by_body.contains_key(&id)
                    && self.by_txid.get(&txid).is_some_and(|ent| ent.fk == id)
                {
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
        c.mark_scanned_many(&[360_251, 360_252]);
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
        c.ensure_plan(11, [9u8; 32]);

        let waiter = Arc::clone(&c);
        let j = thread::spawn(move || {
            waiter
                .wait_heights_ready(&[11], Duration::from_millis(500), || false)
                .expect("should become ready")
        });
        // Yield so the waiter parks on the condvar before we mark scanned.
        thread::yield_now();
        thread::sleep(Duration::from_millis(1));
        c.mark_scanned(11);
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
        assert!(sticky.is_empty()); // still in runway by_txid
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

        // Body path also covers pin skip.
        c.put_body(Fk(70), 1, tx(70), vec![out(1), out(2)], vec![]);
        assert!(c.parent_pin_covered(Fk(70), &[0, 1]));
        assert!(!c.parent_pin_covered(Fk(70), &[0, 2]));
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
        // Wave fill must move prewarmed bodies out (sole consumer), not clone.
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
        // body_range survives take (spend annotate still needs it).
        let taken = c.take_bodies_batch(&[Fk(1), Fk(2), Fk(3)]);
        assert_eq!(taken.len(), 2);
        assert_eq!(taken.get(&1).unwrap().0.txid, t1.txid);
        assert_eq!(taken.get(&2).unwrap().1[0].value, 20);
        assert!(c.get_body(Fk(1)).is_none());
        assert!(c.get_body(Fk(2)).is_none());
        assert_eq!(c.get_body_range(Fk(1)), Some((100, 50)));
        assert_eq!(c.get_body_range(Fk(2)), Some((200, 60)));
        // Second take is empty.
        assert!(c.take_bodies_batch(&[Fk(1)]).is_empty());
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

    /// Regression: `by_txid` keeps depth behind tip (sticky external parents)
    /// but still bounds growth to O(depth), not O(chain).
    #[test]
    fn advance_tip_prunes_by_txid_registrations() {
        let c = ConfirmParentCache::new(32);
        c.advance_tip(0);
        // Simulate prewarm registering many creates along the runway.
        for h in 1u32..=40 {
            c.ensure_plan(h, [h as u8; 32]);
            let t = tx(h as u8);
            c.register_mlocked_create(Fk(h as u64), t.txid, h);
            c.mark_scanned(h);
        }
        assert!(c.by_txid_count() >= 32, "registered runway creates");
        // tip=20, depth=32 → keep height in (0, 52] → all 1..=40 stay (sticky behind tip).
        c.advance_tip(20);
        assert_eq!(c.by_txid_count(), 40, "sticky window keeps depth behind tip");
        assert_eq!(c.get_by_txid(&tx(10).txid), Some(Fk(10)));
        assert_eq!(c.get_by_txid(&tx(30).txid), Some(Fk(30)));
        // tip=45 → min_keep=13, max=77 → keep 14..=40 only.
        c.advance_tip(45);
        assert!(
            c.by_txid_count() <= 27,
            "by_txid leaked: count={}",
            c.by_txid_count()
        );
        assert!(c.get_by_txid(&tx(10).txid).is_none());
        assert_eq!(c.get_by_txid(&tx(30).txid), Some(Fk(30)));
        // Re-need parent at h=50 bumps height; survives until tip passes height+depth.
        let t5 = tx(5);
        c.register_mlocked_create(Fk(5), t5.txid, 50);
        assert_eq!(c.get_by_txid(&t5.txid), Some(Fk(5)));
        c.advance_tip(50);
        assert_eq!(c.get_by_txid(&t5.txid), Some(Fk(5))); // still in sticky behind
        c.advance_tip(50 + 32); // min_keep=50 → height 50 dropped
        assert!(c.get_by_txid(&t5.txid).is_none());
    }
}
