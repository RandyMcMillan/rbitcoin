//! Dedicated confirm engine (Class C tip walk) for IBD.

use super::body::BodyPresence;
use super::status::LoopStats;
use crate::chain::ChainHub;
use bitcoin::hashes::Hash;
use bitcoin::BlockHash;
use rbitcoin_consensus::{PlanStampOutcome, WirePrepPipeline};
use rbitcoin_log::{debug, info, warn};
use rbitcoin_primitives::Fk;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Prep-thread state so prep(N+1) can plan while commit(N) has not advanced tip.
///
/// In-flight maps are `Arc` so each claim only bumps refcounts (no deep clone).
struct PrepAheadState {
    next_tx_start: u64,
    in_flight_creates: std::sync::Arc<HashMap<[u8; 32], Fk>>,
    /// Pin values are Arc so `note_plan_ok` only bumps refcounts (no deep clone).
    in_flight_outs: std::sync::Arc<
        HashMap<
            u64,
            std::sync::Arc<(
                rbitcoin_store::TxRecord,
                Vec<rbitcoin_store::OutputRecord>,
                Vec<u32>,
            )>,
        >,
    >,
    /// Last height successfully prepped (still in pipeline or already committed).
    last_prepped: Option<(u32, [u8; 32])>,
}

impl PrepAheadState {
    fn new(hub: &ChainHub) -> Self {
        let next = hub.query.tx_body_count().saturating_add(1).max(1);
        Self {
            next_tx_start: next,
            in_flight_creates: std::sync::Arc::new(HashMap::new()),
            in_flight_outs: std::sync::Arc::new(HashMap::new()),
            last_prepped: None,
        }
    }

    /// Drop creates that are already **head-findable** (and thus resolvable
    /// without the pipeline map).
    ///
    /// **Must not use body count alone.** Class A commit is body → head →
    /// residency; during `tx.head` seal/roll the body count jumps while head
    /// insert is blocked for seconds. Pruning on body count drops in-flight
    /// parents that head cannot resolve yet → `parent create_fk unresolved`
    /// and a permanent tip blacklist (mainnet ~269050 first segment seal).
    ///
    /// `next_tx_start` still tracks body count (next free create fk).
    fn prune_committed(&mut self, hub: &ChainHub) {
        let body_n = hub.query.tx_body_count();
        // Head-occupied ≈ highest create_fk published into the segmented head
        // (dense 1..N inserts). Keep anything body-ahead-of-head in-flight.
        let head_n = hub.query.tx_head_occupied();
        let creates = std::sync::Arc::make_mut(&mut self.in_flight_creates);
        let outs = std::sync::Arc::make_mut(&mut self.in_flight_outs);
        prune_inflight_maps(head_n, body_n, creates, outs, &mut self.next_tx_start);
        if let Some((h, _)) = self.last_prepped {
            let tip = hub.tip_height().unwrap_or(0);
            if h <= tip {
                self.last_prepped = None;
            }
        }
    }

    fn pipeline_for(&self, path_lo: u32, store_path_lo: u32) -> WirePrepPipeline {
        let parent_hash = if path_lo == store_path_lo {
            None
        } else {
            self.last_prepped
                .filter(|(h, _)| *h + 1 == path_lo)
                .map(|(_, hash)| hash)
        };
        WirePrepPipeline {
            path_lo,
            parent_hash,
            next_tx_start: self.next_tx_start,
            in_flight_creates: std::sync::Arc::clone(&self.in_flight_creates),
            in_flight_outs: std::sync::Arc::clone(&self.in_flight_outs),
        }
    }

    fn note_plan_ok(
        &mut self,
        _hub: &ChainHub,
        plan: &rbitcoin_query::ArchiveWritePlan,
        last_height: u32,
        last_hash: [u8; 32],
    ) {
        let creates = std::sync::Arc::make_mut(&mut self.in_flight_creates);
        let outs = std::sync::Arc::make_mut(&mut self.in_flight_outs);
        // Prefer plan-time Arc pin (layout denserels). Fall back only if lengths
        // mismatch (tests constructing partial ArchiveWritePlan).
        if plan.batch_pin.len() == plan.planned_fks.len() {
            for (fk, pin) in plan.planned_fks.iter().zip(plan.batch_pin.iter()) {
                creates.insert(pin.0.txid, *fk);
                if let Some(id) = fk.get() {
                    outs.insert(id, std::sync::Arc::clone(pin));
                }
            }
        } else {
            // Partial plans: pin half is already CreatePin Arc on packed.
            for ((pin, _ins), fk) in plan.packed.iter().zip(plan.planned_fks.iter()) {
                creates.insert(pin.0.txid, *fk);
                if let Some(id) = fk.get() {
                    outs.insert(id, std::sync::Arc::clone(pin));
                }
            }
        }
        if let Some(last) = plan.planned_fks.last().and_then(|f| f.get()) {
            self.next_tx_start = last.saturating_add(1).max(1);
        }
        self.last_prepped = Some((last_height, last_hash));
    }

    fn clear_all(&mut self, hub: &ChainHub) {
        self.in_flight_creates = std::sync::Arc::new(HashMap::new());
        self.in_flight_outs = std::sync::Arc::new(HashMap::new());
        self.last_prepped = None;
        self.next_tx_start = hub.query.tx_body_count().saturating_add(1).max(1);
    }
}

/// Prune pipeline in-flight maps after commits.
///
/// Keep creates/outs with id **> head_occupied** (not yet head-findable).
/// Advance `next_tx_start` from **body_count** (next free Class A fk).
fn prune_inflight_maps(
    head_occupied: u64,
    body_count: u64,
    creates: &mut HashMap<[u8; 32], Fk>,
    outs: &mut HashMap<
        u64,
        std::sync::Arc<(
            rbitcoin_store::TxRecord,
            Vec<rbitcoin_store::OutputRecord>,
            Vec<u32>,
        )>,
    >,
    next_tx_start: &mut u64,
) {
    creates.retain(|_, fk| fk.get().map(|id| id > head_occupied).unwrap_or(false));
    outs.retain(|id, _| *id > head_occupied);
    *next_tx_start = (*next_tx_start).max(body_count.saturating_add(1).max(1));
}

/// Shared feed of tip-extension **readiness** for the dedicated confirm engine.
///
/// **Sole intake:** peer/rehydrate enqueues wire into the **body queue**, then
/// notes height/hash here. Plan/prep reload wire from the body queue — the feed
/// does **not** retain `Block`s. Class A alone is never enough (no hash-only
/// confirm). Tip-follow reorgs use peer wire via `ChainHub::accept_block`.
///
/// Optional wire slots remain for rare in-process requeue; production requeues
/// strip wire so RAM stays in the body queue + pipeline stage batches only.
///
/// **In-flight tracking:** once plan claims a contiguous run, those heights sit
/// in `inflight` until write finishes (or re-queue). `note` will not re-insert
/// them — otherwise offer re-notes tip+1 every main-loop tick and plan
/// re-claims the same batch (duplicate work).
pub(crate) struct ConfirmFeed {
    pub(crate) inner: std::sync::Mutex<ConfirmFeedInner>,
    cv: std::sync::Condvar,
    stop: AtomicBool,
}

pub(crate) struct ConfirmFeedInner {
    /// height → (hash, optional wire — normally `None`; body queue holds payloads)
    pub(crate) ready: std::collections::BTreeMap<u32, (BlockHash, Option<bitcoin::Block>)>,
    /// Claimed by prep; not yet written or released. Offer must not re-note.
    pub(crate) inflight: std::collections::HashSet<u32>,
}

impl ConfirmFeed {
    pub(crate) fn new() -> Self {
        Self {
            inner: std::sync::Mutex::new(ConfirmFeedInner {
                ready: std::collections::BTreeMap::new(),
                inflight: std::collections::HashSet::new(),
            }),
            cv: std::sync::Condvar::new(),
            stop: AtomicBool::new(false),
        }
    }

    /// Note readiness (wire lives in the body queue — denserels reloads it).
    pub(crate) fn note(&self, height: u32, hash: BlockHash) {
        self.note_wire(height, hash, None);
    }

    /// Note readiness; optional wire is only for tests / rare in-process paths.
    /// Production peer path uses [`Self::note`] (wire lives in the body queue).
    pub(crate) fn note_wire(&self, height: u32, hash: BlockHash, block: Option<bitcoin::Block>) {
        let mut g = self.inner.lock().unwrap();
        if g.inflight.contains(&height) {
            return;
        }
        // Prefer keeping wire if we already have it (test helpers only).
        match g.ready.get_mut(&height) {
            Some(e) => {
                if e.1.is_none() && block.is_some() {
                    e.1 = block;
                }
            }
            None => {
                g.ready.insert(height, (hash, block));
            }
        }
        self.cv.notify_one();
    }

    /// Return heights to the ready map (optionally with wire bodies).
    pub(crate) fn requeue_wire(&self, batch: &[(u32, BlockHash, Option<bitcoin::Block>)]) {
        if batch.is_empty() {
            return;
        }
        let mut g = self.inner.lock().unwrap();
        for &(h, hash, ref block) in batch {
            g.inflight.remove(&h);
            let b = block.clone();
            match g.ready.get_mut(&h) {
                Some(e) => {
                    if e.1.is_none() {
                        e.1 = b;
                    }
                }
                None => {
                    g.ready.insert(h, (hash, b));
                }
            }
        }
        self.cv.notify_one();
    }

    /// Write (or permanent reject) finished — height may be re-offered only after
    /// tip moves past it (or a future requeue path).
    pub(crate) fn finish(&self, heights: impl IntoIterator<Item = u32>) {
        let mut g = self.inner.lock().unwrap();
        for h in heights {
            g.inflight.remove(&h);
        }
        drop(g);
        self.cv.notify_one();
    }

    pub(crate) fn request_stop(&self) {
        self.stop.store(true, Ordering::SeqCst);
        self.cv.notify_all();
    }

    pub(crate) fn stopped(&self) -> bool {
        self.stop.load(Ordering::SeqCst)
    }

    /// `(ready_heights, inflight_heights)` — O(1) under feed mutex.
    pub(crate) fn size_snap(&self) -> (usize, usize) {
        let g = self.inner.lock().unwrap();
        (g.ready.len(), g.inflight.len())
    }
}

pub(crate) enum ConfirmEvent {
    /// Tip advanced; hash is the confirmed block.
    Accepted {
        hash: BlockHash,
    },
    /// Height is the attempted confirm height (for operator logs).
    Reject {
        height: u32,
        hash: BlockHash,
        err: String,
    },
    /// Confirm saw tip+1 without durable Class A — clear optimistic `known` and
    /// drop the feed entry so offer re-probes the store (no permanent blacklist).
    BodyMissing {
        hash: BlockHash,
    },
}

/// Hard cap on consecutive ready heights in one confirm wave.
///
/// Primary bound is soft **input** budget ([`confirm_batch_max_inputs`]); this
/// caps how many thin early-chain blocks pack into one plan/prep/script wave.
pub(crate) const CONFIRM_RUN_MAX_BLOCKS: usize = 144;

/// Default soft max Σ `tx.input` over a packed confirm run.
pub(crate) const CONFIRM_BATCH_INPUTS_DEFAULT: u32 = 8000;

/// How far ahead of tip to pre-note ready bodies into the feed.
/// ≥ [`CONFIRM_RUN_MAX_BLOCKS`] so the engine can fill a full hard-cap wave.
const OFFER_AHEAD: u32 = 192;

/// Soft max inputs per confirm batch (`RBITCOIN_CONFIRM_BATCH_INPUTS`).
pub(crate) fn confirm_batch_max_inputs() -> u32 {
    use std::sync::OnceLock;
    static N: OnceLock<u32> = OnceLock::new();
    *N.get_or_init(|| {
        let raw = std::env::var("RBITCOIN_CONFIRM_BATCH_INPUTS").ok();
        let n = raw
            .as_deref()
            .and_then(|s| s.trim().parse::<u32>().ok())
            .unwrap_or(CONFIRM_BATCH_INPUTS_DEFAULT)
            .clamp(1, 1_000_000);
        if raw.is_some() && n != CONFIRM_BATCH_INPUTS_DEFAULT {
            rbitcoin_log::info!(
                "ibd: confirm batch soft_max_inputs={n} max_blocks={} \
                 (RBITCOIN_CONFIRM_BATCH_INPUTS)",
                CONFIRM_RUN_MAX_BLOCKS
            );
        }
        n
    })
}

/// Σ `tx.input.len()` over a decoded block (confirm pack work meter).
pub(crate) fn block_input_count(block: &bitcoin::Block) -> u32 {
    block
        .txdata
        .iter()
        .map(|tx| tx.input.len() as u32)
        .fold(0u32, u32::saturating_add)
}

/// Whether the packed run should stop **after** accepting a block that left
/// `sum_inputs` / `n_blocks` in this state (soft overshoot + hard block cap).
#[inline]
pub(crate) fn pack_stop_after(sum_inputs: u32, n_blocks: usize, soft_max_inputs: u32, hard_max_blocks: usize) -> bool {
    n_blocks >= hard_max_blocks || sum_inputs > soft_max_inputs
}

/// Default capacity for each of plan→prep, prep→scripts, scripts→write queues.
pub(crate) const CONFIRM_QUEUE_CAP_DEFAULT: usize = 5;
/// Hard clamp for [`confirm_queue_cap`] (env abuse / OOM guard).
const CONFIRM_QUEUE_CAP_MAX: usize = 64;

/// Shared confirm pipeline queue capacity (all three stage queues).
///
/// Env: `RBITCOIN_CONFIRM_QUEUE` (default **5**, clamp **1..=64**). One value
/// sizes planq, prepq, and writeq together.
pub(crate) fn confirm_queue_cap() -> usize {
    use std::sync::OnceLock;
    static CAP: OnceLock<usize> = OnceLock::new();
    *CAP.get_or_init(|| {
        let raw = std::env::var("RBITCOIN_CONFIRM_QUEUE").ok();
        let n = raw
            .as_deref()
            .and_then(|s| s.trim().parse::<usize>().ok())
            .unwrap_or(CONFIRM_QUEUE_CAP_DEFAULT);
        let n = n.clamp(1, CONFIRM_QUEUE_CAP_MAX);
        if raw.is_some() && n != CONFIRM_QUEUE_CAP_DEFAULT {
            rbitcoin_log::info!(
                "ibd: confirm pipeline queues planq/prepq/writeq cap={n} \
                 (RBITCOIN_CONFIRM_QUEUE)"
            );
        }
        n
    })
}

/// Plan→prep `SyncSender` capacity (same as [`confirm_queue_cap`]).
pub(crate) fn plan_queue_cap() -> usize {
    confirm_queue_cap()
}
/// Prep→scripts capacity (same as [`confirm_queue_cap`]).
pub(crate) fn load_queue_cap() -> usize {
    confirm_queue_cap()
}
/// Scripts→write capacity (same as [`confirm_queue_cap`]).
pub(crate) fn write_queue_cap() -> usize {
    confirm_queue_cap()
}

/// Max heights claimable ahead of tip+1 (pipeline depth).
///
/// Plan may start the next run while prep/scripts/write hold earlier ones,
/// but must **not** skip a stuck tip+1 and claim thousands of far heights.
fn max_claim_ahead() -> u32 {
    let q = confirm_queue_cap();
    (q.saturating_mul(3).saturating_add(1) as u32)
        .saturating_mul(CONFIRM_RUN_MAX_BLOCKS as u32)
}

/// Plan-stage output: stamp + pipeline-local parent denserels for prep pin.
struct PlanDone {
    /// Heights/hashes for feed finish/requeue bookkeeping.
    heights_hashes: Vec<(u32, BlockHash)>,
    /// Structure + plan_mega (create_fk stamped); head-miss parents carry
    /// denserels on `ArchiveWritePlan::external_parent_outs`.
    stamped: PlanStampOutcome,
    /// In-flight creates/outs for prep pin (prior uncommitted batches).
    pipeline: WirePrepPipeline,
}

/// Live depths **and contents** of the bounded confirm pipeline queues.
///
/// Updated on successful send/recv so the status loop can log pressure and
/// process-owned retain without peeking into the OS channels.
///
/// High-water marks (`*_hwm`) track max depth since the last
/// [`ConfirmQueueDepths::sample_hwm_and_reset`] (≈5s status tick). Point
/// samples alone almost always show 0 under a plan-limited pipeline.
#[derive(Debug, Default)]
pub(crate) struct ConfirmQueueDepths {
    /// plan → prep (`SyncSender` capacity [`confirm_queue_cap`]).
    plan_to_prep: AtomicUsize,
    /// prep → scripts (`SyncSender` capacity [`confirm_queue_cap`]).
    load_to_scripts: AtomicUsize,
    /// scripts → write (`SyncSender` capacity [`confirm_queue_cap`]).
    scripts_to_write: AtomicUsize,
    /// Max plan→prep depth since last HWM sample.
    plan_hwm: AtomicUsize,
    /// Max prep→scripts depth since last HWM sample.
    load_hwm: AtomicUsize,
    /// Max scripts→write depth since last HWM sample.
    write_hwm: AtomicUsize,
    /// Sum of `batch.len()` sitting in plan→prep.
    plan_blocks: AtomicUsize,
    /// Sum of `batch.len()` sitting in prep→scripts.
    load_blocks: AtomicUsize,
    /// Sum of approx wire bytes of those batches.
    load_wire_bytes: AtomicUsize,
    /// Sum of `BatchParents` entries riding prep→scripts batches.
    load_parents: AtomicUsize,
    /// Sum of `batch.len()` sitting in scripts→write.
    write_blocks: AtomicUsize,
    write_wire_bytes: AtomicUsize,
    write_parents: AtomicUsize,
}

/// Snapshot of confirm pipeline retain (queue depths + batch contents + feed).
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct ConfirmPipelineSizes {
    pub plan_batches: usize,
    pub plan_blocks: usize,
    pub load_batches: usize,
    pub load_blocks: usize,
    pub load_wire_bytes: usize,
    pub load_parents: usize,
    pub write_batches: usize,
    pub write_blocks: usize,
    pub write_wire_bytes: usize,
    pub write_parents: usize,
    pub feed_ready: usize,
    pub feed_inflight: usize,
}

/// Format one confirm pipeline queue slot for logs.
///
/// Depth 0 uses `name<0/cap` (next worker waiting on an empty queue);
/// otherwise `name=n/cap`.
#[inline]
pub(crate) fn format_queue_depth(name: &str, depth: usize, cap: usize) -> String {
    if depth == 0 {
        format!("{name}<0/{cap}")
    } else {
        format!("{name}={depth}/{cap}")
    }
}

/// Confirm pipeline queue depths for progress/perf: `planq… prepq… writeq…`.
///
/// Depth 0 uses `name<0/cap` (consumer waiting on empty queue).
/// `planq` = plan→prep; `prepq` = prep→scripts; `writeq` = scripts→write.
#[inline]
pub(crate) fn format_conf_q(
    plan: usize,
    prep: usize,
    write: usize,
    plan_cap: usize,
    prep_cap: usize,
    write_cap: usize,
) -> String {
    format!(
        "{} {} {}",
        format_queue_depth("planq", plan, plan_cap),
        format_queue_depth("prepq", prep, prep_cap),
        format_queue_depth("writeq", write, write_cap),
    )
}

impl ConfirmQueueDepths {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// `(plan→prep, prep→scripts, scripts→write)`.
    pub(crate) fn snap(&self) -> (usize, usize, usize) {
        (
            self.plan_to_prep.load(Ordering::Relaxed),
            self.load_to_scripts.load(Ordering::Relaxed),
            self.scripts_to_write.load(Ordering::Relaxed),
        )
    }

    /// Max queue depths since last call; resets HWMs to 0.
    pub(crate) fn sample_hwm_and_reset(&self) -> (usize, usize, usize) {
        (
            self.plan_hwm.swap(0, Ordering::Relaxed),
            self.load_hwm.swap(0, Ordering::Relaxed),
            self.write_hwm.swap(0, Ordering::Relaxed),
        )
    }

    /// Full content snapshot (depths + blocks/wire/parents in each queue).
    pub(crate) fn content_snap(&self) -> ConfirmPipelineSizes {
        ConfirmPipelineSizes {
            plan_batches: self.plan_to_prep.load(Ordering::Relaxed),
            plan_blocks: self.plan_blocks.load(Ordering::Relaxed),
            load_batches: self.load_to_scripts.load(Ordering::Relaxed),
            load_blocks: self.load_blocks.load(Ordering::Relaxed),
            load_wire_bytes: self.load_wire_bytes.load(Ordering::Relaxed),
            load_parents: self.load_parents.load(Ordering::Relaxed),
            write_batches: self.scripts_to_write.load(Ordering::Relaxed),
            write_blocks: self.write_blocks.load(Ordering::Relaxed),
            write_wire_bytes: self.write_wire_bytes.load(Ordering::Relaxed),
            write_parents: self.write_parents.load(Ordering::Relaxed),
            feed_ready: 0,
            feed_inflight: 0,
        }
    }

    #[inline]
    fn note_depth_hwm(hwm: &AtomicUsize, depth_after: usize) {
        // AtomicUsize::fetch_max is stable; Relaxed is enough for perf.
        let _ = hwm.fetch_max(depth_after, Ordering::Relaxed);
    }

    fn note_plan_send(&self, blocks: usize) {
        let d = self.plan_to_prep.fetch_add(1, Ordering::Relaxed) + 1;
        Self::note_depth_hwm(&self.plan_hwm, d);
        self.plan_blocks.fetch_add(blocks, Ordering::Relaxed);
    }
    fn note_plan_recv(&self, blocks: usize) {
        self.plan_to_prep.fetch_sub(1, Ordering::Relaxed);
        self.plan_blocks
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| {
                Some(n.saturating_sub(blocks))
            })
            .ok();
    }

    fn note_load_send(&self, blocks: usize, wire_bytes: usize, parents: usize) {
        let d = self.load_to_scripts.fetch_add(1, Ordering::Relaxed) + 1;
        Self::note_depth_hwm(&self.load_hwm, d);
        self.load_blocks.fetch_add(blocks, Ordering::Relaxed);
        self.load_wire_bytes
            .fetch_add(wire_bytes, Ordering::Relaxed);
        self.load_parents.fetch_add(parents, Ordering::Relaxed);
    }
    fn note_load_recv(&self, blocks: usize, wire_bytes: usize, parents: usize) {
        self.load_to_scripts.fetch_sub(1, Ordering::Relaxed);
        self.load_blocks
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| {
                Some(n.saturating_sub(blocks))
            })
            .ok();
        self.load_wire_bytes
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| {
                Some(n.saturating_sub(wire_bytes))
            })
            .ok();
        self.load_parents
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| {
                Some(n.saturating_sub(parents))
            })
            .ok();
    }
    fn note_write_send(&self, blocks: usize, wire_bytes: usize, parents: usize) {
        let d = self.scripts_to_write.fetch_add(1, Ordering::Relaxed) + 1;
        Self::note_depth_hwm(&self.write_hwm, d);
        self.write_blocks.fetch_add(blocks, Ordering::Relaxed);
        self.write_wire_bytes
            .fetch_add(wire_bytes, Ordering::Relaxed);
        self.write_parents.fetch_add(parents, Ordering::Relaxed);
    }
    fn note_write_recv(&self, blocks: usize, wire_bytes: usize, parents: usize) {
        self.scripts_to_write.fetch_sub(1, Ordering::Relaxed);
        self.write_blocks
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| {
                Some(n.saturating_sub(blocks))
            })
            .ok();
        self.write_wire_bytes
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| {
                Some(n.saturating_sub(wire_bytes))
            })
            .ok();
        self.write_parents
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| {
                Some(n.saturating_sub(parents))
            })
            .ok();
    }
}

/// True when a plan/prep error should re-queue the batch (not permanent reject).
///
/// **Policy:** internal confirm invariants are permanent failures — fix the
/// root cause (in-flight prune, denserels pin, claim readiness). Soft-looping
/// hid pipeline bugs and either livelocked tip (requeue forever) or froze it
/// after a hard blacklist. Wire recovery uses soft re-getdata in
/// [`super::events::apply_confirm_reject`] (`unexpected previous header` only)
/// or [`ConfirmEvent::BodyMissing`] — not this path.
///
/// Kept as a named hook so multi-block / prep error handling stays uniform;
/// always returns false.
#[inline]
pub(crate) fn is_confirm_load_retryable(_msg: &str) -> bool {
    false
}

/// OS-thread occupancy for the confirm pipeline (plan / prep / scripts / write).
///
/// Stage `plan_ms` / `script_ms` / … are **work** sums and mis-rank the long
/// pole when planq is empty. These timers include **wait** (claim, recv, send
/// block) so a 5s window can show who is busy vs idle.
pub(crate) mod confirm_thr_stats {
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::Duration;

    static PLAN_CLAIM_NS: AtomicU64 = AtomicU64::new(0);
    static PLAN_RESOLVE_NS: AtomicU64 = AtomicU64::new(0);
    static PLAN_CLONE_NS: AtomicU64 = AtomicU64::new(0);
    static PLAN_STAMP_NS: AtomicU64 = AtomicU64::new(0);
    static PLAN_OTHER_NS: AtomicU64 = AtomicU64::new(0);
    static PLAN_SEND_WAIT_NS: AtomicU64 = AtomicU64::new(0);

    static PREP_RECV_WAIT_NS: AtomicU64 = AtomicU64::new(0);
    static PREP_WORK_NS: AtomicU64 = AtomicU64::new(0);
    static PREP_SEND_WAIT_NS: AtomicU64 = AtomicU64::new(0);

    static SCRIPT_RECV_WAIT_NS: AtomicU64 = AtomicU64::new(0);
    static SCRIPT_WORK_NS: AtomicU64 = AtomicU64::new(0);
    static SCRIPT_SEND_WAIT_NS: AtomicU64 = AtomicU64::new(0);

    static WRITE_RECV_WAIT_NS: AtomicU64 = AtomicU64::new(0);
    static WRITE_WORK_NS: AtomicU64 = AtomicU64::new(0);

    #[inline]
    fn add(a: &AtomicU64, d: Duration) {
        let ns = d.as_nanos() as u64;
        if ns > 0 {
            a.fetch_add(ns, Ordering::Relaxed);
        }
    }

    #[inline]
    pub fn add_plan_claim(d: Duration) {
        add(&PLAN_CLAIM_NS, d);
    }
    #[inline]
    pub fn add_plan_resolve(d: Duration) {
        add(&PLAN_RESOLVE_NS, d);
    }
    #[inline]
    pub fn add_plan_clone(d: Duration) {
        add(&PLAN_CLONE_NS, d);
    }
    #[inline]
    pub fn add_plan_stamp(d: Duration) {
        add(&PLAN_STAMP_NS, d);
    }
    #[inline]
    pub fn add_plan_other(d: Duration) {
        add(&PLAN_OTHER_NS, d);
    }
    #[inline]
    pub fn add_plan_send_wait(d: Duration) {
        add(&PLAN_SEND_WAIT_NS, d);
    }

    #[inline]
    pub fn add_prep_recv_wait(d: Duration) {
        add(&PREP_RECV_WAIT_NS, d);
    }
    #[inline]
    pub fn add_prep_work(d: Duration) {
        add(&PREP_WORK_NS, d);
    }
    #[inline]
    pub fn add_prep_send_wait(d: Duration) {
        add(&PREP_SEND_WAIT_NS, d);
    }

    #[inline]
    pub fn add_script_recv_wait(d: Duration) {
        add(&SCRIPT_RECV_WAIT_NS, d);
    }
    #[inline]
    pub fn add_script_work(d: Duration) {
        add(&SCRIPT_WORK_NS, d);
    }
    #[inline]
    pub fn add_script_send_wait(d: Duration) {
        add(&SCRIPT_SEND_WAIT_NS, d);
    }

    #[inline]
    pub fn add_write_recv_wait(d: Duration) {
        add(&WRITE_RECV_WAIT_NS, d);
    }
    #[inline]
    pub fn add_write_work(d: Duration) {
        add(&WRITE_WORK_NS, d);
    }

    #[derive(Debug, Default, Clone, Copy)]
    pub struct Sample {
        pub plan_claim_ns: u64,
        pub plan_resolve_ns: u64,
        pub plan_clone_ns: u64,
        pub plan_stamp_ns: u64,
        pub plan_other_ns: u64,
        pub plan_send_wait_ns: u64,
        pub prep_recv_wait_ns: u64,
        pub prep_work_ns: u64,
        pub prep_send_wait_ns: u64,
        pub script_recv_wait_ns: u64,
        pub script_work_ns: u64,
        pub script_send_wait_ns: u64,
        pub write_recv_wait_ns: u64,
        pub write_work_ns: u64,
    }

    pub fn sample_and_reset() -> Sample {
        Sample {
            plan_claim_ns: PLAN_CLAIM_NS.swap(0, Ordering::Relaxed),
            plan_resolve_ns: PLAN_RESOLVE_NS.swap(0, Ordering::Relaxed),
            plan_clone_ns: PLAN_CLONE_NS.swap(0, Ordering::Relaxed),
            plan_stamp_ns: PLAN_STAMP_NS.swap(0, Ordering::Relaxed),
            plan_other_ns: PLAN_OTHER_NS.swap(0, Ordering::Relaxed),
            plan_send_wait_ns: PLAN_SEND_WAIT_NS.swap(0, Ordering::Relaxed),
            prep_recv_wait_ns: PREP_RECV_WAIT_NS.swap(0, Ordering::Relaxed),
            prep_work_ns: PREP_WORK_NS.swap(0, Ordering::Relaxed),
            prep_send_wait_ns: PREP_SEND_WAIT_NS.swap(0, Ordering::Relaxed),
            script_recv_wait_ns: SCRIPT_RECV_WAIT_NS.swap(0, Ordering::Relaxed),
            script_work_ns: SCRIPT_WORK_NS.swap(0, Ordering::Relaxed),
            script_send_wait_ns: SCRIPT_SEND_WAIT_NS.swap(0, Ordering::Relaxed),
            write_recv_wait_ns: WRITE_RECV_WAIT_NS.swap(0, Ordering::Relaxed),
            write_work_ns: WRITE_WORK_NS.swap(0, Ordering::Relaxed),
        }
    }
}

/// Spawn confirm **plan** + **prep** + **scripts** + **write** OS threads.
///
/// Plan (claim + structure + stamp create_fk) → depth-5 →
/// prep (pin denserels + assemble) → scripts → write.
/// Overlap: plan(N+1) head-stamp ∥ prep(N) denserels ∥ scripts ∥ write.
/// Handoff is owned [`PlanStampOutcome`] (not residency FIFO).
/// Returns the plan-thread join handle and shared queue-depth counters.
pub(crate) fn spawn_confirm_engine(
    hub: Arc<ChainHub>,
    feed: Arc<ConfirmFeed>,
    event_tx: std::sync::mpsc::Sender<ConfirmEvent>,
    accepted: Arc<AtomicU32>,
    loop_stats: Arc<LoopStats>,
) -> (std::thread::JoinHandle<()>, Arc<ConfirmQueueDepths>) {
    let queues = ConfirmQueueDepths::new();
    let qcap = confirm_queue_cap();
    let (plan_tx, plan_rx) = std::sync::mpsc::sync_channel::<PlanDone>(qcap);
    let (mat_tx, mat_rx) = std::sync::mpsc::sync_channel::<(
        rbitcoin_consensus::LoadedBatch,
        u64, // load work_ns
    )>(qcap);
    let (write_tx, write_rx) =
        std::sync::mpsc::sync_channel::<rbitcoin_consensus::ScriptOkBatch>(qcap);
    // Write reject: plan drops reserved fks + last_prepped so re-plan after
    // Class A partial commit does not drift next_tx_start.
    let prep_ahead_reset = Arc::new(AtomicBool::new(false));

    // Write: structural + class_c + annotate; emits tip events.
    let hub_wb = Arc::clone(&hub);
    let feed_wb = Arc::clone(&feed);
    let event_tx_wb = event_tx.clone();
    let accepted_wb = Arc::clone(&accepted);
    let loop_stats_wb = Arc::clone(&loop_stats);
    let q_wb = Arc::clone(&queues);
    let prep_ahead_reset_wb = Arc::clone(&prep_ahead_reset);
    let write_thr = std::thread::Builder::new()
        .name("ibd-confirm-write".into())
        .spawn(move || {
            info!("ibd: confirm write on dedicated OS thread");
            loop {
                let t_recv = Instant::now();
                let batch = match write_rx.recv() {
                    Ok(b) => b,
                    Err(_) => break,
                };
                confirm_thr_stats::add_write_recv_wait(t_recv.elapsed());
                let n = batch.len();
                let wire = batch.approx_wire_bytes();
                let parents = batch.parent_count();
                q_wb.note_write_recv(n, wire, parents);
                if feed_wb.stopped() || hub_wb.query.confirm_cancelled() {
                    break;
                }
                let first_h = batch.heights_hashes().first().map(|(h, _)| *h).unwrap_or(0);
                let t0 = Instant::now();
                let heights_hashes = batch.heights_hashes();
                match hub_wb.confirm_write(batch) {
                    Ok(_outcomes) => {
                        for (height, raw) in &heights_hashes {
                            let hash = BlockHash::from_byte_array(*raw);
                            // Durable queue: drop payload only after confirm-write.
                            if let Err(e) = hub_wb.query.block_queue_dequeue_height(*height) {
                                rbitcoin_log::debug!(
                                    "ibd: block_queue dequeue h={height}: {e}"
                                );
                            }
                            loop_stats_wb
                                .confirm_blocks
                                .fetch_add(1, Ordering::Relaxed);
                            accepted_wb.fetch_add(1, Ordering::SeqCst);
                            if event_tx_wb
                                .send(ConfirmEvent::Accepted { hash })
                                .is_err()
                            {
                                feed_wb.finish(heights_hashes.iter().map(|(h, _)| *h));
                                return;
                            }
                        }
                        feed_wb.finish(heights_hashes.iter().map(|(h, _)| *h));
                        let elapsed = t0.elapsed();
                        confirm_thr_stats::add_write_work(elapsed);
                        if elapsed.as_millis() > 2_000 {
                            let p = rbitcoin_consensus::confirm_phase_stats::last_write_phases();
                            let ms = rbitcoin_consensus::confirm_phase_stats::LastWritePhases::ms;
                            info!(
                                "ibd: confirm write slow batch={n} first={first_h} wall={:?} \
                                 class_a={}ms ensure={}ms struct={}ms spent={}ms create_h={}ms \
                                 bip68={}ms class_c={}ms spend_ann={}ms tip_gc={}ms",
                                elapsed,
                                ms(p.class_a_ns),
                                ms(p.ensure_ns),
                                ms(p.structural_ns),
                                ms(p.spent_ns),
                                ms(p.create_h_ns),
                                ms(p.bip68_ns),
                                ms(p.class_c_ns),
                                ms(p.spend_ann_ns),
                                ms(p.tip_gc_ns),
                            );
                        }
                    }
                    Err(e) => {
                        confirm_thr_stats::add_write_work(t0.elapsed());
                        let msg = e.to_string();
                        if msg.contains("confirm cancelled") || feed_wb.stopped() {
                            info!("ibd: confirm write aborted: {msg}");
                            break;
                        }
                        let (height, hash) = heights_hashes
                            .first()
                            .map(|(h, raw)| (*h, BlockHash::from_byte_array(*raw)))
                            .unwrap_or((first_h, BlockHash::from_byte_array([0u8; 32])));
                        if hub_wb.has_block(&hash)
                            || (msg.contains("prevout already spent")
                                && heights_hashes.iter().all(|(_, raw)| {
                                    hub_wb.has_block(&BlockHash::from_byte_array(*raw))
                                }))
                        {
                            debug!(
                                "ibd: confirm write skip already-committed @{height} ({msg})"
                            );
                            for (_, raw) in &heights_hashes {
                                let h = BlockHash::from_byte_array(*raw);
                                if hub_wb.has_block(&h) {
                                    loop_stats_wb
                                        .confirm_blocks
                                        .fetch_add(1, Ordering::Relaxed);
                                    accepted_wb.fetch_add(1, Ordering::SeqCst);
                                    let _ = event_tx_wb.send(ConfirmEvent::Accepted { hash: h });
                                }
                            }
                            feed_wb.finish(heights_hashes.iter().map(|(h, _)| *h));
                            continue;
                        }
                        // Write reject — clear inflight (reject event handles
                        // wire soft re-get vs permanent blacklist). Invalidate
                        // prep-ahead caches so reserved create fks / last_prepped
                        // do not drift.
                        prep_ahead_reset_wb.store(true, Ordering::Release);
                        feed_wb.finish(heights_hashes.iter().map(|(h, _)| *h));
                        loop_stats_wb
                            .confirm_reject_stops
                            .fetch_add(1, Ordering::Relaxed);
                        warn!("ibd: confirm write reject @ {height}: {e}");
                        let _ = event_tx_wb.send(ConfirmEvent::Reject {
                            height,
                            hash,
                            err: msg,
                        });
                    }
                }
            }
            info!("ibd: confirm write exit");
        })
        .expect("spawn ibd-confirm-write");

    // Scripts: loaded batch → script verify → write queue.
    let hub_sc = Arc::clone(&hub);
    let feed_sc = Arc::clone(&feed);
    let event_tx_sc = event_tx.clone();
    let loop_stats_sc = Arc::clone(&loop_stats);
    let q_sc = Arc::clone(&queues);
    let scripts = std::thread::Builder::new()
        .name("ibd-confirm".into())
        .spawn(move || {
            info!("ibd: confirm scripts on dedicated OS thread (pure CPU; no store)");
            loop {
                let t_recv = Instant::now();
                let (mat_batch, mat_ns) = match mat_rx.recv() {
                    Ok(x) => x,
                    Err(_) => break,
                };
                confirm_thr_stats::add_script_recv_wait(t_recv.elapsed());
                let n = mat_batch.len();
                let load_wire = mat_batch.approx_wire_bytes();
                let load_parents = mat_batch.parent_count();
                q_sc.note_load_recv(n, load_wire, load_parents);
                if feed_sc.stopped() || hub_sc.query.confirm_cancelled() {
                    break;
                }
                let first_h = mat_batch
                    .heights_hashes()
                    .first()
                    .map(|(h, _)| *h)
                    .unwrap_or(0);
                let heights_hashes = mat_batch.heights_hashes();
                let t0 = Instant::now();
                // Pure: LoadedBatch → ScriptOkBatch; no Query/store.
                match rbitcoin_consensus::confirm_scripts_phase(mat_batch) {
                    Ok(outcome) => {
                        // Script-stage work only (prep wall is in LOAD/CONNECT phase stats).
                        loop_stats_sc
                            .confirm_ns
                            .fetch_add(outcome.work_ns, Ordering::Relaxed);
                        confirm_thr_stats::add_script_work(t0.elapsed());
                        let script_ms = outcome.work_ns / 1_000_000;
                        let mat_ms = mat_ns / 1_000_000;
                        let wb = outcome.batch.len();
                        let ww = outcome.batch.approx_wire_bytes();
                        let wp = outcome.batch.parent_count();
                        let t_send = Instant::now();
                        if write_tx.send(outcome.batch).is_err() {
                            info!("ibd: confirm write channel closed");
                            break;
                        }
                        confirm_thr_stats::add_script_send_wait(t_send.elapsed());
                        q_sc.note_write_send(wb, ww, wp);
                        if script_ms > 2_000 || mat_ms > 2_000 {
                            info!(
                                "ibd: confirm scripts slow batch={n} first={first_h} prep_ms={mat_ms} script_ms={script_ms} wall_ms={}",
                                t0.elapsed().as_millis()
                            );
                        }
                    }
                    Err(e) => {
                        confirm_thr_stats::add_script_work(t0.elapsed());
                        let msg = e.to_string();
                        if msg.contains("confirm cancelled") || feed_sc.stopped() {
                            info!("ibd: confirm scripts aborted: {msg}");
                            break;
                        }
                        let (height, hash) = heights_hashes
                            .first()
                            .map(|(h, raw)| (*h, BlockHash::from_byte_array(*raw)))
                            .unwrap_or((first_h, BlockHash::from_byte_array([0u8; 32])));
                        // Clear inflight so we do not pin tip forever after a script fail.
                        feed_sc.finish(heights_hashes.iter().map(|(h, _)| *h));
                        loop_stats_sc
                            .confirm_reject_stops
                            .fetch_add(1, Ordering::Relaxed);
                        // `e` may include `txid=… vin=…` from script verify annotation.
                        // Height/hash here are batch-first (not necessarily the failing block).
                        warn!(
                            "ibd: confirm scripts reject @ {height} (batch first {hash}): {e}"
                        );
                        let _ = event_tx_sc.send(ConfirmEvent::Reject {
                            height,
                            hash,
                            err: msg,
                        });
                    }
                }
            }
            drop(write_tx);
            let _ = write_thr.join();
            info!("ibd: confirm scripts exit");
        })
        .expect("spawn ibd-confirm");

    // Prep: stamped batches → pin denserels + assemble → scripts.
    let hub_prep = Arc::clone(&hub);
    let feed_prep = Arc::clone(&feed);
    let event_tx_prep = event_tx.clone();
    let loop_stats_prep = Arc::clone(&loop_stats);
    let queues_prep = Arc::clone(&queues);
    let prep_join = std::thread::Builder::new()
        .name("ibd-confirm-load".into())
        .spawn(move || {
            info!(
                "ibd: confirm prep on dedicated OS thread (planq → pin denserels+assemble)"
            );
            loop {
                let t_recv = Instant::now();
                let done = match plan_rx.recv() {
                    Ok(d) => d,
                    Err(_) => break,
                };
                confirm_thr_stats::add_prep_recv_wait(t_recv.elapsed());
                let n = done.heights_hashes.len();
                queues_prep.note_plan_recv(n);
                if feed_prep.stopped() || hub_prep.query.confirm_cancelled() {
                    break;
                }

                let plan_ns = done.stamped.work_ns;
                let heights_hashes = done.heights_hashes;
                let pipe = done.pipeline;
                if heights_hashes.is_empty() {
                    continue;
                }
                let expect_h = heights_hashes[0].0;
                let first_hash = heights_hashes[0].1;

                struct LiveGuard<'a> {
                    stats: &'a LoopStats,
                }
                impl Drop for LiveGuard<'_> {
                    fn drop(&mut self) {
                        self.stats.confirm_end();
                    }
                }
                // Prep live: block count known; inputs already accounted on plan live.
                loop_stats_prep.confirm_begin(expect_h, heights_hashes.len() as u32, 0);
                let _live_guard = LiveGuard {
                    stats: &loop_stats_prep,
                };

                // Pin denserels (Allow) + assemble using owned stamped plan — no re-plan.
                let t_work = Instant::now();
                let mat_res = hub_prep.confirm_wire_prep_from_plan(done.stamped, Some(&pipe));
                confirm_thr_stats::add_prep_work(t_work.elapsed());
                drop(_live_guard);

                if feed_prep.stopped() || hub_prep.query.confirm_cancelled() {
                    drop(mat_tx);
                    let _ = scripts.join();
                    return;
                }

                match mat_res {
                    Ok(outcome) => {
                        let work_ms = outcome.work_ns / 1_000_000;
                        let prepared_n = outcome.batch.len();
                        let wire = outcome.batch.approx_wire_bytes();
                        let parents = outcome.batch.parent_count();
                        let t_send = Instant::now();
                        if mat_tx
                            .send((outcome.batch, outcome.work_ns))
                            .is_err()
                        {
                            info!("ibd: confirm scripts channel closed");
                            return;
                        }
                        confirm_thr_stats::add_prep_send_wait(t_send.elapsed());
                        queues_prep.note_load_send(prepared_n, wire, parents);
                        if work_ms > 2_000 {
                            info!(
                                "ibd: confirm prep slow batch={prepared_n} claim={} first={expect_h} work_ms={work_ms} plan_ms={}",
                                heights_hashes.len(),
                                plan_ns / 1_000_000,
                            );
                        }
                    }
                    Err(e) => {
                        let msg = e.to_string();
                        if msg.contains("confirm cancelled") {
                            info!("ibd: confirm prep cancelled @ {expect_h}");
                            drop(mat_tx);
                            let _ = scripts.join();
                            return;
                        }
                        if is_confirm_load_retryable(&msg) {
                            let retry: Vec<(u32, BlockHash, Option<bitcoin::Block>)> =
                                heights_hashes
                                    .iter()
                                    .filter(|(_, ha)| !hub_prep.has_block(ha))
                                    .map(|(h, ha)| (*h, *ha, None))
                                    .collect();
                            feed_prep.requeue_wire(&retry);
                            static N: AtomicU32 = AtomicU32::new(0);
                            let n = N.fetch_add(1, Ordering::Relaxed) + 1;
                            if n <= 3 || n % 200 == 0 {
                                warn!(
                                    "ibd: confirm prep incomplete @ {expect_h} {first_hash} — re-queue (n={n}): {msg}"
                                );
                            }
                            std::thread::sleep(Duration::from_millis(50));
                            continue;
                        }
                        if heights_hashes.len() > 1 {
                            let tail: Vec<(u32, BlockHash, Option<bitcoin::Block>)> =
                                heights_hashes
                                    .iter()
                                    .skip(1)
                                    .filter(|(_, ha)| !hub_prep.has_block(ha))
                                    .map(|(h, ha)| (*h, *ha, None))
                                    .collect();
                            feed_prep.requeue_wire(&tail);
                        }
                        feed_prep.finish(std::iter::once(expect_h));
                        loop_stats_prep
                            .confirm_reject_stops
                            .fetch_add(1, Ordering::Relaxed);
                        warn!("ibd: confirm prep reject {first_hash} @ {expect_h}: {e}");
                        if event_tx_prep
                            .send(ConfirmEvent::Reject {
                                height: expect_h,
                                hash: first_hash,
                                err: msg,
                            })
                            .is_err()
                        {
                            break;
                        }
                        std::thread::sleep(Duration::from_millis(10));
                    }
                }
            }
            drop(mat_tx);
            let _ = scripts.join();
            info!("ibd: confirm prep exit");
        })
        .expect("spawn ibd-confirm-load");

    // Plan: claim feed → resolve wire → plan+stamp+ensure denserels → prep queue.
    let queues_plan = Arc::clone(&queues);
    let prep_ahead_reset_plan = Arc::clone(&prep_ahead_reset);
    let plan_join = std::thread::Builder::new()
        .name("ibd-confirm-plan".into())
        .spawn(move || {
            info!(
                "ibd: confirm plan on dedicated OS thread (claim → stamp create_fk → prep queue/{})",
                confirm_queue_cap()
            );
            let mut plan_ahead = PrepAheadState::new(&hub);
            loop {
                if feed.stopped() {
                    break;
                }
                let t_claim = Instant::now();
                // Packed run: fully decoded wire + total input count (confirm-side).
                let batch: (Vec<(u32, BlockHash, bitcoin::Block)>, u32) = {
                    let mut g = feed.inner.lock().unwrap();
                    let found: Option<(Vec<(u32, BlockHash, bitcoin::Block)>, u32)> = loop {
                        if feed.stopped() {
                            drop(g);
                            drop(plan_tx);
                            let _ = prep_join.join();
                            return;
                        }
                        let tip = hub.tip_height();
                        let tip_h = tip.unwrap_or(0);
                        let path_lo = if tip.is_none() {
                            0u32
                        } else {
                            tip_h.saturating_add(1)
                        };
                        g.ready.retain(|&h, _| h >= path_lo);
                        g.inflight.retain(|&h| h >= path_lo);

                        let mut claim_at = path_lo;
                        let claim_ahead = max_claim_ahead();
                        while g.inflight.contains(&claim_at)
                            && claim_at < path_lo.saturating_add(claim_ahead)
                        {
                            claim_at = claim_at.saturating_add(1);
                        }
                        let claim_start = if claim_at > path_lo.saturating_add(claim_ahead)
                        {
                            None
                        } else if g.inflight.contains(&claim_at) {
                            None
                        } else if g.ready.contains_key(&claim_at) {
                            Some(claim_at)
                        } else {
                            None
                        };
                        if let Some(expect) = claim_start {
                            let claim_hi = path_lo.saturating_add(claim_ahead);
                            let soft_inputs = confirm_batch_max_inputs();
                            let hard_blocks = CONFIRM_RUN_MAX_BLOCKS;
                            // Online pack: BQ load+decode+count one height at a time.
                            // Drop feed lock while doing IO so other note/finish continue.
                            drop(g);
                            let mut run: Vec<(u32, BlockHash, bitcoin::Block)> =
                                Vec::with_capacity(hard_blocks.min(32));
                            let mut sum_inputs = 0u32;
                            let mut h = expect;
                            let mut body_missing: Vec<(u32, BlockHash)> = Vec::new();
                            let t_pack_io = Instant::now();
                            while run.len() < hard_blocks && h <= claim_hi {
                                let (hash, _opt_wire) = {
                                    let mut gg = feed.inner.lock().unwrap();
                                    if gg.inflight.contains(&h) {
                                        break;
                                    }
                                    let Some(entry) = gg.ready.remove(&h) else {
                                        break;
                                    };
                                    entry
                                };
                                if hub.has_block(&hash) {
                                    h = h.saturating_add(1);
                                    continue;
                                }
                                // Prefer test-injected wire; production loads from BQ.
                                let block = if let Some(b) = _opt_wire {
                                    b
                                } else {
                                    match load_decode_bq_block(&hub, h, &hash) {
                                        Ok(b) => b,
                                        Err(PackWireErr::Missing) => {
                                            body_missing.push((h, hash));
                                            break;
                                        }
                                        Err(PackWireErr::HashMismatch | PackWireErr::Decode) => {
                                            body_missing.push((h, hash));
                                            // Bad wire for this height — drop BQ rec so densify re-gets.
                                            let _ = hub.query.block_queue_dequeue_height(h);
                                            break;
                                        }
                                    }
                                };
                                let inputs = block_input_count(&block);
                                {
                                    let mut gg = feed.inner.lock().unwrap();
                                    gg.inflight.insert(h);
                                }
                                run.push((h, hash, block));
                                sum_inputs = sum_inputs.saturating_add(inputs);
                                h = h.saturating_add(1);
                                if pack_stop_after(
                                    sum_inputs,
                                    run.len(),
                                    soft_inputs,
                                    hard_blocks,
                                ) {
                                    break;
                                }
                            }
                            // BQ load+decode wall (was plan_resolve before online pack).
                            confirm_thr_stats::add_plan_resolve(t_pack_io.elapsed());
                            if !body_missing.is_empty() {
                                for (mh, mhash) in &body_missing {
                                    let _ = event_tx.send(ConfirmEvent::BodyMissing {
                                        hash: *mhash,
                                    });
                                    // Height was removed from ready but not inflight.
                                    feed.finish(std::iter::once(*mh));
                                }
                            }
                            if !run.is_empty() {
                                break Some((run, sum_inputs));
                            }
                            g = feed.inner.lock().unwrap();
                            continue;
                        }
                        let (gg, wait_res) = feed
                            .cv
                            .wait_timeout(g, Duration::from_millis(20))
                            .unwrap();
                        g = gg;
                        if wait_res.timed_out() {
                            break None;
                        }
                    };
                    match found {
                        Some(x) => x,
                        None => {
                            confirm_thr_stats::add_plan_claim(t_claim.elapsed());
                            continue;
                        }
                    }
                };
                confirm_thr_stats::add_plan_claim(t_claim.elapsed());

                let (batch, batch_inputs) = batch;
                if batch.is_empty() {
                    let t_sleep = Instant::now();
                    std::thread::sleep(Duration::from_millis(20));
                    confirm_thr_stats::add_plan_claim(t_sleep.elapsed());
                    continue;
                }

                // Strict invariant: every packed entry is fully decoded (no resolve fill).
                debug_assert!(
                    !batch.is_empty(),
                    "pack produced empty batch after non-empty check"
                );

                let expect_h = batch[0].0;
                if feed.stopped() || hub.query.confirm_cancelled() {
                    // Requeue claimed heights so they are not stuck inflight.
                    let req: Vec<(u32, BlockHash, Option<bitcoin::Block>)> = batch
                        .iter()
                        .map(|(h, ha, b)| (*h, *ha, Some(b.clone())))
                        .collect();
                    feed.requeue_wire(&req);
                    drop(plan_tx);
                    let _ = prep_join.join();
                    return;
                }

                let t_other = Instant::now();
                if prep_ahead_reset_plan.swap(false, Ordering::AcqRel) {
                    plan_ahead.clear_all(&hub);
                }
                plan_ahead.prune_committed(&hub);
                confirm_thr_stats::add_plan_other(t_other.elapsed());

                // Live wall for stall watchdog / perf while plan runs (often multi-s).
                struct LiveGuard<'a> {
                    stats: &'a LoopStats,
                }
                impl Drop for LiveGuard<'_> {
                    fn drop(&mut self) {
                        self.stats.confirm_end();
                    }
                }
                loop_stats.confirm_begin(
                    expect_h,
                    batch.len() as u32,
                    batch_inputs,
                );
                let _live_guard = LiveGuard {
                    stats: &loop_stats,
                };

                // Pack always leaves decoded wire — Arc once for stamp/prep (no re-decode).
                let wire_batch: Vec<(u32, BlockHash, std::sync::Arc<bitcoin::Block>)> = batch
                    .into_iter()
                    .map(|(h, ha, w)| (h, ha, std::sync::Arc::new(w)))
                    .collect();
                let store_path_lo = match hub.tip_height() {
                    None => 0u32,
                    Some(t) => t.saturating_add(1),
                };
                let pipe = plan_ahead.pipeline_for(expect_h, store_path_lo);
                let use_pipe = pipe.path_lo >= store_path_lo;
                let mut wire_batch = wire_batch;
                let t_clone = Instant::now();
                let plan_items: Vec<(
                    rbitcoin_primitives::Height,
                    std::sync::Arc<bitcoin::Block>,
                )> = wire_batch
                    .iter()
                    .map(|(h, _, w)| (rbitcoin_primitives::Height(*h), std::sync::Arc::clone(w)))
                    .collect();
                confirm_thr_stats::add_plan_clone(t_clone.elapsed());
                let t_stamp = Instant::now();
                let plan_res = hub.confirm_wire_plan_phase(
                    &plan_items,
                    if use_pipe { Some(&pipe) } else { None },
                );
                // stamp wall continues through multi-block split retry below.
                let plan_res = match plan_res {
                    Err(e) if wire_batch.len() > 1 => {
                        let msg = e.to_string();
                        if feed.stopped()
                            || hub.query.confirm_cancelled()
                            || msg.contains("confirm cancelled")
                        {
                            Err(e)
                        } else if msg.contains("confirm without archive")
                            || msg.contains("NotFound")
                            || msg.contains("not found")
                            || is_confirm_load_retryable(&msg)
                        {
                            Err(e)
                        } else {
                            warn!(
                                "ibd: confirm plan multi-block fail @ {expect_h} n={} — \
                                 retry first alone, re-queue tail: {msg}",
                                wire_batch.len()
                            );
                            let tail: Vec<(u32, BlockHash, Option<bitcoin::Block>)> = wire_batch
                                .iter()
                                .skip(1)
                                .filter(|(_, ha, _)| !hub.has_block(ha))
                                .map(|(h, ha, _)| (*h, *ha, None))
                                .collect();
                            feed.requeue_wire(&tail);
                            // Only the first height remains for this plan attempt.
                            wire_batch.truncate(1);
                            let one_in = block_input_count(wire_batch[0].2.as_ref());
                            loop_stats.confirm_begin(expect_h, 1, one_in);
                            let one = [(
                                rbitcoin_primitives::Height(expect_h),
                                std::sync::Arc::clone(&wire_batch[0].2),
                            )];
                            hub.confirm_wire_plan_phase(
                                &one,
                                if use_pipe { Some(&pipe) } else { None },
                            )
                        }
                    }
                    other => other,
                };
                confirm_thr_stats::add_plan_stamp(t_stamp.elapsed());
                drop(_live_guard);

                match plan_res {
                    Ok(None) => {
                        let retry: Vec<(u32, BlockHash, Option<bitcoin::Block>)> = wire_batch
                            .iter()
                            .filter(|(_, ha, _)| !hub.has_block(ha))
                            .map(|(h, ha, _)| (*h, *ha, None))
                            .collect();
                        if !retry.is_empty() {
                            static N: AtomicU32 = AtomicU32::new(0);
                            let n = N.fetch_add(1, Ordering::Relaxed) + 1;
                            if n <= 3 || n % 500 == 0 {
                                debug!(
                                    "ibd: confirm plan empty outcome first={expect_h} n={} \
                                     (path not contiguous / already confirmed; re-queue, count={n})",
                                    retry.len()
                                );
                            }
                            feed.requeue_wire(&retry);
                            std::thread::sleep(Duration::from_millis(5));
                        } else {
                            feed.finish(wire_batch.iter().map(|(h, _, _)| *h));
                        }
                    }
                    Ok(Some(stamped)) => {
                        let work_ns = stamped.work_ns;
                        let ms = work_ns / 1_000_000;
                        if ms > 500 {
                            info!(
                                "ibd: confirm plan first={expect_h} n={} stamp_ms={}",
                                wire_batch.len(),
                                ms,
                            );
                        }
                        // Reserve create fks for plan(N+1) while this batch is still
                        // in prep/scripts/write (prep-ahead in-flight).
                        let t_note = Instant::now();
                        if let Some(ref p) = stamped.plan {
                            if let Some((lh, raw)) = wire_batch
                                .iter()
                                .map(|(h, ha, _)| (*h, ha.to_byte_array()))
                                .max_by_key(|(h, _)| *h)
                            {
                                plan_ahead.note_plan_ok(&hub, p, lh, raw);
                            }
                        } else if let Some((lh, ha, _)) = wire_batch.last() {
                            plan_ahead.last_prepped = Some((*lh, ha.to_byte_array()));
                        }
                        // Pipeline after note_plan_ok: prep pin sees prior+this offline denserels.
                        let pipe_for_prep = plan_ahead.pipeline_for(expect_h, store_path_lo);
                        confirm_thr_stats::add_plan_other(t_note.elapsed());
                        let heights_hashes: Vec<(u32, BlockHash)> = wire_batch
                            .iter()
                            .map(|(h, ha, _)| (*h, *ha))
                            .collect();
                        let n = heights_hashes.len();
                        let t_send = Instant::now();
                        if plan_tx
                            .send(PlanDone {
                                heights_hashes,
                                stamped,
                                pipeline: pipe_for_prep,
                            })
                            .is_err()
                        {
                            info!("ibd: confirm plan→prep channel closed");
                            let _ = prep_join.join();
                            return;
                        }
                        confirm_thr_stats::add_plan_send_wait(t_send.elapsed());
                        queues_plan.note_plan_send(n);
                    }
                    Err(e) => {
                        let msg = e.to_string();
                        if msg.contains("confirm cancelled") || feed.stopped() {
                            drop(plan_tx);
                            let _ = prep_join.join();
                            return;
                        }
                        if is_confirm_load_retryable(&msg) {
                            let retry: Vec<(u32, BlockHash, Option<bitcoin::Block>)> = wire_batch
                                .iter()
                                .filter(|(_, ha, _)| !hub.has_block(ha))
                                .map(|(h, ha, _)| (*h, *ha, None))
                                .collect();
                            feed.requeue_wire(&retry);
                            static N: AtomicU32 = AtomicU32::new(0);
                            let n = N.fetch_add(1, Ordering::Relaxed) + 1;
                            if n <= 3 || n % 200 == 0 {
                                warn!(
                                    "ibd: confirm plan incomplete first={expect_h} — re-queue (n={n}): {msg}"
                                );
                            }
                            std::thread::sleep(Duration::from_millis(50));
                            continue;
                        }
                        let (expect, hash, _) = wire_batch[0];
                        if wire_batch.len() > 1 {
                            let tail: Vec<(u32, BlockHash, Option<bitcoin::Block>)> = wire_batch
                                .iter()
                                .skip(1)
                                .filter(|(_, ha, _)| !hub.has_block(ha))
                                .map(|(h, ha, _)| (*h, *ha, None))
                                .collect();
                            feed.requeue_wire(&tail);
                        }
                        plan_ahead.clear_all(&hub);
                        feed.finish(std::iter::once(expect));
                        loop_stats
                            .confirm_reject_stops
                            .fetch_add(1, Ordering::Relaxed);
                        warn!("ibd: confirm plan reject {hash} @ {expect}: {e}");
                        let _ = event_tx.send(ConfirmEvent::Reject {
                            height: expect,
                            hash,
                            err: msg,
                        });
                        std::thread::sleep(Duration::from_millis(10));
                    }
                }
            }
            drop(plan_tx);
            let _ = prep_join.join();
            info!("ibd: confirm plan exit");
        })
        .expect("spawn ibd-confirm-plan");
    (plan_join, queues)
}

enum PackWireErr {
    Missing,
    Decode,
    HashMismatch,
}

/// Confirm-side load+decode of one body-queue height (pack path only).
fn load_decode_bq_block(
    hub: &ChainHub,
    height: u32,
    expect_hash: &BlockHash,
) -> Result<bitcoin::Block, PackWireErr> {
    use bitcoin::consensus::Decodable;
    match hub.query.block_queue_payload(height) {
        Ok(Some(payload)) => {
            let mut cursor = std::io::Cursor::new(payload.as_slice());
            match bitcoin::Block::consensus_decode(&mut cursor) {
                Ok(block) => {
                    if block.block_hash() != *expect_hash {
                        warn!(
                            "ibd: body queue hash mismatch @{height}: feed={expect_hash} payload={}",
                            block.block_hash()
                        );
                        Err(PackWireErr::HashMismatch)
                    } else {
                        Ok(block)
                    }
                }
                Err(e) => {
                    warn!("ibd: body queue decode fail @{height} {expect_hash}: {e}");
                    Err(PackWireErr::Decode)
                }
            }
        }
        Ok(None) => Err(PackWireErr::Missing),
        Err(e) => {
            warn!("ibd: body queue read fail @{height} {expect_hash}: {e}");
            Err(PackWireErr::Missing)
        }
    }
}

/// Offer a run of claim-ready heights starting at tip+1 into the confirm feed.
///
/// Pre-noting ahead of tip lets the engine batch multi-block waves when the
/// **body queue** leads tip. Caps at [`OFFER_AHEAD`].
///
/// Claim-ready = bq / pending wire only (not Class A alone).
///
/// Uses `height_to_hash` for **O(OFFER_AHEAD)** work — never scans the full
/// ordered path (that pegged a core at ~130k headers with tip frozen).
pub(crate) fn offer_confirm_ready(
    feed: &ConfirmFeed,
    height_to_hash: &HashMap<u32, BlockHash>,
    body: &mut BodyPresence,
    hub: &ChainHub,
    max_ready_height: &mut u32,
    max_ready_shared: &AtomicU32,
) -> u32 {
    let expect = match hub.tip_height() {
        None => 0u32,
        Some(t) => t.saturating_add(1),
    };
    let limit = expect.saturating_add(OFFER_AHEAD);
    let mut noted = 0u32;
    for ht in expect..=limit {
        let Some(&hash) = height_to_hash.get(&ht) else {
            break; // missing header on work path
        };
        if hub.has_block(&hash) {
            // Already confirmed; keep walking (tip may lag the RAM set briefly).
            continue;
        }
        if body.is_rejected(&hash) {
            // Tip is frozen on a permanently rejected tip+1 (consensus blacklisted).
            // Without this log, status shows confirm_blks=0 + hole=0 and looks like
            // a silent hot-path stall while archive runs ahead forever.
            if ht == expect {
                static REJECT_STUCK: AtomicU32 = AtomicU32::new(0);
                let n = REJECT_STUCK.fetch_add(1, Ordering::Relaxed) + 1;
                if n <= 3 || n % 100 == 0 {
                    warn!(
                        "ibd: confirm stuck: tip+1={ht} {hash} is blacklisted (rejected earlier); \
                         restart with a fixed binary to clear the in-memory reject set (n={n})"
                    );
                }
            }
            break;
        }
        // bq / pending only — break on first hole so tip densify is not starved.
        if !super::progress::claim_ready(hub, body, ht, &hash) {
            break;
        }
        *max_ready_height = (*max_ready_height).max(ht);
        max_ready_shared.store(*max_ready_height, Ordering::Relaxed);
        feed.note(ht, hash);
        noted += 1;
    }
    noted
}

#[cfg(test)]
mod tests {
    use super::{
        format_conf_q, format_queue_depth, is_confirm_load_retryable, prune_inflight_maps,
        ConfirmFeed, ConfirmQueueDepths,
    };
    use bitcoin::hashes::Hash;
    use bitcoin::BlockHash;
    use rbitcoin_primitives::Fk;
    use std::collections::HashMap;

    /// Body-ahead-of-head (seal window): keep in-flight fks head cannot resolve yet.
    ///
    /// Regression for mainnet tip freeze @269050 — first `tx.head` segment seal
    /// (~3.5s) with body count already past head occupied; pruning on body count
    /// dropped parents and plan failed with `parent create_fk unresolved`.
    #[test]
    fn prune_inflight_keeps_body_ahead_of_head() {
        let mut creates = HashMap::new();
        // head has 1..90; body already wrote 91..100 (seal mid head_insert_many).
        for id in 85u64..=100 {
            let mut txid = [0u8; 32];
            txid[0] = id as u8;
            creates.insert(txid, Fk(id));
        }
        let mut outs = HashMap::new();
        for id in 85u64..=100 {
            outs.insert(
                id,
                std::sync::Arc::new((
                    rbitcoin_store::TxRecord {
                        txid: {
                            let mut t = [0u8; 32];
                            t[0] = id as u8;
                            t
                        },
                        version: 1,
                        locktime: 0,
                        input_start_fk: Fk::NULL,
                        input_count: 0,
                        output_start_fk: Fk::NULL,
                        output_count: 0,
                    },
                    Vec::new(),
                    Vec::new(),
                )),
            );
        }
        let mut next = 50u64;
        prune_inflight_maps(90, 100, &mut creates, &mut outs, &mut next);
        // Head-findable (≤90) dropped; body-ahead (91..100) retained.
        assert_eq!(creates.len(), 10);
        assert_eq!(outs.len(), 10);
        for id in 91u64..=100 {
            assert!(creates.values().any(|f| f.get() == Some(id)), "keep {id}");
            assert!(outs.contains_key(&id), "keep outs {id}");
        }
        for id in 85u64..=90 {
            assert!(!creates.values().any(|f| f.get() == Some(id)), "drop {id}");
        }
        // next_tx_start advances from body, not head.
        assert_eq!(next, 101);
    }

    fn bh(b: u8) -> BlockHash {
        BlockHash::from_byte_array([b; 32])
    }

    /// Contiguous feed claim from `expect`, optional skip of already-confirmed.
    fn claim_feed_run(
        expect: u32,
        max: usize,
        claim_hi: u32,
        feed_has: impl Fn(u32) -> bool,
        already_confirmed: impl Fn(u32) -> bool,
    ) -> Vec<u32> {
        let mut run = Vec::with_capacity(max.min(32));
        let mut h = expect;
        while run.len() < max && h <= claim_hi {
            if !feed_has(h) {
                break;
            }
            if already_confirmed(h) {
                h = h.saturating_add(1);
                continue;
            }
            run.push(h);
            h = h.saturating_add(1);
        }
        run
    }

    /// Offline mirror of online pack: prefix length under soft inputs + hard blocks.
    fn pack_confirm_run_len(
        input_counts: &[u32],
        soft_max_inputs: u32,
        hard_max_blocks: usize,
    ) -> usize {
        if input_counts.is_empty() || hard_max_blocks == 0 {
            return 0;
        }
        let mut sum = 0u32;
        let mut n = 0usize;
        for &c in input_counts {
            sum = sum.saturating_add(c);
            n += 1;
            if super::pack_stop_after(sum, n, soft_max_inputs, hard_max_blocks) {
                break;
            }
        }
        n.max(1).min(input_counts.len())
    }

    #[test]
    fn pack_confirm_run_len_policy() {
        use super::{CONFIRM_BATCH_INPUTS_DEFAULT, CONFIRM_RUN_MAX_BLOCKS};
        // Under budget: take all.
        assert_eq!(pack_confirm_run_len(&[10, 10, 10], 8000, 144), 3);
        // Soft overshoot: include crossing block then stop.
        // 7990 + 100 = 8090 > 8000 → n=2
        assert_eq!(pack_confirm_run_len(&[7990, 100, 50], 8000, 144), 2);
        // First block alone exceeds soft → n=1
        assert_eq!(pack_confirm_run_len(&[50_000, 10], 8000, 144), 1);
        // Block hard cap
        let ones = vec![1u32; 200];
        assert_eq!(
            pack_confirm_run_len(&ones, CONFIRM_BATCH_INPUTS_DEFAULT, CONFIRM_RUN_MAX_BLOCKS),
            CONFIRM_RUN_MAX_BLOCKS
        );
        assert_eq!(pack_confirm_run_len(&[], 8000, 144), 0);
        // Exactly at soft: sum==soft continues? policy is sum > soft stop after take.
        // 4000+4000=8000 not > 8000 → can take more if present
        assert_eq!(pack_confirm_run_len(&[4000, 4000, 1], 8000, 144), 3);
        // After third, sum=8001 > 8000 stops at 3
        assert_eq!(pack_confirm_run_len(&[4000, 4000, 1, 1], 8000, 144), 3);
    }

    #[test]
    fn block_input_count_sums_tx_inputs() {
        use bitcoin::absolute::LockTime;
        use bitcoin::block::{Header, Version};
        use bitcoin::script::ScriptBuf;
        use bitcoin::transaction::Version as TxVersion;
        use bitcoin::{
            Amount, Block, CompactTarget, OutPoint, Sequence, Transaction, TxIn, TxOut, Witness,
        };
        let mk_tx = |n_in: usize| Transaction {
            version: TxVersion::ONE,
            lock_time: LockTime::ZERO,
            input: (0..n_in)
                .map(|i| TxIn {
                    previous_output: OutPoint {
                        txid: bitcoin::Txid::from_byte_array([i as u8; 32]),
                        vout: 0,
                    },
                    script_sig: ScriptBuf::new(),
                    sequence: Sequence::MAX,
                    witness: Witness::new(),
                })
                .collect(),
            output: vec![TxOut {
                value: Amount::from_sat(1),
                script_pubkey: ScriptBuf::new(),
            }],
        };
        let header = Header {
            version: Version::ONE,
            prev_blockhash: BlockHash::from_byte_array([0; 32]),
            merkle_root: bitcoin::TxMerkleNode::from_byte_array([0; 32]),
            time: 1,
            bits: CompactTarget::from_consensus(0x207fffff),
            nonce: 0,
        };
        let block = Block {
            header,
            txdata: vec![mk_tx(1), mk_tx(3), mk_tx(2)],
        };
        assert_eq!(super::block_input_count(&block), 6);
    }

    /// Contiguous claim + skip already-confirmed (pure claim helper).
    #[test]
    fn claim_feed_wave_and_skip_confirmed() {
        let run = claim_feed_run(101, 32, 200, |h| h >= 101 && h < 101 + 40, |_| false);
        assert_eq!(run.len(), 32);
        assert_eq!(run[0], 101);
        assert_eq!(*run.last().unwrap(), 132);
        let run = claim_feed_run(
            10,
            32,
            200,
            |h| h >= 10 && h <= 50,
            |h| h == 10 || h == 11,
        );
        assert_eq!(run.first().copied(), Some(12));
        assert_eq!(run.len(), 32);
    }

    /// Claim must not jump thousands past tip when near pipeline is full.
    #[test]
    fn claim_ahead_cap_blocks_far_skip() {
        let ahead = super::max_claim_ahead();
        assert!(ahead >= super::CONFIRM_RUN_MAX_BLOCKS as u32);
        assert!(
            ahead
                <= 64 * 3 * super::CONFIRM_RUN_MAX_BLOCKS as u32
                    + super::CONFIRM_RUN_MAX_BLOCKS as u32,
            "keep claim window within env clamp: {ahead}"
        );
        let path_lo = 87u32;
        let run = claim_feed_run(
            path_lo,
            super::CONFIRM_RUN_MAX_BLOCKS,
            path_lo + ahead,
            |h| h >= path_lo && h < path_lo + 1000,
            |_| false,
        );
        assert_eq!(run.len(), super::CONFIRM_RUN_MAX_BLOCKS);
        assert_eq!(run[0], path_lo);
        assert!(*run.last().unwrap() <= path_lo + ahead);
    }

    /// requeue_wire after empty prep must clear inflight (Ok(None) leak regression).
    #[test]
    fn requeue_clears_inflight_so_tip_can_retry() {
        let feed = ConfirmFeed::new();
        feed.note(87, bh(1));
        feed.note(88, bh(2));
        {
            let mut g = feed.inner.lock().unwrap();
            g.ready.remove(&87);
            g.ready.remove(&88);
            g.inflight.insert(87);
            g.inflight.insert(88);
        }
        feed.requeue_wire(&[(87, bh(1), None), (88, bh(2), None)]);
        let g = feed.inner.lock().unwrap();
        assert!(!g.inflight.contains(&87));
        assert!(!g.inflight.contains(&88));
        assert!(g.ready.contains_key(&87));
        assert!(g.ready.contains_key(&88));
    }

    #[test]
    fn queue_hwm_tracks_max_depth() {
        let q = ConfirmQueueDepths::new();
        q.note_plan_send(32);
        q.note_plan_send(32);
        assert_eq!(q.snap().0, 2);
        q.note_plan_recv(32);
        assert_eq!(q.snap().0, 1);
        let (ph, lh, wh) = q.sample_hwm_and_reset();
        assert_eq!(ph, 2, "hwm keeps max even after recv");
        assert_eq!(lh, 0);
        assert_eq!(wh, 0);
        let (ph2, _, _) = q.sample_hwm_and_reset();
        assert_eq!(ph2, 0, "hwm resets each sample window");
    }

    #[test]
    fn thr_stats_sample_and_reset() {
        use super::confirm_thr_stats;
        use std::time::Duration;
        let _ = confirm_thr_stats::sample_and_reset(); // clear
        confirm_thr_stats::add_plan_resolve(Duration::from_millis(10));
        confirm_thr_stats::add_plan_clone(Duration::from_millis(5));
        confirm_thr_stats::add_prep_recv_wait(Duration::from_millis(20));
        let s = confirm_thr_stats::sample_and_reset();
        assert!(s.plan_resolve_ns >= 10_000_000);
        assert!(s.plan_clone_ns >= 5_000_000);
        assert!(s.prep_recv_wait_ns >= 20_000_000);
        let busy = s
            .plan_resolve_ns
            .saturating_add(s.plan_clone_ns)
            .saturating_add(s.plan_stamp_ns)
            .saturating_add(s.plan_other_ns);
        assert_eq!(busy, s.plan_resolve_ns + s.plan_clone_ns);
        let z = confirm_thr_stats::sample_and_reset();
        assert_eq!(z.plan_resolve_ns, 0);
    }

    #[test]
    fn is_confirm_load_retryable_always_false() {
        // Policy: no plan/prep soft-requeue. Internal errors permanent; wire
        // recovery is soft re-getdata / BodyMissing only.
        assert!(!is_confirm_load_retryable(
            "confirm: load incomplete (parent package not ready, timeout)"
        ));
        assert!(!is_confirm_load_retryable(
            "confirm: load incomplete (wave body missing from cache)"
        ));
        assert!(!is_confirm_load_retryable(
            "confirm: load incomplete (parent header plan missing above tip)"
        ));
        assert!(!is_confirm_load_retryable(
            "archive: parent create_fk unresolved (contiguous batch required)"
        ));
        assert!(!is_confirm_load_retryable("script failed: false"));
        assert!(!is_confirm_load_retryable("prevout already spent"));
        assert!(!is_confirm_load_retryable("unexpected previous header"));
        assert!(!is_confirm_load_retryable("invariant: plan stage miss"));
    }

    /// note / requeue / finish lifecycle (duplicate scripts bug + re-queue).
    #[test]
    fn feed_note_requeue_finish_surface() {
        let feed = ConfirmFeed::new();
        feed.note(100, bh(1));
        {
            let mut g = feed.inner.lock().unwrap();
            let (hash, wire) = g.ready.remove(&100).unwrap();
            g.inflight.insert(100);
            assert_eq!(hash, bh(1));
            assert!(wire.is_none());
        }
        // Main loop offer would re-note tip+1 every tick — must be ignored.
        feed.note(100, bh(1));
        {
            let g = feed.inner.lock().unwrap();
            assert!(g.ready.is_empty(), "inflight height must not re-enter ready");
            assert!(g.inflight.contains(&100));
        }

        {
            let mut g = feed.inner.lock().unwrap();
            g.inflight.insert(50);
            g.inflight.insert(51);
        }
        feed.requeue_wire(&[(50, bh(5), None), (51, bh(6), None)]);
        {
            let g = feed.inner.lock().unwrap();
            assert!(!g.inflight.contains(&50));
            assert_eq!(g.ready.get(&50).map(|(h, _)| *h), Some(bh(5)));
            assert_eq!(g.ready.get(&51).map(|(h, _)| *h), Some(bh(6)));
        }

        {
            let mut g = feed.inner.lock().unwrap();
            g.inflight.insert(10);
            g.inflight.insert(11);
        }
        feed.finish([10, 11]);
        let g = feed.inner.lock().unwrap();
        assert!(!g.inflight.contains(&10));
        assert!(!g.inflight.contains(&11));
    }

    /// Log tokens + live caps (OPERATOR.md / experimental-mainnet prepq=*/5 writeq=*/5).
    #[test]
    fn queue_depth_log_and_caps_surface() {
        assert_eq!(format_queue_depth("prep", 0, 2), "prep<0/2");
        assert_eq!(format_queue_depth("write", 0, 2), "write<0/2");
        assert_eq!(format_queue_depth("prep", 1, 2), "prep=1/2");
        assert_eq!(format_queue_depth("write", 2, 2), "write=2/2");
        assert_eq!(
            format_conf_q(0, 0, 1, 2, 2, 2),
            "planq<0/2 prepq<0/2 writeq=1/2"
        );
        assert_eq!(
            format_conf_q(1, 1, 0, 2, 2, 2),
            "planq=1/2 prepq=1/2 writeq<0/2"
        );
        assert_eq!(
            format_conf_q(0, 0, 0, 2, 2, 2),
            "planq<0/2 prepq<0/2 writeq<0/2"
        );

        let cap = super::confirm_queue_cap();
        assert!(
            (1..=super::CONFIRM_QUEUE_CAP_MAX).contains(&cap),
            "confirm_queue_cap out of range: {cap}"
        );
        // Default when env unset is 5 (OnceLock: do not set RBITCOIN_CONFIRM_QUEUE in this test).
        assert_eq!(cap, super::CONFIRM_QUEUE_CAP_DEFAULT);
        assert_eq!(super::plan_queue_cap(), cap);
        assert_eq!(super::load_queue_cap(), cap);
        assert_eq!(super::write_queue_cap(), cap);
        assert_eq!(
            format_conf_q(0, 0, 0, cap, cap, cap),
            format!("planq<0/{cap} prepq<0/{cap} writeq<0/{cap}")
        );
        assert_eq!(
            format_conf_q(cap, cap, cap, cap, cap, cap),
            format!("planq={cap}/{cap} prepq={cap}/{cap} writeq={cap}/{cap}")
        );
    }

    #[test]
    fn feed_stop_size_snap_and_empty_requeue() {
        let feed = ConfirmFeed::new();
        assert!(!feed.stopped());
        assert_eq!(feed.size_snap(), (0, 0));
        feed.note(1, bh(1));
        feed.note(2, bh(2));
        {
            let mut g = feed.inner.lock().unwrap();
            g.inflight.insert(3);
        }
        assert_eq!(feed.size_snap(), (2, 1));
        feed.requeue_wire(&[]); // no-op empty
        assert_eq!(feed.size_snap(), (2, 1));
        feed.request_stop();
        assert!(feed.stopped());
    }

    #[test]
    fn claim_feed_stops_at_gap() {
        let run = claim_feed_run(5, 10, 100, |h| h == 5 || h == 6 || h == 8, |_| false);
        // Contiguous only — gap at 7 stops.
        assert_eq!(run, vec![5, 6]);
        let empty = claim_feed_run(1, 8, 100, |_| false, |_| false);
        assert!(empty.is_empty());
    }

    #[test]
    fn confirm_queue_depths_content_snap_and_notes() {
        use super::ConfirmQueueDepths;
        let q = ConfirmQueueDepths::new();
        assert_eq!(q.snap(), (0, 0, 0));
        let c0 = q.content_snap();
        assert_eq!(c0.load_batches, 0);
        assert_eq!(c0.write_batches, 0);
        assert_eq!(c0.feed_ready, 0);
        assert_eq!(c0.feed_inflight, 0);

        q.note_load_send(3, 1000, 2);
        q.note_write_send(2, 500, 1);
        let c1 = q.content_snap();
        assert_eq!(c1.load_batches, 1);
        assert_eq!(c1.load_blocks, 3);
        assert_eq!(c1.load_wire_bytes, 1000);
        assert_eq!(c1.load_parents, 2);
        assert_eq!(c1.write_batches, 1);
        assert_eq!(c1.write_blocks, 2);
        assert_eq!(c1.write_wire_bytes, 500);
        assert_eq!(c1.write_parents, 1);
        assert_eq!(q.snap(), (0, 1, 1));

        q.note_load_recv(3, 1000, 2);
        q.note_write_recv(2, 500, 1);
        let c2 = q.content_snap();
        assert_eq!(c2.load_batches, 0);
        assert_eq!(c2.write_batches, 0);
        assert_eq!(c2.load_blocks, 0);
        assert_eq!(c2.write_blocks, 0);
        // saturating sub: over-recv is safe
        q.note_load_recv(99, 99, 99);
        assert_eq!(q.content_snap().load_blocks, 0);
    }

    #[test]
    fn offer_confirm_ready_walks_height_map() {
        use super::offer_confirm_ready;
        use super::super::body::BodyPresence;
        use rbitcoin_consensus::{ChainParams, Milestone};
        use rbitcoin_query::Query;
        use std::collections::HashMap;
        use std::sync::atomic::AtomicU32;

        let dir = std::env::temp_dir().join(format!(
            "rbitcoin-offer-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let _ = std::fs::create_dir_all(&dir);
        let q = Query::open_or_create(dir.join("store")).unwrap();
        let hub = crate::chain::ChainHub::new(q, ChainParams::regtest(), Milestone::NONE);
        hub.ensure_genesis().unwrap();

        let feed = ConfirmFeed::new();
        let mut body = BodyPresence::new();
        let mut h2h = HashMap::new();
        // Tip is 0; expect tip+1 = 1. Claim-ready = body-queue pending wire only.
        let h1 = bh(0x11);
        h2h.insert(1u32, h1);
        body.mark_pending(h1);
        let mut max_arch = 0u32;
        let shared = AtomicU32::new(0);
        let n = offer_confirm_ready(&feed, &h2h, &mut body, &hub, &mut max_arch, &shared);
        assert_eq!(n, 1);
        assert_eq!(max_arch, 1);
        assert_eq!(shared.load(std::sync::atomic::Ordering::Relaxed), 1);
        assert_eq!(feed.size_snap().0, 1);

        // Rejected tip+1 stops and notes zero new.
        body.mark_rejected(h1);
        feed.finish([1]);
        let n2 = offer_confirm_ready(&feed, &h2h, &mut body, &hub, &mut max_arch, &shared);
        assert_eq!(n2, 0);

        // Gap in height map stops.
        let h2h2 = HashMap::new();
        let n3 = offer_confirm_ready(&feed, &h2h2, &mut body, &hub, &mut max_arch, &shared);
        assert_eq!(n3, 0);

        // Already-confirmed tip heights are skipped (continue walking).
        // Archive+confirm height 1 so has_block is true; offer from tip=1 expects 2.
        let h2 = bh(0x22);
        // tip is still 0 (genesis only) — mark genesis-next already confirmed via
        // has_block is only true for store tip; exercise the continue arm by
        // re-running offer after feed has height 1 already noted (inflight path).
        feed.note(1, h1); // already ready — note is idempotent when not inflight
        {
            let mut g = feed.inner.lock().unwrap();
            g.inflight.insert(1);
            g.ready.remove(&1);
        }
        // With tip+1 inflight, offer still notes if ready map empty for that height.
        let n4 = offer_confirm_ready(&feed, &h2h, &mut body, &hub, &mut max_arch, &shared);
        // rejected path already cleared h1 from ready; height 1 still rejected → 0.
        assert_eq!(n4, 0);

        // Class A alone is not claim-ready: height 1 archived without bq → offer 0.
        body = BodyPresence::new();
        let mut h2h3 = HashMap::new();
        h2h3.insert(1u32, h1);
        h2h3.insert(2u32, h2);
        body.mark_archived(h1);
        feed.finish([1]);
        max_arch = 0;
        let n5 = offer_confirm_ready(&feed, &h2h3, &mut body, &hub, &mut max_arch, &shared);
        assert_eq!(n5, 0, "Class A without body queue must not note confirm feed");

        // Pending wire (bq) is claim-ready.
        body.mark_pending(h1);
        let n6 = offer_confirm_ready(&feed, &h2h3, &mut body, &hub, &mut max_arch, &shared);
        assert_eq!(n6, 1);
        assert_eq!(max_arch, 1);
        assert_eq!(feed.size_snap().0, 1);

        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn claim_feed_skips_inflight_and_confirmed_in_helper() {
        // Pure claim helper: inflight-like skip is modeled by already_confirmed.
        // Heights 1..=10 present; skip 1,2,5 → claim 3,4,6,7,8,9,10 (7).
        let run = claim_feed_run(
            1,
            8,
            100,
            |h| (1..=10).contains(&h),
            |h| h == 1 || h == 2 || h == 5,
        );
        assert_eq!(run.first().copied(), Some(3));
        assert!(!run.contains(&5));
        assert_eq!(run, vec![3, 4, 6, 7, 8, 9, 10]);
        // Max 0 → empty.
        assert!(claim_feed_run(1, 0, 100, |_| true, |_| false).is_empty());
    }
}
