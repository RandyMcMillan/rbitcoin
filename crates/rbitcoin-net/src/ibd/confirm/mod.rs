//! Dedicated confirm engine (Class C tip walk) for IBD.

use super::body::BodyPresence;
use super::status::LoopStats;
use crate::chain::ChainHub;
use bitcoin::hashes::Hash;
use bitcoin::BlockHash;
use rbitcoin_consensus::WireLoadPipeline;
use rbitcoin_log::{debug, info, warn};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Lookup-thread state so lookup(N+1) can run while write(N) has not advanced tip.
///
/// In-flight creates are an **immutable layer log** ([`rbitcoin_query::InFlightLog`]):
/// each successful lookup pack publishes a frozen layer; load receives
/// [`InFlightLog::snapshot`] (Arc bumps only — no `Arc::make_mut` of a shared map).
struct LoadAheadState {
    next_tx_start: u64,
    /// Append-only published packs (lookup thread only mutates via note/prune/clear).
    in_flight: rbitcoin_query::InFlightLog,
    /// Shared sparse parent pins for concurrent load/scripts/write batches.
    parent_store: std::sync::Arc<rbitcoin_query::PipelineParentStore>,
    /// Lookup-published identity union (wave hits still live in the BQ window).
    published: std::sync::Arc<rbitcoin_query::PublishedIds>,
    /// Last height successfully loaded (still in pipeline or already committed).
    last_loaded: Option<(u32, [u8; 32])>,
    /// Last applied [`Query::take_disconnect`] generation.
    disconnect_gen_seen: u64,
}

impl LoadAheadState {
    fn new(hub: &ChainHub) -> Self {
        let next = hub.query.tx_body_count().saturating_add(1).max(1);
        Self {
            next_tx_start: next,
            in_flight: rbitcoin_query::InFlightLog::new(),
            parent_store: std::sync::Arc::new(rbitcoin_query::PipelineParentStore::new()),
            published: std::sync::Arc::clone(hub.query.published_ids()),
            last_loaded: None,
            disconnect_gen_seen: 0,
        }
    }

    /// Drop reorged layers **before** bind so disconnected creates cannot stamp.
    fn apply_disconnect(&mut self, hub: &ChainHub) {
        if let Some(h) = hub.query.take_disconnect(&mut self.disconnect_gen_seen) {
            self.in_flight.drop_from_height(h);
            self.published.unpublish();
            self.publish_mem_stats();
        }
    }

    /// Drop packs TipOnly can see: drain inserted **and** fence covers.
    ///
    /// Call **after** pin + scripts handoff so n−1 still has CreatePin outs
    /// for load (stamp skips body_range when `get_out`). Either signal alone
    /// keeps the layer.
    ///
    /// `next_tx_start` still tracks body count (next free create fk).
    fn prune_committed(&mut self, hub: &ChainHub) {
        let body_n = hub.query.tx_body_count();
        self.in_flight.prune_if_head_ready(
            &hub.query.store().height_fence_snapshot(),
            hub.query.head_drain_fk(),
        );
        self.next_tx_start = self.next_tx_start.max(body_n.saturating_add(1).max(1));
        if let Some((h, _)) = self.last_loaded {
            let tip = hub.tip_height().unwrap_or(0);
            if h <= tip {
                self.last_loaded = None;
            }
        }
        self.publish_mem_stats();
    }

    /// Publish InFlight + PipelineParentStore occupancy for `ibd: sizes`.
    fn publish_mem_stats(&self) {
        let (layers, pins, if_bytes) = self.in_flight.size_snapshot();
        let (weak, live, ps_bytes) = self.parent_store.size_snapshot();
        if weak > live.saturating_mul(2) && weak > 4096 {
            self.parent_store.gc_dead_weaks();
            let (weak, live, ps_bytes) = self.parent_store.size_snapshot();
            rbitcoin_query::process_mem_stats::note(layers, pins, if_bytes, weak, live, ps_bytes);
            return;
        }
        rbitcoin_query::process_mem_stats::note(layers, pins, if_bytes, weak, live, ps_bytes);
    }

    fn pipeline_for(&self, path_lo: u32, store_path_lo: u32) -> WireLoadPipeline {
        let parent_hash = if path_lo == store_path_lo {
            None
        } else {
            self.last_loaded
                .filter(|(h, _)| *h + 1 == path_lo)
                .map(|(_, hash)| hash)
        };
        WireLoadPipeline {
            path_lo,
            parent_hash,
            next_tx_start: self.next_tx_start,
            in_flight: self.in_flight.snapshot(),
            parent_store: std::sync::Arc::clone(&self.parent_store),
            published: std::sync::Arc::clone(&self.published),
        }
    }

    fn note_lookup_ok(
        &mut self,
        _hub: &ChainHub,
        plan: &rbitcoin_query::ArchiveWritePlan,
        last_height: u32,
        last_hash: [u8; 32],
    ) {
        let layer = if plan.batch_pin.len() == plan.planned_fks.len() {
            rbitcoin_query::InFlightLayer::from_plan_pins(
                plan.planned_fks
                    .iter()
                    .zip(plan.batch_pin.iter())
                    .map(|(fk, pin)| (*fk, pin)),
            )
        } else {
            rbitcoin_query::InFlightLayer::from_plan_pins(
                plan.packed
                    .iter()
                    .zip(plan.planned_fks.iter())
                    .map(|((pin, _), fk)| (*fk, pin)),
            )
        };
        self.in_flight
            .note_layer(layer.with_max_height(last_height));
        if let Some(last) = plan.planned_fks.last().and_then(|f| f.get()) {
            self.next_tx_start = last.saturating_add(1).max(1);
        }
        self.last_loaded = Some((last_height, last_hash));
        self.publish_mem_stats();
    }

    /// Publish already-archived create txid→fk for tip-ahead stamp (plan=None packs).
    ///
    /// Without this, lookup(N+k) cannot resolve parents in N..N+k-1 that already
    /// have Class A body but are mid-head-insert / not yet head-probeable, and
    /// stamp fails with `parent create_fk unresolved` (permanent tip blacklist).
    fn note_archived_creates(&mut self, hub: &ChainHub, heights_hashes: &[(u32, BlockHash)]) {
        let mut pairs: Vec<([u8; 32], rbitcoin_primitives::Fk)> = Vec::new();
        let mut max_fk = 0u64;
        for &(h, hash) in heights_hashes {
            let Ok(Some((hfk, _))) = hub.query.get_header_by_hash(&hash.to_byte_array()) else {
                continue;
            };
            let Ok(Some(fks)) = hub.query.store().header_txs.get_list(hfk) else {
                continue;
            };
            for fk in fks {
                let Some(id) = fk.get() else { continue };
                max_fk = max_fk.max(id);
                let Ok(tid) = hub.query.store().txs.body_txid(fk) else {
                    continue;
                };
                if tid != [0u8; 32] {
                    pairs.push((tid, fk));
                }
            }
            let _ = h;
        }
        if pairs.is_empty() {
            return;
        }
        if let Some((_, last_id)) = pairs
            .iter()
            .filter_map(|(_, f)| f.get().map(|id| ((), id)))
            .max_by_key(|(_, id)| *id)
        {
            self.next_tx_start = last_id.saturating_add(1).max(1);
        }
        let _ = max_fk;
        let max_height = heights_hashes.iter().map(|(h, _)| *h).max();
        let mut layer = rbitcoin_query::InFlightLayer::from_txid_fks(pairs);
        if let Some(h) = max_height {
            layer = layer.with_max_height(h);
        }
        self.in_flight.note_layer(layer);
        if let Some(&(h, hash)) = heights_hashes.last() {
            self.last_loaded = Some((h, hash.to_byte_array()));
        }
        self.publish_mem_stats();
    }

    fn clear_all(&mut self, hub: &ChainHub) {
        self.in_flight.clear();
        self.parent_store = std::sync::Arc::new(rbitcoin_query::PipelineParentStore::new());
        self.publish_mem_stats();
        self.last_loaded = None;
        self.next_tx_start = hub.query.tx_body_count().saturating_add(1).max(1);
    }
}

/// Shared feed of tip-extension **readiness** for the dedicated confirm engine.
///
/// **Sole intake:** peer/rehydrate enqueues wire into the **body queue**, then
/// notes height/hash here. Lookup/load reload wire from the body queue — the feed
/// does **not** retain `Block`s. Class A alone is never enough (no hash-only
/// confirm). Tip-follow reorgs use peer wire via `ChainHub::accept_block`.
///
/// Optional wire slots remain for rare in-process requeue; production requeues
/// strip wire so RAM stays in the body queue + pipeline stage batches only.
///
/// **In-flight tracking:** once lookup claims a contiguous run, those heights sit
/// in `inflight` until write finishes (or re-queue). `note` will not re-insert
/// them — otherwise offer re-notes tip+1 every main-loop tick and lookup
/// re-claims the same batch (duplicate work).
pub(crate) struct ConfirmFeed {
    pub(crate) inner: std::sync::Mutex<ConfirmFeedInner>,
    pub(crate) cv: std::sync::Condvar,
    stop: AtomicBool,
}

pub(crate) struct ConfirmFeedInner {
    /// height → (hash, optional wire — normally `None`; body queue holds payloads)
    pub(crate) ready: std::collections::BTreeMap<u32, (BlockHash, Option<bitcoin::Block>)>,
    /// Claimed by load; not yet written or released. Offer must not re-note.
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

    pub(crate) fn notify(&self) {
        self.cv.notify_all();
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
    Accepted { hash: BlockHash },
    /// Height is the attempted confirm height (for operator logs).
    Reject {
        height: u32,
        hash: BlockHash,
        err: String,
    },
    /// Confirm saw tip+1 without durable Class A — clear optimistic `known` and
    /// drop the feed entry so offer re-probes the store (no permanent blacklist).
    BodyMissing { hash: BlockHash },
}

/// Hard cap on consecutive ready heights in one confirm wave.
///
/// Primary bound is soft **input** budget ([`confirm_batch_max_inputs`]); this
/// caps how many thin early-chain blocks pack into one lookup/load/script wave.
pub(crate) const CONFIRM_RUN_MAX_BLOCKS: usize = 144;

/// Default soft max Σ `tx.input` over a packed confirm run.
pub(crate) const CONFIRM_BATCH_INPUTS_DEFAULT: u32 = 8000;

/// How far ahead of tip to pre-note ready bodies into the feed.
/// ≥ [`CONFIRM_RUN_MAX_BLOCKS`] so the engine can fill a full hard-cap wave.
const OFFER_AHEAD: u32 = 192;

/// Soft max inputs per confirm batch (hardcoded production default).
#[inline]
pub(crate) fn confirm_batch_max_inputs() -> u32 {
    CONFIRM_BATCH_INPUTS_DEFAULT
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
pub(crate) fn pack_stop_after(
    sum_inputs: u32,
    n_blocks: usize,
    soft_max_inputs: u32,
    hard_max_blocks: usize,
) -> bool {
    n_blocks >= hard_max_blocks || sum_inputs > soft_max_inputs
}

/// Default load→scripts depth (`scriptq`): script is the long pole; modest buffer.
pub(crate) const SCRIPT_QUEUE_CAP_DEFAULT: usize = 4;
/// Default scripts→write depth: write is bursty (class_a head / tip flush); buffer
/// script output so script thr does not stall on a full writeq.
pub(crate) const WRITE_QUEUE_CAP_DEFAULT: usize = 20;

/// Resolved script / write queue capacities. Load claims BQ; no lookup→load channel.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ConfirmQueueCaps {
    /// load→scripts (`scriptq`)
    pub script: usize,
    /// scripts→write (`writeq`)
    pub write: usize,
}

/// Per-stage confirm pipeline queue capacities (hardcoded production defaults).
///
/// | Queue | Default |
/// |-------|---------|
/// | load→scripts (`scriptq`) | **4** |
/// | scripts→write (`writeq`) | **20** |
///
/// Env overrides removed (Q-04): change defaults in code if needed.
#[inline]
pub(crate) fn confirm_queue_caps() -> ConfirmQueueCaps {
    ConfirmQueueCaps {
        script: SCRIPT_QUEUE_CAP_DEFAULT,
        write: WRITE_QUEUE_CAP_DEFAULT,
    }
}

/// Load→scripts (`scriptq`) capacity.
pub(crate) fn script_queue_cap() -> usize {
    confirm_queue_caps().script
}
/// Scripts→write (`writeq`) capacity.
pub(crate) fn write_queue_cap() -> usize {
    confirm_queue_caps().write
}

/// Max heights claimable ahead of tip+1 (pipeline depth).
///
/// Lookup may start the next run while load/scripts/write hold earlier ones,
/// but must **not** skip a stuck tip+1 and claim thousands of far heights.
/// Depth units = sum of stage caps (write is usually largest).
fn max_claim_ahead() -> u32 {
    let c = confirm_queue_caps();
    let q = c.script.saturating_add(c.write);
    (q.saturating_mul(3).saturating_add(1) as u32).saturating_mul(CONFIRM_RUN_MAX_BLOCKS as u32)
}

/// BQ heights ≥ `path_lo` with resolve-complete and not load-inflight.
pub(crate) fn confirm_ready_count(
    query: &rbitcoin_query::Query,
    path_lo: u32,
    inflight: &std::collections::HashSet<u32>,
) -> usize {
    query
        .block_queue_list_meta()
        .into_iter()
        .filter(|m| m.height >= path_lo && !inflight.contains(&m.height) && m.resolve_complete)
        .count()
}

/// Live depths **and contents** of the bounded confirm pipeline queues.
///
/// Updated on successful send/recv so the status loop can log pressure and
/// process-owned retain without peeking into the OS channels.
///
/// High-water marks (`*_hwm`) track max depth since the last
/// [`ConfirmQueueDepths::sample_hwm_and_reset`] (≈5s status tick). Point
/// samples alone almost always show 0 under a lookup-limited pipeline.
#[derive(Debug, Default)]
pub(crate) struct ConfirmQueueDepths {
    /// load → scripts (`scriptq`; capacity [`script_queue_cap`]).
    load_to_scripts: AtomicUsize,
    /// scripts → write (`writeq`; capacity [`write_queue_cap`]).
    scripts_to_write: AtomicUsize,
    /// Max load→scripts depth since last HWM sample.
    script_hwm: AtomicUsize,
    /// Max scripts→write depth since last HWM sample.
    write_hwm: AtomicUsize,
    /// Sum of `batch.len()` sitting in load→scripts.
    script_blocks: AtomicUsize,
    /// Sum of approx wire bytes of load→scripts batches.
    script_wire_bytes: AtomicUsize,
    /// Sum of `BatchParents` entries riding load→scripts batches.
    script_parents: AtomicUsize,
    /// Sum of `batch.len()` sitting in scripts→write.
    write_blocks: AtomicUsize,
    write_wire_bytes: AtomicUsize,
    /// Sum of `BatchParents` entries in scripts→write (entry count, not unique Arc).
    write_parents: AtomicUsize,
}

/// Snapshot of confirm pipeline retain (queue depths + batch contents + feed).
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct ConfirmPipelineSizes {
    /// BQ resolve-complete heights load can claim (not a queue).
    pub ready: usize,
    pub script_batches: usize,
    pub script_blocks: usize,
    pub script_wire_bytes: usize,
    pub script_parents: usize,
    pub write_batches: usize,
    pub write_blocks: usize,
    pub write_wire_bytes: usize,
    pub write_parents: usize,
    pub feed_ready: usize,
    pub feed_inflight: usize,
}

impl ConfirmPipelineSizes {
    /// Parent entries sitting in scriptq + writeq (pipeline-wide, no budget).
    #[inline]
    pub fn parents_total(&self) -> usize {
        self.script_parents.saturating_add(self.write_parents)
    }
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

/// Confirm pipeline: `ready=` (BQ resolve-complete) + real `scriptq` / `writeq`.
///
/// Depth 0 uses `name<0/cap` (consumer waiting on empty queue).
#[inline]
pub(crate) fn format_conf_q(
    ready: usize,
    script: usize,
    write: usize,
    script_cap: usize,
    write_cap: usize,
) -> String {
    format!(
        "ready={} {} {}",
        ready,
        format_queue_depth("scriptq", script, script_cap),
        format_queue_depth("writeq", write, write_cap),
    )
}

impl ConfirmQueueDepths {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// `(load→scripts, scripts→write)`.
    pub(crate) fn snap(&self) -> (usize, usize) {
        (
            self.load_to_scripts.load(Ordering::Relaxed),
            self.scripts_to_write.load(Ordering::Relaxed),
        )
    }

    /// Max queue depths since last call; resets HWMs to 0.
    pub(crate) fn sample_hwm_and_reset(&self) -> (usize, usize) {
        (
            self.script_hwm.swap(0, Ordering::Relaxed),
            self.write_hwm.swap(0, Ordering::Relaxed),
        )
    }

    /// Full content snapshot (depths + blocks/wire/parents in each queue).
    pub(crate) fn content_snap(&self) -> ConfirmPipelineSizes {
        ConfirmPipelineSizes {
            ready: 0,
            script_batches: self.load_to_scripts.load(Ordering::Relaxed),
            script_blocks: self.script_blocks.load(Ordering::Relaxed),
            script_wire_bytes: self.script_wire_bytes.load(Ordering::Relaxed),
            script_parents: self.script_parents.load(Ordering::Relaxed),
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
        let _ = hwm.fetch_max(depth_after, Ordering::Relaxed);
    }

    /// Batch depth: saturating so a double-recv under teardown cannot wrap to
    /// usize::MAX and panic debug overflow on the next send (`fetch_add + 1`).
    #[inline]
    fn note_batch_depth_send(depth: &AtomicUsize, hwm: &AtomicUsize) {
        let prev = depth
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| {
                Some(n.saturating_add(1))
            })
            .unwrap_or(0);
        Self::note_depth_hwm(hwm, prev.saturating_add(1));
    }

    #[inline]
    fn note_batch_depth_recv(depth: &AtomicUsize) {
        let _ = depth.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| {
            Some(n.saturating_sub(1))
        });
    }

    fn note_script_send(&self, blocks: usize, wire_bytes: usize, parents: usize) {
        Self::note_batch_depth_send(&self.load_to_scripts, &self.script_hwm);
        // Saturating: concurrent note_script_send under parallel load can race past
        // usize::MAX on wire_bytes/parents counters in debug overflow checks.
        self.script_blocks
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| {
                Some(n.saturating_add(blocks))
            })
            .ok();
        self.script_wire_bytes
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| {
                Some(n.saturating_add(wire_bytes))
            })
            .ok();
        self.script_parents
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| {
                Some(n.saturating_add(parents))
            })
            .ok();
    }
    fn note_script_recv(&self, blocks: usize, wire_bytes: usize, parents: usize) {
        Self::note_batch_depth_recv(&self.load_to_scripts);
        self.script_blocks
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| {
                Some(n.saturating_sub(blocks))
            })
            .ok();
        self.script_wire_bytes
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| {
                Some(n.saturating_sub(wire_bytes))
            })
            .ok();
        self.script_parents
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| {
                Some(n.saturating_sub(parents))
            })
            .ok();
    }

    fn note_write_send(&self, blocks: usize, wire_bytes: usize, parents: usize) {
        Self::note_batch_depth_send(&self.scripts_to_write, &self.write_hwm);
        self.write_blocks
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| {
                Some(n.saturating_add(blocks))
            })
            .ok();
        self.write_wire_bytes
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| {
                Some(n.saturating_add(wire_bytes))
            })
            .ok();
        self.write_parents
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| {
                Some(n.saturating_add(parents))
            })
            .ok();
    }
    fn note_write_recv(&self, blocks: usize, wire_bytes: usize, parents: usize) {
        Self::note_batch_depth_recv(&self.scripts_to_write);
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

/// Operator line for load stamp reject. Stamp-stage `missing prevout` is the
/// leftover TipOnly miss remapped from `parent create_fk unresolved` — name
/// that so a race is not logged as a bare invalid-block.
pub(crate) fn stamp_reject_operator_msg(err: &str) -> String {
    if err == "missing prevout" {
        let last = rbitcoin_query::archive_phase_stats::last_plan_batch();
        let miss = rbitcoin_query::archive_phase_stats::last_union_miss();
        let mut s = format!(
            "{err} (leftover parent create_fk unresolved leftover_n={} leftover_hit={}",
            last.head_need, last.head_hit
        );
        if miss.n > 0 {
            s.push_str(&format!(" miss_n={}", miss.n));
            if let Some(raw) = miss.txid {
                s.push_str(&format!(
                    " miss_txid={}",
                    bitcoin::Txid::from_byte_array(raw)
                ));
            }
            s.push_str(&format!(" pending={}", u8::from(miss.pending)));
            if let Some(on) = miss.miss_on {
                s.push_str(&format!(" miss_on={} miss_cands={}", on, miss.miss_cands));
            }
            if rbitcoin_store::leftover_probe_diag_ready() {
                s.push_str(" diag=1");
            }
        }
        s.push(')');
        s
    } else {
        err.to_string()
    }
}

/// Drain scripts→write after `first` is already dequeued (and accounted):
/// non-blocking `try_recv` until empty, merge contiguous into one batch.
///
/// `on_extra` runs only for additionally drained parts (queue depth). Non-contig
/// leftover is returned for the next write iteration (ordered scripts should
/// never hit that path).
fn drain_script_ok_write_queue(
    first: rbitcoin_consensus::ScriptOkBatch,
    rx: &std::sync::mpsc::Receiver<rbitcoin_consensus::ScriptOkBatch>,
    mut on_extra: impl FnMut(&rbitcoin_consensus::ScriptOkBatch),
) -> (
    rbitcoin_consensus::ScriptOkBatch,
    usize,
    Option<rbitcoin_consensus::ScriptOkBatch>,
) {
    let mut batch = first;
    let mut parts = 1usize;
    loop {
        match rx.try_recv() {
            Ok(more) => {
                on_extra(&more);
                match batch.append_contiguous(more) {
                    Ok(()) => {
                        parts = parts.saturating_add(1);
                    }
                    Err(leftover) => {
                        // Height gap / leftover union — write the contiguous prefix first.
                        warn!(
                            "ibd: write batch drain gap after parts={parts} leftover_blks={}",
                            leftover.len()
                        );
                        return (batch, parts, Some(leftover));
                    }
                }
            }
            Err(std::sync::mpsc::TryRecvError::Empty) => break,
            Err(std::sync::mpsc::TryRecvError::Disconnected) => break,
        }
    }
    (batch, parts, None)
}

/// OS-thread occupancy for the confirm pipeline (lookup / load / scripts / write).
///
/// Stage `plan_ms` / `script_ms` / … are **work** sums and mis-rank the long
/// pole when scriptq is empty. These timers include **wait** (claim, recv, send
/// block) so a 5s window can show who is busy vs idle.
pub(crate) mod confirm_thr_stats {
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::Duration;

    static LOOKUP_CLAIM_NS: AtomicU64 = AtomicU64::new(0);
    static LOOKUP_CLONE_NS: AtomicU64 = AtomicU64::new(0);
    static LOOKUP_STAMP_NS: AtomicU64 = AtomicU64::new(0);
    static LOOKUP_OTHER_NS: AtomicU64 = AtomicU64::new(0);
    static LOOKUP_SEND_WAIT_NS: AtomicU64 = AtomicU64::new(0);

    static LOAD_RECV_WAIT_NS: AtomicU64 = AtomicU64::new(0);
    static LOAD_WORK_NS: AtomicU64 = AtomicU64::new(0);
    static LOAD_SEND_WAIT_NS: AtomicU64 = AtomicU64::new(0);

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
    pub fn add_lookup_claim(d: Duration) {
        add(&LOOKUP_CLAIM_NS, d);
    }
    #[inline]
    pub fn add_lookup_clone(d: Duration) {
        add(&LOOKUP_CLONE_NS, d);
    }
    #[inline]
    pub fn add_lookup_stamp(d: Duration) {
        add(&LOOKUP_STAMP_NS, d);
    }
    #[inline]
    pub fn add_lookup_other(d: Duration) {
        add(&LOOKUP_OTHER_NS, d);
    }
    #[inline]
    pub fn add_lookup_send_wait(d: Duration) {
        add(&LOOKUP_SEND_WAIT_NS, d);
    }

    #[inline]
    pub fn add_load_recv_wait(d: Duration) {
        add(&LOAD_RECV_WAIT_NS, d);
    }
    #[inline]
    pub fn add_load_work(d: Duration) {
        add(&LOAD_WORK_NS, d);
    }
    #[inline]
    pub fn add_load_send_wait(d: Duration) {
        add(&LOAD_SEND_WAIT_NS, d);
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
        pub lookup_claim_ns: u64,
        pub lookup_clone_ns: u64,
        pub lookup_stamp_ns: u64,
        pub lookup_other_ns: u64,
        pub lookup_send_wait_ns: u64,
        pub load_recv_wait_ns: u64,
        pub load_work_ns: u64,
        pub load_send_wait_ns: u64,
        pub script_recv_wait_ns: u64,
        pub script_work_ns: u64,
        pub script_send_wait_ns: u64,
        pub write_recv_wait_ns: u64,
        pub write_work_ns: u64,
    }

    pub fn sample_and_reset() -> Sample {
        Sample {
            lookup_claim_ns: LOOKUP_CLAIM_NS.swap(0, Ordering::Relaxed),
            lookup_clone_ns: LOOKUP_CLONE_NS.swap(0, Ordering::Relaxed),
            lookup_stamp_ns: LOOKUP_STAMP_NS.swap(0, Ordering::Relaxed),
            lookup_other_ns: LOOKUP_OTHER_NS.swap(0, Ordering::Relaxed),
            lookup_send_wait_ns: LOOKUP_SEND_WAIT_NS.swap(0, Ordering::Relaxed),
            load_recv_wait_ns: LOAD_RECV_WAIT_NS.swap(0, Ordering::Relaxed),
            load_work_ns: LOAD_WORK_NS.swap(0, Ordering::Relaxed),
            load_send_wait_ns: LOAD_SEND_WAIT_NS.swap(0, Ordering::Relaxed),
            script_recv_wait_ns: SCRIPT_RECV_WAIT_NS.swap(0, Ordering::Relaxed),
            script_work_ns: SCRIPT_WORK_NS.swap(0, Ordering::Relaxed),
            script_send_wait_ns: SCRIPT_SEND_WAIT_NS.swap(0, Ordering::Relaxed),
            write_recv_wait_ns: WRITE_RECV_WAIT_NS.swap(0, Ordering::Relaxed),
            write_work_ns: WRITE_WORK_NS.swap(0, Ordering::Relaxed),
        }
    }
}

/// Spawn confirm **lookup** + **load** + **scripts** + **write** OS threads.
///
/// Lookup (BQ-ahead TipOnly `head_fk`) ∥ load (claim resolve-complete + stamp
/// from in-flight + published union + TipOnly `tx.head` + pin + assemble) → scriptq →
/// scripts → writeq → write.
/// Returns the lookup-thread join handle and shared queue-depth counters.
pub(crate) fn spawn_confirm_engine(
    hub: Arc<ChainHub>,
    feed: Arc<ConfirmFeed>,
    event_tx: std::sync::mpsc::Sender<ConfirmEvent>,
    accepted: Arc<AtomicU32>,
    loop_stats: Arc<LoopStats>,
) -> (std::thread::JoinHandle<()>, Arc<ConfirmQueueDepths>) {
    let queues = ConfirmQueueDepths::new();
    let caps = confirm_queue_caps();
    type ScriptsIn = (rbitcoin_consensus::LoadedBatch, u64);
    let (mat_tx, mat_rx) = std::sync::mpsc::sync_channel::<ScriptsIn>(caps.script);
    let (write_tx, write_rx) =
        std::sync::mpsc::sync_channel::<rbitcoin_consensus::ScriptOkBatch>(caps.write);
    // Write reject: plan drops reserved fks + last_loaded so re-lookup after
    // Class A partial commit does not drift next_tx_start.
    let load_ahead_reset = Arc::new(AtomicBool::new(false));

    let hub_wb = Arc::clone(&hub);
    let feed_wb = Arc::clone(&feed);
    let event_tx_wb = event_tx.clone();
    let accepted_wb = Arc::clone(&accepted);
    let loop_stats_wb = Arc::clone(&loop_stats);
    let q_wb = Arc::clone(&queues);
    let load_ahead_reset_wb = Arc::clone(&load_ahead_reset);
    let write_thr = std::thread::Builder::new()
        .name("ibd-confirm-write".into())
        .spawn(move || {
            info!("ibd: confirm write on dedicated OS thread");
            // Non-contig leftover already note_write_recv'd; write it next iter.
            let mut leftover: Option<rbitcoin_consensus::ScriptOkBatch> = None;
            loop {
                let t_recv = Instant::now();
                let first = match leftover.take() {
                    Some(b) => b,
                    None => match write_rx.recv() {
                        Ok(b) => {
                            let n = b.len();
                            let wire = b.approx_wire_bytes();
                            let parents = b.parent_count();
                            q_wb.note_write_recv(n, wire, parents);
                            b
                        }
                        Err(_) => break,
                    },
                };
                let (batch, parts, next_left) =
                    drain_script_ok_write_queue(first, &write_rx, |b| {
                        let n = b.len();
                        let wire = b.approx_wire_bytes();
                        let parents = b.parent_count();
                        q_wb.note_write_recv(n, wire, parents);
                    });
                leftover = next_left;
                confirm_thr_stats::add_write_recv_wait(t_recv.elapsed());
                if feed_wb.stopped() || hub_wb.query.confirm_cancelled() {
                    break;
                }
                let n = batch.len();
                let first_h = batch.heights_hashes().first().map(|(h, _)| *h).unwrap_or(0);
                let t0 = Instant::now();
                let heights_hashes = batch.heights_hashes();
                match hub_wb.confirm_write(batch) {
                    Ok(_outcomes) => {
                        for (height, raw) in &heights_hashes {
                            let hash = BlockHash::from_byte_array(*raw);
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
                                "ibd: confirm write slow batch={n} parts={parts} first={first_h} wall={:?} \
                                 class_a={}ms ensure={}ms struct={}ms spent={}ms create_h={}ms \
                                 bip68={}ms class_c={}ms spend_ann={}ms tip_gc={}ms tweaks={}ms",
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
                                ms(p.tweak_ns),
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
                        // Reset load-ahead so reserved create fks / last_loaded do not drift.
                        load_ahead_reset_wb.store(true, Ordering::Release);
                        feed_wb.finish(heights_hashes.iter().map(|(h, _)| *h));
                        loop_stats_wb
                            .confirm_reject_stops
                            .fetch_add(1, Ordering::Relaxed);
                        warn!(
                            "ibd: confirm write reject @ {height} batch_parts={parts}: {e}"
                        );
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

    // Feed-ahead: submit N+1 while joining N (blocking join left the pool idle).
    let hub_sc = Arc::clone(&hub);
    let feed_sc = Arc::clone(&feed);
    let event_tx_sc = event_tx.clone();
    let loop_stats_sc = Arc::clone(&loop_stats);
    let q_sc = Arc::clone(&queues);
    let scripts = std::thread::Builder::new()
        .name("ibd-confirm".into())
        .spawn(move || {
            info!(
                "ibd: confirm scripts on dedicated OS thread (pure CPU; coordinator feed-ahead)"
            );
            /// One scripts wave started on a coordinator (not yet joined / written).
            struct Inflight {
                handle: rbitcoin_consensus::ScriptsPhaseHandle,
                meta: rbitcoin_consensus::ScriptsBatchMeta,
            }
            fn start_inflight(
                mat_batch: rbitcoin_consensus::LoadedBatch,
                mat_ns: u64,
                q_sc: &ConfirmQueueDepths,
            ) -> Inflight {
                let n = mat_batch.len();
                let load_wire = mat_batch.approx_wire_bytes();
                let script_parents = mat_batch.parent_count();
                q_sc.note_script_recv(n, load_wire, script_parents);
                let meta =
                    rbitcoin_consensus::ScriptsBatchMeta::from_batch(&mat_batch, mat_ns);
                let handle = rbitcoin_consensus::confirm_scripts_phase_async(mat_batch);
                Inflight { handle, meta }
            }

            let mut current: Option<Inflight> = None;
            let mut lookahead: Option<Inflight> = None;
            loop {
                if feed_sc.stopped() || hub_sc.query.confirm_cancelled() {
                    break;
                }
                if current.is_none() {
                    let t_recv = Instant::now();
                    let (mat_batch, mat_ns) = match mat_rx.recv() {
                        Ok(x) => x,
                        Err(_) => break,
                    };
                    confirm_thr_stats::add_script_recv_wait(t_recv.elapsed());
                    if feed_sc.stopped() || hub_sc.query.confirm_cancelled() {
                        break;
                    }
                    current = Some(start_inflight(mat_batch, mat_ns, q_sc.as_ref()));
                }
                let inflight = match current.take() {
                    Some(i) => i,
                    None => break,
                };
                let result = rbitcoin_consensus::join_scripts_polling(
                    &inflight.handle,
                    std::time::Duration::from_micros(200),
                    || {
                        if lookahead.is_none()
                            && !feed_sc.stopped()
                            && !hub_sc.query.confirm_cancelled()
                        {
                            let t_try = Instant::now();
                            match mat_rx.try_recv() {
                                Ok((mat_batch, mat_ns)) => {
                                    confirm_thr_stats::add_script_recv_wait(t_try.elapsed());
                                    lookahead =
                                        Some(start_inflight(mat_batch, mat_ns, q_sc.as_ref()));
                                }
                                Err(std::sync::mpsc::TryRecvError::Empty) => {}
                                Err(std::sync::mpsc::TryRecvError::Disconnected) => {}
                            }
                        }
                    },
                );
                match result {
                    Ok(outcome) => {
                        loop_stats_sc
                            .confirm_ns
                            .fetch_add(outcome.work_ns, Ordering::Relaxed);
                        confirm_thr_stats::add_script_work(inflight.meta.t0.elapsed());
                        let script_ms = outcome.work_ns / 1_000_000;
                        let mat_ms = inflight.meta.mat_ns / 1_000_000;
                        let wb = outcome.batch.len();
                        let ww = outcome.batch.approx_wire_bytes();
                        let parents = outcome.batch.parent_count();
                        let t_send = Instant::now();
                        if write_tx.send(outcome.batch).is_err() {
                            info!("ibd: confirm write channel closed");
                            if let Some(la) = lookahead.take() {
                                let _ = la.handle.join();
                                feed_sc.finish(la.meta.heights_hashes.iter().map(|(h, _)| *h));
                            }
                            break;
                        }
                        confirm_thr_stats::add_script_send_wait(t_send.elapsed());
                        q_sc.note_write_send(wb, ww, parents);
                        if script_ms > 2_000 || mat_ms > 2_000 {
                            info!(
                                "ibd: confirm scripts slow batch={} first={} load_ms={mat_ms} script_ms={script_ms} wall_ms={}",
                                inflight.meta.n,
                                inflight.meta.first_h,
                                inflight.meta.t0.elapsed().as_millis()
                            );
                        }
                        current = lookahead.take();
                    }
                    Err(e) => {
                        confirm_thr_stats::add_script_work(inflight.meta.t0.elapsed());
                        let msg = e.to_string();
                        if msg.contains("confirm cancelled") || feed_sc.stopped() {
                            info!("ibd: confirm scripts aborted: {msg}");
                            if let Some(la) = lookahead.take() {
                                let _ = la.handle.join();
                                feed_sc.finish(la.meta.heights_hashes.iter().map(|(h, _)| *h));
                            }
                            break;
                        }
                        let (height, hash) = inflight
                            .meta
                            .heights_hashes
                            .first()
                            .map(|(h, raw)| (*h, BlockHash::from_byte_array(*raw)))
                            .unwrap_or((
                                inflight.meta.first_h,
                                BlockHash::from_byte_array([0u8; 32]),
                            ));
                        feed_sc.finish(inflight.meta.heights_hashes.iter().map(|(h, _)| *h));
                        if let Some(la) = lookahead.take() {
                            let _ = la.handle.join();
                            feed_sc.finish(la.meta.heights_hashes.iter().map(|(h, _)| *h));
                        }
                        loop_stats_sc
                            .confirm_reject_stops
                            .fetch_add(1, Ordering::Relaxed);
                        warn!(
                            "ibd: confirm scripts reject @ {height} (batch first {hash}): {e}"
                        );
                        let _ = event_tx_sc.send(ConfirmEvent::Reject {
                            height,
                            hash,
                            err: msg,
                        });
                        current = None;
                    }
                }
            }
            if let Some(i) = current.take() {
                let _ = i.handle.join();
                feed_sc.finish(i.meta.heights_hashes.iter().map(|(h, _)| *h));
            }
            if let Some(la) = lookahead.take() {
                let _ = la.handle.join();
                feed_sc.finish(la.meta.heights_hashes.iter().map(|(h, _)| *h));
            }
            drop(write_tx);
            let _ = write_thr.join();
            info!("ibd: confirm scripts exit");
        })
        .expect("spawn ibd-confirm");

    let hub_load = Arc::clone(&hub);
    let feed_load = Arc::clone(&feed);
    let event_tx_load = event_tx.clone();
    let loop_stats_load = Arc::clone(&loop_stats);
    let queues_load = Arc::clone(&queues);
    let load_ahead_reset_load = Arc::clone(&load_ahead_reset);
    let load_join = std::thread::Builder::new()
        .name("ibd-confirm-load".into())
        .spawn(move || {
            info!(
                "ibd: confirm load on dedicated OS thread (claim resolve-complete → stamp+pin)"
            );
            let mut lookup_ahead = LoadAheadState::new(&hub_load);
            loop {
                if feed_load.stopped() || hub_load.query.confirm_cancelled() {
                    break;
                }
                let t_claim = Instant::now();
                if load_ahead_reset_load.swap(false, Ordering::AcqRel) {
                    lookup_ahead.clear_all(&hub_load);
                }
                lookup_ahead.apply_disconnect(&hub_load);
                let batch: (Vec<(u32, BlockHash, bitcoin::Block)>, u32) = {
                    let mut g = feed_load.inner.lock().unwrap();
                    let found: Option<(Vec<(u32, BlockHash, bitcoin::Block)>, u32)> = loop {
                        if feed_load.stopped() {
                            drop(g);
                            drop(mat_tx);
                            let _ = scripts.join();
                            return;
                        }
                        let tip = hub_load.tip_height();
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
                            drop(g);
                            let mut run: Vec<(u32, BlockHash, bitcoin::Block)> =
                                Vec::with_capacity(hard_blocks.min(32));
                            let mut sum_inputs = 0u32;
                            let mut h = expect;
                            let mut body_missing: Vec<(u32, BlockHash)> = Vec::new();
                            let t_pack_io = Instant::now();
                            while run.len() < hard_blocks && h <= claim_hi {
                                let (hash, opt_wire) = {
                                    let mut gg = feed_load.inner.lock().unwrap();
                                    if gg.inflight.contains(&h) {
                                        break;
                                    }
                                    let Some(entry) = gg.ready.get(&h).cloned() else {
                                        break;
                                    };
                                    if entry.1.is_none()
                                        && !hub_load.query.block_queue_is_resolve_complete(h)
                                    {
                                        break;
                                    }
                                    gg.ready.remove(&h);
                                    entry
                                };
                                if hub_load.has_block(&hash) {
                                    h = h.saturating_add(1);
                                    continue;
                                }
                                let block = if let Some(b) = opt_wire {
                                    b
                                } else {
                                    match load_decode_bq_block(&hub_load, h, &hash) {
                                        Ok(b) => b,
                                        Err(PackWireErr::Missing) => {
                                            body_missing.push((h, hash));
                                            break;
                                        }
                                        Err(PackWireErr::HashMismatch | PackWireErr::Decode) => {
                                            body_missing.push((h, hash));
                                            let _ = hub_load.query.block_queue_dequeue_height(h);
                                            break;
                                        }
                                    }
                                };
                                let inputs = block_input_count(&block);
                                {
                                    let mut gg = feed_load.inner.lock().unwrap();
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
                            confirm_thr_stats::add_load_work(t_pack_io.elapsed());
                            if !body_missing.is_empty() {
                                for (mh, mhash) in &body_missing {
                                    let _ = event_tx_load.send(ConfirmEvent::BodyMissing {
                                        hash: *mhash,
                                    });
                                    feed_load.finish(std::iter::once(*mh));
                                }
                            }
                            if !run.is_empty() {
                                break Some((run, sum_inputs));
                            }
                            g = feed_load.inner.lock().unwrap();
                            continue;
                        }
                        let (gg, wait_res) = feed_load
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
                            confirm_thr_stats::add_load_recv_wait(t_claim.elapsed());
                            continue;
                        }
                    }
                };
                confirm_thr_stats::add_load_recv_wait(t_claim.elapsed());

                let (batch, _batch_inputs) = batch;
                if batch.is_empty() {
                    continue;
                }
                let expect_h = batch[0].0;
                if feed_load.stopped() || hub_load.query.confirm_cancelled() {
                    let req: Vec<(u32, BlockHash, Option<bitcoin::Block>)> = batch
                        .iter()
                        .map(|(h, ha, b)| (*h, *ha, Some(b.clone())))
                        .collect();
                    feed_load.requeue_wire(&req);
                    drop(mat_tx);
                    let _ = scripts.join();
                    return;
                }

                let store_path_lo = match hub_load.tip_height() {
                    None => 0u32,
                    Some(t) => t.saturating_add(1),
                };
                let pipe = lookup_ahead.pipeline_for(expect_h, store_path_lo);
                let use_pipe = pipe.path_lo >= store_path_lo;
                let wire_batch = batch;
                let t_clone = Instant::now();
                let plan_items: Vec<(
                    rbitcoin_primitives::Height,
                    std::sync::Arc<bitcoin::Block>,
                )> = wire_batch
                    .iter()
                    .map(|(h, _, w)| {
                        (
                            rbitcoin_primitives::Height(*h),
                            std::sync::Arc::new(w.clone()),
                        )
                    })
                    .collect();
                confirm_thr_stats::add_lookup_clone(t_clone.elapsed());
                let t_stamp = Instant::now();
                let plan_res = rbitcoin_consensus::confirm_wire_lookup_stamp_with_hits(
                    &hub_load.query,
                    &hub_load.params,
                    hub_load.milestone,
                    &plan_items,
                    if use_pipe { Some(&pipe) } else { None },
                    None,
                );
                confirm_thr_stats::add_load_work(t_stamp.elapsed());
                let stamped = match plan_res {
                    Ok(s) => s,
                    Err(e) => {
                        let msg = e.to_string();
                        if msg.contains("confirm cancelled") || feed_load.stopped() {
                            drop(mat_tx);
                            let _ = scripts.join();
                            return;
                        }
                        let first_hash = wire_batch[0].1;
                        if wire_batch.len() > 1 {
                            let tail: Vec<(u32, BlockHash, Option<bitcoin::Block>)> =
                                wire_batch
                                    .iter()
                                    .skip(1)
                                    .filter(|(_, ha, _)| !hub_load.has_block(ha))
                                    .map(|(h, ha, _)| (*h, *ha, None))
                                    .collect();
                            feed_load.requeue_wire(&tail);
                        }
                        feed_load.finish(std::iter::once(expect_h));
                        lookup_ahead.clear_all(&hub_load);
                        loop_stats_load
                            .confirm_reject_stops
                            .fetch_add(1, Ordering::Relaxed);
                        let log_msg = stamp_reject_operator_msg(&msg);
                        let (if_l, if_n, _) = lookup_ahead.in_flight.size_snapshot();
                        let drain_fk = hub_load.query.head_drain_fk();
                        let fence_h = hub_load.query.fence_tip_height();
                        warn!(
                            "ibd: confirm load stamp reject {first_hash} @ {expect_h}: {log_msg} \
                             iflight={if_l}L/{if_n} drain_fk={drain_fk} fence_h={fence_h:?}"
                        );
                        let _ = event_tx_load.send(ConfirmEvent::Reject {
                            height: expect_h,
                            hash: first_hash,
                            err: log_msg,
                        });
                        std::thread::sleep(Duration::from_millis(50));
                        continue;
                    }
                };
                if let Some(ref p) = stamped.plan {
                    if let Some((lh, raw)) = wire_batch
                        .iter()
                        .map(|(h, ha, _)| (*h, ha.to_byte_array()))
                        .max_by_key(|(h, _)| *h)
                    {
                        lookup_ahead.note_lookup_ok(&hub_load, p, lh, raw);
                    }
                } else {
                    let hh: Vec<(u32, BlockHash)> = wire_batch
                        .iter()
                        .map(|(h, ha, _)| (*h, *ha))
                        .collect();
                    lookup_ahead.note_archived_creates(&hub_load, &hh);
                }
                let pipe = lookup_ahead.pipeline_for(expect_h, store_path_lo);
                let plan_ns = stamped.work_ns;
                let heights_hashes: Vec<(u32, BlockHash)> = wire_batch
                    .iter()
                    .map(|(h, ha, _)| (*h, *ha))
                    .collect();
                let first_hash = heights_hashes[0].1;
                let _ = wire_batch;

                struct LiveGuard<'a> {
                    stats: &'a LoopStats,
                }
                impl Drop for LiveGuard<'_> {
                    fn drop(&mut self) {
                        self.stats.confirm_end();
                    }
                }
                loop_stats_load.confirm_begin(expect_h, heights_hashes.len() as u32, 0);
                let _live_guard = LiveGuard {
                    stats: &loop_stats_load,
                };

                let t_work = Instant::now();
                let mat_res = hub_load.confirm_wire_load_from_plan(stamped, Some(&pipe));
                confirm_thr_stats::add_load_work(t_work.elapsed());
                drop(_live_guard);

                if feed_load.stopped() || hub_load.query.confirm_cancelled() {
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
                        confirm_thr_stats::add_load_send_wait(t_send.elapsed());
                        queues_load.note_script_send(prepared_n, wire, parents);
                        if work_ms > 2_000 {
                            let pin = rbitcoin_query::confirm_load_stats::last_pin_phases();
                            let pms = rbitcoin_query::confirm_load_stats::LastPinPhases::ms;
                            info!(
                                "ibd: confirm load slow batch={prepared_n} claim={} first={expect_h} \
                                 work_ms={work_ms} plan_stamp_ms={} \
                                 pin(adopt={}ms plan={}ms/n={} cold={}ms/n={} contract={}ms publish={}ms) \
                                 parents={}",
                                heights_hashes.len(),
                                plan_ns / 1_000_000,
                                pms(pin.adopt_ns),
                                pms(pin.plan_pin_ns),
                                pin.pin_plan_n,
                                pms(pin.cold_ns),
                                pin.pin_new_n,
                                pms(pin.contract_ns),
                                pms(pin.publish_ns),
                                parents,
                            );
                        }
                    }
                    Err(e) => {
                        let msg = e.to_string();
                        if msg.contains("confirm cancelled") {
                            info!("ibd: confirm load cancelled @ {expect_h}");
                            drop(mat_tx);
                            let _ = scripts.join();
                            return;
                        }
                        if heights_hashes.len() > 1 {
                            let tail: Vec<(u32, BlockHash, Option<bitcoin::Block>)> =
                                heights_hashes
                                    .iter()
                                    .skip(1)
                                    .filter(|(_, ha)| !hub_load.has_block(ha))
                                    .map(|(h, ha)| (*h, *ha, None))
                                    .collect();
                            feed_load.requeue_wire(&tail);
                        }
                        feed_load.finish(std::iter::once(expect_h));
                        loop_stats_load
                            .confirm_reject_stops
                            .fetch_add(1, Ordering::Relaxed);
                        warn!("ibd: confirm load reject {first_hash} @ {expect_h}: {e}");
                        if event_tx_load
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
                // Disconnect prune: drain+fence layers are TipOnly; next claim must not see those outs.
                lookup_ahead.prune_committed(&hub_load);
            }
            drop(mat_tx);
            let _ = scripts.join();
            info!("ibd: confirm load exit");
        })
        .expect("spawn ibd-confirm-load");

    let queues_lookup = Arc::clone(&queues);
    let lookup_join = std::thread::Builder::new()
        .name("ibd-confirm-lookup".into())
        .spawn(move || {
            info!("ibd: confirm lookup on dedicated OS thread (BQ-ahead TipOnly head_fk)");
            let _ = queues_lookup;
            let mut live_union = rbitcoin_query::LiveUnion::new();
            let mut disco_seen = 0u64;
            loop {
                if feed.stopped() {
                    break;
                }
                if hub.query.take_disconnect(&mut disco_seen).is_some() {
                    live_union = rbitcoin_query::LiveUnion::new();
                    hub.query.published_ids().unpublish();
                }
                let t_sel = Instant::now();
                let skip: std::collections::HashSet<u32> = {
                    let g = feed.inner.lock().unwrap();
                    g.inflight.iter().copied().collect()
                };
                let tip = hub.tip_height();
                let path_lo = if tip.is_none() {
                    0u32
                } else {
                    tip.unwrap_or(0).saturating_add(1)
                };
                let wave_h = hub.query.block_queue_unresolved_heights(
                    path_lo,
                    &skip,
                    rbitcoin_consensus::BQ_RESOLVE_WAVE_MAX_BLOCKS,
                );
                confirm_thr_stats::add_lookup_other(t_sel.elapsed());
                let mut did = false;
                if !wave_h.is_empty() {
                    let t_wave = Instant::now();
                    match rbitcoin_consensus::confirm_bq_resolve_wave_with_ids(
                        &hub.query,
                        &hub.params,
                        &wave_h,
                        Some((
                            &mut live_union,
                            hub.query.published_ids().as_ref(),
                            hub.query.parent_id_forget().as_ref(),
                        )),
                    ) {
                        Ok(st) if st.heights > 0 => {
                            did = true;
                            feed.notify();
                        }
                        Ok(_) => {}
                        Err(e) => warn!("ibd: bq resolve wave: {e}"),
                    }
                    confirm_thr_stats::add_lookup_stamp(t_wave.elapsed());
                }
                if !did {
                    let t_wait = Instant::now();
                    let g = feed.inner.lock().unwrap();
                    if feed.stopped() {
                        break;
                    }
                    let (_gg, _) = feed.cv.wait_timeout(g, Duration::from_millis(20)).unwrap();
                    confirm_thr_stats::add_lookup_send_wait(t_wait.elapsed());
                    confirm_thr_stats::add_lookup_claim(t_wait.elapsed());
                }
            }
            feed.notify();
            let _ = load_join.join();
            info!("ibd: confirm lookup exit");
        })
        .expect("spawn ibd-confirm-lookup");
    (lookup_join, queues)
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
/// Claim-ready = body-queue wire only (not Class A alone, not zombie pending).
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
            break;
        };
        if hub.has_block(&hash) {
            continue;
        }
        if body.is_rejected(&hash) {
            // Tip is frozen on a permanently rejected tip+1 (consensus blacklisted).
            // Without this log, status shows confirm_blks=0 + hole=0 and looks like
            // a silent hot-path stall while archive runs ahead forever.
            if ht == expect {
                static REJECT_STUCK: AtomicU32 = AtomicU32::new(0);
                let n = REJECT_STUCK.fetch_add(1, Ordering::Relaxed) + 1;
                if n <= 3 || n.is_multiple_of(100) {
                    warn!(
                        "ibd: confirm stuck: tip+1={ht} {hash} is blacklisted (rejected earlier); \
                         restart with a fixed binary to clear the in-memory reject set (n={n})"
                    );
                }
            }
            break;
        }
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
mod tests;
