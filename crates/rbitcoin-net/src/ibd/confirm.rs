//! Dedicated confirm engine (Class C tip walk) for IBD.

use super::body::BodyPresence;
use super::status::LoopStats;
use crate::chain::ChainHub;
use bitcoin::hashes::Hash;
use bitcoin::BlockHash;
use rbitcoin_consensus::WirePrepPipeline;
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
    in_flight_outs: std::sync::Arc<
        HashMap<
            u64,
            (
                rbitcoin_store::TxRecord,
                Vec<rbitcoin_store::OutputRecord>,
                Vec<u32>,
            ),
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

    /// Drop creates already durable in Class A (after commits).
    fn prune_committed(&mut self, hub: &ChainHub) {
        let durable = hub.query.tx_body_count();
        let creates = std::sync::Arc::make_mut(&mut self.in_flight_creates);
        creates.retain(|_, fk| fk.get().map(|id| id > durable).unwrap_or(false));
        let outs = std::sync::Arc::make_mut(&mut self.in_flight_outs);
        outs.retain(|id, _| *id > durable);
        self.next_tx_start = self.next_tx_start.max(durable.saturating_add(1).max(1));
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
        hub: &ChainHub,
        plan: &rbitcoin_query::ArchiveWritePlan,
        last_height: u32,
        last_hash: [u8; 32],
    ) {
        let creates = std::sync::Arc::make_mut(&mut self.in_flight_creates);
        let outs = std::sync::Arc::make_mut(&mut self.in_flight_outs);
        let secret = hub.query.store().txs.store_secret();
        for ((tx, ins, o), fk) in plan.packed.iter().zip(plan.planned_fks.iter()) {
            creates.insert(tx.txid, *fk);
            if let Some(id) = fk.get() {
                // Offline denserels (same packing as Class A body) so prep(N+1)
                // pin has abs layout without commit(N) body IO.
                let denserels = offline_in_flight_denserels(secret, tx, ins, o);
                outs.insert(id, (tx.clone(), o.clone(), denserels));
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

/// Offline denserels for in-flight pin (must match Class A body packing).
fn offline_in_flight_denserels(
    secret: &rbitcoin_store::StoreSecret,
    tx: &rbitcoin_store::TxRecord,
    ins: &[rbitcoin_store::InputRecord],
    outs: &[rbitcoin_store::OutputRecord],
) -> Vec<u32> {
    let mut raw = Vec::new();
    rbitcoin_store::encode_packed_tx_with_secret(tx, ins, outs, &mut raw, Some(secret));
    rbitcoin_store::decode_packed_tx_outs_with_spender_rels_secret(&raw, Some(secret))
        .map(|(_, _, rels)| rels)
        .unwrap_or_default()
}

/// Shared feed of tip-extension **readiness** for the dedicated confirm engine.
///
/// Live path: peer/rehydrate enqueues wire into the **body queue**
/// (`store/block_queue/` + RAM pending), then notes height/hash here. Prep
/// reloads wire from the body queue — the feed does **not** retain `Block`s.
/// Optional wire slots remain for rare in-process requeue; production requeues
/// strip wire so RAM stays in the body queue + pipeline stage batches only.
///
/// Hash-only notes also cover already-archived Class A fallback (no bq payload).
///
/// **In-flight tracking:** once prep claims a contiguous run, those heights sit in
/// `inflight` until write finishes (or prep re-queues). `note` will not re-insert
/// them — otherwise offer re-notes tip+1 every main-loop tick and prep re-claims
/// the same batch into the prep→scripts queue (duplicate script work).
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

    /// Note a ready height (hash only — Class A already on disk).
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
    /// RAM overflow bodies spilled to durable disk after dequeue freed space.
    ///
    /// Soft `archive_queued` charges must be released for these hashes (payload
    /// is no longer in process RAM). Without this, densify stays gated while
    /// durable `bq` drains under budget for tens of minutes.
    RamFlushed {
        hashes: Vec<BlockHash>,
    },
}

/// How many consecutive ready heights to confirm in one multi-block script wave.
/// Larger waves keep rayon cores busy when the body queue leads tip. Fat single
/// blocks still dominate wall time; this packs thin consecutive heights.
const CONFIRM_RUN_MAX: usize = 32;

/// How far ahead of tip to pre-note ready bodies into the feed.
/// ≥ [`CONFIRM_RUN_MAX`] so the engine can fill a full wave when bodies exist.
const OFFER_AHEAD: u32 = 96;

/// Loaded batches waiting for scripts (load can run ahead of scripts).
pub(crate) const LOAD_QUEUE_CAP: usize = 5;
/// Script-ok batches buffered for write (scripts(N+1) may run while N writes).
pub(crate) const WRITE_QUEUE_CAP: usize = 5;

/// Max heights claimable ahead of tip+1 (pipeline depth).
///
/// Prep may start the next run while scripts/write hold earlier ones, but must
/// **not** skip a stuck tip+1 and claim thousands of far heights. Far claims
/// with `confirm_wire_prep` path_lo=tip+1 return `Ok(None)` and used to leave
/// those heights stuck in `feed.inflight` forever (mainnet: tip=86, inflight=2049).
const MAX_CLAIM_AHEAD: u32 =
    (LOAD_QUEUE_CAP + WRITE_QUEUE_CAP + 1) as u32 * CONFIRM_RUN_MAX as u32;

/// Live depths **and contents** of the two bounded confirm pipeline queues.
///
/// Updated on successful send/recv so the status loop can log pressure and
/// process-owned retain without peeking into the OS channels.
#[derive(Debug, Default)]
pub(crate) struct ConfirmQueueDepths {
    /// load → scripts (`SyncSender` capacity [`LOAD_QUEUE_CAP`]).
    load_to_scripts: AtomicUsize,
    /// scripts → write (`SyncSender` capacity [`WRITE_QUEUE_CAP`]).
    scripts_to_write: AtomicUsize,
    /// Sum of `batch.len()` sitting in load→scripts.
    load_blocks: AtomicUsize,
    /// Sum of approx wire bytes of those batches.
    load_wire_bytes: AtomicUsize,
    /// Sum of `BatchParents` entries riding load→scripts batches.
    load_parents: AtomicUsize,
    /// Sum of `batch.len()` sitting in scripts→write.
    write_blocks: AtomicUsize,
    write_wire_bytes: AtomicUsize,
    write_parents: AtomicUsize,
}

/// Snapshot of confirm pipeline retain (queue depths + batch contents + feed).
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct ConfirmPipelineSizes {
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

/// Confirm pipeline queue depths for progress/perf: `prepq… writeq…`.
///
/// Depth 0 uses `name<0/cap` (consumer waiting on empty queue).
/// `prepq` = prep→scripts; `writeq` = scripts→write.
#[inline]
pub(crate) fn format_conf_q(prep: usize, write: usize, prep_cap: usize, write_cap: usize) -> String {
    format!(
        "{} {}",
        format_queue_depth("prepq", prep, prep_cap),
        format_queue_depth("writeq", write, write_cap),
    )
}

impl ConfirmQueueDepths {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// `(load→scripts depth, scripts→write depth)`.
    pub(crate) fn snap(&self) -> (usize, usize) {
        (
            self.load_to_scripts.load(Ordering::Relaxed),
            self.scripts_to_write.load(Ordering::Relaxed),
        )
    }

    /// Full content snapshot (depths + blocks/wire/parents in each queue).
    pub(crate) fn content_snap(&self) -> ConfirmPipelineSizes {
        ConfirmPipelineSizes {
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

    fn note_load_send(&self, blocks: usize, wire_bytes: usize, parents: usize) {
        self.load_to_scripts.fetch_add(1, Ordering::Relaxed);
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
        self.scripts_to_write.fetch_add(1, Ordering::Relaxed);
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

/// True when a load/script error should re-queue the batch (not permanent reject).
#[inline]
pub(crate) fn is_confirm_load_retryable(msg: &str) -> bool {
    msg.contains("load incomplete")
        || msg.contains("parent package not ready")
        || msg.contains("load not ready")
}

/// Spawn confirm **prep** + **scripts** + **write** OS threads.
///
/// Prep (body queue → plan Class A + pin parents + assemble) on
/// `ibd-confirm-load`; scripts on `ibd-confirm`; sole Class A append +
/// structural + Class C + spend annotate on `ibd-confirm-write`.
/// Overlap: prep(N+1) ∥ scripts(N) ∥ write(N−1).
/// Returns the prep-thread join handle and shared queue-depth counters.
pub(crate) fn spawn_confirm_engine(
    hub: Arc<ChainHub>,
    feed: Arc<ConfirmFeed>,
    event_tx: std::sync::mpsc::Sender<ConfirmEvent>,
    accepted: Arc<AtomicU32>,
    loop_stats: Arc<LoopStats>,
) -> (std::thread::JoinHandle<()>, Arc<ConfirmQueueDepths>) {
    let queues = ConfirmQueueDepths::new();
    let (mat_tx, mat_rx) = std::sync::mpsc::sync_channel::<(
        rbitcoin_consensus::LoadedBatch,
        u64, // load work_ns
    )>(LOAD_QUEUE_CAP);
    let (write_tx, write_rx) = std::sync::mpsc::sync_channel::<rbitcoin_consensus::ScriptOkBatch>(
        WRITE_QUEUE_CAP,
    );
    // Write reject / soft denserels: prep drops reserved fks + last_prepped so
    // re-prep after Class A partial commit does not drift next_tx_start.
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
            while let Ok(batch) = write_rx.recv() {
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
                        let mut ram_flushed: Vec<BlockHash> = Vec::new();
                        for (height, raw) in &heights_hashes {
                            let hash = BlockHash::from_byte_array(*raw);
                            // Durable queue: drop payload only after confirm-write.
                            // Flush may spill other RAM-pending bodies to disk.
                            match hub_wb.query.block_queue_dequeue_height(*height) {
                                Ok((_n, flushed)) => {
                                    for fh in flushed {
                                        ram_flushed
                                            .push(BlockHash::from_byte_array(fh));
                                    }
                                }
                                Err(e) => {
                                    rbitcoin_log::debug!(
                                        "ibd: block_queue dequeue h={height}: {e}"
                                    );
                                }
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
                        if !ram_flushed.is_empty()
                            && event_tx_wb
                                .send(ConfirmEvent::RamFlushed {
                                    hashes: ram_flushed,
                                })
                                .is_err()
                        {
                            feed_wb.finish(heights_hashes.iter().map(|(h, _)| *h));
                            return;
                        }
                        feed_wb.finish(heights_hashes.iter().map(|(h, _)| *h));
                        let elapsed = t0.elapsed();
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
                        // soft-reget vs blacklist). Invalidate prep-ahead caches
                        // so reserved create fks / last_prepped do not drift.
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
            while let Ok((mat_batch, mat_ns)) = mat_rx.recv() {
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
                        let script_ms = outcome.work_ns / 1_000_000;
                        let mat_ms = mat_ns / 1_000_000;
                        let wb = outcome.batch.len();
                        let ww = outcome.batch.approx_wire_bytes();
                        let wp = outcome.batch.parent_count();
                        if write_tx.send(outcome.batch).is_err() {
                            info!("ibd: confirm write channel closed");
                            break;
                        }
                        q_sc.note_write_send(wb, ww, wp);
                        if script_ms > 2_000 || mat_ms > 2_000 {
                            info!(
                                "ibd: confirm scripts slow batch={n} first={first_h} prep_ms={mat_ms} script_ms={script_ms} wall_ms={}",
                                t0.elapsed().as_millis()
                            );
                        }
                    }
                    Err(e) => {
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

    // Load: claim feed → load/wave/wire/assemble → scripts queue.
    // Capture queues for depth accounting (moved into this thread).
    let queues_load = Arc::clone(&queues);
    let prep_ahead_reset_load = Arc::clone(&prep_ahead_reset);
    let load_join = std::thread::Builder::new()
        .name("ibd-confirm-load".into())
        .spawn(move || {
            info!("ibd: confirm prep on dedicated OS thread (body queue → plan+pin+assemble)");
            let mut missing_tries: HashMap<u32, u32> = HashMap::new();
            let mut prep_ahead = PrepAheadState::new(&hub);
            loop {
                if feed.stopped() {
                    break;
                }
                if prep_ahead_reset_load.swap(false, Ordering::AcqRel) {
                    prep_ahead.clear_all(&hub);
                }
                prep_ahead.prune_committed(&hub);

                let batch: Vec<(u32, BlockHash, Option<bitcoin::Block>)> = {
                    let mut g = feed.inner.lock().unwrap();
                    let found = loop {
                        if feed.stopped() {
                            drop(g);
                            drop(mat_tx);
                            let _ = scripts.join();
                            return;
                        }
                        let tip = hub.tip_height();
                        let tip_h = tip.unwrap_or(0);
                        // Genesis: tip None → expect 0; otherwise tip+1.
                        let path_lo = if tip.is_none() {
                            0u32
                        } else {
                            tip_h.saturating_add(1)
                        };
                        g.ready.retain(|&h, _| h >= path_lo);
                        g.inflight.retain(|&h| h >= path_lo);

                        // Claim start = tip+1, or first height after a short
                        // contiguous inflight prefix (prep∥scripts∥write). Never
                        // jump past a hole or past MAX_CLAIM_AHEAD — otherwise
                        // far batches return Ok(None) from wire prep and used to
                        // pin inflight forever while tip stalls.
                        let mut claim_at = path_lo;
                        while g.inflight.contains(&claim_at)
                            && claim_at < path_lo.saturating_add(MAX_CLAIM_AHEAD)
                        {
                            claim_at = claim_at.saturating_add(1);
                        }
                        let claim_start = if claim_at > path_lo.saturating_add(MAX_CLAIM_AHEAD)
                        {
                            None // tip-near pipeline full — wait for write finish
                        } else if g.inflight.contains(&claim_at) {
                            None
                        } else if g.ready.contains_key(&claim_at) {
                            Some(claim_at)
                        } else {
                            None // tip+1 hole (or gap after inflight prefix)
                        };
                        if let Some(expect) = claim_start {
                            let claim_hi = path_lo.saturating_add(MAX_CLAIM_AHEAD);
                            let mut run = Vec::with_capacity(CONFIRM_RUN_MAX);
                            let mut h = expect;
                            while run.len() < CONFIRM_RUN_MAX && h <= claim_hi {
                                if g.inflight.contains(&h) {
                                    break; // don't merge into another claimed run
                                }
                                let Some((hash, wire)) = g.ready.remove(&h) else { break };
                                if hub.has_block(&hash) {
                                    h = h.saturating_add(1);
                                    continue;
                                }
                                g.inflight.insert(h);
                                run.push((h, hash, wire));
                                h = h.saturating_add(1);
                            }
                            if !run.is_empty() {
                                break Some(run);
                            }
                            // Empty after skipping confirmed — retry loop.
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
                        None => continue,
                    }
                };

                if batch.is_empty() {
                    std::thread::sleep(Duration::from_millis(20));
                    continue;
                }

                let expect_h = batch[0].0;
                struct LiveGuard<'a> {
                    stats: &'a LoopStats,
                }
                impl Drop for LiveGuard<'_> {
                    fn drop(&mut self) {
                        self.stats.confirm_end();
                    }
                }
                loop_stats.confirm_begin(expect_h, batch.len() as u32);
                let _live_guard = LiveGuard {
                    stats: &loop_stats,
                };
                if feed.stopped() || hub.query.confirm_cancelled() {
                    drop(_live_guard);
                    drop(mat_tx);
                    let _ = scripts.join();
                    return;
                }

                // Resolve wire from the body queue (peer/rehydrate intake).
                // ConfirmFeed only tracks readiness; payloads live in block_queue
                // until confirm-write dequeues after tip advance.
                let mut batch = batch;
                if let Err(missing) = resolve_batch_wire_from_body_queue(&hub, &mut batch) {
                    // Ghost readiness (noted without payload) or race before bq
                    // offer. Do not WARN every claim tick — that flooded mainnet
                    // logs after restart (~100 WARNs/s). Rate-limit; BodyMissing
                    // clears optimistic pending so densify can re-getdata.
                    static MISS_LOG: AtomicU32 = AtomicU32::new(0);
                    let n = MISS_LOG.fetch_add(1, Ordering::Relaxed) + 1;
                    if n <= 3 || n % 200 == 0 {
                        let (h0, hash0, _) = missing[0];
                        warn!(
                            "ibd: confirm prep missing body queue first=@{h0} {hash0} \
                             miss_n={} batch={} (count={n}; BodyMissing → re-getdata)",
                            missing.len(),
                            batch.len()
                        );
                    }
                    for (_, hash, _) in &missing {
                        let _ = event_tx.send(ConfirmEvent::BodyMissing { hash: *hash });
                    }
                    // Return claimed-with-payload heights to feed as readiness only
                    // (wire stays in body queue — never retain Block on the feed).
                    let req: Vec<(u32, BlockHash, Option<bitcoin::Block>)> = batch
                        .iter()
                        .filter(|(h, _, _)| !missing.iter().any(|(mh, _, _)| mh == h))
                        .map(|(h, ha, _)| (*h, *ha, None))
                        .collect();
                    // Missing ones: finish inflight so offer can re-note after re-get.
                    feed.finish(missing.iter().map(|(h, _, _)| *h));
                    feed.requeue_wire(&req);
                    // Avoid busy-spin when feed is full of ghost readiness notes.
                    std::thread::sleep(Duration::from_millis(5));
                    continue;
                }

                // Prefer unified wire prep when every claimed height has wire.
                // path_lo = first claimed height (tip+1 or after in-flight prep prefix).
                let store_path_lo = match hub.tip_height() {
                    None => 0u32,
                    Some(t) => t.saturating_add(1),
                };
                let pipe = prep_ahead.pipeline_for(expect_h, store_path_lo);
                // Only use pipeline caches when claiming at/after store tip with
                // reserved HWM (always safe; enables prep ahead of tip).
                let use_pipe = pipe.path_lo >= store_path_lo;
                let all_wire = batch.iter().all(|(_, _, w)| w.is_some());
                let mat_res = if all_wire {
                    let wire_batch: Vec<(rbitcoin_primitives::Height, bitcoin::Block)> = batch
                        .iter()
                        .map(|(h, _, w)| {
                            (
                                rbitcoin_primitives::Height(*h),
                                w.clone().expect("all_wire"),
                            )
                        })
                        .collect();
                    if use_pipe {
                        hub.confirm_wire_prep_phase_pipelined(&wire_batch, Some(&pipe))
                    } else {
                        hub.confirm_wire_prep_phase(&wire_batch)
                    }
                } else {
                    // Fallback: already-archived Class A without body-queue payload.
                    let hash_batch: Vec<(u32, BlockHash)> =
                        batch.iter().map(|(h, ha, _)| (*h, *ha)).collect();
                    hub.confirm_load_phase(&hash_batch)
                };
                let mat_res = match mat_res {
                    Err(e) if batch.len() > 1 => {
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
                            // Permanent failure on multi-block: re-queue tail only;
                            // first height stays inflight for the single-block retry.
                            warn!(
                                "ibd: confirm prep multi-block fail @ {expect_h} n={} — \
                                 retry first alone, re-queue tail: {msg}",
                                batch.len()
                            );
                            let tail: Vec<(u32, BlockHash, Option<bitcoin::Block>)> = batch
                                .iter()
                                .skip(1)
                                .filter(|(_, ha, _)| !hub.has_block(ha))
                                .map(|(h, ha, _)| (*h, *ha, None))
                                .collect();
                            feed.requeue_wire(&tail);
                            loop_stats.confirm_begin(expect_h, 1);
                            if let Some((_, _, Some(w))) = batch.first() {
                                let one = [(rbitcoin_primitives::Height(expect_h), w.clone())];
                                if use_pipe {
                                    hub.confirm_wire_prep_phase_pipelined(&one, Some(&pipe))
                                } else {
                                    hub.confirm_wire_prep_phase(&one)
                                }
                            } else {
                                hub.confirm_load_phase(&[(expect_h, batch[0].1)])
                            }
                        }
                    }
                    other => other,
                };
                drop(_live_guard);

                if feed.stopped() || hub.query.confirm_cancelled() {
                    if let Err(e) = &mat_res {
                        let msg = e.to_string();
                        if msg.contains("cancelled") || msg.contains("confirm cancelled") {
                            info!("ibd: confirm prep aborted after stop (cancelled)");
                        } else {
                            info!("ibd: confirm prep aborted after stop: {e}");
                        }
                    }
                    drop(mat_tx);
                    let _ = scripts.join();
                    return;
                }

                match mat_res {
                    Ok(None) => {
                        // Wire prep skipped (not contiguous from tip+1, already
                        // confirmed, empty). Must release claim — leaving heights
                        // in feed.inflight blocks re-note and freezes tip forever.
                        let retry: Vec<(u32, BlockHash, Option<bitcoin::Block>)> = batch
                            .iter()
                            .filter(|(_, ha, _)| !hub.has_block(ha))
                            .map(|(h, ha, _)| (*h, *ha, None))
                            .collect();
                        if !retry.is_empty() {
                            // Pipeline claim ahead of store tip+1 (or brief race):
                            // rate-limit; not a permanent error.
                            static N: AtomicU32 = AtomicU32::new(0);
                            let n = N.fetch_add(1, Ordering::Relaxed) + 1;
                            if n <= 3 || n % 500 == 0 {
                                debug!(
                                    "ibd: confirm prep empty outcome first={expect_h} n={} \
                                     (path not contiguous from tip+1 / already confirmed; \
                                      re-queue, count={n})",
                                    retry.len()
                                );
                            }
                            feed.requeue_wire(&retry);
                            // Avoid busy-spin when tip+1 is not yet claimable as contig.
                            std::thread::sleep(Duration::from_millis(5));
                        } else {
                            feed.finish(batch.iter().map(|(h, _, _)| *h));
                        }
                    }
                    Ok(Some(outcome)) => {
                        let work_ms = outcome.work_ns / 1_000_000;
                        // prepared.len(), not claim size (multi-split can shrink need).
                        let prepared_n = outcome.batch.len();
                        let prepared_heights: std::collections::HashSet<u32> = outcome
                            .batch
                            .heights_hashes()
                            .into_iter()
                            .map(|(h, _)| h)
                            .collect();
                        // Reserve create fks + outs for prep(N+1) while this batch is in-flight.
                        if let Some(plan) = outcome.batch.archive_plan.as_ref() {
                            if let Some((lh, raw)) = outcome
                                .batch
                                .heights_hashes()
                                .into_iter()
                                .max_by_key(|(h, _)| *h)
                            {
                                prep_ahead.note_plan_ok(&hub, plan, lh, raw);
                            }
                        } else if let Some((lh, raw)) = outcome
                            .batch
                            .heights_hashes()
                            .into_iter()
                            .max_by_key(|(h, _)| *h)
                        {
                            prep_ahead.last_prepped = Some((lh, raw));
                        }
                        if prepared_n != batch.len() {
                            // Re-queue claim heights prep dropped (write only finishes prepared).
                            let tail: Vec<(u32, BlockHash, Option<bitcoin::Block>)> = batch
                                .iter()
                                .filter(|(h, ha, _)| {
                                    !prepared_heights.contains(h) && !hub.has_block(ha)
                                })
                                .map(|(h, ha, _)| (*h, *ha, None))
                                .collect();
                            if !tail.is_empty() {
                                warn!(
                                    "ibd: confirm prep prepared_n={prepared_n} != claim_n={} first={expect_h} re-queue tail={}",
                                    batch.len(),
                                    tail.len()
                                );
                                feed.requeue_wire(&tail);
                            }
                        }
                        let wire = outcome.batch.approx_wire_bytes();
                        let parents = outcome.batch.parent_count();
                        if mat_tx
                            .send((outcome.batch, outcome.work_ns))
                            .is_err()
                        {
                            info!("ibd: confirm scripts channel closed");
                            return;
                        }
                        queues_load.note_load_send(prepared_n, wire, parents);
                        if work_ms > 2_000 {
                            info!(
                                "ibd: confirm prep slow batch={prepared_n} claim={} first={expect_h} work_ms={work_ms}",
                                batch.len(),
                            );
                        }
                    }
                    Err(e) => {
                        let (expect, hash, _) = batch[0];
                        let msg = e.to_string();
                        if msg.contains("confirm cancelled") {
                            info!("ibd: confirm prep cancelled @ {expect}");
                            drop(mat_tx);
                            let _ = scripts.join();
                            return;
                        }
                        if is_confirm_load_retryable(&msg) {
                            let retry: Vec<(u32, BlockHash, Option<bitcoin::Block>)> = batch
                                .iter()
                                .filter(|(_, ha, _)| !hub.has_block(ha))
                                .map(|(h, ha, _)| (*h, *ha, None))
                                .collect();
                            feed.requeue_wire(&retry);
                            static N: AtomicU32 = AtomicU32::new(0);
                            let n = N.fetch_add(1, Ordering::Relaxed) + 1;
                            if n <= 3 || n % 200 == 0 {
                                warn!(
                                    "ibd: confirm prep incomplete @ {expect} {hash} — re-queue (n={n}): {msg}"
                                );
                            }
                            std::thread::sleep(Duration::from_millis(50));
                            continue;
                        }
                        if msg.contains("confirm without archive")
                            || msg.contains("NotFound")
                            || msg.contains("not found")
                        {
                            let tries = missing_tries.entry(expect).or_insert(0);
                            *tries = tries.saturating_add(1);
                            let n = *tries;
                            if n == 1 {
                                debug!(
                                    "ibd: confirm prep missing body @{expect} {hash} \
                                     (need body queue / getdata; not re-queuing hash-only)"
                                );
                            } else if n == 10 || n % 100 == 0 {
                                warn!(
                                    "ibd: confirm prep still missing body @{expect} {hash} (n={n})"
                                );
                            }
                            // Do **not** requeue onto the feed: that spins prep with no
                            // progress while soft budget is full and densify is gated.
                            // Finish inflight + BodyMissing so assign can re-getdata tip+1.
                            feed.finish(batch.iter().map(|(h, _, _)| *h));
                            if missing_tries.len() > 256 {
                                missing_tries.retain(|&h, _| h.saturating_add(64) > expect);
                            }
                            if event_tx
                                .send(ConfirmEvent::BodyMissing { hash })
                                .is_err()
                            {
                                break;
                            }
                            std::thread::sleep(Duration::from_millis(5));
                            continue;
                        }
                        if batch.len() > 1 {
                            let tail: Vec<(u32, BlockHash, Option<bitcoin::Block>)> = batch
                                .iter()
                                .skip(1)
                                .filter(|(_, ha, _)| !hub.has_block(ha))
                                .map(|(h, ha, _)| (*h, *ha, None))
                                .collect();
                            feed.requeue_wire(&tail);
                        }
                        // Permanent reject on first height — drop inflight so tip can move
                        // only after operator/event handling; do not re-queue first.
                        // Invalidate reserved fks / in-flight creates past this height.
                        prep_ahead.clear_all(&hub);
                        feed.finish(std::iter::once(expect));
                        missing_tries.remove(&expect);
                        loop_stats
                            .confirm_reject_stops
                            .fetch_add(1, Ordering::Relaxed);
                        warn!("ibd: confirm prep reject {hash} @ {expect}: {e}");
                        if event_tx
                            .send(ConfirmEvent::Reject {
                                height: expect,
                                hash,
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
        })
        .expect("spawn ibd-confirm-load");
    (load_join, queues)
}

/// Fill missing wire slots from the durable/pending body queue.
///
/// Returns `Err(missing)` with the heights that had no payload (and no pre-filled
/// wire). Entries that already have `Some(block)` are left untouched.
fn resolve_batch_wire_from_body_queue(
    hub: &ChainHub,
    batch: &mut [(u32, BlockHash, Option<bitcoin::Block>)],
) -> Result<(), Vec<(u32, BlockHash, Option<bitcoin::Block>)>> {
    use bitcoin::consensus::Decodable;
    let mut missing = Vec::new();
    for entry in batch.iter_mut() {
        if entry.2.is_some() {
            continue;
        }
        let height = entry.0;
        let hash = entry.1;
        match hub.query.block_queue_payload(height) {
            Ok(Some(payload)) => {
                let mut cursor = std::io::Cursor::new(payload.as_slice());
                match bitcoin::Block::consensus_decode(&mut cursor) {
                    Ok(block) => {
                        // Sanity: payload hash should match feed hash.
                        if block.block_hash() != hash {
                            warn!(
                                "ibd: body queue hash mismatch @{height}: feed={hash} payload={}",
                                block.block_hash()
                            );
                            missing.push((height, hash, None));
                            continue;
                        }
                        entry.2 = Some(block);
                    }
                    Err(e) => {
                        warn!("ibd: body queue decode fail @{height} {hash}: {e}");
                        missing.push((height, hash, None));
                    }
                }
            }
            Ok(None) => {
                // No body-queue payload: allow Class A hash-only fallback later.
                // Only treat as missing if the block is also not Class-A ready.
                if !hub.query.is_block_archived(&hash.to_byte_array()).unwrap_or(false) {
                    missing.push((height, hash, None));
                }
            }
            Err(e) => {
                warn!("ibd: body queue read fail @{height} {hash}: {e}");
                missing.push((height, hash, None));
            }
        }
    }
    if missing.is_empty() {
        Ok(())
    } else {
        Err(missing)
    }
}

/// Offer a run of claim-ready heights starting at tip+1 into the confirm feed.
///
/// Pre-noting ahead of tip lets the engine batch multi-block script waves when
/// the body queue (or Class A fallback) leads tip. Caps at [`OFFER_AHEAD`].
///
/// Uses `height_to_hash` for **O(OFFER_AHEAD)** work — never scans the full
/// ordered path (that pegged a core at ~130k headers with tip frozen).
///
/// Does **not** require `ordered_set` membership: after resume seed + tip trim,
/// height_to_hash is the source of truth for the parent cache. Gating on
/// ordered_set left tip frozen with hole=0 when the set lagged the height map.
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
        // Claim-ready = body queue / pending / Class A — not Class A alone.
        // Peer path also notes feed on offer; this fills Class A fallback and
        // re-notes after restart hygiene. Break on first fetch hole so we do
        // not pretip far heights while tip+1 is missing (confirm still claims
        // tip-first; noting far without tip is harmless but wastes map RAM).
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
    use super::{format_conf_q, format_queue_depth, is_confirm_load_retryable, ConfirmFeed};
    use bitcoin::hashes::Hash;
    use bitcoin::BlockHash;

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
        assert!(super::MAX_CLAIM_AHEAD >= super::CONFIRM_RUN_MAX as u32);
        assert!(
            super::MAX_CLAIM_AHEAD <= 512,
            "keep claim window modest: {}",
            super::MAX_CLAIM_AHEAD
        );
        let path_lo = 87u32;
        // Within window: claim a short run only (not the whole far feed).
        let run = claim_feed_run(
            path_lo,
            super::CONFIRM_RUN_MAX,
            path_lo + super::MAX_CLAIM_AHEAD,
            |h| h >= path_lo && h < path_lo + 1000,
            |_| false,
        );
        assert_eq!(run.len(), super::CONFIRM_RUN_MAX);
        assert_eq!(run[0], path_lo);
        assert!(*run.last().unwrap() <= path_lo + super::MAX_CLAIM_AHEAD);
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
    fn wait_timeout_is_confirm_load_retryable_not_reject() {
        assert!(is_confirm_load_retryable(
            "confirm: load incomplete (parent package not ready, timeout)"
        ));
        assert!(is_confirm_load_retryable(
            "confirm: load incomplete (wave body missing from cache)"
        ));
        // Plan-miss MTP (after hybrid median_time_past) must re-queue, not
        // permanent multi-block → n=1 split.
        assert!(is_confirm_load_retryable(
            "confirm: load incomplete (parent header plan missing above tip)"
        ));
        assert!(!is_confirm_load_retryable("script failed: false"));
        assert!(!is_confirm_load_retryable("prevout already spent"));
        // Store-only MTP BadPrev used to hit the silent multi-split path.
        assert!(!is_confirm_load_retryable("unexpected previous header"));
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
        assert_eq!(format_conf_q(0, 1, 2, 2), "prepq<0/2 writeq=1/2");
        assert_eq!(format_conf_q(1, 0, 2, 2), "prepq=1/2 writeq<0/2");
        assert_eq!(format_conf_q(0, 0, 2, 2), "prepq<0/2 writeq<0/2");

        assert_eq!(super::LOAD_QUEUE_CAP, 5);
        assert_eq!(super::WRITE_QUEUE_CAP, 5);
        assert_eq!(
            format_conf_q(0, 0, super::LOAD_QUEUE_CAP, super::WRITE_QUEUE_CAP),
            "prepq<0/5 writeq<0/5"
        );
        assert_eq!(
            format_conf_q(5, 5, super::LOAD_QUEUE_CAP, super::WRITE_QUEUE_CAP),
            "prepq=5/5 writeq=5/5"
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
        assert_eq!(q.snap(), (0, 0));
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
        assert_eq!(q.snap(), (1, 1));

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
        // Tip is 0; expect tip+1 = 1. Provide archived height 1.
        let h1 = bh(0x11);
        h2h.insert(1u32, h1);
        body.mark_archived(h1);
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

        // Non-rejected multi-height walk: height 1 ready+archived, height 2 missing → stop after 1.
        body = BodyPresence::new();
        let mut h2h3 = HashMap::new();
        h2h3.insert(1u32, h1);
        h2h3.insert(2u32, h2);
        body.mark_archived(h1);
        // h2 not archived → offer stops after noting h1.
        feed.finish([1]);
        max_arch = 0;
        let n5 = offer_confirm_ready(&feed, &h2h3, &mut body, &hub, &mut max_arch, &shared);
        assert_eq!(n5, 1);
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
