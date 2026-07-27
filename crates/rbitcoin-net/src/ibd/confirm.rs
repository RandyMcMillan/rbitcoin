//! Dedicated confirm engine (Class C tip walk) for IBD.

use super::body::BodyPresence;
use super::status::LoopStats;
use crate::chain::ChainHub;
use bitcoin::hashes::Hash;
use bitcoin::BlockHash;
use rbitcoin_log::{debug, info, warn};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Shared feed of archived (height, hash) pairs for the dedicated confirm engine.
///
/// **In-flight tracking:** once load claims a contiguous run, those heights sit in
/// `inflight` until write finishes (or load re-queues). `note` will not re-insert
/// them — otherwise offer re-notes tip+1 every main-loop tick and load re-claims
/// the same batch into the load→scripts queue (duplicate script work).
pub(crate) struct ConfirmFeed {
    inner: std::sync::Mutex<ConfirmFeedInner>,
    cv: std::sync::Condvar,
    stop: AtomicBool,
}

struct ConfirmFeedInner {
    ready: std::collections::BTreeMap<u32, BlockHash>,
    /// Claimed by load; not yet written or released. Offer must not re-note.
    inflight: std::collections::HashSet<u32>,
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

    /// Note a ready archived body. No-op if height is already claimed (in-flight).
    pub(crate) fn note(&self, height: u32, hash: BlockHash) {
        let mut g = self.inner.lock().unwrap();
        if g.inflight.contains(&height) {
            return;
        }
        g.ready.insert(height, hash);
        self.cv.notify_one();
    }

    /// Return heights to the ready map (load incomplete / without-archive retry).
    pub(crate) fn requeue(&self, batch: &[(u32, BlockHash)]) {
        if batch.is_empty() {
            return;
        }
        let mut g = self.inner.lock().unwrap();
        for &(h, hash) in batch {
            g.inflight.remove(&h);
            g.ready.insert(h, hash);
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

/// How many consecutive ready heights to confirm in one multi-block script wave.
/// Larger waves keep rayon cores busy when archive leads tip. Fat single blocks
/// still dominate wall time; this packs thin consecutive heights.
const CONFIRM_RUN_MAX: usize = 32;

/// How far ahead of tip to pre-note ready bodies into the feed.
/// ≥ [`CONFIRM_RUN_MAX`] so the engine can fill a full wave when bodies exist.
const OFFER_AHEAD: u32 = 96;

/// Loaded batches waiting for scripts (load can run ahead of scripts).
pub(crate) const LOAD_QUEUE_CAP: usize = 5;
/// Script-ok batches buffered for write (scripts(N+1) may run while N writes).
pub(crate) const WRITE_QUEUE_CAP: usize = 5;

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

/// Confirm pipeline queue depths for progress/perf: `loadq… writeq…`.
///
/// Depth 0 uses `name<0/cap` (consumer waiting on empty queue).
#[inline]
pub(crate) fn format_conf_q(load: usize, write: usize, load_cap: usize, write_cap: usize) -> String {
    format!(
        "{} {}",
        format_queue_depth("loadq", load, load_cap),
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

/// Spawn confirm **load** + **scripts** + **write** OS threads.
///
/// Load (Class A + pin parents → wire → assemble) on
/// `ibd-confirm-load`; scripts on `ibd-confirm`; structural + Class C +
/// spend annotate on `ibd-confirm-write`.
/// Overlap: load(N+1) ∥ scripts(N) ∥ write(N−1).
/// Returns the load-thread join handle and shared queue-depth counters.
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

    // Write: structural + class_c + annotate; emits tip events.
    let hub_wb = Arc::clone(&hub);
    let feed_wb = Arc::clone(&feed);
    let event_tx_wb = event_tx.clone();
    let accepted_wb = Arc::clone(&accepted);
    let loop_stats_wb = Arc::clone(&loop_stats);
    let q_wb = Arc::clone(&queues);
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
                        for (height, raw) in &heights_hashes {
                            let hash = BlockHash::from_byte_array(*raw);
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
                            let _ = height;
                        }
                        feed_wb.finish(heights_hashes.iter().map(|(h, _)| *h));
                        let elapsed = t0.elapsed();
                        if elapsed.as_millis() > 2_000 {
                            let p = rbitcoin_consensus::confirm_phase_stats::last_write_phases();
                            let ms = rbitcoin_consensus::confirm_phase_stats::LastWritePhases::ms;
                            info!(
                                "ibd: confirm write slow batch={n} first={first_h} wall={:?} \
                                 struct={}ms spent={}ms create_h={}ms bip68={}ms class_c={}ms \
                                 spend_ann={}ms tip_gc={}ms",
                                elapsed,
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
                        // Permanent write reject — clear inflight (do not re-queue;
                        // reject event handles blacklist / operator path).
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
                        loop_stats_sc
                            .confirm_ns
                            .fetch_add(mat_ns.saturating_add(outcome.work_ns), Ordering::Relaxed);
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
                                "ibd: confirm scripts slow batch={n} first={first_h} mat_ms={mat_ms} script_ms={script_ms} wall_ms={}",
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
    let load_join = std::thread::Builder::new()
        .name("ibd-confirm-load".into())
        .spawn(move || {
            info!("ibd: confirm load on dedicated OS thread (wave/wire/assemble)");
            let mut missing_tries: HashMap<u32, u32> = HashMap::new();
            loop {
                if feed.stopped() {
                    break;
                }

                let batch: Vec<(u32, BlockHash)> = {
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

                        // Next claim start: walk from tip+1, skip heights already
                        // in-flight (pipeline overlap: load N+1 while scripts N).
                        // Stop at a hole (neither ready nor inflight).
                        let mut claim_at = path_lo;
                        let claim_start = loop {
                            if g.inflight.contains(&claim_at) {
                                claim_at = claim_at.saturating_add(1);
                                continue;
                            }
                            if g.ready.contains_key(&claim_at) {
                                break Some(claim_at);
                            }
                            break None; // hole or nothing ready
                        };
                        if let Some(expect) = claim_start {
                            let mut run = Vec::with_capacity(CONFIRM_RUN_MAX);
                            let mut h = expect;
                            while run.len() < CONFIRM_RUN_MAX {
                                if g.inflight.contains(&h) {
                                    break; // don't merge into another claimed run
                                }
                                let Some(hash) = g.ready.remove(&h) else { break };
                                if hub.has_block(&hash) {
                                    h = h.saturating_add(1);
                                    continue;
                                }
                                g.inflight.insert(h);
                                run.push((h, hash));
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

                let mat_res = hub.confirm_load_phase(&batch);
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
                            // Always log — silent split was hiding BIP68 MTP store-only
                            // BadPrev (n=32 claim → n=1 prepared, tip ~1 blk/cycle).
                            warn!(
                                "ibd: confirm load multi-block fail @ {expect_h} n={} — \
                                 retry first alone, re-queue tail: {msg}",
                                batch.len()
                            );
                            let tail: Vec<(u32, BlockHash)> = batch
                                .iter()
                                .skip(1)
                                .filter(|(_, ha)| !hub.has_block(ha))
                                .copied()
                                .collect();
                            feed.requeue(&tail);
                            // first stays inflight for the single-height retry below
                            loop_stats.confirm_begin(expect_h, 1);
                            hub.confirm_load_phase(&batch[..1])
                        }
                    }
                    other => other,
                };
                drop(_live_guard);

                if feed.stopped() || hub.query.confirm_cancelled() {
                    if let Err(e) = &mat_res {
                        let msg = e.to_string();
                        if msg.contains("cancelled") || msg.contains("confirm cancelled") {
                            info!("ibd: confirm load aborted after stop (cancelled)");
                        } else {
                            info!("ibd: confirm load aborted after stop: {e}");
                        }
                    }
                    drop(mat_tx);
                    let _ = scripts.join();
                    return;
                }

                match mat_res {
                    Ok(None) => {}
                    Ok(Some(outcome)) => {
                        let work_ms = outcome.work_ns / 1_000_000;
                        // prepared.len(), not claim size (multi-split can shrink need).
                        let prepared_n = outcome.batch.len();
                        if prepared_n != batch.len() {
                            warn!(
                                "ibd: confirm load prepared_n={prepared_n} != claim_n={} first={expect_h}",
                                batch.len()
                            );
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
                                "ibd: confirm load slow batch={prepared_n} claim={} first={expect_h} work_ms={work_ms}",
                                batch.len(),
                            );
                        }
                    }
                    Err(e) => {
                        let (expect, hash) = batch[0];
                        let msg = e.to_string();
                        if msg.contains("confirm cancelled") {
                            info!("ibd: confirm load cancelled @ {expect}");
                            drop(mat_tx);
                            let _ = scripts.join();
                            return;
                        }
                        if is_confirm_load_retryable(&msg) {
                            let retry: Vec<(u32, BlockHash)> = batch
                                .iter()
                                .filter(|(_, ha)| !hub.has_block(ha))
                                .copied()
                                .collect();
                            feed.requeue(&retry);
                            static N: AtomicU32 = AtomicU32::new(0);
                            let n = N.fetch_add(1, Ordering::Relaxed) + 1;
                            if n <= 3 || n % 200 == 0 {
                                warn!(
                                    "ibd: confirm load incomplete @ {expect} {hash} — re-queue (n={n}): {msg}"
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
                                    "ibd: confirm without archive {hash} @ {expect} (will re-offer when Class A lands)"
                                );
                            } else if n == 10 || n % 100 == 0 {
                                warn!(
                                    "ibd: confirm without archive still missing {hash} @ {expect} (n={n})"
                                );
                            }
                            let retry: Vec<(u32, BlockHash)> = batch
                                .iter()
                                .filter(|(_, ha)| !hub.has_block(ha))
                                .copied()
                                .collect();
                            feed.requeue(&retry);
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
                            let tail: Vec<(u32, BlockHash)> = batch
                                .iter()
                                .skip(1)
                                .filter(|(_, ha)| !hub.has_block(ha))
                                .copied()
                                .collect();
                            feed.requeue(&tail);
                        }
                        // Permanent reject on first height — drop inflight so tip can move
                        // only after operator/event handling; do not re-queue first.
                        feed.finish(std::iter::once(expect));
                        missing_tries.remove(&expect);
                        loop_stats
                            .confirm_reject_stops
                            .fetch_add(1, Ordering::Relaxed);
                        warn!("ibd: confirm load reject {hash} @ {expect}: {e}");
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

/// Offer a run of ready archived heights starting at tip+1 into the confirm feed.
///
/// Pre-noting ahead of tip lets the engine batch multi-block script waves when
/// archive leads (the post-milestone case). Caps at [`OFFER_AHEAD`].
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
    max_archived_height: &mut u32,
    max_archived_shared: &AtomicU32,
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
        if !body.ready(hub, &hash) {
            break;
        }
        *max_archived_height = (*max_archived_height).max(ht);
        max_archived_shared.store(*max_archived_height, Ordering::Relaxed);
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

    /// Contiguous feed claim (2-stage): up to max heights, skip already-confirmed.
    fn claim_feed_run(
        expect: u32,
        max: usize,
        feed_has: impl Fn(u32) -> bool,
        already_confirmed: impl Fn(u32) -> bool,
    ) -> Vec<u32> {
        let mut run = Vec::with_capacity(max.min(32));
        let mut h = expect;
        while run.len() < max {
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
        let run = claim_feed_run(101, 32, |h| h >= 101 && h < 101 + 40, |_| false);
        assert_eq!(run.len(), 32);
        assert_eq!(run[0], 101);
        assert_eq!(*run.last().unwrap(), 132);
        let run = claim_feed_run(
            10,
            32,
            |h| h >= 10 && h <= 50,
            |h| h == 10 || h == 11,
        );
        assert_eq!(run.first().copied(), Some(12));
        assert_eq!(run.len(), 32);
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
            let hash = g.ready.remove(&100).unwrap();
            g.inflight.insert(100);
            assert_eq!(hash, bh(1));
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
        feed.requeue(&[(50, bh(5)), (51, bh(6))]);
        {
            let g = feed.inner.lock().unwrap();
            assert!(!g.inflight.contains(&50));
            assert_eq!(g.ready.get(&50), Some(&bh(5)));
            assert_eq!(g.ready.get(&51), Some(&bh(6)));
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

    /// Log tokens + live caps (OPERATOR.md / experimental-mainnet loadq=*/5 writeq=*/5).
    #[test]
    fn queue_depth_log_and_caps_surface() {
        assert_eq!(format_queue_depth("load", 0, 2), "load<0/2");
        assert_eq!(format_queue_depth("write", 0, 2), "write<0/2");
        assert_eq!(format_queue_depth("load", 1, 2), "load=1/2");
        assert_eq!(format_queue_depth("write", 2, 2), "write=2/2");
        assert_eq!(format_conf_q(0, 1, 2, 2), "loadq<0/2 writeq=1/2");
        assert_eq!(format_conf_q(1, 0, 2, 2), "loadq=1/2 writeq<0/2");
        assert_eq!(format_conf_q(0, 0, 2, 2), "loadq<0/2 writeq<0/2");

        assert_eq!(super::LOAD_QUEUE_CAP, 5);
        assert_eq!(super::WRITE_QUEUE_CAP, 5);
        assert_eq!(
            format_conf_q(0, 0, super::LOAD_QUEUE_CAP, super::WRITE_QUEUE_CAP),
            "loadq<0/5 writeq<0/5"
        );
        assert_eq!(
            format_conf_q(5, 5, super::LOAD_QUEUE_CAP, super::WRITE_QUEUE_CAP),
            "loadq=5/5 writeq=5/5"
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
        feed.requeue(&[]); // no-op empty
        assert_eq!(feed.size_snap(), (2, 1));
        feed.request_stop();
        assert!(feed.stopped());
    }

    #[test]
    fn claim_feed_stops_at_gap() {
        let run = claim_feed_run(5, 10, |h| h == 5 || h == 6 || h == 8, |_| false);
        // Contiguous only — gap at 7 stops.
        assert_eq!(run, vec![5, 6]);
        let empty = claim_feed_run(1, 8, |_| false, |_| false);
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
            |h| (1..=10).contains(&h),
            |h| h == 1 || h == 2 || h == 5,
        );
        assert_eq!(run.first().copied(), Some(3));
        assert!(!run.contains(&5));
        assert_eq!(run, vec![3, 4, 6, 7, 8, 9, 10]);
        // Max 0 → empty.
        assert!(claim_feed_run(1, 0, |_| true, |_| false).is_empty());
    }
}
