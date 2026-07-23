//! Dedicated confirm engine (Class C tip walk) for IBD.

use super::body::BodyPresence;
use super::status::LoopStats;
use crate::chain::ChainHub;
use bitcoin::hashes::Hash;
use bitcoin::BlockHash;
use rbitcoin_log::{debug, info, warn};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Shared feed of archived (height, hash) pairs for the dedicated confirm engine.
pub(crate) struct ConfirmFeed {
    ready: std::sync::Mutex<std::collections::BTreeMap<u32, BlockHash>>,
    cv: std::sync::Condvar,
    stop: AtomicBool,
}

impl ConfirmFeed {
    pub(crate) fn new() -> Self {
        Self {
            ready: std::sync::Mutex::new(std::collections::BTreeMap::new()),
            cv: std::sync::Condvar::new(),
            stop: AtomicBool::new(false),
        }
    }

    pub(crate) fn note(&self, height: u32, hash: BlockHash) {
        let mut g = self.ready.lock().unwrap();
        g.insert(height, hash);
        self.cv.notify_one();
    }

    pub(crate) fn request_stop(&self) {
        self.stop.store(true, Ordering::SeqCst);
        self.cv.notify_all();
    }

    pub(crate) fn stopped(&self) -> bool {
        self.stop.load(Ordering::SeqCst)
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

/// Max script-ok batches buffered for writeback (scripts(N+1) may run while N writes).
const WRITEBACK_QUEUE: usize = 2;

/// Spawn confirm **script** + **writeback** OS threads.
///
/// Scripts run optimistically on `ibd-confirm`; structural spentness + Class C +
/// spend annotate run FIFO on `ibd-confirm-writeback` so scripts(N+1) can overlap
/// writeback(N). Returns the script-thread join handle (writeback joins on drop
/// of the channel when the script thread exits).
pub(crate) fn spawn_confirm_engine(
    hub: Arc<ChainHub>,
    feed: Arc<ConfirmFeed>,
    event_tx: std::sync::mpsc::Sender<ConfirmEvent>,
    accepted: Arc<AtomicU32>,
    loop_stats: Arc<LoopStats>,
) -> std::thread::JoinHandle<()> {
    let (wb_tx, wb_rx) = std::sync::mpsc::sync_channel::<rbitcoin_consensus::ScriptOkBatch>(
        WRITEBACK_QUEUE,
    );

    // Writeback: structural + class_c + annotate; emits tip events.
    let hub_wb = Arc::clone(&hub);
    let feed_wb = Arc::clone(&feed);
    let event_tx_wb = event_tx.clone();
    let accepted_wb = Arc::clone(&accepted);
    let loop_stats_wb = Arc::clone(&loop_stats);
    let _writeback = std::thread::Builder::new()
        .name("ibd-confirm-writeback".into())
        .spawn(move || {
            info!("ibd: confirm writeback on dedicated OS thread");
            while let Ok(batch) = wb_rx.recv() {
                if feed_wb.stopped() || hub_wb.query.confirm_cancelled() {
                    break;
                }
                let n = batch.len();
                let first_h = batch.heights_hashes().first().map(|(h, _)| *h).unwrap_or(0);
                let t0 = Instant::now();
                let heights_hashes = batch.heights_hashes();
                match hub_wb.confirm_writeback(batch) {
                    Ok(_outcomes) => {
                        // Heights were claimed off the feed at script-take time.
                        for (_height, raw) in &heights_hashes {
                            let hash = BlockHash::from_byte_array(*raw);
                            loop_stats_wb
                                .confirm_blocks
                                .fetch_add(1, Ordering::Relaxed);
                            accepted_wb.fetch_add(1, Ordering::SeqCst);
                            if event_tx_wb
                                .send(ConfirmEvent::Accepted { hash })
                                .is_err()
                            {
                                return;
                            }
                        }
                        // Writeback pure work only (recv already done before t0).
                        // Do not mix into confirm_ns — that is script-stage work.
                        let elapsed = t0.elapsed();
                        if elapsed.as_millis() > 2_000 {
                            info!(
                                "ibd: confirm writeback slow batch={n} first={first_h} {:?}",
                                elapsed
                            );
                        }
                    }
                    Err(e) => {
                        let msg = e.to_string();
                        if msg.contains("confirm cancelled") || feed_wb.stopped() {
                            info!("ibd: confirm writeback aborted: {msg}");
                            break;
                        }
                        // Real batch identity (never all-zero hash).
                        let (height, hash) = heights_hashes
                            .first()
                            .map(|(h, raw)| (*h, BlockHash::from_byte_array(*raw)))
                            .unwrap_or((first_h, BlockHash::from_byte_array([0u8; 32])));
                        // Already confirmed (stale/dup pipeline): do not blacklist.
                        if hub_wb.has_block(&hash)
                            || (msg.contains("prevout already spent")
                                && heights_hashes.iter().all(|(_, raw)| {
                                    hub_wb.has_block(&BlockHash::from_byte_array(*raw))
                                }))
                        {
                            debug!(
                                "ibd: confirm writeback skip already-committed @{height} ({msg})"
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
                            continue;
                        }
                        loop_stats_wb
                            .confirm_reject_stops
                            .fetch_add(1, Ordering::Relaxed);
                        warn!("ibd: confirm writeback reject @ {height}: {e}");
                        let _ = event_tx_wb.send(ConfirmEvent::Reject {
                            height,
                            hash,
                            err: msg,
                        });
                    }
                }
            }
            info!("ibd: confirm writeback exit");
        })
        .expect("spawn ibd-confirm-writeback");

    std::thread::Builder::new()
        .name("ibd-confirm".into())
        .spawn(move || {
            info!("ibd: confirm scripts on dedicated OS thread (writeback pipelined)");
            let mut missing_tries: HashMap<u32, u32> = HashMap::new();
            loop {
                if feed.stopped() {
                    break;
                }

                let batch: Vec<(u32, BlockHash)> = {
                    let mut g = feed.ready.lock().unwrap();
                    let found = loop {
                        if feed.stopped() {
                            drop(g);
                            drop(wb_tx);
                            let _ = _writeback.join();
                            return;
                        }
                        let tip = hub.tip_height();
                        let expect = match tip {
                            None => 0u32,
                            Some(t) => t.saturating_add(1),
                        };
                        if let Some(t) = tip {
                            g.retain(|&h, _| h > t);
                        }
                        if g.contains_key(&expect) {
                            // Claim heights off the feed immediately so we cannot
                            // re-script the same tip+1 while writeback is still
                            // in-flight (that double-queued structural checks and
                            // blacklisted valid blocks as "prevout already spent").
                            let mut run = Vec::with_capacity(CONFIRM_RUN_MAX);
                            let mut h = expect;
                            while run.len() < CONFIRM_RUN_MAX {
                                let Some(hash) = g.remove(&h) else { break };
                                if hub.has_block(&hash) {
                                    h = h.saturating_add(1);
                                    continue;
                                }
                                run.push((h, hash));
                                h = h.saturating_add(1);
                            }
                            break Some(run);
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
                    // Claim skipped already-confirmed heights; wait for tip+1.
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
                    drop(wb_tx);
                    let _ = _writeback.join();
                    return;
                }

                // SCRIPT STAGE only — writeback is async FIFO.
                // `work_ns` excludes prewarm Condvar wait (honest script timing).
                let script_res = hub.confirm_script_phase(&batch);
                let script_res = match script_res {
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
                            || msg.contains("prewarm incomplete")
                        {
                            Err(e)
                        } else {
                            loop_stats.confirm_begin(expect_h, 1);
                            hub.confirm_script_phase(&batch[..1])
                        }
                    }
                    other => other,
                };
                drop(_live_guard);

                if feed.stopped() || hub.query.confirm_cancelled() {
                    if let Err(e) = &script_res {
                        let msg = e.to_string();
                        if msg.contains("cancelled") || msg.contains("confirm cancelled") {
                            info!("ibd: confirm aborted after stop (cancelled)");
                        } else {
                            info!("ibd: confirm aborted after stop: {e}");
                        }
                    }
                    drop(wb_tx);
                    let _ = _writeback.join();
                    return;
                }

                match script_res {
                    Ok(None) => {
                        // Already confirmed — heights already claimed off feed.
                    }
                    Ok(Some(outcome)) => {
                        // Pure script-stage wall only (not prewarm wait, not wb send).
                        loop_stats
                            .confirm_ns
                            .fetch_add(outcome.work_ns, Ordering::Relaxed);
                        let work_ms = outcome.work_ns / 1_000_000;
                        // Hand to writeback — `send` may block on full queue; that
                        // wait is backpressure, not script work (do not time it).
                        if wb_tx.send(outcome.batch).is_err() {
                            info!("ibd: confirm writeback channel closed");
                            return;
                        }
                        if work_ms > 2_000 {
                            info!(
                                "ibd: confirm scripts slow batch={} first={expect_h} work_ms={work_ms}",
                                batch.len(),
                            );
                        }
                    }
                    Err(e) => {
                        let (expect, hash) = batch[0];
                        let msg = e.to_string();
                        if msg.contains("confirm cancelled") {
                            info!("ibd: confirm cancelled @ {expect}");
                            drop(wb_tx);
                            let _ = _writeback.join();
                            return;
                        }
                        if msg.contains("prewarm incomplete") {
                            // Runway lag / package drained — re-queue (do not demote Class A).
                            {
                                let mut g = feed.ready.lock().unwrap();
                                for &(h, ha) in &batch {
                                    if !hub.has_block(&ha) {
                                        g.insert(h, ha);
                                    }
                                }
                                feed.cv.notify_one();
                            }
                            static N: AtomicU32 = AtomicU32::new(0);
                            let n = N.fetch_add(1, Ordering::Relaxed) + 1;
                            if n <= 3 || n % 200 == 0 {
                                warn!(
                                    "ibd: confirm prewarm incomplete @ {expect} {hash} — re-queue (n={n}): {msg}"
                                );
                            }
                            // Give the prewarm worker time to re-hydrate tip+1
                            // (and avoid multi-kHz spam when package is still empty).
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
                            // Re-queue claimed heights so offer can retry after Class A lands.
                            {
                                let mut g = feed.ready.lock().unwrap();
                                for &(h, ha) in &batch {
                                    if !hub.has_block(&ha) {
                                        g.insert(h, ha);
                                    }
                                }
                                feed.cv.notify_one();
                            }
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
                        // Multi-block script failure: re-queue suffix after tip+1 so
                        // a single bad tip does not strand later ready heights.
                        if batch.len() > 1 {
                            let mut g = feed.ready.lock().unwrap();
                            for &(h, ha) in batch.iter().skip(1) {
                                if !hub.has_block(&ha) {
                                    g.insert(h, ha);
                                }
                            }
                            feed.cv.notify_one();
                        }
                        missing_tries.remove(&expect);
                        loop_stats
                            .confirm_reject_stops
                            .fetch_add(1, Ordering::Relaxed);
                        warn!("ibd: confirm script reject {hash} @ {expect}: {e}");
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
            drop(wb_tx);
            let _ = _writeback.join();
        })
        .expect("spawn ibd-confirm")
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
/// height_to_hash is the source of truth for the confirm runway. Gating on
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
