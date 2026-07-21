//! Dedicated confirm engine (Class C tip walk) for IBD.

use super::body::BodyPresence;
use super::LoopStats;
use crate::chain::ChainHub;
use bitcoin::BlockHash;
use rbitcoin_log::{info, warn};
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
}

/// How many consecutive ready heights to confirm in one multi-block script wave.
/// Larger waves keep rayon cores busy when archive leads tip. Fat single blocks
/// still dominate wall time; this packs thin consecutive heights.
const CONFIRM_RUN_MAX: usize = 32;

/// How far ahead of tip to pre-note ready bodies into the feed.
/// ≥ [`CONFIRM_RUN_MAX`] so the engine can fill a full wave when bodies exist.
const OFFER_AHEAD: u32 = 96;

/// Dedicated OS thread: multi-block confirm so script checks fill cores while
/// the IBD event loop stays on network/archive.
pub(crate) fn spawn_confirm_engine(
    hub: Arc<ChainHub>,
    feed: Arc<ConfirmFeed>,
    event_tx: std::sync::mpsc::Sender<ConfirmEvent>,
    accepted: Arc<AtomicU32>,
    loop_stats: Arc<LoopStats>,
) -> std::thread::JoinHandle<()> {
    std::thread::Builder::new()
        .name("ibd-confirm".into())
        .spawn(move || {
            info!("ibd: confirm engine on dedicated OS thread");
            loop {
                if feed.stopped() {
                    break;
                }

                let batch: Vec<(u32, BlockHash)> = {
                    let mut g = feed.ready.lock().unwrap();
                    let found = loop {
                        if feed.stopped() {
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
                            let mut run = Vec::with_capacity(CONFIRM_RUN_MAX);
                            let mut h = expect;
                            while run.len() < CONFIRM_RUN_MAX {
                                let Some(&hash) = g.get(&h) else { break };
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
                    continue;
                }

                // Drop already-confirmed prefix.
                let batch: Vec<(u32, BlockHash)> = batch
                    .into_iter()
                    .filter(|(_, h)| !hub.has_block(h))
                    .collect();
                if batch.is_empty() {
                    // Stale feed entries (has_block but tip not advanced, or
                    // re-noted confirmed hashes). Scrub and back off — a tight
                    // re-loop here pegs one core with confirm_ms=0 forever.
                    let mut g = feed.ready.lock().unwrap();
                    if let Some(t) = hub.tip_height() {
                        g.retain(|&h, _| h > t);
                    }
                    static STALE: AtomicU32 = AtomicU32::new(0);
                    let n = STALE.fetch_add(1, Ordering::Relaxed) + 1;
                    if n <= 5 || n % 200 == 0 {
                        warn!(
                            "ibd: confirm feed stale (batch empty after has_block filter) tip={:?} feed_len={} (n={n})",
                            hub.tip_height(),
                            g.len()
                        );
                    }
                    drop(g);
                    std::thread::sleep(Duration::from_millis(50));
                    continue;
                }

                let t0 = Instant::now();
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
                // Abort before starting a wave if stop was requested while we
                // held the feed lock / built the batch.
                if feed.stopped() || hub.query.confirm_cancelled() {
                    return;
                }
                let res = hub.confirm_run(&batch);
                let res = match res {
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
                        {
                            Err(e)
                        } else {
                            // Attribute failure: retry tip+1 alone.
                            if feed.stopped() || hub.query.confirm_cancelled() {
                                Err(e)
                            } else {
                                loop_stats.confirm_begin(expect_h, 1);
                                hub.confirm_run(&batch[..1])
                            }
                        }
                    }
                    other => other,
                };
                let elapsed = t0.elapsed();
                drop(_live_guard);
                loop_stats
                    .confirm_ns
                    .fetch_add(elapsed.as_nanos() as u64, Ordering::Relaxed);

                // Shutdown: do not emit reject events or blacklist after stop.
                if feed.stopped() || hub.query.confirm_cancelled() {
                    if let Err(e) = &res {
                        info!("ibd: confirm aborted after stop: {e}");
                    }
                    return;
                }

                match res {
                    Ok(outcomes) => {
                        let n = outcomes.len().min(batch.len());
                        let mut g = feed.ready.lock().unwrap();
                        for i in 0..n {
                            let (height, hash) = batch[i];
                            g.remove(&height);
                            loop_stats.confirm_blocks.fetch_add(1, Ordering::Relaxed);
                            accepted.fetch_add(1, Ordering::SeqCst);
                            if event_tx
                                .send(ConfirmEvent::Accepted { hash })
                                .is_err()
                            {
                                return;
                            }
                        }
                        if elapsed.as_millis() > 2_000 {
                            info!(
                                "ibd: confirm_run slow batch={n} first={expect_h} {:?}",
                                elapsed
                            );
                        }
                    }
                    Err(e) => {
                        let (expect, hash) = batch[0];
                        let msg = e.to_string();
                        if msg.contains("confirm cancelled") {
                            info!("ibd: confirm cancelled @ {expect}");
                            return;
                        }
                        if msg.contains("confirm without archive")
                            || msg.contains("NotFound")
                            || msg.contains("not found")
                        {
                            // Transient only for a short window; then drop so we
                            // re-offer / re-getdata instead of spinning forever.
                            static TRANSIENT: AtomicU32 = AtomicU32::new(0);
                            let n = TRANSIENT.fetch_add(1, Ordering::Relaxed) + 1;
                            if n <= 8 || n % 50 == 0 {
                                warn!(
                                    "ibd: confirm transient {hash} @ {expect}: {msg} (n={n})"
                                );
                            }
                            if n >= 40 {
                                // Drop from feed so offer can re-note after body state updates.
                                // Do **not** Reject (that blacklists permanently).
                                let _ = feed.ready.lock().unwrap().remove(&expect);
                                TRANSIENT.store(0, Ordering::Relaxed);
                                warn!(
                                    "ibd: confirm drop transient {hash} @ {expect} after {n} tries: {msg}"
                                );
                            }
                            std::thread::sleep(Duration::from_millis(25));
                            continue;
                        }
                        loop_stats
                            .confirm_reject_stops
                            .fetch_add(1, Ordering::Relaxed);
                        warn!("ibd: confirm reject {hash} @ {expect}: {e}");
                        let _ = feed.ready.lock().unwrap().remove(&expect);
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
