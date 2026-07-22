//! Background confirm-runway parent prewarm (tip+1 … tip+depth).
//!
//! **Owns Class A load** for the confirm runway. Confirm only waits on ready
//! notify (see `confirm_run::wait_for_prewarm`) — it never last-miles while
//! this worker is live.
//!
//! Normal I/O priority (not best-effort): when tip is hard on the runway the
//! warmer must not lose the disk to archive.

use rbitcoin_log::{debug, info};
use rbitcoin_query::{
    prewarm_batch_from_env, prewarm_depth_from_env, prewarm_headroom_from_env, Query,
};
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;

/// Shared runway: main loop publishes tip/arch/hashes; worker prewarms.
pub(crate) struct PrewarmControl {
    pub stop: AtomicBool,
    pub tip: AtomicU32,
    pub arch: AtomicU32,
    /// Contiguous (height, hash) for tip+1.. in order (Arc — no clone per tick).
    pub runway: Mutex<Arc<[(u32, [u8; 32])]>>,
}

impl PrewarmControl {
    pub fn new() -> Self {
        Self {
            stop: AtomicBool::new(false),
            tip: AtomicU32::new(0),
            arch: AtomicU32::new(0),
            runway: Mutex::new(Arc::from([])),
        }
    }

    pub fn request_stop(&self) {
        self.stop.store(true, Ordering::Relaxed);
    }

    pub fn publish(&self, tip: u32, arch: u32, items: Vec<(u32, [u8; 32])>) {
        self.tip.store(tip, Ordering::Relaxed);
        self.arch.store(arch, Ordering::Relaxed);
        *self.runway.lock().unwrap() = Arc::from(items);
    }
}

pub(crate) fn spawn_parent_prewarm(
    query: Arc<Query>,
    ctrl: Arc<PrewarmControl>,
) -> JoinHandle<()> {
    std::thread::Builder::new()
        .name("ibd-parent-prewarm".into())
        .spawn(move || {
            // Intentionally **not** IOPRIO best-effort: prewarm must keep pace
            // with tip. Archive already writes continuously; starving the warmer
            // freezes confirm (which only waits while we are live).
            query.set_prewarm_worker_live(true);
            let depth = prewarm_depth_from_env();
            let batch = prewarm_batch_from_env();
            let headroom = prewarm_headroom_from_env();
            let mlock = rbitcoin_query::prewarm_mlock_from_env();
            let pin_near = rbitcoin_query::prewarm_pin_near_from_env();
            let thin_fk = rbitcoin_query::prewarm_thin_create_fk_only_from_env();
            info!(
                "ibd: parent prewarm worker ON (depth≤{depth}, batch≤{batch}, headroom={headroom}, mlock={mlock}, pin_near={pin_near}, thin_create_fk_only={thin_fk}; env RBITCOIN_PARENT_PREWARM_*)"
            );
            // next_height watermark (not index into a replaced Arc runway).
            // ConfirmParentCache.tip **must** track IBD tip: ensure_plans only
            // accepts heights in (cache.tip, cache.tip+depth]. Without
            // advance_tip, plans stay empty, ready_through stays 0, confirm
            // never moves (mainnet stuck tip=360250 plans=0 thru=0).
            let mut next_height: u32 = 0;
            let mut last_tip = u32::MAX;
            let mut last_info = std::time::Instant::now();
            let mut ever_worked = false;
            while !ctrl.stop.load(Ordering::Relaxed) {
                let tip = ctrl.tip.load(Ordering::Relaxed);
                if tip != last_tip {
                    last_tip = tip;
                    // Sets plan horizon + GC; mutex-serialized with confirm's tip GC.
                    query.advance_parent_runway_tip(tip);
                    // Re-walk runway; is_ready skips heights already marked.
                    next_height = tip.saturating_add(1);
                } else if next_height <= tip {
                    next_height = tip.saturating_add(1);
                }
                let runway = Arc::clone(&*ctrl.runway.lock().unwrap());
                // First index with height >= next_height.
                let start = runway.partition_point(|(h, _)| *h < next_height);
                if runway.is_empty() || start >= runway.len() {
                    // Stuck recovery: walked past runway but nothing ready ahead
                    // of tip (e.g. tip never advanced cache before first walk).
                    let through = query.parent_prewarm_ready_through();
                    if !runway.is_empty() && through <= tip {
                        next_height = tip.saturating_add(1);
                        // fall through next iteration without long sleep
                        std::thread::sleep(Duration::from_millis(1));
                        continue;
                    }
                    if ever_worked && last_info.elapsed() >= Duration::from_secs(30) {
                        let (through, ahead, by_txid, bodies, plans, d) =
                            query.parent_prewarm_perf_snapshot();
                        info!(
                            "ibd: prewarm idle tip={tip} +{ahead} thru={through} by_txid={by_txid} bodies={bodies} plans={plans}/{d} runway={}",
                            runway.len()
                        );
                        last_info = std::time::Instant::now();
                    }
                    std::thread::sleep(Duration::from_millis(20));
                    continue;
                }
                // When behind tip, take a larger bite (up to 2× configured batch).
                let through = query.parent_prewarm_ready_through();
                let ahead = through.saturating_sub(tip);
                let bite = if ahead < headroom.max(16) {
                    (batch as usize).saturating_mul(2).min(256)
                } else {
                    batch as usize
                };
                let end = (start + bite).min(runway.len());
                let slice = &runway[start..end];
                let h0 = slice.first().map(|x| x.0).unwrap_or(0);
                let h1 = slice.last().map(|x| x.0).unwrap_or(0);
                let t0 = std::time::Instant::now();
                match query.prewarm_parents_for_heights(slice) {
                    Ok(st) => {
                        ever_worked = true;
                        next_height = h1.saturating_add(1);
                        let through = query.parent_prewarm_ready_through();
                        let ahead = through.saturating_sub(tip);
                        let ms = t0.elapsed().as_millis();
                        if st.blocks > 0
                            || st.utxo_parents > 0
                            || st.creates_registered > 0
                        {
                            rbitcoin_log::trace!(
                                "ibd: prewarm h={h0}..{h1} blocks={} parents={} creates={} body_io={} parent_io={} pin_cached={} pin_new={} cache_hit={} skip={} +{ahead} thru={through} {ms}ms",
                                st.blocks,
                                st.utxo_parents,
                                st.creates_registered,
                                st.body_tx_reads,
                                st.full_tx_reads,
                                st.pin_already_cached,
                                st.pin_new,
                                st.parent_cache_hits,
                                st.already_ready,
                            );
                        }
                        if last_info.elapsed() >= Duration::from_secs(10) {
                            let (_, _, by_txid, bodies, plans, d) =
                                query.parent_prewarm_perf_snapshot();
                            let hdr = st.header_ns / 1_000_000;
                            let ml = st.body_mlock_ns / 1_000_000;
                            let dec = st.body_decode_ns / 1_000_000;
                            let thin = st.thin_ns / 1_000_000;
                            let t_col = st.thin_collect_ns / 1_000_000;
                            let t_run = st.thin_runway_ns / 1_000_000;
                            let t_head = st.thin_head_ns / 1_000_000;
                            let t_edge = st.thin_edge_ns / 1_000_000;
                            let pin = st.parent_pin_ns / 1_000_000;
                            let put = st.cache_put_ns / 1_000_000;
                            let sticky_n = query.confirm_parent_cache().sticky_confirmed_count();
                            let (mlock_ranges, mlock_bytes) = query.prewarm_mlock_stats();
                            let parents = query.confirm_parent_cache().parent_count();
                            info!(
                                "ibd: prewarm tip={tip} +{ahead} thru={through} by_txid={by_txid} bodies={bodies} parents={parents} sticky={sticky_n} mlock_rng={mlock_ranges} mlock_MiB={} plans={plans}/{d} next_h={next_height} runway={} last_h={h0}..{h1} blks={} body_io={} parent_io={} pin_cached={} pin_new={} head={}/{} mlock_sys={}/{} edges same={} runway={} sticky={} head={} sticky_hit={} {ms}ms (hdr={hdr} mlock={ml} dec={dec} thin={thin}[col={t_col} run={t_run} head={t_head} edge={t_edge}] pin={pin} put={put})",
                                mlock_bytes / (1024 * 1024),
                                runway.len(),
                                st.blocks,
                                st.body_tx_reads,
                                st.full_tx_reads,
                                st.pin_already_cached,
                                st.pin_new,
                                st.head_hits,
                                st.head_lookups,
                                st.mlock_syscalls,
                                st.mlock_skipped,
                                st.edge_same_batch,
                                st.edge_runway,
                                st.edge_sticky,
                                st.edge_head,
                                st.sticky_hits,
                            );
                            last_info = std::time::Instant::now();
                        }
                        // Stay hot when still behind; only yield when we have lead.
                        if end < runway.len() && ahead < headroom.max(16) {
                            // no sleep — tip is chewing the runway
                        } else if end < runway.len() {
                            std::thread::sleep(Duration::from_millis(1));
                        } else {
                            std::thread::sleep(Duration::from_millis(8));
                        }
                    }
                    Err(e) => {
                        next_height = h1.saturating_add(1);
                        let msg = e.to_string();
                        if msg.contains("cancelled") {
                            debug!("ibd: prewarm stopped (cancelled)");
                        } else {
                            debug!("ibd: prewarm error: {e}");
                        }
                        std::thread::sleep(Duration::from_millis(5));
                    }
                }
            }
            query.set_prewarm_worker_live(false);
            info!("ibd: parent prewarm worker stopped");
        })
        .expect("spawn ibd-parent-prewarm")
}
