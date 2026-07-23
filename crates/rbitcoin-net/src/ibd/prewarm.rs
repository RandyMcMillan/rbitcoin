//! Background confirm-runway parent prewarm (tip+1 … tip+depth).
//!
//! **Owns Class A load** for the confirm runway. Confirm only waits on ready
//! notify (see `confirm_run::wait_for_prewarm`) — it never last-miles while
//! this worker is live.
//!
//! Normal I/O priority (not best-effort): when tip is hard on the runway the
//! warmer must not lose the disk to archive.
//!
//! Work cursor is always `max(ready_through, tip) + 1` (first package gap).
//! Bite size is a configured batch (or 2× while building lead) — never 1-block
//! forced rehydrate, which serialized post-restart confirm.

use rbitcoin_log::{debug, info};
use rbitcoin_query::{
    prewarm_batch_from_env, prewarm_depth_from_env, prewarm_headroom_from_env, Query,
};
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;

/// First height the worker should attempt: contiguous package gap after tip.
#[inline]
pub(crate) fn prewarm_work_cursor(tip: u32, ready_through: u32) -> u32 {
    ready_through.max(tip).saturating_add(1)
}

/// Align with confirm script wave ([`super::confirm`] CONFIRM_RUN_MAX = 32).
/// First prewarm bites target this so confirm can start without waiting on a
/// 64/128-block pin pass.
pub(crate) const PREWARM_CONFIRM_WAVE: u32 = 32;

/// How many runway heights to take in one `prewarm_parents_for_heights` call.
///
/// When the contiguous ready lead is below one confirm wave, use a **wave-sized**
/// bite (not full `batch` / 2×). Otherwise the first post-restart call pins 64–128
/// heights for tens of seconds while confirm starves or races mid-construction.
#[inline]
pub(crate) fn prewarm_bite_size(ahead: u32, batch: u32, headroom: u32) -> usize {
    let batch = batch.max(1);
    let wave = PREWARM_CONFIRM_WAVE;
    if ahead < wave {
        // Deliver tip+1‥tip+wave ASAP for the first confirm claim.
        (batch as usize).min(wave as usize)
    } else if ahead < headroom.max(wave) {
        (batch as usize).saturating_mul(2).min(256)
    } else {
        batch as usize
    }
}

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
            // Work cursor is always the first package gap after tip
            // (`ready_through + 1`). Never skip incomplete middle heights:
            // advancing past a hole left ready_through stuck at +1/+2 while the
            // worker chewed far runway (confirm starved, logs looked 1-block).
            // ConfirmParentCache.tip **must** track IBD tip: ensure_plans only
            // accepts heights in (cache.tip, cache.tip+depth]. Without
            // advance_tip, plans stay empty, ready_through stays 0, confirm
            // never moves (mainnet stuck tip=360250 plans=0 thru=0).
            let mut last_tip = u32::MAX;
            let mut last_info = std::time::Instant::now();
            let mut ever_worked = false;
            while !ctrl.stop.load(Ordering::Relaxed) {
                let tip = ctrl.tip.load(Ordering::Relaxed);
                if tip != last_tip {
                    last_tip = tip;
                    // Sets plan horizon + GC; mutex-serialized with confirm's tip GC.
                    query.advance_parent_runway_tip(tip);
                }
                let runway = Arc::clone(&*ctrl.runway.lock().unwrap());
                // Contiguous package watermark: resume at first incomplete height.
                let through = query.parent_prewarm_ready_through();
                let next_height = prewarm_work_cursor(tip, through);
                // First index with height >= next_height.
                let start = runway.partition_point(|(h, _)| *h < next_height);
                if runway.is_empty() || start >= runway.len() {
                    // Stuck recovery: walked past runway but nothing ready ahead
                    // of tip (e.g. tip never advanced cache before first walk).
                    if !runway.is_empty() && through <= tip {
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
                // Bite size: wave-sized until we have a confirm wave of lead,
                // then batch / 2× to build headroom (never 1-block force).
                let ahead = through.saturating_sub(tip);
                let bite = prewarm_bite_size(ahead, batch, headroom);
                let end = (start + bite).min(runway.len());
                let slice = &runway[start..end];
                let h0 = slice.first().map(|x| x.0).unwrap_or(0);
                let h1 = slice.last().map(|x| x.0).unwrap_or(0);
                let t0 = std::time::Instant::now();
                match query.prewarm_parents_for_heights(slice) {
                    Ok(st) => {
                        ever_worked = true;
                        // next work = first incomplete after tip (ready_through+1).
                        let through = query.parent_prewarm_ready_through();
                        let next_height = prewarm_work_cursor(tip, through);
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
                        // Do not advance past the gap — retry from ready_through+1.
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

#[cfg(test)]
mod tests {
    use super::{prewarm_bite_size, prewarm_work_cursor};

    #[test]
    fn work_cursor_resumes_first_package_gap() {
        // After a bite leaves only tip+1..tip+2 ready, resume at tip+3 — never
        // jump to h1+1 past the hole (historical +1/+2 stall).
        assert_eq!(prewarm_work_cursor(100, 102), 103);
        assert_eq!(prewarm_work_cursor(100, 100), 101);
        assert_eq!(prewarm_work_cursor(100, 0), 101); // hollow watermark
        assert_eq!(prewarm_work_cursor(100, 99), 101); // ready behind tip
    }

    #[test]
    fn bite_prioritizes_first_confirm_wave() {
        // Empty / short lead → wave-sized (32), not 64 or 128 — first package
        // delivered promptly for confirm claim.
        assert_eq!(prewarm_bite_size(0, 64, 64), 32);
        assert_eq!(prewarm_bite_size(16, 64, 64), 32);
        assert_eq!(prewarm_bite_size(31, 64, 64), 32);
        // Full wave ready → may 2× while building headroom.
        assert_eq!(prewarm_bite_size(32, 64, 64), 128);
        assert_eq!(prewarm_bite_size(100, 64, 64), 64); // enough lead
        assert_eq!(prewarm_bite_size(0, 1, 64), 1); // env batch=1 only
        assert_eq!(prewarm_bite_size(0, 16, 64), 16); // batch < wave
    }

    #[test]
    fn ahead_after_one_confirm_is_not_a_package_hole() {
        // ready_through fixed, tip +1 → ahead drops by 1 (e.g. +64 → +63).
        // Not a failed bite; just tip advance arithmetic.
        let tip = 100u32;
        let through = 164u32;
        assert_eq!(through.saturating_sub(tip), 64);
        assert_eq!(through.saturating_sub(tip + 1), 63);
    }
}
