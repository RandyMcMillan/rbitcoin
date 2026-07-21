//! Background confirm-runway parent prewarm (tip+1 … tip+depth).
//!
//! **Owns Class A load** for the confirm runway. Confirm waits for batch
//! readiness (see `confirm_run::wait_for_prewarm`); it only last-miles after a
//! grace if this worker has not marked the tip batch ready.
//!
//! Normal I/O priority (not best-effort): when tip is hard on the runway the
//! warmer must not lose the disk to archive. UTXO / durable parents load from
//! store; runway creates register full outs (bodies-first).

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
    /// Contiguous (height, hash) for tip+1.. in order.
    pub runway: Mutex<Vec<(u32, [u8; 32])>>,
}

impl PrewarmControl {
    pub fn new() -> Self {
        Self {
            stop: AtomicBool::new(false),
            tip: AtomicU32::new(0),
            arch: AtomicU32::new(0),
            runway: Mutex::new(Vec::new()),
        }
    }

    pub fn request_stop(&self) {
        self.stop.store(true, Ordering::Relaxed);
    }

    pub fn publish(&self, tip: u32, arch: u32, items: Vec<(u32, [u8; 32])>) {
        self.tip.store(tip, Ordering::Relaxed);
        self.arch.store(arch, Ordering::Relaxed);
        *self.runway.lock().unwrap() = items;
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
            // forces confirm last-mile and kills tip rate.
            let depth = prewarm_depth_from_env();
            let batch = prewarm_batch_from_env();
            let headroom = prewarm_headroom_from_env();
            info!(
                "ibd: parent prewarm worker ON (depth≤{depth}, batch≤{batch}, headroom={headroom} soft; env RBITCOIN_PARENT_PREWARM_*)"
            );
            let mut cursor: usize = 0;
            let mut last_tip = u32::MAX;
            let mut last_info = std::time::Instant::now();
            let mut ever_worked = false;
            while !ctrl.stop.load(Ordering::Relaxed) {
                let tip = ctrl.tip.load(Ordering::Relaxed);
                if tip != last_tip {
                    cursor = 0;
                    last_tip = tip;
                    query.advance_parent_runway_tip(tip);
                }
                let runway = ctrl.runway.lock().unwrap().clone();
                if runway.is_empty() || cursor >= runway.len() {
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
                let end = (cursor + bite).min(runway.len());
                let slice = &runway[cursor..end];
                let h0 = slice.first().map(|x| x.0).unwrap_or(0);
                let h1 = slice.last().map(|x| x.0).unwrap_or(0);
                let t0 = std::time::Instant::now();
                match query.prewarm_parents_for_heights(slice) {
                    Ok(st) => {
                        ever_worked = true;
                        cursor = end;
                        let through = query.parent_prewarm_ready_through();
                        let ahead = through.saturating_sub(tip);
                        let ms = t0.elapsed().as_millis();
                        if st.blocks > 0
                            || st.utxo_parents > 0
                            || st.creates_registered > 0
                        {
                            rbitcoin_log::trace!(
                                "ibd: prewarm h={h0}..{h1} blocks={} parents={} creates={} body_io={} parent_io={} cache_hit={} skip={} +{ahead} thru={through} {ms}ms",
                                st.blocks,
                                st.utxo_parents,
                                st.creates_registered,
                                st.body_tx_reads,
                                st.full_tx_reads,
                                st.parent_cache_hits,
                                st.already_ready,
                            );
                        }
                        if last_info.elapsed() >= Duration::from_secs(10) {
                            let (_, _, by_txid, bodies, plans, d) =
                                query.parent_prewarm_perf_snapshot();
                            info!(
                                "ibd: prewarm tip={tip} +{ahead} thru={through} by_txid={by_txid} bodies={bodies} plans={plans}/{d} cursor={cursor}/{} last_h={h0}..{h1} blks={} body_io={} parent_io={} {ms}ms",
                                runway.len(),
                                st.blocks,
                                st.body_tx_reads,
                                st.full_tx_reads,
                            );
                            last_info = std::time::Instant::now();
                        }
                        // Stay hot when still behind; only yield when we have lead.
                        if cursor < runway.len() && ahead < headroom.max(16) {
                            // no sleep — tip is chewing the runway
                        } else if cursor < runway.len() {
                            std::thread::sleep(Duration::from_millis(1));
                        } else {
                            std::thread::sleep(Duration::from_millis(8));
                        }
                    }
                    Err(e) => {
                        cursor = end;
                        debug!("ibd: prewarm error: {e}");
                        std::thread::sleep(Duration::from_millis(5));
                    }
                }
            }
            info!("ibd: parent prewarm worker stopped");
        })
        .expect("spawn ibd-parent-prewarm")
}
