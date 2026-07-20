//! Background confirm-runway parent prewarm (tip+1 … tip+depth).
//!
//! Best-effort IO priority. UTXO / durable parents load from store; runway
//! creates map txid→fk (outs only for open reserves). Confirm hard-waits only
//! for the batch to be scanned; headroom is soft so a slow warmer cannot
//! freeze tip advance and starve peer downloads.

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
            rbitcoin_store::try_set_io_best_effort();
            let depth = prewarm_depth_from_env();
            let batch = prewarm_batch_from_env();
            let headroom = prewarm_headroom_from_env();
            info!(
                "ibd: parent prewarm worker ON (depth≤{depth}, batch≤{batch}, headroom={headroom} soft; env RBITCOIN_PARENT_PREWARM_*)"
            );
            let mut cursor: usize = 0;
            let mut last_tip = u32::MAX;
            // Start "fresh" so we do not INFO-idle immediately at tip=0/runway=0.
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
                    // Idle INFO only after we have done real work (avoids tip=0 noise).
                    if ever_worked && last_info.elapsed() >= Duration::from_secs(30) {
                        let (through, ahead, parents, bodies, plans, d) =
                            query.parent_prewarm_perf_snapshot();
                        info!(
                            "ibd: prewarm idle tip={tip} +{ahead} thru={through} parents={parents} bodies={bodies} plans={plans}/{d} runway={}",
                            runway.len()
                        );
                        last_info = std::time::Instant::now();
                    }
                    std::thread::sleep(Duration::from_millis(40));
                    continue;
                }
                let end = (cursor + batch as usize).min(runway.len());
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
                        // Per-slice detail only at trace (debug was multi-Hz spam).
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
                        // Periodic INFO: cursor is *after* this slice.
                        if last_info.elapsed() >= Duration::from_secs(10) {
                            let (_, _, parents, bodies, plans, d) =
                                query.parent_prewarm_perf_snapshot();
                            info!(
                                "ibd: prewarm tip={tip} +{ahead} thru={through} parents={parents} bodies={bodies} plans={plans}/{d} cursor={cursor}/{} last_h={h0}..{h1} blks={} body_io={} parent_io={} {ms}ms",
                                runway.len(),
                                st.blocks,
                                st.body_tx_reads,
                                st.full_tx_reads,
                            );
                            last_info = std::time::Instant::now();
                        }
                    }
                    Err(e) => {
                        cursor = end;
                        debug!("ibd: prewarm error: {e}");
                    }
                }
                std::thread::sleep(Duration::from_millis(5));
            }
            info!("ibd: parent prewarm worker stopped");
        })
        .expect("spawn ibd-parent-prewarm")
}
