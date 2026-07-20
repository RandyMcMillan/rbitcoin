//! Background confirm-runway parent prewarm (tip+1 … tip+depth).
//!
//! Best-effort IO priority. Only UTXO-backed parents are loaded from store;
//! same-runway creates store full outs and fill reserved holes (including
//! create-before-reserve). Confirm waits until each batch is ready **and**
//! the warmer is ~2 prewarm batches ahead (`RBITCOIN_PARENT_PREWARM_HEADROOM`).

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
                "ibd: parent prewarm worker ON (depth≤{depth}, batch≤{batch}, headroom={headroom}; env RBITCOIN_PARENT_PREWARM_*)"
            );
            let mut cursor: usize = 0;
            let mut last_tip = u32::MAX;
            while !ctrl.stop.load(Ordering::Relaxed) {
                let tip = ctrl.tip.load(Ordering::Relaxed);
                if tip != last_tip {
                    cursor = 0;
                    last_tip = tip;
                    query.advance_parent_runway_tip(tip);
                }
                let runway = ctrl.runway.lock().unwrap().clone();
                if runway.is_empty() || cursor >= runway.len() {
                    std::thread::sleep(Duration::from_millis(40));
                    continue;
                }
                // Prefer advancing the contiguous ready watermark so confirm
                // headroom is satisfied before re-scanning far-ahead already-ready
                // heights. Cursor still walks the full runway.
                let end = (cursor + batch as usize).min(runway.len());
                let slice = &runway[cursor..end];
                match query.prewarm_parents_for_heights(slice) {
                    Ok(st)
                        if st.blocks > 0
                            || st.utxo_parents > 0
                            || st.reserved > 0
                            || st.creates_registered > 0 =>
                    {
                        debug!(
                            "ibd: prewarm h={}..{} blocks={} utxo_parents={} reserved={} creates={} ready={} through={}",
                            slice.first().map(|x| x.0).unwrap_or(0),
                            slice.last().map(|x| x.0).unwrap_or(0),
                            st.blocks,
                            st.utxo_parents,
                            st.reserved,
                            st.creates_registered,
                            st.already_ready,
                            query.parent_prewarm_ready_through()
                        );
                    }
                    Ok(_) => {}
                    Err(e) => debug!("ibd: prewarm error: {e}"),
                }
                cursor = end;
                std::thread::sleep(Duration::from_millis(5));
            }
            info!("ibd: parent prewarm worker stopped");
        })
        .expect("spawn ibd-parent-prewarm")
}
