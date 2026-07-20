//! Background confirm-runway parent prewarm (tip+1 … tip+PREWARM_DEPTH).
//!
//! Runs on a dedicated OS thread with **best-effort** IO priority (above the
//! idle archive writer). The main IBD loop pushes contiguous archived hashes
//! for the runway; the worker loads Class A bodies + external parents and
//! pins them until Class C spends those outs.

use rbitcoin_log::{debug, info};
use rbitcoin_query::{Query, PREWARM_BATCH, PREWARM_DEPTH};
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;

/// Shared runway state: main loop writes tip + ordered hashes; worker reads.
pub(crate) struct PrewarmControl {
    pub stop: AtomicBool,
    /// Confirmed tip height.
    pub tip: AtomicU32,
    /// Highest archived height known on the work path.
    pub arch: AtomicU32,
    /// Contiguous hashes for heights tip+1, tip+2, … (may lag tip slightly).
    pub runway: Mutex<Vec<[u8; 32]>>,
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

    /// Publish tip/arch and the next runway hashes (height order, tip+1 first).
    pub fn publish(&self, tip: u32, arch: u32, hashes: Vec<[u8; 32]>) {
        self.tip.store(tip, Ordering::Relaxed);
        self.arch.store(arch, Ordering::Relaxed);
        *self.runway.lock().unwrap() = hashes;
    }
}

/// Spawn the prewarm thread. Caller must `request_stop` + join on IBD exit.
pub(crate) fn spawn_parent_prewarm(
    query: Arc<Query>,
    ctrl: Arc<PrewarmControl>,
) -> JoinHandle<()> {
    std::thread::Builder::new()
        .name("ibd-parent-prewarm".into())
        .spawn(move || {
            rbitcoin_store::try_set_io_best_effort();
            info!(
                "ibd: parent prewarm worker ON (depth≤{PREWARM_DEPTH}, batch≤{PREWARM_BATCH})"
            );
            let mut cursor: u32 = 0; // next height offset from tip already done
            while !ctrl.stop.load(Ordering::Relaxed) {
                let tip = ctrl.tip.load(Ordering::Relaxed);
                let arch = ctrl.arch.load(Ordering::Relaxed);
                let target_end = tip.saturating_add(PREWARM_DEPTH).min(arch);
                if target_end <= tip {
                    cursor = 0;
                    std::thread::sleep(Duration::from_millis(80));
                    continue;
                }
                // How far past tip we have already warmed this tip epoch.
                if cursor > target_end.saturating_sub(tip) {
                    cursor = 0;
                }
                let runway = ctrl.runway.lock().unwrap().clone();
                if runway.is_empty() {
                    std::thread::sleep(Duration::from_millis(40));
                    continue;
                }
                // runway[0] = tip+1 hash.
                let start = cursor as usize;
                if start >= runway.len() {
                    std::thread::sleep(Duration::from_millis(40));
                    continue;
                }
                let end = (start + PREWARM_BATCH as usize).min(runway.len());
                let batch = &runway[start..end];
                match query.prewarm_parents_for_block_hashes(batch) {
                    Ok(st) if st.blocks > 0 || st.parents_loaded > 0 || st.outs_pinned > 0 => {
                        debug!(
                            "ibd: prewarm tip+{}..+{} blocks={} bodies={} parents={} pins={} warm={}",
                            start + 1,
                            end,
                            st.blocks,
                            st.bodies_loaded,
                            st.parents_loaded,
                            st.outs_pinned,
                            st.already_warm
                        );
                    }
                    Ok(_) => {}
                    Err(e) => {
                        debug!("ibd: prewarm error: {e}");
                    }
                }
                cursor = end as u32;
                // Brief yield so confirm/archive can use the disk.
                std::thread::sleep(Duration::from_millis(5));
            }
            info!("ibd: parent prewarm worker stopped");
        })
        .expect("spawn ibd-parent-prewarm")
}
