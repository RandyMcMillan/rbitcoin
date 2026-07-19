//! Background budgeted head-overlay spill (A.4).
//!
//! One short disk slice on `point.head` / `tx.head`, then sleep so confirm can
//! fault Class A pages. Soft-cap auto-spill is skipped mid-confirm; this worker
//! still drains past soft/2 under defer so RAM stays bounded without multi-min
//! storms on the archive writer.

use rbitcoin_log::{debug, warn};
use rbitcoin_query::Query;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::Duration;

/// RAII handle: drop signals stop and joins the worker.
pub struct HeadSpillWorker {
    stop: Arc<AtomicBool>,
    join: Option<JoinHandle<()>>,
}

impl HeadSpillWorker {
    pub fn spawn(query: Arc<Query>) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let stop_t = Arc::clone(&stop);
        let join = std::thread::Builder::new()
            .name("ibd-head-spill".into())
            .spawn(move || {
                debug!(
                    "ibd: head spill worker started (chunk≈{}; RBITCOIN_HEAD_SPILL_CHUNK)",
                    rbitcoin_store::spill_chunk_size()
                );
                while !stop_t.load(Ordering::Relaxed) {
                    let (p, t) = match query.spill_heads_step_if_needed() {
                        Ok(v) => v,
                        Err(e) => {
                            warn!("ibd: head spill worker: {e}");
                            std::thread::sleep(Duration::from_millis(100));
                            continue;
                        }
                    };
                    if p + t > 0 {
                        // Yield disk/page cache to confirm between slices.
                        std::thread::sleep(Duration::from_millis(5));
                    } else {
                        std::thread::sleep(Duration::from_millis(50));
                    }
                }
                debug!("ibd: head spill worker stopped");
            })
            .expect("spawn ibd-head-spill");
        Self {
            stop,
            join: Some(join),
        }
    }
}

impl Drop for HeadSpillWorker {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(j) = self.join.take() {
            let _ = j.join();
        }
    }
}
