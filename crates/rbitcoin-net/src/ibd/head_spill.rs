//! Background budgeted head-overlay spill (A.4).
//!
//! Quiet by default: only steps when the overlay is past soft/2 (or soft under
//! confirm), with long sleeps so page-cache storms do not freeze the desktop.
//! Thread is set to Linux IOPRIO_CLASS_IDLE when available.

use rbitcoin_log::{debug, warn};
use rbitcoin_query::Query;
use rbitcoin_store::try_set_io_idle;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::Duration;

/// Sleep after a productive spill slice (host UI / confirm breathe).
const AFTER_SPILL: Duration = Duration::from_millis(80);
/// Sleep when nothing to do.
const IDLE: Duration = Duration::from_millis(250);

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
                try_set_io_idle();
                debug!(
                    "ibd: head spill worker started (chunk≈{}; RBITCOIN_HEAD_SPILL_CHUNK; idle IO prio)",
                    rbitcoin_store::spill_chunk_size()
                );
                while !stop_t.load(Ordering::Relaxed) {
                    let (p, t) = match query.spill_heads_step_if_needed() {
                        Ok(v) => v,
                        Err(e) => {
                            warn!("ibd: head spill worker: {e}");
                            std::thread::sleep(Duration::from_millis(200));
                            continue;
                        }
                    };
                    if p + t > 0 {
                        std::thread::sleep(AFTER_SPILL);
                    } else {
                        std::thread::sleep(IDLE);
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

    /// Signal stop without joining (caller will drop / join later).
    pub fn request_stop(&self) {
        self.stop.store(true, Ordering::Relaxed);
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
