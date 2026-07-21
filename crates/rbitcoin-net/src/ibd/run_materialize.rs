//! Idle-priority materialize of catch-up sorted runs into open-hash heads.
//!
//! Driven by [`rbitcoin_query::run_materialize_control`] hysteresis: when archive
//! lead is large, **peer getdata stops**; once inflight is 0 this worker applies
//! **one run at a time** (point → tx → SH) with paced per-shard head inserts.
//! The archive writer is never paused.

use rbitcoin_log::{debug, info, warn};
use rbitcoin_query::{run_materialize_control, Query};
use rbitcoin_store::try_set_io_idle;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::Duration;

/// Brief yield after a run so confirm can run; keep small — durability is deferred.
const AFTER_RUN: Duration = Duration::from_millis(5);
const IDLE: Duration = Duration::from_millis(200);

/// RAII: drop signals stop and joins.
pub struct RunMaterializeWorker {
    stop: Arc<AtomicBool>,
    join: Option<JoinHandle<()>>,
}

impl RunMaterializeWorker {
    pub fn spawn(query: Arc<Query>) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let stop_t = Arc::clone(&stop);
        let join = std::thread::Builder::new()
            .name("ibd-run-materialize".into())
            .spawn(move || {
                try_set_io_idle();
                info!(
                    "ibd: run materialize worker ON (start_lead={} stop_lead={}; pause peer fetch not writer; env RBITCOIN_RUN_MATERIALIZE_*; idle IO)",
                    run_materialize_control::start_lead_from_env(),
                    run_materialize_control::stop_lead_from_env(),
                );
                let mut logged = false;
                while !stop_t.load(Ordering::Relaxed) {
                    if !run_materialize_control::should_materialize() {
                        logged = false;
                        std::thread::sleep(IDLE);
                        continue;
                    }
                    if !logged && run_materialize_control::should_log_mode() {
                        logged = true;
                        let (t, p, sh) = query.index_run_counts();
                        info!(
                            "ibd: run materialize active lead={} mode={} inflight={} runs t={t}/p={p}/sh={sh} pause_fetch={}",
                            run_materialize_control::arch_lead(),
                            run_materialize_control::mode_label(),
                            run_materialize_control::peer_inflight(),
                            run_materialize_control::should_pause_peer_fetch(),
                        );
                    }
                    match query.materialize_one_index_run() {
                        Ok(None) => {
                            // Nothing left this tick.
                            std::thread::sleep(IDLE);
                        }
                        Ok(Some(_step)) => {
                            // Per-run DEBUG is emitted inside materialize_oldest_run.
                            std::thread::sleep(AFTER_RUN);
                        }
                        Err(e) => {
                            warn!("ibd: run materialize failed: {e}");
                            std::thread::sleep(Duration::from_millis(500));
                        }
                    }
                }
                debug!("ibd: run materialize worker stopped");
            })
            .expect("spawn ibd-run-materialize");
        Self {
            stop,
            join: Some(join),
        }
    }

    pub fn request_stop(&self) {
        self.stop.store(true, Ordering::Relaxed);
    }
}

impl Drop for RunMaterializeWorker {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(j) = self.join.take() {
            let _ = j.join();
        }
    }
}
