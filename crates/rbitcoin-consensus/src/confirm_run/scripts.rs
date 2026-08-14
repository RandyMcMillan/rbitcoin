//! Scripts stage (pure CPU verify).

use super::*;

pub fn confirm_scripts_phase(
    mut batch: LoadedBatch,
) -> Result<ConfirmScriptOutcome, ConsensusError> {
    // Test-only: hold the first in-flight wave until a second async submit is
    // observed (proves production feed-ahead claims during join).
    scripts_feed_test_sync::on_phase_enter();
    let t_work = Instant::now();
    script_wave(&batch.prepared, &batch.script_preverified)?;
    for p in &mut batch.prepared {
        p.jobs.clear();
        p.jobs.shrink_to_fit();
    }
    let work_ns = t_work.elapsed().as_nanos() as u64;
    Ok(ConfirmScriptOutcome {
        batch: ScriptOkBatch {
            prepared: batch.prepared,
            wire_blocks: batch.wire_blocks,
            batch_parents: batch.batch_parents,
            archive_plan: batch.archive_plan,
        },
        work_ns,
    })
}

/// Handle for a scripts stage running on a coordinator (non-blocking start).
///
/// IBD scripts OS thread starts the next batch with [`confirm_scripts_phase_async`]
/// **while** joining the prior (poll claim + short timeouts), so the script
/// workers stay fed even when load→scripts depth is 1.
pub struct ScriptsPhaseHandle {
    rx: std::sync::mpsc::Receiver<Result<ConfirmScriptOutcome, ConsensusError>>,
    /// Per-submit thread name (test). Not the process-global last-writer slot.
    #[cfg(test)]
    phase_thread: std::sync::Arc<std::sync::Mutex<Option<String>>>,
}

impl ScriptsPhaseHandle {
    /// Block until the spawned wave finishes (ordered join).
    pub fn join(self) -> Result<ConfirmScriptOutcome, ConsensusError> {
        self.rx.recv().unwrap_or_else(|_| {
            Err(ConsensusError::BadBlock(
                "scripts phase: worker disconnected before result",
            ))
        })
    }

    /// Join and return the coordinator thread name recorded for **this** handle.
    #[cfg(test)]
    pub fn join_with_phase_thread(self) -> Result<(ConfirmScriptOutcome, String), ConsensusError> {
        let slot = std::sync::Arc::clone(&self.phase_thread);
        let out = self.join()?;
        let name = slot
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clone()
            .unwrap_or_default();
        Ok((out, name))
    }

    /// Wait up to `timeout` for the wave result (production feed-ahead polls
    /// load→scripts `try_recv` between timeouts so N+1 can start mid-join).
    pub fn recv_timeout(
        &self,
        timeout: std::time::Duration,
    ) -> Result<Result<ConfirmScriptOutcome, ConsensusError>, std::sync::mpsc::RecvTimeoutError>
    {
        self.rx.recv_timeout(timeout)
    }
}

/// Submit [`confirm_scripts_phase`] on a detached worker thread without
/// blocking the caller.
///
/// The OS scripts thread must keep claiming N+1 **while** waiting on N’s
/// [`ScriptsPhaseHandle::recv_timeout`] (not only once before a blocking join).
pub fn confirm_scripts_phase_async(batch: LoadedBatch) -> ScriptsPhaseHandle {
    scripts_feed_test_sync::on_async_submit();
    let (tx, rx) = std::sync::mpsc::sync_channel(1);
    #[cfg(test)]
    let phase_thread = std::sync::Arc::new(std::sync::Mutex::new(None));
    #[cfg(test)]
    let slot = std::sync::Arc::clone(&phase_thread);
    crate::script_pool::spawn_coordinator(move || {
        #[cfg(test)]
        {
            *slot.lock().unwrap_or_else(|p| p.into_inner()) =
                Some(std::thread::current().name().unwrap_or("").to_string());
        }
        let r = confirm_scripts_phase(batch);
        let _ = tx.send(r);
    });
    ScriptsPhaseHandle {
        rx,
        #[cfg(test)]
        phase_thread,
    }
}

/// Join `handle`, repeatedly invoking `on_poll` (e.g. load `try_recv` + async
/// submit) so a second ready batch reaches a coordinator **before** this join returns.
///
/// This is the production feed-ahead primitive used under depth-1 channels.
pub fn join_scripts_polling<F>(
    handle: &ScriptsPhaseHandle,
    poll: std::time::Duration,
    mut on_poll: F,
) -> Result<ConfirmScriptOutcome, ConsensusError>
where
    F: FnMut(),
{
    loop {
        on_poll();
        match handle.recv_timeout(poll) {
            Ok(r) => return r,
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => continue,
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                return Err(ConsensusError::BadBlock(
                    "scripts phase: worker disconnected before result",
                ));
            }
        }
    }
}

/// Run script verify for a sequence of loaded batches with **one-batch feed-ahead**.
///
/// While batch *i* is verifying on a coordinator, batch *i+1* (if present) is
/// already submitted so the pool is not idle solely between sequential claim walls.
/// Results are returned **in input order** (height-ordered write handoff).
///
/// Single-batch input is fine (no second submit).
pub fn confirm_scripts_feed_ahead(
    batches: impl IntoIterator<Item = LoadedBatch>,
) -> Result<Vec<ConfirmScriptOutcome>, ConsensusError> {
    let mut iter = batches.into_iter();
    let Some(first) = iter.next() else {
        return Ok(Vec::new());
    };
    let mut current = confirm_scripts_phase_async(first);
    let mut out = Vec::new();
    let mut next = iter.next().map(confirm_scripts_phase_async);
    loop {
        // Keep offering the following batch while joining current.
        let outcome =
            join_scripts_polling(&current, std::time::Duration::from_micros(200), || {
                if next.is_none() {
                    next = iter.next().map(confirm_scripts_phase_async);
                }
            })?;
        out.push(outcome);
        match next.take() {
            Some(h) => current = h,
            None => break,
        }
    }
    Ok(out)
}

/// Drive the **production** scripts claim/feed-ahead pattern from a load→scripts
/// channel (including depth 1): blocking claim for current, then
/// [`join_scripts_polling`] with `try_recv` to start N+1 mid-join.
///
/// Used by the IBD scripts OS thread and unit tests that exercise real
/// `sync_channel(1)` timing.
pub fn scripts_stage_from_load_channel(
    mat_rx: &std::sync::mpsc::Receiver<(LoadedBatch, u64)>,
    mut on_ok: impl FnMut(ConfirmScriptOutcome, ScriptsBatchMeta) -> bool,
    mut on_err: impl FnMut(ConsensusError, ScriptsBatchMeta) -> bool,
    mut should_stop: impl FnMut() -> bool,
) {
    let mut current: Option<(ScriptsPhaseHandle, ScriptsBatchMeta)> = None;
    let mut lookahead: Option<(ScriptsPhaseHandle, ScriptsBatchMeta)> = None;

    let start = |batch: LoadedBatch, mat_ns: u64| -> (ScriptsPhaseHandle, ScriptsBatchMeta) {
        let meta = ScriptsBatchMeta::from_batch(&batch, mat_ns);
        let handle = confirm_scripts_phase_async(batch);
        (handle, meta)
    };

    loop {
        if should_stop() {
            break;
        }
        if current.is_none() {
            let (batch, mat_ns) = match mat_rx.recv() {
                Ok(x) => x,
                Err(_) => break,
            };
            if should_stop() {
                break;
            }
            current = Some(start(batch, mat_ns));
        }
        let (handle, meta) = match current.take() {
            Some(c) => c,
            None => break,
        };
        let result = join_scripts_polling(&handle, std::time::Duration::from_micros(200), || {
            if lookahead.is_none() {
                if let Ok((batch, mat_ns)) = mat_rx.try_recv() {
                    if !should_stop() {
                        lookahead = Some(start(batch, mat_ns));
                    }
                }
            }
        });
        match result {
            Ok(outcome) => {
                let cont = on_ok(outcome, meta);
                if !cont {
                    break;
                }
                current = lookahead.take();
            }
            Err(e) => {
                let cont = on_err(e, meta);
                // Drop later batch without treating it as write-ready.
                if let Some((h, m)) = lookahead.take() {
                    let _ = h.join();
                    let _ = m; // caller finishes heights via on_err if needed
                }
                if !cont {
                    break;
                }
                current = None;
            }
        }
    }
    if let Some((h, _)) = current.take() {
        let _ = h.join();
    }
    if let Some((h, _)) = lookahead.take() {
        let _ = h.join();
    }
}

/// Metadata retained across async scripts submit → ordered write handoff.
#[derive(Clone, Debug)]
pub struct ScriptsBatchMeta {
    pub n: usize,
    pub first_h: u32,
    pub heights_hashes: Vec<(u32, [u8; 32])>,
    pub mat_ns: u64,
    pub t0: Instant,
}

impl ScriptsBatchMeta {
    pub fn from_batch(batch: &LoadedBatch, mat_ns: u64) -> Self {
        let heights_hashes = batch.heights_hashes();
        let first_h = heights_hashes.first().map(|(h, _)| *h).unwrap_or(0);
        Self {
            n: batch.len(),
            first_h,
            heights_hashes,
            mat_ns,
            t0: Instant::now(),
        }
    }
}

/// Test-only sync so unit tests can prove N+1 was submitted while N’s wave is still open.
pub mod scripts_feed_test_sync {
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
    use std::time::{Duration, Instant};

    static SUBMIT_COUNT: AtomicU64 = AtomicU64::new(0);
    static HOLD_FIRST: AtomicBool = AtomicBool::new(false);
    static FIRST_ENTERED: AtomicBool = AtomicBool::new(false);

    /// Reset counters (call at start of each feed-ahead timing test).
    pub fn reset() {
        SUBMIT_COUNT.store(0, Ordering::SeqCst);
        HOLD_FIRST.store(false, Ordering::SeqCst);
        FIRST_ENTERED.store(false, Ordering::SeqCst);
    }

    /// When true, the first [`super::confirm_scripts_phase`] waits until
    /// [`submit_count`] ≥ 2 (second async submit happened mid-wave).
    pub fn set_hold_first_until_second_submit(hold: bool) {
        HOLD_FIRST.store(hold, Ordering::SeqCst);
        FIRST_ENTERED.store(false, Ordering::SeqCst);
    }

    pub fn submit_count() -> u64 {
        SUBMIT_COUNT.load(Ordering::SeqCst)
    }

    pub(super) fn on_async_submit() {
        SUBMIT_COUNT.fetch_add(1, Ordering::SeqCst);
    }

    pub(super) fn on_phase_enter() {
        if !HOLD_FIRST.load(Ordering::SeqCst) {
            return;
        }
        // Only the first wave holds.
        if FIRST_ENTERED.swap(true, Ordering::SeqCst) {
            return;
        }
        let deadline = Instant::now() + Duration::from_secs(5);
        while submit_count() < 2 {
            if Instant::now() > deadline {
                // Avoid hanging the suite if feed-ahead is broken.
                break;
            }
            std::thread::sleep(Duration::from_millis(1));
        }
    }
}

/// LOAD + SCRIPTS in one call (tests / tip path / ChainHub compat).
///
/// Work is full load (Class A + parents) + pure scripts.
pub fn confirm_script_phase(
    query: &Query,
    params: &ChainParams,
    milestone: Milestone,
    blocks: &[(Height, [u8; 32])],
) -> Result<ConfirmScriptOutcome, ConsensusError> {
    let mat = confirm_load_phase(query, params, milestone, blocks)?;
    let mat_ns = mat.work_ns;
    let mut ok = confirm_scripts_phase(mat.batch)?;
    ok.work_ns = ok.work_ns.saturating_add(mat_ns);
    Ok(ok)
}
