//! Scripts stage (pure CPU verify).

use super::*;
use crate::block::{verify_one_script_job, ScriptCheckJob};
use crate::script_pool::{
    fg_has_unclaimed, help_steal, set_script_publisher, start_for_each_owned, OwnedWave,
};
use std::collections::VecDeque;
use std::sync::mpsc::RecvTimeoutError;
use std::thread;
use std::time::Duration;

/// In-flight waves the stage thread will publish while steal is empty.
const SCRIPT_WAVES_MAX: usize = 8;

fn take_script_jobs(
    prepared: &mut [Prepared],
    preverified: &ScriptPreverified,
) -> Vec<ScriptCheckJob> {
    let mut jobs = Vec::new();
    let mut n_skip = 0u64;
    for p in prepared {
        if !p.check_scripts {
            p.jobs.clear();
            continue;
        }
        for job in p.jobs.drain(..) {
            if preverified.contains(&job.txid) {
                n_skip = n_skip.saturating_add(1);
            } else {
                jobs.push(job);
            }
        }
    }
    if n_skip > 0 {
        confirm_phase_stats::SCRIPT_SKIP_MEMPOOL.fetch_add(n_skip, Ordering::Relaxed);
    }
    confirm_phase_stats::SCRIPT_JOBS.fetch_add(jobs.len() as u64, Ordering::Relaxed);
    jobs
}

fn outcome_from(batch: LoadedBatch, t0: Instant) -> ConfirmScriptOutcome {
    let work_ns = t0.elapsed().as_nanos() as u64;
    confirm_phase_stats::SCRIPT_NS.fetch_add(work_ns, Ordering::Relaxed);
    ConfirmScriptOutcome {
        batch: ScriptOkBatch {
            prepared: batch.prepared,
            wire_blocks: batch.wire_blocks,
            batch_parents: batch.batch_parents,
            archive_plan: batch.archive_plan,
        },
        work_ns,
    }
}

struct Inflight {
    batch: LoadedBatch,
    wave: Option<OwnedWave<ScriptCheckJob>>,
    meta: ScriptsBatchMeta,
    t0: Instant,
}

impl Inflight {
    fn start(mut batch: LoadedBatch, mat_ns: u64) -> Result<Self, ConsensusError> {
        let t0 = Instant::now();
        let meta = ScriptsBatchMeta::from_batch(&batch, mat_ns);
        let jobs = take_script_jobs(&mut batch.prepared, &batch.script_preverified);
        let wave = start_for_each_owned(jobs, verify_one_script_job)?;
        Ok(Self {
            batch,
            wave,
            meta,
            t0,
        })
    }

    fn is_complete(&self) -> bool {
        self.wave.as_ref().is_none_or(|w| w.is_complete())
    }

    fn finish(self) -> Result<(ConfirmScriptOutcome, ScriptsBatchMeta), ConsensusError> {
        if let Some(w) = self.wave {
            w.finish()?;
        }
        Ok((outcome_from(self.batch, self.t0), self.meta))
    }
}

pub fn confirm_scripts_phase(batch: LoadedBatch) -> Result<ConfirmScriptOutcome, ConsensusError> {
    Inflight::start(batch, 0)?.finish().map(|(o, _)| o)
}

/// Handle for a scripts stage started on the caller (no coordinator thread).
///
/// IBD uses [`drive_script_waves`]. Tests use this for submit/join overlap.
pub struct ScriptsPhaseHandle {
    state: std::sync::Mutex<HandleState>,
    /// Publisher thread name (test).
    #[cfg(test)]
    phase_thread: std::sync::Arc<std::sync::Mutex<Option<String>>>,
}

enum HandleState {
    Ready(Result<ConfirmScriptOutcome, ConsensusError>),
    Live(Inflight),
    #[cfg(test)]
    Rx(std::sync::mpsc::Receiver<Result<ConfirmScriptOutcome, ConsensusError>>),
}

impl ScriptsPhaseHandle {
    /// Block until the spawned wave finishes (ordered join).
    pub fn join(self) -> Result<ConfirmScriptOutcome, ConsensusError> {
        self.recv_blocking()
    }

    /// Blocking recv without consuming the handle (lookahead join path).
    pub fn recv_blocking(&self) -> Result<ConfirmScriptOutcome, ConsensusError> {
        loop {
            {
                let mut g = self.state.lock().unwrap_or_else(|p| p.into_inner());
                match &mut *g {
                    HandleState::Ready(_) => {
                        let HandleState::Ready(r) = std::mem::replace(
                            &mut *g,
                            HandleState::Ready(Err(ConsensusError::BadBlock(
                                "scripts phase: result taken",
                            ))),
                        ) else {
                            unreachable!();
                        };
                        return r;
                    }
                    HandleState::Live(inf) => {
                        if inf.is_complete() {
                            let HandleState::Live(inf) = std::mem::replace(
                                &mut *g,
                                HandleState::Ready(Err(ConsensusError::BadBlock(
                                    "scripts phase: result taken",
                                ))),
                            ) else {
                                unreachable!();
                            };
                            drop(g);
                            return inf.finish().map(|(o, _)| o);
                        }
                        if let Some(w) = inf.wave.as_ref() {
                            w.wait_complete();
                        }
                    }
                    #[cfg(test)]
                    HandleState::Rx(rx) => {
                        return rx.recv().unwrap_or_else(|_| {
                            Err(ConsensusError::BadBlock(
                                "scripts phase: worker disconnected before result",
                            ))
                        });
                    }
                }
            }
        }
    }

    /// Run `work` on a helper thread (tests). Not a steal worker.
    #[cfg(test)]
    pub fn spawn_fn(
        work: impl FnOnce() -> Result<ConfirmScriptOutcome, ConsensusError> + Send + 'static,
    ) -> Self {
        let (tx, rx) = std::sync::mpsc::sync_channel(1);
        let phase_thread = std::sync::Arc::new(std::sync::Mutex::new(None));
        let slot = std::sync::Arc::clone(&phase_thread);
        let _ = thread::Builder::new()
            .name("scripts-phase-test".into())
            .spawn(move || {
                *slot.lock().unwrap_or_else(|p| p.into_inner()) =
                    Some(thread::current().name().unwrap_or("").to_string());
                let _ = tx.send(work());
            });
        Self {
            state: std::sync::Mutex::new(HandleState::Rx(rx)),
            phase_thread,
        }
    }

    /// Join and return the publisher thread name recorded for **this** handle.
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

    /// Wait up to `timeout` for the wave result.
    pub fn recv_timeout(
        &self,
        timeout: Duration,
    ) -> Result<Result<ConfirmScriptOutcome, ConsensusError>, RecvTimeoutError> {
        let start = Instant::now();
        loop {
            {
                let mut g = self.state.lock().unwrap_or_else(|p| p.into_inner());
                match &mut *g {
                    HandleState::Ready(_) => {
                        let HandleState::Ready(r) = std::mem::replace(
                            &mut *g,
                            HandleState::Ready(Err(ConsensusError::BadBlock(
                                "scripts phase: result taken",
                            ))),
                        ) else {
                            unreachable!();
                        };
                        return Ok(r);
                    }
                    HandleState::Live(inf) => {
                        if inf.is_complete() {
                            let HandleState::Live(inf) = std::mem::replace(
                                &mut *g,
                                HandleState::Ready(Err(ConsensusError::BadBlock(
                                    "scripts phase: result taken",
                                ))),
                            ) else {
                                unreachable!();
                            };
                            drop(g);
                            return Ok(inf.finish().map(|(o, _)| o));
                        }
                    }
                    #[cfg(test)]
                    HandleState::Rx(rx) => {
                        let left = timeout.saturating_sub(start.elapsed());
                        return rx.recv_timeout(left);
                    }
                }
            }
            if start.elapsed() >= timeout {
                return Err(RecvTimeoutError::Timeout);
            }
            thread::sleep(Duration::from_micros(50));
        }
    }
}

/// Publish a wave on **this** thread (steal workers run the jobs).
pub fn confirm_scripts_phase_async(batch: LoadedBatch) -> ScriptsPhaseHandle {
    #[cfg(test)]
    let phase_thread = std::sync::Arc::new(std::sync::Mutex::new(Some(
        thread::current().name().unwrap_or("").to_string(),
    )));
    let state = match Inflight::start(batch, 0) {
        Ok(inf) if inf.is_complete() => HandleState::Ready(inf.finish().map(|(o, _)| o)),
        Ok(inf) => HandleState::Live(inf),
        Err(e) => HandleState::Ready(Err(e)),
    };
    ScriptsPhaseHandle {
        state: std::sync::Mutex::new(state),
        #[cfg(test)]
        phase_thread,
    }
}

/// Join `handle`, invoking `on_poll` so more batches can start before this
/// join returns.
///
/// `on_poll` returns `true` while the caller still wants short `recv_timeout`
/// polls. Once it returns `false`, this **blocks** on `join`.
pub fn join_scripts_polling<F>(
    handle: &ScriptsPhaseHandle,
    poll: Duration,
    mut on_poll: F,
) -> Result<ConfirmScriptOutcome, ConsensusError>
where
    F: FnMut() -> bool,
{
    loop {
        if !on_poll() {
            return handle.recv_blocking();
        }
        match handle.recv_timeout(poll) {
            Ok(r) => return r,
            Err(RecvTimeoutError::Timeout) => {
                continue;
            }
            Err(RecvTimeoutError::Disconnected) => {
                return Err(ConsensusError::BadBlock(
                    "scripts phase: worker disconnected before result",
                ));
            }
        }
    }
}

/// Run script verify for a sequence of loaded batches with feed-ahead.
///
/// Results are returned **in input order** (height-ordered write handoff).
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
        let outcome = join_scripts_polling(&current, Duration::from_micros(200), || {
            if next.is_none() {
                next = iter.next().map(confirm_scripts_phase_async);
            }
            false
        })?;
        out.push(outcome);
        match next.take() {
            Some(h) => current = h,
            None => break,
        }
    }
    Ok(out)
}

/// IBD scripts stage: publish waves from the stage thread, steal-help, in-order
/// write handoff. Starts another `scriptq` batch when the steal list is empty.
pub fn drive_script_waves(
    mat_rx: &std::sync::mpsc::Receiver<(LoadedBatch, u64)>,
    on_ok: impl FnMut(ConfirmScriptOutcome, ScriptsBatchMeta) -> bool,
    on_err: impl FnMut(ConsensusError, ScriptsBatchMeta) -> bool,
    should_stop: impl FnMut() -> bool,
) {
    drive_script_waves_with(mat_rx, |_, _| {}, on_ok, on_err, should_stop);
}

/// [`drive_script_waves`] with `on_take` when a batch leaves `scriptq`.
///
/// `on_take` receives the recv-wait before this batch was taken (zero when
/// `try_recv` hit a ready item).
pub fn drive_script_waves_with(
    mat_rx: &std::sync::mpsc::Receiver<(LoadedBatch, u64)>,
    mut on_take: impl FnMut(&LoadedBatch, Duration),
    mut on_ok: impl FnMut(ConfirmScriptOutcome, ScriptsBatchMeta) -> bool,
    mut on_err: impl FnMut(ConsensusError, ScriptsBatchMeta) -> bool,
    mut should_stop: impl FnMut() -> bool,
) {
    struct ClearPublisher;
    impl Drop for ClearPublisher {
        fn drop(&mut self) {
            set_script_publisher(None);
        }
    }
    set_script_publisher(Some(thread::current()));
    let _clear = ClearPublisher;
    let mut inflight: VecDeque<Inflight> = VecDeque::new();
    loop {
        if should_stop() {
            break;
        }
        while inflight.front().is_some_and(Inflight::is_complete) {
            let front = inflight.pop_front().expect("front");
            let meta_err = front.meta.clone();
            match front.finish() {
                Ok((ok, meta)) => {
                    if !on_ok(ok, meta) {
                        return;
                    }
                }
                Err(e) => {
                    if !on_err(e, meta_err) {
                        return;
                    }
                }
            }
        }
        if !fg_has_unclaimed() && inflight.len() < SCRIPT_WAVES_MAX {
            let t_recv = Instant::now();
            match mat_rx.try_recv() {
                Ok((batch, mat_ns)) => {
                    on_take(&batch, t_recv.elapsed());
                    match Inflight::start(batch, mat_ns) {
                        Ok(f) => inflight.push_back(f),
                        Err(e) => {
                            let meta = ScriptsBatchMeta {
                                n: 0,
                                first_h: 0,
                                heights_hashes: Vec::new(),
                                mat_ns,
                                t0: Instant::now(),
                            };
                            if !on_err(e, meta) {
                                return;
                            }
                        }
                    }
                    continue;
                }
                Err(std::sync::mpsc::TryRecvError::Empty) => {}
                Err(std::sync::mpsc::TryRecvError::Disconnected) => {
                    if inflight.is_empty() {
                        break;
                    }
                }
            }
        }
        if inflight.is_empty() {
            let t_recv = Instant::now();
            match mat_rx.recv() {
                Ok((batch, mat_ns)) => {
                    on_take(&batch, t_recv.elapsed());
                    match Inflight::start(batch, mat_ns) {
                        Ok(f) => inflight.push_back(f),
                        Err(e) => {
                            let meta = ScriptsBatchMeta {
                                n: 0,
                                first_h: 0,
                                heights_hashes: Vec::new(),
                                mat_ns,
                                t0: Instant::now(),
                            };
                            if !on_err(e, meta) {
                                break;
                            }
                        }
                    }
                }
                Err(_) => break,
            }
            continue;
        }
        if help_steal() {
            continue;
        }
        thread::park_timeout(Duration::from_millis(1));
    }
}

/// Drive scripts from load→scripts channel (production IBD + tests).
pub fn scripts_stage_from_load_channel(
    mat_rx: &std::sync::mpsc::Receiver<(LoadedBatch, u64)>,
    on_ok: impl FnMut(ConfirmScriptOutcome, ScriptsBatchMeta) -> bool,
    on_err: impl FnMut(ConsensusError, ScriptsBatchMeta) -> bool,
    should_stop: impl FnMut() -> bool,
) {
    drive_script_waves(mat_rx, on_ok, on_err, should_stop);
}

/// Same loop as [`scripts_stage_from_load_channel`], with an injectable start
/// (tests hold the first wave locally).
#[cfg(test)]
pub fn scripts_stage_from_load_channel_with(
    mat_rx: &std::sync::mpsc::Receiver<(LoadedBatch, u64)>,
    mut start: impl FnMut(LoadedBatch, u64) -> (ScriptsPhaseHandle, ScriptsBatchMeta),
    mut on_ok: impl FnMut(ConfirmScriptOutcome, ScriptsBatchMeta) -> bool,
    mut on_err: impl FnMut(ConsensusError, ScriptsBatchMeta) -> bool,
    mut should_stop: impl FnMut() -> bool,
) {
    let mut current: Option<(ScriptsPhaseHandle, ScriptsBatchMeta)> = None;
    let mut lookahead: Option<(ScriptsPhaseHandle, ScriptsBatchMeta)> = None;

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
        let result = join_scripts_polling(&handle, Duration::from_micros(200), || {
            if lookahead.is_some() || should_stop() {
                return false;
            }
            match mat_rx.try_recv() {
                Ok((batch, mat_ns)) => {
                    lookahead = Some(start(batch, mat_ns));
                    false
                }
                Err(std::sync::mpsc::TryRecvError::Empty) => true,
                Err(std::sync::mpsc::TryRecvError::Disconnected) => false,
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
                if let Some((h, _)) = lookahead.take() {
                    let _ = h.join();
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
