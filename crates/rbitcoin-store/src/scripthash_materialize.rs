//! Sliced k-way SH tip materialize: one worker per prefix shard, ordered publish.

use crate::error::StoreError;
use crate::file::ensure_nofile_budget_at_least;
use crate::scripthash::{ColdProgress, ScriptHashTable, ShBodyLayout, ShShardPack};
use crate::sorted_run::{for_each_merged_rec_shard, shard_record_starts, SortedRunPath};
use rbitcoin_primitives::Fk;
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

const MATERIALIZE_STATUS_INTERVAL: Duration = Duration::from_secs(10);

fn status_interval_due(last: Option<Instant>, now: Instant, interval: Duration) -> bool {
    match last {
        None => true,
        Some(t) => now.saturating_duration_since(t) >= interval,
    }
}

/// Pack at most `cap` shards past the next unpublished prefix shard.
fn pack_ahead_allowed(shard: usize, next_publish: usize, cap: usize) -> bool {
    shard < next_publish.saturating_add(cap.max(1))
}

/// Global pack/publish counters. One observer thread samples these.
struct MaterializeProgress {
    recs_packed: AtomicU64,
    keys_packed: AtomicU64,
    shards_published: AtomicU32,
    creates_published: AtomicU64,
    stop: AtomicBool,
    complete: AtomicBool,
    wake: Condvar,
    wake_mu: Mutex<()>,
}

struct StatusSnapshot {
    keys: u64,
    creates: u64,
    pending: u64,
    shards: u32,
    pct: f64,
}

impl MaterializeProgress {
    fn new() -> Self {
        Self {
            recs_packed: AtomicU64::new(0),
            keys_packed: AtomicU64::new(0),
            shards_published: AtomicU32::new(0),
            creates_published: AtomicU64::new(0),
            stop: AtomicBool::new(false),
            complete: AtomicBool::new(false),
            wake: Condvar::new(),
            wake_mu: Mutex::new(()),
        }
    }

    fn snapshot(&self, total_recs: u64, done: bool) -> StatusSnapshot {
        let creates = self.recs_packed.load(Ordering::Relaxed);
        let published = self.creates_published.load(Ordering::Relaxed);
        let pct = if done {
            100.0
        } else if total_recs > 0 {
            (100.0 * creates as f64 / total_recs as f64).clamp(0.0, 99.9)
        } else {
            0.0
        };
        StatusSnapshot {
            keys: self.keys_packed.load(Ordering::Relaxed),
            creates,
            pending: creates.saturating_sub(published),
            shards: self.shards_published.load(Ordering::Relaxed),
            pct,
        }
    }
}

fn log_materialize_status(
    last_log: &mut Option<Instant>,
    snap: &StatusSnapshot,
    n_shards: usize,
    t0: Instant,
) {
    *last_log = Some(Instant::now());
    let elapsed = t0.elapsed();
    let secs = elapsed.as_secs_f64().max(1e-3);
    let recs_per_s = snap.creates as f64 / secs;
    rbitcoin_log::info!(
        "node: scripthash materialize status keys≈{} creates≈{} pending≈{} \
         pct≈{:.1}% shards={}/{} rate≈{:.0}creates/s elapsed={elapsed:?}",
        snap.keys,
        snap.creates,
        snap.pending,
        snap.pct,
        snap.shards,
        n_shards,
        recs_per_s,
    );
}

struct StopOnDrop<'a>(&'a MaterializeProgress);

impl Drop for StopOnDrop<'_> {
    fn drop(&mut self) {
        self.0.stop.store(true, Ordering::Release);
        self.0.wake.notify_all();
    }
}

/// Result of [`materialize_sh_shards`].
#[derive(Debug, Clone, Copy)]
pub struct ShShardMaterialize {
    pub creates: u64,
    pub keys: u64,
    pub max_fk: u64,
    pub body_flush_ns: u64,
    pub head_fill_ns: u64,
}

fn decode_sh_run_rec(rec: &[u8]) -> Result<([u8; 32], Fk), StoreError> {
    if rec.len() < 40 {
        return Err(StoreError::Corrupt("sh run short record in shard merge"));
    }
    let mut sh = [0u8; 32];
    sh.copy_from_slice(&rec[..32]);
    let fk = Fk(u64::from_le_bytes(rec[32..40].try_into().unwrap()));
    Ok((sh, fk))
}

fn resolve_workers(
    requested: usize,
    n_shards: usize,
    n_runs: usize,
    layout: ShBodyLayout,
) -> usize {
    let n_shards = n_shards.max(1);
    let mut workers = requested.max(1).min(n_shards);
    let k = n_runs.max(1);
    let want = (workers.saturating_mul(k).saturating_add(64)) as u64;
    let (soft, _) = ensure_nofile_budget_at_least(want);
    if soft > 0 && (soft as usize) < want as usize {
        let clamped = (soft as usize / k).max(1).min(n_shards);
        if clamped < workers {
            rbitcoin_log::warn!(
                "store: scripthash shard workers clamped {workers}→{clamped} \
                 (nofile soft={soft} runs={k} fds≈{})",
                workers.saturating_mul(k)
            );
            workers = clamped;
        }
    }
    if layout == ShBodyLayout::Shared && workers > 1 {
        workers = 1;
    }
    workers
}

fn pack_shard(
    table: &ScriptHashTable,
    inputs: &[SortedRunPath],
    cuts: &[Vec<u64>],
    shard: usize,
    cancel: Option<&AtomicBool>,
    progress: &MaterializeProgress,
) -> Result<ShShardPack, StoreError> {
    let mut session = table.pack_shard_session(shard)?;
    for_each_merged_rec_shard(inputs, cuts, shard, false, |rec| {
        if cancel.map(|c| c.load(Ordering::Relaxed)).unwrap_or(false) {
            return Err(StoreError::Cancelled("scripthash shard pack"));
        }
        let (sh, fk) = decode_sh_run_rec(rec)?;
        if fk.is_null() {
            return Ok(());
        }
        session.push_sorted_fk(sh, fk)?;
        progress.recs_packed.fetch_add(1, Ordering::Relaxed);
        Ok(())
    })?;
    let pack = session.finish_pack()?;
    progress.keys_packed.fetch_add(pack.keys, Ordering::Relaxed);
    Ok(pack)
}

fn publish_next(
    table: &ScriptHashTable,
    shard: usize,
    pack: ShShardPack,
    global_bump: u64,
    live_keys: &mut u64,
    live_creates: &mut u64,
    max_fk: &mut u64,
    progress: &MaterializeProgress,
) -> Result<u64, StoreError> {
    *max_fk = (*max_fk).max(pack.max_fk);
    *live_keys = live_keys.saturating_add(pack.keys);
    *live_creates = live_creates.saturating_add(pack.creates);
    let creates = pack.creates;
    let new_bump = if pack.recs.is_empty() && pack.creates == 0 {
        global_bump
    } else {
        table.publish_packed_shard(shard, pack)?
    };
    ColdProgress {
        next_shard: (shard as u32).saturating_add(1),
        body_bump: new_bump,
        live_count: *live_creates,
        keys_written: *live_keys,
    }
    .store(table.store_dir())?;
    progress
        .creates_published
        .fetch_add(creates, Ordering::Relaxed);
    progress.shards_published.fetch_add(1, Ordering::Relaxed);
    Ok(new_bump)
}

/// Pack prefix shards in parallel; publish body + `scripthash.head/NN` in shard order.
pub fn materialize_sh_shards(
    table: &ScriptHashTable,
    inputs: &[SortedRunPath],
    resume_from: usize,
    workers: usize,
    cancel: Option<&AtomicBool>,
) -> Result<ShShardMaterialize, StoreError> {
    let n_shards = table.head_shard_count().max(1);
    if resume_from >= n_shards {
        return Ok(ShShardMaterialize {
            creates: table.entry_count(),
            keys: 0,
            max_fk: 0,
            body_flush_ns: 0,
            head_fill_ns: 0,
        });
    }
    let cuts: Vec<Vec<u64>> = inputs
        .iter()
        .map(|r| shard_record_starts(r, n_shards))
        .collect::<Result<Vec<_>, _>>()?;
    let workers = resolve_workers(
        workers,
        n_shards - resume_from,
        inputs.len(),
        table.body_layout(),
    );
    rbitcoin_log::info!(
        "store: scripthash shard-kway start resume_from={resume_from} n_shards={n_shards} \
         workers={workers} runs={} fds≈{}",
        inputs.len(),
        workers.saturating_mul(inputs.len().max(1))
    );
    let t0 = Instant::now();
    let progress = MaterializeProgress::new();
    let total_recs: u64 = inputs.iter().map(|r| r.count).sum();
    let prior = ColdProgress::load(table.store_dir()).ok().flatten();
    let mut bump = table.alloc_bump();
    let mut live_creates = prior
        .as_ref()
        .map(|p| p.live_count)
        .unwrap_or_else(|| table.entry_count());
    let mut live_keys = prior.as_ref().map(|p| p.keys_written).unwrap_or(0);
    let mut max_fk = 0u64;

    let out = std::thread::scope(|scope| {
        let progress = &progress;
        let _stop = StopOnDrop(progress);
        scope.spawn(move || {
            let mut last = None;
            loop {
                let stop = progress.stop.load(Ordering::Relaxed);
                if status_interval_due(last, Instant::now(), MATERIALIZE_STATUS_INTERVAL) || stop {
                    log_materialize_status(
                        &mut last,
                        &progress.snapshot(
                            total_recs,
                            stop && progress.complete.load(Ordering::Relaxed),
                        ),
                        n_shards,
                        t0,
                    );
                }
                if stop {
                    break;
                }
                let g = progress.wake_mu.lock().unwrap();
                if progress.stop.load(Ordering::Relaxed) {
                    continue;
                }
                let wait = last
                    .map(|t| MATERIALIZE_STATUS_INTERVAL.saturating_sub(t.elapsed()))
                    .unwrap_or(MATERIALIZE_STATUS_INTERVAL);
                let _ = progress.wake.wait_timeout(g, wait);
            }
        });

        if workers <= 1 {
            for shard in resume_from..n_shards {
                if cancel.map(|c| c.load(Ordering::Relaxed)).unwrap_or(false) {
                    return Err(StoreError::Cancelled("scripthash shard pack"));
                }
                let pack = pack_shard(table, inputs, &cuts, shard, cancel, progress)?;
                bump = publish_next(
                    table,
                    shard,
                    pack,
                    bump,
                    &mut live_keys,
                    &mut live_creates,
                    &mut max_fk,
                    progress,
                )?;
            }
        } else {
            let jobs: VecDeque<usize> = (resume_from..n_shards).collect();
            let shared = Arc::new(ShardPool {
                jobs: Mutex::new(jobs),
                done: Mutex::new(HashMap::new()),
                err: Mutex::new(None),
                cv: Condvar::new(),
                next_publish: AtomicUsize::new(resume_from),
                inflight_cap: workers,
            });
            for _ in 0..workers {
                let shared = Arc::clone(&shared);
                let cuts = &cuts;
                scope.spawn(move || loop {
                    match shared.take_pack_job(cancel) {
                        Ok(None) => break,
                        Ok(Some(shard)) => {
                            match pack_shard(table, inputs, cuts, shard, cancel, progress) {
                                Ok(pack) => {
                                    shared.done.lock().unwrap().insert(shard, pack);
                                    shared.cv.notify_all();
                                }
                                Err(e) => {
                                    *shared.err.lock().unwrap() = Some(e);
                                    shared.cv.notify_all();
                                    break;
                                }
                            }
                        }
                        Err(e) => {
                            let mut g = shared.err.lock().unwrap();
                            if g.is_none() {
                                *g = Some(e);
                            }
                            shared.cv.notify_all();
                            break;
                        }
                    }
                });
            }

            for shard in resume_from..n_shards {
                let pack = loop {
                    if let Some(e) = shared.err.lock().unwrap().take() {
                        return Err(e);
                    }
                    if cancel.map(|c| c.load(Ordering::Relaxed)).unwrap_or(false) {
                        return Err(StoreError::Cancelled("scripthash shard publish"));
                    }
                    if let Some(p) = shared.done.lock().unwrap().remove(&shard) {
                        break p;
                    }
                    let g = shared.done.lock().unwrap();
                    let (_g, _) = shared
                        .cv
                        .wait_timeout(g, Duration::from_millis(100))
                        .unwrap();
                };
                bump = publish_next(
                    table,
                    shard,
                    pack,
                    bump,
                    &mut live_keys,
                    &mut live_creates,
                    &mut max_fk,
                    progress,
                )?;
                shared
                    .next_publish
                    .store(shard.saturating_add(1), Ordering::Release);
                shared.cv.notify_all();
            }
        }

        progress.complete.store(true, Ordering::Release);
        Ok(ShShardMaterialize {
            creates: live_creates,
            keys: live_keys,
            max_fk,
            body_flush_ns: 0,
            head_fill_ns: t0.elapsed().as_nanos() as u64,
        })
    })?;
    Ok(out)
}

struct ShardPool {
    jobs: Mutex<VecDeque<usize>>,
    done: Mutex<HashMap<usize, ShShardPack>>,
    err: Mutex<Option<StoreError>>,
    cv: Condvar,
    next_publish: AtomicUsize,
    inflight_cap: usize,
}

impl ShardPool {
    fn take_pack_job(&self, cancel: Option<&AtomicBool>) -> Result<Option<usize>, StoreError> {
        let mut q = self.jobs.lock().unwrap();
        loop {
            if cancel.map(|c| c.load(Ordering::Relaxed)).unwrap_or(false) {
                return Err(StoreError::Cancelled("scripthash shard pack"));
            }
            if self.err.lock().unwrap().is_some() {
                return Ok(None);
            }
            match q.front().copied() {
                None => return Ok(None),
                Some(shard)
                    if pack_ahead_allowed(
                        shard,
                        self.next_publish.load(Ordering::Acquire),
                        self.inflight_cap,
                    ) =>
                {
                    return Ok(q.pop_front());
                }
                Some(_) => {
                    let (g, _) = self.cv.wait_timeout(q, Duration::from_millis(100)).unwrap();
                    q = g;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn materialize_status_interval_due_is_wall_only() {
        let interval = Duration::from_secs(10);
        let t0 = Instant::now();
        assert!(
            status_interval_due(None, t0, interval),
            "first tick must emit"
        );
        assert!(
            !status_interval_due(Some(t0), t0, interval),
            "must not emit before the wall interval"
        );
        assert!(
            status_interval_due(Some(t0), t0 + interval, interval),
            "emit when the wall interval has elapsed"
        );
    }

    #[test]
    fn materialize_status_snapshot_is_global() {
        let p = MaterializeProgress::new();
        p.recs_packed.store(1_000_000, Ordering::Relaxed);
        p.keys_packed.store(400_000, Ordering::Relaxed);
        p.creates_published.store(250_000, Ordering::Relaxed);
        p.shards_published.store(3, Ordering::Relaxed);
        let s = p.snapshot(10_000_000, false);
        assert_eq!(s.creates, 1_000_000, "creates are all packed recs");
        assert_eq!(s.keys, 400_000, "keys are packed shards, not one worker");
        assert_eq!(s.pending, 750_000, "pending is packed minus published");
        assert_eq!(s.shards, 3);
        assert!(
            (s.pct - 10.0).abs() < 0.01,
            "pct uses global recs/total, got {}",
            s.pct
        );
        let done = p.snapshot(10_000_000, true);
        assert!((done.pct - 100.0).abs() < 0.01);
        let mut last = None;
        log_materialize_status(&mut last, &s, 64, Instant::now());
        assert!(last.is_some());
    }

    #[test]
    fn pack_ahead_stays_within_publish_window() {
        assert!(pack_ahead_allowed(0, 0, 8));
        assert!(pack_ahead_allowed(7, 0, 8));
        assert!(
            !pack_ahead_allowed(8, 0, 8),
            "must not pack shard 8 before shard 0 is published"
        );
        assert!(pack_ahead_allowed(8, 1, 8));
        assert!(
            !pack_ahead_allowed(45, 1, 8),
            "must not pack tens of shards ahead of a stuck publish prefix"
        );
        assert!(
            pack_ahead_allowed(1, 1, 1),
            "cap 1 packs only the next shard"
        );
        assert!(!pack_ahead_allowed(2, 1, 1));
    }
}
