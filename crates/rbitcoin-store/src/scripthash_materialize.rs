//! Sliced k-way SH tip materialize: one worker per prefix shard, ordered publish.

use crate::error::StoreError;
use crate::file::ensure_nofile_budget_at_least;
use crate::scripthash::{ColdProgress, ScriptHashTable, ShBodyLayout, ShShardPack};
use crate::sorted_run::{
    for_each_merged_rec_shard, set_thread_idle_io_priority, shard_record_starts, SortedRunPath,
};
use rbitcoin_primitives::Fk;
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

const MATERIALIZE_STATUS_INTERVAL: Duration = Duration::from_secs(10);
const MATERIALIZE_STATUS_CHECK_EVERY: u64 = 1 << 16;

fn materialize_status_should_emit(
    last_log: Option<Instant>,
    now: Instant,
    interval: Duration,
    recs_this_key: u64,
    key_just_closed: bool,
) -> bool {
    let time_due = match last_log {
        None => true,
        Some(t) => now.saturating_duration_since(t) >= interval,
    };
    if !time_due {
        return false;
    }
    if key_just_closed {
        return true;
    }
    recs_this_key > 0 && recs_this_key.is_multiple_of(MATERIALIZE_STATUS_CHECK_EVERY)
}

fn materialize_status_creates(committed: u64, pending_chain: usize) -> u64 {
    committed.saturating_add(pending_chain as u64)
}

fn log_materialize_status(
    last_log: &mut Option<Instant>,
    keys: u64,
    committed_creates: u64,
    pending: usize,
    shards: u32,
    n_shards: usize,
    total_recs: u64,
    body_flush_ns: u64,
    head_fill_ns: u64,
    t0: Instant,
) {
    *last_log = Some(Instant::now());
    let creates = materialize_status_creates(committed_creates, pending);
    let elapsed = t0.elapsed();
    let secs = elapsed.as_secs_f64().max(1e-3);
    let keys_per_s = keys as f64 / secs;
    let pct = if total_recs > 0 {
        (100.0 * creates as f64 / total_recs as f64).clamp(0.0, 99.9)
    } else {
        0.0
    };
    rbitcoin_log::info!(
        "node: scripthash materialize status keys≈{keys} creates≈{creates} pending≈{pending} \
         pct≈{pct:.1}% shards={shards}/{n_shards} rate≈{keys_per_s:.0}keys/s \
         body_flush={:?} head_fill={:?} elapsed={elapsed:?}",
        Duration::from_nanos(body_flush_ns),
        Duration::from_nanos(head_fill_ns),
    );
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
    n_shards: usize,
    total_recs: u64,
    cancel: Option<&AtomicBool>,
    recs_out: &AtomicU64,
    last_log: &Mutex<Option<Instant>>,
    t0: Instant,
) -> Result<ShShardPack, StoreError> {
    let mut session = table.pack_shard_session(shard)?;
    let mut recs_this_key = 0u64;
    for_each_merged_rec_shard(inputs, cuts, shard, false, |rec| {
        if cancel.map(|c| c.load(Ordering::Relaxed)).unwrap_or(false) {
            return Err(StoreError::Cancelled("scripthash shard pack"));
        }
        let (sh, fk) = decode_sh_run_rec(rec)?;
        if fk.is_null() {
            return Ok(());
        }
        session.push_sorted_fk(sh, fk)?;
        recs_this_key = recs_this_key.saturating_add(1);
        recs_out.fetch_add(1, Ordering::Relaxed);
        if recs_this_key.is_multiple_of(MATERIALIZE_STATUS_CHECK_EVERY) {
            let now = Instant::now();
            let mut g = last_log.lock().unwrap();
            if materialize_status_should_emit(
                *g,
                now,
                MATERIALIZE_STATUS_INTERVAL,
                recs_this_key,
                false,
            ) {
                let pending = session
                    .stream_creates_written()
                    .saturating_sub(session.creates_written())
                    as usize;
                log_materialize_status(
                    &mut g,
                    session.keys_written(),
                    session.creates_written(),
                    pending,
                    shard as u32,
                    n_shards,
                    total_recs,
                    session.body_flush_ns,
                    0,
                    t0,
                );
            }
        }
        Ok(())
    })?;
    session.finish_pack()
}

fn publish_next(
    table: &ScriptHashTable,
    shard: usize,
    pack: ShShardPack,
    global_bump: u64,
    live_keys: &mut u64,
    live_creates: &mut u64,
    max_fk: &mut u64,
) -> Result<u64, StoreError> {
    *max_fk = (*max_fk).max(pack.max_fk);
    *live_keys = live_keys.saturating_add(pack.keys);
    *live_creates = live_creates.saturating_add(pack.creates);
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
    let recs_packed = Arc::new(AtomicU64::new(0));
    let last_log = Arc::new(Mutex::new(None));
    let total_recs: u64 = inputs.iter().map(|r| r.count).sum();
    let prior = ColdProgress::load(table.store_dir()).ok().flatten();
    let mut bump = table.alloc_bump();
    let mut live_creates = prior
        .as_ref()
        .map(|p| p.live_count)
        .unwrap_or_else(|| table.entry_count());
    let mut live_keys = prior.as_ref().map(|p| p.keys_written).unwrap_or(0);
    let mut max_fk = 0u64;

    if workers <= 1 {
        for shard in resume_from..n_shards {
            if cancel.map(|c| c.load(Ordering::Relaxed)).unwrap_or(false) {
                return Err(StoreError::Cancelled("scripthash shard pack"));
            }
            let pack = pack_shard(
                table,
                inputs,
                &cuts,
                shard,
                n_shards,
                total_recs,
                cancel,
                recs_packed.as_ref(),
                last_log.as_ref(),
                t0,
            )?;
            bump = publish_next(
                table,
                shard,
                pack,
                bump,
                &mut live_keys,
                &mut live_creates,
                &mut max_fk,
            )?;
            {
                let mut g = last_log.lock().unwrap();
                let done = shard + 1 == n_shards;
                if materialize_status_should_emit(
                    *g,
                    Instant::now(),
                    MATERIALIZE_STATUS_INTERVAL,
                    1,
                    done,
                ) {
                    log_materialize_status(
                        &mut g,
                        live_keys,
                        live_creates,
                        0,
                        (shard + 1) as u32,
                        n_shards,
                        total_recs,
                        0,
                        t0.elapsed().as_nanos() as u64,
                        t0,
                    );
                }
            }
        }
        return Ok(ShShardMaterialize {
            creates: live_creates,
            keys: live_keys,
            max_fk,
            body_flush_ns: 0,
            head_fill_ns: t0.elapsed().as_nanos() as u64,
        });
    }

    let jobs: VecDeque<usize> = (resume_from..n_shards).collect();
    let shared = Arc::new(ShardPool {
        jobs: Mutex::new(jobs),
        done: Mutex::new(HashMap::new()),
        err: Mutex::new(None),
        cv: Condvar::new(),
    });
    std::thread::scope(|scope| {
        for _ in 0..workers {
            let shared = Arc::clone(&shared);
            let recs_packed = Arc::clone(&recs_packed);
            let last_log = Arc::clone(&last_log);
            let cuts = &cuts;
            scope.spawn(move || {
                set_thread_idle_io_priority();
                loop {
                    if cancel.map(|c| c.load(Ordering::Relaxed)).unwrap_or(false) {
                        let mut g = shared.err.lock().unwrap();
                        if g.is_none() {
                            *g = Some(StoreError::Cancelled("scripthash shard pack"));
                        }
                        shared.cv.notify_all();
                        break;
                    }
                    let shard = {
                        let mut q = shared.jobs.lock().unwrap();
                        q.pop_front()
                    };
                    let Some(shard) = shard else {
                        break;
                    };
                    match pack_shard(
                        table,
                        inputs,
                        cuts,
                        shard,
                        n_shards,
                        total_recs,
                        cancel,
                        recs_packed.as_ref(),
                        last_log.as_ref(),
                        t0,
                    ) {
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
            )?;
            {
                let mut g = last_log.lock().unwrap();
                let done = shard + 1 == n_shards;
                if materialize_status_should_emit(
                    *g,
                    Instant::now(),
                    MATERIALIZE_STATUS_INTERVAL,
                    1,
                    done,
                ) {
                    log_materialize_status(
                        &mut g,
                        live_keys,
                        live_creates,
                        0,
                        (shard + 1) as u32,
                        n_shards,
                        total_recs,
                        0,
                        t0.elapsed().as_nanos() as u64,
                        t0,
                    );
                }
            }
        }
        Ok(())
    })?;

    Ok(ShShardMaterialize {
        creates: live_creates,
        keys: live_keys,
        max_fk,
        body_flush_ns: 0,
        head_fill_ns: t0.elapsed().as_nanos() as u64,
    })
}

struct ShardPool {
    jobs: Mutex<VecDeque<usize>>,
    done: Mutex<HashMap<usize, ShShardPack>>,
    err: Mutex<Option<StoreError>>,
    cv: Condvar,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn materialize_status_emits_mid_key_after_interval() {
        let interval = MATERIALIZE_STATUS_INTERVAL;
        let t0 = Instant::now();
        let t_due = t0 + interval;
        assert!(
            !materialize_status_should_emit(
                Some(t0),
                t0,
                interval,
                MATERIALIZE_STATUS_CHECK_EVERY,
                false
            ),
            "must not emit before the wall interval"
        );
        assert!(
            materialize_status_should_emit(Some(t0), t_due, interval, 1, true),
            "key close after interval still emits"
        );
        assert!(
            !materialize_status_should_emit(
                Some(t0),
                t_due,
                interval,
                MATERIALIZE_STATUS_CHECK_EVERY - 1,
                false
            ),
            "mid-key off-stride must not clock every rec"
        );
        assert!(
            materialize_status_should_emit(
                Some(t0),
                t_due,
                interval,
                MATERIALIZE_STATUS_CHECK_EVERY,
                false
            ),
            "mid-key megakey after interval must heartbeat on the rec stride"
        );
        assert_eq!(
            materialize_status_creates(92_697_818, 62_870_375),
            155_568_193
        );
        assert_eq!(materialize_status_creates(u64::MAX, 8), u64::MAX);
        let mut last = None;
        log_materialize_status(&mut last, 1, 1, 0, 1, 4, 10, 0, 0, Instant::now());
        assert!(last.is_some());
    }
}
