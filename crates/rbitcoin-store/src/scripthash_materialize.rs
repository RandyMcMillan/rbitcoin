//! Sliced k-way SH tip materialize: one worker per prefix shard, ordered publish.

use crate::error::StoreError;
use crate::file::{ensure_nofile_budget_at_least, TableFile};
use crate::scripthash::{ColdProgress, ScriptHashTable, ShShardPack};
use crate::sorted_run::{
    for_each_merged_rec_shard, set_thread_idle_io_priority, shard_record_starts, SortedRunPath,
};
use rbitcoin_primitives::{Fk, TableKind};
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

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

fn resolve_workers(requested: usize, n_shards: usize, n_runs: usize) -> usize {
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
    workers
}

fn pack_shard(
    table: &ScriptHashTable,
    inputs: &[SortedRunPath],
    cuts: &[Vec<u64>],
    shard: usize,
    cancel: Option<&AtomicBool>,
    recs_out: &AtomicU64,
) -> Result<ShShardPack, StoreError> {
    let temp_path = table
        .store_dir()
        .join(format!("scripthash.pack.{shard:02x}.body"));
    let _ = std::fs::remove_file(&temp_path);
    let temp = TableFile::create(&temp_path, TableKind::ScriptHash)?;
    let mut session = table.pack_shard_session(temp)?;
    for_each_merged_rec_shard(inputs, cuts, shard, false, |rec| {
        if cancel.map(|c| c.load(Ordering::Relaxed)).unwrap_or(false) {
            return Err(StoreError::Cancelled("scripthash shard pack"));
        }
        let (sh, fk) = decode_sh_run_rec(rec)?;
        if fk.is_null() {
            return Ok(());
        }
        session.push_sorted_fk(sh, fk)?;
        recs_out.fetch_add(1, Ordering::Relaxed);
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
    let temp_path = table
        .store_dir()
        .join(format!("scripthash.pack.{shard:02x}.body"));
    let new_bump = if pack.recs.is_empty() && pack.creates == 0 {
        global_bump
    } else {
        table.publish_packed_shard(shard, pack, global_bump)?
    };
    ColdProgress {
        next_shard: (shard as u32).saturating_add(1),
        body_bump: new_bump,
        live_count: *live_creates,
        keys_written: *live_keys,
    }
    .store(table.store_dir())?;
    let _ = std::fs::remove_file(&temp_path);
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
    let workers = resolve_workers(workers, n_shards - resume_from, inputs.len());
    rbitcoin_log::info!(
        "store: scripthash shard-kway start resume_from={resume_from} n_shards={n_shards} \
         workers={workers} runs={} fds≈{}",
        inputs.len(),
        workers.saturating_mul(inputs.len().max(1))
    );
    let t0 = Instant::now();
    let recs_packed = Arc::new(AtomicU64::new(0));
    let mut bump = table.alloc_bump();
    let mut live_creates = table.entry_count();
    let mut live_keys = 0u64;
    let mut max_fk = 0u64;
    let mut last_log = Instant::now();

    if workers <= 1 {
        for shard in resume_from..n_shards {
            if cancel.map(|c| c.load(Ordering::Relaxed)).unwrap_or(false) {
                return Err(StoreError::Cancelled("scripthash shard pack"));
            }
            let pack = pack_shard(table, inputs, &cuts, shard, cancel, recs_packed.as_ref())?;
            bump = publish_next(
                table,
                shard,
                pack,
                bump,
                &mut live_keys,
                &mut live_creates,
                &mut max_fk,
            )?;
            maybe_status(
                &mut last_log,
                t0,
                live_keys,
                live_creates,
                shard + 1,
                n_shards,
            );
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
                    match pack_shard(table, inputs, cuts, shard, cancel, recs_packed.as_ref()) {
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
            maybe_status(
                &mut last_log,
                t0,
                live_keys,
                live_creates,
                shard + 1,
                n_shards,
            );
        }
        Ok(())
    })?;

    // Drop any leftover unpublished temps (cancel / error already returned).
    for shard in resume_from..n_shards {
        let p = table
            .store_dir()
            .join(format!("scripthash.pack.{shard:02x}.body"));
        let _ = std::fs::remove_file(&p);
    }

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

fn maybe_status(
    last: &mut Instant,
    t0: Instant,
    keys: u64,
    creates: u64,
    next_shard: usize,
    n_shards: usize,
) {
    let now = Instant::now();
    if now.saturating_duration_since(*last) < Duration::from_secs(10) && next_shard < n_shards {
        return;
    }
    *last = now;
    let secs = t0.elapsed().as_secs_f64().max(1e-3);
    rbitcoin_log::info!(
        "node: scripthash materialize keys≈{keys} creates≈{creates} \
         shards={next_shard}/{n_shards} rate≈{:.0}key/s elapsed={:?}",
        keys as f64 / secs,
        t0.elapsed()
    );
}
