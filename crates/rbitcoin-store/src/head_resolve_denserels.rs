//! Plan Shape A head resolve: **txids in → denserels out** (or fk+range short-circuit).
//!
//! Schema **13+** fused FdOnly machine on **one** TLS [`UringSession`], **two waves**:
//! 1. **Hot probe** — page-coalesced loads for non-DONTCACHE segs (ages ≤3) → cands
//! 2. **STAGE_ID / STAGE_IDX** — sidefile identity + idx range (depth-first)
//! 3. If any key still unmatched: **cold probe** (DONTCACHE segs ages ≥4) for
//!    survivors only → full cand list (no per-seg short-circuit) → ID/IDX again
//! 4. **denserels** (optional) — packed body when outs are needed
//!
//! [`resolve_fk_and_range_batch`] is the **stamp short-circuit**: stops after
//! STAGE_IDX, returns `(fk, body_range)` so prep denserels-loads by offset.
//!
//! **IO shape:** one `with_thread_local` owns the ring for both waves. Nested TLS
//! uring is a hard error. Up to [`MAX_IN_FLIGHT`] keys in flight for ID/IDX.
//!
//! Backend: `RBITCOIN_HEAD_RESOLVE_IO` / global `RBITCOIN_IO` (`uring` \| `pread`).

use crate::error::StoreError;
use crate::idx_body_pipeline::{run_idx_body_pipeline, BodyMode, IdxBodyJob};
use crate::io_backend::{self, ReadIoBackend};
use crate::tx_idx::BodyRangeIdxPlan;
use crate::tx_table::{
    decode_packed_tx_outs_with_spender_rels_secret, OutputRecord, TxRecord, TxTable,
};
use crate::txid_body::{TxidBody, TXID_ENTRY_LEN};
use crate::uring_session::{self, UringSession};
use rbitcoin_primitives::Fk;
use std::os::fd::RawFd;
use std::time::Instant;

/// Concurrent keys in the plan head-resolve uring machine (matches default ring).
const MAX_IN_FLIGHT: usize = 128;

/// Sidefile identity pread (32 B).
const STAGE_ID: u64 = 1;
/// `tx.idx` OS-page pread for body_range after identity match.
const STAGE_IDX: u64 = 2;

/// One plan key: candidates deepest-first; buffers stable for in-flight SQEs.
struct KeyWork {
    key_i: u32,
    cands: Vec<u64>,
    cand_i: usize,
    /// Sidefile identity buffer (32 B).
    id_buf: [u8; 32],
    pending_fk: u64,
    pending_rank: u32,
    /// STAGE_IDX plan + page buffers (set after identity hit).
    idx_plan: Option<BodyRangeIdxPlan>,
    idx_bufs: Vec<Vec<u8>>,
    /// Which idx page CQE we are waiting on (0 or 1).
    idx_page_i: u8,
}

/// Stamp short-circuit: **txids → (fk, body_range)** via one TLS uring machine.
///
/// Probe (head pages) → depth-first identity → idx body_range. Prep denserels
/// loads by offset (skip re-idx).
pub fn resolve_fk_and_range_batch(
    table: &TxTable,
    txids: &[[u8; 32]],
) -> Result<Vec<([u8; 32], Option<(Fk, (u64, u64))>)>, StoreError> {
    if txids.is_empty() {
        return Ok(Vec::new());
    }
    match io_backend::head_resolve_io_backend() {
        ReadIoBackend::Uring => match resolve_fk_and_range_uring(table, txids) {
            Ok(v) => Ok(v),
            // Ring open fail (agent 9p / disabled): sync depth-first fallback.
            Err(_) => resolve_fk_and_range_pread(table, txids),
        },
        ReadIoBackend::Pread => resolve_fk_and_range_pread(table, txids),
    }
}

/// Resolve many parent txids to create fk + denserels (plan Shape A full).
///
/// Returns rows in **input order** and denserels-wave wall ns (archive `head_dens`).
pub fn resolve_fk_and_denserels_batch(
    table: &TxTable,
    txids: &[[u8; 32]],
) -> Result<
    (
        Vec<(
            [u8; 32],
            Option<(Fk, Option<(TxRecord, Vec<OutputRecord>, Vec<u32>)>)>,
        )>,
        u64,
    ),
    StoreError,
> {
    if txids.is_empty() {
        return Ok((Vec::new(), 0));
    }
    match io_backend::head_resolve_io_backend() {
        ReadIoBackend::Uring => match resolve_denserels_uring(table, txids) {
            Ok(v) => Ok(v),
            Err(_) => resolve_denserels_pread(table, txids),
        },
        ReadIoBackend::Pread => resolve_denserels_pread(table, txids),
    }
}

// ── pread: two-wave (hot → ID; cold survivors → ID) ─────────────────────────

fn resolve_fk_and_range_pread(
    table: &TxTable,
    txids: &[[u8; 32]],
) -> Result<Vec<([u8; 32], Option<(Fk, (u64, u64))>)>, StoreError> {
    crate::head_resolve_stats::add_keys(txids.len() as u64);

    let mixed: Vec<[u8; 32]> = txids.iter().map(|t| table.secret.mix_txid(t)).collect();
    let side = table.txid_sidefile();
    let first_fks = table.head.first_fks_snapshot();
    let mut local_age = [0u64; crate::head_resolve_stats::AGE_CAP];
    let mut winner: Vec<Option<(Fk, (u64, u64))>> = vec![None; txids.len()];
    let mut body_lookups = 0u64;
    let mut miss_peeks = 0u64;
    let mut id_ns = 0u64;
    let mut idx_ns = 0u64;
    let mut probe_ns = 0u64;
    let mut cands_total = 0u64;

    // Wave 1: hot (cacheable) head segments.
    let t_probe = Instant::now();
    let hot_cands = table.head.probe_candidates_batch_hot(&mixed)?;
    probe_ns = probe_ns.saturating_add(t_probe.elapsed().as_nanos() as u64);
    cands_total = cands_total.saturating_add(hot_cands.iter().map(|c| c.len() as u64).sum());
    id_idx_wave_pread(
        table,
        txids,
        &hot_cands,
        side,
        &mut winner,
        /*skip_if_won=*/ false,
        &mut body_lookups,
        &mut miss_peeks,
        &mut id_ns,
        &mut idx_ns,
        &first_fks,
        &mut local_age,
    )?;

    // Wave 2: full cold depth for keys that missed hot (no further short-circuit).
    let mut need_cold = false;
    let mut active = vec![false; txids.len()];
    for (i, w) in winner.iter().enumerate() {
        if w.is_none() {
            active[i] = true;
            need_cold = true;
        }
    }
    if need_cold {
        let t_probe = Instant::now();
        let cold_cands = table.head.probe_candidates_batch_cold(&mixed, &active)?;
        probe_ns = probe_ns.saturating_add(t_probe.elapsed().as_nanos() as u64);
        cands_total = cands_total.saturating_add(cold_cands.iter().map(|c| c.len() as u64).sum());
        id_idx_wave_pread(
            table,
            txids,
            &cold_cands,
            side,
            &mut winner,
            /*skip_if_won=*/ true,
            &mut body_lookups,
            &mut miss_peeks,
            &mut id_ns,
            &mut idx_ns,
            &first_fks,
            &mut local_age,
        )?;
    }

    crate::head_resolve_stats::add_probe(probe_ns);
    crate::head_resolve_stats::add_cands(cands_total);
    crate::head_resolve_stats::add_body(id_ns);
    crate::head_resolve_stats::add_idx(idx_ns);
    crate::head_resolve_stats::add_body_lookups(body_lookups);
    crate::head_resolve_stats::add_miss_peeks(miss_peeks);
    crate::head_resolve_stats::add_hit_ages(&local_age);

    Ok(txids
        .iter()
        .enumerate()
        .map(|(i, t)| (*t, winner[i]))
        .collect())
}

/// Depth-first sidefile + idx for each key's cand list (pread).
fn id_idx_wave_pread(
    table: &TxTable,
    txids: &[[u8; 32]],
    cands_by_key: &[Vec<Fk>],
    side: &TxidBody,
    winner: &mut [Option<(Fk, (u64, u64))>],
    skip_if_won: bool,
    body_lookups: &mut u64,
    miss_peeks: &mut u64,
    id_ns: &mut u64,
    idx_ns: &mut u64,
    first_fks: &[u64],
    local_age: &mut [u64; crate::head_resolve_stats::AGE_CAP],
) -> Result<(), StoreError> {
    for (ki, cands) in cands_by_key.iter().enumerate() {
        if skip_if_won && winner[ki].is_some() {
            continue;
        }
        for (rank0, &fk) in cands.iter().enumerate() {
            let rank = (rank0 + 1) as u64;
            let t_id = Instant::now();
            let got = match side.get(fk) {
                Ok(t) => t,
                Err(StoreError::NotFound) | Err(StoreError::InvalidFk) => {
                    *id_ns = id_ns.saturating_add(t_id.elapsed().as_nanos() as u64);
                    *miss_peeks = miss_peeks.saturating_add(1);
                    *body_lookups = body_lookups.saturating_add(1);
                    continue;
                }
                Err(e) => return Err(e),
            };
            *id_ns = id_ns.saturating_add(t_id.elapsed().as_nanos() as u64);
            *body_lookups = body_lookups.saturating_add(1);
            if got != txids[ki] {
                *miss_peeks = miss_peeks.saturating_add(1);
                continue;
            }
            crate::head_resolve_stats::add_hit_rank(rank);
            let t_idx = Instant::now();
            match table.body.record_range(fk) {
                Ok((off, len)) if len > 0 => {
                    winner[ki] = Some((fk, (off, len)));
                    crate::head_resolve_stats::note_local_hit_age(local_age, first_fks, fk.0);
                }
                Ok(_) | Err(StoreError::NotFound) | Err(StoreError::Corrupt(_)) => {}
                Err(e) => return Err(e),
            }
            *idx_ns = idx_ns.saturating_add(t_idx.elapsed().as_nanos() as u64);
            break; // depth-first short-circuit within this wave's cands
        }
    }
    Ok(())
}

fn resolve_denserels_pread(
    table: &TxTable,
    txids: &[[u8; 32]],
) -> Result<
    (
        Vec<(
            [u8; 32],
            Option<(Fk, Option<(TxRecord, Vec<OutputRecord>, Vec<u32>)>)>,
        )>,
        u64,
    ),
    StoreError,
> {
    // Identity + range first (depth-first), then one denserels wave for winners.
    let ranges = resolve_fk_and_range_pread(table, txids)?;
    let mut dens_ns = 0u64;
    let mut dens_decoded: std::collections::HashMap<
        usize,
        (TxRecord, Vec<OutputRecord>, Vec<u32>),
    > = std::collections::HashMap::new();

    let mut need: Vec<(usize, Fk, (u64, u64))> = Vec::new();
    for (i, (_tid, row)) in ranges.iter().enumerate() {
        if let Some((fk, range)) = row {
            need.push((i, *fk, *range));
        }
    }
    if !need.is_empty() {
        let t_dens = Instant::now();
        let mut jobs: Vec<IdxBodyJob> = need
            .iter()
            .map(|(_, fk, range)| IdxBodyJob::new(fk.0, Some(*range)))
            .collect();
        run_idx_body_pipeline(&table.body, &mut jobs, BodyMode::OutsDenserels)?;
        dens_ns = t_dens.elapsed().as_nanos() as u64;
        for ((ki, fk, _), job) in need.into_iter().zip(jobs.into_iter()) {
            if !job.ok || job.body.is_empty() {
                continue;
            }
            match decode_packed_tx_outs_with_spender_rels_secret(&job.body, Some(&table.secret)) {
                Ok(mut decoded) => {
                    if let Ok(tid) = table.txid_sidefile().get(fk) {
                        decoded.0.txid = tid;
                    }
                    dens_decoded.insert(ki, decoded);
                }
                Err(StoreError::NotFound) | Err(StoreError::Corrupt(_)) => {}
                Err(e) => return Err(e),
            }
        }
    }

    let mut out = Vec::with_capacity(txids.len());
    for (i, (txid, row)) in ranges.into_iter().enumerate() {
        let mapped = row.map(|(fk, _range)| (fk, dens_decoded.remove(&i)));
        out.push((txid, mapped));
    }
    Ok((out, dens_ns))
}

// ── uring: single TLS machine (probe → ID → IDX) ────────────────────────────

fn resolve_fk_and_range_uring(
    table: &TxTable,
    txids: &[[u8; 32]],
) -> Result<Vec<([u8; 32], Option<(Fk, (u64, u64))>)>, StoreError> {
    crate::head_resolve_stats::add_keys(txids.len() as u64);

    // One TLS ring for the entire machine: head pages, sidefile, idx.
    uring_session::with_thread_local(uring_session::DEFAULT_ENTRIES, |session| {
        resolve_fk_and_range_uring_on(session, table, txids)
    })?
}

fn resolve_fk_and_range_uring_on(
    session: &mut UringSession,
    table: &TxTable,
    txids: &[[u8; 32]],
) -> Result<Vec<([u8; 32], Option<(Fk, (u64, u64))>)>, StoreError> {
    let side = table.txid_sidefile();
    let side_fd: RawFd = side.body_read_fd();
    let side_path = side.file_path().to_path_buf();
    let count = table.body.count();
    let mixed: Vec<[u8; 32]> = txids.iter().map(|t| table.secret.mix_txid(t)).collect();
    let first_fks = table.head.first_fks_snapshot();
    let mut local_age = [0u64; crate::head_resolve_stats::AGE_CAP];

    let mut winner: Vec<Option<(Fk, (u64, u64))>> = vec![None; txids.len()];
    let mut body_lookups = 0u64;
    let mut miss_peeks = 0u64;
    let mut id_ns = 0u64;
    let mut idx_ns = 0u64;
    let mut probe_ns = 0u64;
    let mut cands_total = 0u64;

    // ── Wave 1: hot head pages + ID/IDX ───────────────────────────────────
    let t_probe = Instant::now();
    let hot_cands = table
        .head
        .probe_candidates_batch_hot_on_session(&mixed, session)?;
    probe_ns = probe_ns.saturating_add(t_probe.elapsed().as_nanos() as u64);
    let hot_u64: Vec<Vec<u64>> = hot_cands
        .into_iter()
        .map(|v| v.into_iter().map(|f| f.0).collect())
        .collect();
    cands_total = cands_total.saturating_add(hot_u64.iter().map(|v| v.len() as u64).sum());
    debug_assert_eq!(session.in_flight(), 0);

    id_idx_wave_uring(
        session,
        table,
        txids,
        &hot_u64,
        side,
        side_fd,
        &side_path,
        count,
        &mut winner,
        /*only_unset=*/ false,
        &mut body_lookups,
        &mut miss_peeks,
        &mut id_ns,
        &mut idx_ns,
        &first_fks,
        &mut local_age,
    )?;

    // ── Wave 2: full cold head for survivors + ID/IDX (no further SC) ─────
    let mut need_cold = false;
    let mut active = vec![false; txids.len()];
    for (i, w) in winner.iter().enumerate() {
        if w.is_none() {
            active[i] = true;
            need_cold = true;
        }
    }
    if need_cold {
        let t_probe = Instant::now();
        let cold_cands = table
            .head
            .probe_candidates_batch_cold_on_session(&mixed, &active, session)?;
        probe_ns = probe_ns.saturating_add(t_probe.elapsed().as_nanos() as u64);
        let cold_u64: Vec<Vec<u64>> = cold_cands
            .into_iter()
            .map(|v| v.into_iter().map(|f| f.0).collect())
            .collect();
        cands_total = cands_total.saturating_add(cold_u64.iter().map(|v| v.len() as u64).sum());
        debug_assert_eq!(session.in_flight(), 0);

        id_idx_wave_uring(
            session,
            table,
            txids,
            &cold_u64,
            side,
            side_fd,
            &side_path,
            count,
            &mut winner,
            /*only_unset=*/ true,
            &mut body_lookups,
            &mut miss_peeks,
            &mut id_ns,
            &mut idx_ns,
            &first_fks,
            &mut local_age,
        )?;
    }

    crate::head_resolve_stats::add_probe(probe_ns);
    crate::head_resolve_stats::add_idx(idx_ns);
    crate::head_resolve_stats::add_body(id_ns);
    crate::head_resolve_stats::add_cands(cands_total);
    crate::head_resolve_stats::add_body_lookups(body_lookups);
    crate::head_resolve_stats::add_miss_peeks(miss_peeks);
    crate::head_resolve_stats::add_hit_ages(&local_age);

    Ok(txids
        .iter()
        .enumerate()
        .map(|(i, t)| (*t, winner[i]))
        .collect())
}

/// One ID/IDX wave on a held session for the given cand lists.
///
/// When `only_unset` is true, keys that already have a winner are skipped
/// (wave-2 cold pass after hot hits).
fn id_idx_wave_uring(
    session: &mut UringSession,
    table: &TxTable,
    txids: &[[u8; 32]],
    cands_u64: &[Vec<u64>],
    side: &TxidBody,
    side_fd: RawFd,
    side_path: &std::path::Path,
    count: u64,
    winner: &mut [Option<(Fk, (u64, u64))>],
    only_unset: bool,
    body_lookups: &mut u64,
    miss_peeks: &mut u64,
    id_ns: &mut u64,
    idx_ns: &mut u64,
    first_fks: &[u64],
    local_age: &mut [u64; crate::head_resolve_stats::AGE_CAP],
) -> Result<(), StoreError> {
    let mut done = vec![false; txids.len()];
    if only_unset {
        for (i, w) in winner.iter().enumerate() {
            if w.is_some() {
                done[i] = true;
            }
        }
    }
    let mut next_key = 0usize;
    let mut free_slots: Vec<usize> = (0..MAX_IN_FLIGHT).collect();
    // SQE destinations declared *before* the drain guard: on Err/unwind the
    // guard drops first (drains ring) while these buffers are still live.
    let mut slots: Vec<Option<KeyWork>> = (0..MAX_IN_FLIGHT).map(|_| None).collect();
    let mut in_flight = 0usize;
    let mut ring = DrainSessionOnDrop(session);

    arm_keys(
        table,
        txids,
        cands_u64,
        side,
        &mut ring,
        side_fd,
        count,
        &mut free_slots,
        &mut slots,
        &mut in_flight,
        &mut next_key,
        &mut done,
        id_ns,
    )?;
    ring.sync_submission();
    let _ = ring.submit();

    while in_flight > 0 {
        let t_wait = Instant::now();
        let mut cqes = ring.harvest_ready();
        if cqes.is_empty() {
            ring.submit_and_wait_one()?;
            cqes = ring.harvest_ready();
        }
        let wait_ns = t_wait.elapsed().as_nanos() as u64;

        for (ud, res) in cqes {
            let (kind, slot_u) = uring_session::unpack_ud(ud);
            let slot = slot_u as usize;
            if slot >= slots.len() || slots[slot].is_none() {
                return Err(StoreError::Corrupt("head resolve bad slot"));
            }
            in_flight = in_flight.saturating_sub(1);

            match kind {
                STAGE_ID => {
                    *id_ns = id_ns.saturating_add(wait_ns);
                    *body_lookups = body_lookups.saturating_add(1);
                    let outcome = on_stage_id(
                        table, txids, &mut slots, slot, &mut ring, side, side_fd, side_path, count,
                        res, id_ns, idx_ns, winner, &mut done, miss_peeks,
                    )?;
                    match outcome {
                        SqeOutcome::SqePushed => in_flight += 1,
                        SqeOutcome::Finished => {
                            slots[slot] = None;
                            free_slots.push(slot);
                        }
                    }
                }
                STAGE_IDX => {
                    *idx_ns = idx_ns.saturating_add(wait_ns);
                    let outcome = on_stage_idx(
                        table, &mut slots, slot, &mut ring, side, side_fd, count, res, id_ns,
                        idx_ns, winner, &mut done, miss_peeks, first_fks, local_age,
                    )?;
                    match outcome {
                        SqeOutcome::SqePushed => in_flight += 1,
                        SqeOutcome::Finished => {
                            slots[slot] = None;
                            free_slots.push(slot);
                        }
                    }
                }
                _ => return Err(StoreError::Corrupt("head resolve bad stage")),
            }
        }

        arm_keys(
            table,
            txids,
            cands_u64,
            side,
            &mut ring,
            side_fd,
            count,
            &mut free_slots,
            &mut slots,
            &mut in_flight,
            &mut next_key,
            &mut done,
            id_ns,
        )?;
        ring.sync_submission();
        let _ = ring.submit();
    }

    drop(ring);
    Ok(())
}

/// Drain the session on drop while caller-held SQE buffers are still live.
struct DrainSessionOnDrop<'a>(&'a mut UringSession);

impl std::ops::Deref for DrainSessionOnDrop<'_> {
    type Target = UringSession;
    fn deref(&self) -> &UringSession {
        self.0
    }
}
impl std::ops::DerefMut for DrainSessionOnDrop<'_> {
    fn deref_mut(&mut self) -> &mut UringSession {
        self.0
    }
}
impl Drop for DrainSessionOnDrop<'_> {
    fn drop(&mut self) {
        self.0.drain_all();
    }
}

enum SqeOutcome {
    /// Another SQE was pushed for this slot (ID next cand, ID retry, or IDX page).
    SqePushed,
    /// Key finished (hit with range, or candidates exhausted).
    Finished,
}

/// STAGE_ID CQE: match → plan+push STAGE_IDX; miss → next cand; errors/retry.
fn on_stage_id(
    table: &TxTable,
    txids: &[[u8; 32]],
    slots: &mut [Option<KeyWork>],
    slot: usize,
    session: &mut UringSession,
    side: &TxidBody,
    side_fd: RawFd,
    side_path: &std::path::Path,
    count: u64,
    res: i32,
    id_ns: &mut u64,
    idx_ns: &mut u64,
    winner: &mut [Option<(Fk, (u64, u64))>],
    done: &mut [bool],
    miss_peeks: &mut u64,
) -> Result<SqeOutcome, StoreError> {
    if res < 0 {
        // ENOTSUP (-95): RWF_DONTCACHE unsupported — demote and retry once.
        if res == -95 && crate::bulk_io::rwf_dontcache_ok() {
            crate::bulk_io::note_rwf_dontcache_unsupported();
            if let Some(w) = slots[slot].as_mut() {
                let off = match TxidBody::entry_offset(w.pending_fk) {
                    Ok(o) => o,
                    Err(_) => {
                        return Err(StoreError::io(
                            side_path,
                            std::io::Error::from_raw_os_error(-res),
                        ));
                    }
                };
                w.id_buf = [0u8; 32];
                let ud = uring_session::pack_ud(STAGE_ID, slot as u32);
                session.push_pread_flags(side_fd, off, &mut w.id_buf, ud, 0)?;
                return Ok(SqeOutcome::SqePushed);
            }
        }
        return Err(StoreError::io(
            side_path,
            std::io::Error::from_raw_os_error(-res),
        ));
    }

    if res as usize != TXID_ENTRY_LEN as usize {
        // Short identity — try next cand.
        let still = submit_next_id_or_done(
            side,
            slots[slot].as_mut().unwrap(),
            session,
            side_fd,
            count,
            slot as u32,
            id_ns,
        )?;
        return Ok(if still {
            SqeOutcome::SqePushed
        } else {
            let w = slots[slot].as_ref().unwrap();
            done[w.key_i as usize] = true;
            SqeOutcome::Finished
        });
    }

    let w = slots[slot].as_mut().unwrap();
    let key_i = w.key_i as usize;
    let got = w.id_buf;
    if got != txids[key_i] {
        *miss_peeks = miss_peeks.saturating_add(1);
        let still = submit_next_id_or_done(side, w, session, side_fd, count, slot as u32, id_ns)?;
        return Ok(if still {
            SqeOutcome::SqePushed
        } else {
            done[key_i] = true;
            SqeOutcome::Finished
        });
    }

    // Identity hit → STAGE_IDX for body_range on the same session.
    crate::head_resolve_stats::add_hit_rank(w.pending_rank.max(1) as u64);
    let fk = Fk(w.pending_fk);
    let t_idx = Instant::now();
    let plan = match table.body.plan_body_range_idx(fk) {
        Ok(p) => p,
        Err(StoreError::NotFound) | Err(StoreError::Corrupt(_)) | Err(StoreError::InvalidFk) => {
            *idx_ns = idx_ns.saturating_add(t_idx.elapsed().as_nanos() as u64);
            let still =
                submit_next_id_or_done(side, w, session, side_fd, count, slot as u32, id_ns)?;
            return Ok(if still {
                SqeOutcome::SqePushed
            } else {
                done[key_i] = true;
                SqeOutcome::Finished
            });
        }
        Err(e) => return Err(e),
    };
    *idx_ns = idx_ns.saturating_add(t_idx.elapsed().as_nanos() as u64);

    if plan.pages.is_empty() {
        done[key_i] = true;
        return Ok(SqeOutcome::Finished);
    }

    let bufs: Vec<Vec<u8>> = plan.pages.iter().map(|p| vec![0u8; p.want]).collect();
    w.idx_plan = Some(plan);
    w.idx_bufs = bufs;
    w.idx_page_i = 0;
    submit_idx_page(slots[slot].as_mut().unwrap(), session, slot as u32)?;
    let _ = winner; // set on STAGE_IDX complete
    Ok(SqeOutcome::SqePushed)
}

/// STAGE_IDX CQE: more pages → push next; else decode body_range and finish.
fn on_stage_idx(
    _table: &TxTable,
    slots: &mut [Option<KeyWork>],
    slot: usize,
    session: &mut UringSession,
    side: &TxidBody,
    side_fd: RawFd,
    count: u64,
    res: i32,
    id_ns: &mut u64,
    idx_ns: &mut u64,
    winner: &mut [Option<(Fk, (u64, u64))>],
    done: &mut [bool],
    _miss_peeks: &mut u64,
    first_fks: &[u64],
    local_age: &mut [u64; crate::head_resolve_stats::AGE_CAP],
) -> Result<SqeOutcome, StoreError> {
    let w = slots[slot].as_mut().unwrap();
    let page_i = w.idx_page_i as usize;
    let plan = w.idx_plan.as_ref().expect("STAGE_IDX without plan");
    let page = &plan.pages[page_i];
    let want = page.want;

    if res < 0 {
        if res == -95 && crate::bulk_io::rwf_dontcache_ok() && page.rw_flags != 0 {
            crate::bulk_io::note_rwf_dontcache_unsupported();
            // Retry this page without DONTCACHE.
            let fd = page.fd;
            let off = page.page_off;
            let ud = uring_session::pack_ud(STAGE_IDX, slot as u32);
            let buf = &mut w.idx_bufs[page_i];
            buf.fill(0);
            session.push_pread_flags(fd, off, buf, ud, 0)?;
            return Ok(SqeOutcome::SqePushed);
        }
        // Treat as miss — try next cand identity.
        w.idx_plan = None;
        w.idx_bufs.clear();
        w.idx_page_i = 0;
        let still = submit_next_id_or_done(side, w, session, side_fd, count, slot as u32, id_ns)?;
        return Ok(if still {
            SqeOutcome::SqePushed
        } else {
            let key_i = w.key_i as usize;
            done[key_i] = true;
            SqeOutcome::Finished
        });
    }

    if (res as usize) < want {
        // Short read — fill remainder via libc pread on the same fd (no TLS nest).
        let fd = page.fd;
        let off = page.page_off;
        let buf = &mut w.idx_bufs[page_i];
        let rc = unsafe {
            libc::pread(
                fd,
                buf.as_mut_ptr() as *mut libc::c_void,
                want,
                off as libc::off_t,
            )
        };
        if rc < 0 || (rc as usize) < want {
            w.idx_plan = None;
            w.idx_bufs.clear();
            w.idx_page_i = 0;
            let still =
                submit_next_id_or_done(side, w, session, side_fd, count, slot as u32, id_ns)?;
            return Ok(if still {
                SqeOutcome::SqePushed
            } else {
                let key_i = w.key_i as usize;
                done[key_i] = true;
                SqeOutcome::Finished
            });
        }
    }

    // More idx pages?
    if page_i + 1 < plan.pages.len() {
        w.idx_page_i = (page_i + 1) as u8;
        submit_idx_page(w, session, slot as u32)?;
        return Ok(SqeOutcome::SqePushed);
    }

    // Decode body_range from filled pages.
    let t0 = Instant::now();
    let page_refs: Vec<&[u8]> = w.idx_bufs.iter().map(|b| b.as_slice()).collect();
    let range = match plan.decode_range(&page_refs) {
        Ok((off, len)) if len > 0 => Some((off, len)),
        Ok(_) | Err(StoreError::Corrupt(_)) => None,
        Err(e) => return Err(e),
    };
    *idx_ns = idx_ns.saturating_add(t0.elapsed().as_nanos() as u64);

    let key_i = w.key_i as usize;
    let fk = Fk(w.pending_fk);
    if let Some(r) = range {
        winner[key_i] = Some((fk, r));
        crate::head_resolve_stats::note_local_hit_age(local_age, first_fks, fk.0);
        done[key_i] = true;
        return Ok(SqeOutcome::Finished);
    }

    // Empty/corrupt range — try next cand.
    w.idx_plan = None;
    w.idx_bufs.clear();
    w.idx_page_i = 0;
    let still = submit_next_id_or_done(side, w, session, side_fd, count, slot as u32, id_ns)?;
    Ok(if still {
        SqeOutcome::SqePushed
    } else {
        done[key_i] = true;
        SqeOutcome::Finished
    })
}

fn submit_idx_page(
    work: &mut KeyWork,
    session: &mut UringSession,
    slot: u32,
) -> Result<(), StoreError> {
    let page_i = work.idx_page_i as usize;
    let plan = work.idx_plan.as_ref().expect("idx plan");
    let page = &plan.pages[page_i];
    let fd = page.fd;
    let off = page.page_off;
    let flags = page.rw_flags;
    let ud = uring_session::pack_ud(STAGE_IDX, slot);
    let buf = &mut work.idx_bufs[page_i];
    buf.fill(0);
    session.push_pread_flags(fd, off, buf, ud, flags)?;
    Ok(())
}

fn resolve_denserels_uring(
    table: &TxTable,
    txids: &[[u8; 32]],
) -> Result<
    (
        Vec<(
            [u8; 32],
            Option<(Fk, Option<(TxRecord, Vec<OutputRecord>, Vec<u32>)>)>,
        )>,
        u64,
    ),
    StoreError,
> {
    // fk+range via the fused TLS machine, then denserels body for winners
    // (separate bulk pipeline — range already known, no re-idx).
    let ranges = resolve_fk_and_range_uring(table, txids)?;
    let mut dens_ns = 0u64;
    let mut dens_decoded: std::collections::HashMap<
        usize,
        (TxRecord, Vec<OutputRecord>, Vec<u32>),
    > = std::collections::HashMap::new();

    let mut need: Vec<(usize, Fk, (u64, u64))> = Vec::new();
    for (i, (_tid, row)) in ranges.iter().enumerate() {
        if let Some((fk, range)) = row {
            need.push((i, *fk, *range));
        }
    }
    if !need.is_empty() {
        let t_dens = Instant::now();
        let mut jobs: Vec<IdxBodyJob> = need
            .iter()
            .map(|(_, fk, range)| IdxBodyJob::new(fk.0, Some(*range)))
            .collect();
        run_idx_body_pipeline(&table.body, &mut jobs, BodyMode::OutsDenserels)?;
        dens_ns = t_dens.elapsed().as_nanos() as u64;
        for ((ki, fk, _), job) in need.into_iter().zip(jobs.into_iter()) {
            if !job.ok || job.body.is_empty() {
                continue;
            }
            match decode_packed_tx_outs_with_spender_rels_secret(&job.body, Some(&table.secret)) {
                Ok(mut decoded) => {
                    if let Ok(tid) = table.txid_sidefile().get(fk) {
                        decoded.0.txid = tid;
                    }
                    dens_decoded.insert(ki, decoded);
                }
                Err(StoreError::NotFound) | Err(StoreError::Corrupt(_)) => {}
                Err(e) => return Err(e),
            }
        }
    }

    let mut out = Vec::with_capacity(txids.len());
    for (i, (txid, row)) in ranges.into_iter().enumerate() {
        let mapped = row.map(|(fk, _range)| (fk, dens_decoded.remove(&i)));
        out.push((txid, mapped));
    }
    Ok((out, dens_ns))
}

/// Submit next cand sidefile identity, or false if candidates exhausted.
fn submit_next_id_or_done(
    side: &TxidBody,
    work: &mut KeyWork,
    session: &mut UringSession,
    side_fd: RawFd,
    count: u64,
    slot: u32,
    id_ns: &mut u64,
) -> Result<bool, StoreError> {
    let side_n = side.count();
    while work.cand_i < work.cands.len() {
        let rank = (work.cand_i + 1) as u32;
        let fk = work.cands[work.cand_i];
        work.cand_i += 1;
        if fk == 0 || fk > count {
            continue;
        }
        let t0 = Instant::now();
        let off = match TxidBody::entry_offset(fk) {
            Ok(o) => o,
            Err(_) => {
                *id_ns = id_ns.saturating_add(t0.elapsed().as_nanos() as u64);
                continue;
            }
        };
        *id_ns = id_ns.saturating_add(t0.elapsed().as_nanos() as u64);
        work.pending_fk = fk;
        work.pending_rank = rank;
        work.id_buf = [0u8; 32];
        work.idx_plan = None;
        work.idx_bufs.clear();
        work.idx_page_i = 0;
        let ud = uring_session::pack_ud(STAGE_ID, slot);
        let rw_flags = crate::dontcache_policy::sidefile_sqe_rw_flags(fk, side_n);
        session.push_pread_flags(side_fd, off, &mut work.id_buf, ud, rw_flags)?;
        return Ok(true);
    }
    Ok(false)
}

fn arm_keys(
    _table: &TxTable,
    txids: &[[u8; 32]],
    cands_by_key: &[Vec<u64>],
    side: &TxidBody,
    session: &mut UringSession,
    side_fd: RawFd,
    count: u64,
    free_slots: &mut Vec<usize>,
    slots: &mut [Option<KeyWork>],
    in_flight: &mut usize,
    next_key: &mut usize,
    done: &mut [bool],
    id_ns: &mut u64,
) -> Result<(), StoreError> {
    while *next_key < txids.len()
        && *in_flight < MAX_IN_FLIGHT
        && session.free_sq() > 0
        && !free_slots.is_empty()
    {
        let key_i = *next_key;
        *next_key += 1;
        // Wave-2: winners already marked done; skip without taking a slot.
        if done[key_i] {
            continue;
        }
        let slot = free_slots.pop().unwrap();
        let cands = cands_by_key[key_i].clone();
        if cands.is_empty() {
            free_slots.push(slot);
            done[key_i] = true;
            continue;
        }
        slots[slot] = Some(KeyWork {
            key_i: key_i as u32,
            cands,
            cand_i: 0,
            id_buf: [0u8; 32],
            pending_fk: 0,
            pending_rank: 0,
            idx_plan: None,
            idx_bufs: Vec::new(),
            idx_page_i: 0,
        });
        let submitted = {
            let w = slots[slot].as_mut().unwrap();
            submit_next_id_or_done(side, w, session, side_fd, count, slot as u32, id_ns)?
        };
        if submitted {
            *in_flight += 1;
        } else {
            slots[slot] = None;
            free_slots.push(slot);
            done[key_i] = true;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tx_table::{InputRecord, OutputRecord, TxRecord, TxTable};
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};

    fn tmp(name: &str) -> PathBuf {
        static N: AtomicU64 = AtomicU64::new(0);
        let id = N.fetch_add(1, Ordering::Relaxed);
        let p = std::env::temp_dir().join(format!("rbitcoin-head-res-{name}-{id}"));
        let _ = std::fs::remove_dir_all(&p);
        std::fs::create_dir_all(&p).unwrap();
        p
    }

    fn seed_table(n: u8) -> (PathBuf, TxTable, Vec<[u8; 32]>) {
        let dir = tmp("seed");
        let t = TxTable::create(&dir).unwrap();
        let mut items = Vec::new();
        let mut txids = Vec::new();
        for i in 0..n {
            let mut tid = [0u8; 32];
            tid[0] = i;
            tid[1] = 0xa5;
            tid[2] = 0x5a;
            txids.push(tid);
            let tx = TxRecord {
                txid: tid,
                version: 1,
                locktime: 0,
                input_start_fk: Fk::NULL,
                input_count: 1,
                output_start_fk: Fk::NULL,
                output_count: 1,
            };
            let script: Vec<u8> = (0..((i as usize % 17) + 1)).map(|b| b as u8).collect();
            items.push((
                tx,
                vec![InputRecord::coinbase(u32::MAX, vec![], vec![])],
                vec![OutputRecord::unspent(1000 + i as i64, script)],
            ));
        }
        let _fks = t.put_full_batch_indexed(&items, true).unwrap();
        (dir, t, txids)
    }

    /// Uring machine returns same (fk, body_range) as sequential pread path.
    #[test]
    fn uring_fk_and_range_matches_pread() {
        let (dir, t, txids) = seed_table(40);
        let pread = resolve_fk_and_range_pread(&t, &txids).unwrap();
        // Public entry (uring when available, else pread) must match pure pread.
        let via = resolve_fk_and_range_batch(&t, &txids).unwrap();
        assert_eq!(pread.len(), via.len());
        for (a, b) in pread.iter().zip(via.iter()) {
            assert_eq!(a.0, b.0);
            assert_eq!(a.1, b.1, "txid[0]={}", a.0[0]);
        }
        // Every hit has a non-empty body_range matching record_range.
        for (_tid, row) in &pread {
            if let Some((fk, range)) = row {
                assert_eq!(t.body.record_range(*fk).unwrap(), *range);
            }
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Single-segment store: every winner is sealed_age 0 (open/tip).
    ///
    /// Multi-age mapping is covered by `head_resolve_stats::sealed_age_for_fk_*`.
    /// Global AGE_HIT atomics race parallel tests, so we pin mapping on winners
    /// via `first_fks` and only require the process counters moved for age 0.
    #[test]
    fn resolve_records_winner_age_open_segment() {
        crate::segmented_head::SegmentedTxHead::test_set_soft_span_bytes(0);
        let _ = crate::head_resolve_stats::sample_and_reset();
        let (dir, t, txids) = seed_table(16);
        assert_eq!(
            t.head.segment_count(),
            1,
            "unexpected segs={}",
            t.head.segment_count()
        );
        let first = t.head.first_fks_snapshot();
        assert_eq!(first, vec![1]);
        let got = resolve_fk_and_range_batch(&t, &txids).unwrap();
        let hits = got.iter().filter(|(_, r)| r.is_some()).count() as u64;
        assert_eq!(hits, txids.len() as u64);
        for (_tid, row) in &got {
            if let Some((fk, _)) = row {
                assert_eq!(
                    crate::head_resolve_stats::sealed_age_for_fk(&first, fk.0),
                    Some(0),
                    "fk={}",
                    fk.0
                );
            }
        }
        let s = crate::head_resolve_stats::sample_and_reset();
        // Our hits are age 0; concurrent resolve tests may add more age-0 counts.
        assert!(
            s.age_hit[0] >= hits,
            "age0={} hits={hits} age_hit={:?}",
            s.age_hit[0],
            &s.age_hit[..8]
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// On a small (no cold segs) store, hot∪cold cands equal full probe.
    #[test]
    fn hot_plus_cold_cands_match_full_probe() {
        let (dir, t, txids) = seed_table(24);
        let mixed: Vec<[u8; 32]> = txids.iter().map(|x| t.secret.mix_txid(x)).collect();
        let full = t.head.probe_candidates_batch(&mixed).unwrap();
        let hot = t.head.probe_candidates_batch_hot(&mixed).unwrap();
        let active = vec![true; mixed.len()];
        let cold = t.head.probe_candidates_batch_cold(&mixed, &active).unwrap();
        assert_eq!(full.len(), hot.len());
        for i in 0..full.len() {
            let mut merged = hot[i].clone();
            merged.extend(cold[i].iter().copied());
            assert_eq!(merged, full[i], "key {i}");
        }
        // Tiny store: everything is hot; cold must be empty.
        assert!(cold.iter().all(|c| c.is_empty()));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn miss_and_deepest_create_wins() {
        let dir = tmp("bip30");
        let t = TxTable::create(&dir).unwrap();
        let txid = [0xcd; 32];
        let mk = |hint: u8| {
            (
                TxRecord {
                    txid,
                    version: 1,
                    locktime: 0,
                    input_start_fk: Fk::NULL,
                    input_count: 1,
                    output_start_fk: Fk::NULL,
                    output_count: 1,
                },
                vec![InputRecord {
                    prev_txid: [0u8; 32],
                    create_fk: Fk::NULL,
                    prev_index: u32::MAX,
                    sequence: u32::MAX,
                    script_sig: vec![hint],
                    witness: vec![],
                }],
                vec![OutputRecord::unspent(1, vec![0x51])],
            )
        };
        let _fk1 = t.put_full_batch_indexed(&[mk(1)], true).unwrap()[0];
        let fk2 = t.put_full_batch_indexed(&[mk(2)], true).unwrap()[0];
        let got = resolve_fk_and_range_batch(&t, &[txid, [0xff; 32]]).unwrap();
        assert_eq!(got[0].1.map(|(f, _)| f), Some(fk2));
        assert_eq!(got[1].1, None);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn plan_body_range_idx_matches_record_range() {
        let (dir, t, _txids) = seed_table(20);
        let count = t.body.count();
        for id in 1..=count {
            let fk = Fk(id);
            let expected = t.body.record_range(fk).unwrap();
            let plan = t.body.plan_body_range_idx(fk).unwrap();
            assert!(!plan.pages.is_empty());
            let bufs: Vec<Vec<u8>> = plan
                .pages
                .iter()
                .map(|p| {
                    let mut b = vec![0u8; p.want];
                    let rc = unsafe {
                        libc::pread(
                            p.fd,
                            b.as_mut_ptr() as *mut libc::c_void,
                            p.want,
                            p.page_off as libc::off_t,
                        )
                    };
                    assert!(rc > 0, "pread idx page");
                    b
                })
                .collect();
            let refs: Vec<&[u8]> = bufs.iter().map(|b| b.as_slice()).collect();
            let got = plan.decode_range(&refs).unwrap();
            assert_eq!(got, expected, "fk={fk:?}");
        }
        let _ = std::fs::remove_dir_all(&dir);
    }
}
