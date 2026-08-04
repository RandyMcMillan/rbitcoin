//! Plan Shape A head resolve: **txids in → denserels out** (or fk+range short-circuit).
//!
//! Schema **13+** fused FdOnly machine (no multi‑GiB map):
//! 1. **probe** — [`SegmentedTxHead::probe_candidates_batch`] (page-coalesced)
//! 2. **identity** — dense `txid.body` sidefile (fixed `fk → offset`)
//! 3. **idx** — body range for winners (stamp short-circuit)
//! 4. **denserels** — full packed body when outs are needed
//!
//! [`resolve_fk_and_range_batch`] is the **stamp short-circuit**: same per-key
//! depth-first pipeline as denserels resolve, **stops before denserels**, returns
//! `(fk, body_range)` so prep can denserels-load by offset without re-idx.
//!
//! **IO shape:** one io_uring session streams up to [`MAX_IN_FLIGHT`] keys in
//! parallel. Each key walks its candidate list **depth-first** (CQE hit → done;
//! miss → next cand). There is **no** cross-key depth-round batching of all
//! keys at depth 0 then depth 1 — that regressed plan head_fk wall under RES=0.
//!
//! Backend: `RBITCOIN_HEAD_RESOLVE_IO` / global `RBITCOIN_IO` (`uring` \| `pread`).
//!
//! **Experiment:** ring + key in-flight depth **1024** (was 128) for confirm-plan
//! head resolve — more concurrent sidefile/idx peeks under large `head_fk` waves.

use crate::error::StoreError;
use crate::idx_body_pipeline::{run_idx_body_pipeline, BodyMode, IdxBodyJob};
use crate::io_backend::{self, ReadIoBackend};
use crate::tx_table::{
    decode_packed_tx_outs_with_spender_rels_secret, OutputRecord, TxRecord, TxTable,
};
use crate::txid_body::{TXID_ENTRY_LEN, TxidBody};
use crate::uring_session::{self, UringSession};
use rbitcoin_primitives::Fk;
use std::os::fd::RawFd;
use std::time::Instant;

/// Concurrent keys in the plan head-resolve uring machine (and SQ/CQ size).
///
/// Experiment: 1024 (was 128, matching [`uring_session::DEFAULT_ENTRIES`]).
const MAX_IN_FLIGHT: usize = 1024;
/// io_uring SQ/CQ entries for plan head resolve (must be ≥ [`MAX_IN_FLIGHT`]).
const PLAN_URING_ENTRIES: u32 = 1024;

/// Sidefile identity pread (32 B).
const STAGE_ID: u64 = 1;

/// One plan key: candidates deepest-first; identity buf stable for in-flight SQE.
struct KeyWork {
    key_i: u32,
    cands: Vec<u64>,
    cand_i: usize,
    /// Sidefile identity buffer (32 B).
    id_buf: [u8; 32],
    pending_fk: u64,
    pending_rank: u32,
}

/// Stamp short-circuit: **txids → (fk, body_range)** via probe + sidefile + idx.
///
/// Depth-first per key (io_uring when available). `body_range` from idx after
/// identity match so prep denserels-loads by offset (skip `tx.idx`).
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
            Option<(
                Fk,
                Option<(TxRecord, Vec<OutputRecord>, Vec<u32>)>,
            )>,
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

// ── pread: depth-first per key (no cross-key depth rounds) ──────────────────

fn resolve_fk_and_range_pread(
    table: &TxTable,
    txids: &[[u8; 32]],
) -> Result<Vec<([u8; 32], Option<(Fk, (u64, u64))>)>, StoreError> {
    crate::head_resolve_stats::add_keys(txids.len() as u64);

    let mixed: Vec<[u8; 32]> = txids.iter().map(|t| table.secret.mix_txid(t)).collect();
    let t_probe = Instant::now();
    let all_cands = table.head.probe_candidates_batch(&mixed)?;
    let probe_ns = t_probe.elapsed().as_nanos() as u64;
    let cands_total: u64 = all_cands.iter().map(|c| c.len() as u64).sum();
    crate::head_resolve_stats::add_probe(probe_ns);
    crate::head_resolve_stats::add_cands(cands_total);

    let side = table.txid_sidefile();
    let mut winner: Vec<Option<(Fk, (u64, u64))>> = vec![None; txids.len()];
    let mut body_lookups = 0u64;
    let mut miss_peeks = 0u64;
    let mut id_ns = 0u64;
    let mut idx_ns = 0u64;

    for (ki, cands) in all_cands.iter().enumerate() {
        for (rank0, &fk) in cands.iter().enumerate() {
            let rank = (rank0 + 1) as u64;
            let t_id = Instant::now();
            let got = match side.get(fk) {
                Ok(t) => t,
                Err(StoreError::NotFound) | Err(StoreError::InvalidFk) => {
                    id_ns = id_ns.saturating_add(t_id.elapsed().as_nanos() as u64);
                    miss_peeks = miss_peeks.saturating_add(1);
                    body_lookups = body_lookups.saturating_add(1);
                    continue;
                }
                Err(e) => return Err(e),
            };
            id_ns = id_ns.saturating_add(t_id.elapsed().as_nanos() as u64);
            body_lookups = body_lookups.saturating_add(1);
            if got != txids[ki] {
                miss_peeks = miss_peeks.saturating_add(1);
                continue;
            }
            crate::head_resolve_stats::add_hit_rank(rank);
            let t_idx = Instant::now();
            match table.body.record_range(fk) {
                Ok((off, len)) if len > 0 => {
                    winner[ki] = Some((fk, (off, len)));
                }
                Ok(_) | Err(StoreError::NotFound) | Err(StoreError::Corrupt(_)) => {}
                Err(e) => return Err(e),
            }
            idx_ns = idx_ns.saturating_add(t_idx.elapsed().as_nanos() as u64);
            break; // depth-first short-circuit
        }
    }

    crate::head_resolve_stats::add_body(id_ns);
    crate::head_resolve_stats::add_idx(idx_ns);
    crate::head_resolve_stats::add_body_lookups(body_lookups);
    crate::head_resolve_stats::add_miss_peeks(miss_peeks);

    Ok(txids
        .iter()
        .enumerate()
        .map(|(i, t)| (*t, winner[i]))
        .collect())
}

fn resolve_denserels_pread(
    table: &TxTable,
    txids: &[[u8; 32]],
) -> Result<
    (
        Vec<(
            [u8; 32],
            Option<(
                Fk,
                Option<(TxRecord, Vec<OutputRecord>, Vec<u32>)>,
            )>,
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

// ── uring: per-key depth-first sidefile identity ────────────────────────────

fn resolve_fk_and_range_uring(
    table: &TxTable,
    txids: &[[u8; 32]],
) -> Result<Vec<([u8; 32], Option<(Fk, (u64, u64))>)>, StoreError> {
    crate::head_resolve_stats::add_keys(txids.len() as u64);

    let mut session = UringSession::new(PLAN_URING_ENTRIES)?;
    let side = table.txid_sidefile();
    let side_fd: RawFd = side.body_read_fd();
    let side_path = side.file_path().to_path_buf();
    let count = table.body.count();

    let mixed: Vec<[u8; 32]> = txids.iter().map(|t| table.secret.mix_txid(t)).collect();
    let t_probe = Instant::now();
    let all_cands = table.head.probe_candidates_batch(&mixed)?;
    let probe_ns = t_probe.elapsed().as_nanos() as u64;
    let cands_u64: Vec<Vec<u64>> = all_cands
        .into_iter()
        .map(|v| v.into_iter().map(|f| f.0).collect())
        .collect();
    let cands_total: u64 = cands_u64.iter().map(|v| v.len() as u64).sum();

    let mut winner: Vec<Option<(Fk, (u64, u64))>> = vec![None; txids.len()];
    let mut done = vec![false; txids.len()];
    let mut next_key = 0usize;
    let mut body_lookups = 0u64;
    let mut miss_peeks = 0u64;
    let mut id_ns = 0u64;
    let mut idx_ns = 0u64;

    let mut free_slots: Vec<usize> = (0..MAX_IN_FLIGHT).collect();
    let mut slots: Vec<Option<KeyWork>> = (0..MAX_IN_FLIGHT).map(|_| None).collect();
    let mut in_flight = 0usize;

    arm_id(
        table,
        txids,
        &cands_u64,
        side,
        &mut session,
        side_fd,
        count,
        &mut free_slots,
        &mut slots,
        &mut in_flight,
        &mut next_key,
        &mut done,
        &mut winner,
        &mut id_ns,
    )?;
    session.sync_submission();
    let _ = session.submit();

    while in_flight > 0 {
        let t_wait = Instant::now();
        let mut cqes = session.harvest_ready();
        if cqes.is_empty() {
            session.submit_and_wait_one()?;
            cqes = session.harvest_ready();
        }
        let wait_ns = t_wait.elapsed().as_nanos() as u64;

        for (ud, res) in cqes {
            let (kind, slot_u) = uring_session::unpack_ud(ud);
            let slot = slot_u as usize;
            if slot >= slots.len() || slots[slot].is_none() {
                return Err(StoreError::Corrupt("head resolve bad slot"));
            }
            if kind != STAGE_ID {
                return Err(StoreError::Corrupt("head resolve bad stage (range)"));
            }
            id_ns = id_ns.saturating_add(wait_ns);
            body_lookups = body_lookups.saturating_add(1);
            in_flight = in_flight.saturating_sub(1);

            if res < 0 {
                // ENOTSUP (-95) often means RWF_DONTCACHE unsupported (same as bulk_io).
                // Demote permanently and retry this identity once without flags.
                if res == -95 && crate::bulk_io::rwf_dontcache_ok() {
                    crate::bulk_io::note_rwf_dontcache_unsupported();
                    if let Some(w) = slots[slot].as_mut() {
                        let off = match TxidBody::entry_offset(w.pending_fk) {
                            Ok(o) => o,
                            Err(_) => {
                                slots[slot] = None;
                                free_slots.push(slot);
                                return Err(StoreError::io(
                                    &side_path,
                                    std::io::Error::from_raw_os_error(-res),
                                ));
                            }
                        };
                        w.id_buf = [0u8; 32];
                        let ud = uring_session::pack_ud(STAGE_ID, slot as u32);
                        session.push_pread_flags(side_fd, off, &mut w.id_buf, ud, 0)?;
                        in_flight += 1;
                        continue;
                    }
                }
                slots[slot] = None;
                free_slots.push(slot);
                return Err(StoreError::io(
                    &side_path,
                    std::io::Error::from_raw_os_error(-res),
                ));
            }
            if res as usize != TXID_ENTRY_LEN as usize {
                // Short identity — try next cand.
                let still = submit_next_id_or_done(
                    table,
                    side,
                    slots[slot].as_mut().unwrap(),
                    &mut session,
                    side_fd,
                    count,
                    slot as u32,
                    &mut id_ns,
                )?;
                if still {
                    in_flight += 1;
                } else {
                    let w = slots[slot].take().unwrap();
                    done[w.key_i as usize] = true;
                    free_slots.push(slot);
                }
                continue;
            }

            let still_or_done = on_id_complete_range(
                table,
                txids,
                &mut slots,
                slot,
                &mut session,
                side,
                side_fd,
                count,
                &mut id_ns,
                &mut idx_ns,
                &mut winner,
                &mut done,
                &mut miss_peeks,
            )?;
            match still_or_done {
                IdOutcome::NextCandInFlight => {
                    in_flight += 1;
                }
                IdOutcome::Finished => {
                    slots[slot] = None;
                    free_slots.push(slot);
                }
            }
        }

        arm_id(
            table,
            txids,
            &cands_u64,
            side,
            &mut session,
            side_fd,
            count,
            &mut free_slots,
            &mut slots,
            &mut in_flight,
            &mut next_key,
            &mut done,
            &mut winner,
            &mut id_ns,
        )?;
        session.sync_submission();
        let _ = session.submit();
    }

    crate::head_resolve_stats::add_probe(probe_ns);
    crate::head_resolve_stats::add_idx(idx_ns);
    crate::head_resolve_stats::add_body(id_ns);
    crate::head_resolve_stats::add_cands(cands_total);
    crate::head_resolve_stats::add_body_lookups(body_lookups);
    crate::head_resolve_stats::add_miss_peeks(miss_peeks);

    Ok(txids
        .iter()
        .enumerate()
        .map(|(i, t)| (*t, winner[i]))
        .collect())
}

enum IdOutcome {
    /// Another STAGE_ID SQE was pushed for this slot.
    NextCandInFlight,
    /// Key finished (hit with range, or candidates exhausted).
    Finished,
}

/// Identity CQE for range short-circuit: match → idx range + done; miss → next id.
fn on_id_complete_range(
    table: &TxTable,
    txids: &[[u8; 32]],
    slots: &mut [Option<KeyWork>],
    slot: usize,
    session: &mut UringSession,
    side: &TxidBody,
    side_fd: RawFd,
    count: u64,
    id_ns: &mut u64,
    idx_ns: &mut u64,
    winner: &mut [Option<(Fk, (u64, u64))>],
    done: &mut [bool],
    miss_peeks: &mut u64,
) -> Result<IdOutcome, StoreError> {
    let w = slots[slot].as_mut().unwrap();
    let key_i = w.key_i as usize;
    let got = w.id_buf;
    if got != txids[key_i] {
        *miss_peeks = miss_peeks.saturating_add(1);
        let still = submit_next_id_or_done(
            table, side, w, session, side_fd, count, slot as u32, id_ns,
        )?;
        return Ok(if still {
            IdOutcome::NextCandInFlight
        } else {
            done[key_i] = true;
            IdOutcome::Finished
        });
    }
    crate::head_resolve_stats::add_hit_rank(w.pending_rank.max(1) as u64);
    let fk = Fk(w.pending_fk);
    let t_idx = Instant::now();
    match table.body.record_range(fk) {
        Ok((off, len)) if len > 0 => {
            winner[key_i] = Some((fk, (off, len)));
        }
        Ok(_) | Err(StoreError::NotFound) | Err(StoreError::Corrupt(_)) => {
            // Identity hit but no range — treat as miss, try next cand.
            *idx_ns = idx_ns.saturating_add(t_idx.elapsed().as_nanos() as u64);
            let still = submit_next_id_or_done(
                table, side, w, session, side_fd, count, slot as u32, id_ns,
            )?;
            return Ok(if still {
                IdOutcome::NextCandInFlight
            } else {
                done[key_i] = true;
                IdOutcome::Finished
            });
        }
        Err(e) => return Err(e),
    }
    *idx_ns = idx_ns.saturating_add(t_idx.elapsed().as_nanos() as u64);
    done[key_i] = true;
    Ok(IdOutcome::Finished)
}

fn resolve_denserels_uring(
    table: &TxTable,
    txids: &[[u8; 32]],
) -> Result<
    (
        Vec<(
            [u8; 32],
            Option<(
                Fk,
                Option<(TxRecord, Vec<OutputRecord>, Vec<u32>)>,
            )>,
        )>,
        u64,
    ),
    StoreError,
> {
    // Identity+range via the streaming machine, then denserels body for winners.
    // (Keeps denserels wave as one bulk idx_body pipeline — already efficient.)
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
    _table: &TxTable,
    side: &TxidBody,
    work: &mut KeyWork,
    session: &mut UringSession,
    side_fd: RawFd,
    count: u64,
    slot: u32,
    id_ns: &mut u64,
) -> Result<bool, StoreError> {
    // Sidefile published count (matches Class A count when consistent).
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
        let ud = uring_session::pack_ud(STAGE_ID, slot);
        // Schema 13: far-from-tail sidefile + rwf_dontcache_ok gate.
        let rw_flags = crate::dontcache_policy::sidefile_sqe_rw_flags(fk, side_n);
        session.push_pread_flags(side_fd, off, &mut work.id_buf, ud, rw_flags)?;
        return Ok(true);
    }
    Ok(false)
}

fn arm_id(
    table: &TxTable,
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
    winner: &mut [Option<(Fk, (u64, u64))>],
    id_ns: &mut u64,
) -> Result<(), StoreError> {
    let _ = winner;
    while *next_key < txids.len()
        && *in_flight < MAX_IN_FLIGHT
        && session.free_sq() > 0
        && !free_slots.is_empty()
    {
        let key_i = *next_key;
        *next_key += 1;
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
        });
        let submitted = {
            let w = slots[slot].as_mut().unwrap();
            submit_next_id_or_done(table, side, w, session, side_fd, count, slot as u32, id_ns)?
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
