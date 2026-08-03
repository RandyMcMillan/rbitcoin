//! Plan Shape A head resolve: **txids in → denserels out**.
//!
//! Fused FdOnly machine (no multi‑GiB map):
//! 1. **probe** — [`SegmentedTxHead::probe_candidates_batch`] (page-coalesced)
//! 2. **idx** — OS-page pread of the candidate idx slot (uring or sync)
//! 3. **Prefix33** — ≤32 B body prefix; multi-cand miss → next cand at idx
//! 4. **denserels** — full packed body for the winner; decode outs + denserels
//!
//! Single-cand keys skip Prefix33 and load denserels directly (identity via
//! txid at body start) — same semantics as the prior Shape A path.
//!
//! Backend: `RBITCOIN_HEAD_RESOLVE_IO` / global `RBITCOIN_IO` (`uring` \| `pread`).

use crate::error::StoreError;
use crate::idx_body_pipeline::{run_idx_body_pipeline, BodyMode, IdxBodyJob};
use crate::io_backend::{self, ReadIoBackend};
use crate::tx_idx::IDX_OS_PAGE;
use crate::tx_table::{
    decode_packed_tx_outs_with_spender_rels_secret, OutputRecord, TxRecord, TxTable,
};
use crate::uring_session::{self, UringSession};
use rbitcoin_primitives::Fk;
use std::os::fd::RawFd;
use std::path::Path;
use std::time::Instant;

const MAX_IN_FLIGHT: usize = 128;

const STAGE_IDX: u64 = 1;
const STAGE_PREFIX: u64 = 2;
const STAGE_DENS: u64 = 3;

/// One plan key: candidates deepest-first, buffers stable for in-flight SQEs.
struct KeyWork {
    key_i: u32,
    cands: Vec<u64>,
    cand_i: usize,
    /// Single-cand: denserels body is also the identity check.
    single_cand: bool,
    idx_page: Vec<u8>,
    prefix_buf: [u8; 32],
    prefix_len: usize,
    dens_body: Vec<u8>,
    body_range: Option<(u64, u64)>,
    pending_fk: u64,
    pending_rank: u32,
}

/// Resolve many parent txids to create fk + denserels (plan Shape A).
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
        ReadIoBackend::Uring => match resolve_uring(table, txids) {
            Ok(v) => Ok(v),
            // Fall back if ring open fails (agent 9p / disabled).
            Err(_) => resolve_pread(table, txids),
        },
        ReadIoBackend::Pread => resolve_pread(table, txids),
    }
}

// ── pread path: batch probe + idx/body pipelines ───────────────────────────

fn resolve_pread(
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
    crate::head_resolve_stats::add_keys(txids.len() as u64);

    let mixed: Vec<[u8; 32]> = txids.iter().map(|t| table.secret.mix_txid(t)).collect();
    let t_probe = Instant::now();
    let all_cands = table.head.probe_candidates_batch(&mixed)?;
    let probe_ns = t_probe.elapsed().as_nanos() as u64;
    let cands_total: u64 = all_cands.iter().map(|c| c.len() as u64).sum();
    crate::head_resolve_stats::add_probe(probe_ns);
    crate::head_resolve_stats::add_cands(cands_total);

    let mut winner_fk: Vec<Option<Fk>> = vec![None; txids.len()];
    let mut dens_decoded: std::collections::HashMap<
        usize,
        (TxRecord, Vec<OutputRecord>, Vec<u32>),
    > = std::collections::HashMap::new();
    let mut dens_ns_acc = 0u64;

    // Single-cand: denserels-as-identity (one full body).
    let singles: Vec<(usize, Fk)> = all_cands
        .iter()
        .enumerate()
        .filter_map(|(i, c)| {
            if c.len() == 1 {
                Some((i, c[0]))
            } else {
                None
            }
        })
        .collect();
    if !singles.is_empty() {
        let t_dens = Instant::now();
        let mut jobs: Vec<IdxBodyJob> = singles
            .iter()
            .map(|(_, fk)| IdxBodyJob::new(fk.0, None))
            .collect();
        run_idx_body_pipeline(&table.body, &mut jobs, BodyMode::OutsDenserels)?;
        dens_ns_acc = dens_ns_acc.saturating_add(t_dens.elapsed().as_nanos() as u64);
        let mut body_lookups = 0u64;
        for ((ki, fk), job) in singles.into_iter().zip(jobs.into_iter()) {
            if !job.ok || job.body.is_empty() {
                continue;
            }
            body_lookups = body_lookups.saturating_add(1);
            match TxTable::txid_from_body_prefix(&job.body) {
                Ok(got) if got == txids[ki] => {
                    crate::head_resolve_stats::add_hit_rank(1);
                    winner_fk[ki] = Some(fk);
                    match decode_packed_tx_outs_with_spender_rels_secret(
                        &job.body,
                        Some(&table.secret),
                    ) {
                        Ok(decoded) => {
                            dens_decoded.insert(ki, decoded);
                        }
                        Err(StoreError::NotFound) | Err(StoreError::Corrupt(_)) => {}
                        Err(e) => return Err(e),
                    }
                }
                Ok(_) => crate::head_resolve_stats::add_miss_peeks(1),
                Err(StoreError::NotFound) | Err(StoreError::Corrupt(_)) => {
                    crate::head_resolve_stats::add_miss_peeks(1);
                }
                Err(e) => return Err(e),
            }
        }
        crate::head_resolve_stats::add_body_lookups(body_lookups);
    }

    // Multi-cand: depth-round Prefix33 then denserels for winners.
    let multi_keys: Vec<usize> = all_cands
        .iter()
        .enumerate()
        .filter(|(_, c)| c.len() > 1)
        .map(|(i, _)| i)
        .collect();
    let mut need_dens: Vec<(usize, Fk)> = Vec::new();
    if !multi_keys.is_empty() {
        let max_depth = multi_keys
            .iter()
            .map(|&i| all_cands[i].len())
            .max()
            .unwrap_or(0);
        let mut unresolved: Vec<bool> = vec![false; txids.len()];
        for &i in &multi_keys {
            unresolved[i] = true;
        }
        for depth in 0..max_depth {
            let mut round: Vec<(usize, Fk, u8)> = Vec::new();
            for &ki in &multi_keys {
                if !unresolved[ki] {
                    continue;
                }
                if let Some(&fk) = all_cands[ki].get(depth) {
                    round.push((ki, fk, (depth as u8).saturating_add(1)));
                }
            }
            if round.is_empty() {
                break;
            }
            let t_pipe = Instant::now();
            let mut jobs: Vec<IdxBodyJob> = round
                .iter()
                .map(|(_, fk, _)| IdxBodyJob::new(fk.0, None))
                .collect();
            run_idx_body_pipeline(&table.body, &mut jobs, BodyMode::Prefix33)?;
            let pipe_ns = t_pipe.elapsed().as_nanos() as u64;
            crate::head_resolve_stats::add_idx(pipe_ns / 2);
            crate::head_resolve_stats::add_body(pipe_ns.saturating_sub(pipe_ns / 2));

            let mut body_lookups = 0u64;
            let mut miss_peeks = 0u64;
            for ((ki, fk, rank), job) in round.into_iter().zip(jobs.into_iter()) {
                if !job.ok || job.body.is_empty() {
                    continue;
                }
                body_lookups = body_lookups.saturating_add(1);
                match TxTable::txid_from_body_prefix(&job.body) {
                    Ok(got) if got == txids[ki] => {
                        crate::head_resolve_stats::add_hit_rank(rank as u64);
                        winner_fk[ki] = Some(fk);
                        unresolved[ki] = false;
                        need_dens.push((ki, fk));
                    }
                    Ok(_) => miss_peeks = miss_peeks.saturating_add(1),
                    Err(StoreError::NotFound) | Err(StoreError::Corrupt(_)) => {
                        miss_peeks = miss_peeks.saturating_add(1);
                    }
                    Err(e) => return Err(e),
                }
            }
            crate::head_resolve_stats::add_body_lookups(body_lookups);
            crate::head_resolve_stats::add_miss_peeks(miss_peeks);
            if !unresolved.iter().any(|&u| u) {
                break;
            }
        }
    }

    if !need_dens.is_empty() {
        let t_dens = Instant::now();
        let mut jobs: Vec<IdxBodyJob> = need_dens
            .iter()
            .map(|(_, fk)| IdxBodyJob::new(fk.0, None))
            .collect();
        run_idx_body_pipeline(&table.body, &mut jobs, BodyMode::OutsDenserels)?;
        dens_ns_acc = dens_ns_acc.saturating_add(t_dens.elapsed().as_nanos() as u64);
        for ((ki, _fk), job) in need_dens.into_iter().zip(jobs.into_iter()) {
            if !job.ok || job.body.is_empty() {
                continue;
            }
            match decode_packed_tx_outs_with_spender_rels_secret(&job.body, Some(&table.secret)) {
                Ok(decoded) => {
                    dens_decoded.insert(ki, decoded);
                }
                Err(StoreError::NotFound) | Err(StoreError::Corrupt(_)) => {}
                Err(e) => return Err(e),
            }
        }
    }

    let mut out = Vec::with_capacity(txids.len());
    for (i, txid) in txids.iter().enumerate() {
        let row = winner_fk[i].map(|fk| (fk, dens_decoded.remove(&i)));
        out.push((*txid, row));
    }
    Ok((out, dens_ns_acc))
}

// ── uring machine: probe → idx → prefix|dens → dens ────────────────────────

fn resolve_uring(
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
    crate::head_resolve_stats::add_keys(txids.len() as u64);

    let mut session = UringSession::new(uring_session::DEFAULT_ENTRIES)?;
    let body_fd: RawFd = table.body.body_read_fd();
    let body_path = table.body.body_file_path().to_path_buf();
    let body_pub = table.body.body_published_len();
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

    let mut winner_fk: Vec<Option<Fk>> = vec![None; txids.len()];
    let mut dens_decoded: std::collections::HashMap<
        usize,
        (TxRecord, Vec<OutputRecord>, Vec<u32>),
    > = std::collections::HashMap::new();
    let mut done = vec![false; txids.len()];
    let mut next_key = 0usize;
    let mut body_lookups = 0u64;
    let mut idx_ns = 0u64;
    let mut body_ns = 0u64;
    let mut dens_ns = 0u64;

    let mut free_slots: Vec<usize> = (0..MAX_IN_FLIGHT).collect();
    let mut slots: Vec<Option<KeyWork>> = (0..MAX_IN_FLIGHT).map(|_| None).collect();
    let mut in_flight = 0usize;

    arm(
        table,
        txids,
        &cands_u64,
        &mut session,
        count,
        body_fd,
        body_pub,
        &mut free_slots,
        &mut slots,
        &mut in_flight,
        &mut next_key,
        &mut done,
        &mut winner_fk,
        &mut idx_ns,
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
                return Err(StoreError::Corrupt("denserels resolve bad slot"));
            }

            match kind {
                STAGE_IDX => {
                    idx_ns = idx_ns.saturating_add(wait_ns);
                    if res < 0 {
                        slots[slot] = None;
                        free_slots.push(slot);
                        return Err(StoreError::io(
                            Path::new("tx.idx"),
                            std::io::Error::from_raw_os_error(-res),
                        ));
                    }
                    // still_in_flight: prefix/dens/next-idx SQE pushed for same slot.
                    let still = on_idx_complete(
                        table,
                        &mut slots,
                        slot,
                        res as usize,
                        &mut session,
                        body_fd,
                        body_pub,
                        count,
                        &mut idx_ns,
                    )?;
                    if !still {
                        // Candidates exhausted without a denserels submit.
                        if let Some(w) = slots[slot].take() {
                            let ki = w.key_i as usize;
                            done[ki] = true;
                            winner_fk[ki] = None;
                        }
                        free_slots.push(slot);
                        in_flight = in_flight.saturating_sub(1);
                    }
                }
                STAGE_PREFIX => {
                    body_ns = body_ns.saturating_add(wait_ns);
                    body_lookups = body_lookups.saturating_add(1);
                    if res < 0 {
                        slots[slot] = None;
                        free_slots.push(slot);
                        return Err(StoreError::io(
                            &body_path,
                            std::io::Error::from_raw_os_error(-res),
                        ));
                    }
                    let still = on_prefix_complete(
                        table,
                        txids,
                        &mut slots,
                        slot,
                        res as usize,
                        &mut session,
                        body_fd,
                        body_pub,
                        count,
                        &mut idx_ns,
                        &mut winner_fk,
                        &mut done,
                    )?;
                    if !still {
                        // Miss: all cands tried (winner_fk already None).
                        slots[slot] = None;
                        free_slots.push(slot);
                        in_flight = in_flight.saturating_sub(1);
                    }
                    // still=true → dens or next-idx SQE owns the slot (in_flight unchanged).
                }
                STAGE_DENS => {
                    dens_ns = dens_ns.saturating_add(wait_ns);
                    body_lookups = body_lookups.saturating_add(1);
                    in_flight = in_flight.saturating_sub(1);
                    if res < 0 {
                        slots[slot] = None;
                        free_slots.push(slot);
                        return Err(StoreError::io(
                            &body_path,
                            std::io::Error::from_raw_os_error(-res),
                        ));
                    }
                    on_dens_complete(
                        table,
                        txids,
                        &mut slots,
                        slot,
                        res as usize,
                        &mut dens_decoded,
                        &mut winner_fk,
                        &mut done,
                    )?;
                    slots[slot] = None;
                    free_slots.push(slot);
                }
                _ => return Err(StoreError::Corrupt("denserels resolve bad stage")),
            }
        }

        arm(
            table,
            txids,
            &cands_u64,
            &mut session,
            count,
            body_fd,
            body_pub,
            &mut free_slots,
            &mut slots,
            &mut in_flight,
            &mut next_key,
            &mut done,
            &mut winner_fk,
            &mut idx_ns,
        )?;
        session.sync_submission();
        let _ = session.submit();
    }

    crate::head_resolve_stats::add_probe(probe_ns);
    crate::head_resolve_stats::add_idx(idx_ns);
    crate::head_resolve_stats::add_body(body_ns.saturating_add(dens_ns));
    crate::head_resolve_stats::add_cands(cands_total);
    crate::head_resolve_stats::add_body_lookups(body_lookups);

    let mut out = Vec::with_capacity(txids.len());
    for (i, txid) in txids.iter().enumerate() {
        let row = winner_fk[i].map(|fk| (fk, dens_decoded.remove(&i)));
        out.push((*txid, row));
    }
    Ok((out, dens_ns))
}

/// After idx CQE: submit prefix (multi) or denserels (single). Returns true if still in flight.
fn on_idx_complete(
    table: &TxTable,
    slots: &mut [Option<KeyWork>],
    slot: usize,
    res_len: usize,
    session: &mut UringSession,
    body_fd: RawFd,
    body_pub: u64,
    count: u64,
    idx_ns: &mut u64,
) -> Result<bool, StoreError> {
    let w = slots[slot].as_mut().unwrap();
    if res_len != w.idx_page.len() {
        // short idx — try next cand
        return submit_next_idx_or_done(table, w, session, body_fd, body_pub, count, slot as u32, idx_ns);
    }
    let t0 = Instant::now();
    let range = match table.body.record_range(Fk(w.pending_fk)) {
        Ok(r) => r,
        Err(StoreError::NotFound) | Err(StoreError::Corrupt(_)) => {
            *idx_ns = idx_ns.saturating_add(t0.elapsed().as_nanos() as u64);
            return submit_next_idx_or_done(
                table, w, session, body_fd, body_pub, count, slot as u32, idx_ns,
            );
        }
        Err(e) => return Err(e),
    };
    *idx_ns = idx_ns.saturating_add(t0.elapsed().as_nanos() as u64);
    let (start, full_len) = range;
    if full_len == 0 || start.saturating_add(full_len) > body_pub {
        return submit_next_idx_or_done(
            table, w, session, body_fd, body_pub, count, slot as u32, idx_ns,
        );
    }
    w.body_range = Some((start, full_len));

    if w.single_cand {
        // denserels body = identity check
        let n = full_len as usize;
        w.dens_body.resize(n, 0);
        let ud = uring_session::pack_ud(STAGE_DENS, slot as u32);
        session.push_pread(body_fd, start, &mut w.dens_body[..], ud)?;
        return Ok(true);
    }

    let n = (full_len as usize).min(32);
    w.prefix_len = n;
    w.prefix_buf[..n].fill(0);
    let ud = uring_session::pack_ud(STAGE_PREFIX, slot as u32);
    session.push_pread(body_fd, start, &mut w.prefix_buf[..n], ud)?;
    Ok(true)
}

/// Prefix complete: match → denserels; miss → next idx. Returns true if still in flight.
fn on_prefix_complete(
    table: &TxTable,
    txids: &[[u8; 32]],
    slots: &mut [Option<KeyWork>],
    slot: usize,
    res_len: usize,
    session: &mut UringSession,
    body_fd: RawFd,
    body_pub: u64,
    count: u64,
    idx_ns: &mut u64,
    winner_fk: &mut [Option<Fk>],
    done: &mut [bool],
) -> Result<bool, StoreError> {
    let w = slots[slot].as_mut().unwrap();
    let key_i = w.key_i as usize;
    if res_len != w.prefix_len {
        return submit_next_idx_or_done(
            table, w, session, body_fd, body_pub, count, slot as u32, idx_ns,
        );
    }
    let matched = match TxTable::txid_from_body_prefix(&w.prefix_buf[..w.prefix_len]) {
        Ok(got) if got == txids[key_i] => true,
        Ok(_) => false,
        Err(StoreError::NotFound) | Err(StoreError::Corrupt(_)) => false,
        Err(e) => return Err(e),
    };
    if !matched {
        crate::head_resolve_stats::add_miss_peeks(1);
        return submit_next_idx_or_done(
            table, w, session, body_fd, body_pub, count, slot as u32, idx_ns,
        );
    }
    crate::head_resolve_stats::add_hit_rank(w.pending_rank.max(1) as u64);
    winner_fk[key_i] = Some(Fk(w.pending_fk));
    let (start, full_len) = w.body_range.unwrap_or((0, 0));
    if full_len == 0 {
        done[key_i] = true;
        return Ok(false);
    }
    let n = full_len as usize;
    w.dens_body.resize(n, 0);
    let ud = uring_session::pack_ud(STAGE_DENS, slot as u32);
    session.push_pread(body_fd, start, &mut w.dens_body[..], ud)?;
    Ok(true)
}

fn on_dens_complete(
    table: &TxTable,
    txids: &[[u8; 32]],
    slots: &mut [Option<KeyWork>],
    slot: usize,
    res_len: usize,
    dens_decoded: &mut std::collections::HashMap<
        usize,
        (TxRecord, Vec<OutputRecord>, Vec<u32>),
    >,
    winner_fk: &mut [Option<Fk>],
    done: &mut [bool],
) -> Result<(), StoreError> {
    let w = slots[slot].as_ref().unwrap();
    let key_i = w.key_i as usize;
    let expect = w.dens_body.len();
    if res_len != expect || expect == 0 {
        // Single-cand identity fail or short dens body.
        if w.single_cand {
            crate::head_resolve_stats::add_miss_peeks(1);
            winner_fk[key_i] = None;
        }
        done[key_i] = true;
        return Ok(());
    }
    if w.single_cand {
        match TxTable::txid_from_body_prefix(&w.dens_body) {
            Ok(got) if got == txids[key_i] => {
                crate::head_resolve_stats::add_hit_rank(1);
                winner_fk[key_i] = Some(Fk(w.pending_fk));
            }
            Ok(_) => {
                crate::head_resolve_stats::add_miss_peeks(1);
                winner_fk[key_i] = None;
                done[key_i] = true;
                return Ok(());
            }
            Err(StoreError::NotFound) | Err(StoreError::Corrupt(_)) => {
                crate::head_resolve_stats::add_miss_peeks(1);
                winner_fk[key_i] = None;
                done[key_i] = true;
                return Ok(());
            }
            Err(e) => return Err(e),
        }
    }
    match decode_packed_tx_outs_with_spender_rels_secret(&w.dens_body, Some(&table.secret)) {
        Ok(decoded) => {
            dens_decoded.insert(key_i, decoded);
        }
        Err(StoreError::NotFound) | Err(StoreError::Corrupt(_)) => {}
        Err(e) => return Err(e),
    }
    done[key_i] = true;
    Ok(())
}

/// Submit next cand idx, or false if candidates exhausted (key miss).
fn submit_next_idx_or_done(
    table: &TxTable,
    work: &mut KeyWork,
    session: &mut UringSession,
    _body_fd: RawFd,
    _body_pub: u64,
    count: u64,
    slot: u32,
    idx_ns: &mut u64,
) -> Result<bool, StoreError> {
    while work.cand_i < work.cands.len() {
        let rank = (work.cand_i + 1) as u32;
        let fk = work.cands[work.cand_i];
        work.cand_i += 1;
        if fk == 0 || fk > count {
            continue;
        }
        let t0 = Instant::now();
        let plan = match table.body.idx_page_plan(Fk(fk)) {
            Ok(p) => p,
            Err(StoreError::NotFound) | Err(StoreError::Corrupt(_)) => {
                *idx_ns = idx_ns.saturating_add(t0.elapsed().as_nanos() as u64);
                continue;
            }
            Err(e) => return Err(e),
        };
        *idx_ns = idx_ns.saturating_add(t0.elapsed().as_nanos() as u64);
        work.pending_fk = fk;
        work.pending_rank = rank;
        work.body_range = None;
        if work.idx_page.len() < plan.page_len {
            work.idx_page.resize(plan.page_len, 0);
        } else {
            work.idx_page.truncate(plan.page_len);
            work.idx_page.fill(0);
        }
        let ud = uring_session::pack_ud(STAGE_IDX, slot);
        session.push_pread(plan.fd, plan.page_off, &mut work.idx_page[..], ud)?;
        return Ok(true);
    }
    Ok(false)
}

fn arm(
    table: &TxTable,
    txids: &[[u8; 32]],
    cands_by_key: &[Vec<u64>],
    session: &mut UringSession,
    count: u64,
    body_fd: RawFd,
    body_pub: u64,
    free_slots: &mut Vec<usize>,
    slots: &mut [Option<KeyWork>],
    in_flight: &mut usize,
    next_key: &mut usize,
    done: &mut [bool],
    winner_fk: &mut [Option<Fk>],
    idx_ns: &mut u64,
) -> Result<(), StoreError> {
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
            winner_fk[key_i] = None;
            continue;
        }
        let single = cands.len() == 1;
        slots[slot] = Some(KeyWork {
            key_i: key_i as u32,
            cands,
            cand_i: 0,
            single_cand: single,
            idx_page: vec![0u8; IDX_OS_PAGE as usize],
            prefix_buf: [0u8; 32],
            prefix_len: 0,
            dens_body: Vec::new(),
            body_range: None,
            pending_fk: 0,
            pending_rank: 0,
        });
        let submitted = {
            let w = slots[slot].as_mut().unwrap();
            submit_next_idx_or_done(
                table, w, session, body_fd, body_pub, count, slot as u32, idx_ns,
            )?
        };
        if submitted {
            *in_flight += 1;
        } else {
            slots[slot] = None;
            free_slots.push(slot);
            done[key_i] = true;
            winner_fk[key_i] = None;
        }
    }
    Ok(())
}
