//! Streaming archive head-resolve: FdOnly head probe + FdOnly idx + body
//! prefix verify via uring/pread (`RBITCOIN_HEAD_RESOLVE_IO`).
//!
//! See `docs/io-modality.md`.
//!
//! ## Uring path (completion-driven)
//!
//! Per key, the ring owns the **idx → body** steps (head probe is CPU-side):
//! 1. **probe** (sync) — candidate create_fks deepest-first
//! 2. **STAGE_IDX** — one OS-page pread covering the candidate's idx slot
//! 3. **STAGE_BODY** — ≤32 B body prefix pread; match → done, else next cand at STAGE_IDX
//!
//! Multiple keys are in flight at mixed stages (not “all idx then all body”).
//!
//! Pread path: sequential probe → page-aligned idx → body pread (no ring).

use crate::error::StoreError;
use crate::io_backend::{self, ReadIoBackend};
use crate::tx_idx::IDX_OS_PAGE;
use crate::tx_table::TxTable;
use crate::uring_session::{self, UringSession};
use rbitcoin_primitives::Fk;
use std::os::fd::RawFd;
use std::time::Instant;

/// Max keys with an outstanding IO (idx or body) — ≤ ring depth.
const MAX_IN_FLIGHT_KEYS: usize = 128;

const STAGE_IDX: u64 = 1;
const STAGE_BODY: u64 = 2;

struct KeyWork {
    /// Index into the original `txids` slice.
    key_i: u32,
    /// Candidate fks deepest-first (probe depth high → low).
    cands: Vec<u64>,
    /// Next cand index to try.
    cand_i: usize,
    /// OS-page buffer for idx pread (stable while STAGE_IDX in flight).
    idx_page: Vec<u8>,
    /// Body pread buffer (stable while STAGE_BODY in flight).
    body_buf: [u8; 32],
    body_len: usize,
    /// Create_fk being verified.
    pending_fk: u64,
    /// 1-based probe order of `pending_fk`.
    pending_rank: u32,
}

/// Resolve many txids via FdOnly head probe + FdOnly idx + body prefix (backend from env).
pub fn resolve_batch_streaming(
    table: &TxTable,
    txids: &[[u8; 32]],
) -> Result<Vec<([u8; 32], Option<Fk>)>, StoreError> {
    if txids.is_empty() {
        return Ok(Vec::new());
    }
    match io_backend::head_resolve_io_backend() {
        ReadIoBackend::Uring => resolve_batch_streaming_uring(table, txids),
        ReadIoBackend::Pread => resolve_batch_sync_pread(table, txids),
    }
}

/// Sequential deepest-cand: page-aligned idx + body pread.
fn resolve_batch_sync_pread(
    table: &TxTable,
    txids: &[[u8; 32]],
) -> Result<Vec<([u8; 32], Option<Fk>)>, StoreError> {
    crate::head_resolve_stats::add_keys(txids.len() as u64);
    let body_pub = table.body.body_published_len();
    let count = table.body.count();
    let body_fd = table.body.body_read_fd();
    let body_path = table.body.body_file_path().to_path_buf();

    let mut results: Vec<Option<Fk>> = vec![None; txids.len()];
    let mut cands_total = 0u64;
    let mut body_lookups = 0u64;
    let mut probe_ns = 0u64;
    let mut idx_ns = 0u64;
    let mut body_ns = 0u64;

    for (key_i, txid) in txids.iter().enumerate() {
        let t_probe = Instant::now();
        let mixed = table.secret.mix_txid(txid);
        let raw = table.head.probe_candidates(&mixed)?;
        probe_ns = probe_ns.saturating_add(t_probe.elapsed().as_nanos() as u64);
        cands_total = cands_total.saturating_add(raw.len() as u64);
        let mut matched: Option<Fk> = None;
        for (ci, fk) in raw.into_iter().enumerate() {
            let id = fk.0;
            if id == 0 || id > count {
                continue;
            }
            let t_idx = Instant::now();
            let range = match table.body.record_range(fk) {
                Ok(r) => r,
                Err(StoreError::NotFound) | Err(StoreError::Corrupt(_)) => continue,
                Err(e) => return Err(e),
            };
            idx_ns = idx_ns.saturating_add(t_idx.elapsed().as_nanos() as u64);
            let (start, full_len) = range;
            let n = (full_len as usize).min(32);
            if n == 0 || start.saturating_add(n as u64) > body_pub {
                continue;
            }
            let mut buf = [0u8; 32];
            let t_body = Instant::now();
            let rc = unsafe {
                libc::pread(
                    body_fd,
                    buf.as_mut_ptr() as *mut libc::c_void,
                    n,
                    start as libc::off_t,
                )
            };
            body_ns = body_ns.saturating_add(t_body.elapsed().as_nanos() as u64);
            body_lookups = body_lookups.saturating_add(1);
            if rc < 0 {
                return Err(StoreError::io(
                    &body_path,
                    std::io::Error::last_os_error(),
                ));
            }
            if rc as usize != n {
                crate::head_resolve_stats::add_miss_peeks(1);
                continue;
            }
            match TxTable::txid_from_body_prefix(&buf[..n]) {
                Ok(got) if got == *txid => {
                    crate::head_resolve_stats::add_hit_rank((ci + 1) as u64);
                    matched = Some(fk);
                    break;
                }
                Ok(_) => crate::head_resolve_stats::add_miss_peeks(1),
                Err(StoreError::NotFound) | Err(StoreError::Corrupt(_)) => {
                    crate::head_resolve_stats::add_miss_peeks(1);
                }
                Err(e) => return Err(e),
            }
        }
        results[key_i] = matched;
    }

    crate::head_resolve_stats::add_probe(probe_ns);
    crate::head_resolve_stats::add_idx(idx_ns);
    crate::head_resolve_stats::add_body(body_ns);
    crate::head_resolve_stats::add_cands(cands_total);
    crate::head_resolve_stats::add_body_lookups(body_lookups);

    Ok(txids
        .iter()
        .enumerate()
        .map(|(i, t)| (*t, results[i]))
        .collect())
}

/// io_uring: per-key stages idx → body (many keys mixed in flight).
fn resolve_batch_streaming_uring(
    table: &TxTable,
    txids: &[[u8; 32]],
) -> Result<Vec<([u8; 32], Option<Fk>)>, StoreError> {
    crate::head_resolve_stats::add_keys(txids.len() as u64);

    let mut session = UringSession::new(uring_session::DEFAULT_ENTRIES)?;
    let body_fd: RawFd = table.body.body_read_fd();
    let body_path = table.body.body_file_path().to_path_buf();
    let body_pub = table.body.body_published_len();
    let count = table.body.count();

    let mut results: Vec<Option<Fk>> = vec![None; txids.len()];
    let mut done = vec![false; txids.len()];
    let mut next_key = 0usize;
    let mut cands_total = 0u64;
    let mut body_lookups = 0u64;
    let mut probe_ns = 0u64;
    let mut idx_ns = 0u64;
    let mut body_ns = 0u64;

    let mut free_slots: Vec<usize> = (0..MAX_IN_FLIGHT_KEYS).collect();
    let mut slots: Vec<Option<KeyWork>> = (0..MAX_IN_FLIGHT_KEYS).map(|_| None).collect();
    let mut in_flight_keys = 0usize;

    arm_keys(
        table,
        txids,
        &mut session,
        count,
        &mut free_slots,
        &mut slots,
        &mut in_flight_keys,
        &mut next_key,
        &mut done,
        &mut results,
        &mut cands_total,
        &mut probe_ns,
        &mut idx_ns,
    )?;
    session.sync_submission();
    let _ = session.submit();

    while in_flight_keys > 0 {
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
            if slot >= slots.len() {
                return Err(StoreError::Corrupt("stream resolve bad user_data"));
            }
            if slots[slot].is_none() {
                return Err(StoreError::Corrupt("stream resolve empty slot"));
            }

            if kind == STAGE_IDX {
                idx_ns = idx_ns.saturating_add(wait_ns);
                if res < 0 {
                    slots[slot] = None;
                    free_slots.push(slot);
                    return Err(StoreError::io(
                        Path::new("tx.idx"),
                        std::io::Error::from_raw_os_error(-res),
                    ));
                }
                let submitted = {
                    let w = slots[slot].as_mut().unwrap();
                    if res as usize != w.idx_page.len() {
                        // short read — try next cand
                        advance_after_idx_fail(w);
                        try_submit_idx_or_finish(
                            table,
                            w,
                            &mut session,
                            body_fd,
                            body_pub,
                            count,
                            slot as u32,
                            &mut idx_ns,
                        )?
                    } else {
                        match finish_idx_and_submit_body(
                            table,
                            w,
                            &mut session,
                            body_fd,
                            body_pub,
                            count,
                            slot as u32,
                            &mut idx_ns,
                        )? {
                            SubmitOutcome::BodyInFlight => true,
                            SubmitOutcome::IdxInFlight => true,
                            SubmitOutcome::KeyDone { matched } => {
                                let key_i = w.key_i as usize;
                                results[key_i] = matched;
                                done[key_i] = true;
                                false
                            }
                        }
                    }
                };
                if submitted {
                    // still in flight (idx or body)
                } else {
                    slots[slot] = None;
                    free_slots.push(slot);
                    in_flight_keys = in_flight_keys.saturating_sub(1);
                }
            } else if kind == STAGE_BODY {
                body_ns = body_ns.saturating_add(wait_ns);
                body_lookups = body_lookups.saturating_add(1);
                in_flight_keys = in_flight_keys.saturating_sub(1);
                if res < 0 {
                    slots[slot] = None;
                    free_slots.push(slot);
                    return Err(StoreError::io(
                        &body_path,
                        std::io::Error::from_raw_os_error(-res),
                    ));
                }
                let (key_i, buf_len, pending_fk, prefix_ok) = {
                    let w = slots[slot].as_ref().unwrap();
                    if res as usize != w.body_len {
                        (w.key_i as usize, w.body_len, w.pending_fk, false)
                    } else {
                        (w.key_i as usize, w.body_len, w.pending_fk, true)
                    }
                };
                if !prefix_ok {
                    slots[slot] = None;
                    free_slots.push(slot);
                    return Err(StoreError::Corrupt("stream body pread short"));
                }
                let want = &txids[key_i];
                let matched = {
                    let w = slots[slot].as_ref().unwrap();
                    match TxTable::txid_from_body_prefix(&w.body_buf[..buf_len]) {
                        Ok(got) if got == *want => true,
                        Ok(_) => false,
                        Err(StoreError::NotFound) | Err(StoreError::Corrupt(_)) => false,
                        Err(e) => {
                            slots[slot] = None;
                            free_slots.push(slot);
                            return Err(e);
                        }
                    }
                };

                if matched {
                    let rank = slots[slot]
                        .as_ref()
                        .map(|w| w.pending_rank as u64)
                        .unwrap_or(1);
                    crate::head_resolve_stats::add_hit_rank(rank.max(1));
                    results[key_i] = Some(Fk(pending_fk));
                    done[key_i] = true;
                    slots[slot] = None;
                    free_slots.push(slot);
                } else {
                    crate::head_resolve_stats::add_miss_peeks(1);
                    let rearmed = {
                        let w = slots[slot].as_mut().unwrap();
                        try_submit_idx_or_finish(
                            table,
                            w,
                            &mut session,
                            body_fd,
                            body_pub,
                            count,
                            slot as u32,
                            &mut idx_ns,
                        )?
                    };
                    if rearmed {
                        in_flight_keys += 1;
                    } else {
                        let w = slots[slot].take().unwrap();
                        results[w.key_i as usize] = None;
                        done[w.key_i as usize] = true;
                        free_slots.push(slot);
                    }
                }
            } else {
                return Err(StoreError::Corrupt("stream resolve bad stage"));
            }
        }

        arm_keys(
            table,
            txids,
            &mut session,
            count,
            &mut free_slots,
            &mut slots,
            &mut in_flight_keys,
            &mut next_key,
            &mut done,
            &mut results,
            &mut cands_total,
            &mut probe_ns,
            &mut idx_ns,
        )?;
        session.sync_submission();
        let _ = session.submit();
    }

    for (i, d) in done.iter().enumerate() {
        if !*d {
            results[i] = table.get_fk_by_txid(&txids[i])?;
        }
    }

    crate::head_resolve_stats::add_probe(probe_ns);
    crate::head_resolve_stats::add_idx(idx_ns);
    crate::head_resolve_stats::add_body(body_ns);
    crate::head_resolve_stats::add_cands(cands_total);
    crate::head_resolve_stats::add_body_lookups(body_lookups);

    Ok(txids
        .iter()
        .enumerate()
        .map(|(i, t)| (*t, results[i]))
        .collect())
}

use std::path::Path;

fn arm_keys(
    table: &TxTable,
    txids: &[[u8; 32]],
    session: &mut UringSession,
    count: u64,
    free_slots: &mut Vec<usize>,
    slots: &mut [Option<KeyWork>],
    in_flight_keys: &mut usize,
    next_key: &mut usize,
    done: &mut [bool],
    results: &mut [Option<Fk>],
    cands_total: &mut u64,
    probe_ns: &mut u64,
    idx_ns: &mut u64,
) -> Result<(), StoreError> {
    while *next_key < txids.len()
        && *in_flight_keys < MAX_IN_FLIGHT_KEYS
        && session.free_sq() > 0
        && !free_slots.is_empty()
    {
        let key_i = *next_key;
        *next_key += 1;
        let slot = free_slots.pop().unwrap();

        let t_probe = Instant::now();
        let mixed = table.secret.mix_txid(&txids[key_i]);
        let raw = table.head.probe_candidates(&mixed)?;
        *probe_ns = probe_ns.saturating_add(t_probe.elapsed().as_nanos() as u64);
        *cands_total = cands_total.saturating_add(raw.len() as u64);
        let cands: Vec<u64> = raw.into_iter().map(|f| f.0).collect();
        if cands.is_empty() {
            free_slots.push(slot);
            done[key_i] = true;
            results[key_i] = None;
            continue;
        }

        slots[slot] = Some(KeyWork {
            key_i: key_i as u32,
            cands,
            cand_i: 0,
            idx_page: vec![0u8; IDX_OS_PAGE as usize],
            body_buf: [0u8; 32],
            body_len: 0,
            pending_fk: 0,
            pending_rank: 0,
        });
        let submitted = {
            let w = slots[slot].as_mut().unwrap();
            try_submit_idx_or_finish(
                table,
                w,
                session,
                table.body.body_read_fd(),
                table.body.body_published_len(),
                count,
                slot as u32,
                idx_ns,
            )?
        };
        if submitted {
            *in_flight_keys += 1;
        } else {
            slots[slot] = None;
            free_slots.push(slot);
            done[key_i] = true;
            results[key_i] = None;
        }
    }
    Ok(())
}

enum SubmitOutcome {
    BodyInFlight,
    IdxInFlight,
    KeyDone { matched: Option<Fk> },
}

fn advance_after_idx_fail(work: &mut KeyWork) {
    // cand already advanced in try_submit_idx; nothing extra
    let _ = work;
}

/// After STAGE_IDX CQE: extract start, resolve full range (sync page-hot), submit body or next idx.
fn finish_idx_and_submit_body(
    table: &TxTable,
    work: &mut KeyWork,
    session: &mut UringSession,
    body_fd: RawFd,
    body_pub: u64,
    count: u64,
    slot: u32,
    idx_ns: &mut u64,
) -> Result<SubmitOutcome, StoreError> {
    // Prefer full record_range (page-aligned, may hit cache from the pread we just did).
    let t0 = Instant::now();
    let range = match table.body.record_range(Fk(work.pending_fk)) {
        Ok(r) => r,
        Err(StoreError::NotFound) | Err(StoreError::Corrupt(_)) => {
            *idx_ns = idx_ns.saturating_add(t0.elapsed().as_nanos() as u64);
            return match try_submit_idx_or_finish(
                table, work, session, body_fd, body_pub, count, slot, idx_ns,
            )? {
                true => Ok(SubmitOutcome::IdxInFlight),
                false => Ok(SubmitOutcome::KeyDone { matched: None }),
            };
        }
        Err(e) => return Err(e),
    };
    *idx_ns = idx_ns.saturating_add(t0.elapsed().as_nanos() as u64);
    let (start, full_len) = range;
    let n = (full_len as usize).min(32);
    if n == 0 || start.saturating_add(n as u64) > body_pub {
        return match try_submit_idx_or_finish(
            table, work, session, body_fd, body_pub, count, slot, idx_ns,
        )? {
            true => Ok(SubmitOutcome::IdxInFlight),
            false => Ok(SubmitOutcome::KeyDone { matched: None }),
        };
    }
    work.body_len = n;
    work.body_buf[..n].fill(0);
    let ud = uring_session::pack_ud(STAGE_BODY, slot);
    session.push_pread(body_fd, start, &mut work.body_buf[..n], ud)?;
    Ok(SubmitOutcome::BodyInFlight)
}

/// Submit STAGE_IDX for the next viable cand, or return false if candidates exhausted.
fn try_submit_idx_or_finish(
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
