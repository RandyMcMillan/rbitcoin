//! Streaming archive head-resolve: mmap probe + **io_uring idx + body**.
//!
//! Completion-driven loop (one outstanding op per in-flight key):
//! 1. mmap `probe_fks` → candidates deepest-first (BIP30)
//! 2. io_uring pread 8/16 B `tx.idx` for next cand
//! 3. on idx CQE → io_uring pread ≤33 body bytes; match → done; else next cand
//!
//! Falls back to the caller when io_uring is unavailable.

use crate::error::StoreError;
use crate::file::FILE_HEADER_LEN;
use crate::tx_table::TxTable;
use crate::uring_session::{self, UringSession};
use rbitcoin_primitives::Fk;
use std::os::fd::RawFd;
use std::time::Instant;

/// Max keys with an idx or body pread in flight (≤ ring depth).
const MAX_IN_FLIGHT_KEYS: usize = 512;

const STAGE_IDX: u64 = 0;
const STAGE_BODY: u64 = 1;

struct KeyWork {
    /// Index into the original `txids` slice.
    key_i: u32,
    /// Candidate fks deepest-first (probe depth high → low).
    cands: Vec<u64>,
    /// Next cand index to try.
    cand_i: usize,
    /// Body pread buffer (stable while in flight — lives in `slots[slot]`).
    buf: [u8; 33],
    buf_len: usize,
    /// Idx pread scratch (8 or 16 bytes).
    idx_buf: [u8; 16],
    idx_nbytes: u8,
    /// Create_fk being verified by the outstanding body pread.
    pending_fk: u64,
    /// true = body stage outstanding; false = idx stage.
    body_stage: bool,
}

/// Resolve many txids via mmap head probe + streaming idx/body preads.
///
/// Returns one `(txid, Option<Fk>)` per input in the same order as `txids`.
/// Records [`crate::head_resolve_stats`] probe/idx/body walls and counts.
pub fn resolve_batch_streaming(
    table: &TxTable,
    txids: &[[u8; 32]],
) -> Result<Vec<([u8; 32], Option<Fk>)>, StoreError> {
    if txids.is_empty() {
        return Ok(Vec::new());
    }
    crate::head_resolve_stats::add_keys(txids.len() as u64);

    let mut session = UringSession::new(uring_session::DEFAULT_ENTRIES)?;
    let body_fd: RawFd = table.body.body_read_fd();
    let idx_fd: RawFd = table.body.idx_read_fd();
    let body_path = table.body.body_file_path().to_path_buf();
    let idx_path = table.body.idx_file_path().to_path_buf();
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
        idx_fd,
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
        // Attribute wait to the stage of the first CQE (approx).
        let wait_ns = t_wait.elapsed().as_nanos() as u64;

        for (ud, res) in cqes {
            let (kind, slot_u) = uring_session::unpack_ud(ud);
            let slot = slot_u as usize;
            if slot >= slots.len() {
                return Err(StoreError::Corrupt("stream resolve bad user_data"));
            }
            let work = slots[slot]
                .take()
                .ok_or(StoreError::Corrupt("stream resolve empty slot"))?;
            in_flight_keys = in_flight_keys.saturating_sub(1);

            // Always re-home `work` in `slots[slot]` before push_pread so the
            // buffer pointer stays stable for the in-flight SQE (no use-after-move).
            slots[slot] = Some(work);

            if kind == STAGE_IDX {
                idx_ns = idx_ns.saturating_add(wait_ns);
                let nb = {
                    let w = slots[slot].as_ref().unwrap();
                    w.idx_nbytes as usize
                };
                if res < 0 {
                    slots[slot] = None;
                    free_slots.push(slot);
                    return Err(StoreError::io(
                        &idx_path,
                        std::io::Error::from_raw_os_error(-res),
                    ));
                }
                if res as usize != nb {
                    let submitted = {
                        let w = slots[slot].as_mut().unwrap();
                        try_submit_idx(table, w, &mut session, idx_fd, count, slot as u32, &mut idx_ns)?
                    };
                    if submitted {
                        in_flight_keys += 1;
                    } else {
                        let w = slots[slot].take().unwrap();
                        results[w.key_i as usize] = None;
                        done[w.key_i as usize] = true;
                        free_slots.push(slot);
                    }
                    continue;
                }
                let (start, end, pending_fk) = {
                    let w = slots[slot].as_ref().unwrap();
                    let start = u64::from_le_bytes(w.idx_buf[..8].try_into().unwrap());
                    let end = if w.pending_fk < count {
                        u64::from_le_bytes(w.idx_buf[8..16].try_into().unwrap())
                    } else {
                        body_pub
                    };
                    (start, end, w.pending_fk)
                };
                let _ = pending_fk;
                if end < start {
                    let submitted = {
                        let w = slots[slot].as_mut().unwrap();
                        try_submit_idx(table, w, &mut session, idx_fd, count, slot as u32, &mut idx_ns)?
                    };
                    if submitted {
                        in_flight_keys += 1;
                    } else {
                        let w = slots[slot].take().unwrap();
                        results[w.key_i as usize] = None;
                        done[w.key_i as usize] = true;
                        free_slots.push(slot);
                    }
                    continue;
                }
                let full_len = end - start;
                let n = (full_len as usize).min(33);
                if n == 0 || start.saturating_add(n as u64) > body_pub {
                    let submitted = {
                        let w = slots[slot].as_mut().unwrap();
                        try_submit_idx(table, w, &mut session, idx_fd, count, slot as u32, &mut idx_ns)?
                    };
                    if submitted {
                        in_flight_keys += 1;
                    } else {
                        let w = slots[slot].take().unwrap();
                        results[w.key_i as usize] = None;
                        done[w.key_i as usize] = true;
                        free_slots.push(slot);
                    }
                    continue;
                }
                {
                    let w = slots[slot].as_mut().unwrap();
                    w.buf[..n].fill(0);
                    w.buf_len = n;
                    w.body_stage = true;
                    let ud = uring_session::pack_ud(STAGE_BODY, slot as u32);
                    session.push_pread(body_fd, start, &mut w.buf[..n], ud)?;
                }
                in_flight_keys += 1;
            } else {
                // STAGE_BODY
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
                let (key_i, buf_len, pending_fk, prefix_ok) = {
                    let w = slots[slot].as_ref().unwrap();
                    if res as usize != w.buf_len {
                        (w.key_i as usize, w.buf_len, w.pending_fk, false)
                    } else {
                        (w.key_i as usize, w.buf_len, w.pending_fk, true)
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
                    match TxTable::txid_from_body_prefix(&w.buf[..buf_len]) {
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
                    results[key_i] = Some(Fk(pending_fk));
                    done[key_i] = true;
                    slots[slot] = None;
                    free_slots.push(slot);
                } else {
                    let submitted = {
                        let w = slots[slot].as_mut().unwrap();
                        try_submit_idx(
                            table, w, &mut session, idx_fd, count, slot as u32, &mut idx_ns,
                        )?
                    };
                    if submitted {
                        in_flight_keys += 1;
                    } else {
                        let w = slots[slot].take().unwrap();
                        results[w.key_i as usize] = None;
                        done[w.key_i as usize] = true;
                        free_slots.push(slot);
                    }
                }
            }
        }

        arm_keys(
            table,
            txids,
            &mut session,
            idx_fd,
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

fn arm_keys(
    table: &TxTable,
    txids: &[[u8; 32]],
    session: &mut UringSession,
    idx_fd: RawFd,
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
        let raw = table.head.read().unwrap().probe_fks(&txids[key_i])?;
        *probe_ns = probe_ns.saturating_add(t_probe.elapsed().as_nanos() as u64);
        *cands_total = cands_total.saturating_add(raw.len() as u64);
        let cands: Vec<u64> = raw.into_iter().rev().map(|f| f.0).collect();
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
            buf: [0u8; 33],
            buf_len: 0,
            idx_buf: [0u8; 16],
            idx_nbytes: 0,
            pending_fk: 0,
            body_stage: false,
        });
        let submitted = {
            let w = slots[slot].as_mut().unwrap();
            try_submit_idx(table, w, session, idx_fd, count, slot as u32, idx_ns)?
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

/// Arm next cand's idx pread. Returns false when cands are exhausted.
fn try_submit_idx(
    _table: &TxTable,
    work: &mut KeyWork,
    session: &mut UringSession,
    idx_fd: RawFd,
    count: u64,
    slot: u32,
    idx_ns: &mut u64,
) -> Result<bool, StoreError> {
    while work.cand_i < work.cands.len() {
        let fk = work.cands[work.cand_i];
        work.cand_i += 1;
        if fk == 0 || fk > count {
            continue;
        }
        let t_idx = Instant::now();
        let nbytes: u8 = if fk < count { 16 } else { 8 };
        let off = FILE_HEADER_LEN as u64 + (fk - 1) * 8;
        work.idx_nbytes = nbytes;
        work.pending_fk = fk;
        work.body_stage = false;
        work.idx_buf = [0u8; 16];
        let ud = uring_session::pack_ud(STAGE_IDX, slot);
        session.push_pread(idx_fd, off, &mut work.idx_buf[..nbytes as usize], ud)?;
        *idx_ns = idx_ns.saturating_add(t_idx.elapsed().as_nanos() as u64);
        return Ok(true);
    }
    Ok(false)
}
