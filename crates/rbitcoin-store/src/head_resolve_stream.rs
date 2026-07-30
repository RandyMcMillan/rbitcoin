//! Streaming archive head-resolve: mmap probe + mmap idx + io_uring body.
//!
//! Completion-driven loop (one outstanding body pread per in-flight key):
//! 1. mmap `probe_fks` → candidates deepest-first (BIP30)
//! 2. mmap segmented `tx.idx` `record_range` for next cand
//! 3. io_uring pread ≤32 body bytes (txid); match → done; else next cand
//!
//! Falls back to the caller when io_uring is unavailable.

use crate::error::StoreError;
use crate::tx_table::TxTable;
use crate::uring_session::{self, UringSession};
use rbitcoin_primitives::Fk;
use std::os::fd::RawFd;
use std::time::Instant;

/// Max keys with a body pread in flight (≤ ring depth).
const MAX_IN_FLIGHT_KEYS: usize = 512;

const STAGE_BODY: u64 = 1;

struct KeyWork {
    /// Index into the original `txids` slice.
    key_i: u32,
    /// Candidate fks deepest-first (probe depth high → low).
    cands: Vec<u64>,
    /// Next cand index to try.
    cand_i: usize,
    /// Body pread buffer (stable while in flight — lives in `slots[slot]`).
    buf: [u8; 32],
    buf_len: usize,
    /// Create_fk being verified by the outstanding body pread.
    pending_fk: u64,
}

/// Resolve many txids via mmap head/idx + streaming body preads.
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
        body_fd,
        body_pub,
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
            if slot >= slots.len() || kind != STAGE_BODY {
                return Err(StoreError::Corrupt("stream resolve bad user_data"));
            }
            let work = slots[slot]
                .take()
                .ok_or(StoreError::Corrupt("stream resolve empty slot"))?;
            in_flight_keys = in_flight_keys.saturating_sub(1);
            slots[slot] = Some(work);

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
                    try_submit_body(
                        table, w, &mut session, body_fd, body_pub, count, slot as u32, &mut idx_ns,
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

        arm_keys(
            table,
            txids,
            &mut session,
            body_fd,
            body_pub,
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
    body_fd: RawFd,
    body_pub: u64,
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
        // Keyed probe: mix with datadir secret (never raw txid prefixes).
        let mixed = table.secret.mix_txid(&txids[key_i]);
        let raw = table.head.probe_candidates(&mixed)?;
        *probe_ns = probe_ns.saturating_add(t_probe.elapsed().as_nanos() as u64);
        *cands_total = cands_total.saturating_add(raw.len() as u64);
        // probe_candidates already orders open-first / sealed newest / deep-first.
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
            buf: [0u8; 32],
            buf_len: 0,
            pending_fk: 0,
        });
        let submitted = {
            let w = slots[slot].as_mut().unwrap();
            try_submit_body(
                table, w, session, body_fd, body_pub, count, slot as u32, idx_ns,
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

/// mmap idx range for next cand, then arm body pread. Returns false when cands exhausted.
fn try_submit_body(
    table: &TxTable,
    work: &mut KeyWork,
    session: &mut UringSession,
    body_fd: RawFd,
    body_pub: u64,
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
        let range = match table.body.record_range(Fk(fk)) {
            Ok(r) => r,
            Err(StoreError::NotFound) | Err(StoreError::Corrupt(_)) => continue,
            Err(e) => return Err(e),
        };
        *idx_ns = idx_ns.saturating_add(t_idx.elapsed().as_nanos() as u64);
        let (start, full_len) = range;
        let n = (full_len as usize).min(32);
        if n == 0 || start.saturating_add(n as u64) > body_pub {
            continue;
        }
        work.pending_fk = fk;
        work.buf[..n].fill(0);
        work.buf_len = n;
        let ud = uring_session::pack_ud(STAGE_BODY, slot);
        session.push_pread(body_fd, start, &mut work.buf[..n], ud)?;
        return Ok(true);
    }
    Ok(false)
}
