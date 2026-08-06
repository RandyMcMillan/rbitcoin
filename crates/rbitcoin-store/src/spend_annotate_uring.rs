//! Completion-driven io_uring RMW for Class A spender-meta annotation.
//!
//! Hot path: known absolute offset of the 9-byte `(spender_field:u64, flags:u8)`
//! in `tx.body`. Machine:
//!
//! 1. Submit pread of 9 B  
//! 2. On read: decide sole / multi / promote / idempotent  
//!    - multi or promote: **inline** mmap [`SpenderTable::append`] (needs read
//!      result; same-outpoint edges are serialized so list order is stable)  
//! 3. Submit pwrite of updated 9 B  
//! 4. On write: free the slot and arm more work  
//!
//! At most one in-flight RMW per absolute offset (reorg double-annotate on the
//! same outpoint is serialized). Falls back to the caller on uring setup failure.

use crate::compact::output_flags;
use crate::error::StoreError;
use crate::spender_table::SpenderTable;
use crate::tx_table::TxTable;
use crate::uring_session::{self, UringSession};
use rbitcoin_primitives::Fk;
use std::collections::{HashMap, HashSet, VecDeque};
use std::os::fd::RawFd;

const META_LEN: usize = 9;
const MAX_SLOTS: usize = 128;

enum Phase {
    Reading,
    Writing,
}

struct Slot {
    edge_i: usize,
    phase: Phase,
    /// Read buffer / write payload (9 bytes).
    buf: [u8; META_LEN],
}

/// Annotate spends at absolute meta offsets via io_uring RMW.
///
/// `edges`: `(abs_off, create_tx_fk, vout, spending_tx_fk)`.
/// Returns edges that could not be annotated here (OOB abs, IO error deferred
/// as cold — empty when all succeed). On hard errors returns `Err`.
pub fn put_spend_batch_by_abs_meta_uring(
    txs: &TxTable,
    spenders: &SpenderTable,
    edges: &[(u64, Fk, u32, Fk)],
) -> Result<Vec<(Fk, u32, Fk)>, StoreError> {
    if edges.is_empty() {
        return Ok(Vec::new());
    }
    for &(_, _, _, sfk) in edges {
        if sfk.is_null() {
            return Err(StoreError::InvalidFk);
        }
    }

    let body_fd: RawFd = txs.body.body_read_fd();
    let body_path = txs.body.body_file_path().to_path_buf();
    let body_pub = txs.body.body_published_len();

    // Work list; OOB goes straight to cold.
    let mut cold: Vec<(Fk, u32, Fk)> = Vec::new();
    let mut work: Vec<(u64, Fk, u32, Fk)> = Vec::with_capacity(edges.len());
    for &(abs, cfk, vout, sfk) in edges {
        if abs.saturating_add(META_LEN as u64) > body_pub {
            cold.push((cfk, vout, sfk));
        } else {
            work.push((abs, cfk, vout, sfk));
        }
    }
    if work.is_empty() {
        return Ok(cold);
    }

    uring_session::with_thread_local(uring_session::DEFAULT_ENTRIES, |session| {
    // Pending edge indices not yet started.
    let mut pending: VecDeque<usize> = (0..work.len()).collect();
    // Abs offsets with an RMW in flight (serialize same-outpoint).
    let mut abs_busy: HashSet<u64> = HashSet::new();
    // Optional FIFO of waiters when abs is busy: abs → edge indices.
    let mut abs_wait: HashMap<u64, VecDeque<usize>> = HashMap::new();

    let mut free_slots: Vec<usize> = (0..MAX_SLOTS).collect();
    let mut slots: Vec<Option<Slot>> = (0..MAX_SLOTS).map(|_| None).collect();
    let mut in_flight = 0usize;

    let arm = |session: &mut UringSession,
               free_slots: &mut Vec<usize>,
               slots: &mut [Option<Slot>],
               pending: &mut VecDeque<usize>,
               abs_busy: &mut HashSet<u64>,
               abs_wait: &mut HashMap<u64, VecDeque<usize>>,
               work: &[(u64, Fk, u32, Fk)],
               in_flight: &mut usize,
               body_fd: RawFd|
     -> Result<(), StoreError> {
        while *in_flight < MAX_SLOTS && session.free_sq() > 0 && !free_slots.is_empty() {
            // Prefer a waiter whose abs is free, else next pending with free abs.
            let edge_i = if let Some(ei) = next_ready(pending, abs_busy, abs_wait, work) {
                ei
            } else {
                break;
            };
            let abs = work[edge_i].0;
            abs_busy.insert(abs);
            let slot = free_slots.pop().unwrap();
            slots[slot] = Some(Slot {
                edge_i,
                phase: Phase::Reading,
                buf: [0u8; META_LEN],
            });
            {
                let s = slots[slot].as_mut().unwrap();
                // RMW fallback pread: still drop after (not confirm pure-write path).
                let flags = crate::dontcache_policy::body_sqe_rw_flags();
                session.push_pread_flags(body_fd, abs, &mut s.buf, slot as u64, flags)?;
            }
            *in_flight += 1;
        }
        Ok(())
    };

    arm(
        session,
        &mut free_slots,
        &mut slots,
        &mut pending,
        &mut abs_busy,
        &mut abs_wait,
        &work,
        &mut in_flight,
        body_fd,
    )?;
    session.sync_submission();
    let _ = session.submit();

    while in_flight > 0 {
        let mut cqes = session.harvest_ready();
        if cqes.is_empty() {
            session.submit_and_wait_one()?;
            cqes = session.harvest_ready();
        }

        for (ud, res) in cqes {
            let slot = ud as usize;
            if slot >= slots.len() {
                return Err(StoreError::Corrupt("spend annotate bad user_data"));
            }
            let mut st = slots[slot]
                .take()
                .ok_or(StoreError::Corrupt("spend annotate empty slot"))?;
            in_flight = in_flight.saturating_sub(1);
            let edge_i = st.edge_i;
            let (abs, create_fk, vout, spend_fk) = work[edge_i];

            match st.phase {
                Phase::Reading => {
                    if res < 0 {
                        // ENOTSUP on RWF_DONTCACHE: demote and retry read once.
                        if res == -95 && crate::bulk_io::rwf_dontcache_ok() {
                            crate::bulk_io::note_rwf_dontcache_unsupported();
                            slots[slot] = Some(st);
                            {
                                let s = slots[slot].as_mut().unwrap();
                                session.push_pread_flags(
                                    body_fd,
                                    abs,
                                    &mut s.buf,
                                    slot as u64,
                                    0,
                                )?;
                            }
                            in_flight += 1;
                            continue;
                        }
                        free_slots.push(slot);
                        abs_busy.remove(&abs);
                        return Err(StoreError::io(
                            &body_path,
                            std::io::Error::from_raw_os_error(-res),
                        ));
                    }
                    if res as usize != META_LEN {
                        free_slots.push(slot);
                        abs_busy.remove(&abs);
                        cold.push((create_fk, vout, spend_fk));
                        continue;
                    }

                    let field = Fk(u64::from_le_bytes(st.buf[0..8].try_into().unwrap()));
                    let flags0 = st.buf[8];
                    let multi = flags0 & output_flags::MULTI_SPENDER != 0;

                    let (new_multi, new_field, skip_write) =
                        if !multi && field.is_null() {
                            (false, spend_fk, false)
                        } else if !multi && field == spend_fk {
                            (false, field, true) // idempotent
                        } else if !multi {
                            // Promote sole → multi (reorg / second annotate).
                            let e1 = spenders.append(field, Fk::NULL)?;
                            let e2 = spenders.append(spend_fk, e1)?;
                            (true, e2, false)
                        } else {
                            // Already multi: prepend list node.
                            let e = spenders.append(spend_fk, field)?;
                            (true, e, false)
                        };

                    if skip_write {
                        free_slots.push(slot);
                        abs_busy.remove(&abs);
                        // Release waiter for this abs.
                        if let Some(q) = abs_wait.get_mut(&abs) {
                            if let Some(next_ei) = q.pop_front() {
                                pending.push_front(next_ei);
                            }
                            if q.is_empty() {
                                abs_wait.remove(&abs);
                            }
                        }
                        continue;
                    }

                    st.buf[0..8].copy_from_slice(&new_field.0.to_le_bytes());
                    if new_multi {
                        st.buf[8] = flags0 | output_flags::MULTI_SPENDER;
                    } else {
                        st.buf[8] = flags0 & !output_flags::MULTI_SPENDER;
                    }
                    st.phase = Phase::Writing;
                    // Keep slot occupied for write buffer stability.
                    slots[slot] = Some(st);
                    {
                        let s = slots[slot].as_mut().unwrap();
                        // Policy: all tx.body writes use RWF_DONTCACHE.
                        let flags = crate::dontcache_policy::body_sqe_rw_flags();
                        session.push_pwrite_flags(body_fd, abs, &s.buf, slot as u64, flags)?;
                    }
                    in_flight += 1;
                }
                Phase::Writing => {
                    if res < 0 {
                        // ENOTSUP on RWF_DONTCACHE: demote and retry write once.
                        if res == -95 && crate::bulk_io::rwf_dontcache_ok() {
                            crate::bulk_io::note_rwf_dontcache_unsupported();
                            slots[slot] = Some(st);
                            {
                                let s = slots[slot].as_mut().unwrap();
                                session.push_pwrite_flags(
                                    body_fd,
                                    abs,
                                    &s.buf,
                                    slot as u64,
                                    0,
                                )?;
                            }
                            in_flight += 1;
                            continue;
                        }
                        free_slots.push(slot);
                        abs_busy.remove(&abs);
                        return Err(StoreError::io(
                            &body_path,
                            std::io::Error::from_raw_os_error(-res),
                        ));
                    }
                    if res as usize != META_LEN {
                        free_slots.push(slot);
                        abs_busy.remove(&abs);
                        cold.push((create_fk, vout, spend_fk));
                    } else {
                        free_slots.push(slot);
                        abs_busy.remove(&abs);
                    }
                    if let Some(q) = abs_wait.get_mut(&abs) {
                        if let Some(next_ei) = q.pop_front() {
                            pending.push_front(next_ei);
                        }
                        if q.is_empty() {
                            abs_wait.remove(&abs);
                        }
                    }
                }
            }
        }

        arm(
            session,
            &mut free_slots,
            &mut slots,
            &mut pending,
            &mut abs_busy,
            &mut abs_wait,
            &work,
            &mut in_flight,
            body_fd,
        )?;
        session.sync_submission();
        let _ = session.submit();
    }

    // Any never-started edges (shouldn't happen) → cold.
    while let Some(ei) = pending.pop_front() {
        let (_, cfk, vout, sfk) = work[ei];
        cold.push((cfk, vout, sfk));
    }
    for q in abs_wait.into_values() {
        for ei in q {
            let (_, cfk, vout, sfk) = work[ei];
            cold.push((cfk, vout, sfk));
        }
    }

    Ok(cold)
    })?
}

/// Pick next edge that can start: abs not busy, or from wait queues.
fn next_ready(
    pending: &mut VecDeque<usize>,
    abs_busy: &HashSet<u64>,
    abs_wait: &mut HashMap<u64, VecDeque<usize>>,
    work: &[(u64, Fk, u32, Fk)],
) -> Option<usize> {
    // Drain pending: start if free, else park on wait list.
    while let Some(ei) = pending.pop_front() {
        let abs = work[ei].0;
        if !abs_busy.contains(&abs) {
            return Some(ei);
        }
        abs_wait.entry(abs).or_default().push_back(ei);
    }
    None
}

// ── Pure-write annotate (structural-known meta; no body pread) ─────────────

/// Annotate backend for pure-write path (Class A body never mmap'd).
///
/// Selected via `RBITCOIN_SPEND_ANN` / global `RBITCOIN_IO` (see [`crate::io_backend`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpendAnnBackend {
    /// Store 9 B via io_uring pwrite only (no pread).
    Uring,
    /// Store 9 B via libc `pwrite` (positional, no ring).
    Pwrite,
}

/// Resolve pure-write annotate backend from env hierarchy.
#[inline]
pub fn spend_ann_backend() -> SpendAnnBackend {
    match crate::io_backend::spend_ann_io_backend() {
        crate::io_backend::WriteIoBackend::Uring => SpendAnnBackend::Uring,
        crate::io_backend::WriteIoBackend::Pwrite => SpendAnnBackend::Pwrite,
    }
}

/// Decision from structural snapshot + spend_fk (no body read).
enum AnnotateOp {
    Skip,
    Write([u8; META_LEN]),
}

fn decide_annotate(
    field: Fk,
    flags: u8,
    spend_fk: Fk,
    spenders: &SpenderTable,
) -> Result<AnnotateOp, StoreError> {
    let multi = flags & output_flags::MULTI_SPENDER != 0;
    if !multi && field.is_null() {
        let mut meta = [0u8; META_LEN];
        meta[0..8].copy_from_slice(&spend_fk.0.to_le_bytes());
        meta[8] = flags & !output_flags::MULTI_SPENDER;
        return Ok(AnnotateOp::Write(meta));
    }
    if !multi && field == spend_fk {
        return Ok(AnnotateOp::Skip);
    }
    if !multi {
        // Promote sole → multi.
        let e1 = spenders.append(field, Fk::NULL)?;
        let e2 = spenders.append(spend_fk, e1)?;
        let mut meta = [0u8; META_LEN];
        meta[0..8].copy_from_slice(&e2.0.to_le_bytes());
        meta[8] = flags | output_flags::MULTI_SPENDER;
        return Ok(AnnotateOp::Write(meta));
    }
    // Already multi: prepend list node.
    let e = spenders.append(spend_fk, field)?;
    let mut meta = [0u8; META_LEN];
    meta[0..8].copy_from_slice(&e.0.to_le_bytes());
    meta[8] = flags | output_flags::MULTI_SPENDER;
    Ok(AnnotateOp::Write(meta))
}

/// Pure-write annotate: `known[i]` is structural `(field, flags)` for `abs_edges[i]`.
///
/// Sorts by abs for mmap locality. Returns OOB edges as cold (caller must hard-fail).
pub fn put_spend_batch_by_abs_meta_known(
    txs: &TxTable,
    spenders: &SpenderTable,
    abs_edges: &[(u64, Fk, u32, Fk)],
    known: &[(Fk, u8)],
    backend: SpendAnnBackend,
) -> Result<Vec<(Fk, u32, Fk)>, StoreError> {
    if abs_edges.is_empty() {
        return Ok(Vec::new());
    }
    if abs_edges.len() != known.len() {
        return Err(StoreError::Corrupt(
            "spend annotate known length mismatch",
        ));
    }
    for &(_, _, _, sfk) in abs_edges {
        if sfk.is_null() {
            return Err(StoreError::InvalidFk);
        }
    }

    // Build write list sorted by abs (mmap page locality).
    let mut order: Vec<usize> = (0..abs_edges.len()).collect();
    order.sort_unstable_by_key(|&i| abs_edges[i].0);

    let body_pub = txs.body.body_published_len();
    let mut cold: Vec<(Fk, u32, Fk)> = Vec::new();
    // (abs, create_fk, vout, spend_fk, payload) for non-skip
    let mut writes: Vec<(u64, Fk, u32, Fk, [u8; META_LEN])> = Vec::with_capacity(order.len());

    for &i in &order {
        let (abs, cfk, vout, sfk) = abs_edges[i];
        if abs.saturating_add(META_LEN as u64) > body_pub {
            cold.push((cfk, vout, sfk));
            continue;
        }
        let (field, flags) = known[i];
        match decide_annotate(field, flags, sfk, spenders)? {
            AnnotateOp::Skip => {}
            AnnotateOp::Write(meta) => writes.push((abs, cfk, vout, sfk, meta)),
        }
    }

    if writes.is_empty() {
        return Ok(cold);
    }

    match backend {
        SpendAnnBackend::Uring => put_spend_batch_pure_write_uring(txs, &writes, cold),
        SpendAnnBackend::Pwrite => put_spend_batch_pure_write_pwrite(txs, &writes, cold),
    }
}

/// libc pwrite-only (no pread, no ring) for prepared 9-byte metas.
fn put_spend_batch_pure_write_pwrite(
    txs: &TxTable,
    writes: &[(u64, Fk, u32, Fk, [u8; META_LEN])],
    mut cold: Vec<(Fk, u32, Fk)>,
) -> Result<Vec<(Fk, u32, Fk)>, StoreError> {
    for &(abs, cfk, vout, sfk, meta) in writes {
        // write_at_pwrite on body file via VarTable path: use body write helper.
        if let Err(_) = txs.body.write_body_abs_pwrite(abs, &meta) {
            cold.push((cfk, vout, sfk));
        }
    }
    Ok(cold)
}

/// io_uring pwrite-only (no pread) for prepared 9-byte metas.
fn put_spend_batch_pure_write_uring(
    txs: &TxTable,
    writes: &[(u64, Fk, u32, Fk, [u8; META_LEN])],
    mut cold: Vec<(Fk, u32, Fk)>,
) -> Result<Vec<(Fk, u32, Fk)>, StoreError> {
    if writes.is_empty() {
        return Ok(cold);
    }
    let body_fd: RawFd = txs.body.body_read_fd();
    let body_path = txs.body.body_file_path().to_path_buf();

    // Stable payload buffers for in-flight pwrites (outside TLS open so fallback
    // can still use `writes` / `cold` if the ring is unavailable).
    let mut bufs: Vec<[u8; META_LEN]> = writes.iter().map(|w| w.4).collect();
    let run = uring_session::with_thread_local(uring_session::DEFAULT_ENTRIES, |session| {
    let mut pending: VecDeque<usize> = (0..writes.len()).collect();
    let mut free_slots: Vec<usize> = (0..MAX_SLOTS.min(writes.len().max(1))).collect();
    let mut slots: Vec<Option<usize>> = (0..free_slots.len()).map(|_| None).collect();
    let mut in_flight = 0usize;
    let mut cold_local = cold.clone();

    let arm = |session: &mut UringSession,
               free_slots: &mut Vec<usize>,
               slots: &mut [Option<usize>],
               pending: &mut VecDeque<usize>,
               bufs: &mut [[u8; META_LEN]],
               writes: &[(u64, Fk, u32, Fk, [u8; META_LEN])],
               in_flight: &mut usize,
               body_fd: RawFd|
     -> Result<(), StoreError> {
        while *in_flight < slots.len() && session.free_sq() > 0 && !free_slots.is_empty() {
            let Some(wi) = pending.pop_front() else {
                break;
            };
            let slot = free_slots.pop().unwrap();
            slots[slot] = Some(wi);
            let abs = writes[wi].0;
            // Policy: all tx.body writes use RWF_DONTCACHE.
            let flags = crate::dontcache_policy::body_sqe_rw_flags();
            session.push_pwrite_flags(body_fd, abs, &mut bufs[wi], slot as u64, flags)?;
            *in_flight += 1;
        }
        Ok(())
    };

    arm(
        session,
        &mut free_slots,
        &mut slots,
        &mut pending,
        &mut bufs,
        writes,
        &mut in_flight,
        body_fd,
    )?;
    session.sync_submission();
    let _ = session.submit();

    while in_flight > 0 {
        let mut cqes = session.harvest_ready();
        if cqes.is_empty() {
            session.submit_and_wait_one()?;
            cqes = session.harvest_ready();
        }
        for (ud, res) in cqes {
            let slot = ud as usize;
            if slot >= slots.len() {
                return Err(StoreError::Corrupt("spend pure-write bad user_data"));
            }
            let wi = slots[slot]
                .take()
                .ok_or(StoreError::Corrupt("spend pure-write empty slot"))?;
            in_flight = in_flight.saturating_sub(1);
            if res < 0 {
                if res == -95 && crate::bulk_io::rwf_dontcache_ok() {
                    crate::bulk_io::note_rwf_dontcache_unsupported();
                    slots[slot] = Some(wi);
                    let abs = writes[wi].0;
                    session.push_pwrite_flags(body_fd, abs, &mut bufs[wi], slot as u64, 0)?;
                    in_flight += 1;
                    continue;
                }
                free_slots.push(slot);
                return Err(StoreError::io(
                    &body_path,
                    std::io::Error::from_raw_os_error(-res),
                ));
            }
            free_slots.push(slot);
            if res as usize != META_LEN {
                let (_, cfk, vout, sfk, _) = writes[wi];
                cold_local.push((cfk, vout, sfk));
            }
        }
        arm(
            session,
            &mut free_slots,
            &mut slots,
            &mut pending,
            &mut bufs,
            writes,
            &mut in_flight,
            body_fd,
        )?;
        session.sync_submission();
        let _ = session.submit();
    }

    while let Some(wi) = pending.pop_front() {
        let (_, cfk, vout, sfk, _) = writes[wi];
        cold_local.push((cfk, vout, sfk));
    }
    Ok(cold_local)
    });
    match run {
        Ok(Ok(c)) => Ok(c),
        Ok(Err(e)) => {
            rbitcoin_log::debug!(
                "store: spend annotate pure-write uring error ({e}); pwrite fallback"
            );
            for &(abs, cfk, vout, sfk, meta) in writes {
                if txs.body.write_body_abs_pwrite(abs, &meta).is_err() {
                    cold.push((cfk, vout, sfk));
                }
            }
            Ok(cold)
        }
        Err(e) => {
            rbitcoin_log::debug!(
                "store: spend annotate pure-write uring unavailable ({e}); pwrite fallback"
            );
            for &(abs, cfk, vout, sfk, meta) in writes {
                if txs.body.write_body_abs_pwrite(abs, &meta).is_err() {
                    cold.push((cfk, vout, sfk));
                }
            }
            Ok(cold)
        }
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use crate::tx_table::{InputRecord, OutputRecord, TxRecord, TxTable};
    use std::sync::atomic::{AtomicU64, Ordering};

    fn temp_table() -> (std::path::PathBuf, TxTable, SpenderTable) {
        static N: AtomicU64 = AtomicU64::new(0);
        let id = N.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!("rbitcoin-ann-known-{id}"));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let t = TxTable::create(&dir).unwrap();
        let s = SpenderTable::create(&dir).unwrap();
        (dir, t, s)
    }

    fn put_one(t: &TxTable) -> (Fk, u64, u64) {
        let tx = TxRecord {
            txid: [0x11; 32],
            version: 1,
            locktime: 0,
            input_start_fk: Fk::NULL,
            input_count: 1,
            output_start_fk: Fk::NULL,
            output_count: 1,
        };
        let inputs = vec![InputRecord {
            prev_txid: [0u8; 32],
            create_fk: Fk::NULL,
            prev_index: u32::MAX,
            sequence: u32::MAX,
            script_sig: vec![0x01],
            witness: vec![],
        }];
        let outputs = vec![OutputRecord::unspent(50, vec![0x51])];
        let fk = t
            .put_full_batch_indexed(&[(tx, inputs, outputs)], false)
            .unwrap()[0];
        let (off, len) = t.body_range(fk).unwrap();
        (fk, off, len)
    }

    #[test]
    fn pure_write_known_null_mmap_and_uring() {
        for backend in [SpendAnnBackend::Uring, SpendAnnBackend::Pwrite] {
            let (dir, t, spenders) = temp_table();
            let (cfk, off, len) = put_one(&t);
            let decoded = t.get_meta_and_outputs_batch_at(&[(off, len)]).unwrap();
            let (_m, _o, rels) = decoded[0].as_ref().unwrap();
            let abs = off + u64::from(rels[0]);
            let bulk = t.get_spender_meta_at_abs_batch(&[abs]).unwrap();
            let (field, flags) = bulk[0].expect("meta");
            assert!(field.is_null());
            let sfk = Fk(99);
            let cold = put_spend_batch_by_abs_meta_known(
                &t,
                &spenders,
                &[(abs, cfk, 0, sfk)],
                &[(field, flags)],
                backend,
            )
            .unwrap();
            assert!(cold.is_empty());
            let bulk2 = t.get_spender_meta_at_abs_batch(&[abs]).unwrap();
            let (f2, fl2) = bulk2[0].unwrap();
            assert_eq!(f2, sfk);
            assert_eq!(fl2 & output_flags::MULTI_SPENDER, 0);
            let _ = std::fs::remove_dir_all(&dir);
        }
    }

    /// Shipped uring RMW path must push body SQEs with RWF_DONTCACHE when supported.
    #[test]
    fn uring_rmw_body_sqe_sets_rwf_dontcache() {
        if !crate::bulk_io::io_uring_enabled() {
            // No ring in this environment — write path still requests DONTCACHE.
            assert!(crate::dontcache_policy::body_write());
            return;
        }
        let (dir, t, spenders) = temp_table();
        let (cfk, off, len) = put_one(&t);
        let decoded = t.get_meta_and_outputs_batch_at(&[(off, len)]).unwrap();
        let abs = off + u64::from(decoded[0].as_ref().unwrap().2[0]);
        let bulk = t.get_spender_meta_at_abs_batch(&[abs]).unwrap();
        let (field, flags) = bulk[0].unwrap();
        let _ = uring_session::test_take_last_sqe_rw_flags();
        let cold = put_spend_batch_by_abs_meta_known(
            &t,
            &spenders,
            &[(abs, cfk, 0, Fk(55))],
            &[(field, flags)],
            SpendAnnBackend::Uring,
        )
        .unwrap();
        assert!(cold.is_empty());
        let sqe_flags = uring_session::test_take_last_sqe_rw_flags();
        assert!(
            !sqe_flags.is_empty(),
            "uring spend annotate must push at least one SQE"
        );
        let expect = crate::dontcache_policy::body_sqe_rw_flags();
        assert!(
            sqe_flags.iter().any(|&f| f == expect),
            "body SQE rw_flags must match body_sqe_rw_flags()={expect:#x}; got {sqe_flags:?}"
        );
        // When DONTCACHE is supported, flags must be non-zero on body ops.
        if crate::bulk_io::rwf_dontcache_ok() {
            assert!(
                sqe_flags.iter().any(|&f| f == uring_session::RWF_DONTCACHE),
                "expected RWF_DONTCACHE on body r/w SQEs; got {sqe_flags:?}"
            );
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn pure_write_idempotent_skip() {
        let (dir, t, spenders) = temp_table();
        let (cfk, off, len) = put_one(&t);
        let decoded = t.get_meta_and_outputs_batch_at(&[(off, len)]).unwrap();
        let abs = off + u64::from(decoded[0].as_ref().unwrap().2[0]);
        let sfk = Fk(77);
        let bulk = t.get_spender_meta_at_abs_batch(&[abs]).unwrap();
        let (field, flags) = bulk[0].unwrap();
        put_spend_batch_by_abs_meta_known(
            &t,
            &spenders,
            &[(abs, cfk, 0, sfk)],
            &[(field, flags)],
            SpendAnnBackend::Pwrite,
        )
        .unwrap();
        // Second time with known field==sfk → skip
        put_spend_batch_by_abs_meta_known(
            &t,
            &spenders,
            &[(abs, cfk, 0, sfk)],
            &[(sfk, 0)],
            SpendAnnBackend::Pwrite,
        )
        .unwrap();
        let bulk2 = t.get_spender_meta_at_abs_batch(&[abs]).unwrap();
        assert_eq!(bulk2[0].unwrap().0, sfk);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
