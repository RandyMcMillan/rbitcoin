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
const MAX_SLOTS: usize = 512;

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

    let mut session = UringSession::new(uring_session::DEFAULT_ENTRIES)?;
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
                session.push_pread(body_fd, abs, &mut s.buf, slot as u64)?;
            }
            *in_flight += 1;
        }
        Ok(())
    };

    arm(
        &mut session,
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
                        session.push_pwrite(body_fd, abs, &s.buf, slot as u64)?;
                    }
                    in_flight += 1;
                }
                Phase::Writing => {
                    if res < 0 {
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
            &mut session,
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
