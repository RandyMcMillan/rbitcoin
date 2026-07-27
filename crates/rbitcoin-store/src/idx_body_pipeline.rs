//! Completion-driven **tx.idx → tx.body** io_uring pipeline.
//!
//! One short-lived [`UringSession`] arms idx preads (8/16 B) and, on each
//! successful idx CQE, immediately arms the corresponding body pread — no full
//! phase barrier between tables. Jobs with a pre-known range skip idx.
//!
//! Used by confirm load (full / denserels bodies) and archive head-resolve
//! (prefix33). Fallback when io_uring is unavailable: mmap
//! [`VarTable::record_range_batch`] + [`crate::bulk_io::pread_batch`] body.
//!
//! **Concurrency:** read-only preads on published ranges; safe for prep + load
//! concurrent rings. Caller owns job buffers until the call returns.

use crate::bulk_io::{self, ReadOp};
use crate::error::StoreError;
use crate::file::FILE_HEADER_LEN;
use crate::uring_session::{self, UringSession};
use crate::var_table::VarTable;
use rbitcoin_primitives::Fk;
use std::os::fd::RawFd;

/// What body bytes to fetch after the range is known.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BodyMode {
    /// Full packed Class A payload (confirm create decode).
    Full,
    /// Full packed payload for denserels/outs-only decode (pin_new).
    OutsDenserels,
    /// Leading ≤33 body bytes (txid prefix / head resolve).
    Prefix33,
}

impl BodyMode {
    #[inline]
    fn body_len(self, range_len: u64) -> u64 {
        match self {
            BodyMode::Full | BodyMode::OutsDenserels => range_len,
            BodyMode::Prefix33 => range_len.min(33),
        }
    }
}

/// One cold Class A layout + body job.
///
/// **Input:** set `id` (1-based Class A fk) and optionally `range` when known
/// (sticky / FIFO). **Output:** `range`, `body` (on success), `ok`.
#[derive(Debug)]
pub struct IdxBodyJob {
    /// 1-based create id (`Fk.0` when non-null).
    pub id: u64,
    /// Known `(body_off, body_len)` skips idx; filled by pipeline when resolved.
    pub range: Option<(u64, u64)>,
    /// Body bytes (mode-sized) when `ok`.
    pub body: Vec<u8>,
    /// True when body pread completed for the expected length.
    pub ok: bool,
}

impl IdxBodyJob {
    pub fn new(id: u64, range: Option<(u64, u64)>) -> Self {
        Self {
            id,
            range,
            body: Vec::new(),
            ok: false,
        }
    }

    pub fn from_fk(fk: Fk, range: Option<(u64, u64)>) -> Option<Self> {
        let id = fk.get()?;
        if id == 0 {
            return None;
        }
        Some(Self::new(id, range))
    }
}

const STAGE_IDX: u64 = 0;
const STAGE_BODY: u64 = 1;

/// Env escape hatch: `RBITCOIN_IDX_BODY_PIPELINE=0` forces fallback even when
/// io_uring is available (rollout / A-B).
pub fn pipeline_enabled() -> bool {
    if !bulk_io::io_uring_enabled() {
        return false;
    }
    match std::env::var("RBITCOIN_IDX_BODY_PIPELINE") {
        Ok(s) => {
            let s = s.to_ascii_lowercase();
            s != "0" && s != "false" && s != "off"
        }
        Err(_) => true,
    }
}

/// Run idx→body pipeline (or fallback). Mutates `jobs` in place.
///
/// Jobs with invalid / OOB ids are left `ok = false` without failing the batch
/// (caller applies confirm hard invariants vs head-resolve skip policy).
pub fn run_idx_body_pipeline(
    table: &VarTable,
    jobs: &mut [IdxBodyJob],
    mode: BodyMode,
) -> Result<(), StoreError> {
    if jobs.is_empty() {
        return Ok(());
    }
    if pipeline_enabled() {
        match run_uring(table, jobs, mode) {
            Ok(()) => return Ok(()),
            Err(e) => {
                rbitcoin_log::debug!(
                    "store: idx_body_pipeline uring unavailable ({e}); using fallback"
                );
                // Reset partial progress before fallback.
                for j in jobs.iter_mut() {
                    if j.range.is_none() {
                        j.body.clear();
                        j.ok = false;
                    } else if !j.ok {
                        j.body.clear();
                    }
                }
            }
        }
    }
    run_fallback(table, jobs, mode)
}

fn run_fallback(
    table: &VarTable,
    jobs: &mut [IdxBodyJob],
    mode: BodyMode,
) -> Result<(), StoreError> {
    // Resolve missing ranges via sorted mmap batch (same modality as commit sticky).
    let mut need_fk: Vec<Fk> = Vec::new();
    let mut need_slot: Vec<usize> = Vec::new();
    for (i, j) in jobs.iter().enumerate() {
        if j.range.is_none() && j.id > 0 {
            need_fk.push(Fk(j.id));
            need_slot.push(i);
        }
    }
    if !need_fk.is_empty() {
        let ranges = table.record_range_batch(&need_fk)?;
        for (slot, r) in need_slot.into_iter().zip(ranges.into_iter()) {
            jobs[slot].range = r;
        }
    }

    let body_fd = table.body_read_fd();
    let body_pub = table.body_published_len();
    let body_path = table.body_file_path();

    let mut submitted: Vec<usize> = Vec::new();
    for (i, j) in jobs.iter_mut().enumerate() {
        j.ok = false;
        j.body.clear();
        let Some((off, full_len)) = j.range else {
            continue;
        };
        let want = mode.body_len(full_len);
        if want == 0 || off.saturating_add(want) > body_pub {
            continue;
        }
        j.body.resize(want as usize, 0);
        submitted.push(i);
    }
    if submitted.is_empty() {
        return Ok(());
    }

    // SAFETY: each jobs[i].body is a distinct allocation; submitted indices unique.
    let mut read_ops: Vec<ReadOp<'_>> = Vec::with_capacity(submitted.len());
    for &i in &submitted {
        let off = jobs[i].range.unwrap().0;
        let len = jobs[i].body.len();
        let ptr = jobs[i].body.as_mut_ptr();
        let slice = unsafe { std::slice::from_raw_parts_mut(ptr, len) };
        read_ops.push(ReadOp {
            fd: body_fd,
            offset: off,
            buf: slice,
            result: i32::MIN,
        });
    }
    bulk_io::pread_batch(&mut read_ops);
    for (ro, &i) in read_ops.iter().zip(submitted.iter()) {
        if ro.result < 0 {
            return Err(StoreError::io(
                body_path,
                std::io::Error::from_raw_os_error(-ro.result),
            ));
        }
        if ro.result as usize == jobs[i].body.len() {
            jobs[i].ok = true;
        }
    }
    Ok(())
}

fn run_uring(
    table: &VarTable,
    jobs: &mut [IdxBodyJob],
    mode: BodyMode,
) -> Result<(), StoreError> {
    let count = table.count();
    let body_end = table.body_published_len();
    let idx_fd: RawFd = table.idx_read_fd();
    let body_fd: RawFd = table.body_read_fd();
    let idx_path = table.idx_file_path().to_path_buf();
    let body_path = table.body_file_path().to_path_buf();

    // Per-job idx scratch (stable while in flight).
    let n = jobs.len();
    let mut idx_bufs: Vec<[u8; 16]> = vec![[0u8; 16]; n];
    let mut idx_nbytes: Vec<u8> = vec![0u8; n];
    // 0=need arm, 1=idx inflight, 2=body inflight, 3=done, 4=fail, 5=need body arm
    let mut stage: Vec<u8> = vec![0u8; n];

    // Order: idx-needed jobs by id (idx file locality); body-only after.
    let mut order: Vec<usize> = (0..n).collect();
    order.sort_unstable_by_key(|&i| {
        let j = &jobs[i];
        // body-only (range known) sort after idx jobs but by body off when known
        match j.range {
            Some((off, _)) => (1u8, off),
            None => (0u8, j.id),
        }
    });

    // Validate / pre-mark impossible jobs.
    // Body-only (range already known) does not require a published fk id —
    // callers may use synthetic ids when only `(off,len)` is known.
    for i in 0..n {
        let id = jobs[i].id;
        if let Some((off, len)) = jobs[i].range {
            let want = mode.body_len(len);
            if want == 0 || off.saturating_add(want) > body_end {
                stage[i] = 4;
                jobs[i].ok = false;
            }
            // else stage 0 → arm body only
        } else if id == 0 || id > count {
            stage[i] = 4;
            jobs[i].ok = false;
        } else {
            let nbytes: u8 = if id < count { 16 } else { 8 };
            idx_nbytes[i] = nbytes;
        }
    }

    let mut session = UringSession::new(uring_session::DEFAULT_ENTRIES)?;
    let mut next_arm = 0usize;
    let mut remaining = stage.iter().filter(|&&s| s != 4).count();

    // Arm as many as SQ allows; harvest; repeat until remaining == 0.
    while remaining > 0 {
        // Prefer re-arming stage=5 (idx done, need body) before new idx jobs.
        for i in 0..n {
            if stage[i] != 5 || session.free_sq() == 0 {
                continue;
            }
            let (off, _) = jobs[i].range.expect("stage5 has range");
            let ud = uring_session::pack_ud(STAGE_BODY, i as u32);
            session.push_pread(body_fd, off, &mut jobs[i].body, ud)?;
            stage[i] = 2;
        }

        // Submit phase: fill free SQ slots from sorted order.
        while next_arm < order.len() && session.free_sq() > 0 {
            let i = order[next_arm];
            if stage[i] != 0 {
                next_arm += 1;
                continue;
            }
            if let Some((off, len)) = jobs[i].range {
                let want = mode.body_len(len) as usize;
                if want == 0 {
                    stage[i] = 4;
                    remaining = remaining.saturating_sub(1);
                    next_arm += 1;
                    continue;
                }
                jobs[i].body.resize(want, 0);
                let ud = uring_session::pack_ud(STAGE_BODY, i as u32);
                session.push_pread(body_fd, off, &mut jobs[i].body, ud)?;
                stage[i] = 2;
                next_arm += 1;
            } else {
                let id = jobs[i].id;
                let off = FILE_HEADER_LEN as u64 + (id - 1) * 8;
                let nb = idx_nbytes[i] as usize;
                let ud = uring_session::pack_ud(STAGE_IDX, i as u32);
                session.push_pread(idx_fd, off, &mut idx_bufs[i][..nb], ud)?;
                stage[i] = 1;
                next_arm += 1;
            }
        }
        session.sync_submission();

        if session.in_flight() == 0 {
            break;
        }

        let mut cqes = session.harvest_ready();
        if cqes.is_empty() {
            session.submit_and_wait_one()?;
            cqes = session.harvest_ready();
        } else {
            let _ = session.submit();
        }

        for (ud, res) in cqes {
            let (kind, slot) = uring_session::unpack_ud(ud);
            let i = slot as usize;
            if i >= n {
                return Err(StoreError::Corrupt("idx_body_pipeline bad user_data"));
            }
            match kind {
                STAGE_IDX => {
                    if stage[i] != 1 {
                        return Err(StoreError::Corrupt("idx_body_pipeline idx stage"));
                    }
                    let nb = idx_nbytes[i] as usize;
                    if res < 0 {
                        return Err(StoreError::io(
                            &idx_path,
                            std::io::Error::from_raw_os_error(-res),
                        ));
                    }
                    if res as usize != nb {
                        stage[i] = 4;
                        remaining = remaining.saturating_sub(1);
                        jobs[i].ok = false;
                        continue;
                    }
                    let start = u64::from_le_bytes(idx_bufs[i][..8].try_into().unwrap());
                    let end = if jobs[i].id < count {
                        u64::from_le_bytes(idx_bufs[i][8..16].try_into().unwrap())
                    } else {
                        body_end
                    };
                    if end < start {
                        stage[i] = 4;
                        remaining = remaining.saturating_sub(1);
                        jobs[i].ok = false;
                        continue;
                    }
                    let full_len = end - start;
                    jobs[i].range = Some((start, full_len));
                    let want = mode.body_len(full_len) as usize;
                    if want == 0 || start.saturating_add(want as u64) > body_end {
                        stage[i] = 4;
                        remaining = remaining.saturating_sub(1);
                        jobs[i].ok = false;
                        continue;
                    }
                    jobs[i].body.resize(want, 0);
                    if session.free_sq() > 0 {
                        let ud = uring_session::pack_ud(STAGE_BODY, i as u32);
                        session.push_pread(body_fd, start, &mut jobs[i].body, ud)?;
                        stage[i] = 2;
                    } else {
                        stage[i] = 5; // arm body on next loop
                    }
                }
                STAGE_BODY => {
                    if stage[i] != 2 {
                        return Err(StoreError::Corrupt("idx_body_pipeline body stage"));
                    }
                    if res < 0 {
                        return Err(StoreError::io(
                            &body_path,
                            std::io::Error::from_raw_os_error(-res),
                        ));
                    }
                    if res as usize == jobs[i].body.len() {
                        jobs[i].ok = true;
                        stage[i] = 3;
                    } else {
                        jobs[i].ok = false;
                        stage[i] = 4;
                    }
                    remaining = remaining.saturating_sub(1);
                }
                _ => {
                    return Err(StoreError::Corrupt("idx_body_pipeline bad stage kind"));
                }
            }
        }

        // Re-scan order if body-only jobs were skipped while SQ was full.
        if next_arm >= order.len() && session.in_flight() == 0 {
            let mut any_pending = false;
            for i in 0..n {
                if stage[i] == 0 || stage[i] == 5 {
                    any_pending = true;
                    break;
                }
            }
            if any_pending {
                next_arm = 0;
            } else {
                break;
            }
        }
    }

    session.drain_all();
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tx_table::{
        decode_packed_tx_with_spender_rels, InputRecord, OutputRecord, TxRecord, TxTable,
    };
    use rbitcoin_primitives::Fk;
    use std::sync::atomic::{AtomicU64, Ordering};

    fn temp_tx() -> (std::path::PathBuf, TxTable) {
        static N: AtomicU64 = AtomicU64::new(0);
        let id = N.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!("rbitcoin-idx-body-pipe-{id}"));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let t = TxTable::create(&dir).unwrap();
        (dir, t)
    }

    fn put_n(t: &TxTable, n: u8) -> Vec<Fk> {
        let mut fks = Vec::new();
        for i in 0..n {
            let mut txid = [0u8; 32];
            txid[0] = i.wrapping_add(1);
            let tx = TxRecord {
                txid,
                version: 1,
                locktime: 0,
                input_start_fk: Fk::NULL,
                input_count: 1,
                output_start_fk: Fk::NULL,
                output_count: 1 + (i % 3) as u32,
            };
            let inputs = vec![InputRecord {
                prev_txid: [0u8; 32],
                create_fk: Fk::NULL,
                prev_index: u32::MAX,
                sequence: u32::MAX,
                script_sig: vec![i],
                witness: vec![],
            }];
            let mut outs = Vec::new();
            for j in 0..(1 + (i % 3)) {
                outs.push(OutputRecord::unspent(j as i64 + 1, vec![0x51, j]));
            }
            fks.push(
                t.put_full_batch_indexed(&[(tx, inputs, outs)], true)
                    .unwrap()[0],
            );
        }
        fks
    }

    #[test]
    fn pipeline_full_matches_record_range_and_decode() {
        let (dir, t) = temp_tx();
        let fks = put_n(&t, 12);
        // Unsorted + one pre-known range.
        let (known_off, known_len) = t.body.record_range(fks[3]).unwrap();
        let mut jobs: Vec<IdxBodyJob> = fks
            .iter()
            .enumerate()
            .map(|(i, fk)| {
                let range = if i == 3 {
                    Some((known_off, known_len))
                } else {
                    None
                };
                IdxBodyJob::new(fk.0, range)
            })
            .collect();
        // Shuffle order.
        jobs.swap(0, 7);
        jobs.swap(2, 10);
        run_idx_body_pipeline(&t.body, &mut jobs, BodyMode::Full).unwrap();
        for j in &jobs {
            assert!(j.ok, "id={}", j.id);
            let seq = t.body.record_range(Fk(j.id)).unwrap();
            assert_eq!(j.range, Some(seq));
            let (tx, _ins, outs, rels) =
                decode_packed_tx_with_spender_rels(&j.body).unwrap();
            assert_eq!(outs.len(), rels.len());
            assert_eq!(tx.output_count as usize, outs.len());
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn pipeline_prefix33_and_denserels_modes() {
        let (dir, t) = temp_tx();
        let fks = put_n(&t, 5);
        let mut jobs: Vec<IdxBodyJob> = fks.iter().map(|fk| IdxBodyJob::new(fk.0, None)).collect();
        run_idx_body_pipeline(&t.body, &mut jobs, BodyMode::Prefix33).unwrap();
        for j in &jobs {
            assert!(j.ok);
            assert!(j.body.len() <= 33);
            assert!(!j.body.is_empty());
        }
        let mut jobs2: Vec<IdxBodyJob> = fks.iter().map(|fk| IdxBodyJob::new(fk.0, None)).collect();
        run_idx_body_pipeline(&t.body, &mut jobs2, BodyMode::OutsDenserels).unwrap();
        for j in &jobs2 {
            assert!(j.ok);
            let (_tx, outs, rels) =
                crate::tx_table::decode_packed_tx_outs_with_spender_rels(&j.body).unwrap();
            assert_eq!(outs.len(), rels.len());
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn pipeline_fallback_with_env_off() {
        let (dir, t) = temp_tx();
        let fks = put_n(&t, 4);
        let prev = std::env::var_os("RBITCOIN_IDX_BODY_PIPELINE");
        std::env::set_var("RBITCOIN_IDX_BODY_PIPELINE", "0");
        let mut jobs: Vec<IdxBodyJob> = fks.iter().map(|fk| IdxBodyJob::new(fk.0, None)).collect();
        run_idx_body_pipeline(&t.body, &mut jobs, BodyMode::Full).unwrap();
        for j in &jobs {
            assert!(j.ok, "fallback id={}", j.id);
            assert_eq!(j.range, Some(t.body.record_range(Fk(j.id)).unwrap()));
        }
        match prev {
            Some(v) => std::env::set_var("RBITCOIN_IDX_BODY_PIPELINE", v),
            None => std::env::remove_var("RBITCOIN_IDX_BODY_PIPELINE"),
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn oob_and_null_ids_not_ok() {
        let (dir, t) = temp_tx();
        let fks = put_n(&t, 2);
        let mut jobs = vec![
            IdxBodyJob::new(0, None),
            IdxBodyJob::new(fks[0].0, None),
            IdxBodyJob::new(99_999, None),
        ];
        run_idx_body_pipeline(&t.body, &mut jobs, BodyMode::Full).unwrap();
        assert!(!jobs[0].ok);
        assert!(jobs[1].ok);
        assert!(!jobs[2].ok);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
