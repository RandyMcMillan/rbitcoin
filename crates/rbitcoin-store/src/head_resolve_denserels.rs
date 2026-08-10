//! Plan Shape A head resolve: **txids in → denserels out** (or fk+range short-circuit).
//!
//! Schema **13+** two waves on probe (uring when available):
//! 1. **Hot probe** — page-coalesced loads for non-DONTCACHE segs (ages ≤3) → cands
//! 2. **ID / IDX** — page-grouped `txid.body` identity fill, then depth-first BIP30
//!    match in RAM + `record_range` idx
//! 3. If any key still unmatched: **cold probe** (DONTCACHE segs ages ≥4) for
//!    survivors only → full cand list → ID/IDX again
//! 4. **denserels** (optional) — packed body when outs are needed
//!
//! [`resolve_fk_and_range_batch`] is the **stamp short-circuit**: stops after
//! idx, returns `(fk, body_range)` so prep denserels-loads by offset.
//!
//! **IO shape:** probe may use one TLS [`UringSession`]; sidefile ID is
//! page-grouped bulk pread (one read per OS page of `txid.body`). Nested TLS
//! uring remains a hard error.
//!
//! Backend: `RBITCOIN_HEAD_RESOLVE_IO` / global `RBITCOIN_IO` (`uring` \| `pread`).

use crate::error::StoreError;
use crate::idx_body_pipeline::{run_idx_body_pipeline, BodyMode, IdxBodyJob};
use crate::io_backend::{self, ReadIoBackend};
use crate::tx_table::{
    decode_packed_tx_outs_with_spender_rels_secret, OutputRecord, TxRecord, TxTable,
};
use crate::txid_body::TxidBody;
use crate::uring_session::{self, UringSession};
use rbitcoin_primitives::Fk;
use std::time::Instant;

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
    id_idx_wave(
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
        None,
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
        id_idx_wave(
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
            None,
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

/// Kind tag for idx-page SQEs on the held plan session (`pack_ud` high bits).
const UD_KIND_IDX: u64 = 2;

/// Fill idx page buffers via held session; returns true if all pages complete.
#[cfg(target_os = "linux")]
fn fill_idx_pages(
    sess: &mut UringSession,
    pages: &[crate::tx_idx::IdxPagePlan],
    bufs: &mut [Vec<u8>],
) -> bool {
    // Staged SQEs on the held plan ring (no nested TLS bulk_io session).
    let flags = crate::dontcache_policy::idx_sqe_rw_flags(0, 1);
    for (i, page) in pages.iter().enumerate() {
        let ud = crate::uring_session::pack_ud(UD_KIND_IDX, i as u32);
        if sess
            .push_pread_flags(page.fd, page.page_off, &mut bufs[i], ud, flags)
            .is_err()
        {
            sess.drain_all();
            return false;
        }
    }
    sess.sync_submission();
    let mut results = vec![i32::MIN; pages.len()];
    let need = pages.len();
    let mut done = 0usize;
    while done < need {
        let mut cqes = sess.harvest_ready();
        if cqes.is_empty() {
            if sess.submit_and_wait_one().is_err() {
                sess.drain_all();
                return false;
            }
            cqes = sess.harvest_ready();
            if cqes.is_empty() {
                sess.drain_all();
                return false;
            }
        } else if sess.submit().is_err() {
            sess.drain_all();
            return false;
        }
        for (ud, res) in cqes {
            let (kind, slot) = crate::uring_session::unpack_ud(ud);
            if kind != UD_KIND_IDX || (slot as usize) >= results.len() {
                sess.drain_all();
                return false;
            }
            results[slot as usize] = res;
            done += 1;
        }
    }
    for (i, &res) in results.iter().enumerate() {
        if res < 0 || (res as usize) < pages[i].want {
            let page = &pages[i];
            let rc = unsafe {
                libc::pread(
                    page.fd,
                    bufs[i].as_mut_ptr() as *mut libc::c_void,
                    page.want,
                    page.page_off as libc::off_t,
                )
            };
            if rc < 0 || (rc as usize) < page.want {
                return false;
            }
        }
    }
    true
}

#[cfg(not(target_os = "linux"))]
fn fill_idx_pages(
    _sess: &mut UringSession,
    _pages: &[crate::tx_idx::IdxPagePlan],
    _bufs: &mut [Vec<u8>],
) -> bool {
    false
}

/// Body range for `fk` without nested TLS bulk when a plan session is held.
///
/// - `session == None`: [`VarTable::record_range`] (may use process bulk_io TLS).
/// - `session == Some`: plan idx pages + preads on the **held** session (or libc
///   fallback), then [`BodyRangeIdxPlan::decode_range`].
fn body_range_no_nested_tls(
    table: &TxTable,
    fk: Fk,
    session: Option<&mut UringSession>,
) -> Result<Option<(u64, u64)>, StoreError> {
    let Some(sess) = session else {
        return match table.body.record_range(fk) {
            Ok((off, len)) if len > 0 => Ok(Some((off, len))),
            Ok(_) | Err(StoreError::NotFound) | Err(StoreError::Corrupt(_)) => Ok(None),
            Err(e) => Err(e),
        };
    };
    let plan = match table.body.plan_body_range_idx(fk) {
        Ok(p) => p,
        Err(StoreError::NotFound) | Err(StoreError::Corrupt(_)) | Err(StoreError::InvalidFk) => {
            return Ok(None);
        }
        Err(e) => return Err(e),
    };
    if plan.pages.is_empty() {
        return Ok(None);
    }
    let mut bufs: Vec<Vec<u8>> = plan.pages.iter().map(|p| vec![0u8; p.want]).collect();
    let filled = fill_idx_pages(sess, &plan.pages, &mut bufs);
    if !filled {
        for (i, page) in plan.pages.iter().enumerate() {
            let rc = unsafe {
                libc::pread(
                    page.fd,
                    bufs[i].as_mut_ptr() as *mut libc::c_void,
                    page.want,
                    page.page_off as libc::off_t,
                )
            };
            if rc < 0 || (rc as usize) < page.want {
                return Ok(None);
            }
        }
    }
    let page_refs: Vec<&[u8]> = bufs.iter().map(|b| b.as_slice()).collect();
    match plan.decode_range(&page_refs) {
        Ok((off, len)) if len > 0 => Ok(Some((off, len))),
        Ok(_) | Err(StoreError::Corrupt(_)) => Ok(None),
        Err(e) => Err(e),
    }
}

/// Sidefile ID (page-grouped bulk) then depth-first BIP30 match + idx.
///
/// Collects every still-active key's cand create_fks, fills identities with
/// **one bulk pread per OS page** of `txid.body`, then walks each key's cand
/// list in original order in RAM. Idx uses held session when provided (no
/// nested TLS bulk from `record_range`).
///
/// When `session` is `Some`, ID + IDX page preads ride that **already-held**
/// plan ring. When `None`, libc pread for ID and normal `record_range` for IDX.
fn id_idx_wave(
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
    mut session: Option<&mut UringSession>,
) -> Result<(), StoreError> {
    // ── ID stage: page-grouped bulk fill for all active cands ─────────────
    let mut all_fks: Vec<Fk> = Vec::new();
    for (ki, cands) in cands_by_key.iter().enumerate() {
        if skip_if_won && winner[ki].is_some() {
            continue;
        }
        all_fks.extend_from_slice(cands);
    }
    let t_id = Instant::now();
    let (id_map, _pages) = match session.as_deref_mut() {
        Some(sess) => side.get_many_page_grouped_on_session(&all_fks, sess)?,
        None => side.get_many_page_grouped(&all_fks)?,
    };
    *id_ns = id_ns.saturating_add(t_id.elapsed().as_nanos() as u64);
    *body_lookups = body_lookups.saturating_add(id_map.len() as u64);

    // ── RAM depth-first match + IDX ───────────────────────────────────────
    for (ki, cands) in cands_by_key.iter().enumerate() {
        if skip_if_won && winner[ki].is_some() {
            continue;
        }
        for (rank0, &fk) in cands.iter().enumerate() {
            let rank = (rank0 + 1) as u64;
            let Some(id) = fk.get() else {
                *miss_peeks = miss_peeks.saturating_add(1);
                continue;
            };
            let Some(got) = id_map.get(&id) else {
                *miss_peeks = miss_peeks.saturating_add(1);
                continue;
            };
            if *got != txids[ki] {
                *miss_peeks = miss_peeks.saturating_add(1);
                continue;
            }
            crate::head_resolve_stats::add_hit_rank(rank);
            let t_idx = Instant::now();
            if let Some(range) = body_range_no_nested_tls(table, fk, session.as_deref_mut())? {
                winner[ki] = Some((fk, range));
                crate::head_resolve_stats::note_local_hit_age(local_age, first_fks, fk.0);
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

// ── uring probe path (ID stage is page-grouped bulk, shared with pread) ─────

fn resolve_fk_and_range_uring(
    table: &TxTable,
    txids: &[[u8; 32]],
) -> Result<Vec<([u8; 32], Option<(Fk, (u64, u64))>)>, StoreError> {
    crate::head_resolve_stats::add_keys(txids.len() as u64);

    // TLS ring for head probe waves; sidefile ID is page-grouped pread.
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

    // ── Wave 1: hot head pages (uring) + page-grouped ID/IDX ──────────────
    let t_probe = Instant::now();
    let hot_cands = table
        .head
        .probe_candidates_batch_hot_on_session(&mixed, session)?;
    probe_ns = probe_ns.saturating_add(t_probe.elapsed().as_nanos() as u64);
    cands_total = cands_total.saturating_add(hot_cands.iter().map(|v| v.len() as u64).sum());
    debug_assert_eq!(session.in_flight(), 0);

    id_idx_wave(
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
        Some(session),
    )?;

    // ── Wave 2: full cold head for survivors + ID/IDX ─────────────────────
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
        cands_total = cands_total.saturating_add(cold_cands.iter().map(|v| v.len() as u64).sum());
        debug_assert_eq!(session.in_flight(), 0);

        id_idx_wave(
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
            Some(session),
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
