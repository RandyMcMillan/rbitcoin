//! Plan Shape A head resolve: **txids in → denserels out** (or fk+range short-circuit).
//!
//! Schema **13+** identity machine:
//! 1. **probe** — [`SegmentedTxHead::probe_candidates_batch`] (page-coalesced)
//! 2. **txid.body** — dense sidefile identity (fixed `fk → offset`)
//! 3. **idx** — body range for winners (stamp short-circuit)
//! 4. **denserels** — full packed body when outs are needed
//!
//! [`resolve_fk_and_range_batch`] stops after identity + idx range so prep can
//! denserels-load by offset without re-idx.

use crate::error::StoreError;
use crate::idx_body_pipeline::{run_idx_body_pipeline, BodyMode, IdxBodyJob};
use crate::io_backend::{self, ReadIoBackend};
use crate::tx_table::{
    decode_packed_tx_outs_with_spender_rels_secret, OutputRecord, TxRecord, TxTable,
};
use rbitcoin_primitives::Fk;
use std::time::Instant;

/// Stamp short-circuit: **txids → (fk, body_range)** via probe + **txid.body**
/// identity + idx range (schema 13+).
///
/// Multi-cand identity peeks the dense sidefile (fixed fk→offset), not
/// Prefix33 body. `body_range` from idx so prep denserels-loads by offset.
pub fn resolve_fk_and_range_batch(
    table: &TxTable,
    txids: &[[u8; 32]],
) -> Result<Vec<([u8; 32], Option<(Fk, (u64, u64))>)>, StoreError> {
    if txids.is_empty() {
        return Ok(Vec::new());
    }
    resolve_fk_and_range_sidefile(table, txids)
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
    // Schema 13: identity is `txid.body` (sidefile). The pread machine uses that
    // path; uring multi-stage Prefix33 against body is retired for denserels.
    match io_backend::head_resolve_io_backend() {
        ReadIoBackend::Uring | ReadIoBackend::Pread => resolve_pread(table, txids),
    }
}

/// Depth-round: sidefile identity peeks, then idx range for winners.
fn resolve_fk_and_range_sidefile(
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

    let mut winner: Vec<Option<(Fk, (u64, u64))>> = vec![None; txids.len()];
    let max_depth = all_cands.iter().map(|c| c.len()).max().unwrap_or(0);
    let mut unresolved: Vec<bool> = all_cands.iter().map(|c| !c.is_empty()).collect();

    for depth in 0..max_depth {
        let mut round: Vec<(usize, Fk, u8)> = Vec::new();
        for (ki, cands) in all_cands.iter().enumerate() {
            if !unresolved[ki] {
                continue;
            }
            if let Some(&fk) = cands.get(depth) {
                round.push((ki, fk, (depth as u8).saturating_add(1)));
            }
        }
        if round.is_empty() {
            break;
        }
        // Identity: dense txid.body peeks (no tx.body).
        let t_id = Instant::now();
        let fks: Vec<Fk> = round.iter().map(|(_, fk, _)| *fk).collect();
        let ids = table.txid_sidefile().get_many(&fks)?;
        let id_ns = t_id.elapsed().as_nanos() as u64;
        crate::head_resolve_stats::add_body(id_ns);

        let mut body_lookups = 0u64;
        let mut miss_peeks = 0u64;
        let mut matched: Vec<(usize, Fk, u8)> = Vec::new();
        for ((ki, fk, rank), got) in round.into_iter().zip(ids.into_iter()) {
            body_lookups = body_lookups.saturating_add(1);
            match got {
                Some(got) if got == txids[ki] => {
                    crate::head_resolve_stats::add_hit_rank(rank as u64);
                    matched.push((ki, fk, rank));
                    unresolved[ki] = false;
                }
                Some(_) => miss_peeks = miss_peeks.saturating_add(1),
                None => miss_peeks = miss_peeks.saturating_add(1),
            }
        }
        crate::head_resolve_stats::add_body_lookups(body_lookups);
        crate::head_resolve_stats::add_miss_peeks(miss_peeks);

        // Idx range only for identity winners.
        if !matched.is_empty() {
            let t_idx = Instant::now();
            for &(ki, fk, _rank) in &matched {
                if let Ok((off, len)) = table.body.record_range(fk) {
                    if len > 0 {
                        winner[ki] = Some((fk, (off, len)));
                    }
                }
            }
            crate::head_resolve_stats::add_idx(t_idx.elapsed().as_nanos() as u64);
        }
        if !unresolved.iter().any(|&u| u) {
            break;
        }
    }

    Ok(txids
        .iter()
        .enumerate()
        .map(|(i, t)| (*t, winner[i]))
        .collect())
}

// ── pread path: batch probe + sidefile + denserels body ─────────────────────

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

    // Single-cand: denserels body + sidefile identity.
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
            let got = match table.txid_sidefile().get(fk) {
                Ok(t) => t,
                Err(_) => {
                    crate::head_resolve_stats::add_miss_peeks(1);
                    continue;
                }
            };
            if got != txids[ki] {
                crate::head_resolve_stats::add_miss_peeks(1);
                continue;
            }
            crate::head_resolve_stats::add_hit_rank(1);
            winner_fk[ki] = Some(fk);
            match decode_packed_tx_outs_with_spender_rels_secret(
                &job.body,
                Some(&table.secret),
            ) {
                Ok(mut decoded) => {
                    decoded.0.txid = got;
                    dens_decoded.insert(ki, decoded);
                }
                Err(StoreError::NotFound) | Err(StoreError::Corrupt(_)) => {}
                Err(e) => return Err(e),
            }
        }
        crate::head_resolve_stats::add_body_lookups(body_lookups);
    }

    // Multi-cand: sidefile identity then denserels for winners.
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
            let t_id = Instant::now();
            let fks: Vec<Fk> = round.iter().map(|(_, fk, _)| *fk).collect();
            let ids = table.txid_sidefile().get_many(&fks)?;
            crate::head_resolve_stats::add_body(t_id.elapsed().as_nanos() as u64);

            let mut body_lookups = 0u64;
            let mut miss_peeks = 0u64;
            for ((ki, fk, rank), got) in round.into_iter().zip(ids.into_iter()) {
                body_lookups = body_lookups.saturating_add(1);
                match got {
                    Some(got) if got == txids[ki] => {
                        crate::head_resolve_stats::add_hit_rank(rank as u64);
                        winner_fk[ki] = Some(fk);
                        unresolved[ki] = false;
                        need_dens.push((ki, fk));
                    }
                    Some(_) | None => miss_peeks = miss_peeks.saturating_add(1),
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
        for ((ki, fk), job) in need_dens.into_iter().zip(jobs.into_iter()) {
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
    for (i, txid) in txids.iter().enumerate() {
        let row = winner_fk[i].map(|fk| (fk, dens_decoded.remove(&i)));
        out.push((*txid, row));
    }
    Ok((out, dens_ns_acc))
}
