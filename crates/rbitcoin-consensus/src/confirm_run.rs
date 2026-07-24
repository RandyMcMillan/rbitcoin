//! Multi-block confirm orchestrator (IBD Class C path).
//!
//! Pipeline (optimistic scripts — assumevalid-shaped):
//! ```text
//! LOAD STAGE (ibd-confirm-load OS thread):
//!   Class A + pin parents → resolve → wire → assemble
//!   (only stage that may touch the store / parent cache)
//! SCRIPTS STAGE (ibd-confirm OS thread + rayon):
//!   pure CPU: verify ScriptCheckJob list from LoadedBatch — no Query, no disk
//! WRITE STAGE (ibd-confirm-write OS thread, FIFO):
//!   structural (spentness/maturity/subsidy) → class_c → spend annotate → tip GC
//! ```
//!
//! [`confirm_archived_run`] runs all stages synchronously (tests / tip path).
//! IBD pipelines so load(N+1) ∥ scripts(N) ∥ write(N−1).
//!
//! **No wave fill:** load decodes bodies **once** into batch-local full Class A
//! ([`rbitcoin_query::BatchFullBodies`]) + outs FIFO; thin create_fk edges and
//! sparse parent pins are batch-local. Wire rebuild uses the batch bodies (no
//! second store full-decode for batch creates).
//!
//! **Scripts purity:** [`confirm_scripts_phase`] is a pure function of
//! [`LoadedBatch`] → [`ScriptOkBatch`]. Jobs already carry prevouts, txs, and
//! softfork flags from load; verification must not open tables or the parent cache.

use crate::block::{
    assemble_block_prevouts, bip34_height_script, block_has_witness, structural_validate_spends,
    ScriptCheckJob, ValidationContext,
};
use crate::error::ConsensusError;
use crate::header::{median_time_past_times, validate_header};
use crate::milestone::Milestone;
use crate::params::{genesis_block, ChainParams};
use crate::confirm_phase_stats;
use bitcoin::hashes::Hash;
use bitcoin::{Block, Target};
use rbitcoin_primitives::Height;
use rbitcoin_query::Query;
use rbitcoin_store::StoreError;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::Ordering;
use std::time::Instant;

/// One height resolved for the confirm wave (header + Class A body fks).
struct BodyMeta {
    height: Height,
    hash: [u8; 32],
    header_fk: rbitcoin_primitives::Fk,
    header_rec: rbitcoin_store::HeaderRecord,
    tx_fks: Vec<rbitcoin_primitives::Fk>,
}

/// Assemble output for one height (held through scripts → write).
struct Prepared {
    height: Height,
    header_fk: rbitcoin_primitives::Fk,
    tx_fks: Vec<rbitcoin_primitives::Fk>,
    jobs: Vec<ScriptCheckJob>,
    /// `(prev_txid, vout, spending_tx_fk, create_tx_fk)` — create_fk for Direct
    /// spend annotate without `tx.head`.
    spends: Vec<(
        [u8; 32],
        u32,
        rbitcoin_primitives::Fk,
        rbitcoin_primitives::Fk,
    )>,
    /// Total fees from assemble (for structural coinbase subsidy check).
    fees: i64,
    check_scripts: bool,
    time: u32,
    bits: bitcoin::CompactTarget,
    /// Header hash of this block (prev-link for the next height in the run).
    hash: [u8; 32],
}

/// Wire + assemble complete; script jobs still attached (not yet verified).
///
/// `Send` so IBD can hand off load → scripts threads.
/// Sparse spent-filtered parents ride on the batch (not tip-GCed).
pub struct LoadedBatch {
    prepared: Vec<Prepared>,
    wire_blocks: Vec<Block>,
    /// Per-batch pin map: load → assemble → write structural, then drop.
    batch_parents: rbitcoin_query::BatchParents,
}

/// Script-verified batch ready for ordered write (structural + Class C + spends).
///
/// `Send` so IBD can hand off scripts → write.
pub struct ScriptOkBatch {
    prepared: Vec<Prepared>,
    wire_blocks: Vec<Block>,
    batch_parents: rbitcoin_query::BatchParents,
}

/// Confirm a contiguous tip-extension run of archived bodies (sync all stages).
///
/// Prefer the split phases in IBD for pipeline overlap.
pub fn confirm_archived_run(
    query: &Query,
    params: &ChainParams,
    milestone: Milestone,
    blocks: &[(Height, [u8; 32])],
) -> Result<Vec<rbitcoin_primitives::Fk>, ConsensusError> {
    let mat = confirm_load_phase(query, params, milestone, blocks)?;
    let ok = confirm_scripts_phase(mat.batch)?;
    confirm_write_phase(query, params, milestone, ok.batch)
}

/// Outcome of load: batch ready for scripts + pure work wall.
pub struct ConfirmLoadOutcome {
    pub batch: LoadedBatch,
    /// Full load wall (Class A + parent pin + resolve → assemble).
    pub work_ns: u64,
}

/// Outcome of the scripts stage: ready batch + pure script wall.
pub struct ConfirmScriptOutcome {
    pub batch: ScriptOkBatch,
    /// Script verify only (when produced by [`confirm_scripts_phase`]).
    /// When produced by [`confirm_script_phase`], includes load work too.
    pub work_ns: u64,
}

/// LOAD STAGE: load batch Class A + pin parents →
/// resolve → wire → assemble.
///
/// Does **not** run scripts, advance tip, or probe durable spentness (except
/// provisional same-run doubles during assemble).
///
/// Inline Class A load is included in [`ConfirmLoadOutcome::work_ns`]
/// and also accrued into [`confirm_phase_stats::LOAD_NS`] (historical
/// counter name) for IBD log continuity.
pub fn confirm_load_phase(
    query: &Query,
    params: &ChainParams,
    milestone: Milestone,
    blocks: &[(Height, [u8; 32])],
) -> Result<ConfirmLoadOutcome, ConsensusError> {
    if blocks.is_empty() {
        return Err(ConsensusError::BadBlock("empty confirm batch"));
    }
    for w in blocks.windows(2) {
        if w[1].0 .0 != w[0].0 .0.saturating_add(1) {
            return Err(ConsensusError::BadBlock("confirm run not contiguous"));
        }
    }

    let heights: Vec<u32> = blocks.iter().map(|(h, _)| h.0).collect();
    let items: Vec<(u32, [u8; 32])> = blocks.iter().map(|(h, hash)| (h.0, *hash)).collect();
    let batch_end = heights.last().copied().unwrap_or(0);

    let t_work = Instant::now();

    // Decode bodies once, pin parents + thin edges (batch-local).
    let t_load = Instant::now();
    let (batch_parents, batch_thin, batch_bodies) =
        load_confirm_batch(query, &heights, &items, batch_end)?;
    let load_ns = t_load.elapsed().as_nanos() as u64;
    confirm_phase_stats::LOAD_NS.fetch_add(load_ns, Ordering::Relaxed);

    let t_resolve = Instant::now();
    let metas = resolve_body_metas(query, blocks)?;
    confirm_phase_stats::RESOLVE_NS.fetch_add(
        t_resolve.elapsed().as_nanos() as u64,
        Ordering::Relaxed,
    );

    let wire_blocks = wire_rebuild(query, &metas, &batch_bodies)?;
    let prepared = assemble_run(
        query,
        params,
        milestone,
        metas,
        &wire_blocks,
        &batch_parents,
        &batch_thin,
    )?;

    let work_ns = t_work.elapsed().as_nanos() as u64;
    Ok(ConfirmLoadOutcome {
        batch: LoadedBatch {
            prepared,
            wire_blocks,
            batch_parents,
        },
        work_ns,
    })
}

/// SCRIPTS STAGE: pure verification of jobs already assembled at load.
///
/// **No store / Query / side effects.** Input is a [`LoadedBatch`] (script jobs
/// hold prevouts + txs + softfork flags); output is a [`ScriptOkBatch`] for the
/// write queue. Clears jobs after success so write carries spends/fees only.
///
/// Uses rayon for CPU parallelism only — does not touch disk or process-global
/// tables (aside from the rayon pool and script phase timers).
pub fn confirm_scripts_phase(
    mut batch: LoadedBatch,
) -> Result<ConfirmScriptOutcome, ConsensusError> {
    let t_work = Instant::now();
    script_wave(&batch.prepared)?;
    for p in &mut batch.prepared {
        p.jobs.clear();
        p.jobs.shrink_to_fit();
    }
    let work_ns = t_work.elapsed().as_nanos() as u64;
    Ok(ConfirmScriptOutcome {
        batch: ScriptOkBatch {
            prepared: batch.prepared,
            wire_blocks: batch.wire_blocks,
            batch_parents: batch.batch_parents,
        },
        work_ns,
    })
}

/// LOAD + SCRIPTS in one call (tests / tip path / ChainHub compat).
///
/// Work is full load (Class A + parents) + pure scripts.
pub fn confirm_script_phase(
    query: &Query,
    params: &ChainParams,
    milestone: Milestone,
    blocks: &[(Height, [u8; 32])],
) -> Result<ConfirmScriptOutcome, ConsensusError> {
    let mat = confirm_load_phase(query, params, milestone, blocks)?;
    let mat_ns = mat.work_ns;
    let mut ok = confirm_scripts_phase(mat.batch)?;
    ok.work_ns = ok.work_ns.saturating_add(mat_ns);
    Ok(ok)
}

/// Keep only heights strictly above the confirmed tip (dup write race).
#[inline]
fn write_height_needed(tip: u32, height: u32) -> bool {
    height > tip
}

/// WRITE STAGE: structural → class_c → spend annotate → tip GC (FIFO caller).
pub fn confirm_write_phase(
    query: &Query,
    params: &ChainParams,
    milestone: Milestone,
    mut batch: ScriptOkBatch,
) -> Result<Vec<rbitcoin_primitives::Fk>, ConsensusError> {
    // Idempotent: skip heights already on the confirmed tip (dup pipeline race).
    let tip = query.tip_height().map(|h| h.0).unwrap_or(0);
    let mut prep = Vec::with_capacity(batch.prepared.len());
    let mut wires = Vec::with_capacity(batch.wire_blocks.len());
    for (p, w) in batch
        .prepared
        .into_iter()
        .zip(batch.wire_blocks.into_iter())
    {
        if !write_height_needed(tip, p.height.0) {
            continue;
        }
        prep.push(p);
        wires.push(w);
    }
    if prep.is_empty() {
        return Ok(Vec::new());
    }
    batch.prepared = prep;
    batch.wire_blocks = wires;

    structural_run(
        query,
        params,
        milestone,
        &batch.prepared,
        &batch.wire_blocks,
        &batch.batch_parents,
    )?;
    let n_blocks = batch.prepared.len();
    let out = class_c_commit(query, &mut batch.prepared)?;
    post_commit(query, &batch.prepared)?;
    // batch_parents dropped here with ScriptOkBatch — no tip GC of sparse pins.
    confirm_phase_stats::BLOCKS.fetch_add(n_blocks as u64, Ordering::Relaxed);
    Ok(out)
}

impl LoadedBatch {
    /// Heights and header hashes in this batch (for events / feed scrub).
    pub fn heights_hashes(&self) -> Vec<(u32, [u8; 32])> {
        self.prepared
            .iter()
            .map(|p| (p.height.0, p.hash))
            .collect()
    }

    pub fn len(&self) -> usize {
        self.prepared.len()
    }

    pub fn is_empty(&self) -> bool {
        self.prepared.is_empty()
    }
}

impl ScriptOkBatch {
    /// Heights and header hashes in this batch (for events / feed scrub).
    pub fn heights_hashes(&self) -> Vec<(u32, [u8; 32])> {
        self.prepared
            .iter()
            .map(|p| (p.height.0, p.hash))
            .collect()
    }

    pub fn len(&self) -> usize {
        self.prepared.len()
    }

    pub fn is_empty(&self) -> bool {
        self.prepared.is_empty()
    }
}

#[cfg(test)]
mod write_idempotent_tests {
    use super::write_height_needed;

    /// Heights at or below tip must be stripped before structural write
    /// (dup pipeline race after scripts claim the same tip+1 twice).
    #[test]
    fn filter_keeps_only_heights_above_tip() {
        let tip = 100u32;
        let heights = [98u32, 99, 100, 101, 102];
        let kept: Vec<u32> = heights
            .into_iter()
            .filter(|&h| write_height_needed(tip, h))
            .collect();
        assert_eq!(kept, vec![101, 102]);
        assert!(!write_height_needed(tip, tip));
        assert!(!write_height_needed(0, 0));
        assert!(write_height_needed(0, 1));
    }

    #[test]
    fn three_stage_entry_points_exist() {
        // Load / scripts / write are separate public surfaces for IBD.
        let _m = super::confirm_load_phase;
        let _s = super::confirm_scripts_phase;
        let _w = super::confirm_write_phase;
        let _combined = super::confirm_script_phase;
        let _sync = super::confirm_archived_run;
    }

    /// Scripts stage accepts an empty LoadedBatch without a Query (pure API).
    #[test]
    fn scripts_phase_is_pure_no_query_arg() {
        use super::{confirm_scripts_phase, LoadedBatch};
        let batch = LoadedBatch {
            prepared: Vec::new(),
            wire_blocks: Vec::new(),
            batch_parents: rbitcoin_query::BatchParents::new(),
        };
        let ok = confirm_scripts_phase(batch).expect("empty scripts ok");
        assert!(ok.batch.prepared.is_empty());
        assert!(ok.batch.wire_blocks.is_empty());
    }
}



// ─── phases ───────────────────────────────────────────────────────────────────

/// Load Class A + pin parents + thin edges for the claimed batch.
fn load_confirm_batch(
    query: &Query,
    heights: &[u32],
    items: &[(u32, [u8; 32])],
    _batch_end: u32,
) -> Result<
    (
        rbitcoin_query::BatchParents,
        rbitcoin_query::BatchThin,
        rbitcoin_query::BatchFullBodies,
    ),
    ConsensusError,
> {
    if heights.is_empty() {
        return Ok((
            rbitcoin_query::BatchParents::new(),
            rbitcoin_query::BatchThin::new(),
            rbitcoin_query::BatchFullBodies::new(),
        ));
    }
    let (_st, batch_parents, batch_thin, batch_bodies) = query
        .load_confirm_parents(items)
        .map_err(ConsensusError::Store)?;
    Ok((batch_parents, batch_thin, batch_bodies))
}

fn resolve_body_metas(
    query: &Query,
    blocks: &[(Height, [u8; 32])],
) -> Result<Vec<BodyMeta>, ConsensusError> {
    let mut metas = Vec::with_capacity(blocks.len());
    for &(height, hash) in blocks {
        // Prefer load-stage header plan (no store page faults after load).
        if let Some(plan) = query.confirm_parent_cache().get_header_plan(height.0) {
            if plan.header_rec.hash == hash {
                metas.push(BodyMeta {
                    height,
                    hash,
                    header_fk: plan.header_fk,
                    header_rec: plan.header_rec,
                    tx_fks: plan.tx_fks,
                });
                continue;
            }
        }
        // Store fallback (load miss / hash mismatch).
        let (header_fk, header_rec) = query
            .get_header_by_hash(&hash)
            .map_err(ConsensusError::Store)?
            .ok_or(ConsensusError::Store(StoreError::NotFound))?;
        let tx_fks = query
            .header_tx_fks(header_fk, Some(&hash))
            .map_err(ConsensusError::Store)?
            .ok_or(ConsensusError::Store(StoreError::Corrupt(
                "confirm without archived body",
            )))?;
        metas.push(BodyMeta {
            height,
            hash,
            header_fk,
            header_rec,
            tx_fks,
        });
    }
    Ok(metas)
}

fn wire_rebuild(
    query: &Query,
    metas: &[BodyMeta],
    batch_bodies: &rbitcoin_query::BatchFullBodies,
) -> Result<Vec<Block>, ConsensusError> {
    // Sequential by design: `rayon_audit` benches show par_iter reconstruct is
    // *slower* than sequential for 1–128 blocks. Load decoded Class A once into
    // `batch_bodies` — wire builds `bitcoin::Transaction` from that map (store
    // only if a create is missing from the batch, which should not happen).
    let t0 = Instant::now();
    let mut blks = Vec::with_capacity(metas.len());
    for m in metas {
        let prev_hash = query
            .confirm_parent_cache()
            .get_header_plan(m.height.0)
            .map(|p| p.prev_hash);
        blks.push(
            query
                .reconstruct_archived_block_from_parts_cached(
                    m.header_rec.clone(),
                    m.tx_fks.clone(),
                    prev_hash,
                    Some(batch_bodies),
                )
                .map_err(ConsensusError::Store)?,
        );
    }
    let ns = t0.elapsed().as_nanos() as u64;
    confirm_phase_stats::RECONSTRUCT_WIRE_NS.fetch_add(ns, Ordering::Relaxed);
    confirm_phase_stats::RECONSTRUCT_NS.fetch_add(ns, Ordering::Relaxed);
    Ok(blks)
}

fn assemble_run(
    query: &Query,
    params: &ChainParams,
    milestone: Milestone,
    metas: Vec<BodyMeta>,
    wire_blocks: &[Block],
    batch_parents: &rbitcoin_query::BatchParents,
    batch_thin: &rbitcoin_query::BatchThin,
) -> Result<Vec<Prepared>, ConsensusError> {
    // Provisional same-run double-spend only (not durable spentness).
    let mut pending_spent: HashSet<([u8; 32], u32)> = HashSet::new();
    let mut pending_creates: HashMap<([u8; 32], u32), rbitcoin_primitives::Fk> = HashMap::new();
    let mut time_window: Vec<u32> = Vec::with_capacity(11);
    let mut prepared: Vec<Prepared> = Vec::with_capacity(metas.len());

    for (i, meta) in metas.into_iter().enumerate() {
        let block = &wire_blocks[i];
        let height = meta.height;
        let block_hash = meta.hash;
        let ctx = ValidationContext::at(params, height, milestone);

        if i == 0 {
            // MTP + prev link for the first height of a load batch.
            //
            // IBD pipelines load(N+1) ∥ scripts(N) ∥ write(N−1). Tip GC drops
            // header plans for h ≤ tip when write advances tip. Assemble must
            // not snapshot tip once: a concurrent tip_gc can drop plans while
            // our tip read is still the pre-write value → false "plan missing
            // above tip" (retryable load incomplete spam on restart / dense
            // pipeline). Prefer plan when present; else **store** if confirmed.
            if height.0 >= 1 {
                let prev_h = Height(height.0 - 1);
                let start = prev_h.0.saturating_sub(10);
                let prev_hash = block.header.prev_blockhash.to_byte_array();
                let mut times = Vec::with_capacity(11);
                for h in start..=prev_h.0 {
                    if let Some(plan) = query.confirm_parent_cache().get_header_plan(h) {
                        times.push(plan.header_rec.timestamp);
                        if h == prev_h.0 && plan.header_rec.hash != prev_hash {
                            return Err(ConsensusError::BadPrev);
                        }
                    } else if let Some((_fk, rec)) = query
                        .header_at_height(Height(h))
                        .map_err(ConsensusError::Store)?
                    {
                        // Confirmed (plan tip-GC'd or never cached) — store wins.
                        times.push(rec.timestamp);
                        if h == prev_h.0 && rec.hash != prev_hash {
                            return Err(ConsensusError::BadPrev);
                        }
                    } else {
                        // Unconfirmed parent with no plan: earlier load not ready.
                        return Err(ConsensusError::Store(StoreError::Corrupt(
                            "confirm: load incomplete (parent header plan missing above tip)",
                        )));
                    }
                }
                let mtp = median_time_past_times(&times);
                if block.header.time <= mtp {
                    return Err(ConsensusError::BadHeader("timestamp <= median-time-past"));
                }
                time_window = times;

                // Bits / PoW / checkpoint: store when parent is confirmed; else plan.
                if query
                    .header_at_height(prev_h)
                    .map_err(ConsensusError::Store)?
                    .is_some()
                {
                    validate_header(query, params, height, &block.header)?;
                } else if let Some(prev_plan) =
                    query.confirm_parent_cache().get_header_plan(prev_h.0)
                {
                    if let Some(cp) = params.checkpoint_at(height) {
                        if cp != block.header.block_hash() {
                            return Err(ConsensusError::BadHeader("checkpoint mismatch"));
                        }
                    }
                    let prev_bits =
                        bitcoin::CompactTarget::from_consensus(prev_plan.header_rec.bits);
                    let expected = expected_bits_extending(
                        query,
                        params,
                        height,
                        prev_bits,
                        prev_plan.header_rec.timestamp,
                    )?;
                    if block.header.bits != expected {
                        return Err(ConsensusError::BadHeader("incorrect proof of work bits"));
                    }
                    let target = Target::from_compact(block.header.bits);
                    if target > params.pow_limit {
                        return Err(ConsensusError::BadHeader("target above pow limit"));
                    }
                    block
                        .header
                        .validate_pow(target)
                        .map_err(|_| ConsensusError::InvalidPow)?;
                } else {
                    return Err(ConsensusError::Store(StoreError::Corrupt(
                        "confirm: load incomplete (parent header plan missing above tip)",
                    )));
                }
            } else {
                validate_header(query, params, height, &block.header)?;
            }
        } else {
            let prev = &prepared[i - 1];
            if block.header.prev_blockhash.to_byte_array() != prev.hash {
                return Err(ConsensusError::BadPrev);
            }
            let mtp = median_time_past_times(&time_window);
            if block.header.time <= mtp {
                return Err(ConsensusError::BadHeader("timestamp <= median-time-past"));
            }
            if let Some(cp) = params.checkpoint_at(height) {
                if cp != block.header.block_hash() {
                    return Err(ConsensusError::BadHeader("checkpoint mismatch"));
                }
            }
            let expected = expected_bits_extending(query, params, height, prev.bits, prev.time)?;
            if block.header.bits != expected {
                return Err(ConsensusError::BadHeader("incorrect proof of work bits"));
            }
            let target = Target::from_compact(block.header.bits);
            if target > params.pow_limit {
                return Err(ConsensusError::BadHeader("target above pow limit"));
            }
            block
                .header
                .validate_pow(target)
                .map_err(|_| ConsensusError::InvalidPow)?;
        }

        // Height-gated structure soft forks (archive prep skipped these).
        if params.bip34_active_at(height.0) {
            check_bip34(block, height.0)?;
        }
        if block_has_witness(block) && !params.segwit_active_at(height.0) {
            return Err(ConsensusError::BadBlock("unexpected witness before segwit"));
        }

        // BIP325: full signet challenge on tip confirm only.
        if height.0 > 0 {
            if let Some(challenge) = params.signet_challenge.as_ref() {
                crate::signet::validate_signet_block_solution(block, challenge.as_script())?;
            }
        }

        let t_connect = Instant::now();
        let (script_jobs, spends, fees) = assemble_block_prevouts(
            query,
            block,
            &ctx,
            Some(&meta.tx_fks),
            &mut pending_spent,
            &mut pending_creates,
            batch_parents,
            batch_thin,
        )?;
        confirm_phase_stats::CONNECT_NS
            .fetch_add(t_connect.elapsed().as_nanos() as u64, Ordering::Relaxed);

        time_window.push(block.header.time);
        if time_window.len() > 11 {
            let n = time_window.len() - 11;
            time_window.drain(0..n);
        }

        prepared.push(Prepared {
            height,
            header_fk: meta.header_fk,
            tx_fks: meta.tx_fks,
            jobs: script_jobs,
            spends,
            fees,
            check_scripts: !milestone.skips_scripts_at(height.0),
            time: block.header.time,
            bits: block.header.bits,
            hash: block_hash,
        });
    }
    Ok(prepared)
}

/// Durable spentness + maturity + subsidy after scripts (height order).
fn structural_run(
    query: &Query,
    params: &ChainParams,
    milestone: Milestone,
    prepared: &[Prepared],
    wire_blocks: &[Block],
    batch_parents: &rbitcoin_query::BatchParents,
) -> Result<(), ConsensusError> {
    let t0 = Instant::now();
    let mut pending_spent: HashSet<([u8; 32], u32)> = HashSet::new();
    for (i, p) in prepared.iter().enumerate() {
        let ctx = ValidationContext::at(params, p.height, milestone);
        structural_validate_spends(
            query,
            &wire_blocks[i],
            &ctx,
            Some(&p.tx_fks),
            &p.spends,
            p.fees,
            &mut pending_spent,
            batch_parents,
        )?;
    }
    confirm_phase_stats::STRUCTURAL_NS
        .fetch_add(t0.elapsed().as_nanos() as u64, Ordering::Relaxed);
    Ok(())
}

/// Verify all script jobs in `prepared` (CPU only; jobs are self-contained).
fn script_wave(prepared: &[Prepared]) -> Result<(), ConsensusError> {
    let t_script = Instant::now();
    {
        let mut n_slices = 0usize;
        let mut total_jobs = 0usize;
        let mut only: Option<&[ScriptCheckJob]> = None;
        for p in prepared {
            if p.check_scripts && !p.jobs.is_empty() {
                n_slices += 1;
                total_jobs += p.jobs.len();
                only = Some(p.jobs.as_slice());
            }
        }
        match n_slices {
            0 => {}
            1 => crate::block::verify_scripts_pool(only.unwrap())?,
            _ => {
                let mut all_jobs: Vec<&ScriptCheckJob> = Vec::with_capacity(total_jobs);
                for p in prepared {
                    if p.check_scripts {
                        all_jobs.extend(p.jobs.iter());
                    }
                }
                crate::block::verify_scripts_pool_jobs(&all_jobs)?;
            }
        }
    }
    confirm_phase_stats::SCRIPT_NS
        .fetch_add(t_script.elapsed().as_nanos() as u64, Ordering::Relaxed);
    Ok(())
}

fn class_c_commit(
    query: &Query,
    prepared: &mut [Prepared],
) -> Result<Vec<rbitcoin_primitives::Fk>, ConsensusError> {
    let t_class_c = Instant::now();
    let items: Vec<rbitcoin_query::ConfirmPrepared> = prepared
        .iter_mut()
        .map(|p| rbitcoin_query::ConfirmPrepared {
            height: p.height,
            header_fk: p.header_fk,
            tx_fks: std::mem::take(&mut p.tx_fks),
        })
        .collect();
    let out = query
        .confirm_blocks_run(&items)
        .map_err(ConsensusError::Store)?;
    confirm_phase_stats::CLASS_C_NS
        .fetch_add(t_class_c.elapsed().as_nanos() as u64, Ordering::Relaxed);
    Ok(out)
}

fn post_commit(query: &Query, prepared: &[Prepared]) -> Result<(), ConsensusError> {
    // Direct IBD: batch durable spend annotations for the whole run **before**
    // the next confirm batch (spentness = confirmed-strong + annotation).
    // Tip mode usually writes spends on archive; still safe if spend_index on.
    let t_spent = Instant::now();
    if query.spend_index_enabled() && query.index_mode().uses_durable_spends() {
        // Prefer create_fk + body range from tx.idx (no process-local range cache).
        // Resolve ranges once per unique create, then annotate via ranged batch.
        use std::collections::{HashMap, HashSet};
        let mut pending: Vec<(
            rbitcoin_primitives::Fk,
            u32,
            rbitcoin_primitives::Fk,
        )> = Vec::new();
        let mut n_skip = 0u64;
        let mut unique_creates: HashSet<u64> = HashSet::new();
        for p in prepared {
            for &(_txid, vout, sfk, cfk) in &p.spends {
                if sfk.is_null() || cfk.is_null() {
                    n_skip = n_skip.saturating_add(1);
                    continue;
                }
                pending.push((cfk, vout, sfk));
                if let Some(id) = cfk.get() {
                    unique_creates.insert(id);
                }
            }
        }
        let mut range_by_create: HashMap<u64, (u64, u64)> =
            HashMap::with_capacity(unique_creates.len());
        for id in unique_creates {
            let fk = rbitcoin_primitives::Fk(id);
            if let Ok(r) = query.store().tx_body_range(fk) {
                range_by_create.insert(id, r);
            }
        }
        let mut ranged: Vec<(
            rbitcoin_primitives::Fk,
            u32,
            rbitcoin_primitives::Fk,
            u64,
            u64,
        )> = Vec::with_capacity(pending.len());
        let mut by_create: Vec<(
            rbitcoin_primitives::Fk,
            u32,
            rbitcoin_primitives::Fk,
        )> = Vec::new();
        for (cfk, vout, sfk) in pending {
            if let Some(id) = cfk.get() {
                if let Some(&(off, len)) = range_by_create.get(&id) {
                    ranged.push((cfk, vout, sfk, off, len));
                    continue;
                }
            }
            // Still no range (corrupt / missing body) — fall back to idx path.
            by_create.push((cfk, vout, sfk));
        }
        confirm_phase_stats::SPEND_ANNOTATE_RANGED
            .fetch_add(ranged.len() as u64, Ordering::Relaxed);
        confirm_phase_stats::SPEND_ANNOTATE_IDX
            .fetch_add(by_create.len() as u64, Ordering::Relaxed);
        confirm_phase_stats::SPEND_ANNOTATE_SKIP.fetch_add(n_skip, Ordering::Relaxed);
        if !ranged.is_empty() {
            query
                .store()
                .put_spend_batch_by_create_ranged(&ranged)
                .map_err(ConsensusError::Store)?;
        }
        if !by_create.is_empty() {
            query
                .store()
                .put_spend_batch_by_create(&by_create)
                .map_err(ConsensusError::Store)?;
        }
    }
    confirm_phase_stats::UTXO_APPLY_NS
        .fetch_add(t_spent.elapsed().as_nanos() as u64, Ordering::Relaxed);

    // IBD (Direct): skip per-spend unpin — tip GC drops the same parent outs.
    // Tip mode: still retire spent sparse parents so long-lived cache stays lean.
    let t_unpin = Instant::now();
    if query.index_mode() != rbitcoin_query::IndexMode::Direct {
        let all_spends: Vec<(rbitcoin_primitives::Fk, u32)> = prepared
            .iter()
            .flat_map(|p| {
                p.spends.iter().filter_map(|(_txid, vout, _sfk, cfk)| {
                    if cfk.is_null() {
                        None
                    } else {
                        Some((*cfk, *vout))
                    }
                })
            })
            .collect();
        let _ = query.unpin_spent_parent_outs(&all_spends);
    }
    confirm_phase_stats::UNPIN_NS.fetch_add(
        t_unpin.elapsed().as_nanos() as u64,
        Ordering::Relaxed,
    );

    // Prune confirm-parent cache for heights at/below new tip.
    if let Some(tip) = prepared.last().map(|p| p.height.0) {
        let t_tip = Instant::now();
        query.advance_parent_cache_tip(tip);
        confirm_phase_stats::CACHE_TIP_NS.fetch_add(
            t_tip.elapsed().as_nanos() as u64,
            Ordering::Relaxed,
        );
    }
    Ok(())
}

fn check_bip34(block: &Block, height: u32) -> Result<(), ConsensusError> {
    let coinbase = &block.txdata[0];
    let bytes = coinbase.input[0].script_sig.as_bytes();
    let expected = bip34_height_script(height);
    if bytes.len() < expected.len() || &bytes[..expected.len()] != expected.as_slice() {
        return Err(ConsensusError::BadBlock("bip34 height encoding"));
    }
    Ok(())
}

fn expected_bits_extending(
    query: &Query,
    params: &ChainParams,
    height: Height,
    prev_bits: bitcoin::CompactTarget,
    prev_time: u32,
) -> Result<bitcoin::CompactTarget, ConsensusError> {
    use bitcoin::CompactTarget;
    if height.0 == 0 {
        return Ok(genesis_block(params).header.bits);
    }
    let interval = params.difficulty_adjustment_interval();
    if params.no_pow_retargeting() || height.0 % interval != 0 {
        return Ok(prev_bits);
    }
    let first_height = Height(height.0 - interval);
    let (_fk, first_rec) = query
        .header_at_height(first_height)
        .map_err(ConsensusError::Store)?
        .ok_or(ConsensusError::BadHeader("missing retarget first header"))?;
    let timespan = prev_time.saturating_sub(first_rec.timestamp) as u64;
    Ok(CompactTarget::from_next_work_required(
        prev_bits,
        timespan,
        &params.btc,
    ))
}
