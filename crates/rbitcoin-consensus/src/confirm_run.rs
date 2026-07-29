//! Multi-block confirm orchestrator (IBD / tip Class C path).
//!
//! **Primary height-ordered pipeline** (raw wire → validated tip):
//! ```text
//! PREP STAGE (ibd-confirm-load OS thread):
//!   wire Block → structure → plan Class A (stamp create_fk) → pin parents once
//!   → assemble (uses intake wire; **no Class-A wire rebuild**)
//! SCRIPTS STAGE (ibd-confirm OS thread + rayon):
//!   pure CPU verify — no Query, no disk
//! COMMIT STAGE (ibd-confirm-write OS thread, FIFO):
//!   Class A commit (if plan) + structural + class_c + spend annotate + tip GC
//! ```
//! IBD pipelines prep(N+1) ∥ scripts(N) ∥ commit(N−1). One Class A appender.
//!
//! [`confirm_wire_run`] is the unified entry (tests / tip / IBD).
//! [`confirm_archived_run`] remains for already-archived Class A only.
//!
//! **Scripts purity:** [`confirm_scripts_phase`] is pure
//! [`LoadedBatch`] → [`ScriptOkBatch`].

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

/// Txids already consensus-script-verified under tip-era softforks (live mempool
/// after accept). Empty = verify all jobs (IBD). Passed through load → scripts only.
pub type ScriptPreverified = std::collections::HashSet<[u8; 32]>;

/// Pipeline context so prep(N+1) can run while commit(N) has not advanced tip.
///
/// Prep thread owns reserved create-fk HWM and in-flight creates/outs from
/// batches sitting in load→scripts→write queues. Commit remains sole Class A
/// appender and applies batches in height order.
#[derive(Clone, Debug, Default)]
pub struct WirePrepPipeline {
    /// Expected first height of this batch (store tip+1, or last prepped + 1).
    pub path_lo: u32,
    /// Parent of `path_lo` when ahead of store tip (last wire hash of prior prepped batch).
    pub parent_hash: Option<[u8; 32]>,
    /// Inclusive create-fk start for [`Query::archive_plan_mega_from`].
    pub next_tx_start: u64,
    /// Prior uncommitted plans: create txid → fk.
    pub in_flight_creates: HashMap<[u8; 32], rbitcoin_primitives::Fk>,
    /// Prior uncommitted plans: create fk id → (tx meta, outs) for parent pin.
    pub in_flight_outs: HashMap<
        u64,
        (
            rbitcoin_store::TxRecord,
            Vec<rbitcoin_store::OutputRecord>,
        ),
    >,
}

/// Wire + assemble complete; script jobs still attached (not yet verified).
///
/// `Send` so IBD can hand off prep → scripts threads.
/// Sparse spent-filtered parents ride on the batch (not tip-GCed).
/// When [`archive_plan`] is `Some`, commit stage appends Class A before
/// structural / annotate (single ordered commit era).
pub struct LoadedBatch {
    prepared: Vec<Prepared>,
    wire_blocks: Vec<Block>,
    /// Per-batch pin map: prep → assemble → write structural, then drop.
    batch_parents: rbitcoin_query::BatchParents,
    /// Mempool preverified txids for scripts stage (tip follow); empty on IBD.
    script_preverified: ScriptPreverified,
    /// Planned Class A write from wire prep (committed in write stage).
    pub archive_plan: Option<rbitcoin_query::ArchiveWritePlan>,
}

/// Script-verified batch ready for ordered commit (Class A + structural + C).
///
/// `Send` so IBD can hand off scripts → write.
pub struct ScriptOkBatch {
    prepared: Vec<Prepared>,
    wire_blocks: Vec<Block>,
    batch_parents: rbitcoin_query::BatchParents,
    pub archive_plan: Option<rbitcoin_query::ArchiveWritePlan>,
}

/// Confirm a contiguous tip-extension run of archived bodies (sync all stages).
///
/// Prefer the split phases in IBD for pipeline overlap.
/// Script preverified set is empty (full verify).
pub fn confirm_archived_run(
    query: &Query,
    params: &ChainParams,
    milestone: Milestone,
    blocks: &[(Height, [u8; 32])],
) -> Result<Vec<rbitcoin_primitives::Fk>, ConsensusError> {
    confirm_archived_run_preverified(query, params, milestone, blocks, &ScriptPreverified::new())
}

/// Like [`confirm_archived_run`], skipping script verify for `preverified` txids
/// (tip follow: live mempool txs already checked at accept).
pub fn confirm_archived_run_preverified(
    query: &Query,
    params: &ChainParams,
    milestone: Milestone,
    blocks: &[(Height, [u8; 32])],
    preverified: &ScriptPreverified,
) -> Result<Vec<rbitcoin_primitives::Fk>, ConsensusError> {
    let mat = confirm_load_phase_preverified(query, params, milestone, blocks, preverified)?;
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
    confirm_load_phase_preverified(query, params, milestone, blocks, &ScriptPreverified::new())
}

/// Load with optional mempool script-preverified txids (tip follow).
pub fn confirm_load_phase_preverified(
    query: &Query,
    params: &ChainParams,
    milestone: Milestone,
    blocks: &[(Height, [u8; 32])],
    preverified: &ScriptPreverified,
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
    // Residency after this returns / after wire:
    //   • pin_new dense outs live only in process OutFifo (not on the batch)
    //   • BatchParents holds need-vouts only (rides load→scripts→write queues)
    //   • BatchFullBodies (creates) is used for wire then dropped — not queued
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

    // Wire rebuild needs full create Class A; free it before assemble so the
    // queued LoadedBatch does not retain create full-bodies (only wire blocks).
    let wire_blocks = wire_rebuild(query, &metas, &batch_bodies)?;
    drop(batch_bodies);

    let prepared = assemble_run(
        query,
        params,
        milestone,
        metas,
        &wire_blocks,
        &batch_parents,
        &batch_thin,
    )?;
    // batch_thin only needed for assemble; drop before queue handoff.
    drop(batch_thin);

    let work_ns = t_work.elapsed().as_nanos() as u64;
    Ok(ConfirmLoadOutcome {
        batch: LoadedBatch {
            prepared,
            wire_blocks,
            batch_parents,
            script_preverified: preverified.clone(),
            archive_plan: None,
        },
        work_ns,
    })
}

/// PREP STAGE from **raw wire blocks** (unified height-ordered pipeline).
///
/// - Structure / PoW checks, ensure headers
/// - Plan Class A (assign create fks, stamp inputs) **without** committing
/// - Pin external parents once (denserels); same-batch from plan
/// - Assemble using **intake wire** (no Class-A wire rebuild)
///
/// The plan rides on [`LoadedBatch::archive_plan`] and is committed in write.
///
/// `pipeline`: when `Some`, first height may be ahead of store tip (prep(N+1)
/// while commit(N) in flight). Use reserved create-fk HWM + in-flight creates.
pub fn confirm_wire_prep_phase(
    query: &Query,
    params: &ChainParams,
    milestone: Milestone,
    blocks: &[(Height, Block)],
    preverified: &ScriptPreverified,
) -> Result<ConfirmLoadOutcome, ConsensusError> {
    confirm_wire_prep_phase_pipelined(query, params, milestone, blocks, preverified, None)
}

/// Like [`confirm_wire_prep_phase`] with optional pipeline caches for prep-ahead.
pub fn confirm_wire_prep_phase_pipelined(
    query: &Query,
    params: &ChainParams,
    milestone: Milestone,
    blocks: &[(Height, Block)],
    preverified: &ScriptPreverified,
    pipeline: Option<&WirePrepPipeline>,
) -> Result<ConfirmLoadOutcome, ConsensusError> {
    if blocks.is_empty() {
        return Err(ConsensusError::BadBlock("empty confirm batch"));
    }
    for w in blocks.windows(2) {
        if w[1].0 .0 != w[0].0 .0.saturating_add(1) {
            return Err(ConsensusError::BadBlock("confirm run not contiguous"));
        }
    }

    let t_work = Instant::now();
    let t_load = Instant::now();

    let mut with_fk: Vec<(
        rbitcoin_primitives::Fk,
        rbitcoin_store::HeaderRecord,
        Vec<rbitcoin_query::TxApply>,
    )> = Vec::with_capacity(blocks.len());
    let mut wire_blocks: Vec<Block> = Vec::with_capacity(blocks.len());
    let mut metas: Vec<BodyMeta> = Vec::with_capacity(blocks.len());

    let tip_h = query.tip_height().map(|h| h.0);
    let store_path_lo = match tip_h {
        None => 0u32,
        Some(t) => t.saturating_add(1),
    };
    let path_lo = pipeline.map(|p| p.path_lo).unwrap_or(store_path_lo);

    for (i, (height, block)) in blocks.iter().enumerate() {
        let hash = block.block_hash().to_byte_array();
        let ctx = ValidationContext::at(params, *height, milestone);
        let _ = crate::block::validate_block_structure_hashed(block, &ctx)?;
        // First height must sit at pipeline path_lo (store tip+1, or last prepped+1).
        // Later heights in the same batch validate against prior wire, not store tip.
        if i == 0 {
            if height.0 != path_lo {
                return Err(ConsensusError::BadPrev);
            }
            if path_lo == store_path_lo {
                // Extends confirmed tip: full header validation against store.
                validate_header(query, params, *height, &block.header)?;
            } else {
                // Ahead of tip: parent must match prior prepped batch (or explicit parent).
                let expect_prev = pipeline.and_then(|p| p.parent_hash).unwrap_or([0u8; 32]);
                if block.header.prev_blockhash.to_byte_array() != expect_prev {
                    return Err(ConsensusError::BadPrev);
                }
                let target = bitcoin::Target::from_compact(block.header.bits);
                if target > params.pow_limit {
                    return Err(ConsensusError::BadHeader("target above pow limit"));
                }
                block
                    .header
                    .validate_pow(target)
                    .map_err(|_| ConsensusError::InvalidPow)?;
            }
        } else {
            let prev = &blocks[i - 1].1;
            if block.header.prev_blockhash != prev.block_hash() {
                return Err(ConsensusError::BadPrev);
            }
            // PoW bits/target (no store retarget mid-batch for regtest).
            let target = bitcoin::Target::from_compact(block.header.bits);
            if target > params.pow_limit {
                return Err(ConsensusError::BadHeader("target above pow limit"));
            }
            block
                .header
                .validate_pow(target)
                .map_err(|_| ConsensusError::InvalidPow)?;
        }

        let (header_rec, txs) = crate::prepare_block_for_archive(query, params, block)?;
        let header_fk = if let Some((fk, _)) = query
            .get_header_by_hash(&header_rec.hash)
            .map_err(ConsensusError::Store)?
        {
            fk
        } else {
            query
                .store()
                .put_header(&header_rec)
                .map_err(ConsensusError::Store)?
        };
        query.confirm_parent_cache().put_header_plan(
            height.0,
            header_fk,
            header_rec.clone(),
            Vec::new(),
            block.header.prev_blockhash.to_byte_array(),
        );
        with_fk.push((header_fk, header_rec.clone(), txs));
        wire_blocks.push(block.clone());
        metas.push(BodyMeta {
            height: *height,
            hash,
            header_fk,
            header_rec,
            tx_fks: Vec::new(),
        });
    }

    let (_header_fks, mut need) = query
        .archive_filter_need_bodies(&mut with_fk)
        .map_err(ConsensusError::Store)?;
    let plan = if need.is_empty() {
        for m in &mut metas {
            if let Some(list) = query
                .store()
                .header_txs
                .get_list(m.header_fk)
                .map_err(ConsensusError::Store)?
            {
                m.tx_fks = list;
            }
            let prev = wire_blocks
                .iter()
                .find(|b| b.block_hash().to_byte_array() == m.hash)
                .map(|b| b.header.prev_blockhash.to_byte_array())
                .unwrap_or([0u8; 32]);
            query.confirm_parent_cache().put_header_plan(
                m.height.0,
                m.header_fk,
                m.header_rec.clone(),
                m.tx_fks.clone(),
                prev,
            );
        }
        None
    } else {
        let plan = match pipeline {
            Some(p) => query
                .archive_plan_mega_from(&mut need, p.next_tx_start.max(1), &p.in_flight_creates)
                .map_err(ConsensusError::Store)?,
            None => query
                .archive_plan_mega_owned(&mut need)
                .map_err(ConsensusError::Store)?,
        };
        let mut by_header: HashMap<u64, Vec<rbitcoin_primitives::Fk>> = HashMap::new();
        for &(hfk, first, n) in &plan.per_header_ranges {
            let Some(hid) = hfk.get() else { continue };
            let start = plan
                .planned_fks
                .iter()
                .position(|f| *f == first)
                .unwrap_or(0);
            let n = n as usize;
            let slice = plan.planned_fks[start..start.saturating_add(n).min(plan.planned_fks.len())]
                .to_vec();
            by_header.insert(hid, slice);
        }
        for m in &mut metas {
            if let Some(id) = m.header_fk.get() {
                if let Some(fks) = by_header.get(&id) {
                    m.tx_fks = fks.clone();
                }
            }
            if m.tx_fks.is_empty() {
                if let Some(list) = query
                    .store()
                    .header_txs
                    .get_list(m.header_fk)
                    .map_err(ConsensusError::Store)?
                {
                    m.tx_fks = list;
                }
            }
            let prev = wire_blocks
                .iter()
                .find(|b| b.block_hash().to_byte_array() == m.hash)
                .map(|b| b.header.prev_blockhash.to_byte_array())
                .unwrap_or([0u8; 32]);
            query.confirm_parent_cache().put_header_plan(
                m.height.0,
                m.header_fk,
                m.header_rec.clone(),
                m.tx_fks.clone(),
                prev,
            );
        }
        Some(plan)
    };

    let inflight_outs = pipeline.map(|p| &p.in_flight_outs);
    let (batch_parents, batch_thin) =
        pin_for_wire_batch(query, plan.as_ref(), &metas, &wire_blocks, inflight_outs)?;

    confirm_phase_stats::LOAD_NS.fetch_add(
        t_load.elapsed().as_nanos() as u64,
        Ordering::Relaxed,
    );

    let prepared = assemble_run(
        query,
        params,
        milestone,
        metas,
        &wire_blocks,
        &batch_parents,
        &batch_thin,
    )?;
    drop(batch_thin);

    let work_ns = t_work.elapsed().as_nanos() as u64;
    Ok(ConfirmLoadOutcome {
        batch: LoadedBatch {
            prepared,
            wire_blocks,
            batch_parents,
            script_preverified: preverified.clone(),
            archive_plan: plan,
        },
        work_ns,
    })
}

/// Unified wire → tip (prep + scripts + commit). Primary production entry.
pub fn confirm_wire_run(
    query: &Query,
    params: &ChainParams,
    milestone: Milestone,
    blocks: &[(Height, Block)],
) -> Result<Vec<rbitcoin_primitives::Fk>, ConsensusError> {
    confirm_wire_run_preverified(query, params, milestone, blocks, &ScriptPreverified::new())
}

/// Like [`confirm_wire_run`] with mempool script preverified set.
pub fn confirm_wire_run_preverified(
    query: &Query,
    params: &ChainParams,
    milestone: Milestone,
    blocks: &[(Height, Block)],
    preverified: &ScriptPreverified,
) -> Result<Vec<rbitcoin_primitives::Fk>, ConsensusError> {
    let mat = confirm_wire_prep_phase(query, params, milestone, blocks, preverified)?;
    let ok = confirm_scripts_phase(mat.batch)?;
    confirm_write_phase(query, params, milestone, ok.batch)
}

/// Pin parents for wire prep: same-batch from plan outs; prior pipeline plans;
/// external via denserels IO once.
fn pin_for_wire_batch(
    query: &Query,
    plan: Option<&rbitcoin_query::ArchiveWritePlan>,
    metas: &[BodyMeta],
    wire_blocks: &[Block],
    in_flight_outs: Option<
        &HashMap<u64, (rbitcoin_store::TxRecord, Vec<rbitcoin_store::OutputRecord>)>,
    >,
) -> Result<(rbitcoin_query::BatchParents, rbitcoin_query::BatchThin), ConsensusError> {
    use rbitcoin_query::ThinInput;
    use rbitcoin_store::IdxBodyMode;

    let mut batch_thin: rbitcoin_query::BatchThin = std::collections::HashMap::new();
    let mut parent_vouts: HashMap<u64, Vec<u32>> = HashMap::new();
    let mut batch_create: HashSet<u64> = HashSet::new();

    for m in metas {
        for fk in &m.tx_fks {
            if let Some(id) = fk.get() {
                batch_create.insert(id);
            }
        }
    }

    let mut planned_outs: HashMap<
        u64,
        (rbitcoin_store::TxRecord, Vec<rbitcoin_store::OutputRecord>),
    > = HashMap::new();
    if let Some(ifo) = in_flight_outs {
        for (id, v) in ifo {
            planned_outs.insert(*id, v.clone());
        }
    }
    if let Some(plan) = plan {
        for ((tx, _ins, outs), fk) in plan.packed.iter().zip(plan.planned_fks.iter()) {
            if let Some(id) = fk.get() {
                planned_outs.insert(id, (tx.clone(), outs.clone()));
            }
        }
        for ((tx, ins, _outs), fk) in plan.packed.iter().zip(plan.planned_fks.iter()) {
            let Some(sid) = fk.get() else { continue };
            let mut edges = Vec::with_capacity(ins.len());
            for inp in ins {
                if inp.is_coinbase() || inp.prev_index == u32::MAX {
                    edges.push(ThinInput {
                        create_fk: None,
                        prev_index: u32::MAX,
                    });
                    continue;
                }
                if let Some(pid) = inp.create_fk.get() {
                    edges.push(ThinInput {
                        create_fk: Some(pid),
                        prev_index: inp.prev_index,
                    });
                    if !batch_create.contains(&pid) {
                        parent_vouts.entry(pid).or_default().push(inp.prev_index);
                    }
                } else {
                    edges.push(ThinInput {
                        create_fk: None,
                        prev_index: inp.prev_index,
                    });
                }
            }
            batch_thin.insert(sid, edges);
            let _ = tx;
        }
    } else {
        for (m, block) in metas.iter().zip(wire_blocks.iter()) {
            for (ti, tx) in block.txdata.iter().enumerate() {
                let Some(sfk) = m.tx_fks.get(ti).and_then(|f| f.get()) else {
                    continue;
                };
                let mut edges = Vec::with_capacity(tx.input.len());
                for inp in &tx.input {
                    if inp.previous_output.is_null() {
                        edges.push(ThinInput {
                            create_fk: None,
                            prev_index: u32::MAX,
                        });
                        continue;
                    }
                    let prev_txid = inp.previous_output.txid.to_byte_array();
                    let vout = inp.previous_output.vout;
                    let cfk = query
                        .create_residency()
                        .lookup_fk_by_txid(&prev_txid)
                        .or_else(|| {
                            query
                                .store()
                                .get_fk_by_txid(&prev_txid)
                                .ok()
                                .flatten()
                        });
                    if let Some(fk) = cfk {
                        if let Some(pid) = fk.get() {
                            edges.push(ThinInput {
                                create_fk: Some(pid),
                                prev_index: vout,
                            });
                            if !batch_create.contains(&pid) {
                                parent_vouts.entry(pid).or_default().push(vout);
                            }
                            continue;
                        }
                    }
                    edges.push(ThinInput {
                        create_fk: None,
                        prev_index: vout,
                    });
                }
                batch_thin.insert(sfk, edges);
            }
        }
    }

    let mut batch_parents = rbitcoin_query::BatchParents::with_capacity(
        parent_vouts.len().saturating_add(batch_create.len()),
    );
    for id in &batch_create {
        if let Some((tx, outs)) = planned_outs.get(id) {
            let checked: Vec<u32> = (0..outs.len() as u32).collect();
            let live: Vec<(u32, rbitcoin_store::OutputRecord)> = outs
                .iter()
                .enumerate()
                .map(|(i, o)| (i as u32, o.clone()))
                .collect();
            batch_parents.insert_owned(
                rbitcoin_primitives::Fk(*id),
                tx.clone(),
                live,
                checked,
                if tx.input_count != 1 {
                    Some(false)
                } else {
                    None
                },
                None,
                Vec::new(),
            );
        }
    }

    // Prefer pipeline / same-batch planned outs for external parents before denserels IO.
    let mut still_need: HashMap<u64, Vec<u32>> = HashMap::new();
    for (id, need) in &parent_vouts {
        if batch_parents
            .get_parent_tx(rbitcoin_primitives::Fk(*id))
            .is_some()
        {
            continue;
        }
        if let Some((tx, outs)) = planned_outs.get(id) {
            let mut need = need.clone();
            need.sort_unstable();
            need.dedup();
            let live: Vec<(u32, rbitcoin_store::OutputRecord)> = need
                .iter()
                .filter_map(|&v| outs.get(v as usize).map(|o| (v, o.clone())))
                .collect();
            let cb = if tx.input_count != 1 {
                Some(false)
            } else {
                None
            };
            batch_parents.insert_owned(
                rbitcoin_primitives::Fk(*id),
                tx.clone(),
                live,
                need,
                cb,
                None,
                Vec::new(),
            );
        } else {
            still_need.insert(*id, need.clone());
        }
    }

    if !still_need.is_empty() {
        let fks: Vec<rbitcoin_primitives::Fk> = still_need
            .keys()
            .map(|id| rbitcoin_primitives::Fk(*id))
            .collect();
        let loaded = rbitcoin_query::load_creates_once(
            query.store(),
            query.create_residency(),
            &fks,
            IdxBodyMode::OutsDenserels,
        )
        .map_err(ConsensusError::Store)?;
        for c in loaded {
            let Some(id) = c.fk.get() else { continue };
            let need = still_need.get(&id).cloned().unwrap_or_default();
            let Ok((tx, outs, dense_rels)) =
                rbitcoin_store::decode_packed_tx_outs_with_spender_rels_secret(
                    &c.raw,
                    Some(query.store().txs.store_secret()),
                )
            else {
                continue;
            };
            let mut need = need;
            need.sort_unstable();
            need.dedup();
            let live: Vec<(u32, rbitcoin_store::OutputRecord)> = need
                .iter()
                .filter_map(|&v| outs.get(v as usize).map(|o| (v, o.clone())))
                .collect();
            let sparse = rbitcoin_query::sparse_spender_rels(&dense_rels, &need);
            let cb = if tx.input_count != 1 {
                Some(false)
            } else {
                None
            };
            batch_parents.insert_owned(
                c.fk,
                tx,
                live,
                need,
                cb,
                Some(c.body_range),
                sparse,
            );
        }
    }

    Ok((batch_parents, batch_thin))
}

/// SCRIPTS STAGE: pure verification of jobs already assembled at prep/load.
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
    script_wave(&batch.prepared, &batch.script_preverified)?;
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
            archive_plan: batch.archive_plan,
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

/// Keep heights not yet on the confirmed tip (dup write race).
///
/// `tip == None` means empty chain — genesis (and any load batch) still needs write.
#[inline]
fn write_height_needed(tip: Option<u32>, height: u32) -> bool {
    match tip {
        None => true,
        Some(t) => height > t,
    }
}

/// COMMIT STAGE: optional Class A plan commit → structural → class_c → spend annotate → tip GC.
///
/// When `batch.archive_plan` is set (wire prep path), Class A is appended in this
/// same stage before structural/annotate — single ordered commit era, not a
/// far-ahead archive track.
///
/// Accrues window timers in [`confirm_phase_stats`] and snapshots the last batch
/// for slow-write logs via [`confirm_phase_stats::last_write_phases`].
pub fn confirm_write_phase(
    query: &Query,
    params: &ChainParams,
    milestone: Milestone,
    mut batch: ScriptOkBatch,
) -> Result<Vec<rbitcoin_primitives::Fk>, ConsensusError> {
    // Idempotent: skip heights already on the confirmed tip (dup pipeline race).
    let tip = query.tip_height().map(|h| h.0);
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

    let t_wall = Instant::now();

    // Single commit era: durable Class A for this batch before spentness RMW.
    if let Some(plan) = batch.archive_plan.take() {
        if !plan.is_empty() {
            // Snapshot planned fks + packed outs for offline denserels (no Class-A re-read).
            let planned_fks = plan.planned_fks.clone();
            let packed = plan
                .packed
                .iter()
                .map(|(tx, ins, outs)| (tx.clone(), ins.clone(), outs.clone()))
                .collect::<Vec<_>>();
            query
                .archive_commit_plan(plan)
                .map_err(ConsensusError::Store)?;
            // Layout only for planned creates (same-batch): encode offline +
            // body ranges from commit — zero additional Class A body preads.
            fill_planned_create_layout_after_commit(
                query,
                &mut batch.batch_parents,
                &planned_fks,
                &packed,
            )?;
        }
    }

    // Local Instant totals (not atomic deltas) — sample_and_reset races mid-batch.
    let t_struct = Instant::now();
    let struct_ph = structural_run(
        query,
        params,
        milestone,
        &batch.prepared,
        &batch.wire_blocks,
        &batch.batch_parents,
    )?;
    let structural_ns = t_struct.elapsed().as_nanos() as u64;

    let n_blocks = batch.prepared.len();
    let t_cc = Instant::now();
    let out = class_c_commit(query, &mut batch.prepared)?;
    let class_c_ns = t_cc.elapsed().as_nanos() as u64;

    let (spend_ann_ns, tip_gc_ns) =
        post_commit(query, &batch.prepared, &batch.batch_parents)?;

    // batch_parents dropped here with ScriptOkBatch — no tip GC of sparse pins.
    confirm_phase_stats::BLOCKS.fetch_add(n_blocks as u64, Ordering::Relaxed);
    confirm_phase_stats::note_last_write(confirm_phase_stats::LastWritePhases {
        n_blocks: n_blocks as u32,
        wall_ns: t_wall.elapsed().as_nanos() as u64,
        structural_ns,
        spent_ns: struct_ph.spent_ns,
        create_h_ns: struct_ph.create_h_ns,
        bip68_ns: struct_ph.bip68_ns,
        class_c_ns,
        spend_ann_ns,
        tip_gc_ns,
    });
    Ok(out)
}

/// After Class A commit, set body_range + denserels for **planned** creates only.
///
/// Uses offline pack (same encoding as disk) + `tx_body_range_batch` — **no**
/// Class A body pread / `load_creates_once`. External parents were already
/// pinned with denserels at prep.
fn fill_planned_create_layout_after_commit(
    query: &Query,
    batch_parents: &mut rbitcoin_query::BatchParents,
    planned_fks: &[rbitcoin_primitives::Fk],
    packed: &[(
        rbitcoin_store::TxRecord,
        Vec<rbitcoin_store::InputRecord>,
        Vec<rbitcoin_store::OutputRecord>,
    )],
) -> Result<(), ConsensusError> {
    if planned_fks.is_empty() || packed.is_empty() {
        return Ok(());
    }
    let ranges = query
        .store()
        .tx_body_range_batch(planned_fks)
        .map_err(ConsensusError::Store)?;
    let secret = query.store().txs.store_secret();
    for ((tx, ins, outs), (fk, range)) in packed
        .iter()
        .zip(planned_fks.iter().zip(ranges.into_iter()))
    {
        let Some((off, len)) = range else {
            continue;
        };
        // Offline pack matches durable body layout → denserels without disk read.
        let mut raw = Vec::new();
        rbitcoin_store::encode_packed_tx_with_secret(tx, ins, outs, &mut raw, Some(secret));
        let Ok((_meta, _outs, dense_rels)) =
            rbitcoin_store::decode_packed_tx_outs_with_spender_rels_secret(&raw, Some(secret))
        else {
            continue;
        };
        batch_parents.set_layout(*fk, (off, len), &dense_rels);
    }
    Ok(())
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

    /// Approx wire bytes retained in `wire_blocks` (for queue-content size logs).
    pub fn approx_wire_bytes(&self) -> usize {
        self.wire_blocks.iter().map(|b| b.total_size()).sum()
    }

    /// Sparse parent pin map entries riding this batch.
    pub fn parent_count(&self) -> usize {
        self.batch_parents.len()
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

    /// Approx wire bytes retained in `wire_blocks` (for queue-content size logs).
    pub fn approx_wire_bytes(&self) -> usize {
        self.wire_blocks.iter().map(|b| b.total_size()).sum()
    }

    /// Sparse parent pin map entries riding this batch.
    pub fn parent_count(&self) -> usize {
        self.batch_parents.len()
    }
}

#[cfg(test)]
mod write_idempotent_tests {
    use super::write_height_needed;

    /// Heights at or below tip must be stripped before structural write
    /// (dup pipeline race after scripts claim the same tip+1 twice).
    /// Write filter + stage entry points + empty scripts purity (one surface).
    /// External three-stage path: rbitcoin-test three_stage_confirm_and_parent_pin_surface.
    #[test]
    fn three_stage_write_filter_and_scripts_surface() {
        let tip = Some(100u32);
        let heights = [98u32, 99, 100, 101, 102];
        let kept: Vec<u32> = heights
            .into_iter()
            .filter(|&h| write_height_needed(tip, h))
            .collect();
        assert_eq!(kept, vec![101, 102]);
        assert!(!write_height_needed(tip, 100));
        assert!(!write_height_needed(Some(0), 0));
        assert!(write_height_needed(Some(0), 1));
        // Empty chain: genesis (and all heights) still need write.
        assert!(write_height_needed(None, 0));
        assert!(write_height_needed(None, 1));

        // Load / scripts / write are separate public surfaces for IBD.
        let _m = super::confirm_load_phase;
        let _s = super::confirm_scripts_phase;
        let _w = super::confirm_write_phase;
        let _combined = super::confirm_script_phase;
        let _sync = super::confirm_archived_run;

        use super::{confirm_scripts_phase, LoadedBatch, ScriptPreverified};
        let batch = LoadedBatch {
            prepared: Vec::new(),
            wire_blocks: Vec::new(),
            batch_parents: rbitcoin_query::BatchParents::new(),
            script_preverified: ScriptPreverified::new(),
            archive_plan: None,
        };
        assert!(batch.is_empty());
        assert_eq!(batch.approx_wire_bytes(), 0);
        assert_eq!(batch.parent_count(), 0);
        let ok = confirm_scripts_phase(batch).expect("empty scripts ok");
        assert!(ok.batch.prepared.is_empty());
        assert!(ok.batch.wire_blocks.is_empty());
    }

    #[test]
    fn check_bip34_helper_and_expected_bits_no_retarget() {
        use super::{check_bip34, expected_bits_extending};
        use bitcoin::absolute::LockTime;
        use bitcoin::block::{Header, Version};
        use bitcoin::hashes::Hash;
        use bitcoin::script::ScriptBuf;
        use bitcoin::{
            Amount, Block, BlockHash, CompactTarget, OutPoint, Sequence, Transaction, TxIn, TxOut,
            TxMerkleNode, Witness,
        };
        use crate::params::ChainParams;
        use rbitcoin_primitives::Height;

        let height = 17u32;
        let mut ss = crate::block::bip34_height_script(height);
        while ss.len() < 2 {
            ss.push(0x00);
        }
        let cb = Transaction {
            version: bitcoin::transaction::Version::ONE,
            lock_time: LockTime::ZERO,
            input: vec![TxIn {
                previous_output: OutPoint::null(),
                script_sig: ScriptBuf::from_bytes(ss),
                sequence: Sequence::MAX,
                witness: Witness::new(),
            }],
            output: vec![TxOut {
                value: Amount::from_sat(50),
                script_pubkey: ScriptBuf::from_bytes(vec![0x51]),
            }],
        };
        let block = Block {
            header: Header {
                version: Version::ONE,
                prev_blockhash: BlockHash::from_byte_array([0; 32]),
                merkle_root: TxMerkleNode::from_byte_array([0; 32]),
                time: 1,
                bits: CompactTarget::from_consensus(0x207f_ffff),
                nonce: 0,
            },
            txdata: vec![cb],
        };
        check_bip34(&block, height).unwrap();
        // Wrong height
        assert!(check_bip34(&block, height + 1).is_err());

        // expected_bits_extending without store: height 0 and no_pow_retargeting regtest
        let params = ChainParams::regtest();
        // Cannot call with query easily; unit-test height==0 via expected_bits requires Query.
        // Cover pure branch: no_pow or non-interval uses prev_bits — needs Query only for retarget.
        let _ = (params, expected_bits_extending);
        let _ = Height;
    }

    #[test]
    fn empty_confirm_batch_rejected() {
        // confirm_load_phase empty → BadBlock without store open
        // We only have Query API; use a throwaway path under /tmp when available.
        use super::confirm_load_phase;
        use crate::milestone::Milestone;
        use crate::params::ChainParams;
        use rbitcoin_primitives::Height;
        use rbitcoin_query::Query;
        use std::sync::Once;

        static ONCE: Once = Once::new();
        ONCE.call_once(|| {
            if std::env::var_os("RBITCOIN_HEAD_SCALE").is_none() {
                std::env::set_var("RBITCOIN_HEAD_SCALE", "tiny");
            }
        });
        let path = std::env::temp_dir().join(format!(
            "rbitcoin-confirm-empty-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0)
        ));
        std::fs::create_dir_all(&path).unwrap();
        let q = Query::open_or_create(&path).unwrap();
        let params = ChainParams::regtest();
        let err = match confirm_load_phase(&q, &params, Milestone::NONE, &[]) {
            Ok(_) => panic!("expected empty batch error"),
            Err(e) => e,
        };
        assert!(matches!(err, crate::error::ConsensusError::BadBlock(_)));
        // Non-contiguous
        let err2 = match confirm_load_phase(
            &q,
            &params,
            Milestone::NONE,
            &[(Height(1), [1u8; 32]), (Height(3), [2u8; 32])],
        ) {
            Ok(_) => panic!("expected non-contiguous error"),
            Err(e) => e,
        };
        assert!(matches!(err2, crate::error::ConsensusError::BadBlock(_)));
        let _ = std::fs::remove_dir_all(&path);
    }

    /// load_confirm_batch empty + resolve_body_metas store fallback (no plan).
    #[test]
    fn load_batch_empty_and_resolve_metas_fallback() {
        use super::{load_confirm_batch, resolve_body_metas};
        use crate::accept_and_connect_block;
        use crate::milestone::Milestone;
        use crate::params::ChainParams;
        use rbitcoin_primitives::Height;
        use rbitcoin_query::Query;
        use std::sync::Once;

        static ONCE: Once = Once::new();
        ONCE.call_once(|| {
            if std::env::var_os("RBITCOIN_HEAD_SCALE").is_none() {
                std::env::set_var("RBITCOIN_HEAD_SCALE", "tiny");
            }
        });
        let path = std::env::temp_dir().join(format!(
            "rbitcoin-confirm-loadbatch-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0)
        ));
        std::fs::create_dir_all(&path).unwrap();
        let q = Query::open_or_create(&path).unwrap();
        let (bp, bt, bb) = load_confirm_batch(&q, &[], &[], 0).unwrap();
        let _ = (bp, bt, bb);

        let params = ChainParams::regtest();
        let genesis = bitcoin::blockdata::constants::genesis_block(bitcoin::Network::Regtest);
        accept_and_connect_block(&q, &params, Height::GENESIS, &genesis, Milestone::NONE)
            .unwrap();
        use bitcoin::hashes::Hash;
        let hash = genesis.block_hash().to_byte_array();
        // No header plan in cache → store fallback in resolve_body_metas
        let metas = resolve_body_metas(&q, &[(Height::GENESIS, hash)]).unwrap();
        assert_eq!(metas.len(), 1);
        assert_eq!(metas[0].hash, hash);
        // Missing hash → NotFound
        assert!(resolve_body_metas(&q, &[(Height(9), [0xee; 32])]).is_err());
        let _ = std::fs::remove_dir_all(&path);
    }

    #[test]
    fn expected_bits_extending_height0_and_no_retarget() {
        use super::expected_bits_extending;
        use crate::params::ChainParams;
        use bitcoin::CompactTarget;
        use rbitcoin_primitives::Height;
        use rbitcoin_query::Query;
        use std::sync::Once;

        static ONCE: Once = Once::new();
        ONCE.call_once(|| {
            if std::env::var_os("RBITCOIN_HEAD_SCALE").is_none() {
                std::env::set_var("RBITCOIN_HEAD_SCALE", "tiny");
            }
        });
        let path = std::env::temp_dir().join(format!(
            "rbitcoin-confirm-bits-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0)
        ));
        std::fs::create_dir_all(&path).unwrap();
        let q = Query::open_or_create(&path).unwrap();
        let params = ChainParams::regtest();
        let gbits = expected_bits_extending(
            &q,
            &params,
            Height(0),
            CompactTarget::from_consensus(0),
            0,
        )
        .unwrap();
        assert_eq!(
            gbits,
            crate::params::genesis_block(&params).header.bits
        );
        // No-pow-retargeting: any height returns prev_bits.
        let prev = CompactTarget::from_consensus(0x207f_ffff);
        let b = expected_bits_extending(&q, &params, Height(2016), prev, 100).unwrap();
        assert_eq!(b, prev);

        // ScriptOkBatch empty surfaces (mirror LoadedBatch).
        use super::{confirm_scripts_phase, LoadedBatch, ScriptPreverified};
        let loaded = LoadedBatch {
            prepared: Vec::new(),
            wire_blocks: Vec::new(),
            batch_parents: rbitcoin_query::BatchParents::new(),
            script_preverified: ScriptPreverified::new(),
            archive_plan: None,
        };
        let ok = confirm_scripts_phase(loaded).unwrap();
        assert!(ok.batch.is_empty());
        assert_eq!(ok.batch.len(), 0);
        assert!(ok.batch.heights_hashes().is_empty());
        assert_eq!(ok.batch.approx_wire_bytes(), 0);
        assert_eq!(ok.batch.parent_count(), 0);

        // check_bip34 wrong encoding
        use super::check_bip34;
        use bitcoin::absolute::LockTime;
        use bitcoin::block::{Header, Version};
        use bitcoin::hashes::Hash;
        use bitcoin::script::ScriptBuf;
        use bitcoin::{
            Amount, Block, BlockHash, OutPoint, Sequence, Transaction, TxIn, TxOut, TxMerkleNode,
            Witness,
        };
        let cb = Transaction {
            version: bitcoin::transaction::Version::ONE,
            lock_time: LockTime::ZERO,
            input: vec![TxIn {
                previous_output: OutPoint::null(),
                script_sig: ScriptBuf::from_bytes(vec![0x01, 0x99]),
                sequence: Sequence::MAX,
                witness: Witness::new(),
            }],
            output: vec![TxOut {
                value: Amount::from_sat(1),
                script_pubkey: ScriptBuf::from_bytes(vec![0x51]),
            }],
        };
        let block = Block {
            header: Header {
                version: Version::ONE,
                prev_blockhash: BlockHash::from_byte_array([0; 32]),
                merkle_root: TxMerkleNode::from_byte_array([0; 32]),
                time: 1,
                bits: CompactTarget::from_consensus(0x207f_ffff),
                nonce: 0,
            },
            txdata: vec![cb],
        };
        assert!(check_bip34(&block, 17).is_err());

        let _ = std::fs::remove_dir_all(&path);
    }

    /// Mempool-preverified txids skip script_wave verify (tip follow).
    #[test]
    fn script_wave_skips_preverified_txids() {
        use super::{confirm_scripts_phase, LoadedBatch, Prepared, ScriptPreverified};
        use crate::block::ScriptCheckJob;
        use crate::confirm_phase_stats;
        use bitcoin::absolute::LockTime;
        use bitcoin::hashes::Hash;
        use bitcoin::script::ScriptBuf;
        use bitcoin::transaction::Version as TxVersion;
        use bitcoin::{
            Amount, CompactTarget, OutPoint, Sequence, Transaction, TxIn, TxOut, Witness,
        };
        use rbitcoin_primitives::{Fk, Height};
        use std::sync::atomic::Ordering;

        let prevouts = vec![TxOut {
            value: Amount::from_sat(50_0000_0000),
            // P2PKH-shaped (not anyone-can-spend) so job_needs_script_check is true
            // if we did not skip — invalid empty script_sig would fail without skip.
            script_pubkey: ScriptBuf::from_bytes(vec![
                0x76, 0xa9, 0x14, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0x88,
                0xac,
            ]),
        }];
        let tx = Transaction {
            version: TxVersion::TWO,
            lock_time: LockTime::ZERO,
            input: vec![TxIn {
                previous_output: OutPoint {
                    txid: bitcoin::Txid::from_byte_array([9; 32]),
                    vout: 0,
                },
                script_sig: ScriptBuf::new(),
                sequence: Sequence::MAX,
                witness: Witness::new(),
            }],
            output: vec![TxOut {
                value: Amount::from_sat(49_0000_0000),
                script_pubkey: ScriptBuf::from_bytes(vec![0x51]),
            }],
        };
        let tid = tx.compute_txid().to_byte_array();
        let mut pre = ScriptPreverified::new();
        pre.insert(tid);

        let job = ScriptCheckJob {
            prevouts,
            tx,
            bip65_active: true,
            bip112_active: true,
            bip66_active: true,
            bip16_active: true,
            taproot_active: true,
        };
        let prepared = Prepared {
            height: Height(1),
            header_fk: Fk(1),
            tx_fks: vec![Fk(1)],
            jobs: vec![job],
            spends: vec![],
            fees: 0,
            check_scripts: true,
            time: 1,
            bits: CompactTarget::from_consensus(0x207f_ffff),
            hash: [1u8; 32],
        };
        let batch = LoadedBatch {
            prepared: vec![prepared],
            wire_blocks: vec![],
            batch_parents: rbitcoin_query::BatchParents::new(),
            script_preverified: pre,
            archive_plan: None,
        };
        let before = confirm_phase_stats::SCRIPT_SKIP_MEMPOOL.load(Ordering::Relaxed);
        confirm_scripts_phase(batch).expect("preverified skip avoids bad script fail");
        let after = confirm_phase_stats::SCRIPT_SKIP_MEMPOOL.load(Ordering::Relaxed);
        assert!(after > before, "skip counter should bump");
    }

    /// Prep miss: spend edges without pin denserels must hard-fail (no cold tier).
    #[test]
    fn post_commit_missing_denserels_is_invariant_error() {
        use super::{post_commit, Prepared};
        use crate::milestone::Milestone;
        use crate::params::ChainParams;
        use rbitcoin_primitives::{Fk, Height};
        use rbitcoin_query::{BatchParents, Query};
        use std::sync::Once;

        static ONCE: Once = Once::new();
        ONCE.call_once(|| {
            if std::env::var_os("RBITCOIN_HEAD_SCALE").is_none() {
                std::env::set_var("RBITCOIN_HEAD_SCALE", "tiny");
            }
        });
        let path = std::env::temp_dir().join(format!(
            "rbitcoin-post-commit-inv-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0)
        ));
        std::fs::create_dir_all(&path).unwrap();
        let q = Query::open_or_create(&path).unwrap();
        q.enter_direct_index_mode().unwrap();
        // Spend index on (default for Direct) so post_commit enters annotate.
        let _ = (ChainParams::regtest(), Milestone::NONE);

        let prepared = [Prepared {
            height: Height(1),
            header_fk: Fk(1),
            tx_fks: vec![Fk(10)],
            jobs: vec![],
            spends: vec![([1u8; 32], 0, Fk(10), Fk(2))],
            fees: 0,
            check_scripts: false,
            time: 1,
            bits: bitcoin::CompactTarget::from_consensus(0x207f_ffff),
            hash: [2u8; 32],
        }];
        // Empty BatchParents → get_spender_abs is None.
        let bp = BatchParents::new();
        let err = post_commit(&q, &prepared, &bp).expect_err("missing denserels");
        let msg = format!("{err}");
        assert!(
            msg.contains("invariant") && msg.contains("denserels"),
            "unexpected err: {msg}"
        );
        let _ = std::fs::remove_dir_all(&path);
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
) -> Result<crate::block::StructuralPhaseNs, ConsensusError> {
    use crate::block::StructuralPhaseNs;
    let t0 = Instant::now();
    let mut pending_spent: HashSet<([u8; 32], u32)> = HashSet::new();
    // MTP of height H reused across blocks/spends in this write run.
    let mut mtp_cache: HashMap<u32, u32> = HashMap::new();
    let mut tot = StructuralPhaseNs::default();
    for (i, p) in prepared.iter().enumerate() {
        let ctx = ValidationContext::at(params, p.height, milestone);
        let ph = structural_validate_spends(
            query,
            &wire_blocks[i],
            &ctx,
            Some(&p.tx_fks),
            &p.spends,
            p.fees,
            &mut pending_spent,
            batch_parents,
            &mut mtp_cache,
        )?;
        tot.spent_ns = tot.spent_ns.saturating_add(ph.spent_ns);
        tot.create_h_ns = tot.create_h_ns.saturating_add(ph.create_h_ns);
        tot.bip68_ns = tot.bip68_ns.saturating_add(ph.bip68_ns);
    }
    // Window counters (may race with sampler; last-write uses `tot` instead).
    confirm_phase_stats::STRUCTURAL_NS
        .fetch_add(t0.elapsed().as_nanos() as u64, Ordering::Relaxed);
    confirm_phase_stats::STRUCTURAL_SPENT_NS.fetch_add(tot.spent_ns, Ordering::Relaxed);
    confirm_phase_stats::STRUCTURAL_CREATE_H_NS
        .fetch_add(tot.create_h_ns, Ordering::Relaxed);
    confirm_phase_stats::STRUCTURAL_BIP68_NS.fetch_add(tot.bip68_ns, Ordering::Relaxed);
    Ok(tot)
}

/// Verify script jobs in `prepared` (CPU only). Skips jobs whose txid is in
/// `preverified` (mempool already consensus-checked at accept).
fn script_wave(
    prepared: &[Prepared],
    preverified: &ScriptPreverified,
) -> Result<(), ConsensusError> {
    use bitcoin::hashes::Hash;
    let t_script = Instant::now();
    let mut all_jobs: Vec<&ScriptCheckJob> = Vec::new();
    let mut n_skip = 0u64;
    for p in prepared {
        if !p.check_scripts {
            continue;
        }
        for job in &p.jobs {
            let tid = job.tx.compute_txid().to_byte_array();
            if preverified.contains(&tid) {
                n_skip = n_skip.saturating_add(1);
                continue;
            }
            all_jobs.push(job);
        }
    }
    if n_skip > 0 {
        confirm_phase_stats::SCRIPT_SKIP_MEMPOOL.fetch_add(n_skip, Ordering::Relaxed);
    }
    if !all_jobs.is_empty() {
        crate::block::verify_scripts_pool_jobs(&all_jobs)?;
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

/// Returns `(spend_ann_ns, tip_gc_ns)` measured with local `Instant`s.
fn post_commit(
    query: &Query,
    prepared: &[Prepared],
    batch_parents: &rbitcoin_query::BatchParents,
) -> Result<(u64, u64), ConsensusError> {
    // Confirm write (IBD + tip via accept_and_connect → confirm_archived_run):
    // batch durable spend annotations after Class C. Load pin must supply
    // denserels + body_range so every edge has abs layout — one path only.
    let t_spent = Instant::now();
    if query.spend_index_enabled() && query.index_mode().uses_durable_spends() {
        // Abs-only: no ranged/by_create cold tiers.
        let mut abs_edges: Vec<(
            u64,
            rbitcoin_primitives::Fk,
            u32,
            rbitcoin_primitives::Fk,
        )> = Vec::new();
        let mut n_skip = 0u64;
        for p in prepared {
            for &(_txid, vout, sfk, cfk) in &p.spends {
                if sfk.is_null() || cfk.is_null() {
                    n_skip = n_skip.saturating_add(1);
                    continue;
                }
                let Some(abs) = batch_parents.get_spender_abs(cfk, vout) else {
                    return Err(ConsensusError::Store(StoreError::Corrupt(
                        "invariant: spend annotate missing pin denserels/abs",
                    )));
                };
                abs_edges.push((abs, cfk, vout, sfk));
            }
        }
        confirm_phase_stats::SPEND_ANNOTATE_SKIP.fetch_add(n_skip, Ordering::Relaxed);
        if !abs_edges.is_empty() {
            let cold = query
                .store()
                .put_spend_batch_by_abs_meta(&abs_edges)
                .map_err(ConsensusError::Store)?;
            if !cold.is_empty() {
                return Err(ConsensusError::Store(StoreError::Corrupt(
                    "invariant: spend annotate abs cold (OOB or IO); prep/layout bug",
                )));
            }
            confirm_phase_stats::SPEND_ANNOTATE_RANGED
                .fetch_add(abs_edges.len() as u64, Ordering::Relaxed);
        }
        // Cold tiers removed: SPEND_ANNOTATE_IDX stays 0 on healthy Direct IBD.
    }
    let spend_ann_ns = t_spent.elapsed().as_nanos() as u64;
    confirm_phase_stats::UTXO_APPLY_NS.fetch_add(spend_ann_ns, Ordering::Relaxed);

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
    let mut tip_gc_ns = 0u64;
    if let Some(tip) = prepared.last().map(|p| p.height.0) {
        let t_tip = Instant::now();
        query.advance_parent_cache_tip(tip);
        tip_gc_ns = t_tip.elapsed().as_nanos() as u64;
        confirm_phase_stats::CACHE_TIP_NS.fetch_add(tip_gc_ns, Ordering::Relaxed);
    }
    Ok((spend_ann_ns, tip_gc_ns))
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
