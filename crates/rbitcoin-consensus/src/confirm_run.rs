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
use rbitcoin_store::{SpendAnnBackend, StoreError};
use std::collections::{HashMap, HashSet};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Instant;

/// Pure-write annotate backend from `RBITCOIN_SPEND_ANN` / global `RBITCOIN_IO`.
#[inline]
fn spend_ann_backend_next() -> SpendAnnBackend {
    rbitcoin_store::spend_annotate_uring::spend_ann_backend()
}

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
    /// Prior uncommitted plans: create txid → fk (shared; clone is Arc bump only).
    pub in_flight_creates: std::sync::Arc<HashMap<[u8; 32], rbitcoin_primitives::Fk>>,
    /// Prior uncommitted plans: create fk id → Arc pin material
    /// `(tx meta, outs, denserels)`.
    ///
    /// `denserels` is offline-packed (same as disk) so prep(N+1) can pin abs
    /// layout without waiting for commit(N) body IO; body_range is filled at write.
    /// Values are `Arc` so plan-thread `note_plan_ok` only bumps refcounts.
    pub in_flight_outs: std::sync::Arc<
        HashMap<u64, std::sync::Arc<(
            rbitcoin_store::TxRecord,
            Vec<rbitcoin_store::OutputRecord>,
            Vec<u32>,
        )>>,
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
    /// Shared wire (Arc) so prep→scripts→write does not deep-clone full blocks.
    wire_blocks: Vec<Arc<Block>>,
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
    wire_blocks: Vec<Arc<Block>>,
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
    //   • pin denserels live in CreateResidency (not on the batch)
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
    confirm_wire_prep_phase_pipelined(
        query,
        params,
        milestone,
        blocks,
        preverified,
        None,
        ColdPinMode::Allow,
    )
}

/// Like [`confirm_wire_prep_phase`] with optional pipeline caches for prep-ahead.
///
/// `cold_mode`: IBD prep after denserels stage uses [`ColdPinMode::Forbid`].
pub fn confirm_wire_prep_phase_pipelined(
    query: &Query,
    params: &ChainParams,
    milestone: Milestone,
    blocks: &[(Height, Block)],
    preverified: &ScriptPreverified,
    pipeline: Option<&WirePrepPipeline>,
    cold_mode: ColdPinMode,
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
    let mut ns_wire_arc = 0u64;
    let mut ns_struct = 0u64;
    let mut ns_header = 0u64;
    let mut ns_prepare = 0u64;

    let mut with_fk: Vec<(
        rbitcoin_primitives::Fk,
        rbitcoin_store::HeaderRecord,
        Vec<rbitcoin_query::TxApply>,
    )> = Vec::with_capacity(blocks.len());
    let mut wire_blocks: Vec<Arc<Block>> = Vec::with_capacity(blocks.len());
    let mut metas: Vec<BodyMeta> = Vec::with_capacity(blocks.len());

    let tip_h = query.tip_height().map(|h| h.0);
    let store_path_lo = match tip_h {
        None => 0u32,
        Some(t) => t.saturating_add(1),
    };
    let path_lo = pipeline.map(|p| p.path_lo).unwrap_or(store_path_lo);

    for (i, (height, block)) in blocks.iter().enumerate() {
        // One owned clone into Arc; later pipeline stages only bump the refcount.
        let t = Instant::now();
        let block = Arc::new(block.clone());
        ns_wire_arc = ns_wire_arc.saturating_add(t.elapsed().as_nanos() as u64);
        let hash = block.block_hash().to_byte_array();
        let ctx = ValidationContext::at(params, *height, milestone);
        let t = Instant::now();
        let _ = crate::block::validate_block_structure_hashed(block.as_ref(), &ctx)?;
        ns_struct = ns_struct.saturating_add(t.elapsed().as_nanos() as u64);
        // First height must sit at pipeline path_lo (store tip+1, or last prepped+1).
        // Later heights in the same batch validate against prior wire, not store tip.
        let t = Instant::now();
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
            let prev = &wire_blocks[i - 1];
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
        ns_header = ns_header.saturating_add(t.elapsed().as_nanos() as u64);

        let t = Instant::now();
        let (header_rec, txs) =
            crate::prepare_block_for_archive(query, params, block.as_ref())?;
        ns_prepare = ns_prepare.saturating_add(t.elapsed().as_nanos() as u64);
        let t = Instant::now();
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
        ns_header = ns_header.saturating_add(t.elapsed().as_nanos() as u64);
        with_fk.push((header_fk, header_rec.clone(), txs));
        wire_blocks.push(block);
        metas.push(BodyMeta {
            height: *height,
            hash,
            header_fk,
            header_rec,
            tx_fks: Vec::new(),
        });
    }

    let t_fp = Instant::now();
    let (_header_fks, mut need) = query
        .archive_filter_need_bodies(&mut with_fk)
        .map_err(ConsensusError::Store)?;
    let mut plan = if need.is_empty() {
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
                .archive_plan_mega_from(
                    &mut need,
                    p.next_tx_start.max(1),
                    p.in_flight_creates.as_ref(),
                )
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
    let ns_filter_plan = t_fp.elapsed().as_nanos() as u64;

    let inflight_outs = pipeline.map(|p| p.in_flight_outs.as_ref());
    let (batch_parents, batch_thin, _warm) = pin_for_wire_batch(
        query,
        plan.as_ref(),
        &metas,
        &wire_blocks,
        inflight_outs,
        cold_mode,
    )?;
    // Drop pipeline-local external full-outs after pin (sparse BatchParents remains).
    if let Some(ref mut p) = plan {
        p.clear_external_parent_outs();
    }

    confirm_phase_stats::LOAD_NS.fetch_add(
        t_load.elapsed().as_nanos() as u64,
        Ordering::Relaxed,
    );
    if ns_wire_arc > 0 {
        confirm_phase_stats::PREP_WIRE_ARC_NS.fetch_add(ns_wire_arc, Ordering::Relaxed);
    }
    if ns_struct > 0 {
        confirm_phase_stats::PREP_STRUCT_NS.fetch_add(ns_struct, Ordering::Relaxed);
    }
    if ns_header > 0 {
        confirm_phase_stats::PREP_HEADER_NS.fetch_add(ns_header, Ordering::Relaxed);
    }
    if ns_prepare > 0 {
        confirm_phase_stats::PREP_PREPARE_NS.fetch_add(ns_prepare, Ordering::Relaxed);
    }
    if ns_filter_plan > 0 {
        confirm_phase_stats::PREP_FILTER_PLAN_NS.fetch_add(ns_filter_plan, Ordering::Relaxed);
    }

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

/// Whether wire pin may cold-load denserels from Class A body.
///
/// IBD **plan** stage ensures external-parent denserels into **plan-local**
/// state (and residency **hits** for prior pipeline creates). Prep then uses
/// [`ColdPinMode::Forbid`] so cold denserels is never duplicated on the prep
/// thread. Tests / one-shot [`confirm_wire_prep_phase`] use [`ColdPinMode::Allow`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColdPinMode {
    /// Plan-local / residency miss → `load_creates_once` cold denserels (tests / Allow pin).
    Allow,
    /// Miss after plan ensure → hard `invariant: denserels stage miss` (prep after plan).
    Forbid,
}

/// Stats from plan-stage denserels ensure (external parents → plan-local).
#[derive(Debug, Default, Clone, Copy)]
pub struct DenserelsWarmStats {
    /// Unique external parent creates considered (stamped create_fk, not same-batch).
    pub parents: u32,
    /// Already had denserels in CreateResidency, plan.external_parent_outs, or in-flight.
    pub already: u32,
    /// Cold denserels body loads (into plan-local only — **not** residency).
    pub cold: u32,
    /// Same-batch plan creates (offline denserels at pin — no residency load).
    pub same_batch: u32,
    pub work_ns: u64,
}

/// External parents only: residency **read** (prior pipeline creates) or one
/// `OutsDenserels` cold load into **`plan.external_parent_outs`** (never residency).
///
/// Parent create_fks come from **plan-stamped** inputs (and in-flight). No head
/// resolve here — plan already stamped via batch head + residency caches.
/// Same-batch creates are skipped (pin uses offline denserels).
///
/// Prep pin with [`ColdPinMode::Forbid`] must see every external parent covered
/// via plan-local map, residency hit, in-flight, or same-batch — **not** via
/// residency seed of cold parents.
pub fn ensure_external_parent_denserels_from_plan(
    query: &Query,
    plan: Option<&mut rbitcoin_query::ArchiveWritePlan>,
    in_flight_outs: Option<
        &HashMap<
            u64,
            std::sync::Arc<(
                rbitcoin_store::TxRecord,
                Vec<rbitcoin_store::OutputRecord>,
                Vec<u32>,
            )>,
        >,
    >,
) -> Result<DenserelsWarmStats, ConsensusError> {
    use rbitcoin_query::confirm_load_stats;
    use rbitcoin_store::IdxBodyMode;
    use std::sync::atomic::Ordering;

    let t0 = Instant::now();
    let mut st = DenserelsWarmStats::default();
    let Some(plan) = plan else {
        st.work_ns = t0.elapsed().as_nanos() as u64;
        return Ok(st);
    };

    // Same-batch create ids (offline denserels at pin — do not cold-load Class A).
    let mut batch_create_ids: HashMap<u64, ()> = HashMap::new();
    for fk in &plan.planned_fks {
        if let Some(id) = fk.get() {
            batch_create_ids.insert(id, ());
        }
    }

    // Spent parent create_fk → need vouts (from stamped inputs only).
    let mut parent_vouts: HashMap<u64, Vec<u32>> = HashMap::new();
    let t_collect = Instant::now();
    for ((_pin, ins), _) in plan.packed.iter().zip(plan.planned_fks.iter()) {
        for inp in ins {
            if inp.is_coinbase() || inp.prev_index == u32::MAX {
                continue;
            }
            if let Some(pid) = inp.create_fk.get() {
                parent_vouts.entry(pid).or_default().push(inp.prev_index);
            }
        }
    }
    for vouts in parent_vouts.values_mut() {
        vouts.sort_unstable();
        vouts.dedup();
    }
    let collect_ns = t_collect.elapsed().as_nanos() as u64;

    let mut cold_fks: Vec<rbitcoin_primitives::Fk> = Vec::new();
    for (id, need) in &parent_vouts {
        if batch_create_ids.contains_key(id) {
            st.same_batch = st.same_batch.saturating_add(1);
            continue;
        }
        st.parents = st.parents.saturating_add(1);
        let fk = rbitcoin_primitives::Fk(*id);
        // Plan-local external parent already loaded at Shape A.
        if plan
            .external_parent_outs
            .get(id)
            .is_some_and(|pin| !pin.2.is_empty())
        {
            st.already = st.already.saturating_add(1);
            continue;
        }
        // In-flight offline denserels already available for pin.
        if let Some(ifo) = in_flight_outs {
            if ifo.get(id).is_some_and(|pin| !pin.2.is_empty()) {
                st.already = st.already.saturating_add(1);
                continue;
            }
        }
        // Prior pipeline create still in residency — Arc-share into plan-local.
        if let Some((pin, _range)) = query.create_residency().get_pin(fk) {
            if !pin.2.is_empty()
                && need
                    .iter()
                    .all(|&v| (v as usize) < pin.1.len())
            {
                plan.external_parent_outs
                    .insert(*id, std::sync::Arc::clone(&pin));
                st.already = st.already.saturating_add(1);
                continue;
            }
        }
        cold_fks.push(fk);
    }
    cold_fks.sort_unstable_by_key(|f| f.0);
    cold_fks.dedup();
    st.cold = cold_fks.len() as u32;

    let mut cold_io_ns = 0u64;
    if !cold_fks.is_empty() {
        let t_io = Instant::now();
        // Prefer plan stamp body ranges (skip tx.idx) — uring denserels batch.
        let mut by_range: Vec<(rbitcoin_primitives::Fk, (u64, u64))> = Vec::new();
        let mut need_idx: Vec<rbitcoin_primitives::Fk> = Vec::new();
        for fk in &cold_fks {
            let id = fk.get().unwrap_or(0);
            if let Some(&range) = plan.external_parent_ranges.get(&id) {
                by_range.push((*fk, range));
            } else {
                need_idx.push(*fk);
            }
        }
        if !by_range.is_empty() {
            let t_rng = Instant::now();
            let n_range = by_range.len() as u64;
            let decoded = query
                .store()
                .get_outs_denserels_by_range_batch(&by_range)
                .map_err(ConsensusError::Store)?;
            let rng_ns = t_rng.elapsed().as_nanos() as u64;
            if rng_ns > 0 {
                confirm_load_stats::COLD_RANGE_NS.fetch_add(rng_ns, Ordering::Relaxed);
            }
            confirm_load_stats::COLD_RANGE_N.fetch_add(n_range, Ordering::Relaxed);
            for ((fk, _), row) in by_range.iter().zip(decoded.into_iter()) {
                if let Some((mut tx, outs, dens)) = row {
                    if let Some(id) = fk.get() {
                        fill_create_txid_from_ram(
                            &mut tx,
                            id,
                            Some(plan),
                            query.create_residency(),
                        );
                        plan.external_parent_outs
                            .insert(id, std::sync::Arc::new((tx, outs, dens)));
                    }
                }
            }
            confirm_load_stats::BODY_TX_READS.fetch_add(n_range, Ordering::Relaxed);
            confirm_load_stats::PIN_NEW.fetch_add(n_range, Ordering::Relaxed);
        }
        // Fallback: idx→body denserels (no plan range).
        if !need_idx.is_empty() {
            let t_idx = Instant::now();
            let loaded = rbitcoin_query::load_creates_once_seed(
                query.store(),
                query.create_residency(),
                &need_idx,
                IdxBodyMode::OutsDenserels,
                false,
            )
            .map_err(ConsensusError::Store)?;
            let idx_ns = t_idx.elapsed().as_nanos() as u64;
            let n_idx = loaded.len() as u64;
            if idx_ns > 0 {
                confirm_load_stats::COLD_IDX_NS.fetch_add(idx_ns, Ordering::Relaxed);
            }
            if n_idx > 0 {
                confirm_load_stats::COLD_IDX_N.fetch_add(n_idx, Ordering::Relaxed);
            }
            confirm_load_stats::BODY_TX_READS.fetch_add(n_idx, Ordering::Relaxed);
            confirm_load_stats::FULL_TX_READS.fetch_add(n_idx, Ordering::Relaxed);
            confirm_load_stats::PIN_NEW.fetch_add(n_idx, Ordering::Relaxed);
            for c in loaded {
                let Some(id) = c.fk.get() else {
                    continue;
                };
                let (mut tx, outs, dens) = if let Some(dec) = c.decoded_outs {
                    dec
                } else {
                    rbitcoin_store::decode_packed_tx_outs_with_spender_rels_secret(
                        &c.raw,
                        Some(query.store().txs.store_secret()),
                    )
                    .map_err(|_| {
                        ConsensusError::Store(StoreError::Corrupt(
                            "invariant: plan stage external parent denserels decode failed",
                        ))
                    })?
                };
                fill_create_txid_from_ram(
                    &mut tx,
                    id,
                    Some(plan),
                    query.create_residency(),
                );
                plan.external_parent_outs
                    .insert(id, std::sync::Arc::new((tx, outs, dens)));
            }
        }
        cold_io_ns = t_io.elapsed().as_nanos() as u64;
        if cold_io_ns > 0 {
            confirm_load_stats::COLD_IO_NS.fetch_add(cold_io_ns, Ordering::Relaxed);
            confirm_load_stats::PIN_NEW_META_NS.fetch_add(cold_io_ns, Ordering::Relaxed);
        }
        // Completeness: every cold parent must be plan-local (not residency).
        for fk in &cold_fks {
            let id = fk.get().unwrap_or(0);
            if plan
                .external_parent_outs
                .get(&id)
                .is_none_or(|pin| pin.2.is_empty())
            {
                return Err(ConsensusError::Store(StoreError::Corrupt(
                    "invariant: plan stage failed to load external parent denserels",
                )));
            }
        }
    }

    st.work_ns = t0.elapsed().as_nanos() as u64;
    // Parent mix + subtimers; wall TOTAL_NS is owned by plan stage caller.
    plan_stage_stats::note(
        0, // blocks counted by caller
        st.parents as u64,
        st.already as u64,
        st.cold as u64,
        st.same_batch as u64,
        0,
        collect_ns,
        0,
        cold_io_ns,
    );
    if st.work_ns > 0 {
        confirm_load_stats::PARENT_PIN_NS.fetch_add(st.work_ns, Ordering::Relaxed);
        confirm_load_stats::NS.fetch_add(st.work_ns, Ordering::Relaxed);
    }
    if st.parents > 0 {
        confirm_load_stats::PARENT_UNIQUE.fetch_add(st.parents as u64, Ordering::Relaxed);
    }
    if st.already > 0 {
        confirm_load_stats::PIN_RESIDENCY.fetch_add(st.already as u64, Ordering::Relaxed);
        confirm_load_stats::PIN_CACHE_BODY.fetch_add(st.already as u64, Ordering::Relaxed);
    }
    Ok(st)
}

/// Plan-stage output: structure + plan mega only (create_fk stamped).
///
/// Denserels pin + assemble stay on **prep** so the pipeline stays balanced:
/// plan(N+1) head-stamp overlaps prep(N) denserels IO. Handoff is the owned
/// [`ArchiveWritePlan`] + wire/metas — **not** CreateResidency (FIFO race).
pub struct PlanStampOutcome {
    pub plan: Option<rbitcoin_query::ArchiveWritePlan>,
    /// Wall ns for structure + plan_mega (head stamp).
    pub work_ns: u64,
    metas: Vec<BodyMeta>,
    wire_blocks: Vec<Arc<Block>>,
}

/// IBD **plan** stage: structure + stamp create_fk only (no denserels pin).
///
/// Wire blocks are `Arc` so IBD resolve can decode once and hand off without
/// cloning full `Block` payloads into stamp.
pub fn confirm_wire_plan_stamp(
    query: &Query,
    params: &ChainParams,
    milestone: Milestone,
    blocks: &[(Height, Arc<Block>)],
    pipeline: Option<&WirePrepPipeline>,
) -> Result<PlanStampOutcome, ConsensusError> {
    let t0 = Instant::now();
    let (plan, metas, wire_blocks, plan_ns) =
        wire_plan_phase(query, params, milestone, blocks, pipeline)?;
    plan_stage_stats::BLOCKS.fetch_add(blocks.len() as u64, Ordering::Relaxed);
    plan_stage_stats::HEAD_NS.fetch_add(plan_ns, Ordering::Relaxed);
    let work_ns = t0.elapsed().as_nanos() as u64;
    plan_stage_stats::TOTAL_NS.fetch_add(work_ns, Ordering::Relaxed);
    Ok(PlanStampOutcome {
        plan,
        work_ns,
        metas,
        wire_blocks,
    })
}

/// IBD **prep** after plan: pin denserels once (Allow) + assemble.
///
/// Uses the owned stamped plan — does **not** re-run plan_mega / head resolve.
pub fn confirm_wire_prep_from_plan(
    query: &Query,
    params: &ChainParams,
    milestone: Milestone,
    stamped: PlanStampOutcome,
    pipeline: Option<&WirePrepPipeline>,
    preverified: &ScriptPreverified,
) -> Result<ConfirmLoadOutcome, ConsensusError> {
    let t_work = Instant::now();
    let t_load = Instant::now();
    let PlanStampOutcome {
        mut plan,
        metas,
        wire_blocks,
        ..
    } = stamped;

    let ifo = pipeline.map(|p| p.in_flight_outs.as_ref());
    let (batch_parents, batch_thin, _warm) = pin_for_wire_batch(
        query,
        plan.as_ref(),
        &metas,
        &wire_blocks,
        ifo,
        ColdPinMode::Allow,
    )?;
    // External full-outs were only for pin; sparse BatchParents holds need-vouts.
    // Drop so prep→scripts→write does not retain head-miss denserels material.
    if let Some(ref mut p) = plan {
        p.clear_external_parent_outs();
    }

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

/// Plan + ensure denserels into plan-local external_parent_outs (no pin). Unit tests.
pub fn confirm_wire_plan_and_ensure_denserels(
    query: &Query,
    params: &ChainParams,
    milestone: Milestone,
    blocks: &[(Height, Arc<Block>)],
    pipeline: Option<&WirePrepPipeline>,
) -> Result<(Option<rbitcoin_query::ArchiveWritePlan>, DenserelsWarmStats, u64), ConsensusError>
{
    let t0 = Instant::now();
    let (mut plan, _metas, _wire, plan_ns) =
        wire_plan_phase(query, params, milestone, blocks, pipeline)?;
    plan_stage_stats::BLOCKS.fetch_add(blocks.len() as u64, Ordering::Relaxed);
    plan_stage_stats::HEAD_NS.fetch_add(plan_ns, Ordering::Relaxed);

    let ifo = pipeline.map(|p| p.in_flight_outs.as_ref());
    let warm = ensure_external_parent_denserels_from_plan(query, plan.as_mut(), ifo)?;
    let work_ns = t0.elapsed().as_nanos() as u64;
    plan_stage_stats::TOTAL_NS.fetch_add(work_ns, Ordering::Relaxed);
    Ok((plan, warm, work_ns))
}

/// Structure + prepare + plan_mega only (stamp create_fk). Shared by plan stage.
fn wire_plan_phase(
    query: &Query,
    params: &ChainParams,
    milestone: Milestone,
    blocks: &[(Height, Arc<Block>)],
    pipeline: Option<&WirePrepPipeline>,
) -> Result<
    (
        Option<rbitcoin_query::ArchiveWritePlan>,
        Vec<BodyMeta>,
        Vec<Arc<Block>>,
        u64, // plan wall ns (filter+plan_mega dominate)
    ),
    ConsensusError,
> {
    if blocks.is_empty() {
        return Err(ConsensusError::BadBlock("empty confirm batch"));
    }
    for w in blocks.windows(2) {
        if w[1].0 .0 != w[0].0 .0.saturating_add(1) {
            return Err(ConsensusError::BadBlock("confirm run not contiguous"));
        }
    }

    let mut with_fk: Vec<(
        rbitcoin_primitives::Fk,
        rbitcoin_store::HeaderRecord,
        Vec<rbitcoin_query::TxApply>,
    )> = Vec::with_capacity(blocks.len());
    let mut wire_blocks: Vec<Arc<Block>> = Vec::with_capacity(blocks.len());
    let mut metas: Vec<BodyMeta> = Vec::with_capacity(blocks.len());

    let tip_h = query.tip_height().map(|h| h.0);
    let store_path_lo = match tip_h {
        None => 0u32,
        Some(t) => t.saturating_add(1),
    };
    let path_lo = pipeline.map(|p| p.path_lo).unwrap_or(store_path_lo);

    // Stamp sub-walls (structure + prepare summed over batch; mega below).
    let mut struct_ns = 0u64;
    let mut prepare_ns = 0u64;

    for (i, (height, block)) in blocks.iter().enumerate() {
        let block = Arc::clone(block);
        let hash = block.block_hash().to_byte_array();
        let ctx = ValidationContext::at(params, *height, milestone);
        let t_struct = Instant::now();
        let _ = crate::block::validate_block_structure_hashed(block.as_ref(), &ctx)?;
        if i == 0 {
            if height.0 != path_lo {
                return Err(ConsensusError::BadPrev);
            }
            if path_lo == store_path_lo {
                validate_header(query, params, *height, &block.header)?;
            } else {
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
            let prev = &wire_blocks[i - 1];
            if block.header.prev_blockhash != prev.block_hash() {
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
        struct_ns = struct_ns.saturating_add(t_struct.elapsed().as_nanos() as u64);

        let t_prep = Instant::now();
        let (header_rec, txs) =
            crate::prepare_block_for_archive(query, params, block.as_ref())?;
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
        prepare_ns = prepare_ns.saturating_add(t_prep.elapsed().as_nanos() as u64);
        with_fk.push((header_fk, header_rec.clone(), txs));
        wire_blocks.push(block);
        metas.push(BodyMeta {
            height: *height,
            hash,
            header_fk,
            header_rec,
            tx_fks: Vec::new(),
        });
    }

    let t_filter = Instant::now();
    let (_header_fks, mut need) = query
        .archive_filter_need_bodies(&mut with_fk)
        .map_err(ConsensusError::Store)?;
    let filter_ns = t_filter.elapsed().as_nanos() as u64;
    let t_mega = Instant::now();
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
                .archive_plan_mega_from(
                    &mut need,
                    p.next_tx_start.max(1),
                    p.in_flight_creates.as_ref(),
                )
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
    let mega_ns = t_mega.elapsed().as_nanos() as u64;
    // plan_ns for HEAD_NS: filter + mega (legacy “plan wall” without struct/prepare).
    let plan_ns = filter_ns.saturating_add(mega_ns);
    plan_stamp_sub_stats::note(struct_ns, prepare_ns, filter_ns, mega_ns);
    Ok((plan, metas, wire_blocks, plan_ns))
}

/// Stamp-phase sub-walls for plan_thr diagnosis (structure / prepare / filter / mega).
///
/// Mega is the archive plan_mega wall (assign+collect+res+head_fk+head_dens+stamp+finish
/// already timed in `archive_phase_stats`). `head_fk` = get_fk_by_txid_batch;
/// `head_dens` = plan-time external-parent denserels load; `head` = sum.
pub mod plan_stamp_sub_stats {
    use std::sync::atomic::{AtomicU64, Ordering};

    static STRUCT_NS: AtomicU64 = AtomicU64::new(0);
    static PREPARE_NS: AtomicU64 = AtomicU64::new(0);
    static FILTER_NS: AtomicU64 = AtomicU64::new(0);
    static MEGA_NS: AtomicU64 = AtomicU64::new(0);

    pub fn note(struct_ns: u64, prepare_ns: u64, filter_ns: u64, mega_ns: u64) {
        if struct_ns > 0 {
            STRUCT_NS.fetch_add(struct_ns, Ordering::Relaxed);
        }
        if prepare_ns > 0 {
            PREPARE_NS.fetch_add(prepare_ns, Ordering::Relaxed);
        }
        if filter_ns > 0 {
            FILTER_NS.fetch_add(filter_ns, Ordering::Relaxed);
        }
        if mega_ns > 0 {
            MEGA_NS.fetch_add(mega_ns, Ordering::Relaxed);
        }
    }

    #[derive(Debug, Default, Clone, Copy)]
    pub struct Sample {
        pub struct_ns: u64,
        pub prepare_ns: u64,
        pub filter_ns: u64,
        pub mega_ns: u64,
    }

    pub fn sample_and_reset() -> Sample {
        Sample {
            struct_ns: STRUCT_NS.swap(0, Ordering::Relaxed),
            prepare_ns: PREPARE_NS.swap(0, Ordering::Relaxed),
            filter_ns: FILTER_NS.swap(0, Ordering::Relaxed),
            mega_ns: MEGA_NS.swap(0, Ordering::Relaxed),
        }
    }
}

/// Accumulators for the **plan** pipeline stage (plan+stamp + denserels ensure).
pub mod plan_stage_stats {
    use std::sync::atomic::{AtomicU64, Ordering};

    pub static BLOCKS: AtomicU64 = AtomicU64::new(0);
    pub static PARENTS: AtomicU64 = AtomicU64::new(0);
    pub static ALREADY: AtomicU64 = AtomicU64::new(0);
    pub static COLD: AtomicU64 = AtomicU64::new(0);
    pub static UNRESOLVED: AtomicU64 = AtomicU64::new(0);
    pub static TOTAL_NS: AtomicU64 = AtomicU64::new(0);
    pub static COLLECT_NS: AtomicU64 = AtomicU64::new(0);
    pub static HEAD_NS: AtomicU64 = AtomicU64::new(0);
    pub static COLD_IO_NS: AtomicU64 = AtomicU64::new(0);

    pub fn note(
        blocks: u64,
        parents: u64,
        already: u64,
        cold: u64,
        unresolved: u64,
        total_ns: u64,
        collect_ns: u64,
        head_ns: u64,
        cold_io_ns: u64,
    ) {
        if blocks > 0 {
            BLOCKS.fetch_add(blocks, Ordering::Relaxed);
        }
        if parents > 0 {
            PARENTS.fetch_add(parents, Ordering::Relaxed);
        }
        if already > 0 {
            ALREADY.fetch_add(already, Ordering::Relaxed);
        }
        if cold > 0 {
            COLD.fetch_add(cold, Ordering::Relaxed);
        }
        if unresolved > 0 {
            UNRESOLVED.fetch_add(unresolved, Ordering::Relaxed);
        }
        if total_ns > 0 {
            TOTAL_NS.fetch_add(total_ns, Ordering::Relaxed);
        }
        if collect_ns > 0 {
            COLLECT_NS.fetch_add(collect_ns, Ordering::Relaxed);
        }
        if head_ns > 0 {
            HEAD_NS.fetch_add(head_ns, Ordering::Relaxed);
        }
        if cold_io_ns > 0 {
            COLD_IO_NS.fetch_add(cold_io_ns, Ordering::Relaxed);
        }
    }

    #[derive(Debug, Default, Clone, Copy)]
    pub struct Sample {
        pub blocks: u64,
        pub parents: u64,
        pub already: u64,
        pub cold: u64,
        pub unresolved: u64,
        pub total_ns: u64,
        pub collect_ns: u64,
        pub head_ns: u64,
        pub cold_io_ns: u64,
    }

    pub fn sample_and_reset() -> Sample {
        Sample {
            blocks: BLOCKS.swap(0, Ordering::Relaxed),
            parents: PARENTS.swap(0, Ordering::Relaxed),
            already: ALREADY.swap(0, Ordering::Relaxed),
            cold: COLD.swap(0, Ordering::Relaxed),
            unresolved: UNRESOLVED.swap(0, Ordering::Relaxed),
            total_ns: TOTAL_NS.swap(0, Ordering::Relaxed),
            collect_ns: COLLECT_NS.swap(0, Ordering::Relaxed),
            head_ns: HEAD_NS.swap(0, Ordering::Relaxed),
            cold_io_ns: COLD_IO_NS.swap(0, Ordering::Relaxed),
        }
    }
}

/// Schema 13: denserels body decode leaves `txid` zero. Fill from **RAM only**
/// (plan stamp reverse map, then residency). Never `txid.body` / sidefile pread.
#[inline]
fn fill_create_txid_from_ram(
    tx: &mut rbitcoin_store::TxRecord,
    create_fk_id: u64,
    plan: Option<&rbitcoin_query::ArchiveWritePlan>,
    residency: &rbitcoin_query::CreateResidency,
) {
    if tx.txid != [0u8; 32] {
        return;
    }
    if let Some(p) = plan {
        if let Some(tid) = p.external_parent_txid(create_fk_id) {
            tx.txid = tid;
            return;
        }
    }
    if let Some(tid) = residency.get_txid(rbitcoin_primitives::Fk(create_fk_id)) {
        tx.txid = tid;
    }
}

/// Pin parents for wire prep: **only spent parents** (sparse outs).
///
/// Sources: plan/in-flight packed outs (+ offline denserels) → CreateResidency
/// sparse hit → cold denserels (when [`ColdPinMode::Allow`]). Does not pin every
/// batch create.
fn pin_for_wire_batch(
    query: &Query,
    plan: Option<&rbitcoin_query::ArchiveWritePlan>,
    metas: &[BodyMeta],
    wire_blocks: &[Arc<Block>],
    in_flight_outs: Option<
        &HashMap<
            u64,
            std::sync::Arc<(
                rbitcoin_store::TxRecord,
                Vec<rbitcoin_store::OutputRecord>,
                Vec<u32>,
            )>,
        >,
    >,
    cold_mode: ColdPinMode,
) -> Result<(rbitcoin_query::BatchParents, rbitcoin_query::BatchThin, DenserelsWarmStats), ConsensusError>
{
    use rbitcoin_query::confirm_load_stats;
    use rbitcoin_query::ThinInput;
    use rbitcoin_store::IdxBodyMode;
    use std::sync::atomic::Ordering;

    let t_pin = Instant::now();
    let mut batch_thin: rbitcoin_query::BatchThin = std::collections::HashMap::new();
    let mut parent_vouts: HashMap<u64, Vec<u32>> = HashMap::new();
    let mut n_same_batch = 0u32;

    // id → Arc pin (tx, outs, dense denserels). Spent parents only (after thin pass).
    let mut plan_by_id: HashMap<
        u64,
        std::sync::Arc<(
            rbitcoin_store::TxRecord,
            Vec<rbitcoin_store::OutputRecord>,
            Vec<u32>,
        )>,
    > = HashMap::new();
    // batch_pin by create id (Arc — preferred same-batch pin source).
    // packed pin half shares the same Arc; no separate outs clone.
    let mut batch_pin_by_id: HashMap<
        u64,
        &std::sync::Arc<(
            rbitcoin_store::TxRecord,
            Vec<rbitcoin_store::OutputRecord>,
            Vec<u32>,
        )>,
    > = HashMap::new();
    if let Some(plan) = plan {
        if plan.batch_pin.len() == plan.planned_fks.len() {
            for (fk, pin) in plan.planned_fks.iter().zip(plan.batch_pin.iter()) {
                if let Some(id) = fk.get() {
                    batch_pin_by_id.insert(id, pin);
                }
            }
        } else {
            // Partial plans (tests): fall back to packed pin half.
            for ((pin, _ins), fk) in plan.packed.iter().zip(plan.planned_fks.iter()) {
                if let Some(id) = fk.get() {
                    batch_pin_by_id.insert(id, pin);
                }
            }
        }
        for ((_pin, ins), fk) in plan.packed.iter().zip(plan.planned_fks.iter()) {
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
                    // Same-batch or external parent — only spent creates need pin.
                    parent_vouts.entry(pid).or_default().push(inp.prev_index);
                } else {
                    edges.push(ThinInput {
                        create_fk: None,
                        prev_index: inp.prev_index,
                    });
                }
            }
            batch_thin.insert(sid, edges);
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
                            parent_vouts.entry(pid).or_default().push(vout);
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

    for vouts in parent_vouts.values_mut() {
        vouts.sort_unstable();
        vouts.dedup();
    }

    // Build plan/in-flight pin sources only for spent parents (not every create).
    // 1) Prior uncommitted plans (Arc pin — no deep clone).
    if let Some(ifo) = in_flight_outs {
        for (id, need) in &parent_vouts {
            if plan_by_id.contains_key(id) {
                continue;
            }
            if let Some(pin) = ifo.get(id) {
                let _ = need;
                plan_by_id.insert(*id, std::sync::Arc::clone(pin));
            }
        }
    }
    // 2) This plan's head-miss external parents (shared CreatePin Arc — no deep clone).
    if let Some(plan) = plan {
        for (id, need) in &parent_vouts {
            if plan_by_id.contains_key(id) {
                continue;
            }
            if let Some(pin) = plan.external_parent_outs.get(id) {
                let _ = need;
                plan_by_id.insert(*id, std::sync::Arc::clone(pin));
            }
        }
        // 2b) Plan stamp body ranges → denserels by offset (skip tx.idx; uring batch).
        let mut range_jobs: Vec<(rbitcoin_primitives::Fk, (u64, u64))> = Vec::new();
        for id in parent_vouts.keys() {
            if plan_by_id.contains_key(id) {
                continue;
            }
            if let Some(&range) = plan.external_parent_ranges.get(id) {
                range_jobs.push((rbitcoin_primitives::Fk(*id), range));
            }
        }
        if !range_jobs.is_empty() {
            let t_rng = Instant::now();
            let n_range = range_jobs.len() as u64;
            let decoded = query
                .store()
                .get_outs_denserels_by_range_batch(&range_jobs)
                .map_err(ConsensusError::Store)?;
            let rng_ns = t_rng.elapsed().as_nanos() as u64;
            if rng_ns > 0 {
                confirm_load_stats::COLD_IO_NS.fetch_add(rng_ns, Ordering::Relaxed);
                confirm_load_stats::COLD_RANGE_NS.fetch_add(rng_ns, Ordering::Relaxed);
            }
            confirm_load_stats::COLD_RANGE_N.fetch_add(n_range, Ordering::Relaxed);
            confirm_load_stats::BODY_TX_READS.fetch_add(n_range, Ordering::Relaxed);
            confirm_load_stats::PIN_NEW.fetch_add(n_range, Ordering::Relaxed);
            for ((fk, _), row) in range_jobs.into_iter().zip(decoded.into_iter()) {
                if let (Some(id), Some((mut tx, outs, dens))) = (fk.get(), row) {
                    fill_create_txid_from_ram(
                        &mut tx,
                        id,
                        Some(plan),
                        query.create_residency(),
                    );
                    plan_by_id.insert(id, std::sync::Arc::new((tx, outs, dens)));
                }
            }
        }
    }
    // 3) Same-batch creates: shared batch_pin / packed CreatePin Arc.
    for (id, _need) in &parent_vouts {
        if plan_by_id.contains_key(id) {
            continue;
        }
        if let Some(pin) = batch_pin_by_id.get(id) {
            plan_by_id.insert(*id, std::sync::Arc::clone(pin));
            n_same_batch = n_same_batch.saturating_add(1);
        }
    }

    let mut batch_parents = rbitcoin_query::BatchParents::with_capacity(parent_vouts.len());
    let mut still_need: HashMap<u64, Vec<u32>> = HashMap::new();
    let mut n_plan_pin = 0u64;

    // Plan / in-flight: sparse pin only spent parents (not every batch create).
    // Prefer residency body_range when prior batch already committed; always
    // attach offline denserels so write ensure does not re-read Class A body.
    let t_plan = Instant::now();
    for (id, need) in &parent_vouts {
        if let Some(pin) = plan_by_id.get(id) {
            let (tx, outs, denserels) = pin.as_ref();
            let live: Vec<(u32, rbitcoin_store::OutputRecord)> = need
                .iter()
                .filter_map(|&v| outs.get(v as usize).map(|o| (v, o.clone())))
                .collect();
            if live.len() != need.len() {
                // Incomplete plan outs — fall through to residency/cold.
                still_need.insert(*id, need.clone());
                continue;
            }
            let cb = if tx.input_count != 1 {
                Some(false)
            } else {
                None
            };
            let fk = rbitcoin_primitives::Fk(*id);
            // Body range: plan stamp head range > residency > none.
            let plan_range = plan.and_then(|p| p.external_parent_ranges.get(id).copied());
            // If commit(N) already seeded residency, take body_range (and denserels
            // if offline was empty).
            let (body_range, sparse) =
                if let Some((_rtx, _live, res_sparse, range)) =
                    query.create_residency().get_parent_needed(fk, need)
                {
                    if denserels.is_empty() {
                        (plan_range.or(range), res_sparse)
                    } else {
                        let sp = rbitcoin_query::sparse_spender_rels(denserels, need);
                        (plan_range.or(range), sp)
                    }
                } else if !denserels.is_empty() {
                    (
                        plan_range,
                        rbitcoin_query::sparse_spender_rels(denserels, need),
                    )
                } else {
                    (plan_range, Vec::new())
                };
            batch_parents.insert_owned(
                fk,
                tx.clone(),
                live,
                need.clone(),
                cb,
                body_range,
                sparse,
            );
            n_plan_pin = n_plan_pin.saturating_add(1);
        } else {
            still_need.insert(*id, need.clone());
        }
    }
    let plan_pin_ns = t_plan.elapsed().as_nanos() as u64;

    // CreateResidency sparse denserels hit, then cold denserels once.
    let mut n_res_hit = 0u64;
    let mut n_cold = 0u64;
    let mut res_hit_ns = 0u64;
    let mut cold_io_ns = 0u64;
    let mut cold_decode_ns = 0u64;
    if !still_need.is_empty() {
        let mut cold: HashMap<u64, Vec<u32>> = HashMap::new();
        let t_res = Instant::now();
        for (id, need) in &still_need {
            let fk = rbitcoin_primitives::Fk(*id);
            if let Some((tx, live, sparse, body_range)) =
                query.create_residency().get_parent_needed(fk, need)
            {
                let cb = if tx.input_count != 1 {
                    Some(false)
                } else {
                    None
                };
                batch_parents.insert_owned(fk, tx, live, need.clone(), cb, body_range, sparse);
                n_res_hit = n_res_hit.saturating_add(1);
            } else {
                cold.insert(*id, need.clone());
            }
        }
        res_hit_ns = t_res.elapsed().as_nanos() as u64;

        if !cold.is_empty() {
            if cold_mode == ColdPinMode::Forbid {
                return Err(ConsensusError::Store(StoreError::Corrupt(
                    "invariant: plan stage miss (prep cold denserels forbidden)",
                )));
            }
            let t_io = Instant::now();
            let fks: Vec<rbitcoin_primitives::Fk> = cold
                .keys()
                .map(|id| rbitcoin_primitives::Fk(*id))
                .collect();
            n_cold = fks.len() as u64;
            // External parents: never seed residency (batch-local pin only).
            let loaded = rbitcoin_query::load_creates_once_seed(
                query.store(),
                query.create_residency(),
                &fks,
                IdxBodyMode::OutsDenserels,
                false,
            )
            .map_err(ConsensusError::Store)?;
            cold_io_ns = t_io.elapsed().as_nanos() as u64;
            if cold_io_ns > 0 {
                confirm_load_stats::COLD_IDX_NS.fetch_add(cold_io_ns, Ordering::Relaxed);
            }
            if n_cold > 0 {
                confirm_load_stats::COLD_IDX_N.fetch_add(n_cold, Ordering::Relaxed);
            }

            let t_dec = Instant::now();
            for c in loaded {
                let Some(id) = c.fk.get() else {
                    return Err(ConsensusError::Store(StoreError::Corrupt(
                        "invariant: wire pin cold denserels null create_fk",
                    )));
                };
                let need = cold.get(&id).cloned().unwrap_or_default();
                // Prefer single-decode from load_creates_once; raw is second chance only.
                let (mut tx, outs, dense_rels) = if let Some(dec) = c.decoded_outs {
                    dec
                } else {
                    rbitcoin_store::decode_packed_tx_outs_with_spender_rels_secret(
                        &c.raw,
                        Some(query.store().txs.store_secret()),
                    )
                    .map_err(|_| {
                        ConsensusError::Store(StoreError::Corrupt(
                            "invariant: wire pin cold denserels decode failed",
                        ))
                    })?
                };
                // RAM identity only (plan stamp reverse map / residency) — no sidefile.
                fill_create_txid_from_ram(
                    &mut tx,
                    id,
                    plan,
                    query.create_residency(),
                );
                let mut need = need;
                need.sort_unstable();
                need.dedup();
                if need.is_empty() {
                    need = (0..outs.len() as u32).collect();
                }
                let live: Vec<(u32, rbitcoin_store::OutputRecord)> = need
                    .iter()
                    .filter_map(|&v| outs.get(v as usize).map(|o| (v, o.clone())))
                    .collect();
                if live.len() != need.len() {
                    return Err(ConsensusError::Store(StoreError::Corrupt(
                        "invariant: wire pin cold denserels incomplete outs for need_vouts",
                    )));
                }
                let sparse = rbitcoin_query::sparse_spender_rels(&dense_rels, &need);
                if !rbitcoin_query::layout_covers_need(Some(c.body_range), &sparse, &need) {
                    return Err(ConsensusError::Store(StoreError::Corrupt(
                        "invariant: wire pin cold denserels incomplete for need_vouts",
                    )));
                }
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
            cold_decode_ns = t_dec.elapsed().as_nanos() as u64;
            confirm_load_stats::BODY_TX_READS.fetch_add(n_cold, Ordering::Relaxed);
            confirm_load_stats::FULL_TX_READS.fetch_add(n_cold, Ordering::Relaxed);
            // Every cold parent must have been loaded — silent miss is a prep/store bug.
            for id in cold.keys() {
                let fk = rbitcoin_primitives::Fk(*id);
                if !batch_parents.contains(fk) {
                    return Err(ConsensusError::Store(StoreError::Corrupt(
                        "invariant: wire pin cold denserels missing parent after load",
                    )));
                }
            }
        }
    }

    // Pin contract: every spent parent is in BatchParents with need outs.
    // Denserels/body_range may wait for write ensure (prep-ahead plan pin).
    for (id, need) in &parent_vouts {
        let fk = rbitcoin_primitives::Fk(*id);
        if !batch_parents.contains(fk) {
            return Err(ConsensusError::Store(StoreError::Corrupt(
                "invariant: wire pin missing spent parent",
            )));
        }
        if !need.is_empty() && !batch_parents.pin_covered(fk, need) {
            return Err(ConsensusError::Store(StoreError::Corrupt(
                "invariant: wire pin incomplete outs for spent parent",
            )));
        }
    }

    let n_unique = parent_vouts.len() as u64;
    if n_unique > 0 {
        confirm_load_stats::PARENT_UNIQUE.fetch_add(n_unique, Ordering::Relaxed);
        confirm_load_stats::UTXO_PARENTS.fetch_add(n_unique, Ordering::Relaxed);
    }
    if n_plan_pin > 0 {
        confirm_load_stats::PIN_PLAN.fetch_add(n_plan_pin, Ordering::Relaxed);
        confirm_load_stats::PIN_CACHE_BODY.fetch_add(n_plan_pin, Ordering::Relaxed);
    }
    if n_res_hit > 0 {
        confirm_load_stats::PIN_RESIDENCY.fetch_add(n_res_hit, Ordering::Relaxed);
        confirm_load_stats::PIN_CACHE_BODY.fetch_add(n_res_hit, Ordering::Relaxed);
        confirm_load_stats::PARENT_CACHE_HITS.fetch_add(n_res_hit, Ordering::Relaxed);
    }
    if n_cold > 0 {
        confirm_load_stats::PIN_NEW.fetch_add(n_cold, Ordering::Relaxed);
    }
    if plan_pin_ns > 0 {
        confirm_load_stats::PLAN_PIN_NS.fetch_add(plan_pin_ns, Ordering::Relaxed);
    }
    if res_hit_ns > 0 {
        confirm_load_stats::RES_HIT_NS.fetch_add(res_hit_ns, Ordering::Relaxed);
    }
    if cold_io_ns > 0 {
        confirm_load_stats::COLD_IO_NS.fetch_add(cold_io_ns, Ordering::Relaxed);
        confirm_load_stats::PIN_NEW_META_NS.fetch_add(cold_io_ns, Ordering::Relaxed);
    }
    if cold_decode_ns > 0 {
        confirm_load_stats::COLD_DECODE_NS.fetch_add(cold_decode_ns, Ordering::Relaxed);
    }
    let pin_ns = t_pin.elapsed().as_nanos() as u64;
    if pin_ns > 0 {
        confirm_load_stats::PARENT_PIN_NS.fetch_add(pin_ns, Ordering::Relaxed);
        confirm_load_stats::PIN_BODY_NS.fetch_add(pin_ns, Ordering::Relaxed);
        // Wire path: `NS` is pin wall (legacy load path uses full load_confirm wall).
        confirm_load_stats::NS.fetch_add(pin_ns, Ordering::Relaxed);
    }
    let n_blks = metas.len() as u64;
    if n_blks > 0 {
        confirm_load_stats::BLOCKS.fetch_add(n_blks, Ordering::Relaxed);
    }

    let warm = DenserelsWarmStats {
        // External parents only (unique spent creates not same-batch offline).
        parents: parent_vouts
            .len()
            .saturating_sub(n_same_batch as usize) as u32,
        already: n_res_hit.saturating_add(n_plan_pin.saturating_sub(n_same_batch as u64)) as u32,
        cold: n_cold as u32,
        same_batch: n_same_batch,
        work_ns: pin_ns,
    };
    Ok((batch_parents, batch_thin, warm))
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
/// same stage before structural/annotate — single ordered commit era.
/// **Class A never leads tip** (no dual-track archive-ahead / body DONTNEED lead).
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
    let mut class_a_ns = 0u64;
    let mut ensure_ns = 0u64;
    if let Some(plan) = batch.archive_plan.take() {
        if !plan.is_empty() {
            // Shared CreatePin Arcs only (refcount) for post-commit layout fill —
            // no whole-plan packed deep clone of outs.
            let planned_fks = plan.planned_fks.clone();
            let pins: Vec<rbitcoin_query::CreatePin> = if plan.batch_pin.len()
                == plan.planned_fks.len()
            {
                plan.batch_pin.iter().map(std::sync::Arc::clone).collect()
            } else {
                plan.packed
                    .iter()
                    .map(|(pin, _)| std::sync::Arc::clone(pin))
                    .collect()
            };
            let t_ca = Instant::now();
            query
                .archive_commit_plan(plan)
                .map_err(ConsensusError::Store)?;
            class_a_ns = t_ca.elapsed().as_nanos() as u64;
            // Layout only for planned creates (same-batch): pin denserels +
            // body ranges from commit — zero additional Class A body preads.
            let t_ens = Instant::now();
            fill_planned_create_layout_after_commit(
                query,
                &mut batch.batch_parents,
                &planned_fks,
                &pins,
            )?;
            ensure_ns = ensure_ns.saturating_add(t_ens.elapsed().as_nanos() as u64);
        }
    }
    // Ensure denserels/abs for every spend edge before structural + annotate:
    // - prep-ahead in-flight parents (no denserels at pin time)
    // - already-archived Class A (plan=None) same-batch creates never inserted
    // - partial pin after prior write committed Class A then failed annotate
    {
        let t_ens = Instant::now();
        ensure_spend_abs_layouts(query, &mut batch.batch_parents, &batch.prepared)?;
        ensure_ns = ensure_ns.saturating_add(t_ens.elapsed().as_nanos() as u64);
    }
    if class_a_ns > 0 {
        confirm_phase_stats::CLASS_A_NS.fetch_add(class_a_ns, Ordering::Relaxed);
    }
    if ensure_ns > 0 {
        confirm_phase_stats::ENSURE_LAYOUT_NS.fetch_add(ensure_ns, Ordering::Relaxed);
    }

    // Local Instant totals (not atomic deltas) — sample_and_reset races mid-batch.
    // Structural fills meta_by_abs for pure-write annotate (no second body pread).
    let mut meta_by_abs: std::collections::HashMap<u64, (rbitcoin_primitives::Fk, u8)> =
        std::collections::HashMap::new();
    let t_struct = Instant::now();
    let struct_ph = structural_run(
        query,
        params,
        milestone,
        &batch.prepared,
        &batch.wire_blocks,
        &batch.batch_parents,
        &mut meta_by_abs,
    )?;
    let structural_ns = t_struct.elapsed().as_nanos() as u64;

    let n_blocks = batch.prepared.len();
    let t_cc = Instant::now();
    let out = class_c_commit(query, &mut batch.prepared)?;
    let class_c_ns = t_cc.elapsed().as_nanos() as u64;

    let (spend_ann_ns, tip_gc_ns) =
        post_commit(query, &batch.prepared, &batch.batch_parents, &meta_by_abs)?;

    // batch_parents dropped here with ScriptOkBatch — no tip GC of sparse pins.
    confirm_phase_stats::BLOCKS.fetch_add(n_blocks as u64, Ordering::Relaxed);
    confirm_phase_stats::note_last_write(confirm_phase_stats::LastWritePhases {
        n_blocks: n_blocks as u32,
        wall_ns: t_wall.elapsed().as_nanos() as u64,
        class_a_ns,
        ensure_ns,
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

/// After Class A commit, set body_range (+ denserels if missing) for **pinned**
/// planned creates only.
///
/// Uses `tx_body_range_batch` — **no** Class A body pread. Skips creates not in
/// `batch_parents` (most of the batch). Prefer denserels already set at prep pin;
/// missing denserels come from shared [`rbitcoin_query::CreatePin`] (no packed reclone).
fn fill_planned_create_layout_after_commit(
    query: &Query,
    batch_parents: &mut rbitcoin_query::BatchParents,
    planned_fks: &[rbitcoin_primitives::Fk],
    pins: &[rbitcoin_query::CreatePin],
) -> Result<(), ConsensusError> {
    if planned_fks.is_empty() || pins.is_empty() {
        return Ok(());
    }
    // Only parents actually pinned for spends and still missing abs layout.
    let missing: std::collections::HashSet<u64> = batch_parents
        .fks_missing_layout()
        .into_iter()
        .filter_map(|f| f.get())
        .collect();
    if missing.is_empty() {
        return Ok(());
    }
    let mut need_fks: Vec<rbitcoin_primitives::Fk> = Vec::new();
    let mut need_pin_i: Vec<usize> = Vec::new();
    for (i, fk) in planned_fks.iter().enumerate() {
        let Some(id) = fk.get() else { continue };
        if !missing.contains(&id) {
            continue;
        }
        need_fks.push(*fk);
        need_pin_i.push(i);
    }
    if need_fks.is_empty() {
        return Ok(());
    }
    let ranges = query
        .store()
        .tx_body_range_batch(&need_fks)
        .map_err(ConsensusError::Store)?;
    for ((&fk, range), &pi) in need_fks.iter().zip(ranges.into_iter()).zip(need_pin_i.iter())
    {
        let Some((off, len)) = range else {
            continue;
        };
        // Prep already attached sparse denserels: only body_range was missing.
        batch_parents.set_body_range_only(fk, (off, len));
        if batch_parents.has_abs_layout(fk) {
            continue;
        }
        // No denserels on pin yet — use shared CreatePin denserels (no packed reclone).
        let Some(pin) = pins.get(pi) else {
            continue;
        };
        let (_tx, outs, dense_rels) = pin.as_ref();
        if dense_rels.is_empty() && outs.is_empty() {
            continue;
        }
        batch_parents.set_layout(fk, (off, len), dense_rels);
    }
    Ok(())
}

/// Ensure denserels/abs for every spend edge on the write batch.
///
/// Covers residual gaps after pin offline denserels + fill_planned ranges:
/// 1. Prep-ahead parents not yet committed when prepped (body_range missing)
/// 2. Already-archived Class A same-batch creates never pinned
/// 3. Retry after partial write
///
/// **Residency first** (commit denserels seed) — Class A denserels body load only
/// when residency misses. After this function returns, every non-null spend edge
/// **must** have abs layout — no silent leave-for-later / structural cold paper.
fn ensure_spend_abs_layouts(
    query: &Query,
    batch_parents: &mut rbitcoin_query::BatchParents,
    prepared: &[Prepared],
) -> Result<(), ConsensusError> {
    use rbitcoin_store::IdxBodyMode;
    use std::collections::HashMap;

    let mut need: HashMap<u64, Vec<u32>> = HashMap::new();
    for p in prepared {
        for &(_txid, vout, sfk, cfk) in &p.spends {
            if sfk.is_null() || cfk.is_null() {
                continue;
            }
            if batch_parents.get_spender_abs(cfk, vout).is_some() {
                continue;
            }
            if let Some(id) = cfk.get() {
                need.entry(id).or_default().push(vout);
            }
        }
    }
    // Also repair pins that have outs but no layout (structural cold path would
    // skip unpinned; pinned-without-abs fails structural).
    for fk in batch_parents.fks_missing_layout() {
        if let Some(id) = fk.get() {
            need.entry(id).or_default();
        }
    }
    if need.is_empty() {
        return Ok(());
    }
    for vouts in need.values_mut() {
        vouts.sort_unstable();
        vouts.dedup();
    }

    // 1) Residency denserels + body_range (commit seed / prior cold pin) — no body IO.
    // 1a) Batch residency range lookup for pins that already have denserels (range-only gap).
    let mut ensure_res = 0u64;
    let range_gap_fks: Vec<rbitcoin_primitives::Fk> = need
        .keys()
        .filter_map(|&id| {
            let fk = rbitcoin_primitives::Fk(id);
            if batch_parents.has_spender_rels(fk) && !batch_parents.has_abs_layout(fk) {
                Some(fk)
            } else {
                None
            }
        })
        .collect();
    if !range_gap_fks.is_empty() {
        let res_ranges = query.create_residency().body_ranges_by_fk(&range_gap_fks);
        for (fk, opt) in range_gap_fks.iter().zip(res_ranges.into_iter()) {
            if let Some(range) = opt.or_else(|| batch_parents.get_body_range(*fk)) {
                batch_parents.set_body_range_only(*fk, range);
            }
        }
    }

    let mut still: HashMap<u64, Vec<u32>> = HashMap::new();
    // Pin has denserels but still no body_range after residency — idx only (not denserels IO).
    let mut range_only: Vec<rbitcoin_primitives::Fk> = Vec::new();
    for (id, need_v) in &need {
        let fk = rbitcoin_primitives::Fk(*id);
        // Pin already complete for this create — skip.
        if batch_parents.has_abs_layout(fk)
            && (need_v.is_empty()
                || need_v
                    .iter()
                    .all(|&v| batch_parents.get_spender_abs(fk, v).is_some()))
        {
            ensure_res = ensure_res.saturating_add(1);
            continue;
        }
        // Range-only: denserels already on pin — do not cold-load Class A denserels body.
        if batch_parents.has_spender_rels(fk) {
            if batch_parents.has_abs_layout(fk)
                && (need_v.is_empty()
                    || need_v
                        .iter()
                        .all(|&v| batch_parents.get_spender_abs(fk, v).is_some()))
            {
                ensure_res = ensure_res.saturating_add(1);
                continue;
            }
            range_only.push(fk);
            continue;
        }
        if let Some((tx, live, sparse, body_range)) =
            query.create_residency().get_parent_needed(fk, need_v)
        {
            if let Some(range) = body_range {
                if batch_parents.contains(fk) {
                    // One residency probe: sparse denserels already in hand.
                    batch_parents.set_layout_sparse(fk, range, sparse, need_v);
                } else {
                    let cb = if tx.input_count != 1 {
                        Some(false)
                    } else {
                        None
                    };
                    batch_parents.insert_owned(
                        fk,
                        tx,
                        live,
                        need_v.clone(),
                        cb,
                        Some(range),
                        sparse,
                    );
                }
                ensure_res = ensure_res.saturating_add(1);
                continue;
            }
        }
        still.insert(*id, need_v.clone());
    }

    // 1b) Idx body ranges for pin denserels without range (cheap; no denserels body).
    if !range_only.is_empty() {
        range_only.sort_unstable_by_key(|f| f.0);
        range_only.dedup();
        let ranges = query
            .store()
            .tx_body_range_batch(&range_only)
            .map_err(ConsensusError::Store)?;
        for (fk, opt) in range_only.iter().zip(ranges.into_iter()) {
            let Some(range) = opt else {
                // No idx range yet (e.g. parent not committed) — hard fail at post-condition
                // if spend still needs abs; leave for invariant.
                continue;
            };
            batch_parents.set_body_range_only(*fk, range);
            let id = fk.get().unwrap_or(0);
            let need_v = need.get(&id).cloned().unwrap_or_default();
            if batch_parents.has_abs_layout(*fk)
                && (need_v.is_empty()
                    || need_v
                        .iter()
                        .all(|&v| batch_parents.get_spender_abs(*fk, v).is_some()))
            {
                ensure_res = ensure_res.saturating_add(1);
            } else {
                // denserels present but need_v not covered — should not happen if pin sparse
                // was built for need; fall through to cold denserels as last resort.
                still.entry(id).or_insert(need_v);
            }
        }
    }
    confirm_phase_stats::ENSURE_RES_HIT.fetch_add(ensure_res, Ordering::Relaxed);

    // 2) Class A denserels body for remainder only (must not re-load pin denserels hits).
    if !still.is_empty() {
        let fks: Vec<rbitcoin_primitives::Fk> = still
            .keys()
            .map(|id| rbitcoin_primitives::Fk(*id))
            .collect();
        confirm_phase_stats::ENSURE_COLD_N.fetch_add(fks.len() as u64, Ordering::Relaxed);
        // Structural denserels fill for pin gaps — do not seed residency (parents).
        let loaded = rbitcoin_query::load_creates_once_seed(
            query.store(),
            query.create_residency(),
            &fks,
            IdxBodyMode::OutsDenserels,
            false,
        )
        .map_err(ConsensusError::Store)?;
        let secret = query.store().txs.store_secret();
        for c in loaded {
            let Some(id) = c.fk.get() else {
                return Err(ConsensusError::Store(StoreError::Corrupt(
                    "invariant: ensure denserels null create_fk",
                )));
            };
            let need_v = still.get(&id).cloned().unwrap_or_default();
            let (mut tx, outs, dense_rels) = if let Some(dec) = c.decoded_outs {
                dec
            } else {
                // load_creates_once OutsDenserels should always fill decoded_outs.
                rbitcoin_store::decode_packed_tx_outs_with_spender_rels_secret(
                    &c.raw,
                    Some(secret),
                )
                .map_err(|_| {
                    ConsensusError::Store(StoreError::Corrupt(
                        "invariant: ensure denserels decode failed",
                    ))
                })?
            };
            // Ensure path: residency may already hold identity from Class A seed.
            fill_create_txid_from_ram(&mut tx, id, None, query.create_residency());
            if batch_parents.contains(c.fk) {
                batch_parents.set_layout_for_need(c.fk, c.body_range, &dense_rels, &need_v);
                continue;
            }
            // Not pinned at prep (e.g. already-archived same-batch create): insert
            // with layout so annotate/structural abs paths work.
            let mut checked = need_v;
            if checked.is_empty() {
                checked = (0..outs.len() as u32).collect();
            }
            let live: Vec<(u32, rbitcoin_store::OutputRecord)> = checked
                .iter()
                .filter_map(|&v| outs.get(v as usize).map(|o| (v, o.clone())))
                .collect();
            if live.len() != checked.len() {
                return Err(ConsensusError::Store(StoreError::Corrupt(
                    "invariant: ensure denserels incomplete outs for need_vouts",
                )));
            }
            let sparse = rbitcoin_query::sparse_spender_rels(&dense_rels, &checked);
            if !rbitcoin_query::layout_covers_need(Some(c.body_range), &sparse, &checked) {
                return Err(ConsensusError::Store(StoreError::Corrupt(
                    "invariant: ensure denserels incomplete for need_vouts",
                )));
            }
            let cb = if tx.input_count != 1 {
                Some(false)
            } else {
                None
            };
            batch_parents.insert_owned(
                c.fk,
                tx,
                live,
                checked,
                cb,
                Some(c.body_range),
                sparse,
            );
        }
    }

    // Post-condition: every non-null spend edge has abs — no structural cold paper.
    for p in prepared {
        for &(_txid, vout, sfk, cfk) in &p.spends {
            if sfk.is_null() || cfk.is_null() {
                continue;
            }
            if batch_parents.get_spender_abs(cfk, vout).is_none() {
                return Err(ConsensusError::Store(StoreError::Corrupt(
                    "invariant: ensure denserels/abs incomplete for spend edge",
                )));
            }
        }
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

    /// Plan-stage denserels ensure + Forbid pin: cold path must not re-run on prep.
    /// External parents land in plan-local map only (not CreateResidency).
    #[test]
    fn plan_ensure_denserels_then_forbid_skips_cold_io() {
        use super::{
            ensure_external_parent_denserels_from_plan, pin_for_wire_batch, ColdPinMode,
        };
        use rbitcoin_primitives::Fk;
        use rbitcoin_query::Query;
        use rbitcoin_store::{InputRecord, OutputRecord, TxRecord};
        use std::sync::Once;

        static ONCE: Once = Once::new();
        ONCE.call_once(|| {
            if std::env::var_os("RBITCOIN_HEAD_SCALE").is_none() {
                std::env::set_var("RBITCOIN_HEAD_SCALE", "tiny");
            }
        });
        let path = std::env::temp_dir().join(format!(
            "rbitcoin-plan-ensure-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0)
        ));
        let _ = std::fs::remove_dir_all(&path);
        let q = Query::open_or_create(&path).unwrap();

        let parent_tx = TxRecord {
            txid: [0xab; 32],
            version: 1,
            locktime: 0,
            input_start_fk: Fk::NULL,
            input_count: 1,
            output_start_fk: Fk::NULL,
            output_count: 1,
        };
        let parent_outs = vec![OutputRecord::unspent(50_0000_0000, vec![0x51])];
        let parent_ins = vec![InputRecord::coinbase(u32::MAX, vec![0x01], vec![])];
        let pfk = q
            .store()
            .txs
            .put_full_batch_indexed(&[(parent_tx.clone(), parent_ins, parent_outs)], true)
            .unwrap()[0];
        // Parent on disk only — not in residency (ancient / cold external parent).

        // Plan with stamped parent create_fk (plan stage already did batch head).
        let mut plan = rbitcoin_query::ArchiveWritePlan::empty();
        let spend_tx = TxRecord {
            txid: [0xcd; 32],
            version: 1,
            locktime: 0,
            input_start_fk: Fk::NULL,
            input_count: 1,
            output_start_fk: Fk::NULL,
            output_count: 1,
        };
        let spend_outs = vec![OutputRecord::unspent(1, vec![0x51])];
        plan.packed = vec![(
            std::sync::Arc::new((spend_tx, spend_outs, Vec::new())),
            vec![InputRecord {
                prev_txid: parent_tx.txid,
                create_fk: pfk,
                prev_index: 0,
                sequence: u32::MAX,
                script_sig: vec![],
                witness: vec![],
            }],
        )];
        plan.planned_fks = vec![Fk(2)];

        rbitcoin_query::reset_body_ok_reads();
        let res_len_before = q.create_residency().len();
        let st = ensure_external_parent_denserels_from_plan(&q, Some(&mut plan), None).unwrap();
        assert!(st.cold >= 1, "parent missing denserels must cold-load: {st:?}");
        assert!(
            plan.external_parent_outs
                .get(&pfk.get().unwrap())
                .is_some_and(|p| !p.2.is_empty()),
            "ensure must put denserels in plan-local external_parent_outs"
        );
        assert_eq!(
            q.create_residency().len(),
            res_len_before,
            "external parents must not enter CreateResidency"
        );
        let reads_after = rbitcoin_query::body_ok_reads();

        // Second ensure: plan-local already present → no more body IO.
        let st2 = ensure_external_parent_denserels_from_plan(&q, Some(&mut plan), None).unwrap();
        assert!(st2.already >= 1 && st2.cold == 0, "st2={st2:?}");
        assert_eq!(
            rbitcoin_query::body_ok_reads(),
            reads_after,
            "already-warm denserels must not re-read body"
        );

        // Pin Forbid hits plan-local (no extra cold).
        let (parents, _thin, _warm) =
            pin_for_wire_batch(&q, Some(&plan), &[], &[], None, ColdPinMode::Forbid).unwrap();
        assert!(parents.contains(pfk));
        assert_eq!(
            rbitcoin_query::body_ok_reads(),
            reads_after,
            "pin after plan ensure must not cold denserels again"
        );

        let _ = std::fs::remove_dir_all(&path);
    }

    /// Wire pin: spend parent not loadable → hard invariant (no silent skip).
    #[test]
    fn pin_for_wire_missing_parent_is_invariant_error() {
        use super::pin_for_wire_batch;
        use rbitcoin_primitives::Fk;
        use rbitcoin_query::{ArchiveWritePlan, Query};
        use rbitcoin_store::{InputRecord, OutputRecord, TxRecord};
        use std::sync::Once;

        static ONCE: Once = Once::new();
        ONCE.call_once(|| {
            if std::env::var_os("RBITCOIN_HEAD_SCALE").is_none() {
                std::env::set_var("RBITCOIN_HEAD_SCALE", "tiny");
            }
        });
        let path = std::env::temp_dir().join(format!(
            "rbitcoin-pin-wire-inv-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0)
        ));
        std::fs::create_dir_all(&path).unwrap();
        let q = Query::open_or_create(&path).unwrap();
        q.enter_direct_index_mode().unwrap();

        // Plan create spends external create_fk that has no Class A body / residency.
        let missing_parent = Fk(999_999);
        let spend_tx = TxRecord {
            txid: [0xAAu8; 32],
            version: 1,
            locktime: 0,
            input_start_fk: Fk::NULL,
            input_count: 1,
            output_start_fk: Fk::NULL,
            output_count: 1,
        };
        let spend_ins = vec![InputRecord {
            prev_txid: [0xBBu8; 32],
            create_fk: missing_parent,
            prev_index: 0,
            sequence: u32::MAX,
            script_sig: vec![],
            witness: vec![],
        }];
        let spend_outs = vec![OutputRecord::unspent(1, vec![0x51])];
        let plan = ArchiveWritePlan {
            packed: vec![(
                std::sync::Arc::new((spend_tx, spend_outs, Vec::new())),
                spend_ins,
            )],
            planned_fks: vec![Fk(1)],
            per_header_ranges: vec![],
            spends: vec![],
            batch_creates: vec![],
            external_parent_outs: Default::default(),
            external_parent_ranges: Default::default(),
            external_parent_txids: Default::default(),
            batch_pin: vec![],
            index_tx: false,
            body_est: 0,
        };

        let err = pin_for_wire_batch(&q, Some(&plan), &[], &[], None, super::ColdPinMode::Allow)
            .expect_err("missing parent must hard-fail pin");
        let msg = format!("{err}");
        assert!(
            msg.contains("invariant") && msg.contains("wire pin"),
            "unexpected err: {msg}"
        );
        let _ = std::fs::remove_dir_all(&path);
    }

    /// Wire pin: in-flight outs shorter than need → cold miss → hard invariant.
    #[test]
    fn pin_for_wire_incomplete_outs_is_invariant_error() {
        use super::pin_for_wire_batch;
        use rbitcoin_primitives::Fk;
        use rbitcoin_query::{ArchiveWritePlan, Query};
        use rbitcoin_store::{InputRecord, OutputRecord, TxRecord};
        use std::collections::HashMap;
        use std::sync::Once;

        static ONCE: Once = Once::new();
        ONCE.call_once(|| {
            if std::env::var_os("RBITCOIN_HEAD_SCALE").is_none() {
                std::env::set_var("RBITCOIN_HEAD_SCALE", "tiny");
            }
        });
        let path = std::env::temp_dir().join(format!(
            "rbitcoin-pin-wire-outs-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0)
        ));
        std::fs::create_dir_all(&path).unwrap();
        let q = Query::open_or_create(&path).unwrap();
        q.enter_direct_index_mode().unwrap();

        let parent_id = 77u64;
        let parent_fk = Fk(parent_id);
        // Spend needs vout 0 from parent_id.
        let spend_tx = TxRecord {
            txid: [0xCCu8; 32],
            version: 1,
            locktime: 0,
            input_start_fk: Fk::NULL,
            input_count: 1,
            output_start_fk: Fk::NULL,
            output_count: 1,
        };
        let spend_ins = vec![InputRecord {
            prev_txid: [0xDDu8; 32],
            create_fk: parent_fk,
            prev_index: 0,
            sequence: u32::MAX,
            script_sig: vec![],
            witness: vec![],
        }];
        let plan = ArchiveWritePlan {
            packed: vec![(
                std::sync::Arc::new((
                    spend_tx,
                    vec![OutputRecord::unspent(1, vec![0x51])],
                    Vec::new(),
                )),
                spend_ins,
            )],
            planned_fks: vec![Fk(2)],
            per_header_ranges: vec![],
            spends: vec![],
            batch_creates: vec![],
            external_parent_outs: Default::default(),
            external_parent_ranges: Default::default(),
            external_parent_txids: Default::default(),
            batch_pin: vec![],
            index_tx: false,
            body_est: 0,
        };
        // In-flight "parent" with **empty** outs → live.len() != need → cold path;
        // no Class A body either → end pin contract fails.
        let mut ifo = HashMap::new();
        let parent_tx = TxRecord {
            txid: [0xDDu8; 32],
            version: 1,
            locktime: 0,
            input_start_fk: Fk::NULL,
            input_count: 1,
            output_start_fk: Fk::NULL,
            output_count: 0,
        };
        ifo.insert(
            parent_id,
            std::sync::Arc::new((parent_tx, Vec::new(), Vec::new())),
        );

        let err = pin_for_wire_batch(
            &q,
            Some(&plan),
            &[],
            &[],
            Some(&ifo),
            super::ColdPinMode::Allow,
        )
            .expect_err("incomplete outs must hard-fail pin");
        let msg = format!("{err}");
        assert!(
            msg.contains("invariant") && msg.contains("wire pin"),
            "unexpected err: {msg}"
        );
        let _ = std::fs::remove_dir_all(&path);
    }

    /// After wire pin, external full-outs are cleared; sparse BatchParents remain.
    /// Pin uses Arc::clone of CreatePin (no deep outs clone into plan_by_id).
    #[test]
    fn pin_takes_external_create_pin_arc_then_clear_for_write_queue() {
        use super::pin_for_wire_batch;
        use rbitcoin_primitives::Fk;
        use rbitcoin_query::{ArchiveWritePlan, CreatePin, Query};
        use rbitcoin_store::{InputRecord, OutputRecord, TxRecord};
        use std::sync::{Arc, Once};

        static ONCE: Once = Once::new();
        ONCE.call_once(|| {
            if std::env::var_os("RBITCOIN_HEAD_SCALE").is_none() {
                std::env::set_var("RBITCOIN_HEAD_SCALE", "tiny");
            }
        });
        let path = std::env::temp_dir().join(format!(
            "rbitcoin-pin-external-clear-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0)
        ));
        std::fs::create_dir_all(&path).unwrap();
        let q = Query::open_or_create(&path).unwrap();
        q.enter_direct_index_mode().unwrap();

        let parent_id = 1u64;
        let parent_tx = TxRecord {
            txid: [0x11u8; 32],
            version: 1,
            locktime: 0,
            input_start_fk: Fk::NULL,
            input_count: 1,
            output_start_fk: Fk::NULL,
            output_count: 1,
        };
        let parent_outs = vec![OutputRecord::unspent(50_0000_0000, vec![0x51])];
        let dens = rbitcoin_store::denserels_from_packed_records(
            &parent_tx,
            &[InputRecord::coinbase(u32::MAX, vec![0x01], vec![])],
            &parent_outs,
        );
        let external: CreatePin = Arc::new((parent_tx.clone(), parent_outs, dens));

        let spend_tx = TxRecord {
            txid: [0x22u8; 32],
            version: 1,
            locktime: 0,
            input_start_fk: Fk::NULL,
            input_count: 1,
            output_start_fk: Fk::NULL,
            output_count: 1,
        };
        let spend_ins = vec![InputRecord {
            prev_txid: parent_tx.txid,
            create_fk: Fk(parent_id),
            prev_index: 0,
            sequence: u32::MAX,
            script_sig: vec![],
            witness: vec![],
        }];
        let spend_outs = vec![OutputRecord::unspent(1, vec![0x51])];
        let spend_dens =
            rbitcoin_store::denserels_from_packed_records(&spend_tx, &spend_ins, &spend_outs);
        let spend_pin: CreatePin = Arc::new((spend_tx, spend_outs, spend_dens));

        let mut plan = ArchiveWritePlan {
            packed: vec![(Arc::clone(&spend_pin), spend_ins)],
            planned_fks: vec![Fk(2)],
            per_header_ranges: vec![],
            spends: vec![],
            batch_creates: vec![],
            external_parent_outs: {
                let mut m = std::collections::HashMap::new();
                m.insert(parent_id, Arc::clone(&external));
                m
            },
            external_parent_ranges: Default::default(),
            external_parent_txids: Default::default(),
            batch_pin: vec![Arc::clone(&spend_pin)],
            index_tx: false,
            body_est: 0,
        };

        // Map holds the same Arc as our local handle (not a deep clone of outs).
        assert!(Arc::ptr_eq(
            plan.external_parent_outs.get(&parent_id).unwrap(),
            &external
        ));
        let (parents, _thin, _warm) = pin_for_wire_batch(
            &q,
            Some(&plan),
            &[],
            &[],
            None,
            super::ColdPinMode::Forbid,
        )
        .expect("pin external via CreatePin Arc (Forbid — no cold denserels)");
        assert!(parents.contains(Fk(parent_id)));
        assert!(
            parents.get_parent_out(Fk(parent_id), 0).is_some(),
            "sparse need-vout must be in BatchParents"
        );
        // Plan map still the shared Arc until prep clears it.
        assert!(Arc::ptr_eq(
            plan.external_parent_outs.get(&parent_id).unwrap(),
            &external
        ));

        // Production prep drops external after pin so write queue is lean.
        plan.clear_external_parent_outs();
        assert!(
            plan.external_parent_outs.is_empty(),
            "post-pin plan must not carry external full-outs to scripts/write"
        );
        // Sparse pin still holds the need-vout independently of the plan map.
        assert!(parents.get_parent_out(Fk(parent_id), 0).is_some());
        let _ = std::fs::remove_dir_all(&path);
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
        let meta = std::collections::HashMap::new();
        let err = post_commit(&q, &prepared, &bp, &meta).expect_err("missing denserels");
        let msg = format!("{err}");
        assert!(
            msg.contains("invariant") && msg.contains("denserels"),
            "unexpected err: {msg}"
        );
        let _ = std::fs::remove_dir_all(&path);
    }

    /// W3: pin already has denserels — ensure only attaches body_range (no denserels cold).
    #[test]
    fn ensure_range_only_when_pin_has_denserels_skips_cold_body() {
        use super::{ensure_spend_abs_layouts, Prepared};
        use crate::confirm_phase_stats;
        use rbitcoin_primitives::{Fk, Height};
        use rbitcoin_query::{BatchParents, Query};
        use rbitcoin_store::{InputRecord, OutputRecord, TxRecord};
        use std::sync::atomic::Ordering;
        use std::sync::Once;

        static ONCE: Once = Once::new();
        ONCE.call_once(|| {
            if std::env::var_os("RBITCOIN_HEAD_SCALE").is_none() {
                std::env::set_var("RBITCOIN_HEAD_SCALE", "tiny");
            }
        });
        let path = std::env::temp_dir().join(format!(
            "rbitcoin-ensure-range-only-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0)
        ));
        std::fs::create_dir_all(&path).unwrap();
        let q = Query::open_or_create(&path).unwrap();
        q.enter_direct_index_mode().unwrap();

        let parent_tx = TxRecord {
            txid: [0x11u8; 32],
            version: 1,
            locktime: 0,
            input_start_fk: Fk::NULL,
            input_count: 1,
            output_start_fk: Fk::NULL,
            output_count: 1,
        };
        let parent_ins = vec![InputRecord::coinbase(u32::MAX, vec![0x01], vec![])];
        let parent_outs = vec![OutputRecord::unspent(50, vec![0x51])];
        let dens = rbitcoin_store::denserels_from_packed_records(
            &parent_tx,
            &parent_ins,
            &parent_outs,
        );
        let fks = q
            .store()
            .put_tx_full_batch_indexed(
                &[(parent_tx.clone(), parent_ins, parent_outs.clone())],
                /*index=*/ true,
            )
            .unwrap();
        let parent_fk = fks[0];
        let (body_off, body_len) = q.store().txs.body_range(parent_fk).unwrap();

        // Pin denserels without body_range (prep-ahead shape before commit).
        let mut bp = BatchParents::new();
        bp.insert_owned(
            parent_fk,
            parent_tx,
            vec![(0, parent_outs[0].clone())],
            vec![0],
            Some(true),
            None,
            dens.iter().enumerate().map(|(i, r)| (i as u32, *r)).collect(),
        );
        assert!(bp.has_spender_rels(parent_fk));
        assert!(!bp.has_abs_layout(parent_fk));

        let prepared = [Prepared {
            height: Height(1),
            header_fk: Fk(1),
            tx_fks: vec![Fk(2)],
            jobs: vec![],
            spends: vec![([0x11u8; 32], 0, Fk(2), parent_fk)],
            fees: 0,
            check_scripts: false,
            time: 1,
            bits: bitcoin::CompactTarget::from_consensus(0x207f_ffff),
            hash: [4u8; 32],
        }];

        let _ = confirm_phase_stats::ENSURE_COLD_N.swap(0, Ordering::Relaxed);
        let _ = confirm_phase_stats::ENSURE_RES_HIT.swap(0, Ordering::Relaxed);
        ensure_spend_abs_layouts(&q, &mut bp, &prepared).expect("range-only ensure");
        let cold = confirm_phase_stats::ENSURE_COLD_N.swap(0, Ordering::Relaxed);
        assert_eq!(cold, 0, "must not denserels-body cold when pin has denserels");
        assert!(bp.has_abs_layout(parent_fk));
        assert_eq!(
            bp.get_spender_abs(parent_fk, 0),
            Some(body_off.saturating_add(u64::from(dens[0])))
        );
        let _ = body_len;
        let _ = std::fs::remove_dir_all(&path);
    }

    /// Write-stage ensure must hard-fail when denserels/abs cannot be completed
    /// (no silent leave-for structural cold or post_commit).
    #[test]
    fn ensure_spend_abs_incomplete_is_invariant_error() {
        use super::{ensure_spend_abs_layouts, Prepared};
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
            "rbitcoin-ensure-abs-inv-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0)
        ));
        std::fs::create_dir_all(&path).unwrap();
        let q = Query::open_or_create(&path).unwrap();
        q.enter_direct_index_mode().unwrap();

        let prepared = [Prepared {
            height: Height(1),
            header_fk: Fk(1),
            tx_fks: vec![Fk(10)],
            jobs: vec![],
            // Non-null create_fk that does not exist in Class A → cold load miss.
            spends: vec![([9u8; 32], 0, Fk(10), Fk(999_999))],
            fees: 0,
            check_scripts: false,
            time: 1,
            bits: bitcoin::CompactTarget::from_consensus(0x207f_ffff),
            hash: [3u8; 32],
        }];
        let mut bp = BatchParents::new();
        let err = ensure_spend_abs_layouts(&q, &mut bp, &prepared)
            .expect_err("ensure must hard-fail without denserels");
        let msg = format!("{err}");
        assert!(
            msg.contains("invariant")
                && (msg.contains("ensure denserels") || msg.contains("abs incomplete")),
            "unexpected err: {msg}"
        );
        let _ = std::fs::remove_dir_all(&path);
    }

    /// Pin-covered parent without denserels/abs fails structural (no body-range cold).
    #[test]
    fn structural_pinned_without_abs_is_invariant_error() {
        use crate::block::structural_validate_spends;
        use crate::milestone::Milestone;
        use crate::params::ChainParams;
        use bitcoin::absolute::LockTime;
        use bitcoin::block::{Header, Version};
        use bitcoin::hashes::Hash;
        use bitcoin::script::ScriptBuf;
        use bitcoin::transaction::Version as TxVersion;
        use bitcoin::{
            Amount, Block, BlockHash, CompactTarget, OutPoint, Sequence, Transaction, TxIn, TxOut,
            Witness,
        };
        use rbitcoin_primitives::{Fk, Height};
        use rbitcoin_query::{BatchParents, Query};
        use rbitcoin_store::{OutputRecord, TxRecord};
        use std::collections::{HashMap, HashSet};
        use std::sync::Once;

        static ONCE: Once = Once::new();
        ONCE.call_once(|| {
            if std::env::var_os("RBITCOIN_HEAD_SCALE").is_none() {
                std::env::set_var("RBITCOIN_HEAD_SCALE", "tiny");
            }
        });
        let path = std::env::temp_dir().join(format!(
            "rbitcoin-struct-pin-inv-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0)
        ));
        std::fs::create_dir_all(&path).unwrap();
        let q = Query::open_or_create(&path).unwrap();
        q.enter_direct_index_mode().unwrap();
        let params = ChainParams::regtest();

        // Minimal non-empty block (coinbase only) for structural entry.
        let coinbase = Transaction {
            version: TxVersion::ONE,
            lock_time: LockTime::ZERO,
            input: vec![TxIn {
                previous_output: OutPoint::null(),
                script_sig: ScriptBuf::from_bytes(vec![0x00, 0x01]),
                sequence: Sequence::MAX,
                witness: Witness::new(),
            }],
            output: vec![TxOut {
                value: Amount::from_sat(50_0000_0000),
                script_pubkey: ScriptBuf::from_bytes(vec![0x51]),
            }],
        };
        let mut block = Block {
            header: Header {
                version: Version::ONE,
                prev_blockhash: BlockHash::from_byte_array([0u8; 32]),
                merkle_root: bitcoin::TxMerkleNode::from_byte_array([0u8; 32]),
                time: 1_300_000_000,
                bits: CompactTarget::from_consensus(0x207f_ffff),
                nonce: 0,
            },
            txdata: vec![coinbase],
        };
        block.header.merkle_root = block.compute_merkle_root().unwrap();

        // Parent pin present (outs) but denserels/body_range missing → abs None.
        let mut bp = BatchParents::new();
        let parent_fk = Fk(42);
        let tx = TxRecord {
            txid: [7u8; 32],
            version: 1,
            locktime: 0,
            input_start_fk: Fk::NULL,
            input_count: 1,
            output_start_fk: Fk::NULL,
            output_count: 1,
        };
        let out = OutputRecord::unspent(1, vec![0x51]);
        bp.insert_owned(
            parent_fk,
            tx,
            vec![(0, out)],
            vec![0],
            Some(false),
            None, // no body_range
            vec![], // no denserels
        );

        let spends = vec![([7u8; 32], 0u32, Fk(100), parent_fk)];
        let ctx = crate::block::ValidationContext::at(&params, Height(1), Milestone::NONE);
        let mut pending = HashSet::new();
        let mut mtp = HashMap::new();
        let mut meta_by_abs = HashMap::new();
        let err = structural_validate_spends(
            &q,
            &block,
            &ctx,
            None,
            &spends,
            0,
            &mut pending,
            &bp,
            &mut mtp,
            &mut meta_by_abs,
        )
        .expect_err("pinned without abs must be invariant");
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
) -> Result<Vec<Arc<Block>>, ConsensusError> {
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
        blks.push(Arc::new(
            query
                .reconstruct_archived_block_from_parts_cached(
                    m.header_rec.clone(),
                    m.tx_fks.clone(),
                    prev_hash,
                    Some(batch_bodies),
                )
                .map_err(ConsensusError::Store)?,
        ));
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
    wire_blocks: &[Arc<Block>],
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
    wire_blocks: &[Arc<Block>],
    batch_parents: &rbitcoin_query::BatchParents,
    meta_by_abs: &mut std::collections::HashMap<u64, (rbitcoin_primitives::Fk, u8)>,
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
            wire_blocks[i].as_ref(),
            &ctx,
            Some(&p.tx_fks),
            &p.spends,
            p.fees,
            &mut pending_spent,
            batch_parents,
            &mut mtp_cache,
            meta_by_abs,
        )?;
        tot.spent_ns = tot.spent_ns.saturating_add(ph.spent_ns);
        tot.spent_abs_ns = tot.spent_abs_ns.saturating_add(ph.spent_abs_ns);
        tot.spent_strong_ns = tot.spent_strong_ns.saturating_add(ph.spent_strong_ns);
        tot.spent_cold_ns = tot.spent_cold_ns.saturating_add(ph.spent_cold_ns);
        tot.spent_pending_ns = tot.spent_pending_ns.saturating_add(ph.spent_pending_ns);
        tot.create_h_ns = tot.create_h_ns.saturating_add(ph.create_h_ns);
        tot.bip68_ns = tot.bip68_ns.saturating_add(ph.bip68_ns);
    }
    // Window counters (may race with sampler; last-write uses `tot` instead).
    confirm_phase_stats::STRUCTURAL_NS
        .fetch_add(t0.elapsed().as_nanos() as u64, Ordering::Relaxed);
    confirm_phase_stats::STRUCTURAL_SPENT_NS.fetch_add(tot.spent_ns, Ordering::Relaxed);
    confirm_phase_stats::STRUCTURAL_SPENT_ABS_NS.fetch_add(tot.spent_abs_ns, Ordering::Relaxed);
    confirm_phase_stats::STRUCTURAL_SPENT_STRONG_NS
        .fetch_add(tot.spent_strong_ns, Ordering::Relaxed);
    confirm_phase_stats::STRUCTURAL_SPENT_COLD_NS.fetch_add(tot.spent_cold_ns, Ordering::Relaxed);
    confirm_phase_stats::STRUCTURAL_SPENT_PENDING_NS
        .fetch_add(tot.spent_pending_ns, Ordering::Relaxed);
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
///
/// Pure-write annotate: body meta from `meta_by_abs` (structural snapshot);
/// no body pread. Backend from `RBITCOIN_SPEND_ANN` / global `RBITCOIN_IO`.
fn post_commit(
    query: &Query,
    prepared: &[Prepared],
    batch_parents: &rbitcoin_query::BatchParents,
    meta_by_abs: &std::collections::HashMap<u64, (rbitcoin_primitives::Fk, u8)>,
) -> Result<(u64, u64), ConsensusError> {
    // Confirm write (IBD + tip via accept_and_connect → confirm_archived_run):
    // batch durable spend annotations after Class C. Load pin must supply
    // denserels + body_range so every edge has abs layout — one path only.
    let t_spent = Instant::now();
    if query.spend_index_enabled() && query.index_mode().uses_durable_spends() {
        let mut abs_edges: Vec<(
            u64,
            rbitcoin_primitives::Fk,
            u32,
            rbitcoin_primitives::Fk,
        )> = Vec::new();
        let mut known: Vec<(rbitcoin_primitives::Fk, u8)> = Vec::new();
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
                let Some(&(field, flags)) = meta_by_abs.get(&abs) else {
                    return Err(ConsensusError::Store(StoreError::Corrupt(
                        "invariant: spend annotate missing structural meta (cold forbidden)",
                    )));
                };
                abs_edges.push((abs, cfk, vout, sfk));
                known.push((field, flags));
            }
        }
        confirm_phase_stats::SPEND_ANNOTATE_SKIP.fetch_add(n_skip, Ordering::Relaxed);
        if !abs_edges.is_empty() {
            let backend = spend_ann_backend_next();
            let t_ann = Instant::now();
            let cold = query
                .store()
                .put_spend_batch_by_abs_meta_known(&abs_edges, &known, backend)
                .map_err(ConsensusError::Store)?;
            if !cold.is_empty() {
                return Err(ConsensusError::Store(StoreError::Corrupt(
                    "invariant: spend annotate abs cold (OOB or IO); prep/layout bug",
                )));
            }
            let ann_ns = t_ann.elapsed().as_nanos() as u64;
            confirm_phase_stats::SPEND_ANN_NS.fetch_add(ann_ns, Ordering::Relaxed);
            confirm_phase_stats::SPEND_ANN_N
                .fetch_add(abs_edges.len() as u64, Ordering::Relaxed);
            let _ = backend;
            confirm_phase_stats::SPEND_ANNOTATE_RANGED
                .fetch_add(abs_edges.len() as u64, Ordering::Relaxed);
            confirm_phase_stats::SPEND_ANN_PREAD_SKIP
                .fetch_add(abs_edges.len() as u64, Ordering::Relaxed);
        }
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
