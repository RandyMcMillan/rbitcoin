//! Multi-block confirm orchestrator (IBD / tip Class C path).
//!
//! **Primary height-ordered pipeline** (raw wire → validated tip):
//! ```text
//! LOOKUP STAGE (ibd-confirm-lookup OS thread):
//!   wire Block → structure → stamp create_fk (Class A planned only)
//! LOAD STAGE (ibd-confirm-load OS thread):
//!   pin denserels once → assemble (uses intake wire; **no Class-A wire rebuild**)
//! SCRIPTS STAGE (ibd-confirm OS thread + rayon):
//!   pure CPU verify — no Query, no disk
//! WRITE STAGE (ibd-confirm-write OS thread, FIFO):
//!   Class A commit (if plan) + structural + class_c + spend annotate + tip GC
//! ```
//! IBD pipelines lookup(N+1) ∥ load(N) ∥ scripts(N−1) ∥ write(N−2). One Class A appender.
//!
//! [`confirm_wire_run`] is the unified entry (tests / tip / IBD).
//! [`confirm_archived_run`] remains for already-archived Class A only.
//!
//! **Scripts purity:** [`confirm_scripts_phase`] is pure
//! [`LoadedBatch`] → [`ScriptOkBatch`]. IBD uses
//! [`confirm_scripts_phase_async`] / [`confirm_scripts_feed_ahead`] so the
//! rayon pool stays fed across batch boundaries (one-batch lookahead).

use crate::block::{
    assemble_block_prevouts, bip34_height_script, block_has_witness, structural_validate_spends,
    ScriptCheckJob, ValidationContext,
};
use crate::confirm_phase_stats;
use crate::error::ConsensusError;
use crate::header::{median_time_past_times, validate_header};
use crate::milestone::Milestone;
use crate::params::{genesis_block, ChainParams};
use bitcoin::hashes::Hash;
use bitcoin::{Block, Target};
use rbitcoin_primitives::Height;
use rbitcoin_query::{Query, FkMap, U32Map, U64Map, U64Set};
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
    /// Create txids for this block — **exactly one** `compute_txid` per entry
    /// at structure/entry (plan or archived load). Assemble must use these only.
    txids: Vec<[u8; 32]>,
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

/// Pipeline context so lookup(N+1) can run while write(N) has not advanced tip.
///
/// Lookup thread owns reserved create-fk HWM and in-flight creates/outs from
/// batches sitting in load→scripts→write queues. Write remains sole Class A
/// appender and applies batches in height order.
#[derive(Clone, Debug, Default)]
pub struct WireLoadPipeline {
    /// Expected first height of this batch (store tip+1, or last loaded + 1).
    pub path_lo: u32,
    /// Parent of `path_lo` when ahead of store tip (last wire hash of prior loaded batch).
    pub parent_hash: Option<[u8; 32]>,
    /// Inclusive create-fk start for [`Query::archive_plan_batch_from`].
    pub next_tx_start: u64,
    /// Prior uncommitted packs: immutable layer snapshot (no shared mutable map).
    ///
    /// Load looks up create fk / full CreatePin for parents still only in the
    /// pipeline (body-ahead-of-head). Built via [`rbitcoin_query::InFlightLog::snapshot`].
    pub in_flight: rbitcoin_query::InFlightView,
    /// Pipeline-wide sparse parent pin store (Weak map; load get-or-insert only).
    /// Batches hold `Arc` handles so concurrent stages share one payload per create.
    pub parent_store: std::sync::Arc<rbitcoin_query::PipelineParentStore>,
}

/// Wire + assemble complete; script jobs still attached (not yet verified).
///
/// `Send` so IBD can hand off load → scripts threads.
/// Sparse spent-filtered parents ride on the batch (not tip-GCed).
/// When [`archive_plan`] is `Some`, commit stage appends Class A before
/// structural / annotate (single ordered commit era).
pub struct LoadedBatch {
    prepared: Vec<Prepared>,
    /// Shared wire (Arc) so load→scripts→write does not deep-clone full blocks.
    wire_blocks: Vec<Arc<Block>>,
    /// Per-batch pin map: load → assemble → write structural, then drop.
    batch_parents: rbitcoin_query::BatchParents,
    /// Mempool preverified txids for scripts stage (tip follow); empty on IBD.
    script_preverified: ScriptPreverified,
    /// Planned Class A write from wire lookup/load (committed in write stage).
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
    //   • BatchParents holds need-vouts only (rides load→scripts→write queues)
    //   • BatchFullBodies (creates) is used for wire then dropped — not queued
    let t_load = Instant::now();
    let (batch_parents, batch_thin, batch_bodies) =
        load_confirm_batch(query, &heights, &items, batch_end)?;
    let load_ns = t_load.elapsed().as_nanos() as u64;
    confirm_phase_stats::LOAD_NS.fetch_add(load_ns, Ordering::Relaxed);

    let t_resolve = Instant::now();
    let metas = resolve_body_metas(query, blocks)?;
    confirm_phase_stats::RESOLVE_NS
        .fetch_add(t_resolve.elapsed().as_nanos() as u64, Ordering::Relaxed);

    // Wire rebuild needs full create Class A; free it before assemble so the
    // queued LoadedBatch does not retain create full-bodies (only wire blocks).
    let wire_blocks = wire_rebuild(query, &metas, &batch_bodies)?;
    drop(batch_bodies);

    // Sole compute_txid pass for archived confirm (no plan structure stage).
    let mut metas = metas;
    for (m, w) in metas.iter_mut().zip(wire_blocks.iter()) {
        m.txids = w
            .txdata
            .iter()
            .map(|t| t.compute_txid().to_byte_array())
            .collect();
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

/// LOAD STAGE from **raw wire blocks** (unified height-ordered pipeline).
///
/// One-shot path (tests / tip-follow) runs lookup+load together:
/// - Structure / PoW checks, ensure headers
/// - Stamp Class A create fks **without** committing
/// - Pin external parents once (denserels); same-batch from plan
/// - Assemble using **intake wire** (no Class-A wire rebuild)
///
/// The plan rides on [`LoadedBatch::archive_plan`] and is committed in write.
///
/// `pipeline`: when `Some`, first height may be ahead of store tip (lookup(N+1)
/// while write(N) in flight). Use reserved create-fk HWM + in-flight creates.
pub fn confirm_wire_load_phase(
    query: &Query,
    params: &ChainParams,
    milestone: Milestone,
    blocks: &[(Height, Block)],
    preverified: &ScriptPreverified,
) -> Result<ConfirmLoadOutcome, ConsensusError> {
    confirm_wire_load_phase_pipelined(
        query,
        params,
        milestone,
        blocks,
        preverified,
        None,
        ColdPinMode::Allow,
    )
}

/// Like [`confirm_wire_load_phase`] with optional pipeline caches for load-ahead.
///
/// `cold_mode`: IBD load after lookup denserels ensure uses [`ColdPinMode::Forbid`].
pub fn confirm_wire_load_phase_pipelined(
    query: &Query,
    params: &ChainParams,
    milestone: Milestone,
    blocks: &[(Height, Block)],
    preverified: &ScriptPreverified,
    pipeline: Option<&WireLoadPipeline>,
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
        // Sole compute_txid pass for this block in the confirm pipeline.
        let txids = crate::block::validate_block_structure_hashed(block.as_ref(), &ctx)?;
        ns_struct = ns_struct.saturating_add(t.elapsed().as_nanos() as u64);
        // First height must sit at pipeline path_lo (store tip+1, or last loaded+1).
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
            // Prev wire hash already stored on metas[i-1] — no rehash.
            let prev_hash = metas[i - 1].hash;
            if block.header.prev_blockhash.to_byte_array() != prev_hash {
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
            crate::prepare_block_for_archive_with_txids(query, block.as_ref(), &txids)?;
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
        let prev_bytes = block.header.prev_blockhash.to_byte_array();
        query.confirm_parent_cache().put_header_plan(
            height.0,
            header_fk,
            header_rec.clone(),
            Vec::new(),
            prev_bytes,
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
            txids,
        });
    }

    let t_fp = Instant::now();
    let (_header_fks, mut need) = query
        .archive_filter_need_bodies(&mut with_fk)
        .map_err(ConsensusError::Store)?;
    let mut plan = if need.is_empty() {
        for (i, m) in metas.iter_mut().enumerate() {
            if let Some(list) = query
                .store()
                .header_txs
                .get_list(m.header_fk)
                .map_err(ConsensusError::Store)?
            {
                m.tx_fks = list;
            }
            // Index by batch position — never rehash wire for lookup.
            let prev = wire_blocks[i].header.prev_blockhash.to_byte_array();
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
                .archive_plan_batch_from(&mut need, p.next_tx_start.max(1), &p.in_flight)
                .map_err(ConsensusError::Store)?,
            None => query
                .archive_plan_batch_owned(&mut need)
                .map_err(ConsensusError::Store)?,
        };
        let mut by_header: U64Map<Vec<rbitcoin_primitives::Fk>> = U64Map::default();
        for &(hfk, first, n) in &plan.per_header_ranges {
            let Some(hid) = hfk.get() else { continue };
            let start = plan
                .planned_fks
                .iter()
                .position(|f| *f == first)
                .unwrap_or(0);
            let n = n as usize;
            let slice = plan.planned_fks
                [start..start.saturating_add(n).min(plan.planned_fks.len())]
                .to_vec();
            by_header.insert(hid, slice);
        }
        for (i, m) in metas.iter_mut().enumerate() {
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
            let prev = wire_blocks[i].header.prev_blockhash.to_byte_array();
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

    let inflight = pipeline.map(|p| &p.in_flight);
    let parent_store = pipeline.map(|p| &p.parent_store);
    let parent_pin = match plan.as_ref() {
        Some(p) => ParentPinStamp::from_plan(p),
        None => stamp_parent_pin_archived(query, &metas, &wire_blocks, inflight)?,
    };
    let _ = cold_mode;
    let (batch_parents, batch_thin, _warm) = pin_for_wire_batch(
        query,
        plan.as_ref(),
        &parent_pin,
        &metas,
        &wire_blocks,
        inflight,
        parent_store,
    )?;
    // Freeze plan for write: drop external staging maps (sparse BatchParents remains).
    if let Some(ref mut p) = plan {
        p.freeze_after_pin();
    }

    confirm_phase_stats::LOAD_NS.fetch_add(t_load.elapsed().as_nanos() as u64, Ordering::Relaxed);
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

/// Unified wire → tip (lookup+load + scripts + write). Primary production entry.
pub fn confirm_wire_run(
    query: &Query,
    params: &ChainParams,
    milestone: Milestone,
    blocks: &[(Height, Block)],
) -> Result<Vec<rbitcoin_primitives::Fk>, ConsensusError> {
    confirm_wire_run_preverified(query, params, milestone, blocks, &ScriptPreverified::new())
}

/// Like [`confirm_wire_run`] with mempool script preverified set.
///
/// **Tip-follow / one-shot:** lookup stamp (create_fk + parent body ranges;
/// never `tx.body`) → load pin denserels by range → scripts → write.
///
/// Parent create_fk + body_range + identity are **lookup promises**. Load only
/// reads `tx.body` denserels. Soft spentness recovery for wrong pin identity
/// is not a substitute for a correct lookup/load.
pub fn confirm_wire_run_preverified(
    query: &Query,
    params: &ChainParams,
    milestone: Milestone,
    blocks: &[(Height, Block)],
    preverified: &ScriptPreverified,
) -> Result<Vec<rbitcoin_primitives::Fk>, ConsensusError> {
    if blocks.is_empty() {
        return Err(ConsensusError::BadBlock("empty confirm batch"));
    }
    let arcs: Vec<(Height, Arc<Block>)> = blocks
        .iter()
        .map(|(h, b)| (*h, Arc::new(b.clone())))
        .collect();
    // Lookup: structure + stamp create_fk + parent ranges (no body denserels).
    let stamped = confirm_wire_lookup_stamp(query, params, milestone, &arcs, None)?;
    let mat = confirm_wire_load_from_plan(
        query,
        params,
        milestone,
        stamped,
        None,
        preverified,
        ColdPinMode::Forbid, // legacy arg; load denserels by range only
    )?;
    let ok = confirm_scripts_phase(mat.batch)?;
    confirm_write_phase(query, params, milestone, ok.batch)
}

/// Whether wire pin may cold-load denserels from Class A body.
///
/// IBD **lookup** stage ensures external-parent denserels into **plan-local**
/// state. Load then uses [`ColdPinMode::Forbid`] so cold denserels is never
/// duplicated on the load thread. Tests / one-shot [`confirm_wire_load_phase`]
/// use [`ColdPinMode::Allow`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColdPinMode {
    /// Plan-local miss → `load_creates_once` cold denserels (tests / Allow pin).
    Allow,
    /// Miss after lookup ensure → hard `invariant: denserels stage miss` (load after lookup).
    Forbid,
}

/// Stats from lookup-stage denserels ensure (external parents → plan-local).
#[derive(Debug, Default, Clone, Copy)]
pub struct DenserelsWarmStats {
    /// Unique external parent creates considered (stamped create_fk, not same-batch).
    pub parents: u32,
    /// Already had denserels in plan.external_parent_outs or in-flight.
    pub already: u32,
    /// Cold denserels body loads (into plan-local only).
    pub cold: u32,
    /// Same-batch plan creates (offline denserels at pin).
    pub same_batch: u32,
    pub work_ns: u64,
}

/// External parents only: one `OutsDenserels` cold load into
/// **`plan.external_parent_outs`** (pipeline-local).
///
/// Parent create_fks come from **plan-stamped** inputs (and in-flight). No head
/// resolve here — plan already stamped via batch + head. Same-batch creates are
/// skipped (pin uses offline denserels).
///
/// Load pin with [`ColdPinMode::Forbid`] must see every external parent covered
/// via plan-local map, in-flight, or same-batch.
pub fn ensure_external_parent_denserels_from_plan(
    query: &Query,
    plan: Option<&mut rbitcoin_query::ArchiveWritePlan>,
    in_flight: Option<&rbitcoin_query::InFlightView>,
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
    let mut batch_create_ids: U64Map<()> = U64Map::default();
    for fk in &plan.planned_fks {
        if let Some(id) = fk.get() {
            batch_create_ids.insert(id, ());
        }
    }

    // Spent parent create_fk → need vouts (from stamped inputs only).
    // Also fill reverse map from wire prev_txid (lookup stamp may have omitted
    // when tests build synthetic plans).
    let mut parent_vouts: U64Map<Vec<u32>> = U64Map::default();
    let t_collect = Instant::now();
    for ((_pin, ins), _) in plan.packed.iter().zip(plan.planned_fks.iter()) {
        for inp in ins {
            if inp.is_coinbase() || inp.prev_index == u32::MAX {
                continue;
            }
            if let Some(pid) = inp.create_fk.get() {
                parent_vouts.entry(pid).or_default().push(inp.prev_index);
                if inp.prev_txid != [0u8; 32] {
                    plan.external_parent_txids
                        .entry(pid)
                        .or_insert(inp.prev_txid);
                }
            }
        }
    }
    for vouts in parent_vouts.values_mut() {
        vouts.sort_unstable();
        vouts.dedup();
    }
    let collect_ns = t_collect.elapsed().as_nanos() as u64;

    let mut cold_fks: Vec<rbitcoin_primitives::Fk> = Vec::new();
    for (id, _need) in &parent_vouts {
        if batch_create_ids.contains_key(id) {
            st.same_batch = st.same_batch.saturating_add(1);
            continue;
        }
        st.parents = st.parents.saturating_add(1);
        let fk = rbitcoin_primitives::Fk(*id);
        // Plan-local external parent already loaded (sparse need denserels).
        if plan
            .external_parent_outs
            .get(id)
            .is_some_and(|pin| !pin.1.is_empty() || !pin.2.is_empty())
        {
            st.already = st.already.saturating_add(1);
            continue;
        }
        // In-flight offline denserels already available for pin.
        if let Some(ifo) = in_flight {
            if ifo.get_out(*id).is_some_and(|pin| !pin.2.is_empty()) {
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
        // Prefer plan stamp body ranges (skip tx.idx) — sparse need denserels.
        let mut by_range: Vec<(rbitcoin_primitives::Fk, (u64, u64), [u8; 32], Vec<u32>)> =
            Vec::new();
        let mut need_idx: Vec<rbitcoin_primitives::Fk> = Vec::new();
        for fk in &cold_fks {
            let id = fk.get().unwrap_or(0);
            if let Some(&range) = plan.external_parent_ranges.get(&id) {
                // ensure is load-prep body denserels: identity from plan stamp only.
                let tid = known_create_txid_load(id, Some(plan))?;
                let need = parent_vouts.get(&id).cloned().unwrap_or_default();
                by_range.push((*fk, range, tid, need));
            } else {
                need_idx.push(*fk);
            }
        }
        if !by_range.is_empty() {
            let n_range = by_range.len() as u64;
            let (decoded, body_ns, dec_ns) = query
                .store()
                .get_outs_denserels_by_range_batch(&by_range)
                .map_err(ConsensusError::Store)?;
            let rng_ns = body_ns.saturating_add(dec_ns);
            if rng_ns > 0 {
                confirm_load_stats::COLD_RANGE_NS.fetch_add(rng_ns, Ordering::Relaxed);
            }
            if body_ns > 0 {
                confirm_load_stats::COLD_RANGE_BODY_NS.fetch_add(body_ns, Ordering::Relaxed);
            }
            if dec_ns > 0 {
                confirm_load_stats::COLD_RANGE_DECODE_NS.fetch_add(dec_ns, Ordering::Relaxed);
            }
            confirm_load_stats::COLD_RANGE_N.fetch_add(n_range, Ordering::Relaxed);
            // Keep sparse need-vouts only — no full output_count dense expand
            // (AGENTS prefer-immutable / avoid wasteful mutable bag growth).
            for ((_fk, _range, _tid, need), row) in by_range.into_iter().zip(decoded.into_iter()) {
                let Some(id) = _fk.get() else {
                    continue;
                };
                let Some((tx, live, sparse)) = row else {
                    continue;
                };
                let _ = need;
                plan.external_parent_outs
                    .insert(id, std::sync::Arc::new((tx, live, sparse)));
            }
            confirm_load_stats::BODY_TX_READS.fetch_add(n_range, Ordering::Relaxed);
            confirm_load_stats::PIN_NEW.fetch_add(n_range, Ordering::Relaxed);
        }
        // Fallback: idx→body denserels (no plan range).
        if !need_idx.is_empty() {
            let t_idx = Instant::now();
            let loaded = rbitcoin_query::load_creates_once(
                query.store(),
                &need_idx,
                IdxBodyMode::OutsDenserels,
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
                            "invariant: lookup stage external parent denserels decode failed",
                        ))
                    })?
                };
                fill_create_txid_load(&mut tx, id, Some(plan))?;
                // Sparse need only — drop full dense outs after selecting need vouts.
                let need = parent_vouts.get(&id).cloned().unwrap_or_default();
                let live: Vec<(u32, rbitcoin_store::OutputRecord)> = if need.is_empty() {
                    outs.into_iter()
                        .enumerate()
                        .map(|(i, o)| (i as u32, o))
                        .collect()
                } else {
                    need.iter()
                        .filter_map(|&v| outs.get(v as usize).map(|o| (v, o.clone())))
                        .collect()
                };
                let sparse = if need.is_empty() {
                    dens.into_iter()
                        .enumerate()
                        .filter(|(_, r)| *r != rbitcoin_query::SPENDER_REL_UNKNOWN)
                        .map(|(i, r)| (i as u32, r))
                        .collect()
                } else {
                    rbitcoin_query::sparse_spender_rels(&dens, &need)
                };
                plan.external_parent_outs
                    .insert(id, std::sync::Arc::new((tx, live, sparse)));
            }
        }
        cold_io_ns = t_io.elapsed().as_nanos() as u64;
        if cold_io_ns > 0 {
            confirm_load_stats::COLD_IO_NS.fetch_add(cold_io_ns, Ordering::Relaxed);
            confirm_load_stats::PIN_NEW_META_NS.fetch_add(cold_io_ns, Ordering::Relaxed);
        }
        // Completeness: every cold parent must be plan-local sparse pin.
        for fk in &cold_fks {
            let id = fk.get().unwrap_or(0);
            if plan
                .external_parent_outs
                .get(&id)
                .is_none_or(|pin| pin.1.is_empty() && pin.2.is_empty())
            {
                return Err(ConsensusError::Store(StoreError::Corrupt(
                    "invariant: lookup stage failed to load external parent denserels",
                )));
            }
        }
    }

    st.work_ns = t0.elapsed().as_nanos() as u64;
    // Parent mix + subtimers; wall TOTAL_NS is owned by lookup stage caller.
    lookup_stage_stats::note(
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
        confirm_load_stats::PIN_CACHE_BODY.fetch_add(st.already as u64, Ordering::Relaxed);
    }
    Ok(st)
}

/// Lookup-stamped external parent material for load body denserels.
///
/// **Lookup** fills this via `tx.head` / `tx.idx` / `txid.body` (never `tx.body`).
/// **Load** denserels by range using only these maps (+ plan offline pins).
/// Integer create_fk maps use [`U64Map`] (identity hasher) — pack-scale win over SipHash.
#[derive(Debug, Default, Clone)]
pub struct ParentPinStamp {
    /// create_fk_id → Class A body range.
    pub ranges: U64Map<(u64, u64)>,
    /// create_fk_id → create txid (wire / sidefile at lookup).
    pub txids: U64Map<[u8; 32]>,
    /// prev_txid → create_fk_id (plan=None thin edges without head on load).
    pub create_by_txid: HashMap<[u8; 32], u64>,
}

impl ParentPinStamp {
    pub(crate) fn from_plan(plan: &rbitcoin_query::ArchiveWritePlan) -> Self {
        let mut create_by_txid = HashMap::with_capacity(plan.external_parent_txids.len());
        for (id, tid) in &plan.external_parent_txids {
            create_by_txid.insert(*tid, *id);
        }
        // Plan + stamp both use U64Map (identity hasher) for dense create_fk keys.
        Self {
            ranges: plan.external_parent_ranges.clone(),
            txids: plan.external_parent_txids.clone(),
            create_by_txid,
        }
    }

    #[inline]
    fn create_txid(&self, create_fk_id: u64) -> Option<[u8; 32]> {
        self.txids.get(&create_fk_id).copied().filter(|t| *t != [0u8; 32])
    }
}

/// Lookup-stage output: structure + plan batch (create_fk + parent body ranges).
///
/// **No `tx.body` denserels on lookup.** Load denserels by range from
/// [`ParentPinStamp`] / plan ranges. Handoff is owned plan + parent pin stamp.
pub struct PlanStampOutcome {
    pub plan: Option<rbitcoin_query::ArchiveWritePlan>,
    /// External parent fk/range/txid stamped at lookup (always; including plan=None).
    pub parent_pin: ParentPinStamp,
    /// Wall ns for structure + plan_batch (head stamp).
    pub work_ns: u64,
    metas: Vec<BodyMeta>,
    wire_blocks: Vec<Arc<Block>>,
}

/// IBD **lookup** stage: structure + stamp create_fk + parent body ranges.
///
/// May read `tx.head`, `tx.idx`, `txid.body`. **Never** denserels-decode `tx.body`.
/// Wire blocks are `Arc` so IBD resolve can decode once and hand off without
/// cloning full `Block` payloads into stamp.
pub fn confirm_wire_lookup_stamp(
    query: &Query,
    params: &ChainParams,
    milestone: Milestone,
    blocks: &[(Height, Arc<Block>)],
    pipeline: Option<&WireLoadPipeline>,
) -> Result<PlanStampOutcome, ConsensusError> {
    let t0 = Instant::now();
    let (plan, metas, wire_blocks, plan_ns) =
        wire_lookup_phase(query, params, milestone, blocks, pipeline)?;
    let ifo = pipeline.map(|p| &p.in_flight);
    let parent_pin = match plan.as_ref() {
        Some(p) => ParentPinStamp::from_plan(p),
        None => stamp_parent_pin_archived(query, &metas, &wire_blocks, ifo)?,
    };
    lookup_stage_stats::BLOCKS.fetch_add(blocks.len() as u64, Ordering::Relaxed);
    lookup_stage_stats::HEAD_NS.fetch_add(plan_ns, Ordering::Relaxed);
    let work_ns = t0.elapsed().as_nanos() as u64;
    lookup_stage_stats::TOTAL_NS.fetch_add(work_ns, Ordering::Relaxed);
    Ok(PlanStampOutcome {
        plan,
        parent_pin,
        work_ns,
        metas,
        wire_blocks,
    })
}

/// plan=None rehydrate: stamp external parent create_fk + body_range + txid
/// via head/idx/txid.body so load never probes those tables.
fn stamp_parent_pin_archived(
    query: &Query,
    metas: &[BodyMeta],
    wire_blocks: &[Arc<Block>],
    in_flight: Option<&rbitcoin_query::InFlightView>,
) -> Result<ParentPinStamp, ConsensusError> {
    let mut same_batch: HashMap<[u8; 32], u64> = HashMap::new();
    for m in metas {
        for (tid, fk) in m.txids.iter().zip(m.tx_fks.iter()) {
            if let Some(id) = fk.get() {
                same_batch.insert(*tid, id);
            }
        }
    }
    let mut need_external: HashMap<[u8; 32], ()> = HashMap::new();
    for (m, block) in metas.iter().zip(wire_blocks.iter()) {
        let _ = m;
        for tx in &block.txdata {
            for inp in &tx.input {
                if inp.previous_output.is_null() {
                    continue;
                }
                let prev = inp.previous_output.txid.to_byte_array();
                if same_batch.contains_key(&prev) {
                    continue;
                }
                if prev != [0u8; 32] {
                    need_external.insert(prev, ());
                }
            }
        }
    }
    let mut stamp = ParentPinStamp::default();
    for (tid, id) in &same_batch {
        stamp.create_by_txid.insert(*tid, *id);
        stamp.txids.insert(*id, *tid);
        // same-batch denserels offline at pin — range optional
    }
    let mut need_head: Vec<[u8; 32]> = Vec::new();
    for tid in need_external.keys() {
        if let Some(ifo) = in_flight {
            if let Some(fk) = ifo.get_create_fk(tid) {
                if let Some(id) = fk.get() {
                    stamp.create_by_txid.insert(*tid, id);
                    stamp.txids.insert(id, *tid);
                    if ifo.get_out(id).is_none() {
                        // body on disk expected — fill range below
                        need_head.push(*tid); // reuse batch for range via head
                    }
                    continue;
                }
            }
        }
        need_head.push(*tid);
    }
    // Dedup need_head after mixed in_flight path.
    need_head.sort_unstable();
    need_head.dedup();
    // Drop txs already fully stamped with range from a prior head fill.
    need_head.retain(|t| {
        stamp
            .create_by_txid
            .get(t)
            .map(|id| !stamp.ranges.contains_key(id))
            .unwrap_or(true)
    });
    if !need_head.is_empty() {
        need_head.sort_unstable_by_key(|txid| query.store().txs.head_primary_slot(txid));
        let hits = query
            .store()
            .get_fk_by_txid_batch(&need_head)
            .map_err(ConsensusError::Store)?;
        for (txid, row) in hits {
            if let Some((fk, range)) = row {
                if let Some(id) = fk.get() {
                    stamp.create_by_txid.insert(txid, id);
                    stamp.txids.insert(id, txid);
                    stamp.ranges.insert(id, range);
                }
            }
        }
    }
    // Any create_fk without range and without offline denserels outs: idx body_range.
    // Includes same-batch already-archived creates (plan=None has no CreatePin offline).
    let mut need_range: Vec<rbitcoin_primitives::Fk> = Vec::new();
    let mut seen = HashSet::new();
    for (&id, _) in &stamp.txids {
        if stamp.ranges.contains_key(&id) {
            continue;
        }
        if in_flight.and_then(|i| i.get_out(id)).is_some() {
            continue;
        }
        if seen.insert(id) {
            need_range.push(rbitcoin_primitives::Fk(id));
        }
    }
    if !need_range.is_empty() {
        let ranges = query
            .store()
            .tx_body_range_batch(&need_range)
            .map_err(ConsensusError::Store)?;
        for (fk, row) in need_range.into_iter().zip(ranges.into_iter()) {
            let Some(id) = fk.get() else { continue };
            let Some(range) = row else {
                return Err(ConsensusError::Store(StoreError::Corrupt(
                    "archive: plan=None parent body_range missing after create_fk stamp",
                )));
            };
            stamp.ranges.insert(id, range);
        }
    }
    // Identity fallback: sidefile for any create still missing txid (should be rare).
    for (&id, tid) in stamp.txids.iter_mut() {
        if *tid == [0u8; 32] {
            *tid = known_create_txid_lookup(query, id, None)?;
        }
        let _ = id;
    }
    Ok(stamp)
}

/// IBD **load** after lookup denserels ensure: pin + assemble.
///
/// Uses the owned stamped plan — does **not** re-run plan_batch / head resolve.
///
/// **Cold policy:**
/// - [`ColdPinMode::Forbid`] after denserels ensure when a Class A plan is present
///   (plan-local / range denserels cover external parents).
/// - **Already-archived** (`plan=None`): cold denserels are required (no plan-local
///   pin material). Callers may pass Forbid, but this function **forces Allow** so
///   rehydrate/tip+1 Class A bodies do not hard-fail with
///   `invariant: lookup stage miss`. Parent create identity still comes from
///   dense `txid.body` (schema-13).
pub fn confirm_wire_load_from_plan(
    query: &Query,
    params: &ChainParams,
    milestone: Milestone,
    stamped: PlanStampOutcome,
    pipeline: Option<&WireLoadPipeline>,
    preverified: &ScriptPreverified,
    cold_mode: ColdPinMode,
) -> Result<ConfirmLoadOutcome, ConsensusError> {
    let t_work = Instant::now();
    let t_load = Instant::now();
    let PlanStampOutcome {
        mut plan,
        parent_pin,
        metas,
        wire_blocks,
        ..
    } = stamped;

    // Load denserels by body range from parent_pin (lookup stamped). Never head/idx.
    let _ = cold_mode;

    let ifo = pipeline.map(|p| &p.in_flight);
    let parent_store = pipeline.map(|p| &p.parent_store);
    let (batch_parents, batch_thin, _warm) = pin_for_wire_batch(
        query,
        plan.as_ref(),
        &parent_pin,
        &metas,
        &wire_blocks,
        ifo,
        parent_store,
    )?;
    // Freeze plan for write: drop external staging; sparse BatchParents remains.
    if let Some(ref mut p) = plan {
        p.freeze_after_pin();
    }

    confirm_phase_stats::LOAD_NS.fetch_add(t_load.elapsed().as_nanos() as u64, Ordering::Relaxed);

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
pub fn confirm_wire_lookup_and_ensure_denserels(
    query: &Query,
    params: &ChainParams,
    milestone: Milestone,
    blocks: &[(Height, Arc<Block>)],
    pipeline: Option<&WireLoadPipeline>,
) -> Result<
    (
        Option<rbitcoin_query::ArchiveWritePlan>,
        DenserelsWarmStats,
        u64,
    ),
    ConsensusError,
> {
    let t0 = Instant::now();
    let (mut plan, _metas, _wire, plan_ns) =
        wire_lookup_phase(query, params, milestone, blocks, pipeline)?;
    lookup_stage_stats::BLOCKS.fetch_add(blocks.len() as u64, Ordering::Relaxed);
    lookup_stage_stats::HEAD_NS.fetch_add(plan_ns, Ordering::Relaxed);

    let ifo = pipeline.map(|p| &p.in_flight);
    let warm = ensure_external_parent_denserels_from_plan(query, plan.as_mut(), ifo)?;
    let work_ns = t0.elapsed().as_nanos() as u64;
    lookup_stage_stats::TOTAL_NS.fetch_add(work_ns, Ordering::Relaxed);
    Ok((plan, warm, work_ns))
}

/// Structure + prepare + plan_batch only (stamp create_fk). Shared by lookup stage.
fn wire_lookup_phase(
    query: &Query,
    params: &ChainParams,
    milestone: Milestone,
    blocks: &[(Height, Arc<Block>)],
    pipeline: Option<&WireLoadPipeline>,
) -> Result<
    (
        Option<rbitcoin_query::ArchiveWritePlan>,
        Vec<BodyMeta>,
        Vec<Arc<Block>>,
        u64, // plan wall ns (filter+plan_batch dominate)
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

    // Stamp sub-walls (structure + prepare summed over batch; plan batch below).
    let mut struct_ns = 0u64;
    let mut prepare_ns = 0u64;

    for (i, (height, block)) in blocks.iter().enumerate() {
        let block = Arc::clone(block);
        let hash = block.block_hash().to_byte_array();
        let ctx = ValidationContext::at(params, *height, milestone);
        let t_struct = Instant::now();
        // Sole compute_txid pass for this block in the confirm pipeline.
        let txids = crate::block::validate_block_structure_hashed(block.as_ref(), &ctx)?;
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
            // Prev wire hash already on metas[i-1] — no rehash.
            let prev_hash = metas[i - 1].hash;
            if block.header.prev_blockhash.to_byte_array() != prev_hash {
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
        // Reuse structure txids — no second hash in prepare.
        let (header_rec, txs) =
            crate::prepare_block_for_archive_with_txids(query, block.as_ref(), &txids)?;
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
        let prev_bytes = block.header.prev_blockhash.to_byte_array();
        query.confirm_parent_cache().put_header_plan(
            height.0,
            header_fk,
            header_rec.clone(),
            Vec::new(),
            prev_bytes,
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
            txids,
        });
    }

    let t_filter = Instant::now();
    let (_header_fks, mut need) = query
        .archive_filter_need_bodies(&mut with_fk)
        .map_err(ConsensusError::Store)?;
    let filter_ns = t_filter.elapsed().as_nanos() as u64;
    let t_batch = Instant::now();
    let plan = if need.is_empty() {
        for (i, m) in metas.iter_mut().enumerate() {
            if let Some(list) = query
                .store()
                .header_txs
                .get_list(m.header_fk)
                .map_err(ConsensusError::Store)?
            {
                m.tx_fks = list;
            }
            // Index by batch position — never rehash wire for lookup.
            let prev = wire_blocks[i].header.prev_blockhash.to_byte_array();
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
                .archive_plan_batch_from(&mut need, p.next_tx_start.max(1), &p.in_flight)
                .map_err(ConsensusError::Store)?,
            None => query
                .archive_plan_batch_owned(&mut need)
                .map_err(ConsensusError::Store)?,
        };
        let mut by_header: U64Map<Vec<rbitcoin_primitives::Fk>> = U64Map::default();
        for &(hfk, first, n) in &plan.per_header_ranges {
            let Some(hid) = hfk.get() else { continue };
            let start = plan
                .planned_fks
                .iter()
                .position(|f| *f == first)
                .unwrap_or(0);
            let n = n as usize;
            let slice = plan.planned_fks
                [start..start.saturating_add(n).min(plan.planned_fks.len())]
                .to_vec();
            by_header.insert(hid, slice);
        }
        for (i, m) in metas.iter_mut().enumerate() {
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
            let prev = wire_blocks[i].header.prev_blockhash.to_byte_array();
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
    let batch_ns = t_batch.elapsed().as_nanos() as u64;
    // plan_ns for HEAD_NS: filter + batch (legacy “lookup wall” without struct/prepare).
    let plan_ns = filter_ns.saturating_add(batch_ns);
    plan_stamp_sub_stats::note_last(
        blocks.len() as u64,
        struct_ns,
        prepare_ns,
        filter_ns,
        batch_ns,
    );
    Ok((plan, metas, wire_blocks, plan_ns))
}

/// Stamp-phase sub-walls for lookup_thr diagnosis (structure / prepare / filter / batch).
///
/// Batch is the archive plan_batch wall (assign+collect+res+head_fk+head_dens+stamp+finish
/// already timed in `archive_phase_stats`). `head_fk` = get_fk_by_txid_batch;
/// `head_dens` = plan-time external-parent denserels load; `head` = sum.
///
/// Last-batch fields (overwrite) power slow-plan logs; window sum is still
/// [`sample_and_reset`].
pub mod plan_stamp_sub_stats {
    use std::sync::atomic::{AtomicU64, Ordering};

    static STRUCT_NS: AtomicU64 = AtomicU64::new(0);
    static PREPARE_NS: AtomicU64 = AtomicU64::new(0);
    static FILTER_NS: AtomicU64 = AtomicU64::new(0);
    static BATCH_NS: AtomicU64 = AtomicU64::new(0);
    static LAST_STRUCT_NS: AtomicU64 = AtomicU64::new(0);
    static LAST_PREPARE_NS: AtomicU64 = AtomicU64::new(0);
    static LAST_FILTER_NS: AtomicU64 = AtomicU64::new(0);
    static LAST_BATCH_NS: AtomicU64 = AtomicU64::new(0);
    static LAST_N_BLOCKS: AtomicU64 = AtomicU64::new(0);

    pub fn note(struct_ns: u64, prepare_ns: u64, filter_ns: u64, batch_ns: u64) {
        if struct_ns > 0 {
            STRUCT_NS.fetch_add(struct_ns, Ordering::Relaxed);
        }
        if prepare_ns > 0 {
            PREPARE_NS.fetch_add(prepare_ns, Ordering::Relaxed);
        }
        if filter_ns > 0 {
            FILTER_NS.fetch_add(filter_ns, Ordering::Relaxed);
        }
        if batch_ns > 0 {
            BATCH_NS.fetch_add(batch_ns, Ordering::Relaxed);
        }
    }

    /// Record last stamp sub-walls for one plan batch (slow-plan logs).
    pub fn note_last(
        n_blocks: u64,
        struct_ns: u64,
        prepare_ns: u64,
        filter_ns: u64,
        batch_ns: u64,
    ) {
        note(struct_ns, prepare_ns, filter_ns, batch_ns);
        LAST_N_BLOCKS.store(n_blocks, Ordering::Relaxed);
        LAST_STRUCT_NS.store(struct_ns, Ordering::Relaxed);
        LAST_PREPARE_NS.store(prepare_ns, Ordering::Relaxed);
        LAST_FILTER_NS.store(filter_ns, Ordering::Relaxed);
        LAST_BATCH_NS.store(batch_ns, Ordering::Relaxed);
    }

    #[derive(Debug, Default, Clone, Copy)]
    pub struct Sample {
        pub struct_ns: u64,
        pub prepare_ns: u64,
        pub filter_ns: u64,
        pub batch_ns: u64,
    }

    pub fn sample_and_reset() -> Sample {
        Sample {
            struct_ns: STRUCT_NS.swap(0, Ordering::Relaxed),
            prepare_ns: PREPARE_NS.swap(0, Ordering::Relaxed),
            filter_ns: FILTER_NS.swap(0, Ordering::Relaxed),
            batch_ns: BATCH_NS.swap(0, Ordering::Relaxed),
        }
    }

    /// Last stamp batch (not consumed by sample_and_reset).
    #[derive(Debug, Default, Clone, Copy)]
    pub struct LastStamp {
        pub n_blocks: u32,
        pub struct_ns: u64,
        pub prepare_ns: u64,
        pub filter_ns: u64,
        pub batch_ns: u64,
    }

    impl LastStamp {
        #[inline]
        pub fn ms(ns: u64) -> u64 {
            ns / 1_000_000
        }
    }

    pub fn last_stamp() -> LastStamp {
        LastStamp {
            n_blocks: LAST_N_BLOCKS.load(Ordering::Relaxed) as u32,
            struct_ns: LAST_STRUCT_NS.load(Ordering::Relaxed),
            prepare_ns: LAST_PREPARE_NS.load(Ordering::Relaxed),
            filter_ns: LAST_FILTER_NS.load(Ordering::Relaxed),
            batch_ns: LAST_BATCH_NS.load(Ordering::Relaxed),
        }
    }
}

/// Accumulators for the **lookup** pipeline stage (plan+stamp + denserels ensure).
pub mod lookup_stage_stats {
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

/// Create identity for **load** pin denserels: plan stamp reverse map only.
///
/// **Load never reads `txid.body`.** Lookup stamps `external_parent_txids` from
/// wire `prev_txid` (or lookup-side `txid.body` for plan=None rehydrate). Missing
/// identity here is a lookup miss, not a sidefile fallback.
#[inline]
fn known_create_txid_load(
    create_fk_id: u64,
    plan: Option<&rbitcoin_query::ArchiveWritePlan>,
) -> Result<[u8; 32], ConsensusError> {
    if let Some(p) = plan {
        if let Some(tid) = p.external_parent_txid(create_fk_id) {
            if tid != [0u8; 32] {
                return Ok(tid);
            }
        }
    }
    Err(ConsensusError::Store(StoreError::Corrupt(
        "invariant: lookup stage miss (load parent create identity not stamped)",
    )))
}

/// Lookup-side identity fill: plan RAM first, else `txid.body` (lookup may read
/// the sidefile; load must not call this).
#[inline]
fn known_create_txid_lookup(
    query: &Query,
    create_fk_id: u64,
    plan: Option<&rbitcoin_query::ArchiveWritePlan>,
) -> Result<[u8; 32], ConsensusError> {
    if let Some(p) = plan {
        if let Some(tid) = p.external_parent_txid(create_fk_id) {
            if tid != [0u8; 32] {
                return Ok(tid);
            }
        }
    }
    let tid = query
        .store()
        .txs
        .body_txid(rbitcoin_primitives::Fk(create_fk_id))
        .map_err(ConsensusError::Store)?;
    if tid == [0u8; 32] {
        return Err(ConsensusError::Store(StoreError::Corrupt(
            "invariant: pin parent create identity still zero after txid.body",
        )));
    }
    Ok(tid)
}

/// Schema-13 denserels decode leaves zero identity — stamp from plan RAM only (load).
#[inline]
fn fill_create_txid_load(
    tx: &mut rbitcoin_store::TxRecord,
    create_fk_id: u64,
    plan: Option<&rbitcoin_query::ArchiveWritePlan>,
) -> Result<(), ConsensusError> {
    if tx.txid != [0u8; 32] {
        return Ok(());
    }
    tx.txid = known_create_txid_load(create_fk_id, plan)?;
    Ok(())
}

/// Pin parents for wire load: **only spent parents** (sparse outs).
///
/// Sources: plan/in-flight offline denserels → **body denserels by range** from
/// [`ParentPinStamp`] (lookup-stamped). Load never reads head/idx/txid.body.
fn pin_for_wire_batch(
    query: &Query,
    plan: Option<&rbitcoin_query::ArchiveWritePlan>,
    parent_pin: &ParentPinStamp,
    metas: &[BodyMeta],
    wire_blocks: &[Arc<Block>],
    in_flight: Option<&rbitcoin_query::InFlightView>,
    pipeline_parent_store: Option<&std::sync::Arc<rbitcoin_query::PipelineParentStore>>,
) -> Result<
    (
        rbitcoin_query::BatchParents,
        rbitcoin_query::BatchThin,
        DenserelsWarmStats,
    ),
    ConsensusError,
> {
    use rbitcoin_query::confirm_load_stats;
    use rbitcoin_query::ThinInput;
    use std::sync::atomic::Ordering;

    let t_pin = Instant::now();
    let mut batch_thin: rbitcoin_query::BatchThin = rbitcoin_query::BatchThin::default();
    let mut parent_vouts: U64Map<Vec<u32>> = U64Map::default();
    let mut n_same_batch = 0u32;

    // id → Arc pin (tx, outs, dense denserels). Spent parents only (after thin pass).
    let mut plan_by_id: U64Map<
        std::sync::Arc<(
            rbitcoin_store::TxRecord,
            Vec<rbitcoin_store::OutputRecord>,
            Vec<u32>,
        )>,
    > = U64Map::default();
    // batch_pin by create id (Arc — preferred same-batch pin source).
    // packed pin half shares the same Arc; no separate outs clone.
    let mut batch_pin_by_id: U64Map<
        &std::sync::Arc<(
            rbitcoin_store::TxRecord,
            Vec<rbitcoin_store::OutputRecord>,
            Vec<u32>,
        )>,
    > = U64Map::default();
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
        // plan=None: create_fk from ParentPinStamp (lookup head/idx), never load head.
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
                    if let Some(&pid) = parent_pin.create_by_txid.get(&prev_txid) {
                        edges.push(ThinInput {
                            create_fk: Some(pid),
                            prev_index: vout,
                        });
                        parent_vouts.entry(pid).or_default().push(vout);
                        continue;
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
    if let Some(ifo) = in_flight {
        for (id, need) in &parent_vouts {
            if plan_by_id.contains_key(id) {
                continue;
            }
            if let Some(pin) = ifo.get_out(*id) {
                let _ = need;
                plan_by_id.insert(*id, std::sync::Arc::clone(pin));
            }
        }
    }
    // 2) External sparse pins are applied after adopt (not dense CreatePin).
    // 2b deferred: range denserels after free pins are in batch_parents (sparse API).
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

    let mut batch_parents = match pipeline_parent_store {
        Some(store) => rbitcoin_query::BatchParents::with_store(
            std::sync::Arc::clone(store),
            parent_vouts.len(),
        ),
        None => rbitcoin_query::BatchParents::with_capacity(parent_vouts.len()),
    };
    // One store lock: adopt live shared pins (writeq / peer load overlap) so free
    // path can skip OutputRecord clones when need is already covered.
    let t_adopt = Instant::now();
    if pipeline_parent_store.is_some() {
        batch_parents.adopt_from_store(parent_vouts.keys().copied());
    }
    let adopt_ns = t_adopt.elapsed().as_nanos() as u64;
    let mut still_need: U64Map<Vec<u32>> = U64Map::default();
    let mut n_plan_pin = 0u64;

    // Plan / in-flight / same-batch free pins → BatchParents (local HashMap put;
    // store mutex only at adopt/publish — not per parent).
    let t_plan = Instant::now();
    for (id, need) in &parent_vouts {
        let fk = rbitcoin_primitives::Fk(*id);
        // Cross-batch share hit: pin already covers need after adopt.
        // Pure hit: only refresh meta when plan/layout material is present
        // (skip empty refresh_pin_meta and avoid redundant outs loads).
        if !need.is_empty() && batch_parents.pin_covered(fk, need) {
            if let Some(pin) = plan_by_id.get(id) {
                let (tx, _outs, denserels) = pin.as_ref();
                let cb = if tx.input_count != 1 {
                    Some(false)
                } else {
                    None
                };
                let plan_range = parent_pin.ranges.get(id).copied().or_else(|| {
                    plan.and_then(|p| p.external_parent_ranges.get(id).copied())
                });
                let sparse = if !denserels.is_empty() {
                    rbitcoin_query::sparse_spender_rels(denserels, need)
                } else {
                    Vec::new()
                };
                if cb.is_some() || plan_range.is_some() || !sparse.is_empty() {
                    batch_parents.refresh_pin_meta(fk, cb, plan_range, sparse);
                }
            } else if let Some(plan) = plan {
                // Sparse external: layout/coinbase only from plan-local pin.
                if let Some(ext) = plan.external_parent_outs.get(id) {
                    let (tx, _live, sparse_all) = ext.as_ref();
                    let cb = if tx.input_count != 1 {
                        Some(false)
                    } else {
                        None
                    };
                    let plan_range = parent_pin
                        .ranges
                        .get(id)
                        .copied()
                        .or_else(|| plan.external_parent_ranges.get(id).copied());
                    let sparse: Vec<(u32, u32)> = sparse_all
                        .iter()
                        .copied()
                        .filter(|(v, _)| need.binary_search(v).is_ok())
                        .collect();
                    if cb.is_some() || plan_range.is_some() || !sparse.is_empty() {
                        batch_parents.refresh_pin_meta(fk, cb, plan_range, sparse);
                    }
                }
            }
            n_plan_pin = n_plan_pin.saturating_add(1);
            continue;
        }
        // Sparse external parent pin (need-vouts only — no dense CreatePin).
        if let Some(plan) = plan {
            if let Some(ext) = plan.external_parent_outs.get(id) {
                let (tx, live_all, sparse_all) = ext.as_ref();
                let live: Vec<(u32, rbitcoin_store::OutputRecord)> = need
                    .iter()
                    .filter_map(|&v| {
                        live_all
                            .iter()
                            .find(|(ov, _)| *ov == v)
                            .map(|(_, o)| (v, o.clone()))
                    })
                    .collect();
                if live.len() == need.len() || (need.is_empty() && !live_all.is_empty()) {
                    let live = if need.is_empty() {
                        live_all.clone()
                    } else {
                        live
                    };
                    let checked = if need.is_empty() {
                        live_all.iter().map(|(v, _)| *v).collect()
                    } else {
                        need.clone()
                    };
                    let cb = if tx.input_count != 1 {
                        Some(false)
                    } else {
                        None
                    };
                    let plan_range = parent_pin
                        .ranges
                        .get(id)
                        .copied()
                        .or_else(|| plan.external_parent_ranges.get(id).copied());
                    let sparse: Vec<(u32, u32)> = if need.is_empty() {
                        sparse_all.clone()
                    } else {
                        sparse_all
                            .iter()
                            .copied()
                            .filter(|(v, _)| need.binary_search(v).is_ok())
                            .collect()
                    };
                    batch_parents.insert_owned(
                        fk,
                        tx.clone(),
                        live,
                        checked,
                        cb,
                        plan_range,
                        sparse,
                    );
                    n_plan_pin = n_plan_pin.saturating_add(1);
                    continue;
                }
                // Incomplete sparse pin — fall through to range / cold.
                still_need.insert(*id, need.clone());
                continue;
            }
        }
        if let Some(pin) = plan_by_id.get(id) {
            let (tx, outs, denserels) = pin.as_ref();
            let live: Vec<(u32, rbitcoin_store::OutputRecord)> = need
                .iter()
                .filter_map(|&v| outs.get(v as usize).map(|o| (v, o.clone())))
                .collect();
            if live.len() != need.len() {
                // Incomplete plan outs — fall through to range / cold.
                still_need.insert(*id, need.clone());
                continue;
            }
            let cb = if tx.input_count != 1 {
                Some(false)
            } else {
                None
            };
            let plan_range = parent_pin.ranges.get(id).copied().or_else(|| {
                plan.and_then(|p| p.external_parent_ranges.get(id).copied())
            });
            let (body_range, sparse) = if !denserels.is_empty() {
                (
                    plan_range,
                    rbitcoin_query::sparse_spender_rels(denserels, need),
                )
            } else {
                (plan_range, Vec::new())
            };
            batch_parents.insert_owned(fk, tx.clone(), live, need.clone(), cb, body_range, sparse);
            n_plan_pin = n_plan_pin.saturating_add(1);
        } else {
            still_need.insert(*id, need.clone());
        }
    }
    let plan_pin_ns = t_plan.elapsed().as_nanos() as u64;
    // Batch-local cold walls/counts for last-pin / slow-load logs.
    let mut cold_range_batch_ns = 0u64;
    let mut n_range_new = 0u64;

    // 2b) Body denserels by range for still_need (lookup-stamped ranges only).
    {
        let mut range_jobs: Vec<(rbitcoin_primitives::Fk, (u64, u64), [u8; 32], Vec<u32>)> =
            Vec::new();
        for (id, need) in &still_need {
            let range = parent_pin.ranges.get(id).copied().or_else(|| {
                plan.and_then(|p| p.external_parent_ranges.get(id).copied())
            });
            let Some(range) = range else {
                continue;
            };
            let tid = parent_pin.create_txid(*id).or_else(|| {
                plan.and_then(|p| p.external_parent_txid(*id))
                    .filter(|t| *t != [0u8; 32])
            });
            let Some(tid) = tid else {
                return Err(ConsensusError::Store(StoreError::Corrupt(
                    "invariant: lookup stage miss (load parent create identity not stamped)",
                )));
            };
            range_jobs.push((rbitcoin_primitives::Fk(*id), range, tid, need.clone()));
        }
        if !range_jobs.is_empty() {
            let n_range = range_jobs.len() as u64;
            let (decoded, body_ns, dec_ns) = query
                .store()
                .get_outs_denserels_by_range_batch(&range_jobs)
                .map_err(ConsensusError::Store)?;
            let rng_ns = body_ns.saturating_add(dec_ns);
            cold_range_batch_ns = cold_range_batch_ns.saturating_add(rng_ns);
            if rng_ns > 0 {
                confirm_load_stats::COLD_IO_NS.fetch_add(rng_ns, Ordering::Relaxed);
                confirm_load_stats::COLD_RANGE_NS.fetch_add(rng_ns, Ordering::Relaxed);
            }
            if body_ns > 0 {
                confirm_load_stats::COLD_RANGE_BODY_NS.fetch_add(body_ns, Ordering::Relaxed);
            }
            if dec_ns > 0 {
                confirm_load_stats::COLD_RANGE_DECODE_NS.fetch_add(dec_ns, Ordering::Relaxed);
            }
            confirm_load_stats::COLD_RANGE_N.fetch_add(n_range, Ordering::Relaxed);
            confirm_load_stats::BODY_TX_READS.fetch_add(n_range, Ordering::Relaxed);
            confirm_load_stats::PIN_NEW.fetch_add(n_range, Ordering::Relaxed);
            n_range_new = n_range_new.saturating_add(n_range);
            let t_range_fill = Instant::now();
            for ((fk, range, _tid, need), row) in range_jobs.into_iter().zip(decoded.into_iter()) {
                let Some(id) = fk.get() else {
                    continue;
                };
                let Some((mut tx, live, sparse)) = row else {
                    return Err(ConsensusError::Store(StoreError::Corrupt(
                        "invariant: load denserels by range returned none for stamped parent",
                    )));
                };
                if live.len() != need.len() {
                    return Err(ConsensusError::Store(StoreError::Corrupt(
                        "invariant: load denserels by range incomplete outs for need_vouts",
                    )));
                }
                // Schema-13 decode leaves zero identity — stamp from parent_pin only.
                if tx.txid == [0u8; 32] {
                    tx.txid = parent_pin.create_txid(id).ok_or_else(|| {
                        ConsensusError::Store(StoreError::Corrupt(
                            "invariant: lookup stage miss (load parent create identity not stamped)",
                        ))
                    })?;
                }
                let cb = if tx.input_count != 1 {
                    Some(false)
                } else {
                    None
                };
                batch_parents.insert_owned(fk, tx, live, need, cb, Some(range), sparse);
                still_need.remove(&id);
                n_plan_pin = n_plan_pin.saturating_add(1);
            }
            let range_fill_ns = t_range_fill.elapsed().as_nanos() as u64;
            if range_fill_ns > 0 {
                confirm_load_stats::PIN_RANGE_FILL_NS.fetch_add(range_fill_ns, Ordering::Relaxed);
            }
        }
    }

    // Load IO contract: denserels only via body-by-range (above) or plan/in-flight
    // offline pins. **Never** `tx.idx` / head cold denserels on load (idx is lookup).
    let n_cold = 0u64;
    let cold_io_ns = 0u64;
    let cold_decode_ns = 0u64;
    if !still_need.is_empty() {
        return Err(ConsensusError::Store(StoreError::Corrupt(
            "invariant: lookup stage miss (load parent without body_range denserels)",
        )));
    }

    // Pin contract: every spent parent is in BatchParents with need outs.
    // Denserels/body_range may wait for write ensure (load-ahead plan pin).
    let t_contract = Instant::now();
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
    let contract_ns = t_contract.elapsed().as_nanos() as u64;

    // One store lock: publish Weaks so peer load/writeq can adopt the same Arc.
    let t_publish = Instant::now();
    batch_parents.publish_to_store();
    let publish_ns = t_publish.elapsed().as_nanos() as u64;

    let n_unique = parent_vouts.len() as u64;
    if n_unique > 0 {
        confirm_load_stats::PARENT_UNIQUE.fetch_add(n_unique, Ordering::Relaxed);
        confirm_load_stats::UTXO_PARENTS.fetch_add(n_unique, Ordering::Relaxed);
    }
    if n_plan_pin > 0 {
        confirm_load_stats::PIN_PLAN.fetch_add(n_plan_pin, Ordering::Relaxed);
        confirm_load_stats::PIN_CACHE_BODY.fetch_add(n_plan_pin, Ordering::Relaxed);
    }
    if n_cold > 0 {
        confirm_load_stats::PIN_NEW.fetch_add(n_cold, Ordering::Relaxed);
    }
    if plan_pin_ns > 0 {
        confirm_load_stats::PLAN_PIN_NS.fetch_add(plan_pin_ns, Ordering::Relaxed);
    }
    if adopt_ns > 0 {
        confirm_load_stats::PIN_ADOPT_NS.fetch_add(adopt_ns, Ordering::Relaxed);
    }
    if contract_ns > 0 {
        confirm_load_stats::PIN_CONTRACT_NS.fetch_add(contract_ns, Ordering::Relaxed);
    }
    if publish_ns > 0 {
        confirm_load_stats::PIN_PUBLISH_NS.fetch_add(publish_ns, Ordering::Relaxed);
    }
    // Last-batch pin residual for slow-load logs (overwrite; not window-summed).
    let cold_batch_ns = cold_range_batch_ns
        .saturating_add(cold_io_ns)
        .saturating_add(cold_decode_ns);
    confirm_load_stats::note_last_pin(
        adopt_ns,
        plan_pin_ns,
        cold_batch_ns,
        contract_ns,
        publish_ns,
        n_plan_pin,
        n_cold.saturating_add(n_range_new),
    );
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
        parents: parent_vouts.len().saturating_sub(n_same_batch as usize) as u32,
        already: n_plan_pin.saturating_sub(n_same_batch as u64) as u32,
        cold: n_cold as u32,
        same_batch: n_same_batch,
        work_ns: pin_ns,
    };
    Ok((batch_parents, batch_thin, warm))
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
    // Test-only: hold the first in-flight wave until a second async submit is
    // observed (proves production feed-ahead claims during join).
    scripts_feed_test_sync::on_phase_enter();
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

/// Handle for a scripts stage running on the rayon global pool (non-blocking start).
///
/// IBD scripts OS thread starts the next batch with [`confirm_scripts_phase_async`]
/// **while** joining the prior (poll claim + short timeouts), so rayon stays fed
/// even when load→scripts depth is 1.
pub struct ScriptsPhaseHandle {
    rx: std::sync::mpsc::Receiver<Result<ConfirmScriptOutcome, ConsensusError>>,
}

impl ScriptsPhaseHandle {
    /// Block until the spawned wave finishes (ordered join).
    pub fn join(self) -> Result<ConfirmScriptOutcome, ConsensusError> {
        self.rx.recv().unwrap_or_else(|_| {
            Err(ConsensusError::BadBlock(
                "scripts phase: rayon worker disconnected before result",
            ))
        })
    }

    /// Wait up to `timeout` for the wave result (production feed-ahead polls
    /// load→scripts `try_recv` between timeouts so N+1 can start mid-join).
    pub fn recv_timeout(
        &self,
        timeout: std::time::Duration,
    ) -> Result<Result<ConfirmScriptOutcome, ConsensusError>, std::sync::mpsc::RecvTimeoutError>
    {
        self.rx.recv_timeout(timeout)
    }
}

/// Submit [`confirm_scripts_phase`] onto the **rayon global pool** without
/// blocking the caller.
///
/// The OS scripts thread must keep claiming N+1 **while** waiting on N’s
/// [`ScriptsPhaseHandle::recv_timeout`] (not only once before a blocking join).
pub fn confirm_scripts_phase_async(batch: LoadedBatch) -> ScriptsPhaseHandle {
    scripts_feed_test_sync::on_async_submit();
    let (tx, rx) = std::sync::mpsc::sync_channel(1);
    rayon::spawn(move || {
        let r = confirm_scripts_phase(batch);
        let _ = tx.send(r);
    });
    ScriptsPhaseHandle { rx }
}

/// Join `handle`, repeatedly invoking `on_poll` (e.g. load `try_recv` + async
/// submit) so a second ready batch reaches rayon **before** this join returns.
///
/// This is the production feed-ahead primitive used under depth-1 channels.
pub fn join_scripts_polling<F>(
    handle: &ScriptsPhaseHandle,
    poll: std::time::Duration,
    mut on_poll: F,
) -> Result<ConfirmScriptOutcome, ConsensusError>
where
    F: FnMut(),
{
    loop {
        on_poll();
        match handle.recv_timeout(poll) {
            Ok(r) => return r,
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => continue,
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                return Err(ConsensusError::BadBlock(
                    "scripts phase: rayon worker disconnected before result",
                ));
            }
        }
    }
}

/// Run script verify for a sequence of loaded batches with **one-batch feed-ahead**.
///
/// While batch *i* is verifying on rayon, batch *i+1* (if present) is already
/// submitted so the pool is not idle solely between sequential claim walls.
/// Results are returned **in input order** (height-ordered write handoff).
///
/// Single-batch input is fine (no second submit).
pub fn confirm_scripts_feed_ahead(
    batches: impl IntoIterator<Item = LoadedBatch>,
) -> Result<Vec<ConfirmScriptOutcome>, ConsensusError> {
    let mut iter = batches.into_iter();
    let Some(first) = iter.next() else {
        return Ok(Vec::new());
    };
    let mut current = confirm_scripts_phase_async(first);
    let mut out = Vec::new();
    let mut next = iter.next().map(confirm_scripts_phase_async);
    loop {
        // Keep offering the following batch while joining current.
        let outcome =
            join_scripts_polling(&current, std::time::Duration::from_micros(200), || {
                if next.is_none() {
                    next = iter.next().map(confirm_scripts_phase_async);
                }
            })?;
        out.push(outcome);
        match next.take() {
            Some(h) => current = h,
            None => break,
        }
    }
    Ok(out)
}

/// Drive the **production** scripts claim/feed-ahead pattern from a load→scripts
/// channel (including depth 1): blocking claim for current, then
/// [`join_scripts_polling`] with `try_recv` to start N+1 mid-join.
///
/// Used by the IBD scripts OS thread and unit tests that exercise real
/// `sync_channel(1)` timing.
pub fn scripts_stage_from_load_channel(
    mat_rx: &std::sync::mpsc::Receiver<(LoadedBatch, u64)>,
    mut on_ok: impl FnMut(ConfirmScriptOutcome, ScriptsBatchMeta) -> bool,
    mut on_err: impl FnMut(ConsensusError, ScriptsBatchMeta) -> bool,
    mut should_stop: impl FnMut() -> bool,
) {
    let mut current: Option<(ScriptsPhaseHandle, ScriptsBatchMeta)> = None;
    let mut lookahead: Option<(ScriptsPhaseHandle, ScriptsBatchMeta)> = None;

    let start = |batch: LoadedBatch, mat_ns: u64| -> (ScriptsPhaseHandle, ScriptsBatchMeta) {
        let meta = ScriptsBatchMeta::from_batch(&batch, mat_ns);
        let handle = confirm_scripts_phase_async(batch);
        (handle, meta)
    };

    loop {
        if should_stop() {
            break;
        }
        if current.is_none() {
            let (batch, mat_ns) = match mat_rx.recv() {
                Ok(x) => x,
                Err(_) => break,
            };
            if should_stop() {
                break;
            }
            current = Some(start(batch, mat_ns));
        }
        let (handle, meta) = match current.take() {
            Some(c) => c,
            None => break,
        };
        let result = join_scripts_polling(&handle, std::time::Duration::from_micros(200), || {
            if lookahead.is_none() {
                if let Ok((batch, mat_ns)) = mat_rx.try_recv() {
                    if !should_stop() {
                        lookahead = Some(start(batch, mat_ns));
                    }
                }
            }
        });
        match result {
            Ok(outcome) => {
                let cont = on_ok(outcome, meta);
                if !cont {
                    break;
                }
                current = lookahead.take();
            }
            Err(e) => {
                let cont = on_err(e, meta);
                // Drop later batch without treating it as write-ready.
                if let Some((h, m)) = lookahead.take() {
                    let _ = h.join();
                    let _ = m; // caller finishes heights via on_err if needed
                }
                if !cont {
                    break;
                }
                current = None;
            }
        }
    }
    if let Some((h, _)) = current.take() {
        let _ = h.join();
    }
    if let Some((h, _)) = lookahead.take() {
        let _ = h.join();
    }
}

/// Metadata retained across async scripts submit → ordered write handoff.
#[derive(Clone, Debug)]
pub struct ScriptsBatchMeta {
    pub n: usize,
    pub first_h: u32,
    pub heights_hashes: Vec<(u32, [u8; 32])>,
    pub mat_ns: u64,
    pub t0: Instant,
}

impl ScriptsBatchMeta {
    pub fn from_batch(batch: &LoadedBatch, mat_ns: u64) -> Self {
        let heights_hashes = batch.heights_hashes();
        let first_h = heights_hashes.first().map(|(h, _)| *h).unwrap_or(0);
        Self {
            n: batch.len(),
            first_h,
            heights_hashes,
            mat_ns,
            t0: Instant::now(),
        }
    }
}

/// Test-only sync so unit tests can prove N+1 was submitted while N’s wave is still open.
pub mod scripts_feed_test_sync {
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
    use std::time::{Duration, Instant};

    static SUBMIT_COUNT: AtomicU64 = AtomicU64::new(0);
    static HOLD_FIRST: AtomicBool = AtomicBool::new(false);
    static FIRST_ENTERED: AtomicBool = AtomicBool::new(false);

    /// Reset counters (call at start of each feed-ahead timing test).
    pub fn reset() {
        SUBMIT_COUNT.store(0, Ordering::SeqCst);
        HOLD_FIRST.store(false, Ordering::SeqCst);
        FIRST_ENTERED.store(false, Ordering::SeqCst);
    }

    /// When true, the first [`super::confirm_scripts_phase`] waits until
    /// [`submit_count`] ≥ 2 (second async submit happened mid-wave).
    pub fn set_hold_first_until_second_submit(hold: bool) {
        HOLD_FIRST.store(hold, Ordering::SeqCst);
        FIRST_ENTERED.store(false, Ordering::SeqCst);
    }

    pub fn submit_count() -> u64 {
        SUBMIT_COUNT.load(Ordering::SeqCst)
    }

    pub(super) fn on_async_submit() {
        SUBMIT_COUNT.fetch_add(1, Ordering::SeqCst);
    }

    pub(super) fn on_phase_enter() {
        if !HOLD_FIRST.load(Ordering::SeqCst) {
            return;
        }
        // Only the first wave holds.
        if FIRST_ENTERED.swap(true, Ordering::SeqCst) {
            return;
        }
        let deadline = Instant::now() + Duration::from_secs(5);
        while submit_count() < 2 {
            if Instant::now() > deadline {
                // Avoid hanging the suite if feed-ahead is broken.
                break;
            }
            std::thread::sleep(Duration::from_millis(1));
        }
    }
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
/// When `batch.archive_plan` is set (wire lookup/load path), Class A is appended in this
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
    let mut kept = Vec::with_capacity(batch.prepared.len());
    let mut wires = Vec::with_capacity(batch.wire_blocks.len());
    for (p, w) in batch
        .prepared
        .into_iter()
        .zip(batch.wire_blocks.into_iter())
    {
        if !write_height_needed(tip, p.height.0) {
            continue;
        }
        kept.push(p);
        wires.push(w);
    }
    if kept.is_empty() {
        return Ok(Vec::new());
    }
    batch.prepared = kept;
    batch.wire_blocks = wires;

    let t_wall = Instant::now();

    // Single commit era: durable Class A for this batch before spentness RMW.
    // Keep create pins for SH collect (Class C) — same Arcs as layout fill; avoid
    // re-preading Class A bodies under RES=0 when residency is empty.
    let mut write_create_pins: FkMap<rbitcoin_query::CreatePin> = FkMap::default();
    let mut class_a_ns = 0u64;
    let mut ensure_ns = 0u64;
    if let Some(plan) = batch.archive_plan.take() {
        if !plan.is_empty() {
            // Shared CreatePin Arcs only (refcount) for post-commit layout fill —
            // no whole-plan packed deep clone of outs.
            let planned_fks = plan.planned_fks.clone();
            let pins: Vec<rbitcoin_query::CreatePin> =
                if plan.batch_pin.len() == plan.planned_fks.len() {
                    plan.batch_pin.iter().map(std::sync::Arc::clone).collect()
                } else {
                    plan.packed
                        .iter()
                        .map(|(pin, _)| std::sync::Arc::clone(pin))
                        .collect()
                };
            let t_ca = Instant::now();
            let committed = query
                .archive_commit_plan(plan)
                .map_err(ConsensusError::Store)?;
            class_a_ns = t_ca.elapsed().as_nanos() as u64;
            // Layout + SH pins only after a real append. Idempotent skip (Class A
            // already present) uses store denserels via ensure / class_c cold pins.
            if committed {
                write_create_pins.reserve(planned_fks.len());
                for (fk, pin) in planned_fks.iter().zip(pins.iter()) {
                    write_create_pins.insert(*fk, std::sync::Arc::clone(pin));
                }
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
    }
    // Ensure denserels/abs for every spend edge before structural + annotate:
    // - load-ahead in-flight parents (no denserels at pin time)
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
    let mut meta_by_abs: rbitcoin_query::U64Map<(rbitcoin_primitives::Fk, u8)> =
        rbitcoin_query::U64Map::default();
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
    let cc0 = confirm_phase_stats::CLASS_C_NS.load(Ordering::Relaxed);
    let out = class_c_commit(query, &mut batch.prepared, &write_create_pins)?;
    // Tables only (strong+tip), matching CLASS_C_NS — not join wall / SH.
    let class_c_ns = confirm_phase_stats::CLASS_C_NS
        .load(Ordering::Relaxed)
        .saturating_sub(cc0);

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
/// `batch_parents` (most of the batch). Prefer denserels already set at load pin;
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
    let missing: U64Set = batch_parents
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
    for ((&fk, range), &pi) in need_fks
        .iter()
        .zip(ranges.into_iter())
        .zip(need_pin_i.iter())
    {
        let Some((off, len)) = range else {
            continue;
        };
        // Load already attached sparse denserels: only body_range was missing.
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
/// 1. Load-ahead parents not yet committed when loaded (body_range missing)
/// 2. Already-archived Class A same-batch creates never pinned
/// 3. Retry after partial write
///
/// Prefer pin denserels already on BatchParents; Class A denserels body load only
/// on miss. After this function returns, every non-null spend edge **must** have
/// abs layout — no silent leave-for-later / structural cold paper.
fn ensure_spend_abs_layouts(
    query: &Query,
    batch_parents: &mut rbitcoin_query::BatchParents,
    prepared: &[Prepared],
) -> Result<(), ConsensusError> {
    use rbitcoin_store::IdxBodyMode;

    let mut need: U64Map<Vec<u32>> = U64Map::default();
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

    // 1) Pin denserels + body_range already on BatchParents — no body IO.
    let mut ensure_res = 0u64;
    let mut still: U64Map<Vec<u32>> = U64Map::default();
    // Pin has denserels but still no body_range — idx only (not denserels IO).
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
        // Structural denserels fill for pin gaps (cold Class A).
        let loaded =
            rbitcoin_query::load_creates_once(query.store(), &fks, IdxBodyMode::OutsDenserels)
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
                rbitcoin_store::decode_packed_tx_outs_with_spender_rels_secret(&c.raw, Some(secret))
                    .map_err(|_| {
                        ConsensusError::Store(StoreError::Corrupt(
                            "invariant: ensure denserels decode failed",
                        ))
                    })?
            };
            // Write ensure may read txid.body (not load stage).
            tx.txid = known_create_txid_lookup(query, id, None)?;
            if batch_parents.contains(c.fk) {
                // Layout-only publish with already_covers short-circuit (batched style).
                batch_parents.set_layout_for_need(c.fk, c.body_range, &dense_rels, &need_v);
                continue;
            }
            // Not pinned at load (e.g. already-archived same-batch create): insert
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
            batch_parents.insert_owned(c.fk, tx, live, checked, cb, Some(c.body_range), sparse);
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
        self.prepared.iter().map(|p| (p.height.0, p.hash)).collect()
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

    /// Parent handles in this batch (may share payloads with other batches).
    pub fn parent_count(&self) -> usize {
        self.batch_parents.len()
    }
}

impl ScriptOkBatch {
    /// Heights and header hashes in this batch (for events / feed scrub).
    pub fn heights_hashes(&self) -> Vec<(u32, [u8; 32])> {
        self.prepared.iter().map(|p| (p.height.0, p.hash)).collect()
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

    /// Parent handles in this batch (may share Arc payloads with other batches).
    pub fn parent_count(&self) -> usize {
        self.batch_parents.len()
    }

    /// Absorb another script-ok batch for write batch (FIFO drain).
    ///
    /// Scripts enqueue height-ordered tip extensions; write drains the channel
    /// and merges so Class A + Class C + annotate run once (fewer tip fsyncs).
    /// Returns `Err(other)` if not a contiguous height extension (caller keeps
    /// `other` for the next batch).
    pub fn append_contiguous(&mut self, mut other: Self) -> Result<(), Self> {
        if other.is_empty() {
            return Ok(());
        }
        if self.is_empty() {
            *self = other;
            return Ok(());
        }
        let Some(last) = self.prepared.last() else {
            *self = other;
            return Ok(());
        };
        let Some(first) = other.prepared.first() else {
            return Ok(());
        };
        if first.height.0 != last.height.0.saturating_add(1) {
            return Err(other);
        }
        if self.prepared.len() != self.wire_blocks.len()
            || other.prepared.len() != other.wire_blocks.len()
        {
            return Err(other);
        }
        self.prepared.append(&mut other.prepared);
        self.wire_blocks.append(&mut other.wire_blocks);
        self.batch_parents.extend_from(other.batch_parents);
        match (self.archive_plan.as_mut(), other.archive_plan.take()) {
            (Some(dst), Some(src)) => dst.append(src),
            (None, Some(src)) => self.archive_plan = Some(src),
            _ => {}
        }
        Ok(())
    }
}

#[cfg(test)]
mod write_idempotent_tests {
    use super::write_height_needed;

    /// Batch append: contiguous heights merge; gap returns Err(other).
    #[test]
    fn script_ok_append_contiguous_and_gap() {
        use super::{Prepared, ScriptOkBatch};
        use bitcoin::CompactTarget;
        use rbitcoin_primitives::{Fk, Height};
        use std::sync::Arc;

        fn empty_prepared(h: u32, hash_byte: u8) -> Prepared {
            Prepared {
                height: Height(h),
                header_fk: Fk(h as u64),
                tx_fks: vec![],
                jobs: vec![],
                spends: vec![],
                fees: 0,
                check_scripts: false,
                time: 0,
                bits: CompactTarget::from_consensus(0x207f_ffff),
                hash: [hash_byte; 32],
            }
        }
        fn batch_one(h: u32) -> ScriptOkBatch {
            ScriptOkBatch {
                prepared: vec![empty_prepared(h, h as u8)],
                wire_blocks: vec![Arc::new(crate::params::genesis_block(
                    &crate::params::ChainParams::regtest(),
                ))],
                batch_parents: rbitcoin_query::BatchParents::new(),
                archive_plan: None,
            }
        }
        let mut a = batch_one(10);
        let b = batch_one(11);
        assert!(a.append_contiguous(b).is_ok());
        assert_eq!(a.len(), 2);
        let gap = batch_one(13);
        let err = a.append_contiguous(gap).err().expect("gap");
        assert_eq!(err.len(), 1);
        assert_eq!(a.len(), 2);
        // Contiguous continue after gap reject.
        let c = batch_one(12);
        assert!(a.append_contiguous(c).is_ok());
        assert_eq!(a.len(), 3);
    }

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

    fn empty_loaded_batch() -> super::LoadedBatch {
        super::LoadedBatch {
            prepared: Vec::new(),
            wire_blocks: Vec::new(),
            batch_parents: rbitcoin_query::BatchParents::new(),
            script_preverified: super::ScriptPreverified::new(),
            archive_plan: None,
        }
    }

    /// One-batch feed-ahead path (no lookahead) still succeeds on the real entry.
    #[test]
    fn scripts_feed_ahead_single_batch() {
        use super::confirm_scripts_feed_ahead;
        let outs = confirm_scripts_feed_ahead([empty_loaded_batch()]).expect("single");
        assert_eq!(outs.len(), 1);
        assert!(outs[0].batch.is_empty());
    }

    /// Two ready batches: both verify on the real async path; write order preserved.
    ///
    /// Uses [`confirm_scripts_feed_ahead`] (same submit/join helper production
    /// scripts OS thread uses via [`confirm_scripts_phase_async`]).
    #[test]
    fn scripts_feed_ahead_two_batches_ordered() {
        use super::{confirm_scripts_feed_ahead, confirm_scripts_phase_async};
        // Async handles: start both before joining either (overlap submit).
        let h0 = confirm_scripts_phase_async(empty_loaded_batch());
        let h1 = confirm_scripts_phase_async(empty_loaded_batch());
        let o0 = h0.join().expect("batch0");
        let o1 = h1.join().expect("batch1");
        assert!(o0.batch.is_empty());
        assert!(o1.batch.is_empty());

        // Ordered helper: two batches both ok, returned in input order.
        let outs = confirm_scripts_feed_ahead([empty_loaded_batch(), empty_loaded_batch()])
            .expect("feed-ahead two");
        assert_eq!(outs.len(), 2);
        assert!(outs[0].batch.is_empty());
        assert!(outs[1].batch.is_empty());
    }

    /// Empty iterator is a no-op (pipeline edge).
    #[test]
    fn scripts_feed_ahead_zero_batches() {
        use super::confirm_scripts_feed_ahead;
        let outs = confirm_scripts_feed_ahead(std::iter::empty()).expect("empty");
        assert!(outs.is_empty());
    }

    /// **Production claim timing under depth-1:** batch B is submitted to rayon
    /// while A’s wave is still open (not only after A’s join returns).
    ///
    /// Drives [`scripts_stage_from_load_channel`] (same `try_recv` +
    /// [`join_scripts_polling`] pattern as the IBD scripts OS thread) on a
    /// `sync_channel(1)`. First wave holds in [`confirm_scripts_phase`] until
    /// a second async submit is observed — deadlocks if feed-ahead only
    /// try_recv once before a blocking join.
    #[test]
    fn scripts_stage_depth1_submits_second_before_first_finishes() {
        use super::{
            scripts_feed_test_sync, scripts_stage_from_load_channel, ConfirmScriptOutcome,
            ScriptsBatchMeta,
        };
        use std::sync::mpsc;
        use std::sync::{Arc, Mutex};
        use std::thread;
        use std::time::{Duration, Instant};

        scripts_feed_test_sync::reset();
        scripts_feed_test_sync::set_hold_first_until_second_submit(true);

        // Depth 1 — same default load→scripts capacity class.
        let (mat_tx, mat_rx) = mpsc::sync_channel::<(super::LoadedBatch, u64)>(1);
        let outcomes: Arc<Mutex<Vec<ConfirmScriptOutcome>>> = Arc::new(Mutex::new(Vec::new()));
        let outcomes_w = Arc::clone(&outcomes);

        let stage = thread::spawn(move || {
            scripts_stage_from_load_channel(
                &mat_rx,
                |ok, _meta: ScriptsBatchMeta| {
                    outcomes_w.lock().unwrap().push(ok);
                    true
                },
                |_e, _meta| false,
                || false,
            );
        });

        // Enqueue A; stage claims it (channel free). Hold keeps A's phase open.
        mat_tx.send((empty_loaded_batch(), 0)).expect("send A");
        let deadline = Instant::now() + Duration::from_secs(3);
        while scripts_feed_test_sync::submit_count() < 1 {
            assert!(Instant::now() < deadline, "A never submitted to rayon");
            thread::sleep(Duration::from_millis(1));
        }
        // Enqueue B while A is held mid-wave; feed-ahead must try_recv+submit B.
        mat_tx
            .send((empty_loaded_batch(), 0))
            .expect("send B while A verifying");
        while scripts_feed_test_sync::submit_count() < 2 {
            assert!(
                Instant::now() < deadline,
                "B not submitted before A finished (feed-ahead dead under depth-1)"
            );
            thread::sleep(Duration::from_millis(1));
        }
        // A can finish (hold released by submit_count>=2); both outcomes ordered.
        drop(mat_tx);
        stage.join().expect("stage thread");
        let outs = outcomes.lock().unwrap();
        assert_eq!(outs.len(), 2, "both batches script-ok");
        assert!(outs[0].batch.is_empty());
        assert!(outs[1].batch.is_empty());
        scripts_feed_test_sync::set_hold_first_until_second_submit(false);
        scripts_feed_test_sync::reset();
    }

    #[test]
    fn check_bip34_helper_and_expected_bits_no_retarget() {
        use super::{check_bip34, expected_bits_extending};
        use crate::params::ChainParams;
        use bitcoin::absolute::LockTime;
        use bitcoin::block::{Header, Version};
        use bitcoin::hashes::Hash;
        use bitcoin::script::ScriptBuf;
        use bitcoin::{
            Amount, Block, BlockHash, CompactTarget, OutPoint, Sequence, Transaction, TxIn,
            TxMerkleNode, TxOut, Witness,
        };
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
        accept_and_connect_block(&q, &params, Height::GENESIS, &genesis, Milestone::NONE).unwrap();
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
        let gbits =
            expected_bits_extending(&q, &params, Height(0), CompactTarget::from_consensus(0), 0)
                .unwrap();
        assert_eq!(gbits, crate::params::genesis_block(&params).header.bits);
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
            Amount, Block, BlockHash, OutPoint, Sequence, Transaction, TxIn, TxMerkleNode, TxOut,
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

    /// Multi-block tip-ahead assemble (i>0) calls [`expected_bits_extending`] on a
    /// retarget height. Period-start (`height − interval`) may still be **above**
    /// confirmed tip while already present as a ConfirmParentCache header plan
    /// (put when that height was looked up/loaded earlier).
    ///
    /// Mainnet log 2026-08-07: batch @132992 n=92 includes retarget 133056;
    /// first=131040; tip still ~129k → confirmed miss → "missing retarget first
    /// header" even though the plan cache should hold 131040.
    ///
    /// Ship path must resolve period-start via confirmed **or** header plan.
    #[test]
    fn expected_bits_extending_uses_header_plan_when_period_start_above_tip() {
        use super::expected_bits_extending;
        use crate::params::ChainParams;
        use bitcoin::CompactTarget;
        use rbitcoin_primitives::{Fk, Height};
        use rbitcoin_query::Query;
        use rbitcoin_store::HeaderRecord;
        use std::sync::Once;

        static ONCE: Once = Once::new();
        ONCE.call_once(|| {
            if std::env::var_os("RBITCOIN_HEAD_SCALE").is_none() {
                std::env::set_var("RBITCOIN_HEAD_SCALE", "tiny");
            }
        });
        let path = std::env::temp_dir().join(format!(
            "rbitcoin-retarget-plan-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0)
        ));
        std::fs::create_dir_all(&path).unwrap();
        let q = Query::open_or_create(&path).unwrap();
        let params = ChainParams::mainnet();
        let interval = params.difficulty_adjustment_interval();
        assert_eq!(interval, 2016, "mainnet difficulty interval");

        // Tip empty / genesis not required: period-start 2016 is above tip (None).
        assert!(
            q.header_at_height(Height(2016)).unwrap().is_none(),
            "period-start must not be on confirmed[]"
        );

        // Simulate earlier tip-ahead lookup/load that put the period-start plan.
        let mut hash_first = [0u8; 32];
        hash_first[0..4].copy_from_slice(&2016u32.to_le_bytes());
        hash_first[4] = 0xaa;
        let first_rec = HeaderRecord {
            prev_fk: Fk::NULL,
            version: 1,
            timestamp: 1_234_567,
            bits: 0x1d00ffff,
            nonce: 2016,
            merkle_root: hash_first,
            hash: hash_first,
        };
        let first_fk = q.store().put_header(&first_rec).unwrap();
        q.confirm_parent_cache().put_header_plan(
            2016,
            first_fk,
            first_rec.clone(),
            Vec::new(),
            [0u8; 32],
        );
        assert!(
            q.confirm_parent_cache().get_header_plan(2016).is_some(),
            "plan cache holds period-start (as real put_header_plan during load does)"
        );

        // Mid-batch path: prev bits/time come from prior prepared block in RAM;
        // only period-start is resolved from store/plan.
        let prev_bits = CompactTarget::from_consensus(0x1d00ffff);
        let prev_time = first_rec.timestamp.saturating_add(2015 * 600);
        let retarget_h = Height(4032); // 2 * interval — needs first @ 2016
        assert_eq!(retarget_h.0 % interval, 0);

        let got = expected_bits_extending(&q, &params, retarget_h, prev_bits, prev_time)
            .expect(
                "period-start on ConfirmParentCache must satisfy retarget bits \
                 (tip-ahead multi-block); confirmed-only lookup is the mainnet bug",
            );
        // Sanity: result is a real CompactTarget (same construction as production).
        let timespan = prev_time.saturating_sub(first_rec.timestamp) as u64;
        let expect = CompactTarget::from_next_work_required(prev_bits, timespan, &params.btc);
        assert_eq!(got, expect);

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

        let job = ScriptCheckJob::with_txid(tid, prevouts, tx, true, true, true, true, true);
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

    /// Lookup-stage denserels ensure + Forbid pin: cold path must not re-run on load.
    /// External parents land in plan-local map only.
    #[test]
    fn plan_ensure_denserels_then_forbid_skips_cold_io() {
        use super::{
            ensure_external_parent_denserels_from_plan, pin_for_wire_batch, ParentPinStamp,
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
        // Parent on disk only (ancient / cold external parent).

        // Plan with stamped parent create_fk (lookup stage already did batch head).
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
        let st = ensure_external_parent_denserels_from_plan(&q, Some(&mut plan), None).unwrap();
        assert!(
            st.cold >= 1,
            "parent missing denserels must cold-load: {st:?}"
        );
        assert!(
            plan.external_parent_outs
                .get(&pfk.get().unwrap())
                .is_some_and(|p| !p.1.is_empty() || !p.2.is_empty()),
            "ensure must put sparse denserels in plan-local external_parent_outs"
        );
        // Sparse only — no full output_count expand (parent has 1 out here; multi-out
        // sparse regression covers high output_count without n_out alloc).
        if let Some(p) = plan.external_parent_outs.get(&pfk.get().unwrap()) {
            assert_eq!(p.1.len(), 1, "sparse live must be need-vouts only");
            assert!(
                p.1.iter().all(|(v, _)| *v == 0),
                "sparse live keyed by vout, not dense index"
            );
        }
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
            pin_for_wire_batch(
                &q,
                Some(&plan),
                &ParentPinStamp::from_plan(&plan),
                &[],
                &[],
                None,
                None,
            )
            .unwrap();
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
        use super::{pin_for_wire_batch, ParentPinStamp};
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

        let err = pin_for_wire_batch(
            &q,
            Some(&plan),
            &ParentPinStamp::from_plan(&plan),
            &[],
            &[],
            None,
            None,
        )
        .expect_err("missing parent must hard-fail pin");
        let msg = format!("{err}");
        assert!(
            msg.contains("invariant")
                && (msg.contains("wire pin") || msg.contains("lookup stage miss")),
            "unexpected err: {msg}"
        );
        let _ = std::fs::remove_dir_all(&path);
    }

    /// Wire pin: in-flight outs shorter than need → cold miss → hard invariant.
    #[test]
    fn pin_for_wire_incomplete_outs_is_invariant_error() {
        use super::{pin_for_wire_batch, ParentPinStamp};
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
        let parent_tx = TxRecord {
            txid: [0xDDu8; 32],
            version: 1,
            locktime: 0,
            input_start_fk: Fk::NULL,
            input_count: 1,
            output_start_fk: Fk::NULL,
            output_count: 0,
        };
        let pin = std::sync::Arc::new((parent_tx, Vec::new(), Vec::new()));
        let mut log = rbitcoin_query::InFlightLog::new();
        log.note_layer(rbitcoin_query::InFlightLayer::from_plan_pins([(
            Fk(parent_id),
            &pin,
        )]));
        let ifo = log.snapshot();

        let err = pin_for_wire_batch(
            &q,
            Some(&plan),
            &ParentPinStamp::from_plan(&plan),
            &[],
            &[],
            Some(&ifo),
            None,
        )
        .expect_err("incomplete outs must hard-fail pin");
        let msg = format!("{err}");
        assert!(
            msg.contains("invariant")
                && (msg.contains("wire pin") || msg.contains("lookup stage miss")),
            "unexpected err: {msg}"
        );
        let _ = std::fs::remove_dir_all(&path);
    }

    /// After wire pin, external sparse outs are cleared; sparse BatchParents remain.
    /// Pin uses Arc::clone of SparseExternalPin (no deep outs clone).
    #[test]
    fn pin_takes_external_create_pin_arc_then_clear_for_write_queue() {
        use super::{pin_for_wire_batch, ParentPinStamp};
        use rbitcoin_primitives::Fk;
        use rbitcoin_query::{ArchiveWritePlan, CreatePin, Query, SparseExternalPin};
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
        let parent_out = OutputRecord::unspent(50_0000_0000, vec![0x51]);
        let dens = rbitcoin_store::denserels_from_packed_records(
            &parent_tx,
            &[InputRecord::coinbase(u32::MAX, vec![0x01], vec![])],
            &[parent_out.clone()],
        );
        let sparse_rel = dens.first().copied().unwrap_or(0);
        let external: SparseExternalPin = Arc::new((
            parent_tx.clone(),
            vec![(0, parent_out)],
            vec![(0, sparse_rel)],
        ));

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
                let mut m = rbitcoin_query::U64Map::default();
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
            &ParentPinStamp::from_plan(&plan),
            &[],
            &[],
            None,
            None,
        )
        .expect("pin external via SparseExternalPin Arc (body denserels by range only)");
        assert!(parents.contains(Fk(parent_id)));
        assert!(
            parents.get_parent_out(Fk(parent_id), 0).is_some(),
            "sparse need-vout must be in BatchParents"
        );
        // Plan map still the shared Arc until load clears it.
        assert!(Arc::ptr_eq(
            plan.external_parent_outs.get(&parent_id).unwrap(),
            &external
        ));

        // Production load freezes plan after pin so write queue is lean.
        plan.freeze_after_pin();
        assert!(
            plan.external_parent_outs.is_empty(),
            "post-pin plan must not carry external sparse outs to scripts/write"
        );
        // Sparse pin still holds the need-vout independently of the plan map.
        assert!(parents.get_parent_out(Fk(parent_id), 0).is_some());
        let _ = std::fs::remove_dir_all(&path);
    }

    /// Multi-out parent: ensure/pin keep only spent need-vouts (no n_out expand).
    #[test]
    fn ensure_external_sparse_need_not_full_output_count() {
        use super::{
            ensure_external_parent_denserels_from_plan, pin_for_wire_batch, ParentPinStamp,
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
            "rbitcoin-ensure-sparse-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0)
        ));
        std::fs::create_dir_all(&path).unwrap();
        let q = Query::open_or_create(&path).unwrap();
        q.enter_direct_index_mode().unwrap();

        // Parent with many outs; spend only vout 3.
        let n_out = 64u32;
        let parent_tx = TxRecord {
            txid: [0xab; 32],
            version: 1,
            locktime: 0,
            input_start_fk: Fk::NULL,
            input_count: 1,
            output_start_fk: Fk::NULL,
            output_count: n_out,
        };
        let parent_outs: Vec<_> = (0..n_out)
            .map(|i| OutputRecord::unspent(1000 + i as i64, vec![0x51, i as u8]))
            .collect();
        let parent_ins = vec![InputRecord::coinbase(u32::MAX, vec![0x01], vec![])];
        let pfk = q
            .store()
            .txs
            .put_full_batch_indexed(&[(parent_tx.clone(), parent_ins, parent_outs)], true)
            .unwrap()[0];

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
                prev_index: 3,
                sequence: u32::MAX,
                script_sig: vec![],
                witness: vec![],
            }],
        )];
        plan.planned_fks = vec![Fk(2)];

        let st = ensure_external_parent_denserels_from_plan(&q, Some(&mut plan), None).unwrap();
        assert!(st.cold >= 1, "must cold-load multi-out parent: {st:?}");
        let pin = plan
            .external_parent_outs
            .get(&pfk.get().unwrap())
            .expect("sparse external pin");
        assert_eq!(
            pin.1.len(),
            1,
            "must not expand to full output_count={}",
            n_out
        );
        assert_eq!(pin.1[0].0, 3, "only spent need-vout");
        assert_eq!(pin.1[0].1.value, 1003);
        assert!(
            pin.2.iter().all(|(v, _)| *v == 3),
            "sparse denserels only for need"
        );

        let (parents, _, _) = pin_for_wire_batch(
            &q,
            Some(&plan),
            &ParentPinStamp::from_plan(&plan),
            &[],
            &[],
            None,
            None,
        )
        .unwrap();
        assert!(parents.get_parent_out(Fk(pfk.get().unwrap()), 3).is_some());
        assert!(parents.get_parent_out(Fk(pfk.get().unwrap()), 0).is_none());

        let _ = std::fs::remove_dir_all(&path);
    }

    /// Store start states: S0 new Class A and S1 already-archived both confirm
    /// via shipped lookup→load (body denserels by range; no load head/idx).
    #[test]
    fn store_start_states_lookup_load_confirm() {
        use super::{
            confirm_scripts_phase, confirm_wire_load_from_plan, confirm_wire_lookup_stamp,
            confirm_write_phase, ColdPinMode, ScriptPreverified,
        };
        use crate::{accept_and_archive_block, accept_and_connect_block};
        use crate::milestone::Milestone;
        use crate::params::ChainParams;
        use bitcoin::block::{Header, Version};
        use bitcoin::blockdata::transaction::{
            OutPoint, Transaction, TxIn, TxOut, Version as TxVersion,
        };
        use bitcoin::hashes::Hash;
        use bitcoin::locktime::absolute::LockTime;
        use bitcoin::script::PushBytesBuf;
        use bitcoin::CompactTarget;
        use bitcoin::{Amount, Block, BlockHash, ScriptBuf, Sequence, TxMerkleNode, Witness};
        use rbitcoin_primitives::Height;
        use rbitcoin_query::Query;
        use std::sync::{Arc, Once};

        static ONCE: Once = Once::new();
        ONCE.call_once(|| {
            if std::env::var_os("RBITCOIN_HEAD_SCALE").is_none() {
                std::env::set_var("RBITCOIN_HEAD_SCALE", "tiny");
            }
        });
        let path = std::env::temp_dir().join(format!(
            "rbitcoin-start-states-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0)
        ));
        std::fs::create_dir_all(&path).unwrap();
        let q = Query::open_or_create(&path).unwrap();
        q.set_spend_index(true);
        let params = ChainParams::regtest();
        let ms = Milestone::NONE;
        let maturity = params.coinbase_maturity();

        fn coinbase(height: u32) -> Transaction {
            let mut script = ScriptBuf::new();
            let pb = PushBytesBuf::try_from(height.to_le_bytes().to_vec()).unwrap();
            script.push_slice(pb);
            script.push_opcode(bitcoin::opcodes::all::OP_CHECKSIG);
            Transaction {
                version: TxVersion::ONE,
                lock_time: LockTime::ZERO,
                input: vec![TxIn {
                    previous_output: OutPoint::null(),
                    script_sig: script,
                    sequence: Sequence::MAX,
                    witness: Witness::new(),
                }],
                output: vec![TxOut {
                    value: Amount::from_sat(50_0000_0000),
                    script_pubkey: ScriptBuf::from_bytes(vec![0x51]),
                }],
            }
        }
        fn mine_cb(prev: BlockHash, time: u32, h: u32) -> Block {
            let bits = CompactTarget::from_consensus(0x207f_ffff);
            let mut block = Block {
                header: Header {
                    version: Version::ONE,
                    prev_blockhash: prev,
                    merkle_root: TxMerkleNode::from_byte_array([0; 32]),
                    time,
                    bits,
                    nonce: 0,
                },
                txdata: vec![coinbase(h)],
            };
            block.header.merkle_root = block.compute_merkle_root().unwrap();
            let target = bitcoin::Target::from_compact(bits);
            for nonce in 0..u32::MAX {
                block.header.nonce = nonce;
                if block.header.validate_pow(target).is_ok() {
                    break;
                }
            }
            block
        }
        fn mine_with(prev: BlockHash, time: u32, h: u32, extra: Vec<Transaction>) -> Block {
            let bits = CompactTarget::from_consensus(0x207f_ffff);
            let mut txs = vec![coinbase(h)];
            txs.extend(extra);
            let mut block = Block {
                header: Header {
                    version: Version::ONE,
                    prev_blockhash: prev,
                    merkle_root: TxMerkleNode::from_byte_array([0; 32]),
                    time,
                    bits,
                    nonce: 0,
                },
                txdata: txs,
            };
            block.header.merkle_root = block.compute_merkle_root().unwrap();
            let target = bitcoin::Target::from_compact(bits);
            for nonce in 0..u32::MAX {
                block.header.nonce = nonce;
                if block.header.validate_pow(target).is_ok() {
                    break;
                }
            }
            block
        }
        fn spend(prev: bitcoin::Txid, vout: u32, val: Amount) -> Transaction {
            Transaction {
                version: TxVersion::TWO,
                lock_time: LockTime::ZERO,
                input: vec![TxIn {
                    previous_output: OutPoint { txid: prev, vout },
                    script_sig: ScriptBuf::new(),
                    sequence: Sequence::MAX,
                    witness: Witness::new(),
                }],
                output: vec![TxOut {
                    value: val,
                    script_pubkey: ScriptBuf::from_bytes(vec![0x51]),
                }],
            }
        }

        let genesis = bitcoin::blockdata::constants::genesis_block(bitcoin::Network::Regtest);
        accept_and_connect_block(&q, &params, Height::GENESIS, &genesis, ms).unwrap();
        let mut tip = genesis.block_hash();
        let mut tip_time = genesis.header.time;
        let b1 = mine_cb(tip, tip_time + 600, 1);
        let c1 = b1.txdata[0].compute_txid();
        accept_and_connect_block(&q, &params, Height(1), &b1, ms).unwrap();
        tip = b1.block_hash();
        tip_time = b1.header.time;
        for h in 2..=maturity + 1 {
            let b = mine_cb(tip, tip_time + 600, h);
            accept_and_connect_block(&q, &params, Height(h), &b, ms).unwrap();
            tip = b.block_hash();
            tip_time = b.header.time;
        }

        // S0: new Class A plan — stamp must fill parent body_range; load Forbid ok.
        let h_s0 = maturity + 2;
        let b_s0 = mine_with(
            tip,
            tip_time + 600,
            h_s0,
            vec![spend(c1, 0, Amount::from_sat(49_0000_0000))],
        );
        {
            let arcs = [(Height(h_s0), Arc::new(b_s0.clone()))];
            let stamped =
                confirm_wire_lookup_stamp(&q, &params, ms, &arcs, None).expect("S0 lookup");
            assert!(stamped.plan.is_some(), "S0 must plan Class A");
            assert!(
                !stamped.parent_pin.ranges.is_empty(),
                "S0 lookup must stamp external parent body ranges"
            );
            let mat = confirm_wire_load_from_plan(
                &q,
                &params,
                ms,
                stamped,
                None,
                &ScriptPreverified::new(),
                ColdPinMode::Forbid,
            )
            .expect("S0 load denserels by range");
            let ok = confirm_scripts_phase(mat.batch).expect("S0 scripts");
            confirm_write_phase(&q, &params, ms, ok.batch).expect("S0 write");
        }
        assert_eq!(q.tip_height().map(|h| h.0), Some(h_s0));
        tip = b_s0.block_hash();
        tip_time = b_s0.header.time;

        // S1: already-archived (plan=None) — lookup stamps parent pin; load by range.
        let h_s1 = h_s0 + 1;
        let b_s1 = mine_cb(tip, tip_time + 600, h_s1);
        accept_and_archive_block(&q, &params, Height(h_s1), &b_s1, ms).unwrap();
        assert_eq!(q.tip_height().map(|h| h.0), Some(h_s0));
        {
            let arcs = [(Height(h_s1), Arc::new(b_s1.clone()))];
            let stamped =
                confirm_wire_lookup_stamp(&q, &params, ms, &arcs, None).expect("S1 lookup");
            assert!(stamped.plan.is_none(), "S1 already-archived → plan=None");
            let mat = confirm_wire_load_from_plan(
                &q,
                &params,
                ms,
                stamped,
                None,
                &ScriptPreverified::new(),
                ColdPinMode::Forbid,
            )
            .expect("S1 plan=None load");
            let ok = confirm_scripts_phase(mat.batch).expect("S1 scripts");
            confirm_write_phase(&q, &params, ms, ok.batch).expect("S1 write");
        }
        assert_eq!(q.tip_height().map(|h| h.0), Some(h_s1));

        // Structural: lookup stage source must not denserels-decode body on stamp path.
        let src = include_str!("confirm_run.rs");
        let stamp_fn = src
            .split("pub fn confirm_wire_lookup_stamp")
            .nth(1)
            .and_then(|s| s.split("pub fn confirm_wire_load_from_plan").next())
            .expect("stamp fn slice");
        assert!(
            !stamp_fn.contains("get_outs_denserels_by_range_batch"),
            "lookup stamp must never body denserels-decode"
        );
        assert!(
            !stamp_fn.contains("IdxBodyMode::OutsDenserels"),
            "lookup stamp must never idx denserels body"
        );
        let load_pin = src
            .split("fn pin_for_wire_batch")
            .nth(1)
            .and_then(|s| s.split("pub fn confirm_scripts_phase").next())
            .expect("pin fn slice");
        assert!(
            !load_pin.contains("get_fk_by_txid("),
            "load pin must not probe head"
        );
        assert!(
            !load_pin.contains(".body_txid("),
            "load pin must not read txid.body"
        );
        assert!(
            !load_pin.contains("load_creates_once"),
            "load pin must not idx denserels via load_creates_once"
        );
        assert!(
            load_pin.contains("get_outs_denserels_by_range_batch"),
            "load pin must denserels by known body range"
        );

        let _ = std::fs::remove_dir_all(&path);
    }

    /// Load miss: spend edges without pin denserels must hard-fail (no cold tier).
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
        let meta = rbitcoin_query::U64Map::default();
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
        let dens =
            rbitcoin_store::denserels_from_packed_records(&parent_tx, &parent_ins, &parent_outs);
        let fks = q
            .store()
            .put_tx_full_batch_indexed(
                &[(parent_tx.clone(), parent_ins, parent_outs.clone())],
                /*index=*/ true,
            )
            .unwrap();
        let parent_fk = fks[0];
        let (body_off, body_len) = q.store().txs.body_range(parent_fk).unwrap();

        // Pin denserels without body_range (load-ahead shape before commit).
        let mut bp = BatchParents::new();
        bp.insert_owned(
            parent_fk,
            parent_tx,
            vec![(0, parent_outs[0].clone())],
            vec![0],
            Some(true),
            None,
            dens.iter()
                .enumerate()
                .map(|(i, r)| (i as u32, *r))
                .collect(),
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
        assert_eq!(
            cold, 0,
            "must not denserels-body cold when pin has denserels"
        );
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
        use std::collections::HashSet;
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
            None,   // no body_range
            vec![], // no denserels
        );

        let spends = vec![([7u8; 32], 0u32, Fk(100), parent_fk)];
        let ctx = crate::block::ValidationContext::at(&params, Height(1), Milestone::NONE);
        let mut pending = HashSet::new();
        let mut mtp = rbitcoin_query::U32Map::<u32>::default();
        let mut meta_by_abs = rbitcoin_query::U64Map::default();
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
            rbitcoin_query::BatchThin::default(),
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
                    // Filled after wire rebuild (sole hash for archived path).
                    txids: Vec::new(),
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
            txids: Vec::new(),
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
        // Once-computed at plan/structure — never rehash `block_hash()` here.
        let block_hash = meta.hash;
        let ctx = ValidationContext::at(params, height, milestone);

        // Prev-block MTP: resolved **once** for header rule + BIP16 + BIP113.
        let prev_mtp: u32;

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
                prev_mtp = mtp;
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
                    // Checkpoint uses once-computed meta.hash (no header rehash).
                    if let Some(cp) = params.checkpoint_at(height) {
                        if cp.to_byte_array() != block_hash {
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
                prev_mtp = 0;
                validate_header(query, params, height, &block.header)?;
            }
        } else {
            let prev = &prepared[i - 1];
            if block.header.prev_blockhash.to_byte_array() != prev.hash {
                return Err(ConsensusError::BadPrev);
            }
            // time_window ends at previous block → median is prev-block MTP.
            let mtp = median_time_past_times(&time_window);
            if block.header.time <= mtp {
                return Err(ConsensusError::BadHeader("timestamp <= median-time-past"));
            }
            prev_mtp = mtp;
            if let Some(cp) = params.checkpoint_at(height) {
                if cp.to_byte_array() != block_hash {
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

        let bip16_active =
            crate::block::bip16_active_from_prev_mtp(params, height.0, &block_hash, prev_mtp);

        let t_connect = Instant::now();
        let (script_jobs, spends, fees) = assemble_block_prevouts(
            query,
            block.as_ref(),
            &ctx,
            Some(&meta.tx_fks),
            &mut pending_spent,
            &mut pending_creates,
            batch_parents,
            batch_thin,
            &meta.txids,
            prev_mtp,
            &block_hash,
            bip16_active,
            Some(block), // share wire Arc — no Transaction clone into jobs
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
    meta_by_abs: &mut rbitcoin_query::U64Map<(rbitcoin_primitives::Fk, u8)>,
) -> Result<crate::block::StructuralPhaseNs, ConsensusError> {
    use crate::block::StructuralPhaseNs;
    let t0 = Instant::now();
    let mut pending_spent: HashSet<([u8; 32], u32)> = HashSet::new();
    // MTP of height H reused across blocks/spends in this write run.
    let mut mtp_cache: U32Map<u32> = U32Map::default();
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
    confirm_phase_stats::STRUCTURAL_NS.fetch_add(t0.elapsed().as_nanos() as u64, Ordering::Relaxed);
    confirm_phase_stats::STRUCTURAL_SPENT_NS.fetch_add(tot.spent_ns, Ordering::Relaxed);
    confirm_phase_stats::STRUCTURAL_SPENT_ABS_NS.fetch_add(tot.spent_abs_ns, Ordering::Relaxed);
    confirm_phase_stats::STRUCTURAL_SPENT_STRONG_NS
        .fetch_add(tot.spent_strong_ns, Ordering::Relaxed);
    confirm_phase_stats::STRUCTURAL_SPENT_COLD_NS.fetch_add(tot.spent_cold_ns, Ordering::Relaxed);
    confirm_phase_stats::STRUCTURAL_SPENT_PENDING_NS
        .fetch_add(tot.spent_pending_ns, Ordering::Relaxed);
    confirm_phase_stats::STRUCTURAL_CREATE_H_NS.fetch_add(tot.create_h_ns, Ordering::Relaxed);
    confirm_phase_stats::STRUCTURAL_BIP68_NS.fetch_add(tot.bip68_ns, Ordering::Relaxed);
    Ok(tot)
}

/// Verify script jobs in `prepared` (CPU only). Skips jobs whose txid is in
/// `preverified` (mempool already consensus-checked at accept).
fn script_wave(
    prepared: &[Prepared],
    preverified: &ScriptPreverified,
) -> Result<(), ConsensusError> {
    let t_script = Instant::now();
    let mut all_jobs: Vec<&ScriptCheckJob> = Vec::new();
    let mut n_skip = 0u64;
    for p in prepared {
        if !p.check_scripts {
            continue;
        }
        for job in &p.jobs {
            // txid attached at assemble — always consult mempool preverified (tip).
            if preverified.contains(&job.txid) {
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
    write_create_pins: &FkMap<rbitcoin_query::CreatePin>,
) -> Result<Vec<rbitcoin_primitives::Fk>, ConsensusError> {
    use rbitcoin_query::class_c_phase_stats::{STRONG_NS, TIP_NS};
    use std::sync::atomic::Ordering as QOrd;

    // CLASS_C_NS = strong + tip only (not join wall). SH runs in parallel and
    // has its own SCRIPTHASH_NS / SH_* meters — do not fold SH into class_c.
    let strong0 = STRONG_NS.load(QOrd::Relaxed);
    let tip0 = TIP_NS.load(QOrd::Relaxed);
    let items: Vec<rbitcoin_query::ConfirmPrepared> = prepared
        .iter_mut()
        .map(|p| rbitcoin_query::ConfirmPrepared {
            height: p.height,
            header_fk: p.header_fk,
            tx_fks: std::mem::take(&mut p.tx_fks),
        })
        .collect();
    let pins = if write_create_pins.is_empty() {
        None
    } else {
        Some(write_create_pins)
    };
    let out = query
        .confirm_blocks_run_with_create_pins(&items, pins)
        .map_err(ConsensusError::Store)?;
    let strong_d = STRONG_NS.load(QOrd::Relaxed).saturating_sub(strong0);
    let tip_d = TIP_NS.load(QOrd::Relaxed).saturating_sub(tip0);
    confirm_phase_stats::CLASS_C_NS.fetch_add(strong_d.saturating_add(tip_d), Ordering::Relaxed);
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
    meta_by_abs: &rbitcoin_query::U64Map<(rbitcoin_primitives::Fk, u8)>,
) -> Result<(u64, u64), ConsensusError> {
    // Confirm write (IBD + tip via accept_and_connect → confirm_archived_run):
    // batch durable spend annotations after Class C. Load pin must supply
    // denserels + body_range so every edge has abs layout — one path only.
    let t_spent = Instant::now();
    if query.spend_index_enabled() && query.index_mode().uses_durable_spends() {
        let mut abs_edges: Vec<(u64, rbitcoin_primitives::Fk, u32, rbitcoin_primitives::Fk)> =
            Vec::new();
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
                    "invariant: spend annotate abs cold (OOB or IO); load/layout bug",
                )));
            }
            let ann_ns = t_ann.elapsed().as_nanos() as u64;
            confirm_phase_stats::SPEND_ANN_NS.fetch_add(ann_ns, Ordering::Relaxed);
            confirm_phase_stats::SPEND_ANN_N.fetch_add(abs_edges.len() as u64, Ordering::Relaxed);
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
    confirm_phase_stats::UNPIN_NS.fetch_add(t_unpin.elapsed().as_nanos() as u64, Ordering::Relaxed);

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
    // Period-start may still be above confirmed tip during tip-ahead multi-block
    // load (i>0). Lookup/load already put_header_plan for that height — use it.
    let first_height = Height(height.0 - interval);
    let first_ts = if let Some((_fk, rec)) = query
        .header_at_height(first_height)
        .map_err(ConsensusError::Store)?
    {
        rec.timestamp
    } else if let Some(plan) = query
        .confirm_parent_cache()
        .get_header_plan(first_height.0)
    {
        plan.header_rec.timestamp
    } else {
        return Err(ConsensusError::BadHeader("missing retarget first header"));
    };
    let timespan = prev_time.saturating_sub(first_ts) as u64;
    Ok(CompactTarget::from_next_work_required(
        prev_bits,
        timespan,
        &params.btc,
    ))
}
