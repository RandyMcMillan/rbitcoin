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
use rbitcoin_query::{FkMap, Query, U32Map, U64Map, U64Set};
use rbitcoin_store::{SpendAnnBackend, StoreError};
use std::collections::{HashMap, HashSet};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Instant;

mod pin;
use pin::{ensure_spend_abs_layouts, pin_for_wire_batch};

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
    confirm_wire_load_phase_pipelined(query, params, milestone, blocks, preverified, None)
}

/// Like [`confirm_wire_load_phase`] with optional pipeline caches for load-ahead.
///
/// Single pin path: denserels by body range from lookup stamp (no cold dual path).
pub fn confirm_wire_load_phase_pipelined(
    query: &Query,
    params: &ChainParams,
    milestone: Milestone,
    blocks: &[(Height, Block)],
    preverified: &ScriptPreverified,
    pipeline: Option<&WireLoadPipeline>,
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
            .map_err(ConsensusError::from)?
        {
            fk
        } else {
            query
                .store()
                .put_header(&header_rec)
                .map_err(ConsensusError::from)?
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
        .map_err(ConsensusError::from)?;
    let mut plan = if need.is_empty() {
        for (i, m) in metas.iter_mut().enumerate() {
            if let Some(list) = query
                .store()
                .header_txs
                .get_list(m.header_fk)
                .map_err(ConsensusError::from)?
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
                .map_err(ConsensusError::from)?,
            None => query
                .archive_plan_batch_owned(&mut need)
                .map_err(ConsensusError::from)?,
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
                    .map_err(ConsensusError::from)?
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
    let mat = confirm_wire_load_from_plan(query, params, milestone, stamped, None, preverified)?;
    let ok = confirm_scripts_phase(mat.batch)?;
    confirm_write_phase(query, params, milestone, ok.batch)
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
/// After this returns, load pin must see every external parent covered via
/// plan-local map, in-flight, or same-batch (no cold denserels dual path on load).
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
                .map_err(ConsensusError::from)?;
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
            .map_err(ConsensusError::from)?;
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
        self.txids
            .get(&create_fk_id)
            .copied()
            .filter(|t| *t != [0u8; 32])
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
            .map_err(ConsensusError::from)?;
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
    let mut seen = U64Set::default();
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
            .map_err(ConsensusError::from)?;
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
    // Identities are stamped from wire prev_txid at insert time — never soft-fill
    // from txid.body here (that would be a dual path after lookup promised identity).
    for (&id, tid) in &stamp.txids {
        if *tid == [0u8; 32] {
            return Err(ConsensusError::Store(StoreError::Corrupt(
                "invariant: plan=None parent stamp zero create identity",
            )));
        }
        let _ = id;
    }
    Ok(stamp)
}

/// IBD **load** after lookup denserels ensure: pin + assemble.
///
/// Uses the owned stamped plan — does **not** re-run plan_batch / head resolve.
/// Single path: denserels by body range from lookup stamp (plan-local or
/// plan=None `ParentPinStamp`). Never cold dual-path denserels / txid.body on load.
pub fn confirm_wire_load_from_plan(
    query: &Query,
    params: &ChainParams,
    milestone: Milestone,
    stamped: PlanStampOutcome,
    pipeline: Option<&WireLoadPipeline>,
    preverified: &ScriptPreverified,
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
            .map_err(ConsensusError::from)?
        {
            fk
        } else {
            query
                .store()
                .put_header(&header_rec)
                .map_err(ConsensusError::from)?
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
        .map_err(ConsensusError::from)?;
    let filter_ns = t_filter.elapsed().as_nanos() as u64;
    let t_batch = Instant::now();
    let plan = if need.is_empty() {
        for (i, m) in metas.iter_mut().enumerate() {
            if let Some(list) = query
                .store()
                .header_txs
                .get_list(m.header_fk)
                .map_err(ConsensusError::from)?
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
                .map_err(ConsensusError::from)?,
            None => query
                .archive_plan_batch_owned(&mut need)
                .map_err(ConsensusError::from)?,
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
                    .map_err(ConsensusError::from)?
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
        .map_err(ConsensusError::from)?;
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
                "scripts phase: worker disconnected before result",
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

/// Submit [`confirm_scripts_phase`] on a detached worker thread without
/// blocking the caller.
///
/// The OS scripts thread must keep claiming N+1 **while** waiting on N’s
/// [`ScriptsPhaseHandle::recv_timeout`] (not only once before a blocking join).
pub fn confirm_scripts_phase_async(batch: LoadedBatch) -> ScriptsPhaseHandle {
    scripts_feed_test_sync::on_async_submit();
    let (tx, rx) = std::sync::mpsc::sync_channel(1);
    crate::script_pool::spawn_detached(move || {
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
                    "scripts phase: worker disconnected before result",
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
                .map_err(ConsensusError::from)?;
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
        .map_err(ConsensusError::from)?;
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
        .map_err(ConsensusError::from)?;
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
        // ConfirmParentCache miss or hash mismatch: load header meta from store
        // (cold query path — not a soft recovery for a promised plan hit).
        let (header_fk, header_rec) = query
            .get_header_by_hash(&hash)
            .map_err(ConsensusError::from)?
            .ok_or(ConsensusError::Store(StoreError::NotFound))?;
        let tx_fks = query
            .header_tx_fks(header_fk, Some(&hash))
            .map_err(ConsensusError::from)?
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
                .map_err(ConsensusError::from)?,
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
                        .map_err(ConsensusError::from)?
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
                    .map_err(ConsensusError::from)?
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
        .map_err(ConsensusError::from)?;
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
                .map_err(ConsensusError::from)?;
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
    if params.no_pow_retargeting() || !height.0.is_multiple_of(interval) {
        return Ok(prev_bits);
    }
    // Period-start may still be above confirmed tip during tip-ahead multi-block
    // load (i>0). Lookup/load already put_header_plan for that height — use it.
    let first_height = Height(height.0 - interval);
    let first_ts = if let Some((_fk, rec)) = query
        .header_at_height(first_height)
        .map_err(ConsensusError::from)?
    {
        rec.timestamp
    } else if let Some(plan) = query.confirm_parent_cache().get_header_plan(first_height.0) {
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

#[cfg(test)]
mod write_idempotent_tests;
