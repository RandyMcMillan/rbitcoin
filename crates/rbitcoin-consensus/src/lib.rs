//! Block and transaction validation / confirmability.

mod block;
mod confirm_run;
mod convert;
mod error;
mod header;
mod milestone;
mod params;
pub mod policy;
mod script;
mod signet;

/// Thin public helpers for `script_verify` benches (no UTXO; explicit prevouts only).
pub mod script_bench {
    use bitcoin::{Transaction, TxOut};

    use crate::block::ScriptCheckJob;
    use crate::error::ConsensusError;
    use crate::script;

    /// Owned job payload for benches outside the crate root.
    pub struct JobBytes {
        pub prevouts: Vec<TxOut>,
        pub tx: Transaction,
        /// When false, use pre-BIP66 lax DER (historical mainnet).
        pub bip66_active: bool,
        /// When false, P2SH template is bare HASH160/EQUAL (pre-BIP16).
        pub bip16_active: bool,
    }

    impl JobBytes {
        pub fn new(prevouts: Vec<TxOut>, tx: Transaction) -> Self {
            Self {
                prevouts,
                tx,
                bip66_active: true,
                bip16_active: true,
            }
        }
    }

    pub fn verify_job(job: &JobBytes) -> Result<(), ConsensusError> {
        // Zero-copy view — mirrors production (`&ScriptCheckJob` from connect).
        let j = ScriptCheckJob {
            prevouts: job.prevouts.clone(),
            tx: job.tx.clone(),
            bip65_active: true,
            bip112_active: true,
            bip66_active: job.bip66_active,
            bip16_active: job.bip16_active,
        };
        script::verify_job_all_inputs(&j)
    }

    /// Build owned jobs once, then pool-verify without re-cloning each iteration.
    pub fn owned_jobs(jobs: &[JobBytes]) -> Vec<ScriptCheckJob> {
        jobs.iter()
            .map(|j| ScriptCheckJob {
                prevouts: j.prevouts.clone(),
                tx: j.tx.clone(),
                bip65_active: true,
                bip112_active: true,
                bip66_active: j.bip66_active,
                bip16_active: j.bip16_active,
            })
            .collect()
    }

    pub fn verify_jobs_pool(jobs: &[JobBytes]) -> Result<(), ConsensusError> {
        let owned = owned_jobs(jobs);
        // Prefer the owned-slice entry point (no intermediate `Vec<&_>`).
        crate::block::verify_scripts_pool(&owned)
    }

    pub fn verify_owned_pool(jobs: &[ScriptCheckJob]) -> Result<(), ConsensusError> {
        crate::block::verify_scripts_pool(jobs)
    }

    /// Single job (no pool) — for parallelization strategy benches.
    pub fn verify_one_job(job: &ScriptCheckJob) -> Result<(), ConsensusError> {
        script::verify_job_all_inputs(job)
    }
}

// Re-export job type for benches that drive parallel strategies.
pub use block::ScriptCheckJob;

pub use block::{
    bip34_height_script, block_subsidy, validate_block_connect, validate_block_structure,
    validate_block_structure_hashed, ValidationContext,
};
pub use convert::{block_to_apply, block_to_apply_with_txids, header_to_record};
pub use error::ConsensusError;
pub use header::{expected_next_bits, median_time_past, validate_header};
pub use milestone::Milestone;
pub use params::{default_milestone_height, genesis_block, ChainParams, Checkpoint};
pub use policy::{check_tx_standard, is_push_only, is_standard_script_pubkey, PolicyResult};
pub use signet::{default_signet_challenge, validate_signet_block_solution};

pub fn crate_name() -> &'static str {
    "rbitcoin-consensus"
}

use bitcoin::hashes::Hash;
use bitcoin::{Block, Target};
use rbitcoin_primitives::Height;
use rbitcoin_query::{Query, TxApply};
use rbitcoin_store::{HeaderRecord, StoreError};

/// Confirm the next tip block if its body is already archived.
///
/// IBD diagnostics: wall time spent in each phase (nanoseconds; reset by the sampler).
pub mod confirm_phase_stats {
    use std::sync::atomic::{AtomicU64, Ordering};
    /// Total reconstruct-ish wall (wire rebuild; historical total).
    pub static RECONSTRUCT_NS: AtomicU64 = AtomicU64::new(0);
    /// Full wire `Block` rebuild from Class A rows.
    pub static RECONSTRUCT_WIRE_NS: AtomicU64 = AtomicU64::new(0);
    /// Optimistic assemble (prevout content + jobs; no durable spentness).
    pub static CONNECT_NS: AtomicU64 = AtomicU64::new(0);
    pub static SCRIPT_NS: AtomicU64 = AtomicU64::new(0);
    /// Post-script durable spentness + maturity + subsidy.
    pub static STRUCTURAL_NS: AtomicU64 = AtomicU64::new(0);
    /// Class C wall (`confirm_blocks_run` total).
    pub static CLASS_C_NS: AtomicU64 = AtomicU64::new(0);
    /// Post–Class C durable spend annotation batch.
    ///
    /// Historical name `UTXO_APPLY_NS` / log field `utxo_ms` — this is **not** a
    /// light-UTXO map apply (Catchup removed). Wall time for all annotate paths.
    pub static UTXO_APPLY_NS: AtomicU64 = AtomicU64::new(0);
    /// Annotate edges using cache-held body range (no idx).
    pub static SPEND_ANNOTATE_RANGED: AtomicU64 = AtomicU64::new(0);
    /// Annotate edges with create_fk but no body_range (idx path).
    pub static SPEND_ANNOTATE_IDX: AtomicU64 = AtomicU64::new(0);
    /// Spends skipped (null create_fk or null spend_fk).
    pub static SPEND_ANNOTATE_SKIP: AtomicU64 = AtomicU64::new(0);
    /// Header + body-fk resolve for the batch.
    pub static RESOLVE_NS: AtomicU64 = AtomicU64::new(0);
    /// Confirm load wall (Class A + parent pin) on the load thread.
    pub static LOAD_NS: AtomicU64 = AtomicU64::new(0);
    /// Unpin spent outs from ConfirmParentCache after Class C.
    pub static UNPIN_NS: AtomicU64 = AtomicU64::new(0);
    /// `advance_parent_cache_tip` (drop bodies / GC parents).
    pub static CACHE_TIP_NS: AtomicU64 = AtomicU64::new(0);
    pub static BLOCKS: AtomicU64 = AtomicU64::new(0);

    /// Sample and reset all confirm phases.
    ///
    /// Returns
    /// `(recon, wire, connect, script, class_c, strong, scripthash, tip,
    ///   utxo_apply, blocks, resolve, load, unpin, cache_tip,
    ///   spend_ranged, spend_idx, spend_skip)`.
    /// `strong` / `scripthash` / `tip` come from [`rbitcoin_query::class_c_phase_stats`].
    /// `recon` prefers wire sub-timer, else legacy total.
    #[allow(clippy::type_complexity)]
    pub fn sample_and_reset()
    -> (
        u64,
        u64,
        u64,
        u64,
        u64,
        u64,
        u64,
        u64,
        u64,
        u64,
        u64,
        u64,
        u64,
        u64,
        u64,
        u64,
        u64,
    ) {
        let (strong, sh, tip) = rbitcoin_query::class_c_phase_stats::sample_and_reset();
        let wire = RECONSTRUCT_WIRE_NS.swap(0, Ordering::Relaxed);
        let recon_total = RECONSTRUCT_NS.swap(0, Ordering::Relaxed);
        let recon = if wire > 0 { wire } else { recon_total };
        (
            recon,
            wire,
            CONNECT_NS.swap(0, Ordering::Relaxed),
            SCRIPT_NS.swap(0, Ordering::Relaxed),
            CLASS_C_NS.swap(0, Ordering::Relaxed),
            strong,
            sh,
            tip,
            UTXO_APPLY_NS.swap(0, Ordering::Relaxed),
            BLOCKS.swap(0, Ordering::Relaxed),
            RESOLVE_NS.swap(0, Ordering::Relaxed),
            LOAD_NS.swap(0, Ordering::Relaxed),
            UNPIN_NS.swap(0, Ordering::Relaxed),
            CACHE_TIP_NS.swap(0, Ordering::Relaxed),
            SPEND_ANNOTATE_RANGED.swap(0, Ordering::Relaxed),
            SPEND_ANNOTATE_IDX.swap(0, Ordering::Relaxed),
            SPEND_ANNOTATE_SKIP.swap(0, Ordering::Relaxed),
        )
    }
}

/// Confirm one archived tip+1 block (see [`confirm_archived_run`]).
pub fn confirm_archived_at(
    query: &Query,
    params: &ChainParams,
    height: Height,
    block_hash: &[u8; 32],
    milestone: Milestone,
) -> Result<rbitcoin_primitives::Fk, ConsensusError> {
    let fks = confirm_archived_run(query, params, milestone, &[(height, *block_hash)])?;
    Ok(fks[0])
}

/// Confirm a contiguous tip-extension run of archived bodies (sync all stages).
///
/// See [`confirm_run`]: load → scripts → write. IBD uses the split
/// phases for 3-stage pipeline overlap.
pub use confirm_run::{
    confirm_archived_run, confirm_load_phase, confirm_script_phase, confirm_scripts_phase,
    confirm_write_phase, ConfirmLoadOutcome, ConfirmScriptOutcome, LoadedBatch,
    ScriptOkBatch,
};

/// Accept + archive + confirm in one step (genesis / tip extension tests).
pub fn accept_and_connect_block(
    query: &Query,
    params: &ChainParams,
    height: Height,
    block: &Block,
    milestone: Milestone,
) -> Result<rbitcoin_primitives::Fk, ConsensusError> {
    let ctx = ValidationContext::at(params, height, milestone);
    let txids = validate_block_structure_hashed(block, &ctx)?;
    validate_header(query, params, height, &block.header)?;
    validate_block_connect(query, block, &ctx, None)?;
    let (header_rec, txs) = block_to_apply_with_txids(query, &block.header, &block.txdata, &txids)?;
    let fk = query
        .connect_block(height, &header_rec, &txs)
        .map_err(ConsensusError::Store)?;
    // Post Class C (same order as `confirm_archived_run`): durable spend annotate.
    if query.spend_index_enabled() {
        let tx_fks = query
            .store()
            .header_txs
            .get_list(fk)
            .map_err(ConsensusError::Store)?
            .ok_or(ConsensusError::Store(StoreError::NotFound))?;
        let mut by_create: Vec<(
            rbitcoin_primitives::Fk,
            u32,
            rbitcoin_primitives::Fk,
        )> = Vec::new();
        for (ti, tx) in block.txdata.iter().enumerate() {
            if ti == 0 {
                continue;
            }
            let spend_fk = tx_fks.get(ti).copied().unwrap_or(rbitcoin_primitives::Fk::NULL);
            if spend_fk.is_null() {
                continue;
            }
            for inp in &tx.input {
                let op = inp.previous_output;
                if let Some(cfk) = query
                    .tx_fk_by_txid(op.txid.as_byte_array())
                    .map_err(ConsensusError::Store)?
                {
                    by_create.push((cfk, op.vout, spend_fk));
                }
            }
        }
        if !by_create.is_empty() {
            query
                .store()
                .put_spend_batch_by_create(&by_create)
                .map_err(ConsensusError::Store)?;
        }
    }
    Ok(fk)
}

/// Archive a block body (Class A) without confirming.
pub fn accept_and_archive_block(
    query: &Query,
    params: &ChainParams,
    height: Height,
    block: &Block,
    milestone: Milestone,
) -> Result<(), ConsensusError> {
    let _ = (height, milestone);
    let (header_rec, txs) = prepare_block_for_archive(query, params, block)?;
    query
        .archive_block(&header_rec, &txs)
        .map_err(ConsensusError::Store)?;
    Ok(())
}

/// CPU-side prep for Class A archive.
pub fn prepare_block_for_archive(
    query: &Query,
    params: &ChainParams,
    block: &Block,
) -> Result<(HeaderRecord, Vec<TxApply>), ConsensusError> {
    let hash = block.block_hash().to_byte_array();
    if query
        .is_block_archived(&hash)
        .map_err(ConsensusError::Store)?
    {
        return block_to_apply(query, &block.header, &block.txdata);
    }
    prepare_block_for_archive_new(query, params, block)
}

pub fn prepare_block_for_archive_new(
    query: &Query,
    params: &ChainParams,
    block: &Block,
) -> Result<(HeaderRecord, Vec<TxApply>), ConsensusError> {
    // Height-gated soft forks (BIP34 / pre-segwit witness ban) deferred to confirm.
    let ctx = ValidationContext::archive_structure(params);
    let txids = validate_block_structure_hashed(block, &ctx)?;
    let target = Target::from_compact(block.header.bits);
    if target > params.pow_limit {
        return Err(ConsensusError::BadHeader("target above pow limit"));
    }
    block
        .header
        .validate_pow(target)
        .map_err(|_| ConsensusError::InvalidPow)?;
    let prev = block.header.prev_blockhash;
    if prev.to_byte_array() != [0u8; 32]
        && query
            .get_header_by_hash(prev.as_byte_array())
            .map_err(ConsensusError::Store)?
            .is_none()
    {
        return Err(ConsensusError::BadPrev);
    }
    block_to_apply_with_txids(query, &block.header, &block.txdata, &txids)
}

/// IBD multi-prep: structure + PoW + TxApply with no store access.
pub fn prepare_block_for_archive_ibd(
    params: &ChainParams,
    block: &Block,
) -> Result<(HeaderRecord, Vec<TxApply>), ConsensusError> {
    // Soft-fork height gates at confirm (true height). See ValidationContext::archive_structure.
    let ctx = ValidationContext::archive_structure(params);
    let txids = validate_block_structure_hashed(block, &ctx)?;
    let target = Target::from_compact(block.header.bits);
    if target > params.pow_limit {
        return Err(ConsensusError::BadHeader("target above pow limit"));
    }
    block
        .header
        .validate_pow(target)
        .map_err(|_| ConsensusError::InvalidPow)?;
    // prev_fk left NULL — writer attaches by known header_fk.
    use convert::block_to_apply_with_txids_prev;
    block_to_apply_with_txids_prev(
        rbitcoin_primitives::Fk::NULL,
        &block.header,
        &block.txdata,
        &txids,
    )
}
