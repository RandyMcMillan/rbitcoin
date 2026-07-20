//! Block and transaction validation / confirmability.

mod block;
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
            tx_index: 0,
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
                tx_index: 0,
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
    /// Total reconstruct-ish wall (prefetch + wave fill + wire reconstruct).
    pub static RECONSTRUCT_NS: AtomicU64 = AtomicU64::new(0);
    /// Class A warm for wave-body txs (tx+ins+outs into class_a cache).
    pub static PREFETCH_CLASS_A_NS: AtomicU64 = AtomicU64::new(0);
    /// Wave prevout map build (parents + thin inputs; reuses class_a).
    pub static WAVE_FILL_NS: AtomicU64 = AtomicU64::new(0);
    /// Full wire `Block` rebuild from Class A rows.
    pub static RECONSTRUCT_WIRE_NS: AtomicU64 = AtomicU64::new(0);
    pub static CONNECT_NS: AtomicU64 = AtomicU64::new(0);
    pub static SCRIPT_NS: AtomicU64 = AtomicU64::new(0);
    /// Class C wall (`confirm_blocks_run` total).
    pub static CLASS_C_NS: AtomicU64 = AtomicU64::new(0);
    /// Process-local spend set updates (when durable points are off).
    pub static SPENT_LOCAL_NS: AtomicU64 = AtomicU64::new(0);
    pub static BLOCKS: AtomicU64 = AtomicU64::new(0);

    /// Sample and reset all confirm phases.
    ///
    /// Returns
    /// `(recon, prefetch, wave_fill, wire, connect, script, class_c, strong, scripthash, tip, spent_local, blocks)`.
    /// `strong` / `scripthash` / `tip` come from [`rbitcoin_query::class_c_phase_stats`]
    /// (sub-phases inside Class C). `recon` is the sum of the three reconstruct sub-timers.
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
    ) {
        let (strong, sh, tip) = rbitcoin_query::class_c_phase_stats::sample_and_reset();
        let prefetch = PREFETCH_CLASS_A_NS.swap(0, Ordering::Relaxed);
        let wave = WAVE_FILL_NS.swap(0, Ordering::Relaxed);
        let wire = RECONSTRUCT_WIRE_NS.swap(0, Ordering::Relaxed);
        // Prefer explicit sum of subs; fall back to legacy total if only that was bumped.
        let recon_total = RECONSTRUCT_NS.swap(0, Ordering::Relaxed);
        let recon = {
            let sum = prefetch.saturating_add(wave).saturating_add(wire);
            if sum > 0 {
                sum
            } else {
                recon_total
            }
        };
        (
            recon,
            prefetch,
            wave,
            wire,
            CONNECT_NS.swap(0, Ordering::Relaxed),
            SCRIPT_NS.swap(0, Ordering::Relaxed),
            CLASS_C_NS.swap(0, Ordering::Relaxed),
            strong,
            sh,
            tip,
            SPENT_LOCAL_NS.swap(0, Ordering::Relaxed),
            BLOCKS.swap(0, Ordering::Relaxed),
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

/// Confirm a contiguous tip-extension run of archived bodies.
///
/// When archive leads tip (the post-milestone IBD case), a multi-height run:
/// 1. Connects sequentially (pending spends — no `spent_local` poison on failure)
/// 2. Runs **one** rayon script wave across **all** inputs in the run (fills cores
///    even when single blocks under-utilize workers)
/// 3. Class C in height order
///
/// Single-height calls are the `blocks.len()==1` path (same as before).
///
/// Intermediate heights are not yet in `confirmed[]`, so header checks after the
/// first use the previous block in the run (not `header_at_height`).
pub fn confirm_archived_run(
    query: &Query,
    params: &ChainParams,
    milestone: Milestone,
    blocks: &[(Height, [u8; 32])],
) -> Result<Vec<rbitcoin_primitives::Fk>, ConsensusError> {
    use crate::block::{connect_block_prevouts, ScriptCheckJob};
    use crate::header::median_time_past_times;
    use std::collections::HashSet;
    use std::sync::atomic::Ordering;
    use std::time::Instant;

    if blocks.is_empty() {
        return Ok(Vec::new());
    }
    // Core double-spend: local-only oracle must be complete before any confirm.
    query
        .ensure_spent_oracle_ready()
        .map_err(ConsensusError::Store)?;
    for w in blocks.windows(2) {
        if w[1].0 .0 != w[0].0 .0.saturating_add(1) {
            return Err(ConsensusError::BadBlock("confirm run not contiguous"));
        }
    }

    struct Prepared {
        height: Height,
        header_fk: rbitcoin_primitives::Fk,
        tx_fks: Vec<rbitcoin_primitives::Fk>,
        jobs: Vec<ScriptCheckJob>,
        spends: Vec<([u8; 32], u32)>,
        check_scripts: bool,
        time: u32,
        bits: bitcoin::CompactTarget,
        /// Header hash of this block (prev-link for the next height in the run).
        hash: [u8; 32],
    }

    let mut pending_spent: HashSet<([u8; 32], u32)> = HashSet::new();
    // Same-run creates (overlay for mmap UTXO until Class C apply).
    let mut pending_creates: HashSet<([u8; 32], u32)> = HashSet::new();
    let mut time_window: Vec<u32> = Vec::with_capacity(11);
    let mut prepared: Vec<Prepared> = Vec::with_capacity(blocks.len());

    // (A) Wave prep: Class A warm (body) then parent/thin-input fill, then wire
    // reconstruct. Sub-timers split so logs show where the IO sits.
    let hashes: Vec<[u8; 32]> = blocks.iter().map(|(_, h)| *h).collect();
    let wave_prevouts = {
        let t0 = Instant::now();
        let _ = query
            .prefetch_class_a_for_block_hashes(&hashes)
            .map_err(ConsensusError::Store)?;
        let ns = t0.elapsed().as_nanos() as u64;
        confirm_phase_stats::PREFETCH_CLASS_A_NS.fetch_add(ns, Ordering::Relaxed);
        confirm_phase_stats::RECONSTRUCT_NS.fetch_add(ns, Ordering::Relaxed);

        // Parent/wave map: reuses class_a for bodies; sparse parent outputs only.
        let t1 = Instant::now();
        let (_n, wave) = query
            .prefetch_tip_prevouts_for_block_hashes(&hashes)
            .map_err(ConsensusError::Store)?;
        let ns = t1.elapsed().as_nanos() as u64;
        confirm_phase_stats::WAVE_FILL_NS.fetch_add(ns, Ordering::Relaxed);
        confirm_phase_stats::RECONSTRUCT_NS.fetch_add(ns, Ordering::Relaxed);
        wave
    };

    for (i, &(height, block_hash)) in blocks.iter().enumerate() {
        let t0 = Instant::now();
        let block = query
            .reconstruct_archived_block(&block_hash)
            .map_err(ConsensusError::Store)?
            .ok_or(ConsensusError::Store(StoreError::NotFound))?;
        let ns = t0.elapsed().as_nanos() as u64;
        confirm_phase_stats::RECONSTRUCT_WIRE_NS.fetch_add(ns, Ordering::Relaxed);
        confirm_phase_stats::RECONSTRUCT_NS.fetch_add(ns, Ordering::Relaxed);

        let ctx = ValidationContext {
            params,
            height,
            milestone,
        };

        if i == 0 {
            validate_header(query, params, height, &block.header)?;
            if height.0 >= 1 {
                let prev_h = Height(height.0 - 1);
                let start = prev_h.0.saturating_sub(10);
                for h in start..=prev_h.0 {
                    let (_fk, rec) = query
                        .header_at_height(Height(h))
                        .map_err(ConsensusError::Store)?
                        .ok_or(ConsensusError::BadPrev)?;
                    time_window.push(rec.timestamp);
                }
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
            // Difficulty: non-retarget inherits prev bits; retarget still needs
            // period-start from the confirmed chain (batches ≪ retarget interval).
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

        // BIP34 buried activation (mainnet 227931 — not height 1).
        if params.bip34_active_at(height.0) {
            check_bip34_at_height(&block, height.0)?;
        }

        // BIP325: full signet challenge (CHECKMULTISIG) on tip confirm only —
        // never on archive structure (IBD prep hot path).
        if height.0 > 0 {
            if let Some(challenge) = params.signet_challenge.as_ref() {
                crate::signet::validate_signet_block_solution(&block, challenge.as_script())?;
            }
        }

        let (header_fk, _) = query
            .get_header_by_hash(&block_hash)
            .map_err(ConsensusError::Store)?
            .ok_or(ConsensusError::Store(StoreError::NotFound))?;
        let tx_fks = query
            .store()
            .header_txs
            .get_list(header_fk)
            .map_err(ConsensusError::Store)?
            .ok_or(ConsensusError::Store(StoreError::Corrupt(
                "confirm without archived body",
            )))?;

        let t_connect = Instant::now();
        let (script_jobs, spends) = connect_block_prevouts(
            query,
            &block,
            &ctx,
            Some(&tx_fks),
            Some(&wave_prevouts),
            &mut pending_spent,
            &mut pending_creates,
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
            header_fk,
            tx_fks,
            jobs: script_jobs,
            spends,
            check_scripts: !milestone.skips_scripts_at(height.0),
            time: block.header.time,
            bits: block.header.bits,
            hash: block_hash,
        });
    }

    // One script wave across the whole run (fine per-tx rayon units).
    // Multi-block: single flattened job list so the pool sees one large wave
    // (better steal balance than per-block sequential verify).
    let t_script = Instant::now();
    {
        let mut n_slices = 0usize;
        let mut total_jobs = 0usize;
        let mut only: Option<&[ScriptCheckJob]> = None;
        for p in &prepared {
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
                for p in &prepared {
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

    // Class C: one batched tip extension (no per-block hash re-lookup).
    let t_class_c = Instant::now();
    let n_blocks = prepared.len();
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

    // Collect spends once (spent_local + tip_prevout retirement).
    // Note: `p.tx_fks` was moved into `items` for Class C — do not read it here.
    let mut all_spends: Vec<([u8; 32], u32)> = Vec::new();
    for p in &prepared {
        all_spends.extend_from_slice(&p.spends);
    }

    // Update catch-up spentness oracle + tip_prevout after Class C success.
    let t_spent = Instant::now();
    if query.ibd_utxo_enabled() {
        // Per-height apply: spends then creates. Critical for multi-block runs
        // where height H+1 spends a create from height H in the same batch.
        // Creates come from `items` (tx_fks after mem::take), not emptied prepared.
        // Class C has already advanced tip — never surface UTXO apply as a confirm
        // reject / blacklist (was `ibd utxo duplicate create` @519). Heal via rebuild.
        let apply_res = (|| -> Result<(), ConsensusError> {
            for (p, item) in prepared.iter().zip(items.iter()) {
                let mut creates =
                    Vec::with_capacity(item.tx_fks.len().saturating_mul(2));
                for &fk in &item.tx_fks {
                    let tx = query
                        .get_tx_class_a(fk)
                        .map_err(ConsensusError::Store)?;
                    for v in 0..tx.output_count {
                        creates.push((tx.txid, v));
                    }
                }
                query
                    .apply_ibd_utxo_block(&p.spends, &creates, item.height.0)
                    .map_err(ConsensusError::Store)?;
            }
            Ok(())
        })();
        if let Err(_e) = apply_res {
            query
                .rebuild_ibd_utxo_to_tip()
                .map_err(ConsensusError::Store)?;
        }
    } else {
        query.note_outpoints_spent_local(&all_spends);
    }
    confirm_phase_stats::SPENT_LOCAL_NS
        .fetch_add(t_spent.elapsed().as_nanos() as u64, Ordering::Relaxed);

    // Only after successful Class C: drop spent vouts from tip_prevout so the
    // FIFO budget holds unspent creates / parents, not dead shells.
    query.retire_tip_prevout_spends(&all_spends);

    confirm_phase_stats::BLOCKS.fetch_add(n_blocks as u64, Ordering::Relaxed);
    Ok(out)
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

fn check_bip34_at_height(block: &Block, height: u32) -> Result<(), ConsensusError> {
    use crate::block::bip34_height_script;
    let coinbase = &block.txdata[0];
    let bytes = coinbase.input[0].script_sig.as_bytes();
    let expected = bip34_height_script(height);
    if bytes.len() < expected.len() || &bytes[..expected.len()] != expected.as_slice() {
        return Err(ConsensusError::BadBlock("bip34 height encoding"));
    }
    Ok(())
}

/// Accept + archive + confirm in one step (genesis / tip extension tests).
pub fn accept_and_connect_block(
    query: &Query,
    params: &ChainParams,
    height: Height,
    block: &Block,
    milestone: Milestone,
) -> Result<rbitcoin_primitives::Fk, ConsensusError> {
    let ctx = ValidationContext {
        params,
        height,
        milestone,
    };
    let txids = validate_block_structure_hashed(block, &ctx)?;
    validate_header(query, params, height, &block.header)?;
    validate_block_connect(query, block, &ctx, None)?;
    let (header_rec, txs) = block_to_apply_with_txids(query, &block.header, &block.txdata, &txids)?;
    let fk = query
        .connect_block(height, &header_rec, &txs)
        .map_err(ConsensusError::Store)?;
    // mmap UTXO only after Class C (same order as `confirm_archived_run`).
    if query.ibd_utxo_enabled() {
        let mut spends = Vec::new();
        let mut creates = Vec::new();
        for (ti, tx) in block.txdata.iter().enumerate() {
            let tid = txids[ti];
            if ti > 0 {
                for inp in &tx.input {
                    let op = inp.previous_output;
                    spends.push((op.txid.to_byte_array(), op.vout));
                }
            }
            for (v, _) in tx.output.iter().enumerate() {
                creates.push((tid, v as u32));
            }
        }
        query
            .apply_ibd_utxo_block(&spends, &creates, height.0)
            .map_err(ConsensusError::Store)?;
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
    let ctx = ValidationContext {
        params,
        height: Height::GENESIS,
        milestone: Milestone::NONE,
    };
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
    let ctx = ValidationContext {
        params,
        height: Height::GENESIS,
        milestone: Milestone::NONE,
    };
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
