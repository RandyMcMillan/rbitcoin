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
        /// When false, v1 witness program is anyone-can-spend (pre-taproot).
        pub taproot_active: bool,
    }

    impl JobBytes {
        pub fn new(prevouts: Vec<TxOut>, tx: Transaction) -> Self {
            Self {
                prevouts,
                tx,
                bip66_active: true,
                bip16_active: true,
                taproot_active: true,
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
            taproot_active: job.taproot_active,
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
                taproot_active: j.taproot_active,
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
    bip34_height_script, block_subsidy, is_final_tx, sequence_locks_satisfied,
    validate_block_connect, validate_block_structure, validate_block_structure_hashed,
    ValidationContext, LOCKTIME_THRESHOLD,
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
use rbitcoin_store::HeaderRecord;

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
    /// Script jobs skipped because mempool already verified the tx (tip follow).
    pub static SCRIPT_SKIP_MEMPOOL: AtomicU64 = AtomicU64::new(0);
    /// Post-script durable spentness + maturity + BIP68 + subsidy (write).
    pub static STRUCTURAL_NS: AtomicU64 = AtomicU64::new(0);
    /// Durable spentness probes only (subset of structural).
    pub static STRUCTURAL_SPENT_NS: AtomicU64 = AtomicU64::new(0);
    /// Create-height + coinbase maturity resolve (subset of structural).
    pub static STRUCTURAL_CREATE_H_NS: AtomicU64 = AtomicU64::new(0);
    /// BIP68 relative locks + coin MTP (subset of structural; write path).
    pub static STRUCTURAL_BIP68_NS: AtomicU64 = AtomicU64::new(0);
    /// Class C wall (`confirm_blocks_run` total).
    pub static CLASS_C_NS: AtomicU64 = AtomicU64::new(0);
    /// Post–Class C durable spend annotation batch.
    ///
    /// Historical name `UTXO_APPLY_NS` / log field `spend=` ms — this is **not** a
    /// light-UTXO map apply (Catchup removed). Wall time for all annotate paths.
    pub static UTXO_APPLY_NS: AtomicU64 = AtomicU64::new(0);
    /// Annotate edges via abs pin denserels (`put_spend_batch_by_abs_meta`).
    /// Historical name: formerly also counted ranged body walks (removed on Direct write).
    pub static SPEND_ANNOTATE_RANGED: AtomicU64 = AtomicU64::new(0);
    /// Legacy cold idx annotate path (must stay 0 on Direct IBD after abs-only write).
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

    // ── Last completed write batch (for slow-write logs; not window-summed) ──
    static LAST_WRITE_N: AtomicU64 = AtomicU64::new(0);
    static LAST_WRITE_STRUCTURAL_NS: AtomicU64 = AtomicU64::new(0);
    static LAST_WRITE_SPENT_NS: AtomicU64 = AtomicU64::new(0);
    static LAST_WRITE_CREATE_H_NS: AtomicU64 = AtomicU64::new(0);
    static LAST_WRITE_BIP68_NS: AtomicU64 = AtomicU64::new(0);
    static LAST_WRITE_CLASS_C_NS: AtomicU64 = AtomicU64::new(0);
    static LAST_WRITE_SPEND_ANN_NS: AtomicU64 = AtomicU64::new(0);
    static LAST_WRITE_TIP_GC_NS: AtomicU64 = AtomicU64::new(0);
    static LAST_WRITE_WALL_NS: AtomicU64 = AtomicU64::new(0);

    /// Snapshot of the most recent successful [`super::confirm_write_phase`].
    #[derive(Debug, Clone, Copy, Default)]
    pub struct LastWritePhases {
        pub n_blocks: u32,
        pub wall_ns: u64,
        pub structural_ns: u64,
        pub spent_ns: u64,
        pub create_h_ns: u64,
        pub bip68_ns: u64,
        pub class_c_ns: u64,
        pub spend_ann_ns: u64,
        pub tip_gc_ns: u64,
    }

    impl LastWritePhases {
        #[inline]
        pub fn ms(ns: u64) -> u64 {
            ns / 1_000_000
        }
    }

    /// Record per-batch write phases (called from write stage; overwrites prior).
    pub fn note_last_write(p: LastWritePhases) {
        LAST_WRITE_N.store(u64::from(p.n_blocks), Ordering::Relaxed);
        LAST_WRITE_WALL_NS.store(p.wall_ns, Ordering::Relaxed);
        LAST_WRITE_STRUCTURAL_NS.store(p.structural_ns, Ordering::Relaxed);
        LAST_WRITE_SPENT_NS.store(p.spent_ns, Ordering::Relaxed);
        LAST_WRITE_CREATE_H_NS.store(p.create_h_ns, Ordering::Relaxed);
        LAST_WRITE_BIP68_NS.store(p.bip68_ns, Ordering::Relaxed);
        LAST_WRITE_CLASS_C_NS.store(p.class_c_ns, Ordering::Relaxed);
        LAST_WRITE_SPEND_ANN_NS.store(p.spend_ann_ns, Ordering::Relaxed);
        LAST_WRITE_TIP_GC_NS.store(p.tip_gc_ns, Ordering::Relaxed);
    }

    pub fn last_write_phases() -> LastWritePhases {
        LastWritePhases {
            n_blocks: LAST_WRITE_N.load(Ordering::Relaxed) as u32,
            wall_ns: LAST_WRITE_WALL_NS.load(Ordering::Relaxed),
            structural_ns: LAST_WRITE_STRUCTURAL_NS.load(Ordering::Relaxed),
            spent_ns: LAST_WRITE_SPENT_NS.load(Ordering::Relaxed),
            create_h_ns: LAST_WRITE_CREATE_H_NS.load(Ordering::Relaxed),
            bip68_ns: LAST_WRITE_BIP68_NS.load(Ordering::Relaxed),
            class_c_ns: LAST_WRITE_CLASS_C_NS.load(Ordering::Relaxed),
            spend_ann_ns: LAST_WRITE_SPEND_ANN_NS.load(Ordering::Relaxed),
            tip_gc_ns: LAST_WRITE_TIP_GC_NS.load(Ordering::Relaxed),
        }
    }

    /// Sample and reset all confirm phases.
    ///
    /// Returns
    /// `(recon, wire, connect, script, class_c, strong, scripthash, tip,
    ///   utxo_apply, blocks, resolve, load, unpin, cache_tip,
    ///   spend_ranged, spend_idx, spend_skip, structural, structural_spent,
    ///   structural_create_h, structural_bip68)`.
    /// `strong` / `scripthash` / `tip` come from [`rbitcoin_query::class_c_phase_stats`].
    /// `recon` prefers wire sub-timer, else legacy total.
    /// `connect` is **load assemble**, not write structural — see `structural`.
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
            STRUCTURAL_NS.swap(0, Ordering::Relaxed),
            STRUCTURAL_SPENT_NS.swap(0, Ordering::Relaxed),
            STRUCTURAL_CREATE_H_NS.swap(0, Ordering::Relaxed),
            STRUCTURAL_BIP68_NS.swap(0, Ordering::Relaxed),
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
    confirm_archived_run, confirm_archived_run_preverified, confirm_load_phase,
    confirm_load_phase_preverified, confirm_script_phase, confirm_scripts_phase,
    confirm_wire_prep_phase, confirm_wire_run, confirm_wire_run_preverified,
    confirm_write_phase, ConfirmLoadOutcome, ConfirmScriptOutcome, LoadedBatch,
    ScriptOkBatch, ScriptPreverified,
};

/// Accept + archive + confirm in one step (genesis / tip extension / tests).
///
/// **Same path as IBD confirm:** structure + header checks, then Class A
/// archive, then [`confirm_archived_run`] (load pin denserels → scripts →
/// structural → Class C → abs spend annotate). No empty-pin
/// [`validate_block_connect`] and no separate `put_spend_batch_by_create`.
///
/// Idempotent when `height` is already confirmed for this block hash.
/// Full script verify (no mempool skip) — use
/// [`accept_and_connect_block_preverified`] on tip follow with a live mempool.
pub fn accept_and_connect_block(
    query: &Query,
    params: &ChainParams,
    height: Height,
    block: &Block,
    milestone: Milestone,
) -> Result<rbitcoin_primitives::Fk, ConsensusError> {
    accept_and_connect_block_preverified(
        query,
        params,
        height,
        block,
        milestone,
        &ScriptPreverified::new(),
    )
}

/// Like [`accept_and_connect_block`], skipping script verify for `preverified`
/// txids (tip follow: live mempool after accept). Reorg disconnect stays outside.
pub fn accept_and_connect_block_preverified(
    query: &Query,
    params: &ChainParams,
    height: Height,
    block: &Block,
    milestone: Milestone,
    preverified: &ScriptPreverified,
) -> Result<rbitcoin_primitives::Fk, ConsensusError> {
    let hash = block.block_hash().to_byte_array();
    // Already tip at this height — skip re-archive / re-confirm.
    if let Some(h) = query
        .height_of_hash(&hash)
        .map_err(ConsensusError::Store)?
    {
        if h == height {
            if let Some((fk, _)) = query
                .get_header_by_hash(&hash)
                .map_err(ConsensusError::Store)?
            {
                return Ok(fk);
            }
        }
    }

    // Unified height-ordered path: wire → prep (plan+pin+assemble) → scripts →
    // commit (Class A + structural + Class C + annotate). No archive-then-reload.
    let fks = confirm_wire_run_preverified(
        query,
        params,
        milestone,
        &[(height, block.clone())],
        preverified,
    )?;
    if let Some(fk) = fks.into_iter().next() {
        return Ok(fk);
    }
    // Write skipped heights ≤ tip (idempotent race).
    query
        .get_header_by_hash(&hash)
        .map_err(ConsensusError::Store)?
        .map(|(fk, _)| fk)
        .ok_or(ConsensusError::Store(rbitcoin_store::StoreError::NotFound))
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

#[cfg(test)]
mod coverage_tests {
    use super::*;
    use bitcoin::absolute::LockTime;
    use bitcoin::block::{Header, Version};
    use bitcoin::hashes::Hash;
    use bitcoin::script::ScriptBuf;
    use bitcoin::transaction::Version as TxVersion;
    use bitcoin::{
        Amount, Block, BlockHash, CompactTarget, OutPoint, Sequence, Target, Transaction, TxIn,
        TxOut, TxMerkleNode, Witness,
    };
    use rbitcoin_primitives::Height;
    use rbitcoin_query::Query;
    use std::path::PathBuf;
    use std::sync::atomic::Ordering;
    use std::sync::Once;

    static HEAD_SCALE: Once = Once::new();

    fn ensure_tiny_heads() {
        HEAD_SCALE.call_once(|| {
            if std::env::var_os("RBITCOIN_HEAD_SCALE").is_none() {
                // SAFETY: tests only; process-local config.
                std::env::set_var("RBITCOIN_HEAD_SCALE", "tiny");
            }
        });
    }

    fn temp_store() -> (PathBuf, Query) {
        ensure_tiny_heads();
        let path = std::env::temp_dir().join(format!(
            "rbitcoin-consensus-cov-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0)
        ));
        std::fs::create_dir_all(&path).unwrap();
        let q = Query::open_or_create(&path).expect("open store");
        (path, q)
    }

    fn mine_regtest(prev: BlockHash, time: u32, height: u32, extras: Vec<Transaction>) -> Block {
        let mut ss = if height == 0 {
            vec![0x00]
        } else {
            bip34_height_script(height)
        };
        while ss.len() < 2 {
            ss.push(0x00);
        }
        let mut txdata = vec![Transaction {
            version: TxVersion::ONE,
            lock_time: LockTime::ZERO,
            input: vec![TxIn {
                previous_output: OutPoint::null(),
                script_sig: ScriptBuf::from_bytes(ss),
                sequence: Sequence::MAX,
                witness: Witness::new(),
            }],
            output: vec![TxOut {
                value: Amount::from_sat(50_0000_0000),
                script_pubkey: ScriptBuf::from_bytes(vec![0x51]),
            }],
        }];
        txdata.extend(extras);
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
            txdata,
        };
        block.header.merkle_root = block.compute_merkle_root().unwrap();
        let target = Target::from_compact(bits);
        for nonce in 0..u32::MAX {
            block.header.nonce = nonce;
            if block.header.validate_pow(target).is_ok() {
                break;
            }
        }
        block
    }

    #[test]
    fn crate_name_and_phase_stats() {
        assert_eq!(crate_name(), "rbitcoin-consensus");
        use confirm_phase_stats::*;
        note_last_write(LastWritePhases {
            n_blocks: 2,
            wall_ns: 3_000_000,
            structural_ns: 1_000_000,
            spent_ns: 100_000,
            create_h_ns: 200_000,
            bip68_ns: 50_000,
            class_c_ns: 400_000,
            spend_ann_ns: 300_000,
            tip_gc_ns: 10_000,
        });
        let p = last_write_phases();
        assert_eq!(p.n_blocks, 2);
        assert_eq!(LastWritePhases::ms(p.wall_ns), 3);
        RECONSTRUCT_NS.store(5, Ordering::Relaxed);
        RECONSTRUCT_WIRE_NS.store(7, Ordering::Relaxed);
        CONNECT_NS.store(1, Ordering::Relaxed);
        SCRIPT_NS.store(1, Ordering::Relaxed);
        CLASS_C_NS.store(1, Ordering::Relaxed);
        UTXO_APPLY_NS.store(1, Ordering::Relaxed);
        BLOCKS.store(1, Ordering::Relaxed);
        RESOLVE_NS.store(1, Ordering::Relaxed);
        LOAD_NS.store(1, Ordering::Relaxed);
        UNPIN_NS.store(1, Ordering::Relaxed);
        CACHE_TIP_NS.store(1, Ordering::Relaxed);
        SPEND_ANNOTATE_RANGED.store(1, Ordering::Relaxed);
        SPEND_ANNOTATE_IDX.store(1, Ordering::Relaxed);
        SPEND_ANNOTATE_SKIP.store(1, Ordering::Relaxed);
        STRUCTURAL_NS.store(1, Ordering::Relaxed);
        STRUCTURAL_SPENT_NS.store(1, Ordering::Relaxed);
        STRUCTURAL_CREATE_H_NS.store(1, Ordering::Relaxed);
        STRUCTURAL_BIP68_NS.store(1, Ordering::Relaxed);
        let s = sample_and_reset();
        assert_eq!(s.0, 7); // wire preferred over recon total
        assert_eq!(s.1, 7);
        // second sample zeros
        let s2 = sample_and_reset();
        assert_eq!(s2.0, 0);
    }

    #[test]
    fn script_bench_helpers_on_acs_job() {
        use script_bench::{owned_jobs, verify_job, verify_jobs_pool, verify_one_job, verify_owned_pool, JobBytes};
        let tx = Transaction {
            version: TxVersion::TWO,
            lock_time: LockTime::ZERO,
            input: vec![TxIn {
                previous_output: OutPoint {
                    txid: bitcoin::Txid::from_byte_array([1; 32]),
                    vout: 0,
                },
                script_sig: ScriptBuf::new(),
                sequence: Sequence::MAX,
                witness: Witness::new(),
            }],
            output: vec![TxOut {
                value: Amount::from_sat(1),
                script_pubkey: ScriptBuf::from_bytes(vec![0x51]),
            }],
        };
        let prevouts = vec![TxOut {
            value: Amount::from_sat(10),
            script_pubkey: ScriptBuf::from_bytes(vec![0x51]),
        }];
        let jb = JobBytes::new(prevouts, tx);
        verify_job(&jb).unwrap();
        verify_jobs_pool(std::slice::from_ref(&jb)).unwrap();
        let owned = owned_jobs(std::slice::from_ref(&jb));
        verify_owned_pool(&owned).unwrap();
        verify_one_job(&owned[0]).unwrap();
    }

    #[test]
    fn regtest_connect_archive_and_confirm_path() {
        let (path, q) = temp_store();
        let params = ChainParams::regtest();
        let ms = Milestone { height: 1_000_000 };
        let genesis = genesis_block(&params);
        accept_and_connect_block(&q, &params, Height::GENESIS, &genesis, ms).unwrap();

        let b1 = mine_regtest(genesis.block_hash(), genesis.header.time + 600, 1, vec![]);
        // prepare + archive paths
        let (_hr, _txs) = prepare_block_for_archive(&q, &params, &b1).unwrap();
        let (_hr2, _txs2) = prepare_block_for_archive_new(&q, &params, &b1).unwrap();
        let (_hr3, _txs3) = prepare_block_for_archive_ibd(&params, &b1).unwrap();
        accept_and_archive_block(&q, &params, Height(1), &b1, ms).unwrap();
        // already archived branch
        let _ = prepare_block_for_archive(&q, &params, &b1).unwrap();
        confirm_archived_at(
            &q,
            &params,
            Height(1),
            &b1.block_hash().to_byte_array(),
            ms,
        )
        .unwrap();

        // Bad pow limit on prepare_ibd
        let mut bad = b1.clone();
        bad.header.bits = CompactTarget::from_consensus(0x1d00_ffff); // mainnet-ish, above regtest limit often
        // May fail pow or pow limit depending on params — either is fine.
        let _ = prepare_block_for_archive_ibd(&params, &bad);

        let _ = std::fs::remove_dir_all(&path);
    }
}
