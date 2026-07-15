//! Consensus validation using rust-bitcoin types.
//!
//! Entrypoints are intended for high-level chain application (not isolated unit tests).

mod block;
mod convert;
mod error;
mod header;
mod milestone;
mod params;

pub use block::{
    bip34_height_script, block_subsidy, validate_block_connect, validate_block_structure,
    ValidationContext,
};
pub use convert::{block_to_apply, header_to_record};
pub use error::ConsensusError;
pub use header::validate_header;
pub use milestone::Milestone;
pub use params::{default_milestone_height, genesis_block, ChainParams, Checkpoint};

pub fn crate_name() -> &'static str {
    "rbitcoin-consensus"
}

use bitcoin::block::Block;
use bitcoin::hashes::Hash;
use bitcoin::Target;
use rbitcoin_primitives::Height;
use rbitcoin_query::{Query, TxApply};
use rbitcoin_store::{HeaderRecord, StoreError};

/// Full accept path: structure + header link/PoW + connect checks (prevouts/scripts),
/// then store connect. Returns header FK.
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
    validate_block_structure(block, &ctx)?;
    validate_header(query, params, height, &block.header)?;
    if !milestone.skips_at(height.0) {
        validate_block_connect(query, block, &ctx)?;
    }
    let (header_rec, txs) = block_to_apply(query, &block.header, &block.txdata)?;
    let fk = query
        .connect_block(height, &header_rec, &txs)
        .map_err(ConsensusError::Store)?;
    Ok(fk)
}

/// CPU-side prep for Class A archive: structure, PoW, parent link, `TxApply` encode.
///
/// No Class A body writes. Safe to run on multiple `spawn_blocking` workers
/// (store reads only for parent header / idempotent archived check).
///
/// BIP34 is deferred to confirm (height only reliable tip-ordered).
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
        // Idempotent: re-derive records for writer (writer will no-op put).
        return block_to_apply(query, &block.header, &block.txdata);
    }

    let ctx = ValidationContext {
        params,
        height: Height::GENESIS, // skip BIP34; merkle/weight/witness still checked
        milestone: Milestone::NONE,
    };
    validate_block_structure(block, &ctx)?;

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

    block_to_apply(query, &block.header, &block.txdata)
}

/// Archive a block body into Class A **without** requiring tip+1 / Class C.
///
/// Prep + single-threaded write. Prefer [`prepare_block_for_archive`] +
/// [`Query::archive_block`] on an archive pipeline for parallel prep.
pub fn accept_and_archive_block(
    query: &Query,
    params: &ChainParams,
    height: Height,
    block: &Block,
    milestone: Milestone,
) -> Result<rbitcoin_primitives::Fk, ConsensusError> {
    let _ = (height, milestone);
    let (header_rec, txs) = prepare_block_for_archive(query, params, block)?;
    query
        .archive_block(&header_rec, &txs)
        .map_err(ConsensusError::Store)
}

/// Confirm the next tip block if its body is already archived.
///
/// Full header + connect validation run here (when not under milestone), where
/// the parent is the confirmed tip and prevouts are on the best chain.
pub fn confirm_archived_at(
    query: &Query,
    params: &ChainParams,
    height: Height,
    block_hash: &[u8; 32],
    milestone: Milestone,
) -> Result<rbitcoin_primitives::Fk, ConsensusError> {
    if !milestone.skips_at(height.0) {
        let block = query
            .reconstruct_archived_block(block_hash)
            .map_err(ConsensusError::Store)?
            .ok_or(ConsensusError::Store(StoreError::NotFound))?;
        let ctx = ValidationContext {
            params,
            height,
            milestone,
        };
        validate_header(query, params, height, &block.header)?;
        validate_block_connect(query, &block, &ctx)?;
    }
    query
        .confirm_block(height, block_hash)
        .map_err(ConsensusError::Store)
}
