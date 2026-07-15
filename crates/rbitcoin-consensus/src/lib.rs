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
pub use params::{genesis_block, ChainParams, Checkpoint};

pub fn crate_name() -> &'static str {
    "rbitcoin-consensus"
}

use bitcoin::block::Block;
use bitcoin::hashes::Hash;
use bitcoin::Target;
use rbitcoin_primitives::Height;
use rbitcoin_query::Query;
use rbitcoin_store::StoreError;

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

/// Archive a block body into Class A **without** requiring tip+1 / Class C.
///
/// Used by parallel IBD: download order ≠ connect order. Parent header must
/// already be in the store (`ensure_header` / prior archive). Under milestone,
/// skips full header-chain difficulty checks (PoW on claimed bits still applied).
pub fn accept_and_archive_block(
    query: &Query,
    params: &ChainParams,
    height: Height,
    block: &Block,
    milestone: Milestone,
) -> Result<rbitcoin_primitives::Fk, ConsensusError> {
    let hash = block.block_hash().to_byte_array();
    if query
        .is_block_archived(&hash)
        .map_err(ConsensusError::Store)?
    {
        return query
            .get_header_by_hash(&hash)
            .map_err(ConsensusError::Store)?
            .map(|(fk, _)| fk)
            .ok_or(ConsensusError::Store(StoreError::NotFound));
    }

    let ctx = ValidationContext {
        params,
        height,
        milestone,
    };
    validate_block_structure(block, &ctx)?;

    // PoW against claimed bits (always).
    let target = Target::from_compact(block.header.bits);
    if target > params.pow_limit {
        return Err(ConsensusError::BadHeader("target above pow limit"));
    }
    block
        .header
        .validate_pow(target)
        .map_err(|_| ConsensusError::InvalidPow)?;

    if !milestone.skips_at(height.0) {
        // Full header linkage/difficulty only when not under milestone IBD.
        validate_header(query, params, height, &block.header)?;
        validate_block_connect(query, block, &ctx)?;
    }

    let (header_rec, txs) = block_to_apply(query, &block.header, &block.txdata)?;
    query
        .archive_block(&header_rec, &txs)
        .map_err(ConsensusError::Store)
}

/// Confirm the next tip block if its body is already archived.
pub fn confirm_archived_at(
    query: &Query,
    params: &ChainParams,
    height: Height,
    block_hash: &[u8; 32],
    milestone: Milestone,
) -> Result<rbitcoin_primitives::Fk, ConsensusError> {
    if !milestone.skips_at(height.0) {
        // Full connect validation needs the wire block — reconstruct from archive.
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
