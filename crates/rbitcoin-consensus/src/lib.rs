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
/// Used by parallel IBD: download order ≠ connect order. Parent **header** must
/// already be in the store (`ensure_header` / prior archive).
///
/// **Never** runs tip-ordered checks here (`validate_header` / `validate_block_connect`
/// need a confirmed parent and UTXO view). Those run only in [`confirm_archived_at`].
/// Light checks: structure (BIP34 at `height`), PoW vs claimed bits, pow limit.
pub fn accept_and_archive_block(
    query: &Query,
    params: &ChainParams,
    height: Height,
    block: &Block,
    milestone: Milestone,
) -> Result<rbitcoin_primitives::Fk, ConsensusError> {
    let _ = milestone; // connect/script policy applied at confirm only
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

    // Structure without BIP34: height is only authoritative at confirm (tip order).
    // Passing GENESIS skips BIP34; merkle/weight/witness still checked.
    let _ = height;
    let ctx = ValidationContext {
        params,
        height: Height::GENESIS,
        milestone: Milestone::NONE,
    };
    validate_block_structure(block, &ctx)?;

    // PoW against claimed bits (always). Difficulty *correctness* is checked at confirm.
    let target = Target::from_compact(block.header.bits);
    if target > params.pow_limit {
        return Err(ConsensusError::BadHeader("target above pow limit"));
    }
    block
        .header
        .validate_pow(target)
        .map_err(|_| ConsensusError::InvalidPow)?;

    // Parent header must exist so prev_fk links (header-first IBD / ensure_header).
    let prev = block.header.prev_blockhash;
    if prev.to_byte_array() != [0u8; 32]
        && query
            .get_header_by_hash(prev.as_byte_array())
            .map_err(ConsensusError::Store)?
            .is_none()
    {
        return Err(ConsensusError::BadPrev);
    }

    let (header_rec, txs) = block_to_apply(query, &block.header, &block.txdata)?;
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
