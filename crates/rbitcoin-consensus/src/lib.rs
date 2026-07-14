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
    block_subsidy, validate_block_connect, validate_block_structure, ValidationContext,
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
use rbitcoin_primitives::Height;
use rbitcoin_query::Query;

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
