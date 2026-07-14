use crate::error::ConsensusError;
use crate::params::{check_genesis_hash, ChainParams};
use bitcoin::block::Header;
use bitcoin::hashes::Hash;
use bitcoin::Target;
use rbitcoin_primitives::Height;
use rbitcoin_query::Query;

/// Validate header linkage, checkpoint, and proof-of-work.
pub fn validate_header(
    query: &Query,
    params: &ChainParams,
    height: Height,
    header: &Header,
) -> Result<(), ConsensusError> {
    let hash = header.block_hash();

    if height.0 == 0 {
        if !check_genesis_hash(params, hash) {
            return Err(ConsensusError::BadHeader("genesis hash mismatch"));
        }
    } else {
        let prev_height = Height(height.0 - 1);
        let (prev_fk, prev_rec) = query
            .header_at_height(prev_height)?
            .ok_or(ConsensusError::BadPrev)?;
        let _ = prev_fk;
        if prev_rec.hash != header.prev_blockhash.to_byte_array() {
            return Err(ConsensusError::BadPrev);
        }
    }

    if let Some(cp) = params.checkpoint_at(height) {
        if cp != hash {
            return Err(ConsensusError::BadHeader("checkpoint mismatch"));
        }
    }

    // Proof of work
    let target = Target::from_compact(header.bits);
    if target > params.pow_limit {
        return Err(ConsensusError::BadHeader("target above pow limit"));
    }
    header
        .validate_pow(target)
        .map_err(|_| ConsensusError::InvalidPow)?;

    Ok(())
}
