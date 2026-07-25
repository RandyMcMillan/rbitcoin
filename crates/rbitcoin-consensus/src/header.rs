use crate::error::ConsensusError;
use crate::params::{check_genesis_hash, ChainParams};
use bitcoin::block::Header;
use bitcoin::hashes::Hash;
use bitcoin::{CompactTarget, Target};
use rbitcoin_primitives::Height;
use rbitcoin_query::Query;

/// Validate header linkage, checkpoint, MTP, difficulty bits, and proof-of-work.
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
        let (_prev_fk, prev_rec) = query
            .header_at_height(prev_height)?
            .ok_or(ConsensusError::BadPrev)?;
        if prev_rec.hash != header.prev_blockhash.to_byte_array() {
            return Err(ConsensusError::BadPrev);
        }

        // Median-time-past: timestamp must be strictly greater than MTP of prior blocks.
        let mtp = median_time_past(query, prev_height)?;
        if header.time <= mtp {
            return Err(ConsensusError::BadHeader("timestamp <= median-time-past"));
        }
        // Core: block time must not be more than 2 hours ahead of adjusted network time.
        // We use wall-clock UTC (no peer-time adjustment).
        const MAX_FUTURE_BLOCK_TIME: u64 = 2 * 60 * 60;
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        if u64::from(header.time) > now.saturating_add(MAX_FUTURE_BLOCK_TIME) {
            return Err(ConsensusError::BadHeader("timestamp too far in future"));
        }
    }

    if let Some(cp) = params.checkpoint_at(height) {
        if cp != hash {
            return Err(ConsensusError::BadHeader("checkpoint mismatch"));
        }
    }

    // Expected difficulty / bits
    let expected_bits = expected_next_bits(query, params, height)?;
    if header.bits != expected_bits {
        return Err(ConsensusError::BadHeader("incorrect proof of work bits"));
    }

    // Proof of work against claimed bits (and pow limit)
    let target = Target::from_compact(header.bits);
    if target > params.pow_limit {
        return Err(ConsensusError::BadHeader("target above pow limit"));
    }
    header
        .validate_pow(target)
        .map_err(|_| ConsensusError::InvalidPow)?;

    Ok(())
}

/// Median timestamp of up to 11 blocks ending at `height` (inclusive).
///
/// **Confirm load path:** BIP68/BIP113 run during multi-block assemble while
/// mid-batch heights are not yet in `confirmed[]`. Prefer the durable confirmed
/// chain, then load-stage header plans (same hybrid as [`crate::confirm_run`]
/// header MTP). Heights still above tip with no plan are retryable load
/// incomplete — not permanent `BadPrev` (that silently split batches to n=1).
pub fn median_time_past(query: &Query, height: Height) -> Result<u32, ConsensusError> {
    let mut times = Vec::with_capacity(11);
    let start = height.0.saturating_sub(10);
    let tip = query.tip_height().map(|h| h.0).unwrap_or(0);
    for h in start..=height.0 {
        if let Some((_fk, rec)) = query.header_at_height(Height(h))? {
            times.push(rec.timestamp);
            continue;
        }
        if let Some(plan) = query.confirm_parent_cache().get_header_plan(h) {
            times.push(plan.header_rec.timestamp);
            continue;
        }
        if h > tip {
            return Err(ConsensusError::Store(rbitcoin_store::StoreError::Corrupt(
                "confirm: load incomplete (parent header plan missing above tip)",
            )));
        }
        return Err(ConsensusError::BadPrev);
    }
    Ok(median_time_past_times(&times))
}

/// Median of an already-collected timestamp window (unsorted OK).
pub fn median_time_past_times(times: &[u32]) -> u32 {
    debug_assert!(!times.is_empty());
    let mut sorted = times.to_vec();
    sorted.sort_unstable();
    sorted[sorted.len() / 2]
}

#[cfg(test)]
mod median_time_past_tests {
    use super::median_time_past_times;

    #[test]
    fn mtp_times_picks_middle_of_sorted() {
        assert_eq!(median_time_past_times(&[3, 1, 2]), 2);
        assert_eq!(median_time_past_times(&[10]), 10);
        // Even length: Core takes sorted[len/2] (upper middle).
        assert_eq!(median_time_past_times(&[1, 2, 3, 4]), 3);
    }
}

/// Expected `nBits` for a new header at `height`.
pub fn expected_next_bits(
    query: &Query,
    params: &ChainParams,
    height: Height,
) -> Result<CompactTarget, ConsensusError> {
    if height.0 == 0 {
        // Genesis bits are fixed by the genesis block itself; callers validate hash.
        let g = crate::params::genesis_block(params);
        return Ok(g.header.bits);
    }

    let interval = params.difficulty_adjustment_interval();
    let prev_height = Height(height.0 - 1);
    let (_fk, prev_rec) = query
        .header_at_height(prev_height)?
        .ok_or(ConsensusError::BadPrev)?;
    let prev_bits = CompactTarget::from_consensus(prev_rec.bits);

    if params.no_pow_retargeting() || height.0 % interval != 0 {
        return Ok(prev_bits);
    }

    // Retarget: timespan from first of period to last (height-1).
    let first_height = Height(height.0 - interval);
    let (_fk, first_rec) = query
        .header_at_height(first_height)?
        .ok_or(ConsensusError::BadHeader("missing retarget first header"))?;

    let timespan = prev_rec.timestamp.saturating_sub(first_rec.timestamp) as u64;
    Ok(CompactTarget::from_next_work_required(
        prev_bits,
        timespan,
        &params.btc,
    ))
}
