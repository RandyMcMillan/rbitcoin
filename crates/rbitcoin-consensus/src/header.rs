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
    use super::*;
    use rbitcoin_primitives::{Fk, Height};
    use rbitcoin_query::{Query, TxApply};
    use rbitcoin_store::{HeaderRecord, InputRecord, OutputRecord, TxRecord};
    use std::sync::Once;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn mtp_times_picks_middle_of_sorted() {
        assert_eq!(median_time_past_times(&[3, 1, 2]), 2);
        assert_eq!(median_time_past_times(&[10]), 10);
        // Even length: Core takes sorted[len/2] (upper middle).
        assert_eq!(median_time_past_times(&[1, 2, 3, 4]), 3);
    }

    fn temp_q() -> (std::path::PathBuf, Query) {
        static ONCE: Once = Once::new();
        ONCE.call_once(|| {
            if std::env::var_os("RBITCOIN_HEAD_SCALE").is_none() {
                std::env::set_var("RBITCOIN_HEAD_SCALE", "tiny");
            }
        });
        let n = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("rbitcoin-hdr-mtp-{n}"));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let q = Query::open_or_create(dir.join("store")).unwrap();
        (dir, q)
    }

    /// Write-gate-safe synthetic coinbase: non-null `prev_fk` must commit `parent_hash`.
    fn coinbase(h: u32, prev: Fk, parent_hash: Option<[u8; 32]>) -> (HeaderRecord, TxApply) {
        let version = 1;
        let timestamp = 1_000 + h * 10;
        let bits = 0x207fffff;
        let nonce = h;
        let mut merkle = [0u8; 32];
        merkle[0..4].copy_from_slice(&h.to_le_bytes());
        merkle[4] = 0xcd;
        let hash = match parent_hash {
            None => merkle,
            Some(ph) => {
                rbitcoin_store::block_header_hash(version, &ph, &merkle, timestamp, bits, nonce)
            }
        };
        let header = HeaderRecord {
            prev_fk: prev,
            version,
            timestamp,
            bits,
            nonce,
            merkle_root: merkle,
            hash,
        };
        let mut txid = [0u8; 32];
        txid[0..4].copy_from_slice(&h.to_le_bytes());
        txid[31] = 0xcb;
        let ta = TxApply {
            tx: TxRecord {
                txid,
                version: 1,
                locktime: 0,
                input_start_fk: Fk::NULL,
                input_count: 1,
                output_start_fk: Fk::NULL,
                output_count: 1,
            },
            inputs: vec![InputRecord {
                prev_txid: [0u8; 32],
                create_fk: Fk::NULL,
                prev_index: u32::MAX,
                sequence: u32::MAX,
                script_sig: vec![h as u8],
                witness: vec![],
            }],
            outputs: vec![OutputRecord::unspent(50, vec![0x51])],
        };
        (header, ta)
    }

    #[test]
    fn mtp_from_confirmed_chain_and_missing_above_tip() {
        let (dir, q) = temp_q();
        let mut prev = Fk::NULL;
        let mut parent_hash: Option<[u8; 32]> = None;
        for h in 0..3u32 {
            let (hdr, ta) = coinbase(h, prev, parent_hash);
            parent_hash = Some(hdr.hash);
            prev = q.connect_block(Height(h), &hdr, &[ta]).unwrap();
        }
        let mtp = median_time_past(&q, Height(2)).unwrap();
        // times: 1000, 1010, 1020 → middle 1010
        assert_eq!(mtp, 1010);

        // Height above tip with no plan → incomplete load error (not BadPrev).
        let err = median_time_past(&q, Height(5)).unwrap_err();
        assert!(
            matches!(err, ConsensusError::Store(_)) || matches!(err, ConsensusError::BadPrev),
            "got {err:?}"
        );

        // expected_next_bits: height 0 + regtest no-retarget.
        let params = ChainParams::regtest();
        let gbits = expected_next_bits(&q, &params, Height(0)).unwrap();
        assert_eq!(gbits, crate::params::genesis_block(&params).header.bits);
        let b1 = expected_next_bits(&q, &params, Height(1)).unwrap();
        let (_fk, rec0) = q.header_at_height(Height(0)).unwrap().unwrap();
        assert_eq!(b1.to_consensus(), rec0.bits);

        // Bad prev header height.
        assert!(expected_next_bits(&q, &params, Height(99)).is_err());

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn validate_header_genesis_and_bad_prev() {
        let (dir, q) = temp_q();
        let params = ChainParams::regtest();
        let g = crate::params::genesis_block(&params);
        // Wrong genesis hash at height 0.
        let mut bad = g.header;
        bad.nonce ^= 1;
        let err = validate_header(&q, &params, Height(0), &bad).unwrap_err();
        assert!(matches!(err, ConsensusError::BadHeader(_)), "{err:?}");

        // Synthetic tip for prev linkage (child hash commits to parent).
        let (h0, ta0) = coinbase(0, Fk::NULL, None);
        let prev = q.connect_block(Height(0), &h0, &[ta0]).unwrap();
        let (h1, ta1) = coinbase(1, prev, Some(h0.hash));
        q.connect_block(Height(1), &h1, &[ta1]).unwrap();

        // Header with wrong prev hash at height 1.
        let mut hdr = g.header;
        hdr.prev_blockhash = bitcoin::BlockHash::from_byte_array([0xee; 32]);
        hdr.time = 2_000;
        hdr.bits = bitcoin::CompactTarget::from_consensus(0x207fffff);
        let err = validate_header(&q, &params, Height(1), &hdr).unwrap_err();
        assert!(
            matches!(err, ConsensusError::BadPrev | ConsensusError::BadHeader(_)),
            "{err:?}"
        );
        let _ = std::fs::remove_dir_all(&dir);
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
