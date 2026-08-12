//! Flow-aware fee projection (Engine v2 pure math).
//!
//! Capacity always uses a fixed 10-minute block clock:
//! `capacity_wu(N) = N × BLOCK_WEIGHT_WU`. Live stock + projected inflow above
//! a candidate rate decide the minimum inclusion feerate.

/// Consensus block weight (WU) used as one-block capacity.
pub const BLOCK_WEIGHT_WU: u64 = 4_000_000;

/// Seconds per planned block (product clock; not wall time since last tip).
pub const SECONDS_PER_BLOCK: u64 = 600;

/// Safety margin: include when load ≤ this fraction of capacity (95%).
pub const CAPACITY_SAFETY_NUM: u64 = 95;
pub const CAPACITY_SAFETY_DEN: u64 = 100;

/// Feerate bucket edges in sat/kvB (Libre min relay = 100). Last bucket is +∞.
pub const FEE_BUCKET_EDGES_SAT_PER_KVB: &[u64] = &[
    100, 200, 300, 500, 1_000, 2_000, 5_000, 10_000, 20_000, 50_000, 100_000,
];

/// Index of the bucket that contains `rate_sat_per_kvb` (0 = lowest).
pub fn bucket_index(rate_sat_per_kvb: u64) -> usize {
    let edges = FEE_BUCKET_EDGES_SAT_PER_KVB;
    for (i, &edge) in edges.iter().enumerate() {
        if rate_sat_per_kvb < edge {
            return i.saturating_sub(1).min(edges.len());
        }
        if rate_sat_per_kvb == edge {
            return i;
        }
    }
    // rate >= last edge → top open bucket (index == edges.len())
    // For rates in [edge[i], edge[i+1]) use i; rate >= last → edges.len()
    let mut idx = 0usize;
    for (i, &edge) in edges.iter().enumerate() {
        if rate_sat_per_kvb >= edge {
            idx = i;
        }
    }
    // Open top: rates strictly above last edge stay at last index for closed
    // buckets; treat last edge and above as last closed + one open.
    if rate_sat_per_kvb > *edges.last().unwrap_or(&0) {
        edges.len()
    } else {
        idx
    }
}

/// Number of buckets (edges + open top).
pub fn bucket_count() -> usize {
    FEE_BUCKET_EDGES_SAT_PER_KVB.len() + 1
}

/// Capacity for target block count N (≥ 1).
pub fn capacity_wu(n_blocks: u32) -> u64 {
    let n = n_blocks.max(1) as u64;
    n.saturating_mul(BLOCK_WEIGHT_WU)
}

/// Horizon seconds for target N.
pub fn horizon_secs(n_blocks: u32) -> u64 {
    let n = n_blocks.max(1) as u64;
    n.saturating_mul(SECONDS_PER_BLOCK)
}

/// Effective capacity after safety margin.
pub fn effective_capacity_wu(n_blocks: u32) -> u64 {
    capacity_wu(n_blocks).saturating_mul(CAPACITY_SAFETY_NUM) / CAPACITY_SAFETY_DEN
}

/// Projected weight arriving above rate R over horizon H.
///
/// `inflow_wu_per_s_by_bucket[i]` is λ for bucket i; buckets with min rate > R
/// contribute.
pub fn projected_inflow_wu_above(
    inflow_wu_per_s_by_bucket: &[u64],
    rate_sat_per_kvb: u64,
    horizon_secs: u64,
) -> u64 {
    let n = bucket_count().min(inflow_wu_per_s_by_bucket.len());
    let mut sum = 0u64;
    for i in 0..n {
        let bucket_lo = if i < FEE_BUCKET_EDGES_SAT_PER_KVB.len() {
            FEE_BUCKET_EDGES_SAT_PER_KVB[i]
        } else {
            // Open top: treat as above last edge.
            FEE_BUCKET_EDGES_SAT_PER_KVB
                .last()
                .copied()
                .unwrap_or(0)
                .saturating_add(1)
        };
        // Include bucket if its floor rate is strictly above R (competitors).
        if bucket_lo > rate_sat_per_kvb {
            sum = sum.saturating_add(inflow_wu_per_s_by_bucket[i].saturating_mul(horizon_secs));
        }
    }
    sum
}

/// Minimum feerate (sat/kvB) such that
/// `stock_above(R) + projected_in(R, H) ≤ effective_capacity(N)`.
///
/// `stock_above` is a callback (live mining-chunk weight strictly above R).
/// `candidate_rates` is searched ascending (bucket edges + optional extras).
/// Returns `None` if even the highest candidate cannot fit (caller applies floors).
pub fn min_rate_for_capacity<F>(
    stock_above: F,
    inflow_wu_per_s_by_bucket: &[u64],
    n_blocks: u32,
    candidate_rates: &[u64],
) -> Option<u64>
where
    F: Fn(u64) -> u64,
{
    let cap = effective_capacity_wu(n_blocks);
    let h = horizon_secs(n_blocks);
    let mut best: Option<u64> = None;
    for &r in candidate_rates {
        let load = stock_above(r).saturating_add(projected_inflow_wu_above(
            inflow_wu_per_s_by_bucket,
            r,
            h,
        ));
        if load <= cap {
            best = Some(match best {
                Some(b) => b.min(r),
                None => r,
            });
        }
    }
    best
}

/// Default candidate ladder: bucket edges.
pub fn default_candidate_rates() -> Vec<u64> {
    FEE_BUCKET_EDGES_SAT_PER_KVB.to_vec()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn capacity_is_n_times_block() {
        assert_eq!(capacity_wu(1), BLOCK_WEIGHT_WU);
        assert_eq!(capacity_wu(3), 3 * BLOCK_WEIGHT_WU);
        assert_eq!(capacity_wu(0), BLOCK_WEIGHT_WU); // clamp
        assert_eq!(horizon_secs(2), 1200);
    }

    #[test]
    fn zero_inflow_equals_frontier_depth() {
        // stock_above: 2e6 WU above any rate < 1000, 0 above ≥1000
        let stock = |r: u64| if r < 1000 { 2_000_000 } else { 0 };
        let inflow = vec![0u64; bucket_count()];
        let rates = default_candidate_rates();
        let r = min_rate_for_capacity(stock, &inflow, 1, &rates).expect("fits");
        // Need stock_above(R) ≤ 0.95 * 4e6 → 2e6 always ok; minimum R on ladder that
        // still has load ≤ cap. All rates with stock 2e6 fit; min is lowest edge.
        assert_eq!(r, rates[0]);
        // Deeper N still fits at lowest.
        let r2 = min_rate_for_capacity(stock, &inflow, 5, &rates).unwrap();
        assert!(r2 <= r);
    }

    #[test]
    fn high_bucket_inflow_raises_rate() {
        let stock = |_r: u64| 0u64;
        let mut inflow = vec![0u64; bucket_count()];
        // Massive inflow in high-rate buckets (index for 50k+).
        let hi = bucket_index(50_000);
        inflow[hi] = 50_000; // WU/s → over 600s = 30e6 WU >> 4e6
        let rates = default_candidate_rates();
        let cold = min_rate_for_capacity(stock, &vec![0u64; bucket_count()], 1, &rates).unwrap();
        let hot = min_rate_for_capacity(stock, &inflow, 1, &rates).unwrap();
        assert!(hot >= cold, "hot={hot} cold={cold}");
        assert!(
            hot >= 50_000,
            "should clear high-inflow competitors, got {hot}"
        );
    }

    #[test]
    fn monotone_in_target_blocks() {
        let stock = |r: u64| if r < 5_000 { 3_500_000 } else { 0 };
        let mut inflow = vec![0u64; bucket_count()];
        inflow[bucket_index(1_000)] = 1_000; // modest
        let rates = default_candidate_rates();
        let r1 = min_rate_for_capacity(stock, &inflow, 1, &rates).unwrap();
        let r3 = min_rate_for_capacity(stock, &inflow, 3, &rates).unwrap();
        let r10 = min_rate_for_capacity(stock, &inflow, 10, &rates).unwrap();
        assert!(r3 <= r1, "r3={r3} r1={r1}");
        assert!(r10 <= r3, "r10={r10} r3={r3}");
    }

    #[test]
    fn bucket_index_edges() {
        assert_eq!(bucket_index(100), 0);
        assert_eq!(bucket_index(150), 0);
        assert_eq!(bucket_index(200), 1);
        assert!(bucket_index(1_000_000) >= FEE_BUCKET_EDGES_SAT_PER_KVB.len() - 1);
    }
}
