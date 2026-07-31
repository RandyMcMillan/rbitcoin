//! Head resolve sub-timers (probe / idx / body prefix) + probe-depth metrics.
//!
//! Reset by the IBD ~5s sampler. Used to split archive prep `head_fk` cost into
//! open-address probes vs `tx.idx` range vs thin `tx.body` txid prefix reads,
//! and to measure **winning candidate rank** before denserels-merge work (PR-A).

use std::sync::atomic::{AtomicU64, Ordering};

static PROBE_NS: AtomicU64 = AtomicU64::new(0);
static IDX_NS: AtomicU64 = AtomicU64::new(0);
static BODY_NS: AtomicU64 = AtomicU64::new(0);
/// Keys that entered a head probe (batch or single).
static KEYS: AtomicU64 = AtomicU64::new(0);
/// Candidate fks collected from probes (before body dedupe).
static CANDS: AtomicU64 = AtomicU64::new(0);
/// Unique fks that paid idx+body_txid (one per body peek attempt).
static BODY_LOOKUPS: AtomicU64 = AtomicU64::new(0);
/// Sum of 1-based ranks of the matching cand in probe order (resolved keys only).
static HIT_RANK_SUM: AtomicU64 = AtomicU64::new(0);
/// Number of keys that resolved (contributed to HIT_RANK_SUM).
static HIT_RANK_N: AtomicU64 = AtomicU64::new(0);
/// Body peeks that did **not** match the wanted txid (wrong cand).
static MISS_PEEKS: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Default, Clone, Copy)]
pub struct Sample {
    pub probe_ns: u64,
    pub idx_ns: u64,
    pub body_ns: u64,
    pub keys: u64,
    pub cands: u64,
    pub body_lookups: u64,
    /// Sum of hit ranks (use with [`Self::hit_rank_n`] for mean).
    pub hit_rank_sum: u64,
    pub hit_rank_n: u64,
    pub miss_peeks: u64,
}

impl Sample {
    pub fn sum_ns(&self) -> u64 {
        self.probe_ns
            .saturating_add(self.idx_ns)
            .saturating_add(self.body_ns)
    }

    /// Mean 1-based rank of winning cand (0 if no hits).
    pub fn hit_rank_avg(&self) -> f64 {
        if self.hit_rank_n == 0 {
            0.0
        } else {
            self.hit_rank_sum as f64 / self.hit_rank_n as f64
        }
    }
}

#[inline]
pub fn add_probe(ns: u64) {
    if ns > 0 {
        PROBE_NS.fetch_add(ns, Ordering::Relaxed);
    }
}

#[inline]
pub fn add_idx(ns: u64) {
    if ns > 0 {
        IDX_NS.fetch_add(ns, Ordering::Relaxed);
    }
}

#[inline]
pub fn add_body(ns: u64) {
    if ns > 0 {
        BODY_NS.fetch_add(ns, Ordering::Relaxed);
    }
}

#[inline]
pub fn add_keys(n: u64) {
    if n > 0 {
        KEYS.fetch_add(n, Ordering::Relaxed);
    }
}

#[inline]
pub fn add_cands(n: u64) {
    if n > 0 {
        CANDS.fetch_add(n, Ordering::Relaxed);
    }
}

#[inline]
pub fn add_body_lookups(n: u64) {
    if n > 0 {
        BODY_LOOKUPS.fetch_add(n, Ordering::Relaxed);
    }
}

/// Record a resolved key: `rank` is 1-based index in probe/body order of the match.
#[inline]
pub fn add_hit_rank(rank: u64) {
    if rank > 0 {
        HIT_RANK_SUM.fetch_add(rank, Ordering::Relaxed);
        HIT_RANK_N.fetch_add(1, Ordering::Relaxed);
    }
}

#[inline]
pub fn add_miss_peeks(n: u64) {
    if n > 0 {
        MISS_PEEKS.fetch_add(n, Ordering::Relaxed);
    }
}

pub fn sample_and_reset() -> Sample {
    Sample {
        probe_ns: PROBE_NS.swap(0, Ordering::Relaxed),
        idx_ns: IDX_NS.swap(0, Ordering::Relaxed),
        body_ns: BODY_NS.swap(0, Ordering::Relaxed),
        keys: KEYS.swap(0, Ordering::Relaxed),
        cands: CANDS.swap(0, Ordering::Relaxed),
        body_lookups: BODY_LOOKUPS.swap(0, Ordering::Relaxed),
        hit_rank_sum: HIT_RANK_SUM.swap(0, Ordering::Relaxed),
        hit_rank_n: HIT_RANK_N.swap(0, Ordering::Relaxed),
        miss_peeks: MISS_PEEKS.swap(0, Ordering::Relaxed),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn add_sample_reset_and_sum() {
        add_probe(0);
        add_idx(0);
        add_body(0);
        add_keys(0);
        add_cands(0);
        add_body_lookups(0);
        add_hit_rank(0);
        add_miss_peeks(0);
        let _ = sample_and_reset();

        add_probe(10);
        add_idx(20);
        add_body(30);
        add_keys(4);
        add_cands(5);
        add_body_lookups(6);
        add_hit_rank(1);
        add_hit_rank(3);
        add_miss_peeks(2);
        let s = sample_and_reset();
        assert_eq!(s.probe_ns, 10);
        assert_eq!(s.idx_ns, 20);
        assert_eq!(s.body_ns, 30);
        assert_eq!(s.keys, 4);
        assert_eq!(s.cands, 5);
        assert_eq!(s.body_lookups, 6);
        assert_eq!(s.hit_rank_sum, 4);
        assert_eq!(s.hit_rank_n, 2);
        assert!((s.hit_rank_avg() - 2.0).abs() < 1e-9);
        assert_eq!(s.miss_peeks, 2);
        assert_eq!(s.sum_ns(), 60);

        let empty = sample_and_reset();
        assert_eq!(empty.sum_ns(), 0);
        assert_eq!(empty.keys, 0);
        assert_eq!(empty.hit_rank_n, 0);
    }
}
