//! Head resolve sub-timers (probe / idx / body prefix).
//!
//! Reset by the IBD ~5s sampler. Used to split archive prep `head=` cost into
//! open-address probes vs `tx.idx` range vs thin `tx.body` txid prefix reads.

use std::sync::atomic::{AtomicU64, Ordering};

static PROBE_NS: AtomicU64 = AtomicU64::new(0);
static IDX_NS: AtomicU64 = AtomicU64::new(0);
static BODY_NS: AtomicU64 = AtomicU64::new(0);
/// Keys that entered a head probe (batch or single).
static KEYS: AtomicU64 = AtomicU64::new(0);
/// Candidate fks collected from probes (before body dedupe).
static CANDS: AtomicU64 = AtomicU64::new(0);
/// Unique fks that paid idx+body_txid.
static BODY_LOOKUPS: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Default, Clone, Copy)]
pub struct Sample {
    pub probe_ns: u64,
    pub idx_ns: u64,
    pub body_ns: u64,
    pub keys: u64,
    pub cands: u64,
    pub body_lookups: u64,
}

impl Sample {
    pub fn sum_ns(&self) -> u64 {
        self.probe_ns
            .saturating_add(self.idx_ns)
            .saturating_add(self.body_ns)
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

pub fn sample_and_reset() -> Sample {
    Sample {
        probe_ns: PROBE_NS.swap(0, Ordering::Relaxed),
        idx_ns: IDX_NS.swap(0, Ordering::Relaxed),
        body_ns: BODY_NS.swap(0, Ordering::Relaxed),
        keys: KEYS.swap(0, Ordering::Relaxed),
        cands: CANDS.swap(0, Ordering::Relaxed),
        body_lookups: BODY_LOOKUPS.swap(0, Ordering::Relaxed),
    }
}
