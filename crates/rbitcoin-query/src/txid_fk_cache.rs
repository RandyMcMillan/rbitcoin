//! Byte-capped FIFO process cache: txid → Class A fk.
//!
//! Replaces unbounded `HashMap` during IBD. Miss path is `tx.runs` lookup
//! (see [`crate::tx_run_builder`]) then durable `tx.head` when index is on.

use rbitcoin_primitives::Fk;
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

/// Default ~2 GiB. Override `RBITCOIN_TXID_CACHE_MB` (clamped 64…4096).
pub const DEFAULT_BUDGET_BYTES: usize = 2048 * 1024 * 1024;
const MIN_BUDGET: usize = 64 * 1024 * 1024;
const MAX_BUDGET: usize = 4096 * 1024 * 1024;

/// Approximate heap cost per entry (key + fk + HashMap/VecDeque overhead).
const ENTRY_BYTES: usize = 96;

pub mod stats {
    use super::*;
    pub static HIT: AtomicU64 = AtomicU64::new(0);
    pub static MISS: AtomicU64 = AtomicU64::new(0);
    pub static EVICT: AtomicU64 = AtomicU64::new(0);
    pub static INSERT: AtomicU64 = AtomicU64::new(0);

    pub fn sample_and_reset() -> (u64, u64, u64, u64) {
        (
            HIT.swap(0, Ordering::Relaxed),
            MISS.swap(0, Ordering::Relaxed),
            EVICT.swap(0, Ordering::Relaxed),
            INSERT.swap(0, Ordering::Relaxed),
        )
    }
}

pub struct TxidFkCache {
    inner: Mutex<Inner>,
    budget: usize,
}

struct Inner {
    map: HashMap<[u8; 32], Fk>,
    /// Oldest at front.
    order: VecDeque<[u8; 32]>,
    bytes: usize,
}

impl TxidFkCache {
    pub fn new(budget: usize) -> Self {
        Self {
            inner: Mutex::new(Inner {
                map: HashMap::new(),
                order: VecDeque::new(),
                bytes: 0,
            }),
            budget: budget.max(ENTRY_BYTES),
        }
    }

    pub fn from_env() -> Self {
        let budget = std::env::var("RBITCOIN_TXID_CACHE_MB")
            .ok()
            .and_then(|s| s.parse::<usize>().ok())
            .map(|mb| mb.saturating_mul(1024 * 1024))
            .unwrap_or(DEFAULT_BUDGET_BYTES)
            .clamp(MIN_BUDGET, MAX_BUDGET);
        Self::new(budget)
    }

    /// Test/helper: allow budgets below production floor.
    #[cfg(test)]
    pub fn from_budget_bytes(budget: usize) -> Self {
        Self::new(budget.max(ENTRY_BYTES))
    }

    pub fn budget_bytes(&self) -> usize {
        self.budget
    }

    pub fn approx_bytes(&self) -> usize {
        self.inner.lock().unwrap().bytes
    }

    pub fn len(&self) -> usize {
        self.inner.lock().unwrap().map.len()
    }

    pub fn get(&self, txid: &[u8; 32]) -> Option<Fk> {
        let g = self.inner.lock().unwrap();
        match g.map.get(txid).copied() {
            Some(fk) => {
                stats::HIT.fetch_add(1, Ordering::Relaxed);
                Some(fk)
            }
            None => {
                stats::MISS.fetch_add(1, Ordering::Relaxed);
                None
            }
        }
    }

    pub fn insert(&self, txid: [u8; 32], fk: Fk) {
        if fk.is_null() {
            return;
        }
        let mut g = self.inner.lock().unwrap();
        if let Some(old) = g.map.insert(txid, fk) {
            if old == fk {
                return; // refresh value only
            }
            // Key existed with different fk — keep order position.
            return;
        }
        g.order.push_back(txid);
        g.bytes = g.bytes.saturating_add(ENTRY_BYTES);
        stats::INSERT.fetch_add(1, Ordering::Relaxed);
        while g.bytes > self.budget && !g.order.is_empty() {
            if let Some(old_k) = g.order.pop_front() {
                if g.map.remove(&old_k).is_some() {
                    g.bytes = g.bytes.saturating_sub(ENTRY_BYTES);
                    stats::EVICT.fetch_add(1, Ordering::Relaxed);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fifo_evicts_oldest_under_budget() {
        let c = TxidFkCache::from_budget_bytes(ENTRY_BYTES * 3);
        for i in 0..5u8 {
            let mut t = [0u8; 32];
            t[0] = i;
            c.insert(t, Fk(i as u64 + 1));
        }
        assert!(c.len() <= 3);
        // Oldest (0,1) should be gone; newest present.
        let mut t0 = [0u8; 32];
        t0[0] = 0;
        assert!(c.get(&t0).is_none());
        let mut t4 = [0u8; 32];
        t4[0] = 4;
        assert_eq!(c.get(&t4), Some(Fk(5)));
    }
}
