//! Fixed-budget FIFO of create outs for confirm pin / prevout resolve.
//!
//! Holds **outputs + tx meta only** (no inputs/witness). Capacity is in **output
//! count** (default 2²⁴ ≈ 16.7M outs). Eviction is plain FIFO: oldest create is
//! dropped whole when the next insert would exceed the budget. A `HashMap` maps
//! create fk → entry for O(1) pin lookups.
//!
//! Outs are **content-only** (value + script). Durable spender annotations are
//! **not** cached here — pin-time fields are stale by write. Instead we stash
//! packed `body_range` + per-vout relative offsets of the 9-byte spender meta so
//! write structural spentness can bulk-`pread` current durable state (io_uring).

use rbitcoin_store::{OutputRecord, TxRecord};
use std::collections::{HashMap, VecDeque};

/// Default max outputs held in the FIFO (`1 << 24`).
pub const DEFAULT_OUT_FIFO_CAP: u64 = 1 << 24;

/// Env: `RBITCOIN_CONFIRM_OUT_FIFO` (output count). `0` = default.
pub fn out_fifo_cap_from_env() -> u64 {
    match std::env::var("RBITCOIN_CONFIRM_OUT_FIFO") {
        Ok(s) => {
            let n: u64 = s.parse().unwrap_or(DEFAULT_OUT_FIFO_CAP);
            if n == 0 {
                DEFAULT_OUT_FIFO_CAP
            } else {
                n
            }
        }
        Err(_) => DEFAULT_OUT_FIFO_CAP,
    }
}

/// One create's outs for later pin / prevout resolve.
#[derive(Debug, Clone)]
pub struct CreateOuts {
    pub height: u32,
    pub tx: TxRecord,
    /// Dense: `outputs[vout]`. Content-only (spender fields cleared).
    pub outputs: Vec<OutputRecord>,
    /// Proven coinbase flag when known.
    /// - `Some(true)` / `Some(false)` from archive inputs or multi-in meta
    /// - `None` = unknown (pin_new single-in; write may still check maturity)
    pub coinbase: Option<bool>,
    /// Packed Class A `(body_off, body_len)` in `tx.body` (when known).
    pub body_range: Option<(u64, u64)>,
    /// Dense: `spender_rels[vout]` = relative offset of 9-byte spender meta in body.
    /// Empty when body layout was not recorded (cold fallback path).
    pub spender_rels: Vec<u32>,
}

/// FIFO create-outs cache.
#[derive(Debug)]
pub struct OutFifo {
    /// Oldest create fk at the front.
    order: VecDeque<u64>,
    by_fk: HashMap<u64, CreateOuts>,
    total_outs: u64,
    cap_outs: u64,
}

impl OutFifo {
    pub fn new(cap_outs: u64) -> Self {
        Self {
            order: VecDeque::new(),
            by_fk: HashMap::new(),
            total_outs: 0,
            cap_outs: cap_outs.max(1),
        }
    }

    pub fn with_env_cap() -> Self {
        Self::new(out_fifo_cap_from_env())
    }

    pub fn len(&self) -> usize {
        self.by_fk.len()
    }

    pub fn total_outs(&self) -> u64 {
        self.total_outs
    }

    pub fn cap_outs(&self) -> u64 {
        self.cap_outs
    }

    pub fn contains(&self, id: u64) -> bool {
        self.by_fk.contains_key(&id)
    }

    pub fn get(&self, id: u64) -> Option<&CreateOuts> {
        self.by_fk.get(&id)
    }

    /// Insert or replace. Replaces in place (no FIFO re-order). New creates go
    /// to the back; eviction pops the front until `total_outs + n ≤ cap`.
    ///
    /// Returns create fks that were fully evicted.
    pub fn insert(&mut self, id: u64, entry: CreateOuts) -> Vec<u64> {
        let n = entry.outputs.len() as u64;
        if let Some(old) = self.by_fk.get_mut(&id) {
            self.total_outs = self
                .total_outs
                .saturating_sub(old.outputs.len() as u64)
                .saturating_add(n);
            *old = entry;
            // May temporarily exceed cap if replacement grew; evict others.
            return self.evict_until_fits(0);
        }
        let mut evicted = self.evict_until_fits(n);
        self.order.push_back(id);
        self.total_outs = self.total_outs.saturating_add(n);
        self.by_fk.insert(id, entry);
        evicted.append(&mut self.evict_until_fits(0));
        evicted
    }

    fn evict_until_fits(&mut self, need: u64) -> Vec<u64> {
        let mut evicted = Vec::new();
        while self.total_outs.saturating_add(need) > self.cap_outs && !self.order.is_empty() {
            let Some(old_id) = self.order.pop_front() else {
                break;
            };
            // Stale order entries (replaced in place) — skip if already gone.
            if let Some(old) = self.by_fk.remove(&old_id) {
                self.total_outs = self
                    .total_outs
                    .saturating_sub(old.outputs.len() as u64);
                evicted.push(old_id);
            }
        }
        evicted
    }
}

/// Derive coinbase flag from decoded inputs (called once at put).
pub fn is_coinbase_inputs(tx: &TxRecord, inputs: &[rbitcoin_store::InputRecord]) -> bool {
    if tx.input_count != 1 {
        return false;
    }
    inputs
        .first()
        .is_some_and(|i| i.is_coinbase() || i.prev_index == u32::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rbitcoin_primitives::Fk;
    use rbitcoin_store::OutputRecord;

    fn tx(id: u8, n_out: u32) -> TxRecord {
        let mut txid = [0u8; 32];
        txid[0] = id;
        TxRecord {
            txid,
            version: 1,
            locktime: 0,
            input_start_fk: Fk::NULL,
            input_count: 1,
            output_start_fk: Fk::NULL,
            output_count: n_out,
        }
    }

    fn outs(n: usize) -> Vec<OutputRecord> {
        (0..n)
            .map(|i| OutputRecord::unspent(i as i64, vec![0x51]))
            .collect()
    }

    fn entry(height: u32, id: u8, n: usize, cb: bool) -> CreateOuts {
        CreateOuts {
            height,
            tx: tx(id, n as u32),
            outputs: outs(n),
            coinbase: Some(cb),
            body_range: Some((1000, 500)),
            spender_rels: (0..n as u32).map(|i| 40 + i * 20).collect(),
        }
    }

    #[test]
    fn fifo_evicts_oldest_when_over_out_cap() {
        let mut f = OutFifo::new(5);
        f.insert(1, entry(1, 1, 3, true));
        f.insert(2, entry(2, 2, 3, false));
        // 3+3 > 5 → first create gone
        assert!(!f.contains(1));
        assert!(f.contains(2));
        assert_eq!(f.total_outs(), 3);
    }

    #[test]
    fn replace_in_place_keeps_entry() {
        let mut f = OutFifo::new(10);
        f.insert(7, entry(1, 7, 2, false));
        f.insert(7, entry(1, 7, 4, false));
        assert_eq!(f.len(), 1);
        assert_eq!(f.get(7).unwrap().outputs.len(), 4);
        assert_eq!(f.total_outs(), 4);
    }

    #[test]
    fn pin_lookup_by_vout_and_spender_abs() {
        let mut f = OutFifo::new(100);
        f.insert(9, entry(5, 9, 2, false));
        let e = f.get(9).unwrap();
        assert_eq!(e.outputs[1].value, 1);
        assert_eq!(e.height, 5);
        assert_eq!(e.body_range, Some((1000, 500)));
        assert_eq!(e.spender_rels[1], 60);
        assert!(e.outputs[0].spender_field.is_null());
    }
}
