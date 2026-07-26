//! Fixed-budget FIFO of create outs for confirm pin / prevout resolve.
//!
//! Holds **outputs + slim tx meta only** (no inputs/witness, no durable spender
//! fields). Capacity is in **output count** (default 2²⁴ ≈ 16.7M outs). Eviction
//! is plain FIFO: oldest create is dropped whole when the next insert would
//! exceed the budget. A `HashMap` maps create fk → entry for O(1) pin lookups.
//!
//! **Layout (process RAM):** store-path [`OutputRecord`] / [`TxRecord`] are not
//! retained. Cache rows use [`CompactScript`] (inline ≤34 B scripts) and per-out
//! spender rel offsets (`u32::MAX` = unknown). Reconstruct store types at pin
//! edges only.

use rbitcoin_store::{OutputRecord, TxRecord};
use std::collections::{HashMap, VecDeque};

/// Default max outputs held in the FIFO (`1 << 24`).
pub const DEFAULT_OUT_FIFO_CAP: u64 = 1 << 24;

/// Scripts longer than this go on the heap; standard P2WPKH/P2TR fit inline.
const SCRIPT_INLINE_MAX: usize = 34;

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

/// Compact script bytes: avoid a 24 B `Vec` header for typical standard outputs.
#[derive(Debug, Clone)]
enum CompactScript {
    Inline { len: u8, data: [u8; SCRIPT_INLINE_MAX] },
    Heap(Vec<u8>),
}

impl CompactScript {
    fn from_slice(s: &[u8]) -> Self {
        if s.len() <= SCRIPT_INLINE_MAX {
            let mut data = [0u8; SCRIPT_INLINE_MAX];
            data[..s.len()].copy_from_slice(s);
            CompactScript::Inline {
                len: s.len() as u8,
                data,
            }
        } else {
            CompactScript::Heap(s.to_vec())
        }
    }

    fn as_slice(&self) -> &[u8] {
        match self {
            CompactScript::Inline { len, data } => &data[..*len as usize],
            CompactScript::Heap(v) => v.as_slice(),
        }
    }

    fn to_vec(&self) -> Vec<u8> {
        self.as_slice().to_vec()
    }
}

/// One content-only out in the FIFO (no durable spender annotation).
#[derive(Debug, Clone)]
struct CachedOut {
    value: i64,
    script: CompactScript,
    /// Relative offset of 9-byte spender meta in packed body; `u32::MAX` = unknown.
    spender_rel: u32,
}

impl CachedOut {
    const REL_UNKNOWN: u32 = u32::MAX;

    fn from_output(o: &OutputRecord, spender_rel: u32) -> Self {
        Self {
            value: o.value,
            script: CompactScript::from_slice(&o.script),
            spender_rel,
        }
    }

    fn to_output_record(&self) -> OutputRecord {
        OutputRecord::unspent(self.value, self.script.to_vec())
    }
}

/// One create's outs for later pin / prevout resolve (slim process layout).
#[derive(Debug, Clone)]
pub struct CreateOuts {
    pub height: u32,
    txid: [u8; 32],
    version: i32,
    locktime: u32,
    input_count: u32,
    output_count: u32,
    /// 0 = unknown, 1 = not coinbase, 2 = coinbase.
    coinbase: u8,
    /// Packed body offset; meaningful when `body_len > 0`.
    body_off: u64,
    body_len: u32,
    outs: Vec<CachedOut>,
}

impl CreateOuts {
    /// Build cache row from store types (callers should clear spender fields first).
    pub fn from_store(
        height: u32,
        tx: TxRecord,
        outputs: Vec<OutputRecord>,
        coinbase: Option<bool>,
        body_range: Option<(u64, u64)>,
        spender_rels: Vec<u32>,
    ) -> Self {
        let n = outputs.len();
        let mut outs = Vec::with_capacity(n);
        for (i, o) in outputs.into_iter().enumerate() {
            let rel = spender_rels.get(i).copied().unwrap_or(CachedOut::REL_UNKNOWN);
            // Treat 0-length denserels vec as all-unknown (not all-zero offsets).
            let rel = if spender_rels.is_empty() {
                CachedOut::REL_UNKNOWN
            } else {
                rel
            };
            outs.push(CachedOut::from_output(&o, rel));
        }
        let (body_off, body_len) = match body_range {
            Some((off, len)) => (off, len.min(u32::MAX as u64) as u32),
            None => (0, 0),
        };
        let coinbase = match coinbase {
            None => 0,
            Some(false) => 1,
            Some(true) => 2,
        };
        Self {
            height,
            txid: tx.txid,
            version: tx.version,
            locktime: tx.locktime,
            input_count: tx.input_count,
            output_count: tx.output_count,
            coinbase,
            body_off,
            body_len,
            outs,
        }
    }

    pub fn coinbase(&self) -> Option<bool> {
        match self.coinbase {
            1 => Some(false),
            2 => Some(true),
            _ => None,
        }
    }

    pub fn body_range(&self) -> Option<(u64, u64)> {
        if self.body_len == 0 {
            None
        } else {
            Some((self.body_off, u64::from(self.body_len)))
        }
    }

    pub fn tx_record(&self) -> TxRecord {
        TxRecord {
            txid: self.txid,
            version: self.version,
            locktime: self.locktime,
            input_start_fk: rbitcoin_primitives::Fk::NULL,
            input_count: self.input_count,
            output_start_fk: rbitcoin_primitives::Fk::NULL,
            output_count: self.output_count,
        }
    }

    pub fn txid(&self) -> [u8; 32] {
        self.txid
    }

    pub fn output_count(&self) -> usize {
        self.outs.len()
    }

    /// Dense spender_rels (same length as outs); `u32::MAX` where unknown.
    pub fn dense_spender_rels(&self) -> Vec<u32> {
        self.outs.iter().map(|o| o.spender_rel).collect()
    }

    pub fn all_output_records(&self) -> Vec<OutputRecord> {
        self.outs.iter().map(|o| o.to_output_record()).collect()
    }

    pub fn get_output(&self, vout: u32) -> Option<OutputRecord> {
        self.outs.get(vout as usize).map(|o| o.to_output_record())
    }
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

    /// FIFO order length (should match [`len`] when map/order stay in sync).
    pub fn order_len(&self) -> usize {
        self.order.len()
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
        let n = entry.outs.len() as u64;
        if let Some(old) = self.by_fk.get_mut(&id) {
            self.total_outs = self
                .total_outs
                .saturating_sub(old.outs.len() as u64)
                .saturating_add(n);
            *old = entry;
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
            if let Some(old) = self.by_fk.remove(&old_id) {
                self.total_outs = self.total_outs.saturating_sub(old.outs.len() as u64);
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

    #[test]
    fn compact_script_inline_and_heap() {
        let small = CompactScript::from_slice(&[0x00; 22]);
        assert!(matches!(small, CompactScript::Inline { .. }));
        assert_eq!(small.as_slice().len(), 22);
        let big = CompactScript::from_slice(&[0x51; 40]);
        assert!(matches!(big, CompactScript::Heap(_)));
        assert_eq!(big.as_slice().len(), 40);
    }

    #[test]
    fn create_outs_roundtrip_meta_and_rels() {
        let tx = TxRecord {
            txid: [9u8; 32],
            version: 2,
            locktime: 1,
            input_start_fk: Fk::NULL,
            input_count: 1,
            output_start_fk: Fk::NULL,
            output_count: 2,
        };
        let outs = vec![
            OutputRecord::unspent(10, vec![0x00; 22]),
            OutputRecord::unspent(20, vec![0x51; 40]),
        ];
        let c = CreateOuts::from_store(
            100,
            tx,
            outs,
            Some(false),
            Some((1000, 200)),
            vec![10, 20],
        );
        assert_eq!(c.height, 100);
        assert_eq!(c.txid()[0], 9);
        assert_eq!(c.coinbase(), Some(false));
        assert_eq!(c.body_range(), Some((1000, 200)));
        assert_eq!(c.dense_spender_rels(), vec![10, 20]);
        let tr = c.tx_record();
        assert_eq!(tr.txid, [9u8; 32]);
        assert!(tr.input_start_fk.is_null());
        assert_eq!(c.get_output(0).unwrap().value, 10);
        assert_eq!(c.get_output(1).unwrap().script.len(), 40);
        assert_eq!(c.output_count(), 2);
    }

    #[test]
    fn empty_spender_rels_means_unknown_not_zero() {
        let tx = TxRecord {
            txid: [1u8; 32],
            version: 1,
            locktime: 0,
            input_start_fk: Fk::NULL,
            input_count: 1,
            output_start_fk: Fk::NULL,
            output_count: 1,
        };
        let c = CreateOuts::from_store(
            1,
            tx,
            vec![OutputRecord::unspent(1, vec![0x51])],
            None,
            None,
            Vec::new(),
        );
        assert_eq!(c.dense_spender_rels(), vec![CachedOut::REL_UNKNOWN]);
        assert!(c.body_range().is_none());
    }

    #[test]
    fn out_fifo_cap_env_default() {
        // Unset / garbage → default. Do not assert env mutation across suite.
        let c = out_fifo_cap_from_env();
        assert!(c >= 1);
        assert_eq!(DEFAULT_OUT_FIFO_CAP, 1 << 24);
        let prev = std::env::var_os("RBITCOIN_CONFIRM_OUT_FIFO");
        std::env::set_var("RBITCOIN_CONFIRM_OUT_FIFO", "0");
        assert_eq!(out_fifo_cap_from_env(), DEFAULT_OUT_FIFO_CAP);
        std::env::set_var("RBITCOIN_CONFIRM_OUT_FIFO", "12345");
        assert_eq!(out_fifo_cap_from_env(), 12345);
        std::env::set_var("RBITCOIN_CONFIRM_OUT_FIFO", "not-a-number");
        assert_eq!(out_fifo_cap_from_env(), DEFAULT_OUT_FIFO_CAP);
        match prev {
            Some(v) => std::env::set_var("RBITCOIN_CONFIRM_OUT_FIFO", v),
            None => std::env::remove_var("RBITCOIN_CONFIRM_OUT_FIFO"),
        }
    }
}

// FIFO eviction / pin layout: covered via ConfirmParentCache out_fifo_* + put_dense tests
// and rbitcoin-test three_stage_confirm_and_parent_pin_surface.
