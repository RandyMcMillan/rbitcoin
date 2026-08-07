//! Transaction orphanage for txs missing in-mempool / chain parents.
//!
//! Sized after Bitcoin Core's `TxOrphanage` defaults (master 2024+):
//! - **404_000 weight reserved per peer** (`DEFAULT_RESERVED_ORPHAN_WEIGHT_PER_PEER`)
//! - **Global usage** ≈ reserved × peer budget (we use a fixed ~25-peer budget →
//!   **~10.1M weight** unique orphans — same order as Core with a modest announcer set)
//! - **Latency/count** secondary bound (Core global latency score default **3000**;
//!   we cap unique orphans at **1000** — between legacy 100-tx default and modern score)
//! - Per-tx max **404_000 weight** (standard tx weight)
//!
//! Eviction: FIFO by insert order when over weight or count (simple DoS bound;
//! Core picks DoSiest peer's oldest announcement — we are single-process).

use bitcoin::{Transaction, Txid};
use std::collections::{BTreeSet, HashMap, HashSet, VecDeque};

/// Core `DEFAULT_RESERVED_ORPHAN_WEIGHT_PER_PEER`.
pub const ORPHAN_RESERVED_WEIGHT_PER_PEER: u64 = 404_000;
/// Peer multiplier for a fixed global weight budget (Core scales by announcer peers).
pub const ORPHAN_PEER_BUDGET: u64 = 25;
/// Global unique orphan weight cap (404k × 25 ≈ 10.1M WU).
pub const DEFAULT_ORPHAN_MAX_WEIGHT: u64 = ORPHAN_RESERVED_WEIGHT_PER_PEER * ORPHAN_PEER_BUDGET;
/// Secondary unique-count cap (Core `DEFAULT_MAX_ORPHANAGE_LATENCY_SCORE` = 3000).
pub const DEFAULT_ORPHAN_MAX_COUNT: usize = 3_000;
/// Core `MAX_STANDARD_TX_WEIGHT` — refuse larger orphans.
pub const MAX_ORPHAN_TX_WEIGHT: u64 = 404_000;

#[derive(Debug, Clone)]
struct OrphanEntry {
    tx: Transaction,
    weight: u64,
    /// Missing parent txids (prevout.txid not in mempool/chain at insert).
    missing: BTreeSet<Txid>,
}

/// Side pool of not-yet-acceptable txs waiting on parent(s).
#[derive(Debug, Default)]
pub struct Orphanage {
    by_txid: HashMap<Txid, OrphanEntry>,
    /// parent txid → orphan children waiting on it.
    by_parent: HashMap<Txid, HashSet<Txid>>,
    fifo: VecDeque<Txid>,
    total_weight: u64,
    max_weight: u64,
    max_count: usize,
}

impl Orphanage {
    pub fn new() -> Self {
        Self::with_limits(DEFAULT_ORPHAN_MAX_WEIGHT, DEFAULT_ORPHAN_MAX_COUNT)
    }

    pub fn with_limits(max_weight: u64, max_count: usize) -> Self {
        Self {
            by_txid: HashMap::new(),
            by_parent: HashMap::new(),
            fifo: VecDeque::new(),
            total_weight: 0,
            max_weight: max_weight.max(MAX_ORPHAN_TX_WEIGHT),
            max_count: max_count.max(1),
        }
    }

    pub fn len(&self) -> usize {
        self.by_txid.len()
    }

    pub fn is_empty(&self) -> bool {
        self.by_txid.is_empty()
    }

    pub fn total_weight(&self) -> u64 {
        self.total_weight
    }

    pub fn contains(&self, txid: &Txid) -> bool {
        self.by_txid.contains_key(txid)
    }

    /// Insert orphan waiting on `missing` parent txids. Returns true if newly stored.
    pub fn insert(&mut self, tx: Transaction, missing: BTreeSet<Txid>) -> bool {
        if missing.is_empty() {
            return false;
        }
        let txid = tx.compute_txid();
        if self.by_txid.contains_key(&txid) {
            return false;
        }
        let weight = tx.weight().to_wu();
        if weight > MAX_ORPHAN_TX_WEIGHT {
            return false;
        }
        // Evict FIFO until we fit.
        while !self.by_txid.is_empty()
            && (self.by_txid.len() >= self.max_count
                || self.total_weight.saturating_add(weight) > self.max_weight)
        {
            if !self.evict_oldest() {
                break;
            }
        }
        if self.by_txid.len() >= self.max_count
            || self.total_weight.saturating_add(weight) > self.max_weight
        {
            return false;
        }
        for p in &missing {
            self.by_parent.entry(*p).or_default().insert(txid);
        }
        self.by_txid.insert(
            txid,
            OrphanEntry {
                tx,
                weight,
                missing,
            },
        );
        self.fifo.push_back(txid);
        self.total_weight = self.total_weight.saturating_add(weight);
        true
    }

    fn evict_oldest(&mut self) -> bool {
        let Some(txid) = self.fifo.pop_front() else {
            return false;
        };
        self.remove_txid(&txid);
        true
    }

    fn remove_txid(&mut self, txid: &Txid) {
        let Some(e) = self.by_txid.remove(txid) else {
            return;
        };
        self.total_weight = self.total_weight.saturating_sub(e.weight);
        for p in &e.missing {
            if let Some(set) = self.by_parent.get_mut(p) {
                set.remove(txid);
                if set.is_empty() {
                    self.by_parent.remove(p);
                }
            }
        }
        // fifo may still hold txid if removed mid-list; leave stale ids (skipped on pop)
    }

    /// Take all orphans that listed `parent` as missing (for re-accept).
    pub fn take_children_of(&mut self, parent: &Txid) -> Vec<Transaction> {
        let Some(children) = self.by_parent.remove(parent) else {
            return Vec::new();
        };
        let mut out = Vec::with_capacity(children.len());
        for cid in children {
            if let Some(e) = self.by_txid.remove(&cid) {
                self.total_weight = self.total_weight.saturating_sub(e.weight);
                // Drop other parent links for this orphan.
                for p in &e.missing {
                    if p == parent {
                        continue;
                    }
                    if let Some(set) = self.by_parent.get_mut(p) {
                        set.remove(&cid);
                        if set.is_empty() {
                            self.by_parent.remove(p);
                        }
                    }
                }
                out.push(e.tx);
            }
        }
        out
    }

    /// Drop orphans that are themselves included in a confirmed block.
    ///
    /// Children of confirmed parents are **not** erased here — callers should
    /// [`take_children_of`] and re-accept (Core `AddChildrenToWorkSet` path).
    /// Spending a parent in the block does not invalidate the orphan; the parent
    /// create is now a chain UTXO.
    pub fn erase_for_block(&mut self, block_txids: &[Txid]) {
        let block: HashSet<Txid> = block_txids.iter().copied().collect();
        let drop: Vec<Txid> = self
            .by_txid
            .keys()
            .filter(|t| block.contains(*t))
            .copied()
            .collect();
        for t in drop {
            self.remove_txid(&t);
        }
        // Compact fifo (remove_txid leaves stale ids).
        self.fifo.retain(|t| self.by_txid.contains_key(t));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bitcoin::absolute::LockTime;
    use bitcoin::hashes::Hash;
    use bitcoin::transaction::Version;
    use bitcoin::{Amount, OutPoint, ScriptBuf, Sequence, TxIn, TxOut, Witness};

    fn txid_n(n: u8) -> Txid {
        Txid::from_byte_array([n; 32])
    }

    fn make_orphan(parent: Txid, salt: u8) -> Transaction {
        Transaction {
            version: Version::TWO,
            lock_time: LockTime::ZERO,
            input: vec![TxIn {
                previous_output: OutPoint {
                    txid: parent,
                    vout: 0,
                },
                script_sig: ScriptBuf::new(),
                sequence: Sequence::ENABLE_RBF_NO_LOCKTIME,
                witness: Witness::new(),
            }],
            output: vec![TxOut {
                value: Amount::from_sat(1000 + salt as u64),
                script_pubkey: ScriptBuf::new_p2wpkh(&bitcoin::WPubkeyHash::from_byte_array(
                    [salt; 20],
                )),
            }],
        }
    }

    #[test]
    fn insert_and_take_by_parent() {
        let mut o = Orphanage::new();
        let p = txid_n(1);
        let tx = make_orphan(p, 2);
        let tid = tx.compute_txid();
        let mut miss = BTreeSet::new();
        miss.insert(p);
        assert!(o.insert(tx, miss));
        assert!(o.contains(&tid));
        assert_eq!(o.len(), 1);
        let kids = o.take_children_of(&p);
        assert_eq!(kids.len(), 1);
        assert!(o.is_empty());
    }

    #[test]
    fn fifo_evicts_under_count_cap() {
        let mut o = Orphanage::with_limits(DEFAULT_ORPHAN_MAX_WEIGHT, 2);
        let p = txid_n(9);
        for i in 0..3u8 {
            let tx = make_orphan(p, i + 1);
            let mut miss = BTreeSet::new();
            miss.insert(p);
            o.insert(tx, miss);
        }
        assert!(o.len() <= 2);
        assert!(o.total_weight() <= DEFAULT_ORPHAN_MAX_WEIGHT);
    }

    #[test]
    fn core_like_budget_constants() {
        assert_eq!(ORPHAN_RESERVED_WEIGHT_PER_PEER, 404_000);
        assert_eq!(DEFAULT_ORPHAN_MAX_WEIGHT, 404_000 * 25);
        // ~10 MiB class weight budget for unique orphans.
        assert!(DEFAULT_ORPHAN_MAX_WEIGHT > 10_000_000);
        assert!(DEFAULT_ORPHAN_MAX_WEIGHT < 11_000_000);
    }
}
