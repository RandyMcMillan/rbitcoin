//! In-memory TxGraph: clusters, topo linearization, chunk bounds.
//!
//! Cluster = maximal connected component via in-mempool parent/child edges
//! (spend of another mempool output). Limits match plan §3.2 / Core-class caps.

use bitcoin::{OutPoint, Transaction, Txid, Wtxid};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};

/// Hard cap on txs in one cluster (Core `DEFAULT_CLUSTER_LIMIT` = 64).
pub const MAX_CLUSTER_COUNT: usize = 64;
/// Hard cap on total **virtual size** of one cluster (Core `DEFAULT_CLUSTER_SIZE_LIMIT_KVB` = 101).
///
/// Measured as Σ `get_virtual_size(weight)` over cluster members (= Core kvB limit in vbytes).
pub const MAX_CLUSTER_VSIZE: u64 = 101_000;
/// Same limit as weight units: 101_000 vB × 4 WU/vB.
///
/// Kept for call sites that sum `tx.weight().to_wu()`; prefer comparing vsize when possible.
/// **Was incorrectly 101_000 WU** (4× too tight) — mainnet logs rejected single ~25–65 kvB txs.
pub const MAX_CLUSTER_WEIGHT: u64 = MAX_CLUSTER_VSIZE * 4;

/// One live mempool entry (RAM index; body lives on disk).
#[derive(Debug, Clone)]
pub struct TxEntry {
    pub txid: Txid,
    pub wtxid: Wtxid,
    pub fee_sat: u64,
    pub weight: u64,
    /// Slot index in the durable slot table.
    pub slot: u32,
    /// In-mempool parents (txids this tx spends).
    pub parents: BTreeSet<Txid>,
    /// In-mempool children.
    pub children: BTreeSet<Txid>,
}

impl TxEntry {
    pub fn fee_rate_sat_per_kvb(&self) -> u64 {
        rbitcoin_consensus::policy::fee_rate_sat_per_kvb(self.fee_sat, self.weight)
    }
}

/// Contiguous linearization segment used for fee comparison / eviction (P5).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Chunk {
    /// Txids in mining order within this chunk.
    pub txids: Vec<Txid>,
    pub fee_sat: u64,
    pub weight: u64,
}

impl Chunk {
    pub fn fee_rate_sat_per_kvb(&self) -> u64 {
        rbitcoin_consensus::policy::fee_rate_sat_per_kvb(self.fee_sat, self.weight)
    }
}

/// Cluster identity: sorted member set fingerprint (min txid as representative).
#[derive(Debug, Clone)]
pub struct Cluster {
    pub members: BTreeSet<Txid>,
    pub total_weight: u64,
    /// Mining linearization (topo, high fee-rate first among ready).
    pub linearization: Vec<Txid>,
    pub chunks: Vec<Chunk>,
}

/// Live-set graph. Proportional to mempool size, not file size.
#[derive(Debug, Default)]
pub struct TxGraph {
    entries: HashMap<Txid, TxEntry>,
    /// Mempool-created outpoints spent by another mempool tx.
    spends: HashMap<OutPoint, Txid>,
    /// **All** outpoints spent by live mempool txs (chain UTXOs + mempool), for RBF conflicts.
    conflicts: HashMap<OutPoint, Txid>,
    /// Outputs created by mempool txs: (txid, vout) present while unspent in-mempool.
    created: HashSet<OutPoint>,
    /// Sum of live weights (WU) for eviction budget.
    total_weight: u64,
}

impl TxGraph {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn total_weight(&self) -> u64 {
        self.total_weight
    }

    pub fn get(&self, txid: &Txid) -> Option<&TxEntry> {
        self.entries.get(txid)
    }

    pub fn contains(&self, txid: &Txid) -> bool {
        self.entries.contains_key(txid)
    }

    pub fn iter(&self) -> impl Iterator<Item = (&Txid, &TxEntry)> {
        self.entries.iter()
    }

    /// Whether this outpoint is created by a live mempool tx and not yet spent in-mempool.
    pub fn mempool_utxo(&self, op: &OutPoint) -> bool {
        self.created.contains(op) && !self.spends.contains_key(op)
    }

    /// Txid that created `op` if it is still a live mempool output (possibly spent).
    pub fn creator(&self, op: &OutPoint) -> Option<Txid> {
        if self.created.contains(op) {
            Some(op.txid)
        } else {
            None
        }
    }

    /// Direct conflict: another mempool tx already spends this outpoint.
    pub fn conflict_txid(&self, op: &OutPoint) -> Option<Txid> {
        self.conflicts.get(op).copied()
    }

    /// Conflict set for RBF: conflicting txs plus all their descendants.
    pub fn conflict_set(&self, direct: &[Txid]) -> BTreeSet<Txid> {
        let mut set = BTreeSet::new();
        let mut q = VecDeque::new();
        for t in direct {
            if self.entries.contains_key(t) && set.insert(*t) {
                q.push_back(*t);
            }
        }
        while let Some(cur) = q.pop_front() {
            if let Some(e) = self.entries.get(&cur) {
                for c in &e.children {
                    if set.insert(*c) {
                        q.push_back(*c);
                    }
                }
            }
        }
        set
    }

    /// Aggregate fee/weight of a set of live txs.
    pub fn set_fee_weight(&self, set: &BTreeSet<Txid>) -> (u64, u64) {
        let mut fee = 0u64;
        let mut w = 0u64;
        for t in set {
            if let Some(e) = self.entries.get(t) {
                fee = fee.saturating_add(e.fee_sat);
                w = w.saturating_add(e.weight);
            }
        }
        (fee, w)
    }

    /// Insert entry and wire parent/child edges. Does **not** enforce cluster limits
    /// (caller checks via [`cluster_of`] after insert, and may remove).
    pub fn insert(&mut self, entry: TxEntry, tx: &Transaction) {
        let txid = entry.txid;
        let weight = entry.weight;
        // Wire parents that already exist.
        let parents: BTreeSet<Txid> = tx
            .input
            .iter()
            .filter_map(|i| {
                let op = i.previous_output;
                if self.created.contains(&op) {
                    Some(op.txid)
                } else {
                    None
                }
            })
            .collect();

        let mut e = entry;
        e.parents = parents.clone();
        for p in &parents {
            if let Some(pe) = self.entries.get_mut(p) {
                pe.children.insert(txid);
            }
        }
        for inp in &tx.input {
            let op = inp.previous_output;
            self.conflicts.insert(op, txid);
            if self.created.contains(&op) {
                self.spends.insert(op, txid);
            }
        }
        for (vout, _) in tx.output.iter().enumerate() {
            self.created.insert(OutPoint {
                txid,
                vout: vout as u32,
            });
        }
        self.total_weight = self.total_weight.saturating_add(weight);
        self.entries.insert(txid, e);
    }

    /// Remove a tx and unlink edges. Returns the removed entry if present.
    pub fn remove(&mut self, txid: &Txid, tx: &Transaction) -> Option<TxEntry> {
        let e = self.entries.remove(txid)?;
        self.total_weight = self.total_weight.saturating_sub(e.weight);
        for p in &e.parents {
            if let Some(pe) = self.entries.get_mut(p) {
                pe.children.remove(txid);
            }
        }
        for c in &e.children {
            if let Some(ce) = self.entries.get_mut(c) {
                ce.parents.remove(txid);
            }
        }
        for inp in &tx.input {
            if self.spends.get(&inp.previous_output) == Some(txid) {
                self.spends.remove(&inp.previous_output);
            }
            if self.conflicts.get(&inp.previous_output) == Some(txid) {
                self.conflicts.remove(&inp.previous_output);
            }
        }
        for (vout, _) in tx.output.iter().enumerate() {
            self.created.remove(&OutPoint {
                txid: *txid,
                vout: vout as u32,
            });
        }
        Some(e)
    }

    /// Connected component containing `txid` (undirected parent/child).
    pub fn cluster_of(&self, txid: &Txid) -> Option<Cluster> {
        if !self.entries.contains_key(txid) {
            return None;
        }
        let mut members = BTreeSet::new();
        let mut q = VecDeque::new();
        q.push_back(*txid);
        members.insert(*txid);
        while let Some(cur) = q.pop_front() {
            let e = self.entries.get(&cur)?;
            for n in e.parents.iter().chain(e.children.iter()) {
                if members.insert(*n) {
                    q.push_back(*n);
                }
            }
        }
        let total_weight = members
            .iter()
            .map(|t| self.entries.get(t).map(|e| e.weight).unwrap_or(0))
            .sum();
        let linearization = self.linearize(&members);
        let chunks = self.chunkify(&linearization);
        Some(Cluster {
            members,
            total_weight,
            linearization,
            chunks,
        })
    }

    /// Whether adding `extra_weight` and `extra_count` txs that connect to
    /// `seed` members would exceed cluster limits. `seed` = parent txids already
    /// in mempool that the new tx spends (+ the new tx itself counts as 1).
    pub fn cluster_would_exceed(
        &self,
        parent_txids: &BTreeSet<Txid>,
        extra_count: usize,
        extra_weight: u64,
    ) -> bool {
        // Union of clusters of all parents, plus the new tx.
        let mut members = BTreeSet::new();
        for p in parent_txids {
            if let Some(c) = self.cluster_of(p) {
                members.extend(c.members);
            }
        }
        let base_weight: u64 = members
            .iter()
            .map(|t| self.entries.get(t).map(|e| e.weight).unwrap_or(0))
            .sum();
        let count = members.len() + extra_count;
        let weight = base_weight.saturating_add(extra_weight);
        // Core limits are count + vsize (101 kvB). Weight is 4× vsize (WU).
        count > MAX_CLUSTER_COUNT || weight > MAX_CLUSTER_WEIGHT
    }

    /// Topo linearization: among ready txs (parents already emitted or outside
    /// cluster), pick highest fee_rate, then higher fee, then txid.
    fn linearize(&self, members: &BTreeSet<Txid>) -> Vec<Txid> {
        let mut remaining: BTreeSet<Txid> = members.clone();
        let mut done: HashSet<Txid> = HashSet::new();
        let mut out = Vec::with_capacity(members.len());
        while !remaining.is_empty() {
            let mut best: Option<(u64, u64, Txid)> = None; // rate, fee, txid
            for t in &remaining {
                let e = match self.entries.get(t) {
                    Some(e) => e,
                    None => continue,
                };
                let ready = e
                    .parents
                    .iter()
                    .all(|p| !members.contains(p) || done.contains(p));
                if !ready {
                    continue;
                }
                let rate = e.fee_rate_sat_per_kvb();
                let key = (rate, e.fee_sat, *t);
                // Maximize rate, then fee; for equal, smaller txid for stability.
                let better = match &best {
                    None => true,
                    Some((br, bf, bt)) => {
                        rate > *br
                            || (rate == *br && (e.fee_sat > *bf || (e.fee_sat == *bf && t < bt)))
                    }
                };
                if better {
                    best = Some(key);
                }
            }
            let pick = match best {
                Some((_, _, t)) => t,
                None => {
                    // Cycle or bug — emit remaining in txid order.
                    let t = *remaining.iter().next().unwrap();
                    t
                }
            };
            remaining.remove(&pick);
            done.insert(pick);
            out.push(pick);
        }
        out
    }

    /// Split linearization into chunks where fee rates are non-increasing;
    /// a new chunk starts when the next tx has a strictly higher fee rate than
    /// the running chunk average would allow — simplified: group while fee rate
    /// is ≤ previous tx fee rate (Core-style diagram uses more; this is enough
    /// for eviction ordering foundations).
    fn chunkify(&self, lin: &[Txid]) -> Vec<Chunk> {
        let mut chunks = Vec::new();
        if lin.is_empty() {
            return chunks;
        }
        let mut cur_txids = Vec::new();
        let mut cur_fee = 0u64;
        let mut cur_weight = 0u64;
        let mut prev_rate = u64::MAX;
        for t in lin {
            let e = match self.entries.get(t) {
                Some(e) => e,
                None => continue,
            };
            let rate = e.fee_rate_sat_per_kvb();
            if !cur_txids.is_empty() && rate > prev_rate {
                chunks.push(Chunk {
                    txids: std::mem::take(&mut cur_txids),
                    fee_sat: cur_fee,
                    weight: cur_weight,
                });
                cur_fee = 0;
                cur_weight = 0;
            }
            cur_txids.push(*t);
            cur_fee = cur_fee.saturating_add(e.fee_sat);
            cur_weight = cur_weight.saturating_add(e.weight);
            prev_rate = rate;
        }
        if !cur_txids.is_empty() {
            chunks.push(Chunk {
                txids: cur_txids,
                fee_sat: cur_fee,
                weight: cur_weight,
            });
        }
        chunks
    }

    /// Lowest fee-rate chunk across all clusters (for P5 eviction). `None` if empty.
    pub fn worst_chunk(&self) -> Option<(Txid, Chunk)> {
        // Representative = min member txid of cluster.
        let mut seen = HashSet::new();
        let mut worst: Option<(u64, Txid, Chunk)> = None; // rate, rep, chunk
        for txid in self.entries.keys() {
            if seen.contains(txid) {
                continue;
            }
            let c = self.cluster_of(txid)?;
            for m in &c.members {
                seen.insert(*m);
            }
            let rep = *c.members.iter().next()?;
            for ch in c.chunks {
                let rate = ch.fee_rate_sat_per_kvb();
                let better = match &worst {
                    None => true,
                    Some((wr, _, _)) => rate < *wr,
                };
                if better {
                    worst = Some((rate, rep, ch));
                }
            }
        }
        worst.map(|(_, rep, ch)| (rep, ch))
    }

    /// Rebuild helper: clear and re-insert from an ordered list (parents first best-effort).
    pub fn rebuild_from(&mut self, items: Vec<(TxEntry, Transaction)>) {
        self.entries.clear();
        self.spends.clear();
        self.conflicts.clear();
        self.created.clear();
        self.total_weight = 0;
        // Multi-pass: insert when parents are satisfied or not in the set.
        let mut pending: BTreeMap<Txid, (TxEntry, Transaction)> =
            items.into_iter().map(|(e, tx)| (e.txid, (e, tx))).collect();
        let all: HashSet<Txid> = pending.keys().copied().collect();
        while !pending.is_empty() {
            let ready: Vec<Txid> = pending
                .iter()
                .filter(|(_, (_, tx))| {
                    tx.input.iter().all(|i| {
                        let creator = i.previous_output.txid;
                        !all.contains(&creator) || self.entries.contains_key(&creator)
                    })
                })
                .map(|(t, _)| *t)
                .collect();
            let batch = if ready.is_empty() {
                // Cycle — force one.
                vec![*pending.keys().next().unwrap()]
            } else {
                ready
            };
            for t in batch {
                if let Some((e, tx)) = pending.remove(&t) {
                    self.insert(e, &tx);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bitcoin::absolute::LockTime;
    use bitcoin::hashes::Hash;
    use bitcoin::transaction::Version;
    use bitcoin::{Amount, ScriptBuf, Sequence, TxIn, TxOut, Witness};

    fn txid_n(n: u8) -> Txid {
        Txid::from_byte_array([n; 32])
    }

    fn make_tx(spend: Option<(Txid, u32)>, n_out: u32, seed: u8) -> Transaction {
        let prev = spend.unwrap_or_else(|| (txid_n(0xee), 0));
        Transaction {
            version: Version::TWO,
            lock_time: LockTime::ZERO,
            input: vec![TxIn {
                previous_output: OutPoint {
                    txid: prev.0,
                    vout: prev.1,
                },
                script_sig: ScriptBuf::new(),
                sequence: Sequence::ENABLE_RBF_NO_LOCKTIME,
                witness: Witness::new(),
            }],
            output: (0..n_out)
                .map(|i| TxOut {
                    value: Amount::from_sat(1000 + u64::from(i) + u64::from(seed)),
                    script_pubkey: ScriptBuf::from_bytes(vec![0x51, seed, i as u8]),
                })
                .collect(),
        }
    }

    fn entry_for(tx: &Transaction, fee: u64, slot: u32) -> TxEntry {
        TxEntry {
            txid: tx.compute_txid(),
            wtxid: tx.compute_wtxid(),
            fee_sat: fee,
            weight: tx.weight().to_wu(),
            slot,
            parents: BTreeSet::new(),
            children: BTreeSet::new(),
        }
    }

    #[test]
    fn single_tx_cluster() {
        let mut g = TxGraph::new();
        let tx = make_tx(None, 1, 1);
        let e = entry_for(&tx, 1000, 0);
        let id = e.txid;
        g.insert(e, &tx);
        let c = g.cluster_of(&id).unwrap();
        assert_eq!(c.members.len(), 1);
        assert_eq!(c.linearization, vec![id]);
        assert_eq!(c.chunks.len(), 1);
    }

    #[test]
    fn parent_child_same_cluster_linearized() {
        let mut g = TxGraph::new();
        let parent = make_tx(None, 1, 2);
        let pe = entry_for(&parent, 500, 0);
        let pid = pe.txid;
        g.insert(pe, &parent);

        let child = make_tx(Some((pid, 0)), 1, 3);
        let ce = entry_for(&child, 5000, 1); // higher fee rate child
        let cid = ce.txid;
        g.insert(ce, &child);

        let c = g.cluster_of(&pid).unwrap();
        assert_eq!(c.members.len(), 2);
        // Parent must come before child.
        assert_eq!(c.linearization, vec![pid, cid]);
    }

    #[test]
    fn oversize_cluster_detected() {
        let mut g = TxGraph::new();
        // Build a chain of MAX_CLUSTER_COUNT txs.
        let mut prev_id = txid_n(0xee);
        let mut last = txid_n(0);
        for i in 0..MAX_CLUSTER_COUNT {
            let tx = make_tx(Some((prev_id, 0)), 1, i as u8);
            let e = entry_for(&tx, 100, i as u32);
            prev_id = e.txid;
            last = e.txid;
            g.insert(e, &tx);
        }
        let c = g.cluster_of(&last).unwrap();
        assert_eq!(c.members.len(), MAX_CLUSTER_COUNT);
        // New child would exceed count.
        let parents: BTreeSet<Txid> = [last].into_iter().collect();
        assert!(g.cluster_would_exceed(&parents, 1, 100));
    }

    #[test]
    fn cluster_weight_cap_matches_core_101kvb() {
        // Core DEFAULT_CLUSTER_SIZE_LIMIT_KVB = 101 → 101_000 vB → 404_000 WU.
        assert_eq!(MAX_CLUSTER_VSIZE, 101_000);
        assert_eq!(MAX_CLUSTER_WEIGHT, 404_000);
        let g = TxGraph::new();
        let empty = BTreeSet::new();
        // Single-tx 200 kWU (~50 kvB) is under the cap.
        assert!(!g.cluster_would_exceed(&empty, 1, 200_000));
        // Single-tx 405 kWU exceeds (would also fail MAX_STANDARD_TX_WEIGHT=400k).
        assert!(g.cluster_would_exceed(&empty, 1, 405_000));
        // Just over old wrong 101 kWU cap must still be allowed.
        assert!(!g.cluster_would_exceed(&empty, 1, 102_790));
    }

    #[test]
    fn conflict_set_fee_weight_remove_and_worst() {
        let mut g = TxGraph::new();
        let parent = make_tx(None, 1, 10);
        let pe = entry_for(&parent, 100, 0);
        let pid = pe.txid;
        g.insert(pe, &parent);

        let child = make_tx(Some((pid, 0)), 1, 11);
        let ce = entry_for(&child, 50, 1);
        let cid = ce.txid;
        g.insert(ce, &child);

        // Missing cluster.
        assert!(g.cluster_of(&txid_n(0xff)).is_none());

        let direct = vec![pid];
        let set = g.conflict_set(&direct);
        assert!(set.contains(&pid));
        assert!(set.contains(&cid));
        let (fee, w) = g.set_fee_weight(&set);
        assert_eq!(fee, 150);
        assert!(w > 0);

        assert!(g.worst_chunk().is_some());

        // Remove child then parent.
        assert!(g.remove(&cid, &child).is_some());
        assert!(!g.contains(&cid));
        assert!(g.remove(&pid, &parent).is_some());
        assert!(g.worst_chunk().is_none());
        assert!(g.remove(&pid, &parent).is_none());
    }

    #[test]
    fn rebuild_from_orders_parents_first() {
        let mut g = TxGraph::new();
        let parent = make_tx(None, 1, 20);
        let child = make_tx(Some((parent.compute_txid(), 0)), 1, 21);
        // Deliberately child-first in input list.
        let items = vec![
            (entry_for(&child, 10, 1), child.clone()),
            (entry_for(&parent, 10, 0), parent.clone()),
        ];
        g.rebuild_from(items);
        assert!(g.contains(&parent.compute_txid()));
        assert!(g.contains(&child.compute_txid()));
        let c = g.cluster_of(&parent.compute_txid()).unwrap();
        assert_eq!(c.members.len(), 2);
    }
}
