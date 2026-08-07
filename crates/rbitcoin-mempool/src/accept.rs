//! Single-tx accept: Libre policy + cluster limits + durable slot write.

use crate::error::MempoolError;
use crate::graph::{TxEntry, TxGraph, MAX_CLUSTER_COUNT, MAX_CLUSTER_WEIGHT};
use crate::orphanage::Orphanage;
use crate::store::Mempool;
use bitcoin::consensus::encode::serialize;
use bitcoin::{OutPoint, Transaction, TxOut, Txid};
use rbitcoin_consensus::policy::{self, PolicyResult};
use std::collections::BTreeSet;

/// Resolve prevouts for mempool acceptance (chain UTXO + in-mempool outputs).
pub trait UtxoProvider {
    fn get_txout(&self, op: &OutPoint) -> Option<TxOut>;
}

/// Map-backed provider for tests and simple callers.
pub struct MapUtxoProvider {
    pub map: std::collections::HashMap<OutPoint, TxOut>,
}

impl UtxoProvider for MapUtxoProvider {
    fn get_txout(&self, op: &OutPoint) -> Option<TxOut> {
        self.map.get(op).cloned()
    }
}

/// Max txs in one ancestor package (Core-class package limit).
pub const MAX_PACKAGE_COUNT: usize = 25;
/// Max total weight (WU) of one package.
pub const MAX_PACKAGE_WEIGHT: u64 = 404_000;
/// Default mempool weight budget (WU) — ~75 MvB class; eviction by worst chunk.
pub const DEFAULT_MAX_MEMPOOL_WEIGHT: u64 = 300_000_000;
/// Incremental relay feerate for RBF (same as Libre min: 0.1 sat/vB = 100 sat/kvB).
pub const INCREMENTAL_RELAY_FEE_RATE_SAT_PER_KVB: u64 = 100;

/// Outcome of a successful accept.
#[derive(Debug, Clone)]
pub struct AcceptResult {
    pub txid: Txid,
    pub fee_sat: u64,
    pub weight: u64,
    pub slot: u32,
}

/// Why accept failed (policy / graph / durable / consensus script).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AcceptError {
    Policy(&'static str),
    MissingPrevout(OutPoint),
    /// Tx parked in the orphanage waiting on missing parent(s). Not a hard reject.
    Orphaned(Txid),
    Duplicate(Txid),
    ClusterTooLarge {
        count: usize,
        weight: u64,
    },
    PackageTooLarge {
        count: usize,
        weight: u64,
    },
    PackageEmpty,
    PackageNotTopo,
    /// Conflicting mempool txs exist and replacement does not pay enough.
    RbfInsufficient,
    Coinbase,
    NotFound(Txid),
    Durable(String),
    /// Consensus script verification failed for one or more inputs.
    Script(String),
}

impl std::fmt::Display for AcceptError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AcceptError::Policy(s) => write!(f, "policy: {s}"),
            AcceptError::MissingPrevout(op) => write!(f, "missing prevout {op}"),
            AcceptError::Orphaned(t) => write!(f, "orphaned {t}"),
            AcceptError::Duplicate(t) => write!(f, "duplicate {t}"),
            AcceptError::ClusterTooLarge { count, weight } => {
                write!(f, "cluster too large count={count} weight={weight}")
            }
            AcceptError::PackageTooLarge { count, weight } => {
                write!(f, "package too large count={count} weight={weight}")
            }
            AcceptError::PackageEmpty => f.write_str("package empty"),
            AcceptError::PackageNotTopo => f.write_str("package not topologically ordered"),
            AcceptError::RbfInsufficient => f.write_str("rbf insufficient fee"),
            AcceptError::Coinbase => f.write_str("coinbase"),
            AcceptError::NotFound(t) => write!(f, "not found {t}"),
            AcceptError::Durable(s) => write!(f, "durable: {s}"),
            AcceptError::Script(s) => write!(f, "script: {s}"),
        }
    }
}

impl std::error::Error for AcceptError {}

impl From<MempoolError> for AcceptError {
    fn from(e: MempoolError) -> Self {
        match e {
            MempoolError::Full => AcceptError::Policy("mempool full"),
            other => AcceptError::Durable(other.to_string()),
        }
    }
}

/// Run consensus script verification for every input (mempool / tip height assumed
/// post-all-softforks: BIP16/65/66/112 active).
fn verify_tx_scripts(tx: &Transaction, prevouts: Vec<TxOut>) -> Result<(), AcceptError> {
    if prevouts.len() != tx.input.len() {
        return Err(AcceptError::Script("prevout count mismatch".into()));
    }
    // `script_bench` mirrors production `ScriptCheckJob` with softfork flags on.
    let job = rbitcoin_consensus::script_bench::JobBytes::new(prevouts, tx.clone());
    rbitcoin_consensus::script_bench::verify_job(&job)
        .map_err(|e| AcceptError::Script(e.to_string()))
}

/// Mempool with RAM TxGraph layered on durable store.
pub struct ActiveMempool {
    pub store: Mempool,
    pub graph: TxGraph,
    /// Cached tx bodies for graph rebuild / remove (live set only).
    bodies: std::collections::HashMap<Txid, Transaction>,
    /// Evict worst chunks when live weight exceeds this.
    pub max_weight: u64,
    /// Side pool of txs waiting on missing parents (Core-class weight budget).
    pub orphanage: Orphanage,
}

impl ActiveMempool {
    pub fn open_or_create(dir: impl Into<std::path::PathBuf>) -> Result<Self, MempoolError> {
        Self::open_or_create_with_limit(dir, DEFAULT_MAX_MEMPOOL_WEIGHT)
    }

    pub fn open_or_create_with_limit(
        dir: impl Into<std::path::PathBuf>,
        max_weight: u64,
    ) -> Result<Self, MempoolError> {
        let mut store = Mempool::open_or_create(dir)?;
        let loaded = store.load_live_txs()?;
        let mut graph = TxGraph::new();
        let mut bodies = std::collections::HashMap::new();
        let mut items = Vec::with_capacity(loaded.len());
        for (slot, fee_sat, weight, tx) in loaded {
            let txid = tx.compute_txid();
            let entry = TxEntry {
                txid,
                wtxid: tx.compute_wtxid(),
                fee_sat,
                weight,
                slot,
                parents: BTreeSet::new(),
                children: BTreeSet::new(),
            };
            bodies.insert(txid, tx.clone());
            items.push((entry, tx));
        }
        graph.rebuild_from(items);
        // Keep live_count in sync with graph after rebuild.
        store.set_live_count(graph.len() as u32);
        Ok(Self {
            store,
            graph,
            bodies,
            max_weight,
            orphanage: Orphanage::new(),
        })
    }

    pub fn live_count(&self) -> usize {
        self.graph.len()
    }

    pub fn generation(&self) -> u64 {
        self.store.generation()
    }

    pub fn flush(&mut self) -> Result<(), MempoolError> {
        self.store.flush()
    }

    /// Compact durable storage (drop DEAD holes) and rebuild RAM graph.
    pub fn compact(&mut self) -> Result<(u32, usize), MempoolError> {
        let (live, body_len) = self.store.compact()?;
        let loaded = self.store.load_live_txs()?;
        let mut graph = TxGraph::new();
        let mut bodies = std::collections::HashMap::new();
        let mut items = Vec::with_capacity(loaded.len());
        for (slot, fee_sat, weight, tx) in loaded {
            let txid = tx.compute_txid();
            let entry = TxEntry {
                txid,
                wtxid: tx.compute_wtxid(),
                fee_sat,
                weight,
                slot,
                parents: BTreeSet::new(),
                children: BTreeSet::new(),
            };
            bodies.insert(txid, tx.clone());
            items.push((entry, tx));
        }
        graph.rebuild_from(items);
        self.graph = graph;
        self.bodies = bodies;
        self.store.set_live_count(live);
        Ok((live, body_len))
    }

    /// Compact when DEAD slots are a large fraction of capacity (file growth bound).
    pub fn maybe_compact(&mut self) -> Result<Option<(u32, usize)>, MempoolError> {
        let (_free, live, dead) = self.store.slot_stats();
        if dead == 0 {
            return Ok(None);
        }
        // Compact if dead ≥ 25% of capacity or dead ≥ live (wasteful body).
        let cap = self.store.meta().slot_cap;
        if dead * 4 >= cap || (live > 0 && dead >= live) || (live == 0 && dead > 0) {
            return Ok(Some(self.compact()?));
        }
        Ok(None)
    }

    /// Accept a single transaction under Libre policy + cluster limits.
    ///
    /// Durable order: write body → LIVE slot → RAM graph. Call [`flush`] to
    /// bump generation so a crash keeps the batch.
    ///
    /// When prevouts are missing from both mempool and chain UTXO, the tx is
    /// parked in the [`Orphanage`] (Core-class weight budget) and
    /// [`AcceptError::Orphaned`] is returned — not a hard peer reject.
    pub fn accept_tx(
        &mut self,
        tx: &Transaction,
        utxos: &impl UtxoProvider,
    ) -> Result<AcceptResult, AcceptError> {
        let r = self.accept_tx_inner(tx, utxos)?;
        // Promote orphans waiting on this parent (best-effort; nested orphans re-park).
        self.promote_orphans_of(r.txid, utxos);
        Ok(r)
    }

    fn accept_tx_inner(
        &mut self,
        tx: &Transaction,
        utxos: &impl UtxoProvider,
    ) -> Result<AcceptResult, AcceptError> {
        if tx.is_coinbase() {
            return Err(AcceptError::Coinbase);
        }
        let txid = tx.compute_txid();
        if self.graph.contains(&txid) {
            return Err(AcceptError::Duplicate(txid));
        }
        // Already parked: soft re-announce of the same orphan.
        if self.orphanage.contains(&txid) {
            return Err(AcceptError::Orphaned(txid));
        }

        // Resolve prevouts: in-mempool first, then provider (chain).
        let mut prevouts: Vec<TxOut> = Vec::with_capacity(tx.input.len());
        let mut parent_txids = BTreeSet::new();
        let mut direct_conflicts: BTreeSet<Txid> = BTreeSet::new();
        let mut missing_parents: BTreeSet<Txid> = BTreeSet::new();
        let mut input_value = 0u64;
        for inp in &tx.input {
            let op = inp.previous_output;
            if let Some(c) = self.graph.conflict_txid(&op) {
                if c != txid {
                    direct_conflicts.insert(c);
                }
            }
            let txout = if let Some(creator) = self.graph.creator(&op) {
                if !self.graph.mempool_utxo(&op) {
                    // Spent in-mempool — must RBF the conflict set.
                    if let Some(c) = self.graph.conflict_txid(&op) {
                        direct_conflicts.insert(c);
                    } else {
                        return Err(AcceptError::Policy("mempool double-spend"));
                    }
                    // Still need the value from the creator's output for fee calc.
                }
                parent_txids.insert(creator);
                let parent_tx = self
                    .bodies
                    .get(&creator)
                    .ok_or(AcceptError::Durable("parent body missing".into()))?;
                match parent_tx.output.get(op.vout as usize).cloned() {
                    Some(o) => o,
                    None => {
                        missing_parents.insert(op.txid);
                        continue;
                    }
                }
            } else if let Some(o) = utxos.get_txout(&op) {
                o
            } else {
                missing_parents.insert(op.txid);
                continue;
            };
            input_value = input_value.saturating_add(txout.value.to_sat());
            prevouts.push(txout);
        }

        if !missing_parents.is_empty() {
            // Park when any parent is unknown (Core TX_MISSING_INPUTS → orphanage).
            if self.orphanage.insert(tx.clone(), missing_parents) {
                return Err(AcceptError::Orphaned(txid));
            }
            // Cap full / overweight: surface as missing for diagnostics.
            return Err(AcceptError::MissingPrevout(tx.input[0].previous_output));
        }

        let output_value: u64 = tx.output.iter().map(|o| o.value.to_sat()).sum();
        if output_value > input_value {
            return Err(AcceptError::Policy("negative fee"));
        }
        let fee_sat = input_value - output_value;
        let weight = tx.weight().to_wu();

        match policy::check_libre_admission(tx, fee_sat, weight) {
            PolicyResult::Standard => {}
            PolicyResult::NonStandard(s) => return Err(AcceptError::Policy(s)),
        }

        // Consensus script checks (same interpreter as block connect). Policy
        // alone is not enough — invalid scripts must never enter the pool or
        // be announced / Electrum-broadcast.
        verify_tx_scripts(tx, prevouts)?;

        // Full RBF (Libre): replace conflicts if replacement pays enough.
        let conflict_set = if !direct_conflicts.is_empty() {
            let direct: Vec<Txid> = direct_conflicts.into_iter().collect();
            let set = self.graph.conflict_set(&direct);
            let (old_fee, old_weight) = self.graph.set_fee_weight(&set);
            if !rbf_pays_for_replacement(fee_sat, weight, old_fee, old_weight) {
                return Err(AcceptError::RbfInsufficient);
            }
            set
        } else {
            BTreeSet::new()
        };
        // Parents that remain after RBF (not in conflict set).
        let parent_txids: BTreeSet<Txid> = parent_txids
            .into_iter()
            .filter(|p| !conflict_set.contains(p))
            .collect();

        if self.graph.cluster_would_exceed(&parent_txids, 1, weight) {
            let mut members = BTreeSet::new();
            for p in &parent_txids {
                if let Some(c) = self.graph.cluster_of(p) {
                    members.extend(c.members);
                }
            }
            // Exclude conflicts that will be removed.
            members.retain(|m| !conflict_set.contains(m));
            let base_w: u64 = members
                .iter()
                .filter_map(|t| self.graph.get(t).map(|e| e.weight))
                .sum();
            return Err(AcceptError::ClusterTooLarge {
                count: members.len() + 1,
                weight: base_w.saturating_add(weight),
            });
        }

        // Apply RBF removals first.
        for c in conflict_set.iter().rev() {
            // Descendants first not required if we remove all independently.
            let _ = self.remove_txid(c);
        }

        // Free a slot before durable write: weight eviction alone used to run
        // *after* append, so a full LIVE slot table failed as "corrupt".
        self.ensure_free_slot(Some(txid))?;

        // Durable write.
        let raw = serialize(tx);
        let slot = self.store.append_live_tx(&raw, &txid, fee_sat, weight)?;

        let entry = TxEntry {
            txid,
            wtxid: tx.compute_wtxid(),
            fee_sat,
            weight,
            slot,
            parents: BTreeSet::new(),
            children: BTreeSet::new(),
        };
        self.graph.insert(entry, tx);
        self.bodies.insert(txid, tx.clone());

        // Post-check (belt): cluster limits still hold.
        if let Some(c) = self.graph.cluster_of(&txid) {
            if c.members.len() > MAX_CLUSTER_COUNT || c.total_weight > MAX_CLUSTER_WEIGHT {
                // Roll back RAM + mark slot dead (best-effort).
                self.graph.remove(&txid, tx);
                self.bodies.remove(&txid);
                let _ = self.store.mark_slot_dead(slot);
                return Err(AcceptError::ClusterTooLarge {
                    count: c.members.len(),
                    weight: c.total_weight,
                });
            }
        }

        // Evict worst chunks until under weight budget (never drop the new tx first).
        self.evict_to_budget(Some(txid))?;

        Ok(AcceptResult {
            txid,
            fee_sat,
            weight,
            slot,
        })
    }

    /// Re-try orphans that listed `parent` as missing (recursive via accept_tx).
    fn promote_orphans_of(&mut self, parent: Txid, utxos: &impl UtxoProvider) {
        let children = self.orphanage.take_children_of(&parent);
        for child in children {
            // accept_tx re-parks if still missing other parents; ignores soft errors.
            let _ = self.accept_tx(&child, utxos);
        }
    }

    /// Ensure the durable slot table has a FREE/DEAD entry for the next append.
    ///
    /// Order: if full of LIVE, **grow** the slot table first (weight may still have
    /// headroom — mainnet 4k-slot stall); if at max cap, **evict** worst chunks.
    /// Never surface as store corruption.
    fn ensure_free_slot(&mut self, protect: Option<Txid>) -> Result<(), AcceptError> {
        if self.store.has_free_slot() {
            return Ok(());
        }
        // Prefer grow when under max (legacy 4k / mid-grow full under weight budget).
        match self.store.grow_slots() {
            Ok(()) => {
                if self.store.has_free_slot() {
                    return Ok(());
                }
            }
            Err(MempoolError::Full) => {}
            Err(e) => return Err(e.into()),
        }
        // At MAX_SLOT_CAP or grow failed to free: evict worst chunks.
        let mut guard = 0u32;
        while !self.store.has_free_slot() && guard < 10_000 {
            guard += 1;
            let Some((_rep, chunk)) = self.graph.worst_chunk() else {
                break;
            };
            if chunk.txids.len() == 1 && protect == chunk.txids.first().copied() {
                break;
            }
            let mut removed = 0usize;
            for t in &chunk.txids {
                if protect == Some(*t) {
                    continue;
                }
                if self.graph.contains(t) {
                    self.remove_txid(t)?;
                    removed += 1;
                }
            }
            if removed == 0 {
                break;
            }
        }
        if self.store.has_free_slot() {
            return Ok(());
        }
        Err(AcceptError::Policy("mempool full"))
    }

    /// Remove lowest-feerate chunks until `total_weight <= max_weight`.
    ///
    /// Prefer not to evict `protect` (the just-accepted tx). Returns how many removed.
    pub fn evict_to_budget(&mut self, protect: Option<Txid>) -> Result<usize, AcceptError> {
        let mut removed = 0usize;
        while self.graph.total_weight() > self.max_weight {
            let Some((_rep, chunk)) = self.graph.worst_chunk() else {
                break;
            };
            // If the only remaining chunk is the protected tx, stop (over budget but keep it).
            if chunk.txids.len() == 1 && protect == chunk.txids.first().copied() {
                break;
            }
            // Evict entire worst chunk (mining order: lowest fee-rate diagram segment).
            for t in &chunk.txids {
                if protect == Some(*t) {
                    continue;
                }
                if self.graph.contains(t) {
                    self.remove_txid(t)?;
                    removed += 1;
                }
            }
            if removed == 0 {
                break; // would only remove protected
            }
        }
        Ok(removed)
    }

    /// Accept an ancestor package (CPFP): txs must be parent-before-child.
    ///
    /// On any failure, already-accepted members of this package are rolled back.
    pub fn accept_package(
        &mut self,
        txs: &[Transaction],
        utxos: &impl UtxoProvider,
    ) -> Result<Vec<AcceptResult>, AcceptError> {
        if txs.is_empty() {
            return Err(AcceptError::PackageEmpty);
        }
        let total_weight: u64 = txs.iter().map(|t| t.weight().to_wu()).sum();
        if txs.len() > MAX_PACKAGE_COUNT || total_weight > MAX_PACKAGE_WEIGHT {
            return Err(AcceptError::PackageTooLarge {
                count: txs.len(),
                weight: total_weight,
            });
        }
        // Reject coinbase / duplicates inside package.
        let mut seen = BTreeSet::new();
        let mut pkg_ids = BTreeSet::new();
        for tx in txs {
            if tx.is_coinbase() {
                return Err(AcceptError::Coinbase);
            }
            let id = tx.compute_txid();
            if !seen.insert(id) {
                return Err(AcceptError::Duplicate(id));
            }
            pkg_ids.insert(id);
        }
        // Topo check: every in-package parent must appear earlier.
        for (i, tx) in txs.iter().enumerate() {
            for inp in &tx.input {
                let parent = inp.previous_output.txid;
                if pkg_ids.contains(&parent) {
                    let parent_pos = txs.iter().position(|t| t.compute_txid() == parent);
                    match parent_pos {
                        Some(p) if p < i => {}
                        _ => return Err(AcceptError::PackageNotTopo),
                    }
                }
            }
        }

        let mut accepted: Vec<AcceptResult> = Vec::with_capacity(txs.len());
        for tx in txs {
            match self.accept_tx(tx, utxos) {
                Ok(r) => accepted.push(r),
                Err(e) => {
                    // Roll back this package's accepts (children first).
                    for r in accepted.iter().rev() {
                        let _ = self.remove_txid(&r.txid);
                    }
                    return Err(e);
                }
            }
        }
        Ok(accepted)
    }

    /// Durable remove one live tx (confirm / RBF / eviction).
    pub fn remove_txid(&mut self, txid: &Txid) -> Result<(), AcceptError> {
        let entry = self
            .graph
            .get(txid)
            .ok_or(AcceptError::NotFound(*txid))?
            .clone();
        let tx = self
            .bodies
            .get(txid)
            .cloned()
            .ok_or(AcceptError::Durable("body missing".into()))?;
        self.store.mark_slot_dead(entry.slot)?;
        self.graph.remove(txid, &tx);
        self.bodies.remove(txid);
        Ok(())
    }

    /// Remove all txs that appear in a confirmed block (coinbase ignored if present).
    ///
    /// Missing mempool entries are skipped (already not in pool). Returns how many removed.
    /// May trigger compaction when DEAD slots dominate.
    ///
    /// Also drops orphanage entries that are confirmed or conflict with the block,
    /// and best-effort re-accepts orphans whose parent just confirmed (caller must
    /// pass a UTXO view that includes the new tip — use [`remove_for_block_with_utxo`]).
    pub fn remove_for_block(&mut self, block_txids: &[Txid]) -> Result<usize, AcceptError> {
        self.remove_for_block_with_utxo(
            block_txids,
            &MapUtxoProvider {
                map: std::collections::HashMap::new(),
            },
        )
    }

    /// Like [`remove_for_block`], then promote orphans of confirmed parents via `utxos`.
    pub fn remove_for_block_with_utxo(
        &mut self,
        block_txids: &[Txid],
        utxos: &impl UtxoProvider,
    ) -> Result<usize, AcceptError> {
        let mut n = 0usize;
        for txid in block_txids {
            if self.graph.contains(txid) {
                self.remove_txid(txid)?;
                n += 1;
            }
        }
        // Re-accept orphans waiting on confirmed parents first, then drop any
        // orphan that was itself included in the block.
        for txid in block_txids {
            self.promote_orphans_of(*txid, utxos);
        }
        self.orphanage.erase_for_block(block_txids);
        if n > 0 {
            let _ = self.maybe_compact();
        }
        Ok(n)
    }

    pub fn orphan_count(&self) -> usize {
        self.orphanage.len()
    }

    /// Re-accept non-coinbase txs after a reorg disconnect (best-effort).
    ///
    /// Failures on individual txs are collected; successful accepts remain.
    pub fn reorg_disconnect_reaccept(
        &mut self,
        txs: &[Transaction],
        utxos: &impl UtxoProvider,
    ) -> Vec<Result<AcceptResult, AcceptError>> {
        txs.iter()
            .filter(|t| !t.is_coinbase())
            .map(|t| self.accept_tx(t, utxos))
            .collect()
    }

    /// Lookup a live body (for tests / Electrum unconf).
    pub fn get_tx(&self, txid: &Txid) -> Option<&Transaction> {
        self.bodies.get(txid)
    }
}

/// Full-RBF / package-RBF fee check (Libre: no BIP125 signaling).
///
/// Requires strictly higher absolute fee, higher feerate, and pays incremental
/// relay fee on the **replacement** vsize.
pub fn rbf_pays_for_replacement(
    new_fee: u64,
    new_weight: u64,
    old_fee: u64,
    old_weight: u64,
) -> bool {
    if new_fee <= old_fee {
        return false;
    }
    let new_rate = policy::fee_rate_sat_per_kvb(new_fee, new_weight);
    let old_rate = policy::fee_rate_sat_per_kvb(old_fee, old_weight);
    if new_rate <= old_rate {
        return false;
    }
    let vsize = policy::get_virtual_size(new_weight);
    // ceil(vsize * rate / 1000)
    let inc = vsize
        .saturating_mul(INCREMENTAL_RELAY_FEE_RATE_SAT_PER_KVB)
        .saturating_add(999)
        / 1000;
    new_fee.saturating_sub(old_fee) >= inc
}

#[cfg(test)]
mod tests {
    use super::*;
    use bitcoin::absolute::LockTime;
    use bitcoin::hashes::Hash;
    use bitcoin::transaction::Version;
    use bitcoin::{Amount, ScriptBuf, Sequence, TxIn, Witness};
    use std::collections::HashMap;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn tmp_dir() -> std::path::PathBuf {
        let n = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let p = std::env::temp_dir().join(format!("rbitcoin-mempool-accept-{n}"));
        let _ = std::fs::remove_dir_all(&p);
        p
    }

    fn chain_utxo(value: u64) -> (OutPoint, TxOut, MapUtxoProvider) {
        let op = OutPoint {
            txid: Txid::from_byte_array([0xab; 32]),
            vout: 0,
        };
        let txout = TxOut {
            value: Amount::from_sat(value),
            script_pubkey: ScriptBuf::from_bytes(vec![0x51]),
        };
        let mut map = HashMap::new();
        map.insert(op, txout.clone());
        (op, txout, MapUtxoProvider { map })
    }

    fn spend_tx(op: OutPoint, out_value: u64) -> Transaction {
        Transaction {
            version: Version::TWO,
            lock_time: LockTime::ZERO,
            input: vec![TxIn {
                previous_output: op,
                script_sig: ScriptBuf::new(),
                sequence: Sequence::ENABLE_RBF_NO_LOCKTIME,
                witness: Witness::new(),
            }],
            output: vec![TxOut {
                value: Amount::from_sat(out_value),
                script_pubkey: ScriptBuf::from_bytes(vec![0x51]),
            }],
        }
    }

    #[test]
    fn accept_single_flush_reopen() {
        let dir = tmp_dir();
        let (op, _, utxos) = chain_utxo(100_000);
        let tx = spend_tx(op, 99_000); // fee 1000
        let txid = tx.compute_txid();
        {
            let mut mp = ActiveMempool::open_or_create(&dir).unwrap();
            let r = mp.accept_tx(&tx, &utxos).expect("accept");
            assert_eq!(r.txid, txid);
            assert_eq!(r.fee_sat, 1000);
            assert_eq!(mp.live_count(), 1);
            mp.flush().unwrap();
            assert!(mp.generation() >= 1);
        }
        {
            let mp = ActiveMempool::open_or_create(&dir).unwrap();
            assert_eq!(mp.live_count(), 1);
            assert!(mp.graph.contains(&txid));
            let e = mp.graph.get(&txid).unwrap();
            assert_eq!(e.fee_sat, 1000);
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Consensus script check must reject spends of real templates with empty witness.
    /// (Regression: accept used to skip verify and only apply Libre policy.)
    #[test]
    fn reject_invalid_p2wpkh_script() {
        use bitcoin::WPubkeyHash;

        let dir = tmp_dir();
        // Standard P2WPKH spk — not anyone-can-spend; empty witness fails.
        let wpkh = WPubkeyHash::from_byte_array([0x11; 20]);
        let spk = ScriptBuf::new_p2wpkh(&wpkh);
        let op = OutPoint {
            txid: Txid::from_byte_array([0xab; 32]),
            vout: 0,
        };
        let txout = TxOut {
            value: Amount::from_sat(100_000),
            script_pubkey: spk,
        };
        let mut map = HashMap::new();
        map.insert(op, txout);
        let utxos = MapUtxoProvider { map };
        let tx = spend_tx(op, 99_000); // empty scriptSig + empty witness
        let mut mp = ActiveMempool::open_or_create(&dir).unwrap();
        let err = mp.accept_tx(&tx, &utxos).unwrap_err();
        assert!(
            matches!(err, AcceptError::Script(_)),
            "expected Script reject, got {err}"
        );
        assert_eq!(mp.live_count(), 0);
        // Sanity: ACS still accepted (Libre + consensus anyone-can-spend).
        let (op2, _, utxos2) = chain_utxo(50_000);
        let ok = spend_tx(op2, 49_000);
        mp.accept_tx(&ok, &utxos2).expect("ACS still ok");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn reject_low_feerate() {
        let dir = tmp_dir();
        let (op, _, utxos) = chain_utxo(100_000);
        // fee 1 sat — below 0.1 sat/vB for any real tx weight
        let tx = spend_tx(op, 99_999);
        let mut mp = ActiveMempool::open_or_create(&dir).unwrap();
        let err = mp.accept_tx(&tx, &utxos).unwrap_err();
        assert!(matches!(err, AcceptError::Policy("min relay fee")));
        assert_eq!(mp.live_count(), 0);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn dust_and_op_true_allowed() {
        let dir = tmp_dir();
        let (op, _, utxos) = chain_utxo(100_000);
        // 1-sat output is dust under Core; Libre allows it.
        let tx = spend_tx(op, 1);
        let mut mp = ActiveMempool::open_or_create(&dir).unwrap();
        mp.accept_tx(&tx, &utxos).expect("dust ok");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn child_spends_parent_cluster() {
        let dir = tmp_dir();
        let (op, _, utxos) = chain_utxo(100_000);
        let parent = spend_tx(op, 90_000);
        let pid = parent.compute_txid();
        let mut mp = ActiveMempool::open_or_create(&dir).unwrap();
        mp.accept_tx(&parent, &utxos).unwrap();

        let child = spend_tx(OutPoint { txid: pid, vout: 0 }, 80_000);
        mp.accept_tx(&child, &utxos).unwrap();
        assert_eq!(mp.live_count(), 2);
        let c = mp.graph.cluster_of(&pid).unwrap();
        assert_eq!(c.members.len(), 2);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn oversize_cluster_rejected() {
        let dir = tmp_dir();
        let (op, _, utxos) = chain_utxo(10_000_000);
        let mut mp = ActiveMempool::open_or_create(&dir).unwrap();
        // Chain of MAX_CLUSTER_COUNT, then one more fails.
        let mut prev_op = op;
        for i in 0..MAX_CLUSTER_COUNT {
            // Large fee so policy passes; leave enough for remaining.
            let remain = 10_000_000u64 - (i as u64 + 1) * 1_000;
            let tx = spend_tx(prev_op, remain);
            let last_txid = tx.compute_txid();
            mp.accept_tx(&tx, &utxos)
                .unwrap_or_else(|e| panic!("i={i}: {e}"));
            prev_op = OutPoint {
                txid: last_txid,
                vout: 0,
            };
        }
        assert_eq!(mp.live_count(), MAX_CLUSTER_COUNT);
        let remain = 10_000_000u64 - (MAX_CLUSTER_COUNT as u64 + 1) * 1_000;
        let extra = spend_tx(prev_op, remain);
        let err = mp.accept_tx(&extra, &utxos).unwrap_err();
        assert!(matches!(err, AcceptError::ClusterTooLarge { .. }), "{err}");
        assert_eq!(mp.live_count(), MAX_CLUSTER_COUNT);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn annex_reject() {
        let dir = tmp_dir();
        let (op, _, utxos) = chain_utxo(100_000);
        let mut tx = spend_tx(op, 90_000);
        tx.input[0].witness = Witness::from_slice(&[vec![0x01], vec![0x50, 0x01]]);
        let mut mp = ActiveMempool::open_or_create(&dir).unwrap();
        let err = mp.accept_tx(&tx, &utxos).unwrap_err();
        assert!(matches!(err, AcceptError::Policy("libre annex")));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn cpfp_package_accept() {
        let dir = tmp_dir();
        let (op, _, utxos) = chain_utxo(100_000);
        // Parent low fee still above min relay if weight small.
        let parent = spend_tx(op, 99_000); // fee 1000
        let pid = parent.compute_txid();
        let child = spend_tx(OutPoint { txid: pid, vout: 0 }, 90_000);
        let mut mp = ActiveMempool::open_or_create(&dir).unwrap();
        let res = mp
            .accept_package(&[parent.clone(), child.clone()], &utxos)
            .expect("package");
        assert_eq!(res.len(), 2);
        assert_eq!(mp.live_count(), 2);
        let c = mp.graph.cluster_of(&pid).unwrap();
        assert_eq!(c.members.len(), 2);
        // Wrong order rejected.
        let mut mp2 = ActiveMempool::open_or_create(tmp_dir()).unwrap();
        let err = mp2.accept_package(&[child, parent], &utxos).unwrap_err();
        assert!(matches!(err, AcceptError::PackageNotTopo));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn mine_clears_mempool() {
        let dir = tmp_dir();
        let (op, _, utxos) = chain_utxo(100_000);
        let tx = spend_tx(op, 90_000);
        let txid = tx.compute_txid();
        let mut mp = ActiveMempool::open_or_create(&dir).unwrap();
        mp.accept_tx(&tx, &utxos).unwrap();
        assert_eq!(mp.live_count(), 1);
        let n = mp.remove_for_block(&[txid]).unwrap();
        assert_eq!(n, 1);
        assert_eq!(mp.live_count(), 0);
        mp.flush().unwrap();
        let mp = ActiveMempool::open_or_create(&dir).unwrap();
        assert_eq!(mp.live_count(), 0);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn reorg_reaccept() {
        let dir = tmp_dir();
        let (op, _, utxos) = chain_utxo(100_000);
        let tx = spend_tx(op, 90_000);
        let txid = tx.compute_txid();
        let mut mp = ActiveMempool::open_or_create(&dir).unwrap();
        mp.accept_tx(&tx, &utxos).unwrap();
        mp.remove_for_block(&[txid]).unwrap();
        assert_eq!(mp.live_count(), 0);
        let results = mp.reorg_disconnect_reaccept(&[tx], &utxos);
        assert_eq!(results.len(), 1);
        assert!(results[0].is_ok());
        assert_eq!(mp.live_count(), 1);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn full_rbf_replaces_conflict() {
        let dir = tmp_dir();
        let (op, _, utxos) = chain_utxo(100_000);
        let low = spend_tx(op, 99_000); // fee 1000
        let high = spend_tx(op, 50_000); // fee 50000 — same input, full RBF
        let low_id = low.compute_txid();
        let high_id = high.compute_txid();
        let mut mp = ActiveMempool::open_or_create(&dir).unwrap();
        mp.accept_tx(&low, &utxos).unwrap();
        assert!(mp.graph.contains(&low_id));
        mp.accept_tx(&high, &utxos).expect("rbf");
        assert!(!mp.graph.contains(&low_id));
        assert!(mp.graph.contains(&high_id));
        assert_eq!(mp.live_count(), 1);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn rbf_rejects_insufficient() {
        let dir = tmp_dir();
        let (op, _, utxos) = chain_utxo(100_000);
        let high = spend_tx(op, 50_000); // fee 50000
        let low = spend_tx(op, 99_000); // fee 1000 — cannot replace
        let mut mp = ActiveMempool::open_or_create(&dir).unwrap();
        mp.accept_tx(&high, &utxos).unwrap();
        let err = mp.accept_tx(&low, &utxos).unwrap_err();
        assert!(matches!(err, AcceptError::RbfInsufficient), "{err}");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn chunk_eviction_under_weight_budget() {
        let dir = tmp_dir();
        // Tiny budget forces eviction after a few accepts.
        let mut mp = ActiveMempool::open_or_create_with_limit(&dir, 800).unwrap();
        // Distinct chain UTXOs so they are independent clusters.
        for i in 0u8..8 {
            let op = OutPoint {
                txid: Txid::from_byte_array([i.wrapping_add(1); 32]),
                vout: 0,
            };
            let txout = TxOut {
                value: Amount::from_sat(100_000),
                script_pubkey: ScriptBuf::from_bytes(vec![0x51]),
            };
            let mut map = HashMap::new();
            map.insert(op, txout);
            let utxos = MapUtxoProvider { map };
            // Vary fees so worst-chunk ordering is defined: low fee first.
            let out = 99_000u64 - u64::from(i) * 100; // higher i → higher fee
            let tx = spend_tx(op, out);
            mp.accept_tx(&tx, &utxos)
                .unwrap_or_else(|e| panic!("i={i}: {e}"));
        }
        assert!(mp.graph.total_weight() <= mp.max_weight + 500); // allow one protected overflow
        assert!(mp.live_count() < 8, "some eviction expected");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn rbf_pays_helper() {
        assert!(rbf_pays_for_replacement(10_000, 4000, 1000, 4000));
        assert!(!rbf_pays_for_replacement(1000, 4000, 10_000, 4000));
        assert!(!rbf_pays_for_replacement(1000, 4000, 1000, 4000));
    }

    #[test]
    fn compact_reclaims_dead_and_preserves_live() {
        let dir = tmp_dir();
        let (op, _, utxos) = chain_utxo(100_000);
        let tx = spend_tx(op, 90_000);
        let txid = tx.compute_txid();
        let mut mp = ActiveMempool::open_or_create(&dir).unwrap();
        mp.accept_tx(&tx, &utxos).unwrap();
        let body_before = mp.store.body_logical_len().unwrap();
        // Confirm-remove leaves a DEAD slot (body still holds the old payload).
        mp.remove_for_block(&[txid]).unwrap();
        // Re-accept so we have live + dead history in the body file.
        mp.accept_tx(&tx, &utxos).unwrap();
        let (_f, live, dead) = mp.store.slot_stats();
        assert_eq!(live, 1);
        // remove_for_block may already have auto-compacted; either way compact is safe.
        let _ = dead;
        let (live_after, body_after) = mp.compact().unwrap();
        assert_eq!(live_after, 1);
        assert!(body_after <= body_before + 256);
        assert_eq!(mp.live_count(), 1);
        mp.flush().unwrap();
        let mp2 = ActiveMempool::open_or_create(&dir).unwrap();
        assert_eq!(mp2.live_count(), 1);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn accept_error_display_and_reject_paths() {
        use std::error::Error;
        let errs = [
            AcceptError::Policy("x".into()),
            AcceptError::MissingPrevout(OutPoint {
                txid: Txid::from_byte_array([1; 32]),
                vout: 0,
            }),
            AcceptError::Orphaned(Txid::from_byte_array([4; 32])),
            AcceptError::Duplicate(Txid::from_byte_array([2; 32])),
            AcceptError::ClusterTooLarge {
                count: 3,
                weight: 9,
            },
            AcceptError::PackageTooLarge {
                count: 2,
                weight: 8,
            },
            AcceptError::PackageEmpty,
            AcceptError::PackageNotTopo,
            AcceptError::RbfInsufficient,
            AcceptError::Coinbase,
            AcceptError::NotFound(Txid::from_byte_array([3; 32])),
            AcceptError::Durable("d".into()),
            AcceptError::Script("s".into()),
        ];
        for e in &errs {
            assert!(!e.to_string().is_empty());
            let _ = e as &dyn Error;
        }
        // From MempoolError.
        let from_io: AcceptError = MempoolError::BadMagic.into();
        assert!(from_io.to_string().contains("durable"));
        let from_full: AcceptError = MempoolError::Full.into();
        assert!(matches!(from_full, AcceptError::Policy("mempool full")));

        let dir = tmp_dir();
        let (op, _, utxos) = chain_utxo(100_000);
        let mut mp = ActiveMempool::open_or_create(&dir).unwrap();

        // Coinbase reject.
        let cb = Transaction {
            version: Version::TWO,
            lock_time: LockTime::ZERO,
            input: vec![TxIn {
                previous_output: OutPoint::null(),
                script_sig: ScriptBuf::new(),
                sequence: Sequence::MAX,
                witness: Witness::new(),
            }],
            output: vec![TxOut {
                value: Amount::from_sat(50),
                script_pubkey: ScriptBuf::from_bytes(vec![0x51]),
            }],
        };
        assert!(matches!(
            mp.accept_tx(&cb, &utxos),
            Err(AcceptError::Coinbase)
        ));

        // Package empty / too large.
        assert!(matches!(
            mp.accept_package(&[], &utxos),
            Err(AcceptError::PackageEmpty)
        ));
        // Count over MAX_PACKAGE_COUNT (25).
        let many: Vec<Transaction> = (0..MAX_PACKAGE_COUNT + 1)
            .map(|i| {
                spend_tx(
                    OutPoint {
                        txid: Txid::from_byte_array({
                            let mut b = [0u8; 32];
                            b[0] = i as u8;
                            b
                        }),
                        vout: 0,
                    },
                    1,
                )
            })
            .collect();
        assert!(matches!(
            mp.accept_package(&many, &utxos),
            Err(AcceptError::PackageTooLarge { .. })
        ));

        let tx = spend_tx(op, 99_000);
        mp.accept_tx(&tx, &utxos).unwrap();
        // Duplicate.
        assert!(matches!(
            mp.accept_tx(&tx, &utxos),
            Err(AcceptError::Duplicate(_))
        ));

        // Missing prevout → parked in orphanage (Core-class soft accept).
        let (_op2, _, empty) = chain_utxo(50_000);
        let missing = spend_tx(
            OutPoint {
                txid: Txid::from_byte_array([0xcd; 32]),
                vout: 0,
            },
            1,
        );
        let missing_id = missing.compute_txid();
        assert!(matches!(
            mp.accept_tx(&missing, &empty),
            Err(AcceptError::Orphaned(_))
        ));
        assert!(mp.orphanage.contains(&missing_id));
        assert_eq!(mp.orphan_count(), 1);

        // maybe_compact with only live → None.
        assert!(mp.maybe_compact().unwrap().is_none());

        // Package coinbase / not topo / oversized count.
        assert!(matches!(
            mp.accept_package(&[cb], &utxos),
            Err(AcceptError::Coinbase)
        ));
        let a = spend_tx(op, 98_000);
        let b = spend_tx(
            OutPoint {
                txid: a.compute_txid(),
                vout: 0,
            },
            97_000,
        );
        // Child before parent → not topo.
        assert!(matches!(
            mp.accept_package(&[b.clone(), a.clone()], &utxos),
            Err(AcceptError::PackageNotTopo)
        ));
        // Duplicate in package.
        assert!(matches!(
            mp.accept_package(&[a.clone(), a.clone()], &utxos),
            Err(AcceptError::Duplicate(_))
        ));

        // remove unknown.
        assert!(matches!(
            mp.remove_txid(&Txid::from_byte_array([0xee; 32])),
            Err(AcceptError::NotFound(_))
        ));

        // Negative fee.
        let fat = spend_tx(op, 200_000);
        assert!(matches!(
            mp.accept_tx(&fat, &utxos),
            Err(AcceptError::Policy(_))
        ));

        // rbf_pays_for_replacement pure unit.
        assert!(!rbf_pays_for_replacement(100, 400, 100, 400));
        assert!(!rbf_pays_for_replacement(100, 400, 200, 400));
        // Higher fee and rate with incremental cover.
        assert!(rbf_pays_for_replacement(50_000, 400, 1_000, 400));

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn rbf_replaces_conflicting_spend() {
        let dir = tmp_dir();
        let (op, _, utxos) = chain_utxo(1_000_000);
        let mut mp = ActiveMempool::open_or_create(&dir).unwrap();
        let low = spend_tx(op, 999_000); // fee 1000
        mp.accept_tx(&low, &utxos).unwrap();
        // Conflict: same prevout, higher fee.
        let high = spend_tx(op, 900_000); // fee 100_000
        let r = mp.accept_tx(&high, &utxos).expect("rbf");
        assert_eq!(r.txid, high.compute_txid());
        assert!(!mp.graph.contains(&low.compute_txid()));
        assert!(mp.graph.contains(&high.compute_txid()));
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Child arrives before parent: park in orphanage, promote when parent accepts.
    #[test]
    fn orphan_park_then_promote_on_parent() {
        let dir = tmp_dir();
        let (op, _, utxos) = chain_utxo(100_000);
        let mut mp = ActiveMempool::open_or_create(&dir).unwrap();
        let parent = spend_tx(op, 99_000);
        let parent_id = parent.compute_txid();
        let child = spend_tx(
            OutPoint {
                txid: parent_id,
                vout: 0,
            },
            98_000,
        );
        let child_id = child.compute_txid();

        assert!(matches!(
            mp.accept_tx(&child, &utxos),
            Err(AcceptError::Orphaned(_))
        ));
        assert_eq!(mp.orphan_count(), 1);
        assert!(!mp.graph.contains(&child_id));

        mp.accept_tx(&parent, &utxos).expect("parent");
        assert!(mp.graph.contains(&parent_id));
        assert!(
            mp.graph.contains(&child_id),
            "child should promote when parent enters mempool"
        );
        assert_eq!(mp.orphan_count(), 0);
        assert_eq!(mp.live_count(), 2);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Slot table growth under accept: legacy tiny sidecar must not fail as Durable corrupt.
    #[test]
    fn accept_grows_legacy_tiny_slot_table() {
        use std::fs;
        let dir = tmp_dir();
        fs::create_dir_all(&dir).unwrap();
        // 4-slot meta/slots/body (same layout as store unit test).
        {
            let mut meta = [0u8; 64];
            meta[0..4].copy_from_slice(b"rBMP");
            meta[4..6].copy_from_slice(&1u16.to_le_bytes());
            meta[16..20].copy_from_slice(&4u32.to_le_bytes());
            fs::write(dir.join("meta"), meta).unwrap();
            let mut slots = vec![0u8; 16 + 4 * 48];
            slots[0..4].copy_from_slice(b"rBMP");
            slots[4..6].copy_from_slice(&1u16.to_le_bytes());
            slots[8..12].copy_from_slice(&4u32.to_le_bytes());
            fs::write(dir.join("slots"), &slots).unwrap();
            let mut body = vec![0u8; 16];
            body[0..4].copy_from_slice(b"rBMP");
            body[4..6].copy_from_slice(&1u16.to_le_bytes());
            body[8..16].copy_from_slice(&16u64.to_le_bytes());
            fs::write(dir.join("tx.body"), &body).unwrap();
        }
        // Large weight budget so eviction is not the free-slot path.
        let mut mp = ActiveMempool::open_or_create_with_limit(&dir, 300_000_000).unwrap();
        assert_eq!(mp.store.meta().slot_cap, 4);
        // Four independent chain utxos → four live slots.
        for i in 0..4u8 {
            let op = OutPoint {
                txid: Txid::from_byte_array({
                    let mut b = [0xab; 32];
                    b[0] = i;
                    b
                }),
                vout: 0,
            };
            let mut map = HashMap::new();
            map.insert(
                op,
                TxOut {
                    value: Amount::from_sat(100_000),
                    script_pubkey: ScriptBuf::from_bytes(vec![0x51]),
                },
            );
            let utxos = MapUtxoProvider { map };
            let tx = spend_tx(op, 99_000);
            mp.accept_tx(&tx, &utxos)
                .unwrap_or_else(|e| panic!("accept {i}: {e}"));
        }
        assert_eq!(mp.live_count(), 4);
        // Fifth must grow slots, not Durable(corrupt: slot table full).
        let op = OutPoint {
            txid: Txid::from_byte_array([0xcd; 32]),
            vout: 0,
        };
        let mut map = HashMap::new();
        map.insert(
            op,
            TxOut {
                value: Amount::from_sat(100_000),
                script_pubkey: ScriptBuf::from_bytes(vec![0x51]),
            },
        );
        let utxos = MapUtxoProvider { map };
        let tx = spend_tx(op, 99_000);
        let r = mp.accept_tx(&tx, &utxos);
        assert!(
            r.is_ok(),
            "expected free-slot (evict or grow), got {:?}",
            r.err().map(|e| e.to_string())
        );
        // Evict-for-slot may keep cap=4; grow path raises it. Either is fine —
        // must never be Durable(corrupt: slot table full).
        assert_eq!(mp.live_count(), 5.min(mp.store.meta().slot_cap as usize));
        // Graph and store agree we still hold a full-ish set.
        assert!(mp.live_count() >= 4);
        let _ = fs::remove_dir_all(&dir);
    }
}
