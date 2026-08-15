//! IBD most-work reorg: classify BadPrev, rank candidates, apply via `accept_branch`.
//!
//! See [`docs/design-ibd-most-work-reorg.md`]. Orchestration-thread only.

use crate::chain::{AcceptOutcome, ChainHub};
use crate::error::NetError;
use crate::most_work::{
    select_most_work, sum_work, work_better, InvalidHashSet, SelectOutcome, WorkCandidate,
};
use bitcoin::hashes::Hash;
use bitcoin::{Block, BlockHash, CompactTarget, Target};
use rbitcoin_log::{info, warn};
use rbitcoin_primitives::Height;
use std::collections::HashMap;

/// Classification of tip+1 `unexpected previous header` (BadPrev).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BadPrevClass {
    /// Wire prev is not a known header — soft re-get only.
    CorruptWire { wire_prev: BlockHash },
    /// Wire prev is a known header that is not the current tip (competing path).
    CompetingPath {
        /// Parent of the rejected tip+1 body (winning sibling / branch tip).
        winning_prev: BlockHash,
        /// Current best tip (losing fork when this is the mainnet class stall).
        losing_tip: BlockHash,
    },
}

/// Classify a BadPrev / unexpected-previous reject at confirm tip+1.
///
/// `wire_prev` is the previous-block hash from the rejected block header.
/// `tip_hash` is the current best tip hash.
pub fn classify_bad_prev(
    hub: &ChainHub,
    wire_prev: BlockHash,
    tip_hash: BlockHash,
) -> BadPrevClass {
    if wire_prev == tip_hash {
        // Same as tip — not a competing reorg signal (should not be BadPrev).
        return BadPrevClass::CorruptWire { wire_prev };
    }
    let known = hub
        .query
        .get_header_by_hash(&wire_prev.to_byte_array())
        .ok()
        .flatten()
        .is_some()
        || hub.has_block(&wire_prev);
    if known {
        BadPrevClass::CompetingPath {
            winning_prev: wire_prev,
            losing_tip: tip_hash,
        }
    } else {
        BadPrevClass::CorruptWire { wire_prev }
    }
}

/// Whether a confirm reject string is the soft BadPrev class.
pub fn is_bad_prev_err(err: &str) -> bool {
    err.contains("unexpected previous header") || err.contains("unexpected previous")
}

/// Awaiting a missing body (e.g. winning sibling) before apply can run.
#[derive(Debug, Clone)]
pub struct AwaitingBodies {
    /// Block we already hold (typically tip+1 on the winning path).
    pub held_tip: Block,
    /// Hashes still needed (e.g. winning sibling at tip height).
    pub need: Vec<BlockHash>,
}

/// Process-local reorg state for one IBD run.
///
/// Bodies for **side branches** are held by hash here: the body queue is
/// height-keyed first-wins, so a same-height competitor of the tip cannot
/// live in BQ while the tip path occupies that height.
#[derive(Debug, Default)]
pub struct IbdReorgState {
    pub invalid: InvalidHashSet,
    /// Side-branch / reorg-candidate bodies keyed by block hash.
    held_bodies: HashMap<BlockHash, Block>,
    /// Incomplete gather: need `need` hashes before applying `held_tip` path.
    awaiting: Option<AwaitingBodies>,
    /// Proactive exploration: hashes densify should pull (same-height winner +
    /// extensions) without waiting for BadPrev awaiting.
    explore_need: Vec<BlockHash>,
    /// Candidate tips on an exploration path (for proactive most-work apply).
    explore_tips: Vec<BlockHash>,
}

impl IbdReorgState {
    pub fn new() -> Self {
        Self::default()
    }

    /// Cap on held side bodies (DoS / process RAM). Sized for multi-hop BadPrev
    /// paths (mainnet-class LCA walks) without thrashing; still small enough
    /// that unit tests can exercise eviction without huge chains.
    pub(crate) const HELD_CAP: usize = 32;

    pub fn hold_body(&mut self, block: Block) {
        let h = block.block_hash();
        if self.held_bodies.len() >= Self::HELD_CAP && !self.held_bodies.contains_key(&h) {
            // Drop an arbitrary older entry (HashMap iter order is arbitrary).
            if let Some(k) = self.held_bodies.keys().next().copied() {
                self.held_bodies.remove(&k);
            }
        }
        self.held_bodies.insert(h, block);
        // Filled need → drop from explore densify list.
        self.explore_need.retain(|x| *x != h);
    }

    pub fn get_held(&self, hash: &BlockHash) -> Option<Block> {
        self.held_bodies.get(hash).cloned()
    }

    pub fn set_awaiting(&mut self, held_tip: Block, need: Vec<BlockHash>) {
        self.awaiting = Some(AwaitingBodies { held_tip, need });
    }

    pub fn clear_awaiting(&mut self) {
        self.awaiting = None;
    }

    pub fn awaiting(&self) -> Option<&AwaitingBodies> {
        self.awaiting.as_ref()
    }

    /// True if `hash` is the held tip+1 of an incomplete reorg gather (do not
    /// soft re-getdata / tip-hole race it — densify **mids** instead).
    pub fn is_awaiting_held_tip(&self, hash: &BlockHash) -> bool {
        self.awaiting
            .as_ref()
            .is_some_and(|a| a.held_tip.block_hash() == *hash)
    }

    /// Register hashes (and optional path tip) for exploration densify / apply.
    pub fn register_explore(
        &mut self,
        need: impl IntoIterator<Item = BlockHash>,
        tip: Option<BlockHash>,
    ) {
        for h in need {
            if !self.explore_need.contains(&h) {
                self.explore_need.push(h);
            }
        }
        if let Some(t) = tip {
            if !self.explore_tips.contains(&t) {
                self.explore_tips.push(t);
            }
        }
    }

    pub fn explore_tips(&self) -> &[BlockHash] {
        &self.explore_tips
    }

    /// Registered exploration densify hashes (same-height winner + extensions).
    pub fn explore_need_hashes(&self) -> &[BlockHash] {
        &self.explore_need
    }

    /// True while registered explore hashes still lack **held** bodies.
    ///
    /// Tip+1+ extensions live in BQ, not held — production apply uses
    /// load_reorg_body availability, not this flag. Kept for unit preconditions.
    #[cfg(test)]
    pub fn explore_need_pending(&self) -> bool {
        self.explore_need
            .iter()
            .any(|h| !self.held_bodies.contains_key(h))
    }

    pub fn clear_explore(&mut self) {
        self.explore_need.clear();
        self.explore_tips.clear();
    }

    /// Hashes densify/getdata should still pull for an incomplete reorg
    /// (awaiting gather **or** proactive exploration).
    pub fn need_getdata(&self) -> Vec<BlockHash> {
        let mut out: Vec<BlockHash> = self
            .awaiting
            .as_ref()
            .map(|a| {
                a.need
                    .iter()
                    .filter(|h| !self.held_bodies.contains_key(*h))
                    .copied()
                    .collect()
            })
            .unwrap_or_default();
        for h in &self.explore_need {
            if !self.held_bodies.contains_key(h) && !out.contains(h) {
                out.push(*h);
            }
        }
        out
    }
}

/// Build a `WorkCandidate` for a contiguous body path whose first block's prev
/// is on the best chain. Returns `None` if parent not on chain or path empty.
pub fn candidate_from_blocks(
    hub: &ChainHub,
    blocks: &[Block],
) -> Result<Option<WorkCandidate>, NetError> {
    if blocks.is_empty() {
        return Ok(None);
    }
    for w in blocks.windows(2) {
        if w[1].header.prev_blockhash != w[0].block_hash() {
            return Err(NetError::Protocol("branch not linked"));
        }
    }
    let fork_prev = blocks[0].header.prev_blockhash;
    let Some(lca_h) = hub
        .query
        .height_of_hash(&fork_prev.to_byte_array())
        .map_err(|e| NetError::Consensus(e.to_string()))?
    else {
        return Ok(None);
    };
    let path_work = sum_work(blocks.iter().map(|b| b.header.work()));
    let apply_path: Vec<[u8; 32]> = blocks
        .iter()
        .map(|b| b.block_hash().to_byte_array())
        .collect();
    let tip = *apply_path.last().unwrap();
    Ok(Some(WorkCandidate {
        tip,
        apply_path,
        path_work,
        lca_hash: fork_prev.to_byte_array(),
        lca_height: lca_h.0,
    }))
}

/// Rank candidates vs current tip path work from a shared LCA; skip invalid.
pub fn rank_candidates(
    hub: &ChainHub,
    candidates: &[WorkCandidate],
    invalid: &InvalidHashSet,
) -> Result<SelectOutcome, NetError> {
    if candidates.is_empty() {
        return Ok(SelectOutcome::IgnoreWeaker);
    }
    // Use the first candidate's LCA for best-path work (callers group by LCA).
    let lca_h = candidates[0].lca_height;
    let tip_h = hub.tip_height().unwrap_or(0);
    let mut our = Vec::new();
    if tip_h > lca_h {
        for h in (lca_h + 1)..=tip_h {
            let hdr = hub
                .query
                .wire_header_at_height(Height(h))
                .map_err(|e| NetError::Consensus(e.to_string()))?;
            our.push(hdr.work());
        }
    }
    let best_work = sum_work(our.into_iter());
    Ok(select_most_work(best_work, candidates, &|h| {
        invalid.contains(h)
    }))
}

/// Apply a fully gathered branch via `accept_branch`. On success returns new tip
/// height. On connect failure marks the failing path invalid and restores tip
/// (via `accept_branch` contract).
pub fn apply_reorg_branch(
    hub: &ChainHub,
    blocks: &[Block],
    reorg: &mut IbdReorgState,
) -> Result<AcceptOutcome, NetError> {
    // Layer-1 gate before mutating tip (same rule as accept_branch work check).
    if !candidate_header_work_better(hub, blocks)? {
        return Ok(AcceptOutcome::IgnoredWeaker);
    }
    match hub.accept_branch(blocks) {
        Ok(o @ AcceptOutcome::Accepted { height }) => {
            info!(
                "ibd: most-work reorg accepted tip_h={height} blocks={}",
                blocks.len()
            );
            Ok(o)
        }
        Ok(o) => Ok(o),
        Err(e) => {
            // Mark path invalid so we do not thrash (tip restore is accept_branch).
            for b in blocks {
                reorg.invalid.mark(b.block_hash().to_byte_array());
            }
            warn!("ibd: most-work reorg connect failed (path marked invalid): {e}");
            Err(e)
        }
    }
}

/// Try candidates in most-work order: first successful apply wins; failed paths
/// are invalid-marked and **re-ranked** so a remaining valid heavier N can win.
pub fn try_apply_best_candidate(
    hub: &ChainHub,
    bodies: &HashMap<BlockHash, Block>,
    candidate_tips: &[BlockHash],
    reorg: &mut IbdReorgState,
) -> Result<Option<AcceptOutcome>, NetError> {
    let mut built: Vec<(WorkCandidate, Vec<Block>)> = Vec::new();
    for &tip in candidate_tips {
        if reorg.invalid.contains(tip.to_byte_array()) {
            continue;
        }
        let Some(blocks) = gather_path_to_best_parent(hub, bodies, tip) else {
            continue;
        };
        if let Some(c) = candidate_from_blocks(hub, &blocks)? {
            if !c.apply_path.iter().any(|h| reorg.invalid.contains(*h)) {
                built.push((c, blocks));
            }
        }
    }
    if built.is_empty() {
        return Ok(None);
    }
    // Bounded re-rank loop: each failed/ignored candidate is invalid-marked.
    for _ in 0..built.len().saturating_add(1) {
        let cands: Vec<WorkCandidate> = built
            .iter()
            .filter(|(c, _)| {
                !reorg.invalid.contains(c.tip)
                    && !c.apply_path.iter().any(|h| reorg.invalid.contains(*h))
            })
            .map(|(c, _)| c.clone())
            .collect();
        if cands.is_empty() {
            return Ok(None);
        }
        match rank_candidates(hub, &cands, &reorg.invalid)? {
            SelectOutcome::IgnoreWeaker => return Ok(None),
            SelectOutcome::Switch { candidate_tip, .. } => {
                let Some((_, blocks)) = built.iter().find(|(c, _)| c.tip == candidate_tip) else {
                    reorg.invalid.mark(candidate_tip);
                    continue;
                };
                match apply_reorg_branch(hub, blocks, reorg) {
                    Ok(o @ AcceptOutcome::Accepted { .. }) => return Ok(Some(o)),
                    Ok(_) => {
                        reorg.invalid.mark(candidate_tip);
                        // re-rank remaining
                    }
                    Err(_) => {
                        // apply_reorg_branch already marked the path invalid
                    }
                }
            }
        }
    }
    Ok(None)
}

/// Header hashes from `tip` back to (not including) a best-chain / confirmed
/// ancestor, **oldest-first**. Used so BadPrev densify requests every mid-path
/// body to the LCA (mainnet: d1e0 + 02022e + tip+1, not wire_prev alone).
///
/// Stops on [`ChainHub::has_block`] only — do not call `height_of_hash` for side
/// headers (orphan full confirmed-table scan; mainnet CPU spin on BadPrev).
pub fn header_hashes_to_best_ancestor(
    hub: &ChainHub,
    tip: BlockHash,
) -> Result<Vec<BlockHash>, NetError> {
    use bitcoin::hashes::Hash as _;
    let mut rev = Vec::new();
    let mut cur = tip;
    for _ in 0..10_000 {
        if hub.has_block(&cur) {
            break;
        }
        rev.push(cur);
        let Some((_fk, rec)) = hub
            .query
            .get_header_by_hash(&cur.to_byte_array())
            .map_err(|e| NetError::Consensus(e.to_string()))?
        else {
            break;
        };
        let Some(pfk) = rec.prev_fk.get() else {
            break;
        };
        let parent = hub
            .query
            .get_header(rbitcoin_primitives::Fk(pfk))
            .map_err(|e| NetError::Consensus(e.to_string()))?;
        cur = BlockHash::from_byte_array(parent.hash);
    }
    rev.reverse();
    Ok(rev)
}

/// Walk from `tip` via pending bodies until parent is on best chain.
///
/// Parent-on-chain uses **`has_block` only** — never `height_of_hash` on side
/// headers (that full-scans confirmed[] for orphans and pegged one core on
/// mainnet BadPrev/explore gather).
fn gather_path_to_best_parent(
    hub: &ChainHub,
    bodies: &HashMap<BlockHash, Block>,
    tip: BlockHash,
) -> Option<Vec<Block>> {
    let mut rev = Vec::new();
    let mut cur = tip;
    for _ in 0..10_000 {
        if hub.has_block(&cur) {
            return None;
        }
        let b = bodies.get(&cur)?;
        let prev = b.header.prev_blockhash;
        rev.push(b.clone());
        if hub.has_block(&prev) || prev.to_byte_array() == [0u8; 32] {
            rev.reverse();
            return Some(rev);
        }
        cur = prev;
    }
    None
}

fn parent_hash_of(hub: &ChainHub, hash: BlockHash) -> Result<Option<BlockHash>, NetError> {
    let Some((_, rec)) = hub
        .query
        .get_header_by_hash(&hash.to_byte_array())
        .map_err(|e| NetError::Consensus(e.to_string()))?
    else {
        return Ok(None);
    };
    if rec.prev_fk.is_null() {
        return Ok(Some(BlockHash::from_byte_array([0u8; 32])));
    }
    let parent = hub
        .query
        .get_header(rec.prev_fk)
        .map_err(|e| NetError::Consensus(e.to_string()))?;
    Ok(Some(BlockHash::from_byte_array(parent.hash)))
}

fn header_work_of(hub: &ChainHub, hash: BlockHash) -> Result<Option<bitcoin::Work>, NetError> {
    let Some((_, rec)) = hub
        .query
        .get_header_by_hash(&hash.to_byte_array())
        .map_err(|e| NetError::Consensus(e.to_string()))?
    else {
        return Ok(None);
    };
    Ok(Some(
        Target::from_compact(CompactTarget::from_consensus(rec.bits)).to_work(),
    ))
}

fn our_work_from_lca(hub: &ChainHub, lca_height: u32) -> Result<bitcoin::Work, NetError> {
    let tip_h = hub.tip_height().unwrap_or(0);
    let mut our = Vec::new();
    if tip_h > lca_height {
        for h in (lca_height + 1)..=tip_h {
            let hdr = hub
                .query
                .wire_header_at_height(Height(h))
                .map_err(|e| NetError::Consensus(e.to_string()))?;
            our.push(hdr.work());
        }
    }
    Ok(sum_work(our.into_iter()))
}

/// Index of the last hash in the shortest prefix of `path` (oldest-first)
/// whose header work strictly beats our tip from the same LCA.
pub fn shortest_heavier_header_prefix(
    hub: &ChainHub,
    path: &[BlockHash],
) -> Result<Option<usize>, NetError> {
    if path.is_empty() {
        return Ok(None);
    }
    let Some(parent) = parent_hash_of(hub, path[0])? else {
        return Ok(None);
    };
    let lca_h = hub
        .query
        .height_of_hash(&parent.to_byte_array())
        .map_err(|e| NetError::Consensus(e.to_string()))?
        .map(|h| h.0)
        .unwrap_or(0);
    let ours = our_work_from_lca(hub, lca_h)?;
    let mut acc = bitcoin::Work::from_be_bytes([0u8; 32]);
    for (i, h) in path.iter().enumerate() {
        let Some(w) = header_work_of(hub, *h)? else {
            return Ok(None);
        };
        acc = acc + w;
        if work_better(acc, ours) {
            return Ok(Some(i));
        }
    }
    Ok(None)
}

/// Unconfirmed hashes from after the best-chain LCA to `candidate` (oldest-first)
/// when that header path does not connect to the current tip and is strictly
/// heavier than our tip from the same LCA.
///
/// Callers getdata these **connecting** hashes instead of waiting for a child
/// of the losing tip (BIP110-class: majority 961632/33 while tip is the
/// 2-block minority).
pub fn connecting_hashes_heavier_disconnected(
    hub: &ChainHub,
    candidate: BlockHash,
) -> Result<Option<Vec<BlockHash>>, NetError> {
    let Some(tip) = hub.tip_hash() else {
        return Ok(None);
    };
    if candidate == tip || hub.has_block(&candidate) {
        return Ok(None);
    }
    let Some(prev) = parent_hash_of(hub, candidate)? else {
        return Ok(None);
    };
    if prev == tip {
        // Normal tip+1 extension — not a disconnected most-work chain.
        return Ok(None);
    }
    let path = header_hashes_to_best_ancestor(hub, candidate)?;
    if path.is_empty() {
        return Ok(None);
    }
    // Did not reach a confirmed ancestor: still search the unknown join.
    let Some(join_parent) = parent_hash_of(hub, path[0])? else {
        return Ok(Some(path));
    };
    if !hub.has_block(&join_parent) && join_parent.to_byte_array() != [0u8; 32] {
        return Ok(Some(path));
    }
    if shortest_heavier_header_prefix(hub, &path)?.is_none() {
        return Ok(None);
    }
    Ok(Some(path))
}

/// Register a connecting-hash search for a heavier header path that does not
/// meet the current tip. Explore tip is the **shortest** prefix that beats
/// current tip work — not the header horizon.
pub fn note_disconnected_heavier(
    reorg: &mut IbdReorgState,
    hub: &ChainHub,
    candidate: BlockHash,
) -> Result<bool, NetError> {
    let Some(path) = connecting_hashes_heavier_disconnected(hub, candidate)? else {
        return Ok(false);
    };
    let tip_idx = shortest_heavier_header_prefix(hub, &path)?.unwrap_or(path.len() - 1);
    let end = (tip_idx + 1).min(path.len()).min(IbdReorgState::HELD_CAP);
    if end == 0 {
        return Ok(false);
    }
    let prefix = &path[..end];
    let explore_tip = prefix[prefix.len() - 1];
    reorg.register_explore(prefix.iter().copied(), Some(explore_tip));
    info!(
        "ibd: heavier chain does not connect at tip — search {} connecting block(s) to {explore_tip} (candidate {candidate})",
        prefix.len()
    );
    Ok(true)
}

/// Scan work-path candidates (tip+1 and the far header) for a heavier
/// disconnected fork and register connecting getdata.
pub fn consider_disconnected_heavier(
    st: &mut super::state::IbdWorkState,
    hub: &ChainHub,
) -> Result<bool, NetError> {
    // Already searching connecting hashes — do not re-walk a long header path.
    if !st.reorg.explore_need_hashes().is_empty() {
        return Ok(false);
    }
    let tip_h = hub.tip_height().unwrap_or(0);
    let mut cands = Vec::new();
    if let Some(&h) = st.height_to_hash.get(&tip_h.saturating_add(1)) {
        cands.push(h);
    }
    if let Some(&h) = st.height_to_hash.get(&st.max_ordered_height) {
        if !cands.contains(&h) {
            cands.push(h);
        }
    }
    let mut any = false;
    for h in cands {
        if note_disconnected_heavier(&mut st.reorg, hub, h)? {
            any = true;
            break;
        }
    }
    Ok(any)
}

/// True if candidate tip work (header) is strictly better than our tip path
/// from the same LCA (proactive headers-driven trigger).
pub fn candidate_header_work_better(hub: &ChainHub, blocks: &[Block]) -> Result<bool, NetError> {
    let Some(c) = candidate_from_blocks(hub, blocks)? else {
        return Ok(false);
    };
    let tip_h = hub.tip_height().unwrap_or(0);
    let mut our = Vec::new();
    if tip_h > c.lca_height {
        for h in (c.lca_height + 1)..=tip_h {
            let hdr = hub
                .query
                .wire_header_at_height(Height(h))
                .map_err(|e| NetError::Consensus(e.to_string()))?;
            our.push(hdr.work());
        }
    }
    Ok(work_better(c.path_work, sum_work(our.into_iter())))
}

#[cfg(test)]
mod tests {
    use super::*;
    use bitcoin::absolute::LockTime;
    use bitcoin::block::{Header, Version};
    use bitcoin::script::ScriptBuf;
    use bitcoin::transaction::Version as TxVersion;
    use bitcoin::{
        Amount, CompactTarget, OutPoint, Sequence, Target, Transaction, TxIn, TxOut, Witness,
    };
    use rbitcoin_consensus::{ChainParams, Milestone};
    use rbitcoin_query::Query;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn tmp_hub() -> (std::path::PathBuf, ChainHub) {
        if std::env::var_os("RBITCOIN_HEAD_SCALE").is_none() {
            std::env::set_var("RBITCOIN_HEAD_SCALE", "tiny");
        }
        let dir = std::env::temp_dir().join(format!(
            "rbitcoin-ibd-reorg-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let _ = std::fs::create_dir_all(&dir);
        let q = Query::open_or_create(dir.join("store")).expect("query");
        let hub = ChainHub::new(q, ChainParams::regtest(), Milestone::NONE);
        (dir, hub)
    }

    fn coinbase(height: u32) -> Transaction {
        let mut ss = if height == 0 {
            vec![0x00]
        } else {
            rbitcoin_consensus::bip34_height_script(height)
        };
        while ss.len() < 2 {
            ss.push(0x00);
        }
        Transaction {
            version: TxVersion::ONE,
            lock_time: LockTime::ZERO,
            input: vec![TxIn {
                previous_output: OutPoint::null(),
                script_sig: ScriptBuf::from_bytes(ss),
                sequence: Sequence::MAX,
                witness: Witness::new(),
            }],
            output: vec![TxOut {
                value: Amount::from_sat(50_0000_0000),
                script_pubkey: ScriptBuf::from_bytes(vec![0x51]),
            }],
        }
    }

    fn mine(prev: BlockHash, time: u32, height: u32) -> Block {
        let bits = CompactTarget::from_consensus(0x207f_ffff);
        let header = Header {
            version: Version::ONE,
            prev_blockhash: prev,
            merkle_root: bitcoin::TxMerkleNode::from_byte_array([0u8; 32]),
            time,
            bits,
            nonce: 0,
        };
        let mut block = Block {
            header,
            txdata: vec![coinbase(height)],
        };
        block.header.merkle_root = block.compute_merkle_root().unwrap();
        let target = Target::from_compact(bits);
        for nonce in 0..u32::MAX {
            block.header.nonce = nonce;
            if block.header.validate_pow(target).is_ok() {
                break;
            }
        }
        block
    }

    fn mine_extra(prev: BlockHash, time: u32, height: u32, extra: Vec<Transaction>) -> Block {
        let bits = CompactTarget::from_consensus(0x207f_ffff);
        let header = Header {
            version: Version::ONE,
            prev_blockhash: prev,
            merkle_root: bitcoin::TxMerkleNode::from_byte_array([0u8; 32]),
            time,
            bits,
            nonce: 0,
        };
        let mut txdata = vec![coinbase(height)];
        txdata.extend(extra);
        let mut block = Block { header, txdata };
        block.header.merkle_root = block.compute_merkle_root().unwrap();
        let target = Target::from_compact(bits);
        for nonce in 0..u32::MAX {
            block.header.nonce = nonce;
            if block.header.validate_pow(target).is_ok() {
                break;
            }
        }
        block
    }

    /// Test helper: win-at-fork + optional extension via shipped apply path.
    fn apply_sibling_winning_path(
        hub: &ChainHub,
        winning_at_fork: Block,
        extension: &[Block],
        reorg: &mut IbdReorgState,
    ) -> Result<AcceptOutcome, NetError> {
        let mut bodies = HashMap::new();
        let tip = extension
            .last()
            .map(|b| b.block_hash())
            .unwrap_or_else(|| winning_at_fork.block_hash());
        bodies.insert(winning_at_fork.block_hash(), winning_at_fork);
        for b in extension {
            bodies.insert(b.block_hash(), b.clone());
        }
        match try_apply_best_candidate(hub, &bodies, &[tip], reorg)? {
            Some(o) => Ok(o),
            None => Ok(AcceptOutcome::IgnoredWeaker),
        }
    }

    #[test]
    fn classify_corrupt_vs_competing() {
        let (dir, hub) = tmp_hub();
        hub.ensure_genesis().unwrap();
        let gen = hub.tip_hash().unwrap();
        let lose = mine(gen, 1_500_000_100, 1);
        let win = {
            let mut b = mine(gen, 1_500_000_101, 1);
            if b.block_hash() == lose.block_hash() {
                let target = Target::from_compact(b.header.bits);
                for nonce in 0..u32::MAX {
                    b.header.nonce = nonce;
                    if b.header.validate_pow(target).is_ok() && b.block_hash() != lose.block_hash()
                    {
                        break;
                    }
                }
            }
            b
        };
        hub.accept_block(lose.clone()).unwrap();
        // Winning sibling header known but not tip.
        hub.ensure_header(&win.header).unwrap();
        let tip = hub.tip_hash().unwrap();
        assert_eq!(tip, lose.block_hash());

        match classify_bad_prev(&hub, win.block_hash(), tip) {
            BadPrevClass::CompetingPath {
                winning_prev,
                losing_tip,
            } => {
                assert_eq!(winning_prev, win.block_hash());
                assert_eq!(losing_tip, lose.block_hash());
            }
            other => panic!("expected CompetingPath, got {other:?}"),
        }
        let unknown = BlockHash::from_byte_array([0xde; 32]);
        match classify_bad_prev(&hub, unknown, tip) {
            BadPrevClass::CorruptWire { wire_prev } => assert_eq!(wire_prev, unknown),
            other => panic!("expected CorruptWire, got {other:?}"),
        }
        assert!(is_bad_prev_err("consensus: unexpected previous header"));
        assert!(!is_bad_prev_err("script verification failed"));
        // Same-as-tip wire prev → CorruptWire class (not CompetingPath).
        match classify_bad_prev(&hub, tip, tip) {
            BadPrevClass::CorruptWire { wire_prev } => assert_eq!(wire_prev, tip),
            other => panic!("expected CorruptWire for tip==prev, got {other:?}"),
        }
        // Unlinked branch → protocol error from candidate_from_blocks.
        let a = mine(gen, 1_500_000_300, 1);
        let b = mine(gen, 1_500_000_400, 1); // not child of a
        assert!(candidate_from_blocks(&hub, &[a, b]).is_err());
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn header_hashes_to_best_ancestor_walks_mid_path() {
        let (dir, hub) = tmp_hub();
        hub.ensure_genesis().unwrap();
        let gen = hub.tip_hash().unwrap();
        let l1 = mine(gen, 1_500_060_100, 1);
        hub.accept_block(l1.clone()).unwrap();
        // Side path not confirmed: w1 → w2
        let mut w1 = mine(gen, 1_500_060_101, 1);
        if w1.block_hash() == l1.block_hash() {
            let target = Target::from_compact(w1.header.bits);
            for nonce in 0..u32::MAX {
                w1.header.nonce = nonce;
                if w1.header.validate_pow(target).is_ok() && w1.block_hash() != l1.block_hash() {
                    break;
                }
            }
        }
        hub.ensure_header(&w1.header).unwrap();
        let w2 = mine(w1.block_hash(), 1_500_060_200, 2);
        hub.ensure_header(&w2.header).unwrap();
        let path = header_hashes_to_best_ancestor(&hub, w2.block_hash()).unwrap();
        assert_eq!(
            path,
            vec![w1.block_hash(), w2.block_hash()],
            "oldest-first mid path to LCA"
        );
        // Already on best chain → empty.
        assert!(header_hashes_to_best_ancestor(&hub, l1.block_hash())
            .unwrap()
            .is_empty());
        let _ = std::fs::remove_dir_all(dir);
    }

    /// Mainnet-class sibling: tip on loser; reorg onto win + extension.
    #[test]
    fn sibling_fork_reorg_onto_winning_path() {
        let (dir, hub) = tmp_hub();
        hub.ensure_genesis().unwrap();
        let gen = hub.tip_hash().unwrap();
        let lose = mine(gen, 1_500_001_000, 1);
        let mut win = mine(gen, 1_500_001_001, 1);
        if win.block_hash() == lose.block_hash() {
            let target = Target::from_compact(win.header.bits);
            for nonce in 0..u32::MAX {
                win.header.nonce = nonce;
                if win.header.validate_pow(target).is_ok() && win.block_hash() != lose.block_hash()
                {
                    break;
                }
            }
        }
        hub.accept_block(lose.clone()).unwrap();
        assert_eq!(hub.tip_hash().unwrap(), lose.block_hash());

        // Winning path: win @1 + ext @2 (more work than single lose tip).
        let ext = mine(win.block_hash(), 1_500_001_100, 2);
        let mut reorg = IbdReorgState::new();
        let out =
            apply_sibling_winning_path(&hub, win.clone(), &[ext.clone()], &mut reorg).unwrap();
        assert!(matches!(out, AcceptOutcome::Accepted { height: 2 }));
        assert_eq!(hub.tip_hash().unwrap(), ext.block_hash());
        assert_eq!(hub.tip_height(), Some(2));
        let _ = std::fs::remove_dir_all(dir);
    }

    /// Exploration gather: tip on loser, held bodies for win+ext, apply via
    /// try_apply_best_candidate without a BadPrev reject event.
    #[test]
    fn exploration_bodies_reorg_without_badprev() {
        let (dir, hub) = tmp_hub();
        hub.ensure_genesis().unwrap();
        let gen = hub.tip_hash().unwrap();
        let lose = mine(gen, 1_500_003_000, 1);
        let mut win = mine(gen, 1_500_003_001, 1);
        if win.block_hash() == lose.block_hash() {
            let target = Target::from_compact(win.header.bits);
            for nonce in 0..u32::MAX {
                win.header.nonce = nonce;
                if win.header.validate_pow(target).is_ok() && win.block_hash() != lose.block_hash()
                {
                    break;
                }
            }
        }
        hub.accept_block(lose.clone()).unwrap();
        hub.ensure_header(&win.header).unwrap();
        let ext = mine(win.block_hash(), 1_500_003_100, 2);
        hub.ensure_header(&ext.header).unwrap();

        let mut bodies = HashMap::new();
        bodies.insert(win.block_hash(), win.clone());
        bodies.insert(ext.block_hash(), ext.clone());
        let mut reorg = IbdReorgState::new();
        reorg.register_explore([win.block_hash(), ext.block_hash()], Some(ext.block_hash()));
        // Simulate densify filled held bodies.
        reorg.hold_body(win.clone());
        reorg.hold_body(ext.clone());
        assert!(!reorg.explore_need_pending());

        let out = try_apply_best_candidate(&hub, &bodies, &[ext.block_hash()], &mut reorg)
            .unwrap()
            .expect("exploration path must reorg tip");
        assert!(matches!(out, AcceptOutcome::Accepted { height: 2 }));
        assert_eq!(hub.tip_hash().unwrap(), ext.block_hash());
        let _ = std::fs::remove_dir_all(dir);
    }

    /// Heavier invalid M then valid heavier N (or stay L).
    #[test]
    fn invalid_heavy_then_alternate_or_stay() {
        let (dir, hub) = tmp_hub();
        hub.ensure_genesis().unwrap();
        let gen = hub.tip_hash().unwrap();
        // L: heights 1..=3
        let mut tip = gen;
        let t = 1_500_002_000u32;
        for h in 1..=3u32 {
            let b = mine(tip, t + h * 600, h);
            tip = b.block_hash();
            hub.accept_block(b).unwrap();
        }
        let l_tip = hub.tip_hash().unwrap();
        assert_eq!(hub.tip_height(), Some(3));

        // M: longer path from gen with bad block mid-path (4 blocks, last-1 bad).
        let mut m_blocks = Vec::new();
        let mut p = gen;
        for (i, h) in (1..=5u32).enumerate() {
            let b = if i == 2 {
                let bad_tx = Transaction {
                    version: TxVersion::ONE,
                    lock_time: LockTime::ZERO,
                    input: vec![TxIn {
                        previous_output: OutPoint {
                            txid: bitcoin::Txid::from_byte_array([0xee; 32]),
                            vout: 0,
                        },
                        script_sig: ScriptBuf::new(),
                        sequence: Sequence::MAX,
                        witness: Witness::new(),
                    }],
                    output: vec![TxOut {
                        value: Amount::from_sat(1),
                        script_pubkey: ScriptBuf::from_bytes(vec![0x51]),
                    }],
                };
                mine_extra(p, 1_500_010_000 + i as u32 * 600, h, vec![bad_tx])
            } else {
                mine(p, 1_500_010_000 + i as u32 * 600, h)
            };
            p = b.block_hash();
            m_blocks.push(b);
        }
        let mut reorg = IbdReorgState::new();
        let err = apply_reorg_branch(&hub, &m_blocks, &mut reorg).unwrap_err();
        assert!(matches!(err, NetError::Consensus(_)));
        assert_eq!(
            hub.tip_hash().unwrap(),
            l_tip,
            "tip stays L after invalid M"
        );
        assert!(reorg
            .invalid
            .contains(m_blocks[0].block_hash().to_byte_array()));

        // N: valid path length 4 from gen (work > L's 3).
        let mut n_blocks = Vec::new();
        let mut p = gen;
        for (i, h) in (1..=4u32).enumerate() {
            let b = mine(p, 1_500_020_000 + i as u32 * 600, h);
            p = b.block_hash();
            n_blocks.push(b);
        }
        assert!(candidate_header_work_better(&hub, &n_blocks).unwrap());
        let out = apply_reorg_branch(&hub, &n_blocks, &mut reorg).unwrap();
        assert!(matches!(out, AcceptOutcome::Accepted { height: 4 }));
        assert_eq!(hub.tip_hash().unwrap(), n_blocks[3].block_hash());

        // Re-apply M still marked → apply_reorg marks again; selector skip path:
        let mut bodies = HashMap::new();
        for b in &m_blocks {
            bodies.insert(b.block_hash(), b.clone());
        }
        for b in &n_blocks {
            bodies.insert(b.block_hash(), b.clone());
        }
        // M tip still invalid → try_apply should not move off N to M.
        let pre = hub.tip_hash().unwrap();
        let _ = try_apply_best_candidate(
            &hub,
            &bodies,
            &[m_blocks.last().unwrap().block_hash()],
            &mut reorg,
        )
        .unwrap();
        assert_eq!(hub.tip_hash().unwrap(), pre);

        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn rank_skips_invalid_and_weaker() {
        let (dir, hub) = tmp_hub();
        hub.ensure_genesis().unwrap();
        let gen = hub.tip_hash().unwrap();
        let b1 = mine(gen, 1_500_030_000, 1);
        hub.accept_block(b1.clone()).unwrap();
        // Candidate weaker: empty after tip (single block equal height side).
        let mut side = mine(gen, 1_500_030_001, 1);
        if side.block_hash() == b1.block_hash() {
            let target = Target::from_compact(side.header.bits);
            for nonce in 0..u32::MAX {
                side.header.nonce = nonce;
                if side.header.validate_pow(target).is_ok() && side.block_hash() != b1.block_hash()
                {
                    break;
                }
            }
        }
        let c = candidate_from_blocks(&hub, &[side.clone()])
            .unwrap()
            .unwrap();
        let inv = InvalidHashSet::new();
        let out = rank_candidates(&hub, &[c], &inv).unwrap();
        // Equal-work sibling at same height → not strictly better.
        assert_eq!(out, SelectOutcome::IgnoreWeaker);
        assert_eq!(
            rank_candidates(&hub, &[], &inv).unwrap(),
            SelectOutcome::IgnoreWeaker
        );
        assert!(candidate_from_blocks(&hub, &[]).unwrap().is_none());
        let _ = std::fs::remove_dir_all(dir);
    }

    /// Single try_apply call: heavy invalid M is marked then remaining valid N wins.
    #[test]
    fn try_apply_reranks_after_invalid_heavy_in_one_call() {
        let (dir, hub) = tmp_hub();
        hub.ensure_genesis().unwrap();
        let gen = hub.tip_hash().unwrap();
        // L tip height 2.
        let mut tip = gen;
        for h in 1..=2u32 {
            let b = mine(tip, 1_500_050_000 + h * 600, h);
            tip = b.block_hash();
            hub.accept_block(b).unwrap();
        }
        let l_tip = hub.tip_hash().unwrap();

        // M: length 4 with bad mid block (heavier headers, invalid).
        let mut m_blocks = Vec::new();
        let mut p = gen;
        for (i, h) in (1..=4u32).enumerate() {
            let b = if i == 1 {
                let bad_tx = Transaction {
                    version: TxVersion::ONE,
                    lock_time: LockTime::ZERO,
                    input: vec![TxIn {
                        previous_output: OutPoint {
                            txid: bitcoin::Txid::from_byte_array([0xee; 32]),
                            vout: 0,
                        },
                        script_sig: ScriptBuf::new(),
                        sequence: Sequence::MAX,
                        witness: Witness::new(),
                    }],
                    output: vec![TxOut {
                        value: Amount::from_sat(1),
                        script_pubkey: ScriptBuf::from_bytes(vec![0x51]),
                    }],
                };
                mine_extra(p, 1_500_051_000 + i as u32 * 600, h, vec![bad_tx])
            } else {
                mine(p, 1_500_051_000 + i as u32 * 600, h)
            };
            p = b.block_hash();
            m_blocks.push(b);
        }
        // N: valid length 3 (heavier than L, lighter than M headers).
        let mut n_blocks = Vec::new();
        let mut p = gen;
        for (i, h) in (1..=3u32).enumerate() {
            let b = mine(p, 1_500_052_000 + i as u32 * 600, h);
            p = b.block_hash();
            n_blocks.push(b);
        }
        let mut bodies = HashMap::new();
        for b in m_blocks.iter().chain(n_blocks.iter()) {
            bodies.insert(b.block_hash(), b.clone());
        }
        let mut reorg = IbdReorgState::new();
        let out = try_apply_best_candidate(
            &hub,
            &bodies,
            &[
                m_blocks.last().unwrap().block_hash(),
                n_blocks.last().unwrap().block_hash(),
            ],
            &mut reorg,
        )
        .unwrap()
        .expect("N must win after M invalid in one try_apply call");
        assert!(matches!(out, AcceptOutcome::Accepted { height: 3 }));
        assert_eq!(hub.tip_hash().unwrap(), n_blocks[2].block_hash());
        assert_ne!(hub.tip_hash().unwrap(), l_tip);
        assert!(reorg
            .invalid
            .contains(m_blocks[0].block_hash().to_byte_array()));
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn reorg_state_held_awaiting_need_getdata() {
        let mut st = IbdReorgState::new();
        assert!(st.need_getdata().is_empty());
        assert!(st.awaiting().is_none());
        // Synthetic blocks for hold (hash distinct via tip identity).
        let gen = BlockHash::from_byte_array([0x11; 32]);
        let bits = CompactTarget::from_consensus(0x207f_ffff);
        let held = mine(gen, 1_300_000_000, 1);
        let mut need_h = mine(gen, 1_300_000_001, 1);
        if need_h.block_hash() == held.block_hash() {
            let target = Target::from_compact(bits);
            for nonce in 0..u32::MAX {
                need_h.header.nonce = nonce;
                if need_h.header.validate_pow(target).is_ok()
                    && need_h.block_hash() != held.block_hash()
                {
                    break;
                }
            }
        }
        let need = need_h.block_hash();
        st.set_awaiting(held.clone(), vec![need]);
        assert_eq!(st.need_getdata(), vec![need]);
        st.hold_body(need_h);
        assert!(st.need_getdata().is_empty(), "held satisfies need");
        // Proactive exploration need merges into need_getdata.
        let explore = mine(gen, 1_300_000_050, 1);
        let eh = explore.block_hash();
        st.register_explore([eh], Some(eh));
        assert!(st.explore_need_pending());
        assert_eq!(st.need_getdata(), vec![eh]);
        st.hold_body(explore);
        assert!(!st.explore_need_pending());
        assert!(st.need_getdata().is_empty());
        assert!(st.get_held(&need).is_some());
        st.clear_awaiting();
        assert!(st.awaiting().is_none());
        assert!(st.need_getdata().is_empty());
        // Eviction path when over HELD_CAP (fresh map so only these keys count).
        let mut st_cap = IbdReorgState::new();
        let mut held_keys = Vec::new();
        let mut prev = gen;
        for i in 0u32..(IbdReorgState::HELD_CAP as u32 + 4) {
            let b = mine(prev, 1_300_001_000 + i, 1);
            prev = b.block_hash();
            held_keys.push(b.block_hash());
            st_cap.hold_body(b);
        }
        let still = held_keys
            .iter()
            .filter(|k| st_cap.get_held(k).is_some())
            .count();
        assert_eq!(
            still,
            IbdReorgState::HELD_CAP,
            "held map must stay exactly HELD_CAP after overflow inserts"
        );
        let _ = held;
    }

    /// Journey: try_apply_best_candidate success + invalid skip + empty.
    #[test]
    fn try_apply_selector_journey_success_invalid_skip_and_empty() {
        let (dir, hub) = tmp_hub();
        hub.ensure_genesis().unwrap();
        let gen = hub.tip_hash().unwrap();
        let lose = mine(gen, 1_500_040_000, 1);
        hub.accept_block(lose.clone()).unwrap();
        assert_eq!(hub.tip_height(), Some(1));

        let mut win = Vec::new();
        let mut p = gen;
        for (i, h) in (1..=3u32).enumerate() {
            let b = mine(p, 1_500_041_000 + i as u32 * 700, h);
            p = b.block_hash();
            win.push(b);
        }
        let mut bodies = HashMap::new();
        for b in &win {
            bodies.insert(b.block_hash(), b.clone());
        }
        bodies.insert(lose.block_hash(), lose.clone());

        let mut reorg = IbdReorgState::new();
        assert!(try_apply_best_candidate(&hub, &bodies, &[], &mut reorg)
            .unwrap()
            .is_none());
        let missing = BlockHash::from_byte_array([0xcd; 32]);
        assert!(
            try_apply_best_candidate(&hub, &bodies, &[missing], &mut reorg)
                .unwrap()
                .is_none()
        );

        reorg.invalid.mark(win[2].block_hash().to_byte_array());
        assert!(
            try_apply_best_candidate(&hub, &bodies, &[win[2].block_hash()], &mut reorg)
                .unwrap()
                .is_none()
        );

        reorg = IbdReorgState::new();
        let out = try_apply_best_candidate(&hub, &bodies, &[win[2].block_hash()], &mut reorg)
            .unwrap()
            .expect("must apply winning path");
        assert!(matches!(out, AcceptOutcome::Accepted { height: 3 }));
        assert_eq!(hub.tip_hash().unwrap(), win[2].block_hash());

        reorg.invalid.mark(win[0].block_hash().to_byte_array());
        let sib = apply_sibling_winning_path(&hub, win[0].clone(), &[], &mut reorg).unwrap();
        assert!(matches!(sib, AcceptOutcome::IgnoredWeaker));

        let mut reorg2 = IbdReorgState::new();
        let side = mine(gen, 1_500_042_000, 1);
        let weak = apply_reorg_branch(&hub, &[side], &mut reorg2).unwrap();
        assert!(matches!(weak, AcceptOutcome::IgnoredWeaker));

        let _ = std::fs::remove_dir_all(dir);
    }

    fn distinct_sib(mut b: Block, avoid: BlockHash) -> Block {
        if b.block_hash() == avoid {
            let target = Target::from_compact(b.header.bits);
            for nonce in 0..u32::MAX {
                b.header.nonce = nonce;
                if b.header.validate_pow(target).is_ok() && b.block_hash() != avoid {
                    break;
                }
            }
        }
        b
    }

    /// BIP110-class: tip on a 2-block loser; heavier winner headers do not
    /// connect at tip+1. Name the connecting hashes (not a dead-tip child).
    #[test]
    fn heavier_disconnected_path_names_connecting_hashes() {
        let (dir, hub) = tmp_hub();
        hub.ensure_genesis().unwrap();
        let gen = hub.tip_hash().unwrap();
        let l1 = mine(gen, 1_500_060_100, 1);
        hub.accept_block(l1.clone()).unwrap();
        let l2 = mine(l1.block_hash(), 1_500_060_200, 2);
        hub.accept_block(l2.clone()).unwrap();
        assert_eq!(hub.tip_hash().unwrap(), l2.block_hash());

        let w1 = distinct_sib(mine(gen, 1_500_060_101, 1), l1.block_hash());
        hub.ensure_header(&w1.header).unwrap();
        let w2 = mine(w1.block_hash(), 1_500_060_201, 2);
        hub.ensure_header(&w2.header).unwrap();
        let w3 = mine(w2.block_hash(), 1_500_060_301, 3);
        hub.ensure_header(&w3.header).unwrap();
        let w4 = mine(w3.block_hash(), 1_500_060_401, 4);
        hub.ensure_header(&w4.header).unwrap();

        // Connected loser child is not a disconnected heavier path.
        let l3 = mine(l2.block_hash(), 1_500_060_300, 3);
        hub.ensure_header(&l3.header).unwrap();
        assert!(
            connecting_hashes_heavier_disconnected(&hub, l3.block_hash())
                .unwrap()
                .is_none(),
            "child of current tip is a normal extension, not a connecting search"
        );

        let path = connecting_hashes_heavier_disconnected(&hub, w4.block_hash())
            .unwrap()
            .expect("heavier winner that does not connect at tip must name a path");
        assert_eq!(
            path,
            vec![
                w1.block_hash(),
                w2.block_hash(),
                w3.block_hash(),
                w4.block_hash()
            ],
            "path must be winner mids from LCA, not the loser tip+1"
        );
        assert!(!path.contains(&l3.block_hash()));
        let _ = std::fs::remove_dir_all(dir);
    }

    /// Register only the shortest prefix that beats tip work; apply it once
    /// those connecting bodies are held — no BadPrev, no full-horizon gather.
    #[test]
    fn note_disconnected_heavier_fetches_connecting_prefix_and_reorgs() {
        let (dir, hub) = tmp_hub();
        hub.ensure_genesis().unwrap();
        let gen = hub.tip_hash().unwrap();
        let l1 = mine(gen, 1_500_061_100, 1);
        hub.accept_block(l1.clone()).unwrap();
        let l2 = mine(l1.block_hash(), 1_500_061_200, 2);
        hub.accept_block(l2.clone()).unwrap();

        let w1 = distinct_sib(mine(gen, 1_500_061_101, 1), l1.block_hash());
        hub.ensure_header(&w1.header).unwrap();
        let w2 = mine(w1.block_hash(), 1_500_061_201, 2);
        hub.ensure_header(&w2.header).unwrap();
        let w3 = mine(w2.block_hash(), 1_500_061_301, 3);
        hub.ensure_header(&w3.header).unwrap();
        let w4 = mine(w3.block_hash(), 1_500_061_401, 4);
        hub.ensure_header(&w4.header).unwrap();
        let w5 = mine(w4.block_hash(), 1_500_061_501, 5);
        hub.ensure_header(&w5.header).unwrap();

        let mut reorg = IbdReorgState::new();
        assert!(
            note_disconnected_heavier(&mut reorg, &hub, w5.block_hash()).unwrap(),
            "must register a connecting search for the heavier disconnected path"
        );
        let need = reorg.need_getdata();
        assert!(
            need.contains(&w1.block_hash())
                && need.contains(&w2.block_hash())
                && need.contains(&w3.block_hash()),
            "must search for connecting mids; need={need:?}"
        );
        assert!(
            !need.contains(&w5.block_hash()),
            "must not wait to gather the whole heavier horizon; need={need:?}"
        );
        assert_eq!(
            reorg.explore_tips(),
            &[w3.block_hash()],
            "explore tip is the shortest prefix that beats loser work"
        );

        reorg.hold_body(w1.clone());
        reorg.hold_body(w2.clone());
        reorg.hold_body(w3.clone());
        let mut bodies = HashMap::new();
        bodies.insert(w1.block_hash(), w1.clone());
        bodies.insert(w2.block_hash(), w2.clone());
        bodies.insert(w3.block_hash(), w3.clone());
        let out = try_apply_best_candidate(&hub, &bodies, &[w3.block_hash()], &mut reorg)
            .unwrap()
            .expect("connecting prefix must reorg without BadPrev");
        assert!(matches!(out, AcceptOutcome::Accepted { height: 3 }));
        assert_eq!(hub.tip_hash().unwrap(), w3.block_hash());
        let _ = std::fs::remove_dir_all(dir);
    }

    /// Work-path tip+1 / far header (no resume seed) still registers the
    /// connecting prefix — the live IBD hook, not BadPrev.
    #[test]
    fn consider_disconnected_from_work_path_without_resume_seed() {
        let (dir, hub) = tmp_hub();
        hub.ensure_genesis().unwrap();
        let gen = hub.tip_hash().unwrap();
        let l1 = mine(gen, 1_500_062_100, 1);
        hub.accept_block(l1.clone()).unwrap();
        let l2 = mine(l1.block_hash(), 1_500_062_200, 2);
        hub.accept_block(l2.clone()).unwrap();
        let w1 = distinct_sib(mine(gen, 1_500_062_101, 1), l1.block_hash());
        hub.ensure_header(&w1.header).unwrap();
        let w2 = mine(w1.block_hash(), 1_500_062_201, 2);
        hub.ensure_header(&w2.header).unwrap();
        let w3 = mine(w2.block_hash(), 1_500_062_301, 3);
        hub.ensure_header(&w3.header).unwrap();
        let w4 = mine(w3.block_hash(), 1_500_062_401, 4);
        hub.ensure_header(&w4.header).unwrap();

        let mut st =
            super::super::state::IbdWorkState::new(Vec::new(), hub.tip_hash(), hub.tip_height());
        st.record_height(w3.block_hash(), 3);
        st.record_height(w4.block_hash(), 4);
        st.max_ordered_height = 4;
        assert!(consider_disconnected_heavier(&mut st, &hub).unwrap());
        let need = st.reorg.need_getdata();
        assert!(
            need.contains(&w1.block_hash()) && need.contains(&w2.block_hash()),
            "live consider must search connecting mids without resume seed; need={need:?}"
        );
        let _ = std::fs::remove_dir_all(dir);
    }
}
