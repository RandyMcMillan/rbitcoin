//! IBD most-work reorg: classify BadPrev, rank candidates, apply via `accept_branch`.
//!
//! See [`docs/design-ibd-most-work-reorg.md`]. Orchestration-thread only.

use crate::chain::{AcceptOutcome, ChainHub};
use crate::error::NetError;
use crate::most_work::{
    select_most_work, sum_work, work_better, InvalidHashSet, SelectOutcome, WorkCandidate,
};
use bitcoin::hashes::Hash;
use bitcoin::{Block, BlockHash};
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

/// Process-local invalid apply marks for this IBD run.
#[derive(Debug, Default)]
pub struct IbdReorgState {
    pub invalid: InvalidHashSet,
}

impl IbdReorgState {
    pub fn new() -> Self {
        Self::default()
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

/// Mainnet-class sibling reorg: tip on losing sibling; apply path starts at
/// winning sibling (same height) then extensions.
///
/// `winning_at_fork` is the block at the losing tip's height on the winning
/// chain; `extension` is optional tip+1… already gathered.
pub fn apply_sibling_winning_path(
    hub: &ChainHub,
    winning_at_fork: Block,
    extension: &[Block],
    reorg: &mut IbdReorgState,
) -> Result<AcceptOutcome, NetError> {
    let mut branch = Vec::with_capacity(1 + extension.len());
    branch.push(winning_at_fork);
    branch.extend(extension.iter().cloned());
    // Skip if any hash invalid-marked.
    if branch
        .iter()
        .any(|b| reorg.invalid.contains(b.block_hash().to_byte_array()))
    {
        return Ok(AcceptOutcome::IgnoredWeaker);
    }
    // Prefer selector+gather when bodies are complete (same as multi-candidate path).
    let mut bodies = HashMap::new();
    for b in &branch {
        bodies.insert(b.block_hash(), b.clone());
    }
    let tip = branch.last().unwrap().block_hash();
    match try_apply_best_candidate(hub, &bodies, &[tip], reorg)? {
        Some(o) => Ok(o),
        None => apply_reorg_branch(hub, &branch, reorg),
    }
}

/// Try candidates in most-work order: first successful apply wins; failed paths
/// are invalid-marked and skipped on subsequent ranks.
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
    let cands: Vec<WorkCandidate> = built.iter().map(|(c, _)| c.clone()).collect();
    match rank_candidates(hub, &cands, &reorg.invalid)? {
        SelectOutcome::IgnoreWeaker => Ok(None),
        SelectOutcome::Switch { candidate_tip, .. } => {
            let Some((_, blocks)) = built.iter().find(|(c, _)| c.tip == candidate_tip) else {
                reorg.invalid.mark(candidate_tip);
                return Ok(None);
            };
            match apply_reorg_branch(hub, blocks, reorg) {
                Ok(o @ AcceptOutcome::Accepted { .. }) => Ok(Some(o)),
                Ok(_) => {
                    reorg.invalid.mark(candidate_tip);
                    Ok(None)
                }
                Err(_) => Ok(None), // path marked invalid in apply_reorg_branch
            }
        }
    }
}

/// Walk from `tip` via pending bodies until parent is on best chain.
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
        if hub.has_block(&prev)
            || prev.to_byte_array() == [0u8; 32]
            || hub
                .query
                .height_of_hash(&prev.to_byte_array())
                .ok()
                .flatten()
                .is_some()
        {
            rev.reverse();
            return Some(rev);
        }
        cur = prev;
    }
    None
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
}
