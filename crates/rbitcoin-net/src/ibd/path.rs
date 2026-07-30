//! Ordered work-path seeding and header locator tips.

use super::state::IbdWorkState;
use super::MAX_ORDERED_HEADERS;
use crate::chain::ChainHub;
use bitcoin::hashes::Hash;
use bitcoin::BlockHash;
use rbitcoin_log::{info, warn};
use std::time::Instant;

pub(crate) fn seed_work_path_from_store(st: &mut IbdWorkState, hub: &ChainHub) {
    let Some(tip_hash) = hub.tip_hash() else {
        return;
    };
    let tip_h = hub.tip_height().unwrap_or(0);
    let t0 = Instant::now();
    let path = match hub.query.resume_work_path_after_tip(
        tip_hash.to_byte_array(),
        tip_h,
        MAX_ORDERED_HEADERS,
    ) {
        Ok(p) => p,
        Err(e) => {
            warn!("ibd: resume seed from store failed: {e}");
            return;
        }
    };
    if path.is_empty() {
        return;
    }
    let mut with_body = 0u32;
    let mut ready_prefix_to = tip_h;
    let mut ready_prefix = true;
    for e in &path {
        let hash = BlockHash::from_byte_array(e.hash);
        st.known_headers.insert(hash);
        st.record_height(hash, e.height);
        st.header_fks.insert(hash, e.header_fk);
        st.max_ordered_height = st.max_ordered_height.max(e.height);
        if st.ordered_set.insert(hash) {
            st.ordered.push_back(hash);
        }
        if e.has_body {
            // Class A on disk: remember for densify bookkeeping. Confirm still
            // needs body-queue wire (rehydrate or re-getdata) — not claim-ready alone.
            st.body.mark_archived(hash);
            with_body = with_body.saturating_add(1);
            if ready_prefix {
                ready_prefix_to = e.height;
            }
        } else {
            ready_prefix = false;
        }
    }
    st.max_ready_height = st.max_ready_height.max(ready_prefix_to);
    // Peers may still advertise a higher tip; keep header sync open.
    st.headers_done = false;
    info!(
        "ibd: resume seed ordered={} class_a_bodies={} ready_to={} (store walk {:?})",
        st.ordered.len(),
        with_body,
        ready_prefix_to,
        t0.elapsed()
    );
}

/// Highest hashes on the download path (newest first) for getheaders locators.
pub(crate) fn work_path_tips(st: &IbdWorkState) -> Vec<BlockHash> {
    let mut tips = Vec::with_capacity(8);
    // ordered is tip→far; the back is the highest known header on the path.
    for h in st.ordered.iter().rev().take(4) {
        if st.ordered_set.contains(h) {
            tips.push(*h);
        }
    }
    // Also sample by max height in hash_height if ordered is empty/ghosty.
    if tips.is_empty() {
        if let Some((&h, _)) = st
            .hash_height
            .iter()
            .max_by_key(|(_, &ht)| ht)
        {
            tips.push(h);
        }
    }
    tips
}

#[cfg(test)]
mod tests {
    use super::work_path_tips;
    use super::super::state::IbdWorkState;
    use bitcoin::hashes::Hash;
    use bitcoin::BlockHash;

    fn h(n: u8) -> BlockHash {
        let mut b = [0u8; 32];
        b[0] = n;
        BlockHash::from_byte_array(b)
    }

    #[test]
    fn work_path_tips_from_ordered_newest_first() {
        let mut st = IbdWorkState::new(Vec::new(), None, Some(10));
        // ordered is tip→far (front near tip); tips take from the back (highest).
        for n in 1u8..=6 {
            let hash = h(n);
            st.ordered.push_back(hash);
            st.ordered_set.insert(hash);
            st.record_height(hash, 10 + u32::from(n));
        }
        let tips = work_path_tips(&st);
        assert_eq!(tips.len(), 4);
        assert_eq!(tips[0], h(6));
        assert_eq!(tips[1], h(5));
        assert_eq!(tips[2], h(4));
        assert_eq!(tips[3], h(3));
    }

    #[test]
    fn work_path_tips_skips_ghosts_and_falls_back_to_hash_height() {
        let mut st = IbdWorkState::new(Vec::new(), None, Some(0));
        // Ghost: in deque but not ordered_set.
        st.ordered.push_back(h(1));
        st.ordered.push_back(h(2));
        // No live ordered members → fall back to max height in hash_height.
        st.record_height(h(9), 99);
        st.record_height(h(8), 50);
        let tips = work_path_tips(&st);
        assert_eq!(tips, vec![h(9)]);

        // Empty everything → empty tips.
        let empty = IbdWorkState::new(Vec::new(), None, None);
        assert!(work_path_tips(&empty).is_empty());
    }

    #[test]
    fn work_path_tips_respects_live_set_only() {
        let mut st = IbdWorkState::new(Vec::new(), None, Some(1));
        st.ordered.push_back(h(1));
        st.ordered.push_back(h(2));
        st.ordered.push_back(h(3));
        st.ordered_set.insert(h(1));
        st.ordered_set.insert(h(3)); // h(2) is a middle ghost
        let tips = work_path_tips(&st);
        // rev walk: 3 (live), 2 (ghost skip), 1 (live) — only set members.
        assert_eq!(tips, vec![h(3), h(1)]);
    }

    #[test]
    fn seed_work_path_from_empty_and_genesis_store() {
        use super::seed_work_path_from_store;
        use rbitcoin_consensus::{ChainParams, Milestone};
        use rbitcoin_query::Query;

        let dir = std::env::temp_dir().join(format!(
            "rbitcoin-path-seed-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let _ = std::fs::create_dir_all(&dir);
        let q = Query::open_or_create(dir.join("store")).unwrap();
        let hub = crate::chain::ChainHub::new(q, ChainParams::regtest(), Milestone::NONE);
        // Empty store: tip_hash is None → seed returns immediately.
        let mut st = IbdWorkState::new(Vec::new(), None, None);
        seed_work_path_from_store(&mut st, &hub);
        assert!(st.ordered.is_empty());

        hub.ensure_genesis().unwrap();
        let mut st2 = IbdWorkState::new(
            Vec::new(),
            hub.tip_hash(),
            hub.tip_height(),
        );
        seed_work_path_from_store(&mut st2, &hub);
        // Resume path after tip may be empty (no headers beyond tip).
        assert!(!st2.headers_done); // always left open for peer tip
        let _ = std::fs::remove_dir_all(dir);
    }

    /// Archive headers ahead of tip so resume seed walks a non-empty path
    /// (bodies + a body gap for the contiguous-prefix break).
    #[test]
    fn seed_work_path_archives_ahead_of_tip() {
        use super::seed_work_path_from_store;
        use bitcoin::absolute::LockTime;
        use bitcoin::block::{Header, Version};
        use bitcoin::script::ScriptBuf;
        use bitcoin::transaction::Version as TxVersion;
        use bitcoin::{
            Amount, Block, CompactTarget, OutPoint, Sequence, Target, Transaction, TxIn, TxOut,
            Witness,
        };
        use rbitcoin_consensus::{ChainParams, Milestone};
        use rbitcoin_query::Query;

        if std::env::var_os("RBITCOIN_HEAD_SCALE").is_none() {
            std::env::set_var("RBITCOIN_HEAD_SCALE", "tiny");
        }
        let dir = std::env::temp_dir().join(format!(
            "rbitcoin-path-seed-arch-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let _ = std::fs::create_dir_all(&dir);
        let q = Query::open_or_create(dir.join("store")).unwrap();
        let hub = crate::chain::ChainHub::new(q, ChainParams::regtest(), Milestone::NONE);
        hub.ensure_genesis().unwrap();
        let gen = hub.tip_hash().unwrap();

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

        // Tip stays at genesis; archive heights 1..=2, header-only height 3 (gap).
        let mut tip = gen;
        let time = 1_300_000_000u32;
        for h in 1u32..=2 {
            let b = mine(tip, time + h * 600, h);
            hub.ensure_header(&b.header).unwrap();
            hub.archive_block(h, b.clone()).unwrap();
            tip = b.block_hash();
        }
        let b3 = mine(tip, time + 3 * 600, 3);
        hub.ensure_header(&b3.header).unwrap();
        // no Class A body for h=3 → ready_prefix breaks

        let mut st = IbdWorkState::new(Vec::new(), hub.tip_hash(), hub.tip_height());
        seed_work_path_from_store(&mut st, &hub);
        assert!(
            st.ordered.len() >= 2,
            "resume should seed work path after tip, got {}",
            st.ordered.len()
        );
        assert!(st.max_ordered_height >= 2);
        assert!(st.max_ready_height >= 2); // contiguous claim-ready prefix
        assert!(!st.headers_done);
        // Duplicates on re-seed only insert once into ordered_set.
        let n = st.ordered.len();
        seed_work_path_from_store(&mut st, &hub);
        assert_eq!(st.ordered.len(), n);
        let _ = std::fs::remove_dir_all(dir);
    }
}

