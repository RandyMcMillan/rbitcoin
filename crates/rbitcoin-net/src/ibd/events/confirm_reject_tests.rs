//! Tests for super:: events helpers (peeled from events.rs).

use super::super::state::IbdWorkState;
use super::apply_confirm_reject;
use bitcoin::hashes::Hash;
use bitcoin::BlockHash;

fn h(n: u8) -> BlockHash {
    let mut b = [0u8; 32];
    b[0] = n;
    BlockHash::from_byte_array(b)
}

/// Soft re-get is wire-only (`unexpected previous header`). Internal
/// invariants permanent-blacklist. Zero-hash ignored. Mainnet regressions
/// documented in comments (125653 wire, 219562 denserels, 269050 seal).
#[test]
fn confirm_reject_blacklist_surface() {
    let mut st = IbdWorkState::new(Vec::new(), None, Some(100));
    let zero = BlockHash::from_byte_array([0u8; 32]);
    apply_confirm_reject(
        &mut st,
        101,
        zero,
        "consensus: prevout already spent on best chain",
        None,
        None,
    );
    assert!(!st.body.is_rejected(&zero));

    // Script fail → permanent.
    let mut st = IbdWorkState::new(Vec::new(), None, Some(50));
    let hash = h(7);
    st.body.mark_archived(hash);
    st.ordered.push_back(hash);
    st.ordered_set.insert(hash);
    apply_confirm_reject(
        &mut st,
        51,
        hash,
        "consensus: script verification failed: script false",
        None,
        None,
    );
    assert!(st.body.is_rejected(&hash));
    assert!(!st.ordered_set.contains(&hash));

    // Internal denserels invariant → permanent (fix pin layout, not soft).
    let mut st = IbdWorkState::new(Vec::new(), None, Some(219_561));
    let hash = h(0x5b);
    st.body.mark_archived(hash);
    st.ordered.push_back(hash);
    st.ordered_set.insert(hash);
    apply_confirm_reject(
        &mut st,
        219_562,
        hash,
        "consensus: store: corrupt record: invariant: spend annotate missing pin denserels/abs",
        None,
        None,
    );
    assert!(
        st.body.is_rejected(&hash),
        "denserels layout miss is permanent (fix pipeline, not soft-reget)"
    );

    // parent create_fk unresolved / fk mismatch: permanent (store or pipeline bug).
    let mut st = IbdWorkState::new(Vec::new(), None, Some(269_049));
    let hash = h(0x53);
    st.body.mark_archived(hash);
    st.ordered.push_back(hash);
    st.ordered_set.insert(hash);
    apply_confirm_reject(
        &mut st,
        269_050,
        hash,
        "consensus: store: corrupt record: archive: parent create_fk unresolved (contiguous batch required)",
        None,
        None,
    );
    assert!(
        st.body.is_rejected(&hash),
        "parent create_fk unresolved is permanent (fix pipeline, not soft-requeue)"
    );
    let mut st = IbdWorkState::new(Vec::new(), None, Some(961_467));
    let hash = h(0x68);
    st.body.mark_archived(hash);
    apply_confirm_reject(
        &mut st,
        961_468,
        hash,
        "consensus: store: corrupt record: tx put_full_batch fk mismatch (plan not committed in order)",
        None,
        None,
    );
    assert!(
        st.body.is_rejected(&hash),
        "fk mismatch is permanent (not tip-ahead soft requeue)"
    );

    // Merkle mismatch (corrupt Class A reconstruct) → soft re-get, not blacklist.
    // Drive clear_archived_body when a Query is present (production IBD path).
    let dir = std::env::temp_dir().join(format!(
        "rbitcoin-ev-merkle-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let _ = std::fs::create_dir_all(&dir);
    let q = rbitcoin_query::Query::open_or_create(dir.join("store")).unwrap();
    let hdr = rbitcoin_store::HeaderRecord {
        prev_fk: rbitcoin_primitives::Fk::NULL,
        version: 1,
        timestamp: 1,
        bits: 0x207fffff,
        nonce: 0xae,
        merkle_root: [0xae; 32],
        hash: h(0xae).to_byte_array(),
    };
    let hfk = q.put_header(&hdr).unwrap();
    // Associate a dummy Class A range so clear_body has something to drop.
    q.store()
        .header_txs
        .put_range(hfk, rbitcoin_primitives::Fk(1), 1)
        .unwrap();
    assert!(q.store().header_txs.has_body(hfk).unwrap());

    let mut st = IbdWorkState::new(Vec::new(), None, Some(938_453));
    let hash = h(0xae);
    st.body.mark_archived(hash);
    st.ordered.push_back(hash);
    st.ordered_set.insert(hash);
    apply_confirm_reject(
        &mut st,
        938_454,
        hash,
        "consensus: bad block: merkle root mismatch",
        Some(&q),
        None,
    );
    assert!(
        !st.body.is_rejected(&hash),
        "merkle mismatch must soft re-get (Class A body may be corrupt)"
    );
    assert!(
        !st.body.is_known_archived(&hash),
        "merkle mismatch should demote Class A known so densify re-gets"
    );
    assert!(
        !q.store().header_txs.has_body(hfk).unwrap(),
        "soft re-get must clear corrupt Class A association"
    );
    let _ = std::fs::remove_dir_all(&dir);

    // Wire-only soft: unexpected previous header → re-getdata, not blacklist.
    let mut st = IbdWorkState::new(Vec::new(), None, Some(125_652));
    let hash = h(0x44);
    st.body.mark_archived(hash);
    st.ordered.push_back(hash);
    st.ordered_set.insert(hash);
    apply_confirm_reject(
        &mut st,
        125_653,
        hash,
        "consensus: unexpected previous header",
        None,
        None,
    );
    assert!(
        !st.body.is_rejected(&hash),
        "bad wire must soft re-getdata, not permanent-blacklist tip+1"
    );
    // Retarget window miss: soft (do not freeze tip+1 permanently).
    let mut st = IbdWorkState::new(Vec::new(), None, Some(42_284));
    let hash = h(0x42);
    st.body.mark_archived(hash);
    st.ordered.push_back(hash);
    st.ordered_set.insert(hash);
    apply_confirm_reject(
        &mut st,
        42_285,
        hash,
        "consensus: bad header: missing retarget first header",
        None,
        None,
    );
    assert!(
        !st.body.is_rejected(&hash),
        "missing retarget first header must soft, not permanent-blacklist tip+1"
    );
    assert!(
        st.ordered_set.contains(&hash),
        "soft path leaves ordered path intact"
    );

    // prevout-spent: permanent if it reaches here (write should skip-accept
    // when already committed; soft was a race bandaid).
    let mut st = IbdWorkState::new(Vec::new(), None, Some(362_594));
    let hash = h(0x29);
    st.body.mark_archived(hash);
    st.ordered.push_back(hash);
    st.ordered_set.insert(hash);
    apply_confirm_reject(
        &mut st,
        362_595,
        hash,
        "consensus: prevout already spent on best chain",
        None,
        None,
    );
    assert!(
        st.body.is_rejected(&hash),
        "prevout-spent is permanent at reject layer (write skip-if-committed)"
    );
}

/// Winner body only on BQ-by-hash (not held map / Class A) still reorgs.
#[test]
fn bad_prev_gathers_winner_via_bq_by_hash() {
    use crate::chain::ChainHub;
    use bitcoin::absolute::LockTime;
    use bitcoin::block::{Header, Version};
    use bitcoin::consensus::serialize;
    use bitcoin::script::ScriptBuf;
    use bitcoin::transaction::Version as TxVersion;
    use bitcoin::{
        Amount, CompactTarget, OutPoint, Sequence, Target, Transaction, TxIn, TxOut, Witness,
    };
    use rbitcoin_consensus::{ChainParams, Milestone};
    use rbitcoin_query::Query;
    use std::time::{SystemTime, UNIX_EPOCH};

    if std::env::var_os("RBITCOIN_HEAD_SCALE").is_none() {
        std::env::set_var("RBITCOIN_HEAD_SCALE", "tiny");
    }
    let dir = std::env::temp_dir().join(format!(
        "rbitcoin-badprev-bqhash-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let _ = std::fs::create_dir_all(&dir);
    let q = Query::open_or_create(dir.join("store")).unwrap();
    let hub = ChainHub::new(q, ChainParams::regtest(), Milestone::NONE);
    hub.ensure_genesis().unwrap();
    let gen = hub.tip_hash().unwrap();
    let coinbase = |height: u32| {
        let mut ss = rbitcoin_consensus::bip34_height_script(height);
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
    };
    let mine = |prev: BlockHash, time: u32, height: u32| {
        let bits = CompactTarget::from_consensus(0x207f_ffff);
        let mut block = bitcoin::Block {
            header: Header {
                version: Version::ONE,
                prev_blockhash: prev,
                merkle_root: bitcoin::TxMerkleNode::from_byte_array([0u8; 32]),
                time,
                bits,
                nonce: 0,
            },
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
    };
    let lose = mine(gen, 1_300_000_100, 1);
    let mut win = mine(gen, 1_300_000_101, 1);
    if win.block_hash() == lose.block_hash() {
        let target = Target::from_compact(win.header.bits);
        for nonce in 0..u32::MAX {
            win.header.nonce = nonce;
            if win.header.validate_pow(target).is_ok() && win.block_hash() != lose.block_hash() {
                break;
            }
        }
    }
    hub.accept_block(lose.clone()).unwrap();
    hub.ensure_header(&win.header).unwrap();
    let ext = mine(win.block_hash(), 1_300_000_300, 2);
    hub.ensure_header(&ext.header).unwrap();
    // Winner on BQ under a free height key (tip height already dequeued after confirm).
    hub.query
        .block_queue_offer(1, win.block_hash().to_byte_array(), 0, &serialize(&win))
        .unwrap();
    hub.query
        .block_queue_offer(2, ext.block_hash().to_byte_array(), 0, &serialize(&ext))
        .unwrap();
    assert!(hub
        .query
        .block_queue_payload_by_hash(&win.block_hash().to_byte_array())
        .unwrap()
        .is_some());
    let mut st = IbdWorkState::new(Vec::new(), Some(lose.block_hash()), Some(1));
    // No hold_body — must gather via BQ-by-hash.
    apply_confirm_reject(
        &mut st,
        2,
        ext.block_hash(),
        "consensus: unexpected previous header",
        Some(hub.query.as_ref()),
        Some(&hub),
    );
    assert_eq!(hub.tip_height(), Some(2));
    assert_eq!(hub.tip_hash().unwrap(), ext.block_hash());
    let _ = std::fs::remove_dir_all(dir);
}

/// Mainnet-shaped multi-hop explore: win held (same-height), ext only in BQ.
/// `try_complete_awaiting_reorg` → `try_apply_exploration` must still reorg
/// (must not gate on held-only explore_need_pending).
#[test]
fn exploration_apply_win_held_ext_only_in_bq() {
    use super::try_complete_awaiting_reorg;
    use crate::chain::ChainHub;
    use bitcoin::absolute::LockTime;
    use bitcoin::block::{Header, Version};
    use bitcoin::consensus::serialize;
    use bitcoin::hashes::Hash;
    use bitcoin::script::ScriptBuf;
    use bitcoin::transaction::Version as TxVersion;
    use bitcoin::{
        Amount, BlockHash, CompactTarget, OutPoint, Sequence, Target, Transaction, TxIn, TxOut,
        Witness,
    };
    use rbitcoin_consensus::{ChainParams, Milestone};
    use rbitcoin_query::Query;
    use std::time::{SystemTime, UNIX_EPOCH};

    let dir = std::env::temp_dir().join(format!(
        "rbitcoin-explore-bq-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let _ = std::fs::create_dir_all(&dir);
    let q = Query::open_or_create(dir.join("store")).unwrap();
    let hub = ChainHub::new(q, ChainParams::regtest(), Milestone::NONE);
    hub.ensure_genesis().unwrap();
    let gen = hub.tip_hash().unwrap();
    let coinbase = |height: u32| {
        let mut ss = rbitcoin_consensus::bip34_height_script(height);
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
    };
    let mine = |prev: BlockHash, time: u32, height: u32| {
        let bits = CompactTarget::from_consensus(0x207f_ffff);
        let mut block = bitcoin::Block {
            header: Header {
                version: Version::ONE,
                prev_blockhash: prev,
                merkle_root: bitcoin::TxMerkleNode::from_byte_array([0u8; 32]),
                time,
                bits,
                nonce: 0,
            },
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
    };
    let lose = mine(gen, 1_310_000_100, 1);
    let mut win = mine(gen, 1_310_000_101, 1);
    if win.block_hash() == lose.block_hash() {
        let target = Target::from_compact(win.header.bits);
        for nonce in 0..u32::MAX {
            win.header.nonce = nonce;
            if win.header.validate_pow(target).is_ok() && win.block_hash() != lose.block_hash() {
                break;
            }
        }
    }
    hub.accept_block(lose.clone()).unwrap();
    hub.ensure_header(&win.header).unwrap();
    let ext = mine(win.block_hash(), 1_310_000_200, 2);
    hub.ensure_header(&ext.header).unwrap();

    // Ext only in BQ (height tip+1) — not held. Win held as same-height sibling.
    hub.query
        .block_queue_offer(2, ext.block_hash().to_byte_array(), 0, &serialize(&ext))
        .unwrap();
    assert!(hub
        .query
        .block_queue_payload_by_hash(&ext.block_hash().to_byte_array())
        .unwrap()
        .is_some());
    assert!(hub
        .query
        .block_queue_payload_by_hash(&win.block_hash().to_byte_array())
        .unwrap()
        .is_none());

    let mut st = IbdWorkState::new(Vec::new(), Some(lose.block_hash()), Some(1));
    st.record_height(win.block_hash(), 1);
    st.record_height(ext.block_hash(), 2);
    st.ordered.push_back(win.block_hash());
    st.ordered.push_back(ext.block_hash());
    st.ordered_set.insert(win.block_hash());
    st.ordered_set.insert(ext.block_hash());
    st.reorg.hold_body(win.clone());
    st.reorg
        .register_explore([win.block_hash(), ext.block_hash()], Some(ext.block_hash()));
    // Held-only pending still true (ext not held) — apply must not care.
    assert!(
        st.reorg.explore_need_pending(),
        "precondition: ext not held so held-only pending is true"
    );

    assert!(
        try_complete_awaiting_reorg(&mut st, &hub),
        "exploration apply must succeed with win held + ext in BQ"
    );
    assert_eq!(hub.tip_hash().unwrap(), ext.block_hash());
    assert_eq!(hub.tip_height(), Some(2));
    let _ = std::fs::remove_dir_all(dir);
}

/// Multi-hop with all path bodies already loadable → reorg without await.
#[test]
fn multi_hop_bad_prev_applies_when_full_path_bodies_ready() {
    use crate::chain::ChainHub;
    use bitcoin::absolute::LockTime;
    use bitcoin::block::{Header, Version};
    use bitcoin::consensus::serialize;
    use bitcoin::hashes::Hash;
    use bitcoin::script::ScriptBuf;
    use bitcoin::transaction::Version as TxVersion;
    use bitcoin::{
        Amount, BlockHash, CompactTarget, OutPoint, Sequence, Target, Transaction, TxIn, TxOut,
        Witness,
    };
    use rbitcoin_consensus::{ChainParams, Milestone};
    use rbitcoin_query::Query;
    use std::time::{SystemTime, UNIX_EPOCH};

    let dir = std::env::temp_dir().join(format!(
        "rbitcoin-multi-hop-ready-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let _ = std::fs::create_dir_all(&dir);
    let q = Query::open_or_create(dir.join("store")).unwrap();
    let hub = ChainHub::new(q, ChainParams::regtest(), Milestone::NONE);
    hub.ensure_genesis().unwrap();
    let gen = hub.tip_hash().unwrap();
    let coinbase = |height: u32| {
        let mut ss = rbitcoin_consensus::bip34_height_script(height);
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
    };
    let mine = |prev: BlockHash, time: u32, height: u32| {
        let bits = CompactTarget::from_consensus(0x207f_ffff);
        let mut block = bitcoin::Block {
            header: Header {
                version: Version::ONE,
                prev_blockhash: prev,
                merkle_root: bitcoin::TxMerkleNode::from_byte_array([0u8; 32]),
                time,
                bits,
                nonce: 0,
            },
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
    };
    let l1 = mine(gen, 1_410_000_100, 1);
    hub.accept_block(l1.clone()).unwrap();
    let l2 = mine(l1.block_hash(), 1_410_000_200, 2);
    hub.accept_block(l2.clone()).unwrap();
    let mut w1 = mine(gen, 1_410_000_101, 1);
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
    let w2 = mine(w1.block_hash(), 1_410_000_201, 2);
    hub.ensure_header(&w2.header).unwrap();
    let w3 = mine(w2.block_hash(), 1_410_000_301, 3);
    hub.ensure_header(&w3.header).unwrap();
    // Full path bodies available via BQ-by-hash.
    for (ht, b) in [(1u32, &w1), (2, &w2), (3, &w3)] {
        hub.query
            .block_queue_offer(ht, b.block_hash().to_byte_array(), 0, &serialize(b))
            .unwrap();
    }
    let mut st = IbdWorkState::new(Vec::new(), Some(l2.block_hash()), Some(2));
    apply_confirm_reject(
        &mut st,
        3,
        w3.block_hash(),
        "consensus: unexpected previous header",
        Some(hub.query.as_ref()),
        Some(&hub),
    );
    assert_eq!(
        hub.tip_hash().unwrap(),
        w3.block_hash(),
        "full path ready → reorg without await"
    );
    assert!(st.reorg.awaiting().is_none());
    let _ = std::fs::remove_dir_all(dir);
}

/// Multi-hop fork (log shape): tip already on loser **child**; heavier path
/// needs mid body at fork height, not only wire_prev. BadPrev must densify
/// full LCA path then reorg.
#[test]
fn multi_hop_bad_prev_densifies_full_path_and_reorgs() {
    use super::try_complete_awaiting_reorg;
    use crate::chain::ChainHub;
    use bitcoin::absolute::LockTime;
    use bitcoin::block::{Header, Version};
    use bitcoin::consensus::serialize;
    use bitcoin::hashes::Hash;
    use bitcoin::script::ScriptBuf;
    use bitcoin::transaction::Version as TxVersion;
    use bitcoin::{
        Amount, BlockHash, CompactTarget, OutPoint, Sequence, Target, Transaction, TxIn, TxOut,
        Witness,
    };
    use rbitcoin_consensus::{ChainParams, Milestone};
    use rbitcoin_query::Query;
    use std::time::{SystemTime, UNIX_EPOCH};

    let dir = std::env::temp_dir().join(format!(
        "rbitcoin-multi-hop-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let _ = std::fs::create_dir_all(&dir);
    let q = Query::open_or_create(dir.join("store")).unwrap();
    let hub = ChainHub::new(q, ChainParams::regtest(), Milestone::NONE);
    hub.ensure_genesis().unwrap();
    let gen = hub.tip_hash().unwrap();
    let coinbase = |height: u32| {
        let mut ss = rbitcoin_consensus::bip34_height_script(height);
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
    };
    let mine = |prev: BlockHash, time: u32, height: u32| {
        let bits = CompactTarget::from_consensus(0x207f_ffff);
        let mut block = bitcoin::Block {
            header: Header {
                version: Version::ONE,
                prev_blockhash: prev,
                merkle_root: bitcoin::TxMerkleNode::from_byte_array([0u8; 32]),
                time,
                bits,
                nonce: 0,
            },
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
    };
    let distinct = |mut b: bitcoin::Block, avoid: BlockHash| {
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
    };

    // Loser path: gen → L1 → L2 (tip).
    let l1 = mine(gen, 1_400_000_100, 1);
    hub.accept_block(l1.clone()).unwrap();
    let l2 = mine(l1.block_hash(), 1_400_000_200, 2);
    hub.accept_block(l2.clone()).unwrap();
    assert_eq!(hub.tip_height(), Some(2));
    assert_eq!(hub.tip_hash().unwrap(), l2.block_hash());

    // Heavier path: gen → W1 → W2 → W3 (headers; bodies staged).
    let w1 = distinct(mine(gen, 1_400_000_101, 1), l1.block_hash());
    hub.ensure_header(&w1.header).unwrap();
    let w2 = mine(w1.block_hash(), 1_400_000_201, 2);
    hub.ensure_header(&w2.header).unwrap();
    let w3 = mine(w2.block_hash(), 1_400_000_301, 3);
    hub.ensure_header(&w3.header).unwrap();

    // Only tip+1 body available (W3); mids W1/W2 missing — log shape.
    hub.query
        .block_queue_offer(3, w3.block_hash().to_byte_array(), 0, &serialize(&w3))
        .unwrap();

    let mut st = IbdWorkState::new(Vec::new(), Some(l2.block_hash()), Some(2));
    apply_confirm_reject(
        &mut st,
        3,
        w3.block_hash(),
        "consensus: unexpected previous header",
        Some(hub.query.as_ref()),
        Some(&hub),
    );
    // Must await **both** mid bodies, not only wire_prev (W2).
    let need = st.reorg.need_getdata();
    assert!(
        need.contains(&w1.block_hash()),
        "must densify fork-height mid W1; need={need:?}"
    );
    assert!(
        need.contains(&w2.block_hash()),
        "must densify wire_prev W2; need={need:?}"
    );
    assert_eq!(
        hub.tip_hash().unwrap(),
        l2.block_hash(),
        "tip unchanged while awaiting"
    );
    // Handled without soft re-get of tip+1 (mainnet livelock class).
    assert!(
        st.reorg.is_awaiting_held_tip(&w3.block_hash()),
        "must await held tip+1 W3"
    );
    assert!(
        !st.body.skip_download(&hub, &w1.block_hash())
            || st.reorg.need_getdata().contains(&w1.block_hash()),
        "mids must remain densify targets"
    );
    // Second BadPrev while awaiting must not soft mark_missing tip+1
    // (that re-getdatas tip+1 forever while mids starve).
    apply_confirm_reject(
        &mut st,
        3,
        w3.block_hash(),
        "consensus: unexpected previous header",
        Some(hub.query.as_ref()),
        Some(&hub),
    );
    assert!(
        st.reorg.is_awaiting_held_tip(&w3.block_hash()),
        "still awaiting after re-reject"
    );
    // tip+1 stays pending (held), not demoted to missing for densify re-get.
    assert!(
        st.body.is_pending(&w3.block_hash()) || st.reorg.get_held(&w3.block_hash()).is_some(),
        "tip+1 must stay held/pending while awaiting mids, not soft-missing"
    );
    assert!(
        st.reorg.need_getdata().contains(&w1.block_hash())
            && st.reorg.need_getdata().contains(&w2.block_hash()),
        "mids still densify targets after re-reject"
    );

    // Bodies arrive → complete reorg.
    st.reorg.hold_body(w1.clone());
    st.reorg.hold_body(w2.clone());
    assert!(
        try_complete_awaiting_reorg(&mut st, &hub),
        "full path bodies must apply reorg"
    );
    assert_eq!(hub.tip_hash().unwrap(), w3.block_hash());
    assert_eq!(hub.tip_height(), Some(3));
    let _ = std::fs::remove_dir_all(dir);
}

/// Mainnet-shaped stall repro (no production fix yet): tip already on a
/// **loser child** while the heavier path's mid blocks sit at heights that
/// are already confirmed (with loser bodies). Densify only fills far
/// extensions on the winner path (tip+2+) into the body queue, leaving a
/// tip+1 hole — and something must still be able to densify/load the mids
/// at those already-confirmed heights (reorg need / BlockFramed hold), or
/// IBD spins with hole>0 + conf=0 while BQ grows ahead.
///
/// Log shape: resume explore_need=…, tip frozen, hole≥1, bq soft growing,
/// claim spinning, no reorg until mids load.
#[test]
fn confirmed_height_mids_blocked_while_densify_ahead_leaves_tip_hole() {
    use super::super::assign::{assign_work_ordered, AssignDepth};
    use super::super::path::seed_work_path_from_store;
    use super::super::peer_io::{PeerEvent, PeerSlot};
    use super::super::progress::{claim_ready, tip_fetch_hole};
    use super::super::status::LoopStats;
    use super::super::IbdConfig;
    use super::{apply_peer_event, try_complete_awaiting_reorg};
    use crate::chain::ChainHub;
    use crate::seeds::AddrMan;
    use bitcoin::absolute::LockTime;
    use bitcoin::block::{Header, Version};
    use bitcoin::consensus::serialize;
    use bitcoin::hashes::Hash;
    use bitcoin::script::ScriptBuf;
    use bitcoin::transaction::Version as TxVersion;
    use bitcoin::{
        Amount, BlockHash, CompactTarget, OutPoint, Sequence, Target, Transaction, TxIn, TxOut,
        Witness,
    };
    use rbitcoin_consensus::{ChainParams, Milestone};
    use rbitcoin_query::Query;
    use std::collections::HashSet;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};
    use std::sync::atomic::{AtomicU32, AtomicU64};
    use std::sync::Arc;
    use std::time::{SystemTime, UNIX_EPOCH};
    use tokio::sync::mpsc;

    if std::env::var_os("RBITCOIN_HEAD_SCALE").is_none() {
        std::env::set_var("RBITCOIN_HEAD_SCALE", "tiny");
    }
    let dir = std::env::temp_dir().join(format!(
        "rbitcoin-mid-confirmed-hole-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let _ = std::fs::create_dir_all(&dir);
    let q = Query::open_or_create(dir.join("store")).unwrap();
    let hub = ChainHub::new(q, ChainParams::regtest(), Milestone::NONE);
    hub.ensure_genesis().unwrap();
    let gen = hub.tip_hash().unwrap();

    let coinbase = |height: u32| {
        let mut ss = rbitcoin_consensus::bip34_height_script(height);
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
    };
    let mine = |prev: BlockHash, time: u32, height: u32| {
        let bits = CompactTarget::from_consensus(0x207f_ffff);
        let mut block = bitcoin::Block {
            header: Header {
                version: Version::ONE,
                prev_blockhash: prev,
                merkle_root: bitcoin::TxMerkleNode::from_byte_array([0u8; 32]),
                time,
                bits,
                nonce: 0,
            },
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
    };
    let distinct = |mut b: bitcoin::Block, avoid: BlockHash| {
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
    };

    // Loser path confirmed: gen → L1 → L2 (tip @2). Heights 1 and 2 occupied.
    let l1 = mine(gen, 1_500_000_100, 1);
    hub.accept_block(l1.clone()).unwrap();
    let l2 = mine(l1.block_hash(), 1_500_000_200, 2);
    hub.accept_block(l2.clone()).unwrap();
    assert_eq!(hub.tip_height(), Some(2));
    assert!(hub.has_block(&l1.block_hash()));
    assert!(hub.has_block(&l2.block_hash()));

    // Heavier winner headers only: gen → W1@1 → W2@2 → W3@3 → W4@4 → W5@5.
    // Mid heights 1 and 2 are already confirmed (with L1/L2) — bodies missing.
    let w1 = distinct(mine(gen, 1_500_000_101, 1), l1.block_hash());
    hub.ensure_header(&w1.header).unwrap();
    let w2 = mine(w1.block_hash(), 1_500_000_201, 2);
    hub.ensure_header(&w2.header).unwrap();
    let w3 = mine(w2.block_hash(), 1_500_000_301, 3);
    hub.ensure_header(&w3.header).unwrap();
    let w4 = mine(w3.block_hash(), 1_500_000_401, 4);
    hub.ensure_header(&w4.header).unwrap();
    let w5 = mine(w4.block_hash(), 1_500_000_501, 5);
    hub.ensure_header(&w5.header).unwrap();

    assert!(!hub.has_block(&w1.block_hash()));
    assert!(!hub.has_block(&w2.block_hash()));

    // Resume seed as IBD does after open (mainnet explore_need log).
    let (cmd_tx, _rx) = mpsc::unbounded_channel();
    let task = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .spawn(async {});
    let slot = PeerSlot {
        id: 0,
        addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 18444),
        cmd_tx,
        in_flight: HashSet::new(),
        block_progress_ms: Arc::new(AtomicU64::new(0)),
        peer_height: 100,
        connected_ms: 1,
        first_data_ms: AtomicU64::new(0),
        bytes_rx: AtomicU64::new(0),
        alive: true,
        task,
    };
    let mut st = IbdWorkState::new(vec![slot], hub.tip_hash(), hub.tip_height());
    seed_work_path_from_store(&mut st, &hub);

    assert!(
        st.ordered_set.contains(&w1.block_hash()) && st.ordered_set.contains(&w2.block_hash()),
        "seed must order winner mids at confirmed heights"
    );
    assert_eq!(
        st.height_to_hash.get(&1),
        Some(&w1.block_hash()),
        "height map must track winner W1 at already-confirmed height 1 (not L1)"
    );
    assert_eq!(
        st.height_to_hash.get(&2),
        Some(&w2.block_hash()),
        "height map must track winner W2 at already-confirmed height 2 (not L2)"
    );
    assert_eq!(st.height_to_hash.get(&3), Some(&w3.block_hash()));

    let need = st.reorg.need_getdata();
    assert!(
        need.contains(&w1.block_hash()) && need.contains(&w2.block_hash()),
        "explore/reorg densify must still need mids at confirmed heights; need={need:?}"
    );
    // skip_download must not treat "height already confirmed" as done for the
    // *winner* hash — only hub.has_block(that hash).
    assert!(
        !st.body.skip_download(&hub, &w1.block_hash()),
        "W1 must remain downloadable despite height 1 being confirmed as L1"
    );
    assert!(
        !st.body.skip_download(&hub, &w2.block_hash()),
        "W2 must remain downloadable despite height 2 being confirmed as L2"
    );

    // Densify-ahead shape (mainnet): only a *far* winner body lands in BQ
    // (tip+3), leaving a contiguous hole at tip+1..tip+2. Far payload alone
    // must not reorg; explore_tip is W5 without a full contiguous body path.
    hub.query
        .block_queue_offer(5, w5.block_hash().to_byte_array(), 0, &serialize(&w5))
        .unwrap();
    st.body.mark_pending(w5.block_hash());

    assert!(
        !claim_ready(&hub, &mut st.body, 3, &w3.block_hash()),
        "tip+1 W3 must not be claim-ready when only far densify is in BQ"
    );
    let hole = tip_fetch_hole(&hub, &st.height_to_hash, &mut st.body);
    assert!(
        hole >= 2,
        "must leave tip+1.. hole while densify is only far ahead; hole={hole}"
    );
    assert!(
        !try_complete_awaiting_reorg(&mut st, &hub),
        "far non-contiguous BQ alone must not reorg off loser tip"
    );
    assert_eq!(hub.tip_hash().unwrap(), l2.block_hash());

    // Assign must still getdata mids at confirmed heights + tip hole — not
    // only densify further past the hole.
    let stats = LoopStats::default();
    let cfg = IbdConfig::for_test();
    assign_work_ordered(
        &mut st,
        &hub,
        &cfg,
        &stats,
        1.0,
        3,
        AssignDepth::Full,
        true,
        None,
    );
    assert!(
        st.inflight.contains_key(&w1.block_hash()) && st.inflight.contains_key(&w2.block_hash()),
        "assign reorg need (1b) must getdata both mids at confirmed heights; inflight={:?}",
        st.inflight.keys().collect::<Vec<_>>()
    );
    assert!(
        st.inflight.contains_key(&w3.block_hash()),
        "assign tip-hole race must also getdata tip+1 W3; inflight={:?}",
        st.inflight.keys().collect::<Vec<_>>()
    );

    // Production BlockFramed path for height≤tip mids: hold by hash (BQ
    // first-wins cannot store same-height competitors of confirmed tip).
    let write_next = AtomicU32::new(3);
    let mut book = AddrMan::new();
    let local = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 1);
    for b in [&w1, &w2] {
        apply_peer_event(
            &mut st,
            &hub,
            PeerEvent::BlockFramed {
                peer: 0,
                hash: b.block_hash(),
                payload: serialize(b),
            },
            &write_next,
            &mut book,
            local,
            None,
        );
        assert!(
            st.reorg.get_held(&b.block_hash()).is_some(),
            "BlockFramed mid at confirmed height must hold by hash for reorg; hash={}",
            b.block_hash()
        );
        // Still on loser: exploration tip is far (W5) and W3/W4 not loadable yet.
        assert_eq!(
            hub.tip_hash().unwrap(),
            l2.block_hash(),
            "holding mids alone must not reorg without contiguous path to a tip"
        );
    }

    // Tip+1 body arrives into BQ (height > tip). Exploration may still fail
    // (explore_need includes W4; explore_tip=W5 needs full path). Confirm
    // BadPrev is the production path that densifies LCA→tip+1 and applies.
    apply_peer_event(
        &mut st,
        &hub,
        PeerEvent::BlockFramed {
            peer: 0,
            hash: w3.block_hash(),
            payload: serialize(&w3),
        },
        &write_next,
        &mut book,
        local,
        None,
    );
    assert!(
        hub.query.block_queue_has_height(3) || st.reorg.get_held(&w3.block_hash()).is_some(),
        "W3 tip+1 must land in BQ or held"
    );

    // Exploration may apply if a full path is loadable; else BadPrev densify
    // to LCA must reorg using held mids at confirmed heights.
    let _ = try_complete_awaiting_reorg(&mut st, &hub);
    if hub.tip_hash() == Some(l2.block_hash()) {
        apply_confirm_reject(
            &mut st,
            3,
            w3.block_hash(),
            "consensus: unexpected previous header",
            Some(hub.query.as_ref()),
            Some(&hub),
        );
        if hub.tip_hash() == Some(l2.block_hash()) {
            let _ = try_complete_awaiting_reorg(&mut st, &hub);
        }
    }

    // Contract: with mids held at already-confirmed heights + tip+1 wire,
    // tip must leave the loser fork. Failure here is the mainnet stall class.
    let tip = hub.tip_hash().unwrap();
    assert_ne!(
        tip,
        l2.block_hash(),
        "must reorg off loser once mid bodies at confirmed heights are held + tip+1 wire; \
         tip={tip} need={:?} held_w1={} held_w2={} bq3={}",
        st.reorg.need_getdata(),
        st.reorg.get_held(&w1.block_hash()).is_some(),
        st.reorg.get_held(&w2.block_hash()).is_some(),
        hub.query.block_queue_has_height(3)
    );
    assert!(
        tip == w3.block_hash() || tip == w4.block_hash() || tip == w5.block_hash(),
        "tip must be on winner path; tip={tip}"
    );
    assert!(
        hub.tip_height().unwrap() >= 3,
        "winner tip height must be ≥3; got {:?}",
        hub.tip_height()
    );

    let _ = std::fs::remove_dir_all(dir);
}

/// Zombie `pending` on a mid at an **already-confirmed height** must be
/// demoted by reorg densify assign (1b) the same way tip-hole cover demotes
/// tip+1 zombies. Tip-batch stale expire only walks tip+1.. — without 1b
/// demote, `skip_download` forever and mids never re-getdata.
#[test]
fn zombie_pending_mid_at_confirmed_height_never_reget() {
    use super::super::assign::{assign_work_ordered, AssignDepth};
    use super::super::path::seed_work_path_from_store;
    use super::super::peer_io::PeerSlot;
    use super::super::status::LoopStats;
    use super::super::IbdConfig;
    use crate::chain::ChainHub;
    use bitcoin::absolute::LockTime;
    use bitcoin::block::{Header, Version};
    use bitcoin::hashes::Hash;
    use bitcoin::script::ScriptBuf;
    use bitcoin::transaction::Version as TxVersion;
    use bitcoin::{
        Amount, BlockHash, CompactTarget, OutPoint, Sequence, Target, Transaction, TxIn, TxOut,
        Witness,
    };
    use rbitcoin_consensus::{ChainParams, Milestone};
    use rbitcoin_query::Query;
    use std::collections::HashSet;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};
    use std::sync::atomic::AtomicU64;
    use std::sync::Arc;
    use std::time::{SystemTime, UNIX_EPOCH};
    use tokio::sync::mpsc;

    if std::env::var_os("RBITCOIN_HEAD_SCALE").is_none() {
        std::env::set_var("RBITCOIN_HEAD_SCALE", "tiny");
    }
    let dir = std::env::temp_dir().join(format!(
        "rbitcoin-zombie-mid-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let _ = std::fs::create_dir_all(&dir);
    let q = Query::open_or_create(dir.join("store")).unwrap();
    let hub = ChainHub::new(q, ChainParams::regtest(), Milestone::NONE);
    hub.ensure_genesis().unwrap();
    let gen = hub.tip_hash().unwrap();

    let coinbase = |height: u32| {
        let mut ss = rbitcoin_consensus::bip34_height_script(height);
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
    };
    let mine = |prev: BlockHash, time: u32, height: u32| {
        let bits = CompactTarget::from_consensus(0x207f_ffff);
        let mut block = bitcoin::Block {
            header: Header {
                version: Version::ONE,
                prev_blockhash: prev,
                merkle_root: bitcoin::TxMerkleNode::from_byte_array([0u8; 32]),
                time,
                bits,
                nonce: 0,
            },
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
    };
    let distinct = |mut b: bitcoin::Block, avoid: BlockHash| {
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
    };

    let l1 = mine(gen, 1_510_000_100, 1);
    hub.accept_block(l1.clone()).unwrap();
    let l2 = mine(l1.block_hash(), 1_510_000_200, 2);
    hub.accept_block(l2.clone()).unwrap();
    let w1 = distinct(mine(gen, 1_510_000_101, 1), l1.block_hash());
    hub.ensure_header(&w1.header).unwrap();
    let w2 = mine(w1.block_hash(), 1_510_000_201, 2);
    hub.ensure_header(&w2.header).unwrap();
    let w3 = mine(w2.block_hash(), 1_510_000_301, 3);
    hub.ensure_header(&w3.header).unwrap();

    let (cmd_tx, _rx) = mpsc::unbounded_channel();
    let task = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .spawn(async {});
    let slot = PeerSlot {
        id: 0,
        addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 18445),
        cmd_tx,
        in_flight: HashSet::new(),
        block_progress_ms: Arc::new(AtomicU64::new(0)),
        peer_height: 100,
        connected_ms: 1,
        first_data_ms: AtomicU64::new(0),
        bytes_rx: AtomicU64::new(0),
        alive: true,
        task,
    };
    let mut st = IbdWorkState::new(vec![slot], hub.tip_hash(), hub.tip_height());
    seed_work_path_from_store(&mut st, &hub);
    assert!(st.reorg.need_getdata().contains(&w1.block_hash()));

    // Zombie: pending flag without held body and without BQ wire — same class
    // as tip+1 zombie, but at height ≤ tip so cover_tip_holes never demotes it.
    st.body.mark_pending(w1.block_hash());
    assert!(st.body.is_pending(&w1.block_hash()));
    assert!(st.reorg.get_held(&w1.block_hash()).is_none());
    assert!(!hub.query.block_queue_has_height(1));
    assert!(
        st.body.skip_download(&hub, &w1.block_hash()),
        "precondition: pending mid is skip_download"
    );
    assert!(
        st.reorg.need_getdata().contains(&w1.block_hash()),
        "need_getdata still lists mid (not held)"
    );

    let stats = LoopStats::default();
    let cfg = IbdConfig::for_test();
    assign_work_ordered(
        &mut st,
        &hub,
        &cfg,
        &stats,
        1.0,
        3,
        AssignDepth::Full,
        true,
        None,
    );

    // Desired contract: demote zombie mid and re-getdata (same as tip-hole
    // cover does for tip+1 zombies). Today assign 1b skip_download's the mid
    // forever — this assertion is the red pin for that stall class.
    assert!(
        st.inflight.contains_key(&w1.block_hash()),
        "must re-getdata zombie-pending mid at already-confirmed height; \
         skip_download={} need={:?} inflight={:?}",
        st.body.skip_download(&hub, &w1.block_hash()),
        st.reorg.need_getdata(),
        st.inflight.keys().collect::<Vec<_>>()
    );
    assert!(
        !st.body.is_pending(&w1.block_hash()),
        "zombie mid pending must be demoted to missing before re-get (like tip-hole cover)"
    );

    let _ = std::fs::remove_dir_all(dir);
}

/// Competing BadPrev with bodies available reorgs onto winning path (not soft-livelock).
#[test]
fn bad_prev_competing_path_reorgs_via_apply_confirm_reject() {
    use crate::chain::ChainHub;
    use bitcoin::absolute::LockTime;
    use bitcoin::block::{Header, Version};
    use bitcoin::consensus::serialize;
    use bitcoin::script::ScriptBuf;
    use bitcoin::transaction::Version as TxVersion;
    use bitcoin::{
        Amount, CompactTarget, OutPoint, Sequence, Target, Transaction, TxIn, TxOut, Witness,
    };
    use rbitcoin_consensus::{ChainParams, Milestone};
    use rbitcoin_query::Query;
    use std::time::{SystemTime, UNIX_EPOCH};

    if std::env::var_os("RBITCOIN_HEAD_SCALE").is_none() {
        std::env::set_var("RBITCOIN_HEAD_SCALE", "tiny");
    }
    let dir = std::env::temp_dir().join(format!(
        "rbitcoin-badprev-reorg-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let _ = std::fs::create_dir_all(&dir);
    let q = Query::open_or_create(dir.join("store")).unwrap();
    let hub = ChainHub::new(q, ChainParams::regtest(), Milestone::NONE);
    hub.ensure_genesis().unwrap();
    let gen = hub.tip_hash().unwrap();

    let coinbase = |height: u32| {
        let mut ss = rbitcoin_consensus::bip34_height_script(height);
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
    };
    let mine = |prev: BlockHash, time: u32, height: u32| {
        let bits = CompactTarget::from_consensus(0x207f_ffff);
        let header = Header {
            version: Version::ONE,
            prev_blockhash: prev,
            merkle_root: bitcoin::TxMerkleNode::from_byte_array([0u8; 32]),
            time,
            bits,
            nonce: 0,
        };
        let mut block = bitcoin::Block {
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
    };

    let lose = mine(gen, 1_300_000_100, 1);
    let mut win = mine(gen, 1_300_000_101, 1);
    if win.block_hash() == lose.block_hash() {
        let target = Target::from_compact(win.header.bits);
        for nonce in 0..u32::MAX {
            win.header.nonce = nonce;
            if win.header.validate_pow(target).is_ok() && win.block_hash() != lose.block_hash() {
                break;
            }
        }
    }
    hub.accept_block(lose.clone()).unwrap();
    hub.ensure_header(&win.header).unwrap();
    let ext = mine(win.block_hash(), 1_300_000_300, 2);
    hub.ensure_header(&ext.header).unwrap();
    // Winning sibling body held by hash (cannot share tip height BQ slot).
    // Ext body on BQ at tip+1 — the real BadPrev wire shape.
    let wire = serialize(&ext);
    hub.query
        .block_queue_offer(2, ext.block_hash().to_byte_array(), 0, &wire)
        .unwrap();

    let mut st = IbdWorkState::new(Vec::new(), Some(lose.block_hash()), Some(1));
    st.ordered.push_back(ext.block_hash());
    st.ordered_set.insert(ext.block_hash());
    // Side body arrives as BlockFramed would: hold by hash before reject.
    st.reorg.hold_body(win.clone());
    let pre_tip = hub.tip_hash().unwrap();
    assert_eq!(pre_tip, lose.block_hash());
    // Sole entry: shipped apply_confirm_reject must reorg tip onto ext.
    apply_confirm_reject(
        &mut st,
        2,
        ext.block_hash(),
        "consensus: unexpected previous header",
        Some(hub.query.as_ref()),
        Some(&hub),
    );
    assert_eq!(
        hub.tip_height(),
        Some(2),
        "apply_confirm_reject alone must reorg when winner body is held"
    );
    assert_eq!(
        hub.tip_hash().unwrap(),
        ext.block_hash(),
        "tip must be winning path tip+1 after BadPrev reorg"
    );
    assert_ne!(hub.tip_hash().unwrap(), pre_tip);
    assert!(!st.body.is_rejected(&ext.block_hash()));
    let _ = std::fs::remove_dir_all(dir);
}

/// Without winner body: awaiting densify; after hold_body(winner), re-reject
/// completes reorg via held gather (shipped apply_confirm_reject only).
#[test]
fn bad_prev_awaits_winner_body_then_reorgs_when_held() {
    use crate::chain::ChainHub;
    use bitcoin::absolute::LockTime;
    use bitcoin::block::{Header, Version};
    use bitcoin::consensus::serialize;
    use bitcoin::script::ScriptBuf;
    use bitcoin::transaction::Version as TxVersion;
    use bitcoin::{
        Amount, CompactTarget, OutPoint, Sequence, Target, Transaction, TxIn, TxOut, Witness,
    };
    use rbitcoin_consensus::{ChainParams, Milestone};
    use rbitcoin_query::Query;
    use std::time::{SystemTime, UNIX_EPOCH};

    if std::env::var_os("RBITCOIN_HEAD_SCALE").is_none() {
        std::env::set_var("RBITCOIN_HEAD_SCALE", "tiny");
    }
    let dir = std::env::temp_dir().join(format!(
        "rbitcoin-badprev-await-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let _ = std::fs::create_dir_all(&dir);
    let q = Query::open_or_create(dir.join("store")).unwrap();
    let hub = ChainHub::new(q, ChainParams::regtest(), Milestone::NONE);
    hub.ensure_genesis().unwrap();
    let gen = hub.tip_hash().unwrap();
    let coinbase = |height: u32| {
        let mut ss = rbitcoin_consensus::bip34_height_script(height);
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
    };
    let mine = |prev: BlockHash, time: u32, height: u32| {
        let bits = CompactTarget::from_consensus(0x207f_ffff);
        let mut block = bitcoin::Block {
            header: Header {
                version: Version::ONE,
                prev_blockhash: prev,
                merkle_root: bitcoin::TxMerkleNode::from_byte_array([0u8; 32]),
                time,
                bits,
                nonce: 0,
            },
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
    };
    let lose = mine(gen, 1_300_000_100, 1);
    let mut win = mine(gen, 1_300_000_101, 1);
    if win.block_hash() == lose.block_hash() {
        let target = Target::from_compact(win.header.bits);
        for nonce in 0..u32::MAX {
            win.header.nonce = nonce;
            if win.header.validate_pow(target).is_ok() && win.block_hash() != lose.block_hash() {
                break;
            }
        }
    }
    hub.accept_block(lose.clone()).unwrap();
    hub.ensure_header(&win.header).unwrap();
    let ext = mine(win.block_hash(), 1_300_000_300, 2);
    hub.ensure_header(&ext.header).unwrap();
    hub.query
        .block_queue_offer(2, ext.block_hash().to_byte_array(), 0, &serialize(&ext))
        .unwrap();

    let mut st = IbdWorkState::new(Vec::new(), Some(lose.block_hash()), Some(1));
    // No winner held — CompetingPath awaits densify.
    apply_confirm_reject(
        &mut st,
        2,
        ext.block_hash(),
        "consensus: unexpected previous header",
        Some(hub.query.as_ref()),
        Some(&hub),
    );
    assert_eq!(
        hub.tip_height(),
        Some(1),
        "must not reorg without winner body"
    );
    let need = st.reorg.need_getdata();
    assert!(
        need.contains(&win.block_hash()),
        "must densify-request winning sibling: {need:?}"
    );
    assert!(st.reorg.awaiting().is_some());
    // Winner body arrives (BlockFramed path holds by hash).
    st.reorg.hold_body(win.clone());
    // Re-offer ext wire (soft path may have dequeued BQ).
    hub.query
        .block_queue_offer(2, ext.block_hash().to_byte_array(), 0, &serialize(&ext))
        .unwrap();
    apply_confirm_reject(
        &mut st,
        2,
        ext.block_hash(),
        "consensus: unexpected previous header",
        Some(hub.query.as_ref()),
        Some(&hub),
    );
    assert_eq!(hub.tip_height(), Some(2));
    assert_eq!(hub.tip_hash().unwrap(), ext.block_hash());
    assert!(st.reorg.awaiting().is_none() || st.reorg.need_getdata().is_empty());
    let _ = std::fs::remove_dir_all(dir);
}

/// Wire-path soft budget charged on receive must release on script reject
/// **and** on soft prevout-spent (write emits Reject when has_block is false;
#[test]
fn apply_peer_event_body_and_control_surface() {
    use super::super::peer_io::{PeerEvent, PeerSlot};
    use super::super::state::InflightReq;
    use super::{apply_peer_event, drain_ready_peer_and_archive_events, inject_learned_addrs};
    use crate::seeds::AddrMan;
    use bitcoin::block::{Header, Version};
    use bitcoin::CompactTarget;
    use rbitcoin_consensus::{ChainParams, Milestone};
    use rbitcoin_query::Query;
    use std::collections::HashSet;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};
    use std::sync::atomic::{AtomicU32, AtomicU64};
    use std::sync::Arc;
    use tokio::sync::mpsc;

    fn addr(o: u8) -> SocketAddr {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 1, 0, o)), 18444)
    }
    fn dummy_slot(id: usize, a: SocketAddr) -> PeerSlot {
        let (cmd_tx, _rx) = mpsc::unbounded_channel();
        let task = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .spawn(async {});
        PeerSlot {
            id,
            addr: a,
            cmd_tx,
            in_flight: HashSet::new(),
            block_progress_ms: Arc::new(AtomicU64::new(0)),
            peer_height: 10,
            connected_ms: 1,
            first_data_ms: AtomicU64::new(0),
            bytes_rx: AtomicU64::new(0),
            alive: true,
            task,
        }
    }
    fn dummy_header(prev: BlockHash, n: u8) -> Header {
        Header {
            version: Version::ONE,
            prev_blockhash: prev,
            merkle_root: bitcoin::TxMerkleNode::from_byte_array([n; 32]),
            time: 1_300_000_000 + u32::from(n),
            bits: CompactTarget::from_consensus(0x207fffff),
            nonce: u32::from(n),
        }
    }

    let dir = std::env::temp_dir().join(format!(
        "rbitcoin-ev-apply-{}-{}",
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

    let mut st = IbdWorkState::new(vec![dummy_slot(1, addr(1))], Some(gen), Some(0));
    st.slots[0].in_flight.insert(h(9));
    st.inflight.insert(h(9), InflightReq::new(1));

    let write_next = AtomicU32::new(1);
    let mut book = AddrMan::new();
    let local = addr(99);

    // BlockFramed without known height → missing (re-getdata after height map).
    apply_peer_event(
        &mut st,
        &hub,
        PeerEvent::BlockFramed {
            peer: 1,
            hash: h(9),
            payload: vec![0u8; 80],
        },
        &write_next,
        &mut book,
        local,
        None,
    );
    assert!(st.inflight.is_empty());
    assert!(!st.body.is_pending(&h(9)));

    // Class A known (resume seed) without BQ: still accept peer wire into the
    // body queue so claim_ready can become true after tip-hole re-getdata.
    let class_a_hash = h(0xca);
    st.body.mark_archived(class_a_hash);
    st.record_height(class_a_hash, 1);
    st.height_to_hash.insert(1, class_a_hash);
    st.header_fks
        .insert(class_a_hash, rbitcoin_primitives::Fk(1));
    st.slots[0].in_flight.insert(class_a_hash);
    st.inflight.insert(class_a_hash, InflightReq::new(1));
    // Minimal framed payload (header prefix + empty body is enough for offer).
    let mut payload = vec![0u8; 81];
    payload[0..4].copy_from_slice(&1u32.to_le_bytes()); // version-ish
    apply_peer_event(
        &mut st,
        &hub,
        PeerEvent::BlockFramed {
            peer: 1,
            hash: class_a_hash,
            payload,
        },
        &write_next,
        &mut book,
        local,
        None,
    );
    assert!(
        st.body.is_pending(&class_a_hash),
        "Class A known must still land in pending after wire offer"
    );
    assert!(
        hub.query.block_queue_has_height(1),
        "Class A known must still enter body queue (claim intake)"
    );

    // Decode fail → missing so re-getdata allowed.
    st.body.mark_pending(h(9));
    apply_peer_event(
        &mut st,
        &hub,
        PeerEvent::BlockDecodeFailed {
            peer: 1,
            hash: h(9),
        },
        &write_next,
        &mut book,
        local,
        None,
    );
    assert!(!st.body.is_pending(&h(9)));

    // Headers: attach height from tip parent and order.
    let hdr = dummy_header(gen, 1);
    let hash = hdr.block_hash();
    apply_peer_event(
        &mut st,
        &hub,
        PeerEvent::Headers {
            peer: 1,
            headers: vec![hdr],
        },
        &write_next,
        &mut book,
        local,
        None,
    );
    assert!(st.known_headers.contains(&hash));
    assert!(st.ordered_set.contains(&hash) || st.hash_height.contains_key(&hash));

    // Empty headers with lag → keep headers_done false.
    st.max_peer_height = 100;
    st.empty_header_streak = 0;
    apply_peer_event(
        &mut st,
        &hub,
        PeerEvent::Headers {
            peer: 1,
            headers: vec![],
        },
        &write_next,
        &mut book,
        local,
        None,
    );
    assert!(!st.headers_done);

    // NotFound clears peer inflight.
    st.slots[0].in_flight.insert(h(3));
    st.inflight.insert(h(3), InflightReq::new(1));
    apply_peer_event(
        &mut st,
        &hub,
        PeerEvent::NotFound {
            peer: 1,
            hashes: vec![h(3)],
        },
        &write_next,
        &mut book,
        local,
        None,
    );
    assert!(!st.inflight.contains_key(&h(3)));

    // Addrs + inject filter.
    inject_learned_addrs(&mut book, &[], local, 1);
    inject_learned_addrs(
        &mut book,
        &[
            addr(2),
            local,
            SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 1),
        ],
        local,
        1,
    );
    assert!(book.entry(&addr(2)).is_some());

    // Dead releases work.
    st.slots[0].in_flight.insert(h(4));
    st.inflight.insert(h(4), InflightReq::new(1));
    apply_peer_event(
        &mut st,
        &hub,
        PeerEvent::Dead {
            peer: 1,
            reason: "bye".into(),
        },
        &write_next,
        &mut book,
        local,
        None,
    );
    assert!(!st.slots[0].alive);
    assert!(!st.inflight.contains_key(&h(4)));

    // Drain empty channels.
    let (body_tx, mut body_rx) = mpsc::unbounded_channel();
    let (ctrl_tx, mut ctrl_rx) = mpsc::unbounded_channel();
    let stats = super::super::status::LoopStats::default();
    let ok = drain_ready_peer_and_archive_events(
        &mut st,
        &hub,
        &mut body_rx,
        &mut ctrl_rx,
        &write_next,
        &stats,
        &mut book,
        local,
        None,
    )
    .unwrap();
    assert!(ok);
    drop(body_tx);
    drop(ctrl_tx);

    let _ = std::fs::remove_dir_all(dir);
}

/// Raw BlockFramed → body queue; redelivery keeps one rec; far horizon skipped.
#[test]
fn apply_peer_event_block_framed_bq_horizon_and_headers_done() {
    use super::super::peer_io::{PeerEvent, PeerSlot};
    use super::{apply_peer_event, drain_ready_peer_and_archive_events, inject_learned_addrs};
    use crate::seeds::AddrMan;
    use bitcoin::absolute::LockTime;
    use bitcoin::block::{Header, Version};
    use bitcoin::consensus::Encodable;
    use bitcoin::script::ScriptBuf;
    use bitcoin::transaction::Version as TxVersion;
    use bitcoin::{
        Amount, Block, CompactTarget, OutPoint, Sequence, Transaction, TxIn, TxOut, Witness,
    };
    use rbitcoin_consensus::{ChainParams, Milestone};
    use rbitcoin_query::Query;
    use std::collections::HashSet;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};
    use std::sync::atomic::{AtomicU32, AtomicU64};
    use std::sync::Arc;
    use tokio::sync::mpsc;

    fn dummy_slot(id: usize) -> PeerSlot {
        let (cmd_tx, _rx) = mpsc::unbounded_channel();
        let task = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .spawn(async {});
        PeerSlot {
            id,
            addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 18444),
            cmd_tx,
            in_flight: HashSet::new(),
            block_progress_ms: Arc::new(AtomicU64::new(0)),
            peer_height: 5,
            connected_ms: 1,
            first_data_ms: AtomicU64::new(0),
            bytes_rx: AtomicU64::new(0),
            alive: true,
            task,
        }
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
    fn shell(prev: BlockHash, height: u32, n: u32) -> Block {
        let header = Header {
            version: Version::ONE,
            prev_blockhash: prev,
            merkle_root: bitcoin::TxMerkleNode::from_byte_array([0u8; 32]),
            time: 1_300_000_000 + n,
            bits: CompactTarget::from_consensus(0x207fffff),
            nonce: n,
        };
        let mut b = Block {
            header,
            txdata: vec![coinbase(height)],
        };
        b.header.merkle_root = b.compute_merkle_root().unwrap();
        b
    }
    fn ser(b: &Block) -> Vec<u8> {
        let mut v = Vec::new();
        b.consensus_encode(&mut v).unwrap();
        v
    }

    let dir = std::env::temp_dir().join(format!(
        "rbitcoin-ev-block-{}-{}",
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

    let mut st = IbdWorkState::new(vec![dummy_slot(1)], Some(gen), Some(0));
    let write_next = AtomicU32::new(1);
    let mut book = AddrMan::new();
    let local = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)), 1);

    let b1 = shell(gen, 1, 1);
    let h1 = b1.block_hash();
    st.record_height(h1, 1);
    st.header_fks
        .insert(h1, hub.ensure_header_fk(&b1.header).unwrap());
    apply_peer_event(
        &mut st,
        &hub,
        PeerEvent::BlockFramed {
            peer: 1,
            hash: h1,
            payload: ser(&b1),
        },
        &write_next,
        &mut book,
        local,
        None,
    );
    assert!(st.body.is_pending(&h1));
    assert!(hub.query.block_queue_has_height(1));

    apply_peer_event(
        &mut st,
        &hub,
        PeerEvent::BlockFramed {
            peer: 1,
            hash: h1,
            payload: ser(&b1),
        },
        &write_next,
        &mut book,
        local,
        None,
    );
    assert_eq!(hub.query.block_queue_stats().2, 1);

    let far_h = 1u32 + super::super::CONTIG_DENSIFY_AHEAD + 10;
    let far = shell(h1, far_h, far_h);
    let far_hash = far.block_hash();
    st.record_height(far_hash, far_h);
    apply_peer_event(
        &mut st,
        &hub,
        PeerEvent::BlockFramed {
            peer: 1,
            hash: far_hash,
            payload: ser(&far),
        },
        &write_next,
        &mut book,
        local,
        None,
    );
    assert!(!st.body.is_pending(&far_hash));

    st.max_peer_height = 0;
    st.empty_header_streak = 0;
    st.ordered.clear();
    st.ordered_set.clear();
    st.inflight.clear();
    for _ in 0..2 {
        apply_peer_event(
            &mut st,
            &hub,
            PeerEvent::Headers {
                peer: 1,
                headers: vec![],
            },
            &write_next,
            &mut book,
            local,
            None,
        );
    }
    assert!(st.headers_done);

    use super::super::MAX_PEER_POOL;
    for i in 0..MAX_PEER_POOL {
        book.add(SocketAddr::new(
            IpAddr::V4(Ipv4Addr::new(11, 0, (i / 256) as u8, (i % 256) as u8)),
            8333,
        ));
    }
    let n0 = book.len();
    inject_learned_addrs(
        &mut book,
        &[SocketAddr::new(IpAddr::V4(Ipv4Addr::new(9, 9, 9, 9)), 8333)],
        local,
        1,
    );
    assert_eq!(book.len(), n0);

    let (body_tx, mut body_rx) = mpsc::unbounded_channel();
    let (ctrl_tx, mut ctrl_rx) = mpsc::unbounded_channel();
    body_tx
        .send(PeerEvent::BlockDecodeFailed {
            peer: 1,
            hash: h(0x88),
        })
        .unwrap();
    ctrl_tx
        .send(PeerEvent::Addrs {
            peer: 1,
            addrs: vec![SocketAddr::new(IpAddr::V4(Ipv4Addr::new(8, 8, 8, 8)), 8333)],
        })
        .unwrap();
    let stats = super::super::status::LoopStats::default();
    drain_ready_peer_and_archive_events(
        &mut st,
        &hub,
        &mut body_rx,
        &mut ctrl_rx,
        &write_next,
        &stats,
        &mut book,
        local,
        None,
    )
    .unwrap();
    assert!(
        stats
            .drain_events
            .load(std::sync::atomic::Ordering::Relaxed)
            >= 1
    );
    let _ = std::fs::remove_dir_all(dir);
}

/// Single BlockFramed with raw payload offers BQ + notes ConfirmFeed.
#[test]
fn block_framed_raw_offers_body_queue_with_confirm_feed() {
    use super::super::confirm::ConfirmFeed;
    use super::super::peer_io::{PeerEvent, PeerSlot};
    use super::super::state::InflightReq;
    use super::apply_peer_event;
    use crate::seeds::AddrMan;
    use bitcoin::absolute::LockTime;
    use bitcoin::block::{Header, Version};
    use bitcoin::consensus::Encodable;
    use bitcoin::hashes::Hash;
    use bitcoin::script::ScriptBuf;
    use bitcoin::transaction::Version as TxVersion;
    use bitcoin::{
        Amount, Block, BlockHash, CompactTarget, OutPoint, Sequence, Transaction, TxIn, TxOut,
        Witness,
    };
    use rbitcoin_consensus::{ChainParams, Milestone};
    use rbitcoin_query::Query;
    use std::collections::HashSet;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};
    use std::sync::atomic::{AtomicU32, AtomicU64};
    use std::sync::Arc;
    use tokio::sync::mpsc;

    fn dummy_slot(id: usize) -> PeerSlot {
        let (cmd_tx, _rx) = mpsc::unbounded_channel();
        let task = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .spawn(async {});
        PeerSlot {
            id,
            addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 18444),
            cmd_tx,
            in_flight: HashSet::new(),
            block_progress_ms: Arc::new(AtomicU64::new(0)),
            peer_height: 5,
            connected_ms: 1,
            first_data_ms: AtomicU64::new(0),
            bytes_rx: AtomicU64::new(0),
            alive: true,
            task,
        }
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
    fn shell(prev: BlockHash, height: u32, n: u32) -> Block {
        let header = Header {
            version: Version::ONE,
            prev_blockhash: prev,
            merkle_root: bitcoin::TxMerkleNode::from_byte_array([0u8; 32]),
            time: 1_300_000_000 + n,
            bits: CompactTarget::from_consensus(0x207fffff),
            nonce: n,
        };
        let mut b = Block {
            header,
            txdata: vec![coinbase(height)],
        };
        b.header.merkle_root = b.compute_merkle_root().unwrap();
        b
    }

    let dir = std::env::temp_dir().join(format!(
        "rbitcoin-ev-framed-bq-{}-{}",
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

    let mut st = IbdWorkState::new(vec![dummy_slot(1)], Some(gen), Some(0));
    let write_next = AtomicU32::new(1);
    let mut book = AddrMan::new();
    let local = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)), 1);
    let feed = ConfirmFeed::new();

    let b1 = shell(gen, 1, 1);
    let h1 = b1.block_hash();
    st.record_height(h1, 1);
    st.header_fks
        .insert(h1, hub.ensure_header_fk(&b1.header).unwrap());
    st.slots[0].in_flight.insert(h1);
    st.inflight.insert(h1, InflightReq::new(1));

    let mut payload = Vec::new();
    b1.consensus_encode(&mut payload).unwrap();
    apply_peer_event(
        &mut st,
        &hub,
        PeerEvent::BlockFramed {
            peer: 1,
            hash: h1,
            payload,
        },
        &write_next,
        &mut book,
        local,
        Some(&feed),
    );
    assert!(st.body.is_pending(&h1));
    assert!(hub.query.block_queue_has_height(1));
    assert_eq!(feed.size_snap().0, 1);
    assert!(st.inflight.is_empty());

    let mut payload2 = Vec::new();
    b1.consensus_encode(&mut payload2).unwrap();
    apply_peer_event(
        &mut st,
        &hub,
        PeerEvent::BlockFramed {
            peer: 1,
            hash: h1,
            payload: payload2,
        },
        &write_next,
        &mut book,
        local,
        Some(&feed),
    );
    assert_eq!(hub.query.block_queue_stats().2, 1);
    assert_eq!(feed.size_snap().0, 1);

    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn known_headers_re_admit_to_ordered_after_tip_drain() {
    use super::super::peer_io::{PeerEvent, PeerSlot};
    use super::apply_peer_event;
    use crate::seeds::AddrMan;
    use bitcoin::block::{Header, Version};
    use bitcoin::hashes::Hash;
    use bitcoin::{BlockHash, CompactTarget};
    use rbitcoin_consensus::{ChainParams, Milestone};
    use rbitcoin_query::Query;
    use std::collections::HashSet;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};
    use std::sync::atomic::{AtomicU32, AtomicU64};
    use std::sync::Arc;
    use tokio::sync::mpsc;

    fn dummy_slot(id: usize) -> PeerSlot {
        let (cmd_tx, _rx) = mpsc::unbounded_channel();
        let task = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .spawn(async {});
        PeerSlot {
            id,
            addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 18444),
            cmd_tx,
            in_flight: HashSet::new(),
            block_progress_ms: Arc::new(AtomicU64::new(0)),
            peer_height: 5,
            connected_ms: 1,
            first_data_ms: AtomicU64::new(0),
            bytes_rx: AtomicU64::new(0),
            alive: true,
            task,
        }
    }
    fn dummy_header(prev: BlockHash, n: u32) -> Header {
        Header {
            version: Version::ONE,
            prev_blockhash: prev,
            merkle_root: bitcoin::TxMerkleNode::from_byte_array([n as u8; 32]),
            time: 1_300_000_000 + n,
            bits: CompactTarget::from_consensus(0x207fffff),
            nonce: n,
        }
    }

    let dir = std::env::temp_dir().join(format!(
        "rbitcoin-ev-readmit-{}-{}",
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

    let mut st = IbdWorkState::new(vec![dummy_slot(1)], Some(gen), Some(0));
    let write_next = AtomicU32::new(1);
    let mut book = AddrMan::new();
    let local = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)), 1);

    let hdr = dummy_header(gen, 1);
    let hash = hdr.block_hash();
    apply_peer_event(
        &mut st,
        &hub,
        PeerEvent::Headers {
            peer: 1,
            headers: vec![hdr.clone()],
        },
        &write_next,
        &mut book,
        local,
        None,
    );
    assert!(st.ordered_set.contains(&hash), "first admit");
    assert_eq!(st.hash_height.get(&hash), Some(&1));

    // Tip-drain shape: drop ordered membership; keep known + height (hygiene
    // now also retains those — simulate post-confirm empty path).
    st.ordered.clear();
    st.ordered_set.clear();
    st.height_to_hash.clear();
    assert!(st.known_headers.contains(&hash));
    assert!(st.ordered.is_empty());

    // Peer re-serves the same window (overlap). Must re-admit.
    apply_peer_event(
        &mut st,
        &hub,
        PeerEvent::Headers {
            peer: 1,
            headers: vec![hdr],
        },
        &write_next,
        &mut book,
        local,
        None,
    );
    assert!(
        st.ordered_set.contains(&hash),
        "known header must re-enter ordered after tip drain"
    );
    assert_eq!(st.ordered.len(), 1);

    let _ = std::fs::remove_dir_all(dir);
}
