//! Core functional analogs (inventory `analog=`).
//!
//! Unmodified Core scripts that touch LevelDB / `blocks/` / assumevalid logs
//! cannot `run`. These scenarios keep the behavior we still want:
//!
//! 1. `--milestone` skip-below / check-above + mempool persist + leftover
//!    pool through catch-up then tip-mode purge, then missing prevout still
//!    fails when scripts are skipped (`feature_assumevalid.py`,
//!    `mempool_persist.py`)
//! 2. Reconstruct height 1 after wiping `tx.head/` (`feature_reindex*.py`)

use bitcoin::hashes::Hash;
use bitcoin::{
    Amount, BlockHash, OutPoint, ScriptBuf, Sequence, Transaction, TxIn, TxOut, Witness,
};
use rbitcoin_consensus::{
    accept_and_connect_block, grind_regtest_pow, ChainParams, ConsensusError, Milestone,
};
use rbitcoin_net::MempoolHub;
use rbitcoin_primitives::Height;
use rbitcoin_query::Query;
use rbitcoin_test::mine::{mine_regtest_block, regtest_genesis, spend_anyone_can_spend};
use rbitcoin_test::{
    assert_reconstruct_eq, build_mature_regtest_with_spend, pad_empty_from, MatureRegtestChain,
    TestDatadir,
};
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// One mature pad: mempool persist, restart leftover through catch-up then
/// tip-mode purge, then `--milestone` skip-below / check-above, then missing
/// prevout still fails under a high milestone (scripts skipped).
///
/// Core `feature_assumevalid.py` + `mempool_persist.py`.
#[test]
fn analog_milestone_and_mempool_persist() {
    let params = ChainParams::regtest();
    let td = TestDatadir::new().unwrap();
    let q = Query::open_or_create_tiny(td.store_path()).unwrap();
    let chain = build_mature_regtest_with_spend(&q, &params);

    let spend_block = &chain.blocks[chain.spend_height as usize];
    let spend_txid = spend_block.txdata[1].compute_txid();
    let unconf = spend_anyone_can_spend(spend_txid, 0, Amount::from_sat(48_0000_0000));
    let want = unconf.compute_txid();

    let mp_dir = td.path().join("mempool");
    let q_arc = Arc::new(q);
    {
        let hub = MempoolHub::open_with_weight(&mp_dir, Arc::clone(&q_arc), 50_000_000).unwrap();
        hub.set_relay_enabled(true);
        let r =
            hub.accept_tx(&unconf).expect("accept unconfirmed spend of confirmed anyone-can-spend");
        assert_eq!(r.txid, want);
        hub.flush().expect("SIGTERM-equivalent flush");
        assert!(hub.contains(&want));
    }
    let hub2 = MempoolHub::open_with_weight(&mp_dir, Arc::clone(&q_arc), 50_000_000).unwrap();
    assert!(hub2.contains(&want), "flushed mempool must still hold the tx after reopen");
    assert_eq!(hub2.live_count(), 1);
    drop(hub2);
    let (tip, tip_time, h) =
        pin_restart_catchup_then_tip_purge(&mp_dir, &q_arc, &params, &chain, &want);
    pin_leftover_slots_tmp_and_truncated_body(&mp_dir, &q_arc, &want);

    let q = q_arc.as_ref();
    let mut bad = spend_anyone_can_spend(spend_txid, 0, Amount::from_sat(47_0000_0000));
    bad.input[0].script_sig = ScriptBuf::from_bytes(vec![0x6a]);

    let catchup_tip = h - 1;
    let bad_block = mine_regtest_block(tip, tip_time + 600, h, vec![bad]);

    let ms_skip = Milestone { height: h };
    let ms_check = Milestone { height: h - 1 };
    assert!(ms_skip.skips_scripts_at(h));
    assert!(!ms_check.skips_scripts_at(h));

    let err = accept_and_connect_block(q, &params, Height(h), &bad_block, ms_check)
        .expect_err("invalid script above milestone must fail");
    let msg = err.to_string().to_lowercase();
    assert!(
        msg.contains("script") || msg.contains("opcode") || msg.contains("return"),
        "expected script failure above milestone, got: {err}"
    );
    assert_eq!(q.tip_height(), Some(Height(catchup_tip)));

    accept_and_connect_block(q, &params, Height(h), &bad_block, ms_skip)
        .expect("invalid script below milestone must be skipped");
    assert_eq!(q.tip_height(), Some(Height(h)));

    let ms_hi = Milestone { height: 1_000_000 };
    let mut phantom =
        mine_regtest_block(bad_block.block_hash(), bad_block.header.time + 600, h + 1, vec![]);
    phantom.txdata.push(Transaction {
        version: bitcoin::transaction::Version::TWO,
        lock_time: bitcoin::absolute::LockTime::ZERO,
        input: vec![TxIn {
            previous_output: OutPoint { txid: bitcoin::Txid::from_byte_array([0xcd; 32]), vout: 0 },
            script_sig: ScriptBuf::new(),
            sequence: Sequence::MAX,
            witness: Witness::new(),
        }],
        output: vec![TxOut { value: Amount::from_sat(1), script_pubkey: ScriptBuf::new() }],
    });
    phantom.header.merkle_root = phantom.compute_merkle_root().unwrap();
    grind_regtest_pow(&mut phantom.header);
    let err = accept_and_connect_block(q, &params, Height(h + 1), &phantom, ms_hi)
        .expect_err("prevout must fail");
    assert!(
        matches!(
            err,
            ConsensusError::MissingPrevout
                | ConsensusError::BadTx("bad-txns-inputs-missingorspent")
        ),
        "expected missing-prevout class under milestone, got: {err}"
    );
}

/// Restart with a leftover pool, connect catch-up (relay off), then tip-mode
/// `set_relay_enabled(true)`: same-txid confirmed gone, input-conflict gone,
/// child of a now-confirmed parent kept, DEAD marks durable without `flush`.
fn pin_restart_catchup_then_tip_purge(
    mp_dir: &Path,
    q: &Arc<Query>,
    params: &ChainParams,
    chain: &MatureRegtestChain,
    leftover: &bitcoin::Txid,
) -> (BlockHash, u32, u32) {
    let (pad_tip, pad_time) = pad_empty_from(
        q.as_ref(),
        params,
        chain.tip_hash(),
        chain.blocks.last().unwrap().header.time,
        chain.spend_height + 1,
        chain.spend_height + 5,
    );
    let spend_cb = |h: usize, sats: u64| {
        spend_anyone_can_spend(chain.blocks[h].txdata[0].compute_txid(), 0, Amount::from_sat(sats))
    };
    let same = spend_cb(2, 49_0000_0000);
    let loser = spend_cb(3, 48_0000_0000);
    let winner = spend_cb(3, 47_0000_0000);
    let parent = spend_cb(4, 49_0000_0000);
    let child = spend_anyone_can_spend(parent.compute_txid(), 0, Amount::from_sat(48_0000_0000));
    let extras = [spend_cb(5, 49_0000_0000), spend_cb(6, 49_0000_0000), spend_cb(7, 49_0000_0000)];
    let same_id = same.compute_txid();
    let loser_id = loser.compute_txid();
    let parent_id = parent.compute_txid();
    let child_id = child.compute_txid();
    let extra_ids: Vec<_> = extras.iter().map(|t| t.compute_txid()).collect();
    assert_ne!(loser_id, winner.compute_txid());

    let h = chain.spend_height + 6;
    let blk =
        mine_regtest_block(pad_tip, pad_time + 600, h, vec![same.clone(), winner, parent.clone()]);
    {
        let hub = MempoolHub::open_with_weight(mp_dir, Arc::clone(q), 50_000_000).unwrap();
        hub.set_relay_enabled(true);
        hub.accept_tx(&same).expect("accept same-txid leftover");
        hub.accept_tx(&loser).expect("accept conflict loser");
        hub.accept_tx(&parent).expect("accept parent");
        hub.accept_tx(&child).expect("accept child of parent");
        for tx in &extras {
            hub.accept_tx(tx).expect("accept ballast leftover");
        }
        hub.flush().expect("persist leftover pool");
        hub.set_relay_enabled(false);
        let _ = q.confirm_stats().take_window();
        accept_and_connect_block(q.as_ref(), params, Height(h), &blk, Milestone::NONE)
            .expect("catch-up connect");
        let w = q.confirm_stats().take_window();
        assert_eq!(
            w.arch_write_spend_ns, 0,
            "Class A must not put_spend_batch; spentness is post_commit abs-meta"
        );
        assert_eq!(
            w.fill_missing_n, 0,
            "lookup stamp already bound parent loc; finish must not fill_missing"
        );
        let same_coin = chain.blocks[2].txdata[0].compute_txid();
        let parent_coin = chain.blocks[4].txdata[0].compute_txid();
        assert!(
            q.is_outpoint_spent(same_coin.as_byte_array(), 0).unwrap(),
            "same-txid leftover parent coin must be confirmed-spent after connect"
        );
        assert!(
            q.is_outpoint_spent(parent_coin.as_byte_array(), 0).unwrap(),
            "confirmed parent leftover coin must be confirmed-spent after connect"
        );
        assert!(
            hub.contains(&same_id) && hub.contains(&loser_id) && hub.contains(&parent_id),
            "relay off must leave confirmed and conflicted txs in the leftover pool"
        );
        assert!(hub.contains(&child_id) && hub.contains(leftover));
    }

    {
        let hub = MempoolHub::open_with_weight(mp_dir, Arc::clone(q), 50_000_000).unwrap();
        assert!(
            hub.contains(&same_id) && hub.contains(&loser_id) && hub.contains(&parent_id),
            "reopen after catch-up must load leftover txs before tip-mode purge"
        );
        hub.set_relay_enabled(true);
        assert!(!hub.contains(&same_id), "same-txid confirmed leftover must drop at relay-on");
        assert!(!hub.contains(&loser_id), "input-conflict leftover must drop at relay-on");
        assert!(!hub.contains(&parent_id), "confirmed parent leftover must drop at relay-on");
        assert!(hub.contains(&child_id), "child of a now-confirmed parent must stay");
        assert!(hub.contains(leftover), "unrelated leftover must stay");
        for id in &extra_ids {
            assert!(hub.contains(id), "ballast leftover must stay (no compact)");
        }
    }

    {
        let hub = MempoolHub::open_with_weight(mp_dir, Arc::clone(q), 50_000_000).unwrap();
        assert!(
            !hub.contains(&same_id) && !hub.contains(&loser_id) && !hub.contains(&parent_id),
            "purge DEAD marks must survive drop without flush"
        );
        assert!(hub.contains(&child_id) && hub.contains(leftover));
        hub.set_relay_enabled(true);
        let mut strip = extra_ids;
        strip.push(child_id);
        assert_eq!(hub.remove_for_block(&strip), strip.len());
        hub.flush().expect("restore singleton leftover for slots.tmp pin");
    }

    (blk.block_hash(), blk.header.time, h + 1)
}

fn pin_leftover_slots_tmp_and_truncated_body(mp_dir: &Path, q: &Arc<Query>, want: &bitcoin::Txid) {
    std::fs::copy(mp_dir.join("slots"), mp_dir.join("slots.tmp")).unwrap();
    assert!(mp_dir.join("slots.tmp").exists());
    let hub = MempoolHub::open_with_weight(mp_dir, Arc::clone(q), 50_000_000)
        .expect("open finishes leftover slots.tmp");
    assert!(!mp_dir.join("slots.tmp").exists(), "leftover slots.tmp must be renamed away");
    assert_eq!(hub.live_count(), 1);
    assert!(hub.contains(want));
    drop(hub);

    let body = mp_dir.join("tx.body");
    let bytes = std::fs::read(&body).unwrap();
    assert!(bytes.len() > 1, "flushed body");
    std::fs::write(&body, &bytes[..bytes.len() / 2]).unwrap();
    let err = match MempoolHub::open_with_weight(mp_dir, Arc::clone(q), 50_000_000) {
        Ok(_) => panic!("truncated body vs slots must refuse"),
        Err(e) => e,
    };
    assert!(
        err.to_lowercase().contains("corrupt")
            || err.to_lowercase().contains("slot")
            || err.to_lowercase().contains("body")
            || err.to_lowercase().contains("range"),
        "expected disagree refuse, got {err}"
    );
}

fn first_head_sidecar(head: &Path, ext: &str) -> PathBuf {
    for ent in std::fs::read_dir(head).unwrap() {
        let p = ent.unwrap().path();
        if p.extension().and_then(|e| e.to_str()) == Some(ext) {
            return p;
        }
    }
    panic!("no *.{ext} under {}", head.display());
}

fn assert_query_open_refuses(store: &Path, needle: &str) {
    let err = match Query::open_or_create_tiny(store) {
        Ok(_) => panic!("Query::open must refuse ({needle})"),
        Err(e) => e,
    };
    let msg = err.to_string();
    assert!(msg.contains(needle), "expected {needle:?} in {msg}");
    assert!(store.join("txout.body").is_file(), "Class A kept after {needle} refuse");
}

fn assert_query_rebuilds_from_class_a(store: &Path, b1: &bitcoin::Block, cb_txid: &[u8; 32]) {
    let q = Query::open_or_create_tiny(store).expect("torn current tx.head rebuilds from Class A");
    assert_eq!(q.tip_height(), Some(Height(2)));
    assert!(q.tx_head_occupied() >= 3, "open must rebuild tx.head from Class A bodies");
    assert!(q.get_tx_by_txid(cb_txid).unwrap().is_some(), "txid must resolve after head rebuild");
    assert_reconstruct_eq(&q, 1, b1);
}

/// Archive reconstruct of height 1 after dropping RAM and wiping `tx.head/`
/// (`feature_reindex*.py` / operator delete-head reopen). Same pad: crash-open
/// clamps an unsealed tip; leftover fuse8 v1 refuses; truncated MPHF / empty
/// meta rebuild from Class A.
#[test]
fn analog_reconstruct_after_lost_head() {
    let td = TestDatadir::new().unwrap();
    let params = ChainParams::regtest();
    let genesis = regtest_genesis();
    let store = td.store_path();
    let b1;
    let b2;
    let cb_txid;
    let b3_cb;
    {
        let q = Query::open_or_create_tiny(&store).unwrap();
        accept_and_connect_block(&q, &params, Height::GENESIS, &genesis, Milestone::NONE).unwrap();
        b1 = mine_regtest_block(genesis.block_hash(), genesis.header.time + 600, 1, vec![]);
        accept_and_connect_block(&q, &params, Height(1), &b1, Milestone::NONE).unwrap();
        b2 = mine_regtest_block(b1.block_hash(), b1.header.time + 600, 2, vec![]);
        accept_and_connect_block(&q, &params, Height(2), &b2, Milestone::NONE).unwrap();
        q.flush().unwrap();
        assert_eq!(q.tip_height(), Some(Height(2)));
        cb_txid = b1.txdata[0].compute_txid().to_byte_array();
        let seal_path = store.join("tip_seal");
        let seal = std::fs::read(&seal_path).expect("tip_seal after complete barrier");
        let b3 = mine_regtest_block(b2.block_hash(), b2.header.time + 600, 3, vec![]);
        accept_and_connect_block(&q, &params, Height(3), &b3, Milestone::NONE).unwrap();
        assert_eq!(q.tip_height(), Some(Height(3)));
        b3_cb = b3.txdata[0].compute_txid().to_byte_array();
        std::fs::write(&seal_path, seal).expect("restore pre-height-3 seal");
    }

    let q_clamp = Query::open_or_create_tiny(&store).unwrap();
    assert_eq!(q_clamp.tip_height(), Some(Height(2)));
    let view = q_clamp.pin_chain_view().unwrap().expect("Electrum/RPC chain_tip after crash-open");
    assert_eq!(view.height, Height(2));
    assert_eq!(view.hash, b2.block_hash().to_byte_array());
    let b3_fk = q_clamp.get_tx_by_txid(&b3_cb).unwrap().expect("height-3 Class A kept").0;
    assert!(
        !q_clamp.store().is_confirmed_strong(b3_fk).unwrap(),
        "leftover strong above clamped tip must not be confirmed"
    );
    drop(q_clamp);

    let head = store.join("tx.head");
    assert!(head.is_dir(), "tiny store writes segmented tx.head/");
    std::fs::remove_dir_all(&head).expect("wipe tx.head");

    let q2 = Query::open_or_create_tiny(&store).unwrap();
    assert_eq!(q2.tip_height(), Some(Height(2)));
    assert!(q2.tx_head_occupied() >= 3, "open must rebuild tx.head from Class A bodies");
    assert!(q2.get_tx_by_txid(&cb_txid).unwrap().is_some(), "txid must resolve after head rebuild");
    assert_reconstruct_eq(&q2, 1, &b1);
    let rec = q2
        .reconstruct_block_at_height(Height(1))
        .expect("reconstruct height 1 after wiped tx.head");
    assert_eq!(rec.block_hash(), b1.block_hash());
    drop(q2);

    let fuse = first_head_sidecar(&head, "fuse8");
    let fuse_ok = std::fs::read(&fuse).unwrap();
    let mut v1 = Vec::from(*b"BF8R");
    v1.extend_from_slice(&1u32.to_le_bytes());
    v1.extend_from_slice(&0u64.to_le_bytes());
    std::fs::write(&fuse, &v1).unwrap();
    assert_query_open_refuses(&store, "fuse8 v1");
    std::fs::write(&fuse, fuse_ok).unwrap();

    let mphf = first_head_sidecar(&head, "mphf");
    let mphf_ok = std::fs::read(&mphf).unwrap();
    std::fs::write(&mphf, &mphf_ok[..8.min(mphf_ok.len())]).unwrap();
    assert_query_rebuilds_from_class_a(&store, &b1, &cb_txid);

    std::fs::write(head.join("meta"), []).unwrap();
    assert_query_rebuilds_from_class_a(&store, &b1, &cb_txid);
}
