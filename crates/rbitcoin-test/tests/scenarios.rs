//! High-level functional scenarios (coverage-bearing).
//!
//! Prefer fewer tests at the highest layer that still hit production paths.
//! Lower-level store decode/corruption cases stay only when nothing else covers them.

use bitcoin::hashes::Hash;
use bitcoin::{Amount, BlockHash};
use rbitcoin_cli::cli_main as cli_cli_main;
use rbitcoin_consensus::{
    accept_and_connect_block, validate_block_structure, ChainParams, ConsensusError, Milestone,
    ValidationContext,
};
use rbitcoin_net::outbound_for_ibd;
use rbitcoin_node::{cli_main as node_cli_main, run_node, NodeConfig};
use rbitcoin_primitives::{Fk, Height, Network, TableKind, VERSION};
use rbitcoin_query::Query;
use rbitcoin_rpc::node_rpc_path;
use rbitcoin_store::{HeaderRecord, Store, StoreError, TxRecord};
use rbitcoin_test::mine::{mine_regtest_block, regtest_genesis, spend_anyone_can_spend};
use rbitcoin_test::{smoke_crate_names, TestDatadir};
use rbitcoin_wire_cache::WireRing;
use std::process::{Command, ExitCode};

// ─── Node / CLI lifecycle ───────────────────────────────────────────────────

#[test]
fn node_lifecycle_and_networks() {
    for net in [
        Network::Mainnet,
        Network::Testnet,
        Network::Signet,
        Network::Regtest,
    ] {
        let td = TestDatadir::new().unwrap();
        let cfg = NodeConfig::default()
            .with_datadir(td.path())
            .with_network(net);
        let handle = run_node(cfg).unwrap();
        assert_eq!(handle.network_name(), net.as_str());
        handle.shutdown().unwrap();
    }
    assert!(Network::parse("nope").is_err());
    assert_eq!(Network::parse("REGTEST").unwrap(), Network::Regtest);
    assert!(!VERSION.is_empty());
    for k in 1u16..=10 {
        assert_eq!(TableKind::from_u16(k).unwrap().as_u16(), k);
    }
    assert!(TableKind::from_u16(99).is_none());
    assert!(Fk::NULL.is_null());
    assert_eq!(Height::GENESIS.next(), Some(Height(1)));
}

#[test]
fn node_config_errors() {
    let cfg = NodeConfig {
        datadir: std::path::PathBuf::from(""),
        ..NodeConfig::default()
    };
    assert!(run_node(cfg).is_err());

    let td = TestDatadir::new().unwrap();
    let file = td.path().join("blocked");
    std::fs::write(&file, b"nope").unwrap();
    assert!(run_node(NodeConfig::default().with_datadir(file)).is_err());

    let cfg = NodeConfig {
        wire_depth_blocks: 0,
        archive_durability: true,
        ..NodeConfig::default().with_datadir(td.path().join("w0"))
    };
    let h = run_node(cfg).unwrap();
    assert_eq!(h.wire.depth(), 0);
    h.shutdown().unwrap();
}

#[test]
fn cli_and_node_entrypoints() {
    let td = TestDatadir::new().unwrap();
    for net in ["mainnet", "testnet", "signet", "regtest"] {
        let d = td.path().join(net);
        assert_eq!(
            node_cli_main([
                "rbitcoin-node",
                "--datadir",
                d.to_str().unwrap(),
                "--network",
                net,
                "--smoke",
            ]),
            ExitCode::SUCCESS
        );
    }
    let _ = node_cli_main(["rbitcoin-node", "--help"]);
    let _ = node_cli_main(["rbitcoin-node", "--version"]);
    let _ = cli_cli_main(["rbitcoin-cli", "--help"]);
    let _ = cli_cli_main(["rbitcoin-cli", "--version"]);
    assert_eq!(cli_cli_main(["rbitcoin-cli", "help"]), ExitCode::SUCCESS);
    assert_ne!(cli_cli_main(["rbitcoin-cli"]), ExitCode::SUCCESS);
    assert_ne!(
        cli_cli_main(["rbitcoin-cli", "getblockchaininfo"]),
        ExitCode::SUCCESS
    );
    assert_ne!(
        node_cli_main(["rbitcoin-node", "--not-a-real-option"]),
        ExitCode::SUCCESS
    );
    assert_ne!(
        node_cli_main(["rbitcoin-node", "--datadir"]),
        ExitCode::SUCCESS
    );
    assert_ne!(
        node_cli_main(["rbitcoin-node", "--network", "nope"]),
        ExitCode::SUCCESS
    );
    assert_ne!(cli_cli_main(["rbitcoin-cli", "a", "b"]), ExitCode::SUCCESS);

    std::env::set_var("RBITCOIN_TEST_DROP_STORE", "1");
    assert_ne!(
        node_cli_main([
            "rbitcoin-node",
            "--datadir",
            td.path().join("shutdown-fail").to_str().unwrap(),
            "--smoke",
        ]),
        ExitCode::SUCCESS
    );
    std::env::remove_var("RBITCOIN_TEST_DROP_STORE");

    let node = workspace_bin("rbitcoin-node");
    if node.exists() {
        assert!(Command::new(&node)
            .args([
                "--datadir",
                td.path().join("bin-smoke").to_str().unwrap(),
                "--network",
                "regtest",
                "--smoke",
            ])
            .status()
            .unwrap()
            .success());
    }
}

fn workspace_bin(name: &str) -> std::path::PathBuf {
    let profile = if cfg!(debug_assertions) {
        "debug"
    } else {
        "release"
    };
    let mut p = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    p.push("../../target");
    p.push(profile);
    p.push(name);
    p
}

#[test]
fn placeholder_surfaces() {
    let names = smoke_crate_names();
    assert!(names.contains(&"rbitcoin-store"));
    assert!(names.contains(&"rbitcoin-consensus"));
    let ring = WireRing::new(100);
    assert!(ring.is_empty());
    assert!(!Milestone::NONE.skips_at(0));
    assert!(Milestone { height: 10 }.skips_at(5));
    assert_eq!(outbound_for_ibd(true), 100);
    assert_eq!(node_rpc_path(), "/");
}

// ─── Store: keep only corrupt/error paths not hit by consensus chain tests ──

#[test]
fn store_error_and_corrupt_paths() {
    let td = TestDatadir::new().unwrap();
    let path = td.store_path();
    let s = Store::create(&path).unwrap();
    assert!(matches!(s.get_header(Fk::NULL), Err(StoreError::InvalidFk)));
    assert!(matches!(s.get_header(Fk(99)), Err(StoreError::NotFound)));
    assert!(matches!(
        s.put_spend(&[0u8; 32], 0, Fk::NULL, 0),
        Err(StoreError::InvalidFk)
    ));
    let _ = format!("{}", StoreError::BadMagic);
    let _ = format!("{}", StoreError::BadSchema(3));
    let _ = format!(
        "{}",
        StoreError::BadKind {
            expected: 1,
            got: 2
        }
    );
    let _ = format!("{}", StoreError::NotFound);
    let _ = format!("{}", StoreError::InvalidFk);
    let _ = format!("{}", StoreError::Corrupt("x"));
    let _ = format!("{}", StoreError::NotDirectory(path.clone()));
    drop(s);

    let file_path = td.path().join("notdir");
    std::fs::write(&file_path, b"x").unwrap();
    assert!(matches!(
        Store::create(&file_path),
        Err(StoreError::NotDirectory(_))
    ));

    let bad = td.path().join("badstore");
    std::fs::create_dir_all(&bad).unwrap();
    std::fs::write(bad.join("meta"), b"XXXX\x00\x00").unwrap();
    assert!(matches!(Store::open(&bad), Err(StoreError::BadMagic)));

    let bad2 = td.path().join("badschema");
    std::fs::create_dir_all(&bad2).unwrap();
    let mut meta = Vec::from(*b"RBT1");
    meta.extend_from_slice(&99u16.to_le_bytes());
    std::fs::write(bad2.join("meta"), meta).unwrap();
    assert!(matches!(Store::open(&bad2), Err(StoreError::BadSchema(99))));

    let bad3 = td.path().join("shortmeta");
    std::fs::create_dir_all(&bad3).unwrap();
    std::fs::write(bad3.join("meta"), b"RB").unwrap();
    assert!(matches!(Store::open(&bad3), Err(StoreError::Corrupt(_))));

    let parent_file = td.path().join("parent_is_file");
    std::fs::write(&parent_file, b"x").unwrap();
    assert!(Store::create(parent_file.join("store")).is_err());

    assert!(HeaderRecord::decode(&[0u8; 10]).is_err());
    assert!(TxRecord::decode(&[0u8; 10]).is_err());
}

#[test]
fn store_table_header_and_idx_corrupt() {
    use rbitcoin_primitives::{TableKind, SCHEMA_VERSION, STORE_MAGIC};
    let td = TestDatadir::new().unwrap();
    let store_dir = td.path().join("broken_kind");
    {
        let s = Store::create(&store_dir).unwrap();
        s.flush().unwrap();
    }
    let mut hb = std::fs::read(store_dir.join("header.body")).unwrap();
    hb[6..8].copy_from_slice(&TableKind::Tx.as_u16().to_le_bytes());
    std::fs::write(store_dir.join("header.body"), &hb).unwrap();
    match Store::open(&store_dir) {
        Err(StoreError::BadKind { .. }) => {}
        Err(e) => panic!("expected BadKind, got {e}"),
        Ok(_) => panic!("expected BadKind"),
    }

    let store_dir2 = td.path().join("broken_magic");
    {
        Store::create(&store_dir2).unwrap().flush().unwrap();
    }
    let mut hb = std::fs::read(store_dir2.join("header.body")).unwrap();
    hb[0..4].copy_from_slice(b"XXXX");
    std::fs::write(store_dir2.join("header.body"), &hb).unwrap();
    match Store::open(&store_dir2) {
        Err(StoreError::BadMagic) => {}
        Err(e) => panic!("expected BadMagic, got {e}"),
        Ok(_) => panic!("expected BadMagic"),
    }

    let store_dir3 = td.path().join("broken_schema");
    {
        Store::create(&store_dir3).unwrap().flush().unwrap();
    }
    let mut hb = std::fs::read(store_dir3.join("header.body")).unwrap();
    hb[4..6].copy_from_slice(&123u16.to_le_bytes());
    std::fs::write(store_dir3.join("header.body"), &hb).unwrap();
    match Store::open(&store_dir3) {
        Err(StoreError::BadSchema(123)) => {}
        Err(e) => panic!("expected BadSchema, got {e}"),
        Ok(_) => panic!("expected BadSchema"),
    }

    // Hash head empty body / not power-of-two
    let sd = td.path().join("empty_head");
    {
        Store::create(&sd).unwrap().flush().unwrap();
    }
    let head = sd.join("header.head");
    let mut bytes = std::fs::read(&head).unwrap();
    bytes[8..16].copy_from_slice(&16u64.to_le_bytes());
    bytes.truncate(16);
    std::fs::write(&head, bytes).unwrap();
    match Store::open(&sd) {
        Err(StoreError::Corrupt(_)) => {}
        Err(e) => panic!("expected Corrupt, got {e}"),
        Ok(_) => panic!("expected Corrupt"),
    }

    let sd2 = td.path().join("bad_slots");
    {
        Store::create(&sd2).unwrap().flush().unwrap();
    }
    let head = sd2.join("header.head");
    let mut bytes = std::fs::read(&head).unwrap();
    let logical = 16u64 + 40 * 3;
    bytes.resize(logical as usize, 0);
    bytes[8..16].copy_from_slice(&logical.to_le_bytes());
    std::fs::write(&head, bytes).unwrap();
    match Store::open(&sd2) {
        Err(StoreError::Corrupt(_)) => {}
        Err(e) => panic!("expected Corrupt, got {e}"),
        Ok(_) => panic!("expected Corrupt"),
    }

    let _ = (SCHEMA_VERSION, STORE_MAGIC);
}

// ─── Phase 1 chain ops (still high-level, pre-consensus types) ──────────────

#[test]
fn chain_connect_reorg_and_growth() {
    use rbitcoin_query::TxApply;
    use rbitcoin_store::{InputRecord, OutputRecord};

    let td = TestDatadir::new().unwrap();
    let q = Query::open_or_create(td.store_path()).unwrap();

    // Growth + many blocks
    let mut prev = Fk::NULL;
    for h in 0..120u32 {
        let mut hash = [0u8; 32];
        hash[0..4].copy_from_slice(&h.to_le_bytes());
        let header = HeaderRecord {
            prev_fk: prev,
            version: 1,
            timestamp: h,
            bits: 1,
            nonce: h,
            merkle_root: hash,
            hash,
        };
        let mut txid = [0u8; 32];
        txid[0..4].copy_from_slice(&h.to_le_bytes());
        txid[31] = 0xcb;
        let ta = TxApply {
            tx: TxRecord {
                txid,
                version: 1,
                locktime: 0,
                input_start_fk: Fk::NULL,
                input_count: 0,
                output_start_fk: Fk::NULL,
                output_count: 0,
                raw: vec![h as u8],
            },
            inputs: vec![InputRecord {
                parent_tx_fk: Fk::NULL,
                index: 0,
                prev_txid: [0u8; 32],
                prev_index: u32::MAX,
                sequence: u32::MAX,
                script_sig: vec![0],
            }],
            outputs: vec![OutputRecord {
                parent_tx_fk: Fk::NULL,
                index: 0,
                value: 50_0000_0000,
                script: vec![0x51],
            }],
        };
        prev = q.connect_block(Height(h), &header, &[ta]).unwrap();
    }
    assert_eq!(q.tip_height(), Some(Height(119)));
    q.flush().unwrap();
    drop(q);

    let q = Query::open_or_create(td.store_path()).unwrap();
    assert_eq!(q.tip_height(), Some(Height(119)));
    q.disconnect_tip().unwrap();
    assert_eq!(q.tip_height(), Some(Height(118)));
    assert!(q.connect_block(Height(0), &HeaderRecord {
        prev_fk: Fk::NULL,
        version: 1,
        timestamp: 0,
        bits: 1,
        nonce: 0,
        merkle_root: [0; 32],
        hash: [1; 32],
    }, &[]).is_err());
}

// ─── Phase 2: rust-bitcoin consensus ────────────────────────────────────────

#[test]
fn consensus_regtest_genesis_and_mine_chain() {
    let td = TestDatadir::new().unwrap();
    let q = Query::open_or_create(td.store_path()).unwrap();
    let params = ChainParams::regtest();
    let milestone = Milestone::NONE;

    let genesis = regtest_genesis();
    accept_and_connect_block(&q, &params, Height::GENESIS, &genesis, milestone).unwrap();
    assert_eq!(q.tip_height(), Some(Height::GENESIS));
    assert_eq!(
        q.header_at_height(Height::GENESIS)
            .unwrap()
            .unwrap()
            .1
            .hash,
        genesis.block_hash().to_byte_array()
    );

    let mut tip = genesis.block_hash();
    let mut tip_time = genesis.header.time;
    for h in 1..=20u32 {
        let block = mine_regtest_block(tip, tip_time + 600, h, vec![]);
        accept_and_connect_block(&q, &params, Height(h), &block, milestone).unwrap();
        tip = block.block_hash();
        tip_time = block.header.time;
    }
    assert_eq!(q.tip_height(), Some(Height(20)));
    q.flush().unwrap();

    let q2 = Query::open_or_create(td.store_path()).unwrap();
    assert_eq!(q2.tip_height(), Some(Height(20)));
}

#[test]
fn consensus_reject_bad_pow_and_merkle() {
    let td = TestDatadir::new().unwrap();
    let q = Query::open_or_create(td.store_path()).unwrap();
    let params = ChainParams::regtest();
    let genesis = regtest_genesis();
    accept_and_connect_block(&q, &params, Height::GENESIS, &genesis, Milestone::NONE).unwrap();

    let mut block = mine_regtest_block(genesis.block_hash(), genesis.header.time + 1, 1, vec![]);
    // Break merkle root
    block.header.merkle_root = bitcoin::TxMerkleNode::from_byte_array([0x11; 32]);
    let ctx = ValidationContext {
        params: &params,
        height: Height(1),
        milestone: Milestone::NONE,
    };
    assert!(matches!(
        validate_block_structure(&block, &ctx),
        Err(ConsensusError::BadBlock(_))
    ));

    // Valid structure but wrong prev → header fails
    let block2 = mine_regtest_block(
        BlockHash::from_byte_array([0x22; 32]),
        genesis.header.time + 2,
        1,
        vec![],
    );
    assert!(accept_and_connect_block(&q, &params, Height(1), &block2, Milestone::NONE).is_err());
}

#[test]
fn consensus_spend_and_reject_double_spend() {
    let td = TestDatadir::new().unwrap();
    let q = Query::open_or_create(td.store_path()).unwrap();
    let params = ChainParams::regtest();
    let ms = Milestone::NONE;

    let genesis = regtest_genesis();
    accept_and_connect_block(&q, &params, Height::GENESIS, &genesis, ms).unwrap();

    // Block 1: empty extra
    let b1 = mine_regtest_block(genesis.block_hash(), genesis.header.time + 600, 1, vec![]);
    let cb1_txid = b1.txdata[0].compute_txid();
    accept_and_connect_block(&q, &params, Height(1), &b1, ms).unwrap();

    // Block 2: spend coinbase from b1 (maturity not enforced yet — Phase 2 simplified)
    let spend = spend_anyone_can_spend(cb1_txid, 0, Amount::from_sat(49_0000_0000));
    let b2 = mine_regtest_block(b1.block_hash(), b1.header.time + 600, 2, vec![spend.clone()]);
    accept_and_connect_block(&q, &params, Height(2), &b2, ms).unwrap();
    assert_eq!(
        q.spenders(cb1_txid.as_byte_array(), 0).unwrap().len(),
        1
    );

    // Block 3 attempting second spend of same outpoint must fail connect validation
    let spend2 = spend_anyone_can_spend(cb1_txid, 0, Amount::from_sat(48_0000_0000));
    let b3 = mine_regtest_block(b2.block_hash(), b2.header.time + 600, 3, vec![spend2]);
    let err = accept_and_connect_block(&q, &params, Height(3), &b3, ms);
    assert!(
        matches!(
            err,
            Err(ConsensusError::PrevoutSpent) | Err(ConsensusError::BadTx(_))
        ),
        "got {err:?}"
    );
    assert_eq!(q.tip_height(), Some(Height(2)));
}

#[test]
fn consensus_milestone_skips_connect_checks() {
    let td = TestDatadir::new().unwrap();
    let q = Query::open_or_create(td.store_path()).unwrap();
    let params = ChainParams::regtest();
    // Under milestone, we still do structure + header, skip prevout/script.
    let ms = Milestone { height: 100 };
    assert!(ms.skips_at(1));

    let genesis = regtest_genesis();
    accept_and_connect_block(&q, &params, Height::GENESIS, &genesis, ms).unwrap();
    let b1 = mine_regtest_block(genesis.block_hash(), genesis.header.time + 1, 1, vec![]);
    accept_and_connect_block(&q, &params, Height(1), &b1, ms).unwrap();
    assert_eq!(q.tip_height(), Some(Height(1)));
}

#[test]
fn consensus_params_networks() {
    for net in [
        rbitcoin_primitives::Network::Mainnet,
        rbitcoin_primitives::Network::Testnet,
        rbitcoin_primitives::Network::Signet,
        rbitcoin_primitives::Network::Regtest,
    ] {
        let p = ChainParams::for_network(net);
        let g = rbitcoin_consensus::genesis_block(&p);
        assert_eq!(g.block_hash(), p.genesis_hash);
    }
}
