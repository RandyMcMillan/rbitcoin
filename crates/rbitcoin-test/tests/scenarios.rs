//! High-level functional scenarios (coverage-bearing).
//!
//! Prefer fewer tests at the highest layer that still hit production paths.
//! Mature regtest chains are built once per test that needs them (not thrice).

use bitcoin::hashes::Hash;
use bitcoin::{Amount, BlockHash};
use rbitcoin_cli::cli_main as cli_cli_main;
use rbitcoin_consensus::{
    accept_and_connect_block, validate_block_structure, ChainParams, ConsensusError, Milestone,
    ValidationContext,
};
use rbitcoin_net::{crate_name as net_crate_name, outbound_for_ibd};
use rbitcoin_node::{cli_main as node_cli_main, run_node, NodeConfig};
use rbitcoin_primitives::{Fk, Height, Network, TableKind, VERSION};
use rbitcoin_query::Query;
use rbitcoin_rpc::node_rpc_path;
use rbitcoin_store::{HeaderRecord, Store, StoreError, TxRecord};
use rbitcoin_test::mine::{mine_regtest_block, regtest_genesis, spend_anyone_can_spend};
use rbitcoin_test::{
    assert_reconstruct_eq, build_mature_regtest_with_spend, smoke_crate_names, TestDatadir,
};
use rbitcoin_wire_cache::WireRing;
use std::process::{Command, ExitCode};

// ─── Lifecycle / CLI / surface smoke (collapsed) ────────────────────────────

#[test]
fn node_cli_and_surface_smoke() {
    // Networks + run_node lifecycle
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
    for k in 1u16..=11 {
        assert_eq!(TableKind::from_u16(k).unwrap().as_u16(), k);
    }
    assert!(TableKind::from_u16(99).is_none());
    assert!(Fk::NULL.is_null());
    assert_eq!(Height::GENESIS.next(), Some(Height(1)));

    // Config errors
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

    // Placeholder / net surface
    let names = smoke_crate_names();
    assert!(names.contains(&"rbitcoin-store"));
    assert!(names.contains(&"rbitcoin-consensus"));
    let ring = WireRing::new(100);
    assert!(ring.is_empty());
    assert!(!Milestone::NONE.skips_at(0));
    assert!(Milestone { height: 10 }.skips_at(5));
    assert_eq!(outbound_for_ibd(true), 100);
    assert_eq!(outbound_for_ibd(false), 8);
    assert_eq!(net_crate_name(), "rbitcoin-net");
    assert_eq!(node_rpc_path(), "/");
    let _ = rbitcoin_net::local_service_flags();
    assert_eq!(rbitcoin_net::default_port(Network::Mainnet), 8333);
    assert_eq!(rbitcoin_net::default_port(Network::Regtest), 18444);
    assert!(rbitcoin_net::dns_seeds(Network::Mainnet).len() >= 3);
    assert!(rbitcoin_net::dns_seeds(Network::Regtest).is_empty());
    assert!(!rbitcoin_net::fixed_seed_hosts(Network::Mainnet).is_empty());
    let mut am = rbitcoin_net::AddrMan::with_seeds(Network::Regtest);
    assert!(am.is_empty());
    am.add("127.0.0.1:18444".parse().unwrap());
    assert_eq!(am.len(), 1);
    assert_eq!(am.take_outbound(10).len(), 1);
    let _ = rbitcoin_net::resolve_fixed_seeds(Network::Regtest);

    // Chain params (no mining)
    for net in [
        Network::Mainnet,
        Network::Testnet,
        Network::Signet,
        Network::Regtest,
    ] {
        let p = ChainParams::for_network(net);
        let g = rbitcoin_consensus::genesis_block(&p);
        assert_eq!(g.block_hash(), p.genesis_hash);
    }
    let main = ChainParams::mainnet();
    assert!(!main.checkpoints.is_empty());
    assert_eq!(main.checkpoint_at(Height(0)).unwrap(), main.genesis_hash);
    assert_eq!(main.difficulty_adjustment_interval(), 2016);
    assert!(!main.no_pow_retargeting());
    assert!(ChainParams::regtest().no_pow_retargeting());
    assert_eq!(rbitcoin_consensus::block_subsidy(0, &main), 50_0000_0000);
    assert_eq!(
        rbitcoin_consensus::block_subsidy(210_000, &main),
        25_0000_0000
    );

    // CLI entrypoints
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
    assert_ne!(
        node_cli_main(["rbitcoin-node", "--listen"]),
        ExitCode::SUCCESS
    );
    assert_ne!(
        node_cli_main(["rbitcoin-node", "--listen", "not-an-addr"]),
        ExitCode::SUCCESS
    );
    assert_ne!(
        node_cli_main(["rbitcoin-node", "--connect"]),
        ExitCode::SUCCESS
    );
    assert_ne!(
        node_cli_main(["rbitcoin-node", "--connect", "bad"]),
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

// ─── Store error / corrupt paths (not hit by happy-path chain tests) ────────

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

// ─── Synthetic store growth (no PoW; still triggers hash rehash) ─────────────

#[test]
fn chain_connect_reorg_and_growth() {
    use rbitcoin_query::TxApply;
    use rbitcoin_store::{InputRecord, OutputRecord};

    let td = TestDatadir::new().unwrap();
    let q = Query::open_or_create(td.store_path()).unwrap();

    // Default hash head is 64 slots; 80 blocks (header+tx keys) forces rehash.
    const N: u32 = 80;
    let mut prev = Fk::NULL;
    for h in 0..N {
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
    assert_eq!(q.tip_height(), Some(Height(N - 1)));
    q.flush().unwrap();
    drop(q);

    let q = Query::open_or_create(td.store_path()).unwrap();
    assert_eq!(q.tip_height(), Some(Height(N - 1)));
    q.disconnect_tip().unwrap();
    assert_eq!(q.tip_height(), Some(Height(N - 2)));
    assert!(q
        .connect_block(
            Height(0),
            &HeaderRecord {
                prev_fk: Fk::NULL,
                version: 1,
                timestamp: 0,
                bits: 1,
                nonce: 0,
                merkle_root: [0; 32],
                hash: [1; 32],
            },
            &[]
        )
        .is_err());
}

// ─── Consensus + reconstruct: one mature mine, many assertions ──────────────

/// Single mature-chain build covers:
/// - accept genesis + long mine (reopen tip)
/// - coinbase maturity + spend + double-spend reject
/// - reconstruct after reopen (sampled + multi-tx spend block)
/// - store-backed locator/headers helpers
/// - service flags
#[test]
fn consensus_mature_chain_spend_and_reconstruct() {
    use bitcoin::p2p::ServiceFlags;
    use rbitcoin_net::local_service_flags;

    let td = TestDatadir::new().unwrap();
    let q = Query::open_or_create(td.store_path()).unwrap();
    let params = ChainParams::regtest();

    // ONE maturity pad for both spend and reconstruct paths.
    let chain = build_mature_regtest_with_spend(&q, &params);
    let tip_h = chain.tip_height();
    assert_eq!(q.tip_height(), Some(Height(tip_h)));
    assert_eq!(chain.tip_hash(), chain.blocks.last().unwrap().block_hash());
    assert!(tip_h >= params.coinbase_maturity() + 2);

    // Spend of height-1 coinbase succeeded at tip.
    assert_eq!(
        q.spenders(chain.matured_coinbase_txid.as_byte_array(), 0)
            .unwrap()
            .len(),
        1
    );
    assert!(
        chain.blocks[chain.spend_height as usize].txdata.len() >= 2,
        "spend block should be multi-tx"
    );

    // Double-spend must fail.
    let tip_block = chain.blocks.last().unwrap();
    let spend2 = spend_anyone_can_spend(
        chain.matured_coinbase_txid,
        0,
        Amount::from_sat(48_0000_0000),
    );
    let b_bad = mine_regtest_block(
        tip_block.block_hash(),
        tip_block.header.time + 600,
        tip_h + 1,
        vec![spend2],
    );
    let err = accept_and_connect_block(&q, &params, Height(tip_h + 1), &b_bad, Milestone::NONE);
    assert!(
        matches!(
            err,
            Err(ConsensusError::PrevoutSpent) | Err(ConsensusError::BadTx(_))
        ),
        "got {err:?}"
    );
    assert_eq!(q.tip_height(), Some(Height(tip_h)));

    q.flush().unwrap();
    drop(q);

    // Reopen — reconstruct without RAM cache.
    let q = Query::open_or_create(td.store_path()).unwrap();
    assert_eq!(q.tip_height(), Some(Height(tip_h)));

    // Sample heights: genesis, early, mid, tip (multi-tx). Full 100+ scan is redundant.
    let sample = [0u32, 1, tip_h / 2, tip_h - 1, tip_h];
    for h in sample {
        assert_reconstruct_eq(&q, h, &chain.blocks[h as usize]);
    }

    assert!(q
        .reconstruct_block_by_hash(&[0xab; 32])
        .unwrap()
        .is_none());
    assert!(q.reconstruct_block_at_height(Height(9999)).is_err());

    let loc = q.locator_hashes().unwrap();
    assert!(!loc.is_empty());
    let headers = q
        .headers_after_locator(
            &loc[loc.len().saturating_sub(1)..],
            BlockHash::from_byte_array([0; 32]),
            2000,
        )
        .unwrap();
    assert!(!headers.is_empty());

    let flags = local_service_flags();
    assert!(flags.has(ServiceFlags::NETWORK));
    assert!(flags.has(ServiceFlags::WITNESS));
}

// ─── Phase 6: scripthash index + wire ring + archive epoch ──────────────────

#[test]
fn scripthash_index_history_balance_and_reorg() {
    use rbitcoin_store::script_hash;

    let td = TestDatadir::new().unwrap();
    let q = Query::open_or_create(td.store_path()).unwrap();
    let params = ChainParams::regtest();

    // Build mature chain with a spend of OP_TRUE coinbase (script [0x51]).
    let chain = build_mature_regtest_with_spend(&q, &params);
    let sh = script_hash(&[0x51]);

    let history = q.scripthash_history(&sh).unwrap();
    assert!(
        !history.is_empty(),
        "OP_TRUE outputs should appear in history"
    );
    // Coinbase h1 and spend tx both touch this script.
    assert!(history.len() >= 2);

    let bal = q.scripthash_balance(&sh).unwrap();
    // Many coinbases still unspent; balance should be positive.
    assert!(bal.confirmed > 0, "confirmed={}", bal.confirmed);
    assert_eq!(bal.unconfirmed, 0);

    let utxos = q.scripthash_listunspent(&sh).unwrap();
    assert!(!utxos.is_empty());
    // Spent coinbase from h1 must not be listed.
    assert!(
        !utxos
            .iter()
            .any(|u| u.tx_hash == chain.matured_coinbase_txid.to_byte_array() && u.tx_pos == 0)
    );

    // Reorg: disconnect tip (spend block) → coinbase UTXO returns, history drops spend.
    q.disconnect_tip().unwrap();
    let utxos2 = q.scripthash_listunspent(&sh).unwrap();
    assert!(
        utxos2
            .iter()
            .any(|u| u.tx_hash == chain.matured_coinbase_txid.to_byte_array() && u.tx_pos == 0),
        "after disconnect, matured coinbase should be unspent again"
    );
}

#[test]
fn wire_ring_and_archive_epoch() {
    use bitcoin::consensus::Encodable;
    use rbitcoin_wire_cache::WireRing;

    let td = TestDatadir::new().unwrap();
    let q = Query::open_or_create(td.store_path()).unwrap();
    let params = ChainParams::regtest();
    let genesis = regtest_genesis();
    accept_and_connect_block(&q, &params, Height::GENESIS, &genesis, Milestone::NONE).unwrap();

    let wire_dir = td.path().join("wire");
    let ring = WireRing::with_dir(3, &wire_dir).unwrap();
    assert!(ring.is_empty());

    let mut tip = genesis.block_hash();
    let mut tip_time = genesis.header.time;
    let mut blocks = vec![genesis.clone()];
    for h in 1..=5u32 {
        let b = mine_regtest_block(tip, tip_time + 600, h, vec![]);
        accept_and_connect_block(&q, &params, Height(h), &b, Milestone::NONE).unwrap();
        let mut wire = Vec::new();
        b.consensus_encode(&mut wire).unwrap();
        ring.push(h, b.block_hash().to_byte_array(), wire, h)
            .unwrap();
        tip = b.block_hash();
        tip_time = b.header.time;
        blocks.push(b);
    }
    // depth 3 → keep heights 3,4,5 (tip-2..=tip)
    assert!(!ring.contains_height(1));
    assert!(ring.contains_height(5));
    assert!(ring.get_by_hash(&blocks[5].block_hash().to_byte_array()).is_some());

    // Finalize through height 4 → drop wire ≤ 4
    q.set_archive_mode(true).unwrap();
    q.finalize_through(4).unwrap();
    ring.drop_through(4).unwrap();
    assert!(!ring.contains_height(4));
    assert!(ring.contains_height(5));
    let ep = q.archive_epoch();
    assert!(ep.archive_mode);
    assert_eq!(ep.finalized_height, Some(4));
    assert!(ep.is_soft_zone(5));
    assert!(!ep.is_soft_zone(4));

    // Reopen epoch from disk
    let q2 = Query::open_or_create(td.store_path()).unwrap();
    assert_eq!(q2.archive_epoch().finalized_height, Some(4));

    // Wire ring reloads from disk
    let ring2 = WireRing::with_dir(3, &wire_dir).unwrap();
    assert!(ring2.contains_height(5));
}

#[test]
fn consensus_reject_bad_structure_and_milestone() {
    let td = TestDatadir::new().unwrap();
    let q = Query::open_or_create(td.store_path()).unwrap();
    let params = ChainParams::regtest();
    let genesis = regtest_genesis();
    accept_and_connect_block(&q, &params, Height::GENESIS, &genesis, Milestone::NONE).unwrap();

    let mut block = mine_regtest_block(genesis.block_hash(), genesis.header.time + 1, 1, vec![]);
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

    let block2 = mine_regtest_block(
        BlockHash::from_byte_array([0x22; 32]),
        genesis.header.time + 2,
        1,
        vec![],
    );
    assert!(accept_and_connect_block(&q, &params, Height(1), &block2, Milestone::NONE).is_err());

    // Milestone: skip connect checks on a fresh short chain.
    let td2 = TestDatadir::new().unwrap();
    let q2 = Query::open_or_create(td2.store_path()).unwrap();
    let ms = Milestone { height: 100 };
    assert!(ms.skips_at(1));
    accept_and_connect_block(&q2, &params, Height::GENESIS, &genesis, ms).unwrap();
    let b1 = mine_regtest_block(genesis.block_hash(), genesis.header.time + 1, 1, vec![]);
    accept_and_connect_block(&q2, &params, Height(1), &b1, ms).unwrap();
    assert_eq!(q2.tip_height(), Some(Height(1)));
}
