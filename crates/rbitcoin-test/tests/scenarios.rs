//! High-level functional scenarios (coverage-bearing).
//!
//! Prefer fewer tests at the highest layer that still hit production paths.
//! Mature regtest chains are built once per test that needs them (not thrice).

use bitcoin::hashes::Hash;
use bitcoin::{Amount, BlockHash};
use rbitcoin_cli::cli_main as cli_cli_main;
use rbitcoin_consensus::{
    accept_and_connect_block, validate_block_connect, validate_block_structure, ChainParams,
    ConsensusError, Milestone, ValidationContext,
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
    assert!(!Milestone::NONE.skips_scripts_at(0));
    assert!(Milestone { height: 10 }.skips_scripts_at(5));
    assert_eq!(outbound_for_ibd(true), rbitcoin_net::DEFAULT_IBD_TARGET_PEERS);
    assert_eq!(outbound_for_ibd(true), 16);
    assert_eq!(outbound_for_ibd(false), 8);
    assert_eq!(
        rbitcoin_net::DEFAULT_IBD_OUTBOUND,
        rbitcoin_net::DEFAULT_IBD_TARGET_PEERS
    );
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
    assert_eq!(am.take_outbound_offset(1, 0).len(), 1);
    let _ = rbitcoin_net::resolve_fixed_seeds(Network::Regtest);
    let _ = rbitcoin_net::resolve_dns_seeds(Network::Regtest);
    let _ = rbitcoin_net::resolve_all_seeds(Network::Regtest);
    assert!(!rbitcoin_net::dns_seeds(Network::Signet).is_empty());

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
    assert_ne!(
        node_cli_main(["rbitcoin-node", "--milestone", "x"]),
        ExitCode::SUCCESS
    );
    assert_ne!(
        node_cli_main(["rbitcoin-node", "--max-outbound", "0"]),
        ExitCode::SUCCESS
    );
    let _ = node_cli_main([
        "rbitcoin-node",
        "--help",
    ]);
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
                input_count: 1,
                output_start_fk: Fk::NULL,
                output_count: 1,
            },
            inputs: vec![InputRecord {
                prev_txid: [0u8; 32],
                prev_index: u32::MAX,
                sequence: u32::MAX,
                script_sig: vec![0],
                witness: vec![],
            }],
            outputs: vec![OutputRecord {
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

// ─── IBD Class A: out-of-order archive, idempotent re-archive, head-off ─────

/// After archive-ahead of tip, `resume_work_path_after_tip` must rebuild the
/// ordered path with Class A flags so restart does not re-getdata those bodies.
#[test]
fn resume_work_path_sees_archived_bodies_after_reopen() {
    use rbitcoin_consensus::{
        accept_and_archive_block, accept_and_connect_block, header_to_record, ChainParams,
        Milestone,
    };
    use rbitcoin_test::mine::{mine_regtest_block, regtest_genesis};

    let td = TestDatadir::new().unwrap();
    let params = ChainParams::regtest();
    let ms = Milestone { height: 1_000_000 };
    let genesis = regtest_genesis();

    let hashes = {
        let q = Query::open_or_create(td.store_path()).unwrap();
        accept_and_connect_block(&q, &params, Height::GENESIS, &genesis, ms).unwrap();
        let g_fk = q
            .get_header_by_hash(&genesis.block_hash().to_byte_array())
            .unwrap()
            .unwrap()
            .0;
        let mut tip = genesis.block_hash();
        let mut tip_time = genesis.header.time;
        let mut prev_fk = g_fk;
        let mut out = Vec::new();
        // Confirm stays at 0; archive heights 1..4 ahead (parallel IBD shape).
        for h in 1u32..=4 {
            let b = mine_regtest_block(tip, tip_time + 600, h, vec![]);
            let fk = q
                .ensure_header(&header_to_record(prev_fk, &b.header))
                .unwrap();
            accept_and_archive_block(&q, &params, Height(h), &b, ms).unwrap();
            out.push(b.block_hash().to_byte_array());
            tip = b.block_hash();
            tip_time = b.header.time;
            prev_fk = fk;
        }
        q.flush().unwrap();
        out
    };

    // Cold reopen — process-local ordered path is gone; store still has Class A.
    let q2 = Query::open_or_create(td.store_path()).unwrap();
    let tip_hash = genesis.block_hash().to_byte_array();
    let path = q2
        .resume_work_path_after_tip(tip_hash, 0, 64)
        .expect("resume");
    assert_eq!(path.len(), 4, "expected 4 headers after tip");
    assert!(
        path.iter().all(|e| e.has_body),
        "all resume entries should have Class A bodies"
    );
    for (i, e) in path.iter().enumerate() {
        assert_eq!(e.height, (i as u32) + 1);
        assert_eq!(e.hash, hashes[i]);
        assert!(q2.is_block_archived(&e.hash).unwrap());
    }
}

/// Single scenario covering the signet @2148 failure class and parallel IBD:
/// - archive bodies out of height order (ahead of tip)
/// - re-archive / mega-batch duplicate is idempotent (fk + tx_height stable)
/// - `tx.head` off: prevouts via light UTXO create_fk (external prev on Class A)
/// - durable points off on confirm (process-local / UTXO); backfill restores spenders()
/// - coinbase maturity then spend still connects
#[test]
fn ibd_parallel_archive_idempotent_confirm_without_tx_head() {
    use rbitcoin_consensus::{
        accept_and_archive_block, accept_and_connect_block, confirm_archived_at, header_to_record,
        prepare_block_for_archive, ChainParams, Milestone,
    };
    use rbitcoin_test::mine::{mine_regtest_block, regtest_genesis, spend_anyone_can_spend};

    let td = TestDatadir::new().unwrap();
    let q = Query::open_or_create(td.store_path()).unwrap();
    q.set_tx_index(false);
    q.set_spend_index(false);
    q.enable_ibd_utxo().unwrap();
    let ms = Milestone { height: 1_000_000 };
    let params = ChainParams::regtest();
    let maturity = params.coinbase_maturity();

    let genesis = regtest_genesis();
    accept_and_connect_block(&q, &params, Height::GENESIS, &genesis, ms).unwrap();
    let g_fk = q
        .get_header_by_hash(&genesis.block_hash().to_byte_array())
        .unwrap()
        .unwrap()
        .0;

    // Mine a short pad, then archive **out of order** (2 before 1) like parallel IBD.
    let mut tip = genesis.block_hash();
    let mut tip_time = genesis.header.time;
    let b1 = mine_regtest_block(tip, tip_time + 600, 1, vec![]);
    let cb1 = b1.txdata[0].compute_txid();
    tip = b1.block_hash();
    tip_time = b1.header.time;
    let b2 = mine_regtest_block(tip, tip_time + 600, 2, vec![]);
    tip = b2.block_hash();
    tip_time = b2.header.time;

    // Headers first (body order independent), then bodies 2 then 1.
    let h1_fk = q
        .ensure_header(&header_to_record(g_fk, &b1.header))
        .unwrap();
    let _h2_fk = q
        .ensure_header(&header_to_record(h1_fk, &b2.header))
        .unwrap();
    accept_and_archive_block(&q, &params, Height(2), &b2, ms).unwrap();
    accept_and_archive_block(&q, &params, Height(1), &b1, ms).unwrap();
    // Mega-batch duplicate of the same header (two prep deliveries).
    let (h1_rec, h1_txs) = prepare_block_for_archive(&q, &params, &b1).unwrap();
    let fks_before = q.store().header_txs.get_list(h1_fk).unwrap().unwrap();
    let mut dup = vec![
        (h1_fk, h1_rec.clone(), h1_txs.clone()),
        (h1_fk, h1_rec, h1_txs),
    ];
    q.archive_prepared_with_fks(&mut dup).unwrap();
    assert_eq!(
        q.store().header_txs.get_list(h1_fk).unwrap().unwrap(),
        fks_before,
        "duplicate mega-batch must not reassign tx fks"
    );

    confirm_archived_at(&q, &params, Height(1), &b1.block_hash().to_byte_array(), ms).unwrap();
    // Second peer delivery after confirm: still idempotent.
    accept_and_archive_block(&q, &params, Height(1), &b1, ms).unwrap();
    assert_eq!(
        q.store().tx_height.get(fks_before[0]).unwrap(),
        Some(1),
        "tx_height must remain on first-archive fks"
    );
    confirm_archived_at(&q, &params, Height(2), &b2.block_hash().to_byte_array(), ms).unwrap();

    let last_pad = maturity + 1;
    for h in 3..=last_pad {
        let b = mine_regtest_block(tip, tip_time + 600, h, vec![]);
        accept_and_archive_block(&q, &params, Height(h), &b, ms).unwrap();
        accept_and_archive_block(&q, &params, Height(h), &b, ms).unwrap();
        confirm_archived_at(&q, &params, Height(h), &b.block_hash().to_byte_array(), ms)
            .unwrap();
        tip = b.block_hash();
        tip_time = b.header.time;
    }

    let spend_h = last_pad + 1;
    let spend = spend_anyone_can_spend(cb1, 0, Amount::from_sat(49_0000_0000));
    let b_spend = mine_regtest_block(tip, tip_time + 600, spend_h, vec![spend]);
    accept_and_archive_block(&q, &params, Height(spend_h), &b_spend, ms).unwrap();
    accept_and_archive_block(&q, &params, Height(spend_h), &b_spend, ms).unwrap();
    confirm_archived_at(
        &q,
        &params,
        Height(spend_h),
        &b_spend.block_hash().to_byte_array(),
        ms,
    )
    .expect("mature coinbase spend with head off + double-archive");
    assert_eq!(q.tip_height(), Some(Height(spend_h)));
    // Durable points deferred; process-local set still sees the spend.
    assert!(
        q.is_outpoint_spent(cb1.as_byte_array(), 0).unwrap(),
        "local spent-set must mark spends when spend_index is off"
    );
    assert!(
        q.spenders(cb1.as_byte_array(), 0).unwrap().is_empty(),
        "durable spenders empty until backfill"
    );
    q.set_spend_index(true);
    let (_h, _txs) = q.backfill_point_spends(|_, _, _, _| {}).unwrap();
    assert_eq!(
        q.spenders(cb1.as_byte_array(), 0).unwrap().len(),
        1,
        "backfill must write durable point edges for Electrum/spenders"
    );
    let fks = q.block_tx_fks(Height(spend_h)).unwrap();
    assert!(fks.len() >= 2);
    let inp = q.tx_input(&q.get_tx(fks[1]).unwrap(), 0).unwrap();
    // Class A always external prev_txid; confirm used UTXO create_fk.
    assert_ne!(inp.prev_txid, [0u8; 32]);
    assert_eq!(inp.prev_index, 0);
}

/// Milestone 0 / `spend_index` on: archive writes point edges before Class C.
/// Confirm must use **strong** spenders only — `spenders_raw` would see the
/// archived (non-strong) edge and reject a valid tip (signet height 2148).
#[test]
fn confirm_with_spend_index_ignores_archive_only_point_edges() {
    use rbitcoin_consensus::{
        accept_and_archive_block, accept_and_connect_block, confirm_archived_at, ChainParams,
        Milestone,
    };
    use rbitcoin_test::mine::{mine_regtest_block, regtest_genesis, spend_anyone_can_spend};

    let td = TestDatadir::new().unwrap();
    let q = Query::open_or_create(td.store_path()).unwrap();
    // Full-verify / tip-mode indexing: durable points written on archive + confirm.
    q.set_spend_index(true);
    q.set_tx_index(true);
    let ms = Milestone::NONE; // scripts on; same spend path as milestone 0
    let params = ChainParams::regtest();
    let maturity = params.coinbase_maturity();

    let genesis = regtest_genesis();
    accept_and_connect_block(&q, &params, Height::GENESIS, &genesis, ms).unwrap();
    let mut tip = genesis.block_hash();
    let mut tip_time = genesis.header.time;

    let b1 = mine_regtest_block(tip, tip_time + 600, 1, vec![]);
    let cb1 = b1.txdata[0].compute_txid();
    accept_and_connect_block(&q, &params, Height(1), &b1, ms).unwrap();
    tip = b1.block_hash();
    tip_time = b1.header.time;

    let last_pad = maturity + 1;
    let mut pad_blocks = Vec::new();
    for h in 2..=last_pad {
        let b = mine_regtest_block(tip, tip_time + 600, h, vec![]);
        // Archive ahead of confirm (parallel IBD shape).
        accept_and_archive_block(&q, &params, Height(h), &b, ms).unwrap();
        tip = b.block_hash();
        tip_time = b.header.time;
        pad_blocks.push((h, b.block_hash().to_byte_array()));
    }
    let spend_h = last_pad + 1;
    let spend = spend_anyone_can_spend(cb1, 0, Amount::from_sat(49_0000_0000));
    let b_spend = mine_regtest_block(tip, tip_time + 600, spend_h, vec![spend]);
    // Archive spend **before** confirming the pad — writes point row while
    // spending tx is not yet strong.
    accept_and_archive_block(&q, &params, Height(spend_h), &b_spend, ms).unwrap();
    assert!(
        !q.spenders_raw(cb1.as_byte_array(), 0).unwrap().is_empty(),
        "archive must have written a raw point edge"
    );
    assert!(
        q.spenders(cb1.as_byte_array(), 0).unwrap().is_empty(),
        "spending tx not strong yet — strong spenders empty"
    );
    assert!(
        !q.is_outpoint_spent(cb1.as_byte_array(), 0).unwrap(),
        "is_outpoint_spent must not treat archive-only edges as best-chain spent"
    );

    // Confirm pad then spend; must not false-positive PrevoutSpent.
    for (h, hash) in &pad_blocks {
        confirm_archived_at(&q, &params, Height(*h), hash, ms).unwrap();
    }
    confirm_archived_at(
        &q,
        &params,
        Height(spend_h),
        &b_spend.block_hash().to_byte_array(),
        ms,
    )
    .expect("confirm spend with archive-ahead point edges (signet @2148 class)");
    assert_eq!(q.tip_height(), Some(Height(spend_h)));
    assert_eq!(
        q.spenders(cb1.as_byte_array(), 0).unwrap().len(),
        1,
        "after Class C the spend is strong"
    );
}

/// Simulate kill -9 mid Class C: strong_tx + tx_height + point edges written for
/// tip+1 but `confirmed[]` not advanced. Re-confirm must not false-positive
/// PrevoutSpent (tip is the Class C commit point).
#[test]
fn confirm_survives_partial_class_c_without_tip_advance() {
    use rbitcoin_consensus::{
        accept_and_archive_block, accept_and_connect_block, confirm_archived_at, ChainParams,
        Milestone,
    };
    use rbitcoin_test::mine::{mine_regtest_block, regtest_genesis, spend_anyone_can_spend};

    let td = TestDatadir::new().unwrap();
    let q = Query::open_or_create(td.store_path()).unwrap();
    q.set_spend_index(true);
    q.set_tx_index(true);
    let ms = Milestone::NONE;
    let params = ChainParams::regtest();
    let maturity = params.coinbase_maturity();

    let genesis = regtest_genesis();
    accept_and_connect_block(&q, &params, Height::GENESIS, &genesis, ms).unwrap();
    let mut tip = genesis.block_hash();
    let mut tip_time = genesis.header.time;

    let b1 = mine_regtest_block(tip, tip_time + 600, 1, vec![]);
    let cb1 = b1.txdata[0].compute_txid();
    accept_and_connect_block(&q, &params, Height(1), &b1, ms).unwrap();
    tip = b1.block_hash();
    tip_time = b1.header.time;

    let last_pad = maturity + 1;
    for h in 2..=last_pad {
        let b = mine_regtest_block(tip, tip_time + 600, h, vec![]);
        accept_and_connect_block(&q, &params, Height(h), &b, ms).unwrap();
        tip = b.block_hash();
        tip_time = b.header.time;
    }
    let tip_before = q.tip_height().unwrap();
    assert_eq!(tip_before, Height(last_pad));

    let spend_h = last_pad + 1;
    let spend = spend_anyone_can_spend(cb1, 0, Amount::from_sat(49_0000_0000));
    let b_spend = mine_regtest_block(tip, tip_time + 600, spend_h, vec![spend]);
    accept_and_archive_block(&q, &params, Height(spend_h), &b_spend, ms).unwrap();

    let hash = b_spend.block_hash().to_byte_array();
    let (header_fk, _) = q.get_header_by_hash(&hash).unwrap().unwrap();
    let tx_fks = q.store().header_txs.get_list(header_fk).unwrap().unwrap();
    assert!(tx_fks.len() >= 2, "coinbase + spend");

    // --- Partial Class C (confirm_blocks_run writes strong/tx_height before tip) ---
    // Archive already wrote point edges (spend_index on); the kill-9 window is
    // strong bits without confirmed[] advance — old spenders() treated that as spent.
    let first = tx_fks[0];
    q.store()
        .strong_tx
        .set_strong_range(first, tx_fks.len() as u32, header_fk)
        .unwrap();
    q.store()
        .tx_height
        .set_range(first, tx_fks.len() as u32, Height(spend_h))
        .unwrap();
    // Tip intentionally NOT advanced.
    assert_eq!(q.tip_height(), Some(tip_before));
    assert!(
        q.store().strong_tx.is_strong(tx_fks[1]).unwrap(),
        "sim: spending tx marked strong without tip"
    );
    assert!(
        !q.spenders_raw(cb1.as_byte_array(), 0).unwrap().is_empty(),
        "archive point edge present"
    );
    // Best-chain spenders must ignore strong-above-tip.
    assert!(
        q.spenders(cb1.as_byte_array(), 0).unwrap().is_empty(),
        "spenders must not see uncommitted Class C"
    );
    assert!(
        !q.store().is_confirmed_strong(tx_fks[1]).unwrap(),
        "is_confirmed_strong false while height > tip"
    );

    // Re-confirm tip+1 (restart after kill -9) must succeed.
    confirm_archived_at(&q, &params, Height(spend_h), &hash, ms)
        .expect("re-confirm after partial Class C (kill -9 class)");
    assert_eq!(q.tip_height(), Some(Height(spend_h)));
    assert_eq!(
        q.spenders(cb1.as_byte_array(), 0).unwrap().len(),
        1,
        "after tip commit the spend is confirmed-strong"
    );

    // Open-time repair: leave another partial Class C and reopen.
    let b_next = mine_regtest_block(
        b_spend.block_hash(),
        b_spend.header.time + 600,
        spend_h + 1,
        vec![],
    );
    accept_and_archive_block(&q, &params, Height(spend_h + 1), &b_next, ms).unwrap();
    let hash2 = b_next.block_hash().to_byte_array();
    let (hfk2, _) = q.get_header_by_hash(&hash2).unwrap().unwrap();
    let fks2 = q.store().header_txs.get_list(hfk2).unwrap().unwrap();
    q.store()
        .strong_tx
        .set_strong_range(fks2[0], fks2.len() as u32, hfk2)
        .unwrap();
    q.store()
        .tx_height
        .set_range(fks2[0], fks2.len() as u32, Height(spend_h + 1))
        .unwrap();
    q.flush().unwrap();
    drop(q);

    let q2 = Query::open_or_create(td.store_path()).unwrap();
    assert!(
        !q2.store().strong_tx.is_strong(fks2[0]).unwrap(),
        "open must repair strong bits above tip"
    );
    confirm_archived_at(&q2, &params, Height(spend_h + 1), &hash2, ms)
        .expect("confirm after open repair");
    assert_eq!(q2.tip_height(), Some(Height(spend_h + 1)));
}

/// Resume with `tx.head` off: spend archived with external prev_txid; confirm
/// resolves parent via light UTXO create_fk (no process txid map / warm).
#[test]
fn resume_head_off_utxo_resolves_external_prev() {
    use rbitcoin_consensus::{
        accept_and_archive_block, accept_and_connect_block, confirm_archived_at, ChainParams,
        Milestone,
    };
    use rbitcoin_test::mine::{mine_regtest_block, regtest_genesis, spend_anyone_can_spend};

    let td = TestDatadir::new().unwrap();
    let ms = Milestone { height: 1_000_000 };
    let params = ChainParams::regtest();
    let maturity = params.coinbase_maturity();

    // Session 1: mine + confirm pad so coinbase is mature; leave spend unarchived.
    let (cb1, tip, tip_time, spend_h, b_spend) = {
        let q = Query::open_or_create(td.store_path()).unwrap();
        q.set_tx_index(false);
        q.set_spend_index(false);
        q.enable_ibd_utxo().unwrap();
        let genesis = regtest_genesis();
        accept_and_connect_block(&q, &params, Height::GENESIS, &genesis, ms).unwrap();
        let mut tip = genesis.block_hash();
        let mut tip_time = genesis.header.time;
        let b1 = mine_regtest_block(tip, tip_time + 600, 1, vec![]);
        let cb1 = b1.txdata[0].compute_txid();
        accept_and_connect_block(&q, &params, Height(1), &b1, ms).unwrap();
        tip = b1.block_hash();
        tip_time = b1.header.time;
        let last_pad = maturity + 1;
        for h in 2..=last_pad {
            let b = mine_regtest_block(tip, tip_time + 600, h, vec![]);
            accept_and_connect_block(&q, &params, Height(h), &b, ms).unwrap();
            tip = b.block_hash();
            tip_time = b.header.time;
        }
        let spend_h = last_pad + 1;
        let spend = spend_anyone_can_spend(cb1, 0, Amount::from_sat(49_0000_0000));
        let b_spend = mine_regtest_block(tip, tip_time + 600, spend_h, vec![spend]);
        q.flush().unwrap();
        (cb1, tip, tip_time, spend_h, b_spend)
    };
    let _ = (tip, tip_time);

    // Session 2: reopen UTXO (tip aligned), archive spend with external prev, confirm.
    {
        let q = Query::open_or_create(td.store_path()).unwrap();
        q.set_tx_index(false);
        q.set_spend_index(false);
        q.enable_ibd_utxo().unwrap();
        assert!(
            q.ibd_utxo_create_fk(cb1.as_byte_array(), 0)
                .unwrap()
                .is_some(),
            "light UTXO must retain mature coinbase create_fk across reopen"
        );
        accept_and_archive_block(&q, &params, Height(spend_h), &b_spend, ms).unwrap();
        let fks = q
            .store()
            .header_txs
            .get_list(
                q.get_header_by_hash(&b_spend.block_hash().to_byte_array())
                    .unwrap()
                    .unwrap()
                    .0,
            )
            .unwrap()
            .unwrap();
        let inp = q.tx_input(&q.get_tx(fks[1]).unwrap(), 0).unwrap();
        assert_ne!(
            inp.prev_txid, [0u8; 32],
            "Class A stores external prev_txid (no prev_tx_fk field)"
        );

        confirm_archived_at(
            &q,
            &params,
            Height(spend_h),
            &b_spend.block_hash().to_byte_array(),
            ms,
        )
        .expect("external prev_txid resolves via light UTXO create_fk");
        assert_eq!(q.tip_height(), Some(Height(spend_h)));
        assert!(
            q.is_outpoint_spent(cb1.as_byte_array(), 0).unwrap(),
            "UTXO must see the confirmed spend"
        );
        assert!(
            q.spenders(cb1.as_byte_array(), 0).unwrap().is_empty(),
            "durable points stay empty until spend_index / backfill"
        );
    }
}

/// wave_fill hybrid: process-local spent short-circuits durable point.head probes.
///
/// Archive may already have a point edge for a not-yet-confirmed spend (not
/// strong). Local spent must still suppress the parent live slot so recon
/// does not treat the outpoint as unspent.
#[test]
fn wave_fill_hybrid_local_spent_before_durable() {
    use rbitcoin_consensus::{
        accept_and_archive_block, accept_and_connect_block, ChainParams, Milestone,
    };
    use rbitcoin_test::mine::{mine_regtest_block, regtest_genesis, spend_anyone_can_spend};

    let td = TestDatadir::new().unwrap();
    let q = Query::open_or_create(td.store_path()).unwrap();
    q.set_spend_index(true);
    q.set_tx_index(true);
    let ms = Milestone::NONE;
    let params = ChainParams::regtest();
    let maturity = params.coinbase_maturity();

    let genesis = regtest_genesis();
    accept_and_connect_block(&q, &params, Height::GENESIS, &genesis, ms).unwrap();
    let mut tip = genesis.block_hash();
    let mut tip_time = genesis.header.time;

    let b1 = mine_regtest_block(tip, tip_time + 600, 1, vec![]);
    let cb1 = b1.txdata[0].compute_txid();
    accept_and_connect_block(&q, &params, Height(1), &b1, ms).unwrap();
    tip = b1.block_hash();
    tip_time = b1.header.time;

    let last_pad = maturity + 1;
    for h in 2..=last_pad {
        let b = mine_regtest_block(tip, tip_time + 600, h, vec![]);
        accept_and_connect_block(&q, &params, Height(h), &b, ms).unwrap();
        tip = b.block_hash();
        tip_time = b.header.time;
    }

    let spend_h = last_pad + 1;
    let spend = spend_anyone_can_spend(cb1, 0, bitcoin::Amount::from_sat(49_0000_0000));
    let b_spend = mine_regtest_block(tip, tip_time + 600, spend_h, vec![spend]);
    accept_and_archive_block(&q, &params, Height(spend_h), &b_spend, ms).unwrap();
    let spend_hash = b_spend.block_hash().to_byte_array();

    // Archive wrote a point edge, but spender is not strong yet → durable says unspent.
    assert!(
        !q.store()
            .has_confirmed_strong_spender(cb1.as_byte_array(), 0)
            .unwrap(),
        "pre-confirm archive edge must not count as confirmed-strong spent"
    );

    q.prefetch_class_a_for_block_hashes(&[spend_hash]).unwrap();

    // Without local: wave_fill should expose the parent as live.
    let (_n, wave_live) = q
        .prefetch_tip_prevouts_for_block_hashes(&[spend_hash])
        .unwrap();
    assert!(
        wave_live.has_live_output_txid(cb1.as_byte_array(), 0),
        "unspent parent must be live before local spent is noted"
    );

    // Hybrid A: local spent wins even while tip_prevout still has a live slot
    // (stale tip vs process-local — local is authoritative).
    q.note_outpoint_spent_local(cb1.to_byte_array(), 0);
    assert!(q.is_outpoint_spent(cb1.as_byte_array(), 0).unwrap());
    let (_n, wave_stale_tip) = q
        .prefetch_tip_prevouts_for_block_hashes(&[spend_hash])
        .unwrap();
    assert!(
        !wave_stale_tip.has_live_output_txid(cb1.as_byte_array(), 0),
        "local spent must beat stale tip-live short-circuit"
    );

    // Hybrid B: need_spent path after tip retirement (confirm pairs note+retire).
    q.retire_tip_prevout_spends(&[(cb1.to_byte_array(), 0)]);
    let (_n, wave_spent) = q
        .prefetch_tip_prevouts_for_block_hashes(&[spend_hash])
        .unwrap();
    assert!(
        !wave_spent.has_live_output_txid(cb1.as_byte_array(), 0),
        "local spent must suppress parent live slot without durable strong"
    );
}

/// mmap IBD UTXO: double-spend reject, disconnect undo, rebuild gate.
#[test]
fn spent_local_core_double_spend_and_disconnect() {
    use rbitcoin_consensus::{
        accept_and_connect_block, ChainParams, ConsensusError, Milestone,
    };
    use rbitcoin_test::mine::{mine_regtest_block, regtest_genesis, spend_anyone_can_spend};

    let td = TestDatadir::new().unwrap();
    let q = Query::open_or_create(td.store_path()).unwrap();
    q.set_spend_index(false);
    q.set_tx_index(false);
    q.enable_ibd_utxo().unwrap();
    assert!(q.spent_local_ready(), "empty tip must be ready");
    let ms = Milestone::NONE;
    let params = ChainParams::regtest();
    let maturity = params.coinbase_maturity();

    let genesis = regtest_genesis();
    accept_and_connect_block(&q, &params, Height::GENESIS, &genesis, ms).unwrap();
    let mut tip = genesis.block_hash();
    let mut tip_time = genesis.header.time;

    let b1 = mine_regtest_block(tip, tip_time + 600, 1, vec![]);
    let cb1 = b1.txdata[0].compute_txid();
    accept_and_connect_block(&q, &params, Height(1), &b1, ms).unwrap();
    tip = b1.block_hash();
    tip_time = b1.header.time;

    let last_pad = maturity + 1;
    for h in 2..=last_pad {
        let b = mine_regtest_block(tip, tip_time + 600, h, vec![]);
        accept_and_connect_block(&q, &params, Height(h), &b, ms).unwrap();
        tip = b.block_hash();
        tip_time = b.header.time;
    }

    let spend_h = last_pad + 1;
    let spend = spend_anyone_can_spend(cb1, 0, bitcoin::Amount::from_sat(49_0000_0000));
    let b_spend = mine_regtest_block(tip, tip_time + 600, spend_h, vec![spend]);
    accept_and_connect_block(&q, &params, Height(spend_h), &b_spend, ms).unwrap();
    assert!(q.is_outpoint_spent(cb1.as_byte_array(), 0).unwrap());

    // Double-spend must fail (local oracle).
    let spend2 = spend_anyone_can_spend(cb1, 0, bitcoin::Amount::from_sat(48_0000_0000));
    let b_bad = mine_regtest_block(
        b_spend.block_hash(),
        b_spend.header.time + 600,
        spend_h + 1,
        vec![spend2],
    );
    let err = accept_and_connect_block(&q, &params, Height(spend_h + 1), &b_bad, ms);
    assert!(
        matches!(err, Err(ConsensusError::PrevoutSpent)),
        "double-spend must be PrevoutSpent, got {err:?}"
    );

    // Disconnect tip unspends local.
    q.disconnect_tip().unwrap();
    assert_eq!(q.tip_height(), Some(Height(spend_h - 1)));
    assert!(
        !q.is_outpoint_spent(cb1.as_byte_array(), 0).unwrap(),
        "disconnect must clear local spend"
    );

    // Re-confirm original spend after disconnect.
    accept_and_connect_block(&q, &params, Height(spend_h), &b_spend, ms)
        .expect("re-confirm spend after disconnect");
    assert!(q.is_outpoint_spent(cb1.as_byte_array(), 0).unwrap());

    // Turning spend index off with non-empty tip clears ready → confirm blocked.
    q.set_spend_index(true);
    q.set_spend_index(false);
    assert!(!q.spent_local_ready());
    let blocked = accept_and_connect_block(&q, &params, Height(spend_h + 1), &b_bad, ms);
    assert!(blocked.is_err(), "confirm blocked until rebuild");
    q.rebuild_spent_local_to_tip().unwrap();
    assert!(q.spent_local_ready());
    assert!(
        q.is_outpoint_spent(cb1.as_byte_array(), 0).unwrap(),
        "rebuild must restore spends from confirmed chain"
    );
    let err2 = accept_and_connect_block(&q, &params, Height(spend_h + 1), &b_bad, ms);
    assert!(
        matches!(err2, Err(ConsensusError::PrevoutSpent)),
        "after rebuild double-spend still rejected, got {err2:?}"
    );
}

/// Re-confirm path must not surface `ibd utxo duplicate create` (mainnet @519).
/// Class C can succeed while UTXO apply is retried; inserts are idempotent and
/// height-monotonic.
#[test]
fn ibd_utxo_reapply_same_height_is_idempotent() {
    use rbitcoin_consensus::{
        accept_and_connect_block, ChainParams, Milestone,
    };
    use rbitcoin_test::mine::{mine_regtest_block, regtest_genesis};

    let td = TestDatadir::new().unwrap();
    let q = Query::open_or_create(td.store_path()).unwrap();
    q.set_spend_index(false);
    q.set_tx_index(false);
    q.enable_ibd_utxo().unwrap();
    let ms = Milestone::NONE;
    let params = ChainParams::regtest();

    let genesis = regtest_genesis();
    accept_and_connect_block(&q, &params, Height::GENESIS, &genesis, ms).unwrap();
    let mut tip = genesis.block_hash();
    let mut tip_time = genesis.header.time;

    let b1 = mine_regtest_block(tip, tip_time + 600, 1, vec![]);
    accept_and_connect_block(&q, &params, Height(1), &b1, ms).unwrap();
    tip = b1.block_hash();
    tip_time = b1.header.time;

    // Simulate partial-apply retry: same creates at same tip height.
    let cb = b1.txdata[0].compute_txid().to_byte_array();
    let cb_fk = q
        .ibd_utxo_create_fk(&cb, 0)
        .unwrap()
        .expect("height 1 coinbase in UTXO");
    q.apply_ibd_utxo_block(&[], &[(cb, 0, cb_fk)], 1)
        .expect("re-apply tip height must be no-op / idempotent");
    q.apply_ibd_utxo_block(&[], &[(cb, 0, cb_fk)], 1)
        .expect("second re-apply still ok");

    let b2 = mine_regtest_block(tip, tip_time + 600, 2, vec![]);
    accept_and_connect_block(&q, &params, Height(2), &b2, ms)
        .expect("next height after re-apply must confirm");
    assert_eq!(q.tip_height(), Some(Height(2)));
}

/// Multi-block `confirm_archived_run` must insert UTXO creates (tx_fks live on
/// Class C items after mem::take). Regression for mainnet tip=169 reject @170
/// PrevoutSpent: empty creates left the first real spend as a false miss.
#[test]
fn ibd_utxo_multi_block_run_keeps_creates() {
    use rbitcoin_consensus::{
        accept_and_archive_block, accept_and_connect_block, confirm_archived_run, ChainParams,
        Milestone,
    };
    use rbitcoin_test::mine::{mine_regtest_block, regtest_genesis, spend_anyone_can_spend};

    let td = TestDatadir::new().unwrap();
    let q = Query::open_or_create(td.store_path()).unwrap();
    q.set_spend_index(false);
    q.set_tx_index(false);
    q.enable_ibd_utxo().unwrap();
    let ms = Milestone::NONE;
    let params = ChainParams::regtest();
    let maturity = params.coinbase_maturity();

    let genesis = regtest_genesis();
    accept_and_connect_block(&q, &params, Height::GENESIS, &genesis, ms).unwrap();
    let mut tip = genesis.block_hash();
    let mut tip_time = genesis.header.time;

    let b1 = mine_regtest_block(tip, tip_time + 600, 1, vec![]);
    let cb1 = b1.txdata[0].compute_txid();
    accept_and_archive_block(&q, &params, Height(1), &b1, ms).unwrap();
    tip = b1.block_hash();
    tip_time = b1.header.time;

    let last_pad = maturity + 1;
    let mut run: Vec<(Height, [u8; 32])> = vec![(Height(1), b1.block_hash().to_byte_array())];
    for h in 2..=last_pad {
        let b = mine_regtest_block(tip, tip_time + 600, h, vec![]);
        accept_and_archive_block(&q, &params, Height(h), &b, ms).unwrap();
        tip = b.block_hash();
        tip_time = b.header.time;
        run.push((Height(h), b.block_hash().to_byte_array()));
    }
    let spend_h = last_pad + 1;
    let spend = spend_anyone_can_spend(cb1, 0, Amount::from_sat(49_0000_0000));
    let b_spend = mine_regtest_block(tip, tip_time + 600, spend_h, vec![spend]);
    accept_and_archive_block(&q, &params, Height(spend_h), &b_spend, ms).unwrap();
    run.push((
        Height(spend_h),
        b_spend.block_hash().to_byte_array(),
    ));

    // One multi-height confirm (IBD path): must apply creates from every height
    // so the spend of cb1@0 in the last block does not false-positive as spent.
    confirm_archived_run(&q, &params, ms, &run)
        .expect("multi-block UTXO run must confirm pad+spend");
    assert_eq!(q.tip_height(), Some(Height(spend_h)));
    assert!(
        q.is_outpoint_spent(cb1.as_byte_array(), 0).unwrap(),
        "cb1 coinbase must be spent after multi-block run"
    );
    assert!(
        q.catchup_is_spent(cb1.as_byte_array(), 0).unwrap(),
        "UTXO oracle must mark spent prevout"
    );
    // Spend creates a new unspent outpoint — must be present in UTXO.
    let spend_txid = b_spend.txdata[1].compute_txid();
    assert!(
        !q.catchup_is_spent(spend_txid.as_byte_array(), 0).unwrap(),
        "spend output must be unspent in mmap UTXO after multi-block apply"
    );
}

/// Mainnet @546 shape: single-input parent with 2 outs, next height spends both
/// in one tx. Regression for coinbase_info cache treating `None` + `input_count==1`
/// as coinbase → MissingPrevout on the second prevout of the same parent.
#[test]
fn confirm_spend_both_vouts_of_one_input_parent() {
    use rbitcoin_consensus::{
        accept_and_archive_block, accept_and_connect_block, confirm_archived_at, ChainParams,
        Milestone,
    };
    use rbitcoin_test::mine::{
        mine_regtest_block, regtest_genesis, spend_many_anyone_can_spend, split_anyone_can_spend,
    };

    let td = TestDatadir::new().unwrap();
    let q = Query::open_or_create(td.store_path()).unwrap();
    q.set_spend_index(false);
    q.set_tx_index(false);
    q.enable_ibd_utxo().unwrap();
    let ms = Milestone::NONE;
    let params = ChainParams::regtest();
    let maturity = params.coinbase_maturity();

    let genesis = regtest_genesis();
    accept_and_connect_block(&q, &params, Height::GENESIS, &genesis, ms).unwrap();
    let mut tip = genesis.block_hash();
    let mut tip_time = genesis.header.time;

    let b1 = mine_regtest_block(tip, tip_time + 600, 1, vec![]);
    let cb1 = b1.txdata[0].compute_txid();
    accept_and_connect_block(&q, &params, Height(1), &b1, ms).unwrap();
    tip = b1.block_hash();
    tip_time = b1.header.time;

    let last_pad = maturity + 1;
    for h in 2..=last_pad {
        let b = mine_regtest_block(tip, tip_time + 600, h, vec![]);
        accept_and_connect_block(&q, &params, Height(h), &b, ms).unwrap();
        tip = b.block_hash();
        tip_time = b.header.time;
    }

    // Height H: 1-in / 2-out parent (anyone-can-spend).
    let split_h = last_pad + 1;
    let split = split_anyone_can_spend(
        cb1,
        0,
        &[Amount::from_sat(20_0000_0000), Amount::from_sat(29_0000_0000)],
    );
    let b_split = mine_regtest_block(tip, tip_time + 600, split_h, vec![split]);
    let parent_txid = b_split.txdata[1].compute_txid();
    accept_and_connect_block(&q, &params, Height(split_h), &b_split, ms).unwrap();
    tip = b_split.block_hash();
    tip_time = b_split.header.time;

    // Height H+1: one tx spends both vouts of that parent.
    let merge_h = split_h + 1;
    let merge = spend_many_anyone_can_spend(
        &[(parent_txid, 0), (parent_txid, 1)],
        Amount::from_sat(48_0000_0000),
    );
    let b_merge = mine_regtest_block(tip, tip_time + 600, merge_h, vec![merge]);
    accept_and_archive_block(&q, &params, Height(merge_h), &b_merge, ms).unwrap();
    confirm_archived_at(
        &q,
        &params,
        Height(merge_h),
        &b_merge.block_hash().to_byte_array(),
        ms,
    )
    .expect("spending both vouts of a 1-in parent must not MissingPrevout");
    assert_eq!(q.tip_height(), Some(Height(merge_h)));
    assert!(q.catchup_is_spent(parent_txid.as_byte_array(), 0).unwrap());
    assert!(q.catchup_is_spent(parent_txid.as_byte_array(), 1).unwrap());
}

/// Sequential confirm_archived_run + failed confirm must not poison spent_local.
#[test]
fn confirm_run_sequential_and_failed_no_spend_poison() {
    use rbitcoin_consensus::{
        accept_and_archive_block, accept_and_connect_block, confirm_archived_at,
        confirm_archived_run, ChainParams, Milestone,
    };
    use rbitcoin_test::mine::{mine_regtest_block, regtest_genesis};

    let td = TestDatadir::new().unwrap();
    let q = Query::open_or_create(td.store_path()).unwrap();
    q.set_tx_index(false);
    q.set_spend_index(false);
    let ms = Milestone { height: 0 };
    let params = ChainParams::regtest();

    let genesis = regtest_genesis();
    accept_and_connect_block(&q, &params, Height::GENESIS, &genesis, ms).unwrap();
    let mut tip = genesis.block_hash();
    let mut tip_time = genesis.header.time;
    let mut hashes = Vec::new();
    for h in 1u32..=4 {
        let b = mine_regtest_block(tip, tip_time + 600, h, vec![]);
        accept_and_archive_block(&q, &params, Height(h), &b, ms).unwrap();
        tip = b.block_hash();
        tip_time = b.header.time;
        hashes.push(b.block_hash().to_byte_array());
    }

    let run: Vec<_> = (1u32..=3)
        .map(|h| (Height(h), hashes[(h - 1) as usize]))
        .collect();
    confirm_archived_run(&q, &params, ms, &run).expect("sequential run");
    assert_eq!(q.tip_height(), Some(Height(3)));

    // Missing body fails without advancing tip; next single confirm still works.
    let bad = confirm_archived_at(&q, &params, Height(5), &[0xab; 32], ms);
    assert!(bad.is_err());
    confirm_archived_at(&q, &params, Height(4), &hashes[3], ms).expect("confirm tip+1");
    assert_eq!(q.tip_height(), Some(Height(4)));
}

// ─── Consensus + reconstruct: one mature mine, many assertions ──────────────

/// Single mature-chain build covers:
/// - accept genesis + long mine (reopen tip)
/// - coinbase maturity + spend + double-spend reject
/// - external prev_txid on spend + reconstruct
/// - reconstruct after reopen (sampled + multi-tx spend block)
/// - store-backed locator/headers helpers
/// - service flags
#[test]
fn consensus_mature_chain_spend_and_reconstruct() {
    use bitcoin::p2p::ServiceFlags;
    use rbitcoin_net::local_service_flags;
    use rbitcoin_store::InputRecord;

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
    let spend_block = &chain.blocks[chain.spend_height as usize];
    assert!(
        spend_block.txdata.len() >= 2,
        "spend block should be multi-tx"
    );

    // External prev_txid on Class A + reconstruct.
    let spend_txid = spend_block.txdata[1].compute_txid().to_byte_array();
    let (_spend_fk, rec) = q.get_tx_by_txid(&spend_txid).unwrap().expect("spend indexed");
    let inp = q.tx_input(&rec, 0).unwrap();
    assert_eq!(
        q.resolve_prev_txid(&inp).unwrap(),
        chain.matured_coinbase_txid.to_byte_array()
    );
    let enc = InputRecord {
        prev_txid: inp.prev_txid,
        prev_index: inp.prev_index,
        sequence: inp.sequence,
        script_sig: inp.script_sig.clone(),
        witness: inp.witness.clone(),
    }
    .encode();
    assert!(enc.len() > 32, "external prev includes 32-byte txid: {}", enc.len());
    assert_reconstruct_eq(&q, chain.spend_height, spend_block);
    let cbin = q
        .tx_input(&q.get_tx(q.block_tx_fks(Height(chain.spend_height)).unwrap()[0]).unwrap(), 0)
        .unwrap();
    assert!(cbin.is_coinbase());

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

/// Kill mid-Class-C can leave creates on disk with tip not advanced. After reopen,
/// sequential body warm + skip must prevent duplicate creates (no chain walk).
#[test]
fn scripthash_reopen_warm_prevents_dup_creates() {
    use rbitcoin_store::{script_hash, ScriptHashRecord};
    use std::collections::HashMap;

    let td = TestDatadir::new().unwrap();
    let path = td.store_path();
    let q = Query::open_or_create(&path).unwrap();
    let params = ChainParams::regtest();
    let _chain = build_mature_regtest_with_spend(&q, &params);
    let n0 = q.scripthash_entry_count();
    assert!(n0 > 0);

    // Snapshot durable creates (simulates disk after kill).
    let mut durable: Vec<ScriptHashRecord> = Vec::new();
    q.store()
        .scripthash
        .for_each_live_create(|create_tx_fk, vout| {
            // scripthash unknown from body; only need create_tx_fk+vout for append skip test
            durable.push(ScriptHashRecord {
                scripthash: [0u8; 32],
                create_tx_fk,
                vout,
                next: rbitcoin_primitives::Fk::NULL,
                txid: [0u8; 32],
                value: 0,
                create_height: 0,
            });
        })
        .unwrap();
    assert_eq!(durable.len() as u64, n0);
    drop(q);

    // Reopen: cold process caches. Warm loads create_tx set from body.
    let q = Query::open_or_create(&path).unwrap();
    q.warm_scripthash_create_index().unwrap();

    // Simulate re-confirm: only append create_tx_fks not already durable.
    // Confirm path uses a height watermark; this checks durable body coverage.
    let mut indexed = std::collections::HashSet::new();
    q.store()
        .scripthash
        .for_each_live_create(|c, _| {
            indexed.insert(c.0);
        })
        .unwrap();
    let to_put: Vec<_> = durable
        .into_iter()
        .filter(|r| !indexed.contains(&r.create_tx_fk.0))
        .collect();
    assert!(
        to_put.is_empty(),
        "after warm, all durable create txs must be considered indexed"
    );
    assert_eq!(q.scripthash_entry_count(), n0);

    // Naive append without skip would dup; with skip set empty put — count unchanged.
    let mut heads = HashMap::new();
    q.store()
        .scripthash
        .put_create_batch_append(&to_put, &mut heads)
        .unwrap();
    assert_eq!(q.scripthash_entry_count(), n0); // empty to_put; count unchanged

    let sh = script_hash(&[0x51]);
    let bal = q.scripthash_balance(&sh).unwrap();
    assert!(bal.confirmed > 0);
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
        let prev = b.header.prev_blockhash.to_byte_array();
        ring.push_tip(h, b.block_hash().to_byte_array(), prev, wire)
            .unwrap();
        tip = b.block_hash();
        tip_time = b.header.time;
        blocks.push(b);
    }
    // depth 3 → keep heights 3,4,5 (max_height-2..=max)
    assert!(!ring.contains_height(1));
    assert!(ring.contains_height(5));
    assert!(ring
        .get_by_hash(&blocks[5].block_hash().to_byte_array())
        .is_some());

    // Competing tip at height 5 (same parent as block 5) — both retained.
    let fork = mine_regtest_block(
        blocks[4].block_hash(),
        blocks[4].header.time + 601,
        5,
        vec![],
    );
    assert_ne!(fork.block_hash(), blocks[5].block_hash());
    let mut fork_wire = Vec::new();
    fork.consensus_encode(&mut fork_wire).unwrap();
    ring.push(
        5,
        fork.block_hash().to_byte_array(),
        fork.header.prev_blockhash.to_byte_array(),
        fork_wire,
        true,
    )
    .unwrap();
    let at5 = ring.get_all_at_height(5);
    assert_eq!(at5.len(), 2, "both fork tips at height 5 must be held");
    assert!(ring.contains_hash(&fork.block_hash().to_byte_array()));
    assert!(ring.contains_hash(&blocks[5].block_hash().to_byte_array()));

    // Finalize through height 4 → drop wire ≤ 4 (both tips at 5 remain)
    q.set_archive_mode(true).unwrap();
    q.finalize_through(4).unwrap();
    ring.drop_through(4).unwrap();
    assert!(!ring.contains_height(4));
    assert_eq!(ring.get_all_at_height(5).len(), 2);
    let ep = q.archive_epoch();
    assert!(ep.archive_mode);
    assert_eq!(ep.finalized_height, Some(4));
    assert!(ep.is_soft_zone(5));
    assert!(!ep.is_soft_zone(4));

    let q2 = Query::open_or_create(td.store_path()).unwrap();
    assert_eq!(q2.archive_epoch().finalized_height, Some(4));

    let ring2 = WireRing::with_dir(3, &wire_dir).unwrap();
    assert_eq!(ring2.get_all_at_height(5).len(), 2);
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

    // Milestone: skip scripts only — prevouts still run; coinbase chain is fine.
    let td2 = TestDatadir::new().unwrap();
    let q2 = Query::open_or_create(td2.store_path()).unwrap();
    let ms = Milestone { height: 100 };
    assert!(ms.skips_scripts_at(1));
    assert!(ms.skips_scripts_at(1));
    accept_and_connect_block(&q2, &params, Height::GENESIS, &genesis, ms).unwrap();
    let b1 = mine_regtest_block(genesis.block_hash(), genesis.header.time + 1, 1, vec![]);
    accept_and_connect_block(&q2, &params, Height(1), &b1, ms).unwrap();
    assert_eq!(q2.tip_height(), Some(Height(1)));

    // Under a high milestone, a block that spends a missing prevout must still fail
    // (connect is not fully skipped — only scripts).
    let td3 = TestDatadir::new().unwrap();
    let q3 = Query::open_or_create(td3.store_path()).unwrap();
    let ms_hi = Milestone { height: 1_000_000 };
    accept_and_connect_block(&q3, &params, Height::GENESIS, &genesis, ms_hi).unwrap();
    // Child of genesis but with an extra invalid non-coinbase input path: mine a
    // normal block then mutate... easier: accept only genesis, then try connect
    // with wrong prev link is header failure. Use validate_block_connect on a
    // synthetic spend of unknown outpoint after a valid height-1 coinbase-only
    // block would need a second tx — mine empty block then skip.
    //
    // Spend an outpoint that never existed: build via mine with empty spends and
    // reject a manually broken second block that points at a fake prevout by
    // going through validate_block_connect.
    use bitcoin::{Amount, OutPoint, ScriptBuf, Sequence, Transaction, TxIn, TxOut, Witness};
    let b_ok = mine_regtest_block(genesis.block_hash(), genesis.header.time + 1, 1, vec![]);
    accept_and_connect_block(&q3, &params, Height(1), &b_ok, ms_hi).unwrap();
    // Fabricate a height-2 block spending a nonexistent outpoint.
    let coinbase = &b_ok.txdata[0];
    let _ = coinbase;
    let mut bad = mine_regtest_block(b_ok.block_hash(), b_ok.header.time + 1, 2, vec![]);
    // Append a phantom spend of a random outpoint.
    let phantom = Transaction {
        version: bitcoin::transaction::Version::TWO,
        lock_time: bitcoin::absolute::LockTime::ZERO,
        input: vec![TxIn {
            previous_output: OutPoint {
                txid: bitcoin::Txid::from_byte_array([0xcd; 32]),
                vout: 0,
            },
            script_sig: ScriptBuf::new(),
            sequence: Sequence::MAX,
            witness: Witness::new(),
        }],
        output: vec![TxOut {
            value: Amount::from_sat(1),
            script_pubkey: ScriptBuf::new(),
        }],
    };
    bad.txdata.push(phantom);
    bad.header.merkle_root = bad.compute_merkle_root().unwrap();
    let ctx = ValidationContext {
        params: &params,
        height: Height(2),
        milestone: ms_hi,
    };
    let err = validate_block_connect(&q3, &bad, &ctx, None).expect_err("prevout must fail");
    let msg = err.to_string().to_lowercase();
    assert!(
        msg.contains("prev")
            || msg.contains("not found")
            || msg.contains("missing")
            || msg.contains("input")
            || msg.contains("spend"),
        "expected prevout failure under milestone, got: {err}"
    );
}
