//! Multi-node P2P integration tests.
//!
//! Fast mesh tests run in default `cargo test`.
//! Heavier topology checks are `#[ignore]` and run via `scripts/integration.sh`
//! (periodic / CI nightly).

use bitcoin::hashes::Hash;
use rbitcoin_consensus::{ChainParams, Milestone};
use rbitcoin_net::P2PNode;
use rbitcoin_primitives::Height;
use rbitcoin_query::Query;
use rbitcoin_test::mine::{mine_regtest_block, regtest_genesis};
use std::time::Duration;
use tempfile::TempDir;

async fn start_node(dir: &TempDir) -> P2PNode {
    let q = Query::open_or_create(dir.path().join("store")).unwrap();
    P2PNode::start(
        "127.0.0.1:0".parse().unwrap(),
        q,
        ChainParams::regtest(),
        Milestone::NONE,
    )
    .await
    .expect("listen")
}

async fn seed_chain(node: &P2PNode, blocks: u32) {
    let genesis = regtest_genesis();
    node.ingest_block(0, genesis.clone()).unwrap();
    let mut tip = genesis.block_hash();
    let mut time = genesis.header.time;
    for h in 1..=blocks {
        let b = mine_regtest_block(tip, time + 600, h, vec![]);
        tip = b.block_hash();
        time = b.header.time;
        node.ingest_block(h, b).unwrap();
    }
}

/// Always-on: two nodes, seed has 8 blocks, peer syncs tip.
#[tokio::test]
async fn two_node_header_and_block_sync() {
    let seed_dir = TempDir::new().unwrap();
    let peer_dir = TempDir::new().unwrap();

    let seed = start_node(&seed_dir).await;
    seed_chain(&seed, 8).await;
    assert_eq!(seed.cache.tip_height(), Some(8));
    assert_eq!(seed.query.tip_height(), Some(Height(8)));

    let peer = start_node(&peer_dir).await;
    let n = peer.sync_from(seed.local_addr).await.expect("sync");
    assert!(n >= 8, "downloaded {n}");
    peer.wait_height(8, Duration::from_secs(5))
        .await
        .expect("tip");

    assert_eq!(peer.cache.tip_height(), Some(8));
    assert_eq!(peer.query.tip_height(), Some(Height(8)));
    assert_eq!(
        peer.cache.tip_hash().unwrap(),
        seed.cache.tip_hash().unwrap()
    );

    seed.shutdown().await;
    peer.shutdown().await;
}

/// Phase 4: seeder shuts down and restarts with empty RAM cache; peer still IBD-syncs
/// via store-backed getheaders + reconstruct getdata.
#[tokio::test]
async fn serve_after_restart_via_reconstruct() {
    let seed_dir = TempDir::new().unwrap();
    let peer_dir = TempDir::new().unwrap();

    let seed = start_node(&seed_dir).await;
    seed_chain(&seed, 10).await;
    let tip_hash = seed.cache.tip_hash().unwrap();
    seed.shutdown().await;

    // Restart seeder on same store — cache is empty; serve must use reconstruct.
    let seed = start_node(&seed_dir).await;
    assert!(
        seed.cache.is_empty(),
        "restarted seeder must not rely on warm RAM cache"
    );
    assert_eq!(seed.query.tip_height(), Some(Height(10)));

    let peer = start_node(&peer_dir).await;
    let n = peer.sync_from(seed.local_addr).await.expect("sync after restart");
    assert!(n >= 10, "downloaded {n}");
    peer.wait_height(10, Duration::from_secs(10))
        .await
        .expect("tip");

    assert_eq!(peer.query.tip_height(), Some(Height(10)));
    let peer_tip = peer
        .query
        .header_at_height(Height(10))
        .unwrap()
        .unwrap()
        .1
        .hash;
    assert_eq!(peer_tip, tip_hash.to_byte_array());

    // Peer can reconstruct every height from its own store.
    for h in 0..=10u32 {
        let b = peer
            .query
            .reconstruct_block_at_height(Height(h))
            .expect("peer reconstruct");
        assert_eq!(b.header.block_hash().to_byte_array(), peer
            .query
            .header_at_height(Height(h))
            .unwrap()
            .unwrap()
            .1
            .hash);
    }

    seed.shutdown().await;
    peer.shutdown().await;
}

/// Always-on: peer serves after syncing — second peer can sync from first peer.
#[tokio::test]
async fn three_node_relay_path() {
    let d0 = TempDir::new().unwrap();
    let d1 = TempDir::new().unwrap();
    let d2 = TempDir::new().unwrap();

    let seed = start_node(&d0).await;
    seed_chain(&seed, 5).await;

    let mid = start_node(&d1).await;
    mid.sync_from(seed.local_addr).await.unwrap();
    mid.wait_height(5, Duration::from_secs(5)).await.unwrap();

    let leaf = start_node(&d2).await;
    // Sync from mid (not original seed) — exercises serve after sync.
    leaf.sync_from(mid.local_addr).await.unwrap();
    leaf.wait_height(5, Duration::from_secs(5)).await.unwrap();

    assert_eq!(leaf.cache.tip_hash(), seed.cache.tip_hash());
    assert_eq!(leaf.query.tip_height(), Some(Height(5)));

    seed.shutdown().await;
    mid.shutdown().await;
    leaf.shutdown().await;
}

/// Multi-peer sync API: second address fails, first succeeds.
#[tokio::test]
async fn sync_from_peers_tries_list() {
    let seed_dir = TempDir::new().unwrap();
    let peer_dir = TempDir::new().unwrap();

    let seed = start_node(&seed_dir).await;
    seed_chain(&seed, 4).await;

    let peer = start_node(&peer_dir).await;
    let bad: std::net::SocketAddr = "127.0.0.1:1".parse().unwrap();
    let n = peer
        .sync_from_peers(&[bad, seed.local_addr])
        .await
        .expect("multi-peer sync");
    assert!(n >= 4, "downloaded {n}");
    peer.wait_height(4, Duration::from_secs(5))
        .await
        .unwrap();

    seed.shutdown().await;
    peer.shutdown().await;
}

/// Long-running node entry: listen briefly, connect to seeder, exit via max_run_secs.
#[tokio::test]
async fn node_run_p2p_short() {
    use rbitcoin_node::{run_p2p, NodeConfig};
    use rbitcoin_primitives::Network;

    let seed_dir = TempDir::new().unwrap();
    let node_dir = TempDir::new().unwrap();

    let seed = start_node(&seed_dir).await;
    seed_chain(&seed, 3).await;
    let seed_addr = seed.local_addr;

    let mut cfg = NodeConfig::default()
        .with_datadir(node_dir.path())
        .with_network(Network::Regtest)
        .with_p2p_listen("127.0.0.1:0".parse().unwrap());
    cfg.connect = vec![seed_addr];
    cfg.use_seeds = false;
    cfg.max_run_secs = Some(0); // sync then exit immediately

    run_p2p(cfg).await.expect("run_p2p");

    // Reopen store — should have synced chain
    let q = Query::open_or_create(node_dir.path().join("store")).unwrap();
    assert_eq!(q.tip_height(), Some(Height(3)));

    seed.shutdown().await;
}

/// Periodic / holistic mesh: larger chain, multi-hop, concurrent peers.
/// Run: `cargo test -p rbitcoin-test --test integration_multinode -- --ignored --nocapture`
#[tokio::test]
#[ignore = "periodic multi-node mesh; run via scripts/integration.sh"]
async fn multinode_mesh_periodic() {
    const HEIGHT: u32 = 40;
    let dirs: Vec<TempDir> = (0..4).map(|_| TempDir::new().unwrap()).collect();

    let seed = start_node(&dirs[0]).await;
    seed_chain(&seed, HEIGHT).await;

    // Fan-out: three peers sync from seed concurrently.
    let mut peers = Vec::new();
    for d in dirs.iter().skip(1) {
        peers.push(start_node(d).await);
    }

    let addr = seed.local_addr;
    // Sequential sync is enough for the mesh soak; concurrent dial can be added later.
    for p in &peers {
        p.sync_from(addr).await.expect("peer sync");
        p.wait_height(HEIGHT, Duration::from_secs(30))
            .await
            .expect("height");
        assert_eq!(p.cache.tip_hash(), seed.cache.tip_hash());
    }

    // Cross-link: peer[0] already has chain; peer[1] re-syncs from peer[0] (idempotent).
    let n = peers[1].sync_from(peers[0].local_addr).await.unwrap();
    // May download 0 if already at tip
    let _ = n;
    assert_eq!(peers[1].query.tip_height(), Some(Height(HEIGHT)));

    seed.shutdown().await;
    for p in peers {
        p.shutdown().await;
    }
}
