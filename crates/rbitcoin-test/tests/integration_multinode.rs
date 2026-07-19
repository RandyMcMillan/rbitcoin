//! Multi-node P2P integration tests.
//!
//! Fast mesh tests run in default `cargo test`.
//! Heavier topology checks are `#[ignore]` and run via `scripts/integration.sh`
//! (periodic / CI nightly).

use bitcoin::hashes::Hash;
use bitcoin::BlockHash;
use rbitcoin_consensus::{ChainParams, Milestone};
use rbitcoin_net::P2PNode;
use rbitcoin_primitives::Height;
use rbitcoin_query::Query;
use rbitcoin_test::mine::{mine_regtest_block, regtest_genesis};
use std::time::Duration;
use rbitcoin_test::TempDir;

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
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
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
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
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
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
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

/// Parallel IBD: two seeder peers, client downloads with shared window.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn parallel_ibd_two_peers() {
    use rbitcoin_net::IbdConfig;

    let seed_dir = TempDir::new().unwrap();
    let mid_dir = TempDir::new().unwrap();
    let peer_dir = TempDir::new().unwrap();

    let seed = start_node(&seed_dir).await;
    seed_chain(&seed, 48).await;

    // Second server: hop-sync then both serve the client.
    let mid = start_node(&mid_dir).await;
    mid.sync_from(seed.local_addr).await.unwrap();
    mid.wait_height(48, Duration::from_secs(15)).await.unwrap();

    let client = start_node(&peer_dir).await;
    let n = client
        .parallel_sync(
            &[seed.local_addr, mid.local_addr],
            IbdConfig::for_test(),
        )
        .await
        .expect("parallel ibd");
    assert!(n >= 40, "accepted {n}");
    client
        .wait_height(48, Duration::from_secs(15))
        .await
        .expect("tip");
    assert_eq!(client.query.tip_height(), Some(Height(48)));
    assert_eq!(
        client.hub.tip_hash().unwrap(),
        seed.hub.tip_hash().unwrap()
    );

    seed.shutdown().await;
    mid.shutdown().await;
    client.shutdown().await;
}

/// Multi-peer sync API: second address fails, first succeeds.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
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

/// Phase 5: after IBD, seed announces a new tip; follower picks it up via inv/headers.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tip_follow_after_ibd() {
    let seed_dir = TempDir::new().unwrap();
    let peer_dir = TempDir::new().unwrap();

    let seed = start_node(&seed_dir).await;
    seed_chain(&seed, 5).await;

    let mut peer = start_node(&peer_dir).await;
    // Persistent follow: catch-up + stay connected for tip announce.
    peer.follow_from(seed.local_addr).await.expect("follow");
    peer.wait_height(5, Duration::from_secs(10))
        .await
        .expect("ibd via follow");

    // Seed mines block 6 — inbound peer_session should announce to follower.
    let tip = seed.cache.tip_hash().unwrap();
    let tip_time = seed
        .query
        .header_at_height(Height(5))
        .unwrap()
        .unwrap()
        .1
        .timestamp;
    let b6 = mine_regtest_block(tip, tip_time + 600, 6, vec![]);
    let h6 = b6.block_hash();
    seed.ingest_block(6, b6).unwrap();

    peer.wait_tip_hash(h6, Duration::from_secs(10))
        .await
        .expect("tip follow");
    assert_eq!(peer.query.tip_height(), Some(Height(6)));

    seed.shutdown().await;
    peer.shutdown().await;
}

/// Parallel IBD to seeder tip → long-lived follow → new tip via announce, and
/// a third peer can download history from the client (block relay / serve).
///
/// Guards the post-IBD transition: once at peer tip we must leave parallel IBD
/// and stay in tip-tracking + serve mode (not re-open sequential catch-up).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ibd_to_tip_tracking_and_block_relay() {
    use rbitcoin_net::IbdConfig;

    let seed_dir = TempDir::new().unwrap();
    let client_dir = TempDir::new().unwrap();
    let third_dir = TempDir::new().unwrap();

    let seed = start_node(&seed_dir).await;
    seed_chain(&seed, 20).await;
    let seed_tip_hash = seed.hub.tip_hash().unwrap();

    // 1) Parallel catch-up to the highest tip the seeder has.
    let mut client = start_node(&client_dir).await;
    let n = client
        .parallel_sync(&[seed.local_addr], IbdConfig::for_test())
        .await
        .expect("parallel ibd");
    assert!(n >= 20, "accepted {n}");
    client
        .wait_height(20, Duration::from_secs(15))
        .await
        .expect("client tip after ibd");
    assert_eq!(client.query.tip_height(), Some(Height(20)));
    assert_eq!(client.hub.tip_hash().unwrap(), seed_tip_hash);

    // 2) Transition: persistent follow (tip tracking), not another parallel IBD.
    client
        .follow_from(seed.local_addr)
        .await
        .expect("follow after ibd");

    // 3) Seeder extends tip — client must pick it up via inv/headers on the
    //    follow session (steady-state path).
    let tip = seed.hub.tip_hash().unwrap();
    let tip_time = seed
        .query
        .header_at_height(Height(20))
        .unwrap()
        .unwrap()
        .1
        .timestamp;
    let b21 = mine_regtest_block(tip, tip_time + 600, 21, vec![]);
    let h21 = b21.block_hash();
    seed.ingest_block(21, b21).unwrap();

    client
        .wait_tip_hash(h21, Duration::from_secs(15))
        .await
        .expect("tip tracking after ibd");
    assert_eq!(client.query.tip_height(), Some(Height(21)));

    // 4) Block relay / serve: a third node IBD-syncs **from the client** (not
    //    the original seeder), proving post-IBD history serve works.
    let third = start_node(&third_dir).await;
    let n3 = third
        .sync_from(client.local_addr)
        .await
        .expect("sync from client after ibd");
    assert!(n3 >= 20, "third downloaded {n3}");
    third
        .wait_height(21, Duration::from_secs(15))
        .await
        .expect("third tip");
    assert_eq!(third.hub.tip_hash().unwrap(), h21);

    seed.shutdown().await;
    client.shutdown().await;
    third.shutdown().await;
}



/// Phase 5: most-work reorg — longer branch wins after disconnect/connect.
#[tokio::test]
async fn reorg_to_longer_branch() {
    use rbitcoin_net::{AcceptOutcome, ChainHub};
    use rbitcoin_consensus::{ChainParams, Milestone};

    let dir = TempDir::new().unwrap();
    let q = Query::open_or_create(dir.path().join("store")).unwrap();
    let hub = ChainHub::new(q, ChainParams::regtest(), Milestone::NONE);

    let genesis = regtest_genesis();
    hub.accept_block(genesis.clone()).unwrap();
    let mut tip = genesis.block_hash();
    let mut time = genesis.header.time;
    for h in 1..=4u32 {
        let b = mine_regtest_block(tip, time + 600, h, vec![]);
        tip = b.block_hash();
        time = b.header.time;
        hub.accept_block(b).unwrap();
    }
    assert_eq!(hub.tip_height(), Some(4));

    // Fork from height 2: build longer branch 3',4',5',6'
    let fork_parent = hub
        .query
        .header_at_height(Height(2))
        .unwrap()
        .unwrap()
        .1
        .hash;
    let mut branch = Vec::new();
    let mut p = BlockHash::from_byte_array(fork_parent);
    let mut t = hub
        .query
        .header_at_height(Height(2))
        .unwrap()
        .unwrap()
        .1
        .timestamp;
    for h in 3..=6u32 {
        // Distinct nonces via time offset so hashes differ from original chain
        let b = mine_regtest_block(p, t + 601, h, vec![]);
        p = b.block_hash();
        t = b.header.time;
        branch.push(b);
    }

    let outcome = hub.accept_branch(&branch).unwrap();
    assert!(matches!(outcome, AcceptOutcome::Accepted { height: 6 }));
    assert_eq!(hub.tip_height(), Some(6));
    assert_eq!(hub.tip_hash().unwrap(), branch.last().unwrap().block_hash());
}

/// Long-running node entry: listen briefly, connect to seeder, exit via max_run_secs.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
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
