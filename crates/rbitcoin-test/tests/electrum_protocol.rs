//! Electrum protocol fixtures against a local server + mature regtest chain.

use rbitcoin_consensus::{ChainParams, Milestone};
use rbitcoin_electrum::{electrum_scripthash_hex, run_electrum, ElectrumConfig};
use rbitcoin_query::Query;
use rbitcoin_test::build_mature_regtest_with_spend;
use serde_json::{json, Value};
use std::sync::Arc;
use rbitcoin_test::TempDir;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::sync::broadcast;

#[tokio::test]
async fn electrum_server_version_history_balance() {
    let dir = TempDir::new().unwrap();
    let q = Query::open_or_create(dir.path().join("store")).unwrap();
    let params = ChainParams::regtest();
    let _chain = build_mature_regtest_with_spend(&q, &params);
    let _ = Milestone::NONE;

    let q = Arc::new(q);
    let (tip_tx, _) = broadcast::channel(4);
    let cfg = ElectrumConfig::for_params("127.0.0.1:0".parse().unwrap(), &params);
    let handle = run_electrum(cfg, q.clone(), params, tip_tx.clone(), None)
        .await
        .expect("electrum listen");

    let mut stream = TcpStream::connect(handle.local_addr).await.unwrap();

    // Need re-read pattern: after write we use BufReader which consumes stream.
    // Use split request/response carefully with single stream.
    {
        let req = json!({"jsonrpc":"2.0","id":1,"method":"server.version","params":["test","1.4"]});
        let mut line = serde_json::to_string(&req).unwrap();
        line.push('\n');
        stream.write_all(line.as_bytes()).await.unwrap();
    }
    let mut reader = BufReader::new(&mut stream);
    let mut resp_line = String::new();
    reader.read_line(&mut resp_line).await.unwrap();
    let v: Value = serde_json::from_str(&resp_line).unwrap();
    assert!(v.get("result").is_some(), "{v}");
    let ver = v["result"].as_array().unwrap();
    assert_eq!(ver.len(), 2);

    // OP_TRUE scripthash
    let sh_hex = electrum_scripthash_hex(&[0x51]);
    let req = json!({
        "jsonrpc":"2.0","id":2,
        "method":"blockchain.scripthash.get_history",
        "params":[sh_hex]
    });
    let mut line = serde_json::to_string(&req).unwrap();
    line.push('\n');
    // reader holds &mut stream — drop reader first by re-getting stream from reader
    let stream = reader.into_inner();
    stream.write_all(line.as_bytes()).await.unwrap();
    let mut reader = BufReader::new(stream);
    resp_line.clear();
    reader.read_line(&mut resp_line).await.unwrap();
    let v: Value = serde_json::from_str(&resp_line).unwrap();
    let hist = v["result"].as_array().expect("history array");
    assert!(!hist.is_empty());

    let sh_hex = electrum_scripthash_hex(&[0x51]);
    let req = json!({
        "jsonrpc":"2.0","id":3,
        "method":"blockchain.scripthash.get_balance",
        "params":[sh_hex]
    });
    let mut line = serde_json::to_string(&req).unwrap();
    line.push('\n');
    let stream = reader.into_inner();
    stream.write_all(line.as_bytes()).await.unwrap();
    let mut reader = BufReader::new(stream);
    resp_line.clear();
    reader.read_line(&mut resp_line).await.unwrap();
    let v: Value = serde_json::from_str(&resp_line).unwrap();
    assert!(v["result"]["confirmed"].as_i64().unwrap_or(0) > 0);

    // Empty mempool
    let req = json!({
        "jsonrpc":"2.0","id":4,
        "method":"blockchain.scripthash.get_mempool",
        "params":[electrum_scripthash_hex(&[0x51])]
    });
    let mut line = serde_json::to_string(&req).unwrap();
    line.push('\n');
    let stream = reader.into_inner();
    stream.write_all(line.as_bytes()).await.unwrap();
    let mut reader = BufReader::new(stream);
    resp_line.clear();
    reader.read_line(&mut resp_line).await.unwrap();
    let v: Value = serde_json::from_str(&resp_line).unwrap();
    assert_eq!(v["result"], json!([]));

    // Headers subscribe returns tip
    let req = json!({
        "jsonrpc":"2.0","id":5,
        "method":"blockchain.headers.subscribe",
        "params":[]
    });
    let mut line = serde_json::to_string(&req).unwrap();
    line.push('\n');
    let stream = reader.into_inner();
    stream.write_all(line.as_bytes()).await.unwrap();
    let mut reader = BufReader::new(stream);
    resp_line.clear();
    reader.read_line(&mut resp_line).await.unwrap();
    let v: Value = serde_json::from_str(&resp_line).unwrap();
    assert!(v["result"]["height"].as_u64().unwrap() > 0);
    assert!(v["result"]["hex"].as_str().unwrap().len() == 160); // 80-byte header hex

    // Tip push: server must forward TipNotify to subscribed clients.
    let tip_h = v["result"]["height"].as_u64().unwrap() as u32;
    let tip_hex = v["result"]["hex"].as_str().unwrap().to_string();
    tip_tx
        .send(rbitcoin_electrum::TipNotify {
            height: tip_h + 1,
            header_hex: tip_hex.clone(),
        })
        .expect("tip push");
    resp_line.clear();
    // Notification has no id — wait for one line.
    tokio::time::timeout(std::time::Duration::from_secs(2), reader.read_line(&mut resp_line))
        .await
        .expect("tip notification timeout")
        .unwrap();
    let push: Value = serde_json::from_str(&resp_line).unwrap();
    assert_eq!(
        push["method"].as_str(),
        Some("blockchain.headers.subscribe")
    );
    assert_eq!(push["params"][0]["height"].as_u64(), Some((tip_h + 1) as u64));

    drop(reader);
    handle.shutdown().await;
}

/// Milestone-style IBD leaves tx.head / points empty; tip-mode backfill restores them.
/// Scripthash creates are always written on confirm (not optional / no recovery API).
#[test]
fn backfill_tx_head_and_points_after_index_off() {
    use bitcoin::hashes::Hash;
    use rbitcoin_consensus::{accept_and_connect_block, ChainParams, Milestone};
    use rbitcoin_primitives::Height;
    use rbitcoin_test::mine::{mine_regtest_block, regtest_genesis};

    let dir = TempDir::new().unwrap();
    let q = Query::open_or_create(dir.path().join("store")).unwrap();
    let params = ChainParams::regtest();

    // Simulate milestone IBD: no durable tx.head / points; scripthash still on.
    // Catch-up spentness is light UTXO only (required when spend_index off).
    q.set_tx_index(false);
    q.set_spend_index(false);
    q.enable_ibd_utxo().unwrap();

    let g = regtest_genesis();
    accept_and_connect_block(&q, &params, Height::GENESIS, &g, Milestone::NONE).unwrap();
    let mut tip = g.block_hash();
    let mut time = g.header.time;
    for h in 1..=5u32 {
        time += 600;
        let b = mine_regtest_block(tip, time, h, vec![]);
        accept_and_connect_block(&q, &params, Height(h), &b, Milestone::NONE).unwrap();
        tip = b.block_hash();
    }
    assert_eq!(q.tip_height(), Some(Height(5)));
    // Bodies exist but head was not filled (index off).
    assert!(q.tx_body_count() >= 6);
    assert!(
        q.tx_head_occupied() < q.tx_body_count(),
        "head should lag bodies when index off"
    );
    // Thin SH always written on confirm.
    assert!(
        q.scripthash_entry_count() > 0,
        "scripthash creates present without optional flag"
    );

    let b1 = q.reconstruct_block_at_height(Height(1)).unwrap();
    let cb_txid = b1.txdata[0].compute_txid().to_byte_array();
    assert!(
        q.tx_head_occupied() < q.tx_body_count(),
        "head lags bodies under index-off archive"
    );

    let inserted = q.backfill_tx_index(|_, _, _| {}).unwrap();
    assert!(inserted >= 6, "inserted {inserted}");
    // Tip-mode: re-enable durable head lookups (matches enter_tip_mode).
    q.set_tx_index(true);
    assert!(
        q.get_tx_by_txid(&cb_txid).unwrap().is_some(),
        "txid resolves after tx.head backfill + tx_index on"
    );

    q.set_spend_index(true);
    let (ph, ptxs) = q.backfill_point_spends(|_, _, _, _| {}).unwrap();
    assert_eq!(ph, 6);
    assert!(ptxs >= 6);

    // OP_TRUE coinbase outputs from mine_regtest_block appear under that scripthash.
    let sh = {
        use rbitcoin_store::script_hash;
        script_hash(&[0x51])
    };
    let hist = q.scripthash_history(&sh).unwrap();
    assert!(!hist.is_empty(), "scripthash history non-empty after confirm");
}
