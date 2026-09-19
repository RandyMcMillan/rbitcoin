use super::*;
use rbitcoin_consensus::ChainParams;
use rbitcoin_query::testutil::FixtureChain;

use rbitcoin_query::Query;
use std::collections::{HashMap, HashSet};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;

fn tmp_store() -> (rbitcoin_query::testutil::TempDir, Query) {
    rbitcoin_query::testutil::tiny_query_labeled("electrum")
}

#[allow(clippy::cognitive_complexity)] // one fixture, many parser arms
#[test]
fn config_helpers_and_param_parsers() {
    let params = ChainParams::regtest();
    let cfg = ElectrumConfig::for_params("127.0.0.1:0".parse().unwrap(), &params);
    assert_eq!(
        cfg.genesis_hash_hex,
        rbitcoin_primitives::display_hash_hex(&params.genesis_hash.to_byte_array())
    );
    assert!(cfg.banner.contains("rbitcoin"));
    assert_eq!(cfg.tweaks_chunk, crate::tweaks::SUBSCRIBE_CHUNK);
    assert_eq!(cfg.tweaks_min_dust, crate::tweaks::DEFAULT_TWEAKS_MIN_DUST);

    let sh = electrum_scripthash_hex(&[0x51]);
    assert_eq!(sh.len(), 64);

    assert_eq!(param_u32(&json!([3]), 0).unwrap(), 3);
    assert_eq!(param_u32(&json!(["7"]), 0).unwrap(), 7);
    assert!(param_u32(&json!([]), 0).is_err());
    assert!(param_u32(&json!([true]), 0).is_err());
    assert!(param_u32(&json!([Value::Null]), 0).is_err());
    assert!(param_u32(&json!({"0": 1}), 0).is_err());
    assert!(param_u32(&json!([1.5]), 0).is_err());
    assert!(param_u32(&json!([1u64 << 32]), 0).is_err(), "u32 overflow must not wrap");
    assert_eq!(param_u32(&json!([u32::MAX as u64]), 0).unwrap(), u32::MAX);
    assert!(param_i64(&json!([true]), 0).is_err());
    assert!(param_str(&json!([false]), 0).is_err());
    assert!(param_str(&json!({}), 0).is_err());
    assert!(parse_electrum_request_line("{").is_none());
    assert!(parse_electrum_request_line("").is_none());
    assert!(parse_electrum_request_line("not-json").is_none());
    let ping_line = parse_electrum_request_line(r#"{"id":1,"method":"server.ping"}"#).unwrap();
    assert_eq!(ping_line["method"], "server.ping");
    assert!(parse_electrum_request_line("[1]").unwrap().is_array());
    assert_eq!(param_i64(&json!([-1]), 0).unwrap(), -1);
    assert_eq!(param_i64(&json!(["10"]), 0).unwrap(), 10);
    assert_eq!(param_str(&json!(["hi"]), 0).unwrap(), "hi");
    assert!(param_str(&json!([1]), 0).is_err());

    let mut sh_bytes = [0u8; 32];
    sh_bytes[0] = 0xaa;
    let sh_hex = hash_hex_rev(&sh_bytes);
    let parsed = param_scripthash(&json!([sh_hex]), 0).unwrap();
    assert_eq!(parsed, sh_bytes);
    assert!(param_scripthash(&json!(["aa"]), 0).is_err());

    let tid = param_txid(&json!([sh_hex]), 0).unwrap();
    assert_eq!(tid, sh_bytes);
    assert!(param_txid(&json!(["aabb"]), 0).is_err());
    assert!(param_txid(&json!(["zz".repeat(32)]), 0).is_err());

    // get_history window: 1-arg open + mempool; finite to excludes mempool.
    let (f, mp) = parse_get_history_window(&json!([sh_hex])).unwrap();
    assert_eq!(f.from_height, 0);
    assert!(f.to_height.is_none());
    assert!(mp);
    let (f, mp) = parse_get_history_window(&json!([sh_hex, 5])).unwrap();
    assert_eq!(f.from_height, 5);
    assert!(f.to_height.is_none());
    assert!(mp);
    let (f, mp) = parse_get_history_window(&json!([sh_hex, 2, -1])).unwrap();
    assert_eq!(f.from_height, 2);
    assert!(f.to_height.is_none());
    assert!(mp);
    let (f, mp) = parse_get_history_window(&json!([sh_hex, 1, 10])).unwrap();
    assert_eq!(f.from_height, 1);
    assert_eq!(f.to_height, Some(10));
    assert!(!mp);
    let asof_hex = "ab".repeat(32);
    let tagged = format!("asof:{asof_hex}");
    let (rest, h) =
        take_trailing_asof("blockchain.scripthash.get_balance", &json!([sh_hex, tagged]), true)
            .unwrap();
    assert!(h.is_some());
    assert_eq!(rest, json!([sh_hex]));
    let (rest_win, h_win) = take_trailing_asof(
        "blockchain.scripthash.get_history",
        &json!([sh_hex, 1, 10, tagged]),
        true,
    )
    .unwrap();
    assert!(h_win.is_some());
    assert_eq!(rest_win, json!([sh_hex, 1, 10]));
    let (rest_tx, h_tx) =
        take_trailing_asof("blockchain.transaction.get", &json!([sh_hex, tagged]), true).unwrap();
    assert!(h_tx.is_some());
    assert_eq!(rest_tx, json!([sh_hex]));
    let (rest_merkle, h_merkle) =
        take_trailing_asof("blockchain.transaction.get_merkle", &json!([sh_hex, 0, tagged]), true)
            .unwrap();
    assert!(h_merkle.is_some());
    assert_eq!(rest_merkle, json!([sh_hex, 0]));
    let (_, none) =
        take_trailing_asof("blockchain.scripthash.get_balance", &json!([sh_hex]), true).unwrap();
    assert!(none.is_none());
    let (_, not_hex) =
        take_trailing_asof("blockchain.scripthash.get_balance", &json!([sh_hex, asof_hex]), true)
            .unwrap();
    assert!(not_hex.is_none(), "bare trailing hex must not be asof (future positional hash args)");
    let (_, leftover_obj) = take_trailing_asof(
        "blockchain.scripthash.get_balance",
        &json!([sh_hex, { "other": true }]),
        true,
    )
    .unwrap();
    assert!(leftover_obj.is_none());
    let denied =
        take_trailing_asof("blockchain.scripthash.get_balance", &json!([sh_hex, tagged]), false)
            .unwrap_err();
    assert!(denied.contains("1.4.2-asof"), "asof tag without dialect: {denied}");
    assert!(take_trailing_asof(
        "blockchain.scripthash.get_balance",
        &json!([sh_hex, "asof:zz"]),
        true,
    )
    .unwrap_err()
    .contains("asof:<32-byte hex>"));
    let asof_tag = format!("asof:{}", "ab".repeat(32));
    let (_, ignored) =
        take_trailing_asof("blockchain.block.header", &json!([0, asof_tag]), true).unwrap();
    assert!(
        ignored.is_none(),
        "asof tag on a method that does not accept asof is leftover params, not a view"
    );
    assert!(parse_get_history_window(&json!({})).unwrap_err().contains("array"));

    assert!(parse_get_history_window(&json!([sh_hex, 10, 5])).unwrap_err().contains("from_height"));
    assert!(parse_get_history_window(&json!([sh_hex, 0, -2])).unwrap_err().contains("to_height"));

    let empty_status = scripthash_status(None, &[]).unwrap();
    assert!(empty_status.is_empty());
    let missing = scripthash_status(
        None,
        &[rbitcoin_query::ScriptHashHistoryItem {
            height: 1,
            txid: [1u8; 32],
            tx_fk: Fk::NULL,
            fee: None,
        }],
    )
    .unwrap_err();
    assert!(missing.contains("query"), "confirmed preimage without query: {missing}");
    let (dir, q) = tmp_store();
    let missing_hdr = scripthash_status(
        Some(&q),
        &[rbitcoin_query::ScriptHashHistoryItem {
            height: 1,
            txid: [1u8; 32],
            tx_fk: Fk::NULL,
            fee: None,
        }],
    )
    .unwrap_err();
    assert!(missing_hdr.contains("header missing"), "confirmed row without header: {missing_hdr}");
    let _ = std::fs::remove_dir_all(&dir);

    use bitcoin::hashes::Hash;
    let hdr = bitcoin::block::Header {
        version: bitcoin::block::Version::ONE,
        prev_blockhash: bitcoin::BlockHash::from_byte_array([0; 32]),
        merkle_root: bitcoin::TxMerkleNode::from_byte_array([0; 32]),
        time: 0,
        bits: bitcoin::CompactTarget::from_consensus(0x207fffff),
        nonce: 0,
    };
    let hex = header_hex(&hdr);
    assert_eq!(hex.len(), 160);

    assert_eq!(tick_scan(Some(1), Some(1)), None);
    assert_eq!(tick_scan(Some(0), Some(2)), Some(Some(vec![1, 2])));
    assert_eq!(tick_scan(Some(0), Some(32)), Some(Some((1..=32).collect())));
    assert_eq!(
        tick_scan(Some(0), Some(33)),
        Some(None),
        "gap above TICK_SCAN_MAX_GAP restatuses all"
    );
    assert_eq!(tick_scan(Some(2), Some(1)), Some(None));
    assert_eq!(tick_scan(None, Some(0)), Some(None));
    let mut last = HashMap::new();
    let mut subs = HashSet::new();
    let sh = [9u8; 32];
    subs.insert(sh);
    let first = take_new_status(&mut last, &subs, sh, "aa".into()).unwrap();
    assert_eq!(first, "aa");
    assert!(take_new_status(&mut last, &subs, sh, "aa".into()).is_none());
    assert_eq!(take_new_status(&mut last, &subs, sh, "bb".into()).unwrap(), "bb");
}

#[test]
fn cached_confirming_hash_loads_once_per_height() {
    let mut cache = HashMap::new();
    let mut loads = 0u32;
    let mut load = |h: u32| {
        loads += 1;
        Ok::<_, String>([h as u8; 32])
    };
    let a = cached_confirming_hash(7, &mut cache, &mut load).unwrap();
    let b = cached_confirming_hash(7, &mut cache, &mut load).unwrap();
    let c = cached_confirming_hash(8, &mut cache, &mut load).unwrap();
    assert_eq!(a, [7u8; 32]);
    assert_eq!(b, [7u8; 32]);
    assert_eq!(c, [8u8; 32]);
    assert_eq!(loads, 2, "same height must not load twice");
}

#[test]
fn drop_unsubscribed_status_clears_idle_hashes() {
    let mut last = HashMap::new();
    let gone = [1u8; 32];
    let keep = [2u8; 32];
    last.insert(gone, "x".into());
    last.insert(keep, "y".into());
    let mut subs = HashSet::new();
    subs.insert(keep);
    drop_unsubscribed_status(&mut last, &subs);
    assert_eq!(last.get(&keep), Some(&"y".to_string()));
    assert!(!last.contains_key(&gone));
}

#[test]
fn restatus_notes_scans_intermediate_tick_heights() {
    use rbitcoin_primitives::{Fk, Height};
    use rbitcoin_query::TxApply;
    use rbitcoin_store::{HeaderRecord, InputRecord, OutputRecord, TxRecord};

    let (dir, q) = tmp_store();
    let merkle = [0x51; 32];
    let h0 = HeaderRecord {
        prev_fk: Fk::NULL,
        version: 1,
        timestamp: 1,
        bits: 0x207fffff,
        nonce: 0,
        merkle_root: merkle,
        hash: merkle,
        size: 0,
        weight: 0,
    };
    let mut txid0 = [0u8; 32];
    txid0[0] = 0xa0;
    let ta0 = TxApply {
        tx: TxRecord {
            txid: txid0,
            version: 1,
            locktime: 0,
            input_start_fk: Fk::NULL,
            input_count: 1,
            output_start_fk: Fk::NULL,
            output_count: 1,
        },
        inputs: vec![InputRecord {
            prev_txid: [0u8; 32],
            create_fk: Fk::NULL,
            prev_index: u32::MAX,
            sequence: u32::MAX,
            script_sig: vec![0],
            witness: vec![],
        }],
        outputs: vec![OutputRecord::unspent(50_0000_0000, vec![0x51])],
    };
    let hfk0 = q.connect_block(Height(0), &h0, &[ta0]).unwrap();
    let hash1 = rbitcoin_store::block_header_hash(1, &merkle, &[0x11; 32], 2, 0x207fffff, 1);
    let h1 = HeaderRecord {
        prev_fk: hfk0,
        version: 1,
        timestamp: 2,
        bits: 0x207fffff,
        nonce: 1,
        merkle_root: [0x11; 32],
        hash: hash1,
        size: 0,
        weight: 0,
    };
    let mut txid1 = [0u8; 32];
    txid1[0] = 0xa1;
    q.connect_block(
        Height(1),
        &h1,
        &[TxApply {
            tx: TxRecord {
                txid: txid1,
                version: 1,
                locktime: 0,
                input_start_fk: Fk::NULL,
                input_count: 1,
                output_start_fk: Fk::NULL,
                output_count: 1,
            },
            inputs: vec![InputRecord {
                prev_txid: [0u8; 32],
                create_fk: Fk::NULL,
                prev_index: u32::MAX,
                sequence: u32::MAX,
                script_sig: vec![1],
                witness: vec![],
            }],
            outputs: vec![OutputRecord::unspent(50_0000_0000, vec![0x00])],
        }],
    )
    .unwrap();
    q.apply_sh_pending().unwrap();
    let sh = rbitcoin_store::script_hash(&[0x51]);
    let only_tip = restatus_notes(&q, None, &[sh], Some(&[1]));
    assert!(only_tip.is_empty(), "height 1 does not touch the OP_TRUE script");
    let range = restatus_notes(&q, None, &[sh], Some(&[1, 0]));
    assert_eq!(range.len(), 1, "range must include the height-0 create");
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn negotiate_protocol_intersection_and_asof_dialect() {
    assert_eq!(negotiate_protocol(&json!([])).unwrap(), PROTOCOL_MAX);
    assert_eq!(negotiate_protocol(&json!(["c"])).unwrap(), PROTOCOL_MAX);
    assert_eq!(negotiate_protocol(&json!(["c", "1.4"])).unwrap(), "1.4");
    assert_eq!(negotiate_protocol(&json!(["c", "1.4.2"])).unwrap(), "1.4.2");
    assert_eq!(negotiate_protocol(&json!(["c", ["1.4", "1.4.2"]])).unwrap(), "1.4.2");
    assert_eq!(negotiate_protocol(&json!(["c", PROTOCOL_ASOF])).unwrap(), PROTOCOL_ASOF);
    assert_eq!(negotiate_protocol(&json!(["c", ["1.4", PROTOCOL_ASOF]])).unwrap(), PROTOCOL_ASOF);
    assert_eq!(negotiate_protocol(&json!(["c", "1.6"])).unwrap(), "1.6");
    assert!(negotiate_protocol(&json!(["c", "1.7"])).unwrap_err().contains("unsupported"));
    assert!(negotiate_protocol(&json!(["c", ["1.6.1", "1.7"]]))
        .unwrap_err()
        .contains("unsupported"));
}

async fn electrum_tcp_rpc(stream: &mut TcpStream, id: u32, method: &str, params: Value) -> Value {
    let req = json!({"jsonrpc":"2.0","id":id,"method":method,"params":params});
    let mut line = serde_json::to_string(&req).unwrap();
    line.push('\n');
    stream.write_all(line.as_bytes()).await.unwrap();
    let mut reader = BufReader::new(&mut *stream);
    let mut resp = String::new();
    tokio::time::timeout(std::time::Duration::from_secs(3), reader.read_line(&mut resp))
        .await
        .expect("timeout")
        .unwrap();
    serde_json::from_str(&resp).unwrap()
}

#[allow(clippy::cognitive_complexity)] // one TCP client, static methods + no-hub fees
#[tokio::test]
async fn accept_client_ping_and_shutdown() {
    let (dir, q) = tmp_store();
    let params = ChainParams::regtest();
    let q = std::sync::Arc::new(q);
    let (tip_tx, _) = broadcast::channel(4);
    let cfg = ElectrumConfig::for_params("127.0.0.1:0".parse().unwrap(), &params);
    let banner = cfg.banner.clone();
    let handle = run_electrum(cfg, q, params, tip_tx, None).await.expect("listen");

    let mut stream = TcpStream::connect(handle.local_addr).await.unwrap();
    let v = electrum_tcp_rpc(&mut stream, 1, "server.ping", json!([])).await;
    assert_eq!(v["id"], 1);
    assert!(v.get("result").is_some());
    assert!(v.get("chain_tip").is_none(), "ping must not grow chain_tip: {v}");

    stream.write_all(b"\n").await.unwrap();
    stream.write_all(b"{not json\n").await.unwrap();
    let v = electrum_tcp_rpc(&mut stream, 2, "server.version", json!([])).await;
    assert_eq!(v["id"], 2);
    let ver = v["result"].as_array().expect("version array");
    assert_eq!(ver.len(), 2);
    let cake_server = ver[0].as_str().expect("server.version[0] string");
    assert!(
        cake_server.to_ascii_lowercase().contains("electrs"),
        "Cake skips tweaks unless version[0] contains electrs, got {cake_server:?}"
    );
    assert!(
        cake_server.to_ascii_lowercase().contains("rbitcoin"),
        "version[0] must still identify rbitcoin, got {cake_server:?}"
    );
    assert!(
        cake_server.contains(env!("CARGO_PKG_VERSION")),
        "version[0] must track workspace.package.version, got {cake_server:?}"
    );
    assert_eq!(ver[1], PROTOCOL_MAX);

    let features = electrum_tcp_rpc(&mut stream, 3, "server.features", json!([])).await;
    let features = &features["result"];
    assert_eq!(features["protocol_min"], PROTOCOL_MIN);
    assert_eq!(features["protocol_max"], PROTOCOL_MAX);
    assert_eq!(features["server_version"], ver[0]);
    assert_eq!(features["silent_payments"], json!([0]));
    assert_eq!(features["tweaks"], json!(true));
    assert_eq!(features["chain_tip"], json!(true));
    assert_eq!(features["asof"], json!(true));
    assert_eq!(features["asof_protocol"], PROTOCOL_ASOF);

    let probe =
        electrum_tcp_rpc(&mut stream, 4, "blockchain.tweaks.subscribe", json!([0, 1, false])).await;
    assert_eq!(probe["result"], json!({"0": {}}));

    let banner_v = electrum_tcp_rpc(&mut stream, 5, "server.banner", json!([])).await;
    assert_eq!(banner_v["result"].as_str().unwrap(), banner);

    let don = electrum_tcp_rpc(&mut stream, 6, "server.donation_address", json!([])).await;
    assert!(don["result"].as_str().is_some());

    let peers = electrum_tcp_rpc(&mut stream, 7, "server.peers.subscribe", json!([])).await;
    assert_eq!(peers["result"], json!([]));

    let fee = electrum_tcp_rpc(&mut stream, 8, "blockchain.relayfee", json!([])).await;
    assert!(fee["result"].as_f64().is_some());

    let est = electrum_tcp_rpc(&mut stream, 9, "blockchain.estimatefee", json!([6])).await;
    assert_eq!(est["result"].as_f64(), Some(-1.0));

    let hist = electrum_tcp_rpc(&mut stream, 10, "mempool.get_fee_histogram", json!([])).await;
    assert_eq!(hist["result"], json!([]));

    let unk = electrum_tcp_rpc(&mut stream, 11, "no.such.method", json!([])).await;
    assert!(
        unk["error"]["message"].as_str().unwrap_or("").contains("unknown method")
            || unk["error"].as_str().unwrap_or("").contains("unknown method"),
        "{unk}"
    );

    let bad =
        electrum_tcp_rpc(&mut stream, 12, "blockchain.transaction.broadcast", json!(["zz"])).await;
    assert!(bad.get("error").is_some(), "{bad}");
    let bad_hex = electrum_tcp_rpc(
        &mut stream,
        13,
        "blockchain.transaction.broadcast",
        json!(["01000000000000000000"]),
    )
    .await;
    assert!(bad_hex.get("error").is_some(), "{bad_hex}");

    handle.shutdown().await;
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn chain_view_get_history_stamps_tip_and_changes_on_replace() {
    use rbitcoin_primitives::{Fk, Height};
    use rbitcoin_query::TxApply;
    use rbitcoin_store::{HeaderRecord, InputRecord, OutputRecord, TxRecord};

    let (dir, q) = tmp_store();
    let (h0, t0) = {
        let merkle = [0xab; 32];
        let header = HeaderRecord {
            prev_fk: Fk::NULL,
            version: 1,
            timestamp: 1,
            bits: 0x207fffff,
            nonce: 0,
            merkle_root: merkle,
            hash: merkle,
            size: 0,
            weight: 0,
        };
        let mut txid = [0xcb; 32];
        txid[31] = 0;
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
                create_fk: Fk::NULL,
                prev_index: u32::MAX,
                sequence: u32::MAX,
                script_sig: vec![0],
                witness: vec![],
            }],
            outputs: vec![OutputRecord::unspent(50_0000_0000, vec![0x51])],
        };
        (header, ta)
    };
    let hash0 = h0.hash;
    q.connect_block(Height(0), &h0, &[t0]).unwrap();
    let prev_fk = q.tip_header_fk().unwrap().unwrap();
    let mut h1 = h0.clone();
    h1.prev_fk = prev_fk;
    h1.timestamp = 2;
    h1.nonce = 1;
    h1.hash = rbitcoin_store::block_header_hash(
        h1.version,
        &hash0,
        &h1.merkle_root,
        h1.timestamp,
        h1.bits,
        h1.nonce,
    );
    let mut t1 = {
        let mut txid = [0xcb; 32];
        txid[5] = 0xaa;
        TxApply {
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
                create_fk: Fk::NULL,
                prev_index: u32::MAX,
                sequence: u32::MAX,
                script_sig: vec![1],
                witness: vec![],
            }],
            outputs: vec![OutputRecord::unspent(50_0000_0000, vec![0x51])],
        }
    };
    t1.tx.txid[0] = 1;
    q.connect_block(Height(1), &h1, &[t1]).unwrap();
    let tip_a = hash_hex_rev(&h1.hash);

    let params = ChainParams::regtest();
    let q = std::sync::Arc::new(q);
    let (tip_tx, _) = broadcast::channel(4);
    let cfg = ElectrumConfig::for_params("127.0.0.1:0".parse().unwrap(), &params);
    let handle = run_electrum(cfg, Arc::clone(&q), params, tip_tx, None).await.expect("listen");
    let sh = electrum_scripthash_hex(&[0x51]);

    let mut stream = TcpStream::connect(handle.local_addr).await.unwrap();
    let req = json!({
        "jsonrpc":"2.0","id":1,
        "method":"blockchain.scripthash.get_history","params":[sh]
    });
    let mut line = serde_json::to_string(&req).unwrap();
    line.push('\n');
    stream.write_all(line.as_bytes()).await.unwrap();
    let mut reader = BufReader::new(&mut stream);
    let mut resp = String::new();
    tokio::time::timeout(std::time::Duration::from_secs(3), reader.read_line(&mut resp))
        .await
        .expect("timeout")
        .unwrap();
    let v: Value = serde_json::from_str(&resp).unwrap();
    assert_eq!(v["id"], 1, "{v}");
    assert!(v["result"].as_array().is_some(), "{v}");
    assert_eq!(v["chain_tip"], tip_a, "{v}");
    assert_eq!(v["chain_tip_height"], 1, "{v}");

    q.disconnect_tip().unwrap();
    let mut h1b = h1.clone();
    h1b.nonce = 9;
    h1b.hash = rbitcoin_store::block_header_hash(
        h1b.version,
        &hash0,
        &h1b.merkle_root,
        h1b.timestamp,
        h1b.bits,
        h1b.nonce,
    );
    let mut t1b = {
        let mut txid = [0xcb; 32];
        txid[5] = 0xbb;
        TxApply {
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
                create_fk: Fk::NULL,
                prev_index: u32::MAX,
                sequence: u32::MAX,
                script_sig: vec![2],
                witness: vec![],
            }],
            outputs: vec![OutputRecord::unspent(50_0000_0000, vec![0x51])],
        }
    };
    t1b.tx.txid[0] = 2;
    q.connect_block(Height(1), &h1b, &[t1b]).unwrap();
    let tip_b = hash_hex_rev(&h1b.hash);
    assert_ne!(tip_a, tip_b);

    let stream = reader.into_inner();
    let mut line = serde_json::to_string(&req).unwrap();
    line.push('\n');
    stream.write_all(line.as_bytes()).await.unwrap();
    let mut reader = BufReader::new(stream);
    resp.clear();
    tokio::time::timeout(std::time::Duration::from_secs(3), reader.read_line(&mut resp))
        .await
        .expect("timeout")
        .unwrap();
    let v: Value = serde_json::from_str(&resp).unwrap();
    assert_eq!(v["chain_tip"], tip_b, "{v}");
    assert_eq!(v["chain_tip_height"], 1, "{v}");

    handle.shutdown().await;
    let _ = std::fs::remove_dir_all(&dir);
}

/// On a 1-worker runtime, ping must complete while another socket is inside
/// a real blocking store query (`blockchain.block.headers`).
#[tokio::test(flavor = "current_thread")]
async fn ping_overlaps_blocking_headers_on_one_worker() {
    use rbitcoin_consensus::{accept_and_connect_block, Milestone};
    use rbitcoin_primitives::Height;
    use std::sync::atomic::AtomicU64;

    let (dir, q) = tmp_store();
    let params = ChainParams::regtest();
    let genesis = bitcoin::blockdata::constants::genesis_block(bitcoin::Network::Regtest);
    accept_and_connect_block(&q, &params, Height::GENESIS, &genesis, Milestone::NONE).unwrap();
    // Enough headers that a serial walk stays in-flight after ping is scheduled.
    let _ = rbitcoin_consensus::pad_empty_from(
        &q,
        &params,
        genesis.block_hash(),
        genesis.header.time,
        1,
        80,
        0,
    );
    let q = Arc::new(q);
    let (tip_tx, _) = broadcast::channel(4);
    let cfg = ElectrumConfig::for_params("127.0.0.1:0".parse().unwrap(), &params);
    let handle = run_electrum(cfg, q, params, tip_tx, None).await.expect("listen");
    let addr = handle.local_addr;

    let mut a = TcpStream::connect(addr).await.unwrap();
    let mut b = TcpStream::connect(addr).await.unwrap();
    let a_started = Arc::new(AtomicBool::new(false));
    let a_done_us = Arc::new(AtomicU64::new(0));
    let b_done_us = Arc::new(AtomicU64::new(0));
    let t0 = Instant::now();

    let a_started_c = Arc::clone(&a_started);
    let a_done_c = Arc::clone(&a_done_us);
    let ha = tokio::spawn(async move {
        let req = json!({
            "jsonrpc":"2.0","id":10,
            "method":"blockchain.block.headers","params":[0, 2016]
        });
        let mut line = serde_json::to_string(&req).unwrap();
        line.push('\n');
        a.write_all(line.as_bytes()).await.unwrap();
        a_started_c.store(true, Ordering::SeqCst);
        let mut reader = BufReader::new(a);
        let mut resp = String::new();
        reader.read_line(&mut resp).await.unwrap();
        a_done_c.store(t0.elapsed().as_micros() as u64, Ordering::SeqCst);
        let v: Value = serde_json::from_str(&resp).unwrap();
        assert_eq!(v["id"], 10);
        assert!(v["result"]["count"].as_u64().unwrap() > 50);
    });

    let a_started_c = Arc::clone(&a_started);
    let b_done_c = Arc::clone(&b_done_us);
    let hb = tokio::spawn(async move {
        while !a_started_c.load(Ordering::SeqCst) {
            tokio::task::yield_now().await;
        }
        let req = json!({"jsonrpc":"2.0","id":11,"method":"server.ping","params":[]});
        let mut line = serde_json::to_string(&req).unwrap();
        line.push('\n');
        b.write_all(line.as_bytes()).await.unwrap();
        let mut reader = BufReader::new(b);
        let mut resp = String::new();
        reader.read_line(&mut resp).await.unwrap();
        b_done_c.store(t0.elapsed().as_micros() as u64, Ordering::SeqCst);
        let v: Value = serde_json::from_str(&resp).unwrap();
        assert_eq!(v["id"], 11);
        assert!(v.get("result").is_some());
    });

    ha.await.expect("headers task");
    hb.await.expect("ping task");
    let a_done = a_done_us.load(Ordering::SeqCst);
    let b_done = b_done_us.load(Ordering::SeqCst);
    assert!(
        b_done < a_done,
        "ping finished at {b_done}µs, headers at {a_done}µs (expected overlap)"
    );

    handle.shutdown().await;
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn api_log_records_electrum_method() {
    let (dir, q) = tmp_store();
    let log_path = dir.join("api.jsonl");
    rbitcoin_log::init_api_log(&log_path).unwrap();
    let params = ChainParams::regtest();
    let q = std::sync::Arc::new(q);
    let (tip_tx, _) = broadcast::channel(4);
    let cfg = ElectrumConfig::for_params("127.0.0.1:0".parse().unwrap(), &params);
    let handle = run_electrum(cfg, q, params, tip_tx, None).await.expect("listen");

    let mut stream = TcpStream::connect(handle.local_addr).await.unwrap();
    let req = json!({"jsonrpc":"2.0","id":1,"method":"server.ping","params":[]});
    let mut line = serde_json::to_string(&req).unwrap();
    line.push('\n');
    stream.write_all(line.as_bytes()).await.unwrap();
    let mut reader = BufReader::new(&mut stream);
    let mut resp = String::new();
    tokio::time::timeout(std::time::Duration::from_secs(3), reader.read_line(&mut resp))
        .await
        .expect("timeout")
        .unwrap();
    handle.shutdown().await;
    rbitcoin_log::close_api_log();
    let body = std::fs::read_to_string(&log_path).unwrap();
    assert!(body.contains("\"method\":\"server.ping\""), "api log missing ping: {body}");
    assert!(body.contains("\"surface\":\"electrum\""));
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn electrum_sh_stamp_follows_pending_before_durable_apply() {
    use rbitcoin_primitives::{Fk, Height};
    use rbitcoin_query::TxApply;
    use rbitcoin_store::{HeaderRecord, InputRecord, OutputRecord, TxRecord};

    let (dir, q) = tmp_store();
    let params = ChainParams::regtest();
    let cfg = ElectrumConfig::for_params("127.0.0.1:0".parse().unwrap(), &params);
    let mut merkle = [0u8; 32];
    merkle[0] = 0x51;
    let h0 = HeaderRecord {
        prev_fk: Fk::NULL,
        version: 1,
        timestamp: 1,
        bits: 0x207fffff,
        nonce: 0,
        merkle_root: merkle,
        hash: merkle,
        size: 0,
        weight: 0,
    };
    let ta0 = TxApply {
        tx: TxRecord {
            txid: [0xcb; 32],
            version: 1,
            locktime: 0,
            input_start_fk: Fk::NULL,
            input_count: 1,
            output_start_fk: Fk::NULL,
            output_count: 1,
        },
        inputs: vec![InputRecord {
            prev_txid: [0u8; 32],
            create_fk: Fk::NULL,
            prev_index: u32::MAX,
            sequence: u32::MAX,
            script_sig: vec![0],
            witness: vec![],
        }],
        outputs: vec![OutputRecord::unspent(50_0000_0000, vec![0x51])],
    };
    q.connect_block(Height(0), &h0, &[ta0]).unwrap();
    let prev_fk = q.tip_header_fk().unwrap().unwrap();
    let hash1 = rbitcoin_store::block_header_hash(1, &merkle, &[0x11; 32], 2, 0x207fffff, 1);
    let h1 = HeaderRecord {
        prev_fk,
        version: 1,
        timestamp: 2,
        bits: 0x207fffff,
        nonce: 1,
        merkle_root: [0x11; 32],
        hash: hash1,
        size: 0,
        weight: 0,
    };
    let mut txid1 = [0u8; 32];
    txid1[0] = 0x11;
    txid1[31] = 0xcd;
    q.commit_class_a_only(
        &h1,
        &[TxApply {
            tx: TxRecord {
                txid: txid1,
                version: 1,
                locktime: 0,
                input_start_fk: Fk::NULL,
                input_count: 1,
                output_start_fk: Fk::NULL,
                output_count: 1,
            },
            inputs: vec![InputRecord {
                prev_txid: [0u8; 32],
                create_fk: Fk::NULL,
                prev_index: u32::MAX,
                sequence: u32::MAX,
                script_sig: vec![1],
                witness: vec![],
            }],
            outputs: vec![OutputRecord::unspent(50_0000_0000, vec![0x51])],
        }],
    )
    .unwrap();
    q.confirm_block(Height(1), &hash1).unwrap();
    assert_eq!(q.tip_height(), Some(Height(1)));
    assert_eq!(q.sh_indexed_through_height(), Some(0));

    let sh = electrum_scripthash_hex(&[0x51]);
    let (out, view) = electrum_at_chain_view(
        &q,
        "blockchain.scripthash.get_balance",
        &json!([sh]),
        false,
        |q, rpc_params, view, is_asof| {
            let mut pin_conn = ElectrumConn::new();
            dispatch_pinned(
                "blockchain.scripthash.get_balance",
                rpc_params,
                q,
                &cfg,
                &params,
                None,
                &mut pin_conn,
                view,
                is_asof,
            )
        },
    );
    assert!(out.is_ok(), "{out:?}");
    let v = view.unwrap();
    assert_eq!(v.hash, hash1);
    assert_eq!(v.height, Height(1));

    q.apply_sh_pending().unwrap();
    let (out, view) = electrum_at_chain_view(
        &q,
        "blockchain.scripthash.get_balance",
        &json!([sh]),
        false,
        |q, rpc_params, view, is_asof| {
            let mut pin_conn = ElectrumConn::new();
            dispatch_pinned(
                "blockchain.scripthash.get_balance",
                rpc_params,
                q,
                &cfg,
                &params,
                None,
                &mut pin_conn,
                view,
                is_asof,
            )
        },
    );
    assert!(out.is_ok(), "{out:?}");
    let v = view.unwrap();
    assert_eq!(v.hash, hash1);
    assert_eq!(v.height, Height(1));
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn dispatch_casa_sequence_reuses_sh_join_slot() {
    use rbitcoin_primitives::{Fk, Height};
    use rbitcoin_query::{body_ok_reads, reset_body_ok_reads, TxApply};
    use rbitcoin_store::{HeaderRecord, InputRecord, OutputRecord, TxRecord};

    let (dir, q) = tmp_store();
    let params = ChainParams::regtest();
    let cfg = ElectrumConfig::for_params("127.0.0.1:0".parse().unwrap(), &params);
    let mut prev = Fk::NULL;
    let mut parent_hash: Option<[u8; 32]> = None;
    for h in 0..3u32 {
        let version = 1;
        let timestamp = h + 1;
        let bits = 0x207fffff;
        let nonce = h;
        let mut merkle = [0u8; 32];
        merkle[0..4].copy_from_slice(&h.to_le_bytes());
        merkle[5] = 0xec;
        let hash = match parent_hash {
            None => merkle,
            Some(ph) => {
                rbitcoin_store::block_header_hash(version, &ph, &merkle, timestamp, bits, nonce)
            }
        };
        let header = HeaderRecord {
            prev_fk: prev,
            version,
            timestamp,
            bits,
            nonce,
            merkle_root: merkle,
            hash,
            size: 0,
            weight: 0,
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
                create_fk: Fk::NULL,
                prev_index: u32::MAX,
                sequence: u32::MAX,
                script_sig: vec![h as u8],
                witness: vec![],
            }],
            outputs: vec![OutputRecord::unspent(50_0000_0000, vec![0x51])],
        };
        parent_hash = Some(header.hash);
        prev = q.connect_block(Height(h), &header, &[ta]).unwrap();
    }

    let mut conn = ElectrumConn::new();
    let sh = electrum_scripthash_hex(&[0x51]);
    reset_body_ok_reads();
    let bal = dispatch_with_join(
        "blockchain.scripthash.get_balance",
        &json!([sh]),
        &q,
        &cfg,
        &params,
        None,
        &mut conn,
    )
    .unwrap();
    assert_eq!(bal["confirmed"].as_i64().unwrap(), 150_0000_0000);
    let after_bal = body_ok_reads();
    assert_eq!(after_bal, 3);

    let hist = dispatch_with_join(
        "blockchain.scripthash.get_history",
        &json!([sh]),
        &q,
        &cfg,
        &params,
        None,
        &mut conn,
    )
    .unwrap();
    assert_eq!(hist.as_array().unwrap().len(), 3);
    assert_eq!(body_ok_reads(), after_bal, "get_history must reuse the connection join slot");

    let unspent = dispatch_with_join(
        "blockchain.scripthash.listunspent",
        &json!([sh]),
        &q,
        &cfg,
        &params,
        None,
        &mut conn,
    )
    .unwrap();
    assert_eq!(unspent.as_array().unwrap().len(), 3);
    assert_eq!(body_ok_reads(), after_bal, "listunspent must reuse the connection join slot");

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn max_sh_creates_is_electrum_rpc_error_and_ping_still_works() {
    use rbitcoin_primitives::{Fk, Height};
    use rbitcoin_query::TxApply;
    use rbitcoin_store::{HeaderRecord, InputRecord, OutputRecord, TxRecord};

    let (dir, q) = tmp_store();
    let params = ChainParams::regtest();
    let cfg = ElectrumConfig::for_params("127.0.0.1:0".parse().unwrap(), &params);
    let mut prev = Fk::NULL;
    let mut parent_hash: Option<[u8; 32]> = None;
    for h in 0..3u32 {
        let version = 1;
        let timestamp = h + 1;
        let bits = 0x207fffff;
        let nonce = h;
        let mut merkle = [0u8; 32];
        merkle[0..4].copy_from_slice(&h.to_le_bytes());
        merkle[5] = 0xec;
        let hash = match parent_hash {
            None => merkle,
            Some(ph) => {
                rbitcoin_store::block_header_hash(version, &ph, &merkle, timestamp, bits, nonce)
            }
        };
        let header = HeaderRecord {
            prev_fk: prev,
            version,
            timestamp,
            bits,
            nonce,
            merkle_root: merkle,
            hash,
            size: 0,
            weight: 0,
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
                create_fk: Fk::NULL,
                prev_index: u32::MAX,
                sequence: u32::MAX,
                script_sig: vec![h as u8],
                witness: vec![],
            }],
            outputs: vec![OutputRecord::unspent(50_0000_0000, vec![0x51])],
        };
        parent_hash = Some(header.hash);
        prev = q.connect_block(Height(h), &header, &[ta]).unwrap();
    }

    q.set_max_sh_creates(2);
    let mut conn = ElectrumConn::new();
    let sh = electrum_scripthash_hex(&[0x51]);
    let err = dispatch_with_join(
        "blockchain.scripthash.get_balance",
        &json!([sh]),
        &q,
        &cfg,
        &params,
        None,
        &mut conn,
    )
    .unwrap_err();
    assert!(err.contains("scripthash join exceeds --max-sh-creates"), "{err}");
    let ping =
        dispatch_with_join("server.ping", &json!([]), &q, &cfg, &params, None, &mut conn).unwrap();
    assert!(ping.is_null(), "{ping}");
    let _ = std::fs::remove_dir_all(&dir);
}

/// BCH-style optional `from_height`/`to_height` on get_history; status stays full.
#[test]
fn get_history_height_window_and_status_full() {
    use rbitcoin_primitives::{Fk, Height};
    use rbitcoin_query::TxApply;
    use rbitcoin_store::{HeaderRecord, InputRecord, OutputRecord, TxRecord};

    let (dir, q) = tmp_store();
    let mut prev = Fk::NULL;
    let mut parent_hash: Option<[u8; 32]> = None;
    for h in 0..4u32 {
        let version = 1;
        let timestamp = h + 1;
        let bits = 0x207fffff;
        let nonce = h;
        let mut merkle = [0u8; 32];
        merkle[0..4].copy_from_slice(&h.to_le_bytes());
        merkle[5] = 0xee;
        let hash = match parent_hash {
            None => merkle,
            Some(ph) => {
                rbitcoin_store::block_header_hash(version, &ph, &merkle, timestamp, bits, nonce)
            }
        };
        let header = HeaderRecord {
            prev_fk: prev,
            version,
            timestamp,
            bits,
            nonce,
            merkle_root: merkle,
            hash,
            size: 0,
            weight: 0,
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
                create_fk: Fk::NULL,
                prev_index: u32::MAX,
                sequence: u32::MAX,
                script_sig: vec![h as u8],
                witness: vec![],
            }],
            outputs: vec![OutputRecord::unspent(50_0000_0000, vec![0x51])],
        };
        parent_hash = Some(header.hash);
        prev = q.connect_block(Height(h), &header, &[ta]).unwrap();
    }

    let params = ChainParams::regtest();
    let cfg = ElectrumConfig::for_params("127.0.0.1:0".parse().unwrap(), &params);
    let mut header_sub = false;
    let mut sh_subs = HashSet::new();
    let sh = electrum_scripthash_hex(&[0x51]);

    let full = dispatch(
        "blockchain.scripthash.get_history",
        &json!([sh]),
        &q,
        &cfg,
        &params,
        None,
        &mut header_sub,
        &mut sh_subs,
    )
    .unwrap();
    let full_arr = full.as_array().unwrap();
    assert_eq!(full_arr.len(), 4);

    // Inclusive from, exclusive to → heights 1 and 2 only.
    let windowed = dispatch(
        "blockchain.scripthash.get_history",
        &json!([sh, 1, 3]),
        &q,
        &cfg,
        &params,
        None,
        &mut header_sub,
        &mut sh_subs,
    )
    .unwrap();
    let w = windowed.as_array().unwrap();
    assert_eq!(w.len(), 2);
    assert_eq!(w[0]["height"], 1);
    assert_eq!(w[1]["height"], 2);
    assert!(w.len() < full_arr.len());

    // to_height=-1 is open upper (same as full for confirmed-only).
    let open_to = dispatch(
        "blockchain.scripthash.get_history",
        &json!([sh, 0, -1]),
        &q,
        &cfg,
        &params,
        None,
        &mut header_sub,
        &mut sh_subs,
    )
    .unwrap();
    assert_eq!(open_to.as_array().unwrap().len(), full_arr.len());

    // Subscribe status is always full history, independent of windowed calls.
    let status = dispatch(
        "blockchain.scripthash.subscribe",
        &json!([sh]),
        &q,
        &cfg,
        &params,
        None,
        &mut header_sub,
        &mut sh_subs,
    )
    .unwrap();
    let status_s = status.as_str().unwrap();
    assert!(!status_s.is_empty());
    // Recompute status from full confirmed history and match.
    let sh_bytes = {
        let mut b = rbitcoin_primitives::hex_decode(&sh).unwrap();
        b.reverse();
        let mut out = [0u8; 32];
        out.copy_from_slice(&b);
        out
    };
    let full_hist = q.scripthash_history(&sh_bytes).unwrap();
    assert_eq!(full_hist.len(), 4);
    assert_eq!(scripthash_status(Some(&q), &full_hist).unwrap(), status_s);

    let _ = prev;
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn tip_push_and_lagged_client() {
    let (dir, q) = tmp_store();
    // Need a tip for headers.subscribe; empty chain errors on subscribe.
    use rbitcoin_primitives::{Fk, Height};
    use rbitcoin_query::TxApply;
    use rbitcoin_store::{HeaderRecord, InputRecord, OutputRecord, TxRecord};
    let mut hash = [0u8; 32];
    hash[0] = 1;
    let header = HeaderRecord {
        prev_fk: Fk::NULL,
        version: 1,
        timestamp: 1,
        bits: 0x207fffff,
        nonce: 0,
        merkle_root: hash,
        hash,
        size: 0,
        weight: 0,
    };
    let mut txid = [0u8; 32];
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
            create_fk: Fk::NULL,
            prev_index: u32::MAX,
            sequence: u32::MAX,
            script_sig: vec![0],
            witness: vec![],
        }],
        outputs: vec![OutputRecord::unspent(1, vec![0x51])],
    };
    q.connect_block(Height(0), &header, &[ta]).unwrap();

    let params = ChainParams::regtest();
    let q = std::sync::Arc::new(q);
    let (tip_tx, _) = broadcast::channel(2);
    let cfg = ElectrumConfig::for_params("127.0.0.1:0".parse().unwrap(), &params);
    let handle = run_electrum(cfg, q, params, tip_tx.clone(), None).await.unwrap();
    let mut stream = TcpStream::connect(handle.local_addr).await.unwrap();
    let mut line = serde_json::to_string(&json!({
        "jsonrpc":"2.0","id":1,"method":"blockchain.headers.subscribe","params":[]
    }))
    .unwrap();
    line.push('\n');
    stream.write_all(line.as_bytes()).await.unwrap();
    let mut reader = BufReader::new(&mut stream);
    let mut resp = String::new();
    tokio::time::timeout(std::time::Duration::from_secs(3), reader.read_line(&mut resp))
        .await
        .unwrap()
        .unwrap();
    // Push tip notify.
    tip_tx
        .send(TipNotify { height: 1, header_hex: "aa".repeat(80), reorg_from_height: None })
        .unwrap();
    resp.clear();
    tokio::time::timeout(std::time::Duration::from_secs(3), reader.read_line(&mut resp))
        .await
        .unwrap()
        .unwrap();
    let push: Value = serde_json::from_str(&resp).unwrap();
    assert_eq!(push["method"].as_str(), Some("blockchain.headers.subscribe"));

    handle.shutdown().await;
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn chain_view_status_includes_blockhash() {
    use rbitcoin_primitives::{Fk, Height};
    use rbitcoin_query::TxApply;
    use rbitcoin_store::{HeaderRecord, InputRecord, OutputRecord, TxRecord};

    let (dir, q) = tmp_store();
    let merkle = [0xab; 32];
    let h0 = HeaderRecord {
        prev_fk: Fk::NULL,
        version: 1,
        timestamp: 1,
        bits: 0x207fffff,
        nonce: 0,
        merkle_root: merkle,
        hash: merkle,
        size: 0,
        weight: 0,
    };
    let mut txid = [0xcb; 32];
    txid[31] = 0;
    let t0 = TxApply {
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
            create_fk: Fk::NULL,
            prev_index: u32::MAX,
            sequence: u32::MAX,
            script_sig: vec![0],
            witness: vec![],
        }],
        outputs: vec![OutputRecord::unspent(1, vec![0x51])],
    };
    q.connect_block(Height(0), &h0, std::slice::from_ref(&t0)).unwrap();
    let prev_fk = q.tip_header_fk().unwrap().unwrap();
    let mut h1 = h0.clone();
    h1.prev_fk = prev_fk;
    h1.timestamp = 2;
    h1.nonce = 1;
    h1.hash = rbitcoin_store::block_header_hash(
        h1.version,
        &merkle,
        &h1.merkle_root,
        h1.timestamp,
        h1.bits,
        h1.nonce,
    );
    let mut t1 = t0;
    t1.tx.txid[5] = 0xaa;
    q.connect_block(Height(1), &h1, std::slice::from_ref(&t1)).unwrap();
    let sh = script_hash(&[0x51]);
    let hist_a = q.scripthash_history(&sh).unwrap();
    let status_a = scripthash_status(Some(&q), &hist_a).unwrap();
    let legacy = {
        use bitcoin::hashes::{sha256, Hash as _};
        let mut s = String::new();
        for i in &hist_a {
            s.push_str(&format!("{}:{}:", txid_hex(&i.txid), i.height));
        }
        rbitcoin_primitives::hex_encode(sha256::Hash::hash(s.as_bytes()).to_byte_array())
    };
    assert_ne!(status_a, legacy, "status preimage must include confirming block hash");

    q.disconnect_tip().unwrap();
    let mut h1b = h1.clone();
    h1b.nonce = 9;
    h1b.hash = rbitcoin_store::block_header_hash(
        h1b.version,
        &merkle,
        &h1b.merkle_root,
        h1b.timestamp,
        h1b.bits,
        h1b.nonce,
    );
    q.connect_block(Height(1), &h1b, &[t1]).unwrap();
    let hist_b = q.scripthash_history(&sh).unwrap();
    let status_b = scripthash_status(Some(&q), &hist_b).unwrap();
    assert_eq!(
        hist_a.iter().map(|i| (i.txid, i.height)).collect::<Vec<_>>(),
        hist_b.iter().map(|i| (i.txid, i.height)).collect::<Vec<_>>(),
        "same txs at the same heights"
    );
    assert_ne!(status_a, status_b, "same-height replace must change status via blockhash");
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn chain_view_reorg_notifies_dropped_scripthash() {
    use rbitcoin_query::TxApply;
    use rbitcoin_store::{HeaderRecord, InputRecord, OutputRecord, TxRecord};

    let (dir, q) = tmp_store();
    let mut hash = [0u8; 32];
    hash[0] = 0x42;
    let header = HeaderRecord {
        prev_fk: Fk::NULL,
        version: 1,
        timestamp: 1,
        bits: 0x207fffff,
        nonce: 0,
        merkle_root: hash,
        hash,
        size: 0,
        weight: 0,
    };
    let mut txid = [0u8; 32];
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
            create_fk: Fk::NULL,
            prev_index: u32::MAX,
            sequence: u32::MAX,
            script_sig: vec![0],
            witness: vec![],
        }],
        outputs: vec![OutputRecord::unspent(1, vec![0x51])],
    };
    let hfk0 = q.connect_block(Height(0), &header, &[ta]).unwrap();
    let sh = electrum_scripthash_hex(&[0x51]);

    let params = ChainParams::regtest();
    let q = std::sync::Arc::new(q);
    let (tip_tx, _) = broadcast::channel(2);
    let cfg = ElectrumConfig::for_params("127.0.0.1:0".parse().unwrap(), &params);
    let handle =
        run_electrum(cfg, std::sync::Arc::clone(&q), params, tip_tx.clone(), None).await.unwrap();
    let mut stream = TcpStream::connect(handle.local_addr).await.unwrap();
    let mut line = serde_json::to_string(&json!({
        "jsonrpc":"2.0","id":1,"method":"blockchain.scripthash.subscribe","params":[sh]
    }))
    .unwrap();
    line.push('\n');
    stream.write_all(line.as_bytes()).await.unwrap();
    let mut reader = BufReader::new(&mut stream);
    let mut resp = String::new();
    tokio::time::timeout(std::time::Duration::from_secs(3), reader.read_line(&mut resp))
        .await
        .unwrap()
        .unwrap();
    let first: Value = serde_json::from_str(&resp).unwrap();
    let status0 = first["result"].as_str().unwrap().to_string();
    assert!(!status0.is_empty());
    let _ = hfk0;

    q.disconnect_tip().unwrap();
    let mut hash_b = [0u8; 32];
    hash_b[0] = 0x43;
    let header_b = HeaderRecord {
        prev_fk: Fk::NULL,
        version: 1,
        timestamp: 1,
        bits: 0x207fffff,
        nonce: 1,
        merkle_root: hash_b,
        hash: hash_b,
        size: 0,
        weight: 0,
    };
    let mut txid_b = [0u8; 32];
    txid_b[0] = 0x99;
    txid_b[31] = 0xcd;
    let ta_b = TxApply {
        tx: TxRecord {
            txid: txid_b,
            version: 1,
            locktime: 0,
            input_start_fk: Fk::NULL,
            input_count: 1,
            output_start_fk: Fk::NULL,
            output_count: 1,
        },
        inputs: vec![InputRecord {
            prev_txid: [0u8; 32],
            create_fk: Fk::NULL,
            prev_index: u32::MAX,
            sequence: u32::MAX,
            script_sig: vec![1],
            witness: vec![],
        }],
        outputs: vec![OutputRecord::unspent(1, vec![0x00])],
    };
    q.connect_block(Height(0), &header_b, &[ta_b]).unwrap();
    tip_tx
        .send(TipNotify { height: 0, header_hex: "aa".repeat(80), reorg_from_height: Some(0) })
        .unwrap();
    resp.clear();
    tokio::time::timeout(std::time::Duration::from_secs(3), reader.read_line(&mut resp))
        .await
        .expect("reorg must restatus even when the new block misses the scripthash")
        .unwrap();
    let push: Value = serde_json::from_str(&resp).unwrap();
    assert_eq!(push["method"].as_str(), Some("blockchain.scripthash.subscribe"));
    assert_ne!(
        push["params"][1].as_str().unwrap_or("missing"),
        status0,
        "dropped history must change status"
    );

    handle.shutdown().await;
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn scripthash_status_matches_get_history_row_order() {
    use bitcoin::absolute::LockTime;
    use bitcoin::script::ScriptBuf;
    use bitcoin::transaction::Version as TxVersion;
    use bitcoin::{Amount, OutPoint, Sequence, Transaction, TxIn, TxOut, Witness};
    use rbitcoin_consensus::{accept_and_connect_block, Milestone};
    use rbitcoin_net::MempoolHub;
    use rbitcoin_primitives::{Fk, Height};
    use rbitcoin_query::ScriptHashHistoryItem;
    use rbitcoin_store::script_hash;
    use std::sync::Arc;

    let (dir, q) = tmp_store();
    let params = ChainParams::regtest();
    let genesis = bitcoin::blockdata::constants::genesis_block(bitcoin::Network::Regtest);
    accept_and_connect_block(&q, &params, Height::GENESIS, &genesis, Milestone::NONE).unwrap();
    let (_tip, _tip_time, coinbase_txids) = rbitcoin_consensus::pad_empty_from(
        &q,
        &params,
        genesis.block_hash(),
        genesis.header.time,
        1,
        103,
        2,
    );

    let q_arc = Arc::new(q);
    let mp = MempoolHub::open(dir.join("mempool"), Arc::clone(&q_arc)).unwrap();
    mp.set_relay_enabled(true);

    let spk = ScriptBuf::from_bytes(vec![0x51]);
    let parent = Transaction {
        version: TxVersion::TWO,
        lock_time: LockTime::ZERO,
        input: vec![TxIn {
            previous_output: OutPoint { txid: coinbase_txids[0], vout: 0 },
            script_sig: ScriptBuf::new(),
            sequence: Sequence::ENABLE_RBF_NO_LOCKTIME,
            witness: Witness::new(),
        }],
        output: vec![TxOut {
            value: Amount::from_sat(50_0000_0000 - 1_000),
            script_pubkey: spk.clone(),
        }],
    };
    mp.accept_tx(&parent).expect("accept parent");

    let cfg = ElectrumConfig::for_params("127.0.0.1:0".parse().unwrap(), &params);
    let mut header_sub = false;
    let mut sh_subs = HashSet::new();
    let sh = electrum_scripthash_hex(spk.as_bytes());
    let hist = dispatch(
        "blockchain.scripthash.get_history",
        &json!([sh]),
        &q_arc,
        &cfg,
        &params,
        Some(&mp),
        &mut header_sub,
        &mut sh_subs,
    )
    .unwrap();
    let arr = hist.as_array().unwrap();
    assert!(arr.iter().any(|r| r["height"].as_i64().unwrap() >= 1), "need a confirmed history row");
    assert!(
        arr.last().unwrap()["height"].as_i64().unwrap() <= 0,
        "get_history must append mempool last"
    );

    let rows: Vec<ScriptHashHistoryItem> = arr
        .iter()
        .map(|r| ScriptHashHistoryItem {
            height: r["height"].as_i64().unwrap(),
            txid: param_txid(&json!([r["tx_hash"].as_str().unwrap()]), 0).unwrap(),
            tx_fk: Fk::NULL,
            fee: r["fee"].as_i64(),
        })
        .collect();
    let expected = scripthash_status(Some(q_arc.as_ref()), &rows).unwrap();
    let mut height_sorted = rows.clone();
    height_sorted.sort_by_key(|i| i.height);
    let sorted_hash = scripthash_status(Some(q_arc.as_ref()), &height_sorted).unwrap();
    assert_ne!(sorted_hash, expected, "height-sort must not match get_history order");

    let sh_bytes = script_hash(spk.as_bytes());
    let got = scripthash_status_full(q_arc.as_ref(), &mp, &sh_bytes).unwrap();
    assert_eq!(got, expected);

    let _ = std::fs::remove_dir_all(&dir);
}

/// Unused scripthash listunspent must not rebuild mempool spentness from
/// every live body. Confirmed UTXOs spent by the mempool still drop.
#[test]
fn listunspent_unused_sh_does_not_load_mempool_bodies() {
    use bitcoin::absolute::LockTime;
    use bitcoin::script::ScriptBuf;
    use bitcoin::transaction::Version as TxVersion;
    use bitcoin::{Amount, OutPoint, Sequence, Transaction, TxIn, TxOut, Witness};
    use rbitcoin_consensus::{accept_and_connect_block, Milestone};
    use rbitcoin_net::MempoolHub;
    use rbitcoin_primitives::Height;
    use std::sync::Arc;

    let (dir, q) = tmp_store();
    let params = ChainParams::regtest();
    let genesis = bitcoin::blockdata::constants::genesis_block(bitcoin::Network::Regtest);
    accept_and_connect_block(&q, &params, Height::GENESIS, &genesis, Milestone::NONE).unwrap();
    const N_SPENDS: u32 = 4;
    let (_tip, _tip_time, coinbase_txids) = rbitcoin_consensus::pad_empty_from(
        &q,
        &params,
        genesis.block_hash(),
        genesis.header.time,
        1,
        100 + N_SPENDS,
        N_SPENDS,
    );
    let q_arc = Arc::new(q);
    let mp = MempoolHub::open(dir.join("mempool"), Arc::clone(&q_arc)).unwrap();
    mp.set_relay_enabled(true);
    let spk = ScriptBuf::from_bytes(vec![0x51]);
    for (i, cbtxid) in coinbase_txids.iter().enumerate() {
        let fee = 1_000u64 + i as u64;
        let tx = Transaction {
            version: TxVersion::TWO,
            lock_time: LockTime::ZERO,
            input: vec![TxIn {
                previous_output: OutPoint { txid: *cbtxid, vout: 0 },
                script_sig: ScriptBuf::new(),
                sequence: Sequence::ENABLE_RBF_NO_LOCKTIME,
                witness: Witness::new(),
            }],
            output: vec![TxOut {
                value: Amount::from_sat(50_0000_0000 - fee),
                script_pubkey: spk.clone(),
            }],
        };
        mp.accept_tx(&tx).expect("accept spend");
    }
    let cfg = ElectrumConfig::for_params("127.0.0.1:0".parse().unwrap(), &params);
    let mut header_sub = false;
    let mut sh_subs = HashSet::new();
    let unused = electrum_scripthash_hex(&[0x00]);
    let _ = mp.sample_reset_perf();
    let empty = dispatch(
        "blockchain.scripthash.listunspent",
        &json!([unused]),
        &q_arc,
        &cfg,
        &params,
        Some(&mp),
        &mut header_sub,
        &mut sh_subs,
    )
    .unwrap();
    assert!(empty.as_array().unwrap().is_empty());
    let s = mp.sample_reset_perf();
    assert_eq!(
        s.spent_body_loads, 0,
        "unused scripthash must not load mempool bodies (got {})",
        s.spent_body_loads
    );

    // Spent coinbase of the used script must drop; mempool parent out remains.
    let sh = electrum_scripthash_hex(spk.as_bytes());
    let unspent = dispatch(
        "blockchain.scripthash.listunspent",
        &json!([sh]),
        &q_arc,
        &cfg,
        &params,
        Some(&mp),
        &mut header_sub,
        &mut sh_subs,
    )
    .unwrap();
    let rows = unspent.as_array().unwrap();
    let spent_cb = format!("{}", coinbase_txids[0]);
    assert!(
        rows.iter().all(|r| r["tx_hash"] != spent_cb),
        "mempool-spent coinbase must drop: {rows:?}"
    );
    assert!(rows.iter().any(|r| r["height"] == 0), "mempool output must remain: {rows:?}");
    let _ = std::fs::remove_dir_all(&dir);
}

/// DoS: line without newline beyond max must fail without allocating forever.
#[tokio::test]
async fn read_line_capped_rejects_oversize() {
    use std::io::Cursor;
    use tokio::io::BufReader;
    let mut r = BufReader::new(Cursor::new(vec![b'A'; 64]));
    let err = read_line_capped(&mut r, 32).await.unwrap_err();
    assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
}

#[tokio::test]
async fn read_line_capped_accepts_under_limit() {
    use std::io::Cursor;
    use tokio::io::BufReader;
    let mut r = BufReader::new(Cursor::new(b"{\"id\":1}\nnext\n".as_slice()));
    let line = read_line_capped(&mut r, 1024).await.unwrap().unwrap();
    assert_eq!(line, "{\"id\":1}");
    let line2 = read_line_capped(&mut r, 1024).await.unwrap().unwrap();
    assert_eq!(line2, "next");
}

#[test]
fn broadcast_hex_cap_enforced() {
    let (dir, q) = tmp_store();
    let params = ChainParams::regtest();
    let mut cfg = ElectrumConfig::for_params("127.0.0.1:0".parse().unwrap(), &params);
    cfg.max_broadcast_hex = 16;
    let mut header_sub = false;
    let mut sh_subs = HashSet::new();
    let err = dispatch(
        "blockchain.transaction.broadcast",
        &json!(["aa".repeat(20)]),
        &q,
        &cfg,
        &params,
        None,
        &mut header_sub,
        &mut sh_subs,
    )
    .unwrap_err();
    assert!(err.contains("too large"), "{err}");
    let pkg_err = dispatch(
        "blockchain.transaction.broadcast_package",
        &json!([["aa".repeat(20)]]),
        &q,
        &cfg,
        &params,
        None,
        &mut header_sub,
        &mut sh_subs,
    )
    .unwrap_err();
    assert!(pkg_err.contains("too large"), "{pkg_err}");
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn serve_limits_public_proxy_defaults() {
    let lim = ServeLimits::for_public_proxy();
    assert_eq!(lim.max_connections, DEFAULT_MAX_CONNECTIONS);
    assert_eq!(lim.max_request_bytes, DEFAULT_MAX_REQUEST_BYTES);
    assert_eq!(lim.idle_timeout, Duration::from_secs(DEFAULT_IDLE_TIMEOUT_SECS));
    let params = ChainParams::regtest();
    let cfg = ElectrumConfig::for_params("0.0.0.0:50001".parse().unwrap(), &params);
    assert_eq!(cfg.limits, lim);
    assert_eq!(cfg.max_connections(), DEFAULT_MAX_CONNECTIONS);
    assert_eq!(cfg.max_line_bytes(), DEFAULT_MAX_LINE_BYTES);
}

#[test]
fn tweaks_rpc_result_is_first_height_only() {
    use rbitcoin_primitives::{Fk, Height};
    use rbitcoin_query::TxApply;
    use rbitcoin_store::{HeaderRecord, InputRecord, OutputRecord, TxRecord};

    let (dir, q) = tmp_store();
    let params = ChainParams::regtest();
    let cfg = ElectrumConfig::for_params("127.0.0.1:0".parse().unwrap(), &params);
    let mut prev = Fk::NULL;
    let mut parent_hash: Option<[u8; 32]> = None;
    for h in 0..12u32 {
        let mut merkle = [0u8; 32];
        merkle[0..4].copy_from_slice(&h.to_le_bytes());
        merkle[5] = 0xec;
        let hash = match parent_hash {
            None => merkle,
            Some(ph) => rbitcoin_store::block_header_hash(1, &ph, &merkle, h + 1, 0x207fffff, h),
        };
        let header = HeaderRecord {
            prev_fk: prev,
            version: 1,
            timestamp: h + 1,
            bits: 0x207fffff,
            nonce: h,
            merkle_root: merkle,
            hash,
            size: 0,
            weight: 0,
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
            inputs: vec![InputRecord::coinbase(u32::MAX, vec![h as u8], vec![])],
            outputs: vec![OutputRecord::unspent(50_0000_0000, vec![0x51])],
        };
        prev = q.connect_block(Height(h), &header, &[ta]).unwrap();
        parent_hash = Some(hash);
    }
    let mut header_sub = false;
    let mut sh_subs = HashSet::new();
    let v = dispatch(
        "blockchain.tweaks.subscribe",
        &json!([0, 100, true]),
        &q,
        &cfg,
        &params,
        None,
        &mut header_sub,
        &mut sh_subs,
    )
    .unwrap();
    let obj = v.as_object().unwrap();
    assert_eq!(obj.len(), 1, "RPC result is the first height only");
    assert!(obj.contains_key("0"));
    assert!(!obj.contains_key("1"));
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn tweaks_subscribe_zero_chunk_dones_after_wave0_then_resubscribe() {
    use rbitcoin_primitives::{Fk, Height};
    use rbitcoin_query::TxApply;
    use rbitcoin_store::{HeaderRecord, InputRecord, OutputRecord, TxRecord};

    let (dir, q) = tmp_store();
    let mut prev = Fk::NULL;
    let mut parent_hash: Option<[u8; 32]> = None;
    for h in 0..5u32 {
        let mut merkle = [0u8; 32];
        merkle[0..4].copy_from_slice(&h.to_le_bytes());
        merkle[5] = 0xec;
        let hash = match parent_hash {
            None => merkle,
            Some(ph) => rbitcoin_store::block_header_hash(1, &ph, &merkle, h + 1, 0x207fffff, h),
        };
        let header = HeaderRecord {
            prev_fk: prev,
            version: 1,
            timestamp: h + 1,
            bits: 0x207fffff,
            nonce: h,
            merkle_root: merkle,
            hash,
            size: 0,
            weight: 0,
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
            inputs: vec![InputRecord::coinbase(u32::MAX, vec![h as u8], vec![])],
            outputs: vec![OutputRecord::unspent(50_0000_0000, vec![0x51])],
        };
        prev = q.connect_block(Height(h), &header, &[ta]).unwrap();
        parent_hash = Some(hash);
    }

    let params = ChainParams::regtest();
    let q = std::sync::Arc::new(q);
    let (tip_tx, _) = broadcast::channel(4);
    let mut cfg = ElectrumConfig::for_params("127.0.0.1:0".parse().unwrap(), &params);
    cfg.tweaks_chunk = Duration::ZERO;
    let handle = run_electrum(cfg, q, params, tip_tx, None).await.expect("listen");

    let mut stream = TcpStream::connect(handle.local_addr).await.unwrap();
    async fn read_json(reader: &mut BufReader<&mut TcpStream>) -> Value {
        let mut resp = String::new();
        tokio::time::timeout(Duration::from_secs(3), reader.read_line(&mut resp))
            .await
            .unwrap_or_else(|_| panic!("tweaks stream: timed out"))
            .unwrap();
        serde_json::from_str(&resp).unwrap_or_else(|e| panic!("tweaks stream parse {e}: {resp}"))
    }
    async fn subscribe(stream: &mut TcpStream, id: &str, start: u32, count: u32) {
        let req = json!({
            "jsonrpc":"2.0","id": id,
            "method":"blockchain.tweaks.subscribe",
            "params":[start, count, false]
        });
        let mut line = serde_json::to_string(&req).unwrap();
        line.push('\n');
        stream.write_all(line.as_bytes()).await.unwrap();
    }

    subscribe(&mut stream, "scan", 1, 3).await;
    let mut reader = BufReader::new(&mut stream);

    let result = read_json(&mut reader).await;
    assert_eq!(result["id"], "scan");
    let map = result["result"].as_object().expect("result map");
    assert_eq!(map.len(), 1, "wave 0 result is one height, got {map:?}");
    assert!(map.contains_key("1"), "{map:?}");

    let done = read_json(&mut reader).await;
    assert_eq!(done["method"], "blockchain.tweaks.subscribe");
    assert_eq!(
        done["params"][0]["message"], "done",
        "zero chunk must done after wave 0 with heights left, got {done}"
    );

    drop(reader);
    subscribe(&mut stream, "scan2", 2, 2).await;
    let mut reader = BufReader::new(&mut stream);
    let result2 = read_json(&mut reader).await;
    assert_eq!(result2["id"], "scan2");
    let map2 = result2["result"].as_object().expect("resubscribe result");
    assert!(
        map2.contains_key("2"),
        "Cake noData path resubscribes on the same socket, got {map2:?}"
    );

    let done2 = read_json(&mut reader).await;
    assert_eq!(done2["params"][0]["message"], "done");

    handle.shutdown().await;
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn tweaks_subscribe_pre_taproot_collapses_empty_heights() {
    use rbitcoin_primitives::{Fk, Height};
    use rbitcoin_query::TxApply;
    use rbitcoin_store::{HeaderRecord, InputRecord, OutputRecord, TxRecord};

    let (dir, q) = tmp_store();
    let mut prev = Fk::NULL;
    let mut parent_hash: Option<[u8; 32]> = None;
    for h in 0..5u32 {
        let mut merkle = [0u8; 32];
        merkle[0..4].copy_from_slice(&h.to_le_bytes());
        merkle[5] = 0xec;
        let hash = match parent_hash {
            None => merkle,
            Some(ph) => rbitcoin_store::block_header_hash(1, &ph, &merkle, h + 1, 0x207fffff, h),
        };
        let header = HeaderRecord {
            prev_fk: prev,
            version: 1,
            timestamp: h + 1,
            bits: 0x207fffff,
            nonce: h,
            merkle_root: merkle,
            hash,
            size: 0,
            weight: 0,
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
            inputs: vec![InputRecord::coinbase(u32::MAX, vec![h as u8], vec![])],
            outputs: vec![OutputRecord::unspent(50_0000_0000, vec![0x51])],
        };
        prev = q.connect_block(Height(h), &header, &[ta]).unwrap();
        parent_hash = Some(hash);
    }

    let params = ChainParams::mainnet();
    let q = std::sync::Arc::new(q);
    let (tip_tx, _) = broadcast::channel(4);
    let cfg = ElectrumConfig::for_params("127.0.0.1:0".parse().unwrap(), &params);
    let handle = run_electrum(cfg, q, params, tip_tx, None).await.expect("listen");

    let mut stream = TcpStream::connect(handle.local_addr).await.unwrap();
    let req = json!({
        "jsonrpc":"2.0","id":"scan",
        "method":"blockchain.tweaks.subscribe",
        "params":[0, 5, false]
    });
    let mut line = serde_json::to_string(&req).unwrap();
    line.push('\n');
    stream.write_all(line.as_bytes()).await.unwrap();
    let mut reader = BufReader::new(&mut stream);

    async fn read_json(reader: &mut BufReader<&mut TcpStream>) -> Value {
        let mut resp = String::new();
        tokio::time::timeout(Duration::from_secs(3), reader.read_line(&mut resp))
            .await
            .unwrap_or_else(|_| panic!("tweaks stream: timed out"))
            .unwrap();
        serde_json::from_str(&resp).unwrap_or_else(|e| panic!("tweaks stream parse {e}: {resp}"))
    }

    let result = read_json(&mut reader).await;
    assert_eq!(result["id"], "scan");
    let map = result["result"].as_object().expect("result map");
    assert_eq!(map.len(), 1, "probe/result stays one height, got {map:?}");
    assert!(map.contains_key("0"), "{map:?}");

    let n = read_json(&mut reader).await;
    assert_eq!(n["method"], "blockchain.tweaks.subscribe");
    let p = n["params"][0].as_object().expect("collapsed notify");
    assert_eq!(p.len(), 4, "heights 1..=4 in one notify, got {p:?}");
    for h in 1u32..=4 {
        assert!(p.contains_key(&h.to_string()), "{p:?}");
        assert!(p[&h.to_string()].as_object().unwrap().is_empty());
    }

    let done = read_json(&mut reader).await;
    assert_eq!(done["method"], "blockchain.tweaks.subscribe");
    assert_eq!(done["params"][0]["message"], "done");

    handle.shutdown().await;
    let _ = std::fs::remove_dir_all(&dir);
}

#[allow(clippy::cognitive_complexity)] // one store, every method's missing/wrong-type params
#[test]
fn dispatch_param_type_edges_and_subscribe_cap() {
    let (dir, q) = tmp_store();
    let params = ChainParams::regtest();
    let mut cfg = ElectrumConfig::for_params("127.0.0.1:0".parse().unwrap(), &params);
    cfg.max_scripthash_subs = 1;
    let mut header_sub = false;
    let mut sh_subs = HashSet::new();
    let sh = electrum_scripthash_hex(&[0x51]);
    let sh2 = electrum_scripthash_hex(&[0x52]);

    for (method, args, needle) in [
        ("blockchain.block.header", json!([]), "expected number"),
        ("blockchain.block.header", json!([true]), "expected number"),
        ("blockchain.block.headers", json!([]), "expected number"),
        ("blockchain.scripthash.get_history", json!([]), "expected string"),
        ("blockchain.scripthash.get_history", json!([1]), "expected string"),
        ("blockchain.scripthash.get_balance", json!(["aa"]), "scripthash must be 32 bytes hex"),
        ("blockchain.scripthash.listunspent", json!([true]), "expected string"),
        ("blockchain.scripthash.subscribe", json!([]), "expected string"),
        ("blockchain.scripthash.unsubscribe", json!(["zz".repeat(32)]), "invalid hex"),
        ("blockchain.scripthash.get_mempool", json!({}), "expected string"),
        ("blockchain.transaction.get", json!(["aabb"]), "txid must be 32 bytes hex"),
        ("blockchain.transaction.get_merkle", json!([sh]), "expected number"),
        ("blockchain.transaction.broadcast", json!([]), "expected string"),
        ("blockchain.transaction.broadcast", json!([1]), "expected string"),
        ("blockchain.transaction.broadcast_package", json!([]), "expected array of hex txs"),
        ("blockchain.transaction.broadcast_package", json!([1]), "expected array of hex txs"),
        ("blockchain.transaction.broadcast_package", json!([[1]]), "tx must be hex"),
        ("blockchain.transaction.broadcast_package", json!([["zz"]]), "invalid hex digit"),
        ("blockchain.transaction.broadcast_package", json!([["00"]]), "IO error"),
        ("blockchain.outpoint.get_status", json!([]), "expected string"),
        ("blockchain.outpoint.subscribe", json!([sh]), "param 1 expected number"),
        ("blockchain.outpoint.unsubscribe", json!([true, 0]), "expected string"),
        ("blockchain.silentpayments.subscribe", json!([]), "expected string"),
        ("blockchain.silentpayments.unsubscribe", json!([1, 2]), "expected string"),
        ("blockchain.transaction.id_from_pos", json!([]), "expected number"),
        ("no.such.method", json!([]), "unknown method"),
    ] {
        let err = dispatch(method, &args, &q, &cfg, &params, None, &mut header_sub, &mut sh_subs)
            .unwrap_err();
        assert!(err.contains(needle), "{method} {args}: expected {needle:?} in {err}");
    }

    let asof = format!("asof:{}", "ab".repeat(32));
    let err = dispatch(
        "blockchain.scripthash.get_balance",
        &json!([sh, asof]),
        &q,
        &cfg,
        &params,
        None,
        &mut header_sub,
        &mut sh_subs,
    )
    .unwrap_err();
    assert!(err.contains("1.4.2-asof"), "asof without dialect: {err}");

    dispatch(
        "blockchain.scripthash.subscribe",
        &json!([sh]),
        &q,
        &cfg,
        &params,
        None,
        &mut header_sub,
        &mut sh_subs,
    )
    .unwrap();
    let err = dispatch(
        "blockchain.scripthash.subscribe",
        &json!([sh2]),
        &q,
        &cfg,
        &params,
        None,
        &mut header_sub,
        &mut sh_subs,
    )
    .unwrap_err();
    assert!(err.contains("too many scripthash"), "{err}");
    let again = dispatch(
        "blockchain.scripthash.subscribe",
        &json!([sh]),
        &q,
        &cfg,
        &params,
        None,
        &mut header_sub,
        &mut sh_subs,
    )
    .unwrap();
    assert!(again.as_str().is_some());
    let dropped = dispatch(
        "blockchain.scripthash.unsubscribe",
        &json!([sh]),
        &q,
        &cfg,
        &params,
        None,
        &mut header_sub,
        &mut sh_subs,
    )
    .unwrap();
    assert_eq!(dropped, json!(true));
    let missing = dispatch(
        "blockchain.scripthash.unsubscribe",
        &json!([sh]),
        &q,
        &cfg,
        &params,
        None,
        &mut header_sub,
        &mut sh_subs,
    )
    .unwrap();
    assert_eq!(missing, json!(false));
    dispatch(
        "blockchain.scripthash.subscribe",
        &json!([sh2]),
        &q,
        &cfg,
        &params,
        None,
        &mut header_sub,
        &mut sh_subs,
    )
    .unwrap();

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn silentpayments_unsubscribe_clears_session_scan() {
    let (dir, q) = tmp_store();
    let params = ChainParams::regtest();
    let cfg = ElectrumConfig::for_params("127.0.0.1:0".parse().unwrap(), &params);
    let scan = "0f694e068028a717f8af6b9411f9a133dd3565258714cc226594b34db90c1f2c";
    let spend = "025cc9856d6f8375350e123978daac200c260cb5b5ae83106cab90484dcd8fcf36";
    let other_scan = "1f694e068028a717f8af6b9411f9a133dd3565258714cc226594b34db90c1f2c";
    let args = json!([scan, spend, 0]);
    let other = json!([other_scan, spend, 0]);
    let mut conn = ElectrumConn::new();
    let none = dispatch_with_join(
        "blockchain.silentpayments.unsubscribe",
        &args,
        &q,
        &cfg,
        &params,
        None,
        &mut conn,
    )
    .unwrap();
    assert!(none.as_str().unwrap().contains("sp"), "{none}");
    assert!(conn.sp_sub.is_none());
    conn.sp_sub =
        Some(crate::silent_scan::parse_sub(&args, bitcoin::Network::Regtest, Some(0)).unwrap());
    dispatch_with_join(
        "blockchain.silentpayments.unsubscribe",
        &other,
        &q,
        &cfg,
        &params,
        None,
        &mut conn,
    )
    .unwrap();
    assert!(conn.sp_sub.is_some(), "mismatch address must leave the session scan");
    dispatch_with_join(
        "blockchain.silentpayments.unsubscribe",
        &args,
        &q,
        &cfg,
        &params,
        None,
        &mut conn,
    )
    .unwrap();
    assert!(conn.sp_sub.is_none(), "unsubscribe must drop the session scan");
    let _ = std::fs::remove_dir_all(&dir);
}
