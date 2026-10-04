//! mempool/electrs `/internal/*` bulk REST and `/mempool/txids/page`.

use crate::handlers::{outspend_json, spawn_join};
#[cfg(unix)]
use crate::server::not_found;
use crate::server::{block_hash_hex, parse_hash32, pin_or_reject, store_err, AppState};
#[cfg(unix)]
use crate::tx_json::{
    build_tx_json, build_tx_json_from_tx, build_tx_json_from_tx_with_status, tx_status_json,
};
#[cfg(unix)]
use axum::body::Bytes;
use axum::extract::{Path, Query as AxumQuery, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use bitcoin::hashes::Hash;
use bitcoin::Txid;
#[cfg(unix)]
use rbitcoin_net::{MempoolHub, MempoolTxSnapEntry};
use rbitcoin_query::ChainViewKind;
#[cfg(unix)]
use rbitcoin_store::StoreError;
use serde::Deserialize;
use serde_json::{json, Value};

const DEFAULT_MAX_TXS: usize = 10_000;

#[derive(Deserialize)]
pub struct MaxTxs {
    max_txs: Option<usize>,
}

fn cap_max_txs(q: &MaxTxs) -> usize {
    q.max_txs.unwrap_or(DEFAULT_MAX_TXS).min(DEFAULT_MAX_TXS)
}

fn bad_request(msg: &'static str) -> Response {
    (StatusCode::BAD_REQUEST, msg).into_response()
}

#[cfg(unix)]
#[allow(clippy::result_large_err)] // axum Response
fn parse_txid_array(body: &[u8]) -> Result<Vec<[u8; 32]>, Response> {
    let v: Value = serde_json::from_slice(body).map_err(|_| bad_request("invalid json"))?;
    let arr = v
        .as_array()
        .ok_or_else(|| bad_request("body must be a JSON array of txid hex"))?;
    let mut out = Vec::with_capacity(arr.len());
    for x in arr {
        let s = x
            .as_str()
            .ok_or_else(|| bad_request("txid must be a hex string"))?;
        let id = parse_hash32(s).map_err(|_| bad_request("unparseable txid"))?;
        out.push(id);
    }
    Ok(out)
}

#[cfg(unix)]
fn cache_json_str(
    slot: &std::sync::OnceLock<Box<str>>,
    build: impl FnOnce() -> Result<String, ()>,
) -> Option<&str> {
    if let Some(s) = slot.get() {
        return Some(s.as_ref());
    }
    match build() {
        Ok(s) => {
            let _ = slot.set(s.into());
            slot.get().map(|b| b.as_ref())
        }
        Err(()) => None,
    }
}

#[cfg(unix)]
fn join_cached_objects(parts: &[Option<&str>]) -> String {
    let mut body = String::from("[");
    let mut first = true;
    for p in parts {
        let Some(s) = p else {
            continue;
        };
        if !first {
            body.push(',');
        }
        first = false;
        body.push_str(s);
    }
    body.push(']');
    body
}

#[cfg(unix)]
fn entry_json_str<'a>(
    st: &AppState,
    mp: &MempoolHub,
    e: &'a MempoolTxSnapEntry,
) -> Option<&'a str> {
    cache_json_str(&e.json, || {
        let v = build_tx_json_from_tx(
            &st.query,
            &e.tx,
            st.network,
            Some(e.fee_sat as i64),
            Some(mp),
        )
        .map_err(|_| ())?;
        serde_json::to_string(&v).map_err(|_| ())
    })
}

#[cfg(unix)]
fn entry_json(st: &AppState, mp: &MempoolHub, e: &MempoolTxSnapEntry) -> Option<Value> {
    let raw = entry_json_str(st, mp, e)?;
    serde_json::from_str(raw).ok()
}

#[cfg(unix)]
fn confirmed_or_mempool_tx(st: &AppState, id: &[u8; 32]) -> Option<Value> {
    if let Ok(Some((fk, _))) = st.query.get_tx_by_txid(id) {
        return build_tx_json(&st.query, fk, st.network).ok();
    }
    mempool_tx_json(st, id)
}

#[cfg(unix)]
fn mempool_tx_json(st: &AppState, id: &[u8; 32]) -> Option<Value> {
    let mp = st.mempool.as_ref()?;
    let tid = Txid::from_byte_array(*id);
    if let Some(e) = mp.mempool_tx_snapshot().get(&tid) {
        return entry_json(st, mp, e);
    }
    let tx = mp.get_tx(&tid)?;
    let fee = mp.get_live_meta(&tid).map(|(f, _)| f as i64);
    build_tx_json_from_tx(&st.query, &tx, st.network, fee, Some(mp)).ok()
}

#[cfg(unix)]
pub async fn post_internal_txs(State(st): State<AppState>, body: Bytes) -> Response {
    if body.len() > st.max_body {
        return (StatusCode::PAYLOAD_TOO_LARGE, "body too large").into_response();
    }
    spawn_join(move || {
        let ids = match parse_txid_array(&body) {
            Ok(v) => v,
            Err(r) => return r,
        };
        let mut out = Vec::new();
        for id in ids {
            if let Some(v) = confirmed_or_mempool_tx(&st, &id) {
                out.push(v);
            }
        }
        Json(out).into_response()
    })
    .await
}

#[cfg(unix)]
pub async fn post_internal_mempool_txs(State(st): State<AppState>, body: Bytes) -> Response {
    if body.len() > st.max_body {
        return (StatusCode::PAYLOAD_TOO_LARGE, "body too large").into_response();
    }
    spawn_join(move || {
        let ids = match parse_txid_array(&body) {
            Ok(v) => v,
            Err(r) => return r,
        };
        let mut out = Vec::new();
        for id in ids {
            if let Some(v) = mempool_tx_json(&st, &id) {
                out.push(v);
            }
        }
        Json(out).into_response()
    })
    .await
}

#[cfg(unix)]
fn mempool_tx_page(st: &AppState, last: Option<&Txid>, max: usize) -> Response {
    let Some(mp) = st.mempool.as_ref() else {
        return Json(Value::Array(Vec::new())).into_response();
    };
    let snap = mp.mempool_tx_snapshot();
    let parts: Vec<Option<&str>> = snap
        .page(last, max)
        .iter()
        .map(|e| entry_json_str(st, mp, e))
        .collect();
    let body = join_cached_objects(&parts);
    (
        StatusCode::OK,
        [(axum::http::header::CONTENT_TYPE, "application/json")],
        body,
    )
        .into_response()
}

#[cfg(unix)]
pub async fn get_internal_mempool_txs(
    State(st): State<AppState>,
    AxumQuery(q): AxumQuery<MaxTxs>,
) -> Response {
    let max = cap_max_txs(&q);
    spawn_join(move || mempool_tx_page(&st, None, max)).await
}

#[cfg(unix)]
pub async fn get_internal_mempool_txs_all(State(st): State<AppState>) -> Response {
    spawn_join(move || mempool_tx_page(&st, None, usize::MAX)).await
}

#[cfg(unix)]
pub async fn get_internal_mempool_txs_cursor(
    State(st): State<AppState>,
    Path(last): Path<String>,
    AxumQuery(q): AxumQuery<MaxTxs>,
) -> Response {
    let Ok(id) = parse_hash32(&last) else {
        return bad_request("unparseable txid");
    };
    let last = Txid::from_byte_array(id);
    let max = cap_max_txs(&q);
    spawn_join(move || mempool_tx_page(&st, Some(&last), max)).await
}

fn mempool_txid_page(st: &AppState, last: Option<&Txid>, max: usize) -> Vec<String> {
    let Some(mp) = st.mempool.as_ref() else {
        return Vec::new();
    };
    mp.mempool_tx_snapshot()
        .page(last, max)
        .iter()
        .map(|e| block_hash_hex(&e.txid.to_byte_array()))
        .collect()
}

pub async fn get_mempool_txids_page(
    State(st): State<AppState>,
    AxumQuery(q): AxumQuery<MaxTxs>,
) -> Response {
    let max = cap_max_txs(&q);
    spawn_join(move || Json(mempool_txid_page(&st, None, max)).into_response()).await
}

pub async fn get_mempool_txids_page_cursor(
    State(st): State<AppState>,
    Path(last): Path<String>,
    AxumQuery(q): AxumQuery<MaxTxs>,
) -> Response {
    let Ok(id) = parse_hash32(&last) else {
        return bad_request("unparseable txid");
    };
    let last = Txid::from_byte_array(id);
    let max = cap_max_txs(&q);
    spawn_join(move || Json(mempool_txid_page(&st, Some(&last), max)).into_response()).await
}

#[cfg(unix)]
pub async fn get_internal_block_txs(
    State(st): State<AppState>,
    Path(hash_hex): Path<String>,
) -> Response {
    spawn_join(move || {
        let Ok(hash) = parse_hash32(&hash_hex) else {
            return not_found();
        };
        let Some((header_fk, rec)) = (match st.query.get_header_by_hash(&hash) {
            Ok(v) => v,
            Err(e) => return store_err(e),
        }) else {
            return not_found();
        };
        let fks = match st.query.header_tx_fks(header_fk, Some(&hash)) {
            Ok(Some(fks)) => fks,
            Ok(None) => return not_found(),
            Err(e) => return store_err(e),
        };
        let block = match st
            .query
            .reconstruct_archived_block_from_parts(rec, fks.clone())
        {
            Ok(b) => b,
            Err(e) => return store_err(e),
        };
        if block.txdata.len() != fks.len() {
            return store_err(StoreError::Corrupt(
                "invariant: reconstruct tx count != header_txs",
            ));
        }
        let mut out = Vec::with_capacity(fks.len());
        for (fk, tx) in fks.into_iter().zip(block.txdata.iter()) {
            let status = match tx_status_json(&st.query, fk) {
                Ok(s) => s,
                Err(e) => return store_err(e),
            };
            match build_tx_json_from_tx_with_status(&st.query, tx, st.network, status, None, None) {
                Ok(v) => out.push(v),
                Err(e) => return store_err(e),
            }
        }
        Json(out).into_response()
    })
    .await
}

pub(crate) fn outspends_for_txid_opts(st: &AppState, ids: Vec<Option<[u8; 32]>>) -> Response {
    let view = match pin_or_reject(&st.query, ChainViewKind::Tip, None) {
        Ok(v) => v,
        Err(r) => return r,
    };
    let mp = st.mempool.as_deref();
    let mut out = Vec::with_capacity(ids.len());
    for id in ids {
        let Some(id) = id else {
            out.push(json!([]));
            continue;
        };
        let nout = if let Ok(Some(fk)) = st.query.tx_fk_by_txid(&id) {
            match st.query.store().get_tx_meta_and_outputs(fk) {
                Ok((meta, _)) => meta.output_count,
                Err(e) => return store_err(e),
            }
        } else if let Some(mp) = mp {
            let tid = Txid::from_byte_array(id);
            mp.get_tx(&tid)
                .map(|tx| tx.output.len() as u32)
                .unwrap_or(0)
        } else {
            0
        };
        let mut slots = Vec::with_capacity(nout as usize);
        for vout in 0..nout {
            match outspend_json(&st.query, mp, &id, vout, view.as_ref()) {
                Ok(v) => slots.push(v),
                Err(e) => return store_err(e),
            }
        }
        out.push(Value::Array(slots));
    }
    Json(out).into_response()
}

#[cfg(unix)]
pub(crate) fn outspends_for_txids(st: &AppState, ids: Vec<[u8; 32]>) -> Response {
    outspends_for_txid_opts(st, ids.into_iter().map(Some).collect())
}

#[cfg(unix)]
pub async fn post_outspends_by_txid(State(st): State<AppState>, body: Bytes) -> Response {
    if body.len() > st.max_body {
        return (StatusCode::PAYLOAD_TOO_LARGE, "body too large").into_response();
    }
    spawn_join(move || {
        let ids = match parse_txid_array(&body) {
            Ok(v) => v,
            Err(r) => return r,
        };
        outspends_for_txids(&st, ids)
    })
    .await
}

#[cfg(unix)]
pub async fn post_outspends_by_outpoint(State(st): State<AppState>, body: Bytes) -> Response {
    if body.len() > st.max_body {
        return (StatusCode::PAYLOAD_TOO_LARGE, "body too large").into_response();
    }
    spawn_join(move || {
        let v: Value = match serde_json::from_slice(&body) {
            Ok(v) => v,
            Err(_) => return bad_request("invalid json"),
        };
        let Some(arr) = v.as_array() else {
            return bad_request("body must be a JSON array of txid:vout");
        };
        let view = match pin_or_reject(&st.query, ChainViewKind::Tip, None) {
            Ok(v) => v,
            Err(r) => return r,
        };
        let mp = st.mempool.as_deref();
        let mut out = Vec::with_capacity(arr.len());
        for x in arr {
            let Some(s) = x.as_str() else {
                out.push(json!({"spent": false}));
                continue;
            };
            let Some((tid_s, vout_s)) = s.rsplit_once(':') else {
                out.push(json!({"spent": false}));
                continue;
            };
            let Ok(txid) = parse_hash32(tid_s) else {
                out.push(json!({"spent": false}));
                continue;
            };
            let Ok(vout) = vout_s.parse::<u32>() else {
                out.push(json!({"spent": false}));
                continue;
            };
            match outspend_json(&st.query, mp, &txid, vout, view.as_ref()) {
                Ok(v) => out.push(v),
                Err(e) => return store_err(e),
            }
        }
        Json(out).into_response()
    })
    .await
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use crate::server::{run_esplora, EsploraConfig, EsploraListen};
    use bitcoin::absolute::LockTime;
    use bitcoin::{Amount, OutPoint, ScriptBuf, Sequence, Transaction, TxIn, TxOut, Witness};
    use rbitcoin_consensus::{accept_and_connect_block, ChainParams, Milestone};
    use rbitcoin_net::MempoolHub;
    use rbitcoin_primitives::Height;
    use rbitcoin_query::Query;
    use std::sync::Arc;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::UnixStream;

    #[test]
    fn join_cached_objects_skips_none() {
        assert_eq!(
            join_cached_objects(&[Some("{\"a\":1}"), None, Some("{\"b\":2}")]),
            "[{\"a\":1},{\"b\":2}]"
        );
        assert_eq!(join_cached_objects(&[None, None]), "[]");
    }

    #[test]
    fn cache_json_str_does_not_store_empty_on_err() {
        let slot = std::sync::OnceLock::<Box<str>>::new();
        assert!(cache_json_str(&slot, || Err::<String, ()>(())).is_none());
        assert!(slot.get().is_none());
        assert_eq!(
            cache_json_str(&slot, || Ok::<_, ()>("{\"ok\":true}".into())),
            Some("{\"ok\":true}")
        );
        assert_eq!(slot.get().map(|s| s.as_ref()), Some("{\"ok\":true}"));
    }

    fn spend_true(cb: Txid, fee: u64, spk: ScriptBuf) -> Transaction {
        Transaction {
            version: bitcoin::transaction::Version::TWO,
            lock_time: LockTime::ZERO,
            input: vec![TxIn {
                previous_output: OutPoint { txid: cb, vout: 0 },
                script_sig: ScriptBuf::new(),
                sequence: Sequence::ENABLE_RBF_NO_LOCKTIME,
                witness: Witness::new(),
            }],
            output: vec![TxOut {
                value: Amount::from_sat(50_0000_0000 - fee),
                script_pubkey: spk,
            }],
        }
    }

    struct Pad {
        dir: rbitcoin_query::testutil::TempDir,
        q: Arc<Query>,
        hub: Arc<MempoolHub>,
        cbs: Vec<Txid>,
        genesis: bitcoin::BlockHash,
    }

    fn pad_hub(label: &str, n_cb: u32) -> Pad {
        let (dir, q) = rbitcoin_query::testutil::tiny_query_labeled(label);
        let params = ChainParams::regtest();
        let genesis = bitcoin::blockdata::constants::genesis_block(bitcoin::Network::Regtest);
        accept_and_connect_block(&q, &params, Height::GENESIS, &genesis, Milestone::NONE).unwrap();
        let (_tip, _time, cbs) = rbitcoin_consensus::pad_empty_from(
            &q,
            &params,
            genesis.block_hash(),
            genesis.header.time,
            1,
            100 + n_cb,
            n_cb,
        );
        let q = Arc::new(q);
        let mp = dir.join("mp");
        std::fs::create_dir_all(&mp).unwrap();
        let hub = MempoolHub::open(&mp, Arc::clone(&q)).unwrap();
        hub.set_relay_enabled(true);
        Pad {
            dir,
            q,
            hub,
            cbs,
            genesis: genesis.block_hash(),
        }
    }

    fn parse_http(buf: &[u8]) -> (u16, String) {
        let text = String::from_utf8_lossy(buf).into_owned();
        let status = text
            .lines()
            .next()
            .and_then(|l| l.split_whitespace().nth(1))
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);
        let body = text
            .split("\r\n\r\n")
            .nth(1)
            .unwrap_or("")
            .trim()
            .to_string();
        (status, body)
    }

    async fn http_get_unix(sock: &std::path::Path, path: &str) -> (u16, String) {
        let mut stream = UnixStream::connect(sock).await.expect("unix connect");
        let req = format!("GET {path} HTTP/1.1\r\nHost: api\r\nConnection: close\r\n\r\n");
        stream.write_all(req.as_bytes()).await.unwrap();
        let mut buf = Vec::new();
        stream.read_to_end(&mut buf).await.unwrap();
        parse_http(&buf)
    }

    async fn http_post_unix(sock: &std::path::Path, path: &str, body: &[u8]) -> (u16, String) {
        let mut stream = UnixStream::connect(sock).await.expect("unix connect");
        let req = format!(
            "POST {path} HTTP/1.1\r\nHost: api\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            body.len()
        );
        stream.write_all(req.as_bytes()).await.unwrap();
        stream.write_all(body).await.unwrap();
        let mut buf = Vec::new();
        stream.read_to_end(&mut buf).await.unwrap();
        parse_http(&buf)
    }

    async fn run_unix(
        dir: &std::path::Path,
        q: Arc<Query>,
        mp: Option<Arc<MempoolHub>>,
    ) -> (crate::server::EsploraHandle, std::path::PathBuf) {
        let sock = dir.join(format!("esplora-{}.sock", std::process::id()));
        let cfg = EsploraConfig::with_listen(
            EsploraListen::Unix(sock.clone()),
            bitcoin::Network::Regtest,
        );
        let handle = run_esplora(cfg, q, mp).await.unwrap();
        (handle, sock)
    }

    /// `/txs/test` dry-runs; `GET /broadcast` and `POST /tx` admit.
    async fn test_then_broadcast(sock: &std::path::Path, pad: &Pad, txs: &[Transaction]) {
        use bitcoin::consensus::encode::serialize_hex;

        let hex = serialize_hex(&txs[0]);
        let (st, body) = http_get_unix(sock, "/broadcast").await;
        assert_eq!(st, 400, "{body}");
        assert!(body.contains("Missing tx"), "{body}");

        let body = serde_json::to_vec(&json!([&hex])).unwrap();
        let (st, resp) = http_post_unix(sock, "/txs/test", &body).await;
        assert_eq!(st, 200, "{resp}");
        let arr: Vec<Value> = serde_json::from_str(&resp).unwrap();
        assert_eq!(arr[0]["allowed"], true, "{resp}");
        assert!(
            !pad.hub.contains(&txs[0].compute_txid()),
            "test must not admit"
        );
        let (st, resp) = http_post_unix(sock, "/txs/test?maxfeerate=0.00000001", &body).await;
        assert_eq!(st, 200, "{resp}");
        let arr: Vec<Value> = serde_json::from_str(&resp).unwrap();
        assert_eq!(arr[0]["allowed"], false, "{resp}");
        assert_eq!(arr[0]["reject-reason"], "max-fee-exceeded");
        let too: Vec<String> = (0..26).map(|_| hex.clone()).collect();
        let body = serde_json::to_vec(&too).unwrap();
        let (st, resp) = http_post_unix(sock, "/txs/test", &body).await;
        assert_eq!(st, 400, "{resp}");
        assert!(resp.contains("Exceeded maximum of 25"), "{resp}");

        let (st, resp) = http_get_unix(sock, &format!("/broadcast?tx={hex}")).await;
        assert_eq!(st, 200, "{resp}");
        assert_eq!(resp, txs[0].compute_txid().to_string());
        for tx in &txs[1..] {
            let (st, resp) = http_post_unix(sock, "/tx", serialize_hex(tx).as_bytes()).await;
            assert_eq!(st, 200, "{resp}");
        }
        assert!(txs.iter().all(|t| pad.hub.contains(&t.compute_txid())));
    }

    /// `/mempool` totals, then the public txid pages.
    async fn mempool_totals_and_txid_pages(sock: &std::path::Path, pad: &Pad) {
        let live = pad.hub.list_live_meta();
        let expect_fee: u64 = live.iter().map(|(_, f, _)| *f).sum();
        let expect_vsize: u64 = live.iter().map(|(_, _, w)| w.saturating_add(3) / 4).sum();
        let (st, body) = http_get_unix(sock, "/mempool").await;
        assert_eq!(st, 200, "{body}");
        let mem: Value = serde_json::from_str(&body).unwrap();
        assert_eq!(mem["count"].as_u64(), Some(3), "{body}");
        assert_eq!(mem["total_fee"].as_u64(), Some(expect_fee), "{body}");
        assert_eq!(mem["vsize"].as_u64(), Some(expect_vsize), "{body}");

        let (st, p1) = http_get_unix(sock, "/mempool/txids/page?max_txs=1").await;
        assert_eq!(st, 200, "{p1}");
        let a1: Vec<String> = serde_json::from_str(&p1).unwrap();
        assert_eq!(a1.len(), 1);
        let (st, p2) =
            http_get_unix(sock, &format!("/mempool/txids/page/{}?max_txs=10", a1[0])).await;
        assert_eq!(st, 200, "{p2}");
        let a2: Vec<String> = serde_json::from_str(&p2).unwrap();
        assert_eq!(a2.len(), 2, "{p2}");
        assert!(
            !a2.contains(&a1[0]),
            "cursor page must omit {}: {p2}",
            a1[0]
        );
    }

    async fn internal_mempool_pages(sock: &std::path::Path) {
        let (st, all) = http_get_unix(sock, "/internal/mempool/txs/all").await;
        assert_eq!(st, 200, "{all}");
        let all_arr: Vec<Value> = serde_json::from_str(&all).unwrap();
        assert_eq!(all_arr.len(), 3);
        let (st, p1) = http_get_unix(sock, "/internal/mempool/txs?max_txs=2").await;
        assert_eq!(st, 200, "{p1}");
        let a1: Vec<Value> = serde_json::from_str(&p1).unwrap();
        assert_eq!(a1.len(), 2);
        assert!(a1.iter().all(|v| v.get("txid").is_some()), "{p1}");
        let last = a1[1]["txid"].as_str().unwrap();
        let (st, p2) =
            http_get_unix(sock, &format!("/internal/mempool/txs/{last}?max_txs=2")).await;
        assert_eq!(st, 200, "{p2}");
        let a2: Vec<Value> = serde_json::from_str(&p2).unwrap();
        assert_eq!(a2.len(), 1);
        let last2 = a2[0]["txid"].as_str().unwrap();
        let (st, p3) =
            http_get_unix(sock, &format!("/internal/mempool/txs/{last2}?max_txs=2")).await;
        assert_eq!(st, 200, "{p3}");
        let a3: Vec<Value> = serde_json::from_str(&p3).unwrap();
        assert!(a3.is_empty(), "{p3}");
    }

    async fn internal_txs_and_block_txs(sock: &std::path::Path, pad: &Pad, mem: &str) {
        let conf = pad.cbs[3].to_string();
        let unknown = "00".repeat(32);
        let body = serde_json::to_vec(&json!([conf, mem, unknown])).unwrap();
        let (st, resp) = http_post_unix(sock, "/internal/txs", &body).await;
        assert_eq!(st, 200, "{resp}");
        let arr: Vec<Value> = serde_json::from_str(&resp).unwrap();
        assert_eq!(arr.len(), 2, "{resp}");
        let (st, resp) = http_post_unix(sock, "/internal/txs", br#"["zz"]"#).await;
        assert_eq!(st, 400, "{resp}");
        let (st, resp) = http_post_unix(sock, "/internal/txs", b"[]").await;
        assert_eq!(st, 200, "{resp}");
        assert_eq!(resp, "[]");

        let body = serde_json::to_vec(&json!([conf, mem])).unwrap();
        let (st, resp) = http_post_unix(sock, "/internal/mempool/txs", &body).await;
        assert_eq!(st, 200, "{resp}");
        let arr: Vec<Value> = serde_json::from_str(&resp).unwrap();
        assert_eq!(arr.len(), 1, "confirmed omitted: {resp}");

        let g = pad.genesis.to_string();
        let (st, body) = http_get_unix(sock, &format!("/internal/block/{g}/txs")).await;
        assert_eq!(st, 200, "{body}");
        let arr: Vec<Value> = serde_json::from_str(&body).unwrap();
        assert_eq!(arr.len(), 1, "genesis coinbase");
        assert_eq!(arr[0]["vin"][0]["is_coinbase"], true);
        let (st, ids) = http_get_unix(sock, &format!("/block/{g}/txids")).await;
        assert_eq!(st, 200, "{ids}");
        let txids: Vec<String> = serde_json::from_str(&ids).unwrap();
        assert_eq!(arr.len(), txids.len());
        assert_eq!(arr[0]["txid"].as_str(), Some(txids[0].as_str()));
        let (st, pubp) = http_get_unix(sock, &format!("/block/{g}/txs")).await;
        assert_eq!(st, 200, "{pubp}");
        let pub_arr: Vec<Value> = serde_json::from_str(&pubp).unwrap();
        assert_eq!(pub_arr.len(), 1);
        let (st, miss) =
            http_get_unix(sock, &format!("/internal/block/{}/txs", "11".repeat(32))).await;
        assert_eq!(st, 404, "{miss}");
    }

    /// Mempool spends answer the internal bulk outspends and the public `GET /txs/outspends`.
    async fn outspends_by_txid_outpoint_and_query(sock: &std::path::Path, pad: &Pad) {
        let spent = pad.cbs[0].to_string();
        let unknown = "ff".repeat(32);
        let body = serde_json::to_vec(&json!([spent, unknown])).unwrap();
        let (st, resp) = http_post_unix(sock, "/internal/txs/outspends/by-txid", &body).await;
        assert_eq!(st, 200, "{resp}");
        let arr: Vec<Value> = serde_json::from_str(&resp).unwrap();
        assert_eq!(arr.len(), 2, "same-length slots");
        assert_eq!(arr[0][0]["spent"], true);
        assert!(arr[0][0].get("vin").is_some(), "{resp}");
        assert_eq!(arr[1], json!([]));
        let op = format!("{spent}:0");
        let body = serde_json::to_vec(&json!([op, "bad"])).unwrap();
        let (st, resp) = http_post_unix(sock, "/internal/txs/outspends/by-outpoint", &body).await;
        assert_eq!(st, 200, "{resp}");
        let arr: Vec<Value> = serde_json::from_str(&resp).unwrap();
        assert_eq!(arr.len(), 2);
        assert_eq!(arr[0]["spent"], true);
        assert_eq!(arr[1]["spent"], false);

        let (st, resp) =
            http_get_unix(sock, &format!("/txs/outspends?txids={spent},{unknown}")).await;
        assert_eq!(st, 200, "{resp}");
        let arr: Vec<Value> = serde_json::from_str(&resp).unwrap();
        assert_eq!(arr.len(), 2);
        assert_eq!(arr[0][0]["spent"], true);
        assert_eq!(arr[1], json!([]));
        let too_many = (0..51)
            .map(|_| "aa".repeat(32))
            .collect::<Vec<_>>()
            .join(",");
        let (st, resp) = http_get_unix(sock, &format!("/txs/outspends?txids={too_many}")).await;
        assert_eq!(st, 400, "{resp}");
        assert!(resp.contains("Too many txids requested"), "{resp}");
    }

    /// mempool.space's backend on the group-restricted socket: it tests and
    /// broadcasts three spends, pages the mempool, then pulls the bulk routes.
    #[tokio::test]
    async fn esplora_unix_internal() {
        use std::os::unix::fs::PermissionsExt;

        let pad = pad_hub("unix-internal", 4);
        let (handle, sock) = run_unix(
            std::path::Path::new("/tmp"),
            Arc::clone(&pad.q),
            Some(Arc::clone(&pad.hub)),
        )
        .await;
        let mode = std::fs::metadata(&sock).unwrap().permissions().mode() & 0o777;
        assert_eq!(
            mode, 0o660,
            "esplora socket is group-restricted, got {mode:o}"
        );
        let (st, body) = http_get_unix(&sock, "/blocks/tip/height").await;
        assert_eq!(st, 200, "{body}");
        assert_eq!(body, pad.q.tip_height().unwrap().0.to_string());

        let spk = ScriptBuf::from_bytes(vec![0x51]);
        let txs: Vec<Transaction> = (0..3)
            .map(|i| spend_true(pad.cbs[i], 1_000 + i as u64, spk.clone()))
            .collect();
        test_then_broadcast(&sock, &pad, &txs).await;
        mempool_totals_and_txid_pages(&sock, &pad).await;
        internal_mempool_pages(&sock).await;
        internal_txs_and_block_txs(&sock, &pad, &txs[0].compute_txid().to_string()).await;
        outspends_by_txid_outpoint_and_query(&sock, &pad).await;

        handle.shutdown().await;
        let _ = pad.dir;
    }
}
