//! Esplora HTTP listener (axum + tower limits).

use crate::handlers;
use crate::tx_json::{build_tx_json, tx_status_json};
use axum::extract::{Path, State};
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::Json;
use axum::Router;
use bitcoin::consensus::Encodable;
use bitcoin::Network;
use rbitcoin_electrum::ServeLimits;
use rbitcoin_net::MempoolHub;
use rbitcoin_primitives::Height;
use rbitcoin_query::Query;
use rbitcoin_store::StoreError;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tower::limit::ConcurrencyLimitLayer;
use tower_http::limit::RequestBodyLimitLayer;
use tower_http::timeout::TimeoutLayer;

/// Esplora HTTP server config (listen + shared DoS floor).
#[derive(Clone, Debug)]
pub struct EsploraConfig {
    pub listen: SocketAddr,
    /// Shared with Electrum ([`ServeLimits::for_public_proxy`] defaults).
    pub limits: ServeLimits,
    /// Address encoding network (mainnet/testnet/signet/regtest).
    pub network: Network,
}

impl EsploraConfig {
    pub fn new(listen: SocketAddr) -> Self {
        Self::with_network(listen, Network::Bitcoin)
    }

    pub fn with_network(listen: SocketAddr, network: Network) -> Self {
        Self {
            listen,
            limits: ServeLimits::for_public_proxy(),
            network,
        }
    }
}

pub struct EsploraHandle {
    pub local_addr: SocketAddr,
    shutdown: Arc<AtomicBool>,
    task: JoinHandle<()>,
}

impl EsploraHandle {
    pub async fn shutdown(self) {
        self.shutdown.store(true, Ordering::SeqCst);
        self.task.abort();
        let _ = self.task.await;
    }
}

#[derive(Clone)]
pub(crate) struct AppState {
    pub(crate) query: Arc<Query>,
    pub(crate) network: Network,
    pub(crate) mempool: Option<Arc<MempoolHub>>,
    pub(crate) max_body: usize,
}

/// Start Esplora **plain HTTP** on `config.listen`.
///
/// TLS is external (reverse proxy). App [`ServeLimits`] always apply (concurrency,
/// body size, request timeout) — same floor as Electrum.
///
/// Optional `mempool` enables fee estimates, mempool summary, and `POST /tx`.
pub async fn run_esplora(
    config: EsploraConfig,
    query: Arc<Query>,
    mempool: Option<Arc<MempoolHub>>,
) -> Result<EsploraHandle, std::io::Error> {
    let listener = TcpListener::bind(config.listen).await?;
    let local_addr = listener.local_addr()?;
    let shutdown = Arc::new(AtomicBool::new(false));
    let shutdown_c = shutdown.clone();

    let max_conn = config.limits.max_connections.max(1);
    let max_body = config.limits.max_request_bytes.max(1);
    let idle = config.limits.idle_timeout;
    // Floor for request timeout: at least 1s so unit tests with short idle still work.
    let timeout = idle.max(Duration::from_secs(1));

    let state = AppState {
        query,
        network: config.network,
        mempool,
        max_body,
    };
    let app = Router::new()
        .route("/blocks/tip/height", get(tip_height))
        .route("/blocks/tip/hash", get(tip_hash))
        .route("/block-height/:height", get(block_height))
        .route("/block/:hash/header", get(block_header))
        .route("/block/:hash/txids", get(handlers::block_txids))
        .route("/block/:hash/txs", get(handlers::block_txs_0))
        .route("/block/:hash/txs/:start", get(handlers::block_txs_start))
        .route("/tx/:txid", get(tx_full))
        .route("/tx/:txid/hex", get(tx_hex))
        .route("/tx/:txid/status", get(tx_status))
        .route("/tx/:txid/merkle-proof", get(handlers::tx_merkle_proof))
        .route("/tx/:txid/outspend/:vout", get(handlers::tx_outspend))
        .route("/tx/:txid/outspends", get(handlers::tx_outspends))
        .route("/tx", post(handlers::post_tx))
        .route("/address/:addr", get(handlers::address_info))
        .route("/address/:addr/utxo", get(handlers::address_utxo))
        .route("/address/:addr/txs", get(handlers::address_txs))
        .route("/address/:addr/txs/chain", get(handlers::address_txs_chain))
        .route(
            "/address/:addr/txs/chain/:last",
            get(handlers::address_txs_chain_cursor),
        )
        .route("/scripthash/:hash", get(handlers::scripthash_info))
        .route("/scripthash/:hash/utxo", get(handlers::scripthash_utxo))
        .route("/scripthash/:hash/txs", get(handlers::scripthash_txs))
        .route(
            "/scripthash/:hash/txs/chain",
            get(handlers::scripthash_txs_chain),
        )
        .route(
            "/scripthash/:hash/txs/chain/:last",
            get(handlers::scripthash_txs_chain_cursor),
        )
        .route("/mempool", get(handlers::mempool_info))
        .route("/fee-estimates", get(handlers::fee_estimates))
        .fallback(fallback_404)
        .layer(TimeoutLayer::new(timeout))
        .layer(RequestBodyLimitLayer::new(max_body))
        .layer(ConcurrencyLimitLayer::new(max_conn))
        .with_state(state);

    let task = tokio::spawn(async move {
        let serve = axum::serve(listener, app).with_graceful_shutdown(async move {
            while !shutdown_c.load(Ordering::SeqCst) {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        });
        if let Err(e) = serve.await {
            rbitcoin_log::warn!("esplora: serve ended: {e}");
        }
    });

    Ok(EsploraHandle {
        local_addr,
        shutdown,
        task,
    })
}

async fn tip_height(State(st): State<AppState>) -> Response {
    match st.query.tip_height() {
        Some(h) => (StatusCode::OK, format!("{}", h.0)).into_response(),
        None => (StatusCode::SERVICE_UNAVAILABLE, "no chain tip").into_response(),
    }
}

async fn tip_hash(State(st): State<AppState>) -> Response {
    let Some(h) = st.query.tip_height() else {
        return (StatusCode::SERVICE_UNAVAILABLE, "no chain tip").into_response();
    };
    match st.query.header_at_height(h) {
        Ok(Some((_fk, rec))) => plain_ok(block_hash_hex(&rec.hash)),
        Ok(None) => (StatusCode::SERVICE_UNAVAILABLE, "no tip header").into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

/// `GET /block-height/:height` → display-order block hash (plain text).
async fn block_height(State(st): State<AppState>, Path(height): Path<u32>) -> Response {
    match st.query.header_at_height(Height(height)) {
        Ok(Some((_fk, rec))) => plain_ok(block_hash_hex(&rec.hash)),
        Ok(None) => not_found(),
        Err(e) => store_err(e),
    }
}

/// `GET /block/:hash/header` → 80-byte header hex.
async fn block_header(State(st): State<AppState>, Path(hash_hex): Path<String>) -> Response {
    let Ok(hash) = parse_hash32(&hash_hex) else {
        return not_found();
    };
    // Prefer best-chain height path (fills prev correctly for wire header).
    match st.query.height_of_hash(&hash) {
        Ok(Some(h)) => match st.query.wire_header_at_height(h) {
            Ok(hdr) => match encode_header_hex(&hdr) {
                Ok(hex) => plain_ok(hex),
                Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e).into_response(),
            },
            Err(e) => store_err(e),
        },
        Ok(None) => not_found(),
        Err(e) => store_err(e),
    }
}

/// `GET /tx/:txid` → full Esplora transaction JSON (incl. asm/type/address).
async fn tx_full(State(st): State<AppState>, Path(txid_hex): Path<String>) -> Response {
    let Ok(txid) = parse_hash32(&txid_hex) else {
        return not_found();
    };
    match st.query.get_tx_by_txid(&txid) {
        Ok(Some((fk, _))) => match build_tx_json(&st.query, fk, st.network) {
            Ok(v) => Json(v).into_response(),
            Err(e) => store_err(e),
        },
        Ok(None) => not_found(),
        Err(e) => store_err(e),
    }
}

/// `GET /tx/:txid/hex` → raw consensus-encoded transaction hex.
async fn tx_hex(State(st): State<AppState>, Path(txid_hex): Path<String>) -> Response {
    let Ok(txid) = parse_hash32(&txid_hex) else {
        return not_found();
    };
    match st.query.get_tx_by_txid(&txid) {
        Ok(Some((fk, _))) => match st.query.tx_wire_bytes(fk) {
            Ok(raw) => plain_ok(rbitcoin_primitives::hex_encode(raw)),
            Err(e) => store_err(e),
        },
        Ok(None) => not_found(),
        Err(e) => store_err(e),
    }
}

/// `GET /tx/:txid/status` → Esplora confirmation status JSON.
async fn tx_status(State(st): State<AppState>, Path(txid_hex): Path<String>) -> Response {
    let Ok(txid) = parse_hash32(&txid_hex) else {
        return not_found();
    };
    match st.query.get_tx_by_txid(&txid) {
        Ok(Some((fk, _))) => match tx_status_json(&st.query, fk) {
            Ok(v) => Json(v).into_response(),
            Err(e) => store_err(e),
        },
        Ok(None) => not_found(),
        Err(e) => store_err(e),
    }
}

async fn fallback_404() -> Response {
    not_found()
}

pub(crate) fn plain_ok(body: String) -> Response {
    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "text/plain; charset=utf-8")],
        body,
    )
        .into_response()
}

pub(crate) fn not_found() -> Response {
    (StatusCode::NOT_FOUND, "Not Found").into_response()
}

pub(crate) fn store_err(e: rbitcoin_query::QueryError) -> Response {
    // QueryError is StoreError.
    match e {
        StoreError::NotFound => not_found(),
        other => (StatusCode::INTERNAL_SERVER_ERROR, other.to_string()).into_response(),
    }
}

/// Esplora / Core display order (internal hash bytes reversed).
pub(crate) fn block_hash_hex(hash: &[u8; 32]) -> String {
    let mut rev = *hash;
    rev.reverse();
    rbitcoin_primitives::hex_encode(rev)
}

/// Parse 32-byte hash/txid hex (display order) → internal byte order.
pub(crate) fn parse_hash32(s: &str) -> Result<[u8; 32], ()> {
    let mut bytes = rbitcoin_primitives::hex_decode(s).map_err(|_| ())?;
    if bytes.len() != 32 {
        return Err(());
    }
    bytes.reverse();
    let mut out = [0u8; 32];
    out.copy_from_slice(&bytes);
    Ok(out)
}

fn encode_header_hex(hdr: &bitcoin::block::Header) -> Result<String, String> {
    let mut buf = Vec::with_capacity(80);
    hdr.consensus_encode(&mut buf)
        .map_err(|_| "header encode".to_string())?;
    Ok(rbitcoin_primitives::hex_encode(buf))
}

#[cfg(test)]
mod tests {
    use super::*;
    use rbitcoin_primitives::{Fk, Height};
    use rbitcoin_query::{Query, TxApply};
    use rbitcoin_store::{HeaderRecord, InputRecord, OutputRecord, TxRecord};
    use std::time::{SystemTime, UNIX_EPOCH};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;

    fn temp_query(label: &str) -> (std::path::PathBuf, Query) {
        let n = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("rbitcoin-esplora-{label}-{n}"));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let q = Query::open_or_create(dir.join("store")).unwrap();
        (dir, q)
    }

    fn coinbase(h: u32, prev: Fk) -> (HeaderRecord, TxApply) {
        let mut hash = [0u8; 32];
        hash[0..4].copy_from_slice(&h.to_le_bytes());
        hash[5] = 0xab;
        let header = HeaderRecord {
            prev_fk: prev,
            version: 1,
            timestamp: h + 1,
            bits: 0x207fffff,
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
                create_fk: Fk::NULL,
                prev_index: u32::MAX,
                sequence: u32::MAX,
                script_sig: vec![h as u8],
                witness: vec![],
            }],
            outputs: vec![OutputRecord::unspent(50_0000_0000, vec![0x51])],
        };
        (header, ta)
    }

    async fn http_get(addr: SocketAddr, path: &str) -> (u16, String) {
        let mut stream = TcpStream::connect(addr).await.expect("connect");
        let req = format!("GET {path} HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n");
        stream.write_all(req.as_bytes()).await.unwrap();
        let mut buf = Vec::new();
        stream.read_to_end(&mut buf).await.unwrap();
        let text = String::from_utf8_lossy(&buf);
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

    #[tokio::test]
    async fn tip_endpoints_and_unknown_404() {
        let (dir, q) = temp_query("tip");
        let mut prev = Fk::NULL;
        let mut tip_hash = [0u8; 32];
        for h in 0..3u32 {
            let (header, ta) = coinbase(h, prev);
            tip_hash = header.hash;
            prev = q.connect_block(Height(h), &header, &[ta]).unwrap();
        }
        assert_eq!(q.tip_height(), Some(Height(2)));

        let q = Arc::new(q);
        let cfg = EsploraConfig::new("127.0.0.1:0".parse().unwrap());
        let handle = run_esplora(cfg, q, None).await.expect("listen");
        let addr = handle.local_addr;

        let (st, body) = http_get(addr, "/blocks/tip/height").await;
        assert_eq!(st, 200, "height body={body}");
        assert_eq!(body, "2");

        let (st, body) = http_get(addr, "/blocks/tip/hash").await;
        assert_eq!(st, 200, "hash body={body}");
        assert_eq!(body, block_hash_hex(&tip_hash));
        assert_eq!(body.len(), 64);

        let (st, body) = http_get(addr, "/no/such/path").await;
        assert_eq!(st, 404, "404 body={body}");
        assert!(body.to_ascii_lowercase().contains("not found") || body.contains("Not Found"));

        handle.shutdown().await;
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn empty_chain_tip_is_unavailable() {
        let (dir, q) = temp_query("empty");
        let q = Arc::new(q);
        let cfg = EsploraConfig::new("127.0.0.1:0".parse().unwrap());
        let handle = run_esplora(cfg, q, None).await.expect("listen");
        let (st, _) = http_get(handle.local_addr, "/blocks/tip/height").await;
        assert_eq!(st, 503);
        let (st, _) = http_get(handle.local_addr, "/blocks/tip/hash").await;
        assert_eq!(st, 503);
        handle.shutdown().await;
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn config_defaults_use_public_proxy_limits() {
        let cfg = EsploraConfig::new("0.0.0.0:3000".parse().unwrap());
        assert_eq!(cfg.limits, ServeLimits::for_public_proxy());
    }

    /// Phase A: block-height, header, tx hex, tx status on one fixture store.
    #[tokio::test]
    async fn block_and_tx_read_path() {
        let (dir, q) = temp_query("block-tx");
        let mut prev = Fk::NULL;
        let mut hashes = Vec::new();
        let mut coinbase_txids = Vec::new();
        for h in 0..3u32 {
            let (header, ta) = coinbase(h, prev);
            hashes.push(header.hash);
            coinbase_txids.push(ta.tx.txid);
            prev = q.connect_block(Height(h), &header, &[ta]).unwrap();
        }
        assert_eq!(q.tip_height(), Some(Height(2)));

        let q = Arc::new(q);
        let cfg = EsploraConfig::with_network("127.0.0.1:0".parse().unwrap(), Network::Regtest);
        let handle = run_esplora(cfg, Arc::clone(&q), None)
            .await
            .expect("listen");
        let addr = handle.local_addr;

        // /block-height/1
        let (st, body) = http_get(addr, "/block-height/1").await;
        assert_eq!(st, 200, "block-height body={body}");
        assert_eq!(body, block_hash_hex(&hashes[1]));

        // missing height
        let (st, _) = http_get(addr, "/block-height/99").await;
        assert_eq!(st, 404);

        // /block/:hash/header — 80 bytes → 160 hex chars
        let hash_disp = block_hash_hex(&hashes[1]);
        let (st, body) = http_get(addr, &format!("/block/{hash_disp}/header")).await;
        assert_eq!(st, 200, "header body len={}", body.len());
        assert_eq!(body.len(), 160);
        // Matches Query wire encode.
        let wire = q.wire_header_at_height(Height(1)).unwrap();
        let expected = encode_header_hex(&wire).unwrap();
        assert_eq!(body, expected);

        // unknown hash
        let miss = "ff".repeat(32);
        let (st, _) = http_get(addr, &format!("/block/{miss}/header")).await;
        assert_eq!(st, 404);

        // /tx/:txid/hex
        let txid_disp = block_hash_hex(&coinbase_txids[0]); // same display reverse helper
        let (st, body) = http_get(addr, &format!("/tx/{txid_disp}/hex")).await;
        assert_eq!(st, 200, "tx hex body={body}");
        assert!(!body.is_empty());
        assert!(body.len() % 2 == 0);
        let (fk, _) = q.get_tx_by_txid(&coinbase_txids[0]).unwrap().unwrap();
        let raw = q.tx_wire_bytes(fk).unwrap();
        assert_eq!(body, rbitcoin_primitives::hex_encode(raw));

        // /tx/:txid/status
        let (st, body) = http_get(addr, &format!("/tx/{txid_disp}/status")).await;
        assert_eq!(st, 200, "status body={body}");
        let v: serde_json::Value = serde_json::from_str(&body).expect("status json");
        assert_eq!(v["confirmed"], true);
        assert_eq!(v["block_height"], 0);
        assert_eq!(v["block_hash"], block_hash_hex(&hashes[0]));
        assert!(v.get("block_time").is_some());

        // /tx/:txid full projection (asm/type keys present)
        let (st, body) = http_get(addr, &format!("/tx/{txid_disp}")).await;
        assert_eq!(st, 200, "tx full body={body}");
        let full: serde_json::Value = serde_json::from_str(&body).expect("tx json");
        assert!(full.get("txid").is_some());
        assert!(full.get("vin").is_some());
        assert!(full.get("vout").is_some());
        assert!(full.get("status").is_some());
        assert!(full.get("size").is_some());
        assert!(full.get("weight").is_some());
        assert_eq!(full["fee"], 0); // coinbase
        let v0 = &full["vout"][0];
        assert!(v0.get("scriptpubkey").is_some());
        assert!(v0.get("scriptpubkey_asm").is_some());
        assert!(v0.get("scriptpubkey_type").is_some());
        // OP_TRUE coinbase → unknown type, no address
        assert_eq!(v0["scriptpubkey_type"], "unknown");
        assert!(v0
            .get("scriptpubkey_asm")
            .unwrap()
            .as_str()
            .unwrap()
            .contains("OP_"));
        let vin0 = &full["vin"][0];
        assert_eq!(vin0["is_coinbase"], true);
        assert!(vin0.get("scriptsig_asm").is_some());

        // missing tx
        let (st, _) = http_get(addr, &format!("/tx/{miss}/hex")).await;
        assert_eq!(st, 404);
        let (st, _) = http_get(addr, &format!("/tx/{miss}/status")).await;
        assert_eq!(st, 404);
        let (st, _) = http_get(addr, &format!("/tx/{miss}")).await;
        assert_eq!(st, 404);

        // status helper unit
        let st_json = tx_status_json(&q, fk).unwrap();
        assert_eq!(st_json["confirmed"], true);
        assert_eq!(st_json["block_height"], 0);

        handle.shutdown().await;
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Steps 8–11: txids, merkle-proof, outspends, scripthash stats/utxo/pages, mempool empty.
    #[tokio::test]
    async fn remaining_routes_fixture() {
        use rbitcoin_store::script_hash;

        let (dir, q) = temp_query("remain");
        let mut prev = Fk::NULL;
        let mut hashes = Vec::new();
        let mut coinbase_txids = Vec::new();
        for h in 0..4u32 {
            let (header, ta) = coinbase(h, prev);
            hashes.push(header.hash);
            coinbase_txids.push(ta.tx.txid);
            prev = q.connect_block(Height(h), &header, &[ta]).unwrap();
        }
        let q = Arc::new(q);
        let cfg = EsploraConfig::with_network("127.0.0.1:0".parse().unwrap(), Network::Regtest);
        let handle = run_esplora(cfg, Arc::clone(&q), None)
            .await
            .expect("listen");
        let addr = handle.local_addr;

        let hash0 = block_hash_hex(&hashes[0]);
        let (st, body) = http_get(addr, &format!("/block/{hash0}/txids")).await;
        assert_eq!(st, 200, "{body}");
        let ids: Vec<String> = serde_json::from_str(&body).unwrap();
        assert_eq!(ids.len(), 1);
        assert_eq!(ids[0], block_hash_hex(&coinbase_txids[0]));

        let (st, body) = http_get(addr, &format!("/block/{hash0}/txs")).await;
        assert_eq!(st, 200, "{body}");
        let txs: Vec<serde_json::Value> = serde_json::from_str(&body).unwrap();
        assert_eq!(txs.len(), 1);
        assert!(txs[0].get("txid").is_some());

        // start not multiple of 25 → 400
        let (st, _) = http_get(addr, &format!("/block/{hash0}/txs/1")).await;
        assert_eq!(st, 400);

        let txid0 = block_hash_hex(&coinbase_txids[0]);
        let (st, body) = http_get(addr, &format!("/tx/{txid0}/merkle-proof")).await;
        assert_eq!(st, 200, "{body}");
        let mp: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(mp["block_height"], 0);
        assert_eq!(mp["pos"], 0);
        assert!(mp.get("merkle").is_some());

        let (st, body) = http_get(addr, &format!("/tx/{txid0}/outspend/0")).await;
        assert_eq!(st, 200, "{body}");
        let os: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(os["spent"], false);

        let (st, body) = http_get(addr, &format!("/tx/{txid0}/outspends")).await;
        assert_eq!(st, 200, "{body}");
        let oss: Vec<serde_json::Value> = serde_json::from_str(&body).unwrap();
        assert_eq!(oss.len(), 1);

        // Scripthash for OP_TRUE
        let sh = script_hash(&[0x51]);
        let sh_hex = block_hash_hex(&sh);
        let (st, body) = http_get(addr, &format!("/scripthash/{sh_hex}")).await;
        assert_eq!(st, 200, "{body}");
        let info: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert!(info["chain_stats"]["tx_count"].as_u64().unwrap() >= 4);
        assert!(info["chain_stats"]["funded_txo_count"].as_u64().unwrap() >= 4);

        let (st, body) = http_get(addr, &format!("/scripthash/{sh_hex}/utxo")).await;
        assert_eq!(st, 200, "{body}");
        let utxos: Vec<serde_json::Value> = serde_json::from_str(&body).unwrap();
        assert!(!utxos.is_empty());

        let (st, body) = http_get(addr, &format!("/scripthash/{sh_hex}/txs/chain")).await;
        assert_eq!(st, 200, "{body}");
        let page1: Vec<serde_json::Value> = serde_json::from_str(&body).unwrap();
        assert!(page1.len() <= 25);
        assert!(!page1.is_empty());
        // Newest first: tip coinbase first.
        assert_eq!(page1[0]["status"]["block_height"], 3);

        // Cursor uses Class A / history txids (fixture synthetic ids), not recomputed wire txids.
        use rbitcoin_query::HistoryFilter;
        let full = q
            .scripthash_history_filtered(&sh, &HistoryFilter::esplora_chain_page(None))
            .unwrap();
        assert_eq!(full.len(), 4);
        let after = full[0].txid;
        let page2_items = q
            .scripthash_history_filtered(&sh, &HistoryFilter::esplora_chain_page(Some(after)))
            .unwrap();
        assert_eq!(page2_items.len(), 3);
        assert!(!page2_items.iter().any(|i| i.txid == after));
        let last = block_hash_hex(&after);
        let (st, body) = http_get(addr, &format!("/scripthash/{sh_hex}/txs/chain/{last}")).await;
        assert_eq!(st, 200, "{body}");
        let page2: Vec<serde_json::Value> = serde_json::from_str(&body).unwrap();
        assert_eq!(page2.len(), page2_items.len());

        let (st, body) = http_get(addr, &format!("/scripthash/{sh_hex}/txs")).await;
        assert_eq!(st, 200, "{body}");
        let combined: Vec<serde_json::Value> = serde_json::from_str(&body).unwrap();
        assert!(!combined.is_empty());

        // No mempool hub: empty-ish mempool, fee estimates still 200, POST 503.
        let (st, body) = http_get(addr, "/mempool").await;
        assert_eq!(st, 200, "{body}");
        let mem: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(mem["count"], 0);

        let (st, body) = http_get(addr, "/fee-estimates").await;
        assert_eq!(st, 200, "{body}");
        let fees: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert!(fees.get("1").is_some());

        // POST /tx without hub
        let mut stream = TcpStream::connect(addr).await.unwrap();
        let req = "POST /tx HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Length: 2\r\nConnection: close\r\n\r\nab";
        stream.write_all(req.as_bytes()).await.unwrap();
        let mut buf = Vec::new();
        stream.read_to_end(&mut buf).await.unwrap();
        let text = String::from_utf8_lossy(&buf);
        assert!(
            text.contains("503") || text.contains("mempool"),
            "expected 503 without hub: {text}"
        );

        handle.shutdown().await;
        let _ = std::fs::remove_dir_all(&dir);
    }
}
