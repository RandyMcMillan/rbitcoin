//! Esplora HTTP listener (axum + tower limits).

use axum::extract::State;
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::Router;
use rbitcoin_electrum::ServeLimits;
use rbitcoin_query::Query;
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
}

impl EsploraConfig {
    pub fn new(listen: SocketAddr) -> Self {
        Self {
            listen,
            limits: ServeLimits::for_public_proxy(),
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
struct AppState {
    query: Arc<Query>,
}

/// Start Esplora **plain HTTP** on `config.listen`.
///
/// TLS is external (reverse proxy). App [`ServeLimits`] always apply (concurrency,
/// body size, request timeout) — same floor as Electrum.
pub async fn run_esplora(
    config: EsploraConfig,
    query: Arc<Query>,
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

    let state = AppState { query };
    let app = Router::new()
        .route("/blocks/tip/height", get(tip_height))
        .route("/blocks/tip/hash", get(tip_hash))
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
        Ok(Some((_fk, rec))) => {
            let hex = block_hash_hex(&rec.hash);
            (
                StatusCode::OK,
                [(header::CONTENT_TYPE, "text/plain; charset=utf-8")],
                hex,
            )
                .into_response()
        }
        Ok(None) => (StatusCode::SERVICE_UNAVAILABLE, "no tip header").into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

async fn fallback_404() -> Response {
    (StatusCode::NOT_FOUND, "Not Found").into_response()
}

/// Esplora / Core display order (internal hash bytes reversed).
fn block_hash_hex(hash: &[u8; 32]) -> String {
    let mut rev = *hash;
    rev.reverse();
    rbitcoin_primitives::hex_encode(rev)
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
        let handle = run_esplora(cfg, q).await.expect("listen");
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
        let handle = run_esplora(cfg, q).await.expect("listen");
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
}
