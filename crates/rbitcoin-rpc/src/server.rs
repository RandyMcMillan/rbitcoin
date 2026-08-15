//! HTTP JSON-RPC server (axum) with Basic auth.

use crate::auth::{parse_basic_auth, resolve_rpc_auth, RpcAuth};
use crate::methods::{handle_request, RpcContext, RpcRegtest};
use axum::body::Bytes;
use axum::extract::State;
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use axum::Router;
use rbitcoin_log::info;
use rbitcoin_net::MempoolHub;
use rbitcoin_primitives::Network;
use rbitcoin_query::Query;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;

/// RPC listen configuration.
#[derive(Clone, Debug)]
pub struct RpcConfig {
    pub listen: SocketAddr,
    pub datadir: PathBuf,
    pub network: Network,
    pub rpc_user: Option<String>,
    pub rpc_password: Option<String>,
    /// Override cookie path (default `{datadir}/.cookie`).
    pub cookie_path: Option<PathBuf>,
    /// `getnetworkinfo.subversion`. Empty → `/rbitcoin:VERSION/`.
    pub subversion: Option<String>,
}

/// Live RPC server handle.
pub struct RpcHandle {
    pub local_addr: SocketAddr,
    pub cookie_path: Option<PathBuf>,
    pub auth: RpcAuth,
    pub stop: Arc<AtomicBool>,
    pub connections: Arc<AtomicU64>,
    pub initial_block_download: Arc<AtomicBool>,
    shutdown: Arc<AtomicBool>,
    task: JoinHandle<()>,
}

impl RpcHandle {
    pub async fn shutdown(self) {
        self.shutdown.store(true, Ordering::SeqCst);
        self.task.abort();
        let _ = self.task.await;
    }
}

#[derive(Clone)]
struct AppState {
    ctx: Arc<RpcContext>,
    auth: RpcAuth,
}

/// Start Core-class JSON-RPC on `config.listen` (plain HTTP; TLS via reverse proxy).
pub async fn run_rpc(
    config: RpcConfig,
    query: Arc<Query>,
    mempool: Option<Arc<MempoolHub>>,
    regtest: Option<Arc<dyn RpcRegtest>>,
    peers: Option<Arc<rbitcoin_net::PeerHub>>,
    chain: Option<Arc<rbitcoin_net::ChainHub>>,
) -> Result<RpcHandle, String> {
    let (auth, cookie_path) = resolve_rpc_auth(
        &config.datadir,
        config.rpc_user.as_deref(),
        config.rpc_password.as_deref(),
        config.cookie_path.as_deref(),
    )?;

    if auth.password.is_empty() {
        return Err("RPC auth password empty".into());
    }

    let stop = Arc::new(AtomicBool::new(false));
    let connections = Arc::new(AtomicU64::new(0));
    let ibd = Arc::new(AtomicBool::new(false));
    let ctx = Arc::new(RpcContext {
        query,
        mempool,
        network: config.network,
        start: Instant::now(),
        stop: Arc::clone(&stop),
        connections: Arc::clone(&connections),
        initial_block_download: Arc::clone(&ibd),
        subversion: config.subversion.clone().unwrap_or_else(|| {
            rbitcoin_primitives::rbitcoin_subversion(env!("CARGO_PKG_VERSION"), &[] as &[&str])
                .unwrap_or_else(|_| format!("/rbitcoin:{}/", env!("CARGO_PKG_VERSION")))
        }),
        regtest,
        peers,
        chain,
    });

    let listener = TcpListener::bind(config.listen)
        .await
        .map_err(|e| format!("rpc bind {}: {e}", config.listen))?;
    let local_addr = listener
        .local_addr()
        .map_err(|e| format!("rpc local_addr: {e}"))?;

    let state = AppState {
        ctx,
        auth: auth.clone(),
    };
    let app = Router::new().route("/", post(rpc_post)).with_state(state);

    let shutdown = Arc::new(AtomicBool::new(false));
    let shutdown_w = Arc::clone(&shutdown);
    let task = tokio::spawn(async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(async move {
                while !shutdown_w.load(Ordering::SeqCst) {
                    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                }
            })
            .await
            .ok();
    });

    if let Some(ref p) = cookie_path {
        info!(
            "rpc: HTTP JSON-RPC on {local_addr} (cookie auth {})",
            p.display()
        );
    } else {
        info!("rpc: HTTP JSON-RPC on {local_addr} (rpcuser/rpcpassword auth)");
    }

    Ok(RpcHandle {
        local_addr,
        cookie_path,
        auth,
        stop,
        connections,
        initial_block_download: ibd,
        shutdown,
        task,
    })
}

async fn rpc_post(State(state): State<AppState>, headers: HeaderMap, body: Bytes) -> Response {
    if !authorized(&state.auth, &headers) {
        return (
            StatusCode::UNAUTHORIZED,
            [(header::WWW_AUTHENTICATE, "Basic realm=\"jsonrpc\"")],
            "Unauthorized\n",
        )
            .into_response();
    }
    let parsed: serde_json::Value = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(e) => {
            let err = serde_json::json!({
                "result": null,
                "error": { "code": -32700, "message": format!("parse error: {e}") },
                "id": null,
            });
            return (StatusCode::OK, axum::Json(err)).into_response();
        }
    };
    let ctx = Arc::clone(&state.ctx);
    let joined = tokio::task::spawn_blocking(move || {
        if let Some(arr) = parsed.as_array() {
            serde_json::Value::Array(arr.iter().map(|req| rpc_one(&ctx, req)).collect())
        } else {
            rpc_one(&ctx, &parsed)
        }
    })
    .await;
    match joined {
        Ok(resp) => (StatusCode::OK, axum::Json(resp)).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, format!("rpc join: {e}")).into_response(),
    }
}

fn rpc_one(ctx: &RpcContext, req: &serde_json::Value) -> serde_json::Value {
    let method = req.get("method").and_then(|m| m.as_str()).unwrap_or("");
    let params = req.get("params").cloned().unwrap_or(serde_json::json!([]));
    let params_s = serde_json::to_string(&params).unwrap_or_else(|_| "[]".into());
    let t0 = Instant::now();
    let resp = handle_request(ctx, req);
    let wall_ms = t0.elapsed().as_millis() as u64;
    let err = resp
        .get("error")
        .and_then(|e| e.get("message"))
        .and_then(|m| m.as_str())
        .map(|s| s.to_string());
    rbitcoin_log::api_call("rpc", "-", method, &params_s, wall_ms, err.as_deref());
    resp
}

fn authorized(auth: &RpcAuth, headers: &HeaderMap) -> bool {
    let Some(val) = headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
    else {
        return false;
    };
    let Some((u, p)) = parse_basic_auth(val) else {
        return false;
    };
    auth.matches(&u, &p)
}

/// Build Basic auth header value for clients.
pub fn basic_auth_header(auth: &RpcAuth) -> String {
    use base64::Engine;
    let tok = base64::engine::general_purpose::STANDARD.encode(auth.cookie_line());
    format!("Basic {tok}")
}

/// Call a method against a live server (test helper / smoke).
pub async fn post_rpc(
    addr: SocketAddr,
    auth: &RpcAuth,
    method: &str,
    params: serde_json::Value,
) -> Result<serde_json::Value, String> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let body = serde_json::json!({
        "jsonrpc": "1.0",
        "id": "test",
        "method": method,
        "params": params,
    });
    let body_s = body.to_string();
    let auth_h = basic_auth_header(auth);
    let req = format!(
        "POST / HTTP/1.1\r\nHost: {addr}\r\nAuthorization: {auth_h}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body_s}",
        body_s.len()
    );
    let mut stream = tokio::net::TcpStream::connect(addr)
        .await
        .map_err(|e| format!("connect: {e}"))?;
    stream
        .write_all(req.as_bytes())
        .await
        .map_err(|e| format!("write: {e}"))?;
    let mut buf = Vec::new();
    stream
        .read_to_end(&mut buf)
        .await
        .map_err(|e| format!("read: {e}"))?;
    let text = String::from_utf8_lossy(&buf);
    let body_start = text.find("\r\n\r\n").ok_or("no HTTP body")? + 4;
    let json_body = &text[body_start..];
    serde_json::from_str(json_body).map_err(|e| format!("json: {e} body={json_body}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use rbitcoin_primitives::Network;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[tokio::test]
    async fn rpc_smoke_getblockcount_and_help() {
        let n = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("rbitcoin-rpc-srv-{n}"));
        std::fs::create_dir_all(&dir).unwrap();
        let q = Arc::new(Query::open_or_create(dir.join("store")).unwrap());
        let mp =
            MempoolHub::open_with_weight(dir.join("mempool"), Arc::clone(&q), 300_000_000).unwrap();
        mp.set_relay_enabled(true);
        let cfg = RpcConfig {
            listen: "127.0.0.1:0".parse().unwrap(),
            datadir: dir.clone(),
            network: Network::Regtest,
            rpc_user: Some("testuser".into()),
            rpc_password: Some("testpass".into()),
            cookie_path: None,
            subversion: None,
        };
        let handle = run_rpc(cfg, q, Some(mp), None, None, None).await.unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(80)).await;

        let count = post_rpc(
            handle.local_addr,
            &handle.auth,
            "getblockcount",
            serde_json::json!([]),
        )
        .await
        .unwrap();
        assert!(count["error"].is_null(), "{count}");
        assert_eq!(count["result"], 0);

        let help = post_rpc(
            handle.local_addr,
            &handle.auth,
            "help",
            serde_json::json!([]),
        )
        .await
        .unwrap();
        assert!(help["error"].is_null(), "{help}");
        let s = help["result"].as_str().unwrap();
        assert!(s.contains("getblockchaininfo"));

        let mem = post_rpc(
            handle.local_addr,
            &handle.auth,
            "getmempoolinfo",
            serde_json::json!([]),
        )
        .await
        .unwrap();
        assert!(mem["error"].is_null(), "{mem}");
        assert_eq!(mem["result"]["size"], 0);

        let chain = post_rpc(
            handle.local_addr,
            &handle.auth,
            "getblockchaininfo",
            serde_json::json!([]),
        )
        .await
        .unwrap();
        assert!(chain["error"].is_null(), "{chain}");
        assert_eq!(chain["result"]["chain"], "regtest");

        // 401 without auth
        let mut stream = tokio::net::TcpStream::connect(handle.local_addr)
            .await
            .unwrap();
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let bad = b"POST / HTTP/1.1\r\nHost: x\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{}";
        stream.write_all(bad).await.unwrap();
        let mut buf = Vec::new();
        stream.read_to_end(&mut buf).await.unwrap();
        let text = String::from_utf8_lossy(&buf);
        assert!(
            text.contains("401") || text.contains("Unauthorized"),
            "{text}"
        );

        handle.shutdown().await;
        let _ = std::fs::remove_dir_all(&dir);
    }
}
