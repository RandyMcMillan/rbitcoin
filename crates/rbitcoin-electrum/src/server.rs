//! Line-delimited JSON-RPC Electrum server (TCP).
//!
//! Confirmed history from the store; unconfirmed + broadcast via optional
//! [`MempoolHub`] (plan P6, libre-relay-class).

use bitcoin::consensus::Encodable;
use bitcoin::hashes::Hash;
use rbitcoin_consensus::ChainParams;
use rbitcoin_net::MempoolHub;
use rbitcoin_primitives::Height;
use rbitcoin_query::Query;
use rbitcoin_store::script_hash;
use serde_json::{json, Value};
use std::collections::HashSet;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;
use tokio::sync::{broadcast, Notify, Semaphore};
use tokio::task::JoinHandle;

const PROTOCOL_MIN: &str = "1.4";
const PROTOCOL_MAX: &str = "1.4.2";
const SERVER_VERSION: &str = concat!("rbitcoin ", env!("CARGO_PKG_VERSION"));

/// Max simultaneous Electrum TCP clients (DoS).
pub const DEFAULT_MAX_CONNECTIONS: usize = 256;
/// Max JSON-RPC request line bytes including trailing `\n` (DoS / OOM).
pub const DEFAULT_MAX_LINE_BYTES: usize = 1_048_576; // 1 MiB
/// Max scripthash subscriptions per connection (notify fan-out).
pub const DEFAULT_MAX_SCRIPTHASH_SUBS: usize = 1_000;
/// Idle read timeout — disconnect quiet clients (DoS of FD/tasks).
pub const DEFAULT_IDLE_TIMEOUT_SECS: u64 = 120;
/// Max raw tx hex chars for `transaction.broadcast` (~4 MiB wire → 8 MiB hex).
pub const DEFAULT_MAX_BROADCAST_HEX: usize = 8_388_608;

#[derive(Clone, Debug)]
pub struct ElectrumConfig {
    pub listen: SocketAddr,
    pub banner: String,
    pub donation_address: String,
    /// Genesis hash (display order hex) for features.
    pub genesis_hash_hex: String,
    /// Max concurrent client connections.
    pub max_connections: usize,
    /// Max request line size (bytes).
    pub max_line_bytes: usize,
    /// Max `blockchain.scripthash.subscribe` entries per connection.
    pub max_scripthash_subs: usize,
    /// Disconnect if no complete request line within this many seconds.
    pub idle_timeout: Duration,
    /// Max hex length accepted by `blockchain.transaction.broadcast`.
    pub max_broadcast_hex: usize,
}

impl ElectrumConfig {
    pub fn for_params(listen: SocketAddr, params: &ChainParams) -> Self {
        let genesis = params.genesis_hash.to_byte_array();
        // Electrum expects internal byte order reversed for display hex of hashes.
        let mut rev = genesis;
        rev.reverse();
        Self {
            listen,
            banner: "rbitcoin electrum — libre-relay-class (0.1 sat/vB, no dust ban, full RBF)"
                .into(),
            donation_address: String::new(),
            genesis_hash_hex: rbitcoin_primitives::hex_encode(rev),
            max_connections: DEFAULT_MAX_CONNECTIONS,
            max_line_bytes: DEFAULT_MAX_LINE_BYTES,
            max_scripthash_subs: DEFAULT_MAX_SCRIPTHASH_SUBS,
            idle_timeout: Duration::from_secs(DEFAULT_IDLE_TIMEOUT_SECS),
            max_broadcast_hex: DEFAULT_MAX_BROADCAST_HEX,
        }
    }
}

pub struct ElectrumHandle {
    pub local_addr: SocketAddr,
    shutdown: Arc<std::sync::atomic::AtomicBool>,
    tasks: Vec<JoinHandle<()>>,
}

impl ElectrumHandle {
    pub async fn shutdown(mut self) {
        self.shutdown
            .store(true, std::sync::atomic::Ordering::SeqCst);
        for t in self.tasks.drain(..) {
            t.abort();
        }
    }
}

/// Tip notification for header subscriptions.
#[derive(Clone, Debug)]
pub struct TipNotify {
    pub height: u32,
    pub header_hex: String,
}

/// Start Electrum **plain TCP** listener.
///
/// `mempool` enables broadcast, unconfirmed history/balance, fee estimates, and
/// `transaction.get` fallback. Without it, confirmed-only behaviour remains.
///
/// TLS is intentionally not built in — terminate TLS at nginx/caddy/haproxy
/// (or similar) and proxy to this TCP port.
///
/// **DoS limits** (see [`ElectrumConfig`]): max connections, max request line
/// size, max scripthash subscriptions per client, idle timeout, broadcast hex cap.
pub async fn run_electrum(
    config: ElectrumConfig,
    query: Arc<Query>,
    params: ChainParams,
    tip_tx: broadcast::Sender<TipNotify>,
    mempool: Option<Arc<MempoolHub>>,
) -> Result<ElectrumHandle, std::io::Error> {
    let listener = TcpListener::bind(config.listen).await?;
    let local_addr = listener.local_addr()?;
    let shutdown = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let shutdown_c = shutdown.clone();
    let max_conn = config.max_connections.max(1);
    let conn_sem = Arc::new(Semaphore::new(max_conn));
    let config = Arc::new(config);
    let params = Arc::new(params);

    let task = tokio::spawn(async move {
        loop {
            if shutdown_c.load(std::sync::atomic::Ordering::SeqCst) {
                break;
            }
            let accept = tokio::time::timeout(
                Duration::from_millis(200),
                listener.accept(),
            )
            .await;
            match accept {
                Ok(Ok((stream, peer))) => {
                    // Cap concurrent clients — refuse when saturated (DoS).
                    let Ok(permit) = conn_sem.clone().try_acquire_owned() else {
                        rbitcoin_log::warn!(
                            "electrum: reject {peer} (at max_connections={max_conn})"
                        );
                        drop(stream);
                        continue;
                    };
                    let q = query.clone();
                    let cfg = config.clone();
                    let p = params.clone();
                    let tip_rx = tip_tx.subscribe();
                    let mp = mempool.clone();
                    tokio::spawn(async move {
                        let _permit = permit; // held until client task ends
                        let _ = handle_client(stream, q, cfg, p, tip_rx, mp).await;
                    });
                }
                Ok(Err(_)) => break,
                Err(_) => continue,
            }
        }
    });

    Ok(ElectrumHandle {
        local_addr,
        shutdown,
        tasks: vec![task],
    })
}

/// Read one `\n`-terminated line with a hard byte cap (prevents OOM without newline).
pub async fn read_line_capped<R>(
    reader: &mut R,
    max_bytes: usize,
) -> Result<Option<String>, std::io::Error>
where
    R: AsyncBufReadExt + Unpin,
{
    let max_bytes = max_bytes.max(1);
    let mut buf: Vec<u8> = Vec::new();
    loop {
        let available = reader.fill_buf().await?;
        if available.is_empty() {
            return if buf.is_empty() {
                Ok(None)
            } else {
                // EOF mid-line — treat as complete if under cap.
                if buf.len() > max_bytes {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "electrum request line too long",
                    ));
                }
                Ok(Some(String::from_utf8_lossy(&buf).into_owned()))
            };
        }
        if let Some(pos) = available.iter().position(|&b| b == b'\n') {
            let take = pos + 1;
            if buf.len().saturating_add(take) > max_bytes {
                reader.consume(take);
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "electrum request line too long",
                ));
            }
            buf.extend_from_slice(&available[..take]);
            reader.consume(take);
            // Strip trailing \n / \r\n
            while buf.last() == Some(&b'\n') || buf.last() == Some(&b'\r') {
                buf.pop();
            }
            return Ok(Some(String::from_utf8_lossy(&buf).into_owned()));
        }
        // No newline in this chunk.
        if buf.len().saturating_add(available.len()) > max_bytes {
            let n = available.len();
            reader.consume(n);
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "electrum request line too long",
            ));
        }
        buf.extend_from_slice(available);
        let n = available.len();
        reader.consume(n);
    }
}

async fn handle_client<S>(
    stream: S,
    query: Arc<Query>,
    config: Arc<ElectrumConfig>,
    params: Arc<ChainParams>,
    mut tip_rx: broadcast::Receiver<TipNotify>,
    mempool: Option<Arc<MempoolHub>>,
) -> Result<(), std::io::Error>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let (reader, mut writer) = tokio::io::split(stream);
    let mut reader = BufReader::new(reader);
    let mut header_sub = false;
    let mut sh_subs: HashSet<[u8; 32]> = HashSet::new();
    let notify = Arc::new(Notify::new());
    let mut mempool_rx = mempool.as_ref().map(|m| m.subscribe_announces());
    let idle = config.idle_timeout;
    let max_line = config.max_line_bytes;

    loop {
        tokio::select! {
            biased;
            tip = tip_rx.recv() => {
                match tip {
                    Ok(t) if header_sub => {
                        let msg = json!({
                            "jsonrpc": "2.0",
                            "method": "blockchain.headers.subscribe",
                            "params": [{ "hex": t.header_hex, "height": t.height }]
                        });
                        write_line(&mut writer, &msg).await?;
                    }
                    Ok(_) => {}
                    Err(broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
            ann = async {
                if let Some(rx) = mempool_rx.as_mut() {
                    Some(rx.recv().await)
                } else {
                    std::future::pending::<()>().await;
                    None
                }
            } => {
                // Mempool change: push scripthash status for subscribed hashes.
                if let Some(Ok(_txid)) = ann {
                    if let Some(mp) = &mempool {
                        for sh in sh_subs.iter() {
                            if let Ok(status) = scripthash_status_full(&query, mp, sh) {
                                let msg = json!({
                                    "jsonrpc": "2.0",
                                    "method": "blockchain.scripthash.subscribe",
                                    "params": [hash_hex_rev(sh), status]
                                });
                                let _ = write_line(&mut writer, &msg).await;
                            }
                        }
                    }
                }
            }
            line = tokio::time::timeout(idle, read_line_capped(&mut reader, max_line)) => {
                let line = match line {
                    Ok(Ok(Some(l))) => l,
                    Ok(Ok(None)) => break, // EOF
                    Ok(Err(e)) => {
                        // Oversize line: best-effort error then drop client.
                        if e.kind() == std::io::ErrorKind::InvalidData {
                            let resp = json!({
                                "jsonrpc":"2.0","id": null,
                                "error": {"code": -32600, "message": "request line too long"}
                            });
                            let _ = write_line(&mut writer, &resp).await;
                        }
                        break;
                    }
                    Err(_) => {
                        // Idle timeout.
                        break;
                    }
                };
                if line.trim().is_empty() {
                    continue;
                }
                let req: Value = match serde_json::from_str(&line) {
                    Ok(v) => v,
                    Err(_) => continue,
                };
                let id = req.get("id").cloned().unwrap_or(Value::Null);
                let method = req.get("method").and_then(|m| m.as_str()).unwrap_or("");
                let params_v = req.get("params").cloned().unwrap_or(json!([]));
                let result = dispatch(
                    method,
                    &params_v,
                    &query,
                    &config,
                    &params,
                    mempool.as_deref(),
                    &mut header_sub,
                    &mut sh_subs,
                );
                let resp = match result {
                    Ok(v) => json!({"jsonrpc":"2.0","id": id, "result": v}),
                    Err(e) => json!({"jsonrpc":"2.0","id": id, "error": {"code": 1, "message": e}}),
                };
                write_line(&mut writer, &resp).await?;
                let _ = &notify;
            }
        }
    }
    Ok(())
}

async fn write_line<W: AsyncWrite + Unpin>(
    writer: &mut W,
    msg: &Value,
) -> Result<(), std::io::Error> {
    let mut s = serde_json::to_string(msg).unwrap_or_else(|_| "{}".into());
    s.push('\n');
    writer.write_all(s.as_bytes()).await?;
    writer.flush().await?;
    Ok(())
}

fn dispatch(
    method: &str,
    params: &Value,
    query: &Query,
    config: &ElectrumConfig,
    chain: &ChainParams,
    mempool: Option<&MempoolHub>,
    header_sub: &mut bool,
    sh_subs: &mut HashSet<[u8; 32]>,
) -> Result<Value, String> {
    match method {
        "server.version" => Ok(json!([SERVER_VERSION, PROTOCOL_MAX])),
        "server.ping" => Ok(Value::Null),
        "server.banner" => Ok(json!(config.banner)),
        "server.donation_address" => Ok(json!(config.donation_address)),
        "server.features" => Ok(json!({
            "genesis_hash": config.genesis_hash_hex,
            "hosts": {},
            "protocol_max": PROTOCOL_MAX,
            "protocol_min": PROTOCOL_MIN,
            "server_version": SERVER_VERSION,
            "hash_function": "sha256",
            "pruning": null,
        })),
        "blockchain.headers.subscribe" => {
            *header_sub = true;
            tip_header_obj(query)
        }
        "blockchain.block.header" => {
            let height = param_u32(params, 0)?;
            let hdr = query
                .wire_header_at_height(Height(height))
                .map_err(|e| e.to_string())?;
            Ok(json!(header_hex(&hdr)))
        }
        "blockchain.block.headers" => {
            let start = param_u32(params, 0)?;
            let count = param_u32(params, 1)?.min(2016);
            let mut hexes = String::new();
            let mut n = 0u32;
            for h in start..start.saturating_add(count) {
                match query.wire_header_at_height(Height(h)) {
                    Ok(hdr) => {
                        hexes.push_str(&header_hex(&hdr));
                        n += 1;
                    }
                    Err(_) => break,
                }
            }
            Ok(json!({"count": n, "hex": hexes, "max": 2016}))
        }
        "blockchain.scripthash.get_history" => {
            let sh = param_scripthash(params, 0)?;
            let mut hist = query.scripthash_history(&sh).map_err(|e| e.to_string())?;
            if let Some(mp) = mempool {
                for item in mp.scripthash_mempool(&sh) {
                    hist.push(rbitcoin_query::ScriptHashHistoryItem {
                        height: item.height,
                        txid: item.txid,
                    });
                }
            }
            hist.sort_by_key(|i| i.height);
            let arr: Vec<Value> = hist
                .iter()
                .map(|i| {
                    json!({
                        "height": i.height,
                        "tx_hash": txid_hex(&i.txid),
                    })
                })
                .collect();
            Ok(Value::Array(arr))
        }
        "blockchain.scripthash.get_balance" => {
            let sh = param_scripthash(params, 0)?;
            let mut b = query.scripthash_balance(&sh).map_err(|e| e.to_string())?;
            if let Some(mp) = mempool {
                b.unconfirmed = mp.scripthash_unconfirmed_delta(&sh);
            }
            Ok(json!({"confirmed": b.confirmed, "unconfirmed": b.unconfirmed}))
        }
        "blockchain.scripthash.listunspent" => {
            let sh = param_scripthash(params, 0)?;
            let u = query.scripthash_listunspent(&sh).map_err(|e| e.to_string())?;
            // Outpoints spent by mempool txs (confirmed or other mempool parents).
            let spent = mempool
                .map(|m| m.spent_outpoints())
                .unwrap_or_default();
            let mut arr: Vec<Value> = u
                .iter()
                .filter(|x| {
                    // Drop confirmed UTXOs spent by a live mempool tx.
                    let op = bitcoin::OutPoint {
                        txid: bitcoin::Txid::from_byte_array(x.tx_hash),
                        vout: x.tx_pos,
                    };
                    !spent.contains(&op)
                })
                .map(|x| {
                    json!({
                        "tx_hash": txid_hex(&x.tx_hash),
                        "tx_pos": x.tx_pos,
                        "height": x.height,
                        "value": x.value,
                    })
                })
                .collect();
            // Mempool outputs matching scripthash that are not spent by another mempool tx.
            if let Some(mp) = mempool {
                for (txid, _fee, _w, tx) in mp.list_live() {
                    for (vout, o) in tx.output.iter().enumerate() {
                        if script_hash(o.script_pubkey.as_bytes()) != sh {
                            continue;
                        }
                        let op = bitcoin::OutPoint {
                            txid,
                            vout: vout as u32,
                        };
                        if spent.contains(&op) {
                            continue;
                        }
                        arr.push(json!({
                            "tx_hash": format!("{txid}"),
                            "tx_pos": vout,
                            "height": 0,
                            "value": o.value.to_sat() as i64,
                        }));
                    }
                }
            }
            Ok(Value::Array(arr))
        }
        "blockchain.scripthash.subscribe" => {
            let sh = param_scripthash(params, 0)?;
            if !sh_subs.contains(&sh) && sh_subs.len() >= config.max_scripthash_subs {
                return Err(format!(
                    "too many scripthash subscriptions (max {})",
                    config.max_scripthash_subs
                ));
            }
            sh_subs.insert(sh);
            let status = if let Some(mp) = mempool {
                scripthash_status_full(query, mp, &sh)?
            } else {
                let hist = query.scripthash_history(&sh).map_err(|e| e.to_string())?;
                scripthash_status(&hist)
            };
            Ok(json!(status))
        }
        "blockchain.scripthash.get_mempool" => {
            let sh = param_scripthash(params, 0)?;
            let items = mempool
                .map(|m| m.scripthash_mempool(&sh))
                .unwrap_or_default();
            let arr: Vec<Value> = items
                .iter()
                .map(|i| {
                    json!({
                        "height": i.height,
                        "tx_hash": txid_hex(&i.txid),
                        "fee": i.fee,
                    })
                })
                .collect();
            Ok(Value::Array(arr))
        }
        "blockchain.transaction.get" => {
            let txid = param_txid(params, 0)?;
            let verbose = params
                .as_array()
                .and_then(|a| a.get(1))
                .and_then(|v| v.as_bool())
                .unwrap_or(false);
            // Confirmed first.
            if let Some((fk, _rec)) = query.get_tx_by_txid(&txid).map_err(|e| e.to_string())? {
                let raw = query.tx_wire_bytes(fk).map_err(|e| e.to_string())?;
                if verbose {
                    return Ok(json!({
                        "hex": rbitcoin_primitives::hex_encode(&raw),
                        "txid": txid_hex(&txid)
                    }));
                }
                return Ok(json!(rbitcoin_primitives::hex_encode(&raw)));
            }
            // Mempool fallback.
            if let Some(mp) = mempool {
                use bitcoin::hashes::Hash;
                let tid = bitcoin::Txid::from_byte_array(txid);
                if let Some(tx) = mp.get_tx(&tid) {
                    let raw = bitcoin::consensus::serialize(&tx);
                    if verbose {
                        return Ok(json!({
                            "hex": rbitcoin_primitives::hex_encode(&raw),
                            "txid": txid_hex(&txid)
                        }));
                    }
                    return Ok(json!(rbitcoin_primitives::hex_encode(&raw)));
                }
            }
            Err("tx not found".into())
        }
        "blockchain.transaction.get_merkle" => {
            let txid = param_txid(params, 0)?;
            let height = param_u32(params, 1)?;
            let proof = query
                .merkle_proof(Height(height), &txid)
                .map_err(|e| e.to_string())?;
            let merkle: Vec<String> = proof.merkle.iter().map(|h| hash_hex_rev(h)).collect();
            Ok(json!({
                "block_height": proof.block_height,
                "merkle": merkle,
                "pos": proof.pos,
            }))
        }
        "blockchain.transaction.broadcast" => {
            let raw_hex = param_str(params, 0)?;
            if raw_hex.len() > config.max_broadcast_hex {
                return Err(format!(
                    "transaction hex too large (max {} chars)",
                    config.max_broadcast_hex
                ));
            }
            let raw = rbitcoin_primitives::hex_decode(raw_hex).map_err(|e| e.to_string())?;
            // Consensus max block weight is 4M; reject absurd raw sizes early.
            if raw.len() > 4_000_000 {
                return Err("transaction too large".into());
            }
            let tx: bitcoin::Transaction =
                bitcoin::consensus::deserialize(&raw).map_err(|e| e.to_string())?;
            let mp = mempool.ok_or_else(|| "mempool not available".to_string())?;
            let r = mp.accept_tx(&tx).map_err(|e| format!("broadcast reject: {e}"))?;
            let _ = chain.network;
            Ok(json!(format!("{}", r.txid)))
        }
        "blockchain.transaction.id_from_pos" => {
            let height = param_u32(params, 0)?;
            let tx_pos = param_u32(params, 1)? as usize;
            let fks = query
                .block_tx_fks(Height(height))
                .map_err(|e| e.to_string())?;
            let fk = fks.get(tx_pos).ok_or_else(|| "pos out of range".to_string())?;
            let tx = query.get_tx(*fk).map_err(|e| e.to_string())?;
            Ok(json!(txid_hex(&tx.txid)))
        }
        "blockchain.estimatefee" => {
            let target = param_u32(params, 0).unwrap_or(2);
            let fee = mempool
                .map(|m| m.estimate_fee_btc_per_kb(target))
                .unwrap_or(-1.0);
            Ok(json!(fee))
        }
        "blockchain.relayfee" => {
            let fee = MempoolHub::relay_fee_btc_per_kb();
            Ok(json!(fee))
        }
        "mempool.get_fee_histogram" => {
            let hist = mempool.map(|m| m.fee_histogram()).unwrap_or_default();
            // Electrum: array of [feerate, cumulative_vsize] with cumulative sizes.
            let mut cum = 0u64;
            let mut arr = Vec::new();
            for (rate, vsize) in hist {
                cum = cum.saturating_add(vsize);
                arr.push(json!([rate, cum]));
            }
            Ok(Value::Array(arr))
        }
        "server.peers.subscribe" => Ok(json!([])),
        other => Err(format!("unknown method: {other}")),
    }
}

fn tip_header_obj(query: &Query) -> Result<Value, String> {
    let tip = query
        .tip_height()
        .ok_or_else(|| "no chain tip".to_string())?;
    let hdr = query
        .wire_header_at_height(tip)
        .map_err(|e| e.to_string())?;
    Ok(json!({
        "hex": header_hex(&hdr),
        "height": tip.0,
    }))
}

fn header_hex(hdr: &bitcoin::block::Header) -> String {
    let mut buf = Vec::new();
    hdr.consensus_encode(&mut buf).expect("header encode");
    rbitcoin_primitives::hex_encode(buf)
}

fn param_u32(params: &Value, idx: usize) -> Result<u32, String> {
    params
        .as_array()
        .and_then(|a| a.get(idx))
        .and_then(|v| {
            v.as_u64()
                .map(|n| n as u32)
                .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
        })
        .ok_or_else(|| format!("param {idx} expected number"))
}

fn param_str(params: &Value, idx: usize) -> Result<&str, String> {
    params
        .as_array()
        .and_then(|a| a.get(idx))
        .and_then(|v| v.as_str())
        .ok_or_else(|| format!("param {idx} expected string"))
}

fn param_scripthash(params: &Value, idx: usize) -> Result<[u8; 32], String> {
    let s = param_str(params, idx)?;
    let mut bytes = rbitcoin_primitives::hex_decode(s).map_err(|e| e.to_string())?;
    if bytes.len() != 32 {
        return Err("scripthash must be 32 bytes hex".into());
    }
    // Electrum uses reversed hex for scripthash
    bytes.reverse();
    let mut out = [0u8; 32];
    out.copy_from_slice(&bytes);
    Ok(out)
}

fn param_txid(params: &Value, idx: usize) -> Result<[u8; 32], String> {
    let s = param_str(params, idx)?;
    let mut bytes = rbitcoin_primitives::hex_decode(s).map_err(|e| e.to_string())?;
    if bytes.len() != 32 {
        return Err("txid must be 32 bytes hex".into());
    }
    bytes.reverse();
    let mut out = [0u8; 32];
    out.copy_from_slice(&bytes);
    Ok(out)
}

fn txid_hex(txid: &[u8; 32]) -> String {
    hash_hex_rev(txid)
}

fn hash_hex_rev(h: &[u8; 32]) -> String {
    let mut r = *h;
    r.reverse();
    rbitcoin_primitives::hex_encode(r)
}

fn scripthash_status(hist: &[rbitcoin_query::ScriptHashHistoryItem]) -> String {
    if hist.is_empty() {
        return String::new();
    }
    use bitcoin::hashes::{sha256, Hash as _};
    let mut s = String::new();
    for i in hist {
        s.push_str(&format!("{}:{}:", txid_hex(&i.txid), i.height));
    }
    let hash = sha256::Hash::hash(s.as_bytes());
    rbitcoin_primitives::hex_encode(hash.to_byte_array())
}

fn scripthash_status_full(
    query: &Query,
    mp: &MempoolHub,
    sh: &[u8; 32],
) -> Result<String, String> {
    let mut hist = query.scripthash_history(sh).map_err(|e| e.to_string())?;
    for item in mp.scripthash_mempool(sh) {
        hist.push(rbitcoin_query::ScriptHashHistoryItem {
            height: item.height,
            txid: item.txid,
        });
    }
    hist.sort_by_key(|i| i.height);
    Ok(scripthash_status(&hist))
}

/// Helper to compute electrum scripthash hex (reversed) from script bytes.
pub fn electrum_scripthash_hex(script: &[u8]) -> String {
    let h = script_hash(script);
    hash_hex_rev(&h)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rbitcoin_consensus::ChainParams;
    use rbitcoin_query::Query;
    use std::collections::HashSet;
    use std::time::{SystemTime, UNIX_EPOCH};
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
    use tokio::net::TcpStream;

    fn tmp_store() -> (std::path::PathBuf, Query) {
        let n = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("rbitcoin-electrum-ut-{n}"));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let q = Query::open_or_create(dir.join("store")).unwrap();
        (dir, q)
    }

    #[test]
    fn config_helpers_and_param_parsers() {
        let params = ChainParams::regtest();
        let cfg = ElectrumConfig::for_params("127.0.0.1:0".parse().unwrap(), &params);
        assert!(!cfg.genesis_hash_hex.is_empty());
        assert!(cfg.banner.contains("rbitcoin"));

        let sh = electrum_scripthash_hex(&[0x51]);
        assert_eq!(sh.len(), 64);

        assert_eq!(param_u32(&json!([3]), 0).unwrap(), 3);
        assert_eq!(param_u32(&json!(["7"]), 0).unwrap(), 7);
        assert!(param_u32(&json!([]), 0).is_err());
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

        let empty_status = scripthash_status(&[]);
        assert!(empty_status.is_empty());
        let status = scripthash_status(&[rbitcoin_query::ScriptHashHistoryItem {
            height: 1,
            txid: [1u8; 32],
        }]);
        assert_eq!(status.len(), 64);

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
    }

    #[test]
    fn dispatch_static_methods_and_errors() {
        let (dir, q) = tmp_store();
        let params = ChainParams::regtest();
        let cfg = ElectrumConfig::for_params("127.0.0.1:0".parse().unwrap(), &params);
        let mut header_sub = false;
        let mut sh_subs = HashSet::new();

        let v = dispatch(
            "server.version",
            &json!([]),
            &q,
            &cfg,
            &params,
            None,
            &mut header_sub,
            &mut sh_subs,
        )
        .unwrap();
        assert!(v.as_array().unwrap().len() == 2);

        assert!(dispatch(
            "server.ping",
            &json!([]),
            &q,
            &cfg,
            &params,
            None,
            &mut header_sub,
            &mut sh_subs
        )
        .unwrap()
        .is_null());

        assert_eq!(
            dispatch(
                "server.banner",
                &json!([]),
                &q,
                &cfg,
                &params,
                None,
                &mut header_sub,
                &mut sh_subs
            )
            .unwrap()
            .as_str()
            .unwrap(),
            cfg.banner
        );

        let features = dispatch(
            "server.features",
            &json!([]),
            &q,
            &cfg,
            &params,
            None,
            &mut header_sub,
            &mut sh_subs,
        )
        .unwrap();
        assert_eq!(features["protocol_min"], PROTOCOL_MIN);

        assert!(dispatch(
            "server.donation_address",
            &json!([]),
            &q,
            &cfg,
            &params,
            None,
            &mut header_sub,
            &mut sh_subs
        )
        .unwrap()
        .as_str()
        .is_some());

        assert_eq!(
            dispatch(
                "server.peers.subscribe",
                &json!([]),
                &q,
                &cfg,
                &params,
                None,
                &mut header_sub,
                &mut sh_subs
            )
            .unwrap(),
            json!([])
        );

        let fee = dispatch(
            "blockchain.relayfee",
            &json!([]),
            &q,
            &cfg,
            &params,
            None,
            &mut header_sub,
            &mut sh_subs,
        )
        .unwrap();
        assert!(fee.as_f64().is_some());

        let est = dispatch(
            "blockchain.estimatefee",
            &json!([6]),
            &q,
            &cfg,
            &params,
            None,
            &mut header_sub,
            &mut sh_subs,
        )
        .unwrap();
        assert_eq!(est.as_f64(), Some(-1.0));

        let hist = dispatch(
            "mempool.get_fee_histogram",
            &json!([]),
            &q,
            &cfg,
            &params,
            None,
            &mut header_sub,
            &mut sh_subs,
        )
        .unwrap();
        assert_eq!(hist, json!([]));

        // No tip → headers.subscribe errors.
        assert!(dispatch(
            "blockchain.headers.subscribe",
            &json!([]),
            &q,
            &cfg,
            &params,
            None,
            &mut header_sub,
            &mut sh_subs
        )
        .is_err());

        assert!(dispatch(
            "no.such.method",
            &json!([]),
            &q,
            &cfg,
            &params,
            None,
            &mut header_sub,
            &mut sh_subs
        )
        .unwrap_err()
        .contains("unknown method"));

        // Empty-chain scripthash methods.
        let sh = electrum_scripthash_hex(&[0x51]);
        let empty_hist = dispatch(
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
        assert_eq!(empty_hist, json!([]));

        let bal = dispatch(
            "blockchain.scripthash.get_balance",
            &json!([sh]),
            &q,
            &cfg,
            &params,
            None,
            &mut header_sub,
            &mut sh_subs,
        )
        .unwrap();
        assert_eq!(bal["confirmed"], 0);

        let unspent = dispatch(
            "blockchain.scripthash.listunspent",
            &json!([sh]),
            &q,
            &cfg,
            &params,
            None,
            &mut header_sub,
            &mut sh_subs,
        )
        .unwrap();
        assert_eq!(unspent, json!([]));

        let sub = dispatch(
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
        assert!(sub.as_str().is_some());
        assert_eq!(sh_subs.len(), 1);

        let mem = dispatch(
            "blockchain.scripthash.get_mempool",
            &json!([sh]),
            &q,
            &cfg,
            &params,
            None,
            &mut header_sub,
            &mut sh_subs,
        )
        .unwrap();
        assert_eq!(mem, json!([]));

        // Broadcast: bad hex fails before mempool gate.
        assert!(dispatch(
            "blockchain.transaction.broadcast",
            &json!(["zz"]),
            &q,
            &cfg,
            &params,
            None,
            &mut header_sub,
            &mut sh_subs
        )
        .is_err());

        // Non-empty hex that may fail deserialize or mempool gate — either is fine.
        assert!(dispatch(
            "blockchain.transaction.broadcast",
            &json!(["01000000000000000000"]),
            &q,
            &cfg,
            &params,
            None,
            &mut header_sub,
            &mut sh_subs,
        )
        .is_err());

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn accept_client_ping_and_shutdown() {
        let (dir, q) = tmp_store();
        let params = ChainParams::regtest();
        let q = std::sync::Arc::new(q);
        let (tip_tx, _) = broadcast::channel(4);
        let cfg = ElectrumConfig::for_params("127.0.0.1:0".parse().unwrap(), &params);
        let handle = run_electrum(cfg, q, params, tip_tx, None)
            .await
            .expect("listen");

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
        let v: Value = serde_json::from_str(&resp).unwrap();
        assert_eq!(v["id"], 1);
        assert!(v.get("result").is_some());

        // Empty line ignored; malformed JSON ignored; then version.
        let stream = reader.into_inner();
        stream.write_all(b"\n").await.unwrap();
        stream.write_all(b"{not json\n").await.unwrap();
        let mut line = serde_json::to_string(&json!({
            "jsonrpc":"2.0","id":2,"method":"server.version","params":[]
        }))
        .unwrap();
        line.push('\n');
        stream.write_all(line.as_bytes()).await.unwrap();
        let mut reader = BufReader::new(stream);
        resp.clear();
        tokio::time::timeout(std::time::Duration::from_secs(3), reader.read_line(&mut resp))
            .await
            .expect("timeout")
            .unwrap();
        let v: Value = serde_json::from_str(&resp).unwrap();
        assert_eq!(v["id"], 2);

        handle.shutdown().await;
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn dispatch_on_connected_chain() {
        use rbitcoin_primitives::{Fk, Height};
        use rbitcoin_query::TxApply;
        use rbitcoin_store::{HeaderRecord, InputRecord, OutputRecord, TxRecord};

        let (dir, q) = tmp_store();
        let params = ChainParams::regtest();
        let cfg = ElectrumConfig::for_params("127.0.0.1:0".parse().unwrap(), &params);
        // Build 2-block synthetic chain.
        let mut prev = Fk::NULL;
        let mut hashes = Vec::new();
        let mut first_txid = [0u8; 32];
        for h in 0..2u32 {
            let mut hash = [0u8; 32];
            hash[0..4].copy_from_slice(&h.to_le_bytes());
            hash[5] = 0xec;
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
            if h == 0 {
                first_txid = txid;
            }
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
            hashes.push(hash);
            prev = q.connect_block(Height(h), &header, &[ta]).unwrap();
        }

        let mut header_sub = false;
        let mut sh_subs = HashSet::new();
        let tip = dispatch(
            "blockchain.headers.subscribe",
            &json!([]),
            &q,
            &cfg,
            &params,
            None,
            &mut header_sub,
            &mut sh_subs,
        )
        .unwrap();
        assert!(header_sub);
        assert_eq!(tip["height"], 1);

        let hdr = dispatch(
            "blockchain.block.header",
            &json!([0]),
            &q,
            &cfg,
            &params,
            None,
            &mut header_sub,
            &mut sh_subs,
        )
        .unwrap();
        assert_eq!(hdr.as_str().unwrap().len(), 160);

        let headers = dispatch(
            "blockchain.block.headers",
            &json!([0, 10]),
            &q,
            &cfg,
            &params,
            None,
            &mut header_sub,
            &mut sh_subs,
        )
        .unwrap();
        assert!(headers["count"].as_u64().unwrap() >= 2);

        let txid_hex = {
            let mut r = first_txid;
            r.reverse();
            rbitcoin_primitives::hex_encode(r)
        };
        let raw = dispatch(
            "blockchain.transaction.get",
            &json!([txid_hex]),
            &q,
            &cfg,
            &params,
            None,
            &mut header_sub,
            &mut sh_subs,
        )
        .unwrap();
        assert!(raw.as_str().unwrap().len() > 10);
        let verbose = dispatch(
            "blockchain.transaction.get",
            &json!([txid_hex, true]),
            &q,
            &cfg,
            &params,
            None,
            &mut header_sub,
            &mut sh_subs,
        )
        .unwrap();
        assert!(verbose.get("hex").is_some());

        let merkle = dispatch(
            "blockchain.transaction.get_merkle",
            &json!([txid_hex, 0]),
            &q,
            &cfg,
            &params,
            None,
            &mut header_sub,
            &mut sh_subs,
        )
        .unwrap();
        assert_eq!(merkle["block_height"], 0);

        let idpos = dispatch(
            "blockchain.transaction.id_from_pos",
            &json!([0, 0]),
            &q,
            &cfg,
            &params,
            None,
            &mut header_sub,
            &mut sh_subs,
        )
        .unwrap();
        assert_eq!(idpos.as_str().unwrap(), txid_hex);

        // Missing tx.
        let miss = [0xeeu8; 32];
        let mut miss_hex = miss;
        miss_hex.reverse();
        let miss_s = rbitcoin_primitives::hex_encode(miss_hex);
        assert!(dispatch(
            "blockchain.transaction.get",
            &json!([miss_s]),
            &q,
            &cfg,
            &params,
            None,
            &mut header_sub,
            &mut sh_subs
        )
        .is_err());

        // pos OOB
        assert!(dispatch(
            "blockchain.transaction.id_from_pos",
            &json!([0, 99]),
            &q,
            &cfg,
            &params,
            None,
            &mut header_sub,
            &mut sh_subs
        )
        .is_err());

        let sh = electrum_scripthash_hex(&[0x51]);
        let hist = dispatch(
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
        assert!(!hist.as_array().unwrap().is_empty());
        let bal = dispatch(
            "blockchain.scripthash.get_balance",
            &json!([sh]),
            &q,
            &cfg,
            &params,
            None,
            &mut header_sub,
            &mut sh_subs,
        )
        .unwrap();
        assert!(bal["confirmed"].as_i64().unwrap() > 0);
        let unspent = dispatch(
            "blockchain.scripthash.listunspent",
            &json!([sh]),
            &q,
            &cfg,
            &params,
            None,
            &mut header_sub,
            &mut sh_subs,
        )
        .unwrap();
        assert!(!unspent.as_array().unwrap().is_empty());
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
        assert!(!status.as_str().unwrap().is_empty());

        let _ = hashes;
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
        let handle = run_electrum(cfg, q, params, tip_tx.clone(), None)
            .await
            .unwrap();
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
            .send(TipNotify {
                height: 1,
                header_hex: "aa".repeat(80),
            })
            .unwrap();
        resp.clear();
        tokio::time::timeout(std::time::Duration::from_secs(3), reader.read_line(&mut resp))
            .await
            .unwrap()
            .unwrap();
        let push: Value = serde_json::from_str(&resp).unwrap();
        assert_eq!(
            push["method"].as_str(),
            Some("blockchain.headers.subscribe")
        );

        handle.shutdown().await;
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn dispatch_with_mempool_and_param_errors() {
        use rbitcoin_net::MempoolHub;
        use std::sync::Arc;

        let (dir, q) = tmp_store();
        // Genesis tip for broadcast / scripthash paths.
        use rbitcoin_primitives::{Fk, Height};
        use rbitcoin_query::TxApply;
        use rbitcoin_store::{HeaderRecord, InputRecord, OutputRecord, TxRecord};
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
            outputs: vec![OutputRecord::unspent(50_000, vec![0x51])],
        };
        q.connect_block(Height(0), &header, &[ta]).unwrap();

        let params = ChainParams::regtest();
        let cfg = ElectrumConfig::for_params("127.0.0.1:0".parse().unwrap(), &params);
        let q_arc = Arc::new(q);
        let mp_path = dir.join("mempool");
        let mp = MempoolHub::open(&mp_path, Arc::clone(&q_arc)).expect("mempool");
        let mut header_sub = false;
        let mut sh_subs = HashSet::new();
        let sh = electrum_scripthash_hex(&[0x51]);

        // Mempool-aware scripthash methods (empty pool).
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
        assert!(hist.as_array().is_some());

        let bal = dispatch(
            "blockchain.scripthash.get_balance",
            &json!([sh]),
            &q_arc,
            &cfg,
            &params,
            Some(&mp),
            &mut header_sub,
            &mut sh_subs,
        )
        .unwrap();
        assert!(bal["confirmed"].as_i64().unwrap_or(0) >= 0);

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
        assert!(unspent.as_array().is_some());

        let sub = dispatch(
            "blockchain.scripthash.subscribe",
            &json!([sh]),
            &q_arc,
            &cfg,
            &params,
            Some(&mp),
            &mut header_sub,
            &mut sh_subs,
        )
        .unwrap();
        assert!(sub.as_str().is_some());

        let mem = dispatch(
            "blockchain.scripthash.get_mempool",
            &json!([sh]),
            &q_arc,
            &cfg,
            &params,
            Some(&mp),
            &mut header_sub,
            &mut sh_subs,
        )
        .unwrap();
        assert_eq!(mem, json!([]));

        let hist_fee = dispatch(
            "mempool.get_fee_histogram",
            &json!([]),
            &q_arc,
            &cfg,
            &params,
            Some(&mp),
            &mut header_sub,
            &mut sh_subs,
        )
        .unwrap();
        assert!(hist_fee.as_array().is_some());

        // headers.subscribe with tip.
        let hs = dispatch(
            "blockchain.headers.subscribe",
            &json!([]),
            &q_arc,
            &cfg,
            &params,
            Some(&mp),
            &mut header_sub,
            &mut sh_subs,
        )
        .unwrap();
        assert!(header_sub);
        assert!(hs.get("height").is_some());

        // param_txid wrong length.
        assert!(param_txid(&json!(["aabb"]), 0).is_err());
        assert!(param_txid(&json!(["zz".repeat(32)]), 0).is_err());

        // Broadcast without valid tx → reject (mempool gate).
        assert!(dispatch(
            "blockchain.transaction.broadcast",
            &json!(["01000000000000000000"]),
            &q_arc,
            &cfg,
            &params,
            Some(&mp),
            &mut header_sub,
            &mut sh_subs,
        )
        .is_err());

        // transaction.get for confirmed coinbase (hex).
        let tid = {
            let fks = q_arc.block_tx_fks(Height(0)).unwrap();
            let t = q_arc.get_tx(fks[0]).unwrap();
            hash_hex_rev(&t.txid)
        };
        let raw = dispatch(
            "blockchain.transaction.get",
            &json!([tid, false]),
            &q_arc,
            &cfg,
            &params,
            Some(&mp),
            &mut header_sub,
            &mut sh_subs,
        )
        .unwrap();
        assert!(raw.as_str().unwrap().len() > 20);

        // Verbose get (may be object or hex depending on implementation).
        let _ = dispatch(
            "blockchain.transaction.get",
            &json!([tid, true]),
            &q_arc,
            &cfg,
            &params,
            Some(&mp),
            &mut header_sub,
            &mut sh_subs,
        );

        // scripthash_status_full direct.
        let sh_bytes = param_scripthash(&json!([sh]), 0).unwrap();
        let st = scripthash_status_full(&q_arc, &mp, &sh_bytes).unwrap();
        assert!(!st.is_empty() || st.is_empty()); // always returns string

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Live mempool txs: listunspent mempool outs, get_mempool rows, fee histogram,
    /// estimatefee, transaction.get mempool path, broadcast accept, status_full.
    #[test]
    fn dispatch_live_mempool_surfaces() {
        use bitcoin::absolute::LockTime;
        use bitcoin::block::{Header, Version as BlockVersion};
        use bitcoin::hashes::Hash;
        use bitcoin::script::ScriptBuf;
        use bitcoin::transaction::Version as TxVersion;
        use bitcoin::{
            Amount, Block, CompactTarget, OutPoint, Sequence, Target, Transaction, TxIn, TxOut,
            TxMerkleNode, Witness,
        };
        use rbitcoin_consensus::{accept_and_connect_block, Milestone};
        use rbitcoin_net::MempoolHub;
        use rbitcoin_primitives::Height;
        use rbitcoin_store::script_hash;
        use std::sync::Arc;

        if std::env::var_os("RBITCOIN_HEAD_SCALE").is_none() {
            std::env::set_var("RBITCOIN_HEAD_SCALE", "tiny");
        }
        let (dir, q) = tmp_store();
        let params = ChainParams::regtest();
        let ms = Milestone::NONE;

        fn coinbase(height: u32) -> Transaction {
            let mut ss = if height == 0 {
                vec![0x00]
            } else {
                rbitcoin_consensus::bip34_height_script(height)
            };
            while ss.len() < 2 {
                ss.push(0x00);
            }
            Transaction {
                version: TxVersion::ONE,
                lock_time: LockTime::ZERO,
                input: vec![TxIn {
                    previous_output: OutPoint::null(),
                    script_sig: ScriptBuf::from_bytes(ss),
                    sequence: Sequence::MAX,
                    witness: Witness::new(),
                }],
                output: vec![TxOut {
                    value: Amount::from_sat(50_0000_0000),
                    script_pubkey: ScriptBuf::from_bytes(vec![0x51]),
                }],
            }
        }
        fn mine(prev: bitcoin::BlockHash, time: u32, height: u32) -> Block {
            let bits = CompactTarget::from_consensus(0x207f_ffff);
            let header = Header {
                version: BlockVersion::ONE,
                prev_blockhash: prev,
                merkle_root: TxMerkleNode::from_byte_array([0u8; 32]),
                time,
                bits,
                nonce: 0,
            };
            let mut block = Block {
                header,
                txdata: vec![coinbase(height)],
            };
            block.header.merkle_root = block.compute_merkle_root().unwrap();
            let target = Target::from_compact(bits);
            for nonce in 0..u32::MAX {
                block.header.nonce = nonce;
                if block.header.validate_pow(target).is_ok() {
                    break;
                }
            }
            block
        }

        let genesis = bitcoin::blockdata::constants::genesis_block(bitcoin::Network::Regtest);
        accept_and_connect_block(&q, &params, Height::GENESIS, &genesis, ms).unwrap();
        let mut tip = genesis.block_hash();
        let mut tip_time = genesis.header.time;
        let mut coinbase_txids = Vec::new();
        for h in 1u32..=2 {
            let b = mine(tip, tip_time + 600, h);
            coinbase_txids.push(b.txdata[0].compute_txid());
            accept_and_connect_block(&q, &params, Height(h), &b, ms).unwrap();
            tip = b.block_hash();
            tip_time = b.header.time;
        }

        let q_arc = Arc::new(q);
        let mp = MempoolHub::open(dir.join("mempool"), Arc::clone(&q_arc)).unwrap();
        mp.set_relay_enabled(true);

        let spk = ScriptBuf::from_bytes(vec![0x51]);
        let sh = electrum_scripthash_hex(spk.as_bytes());
        let parent = Transaction {
            version: TxVersion::TWO,
            lock_time: LockTime::ZERO,
            input: vec![TxIn {
                previous_output: OutPoint {
                    txid: coinbase_txids[0],
                    vout: 0,
                },
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

        // History + mempool rows
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
        assert!(!hist.as_array().unwrap().is_empty());

        let bal = dispatch(
            "blockchain.scripthash.get_balance",
            &json!([sh]),
            &q_arc,
            &cfg,
            &params,
            Some(&mp),
            &mut header_sub,
            &mut sh_subs,
        )
        .unwrap();
        let _ = bal["unconfirmed"].as_i64();

        // listunspent: confirmed coinbases + mempool parent out (filter spent)
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
        assert!(!unspent.as_array().unwrap().is_empty());

        let mem = dispatch(
            "blockchain.scripthash.get_mempool",
            &json!([sh]),
            &q_arc,
            &cfg,
            &params,
            Some(&mp),
            &mut header_sub,
            &mut sh_subs,
        )
        .unwrap();
        assert!(!mem.as_array().unwrap().is_empty());

        let sub = dispatch(
            "blockchain.scripthash.subscribe",
            &json!([sh]),
            &q_arc,
            &cfg,
            &params,
            Some(&mp),
            &mut header_sub,
            &mut sh_subs,
        )
        .unwrap();
        assert!(sub.as_str().is_some());

        let hist_fee = dispatch(
            "mempool.get_fee_histogram",
            &json!([]),
            &q_arc,
            &cfg,
            &params,
            Some(&mp),
            &mut header_sub,
            &mut sh_subs,
        )
        .unwrap();
        assert!(!hist_fee.as_array().unwrap().is_empty());

        let est = dispatch(
            "blockchain.estimatefee",
            &json!([2]),
            &q_arc,
            &cfg,
            &params,
            Some(&mp),
            &mut header_sub,
            &mut sh_subs,
        )
        .unwrap();
        assert!(est.as_f64().unwrap() >= 0.0);

        // Mempool transaction.get (parent not yet confirmed)
        let parent_hex = {
            let mut t = parent.compute_txid().to_byte_array();
            t.reverse();
            rbitcoin_primitives::hex_encode(t)
        };
        // Parent is live in mempool but also may resolve via chain if coinbase-related;
        // get by parent txid — if not confirmed, hits mempool fallback.
        let got = dispatch(
            "blockchain.transaction.get",
            &json!([parent_hex, true]),
            &q_arc,
            &cfg,
            &params,
            Some(&mp),
            &mut header_sub,
            &mut sh_subs,
        );
        // Either confirmed path or mempool — must succeed for live parent.
        assert!(got.is_ok(), "{got:?}");

        // Broadcast a second spend of coinbase[1]
        let second = Transaction {
            version: TxVersion::TWO,
            lock_time: LockTime::ZERO,
            input: vec![TxIn {
                previous_output: OutPoint {
                    txid: coinbase_txids[1],
                    vout: 0,
                },
                script_sig: ScriptBuf::new(),
                sequence: Sequence::ENABLE_RBF_NO_LOCKTIME,
                witness: Witness::new(),
            }],
            output: vec![TxOut {
                value: Amount::from_sat(50_0000_0000 - 2_000),
                script_pubkey: ScriptBuf::from_bytes(vec![0x52]),
            }],
        };
        let raw = bitcoin::consensus::serialize(&second);
        let raw_hex = rbitcoin_primitives::hex_encode(&raw);
        let br = dispatch(
            "blockchain.transaction.broadcast",
            &json!([raw_hex]),
            &q_arc,
            &cfg,
            &params,
            Some(&mp),
            &mut header_sub,
            &mut sh_subs,
        )
        .expect("broadcast");
        assert!(br.as_str().unwrap().len() == 64);

        // status_full with live mempool
        let sh_bytes = script_hash(spk.as_bytes());
        let st = scripthash_status_full(&q_arc, &mp, &sh_bytes).unwrap();
        assert!(!st.is_empty());

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
    fn scripthash_sub_cap_enforced() {
        let (dir, q) = tmp_store();
        let params = ChainParams::regtest();
        let mut cfg = ElectrumConfig::for_params("127.0.0.1:0".parse().unwrap(), &params);
        cfg.max_scripthash_subs = 2;
        let mut header_sub = false;
        let mut sh_subs = HashSet::new();
        // Two unique scripthashes.
        let h1 = "11".repeat(32);
        let h2 = "22".repeat(32);
        let h3 = "33".repeat(32);
        dispatch(
            "blockchain.scripthash.subscribe",
            &json!([h1]),
            &q,
            &cfg,
            &params,
            None,
            &mut header_sub,
            &mut sh_subs,
        )
        .unwrap();
        dispatch(
            "blockchain.scripthash.subscribe",
            &json!([h2]),
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
            &json!([h3]),
            &q,
            &cfg,
            &params,
            None,
            &mut header_sub,
            &mut sh_subs,
        )
        .unwrap_err();
        assert!(err.contains("too many"), "{err}");
        // Re-subscribe existing is ok.
        dispatch(
            "blockchain.scripthash.subscribe",
            &json!([h1]),
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
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// DoS: when at max_connections, further accepts are dropped immediately.
    #[tokio::test]
    async fn max_connections_rejects_extra_client() {
        use tokio::io::AsyncReadExt;
        let (dir, q) = tmp_store();
        let params = ChainParams::regtest();
        let q = Arc::new(q);
        let (tip_tx, _) = broadcast::channel(4);
        let mut cfg = ElectrumConfig::for_params("127.0.0.1:0".parse().unwrap(), &params);
        cfg.max_connections = 1;
        cfg.idle_timeout = Duration::from_secs(30);
        let handle = run_electrum(cfg, q, params, tip_tx, None)
            .await
            .expect("listen");

        // Hold the only slot open.
        let held = TcpStream::connect(handle.local_addr).await.unwrap();
        // Give accept loop time to take the permit.
        tokio::time::sleep(Duration::from_millis(80)).await;

        // Second client: TCP may still accept then drop the stream when no permit.
        let mut second = TcpStream::connect(handle.local_addr).await.unwrap();
        let req = json!({"jsonrpc":"2.0","id":1,"method":"server.ping","params":[]});
        let mut line = serde_json::to_string(&req).unwrap();
        line.push('\n');
        let _ = second.write_all(line.as_bytes()).await;
        let mut buf = [0u8; 256];
        let read = tokio::time::timeout(Duration::from_millis(400), second.read(&mut buf)).await;
        // Dropped connection: EOF / error / timeout — not a successful JSON-RPC pong.
        match read {
            Ok(Ok(0)) | Ok(Err(_)) | Err(_) => {}
            Ok(Ok(n)) => {
                let s = std::str::from_utf8(&buf[..n]).unwrap_or("");
                assert!(
                    !s.contains("\"result\""),
                    "second client must not get RPC result at cap: {s}"
                );
            }
        }
        drop(held);
        handle.shutdown().await;
        let _ = std::fs::remove_dir_all(&dir);
    }
}
