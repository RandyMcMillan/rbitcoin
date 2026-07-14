//! Line-delimited JSON-RPC Electrum server (TCP).

use bitcoin::consensus::Encodable;
use bitcoin::hashes::Hash;
use rbitcoin_consensus::ChainParams;
use rbitcoin_primitives::Height;
use rbitcoin_query::Query;
use rbitcoin_store::script_hash;
use serde_json::{json, Value};
use std::collections::HashSet;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{broadcast, Notify};
use tokio::task::JoinHandle;

const PROTOCOL_MIN: &str = "1.4";
const PROTOCOL_MAX: &str = "1.4.2";
const SERVER_VERSION: &str = concat!("rbitcoin ", env!("CARGO_PKG_VERSION"));

#[derive(Clone, Debug)]
pub struct ElectrumConfig {
    pub listen: SocketAddr,
    pub banner: String,
    pub donation_address: String,
    /// Genesis hash (display order hex) for features.
    pub genesis_hash_hex: String,
}

impl ElectrumConfig {
    pub fn for_params(listen: SocketAddr, params: &ChainParams) -> Self {
        let genesis = params.genesis_hash.to_byte_array();
        // Electrum expects internal byte order reversed for display hex of hashes.
        let mut rev = genesis;
        rev.reverse();
        Self {
            listen,
            banner: "rbitcoin electrum (confirmed only)".into(),
            donation_address: String::new(),
            genesis_hash_hex: hex::encode(rev),
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

/// Start Electrum TCP listener. `tip_rx` optional for header push (may lag).
pub async fn run_electrum(
    config: ElectrumConfig,
    query: Arc<Query>,
    params: ChainParams,
    tip_tx: broadcast::Sender<TipNotify>,
) -> Result<ElectrumHandle, std::io::Error> {
    let listener = TcpListener::bind(config.listen).await?;
    let local_addr = listener.local_addr()?;
    let shutdown = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let shutdown_c = shutdown.clone();
    let config = Arc::new(config);
    let params = Arc::new(params);

    let task = tokio::spawn(async move {
        loop {
            if shutdown_c.load(std::sync::atomic::Ordering::SeqCst) {
                break;
            }
            let accept = tokio::time::timeout(
                std::time::Duration::from_millis(200),
                listener.accept(),
            )
            .await;
            match accept {
                Ok(Ok((stream, _))) => {
                    let q = query.clone();
                    let cfg = config.clone();
                    let p = params.clone();
                    let tip_rx = tip_tx.subscribe();
                    tokio::spawn(async move {
                        let _ = handle_client(stream, q, cfg, p, tip_rx).await;
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

async fn handle_client(
    stream: TcpStream,
    query: Arc<Query>,
    config: Arc<ElectrumConfig>,
    params: Arc<ChainParams>,
    mut tip_rx: broadcast::Receiver<TipNotify>,
) -> Result<(), std::io::Error> {
    let (reader, mut writer) = stream.into_split();
    let mut lines = BufReader::new(reader).lines();
    let mut header_sub = false;
    let mut sh_subs: HashSet<[u8; 32]> = HashSet::new();
    let notify = Arc::new(Notify::new());

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
            line = lines.next_line() => {
                let Some(line) = line? else { break; };
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

async fn write_line(
    writer: &mut tokio::net::tcp::OwnedWriteHalf,
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
            let hist = query.scripthash_history(&sh).map_err(|e| e.to_string())?;
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
            let b = query.scripthash_balance(&sh).map_err(|e| e.to_string())?;
            Ok(json!({"confirmed": b.confirmed, "unconfirmed": b.unconfirmed}))
        }
        "blockchain.scripthash.listunspent" => {
            let sh = param_scripthash(params, 0)?;
            let u = query.scripthash_listunspent(&sh).map_err(|e| e.to_string())?;
            let arr: Vec<Value> = u
                .iter()
                .map(|x| {
                    json!({
                        "tx_hash": txid_hex(&x.tx_hash),
                        "tx_pos": x.tx_pos,
                        "height": x.height,
                        "value": x.value,
                    })
                })
                .collect();
            Ok(Value::Array(arr))
        }
        "blockchain.scripthash.subscribe" => {
            let sh = param_scripthash(params, 0)?;
            sh_subs.insert(sh);
            // Status: hash of history status string — simplified empty or status hash
            let hist = query.scripthash_history(&sh).map_err(|e| e.to_string())?;
            Ok(json!(scripthash_status(&hist)))
        }
        "blockchain.scripthash.get_mempool" => Ok(json!([])),
        "blockchain.transaction.get" => {
            let txid = param_txid(params, 0)?;
            let verbose = params
                .as_array()
                .and_then(|a| a.get(1))
                .and_then(|v| v.as_bool())
                .unwrap_or(false);
            let (_fk, rec) = query
                .get_tx_by_txid(&txid)
                .map_err(|e| e.to_string())?
                .ok_or_else(|| "tx not found".to_string())?;
            if verbose {
                Ok(json!({"hex": hex::encode(&rec.raw), "txid": txid_hex(&txid)}))
            } else {
                Ok(json!(hex::encode(&rec.raw)))
            }
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
            // Best-effort: accept hex, return txid if we can decode; no mempool policy.
            let raw_hex = param_str(params, 0)?;
            let raw = hex::decode(raw_hex).map_err(|e| e.to_string())?;
            let tx: bitcoin::Transaction =
                bitcoin::consensus::deserialize(&raw).map_err(|e| e.to_string())?;
            let txid = tx.compute_txid();
            // Documented: no peer push wired in this slice unless net exposes broadcast later.
            let _ = chain.network;
            Ok(json!(format!("{txid}")))
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
        "blockchain.estimatefee" => Ok(json!(-1)),
        "blockchain.relayfee" => Ok(json!(0.00001)),
        "mempool.get_fee_histogram" => Ok(json!([])),
        // Also accept server.peers.subscribe empty
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
    hex::encode(buf)
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
    let mut bytes = hex::decode(s).map_err(|e| e.to_string())?;
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
    let mut bytes = hex::decode(s).map_err(|e| e.to_string())?;
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
    hex::encode(r)
}

fn scripthash_status(hist: &[rbitcoin_query::ScriptHashHistoryItem]) -> String {
    if hist.is_empty() {
        return String::new();
    }
    // Electrum status = sha256 of concatenation of "txid:height:" lines
    use bitcoin::hashes::{sha256, Hash as _};
    let mut s = String::new();
    for i in hist {
        s.push_str(&format!("{}:{}:", txid_hex(&i.txid), i.height));
    }
    let hash = sha256::Hash::hash(s.as_bytes());
    hex::encode(hash.to_byte_array())
}

/// Helper to compute electrum scripthash hex (reversed) from script bytes.
pub fn electrum_scripthash_hex(script: &[u8]) -> String {
    let h = script_hash(script);
    hash_hex_rev(&h)
}
