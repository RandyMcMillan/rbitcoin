//! Tier-1 Core-class JSON-RPC method handlers (pure dispatch over Query/mempool).

use bitcoin::consensus::{deserialize, encode::serialize_hex, Encodable};
use bitcoin::hashes::Hash;
use bitcoin::{Address, Network as BtcNetwork, ScriptBuf, Transaction};
use rbitcoin_net::MempoolHub;
use rbitcoin_primitives::{hex_decode, hex_encode, Height, Network};
use rbitcoin_query::Query;
use serde_json::{json, Value};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

/// Core / Electrum / Esplora **display order** hex for a 32-byte hash or txid.
///
/// Store and rust-bitcoin `to_byte_array()` use **internal** byte order; RPC
/// clients expect the reversed hex (same as `BlockHash`/`Txid` `Display`).
fn hash_hex_display(h: &[u8; 32]) -> String {
    let mut rev = *h;
    rev.reverse();
    hex_encode(rev)
}

/// Parse Core display-order 32-byte hex → internal byte order.
fn parse_hash32_display(hex: &str) -> Result<[u8; 32], Value> {
    let mut b = hex_decode(hex).map_err(|e| rpc_error(ERR_INVALID_PARAMS, e.to_string()))?;
    if b.len() != 32 {
        return Err(rpc_error(
            ERR_INVALID_PARAMS,
            "hash/txid must be 32 bytes hex",
        ));
    }
    b.reverse();
    let mut out = [0u8; 32];
    out.copy_from_slice(&b);
    Ok(out)
}

/// Shared process context for RPC handlers.
pub struct RpcContext {
    pub query: Arc<Query>,
    pub mempool: Option<Arc<MempoolHub>>,
    pub network: Network,
    pub start: Instant,
    pub stop: Arc<AtomicBool>,
    /// Best-effort live peer count (updated by node; 0 if unknown).
    pub connections: Arc<AtomicU64>,
    /// `true` while IBD catch-up is incomplete (node sets).
    pub initial_block_download: Arc<AtomicBool>,
}

impl RpcContext {
    pub fn uptime_secs(&self) -> u64 {
        self.start.elapsed().as_secs()
    }
}

/// JSON-RPC error object (Core-ish codes).
pub fn rpc_error(code: i64, message: impl Into<String>) -> Value {
    json!({ "code": code, "message": message.into() })
}

pub const ERR_MISC: i64 = -1;
pub const ERR_INVALID_PARAMS: i64 = -32602;
pub const ERR_METHOD_NOT_FOUND: i64 = -32601;
pub const ERR_INVALID_REQUEST: i64 = -32600;

/// Dispatch one method. Returns `Ok(result)` or `Err(error_object)`.
pub fn dispatch(ctx: &RpcContext, method: &str, params: &[Value]) -> Result<Value, Value> {
    match method {
        "help" => Ok(help(params)),
        "getrpcinfo" => Ok(getrpcinfo(ctx)),
        "uptime" => Ok(json!(ctx.uptime_secs())),
        "stop" => {
            ctx.stop.store(true, Ordering::SeqCst);
            Ok(json!("rbitcoin stopping"))
        }
        "getblockchaininfo" => getblockchaininfo(ctx),
        "getblockcount" => getblockcount(ctx),
        "getbestblockhash" => getbestblockhash(ctx),
        "getblockhash" => getblockhash(ctx, params),
        "getblockheader" => getblockheader(ctx, params),
        "getblock" => getblock(ctx, params),
        "getdifficulty" => getdifficulty(ctx),
        "getnetworkinfo" => Ok(getnetworkinfo(ctx)),
        "getconnectioncount" => Ok(json!(ctx.connections.load(Ordering::Relaxed))),
        "getpeerinfo" => Ok(json!([])), // best-effort stub; peer detail not exposed yet
        "getmempoolinfo" => getmempoolinfo(ctx),
        "getrawmempool" => getrawmempool(ctx, params),
        "getmempoolentry" => getmempoolentry(ctx, params),
        "getrawtransaction" => getrawtransaction(ctx, params),
        "sendrawtransaction" => sendrawtransaction(ctx, params),
        "testmempoolaccept" => testmempoolaccept(ctx, params),
        "decoderawtransaction" => decoderawtransaction(params),
        "decodescript" => decodescript(params),
        "validateaddress" => validateaddress(ctx, params),
        "estimatesmartfee" => estimatesmartfee(ctx, params),
        "createrawtransaction"
        | "combinerawtransaction"
        | "scantxoutset"
        | "getblocktemplate"
        | "submitblock"
        | "generatetoaddress"
        | "gettxoutsetinfo" => Err(rpc_error(
            ERR_METHOD_NOT_FOUND,
            format!("{method} is not supported (see docs/rpc.md)"),
        )),
        _ => Err(rpc_error(
            ERR_METHOD_NOT_FOUND,
            format!("Method not found: {method}"),
        )),
    }
}

fn help(params: &[Value]) -> Value {
    if let Some(Value::String(m)) = params.first() {
        return json!(method_help(m));
    }
    json!(METHOD_LIST.join("\n"))
}

const METHOD_LIST: &[&str] = &[
    "help",
    "getrpcinfo",
    "uptime",
    "stop",
    "getblockchaininfo",
    "getblockcount",
    "getbestblockhash",
    "getblockhash",
    "getblockheader",
    "getblock",
    "getdifficulty",
    "getnetworkinfo",
    "getconnectioncount",
    "getpeerinfo",
    "getmempoolinfo",
    "getrawmempool",
    "getmempoolentry",
    "getrawtransaction",
    "sendrawtransaction",
    "testmempoolaccept",
    "decoderawtransaction",
    "decodescript",
    "validateaddress",
    "estimatesmartfee",
];

fn method_help(m: &str) -> String {
    match m {
        "estimatesmartfee" => {
            "estimatesmartfee conf_target (mode ignored). Returns this node's 10-minute \
             inclusion frontier feerate (BTC/kvB), not Core historical multi-horizon. \
             See docs/mempool-fee-estimation.md."
                .into()
        }
        "getblockchaininfo" => "getblockchaininfo — tip height, chain, IBD flag.".into(),
        "help" => "help ( \"command\" ) — list methods or describe one.".into(),
        other if METHOD_LIST.contains(&other) => format!("{other} — see docs/rpc.md"),
        other => format!("unknown method {other}"),
    }
}

fn getrpcinfo(ctx: &RpcContext) -> Value {
    json!({
        "active_commands": [],
        "logpath": "",
        "uptime": ctx.uptime_secs(),
        "methods": METHOD_LIST,
    })
}

fn chain_name(n: Network) -> &'static str {
    match n {
        Network::Mainnet => "main",
        Network::Testnet => "test",
        Network::Signet => "signet",
        Network::Regtest => "regtest",
    }
}

fn getblockcount(ctx: &RpcContext) -> Result<Value, Value> {
    let h = ctx.query.tip_height().map(|h| h.0).unwrap_or(0);
    Ok(json!(h))
}

fn getbestblockhash(ctx: &RpcContext) -> Result<Value, Value> {
    let Some(tip) = ctx.query.tip_height() else {
        return Err(rpc_error(ERR_MISC, "no tip"));
    };
    let (_, rec) = ctx
        .query
        .header_at_height(tip)
        .map_err(|e| rpc_error(ERR_MISC, e.to_string()))?
        .ok_or_else(|| rpc_error(ERR_MISC, "tip header missing"))?;
    Ok(json!(hash_hex_display(&rec.hash)))
}

fn getblockhash(ctx: &RpcContext, params: &[Value]) -> Result<Value, Value> {
    let height = params
        .first()
        .and_then(|v| v.as_u64())
        .ok_or_else(|| rpc_error(ERR_INVALID_PARAMS, "height required"))? as u32;
    let (_, rec) = ctx
        .query
        .header_at_height(Height(height))
        .map_err(|e| rpc_error(ERR_MISC, e.to_string()))?
        .ok_or_else(|| rpc_error(ERR_MISC, "Block height out of range"))?;
    Ok(json!(hash_hex_display(&rec.hash)))
}

fn getblockchaininfo(ctx: &RpcContext) -> Result<Value, Value> {
    let tip = ctx.query.tip_height().map(|h| h.0).unwrap_or(0);
    let best = if let Some(h) = ctx.query.tip_height() {
        ctx.query
            .header_at_height(h)
            .ok()
            .flatten()
            .map(|(_, r)| hash_hex_display(&r.hash))
            .unwrap_or_default()
    } else {
        String::new()
    };
    let ibd = ctx.initial_block_download.load(Ordering::Relaxed);
    Ok(json!({
        "chain": chain_name(ctx.network),
        "blocks": tip,
        "headers": tip,
        "bestblockhash": best,
        "difficulty": difficulty_at_tip(ctx).unwrap_or(0.0),
        "verificationprogress": if ibd { 0.5 } else { 1.0 },
        "initialblockdownload": ibd,
        "chainwork": "",
        "size_on_disk": 0,
        "pruned": false,
        "warnings": "",
    }))
}

fn difficulty_from_bits(bits: u32) -> f64 {
    // Compact target → difficulty relative to max target (same class as Core).
    let n_shift = ((bits >> 24) & 0xff) as i32;
    let mut ddiff = (0x0000_ffff_u64 as f64) / ((bits & 0x00ff_ffff) as f64);
    let mut shift = n_shift - 29;
    while shift < 0 {
        ddiff *= 256.0;
        shift += 1;
    }
    while shift > 0 {
        ddiff /= 256.0;
        shift -= 1;
    }
    ddiff
}

fn difficulty_at_tip(ctx: &RpcContext) -> Result<f64, Value> {
    let tip = ctx
        .query
        .tip_height()
        .ok_or_else(|| rpc_error(ERR_MISC, "no tip"))?;
    let (_, rec) = ctx
        .query
        .header_at_height(tip)
        .map_err(|e| rpc_error(ERR_MISC, e.to_string()))?
        .ok_or_else(|| rpc_error(ERR_MISC, "tip header missing"))?;
    Ok(difficulty_from_bits(rec.bits))
}

fn getdifficulty(ctx: &RpcContext) -> Result<Value, Value> {
    Ok(json!(difficulty_at_tip(ctx)?))
}

fn getblockheader(ctx: &RpcContext, params: &[Value]) -> Result<Value, Value> {
    let hash_hex = params
        .first()
        .and_then(|v| v.as_str())
        .ok_or_else(|| rpc_error(ERR_INVALID_PARAMS, "blockhash required"))?;
    let verbose = params.get(1).and_then(|v| v.as_bool()).unwrap_or(true);
    let hash = parse_hash32_display(hash_hex)?;
    let height = ctx
        .query
        .height_of_hash(&hash)
        .map_err(|e| rpc_error(ERR_MISC, e.to_string()))?
        .ok_or_else(|| rpc_error(ERR_MISC, "Block not found"))?;
    let (_, rec) = ctx
        .query
        .header_at_height(height)
        .map_err(|e| rpc_error(ERR_MISC, e.to_string()))?
        .ok_or_else(|| rpc_error(ERR_MISC, "Block not found"))?;
    if !verbose {
        let hdr = ctx
            .query
            .wire_header_at_height(height)
            .map_err(|e| rpc_error(ERR_MISC, e.to_string()))?;
        let mut raw = Vec::new();
        hdr.consensus_encode(&mut raw)
            .map_err(|_| rpc_error(ERR_MISC, "header encode"))?;
        return Ok(json!(hex_encode(raw)));
    }
    let prev = if height.0 > 0 {
        ctx.query
            .header_at_height(Height(height.0 - 1))
            .ok()
            .flatten()
            .map(|(_, r)| hash_hex_display(&r.hash))
            .unwrap_or_default()
    } else {
        String::new()
    };
    Ok(json!({
        "hash": hash_hex_display(&rec.hash),
        "confirmations": confirmations(ctx, height),
        "height": height.0,
        "version": rec.version,
        "versionHex": format!("{:08x}", rec.version),
        "merkleroot": hash_hex_display(&rec.merkle_root),
        "time": rec.timestamp,
        "mediantime": rec.timestamp,
        "nonce": rec.nonce,
        "bits": format!("{:08x}", rec.bits),
        "difficulty": difficulty_from_bits(rec.bits),
        "previousblockhash": prev,
        "nTx": ctx.query.block_tx_fks(height).map(|v| v.len()).unwrap_or(0),
    }))
}

fn getblock(ctx: &RpcContext, params: &[Value]) -> Result<Value, Value> {
    let hash_hex = params
        .first()
        .and_then(|v| v.as_str())
        .ok_or_else(|| rpc_error(ERR_INVALID_PARAMS, "blockhash required"))?;
    let verbosity = params.get(1).and_then(|v| v.as_u64()).unwrap_or(1) as u32;
    let hash = parse_hash32_display(hash_hex)?;
    let height = ctx
        .query
        .height_of_hash(&hash)
        .map_err(|e| rpc_error(ERR_MISC, e.to_string()))?
        .ok_or_else(|| rpc_error(ERR_MISC, "Block not found"))?;
    let block = ctx
        .query
        .reconstruct_block_at_height(height)
        .map_err(|e| rpc_error(ERR_MISC, e.to_string()))?;
    if verbosity == 0 {
        let mut raw = Vec::new();
        block
            .consensus_encode(&mut raw)
            .map_err(|_| rpc_error(ERR_MISC, "block encode"))?;
        return Ok(json!(hex_encode(raw)));
    }
    let txids: Vec<String> = block
        .txdata
        .iter()
        .map(|tx| hash_hex_display(&tx.compute_txid().to_byte_array()))
        .collect();
    let mut obj = json!({
        "hash": hash_hex_display(&hash),
        "confirmations": confirmations(ctx, height),
        "height": height.0,
        "version": block.header.version.to_consensus(),
        "merkleroot": hash_hex_display(&block.header.merkle_root.to_byte_array()),
        "time": block.header.time,
        "nonce": block.header.nonce,
        "bits": format!("{:08x}", block.header.bits.to_consensus()),
        "nTx": block.txdata.len(),
        "tx": txids,
    });
    if verbosity >= 2 {
        let txs: Vec<Value> = block.txdata.iter().map(|tx| tx_to_json(tx, None)).collect();
        obj["tx"] = json!(txs);
    }
    Ok(obj)
}

fn confirmations(ctx: &RpcContext, height: Height) -> u32 {
    let tip = ctx.query.tip_height().map(|h| h.0).unwrap_or(0);
    tip.saturating_sub(height.0).saturating_add(1)
}

fn getnetworkinfo(ctx: &RpcContext) -> Value {
    json!({
        "version": 270000,
        "subversion": format!("/rbitcoin:{}/", env!("CARGO_PKG_VERSION")),
        "protocolversion": 70016,
        "localservices": "0000000000000409",
        "localservicesnames": ["NETWORK", "WITNESS", "NETWORK_LIMITED", "P2P_V2"],
        "localrelay": true,
        "timeoffset": 0,
        "networkactive": true,
        "connections": ctx.connections.load(Ordering::Relaxed),
        "connections_in": 0,
        "connections_out": ctx.connections.load(Ordering::Relaxed),
        "networks": [],
        "relayfee": MempoolHub::relay_fee_btc_per_kb(),
        "incrementalfee": MempoolHub::relay_fee_btc_per_kb(),
        "localaddresses": [],
        "warnings": "BIP324 v2-only; not full Core networkinfo parity",
    })
}

fn getmempoolinfo(ctx: &RpcContext) -> Result<Value, Value> {
    let Some(mp) = ctx.mempool.as_ref() else {
        return Ok(json!({
            "loaded": false,
            "size": 0,
            "bytes": 0,
            "usage": 0,
            "total_fee": 0.0,
            "maxmempool": 0,
            "mempoolminfee": MempoolHub::relay_fee_btc_per_kb(),
            "minrelaytxfee": MempoolHub::relay_fee_btc_per_kb(),
        }));
    };
    let live = mp.list_live_meta();
    let size = live.len();
    let mut bytes = 0u64;
    let mut total_fee = 0u64;
    for (_, fee, weight) in &live {
        bytes += weight / 4;
        total_fee += fee;
    }
    Ok(json!({
        "loaded": true,
        "size": size,
        "bytes": bytes,
        "usage": bytes,
        "total_fee": (total_fee as f64) / 100_000_000.0,
        "maxmempool": 300_000_000u64,
        "mempoolminfee": MempoolHub::relay_fee_btc_per_kb(),
        "minrelaytxfee": MempoolHub::relay_fee_btc_per_kb(),
        "relay_enabled": mp.relay_enabled(),
    }))
}

fn getrawmempool(ctx: &RpcContext, params: &[Value]) -> Result<Value, Value> {
    let verbose = params.first().and_then(|v| v.as_bool()).unwrap_or(false);
    let Some(mp) = ctx.mempool.as_ref() else {
        return Ok(if verbose { json!({}) } else { json!([]) });
    };
    let live = mp.list_live_meta();
    if !verbose {
        let ids: Vec<String> = live
            .iter()
            .map(|(t, _, _)| hash_hex_display(&t.to_byte_array()))
            .collect();
        return Ok(json!(ids));
    }
    let mut map = serde_json::Map::new();
    for (txid, fee, weight) in live {
        let vsize = weight / 4;
        map.insert(
            hash_hex_display(&txid.to_byte_array()),
            json!({
                "vsize": vsize,
                "weight": weight,
                "fee": (fee as f64) / 100_000_000.0,
                "modifiedfee": (fee as f64) / 100_000_000.0,
                "time": 0,
                "height": 0,
                "descendantcount": 1,
                "descendantsize": vsize,
                "descendantfees": fee,
                "ancestorcount": 1,
                "ancestorsize": vsize,
                "ancestorfees": fee,
            }),
        );
    }
    Ok(Value::Object(map))
}

fn getmempoolentry(ctx: &RpcContext, params: &[Value]) -> Result<Value, Value> {
    let hex = params
        .first()
        .and_then(|v| v.as_str())
        .ok_or_else(|| rpc_error(ERR_INVALID_PARAMS, "txid required"))?;
    let want = parse_hash32_display(hex)?;
    let mp = ctx
        .mempool
        .as_ref()
        .ok_or_else(|| rpc_error(ERR_MISC, "mempool not available"))?;
    for (txid, fee, weight) in mp.list_live_meta() {
        if txid.to_byte_array() == want {
            let vsize = weight / 4;
            return Ok(json!({
                "vsize": vsize,
                "weight": weight,
                "fee": (fee as f64) / 100_000_000.0,
                "modifiedfee": (fee as f64) / 100_000_000.0,
            }));
        }
    }
    Err(rpc_error(ERR_MISC, "Transaction not in mempool"))
}

fn getrawtransaction(ctx: &RpcContext, params: &[Value]) -> Result<Value, Value> {
    let hex = params
        .first()
        .and_then(|v| v.as_str())
        .ok_or_else(|| rpc_error(ERR_INVALID_PARAMS, "txid required"))?;
    let verbose = params.get(1).and_then(|v| v.as_bool()).unwrap_or(false);
    let want = parse_hash32_display(hex)?;

    if let Some(mp) = ctx.mempool.as_ref() {
        for (txid, _fee, _w, tx) in mp.list_live() {
            if txid.to_byte_array() == want {
                if !verbose {
                    return Ok(json!(serialize_hex(&tx)));
                }
                return Ok(tx_to_json(&tx, Some(json!({ "in_mempool": true }))));
            }
        }
    }

    let (fk, _) = ctx
        .query
        .get_tx_by_txid(&want)
        .map_err(|e| rpc_error(ERR_MISC, e.to_string()))?
        .ok_or_else(|| rpc_error(ERR_MISC, "No such mempool or blockchain transaction"))?;
    let tx = ctx
        .query
        .reconstruct_tx(fk)
        .map_err(|e| rpc_error(ERR_MISC, e.to_string()))?;
    if !verbose {
        return Ok(json!(serialize_hex(&tx)));
    }
    Ok(tx_to_json(&tx, Some(json!({ "in_mempool": false }))))
}

fn sendrawtransaction(ctx: &RpcContext, params: &[Value]) -> Result<Value, Value> {
    let hex = params
        .first()
        .and_then(|v| v.as_str())
        .ok_or_else(|| rpc_error(ERR_INVALID_PARAMS, "hexstring required"))?;
    let tx = decode_tx_hex(hex)?;
    let mp = ctx
        .mempool
        .as_ref()
        .ok_or_else(|| rpc_error(ERR_MISC, "mempool not available"))?;
    if !mp.relay_enabled() {
        return Err(rpc_error(
            ERR_MISC,
            "mempool relay disabled (still in IBD or tip not ready)",
        ));
    }
    match mp.accept_tx(&tx) {
        Ok(_) => Ok(json!(hash_hex_display(&tx.compute_txid().to_byte_array()))),
        Err(e) => Err(rpc_error(ERR_MISC, e.to_string())),
    }
}

fn testmempoolaccept(ctx: &RpcContext, params: &[Value]) -> Result<Value, Value> {
    let arr = params
        .first()
        .and_then(|v| v.as_array())
        .ok_or_else(|| rpc_error(ERR_INVALID_PARAMS, "rawtxs array required"))?;
    let mp = ctx
        .mempool
        .as_ref()
        .ok_or_else(|| rpc_error(ERR_MISC, "mempool not available"))?;
    let mut out = Vec::new();
    for v in arr {
        let hex = v
            .as_str()
            .ok_or_else(|| rpc_error(ERR_INVALID_PARAMS, "rawtx hex required"))?;
        let tx = decode_tx_hex(hex)?;
        let txid = hash_hex_display(&tx.compute_txid().to_byte_array());
        // Dry-run: accept then remove if we admitted (best-effort). Prefer not
        // mutating — use accept and if ok, remove_for_block to roll back.
        match mp.accept_tx(&tx) {
            Ok(_) => {
                let _ = mp.remove_for_block(&[tx.compute_txid()]);
                out.push(json!({
                    "txid": txid,
                    "allowed": true,
                }));
            }
            Err(e) => {
                out.push(json!({
                    "txid": txid,
                    "allowed": false,
                    "reject-reason": e.to_string(),
                }));
            }
        }
    }
    Ok(json!(out))
}

fn decoderawtransaction(params: &[Value]) -> Result<Value, Value> {
    let hex = params
        .first()
        .and_then(|v| v.as_str())
        .ok_or_else(|| rpc_error(ERR_INVALID_PARAMS, "hexstring required"))?;
    let tx = decode_tx_hex(hex)?;
    Ok(tx_to_json(&tx, None))
}

fn decodescript(params: &[Value]) -> Result<Value, Value> {
    let hex = params
        .first()
        .and_then(|v| v.as_str())
        .ok_or_else(|| rpc_error(ERR_INVALID_PARAMS, "hexstring required"))?;
    let bytes = hex_decode(hex).map_err(|e| rpc_error(ERR_INVALID_PARAMS, e.to_string()))?;
    let script = ScriptBuf::from_bytes(bytes);
    let asm = script.to_asm_string();
    Ok(json!({
        "asm": asm,
        "hex": hex_encode(script.as_bytes()),
        "type": "nonstandard",
    }))
}

fn validateaddress(ctx: &RpcContext, params: &[Value]) -> Result<Value, Value> {
    let addr_s = params
        .first()
        .and_then(|v| v.as_str())
        .ok_or_else(|| rpc_error(ERR_INVALID_PARAMS, "address required"))?;
    let btc_net = match ctx.network {
        Network::Mainnet => BtcNetwork::Bitcoin,
        Network::Testnet => BtcNetwork::Testnet,
        Network::Signet => BtcNetwork::Signet,
        Network::Regtest => BtcNetwork::Regtest,
    };
    match addr_s.parse::<Address<_>>() {
        Ok(a) => {
            let checked = a.require_network(btc_net);
            match checked {
                Ok(addr) => Ok(json!({
                    "isvalid": true,
                    "address": addr.to_string(),
                    "scriptPubKey": hex_encode(addr.script_pubkey().as_bytes()),
                    "isscript": addr.script_pubkey().is_p2sh()
                        || addr.script_pubkey().is_p2wsh()
                        || addr.script_pubkey().is_p2tr(),
                    "iswitness": addr.script_pubkey().is_p2wpkh()
                        || addr.script_pubkey().is_p2wsh()
                        || addr.script_pubkey().is_p2tr(),
                })),
                Err(_) => Ok(json!({ "isvalid": false })),
            }
        }
        Err(_) => Ok(json!({ "isvalid": false })),
    }
}

/// Map Core `estimatesmartfee` to this node's **10-minute inclusion** product.
fn estimatesmartfee(ctx: &RpcContext, params: &[Value]) -> Result<Value, Value> {
    let conf_target = params.first().and_then(|v| v.as_u64()).unwrap_or(2) as u32;
    // Core conf_target is blocks; we map 1–2 (and default) to 10-minute horizon.
    let Some(mp) = ctx.mempool.as_ref() else {
        return Ok(json!({
            "feerate": -1.0,
            "errors": ["mempool unavailable"],
            "blocks": conf_target,
        }));
    };
    let rate = mp.estimate_fee_btc_per_kb(conf_target);
    if rate < 0.0 {
        return Ok(json!({
            "feerate": -1.0,
            "errors": ["Insufficient data or empty mempool"],
            "blocks": conf_target.max(1),
        }));
    }
    Ok(json!({
        "feerate": rate,
        "blocks": conf_target.max(1),
        "errors": Value::Null,
        // Non-Core field: document the product mapping.
        "rbitcoin_model": "10-minute inclusion frontier (not Core historical)",
    }))
}

fn decode_tx_hex(hex: &str) -> Result<Transaction, Value> {
    let b = hex_decode(hex).map_err(|e| rpc_error(ERR_INVALID_PARAMS, e.to_string()))?;
    deserialize(&b).map_err(|e| rpc_error(ERR_INVALID_PARAMS, format!("tx decode: {e}")))
}

fn tx_to_json(tx: &Transaction, extra: Option<Value>) -> Value {
    let txid = hash_hex_display(&tx.compute_txid().to_byte_array());
    let mut vin = Vec::new();
    for (i, inp) in tx.input.iter().enumerate() {
        vin.push(json!({
            "txid": hash_hex_display(&inp.previous_output.txid.to_byte_array()),
            "vout": inp.previous_output.vout,
            "sequence": inp.sequence.to_consensus_u32(),
            "n": i,
        }));
    }
    let mut vout = Vec::new();
    for (i, out) in tx.output.iter().enumerate() {
        vout.push(json!({
            "value": out.value.to_btc(),
            "n": i,
            "scriptPubKey": {
                "hex": hex_encode(out.script_pubkey.as_bytes()),
                "asm": out.script_pubkey.to_asm_string(),
            }
        }));
    }
    let mut obj = json!({
        "txid": txid,
        "hash": hash_hex_display(&tx.compute_wtxid().to_byte_array()),
        "version": tx.version.0,
        "size": tx.total_size(),
        "vsize": tx.vsize(),
        "weight": tx.weight().to_wu(),
        "locktime": tx.lock_time.to_consensus_u32(),
        "vin": vin,
        "vout": vout,
        "hex": serialize_hex(tx),
    });
    if let Some(Value::Object(m)) = extra {
        if let Some(o) = obj.as_object_mut() {
            for (k, v) in m {
                o.insert(k, v);
            }
        }
    }
    obj
}

/// Build a JSON-RPC response object for a single request.
pub fn handle_request(ctx: &RpcContext, body: &Value) -> Value {
    let id = body.get("id").cloned().unwrap_or(Value::Null);
    let method = match body.get("method").and_then(|m| m.as_str()) {
        Some(m) => m,
        None => {
            return json!({
                "result": null,
                "error": rpc_error(ERR_INVALID_REQUEST, "missing method"),
                "id": id,
            });
        }
    };
    let params: Vec<Value> = match body.get("params") {
        None | Some(Value::Null) => Vec::new(),
        Some(Value::Array(a)) => a.clone(),
        Some(Value::Object(_)) => {
            // Core sometimes uses named params; we require positional for v1.
            return json!({
                "result": null,
                "error": rpc_error(ERR_INVALID_PARAMS, "named params not supported; use array"),
                "id": id,
            });
        }
        Some(_) => {
            return json!({
                "result": null,
                "error": rpc_error(ERR_INVALID_PARAMS, "params must be array"),
                "id": id,
            });
        }
    };
    match dispatch(ctx, method, &params) {
        Ok(result) => json!({ "result": result, "error": null, "id": id }),
        Err(error) => json!({ "result": null, "error": error, "id": id }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rbitcoin_primitives::Network;
    use std::path::PathBuf;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    fn ctx_empty() -> (RpcContext, PathBuf) {
        let n = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("rbitcoin-rpc-meth-{n}"));
        std::fs::create_dir_all(&dir).unwrap();
        let q = Arc::new(Query::open_or_create(dir.join("store")).unwrap());
        let mp =
            MempoolHub::open_with_weight(dir.join("mempool"), Arc::clone(&q), 300_000_000).unwrap();
        mp.set_relay_enabled(true);
        let ctx = RpcContext {
            query: q,
            mempool: Some(mp),
            network: Network::Regtest,
            start: Instant::now() - Duration::from_secs(42),
            stop: Arc::new(AtomicBool::new(false)),
            connections: Arc::new(AtomicU64::new(2)),
            initial_block_download: Arc::new(AtomicBool::new(false)),
        };
        (ctx, dir)
    }

    #[test]
    fn help_and_getrpcinfo_list_methods() {
        let (ctx, dir) = ctx_empty();
        let help_all = dispatch(&ctx, "help", &[]).unwrap();
        let s = help_all.as_str().unwrap();
        assert!(s.contains("getblockchaininfo"));
        assert!(s.contains("estimatesmartfee"));
        let info = dispatch(&ctx, "getrpcinfo", &[]).unwrap();
        assert!(info["methods"].as_array().unwrap().len() >= 10);
        assert!(info["uptime"].as_u64().unwrap() >= 42);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn blockchain_empty_store() {
        let (ctx, dir) = ctx_empty();
        let count = dispatch(&ctx, "getblockcount", &[]).unwrap();
        assert_eq!(count, json!(0));
        let info = dispatch(&ctx, "getblockchaininfo", &[]).unwrap();
        assert_eq!(info["chain"], "regtest");
        assert_eq!(info["blocks"], 0);
        assert_eq!(info["initialblockdownload"], false);
        let mem = dispatch(&ctx, "getmempoolinfo", &[]).unwrap();
        assert_eq!(mem["size"], 0);
        let raw = dispatch(&ctx, "getrawmempool", &[]).unwrap();
        assert_eq!(raw, json!([]));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn estimatesmartfee_maps_to_product() {
        let (ctx, dir) = ctx_empty();
        let r = dispatch(&ctx, "estimatesmartfee", &[json!(2)]).unwrap();
        // Empty mempool → negative feerate with errors.
        assert!(r["feerate"].as_f64().unwrap() < 0.0 || r.get("rbitcoin_model").is_some());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn stop_sets_flag() {
        let (ctx, dir) = ctx_empty();
        assert!(!ctx.stop.load(Ordering::SeqCst));
        dispatch(&ctx, "stop", &[]).unwrap();
        assert!(ctx.stop.load(Ordering::SeqCst));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn unsupported_methods_error() {
        let (ctx, dir) = ctx_empty();
        let e = dispatch(&ctx, "scantxoutset", &[]).unwrap_err();
        assert_eq!(e["code"], ERR_METHOD_NOT_FOUND);
        let e2 = dispatch(&ctx, "createrawtransaction", &[]).unwrap_err();
        assert_eq!(e2["code"], ERR_METHOD_NOT_FOUND);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn handle_request_roundtrip() {
        let (ctx, dir) = ctx_empty();
        let body = json!({"jsonrpc":"1.0","id":"t1","method":"getblockcount","params":[]});
        let resp = handle_request(&ctx, &body);
        assert_eq!(resp["id"], "t1");
        assert!(resp["error"].is_null());
        assert_eq!(resp["result"], 0);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn decoderawtransaction_coinbase_like() {
        // Minimal empty-vin is invalid; use a trivial valid bare tx if available.
        // Just ensure invalid hex errors.
        let (ctx, dir) = ctx_empty();
        let e = dispatch(&ctx, "decoderawtransaction", &[json!("00")]).unwrap_err();
        assert_eq!(e["code"], ERR_INVALID_PARAMS);
        let _ = ctx;
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn validateaddress_regtest() {
        let (ctx, dir) = ctx_empty();
        // Invalid
        let r = dispatch(&ctx, "validateaddress", &[json!("notanaddr")]).unwrap();
        assert_eq!(r["isvalid"], false);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn hash_hex_display_matches_blockhash_display_and_reverses_parse() {
        // Fixed non-palindrome internal bytes.
        let mut internal = [0u8; 32];
        for (i, b) in internal.iter_mut().enumerate() {
            *b = i as u8;
        }
        let disp = hash_hex_display(&internal);
        let via_type = bitcoin::BlockHash::from_byte_array(internal).to_string();
        assert_eq!(disp, via_type);
        let back = parse_hash32_display(&disp).unwrap();
        assert_eq!(back, internal);
        // Raw internal hex is not equal to display.
        assert_ne!(disp, rbitcoin_primitives::hex_encode(internal));
    }

    #[test]
    fn all_methods_callable_empty_or_error() {
        let (ctx, dir) = ctx_empty();
        // Control / network always succeed on empty store.
        for m in [
            "uptime",
            "getnetworkinfo",
            "getconnectioncount",
            "getpeerinfo",
            "getmempoolinfo",
            "getrawmempool",
        ] {
            let _ = dispatch(&ctx, m, &[]).expect(m);
        }
        // Empty store: no tip → difficulty errors.
        let _ = dispatch(&ctx, "getdifficulty", &[]);
        let _ = dispatch(&ctx, "getrawmempool", &[json!(true)]).unwrap();
        let _ = dispatch(&ctx, "help", &[json!("estimatesmartfee")]).unwrap();
        let _ = dispatch(&ctx, "help", &[json!("getblockchaininfo")]).unwrap();
        let _ = dispatch(&ctx, "help", &[json!("help")]).unwrap();
        let _ = dispatch(&ctx, "help", &[json!("unknown_method_xyz")]).unwrap();
        // Expected errors (missing params / missing blocks).
        for (m, params) in [
            ("getblockhash", vec![]),
            ("getblockhash", vec![json!(99)]),
            ("getbestblockhash", vec![]),
            ("getblockheader", vec![]),
            ("getblockheader", vec![json!("00".repeat(32))]),
            ("getblock", vec![]),
            ("getblock", vec![json!("00".repeat(32))]),
            ("getrawtransaction", vec![]),
            ("getrawtransaction", vec![json!("00".repeat(32))]),
            ("getmempoolentry", vec![]),
            ("getmempoolentry", vec![json!("00".repeat(32))]),
            ("sendrawtransaction", vec![]),
            ("sendrawtransaction", vec![json!("00")]),
            ("testmempoolaccept", vec![]),
            ("decoderawtransaction", vec![]),
            ("decodescript", vec![]),
            ("validateaddress", vec![]),
            ("nosuchmethod", vec![]),
            ("getblocktemplate", vec![]),
            ("combinerawtransaction", vec![]),
            ("generatetoaddress", vec![]),
            ("gettxoutsetinfo", vec![]),
        ] {
            let _ = dispatch(&ctx, m, &params);
        }
        // decodescript with valid empty-ish hex
        let _ = dispatch(&ctx, "decodescript", &[json!("51")]).unwrap();
        // estimatesmartfee default conf
        let _ = dispatch(&ctx, "estimatesmartfee", &[]).unwrap();
        let _ = dispatch(&ctx, "estimatesmartfee", &[json!(6)]).unwrap();
        // handle_request error shapes
        let _ = handle_request(&ctx, &json!({}));
        let _ = handle_request(&ctx, &json!({"method":"getblockcount","params":{}}));
        let _ = handle_request(&ctx, &json!({"method":"getblockcount","params":1}));
        let _ = handle_request(&ctx, &json!({"id":1,"method":"nosuch","params":[]}));
        // no mempool
        let ctx2 = RpcContext {
            query: Arc::clone(&ctx.query),
            mempool: None,
            network: Network::Regtest,
            start: Instant::now(),
            stop: Arc::new(AtomicBool::new(false)),
            connections: Arc::new(AtomicU64::new(0)),
            initial_block_download: Arc::new(AtomicBool::new(true)),
        };
        let _ = dispatch(&ctx2, "getmempoolinfo", &[]).unwrap();
        let _ = dispatch(&ctx2, "getrawmempool", &[json!(true)]).unwrap();
        let _ = dispatch(&ctx2, "estimatesmartfee", &[json!(1)]).unwrap();
        let _ = dispatch(&ctx2, "sendrawtransaction", &[json!("00")]);
        let info = dispatch(&ctx2, "getblockchaininfo", &[]).unwrap();
        assert_eq!(info["initialblockdownload"], true);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn chain_methods_against_mined_regtest() {
        use bitcoin::consensus::encode::serialize_hex;
        use rbitcoin_consensus::ChainParams;
        use rbitcoin_test::build_mature_regtest_with_spend;

        let n = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("rbitcoin-rpc-chain-{n}"));
        std::fs::create_dir_all(&dir).unwrap();
        let q = Arc::new(Query::open_or_create(dir.join("store")).unwrap());
        let params = ChainParams::for_network(Network::Regtest);
        let chain = build_mature_regtest_with_spend(&q, &params);
        let mp =
            MempoolHub::open_with_weight(dir.join("mempool"), Arc::clone(&q), 300_000_000).unwrap();
        mp.set_relay_enabled(true);
        let ctx = RpcContext {
            query: Arc::clone(&q),
            mempool: Some(mp.clone()),
            network: Network::Regtest,
            start: Instant::now(),
            stop: Arc::new(AtomicBool::new(false)),
            connections: Arc::new(AtomicU64::new(1)),
            initial_block_download: Arc::new(AtomicBool::new(false)),
        };

        let tip_h = chain.tip_height();
        let count = dispatch(&ctx, "getblockcount", &[]).unwrap();
        assert_eq!(count.as_u64().unwrap(), tip_h as u64);

        // Pin: getbestblockhash matches rust-bitcoin BlockHash Display (Core order).
        let tip_hash = chain.tip_hash();
        let tip_display = tip_hash.to_string();
        let internal = tip_hash.to_byte_array();
        assert_ne!(
            tip_display,
            rbitcoin_primitives::hex_encode(internal),
            "regtest tip Display must differ from raw internal hex (non-palindrome)"
        );
        assert_eq!(
            tip_display,
            hash_hex_display(&internal),
            "hash_hex_display must match BlockHash Display"
        );

        let best = dispatch(&ctx, "getbestblockhash", &[]).unwrap();
        let best_s = best.as_str().unwrap().to_string();
        assert_eq!(
            best_s, tip_display,
            "getbestblockhash must be Core/display order, not internal"
        );
        let hash = dispatch(&ctx, "getblockhash", &[json!(tip_h)]).unwrap();
        assert_eq!(hash.as_str().unwrap(), best_s);
        // Lookup must accept display-order hex from Core clients.
        let hdr = dispatch(&ctx, "getblockheader", &[json!(best_s.clone())]).unwrap();
        assert_eq!(hdr["height"], tip_h);
        assert_eq!(hdr["hash"], best_s);
        // Merkleroot field must also be display order (consistent with hash).
        let mr = hdr["merkleroot"].as_str().unwrap();
        assert_eq!(mr.len(), 64);
        assert_eq!(mr, hash_hex_display(&parse_hash32_display(mr).unwrap()));
        let hdr_hex = dispatch(
            &ctx,
            "getblockheader",
            &[json!(best_s.clone()), json!(false)],
        )
        .unwrap();
        assert!(hdr_hex.as_str().unwrap().len() > 10);
        for verb in [0u64, 1, 2] {
            let blk = dispatch(&ctx, "getblock", &[json!(best_s.clone()), json!(verb)]).unwrap();
            if verb == 0 {
                assert!(blk.as_str().unwrap().len() > 20);
            } else {
                assert_eq!(blk["height"], tip_h);
                assert_eq!(blk["hash"], best_s);
                assert!(blk["tx"].as_array().unwrap().len() >= 1);
            }
        }
        let _ = dispatch(&ctx, "getdifficulty", &[]).unwrap();
        let info = dispatch(&ctx, "getblockchaininfo", &[]).unwrap();
        assert_eq!(info["blocks"], tip_h);
        assert_eq!(info["bestblockhash"], best_s);

        // Coinbase of tip for getrawtransaction — use Txid Display (Core order).
        let fks = q.block_tx_fks(rbitcoin_primitives::Height(tip_h)).unwrap();
        let tx = q.reconstruct_tx(fks[0]).unwrap();
        let txid_display = tx.compute_txid().to_string();
        let txid_internal = tx.compute_txid().to_byte_array();
        assert_eq!(txid_display, hash_hex_display(&txid_internal));
        // Internal-order hex must NOT be accepted as a Core-form getrawtransaction id
        // when it differs from display (typical for real txids).
        let internal_hex = rbitcoin_primitives::hex_encode(txid_internal);
        if internal_hex != txid_display {
            let miss = dispatch(&ctx, "getrawtransaction", &[json!(internal_hex)]);
            assert!(
                miss.is_err(),
                "internal-order hex must not resolve as Core display txid"
            );
        }
        let raw = dispatch(&ctx, "getrawtransaction", &[json!(txid_display.clone())]).unwrap();
        assert!(raw.as_str().unwrap().len() > 20);
        let verbose = dispatch(
            &ctx,
            "getrawtransaction",
            &[json!(txid_display.clone()), json!(true)],
        )
        .unwrap();
        assert_eq!(verbose["txid"], txid_display);

        let hex = serialize_hex(&tx);
        let dec = dispatch(&ctx, "decoderawtransaction", &[json!(hex)]).unwrap();
        assert_eq!(dec["txid"], txid_display);

        // testmempoolaccept dry path (may reject coinbase — still exercises code)
        let _ = dispatch(&ctx, "testmempoolaccept", &[json!([hex.clone()])]);
        let _ = dispatch(&ctx, "sendrawtransaction", &[json!(hex)]);

        // validate a regtest address from OP_TRUE is not standard — use bcrt1
        // from a known valid bech32 if available; otherwise still hit error path.
        let _ = dispatch(
            &ctx,
            "validateaddress",
            &[json!("bcrt1qw508d6qejxtdg4y5r3zarvary0c5xw7kygt080")],
        );

        let _ = std::fs::remove_dir_all(&dir);
    }
}
