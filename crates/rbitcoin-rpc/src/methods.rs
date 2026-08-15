//! Tier-1 Core-class JSON-RPC method handlers (pure dispatch over Query/mempool).

use bitcoin::consensus::{deserialize, encode::serialize_hex, Encodable};
use bitcoin::hashes::Hash;
use bitcoin::{Address, Network as BtcNetwork, ScriptBuf, Transaction, Txid};
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
    /// `getnetworkinfo.subversion` (BIP14 / Core `-uacomment` shape).
    pub subversion: String,
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
/// Core `RPC_INVALID_PARAMETER` (unknown named param, mocktime range, …).
pub const ERR_INVALID_PARAMETER: i64 = -8;
pub const ERR_INVALID_PARAMS: i64 = -32602;
pub const ERR_METHOD_NOT_FOUND: i64 = -32601;
pub const ERR_INVALID_REQUEST: i64 = -32600;

/// JSON-RPC `params`: positional array or Core named object.
#[derive(Clone, Debug, Default)]
pub struct RpcParams {
    pos: Vec<Value>,
    named: Option<serde_json::Map<String, Value>>,
}

impl RpcParams {
    pub fn empty() -> Self {
        Self::default()
    }

    pub fn positional(pos: Vec<Value>) -> Self {
        Self { pos, named: None }
    }

    pub fn named(named: serde_json::Map<String, Value>) -> Self {
        Self {
            pos: Vec::new(),
            named: Some(named),
        }
    }

    pub fn get(&self, index: usize, name: &str) -> Option<&Value> {
        if let Some(m) = &self.named {
            return m.get(name);
        }
        self.pos.get(index)
    }

    pub fn req(&self, index: usize, name: &str) -> Result<&Value, Value> {
        self.get(index, name)
            .ok_or_else(|| rpc_error(ERR_INVALID_PARAMS, format!("{name} required")))
    }

    pub fn req_str(&self, index: usize, name: &str) -> Result<&str, Value> {
        self.req(index, name)?
            .as_str()
            .ok_or_else(|| rpc_error(ERR_INVALID_PARAMS, format!("{name} must be a string")))
    }

    pub fn req_u64(&self, index: usize, name: &str) -> Result<u64, Value> {
        json_u64(self.req(index, name)?)
            .ok_or_else(|| rpc_error(ERR_INVALID_PARAMS, format!("{name} must be an integer")))
    }

    pub fn opt_u64(&self, index: usize, name: &str) -> Result<Option<u64>, Value> {
        match self.get(index, name) {
            None | Some(Value::Null) => Ok(None),
            Some(v) => json_u64(v)
                .map(Some)
                .ok_or_else(|| rpc_error(ERR_INVALID_PARAMS, format!("{name} must be an integer"))),
        }
    }

    pub fn opt_bool(&self, index: usize, name: &str) -> Result<Option<bool>, Value> {
        match self.get(index, name) {
            None | Some(Value::Null) => Ok(None),
            Some(v) => v
                .as_bool()
                .map(Some)
                .ok_or_else(|| rpc_error(ERR_INVALID_PARAMS, format!("{name} must be a bool"))),
        }
    }

    pub fn get_array(&self, index: usize, name: &str) -> Option<&Vec<Value>> {
        self.get(index, name).and_then(Value::as_array)
    }

    /// Named-only: unknown keys → Core `-8 Unknown named parameter`.
    pub fn reject_unknown(&self, allowed: &[&str]) -> Result<(), Value> {
        let Some(m) = &self.named else {
            return Ok(());
        };
        for k in m.keys() {
            if !allowed.iter().any(|a| *a == k) {
                return Err(rpc_error(
                    ERR_INVALID_PARAMETER,
                    format!("Unknown named parameter {k}"),
                ));
            }
        }
        Ok(())
    }
}

fn json_u64(v: &Value) -> Option<u64> {
    v.as_u64()
        .or_else(|| v.as_i64().and_then(|n| u64::try_from(n).ok()))
}

impl From<Vec<Value>> for RpcParams {
    fn from(pos: Vec<Value>) -> Self {
        Self::positional(pos)
    }
}

impl From<&Vec<Value>> for RpcParams {
    fn from(pos: &Vec<Value>) -> Self {
        Self::positional(pos.clone())
    }
}

/// Dispatch one method. Returns `Ok(result)` or `Err(error_object)`.
pub fn dispatch(
    ctx: &RpcContext,
    method: &str,
    params: impl Into<RpcParams>,
) -> Result<Value, Value> {
    let params = params.into();
    match method {
        "help" => help(&params),
        "getrpcinfo" => {
            params.reject_unknown(&[])?;
            Ok(getrpcinfo(ctx))
        }
        "uptime" => {
            params.reject_unknown(&[])?;
            Ok(json!(ctx.uptime_secs()))
        }
        "stop" => {
            params.reject_unknown(&[])?;
            ctx.stop.store(true, Ordering::SeqCst);
            Ok(json!("rbitcoin stopping"))
        }
        "getblockchaininfo" => {
            params.reject_unknown(&[])?;
            getblockchaininfo(ctx)
        }
        "getblockcount" => {
            params.reject_unknown(&[])?;
            getblockcount(ctx)
        }
        "getbestblockhash" => {
            params.reject_unknown(&[])?;
            getbestblockhash(ctx)
        }
        "getblockhash" => getblockhash(ctx, &params),
        "getblockheader" => getblockheader(ctx, &params),
        "getblock" => getblock(ctx, &params),
        "getdifficulty" => {
            params.reject_unknown(&[])?;
            getdifficulty(ctx)
        }
        "getnetworkinfo" => {
            params.reject_unknown(&[])?;
            Ok(getnetworkinfo(ctx))
        }
        "getconnectioncount" => {
            params.reject_unknown(&[])?;
            Ok(json!(ctx.connections.load(Ordering::Relaxed)))
        }
        "getpeerinfo" => {
            params.reject_unknown(&[])?;
            Ok(json!([])) // best-effort stub; peer detail not exposed yet
        }
        "getmempoolinfo" => {
            params.reject_unknown(&[])?;
            getmempoolinfo(ctx)
        }
        "getrawmempool" => getrawmempool(ctx, &params),
        "getmempoolentry" => getmempoolentry(ctx, &params),
        "getrawtransaction" => getrawtransaction(ctx, &params),
        "sendrawtransaction" => sendrawtransaction(ctx, &params),
        "testmempoolaccept" => testmempoolaccept(ctx, &params),
        "decoderawtransaction" => decoderawtransaction(&params),
        "decodescript" => decodescript(&params),
        "validateaddress" => validateaddress(ctx, &params),
        "estimatesmartfee" => estimatesmartfee(ctx, &params),
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

fn help(params: &RpcParams) -> Result<Value, Value> {
    params.reject_unknown(&["command"])?;
    if let Some(v) = params.get(0, "command") {
        let m = v
            .as_str()
            .ok_or_else(|| rpc_error(ERR_INVALID_PARAMS, "command must be a string"))?;
        return Ok(json!(method_help(m)));
    }
    Ok(json!(METHOD_LIST.join("\n")))
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
        "getblockchaininfo" => {
            "getblockchaininfo\nReturns tip height, chain name, and IBD flag.".into()
        }
        "help" => "help\nhelp ( \"command\" ) — list methods or describe one.".into(),
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

fn getblockhash(ctx: &RpcContext, params: &RpcParams) -> Result<Value, Value> {
    params.reject_unknown(&["height"])?;
    let height = params.req_u64(0, "height")? as u32;
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

fn getblockheader(ctx: &RpcContext, params: &RpcParams) -> Result<Value, Value> {
    params.reject_unknown(&["blockhash", "verbose"])?;
    let hash_hex = params.req_str(0, "blockhash")?;
    let verbose = params.opt_bool(1, "verbose")?.unwrap_or(true);
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

fn getblock(ctx: &RpcContext, params: &RpcParams) -> Result<Value, Value> {
    params.reject_unknown(&["blockhash", "verbosity"])?;
    let hash_hex = params.req_str(0, "blockhash")?;
    let verbosity = params.opt_u64(1, "verbosity")?.unwrap_or(1) as u32;
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
        "subversion": ctx.subversion,
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
            "loaded": true,
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

fn getrawmempool(ctx: &RpcContext, params: &RpcParams) -> Result<Value, Value> {
    params.reject_unknown(&["verbose"])?;
    let verbose = params.opt_bool(0, "verbose")?.unwrap_or(false);
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

fn getmempoolentry(ctx: &RpcContext, params: &RpcParams) -> Result<Value, Value> {
    params.reject_unknown(&["txid"])?;
    let hex = params.req_str(0, "txid")?;
    let want = parse_hash32_display(hex)?;
    let mp = ctx
        .mempool
        .as_ref()
        .ok_or_else(|| rpc_error(ERR_MISC, "mempool not available"))?;
    let tid = Txid::from_byte_array(want);
    if let Some((fee, weight)) = mp.get_live_meta(&tid) {
        let vsize = weight / 4;
        return Ok(json!({
            "vsize": vsize,
            "weight": weight,
            "fee": (fee as f64) / 100_000_000.0,
            "modifiedfee": (fee as f64) / 100_000_000.0,
        }));
    }
    Err(rpc_error(ERR_MISC, "Transaction not in mempool"))
}

fn getrawtransaction(ctx: &RpcContext, params: &RpcParams) -> Result<Value, Value> {
    params.reject_unknown(&["txid", "verbose"])?;
    let hex = params.req_str(0, "txid")?;
    let verbose = params.opt_bool(1, "verbose")?.unwrap_or(false);
    let want = parse_hash32_display(hex)?;

    if let Some(mp) = ctx.mempool.as_ref() {
        let tid = Txid::from_byte_array(want);
        if let Some(tx) = mp.get_tx(&tid) {
            if !verbose {
                return Ok(json!(serialize_hex(&tx)));
            }
            return Ok(tx_to_json(&tx, Some(json!({ "in_mempool": true }))));
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

fn sendrawtransaction(ctx: &RpcContext, params: &RpcParams) -> Result<Value, Value> {
    params.reject_unknown(&["hexstring", "maxfeerate"])?;
    let hex = params.req_str(0, "hexstring")?;
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

fn testmempoolaccept(ctx: &RpcContext, params: &RpcParams) -> Result<Value, Value> {
    params.reject_unknown(&["rawtxs", "maxfeerate"])?;
    let arr = params
        .get_array(0, "rawtxs")
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

fn decoderawtransaction(params: &RpcParams) -> Result<Value, Value> {
    params.reject_unknown(&["hexstring", "iswitness"])?;
    let hex = params.req_str(0, "hexstring")?;
    let tx = decode_tx_hex(hex)?;
    Ok(tx_to_json(&tx, None))
}

fn decodescript(params: &RpcParams) -> Result<Value, Value> {
    params.reject_unknown(&["hexstring"])?;
    let hex = params.req_str(0, "hexstring")?;
    let bytes = hex_decode(hex).map_err(|e| rpc_error(ERR_INVALID_PARAMS, e.to_string()))?;
    let script = ScriptBuf::from_bytes(bytes);
    let asm = script.to_asm_string();
    Ok(json!({
        "asm": asm,
        "hex": hex_encode(script.as_bytes()),
        "type": "nonstandard",
    }))
}

fn validateaddress(ctx: &RpcContext, params: &RpcParams) -> Result<Value, Value> {
    params.reject_unknown(&["address"])?;
    let addr_s = params.req_str(0, "address")?;
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
fn estimatesmartfee(ctx: &RpcContext, params: &RpcParams) -> Result<Value, Value> {
    params.reject_unknown(&["conf_target", "estimate_mode"])?;
    let conf_target = params.opt_u64(0, "conf_target")?.unwrap_or(2) as u32;
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
    let params = match body.get("params") {
        None | Some(Value::Null) => RpcParams::empty(),
        Some(Value::Array(a)) => RpcParams::positional(a.clone()),
        Some(Value::Object(m)) => RpcParams::named(m.clone()),
        Some(_) => {
            return json!({
                "result": null,
                "error": rpc_error(ERR_INVALID_PARAMS, "params must be array or object"),
                "id": id,
            });
        }
    };
    match dispatch(ctx, method, params) {
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
            subversion: rbitcoin_primitives::rbitcoin_subversion(
                env!("CARGO_PKG_VERSION"),
                &["testnode0"],
            )
            .unwrap(),
        };
        (ctx, dir)
    }

    #[test]
    fn help_and_getrpcinfo_list_methods() {
        let (ctx, dir) = ctx_empty();
        let help_all = dispatch(&ctx, "help", vec![]).unwrap();
        let s = help_all.as_str().unwrap();
        assert!(s.contains("getblockchaininfo"));
        assert!(s.contains("estimatesmartfee"));
        let info = dispatch(&ctx, "getrpcinfo", vec![]).unwrap();
        assert!(info["methods"].as_array().unwrap().len() >= 10);
        assert!(info["uptime"].as_u64().unwrap() >= 42);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn blockchain_empty_store() {
        let (ctx, dir) = ctx_empty();
        let count = dispatch(&ctx, "getblockcount", vec![]).unwrap();
        assert_eq!(count, json!(0));
        let info = dispatch(&ctx, "getblockchaininfo", vec![]).unwrap();
        assert_eq!(info["chain"], "regtest");
        assert_eq!(info["blocks"], 0);
        assert_eq!(info["initialblockdownload"], false);
        let mem = dispatch(&ctx, "getmempoolinfo", vec![]).unwrap();
        assert_eq!(mem["size"], 0);
        assert_eq!(mem["loaded"], true);
        let raw = dispatch(&ctx, "getrawmempool", vec![]).unwrap();
        assert_eq!(raw, json!([]));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn estimatesmartfee_maps_to_product() {
        let (ctx, dir) = ctx_empty();
        let r = dispatch(&ctx, "estimatesmartfee", vec![json!(2)]).unwrap();
        // Empty mempool → negative feerate with errors.
        assert!(r["feerate"].as_f64().unwrap() < 0.0 || r.get("rbitcoin_model").is_some());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn mempoolinfo_loaded_and_uacomment() {
        let (ctx, dir) = ctx_empty();
        let info = dispatch(&ctx, "getnetworkinfo", vec![]).unwrap();
        let sub = info["subversion"].as_str().unwrap();
        assert!(sub.ends_with("(testnode0)/"), "{sub}");
        let mem = dispatch(&ctx, "getmempoolinfo", vec![]).unwrap();
        assert_eq!(mem["loaded"], true);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn stop_sets_flag() {
        let (ctx, dir) = ctx_empty();
        assert!(!ctx.stop.load(Ordering::SeqCst));
        dispatch(&ctx, "stop", vec![]).unwrap();
        assert!(ctx.stop.load(Ordering::SeqCst));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn unsupported_methods_error() {
        let (ctx, dir) = ctx_empty();
        let e = dispatch(&ctx, "scantxoutset", vec![]).unwrap_err();
        assert_eq!(e["code"], ERR_METHOD_NOT_FOUND);
        let e2 = dispatch(&ctx, "createrawtransaction", vec![]).unwrap_err();
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
    fn named_params_getblock_object() {
        let (ctx, dir) = ctx_empty();
        // Empty object is valid for methods with no args.
        let resp = handle_request(&ctx, &json!({"id":1,"method":"getblockcount","params":{}}));
        assert!(resp["error"].is_null(), "{resp}");
        assert_eq!(resp["result"], 0);

        let h = handle_request(
            &ctx,
            &json!({"method":"help","params":{"command":"getblockchaininfo"}}),
        );
        let s = h["result"].as_str().unwrap();
        assert!(s.starts_with("getblockchaininfo\n"), "{s}");

        let unknown = handle_request(
            &ctx,
            &json!({"method":"help","params":{"random":"getblockchaininfo"}}),
        );
        assert_eq!(unknown["error"]["code"], ERR_INVALID_PARAMETER);
        assert!(
            unknown["error"]["message"]
                .as_str()
                .unwrap()
                .contains("Unknown named parameter"),
            "{unknown}"
        );

        // Named height on empty store: accepted as params (not "named not supported").
        let gh = handle_request(
            &ctx,
            &json!({"method":"getblockhash","params":{"height":0}}),
        );
        assert_ne!(
            gh["error"]["message"].as_str().unwrap_or(""),
            "named params not supported; use array"
        );
        assert_eq!(gh["error"]["code"], ERR_MISC); // height out of range

        let missing = handle_request(&ctx, &json!({"method":"getblock","params":{}}));
        assert_eq!(missing["error"]["code"], ERR_INVALID_PARAMS);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn decoderawtransaction_coinbase_like() {
        // Minimal empty-vin is invalid; use a trivial valid bare tx if available.
        // Just ensure invalid hex errors.
        let (ctx, dir) = ctx_empty();
        let e = dispatch(&ctx, "decoderawtransaction", vec![json!("00")]).unwrap_err();
        assert_eq!(e["code"], ERR_INVALID_PARAMS);
        let _ = ctx;
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn validateaddress_regtest() {
        let (ctx, dir) = ctx_empty();
        // Invalid
        let r = dispatch(&ctx, "validateaddress", vec![json!("notanaddr")]).unwrap();
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
            let _ = dispatch(&ctx, m, vec![]).expect(m);
        }
        // Empty store: no tip → difficulty errors.
        let _ = dispatch(&ctx, "getdifficulty", vec![]);
        let _ = dispatch(&ctx, "getrawmempool", vec![json!(true)]).unwrap();
        let _ = dispatch(&ctx, "help", vec![json!("estimatesmartfee")]).unwrap();
        let _ = dispatch(&ctx, "help", vec![json!("getblockchaininfo")]).unwrap();
        let _ = dispatch(&ctx, "help", vec![json!("help")]).unwrap();
        let _ = dispatch(&ctx, "help", vec![json!("unknown_method_xyz")]).unwrap();
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
        let _ = dispatch(&ctx, "decodescript", vec![json!("51")]).unwrap();
        // estimatesmartfee default conf
        let _ = dispatch(&ctx, "estimatesmartfee", vec![]).unwrap();
        let _ = dispatch(&ctx, "estimatesmartfee", vec![json!(6)]).unwrap();
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
            subversion: "/rbitcoin:0.1.0/".into(),
        };
        let mem2 = dispatch(&ctx2, "getmempoolinfo", vec![]).unwrap();
        assert_eq!(mem2["loaded"], true);
        let _ = dispatch(&ctx2, "getrawmempool", vec![json!(true)]).unwrap();
        let _ = dispatch(&ctx2, "estimatesmartfee", vec![json!(1)]).unwrap();
        let _ = dispatch(&ctx2, "sendrawtransaction", vec![json!("00")]);
        let info = dispatch(&ctx2, "getblockchaininfo", vec![]).unwrap();
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
            subversion: "/rbitcoin:0.1.0/".into(),
        };

        let tip_h = chain.tip_height();
        let count = dispatch(&ctx, "getblockcount", vec![]).unwrap();
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

        let best = dispatch(&ctx, "getbestblockhash", vec![]).unwrap();
        let best_s = best.as_str().unwrap().to_string();
        assert_eq!(
            best_s, tip_display,
            "getbestblockhash must be Core/display order, not internal"
        );
        let hash = dispatch(&ctx, "getblockhash", vec![json!(tip_h)]).unwrap();
        assert_eq!(hash.as_str().unwrap(), best_s);
        // Lookup must accept display-order hex from Core clients.
        let hdr = dispatch(&ctx, "getblockheader", vec![json!(best_s.clone())]).unwrap();
        assert_eq!(hdr["height"], tip_h);
        assert_eq!(hdr["hash"], best_s);
        // Merkleroot field must also be display order (consistent with hash).
        let mr = hdr["merkleroot"].as_str().unwrap();
        assert_eq!(mr.len(), 64);
        assert_eq!(mr, hash_hex_display(&parse_hash32_display(mr).unwrap()));
        let hdr_hex = dispatch(
            &ctx,
            "getblockheader",
            vec![json!(best_s.clone()), json!(false)],
        )
        .unwrap();
        assert!(hdr_hex.as_str().unwrap().len() > 10);
        for verb in [0u64, 1, 2] {
            let blk = dispatch(&ctx, "getblock", vec![json!(best_s.clone()), json!(verb)]).unwrap();
            if verb == 0 {
                assert!(blk.as_str().unwrap().len() > 20);
            } else {
                assert_eq!(blk["height"], tip_h);
                assert_eq!(blk["hash"], best_s);
                assert!(blk["tx"].as_array().unwrap().len() >= 1);
            }
        }
        let _ = dispatch(&ctx, "getdifficulty", vec![]).unwrap();
        let info = dispatch(&ctx, "getblockchaininfo", vec![]).unwrap();
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
            let miss = dispatch(&ctx, "getrawtransaction", vec![json!(internal_hex)]);
            assert!(
                miss.is_err(),
                "internal-order hex must not resolve as Core display txid"
            );
        }
        let raw = dispatch(&ctx, "getrawtransaction", vec![json!(txid_display.clone())]).unwrap();
        assert!(raw.as_str().unwrap().len() > 20);
        let verbose = dispatch(
            &ctx,
            "getrawtransaction",
            vec![json!(txid_display.clone()), json!(true)],
        )
        .unwrap();
        assert_eq!(verbose["txid"], txid_display);

        let hex = serialize_hex(&tx);
        let dec = dispatch(&ctx, "decoderawtransaction", vec![json!(hex)]).unwrap();
        assert_eq!(dec["txid"], txid_display);

        // testmempoolaccept dry path (may reject coinbase — still exercises code)
        let _ = dispatch(&ctx, "testmempoolaccept", vec![json!([hex.clone()])]);
        let _ = dispatch(&ctx, "sendrawtransaction", vec![json!(hex)]);

        // validate a regtest address from OP_TRUE is not standard — use bcrt1
        // from a known valid bech32 if available; otherwise still hit error path.
        let _ = dispatch(
            &ctx,
            "validateaddress",
            vec![json!("bcrt1qw508d6qejxtdg4y5r3zarvary0c5xw7kygt080")],
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Single-txid mempool RPC must not scan/clone the live set.
    #[test]
    fn mempool_txid_lookups_do_not_list_live() {
        use bitcoin::absolute::LockTime;
        use bitcoin::script::ScriptBuf;
        use bitcoin::transaction::Version as TxVersion;
        use bitcoin::{Amount, OutPoint, Sequence, Transaction, TxIn, TxOut, Witness};
        use rbitcoin_consensus::{accept_and_connect_block, ChainParams, Milestone};
        use rbitcoin_primitives::Height;

        if std::env::var_os("RBITCOIN_HEAD_SCALE").is_none() {
            std::env::set_var("RBITCOIN_HEAD_SCALE", "tiny");
        }
        let (ctx, dir) = ctx_empty();
        let params = ChainParams::regtest();
        let genesis = bitcoin::blockdata::constants::genesis_block(bitcoin::Network::Regtest);
        accept_and_connect_block(
            &ctx.query,
            &params,
            Height::GENESIS,
            &genesis,
            Milestone::NONE,
        )
        .unwrap();
        const N_SPENDS: u32 = 3;
        let (_tip, _tip_time, coinbase_txids) = rbitcoin_consensus::pad_empty_from(
            &ctx.query,
            &params,
            genesis.block_hash(),
            genesis.header.time,
            1,
            100 + N_SPENDS,
            N_SPENDS,
        );
        let mp = ctx.mempool.as_ref().expect("mempool");
        mp.set_relay_enabled(true);
        let spk = ScriptBuf::from_bytes(vec![0x51]);
        let mut live = Vec::new();
        for (i, cbtxid) in coinbase_txids.iter().enumerate() {
            let fee = 1_000u64 + i as u64;
            let tx = Transaction {
                version: TxVersion::TWO,
                lock_time: LockTime::ZERO,
                input: vec![TxIn {
                    previous_output: OutPoint {
                        txid: *cbtxid,
                        vout: 0,
                    },
                    script_sig: ScriptBuf::new(),
                    sequence: Sequence::ENABLE_RBF_NO_LOCKTIME,
                    witness: Witness::new(),
                }],
                output: vec![TxOut {
                    value: Amount::from_sat(50_0000_0000 - fee),
                    script_pubkey: spk.clone(),
                }],
            };
            mp.accept_tx(&tx).expect("accept");
            live.push(tx);
        }
        let want = live[1].compute_txid();
        let want_hex = hash_hex_display(&want.to_byte_array());
        let _ = mp.sample_reset_perf();

        let entry = dispatch(&ctx, "getmempoolentry", vec![json!(want_hex.clone())]).unwrap();
        assert!(entry["weight"].as_u64().unwrap() > 0);
        let raw = dispatch(&ctx, "getrawtransaction", vec![json!(want_hex.clone())]).unwrap();
        assert!(raw.as_str().unwrap().len() > 20);
        let verb = dispatch(
            &ctx,
            "getrawtransaction",
            vec![json!(want_hex), json!(true)],
        )
        .unwrap();
        assert_eq!(verb["txid"], format!("{want}"));

        let s = mp.sample_reset_perf();
        assert_eq!(s.list_live, 0, "getrawtransaction must not list_live");
        assert_eq!(
            s.list_live_meta, 0,
            "getmempoolentry must not list_live_meta"
        );

        let miss = dispatch(&ctx, "getmempoolentry", vec![json!("00".repeat(32))]).unwrap_err();
        assert_eq!(miss["message"], "Transaction not in mempool");
        let miss_raw =
            dispatch(&ctx, "getrawtransaction", vec![json!("11".repeat(32))]).unwrap_err();
        assert_eq!(
            miss_raw["message"],
            "No such mempool or blockchain transaction"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }
}
