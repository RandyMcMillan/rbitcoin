//! Tier-1 Core-class JSON-RPC method handlers (pure dispatch over Query/mempool).

use bitcoin::consensus::{deserialize, encode::serialize_hex, Encodable};
use bitcoin::hashes::Hash;
use bitcoin::{
    Address, Amount, Block, BlockHash, Network as BtcNetwork, ScriptBuf, Transaction, Txid,
};
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
    /// Regtest generate/submitblock. Node attaches [`ChainHub`] via this trait.
    pub regtest: Option<Arc<dyn RpcRegtest>>,
    /// Live P2P sessions (`getpeerinfo` / `addnode` / `disconnectnode`).
    pub peers: Option<Arc<rbitcoin_net::PeerHub>>,
    /// Live chain (invalidate / reconsider / precious).
    pub chain: Option<Arc<rbitcoin_net::ChainHub>>,
}

/// Outcome of `submitblock` (Core: `null` or a reject-reason string).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SubmitBlockOutcome {
    Accepted,
    Duplicate,
    IgnoredWeaker,
    Rejected(String),
}

/// Regtest-only mine + accept. Implemented by the node (not a mining product).
pub trait RpcRegtest: Send + Sync {
    fn generate_to_script(
        &self,
        nblocks: u32,
        script_pubkey: ScriptBuf,
        extra_txs: Vec<Transaction>,
    ) -> Result<Vec<BlockHash>, String>;

    fn submit_block(&self, block: Block) -> SubmitBlockOutcome;

    /// `0` = wall clock. Regtest harness only.
    fn set_mock_time(&self, timestamp: i64) -> Result<(), String>;
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
/// Core `RPC_CLIENT_NODE_NOT_CONNECTED`.
pub const ERR_CLIENT_NODE_NOT_CONNECTED: i64 = -29;
/// Core `RPC_INVALID_PARAMETER` (unknown named param, mocktime range, …).
pub const ERR_INVALID_PARAMETER: i64 = -8;
/// Core `RPC_VERIFY_REJECTED` (sendrawtransaction / testmempoolaccept).
pub const ERR_VERIFY_REJECTED: i64 = -26;
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

    pub fn named(mut named: serde_json::Map<String, Value>) -> Self {
        // AuthServiceProxy mixed call: `{args: [...], argN: ...}`.
        let pos = match named.remove("args") {
            Some(Value::Array(a)) => a,
            Some(other) => {
                named.insert("args".into(), other);
                Vec::new()
            }
            None => Vec::new(),
        };
        Self {
            pos,
            named: Some(named),
        }
    }

    pub fn get(&self, index: usize, name: &str) -> Option<&Value> {
        if let Some(m) = &self.named {
            if let Some(v) = m.get(name) {
                return Some(v);
            }
            // Mixed object: named miss falls through to the peeled `args` array.
            if !self.pos.is_empty() {
                return self.pos.get(index);
            }
            return None;
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

/// Core `getblock` verbosity: integer, or bool (`false` → 0, `true` → 1).
fn opt_verbosity(params: &RpcParams, index: usize, name: &str) -> Result<u32, Value> {
    match params.get(index, name) {
        None | Some(Value::Null) => Ok(1),
        Some(Value::Bool(false)) => Ok(0),
        Some(Value::Bool(true)) => Ok(1),
        Some(v) => json_u64(v)
            .map(|n| n as u32)
            .ok_or_else(|| rpc_error(ERR_INVALID_PARAMS, format!("{name} must be an integer"))),
    }
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
        "echo" => echo(&params),
        "getrpcinfo" => {
            params.reject_unknown(&[])?;
            Ok(getrpcinfo(ctx))
        }
        "uptime" => {
            params.reject_unknown(&[])?;
            Ok(json!(ctx.uptime_secs()))
        }
        "stop" => {
            params.reject_unknown(&["wait"])?;
            let _wait = params.opt_u64(0, "wait")?;
            ctx.stop.store(true, Ordering::SeqCst);
            Ok(json!("rbitcoin stopping"))
        }
        "syncwithvalidationinterfacequeue" => {
            params.reject_unknown(&[])?;
            // Core waits for wallet/index callbacks. We have no that queue.
            Ok(Value::Null)
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
            Ok(getpeerinfo(ctx))
        }
        "addnode" => addnode(ctx, &params),
        "disconnectnode" => disconnectnode(ctx, &params),
        "addconnection" => addconnection(ctx, &params),
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
        "generatetoaddress" => generatetoaddress(ctx, &params),
        "generatetodescriptor" => generatetodescriptor(ctx, &params),
        "generateblock" => generateblock(ctx, &params),
        "generate" => generate(ctx, &params),
        "submitblock" => submitblock(ctx, &params),
        "setmocktime" => setmocktime(ctx, &params),
        "invalidateblock" => invalidateblock(ctx, &params),
        "reconsiderblock" => reconsiderblock(ctx, &params),
        "preciousblock" => preciousblock(ctx, &params),
        "scantxoutset" => scantxoutset(ctx, &params),
        "gettxout" => gettxout(ctx, &params),
        "getindexinfo" => getindexinfo(ctx, &params),
        "getchaintips" => getchaintips(ctx, &params),
        "waitforblock" => waitforblock(ctx, &params),
        "waitforblockheight" => waitforblockheight(ctx, &params),
        "waitfornewblock" => waitfornewblock(ctx, &params),
        "createrawtransaction"
        | "combinerawtransaction"
        | "getblocktemplate"
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

/// Core `echo` names (`rpc_named_arguments.py`).
const ECHO_NAMES: [&str; 10] = [
    "arg0", "arg1", "arg2", "arg3", "arg4", "arg5", "arg6", "arg7", "arg8", "arg9",
];

/// Return params as a positional array (Core testing RPC).
///
/// Mixed AuthServiceProxy: `{args: [0, 1], arg3: 3}` → `[0, 1, null, 3]`.
/// Named-only `arg9` sizes the array to 10 with null holes.
fn echo(params: &RpcParams) -> Result<Value, Value> {
    params.reject_unknown(&ECHO_NAMES)?;
    if let Some(m) = &params.named {
        for (i, name) in ECHO_NAMES.iter().enumerate() {
            if m.contains_key(*name) && i < params.pos.len() {
                return Err(rpc_error(
                    ERR_INVALID_PARAMETER,
                    format!(
                        "Parameter {name} specified twice both as positional and named argument"
                    ),
                ));
            }
        }
    }

    let mut max_idx: Option<usize> = if params.pos.is_empty() {
        None
    } else {
        Some(params.pos.len() - 1)
    };
    if let Some(m) = &params.named {
        for (i, name) in ECHO_NAMES.iter().enumerate() {
            if m.contains_key(*name) {
                max_idx = Some(max_idx.map(|cur| cur.max(i)).unwrap_or(i));
            }
        }
    }
    let Some(max) = max_idx else {
        return Ok(json!([]));
    };
    let mut out = vec![Value::Null; max + 1];
    for (i, v) in params.pos.iter().enumerate() {
        out[i] = v.clone();
    }
    if let Some(m) = &params.named {
        for (i, name) in ECHO_NAMES.iter().enumerate() {
            if let Some(v) = m.get(*name) {
                out[i] = v.clone();
            }
        }
    }
    Ok(Value::Array(out))
}

const METHOD_LIST: &[&str] = &[
    "help",
    "echo",
    "getrpcinfo",
    "uptime",
    "stop",
    "syncwithvalidationinterfacequeue",
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
    "addnode",
    "disconnectnode",
    "addconnection",
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
    "generatetoaddress",
    "generatetodescriptor",
    "generateblock",
    "generate",
    "scantxoutset",
    "gettxout",
    "getindexinfo",
    "getchaintips",
    "waitforblock",
    "waitforblockheight",
    "waitfornewblock",
    "submitblock",
    "setmocktime",
    "invalidateblock",
    "reconsiderblock",
    "preciousblock",
];

fn method_help(m: &str) -> String {
    match m {
        "estimatesmartfee" => {
            "estimatesmartfee conf_target (mode ignored). Returns this node's 10-minute \
             inclusion frontier feerate (BTC/kvB), not Core historical multi-horizon. \
             See docs/mempool-fee-estimation.md."
                .into()
        }
        "getblockchaininfo" => "getblockchaininfo\nReturns tip height, chain name, and IBD flag.\n\
             chainwork, size_on_disk, and verificationprogress are placeholders."
            .into(),
        "generatetoaddress" => "generatetoaddress nblocks address (maxtries)\n\
             Regtest harness only. Mines nblocks paying address via the P2P accept path."
            .into(),
        "generateblock" => "generateblock output transactions\n\
             Regtest harness only. One block paying output (address or hex script)."
            .into(),
        "generate" => "generate nblocks (maxtries)\n\
             Regtest harness only. Mines to OP_TRUE (no wallet)."
            .into(),
        "generatetodescriptor" => "generatetodescriptor nblocks descriptor (maxtries)\n\
             Regtest harness only. raw(HEX) descriptor or address."
            .into(),
        "scantxoutset" => "scantxoutset action (scanobjects)\n\
             raw() scripts over Class A. MiniWallet support, not Core coins-DB."
            .into(),
        "gettxout" => "gettxout txid n (include_mempool) — Class A + mempool.".into(),
        "getchaintips" => "getchaintips — active tip only.".into(),
        "submitblock" => "submitblock hexdata (dummy)\n\
             Regtest harness only. Accepts a serialized block via the P2P path."
            .into(),
        "help" => "help\nhelp ( \"command\" ) — list methods or describe one.".into(),
        "echo" => "echo\necho ( arg0 ... arg9 ) — return arguments as a positional array.".into(),
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

/// Core-shaped version integer: major*10000 + minor*100 + patch.
fn rpc_client_version(semver: &str) -> u64 {
    let mut it = semver.split('.');
    let maj: u64 = it.next().and_then(|s| s.parse().ok()).unwrap_or(0);
    let min: u64 = it.next().and_then(|s| s.parse().ok()).unwrap_or(0);
    let pat: u64 = it.next().and_then(|s| s.parse().ok()).unwrap_or(0);
    maj.saturating_mul(10_000)
        .saturating_add(min.saturating_mul(100))
        .saturating_add(pat)
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
    let verbosity = opt_verbosity(params, 1, "verbosity")?;
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

fn getpeerinfo(ctx: &RpcContext) -> Value {
    let Some(hub) = ctx.peers.as_ref() else {
        return json!([]);
    };
    let rows: Vec<Value> = hub.snapshot().into_iter().map(peerinfo_json).collect();
    json!(rows)
}

fn peerinfo_json(p: rbitcoin_net::PeerInfo) -> Value {
    let mut recv = serde_json::Map::new();
    for (k, v) in p.bytesrecv_per_msg {
        recv.insert(k, json!(v));
    }
    let mut sent = serde_json::Map::new();
    for (k, v) in p.bytessent_per_msg {
        sent.insert(k, json!(v));
    }
    json!({
        "id": p.id,
        "addr": p.addr.to_string(),
        "addrbind": p.addrbind.to_string(),
        "subver": p.subver,
        "inbound": p.inbound,
        "services": format!("{:016x}", p.services),
        "servicesnames": services_names(p.services),
        "startingheight": p.startingheight,
        "bytesrecv_per_msg": recv,
        "bytessent_per_msg": sent,
        "connection_type": p.conn_type.as_str(),
        "transport_protocol_type": "v2",
        "network": "ipv4",
        "synced_headers": -1,
        "synced_blocks": -1,
    })
}

fn services_names(bits: u64) -> Vec<&'static str> {
    let mut n = Vec::new();
    if bits & 1 != 0 {
        n.push("NETWORK");
    }
    if bits & 8 != 0 {
        n.push("WITNESS");
    }
    if bits & 0x400 != 0 {
        n.push("NETWORK_LIMITED");
    }
    if bits & 0x800 != 0 {
        n.push("P2P_V2");
    }
    n
}

fn require_peers(ctx: &RpcContext) -> Result<&rbitcoin_net::PeerHub, Value> {
    ctx.peers
        .as_deref()
        .ok_or_else(|| rpc_error(ERR_MISC, "P2P session table not attached"))
}

fn addnode(ctx: &RpcContext, params: &RpcParams) -> Result<Value, Value> {
    params.reject_unknown(&["node", "command", "v2transport"])?;
    let hub = require_peers(ctx)?;
    let node = params.req_str(0, "node")?;
    let cmd = params.req_str(1, "command")?;
    let _v2 = params.opt_bool(2, "v2transport")?;
    let addr = rbitcoin_net::parse_peer_addr(node)
        .map_err(|e| rpc_error(ERR_INVALID_PARAMS, e.to_string()))?;
    hub.addnode(addr, cmd).map_err(|e| rpc_error(ERR_MISC, e))?;
    Ok(Value::Null)
}

fn disconnectnode(ctx: &RpcContext, params: &RpcParams) -> Result<Value, Value> {
    params.reject_unknown(&["address", "nodeid"])?;
    let hub = require_peers(ctx)?;
    if let Some(id) = params.opt_u64(1, "nodeid")? {
        if !hub.disconnect_id(id) {
            return Err(rpc_error(
                ERR_CLIENT_NODE_NOT_CONNECTED,
                "Node not found in connected nodes",
            ));
        }
        return Ok(Value::Null);
    }
    if let Some(a) = params.get(0, "address").and_then(|v| v.as_str()) {
        let addr = rbitcoin_net::parse_peer_addr(a)
            .map_err(|e| rpc_error(ERR_INVALID_PARAMS, e.to_string()))?;
        if !hub.disconnect_addr(addr) {
            return Err(rpc_error(
                ERR_CLIENT_NODE_NOT_CONNECTED,
                "Node not found in connected nodes",
            ));
        }
        return Ok(Value::Null);
    }
    Err(rpc_error(ERR_INVALID_PARAMS, "address or nodeid required"))
}

fn addconnection(ctx: &RpcContext, params: &RpcParams) -> Result<Value, Value> {
    params.reject_unknown(&["address", "connection_type", "v2transport"])?;
    let hub = require_peers(ctx)?;
    let address = params.req_str(0, "address")?;
    let typ_s = params.req_str(1, "connection_type")?;
    let _v2 = params.opt_bool(2, "v2transport")?.unwrap_or(true);
    let addr = rbitcoin_net::parse_peer_addr(address)
        .map_err(|e| rpc_error(ERR_INVALID_PARAMS, e.to_string()))?;
    let typ =
        rbitcoin_net::PeerConnType::parse(typ_s).map_err(|e| rpc_error(ERR_INVALID_PARAMS, e))?;
    hub.addconnection(addr, typ)
        .map_err(|e| rpc_error(ERR_MISC, e))?;
    Ok(json!({
        "address": address,
        "connection_type": typ.as_str(),
    }))
}

fn getnetworkinfo(ctx: &RpcContext) -> Value {
    let (cin, cout) = if let Some(hub) = ctx.peers.as_ref() {
        let rows = hub.snapshot();
        let cin = rows.iter().filter(|p| p.inbound).count() as u64;
        let cout = rows.iter().filter(|p| !p.inbound).count() as u64;
        (cin, cout)
    } else {
        (0, ctx.connections.load(Ordering::Relaxed))
    };
    let flags = rbitcoin_net::local_service_flags();
    let svc_bits = flags.to_u64();
    json!({
        "version": rpc_client_version(env!("CARGO_PKG_VERSION")),
        "subversion": ctx.subversion,
        "protocolversion": 70016,
        "localservices": format!("{svc_bits:016x}"),
        "localservicesnames": services_names(svc_bits),
        "localrelay": true,
        "timeoffset": 0,
        "networkactive": true,
        "connections": cin + cout,
        "connections_in": cin,
        "connections_out": cout,
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
            "unbroadcastcount": 0,
            "permitbaremultisig": true,
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
        "maxmempool": mp.max_weight(),
        "mempoolminfee": MempoolHub::relay_fee_btc_per_kb(),
        "minrelaytxfee": MempoolHub::relay_fee_btc_per_kb(),
        "relay_enabled": mp.relay_enabled(),
        "unbroadcastcount": 0,
        "permitbaremultisig": true,
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
        let fee_btc = (fee as f64) / 100_000_000.0;
        let wtxid = mp
            .get_tx(&tid)
            .map(|tx| hash_hex_display(&tx.compute_wtxid().to_byte_array()))
            .unwrap_or_default();
        return Ok(json!({
            "vsize": vsize,
            "weight": weight,
            "wtxid": wtxid,
            "fee": fee_btc,
            "modifiedfee": fee_btc,
            "fees": { "base": fee_btc, "modified": fee_btc },
            "ancestorcount": 1,
            "descendantcount": 1,
        }));
    }
    Err(rpc_error(ERR_MISC, "Transaction not in mempool"))
}

fn getrawtransaction(ctx: &RpcContext, params: &RpcParams) -> Result<Value, Value> {
    params.reject_unknown(&["txid", "verbose"])?;
    let hex = params.req_str(0, "txid")?;
    let verbose = match params.get(1, "verbose") {
        None | Some(Value::Null) => false,
        Some(Value::Bool(b)) => *b,
        Some(v) => json_u64(v)
            .map(|n| n != 0)
            .ok_or_else(|| rpc_error(ERR_INVALID_PARAMS, "verbose must be a bool"))?,
    };
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
        Err(e) => Err(rpc_error(ERR_VERIFY_REJECTED, accept_reject_reason(&e))),
    }
}

fn accept_reject_reason(e: &impl std::fmt::Display) -> String {
    let s = e.to_string();
    if s == "coinbase immature" {
        return "bad-txns-premature-spend-of-coinbase".into();
    }
    if s == "coinbase" {
        return "bad-txns-is-coinbase".into();
    }
    if s.starts_with("missing prevout") {
        return "bad-txns-inputs-missingorspent".into();
    }
    if s.starts_with("duplicate ") {
        return "txn-already-in-mempool".into();
    }
    if s == "inputs-duplicate" {
        return "bad-txns-inputs-duplicate".into();
    }
    if s == "not final" {
        return "bad-txns-nonfinal".into();
    }
    if s == "non-BIP68-final" {
        return "non-BIP68-final".into();
    }
    if s == "rbf insufficient fee" {
        return "insufficient fee".into();
    }
    if let Some(rest) = s.strip_prefix("script: ") {
        return format!("mempool-script-verify-flag-failed ({rest})");
    }
    s
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
                let wtxid = hash_hex_display(&tx.compute_wtxid().to_byte_array());
                out.push(json!({
                    "txid": txid,
                    "wtxid": wtxid,
                    "allowed": true,
                }));
            }
            Err(e) => {
                let wtxid = hash_hex_display(&tx.compute_wtxid().to_byte_array());
                out.push(json!({
                    "txid": txid,
                    "wtxid": wtxid,
                    "allowed": false,
                    "reject-reason": accept_reject_reason(&e),
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

fn require_regtest(ctx: &RpcContext, method: &str) -> Result<(), Value> {
    if ctx.network != Network::Regtest {
        return Err(rpc_error(ERR_MISC, format!("{method} is regtest only")));
    }
    Ok(())
}

fn require_regtest_miner<'a>(
    ctx: &'a RpcContext,
    method: &str,
) -> Result<&'a dyn RpcRegtest, Value> {
    require_regtest(ctx, method)?;
    ctx.regtest
        .as_deref()
        .ok_or_else(|| rpc_error(ERR_MISC, format!("{method} requires a live chain hub")))
}

fn decode_output_script(ctx: &RpcContext, s: &str) -> Result<ScriptBuf, Value> {
    let btc_net = match ctx.network {
        Network::Mainnet => BtcNetwork::Bitcoin,
        Network::Testnet => BtcNetwork::Testnet,
        Network::Signet => BtcNetwork::Signet,
        Network::Regtest => BtcNetwork::Regtest,
    };
    if let Ok(a) = s.parse::<Address<_>>() {
        match a.require_network(btc_net) {
            Ok(addr) => return Ok(addr.script_pubkey()),
            Err(_) => {
                return Err(rpc_error(
                    ERR_INVALID_PARAMS,
                    "address is not valid for this network",
                ));
            }
        }
    }
    let bytes = hex_decode(s).map_err(|e| {
        rpc_error(
            ERR_INVALID_PARAMS,
            format!("output must be an address or hex script: {e}"),
        )
    })?;
    Ok(ScriptBuf::from_bytes(bytes))
}

fn hashes_json(hashes: &[BlockHash]) -> Value {
    json!(hashes.iter().map(|h| h.to_string()).collect::<Vec<_>>())
}

fn mempool_block_txs(ctx: &RpcContext) -> Vec<Transaction> {
    let Some(mp) = ctx.mempool.as_ref() else {
        return Vec::new();
    };
    let txs: Vec<Transaction> = mp.list_live().into_iter().map(|(_, _, _, tx)| tx).collect();
    topo_sort_txs(&txs)
}

fn drain_mempool(ctx: &RpcContext, txs: &[Transaction]) {
    let Some(mp) = ctx.mempool.as_ref() else {
        return;
    };
    let ids: Vec<Txid> = txs.iter().map(Transaction::compute_txid).collect();
    let _ = mp.remove_for_block(&ids);
}

fn topo_sort_txs(txs: &[Transaction]) -> Vec<Transaction> {
    use std::collections::{HashMap, VecDeque};
    if txs.len() <= 1 {
        return txs.to_vec();
    }
    let id_of: HashMap<Txid, usize> = txs
        .iter()
        .enumerate()
        .map(|(i, t)| (t.compute_txid(), i))
        .collect();
    let mut indeg = vec![0usize; txs.len()];
    let mut children = vec![Vec::new(); txs.len()];
    for (i, t) in txs.iter().enumerate() {
        for inp in &t.input {
            if let Some(&p) = id_of.get(&inp.previous_output.txid) {
                children[p].push(i);
                indeg[i] += 1;
            }
        }
    }
    let mut q: VecDeque<usize> = (0..txs.len()).filter(|&i| indeg[i] == 0).collect();
    let mut out = Vec::with_capacity(txs.len());
    while let Some(i) = q.pop_front() {
        out.push(txs[i].clone());
        for &c in &children[i] {
            indeg[c] -= 1;
            if indeg[c] == 0 {
                q.push_back(c);
            }
        }
    }
    if out.len() == txs.len() {
        out
    } else {
        txs.to_vec()
    }
}

fn generate_with_mempool(
    ctx: &RpcContext,
    nblocks: u32,
    script: ScriptBuf,
) -> Result<Value, Value> {
    let miner = require_regtest_miner(ctx, "generate")?;
    let extras = mempool_block_txs(ctx);
    let hashes = miner
        .generate_to_script(nblocks, script, extras.clone())
        .map_err(|e| rpc_error(ERR_MISC, e))?;
    drain_mempool(ctx, &extras);
    Ok(hashes_json(&hashes))
}

fn generatetoaddress(ctx: &RpcContext, params: &RpcParams) -> Result<Value, Value> {
    params.reject_unknown(&["nblocks", "address", "maxtries"])?;
    let _miner = require_regtest_miner(ctx, "generatetoaddress")?;
    let nblocks = params.req_u64(0, "nblocks")? as u32;
    let addr = params.req_str(1, "address")?;
    let _maxtries = params.opt_u64(2, "maxtries")?;
    let script = decode_output_script(ctx, addr)?;
    generate_with_mempool(ctx, nblocks, script)
}

fn generateblock(ctx: &RpcContext, params: &RpcParams) -> Result<Value, Value> {
    params.reject_unknown(&["output", "transactions"])?;
    let miner = require_regtest_miner(ctx, "generateblock")?;
    let output = params.req_str(0, "output")?;
    let script = decode_output_script(ctx, output)?;
    let mut extra = Vec::new();
    if let Some(arr) = params.get_array(1, "transactions") {
        for v in arr {
            let hex = v
                .as_str()
                .ok_or_else(|| rpc_error(ERR_INVALID_PARAMS, "transactions entries must be hex"))?;
            extra.push(decode_tx_hex(hex)?);
        }
    } else if params.get(1, "transactions").is_some() {
        return Err(rpc_error(
            ERR_INVALID_PARAMS,
            "transactions must be an array",
        ));
    } else {
        return Err(rpc_error(ERR_INVALID_PARAMS, "transactions required"));
    }
    let hashes = miner
        .generate_to_script(1, script, extra)
        .map_err(|e| rpc_error(ERR_MISC, e))?;
    let hash = hashes
        .first()
        .ok_or_else(|| rpc_error(ERR_MISC, "generateblock produced no block"))?;
    Ok(json!({ "hash": hash.to_string() }))
}

fn generate(ctx: &RpcContext, params: &RpcParams) -> Result<Value, Value> {
    params.reject_unknown(&["nblocks", "maxtries"])?;
    let _miner = require_regtest_miner(ctx, "generate")?;
    let nblocks = params.req_u64(0, "nblocks")? as u32;
    let _maxtries = params.opt_u64(1, "maxtries")?;
    let script = ScriptBuf::from_bytes(vec![0x51]);
    generate_with_mempool(ctx, nblocks, script)
}

fn generatetodescriptor(ctx: &RpcContext, params: &RpcParams) -> Result<Value, Value> {
    params.reject_unknown(&["num_blocks", "descriptor", "maxtries"])?;
    let _miner = require_regtest_miner(ctx, "generatetodescriptor")?;
    let nblocks = params.req_u64(0, "num_blocks")? as u32;
    let desc = params.req_str(1, "descriptor")?;
    let _maxtries = params.opt_u64(2, "maxtries")?;
    let script = match parse_raw_descriptor(desc) {
        Some(s) => s,
        None => decode_output_script(ctx, desc)?,
    };
    generate_with_mempool(ctx, nblocks, script)
}

/// MiniWallet uses `raw(HEX)#checksum`. Not a full descriptor language.
fn parse_raw_descriptor(desc: &str) -> Option<ScriptBuf> {
    let bare = desc.split('#').next()?.trim();
    let inner = bare.strip_prefix("raw(")?.strip_suffix(")")?;
    let bytes = hex_decode(inner).ok()?;
    Some(ScriptBuf::from_bytes(bytes))
}

/// Enough of Core `scantxoutset` for MiniWallet: `raw(script)` over Class A.
/// Not a coins-DB product (no HD range / combo / addr expansion).
fn scantxoutset(ctx: &RpcContext, params: &RpcParams) -> Result<Value, Value> {
    params.reject_unknown(&["action", "scanobjects"])?;
    let action = params.req_str(0, "action")?;
    match action {
        "status" => return Ok(Value::Null),
        "abort" => return Ok(json!(false)),
        "start" => {}
        other => {
            return Err(rpc_error(
                ERR_INVALID_PARAMETER,
                format!("Invalid action '{other}'"),
            ));
        }
    }
    let objs = params.get_array(1, "scanobjects").ok_or_else(|| {
        rpc_error(
            ERR_MISC,
            "scanobjects argument is required for the start action",
        )
    })?;
    let mut scripts: Vec<Vec<u8>> = Vec::new();
    for o in objs {
        let desc = match o {
            Value::String(s) => s.as_str(),
            Value::Object(m) => m
                .get("desc")
                .and_then(Value::as_str)
                .ok_or_else(|| rpc_error(ERR_INVALID_PARAMS, "scanobject desc required"))?,
            _ => {
                return Err(rpc_error(
                    ERR_INVALID_PARAMS,
                    "scanobjects entries must be descriptor strings",
                ));
            }
        };
        let Some(script) = parse_raw_descriptor(desc) else {
            return Err(rpc_error(
                ERR_INVALID_PARAMS,
                format!("only raw() descriptors are supported (got {desc})"),
            ));
        };
        scripts.push(script.to_bytes());
    }

    let tip = ctx.query.tip_height().unwrap_or(Height(0));
    let best = if let Some(h) = ctx.query.tip_height() {
        ctx.query
            .header_at_height(h)
            .ok()
            .flatten()
            .map(|(_, rec)| hash_hex_display(&rec.hash))
            .unwrap_or_default()
    } else {
        String::new()
    };

    let mut unspents = Vec::new();
    let mut total_sat = 0u64;
    if ctx.query.tip_height().is_some() {
        for h in 0..=tip.0 {
            let block = ctx
                .query
                .reconstruct_block_at_height(Height(h))
                .map_err(|e| rpc_error(ERR_MISC, e.to_string()))?;
            for (ti, tx) in block.txdata.iter().enumerate() {
                let txid = tx.compute_txid();
                let txid_b = txid.to_byte_array();
                let coinbase = ti == 0;
                for (vout, out) in tx.output.iter().enumerate() {
                    let spk = out.script_pubkey.as_bytes();
                    if !scripts.iter().any(|s| s.as_slice() == spk) {
                        continue;
                    }
                    let spent = ctx
                        .query
                        .is_outpoint_spent(&txid_b, vout as u32)
                        .map_err(|e| rpc_error(ERR_MISC, e.to_string()))?;
                    if spent {
                        continue;
                    }
                    let sat = out.value.to_sat();
                    total_sat = total_sat.saturating_add(sat);
                    unspents.push(json!({
                        "txid": txid.to_string(),
                        "vout": vout,
                        "scriptPubKey": hex_encode(spk),
                        "desc": format!("raw({})", hex_encode(spk)),
                        "amount": out.value.to_btc(),
                        "coinbase": coinbase,
                        "height": h,
                    }));
                }
            }
        }
    }

    Ok(json!({
        "success": true,
        "txouts": unspents.len(),
        "height": tip.0,
        "bestblock": best,
        "unspents": unspents,
        "total_amount": Amount::from_sat(total_sat).to_btc(),
    }))
}

fn gettxout(ctx: &RpcContext, params: &RpcParams) -> Result<Value, Value> {
    params.reject_unknown(&["txid", "n", "include_mempool"])?;
    let hex = params.req_str(0, "txid")?;
    let n = params.req_u64(1, "n")? as u32;
    let include_mempool = params.opt_bool(2, "include_mempool")?.unwrap_or(true);
    let want = parse_hash32_display(hex)?;

    if include_mempool {
        if let Some(mp) = ctx.mempool.as_ref() {
            let tid = Txid::from_byte_array(want);
            if let Some(tx) = mp.get_tx(&tid) {
                if let Some(out) = tx.output.get(n as usize) {
                    return Ok(json!({
                        "bestblock": getbestblockhash(ctx)?,
                        "confirmations": 0,
                        "value": out.value.to_btc(),
                        "scriptPubKey": {
                            "hex": hex_encode(out.script_pubkey.as_bytes()),
                            "asm": out.script_pubkey.to_asm_string(),
                        },
                        "coinbase": false,
                    }));
                }
            }
        }
    }

    let (fk, rec) = match ctx
        .query
        .get_tx_by_txid(&want)
        .map_err(|e| rpc_error(ERR_MISC, e.to_string()))?
    {
        Some(v) => v,
        None => return Ok(Value::Null),
    };
    if ctx
        .query
        .is_outpoint_spent(&want, n)
        .map_err(|e| rpc_error(ERR_MISC, e.to_string()))?
    {
        return Ok(Value::Null);
    }
    let out = ctx
        .query
        .tx_output_at_fk(fk, &rec, n)
        .map_err(|e| rpc_error(ERR_MISC, e.to_string()))?;
    let height = ctx
        .query
        .store()
        .tx_height_get(fk)
        .map_err(|e| rpc_error(ERR_MISC, e.to_string()))?
        .unwrap_or(0);
    let tip = ctx.query.tip_height().map(|h| h.0).unwrap_or(0);
    let confs = tip.saturating_sub(height).saturating_add(1);
    let coinbase = rec.input_count == 1
        && ctx
            .query
            .tx_input_at_fk(fk, &rec, 0)
            .map(|inp| inp.is_coinbase())
            .unwrap_or(false);
    Ok(json!({
        "bestblock": getbestblockhash(ctx)?,
        "confirmations": confs,
        "value": Amount::from_sat(out.value as u64).to_btc(),
        "scriptPubKey": {
            "hex": hex_encode(&out.script),
            "asm": ScriptBuf::from_bytes(out.script.clone()).to_asm_string(),
        },
        "coinbase": coinbase,
    }))
}

fn getindexinfo(ctx: &RpcContext, params: &RpcParams) -> Result<Value, Value> {
    params.reject_unknown(&["index_name"])?;
    let tip = ctx.query.tip_height().map(|h| h.0).unwrap_or(0);
    let txindex = json!({
        "synced": true,
        "best_block_height": tip,
    });
    match params.get(0, "index_name") {
        None | Some(Value::Null) => Ok(json!({ "txindex": txindex })),
        Some(Value::String(s)) if s == "txindex" => Ok(json!({ "txindex": txindex })),
        Some(Value::String(_)) => Ok(json!({})),
        Some(_) => Err(rpc_error(ERR_INVALID_PARAMS, "index_name must be a string")),
    }
}

fn getchaintips(ctx: &RpcContext, params: &RpcParams) -> Result<Value, Value> {
    params.reject_unknown(&[])?;
    let Some(h) = ctx.query.tip_height() else {
        return Ok(json!([]));
    };
    let (_, rec) = ctx
        .query
        .header_at_height(h)
        .map_err(|e| rpc_error(ERR_MISC, e.to_string()))?
        .ok_or_else(|| rpc_error(ERR_MISC, "tip header missing"))?;
    Ok(json!([{
        "height": h.0,
        "hash": hash_hex_display(&rec.hash),
        "branchlen": 0,
        "status": "active",
    }]))
}

fn wait_timeout_ms(params: &RpcParams, idx: usize, name: &str) -> Result<u64, Value> {
    Ok(params.opt_u64(idx, name)?.unwrap_or(30_000))
}

fn tip_hash_height(ctx: &RpcContext) -> Result<(String, u32), Value> {
    let h = ctx
        .query
        .tip_height()
        .ok_or_else(|| rpc_error(ERR_MISC, "no tip"))?;
    let (_, rec) = ctx
        .query
        .header_at_height(h)
        .map_err(|e| rpc_error(ERR_MISC, e.to_string()))?
        .ok_or_else(|| rpc_error(ERR_MISC, "tip header missing"))?;
    Ok((hash_hex_display(&rec.hash), h.0))
}

fn waitforblock(ctx: &RpcContext, params: &RpcParams) -> Result<Value, Value> {
    params.reject_unknown(&["blockhash", "timeout"])?;
    let want = params.req_str(0, "blockhash")?.to_string();
    let timeout_ms = wait_timeout_ms(params, 1, "timeout")?;
    let deadline = Instant::now() + std::time::Duration::from_millis(timeout_ms);
    loop {
        let (hash, height) = tip_hash_height(ctx)?;
        if hash == want {
            return Ok(json!({ "hash": hash, "height": height }));
        }
        if Instant::now() >= deadline {
            return Ok(json!({ "hash": hash, "height": height }));
        }
        std::thread::sleep(std::time::Duration::from_millis(50));
    }
}

fn waitforblockheight(ctx: &RpcContext, params: &RpcParams) -> Result<Value, Value> {
    params.reject_unknown(&["height", "timeout"])?;
    let want = params.req_u64(0, "height")? as u32;
    let timeout_ms = wait_timeout_ms(params, 1, "timeout")?;
    let deadline = Instant::now() + std::time::Duration::from_millis(timeout_ms);
    loop {
        let (hash, height) = tip_hash_height(ctx)?;
        if height >= want {
            return Ok(json!({ "hash": hash, "height": height }));
        }
        if Instant::now() >= deadline {
            return Ok(json!({ "hash": hash, "height": height }));
        }
        std::thread::sleep(std::time::Duration::from_millis(50));
    }
}

fn waitfornewblock(ctx: &RpcContext, params: &RpcParams) -> Result<Value, Value> {
    params.reject_unknown(&["timeout"])?;
    let timeout_ms = wait_timeout_ms(params, 0, "timeout")?;
    let (start_hash, _) = tip_hash_height(ctx)?;
    let deadline = Instant::now() + std::time::Duration::from_millis(timeout_ms);
    loop {
        let (hash, height) = tip_hash_height(ctx)?;
        if hash != start_hash {
            return Ok(json!({ "hash": hash, "height": height }));
        }
        if Instant::now() >= deadline {
            return Ok(json!({ "hash": hash, "height": height }));
        }
        std::thread::sleep(std::time::Duration::from_millis(50));
    }
}

fn setmocktime(ctx: &RpcContext, params: &RpcParams) -> Result<Value, Value> {
    params.reject_unknown(&["timestamp"])?;
    require_regtest(ctx, "setmocktime")?;
    let miner = require_regtest_miner(ctx, "setmocktime")?;
    let raw = params.req(0, "timestamp")?;
    let ts = mocktime_i64(raw)?;
    miner
        .set_mock_time(ts)
        .map_err(|e| rpc_error(ERR_MISC, e))?;
    Ok(Value::Null)
}

fn mocktime_i64(v: &Value) -> Result<i64, Value> {
    let n = match v {
        Value::Number(n) => n,
        _ => {
            return Err(rpc_error(
                ERR_INVALID_PARAMETER,
                "timestamp must be an integer",
            ));
        }
    };
    let i = n
        .as_i64()
        .or_else(|| n.as_u64().and_then(|u| i64::try_from(u).ok()));
    let Some(i) = i else {
        return Err(rpc_error(
            ERR_INVALID_PARAMETER,
            "timestamp must be an integer",
        ));
    };
    if i < 0 || i > 9_223_372_036 {
        return Err(rpc_error(
            ERR_INVALID_PARAMETER,
            format!("Mocktime must be in the range [0, 9223372036], not {i}."),
        ));
    }
    Ok(i)
}

fn require_chain(ctx: &RpcContext) -> Result<&rbitcoin_net::ChainHub, Value> {
    ctx.chain
        .as_deref()
        .ok_or_else(|| rpc_error(ERR_MISC, "chain hub not attached"))
}

fn parse_blockhash_param(params: &RpcParams) -> Result<bitcoin::BlockHash, Value> {
    let hex = params.req_str(0, "blockhash")?;
    let b = parse_hash32_display(hex)?;
    Ok(bitcoin::BlockHash::from_byte_array(b))
}

fn invalidateblock(ctx: &RpcContext, params: &RpcParams) -> Result<Value, Value> {
    params.reject_unknown(&["blockhash"])?;
    let hub = require_chain(ctx)?;
    let hash = parse_blockhash_param(params)?;
    hub.invalidate_block(hash)
        .map_err(|e| rpc_error(ERR_MISC, e.to_string()))?;
    Ok(Value::Null)
}

fn reconsiderblock(ctx: &RpcContext, params: &RpcParams) -> Result<Value, Value> {
    params.reject_unknown(&["blockhash"])?;
    let hub = require_chain(ctx)?;
    let hash = parse_blockhash_param(params)?;
    hub.reconsider_block(hash)
        .map_err(|e| rpc_error(ERR_MISC, e.to_string()))?;
    Ok(Value::Null)
}

fn preciousblock(ctx: &RpcContext, params: &RpcParams) -> Result<Value, Value> {
    params.reject_unknown(&["blockhash"])?;
    let hub = require_chain(ctx)?;
    let hash = parse_blockhash_param(params)?;
    hub.precious_block(hash)
        .map_err(|e| rpc_error(ERR_MISC, e.to_string()))?;
    Ok(Value::Null)
}

fn submitblock(ctx: &RpcContext, params: &RpcParams) -> Result<Value, Value> {
    params.reject_unknown(&["hexdata", "dummy"])?;
    let miner = require_regtest_miner(ctx, "submitblock")?;
    let hex = params.req_str(0, "hexdata")?;
    let raw = hex_decode(hex).map_err(|e| rpc_error(ERR_INVALID_PARAMS, e.to_string()))?;
    let block: Block = deserialize(&raw)
        .map_err(|e| rpc_error(ERR_INVALID_PARAMS, format!("block decode: {e}")))?;
    match miner.submit_block(block) {
        SubmitBlockOutcome::Accepted => Ok(Value::Null),
        SubmitBlockOutcome::Duplicate => Ok(json!("duplicate")),
        SubmitBlockOutcome::IgnoredWeaker => Ok(json!("inconclusive")),
        SubmitBlockOutcome::Rejected(reason) => Ok(json!(reason)),
    }
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
            regtest: None,
            peers: None,
            chain: None,
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
        // Documented placeholders — not computed (Q-15).
        assert_eq!(info["chainwork"], "");
        assert_eq!(info["size_on_disk"], 0);
        assert_eq!(info["verificationprogress"], 1.0);
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
        let e = dispatch(&ctx, "gettxoutsetinfo", vec![]).unwrap_err();
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
    fn echo_positional_named_and_mixed_args() {
        let (ctx, dir) = ctx_empty();

        let empty = handle_request(&ctx, &json!({"id":1,"method":"echo","params":[]}));
        assert!(empty["error"].is_null(), "{empty}");
        assert_eq!(empty["result"], json!([]));

        let named = handle_request(&ctx, &json!({"method":"echo","params":{"arg0":0,"arg9":9}}));
        assert!(named["error"].is_null(), "{named}");
        let mut want = vec![Value::Null; 10];
        want[0] = json!(0);
        want[9] = json!(9);
        assert_eq!(named["result"], Value::Array(want));

        let arg1 = handle_request(&ctx, &json!({"method":"echo","params":{"arg1":1}}));
        assert_eq!(arg1["result"], json!([Value::Null, 1]));

        let arg9_null = handle_request(&ctx, &json!({"method":"echo","params":{"arg9":null}}));
        assert_eq!(arg9_null["result"], json!(vec![Value::Null; 10]));

        // AuthServiceProxy mixed: echo(0, 1, arg3=3, arg5=5)
        let mixed = handle_request(
            &ctx,
            &json!({"method":"echo","params":{"args":[0,1],"arg3":3,"arg5":5}}),
        );
        assert!(mixed["error"].is_null(), "{mixed}");
        assert_eq!(
            mixed["result"],
            json!([0, 1, Value::Null, 3, Value::Null, 5])
        );

        let twice = handle_request(
            &ctx,
            &json!({"method":"echo","params":{"args":[0,1],"arg1":1}}),
        );
        assert_eq!(twice["error"]["code"], ERR_INVALID_PARAMETER);
        assert!(
            twice["error"]["message"]
                .as_str()
                .unwrap()
                .contains("specified twice"),
            "{twice}"
        );

        let twice_null = handle_request(
            &ctx,
            &json!({"method":"echo","params":{"args":[0,null,2],"arg1":1}}),
        );
        assert_eq!(twice_null["error"]["code"], ERR_INVALID_PARAMETER);

        // Mixed positional `args` must feed getblockhash(height).
        let gh = handle_request(
            &ctx,
            &json!({"method":"getblockhash","params":{"args":[0]}}),
        );
        assert_ne!(
            gh["error"]["message"].as_str().unwrap_or(""),
            "height required"
        );
        assert_eq!(gh["error"]["code"], ERR_MISC);

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
            "syncwithvalidationinterfacequeue",
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
            regtest: None,
            peers: None,
            chain: None,
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
            regtest: None,
            peers: None,
            chain: None,
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

    struct TestMiner(Arc<rbitcoin_net::ChainHub>);

    impl RpcRegtest for TestMiner {
        fn generate_to_script(
            &self,
            nblocks: u32,
            script_pubkey: ScriptBuf,
            extra_txs: Vec<Transaction>,
        ) -> Result<Vec<BlockHash>, String> {
            self.0
                .generate_to_script(nblocks, script_pubkey, extra_txs)
                .map_err(|e| e.to_string())
        }

        fn submit_block(&self, block: Block) -> SubmitBlockOutcome {
            match self.0.accept_received_block(block) {
                Ok(rbitcoin_net::AcceptOutcome::Accepted { .. }) => SubmitBlockOutcome::Accepted,
                Ok(rbitcoin_net::AcceptOutcome::AlreadyHave) => SubmitBlockOutcome::Duplicate,
                Ok(rbitcoin_net::AcceptOutcome::IgnoredWeaker) => SubmitBlockOutcome::IgnoredWeaker,
                Err(e) => SubmitBlockOutcome::Rejected(e.to_string()),
            }
        }

        fn set_mock_time(&self, timestamp: i64) -> Result<(), String> {
            self.0.clock.set_mock(timestamp);
            Ok(())
        }
    }

    fn ctx_regtest_hub() -> (RpcContext, PathBuf, Arc<rbitcoin_net::ChainHub>) {
        use rbitcoin_consensus::{ChainParams, Milestone};
        let n = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("rbitcoin-rpc-gen-{n}"));
        std::fs::create_dir_all(&dir).unwrap();
        let hub = Arc::new(rbitcoin_net::ChainHub::new(
            Query::open_or_create(dir.join("store")).unwrap(),
            ChainParams::regtest(),
            Milestone::NONE,
        ));
        hub.ensure_genesis().unwrap();
        let mp = MempoolHub::open_with_weight(dir.join("mempool"), hub.query.clone(), 300_000_000)
            .unwrap();
        mp.set_relay_enabled(true);
        let ctx = RpcContext {
            query: hub.query.clone(),
            mempool: Some(mp),
            network: Network::Regtest,
            start: Instant::now(),
            stop: Arc::new(AtomicBool::new(false)),
            connections: Arc::new(AtomicU64::new(0)),
            initial_block_download: Arc::new(AtomicBool::new(false)),
            subversion: "/rbitcoin:0.1.0/".into(),
            regtest: Some(Arc::new(TestMiner(Arc::clone(&hub)))),
            peers: None,
            chain: Some(Arc::clone(&hub)),
        };
        (ctx, dir, hub)
    }

    fn p2wpkh_regtest() -> (String, ScriptBuf) {
        use bitcoin::hashes::Hash;
        use bitcoin::{Address, WPubkeyHash};
        let wpkh = WPubkeyHash::from_byte_array([0x75; 20]);
        let script = ScriptBuf::new_p2wpkh(&wpkh);
        let addr = Address::from_script(&script, BtcNetwork::Regtest)
            .expect("p2wpkh script is a valid address");
        (addr.to_string(), script)
    }

    #[test]
    fn generate_refuses_on_mainnet() {
        let (mut ctx, dir, _hub) = ctx_regtest_hub();
        ctx.network = Network::Mainnet;
        let (addr, _) = p2wpkh_regtest();
        for m in [
            "generatetoaddress",
            "generateblock",
            "generate",
            "submitblock",
            "setmocktime",
        ] {
            let e = match m {
                "generatetoaddress" => dispatch(&ctx, m, vec![json!(1), json!(addr.clone())]),
                "generateblock" => dispatch(&ctx, m, vec![json!(addr.clone()), json!([])]),
                "generate" => dispatch(&ctx, m, vec![json!(1)]),
                "setmocktime" => dispatch(&ctx, m, vec![json!(1)]),
                _ => dispatch(&ctx, m, vec![json!("00")]),
            }
            .unwrap_err();
            let msg = e["message"].as_str().unwrap_or("");
            assert!(
                msg.contains("regtest only"),
                "{m} must refuse on mainnet: {e}"
            );
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn generate_one_to_p2wpkh() {
        let (ctx, dir, _hub) = ctx_regtest_hub();
        let (addr, script) = p2wpkh_regtest();
        let hashes = dispatch(&ctx, "generatetoaddress", vec![json!(1), json!(addr)]).unwrap();
        let arr = hashes.as_array().expect("hash array");
        assert_eq!(arr.len(), 1);
        assert_eq!(dispatch(&ctx, "getblockcount", vec![]).unwrap(), json!(1));
        let best = dispatch(&ctx, "getbestblockhash", vec![]).unwrap();
        assert_eq!(best, arr[0]);
        let blk = dispatch(&ctx, "getblock", vec![best.clone(), json!(2)]).unwrap();
        let hex = blk["tx"][0]["vout"][0]["scriptPubKey"]["hex"]
            .as_str()
            .unwrap();
        assert_eq!(hex, rbitcoin_primitives::hex_encode(script.as_bytes()));
        // Core getblock(hash, False) is verbosity 0 (raw hex).
        let raw = dispatch(&ctx, "getblock", vec![best.clone(), json!(false)]).unwrap();
        assert!(raw.as_str().unwrap().len() > 160);
        let cb_txid = blk["tx"][0]["txid"].as_str().unwrap();
        let raw_tx = dispatch(&ctx, "getrawtransaction", vec![json!(cb_txid), json!(0)]).unwrap();
        assert!(raw_tx.as_str().unwrap().len() > 20);
        let net = dispatch(&ctx, "getnetworkinfo", vec![]).unwrap();
        assert_eq!(net["connections_in"], 0);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn miniwallet_raw_scan_and_gettxout() {
        let (ctx, dir, _hub) = ctx_regtest_hub();
        let desc = "raw(51)";
        let hashes = dispatch(&ctx, "generatetodescriptor", vec![json!(2), json!(desc)]).unwrap();
        assert_eq!(hashes.as_array().unwrap().len(), 2);
        assert_eq!(dispatch(&ctx, "getblockcount", vec![]).unwrap(), json!(2));

        let scan = dispatch(&ctx, "scantxoutset", vec![json!("start"), json!([desc])]).unwrap();
        assert_eq!(scan["success"], true);
        assert_eq!(scan["height"], 2);
        let uns = scan["unspents"].as_array().unwrap();
        assert_eq!(uns.len(), 2, "two generated OP_TRUE coinbases: {scan}");
        assert!(uns.iter().all(|u| u["coinbase"] == true));

        let txid = uns[1]["txid"].as_str().unwrap();
        let utxo = dispatch(&ctx, "gettxout", vec![json!(txid), json!(0)]).unwrap();
        assert!(utxo["confirmations"].as_u64().unwrap() >= 1);
        assert_eq!(utxo["coinbase"], true);
        assert_eq!(utxo["scriptPubKey"]["hex"], "51");

        let tips = dispatch(&ctx, "getchaintips", vec![]).unwrap();
        assert_eq!(tips[0]["status"], "active");
        assert_eq!(tips[0]["height"], 2);

        let waited = dispatch(&ctx, "waitforblockheight", vec![json!(2), json!(100)]).unwrap();
        assert_eq!(waited["height"], 2);

        let idx = dispatch(&ctx, "getindexinfo", vec![]).unwrap();
        assert_eq!(idx["txindex"]["synced"], true);
        assert_eq!(idx["txindex"]["best_block_height"], 2);
        let only = dispatch(&ctx, "getindexinfo", vec![json!("txindex")]).unwrap();
        assert_eq!(only["txindex"]["synced"], true);
        let empty = dispatch(&ctx, "getindexinfo", vec![json!("coinstatsindex")]).unwrap();
        assert_eq!(empty, json!({}));

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn generate_includes_mempool_and_maps_immature() {
        use bitcoin::absolute::LockTime;
        use bitcoin::consensus::encode::serialize;
        use bitcoin::transaction::Version as TxVersion;
        use bitcoin::{OutPoint, Sequence, Transaction, TxIn, TxOut, Witness};

        let (ctx, dir, _hub) = ctx_regtest_hub();
        dispatch(&ctx, "generate", vec![json!(101)]).unwrap();
        assert_eq!(dispatch(&ctx, "getblockcount", vec![]).unwrap(), json!(101));

        let hash1 = dispatch(&ctx, "getblockhash", vec![json!(1)]).unwrap();
        let blk = dispatch(&ctx, "getblock", vec![hash1, json!(2)]).unwrap();
        let cb_txid = blk["tx"][0]["txid"].as_str().unwrap();
        let cb_val =
            (blk["tx"][0]["vout"][0]["value"].as_f64().unwrap() * 100_000_000.0).round() as u64;

        let spend = Transaction {
            version: TxVersion::TWO,
            lock_time: LockTime::ZERO,
            input: vec![TxIn {
                previous_output: OutPoint {
                    txid: Txid::from_byte_array(parse_hash32_display(cb_txid).unwrap()),
                    vout: 0,
                },
                script_sig: ScriptBuf::new(),
                sequence: Sequence::MAX,
                witness: Witness::new(),
            }],
            output: vec![TxOut {
                value: Amount::from_sat(cb_val - 1_000),
                script_pubkey: ScriptBuf::from_bytes(vec![0x51]),
            }],
        };
        let hex = hex_encode(serialize(&spend));
        let tid = dispatch(&ctx, "sendrawtransaction", vec![json!(hex)]).unwrap();
        let pool = dispatch(&ctx, "getrawmempool", vec![]).unwrap();
        assert_eq!(pool, json!([tid]));

        dispatch(&ctx, "generate", vec![json!(1)]).unwrap();
        let empty = dispatch(&ctx, "getrawmempool", vec![]).unwrap();
        assert_eq!(empty, json!([]));
        let tip = dispatch(&ctx, "getbestblockhash", vec![]).unwrap();
        let mined = dispatch(&ctx, "getblock", vec![tip, json!(1)]).unwrap();
        assert_eq!(mined["tx"].as_array().unwrap().len(), 2);

        // At tip 102, coinbase N is mempool-mature when 102 >= N+99 → N<=3.
        let hash2 = dispatch(&ctx, "getblockhash", vec![json!(10)]).unwrap();
        let blk2 = dispatch(&ctx, "getblock", vec![hash2, json!(2)]).unwrap();
        let immature_txid = blk2["tx"][0]["txid"].as_str().unwrap();
        let immature_val =
            (blk2["tx"][0]["vout"][0]["value"].as_f64().unwrap() * 100_000_000.0).round() as u64;
        let bad = Transaction {
            version: TxVersion::TWO,
            lock_time: LockTime::ZERO,
            input: vec![TxIn {
                previous_output: OutPoint {
                    txid: Txid::from_byte_array(parse_hash32_display(immature_txid).unwrap()),
                    vout: 0,
                },
                script_sig: ScriptBuf::new(),
                sequence: Sequence::MAX,
                witness: Witness::new(),
            }],
            output: vec![TxOut {
                value: Amount::from_sat(immature_val - 1_000),
                script_pubkey: ScriptBuf::from_bytes(vec![0x51]),
            }],
        };
        let e = dispatch(
            &ctx,
            "sendrawtransaction",
            vec![json!(hex_encode(serialize(&bad)))],
        )
        .unwrap_err();
        assert_eq!(e["code"], ERR_VERIFY_REJECTED);
        assert_eq!(e["message"], "bad-txns-premature-spend-of-coinbase");

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn submitblock_good_and_bad_merkle() {
        use bitcoin::consensus::encode::serialize;
        use rbitcoin_consensus::mine_regtest_paying;

        let (ctx, dir, hub) = ctx_regtest_hub();
        let (_, script) = p2wpkh_regtest();
        let prev = hub.tip_hash().unwrap();
        let time = hub.tip_header().unwrap().time + 1;
        let good = mine_regtest_paying(prev, time, 1, script.clone(), vec![]);
        let good_hex = rbitcoin_primitives::hex_encode(serialize(&good));
        let r = dispatch(&ctx, "submitblock", vec![json!(good_hex)]).unwrap();
        assert!(r.is_null(), "good submitblock: {r}");
        assert_eq!(dispatch(&ctx, "getblockcount", vec![]).unwrap(), json!(1));

        let prev2 = hub.tip_hash().unwrap();
        let time2 = hub.tip_header().unwrap().time + 1;
        let mut bad = mine_regtest_paying(prev2, time2, 2, script, vec![]);
        bad.header.merkle_root = bitcoin::TxMerkleNode::from_byte_array([0xab; 32]);
        // Re-grind so we fail on merkle, not PoW.
        let target = bitcoin::Target::from_compact(bad.header.bits);
        for nonce in 0..u32::MAX {
            bad.header.nonce = nonce;
            if bad.header.validate_pow(target).is_ok() {
                break;
            }
        }
        let bad_hex = rbitcoin_primitives::hex_encode(serialize(&bad));
        let r = dispatch(&ctx, "submitblock", vec![json!(bad_hex)]).unwrap();
        assert!(!r.is_null(), "bad merkle must not be accepted: {r}");
        let msg = r.as_str().unwrap_or("");
        assert!(
            msg.to_ascii_lowercase().contains("merkle")
                || msg.contains("consensus")
                || msg.contains("bad"),
            "bad merkle reject: {r}"
        );
        assert_eq!(dispatch(&ctx, "getblockcount", vec![]).unwrap(), json!(1));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn invalidate_reconsider_tip() {
        let (ctx, dir, _hub) = ctx_regtest_hub();
        let (addr, _) = p2wpkh_regtest();
        dispatch(&ctx, "generatetoaddress", vec![json!(3), json!(addr)]).unwrap();
        assert_eq!(dispatch(&ctx, "getblockcount", vec![]).unwrap(), json!(3));
        let tip = dispatch(&ctx, "getbestblockhash", vec![])
            .unwrap()
            .as_str()
            .unwrap()
            .to_string();
        dispatch(&ctx, "invalidateblock", vec![json!(tip.clone())]).unwrap();
        assert_eq!(dispatch(&ctx, "getblockcount", vec![]).unwrap(), json!(2));
        dispatch(&ctx, "reconsiderblock", vec![json!(tip)]).unwrap();
        assert_eq!(dispatch(&ctx, "getblockcount", vec![]).unwrap(), json!(3));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn setmocktime_negative_is_invalid_parameter() {
        let (ctx, dir, _hub) = ctx_regtest_hub();
        let e = dispatch(&ctx, "setmocktime", vec![json!(-1)]).unwrap_err();
        assert_eq!(e["code"], ERR_INVALID_PARAMETER);
        assert!(
            e["message"]
                .as_str()
                .unwrap()
                .contains("Mocktime must be in the range [0, 9223372036], not -1."),
            "{e}"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn setmocktime_generate_uses_mock() {
        let (ctx, dir, _hub) = ctx_regtest_hub();
        let mock = 1_600_000_000u64;
        dispatch(&ctx, "setmocktime", vec![json!(mock)]).unwrap();
        let (addr, _) = p2wpkh_regtest();
        dispatch(&ctx, "generatetoaddress", vec![json!(1), json!(addr)]).unwrap();
        let best = dispatch(&ctx, "getbestblockhash", vec![]).unwrap();
        let hdr = dispatch(&ctx, "getblockheader", vec![best]).unwrap();
        let t = hdr["time"].as_u64().unwrap();
        assert!(
            t >= mock && t < mock + 600,
            "generate time {t} should honor mock {mock}"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn setmocktime_future_header_uses_mock() {
        use bitcoin::consensus::encode::serialize;
        use rbitcoin_consensus::mine_regtest_paying;

        let (ctx, dir, hub) = ctx_regtest_hub();
        let mock = 1_600_000_000u64;
        dispatch(&ctx, "setmocktime", vec![json!(mock)]).unwrap();
        let (_, script) = p2wpkh_regtest();
        let prev = hub.tip_hash().unwrap();
        let far = (mock + 3 * 3600) as u32;
        let far_block = mine_regtest_paying(prev, far, 1, script, vec![]);
        let hex = rbitcoin_primitives::hex_encode(serialize(&far_block));
        let r = dispatch(&ctx, "submitblock", vec![json!(hex)]).unwrap();
        let msg = r.as_str().unwrap_or("");
        assert!(
            msg.contains("future") || msg.contains("timestamp"),
            "future header vs mock now: {r}"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn getpeerinfo_empty_without_hub() {
        let (ctx, dir) = ctx_empty();
        let r = dispatch(&ctx, "getpeerinfo", vec![]).unwrap();
        assert_eq!(r, json!([]));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn getpeerinfo_lists_registered_session() {
        use bitcoin::p2p::address::Address;
        use bitcoin::p2p::message_network::VersionMessage;
        use bitcoin::p2p::ServiceFlags;
        use rbitcoin_net::{PeerConnType, PeerHub};
        use std::net::{IpAddr, Ipv4Addr, SocketAddr};

        let (mut ctx, dir) = ctx_empty();
        let hub = PeerHub::new();
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 18444);
        let bind = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 18445);
        let ver = VersionMessage {
            version: 70016,
            services: ServiceFlags::NETWORK | ServiceFlags::WITNESS | ServiceFlags::P2P_V2,
            timestamp: 0,
            receiver: Address::new(&addr, ServiceFlags::NONE),
            sender: Address::new(&bind, ServiceFlags::NONE),
            nonce: 1,
            user_agent: "/rbitcoin:0.1.0(testnode0)/".into(),
            start_height: 0,
            relay: true,
        };
        let live = hub.register(addr, bind, &ver, false, PeerConnType::OutboundFullRelay);
        live.note_recv("pong", 8);
        ctx.peers = Some(hub);
        let r = dispatch(&ctx, "getpeerinfo", vec![]).unwrap();
        let arr = r.as_array().unwrap();
        assert_eq!(arr.len(), 1);
        assert_eq!(arr[0]["subver"], "/rbitcoin:0.1.0(testnode0)/");
        assert_eq!(arr[0]["inbound"], false);
        assert_eq!(arr[0]["addr"], "127.0.0.1:18444");
        assert!(arr[0]["bytesrecv_per_msg"]["pong"].as_u64().unwrap() >= 29);
        let net = dispatch(&ctx, "getnetworkinfo", vec![]).unwrap();
        assert_eq!(net["connections_in"], 0);
        assert_eq!(net["connections_out"], 1);
        assert_eq!(net["connections"], 1);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn addnode_and_disconnectnode_on_table() {
        use rbitcoin_net::PeerHub;
        let (mut ctx, dir) = ctx_empty();
        let hub = PeerHub::new();
        ctx.peers = Some(hub);
        let e = dispatch(&ctx, "addnode", vec![json!("127.0.0.1:1"), json!("onetry")]).unwrap_err();
        assert!(e["message"].as_str().unwrap().contains("dialer"), "{e}");
        let e = dispatch(&ctx, "disconnectnode", vec![json!("127.0.0.1:1")]).unwrap_err();
        assert_eq!(e["code"], ERR_CLIENT_NODE_NOT_CONNECTED);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn addnode_two_nodes_see_each_other() {
        use rbitcoin_consensus::{ChainParams, Milestone};
        use rbitcoin_net::P2PNode;
        use rbitcoin_query::Query;

        let n = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("rbitcoin-rpc-2n-{n}"));
        std::fs::create_dir_all(dir.join("a")).unwrap();
        std::fs::create_dir_all(dir.join("b")).unwrap();
        let qa = Query::open_or_create(dir.join("a/store")).unwrap();
        let qb = Query::open_or_create(dir.join("b/store")).unwrap();
        let params = ChainParams::regtest();
        let na = P2PNode::start_with_agent(
            "127.0.0.1:0".parse().unwrap(),
            qa,
            params.clone(),
            Milestone::NONE,
            "/rbitcoin:0.1.0(testnode0)/".into(),
            rbitcoin_net::DEFAULT_MAX_INBOUND,
        )
        .await
        .unwrap();
        let nb = P2PNode::start_with_agent(
            "127.0.0.1:0".parse().unwrap(),
            qb,
            params,
            Milestone::NONE,
            "/rbitcoin:0.1.0(testnode1)/".into(),
            rbitcoin_net::DEFAULT_MAX_INBOUND,
        )
        .await
        .unwrap();
        let (mut ctx_a, _d0) = ctx_empty();
        ctx_a.peers = Some(Arc::clone(&na.peers));
        let (mut ctx_b, _d1) = ctx_empty();
        ctx_b.peers = Some(Arc::clone(&nb.peers));
        let baddr = nb.local_addr.to_string();
        dispatch(&ctx_a, "addnode", vec![json!(baddr), json!("onetry")]).unwrap();
        let mut saw = false;
        for _ in 0..80 {
            let pa = dispatch(&ctx_a, "getpeerinfo", vec![]).unwrap();
            let pb = dispatch(&ctx_b, "getpeerinfo", vec![]).unwrap();
            let a_ok = pa.as_array().is_some_and(|a| {
                a.iter()
                    .any(|p| p["subver"] == "/rbitcoin:0.1.0(testnode1)/" && p["inbound"] == false)
            });
            let b_ok = pb.as_array().is_some_and(|a| {
                a.iter()
                    .any(|p| p["subver"] == "/rbitcoin:0.1.0(testnode0)/" && p["inbound"] == true)
            });
            if a_ok && b_ok {
                saw = true;
                let pong = pa[0]["bytesrecv_per_msg"]["pong"].as_u64().unwrap_or(0);
                assert!(pong >= 29, "pong bytes {pong}");
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert!(saw, "two nodes must see each other via addnode");
        na.shutdown().await;
        // nb moved? keep drop
        let _ = nb;
        let _ = std::fs::remove_dir_all(&dir);
        let _ = std::fs::remove_dir_all(_d0);
        let _ = std::fs::remove_dir_all(_d1);
    }

    #[test]
    fn rpc_honesty_mempool_budget_and_network_identity() {
        let n = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("rbitcoin-rpc-honest-{n}"));
        std::fs::create_dir_all(&dir).unwrap();
        let q = Arc::new(Query::open_or_create(dir.join("store")).unwrap());
        let mp =
            MempoolHub::open_with_weight(dir.join("mempool"), Arc::clone(&q), 50_000_000).unwrap();
        let ctx = RpcContext {
            query: q,
            mempool: Some(mp),
            network: Network::Regtest,
            start: Instant::now(),
            stop: Arc::new(AtomicBool::new(false)),
            connections: Arc::new(AtomicU64::new(0)),
            initial_block_download: Arc::new(AtomicBool::new(false)),
            subversion: "/rbitcoin:0.1.0/".into(),
            regtest: None,
            peers: None,
            chain: None,
        };
        let mem = dispatch(&ctx, "getmempoolinfo", vec![]).unwrap();
        assert_eq!(
            mem["maxmempool"].as_u64(),
            Some(50_000_000),
            "maxmempool must be the hub weight budget, not a hardcoded 300M"
        );
        let net = dispatch(&ctx, "getnetworkinfo", vec![]).unwrap();
        assert_ne!(
            net["version"].as_u64(),
            Some(270000),
            "must not impersonate Bitcoin Core 27.0"
        );
        assert_eq!(
            net["version"].as_u64(),
            Some(rpc_client_version(env!("CARGO_PKG_VERSION"))),
        );
        assert_eq!(
            rpc_client_version("0.1.0"),
            100,
            "0.1.0 is major*10000+minor*100+patch (not 10000, which is 1.0.0)"
        );
        let flags = rbitcoin_net::local_service_flags();
        let bits = flags.to_u64();
        let hex = format!("{bits:016x}");
        assert_eq!(net["localservices"].as_str(), Some(hex.as_str()));
        let names = net["localservicesnames"].as_array().unwrap();
        let names: Vec<&str> = names.iter().filter_map(|v| v.as_str()).collect();
        assert!(names.contains(&"NETWORK"));
        assert!(names.contains(&"WITNESS"));
        assert!(names.contains(&"P2P_V2"));
        assert!(
            !names.contains(&"NETWORK_LIMITED"),
            "we do not advertise NETWORK_LIMITED: {names:?}"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }
}
