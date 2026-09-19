use super::*;
use bitcoin::address::AddressType;
use bitcoin::consensus::deserialize;
use bitcoin::{Address, ScriptBuf};
use std::str::FromStr;

pub(crate) fn decoderawtransaction(ctx: &RpcContext, params: &RpcParams) -> Result<Value, Value> {
    params.reject_unknown(&["hexstring", "iswitness"])?;
    let hex = params.req_str(0, "hexstring")?;
    let force_witness = params.opt_bool(1, "iswitness")?;
    let raw = hex_decode(hex).map_err(|_| rpc_error(ERR_DESERIALIZATION, "TX decode failed"))?;
    if force_witness == Some(false) && raw.len() >= 6 && raw[4] == 0x00 && raw[5] == 0x01 {
        return Err(rpc_error(ERR_DESERIALIZATION, "TX decode failed"));
    }
    let tx: Transaction =
        deserialize(&raw).map_err(|_| rpc_error(ERR_DESERIALIZATION, "TX decode failed"))?;
    Ok(tx_to_json(&tx, None, rpc_btc_network(ctx.network)))
}

pub(crate) fn decodescript(ctx: &RpcContext, params: &RpcParams) -> Result<Value, Value> {
    params.reject_unknown(&["hexstring"])?;
    let hex = params.req_str(0, "hexstring")?;
    let raw = hex_decode(hex).map_err(|e| rpc_error(ERR_INVALID_PARAMS, e.to_string()))?;
    Ok(script_pubkey_json(&ScriptBuf::from_bytes(raw), rpc_btc_network(ctx.network)))
}

pub(crate) fn validateaddress(ctx: &RpcContext, params: &RpcParams) -> Result<Value, Value> {
    params.reject_unknown(&["address"])?;
    let s = params.req_str(0, "address")?;
    let parsed = match Address::from_str(s) {
        Ok(a) => a,
        Err(_) => return Ok(json!({ "isvalid": false })),
    };
    let addr = match parsed.require_network(rpc_btc_network(ctx.network)) {
        Ok(a) => a,
        Err(_) => return Ok(json!({ "isvalid": false })),
    };
    let spk = addr.script_pubkey();
    let (isscript, iswitness) = match addr.address_type() {
        Some(AddressType::P2pkh) => (false, false),
        Some(AddressType::P2sh) => (true, false),
        Some(AddressType::P2wpkh | AddressType::P2tr | AddressType::P2a) => (false, true),
        Some(AddressType::P2wsh) => (true, true),
        Some(_) | None => (false, addr.witness_program().is_some()),
    };
    let mut obj = json!({
        "isvalid": true,
        "address": addr.to_string(),
        "scriptPubKey": hex_encode(spk.as_bytes()),
        "isscript": isscript,
        "iswitness": iswitness,
    });
    if let Some(wp) = addr.witness_program() {
        if let Some(m) = obj.as_object_mut() {
            m.insert("witness_version".into(), json!(wp.version().to_num()));
            m.insert("witness_program".into(), json!(hex_encode(wp.program().as_bytes())));
        }
    }
    Ok(obj)
}
