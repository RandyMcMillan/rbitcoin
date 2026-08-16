//! Stateless raw-tx RPCs: `createrawtransaction`, `signrawtransactionwithkey`,
//! `createmultisig`. No keystore — keys come in on the call.

use crate::methods::{
    parse_hash32_display, rpc_error, RpcContext, RpcParams, ERR_DESERIALIZATION,
    ERR_INVALID_ADDRESS_OR_KEY, ERR_INVALID_PARAMETER, ERR_INVALID_PARAMS, ERR_MISC,
    ERR_TYPE_ERROR,
};
use bitcoin::absolute::LockTime;
use bitcoin::address::KnownHrp;
use bitcoin::consensus::encode::serialize_hex;
use bitcoin::hashes::Hash;
use bitcoin::key::PrivateKey;
use bitcoin::script::{Builder, PushBytesBuf};
use bitcoin::secp256k1::{Message, Secp256k1};
use bitcoin::sighash::{EcdsaSighashType, SighashCache};
use bitcoin::{
    Address, Amount, Network as BtcNetwork, OutPoint, PublicKey, Script, ScriptBuf, Sequence,
    Transaction, TxIn, TxOut, Txid, Witness,
};
use rbitcoin_primitives::{hex_decode, hex_encode};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::str::FromStr;

pub(crate) fn createrawtransaction(params: &RpcParams) -> Result<Value, Value> {
    params.reject_unknown(&["inputs", "outputs", "locktime", "replaceable"])?;
    let ins = params
        .get_array(0, "inputs")
        .ok_or_else(|| rpc_error(ERR_INVALID_PARAMS, "inputs must be an array"))?;
    let outs = parse_create_outputs(params.get(1, "outputs"))?;
    let locktime = params.opt_u64(2, "locktime")?.unwrap_or(0) as u32;
    let replaceable = params.opt_bool(3, "replaceable")?.unwrap_or(false);
    let seq = if replaceable {
        Sequence::ENABLE_RBF_NO_LOCKTIME
    } else {
        Sequence::MAX
    };

    let mut input = Vec::with_capacity(ins.len());
    for v in ins {
        let o = v
            .as_object()
            .ok_or_else(|| rpc_error(ERR_INVALID_PARAMS, "input must be an object"))?;
        let txid_s = o
            .get("txid")
            .and_then(Value::as_str)
            .ok_or_else(|| rpc_error(ERR_INVALID_PARAMS, "txid required"))?;
        let txid = Txid::from_byte_array(parse_hash32_display(txid_s)?);
        let vout = o
            .get("vout")
            .and_then(|x| x.as_u64().or_else(|| x.as_i64().map(|n| n as u64)))
            .ok_or_else(|| rpc_error(ERR_INVALID_PARAMS, "vout required"))?;
        let sequence = o
            .get("sequence")
            .and_then(|x| x.as_u64())
            .map(|n| Sequence(n as u32))
            .unwrap_or(seq);
        input.push(TxIn {
            previous_output: OutPoint {
                txid,
                vout: vout as u32,
            },
            script_sig: ScriptBuf::new(),
            sequence,
            witness: Witness::new(),
        });
    }

    let mut output = Vec::new();
    for (k, v) in &outs {
        if k == "data" {
            let hex = v
                .as_str()
                .ok_or_else(|| rpc_error(ERR_INVALID_PARAMS, "data must be hex"))?;
            let bytes =
                hex_decode(hex).map_err(|e| rpc_error(ERR_INVALID_PARAMS, e.to_string()))?;
            let payload = PushBytesBuf::try_from(bytes)
                .map_err(|_| rpc_error(ERR_INVALID_PARAMS, "data too large"))?;
            output.push(TxOut {
                value: Amount::ZERO,
                script_pubkey: Builder::new()
                    .push_opcode(bitcoin::opcodes::all::OP_RETURN)
                    .push_slice(payload)
                    .into_script(),
            });
            continue;
        }
        let sats = json_btc_sats(v)?;
        let addr: Address<bitcoin::address::NetworkUnchecked> = k.parse().map_err(|_| {
            rpc_error(
                ERR_INVALID_PARAMETER,
                format!("Invalid Bitcoin address: {k}"),
            )
        })?;
        let addr = addr.assume_checked();
        output.push(TxOut {
            value: Amount::from_sat(sats),
            script_pubkey: addr.script_pubkey(),
        });
    }

    let tx = Transaction {
        version: bitcoin::transaction::Version::TWO,
        lock_time: LockTime::from_consensus(locktime),
        input,
        output,
    };
    Ok(json!(serialize_hex(&tx)))
}

pub(crate) fn createmultisig(ctx: &RpcContext, params: &RpcParams) -> Result<Value, Value> {
    params.reject_unknown(&["nrequired", "keys", "address_type"])?;
    let nrequired = params.req_u64(0, "nrequired")? as usize;
    let keys = params
        .get_array(1, "keys")
        .ok_or_else(|| rpc_error(ERR_INVALID_PARAMS, "keys must be an array"))?;
    let addr_type = params.opt_str(2, "address_type")?.unwrap_or("legacy");
    if addr_type == "bech32m" {
        return Err(rpc_error(
            ERR_INVALID_ADDRESS_OR_KEY,
            "createmultisig cannot create bech32m multisig addresses",
        ));
    }
    if keys.len() > 20 {
        return Err(rpc_error(
            ERR_INVALID_PARAMETER,
            "Number of keys involved in the multisignature address creation > 20",
        ));
    }
    if nrequired == 0 || nrequired > keys.len() {
        return Err(rpc_error(
            ERR_INVALID_PARAMETER,
            "nrequired must be from 1 to the number of keys",
        ));
    }
    let mut pks = Vec::with_capacity(keys.len());
    for k in keys {
        let s = k
            .as_str()
            .ok_or_else(|| rpc_error(ERR_INVALID_PARAMS, "key must be hex"))?;
        let pk = PublicKey::from_str(s)
            .map_err(|e| rpc_error(ERR_INVALID_PARAMETER, format!("Invalid public key: {e}")))?;
        pks.push(pk);
    }
    let redeem = build_multisig(&pks, nrequired)?;
    if addr_type == "legacy" && redeem.len() > 520 {
        return Err(rpc_error(
            ERR_INVALID_PARAMETER,
            format!("redeemScript exceeds size limit: {} > 520", redeem.len()),
        ));
    }
    let net = btc_net(ctx);
    let address = match addr_type {
        "legacy" => Address::p2sh(&redeem, net)
            .map_err(|e| rpc_error(ERR_MISC, e.to_string()))?
            .to_string(),
        "p2sh-segwit" => Address::p2shwsh(&redeem, net).to_string(),
        "bech32" => Address::p2wsh(&redeem, KnownHrp::from(net)).to_string(),
        other => {
            return Err(rpc_error(
                ERR_INVALID_PARAMETER,
                format!("Unknown address type '{other}'"),
            ));
        }
    };
    let mut inner = format!("multi({nrequired}");
    for pk in &pks {
        inner.push(',');
        inner.push_str(&pk.to_string());
    }
    inner.push(')');
    let desc_body = match addr_type {
        "legacy" => format!("sh({inner})"),
        "p2sh-segwit" => format!("sh(wsh({inner}))"),
        "bech32" => format!("wsh({inner})"),
        _ => inner,
    };
    let descriptor = descsum_create(&desc_body);
    Ok(Value::Object(
        [
            ("address".into(), Value::String(address)),
            (
                "redeemScript".into(),
                Value::String(hex_encode(redeem.as_bytes())),
            ),
            ("descriptor".into(), Value::String(descriptor)),
        ]
        .into_iter()
        .collect(),
    ))
}

pub(crate) fn signrawtransactionwithkey(
    ctx: &RpcContext,
    params: &RpcParams,
) -> Result<Value, Value> {
    params.reject_unknown(&["hexstring", "privkeys", "prevtxs", "sighashtype"])?;
    if let Some(s) = params.opt_str(3, "sighashtype")? {
        if !valid_sighash(s) {
            return Err(rpc_error(
                ERR_INVALID_PARAMETER,
                format!("'{s}' is not a valid sighash parameter."),
            ));
        }
    }
    let hex = params.req_str(0, "hexstring")?;
    let mut tx = decode_tx_hex_strict(hex)?;
    let keys_v = params
        .get_array(1, "privkeys")
        .ok_or_else(|| rpc_error(ERR_INVALID_PARAMS, "privkeys must be an array"))?;
    let secp = Secp256k1::new();
    let mut keys: HashMap<[u8; 33], PrivateKey> = HashMap::new();
    let mut keys65: HashMap<[u8; 65], PrivateKey> = HashMap::new();
    for k in keys_v {
        let wif = k
            .as_str()
            .ok_or_else(|| rpc_error(ERR_TYPE_ERROR, "privkey must be a string"))?;
        let pk = PrivateKey::from_wif(wif)
            .map_err(|_| rpc_error(ERR_INVALID_ADDRESS_OR_KEY, "Invalid private key"))?;
        let full = pk.public_key(&secp);
        match full.to_bytes().as_slice() {
            b if b.len() == 33 => {
                let mut a = [0u8; 33];
                a.copy_from_slice(b);
                keys.insert(a, pk);
            }
            b if b.len() == 65 => {
                let mut a = [0u8; 65];
                a.copy_from_slice(b);
                keys65.insert(a, pk);
            }
            _ => {}
        }
    }
    let prev_map = parse_prevtxs(params.get(2, "prevtxs"))?;
    let unsigned = tx.clone();
    let mut cache = SighashCache::new(&unsigned);
    let mut complete = true;
    for (i, vin) in unsigned.input.iter().enumerate() {
        let prev = resolve_prevout(
            ctx,
            &vin.previous_output,
            prev_map.get(&vin.previous_output),
        )?;
        let Some(prev) = prev else {
            complete = false;
            continue;
        };
        let signed = sign_input(
            &mut cache,
            i,
            &prev,
            prev_map.get(&vin.previous_output),
            &keys,
            &keys65,
            &secp,
        )?;
        match signed {
            Some(filled) => apply_input(&mut tx.input[i], filled),
            None => complete = false,
        }
    }
    Ok(Value::Object(
        [
            ("hex".into(), Value::String(serialize_hex(&tx))),
            ("complete".into(), Value::Bool(complete)),
        ]
        .into_iter()
        .collect(),
    ))
}

struct PrevInfo {
    script_pubkey: ScriptBuf,
    amount: Option<Amount>,
    redeem_script: Option<ScriptBuf>,
    witness_script: Option<ScriptBuf>,
}

enum FilledInput {
    Legacy(ScriptBuf),
    Witness {
        script_sig: ScriptBuf,
        witness: Witness,
    },
    Unchanged,
}

fn apply_input(vin: &mut TxIn, filled: FilledInput) {
    match filled {
        FilledInput::Legacy(ss) => vin.script_sig = ss,
        FilledInput::Witness {
            script_sig,
            witness,
        } => {
            vin.script_sig = script_sig;
            vin.witness = witness;
        }
        FilledInput::Unchanged => {}
    }
}

fn decode_tx_hex_strict(hex: &str) -> Result<Transaction, Value> {
    let b = hex_decode(hex).map_err(|_| tx_decode_failed())?;
    let mut sl = b.as_slice();
    let tx =
        bitcoin::consensus::Decodable::consensus_decode(&mut sl).map_err(|_| tx_decode_failed())?;
    if !sl.is_empty() {
        return Err(tx_decode_failed());
    }
    Ok(tx)
}

fn tx_decode_failed() -> Value {
    rpc_error(
        ERR_DESERIALIZATION,
        "TX decode failed. Make sure the tx has at least one input.",
    )
}

fn valid_sighash(s: &str) -> bool {
    matches!(
        s,
        "DEFAULT"
            | "ALL"
            | "NONE"
            | "SINGLE"
            | "ALL|ANYONECANPAY"
            | "NONE|ANYONECANPAY"
            | "SINGLE|ANYONECANPAY"
    )
}

fn parse_prevtxs(v: Option<&Value>) -> Result<HashMap<OutPoint, PrevInfo>, Value> {
    let mut out = HashMap::new();
    let Some(v) = v else {
        return Ok(out);
    };
    if v.is_null() {
        return Ok(out);
    }
    let arr = v
        .as_array()
        .ok_or_else(|| rpc_error(ERR_INVALID_PARAMS, "prevtxs must be an array"))?;
    for item in arr {
        let o = item
            .as_object()
            .ok_or_else(|| rpc_error(ERR_INVALID_PARAMS, "prevtx must be an object"))?;
        let txid_s = o
            .get("txid")
            .and_then(Value::as_str)
            .ok_or_else(|| rpc_error(ERR_INVALID_PARAMS, "txid required"))?;
        let txid = Txid::from_byte_array(parse_hash32_display(txid_s)?);
        let vout =
            o.get("vout")
                .and_then(|x| x.as_u64())
                .ok_or_else(|| rpc_error(ERR_INVALID_PARAMS, "vout required"))? as u32;
        let spk_hex = o
            .get("scriptPubKey")
            .and_then(Value::as_str)
            .ok_or_else(|| rpc_error(ERR_INVALID_PARAMS, "scriptPubKey required"))?;
        let script_pubkey = ScriptBuf::from_bytes(
            hex_decode(spk_hex).map_err(|e| rpc_error(ERR_INVALID_PARAMS, e.to_string()))?,
        );
        let amount = match o.get("amount") {
            Some(a) if !a.is_null() => Some(Amount::from_sat(json_btc_sats(a)?)),
            _ => None,
        };
        let redeem_script = opt_script(o.get("redeemScript"))?;
        let witness_script = opt_script(o.get("witnessScript"))?;
        out.insert(
            OutPoint { txid, vout },
            PrevInfo {
                script_pubkey,
                amount,
                redeem_script,
                witness_script,
            },
        );
    }
    Ok(out)
}

fn opt_script(v: Option<&Value>) -> Result<Option<ScriptBuf>, Value> {
    match v {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(s)) => {
            let b = hex_decode(s).map_err(|e| rpc_error(ERR_INVALID_PARAMS, e.to_string()))?;
            Ok(Some(ScriptBuf::from_bytes(b)))
        }
        Some(_) => Err(rpc_error(ERR_INVALID_PARAMS, "script must be hex")),
    }
}

fn resolve_prevout(
    ctx: &RpcContext,
    op: &OutPoint,
    given: Option<&PrevInfo>,
) -> Result<Option<(ScriptBuf, Amount)>, Value> {
    if let Some(p) = given {
        let amt = p
            .amount
            .or_else(|| lookup_amount(ctx, op))
            .unwrap_or(Amount::ZERO);
        return Ok(Some((p.script_pubkey.clone(), amt)));
    }
    Ok(lookup_txout(ctx, op))
}

fn lookup_amount(ctx: &RpcContext, op: &OutPoint) -> Option<Amount> {
    lookup_txout(ctx, op).map(|(_, a)| a)
}

fn lookup_txout(ctx: &RpcContext, op: &OutPoint) -> Option<(ScriptBuf, Amount)> {
    if let Some(mp) = ctx.mempool.as_ref() {
        if let Some(tx) = mp.get_tx(&op.txid) {
            if let Some(o) = tx.output.get(op.vout as usize) {
                return Some((o.script_pubkey.clone(), o.value));
            }
        }
    }
    let (fk, rec) = ctx
        .query
        .get_tx_by_txid(&op.txid.to_byte_array())
        .ok()
        .flatten()?;
    let out = ctx.query.tx_output_at_fk(fk, &rec, op.vout).ok()?;
    Some((
        ScriptBuf::from_bytes(out.script),
        Amount::from_sat(out.value as u64),
    ))
}

fn sign_input(
    cache: &mut SighashCache<&Transaction>,
    index: usize,
    prev: &(ScriptBuf, Amount),
    info: Option<&PrevInfo>,
    keys: &HashMap<[u8; 33], PrivateKey>,
    keys65: &HashMap<[u8; 65], PrivateKey>,
    secp: &Secp256k1<bitcoin::secp256k1::All>,
) -> Result<Option<FilledInput>, Value> {
    let (spk, amount) = prev;
    if is_p2a(spk) {
        return Ok(Some(FilledInput::Unchanged));
    }
    if let Some(info) = info {
        check_prev_scripts(spk, info)?;
    }
    if spk.is_p2pkh() {
        return sign_p2pkh(cache, index, spk, keys, keys65, secp);
    }
    let witness_script = info.and_then(|p| p.witness_script.clone());
    let redeem = info
        .and_then(|p| p.redeem_script.clone())
        .or_else(|| witness_script.as_ref().map(|ws| ws.as_script().to_p2wsh()));
    if spk.is_p2sh() {
        let Some(redeem) = redeem else {
            return Ok(None);
        };
        if redeem.is_p2wsh() {
            let Some(ws) = witness_script else {
                return Ok(None);
            };
            return sign_p2sh_p2wsh(cache, index, &redeem, &ws, *amount, keys, keys65, secp);
        }
        return sign_p2sh_multisig(cache, index, &redeem, keys, keys65, secp);
    }
    if spk.is_p2wsh() {
        let Some(ws) = witness_script else {
            return Ok(None);
        };
        return sign_p2wsh(cache, index, &ws, *amount, keys, keys65, secp);
    }
    Ok(None)
}

fn check_prev_scripts(spk: &Script, info: &PrevInfo) -> Result<(), Value> {
    if spk.is_p2sh() && info.redeem_script.is_none() && info.witness_script.is_none() {
        return Err(rpc_error(
            ERR_INVALID_PARAMETER,
            "Missing redeemScript/witnessScript",
        ));
    }
    if let (Some(r), Some(w)) = (&info.redeem_script, &info.witness_script) {
        if r.as_script() != w.as_script().to_p2wsh().as_script() {
            return Err(rpc_error(
                ERR_INVALID_PARAMETER,
                "redeemScript does not correspond to witnessScript",
            ));
        }
    }
    if spk.is_p2sh() {
        let redeem = info.redeem_script.clone().or_else(|| {
            info.witness_script
                .as_ref()
                .map(|w| w.as_script().to_p2wsh())
        });
        if let Some(r) = redeem {
            if r.as_script().to_p2sh().as_script() != spk {
                return Err(rpc_error(
                    ERR_INVALID_PARAMETER,
                    "redeemScript/witnessScript does not match scriptPubKey",
                ));
            }
        }
    }
    if spk.is_p2wsh() {
        if let Some(w) = &info.witness_script {
            if w.as_script().to_p2wsh().as_script() != spk {
                return Err(rpc_error(
                    ERR_INVALID_PARAMETER,
                    "redeemScript/witnessScript does not match scriptPubKey",
                ));
            }
        }
    }
    Ok(())
}

fn is_p2a(s: &Script) -> bool {
    s.as_bytes() == [0x51, 0x02, 0x4e, 0x73]
}

fn sign_p2pkh(
    cache: &mut SighashCache<&Transaction>,
    index: usize,
    spk: &Script,
    keys: &HashMap<[u8; 33], PrivateKey>,
    keys65: &HashMap<[u8; 65], PrivateKey>,
    secp: &Secp256k1<bitcoin::secp256k1::All>,
) -> Result<Option<FilledInput>, Value> {
    let Some(pk) = find_p2pkh_key(spk, keys, keys65, secp) else {
        return Ok(None);
    };
    let sighash = cache
        .legacy_signature_hash(index, spk, EcdsaSighashType::All.to_u32())
        .map_err(|e| rpc_error(ERR_MISC, e.to_string()))?;
    let sigv = ecdsa_sig(secp, &pk, sighash.to_byte_array())?;
    let full = pk.public_key(secp);
    let script = Builder::new()
        .push_slice(push_bytes(&sigv)?)
        .push_key(&full)
        .into_script();
    Ok(Some(FilledInput::Legacy(script)))
}

fn sign_p2sh_multisig(
    cache: &mut SighashCache<&Transaction>,
    index: usize,
    redeem: &Script,
    keys: &HashMap<[u8; 33], PrivateKey>,
    keys65: &HashMap<[u8; 65], PrivateKey>,
    secp: &Secp256k1<bitcoin::secp256k1::All>,
) -> Result<Option<FilledInput>, Value> {
    let Some(pks) = multisig_signers(redeem, keys, keys65, secp) else {
        return Ok(None);
    };
    let sighash = cache
        .legacy_signature_hash(index, redeem, EcdsaSighashType::All.to_u32())
        .map_err(|e| rpc_error(ERR_MISC, e.to_string()))?;
    let mut b = Builder::new().push_int(0);
    for pk in pks {
        let sigv = ecdsa_sig(secp, &pk, sighash.to_byte_array())?;
        b = b.push_slice(push_bytes(&sigv)?);
    }
    b = b.push_slice(push_bytes(redeem.as_bytes())?);
    Ok(Some(FilledInput::Legacy(b.into_script())))
}

fn sign_p2wsh(
    cache: &mut SighashCache<&Transaction>,
    index: usize,
    witness_script: &Script,
    amount: Amount,
    keys: &HashMap<[u8; 33], PrivateKey>,
    keys65: &HashMap<[u8; 65], PrivateKey>,
    secp: &Secp256k1<bitcoin::secp256k1::All>,
) -> Result<Option<FilledInput>, Value> {
    if let Some(pks) = multisig_signers(witness_script, keys, keys65, secp) {
        let sighash = cache
            .p2wsh_signature_hash(index, witness_script, amount, EcdsaSighashType::All)
            .map_err(|e| rpc_error(ERR_MISC, e.to_string()))?;
        let mut wit = Witness::new();
        wit.push([]);
        for pk in pks {
            let sigv = ecdsa_sig(secp, &pk, sighash.to_byte_array())?;
            wit.push(&sigv);
        }
        wit.push(witness_script.as_bytes());
        return Ok(Some(FilledInput::Witness {
            script_sig: ScriptBuf::new(),
            witness: wit,
        }));
    }
    if let Some(pk) = p2pk_key(witness_script, keys, keys65, secp) {
        let sighash = cache
            .p2wsh_signature_hash(index, witness_script, amount, EcdsaSighashType::All)
            .map_err(|e| rpc_error(ERR_MISC, e.to_string()))?;
        let sigv = ecdsa_sig(secp, &pk, sighash.to_byte_array())?;
        let mut wit = Witness::new();
        wit.push(&sigv);
        wit.push(witness_script.as_bytes());
        return Ok(Some(FilledInput::Witness {
            script_sig: ScriptBuf::new(),
            witness: wit,
        }));
    }
    if witness_script.is_p2pkh() {
        if let Some(pk) = find_p2pkh_key(witness_script, keys, keys65, secp) {
            let sighash = cache
                .p2wsh_signature_hash(index, witness_script, amount, EcdsaSighashType::All)
                .map_err(|e| rpc_error(ERR_MISC, e.to_string()))?;
            let sigv = ecdsa_sig(secp, &pk, sighash.to_byte_array())?;
            let full = pk.public_key(secp);
            let mut wit = Witness::new();
            wit.push(&sigv);
            wit.push(full.to_bytes());
            wit.push(witness_script.as_bytes());
            return Ok(Some(FilledInput::Witness {
                script_sig: ScriptBuf::new(),
                witness: wit,
            }));
        }
    }
    Ok(None)
}

fn sign_p2sh_p2wsh(
    cache: &mut SighashCache<&Transaction>,
    index: usize,
    redeem: &Script,
    witness_script: &Script,
    amount: Amount,
    keys: &HashMap<[u8; 33], PrivateKey>,
    keys65: &HashMap<[u8; 65], PrivateKey>,
    secp: &Secp256k1<bitcoin::secp256k1::All>,
) -> Result<Option<FilledInput>, Value> {
    let inner = sign_p2wsh(cache, index, witness_script, amount, keys, keys65, secp)?;
    let Some(FilledInput::Witness { witness, .. }) = inner else {
        return Ok(None);
    };
    let script_sig = Builder::new()
        .push_slice(push_bytes(redeem.as_bytes())?)
        .into_script();
    Ok(Some(FilledInput::Witness {
        script_sig,
        witness,
    }))
}

fn p2pk_key(
    script: &Script,
    keys: &HashMap<[u8; 33], PrivateKey>,
    keys65: &HashMap<[u8; 65], PrivateKey>,
    secp: &Secp256k1<bitcoin::secp256k1::All>,
) -> Option<PrivateKey> {
    let _ = secp;
    let mut ins = script.instructions();
    let first = ins.next()?.ok()?;
    let bytes = first.push_bytes()?.as_bytes();
    let last = ins.next()?.ok()?;
    if last.opcode()? != bitcoin::opcodes::all::OP_CHECKSIG {
        return None;
    }
    if ins.next().is_some() {
        return None;
    }
    match bytes.len() {
        33 => {
            let mut a = [0u8; 33];
            a.copy_from_slice(bytes);
            keys.get(&a).copied()
        }
        65 => {
            let mut a = [0u8; 65];
            a.copy_from_slice(bytes);
            keys65.get(&a).copied()
        }
        _ => None,
    }
}

fn find_p2pkh_key(
    spk: &Script,
    keys: &HashMap<[u8; 33], PrivateKey>,
    keys65: &HashMap<[u8; 65], PrivateKey>,
    secp: &Secp256k1<bitcoin::secp256k1::All>,
) -> Option<PrivateKey> {
    for pk in keys.values().chain(keys65.values()) {
        let full = pk.public_key(secp);
        if Address::p2pkh(full, BtcNetwork::Regtest)
            .script_pubkey()
            .as_script()
            == spk
        {
            return Some(*pk);
        }
        if Address::p2pkh(full, BtcNetwork::Testnet)
            .script_pubkey()
            .as_script()
            == spk
        {
            return Some(*pk);
        }
        if Address::p2pkh(full, BtcNetwork::Bitcoin)
            .script_pubkey()
            .as_script()
            == spk
        {
            return Some(*pk);
        }
    }
    None
}

fn multisig_signers(
    script: &Script,
    keys: &HashMap<[u8; 33], PrivateKey>,
    keys65: &HashMap<[u8; 65], PrivateKey>,
    secp: &Secp256k1<bitcoin::secp256k1::All>,
) -> Option<Vec<PrivateKey>> {
    let (nrequired, pubs) = parse_multisig(script)?;
    let mut out = Vec::new();
    for p in pubs {
        let b = p.to_bytes();
        if let Some(pk) = match b.len() {
            33 => {
                let mut a = [0u8; 33];
                a.copy_from_slice(&b);
                keys.get(&a).copied()
            }
            65 => {
                let mut a = [0u8; 65];
                a.copy_from_slice(&b);
                keys65.get(&a).copied()
            }
            _ => None,
        } {
            out.push(pk);
        }
        if out.len() == nrequired {
            return Some(out);
        }
    }
    let _ = secp;
    None
}

fn parse_multisig(script: &Script) -> Option<(usize, Vec<PublicKey>)> {
    let mut ins = script.instructions();
    let nreq = small_int(&ins.next()?.ok()?)?;
    let mut pubs = Vec::new();
    loop {
        let insx = ins.next()?.ok()?;
        if let Some(n) = small_int(&insx) {
            if n != pubs.len() {
                return None;
            }
            break;
        }
        let bytes = insx.push_bytes()?.as_bytes();
        pubs.push(PublicKey::from_slice(bytes).ok()?);
    }
    let last = ins.next()?.ok()?;
    if last.opcode()? != bitcoin::opcodes::all::OP_CHECKMULTISIG {
        return None;
    }
    if nreq == 0 || nreq > pubs.len() {
        return None;
    }
    Some((nreq, pubs))
}

fn small_int(ins: &bitcoin::script::Instruction<'_>) -> Option<usize> {
    match ins {
        bitcoin::script::Instruction::Op(op) => {
            let u = op.to_u8();
            if u == 0 {
                Some(0)
            } else if (bitcoin::opcodes::all::OP_PUSHNUM_1.to_u8()
                ..=bitcoin::opcodes::all::OP_PUSHNUM_16.to_u8())
                .contains(&u)
            {
                Some((u - bitcoin::opcodes::all::OP_PUSHNUM_1.to_u8() + 1) as usize)
            } else {
                None
            }
        }
        bitcoin::script::Instruction::PushBytes(b) if b.as_bytes() == [1] => Some(1),
        _ => None,
    }
}

fn build_multisig(pks: &[PublicKey], nrequired: usize) -> Result<ScriptBuf, Value> {
    if !(1..=16).contains(&nrequired) || pks.is_empty() || pks.len() > 16 {
        // Core allows up to 20 for bech32/p2sh-segwit via raw numbers.
    }
    let mut b = Builder::new().push_int(nrequired as i64);
    for pk in pks {
        b = b.push_key(pk);
    }
    b = b
        .push_int(pks.len() as i64)
        .push_opcode(bitcoin::opcodes::all::OP_CHECKMULTISIG);
    Ok(b.into_script())
}

fn ecdsa_sig(
    secp: &Secp256k1<bitcoin::secp256k1::All>,
    pk: &PrivateKey,
    digest: [u8; 32],
) -> Result<Vec<u8>, Value> {
    let msg = Message::from_digest(digest);
    let sig = secp.sign_ecdsa(&msg, &pk.inner);
    let mut v = sig.serialize_der().to_vec();
    v.push(EcdsaSighashType::All as u8);
    Ok(v)
}

fn push_bytes(b: &[u8]) -> Result<PushBytesBuf, Value> {
    PushBytesBuf::try_from(b.to_vec()).map_err(|_| rpc_error(ERR_MISC, "push overflow"))
}

/// Core `createrawtransaction` outputs: object `{addr: amt}` or ordered
/// array `[{addr: amt}, …]` (single-key objects, including `data`).
fn parse_create_outputs(v: Option<&Value>) -> Result<Vec<(String, Value)>, Value> {
    match v {
        Some(Value::Object(m)) => Ok(m.iter().map(|(k, val)| (k.clone(), val.clone())).collect()),
        Some(Value::Array(arr)) => {
            let mut out = Vec::with_capacity(arr.len());
            for item in arr {
                let o = item.as_object().ok_or_else(|| {
                    rpc_error(
                        ERR_INVALID_PARAMETER,
                        "Invalid parameter, key-value pair not an object as expected",
                    )
                })?;
                if o.len() != 1 {
                    return Err(rpc_error(
                        ERR_INVALID_PARAMETER,
                        "Invalid parameter, key-value pair must contain exactly one key",
                    ));
                }
                let (k, val) = o.iter().next().expect("len==1");
                out.push((k.clone(), val.clone()));
            }
            Ok(out)
        }
        _ => Err(rpc_error(
            ERR_INVALID_PARAMS,
            "outputs must be an object or array",
        )),
    }
}

fn json_btc_sats(v: &Value) -> Result<u64, Value> {
    match v {
        Value::Number(n) => parse_btc_sats(&n.to_string()),
        Value::String(s) => parse_btc_sats(s),
        _ => Err(rpc_error(ERR_TYPE_ERROR, "amount must be a number")),
    }
}

fn parse_btc_sats(s: &str) -> Result<u64, Value> {
    let s = s.trim();
    let (neg, s) = s.strip_prefix('-').map(|r| (true, r)).unwrap_or((false, s));
    if neg {
        return Err(rpc_error(ERR_INVALID_PARAMETER, "amount must be positive"));
    }
    let (whole, frac) = match s.split_once('.') {
        Some((w, f)) => (w, f),
        None => (s, ""),
    };
    if whole.is_empty() && frac.is_empty() {
        return Err(rpc_error(ERR_INVALID_PARAMETER, "invalid amount"));
    }
    let whole_n: u64 = if whole.is_empty() {
        0
    } else {
        whole
            .parse()
            .map_err(|_| rpc_error(ERR_INVALID_PARAMETER, "invalid amount"))?
    };
    let mut frac_digits = frac.chars().take(8).collect::<String>();
    while frac_digits.len() < 8 {
        frac_digits.push('0');
    }
    let frac_n: u64 = frac_digits
        .parse()
        .map_err(|_| rpc_error(ERR_INVALID_PARAMETER, "invalid amount"))?;
    whole_n
        .checked_mul(100_000_000)
        .and_then(|w| w.checked_add(frac_n))
        .ok_or_else(|| rpc_error(ERR_INVALID_PARAMETER, "amount overflow"))
}

fn btc_net(ctx: &RpcContext) -> BtcNetwork {
    match ctx.network {
        rbitcoin_primitives::Network::Mainnet => BtcNetwork::Bitcoin,
        rbitcoin_primitives::Network::Testnet => BtcNetwork::Testnet,
        rbitcoin_primitives::Network::Signet => BtcNetwork::Signet,
        rbitcoin_primitives::Network::Regtest => BtcNetwork::Regtest,
    }
}

/// BIP380 descriptor checksum (`#` + 8 bech32 chars).
pub(crate) fn descsum_create(s: &str) -> String {
    const INPUT: &[u8] =
        b"0123456789()[],'/*abcdefgh@:$%{}IJKLMNOPQRSTUVWXYZ&+-.;<=>?!^_|~ijklmnopqrstuvwxyzABCDEFGH`#\"\\ ";
    const CHECK: &[u8] = b"qpzry9x8gf2tvdw0s3jn54khce6mua7l";
    const GEN: [u64; 5] = [
        0xf5dee51989,
        0xa9fdca3312,
        0x1bab10e32d,
        0x3706b1677a,
        0x644d626ffd,
    ];
    let mut symbols = Vec::new();
    let mut groups = Vec::new();
    for c in s.bytes() {
        let v = match INPUT.iter().position(|&x| x == c) {
            Some(i) => i as u64,
            None => return s.to_string(),
        };
        symbols.push(v & 31);
        groups.push(v >> 5);
        if groups.len() == 3 {
            symbols.push(groups[0] * 9 + groups[1] * 3 + groups[2]);
            groups.clear();
        }
    }
    if groups.len() == 1 {
        symbols.push(groups[0]);
    } else if groups.len() == 2 {
        symbols.push(groups[0] * 3 + groups[1]);
    }
    symbols.extend_from_slice(&[0; 8]);
    let mut chk = 1u64;
    for value in symbols {
        let top = chk >> 35;
        chk = ((chk & 0x7ffffffff) << 5) ^ value;
        for (i, g) in GEN.iter().enumerate() {
            if (top >> i) & 1 == 1 {
                chk ^= g;
            }
        }
    }
    chk ^= 1;
    let mut out = String::from(s);
    out.push('#');
    for i in 0..8 {
        let idx = ((chk >> (5 * (7 - i))) & 31) as usize;
        out.push(CHECK[idx] as char);
    }
    out
}

/// `sh(multi(...))` / `wsh(multi(...))` / `sh(wsh(multi(...)))` → scriptPubKey.
pub(crate) fn parse_wrapped_multi(desc: &str) -> Option<ScriptBuf> {
    let bare = desc.split('#').next()?.trim();
    let (wrap_sh, wrap_wsh, inner) = if let Some(rest) = bare.strip_prefix("sh(wsh(") {
        (true, true, rest.strip_suffix("))")?)
    } else if let Some(rest) = bare.strip_prefix("sh(") {
        (true, false, rest.strip_suffix(")")?)
    } else if let Some(rest) = bare.strip_prefix("wsh(") {
        (false, true, rest.strip_suffix(")")?)
    } else {
        return None;
    };
    let multi = inner.strip_prefix("multi(")?.strip_suffix(")")?;
    let mut parts = multi.split(',');
    let nrequired: usize = parts.next()?.parse().ok()?;
    let mut pks = Vec::new();
    for p in parts {
        pks.push(PublicKey::from_str(p).ok()?);
    }
    let redeem = build_multisig(&pks, nrequired).ok()?;
    let spk = if wrap_sh && wrap_wsh {
        redeem.as_script().to_p2wsh().as_script().to_p2sh()
    } else if wrap_sh {
        redeem.as_script().to_p2sh()
    } else {
        redeem.as_script().to_p2wsh()
    };
    Some(spk)
}
