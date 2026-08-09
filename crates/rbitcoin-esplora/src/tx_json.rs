//! Build Esplora-shaped transaction JSON from store Class A + wire reconstruct.

use crate::script_fields::esplora_script_fields;
use bitcoin::hashes::Hash;
use bitcoin::Network;
use rbitcoin_primitives::hex_encode;
use rbitcoin_primitives::{Fk, Height};
use rbitcoin_query::{Query, QueryError};
use rbitcoin_store::InputRecord;
use serde_json::{json, Value};

/// Esplora `status` object for a Class A tx fk (confirmed or not).
pub fn tx_status_json(query: &Query, tx_fk: Fk) -> Result<Value, QueryError> {
    let confirmed = query.store().is_confirmed_strong(tx_fk)?;
    if !confirmed {
        return Ok(json!({ "confirmed": false }));
    }
    let height = query.store().tx_height.get(tx_fk)?.unwrap_or(0);
    let mut out = json!({
        "confirmed": true,
        "block_height": height,
    });
    if let Some((_fk, rec)) = query.header_at_height(Height(height))? {
        out["block_hash"] = Value::String(block_hash_hex(&rec.hash));
        out["block_time"] = json!(rec.timestamp);
    }
    Ok(out)
}

/// Full `GET /tx/:txid` body (Esplora API.md transaction format).
pub fn build_tx_json(query: &Query, tx_fk: Fk, network: Network) -> Result<Value, QueryError> {
    let wire = query.reconstruct_tx(tx_fk)?;
    let status = tx_status_json(query, tx_fk)?;
    // Store order: (meta, inputs, outputs).
    let (_meta, stored_inputs, _outs) = query.store().get_tx_full(tx_fk)?;

    let mut vin = Vec::with_capacity(wire.input.len());
    let mut fee_in: Option<i64> = Some(0);
    for (i, tin) in wire.input.iter().enumerate() {
        let is_coinbase = tin.previous_output.is_null();
        let mut vin_obj = json!({
            "txid": if is_coinbase {
                "0".repeat(64)
            } else {
                format!("{}", tin.previous_output.txid)
            },
            "vout": if is_coinbase { 0xFFFFFFFFu32 } else { tin.previous_output.vout },
            "is_coinbase": is_coinbase,
            "sequence": tin.sequence.to_consensus_u32(),
        });

        let ss = tin.script_sig.as_bytes();
        let ss_f = esplora_script_fields(ss, network);
        vin_obj["scriptsig"] = Value::String(ss_f.hex);
        vin_obj["scriptsig_asm"] = Value::String(ss_f.asm);

        let wit: Vec<String> = tin.witness.iter().map(hex_encode).collect();
        vin_obj["witness"] = json!(wit);

        if let Some(asm) = inner_redeemscript_asm(ss) {
            vin_obj["inner_redeemscript_asm"] = Value::String(asm);
        }
        if let Some(asm) = inner_witnessscript_asm(&wit) {
            vin_obj["inner_witnessscript_asm"] = Value::String(asm);
        }

        if !is_coinbase {
            if let Some(prev) = prevout_json(query, &stored_inputs, i, tin, network)? {
                if let Some(v) = prev.get("value").and_then(|x| x.as_i64()) {
                    if let Some(acc) = fee_in.as_mut() {
                        *acc = acc.saturating_add(v);
                    }
                } else {
                    fee_in = None;
                }
                vin_obj["prevout"] = prev;
            } else {
                fee_in = None;
            }
        }

        vin.push(vin_obj);
    }

    let mut vout = Vec::with_capacity(wire.output.len());
    let mut out_sum: i64 = 0;
    for tout in &wire.output {
        let val = tout.value.to_sat() as i64;
        out_sum = out_sum.saturating_add(val);
        let spk_f = esplora_script_fields(tout.script_pubkey.as_bytes(), network);
        let mut o = json!({
            "scriptpubkey": spk_f.hex,
            "scriptpubkey_asm": spk_f.asm,
            "scriptpubkey_type": spk_f.script_type,
            "value": val,
        });
        if let Some(addr) = spk_f.address {
            o["scriptpubkey_address"] = Value::String(addr);
        }
        vout.push(o);
    }

    let weight = wire.weight().to_wu();
    let size = wire.total_size();
    // Prefer Class A stored txid so history cursors and /tx routes share identity
    // (reconstructed wire hash matches in production; fixtures may differ).
    let stored_txid = query
        .get_tx(tx_fk)
        .map(|t| t.txid)
        .unwrap_or_else(|_| wire.compute_txid().to_byte_array());
    let mut obj = json!({
        "txid": block_hash_hex(&stored_txid),
        "version": wire.version.0,
        "locktime": wire.lock_time.to_consensus_u32(),
        "size": size,
        "weight": weight,
        "vin": vin,
        "vout": vout,
        "status": status,
    });

    if let Some(ins) = fee_in {
        if wire.is_coinbase() {
            obj["fee"] = json!(0);
        } else {
            obj["fee"] = json!(ins.saturating_sub(out_sum));
        }
    }

    Ok(obj)
}

fn prevout_json(
    query: &Query,
    stored_inputs: &[InputRecord],
    idx: usize,
    tin: &bitcoin::TxIn,
    network: Network,
) -> Result<Option<Value>, QueryError> {
    if let Some(inp) = stored_inputs.get(idx) {
        if !inp.create_fk.is_null() {
            let parent = query.get_tx(inp.create_fk)?;
            if let Ok(out) = query.tx_output_at_fk(inp.create_fk, &parent, inp.prev_index) {
                return Ok(Some(vout_fields(&out.script, out.value, network)));
            }
        }
    }
    let prev_txid = tin.previous_output.txid.to_byte_array();
    if let Some((pfk, prec)) = query.get_tx_by_txid(&prev_txid)? {
        if let Ok(out) = query.tx_output_at_fk(pfk, &prec, tin.previous_output.vout) {
            return Ok(Some(vout_fields(&out.script, out.value, network)));
        }
    }
    Ok(None)
}

fn vout_fields(script: &[u8], value: i64, network: Network) -> Value {
    let f = esplora_script_fields(script, network);
    let mut o = json!({
        "scriptpubkey": f.hex,
        "scriptpubkey_asm": f.asm,
        "scriptpubkey_type": f.script_type,
        "value": value,
    });
    if let Some(addr) = f.address {
        o["scriptpubkey_address"] = Value::String(addr);
    }
    o
}

fn block_hash_hex(hash: &[u8; 32]) -> String {
    let mut rev = *hash;
    rev.reverse();
    hex_encode(rev)
}

/// Last push of scriptSig as redeemscript asm (P2SH).
fn inner_redeemscript_asm(script_sig: &[u8]) -> Option<String> {
    let last = last_push_data(script_sig)?;
    if last.is_empty() {
        return None;
    }
    Some(bitcoin::Script::from_bytes(last).to_asm_string())
}

/// Witness script: last stack item when it looks like a script (P2WSH / nested).
fn inner_witnessscript_asm(witness_hex: &[String]) -> Option<String> {
    if witness_hex.len() < 2 {
        return None;
    }
    let last = witness_hex.last()?;
    let bytes = rbitcoin_primitives::hex_decode(last).ok()?;
    if bytes.is_empty() || bytes.len() > 10_000 {
        return None;
    }
    // Skip DER signatures.
    if bytes.first() == Some(&0x30) {
        return None;
    }
    Some(bitcoin::Script::from_bytes(&bytes).to_asm_string())
}

fn last_push_data(script: &[u8]) -> Option<&[u8]> {
    let mut i = 0;
    let mut last: Option<&[u8]> = None;
    while i < script.len() {
        let op = script[i];
        i += 1;
        if op <= 0x4b {
            let n = op as usize;
            if i + n > script.len() {
                break;
            }
            last = Some(&script[i..i + n]);
            i += n;
        } else if op == 0x4c {
            if i >= script.len() {
                break;
            }
            let n = script[i] as usize;
            i += 1;
            if i + n > script.len() {
                break;
            }
            last = Some(&script[i..i + n]);
            i += n;
        } else if op == 0x4d {
            if i + 2 > script.len() {
                break;
            }
            let n = u16::from_le_bytes([script[i], script[i + 1]]) as usize;
            i += 2;
            if i + n > script.len() {
                break;
            }
            last = Some(&script[i..i + n]);
            i += n;
        } else {
            last = None;
        }
    }
    last
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn last_push_data_direct_and_pushdata() {
        // OP_1 (non-push) clears last.
        assert!(last_push_data(&[0x51]).is_none());
        // Direct push of 2 bytes.
        assert_eq!(last_push_data(&[0x02, 0xaa, 0xbb]), Some(&[0xaa, 0xbb][..]));
        // Truncated direct push → break with no complete last from this op.
        assert!(last_push_data(&[0x03, 0xaa]).is_none());
        // OP_PUSHDATA1
        assert_eq!(
            last_push_data(&[0x4c, 0x02, 0x11, 0x22]),
            Some(&[0x11, 0x22][..])
        );
        assert!(last_push_data(&[0x4c]).is_none()); // missing length
        assert!(last_push_data(&[0x4c, 0x05, 0x01]).is_none()); // truncated body
                                                                // OP_PUSHDATA2
        assert_eq!(
            last_push_data(&[0x4d, 0x02, 0x00, 0x33, 0x44]),
            Some(&[0x33, 0x44][..])
        );
        assert!(last_push_data(&[0x4d, 0x01]).is_none()); // short len field
        assert!(last_push_data(&[0x4d, 0x03, 0x00, 0x01]).is_none()); // short body
                                                                      // Non-push after push clears last.
        assert!(last_push_data(&[0x01, 0xaa, 0x51]).is_none());
        // Empty push then real push.
        assert_eq!(last_push_data(&[0x00, 0x01, 0xee]), Some(&[0xee][..]));
    }

    #[test]
    fn redeem_and_witness_script_asm_helpers() {
        assert!(inner_redeemscript_asm(&[]).is_none());
        assert!(inner_redeemscript_asm(&[0x00]).is_none()); // empty push
        let redeem = inner_redeemscript_asm(&[0x01, 0x51]).unwrap();
        assert!(redeem.contains("OP_1") || redeem.contains("1"));

        assert!(inner_witnessscript_asm(&[]).is_none());
        assert!(inner_witnessscript_asm(&[String::from("51")]).is_none()); // len < 2
                                                                           // Two items, last is OP_TRUE script — not a DER sig.
        let asm = inner_witnessscript_asm(&[String::from("00"), String::from("51")]).unwrap();
        assert!(!asm.is_empty());
        // DER-looking last item skipped.
        assert!(inner_witnessscript_asm(&[String::from("00"), String::from("3000")]).is_none());
        // Empty last stack item.
        assert!(inner_witnessscript_asm(&[String::from("00"), String::new()]).is_none());
        // Invalid hex.
        assert!(inner_witnessscript_asm(&[String::from("00"), String::from("zz")]).is_none());
    }

    #[test]
    fn block_hash_hex_reverses_bytes() {
        let mut h = [0u8; 32];
        h[0] = 0xab;
        h[31] = 0xcd;
        let s = block_hash_hex(&h);
        assert_eq!(s.len(), 64);
        assert!(s.starts_with("cd"));
        assert!(s.ends_with("ab"));
    }

    #[test]
    fn vout_fields_includes_type_and_value() {
        // P2WPKH: 0x00 0x14 + 20 bytes
        let mut spk = vec![0x00, 0x14];
        spk.extend_from_slice(&[0x11; 20]);
        let v = vout_fields(&spk, 50_000, Network::Bitcoin);
        assert_eq!(v["value"], 50_000);
        assert_eq!(v["scriptpubkey_type"], "v0_p2wpkh");
        assert!(v["scriptpubkey"].as_str().unwrap().len() > 0);
        assert!(v.get("scriptpubkey_address").is_some());
    }
}
