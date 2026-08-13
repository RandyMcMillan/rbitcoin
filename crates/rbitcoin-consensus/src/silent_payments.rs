//! BIP-352 Silent Payments **tweak server** (scan-side `A_tweak` only).
//!
//! libsecp256k1 has a `silentpayments` C module; rust-secp256k1 0.29 (bitcoin
//! 0.32) does not bind it. This module uses `PublicKey::combine_keys` +
//! `mul_tweak` plus script extract. We never take a scan private key.

use bitcoin::hashes::{hash160, sha256, Hash, HashEngine};
use bitcoin::key::{Parity, XOnlyPublicKey};
use bitcoin::script::{Instruction, Script};
use bitcoin::secp256k1::{All, PublicKey, Scalar, Secp256k1};
use bitcoin::{OutPoint, Transaction, TxOut, Witness};
use rbitcoin_primitives::{Fk, Height};
use rbitcoin_query::Query;
use rbitcoin_store::{InputRecord, OutputRecord, StoreError};
use std::collections::{BTreeMap, HashMap};
use std::sync::OnceLock;

use crate::error::ConsensusError;
use crate::params::ChainParams;

/// BIP341 NUMS internal key *H* (SHA256 of uncompressed *G* as x-only).
const NUMS_H: [u8; 32] = [
    0x50, 0x92, 0x9b, 0x74, 0xc1, 0xa0, 0x49, 0x54, 0xb7, 0x8b, 0x4b, 0x60, 0x35, 0xe9, 0x7a, 0x5e,
    0x07, 0x8a, 0x5a, 0x0f, 0x28, 0xec, 0x96, 0xd5, 0x47, 0xbf, 0xee, 0x9a, 0xce, 0x80, 0x3a, 0xc0,
];

/// One Taproot output listed for Cake `output_pubkeys`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TaprootOut {
    pub vout: u32,
    pub xonly: [u8; 32],
    pub value: u64,
}

/// Server tweak plus Taproot outs for one eligible tx.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TxTweak {
    /// `input_hash · ΣA` — 33-byte compressed.
    pub tweak: [u8; 33],
    pub output_pubkeys: Vec<TaprootOut>,
}

fn secp() -> &'static Secp256k1<All> {
    static S: OnceLock<Secp256k1<All>> = OnceLock::new();
    S.get_or_init(Secp256k1::new)
}

/// Compute the BIP-352 server tweak for `tx` given prevout scripts.
///
/// `prevouts.len()` must equal `tx.input.len()`. Returns `None` if the tx is
/// not silent-payment eligible (or the pubkey sum is infinity / invalid hash).
pub fn tweak_from_tx(tx: &Transaction, prevouts: &[TxOut]) -> Option<TxTweak> {
    if tx.input.len() != prevouts.len() {
        return None;
    }
    let output_pubkeys = taproot_outs(tx);
    if output_pubkeys.is_empty() {
        return None;
    }
    if prevouts
        .iter()
        .any(|p| witness_version(p.script_pubkey.as_bytes()) > Some(1))
    {
        return None;
    }

    let mut keys = Vec::new();
    for (vin, prev) in tx.input.iter().zip(prevouts.iter()) {
        if let Some(pk) = extract_input_pubkey(
            prev.script_pubkey.as_script(),
            vin.script_sig.as_script(),
            &vin.witness,
        ) {
            keys.push(pk);
        }
    }
    if keys.is_empty() {
        return None;
    }
    let refs: Vec<&PublicKey> = keys.iter().collect();
    let a = PublicKey::combine_keys(&refs).ok()?;
    let tweak = input_hash_mul_a(tx, &a)?;
    Some(TxTweak {
        tweak,
        output_pubkeys,
    })
}

/// Confirmed height → eligible txid (internal order) → tweak.
///
/// Pre-Taproot and missing heights return an empty map (no error). Does not
/// reconstruct a wire block.
pub fn tweaks_for_height(
    query: &Query,
    params: &ChainParams,
    height: Height,
) -> Result<BTreeMap<[u8; 32], TxTweak>, ConsensusError> {
    if !params.taproot_active_at(height.0) {
        return Ok(BTreeMap::new());
    }
    let fks = match query.block_tx_fks(height) {
        Ok(f) => f,
        Err(StoreError::NotFound) => return Ok(BTreeMap::new()),
        Err(e) => return Err(e.into()),
    };
    if fks.is_empty() {
        return Ok(BTreeMap::new());
    }

    let mut bodies = Vec::with_capacity(fks.len());
    for &fk in &fks {
        let (tx, inputs, outputs) = query.store().get_tx_full(fk)?;
        bodies.push((fk, tx, inputs, outputs));
    }

    let mut parent_outs: HashMap<Fk, Vec<OutputRecord>> = HashMap::new();
    let mut parent_txid: HashMap<Fk, [u8; 32]> = HashMap::new();
    for (fk, tx, _, outs) in &bodies {
        parent_outs.insert(*fk, outs.clone());
        parent_txid.insert(*fk, tx.txid);
    }

    let mut missing: Vec<Fk> = Vec::new();
    for (_, _, inputs, outputs) in &bodies {
        if !outputs.iter().any(|o| is_p2tr(&o.script)) {
            continue;
        }
        for inp in inputs {
            if inp.is_coinbase() {
                continue;
            }
            if !parent_outs.contains_key(&inp.create_fk) {
                missing.push(inp.create_fk);
            }
        }
    }
    missing.sort_by_key(|f| f.0);
    missing.dedup();
    for fk in missing {
        match query.store().get_tx_meta_and_outputs(fk) {
            Ok((meta, outs)) => {
                parent_outs.insert(fk, outs);
                parent_txid.insert(fk, meta.txid);
            }
            Err(StoreError::NotFound) => {
                if let Ok(id) = query.store().txs.body_txid(fk) {
                    parent_txid.insert(fk, id);
                }
            }
            Err(e) => return Err(e.into()),
        }
    }

    let mut out = BTreeMap::new();
    for (_fk, tx_rec, inputs, outputs) in &bodies {
        if !outputs.iter().any(|o| is_p2tr(&o.script)) {
            continue;
        }
        let built = match build_tx_and_prevouts(inputs, outputs, &parent_outs, &parent_txid) {
            Some(v) => v,
            None => continue,
        };
        if let Some(tweak) = tweak_from_tx(&built.0, &built.1) {
            out.insert(tx_rec.txid, tweak);
        }
    }
    Ok(out)
}

fn build_tx_and_prevouts(
    inputs: &[InputRecord],
    outputs: &[OutputRecord],
    parent_outs: &HashMap<Fk, Vec<OutputRecord>>,
    parent_txid: &HashMap<Fk, [u8; 32]>,
) -> Option<(Transaction, Vec<TxOut>)> {
    let mut prevouts = Vec::with_capacity(inputs.len());
    let mut txins = Vec::with_capacity(inputs.len());
    for inp in inputs {
        let (prev_txid, prev_script, prev_value) = if inp.is_coinbase() {
            ([0u8; 32], Vec::new(), 0i64)
        } else {
            let tid = *parent_txid.get(&inp.create_fk)?;
            let outs = parent_outs.get(&inp.create_fk)?;
            let o = outs.get(inp.prev_index as usize)?;
            (tid, o.script.clone(), o.value)
        };
        let wit_refs: Vec<&[u8]> = inp.witness.iter().map(|w| w.as_slice()).collect();
        txins.push(bitcoin::TxIn {
            previous_output: OutPoint {
                txid: bitcoin::Txid::from_byte_array(prev_txid),
                vout: inp.prev_index,
            },
            script_sig: bitcoin::ScriptBuf::from_bytes(inp.script_sig.clone()),
            sequence: bitcoin::Sequence::from_consensus(inp.sequence),
            witness: Witness::from_slice(&wit_refs),
        });
        let value = if prev_value < 0 {
            bitcoin::Amount::ZERO
        } else {
            bitcoin::Amount::from_sat(prev_value as u64)
        };
        prevouts.push(TxOut {
            value,
            script_pubkey: bitcoin::ScriptBuf::from_bytes(prev_script),
        });
    }
    let mut txouts = Vec::with_capacity(outputs.len());
    for o in outputs {
        let value = if o.value < 0 {
            bitcoin::Amount::ZERO
        } else {
            bitcoin::Amount::from_sat(o.value as u64)
        };
        txouts.push(TxOut {
            value,
            script_pubkey: bitcoin::ScriptBuf::from_bytes(o.script.clone()),
        });
    }
    Some((
        Transaction {
            version: bitcoin::transaction::Version::TWO,
            lock_time: bitcoin::absolute::LockTime::ZERO,
            input: txins,
            output: txouts,
        },
        prevouts,
    ))
}

fn input_hash_mul_a(tx: &Transaction, a: &PublicKey) -> Option<[u8; 33]> {
    let mut smallest = outpoint_bytes(&tx.input[0].previous_output);
    for vin in tx.input.iter().skip(1) {
        let b = outpoint_bytes(&vin.previous_output);
        if b < smallest {
            smallest = b;
        }
    }
    let ser_a = a.serialize();
    let mut msg = [0u8; 36 + 33];
    msg[..36].copy_from_slice(&smallest);
    msg[36..].copy_from_slice(&ser_a);
    let h = tagged_hash(b"BIP0352/Inputs", &msg);
    if h == [0u8; 32] {
        return None;
    }
    let scalar = Scalar::from_be_bytes(h).ok()?;
    let tweaked = a.mul_tweak(secp(), &scalar).ok()?;
    Some(tweaked.serialize())
}

fn tagged_hash(tag: &[u8], payload: &[u8]) -> [u8; 32] {
    let tagh = sha256::Hash::hash(tag);
    let mut eng = sha256::Hash::engine();
    eng.input(tagh.as_ref());
    eng.input(tagh.as_ref());
    eng.input(payload);
    sha256::Hash::from_engine(eng).to_byte_array()
}

fn outpoint_bytes(op: &OutPoint) -> [u8; 36] {
    let mut b = [0u8; 36];
    b[..32].copy_from_slice(op.txid.as_byte_array());
    b[32..].copy_from_slice(&op.vout.to_le_bytes());
    b
}

fn taproot_outs(tx: &Transaction) -> Vec<TaprootOut> {
    let mut out = Vec::new();
    for (i, o) in tx.output.iter().enumerate() {
        let spk = o.script_pubkey.as_bytes();
        if !is_p2tr(spk) {
            continue;
        }
        let mut xonly = [0u8; 32];
        xonly.copy_from_slice(&spk[2..34]);
        out.push(TaprootOut {
            vout: i as u32,
            xonly,
            value: o.value.to_sat(),
        });
    }
    out
}

fn is_p2tr(spk: &[u8]) -> bool {
    spk.len() == 34 && spk[0] == 0x51 && spk[1] == 0x20
}

fn is_p2wpkh(spk: &[u8]) -> bool {
    spk.len() == 22 && spk[0] == 0x00 && spk[1] == 0x14
}

fn is_p2pkh(spk: &[u8]) -> bool {
    spk.len() == 25
        && spk[0] == 0x76
        && spk[1] == 0xa9
        && spk[2] == 0x14
        && spk[23] == 0x88
        && spk[24] == 0xac
}

fn is_p2sh(spk: &[u8]) -> bool {
    spk.len() == 23 && spk[0] == 0xa9 && spk[1] == 0x14 && spk[22] == 0x87
}

fn witness_version(spk: &[u8]) -> Option<u8> {
    if spk.len() < 4 || spk.len() > 42 {
        return None;
    }
    let version = match spk[0] {
        0x00 => 0u8,
        v @ 0x51..=0x60 => v - 0x50,
        _ => return None,
    };
    let n = spk[1] as usize;
    if !(2..=40).contains(&n) || spk.len() != 2 + n {
        return None;
    }
    Some(version)
}

fn extract_input_pubkey(
    prev: &Script,
    script_sig: &Script,
    witness: &Witness,
) -> Option<PublicKey> {
    let spk = prev.as_bytes();
    if is_p2tr(spk) {
        return extract_p2tr(spk, witness);
    }
    if is_p2wpkh(spk) {
        return last_compressed_witness_pubkey(witness);
    }
    if is_p2sh(spk) {
        if !is_p2sh_p2wpkh_redeem(script_sig) {
            return None;
        }
        return last_compressed_witness_pubkey(witness);
    }
    if is_p2pkh(spk) {
        return p2pkh_script_pubkey(spk, script_sig);
    }
    None
}

fn extract_p2tr(spk: &[u8], witness: &Witness) -> Option<PublicKey> {
    if nums_h_script_path(witness) {
        return None;
    }
    let xonly = XOnlyPublicKey::from_slice(&spk[2..34]).ok()?;
    // Even-Y lift (BIP340 / BIP-352 taproot inputs).
    Some(xonly.public_key(Parity::Even))
}

fn nums_h_script_path(witness: &Witness) -> bool {
    let items = witness_items_no_annex(witness);
    if items.len() < 2 {
        return false;
    }
    let cb = items[items.len() - 1];
    if cb.len() < 33 {
        return false;
    }
    cb[1..33] == NUMS_H
}

fn witness_items_no_annex(witness: &Witness) -> Vec<&[u8]> {
    let mut items: Vec<&[u8]> = witness.iter().collect();
    if items.len() >= 2 {
        if let Some(last) = items.last() {
            if !last.is_empty() && last[0] == 0x50 {
                items.pop();
            }
        }
    }
    items
}

fn last_compressed_witness_pubkey(witness: &Witness) -> Option<PublicKey> {
    let items = witness_items_no_annex(witness);
    let last = items.last()?;
    compressed_pubkey(last)
}

/// P2PKH scriptSigs are third-party malleable. Take the compressed key whose
/// HASH160 matches the prevout (BIP-352: parse even if the template is wrapped).
fn p2pkh_script_pubkey(spk: &[u8], script_sig: &Script) -> Option<PublicKey> {
    if spk.len() != 25 {
        return None;
    }
    let want = &spk[3..23];
    let mut last = None;
    for ins in script_sig.instructions() {
        let Ok(Instruction::PushBytes(b)) = ins else {
            continue;
        };
        if let Some(pk) = compressed_pubkey(b.as_bytes()) {
            let h = hash160::Hash::hash(&pk.serialize());
            if h.as_byte_array() == want {
                last = Some(pk);
            }
        }
    }
    last
}

fn compressed_pubkey(b: &[u8]) -> Option<PublicKey> {
    if b.len() == 33 && (b[0] == 0x02 || b[0] == 0x03) {
        PublicKey::from_slice(b).ok()
    } else {
        None
    }
}

fn is_p2sh_p2wpkh_redeem(script_sig: &Script) -> bool {
    let mut push: Option<Vec<u8>> = None;
    let mut n = 0usize;
    for ins in script_sig.instructions() {
        match ins {
            Ok(Instruction::PushBytes(b)) => {
                n += 1;
                push = Some(b.as_bytes().to_vec());
            }
            Ok(_) => return false,
            Err(_) => return false,
        }
    }
    if n != 1 {
        return false;
    }
    let Some(r) = push else {
        return false;
    };
    r.len() == 22 && r[0] == 0x00 && r[1] == 0x14
}

#[cfg(test)]
mod tests {
    use super::*;
    use bitcoin::absolute::LockTime;
    use bitcoin::consensus::encode::deserialize;
    use bitcoin::hashes::Hash;
    use bitcoin::script::ScriptBuf;
    use bitcoin::transaction::Version as TxVersion;
    use bitcoin::{Amount, Sequence, TxIn};
    use rbitcoin_query::TxApply;
    use rbitcoin_store::{HeaderRecord, TxRecord};
    use serde_json::Value;
    use std::path::PathBuf;
    use std::str::FromStr;
    use std::sync::Once;

    static HEAD_SCALE: Once = Once::new();

    fn ensure_tiny_heads() {
        HEAD_SCALE.call_once(|| {
            if std::env::var_os("RBITCOIN_HEAD_SCALE").is_none() {
                // SAFETY: tests only.
                std::env::set_var("RBITCOIN_HEAD_SCALE", "tiny");
            }
        });
    }

    fn hex_bytes(s: &str) -> Vec<u8> {
        rbitcoin_primitives::hex_decode(s).expect("hex")
    }

    fn decode_witness(s: &str) -> Witness {
        if s.is_empty() {
            return Witness::new();
        }
        let b = hex_bytes(s);
        deserialize::<Witness>(&b).unwrap_or_else(|_| Witness::from_slice(&[&b]))
    }

    fn tx_from_receiving(given: &Value) -> (Transaction, Vec<TxOut>) {
        let vin = given["vin"].as_array().expect("vin");
        let mut input = Vec::new();
        let mut prevouts = Vec::new();
        for v in vin {
            let txid = bitcoin::Txid::from_str(v["txid"].as_str().unwrap()).unwrap();
            let vout = v["vout"].as_u64().unwrap() as u32;
            let script_sig =
                ScriptBuf::from_bytes(hex_bytes(v["scriptSig"].as_str().unwrap_or("")));
            let wit = match &v["txinwitness"] {
                Value::String(s) => decode_witness(s),
                Value::Array(items) => {
                    let stacks: Vec<Vec<u8>> = items
                        .iter()
                        .filter_map(|x| x.as_str().map(hex_bytes))
                        .collect();
                    let refs: Vec<&[u8]> = stacks.iter().map(|s| s.as_slice()).collect();
                    Witness::from_slice(&refs)
                }
                _ => Witness::new(),
            };
            input.push(TxIn {
                previous_output: OutPoint { txid, vout },
                script_sig,
                sequence: Sequence::MAX,
                witness: wit,
            });
            let prev_hex = v["prevout"]["scriptPubKey"]["hex"].as_str().unwrap();
            prevouts.push(TxOut {
                value: Amount::from_sat(1),
                script_pubkey: ScriptBuf::from_bytes(hex_bytes(prev_hex)),
            });
        }
        let mut output = Vec::new();
        if let Some(outs) = given["outputs"].as_array() {
            for o in outs {
                let x = hex_bytes(o.as_str().unwrap());
                assert_eq!(x.len(), 32);
                let mut spk = vec![0x51, 0x20];
                spk.extend_from_slice(&x);
                output.push(TxOut {
                    value: Amount::from_sat(1),
                    script_pubkey: ScriptBuf::from_bytes(spk),
                });
            }
        }
        if output.is_empty() {
            output.push(TxOut {
                value: Amount::from_sat(1),
                script_pubkey: ScriptBuf::from_bytes({
                    let mut s = vec![0x51, 0x20];
                    s.extend_from_slice(&[0u8; 32]);
                    s
                }),
            });
        }
        (
            Transaction {
                version: TxVersion::TWO,
                lock_time: LockTime::ZERO,
                input,
                output,
            },
            prevouts,
        )
    }

    #[test]
    fn official_vectors_receiving_tweaks() {
        let raw = include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/bip352_send_and_receive_test_vectors.json"
        ));
        let cases: Value = serde_json::from_str(raw).expect("vectors json");
        let mut n = 0u32;
        for case in cases.as_array().unwrap() {
            let comment = case["comment"].as_str().unwrap_or("");
            for rec in case["receiving"].as_array().unwrap() {
                let given = &rec["given"];
                let expected = &rec["expected"];
                let (tx, prev) = tx_from_receiving(given);
                let got = tweak_from_tx(&tx, &prev);
                match expected.get("tweak") {
                    Some(Value::String(exp)) => {
                        let t = got.unwrap_or_else(|| panic!("expected tweak for {comment}"));
                        assert_eq!(
                            rbitcoin_primitives::hex_encode(t.tweak),
                            exp.to_ascii_lowercase(),
                            "tweak mismatch: {comment}"
                        );
                        n += 1;
                    }
                    _ => {
                        assert!(
                            got.is_none(),
                            "expected skip for {comment}, got {:?}",
                            got.map(|g| rbitcoin_primitives::hex_encode(g.tweak))
                        );
                        n += 1;
                    }
                }
            }
        }
        assert!(n >= 28, "ran {n} receiving cases");
    }

    #[test]
    fn skip_witness_v2_input() {
        let tx = Transaction {
            version: TxVersion::TWO,
            lock_time: LockTime::ZERO,
            input: vec![TxIn {
                previous_output: OutPoint::null(),
                script_sig: ScriptBuf::new(),
                sequence: Sequence::MAX,
                witness: Witness::new(),
            }],
            output: vec![TxOut {
                value: Amount::from_sat(1),
                script_pubkey: ScriptBuf::from_bytes({
                    let mut s = vec![0x51, 0x20];
                    s.extend_from_slice(&[1u8; 32]);
                    s
                }),
            }],
        };
        let mut v2 = vec![0x52, 0x14];
        v2.extend_from_slice(&[0u8; 20]);
        let prev = vec![TxOut {
            value: Amount::from_sat(1),
            script_pubkey: ScriptBuf::from_bytes(v2),
        }];
        assert!(tweak_from_tx(&tx, &prev).is_none());
    }

    fn tmp_store() -> (PathBuf, Query) {
        ensure_tiny_heads();
        let path = std::env::temp_dir().join(format!(
            "rbitcoin-sp-tweaks-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0)
        ));
        std::fs::create_dir_all(&path).unwrap();
        let q = Query::open_or_create(&path).expect("open store");
        (path, q)
    }

    fn header(h: u32, prev_fk: Fk, prev_hash: Option<[u8; 32]>) -> HeaderRecord {
        let mut merkle = [0u8; 32];
        merkle[0..4].copy_from_slice(&h.to_le_bytes());
        merkle[5] = 0xec;
        let hash = match prev_hash {
            None => merkle,
            Some(ph) => rbitcoin_store::block_header_hash(1, &ph, &merkle, h + 1, 0x207f_ffff, h),
        };
        HeaderRecord {
            prev_fk,
            version: 1,
            timestamp: h + 1,
            bits: 0x207f_ffff,
            nonce: h,
            merkle_root: merkle,
            hash,
        }
    }

    #[test]
    fn tweaks_for_height_p2wpkh_parent_matches_engine() {
        use bitcoin::hashes::hash160;
        use bitcoin::secp256k1::SecretKey;

        let (dir, q) = tmp_store();
        let params = ChainParams::regtest();

        let secp_ctx = Secp256k1::new();
        let sk = SecretKey::from_slice(&[2u8; 32]).unwrap();
        let pk = bitcoin::secp256k1::PublicKey::from_secret_key(&secp_ctx, &sk);
        let ser = pk.serialize();
        let h160 = hash160::Hash::hash(&ser);
        let mut p2wpkh = vec![0x00, 0x14];
        p2wpkh.extend_from_slice(h160.as_ref());
        let (xonly, _) = pk.x_only_public_key();
        let mut p2tr = vec![0x51, 0x20];
        p2tr.extend_from_slice(&xonly.serialize());

        let mut genesis_txid = [0u8; 32];
        genesis_txid[31] = 0xcb;
        let h0 = header(0, Fk::NULL, None);
        let ta0 = TxApply {
            tx: TxRecord {
                txid: genesis_txid,
                version: 1,
                locktime: 0,
                input_start_fk: Fk::NULL,
                input_count: 1,
                output_start_fk: Fk::NULL,
                output_count: 1,
            },
            inputs: vec![InputRecord::coinbase(u32::MAX, vec![0x00], vec![])],
            outputs: vec![OutputRecord::unspent(50_0000_0000, p2wpkh.clone())],
        };
        let fk0 = q.connect_block(Height(0), &h0, &[ta0]).unwrap();
        let create_fk = q.block_tx_fks(Height(0)).unwrap()[0];

        let mut spend_txid = [0u8; 32];
        spend_txid[0] = 0x11;
        spend_txid[31] = 0xcd;
        let h1 = header(1, fk0, Some(h0.hash));
        let ta1 = TxApply {
            tx: TxRecord {
                txid: spend_txid,
                version: 2,
                locktime: 0,
                input_start_fk: Fk::NULL,
                input_count: 1,
                output_start_fk: Fk::NULL,
                output_count: 1,
            },
            inputs: vec![InputRecord {
                prev_txid: genesis_txid,
                create_fk,
                prev_index: 0,
                sequence: u32::MAX,
                script_sig: vec![],
                witness: vec![vec![0u8; 64], ser.to_vec()],
            }],
            outputs: vec![OutputRecord::unspent(49_0000_0000, p2tr.clone())],
        };
        q.connect_block(Height(1), &h1, &[ta1]).unwrap();

        let empty0 = tweaks_for_height(&q, &params, Height(0)).unwrap();
        assert!(empty0.is_empty(), "coinbase is not eligible");

        let got = tweaks_for_height(&q, &params, Height(1)).unwrap();
        assert_eq!(got.len(), 1);
        let t = got.get(&spend_txid).expect("spend txid");

        let engine_tx = Transaction {
            version: TxVersion::TWO,
            lock_time: LockTime::ZERO,
            input: vec![TxIn {
                previous_output: OutPoint {
                    txid: bitcoin::Txid::from_byte_array(genesis_txid),
                    vout: 0,
                },
                script_sig: ScriptBuf::new(),
                sequence: Sequence::MAX,
                witness: Witness::from_slice(&[&[0u8; 64][..], &ser[..]]),
            }],
            output: vec![TxOut {
                value: Amount::from_sat(49_0000_0000),
                script_pubkey: ScriptBuf::from_bytes(p2tr),
            }],
        };
        let engine_prev = vec![TxOut {
            value: Amount::from_sat(50_0000_0000),
            script_pubkey: ScriptBuf::from_bytes(p2wpkh),
        }];
        let expect = tweak_from_tx(&engine_tx, &engine_prev).unwrap();
        assert_eq!(t.tweak, expect.tweak);
        assert_eq!(t.output_pubkeys, expect.output_pubkeys);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn tweaks_for_height_unknown_is_empty() {
        let (dir, q) = tmp_store();
        let params = ChainParams::regtest();
        assert!(tweaks_for_height(&q, &params, Height(0))
            .unwrap()
            .is_empty());
        let main = ChainParams::mainnet();
        assert!(tweaks_for_height(&q, &main, Height(100))
            .unwrap()
            .is_empty());
        let _ = std::fs::remove_dir_all(&dir);
    }
}
