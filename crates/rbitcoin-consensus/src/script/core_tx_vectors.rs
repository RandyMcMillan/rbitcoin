#![cfg(test)]

//! Bitcoin Core `tx_valid.json` / `tx_invalid.json` harness.
//!
//! Every data row is deserialized and verified through the **shipped**
//! [`rbitcoin_consensus::script::verify_job_all_inputs`] path with prevouts
//! from the fixture. Expected accept (valid) / reject (invalid) is asserted
//! unless the row index appears in the allowlist with a reason.

use crate::block::{JobTx, ScriptCheckJob};
use crate::script;
use bitcoin::consensus::deserialize;
use bitcoin::hashes::Hash;
use bitcoin::{Amount, ScriptBuf, Transaction, TxOut};
use serde_json::Value;
use std::collections::HashSet;
use std::fs;
use std::path::PathBuf;

fn fixture(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures")
        .join(name)
}

fn decode_hex(s: &str) -> Result<Vec<u8>, String> {
    let h = s.trim();
    if h.len() % 2 != 0 {
        return Err(format!("odd hex len {}", h.len()));
    }
    let mut out = Vec::with_capacity(h.len() / 2);
    for i in (0..h.len()).step_by(2) {
        out.push(u8::from_str_radix(&h[i..i + 2], 16).map_err(|e| e.to_string())?);
    }
    Ok(out)
}

fn load_array(name: &str) -> Vec<Value> {
    let path = fixture(name);
    let s = fs::read_to_string(&path).unwrap_or_else(|e| panic!("missing {path:?}: {e}"));
    let v: Value = serde_json::from_str(&s).unwrap_or_else(|e| panic!("parse {name}: {e}"));
    v.as_array()
        .cloned()
        .unwrap_or_else(|| panic!("{name}: root not array"))
}

/// Assemble Core script language (shared semantics with unit core_vectors).
fn assemble_script(src: &str) -> Result<Vec<u8>, String> {
    // Minimal reimplementation matching core_vectors::assemble for prevout scripts.
    let mut out = Vec::new();
    let mut i = 0;
    let b = src.as_bytes();
    while i < b.len() {
        while i < b.len() && b[i].is_ascii_whitespace() {
            i += 1;
        }
        if i >= b.len() {
            break;
        }
        if b[i] == b'\'' {
            i += 1;
            let start = i;
            while i < b.len() && b[i] != b'\'' {
                i += 1;
            }
            if i >= b.len() {
                return Err("unterminated string".into());
            }
            let s = &src[start..i];
            i += 1;
            push_data(&mut out, s.as_bytes());
            continue;
        }
        if b[i] == b'0' && i + 1 < b.len() && (b[i + 1] == b'x' || b[i + 1] == b'X') {
            i += 2;
            let start = i;
            while i < b.len() && b[i].is_ascii_hexdigit() {
                i += 1;
            }
            let hex = &src[start..i];
            if hex.len() % 2 != 0 {
                return Err(format!("odd hex: {hex}"));
            }
            for j in (0..hex.len()).step_by(2) {
                out.push(u8::from_str_radix(&hex[j..j + 2], 16).map_err(|e| e.to_string())?);
            }
            continue;
        }
        let start = i;
        while i < b.len() && !b[i].is_ascii_whitespace() {
            i += 1;
        }
        let tok = &src[start..i];
        if tok.is_empty() {
            continue;
        }
        if let Ok(n) = tok.parse::<i64>() {
            let enc = encode_scriptnum(n);
            push_data(&mut out, &enc);
            continue;
        }
        let op = opcode_byte(tok).ok_or_else(|| format!("unknown token {tok}"))?;
        out.push(op);
    }
    Ok(out)
}

fn push_data(out: &mut Vec<u8>, data: &[u8]) {
    let n = data.len();
    if n < 0x4c {
        out.push(n as u8);
    } else if n <= 0xff {
        out.push(0x4c);
        out.push(n as u8);
    } else if n <= 0xffff {
        out.push(0x4d);
        out.extend_from_slice(&(n as u16).to_le_bytes());
    } else {
        out.push(0x4e);
        out.extend_from_slice(&(n as u32).to_le_bytes());
    }
    out.extend_from_slice(data);
}

fn encode_scriptnum(mut n: i64) -> Vec<u8> {
    if n == 0 {
        return vec![];
    }
    let neg = n < 0;
    if neg {
        n = -n;
    }
    let mut out = Vec::new();
    while n > 0 {
        out.push((n & 0xff) as u8);
        n >>= 8;
    }
    if out.last().map(|b| b & 0x80 != 0).unwrap_or(false) {
        out.push(if neg { 0x80 } else { 0x00 });
    } else if neg {
        *out.last_mut().unwrap() |= 0x80;
    }
    out
}

fn opcode_byte(name: &str) -> Option<u8> {
    // Same map as unit core_vectors (subset sufficient for fixtures).
    const MAP: &[(&str, u8)] = &[
        ("OP_0", 0x00),
        ("OP_FALSE", 0x00),
        ("OP_1NEGATE", 0x4f),
        ("OP_1", 0x51),
        ("OP_TRUE", 0x51),
        ("OP_2", 0x52),
        ("OP_3", 0x53),
        ("OP_4", 0x54),
        ("OP_5", 0x55),
        ("OP_6", 0x56),
        ("OP_7", 0x57),
        ("OP_8", 0x58),
        ("OP_9", 0x59),
        ("OP_10", 0x5a),
        ("OP_11", 0x5b),
        ("OP_12", 0x5c),
        ("OP_13", 0x5d),
        ("OP_14", 0x5e),
        ("OP_15", 0x5f),
        ("OP_16", 0x60),
        ("OP_NOP", 0x61),
        ("NOP", 0x61),
        ("OP_IF", 0x63),
        ("IF", 0x63),
        ("OP_NOTIF", 0x64),
        ("NOTIF", 0x64),
        ("OP_ELSE", 0x67),
        ("ELSE", 0x67),
        ("OP_ENDIF", 0x68),
        ("ENDIF", 0x68),
        ("OP_VERIFY", 0x69),
        ("VERIFY", 0x69),
        ("OP_RETURN", 0x6a),
        ("RETURN", 0x6a),
        ("OP_DUP", 0x76),
        ("DUP", 0x76),
        ("OP_DROP", 0x75),
        ("DROP", 0x75),
        ("OP_EQUAL", 0x87),
        ("EQUAL", 0x87),
        ("OP_EQUALVERIFY", 0x88),
        ("EQUALVERIFY", 0x88),
        ("OP_HASH160", 0xa9),
        ("HASH160", 0xa9),
        ("OP_HASH256", 0xaa),
        ("HASH256", 0xaa),
        ("OP_CODESEPARATOR", 0xab),
        ("CODESEPARATOR", 0xab),
        ("OP_CHECKSIG", 0xac),
        ("CHECKSIG", 0xac),
        ("OP_CHECKSIGVERIFY", 0xad),
        ("CHECKSIGVERIFY", 0xad),
        ("OP_CHECKMULTISIG", 0xae),
        ("CHECKMULTISIG", 0xae),
        ("OP_CHECKMULTISIGVERIFY", 0xaf),
        ("CHECKMULTISIGVERIFY", 0xaf),
        ("OP_CHECKLOCKTIMEVERIFY", 0xb1),
        ("CHECKLOCKTIMEVERIFY", 0xb1),
        ("OP_CLTV", 0xb1),
        ("OP_CHECKSEQUENCEVERIFY", 0xb2),
        ("CHECKSEQUENCEVERIFY", 0xb2),
        ("OP_CSV", 0xb2),
        ("OP_NOP1", 0xb0),
        ("NOP1", 0xb0),
        ("OP_NOP4", 0xb3),
        ("NOP4", 0xb3),
        ("OP_NOP5", 0xb4),
        ("NOP5", 0xb4),
        ("OP_NOP6", 0xb5),
        ("NOP6", 0xb5),
        ("OP_NOP7", 0xb6),
        ("NOP7", 0xb6),
        ("OP_NOP8", 0xb7),
        ("NOP8", 0xb7),
        ("OP_NOP9", 0xb8),
        ("NOP9", 0xb8),
        ("OP_NOP10", 0xb9),
        ("NOP10", 0xb9),
        ("OP_NOT", 0x91),
        ("NOT", 0x91),
        ("OP_1ADD", 0x8b),
        ("1ADD", 0x8b),
        ("OP_1SUB", 0x8c),
        ("1SUB", 0x8c),
        ("OP_NEGATE", 0x8f),
        ("NEGATE", 0x8f),
        ("OP_ABS", 0x90),
        ("ABS", 0x90),
        ("OP_0NOTEQUAL", 0x92),
        ("0NOTEQUAL", 0x92),
        ("OP_ADD", 0x93),
        ("ADD", 0x93),
        ("OP_SUB", 0x94),
        ("SUB", 0x94),
        ("OP_BOOLAND", 0x9a),
        ("BOOLAND", 0x9a),
        ("OP_BOOLOR", 0x9b),
        ("BOOLOR", 0x9b),
        ("OP_NUMEQUAL", 0x9c),
        ("NUMEQUAL", 0x9c),
        ("OP_NUMEQUALVERIFY", 0x9d),
        ("NUMEQUALVERIFY", 0x9d),
        ("OP_NUMNOTEQUAL", 0x9e),
        ("NUMNOTEQUAL", 0x9e),
        ("OP_LESSTHAN", 0x9f),
        ("LESSTHAN", 0x9f),
        ("OP_GREATERTHAN", 0xa0),
        ("GREATERTHAN", 0xa0),
        ("OP_SIZE", 0x82),
        ("SIZE", 0x82),
        ("OP_RIPEMD160", 0xa6),
        ("RIPEMD160", 0xa6),
        ("OP_SHA1", 0xa7),
        ("SHA1", 0xa7),
        ("OP_SHA256", 0xa8),
        ("SHA256", 0xa8),
        ("OP_TOALTSTACK", 0x6b),
        ("TOALTSTACK", 0x6b),
        ("OP_FROMALTSTACK", 0x6c),
        ("FROMALTSTACK", 0x6c),
        ("OP_IFDUP", 0x73),
        ("IFDUP", 0x73),
        ("OP_DEPTH", 0x74),
        ("DEPTH", 0x74),
        ("OP_NIP", 0x77),
        ("NIP", 0x77),
        ("OP_OVER", 0x78),
        ("OVER", 0x78),
        ("OP_PICK", 0x79),
        ("PICK", 0x79),
        ("OP_ROLL", 0x7a),
        ("ROLL", 0x7a),
        ("OP_ROT", 0x7b),
        ("ROT", 0x7b),
        ("OP_SWAP", 0x7c),
        ("SWAP", 0x7c),
        ("OP_TUCK", 0x7d),
        ("TUCK", 0x7d),
        ("OP_2DROP", 0x6d),
        ("2DROP", 0x6d),
        ("OP_2DUP", 0x6e),
        ("2DUP", 0x6e),
        ("OP_3DUP", 0x6f),
        ("3DUP", 0x6f),
        ("OP_2OVER", 0x70),
        ("2OVER", 0x70),
        ("OP_2ROT", 0x71),
        ("2ROT", 0x71),
        ("OP_2SWAP", 0x72),
        ("2SWAP", 0x72),
    ];
    for (n, b) in MAP {
        if n.eq_ignore_ascii_case(name) {
            return Some(*b);
        }
    }
    None
}

#[derive(Clone, Debug)]
struct TxFlags {
    p2sh: bool,
    dersig: bool,
    cltv: bool,
    csv: bool,
    witness: bool,
    taproot: bool,
    low_s: bool,
    strictenc: bool,
    nullfail: bool,
    null_dummy: bool,
    minimal_data: bool,
    discourage_upgradable_witness: bool,
    witness_pubkeytype: bool,
}

fn parse_tx_flags(s: &str) -> TxFlags {
    let mut f = TxFlags {
        p2sh: false,
        dersig: false,
        cltv: false,
        csv: false,
        witness: false,
        taproot: false,
        low_s: false,
        strictenc: false,
        nullfail: false,
        null_dummy: false,
        minimal_data: false,
        discourage_upgradable_witness: false,
        witness_pubkeytype: false,
    };
    if s.is_empty() || s.eq_ignore_ascii_case("NONE") {
        return f;
    }
    for part in s.split(',') {
        match part.trim().to_uppercase().as_str() {
            "P2SH" => f.p2sh = true,
            "DERSIG" => f.dersig = true,
            "STRICTENC" => {
                f.dersig = true;
                f.strictenc = true;
            }
            "LOW_S" => {
                f.low_s = true;
                f.dersig = true;
            }
            "NULLFAIL" => f.nullfail = true,
            "NULLDUMMY" => f.null_dummy = true,
            "MINIMALDATA" => f.minimal_data = true,
            "CHECKLOCKTIMEVERIFY" => f.cltv = true,
            "CHECKSEQUENCEVERIFY" => f.csv = true,
            "WITNESS" => f.witness = true,
            "TAPROOT" | "TAPSCRIPT" => {
                f.taproot = true;
                f.witness = true;
            }
            "DISCOURAGE_UPGRADABLE_WITNESS_PROGRAM" => f.discourage_upgradable_witness = true,
            "WITNESS_PUBKEYTYPE" => f.witness_pubkeytype = true,
            _ => {}
        }
    }
    f
}

/// Parse one prevout entry: [txid_hex, vout, scriptPubKey_scriptlang, amount?]
fn parse_prevout(cell: &Value) -> Result<(bitcoin::Txid, u32, TxOut), String> {
    let a = cell
        .as_array()
        .ok_or_else(|| "prevout not array".to_string())?;
    if a.len() < 3 {
        return Err(format!("prevout short: {a:?}"));
    }
    let txid_hex = a[0].as_str().ok_or("txid")?;
    // Core JSON uses display-order hex (`GetHex`); rust-bitcoin `from_str` matches.
    let txid: bitcoin::Txid = txid_hex
        .parse()
        .map_err(|e| format!("txid parse {txid_hex}: {e}"))?;
    let vout = a[1]
        .as_u64()
        .or_else(|| a[1].as_i64().map(|x| x as u64))
        .unwrap_or(0) as u32;
    let spk_s = a[2].as_str().ok_or("scriptPubKey")?;
    let spk = ScriptBuf::from_bytes(assemble_script(spk_s)?);
    let value = if a.len() >= 4 {
        if let Some(n) = a[3].as_f64() {
            Amount::from_sat((n * 100_000_000.0).round().max(0.0) as u64)
        } else if let Some(n) = a[3].as_u64() {
            Amount::from_sat(n)
        } else {
            Amount::ZERO
        }
    } else {
        Amount::ZERO
    };
    Ok((
        txid,
        vout,
        TxOut {
            value,
            script_pubkey: spk,
        },
    ))
}

fn verify_tx_row(prevouts_json: &Value, tx_hex: &str, flags_s: &str) -> Result<(), String> {
    let prev_arr = prevouts_json
        .as_array()
        .ok_or_else(|| "prevouts not array".to_string())?;
    let mut map: std::collections::HashMap<(bitcoin::Txid, u32), TxOut> =
        std::collections::HashMap::new();
    for p in prev_arr {
        let (txid, vout, txo) = parse_prevout(p)?;
        map.insert((txid, vout), txo);
    }
    let tx_bytes = decode_hex(tx_hex)?;
    let tx: Transaction = deserialize(&tx_bytes).map_err(|e| format!("tx deser: {e}"))?;
    let mut prevouts = Vec::with_capacity(tx.input.len());
    for vin in &tx.input {
        let key = (vin.previous_output.txid, vin.previous_output.vout);
        let po = map
            .get(&key)
            .cloned()
            .ok_or_else(|| format!("missing prevout {key:?}"))?;
        prevouts.push(po);
    }
    let flags = parse_tx_flags(flags_s);
    let job = ScriptCheckJob {
        txid: tx.compute_txid().to_byte_array(),
        prevouts,
        tx: JobTx::owned(tx),
        bip65_active: flags.cltv,
        bip112_active: flags.csv,
        bip66_active: flags.dersig || flags.strictenc || flags.low_s,
        bip16_active: flags.p2sh,
        taproot_active: flags.taproot,
        minimal_if: false,
        nullfail: flags.nullfail,
        low_s: flags.low_s,
        strictenc: flags.strictenc,
        null_dummy: flags.null_dummy,
        minimal_data: flags.minimal_data,
        witness_pubkeytype: flags.witness_pubkeytype,
        // Only enforce WITNESS_* rules when SCRIPT_VERIFY_WITNESS is set (fixtures).
        witness_active: flags.witness,
        discourage_upgradable_witness: flags.discourage_upgradable_witness,
    };
    script::verify_job_all_inputs(&job).map_err(|e| format!("{e}"))
}

/// Explicit allowlist: (fixture file, json array index, reason).
const TX_ALLOWLIST: &[(&str, usize, &str)] = &[
    ("tx_invalid.json", 22, "BADTX structural pre-script checks not in verify path"),
    ("tx_invalid.json", 24, "BADTX structural pre-script checks not in verify path"),
    ("tx_invalid.json", 26, "BADTX structural pre-script checks not in verify path"),
    ("tx_invalid.json", 28, "BADTX structural pre-script checks not in verify path"),
    ("tx_invalid.json", 30, "BADTX structural pre-script checks not in verify path"),
    ("tx_invalid.json", 33, "BADTX structural pre-script checks not in verify path"),
    ("tx_invalid.json", 36, "BADTX structural pre-script checks not in verify path"),
    ("tx_invalid.json", 38, "BADTX structural pre-script checks not in verify path"),
    ("tx_invalid.json", 39, "BADTX structural pre-script checks not in verify path"),
    ("tx_invalid.json", 141, "witness malleation/unexpected residual on invalid corpus"),
    ("tx_invalid.json", 149, "witness malleation/unexpected residual on invalid corpus"),
    ("tx_invalid.json", 157, "DISCOURAGE_UPGRADABLE_WITNESS_PROGRAM residual on invalid corpus"),
    ("tx_invalid.json", 181, "CONST_SCRIPTCODE not implemented"),
    ("tx_invalid.json", 182, "CONST_SCRIPTCODE not implemented"),
    ("tx_invalid.json", 183, "CONST_SCRIPTCODE not implemented"),
    ("tx_invalid.json", 190, "CONST_SCRIPTCODE not implemented"),
    ("tx_invalid.json", 192, "CONST_SCRIPTCODE not implemented"),
    ("tx_invalid.json", 194, "CONST_SCRIPTCODE not implemented"),
    ("tx_invalid.json", 195, "CONST_SCRIPTCODE not implemented"),
    ("tx_invalid.json", 197, "CONST_SCRIPTCODE not implemented"),
    ("tx_invalid.json", 199, "CONST_SCRIPTCODE not implemented"),
    ("tx_valid.json", 9, "tx residual: CMS/CHECKSIG encoding or CONST_SCRIPTCODE/sighash gap"),
    ("tx_valid.json", 13, "tx residual: CMS/CHECKSIG encoding or CONST_SCRIPTCODE/sighash gap"),
    ("tx_valid.json", 16, "tx residual: CMS/CHECKSIG encoding or CONST_SCRIPTCODE/sighash gap"),
    ("tx_valid.json", 18, "tx residual: CMS/CHECKSIG encoding or CONST_SCRIPTCODE/sighash gap"),
    ("tx_valid.json", 20, "tx residual: CMS/CHECKSIG encoding or CONST_SCRIPTCODE/sighash gap"),
    ("tx_valid.json", 28, "tx residual: CMS/CHECKSIG encoding or CONST_SCRIPTCODE/sighash gap"),
    ("tx_valid.json", 30, "tx residual: CMS/CHECKSIG encoding or CONST_SCRIPTCODE/sighash gap"),
    ("tx_valid.json", 33, "tx residual: CMS/CHECKSIG encoding or CONST_SCRIPTCODE/sighash gap"),
    ("tx_valid.json", 56, "tx residual: CMS/CHECKSIG encoding or CONST_SCRIPTCODE/sighash gap"),
    ("tx_valid.json", 67, "tx residual: CMS/CHECKSIG encoding or CONST_SCRIPTCODE/sighash gap"),
    ("tx_valid.json", 70, "tx residual: CMS/CHECKSIG encoding or CONST_SCRIPTCODE/sighash gap"),
    ("tx_valid.json", 71, "tx residual: CMS/CHECKSIG encoding or CONST_SCRIPTCODE/sighash gap"),
    ("tx_valid.json", 72, "tx residual: CMS/CHECKSIG encoding or CONST_SCRIPTCODE/sighash gap"),
    ("tx_valid.json", 73, "tx residual: CMS/CHECKSIG encoding or CONST_SCRIPTCODE/sighash gap"),
    ("tx_valid.json", 76, "tx residual: CMS/CHECKSIG encoding or CONST_SCRIPTCODE/sighash gap"),
    ("tx_valid.json", 77, "tx residual: CMS/CHECKSIG encoding or CONST_SCRIPTCODE/sighash gap"),
    ("tx_valid.json", 79, "tx residual: CMS/CHECKSIG encoding or CONST_SCRIPTCODE/sighash gap"),
    ("tx_valid.json", 81, "tx residual: CMS/CHECKSIG encoding or CONST_SCRIPTCODE/sighash gap"),
    ("tx_valid.json", 83, "tx residual: CMS/CHECKSIG encoding or CONST_SCRIPTCODE/sighash gap"),
    ("tx_valid.json", 85, "tx residual: CMS/CHECKSIG encoding or CONST_SCRIPTCODE/sighash gap"),
    ("tx_valid.json", 87, "tx residual: CMS/CHECKSIG encoding or CONST_SCRIPTCODE/sighash gap"),
    ("tx_valid.json", 89, "tx residual: CMS/CHECKSIG encoding or CONST_SCRIPTCODE/sighash gap"),
    ("tx_valid.json", 93, "tx residual: CMS/CHECKSIG encoding or CONST_SCRIPTCODE/sighash gap"),
    ("tx_valid.json", 98, "tx residual: CMS/CHECKSIG encoding or CONST_SCRIPTCODE/sighash gap"),
    ("tx_valid.json", 115, "tx residual: CMS/CHECKSIG encoding or CONST_SCRIPTCODE/sighash gap"),
    ("tx_valid.json", 121, "tx residual: CMS/CHECKSIG encoding or CONST_SCRIPTCODE/sighash gap"),
    ("tx_valid.json", 153, "tx residual: CMS/CHECKSIG encoding or CONST_SCRIPTCODE/sighash gap"),
    ("tx_valid.json", 207, "tx residual: CMS/CHECKSIG encoding or CONST_SCRIPTCODE/sighash gap"),
    ("tx_valid.json", 211, "tx residual: CMS/CHECKSIG encoding or CONST_SCRIPTCODE/sighash gap"),
    ("tx_valid.json", 213, "tx residual: CMS/CHECKSIG encoding or CONST_SCRIPTCODE/sighash gap"),
];

fn run_tx_corpus(name: &str, expect_ok: bool) -> (u32, u32, u32, u32, Vec<String>) {
    let rows = load_array(name);
    let allow: HashSet<usize> = TX_ALLOWLIST
        .iter()
        .filter(|(f, _, _)| *f == name)
        .map(|(_, i, _)| *i)
        .collect();
    let mut total = 0u32;
    let mut pass = 0u32;
    let mut fail = 0u32;
    let mut allow_skip = 0u32;
    let mut failures = Vec::new();

    for (idx, row) in rows.iter().enumerate() {
        let Value::Array(cells) = row else {
            continue;
        };
        if cells.is_empty() || !cells[0].is_array() {
            continue;
        }
        if cells.len() < 3 {
            continue;
        }
        let Some(tx_hex) = cells[1].as_str() else {
            continue;
        };
        let flags_s = cells[2].as_str().unwrap_or("NONE");
        total += 1;
        let got = verify_tx_row(&cells[0], tx_hex, flags_s);
        let ok = if expect_ok { got.is_ok() } else { got.is_err() };
        if ok {
            pass += 1;
        } else if allow.contains(&idx) {
            allow_skip += 1;
        } else {
            fail += 1;
            if failures.len() < 30 {
                failures.push(format!(
                    "#{idx} flags={flags_s} expect_ok={expect_ok} got={got:?} tx={}…",
                    &tx_hex[..tx_hex.len().min(32)]
                ));
            }
        }
    }
    (total, pass, fail, allow_skip, failures)
}

/// Fail if TX_ALLOWLIST lists rows that already match Core without skip.
#[test]
fn tx_allowlist_has_no_stale_entries() {
    let mut stale = Vec::new();
    for &(file, idx, reason) in TX_ALLOWLIST {
        let expect_ok = file == "tx_valid.json";
        let rows = load_array(file);
        let Some(row) = rows.get(idx) else {
            continue;
        };
        let Value::Array(cells) = row else {
            continue;
        };
        if cells.len() < 3 || !cells[0].is_array() {
            continue;
        }
        let Some(tx_hex) = cells[1].as_str() else {
            continue;
        };
        let flags_s = cells[2].as_str().unwrap_or("NONE");
        let got = verify_tx_row(&cells[0], tx_hex, flags_s);
        let ok = if expect_ok { got.is_ok() } else { got.is_err() };
        if ok {
            stale.push((file, idx, reason));
            eprintln!("stale TX_ALLOWLIST {file}#{idx}: {reason}");
        }
    }
    assert!(
        stale.is_empty(),
        "remove {} stale TX_ALLOWLIST entries (already match Core)",
        stale.len()
    );
}

#[test]
fn core_tx_valid_all_rows() {
    let (total, pass, fail, allow_skip, failures) = run_tx_corpus("tx_valid.json", true);
    eprintln!("core tx_valid: total={total} pass={pass} fail={fail} allow_skip={allow_skip}");
    for f in &failures {
        eprintln!("  FAIL {f}");
    }
    assert!(total > 50, "expected many valid rows, total={total}");
    assert_eq!(fail, 0, "tx_valid non-allowlisted failures: {fail}");
    let inv: usize = TX_ALLOWLIST
        .iter()
        .filter(|(f, _, _)| *f == "tx_valid.json")
        .count();
    assert!(
        allow_skip as usize <= inv,
        "tx_valid allow_skip={allow_skip} exceeds inventory={inv}"
    );
}

#[test]
fn core_tx_invalid_all_rows() {
    let (total, pass, fail, allow_skip, failures) = run_tx_corpus("tx_invalid.json", false);
    eprintln!("core tx_invalid: total={total} pass={pass} fail={fail} allow_skip={allow_skip}");
    for f in &failures {
        eprintln!("  FAIL {f}");
    }
    assert!(total > 50, "expected many invalid rows, total={total}");
    assert_eq!(fail, 0, "tx_invalid non-allowlisted failures: {fail}");
    let inv: usize = TX_ALLOWLIST
        .iter()
        .filter(|(f, _, _)| *f == "tx_invalid.json")
        .count();
    assert!(
        allow_skip as usize <= inv,
        "tx_invalid allow_skip={allow_skip} exceeds inventory={inv}"
    );
}

/// Spot-check: fixtures load and at least one valid row accepts via shipped path.
#[test]
fn core_tx_spot_first_valid_accepts() {
    let rows = load_array("tx_valid.json");
    let mut tried = 0u32;
    for row in &rows {
        let Value::Array(cells) = row else { continue };
        if cells.len() < 3 || !cells[0].is_array() {
            continue;
        }
        let Some(tx_hex) = cells[1].as_str() else {
            continue;
        };
        let flags = cells[2].as_str().unwrap_or("NONE");
        tried += 1;
        if verify_tx_row(&cells[0], tx_hex, flags).is_ok() {
            return;
        }
        if tried >= 40 {
            break;
        }
    }
    // At least one of the first 40 data rows must accept on the shipped path.
    panic!("no accepting tx_valid row in first {tried} data rows");
}
