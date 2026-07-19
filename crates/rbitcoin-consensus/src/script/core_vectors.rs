//! Bitcoin Core `script_tests.json` runner (unit-test only).
//!
//! Parses Core's human-readable script language and evaluates scriptSig then
//! scriptPubKey the same way `EvalScript` does for non-witness tests.
//!
//! Signature-heavy rows (CHECKSIG with real keys) are skipped — they need Core's
//! credit/spend tx template. Stack/arithmetic/crypto/control-flow rows drive
//! continuous coverage so IBD is not a consensus fuzzer.

#![cfg(test)]

use super::interpreter::{self, EvalContext, SigVersion};
use bitcoin::absolute::LockTime;
use bitcoin::script::Script;
use bitcoin::{Amount, OutPoint, ScriptBuf, Sequence, Transaction, TxIn, TxOut, Witness};
use serde_json::Value;
use std::path::PathBuf;

fn load_json() -> Value {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/script_tests.json");
    let s = std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("missing {path:?}: {e} (vendor Core script_tests.json)"));
    serde_json::from_str(&s).expect("script_tests.json")
}

/// Assemble Core script string ("1 2 ADD", "0x4c 0x01 0x07", "'Az'") to bytes.
fn assemble(src: &str) -> Result<Vec<u8>, String> {
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
            // string push
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
            // 0xHEX — raw bytes appended (Core: can be multi-byte with spaces inside? usually one token)
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
        // number or opcode name
        let start = i;
        while i < b.len() && !b[i].is_ascii_whitespace() {
            i += 1;
        }
        let tok = &src[start..i];
        if tok.is_empty() {
            continue;
        }
        if let Ok(n) = tok.parse::<i64>() {
            // encode as CScriptNum push
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
    // Core names (subset + aliases)
    static MAP: &[(&str, u8)] = &[
        ("OP_0", 0x00),
        ("0", 0x00), // handled as number usually
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
        ("OP_TOALTSTACK", 0x6b),
        ("TOALTSTACK", 0x6b),
        ("OP_FROMALTSTACK", 0x6c),
        ("FROMALTSTACK", 0x6c),
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
        ("OP_IFDUP", 0x73),
        ("IFDUP", 0x73),
        ("OP_DEPTH", 0x74),
        ("DEPTH", 0x74),
        ("OP_DROP", 0x75),
        ("DROP", 0x75),
        ("OP_DUP", 0x76),
        ("DUP", 0x76),
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
        ("OP_SIZE", 0x82),
        ("SIZE", 0x82),
        ("OP_EQUAL", 0x87),
        ("EQUAL", 0x87),
        ("OP_EQUALVERIFY", 0x88),
        ("EQUALVERIFY", 0x88),
        ("OP_1ADD", 0x8b),
        ("1ADD", 0x8b),
        ("OP_1SUB", 0x8c),
        ("1SUB", 0x8c),
        ("OP_NEGATE", 0x8f),
        ("NEGATE", 0x8f),
        ("OP_ABS", 0x90),
        ("ABS", 0x90),
        ("OP_NOT", 0x91),
        ("NOT", 0x91),
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
        ("OP_LESSTHANOREQUAL", 0xa1),
        ("LESSTHANOREQUAL", 0xa1),
        ("OP_GREATERTHANOREQUAL", 0xa2),
        ("GREATERTHANOREQUAL", 0xa2),
        ("OP_MIN", 0xa3),
        ("MIN", 0xa3),
        ("OP_MAX", 0xa4),
        ("MAX", 0xa4),
        ("OP_WITHIN", 0xa5),
        ("WITHIN", 0xa5),
        ("OP_RIPEMD160", 0xa6),
        ("RIPEMD160", 0xa6),
        ("OP_SHA1", 0xa7),
        ("SHA1", 0xa7),
        ("OP_SHA256", 0xa8),
        ("SHA256", 0xa8),
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
        ("OP_CHECKSIGADD", 0xba),
        ("CHECKSIGADD", 0xba),
        ("OP_NOP1", 0xb0),
        ("NOP1", 0xb0),
        ("OP_CHECKLOCKTIMEVERIFY", 0xb1),
        ("CHECKLOCKTIMEVERIFY", 0xb1),
        ("OP_CLTV", 0xb1),
        ("OP_CHECKSEQUENCEVERIFY", 0xb2),
        ("CHECKSEQUENCEVERIFY", 0xb2),
        ("OP_CSV", 0xb2),
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
        ("OP_RESERVED", 0x50),
        ("RESERVED", 0x50),
        ("OP_VER", 0x62),
        ("VER", 0x62),
        ("OP_RESERVED1", 0x89),
        ("RESERVED1", 0x89),
        ("OP_RESERVED2", 0x8a),
        ("RESERVED2", 0x8a),
    ];
    // build once
    // linear scan is fine for tests
    for (n, b) in MAP {
        if n.eq_ignore_ascii_case(name) {
            return Some(*b);
        }
    }
    None
}

fn dummy_tx() -> (Transaction, Vec<TxOut>) {
    let tx = Transaction {
        version: bitcoin::transaction::Version::TWO,
        lock_time: LockTime::ZERO,
        input: vec![TxIn {
            previous_output: OutPoint::null(),
            script_sig: ScriptBuf::new(),
            sequence: Sequence::MAX,
            witness: Witness::new(),
        }],
        output: vec![TxOut {
            value: Amount::from_sat(0),
            script_pubkey: ScriptBuf::from_bytes(vec![0x51]),
        }],
    };
    let prevouts = vec![TxOut {
        value: Amount::from_sat(0),
        script_pubkey: ScriptBuf::from_bytes(vec![0x51]),
    }];
    (tx, prevouts)
}

fn eval_pair(script_sig: &[u8], script_pubkey: &[u8], cleanstack: bool) -> Result<bool, String> {
    let (tx, prevouts) = dummy_tx();
    let mut stack = Vec::new();
    // scriptSig
    let ss = Script::from_bytes(script_sig);
    let ctx_sig = EvalContext::new(&tx, 0, Amount::ZERO, &prevouts, ss, SigVersion::Base);
    if !script_sig.is_empty() {
        interpreter::eval_script(ss, &mut stack, &ctx_sig).map_err(|e| format!("scriptSig: {e}"))?;
    }
    // scriptPubKey
    let spk = Script::from_bytes(script_pubkey);
    let ctx_pk = EvalContext::new(&tx, 0, Amount::ZERO, &prevouts, spk, SigVersion::Base);
    let need = interpreter::eval_script(spk, &mut stack, &ctx_pk)
        .map_err(|e| format!("scriptPubKey: {e}"))?;
    if !need {
        return Ok(true); // OP_SUCCESS-like
    }
    if cleanstack {
        interpreter::require_clean_true(&stack).map_err(|e| format!("{e}"))?;
    } else {
        interpreter::require_true_top(&stack).map_err(|e| format!("{e}"))?;
    }
    Ok(true)
}

fn is_sig_heavy(sig: &str, pk: &str) -> bool {
    let s = format!("{sig} {pk}").to_uppercase();
    s.contains("CHECKSIG") || s.contains("CHECKMULTISIG") || s.contains("CHECKSIGADD")
}

#[test]
fn core_script_tests_nonsig_majority_pass() {
    let root = load_json();
    let arr = root.as_array().expect("array");
    let mut total = 0u32;
    let mut ran = 0u32;
    let mut pass = 0u32;
    let mut fail = 0u32;
    let mut skip = 0u32;
    let mut failures: Vec<String> = Vec::new();

    for (idx, row) in arr.iter().enumerate() {
        let Value::Array(cells) = row else {
            continue;
        };
        // comments / format strings
        if cells.len() < 4 {
            continue;
        }
        // Skip witness-amount form: first element is array
        let (sig_s, pk_s, flags_s, expect_s) = if cells[0].is_array() {
            // [[wit…], amount]?, scriptSig, scriptPubKey, flags, expect
            if cells.len() < 5 {
                continue;
            }
            // witness tests — skip for this pass
            skip += 1;
            continue;
        } else {
            (
                cells[0].as_str().unwrap_or(""),
                cells[1].as_str().unwrap_or(""),
                cells[2].as_str().unwrap_or(""),
                cells[3].as_str().unwrap_or(""),
            )
        };
        total += 1;
        if is_sig_heavy(sig_s, pk_s) {
            skip += 1;
            continue;
        }
        // Only run tests that use flags we approximate: P2SH/STRICTENC/CLEANSTACK optional
        let flags = flags_s.to_uppercase();
        if flags.contains("WITNESS") || flags.contains("TAPSCRIPT") {
            skip += 1;
            continue;
        }
        let clean = flags.contains("CLEANSTACK");
        let expect_ok = expect_s == "OK";
        let expect_false = expect_s == "EVAL_FALSE";

        let sig_bytes = match assemble(sig_s) {
            Ok(b) => b,
            Err(e) => {
                skip += 1;
                let _ = e;
                continue;
            }
        };
        let pk_bytes = match assemble(pk_s) {
            Ok(b) => b,
            Err(e) => {
                skip += 1;
                let _ = e;
                continue;
            }
        };

        ran += 1;
        let got = eval_pair(&sig_bytes, &pk_bytes, clean);
        // EVAL_FALSE: Core evaluates successfully but final stack is false.
        let ok = if expect_false {
            matches!(got, Err(ref m) if m.contains("script false") || m.contains("cleanstack"))
        } else if expect_ok {
            got.is_ok()
        } else {
            // named error — we just require failure
            got.is_err()
        };

        if ok {
            pass += 1;
        } else {
            fail += 1;
            if failures.len() < 25 {
                failures.push(format!(
                    "#{idx} sig={sig_s:?} pk={pk_s:?} flags={flags_s} expect={expect_s} got={got:?}"
                ));
            }
        }
    }

    eprintln!(
        "core script_tests: total_rows≈{total} ran={ran} pass={pass} fail={fail} skip={skip}"
    );
    for f in &failures {
        eprintln!("  FAIL {f}");
    }
    // Require a strong pass rate on the non-sig subset we can run.
    assert!(ran > 100, "expected to run many Core vectors, ran={ran}");
    let rate = pass as f64 / ran as f64;
    assert!(
        rate >= 0.85,
        "Core non-sig pass rate {rate:.2} too low (pass={pass} fail={fail}); first failures above"
    );
}
