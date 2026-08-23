//! Shared Core script-language assembler and hex decode (unit-test only).

pub(crate) fn decode_hex(s: &str) -> Result<Vec<u8>, String> {
    let h = s.trim();
    if !h.len().is_multiple_of(2) {
        return Err(format!("odd hex len {}", h.len()));
    }
    let mut out = Vec::with_capacity(h.len() / 2);
    for i in (0..h.len()).step_by(2) {
        out.push(u8::from_str_radix(&h[i..i + 2], 16).map_err(|e| e.to_string())?);
    }
    Ok(out)
}

/// Assemble Core script string ("1 2 ADD", "0x4c 0x01 0x07", "'Az'") to bytes.
pub(crate) fn assemble_script(src: &str) -> Result<Vec<u8>, String> {
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
            if !hex.len().is_multiple_of(2) {
                return Err(format!("odd hex: {hex}"));
            }
            // Core: 0xHEX is raw script bytes (opcodes + data), not a push.
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
        // Core ParseScript: -1, 0, 1..16 are single opcodes (OP_1NEGATE / OP_0 /
        // OP_1..OP_16). Other integers use minimal push of CScriptNum bytes.
        // Using raw pushes for 1..16 would fail SCRIPT_VERIFY_MINIMALDATA.
        if let Ok(n) = tok.parse::<i64>() {
            if n == 0 {
                out.push(0x00); // OP_0
            } else if n == -1 {
                out.push(0x4f); // OP_1NEGATE
            } else if (1..=16).contains(&n) {
                out.push(0x50 + n as u8); // OP_1 .. OP_16
            } else {
                let enc = encode_scriptnum(n);
                push_data(&mut out, &enc);
            }
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
    static MAP: &[(&str, u8)] = &[
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
        ("OP_VERIF", 0x65),
        ("VERIF", 0x65),
        ("OP_VERNOTIF", 0x66),
        ("VERNOTIF", 0x66),
        ("OP_RESERVED1", 0x89),
        ("RESERVED1", 0x89),
        ("OP_RESERVED2", 0x8a),
        ("RESERVED2", 0x8a),
        ("OP_CAT", 0x7e),
        ("CAT", 0x7e),
        ("OP_SUBSTR", 0x7f),
        ("SUBSTR", 0x7f),
        ("OP_LEFT", 0x80),
        ("LEFT", 0x80),
        ("OP_RIGHT", 0x81),
        ("RIGHT", 0x81),
        ("OP_INVERT", 0x83),
        ("INVERT", 0x83),
        ("OP_AND", 0x84),
        ("AND", 0x84),
        ("OP_OR", 0x85),
        ("OR", 0x85),
        ("OP_XOR", 0x86),
        ("XOR", 0x86),
        ("OP_2MUL", 0x8d),
        ("2MUL", 0x8d),
        ("OP_2DIV", 0x8e),
        ("2DIV", 0x8e),
        ("OP_MUL", 0x95),
        ("MUL", 0x95),
        ("OP_DIV", 0x96),
        ("DIV", 0x96),
        ("OP_MOD", 0x97),
        ("MOD", 0x97),
        ("OP_LSHIFT", 0x98),
        ("LSHIFT", 0x98),
        ("OP_RSHIFT", 0x99),
        ("RSHIFT", 0x99),
    ];
    for (n, b) in MAP {
        if n.eq_ignore_ascii_case(name) {
            return Some(*b);
        }
    }
    None
}
