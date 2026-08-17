//! Classify standard `scriptPubKey` templates.

use bitcoin::script::Script;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ScriptKind {
    /// OP_0 OP_PUSHBYTES_20 <20>
    P2wpkh,
    /// OP_0 OP_PUSHBYTES_32 <32>
    P2wsh,
    /// OP_1 OP_PUSHBYTES_32 <32>
    P2tr,
    /// OP_DUP OP_HASH160 OP_PUSHBYTES_20 <20> OP_EQUALVERIFY OP_CHECKSIG
    P2pkh,
    /// OP_HASH160 OP_PUSHBYTES_20 <20> OP_EQUAL
    P2sh,
    /// Bare (P2PK, custom, empty spk) — run interpreter
    Bare,
}

/// Core anyone-can-spend templates only. **Empty** `scriptPubKey` is **not** ACS
/// (EvalScript leaves empty stack → fail without TRUE). Only explicit `OP_TRUE`.
///
/// **Not** a short-circuit past `EvalScript(scriptSig)`: Core still runs the
/// scriptSig (CLTV/CSV may live there). Callers must evaluate scriptSig first
/// or use the bare path.
pub(crate) fn is_anyone_can_spend(script: &Script) -> bool {
    script.as_bytes() == [0x51]
}

/// BIP141 witness program: `OP_0`/`OP_1`..`OP_16` + a single push of 2..=40 bytes.
/// Returns `(version, program)` where version is 0 for OP_0 and 1..=16 for OP_1..OP_16.
pub(crate) fn witness_program(script: &Script) -> Option<(u8, &[u8])> {
    let b = script.as_bytes();
    if b.len() < 4 || b.len() > 42 {
        return None;
    }
    let version = match b[0] {
        0x00 => 0u8,
        v @ 0x51..=0x60 => v - 0x50,
        _ => return None,
    };
    let push_len = b[1] as usize;
    // Direct push opcodes 0x02..0x28 only (BIP141 / Core IsWitnessProgram).
    if !(2..=40).contains(&push_len) {
        return None;
    }
    if b.len() != 2 + push_len {
        return None;
    }
    if b[1] != push_len as u8 {
        return None;
    }
    Some((version, &b[2..]))
}

pub(crate) fn classify(script: &Script) -> ScriptKind {
    let b = script.as_bytes();
    if b.len() == 22 && b[0] == 0x00 && b[1] == 0x14 {
        return ScriptKind::P2wpkh;
    }
    if b.len() == 34 && b[0] == 0x00 && b[1] == 0x20 {
        return ScriptKind::P2wsh;
    }
    if b.len() == 34 && b[0] == 0x51 && b[1] == 0x20 {
        return ScriptKind::P2tr;
    }
    if b.len() == 25
        && b[0] == 0x76
        && b[1] == 0xa9
        && b[2] == 0x14
        && b[23] == 0x88
        && b[24] == 0xac
    {
        return ScriptKind::P2pkh;
    }
    if b.len() == 23 && b[0] == 0xa9 && b[1] == 0x14 && b[22] == 0x87 {
        return ScriptKind::P2sh;
    }
    ScriptKind::Bare
}

#[cfg(test)]
mod tests {
    use super::*;
    use bitcoin::script::ScriptBuf;

    #[test]
    fn classify_standard_templates() {
        let p2wpkh = ScriptBuf::from_bytes({
            let mut v = vec![0x00, 0x14];
            v.extend_from_slice(&[0u8; 20]);
            v
        });
        assert_eq!(classify(p2wpkh.as_script()), ScriptKind::P2wpkh);

        let p2wsh = ScriptBuf::from_bytes({
            let mut v = vec![0x00, 0x20];
            v.extend_from_slice(&[0u8; 32]);
            v
        });
        assert_eq!(classify(p2wsh.as_script()), ScriptKind::P2wsh);

        let p2tr = ScriptBuf::from_bytes({
            let mut v = vec![0x51, 0x20];
            v.extend_from_slice(&[0u8; 32]);
            v
        });
        assert_eq!(classify(p2tr.as_script()), ScriptKind::P2tr);

        let p2pkh = ScriptBuf::from_bytes({
            let mut v = vec![0x76, 0xa9, 0x14];
            v.extend_from_slice(&[0u8; 20]);
            v.extend_from_slice(&[0x88, 0xac]);
            v
        });
        assert_eq!(classify(p2pkh.as_script()), ScriptKind::P2pkh);

        let p2sh = ScriptBuf::from_bytes({
            let mut v = vec![0xa9, 0x14];
            v.extend_from_slice(&[0u8; 20]);
            v.push(0x87);
            v
        });
        assert_eq!(classify(p2sh.as_script()), ScriptKind::P2sh);

        assert!(is_anyone_can_spend(
            ScriptBuf::from_bytes(vec![0x51]).as_script()
        ));
        // Empty is bare consensus-eval, not anyone-can-spend (Core parity).
        assert!(!is_anyone_can_spend(ScriptBuf::new().as_script()));
        assert_eq!(classify(ScriptBuf::new().as_script()), ScriptKind::Bare);
        // OP_2..OP_15 bare branch (not ACS).
        assert_eq!(
            classify(ScriptBuf::from_bytes(vec![0x52]).as_script()),
            ScriptKind::Bare
        );

        // BIP141 witness programs.
        let mut v16 = vec![0x60u8, 0x14];
        v16.extend_from_slice(&[0x4cu8; 20]);
        assert_eq!(
            witness_program(ScriptBuf::from_bytes(v16).as_script()).map(|(v, p)| (v, p.len())),
            Some((16, 20))
        );
        let mut v0 = vec![0x00u8, 0x14];
        v0.extend_from_slice(&[0u8; 20]);
        assert_eq!(
            witness_program(ScriptBuf::from_bytes(v0).as_script()).map(|(v, p)| (v, p.len())),
            Some((0, 20))
        );
        assert!(witness_program(ScriptBuf::from_bytes(vec![0x51]).as_script()).is_none());
    }
}
