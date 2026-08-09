//! Pure Esplora script → asm / type / address projection.
//!
//! Matches the documented Esplora API.md vin/vout script fields (not electrs-only
//! extensions). Address is best-effort when the script is a standard pay-to form.

use bitcoin::address::Address;
use bitcoin::script::Script;
use bitcoin::Network;
use rbitcoin_primitives::hex_encode;

/// Documented Esplora scriptpubkey / scriptsig projection fields.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EsploraScriptFields {
    pub hex: String,
    pub asm: String,
    /// e.g. `p2pkh`, `v0_p2wpkh`, `v1_p2tr`, `p2sh`, `unknown`.
    pub script_type: &'static str,
    pub address: Option<String>,
}

/// Project a scriptPubKey (or scriptSig) into Esplora JSON fields.
pub fn esplora_script_fields(script: &[u8], network: Network) -> EsploraScriptFields {
    let s = Script::from_bytes(script);
    let hex = hex_encode(script);
    let asm = s.to_asm_string();
    let script_type = classify_script(s);
    let address = Address::from_script(s, network).ok().map(|a| a.to_string());
    EsploraScriptFields {
        hex,
        asm,
        script_type,
        address,
    }
}

fn classify_script(s: &Script) -> &'static str {
    if s.is_p2pkh() {
        "p2pkh"
    } else if s.is_p2sh() {
        "p2sh"
    } else if s.is_p2wpkh() {
        "v0_p2wpkh"
    } else if s.is_p2wsh() {
        "v0_p2wsh"
    } else if s.is_p2tr() {
        "v1_p2tr"
    } else if s.is_op_return() {
        "op_return"
    } else if s.is_p2pk() {
        "p2pk"
    } else if is_bare_multisig(s) {
        "multisig"
    } else {
        "unknown"
    }
}

/// Minimal bare multisig: ends with CHECKMULTISIG / CHECKMULTISIGVERIFY.
fn is_bare_multisig(s: &Script) -> bool {
    let b = s.as_bytes();
    if b.len() < 3 {
        return false;
    }
    matches!(b[b.len() - 1], 0xae | 0xaf)
}

#[cfg(test)]
mod tests {
    use super::*;
    use bitcoin::hashes::Hash;
    use bitcoin::key::PublicKey;
    use bitcoin::opcodes::all::*;
    use bitcoin::script::Builder;
    use bitcoin::{PubkeyHash, ScriptHash, WPubkeyHash, XOnlyPublicKey};

    /// secp256k1 G compressed / x-only (well-known generator).
    const G_COMPRESSED: [u8; 33] = [
        0x02, 0x79, 0xbe, 0x66, 0x7e, 0xf9, 0xdc, 0xbb, 0xac, 0x55, 0xa0, 0x62, 0x95, 0xce, 0x87,
        0x0b, 0x07, 0x02, 0x9b, 0xfc, 0xdb, 0x2d, 0xce, 0x28, 0xd9, 0x59, 0xf2, 0x81, 0x5b, 0x16,
        0xf8, 0x17, 0x98,
    ];
    const G_XONLY: [u8; 32] = [
        0x79, 0xbe, 0x66, 0x7e, 0xf9, 0xdc, 0xbb, 0xac, 0x55, 0xa0, 0x62, 0x95, 0xce, 0x87, 0x0b,
        0x07, 0x02, 0x9b, 0xfc, 0xdb, 0x2d, 0xce, 0x28, 0xd9, 0x59, 0xf2, 0x81, 0x5b, 0x16, 0xf8,
        0x17, 0x98,
    ];

    #[test]
    fn p2pkh_fields() {
        let h160 = PubkeyHash::from_slice(&[0x11; 20]).unwrap();
        let spk = Builder::new()
            .push_opcode(OP_DUP)
            .push_opcode(OP_HASH160)
            .push_slice(h160)
            .push_opcode(OP_EQUALVERIFY)
            .push_opcode(OP_CHECKSIG)
            .into_script();
        let f = esplora_script_fields(spk.as_bytes(), Network::Bitcoin);
        assert_eq!(f.script_type, "p2pkh");
        assert!(f.asm.contains("OP_DUP"));
        assert!(f.asm.contains("OP_HASH160"));
        assert!(!f.hex.is_empty());
        assert!(f.address.is_some());
    }

    #[test]
    fn p2wpkh_fields() {
        let w = WPubkeyHash::from_slice(&[0x22; 20]).unwrap();
        let spk = Builder::new().push_int(0).push_slice(w).into_script();
        let f = esplora_script_fields(spk.as_bytes(), Network::Bitcoin);
        assert_eq!(f.script_type, "v0_p2wpkh");
        assert!(f.address.as_ref().unwrap().starts_with("bc1q"));
    }

    #[test]
    fn p2tr_fields() {
        let xonly = XOnlyPublicKey::from_slice(&G_XONLY).unwrap();
        let spk = Builder::new()
            .push_int(1)
            .push_x_only_key(&xonly)
            .into_script();
        let f = esplora_script_fields(spk.as_bytes(), Network::Bitcoin);
        assert_eq!(f.script_type, "v1_p2tr");
        assert!(f.address.as_ref().unwrap().starts_with("bc1p"));
    }

    #[test]
    fn p2sh_p2wpkh_outer_is_p2sh() {
        // Outer P2SH wrapping a v0 P2WPKH redeem script (type is still p2sh).
        let w = WPubkeyHash::from_slice(&[0x33; 20]).unwrap();
        let redeem = Builder::new().push_int(0).push_slice(w).into_script();
        let sh = ScriptHash::hash(redeem.as_bytes());
        let spk = Builder::new()
            .push_opcode(OP_HASH160)
            .push_slice(sh)
            .push_opcode(OP_EQUAL)
            .into_script();
        let f = esplora_script_fields(spk.as_bytes(), Network::Bitcoin);
        assert_eq!(f.script_type, "p2sh");
        assert!(f.address.as_ref().unwrap().starts_with('3'));
    }

    #[test]
    fn unknown_op_true() {
        let f = esplora_script_fields(&[0x51], Network::Regtest);
        assert_eq!(f.script_type, "unknown");
        // rust-bitcoin names OP_1 as OP_PUSHNUM_1 in asm.
        assert!(f.asm.contains("1") || f.asm.contains("OP_"));
        assert!(f.address.is_none());
    }

    #[test]
    fn p2pk_type() {
        let pk = PublicKey::from_slice(&G_COMPRESSED).expect("G");
        let spk = Builder::new()
            .push_key(&pk)
            .push_opcode(OP_CHECKSIG)
            .into_script();
        let f = esplora_script_fields(spk.as_bytes(), Network::Bitcoin);
        assert_eq!(f.script_type, "p2pk");
        assert!(f.asm.contains("OP_CHECKSIG"));
    }

    #[test]
    fn p2wsh_op_return_and_bare_multisig_types() {
        // v0 P2WSH: OP_0 + 32-byte push
        let spk_wsh = {
            let mut v = vec![0x00, 0x20];
            v.extend_from_slice(&[0xab; 32]);
            v
        };
        assert_eq!(
            esplora_script_fields(&spk_wsh, Network::Bitcoin).script_type,
            "v0_p2wsh"
        );
        // OP_RETURN
        let opreturn = Builder::new()
            .push_opcode(OP_RETURN)
            .push_slice(b"hi")
            .into_script();
        assert_eq!(
            esplora_script_fields(opreturn.as_bytes(), Network::Bitcoin).script_type,
            "op_return"
        );
        // Bare multisig ends with CHECKMULTISIG
        let multi = Builder::new()
            .push_int(1)
            .push_key(&PublicKey::from_slice(&G_COMPRESSED).unwrap())
            .push_int(1)
            .push_opcode(OP_CHECKMULTISIG)
            .into_script();
        assert_eq!(
            esplora_script_fields(multi.as_bytes(), Network::Bitcoin).script_type,
            "multisig"
        );
        // Short script is not bare multisig
        assert_eq!(
            esplora_script_fields(&[0x51, 0x51], Network::Regtest).script_type,
            "unknown"
        );
    }
}
