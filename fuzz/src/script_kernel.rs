//! In-process script oracle vs `bitcoinconsensus` (fuzz workspace only).

use bitcoin::absolute::LockTime;
use bitcoin::consensus::encode::serialize;
use bitcoin::hashes::Hash;
use bitcoin::transaction::Version as TxVersion;
use bitcoin::{Amount, OutPoint, ScriptBuf, Sequence, Transaction, TxIn, TxOut, Witness};
use bitcoinconsensus::{
    verify_with_flags, Utxo, VERIFY_CHECKLOCKTIMEVERIFY, VERIFY_CHECKSEQUENCEVERIFY, VERIFY_DERSIG,
    VERIFY_NULLDUMMY, VERIFY_P2SH, VERIFY_TAPROOT, VERIFY_WITNESS,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KernelCmp {
    Agree { accept: bool },
    Disagree { ours: bool, core: bool },
}

pub fn kernel_forks(flags: u8) -> (bool, bool, bool, bool, bool, u32) {
    let dersig = flags & 2 != 0;
    let cltv = flags & 4 != 0;
    let csv = flags & 8 != 0;
    let tap = flags & 16 != 0;
    // libbitcoinconsensus VerifyScript asserts P2SH whenever WITNESS is set.
    let p2sh = true;
    let mut core = VERIFY_NULLDUMMY | VERIFY_WITNESS | VERIFY_P2SH;
    if dersig {
        core |= VERIFY_DERSIG;
    }
    if cltv {
        core |= VERIFY_CHECKLOCKTIMEVERIFY;
    }
    if csv {
        core |= VERIFY_CHECKSEQUENCEVERIFY;
    }
    if tap {
        core |= VERIFY_TAPROOT;
    }
    (cltv, csv, dersig, p2sh, tap, core)
}

pub fn parse_kernel_input(data: &[u8]) -> Option<(Vec<u8>, Vec<u8>, u8)> {
    if data.is_empty() {
        return None;
    }
    let flags = data[0];
    let rest = &data[1..];
    let mid = rest.len() / 2;
    Some((rest[..mid].to_vec(), rest[mid..].to_vec(), flags))
}

fn kernel_spend(script_sig: &[u8], script_pubkey: &[u8]) -> (Vec<TxOut>, Transaction) {
    let prevouts = vec![TxOut {
        value: Amount::from_sat(50_0000_0000),
        script_pubkey: ScriptBuf::from_bytes(script_pubkey.to_vec()),
    }];
    let tx = Transaction {
        version: TxVersion::TWO,
        lock_time: LockTime::ZERO,
        input: vec![TxIn {
            previous_output: OutPoint { txid: bitcoin::Txid::from_byte_array([1; 32]), vout: 0 },
            script_sig: ScriptBuf::from_bytes(script_sig.to_vec()),
            sequence: Sequence::MAX,
            witness: Witness::new(),
        }],
        output: vec![TxOut {
            value: Amount::from_sat(49_0000_0000),
            script_pubkey: ScriptBuf::from_bytes(vec![0x51]),
        }],
    };
    (prevouts, tx)
}

pub fn compare_script_kernel(script_sig: &[u8], script_pubkey: &[u8], flags: u8) -> KernelCmp {
    let (bip65, bip112, bip66, bip16, tap, core_flags) = kernel_forks(flags);
    let (prevouts, tx) = kernel_spend(script_sig, script_pubkey);
    let ours = rbitcoin_consensus::verify_tx_scripts_detached_forks(
        prevouts.clone(),
        tx.clone(),
        bip65,
        bip112,
        bip66,
        bip16,
        tap,
    )
    .is_ok();
    let raw = serialize(&tx);
    let spk = prevouts[0].script_pubkey.as_bytes();
    let amount = prevouts[0].value.to_sat();
    let utxo = Utxo {
        script_pubkey: spk.as_ptr(),
        script_pubkey_len: spk.len() as u32,
        value: amount as i64,
    };
    let spent =
        if core_flags & VERIFY_TAPROOT != 0 { Some(std::slice::from_ref(&utxo)) } else { None };
    let core = verify_with_flags(spk, amount, &raw, spent, 0, core_flags).is_ok();
    if ours == core {
        KernelCmp::Agree { accept: ours }
    } else {
        KernelCmp::Disagree { ours, core }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn op_true_and_op_return_agree_with_core() {
        let all = 0x1f;
        match compare_script_kernel(&[], &[0x51], all) {
            KernelCmp::Agree { accept: true } => {}
            other => panic!("OP_TRUE: {other:?}"),
        }
        match compare_script_kernel(&[], &[0x6a], all) {
            KernelCmp::Agree { accept: false } => {}
            other => panic!("OP_RETURN: {other:?}"),
        }
    }

    #[test]
    fn parse_kernel_input_splits_after_flags() {
        let (sig, pk, f) = parse_kernel_input(&[0x1f, 0xaa, 0x51]).unwrap();
        assert_eq!(f, 0x1f);
        assert_eq!(sig, [0xaa]);
        assert_eq!(pk, [0x51]);
        assert!(parse_kernel_input(&[]).is_none());
    }

    #[test]
    fn witness_without_p2sh_bit_does_not_abort() {
        // Nightly crash-047863e3: flags 0x7a (WITNESS on, P2SH bit off).
        // libbitcoinconsensus asserts SCRIPT_VERIFY_P2SH when WITNESS is set.
        let (sig, pk, f) = parse_kernel_input(&[0x7a, 0x51]).unwrap();
        assert_eq!(f, 0x7a);
        assert!(sig.is_empty());
        assert_eq!(pk, [0x51]);
        match compare_script_kernel(&sig, &pk, f) {
            KernelCmp::Agree { accept: true } => {}
            other => panic!("OP_TRUE without P2SH bit: {other:?}"),
        }
        let (_, _, _, p2sh, _, core) = kernel_forks(0);
        assert!(p2sh, "WITNESS requires P2SH");
        assert_ne!(core & VERIFY_P2SH, 0);
    }
}
