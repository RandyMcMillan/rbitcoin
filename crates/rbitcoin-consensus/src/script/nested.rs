//! P2SH nested SegWit and legacy redeem scripts.

use bitcoin::script::{Instruction, Script};
use bitcoin::Transaction;

use bitcoin::sighash::SighashCache;

use super::crypto;
use super::interpreter::{self, EvalContext, SigVersion};
use super::p2wpkh;
use super::p2wsh;
use crate::block::ScriptCheckJob;
use crate::error::ConsensusError;

/// P2SH-P2WPKH: scriptSig is **exactly one** push of 22-byte redeem `00 14 <20>`;
/// witness like P2WPKH.
///
/// Returns `None` when scriptSig is multi-push (legacy P2SH multisig, etc.) so the
/// caller can fall through to [`verify_p2sh_legacy`]. Do **not** hard-fail multi-push
/// here — that rejected valid signet blocks (e.g. height 204802).
pub(crate) fn try_p2sh_p2wpkh(
    job: &ScriptCheckJob,
    input_index: usize,
    tx: &Transaction,
    cache: &mut SighashCache<&Transaction>,
) -> Option<Result<(), ConsensusError>> {
    let redeem = match single_push_script_sig(&tx.input[input_index].script_sig) {
        Ok(Some(r)) => r,
        Ok(None) => return None, // multi-push / non-push — not nested-segwit shape
        Err(e) => return Some(Err(e)), // malformed scriptSig
    };
    if redeem.len() != 22 || redeem[0] != 0x00 || redeem[1] != 0x14 {
        return None;
    }
    let spk = job.prevouts[input_index].script_pubkey.as_bytes();
    if spk.len() != 23 {
        return Some(Err(ConsensusError::Script("p2sh spk".into())));
    }
    let expected_hash = &spk[2..22];
    let actual = crypto::hash160(&redeem);
    if actual.as_slice() != expected_hash {
        return Some(Err(ConsensusError::Script("p2sh redeem hash".into())));
    }
    let mut keyhash = [0u8; 20];
    keyhash.copy_from_slice(&redeem[2..22]);
    Some(p2wpkh::verify_with_keyhash(
        job,
        input_index,
        tx,
        &keyhash,
        &redeem,
        cache,
    ))
}

/// P2SH-P2WSH: scriptSig is **exactly one** push of 34-byte redeem `00 20 <32>`;
/// witness like P2WSH. Multi-push → `None` (legacy path).
pub(crate) fn try_p2sh_p2wsh(
    job: &ScriptCheckJob,
    input_index: usize,
    tx: &Transaction,
) -> Option<Result<(), ConsensusError>> {
    let redeem = match single_push_script_sig(&tx.input[input_index].script_sig) {
        Ok(Some(r)) => r,
        Ok(None) => return None,
        Err(e) => return Some(Err(e)),
    };
    if redeem.len() != 34 || redeem[0] != 0x00 || redeem[1] != 0x20 {
        return None;
    }
    let spk = job.prevouts[input_index].script_pubkey.as_bytes();
    if spk.len() != 23 {
        return Some(Err(ConsensusError::Script("p2sh spk".into())));
    }
    let expected_hash = &spk[2..22];
    let actual = crypto::hash160(&redeem);
    if actual.as_slice() != expected_hash {
        return Some(Err(ConsensusError::Script("p2sh redeem hash".into())));
    }
    let mut scripthash = [0u8; 32];
    scripthash.copy_from_slice(&redeem[2..34]);
    Some(p2wsh::verify_with_scripthash(
        job,
        input_index,
        tx,
        &scripthash,
    ))
}

/// Legacy P2SH: scriptSig is `<…data pushes…> <redeemScript>`; evaluate redeem.
pub(crate) fn verify_p2sh_legacy(
    job: &ScriptCheckJob,
    input_index: usize,
    tx: &Transaction,
) -> Result<(), ConsensusError> {
    let input = &tx.input[input_index];
    let (mut stack, redeem) = split_script_sig_redeem(input.script_sig.as_script())?;

    let spk = job.prevouts[input_index].script_pubkey.as_bytes();
    if spk.len() != 23 {
        return Err(ConsensusError::Script("p2sh spk".into()));
    }
    let expected_hash = &spk[2..22];
    let actual = crypto::hash160(&redeem);
    if actual.as_slice() != expected_hash {
        return Err(ConsensusError::Script("p2sh redeem hash".into()));
    }

    let redeem_script = Script::from_bytes(&redeem);
    let ctx = EvalContext::new_with_flags(
        tx,
        input_index,
        job.prevouts[input_index].value,
        &job.prevouts,
        redeem_script,
        SigVersion::Base,
        job.bip65_active,
        job.bip112_active,
        job.bip66_active,
    );
    if interpreter::eval_script(redeem_script, &mut stack, &ctx)? {
        // BIP16: true top only. Witness nested paths use cleanstack separately.
        interpreter::require_true_top(&stack)?;
    }
    Ok(())
}

/// Parse scriptSig as a **single** data push (nested P2SH-P2W*).
///
/// - `Ok(Some(bytes))` — exactly one push
/// - `Ok(None)` — empty, multi-push, or OP_n / OP_0 (not nested-segwit form)
/// - `Err` — instruction decode failure
fn single_push_script_sig(
    script_sig: &bitcoin::script::ScriptBuf,
) -> Result<Option<Vec<u8>>, ConsensusError> {
    let mut only: Option<Vec<u8>> = None;
    for ins in script_sig.instructions() {
        match ins.map_err(|_| ConsensusError::Script("p2sh scriptSig".into()))? {
            Instruction::PushBytes(b) => {
                if only.is_some() {
                    return Ok(None);
                }
                only = Some(b.as_bytes().to_vec());
            }
            Instruction::Op(_) => {
                // OP_0 / OP_1..OP_16 appear in multisig scriptSigs — not single-push redeem.
                return Ok(None);
            }
        }
    }
    Ok(only)
}

/// All pushes except the last form the initial stack; last push is redeemScript.
fn split_script_sig_redeem(script: &Script) -> Result<(Vec<Vec<u8>>, Vec<u8>), ConsensusError> {
    let mut items = Vec::new();
    for ins in script.instructions() {
        match ins.map_err(|_| ConsensusError::Script("p2sh scriptSig".into()))? {
            Instruction::PushBytes(b) => items.push(b.as_bytes().to_vec()),
            Instruction::Op(op) => {
                // Small integers as pushes for multisig stack
                let n = op.to_u8();
                if n == 0x00 {
                    items.push(vec![]);
                } else if (0x51..=0x60).contains(&n) {
                    items.push(vec![n - 0x50]);
                } else {
                    return Err(ConsensusError::Script("p2sh scriptSig op".into()));
                }
            }
        }
    }
    if items.is_empty() {
        return Err(ConsensusError::Script("p2sh empty scriptSig".into()));
    }
    let redeem = items.pop().unwrap();
    Ok((items, redeem))
}

#[cfg(test)]
mod tests {
    use super::*;
    use bitcoin::absolute::LockTime;
    use bitcoin::hashes::Hash;
    use bitcoin::script::ScriptBuf;
    use bitcoin::{
        Amount, OutPoint, Sequence, Transaction, TxIn, TxOut, Witness,
    };
    use crate::block::ScriptCheckJob;

    fn dummy_tx() -> Transaction {
        Transaction {
            version: bitcoin::transaction::Version::TWO,
            lock_time: LockTime::ZERO,
            input: vec![TxIn {
                previous_output: OutPoint {
                    txid: bitcoin::Txid::from_byte_array([1; 32]),
                    vout: 0,
                },
                script_sig: ScriptBuf::new(),
                sequence: Sequence::MAX,
                witness: Witness::new(),
            }],
            output: vec![TxOut {
                value: Amount::from_sat(1),
                script_pubkey: ScriptBuf::from_bytes(vec![0x51]),
            }],
        }
    }

    fn p2sh_spk(redeem: &[u8]) -> ScriptBuf {
        let h = crypto::hash160(redeem);
        let mut v = vec![0xa9, 0x14];
        v.extend_from_slice(&h);
        v.push(0x87);
        ScriptBuf::from_bytes(v)
    }

    #[test]
    fn single_push_and_split_helpers() {
        // Multi-push → None
        let multi = ScriptBuf::from_bytes(vec![0x01, 0xaa, 0x01, 0xbb]);
        assert!(matches!(single_push_script_sig(&multi), Ok(None)));
        // OP → None
        let op = ScriptBuf::from_bytes(vec![0x51]);
        assert!(matches!(single_push_script_sig(&op), Ok(None)));
        // Single push
        let one = ScriptBuf::from_bytes(vec![0x02, 0xde, 0xad]);
        assert_eq!(
            single_push_script_sig(&one).unwrap().unwrap(),
            vec![0xde, 0xad]
        );
        // Malformed truncated push → Err
        let bad = ScriptBuf::from_bytes(vec![0x05, 0x01]);
        assert!(single_push_script_sig(&bad).is_err());

        // split: OP_0 and OP_1 as stack items
        let ss = Script::from_bytes(&[0x00, 0x51, 0x01, 0xac]);
        let (stack, redeem) = split_script_sig_redeem(ss).unwrap();
        assert_eq!(stack, vec![vec![], vec![0x01]]);
        assert_eq!(redeem, vec![0xac]);
        // empty
        assert!(split_script_sig_redeem(Script::from_bytes(&[])).is_err());
        // unexpected op
        assert!(split_script_sig_redeem(Script::from_bytes(&[0xac])).is_err());
    }

    #[test]
    fn try_nested_error_paths() {
        let mut tx = dummy_tx();
        // Redeem looks like P2WPKH program but wrong outer P2SH hash / spk length
        let redeem = {
            let mut r = vec![0x00, 0x14];
            r.extend([0u8; 20]);
            r
        };
        let mut ss = vec![redeem.len() as u8];
        ss.extend_from_slice(&redeem);
        tx.input[0].script_sig = ScriptBuf::from_bytes(ss);
        let job = ScriptCheckJob {
            prevouts: vec![TxOut {
                value: Amount::from_sat(1),
                script_pubkey: ScriptBuf::from_bytes(vec![0x51]), // not 23-byte P2SH
            }],
            tx: tx.clone(),
            bip65_active: true,
            bip112_active: true,
            bip66_active: true,
            bip16_active: true,
            taproot_active: true,
        };
        let mut cache = SighashCache::new(&job.tx);
        let r = try_p2sh_p2wpkh(&job, 0, &job.tx, &mut cache);
        assert!(matches!(r, Some(Err(_))));

        // Wrong redeem hash
        let job2 = ScriptCheckJob {
            prevouts: vec![TxOut {
                value: Amount::from_sat(1),
                script_pubkey: p2sh_spk(&[0xff]),
            }],
            tx: tx.clone(),
            bip65_active: true,
            bip112_active: true,
            bip66_active: true,
            bip16_active: true,
            taproot_active: true,
        };
        let mut cache2 = SighashCache::new(&job2.tx);
        assert!(matches!(
            try_p2sh_p2wpkh(&job2, 0, &job2.tx, &mut cache2),
            Some(Err(_))
        ));

        // P2WSH redeem shape
        let redeem_wsh = {
            let mut r = vec![0x00, 0x20];
            r.extend([0u8; 32]);
            r
        };
        let mut ss2 = vec![redeem_wsh.len() as u8];
        ss2.extend_from_slice(&redeem_wsh);
        let mut tx3 = dummy_tx();
        tx3.input[0].script_sig = ScriptBuf::from_bytes(ss2);
        let job3 = ScriptCheckJob {
            prevouts: vec![TxOut {
                value: Amount::from_sat(1),
                script_pubkey: ScriptBuf::from_bytes(vec![0x00]), // short spk
            }],
            tx: tx3.clone(),
            bip65_active: true,
            bip112_active: true,
            bip66_active: true,
            bip16_active: true,
            taproot_active: true,
        };
        assert!(matches!(try_p2sh_p2wsh(&job3, 0, &job3.tx), Some(Err(_))));
        // wrong hash
        let job4 = ScriptCheckJob {
            prevouts: vec![TxOut {
                value: Amount::from_sat(1),
                script_pubkey: p2sh_spk(&[0x01]),
            }],
            tx: tx3.clone(),
            bip65_active: true,
            bip112_active: true,
            bip66_active: true,
            bip16_active: true,
            taproot_active: true,
        };
        assert!(matches!(try_p2sh_p2wsh(&job4, 0, &job4.tx), Some(Err(_))));

        // Multi-push → None (fallthrough)
        let mut tx5 = dummy_tx();
        tx5.input[0].script_sig = ScriptBuf::from_bytes(vec![0x01, 0xaa, 0x01, 0xbb]);
        let job5 = ScriptCheckJob {
            prevouts: vec![TxOut {
                value: Amount::from_sat(1),
                script_pubkey: p2sh_spk(&[0xaa]),
            }],
            tx: tx5.clone(),
            bip65_active: true,
            bip112_active: true,
            bip66_active: true,
            bip16_active: true,
            taproot_active: true,
        };
        let mut c5 = SighashCache::new(&job5.tx);
        assert!(try_p2sh_p2wpkh(&job5, 0, &job5.tx, &mut c5).is_none());
        assert!(try_p2sh_p2wsh(&job5, 0, &job5.tx).is_none());

        // Legacy wrong spk / hash / empty
        assert!(verify_p2sh_legacy(&job3, 0, &job3.tx).is_err());
        let mut tx_empty = dummy_tx();
        tx_empty.input[0].script_sig = ScriptBuf::new();
        let job_e = ScriptCheckJob {
            prevouts: vec![TxOut {
                value: Amount::from_sat(1),
                script_pubkey: p2sh_spk(&[0x51]),
            }],
            tx: tx_empty.clone(),
            bip65_active: true,
            bip112_active: true,
            bip66_active: true,
            bip16_active: true,
            taproot_active: true,
        };
        assert!(verify_p2sh_legacy(&job_e, 0, &job_e.tx).is_err());
        // Hash mismatch on legacy
        let mut tx_leg = dummy_tx();
        tx_leg.input[0].script_sig = ScriptBuf::from_bytes(vec![0x01, 0x51]);
        let job_h = ScriptCheckJob {
            prevouts: vec![TxOut {
                value: Amount::from_sat(1),
                script_pubkey: p2sh_spk(&[0xff]),
            }],
            tx: tx_leg.clone(),
            bip65_active: true,
            bip112_active: true,
            bip66_active: true,
            bip16_active: true,
            taproot_active: true,
        };
        assert!(verify_p2sh_legacy(&job_h, 0, &job_h.tx).is_err());
    }
}
