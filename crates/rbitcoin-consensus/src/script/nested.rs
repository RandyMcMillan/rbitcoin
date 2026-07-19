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
