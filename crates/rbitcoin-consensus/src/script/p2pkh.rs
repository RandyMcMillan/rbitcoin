//! Native P2PKH verification (legacy).

use bitcoin::hashes::Hash;
use bitcoin::script::{Instruction, Script};
use bitcoin::sighash::SighashCache;
use bitcoin::Transaction;

use super::crypto;
use crate::block::ScriptCheckJob;
use crate::error::ConsensusError;

pub(crate) fn verify(
    job: &ScriptCheckJob,
    input_index: usize,
    tx: &Transaction,
    cache: &mut SighashCache<&Transaction>,
) -> Result<(), ConsensusError> {
    let _ = tx;
    let spk = job.prevouts[input_index].script_pubkey.as_bytes();
    // 76 a9 14 <20> 88 ac
    debug_assert!(spk.len() == 25);
    let keyhash = &spk[3..23];

    let input = &tx.input[input_index];
    // scriptSig: <sig> <pubkey>
    let (sig_raw, pubkey_raw) = parse_two_pushes(input.script_sig.as_script())?;

    let pk_hash = crypto::hash160(&pubkey_raw);
    if pk_hash.as_slice() != keyhash {
        return Err(ConsensusError::Script("p2pkh pubkey hash".into()));
    }

    let (sig, sighash_ty) = crypto::parse_der_sig(&sig_raw, job.bip66_active)?;
    let pubkey = crypto::parse_pubkey(&pubkey_raw)?;

    let script_code = job.prevouts[input_index].script_pubkey.as_script();
    // Raw hashtype byte (may be 0 — must not normalize to SIGHASH_ALL).
    let sighash = cache
        .legacy_signature_hash(input_index, script_code, sighash_ty)
        .map_err(|_| ConsensusError::Script("p2pkh sighash".into()))?;
    if crypto::verify_ecdsa(sighash.to_byte_array(), &sig, &pubkey) {
        Ok(())
    } else {
        Err(ConsensusError::Script("p2pkh ecdsa".into()))
    }
}

fn parse_two_pushes(script: &Script) -> Result<(Vec<u8>, Vec<u8>), ConsensusError> {
    let mut items = Vec::with_capacity(2);
    for ins in script.instructions() {
        match ins.map_err(|_| ConsensusError::Script("p2pkh scriptSig".into()))? {
            Instruction::PushBytes(b) => items.push(b.as_bytes().to_vec()),
            Instruction::Op(op) if op.to_u8() >= 0x51 && op.to_u8() <= 0x60 => {
                // OP_1..OP_16 — not expected for P2PKH sig/pubkey
                return Err(ConsensusError::Script("p2pkh scriptSig op".into()));
            }
            Instruction::Op(_) => {
                return Err(ConsensusError::Script("p2pkh scriptSig unexpected op".into()));
            }
        }
    }
    if items.len() != 2 {
        return Err(ConsensusError::Script("p2pkh scriptSig len".into()));
    }
    Ok((items[0].clone(), items[1].clone()))
}
