//! Native P2WPKH verification (SegWit v0).

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
    let spk = job.prevouts[input_index].script_pubkey.as_bytes();
    debug_assert!(spk.len() == 22 && spk[0] == 0x00 && spk[1] == 0x14);
    let keyhash = &spk[2..22];

    let input = &tx.input[input_index];
    // Witness: <sig> <pubkey>
    if input.witness.len() != 2 {
        return Err(ConsensusError::Script("p2wpkh witness len".into()));
    }
    let sig_raw = input
        .witness
        .nth(0)
        .ok_or_else(|| ConsensusError::Script("p2wpkh witness".into()))?;
    let pubkey_raw = input
        .witness
        .nth(1)
        .ok_or_else(|| ConsensusError::Script("p2wpkh witness".into()))?;
    if sig_raw.is_empty() || pubkey_raw.is_empty() {
        return Err(ConsensusError::Script("p2wpkh empty witness item".into()));
    }

    let pk_hash = crypto::hash160(pubkey_raw);
    if pk_hash.as_slice() != keyhash {
        return Err(ConsensusError::Script("p2wpkh pubkey hash".into()));
    }

    // Segwit activates after BIP66 on mainnet; always require strict DER.
    let (sig, sighash_ty) = crypto::parse_der_sig(sig_raw, true)?;
    let pubkey = crypto::parse_pubkey(pubkey_raw)?;

    let amount = job.prevouts[input_index].value;
    let spk_script = job.prevouts[input_index].script_pubkey.as_script();
    // Raw hashtype (not from_consensus→to_u32): non-standard bytes e.g. 0x65.
    let _ = cache; // keep signature for callers that share a cache across inputs
    let sighash = crypto::bip143_p2wpkh_signature_hash(tx, input_index, spk_script, amount, sighash_ty)?;
    if crypto::verify_ecdsa(sighash, &sig, &pubkey) {
        Ok(())
    } else {
        Err(ConsensusError::Script("p2wpkh ecdsa".into()))
    }
}

/// Nested P2SH-P2WPKH: `witness_program` is the 22-byte redeem (not outer P2SH spk).
pub(crate) fn verify_with_keyhash(
    job: &ScriptCheckJob,
    input_index: usize,
    tx: &Transaction,
    keyhash: &[u8; 20],
    witness_program: &[u8],
    cache: &mut SighashCache<&Transaction>,
) -> Result<(), ConsensusError> {
    let input = &tx.input[input_index];
    if input.witness.len() != 2 {
        return Err(ConsensusError::Script("p2wpkh witness len".into()));
    }
    let sig_raw = input
        .witness
        .nth(0)
        .ok_or_else(|| ConsensusError::Script("p2wpkh witness".into()))?;
    let pubkey_raw = input
        .witness
        .nth(1)
        .ok_or_else(|| ConsensusError::Script("p2wpkh witness".into()))?;

    let pk_hash = crypto::hash160(pubkey_raw);
    if &pk_hash != keyhash {
        return Err(ConsensusError::Script("p2wpkh pubkey hash".into()));
    }

    let (sig, sighash_ty) = crypto::parse_der_sig(sig_raw, true)?;
    let pubkey = crypto::parse_pubkey(pubkey_raw)?;

    let amount = job.prevouts[input_index].value;
    let spk = bitcoin::script::Script::from_bytes(witness_program);
    let _ = cache;
    let sighash =
        crypto::bip143_p2wpkh_signature_hash(tx, input_index, spk, amount, sighash_ty)?;
    if crypto::verify_ecdsa(sighash, &sig, &pubkey) {
        Ok(())
    } else {
        Err(ConsensusError::Script("p2wpkh ecdsa".into()))
    }
}
