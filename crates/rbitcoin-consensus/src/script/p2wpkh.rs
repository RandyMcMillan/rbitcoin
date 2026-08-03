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
    let sighash = crypto::bip143_p2wpkh_signature_hash(
        tx,
        input_index,
        spk_script,
        amount,
        sighash_ty,
        cache,
    )?;
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
    let sighash = crypto::bip143_p2wpkh_signature_hash(
        tx,
        input_index,
        spk,
        amount,
        sighash_ty,
        cache,
    )?;
    if crypto::verify_ecdsa(sighash, &sig, &pubkey) {
        Ok(())
    } else {
        Err(ConsensusError::Script("p2wpkh ecdsa".into()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bitcoin::absolute::LockTime;
    use bitcoin::script::ScriptBuf;
    use bitcoin::{Amount, OutPoint, Sequence, TxIn, TxOut, Witness};
    use crate::block::ScriptCheckJob;

    fn job_with_witness(items: &[&[u8]]) -> ScriptCheckJob {
        let mut spk = vec![0x00, 0x14];
        spk.extend([0u8; 20]);
        ScriptCheckJob {
            txid: [0u8; 32],
            prevouts: vec![TxOut {
                value: Amount::from_sat(10),
                script_pubkey: ScriptBuf::from_bytes(spk),
            }],
            tx: crate::block::JobTx::owned(Transaction {
                version: bitcoin::transaction::Version::TWO,
                lock_time: LockTime::ZERO,
                input: vec![TxIn {
                    previous_output: OutPoint::null(),
                    script_sig: ScriptBuf::new(),
                    sequence: Sequence::MAX,
                    witness: Witness::from_slice(items),
                }],
                output: vec![TxOut {
                    value: Amount::from_sat(1),
                    script_pubkey: ScriptBuf::from_bytes(vec![0x51]),
                }],
            }),
            bip65_active: true,
            bip112_active: true,
            bip66_active: true,
            bip16_active: true,
            taproot_active: true,
        }
    }

    #[test]
    fn witness_shape_errors() {
        let job = job_with_witness(&[]);
        let mut cache = SighashCache::new(&*job.tx);
        assert!(verify(&job, 0, &*job.tx, &mut cache).is_err());

        let job = job_with_witness(&[&[0x01]]);
        let mut cache = SighashCache::new(&*job.tx);
        assert!(verify(&job, 0, &*job.tx, &mut cache).is_err());

        let job = job_with_witness(&[&[], &[0x02; 33]]);
        let mut cache = SighashCache::new(&*job.tx);
        assert!(verify(&job, 0, &*job.tx, &mut cache).is_err());

        let job = job_with_witness(&[&[0x30, 0x01, 0x01, 0x01], &[0x02; 33]]);
        let mut cache = SighashCache::new(&*job.tx);
        let err = verify(&job, 0, &*job.tx, &mut cache).unwrap_err();
        assert!(format!("{err}").contains("p2wpkh"));

        let redeem = {
            let mut r = vec![0x00, 0x14];
            r.extend([0u8; 20]);
            r
        };
        let job = job_with_witness(&[&[0x01]]);
        let mut cache = SighashCache::new(&*job.tx);
        assert!(verify_with_keyhash(&job, 0, &*job.tx, &[0u8; 20], &redeem, &mut cache).is_err());
    }
}
