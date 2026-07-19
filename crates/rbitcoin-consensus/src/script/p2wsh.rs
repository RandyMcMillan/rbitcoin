//! Native P2WSH verification (SegWit v0).

use bitcoin::script::Script;
use bitcoin::Transaction;

use super::crypto;
use super::interpreter::{self, EvalContext, SigVersion};
use crate::block::ScriptCheckJob;
use crate::error::ConsensusError;

pub(crate) fn verify(
    job: &ScriptCheckJob,
    input_index: usize,
    tx: &Transaction,
) -> Result<(), ConsensusError> {
    let spk = job.prevouts[input_index].script_pubkey.as_bytes();
    debug_assert!(spk.len() == 34 && spk[0] == 0x00 && spk[1] == 0x20);
    let mut scripthash = [0u8; 32];
    scripthash.copy_from_slice(&spk[2..34]);
    verify_with_scripthash(job, input_index, tx, &scripthash)
}

pub(crate) fn verify_with_scripthash(
    job: &ScriptCheckJob,
    input_index: usize,
    tx: &Transaction,
    scripthash: &[u8; 32],
) -> Result<(), ConsensusError> {
    let input = &tx.input[input_index];
    if input.witness.is_empty() {
        return Err(ConsensusError::Script("p2wsh empty witness".into()));
    }
    // Last witness element is the script; remaining are stack.
    let wit_len = input.witness.len();
    let script_bytes = input
        .witness
        .nth(wit_len - 1)
        .ok_or_else(|| ConsensusError::Script("p2wsh witness".into()))?;
    let actual = crypto::sha256(script_bytes);
    if &actual != scripthash {
        return Err(ConsensusError::Script("p2wsh script hash".into()));
    }

    let mut stack: Vec<Vec<u8>> = Vec::with_capacity(wit_len.saturating_sub(1));
    for i in 0..wit_len - 1 {
        let item = input
            .witness
            .nth(i)
            .ok_or_else(|| ConsensusError::Script("p2wsh witness".into()))?;
        stack.push(item.to_vec());
    }

    let script = Script::from_bytes(script_bytes);
    let ctx = EvalContext::new_with_flags(
        tx,
        input_index,
        job.prevouts[input_index].value,
        &job.prevouts,
        script,
        SigVersion::WitnessV0,
        job.bip65_active,
        job.bip112_active,
        job.bip66_active,
    );
    if interpreter::eval_script(script, &mut stack, &ctx)? {
        interpreter::require_clean_true(&stack)?;
    }
    Ok(())
}
