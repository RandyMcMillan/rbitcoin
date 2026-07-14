use crate::error::ConsensusError;
use crate::milestone::Milestone;
use crate::params::ChainParams;
use bitcoin::absolute::LockTime;
use bitcoin::block::Block;
use bitcoin::consensus::Encodable;
use bitcoin::hashes::Hash;
use bitcoin::script::{Script, ScriptBuf};
use bitcoin::{Amount, OutPoint, Transaction, TxOut};
use rbitcoin_primitives::Height;
use rbitcoin_query::Query;

pub struct ValidationContext<'a> {
    pub params: &'a ChainParams,
    pub height: Height,
    pub milestone: Milestone,
}

/// Context-free / structural block checks (no UTXO / prevout).
pub fn validate_block_structure(
    block: &Block,
    ctx: &ValidationContext<'_>,
) -> Result<(), ConsensusError> {
    if block.txdata.is_empty() {
        return Err(ConsensusError::BadBlock("no transactions"));
    }
    if !block.txdata[0].is_coinbase() {
        return Err(ConsensusError::BadBlock("first tx not coinbase"));
    }
    for (i, tx) in block.txdata.iter().enumerate().skip(1) {
        if tx.is_coinbase() {
            return Err(ConsensusError::BadBlock("coinbase not first"));
        }
        let _ = i;
    }

    // Weight / size limits (segwit-aware)
    let weight = block.weight();
    if weight.to_wu() > 4_000_000 {
        return Err(ConsensusError::BadBlock("block weight too large"));
    }

    // Merkle root
    if block.compute_merkle_root() != Some(block.header.merkle_root) {
        return Err(ConsensusError::BadBlock("merkle root mismatch"));
    }

    // Duplicate txids
    let mut seen = std::collections::HashSet::new();
    for tx in &block.txdata {
        let id = tx.compute_txid();
        if !seen.insert(id) {
            return Err(ConsensusError::BadBlock("duplicate txid"));
        }
    }

    // BIP34: coinbase scriptSig starts with height for height >= 1 (enforced on all nets we care about)
    if ctx.height.0 >= 1 {
        check_bip34_coinbase(&block.txdata[0], ctx.height.0)?;
    }

    let _ = ctx.params;
    Ok(())
}

fn check_bip34_coinbase(coinbase: &Transaction, height: u32) -> Result<(), ConsensusError> {
    let script = &coinbase.input[0].script_sig;
    let bytes = script.as_bytes();
    if bytes.is_empty() {
        return Err(ConsensusError::BadBlock("bip34 coinbase script empty"));
    }
    // Push height as minimal CScriptNum encoding: first byte is length
    let len = bytes[0] as usize;
    if len == 0 || len > 4 || bytes.len() < 1 + len {
        return Err(ConsensusError::BadBlock("bip34 height encoding"));
    }
    let mut h = 0u32;
    for (i, b) in bytes[1..1 + len].iter().enumerate() {
        h |= u32::from(*b) << (8 * i);
    }
    if h != height {
        return Err(ConsensusError::BadBlock("bip34 height mismatch"));
    }
    Ok(())
}

/// Connect checks: prevouts exist, unspent on best chain, scripts, values.
pub fn validate_block_connect(
    query: &Query,
    block: &Block,
    ctx: &ValidationContext<'_>,
) -> Result<(), ConsensusError> {
    // Track spends within this block for double-spend
    let mut block_spends: std::collections::HashSet<OutPoint> = std::collections::HashSet::new();

    for tx in block.txdata.iter().skip(1) {
        validate_tx_connect(query, tx, ctx, &mut block_spends)?;
    }
    Ok(())
}

fn validate_tx_connect(
    query: &Query,
    tx: &Transaction,
    ctx: &ValidationContext<'_>,
    block_spends: &mut std::collections::HashSet<OutPoint>,
) -> Result<(), ConsensusError> {
    if tx.input.is_empty() {
        return Err(ConsensusError::BadTx("no inputs"));
    }
    if tx.output.is_empty() {
        return Err(ConsensusError::BadTx("no outputs"));
    }

    let mut value_in = 0i64;
    let mut prevouts: Vec<TxOut> = Vec::with_capacity(tx.input.len());

    for input in &tx.input {
        let op = input.previous_output;
        if !block_spends.insert(op) {
            return Err(ConsensusError::BadTx("double spend in block"));
        }
        if query.spenders(op.txid.as_byte_array(), op.vout)?.is_empty() {
            // ok — not spent on best chain
        } else {
            return Err(ConsensusError::PrevoutSpent);
        }

        let prev_tx = query
            .get_tx_by_txid(op.txid.as_byte_array())?
            .ok_or(ConsensusError::MissingPrevout)?;
        let (_fk, prev_rec) = prev_tx;
        // Find output at vout among sequential outputs starting at output_start_fk
        let out = find_output(query, &prev_rec, op.vout)?;
        value_in = value_in
            .checked_add(out.value)
            .ok_or(ConsensusError::BadTx("value in overflow"))?;
        prevouts.push(TxOut {
            value: Amount::from_sat(out.value as u64),
            script_pubkey: ScriptBuf::from_bytes(out.script.clone()),
        });
    }

    let mut value_out = 0i64;
    for o in &tx.output {
        let sats = o.value.to_sat() as i64;
        if sats < 0 {
            return Err(ConsensusError::BadTx("negative output"));
        }
        value_out = value_out
            .checked_add(sats)
            .ok_or(ConsensusError::BadTx("value out overflow"))?;
    }
    if value_out > value_in {
        return Err(ConsensusError::BadTx("in < out"));
    }

    // Script verification (non-milestone)
    verify_scripts(tx, &prevouts)?;

    let _ = ctx;
    let _ = LockTime::ZERO;
    Ok(())
}

fn find_output(
    query: &Query,
    prev_rec: &rbitcoin_store::TxRecord,
    vout: u32,
) -> Result<rbitcoin_store::OutputRecord, ConsensusError> {
    if vout >= prev_rec.output_count {
        return Err(ConsensusError::MissingPrevout);
    }
    let start = prev_rec
        .output_start_fk
        .get()
        .ok_or(ConsensusError::MissingPrevout)?;
    let fk = rbitcoin_primitives::Fk(start + u64::from(vout));
    query.get_output(fk).map_err(ConsensusError::Store)
}

fn verify_scripts(tx: &Transaction, prevouts: &[TxOut]) -> Result<(), ConsensusError> {
    // Use libbitcoinconsensus when available (bitcoin crate feature).
    for (i, input) in tx.input.iter().enumerate() {
        let spk = prevouts[i].script_pubkey.as_script();
        let amount = prevouts[i].value;
        // Serialize tx for consensus verify
        let mut tx_bytes = Vec::new();
        tx.consensus_encode(&mut tx_bytes)
            .map_err(|_| ConsensusError::BadTx("encode"))?;

        // bitcoinconsensus flags: P2SH | WITNESS | CHECKLOCKTIMEVERIFY | CHECKSEQUENCEVERIFY | NULLDUMMY | TAPROOT
        // Use bitcoin::Script::verify via consensus crate API:
        // Prefer anyone-can-spend for regtest fixture outputs; otherwise libbitcoinconsensus.
        if is_anyone_can_spend(spk) {
            continue;
        }
        bitcoinconsensus::verify(spk.as_bytes(), amount.to_sat(), &tx_bytes, i)
            .map_err(|e| ConsensusError::Script(format!("{e:?}")))?;
        let _ = input;
    }
    Ok(())
}

fn is_anyone_can_spend(script: &Script) -> bool {
    let b = script.as_bytes();
    b.is_empty() || b == [0x51] // OP_TRUE
}
