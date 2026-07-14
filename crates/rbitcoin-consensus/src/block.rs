use crate::error::ConsensusError;
use crate::milestone::Milestone;
use crate::params::ChainParams;
use bitcoin::absolute::LockTime;
use bitcoin::block::Block;
use bitcoin::consensus::Encodable;
use bitcoin::hashes::{sha256d, Hash};
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
    for tx in block.txdata.iter().skip(1) {
        if tx.is_coinbase() {
            return Err(ConsensusError::BadBlock("coinbase not first"));
        }
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

    // BIP34: coinbase scriptSig starts with height for height >= 1
    if ctx.height.0 >= 1 {
        check_bip34_coinbase(&block.txdata[0], ctx.height.0)?;
    }

    // Witness commitment when any non-coinbase has witness data (or coinbase has witness)
    if block_has_witness(block) {
        check_witness_commitment(block)?;
    }

    let _ = ctx.params;
    Ok(())
}

fn block_has_witness(block: &Block) -> bool {
    block.txdata.iter().any(|tx| {
        tx.input
            .iter()
            .any(|i| !i.witness.is_empty())
    })
}

/// BIP141: coinbase must commit to witness merkle root when segwit is used.
fn check_witness_commitment(block: &Block) -> Result<(), ConsensusError> {
    // Commitment header: 0x6a24aa21a9ed || 32-byte hash
    const MAGIC: [u8; 6] = [0x6a, 0x24, 0xaa, 0x21, 0xa9, 0xed];
    let coinbase = &block.txdata[0];
    let mut commitment: Option<[u8; 32]> = None;
    for out in coinbase.output.iter().rev() {
        let b = out.script_pubkey.as_bytes();
        if b.len() >= 38 && b[0..6] == MAGIC {
            let mut h = [0u8; 32];
            h.copy_from_slice(&b[6..38]);
            commitment = Some(h);
            break;
        }
    }
    let Some(committed) = commitment else {
        return Err(ConsensusError::BadBlock("missing witness commitment"));
    };

    // witness root: merkle of wtxids with coinbase wtxid = zeros
    let mut leaves = Vec::with_capacity(block.txdata.len());
    leaves.push([0u8; 32]); // coinbase wtxid
    for tx in block.txdata.iter().skip(1) {
        leaves.push(tx.compute_wtxid().to_byte_array());
    }
    let witness_root = merkle_root_bytes(&leaves);
    // commitment hash = SHA256D(witness_root || witness_reserved_value)
    // Standard reserved value is 32 zero bytes when not using commitment nonce.
    let reserved = [0u8; 32];
    let mut buf = [0u8; 64];
    buf[0..32].copy_from_slice(&witness_root);
    buf[32..64].copy_from_slice(&reserved);
    let hash = sha256d::Hash::hash(&buf);
    if hash.to_byte_array() != committed {
        // Also accept if witness reserved is in coinbase witness stack (BIP141)
        if coinbase.input[0].witness.len() >= 1 {
            let wr = coinbase.input[0].witness.last().unwrap();
            if wr.len() == 32 {
                let mut buf2 = [0u8; 64];
                buf2[0..32].copy_from_slice(&witness_root);
                buf2[32..64].copy_from_slice(wr);
                let hash2 = sha256d::Hash::hash(&buf2);
                if hash2.to_byte_array() == committed {
                    return Ok(());
                }
            }
        }
        return Err(ConsensusError::BadBlock("witness commitment mismatch"));
    }
    Ok(())
}

fn merkle_root_bytes(leaves: &[[u8; 32]]) -> [u8; 32] {
    if leaves.is_empty() {
        return [0u8; 32];
    }
    let mut layer: Vec<[u8; 32]> = leaves.to_vec();
    while layer.len() > 1 {
        let mut next = Vec::with_capacity(layer.len().div_ceil(2));
        let mut i = 0;
        while i < layer.len() {
            let left = layer[i];
            let right = if i + 1 < layer.len() {
                layer[i + 1]
            } else {
                left
            };
            let mut buf = [0u8; 64];
            buf[0..32].copy_from_slice(&left);
            buf[32..64].copy_from_slice(&right);
            next.push(sha256d::Hash::hash(&buf).to_byte_array());
            i += 2;
        }
        layer = next;
    }
    layer[0]
}

/// BIP34: coinbase scriptSig must start with the block height, encoded as Bitcoin
/// Core's `CScript << int64` push (not raw CScriptNum for small values).
///
/// Core `CScript::push_int64`:
/// - 0 → `OP_0` (0x00)
/// - 1..=16 → `OP_1`..=`OP_16` (0x51..=0x60)
/// - else → minimal CScriptNum (`len || little-endian bytes`, sign-aware)
fn check_bip34_coinbase(coinbase: &Transaction, height: u32) -> Result<(), ConsensusError> {
    let script = &coinbase.input[0].script_sig;
    let bytes = script.as_bytes();
    if bytes.is_empty() {
        return Err(ConsensusError::BadBlock("bip34 coinbase script empty"));
    }
    let expected = bip34_height_script(height);
    if bytes.len() < expected.len() || &bytes[..expected.len()] != expected.as_slice() {
        return Err(ConsensusError::BadBlock("bip34 height encoding"));
    }
    Ok(())
}

/// Serialize `height` the same way Core pushes it into the coinbase scriptSig.
#[must_use]
pub fn bip34_height_script(height: u32) -> Vec<u8> {
    let n = height as i64;
    if n == 0 {
        return vec![0x00]; // OP_0
    }
    if (1..=16).contains(&n) {
        // OP_1 = 0x51 … OP_16 = 0x60
        return vec![0x50 + n as u8];
    }
    // CScriptNum::serialize (minimal signed little-endian) + push length prefix.
    let mut num = Vec::new();
    let mut abs = n;
    let neg = abs < 0;
    if neg {
        abs = -abs;
    }
    while abs > 0 {
        num.push((abs & 0xff) as u8);
        abs >>= 8;
    }
    if let Some(last) = num.last() {
        if last & 0x80 != 0 {
            num.push(if neg { 0x80 } else { 0x00 });
        } else if neg {
            let i = num.len() - 1;
            num[i] |= 0x80;
        }
    } else {
        num.push(0);
    }
    let mut out = Vec::with_capacity(1 + num.len());
    out.push(num.len() as u8);
    out.extend_from_slice(&num);
    out
}

/// Connect checks: prevouts exist, unspent on best chain, maturity, scripts, values, subsidy.
pub fn validate_block_connect(
    query: &Query,
    block: &Block,
    ctx: &ValidationContext<'_>,
) -> Result<(), ConsensusError> {
    let mut block_spends: std::collections::HashSet<OutPoint> = std::collections::HashSet::new();
    let mut fees = 0i64;

    for tx in block.txdata.iter().skip(1) {
        let fee = validate_tx_connect(query, tx, ctx, &mut block_spends)?;
        fees = fees
            .checked_add(fee)
            .ok_or(ConsensusError::BadTx("fee overflow"))?;
    }

    // Coinbase value <= subsidy + fees
    let subsidy = block_subsidy(ctx.height.0, ctx.params);
    let mut coinbase_out = 0i64;
    for o in &block.txdata[0].output {
        coinbase_out = coinbase_out
            .checked_add(o.value.to_sat() as i64)
            .ok_or(ConsensusError::BadBlock("coinbase value overflow"))?;
    }
    let max_cb = subsidy
        .checked_add(fees)
        .ok_or(ConsensusError::BadBlock("subsidy+fees overflow"))?;
    if coinbase_out > max_cb {
        return Err(ConsensusError::BadBlock("coinbase excess value"));
    }

    Ok(())
}

/// Halving subsidy (mainnet schedule; regtest uses same formula with params).
pub fn block_subsidy(height: u32, _params: &ChainParams) -> i64 {
    let halvings = height / 210_000;
    if halvings >= 64 {
        return 0;
    }
    50_0000_0000i64 >> halvings
}

fn validate_tx_connect(
    query: &Query,
    tx: &Transaction,
    ctx: &ValidationContext<'_>,
    block_spends: &mut std::collections::HashSet<OutPoint>,
) -> Result<i64, ConsensusError> {
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
        if !query.spenders(op.txid.as_byte_array(), op.vout)?.is_empty() {
            return Err(ConsensusError::PrevoutSpent);
        }

        let prev_tx = query
            .get_tx_by_txid(op.txid.as_byte_array())?
            .ok_or(ConsensusError::MissingPrevout)?;
        let (prev_fk, prev_rec) = prev_tx;

        // Coinbase maturity: if prev is coinbase, need 100 confirmations.
        if is_coinbase_tx_record(&prev_rec) {
            let created = height_of_strong_tx(query, prev_fk)?;
            let maturity = ctx.params.coinbase_maturity();
            if ctx.height.0 < created.saturating_add(maturity) {
                return Err(ConsensusError::BadTx("coinbase immature"));
            }
        }

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

    verify_scripts(tx, &prevouts)?;

    let _ = LockTime::ZERO;
    Ok(value_in - value_out)
}

fn is_coinbase_tx_record(rec: &rbitcoin_store::TxRecord) -> bool {
    // Coinbase has a single null prevout input.
    if rec.input_count != 1 {
        return false;
    }
    // Decode first input if available via raw is more reliable
    if let Ok(tx) = bitcoin::consensus::deserialize::<Transaction>(&rec.raw) {
        return tx.is_coinbase();
    }
    false
}

fn height_of_strong_tx(query: &Query, tx_fk: rbitcoin_primitives::Fk) -> Result<u32, ConsensusError> {
    // Find which confirmed height lists this tx. Linear scan tip — OK for Phase 4.
    let tip = query
        .tip_height()
        .ok_or(ConsensusError::BadTx("no chain for maturity"))?;
    for h in (0..=tip.0).rev() {
        let list = query.block_tx_fks(Height(h))?;
        if list.iter().any(|fk| *fk == tx_fk) {
            // Only count if still strong
            if query.store().strong_tx.is_strong(tx_fk)? {
                return Ok(h);
            }
        }
    }
    Err(ConsensusError::BadTx("coinbase not strong"))
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
    for (i, input) in tx.input.iter().enumerate() {
        let spk = prevouts[i].script_pubkey.as_script();
        let amount = prevouts[i].value;
        let mut tx_bytes = Vec::new();
        tx.consensus_encode(&mut tx_bytes)
            .map_err(|_| ConsensusError::BadTx("encode"))?;

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


#[cfg(test)]
mod bip34_tests {
    use super::bip34_height_script;

    #[test]
    fn small_heights_use_op_n() {
        assert_eq!(bip34_height_script(0), vec![0x00]);
        assert_eq!(bip34_height_script(1), vec![0x51]); // OP_1 — signet block 1
        assert_eq!(bip34_height_script(16), vec![0x60]);
    }

    #[test]
    fn height_17_uses_push() {
        assert_eq!(bip34_height_script(17), vec![0x01, 0x11]);
    }

    #[test]
    fn height_128_sign_byte() {
        // 128 = 0x80 needs trailing 0x00 so it is not negative
        assert_eq!(bip34_height_script(128), vec![0x02, 0x80, 0x00]);
    }
}
