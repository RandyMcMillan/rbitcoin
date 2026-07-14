use crate::error::ConsensusError;
use bitcoin::block::Header;
use bitcoin::consensus::Encodable;
use bitcoin::hashes::Hash;
use bitcoin::Transaction;
// Hash trait for Txid/BlockHash byte conversion
use rbitcoin_primitives::Fk;
use rbitcoin_query::{Query, TxApply};
use rbitcoin_store::{HeaderRecord, InputRecord, OutputRecord, TxRecord};

pub fn header_to_record(prev_fk: Fk, header: &Header) -> HeaderRecord {
    HeaderRecord {
        prev_fk,
        version: header.version.to_consensus(),
        timestamp: header.time,
        bits: header.bits.to_consensus(),
        nonce: header.nonce,
        merkle_root: header.merkle_root.to_byte_array(),
        hash: header.block_hash().to_byte_array(),
    }
}

pub fn block_to_apply(
    query: &Query,
    header: &Header,
    txs: &[Transaction],
) -> Result<(HeaderRecord, Vec<TxApply>), ConsensusError> {
    let prev_fk = if header.prev_blockhash.to_byte_array() == [0u8; 32] {
        Fk::NULL
    } else {
        query
            .get_header_by_hash(header.prev_blockhash.as_byte_array())?
            .map(|(fk, _)| fk)
            .ok_or(ConsensusError::BadPrev)?
    };
    let header_rec = header_to_record(prev_fk, header);
    let mut out = Vec::with_capacity(txs.len());
    for tx in txs {
        out.push(tx_to_apply(tx)?);
    }
    Ok((header_rec, out))
}

fn tx_to_apply(tx: &Transaction) -> Result<TxApply, ConsensusError> {
    let mut raw = Vec::new();
    tx.consensus_encode(&mut raw)
        .map_err(|_| ConsensusError::BadTx("encode"))?;
    let txid = tx.compute_txid().to_byte_array();

    let inputs: Vec<InputRecord> = tx
        .input
        .iter()
        .enumerate()
        .map(|(i, inp)| InputRecord {
            parent_tx_fk: Fk::NULL,
            index: i as u32,
            prev_txid: inp.previous_output.txid.to_byte_array(),
            prev_index: inp.previous_output.vout,
            sequence: inp.sequence.to_consensus_u32(),
            script_sig: inp.script_sig.to_bytes(),
        })
        .collect();

    let outputs: Vec<OutputRecord> = tx
        .output
        .iter()
        .enumerate()
        .map(|(i, o)| OutputRecord {
            parent_tx_fk: Fk::NULL,
            index: i as u32,
            value: o.value.to_sat() as i64,
            script: o.script_pubkey.to_bytes(),
        })
        .collect();

    Ok(TxApply {
        tx: TxRecord {
            txid,
            version: tx.version.0,
            locktime: tx.lock_time.to_consensus_u32(),
            input_start_fk: Fk::NULL,
            input_count: inputs.len() as u32,
            output_start_fk: Fk::NULL,
            output_count: outputs.len() as u32,
            raw,
        },
        inputs,
        outputs,
    })
}
