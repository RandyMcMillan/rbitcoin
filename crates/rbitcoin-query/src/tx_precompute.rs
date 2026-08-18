//! One-pass txid/wtxid/weight + BIP143/BIP341 common hashes (Core
//! `PrecomputedTransactionData` shape).
//!
//! Structure and scripts share this. Spent amounts/scripts are filled later
//! via [`TxPrecompute::finish_spent`] when prevouts exist.

use bitcoin::consensus::encode::{Encodable, VarInt};
use bitcoin::hashes::{sha256, sha256d, Hash};
use bitcoin::{Transaction, TxOut};

/// Job-local hash cache: this tx's ids plus Core-style common midstates.
#[derive(Clone, Debug)]
pub struct TxPrecompute {
    pub txid: [u8; 32],
    pub wtxid: [u8; 32],
    pub base_size: usize,
    pub total_size: usize,
    pub sigops: u64,
    pub out_sum: u64,
    /// BIP144 serialization (`uses_segwit_serialization`).
    pub has_witness: bool,
    /// Single SHA256 of all outpoints (BIP341 / rust-bitcoin `CommonCache`).
    pub sha_prevouts: [u8; 32],
    pub sha_sequences: [u8; 32],
    pub sha_outputs: [u8; 32],
    /// Single SHA256 of spent amounts / scriptPubKeys (after [`Self::finish_spent`]).
    pub sha_amounts: Option<[u8; 32]>,
    pub sha_scriptpubkeys: Option<[u8; 32]>,
}

impl TxPrecompute {
    /// One walk of `tx`. Does not hash spent prevouts.
    pub fn from_tx(tx: &Transaction) -> Self {
        let has_witness = uses_segwit_serialization(tx);

        let mut txid_eng = sha256d::Hash::engine();
        let mut wtxid_eng = sha256d::Hash::engine();
        let mut sha_prev = sha256::Hash::engine();
        let mut sha_seq = sha256::Hash::engine();
        let mut sha_out = sha256::Hash::engine();
        let mut base_size = 0usize;
        let mut total_size = 0usize;
        let mut sigops = 0u64;
        let mut out_sum = 0u64;

        base_size += enc(&mut txid_eng, &tx.version);
        total_size += enc(&mut wtxid_eng, &tx.version);
        if has_witness {
            total_size += enc(&mut wtxid_eng, &0u8);
            total_size += enc(&mut wtxid_eng, &1u8);
        }

        let n_in = VarInt(tx.input.len() as u64);
        base_size += enc(&mut txid_eng, &n_in);
        total_size += enc(&mut wtxid_eng, &n_in);
        for txin in &tx.input {
            base_size += enc(&mut txid_eng, &txin.previous_output);
            total_size += enc(&mut wtxid_eng, &txin.previous_output);
            let _ = txin.previous_output.consensus_encode(&mut sha_prev);

            base_size += enc(&mut txid_eng, &txin.script_sig);
            total_size += enc(&mut wtxid_eng, &txin.script_sig);
            sigops = sigops.saturating_add(script_sigop_count(txin.script_sig.as_bytes(), false));

            base_size += enc(&mut txid_eng, &txin.sequence);
            total_size += enc(&mut wtxid_eng, &txin.sequence);
            let _ = txin.sequence.consensus_encode(&mut sha_seq);
        }

        let n_out = VarInt(tx.output.len() as u64);
        base_size += enc(&mut txid_eng, &n_out);
        total_size += enc(&mut wtxid_eng, &n_out);
        for txout in &tx.output {
            base_size += enc(&mut txid_eng, txout);
            total_size += enc(&mut wtxid_eng, txout);
            let _ = txout.consensus_encode(&mut sha_out);
            sigops =
                sigops.saturating_add(script_sigop_count(txout.script_pubkey.as_bytes(), false));
            let v = txout.value.to_sat();
            out_sum = out_sum.saturating_add(v);
        }

        if has_witness {
            for txin in &tx.input {
                total_size += enc(&mut wtxid_eng, &txin.witness);
            }
        }

        base_size += enc(&mut txid_eng, &tx.lock_time);
        total_size += enc(&mut wtxid_eng, &tx.lock_time);

        Self {
            txid: sha256d::Hash::from_engine(txid_eng).to_byte_array(),
            wtxid: sha256d::Hash::from_engine(wtxid_eng).to_byte_array(),
            base_size,
            total_size,
            sigops,
            out_sum,
            has_witness,
            sha_prevouts: sha256::Hash::from_engine(sha_prev).to_byte_array(),
            sha_sequences: sha256::Hash::from_engine(sha_seq).to_byte_array(),
            sha_outputs: sha256::Hash::from_engine(sha_out).to_byte_array(),
            sha_amounts: None,
            sha_scriptpubkeys: None,
        }
    }

    /// BIP143 `hashPrevouts` = SHA256(sha_prevouts).
    pub fn hash_prevouts(&self) -> [u8; 32] {
        sha256_again(&self.sha_prevouts)
    }

    pub fn hash_sequence(&self) -> [u8; 32] {
        sha256_again(&self.sha_sequences)
    }

    pub fn hash_outputs(&self) -> [u8; 32] {
        sha256_again(&self.sha_outputs)
    }

    pub fn weight_wu(&self) -> u64 {
        (self
            .base_size
            .saturating_mul(3)
            .saturating_add(self.total_size)) as u64
    }

    /// BIP341 spent midstates. Call when `prevouts.len() == tx.input.len()`.
    pub fn finish_spent(&mut self, prevouts: &[TxOut]) {
        let mut enc_amt = sha256::Hash::engine();
        let mut enc_spk = sha256::Hash::engine();
        for prev in prevouts {
            let _ = prev.value.consensus_encode(&mut enc_amt);
            let _ = prev.script_pubkey.consensus_encode(&mut enc_spk);
        }
        self.sha_amounts = Some(sha256::Hash::from_engine(enc_amt).to_byte_array());
        self.sha_scriptpubkeys = Some(sha256::Hash::from_engine(enc_spk).to_byte_array());
    }
}

fn enc(w: &mut impl bitcoin::io::Write, v: &impl Encodable) -> usize {
    v.consensus_encode(w).expect("hash engines do not error")
}

fn sha256_again(single: &[u8; 32]) -> [u8; 32] {
    sha256::Hash::from_byte_array(*single)
        .hash_again()
        .to_byte_array()
}

/// rust-bitcoin `Transaction::uses_segwit_serialization` (private).
fn uses_segwit_serialization(tx: &Transaction) -> bool {
    if tx.input.iter().any(|i| !i.witness.is_empty()) {
        return true;
    }
    tx.input.is_empty()
}

/// Core-style legacy sigop count (CHECKSIG=1, CHECKMULTISIG=20).
fn script_sigop_count(script: &[u8], accurate: bool) -> u64 {
    let mut n = 0u64;
    let mut i = 0usize;
    let mut last_opcode = 0xffu8;
    while i < script.len() {
        let opcode = script[i];
        i += 1;
        if opcode <= 0x4b {
            let push = opcode as usize;
            i = i.saturating_add(push);
        } else if opcode == 0x4c && i < script.len() {
            let push = script[i] as usize;
            i = i.saturating_add(1 + push);
        } else if opcode == 0x4d && i + 1 < script.len() {
            let push = u16::from_le_bytes([script[i], script[i + 1]]) as usize;
            i = i.saturating_add(2 + push);
        } else if opcode == 0x4e && i + 3 < script.len() {
            let push = u32::from_le_bytes(script[i..i + 4].try_into().unwrap_or([0; 4])) as usize;
            i = i.saturating_add(4 + push);
        } else if opcode == 0xac || opcode == 0xad {
            n = n.saturating_add(1);
        } else if opcode == 0xae || opcode == 0xaf {
            if accurate && last_opcode >= 0x51 && last_opcode <= 0x60 {
                n = n.saturating_add(u64::from(last_opcode - 0x50));
            } else {
                n = n.saturating_add(20);
            }
        }
        last_opcode = opcode;
    }
    n
}

#[cfg(test)]
mod tests {
    use super::*;
    use bitcoin::absolute::LockTime;
    use bitcoin::consensus::Encodable;
    use bitcoin::hashes::Hash;
    use bitcoin::script::ScriptBuf;
    use bitcoin::transaction::Version;
    use bitcoin::{Amount, OutPoint, Sequence, TxIn, Witness};

    fn oracle_sha_prevouts(tx: &Transaction) -> [u8; 32] {
        let mut e = sha256::Hash::engine();
        for i in &tx.input {
            i.previous_output.consensus_encode(&mut e).unwrap();
        }
        sha256::Hash::from_engine(e).to_byte_array()
    }

    fn oracle_sha_sequences(tx: &Transaction) -> [u8; 32] {
        let mut e = sha256::Hash::engine();
        for i in &tx.input {
            i.sequence.consensus_encode(&mut e).unwrap();
        }
        sha256::Hash::from_engine(e).to_byte_array()
    }

    fn oracle_sha_outputs(tx: &Transaction) -> [u8; 32] {
        let mut e = sha256::Hash::engine();
        for o in &tx.output {
            o.consensus_encode(&mut e).unwrap();
        }
        sha256::Hash::from_engine(e).to_byte_array()
    }

    fn oracle_sigops(tx: &Transaction) -> u64 {
        let mut n = 0u64;
        for inp in &tx.input {
            n = n.saturating_add(script_sigop_count(inp.script_sig.as_bytes(), false));
        }
        for out in &tx.output {
            n = n.saturating_add(script_sigop_count(out.script_pubkey.as_bytes(), false));
        }
        n
    }

    fn assert_matches_rust_bitcoin(tx: &Transaction) {
        let p = TxPrecompute::from_tx(tx);
        assert_eq!(p.txid, tx.compute_txid().to_byte_array(), "txid");
        assert_eq!(p.wtxid, tx.compute_wtxid().to_byte_array(), "wtxid");
        assert_eq!(p.base_size, tx.base_size(), "base_size");
        assert_eq!(p.total_size, tx.total_size(), "total_size");
        assert_eq!(p.weight_wu(), tx.weight().to_wu(), "weight");
        assert_eq!(p.sigops, oracle_sigops(tx), "sigops");
        assert_eq!(p.sha_prevouts, oracle_sha_prevouts(tx), "sha_prevouts");
        assert_eq!(p.sha_sequences, oracle_sha_sequences(tx), "sha_sequences");
        assert_eq!(p.sha_outputs, oracle_sha_outputs(tx), "sha_outputs");
        assert_eq!(
            p.hash_prevouts(),
            sha256::Hash::from_byte_array(p.sha_prevouts)
                .hash_again()
                .to_byte_array()
        );
    }

    fn legacy_1in() -> Transaction {
        Transaction {
            version: Version::ONE,
            lock_time: LockTime::ZERO,
            input: vec![TxIn {
                previous_output: OutPoint {
                    txid: bitcoin::Txid::from_byte_array([0x11; 32]),
                    vout: 0,
                },
                script_sig: ScriptBuf::from_bytes(vec![0x51, 0x51]),
                sequence: Sequence::MAX,
                witness: Witness::new(),
            }],
            output: vec![TxOut {
                value: Amount::from_sat(50_000),
                script_pubkey: ScriptBuf::from_bytes(vec![0x51]),
            }],
        }
    }

    fn p2wpkh_like() -> Transaction {
        let mut tx = legacy_1in();
        tx.input[0].script_sig = ScriptBuf::new();
        tx.input[0].witness = Witness::from_slice(&[vec![0x30; 71], vec![0x02; 33]]);
        tx
    }

    #[test]
    fn tx_precompute_matches_legacy() {
        assert_matches_rust_bitcoin(&legacy_1in());
    }

    #[test]
    fn tx_precompute_matches_p2wpkh_witness() {
        assert_matches_rust_bitcoin(&p2wpkh_like());
    }

    #[test]
    fn tx_precompute_matches_zero_input_bip144() {
        let tx = Transaction {
            version: Version::TWO,
            lock_time: LockTime::ZERO,
            input: vec![],
            output: vec![TxOut {
                value: Amount::from_sat(1),
                script_pubkey: ScriptBuf::from_bytes(vec![0x51]),
            }],
        };
        assert_matches_rust_bitcoin(&tx);
        assert!(TxPrecompute::from_tx(&tx).has_witness);
    }

    #[test]
    fn tx_precompute_finish_spent_matches_amount_spk_walk() {
        let tx = p2wpkh_like();
        let prev = TxOut {
            value: Amount::from_sat(100_000),
            script_pubkey: ScriptBuf::from_bytes(
                vec![0x00, 0x14].into_iter().chain([0xab; 20]).collect(),
            ),
        };
        let mut p = TxPrecompute::from_tx(&tx);
        p.finish_spent(std::slice::from_ref(&prev));
        let mut ea = sha256::Hash::engine();
        let mut es = sha256::Hash::engine();
        prev.value.consensus_encode(&mut ea).unwrap();
        prev.script_pubkey.consensus_encode(&mut es).unwrap();
        assert_eq!(
            p.sha_amounts,
            Some(sha256::Hash::from_engine(ea).to_byte_array())
        );
        assert_eq!(
            p.sha_scriptpubkeys,
            Some(sha256::Hash::from_engine(es).to_byte_array())
        );
    }
}
