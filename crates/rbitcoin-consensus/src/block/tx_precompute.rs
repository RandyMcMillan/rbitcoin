//! BIP143 consume pin (type lives on Query).

#[cfg(test)]
mod tests {
    use bitcoin::absolute::LockTime;
    use bitcoin::hashes::Hash;
    use bitcoin::script::ScriptBuf;
    use bitcoin::transaction::Version;
    use bitcoin::{Amount, OutPoint, Sequence, Transaction, TxIn, TxOut, Witness};
    use rbitcoin_query::TxPrecompute;

    fn p2wpkh_like() -> Transaction {
        Transaction {
            version: Version::ONE,
            lock_time: LockTime::ZERO,
            input: vec![TxIn {
                previous_output: OutPoint {
                    txid: bitcoin::Txid::from_byte_array([0x11; 32]),
                    vout: 0,
                },
                script_sig: ScriptBuf::new(),
                sequence: Sequence::MAX,
                witness: Witness::from_slice(&[vec![0x30; 71], vec![0x02; 33]]),
            }],
            output: vec![TxOut {
                value: Amount::from_sat(50_000),
                script_pubkey: ScriptBuf::from_bytes(vec![0x51]),
            }],
        }
    }

    #[test]
    fn tx_precompute_bip143_all_matches_sighash_cache() {
        use bitcoin::sighash::{EcdsaSighashType, SighashCache};
        let tx = p2wpkh_like();
        let pre = TxPrecompute::from_tx(&tx);
        let wscript = bitcoin::script::Script::from_bytes(&[0x51]);
        let amt = Amount::from_sat(50_000);
        let ours =
            crate::script::crypto::bip143_p2wsh_signature_hash(&tx, 0, wscript, amt, 0x01, &pre)
                .unwrap();
        let mut cache = SighashCache::new(&tx);
        let theirs = cache
            .p2wsh_signature_hash(0, wscript, amt, EcdsaSighashType::All)
            .unwrap()
            .to_byte_array();
        assert_eq!(ours, theirs);
    }
}
