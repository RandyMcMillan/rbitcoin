//! Regression: mainnet block 290329 tx#416 P2SH CHECKMULTISIG.
//!
//! The redeem script embeds one of the signatures as a data push. Core's
//! CHECKMULTISIG FindAndDeletes **all** stack signatures from scriptCode before
//! ECDSA checks. Without that, SIGHASH_ALL fails with `script false` and IBD
//! rejects a valid tip block.
//!
//! Fixture: full block binary under tests/fixtures/mainnet_block_290329.bin
//! (hash 000000000000000051ac3606d0800821eee065e2b99f8bd652fe7cedb02a1cf5).
//! Prevouts for the failing tx are inlined (values + scriptPubKeys only).

use bitcoin::consensus::deserialize;
use bitcoin::script::ScriptBuf;
use bitcoin::{Amount, Block, TxOut};
use rbitcoin_consensus::script_bench::{self, JobBytes};

const BLOCK: &[u8] = include_bytes!("fixtures/mainnet_block_290329.bin");
const FAIL_TXID: &str = "5df1375ffe61ac35ca178ebb0cab9ea26dedbd0e96005dfcee7e379fa513232f";

#[test]
fn mainnet_290329_p2sh_multisig_with_embedded_sig_accepts() {
    let block: Block = deserialize(BLOCK).expect("block");
    assert_eq!(
        block.block_hash().to_string(),
        "000000000000000051ac3606d0800821eee065e2b99f8bd652fe7cedb02a1cf5"
    );
    let tx = block
        .txdata
        .iter()
        .find(|t| t.compute_txid().to_string() == FAIL_TXID)
        .expect("tx present");
    assert_eq!(tx.input.len(), 2);

    // Prevouts from mainnet (blockstream / store): P2PKH then P2SH.
    let prevouts = vec![
        TxOut {
            value: Amount::from_sat(100_000),
            script_pubkey: ScriptBuf::from_bytes(hex(
                "76a914f6f365c40f0739b61de827a44751e5e99032ed8f88ac",
            )),
        },
        TxOut {
            value: Amount::from_sat(100_000),
            script_pubkey: ScriptBuf::from_bytes(hex(
                "a914d8dacdadb7462ae15cd906f1878706d0da8660e687",
            )),
        },
    ];

    // Height 290329: BIP16 on, BIP66 not yet (363725).
    let mut job = JobBytes::new(prevouts, tx.clone());
    job.bip16_active = true;
    job.bip66_active = false;
    script_bench::verify_job(&job).expect("P2SH CHECKMULTISIG with FindAndDelete");
}

fn hex(s: &str) -> Vec<u8> {
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
        .collect()
}
