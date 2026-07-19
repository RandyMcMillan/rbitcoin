//! Signet height 90719 rejected with `tapscript CHECKSIG failed`.
//!
//! Tx `179341…` spends a P2TR script path whose leaf is sequential
//! CHECKSIGVERIFY / CHECKSIG separated by OP_CODESEPARATOR. BIP342 sighash must
//! include the last executed CODESEPARATOR's **instruction index** (Core
//! `opcode_pos`), not the default `0xFFFFFFFF`.

use bitcoin::consensus::deserialize;
use bitcoin::{Amount, Block, OutPoint, ScriptBuf, TxOut};
use rbitcoin_consensus::script_bench::{self, JobBytes};
use std::path::PathBuf;
use std::str::FromStr;

const BLOCK_HASH: &str = "000001425fa8c62dfd856ae0fee3b36add930a5826778f62c54c5e7a089cb2cd";
const SPEND_TXID: &str = "179341698633641e6079171f4a61eb1fe203611df3618e717951f2636a7c5481";
/// Prevout of the CODESEPARATOR tapscript spend (vout 0 of funding tx).
const PREV_VALUE: u64 = 99_639;
const PREV_SPK_HEX: &str = "5120141cf362a850f2bca99e43abca8783cf5db18baadfef55b9769ea285da326c9f";

fn load_block() -> Block {
    let path =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/signet_block_90719.bin");
    deserialize(&std::fs::read(path).expect("fixture")).expect("block")
}

#[test]
fn block_90719_matches_reject_hash() {
    let b = load_block();
    assert_eq!(format!("{}", b.block_hash()), BLOCK_HASH);
    assert_eq!(b.txdata.len(), 14);
}

#[test]
fn block_90719_codeseparator_tapscript_verifies() {
    let b = load_block();
    let tx = b
        .txdata
        .iter()
        .find(|t| t.compute_txid().to_string() == SPEND_TXID)
        .expect("spend tx")
        .clone();

    // Witness: 3×64-byte sigs, leaf script, control block (no annex).
    assert_eq!(tx.input[0].witness.len(), 5);
    let leaf = tx.input[0].witness.nth(3).expect("leaf");
    // Parse opcodes (byte scan of 0xab/0xad is wrong — those appear inside pubkeys).
    use bitcoin::script::{Instruction, Script};
    let mut n_codesep = 0u32;
    let mut n_csv = 0u32;
    let mut n_cs = 0u32;
    for ins in Script::from_bytes(leaf).instructions() {
        match ins.expect("leaf parse") {
            Instruction::Op(op) => match op.to_u8() {
                0xab => n_codesep += 1,
                0xad => n_csv += 1,
                0xac => n_cs += 1,
                _ => {}
            },
            Instruction::PushBytes(_) => {}
        }
    }
    assert_eq!(
        (n_codesep, n_csv, n_cs),
        (2, 2, 1),
        "expected CODESEP×2 CSV×2 CS×1 leaf"
    );

    let spk = ScriptBuf::from_bytes(hex_bytes(PREV_SPK_HEX));
    let prevout = TxOut {
        value: Amount::from_sat(PREV_VALUE),
        script_pubkey: spk,
    };
    let expected_prev = OutPoint {
        txid: bitcoin::Txid::from_str(
            "dce07b6d74ee3740007ac1f1b9a08510d1c897e516abbc009fb34e7b5b2536d3",
        )
        .unwrap(),
        vout: 0,
    };
    assert_eq!(tx.input[0].previous_output, expected_prev);

    let job = JobBytes::new(vec![prevout], tx);
    script_bench::verify_job(&job).expect("BIP342 CODESEPARATOR tapscript must verify");
}

fn hex_bytes(s: &str) -> Vec<u8> {
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
        .collect()
}
