//! Consensus script-edge regression fixtures (signet + mainnet wire blocks).
//!
//! One integration binary keeps link/build cost low; each `#[test]` still pins a
//! unique hash / opcode / verify path (see docs/consensus-tests.md and comments).

use bitcoin::consensus::deserialize;
use bitcoin::script::ScriptBuf;
use bitcoin::{Amount, Block, OutPoint, TxOut};
use rbitcoin_consensus::{verify_scripts_pool, ScriptCheckJob};
use std::path::PathBuf;
use std::str::FromStr;

fn load_block(name: &str) -> Block {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures")
        .join(name);
    deserialize(&std::fs::read(&path).unwrap_or_else(|e| panic!("fixture {name}: {e}")))
        .unwrap_or_else(|e| panic!("block {name}: {e}"))
}

fn hex_bytes(s: &str) -> Vec<u8> {
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
        .collect()
}

// ── signet 200001: OP_CHECKSIGADD (0xba) ─────────────────────────────────────

#[test]
fn block_200001_deserializes_and_matches_reject_hash() {
    let b = load_block("signet_block_200001.bin");
    assert_eq!(b.txdata.len(), 321);
    assert_eq!(
        format!("{}", b.block_hash()),
        "000000ad6bf1ea934186822de99a611924d94aff8fbcb1ad6be2c790c3b92ae1"
    );
}

#[test]
fn block_200001_witnesses_contain_opcode_0xba() {
    let b = load_block("signet_block_200001.bin");
    let mut found = false;
    for tx in &b.txdata {
        for input in &tx.input {
            for i in 0..input.witness.len() {
                let item = input.witness.nth(i).unwrap_or(&[]);
                if item.contains(&0xba) {
                    found = true;
                }
            }
        }
    }
    assert!(
        found,
        "expected 0xba (OP_CHECKSIGADD) in some witness element of block 200001"
    );
}

// ── signet 200945: OP_1SUB (0x8c) ────────────────────────────────────────────

#[test]
fn block_200945_has_op_1sub_and_matches_hash() {
    let b = load_block("signet_block_200945.bin");
    assert_eq!(
        format!("{}", b.block_hash()),
        "00000065c6d2d4cb574038892a535c50efd66f28265a6ab4c48bd121fef795f7"
    );
    let mut found = false;
    for tx in &b.txdata {
        for input in &tx.input {
            for i in 0..input.witness.len() {
                if input.witness.nth(i).unwrap_or(&[]).contains(&0x8c) {
                    found = true;
                }
            }
            if input.script_sig.as_bytes().contains(&0x8c) {
                found = true;
            }
        }
        for o in &tx.output {
            if o.script_pubkey.as_bytes().contains(&0x8c) {
                found = true;
            }
        }
    }
    assert!(found, "expected 0x8c (OP_1SUB) in block 200945");
}

// ── signet 201393: large tapscript (>10k) ────────────────────────────────────

#[test]
fn block_201393_has_witness_script_over_10k() {
    let b = load_block("signet_block_201393.bin");
    assert_eq!(
        format!("{}", b.block_hash()),
        "0000000c49b6e742379a893a51189b6d27140c5d145bcd67121eb9e93744762e"
    );
    let mut max_item = 0usize;
    for tx in &b.txdata {
        for input in &tx.input {
            for i in 0..input.witness.len() {
                max_item = max_item.max(input.witness.nth(i).map(|w| w.len()).unwrap_or(0));
            }
        }
    }
    assert!(
        max_item > 10_000,
        "expected a witness item >10k (tapscript leaf); max={max_item}"
    );
}

// ── signet 204802: P2SH multi-push ───────────────────────────────────────────

#[test]
fn block_204802_matches_reject_hash() {
    let b = load_block("signet_block_204802.bin");
    assert_eq!(
        format!("{}", b.block_hash()),
        "0000004273035bc6ed29b7197e9c7615da498baeedb7d9e1c5edb4479de7ecc4"
    );
}

// ── signet 219477: P2SH cleanstack ───────────────────────────────────────────

#[test]
fn block_219477_matches_reject_hash() {
    let b = load_block("signet_block_219477.bin");
    assert_eq!(
        format!("{}", b.block_hash()),
        "000000d59c5d06312f71cd887a500cfb3ecdfd8563c5205c4a075ac33ae08fbc"
    );
    assert!(b.txdata.len() > 1);
}

// ── signet 277442: CODESEPARATOR + P2WSH CSV ──────────────────────────────────

#[test]
fn block_277442_matches_reject_hash() {
    let b = load_block("signet_block_277442.bin");
    assert_eq!(
        format!("{}", b.block_hash()),
        "00000006a50036265f927963d06c5c5353317b13a030d01afd6b2c0b2f887a91"
    );
    assert!(b.txdata.len() > 1);
    let fat = b
        .txdata
        .iter()
        .find(|t| t.input.len() > 100)
        .expect("fat multi-input tx");
    assert_eq!(
        format!("{}", fat.compute_txid()),
        "540b5d85f73d6eedef68893e70ce3bb52bdad0354a8204a8a43d2340387dc2ff"
    );
}

// ── signet 90719: BIP342 CODESEPARATOR tapscript ─────────────────────────────

const BLOCK_90719_HASH: &str = "000001425fa8c62dfd856ae0fee3b36add930a5826778f62c54c5e7a089cb2cd";
const SPEND_90719_TXID: &str = "179341698633641e6079171f4a61eb1fe203611df3618e717951f2636a7c5481";
const PREV_90719_VALUE: u64 = 99_639;
const PREV_90719_SPK_HEX: &str =
    "5120141cf362a850f2bca99e43abca8783cf5db18baadfef55b9769ea285da326c9f";

#[test]
fn block_90719_matches_reject_hash() {
    let b = load_block("signet_block_90719.bin");
    assert_eq!(format!("{}", b.block_hash()), BLOCK_90719_HASH);
    assert_eq!(b.txdata.len(), 14);
}

#[test]
fn block_90719_codeseparator_tapscript_verifies() {
    let b = load_block("signet_block_90719.bin");
    let tx = b
        .txdata
        .iter()
        .find(|t| t.compute_txid().to_string() == SPEND_90719_TXID)
        .expect("spend tx")
        .clone();

    assert_eq!(tx.input[0].witness.len(), 5);
    let leaf = tx.input[0].witness.nth(3).expect("leaf");
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

    let spk = ScriptBuf::from_bytes(hex_bytes(PREV_90719_SPK_HEX));
    let prevout = TxOut {
        value: Amount::from_sat(PREV_90719_VALUE),
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

    let job = ScriptCheckJob::new(vec![prevout], tx, true, true, true, true, true);
    verify_scripts_pool(&[job]).expect("BIP342 CODESEPARATOR tapscript must verify");
}

// ── mainnet 290329: P2SH FindAndDelete ───────────────────────────────────────

const MAINNET_290329: &[u8] = include_bytes!("fixtures/mainnet_block_290329.bin");
const FAIL_TXID_290329: &str = "5df1375ffe61ac35ca178ebb0cab9ea26dedbd0e96005dfcee7e379fa513232f";

#[test]
fn mainnet_290329_p2sh_multisig_with_embedded_sig_accepts() {
    let block: Block = deserialize(MAINNET_290329).expect("block");
    assert_eq!(
        block.block_hash().to_string(),
        "000000000000000051ac3606d0800821eee065e2b99f8bd652fe7cedb02a1cf5"
    );
    let tx = block
        .txdata
        .iter()
        .find(|t| t.compute_txid().to_string() == FAIL_TXID_290329)
        .expect("tx present");
    assert_eq!(tx.input.len(), 2);

    let prevouts = vec![
        TxOut {
            value: Amount::from_sat(100_000),
            script_pubkey: ScriptBuf::from_bytes(hex_bytes(
                "76a914f6f365c40f0739b61de827a44751e5e99032ed8f88ac",
            )),
        },
        TxOut {
            value: Amount::from_sat(100_000),
            script_pubkey: ScriptBuf::from_bytes(hex_bytes(
                "a914d8dacdadb7462ae15cd906f1878706d0da8660e687",
            )),
        },
    ];

    let job = ScriptCheckJob::new(prevouts, tx.clone(), true, true, false, true, true);
    verify_scripts_pool(&[job]).expect("P2SH CHECKMULTISIG with FindAndDelete");
}
