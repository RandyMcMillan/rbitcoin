//! Regression fixture: signet height 200001 (first post-`milestone=200000` block
//! that failed with `unknown opcode 0xba` = BIP342 OP_CHECKSIGADD).
//!
//! Full script verify needs prevouts from the store; this locks the wire block
//! and that its witnesses carry 0xba. Interpreter coverage lives in
//! `script::interpreter::success_and_disabled_tests`.

use bitcoin::consensus::deserialize;
use bitcoin::Block;
use std::path::PathBuf;

fn fixture() -> Block {
    let path =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/signet_block_200001.bin");
    let bytes = std::fs::read(&path).expect("fixture signet_block_200001.bin");
    deserialize(&bytes).expect("block")
}

#[test]
fn block_200001_deserializes_and_matches_reject_hash() {
    let b = fixture();
    assert_eq!(b.txdata.len(), 321);
    assert_eq!(
        format!("{}", b.block_hash()),
        "000000ad6bf1ea934186822de99a611924d94aff8fbcb1ad6be2c790c3b92ae1"
    );
}

#[test]
fn block_200001_witnesses_contain_opcode_0xba() {
    let b = fixture();
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
