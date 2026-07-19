//! Signet height 200945 rejected with `unknown opcode 0x8c` (OP_1SUB).

use bitcoin::consensus::deserialize;
use bitcoin::Block;
use std::path::PathBuf;

#[test]
fn block_200945_has_op_1sub_and_matches_hash() {
    let path =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/signet_block_200945.bin");
    let b: Block = deserialize(&std::fs::read(path).unwrap()).unwrap();
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
