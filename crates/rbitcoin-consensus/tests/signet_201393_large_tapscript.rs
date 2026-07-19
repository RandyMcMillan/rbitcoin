//! Signet height 201393 rejected with `script too large` under the legacy 10 000
//! byte cap. BIP342 tapscript has **no** explicit script-size limit.

use bitcoin::consensus::deserialize;
use bitcoin::Block;
use std::path::PathBuf;

#[test]
fn block_201393_has_witness_script_over_10k() {
    let path =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/signet_block_201393.bin");
    let b: Block = deserialize(&std::fs::read(&path).expect("fixture")).expect("block");
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
