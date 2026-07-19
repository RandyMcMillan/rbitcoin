//! Signet height 204802 rejected with false `p2sh scriptSig multi push`.
//! Nested-segwit probe must not hard-fail multi-push scriptSigs.

use bitcoin::consensus::deserialize;
use bitcoin::Block;
use std::path::PathBuf;

#[test]
fn block_204802_matches_reject_hash() {
    let path =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/signet_block_204802.bin");
    let b: Block = deserialize(&std::fs::read(path).unwrap()).unwrap();
    assert_eq!(
        format!("{}", b.block_hash()),
        "0000004273035bc6ed29b7197e9c7615da498baeedb7d9e1c5edb4479de7ecc4"
    );
}
