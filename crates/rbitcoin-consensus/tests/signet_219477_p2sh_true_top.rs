//! Signet height 219477 rejected with false `cleanstack` on legacy P2SH.
//! BIP16 requires a true top only — not witness cleanstack (`len == 1`).

use bitcoin::consensus::deserialize;
use bitcoin::Block;
use std::path::PathBuf;

#[test]
fn block_219477_matches_reject_hash() {
    let path =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/signet_block_219477.bin");
    let b: Block = deserialize(&std::fs::read(path).unwrap()).unwrap();
    assert_eq!(
        format!("{}", b.block_hash()),
        "000000d59c5d06312f71cd887a500cfb3ecdfd8563c5205c4a075ac33ae08fbc"
    );
    // Sanity: block has non-coinbase txs (where the P2SH path runs).
    assert!(b.txdata.len() > 1);
}
