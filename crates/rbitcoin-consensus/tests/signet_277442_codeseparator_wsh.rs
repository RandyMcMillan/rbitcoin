//! Signet height 277442: P2WSH leaf with OP_CSV + OP_CODESEPARATOR + P2PKH-shaped
//! tail. BIP143 scriptCode must be the suffix after CODESEPARATOR (not the full
//! witness script), else CHECKSIG fails with "script false".

use bitcoin::consensus::deserialize;
use bitcoin::Block;
use std::path::PathBuf;

#[test]
fn block_277442_matches_reject_hash() {
    let path =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/signet_block_277442.bin");
    let b: Block = deserialize(&std::fs::read(path).unwrap()).unwrap();
    assert_eq!(
        format!("{}", b.block_hash()),
        "00000006a50036265f927963d06c5c5353317b13a030d01afd6b2c0b2f887a91"
    );
    assert!(b.txdata.len() > 1);
    // Fat WSH-CSV consolidation tx is near the end of the block.
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
