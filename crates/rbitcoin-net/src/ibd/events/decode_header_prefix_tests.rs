//! Tests for super:: events helpers (peeled from events.rs).

use super::decode_block_header_prefix;
use bitcoin::consensus::Encodable;
use rbitcoin_consensus::{genesis_block, ChainParams};

#[test]
fn decode_block_header_prefix_surface() {
    assert!(decode_block_header_prefix(&[]).is_none());
    assert!(decode_block_header_prefix(&[0u8; 79]).is_none());
    let g = genesis_block(&ChainParams::regtest());
    let mut enc = Vec::new();
    g.header.consensus_encode(&mut enc).unwrap();
    assert!(enc.len() >= 80);
    let h = decode_block_header_prefix(&enc).expect("header");
    assert_eq!(h, g.header);
    // Extra payload after 80 bytes is ignored.
    enc.extend_from_slice(&[0xde, 0xad]);
    let h2 = decode_block_header_prefix(&enc).expect("header+tail");
    assert_eq!(h2, g.header);
    // All-zeros is still a consensus-decodable header shape.
    let zeros = decode_block_header_prefix(&[0u8; 80]);
    assert!(zeros.is_some());
}
