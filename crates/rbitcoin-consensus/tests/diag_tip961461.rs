//! Offline forensics / host confirm of tip+1.
use rbitcoin_consensus::{confirm_wire_run, ChainParams, Milestone};
use rbitcoin_primitives::Height;
use rbitcoin_query::Query;
use std::path::PathBuf;

fn parse_hex(s: &str) -> Vec<u8> {
    (0..s.len()).step_by(2).map(|i| u8::from_str_radix(&s[i..i+2], 16).unwrap()).collect()
}

#[test]
#[ignore = "host datadir forensics only"]
fn diag_confirm_wire_run_961461() {
    let dir = PathBuf::from(std::env::var_os("RBITCOIN_DIAG_DATADIR").unwrap());
    let store = if dir.join("store").is_dir() { dir.join("store") } else { dir };
    let q = Query::open_or_create(&store).unwrap();
    eprintln!("tip={:?} bodies={}", q.tip_height(), q.tx_body_count());
    let bytes = parse_hex("000000000000000000006e7a97adec731cb468b9dd4168d9f7cd1b19f4581e8a");
    let mut hash = [0u8; 32];
    for i in 0..32 { hash[i] = bytes[31 - i]; }
    let block = q.reconstruct_archived_block(&hash).unwrap().unwrap();
    let params = ChainParams::mainnet();
    let ms = Milestone { height: 1_000_000 };
    match confirm_wire_run(&q, &params, ms, &[(Height(961_461), block)]) {
        Ok(fks) => eprintln!("SUCCESS fks_len={} tip={:?}", fks.len(), q.tip_height()),
        Err(e) => { eprintln!("FAIL: {e}"); panic!("confirm failed: {e}"); }
    }
}
