#![no_main]

use libfuzzer_sys::fuzz_target;
use rbitcoin_consensus::check_block_wire;

fuzz_target!(|data: &[u8]| {
    let _ = check_block_wire(data);
});
