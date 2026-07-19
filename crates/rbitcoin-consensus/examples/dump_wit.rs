use bitcoin::consensus::deserialize;
use bitcoin::hashes::{sha256, Hash};
use bitcoin::Block;

fn enc(d: &[u8]) -> String {
    d.iter().map(|b| format!("{b:02x}")).collect()
}

fn main() {
    let raw = std::fs::read("/tmp/signet277442/block.bin").unwrap();
    let block: Block = deserialize(&raw).unwrap();
    let target = "540b5d85f73d6eedef68893e70ce3bb52bdad0354a8204a8a43d2340387dc2ff";
    let tx = block
        .txdata
        .iter()
        .find(|t| t.compute_txid().to_string() == target)
        .unwrap();
    println!("n_in={}", tx.input.len());
    for ii in [0usize, 1, 50, 100, 200, 390] {
        if ii >= tx.input.len() {
            continue;
        }
        let inp = &tx.input[ii];
        let n = inp.witness.len();
        let script = inp.witness.nth(n - 1).unwrap();
        println!("in#{ii} wit_n={n} script={}", enc(script));
        let h = sha256::Hash::hash(script);
        println!("  sha256={}", enc(h.as_byte_array()));
        let mut i = 0;
        let b = script;
        print!("  ops:");
        while i < b.len() {
            let op = b[i];
            i += 1;
            if op <= 0x4b {
                let n = op as usize;
                if i + n > b.len() {
                    print!(" TRUNC");
                    break;
                }
                print!(" PUSH{}({})", n, enc(&b[i..i + n]));
                i += n;
            } else {
                print!(" OP_{op:02x}");
            }
        }
        println!();
        if n >= 2 {
            let mid = inp.witness.nth(1).unwrap();
            println!("  mid={}", enc(mid));
        }
        if n >= 1 {
            let sig = inp.witness.nth(0).unwrap();
            println!("  sig_tail={:02x?}", &sig[sig.len().saturating_sub(4)..]);
        }
    }
}
