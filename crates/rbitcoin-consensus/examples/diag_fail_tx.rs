use bitcoin::consensus::deserialize;
use bitcoin::script::Instruction;
use bitcoin::{Block, Transaction};
use rbitcoin_consensus::script_bench::{self, JobBytes};
use std::process::Command;

fn hex_decode(s: &str) -> Vec<u8> {
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
        .collect()
}
fn fetch_tx(txid: &str) -> Transaction {
    let cache = format!("/tmp/b290329/tx_{txid}.bin");
    let raw = if let Ok(b) = std::fs::read(&cache) {
        b
    } else {
        let out = Command::new("curl")
            .args([
                "-sL",
                "-m",
                "60",
                &format!("https://blockstream.info/api/tx/{txid}/hex"),
            ])
            .output()
            .unwrap();
        let hex = String::from_utf8(out.stdout).unwrap();
        let raw = hex_decode(hex.trim());
        let _ = std::fs::write(&cache, &raw);
        raw
    };
    deserialize(&raw).unwrap()
}

fn main() {
    let block: Block = deserialize(&std::fs::read("/tmp/b290329/block.bin").unwrap()).unwrap();
    let want = "5df1375ffe61ac35ca178ebb0cab9ea26dedbd0e96005dfcee7e379fa513232f";
    let (ti, tx) = block
        .txdata
        .iter()
        .enumerate()
        .find(|(_, t)| t.compute_txid().to_string() == want)
        .unwrap();
    println!("tx#{} {} nIn={}", ti, tx.compute_txid(), tx.input.len());
    let mut prevouts = Vec::new();
    for (ii, inp) in tx.input.iter().enumerate() {
        let prev = fetch_tx(&inp.previous_output.txid.to_string());
        let o = prev.output[inp.previous_output.vout as usize].clone();
        prevouts.push(o.clone());
        let ss = inp.script_sig.as_bytes();
        let spk = o.script_pubkey.as_bytes();
        println!(
            "in#{} prev={}:{} val={}",
            ii,
            inp.previous_output.txid,
            inp.previous_output.vout,
            o.value.to_sat()
        );
        println!("  spk_len={} spk={:02x?}", spk.len(), spk);
        println!("  ss_len={} ss={:02x?}", ss.len(), ss);
        print!("  ss_ops:");
        for ins in inp.script_sig.instructions() {
            match ins {
                Ok(Instruction::PushBytes(b)) => print!(" PUSH({})", b.len()),
                Ok(Instruction::Op(op)) => print!(" {:?}", op),
                Err(e) => print!(" ERR{:?}", e),
            }
        }
        println!();
        println!("  witness_items={}", inp.witness.len());
    }
    for bip16 in [true, false] {
        for bip66 in [true, false] {
            let mut job = JobBytes::new(prevouts.clone(), tx.clone());
            job.bip16_active = bip16;
            job.bip66_active = bip66;
            match script_bench::verify_job(&job) {
                Ok(()) => println!("OK bip16={bip16} bip66={bip66}"),
                Err(e) => println!("FAIL bip16={bip16} bip66={bip66}: {e}"),
            }
        }
    }
}
