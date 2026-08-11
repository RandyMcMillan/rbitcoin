use bitcoin::consensus::deserialize;
use bitcoin::{Block, Transaction};
use rbitcoin_consensus::script_bench::{self, JobBytes};
use std::process::Command;

fn hex_decode(s: &str) -> Option<Vec<u8>> {
    let s = s.trim();
    if !s.len().is_multiple_of(2) || s.is_empty() {
        return None;
    }
    let mut out = Vec::with_capacity(s.len() / 2);
    for i in (0..s.len()).step_by(2) {
        out.push(u8::from_str_radix(&s[i..i + 2], 16).ok()?);
    }
    Some(out)
}
fn fetch_tx(txid: &str) -> Option<Transaction> {
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
            .ok()?;
        let hex = String::from_utf8(out.stdout).ok()?;
        let raw = hex_decode(&hex)?;
        let _ = std::fs::write(&cache, &raw);
        raw
    };
    deserialize(&raw).ok()
}

fn main() {
    let path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "/tmp/b290329/block.bin".into());
    let block: Block = deserialize(&std::fs::read(&path).unwrap()).unwrap();
    println!("hash={} ntx={}", block.block_hash(), block.txdata.len());
    let mut n_fail = 0usize;
    let mut n_ok = 0usize;
    let mut n_skip = 0usize;
    for (ti, tx) in block.txdata.iter().enumerate().skip(1) {
        let mut prevouts = Vec::with_capacity(tx.input.len());
        let mut missing = false;
        for inp in &tx.input {
            match fetch_tx(&inp.previous_output.txid.to_string()) {
                Some(prev) => {
                    if let Some(o) = prev.output.get(inp.previous_output.vout as usize) {
                        prevouts.push(o.clone());
                    } else {
                        missing = true;
                        break;
                    }
                }
                None => {
                    missing = true;
                    break;
                }
            }
        }
        if missing {
            n_skip += 1;
            continue;
        }
        let mut job = JobBytes::new(prevouts, tx.clone());
        job.bip66_active = false;
        job.bip16_active = true;
        match script_bench::verify_job(&job) {
            Ok(()) => n_ok += 1,
            Err(e) => {
                n_fail += 1;
                println!("FAIL tx#{ti} {}: {e}", tx.compute_txid());
                if n_fail >= 5 {
                    break;
                }
            }
        }
    }
    println!("ok={n_ok} fail={n_fail} skip={n_skip}");
}
