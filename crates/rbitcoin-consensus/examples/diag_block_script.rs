//! Diagnose which script fails in a wire block (prevouts from mempool.space signet API).
use bitcoin::consensus::{deserialize, Decodable};
use bitcoin::{Block, Transaction, TxOut};
use rbitcoin_consensus::script_bench::{self, JobBytes};
use std::io::Cursor;
use std::process::Command;

fn hex_decode(s: &str) -> Vec<u8> {
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
        .collect()
}

fn fetch_hex(url: &str) -> Option<String> {
    let out = Command::new("curl")
        .args(["-sS", "-m", "30", url])
        .output()
        .ok()?;
    if !out.status.success() {
        return None;
    }
    let s = String::from_utf8(out.stdout).ok()?;
    let t = s.trim().to_string();
    if t.is_empty() || t.starts_with('<') || t.starts_with('{') {
        return None;
    }
    Some(t)
}

fn prevout_from_api(txid: &str, vout: u32) -> Option<TxOut> {
    let url = format!("https://mempool.space/signet/api/tx/{txid}/hex");
    let hex = fetch_hex(&url)?;
    let raw = hex_decode(hex.trim());
    let tx = Transaction::consensus_decode(&mut Cursor::new(raw)).ok()?;
    tx.output.get(vout as usize).cloned()
}

fn main() {
    let path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "/tmp/signet277442/block.bin".into());
    let raw = std::fs::read(&path).expect("block");
    let block: Block = deserialize(&raw).expect("decode block");
    println!("hash={} ntx={}", block.block_hash(), block.txdata.len());

    let mut n_fail = 0usize;
    for (ti, tx) in block.txdata.iter().enumerate().skip(1) {
        let mut prevouts = Vec::with_capacity(tx.input.len());
        let mut missing = false;
        for (ii, inp) in tx.input.iter().enumerate() {
            let tid = inp.previous_output.txid.to_string();
            let v = inp.previous_output.vout;
            match prevout_from_api(&tid, v) {
                Some(o) => prevouts.push(o),
                None => {
                    eprintln!("tx#{ti} in#{ii} missing prev {tid}:{v}");
                    missing = true;
                    break;
                }
            }
        }
        if missing {
            continue;
        }
        let spk_preview: Vec<_> = prevouts
            .iter()
            .enumerate()
            .map(|(ii, p)| {
                let spk = p.script_pubkey.as_bytes();
                (
                    ii,
                    p.value.to_sat(),
                    spk.len(),
                    spk[..spk.len().min(8)].to_vec(),
                )
            })
            .collect();
        let job = JobBytes::new(prevouts, tx.clone());
        if let Err(e) = script_bench::verify_job(&job) {
            n_fail += 1;
            let txid = tx.compute_txid();
            eprintln!("FAIL tx#{ti} {txid}: {e}");
            for (ii, val, len, head) in spk_preview {
                eprintln!("  in#{ii} value={val} spk_len={len} spk0={head:02x?}");
            }
            // dump witness stack sizes
            for (ii, inp) in tx.input.iter().enumerate() {
                let items: Vec<usize> = (0..inp.witness.len())
                    .filter_map(|i| inp.witness.nth(i).map(|b| b.len()))
                    .collect();
                eprintln!(
                    "  in#{ii} witness_item_lens={items:?} script_sig_len={}",
                    inp.script_sig.len()
                );
            }
        }
    }
    println!("done fails={n_fail}");
}
