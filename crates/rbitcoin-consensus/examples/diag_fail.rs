use bitcoin::consensus::deserialize;
use bitcoin::hashes::{hash160, Hash};
use bitcoin::script::Instruction;
use bitcoin::secp256k1::ecdsa::Signature;
use bitcoin::secp256k1::{Message, PublicKey, Secp256k1};
use bitcoin::sighash::SighashCache;
use bitcoin::{Block, Transaction};
use rbitcoin_consensus::script_bench::{self, JobBytes};
use std::fs;
use std::process::Command;

fn hx(s: &str) -> Vec<u8> {
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
        .collect()
}

fn fetch_tx_hex(txid: &str) -> String {
    let path = format!("/tmp/b140493/prev_{txid}.hex");
    if let Ok(s) = fs::read_to_string(&path) {
        return s.trim().to_string();
    }
    let out = Command::new("curl")
        .args([
            "-sL",
            &format!("https://blockstream.info/api/tx/{txid}/hex"),
        ])
        .output()
        .unwrap();
    let s = String::from_utf8(out.stdout).unwrap().trim().to_string();
    fs::write(&path, &s).unwrap();
    s
}

fn main() {
    let block: Block = deserialize(&fs::read("/tmp/b140493/block.bin").unwrap()).unwrap();
    let secp = Secp256k1::verification_only();
    for (i, spend) in block.txdata.iter().enumerate().skip(1) {
        let mut prevouts = Vec::new();
        for vin in &spend.input {
            let pt = vin.previous_output.txid.to_string();
            let prev: Transaction = deserialize(&hx(&fetch_tx_hex(&pt))).unwrap();
            prevouts.push(prev.output[vin.previous_output.vout as usize].clone());
        }
        let mut job = JobBytes::new(prevouts.clone(), spend.clone());
        job.bip66_active = false;
        if script_bench::verify_job(&job).is_ok() {
            continue;
        }
        println!(
            "FAIL tx{i} {} nIn={}",
            spend.compute_txid(),
            spend.input.len()
        );
        let cache = SighashCache::new(spend);
        for (ii, vin) in spend.input.iter().enumerate() {
            let mut items = Vec::new();
            for ins in vin.script_sig.instructions() {
                match ins.unwrap() {
                    Instruction::PushBytes(b) => items.push(b.as_bytes().to_vec()),
                    Instruction::Op(op) => println!("  op {op:?}"),
                }
            }
            println!(
                "in{ii} pushes={} ss_len={}",
                items.len(),
                vin.script_sig.len()
            );
            if items.len() != 2 {
                // dump all push lens
                for (k, it) in items.iter().enumerate() {
                    println!("  p{k}={}", it.len());
                }
                continue;
            }
            let sig_raw = &items[0];
            let pk_raw = &items[1];
            let ht = sig_raw[sig_raw.len() - 1] as u32;
            let der = &sig_raw[..sig_raw.len() - 1];
            let sig = Signature::from_der_lax(der).expect("lax");
            let pk = PublicKey::from_slice(pk_raw).expect("pk");
            let spk = prevouts[ii].script_pubkey.as_script();
            let h = cache.legacy_signature_hash(ii, spk, ht).unwrap();
            let mut sn = sig;
            sn.normalize_s();
            let msg = Message::from_digest(h.to_byte_array());
            let ok = secp.verify_ecdsa(&msg, &sn, &pk).is_ok();
            let kh = hash160::Hash::hash(pk_raw);
            let spkb = prevouts[ii].script_pubkey.as_bytes();
            println!(
                "  ht={ht:#x} verify={ok} keyhash_ok={} spk_len={}",
                spkb.len() == 25 && spkb[3..23] == *kh.as_byte_array(),
                spkb.len()
            );
            // Is spk actually p2pkh classified?
            println!("  spk={:02x?}", spkb);
        }
        // Is error from a different input classification?
        println!("err={:?}", script_bench::verify_job(&job));
        break;
    }
}
