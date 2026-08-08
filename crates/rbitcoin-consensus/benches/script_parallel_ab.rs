//! A/B: sequential vs in-crate script_pool (replaces rayon-era A/B).
//!
//!   cargo bench -p rbitcoin-consensus --bench script_parallel_ab

use std::time::Instant;

use bitcoin::absolute::LockTime;
use bitcoin::hashes::{hash160, Hash};
use bitcoin::secp256k1::{Message, Secp256k1, SecretKey};
use bitcoin::sighash::{EcdsaSighashType, SighashCache};
use bitcoin::{Amount, OutPoint, ScriptBuf, Sequence, Transaction, TxIn, TxOut, Witness};

use rbitcoin_consensus::script_bench::{self, JobBytes};

fn bench(name: &str, iters: u32, mut f: impl FnMut()) {
    for _ in 0..2 {
        f();
    }
    let t0 = Instant::now();
    for _ in 0..iters {
        f();
    }
    println!("{name:48}  {:>10.2?}/op", t0.elapsed() / iters.max(1));
}

fn p2wpkh_job(seed: u8) -> JobBytes {
    let secp = Secp256k1::new();
    let mut sk_bytes = [seed; 32];
    sk_bytes[0] = sk_bytes[0].max(1);
    let sk = SecretKey::from_slice(&sk_bytes).unwrap();
    let pk = bitcoin::PublicKey::new(sk.public_key(&secp));
    let pk_bytes = pk.to_bytes();
    let keyhash = hash160::Hash::hash(&pk_bytes);
    let mut spk = vec![0x00, 0x14];
    spk.extend_from_slice(keyhash.as_byte_array());
    let prevout = TxOut {
        value: Amount::from_sat(50_000),
        script_pubkey: ScriptBuf::from_bytes(spk.clone()),
    };
    let mut tx = Transaction {
        version: bitcoin::transaction::Version::TWO,
        lock_time: LockTime::ZERO,
        input: vec![TxIn {
            previous_output: OutPoint {
                txid: bitcoin::Txid::from_byte_array([seed; 32]),
                vout: 0,
            },
            script_sig: ScriptBuf::new(),
            sequence: Sequence::ENABLE_RBF_NO_LOCKTIME,
            witness: Witness::new(),
        }],
        output: vec![TxOut {
            value: Amount::from_sat(40_000),
            script_pubkey: ScriptBuf::from_bytes(vec![0x51]),
        }],
    };
    let mut cache = SighashCache::new(&tx);
    let sighash = cache
        .p2wpkh_signature_hash(
            0,
            ScriptBuf::from_bytes(spk).as_script(),
            prevout.value,
            EcdsaSighashType::All,
        )
        .unwrap();
    let msg = Message::from_digest(sighash.to_byte_array());
    let sig = secp.sign_ecdsa(&msg, &sk);
    let mut sig_raw = sig.serialize_der().to_vec();
    sig_raw.push(EcdsaSighashType::All as u8);
    tx.input[0].witness = Witness::from_slice(&[sig_raw.as_slice(), pk_bytes.as_slice()]);
    JobBytes::new(vec![prevout], tx)
}

fn main() {
    let jobs: Vec<_> = (0..64).map(|i| p2wpkh_job((i % 50 + 1) as u8)).collect();
    let owned = script_bench::owned_jobs(&jobs);
    println!("script_parallel_ab\n");
    bench("sequential", 15, || {
        for j in &jobs {
            let _ = script_bench::verify_job(j);
        }
    });
    bench("script_pool", 15, || {
        let _ = script_bench::verify_owned_pool(&owned);
    });
}
