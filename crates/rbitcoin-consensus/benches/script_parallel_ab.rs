//! Focused A/B: sequential vs fine par_iter vs production pool.
//! High iteration count, little thermal warmup variance.
//!
//!   cargo bench -p rbitcoin-consensus --bench script_parallel_ab

use std::time::Instant;

use bitcoin::absolute::LockTime;
use bitcoin::hashes::Hash;
use bitcoin::key::TapTweak;
use bitcoin::secp256k1::{Keypair, Message, Secp256k1, SecretKey};
use bitcoin::sighash::{Prevouts, SighashCache, TapSighashType};
use bitcoin::{
    Amount, OutPoint, ScriptBuf, Sequence, Transaction, TxIn, TxOut, Witness,
};
use rbitcoin_consensus::script_bench::{self, JobBytes};
use rayon::prelude::*;

fn p2tr_job(seed: u8) -> JobBytes {
    let secp = Secp256k1::new();
    let mut sk_bytes = [seed; 32];
    sk_bytes[0] = sk_bytes[0].max(1);
    let sk = SecretKey::from_slice(&sk_bytes).unwrap();
    let kp = Keypair::from_secret_key(&secp, &sk);
    let tweaked = kp.tap_tweak(&secp, None);
    let output_key = tweaked.to_keypair().x_only_public_key().0;
    let mut spk = vec![0x51, 0x20];
    spk.extend_from_slice(&output_key.serialize());
    let prevout = TxOut {
        value: Amount::from_sat(50_000),
        script_pubkey: ScriptBuf::from_bytes(spk),
    };
    let mut tx = Transaction {
        version: bitcoin::transaction::Version::TWO,
        lock_time: LockTime::ZERO,
        input: vec![TxIn {
            previous_output: OutPoint::null(),
            script_sig: ScriptBuf::new(),
            sequence: Sequence::ENABLE_RBF_NO_LOCKTIME,
            witness: Witness::new(),
        }],
        output: vec![TxOut {
            value: Amount::from_sat(49_000),
            script_pubkey: ScriptBuf::from_bytes(vec![0x51]),
        }],
    };
    let mut cache = SighashCache::new(&tx);
    let prevouts = Prevouts::All(std::slice::from_ref(&prevout));
    let sighash = cache
        .taproot_key_spend_signature_hash(0, &prevouts, TapSighashType::Default)
        .unwrap();
    let msg = Message::from_digest(sighash.to_byte_array());
    let sig = secp.sign_schnorr_no_aux_rand(&msg, &tweaked.to_keypair());
    tx.input[0].witness = Witness::from_slice(&[sig.as_ref()]);
    JobBytes::new(vec![prevout], tx)
}

fn timed(name: &str, iters: u32, mut f: impl FnMut()) {
    for _ in 0..5 {
        f();
    }
    let t0 = Instant::now();
    for _ in 0..iters {
        f();
    }
    let dt = t0.elapsed();
    let per = dt / iters;
    println!("{name:40} {per:>10.2?}/op  ({iters}×, total {dt:.2?})");
}

fn main() {
    let workers = rayon::current_num_threads();
    println!("rayon threads={workers}\n");

    for (label, n, iters) in [
        ("2 jobs", 2u8, 400u32),
        ("4 jobs", 4, 400),
        ("8 jobs", 8, 300),
        ("16 jobs", 16, 200),
        ("64 jobs", 64, 80),
        ("128 jobs", 128, 40),
    ] {
        let owned = script_bench::owned_jobs(
            &(0..n)
                .map(|i| p2tr_job(i.wrapping_add(11)))
                .collect::<Vec<_>>(),
        );
        println!("=== {label} ===");
        timed("sequential", iters, || {
            for j in &owned {
                script_bench::verify_one_job(j).unwrap();
            }
        });
        timed("fine par_iter (1 job/task)", iters, || {
            owned
                .par_iter()
                .try_for_each(|j| script_bench::verify_one_job(j))
                .unwrap();
        });
        timed("production pool (chunked)", iters, || {
            script_bench::verify_owned_pool(&owned).unwrap();
        });
        // Ideal efficiency
        let t_seq = {
            let t0 = Instant::now();
            for _ in 0..iters {
                for j in &owned {
                    script_bench::verify_one_job(j).unwrap();
                }
            }
            t0.elapsed() / iters
        };
        let t_prod = {
            let t0 = Instant::now();
            for _ in 0..iters {
                script_bench::verify_owned_pool(&owned).unwrap();
            }
            t0.elapsed() / iters
        };
        let t_fine = {
            let t0 = Instant::now();
            for _ in 0..iters {
                owned
                    .par_iter()
                    .try_for_each(|j| script_bench::verify_one_job(j))
                    .unwrap();
            }
            t0.elapsed() / iters
        };
        let ideal = t_seq.as_secs_f64() / workers as f64;
        println!(
            "  speedup fine={:.2}× ({:.0}% eff)  prod={:.2}× ({:.0}% eff)  prod/fine={:.2}\n",
            t_seq.as_secs_f64() / t_fine.as_secs_f64(),
            100.0 * ideal / t_fine.as_secs_f64(),
            t_seq.as_secs_f64() / t_prod.as_secs_f64(),
            100.0 * ideal / t_prod.as_secs_f64(),
            t_fine.as_secs_f64() / t_prod.as_secs_f64(),
        );
    }
}
