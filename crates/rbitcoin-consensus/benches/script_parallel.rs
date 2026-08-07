//! Measure real script-verify parallelization efficiency vs task granularity.
//!
//!   cargo bench -p rbitcoin-consensus --bench script_parallel
//!
//! Hypothesis: per-tx rayon tasks (~30–40 µs ECDSA/Schnorr) are too fine-grained
//! on few-core boxes — steal/scheduling overhead eats the speedup. Compare:
//!   - sequential
//!   - rayon par_iter (today's path, one job per steal unit)
//!   - rayon with_min_len / par_chunks (fat tasks)
//!   - threshold: stay sequential below N jobs

use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

use bitcoin::absolute::LockTime;
use bitcoin::hashes::{hash160, Hash};
use bitcoin::key::TapTweak;
use bitcoin::secp256k1::{Keypair, Message, Secp256k1, SecretKey};
use bitcoin::sighash::{EcdsaSighashType, Prevouts, SighashCache, TapSighashType};
use bitcoin::{Amount, OutPoint, ScriptBuf, Sequence, Transaction, TxIn, TxOut, Witness};
use rayon::prelude::*;
use rbitcoin_consensus::script_bench::{self, JobBytes};

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

fn bench(name: &str, iters: u32, mut f: impl FnMut()) {
    for _ in 0..3 {
        f();
    }
    let t0 = Instant::now();
    for _ in 0..iters {
        f();
    }
    let dt = t0.elapsed();
    let per = dt / iters.max(1);
    println!("{name:58}  {per:>10.2?}/op  ({iters} iters, {dt:.2?} total)");
}

use rbitcoin_consensus::ScriptCheckJob;

/// Today's production shape: rayon par_iter, one job per unit, no min_len.
fn par_today(jobs: &[ScriptCheckJob]) {
    jobs.par_iter()
        .try_for_each(|j| script_bench::verify_one_job(j))
        .unwrap();
}

/// Rayon with min_len — fewer, fatter tasks.
fn par_min_len(jobs: &[ScriptCheckJob], min_len: usize) {
    jobs.par_iter()
        .with_min_len(min_len)
        .try_for_each(|j| script_bench::verify_one_job(j))
        .unwrap();
}

/// Explicit chunks: each task runs a contiguous slice sequentially.
fn par_chunks(jobs: &[ScriptCheckJob], chunk: usize) {
    jobs.par_chunks(chunk.max(1))
        .try_for_each(|slice| {
            for j in slice {
                script_bench::verify_one_job(j)?;
            }
            Ok::<(), rbitcoin_consensus::ConsensusError>(())
        })
        .unwrap();
}

/// Manual work-stealing with atomic counter, batch_size jobs per grab.
fn par_steal_batches(jobs: &[ScriptCheckJob], batch: usize) {
    let n = jobs.len();
    if n == 0 {
        return;
    }
    let workers = rayon::current_num_threads().max(1);
    let next = AtomicUsize::new(0);
    let batch = batch.max(1);
    // Use rayon join tree only to populate workers once.
    rayon::scope(|s| {
        for _ in 0..workers {
            s.spawn(|_| loop {
                let start = next.fetch_add(batch, Ordering::Relaxed);
                if start >= n {
                    break;
                }
                let end = (start + batch).min(n);
                for j in &jobs[start..end] {
                    script_bench::verify_one_job(j).unwrap();
                }
            });
        }
    });
}

fn seq(jobs: &[ScriptCheckJob]) {
    for j in jobs {
        script_bench::verify_one_job(j).unwrap();
    }
}

fn adaptive_chunk(n: usize, workers: usize) -> usize {
    // Aim for ~2–4 chunks per worker so steals can rebalance without
    // one-job tasks. Floor at 1, cap so we still use all cores.
    let w = workers.max(1);
    let target_chunks = w * 2;
    ((n + target_chunks - 1) / target_chunks).max(1)
}

fn main() {
    let workers = rayon::current_num_threads();
    println!(
        "rayon threads={workers}  available_parallelism={:?}\n",
        std::thread::available_parallelism()
    );

    // Build owned job sets (production shape after connect).
    let p2tr_64: Vec<_> = (0..64u8).map(|i| p2tr_job(i.wrapping_add(1))).collect();
    let p2tr_64 = script_bench::owned_jobs(&p2tr_64);
    let p2wpkh_64: Vec<_> = (0..64u8).map(|i| p2wpkh_job(i.wrapping_add(1))).collect();
    let p2wpkh_64 = script_bench::owned_jobs(&p2wpkh_64);
    let p2tr_16: Vec<_> = (0..16u8).map(|i| p2tr_job(i.wrapping_add(30))).collect();
    let p2tr_16 = script_bench::owned_jobs(&p2tr_16);
    let p2tr_8: Vec<_> = (0..8u8).map(|i| p2tr_job(i.wrapping_add(50))).collect();
    let p2tr_8 = script_bench::owned_jobs(&p2tr_8);
    let p2tr_4: Vec<_> = (0..4u8).map(|i| p2tr_job(i.wrapping_add(60))).collect();
    let p2tr_4 = script_bench::owned_jobs(&p2tr_4);
    let p2tr_2: Vec<_> = (0..2u8).map(|i| p2tr_job(i.wrapping_add(70))).collect();
    let p2tr_2 = script_bench::owned_jobs(&p2tr_2);
    // Fat signet-ish multi-block wave: 16 blocks × ~8 1-in P2TR ≈ 128
    let p2tr_128: Vec<_> = (0..128u8).map(|i| p2tr_job(i.wrapping_add(3))).collect();
    let p2tr_128 = script_bench::owned_jobs(&p2tr_128);

    println!("=== 64 × 1-input P2TR (one fat block / small multi-run) ===");
    let iters = 40u32;
    bench("sequential", iters, || seq(&p2tr_64));
    bench("rayon par_iter (no min_len)", iters, || par_today(&p2tr_64));
    bench("production verify_owned_pool", iters, || {
        script_bench::verify_owned_pool(&p2tr_64).unwrap();
    });
    for min in [1, 2, 4, 8, 16] {
        bench(&format!("rayon with_min_len({min})"), iters, || {
            par_min_len(&p2tr_64, min)
        });
    }
    for c in [1, 2, 4, 8, 16, 32] {
        bench(&format!("par_chunks({c})"), iters, || {
            par_chunks(&p2tr_64, c)
        });
    }
    let ac = adaptive_chunk(64, workers);
    bench(&format!("par_chunks(adaptive={ac})"), iters, || {
        par_chunks(&p2tr_64, ac)
    });
    for b in [1, 4, 8, 16] {
        bench(&format!("steal batch={b}"), iters, || {
            par_steal_batches(&p2tr_64, b)
        });
    }

    println!("\n=== 64 × 1-input P2WPKH ===");
    bench("sequential", iters, || seq(&p2wpkh_64));
    bench("rayon par_iter (no min_len)", iters, || {
        par_today(&p2wpkh_64)
    });
    bench("production verify_owned_pool", iters, || {
        script_bench::verify_owned_pool(&p2wpkh_64).unwrap();
    });
    bench("par_chunks(8)", iters, || par_chunks(&p2wpkh_64, 8));

    println!("\n=== 128 × P2TR (16-block confirm wave) ===");
    let iters_b = 20u32;
    bench("sequential", iters_b, || seq(&p2tr_128));
    bench("rayon par_iter (no min_len)", iters_b, || {
        par_today(&p2tr_128)
    });
    bench("production verify_owned_pool", iters_b, || {
        script_bench::verify_owned_pool(&p2tr_128).unwrap();
    });
    bench("par_chunks(8)", iters_b, || par_chunks(&p2tr_128, 8));
    bench("par_chunks(16)", iters_b, || par_chunks(&p2tr_128, 16));

    println!("\n=== Small sets (overhead dominates?) ===");
    let iters_s = 200u32;
    for (label, jobs) in [
        ("2 jobs", &p2tr_2[..]),
        ("4 jobs", &p2tr_4[..]),
        ("8 jobs", &p2tr_8[..]),
        ("16 jobs", &p2tr_16[..]),
    ] {
        println!("--- {label} ---");
        bench("  sequential", iters_s, || seq(jobs));
        bench("  rayon no min_len", iters_s, || par_today(jobs));
        bench("  production pool", iters_s, || {
            script_bench::verify_owned_pool(jobs).unwrap();
        });
    }

    // Efficiency summary on 64 P2TR
    println!("\n=== Efficiency (64 P2TR, timed pass) ===");
    let t0 = Instant::now();
    for _ in 0..30 {
        seq(&p2tr_64);
    }
    let seq_t = t0.elapsed() / 30;
    let t0 = Instant::now();
    for _ in 0..30 {
        par_today(&p2tr_64);
    }
    let fine_t = t0.elapsed() / 30;
    let t0 = Instant::now();
    for _ in 0..30 {
        script_bench::verify_owned_pool(&p2tr_64).unwrap();
    }
    let prod_t = t0.elapsed() / 30;
    let t0 = Instant::now();
    for _ in 0..30 {
        par_min_len(&p2tr_64, 4);
    }
    let min4_t = t0.elapsed() / 30;
    let ideal = seq_t.as_secs_f64() / workers as f64;
    println!(
        "seq={seq_t:?}\n  fine par_iter={fine_t:?} ({:.1}×, {:.0}% eff)\n  production min_len={prod_t:?} ({:.1}×, {:.0}% eff)\n  with_min_len(4)={min4_t:?} ({:.1}×, {:.0}% eff)\n  ideal {workers}-way={ideal:.2e}s",
        seq_t.as_secs_f64() / fine_t.as_secs_f64(),
        100.0 * ideal / fine_t.as_secs_f64(),
        seq_t.as_secs_f64() / prod_t.as_secs_f64(),
        100.0 * ideal / prod_t.as_secs_f64(),
        seq_t.as_secs_f64() / min4_t.as_secs_f64(),
        100.0 * ideal / min4_t.as_secs_f64(),
    );
}
