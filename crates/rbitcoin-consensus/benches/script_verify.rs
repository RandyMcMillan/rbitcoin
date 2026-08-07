//! Microbench: real pure-Rust script verification (P2WPKH + P2TR key-path).
//!
//! Run: `cargo bench -p rbitcoin-consensus --bench script_verify`
//!
//! Measures end-to-end `verify_job_all_inputs` cost (sighash + ECDSA/Schnorr),
//! not synthetic SHA256 work. No UTXO set — jobs carry explicit prevouts.

use std::time::Instant;

use bitcoin::absolute::LockTime;
use bitcoin::hashes::{hash160, Hash};
use bitcoin::key::TapTweak;
use bitcoin::secp256k1::{Keypair, Message, Secp256k1, SecretKey};
use bitcoin::sighash::{EcdsaSighashType, Prevouts, SighashCache, TapSighashType};
use bitcoin::{Amount, OutPoint, ScriptBuf, Sequence, Transaction, TxIn, TxOut, Witness};

fn make_p2wpkh_job(seed: u8) -> rbitcoin_consensus::script_bench::JobBytes {
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
    rbitcoin_consensus::script_bench::JobBytes::new(vec![prevout], tx)
}

fn make_p2tr_job(seed: u8) -> rbitcoin_consensus::script_bench::JobBytes {
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
    rbitcoin_consensus::script_bench::JobBytes::new(vec![prevout], tx)
}

fn bench_kind(name: &str, jobs: &[rbitcoin_consensus::script_bench::JobBytes], iters: usize) {
    // Warmup
    for j in jobs.iter().take(4) {
        rbitcoin_consensus::script_bench::verify_job(j).unwrap();
    }
    let t0 = Instant::now();
    let mut n = 0usize;
    for _ in 0..iters {
        for j in jobs {
            rbitcoin_consensus::script_bench::verify_job(j).unwrap();
            n += 1;
        }
    }
    let dt = t0.elapsed();
    let per = dt / n as u32;
    println!(
        "{name}: {n} verifies in {dt:?} ({per:?}/verify, {:.1}/s)",
        n as f64 / dt.as_secs_f64()
    );
}

fn main() {
    let p2wpkh: Vec<_> = (1u8..=32).map(make_p2wpkh_job).collect();
    let p2tr: Vec<_> = (1u8..=32).map(make_p2tr_job).collect();
    bench_kind("p2wpkh", &p2wpkh, 50);
    bench_kind("p2tr_keypath", &p2tr, 50);
    // Parallel pool: flatten
    let t0 = Instant::now();
    let iters = 20;
    let mut n = 0usize;
    for _ in 0..iters {
        rbitcoin_consensus::script_bench::verify_jobs_pool(&p2wpkh).unwrap();
        n += p2wpkh.len();
    }
    let dt = t0.elapsed();
    println!(
        "p2wpkh_pool: {n} verifies in {dt:?} ({:.1}/s)",
        n as f64 / dt.as_secs_f64()
    );
}
