use std::time::Instant;
use bitcoin::absolute::LockTime;
use bitcoin::hashes::Hash;
use bitcoin::key::TapTweak;
use bitcoin::secp256k1::{Keypair, Message, Secp256k1, SecretKey};
use bitcoin::sighash::{Prevouts, SighashCache, TapSighashType};
use bitcoin::{Amount, OutPoint, ScriptBuf, Sequence, Transaction, TxIn, TxOut, Witness};
use rbitcoin_consensus::script_bench::{self, JobBytes};
use rayon::prelude::*;

fn p2tr(seed: u8) -> JobBytes {
    let secp = Secp256k1::new();
    let mut sk = [seed.max(1); 32];
    sk[0] = sk[0].max(1);
    let sk = SecretKey::from_slice(&sk).unwrap();
    let kp = Keypair::from_secret_key(&secp, &sk);
    let tw = kp.tap_tweak(&secp, None);
    let ok = tw.to_keypair().x_only_public_key().0;
    let mut spk = vec![0x51, 0x20];
    spk.extend_from_slice(&ok.serialize());
    let prevout = TxOut { value: Amount::from_sat(50_000), script_pubkey: ScriptBuf::from_bytes(spk) };
    let mut tx = Transaction {
        version: bitcoin::transaction::Version::TWO,
        lock_time: LockTime::ZERO,
        input: vec![TxIn { previous_output: OutPoint::null(), script_sig: ScriptBuf::new(), sequence: Sequence::ENABLE_RBF_NO_LOCKTIME, witness: Witness::new() }],
        output: vec![TxOut { value: Amount::from_sat(49_000), script_pubkey: ScriptBuf::from_bytes(vec![0x51]) }],
    };
    let mut cache = SighashCache::new(&tx);
    let prevouts = Prevouts::All(std::slice::from_ref(&prevout));
    let sh = cache.taproot_key_spend_signature_hash(0, &prevouts, TapSighashType::Default).unwrap();
    let msg = Message::from_digest(sh.to_byte_array());
    let sig = secp.sign_schnorr_no_aux_rand(&msg, &tw.to_keypair());
    tx.input[0].witness = Witness::from_slice(&[sig.as_ref()]);
    JobBytes::new(vec![prevout], tx)
}

fn med(mut v: Vec<f64>) -> f64 {
    v.sort_by(|a,b| a.partial_cmp(b).unwrap());
    v[v.len()/2]
}

fn measure(iters: u32, mut f: impl FnMut()) -> f64 {
    for _ in 0..10 { f(); }
    let mut samples = Vec::new();
    for _ in 0..21 {
        let t0 = Instant::now();
        for _ in 0..iters { f(); }
        samples.push(t0.elapsed().as_secs_f64() / iters as f64);
    }
    med(samples)
}

fn main() {
    let t = rayon::current_num_threads();
    println!("threads={t}");
    for n in [2usize, 3, 4, 8, 16, 32, 64, 128] {
        let jobs = script_bench::owned_jobs(&(0..n as u8).map(|i| p2tr(i.wrapping_add(5))).collect::<Vec<_>>());
        let iters = match n {
            0..=4 => 500u32,
            5..=16 => 200,
            17..=64 => 80,
            _ => 40,
        };
        let seq = measure(iters, || { for j in &jobs { script_bench::verify_one_job(j).unwrap(); } });
        let fine = measure(iters, || { jobs.par_iter().try_for_each(|j| script_bench::verify_one_job(j)).unwrap(); });
        let min4 = measure(iters, || { jobs.par_iter().with_min_len((n/(t*4)).max(1).min(8)).try_for_each(|j| script_bench::verify_one_job(j)).unwrap(); });
        let chunks = measure(iters, || {
            let nc = (t*2).min(n).max(1);
            let c = n.div_ceil(nc).clamp(1, 64);
            jobs.par_chunks(c).try_for_each(|s| { for j in s { script_bench::verify_one_job(j)?; } Ok::<(), rbitcoin_consensus::ConsensusError>(()) }).unwrap();
        });
        let prod = measure(iters, || { script_bench::verify_owned_pool(&jobs).unwrap(); });
        let thr = measure(iters, || {
            if n < 4 {
                for j in &jobs { script_bench::verify_one_job(j).unwrap(); }
            } else {
                let ml = (n / (t * 4)).max(1).min(8);
                jobs.par_iter().with_min_len(ml).try_for_each(|j| script_bench::verify_one_job(j)).unwrap();
            }
        });
        println!("n={n:3}  seq={:7.1}µs  fine={:7.1}µs ({:.2}×)  min_len={:7.1}µs ({:.2}×)  chunks={:7.1}µs ({:.2}×)  prod={:7.1}µs ({:.2}×)  thr+min={:7.1}µs ({:.2}×)",
            seq*1e6, fine*1e6, seq/fine, min4*1e6, seq/min4, chunks*1e6, seq/chunks, prod*1e6, seq/prod, thr*1e6, seq/thr);
    }
}
