//! Break down script-verify cost: clone vs sighash vs crypto vs full path.
//!
//!   cargo bench -p rbitcoin-consensus --bench script_hotpath --release
//!
//! Also multi-input txs to show shared-cache benefit, and batch of single-input
//! jobs (signet-like: many 1-in P2TR).

use std::time::Instant;

use bitcoin::absolute::LockTime;
use bitcoin::consensus::Encodable;
use bitcoin::hashes::{hash160, Hash};
use bitcoin::key::TapTweak;
use bitcoin::secp256k1::{Keypair, Message, Secp256k1, SecretKey};
use bitcoin::sighash::{EcdsaSighashType, Prevouts, SighashCache, TapSighashType};
use bitcoin::{Amount, OutPoint, ScriptBuf, Sequence, Transaction, TxIn, TxOut, Witness};

fn encode_tx(tx: &Transaction) -> Vec<u8> {
    let mut v = Vec::new();
    tx.consensus_encode(&mut v).unwrap();
    v
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
    println!("{name:56}  {per:>10.2?}/op  ({iters} iters, {dt:.2?} total)");
}

fn p2wpkh_tx(seed: u8, n_in: usize) -> (Transaction, Vec<TxOut>) {
    let secp = Secp256k1::new();
    let mut sk_bytes = [seed; 32];
    sk_bytes[0] = sk_bytes[0].max(1);
    let sk = SecretKey::from_slice(&sk_bytes).unwrap();
    let pk = bitcoin::PublicKey::new(sk.public_key(&secp));
    let pk_bytes = pk.to_bytes();
    let keyhash = hash160::Hash::hash(&pk_bytes);
    let mut spk = vec![0x00, 0x14];
    spk.extend_from_slice(keyhash.as_byte_array());
    let prevouts: Vec<TxOut> = (0..n_in)
        .map(|_| TxOut {
            value: Amount::from_sat(50_000),
            script_pubkey: ScriptBuf::from_bytes(spk.clone()),
        })
        .collect();
    let mut tx = Transaction {
        version: bitcoin::transaction::Version::TWO,
        lock_time: LockTime::ZERO,
        input: (0..n_in)
            .map(|i| TxIn {
                previous_output: OutPoint {
                    txid: bitcoin::Txid::from_byte_array([i as u8; 32]),
                    vout: 0,
                },
                script_sig: ScriptBuf::new(),
                sequence: Sequence::ENABLE_RBF_NO_LOCKTIME,
                witness: Witness::new(),
            })
            .collect(),
        output: vec![TxOut {
            value: Amount::from_sat(40_000),
            script_pubkey: ScriptBuf::from_bytes(vec![0x51]),
        }],
    };
    for i in 0..n_in {
        let mut cache = SighashCache::new(&tx);
        let sighash = cache
            .p2wpkh_signature_hash(
                i,
                ScriptBuf::from_bytes(spk.clone()).as_script(),
                prevouts[i].value,
                EcdsaSighashType::All,
            )
            .unwrap();
        let msg = Message::from_digest(sighash.to_byte_array());
        let sig = secp.sign_ecdsa(&msg, &sk);
        let mut sig_raw = sig.serialize_der().to_vec();
        sig_raw.push(EcdsaSighashType::All as u8);
        tx.input[i].witness = Witness::from_slice(&[sig_raw.as_slice(), pk_bytes.as_slice()]);
    }
    (tx, prevouts)
}

fn p2tr_keypath_tx(seed: u8, n_in: usize) -> (Transaction, Vec<TxOut>) {
    let secp = Secp256k1::new();
    let mut sk_bytes = [seed; 32];
    sk_bytes[0] = sk_bytes[0].max(1);
    let sk = SecretKey::from_slice(&sk_bytes).unwrap();
    let kp = Keypair::from_secret_key(&secp, &sk);
    let tweaked = kp.tap_tweak(&secp, None);
    let output_key = tweaked.to_keypair().x_only_public_key().0;
    let mut spk = vec![0x51, 0x20];
    spk.extend_from_slice(&output_key.serialize());
    let prevouts: Vec<TxOut> = (0..n_in)
        .map(|_| TxOut {
            value: Amount::from_sat(50_000),
            script_pubkey: ScriptBuf::from_bytes(spk.clone()),
        })
        .collect();
    let mut tx = Transaction {
        version: bitcoin::transaction::Version::TWO,
        lock_time: LockTime::ZERO,
        input: (0..n_in)
            .map(|i| TxIn {
                previous_output: OutPoint {
                    txid: bitcoin::Txid::from_byte_array([i as u8; 32]),
                    vout: 0,
                },
                script_sig: ScriptBuf::new(),
                sequence: Sequence::ENABLE_RBF_NO_LOCKTIME,
                witness: Witness::new(),
            })
            .collect(),
        output: vec![TxOut {
            value: Amount::from_sat(40_000),
            script_pubkey: ScriptBuf::from_bytes(vec![0x51]),
        }],
    };
    for i in 0..n_in {
        let mut cache = SighashCache::new(&tx);
        let prev = Prevouts::All(&prevouts);
        let sighash = cache
            .taproot_key_spend_signature_hash(i, &prev, TapSighashType::Default)
            .unwrap();
        let msg = Message::from_digest(sighash.to_byte_array());
        let sig = secp.sign_schnorr_no_aux_rand(&msg, &tweaked.to_keypair());
        tx.input[i].witness = Witness::from_slice(&[sig.as_ref()]);
    }
    (tx, prevouts)
}

fn to_job(tx: &Transaction, prevouts: Vec<TxOut>) -> rbitcoin_consensus::script_bench::JobBytes {
    rbitcoin_consensus::script_bench::JobBytes::new(prevouts, tx.clone())
}

fn main() {
    println!("=== script hotpath breakdown ===\n");
    let (tx1, po1) = p2wpkh_tx(7, 1);
    let job1 = to_job(&tx1, po1.clone());
    let bytes = encode_tx(&tx1);

    // --- single P2WPKH input ---
    println!("--- 1-input P2WPKH ---");
    bench("clone Transaction (connect-side job build)", 50_000, || {
        let _ = tx1.clone();
    });
    bench(
        "encode Transaction → wire bytes (old connect)",
        50_000,
        || {
            let _ = encode_tx(&tx1);
        },
    );
    bench(
        "deserialize wire → Transaction (old verify)",
        50_000,
        || {
            let _: Transaction = bitcoin::consensus::deserialize(&bytes).unwrap();
        },
    );
    bench(
        "encode + deserialize round-trip (old waste)",
        20_000,
        || {
            let b = encode_tx(&tx1);
            let _: Transaction = bitcoin::consensus::deserialize(&b).unwrap();
        },
    );
    bench("SighashCache + p2wpkh_signature_hash only", 20_000, || {
        let mut cache = SighashCache::new(&tx1);
        let _ = cache
            .p2wpkh_signature_hash(
                0,
                po1[0].script_pubkey.as_script(),
                po1[0].value,
                EcdsaSighashType::All,
            )
            .unwrap();
    });
    bench("full verify_job (clone-free path)", 10_000, || {
        rbitcoin_consensus::script_bench::verify_job(&job1).unwrap();
    });

    // ECDSA only: verify same precomputed hash repeatedly
    {
        let secp = Secp256k1::new();
        let sk = SecretKey::from_slice(&[7u8; 32]).unwrap();
        let pk = bitcoin::PublicKey::new(sk.public_key(&secp));
        let msg = Message::from_digest([3u8; 32]);
        let sig = secp.sign_ecdsa(&msg, &sk);
        bench("secp256k1 ECDSA verify only", 50_000, || {
            secp.verify_ecdsa(&msg, &sig, &pk.inner).unwrap();
        });
    }

    // --- single P2TR keypath ---
    println!("\n--- 1-input P2TR key-path ---");
    let (tx_tr, po_tr) = p2tr_keypath_tx(9, 1);
    let job_tr = to_job(&tx_tr, po_tr.clone());
    bench("taproot_key_spend_signature_hash only", 20_000, || {
        let mut cache = SighashCache::new(&tx_tr);
        let prev = Prevouts::All(&po_tr);
        let _ = cache
            .taproot_key_spend_signature_hash(0, &prev, TapSighashType::Default)
            .unwrap();
    });
    bench("full verify_job P2TR keypath", 10_000, || {
        rbitcoin_consensus::script_bench::verify_job(&job_tr).unwrap();
    });
    {
        let secp = Secp256k1::new();
        let sk = SecretKey::from_slice(&[9u8; 32]).unwrap();
        let kp = Keypair::from_secret_key(&secp, &sk);
        let msg = Message::from_digest([5u8; 32]);
        let sig = secp.sign_schnorr_no_aux_rand(&msg, &kp);
        let (xonly, _) = kp.x_only_public_key();
        bench("secp256k1 Schnorr verify only", 50_000, || {
            secp.verify_schnorr(&sig, &msg, &xonly).unwrap();
        });
    }

    // --- multi-input: cache rebuild waste ---
    println!("\n--- 8-input P2WPKH (cache rebuild waste) ---");
    let (tx8, po8) = p2wpkh_tx(11, 8);
    let job8 = to_job(&tx8, po8.clone());
    bench("full verify_job 8-in P2WPKH (shared cache)", 2_000, || {
        rbitcoin_consensus::script_bench::verify_job(&job8).unwrap();
    });
    bench("8× new SighashCache + hash (no ecdsa)", 2_000, || {
        for i in 0..8 {
            let mut cache = SighashCache::new(&tx8);
            let _ = cache
                .p2wpkh_signature_hash(
                    i,
                    po8[i].script_pubkey.as_script(),
                    po8[i].value,
                    EcdsaSighashType::All,
                )
                .unwrap();
        }
    });
    bench(
        "1× SighashCache shared + 8 hashes (no ecdsa)",
        2_000,
        || {
            let mut cache = SighashCache::new(&tx8);
            for i in 0..8 {
                let _ = cache
                    .p2wpkh_signature_hash(
                        i,
                        po8[i].script_pubkey.as_script(),
                        po8[i].value,
                        EcdsaSighashType::All,
                    )
                    .unwrap();
            }
        },
    );

    // --- batch of 1-in jobs like signet tip ---
    println!("\n--- batch: 64 × 1-input P2TR (signet-like) ---");
    let jobs: Vec<_> = (0..64u8)
        .map(|i| {
            let (tx, po) = p2tr_keypath_tx(i.wrapping_add(20), 1);
            to_job(&tx, po)
        })
        .collect();
    // Pre-materialize owned jobs once (production builds them at connect).
    let owned = rbitcoin_consensus::script_bench::owned_jobs(&jobs);
    bench(
        "verify_owned_pool 64× P2TR (rayon, no re-clone)",
        50,
        || {
            rbitcoin_consensus::script_bench::verify_owned_pool(&owned).unwrap();
        },
    );
    bench(
        "verify_jobs_pool 64× P2TR (rayon + clone/iter)",
        50,
        || {
            rbitcoin_consensus::script_bench::verify_jobs_pool(&jobs).unwrap();
        },
    );
    bench("verify_job sequential 64× P2TR", 50, || {
        for j in &jobs {
            rbitcoin_consensus::script_bench::verify_job(j).unwrap();
        }
    });

    // estimate tip rate (owned pool = production shape)
    let t0 = Instant::now();
    for _ in 0..20 {
        rbitcoin_consensus::script_bench::verify_owned_pool(&owned).unwrap();
    }
    let per_batch = t0.elapsed() / 20;
    let inputs = 64u32;
    let us_per = per_batch.as_secs_f64() * 1e6 / f64::from(inputs);
    println!(
        "\n  ≈ {us_per:.0} µs/input pooled P2TR  → theoretical ~{:.0} inputs/s on this machine",
        1e6 / us_per
    );
    println!("  (IBD also pays reconstruct+connect+signet+Class C per block)");
}
