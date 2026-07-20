//! Audit production rayon sites: sequential vs parallel with real work units.
//!
//! Sites under test (only places that still use rayon):
//! 1. Script verify pool (`verify_scripts_pool` / fine `par_iter`)
//! 2. Confirm wire rebuild (`metas.par_iter` → reconstruct)
//!
//! Run:
//!   cargo bench -p rbitcoin-consensus --bench rayon_audit --release
//!
//! Interpreting: "material benefit" means ≥15% wall-time reduction vs sequential
//! at the job counts we see in IBD (scripts: tens–hundreds of txs; wire: 32–256
//! blocks/batch). Below that, assignment/collect overhead dominates.

use std::path::PathBuf;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use bitcoin::absolute::LockTime;
use bitcoin::hashes::Hash;
use bitcoin::key::TapTweak;
use bitcoin::secp256k1::{Keypair, Message, Secp256k1, SecretKey};
use bitcoin::sighash::{EcdsaSighashType, Prevouts, SighashCache, TapSighashType};
use bitcoin::{
    Amount, OutPoint, ScriptBuf, Sequence, Transaction, TxIn, TxOut, Witness,
};
use rbitcoin_consensus::script_bench::{self, JobBytes};
use rbitcoin_consensus::{block_to_apply, genesis_block, ChainParams};
use rbitcoin_query::Query;
use rayon::prelude::*;

fn timed(name: &str, iters: u32, mut f: impl FnMut()) -> Duration {
    // Warmup
    for _ in 0..((iters / 10).max(3).min(20)) {
        f();
    }
    let t0 = Instant::now();
    for _ in 0..iters {
        f();
    }
    let total = t0.elapsed();
    let per = total / iters;
    println!("  {name:42} {per:>12.3?}/op  ({iters}× total {total:.2?})");
    per
}

fn speedup(seq: Duration, par: Duration, workers: usize) -> (f64, f64) {
    let s = seq.as_secs_f64().max(1e-12);
    let p = par.as_secs_f64().max(1e-12);
    let su = s / p;
    let eff = 100.0 * su / workers as f64;
    (su, eff)
}

fn secret_from_seed(seed: u32) -> SecretKey {
    // Deterministic valid secp256k1 keys (avoid all-equal bytes which can be invalid).
    let mut sk_bytes = [0u8; 32];
    sk_bytes[0] = 1;
    sk_bytes[28..32].copy_from_slice(&seed.to_be_bytes());
    SecretKey::from_slice(&sk_bytes).expect("bench secret key")
}

fn p2tr_job(seed: u32) -> JobBytes {
    let secp = Secp256k1::new();
    let sk = secret_from_seed(seed.wrapping_add(1));
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

fn p2wpkh_job(seed: u32) -> JobBytes {
    use bitcoin::secp256k1::PublicKey;
    let secp = Secp256k1::new();
    let sk = secret_from_seed(seed.wrapping_add(1000));
    let pk = PublicKey::from_secret_key(&secp, &sk);
    let wpkh = bitcoin::WPubkeyHash::hash(&pk.serialize());
    let prevout = TxOut {
        value: Amount::from_sat(50_000),
        script_pubkey: ScriptBuf::new_p2wpkh(&wpkh),
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
        .p2wpkh_signature_hash(0, &prevout.script_pubkey, prevout.value, EcdsaSighashType::All)
        .unwrap();
    let msg = Message::from_digest(sighash.to_byte_array());
    let sig = secp.sign_ecdsa(&msg, &sk);
    let mut sig_bytes = sig.serialize_der().to_vec();
    sig_bytes.push(EcdsaSighashType::All as u8);
    tx.input[0].witness = Witness::from_slice(&[sig_bytes, pk.serialize().to_vec()]);
    JobBytes::new(vec![prevout], tx)
}

fn audit_scripts(workers: usize) {
    println!("\n======== 1) SCRIPT VERIFY (production verify_scripts_pool) ========\n");
    println!("Job = real P2TR key-path or P2WPKH (sighash + crypto). Production path for n>1 uses rayon par_iter.\n");

    for (kind, make) in [
        ("P2TR", p2tr_job as fn(u32) -> JobBytes),
        ("P2WPKH", p2wpkh_job as fn(u32) -> JobBytes),
    ] {
        println!("--- {kind} ---");
        for (label, n, iters) in [
            ("1 job (pool skips rayon)", 1u32, 200u32),
            ("2 jobs", 2, 150),
            ("4 jobs", 4, 120),
            ("8 jobs", 8, 80),
            ("16 jobs", 16, 50),
            ("32 jobs", 32, 40),
            ("64 jobs (≈ fat block)", 64, 30),
            ("128 jobs (multi-block wave)", 128, 20),
            ("256 jobs (large confirm run)", 256, 12),
        ] {
            let owned = script_bench::owned_jobs(
                &(0..n).map(|i| make(i.wrapping_add(17))).collect::<Vec<_>>(),
            );
            println!("\n  [{kind} {label}] n={n}");
            let t_seq = timed("sequential for-loop", iters, || {
                for j in &owned {
                    script_bench::verify_one_job(j).unwrap();
                }
            });
            let t_prod = timed("production verify_owned_pool", iters, || {
                script_bench::verify_owned_pool(&owned).unwrap();
            });
            let t_fine = timed("raw par_iter try_for_each", iters, || {
                owned
                    .par_iter()
                    .try_for_each(|j| script_bench::verify_one_job(j))
                    .unwrap();
            });
            if n == 1 {
                let (su, _) = speedup(t_seq, t_prod, 1);
                println!(
                    "  → pool vs seq ratio {:.2}× (should be ~1; single-job fast path)",
                    su
                );
            } else {
                let (su_p, eff_p) = speedup(t_seq, t_prod, workers);
                let (su_f, eff_f) = speedup(t_seq, t_fine, workers);
                let material = if su_p >= 1.15 {
                    "YES material benefit (≥15%)"
                } else if su_p >= 1.05 {
                    "marginal (5–15%)"
                } else {
                    "NO — overhead ≥ gain"
                };
                println!(
                    "  → prod speedup={su_p:.2}× ({eff_p:.0}% of ideal {workers}-way)  fine={su_f:.2}× ({eff_f:.0}%)  {material}"
                );
            }
        }
    }
}

fn tmp_dir(tag: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let p = std::env::temp_dir().join(format!("rbitcoin-rayon-audit-{tag}-{nanos}"));
    let _ = std::fs::remove_dir_all(&p);
    std::fs::create_dir_all(&p).unwrap();
    p
}

fn make_simple_block(
    params: &ChainParams,
    height: u32,
    prev: bitcoin::BlockHash,
    n_extra_tx: u32,
) -> bitcoin::Block {
    use bitcoin::block::{Header, Version};
    let mut txdata = Vec::with_capacity(1 + n_extra_tx as usize);
    let coinbase = Transaction {
        version: bitcoin::transaction::Version::ONE,
        lock_time: LockTime::ZERO,
        input: vec![TxIn {
            previous_output: OutPoint::null(),
            script_sig: ScriptBuf::from_bytes(height.to_le_bytes().to_vec()),
            sequence: Sequence::MAX,
            witness: Witness::new(),
        }],
        output: vec![TxOut {
            value: Amount::from_sat(50 * 100_000_000),
            script_pubkey: ScriptBuf::from_bytes(vec![0x51]),
        }],
    };
    txdata.push(coinbase);
    // Extra anyone-can-spend txs (coinbase-funded style dummies) fatten reconstruct.
    for i in 0..n_extra_tx {
        txdata.push(Transaction {
            version: bitcoin::transaction::Version::TWO,
            lock_time: LockTime::ZERO,
            input: vec![TxIn {
                previous_output: OutPoint {
                    txid: bitcoin::Txid::from_byte_array([height as u8; 32]),
                    vout: i,
                },
                script_sig: ScriptBuf::new(),
                sequence: Sequence::ENABLE_RBF_NO_LOCKTIME,
                witness: Witness::new(),
            }],
            output: vec![TxOut {
                value: Amount::from_sat(1000 + i as u64),
                script_pubkey: ScriptBuf::from_bytes(vec![0x51, (i & 0xff) as u8]),
            }],
        });
    }
    let hashes: Vec<_> = txdata
        .iter()
        .map(|t| t.compute_txid().to_raw_hash())
        .collect();
    let merkle = bitcoin::merkle_tree::calculate_root(hashes.into_iter())
        .unwrap()
        .into();
    let g = genesis_block(params);
    let header = Header {
        version: Version::ONE,
        prev_blockhash: prev,
        merkle_root: merkle,
        time: g.header.time.saturating_add(height * 600),
        bits: g.header.bits,
        nonce: height,
    };
    bitcoin::Block { header, txdata }
}

fn audit_wire_rebuild(workers: usize) {
    println!("\n======== 2) WIRE REBUILD (confirm_run par_iter shape) ========\n");
    println!(
        "Work unit = reconstruct_archived_block_from_parts (Class A body → wire Block).\n\
         Production: metas.par_iter().map(reconstruct).collect()\n"
    );

    let dir = tmp_dir("wire");
    let q = Query::open_or_create(dir.join("store")).unwrap();
    let params = ChainParams::regtest();
    let genesis = genesis_block(&params);
    let (gh, gtxs) = block_to_apply(&q, &genesis.header, &genesis.txdata).unwrap();
    q.archive_block(&gh, &gtxs).unwrap();
    let mut prev = genesis.header.block_hash();

    const N: usize = 128;
    let mut items = Vec::with_capacity(N);
    // Include genesis as first reconstruct target.
    {
        let (hfk, rec) = q.get_header_by_hash(&prev.to_byte_array()).unwrap().unwrap();
        let tx_fks = q.store().header_txs.get_list(hfk).unwrap().unwrap();
        items.push((rec, tx_fks));
    }
    // ~50 txs/block — closer to mid-chain density than coinbase-only.
    const EXTRA_TX: u32 = 49;
    for h in 1u32..N as u32 {
        let b = make_simple_block(&params, h, prev, EXTRA_TX);
        let (rec, txs) = block_to_apply(&q, &b.header, &b.txdata).unwrap();
        q.archive_block(&rec, &txs).unwrap();
        prev = b.header.block_hash();
        let (hfk, rec2) = q.get_header_by_hash(&prev.to_byte_array()).unwrap().unwrap();
        let tx_fks = q.store().header_txs.get_list(hfk).unwrap().unwrap();
        items.push((rec2, tx_fks));
    }
    println!("archived {N} blocks × ~{} txs each for reconstruct\n", 1 + EXTRA_TX);

    for (label, n, iters) in [
        ("1 block", 1usize, 40u32),
        ("4 blocks", 4, 30),
        ("8 blocks", 8, 25),
        ("16 blocks", 16, 20),
        ("32 blocks (confirm batch)", 32, 15),
        ("64 blocks", 64, 10),
        ("128 blocks (fat run)", 128, 6),
    ] {
        let slice = &items[..n];
        println!("\n  [wire {label}] n={n}");
        let t_seq = timed("sequential reconstruct", iters, || {
            let mut out = Vec::with_capacity(n);
            for (rec, fks) in slice {
                out.push(
                    q.reconstruct_archived_block_from_parts(rec.clone(), fks.clone())
                        .unwrap(),
                );
            }
            assert_eq!(out.len(), n);
        });
        let t_par = timed("par_iter reconstruct (production shape)", iters, || {
            let out: Vec<_> = slice
                .par_iter()
                .map(|(rec, fks)| {
                    q.reconstruct_archived_block_from_parts(rec.clone(), fks.clone())
                        .unwrap()
                })
                .collect();
            assert_eq!(out.len(), n);
        });
        if n == 1 {
            let (su, _) = speedup(t_seq, t_par, 1);
            println!("  → par vs seq {su:.2}× (single block; pool overhead expected)");
        } else {
            let (su, eff) = speedup(t_seq, t_par, workers);
            let material = if su >= 1.15 {
                "YES material benefit (≥15%)"
            } else if su >= 1.05 {
                "marginal (5–15%)"
            } else {
                "NO — overhead ≥ gain"
            };
            println!(
                "  → speedup={su:.2}× ({eff:.0}% of ideal {workers}-way)  {material}"
            );
        }
    }

    let _ = std::fs::remove_dir_all(&dir);
}

fn main() {
    let workers = rayon::current_num_threads();
    let cpus = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(0);
    println!("rayon_audit — production rayon sites");
    println!("rayon threads={workers}  available_parallelism={cpus:?}");
    println!("threshold for 'material benefit': ≥15% wall-time cut vs sequential\n");

    audit_scripts(workers);
    audit_wire_rebuild(workers);

    println!("\n======== SUMMARY GUIDE ========");
    println!(
        "- Scripts: production uses rayon only when jobs.len() > 1.\n\
         - Wire rebuild: always par_iter (even n=1 pays pool schedule).\n\
         - If wire shows no benefit at typical batch sizes (32–64), consider\n\
           sequential rebuild or only parallelize when n >= 4.\n\
         - Script waves of 16+ txs should show clear multi-core speedup on crypto."
    );
}
