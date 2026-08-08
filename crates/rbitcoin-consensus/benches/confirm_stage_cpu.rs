//! Pack-scale CPU microbenchmarks for **lookup / scripts / write** confirm work
//! (not load-pin object build — covered by rbitcoin-query pin_pack_cpu).
//!
//! Sizes track soft confirm pack (~8000 inputs / unique parents).
//!
//! ```text
//! cargo bench -p rbitcoin-consensus --bench confirm_stage_cpu
//! ```

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::hash::{BuildHasherDefault, Hasher};
use std::hint::black_box;
use std::time::Instant;

use bitcoin::absolute::LockTime;
use bitcoin::hashes::{hash160, Hash};
use bitcoin::secp256k1::{Message, Secp256k1, SecretKey};
use bitcoin::sighash::{EcdsaSighashType, SighashCache};
use bitcoin::{Amount, OutPoint, ScriptBuf, Sequence, Transaction, TxIn, TxOut, Witness};
use rayon::prelude::*;
use rbitcoin_consensus::script_bench::{self, JobBytes};

const N_INPUTS: usize = 8000;
const N_PARENTS: usize = 6000;
const ITERS: u32 = 50;

#[derive(Default, Clone, Copy)]
struct U64IdentityHasher(u64);
impl Hasher for U64IdentityHasher {
    fn write(&mut self, bytes: &[u8]) {
        for &b in bytes {
            self.0 = self.0.wrapping_mul(0x1000_0000_01b3).wrapping_add(u64::from(b));
        }
    }
    fn write_u64(&mut self, i: u64) {
        self.0 = i;
    }
    fn write_u32(&mut self, i: u32) {
        self.0 = u64::from(i);
    }
    fn finish(&self) -> u64 {
        self.0
    }
}

fn bench(name: &str, iters: u32, mut f: impl FnMut()) {
    for _ in 0..4 {
        f();
    }
    let t0 = Instant::now();
    for _ in 0..iters {
        f();
    }
    let dt = t0.elapsed();
    let per = dt / iters.max(1);
    println!("{name:58}  {per:>12.3?} /op  ({iters} iters, {dt:.3?} total)");
}

fn txid(i: u64) -> [u8; 32] {
    let mut t = [0u8; 32];
    t[..8].copy_from_slice(&i.to_le_bytes());
    t[8] = (i >> 8) as u8;
    t
}

/// Write/assemble: pending_spent HashSet of (txid, vout) — production shape.
fn bench_pending_spent() {
    let keys: Vec<([u8; 32], u32)> = (0..N_INPUTS as u64)
        .map(|i| (txid(i % (N_PARENTS as u64)), (i % 3) as u32))
        .collect();
    bench(
        &format!("write pending_spent HashSet insert+contains x{N_INPUTS}"),
        ITERS,
        || {
            let mut s: HashSet<([u8; 32], u32)> = HashSet::with_capacity(N_INPUTS);
            for k in &keys {
                let hit = s.contains(k);
                black_box(hit);
                s.insert(*k);
            }
            black_box(s.len());
        },
    );
    bench(
        &format!("write pending_spent BTreeSet insert+contains x{N_INPUTS}"),
        ITERS,
        || {
            let mut s: BTreeSet<([u8; 32], u32)> = BTreeSet::new();
            for k in &keys {
                let hit = s.contains(k);
                black_box(hit);
                s.insert(*k);
            }
            black_box(s.len());
        },
    );
    // Sorted vec: push all then sort+dedup once, binary_search for contains-only phase.
    // Matches "build once per block, then query" better than interleaved insert.
    bench(
        &format!("write pending_spent sorted Vec build+bsearch x{N_INPUTS}"),
        ITERS,
        || {
            let mut v = keys.clone();
            v.sort_unstable();
            v.dedup();
            let mut hits = 0u32;
            for k in &keys {
                if v.binary_search(k).is_ok() {
                    hits = hits.wrapping_add(1);
                }
            }
            black_box((v.len(), hits));
        },
    );
    // Interleaved: maintain sorted vec with binary_search insert (worst case for vec).
    bench(
        &format!("write pending_spent sorted Vec interleaved insert x{N_INPUTS}"),
        ITERS / 5 + 1,
        || {
            let mut v: Vec<([u8; 32], u32)> = Vec::with_capacity(N_INPUTS);
            for k in &keys {
                match v.binary_search(k) {
                    Ok(_) => {}
                    Err(i) => v.insert(i, *k),
                }
            }
            black_box(v.len());
        },
    );
}

/// Lookup: create_by_txid / same_batch HashMap<[u8;32], u64>.
fn bench_txid_maps() {
    let pairs: Vec<([u8; 32], u64)> = (0..N_PARENTS as u64).map(|i| (txid(i), i + 1)).collect();
    bench(
        &format!("lookup HashMap txid→fk insert+get x{N_PARENTS}"),
        ITERS,
        || {
            let mut m: HashMap<[u8; 32], u64> = HashMap::with_capacity(N_PARENTS);
            for &(t, id) in &pairs {
                m.insert(t, id);
            }
            let mut s = 0u64;
            for &(t, _) in &pairs {
                s = s.wrapping_add(*m.get(&t).unwrap());
            }
            black_box(s);
        },
    );
    bench(
        &format!("lookup BTreeMap txid→fk insert+get x{N_PARENTS}"),
        ITERS,
        || {
            let mut m: BTreeMap<[u8; 32], u64> = BTreeMap::new();
            for &(t, id) in &pairs {
                m.insert(t, id);
            }
            let mut s = 0u64;
            for &(t, _) in &pairs {
                s = s.wrapping_add(*m.get(&t).unwrap());
            }
            black_box(s);
        },
    );
    bench(
        &format!("lookup sorted Vec txid→fk build+bsearch x{N_PARENTS}"),
        ITERS,
        || {
            let mut v = pairs.clone();
            v.sort_unstable_by(|a, b| a.0.cmp(&b.0));
            let mut s = 0u64;
            for &(t, _) in &pairs {
                let i = v.binary_search_by(|p| p.0.cmp(&t)).unwrap();
                s = s.wrapping_add(v[i].1);
            }
            black_box(s);
        },
    );
}

/// Write create_h path: u64→u32 height map (identity vs default hasher).
fn bench_u64_height_maps() {
    let keys: Vec<u64> = (1..=N_PARENTS as u64).collect();
    bench(
        &format!("write HashMap u64→u32 default hasher x{N_PARENTS}"),
        ITERS * 2,
        || {
            let mut m: HashMap<u64, u32> = HashMap::with_capacity(N_PARENTS);
            for &k in &keys {
                m.insert(k, (k % 1_000_000) as u32);
            }
            let mut s = 0u32;
            for &k in &keys {
                s = s.wrapping_add(*m.get(&k).unwrap());
            }
            black_box(s);
        },
    );
    bench(
        &format!("write HashMap u64→u32 identity hasher x{N_PARENTS}"),
        ITERS * 2,
        || {
            let mut m: HashMap<u64, u32, BuildHasherDefault<U64IdentityHasher>> =
                HashMap::with_capacity_and_hasher(N_PARENTS, BuildHasherDefault::default());
            for &k in &keys {
                m.insert(k, (k % 1_000_000) as u32);
            }
            let mut s = 0u32;
            for &k in &keys {
                s = s.wrapping_add(*m.get(&k).unwrap());
            }
            black_box(s);
        },
    );
    bench(
        &format!("write sorted Vec u64→u32 sequential+bsearch x{N_PARENTS}"),
        ITERS * 2,
        || {
            let v: Vec<(u64, u32)> = keys
                .iter()
                .map(|&k| (k, (k % 1_000_000) as u32))
                .collect();
            // already sorted by construction
            let mut s = 0u32;
            for &k in &keys {
                let i = v.binary_search_by_key(&k, |(kk, _)| *kk).unwrap();
                s = s.wrapping_add(v[i].1);
            }
            black_box(s);
        },
    );
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

/// Scripts stage: pack-scale job count (one checkable input ≈ one job).
fn bench_scripts_pack() {
    // Fewer iters: crypto is heavy. N jobs ≈ soft pack inputs.
    let n_jobs = 512usize; // representative fat pack slice; full 8k too slow for tight loop
    let jobs: Vec<JobBytes> = (0..n_jobs)
        .map(|i| p2wpkh_job(((i % 200) + 1) as u8))
        .collect();
    bench(
        &format!("scripts sequential verify x{n_jobs}"),
        8,
        || {
            for j in &jobs {
                let _ = black_box(script_bench::verify_job(j));
            }
        },
    );
    bench(
        &format!("scripts rayon par_iter verify x{n_jobs}"),
        8,
        || {
            jobs.par_iter().for_each(|j| {
                let _ = black_box(script_bench::verify_job(j));
            });
        },
    );
    bench(
        &format!("scripts rayon par_chunks(32) verify x{n_jobs}"),
        8,
        || {
            jobs.par_chunks(32).for_each(|chunk| {
                for j in chunk {
                    let _ = black_box(script_bench::verify_job(j));
                }
            });
        },
    );
}

fn main() {
    println!("confirm_stage_cpu: N_INPUTS={N_INPUTS} N_PARENTS={N_PARENTS}");
    println!("--- write: pending_spent ---");
    bench_pending_spent();
    println!("--- lookup: txid maps ---");
    bench_txid_maps();
    println!("--- write: u64 height maps ---");
    bench_u64_height_maps();
    println!("--- scripts: pack-scale verify ---");
    bench_scripts_pack();
}
