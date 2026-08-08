//! Pack-scale CPU microbenchmarks for confirm load pin hot paths.
//!
//! Workload size matches soft confirm pack (~8000 unique parents), not full-chain maps.
//! Exercises **shipped** [`BatchParents`] / [`PipelineParentStore`] APIs.
//!
//! ```text
//! cargo bench -p rbitcoin-query --bench pin_pack_cpu --release
//! ```

use std::collections::HashMap;
use std::hint::black_box;
use std::sync::Arc;
use std::time::Instant;

use rbitcoin_primitives::Fk;
use rbitcoin_query::{BatchParents, PipelineParentStore};
use rbitcoin_store::{OutputRecord, TxRecord};

/// Soft pack budget order of magnitude (see `CONFIRM_BATCH_INPUTS_DEFAULT`).
const N_PARENTS: usize = 8000;
const N_COLD: usize = 2500; // ~30% cold denserels
const ITERS: u32 = 40;

fn sample_tx(id: u64) -> TxRecord {
    let mut txid = [0u8; 32];
    txid[..8].copy_from_slice(&id.to_le_bytes());
    TxRecord {
        txid,
        version: 1,
        locktime: 0,
        input_start_fk: Fk::NULL,
        input_count: 1,
        output_start_fk: Fk::NULL,
        output_count: 2,
    }
}

fn sample_out(seed: u64) -> OutputRecord {
    // Realistic-ish script length (P2WPKH-ish 22 bytes) so clone cost is not toy.
    let mut script = vec![0u8; 22];
    script[0] = 0x00;
    script[1] = 0x14;
    script[2..10].copy_from_slice(&seed.to_le_bytes());
    OutputRecord::unspent(50_000, script)
}

fn bench(name: &str, iters: u32, mut f: impl FnMut()) {
    for _ in 0..5 {
        f();
    }
    let t0 = Instant::now();
    for _ in 0..iters {
        f();
    }
    let dt = t0.elapsed();
    let per = dt / iters.max(1);
    println!("{name:56}  {per:>12.3?} /op  ({iters} iters, {dt:.3?} total)");
}

/// Shipped path: vacant `insert_owned` for N parents (cold range_fill shape).
fn bench_insert_owned_vacant() {
    let live0 = vec![(0u32, sample_out(1))];
    bench(
        &format!("BatchParents::insert_owned vacant x{N_PARENTS}"),
        ITERS,
        || {
            let mut bp = BatchParents::with_capacity(N_PARENTS);
            for i in 1..=N_PARENTS as u64 {
                bp.insert_owned(
                    Fk(i),
                    sample_tx(i),
                    live0.clone(),
                    vec![0],
                    Some(false),
                    Some((i * 64, 100)),
                    vec![(0, 32)],
                );
            }
            black_box(bp.len());
        },
    );
}

/// Shipped path: pin_covered after full fill (plan hit loop shape).
fn bench_pin_covered() {
    let mut bp = BatchParents::with_capacity(N_PARENTS);
    let live0 = vec![(0u32, sample_out(1))];
    for i in 1..=N_PARENTS as u64 {
        bp.insert_owned(
            Fk(i),
            sample_tx(i),
            live0.clone(),
            vec![0],
            Some(false),
            Some((i * 64, 100)),
            vec![(0, 32)],
        );
    }
    let need = [0u32];
    bench(
        &format!("BatchParents::pin_covered x{N_PARENTS} (filled)"),
        ITERS * 4,
        || {
            for i in 1..=N_PARENTS as u64 {
                black_box(bp.pin_covered(Fk(i), &need));
            }
        },
    );
}

/// Shipped path: adopt empty store + insert + publish (full pin share cycle).
fn bench_adopt_insert_publish() {
    let live0 = vec![(0u32, sample_out(2))];
    let store = Arc::new(PipelineParentStore::new());
    // Seed store once so adopt has hits on later ops.
    {
        let mut seed = BatchParents::with_store(Arc::clone(&store), N_PARENTS);
        for i in 1..=N_PARENTS as u64 {
            if i % 3 == 0 {
                seed.insert_owned(
                    Fk(i),
                    sample_tx(i),
                    live0.clone(),
                    vec![0],
                    Some(false),
                    Some((i * 64, 100)),
                    vec![(0, 32)],
                );
            }
        }
        seed.publish_to_store();
    }
    let ids: Vec<u64> = (1..=N_PARENTS as u64).collect();
    bench(
        &format!("adopt+insert_miss+publish x{N_PARENTS} (~1/3 adopt hits)"),
        ITERS,
        || {
            let mut bp = BatchParents::with_store(Arc::clone(&store), N_PARENTS);
            bp.adopt_from_store(ids.iter().copied());
            for i in 1..=N_PARENTS as u64 {
                if !bp.contains(Fk(i)) {
                    bp.insert_owned(
                        Fk(i),
                        sample_tx(i),
                        live0.clone(),
                        vec![0],
                        Some(false),
                        Some((i * 64, 100)),
                        vec![(0, 32)],
                    );
                }
            }
            bp.publish_to_store();
            black_box(bp.len());
        },
    );
}

/// Hypothesis: sorted Vec build + binary_search lookup vs HashMap for u64→usize.
fn bench_map_hypothesis() {
    let keys: Vec<u64> = (1..=N_PARENTS as u64).collect();
    // HashMap insert + lookup
    bench(
        &format!("hypothesis HashMap insert+lookup x{N_PARENTS}"),
        ITERS * 4,
        || {
            let mut m = HashMap::with_capacity(N_PARENTS);
            for &k in &keys {
                m.insert(k, k);
            }
            let mut s = 0u64;
            for &k in &keys {
                s = s.wrapping_add(*m.get(&k).unwrap());
            }
            black_box(s);
        },
    );
    // Sorted vec: push unsorted (reversed) then sort + binary_search
    bench(
        &format!("hypothesis sorted Vec build(rev)+lookup x{N_PARENTS}"),
        ITERS * 4,
        || {
            let mut v: Vec<(u64, u64)> = keys.iter().rev().map(|&k| (k, k)).collect();
            v.sort_unstable_by_key(|(k, _)| *k);
            let mut s = 0u64;
            for &k in &keys {
                let i = v.binary_search_by_key(&k, |(kk, _)| *kk).unwrap();
                s = s.wrapping_add(v[i].1);
            }
            black_box(s);
        },
    );
    // Sorted vec: already-sorted push + lookup (best case for pin if parents sorted)
    bench(
        &format!("hypothesis sorted Vec sequential build+lookup x{N_PARENTS}"),
        ITERS * 4,
        || {
            let mut v: Vec<(u64, u64)> = Vec::with_capacity(N_PARENTS);
            for &k in &keys {
                v.push((k, k));
            }
            let mut s = 0u64;
            for &k in &keys {
                let i = v.binary_search_by_key(&k, |(kk, _)| *kk).unwrap();
                s = s.wrapping_add(v[i].1);
            }
            black_box(s);
        },
    );
}

/// Cold denserels fill shape: N_COLD inserts into empty BatchParents.
fn bench_range_fill_cold() {
    let live0 = vec![(0u32, sample_out(3)), (1u32, sample_out(4))];
    bench(
        &format!("range_fill-like insert_owned x{N_COLD}"),
        ITERS * 2,
        || {
            let mut bp = BatchParents::with_capacity(N_COLD);
            for i in 1..=N_COLD as u64 {
                bp.insert_owned(
                    Fk(i),
                    sample_tx(i),
                    live0.clone(),
                    vec![0, 1],
                    Some(false),
                    Some((i * 128, 200)),
                    vec![(0, 40), (1, 80)],
                );
            }
            black_box(bp.len());
        },
    );
}

fn main() {
    println!("pin_pack_cpu: N_PARENTS={N_PARENTS} N_COLD={N_COLD} (pack-scale)");
    println!("--- shipped BatchParents ---");
    bench_insert_owned_vacant();
    bench_range_fill_cold();
    bench_pin_covered();
    bench_adopt_insert_publish();
    println!("--- map hypothesis (not production pins) ---");
    bench_map_hypothesis();
}
