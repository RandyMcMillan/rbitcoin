//! Microbench: script-pool strategies that affect IBD tip rate.
//!
//! Run: `cargo bench -p rbitcoin-consensus --bench script_pool`
//!
//! Work units are synthetic SHA256 rounds sized like real script checks
//! (real verify needs valid txs; we measure **scheduling + parallelism** cost).

use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

/// Stand-in for one script input check. Uses sha256d rounds so cost is closer to
/// ECDSA/script work than DefaultHasher (which made parallel look worse than seq).
fn fake_script_work(seed: u64, rounds: u32) -> u64 {
    use bitcoin_hashes::{sha256d, Hash as _};
    let mut buf = seed.to_le_bytes().to_vec();
    buf.resize(64, 0xab);
    let mut h = [0u8; 32];
    for _ in 0..rounds {
        h = *sha256d::Hash::hash(&buf).as_byte_array();
        buf[..32].copy_from_slice(&h);
    }
    u64::from_le_bytes(h[0..8].try_into().unwrap())
}

/// Per-tx job with `n_inputs` work units (mirrors ScriptCheckJob input fan-out).
struct Job {
    seed: u64,
    n_inputs: usize,
}

fn work_sequential(jobs: &[Job], rounds: u32) -> u64 {
    let mut acc = 0u64;
    for j in jobs {
        for i in 0..j.n_inputs {
            acc ^= fake_script_work(j.seed.wrapping_add(i as u64), rounds);
        }
    }
    acc
}

/// Current design: flatten per-input, steal with atomics, thread::scope.
fn work_parallel_per_input(jobs: &[Job], rounds: u32, max_workers: usize) -> u64 {
    let mut flat: Vec<(usize, usize)> = Vec::new();
    for (ji, j) in jobs.iter().enumerate() {
        for ii in 0..j.n_inputs {
            flat.push((ji, ii));
        }
    }
    if flat.is_empty() {
        return 0;
    }
    let n_workers = max_workers.clamp(1, 32).min(flat.len());
    if n_workers == 1 {
        return work_sequential(jobs, rounds);
    }
    let next = AtomicUsize::new(0);
    let mut partial = vec![0u64; n_workers];
    std::thread::scope(|scope| {
        for slot in partial.iter_mut() {
            let flat = &flat;
            let jobs = jobs;
            let next = &next;
            scope.spawn(move || {
                let mut acc = 0u64;
                loop {
                    let i = next.fetch_add(1, Ordering::Relaxed);
                    if i >= flat.len() {
                        break;
                    }
                    let (ji, ii) = flat[i];
                    acc ^= fake_script_work(jobs[ji].seed.wrapping_add(ii as u64), rounds);
                }
                *slot = acc;
            });
        }
    });
    partial.into_iter().fold(0u64, |a, b| a ^ b)
}

/// Per-tx granularity (old design before per-input flatten).
fn work_parallel_per_tx(jobs: &[Job], rounds: u32, max_workers: usize) -> u64 {
    if jobs.is_empty() {
        return 0;
    }
    let n_workers = max_workers.clamp(1, 32).min(jobs.len());
    if n_workers == 1 {
        return work_sequential(jobs, rounds);
    }
    let next = AtomicUsize::new(0);
    let mut partial = vec![0u64; n_workers];
    std::thread::scope(|scope| {
        for slot in partial.iter_mut() {
            let jobs = jobs;
            let next = &next;
            scope.spawn(move || {
                let mut acc = 0u64;
                loop {
                    let i = next.fetch_add(1, Ordering::Relaxed);
                    if i >= jobs.len() {
                        break;
                    }
                    for ii in 0..jobs[i].n_inputs {
                        acc ^= fake_script_work(jobs[i].seed.wrapping_add(ii as u64), rounds);
                    }
                }
                *slot = acc;
            });
        }
    });
    partial.into_iter().fold(0u64, |a, b| a ^ b)
}

fn bench(name: &str, iters: u32, mut f: impl FnMut() -> u64) {
    // Warmup
    for _ in 0..2 {
        let _ = f();
    }
    let t0 = Instant::now();
    let mut sink = 0u64;
    for _ in 0..iters {
        sink ^= f();
    }
    let dt = t0.elapsed();
    let per = dt / iters;
    println!(
        "{name:48}  {:>8.2?} total  {:>8.2?}/iter  (sink={sink})",
        dt, per
    );
}

fn main() {
    let workers = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1);
    println!("available_parallelism={workers}");
    println!("--- Fat block: 400 txs × 2 inputs (typical post-milestone wall-time driver) ---");
    let fat: Vec<Job> = (0..400)
        .map(|i| Job {
            seed: i as u64 * 17,
            n_inputs: 2,
        })
        .collect();
    // ~real ECDSA/script is heavier; sha256d×N approximates CPU-bound verify.
    let rounds = 400u32;
    let iters = 8u32;
    bench("seq fat", iters, || work_sequential(&fat, rounds));
    bench("par/tx fat", iters, || work_parallel_per_tx(&fat, rounds, workers));
    bench("par/input fat", iters, || {
        work_parallel_per_input(&fat, rounds, workers)
    });

    println!("--- Thin blocks: 8 blocks × 3 txs × 1 input (multi-block thesis) ---");
    let thin_one: Vec<Job> = (0..3)
        .map(|i| Job {
            seed: i as u64,
            n_inputs: 1,
        })
        .collect();
    let thin_multi: Vec<Job> = (0..24)
        .map(|i| Job {
            seed: i as u64 * 3,
            n_inputs: 1,
        })
        .collect();
    let iters_t = 50u32;
    // Simulate: 8× single-block parallel waves vs one multi-block wave of 24 jobs.
    bench("8× par/input thin (single-block loop)", iters_t, || {
        let mut a = 0u64;
        for b in 0..8u64 {
            let jobs: Vec<Job> = thin_one
                .iter()
                .map(|j| Job {
                    seed: j.seed.wrapping_add(b * 100),
                    n_inputs: j.n_inputs,
                })
                .collect();
            a ^= work_parallel_per_input(&jobs, rounds, workers);
        }
        a
    });
    bench("1× par/input 24 jobs (multi-block wave)", iters_t, || {
        work_parallel_per_input(&thin_multi, rounds, workers)
    });
    // Spawn overhead alone: empty-ish waves
    println!("--- Pool spawn overhead (1 input, tiny work) ---");
    let tiny = vec![Job {
        seed: 1,
        n_inputs: 1,
    }];
    let iters_o = 200u32;
    bench("par/input 1-job (spawn tax)", iters_o, || {
        work_parallel_per_input(&tiny, 1, workers)
    });
    bench("seq 1-job", iters_o, || work_sequential(&tiny, 1));
}
