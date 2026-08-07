//! Microbench: single put_spend vs put_spend_batch (Class C / backfill hot path).
//!
//! Run: `cargo bench -p rbitcoin-store --bench spend_batch`

use rbitcoin_primitives::Fk;
use rbitcoin_store::Store;
use std::path::PathBuf;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

fn tmp_dir() -> PathBuf {
    let n = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let p = std::env::temp_dir().join(format!("rbitcoin-bench-spend-{n}"));
    let _ = std::fs::remove_dir_all(&p);
    std::fs::create_dir_all(&p).expect("mkdir");
    p
}

fn bench(name: &str, iters: u32, mut f: impl FnMut()) {
    for _ in 0..2 {
        f();
    }
    let t0 = Instant::now();
    for _ in 0..iters {
        f();
    }
    let dt = t0.elapsed();
    println!("{name:40}  {:>8.2?} total  {:>8.2?}/iter", dt, dt / iters);
}

fn main() {
    let n_edges = 500u32;
    let iters = 12u32;

    println!("edges per op = {n_edges}");

    bench("put_spend × N (loop)", iters, || {
        let p = tmp_dir();
        let store = Store::create(&p).expect("create");
        for i in 0..n_edges {
            let mut txid = [0u8; 32];
            txid[0..4].copy_from_slice(&i.to_le_bytes());
            store.put_spend(&txid, 0, Fk(1 + i as u64), 0).expect("put");
        }
        store.flush().ok();
        drop(store);
        let _ = std::fs::remove_dir_all(&p);
    });

    bench("put_spend_batch(N)", iters, || {
        let p = tmp_dir();
        let store = Store::create(&p).expect("create");
        let mut edges = Vec::with_capacity(n_edges as usize);
        for i in 0..n_edges {
            let mut txid = [0u8; 32];
            txid[0..4].copy_from_slice(&i.to_le_bytes());
            edges.push((txid, 0u32, Fk(1 + i as u64), 0u32));
        }
        store.put_spend_batch(&edges).expect("batch");
        store.flush().ok();
        drop(store);
        let _ = std::fs::remove_dir_all(&p);
    });
}
