//! Profile confirm-path waste against a live signet store (if present).
//!
//!   cargo bench -p rbitcoin-consensus --bench confirm_waste
//!
//! Looks for CPU that does not advance tip: reconstruct integrity re-hash,
//! full-block rebuild, encode, etc.

use bitcoin::consensus::Encodable;
use rbitcoin_primitives::Height;
use rbitcoin_query::Query;
use std::path::Path;
use std::time::Instant;

fn bench(name: &str, iters: u32, mut f: impl FnMut()) {
    for _ in 0..2 {
        f();
    }
    let t0 = Instant::now();
    for _ in 0..iters {
        f();
    }
    let dt = t0.elapsed();
    println!(
        "{name:48}  {:>10.2?} total  {:>10.2?}/iter",
        dt,
        dt / iters.max(1)
    );
}

fn main() {
    let candidates = [
        "datadir-signet/store",
        "./datadir-signet/store",
        "/home/agent/workspace/datadir-signet/store",
    ];
    let path = candidates
        .iter()
        .map(Path::new)
        .find(|p| p.exists());
    let Some(path) = path else {
        println!("no signet store found — synthetic only");
        synthetic();
        return;
    };
    println!("store={}", path.display());
    let q = Query::open_or_create(path).expect("open store");
    let tip = q.tip_height().map(|h| h.0).unwrap_or(0);
    println!("tip={tip}");
    if tip < 10 {
        println!("tip too low for profile");
        return;
    }

    // Profile a range of recent confirmed heights (bodies exist).
    let start = tip.saturating_sub(32);
    let mut hashes = Vec::new();
    for h in start..=tip {
        if let Ok(Some((fk, rec))) = q.header_at_height(Height(h)) {
            let _ = fk;
            hashes.push((h, rec.hash));
        }
    }
    println!("profiling {} blocks from h={start}..{tip}", hashes.len());

    // 1) Full reconstruct (includes per-tx compute_txid integrity today)
    bench("reconstruct_archived_block × N", 3, || {
        for (_, hash) in &hashes {
            let _ = q.reconstruct_archived_block(hash).unwrap();
        }
    });

    // 2) Reconstruct without the txid re-hash (manual path)
    bench("load Class A rows only (no wire rebuild)", 3, || {
        for (_, hash) in &hashes {
            let (header_fk, _rec) = q.get_header_by_hash(hash).unwrap().unwrap();
            let fks = q.store().header_txs.get_list(header_fk).unwrap().unwrap();
            for fk in fks {
                let rec = q.get_tx(fk).unwrap();
                let _inputs = q.tx_input_run(&rec).unwrap();
                if rec.output_count > 0 {
                    let start = rec.output_start_fk.get().unwrap();
                    let _ = q
                        .get_output_run(rbitcoin_primitives::Fk(start), rec.output_count)
                        .unwrap();
                }
            }
        }
    });

    // 3) compute_txid only on reconstructed txs
    bench("compute_txid on all reconstructed txs", 2, || {
        for (_, hash) in &hashes {
            let b = q.reconstruct_archived_block(hash).unwrap().unwrap();
            for tx in &b.txdata {
                let _ = tx.compute_txid();
            }
        }
    });

    // 4) consensus_encode all non-coinbase (script path prep)
    bench("encode non-coinbase txs", 2, || {
        for (_, hash) in &hashes {
            let b = q.reconstruct_archived_block(hash).unwrap().unwrap();
            for tx in b.txdata.iter().skip(1) {
                let mut buf = Vec::new();
                tx.consensus_encode(&mut buf).unwrap();
            }
        }
    });

    // 5) block.block_hash() integrity
    bench("block.block_hash() on reconstructed", 3, || {
        for (_, hash) in &hashes {
            let b = q.reconstruct_archived_block(hash).unwrap().unwrap();
            let _ = b.block_hash();
            let _ = hash;
        }
    });

    synthetic();
}

fn synthetic() {
    println!("--- synthetic hash cost ---");
    use bitcoin::hashes::{sha256d, Hash as _};
    let payload = vec![0xabu8; 250];
    bench("sha256d 100k × 250B", 1, || {
        for i in 0u32..100_000 {
            let mut p = payload.clone();
            p[0] = (i & 0xff) as u8;
            let _ = sha256d::Hash::hash(&p);
        }
    });
}
