//! Microbenchmarks for the IBD main-loop freeze (drain + header apply).
//!
//! Run with:
//!   cargo test -p rbitcoin-net freeze_bench --release -- --nocapture --test-threads=1

use bitcoin::block::{Header, Version};
use bitcoin::hashes::Hash;
use bitcoin::{BlockHash, CompactTarget, Network};
use rbitcoin_consensus::{ChainParams, Milestone};
use rbitcoin_net::ChainHub;
use rbitcoin_query::Query;
use rbitcoin_store::Store;
use std::collections::{HashMap, HashSet, VecDeque};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

struct TempStore {
    path: PathBuf,
}

impl Drop for TempStore {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

fn temp_hub() -> (TempStore, Arc<ChainHub>) {
    // Tiny heads — this module is diagnostic and often ignored, but keep cheap.
    if std::env::var_os("RBITCOIN_HEAD_SCALE").is_none() {
        std::env::set_var("RBITCOIN_HEAD_SCALE", "tiny");
    }
    let stamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    let path = std::env::temp_dir().join(format!("rbitcoin-freeze-bench-{stamp}"));
    let _ = std::fs::remove_dir_all(&path);
    std::fs::create_dir_all(&path).expect("mkdir");
    let _ = Store::create(&path).expect("store create");
    let query = Query::open_or_create(&path).expect("query");
    let params = ChainParams::regtest();
    let hub = Arc::new(ChainHub::new(query, params, Milestone::NONE));
    hub.ensure_genesis().expect("genesis");
    (TempStore { path }, hub)
}

fn dummy_header(prev: BlockHash, nonce: u32) -> Header {
    Header {
        version: Version::from_consensus(4),
        prev_blockhash: prev,
        merkle_root: bitcoin::TxMerkleNode::from_byte_array([nonce as u8; 32]),
        time: 1_700_000_000 + nonce,
        bits: CompactTarget::from_consensus(0x207f_ffff),
        nonce,
    }
}

/// Build a chain of `n` headers on top of genesis; return them in order.
fn chain_headers(hub: &ChainHub, n: u32) -> Vec<Header> {
    let mut out = Vec::with_capacity(n as usize);
    let mut prev = hub
        .tip_hash()
        .unwrap_or_else(|| BlockHash::from_byte_array([0u8; 32]));
    // Prefer genesis hash from store if tip empty.
    if prev.to_byte_array() == [0u8; 32] {
        if let Ok(Some((_, rec))) = hub.query.get_header_by_hash(
            &bitcoin::blockdata::constants::genesis_block(Network::Regtest)
                .block_hash()
                .to_byte_array(),
        ) {
            let _ = rec;
        }
        prev = bitcoin::blockdata::constants::genesis_block(Network::Regtest).block_hash();
    }
    for i in 1..=n {
        let h = dummy_header(prev, i);
        let _ = hub.ensure_header_fk(&h);
        prev = h.block_hash();
        out.push(h);
    }
    out
}

/// Always-on smoke so coverage hits helpers (full microbench remains ignored).
#[test]
fn freeze_bench_helpers_smoke() {
    let (_dir, hub) = temp_hub();
    let headers = chain_headers(&hub, 8);
    assert_eq!(headers.len(), 8);
    for h in &headers {
        let _ = hub.ensure_header_fk(h).unwrap();
    }
    // Second pass hits known headers (hash-head path).
    for h in &headers {
        let _ = hub.ensure_header_fk(h).unwrap();
    }
}

#[test]
#[ignore = "diagnostic microbench; run: cargo test -p rbitcoin-net freeze_bench -- --ignored --nocapture"]
fn freeze_bench_ensure_header_fk_known_vs_new() {
    let (_dir, hub) = temp_hub();
    let headers = chain_headers(&hub, 4_000);
    eprintln!("\n=== ensure_header_fk (store already holds all headers) ===");

    // Cold-ish: first re-ensure of 2000 known headers (hash-head lookup each).
    let batch = &headers[..2_000];
    let t0 = Instant::now();
    for h in batch {
        let _ = hub.ensure_header_fk(h).unwrap();
    }
    let re_ensure = t0.elapsed();

    // RAM fast-path simulation: skip ensure when HashMap already has fk.
    let mut fks = HashMap::new();
    for h in batch {
        let fk = hub.ensure_header_fk(h).unwrap();
        fks.insert(h.block_hash(), fk);
    }
    let t0 = Instant::now();
    let mut hits = 0u32;
    for h in batch {
        let hash = h.block_hash();
        if fks.contains_key(&hash) {
            hits += 1;
            continue;
        }
        let _ = hub.ensure_header_fk(h).unwrap();
    }
    let ram_skip = t0.elapsed();

    eprintln!(
        "  re-ensure 2000 known headers via store: {re_ensure:.2?}  ({:.1} µs/hdr)",
        re_ensure.as_secs_f64() * 1e6 / 2000.0
    );
    eprintln!(
        "  RAM header_fks skip same 2000:          {ram_skip:.2?}  hits={hits}  ({:.3} µs/hdr)",
        ram_skip.as_secs_f64() * 1e6 / 2000.0
    );
    let speedup = re_ensure.as_secs_f64() / ram_skip.as_secs_f64().max(1e-12);
    eprintln!("  speedup if we skip known fks: {speedup:.0}×");
    assert_eq!(hits, 2000);
    // Both paths are sub-ms on warm store; only guard against pathological RAM miss.
    assert!(
        speedup > 0.25,
        "RAM skip path unexpectedly much slower than re-ensure; speedup={speedup:.1}"
    );
}

#[test]
#[ignore = "diagnostic microbench; run: cargo test -p rbitcoin-net freeze_bench -- --ignored --nocapture"]
fn freeze_bench_headers_apply_overlap() {
    let (_dir, hub) = temp_hub();
    let headers = chain_headers(&hub, 3_000);
    eprintln!("\n=== headers apply: first pass vs multi-peer overlap ===");

    // Simulate apply_peer_event Headers first pass (all new).
    let mut known = HashSet::new();
    let mut ordered_set = HashSet::new();
    let mut ordered = VecDeque::new();
    let mut header_fks = HashMap::new();
    let mut hash_height = HashMap::new();

    let apply = |hdrs: &[Header],
                 known: &mut HashSet<BlockHash>,
                 ordered_set: &mut HashSet<BlockHash>,
                 ordered: &mut VecDeque<BlockHash>,
                 header_fks: &mut HashMap<BlockHash, rbitcoin_primitives::Fk>,
                 hash_height: &mut HashMap<BlockHash, u32>,
                 hub: &ChainHub,
                 smart: bool| {
        for (i, hdr) in hdrs.iter().enumerate() {
            let hash = hdr.block_hash();
            if smart && known.contains(&hash) && header_fks.contains_key(&hash) {
                continue;
            }
            if !header_fks.contains_key(&hash) {
                if let Ok(fk) = hub.ensure_header_fk(hdr) {
                    header_fks.insert(hash, fk);
                }
            }
            known.insert(hash);
            hash_height.entry(hash).or_insert(i as u32 + 1);
            if ordered_set.insert(hash) {
                ordered.push_back(hash);
            }
        }
    };

    let batch = &headers[..2_000];
    let t0 = Instant::now();
    apply(
        batch,
        &mut known,
        &mut ordered_set,
        &mut ordered,
        &mut header_fks,
        &mut hash_height,
        &hub,
        false,
    );
    let first = t0.elapsed();

    // 21 peers re-send same batch (naive: always ensure_header_fk).
    let t0 = Instant::now();
    for _ in 0..21 {
        apply(
            batch,
            &mut known,
            &mut ordered_set,
            &mut ordered,
            &mut header_fks,
            &mut hash_height,
            &hub,
            false,
        );
    }
    let naive_overlap = t0.elapsed();

    let t0 = Instant::now();
    for _ in 0..21 {
        apply(
            batch,
            &mut known,
            &mut ordered_set,
            &mut ordered,
            &mut header_fks,
            &mut hash_height,
            &hub,
            true,
        );
    }
    let smart_overlap = t0.elapsed();

    eprintln!("  first pass 2000 new headers:           {first:.2?}");
    eprintln!(
        "  21× re-apply same 2000 (naive store):   {naive_overlap:.2?}  ({:.1} µs/hdr)",
        naive_overlap.as_secs_f64() * 1e6 / (21.0 * 2000.0)
    );
    eprintln!(
        "  21× re-apply same 2000 (smart skip):    {smart_overlap:.2?}  ({:.3} µs/hdr)",
        smart_overlap.as_secs_f64() * 1e6 / (21.0 * 2000.0)
    );
    let ratio = naive_overlap.as_secs_f64() / smart_overlap.as_secs_f64().max(1e-12);
    eprintln!("  naive/smart ratio: {ratio:.2}×  (this is the multi-peer header tax)");
    // Timing is noisy under load; only require both paths finished (no hang).
    // Smart skip should not be *wildly* worse than naive.
    assert!(
        ratio > 0.25,
        "smart path pathologically slower than naive; ratio={ratio:.2}"
    );
}

#[test]
#[ignore = "diagnostic microbench; run: cargo test -p rbitcoin-net freeze_bench -- --ignored --nocapture"]
fn freeze_bench_unbounded_drain_livelock_shape() {
    eprintln!("\n=== drain budget vs livelock shape ===");
    // Model: producer adds events while consumer drains; unbounded drain never
    // returns if production rate ≥ apply rate.
    let mut q: VecDeque<u32> = (0..10_000).collect();
    let mut produced = 10_000u32;
    let apply_us = 50u64; // ~store ensure cost class
    let produce_every = 2u32; // one new event every 2 applies

    // Unbounded: keep draining while any; producer injects during apply.
    let mut applied = 0u64;
    let deadline = Instant::now() + std::time::Duration::from_millis(50);
    while let Some(_) = q.pop_front() {
        applied += 1;
        // simulate apply cost
        let spin_until = Instant::now() + std::time::Duration::from_micros(apply_us);
        while Instant::now() < spin_until {}
        if applied.is_multiple_of(u64::from(produce_every)) {
            q.push_back(produced);
            produced += 1;
        }
        if Instant::now() >= deadline {
            break; // wall stop — would otherwise run forever in real IBD
        }
    }
    let unbounded_left = q.len();
    let unbounded_applied = applied;
    eprintln!(
        "  unbounded (50ms wall): applied={unbounded_applied} left_in_q={unbounded_left}  (never empties under load)"
    );

    // Budgeted: max 512 events then yield (main loop can assign getdata).
    let mut q: VecDeque<u32> = (0..10_000).collect();
    let mut produced = 10_000u32;
    let t0 = Instant::now();
    let mut applied = 0u64;
    const BUDGET: u64 = 512;
    while applied < BUDGET {
        if q.pop_front().is_none() {
            break;
        }
        applied += 1;
        let spin_until = Instant::now() + std::time::Duration::from_micros(apply_us);
        while Instant::now() < spin_until {}
        if applied.is_multiple_of(u64::from(produce_every)) {
            q.push_back(produced);
            produced += 1;
        }
    }
    let budgeted = t0.elapsed();
    eprintln!(
        "  budgeted 512 events: applied={applied} left_in_q={} elapsed={budgeted:.2?}  (returns to assign/status)",
        q.len()
    );
    assert_eq!(applied, BUDGET);
    assert!(
        unbounded_left > 0,
        "livelock model should leave residual queue under continuous production"
    );
}
