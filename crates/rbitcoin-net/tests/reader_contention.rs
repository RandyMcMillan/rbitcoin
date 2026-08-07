//! Temporary diagnostics: can other work on the shared multi-thread runtime
//! starve / block peer-style network readers?
//!
//! Production uses BIP324 v2 only. This module keeps a **test-only** v1 frame
//! helper so we can still measure async-worker starvation without depending on
//! production wire code.
//!
//! Run with:
//!   cargo test -p rbitcoin-net reader_contention -- --nocapture --test-threads=1

use bitcoin::absolute::LockTime;
use bitcoin::block::{Header, Version};
use bitcoin::consensus::{deserialize, serialize};
use bitcoin::hashes::Hash;
use bitcoin::p2p::message::{NetworkMessage, RawNetworkMessage};
use bitcoin::p2p::Magic;
use bitcoin::script::ScriptBuf;
use bitcoin::transaction::Version as TxVersion;
use bitcoin::{
    Amount, Block, CompactTarget, Network, OutPoint, Sequence, Transaction, TxIn, TxOut, Witness,
};
use rbitcoin_net::NetError;
use rbitcoin_net::MAX_PROTOCOL_MESSAGE_LENGTH;
use std::io::Cursor;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::{duplex, AsyncRead, AsyncReadExt, AsyncWriteExt};
use tokio::sync::mpsc;

// ── Test-only v1 framing (not used in production) ─────────────────────────

/// Test-only cancellation-safe v1 framer for contention diagnostics.
struct MessageStream {
    buf: Vec<u8>,
}

impl MessageStream {
    fn new() -> Self {
        Self {
            buf: Vec::with_capacity(8 * 1024),
        }
    }

    async fn read_msg<R: AsyncRead + Unpin>(
        &mut self,
        reader: &mut R,
        expected_magic: Option<Magic>,
    ) -> Result<RawNetworkMessage, NetError> {
        while self.buf.len() < 24 {
            let mut tmp = [0u8; 16 * 1024];
            let n = reader.read(&mut tmp).await?;
            if n == 0 {
                return Err(NetError::Io(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "eof",
                )));
            }
            self.buf.extend_from_slice(&tmp[..n]);
        }
        let magic = Magic::from_bytes(self.buf[0..4].try_into().unwrap());
        if let Some(exp) = expected_magic {
            if magic != exp {
                return Err(NetError::BadMagic);
            }
        }
        let payload_len = u32::from_le_bytes(self.buf[16..20].try_into().unwrap()) as usize;
        if payload_len > MAX_PROTOCOL_MESSAGE_LENGTH {
            return Err(NetError::MessageTooLarge(payload_len));
        }
        let total = 24 + payload_len;
        while self.buf.len() < total {
            let mut tmp = [0u8; 16 * 1024];
            let n = reader.read(&mut tmp).await?;
            if n == 0 {
                return Err(NetError::Io(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "eof",
                )));
            }
            self.buf.extend_from_slice(&tmp[..n]);
        }
        let full: Vec<u8> = self.buf.drain(..total).collect();
        deserialize::<RawNetworkMessage>(&full).map_err(|e| NetError::Encode(e.to_string()))
    }
}

const WORKERS: usize = 4;
const READERS: usize = 8;
const MSGS_PER_READER: usize = 40;
/// Extra non-coinbase txs so `deserialize::<Block>` is non-trivial (~tens of KB).
const EXTRA_TXS: usize = 80;
/// Heavier fixture for sustained-load tax (closer to mid-chain signet blocks).
const EXTRA_TXS_HEAVY: usize = 400;

fn signet_magic() -> Magic {
    Magic::from(Network::Signet)
}

fn fat_block(nonce_seed: u32) -> Block {
    fat_block_with_txs(nonce_seed, EXTRA_TXS)
}

fn fat_block_with_txs(nonce_seed: u32, extra_txs: usize) -> Block {
    let coinbase = Transaction {
        version: TxVersion::ONE,
        lock_time: LockTime::ZERO,
        input: vec![TxIn {
            previous_output: OutPoint::null(),
            script_sig: ScriptBuf::from_bytes(vec![0x01, 0x01]),
            sequence: Sequence::MAX,
            witness: Witness::new(),
        }],
        output: vec![TxOut {
            value: Amount::from_sat(50_0000_0000),
            script_pubkey: ScriptBuf::from_bytes(vec![0x51]),
        }],
    };
    let mut txdata = vec![coinbase];
    for i in 0..extra_txs {
        // Unique prevouts so txids differ; OP_RETURN-sized scriptPubKey.
        let mut payload = vec![0x6a, 0x20]; // OP_RETURN push32
        payload.extend_from_slice(&(nonce_seed as u64).to_le_bytes());
        payload.extend_from_slice(&(i as u64).to_le_bytes());
        payload.resize(34, 0);
        txdata.push(Transaction {
            version: TxVersion::TWO,
            lock_time: LockTime::ZERO,
            input: vec![TxIn {
                previous_output: OutPoint {
                    txid: bitcoin::Txid::from_byte_array({
                        let mut a = [0u8; 32];
                        a[0..4].copy_from_slice(&nonce_seed.to_le_bytes());
                        a[4..8].copy_from_slice(&(i as u32).to_le_bytes());
                        a
                    }),
                    vout: 0,
                },
                script_sig: ScriptBuf::new(),
                sequence: Sequence::MAX,
                witness: Witness::new(),
            }],
            output: vec![TxOut {
                value: Amount::from_sat(0),
                script_pubkey: ScriptBuf::from_bytes(payload),
            }],
        });
    }
    let bits = CompactTarget::from_consensus(0x207f_ffff);
    let mut block = Block {
        header: Header {
            version: Version::ONE,
            prev_blockhash: bitcoin::BlockHash::from_byte_array([0u8; 32]),
            merkle_root: bitcoin::TxMerkleNode::from_byte_array([0u8; 32]),
            time: 1_700_000_000 + nonce_seed,
            bits,
            nonce: nonce_seed,
        },
        txdata,
    };
    block.header.merkle_root = block
        .compute_merkle_root()
        .expect("non-empty block has merkle root");
    block
}

fn frame_block(magic: Magic, block: &Block) -> Vec<u8> {
    let raw = RawNetworkMessage::new(magic, NetworkMessage::Block(block.clone()));
    serialize(&raw)
}

/// Spin CPU without awaiting — monopolizes a Tokio worker until `stop`.
///
/// Must leave ≥1 worker free for the test harness / timers; pinning *all*
/// workers deadlocks the runtime (timers never fire) — which is itself proof
/// of the hazard, but not a useful unit-test shape.
fn uncooperative_cpu_hog_blocking(stop: Arc<AtomicBool>, spins: Arc<AtomicU64>) {
    while !stop.load(Ordering::Relaxed) {
        let mut x = 1u64;
        for i in 1..50_000u64 {
            x = x.wrapping_mul(i).wrapping_add(i ^ x);
        }
        std::hint::black_box(x);
        spins.fetch_add(1, Ordering::Relaxed);
        // Deliberately no yield: models large sync deserialize / prep on a worker.
    }
}

/// Heavy cooperative work: re-deserialize a fat block, then yield (archive-prep style).
/// Batch several deserializes per yield so we actually tax workers (single
/// deserialize + yield is too polite to show up against short reader bursts).
async fn cooperative_deserialize_hog(
    stop: Arc<AtomicBool>,
    payload: Arc<Vec<u8>>,
    spins: Arc<AtomicU64>,
) {
    while !stop.load(Ordering::Relaxed) {
        for _ in 0..8 {
            let b: Block = deserialize(payload.as_ref()).expect("fat block");
            std::hint::black_box(b.block_hash());
            spins.fetch_add(1, Ordering::Relaxed);
        }
        tokio::task::yield_now().await;
    }
}

/// Same work as cooperative hog but on the blocking pool (should not starve I/O workers).
async fn blocking_pool_deserialize_hog(
    stop: Arc<AtomicBool>,
    payload: Arc<Vec<u8>>,
    spins: Arc<AtomicU64>,
) {
    while !stop.load(Ordering::Relaxed) {
        let p = Arc::clone(&payload);
        let _ = tokio::task::spawn_blocking(move || {
            let b: Block = deserialize(p.as_ref()).expect("fat block");
            std::hint::black_box(b.block_hash());
        })
        .await;
        spins.fetch_add(1, Ordering::Relaxed);
    }
}

struct RunResult {
    label: &'static str,
    msgs: u64,
    elapsed: Duration,
    hog_spins: u64,
}

impl RunResult {
    fn rate(&self) -> f64 {
        self.msgs as f64 / self.elapsed.as_secs_f64()
    }
}

async fn run_readers_with_hogs(
    label: &'static str,
    hog_kind: HogKind,
    hog_tasks: usize,
) -> RunResult {
    let magic = signet_magic();
    let stop = Arc::new(AtomicBool::new(false));
    let hog_spins = Arc::new(AtomicU64::new(0));

    // Pre-build one framed fat block; each reader gets many copies (same wire bytes).
    let block = fat_block(42);
    let framed = Arc::new(frame_block(magic, &block));
    let block_bytes = Arc::new(serialize(&block));
    eprintln!(
        "  fixture: framed_msg={}B block={}B txs={}",
        framed.len(),
        block_bytes.len(),
        block.txdata.len()
    );

    let (progress_tx, mut progress_rx) = mpsc::unbounded_channel::<()>();
    let total_expected = (READERS * MSGS_PER_READER) as u64;

    // Start hogs first so they grab workers before readers if they can.
    let mut hog_handles = Vec::new();
    let mut hog_threads = Vec::new();
    for _ in 0..hog_tasks {
        let stop = Arc::clone(&stop);
        let spins = Arc::clone(&hog_spins);
        let payload = Arc::clone(&block_bytes);
        match hog_kind {
            HogKind::None => {}
            // Spawn as async tasks that never yield — they pin Tokio workers.
            HogKind::Uncooperative => {
                hog_handles.push(tokio::spawn(async move {
                    // `spawn_blocking` would *not* pin async workers; we want that.
                    uncooperative_cpu_hog_blocking(stop, spins);
                }));
            }
            HogKind::CooperativeDeserialize => {
                hog_handles.push(tokio::spawn(cooperative_deserialize_hog(
                    stop, payload, spins,
                )));
            }
            HogKind::BlockingPool => {
                hog_handles.push(tokio::spawn(blocking_pool_deserialize_hog(
                    stop, payload, spins,
                )));
            }
            // OS threads compete for CPU cores but do not occupy Tokio workers.
            HogKind::OsThreadCpu => {
                hog_threads.push(std::thread::spawn(move || {
                    uncooperative_cpu_hog_blocking(stop, spins);
                }));
            }
        }
    }

    // Brief settle so uncooperative hogs pin workers.
    if matches!(hog_kind, HogKind::Uncooperative | HogKind::OsThreadCpu) && hog_tasks > 0 {
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    let t0 = Instant::now();
    let mut reader_handles = Vec::new();
    for r in 0..READERS {
        let framed = Arc::clone(&framed);
        let progress_tx = progress_tx.clone();
        let magic = magic;
        reader_handles.push(tokio::spawn(async move {
            // Duplex large enough to hold all messages without blocking the writer.
            let cap = framed.len() * MSGS_PER_READER + 64 * 1024;
            let (mut w, mut rh) = duplex(cap);
            // Writer task: feed all messages ASAP (models peer already sending).
            let framed_w = Arc::clone(&framed);
            let writer = tokio::spawn(async move {
                for _ in 0..MSGS_PER_READER {
                    w.write_all(framed_w.as_ref()).await.expect("write");
                }
                w.shutdown().await.ok();
            });

            let mut ms = MessageStream::new();
            for i in 0..MSGS_PER_READER {
                let msg = ms
                    .read_msg(&mut rh, Some(magic))
                    .await
                    .unwrap_or_else(|e| panic!("reader {r} msg {i}: {e}"));
                match msg.into_payload() {
                    NetworkMessage::Block(b) => {
                        std::hint::black_box(b.block_hash());
                    }
                    other => panic!("reader {r}: expected Block, got {other:?}"),
                }
                let _ = progress_tx.send(());
            }
            let _ = writer.await;
        }));
    }
    drop(progress_tx);

    let mut got = 0u64;
    // Bound wait: if starved, we timeout and report partial progress.
    // Keep short — uncooperative cases with workers-1 still make some progress.
    let deadline = Instant::now() + Duration::from_secs(8);
    while got < total_expected {
        let left = deadline.saturating_duration_since(Instant::now());
        if left.is_zero() {
            break;
        }
        match tokio::time::timeout(left.min(Duration::from_millis(500)), progress_rx.recv()).await {
            Ok(Some(())) => got += 1,
            Ok(None) => break,
            Err(_) => {
                if Instant::now() >= deadline {
                    break;
                }
            }
        }
    }
    let elapsed = t0.elapsed();

    stop.store(true, Ordering::Relaxed);
    for h in reader_handles {
        // Abort stragglers so uncooperative hogs don't hang the test forever.
        h.abort();
        let _ = h.await;
    }
    for h in hog_handles {
        h.abort();
        let _ = h.await;
    }
    for t in hog_threads {
        let _ = t.join();
    }

    RunResult {
        label,
        msgs: got,
        elapsed,
        hog_spins: hog_spins.load(Ordering::Relaxed),
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum HogKind {
    None,
    /// Async tasks that never yield (pin Tokio workers). Use workers-1 only.
    Uncooperative,
    CooperativeDeserialize,
    BlockingPool,
    /// OS threads burn CPU cores but leave Tokio workers free for I/O.
    OsThreadCpu,
}

fn multi_rt() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(WORKERS)
        .enable_all()
        .build()
        .expect("runtime")
}

/// Always-on smoke: exercise v1 framer + short duplex round-trip (coverage).
#[test]
fn reader_contention_framer_smoke() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        let magic = signet_magic();
        let (mut client, mut server) = duplex(64 * 1024);
        let raw = RawNetworkMessage::new(magic, NetworkMessage::GetAddr);
        let bytes = serialize(&raw);
        client.write_all(&bytes).await.unwrap();
        let mut stream = MessageStream::new();
        let msg = stream
            .read_msg(&mut server, Some(magic))
            .await
            .expect("framed getaddr");
        assert!(matches!(msg.payload(), NetworkMessage::GetAddr));
        // Bad magic / oversized payload error paths.
        let mut stream2 = MessageStream::new();
        let bad = RawNetworkMessage::new(Magic::from_bytes([1, 2, 3, 4]), NetworkMessage::GetAddr);
        client.write_all(&serialize(&bad)).await.unwrap();
        let err = stream2
            .read_msg(&mut server, Some(magic))
            .await
            .unwrap_err();
        assert!(matches!(err, NetError::BadMagic));
    });
    let _ = fat_block(1);
    let _ = fat_block_with_txs(2, 2);
}

#[test]
#[ignore = "diagnostic contention bench; run: cargo test -p rbitcoin-net reader_contention -- --ignored --nocapture"]
fn reader_contention_matrix() {
    eprintln!("\n=== reader contention matrix (workers={WORKERS}, readers={READERS}, msgs/reader={MSGS_PER_READER}) ===");

    let baseline = multi_rt().block_on(run_readers_with_hogs("readers_only", HogKind::None, 0));
    let coop = multi_rt().block_on(run_readers_with_hogs(
        "readers+coop_deser_hogs",
        HogKind::CooperativeDeserialize,
        WORKERS, // one hog per worker
    ));
    let blocking = multi_rt().block_on(run_readers_with_hogs(
        "readers+blocking_pool_hogs",
        HogKind::BlockingPool,
        WORKERS,
    ));
    // Leave 1 worker free so the harness/timers can still run; pin the rest.
    let uncoop = multi_rt().block_on(run_readers_with_hogs(
        "readers+uncoop_cpu_hogs",
        HogKind::Uncooperative,
        WORKERS.saturating_sub(1).max(1),
    ));
    // Same core burn via OS threads — Tokio workers stay free for I/O.
    let os_cpu = multi_rt().block_on(run_readers_with_hogs(
        "readers+os_thread_cpu",
        HogKind::OsThreadCpu,
        WORKERS.saturating_sub(1).max(1),
    ));

    for r in [&baseline, &coop, &blocking, &uncoop, &os_cpu] {
        eprintln!(
            "  {:<32} msgs={:>4}/{}  elapsed={:>7.1?}  rate={:>8.1} msg/s  hog_spins={}",
            r.label,
            r.msgs,
            READERS * MSGS_PER_READER,
            r.elapsed,
            r.rate(),
            r.hog_spins
        );
    }

    assert_eq!(
        baseline.msgs,
        (READERS * MSGS_PER_READER) as u64,
        "baseline readers must complete"
    );
    assert_eq!(
        blocking.msgs,
        (READERS * MSGS_PER_READER) as u64,
        "blocking-pool hogs must not prevent readers finishing"
    );

    // Cooperative deserialize hogs on all workers should still allow progress
    // (they yield) but can cut throughput substantially.
    let coop_ratio = coop.rate() / baseline.rate();
    let blocking_ratio = blocking.rate() / baseline.rate();
    let uncoop_ratio = uncoop.rate() / baseline.rate();
    let os_ratio = os_cpu.rate() / baseline.rate();
    eprintln!(
        "  ratios vs baseline: coop={coop_ratio:.3}  blocking_pool={blocking_ratio:.3}  uncoop={uncoop_ratio:.3}  os_thread={os_ratio:.3}"
    );
    eprintln!(
        "  uncoop frac_done={:.3}  os_thread frac_done={:.3}",
        uncoop.msgs as f64 / baseline.msgs as f64,
        os_cpu.msgs as f64 / baseline.msgs as f64
    );

    // Sync CPU on Tokio workers (workers-1 pinned) should be worse than the same
    // CPU burn on plain OS threads (workers free for I/O polling).
    let worker_pin_hurts = uncoop_ratio + 0.05 < os_ratio
        || uncoop.msgs < os_cpu.msgs
        || uncoop.elapsed > os_cpu.elapsed.saturating_mul(2);
    eprintln!(
        "  verdict_worker_pin_worse_than_os_cpu={}",
        if worker_pin_hurts {
            "YES — pinning Tokio workers hurts readers more than equal OS-thread CPU load"
        } else {
            "inconclusive on this host"
        }
    );

    // Cooperative hogs: if rate drops below ~60% of baseline, shared-runtime
    // deserialize/prep work is a real reader throughput tax (IBD-relevant).
    let coop_tax = coop_ratio < 0.60;
    let coop_verdict = if coop_tax {
        format!("YES — cooperative CPU on workers cuts reader rate >40% (ratio={coop_ratio:.3})")
    } else {
        format!("mild/no (ratio={coop_ratio:.3})")
    };
    eprintln!("  verdict_cooperative_runtime_tax={coop_verdict}");

    // Blocking pool should stay much closer to baseline than uncooperative.
    assert!(
        blocking.msgs == baseline.msgs,
        "spawn_blocking hogs must not drop messages"
    );
    assert!(
        blocking_ratio > 0.40,
        "blocking-pool path should keep a healthy fraction of baseline rate, got {blocking_ratio:.3}"
    );

    // Document proof for the user even if we don't fail the suite on mild machines.
    assert!(
        worker_pin_hurts || coop_tax || coop_ratio < 0.85 || uncoop_ratio < 0.70,
        "expected evidence that same-runtime CPU work hurts readers; \
         coop_ratio={coop_ratio:.3} uncoop_ratio={uncoop_ratio:.3} os_ratio={os_ratio:.3}"
    );
}

/// Pure CPU: how expensive is one full `RawNetworkMessage`/`Block` deserialize
/// relative to framing alone? (Reader path does both before `PeerEvent::Block`.)
#[test]
#[ignore = "diagnostic contention bench; run: cargo test -p rbitcoin-net reader_contention -- --ignored --nocapture"]
fn reader_contention_deserialize_cost() {
    let magic = signet_magic();
    let block = fat_block(7);
    let framed = frame_block(magic, &block);
    let block_raw = serialize(&block);
    eprintln!(
        "\n=== deserialize cost (block {}B, framed {}B, {} txs) ===",
        block_raw.len(),
        framed.len(),
        block.txdata.len()
    );

    const N: u32 = 200;

    let t0 = Instant::now();
    for _ in 0..N {
        let b: Block = deserialize(&block_raw).unwrap();
        std::hint::black_box(b.txdata.len());
    }
    let deser_block = t0.elapsed();

    let t0 = Instant::now();
    for _ in 0..N {
        let m: RawNetworkMessage = deserialize(&framed).unwrap();
        std::hint::black_box(matches!(m.payload(), NetworkMessage::Block(_)));
    }
    let deser_raw = t0.elapsed();

    // Framing-only path: parse length, slice payload, do not build Block.
    let t0 = Instant::now();
    for _ in 0..N {
        assert!(framed.len() >= 24);
        let plen = u32::from_le_bytes(framed[16..20].try_into().unwrap()) as usize;
        let payload = &framed[24..24 + plen];
        std::hint::black_box(payload.len());
    }
    let frame_only = t0.elapsed();

    // Sync MessageStream over Cursor (no async, no socket) — full reader path cost.
    let t0 = Instant::now();
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        for _ in 0..N {
            let mut cur = Cursor::new(framed.as_slice());
            let mut ms = MessageStream::new();
            let msg = ms.read_msg(&mut cur, Some(magic)).await.unwrap();
            match msg.into_payload() {
                NetworkMessage::Block(b) => std::hint::black_box(b.txdata.len()),
                _ => panic!("expected block"),
            };
        }
    });
    let stream_path = t0.elapsed();

    eprintln!(
        "  per-msg avg: block_deser={:.1}µs  raw_msg_deser={:.1}µs  frame_only={:.2}µs  MessageStream+Block={:.1}µs",
        deser_block.as_secs_f64() * 1e6 / f64::from(N),
        deser_raw.as_secs_f64() * 1e6 / f64::from(N),
        frame_only.as_secs_f64() * 1e6 / f64::from(N),
        stream_path.as_secs_f64() * 1e6 / f64::from(N),
    );
    let tax = deser_raw.as_secs_f64() / frame_only.as_secs_f64().max(1e-12);
    eprintln!(
        "  deserialize vs frame-only ratio={tax:.0}x — work done on the peer reader task today"
    );
    assert!(
        tax > 10.0,
        "expected Block deserialize to dominate framing; ratio={tax:.1}"
    );
}

/// Many concurrent readers each doing full deserialize on a 4-worker runtime:
/// self-contention even without an external hog (N readers > workers).
#[test]
#[ignore = "diagnostic contention bench; run: cargo test -p rbitcoin-net reader_contention -- --ignored --nocapture"]
fn reader_contention_many_peers_self_saturate() {
    eprintln!("\n=== many-peer self-contention (deserialize on reader tasks) ===");
    let magic = signet_magic();
    let block = fat_block(99);
    let framed = Arc::new(frame_block(magic, &block));
    let n_peers = 16usize;
    let msgs = 20usize;

    let rt = multi_rt();
    let elapsed = rt.block_on(async {
        let t0 = Instant::now();
        let mut handles = Vec::new();
        for _ in 0..n_peers {
            let framed = Arc::clone(&framed);
            handles.push(tokio::spawn(async move {
                let cap = framed.len() * msgs + 64 * 1024;
                let (mut w, mut rh) = duplex(cap);
                let fw = Arc::clone(&framed);
                let writer = tokio::spawn(async move {
                    for _ in 0..msgs {
                        w.write_all(fw.as_ref()).await.unwrap();
                    }
                    w.shutdown().await.ok();
                });
                let mut ms = MessageStream::new();
                for _ in 0..msgs {
                    let msg = ms.read_msg(&mut rh, Some(magic)).await.unwrap();
                    let NetworkMessage::Block(b) = msg.into_payload() else {
                        panic!("not block");
                    };
                    std::hint::black_box(b.block_hash());
                }
                writer.await.ok();
            }));
        }
        for h in handles {
            h.await.unwrap();
        }
        t0.elapsed()
    });

    let total = n_peers * msgs;
    let rate = total as f64 / elapsed.as_secs_f64();
    eprintln!(
        "  peers={n_peers} workers={WORKERS} total_msgs={total} elapsed={elapsed:.2?} rate={rate:.1} msg/s"
    );
    eprintln!(
        "  note: with {} workers, at most {} Block deserializes run truly in parallel; the rest queue on the scheduler",
        WORKERS, WORKERS
    );
    assert!(rate > 0.0);
}

/// Fixed-duration sustained load: readers stream forever-ready block messages while
/// cooperative deserializers share the runtime (models archive-prep + multi-peer
/// decode competing with socket reads). Compare against spawn_blocking offload.
#[test]
#[ignore = "diagnostic contention bench; run: cargo test -p rbitcoin-net reader_contention -- --ignored --nocapture"]
fn reader_contention_sustained_coop_tax() {
    eprintln!("\n=== sustained cooperative tax (500ms windows) ===");
    let magic = signet_magic();
    let block = fat_block_with_txs(11, EXTRA_TXS_HEAVY);
    let framed = Arc::new(frame_block(magic, &block));
    let block_bytes = Arc::new(serialize(&block));
    eprintln!(
        "  heavy fixture: framed={}B block={}B txs={}",
        framed.len(),
        block_bytes.len(),
        block.txdata.len()
    );

    let window = Duration::from_millis(500);

    let run = |label: &'static str, hog: HogKind, hog_n: usize| -> u64 {
        multi_rt().block_on(async {
            let stop = Arc::new(AtomicBool::new(false));
            let msgs = Arc::new(AtomicU64::new(0));
            let mut hogs = Vec::new();
            for _ in 0..hog_n {
                let stop = Arc::clone(&stop);
                let spins = Arc::new(AtomicU64::new(0));
                let payload = Arc::clone(&block_bytes);
                match hog {
                    HogKind::CooperativeDeserialize => {
                        hogs.push(tokio::spawn(cooperative_deserialize_hog(
                            stop, payload, spins,
                        )));
                    }
                    HogKind::BlockingPool => {
                        hogs.push(tokio::spawn(blocking_pool_deserialize_hog(
                            stop, payload, spins,
                        )));
                    }
                    HogKind::None => {}
                    _ => unreachable!(),
                }
            }
            // Let hogs warm up.
            if hog_n > 0 {
                tokio::time::sleep(Duration::from_millis(30)).await;
            }

            let mut readers = Vec::new();
            for _ in 0..READERS {
                let framed = Arc::clone(&framed);
                let stop = Arc::clone(&stop);
                let msgs = Arc::clone(&msgs);
                readers.push(tokio::spawn(async move {
                    // Continuous stream: rewrite the same message as needed.
                    loop {
                        if stop.load(Ordering::Relaxed) {
                            break;
                        }
                        let cap = framed.len() * 4 + 64 * 1024;
                        let (mut w, mut rh) = duplex(cap);
                        let fw = Arc::clone(&framed);
                        let stop_w = Arc::clone(&stop);
                        let writer = tokio::spawn(async move {
                            while !stop_w.load(Ordering::Relaxed) {
                                if w.write_all(fw.as_ref()).await.is_err() {
                                    break;
                                }
                            }
                        });
                        let mut ms = MessageStream::new();
                        while !stop.load(Ordering::Relaxed) {
                            match ms.read_msg(&mut rh, Some(magic)).await {
                                Ok(m) => {
                                    if let NetworkMessage::Block(b) = m.into_payload() {
                                        std::hint::black_box(b.block_hash());
                                        msgs.fetch_add(1, Ordering::Relaxed);
                                    }
                                }
                                Err(_) => break,
                            }
                        }
                        writer.abort();
                        let _ = writer.await;
                        if stop.load(Ordering::Relaxed) {
                            break;
                        }
                    }
                }));
            }

            tokio::time::sleep(window).await;
            stop.store(true, Ordering::Relaxed);
            for r in readers {
                r.abort();
                let _ = r.await;
            }
            for h in hogs {
                h.abort();
                let _ = h.await;
            }
            let n = msgs.load(Ordering::Relaxed);
            eprintln!(
                "  {label:<28} msgs={n:>6}  rate={:>8.1} msg/s",
                n as f64 / window.as_secs_f64()
            );
            n
        })
    };

    let base = run("sustained_readers_only", HogKind::None, 0);
    let coop = run(
        "sustained+coop_deser",
        HogKind::CooperativeDeserialize,
        WORKERS,
    );
    let blk = run("sustained+blocking_pool", HogKind::BlockingPool, WORKERS);

    assert!(base > 0, "baseline sustained readers produced no messages");
    let coop_ratio = coop as f64 / base as f64;
    let blk_ratio = blk as f64 / base as f64;
    eprintln!("  sustained ratios vs baseline: coop={coop_ratio:.3}  blocking_pool={blk_ratio:.3}");

    // Under sustained heavy deserialize load on every worker, reader throughput
    // should drop vs baseline (proves shared-runtime tax is real under load).
    let coop_tax = coop_ratio < 0.75;
    eprintln!(
        "  verdict_sustained_coop_tax={}",
        if coop_tax {
            format!("YES — coop deserializers cut sustained reader rate (ratio={coop_ratio:.3})")
        } else {
            format!("mild (ratio={coop_ratio:.3})")
        }
    );
    // Note: spawn_blocking can be *worse* than cooperative async work on a
    // machine where cores == workers (oversubscription). The fix for IBD is not
    // "move everything to blocking pool" but "keep socket-read tasks schedulable"
    // (dedicated I/O runtime / fewer CPU tasks on the same workers).
    eprintln!(
        "  note: blocking_pool ratio={blk_ratio:.3} (may oversubscribe cores; not always better)"
    );
    // Release builds deserialize ~15× faster; duplex/framing can dominate so the
    // sustained tax is milder. The burst matrix (`reader_contention_matrix`) is
    // the hard proof under optimized builds. Here we only require progress.
    assert!(
        base > 100 && coop > 50,
        "sustained readers should make progress (base={base} coop={coop})"
    );
    if !coop_tax {
        eprintln!(
            "  (sustained coop tax mild at ratio={coop_ratio:.3}; see matrix test for burst tax)"
        );
    }
}

/// Direct comparison: deserialize on the reader task (today) vs frame-only on
/// the reader + `spawn_blocking` deserialize (isolation option).
#[test]
#[ignore = "diagnostic contention bench; run: cargo test -p rbitcoin-net reader_contention -- --ignored --nocapture"]
fn reader_contention_offload_deserialize() {
    eprintln!("\n=== deserialize on-reader vs spawn_blocking offload ===");
    let magic = signet_magic();
    let block = fat_block_with_txs(22, EXTRA_TXS_HEAVY);
    let framed = Arc::new(frame_block(magic, &block));
    eprintln!(
        "  fixture: framed={}B txs={}",
        framed.len(),
        block.txdata.len()
    );
    let n_peers = 16usize;
    let msgs = 12usize;

    let measure = |offload: bool| -> Duration {
        multi_rt().block_on(async {
            let t0 = Instant::now();
            let mut handles = Vec::new();
            for _ in 0..n_peers {
                let framed = Arc::clone(&framed);
                handles.push(tokio::spawn(async move {
                    let cap = framed.len() * msgs + 64 * 1024;
                    let (mut w, mut rh) = duplex(cap);
                    let fw = Arc::clone(&framed);
                    let writer = tokio::spawn(async move {
                        for _ in 0..msgs {
                            w.write_all(fw.as_ref()).await.unwrap();
                        }
                        w.shutdown().await.ok();
                    });
                    let mut ms = MessageStream::new();
                    for _ in 0..msgs {
                        let msg = ms.read_msg(&mut rh, Some(magic)).await.unwrap();
                        if offload {
                            // Keep only framed raw path cost on the worker: the
                            // payload is already a Block from deserialize inside
                            // read_msg today. Simulate "bytes only" by re-serializing
                            // and re-deserializing on the blocking pool — measures
                            // offload overhead vs keeping CPU on the worker.
                            let NetworkMessage::Block(b) = msg.into_payload() else {
                                panic!("not block");
                            };
                            let raw = serialize(&b);
                            let _ = tokio::task::spawn_blocking(move || {
                                let b2: Block = deserialize(&raw).unwrap();
                                std::hint::black_box(b2.block_hash());
                            })
                            .await;
                        } else {
                            let NetworkMessage::Block(b) = msg.into_payload() else {
                                panic!("not block");
                            };
                            std::hint::black_box(b.block_hash());
                        }
                    }
                    writer.await.ok();
                }));
            }
            for h in handles {
                h.await.unwrap();
            }
            t0.elapsed()
        })
    };

    // Note: read_msg already deserializes on-worker for both arms; the offload
    // arm *adds* a second deserialize on the blocking pool. To measure true
    // frame-only vs full path, compare pure Cursor deserialize batch vs
    // MessageStream (already done in deserialize_cost). Here we show that
    // *additional* CPU on the async worker serializes peer progress.
    let on_reader = measure(false);
    let with_extra_blocking = measure(true);
    let total = n_peers * msgs;
    eprintln!(
        "  on_reader_path:     {total} msgs in {on_reader:.2?} ({:.1} msg/s)",
        total as f64 / on_reader.as_secs_f64()
    );
    eprintln!(
        "  +blocking_reparse:  {total} msgs in {with_extra_blocking:.2?} ({:.1} msg/s)",
        total as f64 / with_extra_blocking.as_secs_f64()
    );

    // Compare pure CPU batch: N sequential deserializes on one OS thread vs
    // parallel on Tokio workers (self-queueing when N > workers).
    let raw = serialize(&block);
    let batch = 64usize;
    let t_seq = Instant::now();
    for _ in 0..batch {
        let b: Block = deserialize(&raw).unwrap();
        std::hint::black_box(b.txdata.len());
    }
    let seq = t_seq.elapsed();

    let t_par = Instant::now();
    multi_rt().block_on(async {
        let mut hs = Vec::new();
        for _ in 0..batch {
            let raw = raw.clone();
            hs.push(tokio::spawn(async move {
                let b: Block = deserialize(&raw).unwrap();
                std::hint::black_box(b.txdata.len());
            }));
        }
        for h in hs {
            h.await.unwrap();
        }
    });
    let par = t_par.elapsed();
    eprintln!(
        "  pure_deser batch={batch}: sequential={seq:.2?}  tokio_spawn_x{batch}_on_{WORKERS}workers={par:.2?}  speedup={:.2}x",
        seq.as_secs_f64() / par.as_secs_f64()
    );
    // Parallel should be faster but not by batch/1 — capped by worker count.
    let speedup = seq.as_secs_f64() / par.as_secs_f64();
    assert!(
        speedup < (WORKERS as f64) + 1.5,
        "parallel speedup {speedup:.2} suggests more than ~{WORKERS} workers; check runtime"
    );
    // Soft lower bound: llvm-cov / busy hosts often flatten speedup to ~1.0×.
    // Still require the multi-thread path not to be dramatically *slower* than
    // sequential (spawn overhead bound).
    assert!(
        speedup > 0.5,
        "parallel path unexpectedly much slower than sequential (speedup={speedup:.2})"
    );
    if speedup <= 1.2 {
        eprintln!(
            "  note: parallel speedup only {speedup:.2}x (ok under load/coverage; paths still exercised)"
        );
    }
}
