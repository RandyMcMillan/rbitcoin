//! Concurrent multi-peer block download (libbitcoin-class windowed IBD).
//!
//! Architecture:
//! - N outbound peer workers (each: own TCP stream, command + event channels)
//! - Shared ordered work queue of block hashes after the local tip
//! - Download **window**: up to `window` blocks in-flight, at most `per_peer` per peer
//! - Blocks may arrive out of order; connect when parent is tip
//! - Stall timeout reassigns in-flight hashes to other peers

use crate::chain::{AcceptOutcome, ChainHub};
use crate::codec::{read_msg, write_msg};
use crate::error::NetError;
use crate::peer::handshake;
use bitcoin::block::Header;
use bitcoin::hashes::Hash;
use bitcoin::p2p::message::NetworkMessage;
use bitcoin::p2p::message_blockdata::{GetHeadersMessage, Inventory};
use bitcoin::p2p::Magic;
use bitcoin::{Block, BlockHash};
use std::collections::{HashMap, HashSet, VecDeque};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

/// Tunables for parallel IBD (defaults lean libbitcoin/Core-ish).
#[derive(Clone, Debug)]
pub struct IbdConfig {
    /// Max blocks in-flight across all peers.
    pub window: usize,
    /// Max in-flight getdata per peer.
    pub per_peer: usize,
    /// Reassign getdata if no progress for this long.
    pub stall: Duration,
    /// Max headers to request per getheaders round-trip.
    pub headers_batch: usize,
}

impl Default for IbdConfig {
    fn default() -> Self {
        Self {
            window: 1024,
            per_peer: 16,
            stall: Duration::from_secs(30),
            headers_batch: 2000,
        }
    }
}

impl IbdConfig {
    /// Smaller window for tests.
    pub fn for_test() -> Self {
        Self {
            window: 32,
            per_peer: 8,
            stall: Duration::from_secs(10),
            headers_batch: 2000,
        }
    }
}

enum PeerCmd {
    GetHeaders { locator: Vec<BlockHash> },
    GetData { hashes: Vec<BlockHash> },
    Shutdown,
}

enum PeerEvent {
    Headers { peer: usize, headers: Vec<Header> },
    Block { peer: usize, block: Block },
    /// Peer failed or closed.
    Dead { peer: usize, reason: String },
}

struct PeerSlot {
    id: usize,
    cmd_tx: mpsc::UnboundedSender<PeerCmd>,
    /// Hashes currently requested from this peer.
    in_flight: HashSet<BlockHash>,
    /// When we last received a useful message.
    #[allow(dead_code)]
    last_activity: Instant,
    alive: bool,
    task: JoinHandle<()>,
}

/// Run parallel IBD against `peers` until no more headers/blocks, or all peers die.
///
/// Returns approximate number of blocks accepted this run.
pub async fn parallel_ibd(
    hub: Arc<ChainHub>,
    magic: Magic,
    local_addr: SocketAddr,
    peers: &[SocketAddr],
    cfg: IbdConfig,
) -> Result<u32, NetError> {
    if peers.is_empty() {
        return Err(NetError::Protocol("no peers for parallel ibd"));
    }

    let (ev_tx, mut ev_rx) = mpsc::unbounded_channel::<PeerEvent>();
    let mut slots: Vec<PeerSlot> = Vec::new();

    for (id, &addr) in peers.iter().enumerate() {
        match spawn_peer(id, addr, magic, local_addr, hub.tip_height(), ev_tx.clone()).await {
            Ok(slot) => {
                eprintln!("ibd: parallel peer[{id}] connected {addr}");
                slots.push(slot);
            }
            Err(e) => {
                eprintln!("ibd: parallel peer[{id}] {addr} failed: {e}");
            }
        }
    }
    if slots.is_empty() {
        return Err(NetError::Protocol("no parallel peers connected"));
    }

    // Ordered hashes we still need (height not stored; chain order from headers).
    let mut want: VecDeque<BlockHash> = VecDeque::new();
    // hash → when requested + peer id
    let mut inflight: HashMap<BlockHash, (usize, Instant)> = HashMap::new();
    // Received but not yet connected
    let mut pool: HashMap<BlockHash, Block> = HashMap::new();
    // Known header hashes on the download path (for linkage)
    let mut known_headers: HashSet<BlockHash> = HashSet::new();
    if let Some(h) = hub.tip_hash() {
        known_headers.insert(h);
    }

    let accepted = AtomicU32::new(0);
    let start_tip = hub.tip_height().unwrap_or(0);
    let mut headers_done = false;
    let mut last_progress = Instant::now();
    // Consecutive empty/useless header replies across peers.
    let mut empty_header_streak = 0u32;
    let mut header_req_seq = 0u32;

    // Kick header sync on first alive peer.
    request_headers(&slots, &hub, &mut header_req_seq)?;

    loop {
        // Assign work to free peer slots.
        assign_work(&mut slots, &mut want, &mut inflight, &cfg);

        // Stall reassignment
        let now = Instant::now();
        reassign_stalled(&mut slots, &mut inflight, &mut want, &cfg, now);

        // Exit if nothing left and no inflight
        if headers_done && want.is_empty() && inflight.is_empty() {
            break;
        }
        // Idle with nothing to do
        if want.is_empty()
            && inflight.is_empty()
            && pool.is_empty()
            && last_progress.elapsed() > Duration::from_secs(5)
            && empty_header_streak >= 2
        {
            headers_done = true;
            break;
        }
        // All peers dead
        if slots.iter().all(|s| !s.alive) {
            if accepted.load(Ordering::SeqCst) > 0 {
                break;
            }
            return Err(NetError::Protocol("all parallel peers dead"));
        }

        // Wait for events with timeout so we can reassign stalls.
        let wait = tokio::time::timeout(Duration::from_millis(500), ev_rx.recv()).await;
        match wait {
            Ok(Some(PeerEvent::Headers { peer, headers })) => {
                if let Some(s) = slots.iter_mut().find(|s| s.id == peer) {
                    s.last_activity = Instant::now();
                }
                let batch_len = headers.len();
                let mut added = 0usize;
                for hdr in headers {
                    let hash = hdr.block_hash();
                    if hub.has_block(&hash)
                        || want.iter().any(|h| h == &hash)
                        || inflight.contains_key(&hash)
                        || pool.contains_key(&hash)
                    {
                        known_headers.insert(hash);
                        continue;
                    }
                    let prev = hdr.prev_blockhash;
                    let prev_ok = known_headers.contains(&prev)
                        || hub.has_block(&prev)
                        || prev.to_byte_array() == [0u8; 32]
                        || hub.tip_hash() == Some(prev);
                    if !prev_ok && hub.tip_height().is_some() && !known_headers.is_empty() {
                        continue;
                    }
                    known_headers.insert(hash);
                    if !hub.has_block(&hash) {
                        want.push_back(hash);
                        added += 1;
                    }
                }
                if added > 0 {
                    last_progress = Instant::now();
                    empty_header_streak = 0;
                    // Full batch → ask same peer for more headers
                    if batch_len >= 2000 {
                        let _ = request_headers_from(&slots, peer, &hub, &mut header_req_seq);
                    }
                } else {
                    empty_header_streak = empty_header_streak.saturating_add(1);
                    if empty_header_streak >= slots.len() as u32 {
                        headers_done = true;
                    } else {
                        // Ask a different peer once
                        let _ = request_headers(&slots, &hub, &mut header_req_seq);
                    }
                }
            }
            Ok(Some(PeerEvent::Block { peer, block })) => {
                if let Some(s) = slots.iter_mut().find(|s| s.id == peer) {
                    s.last_activity = Instant::now();
                    s.in_flight.remove(&block.block_hash());
                }
                let hash = block.block_hash();
                inflight.remove(&hash);
                pool.insert(hash, block);
                last_progress = Instant::now();
                drain_connect(&hub, &mut pool, &accepted, start_tip)?;
            }
            Ok(Some(PeerEvent::Dead { peer, reason })) => {
                eprintln!("ibd: peer[{peer}] dead: {reason}");
                if let Some(s) = slots.iter_mut().find(|s| s.id == peer) {
                    s.alive = false;
                    for h in s.in_flight.drain() {
                        inflight.remove(&h);
                        if !hub.has_block(&h) && !pool.contains_key(&h) {
                            want.push_front(h);
                        }
                    }
                }
            }
            Ok(None) => break,
            Err(_) => {
                if last_progress.elapsed() > cfg.stall && want.is_empty() && inflight.is_empty() {
                    headers_done = true;
                    break;
                }
            }
        }
    }

    // Final drain
    drain_connect(&hub, &mut pool, &accepted, start_tip)?;

    for s in &slots {
        let _ = s.cmd_tx.send(PeerCmd::Shutdown);
        s.task.abort();
    }

    let n = accepted.load(Ordering::SeqCst);
    eprintln!(
        "ibd: parallel done accepted={n} tip={:?} (started {start_tip})",
        hub.tip_height()
    );
    Ok(n)
}

async fn spawn_peer(
    id: usize,
    addr: SocketAddr,
    magic: Magic,
    local: SocketAddr,
    tip_h: Option<u32>,
    ev_tx: mpsc::UnboundedSender<PeerEvent>,
) -> Result<PeerSlot, NetError> {
    let mut stream = TcpStream::connect(addr).await?;
    handshake(
        &mut stream,
        magic,
        local,
        addr,
        tip_h.map(|h| h as i32).unwrap_or(0),
        false,
    )
    .await?;

    let (cmd_tx, mut cmd_rx) = mpsc::unbounded_channel::<PeerCmd>();
    let task = tokio::spawn(async move {
        loop {
            tokio::select! {
                cmd = cmd_rx.recv() => {
                    match cmd {
                        Some(PeerCmd::GetHeaders { locator }) => {
                            let gh = GetHeadersMessage::new(
                                locator,
                                BlockHash::from_byte_array([0u8; 32]),
                            );
                            if write_msg(&mut stream, magic, NetworkMessage::GetHeaders(gh))
                                .await
                                .is_err()
                            {
                                let _ = ev_tx.send(PeerEvent::Dead {
                                    peer: id,
                                    reason: "write getheaders failed".into(),
                                });
                                break;
                            }
                        }
                        Some(PeerCmd::GetData { hashes }) => {
                            let inv: Vec<_> = hashes
                                .into_iter()
                                .map(Inventory::WitnessBlock)
                                .collect();
                            if inv.is_empty() {
                                continue;
                            }
                            if write_msg(&mut stream, magic, NetworkMessage::GetData(inv))
                                .await
                                .is_err()
                            {
                                let _ = ev_tx.send(PeerEvent::Dead {
                                    peer: id,
                                    reason: "write getdata failed".into(),
                                });
                                break;
                            }
                        }
                        Some(PeerCmd::Shutdown) | None => break,
                    }
                }
                msg = read_msg(&mut stream) => {
                    match msg {
                        Ok(m) => {
                            if m.magic() != &magic {
                                continue;
                            }
                            match m.payload() {
                                NetworkMessage::Headers(h) => {
                                    let _ = ev_tx.send(PeerEvent::Headers {
                                        peer: id,
                                        headers: h.clone(),
                                    });
                                }
                                NetworkMessage::Block(b) => {
                                    let _ = ev_tx.send(PeerEvent::Block {
                                        peer: id,
                                        block: b.clone(),
                                    });
                                }
                                NetworkMessage::Ping(n) => {
                                    let _ = write_msg(
                                        &mut stream,
                                        magic,
                                        NetworkMessage::Pong(*n),
                                    )
                                    .await;
                                }
                                NetworkMessage::NotFound(_) => {
                                    // treat as soft fail — peer just empty
                                }
                                _ => {}
                            }
                        }
                        Err(e) => {
                            let _ = ev_tx.send(PeerEvent::Dead {
                                peer: id,
                                reason: e.to_string(),
                            });
                            break;
                        }
                    }
                }
            }
        }
    });

    let _ = addr;
    Ok(PeerSlot {
        id,
        cmd_tx,
        in_flight: HashSet::new(),
        last_activity: Instant::now(),
        alive: true,
        task,
    })
}

fn request_headers(
    slots: &[PeerSlot],
    hub: &ChainHub,
    seq: &mut u32,
) -> Result<bool, NetError> {
    let alive: Vec<usize> = slots.iter().filter(|s| s.alive).map(|s| s.id).collect();
    if alive.is_empty() {
        return Ok(false);
    }
    let peer = alive[(*seq as usize) % alive.len()];
    *seq = seq.saturating_add(1);
    request_headers_from(slots, peer, hub, seq)
}

fn request_headers_from(
    slots: &[PeerSlot],
    peer: usize,
    hub: &ChainHub,
    _seq: &mut u32,
) -> Result<bool, NetError> {
    let Some(s) = slots.iter().find(|s| s.id == peer && s.alive) else {
        return Ok(false);
    };
    let locator = hub
        .query
        .locator_hashes()
        .map_err(|e| NetError::Consensus(e.to_string()))?;
    s.cmd_tx
        .send(PeerCmd::GetHeaders { locator })
        .map_err(|_| NetError::Protocol("peer cmd channel closed"))?;
    Ok(true)
}

fn assign_work(
    slots: &mut [PeerSlot],
    want: &mut VecDeque<BlockHash>,
    inflight: &mut HashMap<BlockHash, (usize, Instant)>,
    cfg: &IbdConfig,
) {
    let total_inflight = inflight.len();
    if total_inflight >= cfg.window || want.is_empty() {
        return;
    }
    let mut room = cfg.window - total_inflight;
    // Round-robin over alive peers with free slots.
    let alive_ids: Vec<usize> = slots
        .iter()
        .filter(|s| s.alive)
        .map(|s| s.id)
        .collect();
    if alive_ids.is_empty() {
        return;
    }
    let mut peer_i = 0usize;
    while room > 0 && !want.is_empty() {
        let mut assigned_any = false;
        for _ in 0..alive_ids.len() {
            let pid = alive_ids[peer_i % alive_ids.len()];
            peer_i += 1;
            let slot = match slots.iter_mut().find(|s| s.id == pid) {
                Some(s) if s.alive => s,
                _ => continue,
            };
            let free = cfg.per_peer.saturating_sub(slot.in_flight.len());
            if free == 0 {
                continue;
            }
            let take = free.min(room).min(want.len());
            if take == 0 {
                continue;
            }
            let mut batch = Vec::with_capacity(take);
            for _ in 0..take {
                if let Some(h) = want.pop_front() {
                    if inflight.contains_key(&h) {
                        continue;
                    }
                    batch.push(h);
                    slot.in_flight.insert(h);
                    inflight.insert(h, (pid, Instant::now()));
                }
            }
            if !batch.is_empty() {
                room = room.saturating_sub(batch.len());
                let _ = slot.cmd_tx.send(PeerCmd::GetData { hashes: batch });
                assigned_any = true;
            }
            if room == 0 {
                break;
            }
        }
        if !assigned_any {
            break;
        }
    }
}

fn reassign_stalled(
    slots: &mut [PeerSlot],
    inflight: &mut HashMap<BlockHash, (usize, Instant)>,
    want: &mut VecDeque<BlockHash>,
    cfg: &IbdConfig,
    now: Instant,
) {
    let stalled: Vec<BlockHash> = inflight
        .iter()
        .filter(|(_, (_, t))| now.duration_since(*t) > cfg.stall)
        .map(|(h, _)| *h)
        .collect();
    for h in stalled {
        if let Some((pid, _)) = inflight.remove(&h) {
            if let Some(s) = slots.iter_mut().find(|s| s.id == pid) {
                s.in_flight.remove(&h);
            }
            want.push_front(h);
            eprintln!("ibd: reassign stalled block from peer[{pid}]");
        }
    }
}

fn drain_connect(
    hub: &ChainHub,
    pool: &mut HashMap<BlockHash, Block>,
    accepted: &AtomicU32,
    start_tip: u32,
) -> Result<(), NetError> {
    let mut progress = true;
    while progress {
        progress = false;
        let tip = hub.tip_hash();
        let keys: Vec<BlockHash> = pool.keys().copied().collect();
        for h in keys {
            let Some(b) = pool.get(&h) else {
                continue;
            };
            let prev = b.header.prev_blockhash;
            let connects = match tip {
                None => prev.to_byte_array() == [0u8; 32],
                Some(t) => prev == t,
            };
            if !connects {
                continue;
            }
            let b = pool.remove(&h).unwrap();
            match hub.accept_block(b)? {
                AcceptOutcome::Accepted { .. } | AcceptOutcome::AlreadyHave => {
                    let n = accepted.fetch_add(1, Ordering::SeqCst) + 1;
                    progress = true;
                    if n == 1 || n % 100 == 0 {
                        eprintln!(
                            "ibd: progress tip={:?} (+{n} parallel, started {start_tip})",
                            hub.tip_height()
                        );
                        let _ = std::io::Write::flush(&mut std::io::stderr());
                    }
                }
                AcceptOutcome::IgnoredWeaker => {}
            }
        }
    }
    Ok(())
}
