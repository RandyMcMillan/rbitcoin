//! Concurrent multi-peer block download (libbitcoin-class windowed IBD).
//!
//! Architecture:
//! - N outbound peer workers (each: own TCP stream, command + event channels)
//! - Shared ordered work queue of block hashes after the local tip
//! - Download **window**: up to `window` blocks in-flight, at most `per_peer` per peer
//! - Blocks may arrive out of order; connect when parent is tip
//! - Stall timeout reassigns in-flight hashes to other peers

use crate::chain::{AcceptOutcome, ChainHub};
use crate::codec::{write_msg, MessageStream, MAX_HEADERS_RESULTS, MAX_INV_SIZE};
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
    /// TCP connect + handshake timeout per peer.
    pub connect_timeout: Duration,
}

impl Default for IbdConfig {
    fn default() -> Self {
        Self {
            window: 1024,
            per_peer: 16,
            stall: Duration::from_secs(5),
            headers_batch: MAX_HEADERS_RESULTS,
            connect_timeout: Duration::from_secs(8),
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
            headers_batch: MAX_HEADERS_RESULTS,
            connect_timeout: Duration::from_secs(3),
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

    // Genesis must exist so getheaders locator is real and blocks link to tip.
    hub.ensure_genesis()?;

    let (ev_tx, mut ev_rx) = mpsc::unbounded_channel::<PeerEvent>();

    // Full candidate list kept for mid-IBD redial; dial fails are retried later.
    let peer_pool: Vec<SocketAddr> = peers.to_vec();
    let mut next_peer_id = 0usize;
    let mut slots: Vec<PeerSlot> = Vec::new();

    // Initial concurrent dial (all candidates once).
    dial_batch(
        &mut slots,
        &peer_pool,
        &mut next_peer_id,
        peer_pool.len(),
        magic,
        local_addr,
        hub.tip_height(),
        ev_tx.clone(),
        cfg.connect_timeout,
    )
    .await;
    slots.retain(|s| s.alive);
    if slots.is_empty() {
        return Err(NetError::Protocol("no parallel peers connected"));
    }
    eprintln!("ibd: {} / {} peers ready", slots.len(), peer_pool.len());
    // Don't redial until we've been downloading for a bit.
    let mut last_redial = Instant::now();

    // Ordered download path (chain order after local tip). Front = next to connect.
    let mut ordered: VecDeque<BlockHash> = VecDeque::new();
    let mut ordered_set: HashSet<BlockHash> = HashSet::new();
    // hash → when requested + peer id
    let mut inflight: HashMap<BlockHash, (usize, Instant)> = HashMap::new();
    // Received but not yet connected (hash → block)
    let mut pool: HashMap<BlockHash, Block> = HashMap::new();
    // prev_hash → child block hash in pool (tip extension is O(1))
    let mut pool_by_prev: HashMap<BlockHash, BlockHash> = HashMap::new();
    // Known header hashes on the download path (for linkage)
    let mut known_headers: HashSet<BlockHash> = HashSet::new();
    if let Some(h) = hub.tip_hash() {
        known_headers.insert(h);
    }

    let accepted = AtomicU32::new(0);
    let start_tip = hub.tip_height().unwrap_or(0);
    let mut headers_done = false;
    let mut last_progress = Instant::now();
    let mut last_status = Instant::now();
    // Consecutive empty/useless header replies.
    let mut empty_header_streak = 0u32;
    let mut header_req_seq = 0u32;
    // How far ahead of connected tip we may download.
    let window = cfg.window;

    // Kick header sync — try a few peers (channel may close if handshake race).
    for _ in 0..slots.len().min(4) {
        if request_headers(&slots, &hub, &mut header_req_seq).unwrap_or(false) {
            break;
        }
    }

    loop {
        // Drop already-connected prefixes from the ordered queue.
        while let Some(&front) = ordered.front() {
            if hub.has_block(&front) {
                ordered.pop_front();
                ordered_set.remove(&front);
            } else {
                break;
            }
        }

        // How many hashes still needed beyond tip (ordered not yet in pool).
        let backlog = ordered
            .iter()
            .filter(|h| !pool.contains_key(*h) && !hub.has_block(h))
            .count();
        let ahead = pool.len() + inflight.len();

        // Pipeline headers only while backlog is thin (avoid multi-k orphan pool).
        if !headers_done && backlog < window && pool.len() < window / 2 {
            let _ = request_headers(&slots, &hub, &mut header_req_seq);
        }

        // Assign work: only hashes within the download window from ordered front.
        assign_work_ordered(
            &mut slots,
            &ordered,
            &mut inflight,
            &pool,
            hub.as_ref(),
            &cfg,
            window,
        );

        // Stall reassignment (re-request; ordered path still owns the hashes).
        let now = Instant::now();
        reassign_stalled(&mut slots, &mut inflight, &cfg, now);

        // Redial when we have few live peers (non-blocking-ish: at most every 30s).
        let alive_n = slots.iter().filter(|s| s.alive).count();
        if alive_n < 3 && last_redial.elapsed() > Duration::from_secs(30) {
            let want = (6usize.saturating_sub(alive_n)).max(2).min(6);
            eprintln!("ibd: redialing up to {want} peers (alive={alive_n})…");
            dial_batch(
                &mut slots,
                &peer_pool,
                &mut next_peer_id,
                want,
                magic,
                local_addr,
                hub.tip_height(),
                ev_tx.clone(),
                cfg.connect_timeout,
            )
            .await;
            last_redial = Instant::now();
        }

        // Status line (helps lab debugging; line-buffered-ish)
        if last_status.elapsed() > Duration::from_secs(5) {
            eprintln!(
                "ibd: status tip={:?} ordered={} inflight={} pool={} ahead={ahead} headers_done={headers_done} peers={}",
                hub.tip_height(),
                ordered.len(),
                inflight.len(),
                pool.len(),
                slots.iter().filter(|s| s.alive).count(),
            );
            let _ = std::io::Write::flush(&mut std::io::stderr());
            last_status = Instant::now();
        }

        // Exit if nothing left and no inflight
        if headers_done && ordered.is_empty() && inflight.is_empty() {
            drain_connect(
                &hub,
                &mut pool,
                &mut pool_by_prev,
                &mut ordered,
                &mut ordered_set,
                &accepted,
                start_tip,
            )?;
            if pool.is_empty() {
                break;
            }
            if last_progress.elapsed() > cfg.stall {
                eprintln!(
                    "ibd: dropping {} orphan pool blocks after stall",
                    pool.len()
                );
                pool.clear();
                pool_by_prev.clear();
                break;
            }
        }
        if ordered.is_empty() && inflight.is_empty() && pool.is_empty() && headers_done {
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
        let wait = tokio::time::timeout(Duration::from_millis(100), ev_rx.recv()).await;
        match wait {
            Ok(Some(PeerEvent::Headers { peer, headers })) => {
                if let Some(s) = slots.iter_mut().find(|s| s.id == peer) {
                    s.last_activity = Instant::now();
                }
                let batch_len = headers.len();
                let mut added = 0usize;
                for hdr in headers {
                    let hash = hdr.block_hash();
                    if hub.has_block(&hash) || ordered_set.contains(&hash) || pool.contains_key(&hash)
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
                    ordered.push_back(hash);
                    ordered_set.insert(hash);
                    added += 1;
                }
                if added > 0 {
                    last_progress = Instant::now();
                    empty_header_streak = 0;
                    // Full batch and backlog thin → pipeline more headers
                    if batch_len >= MAX_HEADERS_RESULTS && ordered.len() < window * 2 {
                        let _ = request_headers_from(&slots, peer, &hub, &mut header_req_seq);
                    }
                } else if batch_len == 0 {
                    empty_header_streak = empty_header_streak.saturating_add(1);
                    if empty_header_streak >= (slots.len() as u32).max(2)
                        && ordered.is_empty()
                        && inflight.is_empty()
                    {
                        headers_done = true;
                    } else if empty_header_streak < 8 && ordered.len() < window {
                        let _ = request_headers(&slots, &hub, &mut header_req_seq);
                    } else if empty_header_streak >= 8 {
                        headers_done = true;
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
                let prev = block.header.prev_blockhash;
                pool_by_prev.insert(prev, hash);
                pool.insert(hash, block);
                last_progress = Instant::now();
                drain_connect(
                    &hub,
                    &mut pool,
                    &mut pool_by_prev,
                    &mut ordered,
                    &mut ordered_set,
                    &accepted,
                    start_tip,
                )?;
            }
            Ok(Some(PeerEvent::Dead { peer, reason })) => {
                eprintln!("ibd: peer[{peer}] dead: {reason}");
                if let Some(s) = slots.iter_mut().find(|s| s.id == peer) {
                    s.alive = false;
                    for h in s.in_flight.drain() {
                        inflight.remove(&h);
                    }
                }
            }
            Ok(None) => break,
            Err(_) => {
                // timeout tick — try connect + gap re-request of ordered front
                drain_connect(
                    &hub,
                    &mut pool,
                    &mut pool_by_prev,
                    &mut ordered,
                    &mut ordered_set,
                    &accepted,
                    start_tip,
                )?;
                if last_progress.elapsed() > cfg.stall
                    && ordered.is_empty()
                    && inflight.is_empty()
                    && pool.is_empty()
                {
                    headers_done = true;
                    let _ = headers_done; // used for exit conditions above
                    break;
                }
            }
        }
    }

    // Final drain
    drain_connect(
        &hub,
        &mut pool,
        &mut pool_by_prev,
        &mut ordered,
        &mut ordered_set,
        &accepted,
        start_tip,
    )?;

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

/// Dial up to `count` peers from `pool` concurrently; append successful slots.
/// Candidates rotate via `next_id` so redials walk the list.
async fn dial_batch(
    slots: &mut Vec<PeerSlot>,
    pool: &[SocketAddr],
    next_id: &mut usize,
    count: usize,
    magic: Magic,
    local_addr: SocketAddr,
    tip_h: Option<u32>,
    ev_tx: mpsc::UnboundedSender<PeerEvent>,
    connect_timeout: Duration,
) {
    if count == 0 || pool.is_empty() {
        return;
    }

    let mut join_set = tokio::task::JoinSet::new();
    let n = pool.len();
    let mut spawned = 0usize;
    while spawned < count && spawned < n {
        let idx = (*next_id) % n;
        *next_id = next_id.saturating_add(1);
        let addr = pool[idx];
        let id = *next_id;
        *next_id = next_id.saturating_add(1);
        let ev = ev_tx.clone();
        join_set.spawn(async move {
            let fut = spawn_peer(id, addr, magic, local_addr, tip_h, ev);
            match tokio::time::timeout(connect_timeout, fut).await {
                Ok(Ok(slot)) => Ok(slot),
                Ok(Err(e)) => Err((id, addr, e.to_string())),
                Err(_) => Err((id, addr, format!("connect timeout ({connect_timeout:?})"))),
            }
        });
        spawned += 1;
    }
    while let Some(joined) = join_set.join_next().await {
        match joined {
            Ok(Ok(slot)) => {
                eprintln!("ibd: parallel peer[{}] connected", slot.id);
                slots.push(slot);
            }
            Ok(Err((id, addr, reason))) => {
                eprintln!("ibd: parallel peer[{id}] {addr} failed: {reason}");
            }
            Err(e) => {
                eprintln!("ibd: peer connect task panicked: {e}");
            }
        }
    }
    slots.sort_by_key(|s| s.id);
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
        // Cancellation-safe framer: select! must not desync partial header reads.
        let mut framer = MessageStream::new();
        loop {
            tokio::select! {
                cmd = cmd_rx.recv() => {
                    match cmd {
                        Some(PeerCmd::GetHeaders { locator }) => {
                            let locator = if locator.len() > crate::codec::MAX_LOCATOR_SZ {
                                locator[..crate::codec::MAX_LOCATOR_SZ].to_vec()
                            } else {
                                locator
                            };
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
                            // Cap each getdata to Core MAX_INV_SZ (we also
                            // assign small per-peer batches; this is a hard stop).
                            for chunk in hashes.chunks(MAX_INV_SIZE) {
                                let inv: Vec<_> = chunk
                                    .iter()
                                    .copied()
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
                                    return;
                                }
                            }
                        }
                        Some(PeerCmd::Shutdown) | None => break,
                    }
                }
                msg = framer.read_msg(&mut stream, Some(magic)) => {
                    match msg {
                        Ok(m) => {
                            match m.payload() {
                                NetworkMessage::Headers(h) => {
                                    let headers = if h.len() > MAX_HEADERS_RESULTS {
                                        h[..MAX_HEADERS_RESULTS].to_vec()
                                    } else {
                                        h.clone()
                                    };
                                    let _ = ev_tx.send(PeerEvent::Headers {
                                        peer: id,
                                        headers,
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
    if s.cmd_tx.send(PeerCmd::GetHeaders { locator }).is_err() {
        return Ok(false);
    }
    Ok(true)
}

/// Assign getdata for missing hashes in chain order (nearest tip first).
///
/// Always prioritizes the tip-extension gap even if the orphan pool is large —
/// otherwise a full pool of future blocks freezes IBD forever.
fn assign_work_ordered(
    slots: &mut [PeerSlot],
    ordered: &VecDeque<BlockHash>,
    inflight: &mut HashMap<BlockHash, (usize, Instant)>,
    pool: &HashMap<BlockHash, Block>,
    hub: &ChainHub,
    cfg: &IbdConfig,
    window: usize,
) {
    let total_inflight = inflight.len();
    if total_inflight >= window {
        return;
    }
    let mut room = window - total_inflight;

    let alive_ids: Vec<usize> = slots
        .iter()
        .filter(|s| s.alive)
        .map(|s| s.id)
        .collect();
    if alive_ids.is_empty() {
        return;
    }

    // Candidates: ordered hashes not yet downloaded / requested, nearest tip first.
    // When the pool is already large, only fill the contiguous tip gap so we
    // don't dig the hole deeper with far-ahead getdata.
    let take_n = if pool.len() >= window {
        8
    } else if pool.len() >= window / 4 {
        32
    } else {
        window
    };
    let mut candidates: VecDeque<BlockHash> = ordered
        .iter()
        .copied()
        .filter(|h| !hub.has_block(h) && !pool.contains_key(h) && !inflight.contains_key(h))
        .take(take_n)
        .collect();
    if candidates.is_empty() {
        return;
    }
    // Cap total in-flight when pool is bloated (focus bandwidth on the gap).
    if pool.len() >= window / 4 {
        room = room.min(take_n.saturating_sub(inflight.len()).max(4));
    }

    let mut peer_i = 0usize;
    while room > 0 && !candidates.is_empty() {
        let mut assigned_any = false;
        for _ in 0..alive_ids.len() {
            let pid = alive_ids[peer_i % alive_ids.len()];
            peer_i += 1;
            let slot = match slots.iter_mut().find(|s| s.id == pid) {
                Some(s) if s.alive => s,
                _ => continue,
            };
            let peer_cap = if alive_ids.len() <= 2 {
                cfg.per_peer.max(128)
            } else if alive_ids.len() <= 4 {
                cfg.per_peer.max(64)
            } else {
                cfg.per_peer
            };
            let free = peer_cap.saturating_sub(slot.in_flight.len());
            if free == 0 {
                continue;
            }
            let batch_cap = if alive_ids.len() <= 2 { 64 } else { 16 };
            let take = free.min(room).min(candidates.len()).min(batch_cap);
            if take == 0 {
                continue;
            }
            let mut batch = Vec::with_capacity(take);
            while batch.len() < take {
                let Some(h) = candidates.pop_front() else {
                    break;
                };
                if hub.has_block(&h) || pool.contains_key(&h) || inflight.contains_key(&h) {
                    continue;
                }
                batch.push(h);
                slot.in_flight.insert(h);
                inflight.insert(h, (pid, Instant::now()));
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
    cfg: &IbdConfig,
    now: Instant,
) {
    let stalled: Vec<(BlockHash, usize)> = inflight
        .iter()
        .filter(|(_, (_, t))| now.duration_since(*t) > cfg.stall)
        .map(|(h, (pid, _))| (*h, *pid))
        .collect();
    if stalled.is_empty() {
        return;
    }
    // Clear stall state so assign_work_ordered will re-issue getdata.
    let mut n = 0usize;
    for (h, pid) in stalled {
        if let Some(_) = inflight.remove(&h) {
            if let Some(s) = slots.iter_mut().find(|s| s.id == pid) {
                s.in_flight.remove(&h);
            }
            n += 1;
        }
    }
    if n > 0 {
        eprintln!("ibd: reassign {n} stalled block(s)");
    }
}

fn remove_from_ordered(
    ordered: &mut VecDeque<BlockHash>,
    ordered_set: &mut HashSet<BlockHash>,
    h: BlockHash,
) {
    ordered_set.remove(&h);
    ordered.retain(|x| *x != h);
}

fn drain_connect(
    hub: &ChainHub,
    pool: &mut HashMap<BlockHash, Block>,
    pool_by_prev: &mut HashMap<BlockHash, BlockHash>,
    ordered: &mut VecDeque<BlockHash>,
    ordered_set: &mut HashSet<BlockHash>,
    accepted: &AtomicU32,
    start_tip: u32,
) -> Result<(), NetError> {
    loop {
        // Drop any ordered prefix already in the store.
        while let Some(&front) = ordered.front() {
            if hub.has_block(&front) {
                remove_from_ordered(ordered, ordered_set, front);
            } else {
                break;
            }
        }

        // Parent key for the next connectable block: tip hash, or null for genesis.
        let prev_key = hub
            .tip_hash()
            .unwrap_or_else(|| BlockHash::from_byte_array([0u8; 32]));

        let Some(h) = pool_by_prev.remove(&prev_key) else {
            break;
        };
        let Some(b) = pool.remove(&h) else {
            // Stale index entry; keep draining.
            continue;
        };

        match hub.accept_block(b) {
            Ok(AcceptOutcome::Accepted { .. }) | Ok(AcceptOutcome::AlreadyHave) => {
                remove_from_ordered(ordered, ordered_set, h);
                let n = accepted.fetch_add(1, Ordering::SeqCst) + 1;
                if n == 1 || n % 100 == 0 {
                    eprintln!(
                        "ibd: progress tip={:?} (+{n} parallel, started {start_tip})",
                        hub.tip_height()
                    );
                    let _ = std::io::Write::flush(&mut std::io::stderr());
                }
            }
            Ok(AcceptOutcome::IgnoredWeaker) => {
                remove_from_ordered(ordered, ordered_set, h);
            }
            Err(e) => {
                eprintln!("ibd: reject block {h}: {e}");
                // Keep hash in ordered for re-request; body discarded.
                break;
            }
        }
    }
    Ok(())
}
