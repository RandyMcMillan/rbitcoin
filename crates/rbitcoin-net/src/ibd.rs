//! Concurrent multi-peer block download (libbitcoin-class windowed IBD).
//!
//! Architecture:
//! - N outbound peer workers (each: own TCP stream, command + event channels)
//! - Shared ordered work queue of block hashes after the local tip
//! - Download **window**: up to `window` blocks in-flight, at most `per_peer` per peer
//! - Archive pipeline: parallel **prep** (CPU) → single **writer** (Class A mmap)
//! - Tip **confirm** walks contiguous archived runs (Class C) on the IBD task
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
use rbitcoin_consensus::prepare_block_for_archive;
use rbitcoin_query::TxApply;
use rbitcoin_store::HeaderRecord;
use std::collections::{HashMap, HashSet, VecDeque};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, Semaphore};
use tokio::task::JoinHandle;
use rbitcoin_log::{debug, error, info, warn};

/// Default download / archive horizon after the connected tip (Core
/// `BLOCK_DOWNLOAD_WINDOW`-class: how far ahead we may request overall).
pub const DEFAULT_IBD_WINDOW: usize = 1024;

/// Max blocks in flight to a single peer (Core `MAX_BLOCKS_IN_TRANSIT_PER_PEER`).
pub const DEFAULT_BLOCKS_IN_TRANSIT_PER_PEER: usize = 16;

/// Tunables for parallel IBD (defaults lean libbitcoin/Core-ish).
#[derive(Clone, Debug)]
pub struct IbdConfig {
    /// Global tip-ahead horizon (`ordered[0..window]`); not per-peer load.
    pub window: usize,
    /// Hard cap on outstanding block getdata to one peer (Core = 16).
    pub per_peer: usize,
    /// Desired number of live download peers; we keep redialing until we reach this
    /// (or exhaust the candidate pool).
    pub target_peers: usize,
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
            window: DEFAULT_IBD_WINDOW,
            per_peer: DEFAULT_BLOCKS_IN_TRANSIT_PER_PEER,
            target_peers: 16,
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
            target_peers: 4,
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

/// Wire block queued for archive prep (P0/P1 pipeline).
struct ArchiveJob {
    block: Block,
}

struct PreparedArchive {
    hash: BlockHash,
    header: HeaderRecord,
    txs: Vec<TxApply>,
}

enum ArchiveResult {
    Ok {
        #[allow(dead_code)]
        hash: BlockHash,
    },
    Err { hash: BlockHash, err: String },
}

struct PeerSlot {
    id: usize,
    addr: SocketAddr,
    cmd_tx: mpsc::UnboundedSender<PeerCmd>,
    /// Hashes currently requested from this peer.
    in_flight: HashSet<BlockHash>,
    /// When we last received a useful message.
    #[allow(dead_code)]
    last_activity: Instant,
    alive: bool,
    task: JoinHandle<()>,
}

impl Drop for PeerSlot {
    fn drop(&mut self) {
        // Ensure peer IO tasks die when IBD is cancelled (e.g. signal shutdown).
        let _ = self.cmd_tx.send(PeerCmd::Shutdown);
        self.task.abort();
    }
}

/// Run parallel IBD against `peers` until no more headers/blocks, or all peers die.
///
/// If `cancel` is set, the loop exits cooperatively at the next iteration (used by
/// SIGTERM/SIGINT handling so we can flush before process exit).
///
/// Returns approximate number of blocks accepted this run.
pub async fn parallel_ibd(
    hub: Arc<ChainHub>,
    magic: Magic,
    local_addr: SocketAddr,
    peers: &[SocketAddr],
    cfg: IbdConfig,
) -> Result<u32, NetError> {
    parallel_ibd_cancellable(hub, magic, local_addr, peers, cfg, None).await
}

/// Like [`parallel_ibd`], with an optional cancel flag polled each loop turn.
pub async fn parallel_ibd_cancellable(
    hub: Arc<ChainHub>,
    magic: Magic,
    local_addr: SocketAddr,
    peers: &[SocketAddr],
    cfg: IbdConfig,
    cancel: Option<std::sync::Arc<std::sync::atomic::AtomicBool>>,
) -> Result<u32, NetError> {
    if peers.is_empty() {
        return Err(NetError::Protocol("no peers for parallel ibd"));
    }
    let cancelled = || {
        cancel
            .as_ref()
            .map(|c| c.load(std::sync::atomic::Ordering::SeqCst))
            .unwrap_or(false)
    };

    // Genesis must exist so getheaders locator is real and blocks link to tip.
    hub.ensure_genesis()?;

    let (ev_tx, mut ev_rx) = mpsc::unbounded_channel::<PeerEvent>();

    // Full candidate list kept for mid-IBD redial; dial fails are retried later.
    let peer_pool: Vec<SocketAddr> = peers.to_vec();
    let next_peer_id = Arc::new(AtomicUsize::new(0));
    let mut slots: Vec<PeerSlot> = Vec::new();

    // Initial concurrent dial (all candidates once) — OK to await before loop starts.
    {
        let fresh = dial_batch(
            &peer_pool,
            &next_peer_id,
            peer_pool.len(),
            HashSet::new(),
            magic,
            local_addr,
            hub.tip_height(),
            ev_tx.clone(),
            cfg.connect_timeout,
        )
        .await;
        slots.extend(fresh);
    }
    slots.retain(|s| s.alive);
    if slots.is_empty() {
        return Err(NetError::Protocol("no parallel peers connected"));
    }
    info!(
        "ibd: {} / {} peers ready (target={})",
        slots.len(),
        peer_pool.len(),
        cfg.target_peers
    );
    // Background redial — never .await dial on the IBD event loop (that stalled tip).
    let mut last_redial = Instant::now() - Duration::from_secs(15);
    let mut redial_handle: Option<JoinHandle<Vec<PeerSlot>>> = None;

    // Ordered download path (chain order after local tip). Front = next to confirm.
    let mut ordered: VecDeque<BlockHash> = VecDeque::new();
    let mut ordered_set: HashSet<BlockHash> = HashSet::new();
    // Absolute heights for archive validation / confirm.
    let mut hash_height: HashMap<BlockHash, u32> = HashMap::new();
    // hash → when requested + peer id
    let mut inflight: HashMap<BlockHash, (usize, Instant)> = HashMap::new();
    // Known header hashes on the download path (for linkage)
    let mut known_headers: HashSet<BlockHash> = HashSet::new();
    if let Some(h) = hub.tip_hash() {
        known_headers.insert(h);
        if let Some(th) = hub.tip_height() {
            hash_height.insert(h, th);
        }
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

    // Multi-core archive: rayon prep pool + dedicated OS writer thread.
    let archive_queued = Arc::new(AtomicUsize::new(0));
    let (arch_job_tx, arch_job_rx) = mpsc::unbounded_channel::<ArchiveJob>();
    let (arch_res_tx, mut arch_res_rx) = mpsc::unbounded_channel::<ArchiveResult>();
    let n_cpu = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4);
    // Leave 1 core for async IO / confirm; rest for prep (writer is extra OS thread).
    let n_prep = n_cpu.saturating_sub(1).max(2);
    debug!("ibd: archive pipeline prep_threads={n_prep} cpus={n_cpu} (dedicated writer thread)");
    let pipeline = spawn_archive_pipeline(hub.clone(), arch_job_rx, arch_res_tx, n_prep);

    loop {
        if cancelled() {
            warn!("ibd: cancel requested — stopping parallel IBD");
            break;
        }
        // Yield so a concurrent shutdown select/task can run promptly.
        tokio::task::yield_now().await;

        // Drop already-confirmed prefixes from the ordered queue.
        while let Some(&front) = ordered.front() {
            if hub.has_block(&front) {
                ordered.pop_front();
                ordered_set.remove(&front);
            } else {
                break;
            }
        }

        // Confirm any contiguous archived run at tip (bodies already on disk).
        let confirmed_n = confirm_run(
            hub.as_ref(),
            &mut ordered,
            &mut ordered_set,
            &hash_height,
            &accepted,
            start_tip,
            CONNECT_BATCH,
        )?;
        if confirmed_n > 0 {
            last_progress = Instant::now();
        }

        // Horizon = next `window` hashes after tip. Download holes = not archived.
        let horizon = window;
        let holes = ordered
            .iter()
            .take(horizon)
            .filter(|h| {
                !hub.has_block(h)
                    && !hub.is_archived(h)
                    && !inflight.contains_key(*h)
            })
            .count();
        let archived_ahead = ordered
            .iter()
            .take(horizon)
            .filter(|h| hub.is_archived(h) && !hub.has_block(h))
            .count();
        let ahead = archived_ahead + inflight.len();

        // Pipeline headers when the ordered queue is thin.
        if !headers_done
            && (ordered.is_empty() || (holes < horizon / 4 && ordered.len() < horizon * 2))
        {
            let _ = request_headers(&slots, &hub, &mut header_req_seq);
        }

        // Hard path reset after a long stall with no tip advance.
        // Do not clear a full ordered queue that simply has not finished getdata yet.
        if last_progress.elapsed() > cfg.stall.saturating_mul(6)
            && ordered.is_empty()
            && inflight.is_empty()
        {
            info!(
                "ibd: hard path reset (stall {:?}, ordered empty)",
                last_progress.elapsed()
            );
            let _ = request_headers(&slots, &hub, &mut header_req_seq);
            last_progress = Instant::now();
        }

        // Backpressure: if archive queue is deep, stop issuing new getdata.
        let arch_q = archive_queued.load(Ordering::Relaxed);
        if arch_q < cfg.window.saturating_mul(2) {
            assign_work_ordered(
                &mut slots,
                &ordered,
                &mut inflight,
                hub.as_ref(),
                &cfg,
                horizon,
            );
        }

        // Stall reassignment — skip while tip is advancing (connect may hold the
        // loop longer than stall at high height; reassign storms made it worse).
        let now = Instant::now();
        if last_progress.elapsed() > cfg.stall {
            reassign_stalled(&mut slots, &mut inflight, &cfg, now);
        }

        // Collect finished background dials without blocking the event loop.
        if redial_handle
            .as_ref()
            .map(|h| h.is_finished())
            .unwrap_or(false)
        {
            if let Some(h) = redial_handle.take() {
                match h.await {
                    Ok(fresh) => {
                        let n = fresh.len();
                        for s in fresh {
                            info!("ibd: parallel peer[{}] {} connected", s.id, s.addr);
                            slots.push(s);
                        }
                        if n > 0 {
                            slots.sort_by_key(|s| s.id);
                            info!(
                                "ibd: redial added {n} peer(s); live={}",
                                slots.iter().filter(|s| s.alive).count()
                            );
                        }
                    }
                    Err(e) => warn!("ibd: redial task failed: {e}"),
                }
            }
        }

        // Kick a non-blocking redial if under target and none in flight.
        let alive_n = slots.iter().filter(|s| s.alive).count();
        let target = cfg.target_peers.min(peer_pool.len()).max(1);
        if redial_handle.is_none()
            && alive_n < target
            && last_redial.elapsed() > Duration::from_secs(15)
        {
            let want = (target - alive_n).min(8).max(1);
            let already: HashSet<SocketAddr> = slots
                .iter()
                .filter(|s| s.alive)
                .map(|s| s.addr)
                .collect();
            info!(
                "ibd: redialing up to {want} peers (alive={alive_n}/{target}, pool={})…",
                peer_pool.len()
            );
            let pool = peer_pool.clone();
            let next_id = next_peer_id.clone();
            let tip_h = hub.tip_height();
            let ev = ev_tx.clone();
            let cto = cfg.connect_timeout;
            redial_handle = Some(tokio::spawn(async move {
                dial_batch(
                    &pool,
                    &next_id,
                    want,
                    already,
                    magic,
                    local_addr,
                    tip_h,
                    ev,
                    cto,
                )
                .await
            }));
            last_redial = Instant::now();
        }

        // Status line (helps lab debugging; line-buffered-ish)
        if last_status.elapsed() > Duration::from_secs(5) {
            info!(
                "ibd: status tip={:?} ordered={} inflight={} arch_q={arch_q} archived_ahead={archived_ahead} ahead={ahead} headers_done={headers_done} peers={}",
                hub.tip_height(),
                ordered.len(),
                inflight.len(),
                slots.iter().filter(|s| s.alive).count(),
            );
            let _ = std::io::Write::flush(&mut std::io::stderr());
            last_status = Instant::now();
        }

        // Exit if nothing left and no inflight
        if headers_done && ordered.is_empty() && inflight.is_empty() {
            let _ = confirm_run(
                hub.as_ref(),
                &mut ordered,
                &mut ordered_set,
                &hash_height,
                &accepted,
                start_tip,
                CONNECT_BATCH,
            )?;
            if ordered.is_empty() {
                break;
            }
            if last_progress.elapsed() > cfg.stall {
                break;
            }
        }
        if ordered.is_empty() && inflight.is_empty() && headers_done {
            break;
        }
        // All peers dead
        if slots.iter().all(|s| !s.alive) {
            if accepted.load(Ordering::SeqCst) > 0 {
                break;
            }
            return Err(NetError::Protocol("all parallel peers dead"));
        }

        // Peer events, archive completions, or timeout (stall reassign / confirm).
        let tick = tokio::time::sleep(Duration::from_millis(100));
        tokio::pin!(tick);
        tokio::select! {
            peer_ev = ev_rx.recv() => match peer_ev {
            Some(PeerEvent::Headers { peer, headers }) => {
                if let Some(s) = slots.iter_mut().find(|s| s.id == peer) {
                    s.last_activity = Instant::now();
                }
                let batch_len = headers.len();
                let mut added = 0usize;
                for hdr in headers {
                    let hash = hdr.block_hash();
                    let prev = hdr.prev_blockhash;
                    // Always track height along the header path (even if already known).
                    let h = if let Some(&ph) = hash_height.get(&prev) {
                        ph.saturating_add(1)
                    } else if hub.tip_hash() == Some(prev) || hub.has_block(&prev) {
                        // Parent on confirmed tip chain: only correct if parent is tip
                        // for linear extension; prefer tip+1 when prev is tip.
                        if hub.tip_hash() == Some(prev) {
                            hub.tip_height().unwrap_or(0).saturating_add(1)
                        } else {
                            hash_height
                                .get(&prev)
                                .copied()
                                .unwrap_or_else(|| hub.tip_height().unwrap_or(0).saturating_add(1))
                        }
                    } else if prev.to_byte_array() == [0u8; 32] {
                        0
                    } else {
                        ordered
                            .back()
                            .and_then(|b| hash_height.get(b).copied())
                            .map(|x| x.saturating_add(1))
                            .unwrap_or_else(|| hub.tip_height().unwrap_or(0).saturating_add(1))
                    };
                    hash_height.entry(hash).or_insert(h);

                    if hub.has_block(&hash) || ordered_set.contains(&hash) || hub.is_archived(&hash)
                    {
                        known_headers.insert(hash);
                        let _ = hub.ensure_header(&hdr);
                        continue;
                    }
                    let prev_ok = known_headers.contains(&prev)
                        || hub.has_block(&prev)
                        || prev.to_byte_array() == [0u8; 32]
                        || hub.tip_hash() == Some(prev);
                    if !prev_ok && hub.tip_height().is_some() && !known_headers.is_empty() {
                        continue;
                    }
                    let _ = hub.ensure_header(&hdr);
                    known_headers.insert(hash);
                    ordered.push_back(hash);
                    ordered_set.insert(hash);
                    added += 1;
                }
                if added > 0 {
                    empty_header_streak = 0;
                    // Headers alone are not tip progress; keep last_progress for gap detect.
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
            Some(PeerEvent::Block { peer, block }) => {
                if let Some(s) = slots.iter_mut().find(|s| s.id == peer) {
                    s.last_activity = Instant::now();
                    s.in_flight.remove(&block.block_hash());
                }
                let hash = block.block_hash();
                inflight.remove(&hash);
                // Parent header for prev_fk; body goes to async archive pipeline.
                let _ = hub.ensure_header(&block.header);
                archive_queued.fetch_add(1, Ordering::Relaxed);
                if arch_job_tx.send(ArchiveJob { block }).is_err() {
                    archive_queued.fetch_sub(1, Ordering::Relaxed);
                    warn!("ibd: archive pipeline closed; drop {hash}");
                }
                // Confirm on archive-done / tick only — frees this core for IO/assign.
            }
            Some(PeerEvent::Dead { peer, reason }) => {
                warn!("ibd: peer[{peer}] dead: {reason}");
                if let Some(s) = slots.iter_mut().find(|s| s.id == peer) {
                    s.alive = false;
                    for h in s.in_flight.drain() {
                        inflight.remove(&h);
                    }
                }
            }
            None => break,
            },
            arch = arch_res_rx.recv() => match arch {
                Some(ArchiveResult::Ok { hash: _ }) => {
                    archive_queued.fetch_sub(1, Ordering::Relaxed);
                    let n = confirm_run(
                        hub.as_ref(),
                        &mut ordered,
                        &mut ordered_set,
                        &hash_height,
                        &accepted,
                        start_tip,
                        CONNECT_BATCH,
                    )?;
                    if n > 0 {
                        last_progress = Instant::now();
                    }
                }
                Some(ArchiveResult::Err { hash, err }) => {
                    archive_queued.fetch_sub(1, Ordering::Relaxed);
                    static REJECTS: AtomicU32 = AtomicU32::new(0);
                    let n = REJECTS.fetch_add(1, Ordering::Relaxed) + 1;
                    if n <= 5 || n % 100 == 0 {
                        warn!("ibd: archive reject {hash}: {err} (count={n})");
                    }
                }
                None => {
                    // Pipeline exited unexpectedly.
                    if !cancelled() {
                        warn!("ibd: archive pipeline ended");
                    }
                }
            },
            _ = &mut tick => {
                // timeout tick — keep confirming archived runs
                let n = confirm_run(
                    hub.as_ref(),
                    &mut ordered,
                    &mut ordered_set,
                    &hash_height,
                    &accepted,
                    start_tip,
                    CONNECT_BATCH,
                )?;
                if n > 0 {
                    last_progress = Instant::now();
                }
                if last_progress.elapsed() > cfg.stall
                    && ordered.is_empty()
                    && inflight.is_empty()
                    && archive_queued.load(Ordering::Relaxed) == 0
                {
                    headers_done = true;
                    let _ = headers_done;
                    break;
                }
            }
        }
    }

    // Drain archive pipeline then final confirm.
    drop(arch_job_tx);
    while let Some(r) = arch_res_rx.recv().await {
        archive_queued.fetch_sub(1, Ordering::Relaxed);
        if let ArchiveResult::Err { hash, err } = r {
            warn!("ibd: archive reject {hash}: {err}");
        }
    }
    let _ = pipeline.await;

    while confirm_run(
        hub.as_ref(),
        &mut ordered,
        &mut ordered_set,
        &hash_height,
        &accepted,
        start_tip,
        CONNECT_BATCH,
    )? > 0
    {}

    for s in &slots {
        let _ = s.cmd_tx.send(PeerCmd::Shutdown);
        s.task.abort();
    }

    let n = accepted.load(Ordering::SeqCst);
    info!(
        "ibd: parallel done accepted={n} tip={:?} (started {start_tip})",
        hub.tip_height()
    );
    Ok(n)
}

/// Dial up to `count` peers from `pool` concurrently; return successful slots.
///
/// Safe to run on a background task — does not touch the IBD event loop.
/// Candidates rotate via `next_id`. Skips `already` live addrs.
async fn dial_batch(
    pool: &[SocketAddr],
    next_id: &AtomicUsize,
    count: usize,
    already: HashSet<SocketAddr>,
    magic: Magic,
    local_addr: SocketAddr,
    tip_h: Option<u32>,
    ev_tx: mpsc::UnboundedSender<PeerEvent>,
    connect_timeout: Duration,
) -> Vec<PeerSlot> {
    let mut out = Vec::new();
    if count == 0 || pool.is_empty() {
        return out;
    }

    let mut join_set = tokio::task::JoinSet::new();
    let n = pool.len();
    let mut spawned = 0usize;
    let mut attempts = 0usize;
    while spawned < count && attempts < n {
        attempts += 1;
        let idx = next_id.fetch_add(1, Ordering::Relaxed) % n;
        let addr = pool[idx];
        if already.contains(&addr) {
            continue;
        }
        let id = next_id.fetch_add(1, Ordering::Relaxed);
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
            Ok(Ok(slot)) => out.push(slot),
            Ok(Err((id, addr, reason))) => {
                warn!("ibd: parallel peer[{id}] {addr} failed: {reason}");
            }
            Err(e) => {
                error!("ibd: peer connect task panicked: {e}");
            }
        }
    }
    out.sort_by_key(|s| s.id);
    out
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
        addr,
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

/// Rayon prep pool (multi-core CPU) + dedicated OS thread for Class A mmap writes.
///
/// Tokio only bridges job/result channels so the async IBD loop stays free.
fn spawn_archive_pipeline(
    hub: Arc<ChainHub>,
    mut job_rx: mpsc::UnboundedReceiver<ArchiveJob>,
    result_tx: mpsc::UnboundedSender<ArchiveResult>,
    n_prep: usize,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let n_prep = n_prep.max(2);
        let (write_tx, write_rx) =
            std::sync::mpsc::sync_channel::<PreparedArchive>(n_prep.saturating_mul(8).max(16));
        let write_hub = hub.clone();
        let write_result = result_tx.clone();

        // Dedicated writer OS thread — never shares tokio blocking pool with prep.
        let writer = std::thread::Builder::new()
            .name("ibd-archive-writer".into())
            .spawn(move || {
                // Tight drain loop: process all ready jobs before blocking again.
                loop {
                    let first = match write_rx.recv() {
                        Ok(p) => p,
                        Err(_) => break,
                    };
                    let mut batch = vec![first];
                    while let Ok(p) = write_rx.try_recv() {
                        batch.push(p);
                        if batch.len() >= 32 {
                            break;
                        }
                    }
                    // Multi-block Class A mega-batch: one plan + put_batch per
                    // table across up to 32 prepared bodies (inputs ∥ outputs).
                    let refs: Vec<_> = batch
                        .iter()
                        .map(|p| (&p.header, p.txs.as_slice()))
                        .collect();
                    match write_hub.query.archive_prepared_batch(&refs) {
                        Ok(fks) => {
                            for (prep, _fk) in batch.into_iter().zip(fks) {
                                if write_result
                                    .send(ArchiveResult::Ok { hash: prep.hash })
                                    .is_err()
                                {
                                    return;
                                }
                            }
                        }
                        Err(e) => {
                            // Do not retry one-by-one: a mid-batch failure may have
                            // already appended txs/I/O without header_txs (no has_body
                            // yet); re-archiving would double-append Class A rows.
                            let err = e.to_string();
                            for prep in batch {
                                if write_result
                                    .send(ArchiveResult::Err {
                                        hash: prep.hash,
                                        err: err.clone(),
                                    })
                                    .is_err()
                                {
                                    return;
                                }
                            }
                        }
                    }
                }
            })
            .expect("spawn archive writer");

        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(n_prep)
            .thread_name(|i| format!("ibd-archive-prep-{i}"))
            .build()
            .expect("archive prep pool");

        // In-flight prep permits so we do not unboundedly queue rayon tasks.
        let sem = Arc::new(Semaphore::new(n_prep.saturating_mul(2).max(4)));
        let mut inflight = Vec::new();

        while let Some(job) = job_rx.recv().await {
            let permit = match sem.clone().acquire_owned().await {
                Ok(p) => p,
                Err(_) => break,
            };
            let hub = hub.clone();
            let write_tx = write_tx.clone();
            let result_tx = result_tx.clone();
            let (done_tx, done_rx) = tokio::sync::oneshot::channel::<()>();
            inflight.push(done_rx);

            pool.spawn(move || {
                let _permit = permit;
                let hash = job.block.block_hash();
                match prepare_block_for_archive(&hub.query, &hub.params, &job.block) {
                    Ok((header, txs)) => {
                        if write_tx
                            .send(PreparedArchive { hash, header, txs })
                            .is_err()
                        {
                            let _ = result_tx.send(ArchiveResult::Err {
                                hash,
                                err: "writer closed".into(),
                            });
                        }
                    }
                    Err(e) => {
                        let _ = result_tx.send(ArchiveResult::Err {
                            hash,
                            err: e.to_string(),
                        });
                    }
                }
                let _ = done_tx.send(());
            });

            // Reap completed oneshots without blocking the intake loop.
            let mut i = 0;
            while i < inflight.len() {
                match inflight[i].try_recv() {
                    Err(tokio::sync::oneshot::error::TryRecvError::Empty) => i += 1,
                    _ => {
                        inflight.swap_remove(i);
                    }
                }
            }
        }

        // Wait for remaining prep tasks.
        for rx in inflight {
            let _ = rx.await;
        }
        drop(write_tx);
        // Dropping pool waits for spawned prep jobs.
        drop(pool);
        let _ = writer.join();
    })
}

/// Assign getdata for missing (not yet **archived**) hashes in chain order.
///
/// Far-ahead thrash is prevented by **horizon only**: solely
/// `ordered[0..horizon]` may be requested. Bodies are archived on arrival;
/// the tip confirm path is separate and does not block further downloads.
fn assign_work_ordered(
    slots: &mut [PeerSlot],
    ordered: &VecDeque<BlockHash>,
    inflight: &mut HashMap<BlockHash, (usize, Instant)>,
    hub: &ChainHub,
    cfg: &IbdConfig,
    horizon: usize,
) {
    let mut room = cfg.window.saturating_sub(inflight.len());
    if room == 0 {
        return;
    }

    let alive_ids: Vec<usize> = slots
        .iter()
        .filter(|s| s.alive)
        .map(|s| s.id)
        .collect();
    if alive_ids.is_empty() {
        return;
    }

    // Holes: not confirmed, not archived, not inflight.
    let mut candidates: VecDeque<BlockHash> = ordered
        .iter()
        .take(horizon)
        .copied()
        .filter(|h| {
            !hub.has_block(h) && !hub.is_archived(h) && !inflight.contains_key(h)
        })
        .collect();
    if candidates.is_empty() {
        return;
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
            // Core-style: hard per-peer in-transit cap (not window/n).
            let peer_cap = cfg.per_peer;
            let free = peer_cap.saturating_sub(slot.in_flight.len());
            if free == 0 {
                continue;
            }
            // One getdata batch stays within remaining peer + global room.
            let batch_cap = cfg.per_peer;
            let take = free.min(room).min(candidates.len()).min(batch_cap);
            if take == 0 {
                continue;
            }
            let mut batch = Vec::with_capacity(take);
            while batch.len() < take {
                let Some(h) = candidates.pop_front() else {
                    break;
                };
                if hub.has_block(&h)
                    || hub.is_archived(&h)
                    || inflight.contains_key(&h)
                {
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
    // Clear stall state so assign_work_ordered will re-issue getdata
    // (tip-next is re-prioritized there).
    let mut n = 0usize;
    for (h, pid) in stalled {
        if inflight.remove(&h).is_some() {
            if let Some(s) = slots.iter_mut().find(|s| s.id == pid) {
                s.in_flight.remove(&h);
            }
            n += 1;
        }
    }
    // Avoid log spam (lab25 had hundreds of reassign lines drowning signal).
    if n > 0 && n >= 8 {
        warn!("ibd: reassign {n} stalled block(s)");
    }
}

/// Max tip confirms per event-loop turn (Class C is sequential; keep peers busy).
const CONNECT_BATCH: usize = 128;

/// Mark `h` done in the ordered set. Prefer O(1) front pop; otherwise leave a
/// stale ordered entry (front-trim via `has_block` / set membership skips it).
fn remove_from_ordered(
    ordered: &mut VecDeque<BlockHash>,
    ordered_set: &mut HashSet<BlockHash>,
    h: BlockHash,
) {
    ordered_set.remove(&h);
    if ordered.front() == Some(&h) {
        ordered.pop_front();
    }
    while let Some(&front) = ordered.front() {
        if ordered_set.contains(&front) {
            break;
        }
        ordered.pop_front();
    }
}

/// Confirm up to `budget` tip-extending blocks whose bodies are already archived.
fn confirm_run(
    hub: &ChainHub,
    ordered: &mut VecDeque<BlockHash>,
    ordered_set: &mut HashSet<BlockHash>,
    hash_height: &HashMap<BlockHash, u32>,
    accepted: &AtomicU32,
    start_tip: u32,
    budget: usize,
) -> Result<usize, NetError> {
    if budget == 0 {
        return Ok(0);
    }
    tokio::task::block_in_place(|| {
        confirm_run_sync(
            hub,
            ordered,
            ordered_set,
            hash_height,
            accepted,
            start_tip,
            budget,
        )
    })
}

fn confirm_run_sync(
    hub: &ChainHub,
    ordered: &mut VecDeque<BlockHash>,
    ordered_set: &mut HashSet<BlockHash>,
    hash_height: &HashMap<BlockHash, u32>,
    accepted: &AtomicU32,
    start_tip: u32,
    budget: usize,
) -> Result<usize, NetError> {
    let mut connected = 0usize;
    while connected < budget {
        while let Some(&front) = ordered.front() {
            if hub.has_block(&front) || !ordered_set.contains(&front) {
                ordered.pop_front();
                ordered_set.remove(&front);
            } else {
                break;
            }
        }
        let Some(&need) = ordered.front() else {
            break;
        };
        if !hub.is_archived(&need) {
            break;
        }
        let expect = hub.tip_height().map(|t| t.saturating_add(1)).unwrap_or(0);
        let height = hash_height.get(&need).copied().unwrap_or(expect);
        if height != expect {
            // Prefer tip-linked height; map can drift after reorg/reset.
            // Still confirm at expect if archive has the body.
        }
        match hub.confirm_hash(expect, need) {
            Ok(AcceptOutcome::Accepted { .. }) | Ok(AcceptOutcome::AlreadyHave) => {
                remove_from_ordered(ordered, ordered_set, need);
                connected += 1;
                let n = accepted.fetch_add(1, Ordering::SeqCst) + 1;
                if n == 1 || n % 100 == 0 {
                    info!(
                        "ibd: progress tip={:?} (+{n} parallel, started {start_tip})",
                        hub.tip_height()
                    );
                    let _ = std::io::Write::flush(&mut std::io::stderr());
                }
            }
            Ok(AcceptOutcome::IgnoredWeaker) => {
                remove_from_ordered(ordered, ordered_set, need);
            }
            Err(e) => {
                warn!("ibd: confirm reject {need} @ {expect}: {e}");
                break;
            }
        }
    }
    Ok(connected)
}
