//! Concurrent multi-peer block download (libbitcoin-class windowed IBD).
//!
//! Architecture:
//! - N outbound peer workers (each: own TCP stream, command + event channels)
//! - Shared ordered work queue of block hashes after the local tip
//! - **In-flight cap** (`window`): max concurrent unique getdata; **not** a tip-distance
//!   limit — any unarchived hash on the known header path may be requested so archive
//!   can race to the end of headers while tip waits on holes
//! - Tip-hole hashes race 2 peers immediately; a 3rd only after 10s without body
//! - Near/far densify stay single-peer; at most `per_peer` blocks per peer (Core = 16)
//! - Peers send `getaddr`; learned addrs grow the redial pool beyond seeds
//! - Archive pipeline: dedicated OS **prep** thread → dedicated OS **writer** thread
//! - Tip **confirm** walks contiguous archived runs (Class C) on a dedicated thread
//! - Peer IO: concurrent read/write halves; **frame-only** on socket tasks, block
//!   decode on Tokio blocking pool (never multi-MB deserialize on async workers)
//! - Stall: 30s with no block-download progress on a peer → disconnect + cooldown

mod archive;
mod assign_plan;
mod body;
mod coalesce;
mod confirm;
mod dial;
mod exit;
mod peer_io;
mod perf_log;
mod prewarm;
mod progress;
mod run_materialize;
mod state;

#[cfg(test)]
mod freeze_benches;

use archive::{
    spawn_archive_pipeline, ArchiveJob, ArchivePipelineStats, ArchiveQueueBudget, ArchiveResult,
};
use assign_plan::{classify_height, far_slots_per_peer, remove_from_ordered, want_headers_beyond_soft_cap, WorkClass};
// compact_ordered used via IbdWorkState::hygiene
use confirm::{offer_confirm_ready, spawn_confirm_engine, ConfirmEvent, ConfirmFeed};
use dial::{
    apply_dial_result, dial_batch, dial_blocked_addrs, disconnect_stalled_block_peers,
    expire_addr_cooldown, release_peer_block_work, request_headers, request_headers_from,
};
use peer_io::{
    note_block_progress, note_block_rx, touch_block_progress, PeerCmd, PeerEvent, PeerEventSinks,
    PeerSlot,
};
use crate::seeds::AddrMan;
use exit::{
    all_peers_dead_action, catchup_complete_after_drain, header_lag_behind_peers, path_drained,
    peer_caught_up, AllPeersDead,
};
use prewarm::{spawn_parent_prewarm, PrewarmControl};
use progress::{ibd_pct, work_chain_progress};
use state::IbdWorkState;

use crate::chain::ChainHub;
use crate::codec::MAX_HEADERS_RESULTS;
use crate::error::NetError;
use bitcoin::hashes::Hash;
use bitcoin::p2p::Magic;
use bitcoin::BlockHash;
use std::collections::{HashMap, HashSet, VecDeque};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use rbitcoin_log::{info, warn};

/// Main-loop hot-path timers (atomics; reset every status sample).
///
/// Used to see whether the pegged core is Class C confirm, getdata assign,
/// event drain, or the status scan itself.
///
/// **Live confirm:** `confirm_ns` only accrues when a batch **finishes**. During a
/// multi-second `confirm_run`, [`Self::confirm_live`] is set so status can show
/// in-progress wall + phase counters (which *do* tick mid-batch).
pub(crate) struct LoopStats {
    /// Wall time in confirm engine (`hub.confirm_hash`) for **completed** batches.
    pub(crate) confirm_ns: AtomicU64,
    /// Successful tip accepts this window.
    pub(crate) confirm_blocks: AtomicU64,
    /// Times confirm stopped on a non-skippable reject.
    pub(crate) confirm_reject_stops: AtomicU64,
    /// Wall time in `assign_work_ordered`.
    pub(crate) assign_ns: AtomicU64,
    /// Unique hashes put into getdata this window.
    pub(crate) assign_issued: AtomicU64,
    /// Wall time draining peer/archive channels.
    pub(crate) drain_ns: AtomicU64,
    /// Peer + archive events applied this window.
    pub(crate) drain_events: AtomicU64,
    /// Wall time building the status `work_chain_progress` snapshot.
    pub(crate) status_scan_ns: AtomicU64,
    /// Class A bodies published this run (any height; cumulative).
    /// Includes gap-fills below the archive high-water mark — not just HWM rises.
    pub(crate) archived_bodies: AtomicU64,
    /// In-flight confirm batch (set by confirm OS thread; status only).
    confirm_live: Mutex<Option<ConfirmLive>>,
}

#[derive(Clone, Copy, Debug)]
struct ConfirmLive {
    first_height: u32,
    batch_n: u32,
    started: Instant,
}

impl Default for LoopStats {
    fn default() -> Self {
        Self {
            confirm_ns: AtomicU64::new(0),
            confirm_blocks: AtomicU64::new(0),
            confirm_reject_stops: AtomicU64::new(0),
            assign_ns: AtomicU64::new(0),
            assign_issued: AtomicU64::new(0),
            drain_ns: AtomicU64::new(0),
            drain_events: AtomicU64::new(0),
            status_scan_ns: AtomicU64::new(0),
            archived_bodies: AtomicU64::new(0),
            confirm_live: Mutex::new(None),
        }
    }
}

impl LoopStats {
    pub(crate) fn confirm_begin(&self, first_height: u32, batch_n: u32) {
        *self.confirm_live.lock().unwrap() = Some(ConfirmLive {
            first_height,
            batch_n,
            started: Instant::now(),
        });
    }

    pub(crate) fn confirm_end(&self) {
        *self.confirm_live.lock().unwrap() = None;
    }

    /// `(first_height, batch_n, elapsed_ms)` if a confirm batch is running.
    pub(crate) fn confirm_live_snap(&self) -> Option<(u32, u32, u64)> {
        self.confirm_live.lock().unwrap().as_ref().map(|l| {
            (
                l.first_height,
                l.batch_n,
                l.started.elapsed().as_millis() as u64,
            )
        })
    }

    pub(crate) fn sample_and_reset(&self) -> LoopSample {
        LoopSample {
            confirm_ns: self.confirm_ns.swap(0, Ordering::Relaxed),
            confirm_blocks: self.confirm_blocks.swap(0, Ordering::Relaxed),
            confirm_reject_stops: self.confirm_reject_stops.swap(0, Ordering::Relaxed),
            assign_ns: self.assign_ns.swap(0, Ordering::Relaxed),
            assign_issued: self.assign_issued.swap(0, Ordering::Relaxed),
            drain_ns: self.drain_ns.swap(0, Ordering::Relaxed),
            drain_events: self.drain_events.swap(0, Ordering::Relaxed),
            status_scan_ns: self.status_scan_ns.swap(0, Ordering::Relaxed),
            confirm_live: self.confirm_live_snap(),
        }
    }
}

pub(crate) struct LoopSample {
    pub(crate) confirm_ns: u64,
    pub(crate) confirm_blocks: u64,
    pub(crate) confirm_reject_stops: u64,
    pub(crate) assign_ns: u64,
    pub(crate) assign_issued: u64,
    pub(crate) drain_ns: u64,
    pub(crate) drain_events: u64,
    pub(crate) status_scan_ns: u64,
    /// Live batch if confirm engine is mid-`confirm_run`.
    pub(crate) confirm_live: Option<(u32, u32, u64)>,
}

impl LoopSample {
    fn ms(ns: u64) -> u64 {
        ns / 1_000_000
    }
    pub(crate) fn confirm_ms(&self) -> u64 {
        Self::ms(self.confirm_ns)
    }
    pub(crate) fn assign_ms(&self) -> u64 {
        Self::ms(self.assign_ns)
    }
    pub(crate) fn drain_ms(&self) -> u64 {
        Self::ms(self.drain_ns)
    }
    pub(crate) fn status_scan_ms(&self) -> u64 {
        Self::ms(self.status_scan_ns)
    }
    pub(crate) fn confirm_us_per_block(&self) -> u64 {
        if self.confirm_blocks == 0 {
            0
        } else {
            (self.confirm_ns / self.confirm_blocks) / 1000
        }
    }
    /// Which phase dominated wall time this window (for one-glance diagnosis).
    pub(crate) fn dominant(&self) -> &'static str {
        // Completed-batch timer is 0 while a batch is still running — treat live as confirm.
        if self.confirm_live.is_some() && self.confirm_ns == 0 {
            return "confirm";
        }
        let c = self.confirm_ns;
        let a = self.assign_ns;
        let d = self.drain_ns;
        let s = self.status_scan_ns;
        let m = c.max(a).max(d).max(s);
        if m == 0 {
            "idle"
        } else if m == c {
            "confirm"
        } else if m == a {
            "assign"
        } else if m == d {
            "drain"
        } else {
            "status_scan"
        }
    }
}

/// Default max **concurrent** unique block downloads (in-flight getdata).
///
/// Not a tip-distance ceiling: archive may run to the end of the known header
/// path; this only limits how many bodies we pull at once (backpressure + RAM).
pub const DEFAULT_IBD_WINDOW: usize = 1024;

/// Soft cap on `ordered` length while still requesting more headers.
/// Keeps header sync from unbounded growth if peers never signal done; large
/// enough for full signet / long mainnet catch-up in one run.
/// Hard ceiling on the ordered work path (memory / hygiene bound).
const MAX_ORDERED_HEADERS: usize = 500_000;
/// Soft cap: stop **requesting** more headers once we have this many on the path.
/// Multi-peer getheaders while `ordered` was 100k–500k flooded the main loop with
/// expensive Headers events (drain livelock → multi-minute freezes, getdata starved).
/// ~64k is ample runway for window=1024 archive race + tip holes.
const ORDERED_HEADERS_SOFT_CAP: usize = 64_000;

/// Max blocks in flight to a single peer (Core `MAX_BLOCKS_IN_TRANSIT_PER_PEER`).
///
/// Keeping this at 16 avoids overloading peers with large getdata batches; total
/// concurrency scales with peer count (`peers × 16`), not by piling work on few hosts.
pub const DEFAULT_BLOCKS_IN_TRANSIT_PER_PEER: usize = 16;

/// Near band: tip+1 ..= tip+N (confirm runway + bulk near assign).
const NEAR_DEPTH: u32 = 4096;
/// Max contiguous tip+1.. holes to cover per assign.
const TIP_HOLE_MAX: usize = 32;
/// Max concurrent getdata peers for one tip-hole hash.
pub(crate) const TIP_HOLE_MAX_PEERS: usize = 3;
/// Immediate tip-hole race size (first + second peer).
pub(crate) const TIP_HOLE_IMMEDIATE_PEERS: usize = 2;
/// After the second tip-hole peer is issued, wait this long before a third.
pub(crate) const TIP_HOLE_THIRD_PEER_AFTER: Duration = Duration::from_secs(10);
/// Cap on IBD dial pool after getaddr learning (seeds + discovered).
const MAX_PEER_POOL: usize = 256;
/// Pending (framed, not Class A) longer than this → re-getdata.
const PENDING_STALE: Duration = Duration::from_secs(45);
/// Max hashes per far getdata batch.
const FAR_BATCH_MAX: usize = 16;
/// Cap height scan for far candidates per assign tick.
const FAR_SCAN_BUDGET: usize = 16_384;

/// Tunables for IBD (defaults lean libbitcoin/Core-ish).
#[derive(Clone, Debug)]
pub struct IbdConfig {
    /// Max concurrent unique block getdata (in-flight). Not tip-distance.
    pub window: usize,
    /// Hard cap on outstanding block getdata to one peer (Core = 16).
    pub per_peer: usize,
    /// Desired number of live download peers; we keep redialing until we reach this
    /// (or exhaust the candidate pool).
    pub target_peers: usize,
    /// Disconnect a peer (and reassign its getdata) if it has outstanding block
    /// requests and no block-download progress for this long.
    pub stall: Duration,
    /// Max headers to request per getheaders round-trip.
    pub headers_batch: usize,
    /// TCP connect + handshake timeout per peer.
    pub connect_timeout: Duration,
    /// Optional shared peer book (discovered addrs + flags). Seeded at start and
    /// written back on IBD exit so the node can persist across runs.
    pub peers: Option<std::sync::Arc<std::sync::Mutex<crate::seeds::AddrMan>>>,
}

impl Default for IbdConfig {
    fn default() -> Self {
        Self {
            window: DEFAULT_IBD_WINDOW,
            per_peer: DEFAULT_BLOCKS_IN_TRANSIT_PER_PEER,
            target_peers: crate::DEFAULT_IBD_TARGET_PEERS as usize,
            stall: Duration::from_secs(30),
            headers_batch: MAX_HEADERS_RESULTS,
            connect_timeout: Duration::from_secs(8),
            peers: None,
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
            peers: None,
        }
    }
}

/// Local IBD peer book that flushes back into [`IbdConfig::peers`] on drop.
struct PeerBookSession {
    book: crate::seeds::AddrMan,
    shared: Option<std::sync::Arc<std::sync::Mutex<crate::seeds::AddrMan>>>,
}

impl PeerBookSession {
    fn new(
        shared: Option<std::sync::Arc<std::sync::Mutex<crate::seeds::AddrMan>>>,
        seed_peers: &[SocketAddr],
    ) -> Self {
        let mut book = if let Some(ref s) = shared {
            s.lock().unwrap_or_else(|e| e.into_inner()).clone()
        } else {
            crate::seeds::AddrMan::new()
        };
        book.inject(seed_peers.iter().copied());
        Self { book, shared }
    }

    fn book(&self) -> &crate::seeds::AddrMan {
        &self.book
    }

    fn book_mut(&mut self) -> &mut crate::seeds::AddrMan {
        &mut self.book
    }

    fn flush(&self) {
        if let Some(ref s) = self.shared {
            if let Ok(mut g) = s.lock() {
                *g = self.book.clone();
            }
        }
    }
}

impl Drop for PeerBookSession {
    fn drop(&mut self) {
        self.flush();
    }
}


pub async fn ibd(
    hub: Arc<ChainHub>,
    magic: Magic,
    local_addr: SocketAddr,
    peers: &[SocketAddr],
    cfg: IbdConfig,
) -> Result<u32, NetError> {
    ibd_cancellable(hub, magic, local_addr, peers, cfg, None).await
}

/// Like [`ibd`], with an optional cancel flag polled each loop turn.
pub async fn ibd_cancellable(
    hub: Arc<ChainHub>,
    magic: Magic,
    local_addr: SocketAddr,
    peers: &[SocketAddr],
    cfg: IbdConfig,
    cancel: Option<std::sync::Arc<std::sync::atomic::AtomicBool>>,
) -> Result<u32, NetError> {
    if peers.is_empty() {
        return Err(NetError::Protocol("no peers for ibd"));
    }
    let cancelled = || {
        cancel
            .as_ref()
            .map(|c| c.load(std::sync::atomic::Ordering::SeqCst))
            .unwrap_or(false)
    };

    // Genesis must exist so getheaders locator is real and blocks link to tip.
    hub.ensure_genesis()?;

    // Dual channels: body path (framed/decoded blocks) never waits behind Headers.
    let (body_tx, mut body_rx) = mpsc::unbounded_channel::<PeerEvent>();
    let (ctrl_tx, mut ctrl_rx) = mpsc::unbounded_channel::<PeerEvent>();
    let sinks = PeerEventSinks {
        body: body_tx,
        ctrl: ctrl_tx,
    };

    // Peer TCP tasks share the node multi-thread runtime. Heavy CPU (block
    // deserialize, archive prep, confirm scripts) runs off those workers
    // (blocking pool / dedicated OS threads) so socket tasks stay schedulable.
    // A second nested `ibd-net` runtime was removed: it **panicked on SIGINT**
    // when the outer select dropped this future (`Cannot drop a runtime in an
    // async context`).
    {
        let workers = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1);
        info!(
            "ibd: tokio worker threads≈{workers} (peer decode: blocking pool; archive: 1 OS prep + 1 OS writer; confirm: 1 OS thread)"
        );
    }
    // Dial book: persisted peers (flags) + seeds/connect, ranked by PeerFlags.
    let mut peer_sess = PeerBookSession::new(cfg.peers.clone(), peers);
    let next_peer_id = Arc::new(AtomicUsize::new(0));

    // Initial concurrent dial — cap to ~2× live target (never the whole book).
    // With DNS/peers persistence the book can be 300+ addresses; dialing them all
    // at once saturates FDs and yields 100+ "ready" slots that immediately die.
    let initial_dial_n = cfg
        .target_peers
        .saturating_mul(2)
        .max(peers.len())
        .min(peer_sess.book().len())
        .max(1);
    let initial = dial_batch(
        peer_sess.book(),
        &next_peer_id,
        initial_dial_n,
        HashSet::new(),
        magic,
        local_addr,
        hub.tip_height(),
        sinks.clone(),
        cfg.connect_timeout,
        cancel.as_ref().map(Arc::clone),
    )
    .await;
    apply_dial_result(peer_sess.book_mut(), &initial);
    let mut initial_slots = initial.slots;
    if cancelled() {
        warn!("ibd: cancel during initial dial — stopping");
        for s in &initial_slots {
            let _ = s.cmd_tx.send(PeerCmd::Shutdown);
            s.task.abort();
        }
        return Ok(0);
    }
    initial_slots.retain(|s| s.alive);
    if initial_slots.is_empty() {
        return Err(NetError::Protocol("no peers connected"));
    }
    let init_peer_tip = initial_slots.iter().map(|s| s.peer_height).max().unwrap_or(0);
    info!(
        "ibd: {} / {} peers ready (target={}, book={}, max_peer_height={})",
        initial_slots.len(),
        peers.len(),
        cfg.target_peers,
        peer_sess.book().len(),
        init_peer_tip
    );
    // Background redial — never .await dial on the IBD event loop (that stalled tip).
    let mut last_redial = Instant::now() - Duration::from_secs(15);
    let mut redial_handle: Option<JoinHandle<dial::DialBatchResult>> = None;
    // Consecutive redial batches that added zero peers (network-down / pool dead).
    let mut dark_redial_empty: u32 = 0;

    let accepted = Arc::new(AtomicU32::new(0));
    let start_tip = hub.tip_height().unwrap_or(0);
    let max_archived_shared = Arc::new(AtomicU32::new(start_tip));
    let confirm_lag = Arc::new(AtomicU32::new(0));
    let mut last_progress = Instant::now();
    let mut last_status = Instant::now();
    let mut last_progress_log = Instant::now() - Duration::from_secs(2);
    // Last tip we emitted on an INFO progress line.
    let mut last_logged_tip = start_tip;
    // Max concurrent unique downloads (not tip-distance).
    let window = cfg.window;

    let mut st = IbdWorkState::new(initial_slots, hub.tip_hash(), hub.tip_height());
    // Reload post-tip headers + Class A from disk so restart does not re-getdata
    // bodies that are already archived (ordered path was process-local only).
    seed_work_path_from_store(&mut st, hub.as_ref());

    // Kick header sync — try a few peers (channel may close if handshake race).
    for _ in 0..st.slots.len().min(4) {
        let tips = work_path_tips(&st);
        if request_headers(&st.slots, &hub, &mut st.header_req_seq, &tips).unwrap_or(false) {
            break;
        }
    }

    // Archive pipeline: multi-core prep + exclusive writer (mmap Class A).
    // Byte budget (~1 GiB default via RBITCOIN_ARCHIVE_QUEUE_MB) caps decoded
    // blocks waiting for archive; getdata pauses when full (no drop).
    let archive_queued = ArchiveQueueBudget::from_env();
    info!(
        "ibd: archive queue budget={} MiB (RBITCOIN_ARCHIVE_QUEUE_MB)",
        archive_queued.budget_bytes() / (1024 * 1024)
    );
    let pipe_stats = Arc::new(ArchivePipelineStats::default());
    let loop_stats = Arc::new(LoopStats::default());
    // Seed arch_total from durable Class A on disk (not zero at restart).
    let store_arch_total = hub.query.archived_block_count().unwrap_or(0);
    loop_stats
        .archived_bodies
        .store(store_arch_total, Ordering::Relaxed);
    let mut last_logged_arch_total = store_arch_total;
    if store_arch_total > 0 {
        info!("ibd: store has {store_arch_total} Class A bodies (arch_total seed)");
    }
    let (arch_job_tx, arch_job_rx) = mpsc::unbounded_channel::<ArchiveJob>();
    let (arch_res_tx, mut arch_res_rx) = mpsc::unbounded_channel::<ArchiveResult>();
    let archive_stop = Arc::new(AtomicBool::new(false));
    let mut pipeline = spawn_archive_pipeline(
        hub.clone(),
        arch_job_rx,
        arch_res_tx,
        Arc::clone(&pipe_stats),
        Arc::clone(&archive_queued),
        Arc::clone(&confirm_lag),
        Arc::clone(&archive_stop),
    );

    // Catch-up runs → open-hash materialize under archive hysteresis (idle IO).
    let mut run_materialize_worker =
        Some(run_materialize::RunMaterializeWorker::spawn(Arc::clone(&hub.query)));

    // Fresh cancel state for this IBD session (may have been set on prior stop).
    hub.query.clear_confirm_cancel();

    // Parent prewarm: high-prio runway worker loads/pins parents for tip+1…tip+1k
    // so confirm wave-fill hits RAM instead of contending with archive I/O.
    let prewarm_ctrl = Arc::new(PrewarmControl::new());
    let mut prewarm_join = Some(spawn_parent_prewarm(
        Arc::clone(&hub.query),
        Arc::clone(&prewarm_ctrl),
    ));

    // Dedicated confirm path — never blocks the network/archive event loop.
    let confirm_feed = Arc::new(ConfirmFeed::new());
    // Unbounded: SyncSender(512) deadlocked the confirm OS thread when the main
    // loop lagged on header drain (send blocks → tip frozen, hole=0, confirm_blks=0).
    let (confirm_ev_tx, confirm_ev_rx) = std::sync::mpsc::channel::<ConfirmEvent>();
    let confirm_engine = spawn_confirm_engine(
        hub.clone(),
        Arc::clone(&confirm_feed),
        confirm_ev_tx,
        Arc::clone(&accepted),
        Arc::clone(&loop_stats),
    );
    // Seed engine with any bodies already on disk for the work path.
    offer_confirm_ready(
        &confirm_feed,
        &st.height_to_hash,
        &mut st.body,
        hub.as_ref(),
        &mut st.max_archived_height,
        &max_archived_shared,
    );
    update_confirm_lag(&confirm_lag, hub.tip_height(), st.max_archived_height);

    let mut loop_n = 0u32;
    loop {
        if cancelled() {
            warn!("ibd: cancel requested — stopping IBD");
            break;
        }
        // Yield occasionally so shutdown can run; every-tick yield_now burned
        // scheduler time while confirm already saturates cores.
        loop_n = loop_n.wrapping_add(1);
        if loop_n % 8 == 0 {
            tokio::task::yield_now().await;
        }

        // Confirm results first — keep tip moving even when peer header floods
        // dominate drain budget (and free ordered front for the next offer).
        while let Ok(ev) = confirm_ev_rx.try_recv() {
            match ev {
                ConfirmEvent::Accepted { hash } => {
                    last_progress = Instant::now();
                    remove_from_ordered(&mut st.ordered, &mut st.ordered_set, hash);
                    st.body.mark_archived(hash);
                }
                ConfirmEvent::Reject { height, hash, err } => {
                    apply_confirm_reject(&mut st, height, hash, &err);
                }
            }
        }

        // Drain all ready peer/archive events **before** stall checks.
        if !drain_ready_peer_and_archive_events(
            &mut st,
            hub.as_ref(),
            &mut body_rx,
            &mut ctrl_rx,
            &mut arch_res_rx,
            &arch_job_tx,
            &archive_queued,
            &loop_stats,
            peer_sess.book_mut(),
            local_addr,
        )? {
            break;
        }

        // Drop already-confirmed / past-tip prefixes from the ordered queue.
        let tip_now = hub.tip_height().unwrap_or(0);
        while let Some(&front) = st.ordered.front() {
            let past = hub.has_block(&front)
                || st
                    .hash_height
                    .get(&front)
                    .is_some_and(|&ht| ht <= tip_now);
            if past {
                st.ordered.pop_front();
                st.ordered_set.remove(&front);
            } else {
                break;
            }
        }
        // Compact ghosts + bound hash_height / header_fks (see IbdWorkState::hygiene).
        st.hygiene();

        // NETWORK FIRST: top up getdata before Class C confirm burns the turn.
        // When arch RAM budget is full, still densify **tip-near** so confirm has
        // runway (mid-sync was inflight=0 while arch_q sat at cap).
        // Hysteresis materialize: stop **new** peer fetches (writer keeps draining).
        let arch_bytes = archive_queued.bytes();
        let scope = if rbitcoin_query::run_materialize_control::should_pause_peer_fetch() {
            AssignScope::None
        } else if archive_queued.has_room() {
            AssignScope::Full
        } else {
            AssignScope::TipNearOnly
        };
        assign_work_ordered(
            &mut st,
            hub.as_ref(),
            &cfg,
            &loop_stats,
            arch_bytes,
            archive_queued.budget_bytes(),
            scope,
        );

        // Offer archived bodies to the dedicated confirm engine (non-blocking).
        offer_confirm_ready(
            &confirm_feed,
            &st.height_to_hash,
            &mut st.body,
            hub.as_ref(),
            &mut st.max_archived_height,
            &max_archived_shared,
        );
        update_confirm_lag(&confirm_lag, hub.tip_height(), st.max_archived_height);
        // Apply confirm results without doing Class C on this task.
        while let Ok(ev) = confirm_ev_rx.try_recv() {
            match ev {
                ConfirmEvent::Accepted { hash } => {
                    last_progress = Instant::now();
                    remove_from_ordered(&mut st.ordered, &mut st.ordered_set, hash);
                    st.body.mark_archived(hash);
                }
                ConfirmEvent::Reject { height, hash, err } => {
                    apply_confirm_reject(&mut st, height, hash, &err);
                }
            }
        }
        // Progress may have arrived (peer IO is concurrent).
        if !drain_ready_peer_and_archive_events(
            &mut st,
            hub.as_ref(),
            &mut body_rx,
            &mut ctrl_rx,
            &mut arch_res_rx,
            &arch_job_tx,
            &archive_queued,
            &loop_stats,
            peer_sess.book_mut(),
            local_addr,
        )? {
            break;
        }
        // Immediate re-top-up after Block events freed st.inflight during confirm.
        let arch_bytes2 = archive_queued.bytes();
        let scope2 = if rbitcoin_query::run_materialize_control::should_pause_peer_fetch() {
            AssignScope::None
        } else if archive_queued.has_room() {
            AssignScope::Full
        } else {
            AssignScope::TipNearOnly
        };
        assign_work_ordered(
            &mut st,
            hub.as_ref(),
            &cfg,
            &loop_stats,
            arch_bytes2,
            archive_queued.budget_bytes(),
            scope2,
        );

        // Header sync: soft-cap live work (`ordered_set`), not deque len (ghosts).
        //
        // Sparse far-only archives used to push max_archived ≈ max_ordered while
        // most bodies were still missing. That made `arch_runway` look empty and
        // **bypassed the soft cap forever** → header floods, drain livelock,
        // arch_q=0 / writer idle, ~5–10 unique Class A bodies/s despite high BW.
        // Only bypass soft cap when the ordered path is **mostly archived** (dense).
        {
            let live = st.ordered_set.len();
            let known_arch = st.body.known_len();
            let arch_runway = st
                .max_ordered_height
                .saturating_sub(st.max_archived_height);
            let need_arch_runway = want_headers_beyond_soft_cap(
                live,
                known_arch,
                arch_runway,
                (window as u32).saturating_mul(4).max(2048),
            );
            let under_hard = live < MAX_ORDERED_HEADERS;
            let under_soft = live < ORDERED_HEADERS_SOFT_CAP;
            if !st.headers_done && under_hard && (under_soft || need_arch_runway) {
                let tip_h = hub.tip_height().unwrap_or(0);
                let min_runway = window.saturating_mul(8).max(4096);
                let want_more = live == 0
                    || live < min_runway
                    || header_lag_behind_peers(&st, tip_h) > 0
                    || need_arch_runway;
                if want_more {
                    let tips = work_path_tips(&st);
                    // Cold start / empty path: fan getheaders to several peers so a
                    // single silent zombie cannot stall ordered=0 forever.
                    let fan = if live == 0 {
                        st.slots.iter().filter(|s| s.alive).count().min(4).max(1)
                    } else {
                        1
                    };
                    for _ in 0..fan {
                        if !request_headers(&st.slots, &hub, &mut st.header_req_seq, &tips)
                            .unwrap_or(false)
                        {
                            break;
                        }
                    }
                }
            }
        }

        // Hard path reset after a long stall with no tip advance.
        // Do not clear a full st.ordered queue that simply has not finished getdata yet.
        if last_progress.elapsed() > cfg.stall.saturating_mul(6)
            && st.ordered.is_empty()
            && st.inflight.is_empty()
        {
            info!(
                "ibd: hard path reset (stall {:?}, st.ordered empty)",
                last_progress.elapsed()
            );
            st.headers_done = false;
            let tips = work_path_tips(&st);
            let _ = request_headers(&st.slots, &hub, &mut st.header_req_seq, &tips);
            last_progress = Instant::now();
        }

        // Publish confirm runway for the parent-prewarm worker (tip+1 … tip+depth).
        // Seed plans for the full runway so confirm headroom can wait on
        // unfinished heights (not just the last-mile batch).
        {
            let tip = hub.tip_height().unwrap_or(0);
            let arch = st.max_archived_height;
            let depth = hub.query.parent_prewarm_depth();
            let end = tip.saturating_add(depth).min(arch);
            let mut items = Vec::new();
            if end > tip {
                items.reserve((end - tip) as usize);
                for h in (tip + 1)..=end {
                    if let Some(&hash) = st.height_to_hash.get(&h) {
                        items.push((h, hash.to_byte_array()));
                    } else {
                        break; // keep contiguous
                    }
                }
            }
            hub.query.seed_parent_runway(&items);
            prewarm_ctrl.publish(tip, arch, items);
            // Archive hysteresis: pause Class A / materialize runs when lead is huge.
            let arch_lead = arch.saturating_sub(tip);
            let peer_h = st.max_peer_height;
            let archive_at_tip = peer_h > 0
                && arch.saturating_add(2) >= peer_h
                && arch_lead < 128;
            hub.query.publish_run_materialize_control(
                arch_lead,
                archive_at_tip,
                st.inflight.len() as u32,
            );
        }

        // Stall only after progress events are applied.
        let now = Instant::now();
        disconnect_stalled_block_peers(
            &mut st.slots,
            &mut st.inflight,
            &mut st.addr_cooldown,
            now,
            cfg.stall,
        );
        // Drop dead st.slots so we do not keep ghost rows (Drop aborts IO if needed).
        st.slots.retain(|s| s.alive);
        expire_addr_cooldown(&mut st.addr_cooldown, now);

        // Collect finished background dials without blocking the event loop.
        if redial_handle
            .as_ref()
            .map(|h| h.is_finished())
            .unwrap_or(false)
        {
            if let Some(h) = redial_handle.take() {
                match h.await {
                    Ok(result) => {
                        apply_dial_result(peer_sess.book_mut(), &result);
                        let blocked =
                            dial_blocked_addrs(&st.slots, &st.addr_cooldown, Instant::now());
                        let mut n = 0usize;
                        for s in result.slots {
                            // Race: same addr may have connected on another path.
                            if blocked.contains(&s.addr)
                                || st.slots.iter().any(|x| x.addr == s.addr)
                            {
                                warn!(
                                    "ibd: drop duplicate/cooldown dial peer[{}] {}",
                                    s.id, s.addr
                                );
                                continue;
                            }
                            st.max_peer_height = st.max_peer_height.max(s.peer_height);
                            info!(
                                "ibd: peer[{}] {} connected (peer_height={})",
                                s.id, s.addr, s.peer_height
                            );
                            st.slots.push(s);
                            n += 1;
                        }
                        if n > 0 {
                            st.slots.sort_by_key(|s| s.id);
                            info!(
                                "ibd: redial added {n} peer(s); live={}",
                                st.slots.iter().filter(|s| s.alive).count()
                            );
                            dark_redial_empty = 0;
                            // Fresh peers need getheaders when the work path is empty
                            // (tip=0 / mid-chain peer death wiped slots before responses).
                            if st.ordered.is_empty() && !st.headers_done {
                                let tips = work_path_tips(&st);
                                let _ = request_headers(
                                    &st.slots,
                                    &hub,
                                    &mut st.header_req_seq,
                                    &tips,
                                );
                            }
                        } else {
                            dark_redial_empty = dark_redial_empty.saturating_add(1);
                            warn!(
                                "ibd: redial returned 0 peers (empty_rounds={dark_redial_empty})"
                            );
                        }
                    }
                    Err(e) => warn!("ibd: redial task failed: {e}"),
                }
            }
        }

        // Kick a non-blocking redial if under target and none in flight.
        // When *all* peers are dead (network blip), do not wait for the 15s
        // interval — redial immediately so we never race the exit check.
        let alive_n = st.slots.iter().filter(|s| s.alive).count();
        // Target live peers is independent of pool size; getaddr grows the book
        // so we can reach `target_peers` even when seeds alone were sparse.
        let target = cfg.target_peers.max(1);
        let redial_interval = if alive_n == 0 {
            Duration::from_secs(0)
        } else {
            Duration::from_secs(15)
        };
        if redial_handle.is_none()
            && alive_n < target
            && !peer_sess.book().is_empty()
            && last_redial.elapsed() >= redial_interval
        {
            let want = (target - alive_n).min(8).max(1);
            let already = dial_blocked_addrs(&st.slots, &st.addr_cooldown, Instant::now());
            info!(
                "ibd: redialing up to {want} peers (alive={alive_n}/{target}, book={}, blocked={})…",
                peer_sess.book().len(),
                already.len()
            );
            let book = peer_sess.book().clone();
            let next_id = next_peer_id.clone();
            let tip_h = hub.tip_height();
            let sinks_r = sinks.clone();
            let cto = cfg.connect_timeout;
            let cancel_c = cancel.as_ref().map(Arc::clone);
            redial_handle = Some(tokio::spawn(async move {
                dial_batch(
                    &book,
                    &next_id,
                    want,
                    already,
                    magic,
                    local_addr,
                    tip_h,
                    sinks_r,
                    cto,
                    cancel_c,
                )
                .await
            }));
            last_redial = Instant::now();
        }

        // Single INFO progress path (~1/s when tip or archived advanced).
        // Glance line: tip rate, archive lead, tip-hole, peers, prewarm lead.
        // Status every 5s is pipeline health only (`ibd: perf`).
        if last_progress_log.elapsed() >= Duration::from_secs(1) {
            let prog = work_chain_progress(
                hub.as_ref(),
                &st.ordered,
                &st.ordered_set,
                &mut st.body,
                st.max_peer_height,
                st.max_archived_height,
            );
            let tip_delta = prog.tip.saturating_sub(last_logged_tip);
            // Cumulative Class-A body count (any height); +arch is the interval delta.
            let arch_total = loop_stats.archived_bodies.load(Ordering::Relaxed);
            let arch_delta = arch_total.saturating_sub(last_logged_arch_total);
            if tip_delta > 0 || arch_delta > 0 {
                let pct = ibd_pct(prog.tip, prog.headers);
                let secs = last_progress_log.elapsed().as_secs_f64().max(0.001);
                let tip_rate = tip_delta as f64 / secs;
                let arch_rate = arch_delta as f64 / secs;
                let arch_lead = prog.archived.saturating_sub(prog.tip);
                let peers_n = st.slots.iter().filter(|s| s.alive).count();
                let (pw_through, pw_ahead, _pw_parents, _pw_bodies, _plans, _depth) =
                    hub.query.parent_prewarm_perf_snapshot();
                let (tx_r, pt_r, sh_r) = hub.query.index_run_counts();
                let (mat_runs, mat_keys) =
                    rbitcoin_query::run_materialize_control::sample();
                let mat_mode = rbitcoin_query::run_materialize_control::mode_label();
                let pause_fetch =
                    rbitcoin_query::run_materialize_control::should_pause_peer_fetch();
                info!(
                    "ibd: progress {pct}% tip={} ({tip_rate:.0}/s) arch_hwm={} ({arch_rate:.0}/s lead={arch_lead}) hole={} peers={peers_n} prewarm+{pw_ahead} thru={pw_through} runs t={tx_r}/p={pt_r}/sh={sh_r} mat={mat_mode}/pause_fetch={pause_fetch} +runs={mat_runs}/keys={mat_keys} horizon={}",
                    prog.tip,
                    prog.archived,
                    prog.tip_hole,
                    prog.headers,
                );
                last_logged_tip = prog.tip;
                last_logged_arch_total = arch_total;
                last_progress_log = Instant::now();
                let _ = std::io::Write::flush(&mut std::io::stderr());
            } else {
                last_progress_log = Instant::now();
            }
        }
        if last_status.elapsed() > Duration::from_secs(5) {
            let scan_t0 = Instant::now();
            let prog = work_chain_progress(
                hub.as_ref(),
                &st.ordered,
                &st.ordered_set,
                &mut st.body,
                st.max_peer_height,
                st.max_archived_height,
            );
            loop_stats
                .status_scan_ns
                .fetch_add(scan_t0.elapsed().as_nanos() as u64, Ordering::Relaxed);
            let peers_n = st.slots.iter().filter(|s| s.alive).count();
            let peer_cap = peers_n.saturating_mul(cfg.per_peer);
            let inflight_cap = cfg.window.min(peer_cap).max(1);
            let ahead = prog
                .archived
                .saturating_sub(prog.tip)
                .saturating_add(st.inflight.len() as u32);
            let arch_mb = archive_queued.bytes() / (1024 * 1024);
            let arch_budget_mb = archive_queued.budget_bytes() / (1024 * 1024);
            let arch_q_now = archive_queued.count();

            // One sample/reset, then INFO `ibd: perf` (+ DEBUG `ibd: perf_dbg`).
            let utxo_snap = hub.query.ibd_utxo_perf_snapshot();
            let prewarm_snap = hub.query.parent_prewarm_perf_snapshot();
            let perf = perf_log::sample(
                &loop_stats,
                &pipe_stats,
                st.inflight.len(),
                inflight_cap,
                arch_q_now,
                arch_mb,
                arch_budget_mb,
                st.body.pending_len(),
                st.body.known_len(),
                st.ordered.len(),
                ahead,
                prog.tip_hole,
                peers_n,
                st.headers_done,
                utxo_snap,
                prewarm_snap,
            );
            perf_log::log_sample(&perf);

            // Stall watchdog: archive-complete + tip frozen is a confirm-path bug.
            if last_progress.elapsed() > Duration::from_secs(15)
                && prog.tip_hole == 0
                && st.inflight.is_empty()
                && arch_q_now == 0
                && prog.archived > prog.tip.saturating_add(1)
            {
                let expect = prog.tip.saturating_add(1);
                let hth = st.height_to_hash.get(&expect).copied();
                let ready = hth
                    .map(|h| st.body.is_known_archived(&h) || hub.is_archived(&h))
                    .unwrap_or(false);
                let has = hth.map(|h| hub.has_block(&h)).unwrap_or(false);
                let in_set = hth
                    .map(|h| st.ordered_set.contains(&h))
                    .unwrap_or(false);
                let noted = offer_confirm_ready(
                    &confirm_feed,
                    &st.height_to_hash,
                    &mut st.body,
                    hub.as_ref(),
                    &mut st.max_archived_height,
                    &max_archived_shared,
                );
                update_confirm_lag(&confirm_lag, hub.tip_height(), st.max_archived_height);
                warn!(
                    "ibd: tip stall tip={} expect={expect} hth={} ready={ready} has_block={has} in_ordered={in_set} offer_noted={noted} hwm={} ordered_len={} (idle {:?})",
                    prog.tip,
                    hth.is_some(),
                    prog.archived,
                    st.ordered.len(),
                    last_progress.elapsed(),
                );
            }
            last_status = Instant::now();
        }

        // Exit only when the work path is drained **and** we are at (or past)
        // peer-advertised height, or headers_done with no lag. Never exit solely
        // on headers_done while max_peer_height still dwarfs our tip (signet:
        // false headers_done at h≈2000 with peers at ~313k). See `exit` module.
        let tip_h = hub.tip_height().unwrap_or(0);
        let arch_q = archive_queued.count();
        if path_drained(&st, arch_q)
            && (peer_caught_up(&st, tip_h)
                || (st.headers_done && header_lag_behind_peers(&st, tip_h) <= 2))
        {
            offer_confirm_ready(
                &confirm_feed,
                &st.height_to_hash,
                &mut st.body,
                hub.as_ref(),
                &mut st.max_archived_height,
                &max_archived_shared,
            );
            update_confirm_lag(&confirm_lag, hub.tip_height(), st.max_archived_height);
            let tip_h = hub.tip_height().unwrap_or(0);
            let arch_q = archive_queued.count();
            if catchup_complete_after_drain(&st, tip_h, arch_q) {
                info!(
                    "ibd: catch-up complete tip={tip_h} max_peer_height={} max_archived={} headers_done={} — exiting IBD",
                    st.max_peer_height, st.max_archived_height, st.headers_done
                );
                break;
            }
            if header_lag_behind_peers(&st, tip_h) > 2 {
                // Path empty but peers still ahead — resume header sync.
                st.headers_done = false;
                let tips = work_path_tips(&st);
                let _ = request_headers(&st.slots, &hub, &mut st.header_req_seq, &tips);
            }
        }
        // All peers dead — never treat mid-chain peer death as catch-up complete.
        if st.slots.iter().all(|s| !s.alive) {
            let tip_h = hub.tip_height().unwrap_or(0);
            let arch_q = archive_queued.count();
            match all_peers_dead_action(
                &st,
                tip_h,
                arch_q,
                redial_handle.is_some(),
                dark_redial_empty,
            ) {
                AllPeersDead::CatchupComplete => {
                    info!(
                        "ibd: catch-up complete (no live peers) tip={tip_h} max_peer_height={} max_archived={} — exiting IBD",
                        st.max_peer_height, st.max_archived_height
                    );
                    break;
                }
                AllPeersDead::GiveUpMidCatchup => {
                    warn!(
                        "ibd: all peers dead mid catch-up tip={tip_h} max_peer_height={} lag={} accepted={} empty_redials={} — giving up (not tip mode)",
                        st.max_peer_height,
                        header_lag_behind_peers(&st, tip_h),
                        accepted.load(Ordering::SeqCst),
                        dark_redial_empty
                    );
                    return Err(NetError::Protocol(
                        "all peers dead mid catch-up (not complete)",
                    ));
                }
                AllPeersDead::WaitRedial => {
                    // Redial in flight: fall through to select! and wait.
                }
            }
        }

        // Wait for the next peer/archive event or a short tick.
        // Prefer body path (blocks) over headers so delivered bytes are applied first.
        let tick = tokio::time::sleep(Duration::from_millis(50));
        tokio::pin!(tick);
        tokio::select! {
            biased;
            peer_ev = body_rx.recv() => {
                if cancelled() {
                    warn!("ibd: cancel requested — stopping IBD");
                    break;
                }
                let Some(ev) = peer_ev else { break };
                apply_peer_event(
                    &mut st,
                    hub.as_ref(),
                    ev,
                    &arch_job_tx,
                    &archive_queued,
                    peer_sess.book_mut(),
                    local_addr,
                );
            }
            peer_ev = ctrl_rx.recv() => {
                if cancelled() {
                    warn!("ibd: cancel requested — stopping IBD");
                    break;
                }
                let Some(ev) = peer_ev else { break };
                apply_peer_event(
                    &mut st,
                    hub.as_ref(),
                    ev,
                    &arch_job_tx,
                    &archive_queued,
                    peer_sess.book_mut(),
                    local_addr,
                );
            }
            arch = arch_res_rx.recv() => {
                if cancelled() {
                    warn!("ibd: cancel requested — stopping IBD");
                    break;
                }
                let Some(r) = arch else {
                    if !cancelled() {
                        warn!("ibd: archive pipeline ended");
                    }
                    break;
                };
                apply_archive_result(&mut st, r, &archive_queued, &loop_stats);
            }
            _ = &mut tick => {
                if cancelled() {
                    warn!("ibd: cancel requested — stopping IBD");
                    break;
                }
                offer_confirm_ready(
                    &confirm_feed,
                    &st.height_to_hash,
                    &mut st.body,
                    hub.as_ref(),
                    &mut st.max_archived_height,
                    &max_archived_shared,
                );
                update_confirm_lag(&confirm_lag, hub.tip_height(), st.max_archived_height);
                        while let Ok(ev) = confirm_ev_rx.try_recv() {
                    match ev {
                        ConfirmEvent::Accepted { hash } => {
                            last_progress = Instant::now();
                            remove_from_ordered(&mut st.ordered, &mut st.ordered_set, hash);
                            st.body.mark_archived(hash);
                        }
                        ConfirmEvent::Reject { height, hash, err } => {
                            apply_confirm_reject(&mut st, height, hash, &err);
                        }
                    }
                }
                // Stall with an empty work path: only Ok-exit when truly caught up.
                // Previously this bare `break` treated "no progress for 30s at tip=0
                // while peers die" as success → node entered tip mode at height 0.
                if last_progress.elapsed() > cfg.stall
                    && st.ordered.is_empty()
                    && st.inflight.is_empty()
                    && archive_queued.count() == 0
                {
                    let tip_h = hub.tip_height().unwrap_or(0);
                    if catchup_complete_after_drain(&st, tip_h, 0) {
                        info!(
                            "ibd: catch-up complete (stall, path empty) tip={tip_h} max_peer_height={} — exiting IBD",
                            st.max_peer_height
                        );
                        break;
                    }
                    // Mid catch-up: re-kick headers; never silent Ok-exit.
                    warn!(
                        "ibd: stall with empty path tip={tip_h} max_peer_height={} lag={} peers={} — re-request headers (not complete)",
                        st.max_peer_height,
                        header_lag_behind_peers(&st, tip_h),
                        st.slots.iter().filter(|s| s.alive).count()
                    );
                    st.headers_done = false;
                    let tips = work_path_tips(&st);
                    let _ = request_headers(&st.slots, &hub, &mut st.header_req_seq, &tips);
                    last_progress = Instant::now();
                }
            }
        }
    }

    let cancelled_exit = cancelled();
    let t_teardown = Instant::now();

    // 1) Network first: stop downloads + disconnect every peer immediately.
    //    Do this before waiting on confirm/archive so SIGINT is responsive.
    disconnect_all_peers(&mut st);
    if let Some(h) = redial_handle.take() {
        h.abort();
    }
    info!(
        "ibd: peers disconnected in {:?}",
        t_teardown.elapsed()
    );

    // 2) Signal cooperative stops. Confirm cancel aborts prewarm waits so the
    //    engine can exit; we **always join** it before returning (no ghost rejects
    //    minutes after "clean exit").
    confirm_feed.request_stop();
    hub.query.request_confirm_cancel();
    archive_stop.store(true, Ordering::Relaxed);
    prewarm_ctrl.request_stop();
    if let Some(h) = prewarm_join.take() {
        let _ = h.join();
    }

    if let Some(w) = run_materialize_worker.take() {
        w.request_stop();
        drop(w);
    }

    // 3) Stop feeding the archive queue; prep/writer exit on stop + closed channels.
    drop(arch_job_tx);

    // Confirm join: wait until the OS thread exits. Cancel makes waits abort in
    // milliseconds; a mid-wave script check may still take a bit — better that
    // than logging rejects after "clean exit" from a leaked join.
    info!(
        "ibd: waiting for confirm engine to stop ({:?})…",
        t_teardown.elapsed()
    );
    let confirm_join = tokio::task::spawn_blocking(move || {
        let _ = confirm_engine.join();
    });
    let mut confirm_join = confirm_join;
    loop {
        match tokio::time::timeout(Duration::from_secs(5), &mut confirm_join).await {
            Ok(Ok(())) => {
                info!(
                    "ibd: confirm engine stopped ({:?})",
                    t_teardown.elapsed()
                );
                break;
            }
            Ok(Err(e)) => {
                warn!("ibd: confirm join task: {e}");
                break;
            }
            Err(_) => {
                warn!(
                    "ibd: still waiting for confirm engine ({:?})…",
                    t_teardown.elapsed()
                );
            }
        }
    }

    // Archive pipeline: short wait, then abort the tokio task (OS threads check stop).
    match tokio::time::timeout(Duration::from_secs(15), &mut pipeline).await {
        Ok(Ok(())) => info!(
            "ibd: archive pipeline stopped cleanly ({:?})",
            t_teardown.elapsed()
        ),
        Ok(Err(e)) => warn!("ibd: archive pipeline join: {e}"),
        Err(_) => {
            warn!(
                "ibd: archive pipeline slow after 15s — aborting ({:?})",
                t_teardown.elapsed()
            );
            pipeline.abort();
            let _ = tokio::time::timeout(Duration::from_secs(5), pipeline).await;
        }
    }
    while let Ok(r) = arch_res_rx.try_recv() {
        match r {
            ArchiveResult::Ok { hash, .. } => st.body.mark_archived(hash),
            ArchiveResult::Err { hash, .. } => st.body.mark_missing(hash),
        }
    }

    let n = accepted.load(Ordering::SeqCst);
    info!(
        "ibd: done accepted={n} tip={:?} (started {start_tip}, cancelled={cancelled_exit}, teardown={:?})",
        hub.tip_height(),
        t_teardown.elapsed()
    );
    Ok(n)
}

/// Immediately stop getdata and disconnect every peer (SIGINT / IBD exit).
fn disconnect_all_peers(st: &mut IbdWorkState) {
    let n = st.slots.len();
    if n == 0 {
        return;
    }
    for s in &st.slots {
        let _ = s.cmd_tx.send(PeerCmd::Shutdown);
        s.task.abort();
    }
    st.inflight.clear();
    for s in &mut st.slots {
        s.in_flight.clear();
        s.alive = false;
    }
    st.slots.clear();
    info!("ibd: disconnected {n} peer(s)");
}


/// Header/control events per turn (anti-livelock under multi-peer header spam).
const CTRL_DRAIN_EVENT_BUDGET: u64 = 512;
const CTRL_DRAIN_TIME_BUDGET: Duration = Duration::from_millis(5);
/// Body path (framed/decoded blocks): process as much as possible so delivered
/// bytes are not stranded behind headers. Soft wall so cancel/assign still run.
const BODY_DRAIN_TIME_BUDGET: Duration = Duration::from_millis(40);

/// Non-blocking drain of archive results + peer events.
///
/// **Priority:** archive results → body (`BlockFramed`/`Block`/…) → headers.
/// Delivered block bytes must not wait on header floods (single-FIFO waste).
/// Headers remain budgeted so apply cannot livelock.
fn drain_ready_peer_and_archive_events(
    st: &mut IbdWorkState,
    hub: &ChainHub,
    body_rx: &mut mpsc::UnboundedReceiver<PeerEvent>,
    ctrl_rx: &mut mpsc::UnboundedReceiver<PeerEvent>,
    arch_res_rx: &mut mpsc::UnboundedReceiver<ArchiveResult>,
    arch_job_tx: &mpsc::UnboundedSender<ArchiveJob>,
    archive_queued: &ArchiveQueueBudget,
    loop_stats: &LoopStats,
    peer_book: &mut AddrMan,
    local_addr: SocketAddr,
) -> Result<bool, NetError> {
    let t0 = Instant::now();
    let mut events = 0u64;

    // 1) Archive completions (free arch_q bookkeeping) — drain fully, cheap.
    loop {
        match arch_res_rx.try_recv() {
            Ok(r) => {
                events += 1;
                apply_archive_result(st, r, archive_queued, loop_stats);
            }
            Err(_) => break,
        }
    }

    // 2) Body path: framed/decoded blocks — drain until empty or soft time budget.
    let body_t0 = Instant::now();
    loop {
        if body_t0.elapsed() >= BODY_DRAIN_TIME_BUDGET {
            break;
        }
        match body_rx.try_recv() {
            Ok(ev) => {
                events += 1;
                apply_peer_event(
                    st,
                    hub,
                    ev,
                    arch_job_tx,
                    archive_queued,
                    peer_book,
                    local_addr,
                );
            }
            Err(_) => break,
        }
    }

    // 3) Headers / other control — budgeted (never drop; resume next turn).
    let ctrl_t0 = Instant::now();
    let mut ctrl_n = 0u64;
    while ctrl_n < CTRL_DRAIN_EVENT_BUDGET && ctrl_t0.elapsed() < CTRL_DRAIN_TIME_BUDGET {
        match ctrl_rx.try_recv() {
            Ok(ev) => {
                events += 1;
                ctrl_n += 1;
                apply_peer_event(
                    st,
                    hub,
                    ev,
                    arch_job_tx,
                    archive_queued,
                    peer_book,
                    local_addr,
                );
            }
            Err(_) => break,
        }
    }

    loop_stats
        .drain_ns
        .fetch_add(t0.elapsed().as_nanos() as u64, Ordering::Relaxed);
    loop_stats
        .drain_events
        .fetch_add(events, Ordering::Relaxed);
    Ok(true)
}

fn apply_peer_event(
    st: &mut IbdWorkState,
    hub: &ChainHub,
    ev: PeerEvent,
    arch_job_tx: &mpsc::UnboundedSender<ArchiveJob>,
    archive_queued: &ArchiveQueueBudget,
    peer_book: &mut AddrMan,
    local_addr: SocketAddr,
) {
    match ev {
        PeerEvent::Headers { peer, headers } => {
            let batch_len = headers.len();
            let mut added = 0usize;
            for hdr in headers {
                let hash = hdr.block_hash();
                // Multi-peer overlap re-sends the same 2000-header windows. Full
                // ensure_header_fk (hash-head lookup + maybe put) on every repeat
                // made drain cost climb from ~µs to ~ms per event and froze the
                // main loop for tens of seconds with no status lines.
                if st.known_headers.contains(&hash) && st.header_fks.contains_key(&hash) {
                    if let Some(h) = parent_height(&st.hash_height, hub, hdr.prev_blockhash) {
                        if !st.hash_height.contains_key(&hash) {
                            st.record_height(hash, h);
                        }
                        st.max_ordered_height = st.max_ordered_height.max(h);
                    }
                    continue;
                }
                let prev = hdr.prev_blockhash;
                if let Some(h) = parent_height(&st.hash_height, hub, prev) {
                    if !st.hash_height.contains_key(&hash) {
                        st.record_height(hash, h);
                    }
                    st.max_peer_height = st.max_peer_height.max(h);
                    st.max_ordered_height = st.max_ordered_height.max(h);
                }
                // Persist header row once and cache fk — Block path must not re-hit store.
                if !st.header_fks.contains_key(&hash) {
                    if let Ok(fk) = hub.ensure_header_fk(&hdr) {
                        st.header_fks.insert(hash, fk);
                    }
                }
                if hub.has_block(&hash) {
                    st.known_headers.insert(hash);
                    continue;
                }
                // Confirm-rejected bodies stay blacklisted for this run.
                if st.body.is_rejected(&hash) {
                    continue;
                }
                let prev_ok = st.known_headers.contains(&prev)
                    || hub.has_block(&prev)
                    || prev.to_byte_array() == [0u8; 32]
                    || hub.tip_hash() == Some(prev);
                if !prev_ok && hub.tip_height().is_some() && !st.known_headers.is_empty() {
                    continue;
                }
                st.known_headers.insert(hash);
                // Refuse to put a hash on the confirm path without a height — offer
                // needs ht==tip+1; unknown-height entries used to stall tip silently.
                if !st.hash_height.contains_key(&hash) {
                    continue;
                }
                if st.ordered.len() >= MAX_ORDERED_HEADERS {
                    continue;
                }
                if st.ordered_set.insert(hash) {
                    st.ordered.push_back(hash);
                    added += 1;
                }
            }
            if added > 0 {
                if st.ordered_set.len() == added {
                    // First headers of this run (tip=0 cold start).
                    info!(
                        "ibd: first headers from peer[{peer}] batch={batch_len} added={added} ordered={}",
                        st.ordered_set.len()
                    );
                }
                st.empty_header_streak = 0;
                st.headers_done = false;
                let live = st.ordered_set.len();
                let need_arch_runway = want_headers_beyond_soft_cap(
                    live,
                    st.body.known_len(),
                    st.max_ordered_height.saturating_sub(st.max_archived_height),
                    4096,
                );
                if batch_len >= MAX_HEADERS_RESULTS
                    && live < MAX_ORDERED_HEADERS
                    && (live < ORDERED_HEADERS_SOFT_CAP || need_arch_runway)
                {
                    let tips = work_path_tips(st);
                    let _ = request_headers_from(
                        &st.slots,
                        peer,
                        hub,
                        &mut st.header_req_seq,
                        &tips,
                    );
                }
            } else if batch_len == 0 {
                // True empty headers message only — not "already known" batches.
                st.empty_header_streak = st.empty_header_streak.saturating_add(1);
                let tip_h = hub.tip_height().unwrap_or(0);
                let lag = header_lag_behind_peers(st, tip_h);
                let path_idle = st.ordered.is_empty() && st.inflight.is_empty();
                let peers_n = st.slots.iter().filter(|s| s.alive).count() as u32;
                if st.empty_header_streak >= peers_n.max(2) && path_idle && lag <= 2 {
                    st.headers_done = true;
                } else if lag > 2 {
                    // Peers advertise a higher tip than our work path — empty is a
                    // false EOF (often locator stuck at confirmed tip while archive
                    // leads). Keep requesting with work-path locator; never mark done.
                    if st.empty_header_streak == 1 || st.empty_header_streak % 16 == 0 {
                        warn!(
                            "ibd: empty headers but lag={lag} behind max_peer_height={} (known≈{}, tip={tip_h}) — keep header sync",
                            st.max_peer_height,
                            st.max_archived_height
                                .max(st.hash_height.values().copied().max().unwrap_or(0)),
                        );
                    }
                    st.headers_done = false;
                    if st.empty_header_streak >= 8 {
                        st.empty_header_streak = 0;
                    }
                    let tips = work_path_tips(st);
                    let _ = request_headers(&st.slots, hub, &mut st.header_req_seq, &tips);
                } else if st.empty_header_streak < 8
                    && st.ordered_set.len() < ORDERED_HEADERS_SOFT_CAP
                {
                    let tips = work_path_tips(st);
                    let _ = request_headers(&st.slots, hub, &mut st.header_req_seq, &tips);
                } else if st.empty_header_streak >= 8 && lag <= 2 {
                    st.headers_done = true;
                }
            } else {
                // Non-empty but all already known: advance locator off the work path
                // (do **not** count toward headers_done — multi-peer overlap was
                // marking done after one 2000-header window).
                let live = st.ordered_set.len();
                let need_arch_runway = want_headers_beyond_soft_cap(
                    live,
                    st.body.known_len(),
                    st.max_ordered_height.saturating_sub(st.max_archived_height),
                    4096,
                );
                if live < MAX_ORDERED_HEADERS
                    && (live < ORDERED_HEADERS_SOFT_CAP || need_arch_runway)
                    && (batch_len >= MAX_HEADERS_RESULTS
                        || header_lag_behind_peers(st, hub.tip_height().unwrap_or(0)) > 2
                        || need_arch_runway)
                {
                    let tips = work_path_tips(st);
                    let _ = request_headers_from(
                        &st.slots,
                        peer,
                        hub,
                        &mut st.header_req_seq,
                        &tips,
                    );
                }
            }
        }
        PeerEvent::BlockFramed {
            peer,
            hash,
            wire_bytes,
        } => {
            // Frame complete on the wire — free peer/window slots *now* so assign
            // can top up getdata while deserialize still runs on the blocking pool.
            note_block_rx(&mut st.slots, peer, wire_bytes);
            clear_hash_inflight(&mut st.slots, &mut st.inflight, hash);
            if st.body.is_rejected(&hash)
                || st.body.is_known_archived(&hash)
                || hub.has_block(&hash)
            {
                return;
            }
            // pending ⇒ skip re-getdata until archive-ok / Block / BlockDecodeFailed.
            st.body.mark_pending(hash);
        }
        PeerEvent::BlockDecodeFailed { peer, hash } => {
            note_block_progress(&mut st.slots, peer);
            clear_hash_inflight(&mut st.slots, &mut st.inflight, hash);
            if st.body.is_pending(&hash) {
                st.body.mark_missing(hash);
            }
        }
        PeerEvent::Block { peer, block } => {
            note_block_progress(&mut st.slots, peer);
            let hash = block.block_hash();
            // Idempotent: BlockFramed usually already cleared racers + marked pending.
            clear_hash_inflight(&mut st.slots, &mut st.inflight, hash);
            if st.body.is_rejected(&hash)
                || st.body.is_known_archived(&hash)
                || hub.has_block(&hash)
            {
                return;
            }
            // Prefer RAM-cached fk from getheaders (no store lock on hot path).
            let header_fk = if let Some(&fk) = st.header_fks.get(&hash) {
                fk
            } else {
                match hub.ensure_header_fk(&block.header) {
                    Ok(fk) => {
                        st.header_fks.insert(hash, fk);
                        fk
                    }
                    Err(e) => {
                        warn!("ibd: ensure_header {hash}: {e}");
                        // Allow re-getdata if we never archive this body.
                        if st.body.is_pending(&hash) {
                            st.body.mark_missing(hash);
                        }
                        return;
                    }
                }
            };
            // Prevent re-getdata while prep/writer owns this body.
            st.body.mark_pending(hash);
            let tip_h = hub.tip_height().unwrap_or(0);
            let priority = st
                .hash_height
                .get(&hash)
                .map(|&ht| ht <= tip_h.saturating_add(NEAR_DEPTH))
                .unwrap_or(false);
            // Approx wire size for RAM budget (already-decoded; never drop).
            let wire_bytes = block.total_size();
            archive_queued.charge(wire_bytes);
            if arch_job_tx
                .send(ArchiveJob {
                    block,
                    header_fk,
                    priority,
                    wire_bytes,
                })
                .is_err()
            {
                archive_queued.release(wire_bytes);
                st.body.mark_missing(hash);
                warn!("ibd: archive pipeline closed; drop {hash}");
            }
        }
        PeerEvent::NotFound { peer, hashes } => {
            note_block_progress(&mut st.slots, peer);
            if let Some(s) = st.slots.iter_mut().find(|s| s.id == peer) {
                for h in &hashes {
                    s.in_flight.remove(h);
                    let empty = st
                        .inflight
                        .get_mut(h)
                        .map(|e| e.remove_peer(peer))
                        .unwrap_or(false);
                    if empty {
                        st.inflight.remove(h);
                    }
                }
            }
        }
        PeerEvent::Addrs { peer, addrs } => {
            inject_learned_addrs(peer_book, &addrs, local_addr, peer);
        }
        PeerEvent::Dead { peer, reason } => {
            warn!("ibd: peer[{peer}] dead: {reason}");
            if let Some(s) = st.slots.iter().find(|s| s.id == peer) {
                if let Some((lat, bps)) = s.speed_sample() {
                    peer_book.note_speed(s.addr, lat, bps);
                }
            }
            release_peer_block_work(&mut st.slots, &mut st.inflight, peer);
        }
    }
}

/// Grow the IBD dial book from peer-advertised addresses (getaddr responses).
fn inject_learned_addrs(
    book: &mut AddrMan,
    addrs: &[SocketAddr],
    local_addr: SocketAddr,
    from_peer: usize,
) {
    if addrs.is_empty() || book.len() >= MAX_PEER_POOL {
        return;
    }
    let mut added = 0usize;
    for &a in addrs {
        if book.len() >= MAX_PEER_POOL {
            break;
        }
        if a == local_addr || a.ip().is_unspecified() || a.port() == 0 {
            continue;
        }
        let before = book.len();
        book.add(a);
        if book.len() > before {
            added += 1;
        }
    }
    if added > 0 {
        rbitcoin_log::debug!(
            "ibd: peer[{from_peer}] taught {added} addr(s); book={}",
            book.len()
        );
    }
}

fn apply_archive_result(
    st: &mut IbdWorkState,
    r: ArchiveResult,
    archive_queued: &ArchiveQueueBudget,
    loop_stats: &LoopStats,
) {
    match r {
        ArchiveResult::Ok { hash, wire_bytes } => {
            archive_queued.release(wire_bytes);
            clear_hash_inflight(&mut st.slots, &mut st.inflight, hash);
            // Do **not** confirm here — confirm on the main loop after assign so
            // free getdata slots are refilled before Class C burns the turn.
            // Count only first time we learn Class A (skip multi-peer re-archive).
            let first = !st.body.is_known_archived(&hash);
            st.body.mark_archived(hash);
            if first {
                loop_stats
                    .archived_bodies
                    .fetch_add(1, Ordering::Relaxed);
            }
            if let Some(&ht) = st.hash_height.get(&hash) {
                st.max_archived_height = st.max_archived_height.max(ht);
            }
        }
        ArchiveResult::Err {
            hash,
            err,
            wire_bytes,
        } => {
            archive_queued.release(wire_bytes);
            st.body.mark_missing(hash);
            static REJECTS: AtomicU32 = AtomicU32::new(0);
            let n = REJECTS.fetch_add(1, Ordering::Relaxed) + 1;
            if n <= 5 || n % 100 == 0 {
                warn!("ibd: archive reject {hash}: {err} (count={n})");
            }
        }
    }
}

/// Permanent confirm failure: drop from the work path and never re-offer.
///
/// Without this, `offer_confirm_ready` re-noted ghost/re-queued hashes and the
/// confirm engine spun on the same BadPrev / missing-prevout tip+1 (signet log:
/// same hash every ~30s with tip frozen).
fn update_confirm_lag(lag: &AtomicU32, tip: Option<u32>, max_archived: u32) {
    let t = tip.unwrap_or(0);
    lag.store(max_archived.saturating_sub(t), Ordering::Relaxed);
}

fn apply_confirm_reject(st: &mut IbdWorkState, height: u32, hash: BlockHash, err: &str) {
    st.body.mark_rejected(hash);
    remove_from_ordered(&mut st.ordered, &mut st.ordered_set, hash);
    clear_hash_inflight(&mut st.slots, &mut st.inflight, hash);
    // Rate-limit follow-up noise; the confirm engine already logged the reject.
    static N: AtomicU32 = AtomicU32::new(0);
    let n = N.fetch_add(1, Ordering::Relaxed) + 1;
    if n <= 8 || n % 50 == 0 {
        warn!(
            "ibd: confirm reject applied {hash} @{height}: {err} (blacklisted, count={n})"
        );
    }
}


/// Seed ordered path + body cache from durable Class A after process restart.
///
/// Headers and bodies persist in the store; only the IBD work queue was RAM-only.
/// Without this, restart re-ran getheaders and looked like a full re-archive even
/// when `getdata` eventually skipped known bodies.
fn seed_work_path_from_store(st: &mut IbdWorkState, hub: &ChainHub) {
    let Some(tip_hash) = hub.tip_hash() else {
        return;
    };
    let tip_h = hub.tip_height().unwrap_or(0);
    let t0 = Instant::now();
    let path = match hub.query.resume_work_path_after_tip(
        tip_hash.to_byte_array(),
        tip_h,
        MAX_ORDERED_HEADERS,
    ) {
        Ok(p) => p,
        Err(e) => {
            warn!("ibd: resume seed from store failed: {e}");
            return;
        }
    };
    if path.is_empty() {
        return;
    }
    let mut with_body = 0u32;
    let mut contiguous_arch = tip_h;
    let mut arch_prefix = true;
    for e in &path {
        let hash = BlockHash::from_byte_array(e.hash);
        st.known_headers.insert(hash);
        st.record_height(hash, e.height);
        st.header_fks.insert(hash, e.header_fk);
        st.max_ordered_height = st.max_ordered_height.max(e.height);
        if st.ordered_set.insert(hash) {
            st.ordered.push_back(hash);
        }
        if e.has_body {
            st.body.mark_archived(hash);
            with_body = with_body.saturating_add(1);
            if arch_prefix {
                contiguous_arch = e.height;
            }
        } else {
            arch_prefix = false;
        }
    }
    st.max_archived_height = st.max_archived_height.max(contiguous_arch);
    // Peers may still advertise a higher tip; keep header sync open.
    st.headers_done = false;
    info!(
        "ibd: resume seed ordered={} archived_bodies={} archived_to={} (store walk {:?})",
        st.ordered.len(),
        with_body,
        contiguous_arch,
        t0.elapsed()
    );
}

/// Highest hashes on the download path (newest first) for getheaders locators.
fn work_path_tips(st: &IbdWorkState) -> Vec<BlockHash> {
    let mut tips = Vec::with_capacity(8);
    // ordered is tip→far; the back is the highest known header on the path.
    for h in st.ordered.iter().rev().take(4) {
        if st.ordered_set.contains(h) {
            tips.push(*h);
        }
    }
    // Also sample by max height in hash_height if ordered is empty/ghosty.
    if tips.is_empty() {
        if let Some((&h, _)) = st
            .hash_height
            .iter()
            .max_by_key(|(_, &ht)| ht)
        {
            tips.push(h);
        }
    }
    tips
}

/// Drop `hash` from global inflight and every peer's in_flight set.
///
/// Used when the first body arrives (or archive/confirm settles the hash) so
/// racing peers stop counting it as outstanding work. Late `Block` messages are
/// ignored via [`BodyPresence::skip_download`].
fn clear_hash_inflight(
    slots: &mut [PeerSlot],
    inflight: &mut HashMap<BlockHash, state::InflightReq>,
    hash: BlockHash,
) {
    inflight.remove(&hash);
    for s in slots.iter_mut() {
        s.in_flight.remove(&hash);
    }
}

/// Free peer/global slots for hashes already on the confirmed tip (RAM set).
/// Archived-but-unconfirmed ghosts are cleared on Block / archive-ok via
/// [`clear_hash_inflight`] — avoid `is_archived` store probes every assign.
fn prune_satisfied_inflight(
    slots: &mut [PeerSlot],
    inflight: &mut HashMap<BlockHash, state::InflightReq>,
    hub: &ChainHub,
) {
    inflight.retain(|h, _| !hub.has_block(h));
    for s in slots.iter_mut() {
        s.in_flight.retain(|h| !hub.has_block(h));
    }
}

/// Record `peer` as requesting `hash` (tip-hole may accumulate multiple peers).
fn inflight_add_peer(
    inflight: &mut HashMap<BlockHash, state::InflightReq>,
    hash: BlockHash,
    peer: usize,
) {
    inflight
        .entry(hash)
        .or_insert_with(|| state::InflightReq::new(peer))
        .add_peer(peer);
}

/// Getdata assign scope.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum AssignScope {
    /// Tip hole + near + far densify.
    Full,
    /// Only tip hole + near band — when archive RAM budget is full so peers are
    /// not left `inflight=0` while confirm crawls on already-archived lag.
    TipNearOnly,
    /// No new getdata (run-materialize hysteresis): let inflight drain so
    /// materialize can start; archive writer keeps writing what already arrived.
    None,
}

/// Assign getdata for bodies not yet Class A.
///
/// 1. Tip hole — 2 peers immediately; 3rd after [`TIP_HOLE_THIRD_PEER_AFTER`]
///    from when the second was attached (no stall disconnect required).
/// 2. Near band — tip+1‥tip+[`NEAR_DEPTH`] (single peer per hash).
/// 3. Far — forward densify past near (height-ascending); skipped in
///    [`AssignScope::TipNearOnly`].
/// 4. [`AssignScope::None`] — prune only (no new requests).
fn assign_work_ordered(
    st: &mut IbdWorkState,
    hub: &ChainHub,
    cfg: &IbdConfig,
    loop_stats: &LoopStats,
    _arch_bytes: usize,
    _arch_budget: usize,
    scope: AssignScope,
) {
    let t0 = Instant::now();
    let mut issued = 0u64;
    let alive: Vec<usize> = st
        .slots
        .iter()
        .filter(|s| s.alive)
        .map(|s| s.id)
        .collect();
    if alive.is_empty() {
        return;
    }

    prune_satisfied_inflight(&mut st.slots, &mut st.inflight, hub);

    if matches!(scope, AssignScope::None) {
        // Still expire stale pending so limbo getdata cannot block forever.
        let expired = st.body.expire_stale_pending(PENDING_STALE);
        for h in expired {
            clear_hash_inflight(&mut st.slots, &mut st.inflight, h);
        }
        finish_assign(loop_stats, t0, issued);
        return;
    }

    let expired = st.body.expire_stale_pending(PENDING_STALE);
    for h in expired {
        clear_hash_inflight(&mut st.slots, &mut st.inflight, h);
    }

    let tip = hub.tip_height().unwrap_or(0);
    let near_hi = tip.saturating_add(NEAR_DEPTH);
    let tip_holes = contiguous_tip_holes(st, hub, TIP_HOLE_MAX);
    let tip_hole = !tip_holes.is_empty();

    issued += cover_tip_holes(st, hub, cfg, &alive, &tip_holes);

    let mut room = cfg.window.saturating_sub(st.inflight.len());
    if room == 0 {
        finish_assign(loop_stats, t0, issued);
        return;
    }
    if st.inflight.is_empty()
        && !tip_hole
        && st.max_archived_height > 0
        && st.max_archived_height >= st.max_ordered_height
    {
        finish_assign(loop_stats, t0, issued);
        return;
    }

    // TipNearOnly: give the whole window to tip runway (no far reserve).
    let want_far = matches!(scope, AssignScope::Full);
    let far_cap = if want_far {
        far_slots_per_peer(cfg.per_peer, tip_hole)
    } else {
        0
    };
    let far_window_reserve = if want_far {
        alive
            .len()
            .saturating_mul(far_cap)
            .min(room.saturating_mul(3) / 4)
            .max(far_cap.min(room))
    } else {
        0
    };
    let near_window_cap = room.saturating_sub(far_window_reserve);

    let (near_work, far_work) =
        collect_need(st, hub, tip, near_hi, near_window_cap, room, want_far && far_cap > 0);
    if near_work.is_empty() && far_work.is_empty() {
        finish_assign(loop_stats, t0, issued);
        return;
    }

    let mut peer_i = st.assign_rot;
    st.assign_rot = st.assign_rot.wrapping_add(1);

    let mut near = near_work;
    while room > far_window_reserve && !near.is_empty() {
        let mut any = false;
        for _ in 0..alive.len() {
            if room <= far_window_reserve || near.is_empty() {
                break;
            }
            let pid = alive[peer_i % alive.len()];
            peer_i += 1;
            if !peer_can_take_near(st, pid, cfg.per_peer, far_cap, tip, near_hi) {
                continue;
            }
            let Some(h) = pop_need(&mut near, st, hub) else {
                break;
            };
            if issue_one(st, pid, h, &mut room, &mut issued) {
                any = true;
            }
        }
        if !any {
            break;
        }
    }

    let mut far = far_work;
    while room > 0 && !far.is_empty() {
        let mut any = false;
        for _ in 0..alive.len() {
            if room == 0 || far.is_empty() {
                break;
            }
            let pid = alive[peer_i % alive.len()];
            peer_i += 1;
            let Some(n) = peer_far_free(st, pid, cfg.per_peer, far_cap, tip, near_hi) else {
                continue;
            };
            let take = n.min(room).min(FAR_BATCH_MAX);
            let mut batch = Vec::with_capacity(take);
            while batch.len() < take {
                let Some(h) = pop_need(&mut far, st, hub) else {
                    break;
                };
                batch.push(h);
            }
            if batch.is_empty() {
                continue;
            }
            if issue_batch(st, pid, batch, &mut room, &mut issued) {
                any = true;
            }
        }
        if !any {
            break;
        }
    }

    finish_assign(loop_stats, t0, issued);
}

fn finish_assign(loop_stats: &LoopStats, t0: Instant, issued: u64) {
    loop_stats
        .assign_ns
        .fetch_add(t0.elapsed().as_nanos() as u64, Ordering::Relaxed);
    if issued > 0 {
        loop_stats.assign_issued.fetch_add(issued, Ordering::Relaxed);
    }
}

/// Collect (near, far) hashes that still need getdata.
///
/// Far is **forward-only** from `near_hi+1` (densify Class A behind tip).
/// Does not update `max_archived_height` (that is archive-result / seed only).
fn collect_need(
    st: &mut IbdWorkState,
    hub: &ChainHub,
    tip: u32,
    near_hi: u32,
    near_cap: usize,
    total_room: usize,
    want_far: bool,
) -> (VecDeque<BlockHash>, VecDeque<BlockHash>) {
    let mut near = VecDeque::new();
    let mut far = VecDeque::new();

    for ht in tip.saturating_add(1)..=near_hi {
        if near.len() >= near_cap {
            break;
        }
        let Some(&h) = st.height_to_hash.get(&ht) else {
            continue;
        };
        if !st.ordered_set.contains(&h) || st.inflight.contains_key(&h) {
            continue;
        }
        if st.body.is_known_archived(&h) || st.body.is_pending(&h) {
            continue;
        }
        if st.body.skip_download(hub, &h) {
            continue;
        }
        near.push_back(h);
    }

    if !want_far {
        return (near, far);
    }

    let far_room = total_room.saturating_sub(near.len()).max(
        total_room.saturating_sub(near_cap),
    );
    if far_room == 0 {
        return (near, far);
    }

    let mut inspected = 0usize;
    let far_lo = near_hi.saturating_add(1);
    let far_hi = st.max_ordered_height.max(far_lo);
    for ht in far_lo..=far_hi {
        if far.len() >= far_room || inspected >= FAR_SCAN_BUDGET {
            break;
        }
        let Some(&h) = st.height_to_hash.get(&ht) else {
            continue;
        };
        if !st.ordered_set.contains(&h) || st.inflight.contains_key(&h) {
            continue;
        }
        if st.body.is_known_archived(&h) || st.body.is_pending(&h) {
            continue;
        }
        inspected += 1;
        if st.body.skip_download(hub, &h) {
            continue;
        }
        far.push_back(h);
    }

    (near, far)
}

fn pop_need(
    q: &mut VecDeque<BlockHash>,
    st: &mut IbdWorkState,
    hub: &ChainHub,
) -> Option<BlockHash> {
    while let Some(h) = q.pop_front() {
        if st.body.skip_download(hub, &h) || st.inflight.contains_key(&h) {
            continue;
        }
        return Some(h);
    }
    None
}

fn peer_can_take_near(
    st: &IbdWorkState,
    pid: usize,
    per_peer: usize,
    far_cap: usize,
    tip: u32,
    near_hi: u32,
) -> bool {
    let Some(s) = st.slots.iter().find(|s| s.id == pid && s.alive) else {
        return false;
    };
    if s.in_flight.len() >= per_peer {
        return false;
    }
    // Reserve far_cap slots on the peer for archive-ahead work so near cannot
    // pin every in-flight slot (that froze Class A a few k ahead of tip).
    if far_cap > 0 {
        let (near_n, _far_n) = count_class(&s.in_flight, &st.hash_height, tip, near_hi);
        let near_cap = per_peer.saturating_sub(far_cap);
        if near_n >= near_cap {
            return false;
        }
    }
    true
}

fn peer_far_free(
    st: &IbdWorkState,
    pid: usize,
    per_peer: usize,
    far_cap: usize,
    tip: u32,
    near_hi: u32,
) -> Option<usize> {
    let s = st.slots.iter().find(|s| s.id == pid && s.alive)?;
    let free_total = per_peer.saturating_sub(s.in_flight.len());
    if free_total == 0 {
        return None;
    }
    let far_n = count_class(&s.in_flight, &st.hash_height, tip, near_hi).1;
    let free_far = far_cap.saturating_sub(far_n).min(free_total);
    if free_far == 0 {
        None
    } else {
        Some(free_far)
    }
}

fn count_class(
    in_flight: &HashSet<BlockHash>,
    heights: &HashMap<BlockHash, u32>,
    tip: u32,
    near_hi: u32,
) -> (usize, usize) {
    let depth = near_hi.saturating_sub(tip);
    let mut near = 0usize;
    let mut far = 0usize;
    for h in in_flight {
        match classify_height(heights.get(h).copied(), tip, depth) {
            WorkClass::Near => near += 1,
            WorkClass::Far => far += 1,
        }
    }
    (near, far)
}

fn issue_one(
    st: &mut IbdWorkState,
    pid: usize,
    h: BlockHash,
    room: &mut usize,
    issued: &mut u64,
) -> bool {
    issue_batch(st, pid, vec![h], room, issued)
}

fn issue_batch(
    st: &mut IbdWorkState,
    pid: usize,
    batch: Vec<BlockHash>,
    room: &mut usize,
    issued: &mut u64,
) -> bool {
    if batch.is_empty() {
        return false;
    }
    let Some(idx) = st.slots.iter().position(|s| s.id == pid && s.alive) else {
        return false;
    };
    // Skip hashes this peer already has outstanding (tip-hole re-cover).
    let batch: Vec<BlockHash> = batch
        .into_iter()
        .filter(|h| !st.slots[idx].in_flight.contains(h))
        .collect();
    if batch.is_empty() {
        return false;
    }
    let empty = st.slots[idx].in_flight.is_empty();
    for &h in &batch {
        st.slots[idx].in_flight.insert(h);
    }
    if empty {
        touch_block_progress(&st.slots[idx].block_progress_ms);
    }
    let _ = st.slots[idx]
        .cmd_tx
        .send(PeerCmd::GetData { hashes: batch.clone() });
    for &h in &batch {
        inflight_add_peer(&mut st.inflight, h, pid);
    }
    *issued += batch.len() as u64;
    // Window counts unique hashes: only charge hashes that were not already inflight.
    let new_unique = batch
        .iter()
        .filter(|h| st.inflight.get(*h).map(|e| e.len() == 1).unwrap_or(false))
        .count();
    *room = room.saturating_sub(new_unique);
    true
}

/// Contiguous unready hashes at the ordered front (status `hole=`).
fn contiguous_tip_holes(
    st: &mut IbdWorkState,
    hub: &ChainHub,
    max: usize,
) -> Vec<BlockHash> {
    let mut holes = Vec::new();
    for h in st.ordered.iter().copied() {
        if !st.ordered_set.contains(&h) {
            continue;
        }
        if st.body.is_rejected(&h) {
            continue;
        }
        if hub.has_block(&h) || st.body.ready(hub, &h) {
            break;
        }
        holes.push(h);
        if holes.len() >= max {
            break;
        }
    }
    holes
}

/// Desired concurrent getdata peers for a tip-hole hash.
///
/// - 0–1 outstanding → aim for [`TIP_HOLE_IMMEDIATE_PEERS`] (2) immediately
/// - 2 outstanding → hold until [`TIP_HOLE_THIRD_PEER_AFTER`] from second attach
/// - then allow [`TIP_HOLE_MAX_PEERS`] (3)
pub(crate) fn tip_hole_peer_target(
    already: usize,
    second_peer_at: Option<Instant>,
    now: Instant,
) -> usize {
    if already >= TIP_HOLE_MAX_PEERS {
        return TIP_HOLE_MAX_PEERS;
    }
    if already >= TIP_HOLE_IMMEDIATE_PEERS {
        let ready_for_third = second_peer_at
            .map(|t| now.duration_since(t) >= TIP_HOLE_THIRD_PEER_AFTER)
            .unwrap_or(false);
        if ready_for_third {
            TIP_HOLE_MAX_PEERS
        } else {
            TIP_HOLE_IMMEDIATE_PEERS
        }
    } else {
        TIP_HOLE_IMMEDIATE_PEERS
    }
}

/// Cover each tip-hole hash with staged multi-peer getdata (2 now, 3 after 10s).
/// First delivery clears all racers via [`clear_hash_inflight`].
fn cover_tip_holes(
    st: &mut IbdWorkState,
    hub: &ChainHub,
    cfg: &IbdConfig,
    alive: &[usize],
    holes: &[BlockHash],
) -> u64 {
    if holes.is_empty() || alive.is_empty() {
        return 0;
    }
    let mut issued = 0u64;
    let mut peer_i = st.assign_rot;
    let now = Instant::now();

    for &h in holes {
        if hub.has_block(&h) || st.body.is_pending(&h) || st.body.ready(hub, &h) {
            continue;
        }
        let (already, second_at) = st
            .inflight
            .get(&h)
            .map(|e| (e.len(), e.second_peer_at))
            .unwrap_or((0, None));
        let want = tip_hole_peer_target(already, second_at, now);
        if already >= want {
            continue;
        }
        let mut need = want - already;
        let mut placed_any = false;
        for _ in 0..alive.len() {
            if need == 0 {
                break;
            }
            let pid = alive[peer_i % alive.len()];
            peer_i += 1;
            let Some(idx) = st.slots.iter().position(|s| s.id == pid && s.alive) else {
                continue;
            };
            if st.slots[idx].in_flight.contains(&h) {
                continue;
            }
            if st
                .inflight
                .get(&h)
                .map(|e| e.contains_peer(pid))
                .unwrap_or(false)
            {
                continue;
            }
            if st.slots[idx].in_flight.len() >= cfg.per_peer {
                continue;
            }
            let mut room = 1usize;
            if issue_one(st, pid, h, &mut room, &mut issued) {
                placed_any = true;
                need = need.saturating_sub(1);
            }
        }
        // No free peer slots for a hole with zero coverage — later holes wait.
        if already == 0 && !placed_any {
            break;
        }
    }
    issued
}

#[cfg(test)]
mod tip_hole_race_tests {
    use super::*;

    #[test]
    fn tip_hole_targets_two_immediately() {
        let now = Instant::now();
        assert_eq!(tip_hole_peer_target(0, None, now), 2);
        assert_eq!(tip_hole_peer_target(1, None, now), 2);
    }

    #[test]
    fn tip_hole_third_only_after_grace() {
        let t0 = Instant::now();
        assert_eq!(
            tip_hole_peer_target(2, Some(t0), t0 + Duration::from_secs(9)),
            2
        );
        assert_eq!(
            tip_hole_peer_target(2, Some(t0), t0 + Duration::from_secs(10)),
            3
        );
        assert_eq!(tip_hole_peer_target(3, Some(t0), t0 + Duration::from_secs(60)), 3);
    }
}

/// Height of `child` = parent height + 1 when parent height is known.
fn parent_height(
    hash_height: &HashMap<BlockHash, u32>,
    hub: &ChainHub,
    prev: BlockHash,
) -> Option<u32> {
    if prev.to_byte_array() == [0u8; 32] {
        return Some(0);
    }
    if let Some(&ph) = hash_height.get(&prev) {
        return Some(ph.saturating_add(1));
    }
    if hub.tip_hash() == Some(prev) {
        return Some(hub.tip_height().unwrap_or(0).saturating_add(1));
    }
    None
}

