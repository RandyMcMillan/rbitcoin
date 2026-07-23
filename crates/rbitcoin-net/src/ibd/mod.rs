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
mod assign;
mod assign_plan;
mod body;
mod coalesce;
mod confirm;
mod dial;
mod events;
mod exit;
mod path;
mod peer_io;
mod perf_log;
mod progress;
mod state;
mod status;

#[cfg(test)]
mod freeze_benches;

use archive::{
    spawn_archive_pipeline, ArchiveJob, ArchivePipelineStats, ArchiveQueueBudget, ArchiveResult,
};
use assign_plan::{remove_from_ordered, want_headers_beyond_soft_cap};
// compact_ordered used via IbdWorkState::hygiene
use confirm::{offer_confirm_ready, spawn_confirm_engine, ConfirmEvent, ConfirmFeed};
use dial::{
    apply_dial_result, dial_batch, dial_blocked_addrs, disconnect_stalled_block_peers,
    expire_addr_cooldown, request_headers,
};
use peer_io::{PeerCmd, PeerEvent, PeerEventSinks};
use exit::{
    all_peers_dead_action, catchup_complete_after_drain, header_lag_behind_peers, path_drained,
    peer_caught_up, AllPeersDead,
};
use progress::{
    format_rate, ibd_pct, work_chain_progress, TipRateTracker,
};
use state::IbdWorkState;
use assign::{archive_pipeline_saturated, assign_work_ordered, AssignDepth};
use events::{
    apply_archive_result, apply_confirm_reject, apply_peer_event, disconnect_all_peers,
    drain_ready_peer_and_archive_events, update_confirm_lag,
};
use path::{seed_work_path_from_store, work_path_tips};
use status::LoopStats;

use crate::chain::ChainHub;
use crate::codec::MAX_HEADERS_RESULTS;
use crate::error::NetError;
use bitcoin::p2p::Magic;
use std::collections::HashSet;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use rbitcoin_log::{info, info_bold, warn};

/// Default max **concurrent** unique block downloads (in-flight getdata).
///
/// Not a tip-distance ceiling: archive may run to the end of the known header
/// path; this only limits how many bodies we pull at once (backpressure + RAM).
pub const DEFAULT_IBD_WINDOW: usize = 1024;

/// Soft cap on `ordered` length while still requesting more headers.
/// Keeps header sync from unbounded growth if peers never signal done; large
/// enough for full signet / long mainnet catch-up in one run.
/// Hard ceiling on the ordered work path (memory / hygiene bound).
pub(crate) const MAX_ORDERED_HEADERS: usize = 500_000;
/// Soft cap: stop **requesting** more headers once we have this many on the path.
/// Multi-peer getheaders while `ordered` was 100k–500k flooded the main loop with
/// expensive Headers events (drain livelock → multi-minute freezes, getdata starved).
/// ~64k is ample runway for window=1024 archive race + tip holes.
pub(crate) const ORDERED_HEADERS_SOFT_CAP: usize = 64_000;

/// Max blocks in flight to a single peer (Core `MAX_BLOCKS_IN_TRANSIT_PER_PEER`).
///
/// Keeping this at 16 avoids overloading peers with large getdata batches; total
/// concurrency scales with peer count (`peers × 16`), not by piling work on few hosts.
pub const DEFAULT_BLOCKS_IN_TRANSIT_PER_PEER: usize = 16;

/// Near band: tip+1 ..= tip+N (confirm runway + bulk near assign).
pub(crate) const NEAR_DEPTH: u32 = 4096;
/// Max contiguous tip+1.. holes to cover per assign.
pub(crate) const TIP_HOLE_MAX: usize = 32;
/// Max concurrent getdata peers for one tip-hole hash.
pub(crate) const TIP_HOLE_MAX_PEERS: usize = 3;
/// Immediate tip-hole race size (first + second peer).
pub(crate) const TIP_HOLE_IMMEDIATE_PEERS: usize = 2;
/// After the second tip-hole peer is issued, wait this long before a third.
pub(crate) const TIP_HOLE_THIRD_PEER_AFTER: Duration = Duration::from_secs(10);
/// Cap on IBD dial pool after getaddr learning (seeds + discovered).
pub(crate) const MAX_PEER_POOL: usize = 256;
/// Pending (framed, not Class A) longer than this → re-getdata.
pub(crate) const PENDING_STALE: Duration = Duration::from_secs(45);
/// ContigPark race-band pending: re-getdata much sooner (slow peer / stuck prep).
pub(crate) const CONTIG_PARK_PENDING_STALE: Duration = Duration::from_secs(5);
/// Max hashes per densify getdata batch.
pub(crate) const FAR_BATCH_MAX: usize = 16;
/// Cap height scan for densify candidates per assign tick.
pub(crate) const FAR_SCAN_BUDGET: usize = 16_384;
/// Multi-peer race only on ContigPark `write_next`‥`write_next+R-1` (rest of
/// densify band is single-peer).
pub(crate) const CONTIG_PARK_RACE: usize = 8;
/// ContigPark densify horizon past `write_next` (single-peer feed). Also hard
/// park horizon: refuse to charge/park bodies further ahead.
pub(crate) const CONTIG_DENSIFY_AHEAD: u32 = 2048;

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
    /// Smaller window / short dials for tests (no multi-second connect stalls).
    pub fn for_test() -> Self {
        Self {
            window: 32,
            per_peer: 8,
            target_peers: 4,
            stall: Duration::from_secs(3),
            headers_batch: MAX_HEADERS_RESULTS,
            connect_timeout: Duration::from_millis(400),
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
            "ibd: tokio worker threads≈{workers} (peer decode: blocking pool; archive: 1 OS prep + 1 OS writer; confirm: materialize+scripts+writeback OS threads)"
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
    // Snapshot for genuine 5s rates (progress + perf share this tick).
    let mut last_sample_tip = start_tip;
    let mut tip_rate_tracker = TipRateTracker::new();
    tip_rate_tracker.push(Instant::now(), start_tip);
    // Max concurrent unique downloads (not tip-distance).
    let window = cfg.window;

    let mut st = IbdWorkState::new(initial_slots, hub.tip_hash(), hub.tip_height());
    // Reload post-tip headers + Class A from disk so restart does not re-getdata
    // bodies that are already archived (ordered path was process-local only).
    seed_work_path_from_store(&mut st, hub.as_ref());
    let mut last_sample_arch_hwm = st.max_archived_height;

    // Kick header sync — try a few peers (channel may close if handshake race).
    for _ in 0..st.slots.len().min(4) {
        let tips = work_path_tips(&st);
        if request_headers(&st.slots, &hub, &mut st.header_req_seq, &tips).unwrap_or(false) {
            break;
        }
    }

    // Archive pipeline: multi-core prep + exclusive writer (mmap Class A).
    // Byte budget (default via RBITCOIN_ARCHIVE_QUEUE_MB) caps decoded blocks
    // waiting for archive; far getdata scales with free headroom (hysteresis
    // 90%/70%) — never drops in-flight, only throttles new far densify.
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
    if store_arch_total > 0 {
        info!("ibd: store has {store_arch_total} Class A bodies (arch_total seed)");
    }
    let (arch_job_tx, arch_job_rx) = mpsc::unbounded_channel::<ArchiveJob>();
    let (arch_res_tx, mut arch_res_rx) = mpsc::unbounded_channel::<ArchiveResult>();
    let archive_stop = Arc::new(AtomicBool::new(false));
    // Contiguous Class A writer starts just past the resume archive HWM (or 0).
    let archive_write_next = Arc::new(AtomicU32::new(if hub.tip_height().is_some() {
        st.max_archived_height.saturating_add(1)
    } else {
        0
    }));
    let mut pipeline = spawn_archive_pipeline(
        hub.clone(),
        arch_job_rx,
        arch_res_tx,
        Arc::clone(&pipe_stats),
        Arc::clone(&archive_queued),
        Arc::clone(&confirm_lag),
        Arc::clone(&archive_write_next),
        Arc::clone(&archive_stop),
    );

    // Direct IBD: SH runs merge-only (no progressive head mat worker; bulk at tip).

    // Fresh cancel state for this IBD session (may have been set on prior stop).
    hub.query.clear_confirm_cancel();

    // Materialize owns Class A load for each claimed batch (no background
    // prewarm worker). Parent create body pages for writeback annotate are
    // mlocked with refcounted need_heights; tip GC munlocks after writeback.
    hub.query.set_prewarm_worker_live(false);
    info!(
        "ibd: confirm materialize loads Class A inline (no background prewarm worker; \
         mlock writeback parent body pages only)"
    );

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
                ConfirmEvent::BodyMissing { hash } => {
                    // Stale known vs store: re-probe on next offer (do not mark missing).
                    st.body.demote_known(hash);
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
            &archive_write_next,
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
        // ContigPark densify scaled by archive free headroom (proportional +
        // 90%/70% hysteresis). Hard stop densify/runway when !can_assign (at
        // budget); tip-hole + write_next race always run. In-flight bodies still
        // enqueue (may overshoot). Saturated pipeline → Critical only (CPU).
        let far_scale = archive_queued.far_admission_scale();
        let can_assign_new = archive_queued.can_assign();
        let write_next = archive_write_next.load(Ordering::Relaxed);
        let inflight_before = st.inflight.len();
        let depth = if archive_pipeline_saturated(
            st.body.pending_len(),
            st.inflight.len(),
            archive_queued.fill_ratio(),
        ) {
            AssignDepth::Critical
        } else {
            AssignDepth::Full
        };
        assign_work_ordered(
            &mut st,
            hub.as_ref(),
            &cfg,
            &loop_stats,
            far_scale,
            write_next,
            depth,
            can_assign_new,
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
                ConfirmEvent::BodyMissing { hash } => {
                    st.body.demote_known(hash);
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
            &archive_write_next,
            &loop_stats,
            peer_sess.book_mut(),
            local_addr,
        )? {
            break;
        }
        // Re-assign only if drain/confirm freed meaningful inflight slots
        // (avoid double planner work every loop tick). Empty inflight + saturated
        // pipeline still runs Critical only (write_next race / tip hole).
        let freed = inflight_before.saturating_sub(st.inflight.len());
        if freed >= 8 || st.inflight.is_empty() {
            let far_scale2 = archive_queued.far_admission_scale();
            let can_assign2 = archive_queued.can_assign();
            let write_next2 = archive_write_next.load(Ordering::Relaxed);
            let depth2 = if archive_pipeline_saturated(
                st.body.pending_len(),
                st.inflight.len(),
                archive_queued.fill_ratio(),
            ) {
                AssignDepth::Critical
            } else {
                AssignDepth::Full
            };
            assign_work_ordered(
                &mut st,
                hub.as_ref(),
                &cfg,
                &loop_stats,
                far_scale2,
                write_next2,
                depth2,
                can_assign2,
            );
        }

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

        // Centralized ~5s operator tick: genuine window rates + progress + perf.
        // (No separate 1s "chunk size / elapsed" progress path — that lied.)
        if last_status.elapsed() >= Duration::from_secs(5) {
            let now = Instant::now();
            let window_secs = last_status.elapsed().as_secs_f64().max(0.001);
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

            // Genuine rates over this 5s window (height deltas / wall).
            let tip_delta = prog.tip.saturating_sub(last_sample_tip);
            let arch_delta = prog.archived.saturating_sub(last_sample_arch_hwm);
            let tip_rate = tip_delta as f64 / window_secs;
            let arch_rate = arch_delta as f64 / window_secs;
            let arch_lead = prog.archived.saturating_sub(prog.tip);
            let peers_n = st.slots.iter().filter(|s| s.alive).count();
            let (_pw_through, pw_ahead, _pw_parents, _pw_bodies, _plans, _depth) =
                hub.query.parent_prewarm_perf_snapshot();
            let sh_runs = hub.query.scripthash_run_count();
            let mlock_mb = hub.query.prewarm_mlock_bytes() / (1024 * 1024);
            let pct = ibd_pct(prog.tip, prog.headers);

            tip_rate_tracker.push(now, prog.tip);
            let eta = tip_rate_tracker.eta_string(now, prog.tip, prog.headers);

            // Bold on a TTY so the 5s progress line stands out among perf/debug noise.
            info_bold!(
                "ibd: progress {pct}% tip={} ({}/s) arch_hwm={} ({}/s lead={arch_lead}) hole={} peers={peers_n} prewarm+{pw_ahead} mlock={mlock_mb}MiB sh_runs={sh_runs} horizon={} {eta}",
                prog.tip,
                format_rate(tip_rate),
                prog.archived,
                format_rate(arch_rate),
                prog.tip_hole,
                prog.headers,
            );
            let _ = std::io::Write::flush(&mut std::io::stderr());

            last_sample_tip = prog.tip;
            last_sample_arch_hwm = prog.archived;

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
            let prewarm_snap = hub.query.parent_prewarm_perf_snapshot();
            let (mlock_n, mlock_bytes) = hub.query.prewarm_mlock_stats();
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
                prewarm_snap,
                mlock_n,
                mlock_bytes,
                hub.query.scripthash_run_count(),
                hub.query.archive_txid_sticky_stats(),
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
                    &archive_write_next,
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
                    &archive_write_next,
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
                        ConfirmEvent::BodyMissing { hash } => {
                            st.body.demote_known(hash);
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

    // 2) Signal cooperative stops. Confirm cancel aborts materialize loads so
    //    the engine can exit; we **always join** it before returning (no ghost
    //    rejects minutes after "clean exit").
    confirm_feed.request_stop();
    hub.query.request_confirm_cancel();
    archive_stop.store(true, Ordering::Relaxed);

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
            ArchiveResult::Dropped { hash, requeue, .. } => {
                if requeue {
                    st.body.mark_missing(hash);
                }
            }
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

#[cfg(test)]
mod tip_hole_race_tests {
    use super::assign::tip_hole_peer_target;
    use std::time::{Duration, Instant};

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

#[cfg(test)]
mod park_race_assign_tests {
    use super::assign::{archive_pipeline_saturated, park_race_need};
    use super::state::IbdWorkState;
    use super::CONTIG_PARK_RACE;
    use bitcoin::hashes::Hash;
    use bitcoin::BlockHash;
    use rbitcoin_consensus::{ChainParams, Milestone};
    use rbitcoin_query::Query;

    fn h(n: u32) -> BlockHash {
        let mut b = [0u8; 32];
        b[0..4].copy_from_slice(&n.to_le_bytes());
        BlockHash::from_byte_array(b)
    }

    #[test]
    fn park_race_need_only_write_next_prefix() {
        let dir = std::env::temp_dir().join(format!(
            "rbitcoin-park-race-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let _ = std::fs::create_dir_all(&dir);
        let q = Query::open_or_create(dir.join("store")).unwrap();
        let hub = crate::chain::ChainHub::new(q, ChainParams::regtest(), Milestone::NONE);

        let mut st = IbdWorkState::new(vec![], None, None);
        // Heights 100..=120 on the ordered path; race is write_next..+R-1 only.
        for ht in 100u32..=120 {
            let hash = h(ht);
            st.record_height(hash, ht);
            st.ordered_set.insert(hash);
            st.ordered.push_back(hash);
            st.max_ordered_height = ht;
        }
        // 100 pending (in pipeline), 101 known archived, 102 inflight → race keeps it.
        st.body.mark_pending(h(100));
        st.body.mark_archived(h(101));
        st.inflight
            .insert(h(102), super::state::InflightReq::new(0));

        let need = park_race_need(&mut st, &hub, 100);
        assert!(!need.is_empty(), "expected park-race need");
        // 100 pending + 101 archived skipped; 102 inflight is kept for multi-peer race.
        assert_eq!(need[0], h(102), "inflight kept for race prefix");
        assert!(need.iter().all(|x| *x != h(100) && *x != h(101)));
        // Densify-only heights beyond race prefix must not appear.
        let race_hi = 100 + CONTIG_PARK_RACE as u32 - 1;
        for hash in &need {
            let ht = st.hash_height[hash];
            assert!((100..=race_hi).contains(&ht), "ht={ht} outside race");
        }
        // Height just past race prefix is densify, not multi-peer race need.
        assert!(!need.contains(&h(race_hi + 1)));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn archive_pipeline_saturated_gates_full_assign() {
        assert!(!archive_pipeline_saturated(0, 0, 0.0));
        assert!(!archive_pipeline_saturated(200, 32, 0.95)); // inflight still high
        assert!(archive_pipeline_saturated(96, 0, 0.0));
        assert!(archive_pipeline_saturated(0, 0, 0.85));
        assert!(archive_pipeline_saturated(200, 15, 0.9));
    }
}

