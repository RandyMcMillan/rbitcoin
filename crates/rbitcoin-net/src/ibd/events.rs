//! Peer / archive event drain and apply (IBD main loop).

use super::archive::{ArchiveJob, ArchiveQueueBudget, ArchiveResult};
use super::assign::clear_hash_inflight;
use super::assign_plan::{remove_from_ordered, want_headers_beyond_soft_cap};
use super::dial::{release_peer_block_work, request_headers, request_headers_from};
use super::exit::header_lag_behind_peers;
use super::path::work_path_tips;
use super::peer_io::{note_block_progress, note_block_rx, PeerCmd, PeerEvent};
use super::state::IbdWorkState;
use super::status::LoopStats;
use super::{
    CONTIG_DENSIFY_AHEAD, CONTIG_GAP_FILL_MAX, MAX_ORDERED_HEADERS, MAX_PEER_POOL, NEAR_DEPTH,
    ORDERED_HEADERS_SOFT_CAP,
};
use crate::chain::ChainHub;
use crate::codec::MAX_HEADERS_RESULTS;
use crate::error::NetError;
use crate::seeds::AddrMan;
use bitcoin::hashes::Hash;
use bitcoin::BlockHash;
use rbitcoin_log::{info, warn};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::mpsc;

/// Immediately stop getdata and disconnect every peer (SIGINT / IBD exit).
pub(crate) fn disconnect_all_peers(st: &mut IbdWorkState) {
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
pub(crate) fn drain_ready_peer_and_archive_events(
    st: &mut IbdWorkState,
    hub: &ChainHub,
    body_rx: &mut mpsc::UnboundedReceiver<PeerEvent>,
    ctrl_rx: &mut mpsc::UnboundedReceiver<PeerEvent>,
    arch_res_rx: &mut mpsc::UnboundedReceiver<ArchiveResult>,
    arch_job_tx: &mpsc::UnboundedSender<ArchiveJob>,
    archive_queued: &ArchiveQueueBudget,
    archive_write_next: &AtomicU32,
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
                    archive_write_next,
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
                    archive_write_next,
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

pub(crate) fn apply_peer_event(
    st: &mut IbdWorkState,
    hub: &ChainHub,
    ev: PeerEvent,
    arch_job_tx: &mpsc::UnboundedSender<ArchiveJob>,
    archive_queued: &ArchiveQueueBudget,
    archive_write_next: &AtomicU32,
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
            let tip_h = hub.tip_height().unwrap_or(0);
            let height = st
                .hash_height
                .get(&hash)
                .copied()
                .unwrap_or(u32::MAX);
            let write_next = archive_write_next.load(Ordering::Relaxed);
            // Hard horizon: do not charge/park bodies ContigPark cannot use yet.
            // mark_missing (not pending) so densify can re-get once write_next
            // advances; densify is capped to write_next+CONTIG_DENSIFY_AHEAD so
            // this does not thrash re-download of far heights every tick.
            if height != u32::MAX
                && height > write_next.saturating_add(CONTIG_DENSIFY_AHEAD)
            {
                st.body.mark_missing(hash);
                return;
            }
            // Prevent re-getdata while prep/writer owns this body.
            st.body.mark_pending(hash);
            let priority = st
                .hash_height
                .get(&hash)
                .map(|&ht| {
                    ht <= tip_h.saturating_add(NEAR_DEPTH)
                        || ht <= write_next.saturating_add(CONTIG_GAP_FILL_MAX)
                })
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
                    height,
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
pub(crate) fn inject_learned_addrs(
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

pub(crate) fn apply_archive_result(
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
        ArchiveResult::Dropped {
            hash,
            wire_bytes,
            requeue,
        } => {
            // Duplicate (requeue=false) or beyond-horizon refuse (requeue=true).
            archive_queued.release(wire_bytes);
            if requeue {
                st.body.mark_missing(hash);
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
pub(crate) fn update_confirm_lag(lag: &AtomicU32, tip: Option<u32>, max_archived: u32) {
    let t = tip.unwrap_or(0);
    lag.store(max_archived.saturating_sub(t), Ordering::Relaxed);
}

pub(crate) fn apply_confirm_reject(st: &mut IbdWorkState, height: u32, hash: BlockHash, err: &str) {
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

/// Height of `child` = parent height + 1 when parent height is known.
pub(crate) fn parent_height(
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

