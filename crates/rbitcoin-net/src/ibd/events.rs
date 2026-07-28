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
    CONTIG_DENSIFY_AHEAD, MAX_ORDERED_HEADERS, MAX_PEER_POOL, NEAR_DEPTH,
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
                let need_arch_cache = want_headers_beyond_soft_cap(
                    live,
                    st.body.known_len(),
                    st.max_ordered_height.saturating_sub(st.max_archived_height),
                    4096,
                );
                if batch_len >= MAX_HEADERS_RESULTS
                    && live < MAX_ORDERED_HEADERS
                    && (live < ORDERED_HEADERS_SOFT_CAP || need_arch_cache)
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
                let need_arch_cache = want_headers_beyond_soft_cap(
                    live,
                    st.body.known_len(),
                    st.max_ordered_height.saturating_sub(st.max_archived_height),
                    4096,
                );
                if live < MAX_ORDERED_HEADERS
                    && (live < ORDERED_HEADERS_SOFT_CAP || need_arch_cache)
                    && (batch_len >= MAX_HEADERS_RESULTS
                        || header_lag_behind_peers(st, hub.tip_height().unwrap_or(0)) > 2
                        || need_arch_cache)
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
            // Already budget-charged into the archive job pipeline: drop multi-peer
            // / re-get redelivery **without** a second charge. (`pending` alone is
            // wrong — BlockFramed marks pending before decode/charge.)
            if st.body.is_archive_charged(&hash) {
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
            // Priority: parent cache + ContigPark densify horizon (parkable soon).
            let priority = st
                .hash_height
                .get(&hash)
                .map(|&ht| {
                    ht <= tip_h.saturating_add(NEAR_DEPTH)
                        || ht <= write_next.saturating_add(CONTIG_DENSIFY_AHEAD)
                })
                .unwrap_or(false);
            // Approx wire size for RAM budget (already-decoded).
            // Always enqueue the first copy (may overshoot soft budget). Assign
            // stops new densify getdata via `can_assign`; never dump in-flight
            // peer bytes or refuse to decode what a peer already sent. Soft
            // queue size is bounded only by limiting block *requests*.
            let wire_bytes = block.total_size();
            // Durable multi‑GiB queue (budget-enforced). Restart rehydrates via
            // `rehydrate_block_queue_into_archive` → arch_job (no re-getdata).
            // Decoded Block still goes on the RAM job channel for this process.
            {
                use bitcoin::consensus::Encodable;
                let mut payload = Vec::with_capacity(wire_bytes);
                if block.consensus_encode(&mut payload).is_ok() {
                    let raw = hash.to_byte_array();
                    if let Err(e) = hub.query.block_queue_enqueue(
                        height,
                        raw,
                        header_fk.0,
                        &payload,
                    ) {
                        // Budget full or IO: keep RAM path; densify may re-request later.
                        rbitcoin_log::debug!(
                            "ibd: durable block_queue enqueue skipped ({e}) h={height}"
                        );
                    }
                }
            }
            archive_queued.charge(wire_bytes);
            st.body.mark_archive_charged(hash);
            // Prevent re-getdata while prep/writer owns this body.
            st.body.mark_pending(hash);
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
                st.body.clear_archive_charged(&hash);
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
            st.body.clear_archive_charged(&hash);
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
            st.body.clear_archive_charged(&hash);
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
            st.body.clear_archive_charged(&hash);
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
    // Never blacklist the all-zero sentinel (write used to emit this on
    // mis-attributed rejects). Never freeze tip on pipeline "already spent"
    // races that race a successful commit.
    use bitcoin::hashes::Hash;
    if hash.to_byte_array() == [0u8; 32] {
        warn!("ibd: confirm reject ignored zero-hash @{height}: {err}");
        return;
    }
    if err.contains("prevout already spent") {
        // Likely dup write after claim race (fixed separately). Do not
        // permanent-blacklist a valid tip extension.
        warn!(
            "ibd: confirm reject not blacklisted @{height} {hash}: {err} (treat as transient)"
        );
        return;
    }
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

#[cfg(test)]
mod confirm_reject_tests {
    use super::{
        apply_archive_result, apply_confirm_reject, parent_height, update_confirm_lag,
        disconnect_all_peers,
    };
    use super::super::archive::{ArchiveQueueBudget, ArchiveResult};
    use super::super::state::IbdWorkState;
    use super::super::status::LoopStats;
    use bitcoin::hashes::Hash;
    use bitcoin::BlockHash;
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicU32, Ordering};

    fn h(n: u8) -> BlockHash {
        let mut b = [0u8; 32];
        b[0] = n;
        BlockHash::from_byte_array(b)
    }

    /// Confirm reject blacklist: zero-hash + prevout-spent race stay soft;
    /// real script failures permanent-blacklist. Mainnet tip=362595 regression.
    #[test]
    fn confirm_reject_blacklist_surface() {
        let mut st = IbdWorkState::new(Vec::new(), None, Some(100));
        let zero = BlockHash::from_byte_array([0u8; 32]);
        apply_confirm_reject(
            &mut st,
            101,
            zero,
            "consensus: prevout already spent on best chain",
        );
        assert!(!st.body.is_rejected(&zero));

        // Pipeline race: second write of same tip+1 after first committed —
        // blacklisting freezes IBD forever.
        let mut st = IbdWorkState::new(Vec::new(), None, Some(362_594));
        let hash = h(0x29);
        st.body.mark_archived(hash);
        st.ordered.push_back(hash);
        st.ordered_set.insert(hash);
        apply_confirm_reject(
            &mut st,
            362_595,
            hash,
            "consensus: prevout already spent on best chain",
        );
        assert!(
            !st.body.is_rejected(&hash),
            "prevout-spent race must not permanent-blacklist tip+1"
        );
        assert!(
            st.ordered_set.contains(&hash),
            "transient race must leave ordered path intact"
        );

        let mut st = IbdWorkState::new(Vec::new(), None, Some(50));
        let hash = h(7);
        st.body.mark_archived(hash);
        st.ordered.push_back(hash);
        st.ordered_set.insert(hash);
        apply_confirm_reject(
            &mut st,
            51,
            hash,
            "consensus: script verification failed: script false",
        );
        assert!(st.body.is_rejected(&hash));
        assert!(!st.ordered_set.contains(&hash));
    }

    #[test]
    fn parent_height_zero_map_and_unknown() {
        let mut map = HashMap::new();
        let zero = BlockHash::from_byte_array([0u8; 32]);
        assert_eq!(parent_height(&map, &dummy_hub(), zero), Some(0));
        map.insert(h(1), 40);
        assert_eq!(parent_height(&map, &dummy_hub(), h(1)), Some(41));
        assert_eq!(parent_height(&map, &dummy_hub(), h(99)), None);
    }

    fn dummy_hub() -> crate::chain::ChainHub {
        let dir = std::env::temp_dir().join(format!(
            "rbitcoin-ev-parent-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let _ = std::fs::create_dir_all(&dir);
        let q = rbitcoin_query::Query::open_or_create(dir.join("store")).unwrap();
        // Leak dir for process lifetime of test (cleaned by OS tmp).
        crate::chain::ChainHub::new(
            q,
            rbitcoin_consensus::ChainParams::regtest(),
            rbitcoin_consensus::Milestone::NONE,
        )
    }

    #[test]
    fn apply_archive_result_ok_dropped_err_and_lag() {
        let budget = ArchiveQueueBudget::new(16 * 1024 * 1024);
        let stats = LoopStats::default();
        let mut st = IbdWorkState::new(Vec::new(), None, Some(10));
        let ok_h = h(1);
        st.record_height(ok_h, 20);
        st.body.mark_archive_charged(ok_h);
        budget.charge(1000);
        apply_archive_result(
            &mut st,
            ArchiveResult::Ok {
                hash: ok_h,
                wire_bytes: 1000,
            },
            &budget,
            &stats,
        );
        assert!(st.body.is_known_archived(&ok_h));
        assert!(!st.body.is_archive_charged(&ok_h));
        assert_eq!(st.max_archived_height, 20);
        assert_eq!(budget.count(), 0);
        assert_eq!(stats.archived_bodies.load(Ordering::Relaxed), 1);
        // Second Ok does not double-count first-archive.
        budget.charge(1000);
        st.body.mark_archive_charged(ok_h);
        apply_archive_result(
            &mut st,
            ArchiveResult::Ok {
                hash: ok_h,
                wire_bytes: 1000,
            },
            &budget,
            &stats,
        );
        assert_eq!(stats.archived_bodies.load(Ordering::Relaxed), 1);

        let drop_h = h(2);
        st.body.mark_archive_charged(drop_h);
        budget.charge(500);
        apply_archive_result(
            &mut st,
            ArchiveResult::Dropped {
                hash: drop_h,
                wire_bytes: 500,
                requeue: true,
            },
            &budget,
            &stats,
        );
        assert_eq!(st.body.skip_download_cached(&drop_h), Some(false)); // missing

        let err_h = h(3);
        st.body.mark_archive_charged(err_h);
        budget.charge(200);
        apply_archive_result(
            &mut st,
            ArchiveResult::Err {
                hash: err_h,
                err: "boom".into(),
                wire_bytes: 200,
            },
            &budget,
            &stats,
        );
        assert_eq!(st.body.skip_download_cached(&err_h), Some(false));

        let lag = AtomicU32::new(0);
        update_confirm_lag(&lag, Some(5), 20);
        assert_eq!(lag.load(Ordering::Relaxed), 15);
        update_confirm_lag(&lag, None, 3);
        assert_eq!(lag.load(Ordering::Relaxed), 3);

        // No peers → disconnect is a no-op.
        disconnect_all_peers(&mut st);
        assert!(st.slots.is_empty());
    }

    #[test]
    fn apply_peer_event_body_and_control_surface() {
        use super::{apply_peer_event, inject_learned_addrs, drain_ready_peer_and_archive_events};
        use super::super::archive::ArchiveQueueBudget;
        use super::super::peer_io::{PeerEvent, PeerSlot};
        use super::super::state::InflightReq;
        use crate::seeds::AddrMan;
        use bitcoin::block::{Header, Version};
        use bitcoin::CompactTarget;
        use rbitcoin_consensus::{ChainParams, Milestone};
        use rbitcoin_query::Query;
        use std::collections::HashSet;
        use std::net::{IpAddr, Ipv4Addr, SocketAddr};
        use std::sync::atomic::{AtomicU32, AtomicU64};
        use std::sync::Arc;
        use tokio::sync::mpsc;

        fn addr(o: u8) -> SocketAddr {
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 1, 0, o)), 18444)
        }
        fn dummy_slot(id: usize, a: SocketAddr) -> PeerSlot {
            let (cmd_tx, _rx) = mpsc::unbounded_channel();
            let task = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .spawn(async {});
            PeerSlot {
                id,
                addr: a,
                cmd_tx,
                in_flight: HashSet::new(),
                block_progress_ms: Arc::new(AtomicU64::new(0)),
                peer_height: 10,
                connected_ms: 1,
                first_data_ms: AtomicU64::new(0),
                bytes_rx: AtomicU64::new(0),
                alive: true,
                task,
            }
        }
        fn dummy_header(prev: BlockHash, n: u8) -> Header {
            Header {
                version: Version::ONE,
                prev_blockhash: prev,
                merkle_root: bitcoin::TxMerkleNode::from_byte_array([n; 32]),
                time: 1_300_000_000 + u32::from(n),
                bits: CompactTarget::from_consensus(0x207fffff),
                nonce: u32::from(n),
            }
        }

        let dir = std::env::temp_dir().join(format!(
            "rbitcoin-ev-apply-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let _ = std::fs::create_dir_all(&dir);
        let q = Query::open_or_create(dir.join("store")).unwrap();
        let hub = crate::chain::ChainHub::new(q, ChainParams::regtest(), Milestone::NONE);
        hub.ensure_genesis().unwrap();
        let gen = hub.tip_hash().unwrap();

        let mut st = IbdWorkState::new(vec![dummy_slot(1, addr(1))], Some(gen), Some(0));
        st.slots[0].in_flight.insert(h(9));
        st.inflight.insert(h(9), InflightReq::new(1));

        let (arch_tx, _arch_job_rx) = mpsc::unbounded_channel();
        let (_arch_res_tx, mut arch_res_rx) = mpsc::unbounded_channel();
        let budget = ArchiveQueueBudget::new(16 * 1024 * 1024);
        let write_next = AtomicU32::new(1);
        let mut book = AddrMan::new();
        let local = addr(99);

        // BlockFramed frees inflight + marks pending.
        apply_peer_event(
            &mut st,
            &hub,
            PeerEvent::BlockFramed {
                peer: 1,
                hash: h(9),
                wire_bytes: 100,
            },
            &arch_tx,
            &budget,
            &write_next,
            &mut book,
            local,
        );
        assert!(st.inflight.is_empty());
        assert!(st.body.is_pending(&h(9)));

        // Decode fail → missing so re-getdata allowed.
        apply_peer_event(
            &mut st,
            &hub,
            PeerEvent::BlockDecodeFailed {
                peer: 1,
                hash: h(9),
            },
            &arch_tx,
            &budget,
            &write_next,
            &mut book,
            local,
        );
        assert!(!st.body.is_pending(&h(9)));

        // Headers: attach height from tip parent and order.
        let hdr = dummy_header(gen, 1);
        let hash = hdr.block_hash();
        apply_peer_event(
            &mut st,
            &hub,
            PeerEvent::Headers {
                peer: 1,
                headers: vec![hdr],
            },
            &arch_tx,
            &budget,
            &write_next,
            &mut book,
            local,
        );
        assert!(st.known_headers.contains(&hash));
        assert!(st.ordered_set.contains(&hash) || st.hash_height.contains_key(&hash));

        // Empty headers with lag → keep headers_done false.
        st.max_peer_height = 100;
        st.empty_header_streak = 0;
        apply_peer_event(
            &mut st,
            &hub,
            PeerEvent::Headers {
                peer: 1,
                headers: vec![],
            },
            &arch_tx,
            &budget,
            &write_next,
            &mut book,
            local,
        );
        assert!(!st.headers_done);

        // NotFound clears peer inflight.
        st.slots[0].in_flight.insert(h(3));
        st.inflight.insert(h(3), InflightReq::new(1));
        apply_peer_event(
            &mut st,
            &hub,
            PeerEvent::NotFound {
                peer: 1,
                hashes: vec![h(3)],
            },
            &arch_tx,
            &budget,
            &write_next,
            &mut book,
            local,
        );
        assert!(!st.inflight.contains_key(&h(3)));

        // Addrs + inject filter.
        inject_learned_addrs(&mut book, &[], local, 1);
        inject_learned_addrs(
            &mut book,
            &[addr(2), local, SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 1)],
            local,
            1,
        );
        assert!(book.entry(&addr(2)).is_some());

        // Dead releases work.
        st.slots[0].in_flight.insert(h(4));
        st.inflight.insert(h(4), InflightReq::new(1));
        apply_peer_event(
            &mut st,
            &hub,
            PeerEvent::Dead {
                peer: 1,
                reason: "bye".into(),
            },
            &arch_tx,
            &budget,
            &write_next,
            &mut book,
            local,
        );
        assert!(!st.slots[0].alive);
        assert!(!st.inflight.contains_key(&h(4)));

        // Drain empty channels.
        let (body_tx, mut body_rx) = mpsc::unbounded_channel();
        let (ctrl_tx, mut ctrl_rx) = mpsc::unbounded_channel();
        let stats = LoopStats::default();
        let ok = drain_ready_peer_and_archive_events(
            &mut st,
            &hub,
            &mut body_rx,
            &mut ctrl_rx,
            &mut arch_res_rx,
            &arch_tx,
            &budget,
            &write_next,
            &stats,
            &mut book,
            local,
        )
        .unwrap();
        assert!(ok);
        drop(body_tx);
        drop(ctrl_tx);

        let _ = std::fs::remove_dir_all(dir);
    }

    /// Block path charges budget + enqueues ArchiveJob; charged redelivery skips;
    /// beyond-horizon marks missing; empty headers with idle path marks done.
    #[test]
    fn apply_peer_event_block_charge_horizon_and_headers_done() {
        use super::{
            apply_archive_result, apply_peer_event, drain_ready_peer_and_archive_events,
            inject_learned_addrs,
        };
        use super::super::archive::{ArchiveQueueBudget, ArchiveResult};
        use super::super::peer_io::{PeerEvent, PeerSlot};
        use crate::seeds::AddrMan;
        use bitcoin::absolute::LockTime;
        use bitcoin::block::{Header, Version};
        use bitcoin::script::ScriptBuf;
        use bitcoin::transaction::Version as TxVersion;
        use bitcoin::{
            Amount, Block, CompactTarget, OutPoint, Sequence, Transaction, TxIn, TxOut, Witness,
        };
        use rbitcoin_consensus::{ChainParams, Milestone};
        use rbitcoin_query::Query;
        use std::collections::HashSet;
        use std::net::{IpAddr, Ipv4Addr, SocketAddr};
        use std::sync::atomic::{AtomicU32, AtomicU64};
        use std::sync::Arc;
        use tokio::sync::mpsc;

        fn dummy_slot(id: usize) -> PeerSlot {
            let (cmd_tx, _rx) = mpsc::unbounded_channel();
            let task = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .spawn(async {});
            PeerSlot {
                id,
                addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 18444),
                cmd_tx,
                in_flight: HashSet::new(),
                block_progress_ms: Arc::new(AtomicU64::new(0)),
                peer_height: 5,
                connected_ms: 1,
                first_data_ms: AtomicU64::new(0),
                bytes_rx: AtomicU64::new(0),
                alive: true,
                task,
            }
        }
        fn coinbase(height: u32) -> Transaction {
            let mut ss = if height == 0 {
                vec![0x00]
            } else {
                rbitcoin_consensus::bip34_height_script(height)
            };
            while ss.len() < 2 {
                ss.push(0x00);
            }
            Transaction {
                version: TxVersion::ONE,
                lock_time: LockTime::ZERO,
                input: vec![TxIn {
                    previous_output: OutPoint::null(),
                    script_sig: ScriptBuf::from_bytes(ss),
                    sequence: Sequence::MAX,
                    witness: Witness::new(),
                }],
                output: vec![TxOut {
                    value: Amount::from_sat(50_0000_0000),
                    script_pubkey: ScriptBuf::from_bytes(vec![0x51]),
                }],
            }
        }
        fn shell(prev: BlockHash, height: u32, n: u32) -> Block {
            let header = Header {
                version: Version::ONE,
                prev_blockhash: prev,
                merkle_root: bitcoin::TxMerkleNode::from_byte_array([0u8; 32]),
                time: 1_300_000_000 + n,
                bits: CompactTarget::from_consensus(0x207fffff),
                nonce: n,
            };
            let mut b = Block {
                header,
                txdata: vec![coinbase(height)],
            };
            b.header.merkle_root = b.compute_merkle_root().unwrap();
            b
        }

        let dir = std::env::temp_dir().join(format!(
            "rbitcoin-ev-block-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let _ = std::fs::create_dir_all(&dir);
        let q = Query::open_or_create(dir.join("store")).unwrap();
        let hub = crate::chain::ChainHub::new(q, ChainParams::regtest(), Milestone::NONE);
        hub.ensure_genesis().unwrap();
        let gen = hub.tip_hash().unwrap();

        let mut st = IbdWorkState::new(vec![dummy_slot(1)], Some(gen), Some(0));
        let (arch_tx, mut arch_rx) = mpsc::unbounded_channel();
        let budget = ArchiveQueueBudget::new(16 * 1024 * 1024);
        let write_next = AtomicU32::new(1);
        let mut book = AddrMan::new();
        let local = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)), 1);

        // Near block: charge + enqueue.
        let b1 = shell(gen, 1, 1);
        let h1 = b1.block_hash();
        st.record_height(h1, 1);
        st.header_fks
            .insert(h1, hub.ensure_header_fk(&b1.header).unwrap());
        apply_peer_event(
            &mut st,
            &hub,
            PeerEvent::Block {
                peer: 1,
                block: b1.clone(),
            },
            &arch_tx,
            &budget,
            &write_next,
            &mut book,
            local,
        );
        assert_eq!(budget.count(), 1);
        assert!(st.body.is_archive_charged(&h1));
        assert!(st.body.is_pending(&h1));
        let job = arch_rx.try_recv().expect("ArchiveJob enqueued");
        assert_eq!(job.block.block_hash(), h1);
        assert!(job.priority, "height 1 is near tip/write_next");

        // Redelivery while charged: no second charge.
        apply_peer_event(
            &mut st,
            &hub,
            PeerEvent::Block {
                peer: 1,
                block: b1,
            },
            &arch_tx,
            &budget,
            &write_next,
            &mut book,
            local,
        );
        assert_eq!(budget.count(), 1);
        assert!(arch_rx.try_recv().is_err());

        // Beyond ContigPark horizon → mark_missing, no charge.
        let far_h = 1u32 + super::super::CONTIG_DENSIFY_AHEAD + 10;
        let far = shell(h1, far_h, far_h);
        let far_hash = far.block_hash();
        st.record_height(far_hash, far_h);
        let before = budget.count();
        apply_peer_event(
            &mut st,
            &hub,
            PeerEvent::Block {
                peer: 1,
                block: far,
            },
            &arch_tx,
            &budget,
            &write_next,
            &mut book,
            local,
        );
        assert_eq!(budget.count(), before);
        assert_eq!(st.body.skip_download_cached(&far_hash), Some(false));

        // Dropped without requeue leaves missing unset (charge only).
        st.body.mark_archive_charged(h(0x55));
        budget.charge(100);
        apply_archive_result(
            &mut st,
            ArchiveResult::Dropped {
                hash: h(0x55),
                wire_bytes: 100,
                requeue: false,
            },
            &budget,
            &LoopStats::default(),
        );
        assert!(!st.body.is_archive_charged(&h(0x55)));
        assert_eq!(st.body.skip_download_cached(&h(0x55)), None);

        // Empty headers, path idle, lag ≤ 2 → headers_done.
        st.max_peer_height = 0;
        st.empty_header_streak = 0;
        st.ordered.clear();
        st.ordered_set.clear();
        st.inflight.clear();
        // Two empty messages (peers_n.max(2) = 2 with one alive peer).
        for _ in 0..2 {
            apply_peer_event(
                &mut st,
                &hub,
                PeerEvent::Headers {
                    peer: 1,
                    headers: vec![],
                },
                &arch_tx,
                &budget,
                &write_next,
                &mut book,
                local,
            );
        }
        assert!(st.headers_done, "idle empty-header streak marks done");

        // inject at pool cap is a no-op.
        use super::super::MAX_PEER_POOL;
        for i in 0..MAX_PEER_POOL {
            book.add(SocketAddr::new(
                IpAddr::V4(Ipv4Addr::new(11, 0, (i / 256) as u8, (i % 256) as u8)),
                8333,
            ));
        }
        let n0 = book.len();
        inject_learned_addrs(
            &mut book,
            &[SocketAddr::new(IpAddr::V4(Ipv4Addr::new(9, 9, 9, 9)), 8333)],
            local,
            1,
        );
        assert_eq!(book.len(), n0);

        // Drain with archive result + body event on channels.
        let (body_tx, mut body_rx) = mpsc::unbounded_channel();
        let (ctrl_tx, mut ctrl_rx) = mpsc::unbounded_channel();
        let (arch_res_tx, mut arch_res_rx) = mpsc::unbounded_channel();
        let drop_h = h(0x77);
        st.body.mark_archive_charged(drop_h);
        budget.charge(50);
        arch_res_tx
            .send(ArchiveResult::Ok {
                hash: drop_h,
                wire_bytes: 50,
            })
            .unwrap();
        body_tx
            .send(PeerEvent::BlockDecodeFailed {
                peer: 1,
                hash: h(0x88),
            })
            .unwrap();
        ctrl_tx
            .send(PeerEvent::Addrs {
                peer: 1,
                addrs: vec![SocketAddr::new(IpAddr::V4(Ipv4Addr::new(8, 8, 8, 8)), 8333)],
            })
            .unwrap();
        // Book is full so addrs inject is no-op; still exercises drain path.
        let stats = LoopStats::default();
        drain_ready_peer_and_archive_events(
            &mut st,
            &hub,
            &mut body_rx,
            &mut ctrl_rx,
            &mut arch_res_rx,
            &arch_tx,
            &budget,
            &write_next,
            &stats,
            &mut book,
            local,
        )
        .unwrap();
        assert!(st.body.is_known_archived(&drop_h));
        assert!(stats.drain_events.load(Ordering::Relaxed) >= 1);

        let _ = std::fs::remove_dir_all(dir);
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

