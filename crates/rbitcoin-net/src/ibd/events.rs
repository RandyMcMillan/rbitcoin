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
    arch_job_tx: &mpsc::Sender<ArchiveJob>,
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
    arch_job_tx: &mpsc::Sender<ArchiveJob>,
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
            // First copy is charged then enqueued on a **bounded** arch_job queue.
            // Assign stops densify via `can_assign`. When the queue is full (e.g.
            // Class A writer sleeping on tx.head resize), release charge and
            // mark_missing so process RAM for full `Block`s cannot grow unboundedly.
            let wire_bytes = block.total_size();
            archive_queued.charge(wire_bytes);
            st.body.mark_archive_charged(hash);
            // Prevent re-getdata while prep/writer owns this body.
            st.body.mark_pending(hash);
            match arch_job_tx.try_send(ArchiveJob {
                block,
                header_fk,
                priority,
                wire_bytes,
                height,
            }) {
                Ok(()) => {}
                Err(tokio::sync::mpsc::error::TrySendError::Full(_job)) => {
                    archive_queued.release(wire_bytes);
                    st.body.clear_archive_charged(&hash);
                    st.body.mark_missing(hash);
                    static FULL: std::sync::atomic::AtomicU32 =
                        std::sync::atomic::AtomicU32::new(0);
                    let n = FULL.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1;
                    if n <= 5 || n % 100 == 0 {
                        warn!(
                            "ibd: archive job queue full — drop {hash} (release charge; \
                             re-get later; count={n})"
                        );
                    }
                }
                Err(tokio::sync::mpsc::error::TrySendError::Closed(_job)) => {
                    archive_queued.release(wire_bytes);
                    st.body.clear_archive_charged(&hash);
                    st.body.mark_missing(hash);
                    warn!("ibd: archive pipeline closed; drop {hash}");
                }
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
mod arch_job_queue_tests {
    //! Honest regressions for bounded `arch_job` under writer stall (resize).
    //! Drive **production** [`apply_peer_event`] Full path — not a local reimpl.

    use super::{apply_archive_result, apply_peer_event};
    use super::super::archive::{
        ArchiveJob, ArchiveQueueBudget, ArchiveResult, ARCH_JOB_QUEUE_CAP,
    };
    use super::super::peer_io::PeerEvent;
    use super::super::state::IbdWorkState;
    use super::super::status::LoopStats;
    use crate::chain::ChainHub;
    use crate::seeds::AddrMan;
    use bitcoin::absolute::LockTime;
    use bitcoin::block::{Header, Version};
    use bitcoin::hashes::Hash;
    use bitcoin::transaction::Version as TxVersion;
    use bitcoin::{
        Amount, Block, BlockHash, CompactTarget, OutPoint, ScriptBuf, Sequence, Transaction, TxIn,
        TxOut, Witness,
    };
    use rbitcoin_consensus::{ChainParams, Milestone};
    use rbitcoin_primitives::Fk;
    use rbitcoin_query::Query;
    use std::sync::atomic::AtomicU32;
    use std::sync::Arc;

    fn dummy_block(n: u32) -> Block {
        let coinbase = Transaction {
            version: TxVersion::ONE,
            lock_time: LockTime::ZERO,
            input: vec![TxIn {
                previous_output: OutPoint::null(),
                script_sig: ScriptBuf::from_bytes(vec![
                    (n & 0xff) as u8,
                    ((n >> 8) & 0xff) as u8,
                    ((n >> 16) & 0xff) as u8,
                    ((n >> 24) & 0xff) as u8,
                ]),
                sequence: Sequence::MAX,
                witness: Witness::new(),
            }],
            output: vec![TxOut {
                value: Amount::from_sat(50_0000_0000),
                script_pubkey: ScriptBuf::from_bytes(vec![0x51]),
            }],
        };
        let mut b = Block {
            header: Header {
                version: Version::ONE,
                prev_blockhash: BlockHash::from_byte_array([0u8; 32]),
                merkle_root: bitcoin::TxMerkleNode::from_byte_array([0u8; 32]),
                time: 1_700_000_000 + n,
                bits: CompactTarget::from_consensus(0x207fffff),
                nonce: n,
            },
            txdata: vec![coinbase],
        };
        b.header.merkle_root = b.compute_merkle_root().unwrap();
        b
    }

    fn tmp_hub(label: &str) -> (std::path::PathBuf, ChainHub) {
        let dir = std::env::temp_dir().join(format!(
            "rbitcoin-arch-job-{label}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let q = Query::open_or_create(&dir).unwrap();
        let hub = ChainHub::new(q, ChainParams::regtest(), Milestone::NONE);
        (dir, hub)
    }

    /// Production Full path: fill arch_job to capacity via [`apply_peer_event`],
    /// flood more Blocks, assert budget plateaus at enqueued count (not flood N)
    /// and Full-path hashes are mark_missing + not archive_charged. Then drain
    /// with production [`apply_archive_result`] → budget 0.
    ///
    /// Reverting Full→release in events.rs fails this test (budget grows past CAP).
    #[test]
    fn apply_peer_event_arch_job_full_plateau() {
        let (dir, hub) = tmp_hub("full");
        let hub = Arc::new(hub);
        let mut st = IbdWorkState::new(Vec::new(), None, None);
        let budget = ArchiveQueueBudget::new(512 * 1024 * 1024);
        // Tiny queue so flood is cheap; still production try_send Full branch.
        const Q: usize = 4;
        assert!(Q <= ARCH_JOB_QUEUE_CAP);
        let (arch_tx, mut arch_rx) = tokio::sync::mpsc::channel::<ArchiveJob>(Q);
        let write_next = AtomicU32::new(0);
        let mut book = AddrMan::new();
        let local: std::net::SocketAddr = "127.0.0.1:18444".parse().unwrap();
        let stats = LoopStats::default();

        let before = budget.count();
        assert_eq!(before, 0);

        // Flood far past Q — only Q should stay charged+queued.
        const FLOOD: u32 = 40;
        let mut full_hashes = Vec::new();
        let mut enqueued = 0u32;
        for n in 1..=FLOOD {
            let block = dummy_block(n);
            let hash = block.block_hash();
            // Seed header fk so apply does not need store put (empty store).
            st.header_fks.insert(hash, Fk(n as u64));
            // height MAX → horizon check skipped (parkable).
            apply_peer_event(
                &mut st,
                &hub,
                PeerEvent::Block { peer: 0, block },
                &arch_tx,
                &budget,
                &write_next,
                &mut book,
                local,
            );
            if st.body.is_archive_charged(&hash) {
                enqueued += 1;
            } else {
                // Full path: production released charge + mark_missing.
                assert!(
                    !st.body.is_archive_charged(&hash),
                    "Full path must clear archive_charged"
                );
                // mark_missing puts in missing set (skip_download_cached false).
                full_hashes.push(hash);
            }
        }

        let mid_count = budget.count();
        let mid_bytes = budget.bytes();
        // Channel depth = budget.count() for charged-and-held jobs (Full released).
        assert_eq!(
            enqueued as usize, Q,
            "only Q jobs should stay charged/enqueued, enqueued={enqueued}"
        );
        assert_eq!(
            mid_count, Q,
            "budget must plateau at queue depth Q={Q}, got count={mid_count} (if Full release \
             removed, count would approach FLOOD={FLOOD})"
        );
        assert!(
            mid_bytes > 0,
            "enqueued jobs must hold charged bytes"
        );
        assert!(
            full_hashes.len() >= (FLOOD as usize).saturating_sub(Q),
            "most of flood must hit Full path, full_hashes={}",
            full_hashes.len()
        );
        for h in &full_hashes {
            assert!(
                !st.body.is_archive_charged(h),
                "Full-path hash must not stay charged"
            );
        }

        // Production teardown: Err results for queued jobs (writer dead / stop).
        let mut drained = 0usize;
        while let Ok(j) = arch_rx.try_recv() {
            apply_archive_result(
                &mut st,
                ArchiveResult::Err {
                    hash: j.block.block_hash(),
                    err: "test drain".into(),
                    wire_bytes: j.wire_bytes,
                },
                &budget,
                &stats,
            );
            drained += 1;
        }
        assert_eq!(drained, Q);
        assert_eq!(budget.count(), 0, "after production apply_archive_result residual must be 0");
        assert_eq!(budget.bytes(), 0);

        let report = format!(
            "arch_job Full plateau (production apply_peer_event)\n\
             Q={Q} FLOOD={FLOOD} enqueued={enqueued} full_path={}\n\
             budget mid_count={mid_count} mid_bytes={mid_bytes} after=0\n\
             plateau=budget.count()==Q under flood, then 0 after apply_archive_result\n",
            full_hashes.len()
        );
        eprintln!("{report}");
        if let Ok(out) = std::env::var("RBITCOIN_LEAK_PROBE_OUT") {
            let _ = std::fs::create_dir_all(&out);
            let _ = std::fs::write(format!("{out}/arch-job-full-plateau-report.txt"), &report);
        }

        let _ = std::fs::remove_dir_all(dir);
    }

    /// Growth loop at production [`ARCH_JOB_QUEUE_CAP`]: flood Blocks through
    /// [`apply_peer_event`]; process-owned charged count never exceeds CAP.
    #[test]
    fn apply_peer_event_flood_respects_arch_job_cap() {
        let (dir, hub) = tmp_hub("cap");
        let hub = Arc::new(hub);
        let mut st = IbdWorkState::new(Vec::new(), None, Some(0));
        let budget = ArchiveQueueBudget::new(512 * 1024 * 1024);
        let (arch_tx, mut arch_rx) =
            tokio::sync::mpsc::channel::<ArchiveJob>(ARCH_JOB_QUEUE_CAP);
        let write_next = AtomicU32::new(0);
        let mut book = AddrMan::new();
        let local: std::net::SocketAddr = "127.0.0.1:18444".parse().unwrap();
        let stats = LoopStats::default();

        let flood = ARCH_JOB_QUEUE_CAP.saturating_mul(3) as u32;
        let mut peak = 0usize;
        for n in 1..=flood {
            let block = dummy_block(10_000 + n);
            let hash = block.block_hash();
            st.header_fks.insert(hash, Fk(n as u64));
            apply_peer_event(
                &mut st,
                &hub,
                PeerEvent::Block { peer: 0, block },
                &arch_tx,
                &budget,
                &write_next,
                &mut book,
                local,
            );
            peak = peak.max(budget.count());
            assert!(
                budget.count() <= ARCH_JOB_QUEUE_CAP,
                "charged count {} exceeded ARCH_JOB_QUEUE_CAP={ARCH_JOB_QUEUE_CAP}",
                budget.count()
            );
        }
        assert_eq!(
            peak, ARCH_JOB_QUEUE_CAP,
            "peak charged should reach CAP under flood"
        );

        // Drain via production apply_archive_result.
        while let Ok(j) = arch_rx.try_recv() {
            apply_archive_result(
                &mut st,
                ArchiveResult::Dropped {
                    hash: j.block.block_hash(),
                    wire_bytes: j.wire_bytes,
                    requeue: false,
                },
                &budget,
                &stats,
            );
        }
        assert_eq!(budget.count(), 0);
        assert_eq!(budget.bytes(), 0);

        let report = format!(
            "arch_job CAP growth loop\n\
             CAP={ARCH_JOB_QUEUE_CAP} flood={flood} peak_charged={peak} after=0\n"
        );
        eprintln!("{report}");
        if let Ok(out) = std::env::var("RBITCOIN_LEAK_PROBE_OUT") {
            let _ = std::fs::create_dir_all(&out);
            let _ = std::fs::write(format!("{out}/arch-job-cap-growth-report.txt"), &report);
        }
        let _ = std::fs::remove_dir_all(dir);
    }
}

#[cfg(test)]
mod confirm_reject_tests {
    use super::apply_confirm_reject;
    use super::super::state::IbdWorkState;
    use bitcoin::hashes::Hash;
    use bitcoin::BlockHash;

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

