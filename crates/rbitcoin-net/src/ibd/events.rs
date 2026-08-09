//! Peer / archive event drain and apply (IBD main loop).

use super::assign::clear_hash_inflight;
use super::assign_plan::{remove_from_ordered, want_headers_beyond_soft_cap};
use super::dial::{release_peer_block_work, request_headers, request_headers_from};
use super::exit::{
    header_lag_behind_peers, should_log_empty_headers_lag, should_rerequest_headers_on_empty_lag,
};
use super::path::work_path_tips;
use super::peer_io::{note_block_progress, note_block_rx, PeerCmd, PeerEvent};
use super::state::IbdWorkState;
use super::status::LoopStats;
use super::{CONTIG_DENSIFY_AHEAD, MAX_ORDERED_HEADERS, MAX_PEER_POOL, ORDERED_HEADERS_SOFT_CAP};
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

/// First 80 bytes of a consensus-serialized block → header (no full block decode).
fn decode_block_header_prefix(payload: &[u8]) -> Option<bitcoin::block::Header> {
    use bitcoin::consensus::Decodable;
    if payload.len() < 80 {
        return None;
    }
    let mut cur = std::io::Cursor::new(&payload[..80]);
    bitcoin::block::Header::consensus_decode(&mut cur).ok()
}

/// Header/control events per turn (anti-livelock under multi-peer header spam).
const CTRL_DRAIN_EVENT_BUDGET: u64 = 512;
const CTRL_DRAIN_TIME_BUDGET: Duration = Duration::from_millis(5);
/// Body path (framed/decoded blocks): process as much as possible so delivered
/// bytes are not stranded behind headers. Soft wall so cancel/assign still run.
const BODY_DRAIN_TIME_BUDGET: Duration = Duration::from_millis(40);

/// Non-blocking drain of archive results + peer events.
///
/// **Priority:** body (`BlockFramed`/…) → headers.
/// Delivered block bytes must not wait on header floods (single-FIFO waste).
/// Headers remain budgeted so apply cannot livelock.
///
/// Archive-job dual-track is gone: sole Class A path is body queue → confirm.
pub(crate) fn drain_ready_peer_and_archive_events(
    st: &mut IbdWorkState,
    hub: &ChainHub,
    body_rx: &mut mpsc::UnboundedReceiver<PeerEvent>,
    ctrl_rx: &mut mpsc::UnboundedReceiver<PeerEvent>,
    archive_write_next: &AtomicU32,
    loop_stats: &LoopStats,
    peer_book: &mut AddrMan,
    local_addr: SocketAddr,
    confirm_feed: Option<&super::confirm::ConfirmFeed>,
) -> Result<bool, NetError> {
    let t0 = Instant::now();
    let mut events = 0u64;

    // 1) Body path: framed block wire — drain until empty or soft time budget.
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
                    archive_write_next,
                    peer_book,
                    local_addr,
                    confirm_feed,
                );
            }
            Err(_) => break,
        }
    }

    // 2) Headers / other control — budgeted (never drop; resume next turn).
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
                    archive_write_next,
                    peer_book,
                    local_addr,
                    confirm_feed,
                );
            }
            Err(_) => break,
        }
    }

    loop_stats
        .drain_ns
        .fetch_add(t0.elapsed().as_nanos() as u64, Ordering::Relaxed);
    loop_stats.drain_events.fetch_add(events, Ordering::Relaxed);
    Ok(true)
}

pub(crate) fn apply_peer_event(
    st: &mut IbdWorkState,
    hub: &ChainHub,
    ev: PeerEvent,
    archive_write_next: &AtomicU32,
    peer_book: &mut AddrMan,
    local_addr: SocketAddr,
    confirm_feed: Option<&super::confirm::ConfirmFeed>,
) {
    match ev {
        PeerEvent::Headers { peer, headers } => {
            let batch_len = headers.len();
            let mut added = 0usize;
            // Sequential height walk: after tip drains `ordered` + hygiene, mid-batch
            // parents are often only known via the previous header in this message.
            let mut batch_prev: Option<(BlockHash, u32)> = None;
            for hdr in headers {
                let hash = hdr.block_hash();
                let prev = hdr.prev_blockhash;
                // Multi-peer overlap re-sends the same 2000-header windows. Full
                // ensure_header_fk (hash-head lookup + maybe put) on every repeat
                // made drain cost climb from ~µs to ~ms per event and froze the
                // main loop for tens of seconds with no status lines.
                let already_known =
                    st.known_headers.contains(&hash) && st.header_fks.contains_key(&hash);
                let height = parent_height(&st.hash_height, hub, prev).or_else(|| {
                    batch_prev.and_then(|(ph, pht)| {
                        if ph == prev {
                            Some(pht.saturating_add(1))
                        } else {
                            None
                        }
                    })
                });
                if let Some(h) = height {
                    if !st.hash_height.contains_key(&hash) {
                        st.record_height(hash, h);
                    }
                    st.max_peer_height = st.max_peer_height.max(h);
                    st.max_ordered_height = st.max_ordered_height.max(h);
                    batch_prev = Some((hash, h));
                }
                if !already_known {
                    // Persist header row once and cache fk — Block path must not re-hit store.
                    if !st.header_fks.contains_key(&hash) {
                        if let Ok(fk) = hub.ensure_header_fk(&hdr) {
                            st.header_fks.insert(hash, fk);
                        }
                    }
                }
                if hub.has_block(&hash) {
                    // Still record height (above) so the next header in the batch can
                    // chain parent_height after hygiene wiped hash_height of past tip.
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
                // Re-admit even when already known: tip drain + hygiene empty
                // `ordered` while peers re-serve the same window; early-return without
                // this froze mainnet at tip=292000 (ordered=0, known_hdr=4000, hole=0,
                // bq=0, hard path reset every 180s).
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
                let need_ready_headroom = want_headers_beyond_soft_cap(
                    live,
                    st.body.known_len(),
                    st.max_ordered_height.saturating_sub(st.max_ready_height),
                    4096,
                );
                if batch_len >= MAX_HEADERS_RESULTS
                    && live < MAX_ORDERED_HEADERS
                    && (live < ORDERED_HEADERS_SOFT_CAP || need_ready_headroom)
                {
                    let tips = work_path_tips(st);
                    let _ =
                        request_headers_from(&st.slots, peer, hub, &mut st.header_req_seq, &tips);
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
                    // false EOF (locator stuck / peer-horizon skew). Keep syncing;
                    // never mark headers_done.
                    //
                    // **Do not reset** `empty_header_streak` here: a prior reset every
                    // 8 empties re-triggered `streak == 1` WARNs and re-getheaders
                    // storms (mainnet: thousands of "empty headers but lag=…" lines).
                    if should_log_empty_headers_lag(st.empty_header_streak) {
                        warn!(
                            "ibd: empty headers but lag={lag} behind max_peer_height={} (known≈{}, tip={tip_h}) — keep header sync",
                            st.max_peer_height,
                            st.max_ready_height
                                .max(st.hash_height.values().copied().max().unwrap_or(0)),
                        );
                    }
                    st.headers_done = false;
                    if should_rerequest_headers_on_empty_lag(st.empty_header_streak) {
                        let tips = work_path_tips(st);
                        let _ = request_headers(&st.slots, hub, &mut st.header_req_seq, &tips);
                    }
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
                let need_ready_headroom = want_headers_beyond_soft_cap(
                    live,
                    st.body.known_len(),
                    st.max_ordered_height.saturating_sub(st.max_ready_height),
                    4096,
                );
                if live < MAX_ORDERED_HEADERS
                    && (live < ORDERED_HEADERS_SOFT_CAP || need_ready_headroom)
                    && (batch_len >= MAX_HEADERS_RESULTS
                        || header_lag_behind_peers(st, hub.tip_height().unwrap_or(0)) > 2
                        || need_ready_headroom)
                {
                    let tips = work_path_tips(st);
                    let _ =
                        request_headers_from(&st.slots, peer, hub, &mut st.header_req_seq, &tips);
                }
            }
        }
        PeerEvent::BlockFramed {
            peer,
            hash,
            payload,
        } => {
            // Raw frame payload → in-RAM body queue (no peer full Block decode;
            // block hash already known from framing; parse/txids on confirm pack).
            let wire_bytes = payload.len();
            note_block_rx(&mut st.slots, peer, wire_bytes);
            clear_hash_inflight(&mut st.slots, &mut st.inflight, hash);
            // Drop only permanent reject / already-confirmed. Class A
            // `is_known_archived` alone is **not** claim-ready (confirm needs BQ
            // wire). Resume seed marks Class A known without BQ — still accept
            // peer wire so tip can claim after re-getdata.
            if st.body.is_rejected(&hash) || hub.has_block(&hash) {
                return;
            }
            // Prefer RAM-cached fk from getheaders (no store lock on hot path).
            let header_fk = if let Some(&fk) = st.header_fks.get(&hash) {
                fk
            } else {
                // Header only (first 80 payload bytes) — not a full block decode.
                let header = match decode_block_header_prefix(&payload) {
                    Some(h) => h,
                    None => {
                        st.body.mark_missing(hash);
                        return;
                    }
                };
                match hub.ensure_header_fk(&header) {
                    Ok(fk) => {
                        st.header_fks.insert(hash, fk);
                        fk
                    }
                    Err(e) => {
                        warn!("ibd: ensure_header {hash}: {e}");
                        st.body.mark_missing(hash);
                        return;
                    }
                }
            };
            let tip_h = hub.tip_height().unwrap_or(0);
            let height = st.hash_height.get(&hash).copied();
            let Some(height) = height else {
                // Headers map not ready yet — re-get after height is known.
                st.body.mark_missing(hash);
                return;
            };
            let write_next = archive_write_next.load(Ordering::Relaxed);
            let tip_hi = tip_h.saturating_add(CONTIG_DENSIFY_AHEAD);
            let densify_hi = write_next.saturating_add(CONTIG_DENSIFY_AHEAD);
            if height > tip_hi && height > densify_hi {
                st.body.mark_missing(hash);
                return;
            }
            // Side-branch body at tip height (or any competing hash): hold by
            // hash for most-work reorg. BQ is height first-wins and cannot store
            // a same-height sibling of the confirmed tip.
            let tip_hash = hub.tip_hash();
            if height <= tip_h && tip_hash != Some(hash) {
                if let Ok(block) = bitcoin::consensus::deserialize::<bitcoin::Block>(&payload) {
                    st.reorg.hold_body(block);
                    st.body.mark_pending(hash);
                    if try_complete_awaiting_reorg(st, hub) {
                        return;
                    }
                }
            }
            // Already have wire for this height — keep first copy only.
            if hub.query.block_queue_has_height(height) {
                st.body.mark_pending(hash);
                if let Some(feed) = confirm_feed {
                    feed.note(height, hash);
                }
                // Still may complete reorg if this hash filled awaiting need via hold.
                let _ = try_complete_awaiting_reorg(st, hub);
                return;
            }
            let raw = hash.to_byte_array();
            match hub
                .query
                .block_queue_offer(height, raw, header_fk.0, &payload)
            {
                Ok(_offer) => {
                    // Tip+1 wire may unlock awaiting reorg once winner is held.
                    let _ = try_complete_awaiting_reorg(st, hub);
                }
                Err(e) => {
                    rbitcoin_log::warn!("ibd: body queue offer failed ({e}) h={height}");
                    st.body.mark_missing(hash);
                    return;
                }
            }
            st.body.mark_pending(hash);
            if let Some(feed) = confirm_feed {
                feed.note(height, hash);
            }
        }
        PeerEvent::BlockDecodeFailed { peer, hash } => {
            note_block_progress(&mut st.slots, peer);
            clear_hash_inflight(&mut st.slots, &mut st.inflight, hash);
            if st.body.is_pending(&hash) {
                st.body.mark_missing(hash);
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

/// Permanent confirm failure: drop from the work path and never re-offer.
///
/// Without this, `offer_confirm_ready` re-noted ghost/re-queued hashes and the
/// confirm engine spun on the same BadPrev / missing-prevout tip+1 (signet log:
/// same hash every ~30s with tip frozen).
pub(crate) fn update_confirm_lag(lag: &AtomicU32, tip: Option<u32>, max_ready: u32) {
    let t = tip.unwrap_or(0);
    lag.store(max_ready.saturating_sub(t), Ordering::Relaxed);
}

pub(crate) fn apply_confirm_reject(
    st: &mut IbdWorkState,
    height: u32,
    hash: BlockHash,
    err: &str,
    // When set, drop bad body-queue payload so densify can re-getdata a good block.
    query: Option<&rbitcoin_query::Query>,
    // When set, BadPrev may trigger most-work reorg onto a competing path.
    hub: Option<&crate::chain::ChainHub>,
) {
    // Never blacklist the all-zero sentinel (write used to emit this on
    // mis-attributed rejects).
    use bitcoin::hashes::Hash;
    if hash.to_byte_array() == [0u8; 32] {
        warn!("ibd: confirm reject ignored zero-hash @{height}: {err}");
        return;
    }
    // Soft re-getdata / re-offer: **not permanent blacklist**.
    //
    // - Wire-only: wrong/corrupt body (`unexpected previous header`) — drop
    //   payload and densify. Mainnet tip freeze at 125653 blacklisted this.
    // - **Competing path BadPrev** (wire prev is a known non-tip header): try
    //   most-work reorg before soft re-get (mainnet 961632 class livelock).
    // - Header window: `missing retarget first header` can race when a large
    //   pack needs a retarget base that is not yet visible to `header_at_height`
    //   (or was briefly absent). Permanent blacklist freezes tip+1 forever
    //   (signet partial IBD @~42k). Requeue without blacklisting; claim will
    //   retry once headers/plans catch up.
    //
    // Soft re-getdata **only** for bad wire / missing header window — not for
    // store invariants or tip-ahead races. Soft-requeue of "parent unresolved" /
    // "fk mismatch" hid real bugs and livelocked tip; a corrupt store or bad
    // block must permanent-blacklist so the operator sees the failure.
    //
    // Soft re-get: bad **wire** or **corrupt Class A reconstruct** (merkle).
    // The block *hash* is fine — never permanent-blacklist; clear bad association
    // so densify/getdata can refill. Mainnet tip+1 stall: Class A body for
    // tip+1 failed merkle, hash was blacklisted, tip froze with hole=0.
    let soft_wire = err.contains("unexpected previous header")
        || err.contains("unexpected previous")
        || err.contains("missing retarget first header")
        || err.contains("merkle root mismatch");
    if soft_wire {
        // Competing-path BadPrev: attempt reorg before soft re-get livelock.
        if super::reorg::is_bad_prev_err(err) {
            if let Some(h) = hub {
                if try_reorg_on_bad_prev(st, h, height, hash) {
                    return;
                }
            }
        }
        clear_hash_inflight(&mut st.slots, &mut st.inflight, hash);
        // Evict bad wire so claim_ready is false until a good body arrives.
        if let Some(q) = query {
            let _ = q.block_queue_dequeue_height(height);
            if err.contains("merkle root mismatch") {
                // Corrupt Class A association — drop so rehydrate will not
                // reconstruct the same bad body on the next restart.
                match q.clear_archived_body(hash.as_byte_array()) {
                    Ok(true) => warn!(
                        "ibd: cleared corrupt Class A body for {hash} @{height} (merkle mismatch)"
                    ),
                    Ok(false) => {}
                    Err(e) => warn!("ibd: clear Class A body {hash} @{height}: {e}"),
                }
            }
        }
        st.body.mark_missing(hash);
        // Ensure densify can re-request: not archived-known after Class A clear.
        st.body.demote_known(hash);
        warn!("ibd: confirm reject soft @{height} {hash}: {err} (re-getdata, not blacklisted)");
        return;
    }
    if let Some(q) = query {
        let _ = q.block_queue_dequeue_height(height);
    }
    st.body.mark_rejected(hash);
    remove_from_ordered(&mut st.ordered, &mut st.ordered_set, hash);
    clear_hash_inflight(&mut st.slots, &mut st.inflight, hash);
    // Rate-limit follow-up noise; the confirm engine already logged the reject.
    static N: AtomicU32 = AtomicU32::new(0);
    let n = N.fetch_add(1, Ordering::Relaxed) + 1;
    if n <= 8 || n % 50 == 0 {
        warn!("ibd: confirm reject applied {hash} @{height}: {err} (blacklisted, count={n})");
    }
}

/// Load a full block for `hash` from reorg held map, Class A, or BQ-by-hash.
fn load_reorg_body(
    st: &IbdWorkState,
    hub: &crate::chain::ChainHub,
    hash: BlockHash,
) -> Option<bitcoin::Block> {
    use bitcoin::consensus::deserialize;
    use bitcoin::hashes::Hash as _;
    if let Some(b) = st.reorg.get_held(&hash) {
        return Some(b);
    }
    if let Ok(Some(b)) = hub.query.reconstruct_block_by_hash(&hash.to_byte_array()) {
        return Some(b);
    }
    if let Ok(Some(wire)) = hub.query.block_queue_payload_by_hash(&hash.to_byte_array()) {
        if let Ok(b) = deserialize::<bitcoin::Block>(&wire) {
            return Some(b);
        }
    }
    None
}

/// On BadPrev, if the rejected body's prev is a known competing header, try
/// most-work reorg onto `[winning_prev, rejected_body, …]`. Returns true if
/// tip moved (caller should not soft-reget).
fn try_reorg_on_bad_prev(
    st: &mut IbdWorkState,
    hub: &crate::chain::ChainHub,
    height: u32,
    hash: BlockHash,
) -> bool {
    use super::reorg::{apply_sibling_winning_path, classify_bad_prev, BadPrevClass};
    use crate::chain::AcceptOutcome;
    use bitcoin::consensus::deserialize;
    use rbitcoin_log::info;

    let Some(tip) = hub.tip_hash() else {
        return false;
    };
    // Ext body: BQ at reject height, held map, Class A, or BQ-by-hash.
    let ext = hub
        .query
        .block_queue_payload(height)
        .ok()
        .flatten()
        .and_then(|wire| deserialize::<bitcoin::Block>(&wire).ok())
        .or_else(|| load_reorg_body(st, hub, hash));
    let Some(ext) = ext else {
        return false;
    };
    // Always hold tip+1 for gather retries (BQ height slot is fragile under soft-reget).
    st.reorg.hold_body(ext.clone());
    let wire_prev = ext.header.prev_blockhash;
    match classify_bad_prev(hub, wire_prev, tip) {
        BadPrevClass::CorruptWire { .. } => false,
        BadPrevClass::CompetingPath {
            winning_prev,
            losing_tip,
        } => {
            info!(
                "ibd: BadPrev competing path @{height}: tip={losing_tip} wire_prev={winning_prev} — trying most-work reorg"
            );
            let win_block = load_reorg_body(st, hub, winning_prev);
            let Some(win_block) = win_block else {
                // Same-height competitor is rarely Class A (confirm archives tip path
                // only) and cannot share the tip height BQ slot. Hold ext, densify
                // the winner by hash, complete reorg when its body arrives.
                st.reorg.set_awaiting(ext.clone(), vec![winning_prev]);
                st.body.mark_missing(winning_prev);
                if st.ordered_set.insert(winning_prev) {
                    st.ordered.push_front(winning_prev);
                }
                // Map winner at tip height for assign band walk without overwriting
                // tip's height_to_hash entry used by confirm (tip stays mapped).
                let tip_h = hub.tip_height().unwrap_or(0);
                st.hash_height.insert(winning_prev, tip_h);
                st.known_headers.insert(winning_prev);
                warn!(
                    "ibd: competing reorg awaiting body for winning prev {winning_prev} (held tip+1 {hash})"
                );
                return false;
            };
            st.reorg.hold_body(win_block.clone());
            match apply_sibling_winning_path(hub, win_block, &[ext], &mut st.reorg) {
                Ok(AcceptOutcome::Accepted { height: new_h }) => {
                    info!("ibd: most-work reorg after BadPrev → tip_h={new_h}");
                    let _ = hub.query.block_queue_dequeue_height(height);
                    clear_hash_inflight(&mut st.slots, &mut st.inflight, hash);
                    st.body.mark_archived(hash);
                    st.body.mark_archived(winning_prev);
                    remove_from_ordered(&mut st.ordered, &mut st.ordered_set, losing_tip);
                    st.reorg.clear_awaiting();
                    true
                }
                Ok(other) => {
                    warn!("ibd: competing reorg not applied: {other:?}");
                    false
                }
                Err(e) => {
                    warn!("ibd: competing reorg failed: {e}");
                    false
                }
            }
        }
    }
}

/// After a side-branch body is held, try to finish an awaiting reorg.
fn try_complete_awaiting_reorg(st: &mut IbdWorkState, hub: &crate::chain::ChainHub) -> bool {
    use super::reorg::apply_sibling_winning_path;
    use crate::chain::AcceptOutcome;
    use rbitcoin_log::info;

    let Some(awaiting) = st.reorg.awaiting().cloned() else {
        return false;
    };
    let mut missing = Vec::new();
    let mut wins = Vec::new();
    for h in &awaiting.need {
        if let Some(b) = load_reorg_body(st, hub, *h) {
            st.reorg.hold_body(b.clone());
            wins.push(b);
        } else {
            missing.push(*h);
        }
    }
    if !missing.is_empty() {
        st.reorg.set_awaiting(awaiting.held_tip, missing);
        return false;
    }
    // Sibling path: first need is winning prev at tip height; held_tip is tip+1.
    let Some(win_block) = wins.into_iter().next() else {
        return false;
    };
    let ext = awaiting.held_tip;
    let losing = hub.tip_hash();
    match apply_sibling_winning_path(hub, win_block.clone(), &[ext.clone()], &mut st.reorg) {
        Ok(AcceptOutcome::Accepted { height: new_h }) => {
            info!("ibd: most-work reorg completed after body gather → tip_h={new_h}");
            let tip_h = hub.tip_height().unwrap_or(0);
            let _ = hub.query.block_queue_dequeue_height(tip_h);
            clear_hash_inflight(&mut st.slots, &mut st.inflight, ext.block_hash());
            st.body.mark_archived(ext.block_hash());
            st.body.mark_archived(win_block.block_hash());
            if let Some(l) = losing {
                remove_from_ordered(&mut st.ordered, &mut st.ordered_set, l);
            }
            st.reorg.clear_awaiting();
            true
        }
        Ok(other) => {
            warn!("ibd: awaiting reorg not applied: {other:?}");
            false
        }
        Err(e) => {
            warn!("ibd: awaiting reorg failed: {e}");
            false
        }
    }
}

#[cfg(test)]
mod confirm_reject_tests {
    use super::super::state::IbdWorkState;
    use super::apply_confirm_reject;
    use bitcoin::hashes::Hash;
    use bitcoin::BlockHash;

    fn h(n: u8) -> BlockHash {
        let mut b = [0u8; 32];
        b[0] = n;
        BlockHash::from_byte_array(b)
    }

    /// Soft re-get is wire-only (`unexpected previous header`). Internal
    /// invariants permanent-blacklist. Zero-hash ignored. Mainnet regressions
    /// documented in comments (125653 wire, 219562 denserels, 269050 seal).
    #[test]
    fn confirm_reject_blacklist_surface() {
        let mut st = IbdWorkState::new(Vec::new(), None, Some(100));
        let zero = BlockHash::from_byte_array([0u8; 32]);
        apply_confirm_reject(
            &mut st,
            101,
            zero,
            "consensus: prevout already spent on best chain",
            None,
            None,
        );
        assert!(!st.body.is_rejected(&zero));

        // Script fail → permanent.
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
            None,
            None,
        );
        assert!(st.body.is_rejected(&hash));
        assert!(!st.ordered_set.contains(&hash));

        // Internal denserels invariant → permanent (fix pin layout, not soft).
        let mut st = IbdWorkState::new(Vec::new(), None, Some(219_561));
        let hash = h(0x5b);
        st.body.mark_archived(hash);
        st.ordered.push_back(hash);
        st.ordered_set.insert(hash);
        apply_confirm_reject(
            &mut st,
            219_562,
            hash,
            "consensus: store: corrupt record: invariant: spend annotate missing pin denserels/abs",
            None,
            None,
        );
        assert!(
            st.body.is_rejected(&hash),
            "denserels layout miss is permanent (fix pipeline, not soft-reget)"
        );

        // parent create_fk unresolved / fk mismatch: permanent (store or pipeline bug).
        let mut st = IbdWorkState::new(Vec::new(), None, Some(269_049));
        let hash = h(0x53);
        st.body.mark_archived(hash);
        st.ordered.push_back(hash);
        st.ordered_set.insert(hash);
        apply_confirm_reject(
            &mut st,
            269_050,
            hash,
            "consensus: store: corrupt record: archive: parent create_fk unresolved (contiguous batch required)",
            None,
            None,
        );
        assert!(
            st.body.is_rejected(&hash),
            "parent create_fk unresolved is permanent (fix pipeline, not soft-requeue)"
        );
        let mut st = IbdWorkState::new(Vec::new(), None, Some(961_467));
        let hash = h(0x68);
        st.body.mark_archived(hash);
        apply_confirm_reject(
            &mut st,
            961_468,
            hash,
            "consensus: store: corrupt record: tx put_full_batch fk mismatch (plan not committed in order)",
            None,
            None,
        );
        assert!(
            st.body.is_rejected(&hash),
            "fk mismatch is permanent (not tip-ahead soft requeue)"
        );

        // Merkle mismatch (corrupt Class A reconstruct) → soft re-get, not blacklist.
        // Drive clear_archived_body when a Query is present (production IBD path).
        let dir = std::env::temp_dir().join(format!(
            "rbitcoin-ev-merkle-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let _ = std::fs::create_dir_all(&dir);
        let q = rbitcoin_query::Query::open_or_create(dir.join("store")).unwrap();
        let hdr = rbitcoin_store::HeaderRecord {
            prev_fk: rbitcoin_primitives::Fk::NULL,
            version: 1,
            timestamp: 1,
            bits: 0x207fffff,
            nonce: 0xae,
            merkle_root: [0xae; 32],
            hash: h(0xae).to_byte_array(),
        };
        let hfk = q.put_header(&hdr).unwrap();
        // Associate a dummy Class A range so clear_body has something to drop.
        q.store()
            .header_txs
            .put_range(hfk, rbitcoin_primitives::Fk(1), 1)
            .unwrap();
        assert!(q.store().header_txs.has_body(hfk).unwrap());

        let mut st = IbdWorkState::new(Vec::new(), None, Some(938_453));
        let hash = h(0xae);
        st.body.mark_archived(hash);
        st.ordered.push_back(hash);
        st.ordered_set.insert(hash);
        apply_confirm_reject(
            &mut st,
            938_454,
            hash,
            "consensus: bad block: merkle root mismatch",
            Some(&q),
            None,
        );
        assert!(
            !st.body.is_rejected(&hash),
            "merkle mismatch must soft re-get (Class A body may be corrupt)"
        );
        assert!(
            !st.body.is_known_archived(&hash),
            "merkle mismatch should demote Class A known so densify re-gets"
        );
        assert!(
            !q.store().header_txs.has_body(hfk).unwrap(),
            "soft re-get must clear corrupt Class A association"
        );
        let _ = std::fs::remove_dir_all(&dir);

        // Wire-only soft: unexpected previous header → re-getdata, not blacklist.
        let mut st = IbdWorkState::new(Vec::new(), None, Some(125_652));
        let hash = h(0x44);
        st.body.mark_archived(hash);
        st.ordered.push_back(hash);
        st.ordered_set.insert(hash);
        apply_confirm_reject(
            &mut st,
            125_653,
            hash,
            "consensus: unexpected previous header",
            None,
            None,
        );
        assert!(
            !st.body.is_rejected(&hash),
            "bad wire must soft re-getdata, not permanent-blacklist tip+1"
        );
        // Retarget window miss: soft (do not freeze tip+1 permanently).
        let mut st = IbdWorkState::new(Vec::new(), None, Some(42_284));
        let hash = h(0x42);
        st.body.mark_archived(hash);
        st.ordered.push_back(hash);
        st.ordered_set.insert(hash);
        apply_confirm_reject(
            &mut st,
            42_285,
            hash,
            "consensus: bad header: missing retarget first header",
            None,
            None,
        );
        assert!(
            !st.body.is_rejected(&hash),
            "missing retarget first header must soft, not permanent-blacklist tip+1"
        );
        assert!(
            st.ordered_set.contains(&hash),
            "soft path leaves ordered path intact"
        );

        // prevout-spent: permanent if it reaches here (write should skip-accept
        // when already committed; soft was a race bandaid).
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
            None,
            None,
        );
        assert!(
            st.body.is_rejected(&hash),
            "prevout-spent is permanent at reject layer (write skip-if-committed)"
        );
    }

    /// Competing BadPrev with bodies available reorgs onto winning path (not soft-livelock).
    #[test]
    fn bad_prev_competing_path_reorgs_via_apply_confirm_reject() {
        use crate::chain::ChainHub;
        use bitcoin::absolute::LockTime;
        use bitcoin::block::{Header, Version};
        use bitcoin::consensus::serialize;
        use bitcoin::script::ScriptBuf;
        use bitcoin::transaction::Version as TxVersion;
        use bitcoin::{
            Amount, CompactTarget, OutPoint, Sequence, Target, Transaction, TxIn, TxOut, Witness,
        };
        use rbitcoin_consensus::{ChainParams, Milestone};
        use rbitcoin_query::Query;
        use std::time::{SystemTime, UNIX_EPOCH};

        if std::env::var_os("RBITCOIN_HEAD_SCALE").is_none() {
            std::env::set_var("RBITCOIN_HEAD_SCALE", "tiny");
        }
        let dir = std::env::temp_dir().join(format!(
            "rbitcoin-badprev-reorg-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let _ = std::fs::create_dir_all(&dir);
        let q = Query::open_or_create(dir.join("store")).unwrap();
        let hub = ChainHub::new(q, ChainParams::regtest(), Milestone::NONE);
        hub.ensure_genesis().unwrap();
        let gen = hub.tip_hash().unwrap();

        let coinbase = |height: u32| {
            let mut ss = rbitcoin_consensus::bip34_height_script(height);
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
        };
        let mine = |prev: BlockHash, time: u32, height: u32| {
            let bits = CompactTarget::from_consensus(0x207f_ffff);
            let header = Header {
                version: Version::ONE,
                prev_blockhash: prev,
                merkle_root: bitcoin::TxMerkleNode::from_byte_array([0u8; 32]),
                time,
                bits,
                nonce: 0,
            };
            let mut block = bitcoin::Block {
                header,
                txdata: vec![coinbase(height)],
            };
            block.header.merkle_root = block.compute_merkle_root().unwrap();
            let target = Target::from_compact(bits);
            for nonce in 0..u32::MAX {
                block.header.nonce = nonce;
                if block.header.validate_pow(target).is_ok() {
                    break;
                }
            }
            block
        };

        let lose = mine(gen, 1_300_000_100, 1);
        let mut win = mine(gen, 1_300_000_101, 1);
        if win.block_hash() == lose.block_hash() {
            let target = Target::from_compact(win.header.bits);
            for nonce in 0..u32::MAX {
                win.header.nonce = nonce;
                if win.header.validate_pow(target).is_ok() && win.block_hash() != lose.block_hash()
                {
                    break;
                }
            }
        }
        hub.accept_block(lose.clone()).unwrap();
        hub.ensure_header(&win.header).unwrap();
        let ext = mine(win.block_hash(), 1_300_000_300, 2);
        hub.ensure_header(&ext.header).unwrap();
        // Winning sibling body held by hash (cannot share tip height BQ slot).
        // Ext body on BQ at tip+1 — the real BadPrev wire shape.
        let wire = serialize(&ext);
        hub.query
            .block_queue_offer(2, ext.block_hash().to_byte_array(), 0, &wire)
            .unwrap();

        let mut st = IbdWorkState::new(Vec::new(), Some(lose.block_hash()), Some(1));
        st.ordered.push_back(ext.block_hash());
        st.ordered_set.insert(ext.block_hash());
        // Side body arrives as BlockFramed would: hold by hash before reject.
        st.reorg.hold_body(win.clone());
        let pre_tip = hub.tip_hash().unwrap();
        assert_eq!(pre_tip, lose.block_hash());
        // Sole entry: shipped apply_confirm_reject must reorg tip onto ext.
        apply_confirm_reject(
            &mut st,
            2,
            ext.block_hash(),
            "consensus: unexpected previous header",
            Some(hub.query.as_ref()),
            Some(&hub),
        );
        assert_eq!(
            hub.tip_height(),
            Some(2),
            "apply_confirm_reject alone must reorg when winner body is held"
        );
        assert_eq!(
            hub.tip_hash().unwrap(),
            ext.block_hash(),
            "tip must be winning path tip+1 after BadPrev reorg"
        );
        assert_ne!(hub.tip_hash().unwrap(), pre_tip);
        assert!(!st.body.is_rejected(&ext.block_hash()));
        let _ = std::fs::remove_dir_all(dir);
    }

    /// Wire-path soft budget charged on receive must release on script reject
    /// **and** on soft prevout-spent (write emits Reject when has_block is false;
    #[test]
    fn apply_peer_event_body_and_control_surface() {
        use super::super::peer_io::{PeerEvent, PeerSlot};
        use super::super::state::InflightReq;
        use super::{apply_peer_event, drain_ready_peer_and_archive_events, inject_learned_addrs};
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

        let write_next = AtomicU32::new(1);
        let mut book = AddrMan::new();
        let local = addr(99);

        // BlockFramed without known height → missing (re-getdata after height map).
        apply_peer_event(
            &mut st,
            &hub,
            PeerEvent::BlockFramed {
                peer: 1,
                hash: h(9),
                payload: vec![0u8; 80],
            },
            &write_next,
            &mut book,
            local,
            None,
        );
        assert!(st.inflight.is_empty());
        assert!(!st.body.is_pending(&h(9)));

        // Class A known (resume seed) without BQ: still accept peer wire into the
        // body queue so claim_ready can become true after tip-hole re-getdata.
        let class_a_hash = h(0xca);
        st.body.mark_archived(class_a_hash);
        st.record_height(class_a_hash, 1);
        st.height_to_hash.insert(1, class_a_hash);
        st.header_fks
            .insert(class_a_hash, rbitcoin_primitives::Fk(1));
        st.slots[0].in_flight.insert(class_a_hash);
        st.inflight.insert(class_a_hash, InflightReq::new(1));
        // Minimal framed payload (header prefix + empty body is enough for offer).
        let mut payload = vec![0u8; 81];
        payload[0..4].copy_from_slice(&1u32.to_le_bytes()); // version-ish
        apply_peer_event(
            &mut st,
            &hub,
            PeerEvent::BlockFramed {
                peer: 1,
                hash: class_a_hash,
                payload,
            },
            &write_next,
            &mut book,
            local,
            None,
        );
        assert!(
            st.body.is_pending(&class_a_hash),
            "Class A known must still land in pending after wire offer"
        );
        assert!(
            hub.query.block_queue_has_height(1),
            "Class A known must still enter body queue (claim intake)"
        );

        // Decode fail → missing so re-getdata allowed.
        st.body.mark_pending(h(9));
        apply_peer_event(
            &mut st,
            &hub,
            PeerEvent::BlockDecodeFailed {
                peer: 1,
                hash: h(9),
            },
            &write_next,
            &mut book,
            local,
            None,
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
            &write_next,
            &mut book,
            local,
            None,
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
            &write_next,
            &mut book,
            local,
            None,
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
            &write_next,
            &mut book,
            local,
            None,
        );
        assert!(!st.inflight.contains_key(&h(3)));

        // Addrs + inject filter.
        inject_learned_addrs(&mut book, &[], local, 1);
        inject_learned_addrs(
            &mut book,
            &[
                addr(2),
                local,
                SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 1),
            ],
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
            &write_next,
            &mut book,
            local,
            None,
        );
        assert!(!st.slots[0].alive);
        assert!(!st.inflight.contains_key(&h(4)));

        // Drain empty channels.
        let (body_tx, mut body_rx) = mpsc::unbounded_channel();
        let (ctrl_tx, mut ctrl_rx) = mpsc::unbounded_channel();
        let stats = super::super::status::LoopStats::default();
        let ok = drain_ready_peer_and_archive_events(
            &mut st,
            &hub,
            &mut body_rx,
            &mut ctrl_rx,
            &write_next,
            &stats,
            &mut book,
            local,
            None,
        )
        .unwrap();
        assert!(ok);
        drop(body_tx);
        drop(ctrl_tx);

        let _ = std::fs::remove_dir_all(dir);
    }

    /// Raw BlockFramed → body queue; redelivery keeps one rec; far horizon skipped.
    #[test]
    fn apply_peer_event_block_framed_bq_horizon_and_headers_done() {
        use super::super::peer_io::{PeerEvent, PeerSlot};
        use super::{apply_peer_event, drain_ready_peer_and_archive_events, inject_learned_addrs};
        use crate::seeds::AddrMan;
        use bitcoin::absolute::LockTime;
        use bitcoin::block::{Header, Version};
        use bitcoin::consensus::Encodable;
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
        fn ser(b: &Block) -> Vec<u8> {
            let mut v = Vec::new();
            b.consensus_encode(&mut v).unwrap();
            v
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
        let write_next = AtomicU32::new(1);
        let mut book = AddrMan::new();
        let local = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)), 1);

        let b1 = shell(gen, 1, 1);
        let h1 = b1.block_hash();
        st.record_height(h1, 1);
        st.header_fks
            .insert(h1, hub.ensure_header_fk(&b1.header).unwrap());
        apply_peer_event(
            &mut st,
            &hub,
            PeerEvent::BlockFramed {
                peer: 1,
                hash: h1,
                payload: ser(&b1),
            },
            &write_next,
            &mut book,
            local,
            None,
        );
        assert!(st.body.is_pending(&h1));
        assert!(hub.query.block_queue_has_height(1));

        apply_peer_event(
            &mut st,
            &hub,
            PeerEvent::BlockFramed {
                peer: 1,
                hash: h1,
                payload: ser(&b1),
            },
            &write_next,
            &mut book,
            local,
            None,
        );
        assert_eq!(hub.query.block_queue_stats().2, 1);

        let far_h = 1u32 + super::super::CONTIG_DENSIFY_AHEAD + 10;
        let far = shell(h1, far_h, far_h);
        let far_hash = far.block_hash();
        st.record_height(far_hash, far_h);
        apply_peer_event(
            &mut st,
            &hub,
            PeerEvent::BlockFramed {
                peer: 1,
                hash: far_hash,
                payload: ser(&far),
            },
            &write_next,
            &mut book,
            local,
            None,
        );
        assert!(!st.body.is_pending(&far_hash));

        st.max_peer_height = 0;
        st.empty_header_streak = 0;
        st.ordered.clear();
        st.ordered_set.clear();
        st.inflight.clear();
        for _ in 0..2 {
            apply_peer_event(
                &mut st,
                &hub,
                PeerEvent::Headers {
                    peer: 1,
                    headers: vec![],
                },
                &write_next,
                &mut book,
                local,
                None,
            );
        }
        assert!(st.headers_done);

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

        let (body_tx, mut body_rx) = mpsc::unbounded_channel();
        let (ctrl_tx, mut ctrl_rx) = mpsc::unbounded_channel();
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
        let stats = super::super::status::LoopStats::default();
        drain_ready_peer_and_archive_events(
            &mut st,
            &hub,
            &mut body_rx,
            &mut ctrl_rx,
            &write_next,
            &stats,
            &mut book,
            local,
            None,
        )
        .unwrap();
        assert!(
            stats
                .drain_events
                .load(std::sync::atomic::Ordering::Relaxed)
                >= 1
        );
        let _ = std::fs::remove_dir_all(dir);
    }

    /// Single BlockFramed with raw payload offers BQ + notes ConfirmFeed.
    #[test]
    fn block_framed_raw_offers_body_queue_with_confirm_feed() {
        use super::super::confirm::ConfirmFeed;
        use super::super::peer_io::{PeerEvent, PeerSlot};
        use super::super::state::InflightReq;
        use super::apply_peer_event;
        use crate::seeds::AddrMan;
        use bitcoin::absolute::LockTime;
        use bitcoin::block::{Header, Version};
        use bitcoin::consensus::Encodable;
        use bitcoin::hashes::Hash;
        use bitcoin::script::ScriptBuf;
        use bitcoin::transaction::Version as TxVersion;
        use bitcoin::{
            Amount, Block, BlockHash, CompactTarget, OutPoint, Sequence, Transaction, TxIn, TxOut,
            Witness,
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
            "rbitcoin-ev-framed-bq-{}-{}",
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
        let write_next = AtomicU32::new(1);
        let mut book = AddrMan::new();
        let local = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)), 1);
        let feed = ConfirmFeed::new();

        let b1 = shell(gen, 1, 1);
        let h1 = b1.block_hash();
        st.record_height(h1, 1);
        st.header_fks
            .insert(h1, hub.ensure_header_fk(&b1.header).unwrap());
        st.slots[0].in_flight.insert(h1);
        st.inflight.insert(h1, InflightReq::new(1));

        let mut payload = Vec::new();
        b1.consensus_encode(&mut payload).unwrap();
        apply_peer_event(
            &mut st,
            &hub,
            PeerEvent::BlockFramed {
                peer: 1,
                hash: h1,
                payload,
            },
            &write_next,
            &mut book,
            local,
            Some(&feed),
        );
        assert!(st.body.is_pending(&h1));
        assert!(hub.query.block_queue_has_height(1));
        assert_eq!(feed.size_snap().0, 1);
        assert!(st.inflight.is_empty());

        let mut payload2 = Vec::new();
        b1.consensus_encode(&mut payload2).unwrap();
        apply_peer_event(
            &mut st,
            &hub,
            PeerEvent::BlockFramed {
                peer: 1,
                hash: h1,
                payload: payload2,
            },
            &write_next,
            &mut book,
            local,
            Some(&feed),
        );
        assert_eq!(hub.query.block_queue_stats().2, 1);
        assert_eq!(feed.size_snap().0, 1);

        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn known_headers_re_admit_to_ordered_after_tip_drain() {
        use super::super::peer_io::{PeerEvent, PeerSlot};
        use super::apply_peer_event;
        use crate::seeds::AddrMan;
        use bitcoin::block::{Header, Version};
        use bitcoin::hashes::Hash;
        use bitcoin::{BlockHash, CompactTarget};
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
        fn dummy_header(prev: BlockHash, n: u32) -> Header {
            Header {
                version: Version::ONE,
                prev_blockhash: prev,
                merkle_root: bitcoin::TxMerkleNode::from_byte_array([n as u8; 32]),
                time: 1_300_000_000 + n,
                bits: CompactTarget::from_consensus(0x207fffff),
                nonce: n,
            }
        }

        let dir = std::env::temp_dir().join(format!(
            "rbitcoin-ev-readmit-{}-{}",
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
        let write_next = AtomicU32::new(1);
        let mut book = AddrMan::new();
        let local = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)), 1);

        let hdr = dummy_header(gen, 1);
        let hash = hdr.block_hash();
        apply_peer_event(
            &mut st,
            &hub,
            PeerEvent::Headers {
                peer: 1,
                headers: vec![hdr.clone()],
            },
            &write_next,
            &mut book,
            local,
            None,
        );
        assert!(st.ordered_set.contains(&hash), "first admit");
        assert_eq!(st.hash_height.get(&hash), Some(&1));

        // Tip-drain shape: drop ordered membership; keep known + height (hygiene
        // now also retains those — simulate post-confirm empty path).
        st.ordered.clear();
        st.ordered_set.clear();
        st.height_to_hash.clear();
        assert!(st.known_headers.contains(&hash));
        assert!(st.ordered.is_empty());

        // Peer re-serves the same window (overlap). Must re-admit.
        apply_peer_event(
            &mut st,
            &hub,
            PeerEvent::Headers {
                peer: 1,
                headers: vec![hdr],
            },
            &write_next,
            &mut book,
            local,
            None,
        );
        assert!(
            st.ordered_set.contains(&hash),
            "known header must re-enter ordered after tip drain"
        );
        assert_eq!(st.ordered.len(), 1);

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
