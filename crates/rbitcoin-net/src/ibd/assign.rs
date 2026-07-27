//! Getdata assign: tip-hole race, parent cache, ContigPark feed.
//!
//! Height ownership (single policy):
//! - **Tip holes:** ordered front unready → multi-peer race
//! - **Parent cache:** tip+1‥min(near_hi, write_next−1) → single peer
//! - **ContigPark feed:** write_next‥write_next+W → multi-peer on write_next‥+R−1,
//!   single peer on the rest; capacity scaled by archive budget admission
//! - **Beyond write_next+W:** never request (events also refuse park)

use super::assign_plan::far_slots_per_peer;
use super::peer_io::{touch_block_progress, PeerCmd, PeerSlot};
use super::state::{self, IbdWorkState};
use super::status::LoopStats;
use super::{
    IbdConfig, CONTIG_DENSIFY_AHEAD, CONTIG_PARK_PENDING_STALE, CONTIG_PARK_RACE,
    FAR_BATCH_MAX, FAR_SCAN_BUDGET, NEAR_DEPTH, PENDING_STALE, TIP_HOLE_IMMEDIATE_PEERS,
    TIP_HOLE_MAX, TIP_HOLE_MAX_PEERS, TIP_HOLE_THIRD_PEER_AFTER,
};
use crate::chain::ChainHub;
use bitcoin::BlockHash;
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::Ordering;
use std::time::Instant;

/// How much assign work to do this call.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum AssignDepth {
    /// Tip holes + ContigPark `write_next` multi-peer race only (cheap).
    /// Used when archive pipeline is saturated so we do not spin full scans.
    Critical,
    /// Critical + parent cache + ContigPark densify band.
    Full,
}

/// Drop `hash` from global inflight and every peer's in_flight set.
pub(crate) fn clear_hash_inflight(
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
pub(crate) fn prune_satisfied_inflight(
    slots: &mut [PeerSlot],
    inflight: &mut HashMap<BlockHash, state::InflightReq>,
    hub: &ChainHub,
) {
    inflight.retain(|h, _| !hub.has_block(h));
    for s in slots.iter_mut() {
        s.in_flight.retain(|h| !hub.has_block(h));
    }
}

/// Record `peer` as requesting `hash` (tip-hole / park race may accumulate peers).
pub(crate) fn inflight_add_peer(
    inflight: &mut HashMap<BlockHash, state::InflightReq>,
    hash: BlockHash,
    peer: usize,
) {
    inflight
        .entry(hash)
        .or_insert_with(|| state::InflightReq::new(peer))
        .add_peer(peer);
}

/// Scale ContigPark densify per-peer slots by archive admission (`0.0`..=`1.0`).
pub(crate) fn scale_feed_cap(base: usize, scale: f64) -> usize {
    if base == 0 || scale <= 0.0 {
        return 0;
    }
    if scale >= 1.0 {
        return base;
    }
    let scaled = ((base as f64) * scale).ceil() as usize;
    scaled.max(1).min(base)
}

/// True when the archive pipeline is full of work and getdata inflight is low.
///
/// Full assign scans are wasteful here — bodies are pending/queued, not missing
/// on the wire. Critical assign (tip hole + write_next race) still runs.
pub(crate) fn archive_pipeline_saturated(
    pending_len: usize,
    inflight_len: usize,
    fill_ratio: f64,
) -> bool {
    inflight_len < 16 && (pending_len >= 96 || fill_ratio >= 0.85)
}

/// Assign getdata for bodies not yet Class A.
///
/// `archive_feed_scale` is [`super::archive::ArchiveQueueBudget::far_admission_scale`]
/// (0 = no densify; multi-peer tip/park race still on; 1 = full densify).
///
/// `can_assign_new`: [`super::archive::ArchiveQueueBudget::can_assign`] — when
/// false, only tip-hole + ContigPark race run (fill contig / tip). Densify and
/// confirm-cache getdata stop so charged RAM stays near the budget; bodies
/// already in flight still enqueue (may overshoot).
pub(crate) fn assign_work_ordered(
    st: &mut IbdWorkState,
    hub: &ChainHub,
    cfg: &IbdConfig,
    loop_stats: &LoopStats,
    archive_feed_scale: f64,
    archive_write_next: u32,
    depth: AssignDepth,
    can_assign_new: bool,
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

    // ContigPark race band: always re-request stuck holes so the writer can
    // advance even when the queue is at budget (only first copy is enqueued).
    let wn = archive_write_next;
    let race_hi = wn.saturating_add(CONTIG_PARK_RACE.saturating_sub(1) as u32);
    let gap_expired = st.body.expire_stale_pending_if(CONTIG_PARK_PENDING_STALE, |h| {
        st.hash_height
            .get(h)
            .is_some_and(|&ht| ht >= wn && ht <= race_hi)
    });
    for h in gap_expired {
        clear_hash_inflight(&mut st.slots, &mut st.inflight, h);
    }
    // Global pending-stale only when we have assign room for densify re-get —
    // otherwise thrash bandwidth while the pipeline is full.
    if can_assign_new {
        let expired = st.body.expire_stale_pending(PENDING_STALE);
        for h in expired {
            clear_hash_inflight(&mut st.slots, &mut st.inflight, h);
        }
    }

    let tip = hub.tip_height().unwrap_or(0);
    let near_hi = tip.saturating_add(NEAR_DEPTH);
    let tip_holes = contiguous_tip_holes(st, hub, TIP_HOLE_MAX);

    issued += cover_tip_holes(st, hub, cfg, &alive, &tip_holes);

    // Multi-peer race only on write_next .. write_next+R-1 (not a long band).
    issued += cover_park_race(st, hub, cfg, &alive, archive_write_next);

    // At budget or critical: no densify / confirm-cache assign.
    if !can_assign_new || matches!(depth, AssignDepth::Critical) {
        finish_assign(loop_stats, t0, issued);
        return;
    }

    let mut room = cfg.window.saturating_sub(st.inflight.len());
    if room == 0 {
        finish_assign(loop_stats, t0, issued);
        return;
    }
    if st.inflight.is_empty()
        && tip_holes.is_empty()
        && st.max_archived_height > 0
        && st.max_archived_height >= st.max_ordered_height
    {
        finish_assign(loop_stats, t0, issued);
        return;
    }

    let feed_scale = archive_feed_scale.clamp(0.0, 1.0);
    // Densify capacity in the ContigPark band (beyond the race prefix).
    // scale=0 → zero densify slots (no drip under pressure); cache may still run.
    let tip_hole = !tip_holes.is_empty();
    let base_feed = far_slots_per_peer(cfg.per_peer, tip_hole);
    let feed_cap = scale_feed_cap(base_feed, feed_scale);

    // Parent cache: tip+1 .. min(near_hi, write_next-1). Park feed owns ≥ write_next.
    let cache_hi = if archive_write_next > tip.saturating_add(1) {
        near_hi.min(archive_write_next.saturating_sub(1))
    } else {
        // write_next at/behind tip+1 → park feed covers the near window.
        tip
    };

    let feed_reserve = alive
        .len()
        .saturating_mul(feed_cap)
        .min(room.saturating_mul(3) / 4)
        .max(feed_cap.min(room));
    let cache_cap = room.saturating_sub(feed_reserve);

    let cache = collect_height_window(st, hub, tip, cache_hi, cache_cap);
    // Densify: write_next+R .. write_next+W (race prefix already multi-peered).
    let densify_lo = archive_write_next.saturating_add(CONTIG_PARK_RACE as u32);
    let densify_hi = archive_write_next.saturating_add(CONTIG_DENSIFY_AHEAD);
    let densify = collect_height_band(
        st,
        hub,
        densify_lo,
        densify_hi,
        room.saturating_sub(cache.len()).max(1),
    );

    if cache.is_empty() && densify.is_empty() {
        finish_assign(loop_stats, t0, issued);
        return;
    }

    let mut peer_i = st.assign_rot;
    st.assign_rot = st.assign_rot.wrapping_add(1);

    // Issue parent cache (single peer), leaving feed_reserve for densify.
    let mut cache_q = cache;
    while room > feed_reserve && !cache_q.is_empty() {
        let mut any = false;
        for _ in 0..alive.len() {
            if room <= feed_reserve || cache_q.is_empty() {
                break;
            }
            let pid = alive[peer_i % alive.len()];
            peer_i += 1;
            if !peer_has_slot(st, pid, cfg.per_peer) {
                continue;
            }
            let Some(h) = pop_need(&mut cache_q, st, hub) else {
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

    // ContigPark densify band (single peer per hash, feed_cap slots/peer).
    let mut densify_q = densify;
    while room > 0 && !densify_q.is_empty() {
        let mut any = false;
        for _ in 0..alive.len() {
            if room == 0 || densify_q.is_empty() {
                break;
            }
            let pid = alive[peer_i % alive.len()];
            peer_i += 1;
            let Some(n) = peer_feed_free(st, pid, cfg.per_peer, feed_cap) else {
                continue;
            };
            let take = n.min(room).min(FAR_BATCH_MAX);
            let mut batch = Vec::with_capacity(take);
            while batch.len() < take {
                let Some(h) = pop_need(&mut densify_q, st, hub) else {
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

pub(crate) fn finish_assign(loop_stats: &LoopStats, t0: Instant, issued: u64) {
    loop_stats
        .assign_ns
        .fetch_add(t0.elapsed().as_nanos() as u64, Ordering::Relaxed);
    if issued > 0 {
        loop_stats.assign_issued.fetch_add(issued, Ordering::Relaxed);
    }
}

/// Parent cache: tip+1 ‥ cache_hi (exclusive of ContigPark write_next and above).
fn collect_height_window(
    st: &mut IbdWorkState,
    hub: &ChainHub,
    tip: u32,
    cache_hi: u32,
    cap: usize,
) -> VecDeque<BlockHash> {
    let mut out = VecDeque::new();
    if cache_hi <= tip || cap == 0 {
        return out;
    }
    for ht in tip.saturating_add(1)..=cache_hi {
        if out.len() >= cap {
            break;
        }
        if let Some(h) = need_hash_at(st, hub, ht) {
            out.push_back(h);
        }
    }
    out
}

/// Single-peer need list over an inclusive height band.
fn collect_height_band(
    st: &mut IbdWorkState,
    hub: &ChainHub,
    lo: u32,
    hi: u32,
    cap: usize,
) -> VecDeque<BlockHash> {
    let mut out = VecDeque::new();
    if lo > hi || cap == 0 {
        return out;
    }
    let hi = hi.min(st.max_ordered_height.max(lo));
    let mut inspected = 0usize;
    for ht in lo..=hi {
        if out.len() >= cap || inspected >= FAR_SCAN_BUDGET {
            break;
        }
        inspected += 1;
        if let Some(h) = need_hash_at(st, hub, ht) {
            out.push_back(h);
        }
    }
    out
}

/// Hash at `ht` that still needs a new single-peer getdata (not inflight/pending/done).
fn need_hash_at(st: &mut IbdWorkState, hub: &ChainHub, ht: u32) -> Option<BlockHash> {
    let &h = st.height_to_hash.get(&ht)?;
    if !st.ordered_set.contains(&h) || st.inflight.contains_key(&h) {
        return None;
    }
    if st.body.is_known_archived(&h)
        || st.body.is_pending(&h)
        || st.body.is_archive_charged(&h)
        || st.body.is_rejected(&h)
    {
        return None;
    }
    if st.body.skip_download(hub, &h) {
        return None;
    }
    Some(h)
}

pub(crate) fn pop_need(
    q: &mut VecDeque<BlockHash>,
    st: &mut IbdWorkState,
    hub: &ChainHub,
) -> Option<BlockHash> {
    while let Some(h) = q.pop_front() {
        if st.body.skip_download(hub, &h)
            || st.body.is_archive_charged(&h)
            || st.inflight.contains_key(&h)
        {
            continue;
        }
        return Some(h);
    }
    None
}

fn peer_has_slot(st: &IbdWorkState, pid: usize, per_peer: usize) -> bool {
    st.slots
        .iter()
        .find(|s| s.id == pid && s.alive)
        .is_some_and(|s| s.in_flight.len() < per_peer)
}

/// Free densify slots on peer (cap how many ContigPark-feed hashes per peer).
fn peer_feed_free(
    st: &IbdWorkState,
    pid: usize,
    per_peer: usize,
    feed_cap: usize,
) -> Option<usize> {
    let s = st.slots.iter().find(|s| s.id == pid && s.alive)?;
    let free_total = per_peer.saturating_sub(s.in_flight.len());
    if free_total == 0 || feed_cap == 0 {
        return None;
    }
    // Count how many of this peer's inflight are already densify/race (not tip cache).
    // Approximate: any hash with height > tip+NEAR is feed; also height >= write_next
    // is hard without write_next here — use free_total.min(feed_cap) as simple bound.
    Some(free_total.min(feed_cap))
}

pub(crate) fn issue_one(
    st: &mut IbdWorkState,
    pid: usize,
    h: BlockHash,
    room: &mut usize,
    issued: &mut u64,
) -> bool {
    issue_batch(st, pid, vec![h], room, issued)
}

pub(crate) fn issue_batch(
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
    let new_unique = batch
        .iter()
        .filter(|h| st.inflight.get(*h).map(|e| e.len() == 1).unwrap_or(false))
        .count();
    *room = room.saturating_sub(new_unique);
    true
}

/// Contiguous unready hashes at the ordered front (status `hole=`).
pub(crate) fn contiguous_tip_holes(
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

/// Desired concurrent getdata peers for a tip-hole / park-race hash.
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

/// Multi-peer race for ContigPark `write_next`‥`write_next+R-1` only.
pub(crate) fn cover_park_race(
    st: &mut IbdWorkState,
    hub: &ChainHub,
    cfg: &IbdConfig,
    alive: &[usize],
    write_next: u32,
) -> u64 {
    if alive.is_empty() || CONTIG_PARK_RACE == 0 {
        return 0;
    }
    let mut issued = 0u64;
    let mut peer_i = st.assign_rot;
    st.assign_rot = st.assign_rot.wrapping_add(1);
    let now = Instant::now();
    let hi = write_next.saturating_add(CONTIG_PARK_RACE.saturating_sub(1) as u32);

    for ht in write_next..=hi {
        let Some(&h) = st.height_to_hash.get(&ht) else {
            continue;
        };
        if !st.ordered_set.contains(&h) {
            continue;
        }
        if hub.has_block(&h)
            || st.body.is_pending(&h)
            || st.body.is_archive_charged(&h)
            || st.body.is_rejected(&h)
            || st.body.ready(hub, &h)
        {
            // Already in archive pipeline (charged) or pending: never multi-queue.
            // Multi-peer race is only for hashes not yet enqueued.
            continue;
        }
        // Not known/pending: either need first peer or more race peers.
        if !st.inflight.contains_key(&h) && st.body.skip_download(hub, &h) {
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
        let mut need_peers = want - already;
        let mut placed_any = false;
        for _ in 0..alive.len() {
            if need_peers == 0 {
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
                need_peers = need_peers.saturating_sub(1);
            }
        }
        // write_next with zero coverage and no free peer — stop.
        if ht == write_next && already == 0 && !placed_any {
            break;
        }
    }
    if issued > 0 {
        // Empty-block phase advances write_next every few ms; per-call / per-wn
        // lines drown the log. Aggregate into a periodic summary instead.
        use std::sync::atomic::{AtomicU32, AtomicU64, Ordering as AtomicOrd};
        static TOTAL_ISSUED: AtomicU64 = AtomicU64::new(0);
        static TOTAL_WAVES: AtomicU32 = AtomicU32::new(0);
        static MIN_WN: AtomicU32 = AtomicU32::new(u32::MAX);
        static MAX_WN: AtomicU32 = AtomicU32::new(0);
        static LAST_LOG_MS: AtomicU64 = AtomicU64::new(0);
        const SUMMARY_MS: u64 = 5_000;

        TOTAL_ISSUED.fetch_add(issued, AtomicOrd::Relaxed);
        TOTAL_WAVES.fetch_add(1, AtomicOrd::Relaxed);
        MIN_WN.fetch_min(write_next, AtomicOrd::Relaxed);
        MAX_WN.fetch_max(write_next, AtomicOrd::Relaxed);

        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        let prev_ms = LAST_LOG_MS.load(AtomicOrd::Relaxed);
        if now_ms.saturating_sub(prev_ms) >= SUMMARY_MS
            && LAST_LOG_MS
                .compare_exchange(prev_ms, now_ms, AtomicOrd::Relaxed, AtomicOrd::Relaxed)
                .is_ok()
        {
            let total = TOTAL_ISSUED.swap(0, AtomicOrd::Relaxed);
            let waves = TOTAL_WAVES.swap(0, AtomicOrd::Relaxed);
            let min_wn = MIN_WN.swap(u32::MAX, AtomicOrd::Relaxed);
            let max_wn = MAX_WN.swap(0, AtomicOrd::Relaxed);
            if total > 0 {
                rbitcoin_log::debug!(
                    "ibd: park-race getdata summary issued={total} waves={waves} write_next={min_wn}..{max_wn}"
                );
            }
        }
    }
    issued
}

/// Cover each tip-hole hash with staged multi-peer getdata (2 now, 3 after 10s).
pub(crate) fn cover_tip_holes(
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
        if hub.has_block(&h)
            || st.body.is_pending(&h)
            || st.body.is_archive_charged(&h)
            || st.body.ready(hub, &h)
        {
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
        if already == 0 && !placed_any {
            break;
        }
    }
    issued
}

/// Hashes in ContigPark race band that still need coverage (tests).
#[cfg(test)]
pub(crate) fn park_race_need(
    st: &mut IbdWorkState,
    hub: &ChainHub,
    write_next: u32,
) -> Vec<BlockHash> {
    let mut out = Vec::new();
    let hi = write_next.saturating_add(CONTIG_PARK_RACE.saturating_sub(1) as u32);
    for ht in write_next..=hi {
        let Some(&h) = st.height_to_hash.get(&ht) else {
            continue;
        };
        if !st.ordered_set.contains(&h) {
            continue;
        }
        if st.body.is_known_archived(&h)
            || st.body.is_pending(&h)
            || st.body.is_archive_charged(&h)
            || st.body.is_rejected(&h)
        {
            continue;
        }
        if st.inflight.contains_key(&h) {
            out.push(h);
            continue;
        }
        if st.body.skip_download(hub, &h) {
            continue;
        }
        out.push(h);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::status::LoopStats;
    use bitcoin::hashes::Hash;
    use rbitcoin_consensus::{ChainParams, Milestone};
    use rbitcoin_query::Query;
    use std::collections::HashSet;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Arc;
    use tokio::sync::mpsc;

    fn h(n: u32) -> BlockHash {
        let mut b = [0u8; 32];
        b[0..4].copy_from_slice(&n.to_le_bytes());
        BlockHash::from_byte_array(b)
    }

    fn dummy_slot(id: usize) -> PeerSlot {
        let (cmd_tx, _rx) = mpsc::unbounded_channel();
        let task = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .spawn(async {});
        PeerSlot {
            id,
            addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 18444 + id as u16),
            cmd_tx,
            in_flight: HashSet::new(),
            block_progress_ms: Arc::new(AtomicU64::new(0)),
            peer_height: 100,
            connected_ms: 1,
            first_data_ms: AtomicU64::new(0),
            bytes_rx: AtomicU64::new(0),
            alive: true,
            task,
        }
    }

    fn tmp_hub() -> (std::path::PathBuf, ChainHub) {
        let dir = std::env::temp_dir().join(format!(
            "rbitcoin-assign-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let _ = std::fs::create_dir_all(&dir);
        let q = Query::open_or_create(dir.join("store")).unwrap();
        (
            dir,
            ChainHub::new(q, ChainParams::regtest(), Milestone::NONE),
        )
    }

    #[test]
    fn clear_inflight_add_peer_pop_need_and_tip_holes() {
        let (dir, hub) = tmp_hub();
        let mut st = IbdWorkState::new(vec![dummy_slot(0), dummy_slot(1)], None, Some(0));
        let hash = h(10);
        st.slots[0].in_flight.insert(hash);
        st.slots[1].in_flight.insert(hash);
        inflight_add_peer(&mut st.inflight, hash, 0);
        inflight_add_peer(&mut st.inflight, hash, 1);
        assert_eq!(st.inflight[&hash].len(), 2);
        clear_hash_inflight(&mut st.slots, &mut st.inflight, hash);
        assert!(st.inflight.is_empty());
        assert!(st.slots[0].in_flight.is_empty());
        assert!(st.slots[1].in_flight.is_empty());

        // pop_need skips pending / takes first ready missing.
        let mut q = VecDeque::from([h(1), h(2)]);
        st.body.mark_pending(h(1));
        st.body.mark_missing(h(2));
        assert_eq!(pop_need(&mut q, &mut st, &hub), Some(h(2)));
        assert!(pop_need(&mut q, &mut st, &hub).is_none());

        // Tip holes: ghost skip, then unready, then ready breaks.
        st.ordered.clear();
        st.ordered_set.clear();
        let ghost = h(20);
        let hole = h(21);
        let ready = h(22);
        st.ordered.extend([ghost, hole, ready]);
        st.ordered_set.insert(hole);
        st.ordered_set.insert(ready);
        st.body.mark_missing(hole);
        st.body.mark_archived(ready);
        let holes = contiguous_tip_holes(&mut st, &hub, 8);
        assert_eq!(holes, vec![hole]);

        // issue_one with empty batch / dead peer.
        let mut room = 10usize;
        let mut issued = 0u64;
        assert!(!issue_one(&mut st, 99, h(30), &mut room, &mut issued));
        assert!(!issue_batch(&mut st, 0, vec![], &mut room, &mut issued));
        st.body.mark_missing(h(30));
        assert!(issue_one(&mut st, 0, h(30), &mut room, &mut issued));
        assert!(issued >= 1);
        assert!(st.inflight.contains_key(&h(30)));
        assert!(st.slots[0].in_flight.contains(&h(30)));

        // assign with no alive peers is a no-op.
        st.slots.iter_mut().for_each(|s| s.alive = false);
        let stats = LoopStats::default();
        let cfg = IbdConfig::for_test();
        assign_work_ordered(
            &mut st,
            &hub,
            &cfg,
            &stats,
            1.0,
            1,
            AssignDepth::Full,
            true,
        );

        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn scale_and_saturated_helpers() {
        assert_eq!(scale_feed_cap(0, 1.0), 0);
        assert_eq!(scale_feed_cap(10, 0.0), 0);
        assert_eq!(scale_feed_cap(10, 1.0), 10);
        assert_eq!(scale_feed_cap(10, 0.25), 3); // ceil
        assert!(!archive_pipeline_saturated(0, 20, 1.0));
        assert!(archive_pipeline_saturated(96, 0, 0.0));
    }

    /// Critical depth / can_assign_new=false / room=0 early exits, densify + cache
    /// issue loop, pending expire under can_assign, and park_race_need filter.
    #[test]
    fn assign_depth_densify_cache_and_early_exits() {
        let (dir, hub) = tmp_hub();
        hub.ensure_genesis().unwrap();
        let mut st = IbdWorkState::new(vec![dummy_slot(0), dummy_slot(1)], None, Some(0));
        let stats = LoopStats::default();
        let mut cfg = IbdConfig::for_test();
        cfg.window = 64;
        cfg.per_peer = 8;

        // Map ordered heights 1..12 as missing bodies (tip=0).
        for ht in 1u32..=12 {
            let hash = h(ht);
            st.record_height(hash, ht);
            st.height_to_hash.insert(ht, hash);
            st.ordered_set.insert(hash);
            st.ordered.push_back(hash);
            st.max_ordered_height = ht;
            st.body.mark_missing(hash);
        }

        // Critical depth: only tip-hole + park race; densify skipped.
        assign_work_ordered(
            &mut st,
            &hub,
            &cfg,
            &stats,
            1.0,
            1,
            AssignDepth::Critical,
            true,
        );
        let after_crit = st.inflight.len();
        assert!(after_crit > 0, "critical should still issue tip/race");

        // can_assign_new=false early exit after race (no densify growth path).
        let n_before = st.inflight.len();
        assign_work_ordered(
            &mut st,
            &hub,
            &cfg,
            &stats,
            1.0,
            1,
            AssignDepth::Full,
            false,
        );
        // May re-issue expired race only; must not explode densify.
        assert!(st.inflight.len() <= n_before + 8);

        // Clear inflight + pending so Full densify can issue.
        let hashes: Vec<_> = st.inflight.keys().copied().collect();
        for hash in hashes {
            clear_hash_inflight(&mut st.slots, &mut st.inflight, hash);
            st.body.mark_missing(hash);
        }
        // Stale pending expire when can_assign_new (age zero).
        st.body.mark_pending(h(5));
        // Force age by expiring with Duration::ZERO via body API, then re-mark for assign path.
        let _ = st.body.expire_stale_pending(std::time::Duration::ZERO);
        st.body.mark_pending(h(5));
        // Direct expire path already covered in body tests; assign uses PENDING_STALE (45s).
        // Put a gap-expired race height into pending with age 0 via mark + expire_if.
        st.body.mark_pending(h(1));
        let expired = st
            .body
            .expire_stale_pending_if(std::time::Duration::ZERO, |_| true);
        for hash in expired {
            clear_hash_inflight(&mut st.slots, &mut st.inflight, hash);
            st.body.mark_missing(hash);
        }

        // Full assign with densify scale: write_next=1, densify starts after race.
        assign_work_ordered(
            &mut st,
            &hub,
            &cfg,
            &stats,
            1.0,
            1,
            AssignDepth::Full,
            true,
        );
        assert!(
            st.inflight.len() > 0,
            "full densify/cache should issue getdata"
        );
        assert!(stats.assign_issued.load(Ordering::Relaxed) > 0);

        // room=0 path: fill window with dummy inflight.
        for ht in 20u32..20 + cfg.window as u32 {
            let hash = h(ht + 100);
            inflight_add_peer(&mut st.inflight, hash, 0);
            st.slots[0].in_flight.insert(hash);
        }
        let n_full = st.inflight.len();
        assign_work_ordered(
            &mut st,
            &hub,
            &cfg,
            &stats,
            1.0,
            5,
            AssignDepth::Full,
            true,
        );
        // room=0 → finish without growing densify much (may clear satisfied only).
        assert!(st.inflight.len() <= n_full + 2);

        // peer_has_slot / park_race_need: fill peer0, peer1 free.
        st.inflight.clear();
        st.slots[0].in_flight.clear();
        st.slots[1].in_flight.clear();
        for ht in 1u32..=4 {
            let hash = h(ht);
            st.body.mark_missing(hash);
        }
        // Saturate peer 0.
        for i in 0..cfg.per_peer {
            let hash = h(200 + i as u32);
            st.slots[0].in_flight.insert(hash);
            inflight_add_peer(&mut st.inflight, hash, 0);
        }
        let need = park_race_need(&mut st, &hub, 1);
        assert!(!need.is_empty());
        // Full assign should still place on peer 1 when peer 0 is full.
        assign_work_ordered(
            &mut st,
            &hub,
            &cfg,
            &stats,
            0.5, // partial densify scale
            1,
            AssignDepth::Full,
            true,
        );
        assert!(st.slots[1].in_flight.len() > 0 || st.inflight.len() > cfg.per_peer);

        // Empty densify/cache (all archived) early finish.
        for ht in 1u32..=12 {
            st.body.mark_archived(h(ht));
        }
        st.inflight.clear();
        st.slots.iter_mut().for_each(|s| s.in_flight.clear());
        st.max_archived_height = 12;
        st.max_ordered_height = 12;
        assign_work_ordered(
            &mut st,
            &hub,
            &cfg,
            &stats,
            1.0,
            13,
            AssignDepth::Full,
            true,
        );
        assert!(st.inflight.is_empty());

        let _ = std::fs::remove_dir_all(dir);
    }
}
