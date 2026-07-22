//! Getdata assign: tip-hole race, confirm runway, ContigPark feed.
//!
//! Height ownership (single policy):
//! - **Tip holes:** ordered front unready → multi-peer race
//! - **Confirm runway:** tip+1‥min(near_hi, write_next−1) → single peer
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
    /// Critical + confirm runway + ContigPark densify band.
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
/// (0 = densify drip only / multi-peer race still on; 1 = full densify capacity).
pub(crate) fn assign_work_ordered(
    st: &mut IbdWorkState,
    hub: &ChainHub,
    cfg: &IbdConfig,
    loop_stats: &LoopStats,
    archive_feed_scale: f64,
    archive_write_next: u32,
    depth: AssignDepth,
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

    let expired = st.body.expire_stale_pending(PENDING_STALE);
    for h in expired {
        clear_hash_inflight(&mut st.slots, &mut st.inflight, h);
    }
    // ContigPark race band: re-request much sooner than global pending stale.
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

    let tip = hub.tip_height().unwrap_or(0);
    let near_hi = tip.saturating_add(NEAR_DEPTH);
    let tip_holes = contiguous_tip_holes(st, hub, TIP_HOLE_MAX);

    issued += cover_tip_holes(st, hub, cfg, &alive, &tip_holes);

    // Multi-peer race only on write_next .. write_next+R-1 (not a long band).
    issued += cover_park_race(st, hub, cfg, &alive, archive_write_next);

    if matches!(depth, AssignDepth::Critical) {
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
    let tip_hole = !tip_holes.is_empty();
    let base_feed = far_slots_per_peer(cfg.per_peer, tip_hole);
    let feed_cap = if feed_scale <= 0.0 {
        // Pressure: still drip densify so park can grow a contiguous run.
        2usize.min(cfg.per_peer).max(1)
    } else {
        scale_feed_cap(base_feed, feed_scale)
    };

    // Confirm runway: tip+1 .. min(near_hi, write_next-1). Park feed owns ≥ write_next.
    let runway_hi = if archive_write_next > tip.saturating_add(1) {
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
    let runway_cap = room.saturating_sub(feed_reserve);

    let runway = collect_runway(st, hub, tip, runway_hi, runway_cap);
    // Densify: write_next+R .. write_next+W (race prefix already multi-peered).
    let densify_lo = archive_write_next.saturating_add(CONTIG_PARK_RACE as u32);
    let densify_hi = archive_write_next.saturating_add(CONTIG_DENSIFY_AHEAD);
    let densify = collect_height_band(
        st,
        hub,
        densify_lo,
        densify_hi,
        room.saturating_sub(runway.len()).max(1),
    );

    if runway.is_empty() && densify.is_empty() {
        finish_assign(loop_stats, t0, issued);
        return;
    }

    let mut peer_i = st.assign_rot;
    st.assign_rot = st.assign_rot.wrapping_add(1);

    // Issue confirm runway (single peer), leaving feed_reserve for densify.
    let mut runway_q = runway;
    while room > feed_reserve && !runway_q.is_empty() {
        let mut any = false;
        for _ in 0..alive.len() {
            if room <= feed_reserve || runway_q.is_empty() {
                break;
            }
            let pid = alive[peer_i % alive.len()];
            peer_i += 1;
            if !peer_has_slot(st, pid, cfg.per_peer) {
                continue;
            }
            let Some(h) = pop_need(&mut runway_q, st, hub) else {
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

/// Confirm runway: tip+1 ‥ runway_hi (exclusive of ContigPark write_next and above).
fn collect_runway(
    st: &mut IbdWorkState,
    hub: &ChainHub,
    tip: u32,
    runway_hi: u32,
    cap: usize,
) -> VecDeque<BlockHash> {
    let mut out = VecDeque::new();
    if runway_hi <= tip || cap == 0 {
        return out;
    }
    for ht in tip.saturating_add(1)..=runway_hi {
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
    if st.body.is_known_archived(&h) || st.body.is_pending(&h) || st.body.is_rejected(&h) {
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
        if st.body.skip_download(hub, &h) || st.inflight.contains_key(&h) {
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
    // Count how many of this peer's inflight are already densify/race (not tip runway).
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
            || st.body.is_rejected(&h)
            || st.body.ready(hub, &h)
        {
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
        use std::sync::atomic::{AtomicU32, AtomicU64, Ordering as AtomicOrd};
        static LAST_LOG_WN: AtomicU32 = AtomicU32::new(u32::MAX);
        static LAST_LOG_MS: AtomicU64 = AtomicU64::new(0);
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        let prev_wn = LAST_LOG_WN.load(AtomicOrd::Relaxed);
        let prev_ms = LAST_LOG_MS.load(AtomicOrd::Relaxed);
        if write_next != prev_wn || now_ms.saturating_sub(prev_ms) >= 1000 {
            LAST_LOG_WN.store(write_next, AtomicOrd::Relaxed);
            LAST_LOG_MS.store(now_ms, AtomicOrd::Relaxed);
            rbitcoin_log::debug!(
                "ibd: park-race getdata write_next={write_next} issued={issued}"
            );
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
        if already == 0 && !placed_any {
            break;
        }
    }
    issued
}

/// Hashes in ContigPark race band that still need coverage (tests + diagnostics).
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
