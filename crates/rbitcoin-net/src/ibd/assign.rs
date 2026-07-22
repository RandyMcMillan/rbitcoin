//! Getdata assign: tip-hole race, near band, far densify.

use super::assign_plan::{classify_height, far_slots_per_peer, WorkClass};
use super::peer_io::{touch_block_progress, PeerCmd, PeerSlot};
use super::state::{self, IbdWorkState};
use super::status::LoopStats;
use super::{
    IbdConfig, CONTIG_GAP_FILL_MAX, FAR_BATCH_MAX, FAR_SCAN_BUDGET, NEAR_DEPTH, PENDING_STALE,
    TIP_HOLE_IMMEDIATE_PEERS, TIP_HOLE_MAX, TIP_HOLE_MAX_PEERS, TIP_HOLE_THIRD_PEER_AFTER,
};
use crate::chain::ChainHub;
use bitcoin::BlockHash;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::Ordering;
use std::time::Instant;

/// Drop `hash` from global inflight and every peer's in_flight set.
///
/// Used when the first body arrives (or archive/confirm settles the hash) so
/// racing peers stop counting it as outstanding work. Late `Block` messages are
/// ignored via [`BodyPresence::skip_download`].
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
/// Archived-but-unconfirmed ghosts are cleared on Block / archive-ok via
/// [`clear_hash_inflight`] — avoid `is_archived` store probes every assign.
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

/// Record `peer` as requesting `hash` (tip-hole may accumulate multiple peers).
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

/// Scale far densify capacity by archive budget admission (`0.0`..=`1.0`).
///
/// `0.0` = tip hole + near + ContigPark gap only (no general far densify).
/// `1.0` = full far slots / window reserve.
pub(crate) fn scale_far_cap(base_far_cap: usize, far_scale: f64) -> usize {
    if base_far_cap == 0 || far_scale <= 0.0 {
        return 0;
    }
    if far_scale >= 1.0 {
        return base_far_cap;
    }
    // Ceil so tiny residual headroom still allows a drip of far work.
    let scaled = ((base_far_cap as f64) * far_scale).ceil() as usize;
    scaled.max(1).min(base_far_cap)
}

/// Assign getdata for bodies not yet Class A.
///
/// 1. Tip hole — 2 peers immediately; 3rd after [`TIP_HOLE_THIRD_PEER_AFTER`]
///    from when the second was attached (no stall disconnect required).
/// 2. **Archive ContigPark gap** — heights at/after `archive_write_next` that
///    still need a body (**always**, even when `far_scale == 0`).
/// 3. Near band — tip+1‥tip+[`NEAR_DEPTH`] (single peer per hash).
/// 4. Far — forward densify past near (height-ascending); capacity scaled by
///    `far_scale` from [`super::archive::ArchiveQueueBudget::far_admission_scale`].
///
/// `archive_write_next` is ContigPark's next commit height (shared atomic from
/// the writer). Filling gaps there unblocks parked RAM under a full budget.
pub(crate) fn assign_work_ordered(
    st: &mut IbdWorkState,
    hub: &ChainHub,
    cfg: &IbdConfig,
    loop_stats: &LoopStats,
    far_scale: f64,
    archive_write_next: u32,
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

    let tip = hub.tip_height().unwrap_or(0);
    let near_hi = tip.saturating_add(NEAR_DEPTH);
    let tip_holes = contiguous_tip_holes(st, hub, TIP_HOLE_MAX);
    let tip_hole = !tip_holes.is_empty();

    issued += cover_tip_holes(st, hub, cfg, &alive, &tip_holes);

    // Unstick ContigPark even when far_scale == 0 (budget pressure / hysteresis).
    issued += cover_archive_contig_gap(st, hub, cfg, &alive, archive_write_next);

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

    // Far densify scaled by archive free headroom (proportional + hysteresis).
    // Contig gap already handled above; near always uses remaining room.
    let far_scale = far_scale.clamp(0.0, 1.0);
    let base_far = far_slots_per_peer(cfg.per_peer, tip_hole);
    let far_cap = scale_far_cap(base_far, far_scale);
    let want_far = far_cap > 0;
    // far_cap is already scale-adjusted; reserve uses the reduced per-peer far slots.
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
        collect_need(st, hub, tip, near_hi, near_window_cap, room, want_far);
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

pub(crate) fn finish_assign(loop_stats: &LoopStats, t0: Instant, issued: u64) {
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
pub(crate) fn collect_need(
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

pub(crate) fn peer_can_take_near(
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

pub(crate) fn peer_far_free(
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

pub(crate) fn count_class(
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

/// Collect hashes for ContigPark `write_next`‥`write_next+max-1` that still need
/// getdata (height-ascending). Pure helper for tests and assign.
pub(crate) fn contig_gap_need(
    st: &mut IbdWorkState,
    hub: &ChainHub,
    write_next: u32,
    max: u32,
) -> Vec<BlockHash> {
    if max == 0 {
        return Vec::new();
    }
    let mut out = Vec::with_capacity(max.min(64) as usize);
    let hi = write_next.saturating_add(max.saturating_sub(1));
    for ht in write_next..=hi {
        let Some(&h) = st.height_to_hash.get(&ht) else {
            continue;
        };
        if !st.ordered_set.contains(&h) {
            continue;
        }
        if st.inflight.contains_key(&h)
            || st.body.is_known_archived(&h)
            || st.body.is_pending(&h)
            || st.body.is_rejected(&h)
        {
            continue;
        }
        if st.body.skip_download(hub, &h) {
            continue;
        }
        out.push(h);
    }
    out
}

/// Cover ContigPark's next commit heights with getdata (single peer each).
///
/// Runs even when `far_scale == 0` so a missing far gap cannot freeze the writer
/// while the budget is full of parked later heights.
pub(crate) fn cover_archive_contig_gap(
    st: &mut IbdWorkState,
    hub: &ChainHub,
    cfg: &IbdConfig,
    alive: &[usize],
    write_next: u32,
) -> u64 {
    if alive.is_empty() {
        return 0;
    }
    let need = contig_gap_need(st, hub, write_next, CONTIG_GAP_FILL_MAX);
    if need.is_empty() {
        return 0;
    }
    let mut issued = 0u64;
    let mut room = cfg.window.saturating_sub(st.inflight.len());
    if room == 0 {
        // Still try: reserve a few slots over window for the critical gap
        // (window is soft enough; one missing body unblocks MiB of park).
        room = CONTIG_GAP_FILL_MAX.min(8) as usize;
    }
    let mut peer_i = st.assign_rot;
    st.assign_rot = st.assign_rot.wrapping_add(1);
    for h in need {
        if room == 0 {
            break;
        }
        // Prefer a peer with free per_peer capacity.
        let mut placed = false;
        for _ in 0..alive.len() {
            let pid = alive[peer_i % alive.len()];
            peer_i += 1;
            let Some(idx) = st.slots.iter().position(|s| s.id == pid && s.alive) else {
                continue;
            };
            if st.slots[idx].in_flight.len() >= cfg.per_peer {
                continue;
            }
            if st.slots[idx].in_flight.contains(&h) {
                continue;
            }
            if issue_one(st, pid, h, &mut room, &mut issued) {
                placed = true;
                break;
            }
        }
        if !placed {
            // No free peer slot — stop; next tick will retry.
            break;
        }
    }
    if issued > 0 {
        rbitcoin_log::debug!(
            "ibd: contig-gap getdata write_next={write_next} issued={issued} (unstick ContigPark)"
        );
    }
    issued
}

/// Cover each tip-hole hash with staged multi-peer getdata (2 now, 3 after 10s).
/// First delivery clears all racers via [`clear_hash_inflight`].
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
        // No free peer slots for a hole with zero coverage — later holes wait.
        if already == 0 && !placed_any {
            break;
        }
    }
    issued
}

