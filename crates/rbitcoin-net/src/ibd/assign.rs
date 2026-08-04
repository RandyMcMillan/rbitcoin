//! Getdata assign for the **unified body-queue → prep → scripts → write** path.
//!
//! Policy (operator-facing):
//! - **Tip batch** (tip+1 .. tip+[`TIP_HOLE_MAX`]=32, one confirm run): always
//!   request missing hashes (even if soft body-queue depth is over free floor).
//!   Multi-peer race up to [`TIP_HOLE_MAX_PEERS`] immediately — confirm is
//!   frozen until tip+1 is claim-ready.
//! - **Densify** (tip+1 outward, closest first): fill missing heights up to
//!   [`CONTIG_DENSIFY_AHEAD`]. Two soft assign limits (no hysteresis):
//!   - BQ payload **≤ ~100 MiB** → usual densify ahead to the height horizon
//!   - BQ payload **> ~100 MiB** → only heights confirm will consume in the
//!     next **~1 min** at current tip rate ([`rbitcoin_query::soft_densify_band_hi`])
//! - Never request beyond densify horizon; events refuse far bodies too.
//! - One body-queue copy per height (receive path drops duplicates).

use super::assign_plan::far_slots_per_peer;
use super::peer_io::{touch_block_progress, PeerCmd, PeerSlot};
use super::state::{self, IbdWorkState};
use super::status::LoopStats;
use super::{
    IbdConfig, CONTIG_DENSIFY_AHEAD, FAR_SCAN_BUDGET, PENDING_STALE,
    TIP_HOLE_IMMEDIATE_PEERS, TIP_HOLE_MAX, TIP_HOLE_MAX_PEERS, TIP_HOLE_THIRD_PEER_AFTER,
};
use crate::chain::ChainHub;
use bitcoin::BlockHash;
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::Ordering;
use std::time::Instant;

/// How much assign work to do this call.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum AssignDepth {
    /// Tip-batch multi-peer only (archive soft saturated / no densify room).
    Critical,
    /// Tip batch + densify (gap always; frontier when soft depth allows).
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


/// True when soft BQ confirm window is already covered (or archive RAM full)
/// and getdata inflight is low → Critical (tip race only, skip densify walk).
///
/// High `pending` alone is **not** saturated — pending means we already hold wire.
pub(crate) fn archive_pipeline_saturated(
    _pending_len: usize,
    inflight_len: usize,
    bq_confirm_window_covered: bool,
    archive_fill_ratio: f64,
) -> bool {
    inflight_len < 16 && (bq_confirm_window_covered || archive_fill_ratio >= 0.85)
}

/// Assign getdata for the body-queue pipeline.
///
/// `archive_can_assign`: soft archive RAM headroom for densify.
/// `tip_rate_blocks_per_s`: tip confirm rate for the soft confirm-time window
/// when BQ payload is over [`rbitcoin_query::BQ_SOFT_FREE_BYTES`].
pub(crate) fn assign_work_ordered(
    st: &mut IbdWorkState,
    hub: &ChainHub,
    cfg: &IbdConfig,
    loop_stats: &LoopStats,
    _archive_feed_scale: f64,
    _archive_write_next: u32,
    depth: AssignDepth,
    archive_can_assign: bool,
    tip_rate_blocks_per_s: Option<f64>,
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

    let tip = hub.tip_height().unwrap_or(0);
    let path_lo = if hub.tip_height().is_none() {
        0u32
    } else {
        tip.saturating_add(1)
    };
    let tip_batch_hi = path_lo.saturating_add(TIP_HOLE_MAX.saturating_sub(1) as u32);

    // Stale pending in tip batch only → re-get (don't thrash far pending).
    let tip_expired = st.body.expire_stale_pending_if(PENDING_STALE, |h| {
        st.hash_height
            .get(h)
            .is_some_and(|&ht| ht >= path_lo && ht <= tip_batch_hi)
    });
    for h in tip_expired {
        clear_hash_inflight(&mut st.slots, &mut st.inflight, h);
    }

    // 1) Always cover tip confirm batch (≤32) with multi-peer race.
    let tip_holes = contiguous_tip_holes(st, hub, TIP_HOLE_MAX);
    issued += cover_tip_holes(st, hub, cfg, &alive, &tip_holes);

    // 2) Densify only when archive soft has room and not Critical.
    if !archive_can_assign || matches!(depth, AssignDepth::Critical) {
        finish_assign(loop_stats, t0, issued);
        return;
    }

    let mut room = cfg.window.saturating_sub(st.inflight.len());
    if room == 0 {
        finish_assign(loop_stats, t0, issued);
        return;
    }

    // Leave per-peer headroom for tip races while hole>0.
    let densify_per_peer = far_slots_per_peer(cfg.per_peer, !tip_holes.is_empty());

    let densify_hi = path_lo.saturating_add(CONTIG_DENSIFY_AHEAD);
    let depth_bytes = hub.query.block_queue_stats().1;
    // Under free-byte floor: full densify_hi. Over it: only confirm-time window.
    let band_hi = rbitcoin_query::soft_densify_band_hi(
        path_lo,
        densify_hi,
        depth_bytes,
        tip_rate_blocks_per_s,
    );

    let densify = collect_height_band(st, hub, path_lo, band_hi, room.max(1));
    if densify.is_empty() {
        finish_assign(loop_stats, t0, issued);
        return;
    }

    let mut peer_i = st.assign_rot;
    st.assign_rot = st.assign_rot.wrapping_add(1);
    let mut densify_q = densify;
    while room > 0 && !densify_q.is_empty() {
        let mut any = false;
        for _ in 0..alive.len() {
            if room == 0 || densify_q.is_empty() {
                break;
            }
            let pid = alive[peer_i % alive.len()];
            peer_i += 1;
            if !peer_has_slot(st, pid, densify_per_peer) {
                continue;
            }
            let Some(h) = pop_need(&mut densify_q, st, hub) else {
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

/// Single-peer need list over an inclusive height band.
///
/// Walks closest-to-tip first. Already-pending / body-queue / archived heights
/// are skipped without consuming [`FAR_SCAN_BUDGET`] “need” slots — only the
/// raw walk length is capped — so a full tip buffer no longer blocks densify
/// from seeing the rest of the [`CONTIG_DENSIFY_AHEAD`] band.
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
    let mut walked = 0usize;
    for ht in lo..=hi {
        if out.len() >= cap || walked >= FAR_SCAN_BUDGET {
            break;
        }
        walked += 1;
        if let Some(h) = need_hash_at(st, hub, ht) {
            out.push_back(h);
        }
    }
    out
}

/// Hash at `ht` that still needs a new single-peer getdata (not inflight/pending/done).
fn need_hash_at(st: &mut IbdWorkState, hub: &ChainHub, ht: u32) -> Option<BlockHash> {
    let &h = st.height_to_hash.get(&ht)?;
    if st.inflight.contains_key(&h) {
        return None;
    }
    if st.body.is_known_archived(&h)
        || st.body.is_pending(&h)
        || st.body.is_rejected(&h)
    {
        return None;
    }
    // Body queue already holds wire for this height.
    if hub.query.block_queue_has_height(ht) {
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

/// Contiguous tip+1.. hashes that still need getdata (assign tip-hole race).
///
/// Stops at the first **claim-ready** body (pending / body queue / Class A) so
/// densify priority matches operator `hole=` (fetch gap, not confirm backlog).
pub(crate) fn contiguous_tip_holes(
    st: &mut IbdWorkState,
    hub: &ChainHub,
    max: usize,
) -> Vec<BlockHash> {
    use super::progress::claim_ready;
    let path_lo = match hub.tip_height() {
        None => 0u32,
        Some(t) => t.saturating_add(1),
    };
    let mut holes = Vec::new();
    let limit = path_lo.saturating_add(max as u32 * 4).max(path_lo.saturating_add(max as u32));
    for ht in path_lo..=limit {
        if holes.len() >= max {
            break;
        }
        let Some(&hash) = st.height_to_hash.get(&ht) else {
            break;
        };
        if st.body.is_rejected(&hash) {
            break;
        }
        if claim_ready(hub, &mut st.body, ht, &hash) {
            break;
        }
        // Need getdata (and not already inflight — cover_tip_holes filters that).
        holes.push(hash);
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

        let mut q = VecDeque::from([h(1), h(2)]);
        st.body.mark_pending(h(1));
        st.body.mark_missing(h(2));
        assert_eq!(pop_need(&mut q, &mut st, &hub), Some(h(2)));
        assert!(pop_need(&mut q, &mut st, &hub).is_none());

        st.height_to_hash.clear();
        let hole = h(21);
        let ready = h(22);
        st.height_to_hash.insert(0, hole);
        st.height_to_hash.insert(1, ready);
        st.body.mark_missing(hole);
        st.body.mark_pending(ready);
        let holes = contiguous_tip_holes(&mut st, &hub, 8);
        assert_eq!(holes, vec![hole]);

        let mut room = 10usize;
        let mut issued = 0u64;
        assert!(!issue_one(&mut st, 99, h(30), &mut room, &mut issued));
        assert!(!issue_batch(&mut st, 0, vec![], &mut room, &mut issued));
        st.body.mark_missing(h(30));
        assert!(issue_one(&mut st, 0, h(30), &mut room, &mut issued));
        assert!(issued >= 1);
        assert!(st.inflight.contains_key(&h(30)));
        assert!(st.slots[0].in_flight.contains(&h(30)));

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
            None,
        );

        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn scale_and_saturated_helpers() {
        assert!(!archive_pipeline_saturated(0, 20, false, 1.0));
        assert!(!archive_pipeline_saturated(96, 0, false, 0.0));
        assert!(archive_pipeline_saturated(0, 0, false, 0.85));
        assert!(archive_pipeline_saturated(200, 15, true, 0.0));
        assert!(!archive_pipeline_saturated(0, 32, true, 0.9));
    }

    #[test]
    fn densify_yields_peer_slots_while_tip_hole_open() {
        use super::super::assign_plan::far_slots_per_peer;
        assert_eq!(far_slots_per_peer(16, true), 2);
        assert!(far_slots_per_peer(16, true) < 16);
        assert_eq!(far_slots_per_peer(16, false), 8);
    }

    #[test]
    fn densify_requests_beyond_legacy_2k_when_soft_allows() {
        let (dir, hub) = tmp_hub();
        hub.ensure_genesis().unwrap();
        let mut st = IbdWorkState::new(vec![dummy_slot(0), dummy_slot(1)], None, Some(0));
        let stats = LoopStats::default();
        let mut cfg = IbdConfig::for_test();
        cfg.window = 128;
        cfg.per_peer = 16;

        const HI: u32 = 4096;
        for ht in 1u32..=HI {
            let hash = h(ht);
            st.record_height(hash, ht);
            st.height_to_hash.insert(ht, hash);
            st.ordered_set.insert(hash);
            st.ordered.push_back(hash);
            st.max_ordered_height = ht;
            if ht <= 2500 {
                st.body.mark_pending(hash);
            } else {
                st.body.mark_missing(hash);
            }
        }

        assign_work_ordered(
            &mut st,
            &hub,
            &cfg,
            &stats,
            1.0,
            1,
            AssignDepth::Full,
            true,
            None, // under free (empty BQ) — full densify ahead
        );

        let far: Vec<u32> = st
            .inflight
            .keys()
            .filter_map(|hash| st.hash_height.get(hash).copied())
            .filter(|&ht| ht > 2500)
            .collect();
        assert!(
            !far.is_empty(),
            "expected densify past filled 2.5k prefix; inflight heights={:?}",
            st.inflight
                .keys()
                .filter_map(|hash| st.hash_height.get(hash).copied())
                .collect::<Vec<_>>()
        );
        assert!(
            far.iter().any(|&ht| ht > 2048),
            "legacy CONTIG_DENSIFY_AHEAD=2048 must not be the ceiling; far={far:?}"
        );

        let _ = std::fs::remove_dir_all(dir);
    }

    /// Over free-byte floor: densify only the confirm-time window (rate * 60s).
    #[test]
    fn densify_over_free_bytes_limited_to_confirm_window() {
        use rbitcoin_query::{soft_confirm_window_n, BQ_SOFT_FREE_BYTES};

        let (dir, hub) = tmp_hub();
        hub.ensure_genesis().unwrap();
        // Genesis tip=0 → path_lo=1.
        let mut st = IbdWorkState::new(vec![dummy_slot(0), dummy_slot(1)], None, Some(0));
        let stats = LoopStats::default();
        let mut cfg = IbdConfig::for_test();
        cfg.window = 64;
        cfg.per_peer = 16;

        // Heights 1..=200 missing; fill BQ over free floor with fat payloads.
        for ht in 1u32..=200 {
            let hash = h(ht);
            st.record_height(hash, ht);
            st.height_to_hash.insert(ht, hash);
            st.ordered_set.insert(hash);
            st.ordered.push_back(hash);
            st.max_ordered_height = ht;
            st.body.mark_missing(hash);
        }
        // ~110 MiB in queue (two ~55 MiB chunks) → restricted.
        let chunk = vec![0u8; 55 * 1024 * 1024];
        hub.query
            .block_queue_enqueue(1, h(1).to_byte_array(), 1, &chunk)
            .unwrap();
        hub.query
            .block_queue_enqueue(2, h(2).to_byte_array(), 2, &chunk)
            .unwrap();
        st.body.mark_pending(h(1));
        st.body.mark_pending(h(2));
        assert!(hub.query.block_queue_stats().1 > BQ_SOFT_FREE_BYTES);

        // 0.1 blk/s × 60s → window of 6 heights (path_lo=1 → band_hi=6).
        let rate = Some(0.1);
        let win = soft_confirm_window_n(rate);
        assert_eq!(win, 6);
        let path_lo = 1u32;
        let band_hi = path_lo + win - 1;

        assign_work_ordered(
            &mut st,
            &hub,
            &cfg,
            &stats,
            1.0,
            1,
            AssignDepth::Full,
            true,
            rate,
        );

        let issued_hts: Vec<u32> = st
            .inflight
            .keys()
            .filter_map(|hash| st.hash_height.get(hash).copied())
            .collect();
        assert!(
            !issued_hts.is_empty(),
            "expected densify inside confirm window; issued={issued_hts:?}"
        );
        assert!(
            issued_hts.iter().all(|&ht| ht <= band_hi),
            "no densify past confirm window {band_hi}; issued={issued_hts:?}"
        );

        let _ = std::fs::remove_dir_all(dir);
    }

    /// Under free-byte floor: full densify ahead even with many queued blocks.
    #[test]
    fn densify_under_free_bytes_uses_full_ahead() {
        use rbitcoin_query::BQ_SOFT_FREE_BYTES;

        let (dir, hub) = tmp_hub();
        hub.ensure_genesis().unwrap();
        // Genesis tip=0 → path_lo=1.
        let mut st = IbdWorkState::new(vec![dummy_slot(0), dummy_slot(1)], None, Some(0));
        let stats = LoopStats::default();
        let mut cfg = IbdConfig::for_test();
        cfg.window = 64;
        cfg.per_peer = 16;

        for ht in 1u32..=100 {
            let hash = h(ht);
            st.record_height(hash, ht);
            st.height_to_hash.insert(ht, hash);
            st.ordered_set.insert(hash);
            st.ordered.push_back(hash);
            st.max_ordered_height = ht;
            st.body.mark_missing(hash);
        }
        // Tiny payloads well under free floor.
        for ht in 1u32..=10 {
            hub.query
                .block_queue_enqueue(ht, h(ht).to_byte_array(), ht as u64, b"x")
                .unwrap();
            st.body.mark_pending(h(ht));
        }
        assert!(hub.query.block_queue_stats().1 < BQ_SOFT_FREE_BYTES);

        assign_work_ordered(
            &mut st,
            &hub,
            &cfg,
            &stats,
            1.0,
            1,
            AssignDepth::Full,
            true,
            Some(0.1), // rate would only allow 6 if restricted — must still densify past that
        );

        let issued_hts: Vec<u32> = st
            .inflight
            .keys()
            .filter_map(|hash| st.hash_height.get(hash).copied())
            .collect();
        assert!(
            issued_hts.iter().any(|&ht| ht > 16),
            "under free bytes: densify past 1-min window; issued={issued_hts:?}"
        );

        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn assign_depth_densify_cache_and_early_exits() {
        let (dir, hub) = tmp_hub();
        hub.ensure_genesis().unwrap();
        let mut st = IbdWorkState::new(vec![dummy_slot(0), dummy_slot(1)], None, Some(0));
        let stats = LoopStats::default();
        let mut cfg = IbdConfig::for_test();
        cfg.window = 64;
        cfg.per_peer = 8;

        for ht in 1u32..=12 {
            let hash = h(ht);
            st.record_height(hash, ht);
            st.height_to_hash.insert(ht, hash);
            st.ordered_set.insert(hash);
            st.ordered.push_back(hash);
            st.max_ordered_height = ht;
            st.body.mark_missing(hash);
        }

        assign_work_ordered(
            &mut st,
            &hub,
            &cfg,
            &stats,
            1.0,
            1,
            AssignDepth::Critical,
            true,
            None,
        );
        let after_crit = st.inflight.len();
        assert!(after_crit > 0, "critical should still issue tip/race");

        let n_before = st.inflight.len();
        assign_work_ordered(
            &mut st,
            &hub,
            &cfg,
            &stats,
            1.0,
            1,
            AssignDepth::Full,
            false, // archive cannot assign
            None,
        );
        assert!(st.inflight.len() <= n_before + 8);

        let hashes: Vec<_> = st.inflight.keys().copied().collect();
        for hash in hashes {
            clear_hash_inflight(&mut st.slots, &mut st.inflight, hash);
            st.body.mark_missing(hash);
        }
        st.body.mark_pending(h(5));
        let _ = st.body.expire_stale_pending_if(std::time::Duration::ZERO, |_| true);
        st.body.mark_pending(h(5));
        st.body.mark_pending(h(1));
        let expired = st
            .body
            .expire_stale_pending_if(std::time::Duration::ZERO, |_| true);
        for hash in expired {
            clear_hash_inflight(&mut st.slots, &mut st.inflight, hash);
            st.body.mark_missing(hash);
        }

        assign_work_ordered(
            &mut st,
            &hub,
            &cfg,
            &stats,
            1.0,
            1,
            AssignDepth::Full,
            true,
            None,
        );
        assert!(st.inflight.len() > 0);
        assert!(stats.assign_issued.load(Ordering::Relaxed) > 0);

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
            None,
        );
        assert!(st.inflight.len() <= n_full + 2);

        st.inflight.clear();
        st.slots[0].in_flight.clear();
        st.slots[1].in_flight.clear();
        for ht in 1u32..=4 {
            let hash = h(ht);
            st.body.mark_missing(hash);
        }
        for i in 0..cfg.per_peer {
            let hash = h(200 + i as u32);
            st.slots[0].in_flight.insert(hash);
            inflight_add_peer(&mut st.inflight, hash, 0);
        }
        assign_work_ordered(
            &mut st,
            &hub,
            &cfg,
            &stats,
            0.5,
            1,
            AssignDepth::Full,
            true,
            None,
        );
        assert!(st.slots[1].in_flight.len() > 0 || st.inflight.len() > cfg.per_peer);

        for ht in 1u32..=12 {
            st.body.mark_archived(h(ht));
        }
        st.inflight.clear();
        st.slots.iter_mut().for_each(|s| s.in_flight.clear());
        st.max_ready_height = 12;
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
            None,
        );
        assert!(st.inflight.is_empty());

        let _ = std::fs::remove_dir_all(dir);
    }
}
