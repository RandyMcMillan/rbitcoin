//! Peer dial, header request, stall disconnect / cooldown.

use super::peer_io::{ibd_mono_ms, spawn_peer, PeerCmd, PeerEventSinks, PeerSlot};
use crate::chain::ChainHub;
use crate::error::NetError;
use bitcoin::hashes::Hash;
use bitcoin::p2p::Magic;
use bitcoin::BlockHash;
use rbitcoin_log::{error, warn};
use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// How long to avoid redialing an address after a stall disconnect.
pub(crate) const STALL_ADDR_COOLDOWN: Duration = Duration::from_secs(10 * 60);

pub(crate) async fn dial_batch(
    pool: &[SocketAddr],
    next_id: &AtomicUsize,
    count: usize,
    mut already: HashSet<SocketAddr>,
    magic: Magic,
    local_addr: SocketAddr,
    tip_h: Option<u32>,
    sinks: PeerEventSinks,
    connect_timeout: Duration,
    cancel: Option<Arc<std::sync::atomic::AtomicBool>>,
) -> Vec<PeerSlot> {
    let mut out = Vec::new();
    if count == 0 || pool.is_empty() {
        return out;
    }
    let cancelled = || {
        cancel
            .as_ref()
            .map(|c| c.load(Ordering::SeqCst))
            .unwrap_or(false)
    };

    let mut handles = Vec::new();
    let n = pool.len();
    let mut spawned = 0usize;
    // Scan at most one full rotation of the pool.
    for _ in 0..n {
        if cancelled() {
            break;
        }
        if spawned >= count {
            break;
        }
        let id = next_id.fetch_add(1, Ordering::Relaxed);
        let addr = pool[id % n];
        // One connection per addr per batch (and vs caller-provided already).
        if !already.insert(addr) {
            continue;
        }
        let sinks = sinks.clone();
        handles.push(tokio::spawn(async move {
            let fut = spawn_peer(id, addr, magic, local_addr, tip_h, sinks);
            match tokio::time::timeout(connect_timeout, fut).await {
                Ok(Ok(slot)) => Ok(slot),
                Ok(Err(e)) => Err((id, addr, e.to_string())),
                Err(_) => Err((id, addr, format!("connect timeout ({connect_timeout:?})"))),
            }
        }));
        spawned += 1;
    }
    for h in handles {
        if cancelled() {
            h.abort();
            continue;
        }
        match h.await {
            Ok(Ok(slot)) => out.push(slot),
            Ok(Err((id, addr, reason))) => {
                warn!("ibd: parallel peer[{id}] {addr} failed: {reason}");
            }
            Err(e) => {
                error!("ibd: peer connect task panicked: {e}");
            }
        }
    }
    out.sort_by_key(|s| s.id);
    out
}

pub(crate) fn request_headers(
    slots: &[PeerSlot],
    hub: &ChainHub,
    seq: &mut u32,
    // Best hashes on the IBD work path (newest first preferred). When tip
    // lags archive, store locators alone re-fetch the same 2000-header window.
    work_tips: &[BlockHash],
) -> Result<bool, NetError> {
    let alive: Vec<usize> = slots.iter().filter(|s| s.alive).map(|s| s.id).collect();
    if alive.is_empty() {
        return Ok(false);
    }
    let peer = alive[(*seq as usize) % alive.len()];
    *seq = seq.saturating_add(1);
    request_headers_from(slots, peer, hub, seq, work_tips)
}

pub(crate) fn request_headers_from(
    slots: &[PeerSlot],
    peer: usize,
    hub: &ChainHub,
    _seq: &mut u32,
    work_tips: &[BlockHash],
) -> Result<bool, NetError> {
    let Some(s) = slots.iter().find(|s| s.id == peer && s.alive) else {
        return Ok(false);
    };
    let locator = ibd_header_locator(hub, work_tips)?;
    if s.cmd_tx.send(PeerCmd::GetHeaders { locator }).is_err() {
        return Ok(false);
    }
    Ok(true)
}

/// Locator for IBD getheaders: prefer the **work-path tip** (highest ordered /
/// archived hash) ahead of the confirmed store tip.
///
/// Signet bug: with only `query.locator_hashes()` (confirmed tip), when archive
/// led tip by a full headers window (~2000), peers re-served that same window
/// forever; we marked `headers_done` and exited IBD at height 2000 while
/// `max_peer_height` was still ~313k.
pub(crate) fn ibd_header_locator(
    hub: &ChainHub,
    work_tips: &[BlockHash],
) -> Result<Vec<BlockHash>, NetError> {
    let mut locator = Vec::with_capacity(32);
    for h in work_tips {
        if !locator.contains(h) {
            locator.push(*h);
        }
        if locator.len() >= 8 {
            break;
        }
    }
    if let Some(t) = hub.tip_hash() {
        if !locator.contains(&t) {
            locator.push(t);
        }
    }
    let rest = hub
        .query
        .locator_hashes()
        .map_err(|e| NetError::Consensus(e.to_string()))?;
    for h in rest {
        if !locator.contains(&h) {
            locator.push(h);
        }
        if locator.len() >= crate::codec::MAX_LOCATOR_SZ {
            break;
        }
    }
    if locator.is_empty() {
        locator.push(BlockHash::from_byte_array([0u8; 32]));
    }
    Ok(locator)
}

pub(crate) fn release_peer_block_work(
    slots: &mut [PeerSlot],
    inflight: &mut HashMap<bitcoin::BlockHash, super::state::InflightReq>,
    peer: usize,
) {
    if let Some(s) = slots.iter_mut().find(|s| s.id == peer) {
        s.alive = false;
        for h in s.in_flight.drain() {
            if inflight.get(&h).map(|e| e.peer == peer).unwrap_or(false) {
                inflight.remove(&h);
            }
        }
    }
}

/// Addrs we must not dial: currently connected/slot-held + still-cooling stall bans.
pub(crate) fn dial_blocked_addrs(
    slots: &[PeerSlot],
    cooldown: &HashMap<SocketAddr, Instant>,
    now: Instant,
) -> HashSet<SocketAddr> {
    let mut blocked: HashSet<SocketAddr> = slots.iter().map(|s| s.addr).collect();
    for (&addr, &until) in cooldown {
        if until > now {
            blocked.insert(addr);
        }
    }
    blocked
}

pub(crate) fn expire_addr_cooldown(cooldown: &mut HashMap<SocketAddr, Instant>, now: Instant) {
    cooldown.retain(|_, until| *until > now);
}

/// One stall rule: if a peer has outstanding block getdata and no **block**
/// progress for `stall`, disconnect it and free its work for reassignment.
///
/// Progress = payload bytes (atomic), complete `block`, or `notfound`.
/// Headers/pings do not count. Clock resets when we issue new getdata.
///
/// Stalled addresses enter a cooldown so redial does not immediately re-open
/// the same host under a new peer id (log spam + wasted slots).
pub(crate) fn disconnect_stalled_block_peers(
    slots: &mut [PeerSlot],
    inflight: &mut HashMap<bitcoin::BlockHash, super::state::InflightReq>,
    addr_cooldown: &mut HashMap<SocketAddr, Instant>,
    now: Instant,
    stall: Duration,
) {
    let stall = stall.max(Duration::from_secs(30));
    let stall_ms = stall.as_millis() as u64;
    let now_ms = ibd_mono_ms();
    let stalled_peers: Vec<(usize, usize, SocketAddr)> = slots
        .iter()
        .filter(|s| s.alive && !s.in_flight.is_empty())
        .filter(|s| {
            now_ms.saturating_sub(s.block_progress_ms.load(Ordering::Relaxed)) > stall_ms
        })
        .map(|s| (s.id, s.in_flight.len(), s.addr))
        .collect();
    for (id, n_work, addr) in stalled_peers {
        warn!(
            "ibd: peer[{id}] {addr} stalled (no block progress for {stall:?}, {n_work} in-flight) — disconnect + reassign (cooldown {STALL_ADDR_COOLDOWN:?})"
        );
        addr_cooldown.insert(addr, now + STALL_ADDR_COOLDOWN);
        if let Some(s) = slots.iter_mut().find(|s| s.id == id) {
            let _ = s.cmd_tx.send(PeerCmd::Shutdown);
            s.task.abort();
        }
        release_peer_block_work(slots, inflight, id);
    }
}
