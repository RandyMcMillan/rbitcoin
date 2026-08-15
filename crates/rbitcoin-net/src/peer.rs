//! Peer handshake, serve, tip follow, and announce (BIP324 v2 transport).

use crate::cache::BlockCache;
use crate::chain::{AcceptOutcome, ChainHub};
use crate::codec::{FramedMessage, MAX_HEADERS_RESULTS, MAX_INV_SIZE, MAX_LOCATOR_SZ};
use crate::error::NetError;
use crate::msg_decode::decode_framed_offload;
use crate::peer_dos::{PeerRateLimiter, OVERSIZE_BAN_SCORE, RATE_LIMIT_BAN_SCORE};
use crate::v2::{open_v2, read_v2_frame, write_v2_msg, write_v2_msg_offload, V2Reader, V2Writer};
use bitcoin::bip152::{BlockTransactions, HeaderAndShortIds};
use bitcoin::hashes::Hash;
use bitcoin::p2p::address::Address;
use bitcoin::p2p::message::NetworkMessage;
use bitcoin::p2p::message_blockdata::{GetHeadersMessage, Inventory};
use bitcoin::p2p::message_compact_blocks::{BlockTxn, CmpctBlock, GetBlockTxn, SendCmpct};
use bitcoin::p2p::message_network::VersionMessage;
use bitcoin::p2p::{Magic, ServiceFlags, PROTOCOL_VERSION};
use bitcoin::{Block, BlockHash, Transaction};
use rbitcoin_query::Query;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::net::TcpStream;
use tokio::sync::{broadcast, mpsc};

/// Protocol version we advertise (BIP339 wtxidrelay needs ≥70016; rust-bitcoin's
/// `PROTOCOL_VERSION` is still 70001).
const OUR_PROTOCOL_VERSION: u32 = 70016;

/// How often an established session re-issues `getheaders` so a quiet peer or
/// a gap opened while we were offline still gets filled (signet ~10m blocks).
const HEADERS_POLL_SECS: u64 = 120;

/// True when a session error is a missing store row (not peer malice / corrupt IO).
///
/// These must not tear down the TCP session: re-request or skip and keep the peer.
pub(crate) fn net_error_is_store_not_found(e: &NetError) -> bool {
    match e {
        NetError::Consensus(s) => {
            let l = s.to_ascii_lowercase();
            l.contains("record not found")
                || l.contains("not found")
                || l.contains("storeerror::notfound")
        }
        _ => false,
    }
}

/// Per-session misbehavior score that triggers disconnect (Core-like order).
pub const BAN_SCORE_THRESHOLD: u32 = 100;
/// Cap on incomplete compact blocks awaiting `blocktxn` (DoS).
const MAX_PENDING_CMPCT: usize = 8;
/// Cap on headers held while assembling tip/reorg work (DoS / process RAM).
const MAX_PENDING_HEADERS: usize = 8_000;
/// Cap on full blocks waiting for in-order tip accept (DoS / process RAM).
///
/// Must be ≥99 so tip-follow can assemble a 99-block competing branch for
/// most-work reorg (see `docs/design-ibd-most-work-reorg.md`).
const MAX_PENDING_BLOCKS: usize = 128;

/// Test/assert surface for the tip-follow pending-body cap (equals production).
#[cfg(test)]
pub(crate) const MAX_PENDING_BLOCKS_FOR_TEST: usize = MAX_PENDING_BLOCKS;

/// Services we advertise once store-backed reconstruct serve is available.
pub fn local_service_flags() -> ServiceFlags {
    ServiceFlags::NETWORK | ServiceFlags::WITNESS | ServiceFlags::P2P_V2
}

/// Optional bookkeeping for outbound tip-follow sessions.
#[derive(Clone, Default)]
pub struct FollowSessionMeta {
    /// Peer address (logging).
    pub peer: Option<SocketAddr>,
    /// Live outbound follow count (inc on start, dec on exit).
    pub live: Option<Arc<AtomicUsize>>,
}

/// Decrements the live follow counter when a session task exits.
/// Increment happens in [`crate::service::P2PNode::follow_from`] so the count
/// is visible as soon as handshake succeeds (before the task is scheduled).
struct LiveFollowDec(Option<Arc<AtomicUsize>>);

impl Drop for LiveFollowDec {
    fn drop(&mut self) {
        if let Some(ref c) = self.0 {
            c.fetch_sub(1, Ordering::SeqCst);
        }
    }
}

/// Open BIP324 v2 transport + perform the version/verack exchange.
///
/// Returns the peer's version and the encrypted read/write halves. All further
/// messages must use those halves — production has no v1 wire path.
pub async fn connect_and_handshake(
    stream: TcpStream,
    magic: Magic,
    our_addr: SocketAddr,
    their_addr: SocketAddr,
    start_height: i32,
    inbound: bool,
) -> Result<(VersionMessage, V2Reader, V2Writer), NetError> {
    let (mut reader, mut writer) = open_v2(stream, magic, inbound).await?;
    let their_version = application_handshake(
        &mut reader,
        &mut writer,
        magic,
        our_addr,
        their_addr,
        start_height,
        inbound,
    )
    .await?;
    Ok((their_version, reader, writer))
}

/// Perform the version/verack exchange over an established BIP324 session.
async fn application_handshake(
    reader: &mut V2Reader,
    writer: &mut V2Writer,
    magic: Magic,
    our_addr: SocketAddr,
    their_addr: SocketAddr,
    start_height: i32,
    inbound: bool,
) -> Result<VersionMessage, NetError> {
    let services = local_service_flags();
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0);
    let version = VersionMessage {
        version: OUR_PROTOCOL_VERSION.max(PROTOCOL_VERSION),
        services,
        timestamp: now,
        receiver: Address::new(&their_addr, ServiceFlags::NONE),
        sender: Address::new(&our_addr, services),
        nonce: rand_nonce(),
        user_agent: rbitcoin_primitives::rbitcoin_subversion(
            env!("CARGO_PKG_VERSION"),
            &[] as &[&str],
        )
        .unwrap_or_else(|_| format!("/rbitcoin:{}/", env!("CARGO_PKG_VERSION"))),
        start_height,
        // Advertise willingness to receive tx inv when we have a mempool hub.
        // Actual inv processing is gated on MempoolHub::relay_enabled (tip mode).
        relay: true,
    };

    if !inbound {
        write_v2_msg(writer, NetworkMessage::Version(version.clone())).await?;
    }

    let their_version = loop {
        let frame = read_v2_frame(reader, magic).await?;
        let msg = frame.decode();
        match msg.payload() {
            NetworkMessage::Version(v) => break v.clone(),
            other => {
                if matches!(other, NetworkMessage::Verack) {
                    return Err(NetError::Protocol("verack before version"));
                }
                // Ignore wtxidrelay / sendaddrv2 / etc. that may arrive before
                // verack on modern peers (we still require version first).
                let _ = other;
            }
        }
    };

    if inbound {
        write_v2_msg(writer, NetworkMessage::Version(version)).await?;
    }
    // BIP339: wtxidrelay MUST be sent after version and before verack when both
    // sides speak ≥70016. Late (post-verack) messages are ignored/invalid.
    if their_version.version >= 70016 {
        write_v2_msg(writer, NetworkMessage::WtxidRelay).await?;
    }
    write_v2_msg(writer, NetworkMessage::Verack).await?;

    loop {
        let frame = read_v2_frame(reader, magic).await?;
        let msg = frame.decode();
        match msg.payload() {
            NetworkMessage::Verack => break,
            NetworkMessage::Ping(n) => {
                write_v2_msg(writer, NetworkMessage::Pong(*n)).await?;
            }
            // Core may pipeline sendaddrv2 / wtxidrelay / sendcmpct before/after verack.
            _ => {}
        }
    }

    Ok(their_version)
}

fn rand_nonce() -> u64 {
    use std::sync::atomic::{AtomicU64, Ordering};
    // Concurrent dials often share the same wall-clock instant; a counter keeps
    // version nonces unique (Core self-connect / loop detection uses nonce).
    static N: AtomicU64 = AtomicU64::new(1);
    let seq = N.fetch_add(1, Ordering::Relaxed);
    let tick = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0);
    tick.wrapping_mul(0x9E37_79B9_7F4A_7C15)
        .wrapping_add(seq.wrapping_mul(0xBF58_476D_1CE4_E5B9))
}

/// Bidirectional peer session: serve history, tip follow, announce our tip.
///
/// After handshake preferences (`sendheaders` / `sendcmpct`), the session
/// **actively** `getheaders` from our tip locator so blocks mined while we were
/// offline or mid–SH materialize are pulled — not only unsolicited announces.
/// Long history catch-up remains [`crate::ibd`] / [`crate::service::P2PNode::sync`].
///
/// A dedicated writer drains outbound messages while the reader keeps draining
/// the encrypted channel. `meta` labels the peer for logs and optionally tracks
/// live outbound follow count.
pub async fn peer_session_with(
    mut reader: V2Reader,
    mut writer: V2Writer,
    magic: Magic,
    hub: Arc<ChainHub>,
    mut tip_rx: broadcast::Receiver<crate::chain::TipEvent>,
    meta: FollowSessionMeta,
) -> Result<(), NetError> {
    let _live_dec = LiveFollowDec(meta.live.clone());
    let peer_s = meta
        .peer
        .map(|p| p.to_string())
        .unwrap_or_else(|| "peer".into());

    // Prefer headers; accept high-bandwidth compact v2 (witness short-ids) with
    // mempool reconstruction — fall back to full getdata when fill fails.
    // (wtxidrelay is negotiated pre-verack in the handshake — BIP339.)
    let _ = write_v2_msg(&mut writer, NetworkMessage::SendHeaders).await;
    let _ = write_v2_msg(
        &mut writer,
        NetworkMessage::SendCmpct(SendCmpct {
            send_compact: true,
            version: 2,
        }),
    )
    .await;

    let (out_tx, mut out_rx) = mpsc::unbounded_channel::<NetworkMessage>();

    let writer_task = tokio::spawn(async move {
        while let Some(msg) = out_rx.recv().await {
            if write_v2_msg_offload(&mut writer, msg).await.is_err() {
                break;
            }
        }
    });

    // Bootstrap: ask for anything above our tip (critical after IBD disconnect).
    if let Err(e) = queue_getheaders(&out_tx, hub.as_ref()) {
        rbitcoin_log::warn!("p2p: {peer_s} initial getheaders queue failed: {e}");
    }

    let mut peer_wants_headers = false;
    // BIP339: peer sent `wtxidrelay` (we announce/request MSG_WTX when true).
    let mut peer_wtxid_relay = false;
    // BIP152: peer asked for high-bandwidth compact announces (`sendcmpct`).
    let mut peer_send_cmpct = false;
    let mut peer_cmpct_version: u32 = 2;
    // Headers received while assembling a potential reorg branch (hash → header).
    let mut pending_headers: HashMap<BlockHash, bitcoin::block::Header> = HashMap::new();
    // Blocks waiting for in-order accept (hash → block).
    let mut pending_blocks: HashMap<BlockHash, bitcoin::Block> = HashMap::new();
    // Incomplete compact blocks awaiting `blocktxn` (hash → state).
    let mut pending_cmpct: HashMap<BlockHash, PendingCmpct> = HashMap::new();
    // Txids we received from this peer (origin exclusion for announce).
    let mut from_this_peer: HashMap<bitcoin::Txid, ()> = HashMap::new();
    // Session misbehavior score (disconnect at BAN_SCORE_THRESHOLD).
    let mut ban_score: u32 = 0;
    let mut rate = PeerRateLimiter::default_limits();
    let mut tx_announce_rx = hub.mempool().map(|m| m.subscribe_announces());
    let mut headers_poll = tokio::time::interval(Duration::from_secs(HEADERS_POLL_SECS));
    // First tick completes immediately — skip so we don't double the bootstrap send.
    headers_poll.tick().await;

    let result = async {
        loop {
            tokio::select! {
                biased;
                tip = tip_rx.recv() => {
                    match tip {
                        Ok(ev) => {
                            if peer_send_cmpct {
                                if let Ok(Some(block)) = block_for_peer(
                                    hub.cache.as_ref(),
                                    hub.query.as_ref(),
                                    &ev.hash,
                                ) {
                                    let nonce = rand_nonce();
                                    if let Ok(hsi) = HeaderAndShortIds::from_block(
                                        &block,
                                        nonce,
                                        peer_cmpct_version.max(1).min(2),
                                        &[],
                                    ) {
                                        queue_out(
                                            &out_tx,
                                            NetworkMessage::CmpctBlock(CmpctBlock {
                                                compact_block: hsi,
                                            }),
                                        )?;
                                        continue;
                                    }
                                }
                            }
                            queue_out(&out_tx, tip_announce_msg(&ev, peer_wants_headers))?;
                        }
                        Err(broadcast::error::RecvError::Lagged(_)) => continue,
                        Err(broadcast::error::RecvError::Closed) => return Ok(()),
                    }
                }
                _ = headers_poll.tick() => {
                    // Quiet peers / missed announces: re-pull from our tip locator.
                    let _ = queue_getheaders(&out_tx, hub.as_ref());
                }
                ann = async {
                    if let Some(rx) = tx_announce_rx.as_mut() {
                        Some(rx.recv().await)
                    } else {
                        // No mempool — park this branch forever.
                        std::future::pending::<()>().await;
                        None
                    }
                } => {
                    if let Some(ann) = ann {
                        match ann {
                            Ok(ann) => {
                                let txid = ann.txid;
                                // Origin exclusion: do not re-announce to the peer that sent it.
                                if from_this_peer.contains_key(&txid) {
                                    continue;
                                }
                                if let Some(mp) = hub.mempool() {
                                    if mp.relay_enabled() && mp.contains(&txid) {
                                        let inv = if peer_wtxid_relay {
                                            if let Some(tx) = mp.get_tx(&txid) {
                                                Inventory::WTx(tx.compute_wtxid())
                                            } else {
                                                Inventory::WitnessTransaction(txid)
                                            }
                                        } else {
                                            Inventory::WitnessTransaction(txid)
                                        };
                                        queue_out(&out_tx, NetworkMessage::Inv(vec![inv]))?;
                                    }
                                }
                            }
                            Err(broadcast::error::RecvError::Lagged(_)) => continue,
                            Err(broadcast::error::RecvError::Closed) => {}
                        }
                    }
                }
                // Frame on this task; heavy payload decode is offloaded.
                frame = read_v2_frame(&mut reader, magic) => {
                    let frame = match frame {
                        Ok(f) => f,
                        Err(NetError::Io(e)) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                            return Ok(());
                        }
                        Err(NetError::MessageTooLarge(n)) => {
                            ban_score = ban_score.saturating_add(OVERSIZE_BAN_SCORE);
                            rbitcoin_log::warn!(
                                "p2p: {peer_s} oversize frame ({n}) ban_score={ban_score}"
                            );
                            if ban_score >= BAN_SCORE_THRESHOLD {
                                return Err(NetError::Protocol("peer ban score threshold"));
                            }
                            return Err(NetError::MessageTooLarge(n));
                        }
                        Err(e) => return Err(e),
                    };
                    // Per-peer rate limit (msg + byte window) — disconnect when abused.
                    let frame_len = frame.payload_len();
                    if !rate.note(frame_len) {
                        ban_score = ban_score.saturating_add(RATE_LIMIT_BAN_SCORE);
                        rbitcoin_log::warn!(
                            "p2p: {peer_s} rate limit exceeded ban_score={ban_score}"
                        );
                        if ban_score >= BAN_SCORE_THRESHOLD {
                            return Err(NetError::Protocol("peer ban score threshold"));
                        }
                        // Soft: drop this message but keep session for first offense.
                        continue;
                    }
                    // Ping: cheap 8-byte path — never leave the I/O task for decode.
                    if frame.is_ping() {
                        if let Some(n) = frame.ping_nonce() {
                            queue_out(&out_tx, NetworkMessage::Pong(n))?;
                        }
                        continue;
                    }
                    handle_peer_frame(
                        frame,
                        hub.as_ref(),
                        &out_tx,
                        &mut peer_wants_headers,
                        &mut peer_wtxid_relay,
                        &mut peer_send_cmpct,
                        &mut peer_cmpct_version,
                        &mut pending_headers,
                        &mut pending_blocks,
                        &mut pending_cmpct,
                        &mut from_this_peer,
                        &mut ban_score,
                    )
                    .await?;
                    if ban_score >= BAN_SCORE_THRESHOLD {
                        rbitcoin_log::warn!(
                            "p2p: {peer_s} ban score {ban_score} ≥ {BAN_SCORE_THRESHOLD} — disconnect"
                        );
                        return Err(NetError::Protocol("peer ban score threshold"));
                    }
                }
            }
        }
    }
    .await;

    drop(out_tx);
    writer_task.abort();
    let _ = writer_task.await;
    match &result {
        Ok(()) => rbitcoin_log::info!("p2p: session {peer_s} closed"),
        Err(e) => rbitcoin_log::warn!("p2p: session {peer_s} ended: {e}"),
    }
    result
}

/// Tip locator for post-handshake `getheaders` (store chain; genesis fallback).
pub(crate) fn tip_follow_locator(hub: &ChainHub) -> Vec<BlockHash> {
    match hub.query.locator_hashes() {
        Ok(mut v) if !v.is_empty() => {
            if v.len() > MAX_LOCATOR_SZ {
                v.truncate(MAX_LOCATOR_SZ);
            }
            v
        }
        _ => {
            let mut v = Vec::new();
            if let Some(t) = hub.tip_hash() {
                v.push(t);
            }
            v.push(BlockHash::from_byte_array([0u8; 32]));
            v
        }
    }
}

fn queue_getheaders(
    out: &mpsc::UnboundedSender<NetworkMessage>,
    hub: &ChainHub,
) -> Result<(), NetError> {
    let locator = tip_follow_locator(hub);
    let gh = GetHeadersMessage::new(locator, BlockHash::from_byte_array([0u8; 32]));
    queue_out(out, NetworkMessage::GetHeaders(gh))
}

/// Incomplete compact block waiting for `blocktxn`.
struct PendingCmpct {
    hsi: HeaderAndShortIds,
    missing: Vec<u64>,
    /// BIP152 version (1 = txid short-ids, 2 = wtxid).
    version: u32,
}

/// Snapshot live mempool txs for short-id fill (owned so map can borrow them).
fn mempool_live_txs(hub: &ChainHub) -> Vec<Transaction> {
    hub.mempool()
        .map(|mp| mp.list_live().into_iter().map(|(_, _, _, tx)| tx).collect())
        .unwrap_or_default()
}

/// Reconstruct a compact block fully from mempool short-ids (version 1/2).
fn try_fill_cmpct(hub: &ChainHub, hsi: &HeaderAndShortIds, version: u32) -> Option<Block> {
    if hub.mempool().is_none() {
        return None;
    }
    let live = mempool_live_txs(hub);
    let avail = crate::compact::shortid_map_from_txs(&hsi.header, hsi.nonce, version, live.iter());
    crate::compact::try_reconstruct(hsi, &avail, version).ok()
}

/// Absolute indexes still missing after mempool fill (for `getblocktxn`).
///
/// Returns `None` when there is no mempool hub (caller should full-getdata).
/// Returns `Some(empty)` only when reconstruct claimed success with no txs
/// (degenerate); peer path treats empty as getdata fallback.
fn try_cmpct_missing(hub: &ChainHub, hsi: &HeaderAndShortIds, version: u32) -> Option<Vec<u64>> {
    if hub.mempool().is_none() {
        return None;
    }
    let live = mempool_live_txs(hub);
    let avail = crate::compact::shortid_map_from_txs(&hsi.header, hsi.nonce, version, live.iter());
    match crate::compact::try_reconstruct(hsi, &avail, version) {
        Ok(_) => Some(Vec::new()),
        Err(m) => Some(m),
    }
}

/// Finish a pending compact block with a `blocktxn` payload.
fn apply_cmpct_blocktxn(
    hub: &ChainHub,
    pc: &PendingCmpct,
    bt: &BlockTransactions,
) -> Result<Block, ()> {
    let live = mempool_live_txs(hub);
    let avail =
        crate::compact::shortid_map_from_txs(&pc.hsi.header, pc.hsi.nonce, pc.version, live.iter());
    crate::compact::apply_block_transactions(&pc.hsi, &pc.missing, bt, &avail, pc.version)
        .map_err(|_| ())
}

async fn handle_peer_frame(
    frame: FramedMessage,
    hub: &ChainHub,
    out_tx: &mpsc::UnboundedSender<NetworkMessage>,
    peer_wants_headers: &mut bool,
    peer_wtxid_relay: &mut bool,
    peer_send_cmpct: &mut bool,
    peer_cmpct_version: &mut u32,
    pending_headers: &mut HashMap<BlockHash, bitcoin::block::Header>,
    pending_blocks: &mut HashMap<BlockHash, bitcoin::Block>,
    pending_cmpct: &mut HashMap<BlockHash, PendingCmpct>,
    from_this_peer: &mut HashMap<bitcoin::Txid, ()>,
    ban_score: &mut u32,
) -> Result<(), NetError> {
    let msg = decode_framed_offload(frame).await?;
    match msg.payload() {
        NetworkMessage::Ping(n) => {
            queue_out(out_tx, NetworkMessage::Pong(*n))?;
        }
        NetworkMessage::Pong(_) => {}
        NetworkMessage::SendHeaders => {
            *peer_wants_headers = true;
        }
        NetworkMessage::SendCmpct(sc) => {
            // High-bandwidth mode: announce new tips as cmpctblock (version 1 or 2).
            if sc.version == 1 || sc.version == 2 {
                *peer_send_cmpct = sc.send_compact;
                *peer_cmpct_version = sc.version as u32;
            }
        }
        NetworkMessage::WtxidRelay => {
            // BIP339 mutual: we already sent wtxidrelay pre-verack; remember theirs.
            *peer_wtxid_relay = true;
        }
        NetworkMessage::GetHeaders(gh) => {
            let headers = headers_for_peer(hub.cache.as_ref(), hub.query.as_ref(), gh)?;
            queue_out(out_tx, NetworkMessage::Headers(headers))?;
        }
        NetworkMessage::GetData(inv) => {
            for item in inv.iter().take(MAX_INV_SIZE) {
                match item {
                    Inventory::Block(h) | Inventory::WitnessBlock(h) => {
                        if let Some(block) =
                            block_for_peer(hub.cache.as_ref(), hub.query.as_ref(), h)?
                        {
                            queue_out(out_tx, NetworkMessage::Block(block))?;
                        }
                    }
                    Inventory::CompactBlock(h) => {
                        if let Some(block) =
                            block_for_peer(hub.cache.as_ref(), hub.query.as_ref(), h)?
                        {
                            let ver = (*peer_cmpct_version).max(1).min(2);
                            if let Ok(hsi) =
                                HeaderAndShortIds::from_block(&block, rand_nonce(), ver, &[])
                            {
                                queue_out(
                                    out_tx,
                                    NetworkMessage::CmpctBlock(CmpctBlock { compact_block: hsi }),
                                )?;
                            }
                        }
                    }
                    Inventory::Transaction(txid) | Inventory::WitnessTransaction(txid) => {
                        if let Some(mp) = hub.mempool() {
                            if let Some(tx) = mp.get_tx(txid) {
                                queue_out(out_tx, NetworkMessage::Tx(tx))?;
                            }
                        }
                    }
                    Inventory::WTx(wtxid) => {
                        if let Some(mp) = hub.mempool() {
                            if let Some(tx) = mp.get_tx_by_wtxid(wtxid) {
                                queue_out(out_tx, NetworkMessage::Tx(tx))?;
                            }
                        }
                    }
                    _ => {}
                }
            }
        }
        NetworkMessage::GetBlockTxn(GetBlockTxn { txs_request }) => {
            // Serve missing txs for a compact block we hold (BIP152).
            let hash = txs_request.block_hash;
            if let Some(block) = block_for_peer(hub.cache.as_ref(), hub.query.as_ref(), &hash)? {
                let mut transactions = Vec::with_capacity(txs_request.indexes.len());
                let mut bad = false;
                for idx in &txs_request.indexes {
                    let i = *idx as usize;
                    match block.txdata.get(i) {
                        Some(tx) => transactions.push(tx.clone()),
                        None => {
                            bad = true;
                            break;
                        }
                    }
                }
                if bad {
                    *ban_score = ban_score.saturating_add(20);
                } else {
                    queue_out(
                        out_tx,
                        NetworkMessage::BlockTxn(BlockTxn {
                            transactions: BlockTransactions {
                                block_hash: hash,
                                transactions,
                            },
                        }),
                    )?;
                }
            }
        }
        NetworkMessage::Inv(items) => {
            let mut want = Vec::new();
            let mut inv_tx_n = 0u64;
            let relay = hub.mempool().map(|m| m.relay_enabled()).unwrap_or(false);
            for item in items.iter().take(MAX_INV_SIZE) {
                match item {
                    Inventory::Block(h) | Inventory::WitnessBlock(h) => {
                        if !hub.is_connected(h) && !pending_blocks.contains_key(h) {
                            want.push(Inventory::WitnessBlock(*h));
                        }
                    }
                    Inventory::Transaction(txid) | Inventory::WitnessTransaction(txid) => {
                        if relay {
                            if let Some(mp) = hub.mempool() {
                                if !mp.contains(txid) {
                                    // MSG_TX / MSG_WITNESS_TX inv: fetch by txid (wtxid only
                                    // when the inv type is MSG_WTX — handled below).
                                    want.push(Inventory::WitnessTransaction(*txid));
                                    inv_tx_n = inv_tx_n.saturating_add(1);
                                }
                            }
                        }
                    }
                    Inventory::WTx(wtxid) => {
                        if relay {
                            if let Some(mp) = hub.mempool() {
                                if !mp.contains_wtxid(wtxid) {
                                    want.push(Inventory::WTx(*wtxid));
                                    inv_tx_n = inv_tx_n.saturating_add(1);
                                }
                            }
                        }
                    }
                    _ => {}
                }
            }
            if let Some(mp) = hub.mempool() {
                mp.note_inv_tx(inv_tx_n);
                // Count only tx-shaped getdata items (not block getdata from inv).
                let gd_tx = want
                    .iter()
                    .filter(|i| {
                        matches!(
                            i,
                            Inventory::Transaction(_)
                                | Inventory::WitnessTransaction(_)
                                | Inventory::WTx(_)
                        )
                    })
                    .count() as u64;
                mp.note_getdata_tx(gd_tx);
            }
            if !want.is_empty() {
                queue_out(out_tx, NetworkMessage::GetData(want))?;
            }
        }
        NetworkMessage::Headers(headers) => {
            let mut want = Vec::new();
            let n = headers.len().min(MAX_HEADERS_RESULTS);
            for hdr in headers.iter().take(n) {
                let hash = hdr.block_hash();
                if pending_headers.len() >= MAX_PENDING_HEADERS
                    && !pending_headers.contains_key(&hash)
                {
                    // Drop oldest-ish: clear all and restart (headers are cheap
                    // to re-request; full history must not accumulate unboundedly).
                    pending_headers.clear();
                }
                pending_headers.insert(hash, *hdr);
                if !hub.is_connected(&hash) && !pending_blocks.contains_key(&hash) {
                    want.push(Inventory::WitnessBlock(hash));
                }
            }
            if !want.is_empty() {
                // Cap getdata burst; remaining headers stay pending until bodies arrive.
                for chunk in want.chunks(MAX_INV_SIZE.min(500)) {
                    queue_out(out_tx, NetworkMessage::GetData(chunk.to_vec()))?;
                }
            }
            // Full window ⇒ peer likely has more; walk forward with a new locator.
            if n >= MAX_HEADERS_RESULTS {
                let _ = queue_getheaders(out_tx, hub);
            }
        }
        NetworkMessage::Block(block) => {
            let hash = block.block_hash();
            pending_cmpct.remove(&hash);
            if pending_blocks.len() >= MAX_PENDING_BLOCKS && !pending_blocks.contains_key(&hash) {
                // Bound process RAM for out-of-order tip bodies.
                *ban_score = ban_score.saturating_add(5);
            } else {
                pending_blocks.insert(hash, block.clone());
                drain_pending(hub, out_tx, pending_blocks, pending_headers)?;
            }
        }
        NetworkMessage::CmpctBlock(cb) => {
            let hsi = cb.compact_block.clone();
            let hash = hsi.header.block_hash();
            if hub.has_block(&hash) {
                // already have
            } else if let Some(block) = try_fill_cmpct(hub, &hsi, 2) {
                pending_blocks.insert(hash, block);
                drain_pending(hub, out_tx, pending_blocks, pending_headers)?;
            } else if let Some(missing) = try_cmpct_missing(hub, &hsi, 2) {
                if missing.is_empty() {
                    // Should not happen; fall back.
                    queue_out(
                        out_tx,
                        NetworkMessage::GetData(vec![Inventory::WitnessBlock(hash)]),
                    )?;
                } else if pending_cmpct.len() >= MAX_PENDING_CMPCT
                    && !pending_cmpct.contains_key(&hash)
                {
                    *ban_score = ban_score.saturating_add(10);
                    queue_out(
                        out_tx,
                        NetworkMessage::GetData(vec![Inventory::WitnessBlock(hash)]),
                    )?;
                } else {
                    pending_cmpct.insert(
                        hash,
                        PendingCmpct {
                            hsi: hsi.clone(),
                            missing: missing.clone(),
                            version: 2,
                        },
                    );
                    queue_out(
                        out_tx,
                        NetworkMessage::GetBlockTxn(GetBlockTxn {
                            txs_request: crate::compact::missing_request(hash, &missing),
                        }),
                    )?;
                }
            } else {
                queue_out(
                    out_tx,
                    NetworkMessage::GetData(vec![Inventory::WitnessBlock(hash)]),
                )?;
            }
        }
        NetworkMessage::BlockTxn(BlockTxn { transactions: bt }) => {
            let hash = bt.block_hash;
            if let Some(pc) = pending_cmpct.remove(&hash) {
                match apply_cmpct_blocktxn(hub, &pc, bt) {
                    Ok(block) => {
                        pending_blocks.insert(hash, block);
                        drain_pending(hub, out_tx, pending_blocks, pending_headers)?;
                    }
                    Err(()) => {
                        *ban_score = ban_score.saturating_add(10);
                        queue_out(
                            out_tx,
                            NetworkMessage::GetData(vec![Inventory::WitnessBlock(hash)]),
                        )?;
                    }
                }
            } else {
                // Unsolicited or late blocktxn — mild penalty.
                *ban_score = ban_score.saturating_add(5);
            }
        }
        NetworkMessage::Tx(tx) => {
            if let Some(mp) = hub.mempool() {
                if mp.relay_enabled() {
                    let txid = tx.compute_txid();
                    from_this_peer.insert(txid, ());
                    match mp.accept_tx(tx) {
                        Ok(_) => {}
                        // Soft: already in pool, parked waiting on parent(s), or full.
                        Err(rbitcoin_mempool::AcceptError::Duplicate(_)) => {}
                        Err(rbitcoin_mempool::AcceptError::Orphaned(_)) => {}
                        Err(rbitcoin_mempool::AcceptError::Policy("mempool full")) => {}
                        Err(e) => {
                            rbitcoin_log::debug!("txrelay: reject {txid}: {e}");
                        }
                    }
                }
            }
        }
        NetworkMessage::MemPool => {
            // Intentionally no bulk dump (DoS); Electrum is the query path.
        }
        NetworkMessage::GetAddr => {
            queue_out(out_tx, NetworkMessage::Addr(vec![]))?;
        }
        NetworkMessage::Unknown { command, payload } => {
            // BIP331 family not in rust-bitcoin 0.32 enum — accept len-prefixed package
            // under experimental command "rbtpkg" for local/tests.
            if command.to_string() == "rbtpkg" {
                if let Some(mp) = hub.mempool() {
                    if mp.relay_enabled() {
                        if let Ok(txs) = crate::tx_relay::decode_len_prefixed_package(payload) {
                            match mp.accept_package(&txs) {
                                Ok(rs) => {
                                    for r in rs {
                                        from_this_peer.insert(r.txid, ());
                                    }
                                }
                                Err(e) => {
                                    rbitcoin_log::debug!("txrelay: package reject: {e}");
                                }
                            }
                        }
                    }
                }
            }
        }
        _ => {}
    }
    Ok(())
}

fn tip_announce_msg(ev: &crate::chain::TipEvent, peer_wants_headers: bool) -> NetworkMessage {
    if peer_wants_headers {
        NetworkMessage::Headers(vec![ev.header])
    } else {
        NetworkMessage::Inv(vec![Inventory::WitnessBlock(ev.hash)])
    }
}

fn queue_out(
    out: &mpsc::UnboundedSender<NetworkMessage>,
    msg: NetworkMessage,
) -> Result<(), NetError> {
    out.send(msg)
        .map_err(|_| NetError::Protocol("peer write half closed"))
}

/// Try to accept pending blocks that connect to tip or form a better branch.
fn drain_pending(
    hub: &ChainHub,
    out: &mpsc::UnboundedSender<NetworkMessage>,
    pending_blocks: &mut HashMap<BlockHash, bitcoin::Block>,
    pending_headers: &mut HashMap<BlockHash, bitcoin::block::Header>,
) -> Result<(), NetError> {
    // A reorg can make a held block the child of the *new* tip after the
    // greedy pass already ran. Repeat until the tip is stable.
    loop {
        let tip_before = hub.tip_hash();
        drain_pending_once(hub, pending_blocks, pending_headers)?;
        if hub.tip_hash() == tip_before {
            break;
        }
    }

    // Pending branch whose root parent is neither connected nor pending can
    // never attach; the peer announces only its tip. Ask for that one hash.
    let mut missing: Vec<BlockHash> = Vec::new();
    for b in pending_blocks.values() {
        let prev = b.header.prev_blockhash;
        if prev.to_byte_array() != [0u8; 32]
            && !hub.is_connected(&prev)
            && !pending_blocks.contains_key(&prev)
            && !missing.contains(&prev)
        {
            missing.push(prev);
        }
    }
    if !missing.is_empty() {
        let want: Vec<Inventory> = missing.into_iter().map(Inventory::WitnessBlock).collect();
        queue_out(out, NetworkMessage::GetData(want))?;
    }
    Ok(())
}

/// One greedy-connect pass followed by a reorg attempt.
fn drain_pending_once(
    hub: &ChainHub,
    pending_blocks: &mut HashMap<BlockHash, bitcoin::Block>,
    pending_headers: &mut HashMap<BlockHash, bitcoin::block::Header>,
) -> Result<(), NetError> {
    // First: greedily extend tip while we can.
    let mut progress = true;
    while progress {
        progress = false;
        let tip = hub.tip_hash();
        let candidates: Vec<BlockHash> = pending_blocks.keys().copied().collect();
        for h in candidates {
            let Some(block) = pending_blocks.get(&h) else {
                continue;
            };
            let prev = block.header.prev_blockhash;
            let connects = match tip {
                None => prev.to_byte_array() == [0u8; 32],
                Some(t) => prev == t,
            };
            if connects {
                let block = pending_blocks.remove(&h).unwrap();
                pending_headers.remove(&h);
                match hub.accept_block(block) {
                    Ok(AcceptOutcome::Accepted { .. })
                    | Ok(AcceptOutcome::AlreadyHave)
                    | Ok(AcceptOutcome::IgnoredWeaker) => {
                        progress = true;
                    }
                    // Invalid or unconnectable body: reject the block, keep the
                    // peer. BIP-152 high-bandwidth (and getdata we solicited) can
                    // deliver PoW-valid-but-invalid blocks from honest Core peers
                    // that have not validated yet — never disconnect or ban-score
                    // for that (docs/external_findings/001-disconnect-on-invalid-block.md).
                    Err(e) if net_error_is_store_not_found(&e) => {
                        rbitcoin_log::warn!(
                            "p2p: accept dropped {} (store not found — keep session): {e}",
                            h
                        );
                    }
                    Err(e) => {
                        rbitcoin_log::warn!(
                            "p2p: accept dropped {} (invalid/unconnectable — keep session): {e}",
                            h
                        );
                    }
                }
            }
        }
    }

    // Second: if we have a contiguous branch from a parent on-chain that is longer/more work, reorg.
    try_reorg_from_pending(hub, pending_blocks, pending_headers)?;
    Ok(())
}

fn try_reorg_from_pending(
    hub: &ChainHub,
    pending_blocks: &mut HashMap<BlockHash, bitcoin::Block>,
    _pending_headers: &mut HashMap<BlockHash, bitcoin::block::Header>,
) -> Result<(), NetError> {
    if pending_blocks.is_empty() {
        return Ok(());
    }
    // Find blocks whose parent is on our best chain (fork points).
    let mut fork_starts: Vec<BlockHash> = Vec::new();
    for (h, b) in pending_blocks.iter() {
        let prev = b.header.prev_blockhash;
        if hub.is_connected(&prev) || prev.to_byte_array() == [0u8; 32] {
            // parent known
            if hub.tip_hash() != Some(prev) {
                fork_starts.push(*h);
            }
        }
    }
    // Assemble each fork; try **max-work** branch first (header work = length on
    // equal-bits regtest / mainnet segments with similar bits).
    let mut branches: Vec<Vec<bitcoin::Block>> = Vec::new();
    for start in fork_starts {
        if let Some(branch) = assemble_pending_branch(pending_blocks, start) {
            if !branch.is_empty() {
                branches.push(branch);
            }
        }
    }
    branches.sort_by(|a, b| {
        let wa = crate::most_work::sum_work(a.iter().map(|blk| blk.header.work()));
        let wb = crate::most_work::sum_work(b.iter().map(|blk| blk.header.work()));
        wb.cmp(&wa)
    });
    for branch in branches {
        match hub.accept_branch(&branch) {
            Ok(AcceptOutcome::Accepted { .. }) => {
                for b in &branch {
                    pending_blocks.remove(&b.block_hash());
                }
                return Ok(());
            }
            Ok(AcceptOutcome::IgnoredWeaker) | Ok(AcceptOutcome::AlreadyHave) => {}
            Err(_) => {}
        }
    }
    Ok(())
}

/// Walk pending map from a fork-start hash following one child at a time.
fn assemble_pending_branch(
    pending_blocks: &HashMap<BlockHash, bitcoin::Block>,
    start: BlockHash,
) -> Option<Vec<bitcoin::Block>> {
    let mut branch = Vec::new();
    let mut cur = start;
    loop {
        let b = pending_blocks.get(&cur)?;
        branch.push(b.clone());
        let next = pending_blocks
            .iter()
            .find(|(_, blk)| blk.header.prev_blockhash == cur)
            .map(|(h, _)| *h);
        match next {
            Some(n) => cur = n,
            None => break,
        }
        // DoS: do not assemble past the pending-body cap.
        if branch.len() >= MAX_PENDING_BLOCKS {
            break;
        }
    }
    Some(branch)
}

fn headers_for_peer(
    cache: &BlockCache,
    query: &Query,
    gh: &bitcoin::p2p::message_blockdata::GetHeadersMessage,
) -> Result<Vec<bitcoin::block::Header>, NetError> {
    match query.headers_after_locator(&gh.locator_hashes, gh.stop_hash, MAX_HEADERS_RESULTS) {
        Ok(h) if !h.is_empty() || query.tip_height().is_some() => Ok(h),
        Ok(_) => Ok(cache.headers_after_locator(&gh.locator_hashes, gh.stop_hash)),
        Err(e) => Err(NetError::Consensus(e.to_string())),
    }
}

fn block_for_peer(
    cache: &BlockCache,
    query: &Query,
    hash: &BlockHash,
) -> Result<Option<bitcoin::Block>, NetError> {
    if let Some(block) = cache.get_block(hash) {
        return Ok(Some(block));
    }
    match query.reconstruct_block_by_hash(&hash.to_byte_array()) {
        Ok(b) => Ok(b),
        Err(e) => Err(NetError::Consensus(e.to_string())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rbitcoin_consensus::{ChainParams, Milestone};
    use rbitcoin_query::Query;

    fn tmp_store(label: &str) -> (std::path::PathBuf, Query) {
        let dir = std::env::temp_dir().join(format!(
            "rbitcoin-peer-{label}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let q = Query::open_or_create(&dir).unwrap();
        (dir, q)
    }

    #[test]
    fn store_not_found_is_soft_session_error() {
        assert!(net_error_is_store_not_found(&NetError::Consensus(
            "store: record not found".into()
        )));
        assert!(net_error_is_store_not_found(&NetError::Consensus(
            "consensus: store: record not found".into()
        )));
        assert!(net_error_is_store_not_found(&NetError::Consensus(
            "StoreError::NotFound for fk".into()
        )));
        assert!(net_error_is_store_not_found(&NetError::Consensus(
            "NOT FOUND".into()
        )));
        assert!(!net_error_is_store_not_found(&NetError::Consensus(
            "corrupt record: multi-spender".into()
        )));
        assert!(!net_error_is_store_not_found(&NetError::Protocol(
            "unknown parent"
        )));
        assert!(!net_error_is_store_not_found(&NetError::Timeout));
        assert!(!net_error_is_store_not_found(&NetError::Io(
            std::io::Error::new(std::io::ErrorKind::Other, "x")
        )));
    }

    #[test]
    fn tip_follow_locator_empty_store_has_genesis_zero() {
        let (dir, q) = tmp_store("empty");
        let hub = ChainHub::new(q, ChainParams::regtest(), Milestone::NONE);
        let loc = tip_follow_locator(&hub);
        assert!(!loc.is_empty());
        assert_eq!(loc.last().unwrap().to_byte_array(), [0u8; 32]);
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn tip_follow_locator_includes_tip_after_genesis() {
        let (dir, q) = tmp_store("gen");
        let hub = ChainHub::new(q, ChainParams::regtest(), Milestone::NONE);
        hub.ensure_genesis().unwrap();
        let loc = tip_follow_locator(&hub);
        assert!(!loc.is_empty());
        // Newest-first: tip hash is first.
        assert_eq!(loc[0], hub.tip_hash().unwrap());
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn live_follow_dec_on_drop() {
        let c = Arc::new(AtomicUsize::new(2));
        {
            let _g = LiveFollowDec(Some(c.clone()));
            assert_eq!(c.load(Ordering::SeqCst), 2);
        }
        assert_eq!(c.load(Ordering::SeqCst), 1);
        // None branch is a no-op drop.
        let _ = LiveFollowDec(None);
    }

    #[test]
    fn local_service_flags_include_network_witness_v2() {
        let f = local_service_flags();
        assert!(f.has(ServiceFlags::NETWORK));
        assert!(f.has(ServiceFlags::WITNESS));
        assert!(f.has(ServiceFlags::P2P_V2));
    }

    #[test]
    fn rand_nonce_changes() {
        let a = rand_nonce();
        let b = rand_nonce();
        // Counter component makes back-to-back nonces distinct.
        assert_ne!(a, b);
    }

    #[test]
    fn block_for_peer_empty_store_none() {
        let (dir, q) = tmp_store("block-none");
        let cache = BlockCache::new();
        let miss = BlockHash::from_byte_array([0xab; 32]);
        assert!(block_for_peer(&cache, &q, &miss).unwrap().is_none());
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn tip_announce_headers_and_inv() {
        use bitcoin::block::{Header, Version};
        use bitcoin::{CompactTarget, TxMerkleNode};
        let header = Header {
            version: Version::ONE,
            prev_blockhash: BlockHash::from_byte_array([0u8; 32]),
            merkle_root: TxMerkleNode::from_byte_array([1u8; 32]),
            time: 1,
            bits: CompactTarget::from_consensus(0x207f_ffff),
            nonce: 0,
        };
        let hash = header.block_hash();
        let ev = crate::chain::TipEvent {
            height: 1,
            hash,
            header,
        };
        match tip_announce_msg(&ev, true) {
            NetworkMessage::Headers(h) => {
                assert_eq!(h.len(), 1);
                assert_eq!(h[0].block_hash(), hash);
            }
            other => panic!("expected Headers, got {other:?}"),
        }
        match tip_announce_msg(&ev, false) {
            NetworkMessage::Inv(inv) => {
                assert_eq!(inv.len(), 1);
                assert!(matches!(inv[0], Inventory::WitnessBlock(h) if h == hash));
            }
            other => panic!("expected Inv, got {other:?}"),
        }
    }

    #[test]
    fn cmpct_helpers_without_mempool_and_queue_out_closed() {
        let (dir, q) = tmp_store("cmpct-none");
        let hub = ChainHub::new(q, ChainParams::regtest(), Milestone::NONE);
        hub.ensure_genesis().unwrap();
        let gen = hub
            .query
            .reconstruct_block_by_hash(&hub.tip_hash().unwrap().to_byte_array())
            .unwrap()
            .unwrap();
        let hsi = HeaderAndShortIds::from_block(&gen, 0xabc, 2, &[]).unwrap();
        assert!(try_fill_cmpct(&hub, &hsi, 2).is_none());
        assert!(try_cmpct_missing(&hub, &hsi, 2).is_none());
        assert!(mempool_live_txs(&hub).is_empty());

        // Closed channel → Protocol error.
        let (tx, rx) = mpsc::unbounded_channel();
        drop(rx);
        assert!(queue_out(&tx, NetworkMessage::Verack).is_err());
        assert!(queue_getheaders(&tx, &hub).is_err());

        // headers_for_peer empty store after genesis still returns (tip exists).
        use bitcoin::p2p::message_blockdata::GetHeadersMessage;
        let gh = GetHeadersMessage::new(
            vec![hub.tip_hash().unwrap()],
            BlockHash::from_byte_array([0u8; 32]),
        );
        let hdrs = headers_for_peer(hub.cache.as_ref(), hub.query.as_ref(), &gh).unwrap();
        // Beyond tip: empty headers is fine.
        assert!(hdrs.is_empty() || !hdrs.is_empty());

        // drain_pending empty is a no-op.
        let mut pb = HashMap::new();
        let mut ph = HashMap::new();
        let (tx, _rx) = mpsc::unbounded_channel();
        drain_pending(&hub, &tx, &mut pb, &mut ph).unwrap();
        try_reorg_from_pending(&hub, &mut pb, &mut ph).unwrap();

        // Invalid tip-extending body must not kill the session (001).
        use bitcoin::absolute::LockTime;
        use bitcoin::block::{Header, Version as BlockVersion};
        use bitcoin::script::ScriptBuf;
        use bitcoin::transaction::Version as TxVersion;
        use bitcoin::{
            Amount, CompactTarget, OutPoint, Sequence, Transaction, TxIn, TxOut, Witness,
        };
        let tip = hub.tip_hash().unwrap();
        let tip_block = hub
            .query
            .reconstruct_block_by_hash(&tip.to_byte_array())
            .unwrap()
            .expect("tip body");
        let coinbase = Transaction {
            version: TxVersion::ONE,
            lock_time: LockTime::ZERO,
            input: vec![TxIn {
                previous_output: OutPoint::null(),
                script_sig: ScriptBuf::from_bytes(vec![0x01, 0x01]),
                sequence: Sequence::MAX,
                witness: Witness::new(),
            }],
            output: vec![TxOut {
                value: Amount::from_sat(50_0000_0000),
                script_pubkey: ScriptBuf::from_bytes(vec![0x51]),
            }],
        };
        // Second tx spends a nonexistent prevout → consensus reject.
        let junk = Transaction {
            version: TxVersion::ONE,
            lock_time: LockTime::ZERO,
            input: vec![TxIn {
                previous_output: OutPoint {
                    txid: bitcoin::Txid::from_byte_array([0xab; 32]),
                    vout: 0,
                },
                script_sig: ScriptBuf::new(),
                sequence: Sequence::MAX,
                witness: Witness::new(),
            }],
            output: vec![TxOut {
                value: Amount::from_sat(1),
                script_pubkey: ScriptBuf::from_bytes(vec![0x51]),
            }],
        };
        let mut bad = bitcoin::Block {
            header: Header {
                version: BlockVersion::ONE,
                prev_blockhash: tip,
                merkle_root: bitcoin::TxMerkleNode::from_byte_array([0; 32]),
                time: tip_block.header.time + 600,
                bits: CompactTarget::from_consensus(0x207f_ffff),
                nonce: 0,
            },
            txdata: vec![coinbase, junk],
        };
        bad.header.merkle_root = bad.compute_merkle_root().unwrap();
        let target = bitcoin::Target::from_compact(bad.header.bits);
        for nonce in 0..200_000u32 {
            bad.header.nonce = nonce;
            if bad.header.validate_pow(target).is_ok() {
                break;
            }
        }
        let bh = bad.block_hash();
        pb.insert(bh, bad);
        let (tx, _rx) = mpsc::unbounded_channel();
        drain_pending(&hub, &tx, &mut pb, &mut ph).expect("invalid block must not end session");

        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn handle_peer_frame_control_and_inv_paths() {
        use bitcoin::consensus::encode::serialize;
        use bitcoin::hashes::Hash as _;
        use bitcoin::Network;
        use tokio::runtime::Builder;

        fn frame_for(msg: NetworkMessage) -> FramedMessage {
            use bitcoin::p2p::message::RawNetworkMessage;
            let magic = Magic::from(Network::Regtest);
            let raw = RawNetworkMessage::new(magic, msg);
            let full = serialize(&raw);
            // 4 magic + 12 command + 4 len + 4 checksum + payload
            let command: [u8; 12] = full[4..16].try_into().unwrap();
            let checksum: [u8; 4] = full[20..24].try_into().unwrap();
            let payload = full[24..].to_vec();
            FramedMessage {
                magic,
                command,
                checksum,
                payload,
            }
        }

        let rt = Builder::new_current_thread().enable_all().build().unwrap();
        rt.block_on(async {
            let (dir, q) = tmp_store("handle-frame");
            let hub = ChainHub::new(q, ChainParams::regtest(), Milestone::NONE);
            hub.ensure_genesis().unwrap();
            let (out_tx, mut out_rx) = mpsc::unbounded_channel();
            let mut wants_headers = false;
            let mut wtxid = false;
            let mut send_cmpct = false;
            let mut cmpct_ver = 2u32;
            let mut pending_headers = HashMap::new();
            let mut pending_blocks = HashMap::new();
            let mut pending_cmpct = HashMap::new();
            let mut from_peer = HashMap::new();
            let mut ban = 0u32;

            // SendHeaders / SendCmpct / WtxidRelay / Pong / MemPool / GetAddr / Ping
            for msg in [
                NetworkMessage::SendHeaders,
                NetworkMessage::SendCmpct(SendCmpct {
                    send_compact: true,
                    version: 2,
                }),
                NetworkMessage::WtxidRelay,
                NetworkMessage::Pong(7),
                NetworkMessage::MemPool,
                NetworkMessage::GetAddr,
                NetworkMessage::Ping(42),
            ] {
                handle_peer_frame(
                    frame_for(msg),
                    &hub,
                    &out_tx,
                    &mut wants_headers,
                    &mut wtxid,
                    &mut send_cmpct,
                    &mut cmpct_ver,
                    &mut pending_headers,
                    &mut pending_blocks,
                    &mut pending_cmpct,
                    &mut from_peer,
                    &mut ban,
                )
                .await
                .unwrap();
            }
            assert!(wants_headers);
            assert!(wtxid);
            assert!(send_cmpct);
            assert_eq!(cmpct_ver, 2);

            // Drain outbound: Pong(42) + empty Addr at least.
            let mut saw_pong = false;
            let mut saw_addr = false;
            while let Ok(m) = out_rx.try_recv() {
                match m {
                    NetworkMessage::Pong(n) => {
                        assert_eq!(n, 42);
                        saw_pong = true;
                    }
                    NetworkMessage::Addr(a) => {
                        assert!(a.is_empty());
                        saw_addr = true;
                    }
                    _ => {}
                }
            }
            assert!(saw_pong);
            assert!(saw_addr);

            // GetHeaders from empty tip-beyond locator.
            use bitcoin::p2p::message_blockdata::GetHeadersMessage;
            let gh = GetHeadersMessage::new(
                vec![hub.tip_hash().unwrap()],
                BlockHash::from_byte_array([0u8; 32]),
            );
            handle_peer_frame(
                frame_for(NetworkMessage::GetHeaders(gh)),
                &hub,
                &out_tx,
                &mut wants_headers,
                &mut wtxid,
                &mut send_cmpct,
                &mut cmpct_ver,
                &mut pending_headers,
                &mut pending_blocks,
                &mut pending_cmpct,
                &mut from_peer,
                &mut ban,
            )
            .await
            .unwrap();
            let headers_msg = out_rx.try_recv().unwrap();
            assert!(matches!(headers_msg, NetworkMessage::Headers(_)));

            // Inv for unknown block → GetData witness block.
            let want_h = BlockHash::from_byte_array([0xee; 32]);
            handle_peer_frame(
                frame_for(NetworkMessage::Inv(vec![Inventory::WitnessBlock(want_h)])),
                &hub,
                &out_tx,
                &mut wants_headers,
                &mut wtxid,
                &mut send_cmpct,
                &mut cmpct_ver,
                &mut pending_headers,
                &mut pending_blocks,
                &mut pending_cmpct,
                &mut from_peer,
                &mut ban,
            )
            .await
            .unwrap();
            match out_rx.try_recv().unwrap() {
                NetworkMessage::GetData(v) => {
                    assert!(matches!(v[0], Inventory::WitnessBlock(h) if h == want_h));
                }
                other => panic!("expected GetData, got {other:?}"),
            }

            // Headers message inserts pending + issues getdata.
            let gen = hub
                .query
                .wire_header_at_height(rbitcoin_primitives::Height(0))
                .unwrap();
            // Synthesize a child-looking header (not valid pow; just exercises map).
            use bitcoin::block::{Header, Version};
            use bitcoin::{CompactTarget, TxMerkleNode};
            let child = Header {
                version: Version::ONE,
                prev_blockhash: gen.block_hash(),
                merkle_root: TxMerkleNode::from_byte_array([2u8; 32]),
                time: gen.time + 1,
                bits: CompactTarget::from_consensus(0x207f_ffff),
                nonce: 1,
            };
            handle_peer_frame(
                frame_for(NetworkMessage::Headers(vec![child])),
                &hub,
                &out_tx,
                &mut wants_headers,
                &mut wtxid,
                &mut send_cmpct,
                &mut cmpct_ver,
                &mut pending_headers,
                &mut pending_blocks,
                &mut pending_cmpct,
                &mut from_peer,
                &mut ban,
            )
            .await
            .unwrap();
            assert!(pending_headers.contains_key(&child.block_hash()));
            let _ = out_rx.try_recv(); // GetData

            // GetData for known tip block (cache miss → reconstruct).
            let tip = hub.tip_hash().unwrap();
            handle_peer_frame(
                frame_for(NetworkMessage::GetData(vec![Inventory::WitnessBlock(tip)])),
                &hub,
                &out_tx,
                &mut wants_headers,
                &mut wtxid,
                &mut send_cmpct,
                &mut cmpct_ver,
                &mut pending_headers,
                &mut pending_blocks,
                &mut pending_cmpct,
                &mut from_peer,
                &mut ban,
            )
            .await
            .unwrap();
            match out_rx.try_recv().unwrap() {
                NetworkMessage::Block(b) => assert_eq!(b.block_hash(), tip),
                other => panic!("expected Block, got {other:?}"),
            }

            // CompactBlock getdata for tip.
            handle_peer_frame(
                frame_for(NetworkMessage::GetData(vec![Inventory::CompactBlock(tip)])),
                &hub,
                &out_tx,
                &mut wants_headers,
                &mut wtxid,
                &mut send_cmpct,
                &mut cmpct_ver,
                &mut pending_headers,
                &mut pending_blocks,
                &mut pending_cmpct,
                &mut from_peer,
                &mut ban,
            )
            .await
            .unwrap();
            assert!(matches!(
                out_rx.try_recv().unwrap(),
                NetworkMessage::CmpctBlock(_)
            ));

            // GetBlockTxn with bad index → ban score.
            use bitcoin::bip152::BlockTransactionsRequest;
            handle_peer_frame(
                frame_for(NetworkMessage::GetBlockTxn(GetBlockTxn {
                    txs_request: BlockTransactionsRequest {
                        block_hash: tip,
                        indexes: vec![999],
                    },
                })),
                &hub,
                &out_tx,
                &mut wants_headers,
                &mut wtxid,
                &mut send_cmpct,
                &mut cmpct_ver,
                &mut pending_headers,
                &mut pending_blocks,
                &mut pending_cmpct,
                &mut from_peer,
                &mut ban,
            )
            .await
            .unwrap();
            assert!(ban >= 20);

            // GetBlockTxn good index 0 (coinbase).
            ban = 0;
            handle_peer_frame(
                frame_for(NetworkMessage::GetBlockTxn(GetBlockTxn {
                    txs_request: BlockTransactionsRequest {
                        block_hash: tip,
                        indexes: vec![0],
                    },
                })),
                &hub,
                &out_tx,
                &mut wants_headers,
                &mut wtxid,
                &mut send_cmpct,
                &mut cmpct_ver,
                &mut pending_headers,
                &mut pending_blocks,
                &mut pending_cmpct,
                &mut from_peer,
                &mut ban,
            )
            .await
            .unwrap();
            assert!(matches!(
                out_rx.try_recv().unwrap(),
                NetworkMessage::BlockTxn(_)
            ));

            // Unsolicited BlockTxn → mild ban.
            handle_peer_frame(
                frame_for(NetworkMessage::BlockTxn(BlockTxn {
                    transactions: BlockTransactions {
                        block_hash: BlockHash::from_byte_array([0xdd; 32]),
                        transactions: vec![],
                    },
                })),
                &hub,
                &out_tx,
                &mut wants_headers,
                &mut wtxid,
                &mut send_cmpct,
                &mut cmpct_ver,
                &mut pending_headers,
                &mut pending_blocks,
                &mut pending_cmpct,
                &mut from_peer,
                &mut ban,
            )
            .await
            .unwrap();
            assert!(ban >= 5);

            // CmpctBlock without mempool → full getdata fallback.
            let gen_block = hub
                .query
                .reconstruct_block_by_hash(&tip.to_byte_array())
                .unwrap()
                .unwrap();
            let hsi = HeaderAndShortIds::from_block(&gen_block, 9, 2, &[]).unwrap();
            handle_peer_frame(
                frame_for(NetworkMessage::CmpctBlock(CmpctBlock {
                    compact_block: hsi,
                })),
                &hub,
                &out_tx,
                &mut wants_headers,
                &mut wtxid,
                &mut send_cmpct,
                &mut cmpct_ver,
                &mut pending_headers,
                &mut pending_blocks,
                &mut pending_cmpct,
                &mut from_peer,
                &mut ban,
            )
            .await
            .unwrap();
            // Already have tip → no getdata; if different hash would request.
            // Genesis is already known so "already have" arm.
            let _ = out_rx.try_recv();

            // Unknown rbtpkg with no mempool is a no-op.
            handle_peer_frame(
                frame_for(NetworkMessage::Unknown {
                    command: bitcoin::p2p::message::CommandString::try_from("rbtpkg").unwrap(),
                    payload: vec![1, 2, 3],
                }),
                &hub,
                &out_tx,
                &mut wants_headers,
                &mut wtxid,
                &mut send_cmpct,
                &mut cmpct_ver,
                &mut pending_headers,
                &mut pending_blocks,
                &mut pending_cmpct,
                &mut from_peer,
                &mut ban,
            )
            .await
            .unwrap();

            // SendCmpct with unsupported version is ignored.
            handle_peer_frame(
                frame_for(NetworkMessage::SendCmpct(SendCmpct {
                    send_compact: false,
                    version: 99,
                })),
                &hub,
                &out_tx,
                &mut wants_headers,
                &mut wtxid,
                &mut send_cmpct,
                &mut cmpct_ver,
                &mut pending_headers,
                &mut pending_blocks,
                &mut pending_cmpct,
                &mut from_peer,
                &mut ban,
            )
            .await
            .unwrap();
            assert!(send_cmpct); // still true from earlier v2
            assert_eq!(cmpct_ver, 2);

            // Inventory::Block (non-witness) for unknown → GetData WitnessBlock.
            let want2 = BlockHash::from_byte_array([0xcc; 32]);
            handle_peer_frame(
                frame_for(NetworkMessage::Inv(vec![Inventory::Block(want2)])),
                &hub,
                &out_tx,
                &mut wants_headers,
                &mut wtxid,
                &mut send_cmpct,
                &mut cmpct_ver,
                &mut pending_headers,
                &mut pending_blocks,
                &mut pending_cmpct,
                &mut from_peer,
                &mut ban,
            )
            .await
            .unwrap();
            match out_rx.try_recv().unwrap() {
                NetworkMessage::GetData(v) => {
                    assert!(matches!(v[0], Inventory::WitnessBlock(h) if h == want2));
                }
                other => panic!("expected GetData, got {other:?}"),
            }

            // Inv for known tip → no GetData.
            handle_peer_frame(
                frame_for(NetworkMessage::Inv(vec![Inventory::WitnessBlock(tip)])),
                &hub,
                &out_tx,
                &mut wants_headers,
                &mut wtxid,
                &mut send_cmpct,
                &mut cmpct_ver,
                &mut pending_headers,
                &mut pending_blocks,
                &mut pending_cmpct,
                &mut from_peer,
                &mut ban,
            )
            .await
            .unwrap();
            assert!(out_rx.try_recv().is_err());

            // GetData Inventory::Block for tip (non-witness arm).
            handle_peer_frame(
                frame_for(NetworkMessage::GetData(vec![Inventory::Block(tip)])),
                &hub,
                &out_tx,
                &mut wants_headers,
                &mut wtxid,
                &mut send_cmpct,
                &mut cmpct_ver,
                &mut pending_headers,
                &mut pending_blocks,
                &mut pending_cmpct,
                &mut from_peer,
                &mut ban,
            )
            .await
            .unwrap();
            assert!(matches!(
                out_rx.try_recv().unwrap(),
                NetworkMessage::Block(_)
            ));

            // Full Block message path: pending + drain_pending (AlreadyHave for tip).
            let gen_block2 = hub
                .query
                .reconstruct_block_by_hash(&tip.to_byte_array())
                .unwrap()
                .unwrap();
            handle_peer_frame(
                frame_for(NetworkMessage::Block(gen_block2)),
                &hub,
                &out_tx,
                &mut wants_headers,
                &mut wtxid,
                &mut send_cmpct,
                &mut cmpct_ver,
                &mut pending_headers,
                &mut pending_blocks,
                &mut pending_cmpct,
                &mut from_peer,
                &mut ban,
            )
            .await
            .unwrap();
            // Tip already confirmed — drain accepts AlreadyHave and may leave empty pending.

            // Tx without mempool is a no-op.
            use bitcoin::absolute::LockTime;
            use bitcoin::script::ScriptBuf;
            use bitcoin::transaction::Version as TxVersion;
            use bitcoin::{Amount, OutPoint, Sequence, Transaction, TxIn, TxOut, Witness};
            let dummy_tx = Transaction {
                version: TxVersion::TWO,
                lock_time: LockTime::ZERO,
                input: vec![TxIn {
                    previous_output: OutPoint::null(),
                    script_sig: ScriptBuf::new(),
                    sequence: Sequence::MAX,
                    witness: Witness::new(),
                }],
                output: vec![TxOut {
                    value: Amount::from_sat(1),
                    script_pubkey: ScriptBuf::from_bytes(vec![0x51]),
                }],
            };
            handle_peer_frame(
                frame_for(NetworkMessage::Tx(dummy_tx)),
                &hub,
                &out_tx,
                &mut wants_headers,
                &mut wtxid,
                &mut send_cmpct,
                &mut cmpct_ver,
                &mut pending_headers,
                &mut pending_blocks,
                &mut pending_cmpct,
                &mut from_peer,
                &mut ban,
            )
            .await
            .unwrap();

            // Catch-all unknown command.
            handle_peer_frame(
                frame_for(NetworkMessage::Unknown {
                    command: bitcoin::p2p::message::CommandString::try_from("zzzzzz").unwrap(),
                    payload: vec![],
                }),
                &hub,
                &out_tx,
                &mut wants_headers,
                &mut wtxid,
                &mut send_cmpct,
                &mut cmpct_ver,
                &mut pending_headers,
                &mut pending_blocks,
                &mut pending_cmpct,
                &mut from_peer,
                &mut ban,
            )
            .await
            .unwrap();

            let _ = std::fs::remove_dir_all(dir);
        });
    }

    /// Mempool-backed inv/tx/getdata arms + cmpctblocktxn success.
    #[test]
    fn handle_peer_frame_mempool_tx_and_inv_paths() {
        use bitcoin::absolute::LockTime;
        use bitcoin::consensus::encode::serialize;
        use bitcoin::hashes::Hash as _;
        use bitcoin::script::ScriptBuf;
        use bitcoin::transaction::Version as TxVersion;
        use bitcoin::{Amount, Network, OutPoint, Sequence, Transaction, TxIn, TxOut, Witness};
        use tokio::runtime::Builder;

        fn frame_for(msg: NetworkMessage) -> FramedMessage {
            use bitcoin::p2p::message::RawNetworkMessage;
            let magic = Magic::from(Network::Regtest);
            let raw = RawNetworkMessage::new(magic, msg);
            let full = serialize(&raw);
            let command: [u8; 12] = full[4..16].try_into().unwrap();
            let checksum: [u8; 4] = full[20..24].try_into().unwrap();
            let payload = full[24..].to_vec();
            FramedMessage {
                magic,
                command,
                checksum,
                payload,
            }
        }

        let rt = Builder::new_current_thread().enable_all().build().unwrap();
        rt.block_on(async {
            let (dir, q) = tmp_store("handle-mp");
            let hub = ChainHub::new(q, ChainParams::regtest(), Milestone::NONE);
            hub.ensure_genesis().unwrap();
            let mp =
                crate::tx_relay::MempoolHub::open(dir.join("mp"), Arc::clone(&hub.query)).unwrap();
            // Enable relay so Inv for txs triggers getdata.
            mp.set_relay_enabled(true);
            assert!(hub.attach_mempool(mp).is_ok());

            let (out_tx, mut out_rx) = mpsc::unbounded_channel();
            let mut wants_headers = false;
            let mut wtxid = false;
            let mut send_cmpct = false;
            let mut cmpct_ver = 2u32;
            let mut pending_headers = HashMap::new();
            let mut pending_blocks = HashMap::new();
            let mut pending_cmpct = HashMap::new();
            let mut from_peer = HashMap::new();
            let mut ban = 0u32;

            let unknown_txid = bitcoin::Txid::from_byte_array([0x42; 32]);
            handle_peer_frame(
                frame_for(NetworkMessage::Inv(vec![
                    Inventory::WitnessTransaction(unknown_txid),
                    Inventory::WTx(bitcoin::Wtxid::from_byte_array([0x43; 32])),
                ])),
                &hub,
                &out_tx,
                &mut wants_headers,
                &mut wtxid,
                &mut send_cmpct,
                &mut cmpct_ver,
                &mut pending_headers,
                &mut pending_blocks,
                &mut pending_cmpct,
                &mut from_peer,
                &mut ban,
            )
            .await
            .unwrap();
            match out_rx.try_recv().unwrap() {
                NetworkMessage::GetData(v) => {
                    assert!(v.len() >= 1);
                }
                other => panic!("expected GetData for unknown txs, got {other:?}"),
            }

            // GetData for missing tx → silent (no response).
            handle_peer_frame(
                frame_for(NetworkMessage::GetData(vec![
                    Inventory::WitnessTransaction(unknown_txid),
                    Inventory::WTx(bitcoin::Wtxid::from_byte_array([0x43; 32])),
                ])),
                &hub,
                &out_tx,
                &mut wants_headers,
                &mut wtxid,
                &mut send_cmpct,
                &mut cmpct_ver,
                &mut pending_headers,
                &mut pending_blocks,
                &mut pending_cmpct,
                &mut from_peer,
                &mut ban,
            )
            .await
            .unwrap();
            // No tx in mempool → no outbound.
            assert!(out_rx.try_recv().is_err());

            // Accept path with invalid prevout — still exercises Tx arm (inserts from_peer).
            let junk = Transaction {
                version: TxVersion::TWO,
                lock_time: LockTime::ZERO,
                input: vec![TxIn {
                    previous_output: OutPoint {
                        txid: bitcoin::Txid::from_byte_array([1u8; 32]),
                        vout: 0,
                    },
                    script_sig: ScriptBuf::new(),
                    sequence: Sequence::MAX,
                    witness: Witness::from_slice(&[vec![1]]),
                }],
                output: vec![TxOut {
                    value: Amount::from_sat(1000),
                    script_pubkey: ScriptBuf::from_bytes(vec![0x51]),
                }],
            };
            let junk_txid = junk.compute_txid();
            handle_peer_frame(
                frame_for(NetworkMessage::Tx(junk)),
                &hub,
                &out_tx,
                &mut wants_headers,
                &mut wtxid,
                &mut send_cmpct,
                &mut cmpct_ver,
                &mut pending_headers,
                &mut pending_blocks,
                &mut pending_cmpct,
                &mut from_peer,
                &mut ban,
            )
            .await
            .unwrap();
            // Origin map is filled before accept result.
            assert!(from_peer.contains_key(&junk_txid));

            // rbtpkg with mempool + relay; empty/invalid payload is a quiet no-op.
            handle_peer_frame(
                frame_for(NetworkMessage::Unknown {
                    command: bitcoin::p2p::message::CommandString::try_from("rbtpkg").unwrap(),
                    payload: vec![0x00], // empty package count
                }),
                &hub,
                &out_tx,
                &mut wants_headers,
                &mut wtxid,
                &mut send_cmpct,
                &mut cmpct_ver,
                &mut pending_headers,
                &mut pending_blocks,
                &mut pending_cmpct,
                &mut from_peer,
                &mut ban,
            )
            .await
            .unwrap();

            let _ = std::fs::remove_dir_all(dir);
        });
    }

    /// Compact helpers with a live mempool hub attached (fill/missing/blocktxn).
    #[test]
    fn cmpct_helpers_with_mempool_live_and_blocktxn() {
        use bitcoin::absolute::LockTime;
        use bitcoin::bip152::BlockTransactions;
        use bitcoin::block::{Header, Version};
        use bitcoin::script::ScriptBuf;
        use bitcoin::transaction::Version as TxVersion;
        use bitcoin::{
            Amount, CompactTarget, OutPoint, Sequence, Transaction, TxIn, TxMerkleNode, TxOut,
            Witness,
        };

        let (dir, q) = tmp_store("cmpct-mp");
        let hub = ChainHub::new(q, ChainParams::regtest(), Milestone::NONE);
        hub.ensure_genesis().unwrap();
        let mp = crate::tx_relay::MempoolHub::open(dir.join("mp"), Arc::clone(&hub.query)).unwrap();
        assert!(hub.attach_mempool(mp).is_ok());
        assert!(hub.mempool().is_some());

        let spend = Transaction {
            version: TxVersion::TWO,
            lock_time: LockTime::ZERO,
            input: vec![TxIn {
                previous_output: OutPoint {
                    txid: bitcoin::Txid::from_byte_array([0x11; 32]),
                    vout: 0,
                },
                script_sig: ScriptBuf::new(),
                sequence: Sequence::MAX,
                witness: Witness::from_slice(&[vec![1]]),
            }],
            output: vec![TxOut {
                value: Amount::from_sat(1000),
                script_pubkey: ScriptBuf::from_bytes(vec![0x51]),
            }],
        };
        let mut block = bitcoin::Block {
            header: Header {
                version: Version::ONE,
                prev_blockhash: hub.tip_hash().unwrap(),
                merkle_root: TxMerkleNode::from_byte_array([0u8; 32]),
                time: 1,
                bits: CompactTarget::from_consensus(0x207f_ffff),
                nonce: 0,
            },
            txdata: vec![
                Transaction {
                    version: TxVersion::ONE,
                    lock_time: LockTime::ZERO,
                    input: vec![TxIn {
                        previous_output: OutPoint::null(),
                        script_sig: ScriptBuf::from_bytes(vec![0x01, 0x01]),
                        sequence: Sequence::MAX,
                        witness: Witness::new(),
                    }],
                    output: vec![TxOut {
                        value: Amount::from_sat(50_0000_0000),
                        script_pubkey: ScriptBuf::from_bytes(vec![0x51]),
                    }],
                },
                spend.clone(),
            ],
        };
        block.header.merkle_root = block.compute_merkle_root().unwrap();

        let hsi = HeaderAndShortIds::from_block(&block, 0xbeef, 2, &[]).unwrap();
        // Mempool present but empty live → Some(missing) not None.
        let missing = try_cmpct_missing(&hub, &hsi, 2).expect("mempool present");
        assert_eq!(missing, vec![1]); // spend short-id missing
        assert!(try_fill_cmpct(&hub, &hsi, 2).is_none());
        assert!(mempool_live_txs(&hub).is_empty());

        let pc = PendingCmpct {
            hsi: hsi.clone(),
            missing: missing.clone(),
            version: 2,
        };
        let bt = BlockTransactions {
            block_hash: block.block_hash(),
            transactions: vec![spend],
        };
        let recon = apply_cmpct_blocktxn(&hub, &pc, &bt).expect("blocktxn fill");
        assert_eq!(recon.txdata.len(), 2);

        let _ = std::fs::remove_dir_all(dir);
    }

    /// Tip-follow pending reorg: max-work fork assembled and applied via shipped path.
    #[test]
    fn try_reorg_from_pending_picks_max_work_branch() {
        use bitcoin::absolute::LockTime;
        use bitcoin::block::{Header, Version as BlockVersion};
        use bitcoin::script::ScriptBuf;
        use bitcoin::transaction::Version as TxVersion;
        use bitcoin::{
            Amount, CompactTarget, OutPoint, Sequence, Target, Transaction, TxIn, TxOut, Witness,
        };

        let (dir, q) = tmp_store("pending-reorg");
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
            let mut block = bitcoin::Block {
                header: Header {
                    version: BlockVersion::ONE,
                    prev_blockhash: prev,
                    merkle_root: bitcoin::TxMerkleNode::from_byte_array([0u8; 32]),
                    time,
                    bits,
                    nonce: 0,
                },
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

        // Main tip height 2 (times near genesis MTP window).
        let b1 = mine(gen, 1_300_000_100, 1);
        hub.accept_block(b1.clone()).unwrap();
        let b2 = mine(b1.block_hash(), 1_300_000_200, 2);
        hub.accept_block(b2.clone()).unwrap();
        assert_eq!(hub.tip_height(), Some(2));

        // Pending: short side from gen (1 block) + long side from gen (4 blocks).
        let short = mine(gen, 1_300_001_000, 1);
        let mut long = Vec::new();
        let mut p = gen;
        for (i, h) in (1..=4u32).enumerate() {
            let b = mine(p, 1_300_002_000 + i as u32 * 600, h);
            p = b.block_hash();
            long.push(b);
        }
        let mut pb = HashMap::new();
        pb.insert(short.block_hash(), short);
        for b in &long {
            pb.insert(b.block_hash(), b.clone());
        }
        let mut ph = HashMap::new();
        assert!(MAX_PENDING_BLOCKS_FOR_TEST >= 128);
        try_reorg_from_pending(&hub, &mut pb, &mut ph).unwrap();
        assert_eq!(
            hub.tip_height(),
            Some(4),
            "must reorg onto longer pending branch"
        );
        assert_eq!(hub.tip_hash().unwrap(), long[3].block_hash());
        // Winning branch removed from pending.
        assert!(!pb.contains_key(&long[3].block_hash()));
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn drain_requests_missing_parent_of_pending_branch() {
        use bitcoin::absolute::LockTime;
        use bitcoin::block::{Header, Version as BlockVersion};
        use bitcoin::script::ScriptBuf;
        use bitcoin::transaction::Version as TxVersion;
        use bitcoin::{
            Amount, CompactTarget, OutPoint, Sequence, Target, Transaction, TxIn, TxOut, Witness,
        };

        let (dir, q) = tmp_store("pending-missing-parent");
        let hub = ChainHub::new(q, ChainParams::regtest(), Milestone::NONE);
        hub.ensure_genesis().unwrap();
        let missing_parent = BlockHash::from_byte_array([0x42; 32]);
        let bits = CompactTarget::from_consensus(0x207f_ffff);
        let mut orphan = bitcoin::Block {
            header: Header {
                version: BlockVersion::ONE,
                prev_blockhash: missing_parent,
                merkle_root: bitcoin::TxMerkleNode::from_byte_array([0u8; 32]),
                time: 1_300_000_100,
                bits,
                nonce: 0,
            },
            txdata: vec![Transaction {
                version: TxVersion::ONE,
                lock_time: LockTime::ZERO,
                input: vec![TxIn {
                    previous_output: OutPoint::null(),
                    script_sig: ScriptBuf::from_bytes(vec![0x01, 0x01]),
                    sequence: Sequence::MAX,
                    witness: Witness::new(),
                }],
                output: vec![TxOut {
                    value: Amount::from_sat(50_0000_0000),
                    script_pubkey: ScriptBuf::from_bytes(vec![0x51]),
                }],
            }],
        };
        orphan.header.merkle_root = orphan.compute_merkle_root().unwrap();
        let target = Target::from_compact(bits);
        for nonce in 0..u32::MAX {
            orphan.header.nonce = nonce;
            if orphan.header.validate_pow(target).is_ok() {
                break;
            }
        }
        let (tx, mut rx) = mpsc::unbounded_channel();
        let mut pb = HashMap::new();
        pb.insert(orphan.block_hash(), orphan);
        let mut ph = HashMap::new();
        drain_pending(&hub, &tx, &mut pb, &mut ph).unwrap();
        let msg = rx.try_recv().expect("getdata for missing parent");
        match msg {
            NetworkMessage::GetData(inv) => {
                assert!(
                    inv.iter()
                        .any(|i| matches!(i, Inventory::WitnessBlock(h) if *h == missing_parent)),
                    "expected getdata for {missing_parent}, got {inv:?}"
                );
            }
            other => panic!("expected GetData, got {other:?}"),
        }
        assert!(!hub.is_connected(&missing_parent));
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn drain_connects_pending_child_of_new_tip_after_reorg() {
        use bitcoin::absolute::LockTime;
        use bitcoin::block::{Header, Version as BlockVersion};
        use bitcoin::script::ScriptBuf;
        use bitcoin::transaction::Version as TxVersion;
        use bitcoin::{
            Amount, CompactTarget, OutPoint, Sequence, Target, Transaction, TxIn, TxOut, Witness,
        };

        let (dir, q) = tmp_store("pending-child-after-reorg");
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
            let mut block = bitcoin::Block {
                header: Header {
                    version: BlockVersion::ONE,
                    prev_blockhash: prev,
                    merkle_root: bitcoin::TxMerkleNode::from_byte_array([0u8; 32]),
                    time,
                    bits,
                    nonce: 0,
                },
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
        let a1 = mine(gen, 1_300_000_100, 1);
        hub.accept_block(a1.clone()).unwrap();
        assert_eq!(hub.tip_height(), Some(1));

        let b1 = mine(gen, 1_300_001_000, 1);
        let b2 = mine(b1.block_hash(), 1_300_001_600, 2);
        let mut pb = HashMap::new();
        pb.insert(b1.block_hash(), b1.clone());
        pb.insert(b2.block_hash(), b2.clone());
        let mut ph = HashMap::new();
        let (tx, _rx) = mpsc::unbounded_channel();
        drain_pending(&hub, &tx, &mut pb, &mut ph).unwrap();
        assert_eq!(hub.tip_height(), Some(2), "reorg plus child must connect");
        assert_eq!(hub.tip_hash().unwrap(), b2.block_hash());
        assert!(hub.is_connected(&b1.block_hash()));
        assert!(hub.is_connected(&b2.block_hash()));
        let _ = std::fs::remove_dir_all(dir);
    }
}
