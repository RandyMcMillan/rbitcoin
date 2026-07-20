//! Peer handshake, serve, tip follow, and announce (BIP324 v2 transport).

use crate::cache::BlockCache;
use crate::chain::{AcceptOutcome, ChainHub};
use crate::codec::{FramedMessage, MAX_HEADERS_RESULTS, MAX_INV_SIZE};
use crate::error::NetError;
use crate::msg_decode::decode_framed_offload;
use crate::v2::{
    open_v2, read_v2_frame, write_v2_msg, write_v2_msg_offload, V2Reader, V2Writer,
};
use bitcoin::hashes::Hash;
use bitcoin::p2p::address::Address;
use bitcoin::p2p::message::NetworkMessage;
use bitcoin::p2p::message_blockdata::{GetHeadersMessage, Inventory};
use bitcoin::p2p::message_compact_blocks::SendCmpct;
use bitcoin::p2p::message_network::VersionMessage;
use bitcoin::p2p::{Magic, ServiceFlags, PROTOCOL_VERSION};
use bitcoin::BlockHash;
use rbitcoin_query::Query;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::net::TcpStream;
use tokio::sync::{broadcast, mpsc};

/// Services we advertise once store-backed reconstruct serve is available.
pub fn local_service_flags() -> ServiceFlags {
    ServiceFlags::NETWORK | ServiceFlags::WITNESS | ServiceFlags::P2P_V2
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
    let their_version =
        application_handshake(&mut reader, &mut writer, magic, our_addr, their_addr, start_height, inbound)
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
        version: PROTOCOL_VERSION,
        services,
        timestamp: now,
        receiver: Address::new(&their_addr, ServiceFlags::NONE),
        sender: Address::new(&our_addr, services),
        nonce: rand_nonce(),
        user_agent: "/rbitcoin:0.1.0/".to_string(),
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
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash as _, Hasher};
    let mut h = DefaultHasher::new();
    SystemTime::now().hash(&mut h);
    h.finish()
}

/// Bidirectional peer session: serve history, follow tip, announce our tip.
///
/// `catch_up`: if true (outbound), run getheaders/getdata first. Inbound peers must
/// pass false to avoid deadlock (both sides waiting on each other's getheaders).
///
/// After the sequential handshake / optional catch-up, a dedicated writer drains
/// outbound messages while the reader keeps draining the encrypted channel.
pub async fn peer_session(
    mut reader: V2Reader,
    mut writer: V2Writer,
    magic: Magic,
    hub: Arc<ChainHub>,
    mut tip_rx: broadcast::Receiver<crate::chain::TipEvent>,
    catch_up: bool,
) -> Result<(), NetError> {
    // Prefer headers announcements from peer; we do not send compact by default
    // (no mempool short-ids). Advertise we understand cmpct v1/v2 but request full blocks.
    let _ = write_v2_msg(&mut writer, NetworkMessage::SendHeaders).await;
    let _ = write_v2_msg(
        &mut writer,
        NetworkMessage::SendCmpct(SendCmpct {
            send_compact: false,
            version: 2,
        }),
    )
    .await;
    // Prefer wtxid inventory for txs when peers support it (BIP339).
    let _ = write_v2_msg(&mut writer, NetworkMessage::WtxidRelay).await;

    if catch_up {
        initial_sync(&mut reader, &mut writer, magic, hub.as_ref()).await?;
    }

    let (out_tx, mut out_rx) = mpsc::unbounded_channel::<NetworkMessage>();

    let writer_task = tokio::spawn(async move {
        while let Some(msg) = out_rx.recv().await {
            if write_v2_msg_offload(&mut writer, msg).await.is_err() {
                break;
            }
        }
    });

    let mut peer_wants_headers = false;
    // Headers received while assembling a potential reorg branch (hash → header).
    let mut pending_headers: HashMap<BlockHash, bitcoin::block::Header> = HashMap::new();
    // Blocks waiting for in-order accept (hash → block).
    let mut pending_blocks: HashMap<BlockHash, bitcoin::Block> = HashMap::new();
    // Txids we received from this peer (origin exclusion for announce).
    let mut from_this_peer: HashMap<bitcoin::Txid, ()> = HashMap::new();
    let mut tx_announce_rx = hub.mempool().map(|m| m.subscribe_announces());

    let result = async {
        loop {
            tokio::select! {
                biased;
                tip = tip_rx.recv() => {
                    match tip {
                        Ok(ev) => {
                            queue_out(&out_tx, tip_announce_msg(&ev, peer_wants_headers))?;
                        }
                        Err(broadcast::error::RecvError::Lagged(_)) => continue,
                        Err(broadcast::error::RecvError::Closed) => return Ok(()),
                    }
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
                            Ok(txid) => {
                                // Origin exclusion: do not re-announce to the peer that sent it.
                                if from_this_peer.contains_key(&txid) {
                                    continue;
                                }
                                if hub
                                    .mempool()
                                    .map(|m| m.relay_enabled() && m.contains(&txid))
                                    .unwrap_or(false)
                                {
                                    queue_out(
                                        &out_tx,
                                        NetworkMessage::Inv(vec![Inventory::Transaction(txid)]),
                                    )?;
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
                        Err(e) => return Err(e),
                    };
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
                        &mut pending_headers,
                        &mut pending_blocks,
                        &mut from_this_peer,
                    )
                    .await?;
                }
            }
        }
    }
    .await;

    drop(out_tx);
    writer_task.abort();
    let _ = writer_task.await;
    result
}

async fn handle_peer_frame(
    frame: FramedMessage,
    hub: &ChainHub,
    out_tx: &mpsc::UnboundedSender<NetworkMessage>,
    peer_wants_headers: &mut bool,
    pending_headers: &mut HashMap<BlockHash, bitcoin::block::Header>,
    pending_blocks: &mut HashMap<BlockHash, bitcoin::Block>,
    from_this_peer: &mut HashMap<bitcoin::Txid, ()>,
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
        NetworkMessage::SendCmpct(_) => {
            // Peer may send us compact blocks; we always fall back to getdata.
        }
        NetworkMessage::WtxidRelay => {
            // We already advertise wtxidrelay; acknowledge by no-op.
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
                    Inventory::Transaction(txid)
                    | Inventory::WitnessTransaction(txid) => {
                        if let Some(mp) = hub.mempool() {
                            if let Some(tx) = mp.get_tx(txid) {
                                queue_out(out_tx, NetworkMessage::Tx(tx))?;
                            }
                        }
                    }
                    Inventory::WTx(_wtxid) => {
                        // WTxid index lands with full wtxidrelay; peers can use txid inv.
                    }
                    _ => {}
                }
            }
        }
        NetworkMessage::Inv(items) => {
            let mut want = Vec::new();
            let relay = hub.mempool().map(|m| m.relay_enabled()).unwrap_or(false);
            for item in items.iter().take(MAX_INV_SIZE) {
                match item {
                    Inventory::Block(h) | Inventory::WitnessBlock(h) => {
                        if !hub.has_block(h) {
                            want.push(Inventory::WitnessBlock(*h));
                        }
                    }
                    Inventory::Transaction(txid)
                    | Inventory::WitnessTransaction(txid) => {
                        if relay {
                            if let Some(mp) = hub.mempool() {
                                if !mp.contains(txid) {
                                    want.push(Inventory::WitnessTransaction(*txid));
                                }
                            }
                        }
                    }
                    _ => {}
                }
            }
            if !want.is_empty() {
                queue_out(out_tx, NetworkMessage::GetData(want))?;
            }
        }
        NetworkMessage::Headers(headers) => {
            let mut want = Vec::new();
            for hdr in headers.iter().take(MAX_HEADERS_RESULTS) {
                let hash = hdr.block_hash();
                pending_headers.insert(hash, *hdr);
                if !hub.has_block(&hash) {
                    want.push(Inventory::WitnessBlock(hash));
                }
            }
            if !want.is_empty() {
                queue_out(out_tx, NetworkMessage::GetData(want))?;
            }
        }
        NetworkMessage::Block(block) => {
            pending_blocks.insert(block.block_hash(), block.clone());
            drain_pending(hub, pending_blocks, pending_headers)?;
        }
        NetworkMessage::CmpctBlock(cb) => {
            // No mempool short-id reconstruction — request full witness block.
            let hash = cb.compact_block.header.block_hash();
            if !hub.has_block(&hash) {
                queue_out(
                    out_tx,
                    NetworkMessage::GetData(vec![Inventory::WitnessBlock(hash)]),
                )?;
            }
        }
        NetworkMessage::Tx(tx) => {
            if let Some(mp) = hub.mempool() {
                if mp.relay_enabled() {
                    let txid = tx.compute_txid();
                    from_this_peer.insert(txid, ());
                    match mp.accept_tx(tx) {
                        Ok(_) => {}
                        Err(rbitcoin_mempool::AcceptError::Duplicate(_)) => {}
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
                    Err(e) => return Err(e),
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
        if hub.has_block(&prev) || prev.to_byte_array() == [0u8; 32] {
            // parent known
            if hub.tip_hash() != Some(prev) {
                fork_starts.push(*h);
            }
        }
    }
    for start in fork_starts {
        let mut branch = Vec::new();
        let mut cur = start;
        loop {
            let Some(b) = pending_blocks.get(&cur) else {
                break;
            };
            branch.push(b.clone());
            // find child in pending
            let next = pending_blocks
                .iter()
                .find(|(_, blk)| blk.header.prev_blockhash == cur)
                .map(|(h, _)| *h);
            match next {
                Some(n) => cur = n,
                None => break,
            }
        }
        if branch.is_empty() {
            continue;
        }
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

async fn initial_sync(
    reader: &mut V2Reader,
    writer: &mut V2Writer,
    magic: Magic,
    hub: &ChainHub,
) -> Result<(), NetError> {
    let locator = hub
        .query
        .locator_hashes()
        .map_err(|e| NetError::Consensus(e.to_string()))?;
    let locator = if locator.len() == 1
        && locator[0].to_byte_array() == [0u8; 32]
        && !hub.cache.is_empty()
    {
        hub.cache.locator()
    } else {
        locator
    };
    sync_from_peer(
        reader,
        writer,
        magic,
        locator,
        |hash| hub.has_block(hash),
        |_, block| hub.accept_block(block).map(|_| ()),
    )
    .await?;
    Ok(())
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

/// Outbound sync: getheaders until caught up, then getdata for missing blocks in order.
pub async fn sync_from_peer(
    reader: &mut V2Reader,
    writer: &mut V2Writer,
    magic: Magic,
    local_locator: Vec<BlockHash>,
    mut local_has: impl FnMut(&BlockHash) -> bool,
    mut on_block: impl FnMut(u32, bitcoin::Block) -> Result<(), NetError>,
) -> Result<u32, NetError> {
    let mut downloaded = 0u32;
    let mut locator = local_locator;
    loop {
        let gh = GetHeadersMessage::new(locator.clone(), BlockHash::from_byte_array([0u8; 32]));
        write_v2_msg(writer, NetworkMessage::GetHeaders(gh)).await?;

        let headers = loop {
            let frame = read_v2_frame(reader, magic).await?;
            if frame.is_ping() {
                if let Some(n) = frame.ping_nonce() {
                    write_v2_msg(writer, NetworkMessage::Pong(n)).await?;
                }
                continue;
            }
            let msg = decode_framed_offload(frame).await?;
            match msg.payload() {
                NetworkMessage::Headers(h) => break h.clone(),
                NetworkMessage::Ping(n) => {
                    write_v2_msg(writer, NetworkMessage::Pong(*n)).await?;
                }
                NetworkMessage::SendHeaders
                | NetworkMessage::SendCmpct(_)
                | NetworkMessage::Verack => {}
                _ => {}
            }
        };

        if headers.is_empty() {
            break;
        }

        let mut inv = Vec::new();
        for hdr in &headers {
            let hash = hdr.block_hash();
            if !local_has(&hash) {
                inv.push(Inventory::WitnessBlock(hash));
            }
        }
        if inv.is_empty() {
            if headers.len() < MAX_HEADERS_RESULTS {
                break;
            }
            locator = vec![headers.last().unwrap().block_hash()];
            continue;
        }

        // Honour Core MAX_INV_SZ when requesting (chunk if needed).
        for chunk in inv.chunks(MAX_INV_SIZE) {
            write_v2_msg(writer, NetworkMessage::GetData(chunk.to_vec())).await?;

            let need = chunk.len();
            let mut got = 0;
            while got < need {
                let frame = read_v2_frame(reader, magic).await?;
                if frame.is_ping() {
                    if let Some(n) = frame.ping_nonce() {
                        write_v2_msg(writer, NetworkMessage::Pong(n)).await?;
                    }
                    continue;
                }
                let msg = decode_framed_offload(frame).await?;
                match msg.payload() {
                    NetworkMessage::Block(block) => {
                        on_block(0, block.clone())?;
                        got += 1;
                        downloaded += 1;
                    }
                    NetworkMessage::Ping(n) => {
                        write_v2_msg(writer, NetworkMessage::Pong(*n)).await?;
                    }
                    NetworkMessage::NotFound(_) => {
                        return Err(NetError::Protocol("block not found"));
                    }
                    NetworkMessage::SendHeaders | NetworkMessage::SendCmpct(_) => {}
                    _ => {}
                }
            }
        }

        locator = vec![headers.last().unwrap().block_hash()];
        if headers.len() < MAX_HEADERS_RESULTS {
            break;
        }
    }
    Ok(downloaded)
}
