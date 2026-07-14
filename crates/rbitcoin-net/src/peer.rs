//! Peer handshake, serve, tip follow, and announce.

use crate::cache::BlockCache;
use crate::chain::{AcceptOutcome, ChainHub};
use crate::codec::{read_msg, write_msg};
use crate::error::NetError;
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
use tokio::sync::broadcast;

/// Services we advertise once store-backed reconstruct serve is available.
pub fn local_service_flags() -> ServiceFlags {
    ServiceFlags::NETWORK | ServiceFlags::WITNESS
}

pub async fn handshake(
    stream: &mut TcpStream,
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
        relay: false, // no tx relay
    };

    if !inbound {
        write_msg(stream, magic, NetworkMessage::Version(version.clone())).await?;
    }

    let their_version = loop {
        let msg = read_msg(stream).await?;
        if msg.magic() != &magic {
            return Err(NetError::BadMagic);
        }
        match msg.payload() {
            NetworkMessage::Version(v) => break v.clone(),
            other => {
                if matches!(other, NetworkMessage::Verack) {
                    return Err(NetError::Protocol("verack before version"));
                }
            }
        }
    };

    if inbound {
        write_msg(stream, magic, NetworkMessage::Version(version)).await?;
    }
    write_msg(stream, magic, NetworkMessage::Verack).await?;

    loop {
        let msg = read_msg(stream).await?;
        if msg.magic() != &magic {
            return Err(NetError::BadMagic);
        }
        match msg.payload() {
            NetworkMessage::Verack => break,
            NetworkMessage::Ping(n) => {
                write_msg(stream, magic, NetworkMessage::Pong(*n)).await?;
            }
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
pub async fn peer_session(
    mut stream: TcpStream,
    magic: Magic,
    hub: Arc<ChainHub>,
    mut tip_rx: broadcast::Receiver<crate::chain::TipEvent>,
    catch_up: bool,
) -> Result<(), NetError> {
    // Prefer headers announcements from peer; we do not send compact by default
    // (no mempool short-ids). Advertise we understand cmpct v1/v2 but request full blocks.
    let _ = write_msg(&mut stream, magic, NetworkMessage::SendHeaders).await;
    let _ = write_msg(
        &mut stream,
        magic,
        NetworkMessage::SendCmpct(SendCmpct {
            send_compact: false,
            version: 2,
        }),
    )
    .await;

    if catch_up {
        initial_sync(&mut stream, magic, hub.as_ref()).await?;
    }

    let mut peer_wants_headers = false;
    // Headers received while assembling a potential reorg branch (hash → header).
    let mut pending_headers: HashMap<BlockHash, bitcoin::block::Header> = HashMap::new();
    // Blocks waiting for in-order accept (hash → block).
    let mut pending_blocks: HashMap<BlockHash, bitcoin::Block> = HashMap::new();

    loop {
        tokio::select! {
            biased;
            tip = tip_rx.recv() => {
                match tip {
                    Ok(ev) => {
                        announce_tip(&mut stream, magic, &ev, peer_wants_headers).await?;
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(broadcast::error::RecvError::Closed) => return Ok(()),
                }
            }
            msg = read_msg(&mut stream) => {
                let msg = match msg {
                    Ok(m) => m,
                    Err(NetError::Io(e)) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                        return Ok(());
                    }
                    Err(e) => return Err(e),
                };
                if msg.magic() != &magic {
                    return Err(NetError::BadMagic);
                }
                match msg.payload() {
                    NetworkMessage::Ping(n) => {
                        write_msg(&mut stream, magic, NetworkMessage::Pong(*n)).await?;
                    }
                    NetworkMessage::Pong(_) => {}
                    NetworkMessage::SendHeaders => {
                        peer_wants_headers = true;
                    }
                    NetworkMessage::SendCmpct(_) => {
                        // Peer may send us compact blocks; we always fall back to getdata.
                    }
                    NetworkMessage::GetHeaders(gh) => {
                        let headers = headers_for_peer(hub.cache.as_ref(), hub.query.as_ref(), gh)?;
                        write_msg(&mut stream, magic, NetworkMessage::Headers(headers)).await?;
                    }
                    NetworkMessage::GetData(inv) => {
                        for item in inv {
                            match item {
                                Inventory::Block(h) | Inventory::WitnessBlock(h) => {
                                    if let Some(block) =
                                        block_for_peer(hub.cache.as_ref(), hub.query.as_ref(), h)?
                                    {
                                        write_msg(&mut stream, magic, NetworkMessage::Block(block))
                                            .await?;
                                    }
                                }
                                Inventory::Transaction(_)
                                | Inventory::WitnessTransaction(_)
                                | Inventory::WTx(_) => {}
                                _ => {}
                            }
                        }
                    }
                    NetworkMessage::Inv(items) => {
                        let mut want = Vec::new();
                        for item in items {
                            match item {
                                Inventory::Block(h) | Inventory::WitnessBlock(h) => {
                                    if !hub.has_block(h) {
                                        want.push(Inventory::WitnessBlock(*h));
                                    }
                                }
                                _ => {} // no tx relay
                            }
                        }
                        if !want.is_empty() {
                            write_msg(&mut stream, magic, NetworkMessage::GetData(want)).await?;
                        }
                    }
                    NetworkMessage::Headers(headers) => {
                        let mut want = Vec::new();
                        for hdr in headers {
                            let hash = hdr.block_hash();
                            pending_headers.insert(hash, *hdr);
                            if !hub.has_block(&hash) {
                                want.push(Inventory::WitnessBlock(hash));
                            }
                        }
                        if !want.is_empty() {
                            write_msg(&mut stream, magic, NetworkMessage::GetData(want)).await?;
                        }
                    }
                    NetworkMessage::Block(block) => {
                        pending_blocks.insert(block.block_hash(), block.clone());
                        drain_pending(hub.as_ref(), &mut pending_blocks, &mut pending_headers)?;
                    }
                    NetworkMessage::CmpctBlock(cb) => {
                        // No mempool short-id reconstruction — request full witness block.
                        let hash = cb.compact_block.header.block_hash();
                        if !hub.has_block(&hash) {
                            write_msg(
                                &mut stream,
                                magic,
                                NetworkMessage::GetData(vec![Inventory::WitnessBlock(hash)]),
                            )
                            .await?;
                        }
                    }
                    NetworkMessage::Tx(_) | NetworkMessage::MemPool => {}
                    NetworkMessage::GetAddr => {
                        write_msg(&mut stream, magic, NetworkMessage::Addr(vec![])).await?;
                    }
                    _ => {}
                }
            }
        }
    }
}

async fn announce_tip(
    stream: &mut TcpStream,
    magic: Magic,
    ev: &crate::chain::TipEvent,
    peer_wants_headers: bool,
) -> Result<(), NetError> {
    if peer_wants_headers {
        write_msg(
            stream,
            magic,
            NetworkMessage::Headers(vec![ev.header]),
        )
        .await
    } else {
        write_msg(
            stream,
            magic,
            NetworkMessage::Inv(vec![Inventory::WitnessBlock(ev.hash)]),
        )
        .await
    }
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
    stream: &mut TcpStream,
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
        stream,
        magic,
        locator,
        |hash| hub.has_block(hash),
        |_, block| {
            hub.accept_block(block).map(|_| ())
        },
    )
    .await?;
    Ok(())
}

/// Serve-only loop (no tip announce subscription). Prefer [`peer_session`].
#[allow(dead_code)]
pub async fn serve_peer_loop(
    mut stream: TcpStream,
    magic: Magic,
    cache: Arc<BlockCache>,
    query: Arc<Query>,
) -> Result<(), NetError> {
    loop {
        let msg = match read_msg(&mut stream).await {
            Ok(m) => m,
            Err(NetError::Io(e)) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                return Ok(());
            }
            Err(e) => return Err(e),
        };
        if msg.magic() != &magic {
            return Err(NetError::BadMagic);
        }
        match msg.payload() {
            NetworkMessage::Ping(n) => {
                write_msg(&mut stream, magic, NetworkMessage::Pong(*n)).await?;
            }
            NetworkMessage::GetHeaders(gh) => {
                let headers = headers_for_peer(cache.as_ref(), query.as_ref(), gh)?;
                write_msg(&mut stream, magic, NetworkMessage::Headers(headers)).await?;
            }
            NetworkMessage::GetData(inv) => {
                for item in inv {
                    match item {
                        Inventory::Block(h) | Inventory::WitnessBlock(h) => {
                            if let Some(block) = block_for_peer(cache.as_ref(), query.as_ref(), h)? {
                                write_msg(&mut stream, magic, NetworkMessage::Block(block))
                                    .await?;
                            }
                        }
                        Inventory::Transaction(_)
                        | Inventory::WitnessTransaction(_)
                        | Inventory::WTx(_) => {}
                        _ => {}
                    }
                }
            }
            NetworkMessage::Inv(_) | NetworkMessage::Tx(_) | NetworkMessage::MemPool => {}
            NetworkMessage::GetAddr => {
                write_msg(&mut stream, magic, NetworkMessage::Addr(vec![])).await?;
            }
            _ => {}
        }
    }
}

fn headers_for_peer(
    cache: &BlockCache,
    query: &Query,
    gh: &bitcoin::p2p::message_blockdata::GetHeadersMessage,
) -> Result<Vec<bitcoin::block::Header>, NetError> {
    match query.headers_after_locator(&gh.locator_hashes, gh.stop_hash, 2000) {
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
    stream: &mut TcpStream,
    magic: Magic,
    local_locator: Vec<BlockHash>,
    mut local_has: impl FnMut(&BlockHash) -> bool,
    mut on_block: impl FnMut(u32, bitcoin::Block) -> Result<(), NetError>,
) -> Result<u32, NetError> {
    let mut downloaded = 0u32;
    let mut locator = local_locator;
    loop {
        let gh = GetHeadersMessage::new(locator.clone(), BlockHash::from_byte_array([0u8; 32]));
        write_msg(stream, magic, NetworkMessage::GetHeaders(gh)).await?;

        let headers = loop {
            let msg = read_msg(stream).await?;
            if msg.magic() != &magic {
                return Err(NetError::BadMagic);
            }
            match msg.payload() {
                NetworkMessage::Headers(h) => break h.clone(),
                NetworkMessage::Ping(n) => {
                    write_msg(stream, magic, NetworkMessage::Pong(*n)).await?;
                }
                NetworkMessage::SendHeaders | NetworkMessage::SendCmpct(_) | NetworkMessage::Verack => {}
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
            if headers.len() < 2000 {
                break;
            }
            locator = vec![headers.last().unwrap().block_hash()];
            continue;
        }

        write_msg(stream, magic, NetworkMessage::GetData(inv.clone())).await?;

        let need = inv.len();
        let mut got = 0;
        while got < need {
            let msg = read_msg(stream).await?;
            if msg.magic() != &magic {
                return Err(NetError::BadMagic);
            }
            match msg.payload() {
                NetworkMessage::Block(block) => {
                    on_block(0, block.clone())?;
                    got += 1;
                    downloaded += 1;
                }
                NetworkMessage::Ping(n) => {
                    write_msg(stream, magic, NetworkMessage::Pong(*n)).await?;
                }
                NetworkMessage::NotFound(_) => {
                    return Err(NetError::Protocol("block not found"));
                }
                NetworkMessage::SendHeaders | NetworkMessage::SendCmpct(_) => {}
                _ => {}
            }
        }

        locator = vec![headers.last().unwrap().block_hash()];
        if headers.len() < 2000 {
            break;
        }
    }
    Ok(downloaded)
}
