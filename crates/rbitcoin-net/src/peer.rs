//! Single-peer handshake and message loop.

use crate::cache::BlockCache;
use crate::codec::{read_msg, write_msg};
use crate::error::NetError;
use bitcoin::hashes::Hash;
use bitcoin::p2p::address::Address;
use bitcoin::p2p::message::NetworkMessage;
use bitcoin::p2p::message_blockdata::{GetHeadersMessage, Inventory};
use bitcoin::p2p::message_network::VersionMessage;
use bitcoin::p2p::{Magic, ServiceFlags, PROTOCOL_VERSION};
use bitcoin::BlockHash;
use rbitcoin_query::Query;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::net::TcpStream;

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

    // Expect version
    let their_version = loop {
        let msg = read_msg(stream).await?;
        if msg.magic() != &magic {
            return Err(NetError::BadMagic);
        }
        match msg.payload() {
            NetworkMessage::Version(v) => break v.clone(),
            other => {
                // Ignore until version (some peers send early)
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

    // Expect verack (may already have other messages interleaved — loop)
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
            // Ignore sendheaders etc. during handshake tail
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

/// Serve peer: respond to getheaders/getdata/ping from store (reconstruct) + optional RAM cache.
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
                        | Inventory::WTx(_) => {
                            // No tx relay — silently ignore
                        }
                        _ => {}
                    }
                }
            }
            NetworkMessage::Inv(items) => {
                // Ignore tx inventories; do not request txs
                let _ = items;
            }
            NetworkMessage::Tx(_) | NetworkMessage::MemPool => {
                // Drop — no mempool
            }
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
    // Prefer store-backed headers (works after restart). Fall back to RAM cache.
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
/// Calls `on_block` for each downloaded block (height, block).
///
/// `local_tip_height` / `local_has` / `local_locator` come from store or cache so restart sync works.
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
                _ => {}
            }
        };

        if headers.is_empty() {
            break;
        }

        // Request blocks for each header we don't have
        let mut inv = Vec::new();
        for hdr in &headers {
            let hash = hdr.block_hash();
            if !local_has(&hash) {
                inv.push(Inventory::WitnessBlock(hash));
            }
        }
        if inv.is_empty() {
            // Peer sent headers we already have — stop if short batch
            if headers.len() < 2000 {
                break;
            }
            // Advance locator from last header
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
                    // Height is assigned by the accept callback's store tip+1
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
                _ => {}
            }
        }

        // Update locator for next round from last header in batch
        locator = vec![headers.last().unwrap().block_hash()];
        if headers.len() < 2000 {
            break;
        }
    }
    Ok(downloaded)
}
