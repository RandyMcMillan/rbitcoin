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
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::net::TcpStream;

pub async fn handshake(
    stream: &mut TcpStream,
    magic: Magic,
    our_addr: SocketAddr,
    their_addr: SocketAddr,
    start_height: i32,
    inbound: bool,
) -> Result<VersionMessage, NetError> {
    let services = ServiceFlags::NETWORK;
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

/// Serve peer: respond to getheaders/getdata/ping; ignore tx inv.
pub async fn serve_peer_loop(
    mut stream: TcpStream,
    magic: Magic,
    cache: Arc<BlockCache>,
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
                let headers = cache.headers_after_locator(&gh.locator_hashes, gh.stop_hash);
                write_msg(&mut stream, magic, NetworkMessage::Headers(headers)).await?;
            }
            NetworkMessage::GetData(inv) => {
                for item in inv {
                    match item {
                        Inventory::Block(h) | Inventory::WitnessBlock(h) => {
                            if let Some(block) = cache.get_block(h) {
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

/// Outbound sync: getheaders until caught up, then getdata for missing blocks in order.
/// Calls `on_block` for each downloaded block (height, block).
pub async fn sync_from_peer(
    stream: &mut TcpStream,
    magic: Magic,
    local_cache: &BlockCache,
    mut on_block: impl FnMut(u32, bitcoin::Block) -> Result<(), NetError>,
) -> Result<u32, NetError> {
    let mut downloaded = 0u32;
    loop {
        let locator = local_cache.locator();
        let gh = GetHeadersMessage::new(locator, BlockHash::from_byte_array([0u8; 32]));
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
            if local_cache.get_block(&hash).is_none() {
                inv.push(Inventory::WitnessBlock(hash));
            }
        }
        if inv.is_empty() {
            // Peer sent headers we already have — stop
            if headers.len() < 2000 {
                break;
            }
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
                    let height = local_cache.tip_height().map(|h| h + 1).unwrap_or(0);
                    on_block(height, block.clone())?;
                    // Caller is responsible for updating local_cache via on_block
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

        if headers.len() < 2000 {
            break;
        }
    }
    Ok(downloaded)
}
