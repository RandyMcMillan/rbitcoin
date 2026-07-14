//! Async read/write of Bitcoin P2P messages.

use crate::error::NetError;
use bitcoin::consensus::{deserialize, encode, serialize};
use bitcoin::p2p::message::{NetworkMessage, RawNetworkMessage, MAX_MSG_SIZE};
use bitcoin::p2p::Magic;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

pub async fn write_msg(
    stream: &mut TcpStream,
    magic: Magic,
    payload: NetworkMessage,
) -> Result<(), NetError> {
    let raw = RawNetworkMessage::new(magic, payload);
    let bytes = serialize(&raw);
    stream.write_all(&bytes).await?;
    stream.flush().await?;
    Ok(())
}

pub async fn read_msg(stream: &mut TcpStream) -> Result<RawNetworkMessage, NetError> {
    let mut header = [0u8; 24];
    stream.read_exact(&mut header).await?;
    let payload_len = u32::from_le_bytes(header[16..20].try_into().unwrap()) as usize;
    if payload_len > MAX_MSG_SIZE {
        return Err(NetError::MessageTooLarge(payload_len));
    }
    let mut payload = vec![0u8; payload_len];
    if payload_len > 0 {
        stream.read_exact(&mut payload).await?;
    }
    let mut full = Vec::with_capacity(24 + payload_len);
    full.extend_from_slice(&header);
    full.extend_from_slice(&payload);
    let msg: RawNetworkMessage =
        deserialize(&full).map_err(|e: encode::Error| NetError::Encode(e.to_string()))?;
    Ok(msg)
}
