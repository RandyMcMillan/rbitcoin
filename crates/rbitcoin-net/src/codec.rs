//! Async read/write of Bitcoin P2P messages.

use crate::error::NetError;
use bitcoin::consensus::{deserialize, encode, serialize};
use bitcoin::p2p::message::{CommandString, NetworkMessage, RawNetworkMessage, MAX_MSG_SIZE};
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
    let magic = Magic::from_bytes(header[0..4].try_into().unwrap());
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

    match deserialize::<RawNetworkMessage>(&full) {
        Ok(msg) => Ok(msg),
        Err(e) => {
            // Real peers send extensions/padding that can trip strict payload checks
            // (e.g. "extra bytes after network message payload"). Bytes are already
            // consumed from the socket — surface as Unknown so the peer loop continues.
            let cmd = command_from_header(&header[4..16]);
            let _ = e; // kept for debugging if we log later
            Ok(RawNetworkMessage::new(
                magic,
                NetworkMessage::Unknown {
                    command: cmd,
                    payload,
                },
            ))
        }
    }
}

fn command_from_header(cmd12: &[u8]) -> CommandString {
    let end = cmd12.iter().position(|&b| b == 0).unwrap_or(12);
    let s = std::str::from_utf8(&cmd12[..end]).unwrap_or("unknown");
    CommandString::try_from(s).unwrap_or_else(|_| {
        CommandString::try_from("unknown").expect("literal command string")
    })
}

// Silence unused import warning if encode only used in map_err elsewhere
#[allow(dead_code)]
fn _encode_type_check(e: encode::Error) -> String {
    e.to_string()
}
