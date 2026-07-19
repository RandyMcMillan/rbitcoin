//! BIP324 v2-only encrypted transport.
//!
//! Production peers complete a BIP324 handshake first; application `version` /
//! `verack` and all later messages travel as encrypted packets whose plaintext
//! is the BIP324 v2 message encoding (1-byte short ID or 13-byte long command +
//! payload — no network magic, length, or checksum).
//!
//! Peers that only speak v1 are disconnected ([`NetError::V1Peer`]).

use crate::codec::{
    encode_is_cpu_heavy, command_bytes_ok, FramedMessage, MAX_HEADERS_RESULTS, MAX_INV_SIZE,
    MAX_LOCATOR_SZ, MAX_PROTOCOL_MESSAGE_LENGTH,
};
use crate::error::NetError;
use bip324::futures::{Protocol, ProtocolReader, ProtocolWriter};
use bip324::io::{Payload, ProtocolError, ProtocolFailureSuggestion};
use bip324::{Error as Bip324Error, PacketType, Role};
use bitcoin::consensus::serialize;
use bitcoin::hashes::sha256d;
use bitcoin::hashes::Hash as _;
use bitcoin::p2p::message::NetworkMessage;
use bitcoin::p2p::Magic;
use tokio::io::BufReader;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::TcpStream;

/// Async read half after BIP324 handshake (buffered TCP).
pub type V2Reader = ProtocolReader<BufReader<OwnedReadHalf>>;
/// Async write half after BIP324 handshake.
pub type V2Writer = ProtocolWriter<OwnedWriteHalf>;

// ── BIP324 short message type IDs (Core `V2_MESSAGE_IDS` / BIP324 table) ──

/// Short ID → command name. Index 0 is the long-form escape (not a real message).
/// Matches Bitcoin Core `V2_MESSAGE_IDS` (net.cpp).
const SHORT_IDS: &[&str] = &[
    "", // 0: long encoding follows
    "addr",
    "block",
    "blocktxn",
    "cmpctblock",
    "feefilter",
    "filteradd",
    "filterclear",
    "filterload",
    "getblocks",
    "getblocktxn",
    "getdata",
    "getheaders",
    "headers",
    "inv",
    "mempool",
    "merkleblock",
    "notfound",
    "ping",
    "pong",
    "sendcmpct",
    "tx",
    "getcfilters",
    "cfilter",
    "getcfheaders",
    "cfheaders",
    "getcfcheckpt",
    "cfcheckpt",
    "addrv2",
    // 29–36 unimplemented placeholders (empty)
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "feature", // 37
];

fn short_id_for_command(cmd: &str) -> Option<u8> {
    // Skip index 0 (long-form marker).
    SHORT_IDS
        .iter()
        .enumerate()
        .skip(1)
        .find(|(_, name)| !name.is_empty() && **name == cmd)
        .map(|(i, _)| i as u8)
}

fn command_for_short_id(id: u8) -> Option<&'static str> {
    let idx = id as usize;
    if idx == 0 || idx >= SHORT_IDS.len() {
        return None;
    }
    let name = SHORT_IDS[idx];
    if name.is_empty() {
        None
    } else {
        Some(name)
    }
}

fn command_to_12(cmd: &str) -> [u8; 12] {
    let mut out = [0u8; 12];
    let b = cmd.as_bytes();
    let n = b.len().min(12);
    out[..n].copy_from_slice(&b[..n]);
    out
}

fn command_from_12(cmd12: &[u8; 12]) -> Result<String, NetError> {
    if !command_bytes_ok(cmd12) {
        return Err(NetError::Protocol("invalid message command"));
    }
    let end = cmd12.iter().position(|&b| b == 0).unwrap_or(12);
    std::str::from_utf8(&cmd12[..end])
        .map(|s| s.to_string())
        .map_err(|_| NetError::Protocol("invalid message command"))
}

/// Encode a P2P application message as BIP324 packet contents (short/long type + payload).
pub fn encode_v2_contents(payload: NetworkMessage) -> Result<Vec<u8>, NetError> {
    // Cap outbound inventory-style messages so we never emit Core-rejected sizes.
    match &payload {
        NetworkMessage::Inv(v) | NetworkMessage::GetData(v) | NetworkMessage::NotFound(v) => {
            if v.len() > MAX_INV_SIZE {
                return Err(NetError::MessageTooLarge(v.len()));
            }
        }
        NetworkMessage::Headers(h) => {
            if h.len() > MAX_HEADERS_RESULTS {
                return Err(NetError::MessageTooLarge(h.len()));
            }
        }
        NetworkMessage::GetHeaders(gh) => {
            if gh.locator_hashes.len() > MAX_LOCATOR_SZ {
                return Err(NetError::MessageTooLarge(gh.locator_hashes.len()));
            }
        }
        NetworkMessage::GetBlocks(gb) => {
            if gb.locator_hashes.len() > MAX_LOCATOR_SZ {
                return Err(NetError::MessageTooLarge(gb.locator_hashes.len()));
            }
        }
        _ => {}
    }

    let cmd = match &payload {
        NetworkMessage::Unknown { command, .. } => command.to_string(),
        _ => payload.cmd().to_string(),
    };
    let body = serialize(&payload);
    if body.len() > MAX_PROTOCOL_MESSAGE_LENGTH {
        return Err(NetError::MessageTooLarge(body.len()));
    }

    if let Some(id) = short_id_for_command(&cmd) {
        let mut out = Vec::with_capacity(1 + body.len());
        out.push(id);
        out.extend_from_slice(&body);
        Ok(out)
    } else {
        // Long form: 0x00 + 12-byte ASCII command (null-padded) + payload.
        let mut out = Vec::with_capacity(1 + 12 + body.len());
        out.push(0);
        out.extend_from_slice(&command_to_12(&cmd));
        out.extend_from_slice(&body);
        Ok(out)
    }
}

/// Parse BIP324 packet contents into a [`FramedMessage`] (synthetic checksum for decode path).
pub fn parse_v2_contents(magic: Magic, contents: &[u8]) -> Result<FramedMessage, NetError> {
    if contents.is_empty() {
        return Err(NetError::Protocol("empty v2 message contents"));
    }
    let first = contents[0];
    let (command, payload) = if first != 0 {
        let name = command_for_short_id(first).ok_or(NetError::Protocol("unknown v2 short id"))?;
        (command_to_12(name), contents[1..].to_vec())
    } else {
        if contents.len() < 1 + 12 {
            return Err(NetError::Protocol("truncated v2 long command"));
        }
        let mut cmd12 = [0u8; 12];
        cmd12.copy_from_slice(&contents[1..13]);
        // Validate padding / charset.
        let _ = command_from_12(&cmd12)?;
        (cmd12, contents[13..].to_vec())
    };

    if payload.len() > MAX_PROTOCOL_MESSAGE_LENGTH {
        return Err(NetError::MessageTooLarge(payload.len()));
    }

    // Synthetic checksum so existing FramedMessage::decode (v1-shaped) still works.
    let dig = sha256d::Hash::hash(&payload);
    let ba = dig.to_byte_array();
    let checksum = [ba[0], ba[1], ba[2], ba[3]];

    Ok(FramedMessage {
        magic,
        command,
        checksum,
        payload,
    })
}

fn map_protocol_error(e: ProtocolError) -> NetError {
    match e {
        ProtocolError::Io(_, ProtocolFailureSuggestion::RetryV1) => NetError::V1Peer,
        ProtocolError::Io(io, _) => NetError::Io(io),
        ProtocolError::Internal(Bip324Error::V1Protocol) => NetError::V1Peer,
        ProtocolError::Internal(inner) => NetError::Bip324(inner.to_string()),
    }
}

/// Complete BIP324 handshake on a connected TCP stream; return split encrypted halves.
///
/// Not cancellation-safe (BIP324 handshake). Callers should not wrap this in
/// `select!` without a dedicated task.
pub async fn open_v2(
    stream: TcpStream,
    magic: Magic,
    inbound: bool,
) -> Result<(V2Reader, V2Writer), NetError> {
    let _ = stream.set_nodelay(true);
    let role = if inbound {
        Role::Responder
    } else {
        Role::Initiator
    };
    let magic_bytes = magic.to_bytes();
    let (rh, wh) = stream.into_split();
    // Protocol performs many small reads; BufReader is required for performance.
    let reader = BufReader::new(rh);
    let protocol = Protocol::new(magic_bytes, role, None, None, reader, wh)
        .await
        .map_err(map_protocol_error)?;
    Ok(protocol.into_split())
}

/// Encrypt and send one application message.
pub async fn write_v2_msg(writer: &mut V2Writer, payload: NetworkMessage) -> Result<(), NetError> {
    let contents = encode_v2_contents(payload)?;
    writer
        .write(&Payload::genuine(contents))
        .await
        .map_err(map_protocol_error)
}

/// Like [`write_v2_msg`]; heavy payloads encode on the blocking pool before encrypt.
pub async fn write_v2_msg_offload(
    writer: &mut V2Writer,
    payload: NetworkMessage,
) -> Result<(), NetError> {
    let contents = if encode_is_cpu_heavy(&payload) {
        tokio::task::spawn_blocking(move || encode_v2_contents(payload))
            .await
            .map_err(|_| NetError::Protocol("encode task join failed"))??
    } else {
        encode_v2_contents(payload)?
    };
    writer
        .write(&Payload::genuine(contents))
        .await
        .map_err(map_protocol_error)
}

/// Read the next genuine application frame (skips decoy packets).
///
/// Cancellation-safe (delegates to `ProtocolReader::read`).
pub async fn read_v2_frame(
    reader: &mut V2Reader,
    magic: Magic,
) -> Result<FramedMessage, NetError> {
    read_v2_frame_with_progress(reader, magic, |_| {}).await
}

/// Read the next genuine frame; `on_progress` is invoked with decrypted content
/// length when a full packet arrives (mid-packet progress is inside bip324).
pub async fn read_v2_frame_with_progress<F>(
    reader: &mut V2Reader,
    magic: Magic,
    mut on_progress: F,
) -> Result<FramedMessage, NetError>
where
    F: FnMut(usize),
{
    loop {
        let payload = reader.read().await.map_err(map_protocol_error)?;
        if payload.packet_type() == PacketType::Decoy {
            continue;
        }
        let contents = payload.contents();
        on_progress(contents.len());
        return parse_v2_contents(magic, contents);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bitcoin::Network;
    use tokio::io::duplex;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    fn signet_magic() -> Magic {
        Magic::from(Network::Signet)
    }

    #[test]
    fn short_id_roundtrip_common() {
        assert_eq!(short_id_for_command("block"), Some(2));
        assert_eq!(short_id_for_command("ping"), Some(18));
        assert_eq!(short_id_for_command("version"), None); // long form
        assert_eq!(command_for_short_id(2), Some("block"));
        assert_eq!(command_for_short_id(18), Some("ping"));
    }

    #[test]
    fn encode_parse_verack_long_form() {
        let magic = signet_magic();
        let contents = encode_v2_contents(NetworkMessage::Verack).unwrap();
        // version/verack use long form: 0x00 + "verack" + pad + empty payload
        assert_eq!(contents[0], 0);
        assert_eq!(&contents[1..7], b"verack");
        let frame = parse_v2_contents(magic, &contents).unwrap();
        assert!(matches!(frame.decode().payload(), NetworkMessage::Verack));
    }

    #[test]
    fn encode_parse_ping_short_id() {
        let magic = signet_magic();
        let contents = encode_v2_contents(NetworkMessage::Ping(0xdead_beef)).unwrap();
        assert_eq!(contents[0], 18); // ping short id
        assert_eq!(contents.len(), 1 + 8);
        let frame = parse_v2_contents(magic, &contents).unwrap();
        assert!(frame.is_ping());
        assert_eq!(frame.ping_nonce(), Some(0xdead_beef));
    }

    /// Two ends of a tokio duplex complete BIP324 + application ping/pong.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn bip324_duplex_ping_pong() {
        let magic = signet_magic();
        let magic_b = magic.to_bytes();
        let (client, server) = duplex(64 * 1024);

        let server_task = tokio::spawn(async move {
            let (rh, wh) = tokio::io::split(server);
            let reader = BufReader::new(rh);
            let protocol = Protocol::new(magic_b, Role::Responder, None, None, reader, wh)
                .await
                .expect("server handshake");
            let (mut r, mut w) = protocol.into_split();
            // read genuine packet
            loop {
                let p = r.read().await.expect("server read");
                if p.packet_type() == PacketType::Genuine {
                    let frame = parse_v2_contents(magic, p.contents()).unwrap();
                    assert!(frame.is_ping());
                    let n = frame.ping_nonce().unwrap();
                    let contents = encode_v2_contents(NetworkMessage::Pong(n)).unwrap();
                    w.write(&Payload::genuine(contents)).await.unwrap();
                    break;
                }
            }
        });

        let client_task = async move {
            let (rh, wh) = tokio::io::split(client);
            let reader = BufReader::new(rh);
            let protocol = Protocol::new(magic_b, Role::Initiator, None, None, reader, wh)
                .await
                .expect("client handshake");
            let (mut r, mut w) = protocol.into_split();
            let contents = encode_v2_contents(NetworkMessage::Ping(42)).unwrap();
            w.write(&Payload::genuine(contents)).await.unwrap();
            loop {
                let p = r.read().await.expect("client read");
                if p.packet_type() == PacketType::Genuine {
                    let frame = parse_v2_contents(magic, p.contents()).unwrap();
                    assert!(matches!(frame.decode().payload(), NetworkMessage::Pong(42)));
                    break;
                }
            }
            server_task.await.unwrap();
        };

        tokio::time::timeout(std::time::Duration::from_secs(5), client_task)
            .await
            .expect("bip324 duplex timed out");
    }

    /// V1-looking ellswift slot (network magic in first 4 of 64 bytes) → V1Peer.
    ///
    /// bip324 detects this only after a full 64-byte key read — sending fewer
    /// bytes would hang on `read_exact`.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn v1_peer_rejected() {
        let magic = signet_magic();
        let (mut client, server) = duplex(8 * 1024);

        let server_task = tokio::spawn(async move {
            let (rh, wh) = tokio::io::split(server);
            let reader = BufReader::new(rh);
            Protocol::new(magic.to_bytes(), Role::Responder, None, None, reader, wh).await
        });

        // Drain responder's ellswift key first so its write does not block.
        let mut their_key = [0u8; 64];
        client.read_exact(&mut their_key).await.unwrap();

        // Fake "key" whose first 4 bytes are network magic (Core/bip324 V1 probe).
        let mut v1_key = [0u8; 64];
        v1_key[..4].copy_from_slice(&magic.to_bytes());
        client.write_all(&v1_key).await.unwrap();
        client.flush().await.unwrap();

        let result = tokio::time::timeout(std::time::Duration::from_secs(5), server_task)
            .await
            .expect("v1 detect timed out")
            .expect("server task join");
        let err = match result {
            Ok(_) => panic!("v1-looking peer must fail BIP324 handshake"),
            Err(e) => e,
        };
        let mapped = map_protocol_error(err);
        assert!(
            matches!(mapped, NetError::V1Peer),
            "expected V1Peer, got {mapped}"
        );
    }
}
