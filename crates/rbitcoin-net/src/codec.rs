//! Async read/write of Bitcoin P2P messages.
//!
//! Limits match Bitcoin Core (`net.h` / `serialize.h` policy):
//! - `MAX_PROTOCOL_MESSAGE_LENGTH` = 4_000_000 (payload)
//! - `MAX_INV_SZ` = 50_000
//! - `MAX_HEADERS_RESULTS` = 2_000
//!
//! Framing is **cancellation-safe**: partial socket reads are retained on
//! [`MessageStream`] so `tokio::select!` cannot desync the stream (the classic
//! cause of multi-GB "message too large" lengths from misaligned headers).

use crate::error::NetError;
use bitcoin::consensus::{deserialize, encode, serialize};
use bitcoin::p2p::message::{CommandString, NetworkMessage, RawNetworkMessage};
use bitcoin::p2p::Magic;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;

// ── Core-aligned P2P limits ───────────────────────────────────────────────

/// Bitcoin Core `MAX_PROTOCOL_MESSAGE_LENGTH` — max P2P payload bytes.
///
/// Note: rust-bitcoin's `MAX_MSG_SIZE` is 5_000_000; Core enforces 4_000_000.
/// We use Core's limit so we never accept (or send) messages Core would reject.
pub const MAX_PROTOCOL_MESSAGE_LENGTH: usize = 4_000_000;

/// Bitcoin Core `MAX_INV_SZ` — max inventory items in inv/getdata/notfound.
pub const MAX_INV_SIZE: usize = 50_000;

/// Bitcoin Core `MAX_HEADERS_RESULTS` — max headers in a `headers` message.
pub const MAX_HEADERS_RESULTS: usize = 2_000;

/// Bitcoin Core `MAX_LOCATOR_SZ` — max block locator hashes.
pub const MAX_LOCATOR_SZ: usize = 101;

/// Absolute ceiling on the reassembly buffer (header + max payload + slack).
const MAX_RECV_BUFFER: usize = MAX_PROTOCOL_MESSAGE_LENGTH + 24 + 64 * 1024;

// ── Write ─────────────────────────────────────────────────────────────────

pub async fn write_msg(
    stream: &mut TcpStream,
    magic: Magic,
    payload: NetworkMessage,
) -> Result<(), NetError> {
    write_msg_to(stream, magic, payload).await
}

pub async fn write_msg_to<W: AsyncWrite + Unpin>(
    writer: &mut W,
    magic: Magic,
    payload: NetworkMessage,
) -> Result<(), NetError> {
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

    let raw = RawNetworkMessage::new(magic, payload);
    let bytes = serialize(&raw);
    if bytes.len() > MAX_PROTOCOL_MESSAGE_LENGTH + 24 {
        return Err(NetError::MessageTooLarge(bytes.len()));
    }
    writer.write_all(&bytes).await?;
    writer.flush().await?;
    Ok(())
}

// ── Simple read (non-select paths: handshake, sequential sync) ────────────

/// Read one full message. Prefer [`MessageStream`] inside `tokio::select!`.
pub async fn read_msg(stream: &mut TcpStream) -> Result<RawNetworkMessage, NetError> {
    let mut ms = MessageStream::new();
    ms.read_msg(stream, None).await
}

// ── Cancellation-safe framed reader ───────────────────────────────────────

/// Owns partial receive state so cancelled `read_msg` futures do not lose bytes.
#[derive(Debug, Default)]
pub struct MessageStream {
    buf: Vec<u8>,
}

impl MessageStream {
    pub fn new() -> Self {
        Self { buf: Vec::with_capacity(8 * 1024) }
    }

    /// Bytes currently buffered (for tests / diagnostics).
    pub fn buffered_len(&self) -> usize {
        self.buf.len()
    }

    /// Read the next full P2P message.
    ///
    /// If `expected_magic` is `Some`, the header magic must match or we return
    /// [`NetError::BadMagic`] without consuming the 24-byte header (caller may
    /// disconnect). Invalid length / command reject and disconnect semantics.
    pub async fn read_msg<R: AsyncRead + Unpin>(
        &mut self,
        reader: &mut R,
        expected_magic: Option<Magic>,
    ) -> Result<RawNetworkMessage, NetError> {
        // 1) Header
        self.fill_until(reader, 24).await?;
        let header: [u8; 24] = self.buf[..24]
            .try_into()
            .expect("fill_until(24) guarantees length");

        let magic = Magic::from_bytes(header[0..4].try_into().unwrap());
        if let Some(exp) = expected_magic {
            if magic != exp {
                // Leave bytes in buffer so a reconnect path could inspect; for
                // our use we always disconnect on BadMagic.
                return Err(NetError::BadMagic);
            }
        }

        if !command_bytes_ok(&header[4..16]) {
            return Err(NetError::Protocol("invalid message command"));
        }

        let payload_len = u32::from_le_bytes(header[16..20].try_into().unwrap()) as usize;
        if payload_len > MAX_PROTOCOL_MESSAGE_LENGTH {
            return Err(NetError::MessageTooLarge(payload_len));
        }

        // 2) Payload
        let total = 24 + payload_len;
        self.fill_until(reader, total).await?;

        let full: Vec<u8> = self.buf.drain(..total).collect();
        let payload = full[24..].to_vec();

        match deserialize::<RawNetworkMessage>(&full) {
            Ok(msg) => Ok(msg),
            Err(_e) => {
                // Real peers send extensions/padding that can trip strict payload
                // checks (e.g. "extra bytes after network message payload").
                // Bytes are already framed correctly — surface as Unknown.
                let cmd = command_from_header(&header[4..16]);
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

    async fn fill_until<R: AsyncRead + Unpin>(
        &mut self,
        reader: &mut R,
        need: usize,
    ) -> Result<(), NetError> {
        while self.buf.len() < need {
            if self.buf.len() >= MAX_RECV_BUFFER {
                return Err(NetError::MessageTooLarge(self.buf.len()));
            }
            let mut tmp = [0u8; 16 * 1024];
            let n = reader.read(&mut tmp).await?;
            if n == 0 {
                return Err(NetError::Io(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "peer closed during message read",
                )));
            }
            self.buf.extend_from_slice(&tmp[..n]);
        }
        Ok(())
    }
}

fn command_bytes_ok(cmd12: &[u8]) -> bool {
    // Core: command is ASCII letters, null-padded. Reject binary garbage so we
    // fail fast on mid-stream desync / non-v1 transports (e.g. raw BIP324).
    if cmd12.len() != 12 {
        return false;
    }
    let mut seen_null = false;
    let mut any = false;
    for &b in cmd12 {
        if b == 0 {
            seen_null = true;
            continue;
        }
        if seen_null {
            return false; // non-zero after null padding
        }
        // Core uses lowercase a-z only for command names.
        if !b.is_ascii_lowercase() {
            return false;
        }
        any = true;
    }
    any
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

#[cfg(test)]
mod tests {
    use super::*;
    use bitcoin::Network;
    use tokio::io::duplex;

    fn signet_magic() -> Magic {
        Magic::from(Network::Signet)
    }

    #[tokio::test]
    async fn roundtrip_verack() {
        let magic = signet_magic();
        let (mut a, mut b) = duplex(1024);
        write_msg_to(&mut a, magic, NetworkMessage::Verack)
            .await
            .unwrap();
        let mut ms = MessageStream::new();
        let msg = ms.read_msg(&mut b, Some(magic)).await.unwrap();
        assert_eq!(msg.magic(), &magic);
        assert!(matches!(msg.payload(), NetworkMessage::Verack));
    }

    #[tokio::test]
    async fn rejects_oversize_length() {
        let magic = signet_magic();
        let (mut a, mut b) = duplex(64);
        // Craft a header with absurd payload length
        let mut hdr = [0u8; 24];
        hdr[0..4].copy_from_slice(magic.to_bytes().as_ref());
        hdr[4..9].copy_from_slice(b"block");
        hdr[16..20].copy_from_slice(&(100_000_000u32).to_le_bytes());
        a.write_all(&hdr).await.unwrap();
        let mut ms = MessageStream::new();
        let err = ms.read_msg(&mut b, Some(magic)).await.unwrap_err();
        assert!(matches!(err, NetError::MessageTooLarge(100_000_000)));
    }

    #[tokio::test]
    async fn rejects_bad_magic() {
        let magic = signet_magic();
        let other = Magic::from(Network::Bitcoin);
        let (mut a, mut b) = duplex(1024);
        write_msg_to(&mut a, other, NetworkMessage::Verack)
            .await
            .unwrap();
        let mut ms = MessageStream::new();
        let err = ms.read_msg(&mut b, Some(magic)).await.unwrap_err();
        assert!(matches!(err, NetError::BadMagic));
    }

    #[tokio::test]
    async fn rejects_bad_command() {
        let magic = signet_magic();
        let (mut a, mut b) = duplex(64);
        let mut hdr = [0u8; 24];
        hdr[0..4].copy_from_slice(magic.to_bytes().as_ref());
        // Non-printable command
        hdr[4] = 0xff;
        hdr[5] = 0xfe;
        a.write_all(&hdr).await.unwrap();
        let mut ms = MessageStream::new();
        let err = ms.read_msg(&mut b, Some(magic)).await.unwrap_err();
        assert!(matches!(err, NetError::Protocol(_)));
    }

    #[tokio::test]
    async fn cancel_safe_partial_header() {
        // Simulate select cancellation: read only part of header, drop future,
        // then complete the message with a new future on the same MessageStream.
        use std::future::Future;
        use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};

        let magic = signet_magic();
        let raw = {
            let m = RawNetworkMessage::new(magic, NetworkMessage::Verack);
            serialize(&m)
        };
        assert_eq!(raw.len(), 24);

        let (mut a, mut b) = duplex(64);
        // Write only 10 bytes first
        a.write_all(&raw[..10]).await.unwrap();

        let mut ms = MessageStream::new();
        // Partial read future — poll until Pending then abandon (simulates cancel)
        {
            unsafe fn clone(p: *const ()) -> RawWaker {
                RawWaker::new(p, &VTABLE)
            }
            unsafe fn wake(_: *const ()) {}
            unsafe fn wake_by_ref(_: *const ()) {}
            unsafe fn drop(_: *const ()) {}
            static VTABLE: RawWakerVTable = RawWakerVTable::new(clone, wake, wake_by_ref, drop);
            let waker = unsafe { Waker::from_raw(RawWaker::new(std::ptr::null(), &VTABLE)) };
            let mut cx = Context::from_waker(&waker);
            let mut fut = std::pin::pin!(ms.read_msg(&mut b, Some(magic)));
            // Drive until we either complete (shouldn't) or park waiting for more bytes.
            for _ in 0..8 {
                match fut.as_mut().poll(&mut cx) {
                    Poll::Pending => break,
                    Poll::Ready(Ok(_)) => panic!("should not complete on partial header"),
                    Poll::Ready(Err(e)) => panic!("unexpected err on partial: {e}"),
                }
            }
            // Abandon the future without completing — buf must retain partial bytes.
            std::mem::drop(fut);
        }
        // Deliver the rest
        a.write_all(&raw[10..]).await.unwrap();
        let msg = ms.read_msg(&mut b, Some(magic)).await.unwrap();
        assert!(matches!(msg.payload(), NetworkMessage::Verack));
        assert_eq!(ms.buffered_len(), 0);
    }

    #[test]
    fn core_limits_documented() {
        assert_eq!(MAX_PROTOCOL_MESSAGE_LENGTH, 4_000_000);
        assert_eq!(MAX_INV_SIZE, 50_000);
        assert_eq!(MAX_HEADERS_RESULTS, 2_000);
        assert_eq!(MAX_LOCATOR_SZ, 101);
        // Stricter than rust-bitcoin's 5MB
        assert!(MAX_PROTOCOL_MESSAGE_LENGTH < bitcoin::p2p::message::MAX_MSG_SIZE);
    }
}
