//! Bitcoin P2P message framing helpers (limits + [`FramedMessage`]).
//!
//! **Transport:** production wire is **BIP324 v2 only** — see [`crate::v2`].
//! This module holds Core-aligned size limits, CPU-heavy encode/decode hints,
//! and the framed-message type used after decrypt.
//!
//! **I/O vs CPU split:** socket tasks obtain a [`FramedMessage`] via
//! [`crate::v2::read_v2_frame`] and run [`FramedMessage::decode`] on a
//! blocking worker. Never deserialize multi‑MB `block` payloads on the async
//! I/O worker.

use bitcoin::consensus::deserialize;
use bitcoin::p2p::message::{CommandString, NetworkMessage, RawNetworkMessage};
use bitcoin::p2p::Magic;

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

// ── Encode cost hints ─────────────────────────────────────────────────────

/// Whether encoding this payload is heavy enough to keep off async I/O workers.
#[inline]
pub fn encode_is_cpu_heavy(payload: &NetworkMessage) -> bool {
    match payload {
        NetworkMessage::Block(_) | NetworkMessage::Headers(_) => true,
        NetworkMessage::Inv(v) | NetworkMessage::GetData(v) | NetworkMessage::NotFound(v) => {
            v.len() > 64
        }
        _ => false,
    }
}

// ── Framed message (I/O result without payload deserialize) ───────────────

/// One fully framed P2P message: header fields + raw payload bytes.
///
/// Produced on the socket task; [`FramedMessage::decode`] is CPU and must run
/// off the async I/O worker (blocking pool / rayon / dedicated thread).
#[derive(Debug, Clone)]
pub struct FramedMessage {
    pub magic: Magic,
    /// 12-byte null-padded command (wire form).
    pub command: [u8; 12],
    pub checksum: [u8; 4],
    pub payload: Vec<u8>,
}

impl FramedMessage {
    #[inline]
    pub fn is_block(&self) -> bool {
        self.command == *b"block\0\0\0\0\0\0\0"
    }

    #[inline]
    pub fn is_headers(&self) -> bool {
        self.command == *b"headers\0\0\0\0\0"
    }

    #[inline]
    pub fn is_ping(&self) -> bool {
        self.command == *b"ping\0\0\0\0\0\0\0\0"
    }

    #[inline]
    pub fn is_notfound(&self) -> bool {
        self.command == *b"notfound\0\0\0\0"
    }

    /// Cheap ping nonce extract (8-byte LE payload). No full message deserialize.
    pub fn ping_nonce(&self) -> Option<u64> {
        if !self.is_ping() || self.payload.len() < 8 {
            return None;
        }
        Some(u64::from_le_bytes(self.payload[..8].try_into().ok()?))
    }

    /// Block hash from the wire header (first 80 payload bytes) — no full deserialize.
    ///
    /// Used so IBD can free getdata in-flight as soon as the TCP frame is complete,
    /// while `block` payload decode still runs on the blocking pool. Waiting for
    /// full deserialize to free slots made healthy peers look stalled (socket idle
    /// with `in_flight` still full).
    pub fn block_hash_from_header(&self) -> Option<bitcoin::BlockHash> {
        if !self.is_block() || self.payload.len() < 80 {
            return None;
        }
        use bitcoin::hashes::{sha256d, Hash as _};
        let header = &self.payload[..80];
        let dig = sha256d::Hash::hash(header);
        Some(bitcoin::BlockHash::from_byte_array(dig.to_byte_array()))
    }

    /// CPU-heavy: checksum + payload deserialize into [`RawNetworkMessage`].
    pub fn decode(self) -> RawNetworkMessage {
        let mut full = Vec::with_capacity(24 + self.payload.len());
        full.extend_from_slice(self.magic.to_bytes().as_ref());
        full.extend_from_slice(&self.command);
        full.extend_from_slice(&(self.payload.len() as u32).to_le_bytes());
        full.extend_from_slice(&self.checksum);
        full.extend_from_slice(&self.payload);

        match deserialize::<RawNetworkMessage>(&full) {
            Ok(msg) => msg,
            Err(_e) => {
                // Real peers send extensions/padding that can trip strict payload
                // checks (e.g. "extra bytes after network message payload").
                // Bytes are already framed correctly — surface as Unknown.
                let cmd = command_from_header(&self.command);
                RawNetworkMessage::new(
                    self.magic,
                    NetworkMessage::Unknown {
                        command: cmd,
                        payload: self.payload,
                    },
                )
            }
        }
    }

    /// Commands whose decode cost should never run on an async I/O worker.
    #[inline]
    pub fn decode_is_cpu_heavy(&self) -> bool {
        self.is_block() || self.is_headers() || self.is_notfound()
    }
}

/// Core: command is ASCII letters, null-padded.
pub(crate) fn command_bytes_ok(cmd12: &[u8]) -> bool {
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

#[cfg(test)]
mod tests {
    use super::*;
    use bitcoin::blockdata::constants::genesis_block;
    use bitcoin::consensus::serialize;
    use bitcoin::Network;

    fn signet_magic() -> Magic {
        Magic::from(Network::Signet)
    }

    #[test]
    fn frame_decode_verack_via_synthetic() {
        use bitcoin::hashes::Hash as _;
        let magic = signet_magic();
        let payload = Vec::<u8>::new();
        let dig = bitcoin::hashes::sha256d::Hash::hash(&payload);
        let ba = dig.to_byte_array();
        let frame = FramedMessage {
            magic,
            command: *b"verack\0\0\0\0\0\0",
            checksum: [ba[0], ba[1], ba[2], ba[3]],
            payload,
        };
        assert!(!frame.decode_is_cpu_heavy());
        assert!(matches!(frame.decode().payload(), NetworkMessage::Verack));
    }

    #[test]
    fn block_hash_from_header_matches_full_block() {
        use bitcoin::hashes::Hash as _;
        let magic = Magic::from(Network::Bitcoin);
        let genesis = genesis_block(Network::Bitcoin);
        let want = genesis.block_hash();
        let payload = serialize(&genesis);
        let dig = bitcoin::hashes::sha256d::Hash::hash(&payload);
        let ba = dig.to_byte_array();
        let frame = FramedMessage {
            magic,
            command: *b"block\0\0\0\0\0\0\0",
            checksum: [ba[0], ba[1], ba[2], ba[3]],
            payload,
        };
        assert!(frame.is_block());
        assert_eq!(frame.block_hash_from_header().expect("header hash"), want);
        match frame.decode().payload() {
            NetworkMessage::Block(b) => assert_eq!(b.block_hash(), want),
            _ => panic!("expected block"),
        }
    }

    #[test]
    fn command_bytes_ok_accepts_null_padded() {
        assert!(command_bytes_ok(b"version\0\0\0\0\0"));
        assert!(command_bytes_ok(b"ping\0\0\0\0\0\0\0\0"));
        assert!(!command_bytes_ok(b"\xff\xfe\0\0\0\0\0\0\0\0\0\0"));
        assert!(!command_bytes_ok(b"ping\0x\0\0\0\0\0\0\0")); // non-zero after null
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
