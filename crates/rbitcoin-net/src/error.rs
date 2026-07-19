use std::fmt;
use std::io;

#[derive(Debug)]
pub enum NetError {
    Io(io::Error),
    Encode(String),
    Protocol(&'static str),
    /// Peer does not speak BIP324 v2 (or closed during v2 handshake).
    /// Production is v2-only: disconnect and do not fall back to v1.
    V1Peer,
    /// BIP324 crypto / session error detail.
    Bip324(String),
    Timeout,
    Disconnected,
    MessageTooLarge(usize),
    BadMagic,
    Consensus(String),
}

impl fmt::Display for NetError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            NetError::Io(e) => write!(f, "io: {e}"),
            NetError::Encode(s) => write!(f, "encode: {s}"),
            NetError::Protocol(s) => write!(f, "protocol: {s}"),
            NetError::V1Peer => f.write_str("peer does not speak BIP324 v2"),
            NetError::Bip324(s) => write!(f, "bip324: {s}"),
            NetError::Timeout => f.write_str("timeout"),
            NetError::Disconnected => f.write_str("peer disconnected"),
            NetError::MessageTooLarge(n) => write!(f, "message too large ({n} bytes)"),
            NetError::BadMagic => f.write_str("wrong network magic"),
            NetError::Consensus(s) => write!(f, "consensus: {s}"),
        }
    }
}

impl std::error::Error for NetError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            NetError::Io(e) => Some(e),
            _ => None,
        }
    }
}

impl From<io::Error> for NetError {
    fn from(e: io::Error) -> Self {
        NetError::Io(e)
    }
}
