use std::io;

#[derive(Debug, thiserror::Error)]
pub enum NetError {
    #[error("io: {0}")]
    Io(#[from] io::Error),
    #[error("encode: {0}")]
    Encode(String),
    #[error("protocol: {0}")]
    Protocol(&'static str),
    #[error("timeout")]
    Timeout,
    #[error("peer disconnected")]
    Disconnected,
    #[error("message too large ({0} bytes)")]
    MessageTooLarge(usize),
    #[error("wrong network magic")]
    BadMagic,
    #[error("consensus: {0}")]
    Consensus(String),
}
