use rbitcoin_store::StoreError;
use std::fmt;

#[derive(Debug)]
pub enum ConsensusError {
    Store(StoreError),
    BadHeader(&'static str),
    BadBlock(&'static str),
    BadTx(&'static str),
    Script(String),
    MissingPrevout,
    PrevoutSpent,
    InvalidPow,
    BadPrev,
}

impl fmt::Display for ConsensusError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ConsensusError::Store(e) => write!(f, "store: {e}"),
            ConsensusError::BadHeader(s) => write!(f, "bad header: {s}"),
            ConsensusError::BadBlock(s) => write!(f, "bad block: {s}"),
            ConsensusError::BadTx(s) => write!(f, "bad transaction: {s}"),
            ConsensusError::Script(s) => write!(f, "script verification failed: {s}"),
            ConsensusError::MissingPrevout => f.write_str("missing prevout"),
            ConsensusError::PrevoutSpent => f.write_str("prevout already spent on best chain"),
            ConsensusError::InvalidPow => f.write_str("pow invalid"),
            ConsensusError::BadPrev => f.write_str("unexpected previous header"),
        }
    }
}

impl std::error::Error for ConsensusError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            ConsensusError::Store(e) => Some(e),
            _ => None,
        }
    }
}

impl From<StoreError> for ConsensusError {
    fn from(e: StoreError) -> Self {
        ConsensusError::Store(e)
    }
}
