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

#[cfg(test)]
mod tests {
    use super::*;
    use std::error::Error;

    #[test]
    fn display_and_source_cover_all_variants() {
        let store = ConsensusError::Store(StoreError::NotFound);
        assert!(store.to_string().contains("store:"));
        assert!(store.source().is_some());

        let cases: &[(ConsensusError, &str)] = &[
            (ConsensusError::BadHeader("bits"), "bad header: bits"),
            (ConsensusError::BadBlock("empty"), "bad block: empty"),
            (ConsensusError::BadTx("fee"), "bad transaction: fee"),
            (
                ConsensusError::Script("sig".into()),
                "script verification failed: sig",
            ),
            (ConsensusError::MissingPrevout, "missing prevout"),
            (
                ConsensusError::PrevoutSpent,
                "prevout already spent on best chain",
            ),
            (ConsensusError::InvalidPow, "pow invalid"),
            (ConsensusError::BadPrev, "unexpected previous header"),
        ];
        for (err, needle) in cases {
            assert_eq!(err.to_string(), *needle);
            assert!(err.source().is_none());
        }
    }

    #[test]
    fn from_store_error() {
        let e: ConsensusError = StoreError::NotFound.into();
        assert!(matches!(e, ConsensusError::Store(StoreError::NotFound)));
    }
}
