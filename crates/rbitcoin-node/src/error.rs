use rbitcoin_primitives::ParseNetworkError;
use rbitcoin_store::StoreError;
use std::fmt;
use std::path::PathBuf;

/// Core `MAX_FUTURE_BLOCK_TIME` (two hours). Startup refuses a tip beyond this.
pub const MAX_FUTURE_BLOCK_TIME: u64 = 2 * 60 * 60;

/// Core load-abort text (`rpc_blockchain._test_max_future_block_time` FULL_TEXT).
pub const FUTURE_BLOCK_DB_MSG: &str = "The block database contains a block which appears to be from the future. This may be due to your computer's date and time being set incorrectly. Only rebuild the block database if you are sure that your computer's date and time are correct.\nPlease restart with -reindex or -reindex-chainstate to recover.";

pub fn tip_too_far_in_future(tip_time: u32, now: u64) -> bool {
    u64::from(tip_time) > now.saturating_add(MAX_FUTURE_BLOCK_TIME)
}

#[derive(Debug)]
pub enum NodeError {
    Config(String),
    /// Tip time is more than two hours ahead of the node clock (Core load abort).
    FutureTip,
    Network(ParseNetworkError),
    Datadir {
        path: PathBuf,
        source: std::io::Error,
    },
    Store(StoreError),
}

impl fmt::Display for NodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            NodeError::Config(s) => write!(f, "configuration error: {s}"),
            NodeError::FutureTip => write!(f, "{FUTURE_BLOCK_DB_MSG}"),
            NodeError::Network(e) => write!(f, "{e}"),
            NodeError::Datadir { path, source } => {
                write!(f, "datadir error at {}: {source}", path.display())
            }
            NodeError::Store(e) => write!(f, "{e}"),
        }
    }
}

impl std::error::Error for NodeError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            NodeError::Network(e) => Some(e),
            NodeError::Datadir { source, .. } => Some(source),
            NodeError::Store(e) => Some(e),
            NodeError::Config(_) | NodeError::FutureTip => None,
        }
    }
}

impl From<ParseNetworkError> for NodeError {
    fn from(e: ParseNetworkError) -> Self {
        NodeError::Network(e)
    }
}

impl From<StoreError> for NodeError {
    fn from(e: StoreError) -> Self {
        NodeError::Store(e)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rbitcoin_primitives::ParseNetworkError;
    use std::error::Error;
    use std::io;
    use std::path::PathBuf;

    #[test]
    fn display_source_and_from() {
        let cfg = NodeError::Config("bad".into());
        assert_eq!(format!("{cfg}"), "configuration error: bad");
        assert!(cfg.source().is_none());

        let fut = NodeError::FutureTip;
        assert_eq!(format!("{fut}"), FUTURE_BLOCK_DB_MSG);
        assert!(fut.source().is_none());

        let net: NodeError = ParseNetworkError { input: "x".into() }.into();
        assert!(format!("{net}").contains("unknown network"));
        assert!(net.source().is_some());

        let dd = NodeError::Datadir {
            path: PathBuf::from("/nope"),
            source: io::Error::new(io::ErrorKind::NotFound, "missing"),
        };
        assert!(format!("{dd}").contains("datadir error at /nope"));
        assert!(dd.source().is_some());

        let store: NodeError = StoreError::Corrupt("x").into();
        assert!(format!("{store}").contains("x"));
        // StoreError itself is the source for NodeError::Store.
        assert!(store.source().is_some());
    }

    #[test]
    fn tip_too_far_in_future_is_strictly_beyond_two_hours() {
        assert!(!tip_too_far_in_future(1_000, 1_000));
        assert!(!tip_too_far_in_future(
            1_000 + MAX_FUTURE_BLOCK_TIME as u32,
            1_000
        ));
        assert!(tip_too_far_in_future(
            1_000 + MAX_FUTURE_BLOCK_TIME as u32 + 1,
            1_000
        ));
    }
}
