use rbitcoin_primitives::ParseNetworkError;
use rbitcoin_store::StoreError;
use std::fmt;
use std::path::PathBuf;

#[derive(Debug)]
pub enum NodeError {
    Config(String),
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
            NodeError::Config(_) => None,
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
}
