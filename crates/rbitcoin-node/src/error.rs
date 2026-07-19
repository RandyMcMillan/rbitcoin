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
