use rbitcoin_primitives::ParseNetworkError;
use rbitcoin_store::StoreError;
use std::path::PathBuf;

#[derive(Debug, thiserror::Error)]
pub enum NodeError {
    #[error("configuration error: {0}")]
    Config(String),
    #[error(transparent)]
    Network(#[from] ParseNetworkError),
    #[error("datadir error at {path}: {source}")]
    Datadir {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error(transparent)]
    Store(#[from] StoreError),
}
