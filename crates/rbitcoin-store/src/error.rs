use std::io;
use std::path::PathBuf;

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("io error at {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error("invalid store magic")]
    BadMagic,
    #[error("unsupported schema version {0}")]
    BadSchema(u16),
    #[error("unexpected table kind (expected {expected}, got {got})")]
    BadKind { expected: u16, got: u16 },
    #[error("record not found")]
    NotFound,
    #[error("invalid foreign key")]
    InvalidFk,
    #[error("store path is not a directory: {0}")]
    NotDirectory(PathBuf),
    #[error("corrupt record: {0}")]
    Corrupt(&'static str),
}

impl StoreError {
    pub fn io(path: impl Into<PathBuf>, source: io::Error) -> Self {
        StoreError::Io {
            path: path.into(),
            source,
        }
    }
}
