use std::fmt;
use std::io;
use std::path::PathBuf;

#[derive(Debug)]
pub enum StoreError {
    Io {
        path: PathBuf,
        source: io::Error,
    },
    BadMagic,
    BadSchema(u16),
    BadKind { expected: u16, got: u16 },
    NotFound,
    InvalidFk,
    NotDirectory(PathBuf),
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

impl fmt::Display for StoreError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StoreError::Io { path, source } => {
                write!(f, "io error at {}: {source}", path.display())
            }
            StoreError::BadMagic => f.write_str("invalid store magic"),
            StoreError::BadSchema(v) => write!(f, "unsupported schema version {v}"),
            StoreError::BadKind { expected, got } => {
                write!(
                    f,
                    "unexpected table kind (expected {expected}, got {got})"
                )
            }
            StoreError::NotFound => f.write_str("record not found"),
            StoreError::InvalidFk => f.write_str("invalid foreign key"),
            StoreError::NotDirectory(p) => {
                write!(f, "store path is not a directory: {}", p.display())
            }
            StoreError::Corrupt(m) => write!(f, "corrupt record: {m}"),
        }
    }
}

impl std::error::Error for StoreError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            StoreError::Io { source, .. } => Some(source),
            _ => None,
        }
    }
}
