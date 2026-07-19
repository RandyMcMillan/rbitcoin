use std::fmt;
use std::io;
use std::path::PathBuf;

#[derive(Debug)]
pub enum MempoolError {
    Io { path: PathBuf, source: io::Error },
    BadMagic,
    BadSchema(u16),
    Corrupt(&'static str),
}

impl fmt::Display for MempoolError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MempoolError::Io { path, source } => write!(f, "io {}: {source}", path.display()),
            MempoolError::BadMagic => f.write_str("mempool bad magic"),
            MempoolError::BadSchema(v) => write!(f, "mempool bad schema {v}"),
            MempoolError::Corrupt(s) => write!(f, "mempool corrupt: {s}"),
        }
    }
}

impl std::error::Error for MempoolError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            MempoolError::Io { source, .. } => Some(source),
            _ => None,
        }
    }
}

impl MempoolError {
    pub(crate) fn io(path: impl Into<PathBuf>, source: io::Error) -> Self {
        MempoolError::Io {
            path: path.into(),
            source,
        }
    }
}
