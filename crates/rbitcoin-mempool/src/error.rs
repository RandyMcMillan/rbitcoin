use std::fmt;
use std::io;
use std::path::PathBuf;

#[derive(Debug)]
pub enum MempoolError {
    Io {
        path: PathBuf,
        source: io::Error,
    },
    BadMagic,
    BadSchema(u16),
    Corrupt(&'static str),
    /// Slot table at capacity after grow/evict attempts — not disk corruption.
    Full,
}

impl fmt::Display for MempoolError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MempoolError::Io { path, source } => write!(f, "io {}: {source}", path.display()),
            MempoolError::BadMagic => f.write_str("mempool bad magic"),
            MempoolError::BadSchema(v) => write!(f, "mempool bad schema {v}"),
            MempoolError::Corrupt(s) => write!(f, "mempool corrupt: {s}"),
            MempoolError::Full => f.write_str("mempool full (no free slots)"),
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::error::Error;
    use std::io;

    #[test]
    fn display_and_source_all_variants() {
        let io_err = MempoolError::io("/tmp/mp", io::Error::new(io::ErrorKind::NotFound, "nope"));
        assert!(format!("{io_err}").contains("io /tmp/mp"));
        assert!(io_err.source().is_some());

        let magic = MempoolError::BadMagic;
        assert_eq!(format!("{magic}"), "mempool bad magic");
        assert!(magic.source().is_none());

        let schema = MempoolError::BadSchema(3);
        assert_eq!(format!("{schema}"), "mempool bad schema 3");
        assert!(schema.source().is_none());

        let corrupt = MempoolError::Corrupt("slot OOB");
        assert_eq!(format!("{corrupt}"), "mempool corrupt: slot OOB");
        assert!(corrupt.source().is_none());

        let full = MempoolError::Full;
        assert_eq!(format!("{full}"), "mempool full (no free slots)");
        assert!(full.source().is_none());
    }
}
