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
    /// Soft capacity (e.g. block_queue absolute byte ceiling) — not data corruption.
    /// Caller should buffer in RAM and stop new requests; never spam-log.
    BudgetFull(&'static str),
    /// Cooperative abort (SIGINT / IBD stop) — not data corruption.
    Cancelled(&'static str),
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
            StoreError::BudgetFull(m) => write!(f, "budget full: {m}"),
            StoreError::Cancelled(m) => write!(f, "cancelled: {m}"),
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::error::Error;

    #[test]
    fn display_and_source_arms() {
        let io = StoreError::io("/tmp/x", io::Error::new(io::ErrorKind::NotFound, "nope"));
        let s = io.to_string();
        assert!(s.contains("io error"));
        assert!(s.contains("/tmp/x"));
        assert!(io.source().is_some());

        let arms: Vec<StoreError> = vec![
            StoreError::BadMagic,
            StoreError::BadSchema(9),
            StoreError::BadKind {
                expected: 1,
                got: 2,
            },
            StoreError::NotFound,
            StoreError::InvalidFk,
            StoreError::NotDirectory(PathBuf::from("/not/a/dir")),
            StoreError::Corrupt("broken"),
            StoreError::BudgetFull("block_queue"),
            StoreError::Cancelled("stop"),
        ];
        let texts: Vec<String> = arms.iter().map(|e| e.to_string()).collect();
        assert_eq!(texts[0], "invalid store magic");
        assert!(texts[1].contains("unsupported schema version 9"));
        assert!(texts[2].contains("expected 1"));
        assert!(texts[2].contains("got 2"));
        assert_eq!(texts[3], "record not found");
        assert_eq!(texts[4], "invalid foreign key");
        assert!(texts[5].contains("not a directory"));
        assert!(texts[6].contains("corrupt record: broken"));
        assert!(texts[7].contains("budget full: block_queue"));
        assert!(texts[8].contains("cancelled: stop"));
        for e in &arms {
            assert!(e.source().is_none());
        }
    }
}
