//! Shared helpers for high-level scenarios.
//!
//! Prefer writing scenarios in `tests/` that exercise public crate APIs.

pub mod chain_fixture;
pub mod mine;

pub use chain_fixture::{
    assert_reconstruct_eq, build_mature_regtest_with_spend, pad_empty_from, MatureRegtestChain,
};

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Once;
use std::time::{SystemTime, UNIX_EPOCH};

static TEMP_SEQ: AtomicU64 = AtomicU64::new(0);
static TEST_HEAD_SCALE: Once = Once::new();

/// Avoid multi‑GiB sparse hash heads in tests unless the operator set a scale.
fn ensure_tiny_hash_heads() {
    TEST_HEAD_SCALE.call_once(|| {
        if std::env::var_os("RBITCOIN_HEAD_SCALE").is_none() {
            // SAFETY: single-threaded once; tests only; key is process-local config.
            std::env::set_var("RBITCOIN_HEAD_SCALE", "tiny");
        }
    });
}

/// Temporary directory removed on drop (replaces the `tempfile` crate).
pub struct TempDir {
    path: PathBuf,
}

impl TempDir {
    pub fn new() -> std::io::Result<Self> {
        ensure_tiny_hash_heads();
        let n = TEMP_SEQ.fetch_add(1, Ordering::Relaxed);
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        let path = std::env::temp_dir().join(format!(
            "rbitcoin-test-{}-{}-{}",
            std::process::id(),
            nanos,
            n
        ));
        std::fs::create_dir_all(&path)?;
        Ok(Self { path })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

/// Temporary datadir that is removed when dropped.
pub struct TestDatadir {
    pub dir: TempDir,
}

impl TestDatadir {
    pub fn new() -> std::io::Result<Self> {
        Ok(Self {
            dir: TempDir::new()?,
        })
    }

    pub fn path(&self) -> PathBuf {
        self.dir.path().to_path_buf()
    }

    pub fn store_path(&self) -> PathBuf {
        self.path().join("store")
    }
}

#[cfg(test)]
mod contributing_policy {
    #[test]
    fn contributing_treats_restating_comments_as_smell() {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../CONTRIBUTING.md");
        let text = std::fs::read_to_string(&path)
            .unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
        let lower = text.to_ascii_lowercase();
        assert!(
            text.contains("Source-code comments are a smell"),
            "principle 7 must state comments are a smell"
        );
        assert!(
            lower.contains("what") && text.contains("not clear"),
            "what-restating smell: the code is not clear"
        );
        assert!(
            lower.contains("why")
                && (lower.contains("signature") || lower.contains("function name")),
            "why-restating smell: names or signatures are not carrying the contract"
        );
        assert!(
            lower.contains("weird") && (lower.contains("library") || lower.contains("framework")),
            "weird-approach smell: language, library, or framework is a poor fit"
        );
        assert!(
            lower.contains("invariant") && (lower.contains("quirk") || lower.contains("safety")),
            "remaining comments are a specific invariant, protocol, SAFETY, or quirk"
        );
        assert!(
            text.contains("No restating `//` comments"),
            "review checklist must reject restating line comments"
        );
    }
}
