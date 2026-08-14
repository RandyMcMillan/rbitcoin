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

/// Touch all workspace crate identity symbols so placeholders stay reachable.
pub fn smoke_crate_names() -> Vec<&'static str> {
    vec![
        rbitcoin_store::crate_name(),
        rbitcoin_query::crate_name(),
        rbitcoin_consensus::crate_name(),
        rbitcoin_net::crate_name(),
        rbitcoin_rpc::crate_name(),
        rbitcoin_electrum::crate_name(),
        rbitcoin_cli::crate_name(),
        rbitcoin_node::crate_name(),
    ]
}
