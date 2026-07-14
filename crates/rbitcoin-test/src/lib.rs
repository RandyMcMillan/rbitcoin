//! Shared helpers for high-level scenarios.
//!
//! Prefer writing scenarios in `tests/` that exercise public crate APIs.

pub mod chain_fixture;
pub mod mine;

pub use chain_fixture::{
    assert_reconstruct_eq, build_mature_regtest_with_spend, MatureRegtestChain,
};

use std::path::PathBuf;
use tempfile::TempDir;

/// Temporary datadir that is removed when dropped.
pub struct TestDatadir {
    pub dir: TempDir,
}

impl TestDatadir {
    pub fn new() -> std::io::Result<Self> {
        Ok(Self {
            dir: tempfile::tempdir()?,
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
        rbitcoin_wire_cache::crate_name(),
        rbitcoin_consensus::crate_name(),
        rbitcoin_net::crate_name(),
        rbitcoin_rpc::crate_name(),
        rbitcoin_cli::crate_name(),
        rbitcoin_node::crate_name(),
    ]
}
