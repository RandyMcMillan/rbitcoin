//! Memory-mapped relational store (libbitcoin-class tables).
//!
//! Class A bodies are append-oriented. Class B multimaps use mutable hash heads.
//! Durability epochs land in a later phase; v0 is crash-rebuildable best-effort.

mod error;
mod file;
mod hashhead;
mod header_table;
mod point_table;
mod store;
mod tx_table;

pub use error::StoreError;
pub use header_table::HeaderRecord;
pub use point_table::PointRecord;
pub use store::Store;
pub use tx_table::{OutputRecord, TxRecord};

/// Crate identity for diagnostics and coverage scenarios.
pub fn crate_name() -> &'static str {
    "rbitcoin-store"
}
