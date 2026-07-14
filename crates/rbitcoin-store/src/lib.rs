//! Memory-mapped relational store (libbitcoin-class tables).
//!
//! Class A bodies are append-oriented. Class B multimaps use mutable hash heads.
//! Class C (confirmed / strong_tx) is tip-mutable for reorgs.
//! Archive epochs + tip wire ring: durable-archive soft/hard zones.

mod array_table;
mod chain;
mod epoch;
mod error;
mod file;
mod hashhead;
mod header_table;
mod point_table;
mod scripthash;
mod store;
mod tx_table;
mod var_table;

pub use epoch::ArchiveEpoch;
pub use error::StoreError;
pub use header_table::HeaderRecord;
pub use point_table::PointRecord;
pub use scripthash::{script_hash, ScriptHashRecord, ScriptHashTable, UNSPENT};
pub use store::Store;
pub use tx_table::{InputRecord, OutputRecord, TxRecord};

/// Crate identity for diagnostics and coverage scenarios.
pub fn crate_name() -> &'static str {
    "rbitcoin-store"
}
