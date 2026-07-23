//! Memory-mapped relational store (libbitcoin-class tables).
//!
//! Class A bodies are append-oriented. Class B multimaps use mutable hash heads.
//! Class C (confirmed / strong_tx) is tip-mutable for reorgs.
//! Archive epochs + tip wire ring: durable-archive soft/hard zones.

mod address_head;
mod array_table;
mod bulk_io;
mod chain;
mod compact;
mod epoch;
mod error;
mod file;
pub mod head_resolve_stats;
mod ibd_io_policy;
mod hashhead;
mod open_address;
mod header_table;
mod mlock;
mod point_table;
mod spender_table;
mod scripthash;
mod scripthash_head;
mod scripthash_layout;
mod sharded_hashhead;
mod sorted_run;
mod store;
mod tx_table;
mod var_table;

pub use epoch::ArchiveEpoch;
pub use error::StoreError;
pub use file::{
    ensure_memlock_budget, ensure_nofile_budget, ensure_nofile_budget_at_least,
    try_set_io_best_effort, try_set_io_idle, NOFILE_SOFT_TARGET,
};
pub use mlock::{MlockRange, MlockTable};
pub use ibd_io_policy::{defer_durable_flush, set_defer_durable_flush};
pub use address_head::{
    bits_for_scale, entry_bytes_for_bits, load_needs_resize, probe_index, AddressHead,
    HeadLayout, HEAD_LOAD_CEILING, HEAD_LOAD_START, HEAD_LOAD_WARN, MAINNET_BITS, MAX_BITS,
    TINY_BITS,
};
pub use hashhead::{
    initial_slots_for, HeadRole, HeadScale,
};
pub use sharded_hashhead::{
    shard_count_for_role, shard_count_for_scale, SHARD_COUNT, SHARD_COUNT_TX_SH,
};
pub use header_table::HeaderRecord;
pub use point_table::PointRecord;
pub use scripthash::{script_hash, ScriptHashEntry, ScriptHashRecord, ScriptHashTable};
pub use scripthash_layout::ShHeadValue;
pub use sorted_run::{
    claim_run_for_materialize, crc32, detach_run, list_runs, lookup_key, merge_runs, next_run_path,
    open_run, read_run_body, remove_run, verify_run_body, write_sorted_run, SortedRunPath,
};
pub use store::Store;
pub use bulk_io::bulk_io_workers;
pub use head_resolve_stats::Sample as HeadResolveSample;
pub use tx_table::{
    encode_packed_tx, is_packed_tx_payload, scan_packed_meta_and_prevouts, InputRecord,
    OutputRecord, TxRecord, PACKED_TX_V1,
};

/// Crate identity for diagnostics and coverage scenarios.
pub fn crate_name() -> &'static str {
    "rbitcoin-store"
}
