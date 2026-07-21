//! Memory-mapped relational store (libbitcoin-class tables).
//!
//! Class A bodies are append-oriented. Class B multimaps use mutable hash heads.
//! Class C (confirmed / strong_tx) is tip-mutable for reorgs.
//! Archive epochs + tip wire ring: durable-archive soft/hard zones.

mod array_table;
mod chain;
mod compact;
mod epoch;
mod error;
mod file;
mod ibd_io_policy;
mod hashhead;
mod header_table;
mod point_table;
mod spender_table;
mod scripthash;
mod scripthash_head;
mod scripthash_layout;
mod sharded_hashhead;
mod ibd_utxo;
mod sorted_run;
mod store;
mod tx_table;
mod var_table;

pub use epoch::ArchiveEpoch;
pub use error::StoreError;
pub use file::{
    ensure_nofile_budget, ensure_nofile_budget_at_least, try_set_io_best_effort, try_set_io_idle,
    NOFILE_SOFT_TARGET,
};
pub use ibd_io_policy::{defer_durable_flush, set_defer_durable_flush, shard_pace_enabled};
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
pub use ibd_utxo::{IbdUtxo, DEFAULT_NUM_SLOTS as IBD_UTXO_DEFAULT_SLOTS, VOUT_MAX as IBD_UTXO_VOUT_MAX};
pub use sorted_run::{
    claim_run_for_materialize, crc32, detach_run, list_runs, lookup_key, merge_runs, next_run_path,
    open_run, read_run_body, remove_run, verify_run_body, write_sorted_run, SortedRunPath,
};
pub use store::Store;
pub use tx_table::{
    encode_packed_tx, is_packed_tx_payload, InputRecord, OutputRecord, TxRecord, PACKED_TX_V1,
};

/// Crate identity for diagnostics and coverage scenarios.
pub fn crate_name() -> &'static str {
    "rbitcoin-store"
}
