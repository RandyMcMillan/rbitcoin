//! Memory-mapped relational store (libbitcoin-class tables).
//!
//! Class A bodies are append-oriented. Class B multimaps use mutable hash heads.
//! Class C (confirmed / strong_tx) is tip-mutable for reorgs.
//! Archive epochs + tip wire ring: durable-archive soft/hard zones.

mod address_head;
mod array_table;
mod block_queue;
mod bulk_io;
mod chain;
mod compact;
mod epoch;
mod error;
mod file;
mod fuse8_filter;
mod head_resolve_stream;
pub mod head_resolve_stats;
mod idx_body_pipeline;
mod spend_annotate_uring;
mod store_secret;
mod uring_session;
mod ibd_io_policy;
mod hashhead;
mod open_address;
mod header_table;
mod point_table;
mod spender_table;
mod scripthash;
mod scripthash_head;
mod scripthash_layout;
mod segmented_head;
mod sharded_hashhead;
mod sorted_run;
mod store;
mod tx_idx;
mod tx_table;
mod var_table;

pub use epoch::ArchiveEpoch;
pub use error::StoreError;
pub use tx_table::HeadResizeSizeSnapshot;
pub use file::{
    ensure_nofile_budget, ensure_nofile_budget_at_least, NOFILE_SOFT_TARGET,
};
pub use ibd_io_policy::{defer_durable_flush, set_defer_durable_flush};
pub use address_head::{
    bits_for_scale, entry_bytes_for_bits, is_probe_exhausted_error, layout_for_count,
    load_needs_resize, load_needs_roll,
    page_index, probe_depth_stats_snapshot, probe_index, sample_probe_depth_stats,
    AddressHead, HeadLayout, HEAD_LOAD_CEILING,
    HEAD_LOAD_START, HEAD_LOAD_WARN, MAINNET_BITS, MAX_BITS, MAX_PROBE, MIN_BITS, PAGE_SLOTS,
    PAGE_SLOT_BITS, PROBE_DEPTH_WARN, PROBE_REGION_BYTES, TINY_BITS,
};
pub use segmented_head::{
    sample_lookup_stats as sample_head_lookup_stats,
    snapshot_lookup_stats as snapshot_head_lookup_stats, HeadLookupStats, SegmentedTxHead,
    SEGMENT_HEAD_BITS,
};
pub use hashhead::{
    initial_slots_for, HeadRole, HeadScale,
};
pub use sharded_hashhead::{
    shard_count_for_role, shard_count_for_scale, SHARD_COUNT, SHARD_COUNT_TX_SH,
};
pub use header_table::HeaderRecord;
pub use point_table::PointRecord;
pub use scripthash::{
    script_hash, ScriptHashBulkSession, ScriptHashEntry, ScriptHashRecord, ScriptHashTable,
};
pub use scripthash_head::prefix_shard_of;
pub use scripthash_layout::ShHeadValue;
pub use sorted_run::{
    claim_run_for_materialize, crc32, detach_run, for_each_merged_rec, for_each_merged_rec_opts,
    list_materialize_claims, list_runs, lookup_key, merge_runs, merge_runs_to_file,
    next_run_path, open_run, read_run_body, reduce_runs_to_fanin, remove_run, verify_run_body,
    write_sorted_run, SortedRunPath,
};
pub use store::Store;
pub use bulk_io::{bulk_io_workers, io_uring_enabled};
pub use block_queue::{
    BlockQueue, QueuedBlock, DEFAULT_BLOCK_QUEUE_BUDGET_BYTES,
};
pub use store_secret::{StoreSecret, SECRET_FILE, SECRET_LEN};
pub use head_resolve_stats::Sample as HeadResolveSample;
pub use idx_body_pipeline::{
    run_idx_body_pipeline, BodyMode as IdxBodyMode, IdxBodyJob,
};
pub use tx_table::{
    clear_output_spender_fields, decode_packed_tx, decode_packed_tx_outs_with_spender_rels,
    decode_packed_tx_outs_with_spender_rels_secret, decode_packed_tx_with_spender_rels,
    decode_packed_tx_with_spender_rels_secret, denserels_from_packed_records, encode_packed_tx,
    encode_packed_tx_with_secret,
    is_packed_tx_payload, scan_packed_meta_and_prevouts, InputRecord, OutputRecord, TxRecord,
    BODY_PAGE_SIZE, TXID_PAGE_MAX_OFF, next_tx_body_start,
};

/// Crate identity for diagnostics and coverage scenarios.
pub fn crate_name() -> &'static str {
    "rbitcoin-store"
}
