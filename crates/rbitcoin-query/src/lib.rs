//! Domain query layer over [`rbitcoin_store::Store`].

mod archive;
mod archive_txid_sticky;
mod catchup;
mod chain_view;
mod confirm_parent_cache;
mod connect;
mod confirm_load;
mod reconstruct;
mod run_builder_core;
mod scripthash;
mod sh_builder;
mod wave_prevout;

use bitcoin::absolute::LockTime;
use bitcoin::block::{Header as BlockHeader, Version as BlockVersion};
use bitcoin::hashes::Hash;
use bitcoin::transaction::Version as TxVersion;
use bitcoin::{
    Amount, Block, BlockHash, CompactTarget, OutPoint, ScriptBuf, Sequence, Transaction, TxIn,
    TxMerkleNode, TxOut, Witness,
};
use rbitcoin_primitives::{Fk, Height};
use rbitcoin_store::{
    script_hash, HeaderRecord, InputRecord, OutputRecord, PointRecord, ScriptHashRecord, Store,
    StoreError, TxRecord,
};
use std::collections::{BTreeMap, HashMap};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::sync::Mutex;

pub type QueryError = StoreError;

pub use catchup::IndexMode;
pub use confirm_parent_cache::{
    runway_batch_from_env, runway_depth_from_env, runway_headroom_from_env,
    confirm_mlock_from_env, runway_pin_near_from_env, thin_create_fk_only_from_env,
    DEFAULT_RUNWAY_BATCH as RUNWAY_BATCH, DEFAULT_RUNWAY_DEPTH as RUNWAY_DEPTH,
    DEFAULT_RUNWAY_HEADROOM as RUNWAY_HEADROOM, DEFAULT_RUNWAY_PIN_NEAR as RUNWAY_PIN_NEAR,
    MAX_RUNWAY_DEPTH, MIN_RUNWAY_DEPTH,
};
pub use connect::ConfirmPrepared;
pub use confirm_load::ConfirmLoadStats;
pub use scripthash::{
    ScriptHashBalance, ScriptHashHistoryItem, ScriptHashOutpoint, ScriptHashUtxo,
};
pub use wave_prevout::WavePrevoutCache;

/// Confirm load Class A / parent-pin window counters (IBD ~5s sampler).
///
/// Accrued by `load_confirm_parents` (now called inline from confirm load).
/// Pair with [`Query::parent_runway_perf_snapshot`] for cache watermarks.
pub mod confirm_load_stats {
    use std::sync::atomic::{AtomicU64, Ordering};

    /// Wall time in `load_confirm_parents`.
    pub static NS: AtomicU64 = AtomicU64::new(0);
    pub static BLOCKS: AtomicU64 = AtomicU64::new(0);
    pub static UTXO_PARENTS: AtomicU64 = AtomicU64::new(0);
    pub static CREATES: AtomicU64 = AtomicU64::new(0);
    pub static ALREADY_READY: AtomicU64 = AtomicU64::new(0);
    pub static PARENT_UNIQUE: AtomicU64 = AtomicU64::new(0);
    /// Pin loop: already-stashed outs in by_fk (skip store decode).
    pub static PIN_ALREADY_CACHED: AtomicU64 = AtomicU64::new(0);
    /// Pin filled from runway `by_body` (no Class A re-decode).
    pub static PIN_RUNWAY_BODY: AtomicU64 = AtomicU64::new(0);
    /// Pin that had to load from store.
    pub static PIN_NEW: AtomicU64 = AtomicU64::new(0);
    pub static PARENT_CACHE_HITS: AtomicU64 = AtomicU64::new(0);
    pub static FULL_TX_READS: AtomicU64 = AtomicU64::new(0);
    pub static BODY_TX_READS: AtomicU64 = AtomicU64::new(0);
    pub static MISSING_PARENTS: AtomicU64 = AtomicU64::new(0);
    /// Phase nanoseconds (sum over calls this window).
    pub static HEADER_NS: AtomicU64 = AtomicU64::new(0);
    pub static BODY_MLOCK_NS: AtomicU64 = AtomicU64::new(0);
    pub static BODY_DECODE_NS: AtomicU64 = AtomicU64::new(0);
    pub static THIN_NS: AtomicU64 = AtomicU64::new(0);
    /// Thin sub-phases (collect unique / runway map / head probe / edge walk).
    pub static THIN_COLLECT_NS: AtomicU64 = AtomicU64::new(0);
    pub static THIN_RUNWAY_NS: AtomicU64 = AtomicU64::new(0);
    pub static THIN_HEAD_NS: AtomicU64 = AtomicU64::new(0);
    pub static THIN_EDGE_NS: AtomicU64 = AtomicU64::new(0);
    pub static PARENT_PIN_NS: AtomicU64 = AtomicU64::new(0);
    pub static CACHE_PUT_NS: AtomicU64 = AtomicU64::new(0);
    /// Durable `tx.head` probes during thin resolve.
    pub static HEAD_LOOKUPS: AtomicU64 = AtomicU64::new(0);
    pub static HEAD_HITS: AtomicU64 = AtomicU64::new(0);
    /// Body-page mlock syscalls vs already-pinned skips.
    pub static MLOCK_SYSCALLS: AtomicU64 = AtomicU64::new(0);
    pub static MLOCK_SKIPPED: AtomicU64 = AtomicU64::new(0);
    /// Thin edges classified: same-batch / runway / head / coinbase / miss.
    pub static EDGE_SAME_BATCH: AtomicU64 = AtomicU64::new(0);
    pub static EDGE_RUNWAY: AtomicU64 = AtomicU64::new(0);
    pub static EDGE_HEAD: AtomicU64 = AtomicU64::new(0);
    pub static EDGE_COINBASE: AtomicU64 = AtomicU64::new(0);
    pub static EDGE_STICKY: AtomicU64 = AtomicU64::new(0);
    pub static STICKY_HITS: AtomicU64 = AtomicU64::new(0);

    /// One sampler snapshot (all counters reset).
    #[derive(Debug, Default, Clone, Copy)]
    pub struct Sample {
        pub ns: u64,
        pub blocks: u64,
        pub utxo_parents: u64,
        pub creates: u64,
        pub already_ready: u64,
        pub parent_unique: u64,
        pub pin_already_cached: u64,
        pub pin_runway_body: u64,
        pub pin_new: u64,
        pub cache_hits: u64,
        pub body_tx: u64,
        pub parent_tx: u64,
        pub missing: u64,
        pub header_ns: u64,
        pub body_mlock_ns: u64,
        pub body_decode_ns: u64,
        pub thin_ns: u64,
        pub thin_collect_ns: u64,
        pub thin_runway_ns: u64,
        pub thin_head_ns: u64,
        pub thin_edge_ns: u64,
        pub parent_pin_ns: u64,
        pub cache_put_ns: u64,
        pub head_lookups: u64,
        pub head_hits: u64,
        pub mlock_syscalls: u64,
        pub mlock_skipped: u64,
        pub edge_same_batch: u64,
        pub edge_runway: u64,
        pub edge_head: u64,
        pub edge_coinbase: u64,
        pub edge_sticky: u64,
        pub sticky_hits: u64,
    }

    pub fn sample_and_reset() -> Sample {
        Sample {
            ns: NS.swap(0, Ordering::Relaxed),
            blocks: BLOCKS.swap(0, Ordering::Relaxed),
            utxo_parents: UTXO_PARENTS.swap(0, Ordering::Relaxed),
            creates: CREATES.swap(0, Ordering::Relaxed),
            already_ready: ALREADY_READY.swap(0, Ordering::Relaxed),
            parent_unique: PARENT_UNIQUE.swap(0, Ordering::Relaxed),
            pin_already_cached: PIN_ALREADY_CACHED.swap(0, Ordering::Relaxed),
            pin_runway_body: PIN_RUNWAY_BODY.swap(0, Ordering::Relaxed),
            pin_new: PIN_NEW.swap(0, Ordering::Relaxed),
            cache_hits: PARENT_CACHE_HITS.swap(0, Ordering::Relaxed),
            body_tx: BODY_TX_READS.swap(0, Ordering::Relaxed),
            parent_tx: FULL_TX_READS.swap(0, Ordering::Relaxed),
            missing: MISSING_PARENTS.swap(0, Ordering::Relaxed),
            header_ns: HEADER_NS.swap(0, Ordering::Relaxed),
            body_mlock_ns: BODY_MLOCK_NS.swap(0, Ordering::Relaxed),
            body_decode_ns: BODY_DECODE_NS.swap(0, Ordering::Relaxed),
            thin_ns: THIN_NS.swap(0, Ordering::Relaxed),
            thin_collect_ns: THIN_COLLECT_NS.swap(0, Ordering::Relaxed),
            thin_runway_ns: THIN_RUNWAY_NS.swap(0, Ordering::Relaxed),
            thin_head_ns: THIN_HEAD_NS.swap(0, Ordering::Relaxed),
            thin_edge_ns: THIN_EDGE_NS.swap(0, Ordering::Relaxed),
            parent_pin_ns: PARENT_PIN_NS.swap(0, Ordering::Relaxed),
            cache_put_ns: CACHE_PUT_NS.swap(0, Ordering::Relaxed),
            head_lookups: HEAD_LOOKUPS.swap(0, Ordering::Relaxed),
            head_hits: HEAD_HITS.swap(0, Ordering::Relaxed),
            mlock_syscalls: MLOCK_SYSCALLS.swap(0, Ordering::Relaxed),
            mlock_skipped: MLOCK_SKIPPED.swap(0, Ordering::Relaxed),
            edge_same_batch: EDGE_SAME_BATCH.swap(0, Ordering::Relaxed),
            edge_runway: EDGE_RUNWAY.swap(0, Ordering::Relaxed),
            edge_head: EDGE_HEAD.swap(0, Ordering::Relaxed),
            edge_coinbase: EDGE_COINBASE.swap(0, Ordering::Relaxed),
            edge_sticky: EDGE_STICKY.swap(0, Ordering::Relaxed),
            sticky_hits: STICKY_HITS.swap(0, Ordering::Relaxed),
        }
    }

    #[inline]
    pub(crate) fn note(st: &crate::confirm_load::ConfirmLoadStats, ns: u64) {
        if ns > 0 {
            NS.fetch_add(ns, Ordering::Relaxed);
        }
        macro_rules! add {
            ($field:ident, $atom:ident) => {
                if st.$field > 0 {
                    $atom.fetch_add(st.$field as u64, Ordering::Relaxed);
                }
            };
        }
        add!(blocks, BLOCKS);
        add!(utxo_parents, UTXO_PARENTS);
        add!(creates_registered, CREATES);
        add!(already_ready, ALREADY_READY);
        add!(parent_unique, PARENT_UNIQUE);
        add!(pin_already_cached, PIN_ALREADY_CACHED);
        add!(pin_runway_body, PIN_RUNWAY_BODY);
        add!(pin_new, PIN_NEW);
        add!(parent_cache_hits, PARENT_CACHE_HITS);
        add!(full_tx_reads, FULL_TX_READS);
        add!(body_tx_reads, BODY_TX_READS);
        add!(missing_parents, MISSING_PARENTS);
        add!(header_ns, HEADER_NS);
        add!(body_mlock_ns, BODY_MLOCK_NS);
        add!(body_decode_ns, BODY_DECODE_NS);
        add!(thin_ns, THIN_NS);
        add!(thin_collect_ns, THIN_COLLECT_NS);
        add!(thin_runway_ns, THIN_RUNWAY_NS);
        add!(thin_head_ns, THIN_HEAD_NS);
        add!(thin_edge_ns, THIN_EDGE_NS);
        add!(parent_pin_ns, PARENT_PIN_NS);
        add!(cache_put_ns, CACHE_PUT_NS);
        add!(head_lookups, HEAD_LOOKUPS);
        add!(head_hits, HEAD_HITS);
        add!(mlock_syscalls, MLOCK_SYSCALLS);
        add!(mlock_skipped, MLOCK_SKIPPED);
        add!(edge_same_batch, EDGE_SAME_BATCH);
        add!(edge_runway, EDGE_RUNWAY);
        add!(edge_head, EDGE_HEAD);
        add!(edge_coinbase, EDGE_COINBASE);
        add!(edge_sticky, EDGE_STICKY);
        add!(sticky_hits, STICKY_HITS);
    }
}

/// Archive create_fk resolve counters (reset by the IBD ~5s sampler).
///
/// Phase 1.5: decide whether sticky/head still matter and that archive is not
/// the long pole — compare head need vs sticky and resolve_ns vs write_ns.
pub mod archive_resolve_stats {
    use std::sync::atomic::{AtomicU64, Ordering};

    /// Headers (blocks) packed this window.
    pub static BLOCKS: AtomicU64 = AtomicU64::new(0);
    /// Unique external prev_txids that needed resolve (not same-batch / coinbase).
    pub static EXT_NEED: AtomicU64 = AtomicU64::new(0);
    /// Of EXT_NEED, hits in writer sticky (before head).
    pub static STICKY_HIT: AtomicU64 = AtomicU64::new(0);
    /// Sticky misses probed on durable tx.head.
    pub static HEAD_NEED: AtomicU64 = AtomicU64::new(0);
    /// Head probes that returned a create_fk.
    pub static HEAD_HIT: AtomicU64 = AtomicU64::new(0);
    /// Non-coinbase inputs stamped from same-mega-batch map.
    pub static BATCH_STAMP: AtomicU64 = AtomicU64::new(0);
    /// Non-coinbase inputs stamped from sticky/head resolve map.
    pub static RESOLVED_STAMP: AtomicU64 = AtomicU64::new(0);
    /// Wall ns of resolve (sticky + head) only, not body put.
    pub static RESOLVE_NS: AtomicU64 = AtomicU64::new(0);

    #[derive(Debug, Default, Clone, Copy)]
    pub struct Sample {
        pub blocks: u64,
        pub ext_need: u64,
        pub sticky_hit: u64,
        pub head_need: u64,
        pub head_hit: u64,
        pub batch_stamp: u64,
        pub resolved_stamp: u64,
        pub resolve_ns: u64,
    }

    pub fn sample_and_reset() -> Sample {
        Sample {
            blocks: BLOCKS.swap(0, Ordering::Relaxed),
            ext_need: EXT_NEED.swap(0, Ordering::Relaxed),
            sticky_hit: STICKY_HIT.swap(0, Ordering::Relaxed),
            head_need: HEAD_NEED.swap(0, Ordering::Relaxed),
            head_hit: HEAD_HIT.swap(0, Ordering::Relaxed),
            batch_stamp: BATCH_STAMP.swap(0, Ordering::Relaxed),
            resolved_stamp: RESOLVED_STAMP.swap(0, Ordering::Relaxed),
            resolve_ns: RESOLVE_NS.swap(0, Ordering::Relaxed),
        }
    }

    #[inline]
    pub(crate) fn note(
        blocks: u64,
        ext_need: u64,
        sticky_hit: u64,
        head_need: u64,
        head_hit: u64,
        batch_stamp: u64,
        resolved_stamp: u64,
        resolve_ns: u64,
    ) {
        if blocks > 0 {
            BLOCKS.fetch_add(blocks, Ordering::Relaxed);
        }
        if ext_need > 0 {
            EXT_NEED.fetch_add(ext_need, Ordering::Relaxed);
        }
        if sticky_hit > 0 {
            STICKY_HIT.fetch_add(sticky_hit, Ordering::Relaxed);
        }
        if head_need > 0 {
            HEAD_NEED.fetch_add(head_need, Ordering::Relaxed);
        }
        if head_hit > 0 {
            HEAD_HIT.fetch_add(head_hit, Ordering::Relaxed);
        }
        if batch_stamp > 0 {
            BATCH_STAMP.fetch_add(batch_stamp, Ordering::Relaxed);
        }
        if resolved_stamp > 0 {
            RESOLVED_STAMP.fetch_add(resolved_stamp, Ordering::Relaxed);
        }
        if resolve_ns > 0 {
            RESOLVE_NS.fetch_add(resolve_ns, Ordering::Relaxed);
        }
    }
}

/// Class C sub-phase wall times (nanoseconds; reset by the IBD sampler).
///
/// Split so logs can tell strong/height vs scripthash puts vs tip commit.
/// Scripthash subtimers (`SH_*`) break down collect vs durable append steps.
pub mod class_c_phase_stats {
    use std::sync::atomic::{AtomicU64, Ordering};

    pub static STRONG_NS: AtomicU64 = AtomicU64::new(0);
    /// Wall time of the SH worker (collect + append), not including wait for strong.
    pub static SCRIPTHASH_NS: AtomicU64 = AtomicU64::new(0);
    pub static TIP_NS: AtomicU64 = AtomicU64::new(0);

    /// SH: warm create-tx index (first confirm only; should be ~0 after).
    pub static SH_WARM_NS: AtomicU64 = AtomicU64::new(0);
    /// SH: filter which wave txs need create rows.
    pub static SH_FILTER_NS: AtomicU64 = AtomicU64::new(0);
    /// SH: load creates from Class A for new txs.
    pub static SH_COLLECT_NS: AtomicU64 = AtomicU64::new(0);
    /// SH: sort creates by scripthash.
    pub static SH_SORT_NS: AtomicU64 = AtomicU64::new(0);
    /// SH: seed process/durable heads for new scripthash keys.
    pub static SH_SEED_NS: AtomicU64 = AtomicU64::new(0);
    /// SH: encode + body `write_at`.
    pub static SH_BODY_NS: AtomicU64 = AtomicU64::new(0);
    /// SH: `scripthash.head` insert_many.
    pub static SH_HEAD_NS: AtomicU64 = AtomicU64::new(0);
    /// SH: advance height watermark (was set inserts; now near-zero).
    pub static SH_INDEX_NS: AtomicU64 = AtomicU64::new(0);

    /// `(strong, scripthash, tip)` nanoseconds.
    ///
    /// `scripthash` is the **sum of SH substeps** (not a separate end-to-end
    /// timer), so status windows do not invent large `other_ms` when substeps
    /// and wall are sampled on different ticks.
    pub fn sample_and_reset() -> (u64, u64, u64) {
        (
            STRONG_NS.swap(0, Ordering::Relaxed),
            SCRIPTHASH_NS.swap(0, Ordering::Relaxed),
            TIP_NS.swap(0, Ordering::Relaxed),
        )
    }

    /// `(warm, filter, collect, sort, seed, body, head, index)` nanoseconds.
    pub fn sample_sh_sub_and_reset() -> (u64, u64, u64, u64, u64, u64, u64, u64) {
        (
            SH_WARM_NS.swap(0, Ordering::Relaxed),
            SH_FILTER_NS.swap(0, Ordering::Relaxed),
            SH_COLLECT_NS.swap(0, Ordering::Relaxed),
            SH_SORT_NS.swap(0, Ordering::Relaxed),
            SH_SEED_NS.swap(0, Ordering::Relaxed),
            SH_BODY_NS.swap(0, Ordering::Relaxed),
            SH_HEAD_NS.swap(0, Ordering::Relaxed),
            SH_INDEX_NS.swap(0, Ordering::Relaxed),
        )
    }

    /// Accrue a SH substep and the aggregate `SCRIPTHASH_NS` wall (same window).
    #[inline]
    pub(crate) fn add_sh_part(part: &AtomicU64, ns: u64) {
        if ns == 0 {
            return;
        }
        part.fetch_add(ns, Ordering::Relaxed);
        SCRIPTHASH_NS.fetch_add(ns, Ordering::Relaxed);
    }
}

/// Connect prevout resolution counters (reset by the IBD sampler).
pub mod connect_prevout_stats {
    use std::sync::atomic::{AtomicU64, Ordering};

    pub static WAVE_HIT: AtomicU64 = AtomicU64::new(0);
    pub static CLASS_A_HIT: AtomicU64 = AtomicU64::new(0);
    pub static STORE_MISS: AtomicU64 = AtomicU64::new(0);

    /// `(wave_hit, class_a_hit, store_miss)` then reset.
    pub fn sample_and_reset() -> (u64, u64, u64) {
        (
            WAVE_HIT.swap(0, Ordering::Relaxed),
            CLASS_A_HIT.swap(0, Ordering::Relaxed),
            STORE_MISS.swap(0, Ordering::Relaxed),
        )
    }
}

/// Removed light-UTXO diagnostics (stub so IBD sampler still compiles).
pub mod ibd_utxo_stats {
    /// Always zero — light UTXO deleted.
    pub fn sample_rebuilds_and_reset() -> u64 {
        0
    }
    /// Always `(0, 0)`.
    pub fn sample_probe_flush_and_reset() -> (u64, u64) {
        (0, 0)
    }
}

/// Wave-fill sub-phase wall times (nanoseconds; reset by the IBD sampler).
///
/// Breaks down the dominant `wave_fill` recon cost: body vs parent warm vs spent
/// vs coinbase height. Also tracks store IO cost and parent-cache lock wait.
pub mod wave_fill_stats {
    use std::sync::atomic::{AtomicU64, Ordering};

    /// Wave-body txs from Class A → wave map + parent_needed collect.
    pub static BODY_NS: AtomicU64 = AtomicU64::new(0);
    /// External parent load (cache sparse outs or store meta+outputs).
    pub static PARENT_TX_NS: AtomicU64 = AtomicU64::new(0);
    /// External parent output materialization (subset of parent load when split).
    pub static PARENT_OUT_NS: AtomicU64 = AtomicU64::new(0);
    /// Durable / local spent filter on needed parent vouts.
    pub static SPENT_NS: AtomicU64 = AtomicU64::new(0);
    /// Coinbase create-height for parents.
    pub static CB_HEIGHT_NS: AtomicU64 = AtomicU64::new(0);
    /// Wave bodies moved out of ConfirmParentCache (no clone).
    pub static BODY_CACHE_MOVE: AtomicU64 = AtomicU64::new(0);
    /// Wave bodies re-decoded from store (cache miss / not runway-cached).
    pub static BODY_STORE: AtomicU64 = AtomicU64::new(0);
    /// Wall ns spent in store body decode (subset of BODY_NS on miss).
    pub static BODY_STORE_NS: AtomicU64 = AtomicU64::new(0);
    /// Major page faults observed on the confirm thread during store body loads.
    pub static BODY_STORE_MAJFLT: AtomicU64 = AtomicU64::new(0);
    /// Time waiting on ConfirmParentCache mutex (ns).
    pub static CACHE_LOCK_WAIT_NS: AtomicU64 = AtomicU64::new(0);
    /// Thin edges moved from runway stash (batch take).
    pub static THIN_CACHE_MOVE: AtomicU64 = AtomicU64::new(0);
    /// Thin edges rebuilt by walking inputs (stash miss).
    pub static THIN_REBUILD: AtomicU64 = AtomicU64::new(0);

    /// `(body, parent_tx, parent_out, spent, cb_height)` nanoseconds.
    pub fn sample_and_reset() -> (u64, u64, u64, u64, u64) {
        (
            BODY_NS.swap(0, Ordering::Relaxed),
            PARENT_TX_NS.swap(0, Ordering::Relaxed),
            PARENT_OUT_NS.swap(0, Ordering::Relaxed),
            SPENT_NS.swap(0, Ordering::Relaxed),
            CB_HEIGHT_NS.swap(0, Ordering::Relaxed),
        )
    }

    /// `(cache_move, store, thin_move, thin_rebuild)` counts since last sample.
    pub fn sample_counts_and_reset() -> (u64, u64, u64, u64) {
        (
            BODY_CACHE_MOVE.swap(0, Ordering::Relaxed),
            BODY_STORE.swap(0, Ordering::Relaxed),
            THIN_CACHE_MOVE.swap(0, Ordering::Relaxed),
            THIN_REBUILD.swap(0, Ordering::Relaxed),
        )
    }

    /// `(store_body_ns, store_majflt, cache_lock_wait_ns)`.
    pub fn sample_io_and_reset() -> (u64, u64, u64) {
        (
            BODY_STORE_NS.swap(0, Ordering::Relaxed),
            BODY_STORE_MAJFLT.swap(0, Ordering::Relaxed),
            CACHE_LOCK_WAIT_NS.swap(0, Ordering::Relaxed),
        )
    }

    #[inline]
    pub(crate) fn add(part: &AtomicU64, ns: u64) {
        if ns > 0 {
            part.fetch_add(ns, Ordering::Relaxed);
        }
    }

    #[inline]
    pub(crate) fn add_count(part: &AtomicU64, n: u64) {
        if n > 0 {
            part.fetch_add(n, Ordering::Relaxed);
        }
    }
}

/// ContigPark live snapshot (writer thread updates; sampler reads without reset).
pub mod contig_park_stats {
    use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};

    /// Next height the writer may commit.
    pub static NEXT_H: AtomicU32 = AtomicU32::new(0);
    /// Bodies parked (height ≥ next_h).
    pub static PARKED: AtomicUsize = AtomicUsize::new(0);
    /// Contiguous ready prefix length at next_h.
    pub static READY_PREFIX: AtomicUsize = AtomicUsize::new(0);

    pub fn snapshot() -> (u32, usize, usize) {
        (
            NEXT_H.load(Ordering::Relaxed),
            PARKED.load(Ordering::Relaxed),
            READY_PREFIX.load(Ordering::Relaxed),
        )
    }

    pub fn store(next_h: u32, parked: usize, ready: usize) {
        NEXT_H.store(next_h, Ordering::Relaxed);
        PARKED.store(parked, Ordering::Relaxed);
        READY_PREFIX.store(ready, Ordering::Relaxed);
    }
}


/// One transaction to apply when connecting a block.
#[derive(Clone, Debug)]
pub struct TxApply {
    pub tx: TxRecord,
    pub inputs: Vec<InputRecord>,
    pub outputs: Vec<OutputRecord>,
}

/// One header on the best store path after the confirmed tip (IBD resume).
#[derive(Clone, Debug)]
pub struct ResumeWorkEntry {
    pub height: u32,
    pub hash: [u8; 32],
    pub header_fk: Fk,
    /// True if `header_txs` has a body for this header (Class A ready).
    pub has_body: bool,
}

/// Domain query facade used by higher layers (consensus, net, RPC).
pub struct Query {
    store: Store,
    /// When false, archive **and** confirm skip durable Class B point (spend) writes.
    spend_index: std::sync::atomic::AtomicBool,
    /// When false, archive skips durable `tx.head` inserts.
    tx_index: std::sync::atomic::AtomicBool,
    /// Process-local scripthash → body head fk (confirm append path; avoids durable chain walks).
    sh_heads: Mutex<HashMap<[u8; 32], rbitcoin_store::ShHeadValue>>,
    /// Last height whose SH creates were enqueued/written **after tip commit**.
    /// `u64::MAX` = none. Replaces unbounded `sh_tx_indexed` HashSet.
    sh_indexed_through: AtomicU64,
    /// Block-structured confirm parent runway.
    confirm_parents: confirm_parent_cache::ConfirmParentCache,
    /// Archive writer sticky: txid → create_fk for packing spends (cross mega-batch).
    archive_txid_sticky: archive_txid_sticky::ArchiveTxidSticky,
    /// Direct IBD SH: memtable → sorted runs (bulk materialize at tip).
    sh_run: sh_builder::ShRunBuilder,
    /// Explicit [`IndexMode`] (Direct / Tip).
    index_mode_cell: std::sync::atomic::AtomicU8,
    /// Cooperative cancel for in-flight confirm (load waits). Set on IBD
    /// SIGINT teardown so the confirm OS thread aborts waits before process exit.
    confirm_cancel: std::sync::atomic::AtomicBool,
    /// True while the IBD background parent-runway worker is running.
    /// Confirm never last-miles when this is set (waits on ready notify only).
    legacy_load_worker_live: std::sync::atomic::AtomicBool,
}

impl Query {
    pub fn open_or_create(store_path: impl AsRef<Path>) -> Result<Self, QueryError> {
        let store = Store::open_or_create(store_path.as_ref())?;
        // Heal strong_tx / tx_height written above confirmed tip (kill -9 mid Class C).
        // Tip-bound spenders already ignore those rows; this restores is_strong parity.
        let repaired = store.repair_class_c_above_tip()?;
        if repaired > 0 {
            // Use eprintln so node logs still see it before rbitcoin_log is configured.
            eprintln!(
                "rbitcoin: repaired {repaired} Class C tx rows above confirmed tip (partial confirm / kill -9)"
            );
        }
        let store_path = store.path().to_path_buf();
        // SH watermark: on resume, assume 0..=tip already had SH work committed with tip.
        let sh_through = store
            .tip_height()
            .map(|h| h.0 as u64)
            .unwrap_or(u64::MAX);
        let q = Self {
            store,
            spend_index: std::sync::atomic::AtomicBool::new(true),
            tx_index: std::sync::atomic::AtomicBool::new(true),
            sh_heads: Mutex::new(HashMap::new()),
            sh_indexed_through: AtomicU64::new(sh_through),
            confirm_parents: confirm_parent_cache::ConfirmParentCache::from_env(),
            archive_txid_sticky: archive_txid_sticky::ArchiveTxidSticky::from_env(),
            sh_run: sh_builder::ShRunBuilder::new(&store_path),
            // Open as Tip until IBD selects Direct.
            index_mode_cell: std::sync::atomic::AtomicU8::new(IndexMode::Tip as u8),
            confirm_cancel: std::sync::atomic::AtomicBool::new(false),
            legacy_load_worker_live: std::sync::atomic::AtomicBool::new(false),
        };
        // Warm cache from durable head if present (resume with index on).
        // Full body scan is not done here; fresh genesis IBD fills cache as it archives.
        Ok(q)
    }

    /// Request in-flight confirm to abort cooperative waits (IBD SIGINT).
    pub fn request_confirm_cancel(&self) {
        self.confirm_cancel
            .store(true, std::sync::atomic::Ordering::SeqCst);
        // Wake confirm threads blocked on runway ready.
        self.confirm_parents.notify_ready_waiters();
    }

    /// Clear cancel before a new confirm/IBD session.
    pub fn clear_confirm_cancel(&self) {
        self.confirm_cancel
            .store(false, std::sync::atomic::Ordering::SeqCst);
    }

    /// True after [`Self::request_confirm_cancel`] until cleared.
    pub fn confirm_cancelled(&self) -> bool {
        self.confirm_cancel
            .load(std::sync::atomic::Ordering::SeqCst)
    }

    /// IBD parent-runway worker is running (confirm must only wait, never last-mile).
    pub fn set_legacy_load_worker_live(&self, live: bool) {
        self.legacy_load_worker_live
            .store(live, std::sync::atomic::Ordering::SeqCst);
        if !live {
            // Unblock waiters if worker exits while confirm is waiting.
            self.confirm_parents.notify_ready_waiters();
        }
    }

    pub fn legacy_load_worker_live(&self) -> bool {
        self.legacy_load_worker_live
            .load(std::sync::atomic::Ordering::SeqCst)
    }

    /// Last height with SH creates committed (after tip). `None` if empty chain.
    pub(crate) fn sh_indexed_through_height(&self) -> Option<u32> {
        let v = self.sh_indexed_through.load(AtomicOrdering::Acquire);
        if v == u64::MAX {
            None
        } else {
            Some(v as u32)
        }
    }

    /// Advance SH watermark only after Class C tip commit.
    pub(crate) fn set_sh_indexed_through_height(&self, height: Option<u32>) {
        let v = height.map(|h| h as u64).unwrap_or(u64::MAX);
        self.sh_indexed_through
            .store(v, AtomicOrdering::Release);
    }

    /// Resolve txid → fk: runway cache → durable `tx.head`.
    fn lookup_tx_fk(&self, txid: &[u8; 32]) -> Result<Option<Fk>, QueryError> {
        if let Some(fk) = self.confirm_parents.get_by_txid(txid) {
            return Ok(Some(fk));
        }
        if self.tx_index_enabled() {
            // body_txid verify only — avoid full packed decode on probe misses.
            if let Some(fk) = self.store.get_fk_by_txid(txid)? {
                return Ok(Some(fk));
            }
        }
        Ok(None)
    }

    /// Public resolve by txid (runway + durable head).
    pub fn tx_fk_by_txid(&self, txid: &[u8; 32]) -> Result<Option<Fk>, QueryError> {
        self.lookup_tx_fk(txid)
    }

    /// Durable head probe with **body txid only** (no full packed decode).
    pub fn tx_fk_by_txid_store(&self, txid: &[u8; 32]) -> Result<Option<Fk>, QueryError> {
        Ok(self.store.get_fk_by_txid(txid)?)
    }

    pub fn store(&self) -> &Store {
        &self.store
    }

    pub fn confirm_parent_cache(&self) -> &confirm_parent_cache::ConfirmParentCache {
        &self.confirm_parents
    }

    /// No-op compatibility: SH dedupe is a height watermark (`sh_indexed_through`),
    /// not a HashSet warm from durable body.
    pub fn warm_scripthash_create_index(&self) -> Result<(), QueryError> {
        // Align watermark with tip if durable SH exists (tip mode resume).
        if let Some(tip) = self.tip_height() {
            if self.sh_indexed_through_height().is_none() {
                self.set_sh_indexed_through_height(Some(tip.0));
            }
        }
        Ok(())
    }

    /// Enable/disable durable spend-annotation writes on archive **and** confirm
    /// (schema v5 create-out annotations; default on).
    ///
    /// Direct IBD keeps this **on** (confirm batch after Class C). Tip mode
    /// assumes annotations are already complete — no automatic backfill.
    pub fn set_spend_index(&self, enabled: bool) {
        self.spend_index
            .store(enabled, std::sync::atomic::Ordering::SeqCst);
    }

    pub fn spend_index_enabled(&self) -> bool {
        self.spend_index
            .load(std::sync::atomic::Ordering::SeqCst)
    }

    /// Host-friendly process-exit flush (durability for open tables).
    ///
    /// See [`rbitcoin_store::Store::flush_for_shutdown`].
    pub fn flush_for_shutdown(&self) -> Result<(), QueryError> {
        self.store.flush_for_shutdown()
    }
}

impl Query {
    /// True if this outpoint is spent on the **best chain** (durable confirmed-strong).
    ///
    /// Does **not** treat archive-only point rows as spent: Class A may write
    /// edges before Class C; those spenders are not strong yet.
    pub fn is_outpoint_spent(&self, txid: &[u8; 32], vout: u32) -> Result<bool, QueryError> {
        // Prefer runway create_fk + body range (no tx.head / idx).
        if let Some(cfk) = self.confirm_parents.get_by_txid(txid) {
            let range = self.confirm_parents.get_body_range(cfk);
            return Ok(self
                .store
                .has_confirmed_strong_spender_create(cfk, vout, range)?);
        }
        Ok(self.store.has_confirmed_strong_spender(txid, vout)?)
    }

    /// Spentness by known create fk (wave_fill parent path — no head probe).
    pub fn is_outpoint_spent_create(&self, create_fk: Fk, vout: u32) -> Result<bool, QueryError> {
        let range = self.confirm_parents.get_body_range(create_fk);
        Ok(self
            .store
            .has_confirmed_strong_spender_create(create_fk, vout, range)?)
    }

    /// Unspent subset of vouts on a create (batch; one body walk when ranged).
    pub fn unspent_create_vouts(
        &self,
        create_fk: Fk,
        vouts: &[u32],
    ) -> Result<Vec<u32>, QueryError> {
        let range = self.confirm_parents.get_body_range(create_fk);
        Ok(self.store.unspent_create_vouts(create_fk, vouts, range)?)
    }

    /// Enable/disable txid hash-head inserts on archive (default on). Off under
    /// milestone IBD; Class A bodies remain complete via header_txs fk lists.
    pub fn set_tx_index(&self, enabled: bool) {
        self.tx_index
            .store(enabled, std::sync::atomic::Ordering::SeqCst);
    }

    pub fn tx_index_enabled(&self) -> bool {
        self.tx_index
            .load(std::sync::atomic::Ordering::SeqCst)
    }

    /// Archive writer sticky map occupancy `(len, cap)` for IBD diagnostics.
    pub fn archive_txid_sticky_stats(&self) -> (usize, usize) {
        (
            self.archive_txid_sticky.len(),
            self.archive_txid_sticky.cap(),
        )
    }

    /// Rebuild durable `tx.head` from every Class A body (idempotent).
    ///
    /// Prefer **deleting `tx.head` (+ `tx.head.meta`)** and reopening the store:
    /// [`Store::open`] / [`Query::open_or_create`] recreates an empty head and
    /// runs a full rebuild automatically. This method is for in-process recovery
    /// without a reopen (inserts only missing probe entries).
    ///
    /// `on_progress(done_bodies, total_bodies, inserted)` for operator logs.
    pub fn backfill_tx_index(
        &self,
        on_progress: impl FnMut(u64, u64, u64),
    ) -> Result<u64, QueryError> {
        self.store.txs.backfill_head(on_progress)
    }

    /// Class A tx body count (for backfill heuristics / logs).
    pub fn tx_body_count(&self) -> u64 {
        self.store.txs.count()
    }

    /// Durable `tx.head` occupied slots (for backfill heuristics / logs).
    pub fn tx_head_occupied(&self) -> u64 {
        self.store.txs.head_occupied()
    }

    /// Thin scripthash create row count (diagnostic / tip-mode logs).
    pub fn scripthash_entry_count(&self) -> u64 {
        self.store.scripthash.entry_count()
    }

    /// Multi-list spend body node count (diagnostic).
    ///
    /// Schema v5 **sole** spends do not allocate multi-list rows, so this is
    /// often 0 even with full spend annotations — do **not** treat as “points empty.”
    pub fn point_edge_count(&self) -> u64 {
        self.store.spender_list_count()
    }

    /// Rewrite durable spend annotations for every confirmed non-coinbase input.
    ///
    /// **Not** part of tip entry: Direct IBD already annotates on confirm.
    /// Manual recovery only (corrupt/partial annotations). Prefer reindex when
    /// spentness is wrong at scale. When multi-list count is 0, uses bulk
    /// `put_spend_batch` without probe; otherwise probes for idempotency.
    ///
    /// `on_progress(height, tip, txs_so_far, edges_so_far)`.
    /// Returns `(heights_walked, txs_touched)`.
    pub fn backfill_point_spends(
        &self,
        mut on_progress: impl FnMut(u32, u32, u64, u64),
    ) -> Result<(u32, u64), QueryError> {
        let Some(tip) = self.tip_height() else {
            return Ok((0, 0));
        };
        // Empty index → bulk append. Sparse/partial → probe to avoid dups.
        let probe = self.point_edge_count() > 0;
        let mut txs = 0u64;
        let mut edges_total = 0u64;
        const EDGE_BATCH: usize = 8192;
        const PROGRESS_EVERY: u32 = 10_000;
        let mut edge_batch: Vec<([u8; 32], u32, Fk, u32)> = Vec::with_capacity(EDGE_BATCH);
        let mut last_log = 0u32;

        let flush_batch = |batch: &mut Vec<([u8; 32], u32, Fk, u32)>| -> Result<(), QueryError> {
            if batch.is_empty() {
                return Ok(());
            }
            self.store.put_spend_batch(batch)?;
            batch.clear();
            Ok(())
        };

        for h in 0..=tip.0 {
            let height = Height(h);
            let fks = match self.block_tx_fks(height) {
                Ok(f) => f,
                Err(StoreError::NotFound) => continue,
                Err(e) => return Err(e),
            };
            for fk in fks {
                if probe {
                    // Per-tx path with existence probe (partial reindex).
                    self.mark_spends_for_tx(fk, true)?;
                } else {
                    let mut edges = self.collect_spend_edges(fk, false)?;
                    edges_total += edges.len() as u64;
                    if edge_batch.len() + edges.len() > EDGE_BATCH && !edge_batch.is_empty() {
                        flush_batch(&mut edge_batch)?;
                    }
                    if edges.len() >= EDGE_BATCH {
                        // Single fat tx: write on its own.
                        self.store.put_spend_batch(&edges)?;
                    } else {
                        edge_batch.append(&mut edges);
                        if edge_batch.len() >= EDGE_BATCH {
                            flush_batch(&mut edge_batch)?;
                        }
                    }
                }
                txs += 1;
            }
            if h - last_log >= PROGRESS_EVERY || h == tip.0 {
                on_progress(h, tip.0, txs, edges_total + edge_batch.len() as u64);
                last_log = h;
            }
        }
        flush_batch(&mut edge_batch)?;
        Ok((tip.0.saturating_add(1), txs))
    }

    pub fn tip_height(&self) -> Option<Height> {
        self.store.tip_height()
    }

    pub fn tip_header_fk(&self) -> Result<Option<Fk>, QueryError> {
        match self.tip_height() {
            None => Ok(None),
            Some(h) => Ok(self.store.confirmed.get(h)?),
        }
    }

    pub fn put_header(&self, rec: &HeaderRecord) -> Result<Fk, QueryError> {
        self.store.put_header(rec)
    }

    pub fn get_header(&self, fk: Fk) -> Result<HeaderRecord, QueryError> {
        self.store.get_header(fk)
    }

    pub fn get_header_by_hash(
        &self,
        hash: &[u8; 32],
    ) -> Result<Option<(Fk, HeaderRecord)>, QueryError> {
        if let Some(v) = self.confirm_parents.get_header_by_hash(hash) {
            return Ok(Some(v));
        }
        self.store.get_header_by_hash(hash)
    }

    /// Header tx list: runway cache (load) then store.
    pub fn header_tx_fks(
        &self,
        header_fk: Fk,
        hash: Option<&[u8; 32]>,
    ) -> Result<Option<Vec<Fk>>, QueryError> {
        if let Some(h) = hash {
            if let Some(fks) = self.confirm_parents.get_tx_fks_for_hash(h) {
                return Ok(Some(fks));
            }
        }
        Ok(self.store.header_txs.get_list(header_fk)?)
    }

    pub fn put_tx(&self, rec: &TxRecord) -> Result<Fk, QueryError> {
        self.store.put_tx(rec)
    }

    pub fn get_tx(&self, fk: Fk) -> Result<TxRecord, QueryError> {
        self.get_tx_class_a(fk)
    }

    /// Load tx row: confirm-parent cache → store (no generic Class A cache).
    pub fn get_tx_class_a(&self, fk: Fk) -> Result<TxRecord, QueryError> {
        if let Some(tx) = self.confirm_parents.get_parent_tx(fk) {
            return Ok(tx);
        }
        self.store.get_tx(fk)
    }

    pub fn get_tx_by_txid(&self, txid: &[u8; 32]) -> Result<Option<(Fk, TxRecord)>, QueryError> {
        if let Some(fk) = self.lookup_tx_fk(txid)? {
            return Ok(Some((fk, self.get_tx(fk)?)));
        }
        Ok(None)
    }

    /// Input `i` of a tx row (packed full body via txid→fk).
    ///
    /// Prefer [`Self::tx_input_at_fk`] when the create fk is known (packed Class A
    /// with `tx.head` off).
    pub fn tx_input(&self, tx: &TxRecord, i: u32) -> Result<InputRecord, QueryError> {
        if i >= tx.input_count {
            return Err(StoreError::NotFound);
        }
        let fk = self
            .lookup_tx_fk(&tx.txid)?
            .ok_or(StoreError::NotFound)?;
        self.tx_input_at_fk(fk, tx, i)
    }

    /// Input `i` keyed by known create fk (packed body, no head required).
    pub fn tx_input_at_fk(
        &self,
        create_fk: Fk,
        tx: &TxRecord,
        i: u32,
    ) -> Result<InputRecord, QueryError> {
        if i >= tx.input_count {
            return Err(StoreError::NotFound);
        }
        let (_, inputs, _) = self.store.get_tx_full(create_fk)?;
        inputs
            .get(i as usize)
            .cloned()
            .ok_or(StoreError::NotFound)
    }

    /// Output `vout` of a tx row (run-addressed).
    pub fn tx_output(&self, tx: &TxRecord, vout: u32) -> Result<OutputRecord, QueryError> {
        self.tx_output_attributed(tx, vout, false)
    }

    /// Output at `vout` for a known create fk (packed Class A works without head).
    pub fn tx_output_at_fk(
        &self,
        create_fk: Fk,
        tx: &TxRecord,
        vout: u32,
    ) -> Result<OutputRecord, QueryError> {
        self.tx_output_at_fk_attributed(create_fk, tx, vout, false)
    }

    /// Like [`Self::tx_output`] but records connect cold-path counters when
    /// `count_connect` is true.
    ///
    /// Packed rows without `tx.head` need [`Self::tx_output_at_fk_attributed`].
    pub fn tx_output_attributed(
        &self,
        tx: &TxRecord,
        vout: u32,
        count_connect: bool,
    ) -> Result<OutputRecord, QueryError> {
        if vout >= tx.output_count {
            return Err(StoreError::NotFound);
        }
        // Prefer full-run cache via fk when we know it (txid→fk process cache).
        if let Some(fk) = self.lookup_tx_fk(&tx.txid)? {
            return self.tx_output_at_fk_attributed(fk, tx, vout, count_connect);
        }
        Err(StoreError::NotFound)
    }

    /// Packed output load by known create fk + optional connect counters.
    pub fn tx_output_at_fk_attributed(
        &self,
        create_fk: Fk,
        tx: &TxRecord,
        vout: u32,
        count_connect: bool,
    ) -> Result<OutputRecord, QueryError> {
        use std::sync::atomic::Ordering;
        if vout >= tx.output_count {
            return Err(StoreError::NotFound);
        }
        if let Some((_, o)) = self.confirm_parents.get_parent_out(create_fk, vout) {
            if count_connect {
                connect_prevout_stats::CLASS_A_HIT.fetch_add(1, Ordering::Relaxed);
            }
            return Ok(o);
        }
        // Packed Class A — one body IO.
        let (_, _, outs) = self.store.get_tx_full(create_fk)?;
        let out = outs
            .get(vout as usize)
            .cloned()
            .ok_or(StoreError::NotFound)?;
        if count_connect {
            connect_prevout_stats::STORE_MISS.fetch_add(1, Ordering::Relaxed);
        }
        Ok(out)
    }

    pub fn put_spend(
        &self,
        out_txid: &[u8; 32],
        out_index: u32,
        spending_tx_fk: Fk,
        spending_input_index: u32,
    ) -> Result<Fk, QueryError> {
        self.store
            .put_spend(out_txid, out_index, spending_tx_fk, spending_input_index)
    }

    /// Strong (best-chain confirmed) spenders only.
    pub fn spenders(
        &self,
        out_txid: &[u8; 32],
        out_index: u32,
    ) -> Result<Vec<PointRecord>, QueryError> {
        self.store.spenders(out_txid, out_index)
    }

    pub fn spenders_raw(
        &self,
        out_txid: &[u8; 32],
        out_index: u32,
    ) -> Result<Vec<PointRecord>, QueryError> {
        self.store.spenders_raw(out_txid, out_index)
    }

    /// True if this header hash has a Class A row (may not be confirmed on tip).
    pub fn is_header_archived(&self, hash: &[u8; 32]) -> Result<bool, QueryError> {
        Ok(self.get_header_by_hash(hash)?.is_some())
    }

    /// True if the full block body is in Class A (`header_txs` present).
    ///
    /// Does **not** walk the confirmed chain (that was O(tip) per call and froze
    /// IBD when thousands of header-only rows existed). Callers that need
    /// "confirmed or archived" should check the confirmed set / tip first.
    pub fn is_block_archived(&self, hash: &[u8; 32]) -> Result<bool, QueryError> {
        let Some((fk, _)) = self.get_header_by_hash(hash)? else {
            return Ok(false);
        };
        Ok(self.store.header_txs.has_body(fk)?)
    }

    /// Total headers with a Class A body on disk (durable, any prior run).
    pub fn archived_block_count(&self) -> Result<u64, QueryError> {
        Ok(self.store.archived_block_count()?)
    }

    /// Rebuild the post-tip work path from durable headers + Class A bodies.
    ///
    /// IBD only remembered the ordered path in RAM. On restart it re-ran
    /// getheaders/getdata even though Class A was already on disk. This walks
    /// `header.body` once, builds a prev→children map, and follows the best
    /// (prefer-archived) child chain from the confirmed tip.
    ///
    /// `max` caps how many headers are returned (IBD ordered cap).
    pub fn resume_work_path_after_tip(
        &self,
        tip_hash: [u8; 32],
        tip_height: u32,
        max: usize,
    ) -> Result<Vec<ResumeWorkEntry>, QueryError> {
        if max == 0 {
            return Ok(Vec::new());
        }
        let Some((tip_fk, _)) = self.get_header_by_hash(&tip_hash)? else {
            return Ok(Vec::new());
        };
        let n = self.store.header_count();
        if n == 0 {
            return Ok(Vec::new());
        }

        // prev_fk → list of (child_fk, child_hash)
        let mut children: HashMap<u64, Vec<(Fk, [u8; 32])>> = HashMap::new();
        for id in 1..=n {
            let fk = Fk(id);
            let rec = self.store.get_header(fk)?;
            let prev = rec.prev_fk.get().unwrap_or(0);
            children.entry(prev).or_default().push((fk, rec.hash));
        }

        let mut out = Vec::with_capacity(max.min(4096));
        let mut cur_fk = tip_fk;
        let mut height = tip_height;
        while out.len() < max {
            let Some(kids) = children.get(&cur_fk.0) else {
                break;
            };
            if kids.is_empty() {
                break;
            }
            // Prefer a child that already has a Class A body; among ties, highest fk
            // (later archive / main-chain append order).
            let mut best: Option<(Fk, [u8; 32], bool)> = None;
            for &(fk, hash) in kids {
                let has_body = self.store.header_txs.has_body(fk)?;
                let take = match best {
                    None => true,
                    Some((best_fk, _, best_body)) => {
                        (has_body && !best_body) || (has_body == best_body && fk.0 > best_fk.0)
                    }
                };
                if take {
                    best = Some((fk, hash, has_body));
                }
            }
            let Some((fk, hash, has_body)) = best else {
                break;
            };
            height = height.saturating_add(1);
            out.push(ResumeWorkEntry {
                height,
                hash,
                header_fk: fk,
                has_body,
            });
            cur_fk = fk;
        }
        Ok(out)
    }

    /// Flush header rows + Class A body associations (IBD writer durability).
    pub fn flush_header_archive(&self) -> Result<(), QueryError> {
        Ok(self.store.flush_header_archive()?)
    }

    /// Ensure a header row exists (no txs). Idempotent by hash.
    ///
    /// Used to pipeline header sync into the store so out-of-order bodies can
    /// resolve `prev_fk` without waiting for tip confirm.
    pub fn ensure_header(&self, header: &HeaderRecord) -> Result<Fk, QueryError> {
        if let Some((fk, _)) = self.get_header_by_hash(&header.hash)? {
            return Ok(fk);
        }
        Ok(self.store.put_header(header)?)
    }

    pub fn flush(&self) -> Result<(), QueryError> {
        if !self.store.path().exists() {
            return Err(StoreError::NotDirectory(self.store.path().to_path_buf()));
        }
        self.store.flush()
    }
}

fn wire_header(rec: &HeaderRecord, prev_blockhash: BlockHash) -> BlockHeader {
    BlockHeader {
        version: BlockVersion::from_consensus(rec.version),
        prev_blockhash,
        merkle_root: TxMerkleNode::from_byte_array(rec.merkle_root),
        time: rec.timestamp,
        bits: CompactTarget::from_consensus(rec.bits),
        nonce: rec.nonce,
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MerkleProof {
    pub block_height: u32,
    pub pos: usize,
    pub merkle: Vec<[u8; 32]>,
}

pub fn crate_name() -> &'static str {
    "rbitcoin-query"
}
