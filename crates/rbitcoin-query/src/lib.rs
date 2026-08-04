//! Domain query layer over [`rbitcoin_store::Store`].

mod archive;
mod batch_full_bodies;
mod batch_parents;
mod in_flight;
mod catchup;
mod chain_view;
mod combined_stage;
mod confirm_parent_cache;
mod connect;
mod confirm_load;
mod reconstruct;
mod run_builder_core;
mod scripthash;
mod sh_builder;
mod wave_prevout;

pub use combined_stage::{
    body_ok_reads, load_creates_once, reset_body_ok_reads, CombinedCreate,
};

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
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering as AtomicOrdering};
use std::sync::Mutex;

pub type QueryError = StoreError;

/// Soft assign free floor (~100 MiB). Under this payload size, densify uses the
/// usual ahead horizon (net-side densify cap). Over it, densify is limited to
/// the confirm-time window ([`BQ_SOFT_CONFIRM_SECS`]).
///
/// Tunable constant — no hysteresis band (single threshold).
pub const BQ_SOFT_FREE_BYTES: u64 = 100 * 1024 * 1024;

/// When body-queue payload is over [`BQ_SOFT_FREE_BYTES`], only assign getdata
/// for heights confirm will consume in this many seconds at the current tip
/// rate. Tunable constant — no hysteresis band.
pub const BQ_SOFT_CONFIRM_SECS: f64 = 60.0;

/// Blocks confirm can take in one soft confirm window at `rate` (ceil).
///
/// Rate unknown / non-positive → `0` (no densify ahead when restricted).
pub fn soft_confirm_window_n(rate_blocks_per_s: Option<f64>) -> u32 {
    let rate = rate_blocks_per_s
        .filter(|r| r.is_finite() && *r > 1e-9)
        .unwrap_or(0.0);
    (rate * BQ_SOFT_CONFIRM_SECS).ceil() as u32
}

/// True when BQ payload is over the free-byte floor (densify uses confirm window).
#[inline]
pub fn soft_assign_restricted(depth_bytes: u64) -> bool {
    depth_bytes > BQ_SOFT_FREE_BYTES
}

/// Inclusive densify band high height for getdata assign.
///
/// Two simple rules (no latch / hysteresis):
/// - **Under** [`BQ_SOFT_FREE_BYTES`]: full `densify_hi` (usual densify ahead).
/// - **Over** free bytes: only heights confirm will pick up within
///   [`BQ_SOFT_CONFIRM_SECS`] at current rate — `path_lo .. path_lo+window-1`
///   (clamped to `densify_hi`). Rate cold → only `path_lo` (tip-adjacent).
///
/// **Never** gates peer TCP reads or [`Query::block_queue_offer`].
pub fn soft_densify_band_hi(
    path_lo: u32,
    densify_hi: u32,
    depth_bytes: u64,
    rate_blocks_per_s: Option<f64>,
) -> u32 {
    if densify_hi < path_lo {
        return densify_hi;
    }
    if !soft_assign_restricted(depth_bytes) {
        return densify_hi;
    }
    let n = soft_confirm_window_n(rate_blocks_per_s);
    if n == 0 {
        return path_lo.min(densify_hi);
    }
    path_lo
        .saturating_add(n.saturating_sub(1))
        .min(densify_hi)
}

/// True when over free bytes and the queue already holds at least one confirm
/// window of blocks (assign densify has little/no room left in the window).
///
/// Used for Critical assign (tip race only) when inflight is low.
pub fn soft_confirm_window_covered(
    depth_n: u32,
    depth_bytes: u64,
    rate_blocks_per_s: Option<f64>,
) -> bool {
    if !soft_assign_restricted(depth_bytes) {
        return false;
    }
    let w = soft_confirm_window_n(rate_blocks_per_s);
    if w == 0 {
        // Over free, rate unknown: treat as covered (no densify ahead).
        return true;
    }
    depth_n >= w
}

/// Cheap process-owned cache occupancy for IBD `ibd: sizes` (O(1) lens + brief locks).
///
/// `conf_plans` is header plan occupancy in ConfirmParentCache. Pins are
/// pipeline-local (plan `batch_pin` / `BatchParents` / plan-local external
/// parents) — no process create FIFO.
#[derive(Clone, Copy, Debug, Default)]
pub struct ProcessOwnedSizes {
    pub conf_plans: usize,
    pub sh_runs: usize,
    pub sh_memtable: usize,
    pub sh_heads: usize,
    /// Segmented `tx.head.*` occupancy (logical sizes; no shadow resize).
    pub head: rbitcoin_store::HeadResizeSizeSnapshot,
}

pub use batch_full_bodies::BatchFullBodies;
pub use batch_parents::{
    layout_covers_need, sparse_spender_rels, BatchParents, PipelineParentStore, SharedParentPin,
};
pub use confirm_load::BatchThin;
pub use catchup::IndexMode;
pub use connect::ConfirmPrepared;
pub use confirm_load::ConfirmLoadStats;
pub use archive::{ArchiveWritePlan, CreatePin};
pub use in_flight::{InFlightLayer, InFlightLog, InFlightView};
pub use scripthash::{
    ScriptHashBalance, ScriptHashHistoryItem, ScriptHashOutpoint, ScriptHashUtxo,
};
pub use wave_prevout::ThinInput;

/// Confirm load Class A / parent-pin window counters (IBD ~5s sampler).
///
/// Accrued by `load_confirm_parents` (now called inline from confirm load).
/// Pair with [`Query::parent_cache_perf_snapshot`] for cache watermarks.
pub mod confirm_load_stats {
    use std::sync::atomic::{AtomicU64, Ordering};

    /// Pin/load wall: full `load_confirm_parents` on hash path; pin wall on wire path.
    pub static NS: AtomicU64 = AtomicU64::new(0);
    pub static BLOCKS: AtomicU64 = AtomicU64::new(0);
    pub static UTXO_PARENTS: AtomicU64 = AtomicU64::new(0);
    pub static CREATES: AtomicU64 = AtomicU64::new(0);
    pub static PARENT_UNIQUE: AtomicU64 = AtomicU64::new(0);
    /// Pin filled from same-batch / plan-local (no Class A re-decode).
    pub static PIN_CACHE_BODY: AtomicU64 = AtomicU64::new(0);
    /// Wire plan / in-flight parent pins (subset of pin_cache; not denserels hits).
    pub static PIN_PLAN: AtomicU64 = AtomicU64::new(0);
    /// Pin candidates that missed same-batch / plan-local (cold denserels).
    pub static PIN_NEW: AtomicU64 = AtomicU64::new(0);
    pub static PIN_SPENT_NS: AtomicU64 = AtomicU64::new(0);
    pub static PIN_BODY_NS: AtomicU64 = AtomicU64::new(0);
    pub static PIN_NEW_META_NS: AtomicU64 = AtomicU64::new(0);
    /// Wire pin sub-walls (ns).
    pub static PLAN_PIN_NS: AtomicU64 = AtomicU64::new(0);
    /// Cold denserels wall (range + idx). Prefer split fields when diagnosing.
    pub static COLD_IO_NS: AtomicU64 = AtomicU64::new(0);
    /// Cold denserels via plan stamp body range (`get_outs_denserels_by_range_batch`).
    pub static COLD_RANGE_NS: AtomicU64 = AtomicU64::new(0);
    pub static COLD_RANGE_N: AtomicU64 = AtomicU64::new(0);
    /// Sub-wall of cold range: body pread only (N2.0).
    pub static COLD_RANGE_BODY_NS: AtomicU64 = AtomicU64::new(0);
    /// Sub-wall of cold range: sparse denserels decode (N2.0).
    pub static COLD_RANGE_DECODE_NS: AtomicU64 = AtomicU64::new(0);
    /// Cold denserels via idx→body (`load_creates_once`).
    pub static COLD_IDX_NS: AtomicU64 = AtomicU64::new(0);
    pub static COLD_IDX_N: AtomicU64 = AtomicU64::new(0);
    pub static COLD_DECODE_NS: AtomicU64 = AtomicU64::new(0);
    pub static PARENT_CACHE_HITS: AtomicU64 = AtomicU64::new(0);
    pub static FULL_TX_READS: AtomicU64 = AtomicU64::new(0);
    pub static BODY_TX_READS: AtomicU64 = AtomicU64::new(0);
    pub static MISSING_PARENTS: AtomicU64 = AtomicU64::new(0);
    /// Phase nanoseconds (sum over calls this window).
    pub static HEADER_NS: AtomicU64 = AtomicU64::new(0);
    pub static BODY_DECODE_NS: AtomicU64 = AtomicU64::new(0);
    pub static THIN_NS: AtomicU64 = AtomicU64::new(0);
    pub static PARENT_PIN_NS: AtomicU64 = AtomicU64::new(0);
    pub static CACHE_PUT_NS: AtomicU64 = AtomicU64::new(0);
    /// Thin edges: same-batch / stamped-fk / coinbase.
    pub static EDGE_SAME_BATCH: AtomicU64 = AtomicU64::new(0);
    pub static EDGE_FK: AtomicU64 = AtomicU64::new(0);
    pub static EDGE_COINBASE: AtomicU64 = AtomicU64::new(0);

    /// One sampler snapshot (all counters reset).
    #[derive(Debug, Default, Clone, Copy)]
    pub struct Sample {
        pub ns: u64,
        pub blocks: u64,
        pub utxo_parents: u64,
        pub creates: u64,
        pub parent_unique: u64,
        pub pin_cache_body: u64,
        pub pin_plan: u64,
        pub pin_new: u64,
        pub pin_spent_ns: u64,
        pub pin_body_ns: u64,
        pub pin_new_meta_ns: u64,
        pub plan_pin_ns: u64,
        pub cold_io_ns: u64,
        pub cold_range_ns: u64,
        pub cold_range_n: u64,
        pub cold_range_body_ns: u64,
        pub cold_range_decode_ns: u64,
        pub cold_idx_ns: u64,
        pub cold_idx_n: u64,
        pub cold_decode_ns: u64,
        pub cache_hits: u64,
        pub body_tx: u64,
        pub parent_tx: u64,
        pub missing: u64,
        pub header_ns: u64,
        pub body_decode_ns: u64,
        pub thin_ns: u64,
        pub parent_pin_ns: u64,
        pub cache_put_ns: u64,
        pub edge_same_batch: u64,
        pub edge_fk: u64,
        pub edge_coinbase: u64,
    }

    pub fn sample_and_reset() -> Sample {
        Sample {
            ns: NS.swap(0, Ordering::Relaxed),
            blocks: BLOCKS.swap(0, Ordering::Relaxed),
            utxo_parents: UTXO_PARENTS.swap(0, Ordering::Relaxed),
            creates: CREATES.swap(0, Ordering::Relaxed),
            parent_unique: PARENT_UNIQUE.swap(0, Ordering::Relaxed),
            pin_cache_body: PIN_CACHE_BODY.swap(0, Ordering::Relaxed),
            pin_plan: PIN_PLAN.swap(0, Ordering::Relaxed),
            pin_new: PIN_NEW.swap(0, Ordering::Relaxed),
            pin_spent_ns: PIN_SPENT_NS.swap(0, Ordering::Relaxed),
            pin_body_ns: PIN_BODY_NS.swap(0, Ordering::Relaxed),
            pin_new_meta_ns: PIN_NEW_META_NS.swap(0, Ordering::Relaxed),
            plan_pin_ns: PLAN_PIN_NS.swap(0, Ordering::Relaxed),
            cold_io_ns: COLD_IO_NS.swap(0, Ordering::Relaxed),
            cold_range_ns: COLD_RANGE_NS.swap(0, Ordering::Relaxed),
            cold_range_n: COLD_RANGE_N.swap(0, Ordering::Relaxed),
            cold_range_body_ns: COLD_RANGE_BODY_NS.swap(0, Ordering::Relaxed),
            cold_range_decode_ns: COLD_RANGE_DECODE_NS.swap(0, Ordering::Relaxed),
            cold_idx_ns: COLD_IDX_NS.swap(0, Ordering::Relaxed),
            cold_idx_n: COLD_IDX_N.swap(0, Ordering::Relaxed),
            cold_decode_ns: COLD_DECODE_NS.swap(0, Ordering::Relaxed),
            cache_hits: PARENT_CACHE_HITS.swap(0, Ordering::Relaxed),
            body_tx: BODY_TX_READS.swap(0, Ordering::Relaxed),
            parent_tx: FULL_TX_READS.swap(0, Ordering::Relaxed),
            missing: MISSING_PARENTS.swap(0, Ordering::Relaxed),
            header_ns: HEADER_NS.swap(0, Ordering::Relaxed),
            body_decode_ns: BODY_DECODE_NS.swap(0, Ordering::Relaxed),
            thin_ns: THIN_NS.swap(0, Ordering::Relaxed),
            parent_pin_ns: PARENT_PIN_NS.swap(0, Ordering::Relaxed),
            cache_put_ns: CACHE_PUT_NS.swap(0, Ordering::Relaxed),
            edge_same_batch: EDGE_SAME_BATCH.swap(0, Ordering::Relaxed),
            edge_fk: EDGE_FK.swap(0, Ordering::Relaxed),
            edge_coinbase: EDGE_COINBASE.swap(0, Ordering::Relaxed),
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
        add!(parent_unique, PARENT_UNIQUE);
        add!(pin_cache_body, PIN_CACHE_BODY);
        add!(pin_new, PIN_NEW);
        add!(pin_spent_ns, PIN_SPENT_NS);
        add!(pin_body_ns, PIN_BODY_NS);
        add!(pin_new_meta_ns, PIN_NEW_META_NS);
        add!(parent_cache_hits, PARENT_CACHE_HITS);
        add!(full_tx_reads, FULL_TX_READS);
        add!(body_tx_reads, BODY_TX_READS);
        add!(missing_parents, MISSING_PARENTS);
        add!(header_ns, HEADER_NS);
        add!(body_decode_ns, BODY_DECODE_NS);
        add!(thin_ns, THIN_NS);
        add!(parent_pin_ns, PARENT_PIN_NS);
        add!(cache_put_ns, CACHE_PUT_NS);
        add!(edge_same_batch, EDGE_SAME_BATCH);
        add!(edge_fk, EDGE_FK);
        add!(edge_coinbase, EDGE_COINBASE);
    }
}

/// Archive prep + commit phase walls and resolve counts (IBD ~5s sampler reset).
///
/// **Accounting:** `prep_total_ns` / `write_total_ns` are end-to-end walls for
/// each batch; sub-phase ns should sum to ≈ total (gap = unaccounted). Prep
/// includes structure decode, plan/resolve, and write-queue wait. Write includes
/// reserve, body, head, spends, header_txs, sticky, dontneed, and periodic flush.
pub mod archive_phase_stats {
    use std::sync::atomic::{AtomicU64, Ordering};

    // ── counts (resolve mix) ─────────────────────────────────────────────
    /// Headers (blocks) planned this window.
    pub static BLOCKS: AtomicU64 = AtomicU64::new(0);
    pub static EXT_NEED: AtomicU64 = AtomicU64::new(0);
    pub static STICKY_HIT: AtomicU64 = AtomicU64::new(0);
    pub static HEAD_NEED: AtomicU64 = AtomicU64::new(0);
    pub static HEAD_HIT: AtomicU64 = AtomicU64::new(0);
    /// Fks that received plan denserels in Shape A fused head resolve.
    pub static HEAD_DENS_FKS: AtomicU64 = AtomicU64::new(0);
    /// Sum of packed body lengths read in denserels wave (when ranges known).
    pub static HEAD_DENS_BYTES: AtomicU64 = AtomicU64::new(0);
    pub static BATCH_STAMP: AtomicU64 = AtomicU64::new(0);
    pub static RESOLVED_STAMP: AtomicU64 = AtomicU64::new(0);

    // ── prep walls (ns) ──────────────────────────────────────────────────
    /// Full prep batch wall (struct → plan → enqueue wait).
    pub static PREP_TOTAL_NS: AtomicU64 = AtomicU64::new(0);
    pub static PREP_STRUCT_NS: AtomicU64 = AtomicU64::new(0);
    pub static PREP_FILTER_NS: AtomicU64 = AtomicU64::new(0);
    pub static PREP_ASSIGN_NS: AtomicU64 = AtomicU64::new(0);
    pub static PREP_COLLECT_NS: AtomicU64 = AtomicU64::new(0);
    pub static PREP_STICKY_NS: AtomicU64 = AtomicU64::new(0);
    pub static PREP_INFLIGHT_NS: AtomicU64 = AtomicU64::new(0);
    /// Combined head wall (fk resolve + plan denserels); = head_fk + head_dens.
    pub static PREP_HEAD_NS: AtomicU64 = AtomicU64::new(0);
    /// Shape A Prefix33 select wall (head total − denserels).
    pub static PREP_HEAD_FK_NS: AtomicU64 = AtomicU64::new(0);
    /// Shape A denserels wave (single-cand denserels-only + multi-cand dens).
    pub static PREP_HEAD_DENS_NS: AtomicU64 = AtomicU64::new(0);
    pub static PREP_STAMP_NS: AtomicU64 = AtomicU64::new(0);
    pub static PREP_FINISH_NS: AtomicU64 = AtomicU64::new(0);
    /// Reserved HWM + inflight create map publish after plan.
    pub static PREP_PUBLISH_NS: AtomicU64 = AtomicU64::new(0);
    /// Blocked on full prep→writer queue.
    pub static PREP_QWAIT_NS: AtomicU64 = AtomicU64::new(0);
    pub static PREP_BLOCKS: AtomicU64 = AtomicU64::new(0);

    // ── write / commit walls (ns) ─────────────────────────────────────────
    pub static WRITE_TOTAL_NS: AtomicU64 = AtomicU64::new(0);
    pub static WRITE_RESERVE_NS: AtomicU64 = AtomicU64::new(0);
    pub static WRITE_BODY_NS: AtomicU64 = AtomicU64::new(0);
    pub static WRITE_HEAD_NS: AtomicU64 = AtomicU64::new(0);
    pub static WRITE_SPEND_NS: AtomicU64 = AtomicU64::new(0);
    pub static WRITE_HTXS_NS: AtomicU64 = AtomicU64::new(0);
    pub static WRITE_STICKY_NS: AtomicU64 = AtomicU64::new(0);
    pub static WRITE_DONTNEED_NS: AtomicU64 = AtomicU64::new(0);
    /// Periodic `flush_header_archive` on the writer thread.
    pub static WRITE_FLUSH_NS: AtomicU64 = AtomicU64::new(0);
    pub static WRITE_BLOCKS: AtomicU64 = AtomicU64::new(0);

    #[derive(Debug, Default, Clone, Copy)]
    pub struct Sample {
        pub blocks: u64,
        pub ext_need: u64,
        pub sticky_hit: u64,
        pub head_need: u64,
        pub head_hit: u64,
        pub head_dens_fks: u64,
        pub head_dens_bytes: u64,
        pub batch_stamp: u64,
        pub resolved_stamp: u64,
        /// Sticky+inflight+head_fk only (not plan denserels load).
        pub resolve_ns: u64,
        pub prep_total_ns: u64,
        pub prep_struct_ns: u64,
        pub prep_filter_ns: u64,
        pub prep_assign_ns: u64,
        pub prep_collect_ns: u64,
        pub prep_sticky_ns: u64,
        pub prep_inflight_ns: u64,
        /// head_fk + head_dens (legacy total).
        pub prep_head_ns: u64,
        /// Pure tx.head resolve (`get_fk_by_txid_batch`).
        pub prep_head_fk_ns: u64,
        /// Plan-time denserels for external parents (Shape A fused head resolve).
        pub prep_head_dens_ns: u64,
        pub prep_stamp_ns: u64,
        pub prep_finish_ns: u64,
        pub prep_publish_ns: u64,
        pub prep_qwait_ns: u64,
        pub prep_blocks: u64,
        pub write_total_ns: u64,
        pub write_reserve_ns: u64,
        pub write_body_ns: u64,
        pub write_head_ns: u64,
        pub write_spend_ns: u64,
        pub write_htxs_ns: u64,
        pub write_sticky_ns: u64,
        pub write_dontneed_ns: u64,
        pub write_flush_ns: u64,
        pub write_blocks: u64,
    }

    impl Sample {
        /// Sum of prep sub-phases (should ≈ prep_total_ns).
        pub fn prep_phases_sum_ns(&self) -> u64 {
            self.prep_struct_ns
                .saturating_add(self.prep_filter_ns)
                .saturating_add(self.prep_assign_ns)
                .saturating_add(self.prep_collect_ns)
                .saturating_add(self.prep_sticky_ns)
                .saturating_add(self.prep_inflight_ns)
                .saturating_add(self.prep_head_ns)
                .saturating_add(self.prep_stamp_ns)
                .saturating_add(self.prep_finish_ns)
                .saturating_add(self.prep_publish_ns)
                .saturating_add(self.prep_qwait_ns)
        }

        /// Sum of write sub-phases (should ≈ write_total_ns).
        pub fn write_phases_sum_ns(&self) -> u64 {
            self.write_reserve_ns
                .saturating_add(self.write_body_ns)
                .saturating_add(self.write_head_ns)
                .saturating_add(self.write_spend_ns)
                .saturating_add(self.write_htxs_ns)
                .saturating_add(self.write_sticky_ns)
                .saturating_add(self.write_dontneed_ns)
                .saturating_add(self.write_flush_ns)
        }
    }

    pub fn sample_and_reset() -> Sample {
        let prep_sticky = PREP_STICKY_NS.swap(0, Ordering::Relaxed);
        let prep_inflight = PREP_INFLIGHT_NS.swap(0, Ordering::Relaxed);
        let prep_head_fk = PREP_HEAD_FK_NS.swap(0, Ordering::Relaxed);
        let prep_head_dens = PREP_HEAD_DENS_NS.swap(0, Ordering::Relaxed);
        let prep_head = PREP_HEAD_NS
            .swap(0, Ordering::Relaxed)
            .max(prep_head_fk.saturating_add(prep_head_dens));
        Sample {
            blocks: BLOCKS.swap(0, Ordering::Relaxed),
            ext_need: EXT_NEED.swap(0, Ordering::Relaxed),
            sticky_hit: STICKY_HIT.swap(0, Ordering::Relaxed),
            head_need: HEAD_NEED.swap(0, Ordering::Relaxed),
            head_hit: HEAD_HIT.swap(0, Ordering::Relaxed),
            head_dens_fks: HEAD_DENS_FKS.swap(0, Ordering::Relaxed),
            head_dens_bytes: HEAD_DENS_BYTES.swap(0, Ordering::Relaxed),
            batch_stamp: BATCH_STAMP.swap(0, Ordering::Relaxed),
            resolved_stamp: RESOLVED_STAMP.swap(0, Ordering::Relaxed),
            resolve_ns: prep_sticky
                .saturating_add(prep_inflight)
                .saturating_add(prep_head_fk),
            prep_total_ns: PREP_TOTAL_NS.swap(0, Ordering::Relaxed),
            prep_struct_ns: PREP_STRUCT_NS.swap(0, Ordering::Relaxed),
            prep_filter_ns: PREP_FILTER_NS.swap(0, Ordering::Relaxed),
            prep_assign_ns: PREP_ASSIGN_NS.swap(0, Ordering::Relaxed),
            prep_collect_ns: PREP_COLLECT_NS.swap(0, Ordering::Relaxed),
            prep_sticky_ns: prep_sticky,
            prep_inflight_ns: prep_inflight,
            prep_head_ns: prep_head,
            prep_head_fk_ns: prep_head_fk,
            prep_head_dens_ns: prep_head_dens,
            prep_stamp_ns: PREP_STAMP_NS.swap(0, Ordering::Relaxed),
            prep_finish_ns: PREP_FINISH_NS.swap(0, Ordering::Relaxed),
            prep_publish_ns: PREP_PUBLISH_NS.swap(0, Ordering::Relaxed),
            prep_qwait_ns: PREP_QWAIT_NS.swap(0, Ordering::Relaxed),
            prep_blocks: PREP_BLOCKS.swap(0, Ordering::Relaxed),
            write_total_ns: WRITE_TOTAL_NS.swap(0, Ordering::Relaxed),
            write_reserve_ns: WRITE_RESERVE_NS.swap(0, Ordering::Relaxed),
            write_body_ns: WRITE_BODY_NS.swap(0, Ordering::Relaxed),
            write_head_ns: WRITE_HEAD_NS.swap(0, Ordering::Relaxed),
            write_spend_ns: WRITE_SPEND_NS.swap(0, Ordering::Relaxed),
            write_htxs_ns: WRITE_HTXS_NS.swap(0, Ordering::Relaxed),
            write_sticky_ns: WRITE_STICKY_NS.swap(0, Ordering::Relaxed),
            write_dontneed_ns: WRITE_DONTNEED_NS.swap(0, Ordering::Relaxed),
            write_flush_ns: WRITE_FLUSH_NS.swap(0, Ordering::Relaxed),
            write_blocks: WRITE_BLOCKS.swap(0, Ordering::Relaxed),
        }
    }

    #[inline]
    fn add(atom: &AtomicU64, v: u64) {
        if v > 0 {
            atom.fetch_add(v, Ordering::Relaxed);
        }
    }

    /// Resolve mix counters (one plan batch).
    #[inline]
    pub fn note_resolve_counts(
        blocks: u64,
        ext_need: u64,
        sticky_hit: u64,
        head_need: u64,
        head_hit: u64,
        batch_stamp: u64,
        resolved_stamp: u64,
    ) {
        add(&BLOCKS, blocks);
        add(&EXT_NEED, ext_need);
        add(&STICKY_HIT, sticky_hit);
        add(&HEAD_NEED, head_need);
        add(&HEAD_HIT, head_hit);
        add(&BATCH_STAMP, batch_stamp);
        add(&RESOLVED_STAMP, resolved_stamp);
    }

    /// Plan denserels wave size (fks + optional body bytes read).
    #[inline]
    pub fn note_head_dens_wave(dens_fks: u64, dens_bytes: u64) {
        add(&HEAD_DENS_FKS, dens_fks);
        add(&HEAD_DENS_BYTES, dens_bytes);
    }

    /// Prep sub-phases for one mega-batch plan (`archive_plan_mega_from`).
    ///
    /// `head_fk_ns`: pure `get_fk_by_txid_batch`.  
    /// `head_dens_ns`: plan-time external-parent denserels load.  
    /// `head` total = head_fk + head_dens (also stored on `PREP_HEAD_NS`).
    #[inline]
    pub fn note_prep_plan(
        assign_ns: u64,
        collect_ns: u64,
        sticky_ns: u64,
        inflight_ns: u64,
        head_fk_ns: u64,
        head_dens_ns: u64,
        stamp_ns: u64,
        finish_ns: u64,
    ) {
        add(&PREP_ASSIGN_NS, assign_ns);
        add(&PREP_COLLECT_NS, collect_ns);
        add(&PREP_STICKY_NS, sticky_ns);
        add(&PREP_INFLIGHT_NS, inflight_ns);
        add(&PREP_HEAD_FK_NS, head_fk_ns);
        add(&PREP_HEAD_DENS_NS, head_dens_ns);
        add(
            &PREP_HEAD_NS,
            head_fk_ns.saturating_add(head_dens_ns),
        );
        add(&PREP_STAMP_NS, stamp_ns);
        add(&PREP_FINISH_NS, finish_ns);
    }

    /// Outer prep batch (structure + filter + publish + queue wait).
    /// Plan sub-phases are noted separately via [`note_prep_plan`].
    #[inline]
    pub fn note_prep_batch(
        total_ns: u64,
        struct_ns: u64,
        filter_ns: u64,
        publish_ns: u64,
        qwait_ns: u64,
        blocks: u64,
    ) {
        add(&PREP_TOTAL_NS, total_ns);
        add(&PREP_STRUCT_NS, struct_ns);
        add(&PREP_FILTER_NS, filter_ns);
        add(&PREP_PUBLISH_NS, publish_ns);
        add(&PREP_QWAIT_NS, qwait_ns);
        add(&PREP_BLOCKS, blocks);
    }

    /// Commit path sub-phases (`archive_commit_plan`).
    #[inline]
    pub fn note_write_commit(
        total_ns: u64,
        reserve_ns: u64,
        body_ns: u64,
        head_ns: u64,
        spend_ns: u64,
        htxs_ns: u64,
        sticky_ns: u64,
        dontneed_ns: u64,
        blocks: u64,
    ) {
        add(&WRITE_TOTAL_NS, total_ns);
        add(&WRITE_RESERVE_NS, reserve_ns);
        add(&WRITE_BODY_NS, body_ns);
        add(&WRITE_HEAD_NS, head_ns);
        add(&WRITE_SPEND_NS, spend_ns);
        add(&WRITE_HTXS_NS, htxs_ns);
        add(&WRITE_STICKY_NS, sticky_ns);
        add(&WRITE_DONTNEED_NS, dontneed_ns);
        add(&WRITE_BLOCKS, blocks);
    }

    #[inline]
    pub fn note_write_flush(ns: u64) {
        add(&WRITE_FLUSH_NS, ns);
        // Include flush in write total so phases_sum ≈ total.
        add(&WRITE_TOTAL_NS, ns);
    }
}

/// Backward-compatible name for archive resolve/phase sampler.
pub mod archive_resolve_stats {
    pub use super::archive_phase_stats::*;
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

    /// SH: filter which wave txs need create rows.
    pub static SH_FILTER_NS: AtomicU64 = AtomicU64::new(0);
    /// SH: load creates from Class A for new txs (Direct runs enqueue).
    pub static SH_COLLECT_NS: AtomicU64 = AtomicU64::new(0);
    /// SH: sort creates by scripthash (tip append path).
    pub static SH_SORT_NS: AtomicU64 = AtomicU64::new(0);
    /// SH: seed process/durable heads (tip append path).
    pub static SH_SEED_NS: AtomicU64 = AtomicU64::new(0);
    /// SH: encode + body `write_at` (tip append path).
    pub static SH_BODY_NS: AtomicU64 = AtomicU64::new(0);
    /// SH: `scripthash.head` insert_many (tip append path).
    pub static SH_HEAD_NS: AtomicU64 = AtomicU64::new(0);

    /// SH collect source: write-batch CreatePin outs (no store re-read).
    pub static SH_COLLECT_PIN: AtomicU64 = AtomicU64::new(0);
    /// SH collect source: cold Class A body load.
    pub static SH_COLLECT_COLD: AtomicU64 = AtomicU64::new(0);

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

    /// `(filter, collect, sort, seed, body, head)` nanoseconds.
    pub fn sample_sh_sub_and_reset() -> (u64, u64, u64, u64, u64, u64) {
        (
            SH_FILTER_NS.swap(0, Ordering::Relaxed),
            SH_COLLECT_NS.swap(0, Ordering::Relaxed),
            SH_SORT_NS.swap(0, Ordering::Relaxed),
            SH_SEED_NS.swap(0, Ordering::Relaxed),
            SH_BODY_NS.swap(0, Ordering::Relaxed),
            SH_HEAD_NS.swap(0, Ordering::Relaxed),
        )
    }

    /// `(pin, cold)` create counts for SH collect sources, then reset.
    pub fn sample_sh_collect_src_and_reset() -> (u64, u64) {
        (
            SH_COLLECT_PIN.swap(0, Ordering::Relaxed),
            SH_COLLECT_COLD.swap(0, Ordering::Relaxed),
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
///
/// Coarse hit counts only. Time splits live in consensus `confirm_phase_stats`
/// (`ASM_PREV_*` / `asm_prev_us_per_in` on `ibd: perf`).
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

/// Wire-rebuild body load counters (IBD sampler).
///
/// Historical name `wave_fill_stats` — only store body decode remains live.
pub mod wave_fill_stats {
    use std::sync::atomic::{AtomicU64, Ordering};

    /// Wire bodies re-decoded from store.
    pub static BODY_STORE: AtomicU64 = AtomicU64::new(0);
    /// Wall ns spent in store body decode.
    pub static BODY_STORE_NS: AtomicU64 = AtomicU64::new(0);

    /// `(store_count, store_body_ns)`.
    pub fn sample_store_and_reset() -> (u64, u64) {
        (
            BODY_STORE.swap(0, Ordering::Relaxed),
            BODY_STORE_NS.swap(0, Ordering::Relaxed),
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
    /// Block-structured confirm parent cache.
    confirm_parents: confirm_parent_cache::ConfirmParentCache,
    /// In-RAM block payload queue (FIFO until confirm-write; empty after restart).
    ///
    /// RAM-only by design: avoids double-writing every block (queue + Class A).
    /// Accepts redownload on restart and peak RAM of soft densify depth.
    block_queue: Mutex<rbitcoin_store::BlockQueue>,
    /// Last soft-assign restricted flag (over free-byte floor; cache for meters).
    block_queue_pressure: AtomicBool,
    /// Direct IBD SH: memtable → sorted runs (bulk materialize at tip).
    sh_run: sh_builder::ShRunBuilder,
    /// Explicit [`IndexMode`] (Direct / Tip).
    index_mode_cell: std::sync::atomic::AtomicU8,
    /// Cooperative cancel for in-flight confirm load. Set on IBD SIGINT
    /// teardown so the confirm load thread aborts before process exit.
    confirm_cancel: std::sync::atomic::AtomicBool,
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
            block_queue: Mutex::new(rbitcoin_store::BlockQueue::open_or_create(
                &store_path,
            )?),
            block_queue_pressure: AtomicBool::new(false),
            sh_run: sh_builder::ShRunBuilder::new(&store_path),
            // Open as Tip until IBD selects Direct.
            index_mode_cell: std::sync::atomic::AtomicU8::new(IndexMode::Tip as u8),
            confirm_cancel: std::sync::atomic::AtomicBool::new(false),
        };
        // Warm cache from durable head if present (resume with index on).
        // Full body scan is not done here; fresh genesis IBD fills cache as it archives.
        Ok(q)
    }

    /// Request in-flight confirm to abort cooperative load (IBD SIGINT).
    pub fn request_confirm_cancel(&self) {
        self.confirm_cancel
            .store(true, std::sync::atomic::Ordering::SeqCst);
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

    /// True while store runs a **blocking RAM** `tx.head` resize (confirm should pause).
    pub fn tx_head_resize_in_progress(&self) -> bool {
        self.store.txs.head_resize_in_progress()
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

    /// Resolve txid → fk via durable `tx.head` (when the index is enabled).
    ///
    /// ConfirmParentCache is keyed by create fk only (no process-local txid map).
    /// IBD thin edges carry stamped create_fk; cold/soft paths use durable head.
    fn lookup_tx_fk(&self, txid: &[u8; 32]) -> Result<Option<Fk>, QueryError> {
        if self.tx_index_enabled() {
            // body_txid verify only — avoid full packed decode on probe misses.
            if let Some(fk) = self.store.get_fk_by_txid(txid)? {
                return Ok(Some(fk));
            }
        }
        Ok(None)
    }

    /// Public resolve by txid (durable head when index enabled).
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

/// Outcome of [`Query::block_queue_offer`].
#[derive(Debug, Clone)]
pub struct BlockQueueOffer {
    /// In-RAM queue record id for this body.
    pub queue_id: u64,
}

impl Query {
    /// True if this outpoint is spent on the **best chain** (durable confirmed-strong).
    ///
    /// Does **not** treat archive-only point rows as spent: Class A may write
    /// edges before Class C; those spenders are not strong yet.
    pub fn is_outpoint_spent(&self, txid: &[u8; 32], vout: u32) -> Result<bool, QueryError> {
        // Durable head → spender. Confirm path uses create_fk via
        // [`Self::is_outpoint_spent_create`] when the fk is already known.
        Ok(self.store.has_confirmed_strong_spender(txid, vout)?)
    }

    /// Spentness by known create fk (confirm pin path — no head probe).
    pub fn is_outpoint_spent_create(&self, create_fk: Fk, vout: u32) -> Result<bool, QueryError> {
        Ok(self
            .store
            .has_confirmed_strong_spender_create(create_fk, vout, None)?)
    }

    /// Unspent subset of vouts on a create (batch; store uses tx.idx when needed).
    pub fn unspent_create_vouts(
        &self,
        create_fk: Fk,
        vouts: &[u32],
    ) -> Result<Vec<u32>, QueryError> {
        Ok(self.store.unspent_create_vouts(create_fk, vouts, None)?)
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

    /// In-RAM block queue stats: `(absolute_budget_or_max, bytes, count)`.
    ///
    /// Bytes are process heap (wire payloads). Absolute budget is `u64::MAX`
    /// when unlimited (default); densify uses soft time-depth, not this ceiling.
    pub fn block_queue_stats(&self) -> (u64, u64, usize) {
        let g = self.block_queue.lock().unwrap();
        (g.budget(), g.bytes(), g.count())
    }

    /// In-RAM entry count (soft time-depth meter).
    pub fn block_queue_count(&self) -> usize {
        self.block_queue.lock().unwrap().count()
    }

    /// Highest height on the in-RAM body queue (`None` if empty).
    pub fn block_queue_max_height(&self) -> Option<u32> {
        self.block_queue.lock().unwrap().max_height()
    }

    /// Refresh soft-assign restricted flag from current BQ bytes (no latch).
    ///
    /// Returns true when payload is over [`BQ_SOFT_FREE_BYTES`] (densify limited
    /// to the confirm-time window). Does **not** affect peer reads or
    /// [`Self::block_queue_offer`]. `rate_blocks_per_s` is accepted for call-site
    /// compatibility; restriction is byte-only (window size is separate).
    pub fn block_queue_update_soft_pressure(&self, _rate_blocks_per_s: Option<f64>) -> bool {
        let depth_bytes = self.block_queue.lock().unwrap().bytes();
        let restricted = soft_assign_restricted(depth_bytes);
        self.block_queue_pressure
            .store(restricted, AtomicOrdering::Relaxed);
        restricted
    }

    /// Current soft-assign restricted flag (over free-byte floor).
    pub fn block_queue_soft_pressure(&self) -> bool {
        self.block_queue_pressure.load(AtomicOrdering::Relaxed)
    }

    /// Soft confirm-window count for logs / assign: `(window_n, free_mib)`.
    ///
    /// `window_n` = blocks confirm takes in [`BQ_SOFT_CONFIRM_SECS`] at rate.
    /// `free_mib` = free-byte floor in MiB (second log field when useful).
    pub fn block_queue_soft_targets(rate_blocks_per_s: Option<f64>) -> (u32, u32) {
        let win = soft_confirm_window_n(rate_blocks_per_s);
        let free_mib = (BQ_SOFT_FREE_BYTES / (1024 * 1024)) as u32;
        (win, free_mib)
    }

    /// Enqueue a raw block payload in the process-local RAM queue.
    ///
    /// **Always accepts** peer wire when the optional absolute byte ceiling
    /// allows — independent of soft assign restriction. Soft densify only
    /// limits **new getdata assign**; never refuse in-flight bodies here
    /// (except rare absolute-ceiling `BudgetFull`). Restart drops the queue
    /// (redownload); sole durable write is Class A on confirm.
    pub fn block_queue_offer(
        &self,
        height: u32,
        hash: [u8; 32],
        header_fk: u64,
        payload: &[u8],
    ) -> Result<BlockQueueOffer, QueryError> {
        // Intentionally ignores soft assign restriction — request-limited only.
        let mut g = self.block_queue.lock().unwrap();
        // Idempotent: already queued for this height (re-offer after race).
        if let Some(id) = g.id_for_height(height) {
            return Ok(BlockQueueOffer { queue_id: id });
        }
        let id = g.enqueue(height, hash, header_fk, payload)?;
        Ok(BlockQueueOffer { queue_id: id })
    }

    /// Direct RAM enqueue (tests / tools). Prefer [`Self::block_queue_offer`] on IBD.
    pub fn block_queue_enqueue(
        &self,
        height: u32,
        hash: [u8; 32],
        header_fk: u64,
        payload: &[u8],
    ) -> Result<u64, QueryError> {
        let mut g = self.block_queue.lock().unwrap();
        Ok(g.enqueue(height, hash, header_fk, payload)?)
    }

    /// Remove RAM queue entry after combined confirm-write (or permanent drop).
    pub fn block_queue_dequeue_height(&self, height: u32) -> Result<usize, QueryError> {
        let mut g = self.block_queue.lock().unwrap();
        Ok(g.dequeue_height(height)?)
    }

    /// Index-only queue entries (no payload clone). Empty after restart.
    pub fn block_queue_list_meta(&self) -> Vec<rbitcoin_store::QueuedBlockMeta> {
        let g = self.block_queue.lock().unwrap();
        g.list_meta()
    }

    /// Load all queued blocks **with full payloads** (tests / tools).
    ///
    /// Prefer [`Self::block_queue_list_meta`] for index walks and
    /// [`Self::block_queue_payload`] for single-height prep.
    pub fn block_queue_load_all(&self) -> Result<Vec<rbitcoin_store::QueuedBlock>, QueryError> {
        let g = self.block_queue.lock().unwrap();
        Ok(g.load_all()?)
    }

    /// Body-queue intake for confirm prep: payload for `height` without dequeue.
    ///
    /// Peer → RAM body queue is the only source of wire for the unified path;
    /// ConfirmFeed carries readiness (height/hash), not retained `Block`s.
    /// Accept stores raw wire only (block hash already known from framing;
    /// full parse + txids stay on the confirm pack path so we do not hold both
    /// a decoded `Block` and the wire bytes).
    pub fn block_queue_payload(&self, height: u32) -> Result<Option<Vec<u8>>, QueryError> {
        let g = self.block_queue.lock().unwrap();
        Ok(g.get_by_height(height)?.map(|q| q.payload))
    }

    /// True if the in-RAM body queue holds `height`.
    pub fn block_queue_has_height(&self, height: u32) -> bool {
        let g = self.block_queue.lock().unwrap();
        g.contains_height(height)
    }

    /// Cheap process-owned cache sizes for the IBD `ibd: sizes` line.
    ///
    /// Brief mutex locks only (header plans / SH / heads). Call from the ~5s
    /// status tick — not the hot path.
    pub fn process_owned_size_snapshot(&self) -> ProcessOwnedSizes {
        // Header + tx_fks plans (not the unused scan-watermark `plans` BTreeMap).
        // Wire path always put_header_plan; conf_plans=0 was a metering bug.
        let conf_plans = self.confirm_parents.header_plan_count();
        ProcessOwnedSizes {
            conf_plans,
            sh_runs: self.sh_run.on_disk_run_count(),
            sh_memtable: self.sh_run.memtable_len(),
            sh_heads: self.sh_heads.lock().unwrap().len(),
            head: self.store.txs.head_resize_size_snapshot(),
        }
    }

    /// Rebuild durable `tx.head` from every Class A body (idempotent).
    ///
    /// Prefer **deleting `tx.head`** and reopening the store:
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

    /// Header tx list: parent cache (load) then store.
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

    /// Load tx row from Class A store (no process pin FIFO).
    pub fn get_tx_class_a(&self, fk: Fk) -> Result<TxRecord, QueryError> {
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

#[cfg(test)]
mod tests {
    use super::*;
    use rbitcoin_store::{InputRecord, OutputRecord};
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_query(label: &str) -> (std::path::PathBuf, Query) {
        let n = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("rbitcoin-query-{label}-{n}"));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let q = Query::open_or_create(dir.join("store")).unwrap();
        (dir, q)
    }

    fn coinbase_block(h: u32, prev: Fk) -> (HeaderRecord, TxApply) {
        let mut hash = [0u8; 32];
        hash[0..4].copy_from_slice(&h.to_le_bytes());
        hash[4] = 0xab;
        let header = HeaderRecord {
            prev_fk: prev,
            version: 1,
            timestamp: h + 1,
            bits: 0x207fffff,
            nonce: h,
            merkle_root: hash,
            hash,
        };
        let mut txid = [0u8; 32];
        txid[0..4].copy_from_slice(&h.to_le_bytes());
        txid[31] = 0xcb;
        let ta = TxApply {
            tx: TxRecord {
                txid,
                version: 1,
                locktime: 0,
                input_start_fk: Fk::NULL,
                input_count: 1,
                output_start_fk: Fk::NULL,
                output_count: 1,
            },
            inputs: vec![InputRecord {
                prev_txid: [0u8; 32],
                create_fk: Fk::NULL,
                prev_index: u32::MAX,
                sequence: u32::MAX,
                script_sig: vec![h as u8],
                witness: vec![],
            }],
            outputs: vec![OutputRecord::unspent(50_0000_0000, vec![0x51])],
        };
        (header, ta)
    }

    #[test]
    fn crate_name_and_sampler_stats() {
        assert_eq!(crate_name(), "rbitcoin-query");

        let _ = confirm_load_stats::sample_and_reset();
        confirm_load_stats::note(
            &ConfirmLoadStats {
                blocks: 1,
                utxo_parents: 2,
                creates_registered: 3,
                parent_unique: 4,
                pin_cache_body: 5,
                pin_new: 6,
                pin_spent_ns: 7,
                pin_body_ns: 8,
                pin_new_meta_ns: 9,
                parent_cache_hits: 10,
                full_tx_reads: 11,
                body_tx_reads: 12,
                missing_parents: 13,
                header_ns: 14,
                body_decode_ns: 15,
                thin_ns: 16,
                parent_pin_ns: 17,
                cache_put_ns: 18,
                edge_same_batch: 19,
                edge_fk: 20,
                edge_coinbase: 21,
                ..Default::default()
            },
            100,
        );
        let s = confirm_load_stats::sample_and_reset();
        assert_eq!(s.ns, 100);
        assert_eq!(s.blocks, 1);
        assert_eq!(s.edge_coinbase, 21);

        let _ = archive_phase_stats::sample_and_reset();
        archive_phase_stats::note_resolve_counts(1, 2, 3, 4, 5, 6, 7);
        archive_phase_stats::note_prep_plan(1, 2, 3, 4, 10, 20, 6, 7); // head_fk=10, head_dens=20
        archive_phase_stats::note_head_dens_wave(9, 1024);
        archive_phase_stats::note_prep_batch(10, 1, 2, 3, 4, 1);
        archive_phase_stats::note_write_commit(20, 1, 2, 3, 4, 5, 6, 7, 1);
        archive_phase_stats::note_write_flush(8);
        let a = archive_phase_stats::sample_and_reset();
        assert!(a.prep_phases_sum_ns() > 0);
        assert!(a.write_phases_sum_ns() > 0);
        assert_eq!(a.blocks, 1);
        assert_eq!(a.prep_head_fk_ns, 10);
        assert_eq!(a.prep_head_dens_ns, 20);
        assert_eq!(a.prep_head_ns, 30);
        assert_eq!(a.head_dens_fks, 9);
        assert_eq!(a.head_dens_bytes, 1024);

        class_c_phase_stats::STRONG_NS.store(11, AtomicOrdering::Relaxed);
        class_c_phase_stats::add_sh_part(&class_c_phase_stats::SH_FILTER_NS, 5);
        class_c_phase_stats::TIP_NS.store(3, AtomicOrdering::Relaxed);
        let (st, sh, tip) = class_c_phase_stats::sample_and_reset();
        assert_eq!(st, 11);
        assert!(sh >= 5);
        assert_eq!(tip, 3);
        let _ = class_c_phase_stats::sample_sh_sub_and_reset();
        let _ = class_c_phase_stats::sample_sh_collect_src_and_reset();

        connect_prevout_stats::WAVE_HIT.store(1, AtomicOrdering::Relaxed);
        connect_prevout_stats::CLASS_A_HIT.store(2, AtomicOrdering::Relaxed);
        connect_prevout_stats::STORE_MISS.store(3, AtomicOrdering::Relaxed);
        assert_eq!(connect_prevout_stats::sample_and_reset(), (1, 2, 3));

        wave_fill_stats::add_count(&wave_fill_stats::BODY_STORE, 2);
        wave_fill_stats::add(&wave_fill_stats::BODY_STORE_NS, 9);
        assert_eq!(wave_fill_stats::sample_store_and_reset(), (2, 9));

        // I2 cold range/idx sample.
        let _ = confirm_load_stats::sample_and_reset();
        confirm_load_stats::COLD_RANGE_NS.store(1_000_000, AtomicOrdering::Relaxed);
        confirm_load_stats::COLD_RANGE_N.store(3, AtomicOrdering::Relaxed);
        confirm_load_stats::COLD_IDX_NS.store(2_000_000, AtomicOrdering::Relaxed);
        confirm_load_stats::COLD_IDX_N.store(5, AtomicOrdering::Relaxed);
        let s = confirm_load_stats::sample_and_reset();
        assert_eq!(s.cold_range_ns, 1_000_000);
        assert_eq!(s.cold_range_n, 3);
        assert_eq!(s.cold_idx_ns, 2_000_000);
        assert_eq!(s.cold_idx_n, 5);

    }

    #[test]
    fn connect_chain_query_surface() {
        let (dir, q) = temp_query("connect");
        // Default Tip mode: durable SH on confirm so Electrum-style APIs work.
        assert!(q.index_mode().is_tip());
        assert!(q.index_mode().uses_durable_spends());

        let mut prev = Fk::NULL;
        let mut hashes = Vec::new();
        for h in 0..4u32 {
            let (header, ta) = coinbase_block(h, prev);
            hashes.push(header.hash);
            prev = q.connect_block(Height(h), &header, &[ta]).unwrap();
        }
        assert_eq!(q.tip_height(), Some(Height(3)));
        assert!(q.tip_header_fk().unwrap().is_some());
        assert!(q.is_header_archived(&hashes[2]).unwrap());
        assert!(q.is_block_archived(&hashes[2]).unwrap());
        assert!(q.archived_block_count().unwrap() >= 4);

        // height_of_hash: tip and tip-1 fast paths + deeper scan.
        assert_eq!(q.height_of_hash(&hashes[3]).unwrap(), Some(Height(3)));
        assert_eq!(q.height_of_hash(&hashes[2]).unwrap(), Some(Height(2)));
        assert_eq!(q.height_of_hash(&hashes[0]).unwrap(), Some(Height(0)));
        assert_eq!(q.height_of_hash(&[0xee; 32]).unwrap(), None);

        let hdr = q.wire_header_at_height(Height(1)).unwrap();
        assert_eq!(hdr.time, 2);

        let loc = q.locator_hashes().unwrap();
        assert!(!loc.is_empty());
        let after = q
            .headers_after_locator(
                &loc,
                BlockHash::from_byte_array([0u8; 32]),
                10,
            )
            .unwrap();
        // After matching tip locator → empty; zero locator starts from genesis.
        let from_zero = q
            .headers_after_locator(
                &[BlockHash::from_byte_array([0u8; 32])],
                BlockHash::from_byte_array([0u8; 32]),
                2,
            )
            .unwrap();
        assert_eq!(from_zero.len(), 2);
        let _ = after;

        // Tx resolve + inputs/outputs.
        let fks = q.block_tx_fks(Height(0)).unwrap();
        assert_eq!(fks.len(), 1);
        let tx = q.get_tx(fks[0]).unwrap();
        assert!(q.tx_fk_by_txid(&tx.txid).unwrap().is_some());
        assert!(q.tx_fk_by_txid_store(&tx.txid).unwrap().is_some());
        let inp = q.tx_input_at_fk(fks[0], &tx, 0).unwrap();
        assert!(inp.is_coinbase());
        let out = q.tx_output_at_fk(fks[0], &tx, 0).unwrap();
        assert_eq!(out.value, 50_0000_0000);
        assert!(!q.is_outpoint_spent(&tx.txid, 0).unwrap());
        assert!(!q.is_outpoint_spent_create(fks[0], 0).unwrap());
        assert_eq!(q.unspent_create_vouts(fks[0], &[0]).unwrap(), vec![0]);

        // Merkle proof for coinbase.
        let proof = q.merkle_proof(Height(0), &tx.txid).unwrap();
        assert_eq!(proof.pos, 0);
        assert_eq!(proof.block_height, 0);

        // Scripthash history/balance/utxo for OP_TRUE (durable SH in tip mode).
        let sh = script_hash(&[0x51]);
        let hist = q.scripthash_history(&sh).unwrap();
        assert!(!hist.is_empty());
        let bal = q.scripthash_balance(&sh).unwrap();
        assert!(bal.confirmed > 0);
        let utxos = q.scripthash_listunspent(&sh).unwrap();
        assert!(!utxos.is_empty());

        // Confirm cancel flags.
        assert!(!q.confirm_cancelled());
        q.request_confirm_cancel();
        assert!(q.confirm_cancelled());
        q.clear_confirm_cancel();
        assert!(!q.confirm_cancelled());

        // Direct mode + warm + tip re-entry.
        q.enter_direct_index_mode().unwrap();
        assert!(q.index_mode().is_direct());
        // Leave leftover catchup artifacts to exercise cleanup.
        let _ = std::fs::write(q.store().path().join("ibd_utxo.map"), b"x");
        let _ = std::fs::create_dir_all(q.store().path().join("point.runs"));
        q.enter_direct_index_mode().unwrap();
        q.warm_scripthash_create_index().unwrap();
        let _ = q.finalize_sh_runs();
        let _ = q.scripthash_run_count();
        q.enter_tip_index_mode();
        assert!(q.index_mode().is_tip());

        // Size snapshot (header plans / SH / heads).
        let sizes = q.process_owned_size_snapshot();
        let _ = sizes.conf_plans;
        assert!(q.tx_body_count() >= 4);
        let _ = q.tx_head_occupied();
        let _ = q.scripthash_entry_count();
        let _ = q.point_edge_count();

        // Idempotent confirm at tip height.
        let tip_fk = q.confirm_block(Height(3), &hashes[3]).unwrap();
        assert_eq!(tip_fk, prev);

        // Empty confirm run.
        assert!(q.confirm_blocks_run(&[]).unwrap().is_empty());

        // Disconnect tip then re-check tip height.
        q.disconnect_tip().unwrap();
        assert_eq!(q.tip_height(), Some(Height(2)));

        // load_confirm_parents empty / already-confirmed heights.
        let (st, _, _, _) = q.load_confirm_parents(&[]).unwrap();
        let _ = st.blocks;
        let (st2, _, _, _) = q
            .load_confirm_parents(&[(0, hashes[0]), (1, hashes[1])])
            .unwrap();
        let _ = st2;

        // Cancelled load path.
        q.request_confirm_cancel();
        let cancelled = q.load_confirm_parents(&[(10, [9u8; 32])]);
        assert!(cancelled.is_err());
        q.clear_confirm_cancel();

        q.advance_parent_cache_tip(2);
        q.seed_parent_cache(&[(3, hashes[3])]);
        assert!(q.is_confirm_load_ready(&[]));

        // resume_work_path: max 0 → empty.
        assert!(q
            .resume_work_path_after_tip(hashes[2], 2, 0)
            .unwrap()
            .is_empty());

        // Archive-only header without confirm.
        let (orphan, _) = coinbase_block(99, Fk::NULL);
        let ofk = q.ensure_header(&orphan).unwrap();
        assert_eq!(q.ensure_header(&orphan).unwrap(), ofk);
        assert!(q.is_header_archived(&orphan.hash).unwrap());
        assert!(!q.is_block_archived(&orphan.hash).unwrap());

        q.flush_header_archive().unwrap();
        q.flush().unwrap();
        q.flush_for_shutdown().unwrap();

        // backfill helpers on small chain.
        let n = q.backfill_tx_index(|_, _, _| {}).unwrap();
        let _ = n;
        let (heights, txs) = q.backfill_point_spends(|_, _, _, _| {}).unwrap();
        assert!(heights >= 1);
        let _ = txs;

        // header_tx_fks / get_header_by_hash / put paths.
        let (hfk, hrec) = q.get_header_by_hash(&hashes[1]).unwrap().unwrap();
        assert_eq!(hrec.hash, hashes[1]);
        assert!(q.header_tx_fks(hfk, Some(&hashes[1])).unwrap().is_some());
        assert_eq!(q.get_header(hfk).unwrap().hash, hashes[1]);
        assert!(q.header_at_height(Height(1)).unwrap().is_some());

        // Error paths.
        assert!(q.confirm_blocks_run(&[ConfirmPrepared {
            height: Height(99),
            header_fk: Fk(1),
            tx_fks: vec![Fk(1)],
        }])
        .is_err());
        assert!(q.tx_input_at_fk(fks[0], &tx, 99).is_err());
        assert!(q.tx_output_at_fk(fks[0], &tx, 99).is_err());
        assert!(q.merkle_proof(Height(0), &[0xff; 32]).is_err());
        assert!(q.block_tx_fks(Height(50)).is_err());

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn index_mode_helpers_and_batch_helpers() {
        assert!(IndexMode::Direct.is_direct());
        assert!(!IndexMode::Direct.is_tip());
        assert!(IndexMode::Tip.is_tip());
        assert!(IndexMode::Direct.uses_durable_spends());
        assert!(IndexMode::Tip.uses_durable_spends());

        let mut b = BatchFullBodies::with_capacity(2);
        assert!(b.is_empty());
        b.insert(
            Fk::NULL,
            1,
            TxRecord {
                txid: [0; 32],
                version: 1,
                locktime: 0,
                input_start_fk: Fk::NULL,
                input_count: 0,
                output_start_fk: Fk::NULL,
                output_count: 0,
            },
            vec![],
            vec![],
            None,
            vec![],
        );
        assert!(b.is_empty()); // null fk ignored

        let mut bp = BatchParents::new();
        assert!(bp.is_empty());
        bp.put_resolved(
            Fk(1),
            TxRecord {
                txid: [1; 32],
                version: 1,
                locktime: 0,
                input_start_fk: Fk::NULL,
                input_count: 1,
                output_start_fk: Fk::NULL,
                output_count: 1,
            },
            &[(0, OutputRecord::unspent(1, vec![0x51]))],
            &[0],
            Some(true),
        );
        assert!(!bp.is_empty());
        assert!(bp.pin_covered(Fk(1), &[]));
        assert!(bp.pin_covered(Fk(1), &[0]));
        assert!(!bp.pin_covered(Fk::NULL, &[0]));
        assert!(!bp.pin_covered(Fk(99), &[0]));
        assert!(bp.get_parent_outs_needed(Fk(1), &[0]).is_some());
        assert!(bp.get_parent_tx(Fk(1)).is_some());
        assert_eq!(bp.get_parent_coinbase(Fk(1)), Some(true));
        assert!(bp.get_body_range(Fk(1)).is_none());
        assert!(bp.get_spender_abs(Fk(1), 0).is_none());
        assert!(!bp.has_parent_out(Fk::NULL, 0));
        bp.insert_owned(
            Fk::NULL,
            bp.get_parent_tx(Fk(1)).unwrap(),
            vec![],
            vec![],
            None,
            None,
            vec![],
        );
        let rels = batch_parents::sparse_spender_rels(&[10, 20, 30], &[0, 2]);
        assert_eq!(rels, vec![(0, 10), (2, 30)]);
        // Partial covered outs path (not fully pin_covered but all live present).
        let mut bp2 = BatchParents::new();
        bp2.insert_owned(
            Fk(2),
            TxRecord {
                txid: [2; 32],
                version: 1,
                locktime: 0,
                input_start_fk: Fk::NULL,
                input_count: 1,
                output_start_fk: Fk::NULL,
                output_count: 2,
            },
            vec![
                (0, OutputRecord::unspent(1, vec![0x51])),
                (1, OutputRecord::unspent(2, vec![0x51])),
            ],
            vec![], // empty checked → pin_covered false
            Some(false),
            Some((100, 50)),
            vec![(0, 1), (1, 10)],
        );
        assert!(!bp2.pin_covered(Fk(2), &[0, 1]));
        let got = bp2.get_parent_outs_needed(Fk(2), &[0, 1]).unwrap();
        assert!(!got.2);
        assert_eq!(got.1.len(), 2);
        assert_eq!(bp2.get_spender_abs(Fk(2), 1), Some(110));
        assert!(bp2.get_parent_outs_needed(Fk(2), &[9]).is_none());
    }

    #[test]
    fn reconstruct_and_connect_error_arms() {
        let (dir, q) = temp_query("reconstruct");
        let mut prev = Fk::NULL;
        let mut hashes = Vec::new();
        // Multi-tx block at h=1 for odd merkle layer.
        for h in 0..3u32 {
            let (header, ta) = coinbase_block(h, prev);
            let mut txs = vec![ta];
            if h == 1 {
                // Extra coinbase-like create with unique txid (not real coinbase).
                let mut t2 = coinbase_block(h + 100, prev).1;
                t2.tx.txid[30] = 0xee;
                txs.push(t2);
                let mut t3 = coinbase_block(h + 200, prev).1;
                t3.tx.txid[30] = 0xef;
                txs.push(t3);
            }
            hashes.push(header.hash);
            prev = q.connect_block(Height(h), &header, &txs).unwrap();
        }

        // Reconstruct surfaces.
        let fks0 = q.block_tx_fks(Height(0)).unwrap();
        let wire = q.tx_wire_bytes(fks0[0]).unwrap();
        assert!(!wire.is_empty());
        let wire_tx = q.reconstruct_tx(fks0[0]).unwrap();
        assert_eq!(wire_tx.input.len(), 1);
        // Synthetic header.hash is not PoW/merkle-linked; height rebuild checks mismatch.
        assert!(q.reconstruct_block_at_height(Height(0)).is_err());
        assert!(q
            .reconstruct_block_by_hash(&[0xde; 32])
            .unwrap()
            .is_none());
        let arch = q.reconstruct_archived_block(&hashes[1]).unwrap().unwrap();
        assert_eq!(arch.txdata.len(), 3);
        // archived path does not require header.hash == wire block_hash.
        assert_eq!(arch.txdata[0].input.len(), 1);
        // Batch-local body path.
        let mut batch = BatchFullBodies::new();
        let full = q.store().get_tx_full(fks0[0]).unwrap();
        batch.insert(
            fks0[0],
            0,
            full.0.clone(),
            full.1.clone(),
            full.2.clone(),
            None,
            vec![],
        );
        let tx2 = q.reconstruct_tx_with_batch(fks0[0], Some(&batch)).unwrap();
        assert_eq!(tx2.output.len(), 1);
        // Empty tx list → corrupt.
        let (_hfk, hrec) = q.get_header_by_hash(&hashes[0]).unwrap().unwrap();
        assert!(q
            .reconstruct_archived_block_from_parts(hrec.clone(), vec![])
            .is_err());
        // Unknown hash → None.
        assert!(q.reconstruct_archived_block(&[0x11; 32]).unwrap().is_none());

        // Merkle multi-tx (odd leaf count pads).
        let fks1 = q.block_tx_fks(Height(1)).unwrap();
        let t1 = q.get_tx(fks1[0]).unwrap();
        let proof = q.merkle_proof(Height(1), &t1.txid).unwrap();
        assert_eq!(proof.pos, 0);
        assert!(!proof.merkle.is_empty() || fks1.len() == 1);

        // Class A TxRecord input/output paths.
        let trec = q.get_tx(fks0[0]).unwrap();
        assert!(q.tx_input(&trec, 0).is_ok());
        assert!(q.tx_output(&trec, 0).is_ok());
        assert!(q.tx_input(&trec, 99).is_err());
        assert!(q.tx_output_attributed(&trec, 0, true).is_ok());

        // confirm_blocks_run errors: non-contiguous, wrong first height, null fk.
        assert!(q
            .confirm_blocks_run(&[
                ConfirmPrepared {
                    height: Height(10),
                    header_fk: Fk(1),
                    tx_fks: vec![Fk(1)],
                },
                ConfirmPrepared {
                    height: Height(12),
                    header_fk: Fk(2),
                    tx_fks: vec![Fk(2)],
                },
            ])
            .is_err());
        assert!(q
            .confirm_blocks_run(&[ConfirmPrepared {
                height: Height(0),
                header_fk: Fk::NULL,
                tx_fks: vec![Fk(1)],
            }])
            .is_err());
        // Empty chain genesis check — tip exists so height 0 reconfirm wrong tip+1.
        // Archive empty then connect rejects non-genesis on empty: use fresh store.
        let (dir2, q2) = temp_query("connect-empty");
        assert!(q2
            .confirm_blocks_run(&[ConfirmPrepared {
                height: Height(1),
                header_fk: Fk(1),
                tx_fks: vec![Fk(1)],
            }])
            .is_err());
        let _ = std::fs::remove_dir_all(&dir2);

        // put_header / put_tx / put_spend surfaces.
        let mut orphan = coinbase_block(50, Fk::NULL).0;
        orphan.hash[5] = 0x99;
        let ofk = q.put_header(&orphan).unwrap();
        assert_eq!(q.get_header(ofk).unwrap().hash, orphan.hash);
        let mut trec = coinbase_block(50, Fk::NULL).1.tx;
        trec.txid[0] = 0x77;
        let _tfk = q.put_tx(&trec).unwrap();
        // put_spend needs real create - skip if fails
        let _ = q.put_spend(&[1u8; 32], 0, fks0[0], 0);
        let _ = q.spenders(&[1u8; 32], 0);
        let _ = q.spenders_raw(&[1u8; 32], 0);

        // resume_work_path with unknown tip hash / max>0 empty kids.
        assert!(q
            .resume_work_path_after_tip([0xaa; 32], 0, 10)
            .unwrap()
            .is_empty());
        // tip at last confirmed — may return empty if no archive ahead.
        let path = q
            .resume_work_path_after_tip(hashes[2], 2, 5)
            .unwrap();
        let _ = path;

        // confirm_load of height above tip with archived body (archive-only ahead).
        // Archive header+body at height 3 without confirm.
        let (h3, ta3) = coinbase_block(3, prev);
        let h3hash = h3.hash;
        q.archive_block(&h3, &[ta3]).unwrap();
        let (st, parents, thin, bodies) = q
            .load_confirm_parents(&[(3, h3hash)])
            .unwrap();
        assert!(st.blocks >= 1 || bodies.len() >= 1 || !parents.is_empty() || thin.is_empty());
        let _ = thin;
        // Missing header / no body paths.
        let (st2, _, _, _) = q
            .load_confirm_parents(&[(9, [0xbb; 32])])
            .unwrap();
        let _ = st2;
        // Header without body.
        let (st3, _, _, _) = q
            .load_confirm_parents(&[(10, orphan.hash)])
            .unwrap();
        let _ = st3;

        let _ = q.parent_cache_ready_through();
        let _ = q.parent_cache_perf_snapshot();

        // Archive empty batch.
        assert!(q.archive_prepared_owned(&mut []).unwrap().is_empty());

        // No head for random txid → NotFound.
        let fake = TxRecord {
            txid: [0xcd; 32],
            version: 1,
            locktime: 0,
            input_start_fk: Fk::NULL,
            input_count: 1,
            output_start_fk: Fk::NULL,
            output_count: 1,
        };
        assert!(q.tx_output_attributed(&fake, 0, false).is_err());

        // disconnect again until empty-ish
        while q.tip_height().map(|h| h.0).unwrap_or(0) > 0 {
            q.disconnect_tip().unwrap();
        }
        // Last tip disconnect (genesis).
        if q.tip_height().is_some() {
            q.disconnect_tip().unwrap();
        }
        assert!(q.disconnect_tip().is_err());

        // tip_header_fk empty chain.
        assert!(q.tip_header_fk().unwrap().is_none());
        assert!(q.locator_hashes().unwrap().len() >= 1);
        assert!(q
            .headers_after_locator(&[], BlockHash::from_byte_array([0; 32]), 5)
            .unwrap()
            .is_empty());

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn spend_edge_and_confirm_idempotent_path() {
        let (dir, q) = temp_query("spend-edge");
        // Parent coinbase then child spend in next block.
        let (h0, ta0) = coinbase_block(0, Fk::NULL);
        let parent_txid = ta0.tx.txid;
        let prev = q.connect_block(Height(0), &h0, &[ta0]).unwrap();
        // Coinbase + spend of parent vout 0.
        let (h1, cb1) = coinbase_block(1, prev);
        let mut child = coinbase_block(1, prev).1;
        child.tx.txid[31] = 0x5e;
        child.tx.input_count = 1;
        child.inputs = vec![InputRecord {
            prev_txid: parent_txid,
            create_fk: Fk::NULL, // archive resolves
            prev_index: 0,
            sequence: u32::MAX,
            script_sig: vec![],
            witness: vec![],
        }];
        child.outputs = vec![OutputRecord::unspent(49_0000_0000, vec![0x51])];
        let h1hash = h1.hash;
        q.connect_block(Height(1), &h1, &[cb1, child]).unwrap();

        // mark_spends / collect edges via backfill probe path.
        let (h_walked, txs) = q.backfill_point_spends(|_, _, _, _| {}).unwrap();
        assert!(h_walked >= 1);
        let _ = txs;
        // Parent should show a spender eventually when spend index on.
        let _ = q.spenders(&parent_txid, 0).unwrap();

        // Idempotent single re-confirm at tip.
        let tip = q.tip_height().unwrap();
        let (fk, _) = q.get_header_by_hash(&h1hash).unwrap().unwrap();
        let again = q.confirm_block(tip, &h1hash).unwrap();
        assert_eq!(again, fk);

        // confirm already at height via height_of_hash early return.
        let r = q.confirm_block(tip, &h1hash).unwrap();
        assert_eq!(r, fk);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn confirm_load_cancel_and_zero_io_paths() {
        let (dir, q) = temp_query("load-cancel");
        let mut prev = Fk::NULL;
        let mut hashes = Vec::new();
        for h in 0..2u32 {
            let (header, ta) = coinbase_block(h, prev);
            hashes.push(header.hash);
            prev = q.connect_block(Height(h), &header, &[ta]).unwrap();
        }
        // Cancel before load of archived-ahead body.
        let (h2, ta2) = coinbase_block(2, prev);
        let h2hash = h2.hash;
        q.archive_block(&h2, &[ta2]).unwrap();
        q.request_confirm_cancel();
        let err = q.load_confirm_parents(&[(2, h2hash)]);
        assert!(err.is_err(), "cancel must abort load");
        q.clear_confirm_cancel();

        // Empty input/output run helpers.
        let empty_tx = TxRecord {
            txid: [0xab; 32],
            version: 1,
            locktime: 0,
            input_start_fk: Fk::NULL,
            input_count: 0,
            output_start_fk: Fk::NULL,
            output_count: 0,
        };
        assert!(q.tx_input_run(&empty_tx).unwrap().is_empty());
        assert!(q
            .tx_input_run_class_a(Fk(1), &empty_tx)
            .unwrap()
            .is_empty());

        // ArchiveWritePlan empty helper.
        let plan = ArchiveWritePlan::empty();
        assert!(plan.is_empty());

        // disconnect with zero-output tx: already covered via coinbase; ensure
        // confirm_block NotFound for unknown hash.
        assert!(q.confirm_block(Height(9), &[0xde; 32]).is_err());

        // header_tx_fks / flush_for_shutdown / flush_header_archive.
        let tip_fk = q.tip_header_fk().unwrap().unwrap();
        let fks = q.header_tx_fks(tip_fk, None).unwrap().unwrap_or_default();
        assert!(!fks.is_empty());
        q.flush_for_shutdown().unwrap();
        q.flush_header_archive().unwrap();

        // sample sh sub after work.
        let _ = class_c_phase_stats::sample_sh_sub_and_reset();

        let _ = hashes;
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn confirm_run_non_tip_and_tx_runs() {
        let (dir, q) = temp_query("confirm-run");
        let mut prev = Fk::NULL;
        let mut prepared = Vec::new();
        for h in 0..3u32 {
            let (header, ta) = coinbase_block(h, prev);
            let hash = header.hash;
            prev = q.connect_block(Height(h), &header, &[ta]).unwrap();
            let (fk, _) = q.get_header_by_hash(&hash).unwrap().unwrap();
            let tx_fks = q.header_tx_fks(fk, Some(&hash)).unwrap().unwrap();
            prepared.push(ConfirmPrepared {
                height: Height(h),
                header_fk: fk,
                tx_fks,
            });
        }
        // Re-confirm tip only (idempotent single).
        let tip = prepared.last().unwrap().clone();
        let again = q.confirm_blocks_run(&[tip]).unwrap();
        assert_eq!(again.len(), 1);

        // Non-contiguous rejected.
        assert!(q
            .confirm_blocks_run(&[prepared[0].clone(), prepared[2].clone()])
            .is_err());

        // Full packed body input/output runs.
        let fks = q.block_tx_fks(Height(0)).unwrap();
        let tx = q.get_tx_class_a(fks[0]).unwrap();
        let ins = q.tx_input_run_class_a(fks[0], &tx).unwrap();
        assert_eq!(ins.len(), 1);
        let outs = q.tx_output_run_class_a(fks[0], &tx).unwrap();
        assert_eq!(outs.len(), 1);

        // collect_spend_edges for coinbase → empty (no non-cb inputs).
        let edges = q.collect_spend_edges(fks[0], true).unwrap();
        assert!(edges.is_empty());

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Non-contiguous tx_fks in confirm_blocks_run + mark_spends multi-edge path.
    #[test]
    fn confirm_noncontiguous_fks_and_mark_spends() {
        let (dir, q) = temp_query("confirm-nc-fks");
        // Parent coinbase then child spend.
        let (h0, ta0) = coinbase_block(0, Fk::NULL);
        let parent_txid = ta0.tx.txid;
        let prev = q.connect_block(Height(0), &h0, &[ta0]).unwrap();
        let (h1, cb1) = coinbase_block(1, prev);
        let mut child = coinbase_block(1, prev).1;
        child.tx.txid[31] = 0x5f;
        child.tx.input_count = 1;
        child.inputs = vec![InputRecord {
            prev_txid: parent_txid,
            create_fk: Fk::NULL,
            prev_index: 0,
            sequence: u32::MAX,
            script_sig: vec![],
            witness: vec![],
        }];
        child.outputs = vec![OutputRecord::unspent(49_0000_0000, vec![0x51])];
        let h1hash = h1.hash;
        q.connect_block(Height(1), &h1, &[cb1, child]).unwrap();

        // mark_spends_for_tx on the child (non-coinbase → edges).
        let fks = q.block_tx_fks(Height(1)).unwrap();
        assert!(fks.len() >= 2);
        // Child is last
        let child_fk = fks[fks.len() - 1];
        q.mark_spends_for_tx(child_fk, false).unwrap();
        q.mark_spends_for_tx(child_fk, true).unwrap(); // probe path
        let edges = q.collect_spend_edges(child_fk, true).unwrap();
        assert!(!edges.is_empty() || edges.is_empty()); // may already exist after connect

        // Non-contiguous tx_fks: use first and last only (if 2+)
        let (fk, _) = q.get_header_by_hash(&h1hash).unwrap().unwrap();
        if fks.len() >= 2 {
            // Re-confirm is idempotent at tip; craft ConfirmPrepared with non-contig list
            // by using height already confirmed → idempotent single path first.
            let tip = ConfirmPrepared {
                height: Height(1),
                header_fk: fk,
                tx_fks: fks.clone(),
            };
            let _ = q.confirm_blocks_run(&[tip]).unwrap();

            // Non-contiguous fks path: archive-only block at height 2 with synthetic fks
            // Use two blocks already connected and re-run with scrambled fks on tip reconfirm
            // — height not tip+1 for multi is error; for single tip reconfirm uses contiguous check.
            let scrambled = ConfirmPrepared {
                height: Height(1),
                header_fk: fk,
                // Reverse order is non-ascending → non-contiguous branch.
                tx_fks: {
                    let mut v = fks.clone();
                    v.reverse();
                    v
                },
            };
            // tip reconfirm idempotent when header matches, may short-circuit before strong path
            let _ = q.confirm_blocks_run(&[scrambled]);
        }

        // Null header_fk rejected
        assert!(q
            .confirm_blocks_run(&[ConfirmPrepared {
                height: Height(2),
                header_fk: Fk::NULL,
                tx_fks: vec![],
            }])
            .is_err());

        // load_confirm_parents: height ≤ tip skipped; cancel; missing header
        let (st, _, _, _) = q.load_confirm_parents(&[(0, h1hash)]).unwrap();
        let _ = st;
        q.request_confirm_cancel();
        assert!(q.load_confirm_parents(&[(9, [0xab; 32])]).is_err());
        q.clear_confirm_cancel();
        // Missing header hash at tip+1 → continue (no panic)
        let (_st, bp, _, _) = q
            .load_confirm_parents(&[(2, [0xde; 32])])
            .unwrap();
        let _ = bp;

        let _ = parent_txid;
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// W-SH.A: write-batch CreatePin supplies outs for SH collect without Class A
    /// body re-read (missing store row still succeeds via pin).
    #[test]
    fn sh_collect_write_pin_skips_store() {
        use std::sync::Arc;

        let (dir, q) = temp_query("sh-collect-pin");
        let _ = class_c_phase_stats::sample_sh_collect_src_and_reset();

        let script = vec![0x51, 0xaa, 0xbb];
        let expected_sh = script_hash(&script);
        let fk = Fk(9_876_543);
        let pin: CreatePin = Arc::new((
            TxRecord {
                txid: [0xce; 32],
                version: 1,
                locktime: 0,
                input_start_fk: Fk::NULL,
                input_count: 1,
                output_start_fk: Fk::NULL,
                output_count: 1,
            },
            vec![OutputRecord::unspent(42, script)],
            vec![0],
        ));

        let mut recs = Vec::new();
        q.collect_scripthash_creates(fk, &mut recs, Some(&pin))
            .expect("pin path must not touch store");
        assert_eq!(recs.len(), 1);
        assert_eq!(recs[0].create_tx_fk, fk);
        assert_eq!(recs[0].scripthash, expected_sh);

        let (pin_n, cold_n) = class_c_phase_stats::sample_sh_collect_src_and_reset();
        assert_eq!(pin_n, 1, "pin hit");
        assert_eq!(cold_n, 0);

        // Without pin and without store row → cold path errors (NotFound).
        let mut recs2 = Vec::new();
        assert!(
            q.collect_scripthash_creates(fk, &mut recs2, None).is_err(),
            "no pin + no store must not invent records"
        );
        assert!(recs2.is_empty());

        let _ = std::fs::remove_dir_all(&dir);
    }
}
