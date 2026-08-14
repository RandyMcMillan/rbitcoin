//! Confirm **load** stage types shared with wire pin / assemble.
//!
//! Parent outs / denserels are pipeline-local ([`crate::BatchParents`]).
//! Thin edges are batch-local ([`BatchThin`]). Header plans live on
//! [`crate::confirm_parent_cache::ConfirmParentCache`].

use super::*;
use crate::wave_prevout::ThinInput;
use crate::U64Map;

/// Spend-fk → thin create_fk edges for one confirm batch (assemble only).
pub type BatchThin = U64Map<Vec<ThinInput>>;

#[derive(Debug, Default, Clone, Copy)]
pub struct ConfirmLoadStats {
    pub blocks: u32,
    pub utxo_parents: u32,
    pub creates_registered: u32,
    /// Unique parent create fks pinned this call (after dedup).
    pub parent_unique: u32,
    /// Of `parent_unique`: filled without store denserels IO (same-batch / plan-local).
    pub pin_cache_body: u32,
    /// Of `parent_unique`: missed same-batch (cold denserels).
    pub pin_new: u32,
    /// FIFO hit path resolve.
    pub pin_body_ns: u64,
    /// pin_new meta/outs resolve (excludes spent timer).
    pub pin_new_meta_ns: u64,
    /// Same-batch create edges (identity known in-batch).
    pub parent_cache_hits: u32,
    /// Stamped create_fk on input, parent **not** in this batch (external fk).
    pub edge_fk: u32,
    /// Body txs full-decoded (phase 1).
    pub body_tx_reads: u32,
    /// Parent outs loaded from store (sparse pin).
    pub full_tx_reads: u32,
    /// Unstamped non-coinbase edges (should not occur on healthy v10 Class A).
    pub missing_parents: u32,
    /// Phase wall times (ns).
    pub header_ns: u64,
    pub body_decode_ns: u64,
    pub thin_ns: u64,
    pub parent_pin_ns: u64,
    pub cache_put_ns: u64,
    pub edge_same_batch: u32,
    pub edge_coinbase: u32,
}

impl Query {
    /// Snapshot: `(ready_through, ahead, sparse_parents, bodies, header_plans)`.
    ///
    /// Scan watermark is gone (wire pin is the load path). `ready_through` /
    /// `ahead` / sparse / bodies stay 0 so IBD `ibd: sizes` tuple shape is
    /// unchanged; `header_plans` is the live occupancy.
    pub fn parent_cache_perf_snapshot(&self) -> (u32, u32, usize, usize, usize) {
        (0, 0, 0, 0, self.confirm_parents.header_plan_count())
    }

    pub fn advance_parent_cache_tip(&self, tip: u32) {
        self.confirm_parents.advance_tip(tip);
    }
}
