//! Archive queue budget + budget + durable BQ rehydrate into confirm.
//!
//! Dual-track archive-job Class A pipeline was removed — confirm is sole Class A.

use crate::chain::ChainHub;
use bitcoin::hashes::Hash;
use bitcoin::BlockHash;
use rbitcoin_primitives::Fk;

use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

/// Shared archive-pipeline timers for IBD status (atomics; reset on sample).
#[derive(Default)]
pub(crate) struct ArchivePipelineStats {
    pub(crate) prep_ns: AtomicU64,
    pub(crate) prep_blocks: AtomicU64,
    pub(crate) write_ns: AtomicU64,
    pub(crate) write_batches: AtomicU64,
    pub(crate) write_blocks: AtomicU64,
    pub(crate) write_batch_blocks: AtomicU64,
    pub(crate) write_idle_ns: AtomicU64,
    pub(crate) write_coalesce_ns: AtomicU64,
}

impl ArchivePipelineStats {
    pub(crate) fn sample_and_reset(&self) -> ArchivePipelineSample {
        let prep_ns = self.prep_ns.swap(0, Ordering::Relaxed);
        let prep_blocks = self.prep_blocks.swap(0, Ordering::Relaxed);
        let write_ns = self.write_ns.swap(0, Ordering::Relaxed);
        let write_batches = self.write_batches.swap(0, Ordering::Relaxed);
        let write_blocks = self.write_blocks.swap(0, Ordering::Relaxed);
        let write_batch_blocks = self.write_batch_blocks.swap(0, Ordering::Relaxed);
        let write_idle_ns = self.write_idle_ns.swap(0, Ordering::Relaxed);
        let write_coalesce_ns = self.write_coalesce_ns.swap(0, Ordering::Relaxed);
        ArchivePipelineSample {
            prep_ns,
            prep_blocks,
            write_ns,
            write_batches,
            write_blocks,
            write_batch_blocks,
            write_idle_ns,
            write_coalesce_ns,
        }
    }
}

#[derive(Clone, Debug, Default)]
pub(crate) struct ArchivePipelineSample {
    pub(crate) prep_ns: u64,
    pub(crate) prep_blocks: u64,
    pub(crate) write_ns: u64,
    pub(crate) write_batches: u64,
    pub(crate) write_blocks: u64,
    pub(crate) write_batch_blocks: u64,
    pub(crate) write_idle_ns: u64,
    pub(crate) write_coalesce_ns: u64,
}

impl ArchivePipelineSample {
    pub(crate) fn prep_us_per_block(&self) -> u64 {
        if self.prep_blocks == 0 {
            0
        } else {
            (self.prep_ns / self.prep_blocks) / 1000
        }
    }
    pub(crate) fn write_us_per_block(&self) -> u64 {
        if self.write_blocks == 0 {
            0
        } else {
            (self.write_ns / self.write_blocks) / 1000
        }
    }
    pub(crate) fn avg_batch(&self) -> u64 {
        if self.write_batches == 0 {
            0
        } else {
            self.write_batch_blocks / self.write_batches
        }
    }
    pub(crate) fn write_busy_ms(&self) -> u64 {
        self.write_ns / 1_000_000
    }
    pub(crate) fn write_idle_ms(&self) -> u64 {
        self.write_idle_ns / 1_000_000
    }
    pub(crate) fn write_coalesce_ms(&self) -> u64 {
        self.write_coalesce_ns / 1_000_000
    }
    pub(crate) fn prep_ms(&self) -> u64 {
        self.prep_ns / 1_000_000
    }
}


/// Default RAM budget for decoded blocks waiting in the archive pipeline (~512 MiB).
/// Override with env `RBITCOIN_ARCHIVE_QUEUE_MB`.
///
/// Sized so network stays busy and ContigPark can form mega-batches without a
/// multi‑GiB junkyard. Wire-size undercounts true RSS of decoded `Block` + prep
/// (×1.5 charge); still stacked with parent-body decode + page cache.
pub const DEFAULT_ARCHIVE_QUEUE_BUDGET_BYTES: usize = 512 * 1024 * 1024;

/// Enter “pressure” (far_scale = 0) when fill ≥ this fraction of budget.
pub const ARCHIVE_PRESSURE_ENTER: f64 = 0.90;
/// Leave pressure only after fill ≤ this (hysteresis vs enter).
pub const ARCHIVE_PRESSURE_EXIT: f64 = 0.70;

/// Shared counter of blocks (and approx wire bytes) in the archive pipeline.
///
/// Charged when a decoded body is handed to the job channel; released when the
/// writer (or prep error path) returns [`ArchiveResult`].
///
/// **Assign gate only (never a receive / decode gate):** [`Self::can_assign`] is
/// false once charged fill ≥ budget — stop issuing new densify/cache getdata.
/// Bodies already requested must still be read from TCP, decoded, [`charge`]d,
/// and enqueued (may briefly overshoot). Soft [`Self::far_admission_scale`]
/// (proportional + 90%/70% hysteresis) scales densify capacity before the hard
/// stop. Do **not** stall peer reads or Full-drop arch_job for soft budget.
pub(crate) struct ArchiveQueueBudget {
    count: AtomicUsize,
    bytes: AtomicUsize,
    budget: usize,
    /// Latched high-fill mode: once ≥ [`ARCHIVE_PRESSURE_ENTER`], stays until
    /// ≤ [`ARCHIVE_PRESSURE_EXIT`].
    pressure: AtomicBool,
}

impl ArchiveQueueBudget {
    pub fn new(budget: usize) -> Self {
        Self {
            count: AtomicUsize::new(0),
            bytes: AtomicUsize::new(0),
            // At least 16 MiB so tiny overrides still leave room for a few blocks.
            budget: budget.max(16 * 1024 * 1024),
            pressure: AtomicBool::new(false),
        }
    }

    pub fn from_env() -> Arc<Self> {
        let budget = std::env::var("RBITCOIN_ARCHIVE_QUEUE_MB")
            .ok()
            .and_then(|s| s.parse::<usize>().ok())
            .map(|mb| mb.saturating_mul(1024 * 1024))
            .unwrap_or(DEFAULT_ARCHIVE_QUEUE_BUDGET_BYTES);
        Arc::new(Self::new(budget))
    }

    pub fn budget_bytes(&self) -> usize {
        self.budget
    }

    pub fn count(&self) -> usize {
        self.count.load(Ordering::Relaxed)
    }

    pub fn bytes(&self) -> usize {
        self.bytes.load(Ordering::Relaxed)
    }

    /// Charged bytes / budget (may be > 1 when oversubscribed).
    pub fn fill_ratio(&self) -> f64 {
        let b = self.budget.max(1) as f64;
        self.bytes() as f64 / b
    }

    /// Update pressure latch from current fill; return far admission scale in **0..=1**.
    ///
    /// - **Pressure (A):** enter at fill ≥ 0.90, exit only at fill ≤ 0.70 → scale 0.
    /// - **Proportional (B):** outside pressure, `scale = (1 - fill).clamp(0, 1)`
    ///   so half-full budget ≈ half far work (smooth BW, no cliff at budget).
    ///
    /// ContigPark gap + tip-near are **not** gated by this (assign always covers them).
    pub fn far_admission_scale(&self) -> f64 {
        let fill = self.fill_ratio();
        let was = self.pressure.load(Ordering::Relaxed);
        let (scale, pressure) = Self::far_scale_from(fill, was);
        self.pressure.store(pressure, Ordering::Relaxed);
        scale
    }

    /// Pure helper: scale from fill + pressure latch (shared by production + tests).
    pub(crate) fn far_scale_from(fill: f64, mut pressure: bool) -> (f64, bool) {
        if fill >= ARCHIVE_PRESSURE_ENTER {
            pressure = true;
        } else if fill <= ARCHIVE_PRESSURE_EXIT {
            pressure = false;
        }
        let scale = if pressure {
            0.0
        } else {
            (1.0 - fill).clamp(0.0, 1.0)
        };
        (scale, pressure)
    }

    /// Charge bytes (test / residual soft-budget helpers).
    #[cfg(test)]
    pub fn charge(&self, wire_bytes: usize) {
        let charged = Self::charged_bytes(wire_bytes);
        self.count.fetch_add(1, Ordering::Relaxed);
        self.bytes.fetch_add(charged, Ordering::Relaxed);
    }

    /// Same overhead as [`charge`] — callers must release the charged amount.
    pub fn charged_bytes(wire_bytes: usize) -> usize {
        wire_bytes.saturating_mul(3).saturating_add(4096) / 2
    }

    /// True while charged fill is **strictly below** budget — issue densify /
    /// confirm-cache getdata. Tip-hole and ContigPark race assign ignore this
    /// so a hole at `write_next` can still be filled when the queue is full.
    pub fn can_assign(&self) -> bool {
        self.bytes() < self.budget
    }

    /// Release after archive Ok/Err (or failed send into a closed pipeline).
    ///
    /// Pass the original **wire** size; overhead is re-derived via [`charged_bytes`].
    pub fn release(&self, wire_bytes: usize) {
        let charged = Self::charged_bytes(wire_bytes);
        let _ = self.count.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| {
            Some(n.saturating_sub(1))
        });
        let _ = self.bytes.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| {
            Some(n.saturating_sub(charged))
        });
    }
}

pub(crate) fn rehydrate_block_queue_into_confirm(
    hub: &ChainHub,
    st: &mut super::state::IbdWorkState,
    confirm_feed: &super::confirm::ConfirmFeed,
    _archive_queued: &ArchiveQueueBudget,
) -> Result<usize, String> {
    use rbitcoin_log::{info, warn};

    let tip_opt = hub.tip_height();
    let path_lo = match tip_opt {
        None => 0u32,
        Some(t) => t.saturating_add(1),
    };

    // Meta only — never `block_queue_load_all` (that materializes multi‑GiB of
    // wire into heap at every restart; feed only needs height/hash readiness).
    let queued = hub.query.block_queue_list_meta();
    if queued.is_empty() {
        return Ok(0);
    }
    let mut n = 0usize;
    let mut bytes = 0u64;
    let mut h_min = u32::MAX;
    let mut h_max = 0u32;
    let mut dropped_done = 0usize;
    let mut empty_skip = 0usize;
    let mut unknown_h = 0usize;
    let mut kept_above_tip_flag = 0usize;
    for qb in queued {
        let hash = BlockHash::from_byte_array(qb.hash);
        // Only drop residue at/below confirmed tip (write may have missed dequeue).
        // Heights above tip always keep wire — even if has_block/known looks set
        // (stale RAM or Class A ahead of tip must not erase confirm payload).
        // No tip yet → keep every height (including 0).
        let at_or_below_tip = match tip_opt {
            Some(tip) if qb.height != u32::MAX && qb.height <= tip => true,
            _ => false,
        };
        if at_or_below_tip {
            let _ = hub.query.block_queue_dequeue_height(qb.height);
            dropped_done = dropped_done.saturating_add(1);
            continue;
        }
        if hub.has_block(&hash) || st.body.is_known_archived(&hash) {
            // height > tip but already flagged done — keep payload, still note feed.
            kept_above_tip_flag = kept_above_tip_flag.saturating_add(1);
        }
        // Minimal integrity: rec must have non-empty payload_len; full decode at prep.
        if qb.payload_len == 0 {
            empty_skip = empty_skip.saturating_add(1);
            let _ = hub.query.block_queue_dequeue_height(qb.height);
            continue;
        }
        let wire_bytes = qb.payload_len;
        // Disk-owned: pending so densify will not re-getdata; no soft charge.
        st.body.mark_pending(hash);
        if qb.height != u32::MAX {
            st.record_height(hash, qb.height);
        }
        let header_fk = Fk(qb.header_fk);
        if !header_fk.is_null() {
            st.header_fks.insert(hash, header_fk);
        }
        if qb.height != u32::MAX {
            // Readiness only — prep reloads wire from body queue.
            confirm_feed.note(qb.height, hash);
            bytes = bytes.saturating_add(wire_bytes);
            h_min = h_min.min(qb.height);
            h_max = h_max.max(qb.height);
            n = n.saturating_add(1);
        } else {
            // Unknown height: cannot feed confirm path.
            st.body.mark_missing(hash);
            unknown_h = unknown_h.saturating_add(1);
        }
    }

    // Tip+1..bq_min gap: wire was dequeued/never filled while tip lagged → densify.
    // Skip heights already claimable from confirmed set or Class A (resume seed).
    let mut gap_marked = 0u32;
    if n > 0 && h_min > path_lo {
        for ht in path_lo..h_min {
            let Some(&hash) = st.height_to_hash.get(&ht) else {
                continue;
            };
            if hub.has_block(&hash)
                || st.body.is_known_archived(&hash)
                || hub.is_archived(&hash)
            {
                continue;
            }
            st.body.mark_missing(hash);
            gap_marked = gap_marked.saturating_add(1);
        }
        if gap_marked > 0 {
            warn!(
                "ibd: body queue gap tip+1={path_lo}..{} (bq starts {h_min}) — \
                 marked {gap_marked} missing for densify re-getdata",
                h_min.saturating_sub(1)
            );
        }
    }

    // One summary line for partial-IBD restart (no per-rec spam).
    if n > 0 || dropped_done > 0 || empty_skip > 0 || unknown_h > 0 || gap_marked > 0 {
        let mib = bytes / (1024 * 1024);
        if n > 0 {
            info!(
                "ibd: rehydrate body queue → feed ready n={n} h={h_min}..{h_max} \
                 {mib}MiB (dropped_le_tip={dropped_done} empty={empty_skip} unknown_h={unknown_h} \
                 gap_marked={gap_marked} kept_above_tip_flag={kept_above_tip_flag})"
            );
        } else {
            info!(
                "ibd: rehydrate body queue: no ready entries \
                 (dropped_le_tip={dropped_done} empty={empty_skip} unknown_h={unknown_h} \
                 gap_marked={gap_marked})"
            );
        }
        if empty_skip > 0 {
            warn!(
                "ibd: rehydrate dropped {empty_skip} empty body-queue rec(s)"
            );
        }
    }
    Ok(n)
}

#[cfg(test)]
mod budget_tests {
    use super::ArchiveQueueBudget;

    #[test]
    fn budget_charge_release() {
        let b = ArchiveQueueBudget::new(1024 * 1024);
        assert!(b.can_assign());
        b.charge(1000);
        assert_eq!(b.count(), 1);
        b.release(1000);
        assert_eq!(b.count(), 0);
    }
}
