//! Archive queue budget + budget + durable BQ rehydrate into confirm.
//!
//! Dual-track archive-job Class A pipeline was removed — confirm is sole Class A.

use crate::chain::ChainHub;
use bitcoin::hashes::Hash;
use bitcoin::BlockHash;
use rbitcoin_primitives::Fk;

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

/// Default soft densify budget (~512 MiB). Override with `RBITCOIN_ARCHIVE_QUEUE_MB`.
///
/// Historically charged dual-track archive jobs; now a soft densify/far-scale
/// meter only (no job charge/release on the unified body-queue path).
pub const DEFAULT_ARCHIVE_QUEUE_BUDGET_BYTES: usize = 512 * 1024 * 1024;

/// Enter “pressure” (far_scale = 0) when fill ≥ this fraction of budget.
pub const ARCHIVE_PRESSURE_ENTER: f64 = 0.90;
/// Leave pressure only after fill ≤ this (hysteresis vs enter).
pub const ARCHIVE_PRESSURE_EXIT: f64 = 0.70;

/// Soft densify meter for getdata admission (not a receive/decode gate).
///
/// [`Self::can_assign`] is false once fill ≥ budget — stop new densify getdata.
/// Soft [`Self::far_admission_scale`] (proportional + 90%/70% hysteresis) scales
/// densify capacity before the hard stop. Dual-track job charge/release is gone;
/// without a charger this stays empty and always admits (durable BQ soft depth
/// is the primary densify gate).
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
    /// Tip-hole race is **not** gated by this (assign always covers tip holes).
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

    /// True while charged fill is **strictly below** budget — issue densify
    /// getdata. Soft meter (no dual-track job charges on the unified path).
    pub fn can_assign(&self) -> bool {
        self.bytes() < self.budget
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

    // Index only. After restart the RAM queue is empty (by design — sole durable
    // write is Class A; redownload instead of double disk write). Same-process
    // residual still notes feed readiness.
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
        // Minimal integrity: rec must have non-empty payload_len; full decode at confirm load.
        if qb.payload_len == 0 {
            empty_skip = empty_skip.saturating_add(1);
            let _ = hub.query.block_queue_dequeue_height(qb.height);
            continue;
        }
        let wire_bytes = qb.payload_len;
        // Queue-owned: pending so densify will not re-getdata; no soft charge.
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
            if hub.has_block(&hash) || st.body.is_known_archived(&hash) || hub.is_archived(&hash) {
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
            warn!("ibd: rehydrate dropped {empty_skip} empty body-queue rec(s)");
        }
    }
    Ok(n)
}

#[cfg(test)]
mod budget_tests {
    use super::ArchiveQueueBudget;

    #[test]
    fn budget_empty_can_assign() {
        let b = ArchiveQueueBudget::new(1024 * 1024);
        assert!(b.can_assign());
        assert_eq!(b.count(), 0);
        assert!((b.far_admission_scale() - 1.0).abs() < 1e-9);
    }
}
