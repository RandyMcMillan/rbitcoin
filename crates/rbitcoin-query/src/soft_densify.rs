//! Body-queue soft densify assign policy (IBD getdata window).

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
    path_lo.saturating_add(n.saturating_sub(1)).min(densify_hi)
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
