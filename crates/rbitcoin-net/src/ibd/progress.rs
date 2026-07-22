//! Work-chain progress snapshot, percent helpers, and rate/ETA for IBD logs.

use super::body::BodyPresence;
use crate::chain::ChainHub;
use bitcoin::BlockHash;
use std::collections::{HashSet, VecDeque};
use std::time::{Duration, Instant};

/// Work-chain progress for status / progress logs.
///
/// - `tip`: confirmed best-chain height
/// - `archived`: Class A **high-water** height on the work path (logged as
///   `arch_hwm=`; gap-fills below do not move it — see `arch_total=` / `+arch=`)
/// - `headers`: max peer-advertised / learned header height
/// - `tip_hole`: contiguous unarchived run at the ordered front (blocks tip)
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct WorkChainProgress {
    pub tip: u32,
    pub archived: u32,
    pub headers: u32,
    pub tip_hole: usize,
}

/// Build a status snapshot without walking the full ordered path.
///
/// Full scans were O(path) every 5s on 90k+ headers. High-water marks already
/// track archived/header horizon; only `tip_hole` needs a short front walk.
pub(crate) fn work_chain_progress(
    hub: &ChainHub,
    ordered: &VecDeque<BlockHash>,
    ordered_set: &HashSet<BlockHash>,
    body: &mut BodyPresence,
    max_peer_height: u32,
    max_archived_height: u32,
) -> WorkChainProgress {
    let tip = hub.tip_height().unwrap_or(0);
    let mut tip_hole = 0usize;
    for h in ordered.iter() {
        // Skip ghosts (removed from set but not yet compacted out of the deque).
        if !ordered_set.contains(h) {
            continue;
        }
        // Confirmed prefix still on the deque until trim — not a download hole.
        if hub.has_block(h) {
            continue;
        }
        // First unconfirmed live hash: if not Class A ready, it blocks tip.
        if body.ready(hub, h) {
            break;
        }
        tip_hole += 1;
    }
    WorkChainProgress {
        tip,
        archived: tip.max(max_archived_height),
        headers: tip.max(max_peer_height),
        tip_hole,
    }
}

/// Confirmed tip as a percent of network/header horizon (0–100).
/// Denominator is max(our known headers, best peer-advertised tip).
pub(crate) fn ibd_pct(tip: u32, horizon: u32) -> u32 {
    let denom = horizon.max(tip).max(1);
    ((u64::from(tip) * 100) / u64::from(denom)) as u32
}

/// Rolling tip samples for genuine rate / ETA (not last-batch chunk size).
///
/// Samples are pushed on the centralized 5s status tick. Rates use the oldest
/// sample still inside the requested window (up to ~1h retained).
pub(crate) struct TipRateTracker {
    /// `(when, tip_height)` ascending by time.
    samples: VecDeque<(Instant, u32)>,
    retain: Duration,
}

impl TipRateTracker {
    pub(crate) fn new() -> Self {
        Self {
            samples: VecDeque::new(),
            retain: Duration::from_secs(3600 + 30), // 1h + slack
        }
    }

    /// Record a sample (call once per centralized status tick).
    pub(crate) fn push(&mut self, now: Instant, tip: u32) {
        self.samples.push_back((now, tip));
        self.prune(now);
    }

    fn prune(&mut self, now: Instant) {
        let cutoff = now.checked_sub(self.retain).unwrap_or(now);
        while self.samples.len() > 2 {
            if let Some(&(t, _)) = self.samples.front() {
                if t < cutoff {
                    self.samples.pop_front();
                    continue;
                }
            }
            break;
        }
    }

    /// Blocks/sec of tip advance over the last `window` (using samples).
    ///
    /// Returns `None` if fewer than two samples or zero elapsed.
    pub(crate) fn rate_over(&self, now: Instant, window: Duration) -> Option<f64> {
        if self.samples.len() < 2 {
            return None;
        }
        let &(_, tip_now) = self.samples.back()?;
        let start = now.checked_sub(window).unwrap_or(now);
        // Oldest sample at or after `start`; if all newer, use oldest available.
        let mut base = *self.samples.front()?;
        for &(t, h) in &self.samples {
            if t <= start {
                base = (t, h);
            } else {
                break;
            }
        }
        let (t0, tip0) = base;
        let secs = now.duration_since(t0).as_secs_f64();
        if secs < 1.0 {
            return None;
        }
        let delta = tip_now.saturating_sub(tip0) as f64;
        Some(delta / secs)
    }

    /// Human ETA from tip→horizon using ~1h tip rate. Empty if not progressing.
    pub(crate) fn eta_string(&self, now: Instant, tip: u32, horizon: u32) -> String {
        let remain = horizon.saturating_sub(tip);
        if remain == 0 {
            return "done".into();
        }
        let Some(rate) = self.rate_over(now, Duration::from_secs(3600)) else {
            return "eta=?".into();
        };
        if rate < 1e-6 {
            return "eta=?".into();
        }
        let secs = (remain as f64 / rate).ceil() as u64;
        format!("eta={}", format_duration_short(secs))
    }
}

/// Compact duration for logs: `45s`, `12m`, `3.2h`, `2d5h`.
pub(crate) fn format_duration_short(secs: u64) -> String {
    if secs < 60 {
        return format!("{secs}s");
    }
    if secs < 3600 {
        return format!("{}m", (secs + 30) / 60);
    }
    if secs < 86400 {
        let h = secs as f64 / 3600.0;
        if h < 10.0 {
            return format!("{h:.1}h");
        }
        return format!("{}h", (secs + 1800) / 3600);
    }
    let d = secs / 86400;
    let h = (secs % 86400) / 3600;
    if h == 0 {
        format!("{d}d")
    } else {
        format!("{d}d{h}h")
    }
}

/// Format a per-second rate for progress logs (`0`, `2.4`, `31`).
pub(crate) fn format_rate(rate: f64) -> String {
    if !rate.is_finite() || rate < 0.0 {
        return "0".into();
    }
    if rate < 10.0 {
        format!("{rate:.1}")
    } else {
        format!("{rate:.0}")
    }
}

/// Pure tip-hole count over an ordered path using a boolean ready map
/// (unit-test helper; production uses [`work_chain_progress`]).
#[cfg(test)]
pub(crate) fn tip_hole_from_ready(ready_flags: &[bool]) -> usize {
    let mut tip_hole = 0usize;
    for &ready in ready_flags {
        if ready {
            break;
        }
        tip_hole += 1;
    }
    tip_hole
}

#[cfg(test)]
mod tests {
    use super::{
        format_duration_short, format_rate, ibd_pct, tip_hole_from_ready, TipRateTracker,
    };
    use std::time::{Duration, Instant};

    #[test]
    fn pct_basic() {
        assert_eq!(ibd_pct(0, 100), 0);
        assert_eq!(ibd_pct(50, 100), 50);
        assert_eq!(ibd_pct(100, 100), 100);
    }

    #[test]
    fn pct_tip_above_horizon() {
        // ((200 * 100) / 200) when denom = max(tip, horizon) = 200
        assert_eq!(ibd_pct(200, 100), 100);
    }

    #[test]
    fn pct_zero_horizon() {
        assert_eq!(ibd_pct(0, 0), 0);
        assert_eq!(ibd_pct(5, 0), 100);
    }

    #[test]
    fn tip_hole_contiguous_front() {
        assert_eq!(tip_hole_from_ready(&[]), 0);
        assert_eq!(tip_hole_from_ready(&[true, false, false]), 0);
        assert_eq!(tip_hole_from_ready(&[false, false, true, false]), 2);
        assert_eq!(tip_hole_from_ready(&[false, false, false]), 3);
    }

    #[test]
    fn tip_rate_over_window() {
        let mut t = TipRateTracker::new();
        let t0 = Instant::now();
        t.push(t0, 1000);
        t.push(t0 + Duration::from_secs(100), 1200);
        let rate = t
            .rate_over(t0 + Duration::from_secs(100), Duration::from_secs(100))
            .unwrap();
        assert!((rate - 2.0).abs() < 0.01, "rate={rate}");
    }

    #[test]
    fn eta_and_format() {
        assert_eq!(format_duration_short(45), "45s");
        assert_eq!(format_duration_short(120), "2m");
        assert_eq!(format_rate(2.4), "2.4");
        assert_eq!(format_rate(31.2), "31");
        let mut t = TipRateTracker::new();
        let t0 = Instant::now();
        t.push(t0, 100);
        t.push(t0 + Duration::from_secs(3600), 100 + 3600); // 1 blk/s
        let eta = t.eta_string(t0 + Duration::from_secs(3600), 100 + 3600, 100 + 3600 + 7200);
        assert!(eta.contains("eta="), "{eta}");
        assert!(eta.contains("2.0h") || eta.contains("2h"), "{eta}");
    }
}
