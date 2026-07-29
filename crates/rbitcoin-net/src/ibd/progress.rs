//! Work-chain progress snapshot, percent helpers, and rate/ETA for IBD logs.
//!
//! The operator `ibd: progress` line reports tip pace and durable block-queue
//! occupancy (schema 12), not a separate Class A HWM / archive lead story.

use super::body::BodyPresence;
use crate::chain::ChainHub;
use bitcoin::BlockHash;
use std::collections::{HashSet, VecDeque};
use std::time::Instant;

/// Work-chain progress for status / progress logs.
///
/// - `tip`: confirmed best-chain height
/// - `archived`: Class A high-water on the work path (used for download lead /
///   densify bookkeeping; **not** printed as `arch_hwm=` on the progress line)
/// - `headers`: max peer-advertised / learned header height
/// - `tip_hole`: contiguous unarchived run at the ordered front (blocks tip)
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct WorkChainProgress {
    pub tip: u32,
    pub archived: u32,
    pub headers: u32,
    pub tip_hole: usize,
}

/// Inputs for the pure `ibd: progress` line formatter (unit-tested).
#[derive(Clone, Debug)]
pub(crate) struct ProgressLineInput {
    pub pct: u32,
    pub tip: u32,
    /// Tip advance rate over the last sample window (blocks/s).
    pub tip_rate: f64,
    pub tip_hole: usize,
    pub peers: usize,
    /// Confirm pipeline depths already formatted (`prepq… writeq…`).
    pub conf_q: String,
    pub txs: u64,
    pub horizon: u32,
    /// From [`TipRateTracker::eta_string`] (`eta=…` or `done`).
    pub eta: String,
    /// Durable on-disk block queue: (budget_bytes, used_bytes, entry_count).
    pub bq_budget: u64,
    pub bq_bytes: u64,
    pub bq_count: usize,
}

/// Build the `ibd: progress …` message body (no log level prefix).
///
/// Tip percent/rate, download hole, peers, confirm queues, txs, header horizon,
/// tip-rate ETA, and durable block-queue occupancy. Does **not** emit
/// `arch_hwm=`, a separate archive rate, or Class A `lead=`.
pub(crate) fn format_progress_line(i: &ProgressLineInput) -> String {
    let bq_mib = i.bq_bytes / (1024 * 1024);
    let bq_budget_mib = i.bq_budget / (1024 * 1024);
    format!(
        "ibd: progress {}% tip={} ({}/s) hole={} peers={} {} txs={} horizon={} {} bq={} ({}MiB/{}MiB)",
        i.pct,
        i.tip,
        format_rate(i.tip_rate),
        i.tip_hole,
        i.peers,
        i.conf_q,
        i.txs,
        i.horizon,
        i.eta,
        i.bq_count,
        bq_mib,
        bq_budget_mib,
    )
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

/// Half-life for the tip-rate EWMA used by ETA (seconds).
///
/// ~90s: recent IBD pace dominates (not the last hour of early empty blocks).
/// A single 5s spike only moves the estimate a few percent; a sustained regime
/// change is mostly reflected within ~5–6 minutes.
const ETA_RATE_HALF_LIFE_SECS: f64 = 90.0;

/// Need at least this much wall time of samples before publishing an ETA.
const ETA_MIN_ELAPSED_SECS: f64 = 20.0;

/// Present-biased smoothed tip rate for IBD ETA logs.
///
/// Samples are pushed on the centralized ~5s status tick. ETA uses an EWMA of
/// inter-sample rates (half-life [`ETA_RATE_HALF_LIFE_SECS`]): recent pace
/// dominates, but a single 5s spike only moves the estimate a few percent.
pub(crate) struct TipRateTracker {
    /// EWMA of tip advance rate (blocks/s).
    rate_ema: Option<f64>,
    /// Previous sample used to form the next instantaneous interval rate.
    last: Option<(Instant, u32)>,
    /// First sample time (gate ETA until we have a few ticks of history).
    first_at: Option<Instant>,
}

impl TipRateTracker {
    pub(crate) fn new() -> Self {
        Self {
            rate_ema: None,
            last: None,
            first_at: None,
        }
    }

    /// Record a sample (call once per centralized status tick).
    pub(crate) fn push(&mut self, now: Instant, tip: u32) {
        if self.first_at.is_none() {
            self.first_at = Some(now);
        }
        if let Some((t0, tip0)) = self.last {
            let dt = now.duration_since(t0).as_secs_f64();
            // Ignore zero/negative clock skew; tiny dt would explode inst rate.
            if dt >= 0.5 {
                let inst = tip.saturating_sub(tip0) as f64 / dt;
                let inst = if inst.is_finite() && inst >= 0.0 {
                    inst
                } else {
                    0.0
                };
                self.rate_ema = Some(match self.rate_ema {
                    None => inst,
                    Some(prev) => {
                        // alpha = 1 - exp(-ln2 * dt / half_life)
                        let alpha = 1.0
                            - (-std::f64::consts::LN_2 * dt / ETA_RATE_HALF_LIFE_SECS).exp();
                        // Bound so a pathological long gap cannot fully reset.
                        let alpha = alpha.clamp(0.02, 0.5);
                        alpha * inst + (1.0 - alpha) * prev
                    }
                });
            }
        }
        self.last = Some((now, tip));
    }

    /// Smoothed tip rate (blocks/s) for ETA: present-biased EWMA.
    ///
    /// Returns `None` until enough wall time has elapsed for a stable read.
    pub(crate) fn eta_rate(&self, now: Instant) -> Option<f64> {
        let first = self.first_at?;
        let elapsed = now.duration_since(first).as_secs_f64();
        if elapsed < ETA_MIN_ELAPSED_SECS {
            return None;
        }
        let ema = self.rate_ema.filter(|r| r.is_finite() && *r > 1e-6)?;
        Some(ema)
    }

    /// Human ETA from tip→horizon using smoothed present-biased tip rate.
    pub(crate) fn eta_string(&self, now: Instant, tip: u32, horizon: u32) -> String {
        let remain = horizon.saturating_sub(tip);
        if remain == 0 {
            return "done".into();
        }
        let Some(rate) = self.eta_rate(now) else {
            return "eta=?".into();
        };
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
        format_duration_short, format_progress_line, format_rate, ibd_pct, tip_hole_from_ready,
        ProgressLineInput, TipRateTracker,
    };
    use std::time::{Duration, Instant};
    // TipRateTracker used by both format and EWMA surfaces.

    /// Shipped progress line: tip + durable bq; no Class A arch_hwm / arch rate / lead.
    #[test]
    fn format_progress_line_schema12_tokens() {
        // tip_rate 12.5 → format_rate rounds to "12" (≥10).
        let line = format_progress_line(&ProgressLineInput {
            pct: 42,
            tip: 100_000,
            tip_rate: 12.5,
            tip_hole: 3,
            peers: 8,
            conf_q: "prepq=1/2 writeq=0/2".into(),
            txs: 50_000_000,
            horizon: 900_000,
            eta: "eta=18h".into(),
            bq_budget: 4 * 1024 * 1024 * 1024,
            bq_bytes: 256 * 1024 * 1024,
            bq_count: 17,
        });
        assert_eq!(
            line,
            "ibd: progress 42% tip=100000 (12/s) hole=3 peers=8 prepq=1/2 writeq=0/2 txs=50000000 horizon=900000 eta=18h bq=17 (256MiB/4096MiB)"
        );
        // Legacy dual-pipeline columns must not appear.
        assert!(
            !line.contains("arch_hwm"),
            "must not report Class A arch_hwm: {line}"
        );
        assert!(!line.contains("lead="), "must not report archive lead=: {line}");
        // A second rate next to an arch column is gone; only tip (/s) remains.
        assert_eq!(
            line.matches("/s)").count(),
            1,
            "only tip rate on progress line: {line}"
        );
        // Low tip rate keeps one decimal (still a single /s).
        let slow = format_progress_line(&ProgressLineInput {
            pct: 1,
            tip: 10,
            tip_rate: 2.4,
            tip_hole: 0,
            peers: 1,
            conf_q: "prepq<0/2 writeq<0/2".into(),
            txs: 1,
            horizon: 1000,
            eta: "eta=?".into(),
            bq_budget: 1024 * 1024,
            bq_bytes: 0,
            bq_count: 0,
        });
        assert!(slow.contains("tip=10 (2.4/s)"), "{slow}");
        assert!(slow.contains("bq=0 (0MiB/1MiB)"), "{slow}");
        assert!(!slow.contains("arch_hwm") && !slow.contains("lead="), "{slow}");
    }

    /// Pure helpers for progress lines (pct / tip-hole / duration formatting).
    #[test]
    fn pct_tip_hole_and_format_surface() {
        assert_eq!(ibd_pct(0, 100), 0);
        assert_eq!(ibd_pct(50, 100), 50);
        assert_eq!(ibd_pct(100, 100), 100);
        // denom = max(tip, horizon)
        assert_eq!(ibd_pct(200, 100), 100);
        assert_eq!(ibd_pct(0, 0), 0);
        assert_eq!(ibd_pct(5, 0), 100);

        assert_eq!(tip_hole_from_ready(&[]), 0);
        assert_eq!(tip_hole_from_ready(&[true, false, false]), 0);
        assert_eq!(tip_hole_from_ready(&[false, false, true, false]), 2);
        assert_eq!(tip_hole_from_ready(&[false, false, false]), 3);

        assert_eq!(format_duration_short(45), "45s");
        assert_eq!(format_duration_short(59), "59s");
        assert_eq!(format_duration_short(60), "1m");
        assert_eq!(format_duration_short(120), "2m");
        assert_eq!(format_duration_short(3_600), "1.0h");
        assert_eq!(format_duration_short(9 * 3600), "9.0h");
        assert_eq!(format_duration_short(36_000), "10h");
        assert_eq!(format_duration_short(86_400), "1d");
        assert_eq!(format_duration_short(2 * 86_400), "2d");
        assert_eq!(format_duration_short(90_000), "1d1h");
        assert_eq!(format_rate(0.0), "0.0");
        assert_eq!(format_rate(2.4), "2.4");
        assert_eq!(format_rate(9.9), "9.9");
        assert_eq!(format_rate(10.0), "10");
        assert_eq!(format_rate(31.2), "31");
        assert_eq!(format_rate(f64::NAN), "0");
        assert_eq!(format_rate(f64::INFINITY), "0");
        assert_eq!(format_rate(-1.0), "0");

        // ETA done path.
        let mut done = TipRateTracker::new();
        let t0 = Instant::now();
        done.push(t0, 100);
        assert_eq!(done.eta_string(t0 + Duration::from_secs(30), 100, 100), "done");
    }

    #[test]
    fn work_chain_progress_tip_hole_and_high_water() {
        use super::work_chain_progress;
        use super::super::body::BodyPresence;
        use bitcoin::hashes::Hash;
        use bitcoin::BlockHash;
        use rbitcoin_consensus::{ChainParams, Milestone};
        use rbitcoin_query::Query;
        use std::collections::{HashSet, VecDeque};

        let dir = std::env::temp_dir().join(format!(
            "rbitcoin-wcp-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let _ = std::fs::create_dir_all(&dir);
        let q = Query::open_or_create(dir.join("store")).unwrap();
        let hub = crate::chain::ChainHub::new(q, ChainParams::regtest(), Milestone::NONE);
        hub.ensure_genesis().unwrap();

        let mut ordered = VecDeque::new();
        let mut set = HashSet::new();
        let mut body = BodyPresence::new();
        // Ghost entry + unready hole + ready stop.
        let ghost = BlockHash::from_byte_array([1u8; 32]);
        let hole = BlockHash::from_byte_array([2u8; 32]);
        let ready = BlockHash::from_byte_array([3u8; 32]);
        ordered.push_back(ghost); // not in set
        ordered.push_back(hole);
        ordered.push_back(ready);
        set.insert(hole);
        set.insert(ready);
        body.mark_missing(hole);
        body.mark_archived(ready);

        let p = work_chain_progress(&hub, &ordered, &set, &mut body, 50, 10);
        assert_eq!(p.tip, 0);
        assert_eq!(p.archived, 10); // max(tip, max_archived)
        assert_eq!(p.headers, 50);
        assert_eq!(p.tip_hole, 1); // hole then ready breaks
        let _ = std::fs::remove_dir_all(dir);
    }

    /// EWMA tip-rate: warmup gate, steady ETA, spike resistance, sustained slowdown.
    #[test]
    fn tip_rate_tracker_eta_surface() {
        let t0 = Instant::now();

        // Warmup: only 5s of history — hide ETA until min elapsed.
        let mut cold = TipRateTracker::new();
        cold.push(t0, 0);
        cold.push(t0 + Duration::from_secs(5), 50);
        assert!(cold.eta_rate(t0 + Duration::from_secs(5)).is_none());
        assert_eq!(
            cold.eta_string(t0 + Duration::from_secs(5), 50, 1_000_000),
            "eta=?"
        );

        // Steady 1 blk/s → ~2h ETA for 7200 remaining.
        let mut steady = TipRateTracker::new();
        for i in 0u32..=60 {
            steady.push(t0 + Duration::from_secs(u64::from(i) * 5), 100 + i * 5);
        }
        let now = t0 + Duration::from_secs(300);
        let rate = steady.eta_rate(now).unwrap();
        assert!((rate - 1.0).abs() < 0.05, "rate={rate}");
        let eta = steady.eta_string(now, 100 + 300, 100 + 300 + 7200);
        assert!(eta.contains("eta="), "{eta}");
        assert!(eta.contains("2.0h") || eta.contains("2h"), "{eta}");

        // Spike: ~5 min at 10 blk/s, then one wild tick — EWMA must not jump near spike.
        let mut spike = TipRateTracker::new();
        for i in 0u32..=60 {
            spike.push(t0 + Duration::from_secs(u64::from(i) * 5), i * 50);
        }
        let before = spike.eta_rate(t0 + Duration::from_secs(300)).unwrap();
        assert!((before - 10.0).abs() < 0.5, "before={before}");
        spike.push(t0 + Duration::from_secs(305), 60 * 50 + 2500);
        let after = spike.eta_rate(t0 + Duration::from_secs(305)).unwrap();
        assert!(after < 30.0, "after spike rate should stay near 10, got {after}");
        assert!(after > 8.0, "after={after}");

        // Sustained slowdown: fast early, then dense pace for several minutes.
        let mut slow = TipRateTracker::new();
        for i in 0u32..=12 {
            slow.push(t0 + Duration::from_secs(u64::from(i) * 5), i * 500);
        }
        let fast = slow.eta_rate(t0 + Duration::from_secs(60)).unwrap();
        assert!(fast > 50.0, "fast={fast}");
        let tip0 = 12u32 * 500;
        for i in 1u32..=72 {
            slow.push(
                t0 + Duration::from_secs(60 + u64::from(i) * 5),
                tip0 + i * 50,
            );
        }
        let dense = slow.eta_rate(t0 + Duration::from_secs(60 + 72 * 5)).unwrap();
        assert!(dense < 18.0, "should track denser pace, got {dense}");
        assert!(dense > 5.0, "dense={dense}");
    }
}
