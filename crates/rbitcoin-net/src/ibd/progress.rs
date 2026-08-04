//! Work-chain progress snapshot, percent helpers, and rate/ETA for IBD logs.
//!
//! The operator `ibd: progress` line reports tip pace and in-RAM block-queue
//! occupancy. It does **not** print a retired dual-track Class A high-water or
//! "archive lead" (`arch_hwm` / `lead=`).

use super::body::BodyPresence;
use crate::chain::ChainHub;
use bitcoin::BlockHash;
use std::collections::HashMap;
use std::time::Instant;

/// Cap status-tick hole walk so a huge missing header band cannot burn the loop.
const TIP_HOLE_SCAN_MAX: u32 = 8192;

/// Work-chain progress for status / progress logs.
///
/// - `tip`: confirmed best-chain height
/// - `ready_hwm`: highest claim-ready / offered body height on the work path
///   (body queue densify bookkeeping; **not** printed on the progress line)
/// - `headers`: max peer-advertised / learned header height
/// - `tip_hole`: count of heights from tip+1 until the next **claim-ready**
///   body (body queue / pending wire only) — the fetch gap operators care about
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct WorkChainProgress {
    pub tip: u32,
    pub ready_hwm: u32,
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
    /// Confirm pipeline depths already formatted (`planq… prepq… writeq…`).
    pub conf_q: String,
    pub txs: u64,
    pub horizon: u32,
    /// From [`TipRateTracker::eta_string`] (`eta=…` or `done`).
    pub eta: String,
    /// In-RAM block queue bytes / entry count (process heap wire payloads).
    pub bq_bytes: u64,
    pub bq_count: usize,
    /// Soft densify confirm-window target (block count for ~1 min at tip rate).
    pub bq_soft_stop: u32,
}

/// Build the `ibd: progress …` message body (no log level prefix).
///
/// Tip percent/rate, **fetch hole** (tip→next claim-ready body), peers, confirm
/// queues, txs, header horizon, tip-rate ETA, in-RAM block-queue occupancy.
/// `bq soft=n/win` is count vs 1-min confirm window at tip rate; `RAM=` is queue heap MiB.
/// Count is only in `soft=` (no redundant `n=`).
pub(crate) fn format_progress_line(i: &ProgressLineInput) -> String {
    let bq_mib = i.bq_bytes / (1024 * 1024);
    format!(
        "ibd: progress {}% tip={} ({}/s) hole={} peers={} {} txs={} horizon={} {} bq soft={}/{} RAM={}MiB",
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
        i.bq_soft_stop,
        bq_mib,
    )
}

/// True if confirm plan/prep can claim this height without another getdata.
///
/// **Only** body-queue / pending wire. Class A alone is not claim-ready — the
/// sole confirm intake is bq → plan → prep (wire). Tip-follow reorgs use
/// peer wire via [`crate::chain::ChainHub::accept_block`], not this feed.
pub(crate) fn claim_ready(
    hub: &ChainHub,
    body: &mut BodyPresence,
    height: u32,
    hash: &BlockHash,
) -> bool {
    if hub.has_block(hash) {
        return true;
    }
    if body.is_rejected(hash) {
        return false;
    }
    // Peer/rehydrate already parked wire in the body queue.
    if body.is_pending(hash) {
        return true;
    }
    hub.query.block_queue_has_height(height)
}

/// Count heights from tip+1 until the next claim-ready body (fetch gap).
///
/// - Missing header on the work path → stop (cannot request further).
/// - Rejected tip+1 → stop (not a download gap; confirm is blacklisted).
/// - Claim-ready (queue/pending wire) → stop.
/// - Otherwise increment hole (needs getdata before tip can claim it).
pub(crate) fn tip_fetch_hole(
    hub: &ChainHub,
    height_to_hash: &HashMap<u32, BlockHash>,
    body: &mut BodyPresence,
) -> usize {
    let path_lo = match hub.tip_height() {
        None => 0u32,
        Some(t) => t.saturating_add(1),
    };
    let limit = path_lo.saturating_add(TIP_HOLE_SCAN_MAX);
    let mut flags = Vec::new();
    for ht in path_lo..=limit {
        let Some(&hash) = height_to_hash.get(&ht) else {
            break;
        };
        if body.is_rejected(&hash) {
            break;
        }
        flags.push(claim_ready(hub, body, ht, &hash));
    }
    tip_hole_from_claim_ready(&flags)
}

/// Build a status snapshot for the ~5s operator tick.
pub(crate) fn work_chain_progress(
    hub: &ChainHub,
    height_to_hash: &HashMap<u32, BlockHash>,
    body: &mut BodyPresence,
    max_peer_height: u32,
    max_ready_height: u32,
) -> WorkChainProgress {
    let tip = hub.tip_height().unwrap_or(0);
    let tip_hole = tip_fetch_hole(hub, height_to_hash, body);
    WorkChainProgress {
        tip,
        ready_hwm: tip.max(max_ready_height),
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
                        let alpha = 1.0
                            - (-std::f64::consts::LN_2 * dt / ETA_RATE_HALF_LIFE_SECS).exp();
                        let alpha = alpha.clamp(0.02, 0.5);
                        alpha * inst + (1.0 - alpha) * prev
                    }
                });
            }
        }
        self.last = Some((now, tip));
    }

    /// Smoothed tip rate (blocks/s) for ETA: present-biased EWMA.
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

/// Pure tip-hole count over tip+1.. using claim-ready flags.
///
/// `claim_ready[i]` corresponds to height path_lo+i. Production
/// [`tip_fetch_hole`] walks heights and builds the same sequential hole count.
pub(crate) fn tip_hole_from_claim_ready(claim_ready_flags: &[bool]) -> usize {
    let mut tip_hole = 0usize;
    for &ready in claim_ready_flags {
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
        format_duration_short, format_progress_line, format_rate, ibd_pct, tip_hole_from_claim_ready,
        ProgressLineInput, TipRateTracker,
    };
    use rbitcoin_query::{
        soft_assign_restricted, soft_confirm_window_n, soft_densify_band_hi, BQ_SOFT_FREE_BYTES,
    };
    use std::time::{Duration, Instant};

    /// Shipped progress line: tip + in-RAM bq soft depth; forbid retired tokens.
    #[test]
    fn format_progress_line_schema12_tokens() {
        // tip_rate 12.5 → format_rate rounds to "12" (≥10).
        let line = format_progress_line(&ProgressLineInput {
            pct: 42,
            tip: 100_000,
            tip_rate: 12.5,
            tip_hole: 3,
            peers: 8,
            conf_q: "planq=1/2 prepq=1/2 writeq=0/2".into(),
            txs: 50_000_000,
            horizon: 900_000,
            eta: "eta=18h".into(),
            bq_bytes: 256 * 1024 * 1024,
            bq_count: 17,
            bq_soft_stop: 180,
        });
        assert_eq!(
            line,
            "ibd: progress 42% tip=100000 (12/s) hole=3 peers=8 planq=1/2 prepq=1/2 writeq=0/2 txs=50000000 horizon=900000 eta=18h bq soft=17/180 RAM=256MiB"
        );
        // Current schema tokens present.
        assert!(line.contains(" hole="), "{line}");
        assert!(line.contains(" soft="), "{line}");
        assert!(line.contains(" RAM="), "{line}");
        assert!(!line.contains(" bq n="), "count lives in soft= only: {line}");
        assert!(!line.contains(" disk="), "queue is RAM not disk: {line}");
        assert!(!line.contains("pending_ram="), "no RAM overflow meter: {line}");
        assert!(line.contains("planq"), "{line}");
        assert!(line.contains("prepq"), "{line}");
        // Retired dual-track progress tokens forbidden.
        assert!(
            !line.contains("arch_hwm"),
            "must not report retired arch_hwm: {line}"
        );
        assert!(!line.contains("lead="), "must not report retired lead=: {line}");
        assert!(
            !line.contains("arch="),
            "must not report retired arch= rate token: {line}"
        );
        assert_eq!(
            line.matches("/s)").count(),
            1,
            "only tip rate on progress line: {line}"
        );
        let slow = format_progress_line(&ProgressLineInput {
            pct: 1,
            tip: 10,
            tip_rate: 2.4,
            tip_hole: 0,
            peers: 1,
            conf_q: "planq<0/2 prepq<0/2 writeq<0/2".into(),
            txs: 1,
            horizon: 1000,
            eta: "eta=?".into(),
            bq_bytes: 0,
            bq_count: 0,
            bq_soft_stop: 256,
        });
        assert!(slow.contains("tip=10 (2.4/s)"), "{slow}");
        assert!(slow.contains("bq soft=0/256 RAM=0MiB"), "{slow}");
        assert!(!slow.contains("arch_hwm") && !slow.contains("lead="), "{slow}");
    }

    #[test]
    fn soft_confirm_window_and_band_at_rate() {
        assert_eq!(soft_confirm_window_n(Some(2.0)), 120, "2 blk/s × 60s");
        assert_eq!(soft_confirm_window_n(None), 0, "cold rate");
        assert_eq!(soft_confirm_window_n(Some(5.0)), 300, "5 blk/s × 60s");
        assert_eq!(soft_confirm_window_n(Some(10.0)), 600);

        let free = BQ_SOFT_FREE_BYTES;
        let over = free + 1;
        assert_eq!(soft_densify_band_hi(10, 5000, free, Some(5.0)), 5000);
        assert_eq!(soft_densify_band_hi(10, 5000, over, Some(5.0)), 309);
        assert_eq!(soft_densify_band_hi(10, 5000, over, None), 10);
        assert!(!soft_assign_restricted(free));
        assert!(soft_assign_restricted(over));
    }

    #[test]
    fn pct_tip_hole_and_format_surface() {
        assert_eq!(ibd_pct(0, 100), 0);
        assert_eq!(ibd_pct(50, 100), 50);
        assert_eq!(ibd_pct(100, 100), 100);
        assert_eq!(ibd_pct(200, 100), 100);
        assert_eq!(ibd_pct(0, 0), 0);
        assert_eq!(ibd_pct(5, 0), 100);

        // claim_ready flags from tip+1: false,false,true → hole=2
        assert_eq!(tip_hole_from_claim_ready(&[]), 0);
        assert_eq!(tip_hole_from_claim_ready(&[true, false, false]), 0);
        assert_eq!(tip_hole_from_claim_ready(&[false, false, true, false]), 2);
        assert_eq!(tip_hole_from_claim_ready(&[false, false, false]), 3);

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

        let mut done = TipRateTracker::new();
        let t0 = Instant::now();
        done.push(t0, 100);
        assert_eq!(done.eta_string(t0 + Duration::from_secs(30), 100, 100), "done");
    }

    #[test]
    fn work_chain_progress_fetch_hole_pending_is_ready() {
        use super::work_chain_progress;
        use super::super::body::BodyPresence;
        use bitcoin::hashes::Hash;
        use bitcoin::BlockHash;
        use rbitcoin_consensus::{ChainParams, Milestone};
        use rbitcoin_query::Query;
        use std::collections::HashMap;

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

        // tip=0 (genesis) → path_lo=1. Heights 1,2 missing; 3 pending (has wire).
        let mut h2h = HashMap::new();
        let mut body = BodyPresence::new();
        let h1 = BlockHash::from_byte_array([1u8; 32]);
        let h2 = BlockHash::from_byte_array([2u8; 32]);
        let h3 = BlockHash::from_byte_array([3u8; 32]);
        h2h.insert(1, h1);
        h2h.insert(2, h2);
        h2h.insert(3, h3);
        body.mark_missing(h1);
        body.mark_missing(h2);
        body.mark_pending(h3); // body queue owns wire → claim-ready

        let p = work_chain_progress(&hub, &h2h, &mut body, 50, 10);
        assert_eq!(p.tip, 0);
        assert_eq!(p.ready_hwm, 10);
        assert_eq!(p.headers, 50);
        assert_eq!(
            p.tip_hole, 2,
            "pending at h=3 is claim-ready; hole is only missing 1..2"
        );

        // Pending at tip+1 → hole=0 (fetch already filled; confirm can claim).
        body.mark_pending(h1);
        let p2 = work_chain_progress(&hub, &h2h, &mut body, 50, 10);
        assert_eq!(p2.tip_hole, 0);

        let _ = std::fs::remove_dir_all(dir);
    }

    /// EWMA tip-rate: warmup gate, steady ETA.
    #[test]
    fn tip_rate_tracker_eta_surface() {
        let t0 = Instant::now();

        let mut cold = TipRateTracker::new();
        cold.push(t0, 0);
        cold.push(t0 + Duration::from_secs(5), 50);
        assert!(cold.eta_rate(t0 + Duration::from_secs(5)).is_none());
        assert_eq!(
            cold.eta_string(t0 + Duration::from_secs(5), 50, 1_000_000),
            "eta=?"
        );

        let mut steady = TipRateTracker::new();
        for i in 0u32..=60 {
            steady.push(t0 + Duration::from_secs(u64::from(i) * 5), 100 + i * 5);
        }
        let now = t0 + Duration::from_secs(300);
        let rate = steady.eta_rate(now).expect("warmed");
        assert!((rate - 1.0).abs() < 0.15, "rate={rate}");
        // ~7200 remain at ~1 blk/s → ~2h
        let eta = steady.eta_string(now, 400, 7_600);
        assert!(eta.starts_with("eta="), "{eta}");
        assert!(eta.contains('h') || eta.contains('m'), "{eta}");
    }
}
