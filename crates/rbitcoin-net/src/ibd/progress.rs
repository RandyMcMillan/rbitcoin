//! Work-chain progress snapshot and percent helpers for IBD logs.

use super::body::BodyPresence;
use crate::chain::ChainHub;
use bitcoin::BlockHash;
use std::collections::{HashSet, VecDeque};

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
    use super::{ibd_pct, tip_hole_from_ready};

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
}
