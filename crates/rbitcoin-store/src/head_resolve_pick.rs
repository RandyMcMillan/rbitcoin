//! Newest-first identity pick for head resolve (BIP30 / fence).
//!
//! After a wave fetches every cand's `txid.body`, walk the cand list (deepest
//! first) and take the first `body==want` that is fence-connected (or the first
//! body match when no fence).

use crate::height_fence::HeightFence;
use rbitcoin_primitives::Fk;
use std::collections::HashMap;

/// First txid match that is fence-connected, or first txid match if `heights` is None.
///
/// Walks only `cands[..filled]`. Newer unconnected matches do not win when a
/// fence is present — walk continues. After the first connected hit, older
/// cands are irrelevant.
pub(crate) fn pick_winner(
    cands: &[Fk],
    filled: usize,
    want: &[u8; 32],
    id_of: &HashMap<u64, [u8; 32]>,
    heights: Option<&HeightFence>,
) -> Option<(Fk, u64)> {
    let n = filled.min(cands.len());
    let mut first_match: Option<(Fk, u64)> = None;
    for (i, &fk) in cands.iter().take(n).enumerate() {
        let rank = (i + 1) as u64;
        let Some(id) = fk.get() else {
            continue;
        };
        let Some(got) = id_of.get(&id) else {
            continue;
        };
        if got.as_slice() != want {
            continue;
        }
        if first_match.is_none() {
            first_match = Some((fk, rank));
        }
        if let Some(ht) = heights {
            if ht.height_of(fk).is_some() {
                return Some((fk, rank));
            }
            continue;
        }
        return Some((fk, rank));
    }
    if heights.is_some() {
        // TipThenAny fallback is applied by the caller when no connected hit.
        None
    } else {
        first_match
    }
}

/// Which lookup table a TipOnly leftover miss failed on.
///
/// Confirm leftover is `tx.head` probe → `txid.body` identity → fence → `tx.idx`
/// range. The operator line names the first table that did not produce a usable
/// fact. `idx` is connected identity with no published range (usually Corrupt
/// before leftover); included so a silent range miss is not labeled `head`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LeftoverMissOn {
    /// No probe candidates (hot + cold).
    Head,
    /// Probe cands, no `txid.body` match for the wanted txid.
    Body,
    /// Connected identity, no `tx.idx` / `txout.idx` body range.
    Idx,
    /// Identity match exists, no fence height (TipOnly drops it).
    Fence,
}

impl LeftoverMissOn {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Head => "head",
            Self::Body => "body",
            Self::Idx => "idx",
            Self::Fence => "fence",
        }
    }
}

/// Classify a TipOnly leftover miss from facts the resolve machine already has.
pub fn classify_leftover_miss(
    n_cands: usize,
    had_identity: bool,
    connected: bool,
) -> LeftoverMissOn {
    if n_cands == 0 {
        LeftoverMissOn::Head
    } else if !had_identity {
        LeftoverMissOn::Body
    } else if !connected {
        LeftoverMissOn::Fence
    } else {
        LeftoverMissOn::Idx
    }
}

/// How many of the filled prefix peeks missed the wanted txid (for `miss_peeks`).
pub(crate) fn miss_peeks_in_prefix(
    cands: &[Fk],
    filled: usize,
    want: &[u8; 32],
    id_of: &HashMap<u64, [u8; 32]>,
) -> u64 {
    let n = filled.min(cands.len());
    let mut nmiss = 0u64;
    for &fk in cands.iter().take(n) {
        let Some(id) = fk.get() else {
            nmiss = nmiss.saturating_add(1);
            continue;
        };
        match id_of.get(&id) {
            Some(got) if got.as_slice() == want => {}
            _ => nmiss = nmiss.saturating_add(1),
        }
    }
    nmiss
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::height_fence::{FenceRun, HeightFence};

    fn fence_on(fks: &[u64]) -> HeightFence {
        let runs: Vec<FenceRun> = fks
            .iter()
            .map(|&id| FenceRun {
                first_fk: id,
                count: 1,
                height: 0,
            })
            .collect();
        HeightFence::from_runs(runs)
    }

    fn ids(pairs: &[(u64, [u8; 32])]) -> HashMap<u64, [u8; 32]> {
        pairs.iter().copied().collect()
    }

    #[test]
    fn id_idx_stop_after_connected_skips_older_cand() {
        let want = [0xAAu8; 32];
        let other = [0xBBu8; 32];
        let cands = [Fk(10), Fk(20), Fk(30)];
        let ht = fence_on(&[20]);
        // Newest unconnected match is not a fence winner.
        let map1 = ids(&[(10, want)]);
        assert!(pick_winner(&cands, 1, &want, &map1, Some(&ht)).is_none());
        // Connected match at rank 2 wins; older Fk(30) is irrelevant.
        let map2 = ids(&[(10, want), (20, want)]);
        let (fk, rank) = pick_winner(&cands, 2, &want, &map2, Some(&ht)).unwrap();
        assert_eq!(fk, Fk(20));
        assert_eq!(rank, 2);
        let _ = other;
    }

    #[test]
    fn id_idx_stop_after_connected_unconnected_only_peeks_all() {
        let want = [0xAAu8; 32];
        let cands = [Fk(10), Fk(20)];
        let ht = fence_on(&[99]);
        let map = ids(&[(10, want), (20, want)]);
        assert!(pick_winner(&cands, 2, &want, &map, Some(&ht)).is_none());
        assert_eq!(miss_peeks_in_prefix(&cands, 2, &want, &map), 0);
    }

    #[test]
    fn no_fence_stops_at_first_txid_match() {
        let want = [0xAAu8; 32];
        let cands = [Fk(1), Fk(2)];
        let map = ids(&[(1, want)]);
        let (fk, rank) = pick_winner(&cands, 1, &want, &map, None).unwrap();
        assert_eq!((fk, rank), (Fk(1), 1));
    }

    /// Full cand list + complete id_map: newest unconnected body match does
    /// not win when a shallower cand is fence-connected.
    #[test]
    fn full_map_connected_beats_newer_unconnected() {
        let want = [0xAAu8; 32];
        let cands = [Fk(10), Fk(20)];
        let ht = fence_on(&[20]);
        let map = ids(&[(10, want), (20, want)]);
        let (fk, rank) = pick_winner(&cands, cands.len(), &want, &map, Some(&ht)).unwrap();
        assert_eq!(fk, Fk(20));
        assert_eq!(rank, 2);
    }

    /// A cand with no `id_map` entry is not a match (same as wrong body).
    #[test]
    fn missing_id_map_entry_is_not_a_match() {
        let want = [0xAAu8; 32];
        let cands = [Fk(10), Fk(20)];
        let ht = fence_on(&[10, 20]);
        let map = ids(&[(20, want)]);
        let (fk, rank) = pick_winner(&cands, cands.len(), &want, &map, Some(&ht)).unwrap();
        assert_eq!(fk, Fk(20));
        assert_eq!(rank, 2);
        let empty = ids(&[]);
        assert!(pick_winner(&cands, cands.len(), &want, &empty, Some(&ht)).is_none());
    }

    #[test]
    fn leftover_miss_classifies_head_body_idx_fence() {
        assert_eq!(
            classify_leftover_miss(0, false, false),
            LeftoverMissOn::Head
        );
        assert_eq!(
            classify_leftover_miss(3, false, false),
            LeftoverMissOn::Body
        );
        assert_eq!(
            classify_leftover_miss(3, true, false),
            LeftoverMissOn::Fence
        );
        assert_eq!(classify_leftover_miss(3, true, true), LeftoverMissOn::Idx);
        assert_eq!(LeftoverMissOn::Head.as_str(), "head");
        assert_eq!(LeftoverMissOn::Body.as_str(), "body");
        assert_eq!(LeftoverMissOn::Idx.as_str(), "idx");
        assert_eq!(LeftoverMissOn::Fence.as_str(), "fence");
    }

    #[test]
    fn miss_peeks_counts_wrong_identity_only() {
        let want = [0xAAu8; 32];
        let cands = [Fk(1), Fk(2)];
        let map = ids(&[(1, [0x00; 32]), (2, want)]);
        assert_eq!(miss_peeks_in_prefix(&cands, 2, &want, &map), 1);
    }
}
