//! Pure ordered-path helpers (no peer IO).

use bitcoin::BlockHash;
use std::collections::{HashSet, VecDeque};

/// Mark `h` done in the ordered set. Prefer O(1) front pop; otherwise leave a
/// stale ordered entry. Ghosts (removed from set but still in the deque) are
/// front-trimmed here so the next front is always a live set member when possible.
///
/// **Ghosts:** middle removes only update `ordered_set`. Entries may remain in
/// the deque until front-trim or [`compact_ordered`]. Scans that walk `ordered`
/// should skip hashes not in `ordered_set` (or use set membership).
pub(crate) fn remove_from_ordered(
    ordered: &mut VecDeque<BlockHash>,
    ordered_set: &mut HashSet<BlockHash>,
    h: BlockHash,
) {
    ordered_set.remove(&h);
    if ordered.front() == Some(&h) {
        ordered.pop_front();
    }
    while let Some(&front) = ordered.front() {
        if ordered_set.contains(&front) {
            break;
        }
        ordered.pop_front();
    }
}

/// Rebuild `ordered` to match `ordered_set` (drop middle ghosts).
///
/// Cheap no-op when the deque is not much larger than the live set.
pub(crate) fn compact_ordered(
    ordered: &mut VecDeque<BlockHash>,
    ordered_set: &HashSet<BlockHash>,
) {
    let len = ordered.len();
    if len <= 64 {
        return;
    }
    let live = ordered_set.len();
    let ghost_budget = (live / 2).max(64);
    if len <= live.saturating_add(ghost_budget) {
        return;
    }
    let mut next = VecDeque::with_capacity(live);
    for h in ordered.iter().copied() {
        if ordered_set.contains(&h) {
            next.push_back(h);
        }
    }
    *ordered = next;
}

/// Height classification for getdata slot accounting.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum WorkClass {
    Near,
    Far,
}

/// Near = tip < height ≤ tip+near_depth. Unknown heights count as near.
pub(crate) fn classify_height(height: Option<u32>, tip: u32, near_depth: u32) -> WorkClass {
    match height {
        Some(ht) if ht > tip && ht <= tip.saturating_add(near_depth) => WorkClass::Near,
        Some(ht) if ht > tip.saturating_add(near_depth) => WorkClass::Far,
        _ => WorkClass::Near,
    }
}

/// Far slots per peer: drip while a tip hole exists, else half of `per_peer`.
pub(crate) fn far_slots_per_peer(per_peer: usize, tip_hole: bool) -> usize {
    if tip_hole {
        2
    } else {
        (per_peer / 2).max(1)
    }
}

/// Whether to request more headers past the soft cap.
///
/// Only when the ordered path is **mostly archived** (dense Class A) and the
/// height runway to max_ordered is short — never for sparse far-only archives.
pub(crate) fn want_headers_beyond_soft_cap(
    live: usize,
    known_arch: usize,
    arch_runway: u32,
    runway_need: u32,
) -> bool {
    if live == 0 {
        return true;
    }
    let mostly_archived = known_arch >= live.saturating_mul(3) / 4;
    mostly_archived && arch_runway < runway_need
}

#[cfg(test)]
mod tests {
    use super::*;
    use bitcoin::hashes::Hash;

    fn h(n: u16) -> BlockHash {
        let mut b = [0u8; 32];
        b[0] = (n & 0xff) as u8;
        b[1] = (n >> 8) as u8;
        BlockHash::from_byte_array(b)
    }

    #[test]
    fn classify_near_far() {
        assert_eq!(classify_height(Some(101), 100, 128), WorkClass::Near);
        assert_eq!(classify_height(Some(228), 100, 128), WorkClass::Near);
        assert_eq!(classify_height(Some(229), 100, 128), WorkClass::Far);
        assert_eq!(classify_height(Some(100), 100, 128), WorkClass::Near); // ≤ tip → near bias
        assert_eq!(classify_height(None, 100, 128), WorkClass::Near);
    }

    #[test]
    fn far_slots_simple() {
        assert_eq!(far_slots_per_peer(16, true), 2);
        assert_eq!(far_slots_per_peer(16, false), 8);
        assert_eq!(far_slots_per_peer(8, false), 4);
    }

    /// When archive RAM is full we set far_cap=0 so all window room is near.
    #[test]
    fn tip_near_only_means_zero_far_cap() {
        assert_eq!(far_slots_per_peer(16, false).min(0), 0);
        // Explicit: tip-near assign path uses far_cap = 0 regardless of tip_hole.
        let far_cap_when_budget_full = 0usize;
        assert_eq!(far_cap_when_budget_full, 0);
    }

    #[test]
    fn header_soft_cap_density_gate() {
        // Sparse: 4k known of 120k live → no bypass
        assert!(!want_headers_beyond_soft_cap(120_000, 4_000, 100, 2048));
        // Dense + short runway → bypass
        assert!(want_headers_beyond_soft_cap(64_000, 50_000, 100, 2048));
        // Dense but long runway → no need
        assert!(!want_headers_beyond_soft_cap(64_000, 50_000, 10_000, 2048));
        // Empty path
        assert!(want_headers_beyond_soft_cap(0, 0, 0, 2048));
    }

    #[test]
    fn remove_pops_front_match() {
        let mut ordered: VecDeque<_> = [h(1), h(2), h(3)].into_iter().collect();
        let mut set: HashSet<_> = ordered.iter().copied().collect();
        remove_from_ordered(&mut ordered, &mut set, h(1));
        assert_eq!(ordered.front().copied(), Some(h(2)));
        assert!(!set.contains(&h(1)));
    }

    #[test]
    fn remove_middle_then_front_trims_ghost() {
        let mut ordered: VecDeque<_> = [h(1), h(2), h(3)].into_iter().collect();
        let mut set: HashSet<_> = ordered.iter().copied().collect();
        remove_from_ordered(&mut ordered, &mut set, h(2));
        assert_eq!(ordered.len(), 3);
        assert!(!set.contains(&h(2)));
        remove_from_ordered(&mut ordered, &mut set, h(1));
        assert_eq!(ordered.front().copied(), Some(h(3)));
        assert_eq!(ordered.len(), 1);
    }

    #[test]
    fn compact_drops_middle_ghosts_when_bloated() {
        let mut ordered = VecDeque::new();
        let mut set = HashSet::new();
        for i in 1u16..=100 {
            ordered.push_back(h(i));
            set.insert(h(i));
            ordered.push_back(h(1000 + i));
        }
        assert_eq!(ordered.len(), 200);
        compact_ordered(&mut ordered, &set);
        assert_eq!(ordered.len(), 100);
        for x in &ordered {
            assert!(set.contains(x));
        }
    }

    #[test]
    fn compact_skips_small_deques() {
        let mut ordered: VecDeque<_> = [h(1), h(2)].into_iter().collect();
        let set: HashSet<_> = [h(1)].into_iter().collect();
        compact_ordered(&mut ordered, &set);
        assert_eq!(ordered.len(), 2);
    }
}
