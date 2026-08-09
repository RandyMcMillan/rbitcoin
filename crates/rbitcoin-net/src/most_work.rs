//! Most-work ranking helpers (header work sums, LCA on a parent graph).
//!
//! Layer 1 of [`docs/design-ibd-most-work-reorg.md`]: candidate ranking uses
//! header work only. Apply/validation is separate (`ChainHub::accept_branch`).

use bitcoin::Work;

/// Sum header work values (Bitcoin most-work accumulation).
pub fn sum_work(iter: impl Iterator<Item = Work>) -> Work {
    let mut acc: Option<Work> = None;
    for w in iter {
        acc = Some(match acc {
            None => w,
            Some(a) => a + w,
        });
    }
    acc.unwrap_or_else(|| Work::from_be_bytes([0u8; 32]))
}

/// Strictly more work (Bitcoin most-work rule).
#[inline]
pub fn work_better(new: Work, old: Work) -> bool {
    new > old
}

/// Parent of `hash` in a parent map (`child → parent`). Genesis maps to `None`.
pub type ParentFn<'a> = dyn Fn([u8; 32]) -> Option<[u8; 32]> + 'a;

/// If `hash` is on the best chain, its height; else `None`.
pub type BestHeightFn<'a> = dyn Fn([u8; 32]) -> Option<u32> + 'a;

/// Walk `start` via parents until a hash with a best-chain height is found.
/// Returns `(height, hash)` of the first best-chain ancestor (including `start`).
pub fn first_best_ancestor(
    parent: &ParentFn<'_>,
    on_best: &BestHeightFn<'_>,
    start: [u8; 32],
    max_walk: usize,
) -> Option<(u32, [u8; 32])> {
    let mut cur = start;
    for _ in 0..max_walk {
        if let Some(h) = on_best(cur) {
            return Some((h, cur));
        }
        cur = parent(cur)?;
    }
    None
}

/// Last common ancestor of two tips on the header graph, restricted to the
/// **best chain** via `on_best` (height of hash if confirmed).
///
/// Collects best-chain ancestors of `tip_a`, then walks `tip_b` until a hit.
/// Returns `(height, lca_hash)` on the best chain.
pub fn lca_on_best_chain(
    parent: &ParentFn<'_>,
    on_best: &BestHeightFn<'_>,
    tip_a: [u8; 32],
    tip_b: [u8; 32],
    max_walk: usize,
) -> Option<(u32, [u8; 32])> {
    let mut seen_a = std::collections::HashSet::new();
    let mut cur = tip_a;
    for _ in 0..max_walk {
        if on_best(cur).is_some() {
            seen_a.insert(cur);
        }
        match parent(cur) {
            Some(p) => cur = p,
            None => break,
        }
    }
    if seen_a.is_empty() {
        return None;
    }
    cur = tip_b;
    for _ in 0..max_walk {
        if seen_a.contains(&cur) {
            let h = on_best(cur)?;
            return Some((h, cur));
        }
        match parent(cur) {
            Some(p) => cur = p,
            None => break,
        }
    }
    None
}

/// Hashes on the path from `child` exclusive up through `ancestor` inclusive,
/// ordered **ancestor → … → parent(child)** (apply path order when child is tip+1).
///
/// If `child` equals `ancestor`, returns empty. If ancestor is not reached, returns `None`.
pub fn path_hashes_from_ancestor(
    parent: &ParentFn<'_>,
    ancestor: [u8; 32],
    child: [u8; 32],
    max_walk: usize,
) -> Option<Vec<[u8; 32]>> {
    if child == ancestor {
        return Some(Vec::new());
    }
    let mut rev = Vec::new();
    let mut cur = child;
    for _ in 0..max_walk {
        let p = parent(cur)?;
        rev.push(cur);
        if p == ancestor {
            rev.reverse();
            return Some(rev);
        }
        cur = p;
    }
    None
}

/// Sum work along a path of headers (caller supplies work per hash).
pub fn sum_work_for_hashes(
    hashes: &[[u8; 32]],
    work_of: &dyn Fn([u8; 32]) -> Option<Work>,
) -> Option<Work> {
    let mut works = Vec::with_capacity(hashes.len());
    for h in hashes {
        works.push(work_of(*h)?);
    }
    Some(sum_work(works.into_iter()))
}

/// Header-only candidate for most-work ranking (layer 1).
///
/// `apply_path` is ordered first-after-LCA → … → candidate tip (hashes).
/// `path_work` is the sum of header work along that path (not including LCA).
#[derive(Debug, Clone)]
pub struct WorkCandidate {
    pub tip: [u8; 32],
    pub apply_path: Vec<[u8; 32]>,
    pub path_work: Work,
    pub lca_hash: [u8; 32],
    pub lca_height: u32,
}

/// Ranking result for layer-1 most-work selection (headers only).
///
/// Full-block validation is separate: a selected path may still fail connect,
/// after which the caller marks hashes invalid and re-ranks.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SelectOutcome {
    /// No non-invalid candidate has strictly more work than the best tip path.
    IgnoreWeaker,
    /// Prefer this candidate's header path (still must pass `accept_branch`).
    Switch {
        lca_hash: [u8; 32],
        lca_height: u32,
        apply_path: Vec<[u8; 32]>,
        candidate_tip: [u8; 32],
        candidate_work: Work,
    },
}

/// Pick the strictly best non-invalid candidate by header work.
///
/// `best_path_work` is work of the current tip path from the shared LCA (or
/// comparable segment). A candidate is skipped if its tip or any apply-path
/// hash is invalid-marked.
pub fn select_most_work(
    best_path_work: Work,
    candidates: &[WorkCandidate],
    is_invalid: &dyn Fn([u8; 32]) -> bool,
) -> SelectOutcome {
    let mut best: Option<&WorkCandidate> = None;
    for c in candidates {
        if c.apply_path.is_empty() {
            continue;
        }
        if is_invalid(c.tip) {
            continue;
        }
        if c.apply_path.iter().any(|h| is_invalid(*h)) {
            continue;
        }
        if !work_better(c.path_work, best_path_work) {
            continue;
        }
        match best {
            None => best = Some(c),
            Some(cur) => {
                if work_better(c.path_work, cur.path_work) {
                    best = Some(c);
                }
            }
        }
    }
    match best {
        None => SelectOutcome::IgnoreWeaker,
        Some(c) => SelectOutcome::Switch {
            lca_hash: c.lca_hash,
            lca_height: c.lca_height,
            apply_path: c.apply_path.clone(),
            candidate_tip: c.tip,
            candidate_work: c.path_work,
        },
    }
}

/// Process-local set of invalid block / candidate-tip hashes after failed apply.
#[derive(Debug, Default, Clone)]
pub struct InvalidHashSet {
    hashes: std::collections::HashSet<[u8; 32]>,
}

impl InvalidHashSet {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn mark(&mut self, hash: [u8; 32]) {
        self.hashes.insert(hash);
    }

    pub fn mark_path(&mut self, path: &[[u8; 32]]) {
        for h in path {
            self.hashes.insert(*h);
        }
    }

    pub fn contains(&self, hash: [u8; 32]) -> bool {
        self.hashes.contains(&hash)
    }

    pub fn is_invalid_fn(&self) -> impl Fn([u8; 32]) -> bool + '_ {
        move |h| self.hashes.contains(&h)
    }

    pub fn len(&self) -> usize {
        self.hashes.len()
    }

    pub fn is_empty(&self) -> bool {
        self.hashes.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn w(n: u8) -> Work {
        let mut b = [0u8; 32];
        b[31] = n;
        Work::from_be_bytes(b)
    }

    #[test]
    fn sum_work_and_work_better() {
        let z = Work::from_be_bytes([0u8; 32]);
        assert_eq!(sum_work(std::iter::empty()), z);
        assert_eq!(sum_work([w(1)].into_iter()), w(1));
        assert!(work_better(w(2), w(1)));
        assert!(!work_better(w(1), w(2)));
        assert!(!work_better(w(1), w(1)));
    }

    /// Synthetic graph (display hashes as single-byte ids for readability):
    ///   G(0) → A(1) → B(2) → C(3)   best chain heights 0,1,2,3
    ///            └→ X(4) → Y(5)     side branch
    fn toy_graph() -> (HashMap<[u8; 32], [u8; 32]>, HashMap<[u8; 32], u32>) {
        let h = |n: u8| {
            let mut a = [0u8; 32];
            a[0] = n;
            a
        };
        let mut parent = HashMap::new();
        parent.insert(h(1), h(0));
        parent.insert(h(2), h(1));
        parent.insert(h(3), h(2));
        parent.insert(h(4), h(1));
        parent.insert(h(5), h(4));
        let mut best = HashMap::new();
        best.insert(h(0), 0);
        best.insert(h(1), 1);
        best.insert(h(2), 2);
        best.insert(h(3), 3);
        (parent, best)
    }

    #[test]
    fn lca_sibling_tips_is_common_parent() {
        let (parent_map, best) = toy_graph();
        let parent = |c: [u8; 32]| parent_map.get(&c).copied();
        let on_best = |c: [u8; 32]| best.get(&c).copied();
        let h = |n: u8| {
            let mut a = [0u8; 32];
            a[0] = n;
            a
        };
        // Best tip C(3) vs side tip Y(5): LCA is A(1) at height 1
        let (ht, lca) = lca_on_best_chain(&parent, &on_best, h(3), h(5), 32).unwrap();
        assert_eq!(ht, 1);
        assert_eq!(lca, h(1));
        // Same tip
        let (ht2, lca2) = lca_on_best_chain(&parent, &on_best, h(3), h(3), 32).unwrap();
        assert_eq!(ht2, 3);
        assert_eq!(lca2, h(3));
    }

    #[test]
    fn path_from_ancestor_sibling_branch() {
        let (parent_map, _) = toy_graph();
        let parent = |c: [u8; 32]| parent_map.get(&c).copied();
        let h = |n: u8| {
            let mut a = [0u8; 32];
            a[0] = n;
            a
        };
        // From A to Y: path hashes Y's lineage excluding A = [X, Y]
        let path = path_hashes_from_ancestor(&parent, h(1), h(5), 32).unwrap();
        assert_eq!(path, vec![h(4), h(5)]);
        // Apply path for reorg onto side: works of X and Y vs B and C
        let work_of = |c: [u8; 32]| Some(w(c[0]));
        let side = sum_work_for_hashes(&path, &work_of).unwrap();
        let best_path = path_hashes_from_ancestor(&parent, h(1), h(3), 32).unwrap();
        assert_eq!(best_path, vec![h(2), h(3)]);
        let best_w = sum_work_for_hashes(&best_path, &work_of).unwrap();
        // w(4)+w(5)=9 > w(2)+w(3)=5
        assert!(work_better(side, best_w));
    }

    #[test]
    fn path_missing_ancestor_returns_none() {
        let (parent_map, _) = toy_graph();
        let parent = |c: [u8; 32]| parent_map.get(&c).copied();
        let h = |n: u8| {
            let mut a = [0u8; 32];
            a[0] = n;
            a
        };
        assert!(path_hashes_from_ancestor(&parent, h(9), h(5), 32).is_none());
    }

    fn cand(tip: u8, path: &[u8], path_work: u8, lca: u8, lca_h: u32) -> WorkCandidate {
        let h = |n: u8| {
            let mut a = [0u8; 32];
            a[0] = n;
            a
        };
        WorkCandidate {
            tip: h(tip),
            apply_path: path.iter().map(|&n| h(n)).collect(),
            path_work: w(path_work),
            lca_hash: h(lca),
            lca_height: lca_h,
        }
    }

    #[test]
    fn select_most_work_picks_strictly_better_skips_invalid() {
        // Best path work 5; M work 9; N work 7; equal work 5.
        let best = w(5);
        let m = cand(5, &[4, 5], 9, 1, 1);
        let n = cand(7, &[6, 7], 7, 1, 1);
        let equal = cand(8, &[8], 5, 1, 1);
        let weaker = cand(9, &[9], 3, 1, 1);

        // Prefer M (highest work).
        match select_most_work(
            best,
            &[weaker.clone(), n.clone(), m.clone(), equal.clone()],
            &|_| false,
        ) {
            SelectOutcome::Switch {
                candidate_tip,
                candidate_work,
                ..
            } => {
                assert_eq!(candidate_tip, m.tip);
                assert_eq!(candidate_work, w(9));
            }
            other => panic!("expected Switch to M, got {other:?}"),
        }

        // Mark M invalid → N wins.
        let inv = |h: [u8; 32]| h[0] == 5 || h[0] == 4;
        match select_most_work(best, &[m.clone(), n.clone()], &inv) {
            SelectOutcome::Switch {
                candidate_tip,
                candidate_work,
                ..
            } => {
                assert_eq!(candidate_tip, n.tip);
                assert_eq!(candidate_work, w(7));
            }
            other => panic!("expected Switch to N after M invalid, got {other:?}"),
        }

        // All invalid or not better → IgnoreWeaker.
        assert_eq!(
            select_most_work(best, &[m, equal, weaker], &inv),
            SelectOutcome::IgnoreWeaker
        );
        assert_eq!(
            select_most_work(best, &[], &|_| false),
            SelectOutcome::IgnoreWeaker
        );
    }

    #[test]
    fn invalid_hash_set_marks_and_skips() {
        let mut set = InvalidHashSet::new();
        assert!(set.is_empty());
        let h = [0xab; 32];
        set.mark(h);
        assert!(set.contains(h));
        assert_eq!(set.len(), 1);
        set.mark_path(&[[0x01; 32], [0x02; 32]]);
        assert!(set.contains([0x02; 32]));
        let f = set.is_invalid_fn();
        assert!(f(h));
        assert!(!f([0x00; 32]));
    }

    #[test]
    fn first_best_ancestor_and_lca_edges() {
        let (parent_map, best) = toy_graph();
        let parent = |c: [u8; 32]| parent_map.get(&c).copied();
        let on_best = |c: [u8; 32]| best.get(&c).copied();
        let h = |n: u8| {
            let mut a = [0u8; 32];
            a[0] = n;
            a
        };
        // Side tip Y: first best ancestor is A(1).
        let (ht, anc) = first_best_ancestor(&parent, &on_best, h(5), 32).unwrap();
        assert_eq!(ht, 1);
        assert_eq!(anc, h(1));
        // Already on best.
        let (ht2, anc2) = first_best_ancestor(&parent, &on_best, h(3), 32).unwrap();
        assert_eq!(ht2, 3);
        assert_eq!(anc2, h(3));
        // Max walk too short → None.
        assert!(first_best_ancestor(&parent, &on_best, h(5), 1).is_none());
        // Disconnected tips → no LCA (tip_a not on best, tip_b walk exhausts).
        let orphan = h(99);
        assert!(lca_on_best_chain(&parent, &on_best, orphan, h(3), 8).is_none());
        assert!(lca_on_best_chain(&parent, &on_best, h(3), orphan, 8).is_none());
        // path child == ancestor → empty.
        assert_eq!(
            path_hashes_from_ancestor(&parent, h(5), h(5), 32).unwrap(),
            Vec::<[u8; 32]>::new()
        );
        // Empty apply_path candidate skipped by selector.
        let empty_path = cand(5, &[], 9, 1, 1);
        assert_eq!(
            select_most_work(w(1), &[empty_path], &|_| false),
            SelectOutcome::IgnoreWeaker
        );
        // Empty work of missing hash.
        assert!(sum_work_for_hashes(&[h(9)], &|_| None).is_none());
        // first_best: walk off graph → None.
        assert!(first_best_ancestor(&parent, &on_best, h(99), 4).is_none());
        // path walk budget exhausted without hitting ancestor.
        assert!(path_hashes_from_ancestor(&parent, h(0), h(5), 1).is_none());
    }
}
