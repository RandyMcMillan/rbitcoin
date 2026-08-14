//! Resident create-height fence: O(blocks) image, O(log tip) point query.
//!
//! Class A fks are assigned sequentially, but a reorg leaves **holes**: orphaned
//! rows sit numerically between two still-confirmed runs. `fk ≤ last_tip_fk` is
//! therefore false as a connectedness test. This fence answers “which confirmed
//! height’s `[first_fk, first_fk+count)` contains this fk, if any.”
//!
//! Built only from `confirmed[]` + `header_txs_*` (already L2). No `tx_height`
//! file and no disk binary search.

use crate::chain::{ConfirmedTable, HeaderTxsTable};
use crate::error::StoreError;
use rbitcoin_primitives::{Fk, Height};

/// One confirmed height’s contiguous Class A run.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FenceRun {
    /// 1-based inclusive create_fk of the first tx in the block.
    pub first_fk: u64,
    pub count: u32,
    pub height: u32,
}

impl FenceRun {
    #[inline]
    pub fn contains(self, id: u64) -> bool {
        id >= self.first_fk && id < self.first_fk.saturating_add(u64::from(self.count))
    }
}

/// Height-indexed, first_fk-sorted confirmed runs.
#[derive(Clone, Debug, Default)]
pub struct HeightFence {
    /// Sorted by `first_fk` (Class A assign is increasing; reorgs append higher).
    runs: Vec<FenceRun>,
}

impl HeightFence {
    pub fn empty() -> Self {
        Self { runs: Vec::new() }
    }

    /// Sort by `first_fk` and drop empty counts.
    pub fn from_runs(mut runs: Vec<FenceRun>) -> Self {
        runs.retain(|r| r.count > 0 && r.first_fk > 0);
        runs.sort_unstable_by_key(|r| r.first_fk);
        Self { runs }
    }

    /// Rebuild from confirmed tip + per-header Class A ranges.
    pub fn from_confirmed(
        confirmed: &ConfirmedTable,
        header_txs: &HeaderTxsTable,
    ) -> Result<Self, StoreError> {
        let Some(tip) = confirmed.tip_height() else {
            return Ok(Self::empty());
        };
        let mut runs = Vec::with_capacity(tip.0 as usize + 1);
        for h in 0..=tip.0 {
            let Some(hfk) = confirmed.get(Height(h))? else {
                continue;
            };
            let Some((first, n)) = header_txs.get_range(hfk)? else {
                continue;
            };
            let Some(id) = first.get() else {
                continue;
            };
            if n == 0 {
                continue;
            }
            runs.push(FenceRun {
                first_fk: id,
                count: n,
                height: h,
            });
        }
        Ok(Self::from_runs(runs))
    }

    pub fn len(&self) -> usize {
        self.runs.len()
    }

    pub fn is_empty(&self) -> bool {
        self.runs.is_empty()
    }

    /// Confirm tip+1: append this height’s run (first_fk must be new).
    pub fn extend(&mut self, height: u32, first_fk: Fk, count: u32) {
        let Some(id) = first_fk.get() else {
            return;
        };
        if count == 0 {
            return;
        }
        self.runs.push(FenceRun {
            first_fk: id,
            count,
            height,
        });
        // Cheap: IBD/reorg assigns higher fks. Only sort if the new run is out of order.
        if self.runs.len() >= 2 {
            let n = self.runs.len();
            if self.runs[n - 1].first_fk < self.runs[n - 2].first_fk {
                self.runs.sort_unstable_by_key(|r| r.first_fk);
            }
        }
    }

    /// Disconnect tip: drop the run for `height` (must be the tip height).
    pub fn pop_height(&mut self, height: u32) {
        if let Some(i) = self.runs.iter().position(|r| r.height == height) {
            self.runs.remove(i);
        }
    }

    /// Connected create height, or `None` if `fk` sits in a hole / unconfirmed.
    #[inline]
    pub fn height_of(&self, fk: Fk) -> Option<u32> {
        let id = fk.get()?;
        if self.runs.is_empty() {
            return None;
        }
        let i = self.runs.partition_point(|r| r.first_fk <= id);
        if i == 0 {
            return None;
        }
        let r = self.runs[i - 1];
        if r.contains(id) {
            Some(r.height)
        } else {
            None
        }
    }

    pub fn get_batch(&self, fks: &[Fk]) -> Vec<Option<u32>> {
        fks.iter().map(|&fk| self.height_of(fk)).collect()
    }

    /// Half-open `[lo, hi)` create-fk spans not in any run, covering `1..=last_fk`.
    ///
    /// Adjacent runs emit nothing between them. Empty fence → `[1, last_fk+1)`
    /// when `last_fk > 0`. Used by Class C open repair (complement of the fence).
    pub fn unconnected_ranges(&self, last_fk: u64) -> Vec<(u64, u64)> {
        if last_fk == 0 {
            return Vec::new();
        }
        let mut out = Vec::new();
        let mut prev_end = 1u64;
        let cap = last_fk.saturating_add(1);
        for r in &self.runs {
            if r.first_fk >= cap {
                break;
            }
            if r.first_fk > prev_end {
                out.push((prev_end, r.first_fk.min(cap)));
            }
            let run_end = r.first_fk.saturating_add(u64::from(r.count));
            if run_end > prev_end {
                prev_end = run_end;
            }
        }
        if prev_end < cap {
            out.push((prev_end, cap));
        }
        out
    }

    /// True when every create fk in inclusive `lo..=hi` sits in a fence run.
    ///
    /// In-flight prune: leftover TipOnly accepts a span only if this is true
    /// (max height on the fence is not enough — holes / past last run end).
    pub fn covers_fk_span(&self, lo: u64, hi: u64) -> bool {
        if lo == 0 || hi < lo || self.runs.is_empty() {
            return false;
        }
        let mut need = lo;
        while need <= hi {
            let i = self.runs.partition_point(|r| r.first_fk <= need);
            if i == 0 {
                return false;
            }
            let r = self.runs[i - 1];
            if !r.contains(need) {
                return false;
            }
            need = r.first_fk.saturating_add(u64::from(r.count));
        }
        true
    }

    /// Highest confirmed height present on any run (`None` if empty).
    ///
    /// In-flight prune HWM. Distinct from `confirmed[]` length (`tip_height`):
    /// `set_many` can publish tip before [`Self::extend`].
    pub fn max_height(&self) -> Option<u32> {
        self.runs.iter().map(|r| r.height).max()
    }

    /// Highest create_fk in any connected run (`0` if empty).
    ///
    /// Not `last_tip_fk` as a connectedness test — holes after reorg sit
    /// numerically between runs. Use [`Self::height_of`] per fk; this is
    /// only a prune HWM (min with `tx.head` occupied).
    pub fn max_connected_fk(&self) -> u64 {
        self.runs
            .iter()
            .map(|r| {
                r.first_fk
                    .saturating_add(u64::from(r.count))
                    .saturating_sub(1)
            })
            .max()
            .unwrap_or(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run(first: u64, count: u32, height: u32) -> FenceRun {
        FenceRun {
            first_fk: first,
            count,
            height,
        }
    }

    #[test]
    fn height_fence_adjacent_runs_and_edges() {
        let f = HeightFence::from_runs(vec![run(1, 2, 0), run(3, 3, 1)]);
        assert_eq!(f.height_of(Fk(1)), Some(0));
        assert_eq!(f.height_of(Fk(2)), Some(0));
        assert_eq!(f.height_of(Fk(3)), Some(1));
        assert_eq!(f.height_of(Fk(5)), Some(1));
        assert_eq!(f.height_of(Fk(6)), None);
        assert_eq!(f.height_of(Fk::NULL), None);
        assert_eq!(
            f.get_batch(&[Fk(2), Fk(4), Fk(9)]),
            vec![Some(0), Some(1), None]
        );
    }

    #[test]
    fn height_fence_reorg_hole_is_unconnected() {
        // Confirmed h=0 fks 1..=2, then a discarded block used 3..=5, then
        // confirmed h=1 is a new archive at 6..=8.
        let f = HeightFence::from_runs(vec![run(1, 2, 0), run(6, 3, 1)]);
        assert_eq!(f.height_of(Fk(2)), Some(0));
        assert_eq!(
            f.height_of(Fk(3)),
            None,
            "orphaned fk must not take neighbor height"
        );
        assert_eq!(f.height_of(Fk(5)), None);
        assert_eq!(f.height_of(Fk(6)), Some(1));
        assert_eq!(f.height_of(Fk(8)), Some(1));
    }

    #[test]
    fn height_fence_extend_and_pop_tip() {
        let mut f = HeightFence::empty();
        f.extend(0, Fk(1), 2);
        f.extend(1, Fk(3), 1);
        assert_eq!(f.height_of(Fk(3)), Some(1));
        f.pop_height(1);
        assert_eq!(f.height_of(Fk(3)), None);
        assert_eq!(f.height_of(Fk(1)), Some(0));
        f.pop_height(0);
        assert!(f.is_empty());
    }

    #[test]
    fn height_fence_max_connected_fk_is_last_run_end() {
        assert_eq!(HeightFence::empty().max_connected_fk(), 0);
        let f = HeightFence::from_runs(vec![run(1, 2, 0), run(10, 3, 2)]);
        // last connected run is 10..=12, not 2 — holes must not use last_tip_fk.
        assert_eq!(f.max_connected_fk(), 12);
        assert_eq!(f.height_of(Fk(12)), Some(2));
        assert_eq!(f.height_of(Fk(3)), None);
    }

    #[test]
    fn height_fence_fk_before_first_and_empty() {
        let f = HeightFence::from_runs(vec![run(10, 2, 4)]);
        assert_eq!(f.height_of(Fk(9)), None);
        assert_eq!(f.height_of(Fk(10)), Some(4));
        assert_eq!(f.height_of(Fk(12)), None);
        assert_eq!(HeightFence::empty().height_of(Fk(1)), None);
    }

    #[test]
    fn height_fence_max_height_is_highest_run_not_last_fk() {
        assert_eq!(HeightFence::empty().max_height(), None);
        let f = HeightFence::from_runs(vec![run(1, 2, 0), run(10, 3, 2)]);
        assert_eq!(f.max_height(), Some(2));
        assert_eq!(f.max_connected_fk(), 12);
    }

    #[test]
    fn height_fence_covers_fk_span() {
        assert!(!HeightFence::empty().covers_fk_span(1, 1));
        let f = HeightFence::from_runs(vec![run(1, 2, 0), run(6, 3, 1)]);
        assert!(f.covers_fk_span(1, 2));
        assert!(f.covers_fk_span(6, 8));
        assert!(!f.covers_fk_span(3, 5), "hole is not leftover-visible");
        assert!(
            !f.covers_fk_span(1, 8),
            "span that includes a hole is not covered"
        );
        assert!(!f.covers_fk_span(8, 10), "past last run end");
        assert!(f.covers_fk_span(7, 7));
    }

    #[test]
    fn height_fence_unconnected_ranges() {
        assert_eq!(
            HeightFence::empty().unconnected_ranges(0),
            Vec::<(u64, u64)>::new()
        );
        assert_eq!(
            HeightFence::empty().unconnected_ranges(10),
            vec![(1, 11)],
            "empty fence: the whole 1..=last_fk span is leftover"
        );
        let abut = HeightFence::from_runs(vec![run(1, 2, 0), run(3, 3, 1)]);
        assert_eq!(abut.unconnected_ranges(5), Vec::<(u64, u64)>::new());
        assert_eq!(abut.unconnected_ranges(8), vec![(6, 9)]);
        let hole = HeightFence::from_runs(vec![run(1, 2, 0), run(6, 3, 1)]);
        assert_eq!(hole.unconnected_ranges(8), vec![(3, 6)]);
        assert_eq!(hole.unconnected_ranges(10), vec![(3, 6), (9, 11)]);
        let late = HeightFence::from_runs(vec![run(10, 2, 4)]);
        assert_eq!(late.unconnected_ranges(12), vec![(1, 10), (12, 13)]);
        assert_eq!(late.unconnected_ranges(9), vec![(1, 10)]);
    }
}
