//! Process-local confirm parent cache (load stage).
//!
//! - **Header plans** (`headers` / `hash_to_height`): tip-GCed header + tx_fks.
//! - **Plans / ready_through**: load-scanned watermark for diagnostics.
//!
//! Create outs / denserels / body_range are **pipeline-local** (plan `batch_pin`,
//! [`crate::BatchParents`], plan-local external parents). Thin edges and sparse
//! parent pins are **batch-local** ([`crate::confirm_load::BatchThin`],
//! [`crate::BatchParents`]).
//!
//! Header plans stay **always on**. Scan watermarks (`plans`) track load readiness.

use rbitcoin_primitives::Fk;
use rbitcoin_store::HeaderRecord;
use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};

/// Cached header + body fk list for one cache height (avoids header.head/body
/// and header_txs page faults on confirm resolve).
#[derive(Debug, Clone)]
pub struct HeaderPlanCache {
    pub header_fk: Fk,
    pub header_rec: HeaderRecord,
    pub tx_fks: Vec<Fk>,
    /// Previous block hash (zeros at genesis). Filled at cache so wire rebuild
    /// never `store.get_header(prev_fk)`.
    pub prev_hash: [u8; 32],
}

/// Per-height plan: load scan watermark (ready_through) only.
#[derive(Debug, Default)]
struct HeightPlan {
    hash: [u8; 32],
    /// Load finished a body+thin+pin attempt for this height.
    scanned: bool,
}

impl HeightPlan {
    #[inline]
    fn is_ready(&self) -> bool {
        self.scanned
    }
}

struct Inner {
    /// Highest confirmed tip we have pruned to.
    tip: u32,
    /// Contiguous ready watermark: all heights in `(tip, ready_through]` are ready.
    ready_through: u32,
    /// height → plan
    plans: BTreeMap<u32, HeightPlan>,
    /// height → immutable header + tx list (Arc-publish; tip GC drops Arc).
    headers: HashMap<u32, Arc<HeaderPlanCache>>,
    /// hash → height for O(1) header resolve on confirm.
    hash_to_height: HashMap<[u8; 32], u32>,
}

/// Process-local confirm parent cache (headers + scan watermarks only).
///
/// **Always active** — multi-block wire prep needs tip-ahead header plans for
/// MTP / bits (mainnet tip freeze: `parent header plan missing above tip`).
pub struct ConfirmParentCache {
    inner: Mutex<Inner>,
    /// Mirror of `Inner::ready_through` for lock-free reads.
    ready_through: AtomicU32,
}

impl ConfirmParentCache {
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(Inner {
                tip: 0,
                ready_through: 0,
                plans: BTreeMap::new(),
                headers: HashMap::new(),
                hash_to_height: HashMap::new(),
            }),
            ready_through: AtomicU32::new(0),
        }
    }

    pub fn from_env() -> Self {
        Self::new()
    }

    /// Highest height such that every plan in `(tip, ready_through]` is ready.
    pub fn ready_through(&self) -> u32 {
        self.ready_through.load(Ordering::Relaxed)
    }

    /// Advance tip: drop plans/headers at/below tip.
    ///
    /// Thin edges / sparse pins are batch-local. Called from write `post_commit`.
    pub fn advance_tip(&self, tip: u32) {
        let mut g = self.inner.lock().unwrap();
        g.tip = tip;
        if g.ready_through < tip {
            g.ready_through = tip;
        }
        let drop_h: Vec<u32> = g.plans.range(..=tip).map(|(h, _)| *h).collect();
        for h in drop_h {
            g.plans.remove(&h);
        }
        let drop_hdr: Vec<u32> = g
            .headers
            .keys()
            .copied()
            .filter(|h| *h <= tip)
            .collect();
        for h in drop_hdr {
            if let Some(plan) = g.headers.remove(&h) {
                g.hash_to_height.remove(&plan.header_rec.hash);
            }
        }
        g.recompute_ready_through();
        self.ready_through
            .store(g.ready_through, Ordering::Relaxed);
    }

    /// Cache header + tx list for a cache height (tip-GCed; required for multi-block MTP).
    pub fn put_header_plan(
        &self,
        height: u32,
        header_fk: Fk,
        header_rec: HeaderRecord,
        tx_fks: Vec<Fk>,
        prev_hash: [u8; 32],
    ) {
        let mut g = self.inner.lock().unwrap();
        // Tip-GCed window only — never re-insert at/below tip (would stick until
        // a later advance and inflate conf_plans / RSS with full tx_fks vectors).
        if height <= g.tip {
            return;
        }
        let hash = header_rec.hash;
        // Replace supersedes prior hash at this height — drop stale reverse key.
        let stale_hash = g
            .headers
            .get(&height)
            .map(|old| old.header_rec.hash)
            .filter(|old_hash| *old_hash != hash);
        if let Some(old_hash) = stale_hash {
            g.hash_to_height.remove(&old_hash);
        }
        g.hash_to_height.insert(hash, height);
        // Publish immutable plan Arc (no rewrite-in-place of tx_fks).
        g.headers.insert(
            height,
            Arc::new(HeaderPlanCache {
                header_fk,
                header_rec,
                tx_fks,
                prev_hash,
            }),
        );
    }

    pub fn get_header_by_hash(
        &self,
        hash: &[u8; 32],
    ) -> Option<(Fk, HeaderRecord)> {
        let g = self.inner.lock().unwrap();
        let h = *g.hash_to_height.get(hash)?;
        let plan = g.headers.get(&h)?;
        Some((plan.header_fk, plan.header_rec.clone()))
    }

    pub fn get_header_plan(&self, height: u32) -> Option<HeaderPlanCache> {
        self.inner
            .lock()
            .unwrap()
            .headers
            .get(&height)
            .map(|a| (**a).clone())
    }

    /// Arc-publish get (refcount only — prefer when callers can hold Arc).
    pub fn get_header_plan_arc(&self, height: u32) -> Option<Arc<HeaderPlanCache>> {
        self.inner
            .lock()
            .unwrap()
            .headers
            .get(&height)
            .map(Arc::clone)
    }

    pub fn get_tx_fks_for_hash(&self, hash: &[u8; 32]) -> Option<Vec<Fk>> {
        let g = self.inner.lock().unwrap();
        let h = *g.hash_to_height.get(hash)?;
        g.headers.get(&h).map(|p| p.tx_fks.clone())
    }

    /// Ensure a height plan exists for `hash`.
    pub fn ensure_plan(&self, height: u32, hash: [u8; 32]) {
        let mut g = self.inner.lock().unwrap();
        if height <= g.tip {
            return;
        }
        g.plans.entry(height).or_insert_with(|| HeightPlan {
            hash,
            scanned: false,
        });
        if let Some(p) = g.plans.get_mut(&height) {
            p.hash = hash;
        }
    }

    /// Seed many plans under one lock (confirm load batch).
    pub fn ensure_plans(&self, items: &[(u32, [u8; 32])]) {
        if items.is_empty() {
            return;
        }
        let mut g = self.inner.lock().unwrap();
        for &(height, hash) in items {
            if height <= g.tip {
                continue;
            }
            g.plans.entry(height).or_insert_with(|| HeightPlan {
                hash,
                scanned: false,
            });
            if let Some(p) = g.plans.get_mut(&height) {
                p.hash = hash;
            }
        }
    }

    /// True if cache finished a scan attempt for this height.
    pub fn is_ready(&self, height: u32) -> bool {
        let g = self.inner.lock().unwrap();
        g.plans.get(&height).is_some_and(|p| p.is_ready())
    }

    /// All heights in `heights` ready (scanned).
    pub fn all_ready(&self, heights: &[u32]) -> bool {
        let g = self.inner.lock().unwrap();
        heights
            .iter()
            .all(|h| g.plans.get(h).is_some_and(|p| p.is_ready()))
    }

    /// Mark body scan complete for `height`.
    pub fn mark_scanned(&self, height: u32) {
        self.mark_scanned_many(&[height]);
    }

    /// Mark many heights scanned and recompute ready watermark once.
    pub fn mark_scanned_many(&self, heights: &[u32]) {
        if heights.is_empty() {
            return;
        }
        let mut g = self.inner.lock().unwrap();
        for &height in heights {
            if let Some(p) = g.plans.get_mut(&height) {
                p.scanned = true;
            }
        }
        g.recompute_ready_through();
        self.ready_through
            .store(g.ready_through, Ordering::Relaxed);
    }

    pub fn plan_count(&self) -> usize {
        self.inner.lock().unwrap().plans.len()
    }

    /// Number of cached header + tx_fks plans (IBD `conf_plans=` occupancy).
    ///
    /// Wire path uses [`Self::put_header_plan`]; the scan-watermark [`Self::plan_count`]
    /// map is often empty on that path — do not use it for process-owned sizes.
    pub fn header_plan_count(&self) -> usize {
        self.inner.lock().unwrap().headers.len()
    }
}

impl Inner {
    /// Contiguous **scanned** watermark from tip+1 upward.
    fn recompute_ready_through(&mut self) {
        let mut h = self.tip.saturating_add(1);
        loop {
            match self.plans.get(&h) {
                Some(p) if p.is_ready() => h = h.saturating_add(1),
                _ => break,
            }
        }
        self.ready_through = h.saturating_sub(1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn header_rec(hash: [u8; 32]) -> HeaderRecord {
        HeaderRecord {
            prev_fk: Fk::NULL,
            version: 1,
            timestamp: 1,
            bits: 0x1d00ffff,
            nonce: 0,
            merkle_root: [0u8; 32],
            hash,
        }
    }

    #[test]
    fn ensure_plans_skips_at_or_below_tip() {
        let c = ConfirmParentCache::new();
        c.advance_tip(360_250);
        c.ensure_plans(&[(360_250, [1u8; 32]), (360_251, [2u8; 32])]);
        assert!(!c.is_ready(360_250));
        assert!(!c.is_ready(360_251));
        c.mark_scanned(360_251);
        assert!(c.is_ready(360_251));
        assert_eq!(c.ready_through(), 360_251);
    }

    #[test]
    fn advance_tip_prunes_plans_and_headers() {
        let c = ConfirmParentCache::new();
        c.advance_tip(0);
        c.ensure_plan(1, [1u8; 32]);
        c.put_header_plan(1, Fk(1), header_rec([1u8; 32]), vec![Fk(10)], [0u8; 32]);
        c.mark_scanned(1);
        assert!(c.get_header_plan(1).is_some());
        c.advance_tip(1);
        assert!(!c.is_ready(1));
        assert!(c.get_header_plan(1).is_none());
        assert_eq!(c.plan_count(), 0);
        assert_eq!(c.ready_through(), 1);
    }

    /// Header plans are Arc-published: replace installs a new Arc; tip GC drops it.
    #[test]
    fn header_plan_arc_publish_and_tip_gc() {
        let c = ConfirmParentCache::new();
        c.advance_tip(0);
        c.put_header_plan(1, Fk(1), header_rec([1u8; 32]), vec![Fk(10)], [0u8; 32]);
        let a1 = c.get_header_plan_arc(1).expect("plan");
        assert_eq!(a1.tx_fks, vec![Fk(10)]);
        // Replace publishes a new Arc (not rewrite-in-place of the old body).
        c.put_header_plan(1, Fk(1), header_rec([1u8; 32]), vec![Fk(11), Fk(12)], [0u8; 32]);
        let a2 = c.get_header_plan_arc(1).expect("replaced");
        assert_eq!(a2.tx_fks, vec![Fk(11), Fk(12)]);
        assert!(!std::sync::Arc::ptr_eq(&a1, &a2));
        // Old Arc still valid for holder; cache holds only a2.
        assert_eq!(a1.tx_fks, vec![Fk(10)]);
        assert_eq!(c.header_plan_count(), 1);
        c.advance_tip(1);
        assert!(c.get_header_plan_arc(1).is_none());
        assert_eq!(c.header_plan_count(), 0);
    }

    #[test]
    fn recompute_watermark_scanned() {
        let c = ConfirmParentCache::new();
        c.advance_tip(0);
        c.ensure_plan(1, [1u8; 32]);
        c.put_header_plan(1, Fk(1), header_rec([1u8; 32]), vec![Fk(1001)], [0u8; 32]);
        c.mark_scanned(1);
        c.ensure_plan(2, [2u8; 32]);
        c.put_header_plan(2, Fk(2), header_rec([2u8; 32]), vec![Fk(1002)], [0u8; 32]);
        c.mark_scanned(2);
        assert_eq!(c.ready_through(), 2);
        c.ensure_plan(3, [3u8; 32]);
        assert_eq!(c.ready_through(), 2);
        c.mark_scanned(3);
        assert_eq!(c.ready_through(), 3);
    }

    /// Header plans must work regardless of denserels cache env (regression for
    /// mainnet tip freeze: `parent header plan missing above tip`).
    #[test]
    fn put_header_plan_always_stores_for_mtp() {
        let c = ConfirmParentCache::new();
        c.put_header_plan(1, Fk(1), header_rec([9u8; 32]), vec![Fk(1)], [0u8; 32]);
        let p = c.get_header_plan(1).expect("header plan required for multi-block MTP");
        assert_eq!(p.header_rec.hash, [9u8; 32]);
        assert_eq!(c.header_plan_count(), 1);
    }

    /// At/below tip must not re-enter the header map (process-owned RSS).
    #[test]
    fn put_header_plan_skips_at_or_below_tip() {
        let c = ConfirmParentCache::new();
        c.advance_tip(10);
        c.put_header_plan(10, Fk(10), header_rec([10u8; 32]), vec![Fk(1); 100], [0u8; 32]);
        c.put_header_plan(5, Fk(5), header_rec([5u8; 32]), vec![Fk(1); 100], [0u8; 32]);
        assert_eq!(c.header_plan_count(), 0);
        c.put_header_plan(11, Fk(11), header_rec([11u8; 32]), vec![Fk(1)], [0u8; 32]);
        assert_eq!(c.header_plan_count(), 1);
        c.advance_tip(11);
        assert_eq!(c.header_plan_count(), 0);
    }

    /// Replacing a height drops the stale hash reverse index (no hash_to_height leak).
    #[test]
    fn put_header_plan_replaces_stale_hash_reverse() {
        let c = ConfirmParentCache::new();
        c.put_header_plan(1, Fk(1), header_rec([1u8; 32]), vec![Fk(1)], [0u8; 32]);
        c.put_header_plan(1, Fk(1), header_rec([2u8; 32]), vec![Fk(2)], [0u8; 32]);
        assert!(c.get_header_by_hash(&[1u8; 32]).is_none());
        assert!(c.get_header_by_hash(&[2u8; 32]).is_some());
        assert_eq!(c.header_plan_count(), 1);
    }
}
