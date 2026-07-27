//! Process-local archive sticky: `txid → create_fk` (+ optional body range).
//!
//! Cross mega-batch RAM hit for parent resolve when packing create_fk into spends.
//! Capacity-capped FIFO (~4 M default ≈ 192 MiB planning budget for fk-only;
//! ranges add ~12 B/entry when present).
//!
//! **Hot path:** single Class A archive writer. `insert_many` / `lookup_batch` take
//! the mutex once per call. Lookup hits and re-inserts **touch** FIFO recency so
//! parents are not immediately evicted by a flood of new creates.
//!
//! **Body ranges:** when known (prewarm sequential idx, or post-body commit), each
//! entry also stores `(body_off, body_len)` so a hit skips both **`tx.head`** and
//! **`tx.idx`** (confirm pin_new / any `body_range` consumer via [`body_ranges_by_fk`]).

use rbitcoin_primitives::Fk;
use std::collections::{HashMap, VecDeque};
use std::sync::Mutex;

/// Default sticky capacity (entries). ~48 B effective/entry → ~192 MiB planning budget.
pub const DEFAULT_ARCHIVE_TXID_STICKY_CAP: usize = 4_000_000;

pub fn archive_txid_sticky_cap_from_env() -> usize {
    std::env::var("RBITCOIN_ARCHIVE_TXID_STICKY_CAP")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(DEFAULT_ARCHIVE_TXID_STICKY_CAP)
        .clamp(100_000, 20_000_000)
}

/// Lookup hit: create fk and optional packed body range (skip head + idx when set).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StickyHit {
    pub fk: Fk,
    /// `(absolute body off, len)` when known; `None` ⇒ fk only (still skips head).
    pub body_range: Option<(u64, u64)>,
}

struct Entry {
    fk: u64,
    /// 0 = range unknown.
    body_off: u64,
    body_len: u32,
    /// Monotonic stamp (u32); FIFO records carry stamp at push time.
    stamp: u32,
}

impl Entry {
    fn range(&self) -> Option<(u64, u64)> {
        if self.body_len == 0 {
            None
        } else {
            Some((self.body_off, u64::from(self.body_len)))
        }
    }

    fn set_range(&mut self, range: Option<(u64, u64)>) {
        match range {
            Some((off, len)) if len > 0 && len <= u64::from(u32::MAX) => {
                self.body_off = off;
                self.body_len = len as u32;
            }
            _ => {
                self.body_off = 0;
                self.body_len = 0;
            }
        }
    }
}

struct Inner {
    map: HashMap<[u8; 32], Entry>,
    /// Live stamp → txid (one entry per live stamp; touch rewrites stamp).
    stamp_to_txid: HashMap<u32, [u8; 32]>,
    /// create_fk → body range when known (confirm pin_new skips idx).
    fk_to_range: HashMap<u64, (u64, u32)>,
    /// Eviction order: stamps only (no 32 B txid per fifo record).
    fifo: VecDeque<u32>,
    cap: usize,
    next_stamp: u32,
}

/// Writer-thread sticky map (shared via `Query`; Mutex for API simplicity).
pub struct ArchiveTxidSticky {
    inner: Mutex<Inner>,
}

impl ArchiveTxidSticky {
    pub fn new(cap: usize) -> Self {
        let cap = cap.max(1).min(20_000_000);
        let init = cap.min(1 << 20);
        Self {
            inner: Mutex::new(Inner {
                map: HashMap::with_capacity(init),
                stamp_to_txid: HashMap::with_capacity(init),
                fk_to_range: HashMap::with_capacity(init),
                fifo: VecDeque::with_capacity(init),
                cap,
                next_stamp: 1,
            }),
        }
    }

    pub fn from_env() -> Self {
        Self::new(archive_txid_sticky_cap_from_env())
    }

    pub fn len(&self) -> usize {
        self.inner.lock().unwrap().map.len()
    }

    pub fn cap(&self) -> usize {
        self.inner.lock().unwrap().cap
    }

    /// `(map_len, cap, fifo_len)` under one lock.
    pub fn size_stats(&self) -> (usize, usize, usize) {
        let g = self.inner.lock().unwrap();
        (g.map.len(), g.cap, g.fifo.len())
    }

    pub fn reserve_for_prewarm(&self, n: usize) {
        let mut g = self.inner.lock().unwrap();
        let want = n.min(g.cap);
        let have = g.map.len();
        g.map.reserve(want.saturating_sub(have));
        g.stamp_to_txid.reserve(want.saturating_sub(have));
        g.fk_to_range.reserve(want.saturating_sub(have));
    }

    /// Insert fk-only mappings (body range unknown).
    pub fn insert_many(&self, entries: &[([u8; 32], Fk)]) {
        if entries.is_empty() {
            return;
        }
        let mut g = self.inner.lock().unwrap();
        for &(txid, fk) in entries {
            if fk.is_null() {
                continue;
            }
            g.insert_one(txid, fk.0, None);
        }
    }

    /// Insert mappings with packed body ranges (from idx at insert time).
    pub fn insert_many_with_ranges(&self, entries: &[([u8; 32], Fk, u64, u64)]) {
        if entries.is_empty() {
            return;
        }
        let mut g = self.inner.lock().unwrap();
        for &(txid, fk, off, len) in entries {
            if fk.is_null() {
                continue;
            }
            let range = if len > 0 { Some((off, len)) } else { None };
            g.insert_one(txid, fk.0, range);
        }
    }

    /// Lookup by txid: hits skip head; range present ⇒ also skip idx for that create.
    pub fn lookup_batch(&self, txids: &[[u8; 32]]) -> HashMap<[u8; 32], StickyHit> {
        if txids.is_empty() {
            return HashMap::new();
        }
        let mut g = self.inner.lock().unwrap();
        let mut out = HashMap::with_capacity(txids.len() / 2);
        for t in txids {
            let Some(e) = g.map.get(t) else {
                continue;
            };
            let hit = StickyHit {
                fk: Fk(e.fk),
                body_range: e.range(),
            };
            g.touch(*t);
            out.insert(*t, hit);
        }
        g.maybe_compact_fifo();
        out
    }

    /// Bulk body ranges by create fk (confirm pin_new). Misses are `None`.
    pub fn body_ranges_by_fk(&self, fks: &[Fk]) -> Vec<Option<(u64, u64)>> {
        if fks.is_empty() {
            return Vec::new();
        }
        let g = self.inner.lock().unwrap();
        fks.iter()
            .map(|fk| {
                let Some(id) = fk.get() else {
                    return None;
                };
                g.fk_to_range
                    .get(&id)
                    .map(|&(off, len)| (off, u64::from(len)))
            })
            .collect()
    }

    /// Test-only: set the next stamp near wrap so the next alloc rewrites.
    #[cfg(test)]
    pub(crate) fn force_next_stamp(&self, stamp: u32) {
        self.inner.lock().unwrap().next_stamp = stamp;
    }
}

impl Inner {
    fn alloc_stamp(&mut self) -> u32 {
        // Skip 0 (unused). On wrap, compact aggressively.
        let s = self.next_stamp;
        self.next_stamp = self.next_stamp.wrapping_add(1);
        if self.next_stamp == 0 {
            self.next_stamp = 1;
            self.rewrite_all_stamps();
        }
        if s == 0 {
            return self.alloc_stamp();
        }
        s
    }

    /// Rare stamp wrap: reassign dense stamps 1..n.
    fn rewrite_all_stamps(&mut self) {
        self.stamp_to_txid.clear();
        self.fifo.clear();
        let keys: Vec<[u8; 32]> = self.map.keys().copied().collect();
        let mut n = 1u32;
        for k in keys {
            if let Some(e) = self.map.get_mut(&k) {
                e.stamp = n;
                self.stamp_to_txid.insert(n, k);
                self.fifo.push_back(n);
                n = n.wrapping_add(1);
                if n == 0 {
                    n = 1;
                }
            }
        }
        self.next_stamp = n;
    }

    fn touch(&mut self, txid: [u8; 32]) {
        let old_stamp = match self.map.get(&txid) {
            Some(e) => e.stamp,
            None => return,
        };
        self.stamp_to_txid.remove(&old_stamp);
        let stamp = self.alloc_stamp();
        if let Some(e) = self.map.get_mut(&txid) {
            e.stamp = stamp;
        }
        self.stamp_to_txid.insert(stamp, txid);
        self.fifo.push_back(stamp);
    }

    fn insert_one(&mut self, txid: [u8; 32], fk: u64, range: Option<(u64, u64)>) {
        if let Some(e) = self.map.get_mut(&txid) {
            let old_fk = e.fk;
            if old_fk != fk {
                self.fk_to_range.remove(&old_fk);
            }
            e.fk = fk;
            // Prefer new range when provided; keep existing if update is fk-only.
            if range.is_some() {
                e.set_range(range);
            }
            if let Some(r) = e.range() {
                self.fk_to_range
                    .insert(fk, (r.0, r.1.min(u64::from(u32::MAX)) as u32));
            } else {
                self.fk_to_range.remove(&fk);
            }
            let old = e.stamp;
            self.stamp_to_txid.remove(&old);
            let stamp = self.alloc_stamp();
            if let Some(e) = self.map.get_mut(&txid) {
                e.stamp = stamp;
            }
            self.stamp_to_txid.insert(stamp, txid);
            self.fifo.push_back(stamp);
        } else {
            while self.map.len() >= self.cap {
                if !self.evict_one() {
                    break;
                }
            }
            let stamp = self.alloc_stamp();
            let mut e = Entry {
                fk,
                body_off: 0,
                body_len: 0,
                stamp,
            };
            e.set_range(range);
            if let Some(r) = e.range() {
                self.fk_to_range
                    .insert(fk, (r.0, r.1.min(u64::from(u32::MAX)) as u32));
            }
            self.map.insert(txid, e);
            self.stamp_to_txid.insert(stamp, txid);
            self.fifo.push_back(stamp);
        }
        self.maybe_compact_fifo();
    }

    fn evict_one(&mut self) -> bool {
        while let Some(stamp) = self.fifo.pop_front() {
            let Some(txid) = self.stamp_to_txid.remove(&stamp) else {
                continue; // stale fifo stamp
            };
            match self.map.get(&txid) {
                Some(e) if e.stamp == stamp => {
                    let fk = e.fk;
                    self.map.remove(&txid);
                    self.fk_to_range.remove(&fk);
                    return true;
                }
                _ => {}
            }
        }
        false
    }

    fn maybe_compact_fifo(&mut self) {
        let limit = self.cap.saturating_mul(2).max(self.map.len().saturating_mul(2));
        if self.fifo.len() <= limit {
            return;
        }
        let mut kept = VecDeque::with_capacity(self.map.len().saturating_add(64));
        let mut seen: HashMap<u32, ()> = HashMap::with_capacity(self.map.len());
        while let Some(stamp) = self.fifo.pop_back() {
            if self.stamp_to_txid.contains_key(&stamp) && seen.insert(stamp, ()).is_none() {
                kept.push_front(stamp);
            }
        }
        self.fifo = kept;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sticky_map_stays_at_cap_under_unique_flood() {
        let s = ArchiveTxidSticky::new(1000);
        for i in 0..5000u64 {
            let mut t = [0u8; 32];
            t[0..8].copy_from_slice(&i.to_le_bytes());
            s.insert_many(&[(t, Fk(i + 1))]);
            assert!(s.len() <= s.cap(), "len {} > cap {}", s.len(), s.cap());
        }
        assert_eq!(s.len(), s.cap());
    }

    #[test]
    fn lookup_touch_keeps_parent_under_flood() {
        let s = ArchiveTxidSticky::new(100);
        let mut parent = [0u8; 32];
        parent[31] = 0xff;
        s.insert_many(&[(parent, Fk(42))]);
        for i in 0..1000u64 {
            let _ = s.lookup_batch(&[parent]);
            let mut cold = [0u8; 32];
            cold[0..8].copy_from_slice(&i.to_le_bytes());
            s.insert_many(&[(cold, Fk(i + 100))]);
        }
        let hit = s.lookup_batch(&[parent]);
        assert_eq!(hit.get(&parent).map(|h| h.fk), Some(Fk(42)));
    }

    #[test]
    fn range_cached_on_insert_and_fk_lookup() {
        let s = ArchiveTxidSticky::new(100);
        let t = [9u8; 32];
        s.insert_many_with_ranges(&[(t, Fk(7), 1000, 50)]);
        let hit = s.lookup_batch(&[t]);
        assert_eq!(hit[&t].fk, Fk(7));
        assert_eq!(hit[&t].body_range, Some((1000, 50)));
        let by_fk = s.body_ranges_by_fk(&[Fk(7), Fk(8)]);
        assert_eq!(by_fk, vec![Some((1000, 50)), None]);
        // fk-only update keeps range
        s.insert_many(&[(t, Fk(7))]);
        assert_eq!(s.lookup_batch(&[t])[&t].body_range, Some((1000, 50)));
        // new range overwrites
        s.insert_many_with_ranges(&[(t, Fk(7), 2000, 80)]);
        assert_eq!(s.lookup_batch(&[t])[&t].body_range, Some((2000, 80)));
        assert_eq!(s.body_ranges_by_fk(&[Fk(7)]), vec![Some((2000, 80))]);
    }

    #[test]
    fn fifo_does_not_store_txid_bytes_per_record() {
        let s = ArchiveTxidSticky::new(100);
        for i in 0..50u64 {
            let mut t = [0u8; 32];
            t[0..8].copy_from_slice(&i.to_le_bytes());
            s.insert_many(&[(t, Fk(i + 1))]);
        }
        let (len, _cap, fifo) = s.size_stats();
        assert_eq!(len, 50);
        assert!(fifo >= 50 && fifo < 50 * 4, "fifo={fifo}");
    }

    #[test]
    fn empty_ops_null_fk_and_reserve() {
        let s = ArchiveTxidSticky::new(2);
        s.insert_many(&[]);
        assert!(s.lookup_batch(&[]).is_empty());
        s.reserve_for_prewarm(8);
        let t1 = [1u8; 32];
        let t2 = [2u8; 32];
        let t3 = [3u8; 32];
        s.insert_many(&[(t1, Fk::NULL), (t1, Fk(1)), (t2, Fk(2)), (t3, Fk(3))]);
        assert!(s.len() <= 2);
        s.insert_many(&[(t3, Fk(30))]);
        let hit = s.lookup_batch(&[t1, t2, t3]);
        assert!(hit.len() <= 2);
    }

    #[test]
    fn stamp_wrap_rewrite_and_stale_fifo_evict() {
        let s = ArchiveTxidSticky::new(4);
        for i in 0..4u64 {
            let mut t = [0u8; 32];
            t[0..8].copy_from_slice(&i.to_le_bytes());
            s.insert_many(&[(t, Fk(i + 1))]);
        }
        s.force_next_stamp(u32::MAX);
        let mut t = [0u8; 32];
        t[0] = 0xee;
        s.insert_many(&[(t, Fk(99))]);
        let hit = s.lookup_batch(&[t]);
        assert!(hit.get(&t).is_some() || s.len() <= 4);

        let parent = {
            let mut p = [0u8; 32];
            p[31] = 0xaa;
            p
        };
        s.insert_many(&[(parent, Fk(7))]);
        for _ in 0..20 {
            let _ = s.lookup_batch(&[parent]);
        }
        for i in 0..20u64 {
            let mut cold = [0u8; 32];
            cold[0..8].copy_from_slice(&(i + 1000).to_le_bytes());
            s.insert_many(&[(cold, Fk(i + 1000))]);
        }
        assert!(s.len() <= s.cap());
        let _ = s.lookup_batch(&[parent]);
        assert_eq!(s.cap(), 4);
        let _ = ArchiveTxidSticky::from_env();
        let _ = archive_txid_sticky_cap_from_env();
    }
}
