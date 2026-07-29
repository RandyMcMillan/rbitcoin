//! Process-local archive sticky: `txid → create_fk` (+ optional body range).
//!
//! Cross mega-batch RAM hit for parent resolve when packing create_fk into spends.
//! Capacity-capped **raw FIFO** (~8 M default ≈ 384 MiB planning budget for fk-only;
//! ranges add ~12 B/entry when present).
//!
//! **Hot path:** single Class A archive writer. `insert_many` / `lookup_batch` take
//! the mutex once per call. **No LRU / touch:** lookups are read-only; re-insert of
//! an existing txid updates the entry in place without reordering the FIFO.
//! Eviction always drops the oldest insert (front of the queue).
//!
//! **Body ranges:** when known (prewarm sequential idx, or post-body commit), each
//! entry also stores `(body_off, body_len)` so a hit skips both **`tx.head`** and
//! **`tx.idx`** (confirm pin_new / any `body_range` consumer via [`body_ranges_by_fk`]).

use rbitcoin_primitives::Fk;
use std::collections::{HashMap, VecDeque};
use std::sync::Mutex;

/// Default sticky capacity (entries). ~48 B effective/entry → ~384 MiB planning budget.
pub const DEFAULT_ARCHIVE_TXID_STICKY_CAP: usize = 8_000_000;

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
    /// create_fk → body range when known (confirm pin_new skips idx).
    fk_to_range: HashMap<u64, (u64, u32)>,
    /// Eviction order: oldest insert at front (raw FIFO; no touch/reorder).
    fifo: VecDeque<[u8; 32]>,
    cap: usize,
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
                fk_to_range: HashMap::with_capacity(init),
                fifo: VecDeque::with_capacity(init),
                cap,
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
        g.fk_to_range.reserve(want.saturating_sub(have));
        g.fifo.reserve(want.saturating_sub(have));
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
    /// Read-only — does **not** reorder the FIFO.
    /// Legacy mirror lookup (wire plan uses CreateResidency only; kept for tests/prewarm stats).
    #[allow(dead_code)]
    pub fn lookup_batch(&self, txids: &[[u8; 32]]) -> HashMap<[u8; 32], StickyHit> {
        if txids.is_empty() {
            return HashMap::new();
        }
        let g = self.inner.lock().unwrap();
        let mut out = HashMap::with_capacity(txids.len() / 2);
        for t in txids {
            let Some(e) = g.map.get(t) else {
                continue;
            };
            out.insert(
                *t,
                StickyHit {
                    fk: Fk(e.fk),
                    body_range: e.range(),
                },
            );
        }
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
}

impl Inner {
    fn insert_one(&mut self, txid: [u8; 32], fk: u64, range: Option<(u64, u64)>) {
        if let Some(e) = self.map.get_mut(&txid) {
            // In-place update only — do not re-queue (raw FIFO, not LRU).
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
            return;
        }
        while self.map.len() >= self.cap {
            if !self.evict_one() {
                break;
            }
        }
        let mut e = Entry {
            fk,
            body_off: 0,
            body_len: 0,
        };
        e.set_range(range);
        if let Some(r) = e.range() {
            self.fk_to_range
                .insert(fk, (r.0, r.1.min(u64::from(u32::MAX)) as u32));
        }
        self.map.insert(txid, e);
        self.fifo.push_back(txid);
    }

    fn evict_one(&mut self) -> bool {
        while let Some(txid) = self.fifo.pop_front() {
            if let Some(e) = self.map.remove(&txid) {
                self.fk_to_range.remove(&e.fk);
                return true;
            }
            // Stale fifo entry (should not happen with pure FIFO insert).
        }
        false
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
        // Oldest inserts are gone (raw FIFO).
        let mut first = [0u8; 32];
        first[0..8].copy_from_slice(&0u64.to_le_bytes());
        assert!(s.lookup_batch(&[first]).is_empty());
        let mut last = [0u8; 32];
        last[0..8].copy_from_slice(&4999u64.to_le_bytes());
        assert_eq!(
            s.lookup_batch(&[last]).get(&last).map(|h| h.fk),
            Some(Fk(5000))
        );
    }

    /// Lookup must not keep a parent under insert flood (not an LRU).
    #[test]
    fn lookup_does_not_touch_fifo_order() {
        let s = ArchiveTxidSticky::new(100);
        let mut parent = [0u8; 32];
        parent[31] = 0xff;
        s.insert_many(&[(parent, Fk(42))]);
        for i in 0..1000u64 {
            // Lookups must not refresh eviction order.
            let _ = s.lookup_batch(&[parent]);
            let mut cold = [0u8; 32];
            cold[0..8].copy_from_slice(&i.to_le_bytes());
            s.insert_many(&[(cold, Fk(i + 100))]);
        }
        let hit = s.lookup_batch(&[parent]);
        assert!(
            hit.get(&parent).is_none(),
            "raw FIFO: parent at front must be evicted despite lookups"
        );
        assert_eq!(s.len(), s.cap());
    }

    #[test]
    fn reinsert_updates_in_place_without_evict_refresh() {
        let s = ArchiveTxidSticky::new(3);
        let a = [1u8; 32];
        let b = [2u8; 32];
        let c = [3u8; 32];
        let d = [4u8; 32];
        s.insert_many(&[(a, Fk(1)), (b, Fk(2)), (c, Fk(3))]);
        // Re-insert a with new fk — stays in place (oldest), does not move to back.
        s.insert_many(&[(a, Fk(10))]);
        assert_eq!(s.lookup_batch(&[a])[&a].fk, Fk(10));
        assert_eq!(s.len(), 3);
        // New insert d evicts a (still oldest).
        s.insert_many(&[(d, Fk(4))]);
        assert!(s.lookup_batch(&[a]).is_empty());
        assert_eq!(s.lookup_batch(&[d])[&d].fk, Fk(4));
        assert_eq!(s.len(), 3);
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
    fn fifo_len_matches_map_under_pure_fifo() {
        let s = ArchiveTxidSticky::new(100);
        for i in 0..50u64 {
            let mut t = [0u8; 32];
            t[0..8].copy_from_slice(&i.to_le_bytes());
            s.insert_many(&[(t, Fk(i + 1))]);
        }
        let (len, _cap, fifo) = s.size_stats();
        assert_eq!(len, 50);
        assert_eq!(fifo, 50, "pure FIFO: order length equals map size");
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
        s.insert_many(&[(t3, Fk(30))]); // in-place if t3 present, else new
        let hit = s.lookup_batch(&[t1, t2, t3]);
        assert!(hit.len() <= 2);
        let _ = ArchiveTxidSticky::from_env();
        let _ = archive_txid_sticky_cap_from_env();
    }
}
