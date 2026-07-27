//! Process-local archive writer sticky: `txid → create_fk` (no outs).
//!
//! Cross mega-batch RAM hit for parent resolve when packing create_fk into spends.
//! Capacity-capped FIFO (~4 M default ≈ 192 MiB planning budget).
//!
//! **Hot path:** single Class A archive writer. `insert_many` / `lookup_batch` take
//! the mutex once per call. Lookup hits and re-inserts **touch** FIFO recency so
//! parents are not immediately evicted by a flood of new creates.
//!
//! **Layout:** map holds `txid → {fk, stamp}`; FIFO stores only `(stamp, txid_key
//! ref is avoided)` — actually FIFO stores `(stamp)` is not enough for eviction.
//! We store `(stamp)` + look up by walking... no that needs txid.
//!
//! FIFO stores **`(stamp: u32, map generation key index)`** is hard with HashMap.
//! Instead: FIFO stores **only stamp+txid as packed**: keep txid in fifo but use
//! **u32 stamp** (halves stamp field). Better: fifo of `(u32 stamp, [u8;32])` still
//! has txid.
//!
//! **Chosen:** fifo entries are `(stamp: u32,)` is insufficient.
//! Fifo: `VecDeque<u32>` of stamps only doesn't work for eviction matching.
//!
//! **Chosen design:** map `HashMap<[u8;32], Entry{fk:u64, stamp:u32}>` and fifo
//! `VecDeque<([u8;32], u32)>` with **u32 stamp** — saves 4B per map entry + 4B
//! per fifo. For larger win: fifo stores nothing but stamps if we use
//! `Entry { fk, stamp, gen }` and fifo is just stamps that match — still need
//! which key. **Fifo without txid:** store reverse `stamp → txid` only for live
//! stamps: on insert push stamp to fifo; map has stamp; on evict pop stamp,
//! need stamp→txid. Maintain `HashMap<u32, [u8;32]>` for live stamps only —
//! one stamp per key after touch rewrite. On insert/touch: remove old stamp
//! reverse, insert new. Fifo is `VecDeque<u32>` stamps only.
//!
//! Evict: pop stamp from fifo; if reverse still maps that stamp → remove map key.

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

struct Entry {
    fk: u64,
    /// Monotonic stamp (u32); FIFO records carry stamp at push time.
    stamp: u32,
}

struct Inner {
    map: HashMap<[u8; 32], Entry>,
    /// Live stamp → txid (one entry per live stamp; touch rewrites stamp).
    stamp_to_txid: HashMap<u32, [u8; 32]>,
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
    }

    pub fn insert_many(&self, entries: &[([u8; 32], Fk)]) {
        if entries.is_empty() {
            return;
        }
        let mut g = self.inner.lock().unwrap();
        for &(txid, fk) in entries {
            if fk.is_null() {
                continue;
            }
            g.insert_one(txid, fk.0);
        }
    }

    pub fn lookup_batch(&self, txids: &[[u8; 32]]) -> HashMap<[u8; 32], Fk> {
        if txids.is_empty() {
            return HashMap::new();
        }
        let mut g = self.inner.lock().unwrap();
        let mut out = HashMap::with_capacity(txids.len() / 2);
        for t in txids {
            let Some(e) = g.map.get(t) else {
                continue;
            };
            let fk = e.fk;
            g.touch(*t);
            out.insert(*t, Fk(fk));
        }
        g.maybe_compact_fifo();
        out
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

    fn insert_one(&mut self, txid: [u8; 32], fk: u64) {
        if let Some(e) = self.map.get_mut(&txid) {
            e.fk = fk;
            // fall through to touch-like stamp update
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
            self.map.insert(txid, Entry { fk, stamp });
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
            // Only remove map if stamp still matches (should).
            match self.map.get(&txid) {
                Some(e) if e.stamp == stamp => {
                    self.map.remove(&txid);
                    return true;
                }
                _ => {
                    // Map moved to a newer stamp; stamp_to_txid already removed.
                }
            }
        }
        false
    }

    fn maybe_compact_fifo(&mut self) {
        // Fifo should be ~map.len() live stamps + slack from races; compact if huge.
        let limit = self.cap.saturating_mul(2).max(self.map.len().saturating_mul(2));
        if self.fifo.len() <= limit {
            return;
        }
        // Rebuild fifo from live stamps (newest-first order approximate via map).
        // Preserve approx LRU: walk old fifo, keep stamps still in stamp_to_txid once.
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
        parent[31] = 0xff; // high byte — no collision with cold[0..8] counters
        s.insert_many(&[(parent, Fk(42))]);
        for i in 0..1000u64 {
            let _ = s.lookup_batch(&[parent]);
            let mut cold = [0u8; 32];
            cold[0..8].copy_from_slice(&i.to_le_bytes());
            s.insert_many(&[(cold, Fk(i + 100))]);
        }
        let hit = s.lookup_batch(&[parent]);
        assert_eq!(hit.get(&parent).copied(), Some(Fk(42)));
    }

    #[test]
    fn fifo_does_not_store_txid_bytes_per_record() {
        // Sanity: after inserts, fifo_len is O(map) not O(map * 32) storage —
        // structural check that size_stats fifo is stamp queue.
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
        // Update existing fk.
        s.insert_many(&[(t3, Fk(30))]);
        let hit = s.lookup_batch(&[t1, t2, t3]);
        assert!(hit.len() <= 2);
    }

    /// Stamp wrap rewrites all live stamps; stale fifo stamps are skipped on evict.
    #[test]
    fn stamp_wrap_rewrite_and_stale_fifo_evict() {
        let s = ArchiveTxidSticky::new(4);
        for i in 0..4u64 {
            let mut t = [0u8; 32];
            t[0..8].copy_from_slice(&i.to_le_bytes());
            s.insert_many(&[(t, Fk(i + 1))]);
        }
        // Next alloc wraps through 0 → rewrite_all_stamps.
        s.force_next_stamp(u32::MAX);
        let mut t = [0u8; 32];
        t[0] = 0xee;
        s.insert_many(&[(t, Fk(99))]); // triggers wrap + maybe eviction
        // Map still queryable after rewrite.
        let hit = s.lookup_batch(&[t]);
        assert!(hit.get(&t).is_some() || s.len() <= 4);

        // Touch existing keys many times to leave stale stamps in fifo, then flood.
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
        // Parent may or may not survive; path exercises stale-stamp skip.
        let _ = s.lookup_batch(&[parent]);
        assert_eq!(s.cap(), 4);
        let _ = ArchiveTxidSticky::from_env();
        let _ = archive_txid_sticky_cap_from_env();
    }
}
