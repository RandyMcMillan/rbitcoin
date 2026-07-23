//! Process-local archive writer sticky: `txid → create_fk` (no outs / mlock).
//!
//! Cross mega-batch RAM hit for parent resolve when packing create_fk into spends.
//! Capacity-capped FIFO (~4 M default ≈ 192 MiB planning budget).
//!
//! **Hot path:** single Class A archive writer. `insert_many` / `lookup_batch` take
//! the mutex once per call. Lookup hits and re-inserts **touch** FIFO recency so
//! parents are not immediately evicted by a flood of new creates.

use rbitcoin_primitives::Fk;
use std::collections::{HashMap, VecDeque};
use std::sync::Mutex;

/// Default sticky capacity (entries). ~48 B effective/entry → ~192 MiB.
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
    /// Monotonic stamp; FIFO records carry the stamp at push time. Stale pops skip.
    stamp: u64,
}

struct Inner {
    map: HashMap<[u8; 32], Entry>,
    /// `(txid, stamp)` — may contain stale stamps after touch.
    fifo: VecDeque<([u8; 32], u64)>,
    cap: usize,
    next_stamp: u64,
}

/// Writer-thread sticky map (shared via `Query`; Mutex for API simplicity).
pub struct ArchiveTxidSticky {
    inner: Mutex<Inner>,
}

impl ArchiveTxidSticky {
    pub fn new(cap: usize) -> Self {
        let cap = cap.clamp(100_000, 20_000_000);
        // Modest initial capacity; [`Self::reserve_for_prewarm`] grows for startup fill.
        Self {
            inner: Mutex::new(Inner {
                map: HashMap::with_capacity(cap.min(1 << 20)),
                fifo: VecDeque::with_capacity(cap.min(1 << 20)),
                cap,
                next_stamp: 1,
            }),
        }
    }

    pub fn from_env() -> Self {
        Self::new(archive_txid_sticky_cap_from_env())
    }

    /// Current entries (for IBD diagnostics).
    pub fn len(&self) -> usize {
        self.inner.lock().unwrap().map.len()
    }

    pub fn cap(&self) -> usize {
        self.inner.lock().unwrap().cap
    }

    /// Grow map toward full cap before a linear prewarm (one rehash, not thrash).
    pub fn reserve_for_prewarm(&self, n: usize) {
        let mut g = self.inner.lock().unwrap();
        let want = n.min(g.cap);
        let have = g.map.len();
        g.map.reserve(want.saturating_sub(have));
    }

    /// Batch insert under **one** lock (mega-batch path).
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

    /// Batch lookup under **one** lock. Hits are **touched** (FIFO recency) so
    /// frequently resolved parents survive create-flood eviction.
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
            let stamp = g.next_stamp;
            g.next_stamp = g.next_stamp.saturating_add(1);
            if let Some(e) = g.map.get_mut(t) {
                e.stamp = stamp;
            }
            g.fifo.push_back((*t, stamp));
            out.insert(*t, Fk(fk));
        }
        // Bound fifo growth from touch duplicates (lazy GC of stale stamps).
        g.maybe_compact_fifo();
        out
    }
}

impl Inner {
    fn insert_one(&mut self, txid: [u8; 32], fk: u64) {
        let stamp = self.next_stamp;
        self.next_stamp = self.next_stamp.saturating_add(1);
        match self.map.get_mut(&txid) {
            Some(e) => {
                e.fk = fk;
                e.stamp = stamp;
                self.fifo.push_back((txid, stamp));
            }
            None => {
                while self.map.len() >= self.cap {
                    if !self.evict_one() {
                        break;
                    }
                }
                self.map.insert(txid, Entry { fk, stamp });
                self.fifo.push_back((txid, stamp));
            }
        }
        self.maybe_compact_fifo();
    }

    /// Pop FIFO until a live stamp is removed from the map.
    fn evict_one(&mut self) -> bool {
        while let Some((old, stamp)) = self.fifo.pop_front() {
            match self.map.get(&old) {
                Some(e) if e.stamp == stamp => {
                    self.map.remove(&old);
                    return true;
                }
                _ => {
                    // Stale fifo record (touched/re-inserted later) — skip.
                }
            }
        }
        false
    }

    /// Drop stale fifo heads without removing live map entries.
    fn maybe_compact_fifo(&mut self) {
        // Allow some slack for touch dups; compact when fifo >> map.
        let limit = self.cap.saturating_mul(3).max(self.map.len().saturating_mul(2));
        if self.fifo.len() <= limit {
            return;
        }
        let mut kept = VecDeque::with_capacity(self.map.len().saturating_add(64));
        while let Some((txid, stamp)) = self.fifo.pop_front() {
            if self.map.get(&txid).is_some_and(|e| e.stamp == stamp) {
                kept.push_back((txid, stamp));
            }
        }
        self.fifo = kept;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sticky_fifo_and_lookup() {
        let s = ArchiveTxidSticky::new(100_000);
        let t1 = [1u8; 32];
        let t2 = [2u8; 32];
        s.insert_many(&[(t1, Fk(10)), (t2, Fk(20))]);
        let batch = s.lookup_batch(&[t1, t2, [3u8; 32]]);
        assert_eq!(batch.get(&t1), Some(&Fk(10)));
        assert_eq!(batch.get(&t2), Some(&Fk(20)));
        assert!(!batch.contains_key(&[3u8; 32]));
    }

    #[test]
    fn sticky_evicts_fifo() {
        let s = ArchiveTxidSticky::new(100_000);
        let mut entries = Vec::new();
        for i in 0u32..10 {
            let mut t = [0u8; 32];
            t[0..4].copy_from_slice(&i.to_le_bytes());
            entries.push((t, Fk(i as u64 + 1)));
        }
        s.insert_many(&entries);
        let keys: Vec<[u8; 32]> = entries.iter().map(|(t, _)| *t).collect();
        let hits = s.lookup_batch(&keys);
        assert_eq!(hits.len(), 10);
        s.insert_many(&[(keys[0], Fk(999))]);
        assert_eq!(s.lookup_batch(&[keys[0]]).get(&keys[0]), Some(&Fk(999)));
    }

    #[test]
    fn touch_protects_hot_keys_from_create_flood() {
        // Cap 100_000 (minimum clamp). Insert 100k cold, touch one key, flood
        // with 50k new creates — touched key must survive.
        let s = ArchiveTxidSticky::new(100_000);
        let hot = {
            let mut t = [0u8; 32];
            t[0] = 0xab;
            t
        };
        s.insert_many(&[(hot, Fk(1))]);
        // Fill to capacity with cold keys.
        let mut cold = Vec::with_capacity(100_000);
        for i in 1u32..100_000 {
            let mut t = [0u8; 32];
            t[0..4].copy_from_slice(&i.to_le_bytes());
            t[4] = 0xcc;
            cold.push((t, Fk(i as u64 + 1)));
        }
        s.insert_many(&cold);
        assert_eq!(s.len(), 100_000);
        // Touch hot (lookup).
        let hit = s.lookup_batch(&[hot]);
        assert_eq!(hit.get(&hot), Some(&Fk(1)));
        // Flood more creates (evict oldest).
        let mut flood = Vec::with_capacity(50_000);
        for i in 0u32..50_000 {
            let mut t = [0u8; 32];
            t[0..4].copy_from_slice(&i.to_le_bytes());
            t[4] = 0xdd;
            flood.push((t, Fk(1_000_000 + i as u64)));
        }
        s.insert_many(&flood);
        assert_eq!(s.len(), 100_000);
        assert_eq!(
            s.lookup_batch(&[hot]).get(&hot),
            Some(&Fk(1)),
            "hot parent must survive create flood after touch"
        );
    }

    #[test]
    fn insert_many_single_lock_matches_sequential() {
        let s = ArchiveTxidSticky::new(100_000);
        let mut entries = Vec::new();
        for i in 0u32..1000 {
            let mut t = [0u8; 32];
            t[0..4].copy_from_slice(&i.to_le_bytes());
            entries.push((t, Fk(i as u64 + 1)));
        }
        s.insert_many(&entries);
        assert_eq!(s.len(), 1000);
        let keys: Vec<_> = entries.iter().map(|(t, _)| *t).collect();
        assert_eq!(s.lookup_batch(&keys).len(), 1000);
    }
}
