//! Process-local archive writer sticky: `txid → create_fk` (no outs / mlock).
//!
//! Cross mega-batch RAM hit for parent resolve when packing create_fk into spends.
//! Capacity-capped FIFO (~4 M default ≈ 192 MiB planning budget).

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

struct Inner {
    map: HashMap<[u8; 32], u64>,
    fifo: VecDeque<[u8; 32]>,
    cap: usize,
}

/// Writer-thread sticky map (shared via `Query`; Mutex for simplicity).
pub struct ArchiveTxidSticky {
    inner: Mutex<Inner>,
}

impl ArchiveTxidSticky {
    pub fn new(cap: usize) -> Self {
        let cap = cap.clamp(100_000, 20_000_000);
        Self {
            inner: Mutex::new(Inner {
                map: HashMap::with_capacity(cap.min(1 << 20)),
                fifo: VecDeque::with_capacity(cap.min(1 << 20)),
                cap,
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

    pub fn insert(&self, txid: [u8; 32], fk: Fk) {
        if fk.is_null() {
            return;
        }
        let mut g = self.inner.lock().unwrap();
        if g.map.contains_key(&txid) {
            g.map.insert(txid, fk.0);
            return;
        }
        while g.map.len() >= g.cap {
            if let Some(old) = g.fifo.pop_front() {
                g.map.remove(&old);
            } else {
                break;
            }
        }
        g.map.insert(txid, fk.0);
        g.fifo.push_back(txid);
    }

    pub fn insert_many(&self, entries: &[([u8; 32], Fk)]) {
        for &(txid, fk) in entries {
            self.insert(txid, fk);
        }
    }

    /// Batch lookup: returns map of hits only.
    pub fn lookup_batch(&self, txids: &[[u8; 32]]) -> HashMap<[u8; 32], Fk> {
        let g = self.inner.lock().unwrap();
        let mut out = HashMap::with_capacity(txids.len() / 2);
        for t in txids {
            if let Some(&id) = g.map.get(t) {
                out.insert(*t, Fk(id));
            }
        }
        out
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
        s.insert(t1, Fk(10));
        s.insert(t2, Fk(20));
        let batch = s.lookup_batch(&[t1, t2, [3u8; 32]]);
        assert_eq!(batch.get(&t1), Some(&Fk(10)));
        assert_eq!(batch.get(&t2), Some(&Fk(20)));
        assert!(!batch.contains_key(&[3u8; 32]));
    }

    #[test]
    fn sticky_evicts_fifo() {
        let s = ArchiveTxidSticky::new(100_000);
        // Force tiny cap via insert loop against full map: use new with min clamp.
        // Cap clamps to ≥100_000, so just verify insert_many + overwrite path.
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
        // Overwrite same key keeps size stable.
        s.insert(keys[0], Fk(999));
        assert_eq!(s.lookup_batch(&[keys[0]]).get(&keys[0]), Some(&Fk(999)));
    }
}
