//! Overflow open-hash for `tx.head` inserts that exhaust probe depth on the primary.
//!
//! While the primary address head is full/resizing, depth-exhausted inserts land
//! here so the archive/confirm write path does **not** stall on a multi‑GiB rehash.
//!
//! **Lookup order (depth-first):** probe overflow first, then the primary head.
//! Keys are whatever the caller passes (callers should pass **mixed** txids from
//! [`crate::store_secret::StoreSecret::mix_txid`]).
//!
//! **Lifetime swap:** when a wider `tx.head` replaces the overflowing primary, the
//! shadow was filled from Class A and already holds every create. Overflow is
//! **cleared** (not drained/re-inserted) — it was a sidecar of the old head only.
//!
//! Layout: process-local dense table (default 1 M slots × 8 B fk + 32 B key) —
//! bounded RAM for “failed primary insert” volume, not a second full mainnet head.
//! Persisted under `tx.head.overflow` as a simple array of `(key32, fk8)` occupied
//! slots for crash recovery of inserts that landed only in overflow before swap.

use crate::error::StoreError;
use rbitcoin_primitives::Fk;
use std::path::{Path, PathBuf};

/// Default slot count (power of two). ~40 MiB for keys+fk at full occupancy planning.
pub const DEFAULT_OVERFLOW_SLOTS: usize = 1 << 20;

const ENTRY_BYTES: usize = 32 + 8; // key + fk

/// In-memory overflow map (sole insert thread; N readers of published slots).
pub struct HeadOverflow {
    path: PathBuf,
    /// Power-of-two slots.
    slots: usize,
    /// Parallel arrays: key zeroed ⇒ empty.
    keys: Vec<[u8; 32]>,
    fks: Vec<u64>,
    occupied: usize,
}

impl HeadOverflow {
    pub fn create(store_dir: &Path) -> Result<Self, StoreError> {
        Self::create_with_slots(store_dir, DEFAULT_OVERFLOW_SLOTS)
    }

    pub fn create_with_slots(store_dir: &Path, slots: usize) -> Result<Self, StoreError> {
        let slots = slots.max(16).next_power_of_two();
        let path = overflow_path(store_dir);
        let o = Self {
            path,
            slots,
            keys: vec![[0u8; 32]; slots],
            fks: vec![0u64; slots],
            occupied: 0,
        };
        o.persist()?;
        Ok(o)
    }

    pub fn open(store_dir: &Path) -> Result<Self, StoreError> {
        let path = overflow_path(store_dir);
        if !path.exists() {
            return Self::create(store_dir);
        }
        let raw = std::fs::read(&path).map_err(|e| StoreError::io(&path, e))?;
        if raw.len() < 16 {
            return Err(StoreError::Corrupt("tx.head.overflow short header"));
        }
        if &raw[0..4] != b"HOVF" {
            return Err(StoreError::Corrupt("tx.head.overflow magic"));
        }
        let slots = u32::from_le_bytes(raw[4..8].try_into().unwrap()) as usize;
        if !slots.is_power_of_two() || slots < 16 {
            return Err(StoreError::Corrupt("tx.head.overflow slots"));
        }
        let expect = 16 + slots * ENTRY_BYTES;
        if raw.len() < expect {
            return Err(StoreError::Corrupt("tx.head.overflow truncated"));
        }
        let mut keys = vec![[0u8; 32]; slots];
        let mut fks = vec![0u64; slots];
        let mut occupied = 0usize;
        for i in 0..slots {
            let off = 16 + i * ENTRY_BYTES;
            keys[i].copy_from_slice(&raw[off..off + 32]);
            fks[i] = u64::from_le_bytes(raw[off + 32..off + 40].try_into().unwrap());
            if keys[i] != [0u8; 32] {
                occupied += 1;
            }
        }
        Ok(Self {
            path,
            slots,
            keys,
            fks,
            occupied,
        })
    }

    pub fn occupied(&self) -> usize {
        self.occupied
    }

    pub fn slots(&self) -> usize {
        self.slots
    }

    pub fn is_empty(&self) -> bool {
        self.occupied == 0
    }

    /// Insert `key` → `fk`. Idempotent if same fk already present.
    /// Returns `true` if a new mapping was written.
    pub fn insert(&mut self, key: &[u8; 32], fk: Fk) -> Result<bool, StoreError> {
        if fk.is_null() {
            return Err(StoreError::InvalidFk);
        }
        if *key == [0u8; 32] {
            return Err(StoreError::Corrupt("overflow refuses all-zero key"));
        }
        if self.occupied * 8 >= self.slots * 7 {
            return Err(StoreError::Corrupt("tx.head.overflow load too high"));
        }
        let mask = self.slots - 1;
        let mut i = primary(key, self.slots);
        for _ in 0..self.slots {
            if self.keys[i] == [0u8; 32] {
                self.keys[i] = *key;
                self.fks[i] = fk.0;
                self.occupied += 1;
                return Ok(true);
            }
            if self.keys[i] == *key {
                if self.fks[i] == fk.0 {
                    return Ok(false);
                }
                // Same key, different fk — BIP30 depth: keep existing (first insert wins).
                return Ok(false);
            }
            i = (i + 1) & mask;
        }
        Err(StoreError::Corrupt("tx.head.overflow probe exhausted"))
    }

    /// Lookup by mixed key. Empty key never hits.
    pub fn get(&self, key: &[u8; 32]) -> Option<Fk> {
        if *key == [0u8; 32] {
            return None;
        }
        let mask = self.slots - 1;
        let mut i = primary(key, self.slots);
        for _ in 0..self.slots {
            if self.keys[i] == [0u8; 32] {
                return None;
            }
            if self.keys[i] == *key {
                return Some(Fk(self.fks[i]));
            }
            i = (i + 1) & mask;
        }
        None
    }

    /// All occupied `(key, fk)` pairs (for bulk merge into primary after resize).
    pub fn iter_occupied(&self) -> impl Iterator<Item = ([u8; 32], Fk)> + '_ {
        self.keys
            .iter()
            .zip(self.fks.iter())
            .filter(|(k, _)| **k != [0u8; 32])
            .map(|(k, f)| (*k, Fk(*f)))
    }

    /// Persist current table (caller invokes after batches of inserts).
    pub fn persist(&self) -> Result<(), StoreError> {
        let mut raw = Vec::with_capacity(16 + self.slots * ENTRY_BYTES);
        raw.extend_from_slice(b"HOVF");
        raw.extend_from_slice(&(self.slots as u32).to_le_bytes());
        raw.extend_from_slice(&0u32.to_le_bytes()); // reserved
        raw.extend_from_slice(&0u32.to_le_bytes());
        for i in 0..self.slots {
            raw.extend_from_slice(&self.keys[i]);
            raw.extend_from_slice(&self.fks[i].to_le_bytes());
        }
        let tmp = self.path.with_extension("overflow.tmp");
        std::fs::write(&tmp, &raw).map_err(|e| StoreError::io(&tmp, e))?;
        std::fs::rename(&tmp, &self.path).map_err(|e| StoreError::io(&self.path, e))?;
        Ok(())
    }

    /// Drop all entries (after successful merge into primary).
    pub fn clear(&mut self) -> Result<(), StoreError> {
        for k in &mut self.keys {
            *k = [0u8; 32];
        }
        for f in &mut self.fks {
            *f = 0;
        }
        self.occupied = 0;
        self.persist()
    }
}

#[inline]
fn primary(key: &[u8; 32], slots: usize) -> usize {
    let mut h = 0xcbf29ce484222325u64;
    for b in key {
        h ^= u64::from(*b);
        h = h.wrapping_mul(0x100000001b3);
    }
    (h as usize) & (slots - 1)
}

#[inline]
pub fn overflow_path(store_dir: &Path) -> PathBuf {
    store_dir.join("tx.head.overflow")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    fn temp() -> PathBuf {
        static N: AtomicU64 = AtomicU64::new(0);
        let id = N.fetch_add(1, Ordering::Relaxed);
        let d = std::env::temp_dir().join(format!("rbitcoin-hovf-{id}"));
        let _ = std::fs::remove_dir_all(&d);
        std::fs::create_dir_all(&d).unwrap();
        d
    }

    #[test]
    fn insert_get_persist_reopen() {
        let dir = temp();
        let mut o = HeadOverflow::create_with_slots(&dir, 64).unwrap();
        let k = [9u8; 32];
        assert!(o.insert(&k, Fk(42)).unwrap());
        assert!(!o.insert(&k, Fk(42)).unwrap());
        assert_eq!(o.get(&k), Some(Fk(42)));
        o.persist().unwrap();
        let o2 = HeadOverflow::open(&dir).unwrap();
        assert_eq!(o2.get(&k), Some(Fk(42)));
        assert_eq!(o2.occupied(), 1);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn linear_probe_and_miss() {
        let dir = temp();
        let mut o = HeadOverflow::create_with_slots(&dir, 32).unwrap();
        for i in 1..20u64 {
            let mut k = [0u8; 32];
            k[0..8].copy_from_slice(&i.to_le_bytes());
            o.insert(&k, Fk(i)).unwrap();
        }
        let mut miss = [0xffu8; 32];
        miss[0] = 0xab;
        assert!(o.get(&miss).is_none());
        let mut k = [0u8; 32];
        k[0..8].copy_from_slice(&5u64.to_le_bytes());
        assert_eq!(o.get(&k), Some(Fk(5)));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn clear_empties() {
        let dir = temp();
        let mut o = HeadOverflow::create_with_slots(&dir, 32).unwrap();
        let k = [3u8; 32];
        o.insert(&k, Fk(1)).unwrap();
        o.clear().unwrap();
        assert!(o.is_empty());
        assert!(o.get(&k).is_none());
        let _ = std::fs::remove_dir_all(&dir);
    }
}
