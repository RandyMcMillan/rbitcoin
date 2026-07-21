//! Partitioned hash head: key → first record fk, sharded by `key[0]`.
//!
//! **Why shard:** a single multi‑GiB open-addressed table is hostile once it
//! exceeds page cache (random probe thrash on 16 GiB hosts). Each insert only
//! touches one shard file (~1/256 of total), rehashes stay local, and mega-batch
//! `insert_many` groups by shard so disk RMW is sequential within each file.
//!
//! **Layout**
//! - New creates: directory `name/` with shards `00`…`ff` (256 files).
//! - Legacy: single file `name` (one logical shard) still opens.

use crate::error::StoreError;
use crate::hashhead::{initial_slots_for, HeadRole, HeadScale, HashHead};
use rbitcoin_primitives::Fk;
use std::path::PathBuf;
use std::time::Duration;

/// Mainnet **header** shard count (`key[0]` → 256 files). Fine partitioning keeps
/// each rehash local on large open-address tables.
pub const SHARD_COUNT: usize = 256;
/// Mainnet **tx / scripthash** shard count (16 files). Enough to bound rehash
/// size without the FD cost of 256-way for smaller heads.
pub const SHARD_COUNT_TX_SH: usize = 16;

/// Inter-shard pause after a paced insert (ms). Override `RBITCOIN_HEAD_SHARD_PACE_MS`.
pub fn shard_pace_ms() -> u64 {
    std::env::var("RBITCOIN_HEAD_SHARD_PACE_MS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(25)
        .min(5_000)
}

/// How many shards to create for the active scale (legacy helper; uses header layout).
pub fn shard_count_for_scale() -> usize {
    shard_count_for_role(HeadRole::Header)
}

/// Shard count for a new head of `role` (existing dirs keep their layout on open).
///
/// - **Header:** 256 (rehash locality for large open-address tables)
/// - **Tx / ScriptHash:** 16 (smaller indexes; fewer FDs)
pub fn shard_count_for_role(role: HeadRole) -> usize {
    match HeadScale::from_env() {
        HeadScale::Tiny => 1,
        HeadScale::Mainnet => match role {
            HeadRole::Header => SHARD_COUNT,
            HeadRole::Tx | HeadRole::ScriptHash => SHARD_COUNT_TX_SH,
        },
    }
}

/// Initial slots **per shard** (not global).
///
/// Override with `RBITCOIN_HEAD_SLOTS_*` as **per-shard** slot count when set.
pub fn initial_slots_per_shard(role: HeadRole) -> u64 {
    // Explicit env = per-shard slots (power of two).
    if initial_slots_for(role) != HeadScale::from_env().initial_slots(role) {
        return initial_slots_for(role).max(2).next_power_of_two();
    }
    match HeadScale::from_env() {
        HeadScale::Tiny => 64,
        HeadScale::Mainnet => match role {
            HeadRole::Header => 1 << 12,
            HeadRole::ScriptHash | HeadRole::Tx => 1 << 16,
        },
    }
}

pub struct ShardedHashHead {
    shards: Vec<HashHead>,
}

impl ShardedHashHead {
    pub fn create_for_role(
        path: impl Into<PathBuf>,
        role: HeadRole,
    ) -> Result<Self, StoreError> {
        Self::create_sharded(
            path,
            shard_count_for_role(role),
            initial_slots_per_shard(role),
        )
    }

    /// Create with explicit shard count and per-shard slot size (tests / tooling).
    pub fn create_sharded(
        path: impl Into<PathBuf>,
        shard_count: usize,
        slots_each: u64,
    ) -> Result<Self, StoreError> {
        let path = path.into();
        let n = shard_count.max(1);
        let per = slots_each.max(2).next_power_of_two();
        if n == 1 {
            let h = HashHead::create_with_slots(&path, per)?;
            return Ok(Self { shards: vec![h] });
        }
        if path.exists() {
            return Err(StoreError::io(
                &path,
                std::io::Error::new(
                    std::io::ErrorKind::AlreadyExists,
                    "sharded hash-head path exists",
                ),
            ));
        }
        std::fs::create_dir_all(&path).map_err(|e| StoreError::io(&path, e))?;
        let mut shards = Vec::with_capacity(n);
        for i in 0..n {
            let shard_path = path.join(format!("{i:02x}"));
            shards.push(HashHead::create_with_slots(shard_path, per)?);
        }
        rbitcoin_log::trace!(
            "store: sharded hash-head create path={} shards={} slots_each={} (~{:.2} GiB total sparse)",
            path.display(),
            n,
            per,
            (n as u64 * per * 24) as f64 / (1024.0 * 1024.0 * 1024.0)
        );
        Ok(Self { shards })
    }

    /// Open a legacy single-file head **or** a sharded directory.
    pub fn open_for_role(
        path: impl Into<PathBuf>,
        _role: HeadRole,
    ) -> Result<Self, StoreError> {
        let path = path.into();
        if path.is_dir() {
            let mut names: Vec<String> = std::fs::read_dir(&path)
                .map_err(|e| StoreError::io(&path, e))?
                .filter_map(|e| e.ok())
                .filter(|e| e.file_type().map(|t| t.is_file()).unwrap_or(false))
                .map(|e| e.file_name().to_string_lossy().into_owned())
                // Shard files are `00`…`ff`; ignore multi-list siblings (`00.mlt`).
                .filter(|name| {
                    name.len() <= 2
                        && !name.is_empty()
                        && name.chars().all(|c| c.is_ascii_hexdigit())
                })
                .collect();
            names.sort();
            if names.is_empty() {
                return Err(StoreError::Corrupt("sharded hash-head empty directory"));
            }
            let mut shards = Vec::with_capacity(names.len());
            for (i, name) in names.iter().enumerate() {
                let expect = format!("{i:02x}");
                if name != &expect && name != &format!("{i:x}") {
                    if name != &format!("{i:02x}") {
                        return Err(StoreError::Corrupt(
                            "sharded hash-head unexpected shard name",
                        ));
                    }
                }
                shards.push(HashHead::open(path.join(name))?);
            }
            return Ok(Self { shards });
        }
        if path.is_file() {
            let h = HashHead::open(&path)?;
            return Ok(Self { shards: vec![h] });
        }
        Err(StoreError::io(
            &path,
            std::io::Error::new(std::io::ErrorKind::NotFound, "hash-head path missing"),
        ))
    }

    #[inline]
    fn shard_of(&self, key: &[u8; 32]) -> usize {
        let n = self.shards.len();
        if n == 1 {
            0
        } else {
            (key[0] as usize) % n
        }
    }

    /// Occupied slots across shards (header diagnostics / tests).
    #[allow(dead_code)]
    pub fn occupied(&self) -> u64 {
        self.shards.iter().map(|s| s.occupied()).sum()
    }

    /// Pre-size every shard for roughly `additional` new keys (spread evenly).
    #[allow(dead_code)]
    pub fn reserve_additional(&self, additional: u64) -> Result<(), StoreError> {
        if additional == 0 {
            return Ok(());
        }
        let n = self.shards.len() as u64;
        let per = additional.div_ceil(n).max(1);
        for s in &self.shards {
            s.reserve_additional(per)?;
        }
        Ok(())
    }

    /// All Class A fks for this key's 16-byte prefix (sole or multi-list).
    pub fn get_all(&self, key: &[u8; 32]) -> Result<Vec<Fk>, StoreError> {
        self.shards[self.shard_of(key)].get_all(key)
    }

    pub fn get(&self, key: &[u8; 32]) -> Result<Option<Fk>, StoreError> {
        self.shards[self.shard_of(key)].get(key)
    }

    pub fn insert(&self, key: &[u8; 32], fk: Fk) -> Result<Option<Fk>, StoreError> {
        debug_assert!(!fk.is_null());
        let prev = self.get(key)?;
        self.insert_many(&[(*key, fk)])?;
        Ok(prev)
    }

    pub fn insert_many(&self, entries: &[([u8; 32], Fk)]) -> Result<(), StoreError> {
        self.insert_many_disk_inner(entries, false)
    }

    /// IBD run-materialize path: insert one shard at a time and **pause between
    /// shards** when pacing is enabled so rehashes do not stack.
    ///
    /// Still used for header bulk paths / tests; tx head no longer calls this.
    #[allow(dead_code)]
    pub fn insert_many_paced(&self, entries: &[([u8; 32], Fk)]) -> Result<(), StoreError> {
        if entries.is_empty() {
            return Ok(());
        }
        self.insert_many_disk_inner(entries, true)
    }

    fn insert_many_disk_inner(
        &self,
        entries: &[([u8; 32], Fk)],
        pace: bool,
    ) -> Result<(), StoreError> {
        if entries.is_empty() {
            return Ok(());
        }
        let n = self.shards.len();
        if n == 1 {
            return self.shards[0].insert_many(entries);
        }
        let mut buckets: Vec<Vec<([u8; 32], Fk)>> = (0..n).map(|_| Vec::new()).collect();
        for &(key, fk) in entries {
            buckets[self.shard_of(&key)].push((key, fk));
        }
        let pace_ms = if pace && crate::ibd_io_policy::shard_pace_enabled() {
            shard_pace_ms()
        } else {
            0
        };
        let mut any = false;
        for (i, bucket) in buckets.into_iter().enumerate() {
            if bucket.is_empty() {
                continue;
            }
            if pace && any && pace_ms > 0 {
                std::thread::sleep(Duration::from_millis(pace_ms));
            }
            self.shards[i].insert_many(&bucket)?;
            any = true;
        }
        Ok(())
    }

    pub fn flush(&self) -> Result<(), StoreError> {
        for s in &self.shards {
            s.flush()?;
        }
        Ok(())
    }

    pub fn flush_async(&self) -> Result<(), StoreError> {
        for s in &self.shards {
            s.flush_async()?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rbitcoin_primitives::Fk;

    fn tmp_dir() -> PathBuf {
        let p = std::env::temp_dir().join(format!(
            "rbitcoin-sharded-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let _ = std::fs::remove_dir_all(&p);
        p
    }

    #[test]
    fn sharded_roundtrip_and_partition() {
        let dir = tmp_dir();
        let path = dir.join("head");
        let h = ShardedHashHead::create_sharded(&path, 16, 64).unwrap();
        assert_eq!(h.shards.len(), 16);
        assert!(path.is_dir());

        let mut batch = Vec::new();
        for i in 0u64..1000 {
            let mut key = [0u8; 32];
            key[0] = (i % 256) as u8;
            key[1..9].copy_from_slice(&i.to_le_bytes());
            batch.push((key, Fk(i + 1)));
        }
        h.insert_many(&batch).unwrap();
        for (key, fk) in &batch {
            assert_eq!(h.get(key).unwrap(), Some(*fk));
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn legacy_single_file_still_opens() {
        let dir = tmp_dir();
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("head");
        {
            let h = HashHead::create_with_slots(&path, 64).unwrap();
            h.insert(&[9u8; 32], Fk(42)).unwrap();
            h.flush().unwrap();
        }
        let h = ShardedHashHead::open_for_role(&path, HeadRole::Tx).unwrap();
        assert_eq!(h.shards.len(), 1);
        assert_eq!(h.get(&[9u8; 32]).unwrap(), Some(Fk(42)));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn open_for_role_roundtrip_sharded() {
        let dir = tmp_dir();
        let path = dir.join("head");
        let h = ShardedHashHead::create_sharded(&path, 16, 64).unwrap();
        let mut batch = Vec::new();
        for i in 0u64..200 {
            let mut key = [0u8; 32];
            key[0] = (i % 256) as u8;
            key[1..9].copy_from_slice(&i.to_le_bytes());
            batch.push((key, Fk(i + 1)));
        }
        h.insert_many(&batch).unwrap();
        h.flush().unwrap();
        drop(h);
        let h2 = ShardedHashHead::open_for_role(&path, HeadRole::Header).unwrap();
        assert_eq!(h2.get(&batch[0].0).unwrap(), Some(batch[0].1));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn insert_many_paced_matches() {
        let dir = tmp_dir();
        let h = ShardedHashHead::create_sharded(dir.join("head"), 16, 64).unwrap();
        let mut batch = Vec::new();
        for i in 0u64..500 {
            let mut key = [0u8; 32];
            key[0] = (i % 256) as u8;
            key[1..9].copy_from_slice(&(i.wrapping_mul(0x9e3779b97f4a7c15)).to_le_bytes());
            batch.push((key, Fk(i + 1)));
        }
        h.insert_many_paced(&batch).unwrap();
        for (k, fk) in &batch {
            assert_eq!(h.get(k).unwrap(), Some(*fk));
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn get_all_prefix_collision() {
        let dir = tmp_dir();
        let h = ShardedHashHead::create_sharded(dir.join("head"), 16, 64).unwrap();
        let mut k1 = [0u8; 32];
        k1[0] = 1;
        k1[15] = 1;
        let mut k2 = k1;
        k2[16] = 9;
        h.insert(&k1, Fk(10)).unwrap();
        h.insert(&k2, Fk(20)).unwrap();
        let all = h.get_all(&k1).unwrap();
        assert!(all.contains(&Fk(10)) && all.contains(&Fk(20)));
        let _ = std::fs::remove_dir_all(&dir);
    }

}
