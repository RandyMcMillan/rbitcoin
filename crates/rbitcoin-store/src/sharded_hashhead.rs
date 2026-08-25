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
use crate::hashhead::{initial_slots_for, HashHead, HeadRole, HeadScale};
use rbitcoin_primitives::Fk;
use std::path::PathBuf;

/// Historical 256-way header layout (new headers are single-file; constant kept
/// for docs / env notes). Not used for new creates.
pub const SHARD_COUNT: usize = 256;
/// Mainnet **scripthash** shard count (64 files; ~0.5–1 GiB OA image per shard at tip).
pub const SHARD_COUNT_TX_SH: usize = 64;
/// Alias for docs / call sites that mean SH specifically.
pub const SHARD_COUNT_SCRIPTHASH: usize = SHARD_COUNT_TX_SH;

/// How many shards to create for the active scale (legacy helper; uses header layout).
pub fn shard_count_for_scale() -> usize {
    shard_count_for_role(HeadRole::Header)
}

/// Shard count for a new head of `role` (existing dirs keep their layout on open).
///
/// - **Header:** **1** (single file; ~1 M headers ever — no need for 256-way)
/// - **ScriptHash:** **64** (cold live OA image ~0.5–1 GiB/shard on mainnet)
pub fn shard_count_for_role(role: HeadRole) -> usize {
    match HeadScale::from_env() {
        HeadScale::Tiny => 1,
        HeadScale::Mainnet => match role {
            HeadRole::Header => 1,
            HeadRole::ScriptHash => SHARD_COUNT_TX_SH,
        },
    }
}

/// Initial slots **per shard** (not global). For header, shard count is 1 so this
/// is the full table size (~1 M slots × 24 B ≈ 24 MiB).
///
/// Override with `RBITCOIN_HEAD_SLOTS_*` as **per-shard** slot count when set.
pub fn initial_slots_per_shard(role: HeadRole) -> u64 {
    if initial_slots_for(role) != HeadScale::from_env().initial_slots(role) {
        return initial_slots_for(role).max(2).next_power_of_two();
    }
    match HeadScale::from_env() {
        HeadScale::Tiny => 64,
        HeadScale::Mainnet => match role {
            // Single-file: enough for full mainnet headers at 7/8 load.
            HeadRole::Header => 1 << 20,
            HeadRole::ScriptHash => 1 << 16,
        },
    }
}

pub struct ShardedHashHead {
    shards: Vec<HashHead>,
}

impl ShardedHashHead {
    pub fn create_for_role(path: impl Into<PathBuf>, role: HeadRole) -> Result<Self, StoreError> {
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
    pub fn open_for_role(path: impl Into<PathBuf>, _role: HeadRole) -> Result<Self, StoreError> {
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

    /// All Class A fks for this key's 16-byte prefix (sole or multi-list).
    pub fn get_all(&self, key: &[u8; 32]) -> Result<Vec<Fk>, StoreError> {
        self.shards[self.shard_of(key)].get_all(key)
    }

    pub fn get(&self, key: &[u8; 32]) -> Result<Option<Fk>, StoreError> {
        self.shards[self.shard_of(key)].get(key)
    }

    pub fn insert(&self, key: &[u8; 32], fk: Fk) -> Result<Option<Fk>, StoreError> {
        // Single-shard path uses HashHead::insert (prev + insert_many_with).
        // Multi-shard: get + insert_many keeps shard batching correct.
        if self.shards.len() == 1 {
            return self.shards[0].insert(key, fk);
        }
        debug_assert!(!fk.is_null());
        let prev = self.get(key)?;
        self.insert_many(&[(*key, fk)])?;
        Ok(prev)
    }

    /// Sum of occupied slots across shards (load observer / sizes).
    #[cfg(test)]
    #[inline]
    pub fn occupied(&self) -> u64 {
        self.shards.iter().map(|s| s.occupied()).sum()
    }

    pub fn insert_many(&self, entries: &[([u8; 32], Fk)]) -> Result<(), StoreError> {
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
        for (i, bucket) in buckets.into_iter().enumerate() {
            if bucket.is_empty() {
                continue;
            }
            self.shards[i].insert_many(&bucket)?;
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
        let h = ShardedHashHead::create_sharded(&path, 16, 256).unwrap();
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
        let h = ShardedHashHead::open_for_role(&path, HeadRole::Header).unwrap();
        assert_eq!(h.shards.len(), 1);
        assert_eq!(h.get(&[9u8; 32]).unwrap(), Some(Fk(42)));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn open_for_role_roundtrip_sharded() {
        let dir = tmp_dir();
        let path = dir.join("head");
        let h = ShardedHashHead::create_sharded(&path, 16, 256).unwrap();
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

    #[test]
    fn shard_helpers_and_error_paths() {
        // Under cfg(test) / cargo-test binary default scale is Tiny → 1 shard.
        assert_eq!(shard_count_for_scale(), 1);
        assert_eq!(shard_count_for_role(HeadRole::Header), 1);
        assert_eq!(shard_count_for_role(HeadRole::ScriptHash), 1);
        assert_eq!(initial_slots_per_shard(HeadRole::Header), 64);

        let dir = tmp_dir();
        std::fs::create_dir_all(&dir).unwrap();
        // create_sharded n==1 uses single file.
        let path = dir.join("one");
        let h = ShardedHashHead::create_sharded(&path, 1, 32).unwrap();
        assert_eq!(h.shards.len(), 1);
        assert!(path.is_file());
        h.insert_many(&[]).unwrap();
        h.flush().unwrap();
        h.flush_async().unwrap();
        drop(h);

        // create_sharded path exists
        let path2 = dir.join("exists");
        std::fs::create_dir_all(&path2).unwrap();
        assert!(ShardedHashHead::create_sharded(&path2, 4, 64).is_err());

        // empty dir open
        let empty = dir.join("empty");
        std::fs::create_dir_all(&empty).unwrap();
        assert!(matches!(
            ShardedHashHead::open_for_role(&empty, HeadRole::Header),
            Err(StoreError::Corrupt(_))
        ));

        // missing path
        let missing = dir.join("nope");
        assert!(ShardedHashHead::open_for_role(&missing, HeadRole::Header).is_err());

        // Unexpected shard file name (not sequential hex).
        let bad_names = dir.join("bad-names");
        std::fs::create_dir_all(&bad_names).unwrap();
        // Create a valid shard 00 then a non-sequential name "ff" when only one expected order.
        {
            let _ = HashHead::create_with_slots(bad_names.join("00"), 32).unwrap();
            let _ = HashHead::create_with_slots(bad_names.join("ff"), 32).unwrap();
        }
        // Sorted names: "00", "ff" — index 1 expects "01"/"1", not "ff" → Corrupt.
        assert!(matches!(
            ShardedHashHead::open_for_role(&bad_names, HeadRole::Header),
            Err(StoreError::Corrupt(_))
        ));

        // RBITCOIN_HEAD_SLOTS_* override → initial_slots_per_shard power-of-two path.
        std::env::set_var("RBITCOIN_HEAD_SLOTS_HEADER", "100");
        let slots = initial_slots_per_shard(HeadRole::Header);
        assert_eq!(slots, 128); // next_power_of_two(100.max(2))
        std::env::remove_var("RBITCOIN_HEAD_SLOTS_HEADER");

        // create_for_role under tiny (single-file header head)
        let role_path = dir.join("role");
        let h = ShardedHashHead::create_for_role(&role_path, HeadRole::Header).unwrap();
        assert_eq!(h.shards.len(), 1);
        assert_eq!(h.occupied(), 0);
        h.insert(&[1u8; 32], Fk(7)).unwrap();
        assert!(h.occupied() >= 1);
        // single-shard insert_many path
        h.insert_many(&[([2u8; 32], Fk(8))]).unwrap();
        assert!(h.occupied() >= 2);

        HeadScale::test_with(HeadScale::Mainnet, || {
            assert_eq!(shard_count_for_role(HeadRole::Header), 1);
            assert_eq!(
                shard_count_for_role(HeadRole::ScriptHash),
                SHARD_COUNT_TX_SH
            );
            assert_eq!(initial_slots_per_shard(HeadRole::Header), 1 << 20);
            assert_eq!(initial_slots_per_shard(HeadRole::ScriptHash), 1 << 16);
        });
        let _ = std::fs::remove_dir_all(&dir);
    }
}
