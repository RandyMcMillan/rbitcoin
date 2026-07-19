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
//!
//! Write-behind overlay (if enabled) lives on this facade; shards stay write-through.

use crate::error::StoreError;
use crate::hashhead::{initial_slots_for, HeadRole, HeadScale, HashHead};
use rbitcoin_primitives::Fk;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Mutex;
use std::time::{Duration, Instant};

/// Mainnet / full-validation shard count (`key[0]`).
pub const SHARD_COUNT: usize = 256;

/// Aggregate spill chatter at DEBUG this often; per-event is TRACE.
const SPILL_DEBUG_INTERVAL: Duration = Duration::from_secs(30);

/// How many shards to create for the active scale.
pub fn shard_count_for_scale() -> usize {
    match HeadScale::from_env() {
        // Single file keeps unit/integration tests light.
        HeadScale::Tiny => 1,
        HeadScale::Mainnet => SHARD_COUNT,
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
        // Moderate per-shard start: grows with occupancy; avoids empty multi‑GiB tables.
        // Header: 2¹² × 256 ≈ 1M total slots. Point/tx/sh: 2¹⁶ × 40 B ≈ 2.5 MiB/shard
        // (×256 ≈ 640 MiB total sparse at create).
        HeadScale::Mainnet => match role {
            HeadRole::Header => 1 << 12,
            HeadRole::ScriptHash | HeadRole::Point | HeadRole::Tx => 1 << 16,
        },
    }
}

pub struct ShardedHashHead {
    shards: Vec<HashHead>,
    path: PathBuf,
    overlay: Mutex<Option<WriteBehind>>,
    spill_stats: Mutex<SpillStats>,
    /// When true, soft-cap spill is deferred (confirm connect prefers RAM overlay).
    /// Hard cap still partial-spills at 2× max_entries to bound RAM (smaller dumps).
    defer_spill: AtomicBool,
}

struct WriteBehind {
    map: HashMap<[u8; 32], Fk>,
    max_entries: usize,
}

struct SpillStats {
    events: u64,
    entries: u64,
    window_start: Instant,
}

impl SpillStats {
    fn new() -> Self {
        Self {
            events: 0,
            entries: 0,
            window_start: Instant::now(),
        }
    }
}

impl ShardedHashHead {
    pub fn create_for_role(
        path: impl Into<PathBuf>,
        role: HeadRole,
    ) -> Result<Self, StoreError> {
        Self::create_sharded(path, shard_count_for_scale(), initial_slots_per_shard(role))
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
            return Ok(Self {
                shards: vec![h],
                path,
                overlay: Mutex::new(None),
                spill_stats: Mutex::new(SpillStats::new()),
                defer_spill: AtomicBool::new(false),
            });
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
            "store: sharded hash-head create path={} shards={} slots_each={} (~{:.2} GiB total sparse)",
            path.display(),
            n,
            per,
            (n as u64 * per * 40) as f64 / (1024.0 * 1024.0 * 1024.0)
        );
        Ok(Self {
            shards,
            path,
            overlay: Mutex::new(None),
            spill_stats: Mutex::new(SpillStats::new()),
            defer_spill: AtomicBool::new(false),
        })
    }

    /// Open a legacy single-file head **or** a sharded directory. Does **not**
    /// force-grow to a mainnet floor (that blew empty tables to multi‑GiB).
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
                .collect();
            names.sort();
            if names.is_empty() {
                return Err(StoreError::Corrupt("sharded hash-head empty directory"));
            }
            // Expect contiguous hex shard names (00.. or 00..ff).
            let mut shards = Vec::with_capacity(names.len());
            for (i, name) in names.iter().enumerate() {
                let expect = format!("{i:02x}");
                if name != &expect && name != &format!("{i:x}") {
                    // Allow only zero-padded 2-digit hex in order.
                    if name != &format!("{i:02x}") {
                        return Err(StoreError::Corrupt(
                            "sharded hash-head unexpected shard name",
                        ));
                    }
                }
                shards.push(HashHead::open(path.join(name))?);
            }
            return Ok(Self {
                shards,
                path,
                overlay: Mutex::new(None),
                spill_stats: Mutex::new(SpillStats::new()),
                defer_spill: AtomicBool::new(false),
            });
        }
        if path.is_file() {
            let h = HashHead::open(&path)?;
            return Ok(Self {
                shards: vec![h],
                path,
                overlay: Mutex::new(None),
                spill_stats: Mutex::new(SpillStats::new()),
                defer_spill: AtomicBool::new(false),
            });
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
            // Production uses n=256 so this is key[0]; tests may use fewer shards.
            (key[0] as usize) % n
        }
    }

    #[allow(dead_code)] // diagnostics / tests
    pub fn shard_count(&self) -> usize {
        self.shards.len()
    }

    pub fn occupied(&self) -> u64 {
        self.shards.iter().map(|s| s.occupied()).sum()
    }

    #[allow(dead_code)] // diagnostics / tests
    pub fn write_behind_len(&self) -> usize {
        self.overlay
            .lock()
            .unwrap()
            .as_ref()
            .map(|o| o.map.len())
            .unwrap_or(0)
    }

    pub fn enable_write_behind(&self, max_entries: usize) -> Result<(), StoreError> {
        let max_entries = max_entries.max(1);
        self.spill_write_behind()?;
        *self.overlay.lock().unwrap() = Some(WriteBehind {
            map: HashMap::new(),
            max_entries,
        });
        Ok(())
    }

    pub fn disable_write_behind(&self) -> Result<(), StoreError> {
        self.spill_write_behind()?;
        *self.overlay.lock().unwrap() = None;
        Ok(())
    }

    /// Spill **all** pending overlay entries to disk (flush / disable).
    pub fn spill_write_behind(&self) -> Result<(), StoreError> {
        self.spill_write_behind_down_to(0)
    }

    /// Spill until overlay holds at most `keep` entries (partial / cadence-friendly).
    ///
    /// Used for soft/hard caps so we never dump multi‑million key overlays in one
    /// storm; smaller more frequent spills keep confirm page cache warmer.
    pub fn spill_write_behind_down_to(&self, keep: usize) -> Result<(), StoreError> {
        let batch = {
            let mut guard = self.overlay.lock().unwrap();
            let Some(ov) = guard.as_mut() else {
                return Ok(());
            };
            let n = ov.map.len();
            if n <= keep {
                return Ok(());
            }
            let n_spill = n - keep;
            // Drain all then re-insert keep: HashMap has no stable "take first N".
            let mut all: Vec<([u8; 32], Fk)> = ov.map.drain().collect();
            // Spill the head of the vec; keep the tail (recent inserts more likely
            // still useful for probes during the same confirm wave).
            let spill_end = n_spill.min(all.len());
            let spill: Vec<_> = all.drain(..spill_end).collect();
            for (k, v) in all {
                ov.map.insert(k, v);
            }
            spill
        };
        if batch.is_empty() {
            return Ok(());
        }
        let n = batch.len();
        rbitcoin_log::trace!(
            "store: hash-head spill path={} entries={} keep_target={} shards={}",
            self.path.display(),
            n,
            keep,
            self.shards.len()
        );
        self.insert_many_disk(&batch)?;
        self.note_spill(n);
        Ok(())
    }

    fn note_spill(&self, entries: usize) {
        let mut s = self.spill_stats.lock().unwrap();
        s.events = s.events.saturating_add(1);
        s.entries = s.entries.saturating_add(entries as u64);
        if s.window_start.elapsed() < SPILL_DEBUG_INTERVAL {
            return;
        }
        if s.events == 0 {
            s.window_start = Instant::now();
            return;
        }
        rbitcoin_log::debug!(
            "store: hash-head spill summary path={} events={} entries={} window={:?}",
            self.path.display(),
            s.events,
            s.entries,
            s.window_start.elapsed()
        );
        s.events = 0;
        s.entries = 0;
        s.window_start = Instant::now();
    }

    /// Reserve roughly `additional` new keys, spread across shards.
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

    pub fn get(&self, key: &[u8; 32]) -> Result<Option<Fk>, StoreError> {
        {
            let guard = self.overlay.lock().unwrap();
            if let Some(ov) = guard.as_ref() {
                if let Some(&fk) = ov.map.get(key) {
                    return Ok(Some(fk));
                }
            }
        }
        self.shards[self.shard_of(key)].get(key)
    }

    pub fn insert(&self, key: &[u8; 32], fk: Fk) -> Result<Option<Fk>, StoreError> {
        debug_assert!(!fk.is_null());
        let prev = self.get(key)?;
        self.insert_many(&[(*key, fk)])?;
        Ok(prev)
    }

    pub fn insert_many(&self, entries: &[([u8; 32], Fk)]) -> Result<(), StoreError> {
        if entries.is_empty() {
            return Ok(());
        }
        {
            let mut guard = self.overlay.lock().unwrap();
            if let Some(ov) = guard.as_mut() {
                for (key, fk) in entries {
                    debug_assert!(!fk.is_null());
                    ov.map.insert(*key, *fk);
                }
                let max = ov.max_entries;
                let n = ov.map.len();
                let soft = n >= max;
                // Hard at 2× (was 4×): smaller storms; still bounds RAM under defer.
                let hard = n >= max.saturating_mul(2);
                let defer = self.defer_spill.load(Ordering::Relaxed);
                // Partial keep: leave half cap in RAM for probe locality / next batch.
                let keep = max / 2;
                if (soft && !defer) || hard {
                    drop(guard);
                    self.spill_write_behind_down_to(keep)?;
                }
                return Ok(());
            }
        }
        self.insert_many_disk(entries)
    }

    /// Defer soft-cap overlay spills (confirm connect wants RAM-coherent probes).
    ///
    /// Clearing defer triggers a **partial** spill down to half soft-cap (not a full
    /// multi‑million dump between every confirm wave).
    pub fn set_defer_spill(&self, defer: bool) -> Result<(), StoreError> {
        self.defer_spill.store(defer, Ordering::Relaxed);
        if !defer {
            let keep = {
                let guard = self.overlay.lock().unwrap();
                guard.as_ref().map(|o| o.max_entries / 2).unwrap_or(0)
            };
            self.spill_write_behind_down_to(keep)?;
        }
        Ok(())
    }

    /// Partition by shard, then slot-sorted insert per shard (disk path).
    fn insert_many_disk(&self, entries: &[([u8; 32], Fk)]) -> Result<(), StoreError> {
        let n = self.shards.len();
        if n == 1 {
            return self.shards[0].insert_many(entries);
        }
        let mut buckets: Vec<Vec<([u8; 32], Fk)>> = (0..n).map(|_| Vec::new()).collect();
        for &(key, fk) in entries {
            buckets[self.shard_of(&key)].push((key, fk));
        }
        for (i, bucket) in buckets.into_iter().enumerate() {
            if !bucket.is_empty() {
                self.shards[i].insert_many(&bucket)?;
            }
        }
        Ok(())
    }

    pub fn flush(&self) -> Result<(), StoreError> {
        self.spill_write_behind()?;
        for s in &self.shards {
            s.flush()?;
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
        // 16 shards keeps the test light while exercising partition logic.
        let h = ShardedHashHead::create_sharded(&path, 16, 64).unwrap();
        assert_eq!(h.shard_count(), 16);
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
        h.flush().unwrap();
        drop(h);

        // open_for_role expects 256 shards for a directory — write a full 256 set for open test.
        let dir2 = tmp_dir();
        let path2 = dir2.join("head256");
        let h_full = ShardedHashHead::create_sharded(&path2, SHARD_COUNT, 64).unwrap();
        h_full.insert_many(&batch).unwrap();
        h_full.flush().unwrap();
        drop(h_full);
        let h2 = ShardedHashHead::open_for_role(&path2, HeadRole::Point).unwrap();
        assert_eq!(h2.shard_count(), SHARD_COUNT);
        assert_eq!(h2.occupied(), 1000);
        assert_eq!(h2.get(&batch[0].0).unwrap(), Some(batch[0].1));
        let _ = std::fs::remove_dir_all(&dir);
        let _ = std::fs::remove_dir_all(&dir2);
    }

    #[test]
    fn legacy_single_file_still_opens() {
        let path = tmp_dir().join("legacy.head");
        let _ = std::fs::create_dir_all(path.parent().unwrap());
        {
            let h = HashHead::create_with_slots(&path, 64).unwrap();
            h.insert(&[9u8; 32], Fk(42)).unwrap();
            h.flush().unwrap();
        }
        let h = ShardedHashHead::open_for_role(&path, HeadRole::Tx).unwrap();
        assert_eq!(h.shard_count(), 1);
        assert_eq!(h.get(&[9u8; 32]).unwrap(), Some(Fk(42)));
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn write_behind_spill_groups_by_shard() {
        let dir = tmp_dir();
        let path = dir.join("head");
        let h = ShardedHashHead::create_sharded(&path, 16, 64).unwrap();
        h.enable_write_behind(10_000).unwrap();
        let mut batch = Vec::new();
        for i in 0u64..500 {
            let mut key = [0u8; 32];
            key[0] = (i % 256) as u8;
            key[8..16].copy_from_slice(&i.to_le_bytes());
            batch.push((key, Fk(i + 1)));
        }
        h.insert_many(&batch).unwrap();
        assert!(h.write_behind_len() > 0);
        h.spill_write_behind().unwrap();
        assert_eq!(h.write_behind_len(), 0);
        assert_eq!(h.occupied(), 500);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
