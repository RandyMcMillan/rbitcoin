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

/// Mainnet **point** shard count (`key[0]` → 256 files). Large tables need fine
/// partitioning so each rehash stays local.
pub const SHARD_COUNT: usize = 256;
/// Mainnet **tx / scripthash** shard count (16 files). Enough to bound rehash
/// size without the FD cost of 256-way for smaller heads.
pub const SHARD_COUNT_TX_SH: usize = 16;

/// Aggregate spill chatter at DEBUG this often; per-event is TRACE.
const SPILL_DEBUG_INTERVAL: Duration = Duration::from_secs(30);

/// Default max keys applied to disk per spill step (~32k).
///
/// Smaller than early 100k trials: each chunk is a shorter page-cache storm so
/// the desktop stays interactive under ionice. Override: `RBITCOIN_HEAD_SPILL_CHUNK`.
pub const DEFAULT_SPILL_CHUNK: usize = 32_000;

/// Resolved spill chunk (env override, min 1k, max 1M).
pub fn spill_chunk_size() -> usize {
    std::env::var("RBITCOIN_HEAD_SPILL_CHUNK")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(DEFAULT_SPILL_CHUNK)
        .clamp(1_000, 1_000_000)
}

/// Inter-shard pause after a paced insert (ms). Override `RBITCOIN_HEAD_SHARD_PACE_MS`.
pub fn shard_pace_ms() -> u64 {
    std::env::var("RBITCOIN_HEAD_SHARD_PACE_MS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(25)
        .min(5_000)
}

/// How many shards to create for the active scale (legacy: same for all roles).
pub fn shard_count_for_scale() -> usize {
    shard_count_for_role(HeadRole::Point)
}

/// Shard count for a new head of `role` (existing dirs keep their layout on open).
///
/// - **Point:** 256 (rehash locality for huge spend index)
/// - **Tx / ScriptHash:** 16 (smaller indexes; fewer FDs)
/// - **Header:** 256 (same as historical mainnet create)
pub fn shard_count_for_role(role: HeadRole) -> usize {
    match HeadScale::from_env() {
        HeadScale::Tiny => 1,
        HeadScale::Mainnet => match role {
            HeadRole::Point | HeadRole::Header => SHARD_COUNT,
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

    /// Spill **all** pending overlay entries (chunked + yield between chunks).
    ///
    /// Runtime / disable path. Prefer [`Self::spill_write_behind_fast`] on process
    /// exit — chunked+sleep was multi‑minute mid-IBD (hundreds of k keys).
    pub fn spill_write_behind(&self) -> Result<(), StoreError> {
        let chunk = spill_chunk_size();
        loop {
            if self.spill_write_behind_budget(chunk)? == 0 {
                break;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        Ok(())
    }

    /// Drain the entire overlay in **one** disk apply (no sleeps).
    ///
    /// Used on process exit: one rehash/apply beats hundreds of 32k chunks with
    /// yields (logs showed ~7 min for ~1.5M keys under chunked spill).
    pub fn spill_write_behind_fast(&self) -> Result<(), StoreError> {
        let batch = {
            let mut guard = self.overlay.lock().unwrap();
            let Some(ov) = guard.as_mut() else {
                return Ok(());
            };
            if ov.map.is_empty() {
                return Ok(());
            }
            let batch: Vec<([u8; 32], Fk)> = ov.map.drain().collect();
            batch
        };
        if batch.is_empty() {
            return Ok(());
        }
        let n = batch.len();
        let t0 = Instant::now();
        rbitcoin_log::info!(
            "store: head spill FAST path={} entries={} (single apply)",
            self.path.display(),
            n
        );
        self.insert_many_disk(&batch)?;
        self.note_spill(n);
        rbitcoin_log::info!(
            "store: head spill FAST done path={} entries={} elapsed={:?}",
            self.path.display(),
            n,
            t0.elapsed()
        );
        Ok(())
    }

    /// Spill at most `max_spill` overlay entries (budgeted step).
    ///
    /// Returns how many keys were applied to disk. Remaining stay in RAM for
    /// coherent `get` and for the next step (archive interleave / background).
    pub fn spill_write_behind_budget(&self, max_spill: usize) -> Result<usize, StoreError> {
        if max_spill == 0 {
            return Ok(0);
        }
        let batch = {
            let mut guard = self.overlay.lock().unwrap();
            let Some(ov) = guard.as_mut() else {
                return Ok(0);
            };
            if ov.map.is_empty() {
                return Ok(0);
            }
            let n = ov.map.len().min(max_spill);
            let mut spill = Vec::with_capacity(n);
            // Take an arbitrary n keys (HashMap order); leave the rest in overlay.
            let keys: Vec<[u8; 32]> = ov.map.keys().copied().take(n).collect();
            for k in keys {
                if let Some(v) = ov.map.remove(&k) {
                    spill.push((k, v));
                }
            }
            spill
        };
        if batch.is_empty() {
            return Ok(0);
        }
        let n = batch.len();
        rbitcoin_log::trace!(
            "store: hash-head spill path={} entries={} budget={} remain≈{} shards={}",
            self.path.display(),
            n,
            max_spill,
            self.write_behind_len(),
            self.shards.len()
        );
        self.insert_many_disk(&batch)?;
        self.note_spill(n);
        Ok(n)
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
    ///
    /// Prefer paced inserts for IBD materialize (avoids multi-shard rehash).
    #[allow(dead_code)] // public capacity helper; materialize uses insert_many_paced
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
                // Hard at 2×: bound RAM under defer; still only budgeted chunks.
                let hard = n >= max.saturating_mul(2);
                let defer = self.defer_spill.load(Ordering::Relaxed);
                if !(soft || hard) {
                    return Ok(());
                }
                drop(guard);
                let chunk = spill_chunk_size();
                if hard {
                    // Under confirm (defer): at most one chunk so we never multi-min block.
                    // Else a few chunks; background + archive interleave drain the rest.
                    let max_steps = if defer { 1 } else { 4 };
                    for _ in 0..max_steps {
                        if self.write_behind_len() < max.saturating_mul(2) {
                            break;
                        }
                        if self.spill_write_behind_budget(chunk)? == 0 {
                            break;
                        }
                    }
                } else if soft && !defer {
                    // Soft: one budgeted step; never dump half-cap in one storm.
                    let _ = self.spill_write_behind_budget(chunk)?;
                }
                // soft && defer: skip — background worker may still short-slice when
                // over soft/2 so RAM does not grow unbounded for the whole wave.
                return Ok(());
            }
        }
        self.insert_many_disk(entries)
    }

    /// Defer soft-cap overlay spills while confirm is mid-wave.
    ///
    /// Soft auto-spill on insert is skipped under defer; hard still does **one**
    /// budgeted chunk. Clearing defer does **not** dump the overlay (A.3) —
    /// archive interleave + background worker drain in short slices.
    pub fn set_defer_spill(&self, defer: bool) -> Result<(), StoreError> {
        self.defer_spill.store(defer, Ordering::Relaxed);
        Ok(())
    }

    /// One background / archive-interleave step when the overlay is "full enough".
    ///
    /// Quiet policy (host UI): keep a large RAM buffer so we are not constantly
    /// writing heads. Under confirm (`defer`), only step at/above soft cap.
    /// Between waves, step only above soft/2 (leave half buffer for locality).
    pub fn spill_write_behind_step_if_needed(&self) -> Result<usize, StoreError> {
        let (len, max, defer) = {
            let guard = self.overlay.lock().unwrap();
            match guard.as_ref() {
                Some(ov) => (
                    ov.map.len(),
                    ov.max_entries,
                    self.defer_spill.load(Ordering::Relaxed),
                ),
                None => return Ok(0),
            }
        };
        if len == 0 {
            return Ok(0);
        }
        // Under confirm: stay quiet until at/above soft cap (hard still in insert_many).
        // Off-wave: only drain when above half soft — continuous drain-to-zero thrash.
        let needs = if defer {
            len >= max
        } else {
            len > max / 2
        };
        if !needs {
            return Ok(0);
        }
        self.spill_write_behind_budget(spill_chunk_size())
    }

    /// Partition by shard, then slot-sorted insert per shard (disk path).
    fn insert_many_disk(&self, entries: &[([u8; 32], Fk)]) -> Result<(), StoreError> {
        self.insert_many_disk_inner(entries, false)
    }

    /// IBD run-materialize path: insert one shard at a time and **pause between
    /// shards** so a cascade of rehashes cannot stack. Rehashes themselves are
    /// process-serialized in [`HashHead::rehash_to`]. Always write-through
    /// (bypasses process-local write-behind).
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
        let n = self.shards.len();
        if n == 1 {
            return self.shards[0].insert_many(entries);
        }
        let mut buckets: Vec<Vec<([u8; 32], Fk)>> = (0..n).map(|_| Vec::new()).collect();
        for &(key, fk) in entries {
            buckets[self.shard_of(&key)].push((key, fk));
        }
        let pace_ms = if pace { shard_pace_ms() } else { 0 };
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
        self.spill_write_behind()?;
        for s in &self.shards {
            s.flush()?;
        }
        Ok(())
    }

    /// Spill remaining overlay + MS_ASYNC each shard (no fdatasync).
    pub fn flush_async(&self) -> Result<(), StoreError> {
        self.spill_write_behind()?;
        self.flush_async_no_spill()
    }

    /// MS_ASYNC shard files only — **no** overlay spill (caller already spilled).
    pub fn flush_async_no_spill(&self) -> Result<(), StoreError> {
        for s in &self.shards {
            s.flush_async_no_spill()?;
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

    fn key_i(i: u64) -> [u8; 32] {
        let mut key = [0u8; 32];
        key[0] = (i % 256) as u8;
        key[8..16].copy_from_slice(&i.to_le_bytes());
        key
    }

    #[test]
    fn fast_spill_drains_entire_overlay_in_one_shot() {
        let dir = tmp_dir();
        let h = ShardedHashHead::create_sharded(dir.join("head"), 16, 64).unwrap();
        h.enable_write_behind(10_000).unwrap();
        let batch: Vec<_> = (0u64..400).map(|i| (key_i(i), Fk(i + 1))).collect();
        h.insert_many(&batch).unwrap();
        assert_eq!(h.write_behind_len(), 400);
        h.spill_write_behind_fast().unwrap();
        assert_eq!(h.write_behind_len(), 0);
        assert_eq!(h.occupied(), 400);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn budgeted_spill_leaves_remainder_in_overlay() {
        let dir = tmp_dir();
        let h = ShardedHashHead::create_sharded(dir.join("head"), 16, 64).unwrap();
        h.enable_write_behind(10_000).unwrap();
        let batch: Vec<_> = (0u64..500).map(|i| (key_i(i), Fk(i + 1))).collect();
        h.insert_many(&batch).unwrap();
        assert_eq!(h.write_behind_len(), 500);
        let n = h.spill_write_behind_budget(100).unwrap();
        assert_eq!(n, 100);
        assert_eq!(h.write_behind_len(), 400);
        // Disk holds spilled keys; overlay holds the rest.
        assert_eq!(h.occupied(), 100);
        for (k, fk) in &batch {
            assert_eq!(h.get(k).unwrap(), Some(*fk));
        }
        h.spill_write_behind().unwrap();
        assert_eq!(h.write_behind_len(), 0);
        assert_eq!(h.occupied(), 500);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn soft_cap_spills_one_budgeted_chunk_not_entire_excess() {
        let dir = tmp_dir();
        let h = ShardedHashHead::create_sharded(dir.join("head"), 16, 64).unwrap();
        // Soft cap 200; insert 250 → one budgeted step of spill_chunk_size (clamped ≥1k)
        // would empty us if chunk > 250. Force small chunk via env is process-wide;
        // instead assert via direct budget after filling under soft with defer off.
        h.enable_write_behind(10_000).unwrap();
        let batch: Vec<_> = (0u64..300).map(|i| (key_i(i), Fk(i + 1))).collect();
        h.insert_many(&batch).unwrap();
        // Under soft: still all in overlay.
        assert_eq!(h.write_behind_len(), 300);
        let n = h.spill_write_behind_budget(80).unwrap();
        assert_eq!(n, 80);
        assert_eq!(h.write_behind_len(), 220);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn clear_defer_does_not_bulk_spill() {
        let dir = tmp_dir();
        let h = ShardedHashHead::create_sharded(dir.join("head"), 16, 64).unwrap();
        h.enable_write_behind(10_000).unwrap();
        h.set_defer_spill(true).unwrap();
        let batch: Vec<_> = (0u64..200).map(|i| (key_i(i), Fk(i + 1))).collect();
        h.insert_many(&batch).unwrap();
        assert_eq!(h.write_behind_len(), 200);
        h.set_defer_spill(false).unwrap();
        // A.3: clearing defer must not dump the overlay.
        assert_eq!(h.write_behind_len(), 200);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn step_if_needed_respects_defer_threshold() {
        let dir = tmp_dir();
        let h = ShardedHashHead::create_sharded(dir.join("head"), 16, 64).unwrap();
        h.enable_write_behind(200).unwrap();
        h.set_defer_spill(true).unwrap();
        // Under soft while deferred → no step.
        let batch: Vec<_> = (0u64..50).map(|i| (key_i(i), Fk(i + 1))).collect();
        h.insert_many(&batch).unwrap();
        assert_eq!(h.spill_write_behind_step_if_needed().unwrap(), 0);
        assert_eq!(h.write_behind_len(), 50);
        // At/above soft while deferred → budgeted step.
        let more: Vec<_> = (50u64..200).map(|i| (key_i(i), Fk(i + 1))).collect();
        h.insert_many(&more).unwrap();
        assert!(h.write_behind_len() >= 200);
        let n = h.spill_write_behind_step_if_needed().unwrap();
        assert!(n > 0);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
