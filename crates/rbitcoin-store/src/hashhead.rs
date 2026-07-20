//! Growable hash head: key (32 bytes) → first record fk (u64).
//!
//! Linear probing over a power-of-two slot table. Rehashes (doubles slots) when
//! load factor exceeds [`MAX_LOAD_NUM`]/[`MAX_LOAD_DEN`].
//!
//! **File-backed probes:** slots live in the mmap table file. We do **not** keep a
//! full process `Vec` copy of multi-GB heads (signet: `point.head`/`tx.head` were
//! 5–6 GiB each, doubling RSS and blowing swap). Occupied keys are collected into
//! a compact `Vec` only during rehash.
//!
//! **IBD write path:**
//! - [`HashHead::insert_many`] sorts by primary slot and applies RMW through a
//!   small page cache so probes within a batch hit sequential mmap pages.
//! - Optional **write-behind overlay** (`enable_write_behind`) absorbs upserts in
//!   a process-local map and spills sorted batches when the cap is hit or on
//!   [`flush`] / rehash — cutting continuous random head IO during full-validation
//!   IBD while keeping `get` coherent.

use crate::error::StoreError;
use crate::file::{TableFile, FILE_HEADER_LEN};
use rbitcoin_primitives::{Fk, TableKind};
use std::collections::{BTreeMap, HashMap};
use std::sync::Mutex;
use std::time::{Duration, Instant};

const SLOT_SIZE: usize = 40; // 32 key + 8 fk
const DEFAULT_SLOTS: u64 = 64;
/// Rehash when occupied/slots ≥ 7/8.
const MAX_LOAD_NUM: u64 = 7;
const MAX_LOAD_DEN: u64 = 8;
/// Slots per write-behind chunk (128 × 40 B = 5 KiB). Slot-aligned so probes
/// never straddle a cache buffer (40 B does not divide 4 KiB after the 16 B header).
const SLOTS_PER_CHUNK: u64 = 128;
/// Max chunks held in the insert cache (~1.25 MiB).
const CHUNK_CACHE_MAX: usize = 256;
/// Default write-behind cap when enabled without an explicit size.
pub const DEFAULT_WRITE_BEHIND_CAP: usize = 512 * 1024;
/// Aggregate spill / rehash chatter at DEBUG this often; per-event is TRACE.
const SPILL_DEBUG_INTERVAL: Duration = Duration::from_secs(30);
/// Single rehash still WARN if clear size or wall time exceeds these (host risk).
const REHASH_WARN_BYTES: u64 = 64 * 1024 * 1024; // 64 MiB
const REHASH_WARN_MS: u128 = 500;

/// Which hash-head file (drives mainnet pre-size).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HeadRole {
    Header,
    Tx,
    Point,
    ScriptHash,
}

/// Disk pre-size policy for hash heads.
///
/// Override with `RBITCOIN_HEAD_SCALE=tiny|mainnet` (default **mainnet**).
/// Integration tests set `tiny` so they do not allocate multi‑GiB sparse files.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HeadScale {
    /// Minimal (64 slots) — unit/integration tests.
    Tiny,
    /// Full-mainnet IBD: sharded heads with moderate **per-shard** sparse start.
    Mainnet,
}

impl HeadScale {
    /// Resolve from `RBITCOIN_HEAD_SCALE` (`tiny`/`test`/`mainnet`/`full`).
    ///
    /// Default: [`HeadScale::Mainnet`] in normal builds; [`HeadScale::Tiny`] when
    /// this crate is compiled with `cfg(test)` so unit tests stay small.
    pub fn from_env() -> Self {
        match std::env::var("RBITCOIN_HEAD_SCALE")
            .map(|s| s.to_ascii_lowercase())
            .ok()
            .as_deref()
        {
            Some("tiny") | Some("test") | Some("small") => HeadScale::Tiny,
            Some("mainnet") | Some("full") | Some("large") => HeadScale::Mainnet,
            Some(other) => {
                rbitcoin_log::warn!(
                    "store: unknown RBITCOIN_HEAD_SCALE={other:?}, using mainnet"
                );
                HeadScale::Mainnet
            }
            None => {
                if cfg!(test) {
                    HeadScale::Tiny
                } else {
                    HeadScale::Mainnet
                }
            }
        }
    }

    /// Default initial slots for a **single** hash-head file (legacy / unsharded).
    /// Sharded creates use [`crate::sharded_hashhead::initial_slots_per_shard`].
    pub fn initial_slots(self, role: HeadRole) -> u64 {
        match self {
            HeadScale::Tiny => DEFAULT_SLOTS,
            // Unsharded fallback only (legacy single-file). Prefer sharded layout.
            HeadScale::Mainnet => match role {
                HeadRole::Header => 1 << 20,
                HeadRole::ScriptHash | HeadRole::Point | HeadRole::Tx => 1 << 22,
            },
        }
    }
}

/// Effective initial slots for `role` (env scale + optional per-role override).
///
/// Per-role: `RBITCOIN_HEAD_SLOTS_HEADER`, `_TX`, `_POINT`, `_SCRIPTHASH`
/// (decimal slot count, rounded up to power of two).
pub fn initial_slots_for(role: HeadRole) -> u64 {
    let env_key = match role {
        HeadRole::Header => "RBITCOIN_HEAD_SLOTS_HEADER",
        HeadRole::Tx => "RBITCOIN_HEAD_SLOTS_TX",
        HeadRole::Point => "RBITCOIN_HEAD_SLOTS_POINT",
        HeadRole::ScriptHash => "RBITCOIN_HEAD_SLOTS_SCRIPTHASH",
    };
    if let Ok(s) = std::env::var(env_key) {
        if let Ok(n) = s.parse::<u64>() {
            return n.max(2).next_power_of_two();
        }
    }
    HeadScale::from_env().initial_slots(role)
}

pub struct HashHead {
    file: TableFile,
    state: Mutex<HashState>,
    /// Process-local write-behind (IBD). `None` = write-through (default).
    overlay: Mutex<Option<WriteBehind>>,
    /// Rolled-up spill counters for periodic DEBUG (per-event is TRACE).
    spill_stats: Mutex<SpillStats>,
}

struct HashState {
    slots: u64,
    occupied: u64,
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

/// Process-wide rehash rollup (many small shard rehashes during IBD materialize).
struct RehashStats {
    events: u64,
    keys: u64,
    bytes_cleared: u64,
    elapsed_ms: u64,
    max_clear_bytes: u64,
    window_start: Instant,
}

impl RehashStats {
    fn new() -> Self {
        Self {
            events: 0,
            keys: 0,
            bytes_cleared: 0,
            elapsed_ms: 0,
            max_clear_bytes: 0,
            window_start: Instant::now(),
        }
    }
}

impl HashHead {
    /// Create pre-sized for a store role (single-file; prefer sharded facade).
    #[allow(dead_code)]
    pub fn create_for_role(
        path: impl Into<std::path::PathBuf>,
        role: HeadRole,
    ) -> Result<Self, StoreError> {
        Self::create_with_slots(path, initial_slots_for(role))
    }

    /// Create with an explicit power-of-two slot count (sparse-allocated).
    pub fn create_with_slots(
        path: impl Into<std::path::PathBuf>,
        slots: u64,
    ) -> Result<Self, StoreError> {
        let slots = slots.max(2).next_power_of_two();
        let file = TableFile::create(path, TableKind::HashHead)?;
        let body_bytes = SLOT_SIZE as u64 * slots;
        let need = FILE_HEADER_LEN as u64 + body_bytes;
        // Prefer fallocate + punch-hole over multi‑GiB zero writes.
        file.ensure_capacity(need)?;
        file.set_logical_len(need)?;
        file.zero_range(FILE_HEADER_LEN as u64, body_bytes)?;
        if slots > DEFAULT_SLOTS {
            rbitcoin_log::trace!(
                "store: hash-head create path={} slots={} (~{:.2} GiB sparse)",
                file.path().display(),
                slots,
                body_bytes as f64 / (1024.0 * 1024.0 * 1024.0)
            );
        }
        Ok(Self {
            file,
            state: Mutex::new(HashState {
                slots,
                occupied: 0,
            }),
            overlay: Mutex::new(None),
            spill_stats: Mutex::new(SpillStats::new()),
        })
    }

    pub fn open(path: impl Into<std::path::PathBuf>) -> Result<Self, StoreError> {
        let file = TableFile::open(path, TableKind::HashHead)?;
        let body = file.logical_len().saturating_sub(FILE_HEADER_LEN as u64);
        if body % SLOT_SIZE as u64 != 0 || body == 0 {
            return Err(StoreError::Corrupt("hash head size"));
        }
        let slots = body / SLOT_SIZE as u64;
        if !slots.is_power_of_two() {
            return Err(StoreError::Corrupt("hash head slots not power of two"));
        }
        // Stream-count occupied slots in chunks — never allocate the full table.
        let mut occupied = 0u64;
        let mut buf = vec![0u8; SLOT_SIZE * 4096]; // 160 KiB scan buffer
        let mut slot = 0u64;
        while slot < slots {
            let n = ((slots - slot) as usize).min(4096);
            let off = FILE_HEADER_LEN as u64 + slot * SLOT_SIZE as u64;
            let bytes = n * SLOT_SIZE;
            file.read_at(off, &mut buf[..bytes])?;
            for i in 0..n {
                let base = i * SLOT_SIZE;
                let k: [u8; 32] = buf[base..base + 32].try_into().unwrap();
                let fk = u64::from_le_bytes(buf[base + 32..base + 40].try_into().unwrap());
                if !is_empty_slot(&k, fk) {
                    occupied += 1;
                }
            }
            slot += n as u64;
        }
        Ok(Self {
            file,
            state: Mutex::new(HashState { slots, occupied }),
            overlay: Mutex::new(None),
            spill_stats: Mutex::new(SpillStats::new()),
        })
    }

    /// Current slot table size (power of two).
    #[allow(dead_code)] // diagnostics / tests
    pub fn slots(&self) -> u64 {
        self.state.lock().unwrap().slots
    }

    fn hash_slot(key: &[u8; 32], slots: u64) -> u64 {
        let mut h: u64 = 0xcbf29ce484222325;
        for b in key {
            h ^= u64::from(*b);
            h = h.wrapping_mul(0x100000001b3);
        }
        h & (slots - 1)
    }

    fn slot_file_off(slot: u64) -> u64 {
        FILE_HEADER_LEN as u64 + slot * SLOT_SIZE as u64
    }

    fn read_slot(&self, slot: u64) -> Result<([u8; 32], u64), StoreError> {
        let mut buf = [0u8; SLOT_SIZE];
        self.file.read_at(Self::slot_file_off(slot), &mut buf)?;
        let k: [u8; 32] = buf[0..32].try_into().unwrap();
        let fk = u64::from_le_bytes(buf[32..40].try_into().unwrap());
        Ok((k, fk))
    }

    /// Number of occupied hash slots on disk (excludes pure overlay inserts until spill).
    pub fn occupied(&self) -> u64 {
        self.state.lock().unwrap().occupied
    }

    /// Pending write-behind entries (0 if overlay disabled).
    pub fn write_behind_len(&self) -> usize {
        self.overlay
            .lock()
            .unwrap()
            .as_ref()
            .map(|o| o.map.len())
            .unwrap_or(0)
    }

    /// Enable process-local write-behind for upserts.
    ///
    /// Inserts accumulate in RAM; when the map reaches `max_entries` (or on
    /// [`Self::spill_write_behind`] / [`Self::flush`] / rehash), entries are
    /// spilled with slot-sorted page-buffered apply. `get` remains coherent.
    ///
    /// Spills any existing overlay first if re-enabled with a new cap.
    /// Production tables use [`crate::sharded_hashhead::ShardedHashHead`]'s overlay.
    #[allow(dead_code)]
    pub fn enable_write_behind(&self, max_entries: usize) -> Result<(), StoreError> {
        let max_entries = max_entries.max(1);
        // Spill previous overlay so we never drop pending keys.
        self.spill_write_behind()?;
        *self.overlay.lock().unwrap() = Some(WriteBehind {
            map: HashMap::new(),
            max_entries,
        });
        Ok(())
    }

    /// Disable write-behind after spilling all pending entries.
    #[allow(dead_code)] // used via ShardedHashHead facade / tests
    pub fn disable_write_behind(&self) -> Result<(), StoreError> {
        self.spill_write_behind()?;
        *self.overlay.lock().unwrap() = None;
        Ok(())
    }

    /// Spill overlay to the file-backed table (slot-sorted). No-op if empty/off.
    pub fn spill_write_behind(&self) -> Result<(), StoreError> {
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
        rbitcoin_log::trace!(
            "store: hash-head spill path={} entries={}",
            self.file.path().display(),
            n
        );
        self.insert_many_file(&batch, |_| {})?;
        self.note_spill(n);
        Ok(())
    }

    /// Per-event TRACE already emitted; roll up a DEBUG line every
    /// [`SPILL_DEBUG_INTERVAL`] so IBD DEBUG logs stay readable.
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
            self.file.path().display(),
            s.events,
            s.entries,
            s.window_start.elapsed()
        );
        s.events = 0;
        s.entries = 0;
        s.window_start = Instant::now();
    }

    /// Minimum power-of-two slot count so `keys` stay under load factor 7/8.
    fn slots_for_keys(keys: u64) -> u64 {
        if keys == 0 {
            return DEFAULT_SLOTS;
        }
        // keys/slots < NUM/DEN  ⇒  slots > keys * DEN / NUM
        let min = keys
            .saturating_mul(MAX_LOAD_DEN)
            .div_ceil(MAX_LOAD_NUM)
            .max(1);
        min.next_power_of_two().max(DEFAULT_SLOTS)
    }

    /// Ensure capacity for roughly `additional` new keys (load factor 7/8).
    ///
    /// Counts pending overlay entries toward load. Grows to the **target** slot
    /// count in a single rehash (not one double-at-a-time loop), so a large spill
    /// does not re-copy the live table log₂(N) times.
    pub fn reserve_additional(&self, additional: u64) -> Result<(), StoreError> {
        if additional == 0 {
            return Ok(());
        }
        let overlay_len = self.write_behind_len() as u64;
        let (occupied, slots) = {
            let state = self.state.lock().unwrap();
            (state.occupied, state.slots)
        };
        let target_keys = occupied
            .saturating_add(overlay_len)
            .saturating_add(additional);
        let need = Self::slots_for_keys(target_keys);
        if need <= slots {
            return Ok(());
        }
        // Rehash needs a clean file view of all keys.
        if overlay_len > 0 {
            self.spill_write_behind()?;
            // Occupied now includes former overlay; only `additional` is still pending.
            return self.reserve_additional(additional);
        }
        self.rehash_to(need)
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
        self.get_file(key)
    }

    fn get_file(&self, key: &[u8; 32]) -> Result<Option<Fk>, StoreError> {
        let slots = self.state.lock().unwrap().slots;
        let mut slot = Self::hash_slot(key, slots);
        for _ in 0..slots {
            let (k, fk) = self.read_slot(slot)?;
            if is_empty_slot(&k, fk) {
                return Ok(None);
            }
            if &k == key {
                return Ok(Fk::new(fk));
            }
            slot = (slot + 1) & (slots - 1);
        }
        Ok(None)
    }

    #[allow(dead_code)] // unit tests; production uses insert_many / ShardedHashHead
    pub fn insert(&self, key: &[u8; 32], fk: Fk) -> Result<Option<Fk>, StoreError> {
        debug_assert!(!fk.is_null());
        let mut prev = None;
        self.insert_many_with(&[(*key, fk)], |p| prev = p)?;
        Ok(prev)
    }

    pub fn insert_many(&self, entries: &[([u8; 32], Fk)]) -> Result<(), StoreError> {
        self.insert_many_with(entries, |_| {})
    }

    fn insert_many_with(
        &self,
        entries: &[([u8; 32], Fk)],
        mut on_prev: impl FnMut(Option<Fk>),
    ) -> Result<(), StoreError> {
        if entries.is_empty() {
            return Ok(());
        }

        // Overlay path: merge into RAM; spill when over cap. `on_prev` sees
        // previous overlay/file value when known (file miss = None for new keys
        // only if we probe; for hot IBD we skip file probe on pure upsert and
        // report None when key was absent from overlay — callers that need
        // true prev use get first).
        {
            let mut guard = self.overlay.lock().unwrap();
            if let Some(ov) = guard.as_mut() {
                for (key, fk) in entries {
                    debug_assert!(!fk.is_null());
                    let prev = ov.map.insert(*key, *fk);
                    on_prev(prev);
                }
                let over = ov.map.len() >= ov.max_entries;
                if over {
                    drop(guard);
                    self.spill_write_behind()?;
                }
                return Ok(());
            }
        }

        self.insert_many_file(entries, on_prev)
    }

    /// Slot-sorted, page-buffered apply to the mmap table (no overlay).
    fn insert_many_file(
        &self,
        entries: &[([u8; 32], Fk)],
        mut on_prev: impl FnMut(Option<Fk>),
    ) -> Result<(), StoreError> {
        if entries.is_empty() {
            return Ok(());
        }
        self.reserve_additional(entries.len() as u64)?;

        // Sort by primary hash slot so linear probes walk nearby pages.
        let mut work: Vec<([u8; 32], Fk)> = entries.to_vec();
        let slots_now = self.state.lock().unwrap().slots;
        work.sort_unstable_by_key(|(k, _)| Self::hash_slot(k, slots_now));

        let mut i = 0usize;
        while i < work.len() {
            let slots = self.state.lock().unwrap().slots;
            // Re-sort remaining if a rehash changed the slot map.
            if i > 0 {
                work[i..].sort_unstable_by_key(|(k, _)| Self::hash_slot(k, slots));
            }
            let mut cache = SlotPageCache::new(self, slots);
            let mut need_rehash = false;
            while i < work.len() {
                let (key, fk) = work[i];
                debug_assert!(!fk.is_null());
                match cache.try_insert(&key, fk)? {
                    InsertResult::Done(prev) => {
                        {
                            let mut state = self.state.lock().unwrap();
                            if prev.is_none() {
                                state.occupied = state.occupied.saturating_add(1);
                            }
                        }
                        on_prev(prev);
                        i += 1;
                    }
                    InsertResult::NeedRehash => {
                        need_rehash = true;
                        break;
                    }
                }
            }
            cache.flush()?;
            if need_rehash {
                // Probe failed at current size — jump at least 2×, more if a large
                // remainder is still waiting.
                let (slots, occupied) = {
                    let state = self.state.lock().unwrap();
                    (state.slots, state.occupied)
                };
                let remain = (work.len() - i) as u64;
                let need = Self::slots_for_keys(occupied.saturating_add(remain))
                    .max(slots.saturating_mul(2));
                self.rehash_to(need)?;
            }
        }
        Ok(())
    }

    /// Process-wide: at most one hash-head shard rehash at a time (IBD materialize
    /// must not stack multi-shard resizes into one host freeze).
    fn rehash_gate() -> &'static std::sync::Mutex<()> {
        static GATE: std::sync::OnceLock<std::sync::Mutex<()>> = std::sync::OnceLock::new();
        GATE.get_or_init(|| std::sync::Mutex::new(()))
    }

    /// Process-wide rehash counters (sharded heads rehash many small files).
    fn rehash_stats() -> &'static Mutex<RehashStats> {
        static S: std::sync::OnceLock<Mutex<RehashStats>> = std::sync::OnceLock::new();
        S.get_or_init(|| Mutex::new(RehashStats::new()))
    }

    /// Note one completed rehash; TRACE always, WARN only if large/slow, DEBUG rollup.
    fn note_rehash(path: &std::path::Path, old_slots: u64, new_slots: u64, occupied: u64, elapsed: Duration) {
        let new_bytes = SLOT_SIZE as u64 * new_slots;
        let ms = elapsed.as_millis();
        rbitcoin_log::trace!(
            "store: hash-head rehash path={} {}→{} slots occupied={} clear≈{:.1} MiB elapsed={:?}",
            path.display(),
            old_slots,
            new_slots,
            occupied,
            new_bytes as f64 / (1024.0 * 1024.0),
            elapsed
        );
        if new_bytes >= REHASH_WARN_BYTES || ms >= REHASH_WARN_MS {
            rbitcoin_log::warn!(
                "store: hash-head rehash LARGE path={} {}→{} slots occupied={} (~{:.1} GiB) elapsed={:?}",
                path.display(),
                old_slots,
                new_slots,
                occupied,
                new_bytes as f64 / (1024.0 * 1024.0 * 1024.0),
                elapsed
            );
        }
        let mut s = Self::rehash_stats().lock().unwrap();
        s.events = s.events.saturating_add(1);
        s.keys = s.keys.saturating_add(occupied);
        s.bytes_cleared = s.bytes_cleared.saturating_add(new_bytes);
        s.elapsed_ms = s.elapsed_ms.saturating_add(ms as u64);
        if new_bytes > s.max_clear_bytes {
            s.max_clear_bytes = new_bytes;
        }
        if s.window_start.elapsed() < SPILL_DEBUG_INTERVAL {
            return;
        }
        if s.events == 0 {
            s.window_start = Instant::now();
            return;
        }
        rbitcoin_log::debug!(
            "store: hash-head rehash summary events={} keys={} clear≈{:.1} MiB max_clear≈{:.1} MiB wall_ms={} window={:?}",
            s.events,
            s.keys,
            s.bytes_cleared as f64 / (1024.0 * 1024.0),
            s.max_clear_bytes as f64 / (1024.0 * 1024.0),
            s.elapsed_ms,
            s.window_start.elapsed()
        );
        *s = RehashStats::new();
    }

    /// Grow to `new_slots` (power of two) and reinsert only **occupied** entries.
    ///
    /// **Host freeze risk:** multi‑GiB heads (e.g. `point.head` ~10 GiB+) used to
    /// zero-fill the whole table with `write_at` — a multi‑second IO storm. We now
    /// punch a hole (or sparse-clear) then reinsert only live keys.
    ///
    /// Serialized process-wide so paced materialize never runs two rehashes at once.
    /// Per-shard chatter is TRACE + periodic DEBUG summary (WARN only if large/slow).
    fn rehash_to(&self, new_slots: u64) -> Result<(), StoreError> {
        let new_slots = new_slots.max(2).next_power_of_two();
        let _rehash_serial = Self::rehash_gate()
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        // Overlay must be empty so the file is the source of truth.
        {
            let guard = self.overlay.lock().unwrap();
            if let Some(ov) = guard.as_ref() {
                if !ov.map.is_empty() {
                    drop(guard);
                    self.spill_write_behind()?;
                }
            }
        }

        let (old_slots, occupied) = {
            let state = self.state.lock().unwrap();
            (state.slots, state.occupied)
        };
        if new_slots <= old_slots {
            return Ok(());
        }
        let new_bytes = SLOT_SIZE as u64 * new_slots;
        let t0 = Instant::now();

        // Collect live entries only (~occupied × 40 B, not empty slots).
        let mut entries: Vec<([u8; 32], u64)> = Vec::new();
        entries
            .try_reserve_exact(occupied as usize)
            .map_err(|_| StoreError::Corrupt("hash head rehash OOM"))?;
        let mut buf = vec![0u8; SLOT_SIZE * 4096];
        let mut slot = 0u64;
        while slot < old_slots {
            let n = ((old_slots - slot) as usize).min(4096);
            let off = FILE_HEADER_LEN as u64 + slot * SLOT_SIZE as u64;
            let bytes = n * SLOT_SIZE;
            self.file.read_at(off, &mut buf[..bytes])?;
            for i in 0..n {
                let base = i * SLOT_SIZE;
                let k: [u8; 32] = buf[base..base + 32].try_into().unwrap();
                let fk = u64::from_le_bytes(buf[base + 32..base + 40].try_into().unwrap());
                if !is_empty_slot(&k, fk) {
                    entries.push((k, fk));
                }
            }
            slot += n as u64;
        }

        // Grow file, then clear the **entire** slot region without zero-fill writes.
        let need = FILE_HEADER_LEN as u64 + new_bytes;
        self.file.ensure_capacity(need)?;
        self.file.set_logical_len(need)?;
        self.file.zero_range(FILE_HEADER_LEN as u64, new_bytes)?;

        {
            let mut state = self.state.lock().unwrap();
            state.slots = new_slots;
            state.occupied = 0;
        }

        // Slot-sorted reinsert for better page locality on large tables.
        entries.sort_unstable_by_key(|(k, _)| Self::hash_slot(k, new_slots));
        let n_entries = entries.len() as u64;
        let mut cache = SlotPageCache::new(self, new_slots);
        for (k, fk) in entries {
            match cache.try_insert(&k, Fk(fk))? {
                InsertResult::Done(_) => {}
                InsertResult::NeedRehash => {
                    cache.flush()?;
                    return Err(StoreError::Corrupt("hash rehash failed"));
                }
            }
        }
        cache.flush()?;
        self.state.lock().unwrap().occupied = n_entries;
        Self::note_rehash(
            self.file.path(),
            old_slots,
            new_slots,
            n_entries,
            t0.elapsed(),
        );
        Ok(())
    }

    /// Spill write-behind (if any). Kept for API compatibility.
    #[allow(dead_code)]
    pub fn persist(&self) -> Result<(), StoreError> {
        self.spill_write_behind()
    }

    pub fn flush(&self) -> Result<(), StoreError> {
        self.spill_write_behind()?;
        self.file.flush()
    }

    #[allow(dead_code)] // used via ShardedHashHead facade / tests
    pub fn flush_async(&self) -> Result<(), StoreError> {
        self.spill_write_behind()?;
        self.flush_async_no_spill()
    }

    pub fn flush_async_no_spill(&self) -> Result<(), StoreError> {
        self.file.flush_async()
    }
}

/// Chunk-buffered slot RMW for a fixed `slots` generation.
struct SlotPageCache<'a> {
    head: &'a HashHead,
    slots: u64,
    chunks: BTreeMap<u64, CachedChunk>,
}

struct CachedChunk {
    /// First slot index covered by `data`.
    base_slot: u64,
    data: Vec<u8>,
    dirty: bool,
}

impl<'a> SlotPageCache<'a> {
    fn new(head: &'a HashHead, slots: u64) -> Self {
        Self {
            head,
            slots,
            chunks: BTreeMap::new(),
        }
    }

    fn try_insert(&mut self, key: &[u8; 32], fk: Fk) -> Result<InsertResult, StoreError> {
        let mut slot = HashHead::hash_slot(key, self.slots);
        for _ in 0..self.slots {
            let (k, old_fk) = self.read_slot(slot)?;
            if is_empty_slot(&k, old_fk) {
                self.write_slot(slot, key, fk.0)?;
                return Ok(InsertResult::Done(None));
            }
            if &k == key {
                self.write_slot(slot, key, fk.0)?;
                return Ok(InsertResult::Done(Fk::new(old_fk)));
            }
            slot = (slot + 1) & (self.slots - 1);
        }
        Ok(InsertResult::NeedRehash)
    }

    fn read_slot(&mut self, slot: u64) -> Result<([u8; 32], u64), StoreError> {
        let chunk = self.ensure_chunk(slot)?;
        let rel = ((slot - chunk.base_slot) as usize) * SLOT_SIZE;
        let k: [u8; 32] = chunk.data[rel..rel + 32].try_into().unwrap();
        let fk = u64::from_le_bytes(chunk.data[rel + 32..rel + 40].try_into().unwrap());
        Ok((k, fk))
    }

    fn write_slot(&mut self, slot: u64, key: &[u8; 32], fk: u64) -> Result<(), StoreError> {
        let chunk = self.ensure_chunk(slot)?;
        let rel = ((slot - chunk.base_slot) as usize) * SLOT_SIZE;
        chunk.data[rel..rel + 32].copy_from_slice(key);
        chunk.data[rel + 32..rel + 40].copy_from_slice(&fk.to_le_bytes());
        chunk.dirty = true;
        Ok(())
    }

    fn ensure_chunk(&mut self, slot: u64) -> Result<&mut CachedChunk, StoreError> {
        let chunk_idx = slot / SLOTS_PER_CHUNK;
        if !self.chunks.contains_key(&chunk_idx) {
            if self.chunks.len() >= CHUNK_CACHE_MAX {
                self.flush()?;
            }
            let base_slot = chunk_idx * SLOTS_PER_CHUNK;
            let n = ((self.slots - base_slot) as usize).min(SLOTS_PER_CHUNK as usize);
            let off = HashHead::slot_file_off(base_slot);
            let len = n * SLOT_SIZE;
            let mut data = vec![0u8; len];
            self.head.file.read_at(off, &mut data)?;
            self.chunks.insert(
                chunk_idx,
                CachedChunk {
                    base_slot,
                    data,
                    dirty: false,
                },
            );
        }
        Ok(self.chunks.get_mut(&chunk_idx).unwrap())
    }

    fn flush(&mut self) -> Result<(), StoreError> {
        // BTreeMap iterates in chunk/slot order → sequential mmap writeback.
        for (_, chunk) in self.chunks.iter_mut() {
            if chunk.dirty {
                let off = HashHead::slot_file_off(chunk.base_slot);
                self.head.file.write_at(off, &chunk.data)?;
                chunk.dirty = false;
            }
        }
        self.chunks.clear();
        Ok(())
    }
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

fn is_empty_slot(k: &[u8; 32], fk: u64) -> bool {
    fk == 0 && *k == [0u8; 32]
}

enum InsertResult {
    Done(Option<Fk>),
    NeedRehash,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp_path() -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "rbitcoin-hh-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ))
    }

    #[test]
    fn dense_load_inserts_without_early_rehash() {
        let path = tmp_path();
        let h = HashHead::create_with_slots(&path, DEFAULT_SLOTS).unwrap();
        for i in 0u64..50 {
            let mut key = [0u8; 32];
            key[0..8].copy_from_slice(&i.to_le_bytes());
            h.insert(&key, Fk(i + 1)).unwrap();
        }
        for i in 0u64..50 {
            let mut key = [0u8; 32];
            key[0..8].copy_from_slice(&i.to_le_bytes());
            assert_eq!(h.get(&key).unwrap(), Some(Fk(i + 1)));
        }
        for i in 50u64..70 {
            let mut key = [0u8; 32];
            key[0..8].copy_from_slice(&i.to_le_bytes());
            h.insert(&key, Fk(i + 1)).unwrap();
        }
        assert_eq!(h.get(&[0u8; 32]).unwrap(), Some(Fk(1)));
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn persist_survives_reopen() {
        let path = tmp_path();
        {
            let h = HashHead::create_with_slots(&path, DEFAULT_SLOTS).unwrap();
            let mut batch = Vec::new();
            for i in 0u64..100 {
                let mut key = [0u8; 32];
                key[0..8].copy_from_slice(&i.to_le_bytes());
                batch.push((key, Fk(i + 1)));
            }
            h.insert_many(&batch).unwrap();
            h.flush().unwrap();
        }
        let h = HashHead::open(&path).unwrap();
        assert_eq!(h.occupied(), 100);
        for i in 0u64..100 {
            let mut key = [0u8; 32];
            key[0..8].copy_from_slice(&i.to_le_bytes());
            assert_eq!(h.get(&key).unwrap(), Some(Fk(i + 1)));
        }
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn reserve_additional_grows_before_insert() {
        let path = tmp_path();
        let h = HashHead::create_with_slots(&path, DEFAULT_SLOTS).unwrap();
        h.reserve_additional(10_000).unwrap();
        let mut batch = Vec::new();
        for i in 0u64..5_000 {
            let mut key = [0u8; 32];
            key[0..8].copy_from_slice(&i.to_le_bytes());
            batch.push((key, Fk(i + 1)));
        }
        h.insert_many(&batch).unwrap();
        assert_eq!(h.occupied(), 5_000);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn reserve_additional_jumps_to_target_slots() {
        // Formerly doubled in a loop (log₂ empty rehashes). One jump to capacity.
        let path = tmp_path();
        let h = HashHead::create_with_slots(&path, DEFAULT_SLOTS).unwrap();
        assert_eq!(h.slots(), 64);
        h.reserve_additional(10_000).unwrap();
        let slots = h.slots();
        // slots_for_keys(10000) = next_pow2(ceil(10000*8/7)) = next_pow2(11429) = 16384
        assert_eq!(slots, 16_384);
        // Second reserve for same size is a no-op (no smaller/equal grow).
        h.reserve_additional(10_000).unwrap();
        assert_eq!(h.slots(), 16_384);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn create_with_slots_sparse_presize() {
        let path = tmp_path();
        let h = HashHead::create_with_slots(&path, 1024).unwrap();
        assert_eq!(h.slots(), 1024);
        assert_eq!(h.occupied(), 0);
        h.insert(&[1u8; 32], Fk(1)).unwrap();
        assert_eq!(h.get(&[1u8; 32]).unwrap(), Some(Fk(1)));
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn mainnet_scale_slot_targets() {
        assert_eq!(HeadScale::Tiny.initial_slots(HeadRole::Point), 64);
        assert!(HeadScale::Mainnet.initial_slots(HeadRole::Point) >= 64);
    }

    #[test]
    fn large_spill_does_not_multi_rehash_existing() {
        // Fill past first rehash, enable overlay, spill a large batch once.
        let path = tmp_path();
        let h = HashHead::create_with_slots(&path, DEFAULT_SLOTS).unwrap();
        let mut batch = Vec::new();
        for i in 0u64..200 {
            let mut key = [0u8; 32];
            key[0..8].copy_from_slice(&i.to_le_bytes());
            batch.push((key, Fk(i + 1)));
        }
        h.insert_many(&batch).unwrap();
        let slots_after_seed = h.state.lock().unwrap().slots;
        h.enable_write_behind(50_000).unwrap();
        let mut more = Vec::new();
        for i in 200u64..20_200 {
            let mut key = [0u8; 32];
            key[0..8].copy_from_slice(&i.to_le_bytes());
            more.push((key, Fk(i + 1)));
        }
        h.insert_many(&more).unwrap();
        // Under cap — still in overlay.
        assert!(h.write_behind_len() > 0);
        h.spill_write_behind().unwrap();
        assert_eq!(h.occupied(), 20_200);
        let slots = h.state.lock().unwrap().slots;
        // Capacity for 20200 keys: next_pow2(ceil(20200*8/7)) = next_pow2(23086) = 32768
        assert!(slots >= 32_768, "slots={slots} seed_slots={slots_after_seed}");
        // All keys visible.
        for i in [0u64, 199, 200, 20_199] {
            let mut key = [0u8; 32];
            key[0..8].copy_from_slice(&i.to_le_bytes());
            assert_eq!(h.get(&key).unwrap(), Some(Fk(i + 1)));
        }
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn open_does_not_require_full_ram_copy() {
        let path = tmp_path();
        let h = HashHead::create_with_slots(&path, DEFAULT_SLOTS).unwrap();
        h.insert(&[1u8; 32], Fk(42)).unwrap();
        h.flush().unwrap();
        drop(h);
        let h = HashHead::open(&path).unwrap();
        assert_eq!(h.get(&[1u8; 32]).unwrap(), Some(Fk(42)));
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn write_behind_get_coherent_until_spill() {
        let path = tmp_path();
        let h = HashHead::create_with_slots(&path, DEFAULT_SLOTS).unwrap();
        h.enable_write_behind(10_000).unwrap();
        let mut key = [0u8; 32];
        key[0] = 7;
        h.insert(&key, Fk(99)).unwrap();
        assert_eq!(h.write_behind_len(), 1);
        // Visible via get before spill; not yet counted as disk occupied.
        assert_eq!(h.get(&key).unwrap(), Some(Fk(99)));
        assert_eq!(h.occupied(), 0);
        h.spill_write_behind().unwrap();
        assert_eq!(h.write_behind_len(), 0);
        assert_eq!(h.occupied(), 1);
        assert_eq!(h.get(&key).unwrap(), Some(Fk(99)));
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn write_behind_auto_spills_at_cap() {
        let path = tmp_path();
        let h = HashHead::create_with_slots(&path, DEFAULT_SLOTS).unwrap();
        h.enable_write_behind(64).unwrap();
        let mut batch = Vec::new();
        for i in 0u64..64 {
            let mut key = [0u8; 32];
            key[0..8].copy_from_slice(&i.to_le_bytes());
            batch.push((key, Fk(i + 1)));
        }
        h.insert_many(&batch).unwrap();
        // Cap hit → spill; overlay empty, disk holds keys.
        assert_eq!(h.write_behind_len(), 0);
        assert_eq!(h.occupied(), 64);
        for i in 0u64..64 {
            let mut key = [0u8; 32];
            key[0..8].copy_from_slice(&i.to_le_bytes());
            assert_eq!(h.get(&key).unwrap(), Some(Fk(i + 1)));
        }
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn write_behind_flush_spills_and_survives_reopen() {
        let path = tmp_path();
        {
            let h = HashHead::create_with_slots(&path, DEFAULT_SLOTS).unwrap();
            h.enable_write_behind(DEFAULT_WRITE_BEHIND_CAP).unwrap();
            for i in 0u64..200 {
                let mut key = [0u8; 32];
                key[0..8].copy_from_slice(&i.to_le_bytes());
                h.insert(&key, Fk(i + 1)).unwrap();
            }
            assert!(h.write_behind_len() > 0);
            h.flush().unwrap();
        }
        let h = HashHead::open(&path).unwrap();
        assert_eq!(h.occupied(), 200);
        for i in 0u64..200 {
            let mut key = [0u8; 32];
            key[0..8].copy_from_slice(&i.to_le_bytes());
            assert_eq!(h.get(&key).unwrap(), Some(Fk(i + 1)));
        }
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn slot_sorted_batch_matches_sequential_inserts() {
        let path = tmp_path();
        let h = HashHead::create_with_slots(&path, DEFAULT_SLOTS).unwrap();
        let mut batch = Vec::new();
        for i in 0u64..500 {
            let mut key = [0u8; 32];
            // Scramble key so primary slots are not insertion order.
            key[0..8].copy_from_slice(&(i.wrapping_mul(0x9e3779b97f4a7c15)).to_le_bytes());
            batch.push((key, Fk(i + 1)));
        }
        h.insert_many(&batch).unwrap();
        for (key, fk) in &batch {
            assert_eq!(h.get(key).unwrap(), Some(*fk));
        }
        // Overwrite subset
        let mut upd = Vec::new();
        for i in 0u64..50 {
            let mut key = [0u8; 32];
            key[0..8].copy_from_slice(&(i.wrapping_mul(0x9e3779b97f4a7c15)).to_le_bytes());
            upd.push((key, Fk(10_000 + i)));
        }
        h.insert_many(&upd).unwrap();
        for (key, fk) in &upd {
            assert_eq!(h.get(key).unwrap(), Some(*fk));
        }
        assert_eq!(h.occupied(), 500);
        let _ = std::fs::remove_file(&path);
    }
}
