//! Growable hash head: key **prefix** (16 bytes) → record fk (u64), with multi-fk chains.
//!
//! Linear probing over a power-of-two slot table. Rehashes (doubles slots) when
//! load factor exceeds [`MAX_LOAD_NUM`]/[`MAX_LOAD_DEN`].
//!
//! **16-byte keys** (first 16 of full 32-byte hash) cut head size ~40%. Callers that
//! need exact identity (tx.head, header.head) **verify** by loading the body.
//! When multiple Class A rows share a prefix (or BIP30 duplicate full txids), the
//! packed value sets the high bit and points at a multi-list (`.mlt` sibling file):
//! `create_fk:u64 | next:u64`.
//!
//! **IBD write path:** page-cache insert_many (write-through).

use crate::error::StoreError;
use crate::file::{TableFile, FILE_HEADER_LEN};
use rbitcoin_primitives::{Fk, TableKind};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::{Duration, Instant};

/// Open-address key length (prefix of full 32-byte hash).
pub const HEAD_KEY_LEN: usize = 16;
/// Slot = key[16] + packed value[8].
const SLOT_SIZE: usize = HEAD_KEY_LEN + 8;
/// High bit of packed value: multi-list head (else sole create_fk).
const MULTI_BIT: u64 = 1u64 << 63;
const MULTI_REC_LEN: usize = 16; // create_fk | next
const DEFAULT_SLOTS: u64 = 64;
use crate::open_address::{self, MAX_LOAD_DEN, MAX_LOAD_NUM};
/// Slots per page-cache RMW chunk (128 × 24 B = 3 KiB).
const SLOTS_PER_CHUNK: u64 = 128;

pub type HeadKey = [u8; HEAD_KEY_LEN];

#[inline]
pub fn head_key_prefix(full: &[u8; 32]) -> HeadKey {
    let mut k = [0u8; HEAD_KEY_LEN];
    k.copy_from_slice(&full[0..HEAD_KEY_LEN]);
    k
}

#[inline]
fn pack_sole(fk: Fk) -> u64 {
    debug_assert_eq!(fk.0 & MULTI_BIT, 0);
    fk.0
}

#[inline]
fn pack_multi(list_head: Fk) -> u64 {
    debug_assert!(!list_head.is_null());
    list_head.0 | MULTI_BIT
}

#[inline]
fn unpack_value(v: u64) -> (bool, Fk) {
    if v & MULTI_BIT != 0 {
        (true, Fk(v & !MULTI_BIT))
    } else {
        (false, Fk(v))
    }
}
/// Max chunks held in the insert cache (~1.25 MiB).
const CHUNK_CACHE_MAX: usize = 256;
/// Aggregate rehash chatter at DEBUG this often; per-event is TRACE.
const SPILL_DEBUG_INTERVAL: Duration = Duration::from_secs(30);
/// Single rehash still WARN if clear size or wall time exceeds these (host risk).
const REHASH_WARN_BYTES: u64 = 64 * 1024 * 1024; // 64 MiB
const REHASH_WARN_MS: u128 = 500;

/// Which hash-head file (drives mainnet pre-size).
///
/// Schema v5 removed the durable `point.head` spend multimap; spend edges live
/// on create outputs. Roles here are only live open-hash tables.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HeadRole {
    Header,
    Tx,
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

/// True when this process is a `cargo test` binary (`target/*/deps/*`).
fn running_as_cargo_test_binary() -> bool {
    std::env::current_exe()
        .ok()
        .map(|p| {
            let s = p.to_string_lossy();
            // Unix + Windows deps layout for cargo test executables.
            s.contains("/deps/") || s.contains("\\deps\\")
        })
        .unwrap_or(false)
}

impl HeadScale {
    /// Resolve from `RBITCOIN_HEAD_SCALE` (`tiny`/`test`/`mainnet`/`full`).
    ///
    /// Default: [`HeadScale::Mainnet`] for normal binaries; [`HeadScale::Tiny`]
    /// when this crate is under `cfg(test)` or the process is a cargo test binary.
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
                // Tiny for unit tests of this crate, and for cargo test binaries
                // (they live under target/*/deps/; store is a non-test dep so
                // cfg!(test) is false there — without this, 8 GiB mainnet heads).
                if cfg!(test) || running_as_cargo_test_binary() {
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
                HeadRole::ScriptHash | HeadRole::Tx => 1 << 22,
            },
        }
    }
}

/// Effective initial slots for `role` (env scale + optional per-role override).
///
/// Per-role: `RBITCOIN_HEAD_SLOTS_HEADER`, `_TX`, `_SCRIPTHASH`
/// (decimal slot count, rounded up to power of two).
pub fn initial_slots_for(role: HeadRole) -> u64 {
    let env_key = match role {
        HeadRole::Header => "RBITCOIN_HEAD_SLOTS_HEADER",
        HeadRole::Tx => "RBITCOIN_HEAD_SLOTS_TX",
        HeadRole::ScriptHash => "RBITCOIN_HEAD_SLOTS_SCRIPTHASH",
    };
    if let Ok(s) = std::env::var(env_key) {
        if let Ok(n) = s.parse::<u64>() {
            return n.max(2).next_power_of_two();
        }
    }
    HeadScale::from_env().initial_slots(role)
}

/// Append-only multi-fk list for a single 16-byte head key (prefix / BIP30).
struct MultiList {
    file: TableFile,
    count: Mutex<u64>,
}

impl MultiList {
    fn path_for(head_path: &Path) -> PathBuf {
        let mut p = head_path.as_os_str().to_os_string();
        p.push(".mlt");
        PathBuf::from(p)
    }

    fn create(head_path: &Path) -> Result<Self, StoreError> {
        let path = Self::path_for(head_path);
        let file = TableFile::create(path, TableKind::ArrayLink)?;
        Ok(Self {
            file,
            count: Mutex::new(0),
        })
    }

    fn open(head_path: &Path) -> Result<Self, StoreError> {
        let path = Self::path_for(head_path);
        if !path.exists() {
            return Self::create(head_path);
        }
        let file = TableFile::open(path, TableKind::ArrayLink)?;
        let body = file.logical_len().saturating_sub(FILE_HEADER_LEN as u64);
        if body % MULTI_REC_LEN as u64 != 0 {
            return Err(StoreError::Corrupt("hash head multi-list size"));
        }
        Ok(Self {
            file,
            count: Mutex::new(body / MULTI_REC_LEN as u64),
        })
    }

    fn offset(id: u64) -> u64 {
        FILE_HEADER_LEN as u64 + (id - 1) * MULTI_REC_LEN as u64
    }

    fn append(&self, create_fk: Fk, next: Fk) -> Result<Fk, StoreError> {
        let mut count = self.count.lock().unwrap();
        let id = *count + 1;
        let mut buf = [0u8; MULTI_REC_LEN];
        buf[0..8].copy_from_slice(&create_fk.0.to_le_bytes());
        buf[8..16].copy_from_slice(&next.0.to_le_bytes());
        self.file.write_at(Self::offset(id), &buf)?;
        *count = id;
        Ok(Fk(id))
    }

    fn get(&self, fk: Fk) -> Result<(Fk, Fk), StoreError> {
        let id = fk.get().ok_or(StoreError::InvalidFk)?;
        let count = *self.count.lock().unwrap();
        if id == 0 || id > count {
            return Err(StoreError::NotFound);
        }
        let mut buf = [0u8; MULTI_REC_LEN];
        self.file.read_at(Self::offset(id), &mut buf)?;
        Ok((
            Fk(u64::from_le_bytes(buf[0..8].try_into().unwrap())),
            Fk(u64::from_le_bytes(buf[8..16].try_into().unwrap())),
        ))
    }

    fn collect(&self, head: Fk) -> Result<Vec<Fk>, StoreError> {
        let mut out = Vec::new();
        let mut cur = head;
        let mut guard = 0u32;
        while !cur.is_null() {
            let (create_fk, next) = self.get(cur)?;
            out.push(create_fk);
            cur = next;
            guard += 1;
            if guard > 1_000_000 {
                return Err(StoreError::Corrupt("hash head multi-list cycle"));
            }
        }
        Ok(out)
    }

    fn contains(&self, head: Fk, target: Fk) -> Result<bool, StoreError> {
        Ok(self.collect(head)?.iter().any(|f| *f == target))
    }

    fn flush(&self) -> Result<(), StoreError> {
        self.file.flush()
    }

    fn flush_async(&self) -> Result<(), StoreError> {
        self.file.flush_async()
    }
}

pub struct HashHead {
    file: TableFile,
    multi: MultiList,
    state: Mutex<HashState>,
}

struct HashState {
    slots: u64,
    occupied: u64,
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
    /// Create with an explicit power-of-two slot count (sparse-allocated).
    pub fn create_with_slots(
        path: impl Into<std::path::PathBuf>,
        slots: u64,
    ) -> Result<Self, StoreError> {
        let path = path.into();
        let slots = slots.max(2).next_power_of_two();
        let file = TableFile::create(&path, TableKind::HashHead)?;
        let multi = MultiList::create(&path)?;
        let body_bytes = SLOT_SIZE as u64 * slots;
        let need = FILE_HEADER_LEN as u64 + body_bytes;
        file.ensure_capacity(need)?;
        file.set_logical_len(need)?;
        file.zero_range(FILE_HEADER_LEN as u64, body_bytes)?;
        if slots > DEFAULT_SLOTS {
            rbitcoin_log::trace!(
                "store: hash-head create path={} slots={} (~{:.2} GiB sparse)",
                file.path().display(),
                slots,
                body_bytes as f64 / (1024.0 * 1024.0 * 1024.0)
            );
        }
        Ok(Self {
            file,
            multi,
            state: Mutex::new(HashState {
                slots,
                occupied: 0,
            }),
        })
    }

    pub fn open(path: impl Into<std::path::PathBuf>) -> Result<Self, StoreError> {
        let path = path.into();
        let file = TableFile::open(&path, TableKind::HashHead)?;
        let multi = MultiList::open(&path)?;
        let body = file.logical_len().saturating_sub(FILE_HEADER_LEN as u64);
        if body % SLOT_SIZE as u64 != 0 || body == 0 {
            return Err(StoreError::Corrupt("hash head size"));
        }
        let slots = body / SLOT_SIZE as u64;
        if !slots.is_power_of_two() {
            return Err(StoreError::Corrupt("hash head slots not power of two"));
        }
        let mut occupied = 0u64;
        let mut buf = vec![0u8; SLOT_SIZE * 4096];
        let mut slot = 0u64;
        while slot < slots {
            let n = ((slots - slot) as usize).min(4096);
            let off = FILE_HEADER_LEN as u64 + slot * SLOT_SIZE as u64;
            let bytes = n * SLOT_SIZE;
            file.read_at(off, &mut buf[..bytes])?;
            for i in 0..n {
                let base = i * SLOT_SIZE;
                let k: HeadKey = buf[base..base + HEAD_KEY_LEN].try_into().unwrap();
                let packed = u64::from_le_bytes(
                    buf[base + HEAD_KEY_LEN..base + SLOT_SIZE].try_into().unwrap(),
                );
                if !is_empty_slot(&k, packed) {
                    occupied += 1;
                }
            }
            slot += n as u64;
        }
        Ok(Self {
            file,
            multi,
            state: Mutex::new(HashState { slots, occupied }),
        })
    }

    fn hash_slot(key: &HeadKey, slots: u64) -> u64 {
        open_address::primary_slot(key, slots)
    }

    fn slot_file_off(slot: u64) -> u64 {
        FILE_HEADER_LEN as u64 + slot * SLOT_SIZE as u64
    }

    fn read_slot(&self, slot: u64) -> Result<(HeadKey, u64), StoreError> {
        let mut buf = [0u8; SLOT_SIZE];
        self.file.read_at(Self::slot_file_off(slot), &mut buf)?;
        let k: HeadKey = buf[0..HEAD_KEY_LEN].try_into().unwrap();
        let packed = u64::from_le_bytes(buf[HEAD_KEY_LEN..SLOT_SIZE].try_into().unwrap());
        Ok((k, packed))
    }

    /// All Class A fks for this full 32-byte key's 16-byte prefix (sole or multi).
    ///
    /// Newest-first; callers that need exact identity must verify the body.
    pub fn get_all(&self, full: &[u8; 32]) -> Result<Vec<Fk>, StoreError> {
        let key = head_key_prefix(full);
        self.get_all_prefix(&key)
    }

    fn get_all_prefix(&self, key: &HeadKey) -> Result<Vec<Fk>, StoreError> {
        let slots = self.state.lock().unwrap().slots;
        let mut slot = Self::hash_slot(key, slots);
        for _ in 0..slots {
            let (k, packed) = self.read_slot(slot)?;
            if is_empty_slot(&k, packed) {
                return Ok(Vec::new());
            }
            if &k == key {
                let (multi, head) = unpack_value(packed);
                if multi {
                    return self.multi.collect(head);
                }
                if head.is_null() {
                    return Ok(Vec::new());
                }
                return Ok(vec![head]);
            }
            slot = (slot + 1) & (slots - 1);
        }
        Ok(Vec::new())
    }

    /// Number of occupied hash slots (unit tests only).
    #[cfg(test)]
    pub fn occupied(&self) -> u64 {
        self.state.lock().unwrap().occupied
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
    /// Grows to the **target** slot count in a single rehash (not one
    /// double-at-a-time loop).
    pub fn reserve_additional(&self, additional: u64) -> Result<(), StoreError> {
        if additional == 0 {
            return Ok(());
        }
        let (occupied, slots) = {
            let state = self.state.lock().unwrap();
            (state.occupied, state.slots)
        };
        let target_keys = occupied.saturating_add(additional);
        let need = Self::slots_for_keys(target_keys);
        if need <= slots {
            return Ok(());
        }
        self.rehash_to(need)
    }

    /// First mapped fk for this full key prefix (newest in multi lists).
    ///
    /// Callers that need exact identity must verify the body (txid/hash) or use
    /// [`Self::get_all`] and filter.
    pub fn get(&self, key: &[u8; 32]) -> Result<Option<Fk>, StoreError> {
        let all = self.get_all(key)?;
        Ok(all.first().copied())
    }


    /// Single-key insert (unit tests; production uses [`Self::insert_many`]).
    #[cfg(test)]
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
        on_prev: impl FnMut(Option<Fk>),
    ) -> Result<(), StoreError> {
        if entries.is_empty() {
            return Ok(());
        }

        self.insert_many_file(entries, on_prev)
    }

    /// Slot-sorted, page-buffered apply to the mmap table.
    ///
    /// When the table is **empty** (first load / run materialize into a cold
    /// shard), builds the open-addressing table in RAM and writes it in one
    /// sequential pass — same idea as the offline SH bulk builder (no RMW of
    /// zero pages, no growth rehash cascade mid-batch).
    fn insert_many_file(
        &self,
        entries: &[([u8; 32], Fk)],
        mut on_prev: impl FnMut(Option<Fk>),
    ) -> Result<(), StoreError> {
        if entries.is_empty() {
            return Ok(());
        }
        self.reserve_additional(entries.len() as u64)?;

        let occupied = self.state.lock().unwrap().occupied;
        if occupied == 0 {
            return self.bulk_fill_empty(entries, on_prev);
        }

        // Sort by primary hash slot so linear probes walk nearby pages.
        let mut work: Vec<([u8; 32], Fk)> = entries.to_vec();
        let slots_now = self.state.lock().unwrap().slots;
        work.sort_unstable_by_key(|(k, _)| Self::hash_slot(&head_key_prefix(k), slots_now));

        let mut i = 0usize;
        while i < work.len() {
            let slots = self.state.lock().unwrap().slots;
            // Re-sort remaining if a rehash changed the slot map.
            if i > 0 {
                work[i..].sort_unstable_by_key(|(k, _)| Self::hash_slot(&head_key_prefix(k), slots));
            }
            let mut cache = SlotPageCache::new(self, slots);
            let mut need_rehash = false;
            while i < work.len() {
                let (key, fk) = work[i];
                debug_assert!(!fk.is_null());
                match cache.try_insert_merge(&key, fk)? {
                    InsertResult::Done { prev, new_slot } => {
                        if new_slot {
                            let mut state = self.state.lock().unwrap();
                            state.occupied = state.occupied.saturating_add(1);
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

    /// Place `entries` into a currently-empty slot table (caller reserved capacity).
    ///
    /// Same 16-byte prefix with a different fk becomes a multi-list (BIP30 / prefix
    /// collision). Builds the full slot image in RAM then one `write_at` — used for
    /// cold run→head materialize.
    fn bulk_fill_empty(
        &self,
        entries: &[([u8; 32], Fk)],
        mut on_prev: impl FnMut(Option<Fk>),
    ) -> Result<(), StoreError> {
        debug_assert_eq!(self.state.lock().unwrap().occupied, 0);
        let slots = self.state.lock().unwrap().slots;
        let nbytes = (slots as usize).saturating_mul(SLOT_SIZE);
        let mut table = vec![0u8; nbytes];
        let mut occupied = 0u64;

        for &(full, fk) in entries {
            debug_assert!(!fk.is_null());
            let key = head_key_prefix(&full);
            let mut slot = Self::hash_slot(&key, slots);
            let mut placed = false;
            for _ in 0..slots {
                let off = (slot as usize) * SLOT_SIZE;
                let slot_key: HeadKey = table[off..off + HEAD_KEY_LEN].try_into().unwrap();
                let packed = u64::from_le_bytes(
                    table[off + HEAD_KEY_LEN..off + SLOT_SIZE].try_into().unwrap(),
                );
                if is_empty_slot(&slot_key, packed) {
                    table[off..off + HEAD_KEY_LEN].copy_from_slice(&key);
                    table[off + HEAD_KEY_LEN..off + SLOT_SIZE]
                        .copy_from_slice(&pack_sole(fk).to_le_bytes());
                    occupied = occupied.saturating_add(1);
                    on_prev(None);
                    placed = true;
                    break;
                }
                if slot_key == key {
                    let new_packed = merge_packed(&self.multi, packed, fk)?;
                    table[off + HEAD_KEY_LEN..off + SLOT_SIZE]
                        .copy_from_slice(&new_packed.to_le_bytes());
                    let (_, old_head) = unpack_value(packed);
                    on_prev(if old_head.is_null() { None } else { Some(old_head) });
                    placed = true;
                    break;
                }
                slot = (slot + 1) & (slots - 1);
            }
            if !placed {
                // Should not happen after reserve_additional for unique-ish keys.
                return Err(StoreError::Corrupt("hash head bulk_fill full"));
            }
        }

        self.file
            .write_at(FILE_HEADER_LEN as u64, &table)?;
        self.state.lock().unwrap().occupied = occupied;
        Ok(())
    }

    /// Process-wide rehash serialization (shared with scripthash heads).
    fn rehash_gate() -> &'static std::sync::Mutex<()> {
        open_address::rehash_gate()
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
    /// **Host freeze risk:** multi‑GiB heads (e.g. large `tx.head` shards) used to
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

        let (old_slots, occupied) = {
            let state = self.state.lock().unwrap();
            (state.slots, state.occupied)
        };
        if new_slots <= old_slots {
            return Ok(());
        }
        let new_bytes = SLOT_SIZE as u64 * new_slots;
        let t0 = Instant::now();

        // Collect live entries only (~occupied × slot, not empty slots). Packed
        // values (sole or MULTI_BIT multi heads) are preserved as-is.
        let mut entries: Vec<(HeadKey, u64)> = Vec::new();
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
                let k: HeadKey = buf[base..base + HEAD_KEY_LEN].try_into().unwrap();
                let packed = u64::from_le_bytes(
                    buf[base + HEAD_KEY_LEN..base + SLOT_SIZE].try_into().unwrap(),
                );
                if !is_empty_slot(&k, packed) {
                    entries.push((k, packed));
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
        for (k, packed) in entries {
            match cache.try_place_raw(&k, packed)? {
                InsertResult::Done { .. } => {}
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




    pub fn flush(&self) -> Result<(), StoreError> {
        self.multi.flush()?;
        self.file.flush()
    }

    pub fn flush_async(&self) -> Result<(), StoreError> {
        self.multi.flush_async()?;
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

    /// Insert / merge `fk` under the 16-byte prefix of `full`.
    fn try_insert_merge(
        &mut self,
        full: &[u8; 32],
        fk: Fk,
    ) -> Result<InsertResult, StoreError> {
        let key = head_key_prefix(full);
        let mut slot = HashHead::hash_slot(&key, self.slots);
        for _ in 0..self.slots {
            let (k, packed) = self.read_slot(slot)?;
            if is_empty_slot(&k, packed) {
                self.write_slot(slot, &key, pack_sole(fk))?;
                return Ok(InsertResult::Done {
                    prev: None,
                    new_slot: true,
                });
            }
            if k == key {
                let (_, old_head) = unpack_value(packed);
                let new_packed = merge_packed(&self.head.multi, packed, fk)?;
                self.write_slot(slot, &key, new_packed)?;
                return Ok(InsertResult::Done {
                    prev: if old_head.is_null() {
                        None
                    } else {
                        Some(old_head)
                    },
                    new_slot: false,
                });
            }
            slot = (slot + 1) & (self.slots - 1);
        }
        Ok(InsertResult::NeedRehash)
    }

    /// Place a pre-packed slot value during rehash (no multi merge).
    fn try_place_raw(
        &mut self,
        key: &HeadKey,
        packed: u64,
    ) -> Result<InsertResult, StoreError> {
        let mut slot = HashHead::hash_slot(key, self.slots);
        for _ in 0..self.slots {
            let (k, old) = self.read_slot(slot)?;
            if is_empty_slot(&k, old) {
                self.write_slot(slot, key, packed)?;
                return Ok(InsertResult::Done {
                    prev: None,
                    new_slot: true,
                });
            }
            if &k == key {
                // Should not collide during rehash of unique prefixes.
                self.write_slot(slot, key, packed)?;
                return Ok(InsertResult::Done {
                    prev: Some(Fk(old)),
                    new_slot: false,
                });
            }
            slot = (slot + 1) & (self.slots - 1);
        }
        Ok(InsertResult::NeedRehash)
    }

    fn read_slot(&mut self, slot: u64) -> Result<(HeadKey, u64), StoreError> {
        let chunk = self.ensure_chunk(slot)?;
        let rel = ((slot - chunk.base_slot) as usize) * SLOT_SIZE;
        let k: HeadKey = chunk.data[rel..rel + HEAD_KEY_LEN].try_into().unwrap();
        let packed = u64::from_le_bytes(
            chunk.data[rel + HEAD_KEY_LEN..rel + SLOT_SIZE]
                .try_into()
                .unwrap(),
        );
        Ok((k, packed))
    }

    fn write_slot(&mut self, slot: u64, key: &HeadKey, packed: u64) -> Result<(), StoreError> {
        let chunk = self.ensure_chunk(slot)?;
        let rel = ((slot - chunk.base_slot) as usize) * SLOT_SIZE;
        chunk.data[rel..rel + HEAD_KEY_LEN].copy_from_slice(key);
        chunk.data[rel + HEAD_KEY_LEN..rel + SLOT_SIZE].copy_from_slice(&packed.to_le_bytes());
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


fn is_empty_slot(k: &HeadKey, packed: u64) -> bool {
    packed == 0 && *k == [0u8; HEAD_KEY_LEN]
}

/// Merge `fk` into an existing packed slot value (sole or multi-list head).
///
/// - Same fk already present → unchanged packed value.
/// - Sole → different fk: convert to multi (new first, then old).
/// - Multi: prepend if not already in the chain.
fn merge_packed(multi: &MultiList, packed: u64, fk: Fk) -> Result<u64, StoreError> {
    let (is_multi, head) = unpack_value(packed);
    if is_multi {
        if multi.contains(head, fk)? {
            return Ok(packed);
        }
        let new_head = multi.append(fk, head)?;
        return Ok(pack_multi(new_head));
    }
    if head.is_null() {
        return Ok(pack_sole(fk));
    }
    if head == fk {
        return Ok(packed);
    }
    // sole → multi: newest first
    let old_node = multi.append(head, Fk::NULL)?;
    let new_head = multi.append(fk, old_node)?;
    Ok(pack_multi(new_head))
}

enum InsertResult {
    Done {
        prev: Option<Fk>,
        /// True when a previously empty open-address slot became occupied.
        new_slot: bool,
    },
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
        cleanup_hh(&path);
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
        cleanup_hh(&path);
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
        cleanup_hh(&path);
    }

    #[test]
    fn bulk_fill_empty_roundtrip() {
        // Cold materialize path: empty table + pre-size + one insert_many.
        let path = tmp_path();
        let h = HashHead::create_with_slots(&path, DEFAULT_SLOTS).unwrap();
        let mut batch = Vec::new();
        for i in 0u64..20_000 {
            let mut key = [0u8; 32];
            key[0..8].copy_from_slice(&i.to_le_bytes());
            batch.push((key, Fk(i + 1)));
        }
        h.reserve_additional(batch.len() as u64).unwrap();
        assert_eq!(h.occupied(), 0);
        h.insert_many(&batch).unwrap();
        assert_eq!(h.occupied(), 20_000);
        for i in [0u64, 1, 9999, 19_999] {
            let mut key = [0u8; 32];
            key[0..8].copy_from_slice(&i.to_le_bytes());
            assert_eq!(h.get(&key).unwrap(), Some(Fk(i + 1)));
        }
        // Second wave uses RMW path (non-empty).
        let mut more = Vec::new();
        for i in 20_000u64..21_000 {
            let mut key = [0u8; 32];
            key[0..8].copy_from_slice(&i.to_le_bytes());
            more.push((key, Fk(i + 1)));
        }
        h.insert_many(&more).unwrap();
        assert_eq!(h.occupied(), 21_000);
        assert_eq!(h.get(&[0u8; 32]).unwrap(), Some(Fk(1)));
        cleanup_hh(&path);
    }

    #[test]
    fn reserve_additional_jumps_to_target_slots() {
        // Formerly doubled in a loop (log₂ empty rehashes). One jump to capacity.
        let path = tmp_path();
        let h = HashHead::create_with_slots(&path, DEFAULT_SLOTS).unwrap();
        assert_eq!(h.state.lock().unwrap().slots, 64);
        h.reserve_additional(10_000).unwrap();
        let slots = h.state.lock().unwrap().slots;
        // slots_for_keys(10000) = next_pow2(ceil(10000*8/7)) = next_pow2(11429) = 16384
        assert_eq!(slots, 16_384);
        // Second reserve for same size is a no-op (no smaller/equal grow).
        h.reserve_additional(10_000).unwrap();
        assert_eq!(h.state.lock().unwrap().slots, 16_384);
        cleanup_hh(&path);
    }

    #[test]
    fn create_with_slots_sparse_presize() {
        let path = tmp_path();
        let h = HashHead::create_with_slots(&path, 1024).unwrap();
        assert_eq!(h.state.lock().unwrap().slots, 1024);
        assert_eq!(h.occupied(), 0);
        h.insert(&[1u8; 32], Fk(1)).unwrap();
        assert_eq!(h.get(&[1u8; 32]).unwrap(), Some(Fk(1)));
        cleanup_hh(&path);
    }

    #[test]
    fn mainnet_scale_slot_targets() {
        assert_eq!(HeadScale::Tiny.initial_slots(HeadRole::Tx), 64);
        assert!(HeadScale::Mainnet.initial_slots(HeadRole::Tx) >= 64);
        assert!(HeadScale::Mainnet.initial_slots(HeadRole::Header) >= 64);
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
        cleanup_hh(&path);
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
        cleanup_hh(&path);
    }

    fn cleanup_hh(path: &std::path::Path) {
        let _ = std::fs::remove_file(path);
        let mut mlt = path.as_os_str().to_os_string();
        mlt.push(".mlt");
        let _ = std::fs::remove_file(mlt);
    }

    #[test]
    fn prefix_collision_multi_list() {
        let path = tmp_path();
        let h = HashHead::create_with_slots(&path, DEFAULT_SLOTS).unwrap();
        // Same first 16 bytes, different trailing bytes.
        let mut k1 = [0u8; 32];
        k1[0] = 0xab;
        k1[15] = 1;
        k1[16] = 1;
        let mut k2 = k1;
        k2[16] = 2;
        h.insert(&k1, Fk(10)).unwrap();
        h.insert(&k2, Fk(20)).unwrap();
        assert_eq!(h.occupied(), 1); // one prefix slot
        let all1 = h.get_all(&k1).unwrap();
        let all2 = h.get_all(&k2).unwrap();
        assert_eq!(all1, all2);
        assert!(all1.contains(&Fk(10)) && all1.contains(&Fk(20)));
        // Newest first
        assert_eq!(all1[0], Fk(20));
        assert_eq!(h.get(&k1).unwrap(), Some(Fk(20)));
        cleanup_hh(&path);
    }

    #[test]
    fn bip30_same_full_key_multi() {
        let path = tmp_path();
        let h = HashHead::create_with_slots(&path, DEFAULT_SLOTS).unwrap();
        let key = [0x42u8; 32];
        h.insert(&key, Fk(1)).unwrap();
        h.insert(&key, Fk(2)).unwrap(); // same txid, second Class A row
        let all = h.get_all(&key).unwrap();
        assert_eq!(all, vec![Fk(2), Fk(1)]);
        assert_eq!(h.occupied(), 1);
        // Idempotent re-insert of existing fk
        h.insert(&key, Fk(2)).unwrap();
        assert_eq!(h.get_all(&key).unwrap(), vec![Fk(2), Fk(1)]);
        h.flush().unwrap();
        let h2 = HashHead::open(&path).unwrap();
        assert_eq!(h2.get_all(&key).unwrap(), vec![Fk(2), Fk(1)]);
        cleanup_hh(&path);
    }

    #[test]
    fn multi_survives_rehash() {
        let path = tmp_path();
        let h = HashHead::create_with_slots(&path, DEFAULT_SLOTS).unwrap();
        let mut k1 = [0u8; 32];
        k1[0..4].copy_from_slice(&1u32.to_le_bytes());
        let mut k2 = k1;
        k2[20] = 9;
        h.insert(&k1, Fk(100)).unwrap();
        h.insert(&k2, Fk(200)).unwrap();
        // Force growth
        let mut batch = Vec::new();
        for i in 0u64..200 {
            let mut key = [0u8; 32];
            key[0..8].copy_from_slice(&(i + 1000).to_le_bytes());
            batch.push((key, Fk(i + 1)));
        }
        h.insert_many(&batch).unwrap();
        let all = h.get_all(&k1).unwrap();
        assert!(all.contains(&Fk(100)) && all.contains(&Fk(200)));
        cleanup_hh(&path);
    }
}
