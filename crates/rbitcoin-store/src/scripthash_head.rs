//! Open-addressed head for hybrid scripthash: key[16] → value[16] (32 B slots).
//!
//! Key is the first 16 bytes of Electrum SHA256(spk). Public APIs take full 32 B
//! hashes and truncate. Values are [`ShHeadValue`] encodings (two u64s).

use crate::error::StoreError;
use crate::file::{TableFile, FILE_HEADER_LEN};
use crate::hashhead::{initial_slots_for, HeadRole, HeadScale};
use crate::scripthash_layout::{
    head_key_from_full, ShHeadKey, ShHeadValue, SH_HEAD_KEY_LEN, SH_HEAD_SLOT_SIZE,
    SH_HEAD_VALUE_LEN,
};
use crate::sharded_hashhead::{initial_slots_per_shard, shard_count_for_role};
use rbitcoin_primitives::TableKind;
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Mutex;
use std::time::Instant;

use crate::open_address::{self, MAX_LOAD_DEN, MAX_LOAD_NUM};

const DEFAULT_SLOTS: u64 = 64;
const SLOTS_PER_CHUNK: u64 = 128; // 128 × 32 B = 4 KiB
const CHUNK_CACHE_MAX: usize = 256;

pub struct ScriptHashHead {
    file: TableFile,
    state: Mutex<HashState>,
}

struct HashState {
    slots: u64,
    occupied: u64,
}

impl ScriptHashHead {
    pub fn create_with_slots(path: impl Into<PathBuf>, slots: u64) -> Result<Self, StoreError> {
        let slots = slots.max(2).next_power_of_two();
        let file = TableFile::create(path, TableKind::HashHead)?;
        let body_bytes = SH_HEAD_SLOT_SIZE as u64 * slots;
        let need = FILE_HEADER_LEN as u64 + body_bytes;
        file.ensure_capacity(need)?;
        file.set_logical_len(need)?;
        file.zero_range(FILE_HEADER_LEN as u64, body_bytes)?;
        Ok(Self {
            file,
            state: Mutex::new(HashState {
                slots,
                occupied: 0,
            }),
        })
    }

    pub fn open(path: impl Into<PathBuf>) -> Result<Self, StoreError> {
        let file = TableFile::open(path, TableKind::HashHead)?;
        Self::from_file(file)
    }

    fn from_file(file: TableFile) -> Result<Self, StoreError> {
        let body = file.logical_len().saturating_sub(FILE_HEADER_LEN as u64);
        if body % SH_HEAD_SLOT_SIZE as u64 != 0 || body == 0 {
            return Err(StoreError::Corrupt("scripthash head size"));
        }
        let slots = body / SH_HEAD_SLOT_SIZE as u64;
        if !slots.is_power_of_two() {
            return Err(StoreError::Corrupt(
                "scripthash head slots not power of two",
            ));
        }
        let mut occupied = 0u64;
        let mut buf = vec![0u8; SH_HEAD_SLOT_SIZE * 1024];
        let mut slot = 0u64;
        while slot < slots {
            let n = ((slots - slot) as usize).min(1024);
            let off = FILE_HEADER_LEN as u64 + slot * SH_HEAD_SLOT_SIZE as u64;
            let bytes = n * SH_HEAD_SLOT_SIZE;
            file.read_at(off, &mut buf[..bytes])?;
            for i in 0..n {
                let base = i * SH_HEAD_SLOT_SIZE;
                let k: ShHeadKey = buf[base..base + SH_HEAD_KEY_LEN].try_into().unwrap();
                let v: [u8; SH_HEAD_VALUE_LEN] = buf
                    [base + SH_HEAD_KEY_LEN..base + SH_HEAD_SLOT_SIZE]
                    .try_into()
                    .unwrap();
                if !is_empty_slot(&k, &v) {
                    occupied += 1;
                }
            }
            slot += n as u64;
        }
        Ok(Self {
            file,
            state: Mutex::new(HashState { slots, occupied }),
        })
    }

    fn hash_slot(key: &ShHeadKey, slots: u64) -> u64 {
        open_address::primary_slot(key, slots)
    }

    fn slot_file_off(slot: u64) -> u64 {
        FILE_HEADER_LEN as u64 + slot * SH_HEAD_SLOT_SIZE as u64
    }

    fn to_key(full: &[u8; 32]) -> ShHeadKey {
        head_key_from_full(full)
    }

    /// Zero all slots and reset occupied (cold rematerialize after partial load).
    pub fn reinit_empty(&self) -> Result<(), StoreError> {
        let slots = {
            let state = self.state.lock().unwrap();
            state.slots
        };
        let body_bytes = SH_HEAD_SLOT_SIZE as u64 * slots;
        self.file
            .zero_range(FILE_HEADER_LEN as u64, body_bytes)?;
        self.state.lock().unwrap().occupied = 0;
        Ok(())
    }

    pub fn get(&self, full: &[u8; 32]) -> Result<Option<ShHeadValue>, StoreError> {
        let key = Self::to_key(full);
        let slots = self.state.lock().unwrap().slots;
        let mut slot = Self::hash_slot(&key, slots);
        for _ in 0..slots {
            let (k, v) = self.read_slot(slot)?;
            if is_empty_slot(&k, &v) {
                return Ok(None);
            }
            if k == key {
                let val = ShHeadValue::decode(&v)?;
                if val.is_empty() {
                    return Ok(None);
                }
                return Ok(Some(val));
            }
            slot = (slot + 1) & (slots - 1);
        }
        Ok(None)
    }

    fn read_slot(&self, slot: u64) -> Result<(ShHeadKey, [u8; SH_HEAD_VALUE_LEN]), StoreError> {
        let mut buf = [0u8; SH_HEAD_SLOT_SIZE];
        self.file.read_at(Self::slot_file_off(slot), &mut buf)?;
        let k: ShHeadKey = buf[0..SH_HEAD_KEY_LEN].try_into().unwrap();
        let v: [u8; SH_HEAD_VALUE_LEN] =
            buf[SH_HEAD_KEY_LEN..SH_HEAD_SLOT_SIZE].try_into().unwrap();
        Ok((k, v))
    }

    pub fn insert(&self, full: &[u8; 32], value: &ShHeadValue) -> Result<(), StoreError> {
        self.insert_many(&[(*full, value.clone())])
    }

    /// Soft-clear value; keeps probe chain.
    pub fn clear_key(&self, full: &[u8; 32]) -> Result<bool, StoreError> {
        let key = Self::to_key(full);
        let slots = self.state.lock().unwrap().slots;
        let mut slot = Self::hash_slot(&key, slots);
        for _ in 0..slots {
            let (k, v) = self.read_slot(slot)?;
            if is_empty_slot(&k, &v) {
                return Ok(false);
            }
            if k == key {
                let mut buf = [0u8; SH_HEAD_SLOT_SIZE];
                buf[0..SH_HEAD_KEY_LEN].copy_from_slice(&key);
                self.file.write_at(Self::slot_file_off(slot), &buf)?;
                return Ok(true);
            }
            slot = (slot + 1) & (slots - 1);
        }
        Ok(false)
    }

    pub fn insert_many(&self, entries: &[([u8; 32], ShHeadValue)]) -> Result<(), StoreError> {
        if entries.is_empty() {
            return Ok(());
        }
        let mut upserts: Vec<(ShHeadKey, ShHeadValue)> = Vec::with_capacity(entries.len());
        for (full, v) in entries {
            let key = Self::to_key(full);
            if v.is_empty() {
                self.clear_key(full)?;
            } else {
                upserts.push((key, v.clone()));
            }
        }
        if upserts.is_empty() {
            return Ok(());
        }
        self.reserve_additional(upserts.len() as u64)?;

        if self.state.lock().unwrap().occupied == 0 {
            return self.bulk_fill_empty(&upserts);
        }

        let mut work = upserts;
        let slots_now = self.state.lock().unwrap().slots;
        work.sort_unstable_by_key(|(k, _)| Self::hash_slot(k, slots_now));

        let mut i = 0usize;
        while i < work.len() {
            let slots = self.state.lock().unwrap().slots;
            if i > 0 {
                work[i..].sort_unstable_by_key(|(k, _)| Self::hash_slot(k, slots));
            }
            let mut cache = SlotPageCache::new(self, slots);
            let mut need_rehash = false;
            while i < work.len() {
                let (key, ref val) = work[i];
                let enc = val.encode();
                match cache.try_insert(&key, &enc)? {
                    InsertResult::Done(was_empty) => {
                        if was_empty {
                            let mut state = self.state.lock().unwrap();
                            state.occupied = state.occupied.saturating_add(1);
                        }
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

    fn bulk_fill_empty(&self, entries: &[(ShHeadKey, ShHeadValue)]) -> Result<(), StoreError> {
        debug_assert_eq!(self.state.lock().unwrap().occupied, 0);
        let slots = self.state.lock().unwrap().slots;
        let nbytes = (slots as usize).saturating_mul(SH_HEAD_SLOT_SIZE);
        let mut table = vec![0u8; nbytes];
        let mut occupied = 0u64;
        for (key, val) in entries {
            let enc = val.encode();
            let mut slot = Self::hash_slot(key, slots);
            let mut placed = false;
            for _ in 0..slots {
                let off = (slot as usize) * SH_HEAD_SLOT_SIZE;
                let slot_key: ShHeadKey = table[off..off + SH_HEAD_KEY_LEN].try_into().unwrap();
                let slot_v: [u8; SH_HEAD_VALUE_LEN] = table
                    [off + SH_HEAD_KEY_LEN..off + SH_HEAD_SLOT_SIZE]
                    .try_into()
                    .unwrap();
                if is_empty_slot(&slot_key, &slot_v) {
                    table[off..off + SH_HEAD_KEY_LEN].copy_from_slice(key);
                    table[off + SH_HEAD_KEY_LEN..off + SH_HEAD_SLOT_SIZE].copy_from_slice(&enc);
                    occupied = occupied.saturating_add(1);
                    placed = true;
                    break;
                }
                if &slot_key == key {
                    table[off + SH_HEAD_KEY_LEN..off + SH_HEAD_SLOT_SIZE].copy_from_slice(&enc);
                    placed = true;
                    break;
                }
                slot = (slot + 1) & (slots - 1);
            }
            if !placed {
                return Err(StoreError::Corrupt("scripthash head bulk_fill full"));
            }
        }
        self.file.write_at(FILE_HEADER_LEN as u64, &table)?;
        self.state.lock().unwrap().occupied = occupied;
        Ok(())
    }

    fn slots_for_keys(keys: u64) -> u64 {
        if keys == 0 {
            return DEFAULT_SLOTS;
        }
        let min = keys
            .saturating_mul(MAX_LOAD_DEN)
            .div_ceil(MAX_LOAD_NUM)
            .max(1);
        min.next_power_of_two().max(DEFAULT_SLOTS)
    }

    pub fn occupied(&self) -> u64 {
        self.state.lock().unwrap().occupied
    }

    pub fn reserve_additional(&self, additional: u64) -> Result<(), StoreError> {
        if additional == 0 {
            return Ok(());
        }
        let (occupied, slots) = {
            let state = self.state.lock().unwrap();
            (state.occupied, state.slots)
        };
        let need = Self::slots_for_keys(occupied.saturating_add(additional));
        if need > slots {
            if occupied == 0 {
                // Cold bulk: grow empty table without scanning zeros.
                self.grow_empty_to(need)?;
            } else {
                self.rehash_to(need)?;
            }
        }
        Ok(())
    }

    /// Expand an **empty** open-address table to `new_slots` (power of two).
    ///
    /// Used by cold materialize so pre-size is fallocate/zero only — no slot scan.
    fn grow_empty_to(&self, new_slots: u64) -> Result<(), StoreError> {
        let new_slots = new_slots.max(2).next_power_of_two();
        let (old_slots, occupied) = {
            let state = self.state.lock().unwrap();
            (state.slots, state.occupied)
        };
        if occupied != 0 {
            return self.rehash_to(new_slots);
        }
        if new_slots <= old_slots {
            return Ok(());
        }
        let new_bytes = SH_HEAD_SLOT_SIZE as u64 * new_slots;
        let need = FILE_HEADER_LEN as u64 + new_bytes;
        self.file.ensure_capacity(need)?;
        self.file.set_logical_len(need)?;
        // Zero full body (new region may reuse stale bytes past old logical len).
        self.file.zero_range(FILE_HEADER_LEN as u64, new_bytes)?;
        self.state.lock().unwrap().slots = new_slots;
        Ok(())
    }

    fn rehash_to(&self, new_slots: u64) -> Result<(), StoreError> {
        let new_slots = new_slots.max(2).next_power_of_two();
        // Share process-wide gate with HashHead (no stacked multi-table freezes).
        let _rehash_serial = open_address::rehash_gate()
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let (old_slots, occupied) = {
            let state = self.state.lock().unwrap();
            (state.slots, state.occupied)
        };
        if new_slots <= old_slots {
            return Ok(());
        }
        let new_bytes = SH_HEAD_SLOT_SIZE as u64 * new_slots;
        let t0 = Instant::now();

        let mut entries: Vec<(ShHeadKey, [u8; SH_HEAD_VALUE_LEN])> = Vec::new();
        entries
            .try_reserve_exact(occupied as usize)
            .map_err(|_| StoreError::Corrupt("scripthash head rehash OOM"))?;
        let mut buf = vec![0u8; SH_HEAD_SLOT_SIZE * 1024];
        let mut slot = 0u64;
        while slot < old_slots {
            let n = ((old_slots - slot) as usize).min(1024);
            let off = FILE_HEADER_LEN as u64 + slot * SH_HEAD_SLOT_SIZE as u64;
            let bytes = n * SH_HEAD_SLOT_SIZE;
            self.file.read_at(off, &mut buf[..bytes])?;
            for i in 0..n {
                let base = i * SH_HEAD_SLOT_SIZE;
                let k: ShHeadKey = buf[base..base + SH_HEAD_KEY_LEN].try_into().unwrap();
                let v: [u8; SH_HEAD_VALUE_LEN] = buf
                    [base + SH_HEAD_KEY_LEN..base + SH_HEAD_SLOT_SIZE]
                    .try_into()
                    .unwrap();
                if !is_empty_slot(&k, &v) {
                    entries.push((k, v));
                }
            }
            slot += n as u64;
        }

        let need = FILE_HEADER_LEN as u64 + new_bytes;
        self.file.ensure_capacity(need)?;
        self.file.set_logical_len(need)?;
        self.file.zero_range(FILE_HEADER_LEN as u64, new_bytes)?;
        {
            let mut state = self.state.lock().unwrap();
            state.slots = new_slots;
            state.occupied = 0;
        }

        entries.sort_unstable_by_key(|(k, _)| Self::hash_slot(k, new_slots));
        let n_entries = entries.len() as u64;
        let mut cache = SlotPageCache::new(self, new_slots);
        for (k, v) in &entries {
            match cache.try_insert(k, v)? {
                InsertResult::Done(_) => {}
                InsertResult::NeedRehash => {
                    cache.flush()?;
                    return Err(StoreError::Corrupt("scripthash head rehash failed"));
                }
            }
        }
        cache.flush()?;
        self.state.lock().unwrap().occupied = n_entries;
        rbitcoin_log::trace!(
            "store: scripthash head rehash path={} {}→{} slots occupied={} elapsed={:?}",
            self.file.path().display(),
            old_slots,
            new_slots,
            n_entries,
            t0.elapsed()
        );
        Ok(())
    }

    /// Visit every occupied non-empty head value (key is zero-padded to 32 B for API).
    pub fn for_each_occupied(
        &self,
        mut f: impl FnMut([u8; 32], ShHeadValue) -> Result<(), StoreError>,
    ) -> Result<(), StoreError> {
        let slots = self.state.lock().unwrap().slots;
        let mut buf = vec![0u8; SH_HEAD_SLOT_SIZE * 1024];
        let mut slot = 0u64;
        while slot < slots {
            let n = ((slots - slot) as usize).min(1024);
            let off = FILE_HEADER_LEN as u64 + slot * SH_HEAD_SLOT_SIZE as u64;
            let bytes = n * SH_HEAD_SLOT_SIZE;
            self.file.read_at(off, &mut buf[..bytes])?;
            for i in 0..n {
                let base = i * SH_HEAD_SLOT_SIZE;
                let k: ShHeadKey = buf[base..base + SH_HEAD_KEY_LEN].try_into().unwrap();
                let v: [u8; SH_HEAD_VALUE_LEN] = buf
                    [base + SH_HEAD_KEY_LEN..base + SH_HEAD_SLOT_SIZE]
                    .try_into()
                    .unwrap();
                if is_empty_slot(&k, &v) {
                    continue;
                }
                let val = ShHeadValue::decode(&v)?;
                if !val.is_empty() {
                    let mut full = [0u8; 32];
                    full[0..SH_HEAD_KEY_LEN].copy_from_slice(&k);
                    f(full, val)?;
                }
            }
            slot += n as u64;
        }
        Ok(())
    }

    pub fn flush(&self) -> Result<(), StoreError> {
        self.file.flush()
    }

    pub fn flush_async(&self) -> Result<(), StoreError> {
        self.file.flush_async()
    }
}

fn is_empty_slot(k: &ShHeadKey, v: &[u8; SH_HEAD_VALUE_LEN]) -> bool {
    *k == [0u8; SH_HEAD_KEY_LEN] && *v == [0u8; SH_HEAD_VALUE_LEN]
}

enum InsertResult {
    Done(bool),
    NeedRehash,
}

struct SlotPageCache<'a> {
    head: &'a ScriptHashHead,
    slots: u64,
    chunks: BTreeMap<u64, CachedChunk>,
}

struct CachedChunk {
    base_slot: u64,
    data: Vec<u8>,
    dirty: bool,
}

impl<'a> SlotPageCache<'a> {
    fn new(head: &'a ScriptHashHead, slots: u64) -> Self {
        Self {
            head,
            slots,
            chunks: BTreeMap::new(),
        }
    }

    fn try_insert(
        &mut self,
        key: &ShHeadKey,
        value: &[u8; SH_HEAD_VALUE_LEN],
    ) -> Result<InsertResult, StoreError> {
        let mut slot = ScriptHashHead::hash_slot(key, self.slots);
        for _ in 0..self.slots {
            let (k, old_v) = self.read_slot(slot)?;
            if is_empty_slot(&k, &old_v) {
                self.write_slot(slot, key, value)?;
                return Ok(InsertResult::Done(true));
            }
            if &k == key {
                self.write_slot(slot, key, value)?;
                return Ok(InsertResult::Done(false));
            }
            slot = (slot + 1) & (self.slots - 1);
        }
        Ok(InsertResult::NeedRehash)
    }

    fn read_slot(&mut self, slot: u64) -> Result<(ShHeadKey, [u8; SH_HEAD_VALUE_LEN]), StoreError> {
        let chunk = self.ensure_chunk(slot)?;
        let rel = ((slot - chunk.base_slot) as usize) * SH_HEAD_SLOT_SIZE;
        let k: ShHeadKey = chunk.data[rel..rel + SH_HEAD_KEY_LEN].try_into().unwrap();
        let v: [u8; SH_HEAD_VALUE_LEN] = chunk.data[rel + SH_HEAD_KEY_LEN..rel + SH_HEAD_SLOT_SIZE]
            .try_into()
            .unwrap();
        Ok((k, v))
    }

    fn write_slot(
        &mut self,
        slot: u64,
        key: &ShHeadKey,
        value: &[u8; SH_HEAD_VALUE_LEN],
    ) -> Result<(), StoreError> {
        let chunk = self.ensure_chunk(slot)?;
        let rel = ((slot - chunk.base_slot) as usize) * SH_HEAD_SLOT_SIZE;
        chunk.data[rel..rel + SH_HEAD_KEY_LEN].copy_from_slice(key);
        chunk.data[rel + SH_HEAD_KEY_LEN..rel + SH_HEAD_SLOT_SIZE].copy_from_slice(value);
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
            let off = ScriptHashHead::slot_file_off(base_slot);
            let len = n * SH_HEAD_SLOT_SIZE;
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
        for (_, chunk) in self.chunks.iter_mut() {
            if chunk.dirty {
                let off = ScriptHashHead::slot_file_off(chunk.base_slot);
                self.head.file.write_at(off, &chunk.data)?;
                chunk.dirty = false;
            }
        }
        self.chunks.clear();
        Ok(())
    }
}

/// Sharded facade (16-way mainnet) over [`ScriptHashHead`].
pub struct ShardedScriptHashHead {
    shards: Vec<ScriptHashHead>,
}

impl ShardedScriptHashHead {
    pub fn create_for_role(path: impl Into<PathBuf>, role: HeadRole) -> Result<Self, StoreError> {
        debug_assert_eq!(role, HeadRole::ScriptHash);
        Self::create_sharded(
            path,
            shard_count_for_role(role),
            initial_slots_per_shard(role),
        )
    }

    pub fn create_sharded(
        path: impl Into<PathBuf>,
        shard_count: usize,
        slots_each: u64,
    ) -> Result<Self, StoreError> {
        let path = path.into();
        let n = shard_count.max(1);
        let per = slots_each.max(2).next_power_of_two();
        if n == 1 {
            let h = ScriptHashHead::create_with_slots(&path, per)?;
            return Ok(Self { shards: vec![h] });
        }
        if path.exists() {
            return Err(StoreError::io(
                &path,
                std::io::Error::new(
                    std::io::ErrorKind::AlreadyExists,
                    "sharded scripthash head path exists",
                ),
            ));
        }
        std::fs::create_dir_all(&path).map_err(|e| StoreError::io(&path, e))?;
        let mut shards = Vec::with_capacity(n);
        for i in 0..n {
            let shard_path = path.join(format!("{i:02x}"));
            shards.push(ScriptHashHead::create_with_slots(shard_path, per)?);
        }
        let _ = HeadScale::from_env();
        let _ = initial_slots_for(HeadRole::ScriptHash);
        Ok(Self { shards })
    }

    /// Zero every shard (cold rematerialize).
    pub fn reinit_empty(&self) -> Result<(), StoreError> {
        for s in &self.shards {
            s.reinit_empty()?;
        }
        Ok(())
    }

    pub fn open_for_role(path: impl Into<PathBuf>, _role: HeadRole) -> Result<Self, StoreError> {
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
                return Err(StoreError::Corrupt("sharded scripthash head empty"));
            }
            let mut shards = Vec::with_capacity(names.len());
            for (i, name) in names.iter().enumerate() {
                let expect = format!("{i:02x}");
                if name != &expect {
                    return Err(StoreError::Corrupt(
                        "sharded scripthash head unexpected shard name",
                    ));
                }
                shards.push(ScriptHashHead::open(path.join(name))?);
            }
            return Ok(Self { shards });
        }
        if path.is_file() {
            return Ok(Self {
                shards: vec![ScriptHashHead::open(path)?],
            });
        }
        Err(StoreError::io(
            &path,
            std::io::Error::new(std::io::ErrorKind::NotFound, "scripthash head missing"),
        ))
    }

    #[inline]
    fn shard_of(&self, full: &[u8; 32]) -> usize {
        let n = self.shards.len();
        if n == 1 {
            0
        } else {
            (full[0] as usize) % n
        }
    }

    pub fn get(&self, key: &[u8; 32]) -> Result<Option<ShHeadValue>, StoreError> {
        self.shards[self.shard_of(key)].get(key)
    }

    pub fn insert(&self, key: &[u8; 32], value: &ShHeadValue) -> Result<(), StoreError> {
        self.shards[self.shard_of(key)].insert(key, value)
    }

    pub fn clear_key(&self, key: &[u8; 32]) -> Result<bool, StoreError> {
        self.shards[self.shard_of(key)].clear_key(key)
    }

    pub fn shard_count(&self) -> usize {
        self.shards.len()
    }

    /// Shard index for a full Electrum scripthash (same as insert routing).
    #[inline]
    pub fn shard_index(&self, full: &[u8; 32]) -> usize {
        self.shard_of(full)
    }

    /// True when every shard has zero occupied slots (cold bulk fill precondition).
    pub fn is_empty(&self) -> bool {
        self.shards.iter().all(|s| s.occupied() == 0)
    }

    /// Pre-size shards for an upcoming bulk insert (`additional` keys globally).
    pub fn reserve_additional(&self, additional: u64) -> Result<(), StoreError> {
        if self.shards.is_empty() {
            return Ok(());
        }
        // Spread estimate evenly; each shard grows if needed.
        let per = additional.div_ceil(self.shards.len() as u64).max(1);
        for s in &self.shards {
            s.reserve_additional(per)?;
        }
        Ok(())
    }

    /// Pre-size every **empty** shard so a single RAM [`ScriptHashHead::insert_many`]
    /// bulk-fill can place `expected_keys` (global) without mid-fill rehash.
    ///
    /// Inflates by 25% then even-splits across shards (margin for `key[0] % n` skew).
    /// Slot table bytes = `slots × 32`; that is the peak RAM for one
    /// `bulk_fill_empty` (one shard at a time at finish).
    pub fn reserve_for_cold_bulk(&self, expected_keys: u64) -> Result<(), StoreError> {
        if expected_keys == 0 {
            return Ok(());
        }
        if !self.is_empty() {
            return Err(StoreError::Corrupt(
                "scripthash head reserve_for_cold_bulk: head not empty",
            ));
        }
        let inflated = expected_keys.saturating_mul(5).div_ceil(4).max(1);
        self.reserve_additional(inflated)
    }

    /// Cold materialize: one empty-table bulk fill per shard, releasing each
    /// bucket before the next so only one full shard image is in RAM.
    ///
    /// `buckets[i]` must contain only keys for shard `i`. Consumes the vectors.
    /// Requires every shard to be empty (call after reinit / fresh create and
    /// [`reserve_for_cold_bulk`]).
    pub fn bulk_fill_shards_cold(
        &self,
        buckets: &mut [Vec<([u8; 32], ShHeadValue)>],
    ) -> Result<(), StoreError> {
        if buckets.len() != self.shards.len() {
            return Err(StoreError::Corrupt(
                "scripthash bulk_fill_shards_cold: bucket/shard count mismatch",
            ));
        }
        for (i, s) in self.shards.iter().enumerate() {
            if s.occupied() != 0 {
                return Err(StoreError::Corrupt(
                    "scripthash bulk_fill_shards_cold: shard not empty",
                ));
            }
            let mut bucket = std::mem::take(&mut buckets[i]);
            if bucket.is_empty() {
                continue;
            }
            // Ensure capacity for this shard's actual key count (skew).
            s.reserve_additional(bucket.len() as u64)?;
            // insert_many → bulk_fill_empty while occupied==0: one RAM table + write.
            s.insert_many(&bucket)?;
            // Drop entries before building the next shard image.
            bucket.clear();
            bucket.shrink_to_fit();
            drop(bucket);
        }
        Ok(())
    }

    /// Insert head values, applying **one shard at a time** (sorted within shard).
    ///
    /// When `flush_each_shard` is true (large materialize runs), flush the shard
    /// file after its bucket so the working set does not keep every shard dirty
    /// at once. Small runs skip the per-shard flush and rely on later table flush.
    pub fn insert_many_sharded(
        &self,
        entries: &[([u8; 32], ShHeadValue)],
        flush_each_shard: bool,
    ) -> Result<(), StoreError> {
        if entries.is_empty() {
            return Ok(());
        }
        let n = self.shards.len();
        if n == 1 {
            self.shards[0].reserve_additional(entries.len() as u64)?;
            self.shards[0].insert_many(entries)?;
            if flush_each_shard {
                self.shards[0].flush()?;
            }
            return Ok(());
        }
        let mut buckets: Vec<Vec<([u8; 32], ShHeadValue)>> = (0..n).map(|_| Vec::new()).collect();
        for (k, v) in entries {
            buckets[self.shard_of(k)].push((*k, v.clone()));
        }
        for (i, mut bucket) in buckets.into_iter().enumerate() {
            if bucket.is_empty() {
                continue;
            }
            // Stable order within shard for probe locality.
            bucket.sort_unstable_by(|a, b| a.0.cmp(&b.0));
            self.shards[i].reserve_additional(bucket.len() as u64)?;
            self.shards[i].insert_many(&bucket)?;
            if flush_each_shard {
                // Release dirty pages for this shard before touching the next.
                self.shards[i].flush()?;
            }
        }
        Ok(())
    }

    pub fn for_each_occupied(
        &self,
        mut f: impl FnMut([u8; 32], ShHeadValue) -> Result<(), StoreError>,
    ) -> Result<(), StoreError> {
        for shard in &self.shards {
            shard.for_each_occupied(&mut f)?;
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
    use crate::scripthash_layout::ShEntry;
    use rbitcoin_primitives::Fk;

    #[test]
    fn head_insert_get_clear() {
        let path = std::env::temp_dir().join(format!(
            "rbitcoin-shhead-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let _ = std::fs::remove_file(&path);
        let h = ScriptHashHead::create_with_slots(&path, 64).unwrap();
        let mut key = [0u8; 32];
        key[0] = 7;
        let val = ShHeadValue::inline_one(ShEntry::new(Fk(42)));
        h.insert(&key, &val).unwrap();
        assert_eq!(h.get(&key).unwrap().unwrap(), val);
        assert!(h.clear_key(&key).unwrap());
        assert!(h.get(&key).unwrap().is_none());
        let _ = std::fs::remove_file(&path);
    }
}
