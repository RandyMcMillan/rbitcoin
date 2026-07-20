//! Open-addressed head for hybrid scripthash: key[32] → value[32] (64 B slots).
//!
//! Same linear-probe / rehash policy as [`crate::hashhead::HashHead`], but values
//! are full [`ShHeadValue`] encodings rather than a single `Fk`. No write-behind
//! overlay (SH confirm path uses direct paced inserts).

use crate::error::StoreError;
use crate::file::{TableFile, FILE_HEADER_LEN};
use crate::hashhead::{initial_slots_for, HeadRole, HeadScale};
use crate::scripthash_layout::{ShHeadValue, SH_HEAD_SLOT_SIZE, SH_HEAD_VALUE_LEN};
use crate::sharded_hashhead::{initial_slots_per_shard, shard_count_for_role};
use rbitcoin_primitives::TableKind;
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Mutex;
use std::time::Instant;

const DEFAULT_SLOTS: u64 = 64;
const MAX_LOAD_NUM: u64 = 7;
const MAX_LOAD_DEN: u64 = 8;
const SLOTS_PER_CHUNK: u64 = 64; // 64 × 64 B = 4 KiB
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
    pub fn create_with_slots(
        path: impl Into<PathBuf>,
        slots: u64,
    ) -> Result<Self, StoreError> {
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
            return Err(StoreError::Corrupt("scripthash head slots not power of two"));
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
                let k: [u8; 32] = buf[base..base + 32].try_into().unwrap();
                let v: [u8; SH_HEAD_VALUE_LEN] =
                    buf[base + 32..base + SH_HEAD_SLOT_SIZE].try_into().unwrap();
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

    fn hash_slot(key: &[u8; 32], slots: u64) -> u64 {
        let mut h: u64 = 0xcbf29ce484222325;
        for b in key {
            h ^= u64::from(*b);
            h = h.wrapping_mul(0x100000001b3);
        }
        h & (slots - 1)
    }

    fn slot_file_off(slot: u64) -> u64 {
        FILE_HEADER_LEN as u64 + slot * SH_HEAD_SLOT_SIZE as u64
    }

    pub fn get(&self, key: &[u8; 32]) -> Result<Option<ShHeadValue>, StoreError> {
        let slots = self.state.lock().unwrap().slots;
        let mut slot = Self::hash_slot(key, slots);
        for _ in 0..slots {
            let (k, v) = self.read_slot(slot)?;
            if is_empty_slot(&k, &v) {
                return Ok(None);
            }
            if &k == key {
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

    fn read_slot(&self, slot: u64) -> Result<([u8; 32], [u8; SH_HEAD_VALUE_LEN]), StoreError> {
        let mut buf = [0u8; SH_HEAD_SLOT_SIZE];
        self.file.read_at(Self::slot_file_off(slot), &mut buf)?;
        let k: [u8; 32] = buf[0..32].try_into().unwrap();
        let v: [u8; SH_HEAD_VALUE_LEN] = buf[32..SH_HEAD_SLOT_SIZE].try_into().unwrap();
        Ok((k, v))
    }

    pub fn insert(&self, key: &[u8; 32], value: &ShHeadValue) -> Result<(), StoreError> {
        self.insert_many(&[(*key, value.clone())])
    }

    /// Remove creates for `key` (soft-clear value; keeps probe chain).
    pub fn clear_key(&self, key: &[u8; 32]) -> Result<bool, StoreError> {
        let slots = self.state.lock().unwrap().slots;
        let mut slot = Self::hash_slot(key, slots);
        for _ in 0..slots {
            let (k, v) = self.read_slot(slot)?;
            if is_empty_slot(&k, &v) {
                return Ok(false);
            }
            if &k == key {
                // Soft-delete: keep key, zero value so probe chains stay valid.
                let mut buf = [0u8; SH_HEAD_SLOT_SIZE];
                buf[0..32].copy_from_slice(key);
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
        // Filter empties → clear_key instead
        let mut upserts = Vec::with_capacity(entries.len());
        for (k, v) in entries {
            if v.is_empty() {
                self.clear_key(k)?;
            } else {
                upserts.push((*k, v.clone()));
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

    /// Cold first load: build open-addressing table in RAM, one sequential write.
    fn bulk_fill_empty(&self, entries: &[([u8; 32], ShHeadValue)]) -> Result<(), StoreError> {
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
                let slot_key: [u8; 32] = table[off..off + 32].try_into().unwrap();
                let slot_v: [u8; SH_HEAD_VALUE_LEN] =
                    table[off + 32..off + SH_HEAD_SLOT_SIZE].try_into().unwrap();
                if is_empty_slot(&slot_key, &slot_v) {
                    table[off..off + 32].copy_from_slice(key);
                    table[off + 32..off + SH_HEAD_SLOT_SIZE].copy_from_slice(&enc);
                    occupied = occupied.saturating_add(1);
                    placed = true;
                    break;
                }
                if &slot_key == key {
                    table[off + 32..off + SH_HEAD_SLOT_SIZE].copy_from_slice(&enc);
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
            self.rehash_to(need)?;
        }
        Ok(())
    }

    fn rehash_to(&self, new_slots: u64) -> Result<(), StoreError> {
        let new_slots = new_slots.max(2).next_power_of_two();
        let (old_slots, occupied) = {
            let state = self.state.lock().unwrap();
            (state.slots, state.occupied)
        };
        if new_slots <= old_slots {
            return Ok(());
        }
        let new_bytes = SH_HEAD_SLOT_SIZE as u64 * new_slots;
        let t0 = Instant::now();

        let mut entries: Vec<([u8; 32], [u8; SH_HEAD_VALUE_LEN])> = Vec::new();
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
                let k: [u8; 32] = buf[base..base + 32].try_into().unwrap();
                let v: [u8; SH_HEAD_VALUE_LEN] =
                    buf[base + 32..base + SH_HEAD_SLOT_SIZE].try_into().unwrap();
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

    /// Visit every occupied non-empty head value.
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
                let k: [u8; 32] = buf[base..base + 32].try_into().unwrap();
                let v: [u8; SH_HEAD_VALUE_LEN] =
                    buf[base + 32..base + SH_HEAD_SLOT_SIZE].try_into().unwrap();
                if is_empty_slot(&k, &v) {
                    continue;
                }
                let val = ShHeadValue::decode(&v)?;
                if !val.is_empty() {
                    f(k, val)?;
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

fn is_empty_slot(k: &[u8; 32], v: &[u8; SH_HEAD_VALUE_LEN]) -> bool {
    *k == [0u8; 32] && *v == [0u8; SH_HEAD_VALUE_LEN]
}

enum InsertResult {
    /// `true` if the slot was previously empty (new key).
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
        key: &[u8; 32],
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

    fn read_slot(
        &mut self,
        slot: u64,
    ) -> Result<([u8; 32], [u8; SH_HEAD_VALUE_LEN]), StoreError> {
        let chunk = self.ensure_chunk(slot)?;
        let rel = ((slot - chunk.base_slot) as usize) * SH_HEAD_SLOT_SIZE;
        let k: [u8; 32] = chunk.data[rel..rel + 32].try_into().unwrap();
        let v: [u8; SH_HEAD_VALUE_LEN] = chunk.data[rel + 32..rel + SH_HEAD_SLOT_SIZE]
            .try_into()
            .unwrap();
        Ok((k, v))
    }

    fn write_slot(
        &mut self,
        slot: u64,
        key: &[u8; 32],
        value: &[u8; SH_HEAD_VALUE_LEN],
    ) -> Result<(), StoreError> {
        let chunk = self.ensure_chunk(slot)?;
        let rel = ((slot - chunk.base_slot) as usize) * SH_HEAD_SLOT_SIZE;
        chunk.data[rel..rel + 32].copy_from_slice(key);
        chunk.data[rel + 32..rel + SH_HEAD_SLOT_SIZE].copy_from_slice(value);
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
        let _ = HeadScale::from_env(); // ensure env resolved for tests
        let _ = initial_slots_for(HeadRole::ScriptHash);
        Ok(Self { shards })
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
    fn shard_of(&self, key: &[u8; 32]) -> usize {
        let n = self.shards.len();
        if n == 1 {
            0
        } else {
            (key[0] as usize) % n
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

    pub fn insert_many(&self, entries: &[([u8; 32], ShHeadValue)]) -> Result<(), StoreError> {
        if entries.is_empty() {
            return Ok(());
        }
        let n = self.shards.len();
        if n == 1 {
            return self.shards[0].insert_many(entries);
        }
        let mut buckets: Vec<Vec<([u8; 32], ShHeadValue)>> = (0..n).map(|_| Vec::new()).collect();
        for (k, v) in entries {
            buckets[self.shard_of(k)].push((*k, v.clone()));
        }
        for (i, bucket) in buckets.into_iter().enumerate() {
            if !bucket.is_empty() {
                self.shards[i].insert_many(&bucket)?;
            }
        }
        Ok(())
    }

    pub fn insert_many_paced(&self, entries: &[([u8; 32], ShHeadValue)]) -> Result<(), StoreError> {
        self.insert_many(entries)
    }

    /// Pre-size shards for a following bulk head insert (run materialize).
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

    fn tmp() -> PathBuf {
        std::env::temp_dir().join(format!(
            "rbitcoin-shh-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ))
    }

    #[test]
    fn head_insert_get_clear() {
        let path = tmp();
        let h = ScriptHashHead::create_with_slots(&path, 64).unwrap();
        let mut key = [0u8; 32];
        key[0] = 7;
        let v = ShHeadValue::inline_one(ShEntry::new(Fk(9), 1));
        h.insert(&key, &v).unwrap();
        assert_eq!(h.get(&key).unwrap().unwrap(), v);
        h.clear_key(&key).unwrap();
        assert!(h.get(&key).unwrap().is_none());
        let _ = std::fs::remove_file(&path);
    }
}
