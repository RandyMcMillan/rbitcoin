//! Growable hash head: key (32 bytes) → first record fk (u64).
//!
//! Linear probing over a power-of-two slot table. Rehashes (doubles slots) when
//! load factor exceeds [`MAX_LOAD_NUM`]/[`MAX_LOAD_DEN`].

use crate::error::StoreError;
use crate::file::{TableFile, FILE_HEADER_LEN};
use parking_lot::Mutex;
use rbitcoin_primitives::{Fk, TableKind};

const SLOT_SIZE: usize = 40; // 32 key + 8 fk
const DEFAULT_SLOTS: u64 = 64;
/// Rehash when occupied/slots > 1/2 (keeps linear probe short; avoids "full on get").
const MAX_LOAD_NUM: u64 = 1;
const MAX_LOAD_DEN: u64 = 2;

pub struct HashHead {
    file: TableFile,
    /// (slot count, occupied count)
    state: Mutex<HashState>,
}

#[derive(Clone, Copy, Debug)]
struct HashState {
    slots: u64,
    occupied: u64,
}

impl HashHead {
    pub fn create(path: impl Into<std::path::PathBuf>) -> Result<Self, StoreError> {
        let file = TableFile::create(path, TableKind::HashHead)?;
        let bytes = SLOT_SIZE as u64 * DEFAULT_SLOTS;
        let zeros = vec![0u8; bytes as usize];
        file.write_at(FILE_HEADER_LEN as u64, &zeros)?;
        Ok(Self {
            file,
            state: Mutex::new(HashState {
                slots: DEFAULT_SLOTS,
                occupied: 0,
            }),
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
        // Count occupied for load-factor tracking.
        let mut occupied = 0u64;
        let tmp = Self {
            file,
            state: Mutex::new(HashState { slots, occupied: 0 }),
        };
        for slot in 0..slots {
            let (k, fk) = tmp.read_slot(slot)?;
            if !is_empty_slot(&k, fk) {
                occupied += 1;
            }
        }
        *tmp.state.lock() = HashState { slots, occupied };
        Ok(tmp)
    }

    fn slot_offset(slot: u64) -> u64 {
        FILE_HEADER_LEN as u64 + slot * SLOT_SIZE as u64
    }

    fn hash_slot(key: &[u8; 32], slots: u64) -> u64 {
        let mut h: u64 = 0xcbf29ce484222325;
        for b in key {
            h ^= u64::from(*b);
            h = h.wrapping_mul(0x100000001b3);
        }
        h & (slots - 1)
    }

    fn read_slot(&self, slot: u64) -> Result<([u8; 32], u64), StoreError> {
        let mut buf = [0u8; SLOT_SIZE];
        self.file.read_at(Self::slot_offset(slot), &mut buf)?;
        let k: [u8; 32] = buf[0..32].try_into().unwrap();
        let fk = u64::from_le_bytes(buf[32..40].try_into().unwrap());
        Ok((k, fk))
    }

    fn write_slot(&self, slot: u64, key: &[u8; 32], fk: u64) -> Result<(), StoreError> {
        let mut out = [0u8; SLOT_SIZE];
        out[0..32].copy_from_slice(key);
        out[32..40].copy_from_slice(&fk.to_le_bytes());
        self.file.write_at(Self::slot_offset(slot), &out)
    }

    pub fn get(&self, key: &[u8; 32]) -> Result<Option<Fk>, StoreError> {
        // Hold state lock for the full probe so we never observe a mid-rehash table.
        let state = self.state.lock();
        let slots = state.slots;
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
        // Table at capacity without an empty probe slot — key is not present.
        Ok(None)
    }

    /// Insert or replace mapping. Grows the table when load factor is high.
    pub fn insert(&self, key: &[u8; 32], fk: Fk) -> Result<Option<Fk>, StoreError> {
        debug_assert!(!fk.is_null());
        let mut prev = None;
        self.insert_many_with(&[( *key, fk )], |p| prev = p)?;
        Ok(prev)
    }

    /// Insert many mappings, rehashing as needed. Prefer this over N×[`insert`]
    /// during multi-tx archive (one capacity plan, fewer lock churn).
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
        let mut i = 0usize;
        while i < entries.len() {
            // Grow until remaining entries fit load factor (worst case all new).
            loop {
                let state = self.state.lock();
                let remaining = (entries.len() - i) as u64;
                if state
                    .occupied
                    .saturating_add(remaining)
                    .saturating_mul(MAX_LOAD_DEN)
                    < state.slots.saturating_mul(MAX_LOAD_NUM)
                {
                    break;
                }
                drop(state);
                self.rehash()?;
            }
            let mut state = self.state.lock();
            while i < entries.len() {
                let (key, fk) = &entries[i];
                debug_assert!(!fk.is_null());
                match self.try_insert_locked(&mut state, key, *fk)? {
                    InsertResult::Done(prev) => {
                        on_prev(prev);
                        i += 1;
                    }
                    InsertResult::NeedRehash => {
                        drop(state);
                        self.rehash()?;
                        break;
                    }
                }
            }
        }
        Ok(())
    }

    fn try_insert_locked(
        &self,
        state: &mut HashState,
        key: &[u8; 32],
        fk: Fk,
    ) -> Result<InsertResult, StoreError> {
        let slots = state.slots;
        let mut slot = Self::hash_slot(key, slots);
        for _ in 0..slots {
            let (k, old_fk) = self.read_slot(slot)?;
            if is_empty_slot(&k, old_fk) {
                self.write_slot(slot, key, fk.0)?;
                state.occupied = state.occupied.saturating_add(1);
                return Ok(InsertResult::Done(None));
            }
            if &k == key {
                self.write_slot(slot, key, fk.0)?;
                return Ok(InsertResult::Done(Fk::new(old_fk)));
            }
            slot = (slot + 1) & (slots - 1);
        }
        Ok(InsertResult::NeedRehash)
    }

    fn rehash(&self) -> Result<(), StoreError> {
        let mut state = self.state.lock();
        let old_slots = state.slots;
        let new_slots = old_slots.saturating_mul(2).max(2);
        // Collect live entries.
        let mut entries: Vec<([u8; 32], u64)> = Vec::new();
        for slot in 0..old_slots {
            let (k, fk) = self.read_slot(slot)?;
            if !is_empty_slot(&k, fk) {
                entries.push((k, fk));
            }
        }
        // Resize file region to new slot table (zeroed in chunks — a single
        // multi-hundred-MB `vec![0; n]` during IBD was a major pause / OOM risk
        // once tx.head grew past a few hundred MB).
        let new_bytes = SLOT_SIZE as u64 * new_slots;
        const CHUNK: usize = 1024 * 1024;
        let mut offset = FILE_HEADER_LEN as u64;
        let mut remaining = new_bytes;
        let mut zeros = vec![0u8; CHUNK.min(new_bytes as usize).max(1)];
        while remaining > 0 {
            let n = remaining.min(zeros.len() as u64) as usize;
            if zeros.len() != n {
                zeros.resize(n, 0);
            }
            self.file.write_at(offset, &zeros[..n])?;
            offset += n as u64;
            remaining -= n as u64;
        }
        self.file
            .set_logical_len(FILE_HEADER_LEN as u64 + new_bytes)?;
        state.slots = new_slots;
        state.occupied = 0;
        // Reinsert under the same lock (new table empty; load is low).
        for (k, fk) in entries {
            match self.try_insert_locked(&mut state, &k, Fk(fk))? {
                InsertResult::Done(_) => {}
                InsertResult::NeedRehash => {
                    return Err(StoreError::Corrupt("hash rehash failed"));
                }
            }
        }
        Ok(())
    }

    pub fn flush(&self) -> Result<(), StoreError> {
        self.file.flush()
    }
}

fn is_empty_slot(k: &[u8; 32], fk: u64) -> bool {
    fk == 0 && *k == [0u8; 32]
}

enum InsertResult {
    Done(Option<Fk>),
    NeedRehash,
}
