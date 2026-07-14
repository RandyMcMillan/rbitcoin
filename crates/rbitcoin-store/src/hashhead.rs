//! Growable hash head: key (32 bytes) → first record fk (u64).
//!
//! Linear probing over a power-of-two slot table. Rehashes (doubles slots) when full.

use crate::error::StoreError;
use crate::file::{TableFile, FILE_HEADER_LEN};
use parking_lot::Mutex;
use rbitcoin_primitives::{Fk, TableKind};

const SLOT_SIZE: usize = 40; // 32 key + 8 fk
const DEFAULT_SLOTS: u64 = 64;

pub struct HashHead {
    file: TableFile,
    slots: Mutex<u64>,
}

impl HashHead {
    pub fn create(path: impl Into<std::path::PathBuf>) -> Result<Self, StoreError> {
        let file = TableFile::create(path, TableKind::HashHead)?;
        let bytes = SLOT_SIZE as u64 * DEFAULT_SLOTS;
        let zeros = vec![0u8; bytes as usize];
        file.write_at(FILE_HEADER_LEN as u64, &zeros)?;
        Ok(Self {
            file,
            slots: Mutex::new(DEFAULT_SLOTS),
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
        Ok(Self {
            file,
            slots: Mutex::new(slots),
        })
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
        let slots = *self.slots.lock();
        let mut slot = Self::hash_slot(key, slots);
        for _ in 0..slots {
            let (k, fk) = self.read_slot(slot)?;
            if fk == 0 && k == [0u8; 32] {
                return Ok(None);
            }
            if &k == key {
                return Ok(Fk::new(fk));
            }
            slot = (slot + 1) & (slots - 1);
        }
        // Table full of non-matching keys — should not happen if we rehash on insert.
        Err(StoreError::Corrupt("hash head full on get"))
    }

    /// Insert or replace mapping. Grows the table when full.
    pub fn insert(&self, key: &[u8; 32], fk: Fk) -> Result<Option<Fk>, StoreError> {
        debug_assert!(!fk.is_null());
        loop {
            match self.try_insert(key, fk)? {
                InsertResult::Done(prev) => return Ok(prev),
                InsertResult::NeedRehash => self.rehash()?,
            }
        }
    }

    fn try_insert(&self, key: &[u8; 32], fk: Fk) -> Result<InsertResult, StoreError> {
        let slots = *self.slots.lock();
        let mut slot = Self::hash_slot(key, slots);
        for _ in 0..slots {
            let off_slot = slot;
            let (k, old_fk) = self.read_slot(off_slot)?;
            if old_fk == 0 && k == [0u8; 32] {
                self.write_slot(off_slot, key, fk.0)?;
                return Ok(InsertResult::Done(None));
            }
            if &k == key {
                self.write_slot(off_slot, key, fk.0)?;
                return Ok(InsertResult::Done(Fk::new(old_fk)));
            }
            slot = (slot + 1) & (slots - 1);
        }
        Ok(InsertResult::NeedRehash)
    }

    fn rehash(&self) -> Result<(), StoreError> {
        let mut slots_guard = self.slots.lock();
        let old_slots = *slots_guard;
        let new_slots = old_slots.saturating_mul(2).max(2);
        // Collect live entries.
        let mut entries: Vec<([u8; 32], u64)> = Vec::new();
        for slot in 0..old_slots {
            let (k, fk) = self.read_slot(slot)?;
            if fk != 0 || k != [0u8; 32] {
                entries.push((k, fk));
            }
        }
        // Resize file region to new slot table (zeroed).
        let new_bytes = SLOT_SIZE as u64 * new_slots;
        let zeros = vec![0u8; new_bytes as usize];
        self.file.write_at(FILE_HEADER_LEN as u64, &zeros)?;
        self.file
            .set_logical_len(FILE_HEADER_LEN as u64 + new_bytes)?;
        *slots_guard = new_slots;
        drop(slots_guard);
        // Reinsert without recursive rehash (new table is empty and large enough).
        for (k, fk) in entries {
            match self.try_insert(&k, Fk(fk))? {
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

enum InsertResult {
    Done(Option<Fk>),
    NeedRehash,
}
