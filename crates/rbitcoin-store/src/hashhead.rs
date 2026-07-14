//! Simple fixed-slot hash head: key (32 bytes) -> first record fk (u64).
//!
//! v0 uses linear probing over a power-of-two slot table stored in a [`TableFile`].

use crate::error::StoreError;
use crate::file::{TableFile, FILE_HEADER_LEN};
use rbitcoin_primitives::{Fk, TableKind};

const SLOT_SIZE: usize = 40; // 32 key + 8 fk
/// Power-of-two slot count. Modest default keeps full-table scenarios cheap in tests.
const DEFAULT_SLOTS: u64 = 64;

pub struct HashHead {
    file: TableFile,
    slots: u64,
}

impl HashHead {
    pub fn create(path: impl Into<std::path::PathBuf>) -> Result<Self, StoreError> {
        let file = TableFile::create(path, TableKind::HashHead)?;
        let bytes = SLOT_SIZE as u64 * DEFAULT_SLOTS;
        let zeros = vec![0u8; bytes as usize];
        // Write slots starting at FILE_HEADER_LEN.
        file.write_at(FILE_HEADER_LEN as u64, &zeros)?;
        // Logical length already advanced by write_at.
        Ok(Self {
            file,
            slots: DEFAULT_SLOTS,
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
        Ok(Self { file, slots })
    }

    fn slot_offset(&self, slot: u64) -> u64 {
        FILE_HEADER_LEN as u64 + slot * SLOT_SIZE as u64
    }

    fn hash_slot(key: &[u8; 32], slots: u64) -> u64 {
        // FNV-1a 64-bit over key, mask to slots.
        let mut h: u64 = 0xcbf29ce484222325;
        for b in key {
            h ^= u64::from(*b);
            h = h.wrapping_mul(0x100000001b3);
        }
        h & (slots - 1)
    }

    pub fn get(&self, key: &[u8; 32]) -> Result<Option<Fk>, StoreError> {
        let mut slot = Self::hash_slot(key, self.slots);
        for _ in 0..self.slots {
            let mut buf = [0u8; SLOT_SIZE];
            self.file.read_at(self.slot_offset(slot), &mut buf)?;
            let k: [u8; 32] = buf[0..32].try_into().unwrap();
            let fk = u64::from_le_bytes(buf[32..40].try_into().unwrap());
            if fk == 0 && k == [0u8; 32] {
                return Ok(None);
            }
            if &k == key {
                return Ok(Fk::new(fk));
            }
            slot = (slot + 1) & (self.slots - 1);
        }
        Err(StoreError::Corrupt("hash head full on get"))
    }

    /// Insert or replace mapping. Returns previous fk if any.
    pub fn insert(&self, key: &[u8; 32], fk: Fk) -> Result<Option<Fk>, StoreError> {
        // Callers must pass a non-null FK (header/tx put paths always do).
        debug_assert!(!fk.is_null());
        let mut slot = Self::hash_slot(key, self.slots);
        for _ in 0..self.slots {
            let off = self.slot_offset(slot);
            let mut buf = [0u8; SLOT_SIZE];
            self.file.read_at(off, &mut buf)?;
            let k: [u8; 32] = buf[0..32].try_into().unwrap();
            let old_fk = u64::from_le_bytes(buf[32..40].try_into().unwrap());
            if old_fk == 0 && k == [0u8; 32] {
                // empty
                let mut out = [0u8; SLOT_SIZE];
                out[0..32].copy_from_slice(key);
                out[32..40].copy_from_slice(&fk.0.to_le_bytes());
                self.file.write_at(off, &out)?;
                return Ok(None);
            }
            if &k == key {
                let prev = Fk::new(old_fk);
                let mut out = [0u8; SLOT_SIZE];
                out[0..32].copy_from_slice(key);
                out[32..40].copy_from_slice(&fk.0.to_le_bytes());
                self.file.write_at(off, &out)?;
                return Ok(prev);
            }
            slot = (slot + 1) & (self.slots - 1);
        }
        Err(StoreError::Corrupt("hash head full on insert"))
    }

    pub fn flush(&self) -> Result<(), StoreError> {
        self.file.flush()
    }
}
