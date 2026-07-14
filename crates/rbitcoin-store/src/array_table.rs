//! Growable dense array of little-endian `u64` values.
//!
//! 0-based indexing. Used for Class C: confirmed[height], strong_tx[tx_fk-1], etc.

use crate::error::StoreError;
use crate::file::{TableFile, FILE_HEADER_LEN};
use rbitcoin_primitives::TableKind;
use std::path::Path;

const ELEM: u64 = 8;

pub struct ArrayTable {
    file: TableFile,
    len: parking_lot::Mutex<u64>,
}

impl ArrayTable {
    pub fn create(path: impl AsRef<Path>, kind: TableKind) -> Result<Self, StoreError> {
        let file = TableFile::create(path.as_ref(), kind)?;
        Ok(Self {
            file,
            len: parking_lot::Mutex::new(0),
        })
    }

    pub fn open(path: impl AsRef<Path>, kind: TableKind) -> Result<Self, StoreError> {
        let file = TableFile::open(path.as_ref(), kind)?;
        let body = file.logical_len().saturating_sub(FILE_HEADER_LEN as u64);
        if body % ELEM != 0 {
            return Err(StoreError::Corrupt("array table size"));
        }
        Ok(Self {
            file,
            len: parking_lot::Mutex::new(body / ELEM),
        })
    }

    pub fn len(&self) -> u64 {
        *self.len.lock()
    }

    fn offset(index: u64) -> u64 {
        FILE_HEADER_LEN as u64 + index * ELEM
    }

    pub fn get(&self, index: u64) -> Result<u64, StoreError> {
        let len = *self.len.lock();
        if index >= len {
            return Ok(0);
        }
        let mut buf = [0u8; 8];
        self.file.read_at(Self::offset(index), &mut buf)?;
        Ok(u64::from_le_bytes(buf))
    }

    /// Set value at `index`, growing with zero-fill if needed.
    pub fn set(&self, index: u64, value: u64) -> Result<(), StoreError> {
        let mut len = self.len.lock();
        if index >= *len {
            for i in *len..index {
                self.file.write_at(Self::offset(i), &0u64.to_le_bytes())?;
            }
            *len = index + 1;
            // ensure slot index exists
            self.file
                .write_at(Self::offset(index), &value.to_le_bytes())?;
        } else {
            self.file
                .write_at(Self::offset(index), &value.to_le_bytes())?;
        }
        Ok(())
    }

    /// Shrink to `new_len` slots (tip disconnect).
    pub fn truncate(&self, new_len: u64) -> Result<(), StoreError> {
        let mut len = self.len.lock();
        if new_len > *len {
            return Err(StoreError::Corrupt("array truncate grows"));
        }
        *len = new_len;
        let logical = FILE_HEADER_LEN as u64 + new_len * ELEM;
        self.file.set_logical_len(logical)?;
        Ok(())
    }

    pub fn flush(&self) -> Result<(), StoreError> {
        self.file.flush()
    }
}
