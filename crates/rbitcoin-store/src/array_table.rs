//! Growable dense array of little-endian `u64` values.
//!
//! 0-based indexing. Used for Class C: confirmed[height], header_txs arrays, etc.
//!
//! Length is an atomic publish barrier: slots `0..len` are complete.

use crate::error::StoreError;
use crate::file::{TableFile, FILE_HEADER_LEN};
use rbitcoin_primitives::TableKind;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

const ELEM: u64 = 8;

pub struct ArrayTable {
    file: TableFile,
    len: AtomicU64,
}

impl ArrayTable {
    pub fn create(path: impl AsRef<Path>, kind: TableKind) -> Result<Self, StoreError> {
        let file = TableFile::create(path.as_ref(), kind)?;
        Ok(Self {
            file,
            len: AtomicU64::new(0),
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
            len: AtomicU64::new(body / ELEM),
        })
    }

    pub fn len(&self) -> u64 {
        self.len.load(Ordering::Acquire)
    }

    fn offset(index: u64) -> u64 {
        FILE_HEADER_LEN as u64 + index * ELEM
    }

    pub fn get(&self, index: u64) -> Result<u64, StoreError> {
        let len = self.len.load(Ordering::Acquire);
        if index >= len {
            return Ok(0);
        }
        let mut buf = [0u8; 8];
        self.file.read_at(Self::offset(index), &mut buf)?;
        Ok(u64::from_le_bytes(buf))
    }

    /// Set value at `index`, growing with zero-fill if needed.
    pub fn set(&self, index: u64, value: u64) -> Result<(), StoreError> {
        let len = self.len.load(Ordering::Acquire);
        if index >= len {
            for i in len..index {
                self.file.write_at(Self::offset(i), &0u64.to_le_bytes())?;
            }
            self.file
                .write_at(Self::offset(index), &value.to_le_bytes())?;
            self.len.store(index + 1, Ordering::Release);
        } else {
            self.file
                .write_at(Self::offset(index), &value.to_le_bytes())?;
        }
        Ok(())
    }

    /// Set many (index, value) pairs. Grows once to max index (blob zero-fill),
    /// then writes each value — avoids O(gap) per-slot grow in a loop.
    pub fn set_many(&self, pairs: &[(u64, u64)]) -> Result<(), StoreError> {
        if pairs.is_empty() {
            return Ok(());
        }
        let max_idx = pairs.iter().map(|(i, _)| *i).max().unwrap();
        let len = self.len.load(Ordering::Acquire);
        if max_idx >= len {
            let new_len = max_idx + 1;
            let gap = new_len - len;
            const CHUNK: usize = 1024 * 1024;
            let mut offset = Self::offset(len);
            let mut remaining = gap * ELEM;
            let zeros = vec![0u8; CHUNK];
            while remaining > 0 {
                let n = remaining.min(zeros.len() as u64) as usize;
                self.file.write_at(offset, &zeros[..n])?;
                offset += n as u64;
                remaining -= n as u64;
            }
            self.len.store(new_len, Ordering::Release);
        }
        for &(index, value) in pairs {
            self.file
                .write_at(Self::offset(index), &value.to_le_bytes())?;
        }
        Ok(())
    }

    /// Shrink to `new_len` slots (tip disconnect).
    pub fn truncate(&self, new_len: u64) -> Result<(), StoreError> {
        let len = self.len.load(Ordering::Acquire);
        if new_len > len {
            return Err(StoreError::Corrupt("array truncate grows"));
        }
        self.len.store(new_len, Ordering::Release);
        let logical = FILE_HEADER_LEN as u64 + new_len * ELEM;
        self.file.set_logical_len(logical)?;
        Ok(())
    }

    /// `mlock` pages covering slots `[start_index, start_index + count)`.
    pub fn mlock_indices(
        &self,
        start_index: u64,
        count: u64,
    ) -> Result<(u64, u64), StoreError> {
        if count == 0 {
            return Ok((0, 0));
        }
        let off = Self::offset(start_index);
        let len = count * ELEM;
        self.file.mlock_range(off, len)
    }

    pub fn munlock_pages(&self, page_start: u64, page_len: u64) {
        self.file.munlock_range(page_start, page_len);
    }

    pub fn flush(&self) -> Result<(), StoreError> {
        self.file.flush()
    }

    pub fn flush_async(&self) -> Result<(), StoreError> {
        self.file.flush_async()
    }
}
