//! Growable dense array of little-endian `u64` values.
//!
//! 0-based indexing. Used for Class C: confirmed[height], header_txs arrays, etc.
//!
//! # L2 write-behind (phase 6)
//!
//! Compact tables (body ≤ [`class_c_inram_max_bytes`]) load fully into process
//! RAM on open. Mutates update the `Vec` only and mark dirty. Disk is updated on
//! [`Self::flush`] / barrier:
//!
//! - **Append-only** (dirty only at indices ≥ last flushed len): write the new
//!   **suffix** only, then extend HWM — never overwrites published prefix bytes.
//! - **Truncate**: shrink HWM only (complete-or-fail length publish).
//! - **In-prefix mutate** (rare for tip Class C): full body rewrite (residual
//!   mid-pwrite tear risk on same-size published range — mitigated by tip-last
//!   barrier so `confirmed` is not advanced until strong/height are durable).
//!
//! Tables over the cap stay pure fd L0 (per-slot write-through).

use crate::error::StoreError;
use crate::file::{TableFile, FILE_HEADER_LEN};
use rbitcoin_primitives::TableKind;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::RwLock;

const ELEM: u64 = 8;

/// Default max body size for L2 InRam (256 MiB). Over → pure FdOnly L0.
pub const DEFAULT_CLASS_C_INRAM_MAX_BYTES: u64 = 256 * 1024 * 1024;

/// Cap for loading Class C arrays into process RAM.
///
/// Env: `RBITCOIN_CLASS_C_INRAM_MAX_MB` (integer MiB). Unset → 256 MiB.
pub fn class_c_inram_max_bytes() -> u64 {
    match std::env::var("RBITCOIN_CLASS_C_INRAM_MAX_MB") {
        Ok(s) => {
            let mb: u64 = s.trim().parse().unwrap_or(256);
            mb.saturating_mul(1024 * 1024)
        }
        Err(_) => DEFAULT_CLASS_C_INRAM_MAX_BYTES,
    }
}

pub struct ArrayTable {
    file: TableFile,
    len: AtomicU64,
    /// `Some` = L2 authoritative image; `None` = L0 fd-only path.
    data: RwLock<Option<Vec<u64>>>,
    dirty: AtomicBool,
    /// Min index mutated since last flush (`u64::MAX` = none).
    dirty_lo: AtomicU64,
    /// Element count last successfully flushed to disk (L2).
    disk_len: AtomicU64,
}

impl ArrayTable {
    pub fn create(path: impl AsRef<Path>, kind: TableKind) -> Result<Self, StoreError> {
        let file = TableFile::create(path.as_ref(), kind)?;
        Ok(Self {
            file,
            len: AtomicU64::new(0),
            data: RwLock::new(Some(Vec::new())),
            dirty: AtomicBool::new(false),
            dirty_lo: AtomicU64::new(u64::MAX),
            disk_len: AtomicU64::new(0),
        })
    }

    pub fn open(path: impl AsRef<Path>, kind: TableKind) -> Result<Self, StoreError> {
        let file = TableFile::open(path.as_ref(), kind)?;
        let body = file.logical_len().saturating_sub(FILE_HEADER_LEN as u64);
        if body % ELEM != 0 {
            return Err(StoreError::Corrupt("array table size"));
        }
        let n = body / ELEM;
        let data = if body <= class_c_inram_max_bytes() {
            let mut v = vec![0u64; n as usize];
            if n > 0 {
                let mut bytes = vec![0u8; body as usize];
                file.read_at(FILE_HEADER_LEN as u64, &mut bytes)?;
                for (i, chunk) in bytes.chunks_exact(8).enumerate() {
                    v[i] = u64::from_le_bytes(chunk.try_into().unwrap());
                }
            }
            Some(v)
        } else {
            None
        };
        Ok(Self {
            file,
            len: AtomicU64::new(n),
            data: RwLock::new(data),
            dirty: AtomicBool::new(false),
            dirty_lo: AtomicU64::new(u64::MAX),
            disk_len: AtomicU64::new(n),
        })
    }

    pub fn len(&self) -> u64 {
        self.len.load(Ordering::Acquire)
    }

    fn offset(index: u64) -> u64 {
        FILE_HEADER_LEN as u64 + index * ELEM
    }

    fn mark_dirty_index(&self, index: u64) {
        self.dirty.store(true, Ordering::Release);
        let mut cur = self.dirty_lo.load(Ordering::Relaxed);
        while index < cur {
            match self.dirty_lo.compare_exchange_weak(
                cur,
                index,
                Ordering::Release,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(c) => cur = c,
            }
        }
    }

    pub fn get(&self, index: u64) -> Result<u64, StoreError> {
        let len = self.len.load(Ordering::Acquire);
        if index >= len {
            return Ok(0);
        }
        let guard = self.data.read().unwrap_or_else(|e| e.into_inner());
        if let Some(ref v) = *guard {
            return Ok(v[index as usize]);
        }
        drop(guard);
        let mut buf = [0u8; 8];
        self.file.read_at(Self::offset(index), &mut buf)?;
        Ok(u64::from_le_bytes(buf))
    }

    pub fn set(&self, index: u64, value: u64) -> Result<(), StoreError> {
        let mut guard = self.data.write().unwrap_or_else(|e| e.into_inner());
        if let Some(ref mut v) = *guard {
            let need = index as usize + 1;
            if v.len() < need {
                v.resize(need, 0);
            }
            v[index as usize] = value;
            self.len.store(v.len() as u64, Ordering::Release);
            self.mark_dirty_index(index);
            return Ok(());
        }
        drop(guard);
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

    pub fn set_many(&self, pairs: &[(u64, u64)]) -> Result<(), StoreError> {
        if pairs.is_empty() {
            return Ok(());
        }
        let max_idx = pairs.iter().map(|(i, _)| *i).max().unwrap();
        let min_idx = pairs.iter().map(|(i, _)| *i).min().unwrap();
        let mut guard = self.data.write().unwrap_or_else(|e| e.into_inner());
        if let Some(ref mut v) = *guard {
            let need = max_idx as usize + 1;
            if v.len() < need {
                v.resize(need, 0);
            }
            for &(index, value) in pairs {
                v[index as usize] = value;
            }
            self.len.store(v.len() as u64, Ordering::Release);
            self.mark_dirty_index(min_idx);
            return Ok(());
        }
        drop(guard);
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

    pub fn truncate(&self, new_len: u64) -> Result<(), StoreError> {
        let len = self.len.load(Ordering::Acquire);
        if new_len > len {
            return Err(StoreError::Corrupt("array truncate grows"));
        }
        let mut guard = self.data.write().unwrap_or_else(|e| e.into_inner());
        if let Some(ref mut v) = *guard {
            v.truncate(new_len as usize);
            self.len.store(new_len, Ordering::Release);
            // Shrink is a length-only publish; mark dirty from 0 so flush takes
            // the truncate path (set_logical_len), not an append.
            self.mark_dirty_index(0);
            return Ok(());
        }
        drop(guard);
        self.len.store(new_len, Ordering::Release);
        let logical = FILE_HEADER_LEN as u64 + new_len * ELEM;
        self.file.set_logical_len(logical)?;
        Ok(())
    }

    /// Persist dirty L2 image. Prefers append-only suffix writes so published
    /// prefix bytes are never overwritten mid-barrier.
    pub fn flush_dirty(&self) -> Result<(), StoreError> {
        let guard = self.data.read().unwrap_or_else(|e| e.into_inner());
        let Some(ref v) = *guard else {
            return Ok(());
        };
        if !self.dirty.load(Ordering::Acquire) {
            return Ok(());
        }
        let n = v.len() as u64;
        let disk = self.disk_len.load(Ordering::Acquire);
        let dirty_lo = self.dirty_lo.load(Ordering::Acquire);

        if n < disk {
            // Truncate: shrink HWM only — published prefix of length n stays.
            drop(guard);
            let logical = FILE_HEADER_LEN as u64 + n * ELEM;
            self.file.set_logical_len(logical)?;
            self.disk_len.store(n, Ordering::Release);
            self.dirty.store(false, Ordering::Release);
            self.dirty_lo.store(u64::MAX, Ordering::Release);
            return Ok(());
        }

        if n == disk && dirty_lo >= disk {
            // No length change and no in-prefix dirty — nothing to write.
            drop(guard);
            self.dirty.store(false, Ordering::Release);
            self.dirty_lo.store(u64::MAX, Ordering::Release);
            return Ok(());
        }

        if dirty_lo >= disk && n > disk {
            // Pure append: write only new slots, then extend HWM.
            // Complete-or-fail: pwrite suffix fully before publish.
            let start = disk as usize;
            let mut bytes = vec![0u8; ((n - disk) as usize) * 8];
            for (i, &val) in v[start..].iter().enumerate() {
                bytes[i * 8..(i + 1) * 8].copy_from_slice(&val.to_le_bytes());
            }
            drop(guard);
            self.file.write_at(Self::offset(disk), &bytes)?;
            // write_at already published HWM to cover the suffix end.
            let logical = FILE_HEADER_LEN as u64 + n * ELEM;
            // Ensure HWM exact (write_at publishes end of write which equals this).
            debug_assert_eq!(self.file.logical_len(), logical);
            let _ = logical;
            self.disk_len.store(n, Ordering::Release);
            self.dirty.store(false, Ordering::Release);
            self.dirty_lo.store(u64::MAX, Ordering::Release);
            return Ok(());
        }

        // In-prefix mutate: write from first dirty index through end only
        // (confirmed tip extension almost always dirties only the high slots).
        let from = dirty_lo.min(n);
        let mut bytes = vec![0u8; ((n - from) as usize) * 8];
        for (i, &val) in v[from as usize..].iter().enumerate() {
            bytes[i * 8..(i + 1) * 8].copy_from_slice(&val.to_le_bytes());
        }
        drop(guard);
        if !bytes.is_empty() {
            self.file.write_at(Self::offset(from), &bytes)?;
        }
        if n != disk {
            let logical = FILE_HEADER_LEN as u64 + n * ELEM;
            self.file.set_logical_len(logical)?;
        }
        self.disk_len.store(n, Ordering::Release);
        self.dirty.store(false, Ordering::Release);
        self.dirty_lo.store(u64::MAX, Ordering::Release);
        Ok(())
    }

    pub fn flush(&self) -> Result<(), StoreError> {
        self.flush_dirty()?;
        self.file.flush()
    }

    pub fn flush_async(&self) -> Result<(), StoreError> {
        self.flush_dirty()?;
        self.file.flush_async()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rbitcoin_primitives::TableKind;

    fn tmp_path() -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "rbitcoin-array-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ))
    }

    #[test]
    fn array_set_get_grow_set_many_truncate_flush() {
        let path = tmp_path();
        let _ = std::fs::remove_file(&path);
        let t = ArrayTable::create(&path, TableKind::Confirmed).unwrap();
        assert_eq!(t.len(), 0);
        assert_eq!(t.get(0).unwrap(), 0);
        t.set(3, 30).unwrap();
        assert_eq!(t.len(), 4);
        assert_eq!(t.get(0).unwrap(), 0);
        assert_eq!(t.get(3).unwrap(), 30);
        t.set(1, 11).unwrap();
        assert_eq!(t.get(1).unwrap(), 11);
        t.set_many(&[]).unwrap();
        t.set_many(&[(10, 100), (5, 50)]).unwrap();
        assert_eq!(t.len(), 11);
        assert_eq!(t.get(5).unwrap(), 50);
        assert_eq!(t.get(10).unwrap(), 100);
        t.flush().unwrap();
        t.flush_async().unwrap();
        drop(t);
        let t = ArrayTable::open(&path, TableKind::Confirmed).unwrap();
        assert_eq!(t.len(), 11);
        assert_eq!(t.get(3).unwrap(), 30);
        t.truncate(4).unwrap();
        assert_eq!(t.len(), 4);
        assert!(matches!(t.truncate(99), Err(StoreError::Corrupt(_))));
        t.flush().unwrap();
        {
            let mut raw = std::fs::read(&path).unwrap();
            let bad = (FILE_HEADER_LEN as u64 + 3).to_le_bytes();
            raw[8..16].copy_from_slice(&bad);
            std::fs::write(&path, &raw).unwrap();
        }
        assert!(matches!(
            ArrayTable::open(&path, TableKind::Confirmed),
            Err(StoreError::Corrupt(_))
        ));
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn write_behind_no_flush_reopen_keeps_old_image() {
        let path = tmp_path();
        let _ = std::fs::remove_file(&path);
        {
            let t = ArrayTable::create(&path, TableKind::Confirmed).unwrap();
            t.set_many(&[(0, 10), (1, 20)]).unwrap();
            t.flush().unwrap();
            t.set(0, 999).unwrap();
            t.set(2, 30).unwrap();
            assert_eq!(t.get(0).unwrap(), 999);
            assert_eq!(t.len(), 3);
        }
        let t = ArrayTable::open(&path, TableKind::Confirmed).unwrap();
        assert_eq!(t.len(), 2, "len must stay at last flushed image");
        assert_eq!(t.get(0).unwrap(), 10);
        assert_eq!(t.get(1).unwrap(), 20);
        assert_eq!(t.get(2).unwrap(), 0);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn write_behind_flush_reopen_sees_new_image() {
        let path = tmp_path();
        let _ = std::fs::remove_file(&path);
        {
            let t = ArrayTable::create(&path, TableKind::Confirmed).unwrap();
            t.set_many(&[(0, 1), (1, 2)]).unwrap();
            t.flush().unwrap();
            t.set_many(&[(0, 7), (1, 8), (2, 9)]).unwrap();
            t.flush().unwrap();
        }
        let t = ArrayTable::open(&path, TableKind::Confirmed).unwrap();
        assert_eq!(t.len(), 3);
        assert_eq!(t.get(0).unwrap(), 7);
        assert_eq!(t.get(1).unwrap(), 8);
        assert_eq!(t.get(2).unwrap(), 9);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn write_behind_truncate_flush_reopen() {
        let path = tmp_path();
        let _ = std::fs::remove_file(&path);
        {
            let t = ArrayTable::create(&path, TableKind::Confirmed).unwrap();
            t.set_many(&[(0, 1), (1, 2), (2, 3), (3, 4)]).unwrap();
            t.flush().unwrap();
            t.truncate(2).unwrap();
            assert_eq!(t.len(), 2);
            drop(t);
            let t = ArrayTable::open(&path, TableKind::Confirmed).unwrap();
            assert_eq!(t.len(), 4);
            t.truncate(2).unwrap();
            t.flush().unwrap();
        }
        let t = ArrayTable::open(&path, TableKind::Confirmed).unwrap();
        assert_eq!(t.len(), 2);
        assert_eq!(t.get(0).unwrap(), 1);
        assert_eq!(t.get(1).unwrap(), 2);
        let _ = std::fs::remove_file(&path);
    }

    /// Tip extension is append-only: after first flush, only suffix is written.
    #[test]
    fn append_only_flush_extends_without_losing_prefix() {
        let path = tmp_path();
        let _ = std::fs::remove_file(&path);
        {
            let t = ArrayTable::create(&path, TableKind::Confirmed).unwrap();
            t.set_many(&[(0, 100), (1, 101)]).unwrap();
            t.flush().unwrap();
            // Pure tip extension (dirty_lo = 2 >= disk_len = 2).
            t.set(2, 102).unwrap();
            t.flush().unwrap();
        }
        let t = ArrayTable::open(&path, TableKind::Confirmed).unwrap();
        assert_eq!(t.len(), 3);
        assert_eq!(t.get(0).unwrap(), 100);
        assert_eq!(t.get(1).unwrap(), 101);
        assert_eq!(t.get(2).unwrap(), 102);
        let _ = std::fs::remove_file(&path);
    }
}
