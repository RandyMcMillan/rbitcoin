//! Growable variable-length record table (schema v2+).
//!
//! Layout:
//! - `{stem}.body` — file header + append-only **unframed** payloads
//! - `{stem}.idx`  — file header + dense u64 absolute offsets into body
//!
//! Record length is derived from the index: `len(i) = start(i+1) - start(i)`,
//! and for the last record `logical_body_end - start`. No per-record `u32`
//! frame (eliminates double-length encoding when payloads are self-describing).

use crate::error::StoreError;
use crate::file::{TableFile, FILE_HEADER_LEN};
use rbitcoin_primitives::{Fk, TableKind};
use std::path::{Path, PathBuf};

pub struct VarTable {
    body: TableFile,
    idx: TableFile,
    count: std::sync::Mutex<u64>,
}

impl VarTable {
    pub fn create(dir: &Path, stem: &str, body_kind: TableKind) -> Result<Self, StoreError> {
        let body = TableFile::create(Self::body_path(dir, stem), body_kind)?;
        let idx = TableFile::create(Self::idx_path(dir, stem), TableKind::ArrayLink)?;
        Ok(Self {
            body,
            idx,
            count: std::sync::Mutex::new(0),
        })
    }

    pub fn open(dir: &Path, stem: &str, body_kind: TableKind) -> Result<Self, StoreError> {
        let body = TableFile::open(Self::body_path(dir, stem), body_kind)?;
        let idx = TableFile::open(Self::idx_path(dir, stem), TableKind::ArrayLink)?;
        let idx_body = idx.logical_len().saturating_sub(FILE_HEADER_LEN as u64);
        if idx_body % 8 != 0 {
            return Err(StoreError::Corrupt("var idx size"));
        }
        let count = idx_body / 8;
        Ok(Self {
            body,
            idx,
            count: std::sync::Mutex::new(count),
        })
    }

    fn body_path(dir: &Path, stem: &str) -> PathBuf {
        dir.join(format!("{stem}.body"))
    }

    fn idx_path(dir: &Path, stem: &str) -> PathBuf {
        dir.join(format!("{stem}.idx"))
    }

    pub fn count(&self) -> u64 {
        *self.count.lock().unwrap()
    }

    /// Current body logical length (including file header).
    pub fn body_logical_len(&self) -> u64 {
        self.body.logical_len()
    }

    /// Best-effort drop of body page-cache for a byte range (see
    /// [`crate::file::TableFile::advise_dont_need`]).
    pub fn advise_body_dont_need(&self, offset: u64, len: u64) {
        self.body.advise_dont_need(offset, len);
    }

    /// Absolute `(offset, len)` of the unframed payload for `fk`.
    pub fn record_range(&self, fk: Fk) -> Result<(u64, u64), StoreError> {
        let id = fk.get().ok_or(StoreError::InvalidFk)?;
        let count = *self.count.lock().unwrap();
        let start = self.record_start(id, count)?;
        let end = self.record_end(id, count)?;
        if end < start {
            return Err(StoreError::Corrupt("var record end < start"));
        }
        Ok((start, end - start))
    }

    /// `mlock` the body pages covering `fk`. Returns page-aligned `(start, len)`.
    pub fn mlock_record(&self, fk: Fk) -> Result<(u64, u64), StoreError> {
        let (off, len) = self.record_range(fk)?;
        self.mlock_body_range(off, len)
    }

    /// `mlock` body pages for a known absolute range (no idx).
    pub fn mlock_body_range(&self, offset: u64, len: u64) -> Result<(u64, u64), StoreError> {
        self.body.mlock_range(offset, len)
    }

    /// `mlock` the `idx` slot for `fk` (8-byte absolute body offset).
    pub fn mlock_idx_entry(&self, fk: Fk) -> Result<(u64, u64), StoreError> {
        let id = fk.get().ok_or(StoreError::InvalidFk)?;
        let off = FILE_HEADER_LEN as u64 + (id - 1) * 8;
        self.idx.mlock_range(off, 8)
    }

    /// Best-effort `munlock` for a prior body [`mlock_record`] page range.
    pub fn munlock_body_pages(&self, page_start: u64, page_len: u64) {
        self.body.munlock_range(page_start, page_len);
    }

    /// Best-effort `munlock` for a prior [`mlock_idx_entry`] page range.
    pub fn munlock_idx_pages(&self, page_start: u64, page_len: u64) {
        self.idx.munlock_range(page_start, page_len);
    }

    /// Inspect record bytes without copying into a `Vec`.
    pub fn with_raw<R>(
        &self,
        fk: Fk,
        f: impl FnOnce(&[u8]) -> Result<R, StoreError>,
    ) -> Result<R, StoreError> {
        let (off, len) = self.record_range(fk)?;
        self.body.with_bytes(off, len, f).and_then(|r| r)
    }

    /// Inspect body bytes at a known absolute range (no idx read).
    pub fn with_bytes_at<R>(
        &self,
        offset: u64,
        len: u64,
        f: impl FnOnce(&[u8]) -> Result<R, StoreError>,
    ) -> Result<R, StoreError> {
        self.body.with_bytes(offset, len, f).and_then(|r| r)
    }

    /// Absolute write into body file (no idx; caller has a cache-held range).
    pub fn write_body_abs(&self, abs_offset: u64, data: &[u8]) -> Result<(), StoreError> {
        self.body.write_at(abs_offset, data)
    }

    /// Pre-grow body (+ idx) capacity so a following mega `put_batch` does not
    /// remap mid-write.
    pub fn reserve_append(&self, body_bytes: u64, n_records: u64) -> Result<(), StoreError> {
        let body_need = self.body.logical_len().saturating_add(body_bytes);
        self.body.ensure_capacity(body_need)?;
        let idx_need = FILE_HEADER_LEN as u64 + (self.count() + n_records) * 8;
        self.idx.ensure_capacity(idx_need)?;
        Ok(())
    }

    /// Encode `n` records into one body blob then one write.
    ///
    /// Encoding runs **outside** the count lock (caller is the exclusive writer);
    /// only the short append+idx update is locked. `encode(i, buf)` appends the
    /// **unframed** payload for record `i`. Length comes from idx deltas on read.
    pub fn put_batch_encode(
        &self,
        n: usize,
        estimate_bytes: usize,
        mut encode: impl FnMut(usize, &mut Vec<u8>),
    ) -> Result<Vec<Fk>, StoreError> {
        if n == 0 {
            return Ok(Vec::new());
        }
        let (start, base_count) = {
            let count = self.count.lock().unwrap();
            (
                self.body.logical_len().max(FILE_HEADER_LEN as u64),
                *count,
            )
        };
        let mut body_blob = Vec::with_capacity(estimate_bytes);
        let mut idx_blob = Vec::with_capacity(n * 8);
        let mut fks = Vec::with_capacity(n);
        let mut cursor = start;
        for i in 0..n {
            fks.push(Fk(base_count + 1 + i as u64));
            idx_blob.extend_from_slice(&cursor.to_le_bytes());
            let before = body_blob.len();
            encode(i, &mut body_blob);
            cursor += (body_blob.len() - before) as u64;
        }
        let mut count = self.count.lock().unwrap();
        debug_assert_eq!(*count, base_count);
        if *count != base_count {
            return Err(StoreError::Corrupt("var put_batch_encode race"));
        }
        self.body.write_at(start, &body_blob)?;
        let off_pos = FILE_HEADER_LEN as u64 + (*count) * 8;
        self.idx.write_at(off_pos, &idx_blob)?;
        *count += n as u64;
        Ok(fks)
    }

    /// Absolute start offset of record `fk` in body (for length-from-idx).
    fn record_start(&self, id: u64, count: u64) -> Result<u64, StoreError> {
        if id == 0 || id > count {
            return Err(StoreError::NotFound);
        }
        let mut off_buf = [0u8; 8];
        self.idx
            .read_at(FILE_HEADER_LEN as u64 + (id - 1) * 8, &mut off_buf)?;
        Ok(u64::from_le_bytes(off_buf))
    }

    /// Exclusive end offset of record `id` (start of next, or body logical end).
    fn record_end(&self, id: u64, count: u64) -> Result<u64, StoreError> {
        if id < count {
            self.record_start(id + 1, count)
        } else {
            Ok(self.body.logical_len().max(FILE_HEADER_LEN as u64))
        }
    }

    /// Raw unframed payload for `fk`.
    pub fn get_raw(&self, fk: Fk) -> Result<Vec<u8>, StoreError> {
        let id = fk.get().ok_or(StoreError::InvalidFk)?;
        let count = *self.count.lock().unwrap();
        let start = self.record_start(id, count)?;
        let end = self.record_end(id, count)?;
        if end < start {
            return Err(StoreError::Corrupt("var record end < start"));
        }
        let len = (end - start) as usize;
        let mut buf = vec![0u8; len];
        if len > 0 {
            self.body.read_at(start, &mut buf)?;
        }
        Ok(buf)
    }

    pub fn flush(&self) -> Result<(), StoreError> {
        self.body.flush()?;
        self.idx.flush()?;
        Ok(())
    }

    /// HWM + MS_ASYNC (no fdatasync) — host-friendly process exit.
    pub fn flush_async(&self) -> Result<(), StoreError> {
        self.body.flush_async()?;
        self.idx.flush_async()?;
        Ok(())
    }
}
