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

    /// Pre-grow body (+ idx) capacity so a following mega `put_batch` does not
    /// remap mid-write.
    pub fn reserve_append(&self, body_bytes: u64, n_records: u64) -> Result<(), StoreError> {
        let body_need = self.body.logical_len().saturating_add(body_bytes);
        self.body.ensure_capacity(body_need)?;
        let idx_need = FILE_HEADER_LEN as u64 + (self.count() + n_records) * 8;
        self.idx.ensure_capacity(idx_need)?;
        Ok(())
    }

    /// Append a single unframed payload (API for non-batch writers).
    #[allow(dead_code)]
    pub fn put(&self, payload: &[u8]) -> Result<Fk, StoreError> {
        let mut fks = self.put_batch(std::slice::from_ref(&payload))?;
        Ok(fks.pop().expect("one payload"))
    }

    /// Append many unframed payloads under one count lock with a single body
    /// write and a single idx write.
    #[allow(dead_code)]
    pub fn put_batch(&self, payloads: &[&[u8]]) -> Result<Vec<Fk>, StoreError> {
        if payloads.is_empty() {
            return Ok(Vec::new());
        }
        let mut count = self.count.lock().unwrap();
        let start = self.body.logical_len().max(FILE_HEADER_LEN as u64);
        let total: usize = payloads.iter().map(|p| p.len()).sum();
        let mut body_blob = Vec::with_capacity(total);
        let mut idx_blob = Vec::with_capacity(payloads.len() * 8);
        let mut fks = Vec::with_capacity(payloads.len());
        let mut cursor = start;
        for (i, p) in payloads.iter().enumerate() {
            fks.push(Fk(*count + 1 + i as u64));
            idx_blob.extend_from_slice(&cursor.to_le_bytes());
            body_blob.extend_from_slice(p);
            cursor += p.len() as u64;
        }
        self.body.write_at(start, &body_blob)?;
        let off_pos = FILE_HEADER_LEN as u64 + (*count) * 8;
        self.idx.write_at(off_pos, &idx_blob)?;
        *count += payloads.len() as u64;
        Ok(fks)
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
