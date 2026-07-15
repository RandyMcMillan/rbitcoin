//! Growable variable-length record table.
//!
//! Layout:
//! - `{stem}.body` — file header + append-only framed payloads
//! - `{stem}.idx`  — file header + dense u64 absolute offsets into body (1-based fk *i* at slot *i-1*)

use crate::error::StoreError;
use crate::file::{TableFile, FILE_HEADER_LEN};
use rbitcoin_primitives::{Fk, TableKind};
use std::path::{Path, PathBuf};

pub struct VarTable {
    body: TableFile,
    idx: TableFile,
    count: parking_lot::Mutex<u64>,
}

impl VarTable {
    pub fn create(dir: &Path, stem: &str, body_kind: TableKind) -> Result<Self, StoreError> {
        let body = TableFile::create(Self::body_path(dir, stem), body_kind)?;
        let idx = TableFile::create(Self::idx_path(dir, stem), TableKind::ArrayLink)?;
        Ok(Self {
            body,
            idx,
            count: parking_lot::Mutex::new(0),
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
            count: parking_lot::Mutex::new(count),
        })
    }

    fn body_path(dir: &Path, stem: &str) -> PathBuf {
        dir.join(format!("{stem}.body"))
    }

    fn idx_path(dir: &Path, stem: &str) -> PathBuf {
        dir.join(format!("{stem}.idx"))
    }

    pub fn count(&self) -> u64 {
        *self.count.lock()
    }

    pub fn put(&self, payload: &[u8]) -> Result<Fk, StoreError> {
        let mut fks = self.put_batch(std::slice::from_ref(&payload))?;
        Ok(fks.pop().expect("one payload"))
    }

    /// Append many framed payloads under one count lock with a single body
    /// write and a single idx write — major win when archiving multi-input txs.
    pub fn put_batch(&self, payloads: &[&[u8]]) -> Result<Vec<Fk>, StoreError> {
        if payloads.is_empty() {
            return Ok(Vec::new());
        }
        let mut count = self.count.lock();
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

    /// Encode `n` records into one body blob (length-prefix each) then one write.
    ///
    /// Encoding runs **outside** the count lock (caller is the exclusive writer);
    /// only the short append+idx update is locked. `encode(i, buf)` appends the
    /// **unframed** payload for record `i`.
    pub fn put_batch_encode(
        &self,
        n: usize,
        estimate_bytes: usize,
        mut encode: impl FnMut(usize, &mut Vec<u8>),
    ) -> Result<Vec<Fk>, StoreError> {
        if n == 0 {
            return Ok(Vec::new());
        }
        // Snapshot placement under lock, then encode without holding it.
        let (start, base_count) = {
            let count = self.count.lock();
            (
                self.body.logical_len().max(FILE_HEADER_LEN as u64),
                *count,
            )
        };
        let mut body_blob = Vec::with_capacity(estimate_bytes.saturating_add(n * 4));
        let mut idx_blob = Vec::with_capacity(n * 8);
        let mut fks = Vec::with_capacity(n);
        let mut cursor = start;
        for i in 0..n {
            fks.push(Fk(base_count + 1 + i as u64));
            idx_blob.extend_from_slice(&cursor.to_le_bytes());
            let frame_at = body_blob.len();
            body_blob.extend_from_slice(&0u32.to_le_bytes());
            encode(i, &mut body_blob);
            let total = (body_blob.len() - frame_at) as u32;
            body_blob[frame_at..frame_at + 4].copy_from_slice(&total.to_le_bytes());
            cursor += u64::from(total);
        }
        let mut count = self.count.lock();
        // Exclusive writer: no concurrent put may advance count.
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

    /// Append a pre-built body blob + idx offsets (relative to `start` already
    /// applied in idx values as absolute body offsets). Used when encode was
    /// parallelized outside this table.
    pub fn put_batch_prebuilt(
        &self,
        body_blob: &[u8],
        idx_abs_offsets: &[u64],
    ) -> Result<Vec<Fk>, StoreError> {
        if idx_abs_offsets.is_empty() {
            return Ok(Vec::new());
        }
        let n = idx_abs_offsets.len();
        let mut count = self.count.lock();
        let start = self.body.logical_len().max(FILE_HEADER_LEN as u64);
        if idx_abs_offsets[0] != start {
            return Err(StoreError::Corrupt("prebuilt idx start mismatch"));
        }
        let mut fks = Vec::with_capacity(n);
        let mut idx_blob = Vec::with_capacity(n * 8);
        for (i, off) in idx_abs_offsets.iter().enumerate() {
            fks.push(Fk(*count + 1 + i as u64));
            idx_blob.extend_from_slice(&off.to_le_bytes());
        }
        self.body.write_at(start, body_blob)?;
        let off_pos = FILE_HEADER_LEN as u64 + (*count) * 8;
        self.idx.write_at(off_pos, &idx_blob)?;
        *count += n as u64;
        Ok(fks)
    }

    pub fn get_raw(&self, fk: Fk) -> Result<Vec<u8>, StoreError> {
        let id = fk.get().ok_or(StoreError::InvalidFk)?;
        let count = *self.count.lock();
        if id == 0 || id > count {
            return Err(StoreError::NotFound);
        }
        let mut off_buf = [0u8; 8];
        self.idx
            .read_at(FILE_HEADER_LEN as u64 + (id - 1) * 8, &mut off_buf)?;
        let start = u64::from_le_bytes(off_buf);
        let mut len_buf = [0u8; 4];
        self.body.read_at(start, &mut len_buf)?;
        let total = u32::from_le_bytes(len_buf) as usize;
        if total < 4 {
            return Err(StoreError::Corrupt("var record len"));
        }
        let mut buf = vec![0u8; total];
        self.body.read_at(start, &mut buf)?;
        Ok(buf)
    }

    pub fn flush(&self) -> Result<(), StoreError> {
        self.body.flush()?;
        self.idx.flush()?;
        Ok(())
    }
}

/// Prefix payload with u32 total length (including the 4-byte prefix).
pub fn framed(payload: &[u8]) -> Vec<u8> {
    let total = (4 + payload.len()) as u32;
    let mut out = Vec::with_capacity(total as usize);
    out.extend_from_slice(&total.to_le_bytes());
    out.extend_from_slice(payload);
    out
}
