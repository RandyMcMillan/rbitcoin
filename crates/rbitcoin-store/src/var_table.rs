//! Growable variable-length record table (schema v2+).
//!
//! Layout:
//! - `{stem}.body` — file header + append-only **unframed** payloads
//! - `{stem}.idx`  — file header + dense u64 absolute offsets into body
//!
//! Record length is derived from the index: `len(i) = start(i+1) - start(i)`,
//! and for the last record `logical_body_end - start`. No per-record `u32`
//! frame (eliminates double-length encoding when payloads are self-describing).
//!
//! # Publish order (lock-free)
//!
//! Single appender: **body bytes → idx slots → `count` Release**. Readers load
//! `count` with Acquire and only observe complete records.

use crate::error::StoreError;
use crate::file::{TableFile, FILE_HEADER_LEN};
use rbitcoin_primitives::{Fk, TableKind};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

pub struct VarTable {
    body: TableFile,
    idx: TableFile,
    count: AtomicU64,
}

impl VarTable {
    pub fn create(dir: &Path, stem: &str, body_kind: TableKind) -> Result<Self, StoreError> {
        let body = TableFile::create(Self::body_path(dir, stem), body_kind)?;
        let idx = TableFile::create(Self::idx_path(dir, stem), TableKind::ArrayLink)?;
        Ok(Self {
            body,
            idx,
            count: AtomicU64::new(0),
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
            count: AtomicU64::new(count),
        })
    }

    fn body_path(dir: &Path, stem: &str) -> PathBuf {
        dir.join(format!("{stem}.body"))
    }

    fn idx_path(dir: &Path, stem: &str) -> PathBuf {
        dir.join(format!("{stem}.idx"))
    }

    pub fn count(&self) -> u64 {
        self.count.load(Ordering::Acquire)
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
        let count = self.count.load(Ordering::Acquire);
        let start = self.record_start(id, count)?;
        let end = self.record_end(id, count)?;
        if end < start {
            return Err(StoreError::Corrupt("var record end < start"));
        }
        Ok((start, end - start))
    }

    #[inline]
    pub(crate) fn idx_read_fd(&self) -> std::os::fd::RawFd {
        self.idx.read_fd()
    }

    #[inline]
    pub(crate) fn body_read_fd(&self) -> std::os::fd::RawFd {
        self.body.read_fd()
    }

    #[inline]
    pub(crate) fn idx_file_path(&self) -> &Path {
        self.idx.path()
    }

    #[inline]
    pub(crate) fn body_file_path(&self) -> &Path {
        self.body.path()
    }

    #[inline]
    pub(crate) fn idx_published_len(&self) -> u64 {
        self.idx.logical_len()
    }

    #[inline]
    pub(crate) fn body_published_len(&self) -> u64 {
        self.body.logical_len()
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
    /// Encoding runs outside any count barrier (single appender role). Publish
    /// order: body → idx → `count` Release.
    pub fn put_batch_encode(
        &self,
        n: usize,
        estimate_bytes: usize,
        mut encode: impl FnMut(usize, &mut Vec<u8>),
    ) -> Result<Vec<Fk>, StoreError> {
        if n == 0 {
            return Ok(Vec::new());
        }
        let base_count = self.count.load(Ordering::Acquire);
        let start = self.body.logical_len().max(FILE_HEADER_LEN as u64);
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
        // Single appender: count must still equal base.
        if self.count.load(Ordering::Acquire) != base_count {
            return Err(StoreError::Corrupt("var put_batch_encode race"));
        }
        self.body.write_at(start, &body_blob)?;
        let off_pos = FILE_HEADER_LEN as u64 + base_count * 8;
        self.idx.write_at(off_pos, &idx_blob)?;
        // Publish complete records.
        self.count
            .store(base_count + n as u64, Ordering::Release);
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
        let count = self.count.load(Ordering::Acquire);
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

    /// Read only the first `buf.len()` bytes at absolute body `(offset, len)`.
    /// Avoids allocating / faulting the full record when only a fixed prefix is
    /// needed (e.g. Class A txid). Returns bytes actually copied into `buf`.
    pub fn read_prefix_at(
        &self,
        offset: u64,
        len: u64,
        buf: &mut [u8],
    ) -> Result<usize, StoreError> {
        if buf.is_empty() {
            return Ok(0);
        }
        let n = (len as usize).min(buf.len());
        if n == 0 {
            return Ok(0);
        }
        self.body.read_at(offset, &mut buf[..n])?;
        Ok(n)
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

#[cfg(test)]
mod tests {
    use super::*;
    use rbitcoin_primitives::TableKind;
    use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
    use std::sync::{Arc, Barrier};
    use std::thread;

    #[test]
    fn put_batch_publish_visible_to_concurrent_readers() {
        static N: AtomicU64 = AtomicU64::new(0);
        let id = N.fetch_add(1, AtomicOrdering::Relaxed);
        let dir = std::env::temp_dir().join(format!("rbitcoin-var-pub-{id}"));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let t = Arc::new(VarTable::create(&dir, "tx", TableKind::Tx).unwrap());

        let barrier = Arc::new(Barrier::new(4));
        let mut handles = Vec::new();

        {
            let t = Arc::clone(&t);
            let barrier = Arc::clone(&barrier);
            handles.push(thread::spawn(move || {
                barrier.wait();
                for batch in 0..50u8 {
                    let payload = vec![batch; 128];
                    t.put_batch_encode(4, 512, |_i, buf| {
                        buf.extend_from_slice(&payload);
                    })
                    .unwrap();
                }
            }));
        }

        for _ in 0..3 {
            let t = Arc::clone(&t);
            let barrier = Arc::clone(&barrier);
            handles.push(thread::spawn(move || {
                barrier.wait();
                for _ in 0..2000 {
                    let c = t.count();
                    if c == 0 {
                        continue;
                    }
                    // Every published fk must fully decode as raw of expected size.
                    let fk = Fk(c); // last published
                    let raw = t.get_raw(fk).unwrap();
                    assert_eq!(raw.len(), 128);
                    assert!(raw.iter().all(|&b| b == raw[0]));
                }
            }));
        }

        for h in handles {
            h.join().unwrap();
        }
        assert_eq!(t.count(), 200);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
