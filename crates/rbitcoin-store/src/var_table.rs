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
//! Single appender: **body bytes → idx slots → `(count, body_end)` via seqlock**.
//! Readers load a consistent `(count, published_body_end)` pair (never
//! `(old_count, new_end)`).

use crate::error::StoreError;
use crate::file::{TableFile, FILE_HEADER_LEN};
use rbitcoin_primitives::{Fk, TableKind};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

pub struct VarTable {
    body: TableFile,
    idx: TableFile,
    count: AtomicU64,
    /// Body exclusive-end of the last **published** record.
    /// Must not use live `body.logical_len()` for last-record length: the single
    /// appender may extend body for the *next* batch before publishing count.
    published_body_end: AtomicU64,
    /// Seqlock for `(count, published_body_end)`: odd = writer critical section,
    /// even = stable. Prevents readers from pairing a stale count with a newer end
    /// (writer stores end then count; naive double-load of count alone is racy).
    publish_seq: AtomicU64,
}

impl VarTable {
    pub fn create(dir: &Path, stem: &str, body_kind: TableKind) -> Result<Self, StoreError> {
        let body = TableFile::create(Self::body_path(dir, stem), body_kind)?;
        let idx = TableFile::create(Self::idx_path(dir, stem), TableKind::ArrayLink)?;
        Ok(Self {
            body,
            idx,
            count: AtomicU64::new(0),
            published_body_end: AtomicU64::new(FILE_HEADER_LEN as u64),
            publish_seq: AtomicU64::new(0),
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
        let body_end = body.logical_len().max(FILE_HEADER_LEN as u64);
        Ok(Self {
            body,
            idx,
            count: AtomicU64::new(count),
            published_body_end: AtomicU64::new(body_end),
            publish_seq: AtomicU64::new(0),
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
        let (count, body_end) = self.published_meta();
        let start = self.record_start(id, count)?;
        let end = self.record_end_with(id, count, body_end)?;
        if end < start {
            return Err(StoreError::Corrupt("var record end < start"));
        }
        Ok((start, end - start))
    }

    /// Contiguous `(offset, len)` for Class A ids `first..=last` (1-based).
    ///
    /// One sequential `tx.idx` pread of the start offsets (plus the next start
    /// when `last < count`); lengths are adjacent-start deltas, or body end for
    /// the last published record. Used by head-resize bulk `body_txid` fill.
    pub fn record_ranges(&self, first: u64, last: u64) -> Result<Vec<(u64, u64)>, StoreError> {
        if first == 0 {
            return Err(StoreError::InvalidFk);
        }
        if last < first {
            return Ok(Vec::new());
        }
        let (count, body_end) = self.published_meta();
        if last > count {
            return Err(StoreError::NotFound);
        }
        let n = (last - first + 1) as usize;
        // Starts for first..=last; if last is not the last published record we
        // also need start(last+1) for its exclusive end.
        let need_next = last < count;
        let n_starts = n + usize::from(need_next);
        let mut raw = vec![0u8; n_starts * 8];
        let idx_off = FILE_HEADER_LEN as u64 + (first - 1) * 8;
        self.idx.read_at(idx_off, &mut raw)?;
        let mut starts = Vec::with_capacity(n_starts);
        for i in 0..n_starts {
            let s = i * 8;
            starts.push(u64::from_le_bytes(raw[s..s + 8].try_into().unwrap()));
        }
        let mut out = Vec::with_capacity(n);
        for i in 0..n {
            let start = starts[i];
            let end = if i + 1 < starts.len() {
                starts[i + 1]
            } else {
                body_end
            };
            if end < start {
                return Err(StoreError::Corrupt("var record end < start"));
            }
            out.push((start, end - start));
        }
        Ok(out)
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
        self.published_meta().1
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
        // Publish complete records under seqlock: enter (odd) → end+count → leave (even).
        // Without the seqlock, a reader can observe (old_count, new_end) between the
        // two stores and inflate last-record length (regression: left 640 right 128).
        let new_end = start.saturating_add(body_blob.len() as u64);
        let new_count = base_count + n as u64;
        self.publish_begin();
        self.published_body_end.store(new_end, Ordering::Relaxed);
        self.count.store(new_count, Ordering::Relaxed);
        self.publish_end();
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

    #[inline]
    fn publish_begin(&self) {
        // Odd seq = writer in critical section. Single appender: no concurrent begin.
        let prev = self.publish_seq.fetch_add(1, Ordering::Relaxed);
        debug_assert_eq!(prev & 1, 0, "nested/concurrent publish_begin");
        std::sync::atomic::fence(Ordering::Release);
    }

    #[inline]
    fn publish_end(&self) {
        // Even seq = stable; Release so readers' Acquire sees end+count stores.
        let prev = self.publish_seq.fetch_add(1, Ordering::Release);
        debug_assert_eq!(prev & 1, 1, "publish_end without begin");
    }

    /// Consistent `(count, published_body_end)` via seqlock (never torn pair).
    fn published_meta(&self) -> (u64, u64) {
        loop {
            let s1 = self.publish_seq.load(Ordering::Acquire);
            if s1 & 1 != 0 {
                // Writer mid-publish.
                std::hint::spin_loop();
                continue;
            }
            let end = self
                .published_body_end
                .load(Ordering::Relaxed)
                .max(FILE_HEADER_LEN as u64);
            let count = self.count.load(Ordering::Relaxed);
            // Pair with Acquire load of seq so we observe stores before publish_end.
            let s2 = self.publish_seq.load(Ordering::Acquire);
            if s1 == s2 {
                return (count, end);
            }
        }
    }

    /// Exclusive end offset of record `id` given a consistent `(count, body_end)`.
    fn record_end_with(
        &self,
        id: u64,
        count: u64,
        published_body_end: u64,
    ) -> Result<u64, StoreError> {
        if id < count {
            self.record_start(id + 1, count)
        } else if id == count {
            Ok(published_body_end)
        } else {
            Err(StoreError::NotFound)
        }
    }

    /// Raw unframed payload for `fk`.
    pub fn get_raw(&self, fk: Fk) -> Result<Vec<u8>, StoreError> {
        let id = fk.get().ok_or(StoreError::InvalidFk)?;
        // Pair count with published_body_end so last-record length cannot span a
        // later batch's body tail (concurrent appender).
        let (count, body_end) = self.published_meta();
        let start = self.record_start(id, count)?;
        let end = self.record_end_with(id, count, body_end)?;
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

    /// Concurrent readers must never see last-record len spanning a later batch
    /// (regression: raw.len() 640 vs expected 128 from torn (old_count, new_end)).
    #[test]
    fn put_batch_publish_visible_to_concurrent_readers() {
        let _stress = crate::file::TEST_MMAP_STRESS_LOCK
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        static N: AtomicU64 = AtomicU64::new(0);
        let id = N.fetch_add(1, AtomicOrdering::Relaxed);
        let dir = std::env::temp_dir().join(format!("rbitcoin-var-pub-{id}"));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let t = Arc::new(VarTable::create(&dir, "tx", TableKind::Tx).unwrap());

        let barrier = Arc::new(Barrier::new(5));
        let mut handles = Vec::new();

        {
            let t = Arc::clone(&t);
            let barrier = Arc::clone(&barrier);
            handles.push(thread::spawn(move || {
                barrier.wait();
                // Many small batches maximize the end-then-count publish window.
                for batch in 0..200u8 {
                    let payload = vec![batch; 128];
                    t.put_batch_encode(4, 512, |_i, buf| {
                        buf.extend_from_slice(&payload);
                    })
                    .unwrap();
                }
            }));
        }

        for _ in 0..4 {
            let t = Arc::clone(&t);
            let barrier = Arc::clone(&barrier);
            handles.push(thread::spawn(move || {
                barrier.wait();
                for _ in 0..20_000 {
                    let c = t.count();
                    if c == 0 {
                        continue;
                    }
                    // Seqlock pair: last published fk must fully decode as 128B.
                    let (meta_c, meta_end) = t.published_meta();
                    if meta_c == 0 {
                        continue;
                    }
                    let fk = Fk(meta_c);
                    let raw = t.get_raw(fk).unwrap();
                    assert_eq!(
                        raw.len(),
                        128,
                        "torn publish meta_c={meta_c} meta_end={meta_end} count={c}"
                    );
                    assert!(raw.iter().all(|&b| b == raw[0]));
                }
            }));
        }

        // Hard deadline: a panic-before-barrier used to hang join forever.
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        thread::spawn(move || {
            for h in handles {
                h.join().unwrap();
            }
            let _ = done_tx.send(());
        });
        done_rx
            .recv_timeout(std::time::Duration::from_secs(30))
            .expect("concurrent var_table workers timed out (hang?)");
        assert_eq!(t.count(), 800);
        // Final last-record still consistent.
        let raw = t.get_raw(Fk(t.count())).unwrap();
        assert_eq!(raw.len(), 128);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// published_meta never returns a pair where end belongs to a later count.
    #[test]
    fn published_meta_seqlock_matches_last_record_len() {
        let dir = std::env::temp_dir().join(format!(
            "rbitcoin-var-seqlock-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let t = Arc::new(VarTable::create(&dir, "tx", TableKind::Tx).unwrap());
        let stop = Arc::new(AtomicU64::new(0));
        let t_w = Arc::clone(&t);
        let stop_w = Arc::clone(&stop);
        let writer = thread::spawn(move || {
            let mut batch = 0u8;
            while stop_w.load(AtomicOrdering::Acquire) == 0 {
                let payload = vec![batch; 64];
                t_w.put_batch_encode(8, 512, |_i, buf| {
                    buf.extend_from_slice(&payload);
                })
                .unwrap();
                batch = batch.wrapping_add(1);
            }
        });
        for _ in 0..50_000 {
            let (c, end) = t.published_meta();
            if c == 0 {
                continue;
            }
            let start = t.record_start(c, c).unwrap();
            assert!(
                end >= start + 64,
                "end={end} start={start} c={c}"
            );
            // Exclusive end of last record is exactly one payload (64B) after start
            // when all records in a batch share size — also true across batches of 64B.
            assert_eq!(
                end - start,
                64,
                "seqlock pair torn: c={c} end={end} start={start}"
            );
        }
        stop.store(1, AtomicOrdering::Release);
        writer.join().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn record_ranges_matches_record_range() {
        let dir = std::env::temp_dir().join(format!(
            "rbitcoin-var-ranges-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let t = VarTable::create(&dir, "tx", TableKind::Tx).unwrap();
        for batch in 0..10u8 {
            let payload = vec![batch; 16 + batch as usize];
            t.put_batch_encode(3, 128, |_i, buf| {
                buf.extend_from_slice(&payload);
            })
            .unwrap();
        }
        assert_eq!(t.count(), 30);
        // Interior range.
        let bulk = t.record_ranges(5, 12).unwrap();
        assert_eq!(bulk.len(), 8);
        for (i, (off, len)) in bulk.iter().enumerate() {
            let (o, l) = t.record_range(Fk(5 + i as u64)).unwrap();
            assert_eq!((*off, *len), (o, l), "id={}", 5 + i);
        }
        // Through last published id.
        let bulk_end = t.record_ranges(28, 30).unwrap();
        for (i, (off, len)) in bulk_end.iter().enumerate() {
            let (o, l) = t.record_range(Fk(28 + i as u64)).unwrap();
            assert_eq!((*off, *len), (o, l));
        }
        // Empty / single.
        assert!(t.record_ranges(4, 3).unwrap().is_empty());
        assert_eq!(
            t.record_ranges(1, 1).unwrap()[0],
            t.record_range(Fk(1)).unwrap()
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn var_table_surface_helpers_and_errors() {
        let dir = std::env::temp_dir().join(format!(
            "rbitcoin-var-surface-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let t = VarTable::create(&dir, "tx", TableKind::Tx).unwrap();
        assert_eq!(t.count(), 0);
        assert!(t.body_logical_len() >= FILE_HEADER_LEN as u64);
        t.advise_body_dont_need(0, 0);
        assert_eq!(t.put_batch_encode(0, 0, |_, _| {}).unwrap().len(), 0);
        t.reserve_append(1024, 8).unwrap();
        let fks = t
            .put_batch_encode(3, 64, |i, buf| {
                buf.extend_from_slice(&[i as u8; 16]);
            })
            .unwrap();
        assert_eq!(fks.len(), 3);
        assert_eq!(t.count(), 3);
        let raw = t.get_raw(fks[1]).unwrap();
        assert_eq!(raw, vec![1u8; 16]);
        let via = t
            .with_raw(fks[1], |b| {
                assert_eq!(b.len(), 16);
                Ok(b[0])
            })
            .unwrap();
        assert_eq!(via, 1);
        let (off, len) = t.record_range(fks[0]).unwrap();
        let mut prefix = [0u8; 4];
        assert_eq!(t.read_prefix_at(off, len, &mut prefix).unwrap(), 4);
        assert_eq!(prefix, [0, 0, 0, 0]);
        assert_eq!(t.read_prefix_at(off, len, &mut []).unwrap(), 0);
        t.with_bytes_at(off, len, |b| {
            assert_eq!(b.len(), 16);
            Ok(())
        })
        .unwrap();
        // Patch first byte of first record.
        t.write_body_abs(off, &[0xff]).unwrap();
        assert_eq!(t.get_raw(fks[0]).unwrap()[0], 0xff);
        assert!(matches!(
            t.record_range(Fk::NULL),
            Err(StoreError::InvalidFk)
        ));
        assert!(matches!(
            t.record_range(Fk(99)),
            Err(StoreError::NotFound)
        ));
        assert!(matches!(
            t.record_ranges(0, 1),
            Err(StoreError::InvalidFk)
        ));
        assert!(matches!(
            t.record_ranges(1, 99),
            Err(StoreError::NotFound)
        ));
        assert!(matches!(t.get_raw(Fk::NULL), Err(StoreError::InvalidFk)));
        t.flush().unwrap();
        t.flush_async().unwrap();
        drop(t);
        let t = VarTable::open(&dir, "tx", TableKind::Tx).unwrap();
        assert_eq!(t.count(), 3);
        assert_eq!(t.get_raw(Fk(2)).unwrap().len(), 16);
        // Corrupt idx size: clamp logical below HWM to non-multiple of 8.
        {
            let idx = dir.join("tx.idx");
            std::fs::OpenOptions::new()
                .write(true)
                .open(&idx)
                .unwrap()
                .set_len((FILE_HEADER_LEN + 3) as u64)
                .unwrap();
        }
        assert!(matches!(
            VarTable::open(&dir, "tx", TableKind::Tx),
            Err(StoreError::Corrupt(_))
        ));
        let _ = std::fs::remove_dir_all(&dir);
    }
}
