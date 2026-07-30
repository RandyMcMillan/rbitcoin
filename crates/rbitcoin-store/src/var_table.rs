//! Growable variable-length record table (schema v11+ Class A).
//!
//! Layout:
//! - `{stem}.body` — file header + append-only **unframed** payloads
//! - `{stem}.idx.meta` + `{stem}.idx.NNNNNN` — segmented **u32 stride-8**
//!   offsets (see [`crate::tx_idx::TxIdx`])
//!
//! Record length is derived from the index: `len(i) = start(i+1) - start(i)`,
//! and for the last record `logical_body_end - start`. Starts are **8-byte
//! aligned** (and for Class A, txid does not straddle a 4 KiB page).
//!
//! # Publish order (lock-free)
//!
//! Single appender: **body bytes → idx slots → `(count, body_end)` via seqlock**.
//! Readers load a consistent `(count, published_body_end)` pair (never
//! `(old_count, new_end)`).

use crate::error::StoreError;
use crate::file::{TableFile, FILE_HEADER_LEN};
use crate::tx_idx::TxIdx;
use rbitcoin_primitives::{Fk, TableKind};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

/// Fixed 4 KiB page for on-disk txid non-straddle rule (must match `tx_table`).
const TX_BODY_PAGE: u64 = 4096;
const TXID_PAGE_MAX_OFF: u64 = TX_BODY_PAGE - 32;

/// Next 8-byte-aligned body start where a 32-byte txid does not cross a page.
#[inline]
fn next_aligned_tx_start(cursor: u64) -> u64 {
    let mut s = cursor.saturating_add(7) & !7u64;
    while s % TX_BODY_PAGE > TXID_PAGE_MAX_OFF {
        s = s.saturating_add(8);
    }
    s
}

pub struct VarTable {
    body: TableFile,
    idx: TxIdx,
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
        let idx = TxIdx::create(dir, stem)?;
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
        let idx = TxIdx::open(dir, stem)?;
        let count = idx.slot_count();
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
    ///
    /// **Interior records (`id < count`):** starts of `id` and `id+1` (may span
    /// idx segments).
    ///
    /// **Last record (`id == count`):** seqlock `(count, body_end)` + start.
    pub fn record_range(&self, fk: Fk) -> Result<(u64, u64), StoreError> {
        let id = fk.get().ok_or(StoreError::InvalidFk)?;
        if id == 0 {
            return Err(StoreError::InvalidFk);
        }
        let count = self.count.load(Ordering::Acquire);
        if id > count {
            return Err(StoreError::NotFound);
        }
        if id < count {
            return self.idx.record_range_interior(id);
        }
        let (count2, body_end) = self.published_meta();
        if id > count2 {
            return Err(StoreError::NotFound);
        }
        if id < count2 {
            return self.idx.record_range_interior(id);
        }
        let start = self.record_start(id, count2)?;
        if body_end < start {
            return Err(StoreError::Corrupt("var record end < start"));
        }
        Ok((start, body_end - start))
    }

    /// Contiguous `(offset, len)` for Class A ids `first..=last` (1-based).
    pub fn record_ranges(&self, first: u64, last: u64) -> Result<Vec<(u64, u64)>, StoreError> {
        let (count, body_end) = self.published_meta();
        self.idx.record_ranges(first, last, count, body_end)
    }

    /// Bulk body ranges for arbitrary fks — **sorted** walk of segmented idx.
    ///
    /// Output order matches `fks`. Null / OOB ids yield `None` (not an error).
    /// Contiguous id runs use one sequential load via [`Self::record_ranges`];
    /// sparse singles use [`Self::record_range`].
    pub fn record_range_batch(
        &self,
        fks: &[Fk],
    ) -> Result<Vec<Option<(u64, u64)>>, StoreError> {
        if fks.is_empty() {
            return Ok(Vec::new());
        }
        let count = self.count.load(Ordering::Acquire);
        let mut out: Vec<Option<(u64, u64)>> = vec![None; fks.len()];
        let mut jobs: Vec<(usize, u64)> = Vec::with_capacity(fks.len());
        for (i, fk) in fks.iter().enumerate() {
            let Some(id) = fk.get() else {
                continue;
            };
            if id == 0 || id > count {
                continue;
            }
            jobs.push((i, id));
        }
        if jobs.is_empty() {
            return Ok(out);
        }
        jobs.sort_unstable_by_key(|(_, id)| *id);

        let mut run_start = 0usize;
        while run_start < jobs.len() {
            let first_id = jobs[run_start].1;
            let mut last_unique = first_id;
            let mut run_end = run_start + 1;
            while run_end < jobs.len() {
                let id = jobs[run_end].1;
                if id == last_unique {
                    run_end += 1;
                    continue;
                }
                if id == last_unique + 1 {
                    last_unique = id;
                    run_end += 1;
                    continue;
                }
                break;
            }
            let ranges = self.record_ranges(first_id, last_unique)?;
            for j in run_start..run_end {
                let (orig_i, id) = jobs[j];
                let slot = (id - first_id) as usize;
                out[orig_i] = Some(ranges[slot]);
            }
            run_start = run_end;
        }
        Ok(out)
    }

    /// Segmented idx has no single fd — callers resolve ranges via
    /// [`Self::record_range`] / batch APIs (mmap), then body pread.
    #[inline]
    pub(crate) fn body_read_fd(&self) -> std::os::fd::RawFd {
        self.body.read_fd()
    }

    #[inline]
    pub(crate) fn body_file_path(&self) -> &Path {
        self.body.path()
    }

    #[inline]
    pub(crate) fn body_published_len(&self) -> u64 {
        self.published_meta().1
    }

    /// Pin body map once; `f(map_bytes, published_len)`.
    #[inline]
    pub(crate) fn with_body_map_pin<R>(&self, f: impl FnOnce(&[u8], u64) -> R) -> R {
        self.body.with_map_pin(f)
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

    /// Pre-grow body (+ idx tail) capacity so a following mega `put_batch` does not
    /// remap mid-write.
    pub fn reserve_append(&self, body_bytes: u64, n_records: u64) -> Result<(), StoreError> {
        let body_need = self.body.logical_len().saturating_add(body_bytes);
        self.body.ensure_capacity(body_need)?;
        self.idx.reserve_slots(n_records)?;
        Ok(())
    }

    /// Encode `n` records into one body blob then one write.
    ///
    /// Encoding runs outside any count barrier (single appender role). Publish
    /// order: body → idx → `count` Release. Record starts are always 8-aligned
    /// (stride idx) with the Class A page non-straddle rule.
    pub fn put_batch_encode(
        &self,
        n: usize,
        estimate_bytes: usize,
        encode: impl FnMut(usize, &mut Vec<u8>),
    ) -> Result<Vec<Fk>, StoreError> {
        self.put_batch_encode_inner(n, estimate_bytes, encode)
    }

    /// Same as [`put_batch_encode`] (alignment is always on for stride idx).
    pub fn put_batch_encode_aligned(
        &self,
        n: usize,
        estimate_bytes: usize,
        encode: impl FnMut(usize, &mut Vec<u8>),
    ) -> Result<Vec<Fk>, StoreError> {
        self.put_batch_encode_inner(n, estimate_bytes, encode)
    }

    fn put_batch_encode_inner(
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
        let mut starts = Vec::with_capacity(n);
        let mut fks = Vec::with_capacity(n);
        let mut cursor = start;
        for i in 0..n {
            fks.push(Fk(base_count + 1 + i as u64));
            let aligned = next_aligned_tx_start(cursor);
            let pad = aligned.saturating_sub(cursor) as usize;
            if pad > 0 {
                body_blob.resize(body_blob.len() + pad, 0);
                cursor = aligned;
            }
            starts.push(cursor);
            let before = body_blob.len();
            encode(i, &mut body_blob);
            cursor += (body_blob.len() - before) as u64;
        }
        // Single appender: count must still equal base.
        if self.count.load(Ordering::Acquire) != base_count {
            return Err(StoreError::Corrupt("var put_batch_encode race"));
        }
        let use_fd = std::env::var("RBITCOIN_FD_APPEND")
            .map(|s| s != "0" && s != "false" && s != "off")
            .unwrap_or(true);
        if use_fd {
            self.body.write_at_pwrite(start, &body_blob)?;
        } else {
            self.body.write_at(start, &body_blob)?;
        }
        // Idx after body (publish order).
        self.idx.append_starts(base_count, &starts)?;
        let new_end = start.saturating_add(body_blob.len() as u64);
        let new_count = base_count + n as u64;
        self.publish_begin();
        self.published_body_end.store(new_end, Ordering::Relaxed);
        self.count.store(new_count, Ordering::Relaxed);
        self.publish_end();
        Ok(fks)
    }

    /// Absolute start offset of record `fk` in body (for length-from-idx).
    pub(crate) fn record_start(&self, id: u64, count: u64) -> Result<u64, StoreError> {
        if id == 0 || id > count {
            return Err(StoreError::NotFound);
        }
        self.idx.record_start(id)
    }

    #[inline]
    fn publish_begin(&self) {
        let prev = self.publish_seq.fetch_add(1, Ordering::Relaxed);
        debug_assert_eq!(prev & 1, 0, "nested/concurrent publish_begin");
        std::sync::atomic::fence(Ordering::Release);
    }

    #[inline]
    fn publish_end(&self) {
        let prev = self.publish_seq.fetch_add(1, Ordering::Release);
        debug_assert_eq!(prev & 1, 1, "publish_end without begin");
    }

    /// Consistent `(count, published_body_end)` via seqlock (never torn pair).
    pub(crate) fn published_meta(&self) -> (u64, u64) {
        loop {
            let s1 = self.publish_seq.load(Ordering::Acquire);
            if s1 & 1 != 0 {
                std::hint::spin_loop();
                continue;
            }
            let end = self
                .published_body_end
                .load(Ordering::Relaxed)
                .max(FILE_HEADER_LEN as u64);
            let count = self.count.load(Ordering::Relaxed);
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

    /// Diagnostics: number of idx segment files.
    pub fn idx_segment_count(&self) -> usize {
        self.idx.segment_count()
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
    fn put_batch_fd_append_roundtrip() {
        static N: AtomicU64 = AtomicU64::new(0);
        let id = N.fetch_add(1, AtomicOrdering::Relaxed);
        let dir = std::env::temp_dir().join(format!("rbitcoin-var-fd-{id}"));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let t = VarTable::create(&dir, "tx", TableKind::Tx).unwrap();
        let fks = t
            .put_batch_encode(3, 64, |i, buf| {
                buf.extend_from_slice(&[i as u8 + 1; 16]);
            })
            .unwrap();
        assert_eq!(fks.len(), 3);
        assert_eq!(t.count(), 3);
        for (i, fk) in fks.iter().enumerate() {
            let body = t.get_raw(*fk).unwrap();
            // May include alignment pad as trailing zeros.
            assert!(body.len() >= 16);
            assert_eq!(&body[..16], &[i as u8 + 1; 16]);
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

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
                    let (meta_c, meta_end) = t.published_meta();
                    if meta_c == 0 {
                        continue;
                    }
                    let fk = Fk(meta_c);
                    let raw = t.get_raw(fk).unwrap();
                    // 128-byte payloads are 8-aligned; last record has no pad
                    // until the next batch lands.
                    assert!(
                        raw.len() >= 128,
                        "torn publish meta_c={meta_c} meta_end={meta_end} count={c} len={}",
                        raw.len()
                    );
                    assert!(raw[..128].iter().all(|&b| b == raw[0]));
                }
            }));
        }

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
        let raw = t.get_raw(Fk(t.count())).unwrap();
        assert!(raw.len() >= 128);
        let _ = std::fs::remove_dir_all(&dir);
    }

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
            // 64-byte aligned records: last length is exactly 64 until next pad.
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
    fn record_range_interior_matches_adjacent_starts() {
        let dir = std::env::temp_dir().join(format!(
            "rbitcoin-var-interior-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let t = VarTable::create(&dir, "tx", TableKind::Tx).unwrap();
        t.put_batch_encode(5, 256, |i, buf| {
            buf.extend_from_slice(&vec![i as u8; 8 + i * 3]);
        })
        .unwrap();
        assert_eq!(t.count(), 5);
        for id in 1..=5u64 {
            let (off, len) = t.record_range(Fk(id)).unwrap();
            let raw = t.get_raw(Fk(id)).unwrap();
            assert_eq!(raw.len() as u64, len, "id={id}");
            assert_eq!(raw[0], (id - 1) as u8);
            if id < 5 {
                let (next_off, _) = t.record_range(Fk(id + 1)).unwrap();
                assert_eq!(off + len, next_off, "interior abut id={id}");
            }
            assert_eq!(off % 8, 0, "id={id}");
        }
        let bulk = t.record_ranges(1, 5).unwrap();
        for (i, &(off, len)) in bulk.iter().enumerate() {
            assert_eq!(
                (off, len),
                t.record_range(Fk(1 + i as u64)).unwrap(),
                "bulk vs single id={}",
                1 + i
            );
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn record_range_batch_sorted_mmap_matches_serial() {
        let dir = std::env::temp_dir().join(format!(
            "rbitcoin-var-range-batch-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let t = VarTable::create(&dir, "tx", TableKind::Tx).unwrap();
        t.put_batch_encode(20, 256, |i, buf| {
            buf.extend_from_slice(&vec![i as u8; 10 + (i % 5)]);
        })
        .unwrap();
        assert_eq!(t.count(), 20);

        let fks = vec![
            Fk(15),
            Fk(3),
            Fk(3),
            Fk(1),
            Fk::NULL,
            Fk(20),
            Fk(99),
            Fk(10),
            Fk(11),
            Fk(12),
        ];
        let batch = t.record_range_batch(&fks).unwrap();
        assert_eq!(batch.len(), fks.len());
        assert_eq!(batch[4], None);
        assert_eq!(batch[6], None);
        for (i, fk) in fks.iter().enumerate() {
            if batch[i].is_none() {
                continue;
            }
            let seq = t.record_range(*fk).unwrap();
            assert_eq!(batch[i], Some(seq), "fk={fk:?} i={i}");
        }
        assert_eq!(batch[1], batch[2]);
        let contig = t.record_ranges(10, 12).unwrap();
        assert_eq!(batch[7], Some(contig[0]));
        assert_eq!(batch[8], Some(contig[1]));
        assert_eq!(batch[9], Some(contig[2]));
        assert!(t.record_range_batch(&[]).unwrap().is_empty());
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
        let bulk = t.record_ranges(5, 12).unwrap();
        assert_eq!(bulk.len(), 8);
        for (i, (off, len)) in bulk.iter().enumerate() {
            let (o, l) = t.record_range(Fk(5 + i as u64)).unwrap();
            assert_eq!((*off, *len), (o, l), "id={}", 5 + i);
        }
        let bulk_end = t.record_ranges(28, 30).unwrap();
        for (i, (off, len)) in bulk_end.iter().enumerate() {
            let (o, l) = t.record_range(Fk(28 + i as u64)).unwrap();
            assert_eq!((*off, *len), (o, l));
        }
        assert!(t.record_ranges(4, 3).unwrap().is_empty());
        assert_eq!(
            t.record_ranges(1, 1).unwrap()[0],
            t.record_range(Fk(1)).unwrap()
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn multi_segment_via_soft_span() {
        let dir = std::env::temp_dir().join(format!(
            "rbitcoin-var-multiseg-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        std::env::set_var("RBITCOIN_TX_IDX_SOFT_SPAN", "128");
        let t = VarTable::create(&dir, "tx", TableKind::Tx).unwrap();
        // Each record ~100 B → soft 128 forces new segment often.
        for i in 0..12u8 {
            t.put_batch_encode(1, 128, |_j, buf| {
                buf.extend_from_slice(&vec![i; 100]);
            })
            .unwrap();
        }
        assert!(t.idx_segment_count() >= 2, "segs={}", t.idx_segment_count());
        for id in 1..=12u64 {
            let raw = t.get_raw(Fk(id)).unwrap();
            assert_eq!(raw[0], (id - 1) as u8);
            assert!(raw.len() >= 100);
        }
        drop(t);
        let t = VarTable::open(&dir, "tx", TableKind::Tx).unwrap();
        assert_eq!(t.count(), 12);
        assert_eq!(t.get_raw(Fk(12)).unwrap()[0], 11);
        std::env::remove_var("RBITCOIN_TX_IDX_SOFT_SPAN");
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
        assert!(raw.len() >= 16);
        assert_eq!(raw[0], 1);
        let via = t
            .with_raw(fks[1], |b| {
                assert!(b.len() >= 16);
                Ok(b[0])
            })
            .unwrap();
        assert_eq!(via, 1);
        let (off, len) = t.record_range(fks[0]).unwrap();
        assert_eq!(off % 8, 0);
        let mut prefix = [0u8; 4];
        assert_eq!(t.read_prefix_at(off, len, &mut prefix).unwrap(), 4);
        assert_eq!(prefix, [0, 0, 0, 0]);
        assert_eq!(t.read_prefix_at(off, len, &mut []).unwrap(), 0);
        t.with_bytes_at(off, len, |b| {
            assert!(b.len() >= 16);
            Ok(())
        })
        .unwrap();
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
        assert!(t.get_raw(Fk(2)).unwrap().len() >= 16);
        // Corrupt meta magic.
        {
            let meta = dir.join("tx.idx.meta");
            std::fs::write(&meta, b"XXXX").unwrap();
        }
        assert!(matches!(
            VarTable::open(&dir, "tx", TableKind::Tx),
            Err(StoreError::Corrupt(_)) | Err(StoreError::Io { .. })
        ));
        let _ = std::fs::remove_dir_all(&dir);
    }
}
