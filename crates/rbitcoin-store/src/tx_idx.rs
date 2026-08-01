//! Segmented Class A body index (`tx.idx.*`): u32 stride-8 offsets from a
//! per-segment `body_base`.
//!
//! Layout (schema 12+ after layout migration):
//! ```text
//! store/
//!   tx.idx/
//!     meta                   # segment map
//!     000000                 # dense u32 LE stride units
//!     000001
//!     …
//! ```
//!
//! **Migration:** flat `tx.idx.meta` + `tx.idx.NNNNNN` are renamed into `tx.idx/`
//! on open (same meta bytes).
//!
//! ```text
//! abs_start = body_base + (u32_le[i] as u64) * STRIDE
//! i = fk - first_fk   (fk 1-based; first_fk inclusive)
//! ```
//!
//! Hard span per segment: `2^32 * 8` ≈ 32 GiB. Soft rollover earlier (default
//! 16 GiB; override with `RBITCOIN_TX_IDX_SOFT_SPAN` bytes).

use crate::error::StoreError;
use crate::file::{TableFile, FILE_HEADER_LEN};
use rbitcoin_primitives::{TableKind, SCHEMA_VERSION, STORE_MAGIC};
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

/// Body offset unit for idx relatives (matches 8-byte-aligned record starts).
pub const IDX_STRIDE: u64 = 8;

/// Default soft body span before opening a new segment (~16 GiB).
pub const DEFAULT_SOFT_SPAN: u64 = 16 << 30;

/// Hard max: `u32::MAX` stride units × 8.
pub const HARD_SPAN: u64 = (u32::MAX as u64) * IDX_STRIDE;

const META_VERSION: u32 = 1;
/// magic4 + schema2 + kind2 + meta_ver4 + seg_count4 + reserved4
const META_HEADER_LEN: usize = 20;
/// first_fk8 + count8 + body_base8 + file_id4 + reserved4
const SEG_DESC_LEN: usize = 32;

#[derive(Clone)]
struct Segment {
    first_fk: u64,
    count: u64,
    body_base: u64,
    file_id: u32,
    file: Arc<TableFile>,
}

/// Multi-file u32 stride index for one var body stem (`tx`).
pub struct TxIdx {
    dir: PathBuf,
    stem: String,
    /// Published segment list (Arc swap under write lock on roll).
    segments: RwLock<Arc<Vec<Segment>>>,
    /// Next file_id to allocate (monotone).
    next_file_id: std::sync::atomic::AtomicU32,
}

impl TxIdx {
    pub fn create(dir: &Path, stem: &str) -> Result<Self, StoreError> {
        let dir = dir.to_path_buf();
        let stem = stem.to_string();
        ensure_idx_layout(&dir, &stem)?;
        // Empty meta (0 segments).
        write_meta(&dir, &stem, &[])?;
        Ok(Self {
            dir,
            stem,
            segments: RwLock::new(Arc::new(Vec::new())),
            next_file_id: std::sync::atomic::AtomicU32::new(0),
        })
    }

    pub fn open(dir: &Path, stem: &str) -> Result<Self, StoreError> {
        let dir = dir.to_path_buf();
        let stem = stem.to_string();
        ensure_idx_layout(&dir, &stem)?;
        let descs = read_meta(&dir, &stem)?;
        let mut segs = Vec::with_capacity(descs.len());
        let mut max_id = 0u32;
        for d in descs {
            let path = segment_path(&dir, &stem, d.file_id);
            let file = TableFile::open(&path, TableKind::ArrayLink)?;
            let slot_bytes = file
                .logical_len()
                .saturating_sub(FILE_HEADER_LEN as u64);
            if slot_bytes % 4 != 0 {
                return Err(StoreError::Corrupt("tx.idx segment size"));
            }
            let n_slots = slot_bytes / 4;
            if n_slots != d.count {
                return Err(StoreError::Corrupt("tx.idx segment count mismatch"));
            }
            max_id = max_id.max(d.file_id);
            segs.push(Segment {
                first_fk: d.first_fk,
                count: d.count,
                body_base: d.body_base,
                file_id: d.file_id,
                file: Arc::new(file),
            });
        }
        // Validate non-overlap / monotone first_fk.
        for w in segs.windows(2) {
            let a = &w[0];
            let b = &w[1];
            let a_end = a.first_fk.saturating_add(a.count);
            if b.first_fk != a_end {
                return Err(StoreError::Corrupt("tx.idx segment fk gap/overlap"));
            }
            if a.body_base % IDX_STRIDE != 0 || b.body_base % IDX_STRIDE != 0 {
                return Err(StoreError::Corrupt("tx.idx body_base unaligned"));
            }
        }
        Ok(Self {
            dir,
            stem,
            segments: RwLock::new(Arc::new(segs)),
            next_file_id: std::sync::atomic::AtomicU32::new(max_id.saturating_add(1)),
        })
    }

    /// Total published slots (= Class A count when in sync).
    pub fn slot_count(&self) -> u64 {
        let segs = self.segments_snapshot();
        segs.last()
            .map(|s| s.first_fk.saturating_add(s.count).saturating_sub(1))
            .unwrap_or(0)
    }

    fn segments_snapshot(&self) -> Arc<Vec<Segment>> {
        Arc::clone(
            &self
                .segments
                .read()
                .unwrap_or_else(|e| e.into_inner()),
        )
    }

    fn soft_span() -> u64 {
        std::env::var("RBITCOIN_TX_IDX_SOFT_SPAN")
            .ok()
            .and_then(|s| s.parse().ok())
            .filter(|&v| v >= IDX_STRIDE)
            .unwrap_or(DEFAULT_SOFT_SPAN)
            .min(HARD_SPAN)
    }

    /// Absolute body start for 1-based `id` (must be ≤ published count).
    pub fn record_start(&self, id: u64) -> Result<u64, StoreError> {
        if id == 0 {
            return Err(StoreError::NotFound);
        }
        let segs = self.segments_snapshot();
        let seg = find_segment(&segs, id).ok_or(StoreError::NotFound)?;
        let i = id - seg.first_fk;
        if i >= seg.count {
            return Err(StoreError::NotFound);
        }
        read_start(seg, i)
    }

    /// `(offset, len)` for interior id (`id < count`); needs start(id+1).
    pub fn record_range_interior(&self, id: u64) -> Result<(u64, u64), StoreError> {
        let start = self.record_start(id)?;
        let end = self.record_start(id + 1)?;
        if end < start {
            return Err(StoreError::Corrupt("var record end < start"));
        }
        Ok((start, end - start))
    }

    /// Contiguous ranges `first..=last` (1-based). `body_end` for last published.
    pub fn record_ranges(
        &self,
        first: u64,
        last: u64,
        count: u64,
        body_end: u64,
    ) -> Result<Vec<(u64, u64)>, StoreError> {
        if first == 0 {
            return Err(StoreError::InvalidFk);
        }
        if last < first {
            return Ok(Vec::new());
        }
        if last > count {
            return Err(StoreError::NotFound);
        }
        let n = (last - first + 1) as usize;
        let need_next = last < count;
        let mut starts = Vec::with_capacity(n + usize::from(need_next));
        // Read starts first..=last [+ last+1] possibly across segments.
        let end_id = if need_next { last + 1 } else { last };
        self.collect_starts(first, end_id, &mut starts)?;
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

    fn collect_starts(&self, first: u64, last: u64, out: &mut Vec<u64>) -> Result<(), StoreError> {
        if last < first {
            return Ok(());
        }
        let segs = self.segments_snapshot();
        let mut id = first;
        while id <= last {
            let seg = find_segment(&segs, id).ok_or(StoreError::NotFound)?;
            let seg_last_fk = seg.first_fk + seg.count - 1;
            let take_last = last.min(seg_last_fk);
            let i0 = id - seg.first_fk;
            let i1 = take_last - seg.first_fk;
            // Bulk read u32 slots [i0..=i1].
            let n = (i1 - i0 + 1) as usize;
            let mut raw = vec![0u8; n * 4];
            let off = FILE_HEADER_LEN as u64 + i0 * 4;
            seg.file.read_at(off, &mut raw)?;
            for k in 0..n {
                let rel = u32::from_le_bytes(raw[k * 4..k * 4 + 4].try_into().unwrap());
                let abs = seg
                    .body_base
                    .checked_add((rel as u64).checked_mul(IDX_STRIDE).ok_or(
                        StoreError::Corrupt("tx.idx stride overflow"),
                    )?)
                    .ok_or(StoreError::Corrupt("tx.idx abs overflow"))?;
                out.push(abs);
            }
            id = take_last + 1;
        }
        Ok(())
    }

    /// Append `n` absolute starts (must be 8-aligned, monotone, published after body).
    ///
    /// `base_count` is published count **before** this batch; starts[i] is for
    /// fk = base_count + 1 + i.
    pub fn append_starts(&self, base_count: u64, starts: &[u64]) -> Result<(), StoreError> {
        if starts.is_empty() {
            return Ok(());
        }
        for &s in starts {
            if s % IDX_STRIDE != 0 {
                return Err(StoreError::Corrupt("tx.idx start not stride-aligned"));
            }
        }
        let soft = Self::soft_span();
        let use_fd = crate::io_backend::class_a_append_uses_pwrite();

        let mut i = 0usize;
        while i < starts.len() {
            // Ensure tail segment can take starts[i].
            self.ensure_tail_for(base_count + 1 + i as u64, starts[i], soft)?;
            let segs = self.segments_snapshot();
            let tail = segs.last().ok_or(StoreError::Corrupt("tx.idx no segment"))?;
            let body_base = tail.body_base;
            // How many consecutive starts fit in this segment?
            let mut j = i;
            while j < starts.len() {
                let abs = starts[j];
                if abs < body_base {
                    return Err(StoreError::Corrupt("tx.idx start < body_base"));
                }
                let delta = abs - body_base;
                if delta % IDX_STRIDE != 0 {
                    return Err(StoreError::Corrupt("tx.idx start not on stride"));
                }
                let rel = delta / IDX_STRIDE;
                if rel > u32::MAX as u64 {
                    break; // need new segment
                }
                // Soft span: allow first slot of segment always; else roll when
                // span from base would exceed soft (except we already placed some).
                if j > i && delta > soft {
                    break;
                }
                if j == i && delta > soft && tail.count > 0 {
                    // Should have rolled in ensure_tail; treat as hard need.
                    break;
                }
                j += 1;
            }
            if j == i {
                // Single start cannot fit — force new segment with body_base=abs.
                self.roll_segment(base_count + 1 + i as u64, starts[i])?;
                continue;
            }
            // Encode u32s for starts[i..j]
            let n = j - i;
            let mut blob = Vec::with_capacity(n * 4);
            for &abs in &starts[i..j] {
                let rel = ((abs - body_base) / IDX_STRIDE) as u32;
                blob.extend_from_slice(&rel.to_le_bytes());
            }
            let slot_off = FILE_HEADER_LEN as u64 + tail.count * 4;
            // Re-borrow file via snapshot (tail Arc).
            let segs = self.segments_snapshot();
            let tail = segs.last().unwrap();
            if use_fd {
                tail.file.write_at_pwrite(slot_off, &blob)?;
            } else {
                tail.file.write_at(slot_off, &blob)?;
            }
            // Update tail count in segment list.
            {
                let mut guard = self.segments.write().unwrap_or_else(|e| e.into_inner());
                let mut new_list = (**guard).clone();
                let t = new_list.last_mut().unwrap();
                t.count += n as u64;
                *guard = Arc::new(new_list);
            }
            i = j;
        }
        // Persist meta after batch (counts updated).
        let segs = self.segments_snapshot();
        write_meta_from_segs(&self.dir, &self.stem, &segs)?;
        Ok(())
    }

    fn ensure_tail_for(&self, first_new_fk: u64, abs_start: u64, soft: u64) -> Result<(), StoreError> {
        let segs = self.segments_snapshot();
        if segs.is_empty() {
            return self.roll_segment(first_new_fk, abs_start);
        }
        let tail = segs.last().unwrap();
        if abs_start < tail.body_base {
            return Err(StoreError::Corrupt("tx.idx start < body_base"));
        }
        let delta = abs_start - tail.body_base;
        if delta % IDX_STRIDE != 0 {
            return Err(StoreError::Corrupt("tx.idx start not on stride"));
        }
        let rel = delta / IDX_STRIDE;
        if rel > u32::MAX as u64 || (tail.count > 0 && delta > soft) {
            return self.roll_segment(first_new_fk, abs_start);
        }
        // Empty brand-new segment: body_base should match first write.
        if tail.count == 0 && tail.body_base != abs_start {
            // Reset empty segment base (only possible if we rolled early).
            return self.roll_segment(first_new_fk, abs_start);
        }
        Ok(())
    }

    fn roll_segment(&self, first_fk: u64, body_base: u64) -> Result<(), StoreError> {
        if body_base % IDX_STRIDE != 0 {
            return Err(StoreError::Corrupt("tx.idx body_base unaligned"));
        }
        let file_id = self
            .next_file_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let path = segment_path(&self.dir, &self.stem, file_id);
        // Replace if empty leftover.
        let _ = std::fs::remove_file(&path);
        let file = TableFile::create(&path, TableKind::ArrayLink)?;
        let seg = Segment {
            first_fk,
            count: 0,
            body_base,
            file_id,
            file: Arc::new(file),
        };
        {
            let mut guard = self.segments.write().unwrap_or_else(|e| e.into_inner());
            let mut new_list = (**guard).clone();
            // Drop empty trailing segment if any.
            if let Some(last) = new_list.last() {
                if last.count == 0 {
                    new_list.pop();
                }
            }
            new_list.push(seg);
            *guard = Arc::new(new_list);
        }
        let segs = self.segments_snapshot();
        write_meta_from_segs(&self.dir, &self.stem, &segs)?;
        Ok(())
    }

    pub fn flush(&self) -> Result<(), StoreError> {
        let segs = self.segments_snapshot();
        for s in segs.iter() {
            s.file.flush()?;
        }
        // Meta is rewritten on append; fsync via re-write.
        write_meta_from_segs(&self.dir, &self.stem, &segs)?;
        Ok(())
    }

    pub fn flush_async(&self) -> Result<(), StoreError> {
        let segs = self.segments_snapshot();
        for s in segs.iter() {
            s.file.flush_async()?;
        }
        Ok(())
    }

    /// Pre-grow capacity for `n_records` more slots on the tail segment.
    pub fn reserve_slots(&self, n_records: u64) -> Result<(), StoreError> {
        if n_records == 0 {
            return Ok(());
        }
        let segs = self.segments_snapshot();
        if let Some(tail) = segs.last() {
            let need = FILE_HEADER_LEN as u64 + (tail.count + n_records) * 4;
            tail.file.ensure_capacity(need)?;
        }
        Ok(())
    }

    /// Number of open segment files (tests / diagnostics).
    pub fn segment_count(&self) -> usize {
        self.segments_snapshot().len()
    }
}

fn read_start(seg: &Segment, i: u64) -> Result<u64, StoreError> {
    let mut buf = [0u8; 4];
    seg.file
        .read_at(FILE_HEADER_LEN as u64 + i * 4, &mut buf)?;
    let rel = u32::from_le_bytes(buf) as u64;
    seg.body_base
        .checked_add(rel.checked_mul(IDX_STRIDE).ok_or(StoreError::Corrupt(
            "tx.idx stride overflow",
        ))?)
        .ok_or(StoreError::Corrupt("tx.idx abs overflow"))
}

fn find_segment(segs: &[Segment], id: u64) -> Option<&Segment> {
    if segs.is_empty() || id == 0 {
        return None;
    }
    // Binary search by first_fk.
    let mut lo = 0usize;
    let mut hi = segs.len();
    while lo < hi {
        let mid = (lo + hi) / 2;
        let s = &segs[mid];
        let end = s.first_fk + s.count; // exclusive
        if id < s.first_fk {
            hi = mid;
        } else if id >= end {
            lo = mid + 1;
        } else {
            return Some(s);
        }
    }
    None
}

#[derive(Clone, Copy)]
struct SegDesc {
    first_fk: u64,
    count: u64,
    body_base: u64,
    file_id: u32,
}

/// Directory holding `{stem}.idx` segments + meta (`store/tx.idx/…`).
#[inline]
fn idx_root(dir: &Path, stem: &str) -> PathBuf {
    dir.join(format!("{stem}.idx"))
}

fn segment_path(dir: &Path, stem: &str, file_id: u32) -> PathBuf {
    idx_root(dir, stem).join(format!("{file_id:06}"))
}

fn meta_path(dir: &Path, stem: &str) -> PathBuf {
    idx_root(dir, stem).join("meta")
}

fn flat_meta_path(dir: &Path, stem: &str) -> PathBuf {
    dir.join(format!("{stem}.idx.meta"))
}

fn flat_segment_path(dir: &Path, stem: &str, file_id: u32) -> PathBuf {
    dir.join(format!("{stem}.idx.{file_id:06}"))
}

/// Ensure `tx.idx/` exists; migrate flat `tx.idx.meta` + segment files if present.
fn ensure_idx_layout(dir: &Path, stem: &str) -> Result<(), StoreError> {
    let root = idx_root(dir, stem);
    let new_meta = meta_path(dir, stem);
    if new_meta.is_file() {
        return Ok(());
    }
    let flat_meta = flat_meta_path(dir, stem);
    std::fs::create_dir_all(&root).map_err(|e| StoreError::io(&root, e))?;
    if !flat_meta.is_file() {
        return Ok(());
    }
    // Read flat meta before rename so we know which segment files to move.
    let descs = read_meta_buf(
        &std::fs::read(&flat_meta).map_err(|e| StoreError::io(&flat_meta, e))?,
    )?;
    let mut moved = 0u32;
    for d in &descs {
        let src = flat_segment_path(dir, stem, d.file_id);
        let dst = segment_path(dir, stem, d.file_id);
        if src.is_file() {
            std::fs::rename(&src, &dst).map_err(|e| StoreError::io(&dst, e))?;
            moved = moved.saturating_add(1);
        }
    }
    // Catch any leftover flat segments (e.g. empty trailing file).
    if let Ok(rd) = std::fs::read_dir(dir) {
        let prefix = format!("{stem}.idx.");
        for ent in rd.flatten() {
            let name = ent.file_name();
            let s = name.to_string_lossy();
            if s == format!("{stem}.idx.meta") {
                continue;
            }
            if let Some(rest) = s.strip_prefix(&prefix) {
                if rest.chars().all(|c| c.is_ascii_digit()) && rest.len() == 6 {
                    let dst = root.join(rest);
                    if !dst.exists() && ent.path().is_file() {
                        let _ = std::fs::rename(ent.path(), &dst);
                        moved = moved.saturating_add(1);
                    }
                }
            }
        }
    }
    std::fs::rename(&flat_meta, &new_meta).map_err(|e| StoreError::io(&new_meta, e))?;
    rbitcoin_log::info!(
        "store: migrated {stem}.idx layout → {}/ (segments_moved={moved})",
        root.display()
    );
    Ok(())
}

fn write_meta(dir: &Path, stem: &str, descs: &[SegDesc]) -> Result<(), StoreError> {
    let path = meta_path(dir, stem);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|e| StoreError::io(parent, e))?;
    }
    let mut buf = Vec::with_capacity(META_HEADER_LEN + descs.len() * SEG_DESC_LEN);
    buf.extend_from_slice(&STORE_MAGIC);
    buf.extend_from_slice(&SCHEMA_VERSION.to_le_bytes());
    buf.extend_from_slice(&TableKind::ArrayLink.as_u16().to_le_bytes());
    buf.extend_from_slice(&META_VERSION.to_le_bytes());
    buf.extend_from_slice(&(descs.len() as u32).to_le_bytes());
    buf.extend_from_slice(&0u32.to_le_bytes()); // reserved
    for d in descs {
        buf.extend_from_slice(&d.first_fk.to_le_bytes());
        buf.extend_from_slice(&d.count.to_le_bytes());
        buf.extend_from_slice(&d.body_base.to_le_bytes());
        buf.extend_from_slice(&d.file_id.to_le_bytes());
        buf.extend_from_slice(&0u32.to_le_bytes());
    }
    // Atomic-ish replace: write temp + rename.
    let tmp = path.with_extension("tmp");
    std::fs::write(&tmp, &buf).map_err(|e| StoreError::io(&tmp, e))?;
    std::fs::rename(&tmp, &path).map_err(|e| StoreError::io(&path, e))?;
    Ok(())
}

fn write_meta_from_segs(dir: &Path, stem: &str, segs: &[Segment]) -> Result<(), StoreError> {
    let descs: Vec<SegDesc> = segs
        .iter()
        .map(|s| SegDesc {
            first_fk: s.first_fk,
            count: s.count,
            body_base: s.body_base,
            file_id: s.file_id,
        })
        .collect();
    write_meta(dir, stem, &descs)
}

fn read_meta(dir: &Path, stem: &str) -> Result<Vec<SegDesc>, StoreError> {
    let path = meta_path(dir, stem);
    let buf = std::fs::read(&path).map_err(|e| StoreError::io(&path, e))?;
    read_meta_buf(&buf)
}

fn read_meta_buf(buf: &[u8]) -> Result<Vec<SegDesc>, StoreError> {
    if buf.len() < META_HEADER_LEN {
        return Err(StoreError::Corrupt("tx.idx.meta short"));
    }
    if buf[0..4] != STORE_MAGIC {
        return Err(StoreError::Corrupt("tx.idx.meta magic"));
    }
    let ver = u16::from_le_bytes(buf[4..6].try_into().unwrap());
    if ver != SCHEMA_VERSION {
        return Err(StoreError::Corrupt("tx.idx.meta schema"));
    }
    let meta_ver = u32::from_le_bytes(buf[8..12].try_into().unwrap());
    if meta_ver != META_VERSION {
        return Err(StoreError::Corrupt("tx.idx.meta version"));
    }
    let n = u32::from_le_bytes(buf[12..16].try_into().unwrap()) as usize;
    let need = META_HEADER_LEN + n * SEG_DESC_LEN;
    if buf.len() < need {
        return Err(StoreError::Corrupt("tx.idx.meta truncated"));
    }
    let mut out = Vec::with_capacity(n);
    for i in 0..n {
        let o = META_HEADER_LEN + i * SEG_DESC_LEN;
        let first_fk = u64::from_le_bytes(buf[o..o + 8].try_into().unwrap());
        let count = u64::from_le_bytes(buf[o + 8..o + 16].try_into().unwrap());
        let body_base = u64::from_le_bytes(buf[o + 16..o + 24].try_into().unwrap());
        let file_id = u32::from_le_bytes(buf[o + 24..o + 28].try_into().unwrap());
        if first_fk == 0 && count > 0 {
            return Err(StoreError::Corrupt("tx.idx.meta first_fk"));
        }
        if body_base % IDX_STRIDE != 0 {
            return Err(StoreError::Corrupt("tx.idx.meta body_base"));
        }
        out.push(SegDesc {
            first_fk,
            count,
            body_base,
            file_id,
        });
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn multi_segment_stride_roundtrip() {
        let dir = std::env::temp_dir().join(format!(
            "rbitcoin-txidx-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        // Tiny soft span → many segments.
        std::env::set_var("RBITCOIN_TX_IDX_SOFT_SPAN", "64");
        let idx = TxIdx::create(&dir, "tx").unwrap();
        // Three batches that force rolls (span > 64).
        let s1 = [16u64, 24, 32, 40];
        idx.append_starts(0, &s1).unwrap();
        assert_eq!(idx.slot_count(), 4);
        let s2 = [16 + 128, 16 + 128 + 16]; // far → new segment
        idx.append_starts(4, &s2).unwrap();
        assert!(idx.segment_count() >= 2);
        assert_eq!(idx.record_start(1).unwrap(), 16);
        assert_eq!(idx.record_start(5).unwrap(), 16 + 128);
        let (off, len) = idx.record_range_interior(4).unwrap();
        assert_eq!(off, 40);
        assert_eq!(len, (16 + 128) - 40);
        // Reopen.
        drop(idx);
        let idx = TxIdx::open(&dir, "tx").unwrap();
        assert_eq!(idx.slot_count(), 6);
        assert_eq!(idx.record_start(6).unwrap(), 16 + 128 + 16);
        let ranges = idx.record_ranges(1, 6, 6, 16 + 128 + 16 + 8).unwrap();
        assert_eq!(ranges.len(), 6);
        assert_eq!(ranges[0].0, 16);
        std::env::remove_var("RBITCOIN_TX_IDX_SOFT_SPAN");
        // New layout lives under tx.idx/
        assert!(dir.join("tx.idx").join("meta").is_file());
        assert!(!dir.join("tx.idx.meta").exists());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn migrates_flat_idx_layout_on_open() {
        let dir = std::env::temp_dir().join(format!(
            "rbitcoin-txidx-migrate-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        std::env::set_var("RBITCOIN_TX_IDX_SOFT_SPAN", "64");
        {
            let idx = TxIdx::create(&dir, "tx").unwrap();
            idx.append_starts(0, &[16u64, 24, 32, 40]).unwrap();
            idx.append_starts(4, &[16 + 128, 16 + 128 + 16]).unwrap();
            idx.flush().unwrap();
        }
        // Flatten layout back to legacy paths (simulate old datadir).
        let root = dir.join("tx.idx");
        assert!(root.is_dir());
        let meta_new = root.join("meta");
        let meta_flat = dir.join("tx.idx.meta");
        std::fs::rename(&meta_new, &meta_flat).unwrap();
        for ent in std::fs::read_dir(&root).unwrap().flatten() {
            let name = ent.file_name();
            let s = name.to_string_lossy();
            if s.chars().all(|c| c.is_ascii_digit()) {
                let dst = dir.join(format!("tx.idx.{s}"));
                std::fs::rename(ent.path(), &dst).unwrap();
            }
        }
        let _ = std::fs::remove_dir_all(&root);
        assert!(meta_flat.is_file());
        assert!(!dir.join("tx.idx").join("meta").exists());

        let idx = TxIdx::open(&dir, "tx").unwrap();
        assert_eq!(idx.slot_count(), 6);
        assert_eq!(idx.record_start(1).unwrap(), 16);
        assert!(dir.join("tx.idx").join("meta").is_file());
        assert!(!meta_flat.exists());
        std::env::remove_var("RBITCOIN_TX_IDX_SOFT_SPAN");
        let _ = std::fs::remove_dir_all(&dir);
    }
}
