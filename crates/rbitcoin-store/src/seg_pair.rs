//! Paired `idx/NNNNNN` + `body/NNNNNN` segment files for height-dense side
//! tables (sp_tweaks, blockfilter). Each pair holds a run of slots starting
//! at `first_slot`; a table rolls to the next pair when body offsets would
//! pass `u32`.

use crate::error::StoreError;
use crate::file::{TableFile, FILE_HEADER_LEN};
use rbitcoin_primitives::{le_u64, TableKind, STORE_MAGIC};
use std::io::Read;
use std::path::{Path, PathBuf};

pub struct SegPair {
    pub file_id: u32,
    pub first_slot: u64,
    pub n_slots: u64,
    pub idx: TableFile,
    pub body: TableFile,
    /// End of the last record for readers, below the body length. A failed
    /// put sets it until reopen, so the old last record does not read the
    /// put's bytes while the disk lengths stay as the put left them.
    pub read_end: Option<u64>,
}

impl SegPair {
    /// Where the last record ends for a reader.
    pub fn last_record_end(&self) -> u64 {
        self.read_end.unwrap_or_else(|| self.body.logical_len())
    }
}

pub struct SegDirs {
    pub idx: PathBuf,
    pub body: PathBuf,
    pub body_kind: TableKind,
}

impl SegDirs {
    pub fn new(idx: PathBuf, body: PathBuf, body_kind: TableKind) -> Self {
        Self {
            idx,
            body,
            body_kind,
        }
    }

    pub fn idx_path(&self, file_id: u32) -> PathBuf {
        self.idx.join(format!("{file_id:06}"))
    }

    pub fn body_path(&self, file_id: u32) -> PathBuf {
        self.body.join(format!("{file_id:06}"))
    }

    /// Create pair `file_id`. On an error neither file is left behind, so
    /// the next roll can create it again.
    pub fn create(&self, file_id: u32, first_slot: u64) -> Result<SegPair, StoreError> {
        let made = self.create_files(file_id, first_slot);
        if made.is_err() {
            self.discard(file_id);
        }
        made
    }

    fn create_files(&self, file_id: u32, first_slot: u64) -> Result<SegPair, StoreError> {
        let idx = TableFile::create(self.idx_path(file_id), TableKind::ArrayLink)?;
        idx.set_grow_tight(true);
        let body = TableFile::create(self.body_path(file_id), self.body_kind)?;
        // A put appends with no mark, so the new names have to be durable
        // before a length in either file can be, and a name that survives
        // must find its header.
        idx.sync_data_only()?;
        body.sync_data_only()?;
        for d in [&self.idx, &self.body] {
            crate::file::fsync_parent_dir(d).map_err(|e| StoreError::io(d, e))?;
        }
        Ok(SegPair {
            file_id,
            first_slot,
            n_slots: 0,
            idx,
            body,
            read_end: None,
        })
    }

    /// Unlink both files of pair `file_id` and sync their directories. Best
    /// effort: a pair that holds no committed row may be left for open.
    fn discard(&self, file_id: u32) {
        let _ = std::fs::remove_file(self.idx_path(file_id));
        let _ = std::fs::remove_file(self.body_path(file_id));
        for d in [&self.idx, &self.body] {
            let _ = crate::file::fsync_parent_dir(d);
        }
    }

    /// Open `000000..` until both files of a pair are missing. `n_slots` is
    /// the whole slots in each idx; callers check any remainder.
    ///
    /// A crash inside [`Self::create`] can keep one name of the newest pair
    /// and lose the other, or keep both names while a header never reached
    /// the disk: a filesystem may commit a new name before its data. No
    /// length is published before both headers are synced, so that pair
    /// holds no row: open removes it. In any earlier pair, or in a newest
    /// pair where either surviving file shows a published length, either is
    /// still corruption and nothing is unlinked. A
    /// presence check that fails is an error, never a missing file.
    pub fn open(&self, slot: u64) -> Result<Vec<SegPair>, StoreError> {
        let mut segs = Vec::new();
        let mut first_slot = 0u64;
        for file_id in 0u32.. {
            let (ip, bp) = (self.idx_path(file_id), self.body_path(file_id));
            let (has_idx, has_body) = (present(&ip)?, present(&bp)?);
            if !has_idx && !has_body {
                break;
            }
            let next = file_id.saturating_add(1);
            let newest = !present(&self.idx_path(next))? && !present(&self.body_path(next))?;
            if !has_idx || !has_body {
                if !newest {
                    return Err(StoreError::Corrupt("segment pair incomplete"));
                }
                let survivor = if has_idx { &ip } else { &bp };
                if header_len(survivor)?.is_some_and(|len| len > FILE_HEADER_LEN as u64) {
                    return Err(StoreError::Corrupt(
                        "newest segment pair has published rows",
                    ));
                }
                self.discard(file_id);
                break;
            }
            if newest && never_written(&ip, &bp)? {
                self.discard(file_id);
                break;
            }
            let idx = TableFile::open(&ip, TableKind::ArrayLink)?;
            idx.set_grow_tight(true);
            let body = TableFile::open(&bp, self.body_kind)?;
            let n_slots = idx.logical_len().saturating_sub(FILE_HEADER_LEN as u64) / slot;
            segs.push(SegPair {
                file_id,
                first_slot,
                n_slots,
                idx,
                body,
                read_end: None,
            });
            first_slot += n_slots;
        }
        if segs.is_empty() {
            return Err(StoreError::Corrupt("segment pairs missing"));
        }
        Ok(segs)
    }

    /// Append the next pair after the last one.
    pub fn roll(&self, segs: &mut Vec<SegPair>) -> Result<(), StoreError> {
        let file_id =
            u32::try_from(segs.len()).map_err(|_| StoreError::Corrupt("too many segment pairs"))?;
        let first_slot = segs.iter().map(|s| s.n_slots).sum();
        segs.push(self.create(file_id, first_slot)?);
        Ok(())
    }

    /// Keep the first `keep` pairs and unlink the rest.
    pub fn drop_after(&self, segs: &mut Vec<SegPair>, keep: usize) {
        let ids: Vec<u32> = segs.iter().skip(keep).map(|s| s.file_id).collect();
        // Close before unlink: Windows refuses to remove an open file.
        segs.truncate(keep);
        for id in ids {
            let _ = std::fs::remove_file(self.idx_path(id));
            let _ = std::fs::remove_file(self.body_path(id));
        }
    }
}

/// `Ok(false)` only when `path` is absent. Any other stat error is returned.
fn present(path: &Path) -> Result<bool, StoreError> {
    path.try_exists().map_err(|e| StoreError::io(path, e))
}

/// The logical length in `path`'s header, or `None` when the file is shorter
/// than a header or does not start with the store magic: a header that never
/// reached the disk.
fn header_len(path: &Path) -> Result<Option<u64>, StoreError> {
    let mut header = [0u8; FILE_HEADER_LEN];
    let read = std::fs::File::open(path).and_then(|mut f| f.read_exact(&mut header));
    match read {
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => Ok(None),
        Err(e) => Err(StoreError::io(path, e)),
        Ok(()) if header[0..4] != STORE_MAGIC => Ok(None),
        Ok(()) => Ok(Some(le_u64!(header, 8))),
    }
}

/// A pair whose header did not persist, where neither file shows a length
/// past its header. Its create never returned, so no row was published.
fn never_written(idx: &Path, body: &Path) -> Result<bool, StoreError> {
    let hdr = FILE_HEADER_LEN as u64;
    Ok(match (header_len(idx)?, header_len(body)?) {
        (Some(_), Some(_)) => false,
        (i, b) => i.unwrap_or(hdr) <= hdr && b.unwrap_or(hdr) <= hdr,
    })
}

/// Pair index and local slot of global slot `g`, if written.
pub fn locate(segs: &[SegPair], g: u64) -> Option<(usize, u64)> {
    segs.iter()
        .enumerate()
        .find(|(_, s)| g >= s.first_slot && g < s.first_slot + s.n_slots)
        .map(|(si, s)| (si, g - s.first_slot))
}
