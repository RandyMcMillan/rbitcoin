//! Paired `idx/NNNNNN` + `body/NNNNNN` segment files for height-dense side
//! tables (sp_tweaks, blockfilter). Each pair holds a run of slots starting
//! at `first_slot`; a table rolls to the next pair when body offsets would
//! pass `u32`.

use crate::error::StoreError;
use crate::file::{TableFile, FILE_HEADER_LEN};
use rbitcoin_primitives::TableKind;
use std::path::PathBuf;

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
    /// and lose the other. No length is published before both names are
    /// synced, so that pair holds no row: open removes it. One missing file
    /// in any earlier pair is still corruption.
    pub fn open(&self, slot: u64) -> Result<Vec<SegPair>, StoreError> {
        let mut segs = Vec::new();
        let mut first_slot = 0u64;
        for file_id in 0u32.. {
            let (ip, bp) = (self.idx_path(file_id), self.body_path(file_id));
            if !ip.exists() && !bp.exists() {
                break;
            }
            if !ip.exists() || !bp.exists() {
                let next = file_id.saturating_add(1);
                if self.idx_path(next).exists() || self.body_path(next).exists() {
                    return Err(StoreError::Corrupt("segment pair incomplete"));
                }
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

/// Pair index and local slot of global slot `g`, if written.
pub fn locate(segs: &[SegPair], g: u64) -> Option<(usize, u64)> {
    segs.iter()
        .enumerate()
        .find(|(_, s)| g >= s.first_slot && g < s.first_slot + s.n_slots)
        .map(|(si, s)| (si, g - s.first_slot))
}
