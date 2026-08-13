//! Thin BIP-352 tweak index (`sp_tweaks.idx` + `sp_tweaks.body`).
//!
//! Optional schema-14 side product. Missing files are not [`StoreError::Corrupt`].
//! Persist is **tweaks only** — no txids, outs, or parent scripts.
//!
//! ```text
//! idx slot[i]  = block_fk:u64 ‖ off:u32     // height = origin + i
//! body record  = u8 len ‖ [u8; len]         // 0 = none; 33 = compressed A_tweak
//! ```

use crate::error::StoreError;
use crate::file::{TableFile, FILE_HEADER_LEN};
use rbitcoin_primitives::{Fk, Height, TableKind};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

/// Compressed BIP-352 server tweak (Cake wire).
pub const TWEAK_LEN: u8 = 33;
/// Idx bytes after the 16-byte table header: `origin_height:u32` + pad.
const IDX_PREFIX: u64 = FILE_HEADER_LEN as u64 + 8;
const SLOT: u64 = 12;

/// One SP-era height: confirmed `header_fk` + start of that block’s body run.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SpTweaksSlot {
    pub block_fk: Fk,
    pub off: u32,
}

/// Height-dense thin tweak table.
pub struct SpTweaksTable {
    idx: TableFile,
    body: TableFile,
    origin: u32,
    slots: AtomicU64,
}

impl SpTweaksTable {
    pub fn idx_path(dir: &Path) -> PathBuf {
        dir.join("sp_tweaks.idx")
    }

    pub fn body_path(dir: &Path) -> PathBuf {
        dir.join("sp_tweaks.body")
    }

    pub fn files_present(dir: &Path) -> bool {
        Self::idx_path(dir).exists() && Self::body_path(dir).exists()
    }

    pub fn create(dir: impl AsRef<Path>, origin: Height) -> Result<Self, StoreError> {
        let dir = dir.as_ref();
        let idx = TableFile::create(Self::idx_path(dir), TableKind::ArrayLink)?;
        let body = TableFile::create(Self::body_path(dir), TableKind::SpTweaks)?;
        let mut prefix = [0u8; 8];
        prefix[..4].copy_from_slice(&origin.0.to_le_bytes());
        idx.write_at_pwrite(FILE_HEADER_LEN as u64, &prefix)?;
        Ok(Self {
            idx,
            body,
            origin: origin.0,
            slots: AtomicU64::new(0),
        })
    }

    pub fn open(dir: impl AsRef<Path>) -> Result<Self, StoreError> {
        let dir = dir.as_ref();
        let idx = TableFile::open(Self::idx_path(dir), TableKind::ArrayLink)?;
        let body = TableFile::open(Self::body_path(dir), TableKind::SpTweaks)?;
        if idx.logical_len() < IDX_PREFIX {
            return Err(StoreError::Corrupt("sp_tweaks.idx missing origin"));
        }
        let mut prefix = [0u8; 8];
        idx.read_at(FILE_HEADER_LEN as u64, &mut prefix)?;
        let origin = u32::from_le_bytes(prefix[..4].try_into().unwrap());
        let extra = idx.logical_len().saturating_sub(IDX_PREFIX);
        if extra % SLOT != 0 {
            return Err(StoreError::Corrupt("sp_tweaks.idx size"));
        }
        Ok(Self {
            idx,
            body,
            origin,
            slots: AtomicU64::new(extra / SLOT),
        })
    }

    /// Open existing files, or create empty ones. Missing pair is not corrupt.
    pub fn open_or_create(dir: impl AsRef<Path>, origin: Height) -> Result<Self, StoreError> {
        let dir = dir.as_ref();
        let idx_p = Self::idx_path(dir);
        let body_p = Self::body_path(dir);
        match (idx_p.exists(), body_p.exists()) {
            (true, true) => {
                let t = Self::open(dir)?;
                if t.origin != origin.0 {
                    return Err(StoreError::Corrupt("sp_tweaks origin mismatch"));
                }
                Ok(t)
            }
            (false, false) => {
                rbitcoin_log::warn!(
                    "sp_tweaks: creating empty index origin={} dir={}",
                    origin.0,
                    dir.display()
                );
                Self::create(dir, origin)
            }
            (true, false) | (false, true) => {
                rbitcoin_log::warn!(
                    "sp_tweaks: incomplete files (idx={} body={}); recreating origin={}",
                    idx_p.exists(),
                    body_p.exists(),
                    origin.0
                );
                let _ = std::fs::remove_file(&idx_p);
                let _ = std::fs::remove_file(&body_p);
                Self::create(dir, origin)
            }
        }
    }

    pub fn origin_height(&self) -> Height {
        Height(self.origin)
    }

    pub fn slot_count(&self) -> u64 {
        self.slots.load(Ordering::Acquire)
    }

    /// Next height this table will accept (`origin` when empty).
    pub fn next_height(&self) -> Height {
        Height(self.origin.saturating_add(self.slot_count() as u32))
    }

    pub fn body_logical_len(&self) -> u64 {
        self.body.logical_len()
    }

    pub fn flush(&self) -> Result<(), StoreError> {
        self.body.flush()?;
        self.idx.flush()
    }

    fn slot_off(i: u64) -> u64 {
        IDX_PREFIX + i * SLOT
    }

    fn read_slot(&self, i: u64) -> Result<SpTweaksSlot, StoreError> {
        let mut buf = [0u8; SLOT as usize];
        self.idx.read_at(Self::slot_off(i), &mut buf)?;
        let fk = u64::from_le_bytes(buf[0..8].try_into().unwrap());
        let off = u32::from_le_bytes(buf[8..12].try_into().unwrap());
        Ok(SpTweaksSlot {
            block_fk: Fk(fk),
            off,
        })
    }

    fn write_slot(&self, i: u64, slot: SpTweaksSlot) -> Result<(), StoreError> {
        let mut buf = [0u8; SLOT as usize];
        buf[0..8].copy_from_slice(&slot.block_fk.0.to_le_bytes());
        buf[8..12].copy_from_slice(&slot.off.to_le_bytes());
        self.idx.write_at_pwrite(Self::slot_off(i), &buf)
    }

    /// Encode one tx: `0` or `33 ‖ tweak`.
    pub fn encode_records(records: &[Option<[u8; 33]>]) -> Vec<u8> {
        let mut out = Vec::with_capacity(records.len().saturating_mul(2));
        for r in records {
            match r {
                None => out.push(0),
                Some(t) => {
                    out.push(TWEAK_LEN);
                    out.extend_from_slice(t);
                }
            }
        }
        out
    }

    fn decode_records(bytes: &[u8], n_tx: u32) -> Result<Vec<Option<[u8; 33]>>, StoreError> {
        let mut recs = Vec::with_capacity(n_tx as usize);
        let mut i = 0usize;
        for _ in 0..n_tx {
            if i >= bytes.len() {
                return Err(StoreError::Corrupt("sp_tweaks short body"));
            }
            let len = bytes[i];
            i += 1;
            match len {
                0 => recs.push(None),
                TWEAK_LEN => {
                    if i + 33 > bytes.len() {
                        return Err(StoreError::Corrupt("sp_tweaks short tweak"));
                    }
                    let mut t = [0u8; 33];
                    t.copy_from_slice(&bytes[i..i + 33]);
                    recs.push(Some(t));
                    i += 33;
                }
                _ => return Err(StoreError::Corrupt("sp_tweaks bad len (want 0 or 33)")),
            }
        }
        if i != bytes.len() {
            return Err(StoreError::Corrupt("sp_tweaks n_tx/body mismatch"));
        }
        Ok(recs)
    }

    fn decode_eligible(bytes: &[u8], n_tx: u32) -> Result<Vec<(u32, [u8; 33])>, StoreError> {
        let mut elig = Vec::new();
        let mut i = 0usize;
        for tx_i in 0..n_tx {
            if i >= bytes.len() {
                return Err(StoreError::Corrupt("sp_tweaks short body"));
            }
            let len = bytes[i];
            i += 1;
            match len {
                0 => {}
                TWEAK_LEN => {
                    if i + 33 > bytes.len() {
                        return Err(StoreError::Corrupt("sp_tweaks short tweak"));
                    }
                    let mut t = [0u8; 33];
                    t.copy_from_slice(&bytes[i..i + 33]);
                    elig.push((tx_i, t));
                    i += 33;
                }
                _ => return Err(StoreError::Corrupt("sp_tweaks bad len (want 0 or 33)")),
            }
        }
        if i != bytes.len() {
            return Err(StoreError::Corrupt("sp_tweaks n_tx/body mismatch"));
        }
        Ok(elig)
    }

    /// Append the next SP-era height. `records.len()` is `header_txs` count.
    pub fn put_block(
        &self,
        height: Height,
        block_fk: Fk,
        records: &[Option<[u8; 33]>],
    ) -> Result<(), StoreError> {
        if block_fk.is_null() {
            return Err(StoreError::InvalidFk);
        }
        if height.0 < self.origin {
            return Err(StoreError::Corrupt("sp_tweaks put below origin"));
        }
        if height != self.next_height() {
            return Err(StoreError::Corrupt("sp_tweaks put not next height"));
        }
        let body_off = self.body.logical_len();
        if body_off > u32::MAX as u64 {
            return Err(StoreError::Corrupt("sp_tweaks body exceeds u32 off"));
        }
        let encoded = Self::encode_records(records);
        if !encoded.is_empty() {
            self.body.write_at_pwrite(body_off, &encoded)?;
        }
        let i = self.slot_count();
        self.write_slot(
            i,
            SpTweaksSlot {
                block_fk,
                off: body_off as u32,
            },
        )?;
        self.slots.store(i + 1, Ordering::Release);
        Ok(())
    }

    /// `None` = hole (missing slot or `block_fk` mismatch). Present empty-eligible
    /// is `Some` of `n_tx` `None`s.
    pub fn get_block(
        &self,
        height: Height,
        block_fk: Fk,
        n_tx: u32,
    ) -> Result<Option<Vec<Option<[u8; 33]>>>, StoreError> {
        if height.0 < self.origin {
            return Ok(None);
        }
        let i = u64::from(height.0 - self.origin);
        let n = self.slot_count();
        if i >= n {
            return Ok(None);
        }
        let slot = self.read_slot(i)?;
        if slot.block_fk != block_fk {
            return Ok(None);
        }
        let start = u64::from(slot.off);
        let end = if i + 1 < n {
            u64::from(self.read_slot(i + 1)?.off)
        } else {
            self.body.logical_len()
        };
        if end < start {
            return Err(StoreError::Corrupt("sp_tweaks off order"));
        }
        let len = (end - start) as usize;
        if start < FILE_HEADER_LEN as u64 {
            return Err(StoreError::Corrupt("sp_tweaks off in header"));
        }
        let mut buf = vec![0u8; len];
        if len > 0 {
            self.body.read_at(start, &mut buf)?;
        }
        Ok(Some(Self::decode_records(&buf, n_tx)?))
    }

    /// Eligible tweaks only: `(tx_index_in_block, tweak)`. Hole → `None`.
    pub fn get_eligible(
        &self,
        height: Height,
        block_fk: Fk,
        n_tx: u32,
    ) -> Result<Option<Vec<(u32, [u8; 33])>>, StoreError> {
        if height.0 < self.origin {
            return Ok(None);
        }
        let i = u64::from(height.0 - self.origin);
        let n = self.slot_count();
        if i >= n {
            return Ok(None);
        }
        let slot = self.read_slot(i)?;
        if slot.block_fk != block_fk {
            return Ok(None);
        }
        let start = u64::from(slot.off);
        let end = if i + 1 < n {
            u64::from(self.read_slot(i + 1)?.off)
        } else {
            self.body.logical_len()
        };
        if end < start {
            return Err(StoreError::Corrupt("sp_tweaks off order"));
        }
        let len = (end - start) as usize;
        if start < FILE_HEADER_LEN as u64 {
            return Err(StoreError::Corrupt("sp_tweaks off in header"));
        }
        let mut buf = vec![0u8; len];
        if len > 0 {
            self.body.read_at(start, &mut buf)?;
        }
        Ok(Some(Self::decode_eligible(&buf, n_tx)?))
    }

    /// Drop heights **above** `new_tip` (inclusive keep `origin..=new_tip`).
    pub fn truncate_above(&self, new_tip: Height) -> Result<(), StoreError> {
        let keep = if new_tip.0 < self.origin {
            0
        } else {
            u64::from(new_tip.0 - self.origin + 1)
        };
        self.truncate_keep_slots(keep)
    }

    /// `tip == None` drops every slot (empty chain).
    pub fn truncate_through_tip(&self, tip: Option<Height>) -> Result<(), StoreError> {
        match tip {
            None => self.truncate_keep_slots(0),
            Some(h) => self.truncate_above(h),
        }
    }

    fn truncate_keep_slots(&self, keep: u64) -> Result<(), StoreError> {
        let n = self.slot_count();
        if keep >= n {
            return Ok(());
        }
        let new_body = if keep == 0 {
            FILE_HEADER_LEN as u64
        } else {
            u64::from(self.read_slot(keep)?.off)
        };
        self.idx.set_logical_len(IDX_PREFIX + keep * SLOT)?;
        self.body
            .set_logical_len(new_body.max(FILE_HEADER_LEN as u64))?;
        self.slots.store(keep, Ordering::Release);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn tmp_dir() -> PathBuf {
        let p = std::env::temp_dir().join(format!(
            "rbitcoin-sptweaks-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0)
        ));
        fs::create_dir_all(&p).unwrap();
        p
    }

    #[test]
    fn put_get_len_tweak_no_txid() {
        let dir = tmp_dir();
        let t = SpTweaksTable::create(&dir, Height(0)).unwrap();
        let mut tweak = [0u8; 33];
        tweak[0] = 0x02;
        tweak[32] = 0xab;
        t.put_block(Height(0), Fk(7), &[None, Some(tweak), None])
            .unwrap();

        let got = t.get_block(Height(0), Fk(7), 3).unwrap().expect("present");
        assert_eq!(got, vec![None, Some(tweak), None]);

        // Body is `00 21 <33> 00` — no txid, no outs. File may be fallocate-padded.
        let want = SpTweaksTable::encode_records(&[None, Some(tweak), None]);
        assert_eq!(want.len(), 1 + 1 + 33 + 1);
        assert_eq!(want[0], 0);
        assert_eq!(want[1], 33);
        let body_path = SpTweaksTable::body_path(&dir);
        let raw = fs::read(&body_path).unwrap();
        let pub_len = t.body_logical_len() as usize;
        assert_eq!(pub_len, FILE_HEADER_LEN + want.len());
        assert_eq!(&raw[FILE_HEADER_LEN..pub_len], want.as_slice());

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn hole_vs_empty_eligible() {
        let dir = tmp_dir();
        let t = SpTweaksTable::create(&dir, Height(10)).unwrap();
        t.put_block(Height(10), Fk(1), &[None, None]).unwrap();
        let empty = t.get_block(Height(10), Fk(1), 2).unwrap().expect("present");
        assert_eq!(empty, vec![None, None]);
        assert!(t.get_block(Height(11), Fk(1), 1).unwrap().is_none());
        assert!(t.get_block(Height(9), Fk(1), 1).unwrap().is_none());
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn block_fk_mismatch_is_hole() {
        let dir = tmp_dir();
        let t = SpTweaksTable::create(&dir, Height(0)).unwrap();
        t.put_block(Height(0), Fk(3), &[None]).unwrap();
        assert!(t.get_block(Height(0), Fk(99), 1).unwrap().is_none());
        assert!(t.get_block(Height(0), Fk(3), 1).unwrap().is_some());
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn truncate_above_and_reopen() {
        let dir = tmp_dir();
        let t = SpTweaksTable::create(&dir, Height(0)).unwrap();
        let mut a = [0u8; 33];
        a[0] = 0x03;
        t.put_block(Height(0), Fk(1), &[Some(a)]).unwrap();
        t.put_block(Height(1), Fk(2), &[None]).unwrap();
        t.put_block(Height(2), Fk(3), &[None, None]).unwrap();
        assert_eq!(t.next_height(), Height(3));
        t.truncate_above(Height(0)).unwrap();
        assert_eq!(t.next_height(), Height(1));
        assert!(t.get_block(Height(1), Fk(2), 1).unwrap().is_none());
        let got = t.get_block(Height(0), Fk(1), 1).unwrap().unwrap();
        assert_eq!(got, vec![Some(a)]);

        t.flush().unwrap();
        drop(t);
        let t = SpTweaksTable::open(&dir).unwrap();
        assert_eq!(t.origin_height(), Height(0));
        assert_eq!(t.next_height(), Height(1));
        assert_eq!(
            t.get_block(Height(0), Fk(1), 1).unwrap().unwrap(),
            vec![Some(a)]
        );
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn missing_files_open_or_create_not_corrupt() {
        let dir = tmp_dir();
        let t = SpTweaksTable::open_or_create(&dir, Height(5)).unwrap();
        assert_eq!(t.origin_height(), Height(5));
        assert_eq!(t.slot_count(), 0);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn refuse_fat_len() {
        let dir = tmp_dir();
        let t = SpTweaksTable::create(&dir, Height(0)).unwrap();
        t.put_block(Height(0), Fk(1), &[None]).unwrap();
        t.flush().unwrap();
        drop(t);
        // Corrupt the body record to a fat length.
        let mut raw = fs::read(SpTweaksTable::body_path(&dir)).unwrap();
        raw[FILE_HEADER_LEN] = 32; // not 0 or 33
        fs::write(SpTweaksTable::body_path(&dir), &raw).unwrap();
        let t = SpTweaksTable::open(&dir).unwrap();
        let err = t.get_block(Height(0), Fk(1), 1).unwrap_err();
        assert!(
            matches!(err, StoreError::Corrupt(m) if m.contains("bad len")),
            "got {err:?}"
        );
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn put_not_next_height_errors() {
        let dir = tmp_dir();
        let t = SpTweaksTable::create(&dir, Height(0)).unwrap();
        let err = t.put_block(Height(2), Fk(1), &[None]).unwrap_err();
        assert!(matches!(err, StoreError::Corrupt(_)));
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn open_or_create_repairs_incomplete_and_checks_origin() {
        let dir = tmp_dir();
        fs::write(SpTweaksTable::idx_path(&dir), b"partial").unwrap();
        let t = SpTweaksTable::open_or_create(&dir, Height(3)).unwrap();
        assert_eq!(t.origin_height(), Height(3));
        drop(t);
        let Err(err) = SpTweaksTable::open_or_create(&dir, Height(9)) else {
            panic!("expected origin mismatch");
        };
        assert!(
            matches!(err, StoreError::Corrupt(m) if m.contains("origin")),
            "{err:?}"
        );
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn open_rejects_truncated_idx() {
        let dir = tmp_dir();
        let t = SpTweaksTable::create(&dir, Height(0)).unwrap();
        t.flush().unwrap();
        drop(t);
        // Keep header but wipe origin prefix.
        let p = SpTweaksTable::idx_path(&dir);
        let mut raw = fs::read(&p).unwrap();
        raw.truncate(FILE_HEADER_LEN);
        fs::write(&p, &raw).unwrap();
        let Err(err) = SpTweaksTable::open(&dir) else {
            panic!("expected truncated idx");
        };
        assert!(matches!(err, StoreError::Corrupt(_)), "{err:?}");
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn put_null_fk_and_below_origin() {
        let dir = tmp_dir();
        let t = SpTweaksTable::create(&dir, Height(5)).unwrap();
        assert!(matches!(
            t.put_block(Height(5), Fk::NULL, &[None]),
            Err(StoreError::InvalidFk)
        ));
        assert!(matches!(
            t.put_block(Height(4), Fk(1), &[None]),
            Err(StoreError::Corrupt(_))
        ));
        t.put_block(Height(5), Fk(1), &[None]).unwrap();
        // Wrong n_tx vs stored bytes.
        let err = t.get_block(Height(5), Fk(1), 2).unwrap_err();
        assert!(matches!(err, StoreError::Corrupt(_)), "{err:?}");
        t.truncate_through_tip(None).unwrap();
        assert_eq!(t.slot_count(), 0);
        t.truncate_above(Height(99)).unwrap();
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn decode_short_tweak_and_n_tx_mismatch() {
        let dir = tmp_dir();
        let t = SpTweaksTable::create(&dir, Height(0)).unwrap();
        let mut tw = [0u8; 33];
        tw[0] = 0x02;
        t.put_block(Height(0), Fk(1), &[Some(tw)]).unwrap();
        t.flush().unwrap();
        drop(t);
        let p = SpTweaksTable::body_path(&dir);
        let mut raw = fs::read(&p).unwrap();
        // Truncate payload so 33-byte tweak is short.
        raw.truncate(FILE_HEADER_LEN + 1 + 8);
        fs::write(&p, &raw).unwrap();
        let t = SpTweaksTable::open(&dir).unwrap();
        let err = t.get_block(Height(0), Fk(1), 1).unwrap_err();
        assert!(matches!(err, StoreError::Corrupt(_)), "{err:?}");
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn get_two_heights_uses_next_slot_off() {
        let dir = tmp_dir();
        let t = SpTweaksTable::create(&dir, Height(0)).unwrap();
        t.put_block(Height(0), Fk(1), &[None]).unwrap();
        t.put_block(Height(1), Fk(2), &[None, None]).unwrap();
        assert_eq!(
            t.get_block(Height(0), Fk(1), 1).unwrap().unwrap(),
            vec![None]
        );
        assert_eq!(
            t.get_block(Height(1), Fk(2), 2).unwrap().unwrap(),
            vec![None, None]
        );
        let _ = fs::remove_dir_all(&dir);
    }
}
