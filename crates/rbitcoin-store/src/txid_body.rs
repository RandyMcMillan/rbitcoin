//! Dense **create_fk-ordered** txid sidefile (`txid.body`).
//!
//! Layout (schema 13+):
//! ```text
//! offset 0..32   — 32-byte file header (standard 16-byte TableFile header + 16 pad)
//! offset 32+i*32 — txid for create_fk = i+1
//! ```
//!
//! Append-published with Class A body/idx on the sole Class A write path.
//! Identity peeks use fixed `off = 32 + (fk-1)*32` — no `tx.idx` / body prefix.

use crate::error::StoreError;
use crate::file::{TableFile, FILE_HEADER_LEN};
use rbitcoin_primitives::{Fk, TableKind};
use std::path::{Path, PathBuf};

/// Bytes before first txid entry (TableFile header 16 + pad to 32).
pub const TXID_BODY_HEADER: u64 = 32;
/// One create identity.
pub const TXID_ENTRY_LEN: u64 = 32;
/// Sidefile reads more than this many entries from the **tail** use DONTCACHE.
pub const TXID_DONTCACHE_FROM_TAIL: u64 = 100_000_000;

/// Dense create_fk → txid table.
pub struct TxidBody {
    file: TableFile,
    /// Published entry count (matches Class A body count when consistent).
    count: std::sync::atomic::AtomicU64,
}

impl TxidBody {
    pub fn create(dir: &Path) -> Result<Self, StoreError> {
        let path = Self::path(dir);
        let file = TableFile::create(path, TableKind::TxidBody)?;
        // Pad header region to 32 so entries start page-friendly / plan layout.
        let pad = vec![0u8; (TXID_BODY_HEADER as usize).saturating_sub(FILE_HEADER_LEN)];
        if !pad.is_empty() {
            file.write_at_pwrite(FILE_HEADER_LEN as u64, &pad)?;
            // Logical end already advanced by write_at_pwrite HWM.
        }
        Ok(Self {
            file,
            count: std::sync::atomic::AtomicU64::new(0),
        })
    }

    pub fn open(dir: &Path) -> Result<Self, StoreError> {
        let path = Self::path(dir);
        let file = TableFile::open(path, TableKind::TxidBody)?;
        let len = file.logical_len();
        let count = if len <= TXID_BODY_HEADER {
            0
        } else {
            (len - TXID_BODY_HEADER) / TXID_ENTRY_LEN
        };
        Ok(Self {
            file,
            count: std::sync::atomic::AtomicU64::new(count),
        })
    }

    fn path(dir: &Path) -> PathBuf {
        dir.join("txid.body")
    }

    pub fn count(&self) -> u64 {
        self.count.load(std::sync::atomic::Ordering::Acquire)
    }

    /// Absolute file offset of the 32-byte entry for `fk` (1-based).
    #[inline]
    pub fn entry_offset(fk: u64) -> Result<u64, StoreError> {
        if fk == 0 {
            return Err(StoreError::InvalidFk);
        }
        Ok(TXID_BODY_HEADER + (fk - 1) * TXID_ENTRY_LEN)
    }

    /// Sidefile peeks never request RWF_DONTCACHE (permanent spend-only policy).
    #[inline]
    pub fn dontcache_for_fk(&self, _fk: u64) -> bool {
        false
    }

    pub fn body_read_fd(&self) -> std::os::fd::RawFd {
        self.file.read_fd()
    }

    pub fn file_path(&self) -> &Path {
        self.file.path()
    }

    pub fn logical_len(&self) -> u64 {
        self.file.logical_len()
    }

    /// Append `txids` for consecutive create_fks starting at `base_count+1`.
    ///
    /// Must be called once per Class A body batch with the same length; order
    /// matches create_fk order. Publishes count after durable write.
    pub fn append_batch(&self, base_count: u64, txids: &[[u8; 32]]) -> Result<(), StoreError> {
        if txids.is_empty() {
            return Ok(());
        }
        let cur = self.count.load(std::sync::atomic::Ordering::Acquire);
        if cur != base_count {
            return Err(StoreError::Corrupt("txid.body count mismatch on append"));
        }
        let start = TXID_BODY_HEADER + base_count * TXID_ENTRY_LEN;
        let mut blob = Vec::with_capacity(txids.len() * 32);
        for t in txids {
            blob.extend_from_slice(t);
        }
        self.file.write_at_pwrite(start, &blob)?;
        let new = base_count + txids.len() as u64;
        self.count
            .store(new, std::sync::atomic::Ordering::Release);
        Ok(())
    }

    /// Read txid for create_fk (bulk pread; no RWF_DONTCACHE).
    pub fn get(&self, fk: Fk) -> Result<[u8; 32], StoreError> {
        let id = fk.get().ok_or(StoreError::InvalidFk)?;
        let n = self.count();
        if id > n {
            return Err(StoreError::NotFound);
        }
        let off = Self::entry_offset(id)?;
        let mut buf = [0u8; 32];
        let rc = crate::bulk_io::pread_single(
            self.file.read_fd(),
            off,
            &mut buf,
            self.dontcache_for_fk(id),
        );
        if rc < 0 {
            return Err(StoreError::io(
                self.file.path(),
                std::io::Error::from_raw_os_error(-rc),
            ));
        }
        if (rc as usize) != 32 {
            // Short — complete via plain pread.
            self.file.pread_at(off, &mut buf)?;
        }
        Ok(buf)
    }

    /// Bulk read txids for consecutive fks `first..=last` (1-based).
    pub fn get_range(&self, first: u64, last: u64) -> Result<Vec<[u8; 32]>, StoreError> {
        if last < first {
            return Ok(Vec::new());
        }
        let n = self.count();
        if first == 0 || last > n {
            return Err(StoreError::NotFound);
        }
        let count = (last - first + 1) as usize;
        let off = Self::entry_offset(first)?;
        let mut blob = vec![0u8; count * 32];
        let rc = crate::bulk_io::pread_single(self.file.read_fd(), off, &mut blob, false);
        if rc < 0 {
            return Err(StoreError::io(
                self.file.path(),
                std::io::Error::from_raw_os_error(-rc),
            ));
        }
        if (rc as usize) != blob.len() {
            self.file.pread_at(off, &mut blob)?;
        }
        let mut out = Vec::with_capacity(count);
        for i in 0..count {
            let s = i * 32;
            out.push(blob[s..s + 32].try_into().unwrap());
        }
        Ok(out)
    }

    /// Fill `out[i]` with txid for `fks[i]` (scattered fks).
    pub fn get_many(&self, fks: &[Fk]) -> Result<Vec<Option<[u8; 32]>>, StoreError> {
        if fks.is_empty() {
            return Ok(Vec::new());
        }
        let n = self.count();
        let mut out = vec![None; fks.len()];
        // Resolve valid (index, offset) jobs.
        let mut jobs: Vec<(usize, u64)> = Vec::with_capacity(fks.len());
        for (i, fk) in fks.iter().enumerate() {
            let Some(id) = fk.get() else {
                continue;
            };
            if id == 0 || id > n {
                continue;
            }
            let off = Self::entry_offset(id)?;
            jobs.push((i, off));
        }
        if jobs.is_empty() {
            return Ok(out);
        }
        let mut bufs: Vec<[u8; 32]> = vec![[0u8; 32]; jobs.len()];
        {
            use crate::bulk_io::{self, ReadOp};
            let fd = self.file.read_fd();
            // SAFETY: each bufs[j] is a distinct element of the vec.
            let mut ops: Vec<ReadOp<'_>> = Vec::with_capacity(jobs.len());
            for (j, &(_i, off)) in jobs.iter().enumerate() {
                let ptr = bufs[j].as_mut_ptr();
                let slice = unsafe { std::slice::from_raw_parts_mut(ptr, 32) };
                ops.push(ReadOp {
                    fd,
                    offset: off,
                    buf: slice,
                    result: i32::MIN,
                    dontcache: self.dontcache_for_fk(0),
                });
            }
            bulk_io::pread_batch(&mut ops);
            for (j, op) in ops.iter().enumerate() {
                if op.result < 0 {
                    return Err(StoreError::io(
                        self.file.path(),
                        std::io::Error::from_raw_os_error(-op.result),
                    ));
                }
                out[jobs[j].0] = Some(bufs[j]);
            }
        }
        Ok(out)
    }

    /// Last DONTCACHE plan for a sidefile read of `fk` (tests / diagnostics).
    pub fn read_op_dontcache(&self, fk: u64) -> bool {
        self.dontcache_for_fk(fk)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn tmp() -> std::path::PathBuf {
        let n = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let p = std::env::temp_dir().join(format!("rbitcoin-txid-body-{n}"));
        let _ = std::fs::remove_dir_all(&p);
        std::fs::create_dir_all(&p).unwrap();
        p
    }

    #[test]
    fn append_and_get_roundtrip() {
        let dir = tmp();
        let t = TxidBody::create(&dir).unwrap();
        assert_eq!(t.count(), 0);
        let a = [1u8; 32];
        let b = [2u8; 32];
        t.append_batch(0, &[a, b]).unwrap();
        assert_eq!(t.count(), 2);
        assert_eq!(t.get(Fk(1)).unwrap(), a);
        assert_eq!(t.get(Fk(2)).unwrap(), b);
        assert_eq!(t.get_range(1, 2).unwrap(), vec![a, b]);
        // reopen
        drop(t);
        let t2 = TxidBody::open(&dir).unwrap();
        assert_eq!(t2.count(), 2);
        assert_eq!(t2.get(Fk(2)).unwrap(), b);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn dontcache_for_fk_always_false() {
        let dir = tmp();
        let t = TxidBody::create(&dir).unwrap();
        assert!(!t.dontcache_for_fk(1));
        t.count
            .store(TXID_DONTCACHE_FROM_TAIL + 10, std::sync::atomic::Ordering::Release);
        assert!(!t.dontcache_for_fk(1));
        assert!(!t.dontcache_for_fk(TXID_DONTCACHE_FROM_TAIL + 5));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn entry_offset_layout() {
        assert_eq!(TxidBody::entry_offset(1).unwrap(), 32);
        assert_eq!(TxidBody::entry_offset(2).unwrap(), 64);
        assert!(TxidBody::entry_offset(0).is_err());
    }
}
