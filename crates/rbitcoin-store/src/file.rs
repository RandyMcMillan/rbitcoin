//! Growable mmap-backed table files with a common header.

use crate::error::StoreError;
use memmap2::MmapMut;
use parking_lot::Mutex;
use rbitcoin_primitives::{TableKind, SCHEMA_VERSION, STORE_MAGIC};
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::PathBuf;

pub const FILE_HEADER_LEN: usize = 16;

pub struct TableFile {
    path: PathBuf,
    file: Mutex<File>,
    map: Mutex<MmapMut>,
    /// Logical length including header.
    len: Mutex<u64>,
}

impl TableFile {
    pub fn create(path: impl Into<PathBuf>, kind: TableKind) -> Result<Self, StoreError> {
        let path = path.into();
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&path)
            .map_err(|e| StoreError::io(&path, e))?;

        let mut header = [0u8; FILE_HEADER_LEN];
        header[0..4].copy_from_slice(&STORE_MAGIC);
        header[4..6].copy_from_slice(&SCHEMA_VERSION.to_le_bytes());
        header[6..8].copy_from_slice(&kind.as_u16().to_le_bytes());
        file.write_all(&header)
            .map_err(|e| StoreError::io(&path, e))?;
        file.flush().map_err(|e| StoreError::io(&path, e))?;

        // Start with a small mapped region; grows on write.
        let initial = FILE_HEADER_LEN as u64 + 64;
        file.set_len(initial)
            .map_err(|e| StoreError::io(&path, e))?;
        // SAFETY: exclusive file we just created; length set above.
        let map = unsafe { MmapMut::map_mut(&file) }.map_err(|e| StoreError::io(&path, e))?;

        let _ = kind;
        Ok(Self {
            path,
            file: Mutex::new(file),
            map: Mutex::new(map),
            len: Mutex::new(FILE_HEADER_LEN as u64),
        })
    }

    pub fn open(path: impl Into<PathBuf>, kind: TableKind) -> Result<Self, StoreError> {
        let path = path.into();
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .map_err(|e| StoreError::io(&path, e))?;

        let mut header = [0u8; FILE_HEADER_LEN];
        file.read_exact(&mut header)
            .map_err(|e| StoreError::io(&path, e))?;
        if header[0..4] != STORE_MAGIC {
            return Err(StoreError::BadMagic);
        }
        let ver = u16::from_le_bytes([header[4], header[5]]);
        if ver != SCHEMA_VERSION {
            return Err(StoreError::BadSchema(ver));
        }
        let got = u16::from_le_bytes([header[6], header[7]]);
        if got != kind.as_u16() {
            return Err(StoreError::BadKind {
                expected: kind.as_u16(),
                got,
            });
        }

        let file_len = file.metadata().map_err(|e| StoreError::io(&path, e))?.len();
        // Header was fully read; file_len is at least FILE_HEADER_LEN.

        // v0: reserved bytes 8..16 store logical length (including header).
        let mut logical = u64::from_le_bytes(header[8..16].try_into().unwrap());
        if logical < FILE_HEADER_LEN as u64 {
            logical = FILE_HEADER_LEN as u64;
        }
        if logical > file_len {
            // Clamp corrupt HWMs instead of refusing open (rebuildable store).
            logical = file_len;
        }

        let map = unsafe { MmapMut::map_mut(&file) }.map_err(|e| StoreError::io(&path, e))?;
        Ok(Self {
            path,
            file: Mutex::new(file),
            map: Mutex::new(map),
            len: Mutex::new(logical),
        })
    }

    pub fn logical_len(&self) -> u64 {
        *self.len.lock()
    }

    pub fn read_at(&self, offset: u64, buf: &mut [u8]) -> Result<(), StoreError> {
        let end = offset.saturating_add(buf.len() as u64);
        let len = *self.len.lock();
        if end > len {
            return Err(StoreError::Corrupt("read past logical end"));
        }
        let map = self.map.lock();
        buf.copy_from_slice(&map[offset as usize..end as usize]);
        Ok(())
    }

    pub fn write_at(&self, offset: u64, bytes: &[u8]) -> Result<(), StoreError> {
        let end = offset.saturating_add(bytes.len() as u64);
        self.ensure_capacity(end)?;
        {
            let mut map = self.map.lock();
            map[offset as usize..end as usize].copy_from_slice(bytes);
        }
        let mut len = self.len.lock();
        if end > *len {
            *len = end;
            self.persist_logical_len(*len)?;
        }
        Ok(())
    }

    fn ensure_capacity(&self, need: u64) -> Result<(), StoreError> {
        let mut map = self.map.lock();
        if need <= map.len() as u64 {
            return Ok(());
        }
        let mut new_cap = (map.len() as u64).max(64);
        while new_cap < need {
            new_cap = new_cap.saturating_mul(2).max(need);
        }
        let file = self.file.lock();
        file.set_len(new_cap)
            .map_err(|e| StoreError::io(&self.path, e))?;
        // SAFETY: we hold exclusive map mutex; file length expanded.
        let new_map =
            unsafe { MmapMut::map_mut(&*file) }.map_err(|e| StoreError::io(&self.path, e))?;
        *map = new_map;
        Ok(())
    }

    fn persist_logical_len(&self, logical: u64) -> Result<(), StoreError> {
        let mut file = self.file.lock();
        file.seek(SeekFrom::Start(8))
            .map_err(|e| StoreError::io(&self.path, e))?;
        file.write_all(&logical.to_le_bytes())
            .map_err(|e| StoreError::io(&self.path, e))?;
        {
            let mut map = self.map.lock();
            if map.len() >= 16 {
                map[8..16].copy_from_slice(&logical.to_le_bytes());
            }
        }
        Ok(())
    }

    pub fn flush(&self) -> Result<(), StoreError> {
        self.map
            .lock()
            .flush()
            .map_err(|e| StoreError::io(&self.path, e))?;
        self.file
            .lock()
            .sync_data()
            .map_err(|e| StoreError::io(&self.path, e))?;
        Ok(())
    }
}
