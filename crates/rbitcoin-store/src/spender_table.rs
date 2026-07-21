//! Multi-spender list body (schema v5).
//!
//! Common case stores a sole `spending_tx_fk` on the create output. Only when an
//! outpoint has multiple annotated spenders do we allocate nodes here.
//!
//! Fixed 16 B records (1-based fk): `spending_tx_fk:u64 | next:u64`.

use crate::error::StoreError;
use crate::file::{TableFile, FILE_HEADER_LEN};
use rbitcoin_primitives::{Fk, TableKind};
use std::path::Path;
use std::sync::Mutex;

pub const SPENDER_RECORD_LEN: usize = 16;

pub struct SpenderTable {
    body: TableFile,
    count: Mutex<u64>,
}

impl SpenderTable {
    pub fn create(dir: &Path) -> Result<Self, StoreError> {
        let body = TableFile::create(dir.join("spenders.body"), TableKind::Spender)?;
        Ok(Self {
            body,
            count: Mutex::new(0),
        })
    }

    pub fn open(dir: &Path) -> Result<Self, StoreError> {
        let path = dir.join("spenders.body");
        if !path.exists() {
            return Self::create(dir);
        }
        let body = TableFile::open(path, TableKind::Spender)?;
        let body_len = body.logical_len().saturating_sub(FILE_HEADER_LEN as u64);
        if body_len % SPENDER_RECORD_LEN as u64 != 0 {
            return Err(StoreError::Corrupt("spenders body size"));
        }
        let count = body_len / SPENDER_RECORD_LEN as u64;
        Ok(Self {
            body,
            count: Mutex::new(count),
        })
    }

    pub fn count(&self) -> u64 {
        *self.count.lock().unwrap()
    }

    fn offset(id: u64) -> u64 {
        FILE_HEADER_LEN as u64 + (id - 1) * SPENDER_RECORD_LEN as u64
    }

    /// Append one list node. Returns its fk.
    pub fn append(&self, spending_tx_fk: Fk, next: Fk) -> Result<Fk, StoreError> {
        if spending_tx_fk.is_null() {
            return Err(StoreError::InvalidFk);
        }
        let mut count = self.count.lock().unwrap();
        let id = *count + 1;
        let mut buf = [0u8; SPENDER_RECORD_LEN];
        buf[0..8].copy_from_slice(&spending_tx_fk.0.to_le_bytes());
        buf[8..16].copy_from_slice(&next.0.to_le_bytes());
        self.body.write_at(Self::offset(id), &buf)?;
        *count = id;
        Ok(Fk(id))
    }

    pub fn get(&self, fk: Fk) -> Result<(Fk, Fk), StoreError> {
        let id = fk.get().ok_or(StoreError::InvalidFk)?;
        let count = *self.count.lock().unwrap();
        if id == 0 || id > count {
            return Err(StoreError::NotFound);
        }
        let mut buf = [0u8; SPENDER_RECORD_LEN];
        self.body.read_at(Self::offset(id), &mut buf)?;
        Ok((
            Fk(u64::from_le_bytes(buf[0..8].try_into().unwrap())),
            Fk(u64::from_le_bytes(buf[8..16].try_into().unwrap())),
        ))
    }

    pub fn mlock_record(&self, fk: Fk) -> Result<(u64, u64), StoreError> {
        let id = fk.get().ok_or(StoreError::InvalidFk)?;
        self.body
            .mlock_range(Self::offset(id), SPENDER_RECORD_LEN as u64)
    }

    pub fn munlock_pages(&self, page_start: u64, page_len: u64) {
        self.body.munlock_range(page_start, page_len);
    }

    pub fn flush(&self) -> Result<(), StoreError> {
        self.body.flush()
    }

    pub fn flush_async(&self) -> Result<(), StoreError> {
        self.body.flush_async()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn append_get_chain() {
        let dir = std::env::temp_dir().join(format!(
            "rbitcoin-spender-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let t = SpenderTable::create(&dir).unwrap();
        let a = t.append(Fk(10), Fk::NULL).unwrap();
        let b = t.append(Fk(11), a).unwrap();
        assert_eq!(t.get(a).unwrap(), (Fk(10), Fk::NULL));
        assert_eq!(t.get(b).unwrap(), (Fk(11), a));
        let _ = std::fs::remove_dir_all(&dir);
    }
}
