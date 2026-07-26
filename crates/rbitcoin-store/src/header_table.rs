use crate::error::StoreError;
use crate::file::{TableFile, FILE_HEADER_LEN};
use crate::sharded_hashhead::ShardedHashHead;
use rbitcoin_primitives::{Fk, TableKind};

/// Fixed-size header body record (88 bytes). See SCHEMA.md.
pub const HEADER_RECORD_LEN: usize = 88;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HeaderRecord {
    pub prev_fk: Fk,
    pub version: i32,
    pub timestamp: u32,
    pub bits: u32,
    pub nonce: u32,
    pub merkle_root: [u8; 32],
    pub hash: [u8; 32],
}

impl HeaderRecord {
    pub fn encode(&self) -> [u8; HEADER_RECORD_LEN] {
        let mut out = [0u8; HEADER_RECORD_LEN];
        out[0..8].copy_from_slice(&self.prev_fk.0.to_le_bytes());
        out[8..12].copy_from_slice(&self.version.to_le_bytes());
        out[12..16].copy_from_slice(&self.timestamp.to_le_bytes());
        out[16..20].copy_from_slice(&self.bits.to_le_bytes());
        out[20..24].copy_from_slice(&self.nonce.to_le_bytes());
        out[24..56].copy_from_slice(&self.merkle_root);
        out[56..88].copy_from_slice(&self.hash);
        out
    }

    pub fn decode(buf: &[u8]) -> Result<Self, StoreError> {
        if buf.len() < HEADER_RECORD_LEN {
            return Err(StoreError::Corrupt("short header record"));
        }
        Ok(Self {
            prev_fk: Fk(u64::from_le_bytes(buf[0..8].try_into().unwrap())),
            version: i32::from_le_bytes(buf[8..12].try_into().unwrap()),
            timestamp: u32::from_le_bytes(buf[12..16].try_into().unwrap()),
            bits: u32::from_le_bytes(buf[16..20].try_into().unwrap()),
            nonce: u32::from_le_bytes(buf[20..24].try_into().unwrap()),
            merkle_root: buf[24..56].try_into().unwrap(),
            hash: buf[56..88].try_into().unwrap(),
        })
    }
}

pub struct HeaderTable {
    body: TableFile,
    head: ShardedHashHead,
    count: std::sync::atomic::AtomicU64,
}

impl HeaderTable {
    pub fn create(dir: &std::path::Path) -> Result<Self, StoreError> {
        let body = TableFile::create(dir.join("header.body"), TableKind::Header)?;
        let head = ShardedHashHead::create_for_role(
            dir.join("header.head"),
            crate::hashhead::HeadRole::Header,
        )?;
        Ok(Self {
            body,
            head,
            count: std::sync::atomic::AtomicU64::new(0),
        })
    }

    pub fn open(dir: &std::path::Path) -> Result<Self, StoreError> {
        let body = TableFile::open(dir.join("header.body"), TableKind::Header)?;
        let head = ShardedHashHead::open_for_role(
            dir.join("header.head"),
            crate::hashhead::HeadRole::Header,
        )?;
        let body_len = body.logical_len().saturating_sub(FILE_HEADER_LEN as u64);
        if body_len % HEADER_RECORD_LEN as u64 != 0 {
            return Err(StoreError::Corrupt("header body size"));
        }
        let count = body_len / HEADER_RECORD_LEN as u64;
        Ok(Self {
            body,
            head,
            count: std::sync::atomic::AtomicU64::new(count),
        })
    }

    /// Append header and publish into the hash head. Returns new FK.
    pub fn put(&self, rec: &HeaderRecord) -> Result<Fk, StoreError> {
        use std::sync::atomic::Ordering;
        // Single appender: body → count → head (allocate-then-publish).
        let base = self.count.load(Ordering::Acquire);
        let fk = Fk(base + 1);
        let offset = FILE_HEADER_LEN as u64 + base * HEADER_RECORD_LEN as u64;
        let bytes = rec.encode();
        self.body.write_at(offset, &bytes)?;
        self.count.store(base + 1, Ordering::Release);
        self.head.insert(&rec.hash, fk)?;
        Ok(fk)
    }

    pub fn get(&self, fk: Fk) -> Result<HeaderRecord, StoreError> {
        use std::sync::atomic::Ordering;
        let id = fk.get().ok_or(StoreError::InvalidFk)?;
        let count = self.count.load(Ordering::Acquire);
        if id == 0 || id > count {
            return Err(StoreError::NotFound);
        }
        let offset = FILE_HEADER_LEN as u64 + (id - 1) * HEADER_RECORD_LEN as u64;
        let mut buf = [0u8; HEADER_RECORD_LEN];
        self.body.read_at(offset, &mut buf)?;
        HeaderRecord::decode(&buf)
    }

    pub fn get_by_hash(&self, hash: &[u8; 32]) -> Result<Option<(Fk, HeaderRecord)>, StoreError> {
        // 16-byte head prefix may collide — verify full hash on the body.
        for fk in self.head.get_all(hash)? {
            let rec = self.get(fk)?;
            if rec.hash == *hash {
                return Ok(Some((fk, rec)));
            }
        }
        Ok(None)
    }





    /// Number of header rows currently stored (highest fk = this value).
    pub fn count(&self) -> u64 {
        self.count.load(std::sync::atomic::Ordering::Acquire)
    }

    pub fn flush(&self) -> Result<(), StoreError> {
        self.body.flush()?;
        self.head.flush()?;
        Ok(())
    }

    pub fn flush_async(&self) -> Result<(), StoreError> {
        self.body.flush_async()?;
        self.head.flush_async()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp() -> std::path::PathBuf {
        let p = std::env::temp_dir().join(format!(
            "rbitcoin-header-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let _ = std::fs::remove_dir_all(&p);
        std::fs::create_dir_all(&p).unwrap();
        p
    }

    fn sample(hash: [u8; 32]) -> HeaderRecord {
        HeaderRecord {
            prev_fk: Fk::NULL,
            version: 1,
            timestamp: 100,
            bits: 0x1d00ffff,
            nonce: 7,
            merkle_root: [2u8; 32],
            hash,
        }
    }

    #[test]
    fn header_put_get_by_hash_open_flush() {
        let dir = tmp();
        let t = HeaderTable::create(&dir).unwrap();
        let h1 = [1u8; 32];
        let h2 = [2u8; 32];
        let fk1 = t.put(&sample(h1)).unwrap();
        let fk2 = t.put(&sample(h2)).unwrap();
        assert_eq!(t.count(), 2);
        assert_eq!(t.get(fk1).unwrap().hash, h1);
        assert_eq!(t.get(fk2).unwrap().hash, h2);
        assert_eq!(t.get_by_hash(&h1).unwrap().unwrap().0, fk1);
        assert!(t.get_by_hash(&[9u8; 32]).unwrap().is_none());
        assert!(matches!(t.get(Fk::NULL), Err(StoreError::InvalidFk)));
        assert!(matches!(t.get(Fk(99)), Err(StoreError::NotFound)));
        // short decode
        assert!(matches!(
            HeaderRecord::decode(&[0u8; 10]),
            Err(StoreError::Corrupt(_))
        ));
        t.flush().unwrap();
        t.flush_async().unwrap();
        drop(t);
        let t = HeaderTable::open(&dir).unwrap();
        assert_eq!(t.count(), 2);
        assert_eq!(t.get_by_hash(&h2).unwrap().unwrap().1.nonce, 7);
        // Shrink OS file below HWM so open clamps logical to a non-record size.
        {
            use crate::file::FILE_HEADER_LEN;
            let body = dir.join("header.body");
            std::fs::OpenOptions::new()
                .write(true)
                .open(&body)
                .unwrap()
                .set_len((FILE_HEADER_LEN + 3) as u64)
                .unwrap();
        }
        assert!(matches!(
            HeaderTable::open(&dir),
            Err(StoreError::Corrupt(_))
        ));
        let _ = std::fs::remove_dir_all(&dir);
    }
}
