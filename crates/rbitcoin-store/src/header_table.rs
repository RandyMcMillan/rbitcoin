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
    count: std::sync::Mutex<u64>,
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
            count: std::sync::Mutex::new(0),
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
            count: std::sync::Mutex::new(count),
        })
    }

    /// Append header and publish into the hash head. Returns new FK.
    pub fn put(&self, rec: &HeaderRecord) -> Result<Fk, StoreError> {
        let mut count = self.count.lock().unwrap();
        let fk = Fk(*count + 1);
        let offset = FILE_HEADER_LEN as u64 + (*count) * HEADER_RECORD_LEN as u64;
        let bytes = rec.encode();
        // Allocate body first, then publish head (allocate-then-publish).
        self.body.write_at(offset, &bytes)?;
        // Advance logical length if needed is handled by write_at.
        *count += 1;
        self.head.insert(&rec.hash, fk)?;
        Ok(fk)
    }

    pub fn get(&self, fk: Fk) -> Result<HeaderRecord, StoreError> {
        let id = fk.get().ok_or(StoreError::InvalidFk)?;
        let count = *self.count.lock().unwrap();
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
        *self.count.lock().unwrap()
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
