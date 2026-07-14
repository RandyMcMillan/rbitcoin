//! Class B scripthash multimap (Electrum: SHA256(scriptPubKey)).

use crate::error::StoreError;
use crate::file::{TableFile, FILE_HEADER_LEN};
use crate::hashhead::HashHead;
use rbitcoin_primitives::{Fk, TableKind};
use sha2::{Digest, Sha256};

/// Electrum scripthash = SHA256(scriptPubKey) (binary; API often reverses for hex).
pub fn script_hash(script: &[u8]) -> [u8; 32] {
    let mut h = Sha256::new();
    h.update(script);
    h.finalize().into()
}

/// Fixed scripthash entry (108 bytes).
pub const SCRIPTHASH_RECORD_LEN: usize = 108;

/// `spend_height` when the output is unspent on the best chain.
pub const UNSPENT: u32 = u32::MAX;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ScriptHashRecord {
    /// SHA256(scriptPubKey) — Electrum binary order (not reversed).
    pub scripthash: [u8; 32],
    pub txid: [u8; 32],
    pub vout: u32,
    pub value: i64,
    pub create_height: u32,
    pub create_tx_fk: Fk,
    /// [`UNSPENT`] if not spent on best chain.
    pub spend_height: u32,
    pub spend_tx_fk: Fk,
    pub next: Fk,
}

impl ScriptHashRecord {
    pub fn encode(&self) -> [u8; SCRIPTHASH_RECORD_LEN] {
        let mut out = [0u8; SCRIPTHASH_RECORD_LEN];
        out[0..32].copy_from_slice(&self.scripthash);
        out[32..64].copy_from_slice(&self.txid);
        out[64..68].copy_from_slice(&self.vout.to_le_bytes());
        out[68..76].copy_from_slice(&self.value.to_le_bytes());
        out[76..80].copy_from_slice(&self.create_height.to_le_bytes());
        out[80..88].copy_from_slice(&self.create_tx_fk.0.to_le_bytes());
        out[88..92].copy_from_slice(&self.spend_height.to_le_bytes());
        out[92..100].copy_from_slice(&self.spend_tx_fk.0.to_le_bytes());
        out[100..108].copy_from_slice(&self.next.0.to_le_bytes());
        out
    }

    pub fn decode(buf: &[u8]) -> Result<Self, StoreError> {
        if buf.len() < SCRIPTHASH_RECORD_LEN {
            return Err(StoreError::Corrupt("short scripthash record"));
        }
        Ok(Self {
            scripthash: buf[0..32].try_into().unwrap(),
            txid: buf[32..64].try_into().unwrap(),
            vout: u32::from_le_bytes(buf[64..68].try_into().unwrap()),
            value: i64::from_le_bytes(buf[68..76].try_into().unwrap()),
            create_height: u32::from_le_bytes(buf[76..80].try_into().unwrap()),
            create_tx_fk: Fk(u64::from_le_bytes(buf[80..88].try_into().unwrap())),
            spend_height: u32::from_le_bytes(buf[88..92].try_into().unwrap()),
            spend_tx_fk: Fk(u64::from_le_bytes(buf[92..100].try_into().unwrap())),
            next: Fk(u64::from_le_bytes(buf[100..108].try_into().unwrap())),
        })
    }

    pub fn is_unspent(&self) -> bool {
        self.spend_height == UNSPENT
    }
}

pub struct ScriptHashTable {
    body: TableFile,
    head: HashHead,
    count: parking_lot::Mutex<u64>,
}

impl ScriptHashTable {
    pub fn create(dir: &std::path::Path) -> Result<Self, StoreError> {
        Ok(Self {
            body: TableFile::create(dir.join("scripthash.body"), TableKind::ScriptHash)?,
            head: HashHead::create(dir.join("scripthash.head"))?,
            count: parking_lot::Mutex::new(0),
        })
    }

    pub fn open(dir: &std::path::Path) -> Result<Self, StoreError> {
        let body = TableFile::open(dir.join("scripthash.body"), TableKind::ScriptHash)?;
        let head = HashHead::open(dir.join("scripthash.head"))?;
        let body_len = body.logical_len().saturating_sub(FILE_HEADER_LEN as u64);
        if body_len % SCRIPTHASH_RECORD_LEN as u64 != 0 {
            return Err(StoreError::Corrupt("scripthash body size"));
        }
        let count = body_len / SCRIPTHASH_RECORD_LEN as u64;
        Ok(Self {
            body,
            head,
            count: parking_lot::Mutex::new(count),
        })
    }

    pub fn put_create(&self, rec: &ScriptHashRecord) -> Result<Fk, StoreError> {
        let key = rec.scripthash;
        let prev_head = self.head.get(&key)?.unwrap_or(Fk::NULL);
        let mut count = self.count.lock();
        let fk = Fk(*count + 1);
        let mut stored = rec.clone();
        stored.next = prev_head;
        let offset = FILE_HEADER_LEN as u64 + (*count) * SCRIPTHASH_RECORD_LEN as u64;
        self.body.write_at(offset, &stored.encode())?;
        *count += 1;
        self.head.insert(&key, fk)?;
        Ok(fk)
    }

    pub fn get(&self, fk: Fk) -> Result<ScriptHashRecord, StoreError> {
        let id = fk.get().ok_or(StoreError::InvalidFk)?;
        let count = *self.count.lock();
        if id == 0 || id > count {
            return Err(StoreError::NotFound);
        }
        let offset = FILE_HEADER_LEN as u64 + (id - 1) * SCRIPTHASH_RECORD_LEN as u64;
        let mut buf = [0u8; SCRIPTHASH_RECORD_LEN];
        self.body.read_at(offset, &mut buf)?;
        ScriptHashRecord::decode(&buf)
    }

    fn write_record(&self, fk: Fk, rec: &ScriptHashRecord) -> Result<(), StoreError> {
        let id = fk.get().ok_or(StoreError::InvalidFk)?;
        let offset = FILE_HEADER_LEN as u64 + (id - 1) * SCRIPTHASH_RECORD_LEN as u64;
        self.body.write_at(offset, &rec.encode())
    }

    /// All entries for a scripthash (chain order newest-first).
    pub fn entries(&self, scripthash: &[u8; 32]) -> Result<Vec<(Fk, ScriptHashRecord)>, StoreError> {
        let mut out = Vec::new();
        let mut cur = self.head.get(scripthash)?;
        while let Some(fk) = cur {
            let rec = self.get(fk)?;
            let next = if rec.next.is_null() {
                None
            } else {
                Some(rec.next)
            };
            out.push((fk, rec));
            cur = next;
        }
        Ok(out)
    }

    /// Mark an outpoint spent at `height` by `spend_tx_fk` (best-chain).
    pub fn mark_spent(
        &self,
        scripthash: &[u8; 32],
        txid: &[u8; 32],
        vout: u32,
        spend_height: u32,
        spend_tx_fk: Fk,
    ) -> Result<bool, StoreError> {
        for (fk, mut rec) in self.entries(scripthash)? {
            if &rec.txid == txid && rec.vout == vout && rec.is_unspent() {
                rec.spend_height = spend_height;
                rec.spend_tx_fk = spend_tx_fk;
                self.write_record(fk, &rec)?;
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Clear spend if it was recorded at `height` (reorg tip disconnect).
    pub fn clear_spend_at_height(
        &self,
        scripthash: &[u8; 32],
        txid: &[u8; 32],
        vout: u32,
        height: u32,
    ) -> Result<(), StoreError> {
        for (fk, mut rec) in self.entries(scripthash)? {
            if &rec.txid == txid && rec.vout == vout && rec.spend_height == height {
                rec.spend_height = UNSPENT;
                rec.spend_tx_fk = Fk::NULL;
                self.write_record(fk, &rec)?;
                return Ok(());
            }
        }
        Ok(())
    }

    pub fn flush(&self) -> Result<(), StoreError> {
        self.body.flush()?;
        self.head.flush()?;
        Ok(())
    }
}
