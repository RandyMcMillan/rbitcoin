use crate::error::StoreError;
use crate::file::{TableFile, FILE_HEADER_LEN};
use crate::hashhead::HashHead;
use rbitcoin_primitives::{Fk, TableKind};

/// Class B point multimap entry (fixed 56 bytes).
pub const POINT_RECORD_LEN: usize = 56;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PointRecord {
    pub out_txid: [u8; 32],
    pub out_index: u32,
    pub spending_tx_fk: Fk,
    pub spending_input_index: u32,
    pub next: Fk,
}

impl PointRecord {
    pub fn encode(&self) -> [u8; POINT_RECORD_LEN] {
        let mut out = [0u8; POINT_RECORD_LEN];
        out[0..32].copy_from_slice(&self.out_txid);
        out[32..36].copy_from_slice(&self.out_index.to_le_bytes());
        out[36..44].copy_from_slice(&self.spending_tx_fk.0.to_le_bytes());
        out[44..48].copy_from_slice(&self.spending_input_index.to_le_bytes());
        out[48..56].copy_from_slice(&self.next.0.to_le_bytes());
        out
    }

    pub fn decode(buf: &[u8]) -> Result<Self, StoreError> {
        if buf.len() < POINT_RECORD_LEN {
            return Err(StoreError::Corrupt("short point record"));
        }
        Ok(Self {
            out_txid: buf[0..32].try_into().unwrap(),
            out_index: u32::from_le_bytes(buf[32..36].try_into().unwrap()),
            spending_tx_fk: Fk(u64::from_le_bytes(buf[36..44].try_into().unwrap())),
            spending_input_index: u32::from_le_bytes(buf[44..48].try_into().unwrap()),
            next: Fk(u64::from_le_bytes(buf[48..56].try_into().unwrap())),
        })
    }

    pub fn outpoint_key(out_txid: &[u8; 32], out_index: u32) -> [u8; 32] {
        // v0: hash key = first 28 bytes of txid || out_index le (simple). Better: sha256d later.
        let mut key = [0u8; 32];
        key[0..28].copy_from_slice(&out_txid[0..28]);
        key[28..32].copy_from_slice(&out_index.to_le_bytes());
        key
    }
}

pub struct PointTable {
    body: TableFile,
    head: HashHead,
    count: parking_lot::Mutex<u64>,
}

impl PointTable {
    pub fn create(dir: &std::path::Path) -> Result<Self, StoreError> {
        Ok(Self {
            body: TableFile::create(dir.join("point.body"), TableKind::Point)?,
            head: HashHead::create(dir.join("point.head"))?,
            count: parking_lot::Mutex::new(0),
        })
    }

    pub fn open(dir: &std::path::Path) -> Result<Self, StoreError> {
        let body = TableFile::open(dir.join("point.body"), TableKind::Point)?;
        let head = HashHead::open(dir.join("point.head"))?;
        let body_len = body.logical_len().saturating_sub(FILE_HEADER_LEN as u64);
        if body_len % POINT_RECORD_LEN as u64 != 0 {
            return Err(StoreError::Corrupt("point body size"));
        }
        let count = body_len / POINT_RECORD_LEN as u64;
        Ok(Self {
            body,
            head,
            count: parking_lot::Mutex::new(count),
        })
    }

    /// Append a spend index entry. Chains onto existing head for the outpoint key.
    pub fn put_spend(
        &self,
        out_txid: &[u8; 32],
        out_index: u32,
        spending_tx_fk: Fk,
        spending_input_index: u32,
    ) -> Result<Fk, StoreError> {
        if spending_tx_fk.is_null() {
            return Err(StoreError::InvalidFk);
        }
        let key = PointRecord::outpoint_key(out_txid, out_index);
        let prev_head = self.head.get(&key)?.unwrap_or(Fk::NULL);
        let mut count = self.count.lock();
        let fk = Fk(*count + 1);
        let rec = PointRecord {
            out_txid: *out_txid,
            out_index,
            spending_tx_fk,
            spending_input_index,
            next: prev_head,
        };
        let offset = FILE_HEADER_LEN as u64 + (*count) * POINT_RECORD_LEN as u64;
        self.body.write_at(offset, &rec.encode())?;
        *count += 1;
        self.head.insert(&key, fk)?;
        Ok(fk)
    }

    pub fn get(&self, fk: Fk) -> Result<PointRecord, StoreError> {
        let id = fk.get().ok_or(StoreError::InvalidFk)?;
        let count = *self.count.lock();
        if id == 0 || id > count {
            return Err(StoreError::NotFound);
        }
        let offset = FILE_HEADER_LEN as u64 + (id - 1) * POINT_RECORD_LEN as u64;
        let mut buf = [0u8; POINT_RECORD_LEN];
        self.body.read_at(offset, &mut buf)?;
        PointRecord::decode(&buf)
    }

    /// Collect all spenders of an outpoint (may be empty).
    pub fn spenders(
        &self,
        out_txid: &[u8; 32],
        out_index: u32,
    ) -> Result<Vec<PointRecord>, StoreError> {
        let key = PointRecord::outpoint_key(out_txid, out_index);
        let mut out = Vec::new();
        let mut cur = self.head.get(&key)?;
        while let Some(fk) = cur {
            let rec = self.get(fk)?;
            let next = if rec.next.is_null() {
                None
            } else {
                Some(rec.next)
            };
            out.push(rec);
            cur = next;
        }
        Ok(out)
    }

    pub fn flush(&self) -> Result<(), StoreError> {
        self.body.flush()?;
        self.head.flush()?;
        Ok(())
    }
}
