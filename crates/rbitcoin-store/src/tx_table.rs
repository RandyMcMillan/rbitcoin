use crate::error::StoreError;
use crate::file::{TableFile, FILE_HEADER_LEN};
use crate::hashhead::HashHead;
use rbitcoin_primitives::{Fk, TableKind};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TxRecord {
    pub txid: [u8; 32],
    pub version: i32,
    pub locktime: u32,
    pub input_start_fk: Fk,
    pub input_count: u32,
    pub output_start_fk: Fk,
    pub output_count: u32,
    pub raw: Vec<u8>,
}

impl TxRecord {
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(32 + 4 + 4 + 8 + 4 + 8 + 4 + 4 + self.raw.len());
        out.extend_from_slice(&self.txid);
        out.extend_from_slice(&self.version.to_le_bytes());
        out.extend_from_slice(&self.locktime.to_le_bytes());
        out.extend_from_slice(&self.input_start_fk.0.to_le_bytes());
        out.extend_from_slice(&self.input_count.to_le_bytes());
        out.extend_from_slice(&self.output_start_fk.0.to_le_bytes());
        out.extend_from_slice(&self.output_count.to_le_bytes());
        out.extend_from_slice(&(self.raw.len() as u32).to_le_bytes());
        out.extend_from_slice(&self.raw);
        out
    }

    pub fn decode(buf: &[u8]) -> Result<Self, StoreError> {
        if buf.len() < 32 + 4 + 4 + 8 + 4 + 8 + 4 + 4 {
            return Err(StoreError::Corrupt("short tx record"));
        }
        let txid: [u8; 32] = buf[0..32].try_into().unwrap();
        let version = i32::from_le_bytes(buf[32..36].try_into().unwrap());
        let locktime = u32::from_le_bytes(buf[36..40].try_into().unwrap());
        let input_start_fk = Fk(u64::from_le_bytes(buf[40..48].try_into().unwrap()));
        let input_count = u32::from_le_bytes(buf[48..52].try_into().unwrap());
        let output_start_fk = Fk(u64::from_le_bytes(buf[52..60].try_into().unwrap()));
        let output_count = u32::from_le_bytes(buf[60..64].try_into().unwrap());
        let raw_len = u32::from_le_bytes(buf[64..68].try_into().unwrap()) as usize;
        if buf.len() < 68 + raw_len {
            return Err(StoreError::Corrupt("tx raw truncated"));
        }
        let raw = buf[68..68 + raw_len].to_vec();
        Ok(Self {
            txid,
            version,
            locktime,
            input_start_fk,
            input_count,
            output_start_fk,
            output_count,
            raw,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OutputRecord {
    pub parent_tx_fk: Fk,
    pub index: u32,
    pub value: i64,
    pub script: Vec<u8>,
}

impl OutputRecord {
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(8 + 4 + 8 + 4 + self.script.len());
        out.extend_from_slice(&self.parent_tx_fk.0.to_le_bytes());
        out.extend_from_slice(&self.index.to_le_bytes());
        out.extend_from_slice(&self.value.to_le_bytes());
        out.extend_from_slice(&(self.script.len() as u32).to_le_bytes());
        out.extend_from_slice(&self.script);
        out
    }

    pub fn decode(buf: &[u8]) -> Result<Self, StoreError> {
        if buf.len() < 8 + 4 + 8 + 4 {
            return Err(StoreError::Corrupt("short output record"));
        }
        let parent_tx_fk = Fk(u64::from_le_bytes(buf[0..8].try_into().unwrap()));
        let index = u32::from_le_bytes(buf[8..12].try_into().unwrap());
        let value = i64::from_le_bytes(buf[12..20].try_into().unwrap());
        let script_len = u32::from_le_bytes(buf[20..24].try_into().unwrap()) as usize;
        if buf.len() < 24 + script_len {
            return Err(StoreError::Corrupt("output script truncated"));
        }
        Ok(Self {
            parent_tx_fk,
            index,
            value,
            script: buf[24..24 + script_len].to_vec(),
        })
    }
}

/// Index of variable records: count + offset table + payloads in one body file.
///
/// Layout after file header:
/// - u64 count
/// - count × u64 absolute offsets into this file for each record (1-based index i at slot i-1)
/// - payloads appended after the offset table growth region
///
/// v0 simplification: offsets table is pre-reserved for `capacity` entries; payloads append.
struct VarTable {
    body: TableFile,
    count: parking_lot::Mutex<u64>,
    capacity: u64,
}

const VAR_COUNT_OFFSET: u64 = FILE_HEADER_LEN as u64;
const VAR_OFFSETS_START: u64 = VAR_COUNT_OFFSET + 8;

impl VarTable {
    fn offsets_bytes(capacity: u64) -> u64 {
        capacity * 8
    }

    fn payload_start(capacity: u64) -> u64 {
        VAR_OFFSETS_START + Self::offsets_bytes(capacity)
    }

    pub fn create(
        path: impl Into<std::path::PathBuf>,
        kind: TableKind,
        capacity: u64,
    ) -> Result<Self, StoreError> {
        let body = TableFile::create(path, kind)?;
        // count = 0
        body.write_at(VAR_COUNT_OFFSET, &0u64.to_le_bytes())?;
        let zeros = vec![0u8; Self::offsets_bytes(capacity) as usize];
        body.write_at(VAR_OFFSETS_START, &zeros)?;
        // Offset table write leaves logical_len at payload_start.
        debug_assert!(body.logical_len() >= Self::payload_start(capacity));
        Ok(Self {
            body,
            count: parking_lot::Mutex::new(0),
            capacity,
        })
    }

    pub fn open(
        path: impl Into<std::path::PathBuf>,
        kind: TableKind,
        capacity: u64,
    ) -> Result<Self, StoreError> {
        let body = TableFile::open(path, kind)?;
        let mut count_buf = [0u8; 8];
        body.read_at(VAR_COUNT_OFFSET, &mut count_buf)?;
        let count = u64::from_le_bytes(count_buf);
        Ok(Self {
            body,
            count: parking_lot::Mutex::new(count),
            capacity,
        })
    }

    pub fn put(&self, payload: &[u8]) -> Result<Fk, StoreError> {
        let mut count = self.count.lock();
        if *count >= self.capacity {
            return Err(StoreError::Corrupt("var table capacity exhausted"));
        }
        let fk = Fk(*count + 1);
        // Append payload at end of logical file (at least payload_start).
        let start = self
            .body
            .logical_len()
            .max(Self::payload_start(self.capacity));
        self.body.write_at(start, payload)?;
        // Length-prefix each record for decode: u32 len + bytes already in payload encoding.
        // We store raw encoded record only; readers use length from encoding.
        // For get we need to know length — encode stores self-describing records.
        // Write offset for this fk.
        let off_pos = VAR_OFFSETS_START + (*count) * 8;
        self.body.write_at(off_pos, &start.to_le_bytes())?;
        *count += 1;
        self.body.write_at(VAR_COUNT_OFFSET, &count.to_le_bytes())?;
        Ok(fk)
    }

    pub fn get_raw(&self, fk: Fk) -> Result<Vec<u8>, StoreError> {
        let id = fk.get().ok_or(StoreError::InvalidFk)?;
        let count = *self.count.lock();
        if id == 0 || id > count {
            return Err(StoreError::NotFound);
        }
        let mut off_buf = [0u8; 8];
        self.body
            .read_at(VAR_OFFSETS_START + (id - 1) * 8, &mut off_buf)?;
        let start = u64::from_le_bytes(off_buf);
        // Peek length: for tx/output encodings, length is embedded. Read a generous chunk
        // by scanning: we store u32 total_len prefix for var records in this helper.
        let mut len_buf = [0u8; 4];
        self.body.read_at(start, &mut len_buf)?;
        let total = u32::from_le_bytes(len_buf) as usize;
        if total < 4 {
            return Err(StoreError::Corrupt("var record len"));
        }
        let mut buf = vec![0u8; total];
        self.body.read_at(start, &mut buf)?;
        Ok(buf)
    }

    pub fn flush(&self) -> Result<(), StoreError> {
        self.body.flush()
    }
}

/// Prefix payload with u32 total length (including the 4-byte prefix).
fn framed(payload: &[u8]) -> Vec<u8> {
    let total = (4 + payload.len()) as u32;
    let mut out = Vec::with_capacity(total as usize);
    out.extend_from_slice(&total.to_le_bytes());
    out.extend_from_slice(payload);
    out
}

const TX_CAPACITY: u64 = 32;

pub struct TxTable {
    body: VarTable,
    head: HashHead,
}

impl TxTable {
    pub fn create(dir: &std::path::Path) -> Result<Self, StoreError> {
        Ok(Self {
            body: VarTable::create(dir.join("tx.body"), TableKind::Tx, TX_CAPACITY)?,
            head: HashHead::create(dir.join("tx.head"))?,
        })
    }

    pub fn open(dir: &std::path::Path) -> Result<Self, StoreError> {
        Ok(Self {
            body: VarTable::open(dir.join("tx.body"), TableKind::Tx, TX_CAPACITY)?,
            head: HashHead::open(dir.join("tx.head"))?,
        })
    }

    pub fn put(&self, rec: &TxRecord) -> Result<Fk, StoreError> {
        let payload = framed(&rec.encode());
        let fk = self.body.put(&payload)?;
        self.head.insert(&rec.txid, fk)?;
        Ok(fk)
    }

    pub fn get(&self, fk: Fk) -> Result<TxRecord, StoreError> {
        let raw = self.body.get_raw(fk)?;
        // get_raw returns a framed buffer with total >= 4.
        TxRecord::decode(&raw[4..])
    }

    pub fn get_by_txid(&self, txid: &[u8; 32]) -> Result<Option<(Fk, TxRecord)>, StoreError> {
        match self.head.get(txid)? {
            None => Ok(None),
            Some(fk) => Ok(Some((fk, self.get(fk)?))),
        }
    }

    pub fn flush(&self) -> Result<(), StoreError> {
        self.body.flush()?;
        self.head.flush()?;
        Ok(())
    }
}

const OUTPUT_CAPACITY: u64 = 64;

pub struct OutputTable {
    body: VarTable,
}

impl OutputTable {
    pub fn create(dir: &std::path::Path) -> Result<Self, StoreError> {
        Ok(Self {
            body: VarTable::create(dir.join("output.body"), TableKind::Output, OUTPUT_CAPACITY)?,
        })
    }

    pub fn open(dir: &std::path::Path) -> Result<Self, StoreError> {
        Ok(Self {
            body: VarTable::open(dir.join("output.body"), TableKind::Output, OUTPUT_CAPACITY)?,
        })
    }

    pub fn put(&self, rec: &OutputRecord) -> Result<Fk, StoreError> {
        let payload = framed(&rec.encode());
        self.body.put(&payload)
    }

    pub fn get(&self, fk: Fk) -> Result<OutputRecord, StoreError> {
        let raw = self.body.get_raw(fk)?;
        OutputRecord::decode(&raw[4..])
    }

    pub fn flush(&self) -> Result<(), StoreError> {
        self.body.flush()
    }
}
