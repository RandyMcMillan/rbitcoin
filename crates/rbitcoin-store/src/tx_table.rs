use crate::error::StoreError;
use crate::hashhead::HashHead;
use crate::var_table::{framed, VarTable};
use rbitcoin_primitives::{Fk, TableKind};
use std::path::Path;

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

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InputRecord {
    pub parent_tx_fk: Fk,
    pub index: u32,
    pub prev_txid: [u8; 32],
    pub prev_index: u32,
    pub sequence: u32,
    pub script_sig: Vec<u8>,
}

impl InputRecord {
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(8 + 4 + 32 + 4 + 4 + 4 + self.script_sig.len());
        out.extend_from_slice(&self.parent_tx_fk.0.to_le_bytes());
        out.extend_from_slice(&self.index.to_le_bytes());
        out.extend_from_slice(&self.prev_txid);
        out.extend_from_slice(&self.prev_index.to_le_bytes());
        out.extend_from_slice(&self.sequence.to_le_bytes());
        out.extend_from_slice(&(self.script_sig.len() as u32).to_le_bytes());
        out.extend_from_slice(&self.script_sig);
        out
    }

    pub fn decode(buf: &[u8]) -> Result<Self, StoreError> {
        if buf.len() < 8 + 4 + 32 + 4 + 4 + 4 {
            return Err(StoreError::Corrupt("short input record"));
        }
        let parent_tx_fk = Fk(u64::from_le_bytes(buf[0..8].try_into().unwrap()));
        let index = u32::from_le_bytes(buf[8..12].try_into().unwrap());
        let prev_txid: [u8; 32] = buf[12..44].try_into().unwrap();
        let prev_index = u32::from_le_bytes(buf[44..48].try_into().unwrap());
        let sequence = u32::from_le_bytes(buf[48..52].try_into().unwrap());
        let script_len = u32::from_le_bytes(buf[52..56].try_into().unwrap()) as usize;
        if buf.len() < 56 + script_len {
            return Err(StoreError::Corrupt("input script truncated"));
        }
        Ok(Self {
            parent_tx_fk,
            index,
            prev_txid,
            prev_index,
            sequence,
            script_sig: buf[56..56 + script_len].to_vec(),
        })
    }
}

pub struct TxTable {
    body: VarTable,
    head: HashHead,
}

impl TxTable {
    pub fn create(dir: &Path) -> Result<Self, StoreError> {
        Ok(Self {
            body: VarTable::create(dir, "tx", TableKind::Tx)?,
            head: HashHead::create(dir.join("tx.head"))?,
        })
    }

    pub fn open(dir: &Path) -> Result<Self, StoreError> {
        Ok(Self {
            body: VarTable::open(dir, "tx", TableKind::Tx)?,
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

pub struct OutputTable {
    body: VarTable,
}

impl OutputTable {
    pub fn create(dir: &Path) -> Result<Self, StoreError> {
        Ok(Self {
            body: VarTable::create(dir, "output", TableKind::Output)?,
        })
    }

    pub fn open(dir: &Path) -> Result<Self, StoreError> {
        Ok(Self {
            body: VarTable::open(dir, "output", TableKind::Output)?,
        })
    }

    pub fn count(&self) -> u64 {
        self.body.count()
    }

    pub fn put(&self, rec: &OutputRecord) -> Result<Fk, StoreError> {
        let payload = framed(&rec.encode());
        self.body.put(&payload)
    }

    /// Batch-append output records (one body write + one idx write).
    pub fn put_batch(&self, recs: &[OutputRecord]) -> Result<Vec<Fk>, StoreError> {
        if recs.is_empty() {
            return Ok(Vec::new());
        }
        let owned: Vec<Vec<u8>> = recs.iter().map(|r| framed(&r.encode())).collect();
        let refs: Vec<&[u8]> = owned.iter().map(|v| v.as_slice()).collect();
        self.body.put_batch(&refs)
    }

    pub fn get(&self, fk: Fk) -> Result<OutputRecord, StoreError> {
        let raw = self.body.get_raw(fk)?;
        OutputRecord::decode(&raw[4..])
    }

    pub fn flush(&self) -> Result<(), StoreError> {
        self.body.flush()
    }
}

pub struct InputTable {
    body: VarTable,
}

impl InputTable {
    pub fn create(dir: &Path) -> Result<Self, StoreError> {
        Ok(Self {
            body: VarTable::create(dir, "input", TableKind::Input)?,
        })
    }

    pub fn open(dir: &Path) -> Result<Self, StoreError> {
        Ok(Self {
            body: VarTable::open(dir, "input", TableKind::Input)?,
        })
    }

    pub fn count(&self) -> u64 {
        self.body.count()
    }

    pub fn put(&self, rec: &InputRecord) -> Result<Fk, StoreError> {
        let payload = framed(&rec.encode());
        self.body.put(&payload)
    }

    /// Batch-append input records (one body write + one idx write).
    pub fn put_batch(&self, recs: &[InputRecord]) -> Result<Vec<Fk>, StoreError> {
        if recs.is_empty() {
            return Ok(Vec::new());
        }
        let owned: Vec<Vec<u8>> = recs.iter().map(|r| framed(&r.encode())).collect();
        let refs: Vec<&[u8]> = owned.iter().map(|v| v.as_slice()).collect();
        self.body.put_batch(&refs)
    }

    pub fn get(&self, fk: Fk) -> Result<InputRecord, StoreError> {
        let raw = self.body.get_raw(fk)?;
        InputRecord::decode(&raw[4..])
    }

    pub fn flush(&self) -> Result<(), StoreError> {
        self.body.flush()
    }
}
