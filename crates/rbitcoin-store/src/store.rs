use crate::chain::{BlockTxsTable, ConfirmedTable, StrongTxTable};
use crate::error::StoreError;
use crate::header_table::{HeaderRecord, HeaderTable};
use crate::point_table::PointTable;
use crate::tx_table::{InputRecord, InputTable, OutputRecord, OutputTable, TxRecord, TxTable};
use rbitcoin_primitives::{Fk, Height, SCHEMA_VERSION, STORE_MAGIC};
use std::fs::OpenOptions;
use std::io::Write;
use std::path::{Path, PathBuf};

/// Top-level store handle for a datadir `store/` directory.
pub struct Store {
    path: PathBuf,
    pub headers: HeaderTable,
    pub txs: TxTable,
    pub inputs: InputTable,
    pub outputs: OutputTable,
    pub points: PointTable,
    pub confirmed: ConfirmedTable,
    pub strong_tx: StrongTxTable,
    pub block_txs: BlockTxsTable,
}

impl Store {
    pub fn create(path: impl Into<PathBuf>) -> Result<Self, StoreError> {
        let path = path.into();
        if path.exists() {
            if !path.is_dir() {
                return Err(StoreError::NotDirectory(path));
            }
        } else {
            std::fs::create_dir_all(&path).map_err(|e| StoreError::io(&path, e))?;
        }
        write_meta(&path)?;
        Ok(Self {
            headers: HeaderTable::create(&path)?,
            txs: TxTable::create(&path)?,
            inputs: InputTable::create(&path)?,
            outputs: OutputTable::create(&path)?,
            points: PointTable::create(&path)?,
            confirmed: ConfirmedTable::create(&path)?,
            strong_tx: StrongTxTable::create(&path)?,
            block_txs: BlockTxsTable::create(&path)?,
            path,
        })
    }

    pub fn open(path: impl Into<PathBuf>) -> Result<Self, StoreError> {
        let path = path.into();
        if !path.is_dir() {
            return Err(StoreError::NotDirectory(path));
        }
        check_meta(&path)?;
        Ok(Self {
            headers: HeaderTable::open(&path)?,
            txs: TxTable::open(&path)?,
            inputs: InputTable::open(&path)?,
            outputs: OutputTable::open(&path)?,
            points: PointTable::open(&path)?,
            confirmed: ConfirmedTable::open(&path)?,
            strong_tx: StrongTxTable::open(&path)?,
            block_txs: BlockTxsTable::open(&path)?,
            path,
        })
    }

    pub fn open_or_create(path: impl Into<PathBuf>) -> Result<Self, StoreError> {
        let path = path.into();
        if path.join("meta").exists() {
            Self::open(path)
        } else {
            Self::create(path)
        }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn tip_height(&self) -> Option<Height> {
        self.confirmed.tip_height()
    }

    pub fn put_header(&self, rec: &HeaderRecord) -> Result<Fk, StoreError> {
        self.headers.put(rec)
    }

    pub fn get_header(&self, fk: Fk) -> Result<HeaderRecord, StoreError> {
        self.headers.get(fk)
    }

    pub fn get_header_by_hash(
        &self,
        hash: &[u8; 32],
    ) -> Result<Option<(Fk, HeaderRecord)>, StoreError> {
        self.headers.get_by_hash(hash)
    }

    pub fn put_tx(&self, rec: &TxRecord) -> Result<Fk, StoreError> {
        self.txs.put(rec)
    }

    pub fn get_tx(&self, fk: Fk) -> Result<TxRecord, StoreError> {
        self.txs.get(fk)
    }

    pub fn get_tx_by_txid(&self, txid: &[u8; 32]) -> Result<Option<(Fk, TxRecord)>, StoreError> {
        self.txs.get_by_txid(txid)
    }

    pub fn put_input(&self, rec: &InputRecord) -> Result<Fk, StoreError> {
        self.inputs.put(rec)
    }

    pub fn get_input(&self, fk: Fk) -> Result<InputRecord, StoreError> {
        self.inputs.get(fk)
    }

    pub fn put_output(&self, rec: &OutputRecord) -> Result<Fk, StoreError> {
        self.outputs.put(rec)
    }

    pub fn get_output(&self, fk: Fk) -> Result<OutputRecord, StoreError> {
        self.outputs.get(fk)
    }

    pub fn put_spend(
        &self,
        out_txid: &[u8; 32],
        out_index: u32,
        spending_tx_fk: Fk,
        spending_input_index: u32,
    ) -> Result<Fk, StoreError> {
        self.points
            .put_spend(out_txid, out_index, spending_tx_fk, spending_input_index)
    }

    /// Spenders whose spending transaction is currently strong (confirmed on best chain).
    pub fn spenders(
        &self,
        out_txid: &[u8; 32],
        out_index: u32,
    ) -> Result<Vec<crate::point_table::PointRecord>, StoreError> {
        let all = self.points.spenders(out_txid, out_index)?;
        let mut out = Vec::new();
        for p in all {
            if self.strong_tx.is_strong(p.spending_tx_fk)? {
                out.push(p);
            }
        }
        Ok(out)
    }

    /// All point rows including unconfirmed historical spends (raw multimap).
    pub fn spenders_raw(
        &self,
        out_txid: &[u8; 32],
        out_index: u32,
    ) -> Result<Vec<crate::point_table::PointRecord>, StoreError> {
        self.points.spenders(out_txid, out_index)
    }

    pub fn flush(&self) -> Result<(), StoreError> {
        self.headers.flush()?;
        self.txs.flush()?;
        self.inputs.flush()?;
        self.outputs.flush()?;
        self.points.flush()?;
        self.confirmed.flush()?;
        self.strong_tx.flush()?;
        self.block_txs.flush()?;
        Ok(())
    }
}

fn write_meta(dir: &Path) -> Result<(), StoreError> {
    let path = dir.join("meta");
    let mut f = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&path)
        .map_err(|e| StoreError::io(&path, e))?;
    f.write_all(&STORE_MAGIC)
        .map_err(|e| StoreError::io(&path, e))?;
    f.write_all(&SCHEMA_VERSION.to_le_bytes())
        .map_err(|e| StoreError::io(&path, e))?;
    f.flush().map_err(|e| StoreError::io(&path, e))?;
    Ok(())
}

fn check_meta(dir: &Path) -> Result<(), StoreError> {
    let path = dir.join("meta");
    let bytes = std::fs::read(&path).map_err(|e| StoreError::io(&path, e))?;
    if bytes.len() < 6 {
        return Err(StoreError::Corrupt("meta too short"));
    }
    if bytes[0..4] != STORE_MAGIC {
        return Err(StoreError::BadMagic);
    }
    let ver = u16::from_le_bytes([bytes[4], bytes[5]]);
    if ver != SCHEMA_VERSION {
        return Err(StoreError::BadSchema(ver));
    }
    Ok(())
}
