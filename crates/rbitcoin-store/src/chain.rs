//! Class C tip/confirmation tables and per-height block tx lists.

use crate::array_table::ArrayTable;
use crate::error::StoreError;
use crate::var_table::{framed, VarTable};
use rbitcoin_primitives::{Fk, Height, TableKind};
use std::path::Path;

/// Best-chain height → header fk. Length = tip_height + 1 when non-empty.
pub struct ConfirmedTable {
    arr: ArrayTable,
}

impl ConfirmedTable {
    pub fn create(dir: &Path) -> Result<Self, StoreError> {
        Ok(Self {
            arr: ArrayTable::create(dir.join("confirmed.body"), TableKind::Confirmed)?,
        })
    }

    pub fn open(dir: &Path) -> Result<Self, StoreError> {
        Ok(Self {
            arr: ArrayTable::open(dir.join("confirmed.body"), TableKind::Confirmed)?,
        })
    }

    pub fn tip_height(&self) -> Option<Height> {
        let n = self.arr.len();
        if n == 0 {
            None
        } else {
            Some(Height((n - 1) as u32))
        }
    }

    pub fn get(&self, height: Height) -> Result<Option<Fk>, StoreError> {
        let v = self.arr.get(u64::from(height.0))?;
        Ok(Fk::new(v))
    }

    pub fn set(&self, height: Height, header_fk: Fk) -> Result<(), StoreError> {
        if header_fk.is_null() {
            return Err(StoreError::InvalidFk);
        }
        self.arr.set(u64::from(height.0), header_fk.0)
    }

    /// Disconnect tip: height must be current tip.
    pub fn disconnect_tip(&self, height: Height) -> Result<(), StoreError> {
        match self.tip_height() {
            Some(t) if t == height => {
                self.arr.truncate(u64::from(height.0))?;
                Ok(())
            }
            Some(_) => Err(StoreError::Corrupt("disconnect not at tip")),
            None => Err(StoreError::Corrupt("disconnect empty chain")),
        }
    }

    pub fn flush(&self) -> Result<(), StoreError> {
        self.arr.flush()
    }
}

/// Per-tx confirmation: index = tx_fk - 1 → header_fk (0 = not strong).
pub struct StrongTxTable {
    arr: ArrayTable,
}

impl StrongTxTable {
    pub fn create(dir: &Path) -> Result<Self, StoreError> {
        Ok(Self {
            arr: ArrayTable::create(dir.join("strong_tx.body"), TableKind::StrongTx)?,
        })
    }

    pub fn open(dir: &Path) -> Result<Self, StoreError> {
        Ok(Self {
            arr: ArrayTable::open(dir.join("strong_tx.body"), TableKind::StrongTx)?,
        })
    }

    pub fn get(&self, tx_fk: Fk) -> Result<Option<Fk>, StoreError> {
        let id = tx_fk.get().ok_or(StoreError::InvalidFk)?;
        let v = self.arr.get(id - 1)?;
        Ok(Fk::new(v))
    }

    pub fn set_strong(&self, tx_fk: Fk, header_fk: Fk) -> Result<(), StoreError> {
        let id = tx_fk.get().ok_or(StoreError::InvalidFk)?;
        if header_fk.is_null() {
            return Err(StoreError::InvalidFk);
        }
        self.arr.set(id - 1, header_fk.0)
    }

    pub fn set_unstrong(&self, tx_fk: Fk) -> Result<(), StoreError> {
        let id = tx_fk.get().ok_or(StoreError::InvalidFk)?;
        if id > self.arr.len() {
            return Ok(());
        }
        self.arr.set(id - 1, 0)
    }

    pub fn is_strong(&self, tx_fk: Fk) -> Result<bool, StoreError> {
        Ok(self.get(tx_fk)?.is_some())
    }

    pub fn flush(&self) -> Result<(), StoreError> {
        self.arr.flush()
    }
}

/// Per-height list of tx fks for connect/disconnect.
pub struct BlockTxsTable {
    lists: VarTable,
    /// height → list record fk
    by_height: ArrayTable,
}

impl BlockTxsTable {
    pub fn create(dir: &Path) -> Result<Self, StoreError> {
        Ok(Self {
            lists: VarTable::create(dir, "block_txs", TableKind::ArrayLink)?,
            by_height: ArrayTable::create(dir.join("block_txs_height.body"), TableKind::ArrayLink)?,
        })
    }

    pub fn open(dir: &Path) -> Result<Self, StoreError> {
        Ok(Self {
            lists: VarTable::open(dir, "block_txs", TableKind::ArrayLink)?,
            by_height: ArrayTable::open(dir.join("block_txs_height.body"), TableKind::ArrayLink)?,
        })
    }

    pub fn put_list(&self, height: Height, tx_fks: &[Fk]) -> Result<(), StoreError> {
        let mut payload = Vec::with_capacity(4 + tx_fks.len() * 8);
        payload.extend_from_slice(&(tx_fks.len() as u32).to_le_bytes());
        for fk in tx_fks {
            payload.extend_from_slice(&fk.0.to_le_bytes());
        }
        let list_fk = self.lists.put(&framed(&payload))?;
        self.by_height.set(u64::from(height.0), list_fk.0)?;
        Ok(())
    }

    pub fn get_list(&self, height: Height) -> Result<Vec<Fk>, StoreError> {
        let list_fk_raw = self.by_height.get(u64::from(height.0))?;
        let list_fk = Fk::new(list_fk_raw).ok_or(StoreError::NotFound)?;
        let raw = self.lists.get_raw(list_fk)?;
        let payload = &raw[4..];
        if payload.len() < 4 {
            return Err(StoreError::Corrupt("short block tx list"));
        }
        let n = u32::from_le_bytes(payload[0..4].try_into().unwrap()) as usize;
        if payload.len() < 4 + n * 8 {
            return Err(StoreError::Corrupt("block tx list truncated"));
        }
        let mut out = Vec::with_capacity(n);
        for i in 0..n {
            let o = 4 + i * 8;
            let v = u64::from_le_bytes(payload[o..o + 8].try_into().unwrap());
            out.push(Fk(v));
        }
        Ok(out)
    }

    pub fn clear_height(&self, height: Height) -> Result<(), StoreError> {
        // Leave list body orphaned; clear index. Truncate height array if tip.
        let h = u64::from(height.0);
        if h + 1 == self.by_height.len() {
            self.by_height.truncate(h)?;
        } else if h < self.by_height.len() {
            self.by_height.set(h, 0)?;
        }
        Ok(())
    }

    pub fn flush(&self) -> Result<(), StoreError> {
        self.lists.flush()?;
        self.by_height.flush()?;
        Ok(())
    }
}
