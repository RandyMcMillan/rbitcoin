//! Domain query layer over [`rbitcoin_store::Store`].

use rbitcoin_primitives::Fk;
use rbitcoin_store::{HeaderRecord, OutputRecord, PointRecord, Store, StoreError, TxRecord};
use std::path::Path;

pub type QueryError = StoreError;

/// Read/write query facade used by higher layers (consensus, wallet, RPC).
pub struct Query {
    store: Store,
}

impl Query {
    pub fn open_or_create(store_path: impl AsRef<Path>) -> Result<Self, QueryError> {
        Ok(Self {
            store: Store::open_or_create(store_path.as_ref())?,
        })
    }

    pub fn store(&self) -> &Store {
        &self.store
    }

    pub fn put_header(&self, rec: &HeaderRecord) -> Result<Fk, QueryError> {
        self.store.put_header(rec)
    }

    pub fn get_header(&self, fk: Fk) -> Result<HeaderRecord, QueryError> {
        self.store.get_header(fk)
    }

    pub fn get_header_by_hash(
        &self,
        hash: &[u8; 32],
    ) -> Result<Option<(Fk, HeaderRecord)>, QueryError> {
        self.store.get_header_by_hash(hash)
    }

    pub fn put_tx(&self, rec: &TxRecord) -> Result<Fk, QueryError> {
        self.store.put_tx(rec)
    }

    pub fn get_tx(&self, fk: Fk) -> Result<TxRecord, QueryError> {
        self.store.get_tx(fk)
    }

    pub fn get_tx_by_txid(&self, txid: &[u8; 32]) -> Result<Option<(Fk, TxRecord)>, QueryError> {
        self.store.get_tx_by_txid(txid)
    }

    pub fn put_output(&self, rec: &OutputRecord) -> Result<Fk, QueryError> {
        self.store.put_output(rec)
    }

    pub fn get_output(&self, fk: Fk) -> Result<OutputRecord, QueryError> {
        self.store.get_output(fk)
    }

    pub fn put_spend(
        &self,
        out_txid: &[u8; 32],
        out_index: u32,
        spending_tx_fk: Fk,
        spending_input_index: u32,
    ) -> Result<Fk, QueryError> {
        self.store
            .put_spend(out_txid, out_index, spending_tx_fk, spending_input_index)
    }

    pub fn spenders(
        &self,
        out_txid: &[u8; 32],
        out_index: u32,
    ) -> Result<Vec<PointRecord>, QueryError> {
        self.store.spenders(out_txid, out_index)
    }

    pub fn flush(&self) -> Result<(), QueryError> {
        if !self.store.path().exists() {
            return Err(StoreError::NotDirectory(self.store.path().to_path_buf()));
        }
        self.store.flush()
    }
}

pub fn crate_name() -> &'static str {
    "rbitcoin-query"
}
