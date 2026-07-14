//! Domain query layer over [`rbitcoin_store::Store`].

use bitcoin::block::{Header as BlockHeader, Version as BlockVersion};
use bitcoin::consensus::Decodable;
use bitcoin::hashes::Hash;
use bitcoin::{Block, BlockHash, CompactTarget, Transaction, TxMerkleNode};
use rbitcoin_primitives::{Fk, Height};
use rbitcoin_store::{
    HeaderRecord, InputRecord, OutputRecord, PointRecord, Store, StoreError, TxRecord,
};
use std::path::Path;

pub type QueryError = StoreError;

/// One transaction to apply when connecting a block.
#[derive(Clone, Debug)]
pub struct TxApply {
    pub tx: TxRecord,
    pub inputs: Vec<InputRecord>,
    pub outputs: Vec<OutputRecord>,
}

/// Domain query facade used by higher layers (consensus, net, RPC).
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

    pub fn tip_height(&self) -> Option<Height> {
        self.store.tip_height()
    }

    pub fn tip_header_fk(&self) -> Result<Option<Fk>, QueryError> {
        match self.tip_height() {
            None => Ok(None),
            Some(h) => Ok(self.store.confirmed.get(h)?),
        }
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

    pub fn put_input(&self, rec: &InputRecord) -> Result<Fk, QueryError> {
        self.store.put_input(rec)
    }

    pub fn get_input(&self, fk: Fk) -> Result<InputRecord, QueryError> {
        self.store.get_input(fk)
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

    /// Strong (best-chain confirmed) spenders only.
    pub fn spenders(
        &self,
        out_txid: &[u8; 32],
        out_index: u32,
    ) -> Result<Vec<PointRecord>, QueryError> {
        self.store.spenders(out_txid, out_index)
    }

    pub fn spenders_raw(
        &self,
        out_txid: &[u8; 32],
        out_index: u32,
    ) -> Result<Vec<PointRecord>, QueryError> {
        self.store.spenders_raw(out_txid, out_index)
    }

    /// Connect a block at `height` (genesis or tip+1).
    ///
    /// Writes Class A rows and point spends, then Class C confirmation.
    pub fn connect_block(
        &self,
        height: Height,
        header: &HeaderRecord,
        txs: &[TxApply],
    ) -> Result<Fk, QueryError> {
        match self.tip_height() {
            None => {
                if height != Height::GENESIS {
                    return Err(StoreError::Corrupt("first block must be genesis height"));
                }
            }
            Some(tip) => {
                let expect = tip.next().ok_or(StoreError::Corrupt("height overflow"))?;
                if height != expect {
                    return Err(StoreError::Corrupt("connect height not tip+1"));
                }
            }
        }

        let header_fk = self.store.put_header(header)?;
        let mut tx_fks = Vec::with_capacity(txs.len());

        for ta in txs {
            let in_start = if ta.inputs.is_empty() {
                Fk::NULL
            } else {
                Fk(self.store.inputs.count() + 1)
            };
            let out_start = if ta.outputs.is_empty() {
                Fk::NULL
            } else {
                Fk(self.store.outputs.count() + 1)
            };

            let mut tx = ta.tx.clone();
            tx.input_start_fk = in_start;
            tx.input_count = ta.inputs.len() as u32;
            tx.output_start_fk = out_start;
            tx.output_count = ta.outputs.len() as u32;
            // Put I/O first so parent_tx_fk can be set — need tx_fk first though.
            // Order: put tx (with correct starts for upcoming I/O fks), then I/O with parent.
            let tx_fk = self.store.put_tx(&tx)?;

            for (i, inp) in ta.inputs.iter().enumerate() {
                let mut rec = inp.clone();
                rec.parent_tx_fk = tx_fk;
                rec.index = i as u32;
                self.store.put_input(&rec)?;
                if rec.prev_txid != [0u8; 32] {
                    self.store
                        .put_spend(&rec.prev_txid, rec.prev_index, tx_fk, rec.index)?;
                }
            }
            for (i, out) in ta.outputs.iter().enumerate() {
                let mut rec = out.clone();
                rec.parent_tx_fk = tx_fk;
                rec.index = i as u32;
                self.store.put_output(&rec)?;
            }

            self.store.strong_tx.set_strong(tx_fk, header_fk)?;
            tx_fks.push(tx_fk);
        }

        self.store.block_txs.put_list(height, &tx_fks)?;
        self.store.confirmed.set(height, header_fk)?;
        Ok(header_fk)
    }

    /// Disconnect the current tip (Class C only; archive rows remain).
    pub fn disconnect_tip(&self) -> Result<(), QueryError> {
        let height = self
            .tip_height()
            .ok_or(StoreError::Corrupt("no tip to disconnect"))?;
        let tx_fks = self.store.block_txs.get_list(height)?;
        for fk in &tx_fks {
            self.store.strong_tx.set_unstrong(*fk)?;
        }
        self.store.block_txs.clear_height(height)?;
        self.store.confirmed.disconnect_tip(height)?;
        Ok(())
    }

    pub fn block_tx_fks(&self, height: Height) -> Result<Vec<Fk>, QueryError> {
        self.store.block_txs.get_list(height)
    }

    pub fn header_at_height(
        &self,
        height: Height,
    ) -> Result<Option<(Fk, HeaderRecord)>, QueryError> {
        match self.store.confirmed.get(height)? {
            None => Ok(None),
            Some(fk) => Ok(Some((fk, self.store.get_header(fk)?))),
        }
    }

    /// Best-chain height of a header hash, if it is confirmed.
    pub fn height_of_hash(&self, hash: &[u8; 32]) -> Result<Option<Height>, QueryError> {
        let Some(tip) = self.tip_height() else {
            return Ok(None);
        };
        // Walk from tip down; confirmed chain is contiguous from 0.
        for h in (0..=tip.0).rev() {
            let height = Height(h);
            if let Some((_, rec)) = self.header_at_height(height)? {
                if &rec.hash == hash {
                    return Ok(Some(height));
                }
            }
        }
        Ok(None)
    }

    /// Wire header for a confirmed height (resolves prev hash from archive).
    pub fn wire_header_at_height(&self, height: Height) -> Result<BlockHeader, QueryError> {
        let (_fk, rec) = self
            .header_at_height(height)?
            .ok_or(StoreError::NotFound)?;
        self.wire_header_from_record(&rec)
    }

    fn wire_header_from_record(&self, rec: &HeaderRecord) -> Result<BlockHeader, QueryError> {
        let prev_blockhash = if rec.prev_fk.is_null() {
            BlockHash::from_byte_array([0u8; 32])
        } else {
            let prev = self.store.get_header(rec.prev_fk)?;
            BlockHash::from_byte_array(prev.hash)
        };
        Ok(wire_header(rec, prev_blockhash))
    }

    /// Reconstruct a full wire block at a confirmed height from the relational archive.
    ///
    /// Uses header fields + ordered `TxRecord.raw` (full witness consensus encoding).
    pub fn reconstruct_block_at_height(&self, height: Height) -> Result<Block, QueryError> {
        let header = self.wire_header_at_height(height)?;
        let tx_fks = self.block_tx_fks(height)?;
        if tx_fks.is_empty() {
            return Err(StoreError::Corrupt("block has no transactions"));
        }
        let mut txdata = Vec::with_capacity(tx_fks.len());
        for fk in tx_fks {
            let rec = self.get_tx(fk)?;
            let tx = decode_tx_raw(&rec.raw)?;
            txdata.push(tx);
        }
        let block = Block { header, txdata };
        // Integrity: reconstructed hash must match stored header hash.
        let (_fk, stored) = self
            .header_at_height(height)?
            .ok_or(StoreError::NotFound)?;
        if block.block_hash().to_byte_array() != stored.hash {
            return Err(StoreError::Corrupt("reconstruct hash mismatch"));
        }
        Ok(block)
    }

    /// Reconstruct by block hash if the hash is on the best (confirmed) chain.
    pub fn reconstruct_block_by_hash(&self, hash: &[u8; 32]) -> Result<Option<Block>, QueryError> {
        match self.height_of_hash(hash)? {
            None => Ok(None),
            Some(h) => Ok(Some(self.reconstruct_block_at_height(h)?)),
        }
    }

    /// Locator hashes newest-first for P2P `getheaders` (from confirmed chain).
    pub fn locator_hashes(&self) -> Result<Vec<BlockHash>, QueryError> {
        let Some(tip) = self.tip_height() else {
            return Ok(vec![BlockHash::from_byte_array([0u8; 32])]);
        };
        let mut out = Vec::new();
        let mut h = tip.0 as i64;
        let mut step = 1i64;
        while h >= 0 {
            let (_fk, rec) = self
                .header_at_height(Height(h as u32))?
                .ok_or(StoreError::NotFound)?;
            out.push(BlockHash::from_byte_array(rec.hash));
            if out.len() >= 10 {
                step *= 2;
            }
            h -= step;
        }
        // Always include genesis.
        if let Some((_fk, rec)) = self.header_at_height(Height::GENESIS)? {
            let g = BlockHash::from_byte_array(rec.hash);
            if out.last() != Some(&g) {
                out.push(g);
            }
        }
        Ok(out)
    }

    /// Headers on the best chain after the first matching locator entry, up to `limit` (max 2000).
    pub fn headers_after_locator(
        &self,
        locator: &[BlockHash],
        stop: BlockHash,
        limit: usize,
    ) -> Result<Vec<BlockHeader>, QueryError> {
        let Some(tip) = self.tip_height() else {
            return Ok(Vec::new());
        };
        let limit = limit.min(2000);
        let mut start = 0u32;
        'outer: for loc in locator {
            if loc.to_byte_array() == [0u8; 32] {
                start = 0;
                break;
            }
            // Find height of locator on our chain.
            if let Some(h) = self.height_of_hash(&loc.to_byte_array())? {
                start = h.0.saturating_add(1);
                break 'outer;
            }
        }
        // If no locator matched, Bitcoin peers typically start from genesis; we start at 0.
        let mut out = Vec::new();
        let mut h = start;
        while h <= tip.0 && out.len() < limit {
            let hdr = self.wire_header_at_height(Height(h))?;
            let hash = hdr.block_hash();
            out.push(hdr);
            if hash == stop && stop.to_byte_array() != [0u8; 32] {
                break;
            }
            h += 1;
        }
        Ok(out)
    }

    pub fn flush(&self) -> Result<(), QueryError> {
        if !self.store.path().exists() {
            return Err(StoreError::NotDirectory(self.store.path().to_path_buf()));
        }
        self.store.flush()
    }
}

fn wire_header(rec: &HeaderRecord, prev_blockhash: BlockHash) -> BlockHeader {
    BlockHeader {
        version: BlockVersion::from_consensus(rec.version),
        prev_blockhash,
        merkle_root: TxMerkleNode::from_byte_array(rec.merkle_root),
        time: rec.timestamp,
        bits: CompactTarget::from_consensus(rec.bits),
        nonce: rec.nonce,
    }
}

fn decode_tx_raw(raw: &[u8]) -> Result<Transaction, QueryError> {
    let mut cursor = raw;
    Transaction::consensus_decode(&mut cursor).map_err(|_| StoreError::Corrupt("tx raw decode"))
}

pub fn crate_name() -> &'static str {
    "rbitcoin-query"
}
