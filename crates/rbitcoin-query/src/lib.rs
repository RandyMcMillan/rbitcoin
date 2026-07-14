//! Domain query layer over [`rbitcoin_store::Store`].

use bitcoin::block::{Header as BlockHeader, Version as BlockVersion};
use bitcoin::consensus::Decodable;
use bitcoin::hashes::Hash;
use bitcoin::{Block, BlockHash, CompactTarget, Transaction, TxMerkleNode};
use rbitcoin_primitives::{Fk, Height};
use rbitcoin_store::{
    script_hash, HeaderRecord, InputRecord, OutputRecord, PointRecord, ScriptHashRecord, Store,
    StoreError, TxRecord, UNSPENT,
};
use std::collections::BTreeMap;
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
    /// When false, connect/disconnect skip scripthash multimap (IBD fast path).
    scripthash_index: std::sync::atomic::AtomicBool,
}

impl Query {
    pub fn open_or_create(store_path: impl AsRef<Path>) -> Result<Self, QueryError> {
        Ok(Self {
            store: Store::open_or_create(store_path.as_ref())?,
            scripthash_index: std::sync::atomic::AtomicBool::new(true),
        })
    }

    pub fn store(&self) -> &Store {
        &self.store
    }

    /// Enable/disable Electrum scripthash index writes (default on). Off during IBD for speed.
    pub fn set_scripthash_index(&self, enabled: bool) {
        self.scripthash_index
            .store(enabled, std::sync::atomic::Ordering::SeqCst);
    }

    pub fn scripthash_index_enabled(&self) -> bool {
        self.scripthash_index
            .load(std::sync::atomic::Ordering::SeqCst)
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

        // If this hash is already confirmed, idempotent success.
        if let Some(h) = self.height_of_hash(&header.hash)? {
            if h == height {
                if let Some((fk, _)) = self.get_header_by_hash(&header.hash)? {
                    return Ok(fk);
                }
            }
        }

        // Write txs first; only publish the header/hash-head once the block body
        // is fully archived so a mid-connect crash cannot freeze has_block.
        let mut tx_fks = Vec::with_capacity(txs.len());
        // Placeholder header fk — we put the header after txs when possible.
        // (strong_tx needs header_fk; put header early but only set confirmed at end.)
        let header_fk = self.store.put_header(header)?;

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
                    if self.scripthash_index_enabled() {
                        if let Some(sh) =
                            self.script_hash_for_outpoint(&rec.prev_txid, rec.prev_index)?
                        {
                            let _ = self.store.scripthash.mark_spent(
                                &sh,
                                &rec.prev_txid,
                                rec.prev_index,
                                height.0,
                                tx_fk,
                            )?;
                        }
                    }
                }
            }
            for (i, out) in ta.outputs.iter().enumerate() {
                let mut rec = out.clone();
                rec.parent_tx_fk = tx_fk;
                rec.index = i as u32;
                self.store.put_output(&rec)?;
                if self.scripthash_index_enabled() {
                    let sh = script_hash(&rec.script);
                    self.store.scripthash.put_create(&ScriptHashRecord {
                        scripthash: sh,
                        txid: ta.tx.txid,
                        vout: i as u32,
                        value: rec.value,
                        create_height: height.0,
                        create_tx_fk: tx_fk,
                        spend_height: UNSPENT,
                        spend_tx_fk: Fk::NULL,
                        next: Fk::NULL,
                    })?;
                }
            }

            self.store.strong_tx.set_strong(tx_fk, header_fk)?;
            tx_fks.push(tx_fk);
        }

        self.store.block_txs.put_list(height, &tx_fks)?;
        self.store.confirmed.set(height, header_fk)?;
        Ok(header_fk)
    }

    /// Disconnect the current tip (Class C + scripthash spend clear; archive rows remain).
    pub fn disconnect_tip(&self) -> Result<(), QueryError> {
        let height = self
            .tip_height()
            .ok_or(StoreError::Corrupt("no tip to disconnect"))?;
        let tx_fks = self.store.block_txs.get_list(height)?;

        // Clear spends recorded at this height; creates become unstrong with their txs.
        for &tx_fk in &tx_fks {
            let tx = self.store.get_tx(tx_fk)?;
            if self.scripthash_index_enabled() && tx.input_count > 0 {
                let start = tx.input_start_fk.get().ok_or(StoreError::InvalidFk)?;
                for i in 0..tx.input_count {
                    let inp = self.store.get_input(Fk(start + u64::from(i)))?;
                    if inp.prev_txid != [0u8; 32] {
                        if let Some(sh) =
                            self.script_hash_for_outpoint(&inp.prev_txid, inp.prev_index)?
                        {
                            self.store.scripthash.clear_spend_at_height(
                                &sh,
                                &inp.prev_txid,
                                inp.prev_index,
                                height.0,
                            )?;
                        }
                    }
                }
            }
            self.store.strong_tx.set_unstrong(tx_fk)?;
        }
        self.store.block_txs.clear_height(height)?;
        self.store.confirmed.disconnect_tip(height)?;
        Ok(())
    }

    fn script_hash_for_outpoint(
        &self,
        txid: &[u8; 32],
        vout: u32,
    ) -> Result<Option<[u8; 32]>, QueryError> {
        let Some((_fk, prev)) = self.store.get_tx_by_txid(txid)? else {
            return Ok(None);
        };
        if vout >= prev.output_count {
            return Ok(None);
        }
        let start = match prev.output_start_fk.get() {
            Some(s) => s,
            None => return Ok(None),
        };
        let out = self.store.get_output(Fk(start + u64::from(vout)))?;
        Ok(Some(script_hash(&out.script)))
    }

    /// Confirmed Electrum-style history for a scripthash: (height, txid) pairs, height order.
    pub fn scripthash_history(
        &self,
        scripthash: &[u8; 32],
    ) -> Result<Vec<ScriptHashHistoryItem>, QueryError> {
        let entries = self.store.scripthash.entries(scripthash)?;
        let mut by_txid: BTreeMap<[u8; 32], i64> = BTreeMap::new();
        for (_fk, rec) in entries {
            if !self.store.strong_tx.is_strong(rec.create_tx_fk)? {
                continue;
            }
            // Funding tx
            by_txid
                .entry(rec.txid)
                .and_modify(|h| *h = (*h).min(i64::from(rec.create_height)))
                .or_insert(i64::from(rec.create_height));
            // Spending tx if spent on best chain
            if !rec.is_unspent() {
                if let Some(sfk) = rec.spend_tx_fk.get() {
                    if self.store.strong_tx.is_strong(Fk(sfk))? {
                        let spend_tx = self.store.get_tx(Fk(sfk))?;
                        by_txid
                            .entry(spend_tx.txid)
                            .and_modify(|h| *h = (*h).min(i64::from(rec.spend_height)))
                            .or_insert(i64::from(rec.spend_height));
                    }
                }
            }
        }
        let mut items: Vec<ScriptHashHistoryItem> = by_txid
            .into_iter()
            .map(|(txid, height)| ScriptHashHistoryItem { height, txid })
            .collect();
        items.sort_by_key(|i| i.height);
        Ok(items)
    }

    /// Confirmed balance (confirmed funded − confirmed spent) for a scripthash.
    pub fn scripthash_balance(&self, scripthash: &[u8; 32]) -> Result<ScriptHashBalance, QueryError> {
        let mut confirmed = 0i64;
        for (_fk, rec) in self.store.scripthash.entries(scripthash)? {
            if !self.store.strong_tx.is_strong(rec.create_tx_fk)? {
                continue;
            }
            if rec.is_unspent() {
                confirmed = confirmed.saturating_add(rec.value);
            }
        }
        Ok(ScriptHashBalance {
            confirmed,
            unconfirmed: 0,
        })
    }

    /// Confirmed UTXOs for a scripthash.
    pub fn scripthash_listunspent(
        &self,
        scripthash: &[u8; 32],
    ) -> Result<Vec<ScriptHashUtxo>, QueryError> {
        let mut out = Vec::new();
        for (_fk, rec) in self.store.scripthash.entries(scripthash)? {
            if !self.store.strong_tx.is_strong(rec.create_tx_fk)? {
                continue;
            }
            if rec.is_unspent() {
                out.push(ScriptHashUtxo {
                    tx_hash: rec.txid,
                    tx_pos: rec.vout,
                    height: rec.create_height,
                    value: rec.value,
                });
            }
        }
        out.sort_by(|a, b| a.height.cmp(&b.height).then(a.tx_pos.cmp(&b.tx_pos)));
        Ok(out)
    }

    pub fn set_archive_mode(&self, enabled: bool) -> Result<(), QueryError> {
        self.store.set_archive_mode(enabled)
    }

    pub fn finalize_through(&self, height: u32) -> Result<(), QueryError> {
        self.store.finalize_through(height)
    }

    pub fn archive_epoch(&self) -> rbitcoin_store::ArchiveEpoch {
        self.store.epoch()
    }

    /// Merkle proof for `txid` in the confirmed block at `height` (Electrum get_merkle).
    pub fn merkle_proof(
        &self,
        height: Height,
        txid: &[u8; 32],
    ) -> Result<MerkleProof, QueryError> {
        use bitcoin::hashes::{sha256d, Hash as _};

        let fks = self.block_tx_fks(height)?;
        let mut txids = Vec::with_capacity(fks.len());
        let mut pos = None;
        for (i, fk) in fks.iter().enumerate() {
            let tx = self.get_tx(*fk)?;
            if &tx.txid == txid {
                pos = Some(i);
            }
            txids.push(tx.txid);
        }
        let pos = pos.ok_or(StoreError::NotFound)?;
        let mut branch = Vec::new();
        let mut idx = pos;
        let mut layer: Vec<[u8; 32]> = txids;
        while layer.len() > 1 {
            if layer.len() % 2 == 1 {
                layer.push(*layer.last().unwrap());
            }
            let sibling = if idx % 2 == 0 {
                layer[idx + 1]
            } else {
                layer[idx - 1]
            };
            branch.push(sibling);
            let mut next = Vec::with_capacity(layer.len() / 2);
            let mut i = 0;
            while i < layer.len() {
                let mut buf = [0u8; 64];
                buf[0..32].copy_from_slice(&layer[i]);
                buf[32..64].copy_from_slice(&layer[i + 1]);
                next.push(sha256d::Hash::hash(&buf).to_byte_array());
                i += 2;
            }
            layer = next;
            idx /= 2;
        }
        Ok(MerkleProof {
            block_height: height.0,
            pos,
            merkle: branch,
        })
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

    /// Best-chain height of a header hash, if it is **confirmed** on the tip chain.
    ///
    /// Archive may contain orphan header rows (partial connect failures). Those are
    /// not reported here — only hashes reachable as `confirmed[height]`.
    pub fn height_of_hash(&self, hash: &[u8; 32]) -> Result<Option<Height>, QueryError> {
        let Some(tip) = self.tip_height() else {
            // Only genesis can be "confirmed" with no tip.
            return Ok(None);
        };
        // Fast path: tip
        if let Some((tip_fk, rec)) = self.header_at_height(tip)? {
            if &rec.hash == hash {
                return Ok(Some(tip));
            }
            // Fast path: tip-1 (common parent checks)
            if tip.0 > 0 {
                if let Some((_, prec)) = self.header_at_height(Height(tip.0 - 1))? {
                    if &prec.hash == hash {
                        return Ok(Some(Height(tip.0 - 1)));
                    }
                }
            }
            let _ = tip_fk;
        }
        // Must appear in archive at all.
        let Some((fk, _rec)) = self.get_header_by_hash(hash)? else {
            return Ok(None);
        };
        // Confirm it is the header at some best-chain height by walking tip→genesis
        // via confirmed table only (not the orphaned archive row).
        // Prefer short reverse scan from tip (IBD / locator hot path).
        const RECENT: u32 = 4096;
        let start = tip.0.saturating_sub(RECENT);
        for h in (start..=tip.0).rev() {
            let height = Height(h);
            if let Some((hfk, rec)) = self.header_at_height(height)? {
                if hfk == fk || &rec.hash == hash {
                    return Ok(Some(height));
                }
            }
        }
        // Full scan only if not in recent window (rare for IBD).
        if start > 0 {
            for h in (0..start).rev() {
                let height = Height(h);
                if let Some((hfk, rec)) = self.header_at_height(height)? {
                    if hfk == fk || &rec.hash == hash {
                        return Ok(Some(height));
                    }
                }
            }
        }
        // Present in archive but not on best chain (orphan header row).
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

/// Electrum `blockchain.scripthash.get_history` row (confirmed only in v1).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ScriptHashHistoryItem {
    pub height: i64,
    pub txid: [u8; 32],
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ScriptHashBalance {
    pub confirmed: i64,
    pub unconfirmed: i64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ScriptHashUtxo {
    pub tx_hash: [u8; 32],
    pub tx_pos: u32,
    pub height: u32,
    pub value: i64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MerkleProof {
    pub block_height: u32,
    pub pos: usize,
    pub merkle: Vec<[u8; 32]>,
}

pub fn crate_name() -> &'static str {
    "rbitcoin-query"
}
