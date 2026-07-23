//! Reconstruct wire blocks/txs and merkle proofs.

use super::*;
use std::collections::HashMap;
use std::time::Instant;

impl Query {
    /// Resolve create maturity: `None` = not coinbase, `Some(h)` = coinbase height.
    ///
    /// Only true coinbases (1-in + null prev), not every 1-in tx. Prevout scan
    /// skips script/witness. Load stashes this on parent pin when possible.
    pub(crate) fn resolve_parent_coinbase_height(
        &self,
        fk: Fk,
        input_count: u32,
        body_range: Option<(u64, u64)>,
    ) -> Result<Option<u32>, QueryError> {
        if input_count != 1 {
            return Ok(None);
        }
        if !self.parent_is_coinbase_at(fk, body_range)? {
            return Ok(None);
        }
        Ok(self.store.tx_height.get(fk)?)
    }

    /// True if Class A body is a coinbase (1-in, null prevout). Prevout-only decode.
    fn parent_is_coinbase_at(
        &self,
        fk: Fk,
        body_range: Option<(u64, u64)>,
    ) -> Result<bool, QueryError> {
        let (meta, prevouts) = match body_range {
            Some((off, len)) => self.store.get_tx_meta_and_prevouts_at(off, len)?,
            None => {
                if let Some((off, len)) = self.confirm_parents.get_body_range(fk) {
                    self.store.get_tx_meta_and_prevouts_at(off, len)?
                } else {
                    self.store.get_tx_meta_and_prevouts(fk)?
                }
            }
        };
        if meta.input_count != 1 {
            return Ok(false);
        }
        Ok(prevouts
            .first()
            .is_some_and(|(fk, v)| fk.is_null() && *v == 0xffff_ffff))
    }

    /// Body for wire rebuild / RPC: clone from parent cache if present, else store.
    fn load_body_from_cache(
        &self,
        fk: Fk,
    ) -> Result<(TxRecord, Vec<OutputRecord>, Vec<InputRecord>), QueryError> {
        if let Some((tx, outs, inputs)) = self.confirm_parents.get_body(fk) {
            use crate::wave_fill_stats::{self as wf, add_count as wf_count};
            wf_count(&wf::BODY_CACHE_MOVE, 1);
            return Ok((tx, outs, inputs));
        }
        self.load_body_from_store(fk)
    }

    fn load_body_from_store(
        &self,
        fk: Fk,
    ) -> Result<(TxRecord, Vec<OutputRecord>, Vec<InputRecord>), QueryError> {
        use crate::wave_fill_stats::{self as wf, add as wf_add, add_count as wf_count};
        let t0 = Instant::now();
        let maj_before = thread_majflt();
        wf_count(&wf::BODY_STORE, 1);
        // Prefer cache-held idx range (skip idx page fault); body pages mlocked.
        let res = if let Some((off, len)) = self.confirm_parents.get_body_range(fk) {
            let (tx, inputs, outs) = self.store.get_tx_full_at(off, len)?;
            Ok((tx, outs, inputs))
        } else {
            let (tx, inputs, outs) = self.store.get_tx_full(fk)?;
            Ok((tx, outs, inputs))
        };
        wf_add(&wf::BODY_STORE_NS, t0.elapsed().as_nanos() as u64);
        if let (Some(b), Some(a)) = (maj_before, thread_majflt()) {
            if a > b {
                wf::add_count(&wf::BODY_STORE_MAJFLT, a - b);
            }
        }
        res
    }

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
            let tx = self.get_tx_class_a(*fk)?;
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
        let header_fk = self
            .store
            .confirmed
            .get(height)?
            .ok_or(StoreError::NotFound)?;
        self.store
            .header_txs
            .get_list(header_fk)?
            .ok_or(StoreError::Corrupt("confirmed header missing body list"))
    }

    /// Reconstruct a consensus `Transaction` from Class A rows (no stored raw).
    pub fn reconstruct_tx(&self, tx_fk: Fk) -> Result<Transaction, QueryError> {
        let (rec, stored_outputs, mut stored_inputs) = self.load_body_from_cache(tx_fk)?;
        let mut cache = HashMap::new();
        self.fill_input_prev_txids_cached(&mut stored_inputs, &mut cache)?;
        Ok(Self::transaction_from_class_a(
            rec,
            stored_outputs,
            stored_inputs,
        ))
    }

    fn transaction_from_class_a(
        rec: TxRecord,
        stored_outputs: Vec<OutputRecord>,
        stored_inputs: Vec<InputRecord>,
    ) -> Transaction {
        // Soft prev_txid may be zero after disk decode — fill from create body below
        // only when caller used fill_input_prev_txids first. Prefer non-zero soft.
        let mut input = Vec::with_capacity(stored_inputs.len());
        for inp in stored_inputs {
            let prev_txid = inp.prev_txid;
            input.push(TxIn {
                previous_output: OutPoint {
                    txid: bitcoin::Txid::from_byte_array(prev_txid),
                    vout: inp.prev_index,
                },
                script_sig: ScriptBuf::from_bytes(inp.script_sig),
                sequence: Sequence::from_consensus(inp.sequence),
                witness: {
                    let refs: Vec<&[u8]> = inp.witness.iter().map(|w| w.as_slice()).collect();
                    Witness::from_slice(&refs)
                },
            });
        }
        let mut output = Vec::with_capacity(stored_outputs.len());
        for out in stored_outputs {
            output.push(TxOut {
                value: Amount::from_sat(out.value as u64),
                script_pubkey: ScriptBuf::from_bytes(out.script),
            });
        }
        Transaction {
            version: TxVersion(rec.version),
            lock_time: LockTime::from_consensus(rec.locktime),
            input,
            output,
        }
    }

    /// Resolve soft `prev_txid` from create_fk without re-reading every parent body.
    ///
    /// Schema v10 stamps create_fk and leaves soft prev_txid zero on disk. Prefer:
    /// 1. already-filled soft prev_txid
    /// 2. confirm parent cache (sparse pin / body)
    /// 3. store `body_txid` (deduped via `cache` across a block)
    pub(crate) fn fill_input_prev_txids_cached(
        &self,
        inputs: &mut [InputRecord],
        cache: &mut HashMap<u64, [u8; 32]>,
    ) -> Result<(), QueryError> {
        for inp in inputs.iter_mut() {
            if inp.is_coinbase() {
                inp.prev_txid = [0u8; 32];
                continue;
            }
            if inp.prev_txid != [0u8; 32] {
                continue;
            }
            let Some(id) = inp.create_fk.get() else {
                return Err(StoreError::Corrupt(
                    "input missing create_fk for wire rebuild",
                ));
            };
            if let Some(&txid) = cache.get(&id) {
                inp.prev_txid = txid;
                continue;
            }
            let txid = match self.confirm_parents.get_parent_txid(Fk(id)) {
                Some(t) => t,
                None => self.store.txs.body_txid(Fk(id))?,
            };
            cache.insert(id, txid);
            inp.prev_txid = txid;
        }
        Ok(())
    }

    /// Consensus-encoded wire bytes for a stored tx (Electrum / RPC).
    pub fn tx_wire_bytes(&self, tx_fk: Fk) -> Result<Vec<u8>, QueryError> {
        use bitcoin::consensus::Encodable;
        let tx = self.reconstruct_tx(tx_fk)?;
        let mut raw = Vec::new();
        tx.consensus_encode(&mut raw)
            .map_err(|_| StoreError::Corrupt("tx encode"))?;
        Ok(raw)
    }

    pub fn reconstruct_archived_block(
        &self,
        hash: &[u8; 32],
    ) -> Result<Option<Block>, QueryError> {
        let Some((header_fk, rec)) = self.get_header_by_hash(hash)? else {
            return Ok(None);
        };
        let Some(tx_fks) = self.store.header_txs.get_list(header_fk)? else {
            return Ok(None);
        };
        self.reconstruct_archived_block_from_parts(rec, tx_fks)
            .map(Some)
    }

    /// Wire rebuild when header row + tx fk list are already known (confirm batch).
    pub fn reconstruct_archived_block_from_parts(
        &self,
        rec: HeaderRecord,
        tx_fks: Vec<Fk>,
    ) -> Result<Block, QueryError> {
        self.reconstruct_archived_block_from_parts_cached(rec, tx_fks, None)
    }

    /// Confirm hot path: bodies from load-stage parent cache (store fallback).
    ///
    /// `prev_hash`: when set (load header plan), wire header needs no store IO.
    pub fn reconstruct_archived_block_from_parts_cached(
        &self,
        rec: HeaderRecord,
        tx_fks: Vec<Fk>,
        prev_hash: Option<[u8; 32]>,
    ) -> Result<Block, QueryError> {
        if tx_fks.is_empty() {
            return Err(StoreError::Corrupt("block has no transactions"));
        }
        let header = self.wire_header_from_record_prev(&rec, prev_hash)?;
        let mut txdata = Vec::with_capacity(tx_fks.len());
        // Dedup create_fk → txid across the whole block.
        let mut prev_txid_cache: HashMap<u64, [u8; 32]> = HashMap::new();
        for fk in tx_fks {
            let (rec_tx, stored_outputs, mut stored_inputs) = self.load_body_from_cache(fk)?;
            self.fill_input_prev_txids_cached(&mut stored_inputs, &mut prev_txid_cache)?;
            txdata.push(Self::transaction_from_class_a(
                rec_tx,
                stored_outputs,
                stored_inputs,
            ));
        }
        Ok(Block { header, txdata })
    }

    /// Reconstruct a full wire block at a confirmed height from the relational archive.
    pub fn reconstruct_block_at_height(&self, height: Height) -> Result<Block, QueryError> {
        let header = self.wire_header_at_height(height)?;
        let tx_fks = self.block_tx_fks(height)?;
        if tx_fks.is_empty() {
            return Err(StoreError::Corrupt("block has no transactions"));
        }
        let mut txdata = Vec::with_capacity(tx_fks.len());
        for fk in tx_fks {
            txdata.push(self.reconstruct_tx(fk)?);
        }
        let block = Block { header, txdata };
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
}

/// Process majflt sample (Linux); `None` when unavailable.
#[cfg(target_os = "linux")]
fn thread_majflt() -> Option<u64> {
    let s = std::fs::read_to_string("/proc/self/status").ok()?;
    for line in s.lines() {
        if let Some(rest) = line.strip_prefix("MajFlt:") {
            return rest.trim().split_whitespace().next()?.parse().ok();
        }
    }
    None
}

#[cfg(not(target_os = "linux"))]
fn thread_majflt() -> Option<u64> {
    None
}
