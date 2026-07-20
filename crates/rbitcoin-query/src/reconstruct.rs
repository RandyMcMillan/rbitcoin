//! Reconstruct wire blocks/txs and merkle proofs; confirm wave fill.

use super::*;
use std::collections::{HashMap, HashSet};
use std::time::Instant;

impl Query {
    /// Build wave prevout map. **Requires** prewarm: bodies + parents in cache.
    ///
    /// Prefers stashed thin edges + [`crate::confirm_parent_cache`] so confirm
    /// does not re-walk inputs or re-read Class A after a full prewarm.
    pub fn wave_fill_for_block_hashes(
        &self,
        hashes: &[[u8; 32]],
    ) -> Result<(usize, crate::WavePrevoutCache), QueryError> {
        use crate::wave_fill_stats::{self as wf, add as wf_add};
        use crate::wave_prevout::ThinInput;

        let mut wave_fks: HashSet<u64> = HashSet::new();
        let mut wave_tx_fks: Vec<Fk> = Vec::new();
        for hash in hashes {
            let Some((header_fk, _)) = self.get_header_by_hash(hash)? else {
                continue;
            };
            let Some(tx_fks) = self.store.header_txs.get_list(header_fk)? else {
                continue;
            };
            for fk in tx_fks {
                if let Some(id) = fk.get() {
                    wave_fks.insert(id);
                }
                wave_tx_fks.push(fk);
            }
        }

        let mut wave =
            crate::WavePrevoutCache::with_capacity(wave_tx_fks.len(), wave_tx_fks.len());
        let mut noted = 0usize;

        // Pass 2: wave bodies from prewarm body cache (store only if missing).
        // Prefer stashed thin edges from prewarm; fall back to walking inputs.
        let t_body = Instant::now();
        let mut parent_needed: HashMap<u64, HashSet<u32>> = HashMap::new();
        for &fk in &wave_tx_fks {
            let (tx, outs, inputs) = self.load_body_prewarmed(fk)?;
            let cb_h = self.coinbase_height_for_tx_with_input0(fk, &tx, inputs.first())?;
            wave.insert_parent_live(fk, tx, outs, Some(cb_h));
            noted += 1;

            let edges: Vec<ThinInput> =
                if let Some(stashed) = self.confirm_parents.get_thin_inputs(fk) {
                    stashed
                        .into_iter()
                        .map(|e| ThinInput {
                            create_fk: e.create_fk,
                            prev_index: e.prev_index,
                        })
                        .collect()
                } else if inputs.is_empty() {
                    Vec::new()
                } else {
                    // Fallback: no prewarm thin edges (tests / last-mile miss).
                    self.thin_edges_from_inputs(&inputs, &wave)?
                };

            if edges.is_empty() {
                continue;
            }
            for e in &edges {
                let Some(pid) = e.create_fk else {
                    continue;
                };
                if wave_fks.contains(&pid) {
                    continue;
                }
                parent_needed
                    .entry(pid)
                    .or_default()
                    .insert(e.prev_index);
            }
            wave.insert_thin_inputs(fk, edges);
        }
        wf_add(&wf::BODY_NS, t_body.elapsed().as_nanos() as u64);

        // Pass 3: external parents from ConfirmParentCache (prewarm filled).
        let mut parents: Vec<(u64, HashSet<u32>)> = parent_needed.into_iter().collect();
        parents.sort_unstable_by_key(|(pid, _)| *pid);

        let t_tx = Instant::now();
        for (pid, needed_vouts) in &parents {
            let fk = Fk(*pid);
            let (tx, outs_map) = if let Some((tx, outs)) = self.confirm_parents.get_parent_outs(fk)
            {
                (tx, outs)
            } else if let Some((tx, outs, _)) = self.confirm_parents.get_body(fk) {
                let mut m = HashMap::new();
                for &v in needed_vouts {
                    if let Some(o) = outs.get(v as usize) {
                        m.insert(v, o.clone());
                    }
                }
                (tx, m)
            } else {
                // Prewarm miss — should be rare; keep last-resort for safety.
                let (tx, _, raw) = self.store.get_tx_full(fk)?;
                let mut m = HashMap::new();
                for &v in needed_vouts {
                    if let Some(o) = raw.get(v as usize) {
                        m.insert(v, o.clone());
                    }
                }
                (tx, m)
            };
            wf_add(&wf::PARENT_TX_NS, 0);
            wf_add(&wf::PARENT_OUT_NS, 0);

            let n = tx.output_count as usize;
            let mut slots: Vec<Option<OutputRecord>> = vec![None; n];
            for &v in needed_vouts {
                let vi = v as usize;
                if vi >= n {
                    continue;
                }
                if self.catchup_is_spent(&tx.txid, v)? {
                    continue;
                }
                if let Some(o) = outs_map.get(&v) {
                    slots[vi] = Some(o.clone());
                }
            }
            wf_add(&wf::SPENT_NS, 0);

            let t_cb = Instant::now();
            let cb_h = self.coinbase_height_for_tx(fk, &tx)?;
            wf_add(&wf::CB_HEIGHT_NS, t_cb.elapsed().as_nanos() as u64);

            wave.insert_parent_slots(fk, tx, slots, Some(cb_h));
            noted += 1;
        }
        let _ = t_tx;

        Ok((noted, wave))
    }

    /// Build thin create-fk edges by walking inputs (wave_fill fallback).
    fn thin_edges_from_inputs(
        &self,
        inputs: &[InputRecord],
        wave: &crate::WavePrevoutCache,
    ) -> Result<Vec<crate::wave_prevout::ThinInput>, QueryError> {
        use crate::wave_prevout::ThinInput;
        let mut edges = Vec::with_capacity(inputs.len());
        for inp in inputs {
            if inp.is_coinbase() {
                edges.push(ThinInput {
                    create_fk: None,
                    prev_index: inp.prev_index,
                });
                continue;
            }
            let create_fk = wave
                .get_by_txid(&inp.prev_txid, inp.prev_index)
                .map(|(pfk, _, _)| pfk)
                .or_else(|| self.confirm_parents.get_by_txid(&inp.prev_txid))
                .or(self
                    .ibd_utxo_create_fk(&inp.prev_txid, inp.prev_index)
                    .ok()
                    .flatten())
                .or(self.tx_fk_by_txid(&inp.prev_txid).ok().flatten());
            edges.push(ThinInput {
                create_fk: create_fk.and_then(|f| f.get()),
                prev_index: inp.prev_index,
            });
        }
        Ok(edges)
    }

    /// Body for wave/wire: prewarm cache first, then store.
    fn load_body_prewarmed(
        &self,
        fk: Fk,
    ) -> Result<(TxRecord, Vec<OutputRecord>, Vec<InputRecord>), QueryError> {
        if let Some((tx, outs, inputs)) = self.confirm_parents.get_body(fk) {
            return Ok((tx, outs, inputs));
        }
        self.load_body_from_store(fk)
    }

    fn load_body_from_store(
        &self,
        fk: Fk,
    ) -> Result<(TxRecord, Vec<OutputRecord>, Vec<InputRecord>), QueryError> {
        let (tx, inputs, outs) = self.store.get_tx_full(fk)?;
        Ok((tx, outs, inputs))
    }

    fn coinbase_height_for_tx(
        &self,
        fk: Fk,
        tx: &TxRecord,
    ) -> Result<Option<u32>, QueryError> {
        if tx.input_count != 1 {
            return Ok(None);
        }
        let inp = self.tx_input_at_fk(fk, tx, 0)?;
        self.coinbase_height_for_tx_with_input0(fk, tx, Some(&inp))
    }

    fn coinbase_height_for_tx_with_input0(
        &self,
        fk: Fk,
        tx: &TxRecord,
        input0: Option<&InputRecord>,
    ) -> Result<Option<u32>, QueryError> {
        if tx.input_count != 1 {
            return Ok(None);
        }
        let inp = match input0 {
            Some(i) => i,
            None => {
                let i = self.tx_input_at_fk(fk, tx, 0)?;
                return self.coinbase_height_for_tx_with_input0(fk, tx, Some(&i));
            }
        };
        let is_cb = inp.is_coinbase()
            || (inp.prev_txid == [0u8; 32] && inp.prev_index == 0xffff_ffff);
        if !is_cb {
            return Ok(None);
        }
        Ok(self.store.tx_height.get(fk)?)
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
        let (rec, stored_outputs, stored_inputs) = self.load_body_prewarmed(tx_fk)?;
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
        if tx_fks.is_empty() {
            return Err(StoreError::Corrupt("block has no transactions"));
        }
        let header = self.wire_header_from_record(&rec)?;
        let mut txdata = Vec::with_capacity(tx_fks.len());
        for fk in tx_fks {
            txdata.push(self.reconstruct_tx(fk)?);
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
