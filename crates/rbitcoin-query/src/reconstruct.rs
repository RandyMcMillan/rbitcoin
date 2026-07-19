//! Reconstruct wire blocks/txs and merkle proofs.

use super::*;

impl Query {
    /// Prefetch Class A (tx + input/output runs) for archived blocks about to be
    /// confirmed. Call once per confirm wave so reconstruct hits RAM.
    ///
    /// Loads **store** directly (does not go through tip_prevout). Returns number
    /// of txs inserted into [`crate::class_a_cache::ClassACache`].
    pub fn prefetch_class_a_for_block_hashes(
        &self,
        hashes: &[[u8; 32]],
    ) -> Result<usize, QueryError> {
        let mut noted = 0usize;
        for hash in hashes {
            let Some((header_fk, _)) = self.get_header_by_hash(hash)? else {
                continue;
            };
            let Some(tx_fks) = self.store.header_txs.get_list(header_fk)? else {
                continue;
            };
            for fk in tx_fks {
                if self.class_a_cache.has_reconstruct_ready(fk) {
                    continue;
                }
                // Bypass Query::get_tx so we don't thin-note or tip_prevout miss-spam.
                let tx = self.store.get_tx(fk)?;
                self.remember_txid(tx.txid, fk);
                let inputs = if tx.input_count > 0 {
                    let run = tx.input_start_fk.get().ok_or(StoreError::InvalidFk)?;
                    Some(self.store.get_input_run(Fk(run), tx.input_count)?)
                } else {
                    None
                };
                let outputs = if tx.output_count > 0 {
                    let run = tx.output_start_fk.get().ok_or(StoreError::InvalidFk)?;
                    Some(self.store.get_output_run(Fk(run), tx.output_count)?)
                } else {
                    None
                };
                self.class_a_cache.note(fk, tx, outputs, inputs);
                noted += 1;
            }
        }
        Ok(noted)
    }

    /// Prefetch parents + wave-body creates into [`WavePrevoutCache`].
    ///
    /// Call after [`Self::prefetch_class_a_for_block_hashes`] so body runs are warm.
    /// Wave-body rows are taken from Class A **without** re-reading the store when
    /// reconstruct-ready (avoids double Class A IO). External parents load **only
    /// needed vouts** (sparse `get_output_at`), not full output runs.
    ///
    /// Returns `(entries_loaded, wave_map)`.
    pub fn prefetch_tip_prevouts_for_block_hashes(
        &self,
        hashes: &[[u8; 32]],
    ) -> Result<(usize, crate::WavePrevoutCache), QueryError> {
        use crate::wave_prevout::ThinInput;
        use std::collections::{HashMap, HashSet};

        // Pass 1: wave body fks.
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

        // Pass 2: wave bodies from Class A (prefetched) — one cache hit each, no store.
        let mut parent_needed: HashMap<u64, HashSet<u32>> = HashMap::new();
        for &fk in &wave_tx_fks {
            let (tx, outs, inputs) = self.wave_body_from_class_a(fk)?;
            // Parent-as-create for same-run spends.
            let cb_h = self.coinbase_height_for_tx_with_input0(fk, &tx, inputs.first())?;
            wave.insert_parent_live(fk, tx.clone(), outs, Some(cb_h));
            wave.insert_tx(fk, tx.clone());
            noted += 1;

            if inputs.is_empty() {
                continue;
            }
            let mut edges = Vec::with_capacity(inputs.len());
            for inp in &inputs {
                let prev_fk = inp.prev_tx_fk.get();
                edges.push(ThinInput {
                    prev_tx_fk: prev_fk,
                    prev_index: inp.prev_index,
                });
                if inp.is_coinbase() {
                    continue;
                }
                let Some(pid) = prev_fk else {
                    continue;
                };
                if wave_fks.contains(&pid) {
                    continue;
                }
                parent_needed
                    .entry(pid)
                    .or_default()
                    .insert(inp.prev_index);
            }
            wave.insert_thin_inputs(fk, edges);
        }

        // Pass 3: external parents — sparse needed vouts only (no full run).
        let spend_index_on = self.spend_index_enabled();
        for (pid, needed_vouts) in parent_needed {
            let fk = Fk(pid);
            // Prefer tip_prevout live slots (write-through unspent).
            if needed_vouts
                .iter()
                .any(|&v| self.tip_prevout_cache.has_live_output(fk, v))
                || self.tip_prevout_cache.has_full_outputs(fk)
            {
                if let Some(tx) = self.tip_prevout_cache.get_tx(fk) {
                    let n = tx.output_count as usize;
                    let mut slots: Vec<Option<OutputRecord>> = vec![None; n];
                    let mut any = false;
                    for &v in &needed_vouts {
                        if let Some(o) = self.tip_prevout_cache.get_output_at(fk, v) {
                            if (v as usize) < slots.len() {
                                slots[v as usize] = Some(o);
                                any = true;
                            }
                        }
                    }
                    if any {
                        let cb_h = self.coinbase_height_for_tx(fk, &tx)?;
                        wave.insert_parent_slots(fk, tx, slots, Some(cb_h));
                        noted += 1;
                        continue;
                    }
                }
            }

            let tx = self.get_tx_class_a(fk)?;
            let slots =
                self.parent_needed_output_slots(fk, &tx, &needed_vouts, spend_index_on)?;
            let cb_h = self.coinbase_height_for_tx(fk, &tx)?;
            wave.insert_parent_slots(fk, tx, slots, Some(cb_h));
            noted += 1;
        }
        Ok((noted, wave))
    }

    /// Wave-body tx material from Class A after prefetch (no second store pass).
    ///
    /// Falls back to store+cache fill only if prefetch missed (eviction / race).
    fn wave_body_from_class_a(
        &self,
        fk: Fk,
    ) -> Result<(TxRecord, Vec<OutputRecord>, Vec<InputRecord>), QueryError> {
        // Fast path: reconstruct-ready entry — clone from cache only.
        if self.class_a_cache.has_reconstruct_ready(fk) {
            let tx = self
                .class_a_cache
                .get_tx(fk)
                .ok_or(StoreError::Corrupt("class_a reconstruct-ready without tx"))?;
            let outs = if tx.output_count == 0 {
                Vec::new()
            } else {
                self.class_a_cache
                    .get_outputs(fk)
                    .ok_or(StoreError::Corrupt("class_a reconstruct-ready without outs"))?
            };
            let inputs = if tx.input_count == 0 {
                Vec::new()
            } else {
                self.class_a_cache
                    .get_inputs(fk)
                    .ok_or(StoreError::Corrupt("class_a reconstruct-ready without ins"))?
            };
            return Ok((tx, outs, inputs));
        }
        // Cold fallback: one store load, fill class_a, return owned.
        let tx = self.get_tx_class_a(fk)?;
        let outs = if tx.output_count == 0 {
            Vec::new()
        } else {
            self.tx_output_run_class_a(&tx)?
        };
        let inputs = if tx.input_count == 0 {
            Vec::new()
        } else {
            self.tx_input_run(&tx)?
        };
        Ok((tx, outs, inputs))
    }

    /// Load only **needed** parent vouts (sparse). Avoids full `get_output_run` when
    /// the parent has many outputs but only a few are spent in this wave.
    ///
    /// Strategy:
    /// 1. Class A per-vout if the full run is already cached
    /// 2. Else if many vouts needed relative to count → one full run (sequential)
    /// 3. Else per-vout `get_output_at` (random, small)
    fn parent_needed_output_slots(
        &self,
        fk: Fk,
        tx: &TxRecord,
        needed_vouts: &std::collections::HashSet<u32>,
        spend_index_on: bool,
    ) -> Result<Vec<Option<OutputRecord>>, QueryError> {
        let n = tx.output_count as usize;
        let mut slots: Vec<Option<OutputRecord>> = vec![None; n];
        if n == 0 || needed_vouts.is_empty() {
            return Ok(slots);
        }

        // Prefer already-cached full run (no extra IO).
        if let Some(raw) = self.class_a_cache.get_outputs(fk) {
            for &v in needed_vouts {
                let vi = v as usize;
                if vi >= raw.len() {
                    continue;
                }
                if !self.outpoint_spent_for_wave_fill(&tx.txid, v, spend_index_on)? {
                    slots[vi] = Some(raw[vi].clone());
                }
            }
            return Ok(slots);
        }

        let need_n = needed_vouts.len();
        // Full run when densish: avoids N random probes for large needed sets.
        let use_full = need_n >= 8 || need_n.saturating_mul(4) >= n.max(1);
        if use_full {
            let raw = self.tx_output_run_class_a(tx)?;
            for &v in needed_vouts {
                let vi = v as usize;
                if vi >= raw.len() {
                    continue;
                }
                if !self.outpoint_spent_for_wave_fill(&tx.txid, v, spend_index_on)? {
                    slots[vi] = Some(raw[vi].clone());
                }
            }
            return Ok(slots);
        }

        // Sparse: one random read per needed vout — do **not** fill class_a with full run.
        let run = tx.output_start_fk.get().ok_or(StoreError::InvalidFk)?;
        for &v in needed_vouts {
            let vi = v as usize;
            if vi >= n {
                continue;
            }
            if self.outpoint_spent_for_wave_fill(&tx.txid, v, spend_index_on)? {
                continue;
            }
            // class_a single-slot hit first
            if let Some(o) = self.class_a_cache.get_output_at(fk, v) {
                slots[vi] = Some(o);
                continue;
            }
            let o = self.store.get_output_at(Fk(run), tx.output_count, v)?;
            slots[vi] = Some(o);
        }
        Ok(slots)
    }

    fn outpoint_spent_for_wave_fill(
        &self,
        txid: &[u8; 32],
        vout: u32,
        spend_index_on: bool,
    ) -> Result<bool, QueryError> {
        if spend_index_on {
            Ok(self.store.has_confirmed_strong_spender(txid, vout)?)
        } else {
            Ok(self
                .spent_local
                .lock()
                .unwrap()
                .contains(&(*txid, vout)))
        }
    }

    /// `(is_coinbase → create height)`: `None` = not coinbase; `Some(h)` = coinbase.
    fn coinbase_height_for_tx(
        &self,
        fk: Fk,
        tx: &TxRecord,
    ) -> Result<Option<u32>, QueryError> {
        if tx.input_count != 1 {
            return Ok(None);
        }
        let inp = self.tx_input(tx, 0)?;
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
                // Single input read (no full run) for external parents.
                return {
                    let i = self.tx_input(tx, 0)?;
                    self.coinbase_height_for_tx_with_input0(fk, tx, Some(&i))
                };
            }
        };
        let is_cb = inp.is_coinbase()
            || (inp.prev_txid == [0u8; 32]
                && inp.prev_tx_fk.is_null()
                && inp.prev_index == 0xffff_ffff);
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

    /// Tx fks for a confirmed height: `confirmed[h]` → `header_txs` list.
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
    ///
    /// Does **not** re-hash the tx to check `rec.txid` — that doubled SHA256d work on
    /// every IBD confirm reconstruct. Integrity is enforced at archive write time.
    ///
    /// **Cache path:** Class A → store only. Never probes or miss-fills
    /// [`crate::tip_prevout_cache`] — that cache is for connect prevout locality;
    /// wave-body reconstruct would only generate MISSes and thrash the tip window.
    pub fn reconstruct_tx(&self, tx_fk: Fk) -> Result<Transaction, QueryError> {
        let rec = self.get_tx_class_a(tx_fk)?;
        let stored_inputs = self.tx_input_run(&rec)?;
        let mut input = Vec::with_capacity(stored_inputs.len());
        for inp in stored_inputs {
            let prev_txid = self.resolve_prev_txid(&inp)?;
            let witness = if inp.witness.is_empty() {
                Witness::new()
            } else {
                let refs: Vec<&[u8]> = inp.witness.iter().map(|i| i.as_slice()).collect();
                Witness::from_slice(&refs)
            };
            input.push(TxIn {
                previous_output: OutPoint {
                    txid: bitcoin::Txid::from_byte_array(prev_txid),
                    vout: inp.prev_index,
                },
                script_sig: ScriptBuf::from_bytes(inp.script_sig),
                sequence: Sequence::from_consensus(inp.sequence),
                witness,
            });
        }
        let stored_outputs = self.tx_output_run_class_a(&rec)?;
        let mut output = Vec::with_capacity(stored_outputs.len());
        for out in stored_outputs {
            output.push(TxOut {
                value: Amount::from_sat(out.value as u64),
                script_pubkey: ScriptBuf::from_bytes(out.script),
            });
        }
        Ok(Transaction {
            version: TxVersion(rec.version),
            lock_time: LockTime::from_consensus(rec.locktime),
            input,
            output,
        })
    }

    /// Consensus-encoded wire bytes for a stored tx (Electrum / RPC).
    pub fn tx_wire_bytes(&self, tx_fk: Fk) -> Result<Vec<u8>, QueryError> {
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
        if tx_fks.is_empty() {
            return Err(StoreError::Corrupt("block has no transactions"));
        }
        let header = self.wire_header_from_record(&rec)?;
        let mut txdata = Vec::with_capacity(tx_fks.len());
        for fk in tx_fks {
            txdata.push(self.reconstruct_tx(fk)?);
        }
        Ok(Some(Block { header, txdata }))
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
