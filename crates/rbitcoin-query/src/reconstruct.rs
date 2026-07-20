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
    ///
    /// Cold-start correctness (vs earlier wave_fill experiments):
    /// - Tip short-circuit only when **every** needed vout is already live in tip
    /// - External parents are **sorted by fk** and bulk-warmed (tx + outs) then
    ///   spent-filtered (**local then durable**); missing unspent outs re-load
    ///   from store (no holes)
    /// - Live slots are promoted into tip_prevout for the next wave only after
    ///   spent filter (merge never clears existing lives; refuses txid mismatch)
    ///
    /// Sub-timers: [`crate::wave_fill_stats`].
    ///
    /// Returns `(entries_loaded, wave_map)`.
    pub fn prefetch_tip_prevouts_for_block_hashes(
        &self,
        hashes: &[[u8; 32]],
    ) -> Result<(usize, crate::WavePrevoutCache), QueryError> {
        use crate::wave_fill_stats::{self as wf, add as wf_add};
        use crate::wave_prevout::ThinInput;
        use std::collections::{HashMap, HashSet};
        use std::time::Instant;

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
        let t_body = Instant::now();
        let mut parent_needed: HashMap<u64, HashSet<u32>> = HashMap::new();
        for &fk in &wave_tx_fks {
            let (tx, outs, inputs) = self.wave_body_from_class_a(fk)?;
            // Parent-as-create for same-run spends. One TxRecord move into
            // parents (get_tx falls back to parents; skip redundant insert_tx).
            let cb_h = self.coinbase_height_for_tx_with_input0(fk, &tx, inputs.first())?;
            wave.insert_parent_live(fk, tx, outs, Some(cb_h));
            noted += 1;

            if inputs.is_empty() {
                continue;
            }
            let mut edges = Vec::with_capacity(inputs.len());
            for inp in &inputs {
                if inp.is_coinbase() {
                    edges.push(ThinInput {
                        create_fk: None,
                        prev_index: inp.prev_index,
                    });
                    continue;
                }
                // Resolve create fk: same-wave body → light UTXO → durable head/runs.
                let create_fk = wave
                    .get_by_txid(&inp.prev_txid, inp.prev_index)
                    .map(|(pfk, _, _)| pfk)
                    .or(self
                        .ibd_utxo_create_fk(&inp.prev_txid, inp.prev_index)
                        .ok()
                        .flatten())
                    .or(self.tx_fk_by_txid(&inp.prev_txid).ok().flatten());
                edges.push(ThinInput {
                    create_fk: create_fk.and_then(|f| f.get()),
                    prev_index: inp.prev_index,
                });
                let Some(pfk) = create_fk else {
                    continue;
                };
                let pid = match pfk.get() {
                    Some(id) => id,
                    None => continue,
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
        wf_add(&wf::BODY_NS, t_body.elapsed().as_nanos() as u64);

        // Pass 3: external parents, sorted by fk for Class A / body locality.
        // Spent checks are deferred and sorted by point.head key for shard locality
        // (mid-IBD logs: spent_ms ≫ parent_tx/out).
        let mut parents: Vec<(u64, HashSet<u32>)> = parent_needed.into_iter().collect();
        parents.sort_unstable_by_key(|(pid, _)| *pid);
        let spend_index_on = self.spend_index_enabled();

        struct ParentWork {
            fk: Fk,
            tx: TxRecord,
            /// Slots partially filled (tip-live / pending spent probe).
            slots: Vec<Option<OutputRecord>>,
            outs_map: HashMap<u32, OutputRecord>,
            /// Vouts that still need durable / local spent filter.
            need_spent: Vec<u32>,
        }
        let mut works: Vec<ParentWork> = Vec::with_capacity(parents.len());

        for (pid, needed_vouts) in &parents {
            let fk = Fk(*pid);

            // Tip short-circuit only when **complete** (all needed live) and none
            // are catch-up spent (UTXO miss).
            let tip_complete = !needed_vouts.is_empty()
                && needed_vouts
                    .iter()
                    .all(|&v| self.tip_prevout_cache.has_live_output(fk, v));
            if tip_complete {
                if let Some(tx) = self.tip_prevout_cache.get_tx(fk) {
                    let mut blocked = false;
                    for &v in needed_vouts {
                        if self.catchup_is_spent(&tx.txid, v)? {
                            blocked = true;
                            break;
                        }
                    }
                    if !blocked {
                        let n = tx.output_count as usize;
                        let mut slots: Vec<Option<OutputRecord>> = vec![None; n];
                        let mut ok = true;
                        for &v in needed_vouts {
                            match self.tip_prevout_cache.get_output_at(fk, v) {
                                Some(o) if (v as usize) < slots.len() => {
                                    slots[v as usize] = Some(o);
                                }
                                _ => {
                                    ok = false;
                                    break;
                                }
                            }
                        }
                        if ok {
                            let t_cb = Instant::now();
                            let cb_h = self.coinbase_height_for_tx(fk, &tx)?;
                            wf_add(&wf::CB_HEIGHT_NS, t_cb.elapsed().as_nanos() as u64);
                            wave.insert_parent_slots(fk, tx, slots, Some(cb_h));
                            noted += 1;
                            continue;
                        }
                    }
                }
            }

            // Bulk warm: tx then outs (sorted parent loop ⇒ sequential-ish store).
            let t_tx = Instant::now();
            let tx = self.get_tx_class_a(fk)?;
            wf_add(&wf::PARENT_TX_NS, t_tx.elapsed().as_nanos() as u64);

            let t_out = Instant::now();
            let mut outs_map: HashMap<u32, OutputRecord> =
                HashMap::with_capacity(needed_vouts.len());
            let n = tx.output_count as usize;
            let need_n = needed_vouts.len();
            // Prefer full run when Class A already holds it, for small txs, or
            // when we need a non-trivial fraction of outs (sparse random I/O loses).
            if n > 0 {
                if let Some(raw) = self.class_a_cache.get_outputs(fk) {
                    for &v in needed_vouts {
                        let vi = v as usize;
                        if vi < raw.len() {
                            outs_map.insert(v, raw[vi].clone());
                        }
                    }
                } else {
                    let use_full = need_n >= 2
                        || n <= 128
                        || need_n.saturating_mul(3) >= n.max(1);
                    if use_full {
                        let raw = self.tx_output_run_class_a(fk, &tx)?;
                        for &v in needed_vouts {
                            let vi = v as usize;
                            if vi < raw.len() {
                                outs_map.insert(v, raw[vi].clone());
                            }
                        }
                    } else {
                        let run = tx.output_start_fk.get().ok_or(StoreError::InvalidFk)?;
                        let mut vouts: Vec<u32> = needed_vouts.iter().copied().collect();
                        vouts.sort_unstable();
                        for v in vouts {
                            if (v as usize) >= n {
                                continue;
                            }
                            if let Some(o) = self.class_a_cache.get_output_at(fk, v) {
                                outs_map.insert(v, o);
                                continue;
                            }
                            let o = self.store.get_output_at(Fk(run), tx.output_count, v)?;
                            outs_map.insert(v, o);
                        }
                    }
                }
            }
            wf_add(&wf::PARENT_OUT_NS, t_out.elapsed().as_nanos() as u64);

            // Partial tip cover: live tip slots skip durable spent (write-through)
            // unless catch-up oracle already marks spent.
            let mut slots: Vec<Option<OutputRecord>> = vec![None; n];
            let mut need_spent = Vec::with_capacity(needed_vouts.len());
            for &v in needed_vouts {
                let vi = v as usize;
                if vi >= n {
                    continue;
                }
                if self.catchup_is_spent(&tx.txid, v)? {
                    // Spent — leave slot None; no durable probe.
                    continue;
                }
                if self.tip_prevout_cache.has_live_output(fk, v) {
                    if let Some(o) = self.tip_prevout_cache.get_output_at(fk, v) {
                        slots[vi] = Some(o);
                    } else if let Some(o) = outs_map.remove(&v) {
                        slots[vi] = Some(o);
                    }
                    continue;
                }
                need_spent.push(v);
            }

            works.push(ParentWork {
                fk,
                tx,
                slots,
                outs_map,
                need_spent,
            });
        }

        // Spent filter for remaining need_spent.
        // Catch-up path already dropped UTXO-spent vouts above — no second probe.
        let t_sp_all = Instant::now();
        let mut spent_flags: HashMap<(usize, u32), bool> =
            HashMap::with_capacity(works.iter().map(|w| w.need_spent.len()).sum());
        let mut durable_probes: Vec<(usize, u32, [u8; 32])> = Vec::new();
        for (wi, w) in works.iter().enumerate() {
            for &v in &w.need_spent {
                if spend_index_on {
                    let key = rbitcoin_store::PointRecord::outpoint_key(&w.tx.txid, v);
                    durable_probes.push((wi, v, key));
                } else {
                    spent_flags.insert((wi, v), false);
                }
            }
        }
        if !durable_probes.is_empty() {
            durable_probes.sort_unstable_by(|a, b| a.2.cmp(&b.2));
            let tip = self.store.confirmed.tip_height().map(|t| t.0);
            for &(wi, v, ref key) in &durable_probes {
                let spent = self.store.has_confirmed_strong_spender_key(key, tip)?;
                spent_flags.insert((wi, v), spent);
            }
        }
        wf_add(&wf::SPENT_NS, t_sp_all.elapsed().as_nanos() as u64);

        // Assemble live slots, promote, insert wave.
        for (wi, mut w) in works.into_iter().enumerate() {
            for &v in &w.need_spent {
                let vi = v as usize;
                if vi >= w.slots.len() {
                    continue;
                }
                if spent_flags.get(&(wi, v)).copied().unwrap_or(true) {
                    continue;
                }
                if let Some(o) = w.outs_map.remove(&v) {
                    w.slots[vi] = Some(o);
                    continue;
                }
                let run = w.tx.output_start_fk.get().ok_or(StoreError::InvalidFk)?;
                let o = self
                    .store
                    .get_output_at(Fk(run), w.tx.output_count, v)?;
                w.slots[vi] = Some(o);
            }

            let t_cb = Instant::now();
            let cb_h = self.coinbase_height_for_tx(w.fk, &w.tx)?;
            wf_add(&wf::CB_HEIGHT_NS, t_cb.elapsed().as_nanos() as u64);

            let t_tip = Instant::now();
            self.tip_prevout_cache
                .note_live_slots(w.fk, w.tx.clone(), &w.slots);
            // Pin live prevouts until the spending input is Class-C confirmed.
            for (v, slot) in w.slots.iter().enumerate() {
                if slot.is_some() {
                    self.class_a_cache.pin_out(w.fk, v as u32);
                }
            }
            wf_add(&wf::TIP_NOTE_NS, t_tip.elapsed().as_nanos() as u64);

            wave.insert_parent_slots(w.fk, w.tx, w.slots, Some(cb_h));
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
        // Fast path: one Class A mutex, full body.
        if let Some(parts) = self.class_a_cache.get_reconstruct_parts(fk) {
            return Ok(parts);
        }
        // Cold fallback: one store load, fill class_a, return owned.
        let tx = self.get_tx_class_a(fk)?;
        let outs = if tx.output_count == 0 {
            Vec::new()
        } else {
            self.tx_output_run_class_a(fk, &tx)?
        };
        let inputs = if tx.input_count == 0 {
            Vec::new()
        } else {
            self.tx_input_run(&tx)?
        };
        Ok((tx, outs, inputs))
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
        if let Some((rec, outs, inputs)) = self.class_a_cache.get_reconstruct_parts(tx_fk) {
            return Ok(Self::transaction_from_class_a(rec, outs, inputs));
        }
        let rec = self.get_tx_class_a(tx_fk)?;
        let stored_inputs = self.tx_input_run(&rec)?;
        let stored_outputs = self.tx_output_run_class_a(tx_fk, &rec)?;
        Ok(Self::transaction_from_class_a(
            rec,
            stored_outputs,
            stored_inputs,
        ))
    }

    /// Build a wire [`Transaction`] from Class A rows (moves script bytes).
    fn transaction_from_class_a(
        rec: TxRecord,
        stored_outputs: Vec<OutputRecord>,
        stored_inputs: Vec<InputRecord>,
    ) -> Transaction {
        let mut input = Vec::with_capacity(stored_inputs.len());
        for inp in stored_inputs {
            // External prev only on Class A (LOCAL_PREV rejected at decode).
            let prev_txid = inp.prev_txid;
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
