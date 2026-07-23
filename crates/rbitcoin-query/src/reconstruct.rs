//! Reconstruct wire blocks/txs and merkle proofs; confirm wave fill.

use super::*;
use std::collections::{HashMap, HashSet};
use std::time::Instant;

impl Query {
    /// Build wave prevout map. **Requires** load: bodies + parents ready.
    ///
    /// Prefer [`Self::wave_fill_for_tx_fk_lists`] when header + tx lists are
    /// already resolved (confirm batch) to avoid re-probing header head.
    pub fn wave_fill_for_block_hashes(
        &self,
        hashes: &[[u8; 32]],
    ) -> Result<(usize, crate::WavePrevoutCache), QueryError> {
        let mut lists: Vec<Vec<Fk>> = Vec::with_capacity(hashes.len());
        for hash in hashes {
            let Some((header_fk, _)) = self.get_header_by_hash(hash)? else {
                continue;
            };
            let Some(tx_fks) = self.header_tx_fks(header_fk, Some(hash))? else {
                continue;
            };
            lists.push(tx_fks);
        }
        let refs: Vec<&[Fk]> = lists.iter().map(|v| v.as_slice()).collect();
        self.wave_fill_for_tx_fk_lists(&refs)
    }

    /// Wave fill from already-resolved per-block Class A fk lists (confirm hot path).
    ///
    /// - Wave-body txs: **move** load-stage bodies out of the parent cache (no clone);
    ///   store decode only on cache miss (should be rare after load).
    /// - Thin edges: batch-moved from load stash — no remap.
    /// - External parents: prefer sparse pin from load; store outs-only fallback.
    pub fn wave_fill_for_tx_fk_lists(
        &self,
        per_block: &[&[Fk]],
    ) -> Result<(usize, crate::WavePrevoutCache), QueryError> {
        use crate::wave_fill_stats::{self as wf, add as wf_add, add_count as wf_count};

        // Prefer parent cache; **store-fallback on miss** (2-stage). Hard
        // cache-only was pipeline-era extra work that re-queued batches instead
        // of finishing the same Class A load the old path did inline.
        let mut wave_fks: HashSet<u64> = HashSet::new();
        let mut wave_tx_fks: Vec<Fk> = Vec::new();
        for list in per_block {
            for &fk in *list {
                if let Some(id) = fk.get() {
                    wave_fks.insert(id);
                }
                wave_tx_fks.push(fk);
            }
        }

        let mut wave =
            crate::WavePrevoutCache::with_capacity(wave_tx_fks.len(), wave_tx_fks.len());
        let mut noted = 0usize;

        // Pass 2: move bodies from cache (no clone — 2-stage hot path).
        let t_body = Instant::now();
        let mut taken_bodies = self.confirm_parents.take_bodies_batch(&wave_tx_fks);
        let mut taken_thin = self.confirm_parents.take_thin_inputs_batch(&wave_tx_fks);
        // parent_fk → needed vouts (small lists; sort/dedup later).
        let mut parent_needed: HashMap<u64, Vec<u32>> = HashMap::new();
        let mut n_cache = 0u64;
        let mut n_store = 0u64;
        let mut n_thin_move = 0u64;
        let mut n_thin_rebuild = 0u64;
        for &fk in &wave_tx_fks {
            let id = fk.get();
            let (tx, outs, inputs) = if let Some(id) = id {
                if let Some(parts) = taken_bodies.remove(&id) {
                    n_cache = n_cache.saturating_add(1);
                    parts
                } else {
                    n_store = n_store.saturating_add(1);
                    self.load_body_from_store(fk)?
                }
            } else {
                n_store = n_store.saturating_add(1);
                self.load_body_from_store(fk)?
            };
            let cb_h: Option<Option<u32>> =
                Some(self.coinbase_height_for_tx_with_input0(fk, &tx, inputs.first())?);

            // Thin edges before moving outs/inputs into wave.
            let edges = if let Some(id) = id {
                if let Some(stashed) = taken_thin.remove(&id) {
                    n_thin_move = n_thin_move.saturating_add(1);
                    stashed
                } else if inputs.is_empty() {
                    Vec::new()
                } else {
                    n_thin_rebuild = n_thin_rebuild.saturating_add(1);
                    self.thin_edges_from_inputs(&inputs, &wave)?
                }
            } else if inputs.is_empty() {
                Vec::new()
            } else {
                n_thin_rebuild = n_thin_rebuild.saturating_add(1);
                self.thin_edges_from_inputs(&inputs, &wave)?
            };

            for e in &edges {
                let Some(pid) = e.create_fk else {
                    continue;
                };
                if wave_fks.contains(&pid) {
                    continue;
                }
                parent_needed.entry(pid).or_default().push(e.prev_index);
            }
            wave.insert_thin_inputs(fk, edges);

            // Parent + body_wire share one Arc of outs (no outs.clone() at fill).
            wave.insert_wave_body(fk, tx, outs, inputs, cb_h);
            noted += 1;
        }
        wf_add(&wf::BODY_NS, t_body.elapsed().as_nanos() as u64);
        wf_count(&wf::BODY_CACHE_MOVE, n_cache);
        wf_count(&wf::BODY_STORE, n_store);
        wf_count(&wf::THIN_CACHE_MOVE, n_thin_move);
        wf_count(&wf::THIN_REBUILD, n_thin_rebuild);
        // Drop leftover maps early (should be empty when cache complete).
        drop(taken_bodies);
        drop(taken_thin);

        // Pass 3: external parents — only needed vouts, sparse map.
        let mut parents: Vec<(u64, Vec<u32>)> = parent_needed.into_iter().collect();
        parents.sort_unstable_by_key(|(pid, _)| *pid);
        for (_, vouts) in &mut parents {
            vouts.sort_unstable();
            vouts.dedup();
        }

        for (pid, needed_vouts) in &parents {
            let fk = Fk(*pid);
            let t_par = Instant::now();
            // Prefer parent pin; store-fallback like 2-stage.
            let (tx, mut candidates, spent_filtered) =
                match self.confirm_parents.get_parent_outs_needed(fk, needed_vouts) {
                    Some((tx, live, filtered)) => (tx, live, filtered),
                    None => self.load_parent_needed_outs(fk, needed_vouts)?,
                };
            wf_add(&wf::PARENT_TX_NS, t_par.elapsed().as_nanos() as u64);

            let n_out = tx.output_count;
            let t_spent = Instant::now();
            let live: Vec<(u32, OutputRecord)> = if spent_filtered {
                candidates
            } else {
                let range = self.confirm_parents.get_body_range(fk);
                let unspent: HashSet<u32> = self
                    .store
                    .unspent_create_vouts(fk, needed_vouts, range)?
                    .into_iter()
                    .collect();
                candidates
                    .drain(..)
                    .filter(|(v, _)| unspent.contains(v))
                    .collect()
            };
            wf_add(&wf::SPENT_NS, t_spent.elapsed().as_nanos() as u64);

            let t_cb = Instant::now();
            let cb = if let Some(resolved) =
                self.confirm_parents.get_parent_coinbase_height(fk)
            {
                Some(resolved)
            } else {
                Some(self.resolve_parent_coinbase_height(
                    fk,
                    tx.input_count,
                    self.confirm_parents.get_body_range(fk),
                )?)
            };
            wf_add(&wf::CB_HEIGHT_NS, t_cb.elapsed().as_nanos() as u64);

            wave.insert_parent_sparse(fk, tx, n_out, live, cb);
            noted += 1;
        }

        Ok((noted, wave))
    }

    /// Resolve create maturity: `None` = not coinbase, `Some(h)` = coinbase height.
    ///
    /// Only true coinbases (1-in + null prev), not every 1-in tx. Prevout scan
    /// skips script/witness. Runway stashes this so wave rarely calls here.
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

    /// External parent: only the needed vouts (no full dense outs / no inputs).
    ///
    /// Third tuple field: `spent_filtered` — cache already dropped spent outs.
    fn load_parent_needed_outs(
        &self,
        fk: Fk,
        needed: &[u32],
    ) -> Result<(TxRecord, Vec<(u32, OutputRecord)>, bool), QueryError> {
        // Sparse by_fk / body subset under one lock — clones only requested vouts.
        if let Some((tx, live, filtered)) =
            self.confirm_parents.get_parent_outs_needed(fk, needed)
        {
            return Ok((tx, live, filtered));
        }
        // Store outs-only decode (skip parent input/witness alloc). Clone only
        // the few needed vouts (typically 1–2); drop the rest with `outs`.
        let (tx, outs) = if let Some((off, len)) = self.confirm_parents.get_body_range(fk) {
            self.store.get_tx_meta_and_outputs_at(off, len)?
        } else {
            self.store.get_tx_meta_and_outputs(fk)?
        };
        let mut live = Vec::with_capacity(needed.len());
        for &v in needed {
            if let Some(o) = outs.get(v as usize) {
                live.push((v, o.clone()));
            }
        }
        Ok((tx, live, false))
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
            // v10: prefer stamped create_fk (no head / by_txid).
            if !inp.create_fk.is_null() {
                edges.push(ThinInput {
                    create_fk: inp.create_fk.get(),
                    prev_index: inp.prev_index,
                });
                continue;
            }
            let create_fk = wave
                .get_by_txid(&inp.prev_txid, inp.prev_index)
                .map(|(pfk, _, _)| pfk)
                .or_else(|| self.confirm_parents.get_by_txid(&inp.prev_txid))
                .or(self.tx_fk_by_txid(&inp.prev_txid).ok().flatten());
            edges.push(ThinInput {
                create_fk: create_fk.and_then(|f| f.get()),
                prev_index: inp.prev_index,
            });
        }
        Ok(edges)
    }

    /// Body for RPC/Electrum reconstruct: clone from cache if present, else store.
    ///
    /// Confirm wave fill uses [`ConfirmParentCache::take_bodies_batch`] (move-out)
    /// instead — do not call this on the confirm hot path.
    fn load_body_from_cache(
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
        use crate::wave_fill_stats::{self as wf, add as wf_add};
        let t0 = Instant::now();
        let maj_before = thread_majflt();
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
        let (rec, stored_outputs, mut stored_inputs) = self.load_body_from_cache(tx_fk)?;
        let mut cache = HashMap::new();
        self.fill_input_prev_txids_cached(&mut stored_inputs, None, &mut cache, false)?;
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
    /// Schema v10 stamps create_fk and leaves soft prev_txid zero on disk. Wire
    /// rebuild used to call [`Store::body_txid`] once per input (~thousands of
    /// body reads/block). Prefer, in order:
    /// 1. already-filled soft prev_txid
    /// 2. wave parent / body_wire TxRecord (confirm hot path — already decoded)
    /// 3. confirm parent cache (cache sparse / cache body)
    /// 4. store `body_txid` (deduped via `cache` across a block)
    pub(crate) fn fill_input_prev_txids_cached(
        &self,
        inputs: &mut [InputRecord],
        wave: Option<&crate::WavePrevoutCache>,
        cache: &mut HashMap<u64, [u8; 32]>,
        _cache_only: bool,
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
            let from_ram = if let Some(w) = wave {
                w.get_tx(Fk(id)).map(|t| t.txid)
            } else {
                None
            }
            .or_else(|| self.confirm_parents.get_parent_txid(Fk(id)));
            let txid = match from_ram {
                Some(t) => t,
                // Unique create: one body prefix read, then reused via `cache`.
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
        self.reconstruct_archived_block_from_parts_wave(rec, tx_fks, None, None)
    }

    /// Like [`Self::reconstruct_archived_block_from_parts`] but reuses wave-fill
    /// body decodes (one Class A parse per wave-body tx for the whole confirm run).
    ///
    /// `prev_hash`: when set (load header plan), wire header needs no store IO.
    /// Body misses fall back to store decode.
    pub fn reconstruct_archived_block_from_parts_wave(
        &self,
        rec: HeaderRecord,
        tx_fks: Vec<Fk>,
        mut wave: Option<&mut crate::WavePrevoutCache>,
        prev_hash: Option<[u8; 32]>,
    ) -> Result<Block, QueryError> {
        if tx_fks.is_empty() {
            return Err(StoreError::Corrupt("block has no transactions"));
        }
        let header = self.wire_header_from_record_prev(&rec, prev_hash)?;
        let mut txdata = Vec::with_capacity(tx_fks.len());
        // Dedup create_fk → txid across the whole block (and prefer wave parents).
        let mut prev_txid_cache: HashMap<u64, [u8; 32]> = HashMap::new();
        for fk in tx_fks {
            if let Some(w) = wave.as_deref_mut() {
                if let Some((tx, outs, mut ins)) = w.take_body_wire(fk) {
                    self.fill_input_prev_txids_cached(
                        &mut ins,
                        Some(w),
                        &mut prev_txid_cache,
                        false,
                    )?;
                    txdata.push(Self::transaction_from_class_a(tx, outs, ins));
                    continue;
                }
            }
            // No wave body: store-fallback (2-stage).
            let (rec_tx, stored_outputs, mut stored_inputs) = self.load_body_from_cache(fk)?;
            self.fill_input_prev_txids_cached(
                &mut stored_inputs,
                wave.as_deref(),
                &mut prev_txid_cache,
                false,
            )?;
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
