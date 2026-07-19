//! Class A archive write path.

use super::*;

impl Query {
    pub fn archive_block(
        &self,
        header: &HeaderRecord,
        txs: &[TxApply],
    ) -> Result<Fk, QueryError> {
        // Single-block path: one clone into owned mega-batch.
        let mut items = vec![(header.clone(), txs.to_vec())];
        let mut out = self.archive_prepared_owned(&mut items)?;
        Ok(out.pop().expect("one archive result"))
    }

    /// Archive many prepared blocks, **moving** `TxApply` payloads (no re-clone).
    ///
    /// Libbitcoin-style: plan FKs, contiguous put of txs/ins/outs, bulk hash
    /// heads. Prefer this from the IBD writer over N×[`archive_block`].
    ///
    /// `items[i].1` is drained (empty on success). Returns header fk per item.
    pub fn archive_prepared_owned(
        &self,
        items: &mut [(HeaderRecord, Vec<TxApply>)],
    ) -> Result<Vec<Fk>, QueryError> {
        if items.is_empty() {
            return Ok(Vec::new());
        }
        let mut with_fk: Vec<(Fk, HeaderRecord, Vec<TxApply>)> = Vec::with_capacity(items.len());
        for (header, txs) in items.iter_mut() {
            let fk = if let Some((fk, _)) = self.get_header_by_hash(&header.hash)? {
                fk
            } else {
                self.store.put_header(header)?
            };
            with_fk.push((fk, header.clone(), std::mem::take(txs)));
        }
        self.archive_prepared_with_fks(&mut with_fk)
    }

    /// Hot IBD path: header fk already known (from ensure_header).
    ///
    /// **Idempotent**: if a body is already stored for `header_fk`, skips the
    /// write and warms the process txid→fk cache. Multi-peer block delivery
    /// must not re-append Class A txs — that would orphan `tx_height`/`strong`
    /// on the previous fks (signet tip stuck at 2148: coinbase missing tx_height).
    pub fn archive_prepared_with_fks(
        &self,
        items: &mut [(Fk, HeaderRecord, Vec<TxApply>)],
    ) -> Result<Vec<Fk>, QueryError> {
        if items.is_empty() {
            return Ok(Vec::new());
        }
        let mut header_fks = Vec::with_capacity(items.len());
        let mut need: Vec<(Fk, Vec<TxApply>)> = Vec::with_capacity(items.len());
        // First occurrence wins inside one mega-batch (duplicate peer deliveries).
        let mut seen_headers = std::collections::HashSet::new();
        for (fk, _header, txs) in items.iter_mut() {
            header_fks.push(*fk);
            if !seen_headers.insert(*fk) {
                let _ = std::mem::take(txs);
                continue;
            }
            if self.store.header_txs.has_body(*fk)? {
                // Keep existing first_tx_fk / tx_height / strong alignment.
                self.warm_txid_cache_for_header(*fk)?;
                let _ = std::mem::take(txs);
                continue;
            }
            if !txs.is_empty() {
                need.push((*fk, std::mem::take(txs)));
            }
        }
        if !need.is_empty() {
            self.archive_bodies_mega_owned(&mut need)?;
        }
        Ok(header_fks)
    }

    /// Mega-batch Class A: one put_batch per table; I/O are **per-tx runs**.
    ///
    /// Resolves `prev_tx_fk` for inputs when the prev tx is in this batch or
    /// (if tx index on) already in the store — so disk stores compact local
    /// prevs instead of 32-byte txids.
    fn archive_bodies_mega_owned(
        &self,
        need: &mut [(Fk, Vec<TxApply>)],
    ) -> Result<(), QueryError> {
        use std::collections::HashMap;

        let mut next_tx = self.store.txs.count() + 1;
        // One run FK per non-empty I/O side (not per individual input/output).
        let mut next_in_run = self.store.inputs.count() + 1;
        let mut next_out_run = self.store.outputs.count() + 1;

        let mut all_txs: Vec<TxRecord> = Vec::new();
        let mut all_input_runs: Vec<Vec<InputRecord>> = Vec::new();
        let mut all_output_runs: Vec<Vec<OutputRecord>> = Vec::new();
        let mut per_header_ranges: Vec<(Fk, Fk, u32)> = Vec::with_capacity(need.len());
        let mut spends: Vec<([u8; 32], u32, Fk, u32)> = Vec::new();
        let spend_on = self.spend_index_enabled();
        let index_tx = self.tx_index_enabled();

        // Batch-local txid → planned fk (same-batch / same-block spends).
        let mut batch_txid_fk: HashMap<[u8; 32], Fk> = HashMap::new();
        {
            let mut plan_tx = next_tx;
            for (_h, txs) in need.iter() {
                for ta in txs.iter() {
                    batch_txid_fk.insert(ta.tx.txid, Fk(plan_tx));
                    plan_tx += 1;
                }
            }
        }

        for (header_fk, txs) in need.iter_mut() {
            if txs.is_empty() {
                continue;
            }
            let first_tx_fk = Fk(next_tx);
            let n_txs = txs.len() as u32;
            for ta in txs.drain(..) {
                let n_in = ta.inputs.len() as u32;
                let n_out = ta.outputs.len() as u32;
                let tx_fk = Fk(next_tx);
                next_tx += 1;
                let in_start = if n_in == 0 {
                    Fk::NULL
                } else {
                    let fk = Fk(next_in_run);
                    next_in_run += 1;
                    fk
                };
                let out_start = if n_out == 0 {
                    Fk::NULL
                } else {
                    let fk = Fk(next_out_run);
                    next_out_run += 1;
                    fk
                };

                let mut tx = ta.tx;
                tx.input_start_fk = in_start;
                tx.input_count = n_in;
                tx.output_start_fk = out_start;
                tx.output_count = n_out;
                all_txs.push(tx);

                if n_in > 0 {
                    let mut inputs = ta.inputs;
                    for (i, inp) in inputs.iter_mut().enumerate() {
                        if !inp.is_coinbase() {
                            // Prefer batch-local fk, then process cache / durable head.
                            // Must stamp prev_tx_fk even when tx.head is off so tip
                            // confirm can resolve prevouts without MissingPrevout.
                            if let Some(&fk) = batch_txid_fk.get(&inp.prev_txid) {
                                inp.prev_tx_fk = fk;
                            } else if let Some(fk) = self.lookup_tx_fk(&inp.prev_txid)? {
                                inp.prev_tx_fk = fk;
                            }
                            if spend_on {
                                spends.push((inp.prev_txid, inp.prev_index, tx_fk, i as u32));
                            }
                        }
                    }
                    all_input_runs.push(inputs);
                }

                if n_out > 0 {
                    all_output_runs.push(ta.outputs);
                }
            }
            per_header_ranges.push((*header_fk, first_tx_fk, n_txs));
        }

        let tx_est: u64 = (all_txs.len() * TxRecord::ENCODED_LEN) as u64;
        let in_est: u64 = all_input_runs
            .iter()
            .map(|r| r.iter().map(|x| x.encoded_len() as u64).sum::<u64>())
            .sum();
        let out_est: u64 = all_output_runs
            .iter()
            .map(|r| r.iter().map(|x| x.encoded_len() as u64).sum::<u64>())
            .sum();
        self.store
            .txs
            .reserve_append(tx_est, all_txs.len() as u64)?;
        self.store
            .inputs
            .reserve_append(in_est, all_input_runs.len() as u64)?;
        self.store
            .outputs
            .reserve_append(out_est, all_output_runs.len() as u64)?;

        let in_refs: Vec<&[InputRecord]> = all_input_runs.iter().map(|r| r.as_slice()).collect();
        let out_refs: Vec<&[OutputRecord]> =
            all_output_runs.iter().map(|r| r.as_slice()).collect();
        let (tx_res, in_res, out_res) = std::thread::scope(|scope| {
            let tx_h = scope.spawn(|| self.store.txs.put_batch_indexed(&all_txs, index_tx));
            let in_h = scope.spawn(|| self.store.inputs.put_runs(&in_refs));
            let out_h = scope.spawn(|| self.store.outputs.put_runs(&out_refs));
            (
                tx_h.join().expect("txs put"),
                in_h.join().expect("inputs put"),
                out_h.join().expect("outputs put"),
            )
        });
        let got_tx_fks = tx_res?;
        in_res?;
        out_res?;
        if got_tx_fks.len() != all_txs.len() {
            return Err(StoreError::Corrupt("tx put_batch length"));
        }
        if let Some(&(_, first_tx, _)) = per_header_ranges.first() {
            if got_tx_fks.first().copied() != Some(first_tx) {
                return Err(StoreError::Corrupt("tx put_batch fk mismatch"));
            }
        }
        // Always remember txid→fk (even when durable head is off).
        //
        // Class A cache: only bulk-fill from archive when we are tip-following
        // (small archive lead). Under IBD with a large lead, archive-newest FIFO
        // thrash fights confirm — tip_prevout_cache + miss-fill own that path.
        let fill_class_a = self.should_fill_class_a_from_archive();
        {
            let mut in_i = 0usize;
            let mut out_i = 0usize;
            for (rec, fk) in all_txs.iter().zip(got_tx_fks.iter()) {
                self.remember_txid(rec.txid, *fk);
                let inputs = if rec.input_count > 0 {
                    let v = all_input_runs[in_i].clone();
                    in_i += 1;
                    Some(v)
                } else {
                    None
                };
                let outputs = if rec.output_count > 0 {
                    let v = all_output_runs[out_i].clone();
                    out_i += 1;
                    Some(v)
                } else {
                    None
                };
                if fill_class_a {
                    self.class_a_cache
                        .note(*fk, rec.clone(), outputs, inputs);
                }
            }
        }

        if spend_on && !spends.is_empty() {
            // One body write + batched head inserts (not N× put_spend probes).
            self.store.put_spend_batch(&spends)?;
        }

        self.store.header_txs.put_ranges_batch(&per_header_ranges)?;
        Ok(())
    }

    /// Resolve prev outpoint txid for an input (local fk or stored external hash).
    ///
    /// Uses Class A → store only (not tip_prevout): we only need the parent
    /// **txid** field, not prevout outputs. Reconstruct and Class C spend edges
    /// call this heavily; probing tip_prevout would only generate MISS noise.
    pub fn resolve_prev_txid(&self, inp: &InputRecord) -> Result<[u8; 32], QueryError> {
        if inp.is_coinbase() {
            return Ok([0u8; 32]);
        }
        if let Some(fk) = inp.prev_tx_fk.get() {
            return Ok(self.get_tx_class_a(Fk(fk))?.txid);
        }
        Ok(inp.prev_txid)
    }

    /// Confirm an already-archived block at `height` (genesis or tip+1).
    ///
    /// Always: Class C strong + confirmed, **tx_height**, and **point spends** for
    pub fn set_archive_mode(&self, enabled: bool) -> Result<(), QueryError> {
        self.store.set_archive_mode(enabled)
    }

    pub fn finalize_through(&self, height: u32) -> Result<(), QueryError> {
        self.store.finalize_through(height)
    }

    pub fn archive_epoch(&self) -> rbitcoin_store::ArchiveEpoch {
        self.store.epoch()
    }

}
