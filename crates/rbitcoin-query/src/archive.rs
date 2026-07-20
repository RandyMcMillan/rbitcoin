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

    /// Mega-batch Class A: **packed full-tx** rows (one `tx.body` payload per
    /// tx → single IO on `get_tx_full`). Split input/output tables are no longer
    /// written for new archives (legacy rows remain readable).
    ///
    /// Non-coinbase inputs always store external `prev_txid` + vout (no Class A
    /// `prev_tx_fk`). Confirm uses light UTXO create_fk; tip mode uses points/head.
    fn archive_bodies_mega_owned(
        &self,
        need: &mut [(Fk, Vec<TxApply>)],
    ) -> Result<(), QueryError> {
        let mut next_tx = self.store.txs.count() + 1;

        let mut packed: Vec<(TxRecord, Vec<InputRecord>, Vec<OutputRecord>)> = Vec::new();
        let mut per_header_ranges: Vec<(Fk, Fk, u32)> = Vec::with_capacity(need.len());
        let mut spends: Vec<([u8; 32], u32, Fk, u32)> = Vec::new();
        // (out_txid, out_idx, spend_fk, in_idx, height) for point runs
        let mut run_spends: Vec<([u8; 32], u32, Fk, u32, u32)> = Vec::new();
        let spend_on = self.spend_index_enabled();
        let point_runs = self.point_run_enabled();
        let index_tx = self.tx_index_enabled();
        let tx_runs = self.tx_run_enabled();

        for (header_fk, txs) in need.iter_mut() {
            if txs.is_empty() {
                continue;
            }
            // Height optional in point runs (0 if unknown); rebuild uses confirmed walk.
            let hdr_height = 0u32;
            let first_tx_fk = Fk(next_tx);
            let n_txs = txs.len() as u32;
            for ta in txs.drain(..) {
                let n_in = ta.inputs.len() as u32;
                let n_out = ta.outputs.len() as u32;
                let tx_fk = Fk(next_tx);
                next_tx += 1;

                let mut tx = ta.tx;
                // Packed rows are self-contained; I/O fks stay NULL.
                tx.input_start_fk = Fk::NULL;
                tx.input_count = n_in;
                tx.output_start_fk = Fk::NULL;
                tx.output_count = n_out;

                let mut inputs = ta.inputs;
                for (i, inp) in inputs.iter_mut().enumerate() {
                    if !inp.is_coinbase() {
                        if spend_on {
                            spends.push((inp.prev_txid, inp.prev_index, tx_fk, i as u32));
                        } else if point_runs {
                            run_spends.push((
                                inp.prev_txid,
                                inp.prev_index,
                                tx_fk,
                                i as u32,
                                hdr_height,
                            ));
                        }
                    }
                }
                packed.push((tx, inputs, ta.outputs));
            }
            per_header_ranges.push((*header_fk, first_tx_fk, n_txs));
        }

        let body_est: u64 = packed
            .iter()
            .map(|(_tx, ins, outs)| {
                (1 + TxRecord::ENCODED_LEN) as u64
                    + ins.iter().map(|x| x.encoded_len() as u64).sum::<u64>()
                    + outs.iter().map(|x| x.encoded_len() as u64).sum::<u64>()
            })
            .sum();
        self.store
            .txs
            .reserve_append(body_est, packed.len() as u64)?;

        let got_tx_fks = self.store.put_tx_full_batch_indexed(&packed, index_tx)?;
        if got_tx_fks.len() != packed.len() {
            return Err(StoreError::Corrupt("tx put_full_batch length"));
        }
        if let Some(&(_, first_tx, _)) = per_header_ranges.first() {
            if got_tx_fks.first().copied() != Some(first_tx) {
                return Err(StoreError::Corrupt("tx put_full_batch fk mismatch"));
            }
        }
        // No generic Class A cache: confirm parents live in ConfirmParentCache
        // (prewarm). Archive only enqueues catch-up index runs.
        for ((rec, _, _), fk) in packed.iter().zip(got_tx_fks.iter()) {
            if tx_runs {
                self.enqueue_tx_run(rec.txid, *fk);
            }
        }

        if spend_on && !spends.is_empty() {
            // One body write + batched head inserts (not N× put_spend probes).
            self.store.put_spend_batch(&spends)?;
        } else if point_runs && !run_spends.is_empty() {
            self.enqueue_point_run_edges(&run_spends);
        }

        self.store.header_txs.put_ranges_batch(&per_header_ranges)?;
        Ok(())
    }

    /// Resolve prev outpoint txid for an input (local fk or stored external hash).
    ///
    /// Parent **txid** only (not prevout outs). Reconstruct / Class C spend edges.
    pub fn resolve_prev_txid(&self, inp: &InputRecord) -> Result<[u8; 32], QueryError> {
        if inp.is_coinbase() {
            return Ok([0u8; 32]);
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
