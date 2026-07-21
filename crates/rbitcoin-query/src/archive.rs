//! Class A archive write path.

use super::*;

/// When Class A high-water is this many blocks ahead of the prewarm watermark,
/// drop just-written `tx.body` pages from the page cache so archive dirty pages
/// do not crowd out confirm/prewarm. Below this, keep pages (prewarm may need them).
const ARCHIVE_BODY_DONTNEED_LEAD: u32 = 1024;

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
        // Tip: archive writes durable spends. Direct: confirm batch-writes after Class C.
        // Catchup: no durable spends (point runs).
        let archive_spends =
            self.spend_index_enabled() && self.index_mode().is_tip();
        let index_tx = self.tx_index_enabled();
        let tx_runs = self.tx_run_enabled();

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

                let mut tx = ta.tx;
                // Packed rows are self-contained; I/O fks stay NULL.
                tx.input_start_fk = Fk::NULL;
                tx.input_count = n_in;
                tx.output_start_fk = Fk::NULL;
                tx.output_count = n_out;

                let mut inputs = ta.inputs;
                for (i, inp) in inputs.iter_mut().enumerate() {
                    if !inp.is_coinbase() && archive_spends {
                        // Tip mode: durable spend on output (resolve create via tx.head).
                        spends.push((inp.prev_txid, inp.prev_index, tx_fk, i as u32));
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

        let body_off = self.store.txs.body_logical_len();
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

        if archive_spends && !spends.is_empty() {
            // Tip: annotate create outputs (tx.head resolve per prevout).
            self.store.put_spend_batch(&spends)?;
        }

        self.store.header_txs.put_ranges_batch(&per_header_ranges)?;

        // Far archive lead: free just-written body pages so they do not sit in
        // the page cache while prewarm/confirm work near tip.
        let body_end = self.store.txs.body_logical_len();
        let body_len = body_end.saturating_sub(body_off);
        if body_len > 0 && self.archive_far_ahead_of_prewarm()? {
            self.store.txs.advise_body_dont_need(body_off, body_len);
        }
        Ok(())
    }

    /// True when Class A high-water is more than [`ARCHIVE_BODY_DONTNEED_LEAD`]
    /// blocks ahead of the prewarm ready watermark (or tip if prewarm idle).
    fn archive_far_ahead_of_prewarm(&self) -> Result<bool, QueryError> {
        let bodies = self.store.header_txs.count_bodies()?;
        if bodies == 0 {
            return Ok(false);
        }
        // Contiguous IBD: highest archived height ≈ body count − 1.
        let arch_hi = (bodies - 1) as u32;
        let tip = self.tip_height().map(|h| h.0).unwrap_or(0);
        let prewarm = self.parent_prewarm_ready_through().max(tip);
        Ok(arch_hi.saturating_sub(prewarm) > ARCHIVE_BODY_DONTNEED_LEAD)
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
