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

    /// Mega-batch Class A: **packed full-tx** rows (one `tx.body` payload per tx).
    ///
    /// Non-coinbase inputs store **create_fk + vout** (schema v10). Parent fks are
    /// resolved per header: same-block map → writer sticky → durable `tx.head`.
    ///
    /// Headers are committed **one at a time** (put + sticky after success) so:
    /// - out-of-order body delivery does not poison earlier coinbase headers when a
    ///   later spend's parent is not archived yet;
    /// - sticky never advertises create fks that were not written (failed batch).
    ///
    /// Missing parent → [`StoreError::NotFound`] (transient; IBD requeues), not
    /// Corrupt (which looked like a permanent schema failure).
    fn archive_bodies_mega_owned(
        &self,
        need: &mut [(Fk, Vec<TxApply>)],
    ) -> Result<(), QueryError> {
        let mut first_err: Option<QueryError> = None;
        for (header_fk, txs) in need.iter_mut() {
            if txs.is_empty() {
                continue;
            }
            // Skip already-archived (idempotent multi-peer).
            if self.store.header_txs.has_body(*header_fk)? {
                let _ = std::mem::take(txs);
                continue;
            }
            match self.archive_one_header_body(*header_fk, txs) {
                Ok(()) => {}
                Err(e) => {
                    // Keep going so later headers in the batch (e.g. lower height
                    // coinbases that landed after a spend) still commit when possible.
                    // First error is returned after the pass; IBD requeues failures.
                    if first_err.is_none() {
                        first_err = Some(e);
                    }
                }
            }
        }
        match first_err {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }

    /// Archive one header's Class A bodies; update sticky only after put succeeds.
    fn archive_one_header_body(
        &self,
        header_fk: Fk,
        txs: &mut Vec<TxApply>,
    ) -> Result<(), QueryError> {
        use std::collections::{HashMap, HashSet};

        if txs.is_empty() {
            return Ok(());
        }
        let archive_spends =
            self.spend_index_enabled() && self.index_mode().is_tip();
        let index_tx = self.tx_index_enabled();

        let mut next_tx = self.store.txs.count() + 1;
        let first_tx_fk = Fk(next_tx);
        let n_txs = txs.len() as u32;

        // Same-block creates (child may spend parent coinbase in this block).
        let mut block_map: HashMap<[u8; 32], Fk> = HashMap::with_capacity(txs.len());
        let mut work: Vec<(Fk, TxRecord, Vec<InputRecord>, Vec<OutputRecord>)> =
            Vec::with_capacity(txs.len());

        for ta in txs.drain(..) {
            let n_in = ta.inputs.len() as u32;
            let n_out = ta.outputs.len() as u32;
            let tx_fk = Fk(next_tx);
            next_tx += 1;

            let mut tx = ta.tx;
            tx.input_start_fk = Fk::NULL;
            tx.input_count = n_in;
            tx.output_start_fk = Fk::NULL;
            tx.output_count = n_out;

            block_map.insert(tx.txid, tx_fk);
            work.push((tx_fk, tx, ta.inputs, ta.outputs));
        }

        // Unique external parents (not same-block).
        let mut need_external: HashSet<[u8; 32]> = HashSet::new();
        for (_sfk, _tx, inputs, _) in &work {
            for inp in inputs {
                if inp.is_coinbase() || !inp.create_fk.is_null() {
                    continue;
                }
                if block_map.contains_key(&inp.prev_txid) {
                    continue;
                }
                if inp.prev_txid == [0u8; 32] {
                    continue;
                }
                need_external.insert(inp.prev_txid);
            }
        }

        let need_vec: Vec<[u8; 32]> = need_external.iter().copied().collect();
        // Sticky holds **committed** creates only (previous successful headers).
        let mut resolved: HashMap<[u8; 32], Fk> =
            self.archive_txid_sticky.lookup_batch(&need_vec);
        let mut need_head: Vec<[u8; 32]> = Vec::new();
        for t in &need_vec {
            if !resolved.contains_key(t) {
                need_head.push(*t);
            }
        }

        // Head read is independent of whether we index this put (always try).
        if !need_head.is_empty() {
            need_head.sort_unstable_by_key(|txid| self.store.txs.head_primary_slot(txid));
            let hits = self.store.get_fk_by_txid_batch(&need_head)?;
            for (txid, fk_opt) in hits {
                if let Some(fk) = fk_opt {
                    resolved.insert(txid, fk);
                }
            }
        }

        // Stamp create_fk; missing parent is transient (body arrived before parent).
        let mut packed: Vec<(TxRecord, Vec<InputRecord>, Vec<OutputRecord>)> =
            Vec::with_capacity(work.len());
        let mut spends: Vec<([u8; 32], u32, Fk, u32)> = Vec::new();
        for (tx_fk, tx, mut inputs, outputs) in work {
            for (i, inp) in inputs.iter_mut().enumerate() {
                if inp.is_coinbase() {
                    inp.create_fk = Fk::NULL;
                    inp.prev_index = u32::MAX;
                    continue;
                }
                if inp.create_fk.is_null() {
                    let cfk = block_map
                        .get(&inp.prev_txid)
                        .copied()
                        .or_else(|| resolved.get(&inp.prev_txid).copied())
                        .ok_or(StoreError::NotFound)?;
                    inp.create_fk = cfk;
                }
                if archive_spends {
                    spends.push((inp.prev_txid, inp.prev_index, tx_fk, i as u32));
                }
            }
            packed.push((tx, inputs, outputs));
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
        if got_tx_fks.first().copied() != Some(first_tx_fk) {
            return Err(StoreError::Corrupt("tx put_full_batch fk mismatch"));
        }

        if archive_spends && !spends.is_empty() {
            self.store.put_spend_batch(&spends)?;
        }

        self.store
            .header_txs
            .put_ranges_batch(&[(header_fk, first_tx_fk, n_txs)])?;

        // Sticky only after durable put — never advertise uncommitted fks.
        let sticky_regs: Vec<([u8; 32], Fk)> = packed
            .iter()
            .zip(got_tx_fks.iter())
            .map(|((tx, _, _), fk)| (tx.txid, *fk))
            .collect();
        self.archive_txid_sticky.insert_many(&sticky_regs);
        // Also sticky head-resolved parents (cold parents stay hot for next spends).
        for (txid, fk) in &resolved {
            self.archive_txid_sticky.insert(*txid, *fk);
        }

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

    /// Resolve prev outpoint txid for an input.
    ///
    /// Schema v10: soft `prev_txid` may be zero after disk decode; fall back to
    /// create body txid via `create_fk`. Parent **txid** only (not prevout outs).
    pub fn resolve_prev_txid(&self, inp: &InputRecord) -> Result<[u8; 32], QueryError> {
        if inp.is_coinbase() {
            return Ok([0u8; 32]);
        }
        if inp.prev_txid != [0u8; 32] {
            return Ok(inp.prev_txid);
        }
        if inp.create_fk.is_null() {
            return Err(StoreError::Corrupt("input missing create_fk for prev_txid"));
        }
        Ok(self.store.txs.body_txid(inp.create_fk)?)
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
