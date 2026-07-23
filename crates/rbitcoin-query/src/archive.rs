//! Class A archive write path.

use super::*;

/// When Class A high-water is this many blocks ahead of the parent cache watermark,
/// drop just-written `tx.body` pages from the page cache so archive dirty pages
/// do not crowd out confirm/cache. Below this, keep pages (cache may need them).
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
    /// Caller should pass a **height-contiguous** run so create_fk parents are
    /// already committed or in this batch (IBD writer enforces this).
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
    /// resolved here: same mega-batch map → writer sticky → durable `tx.head`.
    ///
    /// IBD feeds **height-contiguous** mega-batches only (out-of-order bodies are
    /// parked until the next height is ready). That keeps same-batch + sticky
    /// resolve complete without per-block fallback.
    fn archive_bodies_mega_owned(
        &self,
        need: &mut [(Fk, Vec<TxApply>)],
    ) -> Result<(), QueryError> {
        use std::collections::{HashMap, HashSet};
        use std::time::Instant;

        let mut next_tx = self.store.txs.count() + 1;
        let n_headers = need.iter().filter(|(_, t)| !t.is_empty()).count() as u64;

        // Pass 1: assign create fks + build batch_map (txid → create_fk).
        let mut batch_map: HashMap<[u8; 32], Fk> = HashMap::new();
        let mut work: Vec<(Fk, TxRecord, Vec<InputRecord>, Vec<OutputRecord>)> =
            Vec::new();
        let mut per_header_ranges: Vec<(Fk, Fk, u32)> = Vec::with_capacity(need.len());
        let mut spends: Vec<([u8; 32], u32, Fk, u32)> = Vec::new();
        let archive_spends =
            self.spend_index_enabled() && self.index_mode().is_tip();
        let index_tx = self.tx_index_enabled();

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
                tx.input_start_fk = Fk::NULL;
                tx.input_count = n_in;
                tx.output_start_fk = Fk::NULL;
                tx.output_count = n_out;

                batch_map.insert(tx.txid, tx_fk);
                work.push((tx_fk, tx, ta.inputs, ta.outputs));
            }
            per_header_ranges.push((*header_fk, first_tx_fk, n_txs));
        }

        // Pass 2: unique external prev_txids that still need fk.
        let t_resolve = Instant::now();
        let mut need_external: HashSet<[u8; 32]> = HashSet::new();
        for (_sfk, _tx, inputs, _) in &work {
            for inp in inputs {
                if inp.is_coinbase() || !inp.create_fk.is_null() {
                    continue;
                }
                if batch_map.contains_key(&inp.prev_txid) {
                    continue;
                }
                if inp.prev_txid == [0u8; 32] {
                    continue;
                }
                need_external.insert(inp.prev_txid);
            }
        }

        // Sticky (committed prior batches) then durable head for misses.
        let need_vec: Vec<[u8; 32]> = need_external.iter().copied().collect();
        let sticky_hits = self.archive_txid_sticky.lookup_batch(&need_vec);
        let sticky_hit_n = sticky_hits.len() as u64;
        let mut resolved: HashMap<[u8; 32], Fk> = sticky_hits;
        let mut need_head: Vec<[u8; 32]> = Vec::new();
        for t in &need_vec {
            if !resolved.contains_key(t) {
                need_head.push(*t);
            }
        }
        let head_need_n = need_head.len() as u64;
        let mut head_hit_n = 0u64;

        if !need_head.is_empty() {
            need_head.sort_unstable_by_key(|txid| self.store.txs.head_primary_slot(txid));
            let hits = self.store.get_fk_by_txid_batch(&need_head)?;
            for (txid, fk_opt) in hits {
                if let Some(fk) = fk_opt {
                    resolved.insert(txid, fk);
                    head_hit_n = head_hit_n.saturating_add(1);
                }
            }
        }
        let resolve_ns = t_resolve.elapsed().as_nanos() as u64;

        // Pass 3: stamp create_fk on inputs; tip spends list.
        let mut packed: Vec<(TxRecord, Vec<InputRecord>, Vec<OutputRecord>)> =
            Vec::with_capacity(work.len());
        let mut batch_stamp = 0u64;
        let mut resolved_stamp = 0u64;
        for (tx_fk, tx, mut inputs, outputs) in work {
            for (i, inp) in inputs.iter_mut().enumerate() {
                if inp.is_coinbase() {
                    inp.create_fk = Fk::NULL;
                    inp.prev_index = u32::MAX;
                    continue;
                }
                if inp.create_fk.is_null() {
                    if let Some(&cfk) = batch_map.get(&inp.prev_txid) {
                        inp.create_fk = cfk;
                        batch_stamp = batch_stamp.saturating_add(1);
                    } else if let Some(&cfk) = resolved.get(&inp.prev_txid) {
                        inp.create_fk = cfk;
                        resolved_stamp = resolved_stamp.saturating_add(1);
                    } else {
                        return Err(StoreError::Corrupt(
                            "archive: parent create_fk unresolved (contiguous batch required)",
                        ));
                    }
                }
                if archive_spends {
                    spends.push((inp.prev_txid, inp.prev_index, tx_fk, i as u32));
                }
            }
            packed.push((tx, inputs, outputs));
        }
        crate::archive_resolve_stats::note(
            n_headers,
            need_vec.len() as u64,
            sticky_hit_n,
            head_need_n,
            head_hit_n,
            batch_stamp,
            resolved_stamp,
            resolve_ns,
        );

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

        if archive_spends && !spends.is_empty() {
            self.store.put_spend_batch(&spends)?;
        }

        self.store.header_txs.put_ranges_batch(&per_header_ranges)?;

        // Sticky only after durable put — never advertise uncommitted fks.
        let sticky_regs: Vec<([u8; 32], Fk)> = packed
            .iter()
            .zip(got_tx_fks.iter())
            .map(|((tx, _, _), fk)| (tx.txid, *fk))
            .collect();
        self.archive_txid_sticky.insert_many(&sticky_regs);
        for (txid, fk) in &resolved {
            self.archive_txid_sticky.insert(*txid, *fk);
        }

        let body_end = self.store.txs.body_logical_len();
        let body_len = body_end.saturating_sub(body_off);
        if body_len > 0 && self.archive_far_ahead_of_confirm()? {
            self.store.txs.advise_body_dont_need(body_off, body_len);
        }
        Ok(())
    }

    /// True when Class A high-water is more than [`ARCHIVE_BODY_DONTNEED_LEAD`]
    /// blocks ahead of the parent cache ready watermark (or tip if cache idle).
    fn archive_far_ahead_of_confirm(&self) -> Result<bool, QueryError> {
        let bodies = self.store.header_txs.count_bodies()?;
        if bodies == 0 {
            return Ok(false);
        }
        // Contiguous IBD: highest archived height ≈ body count − 1.
        let arch_hi = (bodies - 1) as u32;
        let tip = self.tip_height().map(|h| h.0).unwrap_or(0);
        let cache = self.parent_cache_ready_through().max(tip);
        Ok(arch_hi.saturating_sub(cache) > ARCHIVE_BODY_DONTNEED_LEAD)
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
