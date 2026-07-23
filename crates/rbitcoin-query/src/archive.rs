//! Class A archive write path.
//!
//! Split for IBD dual-thread:
//! - **Plan** ([`Query::archive_plan_mega_owned`]): store **reads** — assign create
//!   fks, sticky + `tx.head` resolve, stamp inputs.
//! - **Commit** ([`Query::archive_commit_plan`]): store **writes** — body append,
//!   head index, header_txs, sticky publish.

use super::*;

/// Write-ready mega-batch from plan (prep) to commit (writer).
///
/// Planned create fks match `txs.count()+1…` at plan time; commit fails if the
/// appender returns different fks (another writer interleave — must not happen).
#[derive(Debug)]
pub struct ArchiveWritePlan {
    pub packed: Vec<(TxRecord, Vec<InputRecord>, Vec<OutputRecord>)>,
    pub planned_fks: Vec<Fk>,
    pub per_header_ranges: Vec<(Fk, Fk, u32)>,
    pub spends: Vec<([u8; 32], u32, Fk, u32)>,
    pub sticky_creates: Vec<([u8; 32], Fk)>,
    pub head_resolved: Vec<([u8; 32], Fk)>,
    pub index_tx: bool,
    pub body_est: u64,
    /// Snapshot of “far ahead of confirm” at plan time.
    pub advise_dont_need: bool,
}

impl ArchiveWritePlan {
    pub fn empty() -> Self {
        Self {
            packed: Vec::new(),
            planned_fks: Vec::new(),
            per_header_ranges: Vec::new(),
            spends: Vec::new(),
            sticky_creates: Vec::new(),
            head_resolved: Vec::new(),
            index_tx: false,
            body_est: 0,
            advise_dont_need: false,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.packed.is_empty()
    }
}

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
            let plan = self.archive_plan_mega_owned(&mut need)?;
            self.archive_commit_plan(plan)?;
        }
        Ok(header_fks)
    }

    /// Filter already-archived headers (store **read**). Returns items that still
    /// need Class A, plus the header_fk list for the caller's result order.
    ///
    /// Used by IBD prep after structure decode.
    pub fn archive_filter_need_bodies(
        &self,
        items: &mut [(Fk, HeaderRecord, Vec<TxApply>)],
    ) -> Result<(Vec<Fk>, Vec<(Fk, Vec<TxApply>)>), QueryError> {
        let mut header_fks = Vec::with_capacity(items.len());
        let mut need: Vec<(Fk, Vec<TxApply>)> = Vec::with_capacity(items.len());
        let mut seen_headers = std::collections::HashSet::new();
        for (fk, _header, txs) in items.iter_mut() {
            header_fks.push(*fk);
            if !seen_headers.insert(*fk) {
                let _ = std::mem::take(txs);
                continue;
            }
            if self.store.header_txs.has_body(*fk)? {
                let _ = std::mem::take(txs);
                continue;
            }
            if !txs.is_empty() {
                need.push((*fk, std::mem::take(txs)));
            }
        }
        Ok((header_fks, need))
    }

    /// **Prep / read path:** assign create fks, sticky + head resolve, stamp
    /// inputs. No Class A body/head writes (those are [`Self::archive_commit_plan`]).
    ///
    /// Caller must not plan the next mega-batch until the previous plan has been
    /// committed — planned fks are `txs.count()+1…` at plan time.
    pub fn archive_plan_mega_owned(
        &self,
        need: &mut [(Fk, Vec<TxApply>)],
    ) -> Result<ArchiveWritePlan, QueryError> {
        use std::collections::{HashMap, HashSet};
        use std::time::Instant;

        if need.is_empty() {
            return Ok(ArchiveWritePlan::empty());
        }

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

        // Pass 2: unique external prev_txids that still need fk (reads only).
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

        let need_vec: Vec<[u8; 32]> = need_external.iter().copied().collect();
        let mut resolved = self.archive_txid_sticky.lookup_batch(&need_vec);
        let sticky_hit_n = resolved.len() as u64;
        let mut need_head: Vec<[u8; 32]> = Vec::new();
        for t in &need_vec {
            if !resolved.contains_key(t) {
                need_head.push(*t);
            }
        }
        let head_need_n = need_head.len() as u64;
        let mut head_hit_n = 0u64;
        let mut head_resolved: Vec<([u8; 32], Fk)> = Vec::new();

        if !need_head.is_empty() {
            need_head.sort_unstable_by_key(|txid| self.store.txs.head_primary_slot(txid));
            let hits = self.store.get_fk_by_txid_batch(&need_head)?;
            for (txid, fk_opt) in hits {
                if let Some(fk) = fk_opt {
                    resolved.insert(txid, fk);
                    head_resolved.push((txid, fk));
                    head_hit_n = head_hit_n.saturating_add(1);
                }
            }
        }
        let resolve_ns = t_resolve.elapsed().as_nanos() as u64;

        // Pass 3: stamp create_fk on inputs; tip spends list.
        let mut packed: Vec<(TxRecord, Vec<InputRecord>, Vec<OutputRecord>)> =
            Vec::with_capacity(work.len());
        let mut planned_fks: Vec<Fk> = Vec::with_capacity(work.len());
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
            planned_fks.push(tx_fk);
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

        let sticky_creates: Vec<([u8; 32], Fk)> = packed
            .iter()
            .zip(planned_fks.iter())
            .map(|((tx, _, _), fk)| (tx.txid, *fk))
            .collect();

        Ok(ArchiveWritePlan {
            packed,
            planned_fks,
            per_header_ranges,
            spends,
            sticky_creates,
            head_resolved,
            index_tx,
            body_est,
            advise_dont_need: self.archive_far_ahead_of_confirm()?,
        })
    }

    /// **Writer / write path:** durable Class A put + sticky publish.
    ///
    /// No sticky/head **lookups** — only appends and sticky inserts.
    pub fn archive_commit_plan(&self, plan: ArchiveWritePlan) -> Result<(), QueryError> {
        if plan.packed.is_empty() {
            return Ok(());
        }

        self.store
            .txs
            .reserve_append(plan.body_est, plan.packed.len() as u64)?;

        let body_off = self.store.txs.body_logical_len();
        let got_tx_fks = self
            .store
            .put_tx_full_batch_indexed(&plan.packed, plan.index_tx)?;
        if got_tx_fks.len() != plan.packed.len() {
            return Err(StoreError::Corrupt("tx put_full_batch length"));
        }
        if got_tx_fks != plan.planned_fks {
            return Err(StoreError::Corrupt(
                "tx put_full_batch fk mismatch (plan not committed in order)",
            ));
        }

        if !plan.spends.is_empty() {
            self.store.put_spend_batch(&plan.spends)?;
        }

        if !plan.per_header_ranges.is_empty() {
            self.store
                .header_txs
                .put_ranges_batch(&plan.per_header_ranges)?;
        }

        // Sticky only after durable put — never advertise uncommitted fks.
        self.archive_txid_sticky.insert_many(&plan.sticky_creates);
        if !plan.head_resolved.is_empty() {
            self.archive_txid_sticky
                .insert_many(&plan.head_resolved);
        }

        let body_end = self.store.txs.body_logical_len();
        let body_len = body_end.saturating_sub(body_off);
        if body_len > 0 && plan.advise_dont_need {
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
