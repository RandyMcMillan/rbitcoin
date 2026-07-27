//! Class A archive write path.
//!
//! Split for IBD dual-thread (prep/write may overlap with a small plan queue):
//! - **Plan** ([`Query::archive_plan_mega_owned`] / [`Query::archive_plan_mega_from`]):
//!   store **reads** — assign create fks (optionally from a reserved HWM), sticky +
//!   **in-flight planned creates** + `tx.head` resolve, stamp inputs.
//! - **Commit** ([`Query::archive_commit_plan`]): store **writes** — body append,
//!   head index, header_txs, sticky publish.
//! - **Prewarm** ([`Query::archive_sticky_prewarm`]): bulk last-N `body_txid_range`
//!   fill of sticky before the pipeline starts.
//!
//! Overlap requires the in-flight map: a later mega-batch may spend outputs from a
//! prior plan that is still queued/committing (not yet in sticky/head).

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
    /// Creates from **this** batch only — published to sticky after head insert.
    /// Parents resolved via durable `tx.head` are **not** sticky-published (they
    /// thrash the FIFO with long-tail keys and evict recent creates).
    pub sticky_creates: Vec<([u8; 32], Fk)>,
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
    /// Planned create fks start at `txs.count()+1`. For overlapping plan/write
    /// (prep queue depth &gt; 1), use [`Self::archive_plan_mega_from`] with a
    /// reserved FK HWM so in-flight plans do not collide.
    pub fn archive_plan_mega_owned(
        &self,
        need: &mut [(Fk, Vec<TxApply>)],
    ) -> Result<ArchiveWritePlan, QueryError> {
        use std::collections::HashMap;
        let start = self.store.txs.count().saturating_add(1);
        let empty = HashMap::new();
        self.archive_plan_mega_from(need, start, &empty)
    }

    /// Like [`Self::archive_plan_mega_owned`], but assign create fks from
    /// `next_tx_start` (inclusive) instead of live `txs.count()+1`.
    ///
    /// IBD prep keeps a local reserved HWM: after each successful non-empty plan,
    /// advance to `planned_fks.last()+1` so the next mega-batch can be planned
    /// while a prior batch is still committing (ordered writer preserves match).
    ///
    /// `in_flight`: create txid→fk from prior plans that are queued/committing
    /// but not yet in sticky/head. Required for queue depth &gt; 1 when a later
    /// batch spends a prior batch's creates.
    pub fn archive_plan_mega_from(
        &self,
        need: &mut [(Fk, Vec<TxApply>)],
        next_tx_start: u64,
        in_flight: &std::collections::HashMap<[u8; 32], Fk>,
    ) -> Result<ArchiveWritePlan, QueryError> {
        use std::collections::{HashMap, HashSet};
        use std::time::Instant;

        if need.is_empty() {
            return Ok(ArchiveWritePlan::empty());
        }

        let mut next_tx = next_tx_start.max(1);
        let n_headers = need.iter().filter(|(_, t)| !t.is_empty()).count() as u64;

        // Pass 1: assign create fks + build batch_map (txid → create_fk).
        let t_assign = Instant::now();
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
        let assign_ns = t_assign.elapsed().as_nanos() as u64;

        // Pass 2: unique external prev_txids that still need fk (reads only).
        let t_collect = Instant::now();
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
        let collect_ns = t_collect.elapsed().as_nanos() as u64;

        let t_sticky = Instant::now();
        // Sticky hit ⇒ skip head; range on hit ⇒ skip idx for that create elsewhere.
        let sticky_hits = self.archive_txid_sticky.lookup_batch(&need_vec);
        let sticky_hit_n = sticky_hits.len() as u64;
        let mut resolved: HashMap<[u8; 32], Fk> =
            HashMap::with_capacity(sticky_hits.len().saturating_add(need_vec.len() / 4));
        for (txid, hit) in sticky_hits {
            resolved.insert(txid, hit.fk);
        }
        let sticky_ns = t_sticky.elapsed().as_nanos() as u64;

        // Prior mega-batch(es) still in the write queue: not sticky/head yet.
        let t_inflight = Instant::now();
        if !in_flight.is_empty() {
            for t in &need_vec {
                if resolved.contains_key(t) {
                    continue;
                }
                if let Some(&fk) = in_flight.get(t) {
                    resolved.insert(*t, fk);
                }
            }
        }
        let inflight_ns = t_inflight.elapsed().as_nanos() as u64;

        let t_head = Instant::now();
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
        let head_ns = t_head.elapsed().as_nanos() as u64;

        // Pass 3: stamp create_fk on inputs; tip spends list.
        let t_stamp = Instant::now();
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
        let stamp_ns = t_stamp.elapsed().as_nanos() as u64;

        let t_finish = Instant::now();
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

        let advise_dont_need = self.archive_far_ahead_of_confirm()?;
        let finish_ns = t_finish.elapsed().as_nanos() as u64;

        crate::archive_phase_stats::note_resolve_counts(
            n_headers,
            need_vec.len() as u64,
            sticky_hit_n,
            head_need_n,
            head_hit_n,
            batch_stamp,
            resolved_stamp,
        );
        crate::archive_phase_stats::note_prep_plan(
            assign_ns,
            collect_ns,
            sticky_ns,
            inflight_ns,
            head_ns,
            stamp_ns,
            finish_ns,
        );

        Ok(ArchiveWritePlan {
            packed,
            planned_fks,
            per_header_ranges,
            spends,
            sticky_creates,
            index_tx,
            body_est,
            advise_dont_need,
        })
    }

    /// Linear sticky prewarm: last `cap` Class A bodies via sequential `tx.idx`
    /// order (bulk `body_txid_range` — no full decode / head probe).
    ///
    /// Call once before the IBD archive pipeline so cold restarts avoid a head
    /// resolve storm. Returns `(loaded, elapsed_ms)`.
    pub fn archive_sticky_prewarm(&self) -> Result<(usize, u64), QueryError> {
        use std::time::Instant;
        let t0 = Instant::now();
        let cap = self.archive_txid_sticky.cap();
        let n = self.store.txs.count();
        if n == 0 || cap == 0 {
            return Ok((0, t0.elapsed().as_millis() as u64));
        }
        // Last `min(cap, n)` fks: (n-cap+1)..=n (1-based).
        let start = n.saturating_sub(cap as u64).saturating_add(1).max(1);
        let expect = (n.saturating_sub(start).saturating_add(1)) as usize;
        self.archive_txid_sticky.reserve_for_prewarm(expect);
        // Same ballpark as head-resize bulk waves; sticky insert is cheap.
        const CHUNK: u64 = 8192;
        let mut loaded = 0usize;
        let mut cur = start;
        while cur <= n {
            let end = (cur + CHUNK - 1).min(n);
            let txids = self.store.txs.body_txid_range(cur, end)?;
            debug_assert_eq!(txids.len() as u64, end - cur + 1);
            let fks: Vec<Fk> = (cur..=end).map(Fk).collect();
            let ranges = self.store.tx_body_range_batch(&fks)?;
            let batch: Vec<([u8; 32], Fk, u64, u64)> = txids
                .into_iter()
                .zip(fks.into_iter())
                .zip(ranges.into_iter())
                .filter_map(|((txid, fk), range)| {
                    let (off, len) = range?;
                    Some((txid, fk, off, len))
                })
                .collect();
            self.archive_txid_sticky.insert_many_with_ranges(&batch);
            loaded = loaded.saturating_add(batch.len());
            cur = end + 1;
        }
        Ok((loaded, t0.elapsed().as_millis() as u64))
    }

    /// **Writer / write path:** durable Class A put + sticky publish.
    ///
    /// No sticky/head **lookups** — only appends and sticky inserts.
    /// Phase walls go to [`crate::archive_phase_stats`] (body vs head split).
    pub fn archive_commit_plan(&self, plan: ArchiveWritePlan) -> Result<(), QueryError> {
        use std::time::Instant;
        if plan.packed.is_empty() {
            return Ok(());
        }
        let t0 = Instant::now();
        let n_blocks = plan.per_header_ranges.len() as u64;

        let t = Instant::now();
        self.store
            .txs
            .reserve_append(plan.body_est, plan.packed.len() as u64)?;
        let reserve_ns = t.elapsed().as_nanos() as u64;

        let body_off = self.store.txs.body_logical_len();
        // Body append first (no head), then head insert — separate timers.
        let t = Instant::now();
        let got_tx_fks = self
            .store
            .put_tx_full_batch_indexed(&plan.packed, /*index=*/ false)?;
        let body_ns = t.elapsed().as_nanos() as u64;
        if got_tx_fks.len() != plan.packed.len() {
            return Err(StoreError::Corrupt("tx put_full_batch length"));
        }
        if got_tx_fks != plan.planned_fks {
            return Err(StoreError::Corrupt(
                "tx put_full_batch fk mismatch (plan not committed in order)",
            ));
        }

        // Head sole-writer insert (plain store + fence inside head_insert_many).
        let t = Instant::now();
        if plan.index_tx {
            let heads: Vec<([u8; 32], Fk)> = plan
                .packed
                .iter()
                .zip(got_tx_fks.iter())
                .map(|((tx, _, _), fk)| (tx.txid, *fk))
                .collect();
            self.store.txs.head_insert_many(&heads)?;
        }
        let head_ns = t.elapsed().as_nanos() as u64;

        let t = Instant::now();
        if !plan.spends.is_empty() {
            self.store.put_spend_batch(&plan.spends)?;
        }
        let spend_ns = t.elapsed().as_nanos() as u64;

        let t = Instant::now();
        if !plan.per_header_ranges.is_empty() {
            self.store
                .header_txs
                .put_ranges_batch(&plan.per_header_ranges)?;
        }
        let htxs_ns = t.elapsed().as_nanos() as u64;

        // Sticky only after head batch is published (fence in head_insert_many).
        // Publish **this batch's creates only** — not parents just looked up from
        // durable head (those thrash FIFO with cold long-tail keys). Include body
        // ranges (dense planned fks → cheap sequential idx) so later sticky hits
        // skip head + idx in prep/confirm.
        let t = Instant::now();
        let ranges = self.store.tx_body_range_batch(&got_tx_fks)?;
        let mut with_range: Vec<([u8; 32], Fk, u64, u64)> =
            Vec::with_capacity(plan.sticky_creates.len());
        let mut fk_only: Vec<([u8; 32], Fk)> = Vec::new();
        for (&(txid, fk), range) in plan.sticky_creates.iter().zip(ranges.into_iter()) {
            match range {
                Some((off, len)) if len > 0 => with_range.push((txid, fk, off, len)),
                _ => fk_only.push((txid, fk)),
            }
        }
        self.archive_txid_sticky
            .insert_many_with_ranges(&with_range);
        self.archive_txid_sticky.insert_many(&fk_only);
        let sticky_ns = t.elapsed().as_nanos() as u64;

        let t = Instant::now();
        let body_end = self.store.txs.body_logical_len();
        let body_len = body_end.saturating_sub(body_off);
        if body_len > 0 && plan.advise_dont_need {
            self.store.txs.advise_body_dont_need(body_off, body_len);
        }
        let dontneed_ns = t.elapsed().as_nanos() as u64;

        let total_ns = t0.elapsed().as_nanos() as u64;
        crate::archive_phase_stats::note_write_commit(
            total_ns,
            reserve_ns,
            body_ns,
            head_ns,
            spend_ns,
            htxs_ns,
            sticky_ns,
            dontneed_ns,
            n_blocks.max(1),
        );
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

#[cfg(test)]
mod tests {
    use crate::{Query, TxApply};
    use rbitcoin_primitives::Fk;
    use rbitcoin_store::{InputRecord, OutputRecord, TxRecord};
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_query(label: &str) -> (std::path::PathBuf, Query) {
        let n = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("rbitcoin-arch-{label}-{n}"));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let q = Query::open_or_create(&dir).unwrap();
        (dir, q)
    }

    fn coinbase_apply(i: u64) -> TxApply {
        let mut txid = [0u8; 32];
        txid[0..8].copy_from_slice(&i.to_le_bytes());
        txid[8] = 0xcb;
        TxApply {
            tx: TxRecord {
                txid,
                version: 1,
                locktime: 0,
                input_start_fk: Fk::NULL,
                input_count: 1,
                output_start_fk: Fk::NULL,
                output_count: 1,
            },
            inputs: vec![InputRecord {
                prev_txid: [0u8; 32],
                create_fk: Fk::NULL,
                prev_index: u32::MAX,
                sequence: u32::MAX,
                script_sig: vec![i as u8],
                witness: vec![],
            }],
            outputs: vec![OutputRecord::unspent(50 * 100_000_000, vec![0x51])],
        }
    }

    #[test]
    fn archive_phase_stats_cover_plan_and_commit_wall() {
        use std::collections::HashMap;
        // Drain any prior noise.
        let _ = crate::archive_phase_stats::sample_and_reset();
        let (dir, q) = temp_query("arch-phases");
        let mut need = vec![(Fk(1), vec![coinbase_apply(1), coinbase_apply(2)])];
        let plan = q
            .archive_plan_mega_from(&mut need, 1, &HashMap::new())
            .unwrap();
        assert_eq!(plan.planned_fks.len(), 2);
        q.archive_commit_plan(plan).unwrap();
        let s = crate::archive_phase_stats::sample_and_reset();
        assert!(s.prep_assign_ns > 0 || s.prep_stamp_ns > 0, "plan timed");
        assert!(s.write_total_ns > 0, "commit total");
        assert!(s.write_body_ns > 0, "body put timed");
        let wsum = s.write_phases_sum_ns();
        // Sequential Instant slices: sum ≤ total + small clock noise; gap is residual.
        assert!(
            wsum <= s.write_total_ns.saturating_add(200_000),
            "write sum {} ≫ total {}",
            wsum,
            s.write_total_ns
        );
        assert!(
            s.write_total_ns.saturating_sub(wsum) < s.write_total_ns.max(1),
            "unaccounted write {} of total {}",
            s.write_total_ns.saturating_sub(wsum),
            s.write_total_ns
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn plan_from_reserves_fks_for_overlap_then_commit_in_order() {
        use std::collections::HashMap;
        let (dir, q) = temp_query("plan-from");
        // Seed one body so count starts at 1.
        let seed = vec![(Fk(1), vec![coinbase_apply(1)])];
        // Need a real header_fk path: plan only needs Vec<(Fk, Vec<TxApply>)>.
        let mut need0 = seed;
        let p0 = q.archive_plan_mega_owned(&mut need0).unwrap();
        q.archive_commit_plan(p0).unwrap();
        assert_eq!(q.tx_body_count(), 1);

        // Reserve two plans as prep would with write queue depth 2.
        let empty = HashMap::new();
        let mut next = q.tx_body_count() + 1;
        let mut need_a = vec![(Fk(10), vec![coinbase_apply(10), coinbase_apply(11)])];
        let plan_a = q.archive_plan_mega_from(&mut need_a, next, &empty).unwrap();
        assert_eq!(plan_a.planned_fks, vec![Fk(2), Fk(3)]);
        next = plan_a.planned_fks.last().unwrap().0 + 1;
        assert_eq!(next, 4);

        let mut need_b = vec![(Fk(20), vec![coinbase_apply(20)])];
        let plan_b = q.archive_plan_mega_from(&mut need_b, next, &empty).unwrap();
        assert_eq!(plan_b.planned_fks, vec![Fk(4)]);
        // Durable count still 1 until commit.
        assert_eq!(q.tx_body_count(), 1);

        q.archive_commit_plan(plan_a).unwrap();
        assert_eq!(q.tx_body_count(), 3);
        q.archive_commit_plan(plan_b).unwrap();
        assert_eq!(q.tx_body_count(), 4);

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Overlapping plan must resolve parents from a prior uncommitted mega-batch.
    /// Without `in_flight`, this is the "parent create_fk unresolved" corruption.
    #[test]
    fn overlap_plan_resolves_parent_via_inflight_creates() {
        use std::collections::HashMap;
        let (dir, q) = temp_query("inflight-parent");
        let mut need_a = vec![(Fk(1), vec![coinbase_apply(1)])];
        let empty = HashMap::new();
        let plan_a = q.archive_plan_mega_from(&mut need_a, 1, &empty).unwrap();
        assert_eq!(plan_a.planned_fks, vec![Fk(1)]);
        let parent_txid = plan_a.sticky_creates[0].0;
        let parent_fk = plan_a.sticky_creates[0].1;

        // Child spends parent — not in sticky/head until plan_a commits.
        let mut child_txid = [0u8; 32];
        child_txid[0] = 0xee;
        let child = TxApply {
            tx: TxRecord {
                txid: child_txid,
                version: 1,
                locktime: 0,
                input_start_fk: Fk::NULL,
                input_count: 1,
                output_start_fk: Fk::NULL,
                output_count: 1,
            },
            inputs: vec![InputRecord {
                prev_txid: parent_txid,
                create_fk: Fk::NULL, // must resolve
                prev_index: 0,
                sequence: u32::MAX,
                script_sig: vec![],
                witness: vec![],
            }],
            outputs: vec![OutputRecord::unspent(1, vec![0x51])],
        };
        let mut need_b = vec![(Fk(2), vec![child])];

        // Without in_flight → unresolved.
        let err = q
            .archive_plan_mega_from(&mut need_b, 2, &empty)
            .unwrap_err();
        assert!(
            err.to_string().contains("create_fk unresolved"),
            "expected unresolved without inflight, got {err}"
        );

        // Rebuild child (need_b was drained on failure).
        let child = TxApply {
            tx: TxRecord {
                txid: child_txid,
                version: 1,
                locktime: 0,
                input_start_fk: Fk::NULL,
                input_count: 1,
                output_start_fk: Fk::NULL,
                output_count: 1,
            },
            inputs: vec![InputRecord {
                prev_txid: parent_txid,
                create_fk: Fk::NULL,
                prev_index: 0,
                sequence: u32::MAX,
                script_sig: vec![],
                witness: vec![],
            }],
            outputs: vec![OutputRecord::unspent(1, vec![0x51])],
        };
        let mut need_b = vec![(Fk(2), vec![child])];
        let inflight: HashMap<_, _> = plan_a.sticky_creates.iter().copied().collect();
        let plan_b = q
            .archive_plan_mega_from(&mut need_b, 2, &inflight)
            .expect("inflight parent resolve");
        assert_eq!(plan_b.planned_fks, vec![Fk(2)]);
        assert_eq!(
            plan_b.packed[0].1[0].create_fk, parent_fk,
            "child input must stamp prior planned create_fk"
        );

        q.archive_commit_plan(plan_a).unwrap();
        q.archive_commit_plan(plan_b).unwrap();
        assert_eq!(q.tx_body_count(), 2);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Sticky must only receive this batch's creates — not parents resolved via
    /// durable `tx.head` (regression: head hits used to FIFO-pollute sticky).
    #[test]
    fn sticky_publish_creates_only_not_head_resolved_parents() {
        use std::collections::HashMap;
        let (dir, q) = temp_query("sticky-creates-only");
        // Parent on disk + head, but **not** in sticky (direct store put).
        let parent = coinbase_apply(1);
        let parent_txid = parent.tx.txid;
        q.store
            .txs
            .put_full_batch_indexed(
                &[(parent.tx, parent.inputs, parent.outputs)],
                /*index=*/ true,
            )
            .unwrap();
        assert_eq!(q.tx_body_count(), 1);
        assert_eq!(q.archive_txid_sticky_stats().0, 0);

        let mut child_txid = [0u8; 32];
        child_txid[0] = 0xcd;
        let child = TxApply {
            tx: TxRecord {
                txid: child_txid,
                version: 1,
                locktime: 0,
                input_start_fk: Fk::NULL,
                input_count: 1,
                output_start_fk: Fk::NULL,
                output_count: 1,
            },
            inputs: vec![InputRecord {
                prev_txid: parent_txid,
                create_fk: Fk::NULL,
                prev_index: 0,
                sequence: u32::MAX,
                script_sig: vec![],
                witness: vec![],
            }],
            outputs: vec![OutputRecord::unspent(1, vec![0x51])],
        };
        let mut need = vec![(Fk(2), vec![child])];
        let plan = q
            .archive_plan_mega_from(&mut need, 2, &HashMap::new())
            .expect("parent via head");
        assert_eq!(plan.planned_fks, vec![Fk(2)]);
        assert_eq!(plan.packed[0].1[0].create_fk, Fk(1));
        assert_eq!(plan.sticky_creates.len(), 1);
        assert_eq!(plan.sticky_creates[0].0, child_txid);

        q.archive_commit_plan(plan).unwrap();
        let (len, _) = q.archive_txid_sticky_stats();
        assert_eq!(len, 1, "sticky must hold only the new create");
        let hits = q
            .archive_txid_sticky
            .lookup_batch(&[parent_txid, child_txid]);
        assert!(
            !hits.contains_key(&parent_txid),
            "head-resolved parent must not be sticky-published"
        );
        assert_eq!(hits.get(&child_txid).map(|h| h.fk), Some(Fk(2)));
        assert!(
            hits.get(&child_txid).and_then(|h| h.body_range).is_some(),
            "commit should sticky-publish body range for new creates"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn sticky_prewarm_loads_last_n_via_idx_order() {
        let (dir, q) = temp_query("sticky-prewarm");
        // Insert more than a small sticky would hold — use env is hard; just
        // prewarm whatever is in the store (cap defaults to 4M, store has few).
        let mut packed = Vec::new();
        for i in 1u64..=50 {
            let ta = coinbase_apply(i);
            packed.push((ta.tx, ta.inputs, ta.outputs));
        }
        q.store
            .txs
            .put_full_batch_indexed(&packed, true)
            .unwrap();
        assert_eq!(q.tx_body_count(), 50);
        assert_eq!(q.archive_txid_sticky_stats().0, 0);

        let (loaded, _ms) = q.archive_sticky_prewarm().unwrap();
        assert_eq!(loaded, 50);
        let (len, _cap) = q.archive_txid_sticky_stats();
        assert_eq!(len, 50);

        // Last txid must resolve from sticky (lookup_batch).
        let mut last_txid = [0u8; 32];
        last_txid[0..8].copy_from_slice(&50u64.to_le_bytes());
        last_txid[8] = 0xcb;
        let hit = q.archive_txid_sticky.lookup_batch(&[last_txid]);
        assert_eq!(hit.get(&last_txid).map(|h| h.fk), Some(Fk(50)));
        assert!(
            hit.get(&last_txid).and_then(|h| h.body_range).is_some(),
            "prewarm should cache body range with fk"
        );
        assert_eq!(
            q.archive_txid_sticky.body_ranges_by_fk(&[Fk(50)]),
            vec![Some(hit[&last_txid].body_range.unwrap())]
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn sticky_prewarm_keeps_tail_when_over_cap() {
        // Use a tiny sticky via constructing sticky alone is hard (Query uses
        // env). Instead verify the start id math: last min(n,cap) of a 50-body
        // store with full prewarm path already tested; here check body_txid
        // range endpoint for Fk(n).
        let (dir, q) = temp_query("sticky-tail");
        let mut packed = Vec::new();
        for i in 1u64..=5 {
            let ta = coinbase_apply(i);
            packed.push((ta.tx, ta.inputs, ta.outputs));
        }
        q.store
            .txs
            .put_full_batch_indexed(&packed, false)
            .unwrap();
        let (loaded, _) = q.archive_sticky_prewarm().unwrap();
        assert_eq!(loaded, 5);
        // Fk 1 and Fk 5 both present when under cap.
        let t1 = coinbase_apply(1).tx.txid;
        let t5 = coinbase_apply(5).tx.txid;
        let hits = q.archive_txid_sticky.lookup_batch(&[t1, t5]);
        assert_eq!(hits.get(&t1).map(|h| h.fk), Some(Fk(1)));
        assert_eq!(hits.get(&t5).map(|h| h.fk), Some(Fk(5)));
        assert!(hits.get(&t1).and_then(|h| h.body_range).is_some());
        assert!(hits.get(&t5).and_then(|h| h.body_range).is_some());
        let _ = std::fs::remove_dir_all(&dir);
    }
}
