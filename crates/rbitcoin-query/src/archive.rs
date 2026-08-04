//! Class A archive write path.
//!
//! Split for IBD dual-thread (prep/write may overlap with a small plan queue):
//! - **Plan** ([`Query::archive_plan_mega_owned`] / [`Query::archive_plan_mega_from`]):
//!   store **reads** — assign create fks (optionally from a reserved HWM),
//!   in-flight planned creates + `tx.head` resolve, stamp inputs.
//!   Head-miss parents use **fk-only** head resolve (no denserels on plan stamp);
//!   denserels for pin load at prep/ensure into plan-local maps only.
//! - **Commit** ([`Query::archive_commit_plan`]): store **writes** — body append,
//!   head index, header_txs. Pipeline pins stay on the plan (`batch_pin`); no
//!   process create FIFO seed.
//!
//! Overlap requires the in-flight map: a later mega-batch may spend outputs from a
//! prior plan that is still queued/committing (not yet in head).

use super::*;

/// Shared immutable create pin: tx meta + full outs + layout denserels.
///
/// One Arc per create — plan `packed` pin half, `batch_pin`, and prep-ahead
/// `in_flight_outs` all Arc-clone this (no deep outs clone between stages).
pub type CreatePin = std::sync::Arc<(TxRecord, Vec<OutputRecord>, Vec<u32>)>;

/// Write-ready mega-batch from plan (prep) to commit (writer).
///
/// Planned create fks match `txs.count()+1…` at plan time; commit fails if the
/// appender returns different fks (another writer interleave — must not happen).
#[derive(Debug)]
pub struct ArchiveWritePlan {
    /// Body-append rows: shared [`CreatePin`] (tx + outs + denserels) + inputs.
    /// Outs live once in the pin Arc (not duplicated alongside inputs).
    pub packed: Vec<(CreatePin, Vec<InputRecord>)>,
    pub planned_fks: Vec<Fk>,
    pub per_header_ranges: Vec<(Fk, Fk, u32)>,
    pub spends: Vec<([u8; 32], u32, Fk, u32)>,
    /// Creates from **this** batch only (txid→fk for in-flight / publish).
    pub batch_creates: Vec<([u8; 32], Fk)>,
    /// Pipeline-local full pins for external parents (outs+denserels).
    ///
    /// Filled by ensure/prep denserels (often from
    /// [`Self::external_parent_ranges`]). **Dropped after pin**
    /// ([`Self::clear_external_parent_outs`]).
    pub external_parent_outs: std::collections::HashMap<u64, CreatePin>,
    /// Head-resolved external parents: create_fk → Class A `(body_off, body_len)`.
    ///
    /// Filled at plan stamp (fk+range short-circuit). Prep denserels-loads by
    /// offset (skip `tx.idx`) into [`Self::external_parent_outs`]. Still live —
    /// not obsolete after schema-13 `txid.body` (identity is separate from range).
    pub external_parent_ranges: std::collections::HashMap<u64, (u64, u64)>,
    /// **RAM-only** reverse of stamp resolve: create_fk id → parent `prev_txid`.
    ///
    /// Built when in-flight / head resolve binds `prev_txid → fk`.
    /// Prep pin fills schema-13 zero body `TxRecord.txid` from this map — **never**
    /// re-pread `txid.body` on the pin path.
    pub external_parent_txids: std::collections::HashMap<u64, [u8; 32]>,
    /// Prep-ahead pin material for **this batch's creates**, parallel to
    /// [`Self::planned_fks`]: same [`CreatePin`] Arcs as [`Self::packed`] (refcount
    /// only). Confirm `note_plan_ok` only `Arc::clone`s into in-flight outs.
    pub batch_pin: Vec<CreatePin>,
    pub index_tx: bool,
    pub body_est: u64,
}

impl ArchiveWritePlan {
    pub fn empty() -> Self {
        Self {
            packed: Vec::new(),
            planned_fks: Vec::new(),
            per_header_ranges: Vec::new(),
            spends: Vec::new(),
            batch_creates: Vec::new(),
            external_parent_outs: std::collections::HashMap::new(),
            external_parent_ranges: std::collections::HashMap::new(),
            external_parent_txids: std::collections::HashMap::new(),
            batch_pin: Vec::new(),
            index_tx: false,
            body_est: 0,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.packed.is_empty()
    }

    /// Wire `prev_txid` known for this create_fk at plan stamp (RAM only).
    #[inline]
    pub fn external_parent_txid(&self, create_fk_id: u64) -> Option<[u8; 32]> {
        self.external_parent_txids.get(&create_fk_id).copied()
    }

    /// Drop pipeline-local external full-outs after denserels pin.
    ///
    /// Sparse need-vouts already live in [`crate::BatchParents`]; commit never
    /// reads this map. Ranges + txid reverse may be cleared with outs (prep done).
    pub fn clear_external_parent_outs(&mut self) {
        self.external_parent_outs.clear();
        self.external_parent_outs.shrink_to_fit();
        self.external_parent_ranges.clear();
        self.external_parent_ranges.shrink_to_fit();
        self.external_parent_txids.clear();
        self.external_parent_txids.shrink_to_fit();
    }

    /// Append another plan for write megabatch (height-ordered Class A).
    ///
    /// Callers must drain scripts→write in height order so `planned_fks` stay
    /// contiguous and match the sole Class A appender sequence. External-parent
    /// maps are usually empty by write time (cleared after prep pin).
    pub fn append(&mut self, mut other: Self) {
        if other.is_empty() && other.per_header_ranges.is_empty() {
            return;
        }
        self.packed.append(&mut other.packed);
        self.planned_fks.append(&mut other.planned_fks);
        self.per_header_ranges.append(&mut other.per_header_ranges);
        self.spends.append(&mut other.spends);
        self.batch_creates.append(&mut other.batch_creates);
        self.batch_pin.append(&mut other.batch_pin);
        self.index_tx |= other.index_tx;
        self.body_est = self.body_est.saturating_add(other.body_est);
        // External maps: rare residual; union by create id (first wins).
        for (k, v) in other.external_parent_outs {
            self.external_parent_outs.entry(k).or_insert(v);
        }
        for (k, v) in other.external_parent_ranges {
            self.external_parent_ranges.entry(k).or_insert(v);
        }
        for (k, v) in other.external_parent_txids {
            self.external_parent_txids.entry(k).or_insert(v);
        }
    }
}

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
        let start = self.store.txs.count().saturating_add(1);
        self.archive_plan_mega_from(need, start, &crate::InFlightView::empty())
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
        in_flight: &crate::InFlightView,
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

        // sticky_* slots kept at 0 (process pin FIFO removed; head + in-flight only).
        let sticky_ns = 0u64;
        let sticky_hit_n = 0u64;
        let mut resolved: HashMap<[u8; 32], Fk> =
            HashMap::with_capacity(need_vec.len() / 2);

        // Prior mega-batch(es) still in the write queue: not head yet.
        let t_inflight = Instant::now();
        if !in_flight.is_empty() {
            for t in &need_vec {
                if resolved.contains_key(t) {
                    continue;
                }
                if let Some(fk) = in_flight.get_create_fk(t) {
                    resolved.insert(*t, fk);
                }
            }
        }
        let inflight_ns = t_inflight.elapsed().as_nanos() as u64;

        let mut need_head: Vec<[u8; 32]> = Vec::new();
        for t in &need_vec {
            if !resolved.contains_key(t) {
                need_head.push(*t);
            }
        }
        let head_need_n = need_head.len() as u64;
        let mut head_hit_n = 0u64;

        // External parents missing in-flight: Shape A **fk+range** short-circuit
        // (probe + idx + identity; no denserels body). Prep loads denserels by
        // known body_range (skip tx.idx). Identity for pin is the lookup key
        // already in RAM (`resolved`) — never re-read txid.body at prep.
        let external_parent_outs: std::collections::HashMap<u64, CreatePin> =
            std::collections::HashMap::new();
        let mut external_parent_ranges: std::collections::HashMap<u64, (u64, u64)> =
            std::collections::HashMap::new();
        let mut external_parent_txids: std::collections::HashMap<u64, [u8; 32]> =
            std::collections::HashMap::with_capacity(resolved.len().saturating_add(need_head.len()));
        // Reverse map for in-flight / head binds already in `resolved`.
        for (txid, fk) in &resolved {
            if let Some(id) = fk.get() {
                external_parent_txids.insert(id, *txid);
            }
        }
        let t_head = Instant::now();
        let head_dens_ns = 0u64;
        if !need_head.is_empty() {
            need_head.sort_unstable_by_key(|txid| self.store.txs.head_primary_slot(txid));
            let hits = self.store.get_fk_by_txid_batch(&need_head)?;
            for (txid, row) in hits {
                if let Some((fk, range)) = row {
                    resolved.insert(txid, fk);
                    head_hit_n = head_hit_n.saturating_add(1);
                    if let Some(id) = fk.get() {
                        external_parent_ranges.insert(id, range);
                        external_parent_txids.insert(id, txid);
                    }
                }
            }
        }
        let head_total_ns = t_head.elapsed().as_nanos() as u64;
        let head_fk_ns = head_total_ns; // denserels not on plan stamp path
        crate::archive_phase_stats::note_head_dens_wave(0, 0);

        // Pass 3: stamp create_fk on inputs; tip spends list; build shared CreatePin.
        // Outs move into Arc once — packed pin half and batch_pin share that Arc.
        let t_stamp = Instant::now();
        let mut packed: Vec<(CreatePin, Vec<InputRecord>)> = Vec::with_capacity(work.len());
        let mut batch_pin: Vec<CreatePin> = Vec::with_capacity(work.len());
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
            // Layout denserels once; move tx+outs into shared Arc (no second outs clone).
            let dens = rbitcoin_store::denserels_from_packed_records(&tx, &inputs, &outputs);
            let pin = std::sync::Arc::new((tx, outputs, dens));
            batch_pin.push(std::sync::Arc::clone(&pin));
            packed.push((pin, inputs));
        }
        let stamp_ns = t_stamp.elapsed().as_nanos() as u64;

        let t_finish = Instant::now();
        let body_est: u64 = packed
            .iter()
            .map(|(pin, ins)| {
                let (_tx, outs, _dens) = pin.as_ref();
                (1 + TxRecord::ENCODED_LEN) as u64
                    + ins.iter().map(|x| x.encoded_len() as u64).sum::<u64>()
                    + outs.iter().map(|x| x.encoded_len() as u64).sum::<u64>()
            })
            .sum();

        let batch_creates: Vec<([u8; 32], Fk)> = packed
            .iter()
            .zip(planned_fks.iter())
            .map(|((pin, _), fk)| (pin.0.txid, *fk))
            .collect();

        // Finish is cheap: body_est + batch_creates only.
        // No `count_bodies` / far-ahead scan — Class A never leads tip (unified
        // confirm commit is the sole Class A appender); body DONTNEED lead
        // heuristics were dead work that cost O(headers) RwLock gets per plan.
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
            head_fk_ns,
            head_dens_ns,
            stamp_ns,
            finish_ns,
        );

        Ok(ArchiveWritePlan {
            packed,
            planned_fks,
            per_header_ranges,
            spends,
            batch_creates,
            external_parent_outs,
            external_parent_ranges,
            external_parent_txids,
            batch_pin,
            index_tx,
            body_est,
        })
    }

    /// **Writer / write path:** durable Class A put (body / head / spends / htxs).
    ///
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

        // Body append first (no head), then head insert — separate timers.
        // Encode from shared CreatePin + inputs (no deep outs reclone).
        let t = Instant::now();
        let got_tx_fks = self
            .store
            .put_tx_full_batch_from_pins(&plan.packed, /*index=*/ false)?;
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
                .map(|((pin, _), fk)| (pin.0.txid, *fk))
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

        // write_sticky_ns stays 0 (legacy slot; process pin FIFO removed).
        let sticky_ns = 0u64;

        // No body DONTNEED after Class A commit: Class A never leads tip, so
        // just-written pages may still be tip-hot for confirm/cache.
        // write_dontneed_ns stays 0 (legacy phase-stat slot).
        let total_ns = t0.elapsed().as_nanos() as u64;
        crate::archive_phase_stats::note_write_commit(
            total_ns,
            reserve_ns,
            body_ns,
            head_ns,
            spend_ns,
            htxs_ns,
            sticky_ns,
            0, // dontneed_ns — lead heuristic removed
            n_blocks.max(1),
        );
        Ok(())
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

    /// batch_pin Arc denserels match encode+decode layout (PR-A/B pin handoff).
    #[test]
    fn plan_batch_pin_arc_denserels_match_layout() {
        use std::collections::HashMap;
        use std::sync::Arc;
        let (dir, q) = temp_query("batch-pin-arc");
        let mut need = vec![(Fk(1), vec![coinbase_apply(1), coinbase_apply(2)])];
        let plan = q
            .archive_plan_mega_from(&mut need, 1, &crate::InFlightView::empty())
            .unwrap();
        assert_eq!(plan.batch_pin.len(), plan.planned_fks.len());
        assert_eq!(plan.batch_pin.len(), plan.packed.len());
        // packed pin half and batch_pin share the same Arc (no outs double-store).
        for ((pin_packed, _), pin) in plan.packed.iter().zip(plan.batch_pin.iter()) {
            assert!(
                Arc::ptr_eq(pin_packed, pin),
                "packed and batch_pin must share CreatePin Arc"
            );
            // plan construction: one Arc for packed + one for batch_pin.
            assert_eq!(Arc::strong_count(pin), 2);
        }
        // Simulated note_plan_ok: Arc::clone only (strong_count rises, no deep clone).
        let mut ifo: HashMap<u64, super::CreatePin> = HashMap::new();
        for (fk, pin) in plan.planned_fks.iter().zip(plan.batch_pin.iter()) {
            if let Some(id) = fk.get() {
                ifo.insert(id, Arc::clone(pin));
                assert_eq!(Arc::strong_count(pin), 3);
            }
        }
        for ((pin, ins), _) in plan.packed.iter().zip(plan.batch_pin.iter()) {
            let (tx, outs, dens) = pin.as_ref();
            let layout = rbitcoin_store::denserels_from_packed_records(tx, ins, outs);
            assert_eq!(*dens, layout);
            let mut raw = Vec::new();
            rbitcoin_store::encode_packed_tx(tx, ins, outs, &mut raw);
            let (_, _, decode_rels) =
                rbitcoin_store::decode_packed_tx_outs_with_spender_rels(&raw).unwrap();
            assert_eq!(*dens, decode_rels);
        }
        assert_eq!(ifo.len(), 2);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn archive_phase_stats_cover_plan_and_commit_wall() {
        // Drain any prior noise.
        let _ = crate::archive_phase_stats::sample_and_reset();
        let (dir, q) = temp_query("arch-phases");
        let mut need = vec![(Fk(1), vec![coinbase_apply(1), coinbase_apply(2)])];
        let plan = q
            .archive_plan_mega_from(&mut need, 1, &crate::InFlightView::empty())
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
        let (dir, q) = temp_query("plan-from");
        // Seed one body so count starts at 1.
        let seed = vec![(Fk(1), vec![coinbase_apply(1)])];
        // Need a real header_fk path: plan only needs Vec<(Fk, Vec<TxApply>)>.
        let mut need0 = seed;
        let p0 = q.archive_plan_mega_owned(&mut need0).unwrap();
        q.archive_commit_plan(p0).unwrap();
        assert_eq!(q.tx_body_count(), 1);

        // Reserve two plans as prep would with write queue depth 2.
        let empty = crate::InFlightView::empty();
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
        let (dir, q) = temp_query("inflight-parent");
        let mut need_a = vec![(Fk(1), vec![coinbase_apply(1)])];
        let empty = crate::InFlightView::empty();
        let plan_a = q.archive_plan_mega_from(&mut need_a, 1, &empty).unwrap();
        assert_eq!(plan_a.planned_fks, vec![Fk(1)]);
        let parent_txid = plan_a.batch_creates[0].0;
        let parent_fk = plan_a.batch_creates[0].1;

        // Child spends parent — not in head until plan_a commits.
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
        let mut inflight_log = crate::InFlightLog::new();
        inflight_log.note_layer(crate::InFlightLayer::from_plan_pins(
            plan_a
                .planned_fks
                .iter()
                .zip(plan_a.batch_pin.iter())
                .map(|(fk, pin)| (*fk, pin)),
        ));
        let inflight = inflight_log.snapshot();
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

    /// Prep pin identity: reverse map + range denserels (no sidefile re-read).
    #[test]
    fn plan_external_parent_txid_fills_range_denserels_pin() {
        let (dir, q) = temp_query("plan-parent-txid-ram");
        let parent = coinbase_apply(7);
        let parent_txid = parent.tx.txid;
        let fks = q
            .store
            .txs
            .put_full_batch_indexed(
                &[(parent.tx.clone(), parent.inputs.clone(), parent.outputs.clone())],
                true,
            )
            .unwrap();
        let parent_fk = fks[0];
        let pid = parent_fk.get().unwrap();
        let range = q.store.txs.body_range(parent_fk).unwrap();

        // Simulate plan stamp reverse map (txid→fk invert).
        let mut plan = super::ArchiveWritePlan::empty();
        plan.external_parent_ranges.insert(pid, range);
        plan.external_parent_txids.insert(pid, parent_txid);

        let known = plan.external_parent_txid(pid).expect("reverse map");
        let (rows, _body_ns, _dec_ns) = q
            .store
            .get_outs_denserels_by_range_batch(&[(parent_fk, range, known, vec![0])])
            .unwrap();
        let (tx, live, sparse) = rows[0].as_ref().expect("denserels");
        assert_eq!(tx.txid, parent_txid, "API sets known_txid (RAM), not sidefile");
        assert_eq!(live.len(), 1);
        assert_eq!(sparse.len(), 1);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Plan head path stamps create_fk only — denserels stay plan-local at prep;
    /// commit does not process-seed parents or creates into a pin FIFO.
    #[test]
    fn plan_head_resolved_parents_plan_local_only() {
        let (dir, q) = temp_query("plan-creates-only");
        // Parent on disk + head.
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
            .archive_plan_mega_from(&mut need, 2, &crate::InFlightView::empty())
            .expect("parent via head");
        assert_eq!(plan.planned_fks, vec![Fk(2)]);
        assert_eq!(plan.packed[0].1[0].create_fk, Fk(1));
        assert_eq!(plan.batch_creates.len(), 1);
        assert_eq!(plan.batch_creates[0].0, child_txid);
        // Plan stamp is fk+range only — denserels load at prep by offset.
        assert!(
            plan.external_parent_outs.is_empty(),
            "plan must not denserels-load head parents"
        );
        assert!(
            plan.external_parent_ranges.get(&1).is_some_and(|r| r.1 > 0),
            "plan must record Class A body range for head-resolved parent"
        );
        assert_eq!(
            plan.external_parent_txid(1),
            Some(parent_txid),
            "plan reverse map: create_fk → prev_txid from stamp resolve (RAM)"
        );

        // Commit succeeds; batch_pin retained on plan path only (dropped with plan).
        let batch_pin_len = plan.batch_pin.len();
        q.archive_commit_plan(plan).unwrap();
        assert_eq!(batch_pin_len, 1);
        assert_eq!(q.tx_body_count(), 2);

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// packed pin half and batch_pin share one CreatePin Arc (no outs double-store).
    #[test]
    fn plan_packed_and_batch_pin_share_create_pin_arc() {
        use std::sync::Arc;
        let (dir, q) = temp_query("shared-create-pin");
        let mut need = vec![(Fk(1), vec![coinbase_apply(1)])];
        let plan = q
            .archive_plan_mega_from(&mut need, 1, &crate::InFlightView::empty())
            .unwrap();
        assert_eq!(plan.packed.len(), 1);
        assert_eq!(plan.batch_pin.len(), 1);
        assert!(
            Arc::ptr_eq(&plan.packed[0].0, &plan.batch_pin[0]),
            "outs must live in one Arc shared by packed and batch_pin"
        );
        // note_plan_ok only Arc-clones into in-flight.
        let ifo_pin = Arc::clone(&plan.batch_pin[0]);
        assert!(Arc::ptr_eq(&ifo_pin, &plan.batch_pin[0]));
        assert_eq!(Arc::strong_count(&plan.batch_pin[0]), 3);
        q.archive_commit_plan(plan).unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }
}
