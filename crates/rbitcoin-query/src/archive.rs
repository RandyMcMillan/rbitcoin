//! Class A archive write path.
//!
//! Split for IBD dual-thread (prep/write may overlap with a small plan queue):
//! - **Plan** ([`Query::archive_plan_batch_owned`] / [`Query::archive_plan_batch_from`]):
//!   store **reads** — assign create fks (optionally from a reserved HWM),
//!   in-flight planned creates + `tx.head` resolve, stamp inputs.
//!   Head-miss parents use **fk-only** head resolve (no denserels on plan stamp);
//!   denserels for pin load at prep/ensure into plan-local maps only.
//! - **Commit** ([`Query::archive_commit_plan`]): store **writes** — body append,
//!   head index, header_txs. Pipeline pins stay on the plan (`batch_pin`); no
//!   process create FIFO seed.
//!
//! Overlap requires the in-flight map: a later plan batch may spend outputs from a
//! prior plan that is still queued/committing (not yet in head).

use super::*;

/// Shared immutable create pin: tx meta + full outs.
///
/// One Arc per create — plan `packed` pin half, `batch_pin`, and prep-ahead
/// `in_flight_outs` all Arc-clone this (no deep outs clone between stages).
pub type CreatePin = std::sync::Arc<(TxRecord, Vec<OutputRecord>)>;

/// Approx heap bytes for one [`CreatePin`] payload (for IBD `sizes` metering).
///
/// Counts owned output scripts + fixed record overhead — not Arc
/// refcount sharing (each strong Arc still "owns" the allocation once).
#[inline]
pub fn create_pin_approx_bytes(pin: &CreatePin) -> usize {
    let (_tx, outs) = pin.as_ref();
    let mut n = 96usize; // TxRecord + Arc shell overhead (order-of-magnitude)
    for o in outs {
        n = n.saturating_add(24).saturating_add(o.script.len());
    }
    n = n.saturating_add(outs.capacity().saturating_mul(24)); // Vec spare
    n
}

/// Sparse external parent outs for pin (need-vouts only).
///
/// `(tx, live need outs)` — **not** a full `output_count`-sized expand.
/// Transient on the plan until pin; sparse need then lives in [`crate::BatchParents`].
pub type SparseExternalPin = std::sync::Arc<(TxRecord, Vec<(u32, OutputRecord)>)>;

/// Write-ready plan batch from lookup/load to commit (writer).
///
/// Planned create fks match `txs.count()+1…` at plan time; commit fails if the
/// appender returns different fks (another writer interleave — must not happen).
#[derive(Debug)]
pub struct ArchiveWritePlan {
    /// Body-append rows: shared [`CreatePin`] (tx + outs) + inputs.
    /// Outs live once in the pin Arc (not duplicated alongside inputs).
    pub packed: Vec<(CreatePin, Vec<InputRecord>)>,
    pub planned_fks: Vec<Fk>,
    pub per_header_ranges: Vec<(Fk, Fk, u32)>,
    pub spends: Vec<([u8; 32], u32, Fk, u32)>,
    /// Creates from **this** batch only (txid→fk for in-flight / publish).
    pub batch_creates: Vec<([u8; 32], Fk)>,
    /// Pipeline-local **sparse** external parent pins (need-vouts only).
    ///
    /// Filled by ensure/prep denserels (often from
    /// [`Self::external_parent_ranges`]). **Dropped after pin**
    /// ([`Self::clear_external_parent_outs`]).
    pub external_parent_outs: crate::U64Map<SparseExternalPin>,
    /// Head-resolved external parents: create_fk → Class A `(body_off, body_len)`.
    ///
    /// Filled at plan stamp (fk+range short-circuit). Prep denserels-loads by
    /// offset (skip `tx.idx`) into [`Self::external_parent_outs`]. Still live —
    /// not obsolete after schema-13 `txid.body` (identity is separate from range).
    /// Pack-scale identity hasher ([`crate::U64Map`]): dense create_fks load evenly.
    pub external_parent_ranges: crate::U64Map<(u64, u64)>,
    /// **RAM-only** reverse of stamp resolve: create_fk id → parent `prev_txid`.
    ///
    /// Built when in-flight / head resolve binds `prev_txid → fk`.
    /// Prep pin fills schema-13 zero body `TxRecord.txid` from this map — **never**
    /// re-pread `txid.body` on the pin path.
    pub external_parent_txids: crate::U64Map<[u8; 32]>,
    /// Prep-ahead pin material for **this batch's creates**, parallel to
    /// [`Self::planned_fks`]: same [`CreatePin`] Arcs as [`Self::packed`] (refcount
    /// only). Confirm `note_lookup_ok` only `Arc::clone`s into in-flight outs.
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
            external_parent_outs: crate::U64Map::default(),
            external_parent_ranges: crate::U64Map::default(),
            external_parent_txids: crate::U64Map::default(),
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

    /// Drop pipeline-local external sparse outs after denserels pin.
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

    /// Freeze plan for write batch: drop all pin-staging maps.
    ///
    /// After this, the plan is a **commit payload** only (`packed` / `planned_fks`
    /// / headers / spends / `batch_pin`). Prep must call this (or
    /// [`Self::clear_external_parent_outs`]) before enqueue to scripts/write so
    /// batch-merge never mutates growing external HashMaps.
    pub fn freeze_after_pin(&mut self) {
        self.clear_external_parent_outs();
    }

    /// Drop headers that already have Class A body (partial-commit / retry).
    ///
    /// Returns `true` if anything remains to append. Used by
    /// [`Query::archive_commit_plan`] so a second confirm attempt after Class A
    /// succeeded but tip failed does not re-append the same txs.
    pub fn retain_headers_needing_body(
        &mut self,
        mut has_body: impl FnMut(Fk) -> Result<bool, QueryError>,
    ) -> Result<bool, QueryError> {
        if self.per_header_ranges.is_empty() {
            return Ok(!self.packed.is_empty());
        }
        let mut keep_fks: crate::U64Set = crate::U64Set::default();
        let mut new_ranges: Vec<(Fk, Fk, u32)> = Vec::with_capacity(self.per_header_ranges.len());
        for &(hfk, first, n) in &self.per_header_ranges {
            if has_body(hfk)? {
                continue;
            }
            new_ranges.push((hfk, first, n));
            let start = self
                .planned_fks
                .iter()
                .position(|f| *f == first)
                .unwrap_or(0);
            let end = start.saturating_add(n as usize).min(self.planned_fks.len());
            for f in &self.planned_fks[start..end] {
                if let Some(id) = f.get() {
                    keep_fks.insert(id);
                }
            }
        }
        if new_ranges.is_empty() {
            *self = Self::empty();
            return Ok(false);
        }
        if new_ranges.len() == self.per_header_ranges.len() {
            return Ok(true);
        }
        // Compact packed / planned_fks / batch_pin to kept create fks (order preserved).
        let old_packed = std::mem::take(&mut self.packed);
        let old_fks = std::mem::take(&mut self.planned_fks);
        let old_pin = std::mem::take(&mut self.batch_pin);
        let mut new_packed = Vec::with_capacity(keep_fks.len());
        let mut new_fks = Vec::with_capacity(keep_fks.len());
        let mut new_pin = Vec::with_capacity(keep_fks.len());
        for (i, fk) in old_fks.into_iter().enumerate() {
            let Some(id) = fk.get() else {
                continue;
            };
            if !keep_fks.contains(&id) {
                continue;
            }
            new_fks.push(fk);
            if i < old_packed.len() {
                new_packed.push(old_packed[i].clone());
            }
            if i < old_pin.len() {
                new_pin.push(std::sync::Arc::clone(&old_pin[i]));
            }
        }
        self.packed = new_packed;
        self.planned_fks = new_fks;
        self.batch_pin = new_pin;
        self.per_header_ranges = new_ranges;
        self.spends
            .retain(|(_, _, spend_fk, _)| spend_fk.get().is_some_and(|id| keep_fks.contains(&id)));
        self.batch_creates
            .retain(|(_, fk)| fk.get().is_some_and(|id| keep_fks.contains(&id)));
        // body_est is an upper bound; leave as-is (overestimate is safe for reserve).
        Ok(!self.packed.is_empty())
    }

    /// Append another **frozen** plan for write batch (height-ordered Class A).
    ///
    /// Callers must drain scripts→write in height order so `planned_fks` stay
    /// contiguous and match the sole Class A appender sequence.
    ///
    /// External staging maps are **discarded** (not union-merged): they are
    /// pin-time only and must already be empty after [`Self::freeze_after_pin`].
    /// Commit composition is pure vector concat of the frozen halves.
    pub fn append(&mut self, mut other: Self) {
        if other.is_empty() && other.per_header_ranges.is_empty() {
            return;
        }
        // Drop residual staging (should be empty after freeze_after_pin).
        other.external_parent_outs.clear();
        other.external_parent_ranges.clear();
        other.external_parent_txids.clear();
        self.external_parent_outs.clear();
        self.external_parent_ranges.clear();
        self.external_parent_txids.clear();

        self.packed.append(&mut other.packed);
        self.planned_fks.append(&mut other.planned_fks);
        self.per_header_ranges.append(&mut other.per_header_ranges);
        self.spends.append(&mut other.spends);
        self.batch_creates.append(&mut other.batch_creates);
        self.batch_pin.append(&mut other.batch_pin);
        self.index_tx |= other.index_tx;
        self.body_est = self.body_est.saturating_add(other.body_est);
    }
}

impl Query {
    pub fn archive_block(&self, header: &HeaderRecord, txs: &[TxApply]) -> Result<Fk, QueryError> {
        // Single-block path: one clone into owned plan batch.
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
    /// must not re-append Class A txs — that would orphan `header_txs`/`strong`
    /// on the previous fks (signet tip stuck at 2148: coinbase missing height).
    pub fn archive_prepared_with_fks(
        &self,
        items: &mut [(Fk, HeaderRecord, Vec<TxApply>)],
    ) -> Result<Vec<Fk>, QueryError> {
        if items.is_empty() {
            return Ok(Vec::new());
        }
        let mut header_fks = Vec::with_capacity(items.len());
        let mut need: Vec<(Fk, Vec<TxApply>)> = Vec::with_capacity(items.len());
        // First occurrence wins inside one plan batch (duplicate peer deliveries).
        let mut seen_headers = crate::FkSet::default();
        for (fk, _header, txs) in items.iter_mut() {
            header_fks.push(*fk);
            if !seen_headers.insert(*fk) {
                let _ = std::mem::take(txs);
                continue;
            }
            if self.store.header_txs.has_body(*fk)? {
                // Keep existing first_tx_fk / fence / strong alignment.
                let _ = std::mem::take(txs);
                continue;
            }
            if !txs.is_empty() {
                need.push((*fk, std::mem::take(txs)));
            }
        }
        if !need.is_empty() {
            let plan = self.archive_plan_batch_owned(&mut need)?;
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
        let mut seen_headers = crate::FkSet::default();
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
    /// (prep queue depth &gt; 1), use [`Self::archive_plan_batch_from`] with a
    /// reserved FK HWM so in-flight plans do not collide.
    pub fn archive_plan_batch_owned(
        &self,
        need: &mut [(Fk, Vec<TxApply>)],
    ) -> Result<ArchiveWritePlan, QueryError> {
        let start = self.store.txs.count().saturating_add(1);
        self.archive_plan_batch_from_store(need, start, &crate::InFlightView::empty(), None)
    }

    /// Like [`Self::archive_plan_batch_owned`], but assign create fks from
    /// `next_tx_start` (inclusive) instead of live `txs.count()+1`.
    ///
    /// IBD prep keeps a local reserved HWM: after each successful non-empty plan,
    /// advance to `planned_fks.last()+1` so the next plan batch can be planned
    /// while a prior batch is still committing (ordered writer preserves match).
    ///
    /// `in_flight`: create txid→fk from prior plans that are queued/committing
    /// but not yet in sticky/head. Required for queue depth &gt; 1 when a later
    /// batch spends a prior batch's creates.
    pub fn archive_plan_batch_from(
        &self,
        need: &mut [(Fk, Vec<TxApply>)],
        next_tx_start: u64,
        in_flight: &crate::InFlightView,
    ) -> Result<ArchiveWritePlan, QueryError> {
        self.archive_plan_batch_from_store(need, next_tx_start, in_flight, None)
    }

    /// [`Self::archive_plan_batch_from`] plus live [`crate::PipelineParentStore`]
    /// (`txid → create_fk` + range) before `tx.head`.
    pub fn archive_plan_batch_from_store(
        &self,
        need: &mut [(Fk, Vec<TxApply>)],
        next_tx_start: u64,
        in_flight: &crate::InFlightView,
        parent_store: Option<&crate::PipelineParentStore>,
    ) -> Result<ArchiveWritePlan, QueryError> {
        use std::collections::{HashMap, HashSet};
        use std::time::Instant;

        if need.is_empty() {
            return Ok(ArchiveWritePlan::empty());
        }

        let mut next_tx = next_tx_start.max(1);
        let n_headers = need.iter().filter(|(_, t)| !t.is_empty()).count() as u64;

        // Pass 1: assign contiguous create fks + batch_map (for parent resolve only).
        // No durable tx.head create reuse; no cross-block body reuse. Duplicate
        // hash must be dropped before plan (has_body / mid-pipeline).
        let t_assign = Instant::now();
        let mut batch_map: HashMap<[u8; 32], Fk> = HashMap::new();
        let mut work: Vec<(Fk, TxRecord, Vec<InputRecord>, Vec<OutputRecord>)> = Vec::new();
        let mut per_header_ranges: Vec<(Fk, Fk, u32)> = Vec::with_capacity(need.len());
        let mut spends: Vec<([u8; 32], u32, Fk, u32)> = Vec::new();
        let archive_spends = self.spend_index_enabled() && self.index_mode().is_tip();
        let index_tx = self.tx_index_enabled();

        for (header_fk, txs) in need.iter_mut() {
            if txs.is_empty() {
                continue;
            }
            // Same block hash must not reach here twice: caller drops duplicates
            // mid-pipeline / has_body. Fresh contiguous create fks for this body.
            let first_tx_fk = Fk(next_tx);
            let n_txs = txs.len() as u32;
            let mut seen_in_block: HashSet<[u8; 32]> = HashSet::with_capacity(txs.len());
            for ta in txs.drain(..) {
                let n_in = ta.inputs.len() as u32;
                let n_out = ta.outputs.len() as u32;
                // Duplicate txid in one block is a consensus violation — hard error.
                if !seen_in_block.insert(ta.tx.txid) {
                    return Err(StoreError::Corrupt(
                        "duplicate txid in block body (consensus violation)",
                    )
                    .into());
                }
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

        // sticky_* slots kept at 0 (process pin FIFO removed). Live pins are
        // consulted via PipelineParentStore::lookup_txid (same Weak lifecycle).
        let sticky_ns = 0u64;
        let sticky_hit_n = 0u64;
        let mut resolved: HashMap<[u8; 32], Fk> = HashMap::with_capacity(need_vec.len() / 2);

        // Prior plan batch(es) still in the write queue: not head yet.
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

        // Live pipeline pins (same Weak lifetime as outs share). Not in-flight
        // creates and not the killed process pin FIFO.
        let t_pin_txid = Instant::now();
        let mut pin_txid_n = 0u64;
        let mut pin_ranges: Vec<(u64, (u64, u64))> = Vec::new();
        if let Some(store) = parent_store {
            for t in &need_vec {
                if resolved.contains_key(t) {
                    continue;
                }
                if let Some((fk, range)) = store.lookup_txid(t) {
                    resolved.insert(*t, fk);
                    if let Some(id) = fk.get() {
                        pin_ranges.push((id, range));
                    }
                    pin_txid_n = pin_txid_n.saturating_add(1);
                }
            }
        }
        let pin_txid_ns = t_pin_txid.elapsed().as_nanos() as u64;

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
        let external_parent_outs: crate::U64Map<SparseExternalPin> = crate::U64Map::default();
        let mut external_parent_ranges: crate::U64Map<(u64, u64)> = crate::U64Map::default();
        let mut external_parent_txids: crate::U64Map<[u8; 32]> =
            crate::U64Map::with_capacity_and_hasher(
                resolved.len().saturating_add(need_head.len()),
                Default::default(),
            );
        // Reverse map for in-flight / pin / head binds already in `resolved`.
        for (txid, fk) in &resolved {
            if let Some(id) = fk.get() {
                external_parent_txids.insert(id, *txid);
            }
        }
        for (id, range) in pin_ranges {
            external_parent_ranges.insert(id, range);
        }
        let t_head = Instant::now();
        let head_dens_ns = 0u64;
        if !need_head.is_empty() {
            need_head.sort_unstable_by_key(|txid| self.store.txs.head_primary_slot(txid));
            let hits = self.store.get_fk_by_txid_batch_mode(
                &need_head,
                rbitcoin_store::TxidResolveMode::TipThenAny,
            )?;
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
                        // Last chance: single head probe (batch may have missed
                        // a race mid-insert). Still fail if absent.
                        // Range is filled after stamp (idx) so load never re-probes head.
                        if let Ok(Some(cfk)) = self.store.get_fk_by_txid(&inp.prev_txid) {
                            inp.create_fk = cfk;
                            resolved.insert(inp.prev_txid, cfk);
                            if let Some(id) = cfk.get() {
                                external_parent_txids.insert(id, inp.prev_txid);
                            }
                            resolved_stamp = resolved_stamp.saturating_add(1);
                        } else {
                            return Err(StoreError::Corrupt(
                                "archive: parent create_fk unresolved (contiguous batch required)",
                            ));
                        }
                    }
                }
                if archive_spends {
                    spends.push((inp.prev_txid, inp.prev_index, tx_fk, i as u32));
                }
            }
            planned_fks.push(tx_fk);
            let pin = std::sync::Arc::new((tx, outputs));
            batch_pin.push(std::sync::Arc::clone(&pin));
            packed.push((pin, inputs));
        }
        let stamp_ns = t_stamp.elapsed().as_nanos() as u64;

        // Lookup IO contract: every external parent create_fk must carry body_range
        // (tx.idx) so load denserels from tx.body by range only — never head/idx.
        // In-flight create_fk without prior head hit, and last-chance probes, used
        // to leave ranges empty → load Forbid / cold denserels miss (mainnet 961466).
        {
            let mut batch_create_ids: crate::U64Set =
                crate::U64Set::with_capacity_and_hasher(planned_fks.len(), Default::default());
            for fk in &planned_fks {
                if let Some(id) = fk.get() {
                    batch_create_ids.insert(id);
                }
            }
            let mut need_range: Vec<Fk> = Vec::new();
            let mut seen_range: crate::U64Set = crate::U64Set::default();
            for ((_pin, ins), _) in packed.iter().zip(planned_fks.iter()) {
                for inp in ins {
                    if inp.is_coinbase() || inp.prev_index == u32::MAX {
                        continue;
                    }
                    let Some(id) = inp.create_fk.get() else {
                        continue;
                    };
                    if batch_create_ids.contains(&id) {
                        continue; // same-batch: offline denserels at pin
                    }
                    if external_parent_ranges.contains_key(&id) {
                        continue;
                    }
                    // Uncommitted tip-ahead: full CreatePin already in in_flight —
                    // load pin uses offline denserels; no body_range yet.
                    if in_flight.get_out(id).is_some() {
                        continue;
                    }
                    if seen_range.insert(id) {
                        need_range.push(Fk(id));
                    }
                    // Wire reverse map may already be set; ensure identity key present.
                    if !external_parent_txids.contains_key(&id) {
                        if inp.prev_txid != [0u8; 32] {
                            external_parent_txids.insert(id, inp.prev_txid);
                        }
                    }
                }
            }
            if !need_range.is_empty() {
                let ranges = self.store.tx_body_range_batch(&need_range)?;
                for (fk, row) in need_range.into_iter().zip(ranges.into_iter()) {
                    let Some(id) = fk.get() else {
                        continue;
                    };
                    match row {
                        Some(range) => {
                            external_parent_ranges.insert(id, range);
                        }
                        None => {
                            return Err(StoreError::Corrupt(
                                "archive: external parent body_range missing after create_fk stamp (lookup idx)",
                            ));
                        }
                    }
                }
            }
        }

        let t_finish = Instant::now();
        let body_est: u64 = packed
            .iter()
            .map(|(pin, ins)| {
                let (_tx, outs) = pin.as_ref();
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
        crate::archive_phase_stats::note_pin_txid(pin_txid_n, pin_txid_ns);
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
    /// **Idempotent:** headers that already have `header_txs` body are stripped
    /// (partial prior commit after structural/tip fail). If every header is
    /// already archived, this is a no-op and returns `Ok(false)` — no second
    /// body append / fk mismatch. Returns `Ok(true)` when body was appended.
    ///
    /// Phase walls go to [`crate::archive_phase_stats`] (body vs head split).
    ///
    /// Drains write-behind `tx.head` before return. Confirm write uses
    /// [`Self::archive_commit_plan_defer_head`] to overlap drain with Class C.
    pub fn archive_commit_plan(&self, plan: ArchiveWritePlan) -> Result<bool, QueryError> {
        let committed = self.archive_commit_plan_defer_head(plan)?;
        if committed {
            let _ = self.drain_pending_tx_head()?;
        }
        Ok(committed)
    }

    /// Like [`Self::archive_commit_plan`] but leaves `tx.head` in the pending map.
    pub fn archive_commit_plan_defer_head(
        &self,
        mut plan: ArchiveWritePlan,
    ) -> Result<bool, QueryError> {
        use std::time::Instant;
        if plan.packed.is_empty() {
            return Ok(false);
        }
        // Skip re-append of headers already linked in header_txs (retry path).
        if !plan.retain_headers_needing_body(|hfk| self.store.header_txs.has_body(hfk))? {
            return Ok(false);
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

        // Head write-behind: publish pending txid→fk so resolve can hit before drain.
        let t = Instant::now();
        if plan.index_tx {
            let heads: Vec<([u8; 32], Fk)> = plan
                .packed
                .iter()
                .zip(got_tx_fks.iter())
                .map(|((pin, _), fk)| (pin.0.txid, *fk))
                .collect();
            self.store.txs.head_drain_pending_if_full()?;
            self.store.txs.head_note_pending(&heads);
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

        let total_ns = t0.elapsed().as_nanos() as u64;
        crate::archive_phase_stats::note_write_commit(
            total_ns,
            reserve_ns,
            body_ns,
            head_ns,
            spend_ns,
            htxs_ns,
            n_blocks.max(1),
        );
        Ok(true)
    }

    /// Drain write-behind `tx.head` inserts (page-grouped).
    pub fn drain_pending_tx_head(&self) -> Result<u64, QueryError> {
        Ok(self.store.txs.head_drain_pending()?)
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
        use std::sync::Arc;
        let (dir, q) = temp_query("batch-pin-arc");
        let mut need = vec![(Fk(1), vec![coinbase_apply(1), coinbase_apply(2)])];
        let plan = q
            .archive_plan_batch_from(&mut need, 1, &crate::InFlightView::empty())
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
        // Simulated note_lookup_ok: Arc::clone only (strong_count rises, no deep clone).
        let mut ifo: crate::U64Map<super::CreatePin> = crate::U64Map::default();
        for (fk, pin) in plan.planned_fks.iter().zip(plan.batch_pin.iter()) {
            if let Some(id) = fk.get() {
                ifo.insert(id, Arc::clone(pin));
                assert_eq!(Arc::strong_count(pin), 3);
            }
        }
        for ((pin, ins), _) in plan.packed.iter().zip(plan.batch_pin.iter()) {
            let (tx, outs) = pin.as_ref();
            let mut raw = Vec::new();
            rbitcoin_store::encode_packed_tx(tx, ins, outs, &mut raw);
            let (meta, dec_outs, _) =
                rbitcoin_store::decode_packed_tx_outs_with_spender_rels(&raw).unwrap();
            assert_eq!(meta.output_count as usize, dec_outs.len());
            assert_eq!(outs.len(), dec_outs.len());
        }
        assert_eq!(ifo.len(), 2);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn archive_phase_stats_cover_plan_and_commit_wall() {
        // Exclusive lock so a parallel sample_and_reset cannot steal this
        // window (llvm-cov / cargo test --workspace).
        crate::archive_phase_stats::with_exclusive(|| {
            let _ = crate::archive_phase_stats::sample_and_reset();
            let (dir, q) = temp_query("arch-phases");
            let mut need = vec![(Fk(1), vec![coinbase_apply(1), coinbase_apply(2)])];
            let plan = q
                .archive_plan_batch_from(&mut need, 1, &crate::InFlightView::empty())
                .unwrap();
            assert_eq!(plan.planned_fks.len(), 2);
            q.archive_commit_plan(plan).unwrap();
            let s = crate::archive_phase_stats::sample_and_reset();
            // Counts always fire; Instant slices can be 0 ns on a coarse clock.
            assert!(
                s.blocks >= 1 || s.prep_assign_ns > 0 || s.prep_stamp_ns > 0,
                "plan noted"
            );
            assert!(s.write_blocks >= 1 || s.write_total_ns > 0, "commit total");
            assert!(s.write_blocks >= 1 || s.write_body_ns > 0, "body put timed");
            let wsum = s.write_phases_sum_ns();
            // Sequential Instant slices: sum ≤ total + small clock noise.
            assert!(
                wsum <= s.write_total_ns.saturating_add(200_000),
                "write sum {} ≫ total {}",
                wsum,
                s.write_total_ns
            );
            let _ = std::fs::remove_dir_all(&dir);
        });
    }

    #[test]
    fn plan_from_reserves_fks_for_overlap_then_commit_in_order() {
        let (dir, q) = temp_query("plan-from");
        // Seed one body so count starts at 1.
        let seed = vec![(Fk(1), vec![coinbase_apply(1)])];
        // Need a real header_fk path: plan only needs Vec<(Fk, Vec<TxApply>)>.
        let mut need0 = seed;
        let p0 = q.archive_plan_batch_owned(&mut need0).unwrap();
        q.archive_commit_plan(p0).unwrap();
        assert_eq!(q.tx_body_count(), 1);

        // Reserve two plans as prep would with write queue depth 2.
        let empty = crate::InFlightView::empty();
        let mut next = q.tx_body_count() + 1;
        let mut need_a = vec![(Fk(10), vec![coinbase_apply(10), coinbase_apply(11)])];
        let plan_a = q
            .archive_plan_batch_from(&mut need_a, next, &empty)
            .unwrap();
        assert_eq!(plan_a.planned_fks, vec![Fk(2), Fk(3)]);
        next = plan_a.planned_fks.last().unwrap().0 + 1;
        assert_eq!(next, 4);

        let mut need_b = vec![(Fk(20), vec![coinbase_apply(20)])];
        let plan_b = q
            .archive_plan_batch_from(&mut need_b, next, &empty)
            .unwrap();
        assert_eq!(plan_b.planned_fks, vec![Fk(4)]);
        // Durable count still 1 until commit.
        assert_eq!(q.tx_body_count(), 1);

        q.archive_commit_plan(plan_a).unwrap();
        assert_eq!(q.tx_body_count(), 3);
        q.archive_commit_plan(plan_b).unwrap();
        assert_eq!(q.tx_body_count(), 4);

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Overlapping plan must resolve parents from a prior uncommitted plan batch.
    /// Without `in_flight`, this is the "parent create_fk unresolved" corruption.
    #[test]
    fn overlap_plan_resolves_parent_via_inflight_creates() {
        let (dir, q) = temp_query("inflight-parent");
        let mut need_a = vec![(Fk(1), vec![coinbase_apply(1)])];
        let empty = crate::InFlightView::empty();
        let plan_a = q.archive_plan_batch_from(&mut need_a, 1, &empty).unwrap();
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
            .archive_plan_batch_from(&mut need_b, 2, &empty)
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
            .archive_plan_batch_from(&mut need_b, 2, &inflight)
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
                &[(
                    parent.tx.clone(),
                    parent.inputs.clone(),
                    parent.outputs.clone(),
                )],
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
            .get_outs_by_range_batch(&[(parent_fk, range, known, vec![0])])
            .unwrap();
        let (tx, live, sparse) = rows[0].as_ref().expect("denserels");
        assert_eq!(
            tx.txid, parent_txid,
            "API sets known_txid (RAM), not sidefile"
        );
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
            .archive_plan_batch_from(&mut need, 2, &crate::InFlightView::empty())
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

    /// Creates-only in_flight (txid→fk, no denserels outs) must still get
    /// body_range via idx so load denserels-by-range works (mainnet 961466 class).
    #[test]
    fn plan_inflight_creates_only_fills_parent_body_range() {
        let (dir, q) = temp_query("plan-inflight-range");
        let parent = coinbase_apply(1);
        let parent_txid = parent.tx.txid;
        q.store
            .txs
            .put_full_batch_indexed(
                &[(parent.tx, parent.inputs, parent.outputs)],
                /*index=*/ true,
            )
            .unwrap();
        // Creates-only layer: fk known, no CreatePin outs (archived mid-head race).
        let mut log = crate::InFlightLog::new();
        log.note_layer(crate::InFlightLayer::from_txid_fks([(parent_txid, Fk(1))]));
        let ifo = log.snapshot();
        assert!(ifo.get_out(1).is_none());

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
            .archive_plan_batch_from(&mut need, 2, &ifo)
            .expect("parent via creates-only in_flight");
        assert_eq!(plan.packed[0].1[0].create_fk, Fk(1));
        assert!(
            plan.external_parent_ranges.get(&1).is_some_and(|r| r.1 > 0),
            "creates-only in_flight must still stamp body_range for load denserels"
        );
        assert_eq!(plan.external_parent_txid(1), Some(parent_txid));
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// packed pin half and batch_pin share one CreatePin Arc (no outs double-store).
    #[test]
    fn plan_packed_and_batch_pin_share_create_pin_arc() {
        use std::sync::Arc;
        let (dir, q) = temp_query("shared-create-pin");
        let mut need = vec![(Fk(1), vec![coinbase_apply(1)])];
        let plan = q
            .archive_plan_batch_from(&mut need, 1, &crate::InFlightView::empty())
            .unwrap();
        assert_eq!(plan.packed.len(), 1);
        assert_eq!(plan.batch_pin.len(), 1);
        assert!(
            Arc::ptr_eq(&plan.packed[0].0, &plan.batch_pin[0]),
            "outs must live in one Arc shared by packed and batch_pin"
        );
        // note_lookup_ok only Arc-clones into in-flight.
        let ifo_pin = Arc::clone(&plan.batch_pin[0]);
        assert!(Arc::ptr_eq(&ifo_pin, &plan.batch_pin[0]));
        assert_eq!(Arc::strong_count(&plan.batch_pin[0]), 3);
        q.archive_commit_plan(plan).unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Live pipeline pin supplies create_fk + range without `tx.head`.
    #[test]
    fn archive_plan_batch_from_store_hits_pin_txid() {
        use crate::{BatchParents, PipelineParentStore};
        use std::sync::Arc;
        let (dir, q) = temp_query("pin-txid-stamp");
        let parent_txid = {
            let mut t = [0u8; 32];
            t[0] = 0x11;
            t
        };
        let store = Arc::new(PipelineParentStore::new());
        let mut bp = BatchParents::with_store(Arc::clone(&store), 1);
        bp.insert_owned(
            Fk(99),
            TxRecord {
                txid: parent_txid,
                version: 1,
                locktime: 0,
                input_start_fk: Fk::NULL,
                input_count: 1,
                output_start_fk: Fk::NULL,
                output_count: 1,
            },
            vec![(0, OutputRecord::unspent(1, vec![0x51]))],
            vec![0],
            Some(false),
            Some((5000, 40)),
            Vec::new(),
        );
        bp.publish_to_store();
        let _keep = bp;

        let child_txid = {
            let mut t = [0u8; 32];
            t[0] = 0x22;
            t
        };
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
        let mut need = vec![(Fk(1), vec![child])];
        crate::archive_phase_stats::with_exclusive(|| {
            let _ = crate::archive_phase_stats::sample_and_reset();
            let plan = q
                .archive_plan_batch_from_store(
                    &mut need,
                    1,
                    &crate::InFlightView::empty(),
                    Some(store.as_ref()),
                )
                .expect("pin-txid stamp");
            assert_eq!(plan.packed[0].1[0].create_fk, Fk(99));
            assert_eq!(plan.external_parent_ranges.get(&99), Some(&(5000, 40)));
            let mix = crate::archive_phase_stats::sample_and_reset();
            assert_eq!(mix.pin_txid_n, 1);
            assert_eq!(mix.head_need, 0);
        });
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Freeze + append: batch-merge is vector concat of frozen commit halves;
    /// external staging maps are dropped (not union-mutated).
    #[test]
    fn freeze_after_pin_then_append_preserves_fk_order() {
        use std::sync::Arc;
        let (dir, q) = temp_query("freeze-append");
        let mut need_a = vec![(Fk(1), vec![coinbase_apply(1)])];
        let mut plan_a = q
            .archive_plan_batch_from(&mut need_a, 1, &crate::InFlightView::empty())
            .unwrap();
        // Simulate residual staging (must not survive freeze/append).
        plan_a.external_parent_outs.insert(
            99,
            Arc::new((
                coinbase_apply(99).tx,
                vec![(0, OutputRecord::unspent(1, vec![0x51]))],
            )),
        );
        plan_a.external_parent_ranges.insert(99, (0, 1));
        plan_a.external_parent_txids.insert(99, [9u8; 32]);
        plan_a.freeze_after_pin();
        assert!(plan_a.external_parent_outs.is_empty());
        assert!(plan_a.external_parent_ranges.is_empty());
        assert!(plan_a.external_parent_txids.is_empty());

        let mut need_b = vec![(Fk(2), vec![coinbase_apply(2), coinbase_apply(3)])];
        let mut plan_b = q
            .archive_plan_batch_from(&mut need_b, 2, &crate::InFlightView::empty())
            .unwrap();
        plan_b.external_parent_outs.insert(
            88,
            Arc::new((
                coinbase_apply(88).tx,
                vec![(0, OutputRecord::unspent(1, vec![0x51]))],
            )),
        );
        plan_b.freeze_after_pin();

        let fks_a = plan_a.planned_fks.clone();
        let fks_b = plan_b.planned_fks.clone();
        assert_eq!(fks_a.len(), 1);
        assert_eq!(fks_b.len(), 2);

        plan_a.append(plan_b);
        assert!(
            plan_a.external_parent_outs.is_empty(),
            "append must not keep external staging maps"
        );
        assert_eq!(plan_a.planned_fks.len(), 3);
        assert_eq!(&plan_a.planned_fks[..1], &fks_a[..]);
        assert_eq!(&plan_a.planned_fks[1..], &fks_b[..]);
        assert_eq!(plan_a.packed.len(), 3);
        assert_eq!(plan_a.batch_pin.len(), 3);
        // Contiguous Class A commit of the merged frozen plan.
        assert!(q.archive_commit_plan(plan_a).unwrap());
        assert_eq!(q.tx_body_count(), 3);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Second commit after header_txs is linked must not re-append body (partial
    /// confirm retry / crash recovery).
    #[test]
    fn archive_commit_plan_idempotent_when_header_already_has_body() {
        use rbitcoin_store::HeaderRecord;

        let (dir, q) = temp_query("arch-idempotent");
        let header = HeaderRecord {
            prev_fk: Fk::NULL,
            version: 1,
            timestamp: 1,
            bits: 1,
            nonce: 1,
            merkle_root: [1u8; 32],
            hash: [2u8; 32],
        };
        let hfk = q.ensure_header(&header).unwrap();
        let mut need = vec![(hfk, vec![coinbase_apply(42)])];
        let plan = q.archive_plan_batch_owned(&mut need).unwrap();
        assert!(!plan.is_empty());
        assert!(q.archive_commit_plan(plan).unwrap(), "first commit appends");
        let n = q.tx_body_count();
        assert!(n >= 1);
        assert!(q.store().header_txs.has_body(hfk).unwrap());

        // Rebuild a plan as if lookup incorrectly re-planned the same header.
        let mut need2 = vec![(hfk, vec![coinbase_apply(42)])];
        let plan2 = q.archive_plan_batch_owned(&mut need2).unwrap();
        // filter_need empties txs when has_body — plan may be empty. Force a
        // non-empty plan by planning against a fresh need then swapping ranges.
        if plan2.is_empty() {
            // Production path: archive_filter_need_bodies clears need → empty plan.
            // Commit empty is no-op.
            assert!(!q.archive_commit_plan(plan2).unwrap());
        } else {
            assert!(
                !q.archive_commit_plan(plan2).unwrap(),
                "second commit must skip re-append"
            );
        }
        assert_eq!(
            q.tx_body_count(),
            n,
            "tx body count must not grow on idempotent re-commit"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn retain_headers_needing_body_strips_archived() {
        let mut plan = super::ArchiveWritePlan::empty();
        plan.planned_fks = vec![Fk(1), Fk(2), Fk(3)];
        plan.per_header_ranges = vec![(Fk(10), Fk(1), 2), (Fk(20), Fk(3), 1)];
        // Minimal packed rows so retain can compact.
        let dummy_pin = |i: u8| {
            std::sync::Arc::new((
                TxRecord {
                    txid: [i; 32],
                    version: 1,
                    locktime: 0,
                    input_start_fk: Fk::NULL,
                    input_count: 0,
                    output_start_fk: Fk::NULL,
                    output_count: 0,
                },
                Vec::new(),
            ))
        };
        plan.packed = vec![
            (dummy_pin(1), Vec::new()),
            (dummy_pin(2), Vec::new()),
            (dummy_pin(3), Vec::new()),
        ];
        plan.batch_pin = vec![dummy_pin(1), dummy_pin(2), dummy_pin(3)];
        plan.spends = vec![([0u8; 32], 0, Fk(1), 0), ([0u8; 32], 0, Fk(3), 0)];
        // Header 10 already has body; 20 needs body.
        let keep = plan
            .retain_headers_needing_body(|hfk| Ok(hfk == Fk(10)))
            .unwrap();
        assert!(keep);
        assert_eq!(plan.per_header_ranges, vec![(Fk(20), Fk(3), 1)]);
        assert_eq!(plan.planned_fks, vec![Fk(3)]);
        assert_eq!(plan.packed.len(), 1);
        assert_eq!(plan.spends.len(), 1);
        assert_eq!(plan.spends[0].2, Fk(3));
    }

    /// retain_headers edges: empty ranges, all have body, no-op full keep, null fks.
    #[test]
    fn retain_headers_needing_body_edge_matrix() {
        let dummy_pin = |i: u8| {
            std::sync::Arc::new((
                TxRecord {
                    txid: [i; 32],
                    version: 1,
                    locktime: 0,
                    input_start_fk: Fk::NULL,
                    input_count: 0,
                    output_start_fk: Fk::NULL,
                    output_count: 0,
                },
                Vec::new(),
            ))
        };

        // No per_header_ranges: keep iff packed non-empty.
        let mut empty_ranges = super::ArchiveWritePlan::empty();
        empty_ranges.packed = vec![(dummy_pin(1), Vec::new())];
        assert!(empty_ranges
            .retain_headers_needing_body(|_| Ok(false))
            .unwrap());
        let mut empty_all = super::ArchiveWritePlan::empty();
        assert!(!empty_all
            .retain_headers_needing_body(|_| Ok(false))
            .unwrap());

        // All headers already have body → clear plan, false.
        let mut all_have = super::ArchiveWritePlan::empty();
        all_have.planned_fks = vec![Fk(1), Fk(2)];
        all_have.per_header_ranges = vec![(Fk(10), Fk(1), 1), (Fk(20), Fk(2), 1)];
        all_have.packed = vec![(dummy_pin(1), Vec::new()), (dummy_pin(2), Vec::new())];
        all_have.batch_pin = vec![dummy_pin(1), dummy_pin(2)];
        all_have.batch_creates = vec![([1u8; 32], Fk(1)), ([2u8; 32], Fk(2))];
        assert!(!all_have.retain_headers_needing_body(|_| Ok(true)).unwrap());
        assert!(all_have.is_empty());
        assert!(all_have.per_header_ranges.is_empty());

        // Full keep (no strip): true without compact.
        let mut full = super::ArchiveWritePlan::empty();
        full.planned_fks = vec![Fk(1)];
        full.per_header_ranges = vec![(Fk(10), Fk(1), 1)];
        full.packed = vec![(dummy_pin(1), Vec::new())];
        full.batch_pin = vec![dummy_pin(1)];
        assert!(full.retain_headers_needing_body(|_| Ok(false)).unwrap());
        assert_eq!(full.planned_fks, vec![Fk(1)]);

        // Null planned fk skipped during compact.
        let mut with_null = super::ArchiveWritePlan::empty();
        with_null.planned_fks = vec![Fk::NULL, Fk(5)];
        with_null.per_header_ranges = vec![(Fk(1), Fk::NULL, 1), (Fk(2), Fk(5), 1)];
        with_null.packed = vec![(dummy_pin(0), Vec::new()), (dummy_pin(5), Vec::new())];
        with_null.batch_pin = vec![dummy_pin(0), dummy_pin(5)];
        // Header 1 already has body; header 2 needs body (first=Fk(5)).
        assert!(with_null
            .retain_headers_needing_body(|hfk| Ok(hfk == Fk(1)))
            .unwrap());
        assert_eq!(with_null.planned_fks, vec![Fk(5)]);

        // external_parent_txid / clear_external / append empty other.
        let mut plan = super::ArchiveWritePlan::empty();
        plan.external_parent_txids.insert(7, [0xab; 32]);
        assert_eq!(plan.external_parent_txid(7), Some([0xab; 32]));
        assert!(plan.external_parent_txid(8).is_none());
        plan.clear_external_parent_outs();
        assert!(plan.external_parent_txids.is_empty());
        plan.append(super::ArchiveWritePlan::empty());
        assert!(plan.is_empty());
    }
}
