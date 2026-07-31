//! Class A archive write path.
//!
//! Split for IBD dual-thread (prep/write may overlap with a small plan queue):
//! - **Plan** ([`Query::archive_plan_mega_owned`] / [`Query::archive_plan_mega_from`]):
//!   store **reads** — assign create fks (optionally from a reserved HWM),
//!   **CreateResidency** + in-flight planned creates + `tx.head` resolve, stamp inputs.
//!   Head-miss parents use Shape A resolve (Prefix33 select + one denserels)
//!   into [`ArchiveWritePlan::external_parent_outs`] (pipeline-local; not FIFO).
//! - **Commit** ([`Query::archive_commit_plan`]): store **writes** — body append,
//!   head index, header_txs, residency denserels seed.
//! - **Prewarm** ([`Query::archive_residency_prewarm`]): startup cache fill —
//!   last-N `body_txid_range` ranges, bounded denserels (`OutsDenserels`), and
//!   tip-ahead confirm header plans — before the IBD pipeline starts.
//!
//! Overlap requires the in-flight map: a later mega-batch may spend outputs from a
//! prior plan that is still queued/committing (not yet in residency/head).

use super::*;

/// Cap on denserels body loads during startup prewarm (not full create_cap).
///
/// Full denserels for 8M creates is multi-minute IO + RAM; mid/late mainnet pin
/// miss rate is structural (~35–50%). Bound recent denserels so first batches
/// are warm without a multi-GiB startup tax.
pub const PREWARM_DENSERELS_MAX_CREATES: usize = 262_144;

/// Max tip-ahead header plans seeded at startup (MTP / header_txs cache).
pub const PREWARM_HEADER_PLANS_MAX: usize = 4_096;

/// Stats from [`Query::archive_residency_prewarm`].
#[derive(Debug, Default, Clone, Copy)]
pub struct ResidencyPrewarmStats {
    /// Range-only CreateResidency rows (fk/txid/body_range).
    pub ranges: usize,
    /// Creates that received denserels/outs this prewarm.
    pub denserels_creates: usize,
    /// Residency `total_outs` after denserels phase.
    pub denserels_outs: u64,
    /// Confirm header plans cached (tip-ahead Class A prefix).
    pub header_plans: usize,
    pub range_ms: u64,
    pub denserels_ms: u64,
    pub headers_ms: u64,
    /// End-to-end wall.
    pub ms: u64,
}

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
    /// Creates from **this** batch only — seeded into CreateResidency after head.
    /// Parents resolved via durable `tx.head` are **not** published (they thrash
    /// the FIFO with long-tail keys and evict recent creates).
    pub batch_creates: Vec<([u8; 32], Fk)>,
    /// External parents that missed CreateResidency at plan (head-resolved): full
    /// outs+denserels loaded once for prep pin. **Pipeline-local** — not written
    /// into CreateResidency FIFO (long-tail thrash). Prep must consult this map
    /// before cold denserels IO (a create_fk residency miss is also a denserels miss).
    pub external_parent_outs: std::collections::HashMap<
        u64,
        (TxRecord, Vec<OutputRecord>, Vec<u32>),
    >,
    /// Prep-ahead pin material for **this batch's creates**, parallel to
    /// [`Self::planned_fks`]: `Arc<(TxRecord, outs, denserels)>`.
    ///
    /// Built once at plan finish via layout denserels. Confirm `note_plan_ok`
    /// only `Arc::clone`s into in-flight outs (no deep clone / re-pack).
    pub batch_pin: Vec<std::sync::Arc<(TxRecord, Vec<OutputRecord>, Vec<u32>)>>,
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
            batch_creates: Vec::new(),
            external_parent_outs: std::collections::HashMap::new(),
            batch_pin: Vec::new(),
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
        // CreateResidency txid→fk (sole hot map). Counter/timer still named sticky_*
        // in archive_phase_stats; IBD logs label them res_txid / res_txid_hit.
        let mut resolved: HashMap<[u8; 32], Fk> =
            HashMap::with_capacity(need_vec.len() / 2);
        let mut sticky_hit_n = 0u64;
        for t in &need_vec {
            if let Some(fk) = self.create_residency.lookup_fk_by_txid(t) {
                resolved.insert(*t, fk);
                sticky_hit_n = sticky_hit_n.saturating_add(1);
            }
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

        let mut need_head: Vec<[u8; 32]> = Vec::new();
        for t in &need_vec {
            if !resolved.contains_key(t) {
                need_head.push(*t);
            }
        }
        let head_need_n = need_head.len() as u64;
        let mut head_hit_n = 0u64;

        // External parents that miss CreateResidency (head path). Shape A:
        // Prefix33 select + **one** denserels body per winner into
        // pipeline-local `external_parent_outs` (not residency FIFO).
        //
        // Timers: head_fk = wall − dens_ns; head_dens = denserels-wave ns from
        // the fused resolve (single-cand denserels-only + multi-cand dens wave).
        let mut external_parent_outs: std::collections::HashMap<
            u64,
            (TxRecord, Vec<OutputRecord>, Vec<u32>),
        > = std::collections::HashMap::new();
        let mut dens_fks_n = 0u64;
        let mut dens_bytes = 0u64;
        let t_head = Instant::now();
        let mut head_dens_ns = 0u64;
        if !need_head.is_empty() {
            need_head.sort_unstable_by_key(|txid| self.store.txs.head_primary_slot(txid));
            let (hits, dens_ns) = self.store.get_fk_and_outs_by_txid_batch(&need_head)?;
            head_dens_ns = dens_ns;
            for (txid, row) in hits {
                if let Some((fk, outs_opt)) = row {
                    resolved.insert(txid, fk);
                    head_hit_n = head_hit_n.saturating_add(1);
                    if let Some((tx, outs, dens)) = outs_opt {
                        dens_fks_n = dens_fks_n.saturating_add(1);
                        dens_bytes = dens_bytes
                            .saturating_add(dens.len() as u64 * 4)
                            .saturating_add(
                                outs.iter().map(|o| o.script.len() as u64 + 16).sum::<u64>(),
                            );
                        if let Some(id) = fk.get() {
                            external_parent_outs.insert(id, (tx, outs, dens));
                        }
                    }
                }
            }
        }
        let head_total_ns = t_head.elapsed().as_nanos() as u64;
        let head_fk_ns = head_total_ns.saturating_sub(head_dens_ns);
        crate::archive_phase_stats::note_head_dens_wave(dens_fks_n, dens_bytes);

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

        let batch_creates: Vec<([u8; 32], Fk)> = packed
            .iter()
            .zip(planned_fks.iter())
            .map(|((tx, _, _), fk)| (tx.txid, *fk))
            .collect();

        // Prep-ahead pin denserels once (layout offsets = Class A packing).
        // Arc so note_plan_ok only bumps refcounts into in_flight_outs.
        let batch_pin: Vec<std::sync::Arc<(TxRecord, Vec<OutputRecord>, Vec<u32>)>> = packed
            .iter()
            .map(|(tx, ins, outs)| {
                let dens = rbitcoin_store::denserels_from_packed_records(tx, ins, outs);
                std::sync::Arc::new((tx.clone(), outs.clone(), dens))
            })
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
            batch_pin,
            index_tx,
            body_est,
            advise_dont_need,
        })
    }

    /// Startup cache prewarm: ranges + bounded denserels + tip-ahead header plans.
    ///
    /// Call once before IBD so cold restarts avoid head-resolve storms and the
    /// first confirm batches are not fully cold.
    ///
    /// Phases (all process-local; no durable writes):
    /// 1. **Ranges** — last `create_cap` Class A via sequential `tx.idx`
    ///    (`body_txid_range` + body range batch). Cheap, fills create_cap.
    /// 2. **Header plans** — tip+1..tip+N work path with Class A bodies into
    ///    [`ConfirmParentCache`] (header + `header_txs` + prev_hash).
    /// 3. **Denserels** — `OutsDenserels` body decode into CreateResidency for
    ///    tip-window creates first, then newest Class A fill. Stops at
    ///    [`PREWARM_DENSERELS_MAX_CREATES`] or ~⅞ of `out_cap` (leave headroom
    ///    for commit seed). Never loads full create_cap denserels.
    pub fn archive_residency_prewarm(&self) -> Result<ResidencyPrewarmStats, QueryError> {
        use std::time::Instant;
        let t0 = Instant::now();
        let mut st = ResidencyPrewarmStats::default();

        // No long-lived cache: skip multi‑GiB fill; commit res_seed + pin still
        // populate a small FIFO for the in-flight window.
        if !self.create_residency.cache_enabled() {
            st.ms = t0.elapsed().as_millis() as u64;
            return Ok(st);
        }

        // ── 1. Range-only CreateResidency fill ────────────────────────────
        let t_range = Instant::now();
        let cap = self.create_residency.size_stats().1;
        let n = self.store.txs.count();
        if n > 0 && cap > 0 {
            // Last `min(cap, n)` fks: (n-cap+1)..=n (1-based).
            let start = n.saturating_sub(cap as u64).saturating_add(1).max(1);
            const CHUNK: u64 = 8192;
            let mut cur = start;
            while cur <= n {
                let end = (cur + CHUNK - 1).min(n);
                let txids = self.store.txs.body_txid_range(cur, end)?;
                debug_assert_eq!(txids.len() as u64, end - cur + 1);
                let fks: Vec<Fk> = (cur..=end).map(Fk).collect();
                let ranges = self.store.tx_body_range_batch(&fks)?;
                for ((txid, fk), range) in txids
                    .into_iter()
                    .zip(fks.into_iter())
                    .zip(ranges.into_iter())
                {
                    let Some((off, len)) = range else {
                        continue;
                    };
                    self.create_residency
                        .insert_fk_txid_range(fk, txid, Some((off, len)));
                    st.ranges = st.ranges.saturating_add(1);
                }
                cur = end + 1;
            }
        }
        st.range_ms = t_range.elapsed().as_millis() as u64;

        // ── 2. Tip-ahead header plans ─────────────────────────────────────
        let t_hdr = Instant::now();
        let mut tip_window_fks: Vec<Fk> = Vec::new();
        if let Some(tip_h) = self.tip_height().map(|h| h.0) {
            if let Some(tip_fk) = self.tip_header_fk()? {
                let tip_rec = self.store.get_header(tip_fk)?;
                let path = self.resume_work_path_after_tip(
                    tip_rec.hash,
                    tip_h,
                    PREWARM_HEADER_PLANS_MAX,
                )?;
                // Seed tip so advance GC baseline matches confirmed tip.
                self.confirm_parents.advance_tip(tip_h);
                for e in &path {
                    if !e.has_body {
                        // Contiguous Class A prefix only — stop at first hole.
                        break;
                    }
                    if !self.store.header_txs.has_body(e.header_fk)? {
                        break;
                    }
                    let Some(tx_fks) = self.store.header_txs.get_list(e.header_fk)? else {
                        break;
                    };
                    if tx_fks.is_empty() {
                        break;
                    }
                    let header_rec = self.store.get_header(e.header_fk)?;
                    let prev_hash = if header_rec.prev_fk.is_null() {
                        [0u8; 32]
                    } else {
                        match self.store.get_header(header_rec.prev_fk) {
                            Ok(prev) => prev.hash,
                            Err(_) => break,
                        }
                    };
                    self.confirm_parents.put_header_plan(
                        e.height,
                        e.header_fk,
                        header_rec,
                        tx_fks.clone(),
                        prev_hash,
                    );
                    self.confirm_parents.ensure_plan(e.height, e.hash);
                    tip_window_fks.extend(tx_fks.into_iter().filter(|f| f.get().is_some()));
                    st.header_plans = st.header_plans.saturating_add(1);
                }
            }
        }
        st.headers_ms = t_hdr.elapsed().as_millis() as u64;

        // ── 3. Bounded denserels (tip-window first, then recent fill) ──────
        let t_den = Instant::now();
        let mut denserels_budget = PREWARM_DENSERELS_MAX_CREATES;
        let out_stop = {
            let (_, _, _, out_cap) = self.create_residency.size_stats();
            // Leave ~⅛ out_cap for commit seed; never thrash the out FIFO full.
            out_cap.saturating_mul(7) / 8
        };

        // Prefer creates that first confirm batches will touch.
        tip_window_fks.sort_unstable_by_key(|f| f.0);
        tip_window_fks.dedup();
        denserels_budget = denserels_budget.saturating_sub(self.prewarm_denserels_fks(
            &tip_window_fks,
            denserels_budget,
            out_stop,
            &mut st,
        )?);

        // Fill remaining budget with newest Class A (oldest→newest in window so
        // newest lands last on outs_order and survives out slim longer).
        if denserels_budget > 0 && n > 0 {
            let fill_n = denserels_budget.min(n as usize) as u64;
            let start = n.saturating_sub(fill_n).saturating_add(1).max(1);
            const CHUNK: u64 = 4096;
            let mut cur = start;
            while cur <= n && denserels_budget > 0 {
                let (_, _, total_outs, _) = self.create_residency.size_stats();
                if total_outs >= out_stop {
                    break;
                }
                let end = (cur + CHUNK - 1).min(n);
                let fks: Vec<Fk> = (cur..=end).map(Fk).collect();
                let used = self.prewarm_denserels_fks(&fks, denserels_budget, out_stop, &mut st)?;
                denserels_budget = denserels_budget.saturating_sub(used);
                cur = end + 1;
            }
        }
        st.denserels_ms = t_den.elapsed().as_millis() as u64;
        let (_, _, total_outs, _) = self.create_residency.size_stats();
        st.denserels_outs = total_outs;
        st.ms = t0.elapsed().as_millis() as u64;
        Ok(st)
    }

    /// Load denserels for `fks` via combined path until budget or out stop.
    /// Returns how many creates successfully received denserels this call.
    fn prewarm_denserels_fks(
        &self,
        fks: &[Fk],
        budget: usize,
        out_stop: u64,
        st: &mut ResidencyPrewarmStats,
    ) -> Result<usize, QueryError> {
        if fks.is_empty() || budget == 0 {
            return Ok(0);
        }
        let (_, _, total_outs, _) = self.create_residency.size_stats();
        if total_outs >= out_stop {
            return Ok(0);
        }
        // Skip creates that already hold denserels (commit seed / prior chunk).
        let need: Vec<Fk> = fks
            .iter()
            .copied()
            .filter(|fk| fk.get().is_some() && !self.create_residency.has_outs(*fk))
            .take(budget)
            .collect();
        if need.is_empty() {
            return Ok(0);
        }
        const CHUNK: usize = 4096;
        let mut loaded = 0usize;
        for chunk in need.chunks(CHUNK) {
            let (_, _, total_outs, _) = self.create_residency.size_stats();
            if total_outs >= out_stop {
                break;
            }
            let creates = crate::combined_stage::load_creates_once(
                &self.store,
                &self.create_residency,
                chunk,
                rbitcoin_store::IdxBodyMode::OutsDenserels,
            )?;
            loaded = loaded.saturating_add(creates.len());
            st.denserels_creates = st.denserels_creates.saturating_add(creates.len());
        }
        Ok(loaded)
    }

    /// **Writer / write path:** durable Class A put + residency denserels seed.
    ///
    /// No residency/head **lookups** — only appends and residency inserts.
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

        // Publish **this batch's creates only** into CreateResidency after head is
        // durable. Seed denserels offline so prep(N+1) pin hits without Class A
        // body re-read. Timer logged as res_seed (write_sticky_ns atom).
        //
        // Prefer plan-time `batch_pin` (layout denserels) — no secret encode+decode.
        // Fallback: layout denserels from packed records (still no full re-pack).
        let t = Instant::now();
        let ranges = self.store.tx_body_range_batch(&got_tx_fks)?;
        let use_pin = plan.batch_pin.len() == plan.packed.len()
            && plan.batch_pin.len() == got_tx_fks.len();
        if use_pin {
            for ((pin, fk), range) in plan
                .batch_pin
                .iter()
                .zip(got_tx_fks.iter())
                .zip(ranges.into_iter())
            {
                let body_range = match range {
                    Some((off, len)) if len > 0 => Some((off, len)),
                    _ => None,
                };
                let (tx, outs, denserels) = pin.as_ref();
                self.create_residency.put_outs(
                    *fk,
                    tx.clone(),
                    outs.clone(),
                    denserels.clone(),
                    body_range,
                );
            }
        } else {
            for (((tx, ins, outs), fk), range) in plan
                .packed
                .iter()
                .zip(got_tx_fks.iter())
                .zip(ranges.into_iter())
            {
                let body_range = match range {
                    Some((off, len)) if len > 0 => Some((off, len)),
                    _ => None,
                };
                let denserels =
                    rbitcoin_store::denserels_from_packed_records(tx, ins, outs);
                self.create_residency.put_outs(
                    *fk,
                    tx.clone(),
                    outs.clone(),
                    denserels,
                    body_range,
                );
            }
        }
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
    use std::sync::Mutex;
    use std::time::{SystemTime, UNIX_EPOCH};

    /// Serialize `RBITCOIN_CONFIRM_CACHE` mutations across parallel tests.
    static CACHE_ENV_LOCK: Mutex<()> = Mutex::new(());

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

    /// Open with multi‑GiB denserels history enabled (prewarm path tests).
    fn temp_query_full_history(label: &str) -> (std::path::PathBuf, Query, std::sync::MutexGuard<'static, ()>) {
        let g = CACHE_ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let prev = std::env::var_os("RBITCOIN_CONFIRM_CACHE");
        std::env::set_var("RBITCOIN_CONFIRM_CACHE", "1");
        let (dir, q) = temp_query(label);
        // Restore so other tests see process default after drop of guard… but
        // env stays until we restore here so Query open already sampled it.
        match prev {
            Some(v) => std::env::set_var("RBITCOIN_CONFIRM_CACHE", v),
            None => std::env::remove_var("RBITCOIN_CONFIRM_CACHE"),
        }
        assert!(
            q.create_residency().cache_enabled(),
            "full-history query must enable denserels cache"
        );
        (dir, q, g)
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
            .archive_plan_mega_from(&mut need, 1, &HashMap::new())
            .unwrap();
        assert_eq!(plan.batch_pin.len(), plan.planned_fks.len());
        assert_eq!(plan.batch_pin.len(), plan.packed.len());
        // Simulated note_plan_ok: Arc::clone only (strong_count rises, no deep clone).
        let mut ifo: HashMap<u64, Arc<(TxRecord, Vec<OutputRecord>, Vec<u32>)>> = HashMap::new();
        for (fk, pin) in plan.planned_fks.iter().zip(plan.batch_pin.iter()) {
            if let Some(id) = fk.get() {
                assert_eq!(Arc::strong_count(pin), 1);
                ifo.insert(id, Arc::clone(pin));
                assert_eq!(Arc::strong_count(pin), 2);
            }
        }
        for ((tx, ins, outs), pin) in plan.packed.iter().zip(plan.batch_pin.iter()) {
            assert_eq!(pin.0.txid, tx.txid);
            assert_eq!(pin.1, *outs);
            let layout = rbitcoin_store::denserels_from_packed_records(tx, ins, outs);
            assert_eq!(pin.2, layout);
            let mut raw = Vec::new();
            rbitcoin_store::encode_packed_tx(tx, ins, outs, &mut raw);
            let (_, _, decode_rels) =
                rbitcoin_store::decode_packed_tx_outs_with_spender_rels(&raw).unwrap();
            assert_eq!(pin.2, decode_rels);
        }
        assert_eq!(ifo.len(), 2);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// W1: commit res_seed from batch_pin denserels lands in CreateResidency
    /// (no encode+decode path required for pin hit).
    #[test]
    fn commit_res_seed_from_batch_pin_hits_residency() {
        use std::collections::HashMap;
        let (dir, q) = temp_query("res-seed-pin");
        let mut need = vec![(Fk(1), vec![coinbase_apply(7), coinbase_apply(8)])];
        let plan = q
            .archive_plan_mega_from(&mut need, 1, &HashMap::new())
            .unwrap();
        assert_eq!(plan.batch_pin.len(), 2);
        let pin0 = plan.batch_pin[0].clone();
        let fk0 = plan.planned_fks[0];
        q.archive_commit_plan(plan).unwrap();
        let need_v = vec![0u32];
        let got = q
            .create_residency()
            .get_parent_needed(fk0, &need_v)
            .expect("residency should have denserels after res_seed");
        let (tx, live, sparse, _range) = got;
        assert_eq!(tx.txid, pin0.0.txid);
        assert_eq!(live.len(), 1);
        assert!(!sparse.is_empty() || pin0.2.is_empty());
        // Dense rel for vout 0 matches plan pin.
        if !pin0.2.is_empty() {
            let expected = crate::sparse_spender_rels(&pin0.2, &need_v);
            assert_eq!(sparse, expected);
        }
        let _ = std::fs::remove_dir_all(&dir);
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
        let parent_txid = plan_a.batch_creates[0].0;
        let parent_fk = plan_a.batch_creates[0].1;

        // Child spends parent — not in residency/head until plan_a commits.
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
        let inflight: HashMap<_, _> = plan_a.batch_creates.iter().copied().collect();
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

    /// Commit denserels seed is for this batch's creates only.
    /// Head-resolved parents load outs+denserels into **pipeline-local**
    /// `external_parent_outs` (not CreateResidency FIFO).
    #[test]
    fn residency_publish_creates_only_not_head_resolved_parents() {
        use std::collections::HashMap;
        let (dir, q) = temp_query("residency-creates-only");
        // Parent on disk + head, but **not** in residency (direct store put).
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
        assert_eq!(q.create_residency().len(), 0);

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
        assert_eq!(plan.batch_creates.len(), 1);
        assert_eq!(plan.batch_creates[0].0, child_txid);
        // Pipeline-local denserels for head-miss parent (prep pin source).
        let (ptx, pouts, pdens) = plan
            .external_parent_outs
            .get(&1)
            .expect("head-miss parent denserels on plan");
        assert_eq!(ptx.txid, parent_txid);
        assert!(!pouts.is_empty(), "outs loaded for pin");
        assert!(!pdens.is_empty() || pouts.len() == 1, "denserels for outs");
        // Must not thrash CreateResidency with long-tail head parents.
        assert!(
            q.create_residency().lookup_fk_by_txid(&parent_txid).is_none(),
            "head path must not FIFO-seed parent into residency"
        );
        assert!(
            q.create_residency().get_outs(Fk(1)).is_none(),
            "head path must not denserels-seed parents into residency"
        );

        q.archive_commit_plan(plan).unwrap();
        assert_eq!(
            q.create_residency().lookup_fk_by_txid(&child_txid),
            Some(Fk(2))
        );
        assert!(
            q.create_residency().get_outs(Fk(2)).is_some(),
            "commit must seed denserels into residency for pin hits"
        );
        assert!(
            q.create_residency().get_outs(Fk(1)).is_none(),
            "head-resolved parent must not be denserels-published as a new create"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn lean_default_skips_residency_prewarm() {
        // Product default: no multi‑GiB denserels prewarm. Hold cache env lock so
        // parallel full-history tests do not leave CONFIRM_CACHE=1 during open.
        let _g = CACHE_ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let prev = std::env::var_os("RBITCOIN_CONFIRM_CACHE");
        std::env::remove_var("RBITCOIN_CONFIRM_CACHE");
        let (dir, q) = temp_query("lean-skip-prewarm");
        match prev {
            Some(v) => std::env::set_var("RBITCOIN_CONFIRM_CACHE", v),
            None => std::env::remove_var("RBITCOIN_CONFIRM_CACHE"),
        }
        assert!(!q.create_residency().cache_enabled());
        let mut packed = Vec::new();
        for i in 1u64..=10 {
            let ta = coinbase_apply(i);
            packed.push((ta.tx, ta.inputs, ta.outputs));
        }
        q.store
            .txs
            .put_full_batch_indexed(&packed, true)
            .unwrap();
        let st = q.archive_residency_prewarm().unwrap();
        assert_eq!(st.ranges, 0);
        assert_eq!(st.denserels_creates, 0);
        assert_eq!(q.create_residency().len(), 0);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn residency_prewarm_loads_last_n_via_idx_order() {
        let (dir, q, _env) = temp_query_full_history("residency-prewarm");
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
        assert_eq!(q.create_residency().len(), 0);

        let st = q.archive_residency_prewarm().unwrap();
        assert_eq!(st.ranges, 50);
        assert_eq!(q.create_residency().len(), 50);
        // No tip → no header plans; denserels still fill recent creates.
        assert_eq!(st.header_plans, 0);
        assert!(
            st.denserels_creates > 0 && st.denserels_outs > 0,
            "prewarm must load denserels: denserels_creates={} outs={}",
            st.denserels_creates,
            st.denserels_outs
        );
        assert!(
            q.create_residency().has_outs(Fk(50)),
            "newest create should hold denserels after prewarm"
        );

        let mut last_txid = [0u8; 32];
        last_txid[0..8].copy_from_slice(&50u64.to_le_bytes());
        last_txid[8] = 0xcb;
        assert_eq!(
            q.create_residency().lookup_fk_by_txid(&last_txid),
            Some(Fk(50))
        );
        let range = q.create_residency().body_ranges_by_fk(&[Fk(50)]);
        assert!(
            range[0].is_some(),
            "prewarm should cache body range with fk"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn residency_prewarm_keeps_tail_when_under_cap() {
        let (dir, q, _env) = temp_query_full_history("residency-tail");
        let mut packed = Vec::new();
        for i in 1u64..=5 {
            let ta = coinbase_apply(i);
            packed.push((ta.tx, ta.inputs, ta.outputs));
        }
        q.store
            .txs
            .put_full_batch_indexed(&packed, false)
            .unwrap();
        let st = q.archive_residency_prewarm().unwrap();
        assert_eq!(st.ranges, 5);
        let t1 = coinbase_apply(1).tx.txid;
        let t5 = coinbase_apply(5).tx.txid;
        assert_eq!(q.create_residency().lookup_fk_by_txid(&t1), Some(Fk(1)));
        assert_eq!(q.create_residency().lookup_fk_by_txid(&t5), Some(Fk(5)));
        assert!(q.create_residency().body_ranges_by_fk(&[Fk(1)])[0].is_some());
        assert!(q.create_residency().body_ranges_by_fk(&[Fk(5)])[0].is_some());
        assert!(q.create_residency().has_outs(Fk(1)));
        assert!(q.create_residency().has_outs(Fk(5)));
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Tip-ahead Class A path must seed ConfirmParentCache header plans.
    #[test]
    fn residency_prewarm_seeds_tip_ahead_header_plans() {
        use crate::Height;
        use rbitcoin_store::HeaderRecord;

        let (dir, q, _env) = temp_query_full_history("prewarm-hdr-plans");
        // Confirm genesis.
        let mut hash0 = [0u8; 32];
        hash0[0] = 0xa0;
        let h0 = HeaderRecord {
            prev_fk: Fk::NULL,
            version: 1,
            timestamp: 1,
            bits: 0x207f_ffff,
            nonce: 0,
            merkle_root: hash0,
            hash: hash0,
        };
        q.connect_block(Height(0), &h0, &[coinbase_apply(1)])
            .expect("connect genesis");
        assert_eq!(q.tip_height(), Some(Height(0)));
        let mut prev = q.tip_header_fk().unwrap().unwrap();

        // Archive tip+1 and tip+2 without confirming (Class A ahead of tip).
        for h in 1..=2u32 {
            let mut hash = [0u8; 32];
            hash[0] = 0xa0 + h as u8;
            let header = HeaderRecord {
                prev_fk: prev,
                version: 1,
                timestamp: 1 + h,
                bits: 0x207f_ffff,
                nonce: h,
                merkle_root: hash,
                hash,
            };
            prev = q
                .archive_block(&header, &[coinbase_apply((h as u64) + 10)])
                .expect("archive ahead");
        }

        let st = q.archive_residency_prewarm().unwrap();
        assert!(
            st.header_plans >= 2,
            "expected tip-ahead header plans, got {}",
            st.header_plans
        );
        assert!(
            q.confirm_parent_cache().get_header_plan(1).is_some(),
            "height 1 plan missing"
        );
        assert!(
            q.confirm_parent_cache().get_header_plan(2).is_some(),
            "height 2 plan missing"
        );
        // Archive commit already denserels-seeds its creates; prewarm may load 0
        // additional denserels but residency outs must be non-zero either way.
        assert!(
            st.denserels_outs > 0,
            "residency outs after prewarm must be >0 (got denserels_creates={} outs={})",
            st.denserels_creates,
            st.denserels_outs
        );
        assert!(
            st.ranges >= 3,
            "range prewarm should cover genesis + ahead creates"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }
}
