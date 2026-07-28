//! Confirm **load** stage: Class A decode + parent pin for one claimed batch.
//!
//! For each height in the batch (ascending):
//! 1. **Cache** header + `header_txs` (process-local, tip-GCed).
//! 2. **Full Class A decode once** into [`BatchFullBodies`] (wire) + outs FIFO
//!    (pin hits; inputs dropped there).
//! 3. **Thin edges** + **sparse parent pin** as **batch-local** maps.
//! Body ranges from `tx.idx` on demand.

use super::*;
use crate::batch_full_bodies::BatchFullBodies;
use crate::batch_parents::BatchParents;
use crate::wave_prevout::ThinInput;
use std::collections::{HashMap, HashSet};
use std::time::Instant;

/// Spend-fk → thin create_fk edges for one confirm batch (assemble only).
pub type BatchThin = HashMap<u64, Vec<ThinInput>>;

#[derive(Debug, Default, Clone, Copy)]
pub struct ConfirmLoadStats {
    pub blocks: u32,
    pub utxo_parents: u32,
    pub creates_registered: u32,
    /// Unique parent create fks pinned this call (after dedup).
    pub parent_unique: u32,
    /// Of `parent_unique`: filled without store denserels IO (FIFO / same-batch / residency).
    pub pin_cache_body: u32,
    /// Subset of no-IO pins that came from CreateResidency (schema 12).
    pub pin_residency: u32,
    /// Of `parent_unique`: missed FIFO/same-batch (may still hit residency before store).
    pub pin_new: u32,
    /// Historical: spent-filter during pin (now always 0 — structural owns spentness).
    pub pin_spent_ns: u64,
    /// FIFO hit path resolve (excludes spent timer).
    pub pin_body_ns: u64,
    /// pin_new meta/outs resolve (excludes spent timer).
    pub pin_new_meta_ns: u64,
    /// Same-batch create edges (identity known in-batch).
    pub parent_cache_hits: u32,
    /// Stamped create_fk on input, parent **not** in this batch (external fk).
    pub edge_fk: u32,
    /// Body txs full-decoded (phase 1).
    pub body_tx_reads: u32,
    /// Parent outs loaded from store (sparse pin).
    pub full_tx_reads: u32,
    /// Unstamped non-coinbase edges (should not occur on healthy v10 Class A).
    pub missing_parents: u32,
    /// Phase wall times (ns).
    pub header_ns: u64,
    pub body_decode_ns: u64,
    pub thin_ns: u64,
    pub parent_pin_ns: u64,
    pub cache_put_ns: u64,
    pub edge_same_batch: u32,
    pub edge_coinbase: u32,
}

impl Query {
    pub fn parent_cache_ready_through(&self) -> u32 {
        self.confirm_parents.ready_through()
    }

    /// Snapshot: `(ready_through, ahead, sparse_parents, bodies, plans)`.
    ///
    /// `ahead` is ready_through − tip (in-flight load watermark, not a depth knobs).
    /// Third field is always 0 (sparse parents are per-batch, not shared).
    pub fn parent_cache_perf_snapshot(&self) -> (u32, u32, usize, usize, usize) {
        let tip = self.tip_height().map(|h| h.0).unwrap_or(0);
        let through = self.confirm_parents.ready_through();
        let ahead = through.saturating_sub(tip);
        (
            through,
            ahead,
            0,
            self.confirm_parents.body_count(),
            self.confirm_parents.plan_count(),
        )
    }

    pub fn advance_parent_cache_tip(&self, tip: u32) {
        self.confirm_parents.advance_tip(tip);
    }

    pub fn seed_parent_cache(&self, items: &[(u32, [u8; 32])]) {
        self.confirm_parents.ensure_plans(items);
    }

    /// True when every height has been load-scanned (watermark / tests).
    pub fn is_confirm_load_ready(&self, heights: &[u32]) -> bool {
        self.confirm_parents.all_ready(heights)
    }

    /// Load Class A for heights: outs FIFO + **per-batch** parent pin + thin edges.
    ///
    /// Returns `(stats, batch_parents, batch_thin, batch_bodies)`. Thin edges and
    /// full bodies are assemble/wire-only and must not be stored on the process
    /// parent cache long-term (FIFO keeps outs only).
    pub fn load_confirm_parents(
        &self,
        items: &[(u32, [u8; 32])],
    ) -> Result<(ConfirmLoadStats, BatchParents, BatchThin, BatchFullBodies), QueryError> {
        let t0 = Instant::now();
        let mut st = ConfirmLoadStats::default();
        let mut batch_parents = BatchParents::new();
        let mut batch_thin = BatchThin::new();
        let mut batch_bodies = BatchFullBodies::new();
        if items.is_empty() {
            return Ok((st, batch_parents, batch_thin, batch_bodies));
        }
        // None tip: include genesis (height 0). `unwrap_or(0)` would skip h=0.
        let tip = self.tip_height().map(|h| h.0);

        // Always re-decode / re-pin claimed heights (batch-local thin + parents).
        let mut work: Vec<(u32, [u8; 32])> = Vec::new();
        for &(height, hash) in items {
            if self.confirm_cancelled() {
                crate::confirm_load_stats::note(&st, t0.elapsed().as_nanos() as u64);
                return Err(StoreError::Cancelled("confirm cancelled"));
            }
            if let Some(t) = tip {
                if height <= t {
                    continue;
                }
            }
            work.push((height, hash));
        }
        if work.is_empty() {
            crate::confirm_load_stats::note(&st, t0.elapsed().as_nanos() as u64);
            return Ok((st, batch_parents, batch_thin, batch_bodies));
        }
        self.confirm_parents.ensure_plans(&work);

        let mut height_tx_fks: Vec<(u32, Vec<Fk>)> = Vec::with_capacity(work.len());
        // Full-decoded bodies once for wire + FIFO outs (decode once).
        // (txid, edges): each edge is (create_fk_opt, soft prev_txid, vout).
        // v10: create_fk is stamped at archive; soft prev_txid may be zero.
        let mut body_prevouts: HashMap<u64, ([u8; 32], Vec<(Option<u64>, [u8; 32], u32)>)> =
            HashMap::new();
        let mut parent_need: HashMap<u64, Vec<u32>> = HashMap::new(); // parent_fk → need heights
        // parent_fk → needed prev_index (vouts) for sparse outs stash.
        let mut parent_vouts: HashMap<u64, Vec<u32>> = HashMap::new();
        let mut thin_by_spend: BatchThin = BatchThin::new();
        let mut batch_create_ids: HashSet<u64> = HashSet::new();

        for &(height, hash) in &work {
            if self.confirm_cancelled() {
                crate::confirm_load_stats::note(&st, t0.elapsed().as_nanos() as u64);
                return Err(StoreError::Cancelled("confirm cancelled"));
            }

            // ── Header + header_txs ────────────────────────────────────────
            let t_hdr = Instant::now();
            let Some((header_fk, header_rec)) = self.store.get_header_by_hash(&hash)? else {
                st.header_ns = st.header_ns.saturating_add(t_hdr.elapsed().as_nanos() as u64);
                continue;
            };
            if !self.store.header_txs.has_body(header_fk)? {
                st.header_ns = st.header_ns.saturating_add(t_hdr.elapsed().as_nanos() as u64);
                continue;
            }
            let Some(tx_fks) = self.store.header_txs.get_list(header_fk)? else {
                st.header_ns = st.header_ns.saturating_add(t_hdr.elapsed().as_nanos() as u64);
                continue;
            };
            if tx_fks.is_empty() {
                st.header_ns = st.header_ns.saturating_add(t_hdr.elapsed().as_nanos() as u64);
                continue;
            }
            // Body readiness: first tx must have an idx range (no tx.head probe).
            if let Some(&first) = tx_fks.first() {
                if self.store.tx_body_range(first).is_err() {
                    st.header_ns = st.header_ns.saturating_add(t_hdr.elapsed().as_nanos() as u64);
                    continue;
                }
            }
            let prev_hash = if header_rec.prev_fk.is_null() {
                [0u8; 32]
            } else {
                match self.store.get_header(header_rec.prev_fk) {
                    Ok(prev) => prev.hash,
                    Err(_) => {
                        st.header_ns =
                            st.header_ns.saturating_add(t_hdr.elapsed().as_nanos() as u64);
                        continue;
                    }
                }
            };
            self.confirm_parents.put_header_plan(
                height,
                header_fk,
                header_rec,
                tx_fks.clone(),
                prev_hash,
            );
            st.header_ns = st.header_ns.saturating_add(t_hdr.elapsed().as_nanos() as u64);

            // ── body ranges + full Class A decode ─────────────────────────
            st.blocks = st.blocks.saturating_add(1);
            let t_dec = Instant::now();

            let fks_work: std::borrow::Cow<'_, [Fk]> = if tx_fks_is_sorted_ascending(&tx_fks) {
                std::borrow::Cow::Borrowed(tx_fks.as_slice())
            } else {
                let mut v = tx_fks.clone();
                v.sort_unstable_by_key(|f| f.0);
                std::borrow::Cow::Owned(v)
            };

            // Combined path: residency ranges + one idx→body wave seeds residency
            // (archive prep + confirm pin share the same map — no dual thrash).
            let range_fks: Vec<Fk> = fks_work
                .iter()
                .copied()
                .filter(|fk| fk.get().is_some())
                .collect();
            let creates = crate::combined_stage::load_creates_once(
                &self.store,
                &self.create_residency,
                &range_fks,
                rbitcoin_store::IdxBodyMode::Full,
            )?;
            // Map fk → raw for this block's create decode.
            let mut by_fk: HashMap<u64, crate::combined_stage::CombinedCreate> =
                HashMap::with_capacity(creates.len());
            for c in creates {
                by_fk.insert(c.fk.get().unwrap_or(0), c);
            }
            for fk in &range_fks {
                if self.confirm_cancelled() {
                    crate::confirm_load_stats::note(&st, t0.elapsed().as_nanos() as u64);
                    return Err(StoreError::Cancelled("confirm cancelled"));
                }
                let Some(id) = fk.get() else {
                    continue;
                };
                let Some(c) = by_fk.remove(&id) else {
                    return Err(StoreError::Corrupt(
                        "invariant: confirm load create missing body range",
                    )
                    .into());
                };
                let Ok((tx, inputs, outs, denserels)) =
                    rbitcoin_store::decode_packed_tx_with_spender_rels_secret(
                        &c.raw,
                        Some(self.store.txs.store_secret()),
                    )
                else {
                    continue;
                };
                let body_range = Some(c.body_range);
                st.body_tx_reads = st.body_tx_reads.saturating_add(1);
                let prevouts: Vec<(Option<u64>, [u8; 32], u32)> = inputs
                    .iter()
                    .map(|inp| {
                        let soft = if inp.create_fk.is_null() {
                            inp.prev_txid
                        } else {
                            [0u8; 32]
                        };
                        (inp.create_fk.get(), soft, inp.prev_index)
                    })
                    .collect();
                batch_create_ids.insert(id);
                body_prevouts.insert(id, (tx.txid, prevouts));
                batch_bodies.insert(*fk, height, tx, inputs, outs, body_range, denserels);
                st.creates_registered = st.creates_registered.saturating_add(1);
            }
            st.body_decode_ns = st
                .body_decode_ns
                .saturating_add(t_dec.elapsed().as_nanos() as u64);
            height_tx_fks.push((height, tx_fks));
        }

        // ── Cache put (OutFifo + unified residency for shared pin hits) ─
        let t_put = Instant::now();
        self.confirm_parents.put_bodies_from_batch_full(&batch_bodies);
        // Dual-write outs into residency so later batches pin without re-IO.
        for (fk, body) in batch_bodies.iter() {
            self.create_residency.put_outs(
                fk,
                body.tx.clone(),
                body.outputs.clone(),
                body.denserels.clone(),
                body.body_range,
            );
        }
        st.cache_put_ns = st
            .cache_put_ns
            .saturating_add(t_put.elapsed().as_nanos() as u64);

        // ── Thin edges: stamped create_fk only (schema v10) ────────────────
        // Soft prev_txid / sticky / head resolve removed — archive stamps create_fk.
        let t_thin = Instant::now();
        for (height, tx_fks) in &height_tx_fks {
            for fk in tx_fks {
                let Some(id) = fk.get() else {
                    continue;
                };
                let Some((_txid, prevouts)) = body_prevouts.get(&id) else {
                    continue;
                };
                let mut edges: Vec<ThinInput> = Vec::with_capacity(prevouts.len());
                for &(create_fk_opt, _prev_txid, prev_index) in prevouts {
                    if create_fk_opt.is_none() && prev_index == u32::MAX {
                        edges.push(ThinInput {
                            create_fk: None,
                            prev_index,
                        });
                        st.edge_coinbase = st.edge_coinbase.saturating_add(1);
                        continue;
                    }
                    if let Some(pid) = create_fk_opt {
                        edges.push(ThinInput {
                            create_fk: Some(pid),
                            prev_index,
                        });
                        st.utxo_parents = st.utxo_parents.saturating_add(1);
                        parent_need.entry(pid).or_default().push(*height);
                        parent_vouts.entry(pid).or_default().push(prev_index);
                        if batch_create_ids.contains(&pid) {
                            st.parent_cache_hits = st.parent_cache_hits.saturating_add(1);
                            st.edge_same_batch = st.edge_same_batch.saturating_add(1);
                        } else {
                            st.edge_fk = st.edge_fk.saturating_add(1);
                        }
                        continue;
                    }
                    // Unstamped non-coinbase: corrupt / pre-v10 body.
                    edges.push(ThinInput {
                        create_fk: None,
                        prev_index,
                    });
                    st.missing_parents = st.missing_parents.saturating_add(1);
                }
                thin_by_spend.insert(id, edges);
            }
        }
        st.thin_ns = st.thin_ns.saturating_add(t_thin.elapsed().as_nanos() as u64);

        // ── Pin parents into per-batch BatchParents ───────────────────────
        // Sparse spent-filtered outs live on the batch object (not tip-GCed).
        // Outs FIFO supplies pin_cache hits; body ranges from store tx.idx.
        //
        // Ownership path: FIFO hits move once into BatchParents (no
        // intermediate sparse_parents + re-clone). Vec-backed ParentEntry
        // avoids HashMap/HashSet allocs per parent at pin volume.
        let t_par = Instant::now();
        let mut uniq_parents: Vec<u64> = parent_need.keys().copied().collect();
        uniq_parents.sort_unstable();
        st.parent_unique = st.parent_unique.saturating_add(uniq_parents.len() as u32);

        // (pid, need_vouts)
        let mut pin_jobs: Vec<(u64, Vec<u32>)> = Vec::with_capacity(uniq_parents.len());
        for pid in uniq_parents {
            let _ = parent_need.remove(&pid);
            let mut need_vouts = parent_vouts.remove(&pid).unwrap_or_default();
            need_vouts.sort_unstable();
            need_vouts.dedup();
            pin_jobs.push((pid, need_vouts));
        }
        batch_parents = BatchParents::with_capacity(pin_jobs.len());

        // ── FIFO / same-batch hits (pin_cache) ────────────────────────────
        // Coinbase height: only free hints already on the FIFO entry. Store
        // resolve is deferred to structural write (maturity). Unset (`None`)
        // means write must look up if needed.
        //
        // Same-batch creates are in OutFifo with denserels after put_bodies_from_batch_full;
        // if FIFO missed (cap eviction) fall back to batch_bodies layout without idx.
        let t_body = Instant::now();
        let body_keys: Vec<(u64, &[u32])> = pin_jobs
            .iter()
            .filter(|(_, vouts)| !vouts.is_empty())
            .map(|(pid, vouts)| (*pid, vouts.as_slice()))
            .collect();
        let mut body_hits = self
            .confirm_parents
            .get_bodies_for_pin_batch(&body_keys);

        let mut pin_new_pending: Vec<(u64, Vec<u32>)> = Vec::new();
        for (pid, need_vouts) in pin_jobs {
            if self.confirm_cancelled() {
                crate::confirm_load_stats::note(&st, t0.elapsed().as_nanos() as u64);
                return Err(StoreError::Cancelled("confirm cancelled"));
            }
            let fk = Fk(pid);
            if need_vouts.is_empty() {
                continue;
            }
            if let Some((_create_h, tx, outs, cb_hint, body_range, sparse_rels)) =
                body_hits.remove(&pid)
            {
                if crate::batch_parents::layout_covers_need(
                    body_range,
                    &sparse_rels,
                    &need_vouts,
                ) {
                    st.pin_cache_body = st.pin_cache_body.saturating_add(1);
                    let live = slim_outs_to_need(outs, &need_vouts);
                    batch_parents.insert_owned(
                        fk,
                        tx,
                        live,
                        need_vouts,
                        cb_hint,
                        body_range,
                        sparse_rels,
                    );
                    st.utxo_parents = st.utxo_parents.saturating_add(1);
                    continue;
                }
            }
            // Same-batch create: pin from batch_bodies (no idx / body re-read).
            if let Some(body) = batch_bodies.get(fk) {
                let sparse =
                    crate::batch_parents::sparse_spender_rels(&body.denserels, &need_vouts);
                if crate::batch_parents::layout_covers_need(
                    body.body_range,
                    &sparse,
                    &need_vouts,
                ) {
                    st.pin_cache_body = st.pin_cache_body.saturating_add(1);
                    st.parent_cache_hits = st.parent_cache_hits.saturating_add(1);
                    let live = slim_dense_outs_to_need(&body.outputs, &need_vouts);
                    let cb = Some(crate::out_fifo::is_coinbase_inputs(&body.tx, &body.inputs));
                    batch_parents.insert_owned(
                        fk,
                        body.tx.clone(),
                        live,
                        need_vouts,
                        cb,
                        body.body_range,
                        sparse,
                    );
                    st.utxo_parents = st.utxo_parents.saturating_add(1);
                    continue;
                }
            }
            st.pin_new = st.pin_new.saturating_add(1);
            pin_new_pending.push((pid, need_vouts));
        }
        st.pin_body_ns = st
            .pin_body_ns
            .saturating_add(t_body.elapsed().as_nanos() as u64);

        // ── pin_new: idx→body pipeline in **chunks** ─────────────────────
        // Holding ~90k full packed bodies + dense outs at once blew RSS.
        // Chunk so peak is O(PIN_NEW_CHUNK) bodies. Range resolve is one sticky
        // + one OutFifo batch for the whole pin_new set (two mutex takes), then
        // body IO runs in chunks via idx→body pipeline.
        const PIN_NEW_CHUNK: usize = 4096;
        let t_new = Instant::now();
        if self.confirm_cancelled() {
            crate::confirm_load_stats::note(&st, t0.elapsed().as_nanos() as u64);
            return Err(StoreError::Cancelled("confirm cancelled"));
        }
        // pin_new via combined residency: one denserels load seeds residency;
        // OutFifo dual-write kept for legacy cross-checks.
        // Prefer already-resident outs (same creates archive just published).
        let mut still_need: Vec<(u64, Vec<u32>)> = Vec::new();
        for (pid, need_vouts) in &pin_new_pending {
            let fk = Fk(*pid);
            if let Some((tx, outs, dense_rels, range)) = self.create_residency.get_outs(fk) {
                let live = slim_dense_outs_to_need(&outs, need_vouts);
                let sparse = crate::batch_parents::sparse_spender_rels(&dense_rels, need_vouts);
                if crate::batch_parents::layout_covers_need(range, &sparse, need_vouts) {
                    let cb = if tx.input_count != 1 {
                        Some(false)
                    } else {
                        None
                    };
                    batch_parents.insert_owned(
                        fk,
                        tx,
                        live,
                        need_vouts.clone(),
                        cb,
                        range,
                        sparse,
                    );
                    st.utxo_parents = st.utxo_parents.saturating_add(1);
                    // No store denserels IO: count as cache + residency hit.
                    st.pin_cache_body = st.pin_cache_body.saturating_add(1);
                    st.pin_residency = st.pin_residency.saturating_add(1);
                    // Was classified pin_new before residency check — undo that.
                    st.pin_new = st.pin_new.saturating_sub(1);
                    continue;
                }
            }
            still_need.push((*pid, need_vouts.clone()));
        }
        for chunk in still_need.chunks(PIN_NEW_CHUNK) {
            if self.confirm_cancelled() {
                crate::confirm_load_stats::note(&st, t0.elapsed().as_nanos() as u64);
                return Err(StoreError::Cancelled("confirm cancelled"));
            }
            let fks: Vec<Fk> = chunk.iter().map(|(pid, _)| Fk(*pid)).collect();
            let loaded = crate::combined_stage::load_creates_once(
                &self.store,
                &self.create_residency,
                &fks,
                rbitcoin_store::IdxBodyMode::OutsDenserels,
            )?;
            let mut by_id: HashMap<u64, crate::combined_stage::CombinedCreate> =
                HashMap::with_capacity(loaded.len());
            for c in loaded {
                by_id.insert(c.fk.get().unwrap_or(0), c);
            }
            const FIFO_FLUSH: usize = 256;
            let mut fifo_seed: Vec<(
                Fk,
                u32,
                rbitcoin_store::TxRecord,
                Vec<rbitcoin_store::OutputRecord>,
                Option<(u64, u64)>,
                Vec<u32>,
            )> = Vec::with_capacity(FIFO_FLUSH);

            for (pid, need_vouts) in chunk {
                if self.confirm_cancelled() {
                    crate::confirm_load_stats::note(&st, t0.elapsed().as_nanos() as u64);
                    return Err(StoreError::Cancelled("confirm cancelled"));
                }
                let fk = Fk(*pid);
                if need_vouts.is_empty() {
                    continue;
                }
                let Some(c) = by_id.remove(pid) else {
                    return Err(StoreError::Corrupt(
                        "invariant: pin_new parent missing body range",
                    )
                    .into());
                };
                let range = Some(c.body_range);
                let Ok((tx, outs, dense_rels)) =
                    rbitcoin_store::decode_packed_tx_outs_with_spender_rels_secret(
                        &c.raw,
                        Some(self.store.txs.store_secret()),
                    )
                else {
                    return Err(StoreError::Corrupt(
                        "invariant: pin_new failed to decode parent denserels",
                    )
                    .into());
                };
                let live = slim_dense_outs_to_need(&outs, need_vouts);
                let sparse = crate::batch_parents::sparse_spender_rels(&dense_rels, need_vouts);
                if !crate::batch_parents::layout_covers_need(range, &sparse, need_vouts) {
                    return Err(StoreError::Corrupt(
                        "invariant: pin_new denserels incomplete for need_vouts",
                    )
                    .into());
                }
                let cb = if tx.input_count != 1 {
                    Some(false)
                } else {
                    None
                };
                fifo_seed.push((fk, 0, tx.clone(), outs, range, dense_rels));
                batch_parents.insert_owned(
                    fk,
                    tx,
                    live,
                    need_vouts.clone(),
                    cb,
                    range,
                    sparse,
                );
                st.full_tx_reads = st.full_tx_reads.saturating_add(1);
                if fifo_seed.len() >= FIFO_FLUSH {
                    self.confirm_parents
                        .put_dense_outs_batch(std::mem::take(&mut fifo_seed));
                }
                st.utxo_parents = st.utxo_parents.saturating_add(1);
            }
            if !fifo_seed.is_empty() {
                self.confirm_parents.put_dense_outs_batch(fifo_seed);
            }
        }
        st.pin_new_meta_ns = st
            .pin_new_meta_ns
            .saturating_add(t_new.elapsed().as_nanos() as u64);

        // Parents already moved into BatchParents; thin stays batch-local.
        st.parent_pin_ns = st
            .parent_pin_ns
            .saturating_add(t_par.elapsed().as_nanos() as u64);

        batch_thin = thin_by_spend;

        let scanned: Vec<u32> = height_tx_fks.iter().map(|(h, _)| *h).collect();
        if !scanned.is_empty() {
            self.confirm_parents.mark_scanned_many(&scanned);
        }

        crate::confirm_load_stats::note(&st, t0.elapsed().as_nanos() as u64);
        Ok((st, batch_parents, batch_thin, batch_bodies))
    }

    pub fn load_confirm_parents_for_hashes(
        &self,
        hashes: &[[u8; 32]],
    ) -> Result<(ConfirmLoadStats, BatchParents, BatchThin, BatchFullBodies), QueryError> {
        let tip = self.tip_height().map(|h| h.0).unwrap_or(0);
        let mut items = Vec::with_capacity(hashes.len());
        for (i, hash) in hashes.iter().enumerate() {
            let h = tip.saturating_add(1).saturating_add(i as u32);
            items.push((h, *hash));
        }
        self.load_confirm_parents(&items)
    }

    /// No-op: sparse parents are batch-local and drop with the confirm batch.
    pub fn unpin_spent_parent_outs(
        &self,
        _spends: &[(Fk, u32)],
    ) -> Result<(), QueryError> {
        Ok(())
    }
}

/// True when `fks` is empty or non-decreasing by Class A id (archive order).
#[inline]
fn tx_fks_is_sorted_ascending(fks: &[Fk]) -> bool {
    fks.windows(2).all(|w| w[0].0 <= w[1].0)
}

/// Keep only outs whose vout is in `need` (sparse pin map for assemble).
///
/// Does not consult spenders — load is optimistic for scripts; structural
/// rejects already-spent prevouts after verification.
fn slim_outs_to_need(
    outs: Vec<(u32, rbitcoin_store::OutputRecord)>,
    need: &[u32],
) -> Vec<(u32, rbitcoin_store::OutputRecord)> {
    if need.is_empty() {
        return Vec::new();
    }
    if need.len() == 1 {
        let v = need[0];
        return outs.into_iter().filter(|(vout, _)| *vout == v).collect();
    }
    let need_set: HashSet<u32> = need.iter().copied().collect();
    outs.into_iter()
        .filter(|(vout, _)| need_set.contains(vout))
        .collect()
}

/// Dense `outs[vout]` → sparse need list (clone of need only).
///
/// Caller should move `outs` into OutFifo after this so the only residual
/// batch-local copies of scripts are the need-vouts in [`BatchParents`].
fn slim_dense_outs_to_need(
    outs: &[rbitcoin_store::OutputRecord],
    need: &[u32],
) -> Vec<(u32, rbitcoin_store::OutputRecord)> {
    let mut live = Vec::with_capacity(need.len());
    for &v in need {
        if let Some(o) = outs.get(v as usize) {
            live.push((v, o.clone()));
        }
    }
    live
}

#[cfg(test)]
mod pin_new_residency_tests {
    use super::slim_dense_outs_to_need;
    use rbitcoin_store::OutputRecord;

    #[test]
    fn slim_does_not_retain_unneeded_vouts() {
        let dense = vec![
            OutputRecord::unspent(1, vec![1; 64]),
            OutputRecord::unspent(2, vec![2; 64]),
            OutputRecord::unspent(3, vec![3; 256]), // large unneeded
        ];
        let live = slim_dense_outs_to_need(&dense, &[0, 1]);
        assert_eq!(live.len(), 2);
        assert_eq!(live[0].1.script.len(), 64);
        assert_eq!(live[1].1.script.len(), 64);
        // dense still owned by caller for FIFO; after move to FIFO only need stays in batch.
        assert_eq!(dense.len(), 3);
        let _fifo = dense; // move away — batch would only keep `live`
        assert_eq!(live.len(), 2);
    }
}

#[cfg(test)]
mod slim_outs_tests {
    use super::{slim_dense_outs_to_need, slim_outs_to_need};
    use rbitcoin_store::OutputRecord;

    fn o(n: u8) -> OutputRecord {
        OutputRecord::unspent(n as i64, vec![n])
    }

    #[test]
    fn slim_outs_keeps_need_even_if_would_be_spent() {
        // Regression: pin must not drop needed vouts via spenders filter.
        // Spentness is structural; scripts need the value/script here.
        let outs = vec![(0, o(10)), (1, o(11)), (2, o(12))];
        let live = slim_outs_to_need(outs, &[1, 2]);
        assert_eq!(live.len(), 2);
        assert_eq!(live[0].0, 1);
        assert_eq!(live[1].0, 2);
    }

    #[test]
    fn slim_dense_maps_vout_index() {
        let outs = vec![o(0), o(1), o(2)];
        let live = slim_dense_outs_to_need(&outs, &[0, 2]);
        assert_eq!(live.len(), 2);
        assert_eq!(live[0].1.value, 0);
        assert_eq!(live[1].1.value, 2);
    }

    #[test]
    fn slim_outs_empty_need() {
        assert!(slim_outs_to_need(vec![(0, o(1))], &[]).is_empty());
    }
}
