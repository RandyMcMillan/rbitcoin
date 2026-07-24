//! Confirm **load** stage: Class A decode + parent pin for one claimed batch.
//!
//! For each height in the batch (ascending):
//! 1. **Cache** header + `header_txs` (process-local, tip-GCed).
//! 2. **Full Class A decode** into the outs FIFO (pin hits); wire uses store.
//! 3. **Thin edges** + **sparse parent pin** as **batch-local** maps.
//! Body ranges from `tx.idx` on demand.

use super::*;
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
    pub reserved: u32,
    pub creates_registered: u32,
    /// Unique parent create fks pinned this call (after dedup).
    pub parent_unique: u32,
    /// Of `parent_unique`: filled from outs FIFO (no Class A re-decode).
    pub pin_cache_body: u32,
    /// Of `parent_unique`: first-time sparse pin (store decode).
    pub pin_new: u32,
    /// Wall ns in `unspent_create_vouts` during pin (store spent-filter).
    pub pin_spent_ns: u64,
    /// Body-LRU batch lookup + pin_cache resolve (excludes spent timer).
    pub pin_body_ns: u64,
    /// pin_new range + meta/outs resolve (excludes spent timer).
    pub pin_new_meta_ns: u64,
    /// Body-range put under parent cache lock (parents insert during body/new).
    pub pin_put_ns: u64,
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
    /// Thin sub-phases (sum into `thin_ns`).
    pub thin_collect_ns: u64,
    pub thin_cache_ns: u64,
    pub thin_head_ns: u64,
    pub thin_edge_ns: u64,
    pub parent_pin_ns: u64,
    pub cache_put_ns: u64,
    pub head_lookups: u32,
    pub head_hits: u32,
    pub edge_same_batch: u32,
    pub edge_cache: u32,
    pub edge_head: u32,
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
    /// Returns `(stats, batch_parents, batch_thin)`. Thin edges are assemble-only
    /// and must not be stored on the process parent cache.
    pub fn load_confirm_parents(
        &self,
        items: &[(u32, [u8; 32])],
    ) -> Result<(ConfirmLoadStats, BatchParents, BatchThin), QueryError> {
        let t0 = Instant::now();
        let mut st = ConfirmLoadStats::default();
        let mut batch_parents = BatchParents::new();
        let mut batch_thin = BatchThin::new();
        if items.is_empty() {
            return Ok((st, batch_parents, batch_thin));
        }
        let tip = self.tip_height().map(|h| h.0).unwrap_or(0);

        // Always re-decode / re-pin claimed heights (batch-local thin + parents).
        let mut work: Vec<(u32, [u8; 32])> = Vec::new();
        for &(height, hash) in items {
            if self.confirm_cancelled() {
                crate::confirm_load_stats::note(&st, t0.elapsed().as_nanos() as u64);
                return Err(StoreError::Cancelled("confirm cancelled"));
            }
            if height <= tip {
                continue;
            }
            work.push((height, hash));
        }
        if work.is_empty() {
            crate::confirm_load_stats::note(&st, t0.elapsed().as_nanos() as u64);
            return Ok((st, batch_parents, batch_thin));
        }
        self.confirm_parents.ensure_plans(&work);

        let mut height_tx_fks: Vec<(u32, Vec<Fk>)> = Vec::with_capacity(work.len());
        // Full-decoded cache bodies for wave (decode once).
        let mut body_fulls: Vec<(
            Fk,
            u32,
            rbitcoin_store::TxRecord,
            Vec<rbitcoin_store::OutputRecord>,
            Vec<rbitcoin_store::InputRecord>,
        )> = Vec::new();
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

            let mut height_fks_resolved: Vec<(Fk, Option<(u64, u64)>)> =
                Vec::with_capacity(fks_work.len());
            // Bulk idx range resolve (concurrent) — kernel can schedule many reads.
            let range_fks: Vec<Fk> = fks_work
                .iter()
                .copied()
                .filter(|fk| fk.get().is_some())
                .collect();
            let ranges = self.store.tx_body_range_batch(&range_fks)?;
            for (fk, range_opt) in range_fks.iter().zip(ranges.into_iter()) {
                if let Some((off, len)) = range_opt {
                    height_fks_resolved.push((*fk, Some((off, len))));
                } else {
                    height_fks_resolved.push((*fk, None));
                }
            }

            // Full body decode (skip store when cache already has the body).
            // Partition: cache hits vs need store bulk full decode.
            let mut need_full: Vec<(Fk, u64, u64)> = Vec::new();
            let mut need_full_meta: Vec<(usize, Fk)> = Vec::new(); // index into height_fks_resolved
            for (i, &(fk, range)) in height_fks_resolved.iter().enumerate() {
                if self.confirm_cancelled() {
                    crate::confirm_load_stats::note(&st, t0.elapsed().as_nanos() as u64);
                    return Err(StoreError::Cancelled("confirm cancelled"));
                }
                let Some(id) = fk.get() else {
                    continue;
                };
                if let Some((off, len)) = range {
                    need_full_meta.push((i, fk));
                    need_full.push((fk, off, len));
                } else {
                    // Rare: no range — sequential fallback.
                    let (tx, inputs, outs) = self.store.get_tx_full(fk)?;
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
                    body_fulls.push((fk, height, tx, outs, inputs));
                    st.creates_registered = st.creates_registered.saturating_add(1);
                }
            }
            if !need_full.is_empty() {
                let decoded = self.store.get_tx_full_batch_at(&need_full)?;
                for ((_, fk), got) in need_full_meta.iter().zip(decoded.into_iter()) {
                    let Some(id) = fk.get() else {
                        continue;
                    };
                    let Some((tx, inputs, outs)) = got else {
                        continue;
                    };
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
                    body_fulls.push((*fk, height, tx, outs, inputs));
                    st.creates_registered = st.creates_registered.saturating_add(1);
                }
            }
            st.body_decode_ns = st
                .body_decode_ns
                .saturating_add(t_dec.elapsed().as_nanos() as u64);
            height_tx_fks.push((height, tx_fks));
        }

        // ── Cache put (create outs FIFO; idx→body via store) ───────────────
        let t_put = Instant::now();
        self.confirm_parents.put_bodies_batch(body_fulls);
        st.cache_put_ns = st
            .cache_put_ns
            .saturating_add(t_put.elapsed().as_nanos() as u64);

        // ── Thin edges: stamped create_fk only (schema v10) ────────────────
        // Soft prev_txid / sticky / head resolve removed — archive stamps create_fk.
        let t_thin = Instant::now();
        let t_edge = Instant::now();
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
        st.thin_edge_ns = st
            .thin_edge_ns
            .saturating_add(t_edge.elapsed().as_nanos() as u64);
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

        let mut pin_new_jobs: Vec<(u64, Vec<u32>, Option<(u64, u64)>)> = Vec::new();

        let t_body = Instant::now();
        let body_keys: Vec<(u64, &[u32])> = pin_jobs
            .iter()
            .filter(|(_, vouts)| !vouts.is_empty())
            .map(|(pid, vouts)| (*pid, vouts.as_slice()))
            .collect();
        let mut body_hits = self
            .confirm_parents
            .get_bodies_for_pin_batch(&body_keys);

        for (pid, need_vouts) in pin_jobs {
            if self.confirm_cancelled() {
                crate::confirm_load_stats::note(&st, t0.elapsed().as_nanos() as u64);
                return Err(StoreError::Cancelled("confirm cancelled"));
            }
            let fk = Fk(pid);
            if !need_vouts.is_empty() {
                if let Some((create_h, tx, outs, cb_hint)) = body_hits.remove(&pid) {
                    st.pin_cache_body = st.pin_cache_body.saturating_add(1);
                    // Same-batch creates: no durable spenders yet — all body
                    // outs for need_vouts are live (move, no re-clone).
                    let same_batch = batch_create_ids.contains(&pid);
                    let live = if same_batch {
                        outs
                    } else {
                        // Range from tx.idx (no process-local body_range cache).
                        let range = self.store.tx_body_range(fk).ok();
                        let t_sp = Instant::now();
                        let unspent = self
                            .store
                            .unspent_create_vouts(fk, &need_vouts, range)
                            .unwrap_or_else(|_| need_vouts.clone());
                        st.pin_spent_ns = st
                            .pin_spent_ns
                            .saturating_add(t_sp.elapsed().as_nanos() as u64);
                        let unspent_set: HashSet<u32> = unspent.into_iter().collect();
                        let mut live = Vec::with_capacity(outs.len());
                        for (v, o) in outs {
                            if unspent_set.contains(&v) {
                                live.push((v, o));
                            }
                        }
                        live
                    };
                    let cb_stash = match cb_hint {
                        Some(true) => Some(Some(create_h)),
                        Some(false) => Some(None),
                        None => {
                            let range = self.store.tx_body_range(fk).ok();
                            match self.resolve_parent_coinbase_height(
                                fk,
                                tx.input_count,
                                range,
                            ) {
                                Ok(v) => Some(v),
                                Err(_) => Some(None),
                            }
                        }
                    };
                    // Move tx/live/need_vouts into batch map (single insert).
                    batch_parents.insert_owned(fk, tx, live, need_vouts, cb_stash);
                    st.utxo_parents = st.utxo_parents.saturating_add(1);
                    continue;
                }
            }
            st.pin_new = st.pin_new.saturating_add(1);
            let range = self.store.tx_body_range(fk).ok();
            pin_new_jobs.push((pid, need_vouts, range));
        }
        st.pin_body_ns = st
            .pin_body_ns
            .saturating_add(t_body.elapsed().as_nanos() as u64);

        let t_new = Instant::now();
        if self.confirm_cancelled() {
            crate::confirm_load_stats::note(&st, t0.elapsed().as_nanos() as u64);
            return Err(StoreError::Cancelled("confirm cancelled"));
        }
        let mut bulk_idx: Vec<usize> = Vec::new();
        let mut bulk_ranges: Vec<(u64, u64)> = Vec::new();
        for (i, (_pid, need_vouts, range)) in pin_new_jobs.iter().enumerate() {
            if let Some((off, len)) = range {
                if !need_vouts.is_empty() {
                    bulk_idx.push(i);
                    bulk_ranges.push((*off, *len));
                }
            }
        }
        let bulk_decoded = if bulk_ranges.is_empty() {
            Vec::new()
        } else {
            self.store
                .get_tx_meta_and_outputs_batch_at(&bulk_ranges)?
        };
        let mut bulk_by_job: HashMap<
            usize,
            (rbitcoin_store::TxRecord, Vec<rbitcoin_store::OutputRecord>),
        > = HashMap::with_capacity(bulk_idx.len());
        for (ji, got) in bulk_idx.into_iter().zip(bulk_decoded.into_iter()) {
            if let Some(v) = got {
                bulk_by_job.insert(ji, v);
            }
        }

        for (ji, (pid, need_vouts, range)) in pin_new_jobs.into_iter().enumerate() {
            if self.confirm_cancelled() {
                crate::confirm_load_stats::note(&st, t0.elapsed().as_nanos() as u64);
                return Err(StoreError::Cancelled("confirm cancelled"));
            }
            let fk = Fk(pid);
            if let Some((off, len)) = range {
                if !need_vouts.is_empty() {
                    if let Some((tx, outs)) = bulk_by_job.remove(&ji) {
                        let t_sp = Instant::now();
                        let unspent = self
                            .store
                            .unspent_create_vouts(fk, &need_vouts, Some((off, len)))
                            .unwrap_or_default();
                        st.pin_spent_ns = st
                            .pin_spent_ns
                            .saturating_add(t_sp.elapsed().as_nanos() as u64);
                        let unspent_set: HashSet<u32> = unspent.into_iter().collect();
                        let mut live = Vec::with_capacity(unspent_set.len());
                        for &v in &need_vouts {
                            if unspent_set.contains(&v) {
                                if let Some(o) = outs.get(v as usize) {
                                    live.push((v, o.clone()));
                                }
                            }
                        }
                        let cb_stash = match self.resolve_parent_coinbase_height(
                            fk,
                            tx.input_count,
                            Some((off, len)),
                        ) {
                            Ok(v) => Some(v),
                            Err(_) => None,
                        };
                        batch_parents.insert_owned(fk, tx, live, need_vouts, cb_stash);
                        st.full_tx_reads = st.full_tx_reads.saturating_add(1);
                    }
                }
            } else if !need_vouts.is_empty() {
                if let Ok((tx, outs)) = self.store.get_tx_meta_and_outputs(fk) {
                    let t_sp = Instant::now();
                    let unspent = self
                        .store
                        .unspent_create_vouts(fk, &need_vouts, None)
                        .unwrap_or_default();
                    st.pin_spent_ns = st
                        .pin_spent_ns
                        .saturating_add(t_sp.elapsed().as_nanos() as u64);
                    let unspent_set: HashSet<u32> = unspent.into_iter().collect();
                    let mut live = Vec::with_capacity(unspent_set.len());
                    for &v in &need_vouts {
                        if unspent_set.contains(&v) {
                            if let Some(o) = outs.get(v as usize) {
                                live.push((v, o.clone()));
                            }
                        }
                    }
                    let cb_stash = match self
                        .resolve_parent_coinbase_height(fk, tx.input_count, None)
                    {
                        Ok(v) => Some(v),
                        Err(_) => None,
                    };
                    batch_parents.insert_owned(fk, tx, live, need_vouts, cb_stash);
                    st.full_tx_reads = st.full_tx_reads.saturating_add(1);
                }
            }
            st.utxo_parents = st.utxo_parents.saturating_add(1);
        }
        st.pin_new_meta_ns = st
            .pin_new_meta_ns
            .saturating_add(t_new.elapsed().as_nanos() as u64);

        // Parents already moved into BatchParents; thin stays batch-local.
        st.pin_put_ns = 0;
        st.parent_pin_ns = st
            .parent_pin_ns
            .saturating_add(t_par.elapsed().as_nanos() as u64);

        batch_thin = thin_by_spend;

        let scanned: Vec<u32> = height_tx_fks.iter().map(|(h, _)| *h).collect();
        if !scanned.is_empty() {
            self.confirm_parents.mark_scanned_many(&scanned);
        }

        crate::confirm_load_stats::note(&st, t0.elapsed().as_nanos() as u64);
        Ok((st, batch_parents, batch_thin))
    }

    pub fn load_confirm_parents_for_hashes(
        &self,
        hashes: &[[u8; 32]],
    ) -> Result<(ConfirmLoadStats, BatchParents, BatchThin), QueryError> {
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
