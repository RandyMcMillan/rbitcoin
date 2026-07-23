//! Confirm **load** stage: Class A decode + parent pin/mlock for one claimed batch.
//!
//! For each height in the batch (ascending):
//! 1. **Cache** header + `header_txs` + body ranges.
//! 2. **Full Class A decode** into `by_body` (wire/assemble clone from here).
//! 3. **Thin edges** (stamped create_fk) + **sparse parent pin** (spent-filtered outs).
//! 4. **`mlock`** (default on; `RBITCOIN_CONFIRM_MLOCK=0` off): **only parent
//!    create `tx.body` pages** that write will annotate. Refcounted by needing
//!    batch heights; tip GC after write munlocks when no active batch needs them.
//!
//! No background worker / no wave fill: load owns the batch's Class A + parents.
//! Bodies stay in `by_body` (post-tip LRU) for pin hits; thin edges + `by_fk`
//! stay until tip GC after write.
//!
//! Env: `RBITCOIN_CONFIRM_MLOCK` (legacy `RBITCOIN_PARENT_PREWARM_MLOCK` alias).

use super::*;
use crate::confirm_parent_cache::{confirm_mlock_from_env, StashedThinInput};
use std::collections::{HashMap, HashSet};
use std::time::Instant;

#[derive(Debug, Default, Clone, Copy)]
pub struct ConfirmLoadStats {
    pub blocks: u32,
    pub utxo_parents: u32,
    pub reserved: u32,
    pub creates_registered: u32,
    pub already_ready: u32,
    /// Unique parent create fks pinned this call (after dedup).
    pub parent_unique: u32,
    /// Of `parent_unique`: outs already stashed in by_fk — re-pin touch only.
    pub pin_already_cached: u32,
    /// Of `parent_unique`: filled from cache `by_body` (no Class A re-decode).
    pub pin_cache_body: u32,
    /// Of `parent_unique`: first-time sparse pin (store decode).
    pub pin_new: u32,
    /// Cover miss: no by_fk row (among uncovered parents).
    pub pin_cover_miss_no_fk: u32,
    /// Cover miss: by_fk present but checked incomplete for need vouts.
    pub pin_cover_miss_partial: u32,
    /// Wall ns in `unspent_create_vouts` during pin (store spent-filter).
    pub pin_spent_ns: u64,
    /// Wall ns in pin-loop mlock helpers.
    pub pin_mlock_ns: u64,
    /// Same-batch create edges (identity known in-batch). Not pin body hit rate.
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
    pub body_mlock_ns: u64,
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
    pub mlock_syscalls: u32,
    pub mlock_skipped: u32,
    pub edge_same_batch: u32,
    pub edge_cache: u32,
    pub edge_head: u32,
    pub edge_coinbase: u32,
}

impl Query {
    /// Batch-mlock **unique** page-aligned body spans for many parents.
    ///
    /// Each item is `(need_heights, body_offset, body_len)`. Overlapping /
    /// adjacent pages merge so one kernel `mlock` covers many parents. Notes
    /// need_heights **immediately per merged span** so later spans in the same
    /// batch can skip already-held pages (do not regress “note only at end”).
    ///
    /// Returns `(syscalls, skipped)`.
    fn mlock_unique_body_ranges(&self, items: &[(Vec<u32>, u64, u64)]) -> (u32, u32) {
        if items.is_empty() {
            return (0, 0);
        }
        const PAGE: u64 = 4096;
        // (page_start, page_len, heights_idx into a side table) — expand then merge.
        let mut spans: Vec<(u64, u64, usize)> = Vec::with_capacity(items.len());
        let mut height_sets: Vec<Vec<u32>> = Vec::with_capacity(items.len());
        for (hs, off, len) in items {
            if *len == 0 || hs.is_empty() {
                continue;
            }
            let start = *off & !(PAGE - 1);
            let end = off.saturating_add(*len).saturating_add(PAGE - 1) & !(PAGE - 1);
            let plen = end.saturating_sub(start);
            if plen == 0 {
                continue;
            }
            let idx = height_sets.len();
            height_sets.push(hs.clone());
            spans.push((start, plen, idx));
        }
        if spans.is_empty() {
            return (0, 0);
        }
        spans.sort_unstable_by_key(|(s, _, _)| *s);
        // Merge overlapping/adjacent; union height set indices.
        let mut merged: Vec<(u64, u64, HashSet<usize>)> = Vec::with_capacity(spans.len());
        for (s, l, idx) in spans {
            if let Some((ms, ml, set)) = merged.last_mut() {
                let mend = ms.saturating_add(*ml);
                if s <= mend {
                    let new_end = s.saturating_add(l).max(mend);
                    *ml = new_end.saturating_sub(*ms);
                    set.insert(idx);
                    continue;
                }
            }
            let mut set = HashSet::with_capacity(1);
            set.insert(idx);
            merged.push((s, l, set));
        }
        let mut syscalls = 0u32;
        let mut skipped = 0u32;
        for (ps, pl, idx_set) in merged {
            // Union need heights for this span.
            let mut heights: Vec<u32> = Vec::new();
            for i in idx_set {
                heights.extend(height_sets[i].iter().copied());
            }
            heights.sort_unstable();
            heights.dedup();
            if heights.is_empty() {
                continue;
            }
            let candidate = rbitcoin_store::MlockRange {
                table: rbitcoin_store::MlockTable::TxBody,
                page_start: ps,
                page_len: pl,
            };
            let noted: Vec<rbitcoin_store::MlockRange> =
                if self.confirm_parents.is_range_pinned(&candidate) {
                    skipped = skipped.saturating_add(1);
                    vec![candidate]
                } else {
                    let ml = self.store.mlock_tx_body_at(ps, pl);
                    syscalls = syscalls.saturating_add(1);
                    if ml.is_empty() {
                        vec![candidate]
                    } else {
                        ml
                    }
                };
            // Note immediately so the next merged span can skip shared pages.
            self.confirm_parents
                .note_mlock_ranges_for_heights(&heights, &noted);
        }
        (syscalls, skipped)
    }

    pub fn parent_cache_ready_through(&self) -> u32 {
        self.confirm_parents.ready_through()
    }

    /// Snapshot: `(ready_through, ahead, sparse_parents, bodies, plans)`.
    ///
    /// `ahead` is ready_through − tip (in-flight load watermark, not a depth knobs).
    /// Third field is sparse `by_fk` parent count.
    pub fn parent_cache_perf_snapshot(&self) -> (u32, u32, usize, usize, usize) {
        let tip = self.tip_height().map(|h| h.0).unwrap_or(0);
        let through = self.confirm_parents.ready_through();
        let ahead = through.saturating_sub(tip);
        (
            through,
            ahead,
            self.confirm_parents.parent_count(),
            self.confirm_parents.body_count(),
            self.confirm_parents.plan_count(),
        )
    }

    /// Unique mlocked cache pages in bytes (parent cache pins).
    pub fn confirm_mlock_bytes(&self) -> u64 {
        self.confirm_parents.mlock_bytes()
    }

    /// `(range_count, unique_page_bytes)` for mlock diagnostics.
    pub fn confirm_mlock_stats(&self) -> (usize, u64) {
        self.confirm_parents.mlock_stats()
    }

    pub fn advance_parent_cache_tip(&self, tip: u32) {
        let unlocks = self.confirm_parents.advance_tip(tip);
        for r in &unlocks {
            self.store.munlock_range(r);
        }
    }

    pub fn seed_parent_cache(&self, items: &[(u32, [u8; 32])]) {
        self.confirm_parents.ensure_plans(items);
    }

    pub fn is_confirm_load_ready(&self, heights: &[u32]) -> bool {
        self.confirm_parents.all_ready(heights)
    }

    /// Wait until every height is load-ready (Condvar). Used by tests / cancel;
    /// production load is inline ([`Self::load_confirm_parents`]), not wait-based.
    pub fn wait_confirm_load_ready(
        &self,
        heights: &[u32],
        timeout: std::time::Duration,
    ) -> Result<(), QueryError> {
        if heights.is_empty() {
            return Ok(());
        }
        match self.confirm_parents.wait_heights_ready(heights, timeout, || {
            self.confirm_cancelled()
        }) {
            Ok(()) => Ok(()),
            Err(true) => Err(StoreError::Cancelled("confirm cancelled")),
            // Message must contain "load incomplete" so the confirm engine
            // re-queues the batch instead of treating a wait timeout as a
            // permanent reject.
            Err(false) => Err(StoreError::Corrupt(
                "confirm: load incomplete (parent package not ready, timeout)",
            )),
        }
    }

    /// Load Class A for heights into the confirm parent cache (load stage / tests).
    ///
    /// Full-decode bodies + parent pin; mlock **parent create body pages only**
    /// (write spender annotate). Pins every parent needed by heights in `items`
    /// (no tip-near pin window — batch is the unit of work).
    pub fn load_confirm_parents(
        &self,
        items: &[(u32, [u8; 32])],
    ) -> Result<ConfirmLoadStats, QueryError> {
        let t0 = Instant::now();
        let mut st = ConfirmLoadStats::default();
        if items.is_empty() {
            return Ok(st);
        }
        let tip = self.tip_height().map(|h| h.0).unwrap_or(0);

        let mut work: Vec<(u32, [u8; 32])> = Vec::new();
        for &(height, hash) in items {
            if self.confirm_cancelled() {
                crate::confirm_load_stats::note(&st, t0.elapsed().as_nanos() as u64);
                return Err(StoreError::Cancelled("confirm cancelled"));
            }
            if height <= tip {
                continue;
            }
            // Skip if already scanned this height (2-stage ready) with matching hash.
            if self.confirm_parents.is_ready(height) {
                if self
                    .confirm_parents
                    .get_header_plan(height)
                    .is_some_and(|p| p.header_rec.hash == hash)
                {
                    st.already_ready = st.already_ready.saturating_add(1);
                    continue;
                }
            }
            // Incomplete or hash mismatch: re-decode / re-pin.
            work.push((height, hash));
        }
        if work.is_empty() {
            crate::confirm_load_stats::note(&st, t0.elapsed().as_nanos() as u64);
            return Ok(st);
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
        let mut thin_by_spend: HashMap<u64, Vec<StashedThinInput>> = HashMap::new();
        let mut batch_create_ids: HashSet<u64> = HashSet::new();
        let mut body_ranges: Vec<(Fk, u64, u64)> = Vec::new();

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

            // ── body ranges (decode); mlock only write parent pages later ─
            // Wave bodies / Class C are read then written on write via
            // strong/height — those pages are small/sparse; the multi-page
            // write path that benefits from mlock is parent create bodies
            // (spender annotate). See pin loop below.
            let t_ml = Instant::now();
            st.blocks = st.blocks.saturating_add(1);

            let fks_work: std::borrow::Cow<'_, [Fk]> = if tx_fks_is_sorted_ascending(&tx_fks) {
                std::borrow::Cow::Borrowed(tx_fks.as_slice())
            } else {
                let mut v = tx_fks.clone();
                v.sort_unstable_by_key(|f| f.0);
                std::borrow::Cow::Owned(v)
            };

            let mut height_fks_resolved: Vec<(Fk, Option<(u64, u64)>)> =
                Vec::with_capacity(fks_work.len());
            for fk in fks_work.iter() {
                if fk.get().is_none() {
                    continue;
                }
                match self.store.tx_body_range(*fk) {
                    Ok((off, len)) => {
                        body_ranges.push((*fk, off, len));
                        height_fks_resolved.push((*fk, Some((off, len))));
                    }
                    Err(_) => {
                        height_fks_resolved.push((*fk, None));
                    }
                }
            }
            st.body_mlock_ns = st
                .body_mlock_ns
                .saturating_add(t_ml.elapsed().as_nanos() as u64);

            // ── Full body decode (skip store when cache already has the body) ─
            let t_dec = Instant::now();
            for &(fk, range) in &height_fks_resolved {
                if self.confirm_cancelled() {
                    crate::confirm_load_stats::note(&st, t0.elapsed().as_nanos() as u64);
                    return Err(StoreError::Cancelled("confirm cancelled"));
                }
                let Some(id) = fk.get() else {
                    continue;
                };
                // Sliding-window re-touch: body still in cache → no store re-decode.
                if let Some((txid, prevouts)) = self.confirm_parents.body_prevout_edges(fk) {
                    batch_create_ids.insert(id);
                    body_prevouts.insert(id, (txid, prevouts));
                    st.creates_registered = st.creates_registered.saturating_add(1);
                    continue;
                }
                let (tx, inputs, outs) = if let Some((off, len)) = range {
                    self.store.get_tx_full_at(off, len)?
                } else {
                    self.store.get_tx_full(fk)?
                };
                st.body_tx_reads = st.body_tx_reads.saturating_add(1);
                // v10: create_fk on inputs — soft prev_txid zero when fk stamped.
                let prevouts: Vec<(Option<u64>, [u8; 32], u32)> = inputs
                    .iter()
                    .map(|i| {
                        let soft = if i.create_fk.is_null() {
                            i.prev_txid
                        } else {
                            [0u8; 32]
                        };
                        (i.create_fk.get(), soft, i.prev_index)
                    })
                    .collect();
                batch_create_ids.insert(id);
                body_prevouts.insert(id, (tx.txid, prevouts));
                body_fulls.push((fk, height, tx, outs, inputs));
                st.creates_registered = st.creates_registered.saturating_add(1);
            }
            st.body_decode_ns = st
                .body_decode_ns
                .saturating_add(t_dec.elapsed().as_nanos() as u64);
            height_tx_fks.push((height, tx_fks));
        }

        // ── Cache put (bodies + ranges) ────────────────────────────────────
        let t_put = Instant::now();
        self.confirm_parents.put_body_ranges_batch(&body_ranges);
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
                let mut edges: Vec<StashedThinInput> = Vec::with_capacity(prevouts.len());
                for &(create_fk_opt, _prev_txid, prev_index) in prevouts {
                    if create_fk_opt.is_none() && prev_index == u32::MAX {
                        edges.push(StashedThinInput {
                            create_fk: None,
                            prev_index,
                        });
                        st.edge_coinbase = st.edge_coinbase.saturating_add(1);
                        continue;
                    }
                    if let Some(pid) = create_fk_opt {
                        edges.push(StashedThinInput {
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
                    edges.push(StashedThinInput {
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

        // ── Pin parents (sparse outs) + mlock parent create body pages only ─
        // Write annotates spenders into create outputs; keep those body pages
        // resident until tip GC (need_heights refcount → 0).
        let do_mlock = confirm_mlock_from_env();
        let t_par = Instant::now();
        let mut uniq_parents: Vec<u64> = parent_need.keys().copied().collect();
        uniq_parents.sort_unstable();
        st.parent_unique = st.parent_unique.saturating_add(uniq_parents.len() as u32);

        // Build need lists once; cover-check under one cache lock.
        let mut pin_jobs: Vec<(u64, u32, Vec<u32>, Vec<u32>)> =
            Vec::with_capacity(uniq_parents.len());
        for pid in uniq_parents {
            let mut need_hs = parent_need.remove(&pid).unwrap_or_default();
            need_hs.sort_unstable();
            need_hs.dedup();
            let max_h = need_hs.last().copied().unwrap_or(0);
            let mut need_vouts = parent_vouts.remove(&pid).unwrap_or_default();
            need_vouts.sort_unstable();
            need_vouts.dedup();
            pin_jobs.push((pid, max_h, need_hs, need_vouts));
        }
        let cover_keys: Vec<(u64, Vec<u32>)> = pin_jobs
            .iter()
            .map(|(pid, _, _, vouts)| (*pid, vouts.clone()))
            .collect();
        let (covered, miss_no_fk, miss_partial) =
            self.confirm_parents.parent_pins_covered_detail(&cover_keys);
        st.pin_cover_miss_no_fk = miss_no_fk;
        st.pin_cover_miss_partial = miss_partial;
        let mut touch_batch: Vec<(u32, u64, Vec<u32>)> = Vec::new();
        let mut parent_ranges: Vec<(Fk, u64, u64)> = Vec::new();
        // Unique-range mlock: collect (need_heights, off, len) for cover + pin_new;
        // one page-merged pass notes immediately per span (skip works in-batch).
        // Body-LRU pin_cache skips kernel mlock (pages already in RAM).
        let mut mlock_items: Vec<(Vec<u32>, u64, u64)> = Vec::new();
        let mut mlock_no_range: Vec<(Vec<u32>, Fk)> = Vec::new();
        // (max_height, fk, tx, live outs, checked vouts, coinbase_height, create_height)
        let mut sparse_parents: Vec<(
            u32,
            Fk,
            rbitcoin_store::TxRecord,
            Vec<(u32, rbitcoin_store::OutputRecord)>,
            Vec<u32>,
            Option<Option<u32>>,
            Option<u32>,
        )> = Vec::with_capacity(pin_jobs.len());

        // Split covered vs rest; batch body-LRU pin under one cache lock (slim outs).
        let mut uncovered: Vec<(usize, u64, u32, Vec<u32>, Vec<u32>)> = Vec::new();
        for (i, (pid, max_h, need_hs, need_vouts)) in pin_jobs.into_iter().enumerate() {
            if covered.get(i).copied().unwrap_or(false) {
                st.pin_already_cached = st.pin_already_cached.saturating_add(1);
                for &h in &need_hs {
                    touch_batch.push((h, pid, need_vouts.clone()));
                }
                let fk = Fk(pid);
                if do_mlock {
                    if let Some((off, len)) = self
                        .confirm_parents
                        .get_body_range(fk)
                        .or_else(|| self.store.tx_body_range(fk).ok())
                    {
                        mlock_items.push((need_hs, off, len));
                    } else {
                        mlock_no_range.push((need_hs, fk));
                    }
                }
                st.utxo_parents = st.utxo_parents.saturating_add(1);
            } else {
                uncovered.push((i, pid, max_h, need_hs, need_vouts));
            }
        }

        let body_pin_keys: Vec<(u64, Vec<u32>)> = uncovered
            .iter()
            .filter(|(_, _, _, _, vouts)| !vouts.is_empty())
            .map(|(_, pid, _, _, vouts)| (*pid, vouts.clone()))
            .collect();
        let body_hits = self
            .confirm_parents
            .get_bodies_for_pin_batch(&body_pin_keys);

        // pin_new jobs: resolve ranges first so unique-range mlock runs before
        // meta/out reads (warm pages), then decode.
        let mut pin_new_jobs: Vec<(u64, u32, Vec<u32>, Vec<u32>, Option<(u64, u64)>)> =
            Vec::new();
        for (_i, pid, max_h, need_hs, need_vouts) in uncovered {
            if self.confirm_cancelled() {
                crate::confirm_load_stats::note(&st, t0.elapsed().as_nanos() as u64);
                return Err(StoreError::Cancelled("confirm cancelled"));
            }
            let fk = Fk(pid);
            // Prefer cache body (runway / post-confirm LRU) — slim clone, no full inputs.
            if !need_vouts.is_empty() {
                if let Some((create_h, tx, outs, cb_hint, range)) = body_hits.get(&pid) {
                    st.pin_cache_body = st.pin_cache_body.saturating_add(1);
                    if let Some((off, len)) = *range {
                        parent_ranges.push((fk, off, len));
                    }
                    // Only spent-filter vouts not already in by_fk.checked.
                    let already = self.confirm_parents.parent_checked_vouts(fk, &need_vouts);
                    let already_set: HashSet<u32> = already.iter().copied().collect();
                    let need_filter: Vec<u32> = need_vouts
                        .iter()
                        .copied()
                        .filter(|v| !already_set.contains(v))
                        .collect();
                    let t_sp = Instant::now();
                    let mut unspent_set: HashSet<u32> = already_set;
                    if !need_filter.is_empty() {
                        if let Ok(u) =
                            self.store
                                .unspent_create_vouts(fk, &need_filter, *range)
                        {
                            unspent_set.extend(u);
                        } else {
                            unspent_set.extend(need_filter.iter().copied());
                        }
                    }
                    st.pin_spent_ns = st
                        .pin_spent_ns
                        .saturating_add(t_sp.elapsed().as_nanos() as u64);
                    let mut live = Vec::with_capacity(unspent_set.len());
                    for (v, o) in outs {
                        if unspent_set.contains(v) {
                            live.push((*v, o.clone()));
                        }
                    }
                    let cb_stash = match *cb_hint {
                        Some(true) => Some(Some(*create_h)),
                        Some(false) => Some(None),
                        None => {
                            // Ambiguous 1-in: fall back to store height if available.
                            match self.resolve_parent_coinbase_height(
                                fk,
                                tx.input_count,
                                *range,
                            ) {
                                Ok(v) => Some(v),
                                Err(_) => Some(None),
                            }
                        }
                    };
                    sparse_parents.push((
                        max_h,
                        fk,
                        tx.clone(),
                        live,
                        need_vouts.clone(),
                        cb_stash,
                        Some(*create_h),
                    ));
                    for &h in &need_hs {
                        touch_batch.push((h, pid, need_vouts.clone()));
                    }
                    st.utxo_parents = st.utxo_parents.saturating_add(1);
                    continue;
                }
            }
            st.pin_new = st.pin_new.saturating_add(1);
            let range = self
                .confirm_parents
                .get_body_range(fk)
                .or_else(|| self.store.tx_body_range(fk).ok());
            if let Some((off, len)) = range {
                parent_ranges.push((fk, off, len));
                if do_mlock {
                    mlock_items.push((need_hs.clone(), off, len));
                }
            } else if do_mlock {
                mlock_no_range.push((need_hs.clone(), fk));
            }
            pin_new_jobs.push((pid, max_h, need_hs, need_vouts, range));
        }

        // Unique-range mlock before pin_new meta reads (and cover need_height notes).
        if do_mlock && (!mlock_items.is_empty() || !mlock_no_range.is_empty()) {
            let t_ml = Instant::now();
            if !mlock_items.is_empty() {
                let (sys, sk) = self.mlock_unique_body_ranges(&mlock_items);
                st.mlock_syscalls = st.mlock_syscalls.saturating_add(sys);
                st.mlock_skipped = st.mlock_skipped.saturating_add(sk);
            }
            for (need_hs, fk) in &mlock_no_range {
                let body_ml = self.store.mlock_tx_body_only(*fk);
                st.mlock_syscalls = st.mlock_syscalls.saturating_add(1);
                self.confirm_parents
                    .note_mlock_ranges_for_heights(need_hs, &body_ml);
            }
            st.pin_mlock_ns = st
                .pin_mlock_ns
                .saturating_add(t_ml.elapsed().as_nanos() as u64);
        }

        // Sparse outs + spent filter for pin_new (pages warm when mlock on).
        for (pid, max_h, need_hs, need_vouts, range) in pin_new_jobs {
            if self.confirm_cancelled() {
                crate::confirm_load_stats::note(&st, t0.elapsed().as_nanos() as u64);
                return Err(StoreError::Cancelled("confirm cancelled"));
            }
            let fk = Fk(pid);
            if let Some((off, len)) = range {
                if !need_vouts.is_empty() {
                    match self.store.get_tx_meta_and_outputs_at(off, len) {
                        Ok((tx, outs)) => {
                            let t_sp = Instant::now();
                            let unspent = self
                                .store
                                .unspent_create_vouts(fk, &need_vouts, Some((off, len)))
                                .unwrap_or_default();
                            st.pin_spent_ns = st
                                .pin_spent_ns
                                .saturating_add(t_sp.elapsed().as_nanos() as u64);
                            let unspent_set: std::collections::HashSet<u32> =
                                unspent.into_iter().collect();
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
                            sparse_parents.push((
                                max_h,
                                fk,
                                tx,
                                live,
                                need_vouts.clone(),
                                cb_stash,
                                None,
                            ));
                            for &h in &need_hs {
                                touch_batch.push((h, pid, need_vouts.clone()));
                            }
                            st.full_tx_reads = st.full_tx_reads.saturating_add(1);
                        }
                        Err(_) => {
                            // Leave wave to store-decode; range still registered.
                        }
                    }
                }
            } else if !need_vouts.is_empty() {
                // No range: try idx-based outs for sparse stash (rare).
                if let Ok((tx, outs)) = self.store.get_tx_meta_and_outputs(fk) {
                    let t_sp = Instant::now();
                    let unspent = self
                        .store
                        .unspent_create_vouts(fk, &need_vouts, None)
                        .unwrap_or_default();
                    st.pin_spent_ns = st
                        .pin_spent_ns
                        .saturating_add(t_sp.elapsed().as_nanos() as u64);
                    let unspent_set: std::collections::HashSet<u32> =
                        unspent.into_iter().collect();
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
                    sparse_parents.push((
                        max_h,
                        fk,
                        tx,
                        live,
                        need_vouts.clone(),
                        cb_stash,
                        None,
                    ));
                    for &h in &need_hs {
                        touch_batch.push((h, pid, need_vouts.clone()));
                    }
                    st.full_tx_reads = st.full_tx_reads.saturating_add(1);
                }
            }
            st.utxo_parents = st.utxo_parents.saturating_add(1);
        }
        // Put by_fk first so touch can attach need_fk for pure pin_new parents
        // (touch before put was a no-op when neither by_fk nor by_body held them).
        if !sparse_parents.is_empty() {
            self.confirm_parents
                .put_parent_outs_resolved_batch(&sparse_parents);
        }
        if !parent_ranges.is_empty() {
            self.confirm_parents.put_body_ranges_batch(&parent_ranges);
        }
        if !touch_batch.is_empty() {
            self.confirm_parents
                .touch_parent_needs_batch(&touch_batch);
        }
        st.parent_pin_ns = st
            .parent_pin_ns
            .saturating_add(t_par.elapsed().as_nanos() as u64);

        let thin_items: Vec<(Fk, Vec<StashedThinInput>)> = thin_by_spend
            .into_iter()
            .map(|(id, edges)| (Fk(id), edges))
            .collect();
        self.confirm_parents.put_thin_inputs_batch(thin_items);

        // 2-stage semantics: mark every height that got a header+body attempt
        // (present in height_tx_fks). Confirm wait unblocks on scanned; wave
        // store-fallbacks residual parent misses — same work as pre-pipeline,
        // not a stricter package_ready gate that delayed confirm for no gain.
        let scanned: Vec<u32> = height_tx_fks.iter().map(|(h, _)| *h).collect();
        if !scanned.is_empty() {
            self.confirm_parents.mark_scanned_many(&scanned);
        }

        crate::confirm_load_stats::note(&st, t0.elapsed().as_nanos() as u64);
        Ok(st)
    }

    pub fn load_confirm_parents_for_hashes(
        &self,
        hashes: &[[u8; 32]],
    ) -> Result<ConfirmLoadStats, QueryError> {
        let tip = self.tip_height().map(|h| h.0).unwrap_or(0);
        let mut items = Vec::with_capacity(hashes.len());
        for (i, hash) in hashes.iter().enumerate() {
            let h = tip.saturating_add(1).saturating_add(i as u32);
            items.push((h, *hash));
        }
        self.load_confirm_parents(&items)
    }

    /// Retire spent parent outs from the parent cache cache by **create_fk** (schema v10).
    ///
    /// Prefer this over txid lookup: spends are already stamped with create_fk at
    /// connect time.
    pub fn unpin_spent_parent_outs(
        &self,
        spends: &[(Fk, u32)],
    ) -> Result<(), QueryError> {
        if spends.is_empty() {
            return Ok(());
        }
        self.confirm_parents.retire_spends(spends);
        Ok(())
    }
}

/// True when `fks` is empty or non-decreasing by Class A id (archive order).
#[inline]
fn tx_fks_is_sorted_ascending(fks: &[Fk]) -> bool {
    fks.windows(2).all(|w| w[0].0 <= w[1].0)
}
