//! Confirm-runway prewarm: RAM-cache lookups; optional mlock; full-decode bodies once.
//!
//! For each height in the batch (ascending):
//! 1. **Cache**: header + `header_txs`, body ranges.
//! 2. **`mlock`** (default on; `RBITCOIN_PARENT_PREWARM_MLOCK=0` to disable): body + Class C.
//! 3. **Full Class A decode** into `by_body` (wave/wire reuse).
//! 4. **Thin edges** (default create_fk-only) + **parent pin** (full depth by default;
//!    `PIN_NEAR=K` limits to tip+1‥tip+K).
//!
//! Env: `RBITCOIN_PARENT_PREWARM_{DEPTH,BATCH,HEADROOM,MLOCK,PIN_NEAR,THIN_CREATE_FK_ONLY}`.

use super::*;
use crate::confirm_parent_cache::{
    prewarm_headroom_from_env, prewarm_mlock_from_env, prewarm_pin_near_from_env,
    prewarm_thin_create_fk_only_from_env, StashedThinInput,
};
use std::collections::{HashMap, HashSet};
use std::time::Instant;

#[derive(Debug, Default, Clone, Copy)]
pub struct PrewarmStats {
    pub blocks: u32,
    pub utxo_parents: u32,
    pub reserved: u32,
    pub creates_registered: u32,
    pub already_ready: u32,
    /// Unique parent create fks pinned this call (after dedup).
    pub parent_unique: u32,
    /// Of `parent_unique`: outs already stashed — skip store decode (re-pin).
    pub pin_already_cached: u32,
    /// Of `parent_unique`: first-time sparse pin (store decode).
    pub pin_new: u32,
    /// Parent outs served from runway txid map / same-batch.
    pub parent_cache_hits: u32,
    /// Body txs mlocked + full-decoded (phase 1).
    pub body_tx_reads: u32,
    /// Parent body mlocks (phase 2).
    pub full_tx_reads: u32,
    /// External parents that could not be resolved.
    pub missing_parents: u32,
    /// Phase wall times (ns).
    pub header_ns: u64,
    pub body_mlock_ns: u64,
    pub body_decode_ns: u64,
    pub thin_ns: u64,
    /// Thin sub-phases (sum into `thin_ns`).
    pub thin_collect_ns: u64,
    pub thin_runway_ns: u64,
    pub thin_head_ns: u64,
    pub thin_edge_ns: u64,
    pub parent_pin_ns: u64,
    pub cache_put_ns: u64,
    pub head_lookups: u32,
    pub head_hits: u32,
    pub mlock_syscalls: u32,
    pub mlock_skipped: u32,
    pub edge_same_batch: u32,
    pub edge_runway: u32,
    pub edge_head: u32,
    pub edge_coinbase: u32,
    /// Thin edges resolved via confirmed sticky map.
    pub edge_sticky: u32,
    /// Unique externals resolved via sticky (subset of need_external).
    pub sticky_hits: u32,
}

impl Query {
    /// Note ranges for `height` (always). Prefer calling after an mlock that
    /// skipped already-pinned spans so need_heights stay accurate for GC.
    fn mlock_note_skip_pinned(&self, height: u32, ranges: &[rbitcoin_store::MlockRange]) {
        self.confirm_parents.note_mlock_ranges(height, ranges);
    }

    /// Page-align + merge body `(offset, len)`, `mlock` only spans not already
    /// pinned, note all spans for each of `heights`.
    ///
    /// Returns `(ranges, syscalls, skipped)`.
    fn mlock_body_spans_for_heights(
        &self,
        heights: &[u32],
        abs_ranges: &[(u64, u64)],
    ) -> (Vec<rbitcoin_store::MlockRange>, u32, u32) {
        if abs_ranges.is_empty() || heights.is_empty() {
            return (Vec::new(), 0, 0);
        }
        const PAGE: u64 = 4096;
        let mut spans: Vec<(u64, u64)> = Vec::with_capacity(abs_ranges.len());
        for &(off, len) in abs_ranges {
            if len == 0 {
                continue;
            }
            let start = off & !(PAGE - 1);
            let end = off.saturating_add(len).saturating_add(PAGE - 1) & !(PAGE - 1);
            let plen = end.saturating_sub(start);
            if plen > 0 {
                spans.push((start, plen));
            }
        }
        if spans.is_empty() {
            return (Vec::new(), 0, 0);
        }
        spans.sort_unstable_by_key(|(s, _)| *s);
        let mut merged: Vec<(u64, u64)> = Vec::with_capacity(spans.len());
        for (s, l) in spans {
            if let Some((ms, ml)) = merged.last_mut() {
                let mend = ms.saturating_add(*ml);
                if s <= mend {
                    let new_end = s.saturating_add(l).max(mend);
                    *ml = new_end.saturating_sub(*ms);
                    continue;
                }
            }
            merged.push((s, l));
        }
        let mut noted = Vec::with_capacity(merged.len());
        let mut syscalls = 0u32;
        let mut skipped = 0u32;
        for (ps, pl) in merged {
            let candidate = rbitcoin_store::MlockRange {
                table: rbitcoin_store::MlockTable::TxBody,
                page_start: ps,
                page_len: pl,
            };
            if self.confirm_parents.is_range_pinned(&candidate) {
                noted.push(candidate);
                skipped = skipped.saturating_add(1);
                continue;
            }
            let ml = self.store.mlock_tx_body_at(ps, pl);
            syscalls = syscalls.saturating_add(1);
            if ml.is_empty() {
                noted.push(candidate);
            } else {
                noted.extend(ml);
            }
        }
        for &h in heights {
            self.confirm_parents.note_mlock_ranges(h, &noted);
        }
        (noted, syscalls, skipped)
    }

    pub fn parent_prewarm_depth(&self) -> u32 {
        self.confirm_parents.depth()
    }

    pub fn parent_prewarm_ready_through(&self) -> u32 {
        self.confirm_parents.ready_through()
    }

    /// Snapshot: `(ready_through, ahead, by_txid, bodies, plans, depth)`.
    ///
    /// `by_txid` is the runway txid map size (should stay O(depth), not O(chain)).
    pub fn parent_prewarm_perf_snapshot(&self) -> (u32, u32, usize, usize, usize, u32) {
        let tip = self.tip_height().map(|h| h.0).unwrap_or(0);
        let through = self.confirm_parents.ready_through();
        let ahead = through.saturating_sub(tip);
        (
            through,
            ahead,
            self.confirm_parents.by_txid_count(),
            self.confirm_parents.body_count(),
            self.confirm_parents.plan_count(),
            self.confirm_parents.depth(),
        )
    }

    /// Unique mlocked runway pages in bytes (confirm prewarm pins).
    pub fn prewarm_mlock_bytes(&self) -> u64 {
        self.confirm_parents.mlock_bytes()
    }

    /// `(range_count, unique_page_bytes)` for mlock diagnostics.
    pub fn prewarm_mlock_stats(&self) -> (usize, u64) {
        self.confirm_parents.mlock_stats()
    }

    pub fn advance_parent_runway_tip(&self, tip: u32) {
        let unlocks = self.confirm_parents.advance_tip(tip);
        for r in &unlocks {
            self.store.munlock_range(r);
        }
    }

    pub fn seed_parent_runway(&self, items: &[(u32, [u8; 32])]) {
        self.confirm_parents.ensure_plans(items);
    }

    pub fn is_prewarm_ready(&self, heights: &[u32]) -> bool {
        self.confirm_parents.all_ready(heights)
    }

    /// Wait until every height is prewarm-ready (Condvar; no Class A load).
    pub fn wait_prewarm_ready(
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
            // Message must contain "prewarm incomplete" so the confirm engine
            // re-queues the batch instead of treating a wait timeout as a
            // permanent script reject (historical 600s live n=32 → reject tip).
            Err(false) => Err(StoreError::Corrupt(
                "confirm: prewarm incomplete (parent package not ready, timeout)",
            )),
        }
    }

    /// Wait for batch ready, then optionally soft-wait headroom (never last-mile).
    pub fn wait_prewarm_ready_with_headroom(
        &self,
        heights: &[u32],
        batch_end: u32,
        headroom: Option<u32>,
        timeout: std::time::Duration,
    ) -> Result<(), QueryError> {
        if heights.is_empty() {
            return Ok(());
        }
        self.wait_prewarm_ready(heights, timeout)?;
        if self.confirm_cancelled() {
            return Err(StoreError::Cancelled("confirm cancelled"));
        }
        let hr = headroom.unwrap_or_else(prewarm_headroom_from_env);
        if hr == 0 || self.confirm_parents.headroom_ready(batch_end, hr) {
            return Ok(());
        }
        // Soft headroom only — tip must not freeze for runway lead.
        // Brief sleeps; mark_scanned notifies will still advance ready_through.
        let soft = std::time::Duration::from_millis(50);
        let start = std::time::Instant::now();
        while start.elapsed() < soft {
            if self.confirm_cancelled() {
                return Err(StoreError::Cancelled("confirm cancelled"));
            }
            if self.confirm_parents.headroom_ready(batch_end, hr) {
                return Ok(());
            }
            std::thread::sleep(std::time::Duration::from_millis(2));
        }
        Ok(())
    }

    /// Prewarm: cache small maps; mlock body + Class C; **full-decode runway bodies**.
    pub fn prewarm_parents_for_heights(
        &self,
        items: &[(u32, [u8; 32])],
    ) -> Result<PrewarmStats, QueryError> {
        let t0 = Instant::now();
        let mut st = PrewarmStats::default();
        if items.is_empty() {
            return Ok(st);
        }
        let tip = self.tip_height().map(|h| h.0).unwrap_or(0);

        let mut work: Vec<(u32, [u8; 32])> = Vec::new();
        for &(height, hash) in items {
            if self.confirm_cancelled() {
                crate::parent_prewarm_stats::note(&st, t0.elapsed().as_nanos() as u64);
                return Err(StoreError::Cancelled("confirm cancelled"));
            }
            if height <= tip {
                continue;
            }
            // package_ready = full confirm handoff (bodies + edges + parents).
            if self.confirm_parents.package_ready(height) {
                // Hash must still match the plan (reorg / re-note).
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
            crate::parent_prewarm_stats::note(&st, t0.elapsed().as_nanos() as u64);
            return Ok(st);
        }
        self.confirm_parents.ensure_plans(&work);

        let mut height_tx_fks: Vec<(u32, Vec<Fk>)> = Vec::with_capacity(work.len());
        // Full-decoded runway bodies for wave (decode once).
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
        let mut batch_creates: HashMap<[u8; 32], Fk> = HashMap::new();
        let mut batch_create_ids: HashSet<u64> = HashSet::new();
        let mut body_ranges: Vec<(Fk, u64, u64)> = Vec::new();

        for &(height, hash) in &work {
            if self.confirm_cancelled() {
                crate::parent_prewarm_stats::note(&st, t0.elapsed().as_nanos() as u64);
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

            // ── optional mlock + body ranges ────────────────────────────────
            let do_mlock = prewarm_mlock_from_env();
            let t_ml = Instant::now();
            st.blocks = st.blocks.saturating_add(1);

            let fks_work: std::borrow::Cow<'_, [Fk]> = if tx_fks_is_sorted_ascending(&tx_fks) {
                std::borrow::Cow::Borrowed(tx_fks.as_slice())
            } else {
                let mut v = tx_fks.clone();
                v.sort_unstable_by_key(|f| f.0);
                std::borrow::Cow::Owned(v)
            };

            let mut height_body_abs: Vec<(u64, u64)> = Vec::with_capacity(fks_work.len());
            let mut height_fks_resolved: Vec<(Fk, Option<(u64, u64)>)> =
                Vec::with_capacity(fks_work.len());
            for fk in fks_work.iter() {
                if fk.get().is_none() {
                    continue;
                }
                match self.store.tx_body_range(*fk) {
                    Ok((off, len)) => {
                        height_body_abs.push((off, len));
                        body_ranges.push((*fk, off, len));
                        height_fks_resolved.push((*fk, Some((off, len))));
                    }
                    Err(_) => {
                        height_fks_resolved.push((*fk, None));
                    }
                }
            }
            if do_mlock {
                self.mlock_note_skip_pinned(
                    height,
                    &self.store.mlock_confirmed_height(height),
                );
                let (_body_ml, sys, sk) =
                    self.mlock_body_spans_for_heights(&[height], &height_body_abs);
                st.mlock_syscalls = st.mlock_syscalls.saturating_add(sys);
                st.mlock_skipped = st.mlock_skipped.saturating_add(sk);
                for &(fk, range) in &height_fks_resolved {
                    if range.is_some() {
                        continue;
                    }
                    let body_ml = self.store.mlock_tx_body_only(fk);
                    st.mlock_syscalls = st.mlock_syscalls.saturating_add(1);
                    self.mlock_note_skip_pinned(height, &body_ml);
                }
                for &(fk, _) in &height_fks_resolved {
                    let cc = self.store.mlock_class_c_tx(fk);
                    self.mlock_note_skip_pinned(height, &cc);
                }
            }
            st.body_mlock_ns = st
                .body_mlock_ns
                .saturating_add(t_ml.elapsed().as_nanos() as u64);

            // ── Full body decode (skip store when runway already has the body) ─
            let t_dec = Instant::now();
            for &(fk, range) in &height_fks_resolved {
                if self.confirm_cancelled() {
                    crate::parent_prewarm_stats::note(&st, t0.elapsed().as_nanos() as u64);
                    return Err(StoreError::Cancelled("confirm cancelled"));
                }
                let Some(id) = fk.get() else {
                    continue;
                };
                // Sliding-window re-touch: body still in cache → no store re-decode.
                if let Some((txid, prevouts)) = self.confirm_parents.body_prevout_edges(fk) {
                    batch_creates.insert(txid, fk);
                    batch_create_ids.insert(id);
                    body_prevouts.insert(id, (txid, prevouts));
                    st.creates_registered = st.creates_registered.saturating_add(1);
                    // by_txid/sticky already registered on first put_bodies_batch.
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
                batch_creates.insert(tx.txid, fk);
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
        // put_bodies_batch already inserts by_txid + sticky via insert_body —
        // no second register_mlocked_creates_batch (that doubled by_txid work).
        let t_put = Instant::now();
        self.confirm_parents.put_body_ranges_batch(&body_ranges);
        self.confirm_parents.put_bodies_batch(body_fulls);
        st.cache_put_ns = st
            .cache_put_ns
            .saturating_add(t_put.elapsed().as_nanos() as u64);

        // ── Thin edges (create_fk-first; optional soft/head legacy) ─────────
        let thin_fk_only = prewarm_thin_create_fk_only_from_env();
        let pin_near = prewarm_pin_near_from_env();
        // 0 = pin all runway heights; else only tip+1‥tip+pin_near.
        let pin_hi = if pin_near == 0 {
            u32::MAX
        } else {
            tip.saturating_add(pin_near)
        };
        let t_thin = Instant::now();

        let mut local_fk: HashMap<[u8; 32], Option<Fk>> =
            HashMap::with_capacity(batch_creates.len().saturating_mul(2));
        for (txid, fk) in &batch_creates {
            local_fk.insert(*txid, Some(*fk));
        }

        let t_collect = Instant::now();
        let mut need_external: HashSet<[u8; 32]> = HashSet::new();
        let mut external_max_h: HashMap<[u8; 32], u32> = HashMap::new();
        if !thin_fk_only {
            // Legacy: collect soft prev_txid for unstamped edges only.
            for (height, tx_fks) in &height_tx_fks {
                for fk in tx_fks {
                    let Some(id) = fk.get() else {
                        continue;
                    };
                    let Some((_txid, prevouts)) = body_prevouts.get(&id) else {
                        continue;
                    };
                    for &(create_fk_opt, prev_txid, prev_index) in prevouts {
                        if create_fk_opt.is_some() {
                            continue;
                        }
                        if prev_txid == [0u8; 32] {
                            continue;
                        }
                        if prev_index == u32::MAX {
                            continue;
                        }
                        if batch_creates.contains_key(&prev_txid) {
                            continue;
                        }
                        need_external.insert(prev_txid);
                        external_max_h
                            .entry(prev_txid)
                            .and_modify(|h| *h = (*h).max(*height))
                            .or_insert(*height);
                    }
                }
            }
        }
        st.thin_collect_ns = st
            .thin_collect_ns
            .saturating_add(t_collect.elapsed().as_nanos() as u64);

        let mut sticky_sourced: HashSet<[u8; 32]> = HashSet::new();
        let mut head_sourced: HashSet<[u8; 32]> = HashSet::new();
        if !need_external.is_empty() {
            let t_runway = Instant::now();
            let need_vec: Vec<[u8; 32]> = need_external.iter().copied().collect();
            let (map_hits, sticky_only) =
                self.confirm_parents.lookup_txids_batch(&need_vec);
            sticky_sourced = sticky_only;
            st.sticky_hits = st
                .sticky_hits
                .saturating_add(sticky_sourced.len() as u32);
            let mut need_head: Vec<[u8; 32]> = Vec::with_capacity(need_vec.len() / 2);
            for txid in &need_vec {
                if let Some(fk) = map_hits.get(txid).copied() {
                    local_fk.insert(*txid, Some(fk));
                } else {
                    need_head.push(*txid);
                }
            }
            st.thin_runway_ns = st
                .thin_runway_ns
                .saturating_add(t_runway.elapsed().as_nanos() as u64);

            let t_head = Instant::now();
            need_head.sort_unstable_by_key(|txid| self.store.txs.head_primary_slot(txid));
            st.head_lookups = st.head_lookups.saturating_add(need_head.len() as u32);
            let head_hits: Vec<([u8; 32], Option<Fk>)> = if need_head.is_empty() {
                Vec::new()
            } else {
                if self.confirm_cancelled() {
                    crate::parent_prewarm_stats::note(&st, t0.elapsed().as_nanos() as u64);
                    return Err(StoreError::Cancelled("confirm cancelled"));
                }
                self.store
                    .get_fk_by_txid_batch(&need_head)
                    .unwrap_or_else(|_| {
                        need_head
                            .iter()
                            .map(|txid| (*txid, self.store.get_fk_by_txid(txid).ok().flatten()))
                            .collect()
                    })
            };
            let mut head_regs: Vec<(Fk, [u8; 32], u32)> = Vec::with_capacity(head_hits.len());
            for (txid, fk_opt) in head_hits {
                match fk_opt {
                    Some(fk) => {
                        st.head_hits = st.head_hits.saturating_add(1);
                        local_fk.insert(txid, Some(fk));
                        head_sourced.insert(txid);
                        let h = external_max_h.get(&txid).copied().unwrap_or(0);
                        head_regs.push((fk, txid, h));
                    }
                    None => {
                        local_fk.insert(txid, None);
                    }
                }
            }
            if !head_regs.is_empty() {
                self.confirm_parents
                    .register_mlocked_creates_batch(&head_regs);
            }
            st.thin_head_ns = st
                .thin_head_ns
                .saturating_add(t_head.elapsed().as_nanos() as u64);
        }

        // ── thin: edge walk ────────────────────────────────────────────────
        let t_edge = Instant::now();
        for (height, tx_fks) in &height_tx_fks {
            let pin_this_height = *height <= pin_hi;
            for fk in tx_fks {
                let Some(id) = fk.get() else {
                    continue;
                };
                let Some((_txid, prevouts)) = body_prevouts.get(&id) else {
                    continue;
                };
                let mut edges: Vec<StashedThinInput> = Vec::with_capacity(prevouts.len());
                for &(create_fk_opt, prev_txid, prev_index) in prevouts {
                    if create_fk_opt.is_none() && prev_index == u32::MAX {
                        edges.push(StashedThinInput {
                            create_fk: None,
                            prev_index,
                        });
                        st.edge_coinbase = st.edge_coinbase.saturating_add(1);
                        continue;
                    }
                    // v10 hot path: stamped create_fk.
                    if let Some(pid) = create_fk_opt {
                        edges.push(StashedThinInput {
                            create_fk: Some(pid),
                            prev_index,
                        });
                        st.utxo_parents = st.utxo_parents.saturating_add(1);
                        // Same-batch create (even cross-height) still needs a
                        // spent-filtered by_fk pin: package_ready / cache-only
                        // wave skip bare by_body. Pin from runway body (no
                        // store re-decode) in the pin phase below.
                        if batch_create_ids.contains(&pid) {
                            st.parent_cache_hits = st.parent_cache_hits.saturating_add(1);
                            st.edge_same_batch = st.edge_same_batch.saturating_add(1);
                            if pin_this_height {
                                parent_need.entry(pid).or_default().push(*height);
                                parent_vouts.entry(pid).or_default().push(prev_index);
                            }
                        } else if pin_this_height {
                            parent_need.entry(pid).or_default().push(*height);
                            parent_vouts.entry(pid).or_default().push(prev_index);
                            st.parent_cache_hits = st.parent_cache_hits.saturating_add(1);
                            st.edge_runway = st.edge_runway.saturating_add(1);
                        } else {
                            // Far runway: edge identity only; wave loads parent if needed.
                            st.edge_runway = st.edge_runway.saturating_add(1);
                        }
                        continue;
                    }
                    if thin_fk_only {
                        // Unstamped non-coinbase: miss (no soft/head on default path).
                        edges.push(StashedThinInput {
                            create_fk: None,
                            prev_index,
                        });
                        st.missing_parents = st.missing_parents.saturating_add(1);
                        continue;
                    }
                    // Legacy soft prev_txid resolve.
                    if let Some(&cfk) = batch_creates.get(&prev_txid) {
                        edges.push(StashedThinInput {
                            create_fk: cfk.get(),
                            prev_index,
                        });
                        st.utxo_parents = st.utxo_parents.saturating_add(1);
                        st.parent_cache_hits = st.parent_cache_hits.saturating_add(1);
                        st.edge_same_batch = st.edge_same_batch.saturating_add(1);
                        continue;
                    }
                    match local_fk.get(&prev_txid).copied() {
                        Some(Some(cfk)) => {
                            edges.push(StashedThinInput {
                                create_fk: cfk.get(),
                                prev_index,
                            });
                            st.utxo_parents = st.utxo_parents.saturating_add(1);
                            if head_sourced.contains(&prev_txid) {
                                st.edge_head = st.edge_head.saturating_add(1);
                            } else if sticky_sourced.contains(&prev_txid) {
                                st.parent_cache_hits = st.parent_cache_hits.saturating_add(1);
                                st.edge_sticky = st.edge_sticky.saturating_add(1);
                            } else {
                                st.parent_cache_hits = st.parent_cache_hits.saturating_add(1);
                                st.edge_runway = st.edge_runway.saturating_add(1);
                            }
                            if let Some(pid) = cfk.get() {
                                if pin_this_height {
                                    parent_need.entry(pid).or_default().push(*height);
                                    parent_vouts.entry(pid).or_default().push(prev_index);
                                }
                            }
                        }
                        Some(None) | None => {
                            edges.push(StashedThinInput {
                                create_fk: None,
                                prev_index,
                            });
                            st.missing_parents = st.missing_parents.saturating_add(1);
                        }
                    }
                }
                thin_by_spend.insert(id, edges);
            }
        }
        st.thin_edge_ns = st
            .thin_edge_ns
            .saturating_add(t_edge.elapsed().as_nanos() as u64);
        st.thin_ns = st.thin_ns.saturating_add(t_thin.elapsed().as_nanos() as u64);

        // ── Pin external parents (tip-near; optional mlock + sparse outs) ───
        let do_mlock = prewarm_mlock_from_env();
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
        let covered = self.confirm_parents.parent_pins_covered(&cover_keys);
        let mut touch_batch: Vec<(u32, u64, Vec<u32>)> = Vec::new();
        let mut parent_ranges: Vec<(Fk, u64, u64)> = Vec::new();
        // (max_height, fk, tx, live outs, checked vouts, coinbase_height)
        let mut sparse_parents: Vec<(
            u32,
            Fk,
            rbitcoin_store::TxRecord,
            Vec<(u32, rbitcoin_store::OutputRecord)>,
            Vec<u32>,
            Option<Option<u32>>,
        )> = Vec::with_capacity(pin_jobs.len());
        for (i, (pid, max_h, need_hs, need_vouts)) in pin_jobs.into_iter().enumerate() {
            if self.confirm_cancelled() {
                crate::parent_prewarm_stats::note(&st, t0.elapsed().as_nanos() as u64);
                return Err(StoreError::Cancelled("confirm cancelled"));
            }
            let fk = Fk(pid);
            if covered.get(i).copied().unwrap_or(false) {
                st.pin_already_cached = st.pin_already_cached.saturating_add(1);
                // Keep-alive for GC on every needing height (not only max_h).
                for &h in &need_hs {
                    touch_batch.push((h, pid, need_vouts.clone()));
                }
                st.utxo_parents = st.utxo_parents.saturating_add(1);
                continue;
            }
            st.pin_new = st.pin_new.saturating_add(1);

            // Prefer runway full body (same-bite / prior-bite creates) — no Class A
            // re-decode. package_ready still needs spent-filtered by_fk.
            if !need_vouts.is_empty() {
                if let Some((create_h, tx, outs, inputs)) =
                    self.confirm_parents.get_body_for_pin(fk)
                {
                    let range = self.confirm_parents.get_body_range(fk);
                    if let Some((off, len)) = range {
                        parent_ranges.push((fk, off, len));
                        if do_mlock {
                            let (_, sys, sk) =
                                self.mlock_body_spans_for_heights(&need_hs, &[(off, len)]);
                            st.mlock_syscalls = st.mlock_syscalls.saturating_add(sys);
                            st.mlock_skipped = st.mlock_skipped.saturating_add(sk);
                            let cc = self.store.mlock_class_c_tx(fk);
                            for h in &need_hs {
                                self.mlock_note_skip_pinned(*h, &cc);
                            }
                        }
                    }
                    let unspent = self
                        .store
                        .unspent_create_vouts(fk, &need_vouts, range)
                        .unwrap_or_else(|_| need_vouts.clone());
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
                    let cb_stash = if tx.input_count == 1
                        && inputs
                            .first()
                            .is_some_and(|i| i.is_coinbase() || i.prev_index == u32::MAX)
                    {
                        Some(Some(create_h))
                    } else if tx.input_count == 1 {
                        // Ambiguous 1-in: fall back to store height if available.
                        match self.resolve_parent_coinbase_height(fk, tx.input_count, range) {
                            Ok(v) => Some(v),
                            Err(_) => Some(None),
                        }
                    } else {
                        Some(None)
                    };
                    sparse_parents.push((max_h, fk, tx, live, need_vouts.clone(), cb_stash));
                    for &h in &need_hs {
                        touch_batch.push((h, pid, need_vouts.clone()));
                    }
                    st.utxo_parents = st.utxo_parents.saturating_add(1);
                    continue;
                }
            }

            // Prefer cached body range over idx read.
            let range = self
                .confirm_parents
                .get_body_range(fk)
                .or_else(|| self.store.tx_body_range(fk).ok());
            if let Some((off, len)) = range {
                parent_ranges.push((fk, off, len));
                if do_mlock {
                    let (_, sys, sk) =
                        self.mlock_body_spans_for_heights(&need_hs, &[(off, len)]);
                    st.mlock_syscalls = st.mlock_syscalls.saturating_add(sys);
                    st.mlock_skipped = st.mlock_skipped.saturating_add(sk);
                    let cc = self.store.mlock_class_c_tx(fk);
                    for h in &need_hs {
                        self.mlock_note_skip_pinned(*h, &cc);
                    }
                }
                // Sparse outs + spent filter + coinbase height for wave.
                if !need_vouts.is_empty() {
                    match self.store.get_tx_meta_and_outputs_at(off, len) {
                        Ok((tx, outs)) => {
                            let unspent = self
                                .store
                                .unspent_create_vouts(fk, &need_vouts, Some((off, len)))
                                .unwrap_or_default();
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
                            // Ok(v) → Some(v) stashed; Err → None (wave recomputes).
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
            } else {
                if do_mlock {
                    let body_ml = self.store.mlock_tx_body_only(fk);
                    st.mlock_syscalls = st.mlock_syscalls.saturating_add(1);
                    for h in &need_hs {
                        self.mlock_note_skip_pinned(*h, &body_ml);
                    }
                    let cc = self.store.mlock_class_c_tx(fk);
                    for h in &need_hs {
                        self.mlock_note_skip_pinned(*h, &cc);
                    }
                }
                // No range: try idx-based outs for sparse stash (rare).
                if !need_vouts.is_empty() {
                    if let Ok((tx, outs)) = self.store.get_tx_meta_and_outputs(fk) {
                        let unspent = self
                            .store
                            .unspent_create_vouts(fk, &need_vouts, None)
                            .unwrap_or_default();
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
                        ));
                        for &h in &need_hs {
                            touch_batch.push((h, pid, need_vouts.clone()));
                        }
                        st.full_tx_reads = st.full_tx_reads.saturating_add(1);
                    }
                }
            }
            st.utxo_parents = st.utxo_parents.saturating_add(1);
        }
        if !touch_batch.is_empty() {
            self.confirm_parents
                .touch_parent_needs_batch(&touch_batch);
        }
        if !parent_ranges.is_empty() {
            self.confirm_parents.put_body_ranges_batch(&parent_ranges);
        }
        if !sparse_parents.is_empty() {
            self.confirm_parents
                .put_parent_outs_resolved_batch(&sparse_parents);
        }
        st.parent_pin_ns = st
            .parent_pin_ns
            .saturating_add(t_par.elapsed().as_nanos() as u64);

        let thin_items: Vec<(Fk, Vec<StashedThinInput>)> = thin_by_spend
            .into_iter()
            .map(|(id, edges)| (Fk(id), edges))
            .collect();
        self.confirm_parents.put_thin_inputs_batch(thin_items);

        // Mark only heights with a complete package (content-based package_ready).
        // Hollow early-continues never get bodies → never ready → never block tip+1.
        let mut ok: Vec<u32> = Vec::with_capacity(work.len());
        for &(h, _) in &work {
            if self.confirm_parents.package_ready(h) {
                ok.push(h);
            }
        }
        if !ok.is_empty() {
            self.confirm_parents.mark_scanned_many(&ok);
        } else if !work.is_empty() {
            // Bite produced zero complete packages — still recompute watermark so
            // a mid-bite invalidation cannot leave ready_through optimistically high.
            self.confirm_parents.recompute_ready_watermark();
        }

        crate::parent_prewarm_stats::note(&st, t0.elapsed().as_nanos() as u64);
        Ok(st)
    }

    pub fn prewarm_parents_for_block_hashes(
        &self,
        hashes: &[[u8; 32]],
    ) -> Result<PrewarmStats, QueryError> {
        let tip = self.tip_height().map(|h| h.0).unwrap_or(0);
        let mut items = Vec::with_capacity(hashes.len());
        for (i, hash) in hashes.iter().enumerate() {
            let h = tip.saturating_add(1).saturating_add(i as u32);
            items.push((h, *hash));
        }
        self.prewarm_parents_for_heights(&items)
    }

    /// Retire spent parent outs from the runway cache by **create_fk** (schema v10).
    ///
    /// Prefer this over txid lookup: spends are already stamped with create_fk at
    /// connect time, so we avoid a large `by_txid` walk on every confirm batch.
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
