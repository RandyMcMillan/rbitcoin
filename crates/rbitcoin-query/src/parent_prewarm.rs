//! Confirm-runway prewarm: **RAM-cache small lookups; mlock large/write pages**.
//!
//! For each height in the batch (ascending):
//! 1. **Cache** (no mlock): header head/body result, `header_txs` list, `tx.head`
//!    → create fk, `tx.idx` body ranges.
//! 2. **`mlock`**: `tx.body` for runway txs + external parents; Class C
//!    `strong_tx` / `tx_height` for those fks; `confirmed[h]`.
//! 3. Prevout-scan bodies (meta only); stash thin edges; mark scanned.
//!
//! Never mlock `spenders.body` (no multi-spend writes during IBD).
//! Mlock ranges are refcounted and released on tip advance.

use super::*;
use crate::confirm_parent_cache::{prewarm_headroom_from_env, StashedThinInput};
use std::collections::HashMap;

#[derive(Debug, Default, Clone, Copy)]
pub struct PrewarmStats {
    pub blocks: u32,
    pub utxo_parents: u32,
    pub reserved: u32,
    pub creates_registered: u32,
    pub already_ready: u32,
    /// Unique parent create fks pinned this call (after dedup).
    pub parent_unique: u32,
    /// Parent outs served from runway txid map / same-batch.
    pub parent_cache_hits: u32,
    /// Body txs mlocked + prevout-scanned (phase 1).
    pub body_tx_reads: u32,
    /// Parent body mlocks (phase 2).
    pub full_tx_reads: u32,
    /// External parents that could not be resolved.
    pub missing_parents: u32,
}

impl Query {
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

    pub fn wait_prewarm_ready(
        &self,
        heights: &[u32],
        timeout: std::time::Duration,
    ) -> Result<(), QueryError> {
        if heights.is_empty() {
            return Ok(());
        }
        let start = std::time::Instant::now();
        loop {
            if self.confirm_cancelled() {
                return Err(StoreError::Corrupt("confirm cancelled"));
            }
            if self.confirm_parents.all_ready(heights) {
                return Ok(());
            }
            if start.elapsed() >= timeout {
                return Err(StoreError::Corrupt(
                    "confirm parent prewarm not ready (timeout)",
                ));
            }
            std::thread::sleep(std::time::Duration::from_millis(2));
        }
    }

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
            return Err(StoreError::Corrupt("confirm cancelled"));
        }
        let hr = headroom.unwrap_or_else(prewarm_headroom_from_env);
        if hr == 0 || self.confirm_parents.headroom_ready(batch_end, hr) {
            return Ok(());
        }
        let soft = std::time::Duration::from_millis(50);
        let start = std::time::Instant::now();
        while start.elapsed() < soft {
            if self.confirm_cancelled() {
                return Err(StoreError::Corrupt("confirm cancelled"));
            }
            if self.confirm_parents.headroom_ready(batch_end, hr) {
                return Ok(());
            }
            std::thread::sleep(std::time::Duration::from_millis(2));
        }
        Ok(())
    }

    /// Prewarm: cache small maps; mlock body + Class C write pages only.
    pub fn prewarm_parents_for_heights(
        &self,
        items: &[(u32, [u8; 32])],
    ) -> Result<PrewarmStats, QueryError> {
        let t0 = std::time::Instant::now();
        let mut st = PrewarmStats::default();
        if items.is_empty() {
            return Ok(st);
        }
        let tip = self.tip_height().map(|h| h.0).unwrap_or(0);

        let mut work: Vec<(u32, [u8; 32])> = Vec::new();
        for &(height, hash) in items {
            if self.confirm_cancelled() {
                crate::parent_prewarm_stats::note(&st, t0.elapsed().as_nanos() as u64);
                return Err(StoreError::Corrupt("confirm cancelled"));
            }
            if height <= tip {
                continue;
            }
            if self.confirm_parents.is_ready(height) {
                st.already_ready = st.already_ready.saturating_add(1);
                continue;
            }
            work.push((height, hash));
        }
        if work.is_empty() {
            crate::parent_prewarm_stats::note(&st, t0.elapsed().as_nanos() as u64);
            return Ok(st);
        }
        self.confirm_parents.ensure_plans(&work);

        let mut height_tx_fks: Vec<(u32, Vec<Fk>)> = Vec::with_capacity(work.len());
        let mut body_prevouts: HashMap<u64, ([u8; 32], Vec<([u8; 32], u32)>)> = HashMap::new();
        let mut create_regs: Vec<(Fk, [u8; 32], u32)> = Vec::new();
        let mut parent_need: HashMap<u64, Vec<u32>> = HashMap::new(); // parent_fk → need heights
        let mut thin_by_spend: HashMap<u64, Vec<StashedThinInput>> = HashMap::new();
        let mut batch_creates: HashMap<[u8; 32], Fk> = HashMap::new();
        let mut body_ranges: Vec<(Fk, u64, u64)> = Vec::new();

        for &(height, hash) in &work {
            if self.confirm_cancelled() {
                crate::parent_prewarm_stats::note(&st, t0.elapsed().as_nanos() as u64);
                return Err(StoreError::Corrupt("confirm cancelled"));
            }

            // ── Small: load + RAM-cache header / header_txs (no mlock) ─────
            let Some((header_fk, header_rec)) = self.store.get_header_by_hash(&hash)? else {
                continue;
            };
            if !self.store.header_txs.has_body(header_fk)? {
                continue;
            }
            let Some(tx_fks) = self.store.header_txs.get_list(header_fk)? else {
                continue;
            };
            if tx_fks.is_empty() {
                continue;
            }
            // Body readiness: first tx must have an idx range (no tx.head probe).
            // Direct archive indexes head with bodies; head lag must not skip the height.
            if let Some(&first) = tx_fks.first() {
                if self.store.tx_body_range(first).is_err() {
                    continue;
                }
            }

            self.confirm_parents.put_header_plan(
                height,
                header_fk,
                header_rec,
                tx_fks.clone(),
            );

            // ── Large/write: mlock confirmed[h] ─────────────────────────────
            let conf_r = self.store.mlock_confirmed_height(height);
            self.confirm_parents.note_mlock_ranges(height, &conf_r);

            st.blocks = st.blocks.saturating_add(1);

            // header_txs is almost always ascending fk; avoid clone+sort when so.
            let fks_work: std::borrow::Cow<'_, [Fk]> = if tx_fks_is_sorted_ascending(&tx_fks) {
                std::borrow::Cow::Borrowed(tx_fks.as_slice())
            } else {
                let mut v = tx_fks.clone();
                v.sort_unstable_by_key(|f| f.0);
                std::borrow::Cow::Owned(v)
            };

            for fk in fks_work.iter() {
                if self.confirm_cancelled() {
                    crate::parent_prewarm_stats::note(&st, t0.elapsed().as_nanos() as u64);
                    return Err(StoreError::Corrupt("confirm cancelled"));
                }
                let Some(id) = fk.get() else {
                    continue;
                };

                // Cache idx range (small); mlock body only.
                if let Ok((off, len)) = self.store.tx_body_range(*fk) {
                    body_ranges.push((*fk, off, len));
                    let body_ml = self.store.mlock_tx_body_only(*fk);
                    self.confirm_parents.note_mlock_ranges(height, &body_ml);

                    // Class C write slots for runway creates.
                    let cc = self.store.mlock_class_c_tx(*fk);
                    self.confirm_parents.note_mlock_ranges(height, &cc);

                    let (meta, prevouts) =
                        self.store.get_tx_meta_and_prevouts_at(off, len)?;
                    st.body_tx_reads = st.body_tx_reads.saturating_add(1);
                    // tx.head result → by_txid cache (no head mlock).
                    create_regs.push((*fk, meta.txid, height));
                    batch_creates.insert(meta.txid, *fk);
                    body_prevouts.insert(id, (meta.txid, prevouts));
                    st.creates_registered = st.creates_registered.saturating_add(1);
                } else {
                    // Fallback: mlock via fk (will touch idx once).
                    let body_ml = self.store.mlock_tx_body_only(*fk);
                    self.confirm_parents.note_mlock_ranges(height, &body_ml);
                    let cc = self.store.mlock_class_c_tx(*fk);
                    self.confirm_parents.note_mlock_ranges(height, &cc);
                    let (meta, prevouts) = self.store.get_tx_meta_and_prevouts(*fk)?;
                    st.body_tx_reads = st.body_tx_reads.saturating_add(1);
                    create_regs.push((*fk, meta.txid, height));
                    batch_creates.insert(meta.txid, *fk);
                    body_prevouts.insert(id, (meta.txid, prevouts));
                    st.creates_registered = st.creates_registered.saturating_add(1);
                }
            }
            height_tx_fks.push((height, tx_fks));
        }

        self.confirm_parents.put_body_ranges_batch(&body_ranges);
        self.confirm_parents
            .register_mlocked_creates_batch(&create_regs);

        // ── Thin edges + external parents ───────────────────────────────────
        for (height, tx_fks) in &height_tx_fks {
            for fk in tx_fks {
                let Some(id) = fk.get() else {
                    continue;
                };
                let Some((_txid, prevouts)) = body_prevouts.get(&id) else {
                    continue;
                };
                let mut edges: Vec<StashedThinInput> = Vec::with_capacity(prevouts.len());
                for &(prev_txid, prev_index) in prevouts {
                    if prev_txid == [0u8; 32] && prev_index == u32::MAX {
                        edges.push(StashedThinInput {
                            create_fk: None,
                            prev_index,
                        });
                        continue;
                    }
                    if let Some(&cfk) = batch_creates.get(&prev_txid) {
                        edges.push(StashedThinInput {
                            create_fk: cfk.get(),
                            prev_index,
                        });
                        st.utxo_parents = st.utxo_parents.saturating_add(1);
                        st.parent_cache_hits = st.parent_cache_hits.saturating_add(1);
                        continue;
                    }
                    if let Some(cfk) = self
                        .confirm_parents
                        .get_by_txid_if_out(&prev_txid, prev_index)
                    {
                        edges.push(StashedThinInput {
                            create_fk: cfk.get(),
                            prev_index,
                        });
                        st.utxo_parents = st.utxo_parents.saturating_add(1);
                        st.parent_cache_hits = st.parent_cache_hits.saturating_add(1);
                        if let Some(pid) = cfk.get() {
                            parent_need.entry(pid).or_default().push(*height);
                        }
                        continue;
                    }
                    // Durable head lookup once; cache fk in by_txid (no head mlock).
                    if let Some(create_fk) = self.tx_fk_by_txid(&prev_txid).ok().flatten() {
                        edges.push(StashedThinInput {
                            create_fk: create_fk.get(),
                            prev_index,
                        });
                        if let Some(pid) = create_fk.get() {
                            self.confirm_parents.register_mlocked_create(
                                create_fk,
                                prev_txid,
                                *height,
                            );
                            parent_need.entry(pid).or_default().push(*height);
                        }
                        continue;
                    }
                    edges.push(StashedThinInput {
                        create_fk: None,
                        prev_index,
                    });
                    st.missing_parents = st.missing_parents.saturating_add(1);
                }
                thin_by_spend.insert(id, edges);
            }
        }

        let mut uniq_parents: Vec<u64> = parent_need.keys().copied().collect();
        uniq_parents.sort_unstable();
        st.parent_unique = st.parent_unique.saturating_add(uniq_parents.len() as u32);

        for pid in uniq_parents {
            if self.confirm_cancelled() {
                crate::parent_prewarm_stats::note(&st, t0.elapsed().as_nanos() as u64);
                return Err(StoreError::Corrupt("confirm cancelled"));
            }
            let fk = Fk(pid);
            let mut need_hs = parent_need.remove(&pid).unwrap_or_default();
            need_hs.sort_unstable();
            need_hs.dedup();
            let need_h = need_hs.first().copied().unwrap_or(0);

            // Cache idx range; mlock body only (no spenders).
            if let Ok((off, len)) = self.store.tx_body_range(fk) {
                body_ranges.push((fk, off, len));
                let body_ml = self.store.mlock_tx_body_only(fk);
                for h in &need_hs {
                    self.confirm_parents.note_mlock_ranges(*h, &body_ml);
                }
                // Class C for spentness checks on this parent create.
                let cc = self.store.mlock_class_c_tx(fk);
                for h in &need_hs {
                    self.confirm_parents.note_mlock_ranges(*h, &cc);
                }
                if let Ok((meta, _)) = self.store.get_tx_meta_and_prevouts_at(off, len) {
                    self.confirm_parents
                        .register_mlocked_create(fk, meta.txid, need_h);
                }
            } else {
                let body_ml = self.store.mlock_tx_body_only(fk);
                for h in &need_hs {
                    self.confirm_parents.note_mlock_ranges(*h, &body_ml);
                }
                let cc = self.store.mlock_class_c_tx(fk);
                for h in &need_hs {
                    self.confirm_parents.note_mlock_ranges(*h, &cc);
                }
            }
            st.full_tx_reads = st.full_tx_reads.saturating_add(1);
            st.utxo_parents = st.utxo_parents.saturating_add(1);
        }
        // Parent body ranges may have been added after first batch put.
        self.confirm_parents.put_body_ranges_batch(&body_ranges);

        let thin_items: Vec<(Fk, Vec<StashedThinInput>)> = thin_by_spend
            .into_iter()
            .map(|(id, edges)| (Fk(id), edges))
            .collect();
        self.confirm_parents.put_thin_inputs_batch(thin_items);

        let scanned: Vec<u32> = work.iter().map(|(h, _)| *h).collect();
        self.confirm_parents.mark_scanned_many(&scanned);

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

    pub fn unpin_spent_parent_outs(
        &self,
        spends: &[([u8; 32], u32)],
    ) -> Result<(), QueryError> {
        if spends.is_empty() {
            return Ok(());
        }
        let mut resolved: Vec<(Fk, u32)> = Vec::with_capacity(spends.len());
        for &(txid, vout) in spends {
            if let Some(fk) = self.confirm_parents.get_by_txid(&txid) {
                resolved.push((fk, vout));
            }
        }
        self.confirm_parents.retire_spends(&resolved);
        Ok(())
    }
}

/// True when `fks` is empty or non-decreasing by Class A id (archive order).
#[inline]
fn tx_fks_is_sorted_ascending(fks: &[Fk]) -> bool {
    fks.windows(2).all(|w| w[0].0 <= w[1].0)
}
