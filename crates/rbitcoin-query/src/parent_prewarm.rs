//! Confirm-runway prewarm: **mlock every store page confirm will touch**.
//!
//! For each height in the batch (ascending):
//! 1. `mlock` header head+body, `header_txs` slots, `confirmed[h]`.
//! 2. For each body tx: `mlock` `tx.idx`+`tx.body`, `tx.head` probe, Class C
//!    strong/height slots; scan **prevouts only** (no full parse into RAM).
//! 3. External parents: `mlock` head probe, Class A idx+body, spend-oracle
//!    (spender list + Class C for annotated spenders).
//! 4. Stash thin create-fk edges + `txid → fk`; mark scanned.
//!
//! Ranges are refcounted and released on tip advance when no runway height still
//! needs them. Confirm wave/connect full-parses from the resident store.

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
    /// Unique parent create fks mlocked this call (after dedup).
    pub parent_unique: u32,
    /// Parent outs served from runway txid map / same-batch (no head probe).
    pub parent_cache_hits: u32,
    /// Body txs mlocked + prevout-scanned (phase 1).
    pub body_tx_reads: u32,
    /// Parent body mlocks (phase 2).
    pub full_tx_reads: u32,
    /// External parents that could not be resolved (should be 0 after phase 1).
    pub missing_parents: u32,
}

impl Query {
    pub fn parent_prewarm_depth(&self) -> u32 {
        self.confirm_parents.depth()
    }

    /// Contiguous ready watermark: all heights in `(tip, ready_through]` ready.
    pub fn parent_prewarm_ready_through(&self) -> u32 {
        self.confirm_parents.ready_through()
    }

    /// Snapshot for IBD progress/perf:
    /// `(ready_through, ahead, parents, bodies, plans, depth)`.
    ///
    /// Bodies = mlocked runway block txs (not fully parsed into RAM).
    pub fn parent_prewarm_perf_snapshot(&self) -> (u32, u32, usize, usize, usize, u32) {
        let tip = self.tip_height().map(|h| h.0).unwrap_or(0);
        let through = self.confirm_parents.ready_through();
        let ahead = through.saturating_sub(tip);
        (
            through,
            ahead,
            self.confirm_parents.parent_count(),
            self.confirm_parents.body_count(),
            self.confirm_parents.plan_count(),
            self.confirm_parents.depth(),
        )
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

    /// Prewarm a contiguous runway slice: **mlock bodies**, scan prevouts, mlock parents.
    ///
    /// `items` must be height-ascending. Does **not** fully parse txs into the
    /// confirm parent cache — confirm wave/connect parse from store after mlock.
    ///
    /// Does **not** run tip GC (caller / confirm owns `advance_parent_runway_tip`).
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

        // Work list: heights still needing scan.
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

        // ── Phase 1: resolve header → tx fks; mlock all tables confirm needs ─
        // For each height: header head+body, header_txs, tx idx+body, tx.head,
        // class C slots for runway txs, confirmed[height]. Then prevout-scan.
        let mut height_tx_fks: Vec<(u32, Vec<Fk>)> = Vec::with_capacity(work.len());
        let mut body_prevouts: HashMap<u64, ([u8; 32], Vec<([u8; 32], u32)>)> = HashMap::new();
        let mut create_regs: Vec<(Fk, [u8; 32], u32)> = Vec::new();
        // parent (create_fk, vout) → spend heights that need it
        let mut parent_vouts: HashMap<(u64, u32), Vec<u32>> = HashMap::new();
        let mut thin_by_spend: HashMap<u64, Vec<StashedThinInput>> = HashMap::new();
        let catchup = self.ibd_utxo_enabled();
        let mut batch_creates: HashMap<[u8; 32], Fk> = HashMap::new();

        for &(height, hash) in &work {
            if self.confirm_cancelled() {
                crate::parent_prewarm_stats::note(&st, t0.elapsed().as_nanos() as u64);
                return Err(StoreError::Corrupt("confirm cancelled"));
            }
            // Header head + body pages.
            let (found, hdr_ranges) = self.store.mlock_header_for_hash(&hash)?;
            self.confirm_parents
                .note_mlock_ranges(height, &hdr_ranges);
            let Some((header_fk, _)) = found else {
                continue;
            };
            // header_txs first+count.
            let ht_ranges = self.store.mlock_header_txs_for(header_fk);
            self.confirm_parents.note_mlock_ranges(height, &ht_ranges);

            if !self.store.header_txs.has_body(header_fk)? {
                continue;
            }
            let Some(tx_fks) = self.store.header_txs.get_list(header_fk)? else {
                continue;
            };
            if tx_fks.is_empty() {
                continue;
            }
            if self.tx_index_enabled() {
                if let Some(&first) = tx_fks.first() {
                    let meta = self.store.get_tx(first)?;
                    if self.store.txs.get_by_txid(&meta.txid)?.is_none() {
                        continue;
                    }
                }
            }

            // confirmed[height] for tip write path.
            let conf_r = self.store.mlock_confirmed_height(height);
            self.confirm_parents.note_mlock_ranges(height, &conf_r);

            st.blocks = st.blocks.saturating_add(1);

            // Sort body fks for sequential body locality.
            let mut sorted_fks = tx_fks.clone();
            sorted_fks.sort_unstable_by_key(|f| f.0);

            for fk in &sorted_fks {
                if self.confirm_cancelled() {
                    crate::parent_prewarm_stats::note(&st, t0.elapsed().as_nanos() as u64);
                    return Err(StoreError::Corrupt("confirm cancelled"));
                }
                let Some(id) = fk.get() else {
                    continue;
                };
                // idx + body
                let ca = self.store.mlock_tx_class_a(*fk);
                self.confirm_parents.note_mlock_ranges(height, &ca);
                // Class C write slots for this create.
                let cc = self.store.mlock_class_c_tx(*fk);
                self.confirm_parents.note_mlock_ranges(height, &cc);

                let (meta, prevouts) = self.store.get_tx_meta_and_prevouts(*fk)?;
                st.body_tx_reads = st.body_tx_reads.saturating_add(1);
                // tx.head probe for create (put_spend / get_by_txid).
                let head_r = self.store.mlock_tx_head_for(&meta.txid);
                self.confirm_parents.note_mlock_ranges(height, &head_r);

                create_regs.push((*fk, meta.txid, height));
                batch_creates.insert(meta.txid, *fk);
                body_prevouts.insert(id, (meta.txid, prevouts));
                st.creates_registered = st.creates_registered.saturating_add(1);
            }
            height_tx_fks.push((height, tx_fks));
        }

        self.confirm_parents
            .register_mlocked_creates_batch(&create_regs);

        // ── Phase 2: thin edges + mlock external parents + spend oracle ─────
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
                        // Still pin head + body + oracle for earlier-runway creates.
                        if let Some(pid) = cfk.get() {
                            parent_vouts.entry((pid, prev_index)).or_default().push(*height);
                        }
                        continue;
                    }
                    if let Some(create_fk) = self.ibd_utxo_create_fk(&prev_txid, prev_index)? {
                        edges.push(StashedThinInput {
                            create_fk: create_fk.get(),
                            prev_index,
                        });
                        if let Some(pid) = create_fk.get() {
                            parent_vouts.entry((pid, prev_index)).or_default().push(*height);
                        }
                        continue;
                    }
                    // Durable head: pin head probe first so lookup is warm.
                    let head_r = self.store.mlock_tx_head_for(&prev_txid);
                    self.confirm_parents.note_mlock_ranges(*height, &head_r);
                    if let Some(create_fk) = self.tx_fk_by_txid(&prev_txid).ok().flatten() {
                        edges.push(StashedThinInput {
                            create_fk: create_fk.get(),
                            prev_index,
                        });
                        if catchup && self.catchup_is_spent(&prev_txid, prev_index)? {
                            st.utxo_parents = st.utxo_parents.saturating_add(1);
                            continue;
                        }
                        if let Some(pid) = create_fk.get() {
                            parent_vouts.entry((pid, prev_index)).or_default().push(*height);
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

        // Unique parent create fks → mlock class A + head + spend oracle.
        let mut uniq_parents: Vec<u64> = parent_vouts.keys().map(|(p, _)| *p).collect();
        uniq_parents.sort_unstable();
        uniq_parents.dedup();
        st.parent_unique = st.parent_unique.saturating_add(uniq_parents.len() as u32);

        for pid in uniq_parents {
            if self.confirm_cancelled() {
                crate::parent_prewarm_stats::note(&st, t0.elapsed().as_nanos() as u64);
                return Err(StoreError::Corrupt("confirm cancelled"));
            }
            let fk = Fk(pid);
            // Collect need heights for this parent (any vout).
            let mut need_hs: Vec<u32> = parent_vouts
                .iter()
                .filter(|((p, _), _)| *p == pid)
                .flat_map(|(_, hs)| hs.iter().copied())
                .collect();
            need_hs.sort_unstable();
            need_hs.dedup();
            let need_h = need_hs.first().copied().unwrap_or(0);

            let ca = self.store.mlock_tx_class_a(fk);
            for h in &need_hs {
                self.confirm_parents.note_mlock_ranges(*h, &ca);
            }
            // Head for create txid (from body meta if available).
            if let Ok((meta, _)) = self.store.get_tx_meta_and_prevouts(fk) {
                let head_r = self.store.mlock_tx_head_for(&meta.txid);
                for h in &need_hs {
                    self.confirm_parents.note_mlock_ranges(*h, &head_r);
                }
                self.confirm_parents
                    .register_mlocked_create(fk, meta.txid, need_h);
            }
            // Spend-oracle pages for each needed vout.
            for ((p, vout), hs) in &parent_vouts {
                if *p != pid {
                    continue;
                }
                let oracle = self.store.mlock_spend_oracle(fk, *vout);
                for h in hs {
                    self.confirm_parents.note_mlock_ranges(*h, &oracle);
                }
            }
            st.full_tx_reads = st.full_tx_reads.saturating_add(1);
            st.utxo_parents = st.utxo_parents.saturating_add(1);
        }

        let thin_items: Vec<(Fk, Vec<StashedThinInput>)> = thin_by_spend
            .into_iter()
            .map(|(id, edges)| (Fk(id), edges))
            .collect();
        self.confirm_parents.put_thin_inputs_batch(thin_items);

        // ── Phase 3: mark scanned ────────────────────────────────────────────
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

    /// Drop spent outs from the confirm parent runway after Class C.
    ///
    /// **Catch-up (light UTXO on):** no-op.
    /// **Direct / tip:** retire sparse parsed parent outs if present; mlocked
    /// parents stay until tip GC (`advance_parent_runway_tip`).
    pub fn unpin_spent_parent_outs(
        &self,
        spends: &[([u8; 32], u32)],
    ) -> Result<(), QueryError> {
        if spends.is_empty() {
            return Ok(());
        }
        if self.ibd_utxo_enabled() {
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
