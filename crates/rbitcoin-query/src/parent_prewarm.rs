//! Confirm-runway prewarm: **mlock body pages**, minimal input walk for parents.
//!
//! For a batch of heights (ascending):
//! 1. Resolve header → tx fks; `mlock` each Class A body; scan **prevouts only**
//!    (no script/witness/output allocation) to discover parent creates.
//! 2. Resolve external parent fks; `mlock` those parent bodies (no full parse).
//! 3. Stash thin create-fk edges + `txid → fk` for runway creates; mark scanned.
//!
//! Confirm wave/connect **full-parses** from the (now resident) store. This path
//! exists so confirm wall time is not dominated by page faults on Class A.

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
        for (page_start, page_len) in unlocks {
            self.store.munlock_tx_body_pages(page_start, page_len);
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

        // ── Phase 1: resolve header → tx fks, mlock + prevout-scan bodies ────
        let mut height_tx_fks: Vec<(u32, Vec<Fk>)> = Vec::with_capacity(work.len());
        let mut all_body_fks: Vec<(u32, u64)> = Vec::new(); // (height, fk)
        for &(height, hash) in &work {
            if self.confirm_cancelled() {
                crate::parent_prewarm_stats::note(&st, t0.elapsed().as_nanos() as u64);
                return Err(StoreError::Corrupt("confirm cancelled"));
            }
            let Some((header_fk, _)) = self.get_header_by_hash(&hash)? else {
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
            if self.tx_index_enabled() {
                if let Some(&first) = tx_fks.first() {
                    let meta = self.store.get_tx(first)?;
                    if self.store.txs.get_by_txid(&meta.txid)?.is_none() {
                        continue;
                    }
                }
            }
            st.blocks = st.blocks.saturating_add(1);
            for fk in &tx_fks {
                if let Some(id) = fk.get() {
                    all_body_fks.push((height, id));
                }
            }
            height_tx_fks.push((height, tx_fks));
        }

        // Sort by fk for sequential body locality under archive load.
        all_body_fks.sort_unstable_by_key(|(_, id)| *id);
        all_body_fks.dedup_by_key(|(_, id)| *id);

        // fk → (txid, prevouts)
        let mut body_prevouts: HashMap<u64, ([u8; 32], Vec<([u8; 32], u32)>)> =
            HashMap::with_capacity(all_body_fks.len());
        let mut mlock_notes: Vec<(Fk, u64, u64, Option<u32>, u32)> = Vec::new();
        let mut create_regs: Vec<(Fk, [u8; 32], u32)> = Vec::new();

        // height for each body fk (for mlock note)
        let mut body_height: HashMap<u64, u32> = HashMap::with_capacity(all_body_fks.len());
        for (height, tx_fks) in &height_tx_fks {
            for fk in tx_fks {
                if let Some(id) = fk.get() {
                    body_height.insert(id, *height);
                }
            }
        }

        for &(_h, id) in &all_body_fks {
            if self.confirm_cancelled() {
                crate::parent_prewarm_stats::note(&st, t0.elapsed().as_nanos() as u64);
                return Err(StoreError::Corrupt("confirm cancelled"));
            }
            let fk = Fk(id);
            let height = body_height.get(&id).copied().unwrap_or(0);
            // mlock pages first so the subsequent mmap walk does not soft-fault.
            match self.store.mlock_tx_body(fk) {
                Ok((page_start, page_len)) => {
                    mlock_notes.push((fk, page_start, page_len, Some(height), height));
                }
                Err(e) => {
                    // Soft-fail: still scan (may page-fault); log once per batch via stats.
                    rbitcoin_log::trace!("prewarm: mlock body fk={id} failed: {e}");
                }
            }
            let (meta, prevouts) = self.store.get_tx_meta_and_prevouts(fk)?;
            st.body_tx_reads = st.body_tx_reads.saturating_add(1);
            create_regs.push((fk, meta.txid, height));
            body_prevouts.insert(id, (meta.txid, prevouts));
            st.creates_registered = st.creates_registered.saturating_add(1);
        }

        // Publish create map early so same-batch parents resolve while we walk.
        self.confirm_parents
            .register_mlocked_creates_batch(&create_regs);
        self.confirm_parents.note_mlocks_batch(&mlock_notes);

        // ── Phase 2: thin edges + mlock external parents ─────────────────────
        let mut need_parent: Vec<(u32, u64)> = Vec::new(); // (spend_height, parent_fk)
        let mut thin_by_spend: HashMap<u64, Vec<StashedThinInput>> = HashMap::new();
        let catchup = self.ibd_utxo_enabled();

        // Same-batch creates accumulate as we scan heights ascending.
        let mut batch_creates: HashMap<[u8; 32], Fk> = HashMap::new();

        for (height, tx_fks) in &height_tx_fks {
            for fk in tx_fks {
                let Some(id) = fk.get() else {
                    continue;
                };
                if let Some((txid, _)) = body_prevouts.get(&id) {
                    batch_creates.insert(*txid, *fk);
                }
            }
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
                    // Same-bite create (this or earlier height in batch).
                    if let Some(&cfk) = batch_creates.get(&prev_txid) {
                        edges.push(StashedThinInput {
                            create_fk: cfk.get(),
                            prev_index,
                        });
                        st.utxo_parents = st.utxo_parents.saturating_add(1);
                        st.parent_cache_hits = st.parent_cache_hits.saturating_add(1);
                        continue;
                    }
                    // Earlier runway / mlocked create.
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
                        continue;
                    }
                    // Live UTXO parent (catch-up mode).
                    if let Some(create_fk) = self.ibd_utxo_create_fk(&prev_txid, prev_index)? {
                        edges.push(StashedThinInput {
                            create_fk: create_fk.get(),
                            prev_index,
                        });
                        if let Some(pid) = create_fk.get() {
                            need_parent.push((*height, pid));
                        }
                        continue;
                    }
                    // Durable head.
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
                            need_parent.push((*height, pid));
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

        need_parent.sort_unstable();
        need_parent.dedup();
        let mut uniq_parents: Vec<u64> = need_parent.iter().map(|(_, p)| *p).collect();
        uniq_parents.sort_unstable();
        uniq_parents.dedup();
        st.parent_unique = st.parent_unique.saturating_add(uniq_parents.len() as u32);

        // Map parent fk → one need height (for mlock tracking GC).
        let mut parent_need_h: HashMap<u64, u32> = HashMap::with_capacity(uniq_parents.len());
        for &(h, pid) in &need_parent {
            parent_need_h.entry(pid).or_insert(h);
        }

        let mut parent_mlock_notes: Vec<(Fk, u64, u64, Option<u32>, u32)> = Vec::new();
        for pid in uniq_parents {
            if self.confirm_cancelled() {
                crate::parent_prewarm_stats::note(&st, t0.elapsed().as_nanos() as u64);
                return Err(StoreError::Corrupt("confirm cancelled"));
            }
            let fk = Fk(pid);
            let need_h = parent_need_h.get(&pid).copied().unwrap_or(0);
            match self.store.mlock_tx_body(fk) {
                Ok((page_start, page_len)) => {
                    parent_mlock_notes.push((fk, page_start, page_len, None, need_h));
                    st.full_tx_reads = st.full_tx_reads.saturating_add(1);
                    st.utxo_parents = st.utxo_parents.saturating_add(1);
                }
                Err(e) => {
                    rbitcoin_log::trace!("prewarm: mlock parent fk={pid} failed: {e}");
                    // Still count as attempted parent pin; confirm may fault.
                    st.full_tx_reads = st.full_tx_reads.saturating_add(1);
                }
            }
        }
        self.confirm_parents.note_mlocks_batch(&parent_mlock_notes);

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
