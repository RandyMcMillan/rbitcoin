//! Confirm-runway prewarm: **bodies first, then parents**.
//!
//! For a batch of heights (ascending):
//! 1. Load every full Class A body in the batch; register **all** creates so
//!    same-batch / runway spends need no UTXO and no reservations.
//! 2. Collect external parent needs; sort/dedup by create fk; load once each.
//!
//! After prewarm, confirm should only **write** (Class C, light UTXO, tip).
//! Wave/wire rebuild prefer the body cache over store.

use super::*;
use crate::confirm_parent_cache::prewarm_headroom_from_env;
use std::collections::HashMap;

#[derive(Debug, Default, Clone, Copy)]
pub struct PrewarmStats {
    pub blocks: u32,
    pub utxo_parents: u32,
    pub reserved: u32,
    pub creates_registered: u32,
    pub already_ready: u32,
    /// Unique parent create fks loaded from store this call (after dedup).
    pub parent_unique: u32,
    /// Parent outs served from confirm_parents / same-batch body (no store).
    pub parent_cache_hits: u32,
    /// `get_tx_full` calls for **block bodies** (phase 1).
    pub body_tx_reads: u32,
    /// `get_tx_full` calls for **external parents** (phase 2).
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
    /// Bodies = full Class A blocks cached for the runway (bodies-first prewarm).
    /// Reservations are no longer used on the happy path.
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
        self.confirm_parents.advance_tip(tip);
    }

    pub fn seed_parent_runway(&self, items: &[(u32, [u8; 32])]) {
        for &(height, hash) in items {
            self.confirm_parents.ensure_plan(height, hash);
        }
    }

    pub fn parent_pin_count(&self) -> usize {
        self.confirm_parents.parent_count()
            + self.confirm_parents.reserved_count()
            + self.confirm_parents.body_count()
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

    /// Prewarm a contiguous runway slice: **all bodies first**, then parents.
    ///
    /// `items` must be height-ascending. No reservations: same-batch creates are
    /// registered in phase 1; external parents load in phase 2 from UTXO/store.
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
        self.confirm_parents.advance_tip(tip);

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
            self.confirm_parents.ensure_plan(height, hash);
            work.push((height, hash));
        }
        if work.is_empty() {
            crate::parent_prewarm_stats::note(&st, t0.elapsed().as_nanos() as u64);
            return Ok(st);
        }

        // ── Phase 1: full block bodies (creates available without UTXO) ─────
        // Per-height: list of (fk, tx, outs, inputs) for phase 2 spend scan.
        let mut phase1: Vec<(u32, Vec<(Fk, TxRecord, Vec<OutputRecord>, Vec<InputRecord>)>)> =
            Vec::with_capacity(work.len());

        for &(height, hash) in &work {
            if self.confirm_cancelled() {
                crate::parent_prewarm_stats::note(&st, t0.elapsed().as_nanos() as u64);
                return Err(StoreError::Corrupt("confirm cancelled"));
            }
            let Some((header_fk, _)) = self.get_header_by_hash(&hash)? else {
                continue;
            };
            let Some(tx_fks) = self.store.header_txs.get_list(header_fk)? else {
                continue;
            };
            st.blocks = st.blocks.saturating_add(1);
            let mut bodies = Vec::with_capacity(tx_fks.len());
            for &fk in &tx_fks {
                let (tx, inputs, outs) = self.store.get_tx_full(fk)?;
                st.body_tx_reads = st.body_tx_reads.saturating_add(1);
                st.full_tx_reads = st.full_tx_reads.saturating_add(1);
                // Full body for zero-read confirm wave/wire.
                self.confirm_parents
                    .put_body(fk, height, tx.clone(), outs.clone(), inputs.clone());
                // All outs so later heights / same batch spend without reserve.
                self.confirm_parents
                    .register_runway_creates(fk, &tx, &outs, height);
                st.creates_registered = st.creates_registered.saturating_add(1);
                bodies.push((fk, tx, outs, inputs));
            }
            phase1.push((height, bodies));
        }

        // ── Phase 2: external parents (sort/dedup store loads) ──────────────
        // (create_fk_id, vout) → need put for each (height, fk, vout)
        let mut need_load: Vec<(u64, u32)> = Vec::new();
        let mut need_put: Vec<(u32, Fk, u32)> = Vec::new();

        for (height, bodies) in &phase1 {
            let mut local: HashMap<[u8; 32], Fk> = HashMap::with_capacity(bodies.len());
            for (fk, tx, _, _) in bodies {
                local.insert(tx.txid, *fk);
            }
            for (_fk, _tx, _outs, inputs) in bodies {
                for inp in inputs {
                    if inp.is_coinbase() {
                        continue;
                    }
                    // Same-block create (from phase 1 body).
                    if let Some(&cfk) = local.get(&inp.prev_txid) {
                        if let Some((_, ptx, bouts, _)) =
                            bodies.iter().find(|(f, _, _, _)| *f == cfk)
                        {
                            if let Some(o) = bouts.get(inp.prev_index as usize) {
                                self.confirm_parents.put_utxo_parent(
                                    *height,
                                    cfk,
                                    ptx.clone(),
                                    inp.prev_index,
                                    o.clone(),
                                );
                                st.utxo_parents = st.utxo_parents.saturating_add(1);
                                st.parent_cache_hits = st.parent_cache_hits.saturating_add(1);
                            }
                        }
                        continue;
                    }
                    // Runway create from earlier prewarmed height / phase 1 of this batch.
                    if let Some(cfk) = self.confirm_parents.get_by_txid(&inp.prev_txid) {
                        if let Some((ptx, o)) =
                            self.confirm_parents.get_parent_out(cfk, inp.prev_index)
                        {
                            self.confirm_parents.put_utxo_parent(
                                *height,
                                cfk,
                                ptx,
                                inp.prev_index,
                                o,
                            );
                            st.utxo_parents = st.utxo_parents.saturating_add(1);
                            st.parent_cache_hits = st.parent_cache_hits.saturating_add(1);
                            continue;
                        }
                    }
                    // Confirmed parent: UTXO / tip_prevout / durable head.
                    let create_fk = self
                        .ibd_utxo_create_fk(&inp.prev_txid, inp.prev_index)?
                        .or_else(|| {
                            self.tip_prevout_cache
                                .get_tx_and_output_by_txid(&inp.prev_txid, inp.prev_index)
                                .map(|(fk, _, _)| fk)
                        })
                        .or(self.tx_fk_by_txid(&inp.prev_txid).ok().flatten());

                    match create_fk {
                        Some(create_fk) => {
                            if let Some((ptx, o)) =
                                self.confirm_parents.get_parent_out(create_fk, inp.prev_index)
                            {
                                self.confirm_parents.put_utxo_parent(
                                    *height,
                                    create_fk,
                                    ptx,
                                    inp.prev_index,
                                    o,
                                );
                                st.utxo_parents = st.utxo_parents.saturating_add(1);
                                st.parent_cache_hits = st.parent_cache_hits.saturating_add(1);
                                continue;
                            }
                            if let Some(id) = create_fk.get() {
                                need_load.push((id, inp.prev_index));
                                need_put.push((*height, create_fk, inp.prev_index));
                            }
                        }
                        None => {
                            // Should not happen after phase-1 full-out register for
                            // runway creates; count for diagnostics (no reserve).
                            st.missing_parents = st.missing_parents.saturating_add(1);
                        }
                    }
                }
            }
        }

        need_load.sort_unstable();
        need_load.dedup();
        let mut uniq_fks: Vec<u64> = need_load.iter().map(|(f, _)| *f).collect();
        uniq_fks.sort_unstable();
        uniq_fks.dedup();
        st.parent_unique = st.parent_unique.saturating_add(uniq_fks.len() as u32);

        let mut loaded: HashMap<u64, (TxRecord, Vec<OutputRecord>)> =
            HashMap::with_capacity(uniq_fks.len());
        for fk_id in uniq_fks {
            if self.confirm_cancelled() {
                crate::parent_prewarm_stats::note(&st, t0.elapsed().as_nanos() as u64);
                return Err(StoreError::Corrupt("confirm cancelled"));
            }
            let fk = Fk(fk_id);
            let (ptx, _ins, outs) = self.store.get_tx_full(fk)?;
            st.full_tx_reads = st.full_tx_reads.saturating_add(1);
            loaded.insert(fk_id, (ptx, outs));
        }

        let mut tip_seeded: HashMap<u64, ()> = HashMap::new();
        for &(height, create_fk, vout) in &need_put {
            let Some(id) = create_fk.get() else {
                continue;
            };
            let Some((ptx, outs)) = loaded.get(&id) else {
                continue;
            };
            let Some(o) = outs.get(vout as usize) else {
                return Err(StoreError::NotFound);
            };
            self.confirm_parents.put_utxo_parent(
                height,
                create_fk,
                ptx.clone(),
                vout,
                o.clone(),
            );
            if tip_seeded.insert(id, ()).is_none() {
                let n = ptx.output_count as usize;
                let mut slots = vec![None; n];
                for (v, o) in outs.iter().enumerate() {
                    if v < n {
                        slots[v] = Some(o.clone());
                    }
                }
                self.tip_prevout_cache
                    .note_live_slots(create_fk, ptx.clone(), &slots);
            }
            st.utxo_parents = st.utxo_parents.saturating_add(1);
        }

        // ── Phase 3: mark scanned ───────────────────────────────────────────
        for &(height, _) in &work {
            self.confirm_parents.mark_scanned(height);
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

    pub fn unpin_spent_parent_outs(
        &self,
        spends: &[([u8; 32], u32)],
    ) -> Result<(), QueryError> {
        for &(txid, vout) in spends {
            let create_fk = self
                .ibd_utxo_create_fk(&txid, vout)?
                .or_else(|| self.confirm_parents.get_by_txid(&txid))
                .or_else(|| {
                    self.tip_prevout_cache
                        .get_tx_and_output_by_txid(&txid, vout)
                        .map(|(fk, _, _)| fk)
                })
                .or(self.tx_fk_by_txid(&txid).ok().flatten());
            if let Some(fk) = create_fk {
                self.confirm_parents.retire_spend(fk, vout);
            }
        }
        Ok(())
    }
}
