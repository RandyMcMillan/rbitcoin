//! Confirm-runway prewarm: **bodies first, then parents**.
//!
//! For a batch of heights (ascending):
//! 1. Load every full Class A body in the batch; register **all** creates so
//!    same-batch / runway spends need no UTXO and no reservations.
//! 2. Collect external parent needs; sort/dedup by create fk; load once each.
//!    Stash per-tx **thin create-fk edges** so wave_fill does not re-walk inputs.
//!
//! After prewarm, confirm should only **write** (Class C, light UTXO).
//! Wave/wire rebuild prefer the body + thin-edge cache over store.

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

    /// Prewarm a contiguous runway slice: **all bodies first**, then parents.
    ///
    /// `items` must be height-ascending. No reservations: same-batch creates are
    /// registered in phase 1; external parents load in phase 2 from UTXO/store.
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

        // ── Phase 1: resolve header → tx fks, then load bodies by **sorted fk** ─
        // Sorting improves sequential locality in packed `tx.body` under archive load.
        let mut height_tx_fks: Vec<(u32, Vec<Fk>)> = Vec::with_capacity(work.len());
        let mut all_body_fks: Vec<u64> = Vec::new();
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
            for fk in &tx_fks {
                if let Some(id) = fk.get() {
                    all_body_fks.push(id);
                }
            }
            height_tx_fks.push((height, tx_fks));
        }

        all_body_fks.sort_unstable();
        all_body_fks.dedup();
        let mut body_by_fk: HashMap<u64, (TxRecord, Vec<OutputRecord>, Vec<InputRecord>)> =
            HashMap::with_capacity(all_body_fks.len());
        for id in all_body_fks {
            if self.confirm_cancelled() {
                crate::parent_prewarm_stats::note(&st, t0.elapsed().as_nanos() as u64);
                return Err(StoreError::Corrupt("confirm cancelled"));
            }
            let (tx, inputs, outs) = self.store.get_tx_full(Fk(id))?;
            st.body_tx_reads = st.body_tx_reads.saturating_add(1);
            st.full_tx_reads = st.full_tx_reads.saturating_add(1);
            body_by_fk.insert(id, (tx, outs, inputs));
        }

        let mut phase1: Vec<(u32, Vec<(Fk, TxRecord, Vec<OutputRecord>, Vec<InputRecord>)>)> =
            Vec::with_capacity(height_tx_fks.len());
        for (height, tx_fks) in height_tx_fks {
            let mut bodies = Vec::with_capacity(tx_fks.len());
            for fk in tx_fks {
                let Some(id) = fk.get() else {
                    continue;
                };
                let Some((tx, outs, inputs)) = body_by_fk.get(&id) else {
                    continue;
                };
                self.confirm_parents.put_body_and_creates(
                    fk,
                    height,
                    tx.clone(),
                    outs.clone(),
                    inputs.clone(),
                );
                st.creates_registered = st.creates_registered.saturating_add(1);
                bodies.push((fk, tx.clone(), outs.clone(), inputs.clone()));
            }
            phase1.push((height, bodies));
        }
        drop(body_by_fk);

        // ── Phase 2: external parents + stash thin edges for wave_fill ──────
        let mut need_load: Vec<(u64, u32)> = Vec::new();
        let mut need_put: Vec<(u32, Fk, u32)> = Vec::new();
        let mut parent_puts: Vec<(u32, Fk, TxRecord, u32, OutputRecord)> = Vec::new();
        let mut thin_by_spend: HashMap<u64, Vec<StashedThinInput>> = HashMap::new();
        let catchup = self.ibd_utxo_enabled();

        for (height, bodies) in &phase1 {
            let mut local: HashMap<[u8; 32], Fk> = HashMap::with_capacity(bodies.len());
            for (fk, tx, _, _) in bodies {
                local.insert(tx.txid, *fk);
            }
            for (spend_fk, _tx, _outs, inputs) in bodies {
                let mut edges: Vec<StashedThinInput> = Vec::with_capacity(inputs.len());
                for inp in inputs {
                    if inp.is_coinbase() {
                        edges.push(StashedThinInput {
                            create_fk: None,
                            prev_index: inp.prev_index,
                        });
                        continue;
                    }
                    // Same-block create: outs already in by_fk via put_body_and_creates.
                    if let Some(&cfk) = local.get(&inp.prev_txid) {
                        edges.push(StashedThinInput {
                            create_fk: cfk.get(),
                            prev_index: inp.prev_index,
                        });
                        st.utxo_parents = st.utxo_parents.saturating_add(1);
                        st.parent_cache_hits = st.parent_cache_hits.saturating_add(1);
                        continue;
                    }
                    // Runway create: outs already cached — thin edge only (no put).
                    if let Some(cfk) = self.confirm_parents.get_by_txid(&inp.prev_txid) {
                        if self
                            .confirm_parents
                            .get_parent_out(cfk, inp.prev_index)
                            .is_some()
                        {
                            edges.push(StashedThinInput {
                                create_fk: cfk.get(),
                                prev_index: inp.prev_index,
                            });
                            st.utxo_parents = st.utxo_parents.saturating_add(1);
                            st.parent_cache_hits = st.parent_cache_hits.saturating_add(1);
                            continue;
                        }
                    }
                    // Confirmed parent: light UTXO → durable head.
                    let create_fk = self
                        .confirm_parents
                        .get_by_txid(&inp.prev_txid)
                        .or(self.ibd_utxo_create_fk(&inp.prev_txid, inp.prev_index)?)
                        .or(self.tx_fk_by_txid(&inp.prev_txid).ok().flatten());

                    match create_fk {
                        Some(create_fk) => {
                            edges.push(StashedThinInput {
                                create_fk: create_fk.get(),
                                prev_index: inp.prev_index,
                            });
                            if let Some((ptx, o)) =
                                self.confirm_parents.get_parent_out(create_fk, inp.prev_index)
                            {
                                // Rare: partial parent entry — put this vout only.
                                parent_puts.push((*height, create_fk, ptx, inp.prev_index, o));
                                st.utxo_parents = st.utxo_parents.saturating_add(1);
                                st.parent_cache_hits = st.parent_cache_hits.saturating_add(1);
                                continue;
                            }
                            // Already spent: wave will drop the slot; skip parent body IO.
                            if catchup
                                && self.catchup_is_spent(&inp.prev_txid, inp.prev_index)?
                            {
                                st.utxo_parents = st.utxo_parents.saturating_add(1);
                                continue;
                            }
                            if let Some(id) = create_fk.get() {
                                need_load.push((id, inp.prev_index));
                                need_put.push((*height, create_fk, inp.prev_index));
                            }
                        }
                        None => {
                            edges.push(StashedThinInput {
                                create_fk: None,
                                prev_index: inp.prev_index,
                            });
                            st.missing_parents = st.missing_parents.saturating_add(1);
                        }
                    }
                }
                if let Some(id) = spend_fk.get() {
                    thin_by_spend.insert(id, edges);
                }
            }
        }

        need_load.sort_unstable();
        need_load.dedup();
        let mut uniq_fks: Vec<u64> = need_load.iter().map(|(f, _)| *f).collect();
        uniq_fks.sort_unstable();
        uniq_fks.dedup();
        st.parent_unique = st.parent_unique.saturating_add(uniq_fks.len() as u32);

        // Parallel parent meta+outs loads (sorted fks; mmap reads are concurrent-safe).
        let loaded: HashMap<u64, (TxRecord, Vec<OutputRecord>)> = {
            use rayon::prelude::*;
            let store = self.store();
            let rows: Result<Vec<_>, QueryError> = uniq_fks
                .par_iter()
                .map(|&fk_id| {
                    let (ptx, outs) = store.get_tx_meta_and_outputs(Fk(fk_id))?;
                    Ok((fk_id, (ptx, outs)))
                })
                .collect();
            let rows = rows?;
            st.full_tx_reads = st.full_tx_reads.saturating_add(rows.len() as u32);
            rows.into_iter().collect()
        };

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
            parent_puts.push((height, create_fk, ptx.clone(), vout, o.clone()));
            st.utxo_parents = st.utxo_parents.saturating_add(1);
        }

        self.confirm_parents.put_utxo_parents_batch(&parent_puts);

        let thin_items: Vec<(Fk, Vec<StashedThinInput>)> = thin_by_spend
            .into_iter()
            .map(|(id, edges)| (Fk(id), edges))
            .collect();
        self.confirm_parents.put_thin_inputs_batch(&thin_items);

        // ── Phase 3: mark scanned (one ready_through recompute) ─────────────
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
    /// **Catch-up (light UTXO on):** no-op. Spentness for the next wave is
    /// decided by [`Query::catchup_is_spent`], not by zeroing cache slots.
    /// Skipping avoids O(spends) UTXO/head probes that dominated confirm wall
    /// (~50–60% in mainnet logs). Runway RAM stays bounded by tip advance
    /// (drop plans/bodies + parent GC).
    ///
    /// **Tip-follow / index mode:** cheap path only — retire outs already
    /// present in the runway (`by_txid`). No UTXO or durable-head lookup.
    pub fn unpin_spent_parent_outs(
        &self,
        spends: &[([u8; 32], u32)],
    ) -> Result<(), QueryError> {
        if spends.is_empty() {
            return Ok(());
        }
        // IBD catch-up: wave_fill filters parent slots with catchup_is_spent;
        // connect hits the wave. Cache hygiene is tip GC, not per-spend UTXO.
        if self.ibd_utxo_enabled() {
            return Ok(());
        }
        // Tip-follow: only touch parents already pinned on the runway.
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
