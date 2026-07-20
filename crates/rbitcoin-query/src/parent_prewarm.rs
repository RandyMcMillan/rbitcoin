//! Confirm-runway parent prewarm against [`crate::confirm_parent_cache`].
//!
//! Only loads parents present in the light UTXO (confirmed creates). Outpoints
//! not in the UTXO are **reserved** for creates that live on not-yet-confirmed
//! runway blocks; those fill when the creating body is registered during
//! prewarm of earlier heights (or immediately if the create was registered
//! first with full outs — create-before-reserve).
//!
//! Confirm must not start until the batch heights are **scanned** (ready) **and**
//! the warmer holds the configured headroom past `batch_end`. Open reservations
//! do not block readiness: a batch may create a parent and spend it in a later
//! height of the same run (wave resolves same-wave / runway creates).

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
    /// Parent outs served from confirm_parents / tip_prevout (no store).
    pub parent_cache_hits: u32,
    /// `get_tx_full` calls (prefer packed = 1 body IO).
    pub full_tx_reads: u32,
    /// Legacy split-table fallback reads (counted inside store path as full_tx).
    pub body_tx_reads: u32,
}

impl Query {
    pub fn parent_prewarm_depth(&self) -> u32 {
        self.confirm_parents.depth()
    }

    /// Contiguous ready watermark: all heights in `(tip, ready_through]` ready.
    pub fn parent_prewarm_ready_through(&self) -> u32 {
        self.confirm_parents.ready_through()
    }

    /// Snapshot for IBD progress/perf: `(ready_through, ahead, parents, open_reserves, plans, depth)`.
    ///
    /// `ahead` = how many blocks past tip the warmer has fully scanned
    /// (`ready_through.saturating_sub(tip)`).
    pub fn parent_prewarm_perf_snapshot(&self) -> (u32, u32, usize, usize, usize, u32) {
        let tip = self.tip_height().map(|h| h.0).unwrap_or(0);
        let through = self.confirm_parents.ready_through();
        let ahead = through.saturating_sub(tip);
        (
            through,
            ahead,
            self.confirm_parents.parent_count(),
            self.confirm_parents.reserved_count(),
            self.confirm_parents.plan_count(),
            self.confirm_parents.depth(),
        )
    }

    /// Drop runway plans at/below confirmed tip after Class C.
    pub fn advance_parent_runway_tip(&self, tip: u32) {
        self.confirm_parents.advance_tip(tip);
    }

    /// Seed height plans for the published runway (no body scan). Lets confirm
    /// headroom see unfinished heights instead of treating a short plan map as
    /// "runway ended".
    pub fn seed_parent_runway(&self, items: &[(u32, [u8; 32])]) {
        for &(height, hash) in items {
            self.confirm_parents.ensure_plan(height, hash);
        }
    }

    pub fn parent_pin_count(&self) -> usize {
        // Compat name: reserved + stored parent outs.
        self.confirm_parents.parent_count() + self.confirm_parents.reserved_count()
    }

    /// True when every height in the list is prewarm-scanned.
    /// Open reservations do not make a height unready.
    pub fn is_prewarm_ready(&self, heights: &[u32]) -> bool {
        self.confirm_parents.all_ready(heights)
    }

    /// Block until all `heights` are scanned (ready), `timeout` elapses, or
    /// [`Query::confirm_cancelled`].
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

    /// Wait until batch heights are **scanned** (hard). Then briefly prefer
    /// warmer headroom past `batch_end`, but **never block confirm** on it —
    /// mainnet showed hard headroom waits freezing tip for minutes while the
    /// worker chewed runway IO, which stalled peers (no archive drain).
    ///
    /// Aborts promptly on [`Query::confirm_cancelled`] (IBD SIGINT).
    /// Open reservations never block readiness (same-batch create→spend).
    /// `headroom == None` uses [`prewarm_headroom_from_env`].
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
        // Hard: this batch must be scanned (or cancel).
        self.wait_prewarm_ready(heights, timeout)?;
        if self.confirm_cancelled() {
            return Err(StoreError::Corrupt("confirm cancelled"));
        }
        let hr = headroom.unwrap_or_else(prewarm_headroom_from_env);
        if hr == 0 || self.confirm_parents.headroom_ready(batch_end, hr) {
            return Ok(());
        }
        // Soft: give the worker a short moment to pull further ahead, then go.
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

    /// Prewarm parents for archived runway blocks (height-ordered).
    ///
    /// `items` are `(height, hash)` ascending. Only UTXO-backed parents are
    /// loaded from store; others are reserved for runway creates.
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
            self.prewarm_one_height(height, &hash, &mut st)?;
        }
        crate::parent_prewarm_stats::note(&st, t0.elapsed().as_nanos() as u64);
        Ok(st)
    }

    /// Back-compat: hashes only (height unknown — treat as unordered scan).
    pub fn prewarm_parents_for_block_hashes(
        &self,
        hashes: &[[u8; 32]],
    ) -> Result<PrewarmStats, QueryError> {
        let tip = self.tip_height().map(|h| h.0).unwrap_or(0);
        let mut items = Vec::with_capacity(hashes.len());
        for (i, hash) in hashes.iter().enumerate() {
            // Caller (IBD) passes tip+1.. in order; recover height as tip+1+i.
            let h = tip.saturating_add(1).saturating_add(i as u32);
            items.push((h, *hash));
        }
        self.prewarm_parents_for_heights(&items)
    }

    fn prewarm_one_height(
        &self,
        height: u32,
        hash: &[u8; 32],
        st: &mut PrewarmStats,
    ) -> Result<(), QueryError> {
        let Some((header_fk, _)) = self.get_header_by_hash(hash)? else {
            return Ok(());
        };
        let Some(tx_fks) = self.store.header_txs.get_list(header_fk)? else {
            return Ok(());
        };
        st.blocks = st.blocks.saturating_add(1);

        // Load this block's bodies (packed = 1 IO each). Register creates first.
        let mut body_creates: Vec<(Fk, TxRecord, Vec<OutputRecord>, Vec<InputRecord>)> =
            Vec::with_capacity(tx_fks.len());
        let mut local: HashMap<[u8; 32], Fk> = HashMap::with_capacity(tx_fks.len());
        for &fk in &tx_fks {
            let (tx, inputs, outs) = self.store.get_tx_full(fk)?;
            st.full_tx_reads = st.full_tx_reads.saturating_add(1);
            st.body_tx_reads = st.body_tx_reads.saturating_add(1);
            self.confirm_parents
                .register_runway_creates(fk, &tx, &outs, height);
            st.creates_registered = st.creates_registered.saturating_add(1);
            local.insert(tx.txid, fk);
            body_creates.push((fk, tx, outs, inputs));
        }

        // Collect external parent needs: resolve fk, then sort/dedup before store IO.
        // (create_fk, vout) — only those not same-block and not already cached.
        let mut need_load: Vec<(u64, u32)> = Vec::new();
        let mut need_put: Vec<(u32, Fk, u32)> = Vec::new(); // (height, fk, vout) for cache hits path

        for (_fk, _tx, _outs, inputs) in &body_creates {
            for inp in inputs {
                if inp.is_coinbase() {
                    continue;
                }
                // Same-block create: no store.
                if let Some(&cfk) = local.get(&inp.prev_txid) {
                    if let Some((_, ptx, outs, _)) =
                        body_creates.iter().find(|(f, _, _, _)| *f == cfk)
                    {
                        if let Some(o) = outs.get(inp.prev_index as usize) {
                            self.confirm_parents.put_utxo_parent(
                                height,
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

                let create_fk = self
                    .ibd_utxo_create_fk(&inp.prev_txid, inp.prev_index)?
                    .or_else(|| self.confirm_parents.get_by_txid(&inp.prev_txid))
                    .or_else(|| {
                        self.tip_prevout_cache
                            .get_tx_and_output_by_txid(&inp.prev_txid, inp.prev_index)
                            .map(|(fk, _, _)| fk)
                    })
                    .or(self.tx_fk_by_txid(&inp.prev_txid).ok().flatten());

                match create_fk {
                    Some(create_fk) => {
                        if self
                            .confirm_parents
                            .get_parent_out(create_fk, inp.prev_index)
                            .is_some()
                        {
                            // Already in runway cache — re-note for this height.
                            if let Some((ptx, o)) =
                                self.confirm_parents.get_parent_out(create_fk, inp.prev_index)
                            {
                                self.confirm_parents.put_utxo_parent(
                                    height,
                                    create_fk,
                                    ptx,
                                    inp.prev_index,
                                    o,
                                );
                                st.utxo_parents = st.utxo_parents.saturating_add(1);
                                st.parent_cache_hits = st.parent_cache_hits.saturating_add(1);
                            }
                            continue;
                        }
                        if let Some(fk_id) = create_fk.get() {
                            need_load.push((fk_id, inp.prev_index));
                            need_put.push((height, create_fk, inp.prev_index));
                        }
                    }
                    None => {
                        self.confirm_parents.reserve(
                            height,
                            inp.prev_txid,
                            inp.prev_index,
                        );
                        st.reserved = st.reserved.saturating_add(1);
                    }
                }
            }
        }

        // Sort + dedup by (create_fk, vout); load once per unique create fk.
        need_load.sort_unstable();
        need_load.dedup();
        let mut uniq_fks: Vec<u64> = need_load.iter().map(|(f, _)| *f).collect();
        uniq_fks.sort_unstable();
        uniq_fks.dedup();
        st.parent_unique = st.parent_unique.saturating_add(uniq_fks.len() as u32);

        // One get_tx_full per unique create fk (packed = 1 body IO).
        let mut loaded: HashMap<u64, (TxRecord, Vec<OutputRecord>)> =
            HashMap::with_capacity(uniq_fks.len());
        for fk_id in uniq_fks {
            let fk = Fk(fk_id);
            let (ptx, _ins, outs) = self.store.get_tx_full(fk)?;
            st.full_tx_reads = st.full_tx_reads.saturating_add(1);
            loaded.insert(fk_id, (ptx, outs));
        }

        // Apply needed parent outs; seed tip_prevout once per unique create fk.
        let mut tip_seeded: HashMap<u64, ()> = HashMap::new();
        for &(_h, create_fk, vout) in &need_put {
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

        self.confirm_parents.mark_scanned(height);
        Ok(())
    }

    /// Drop spent parent outs after Class C (resolve fk via UTXO / cache).
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
