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
}

impl Query {
    pub fn parent_prewarm_depth(&self) -> u32 {
        self.confirm_parents.depth()
    }

    /// Contiguous ready watermark: all heights in `(tip, ready_through]` ready.
    pub fn parent_prewarm_ready_through(&self) -> u32 {
        self.confirm_parents.ready_through()
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

    /// Block until all `heights` are scanned (ready) or `timeout` elapses.
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

    /// Wait until batch heights are scanned **and** the warmer is `headroom`
    /// blocks past `batch_end` (or the published runway ends).
    ///
    /// Open reservations never block this wait (same-batch create→spend).
    /// `headroom == None` uses [`prewarm_headroom_from_env`] (default 2× batch).
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
        let hr = headroom.unwrap_or_else(prewarm_headroom_from_env);
        let start = std::time::Instant::now();
        loop {
            if self.confirm_parents.all_ready(heights)
                && self.confirm_parents.headroom_ready(batch_end, hr)
            {
                return Ok(());
            }
            if start.elapsed() >= timeout {
                return Err(StoreError::Corrupt(
                    "confirm parent prewarm headroom not ready (timeout)",
                ));
            }
            std::thread::sleep(std::time::Duration::from_millis(2));
        }
    }

    /// Prewarm parents for archived runway blocks (height-ordered).
    ///
    /// `items` are `(height, hash)` ascending. Only UTXO-backed parents are
    /// loaded from store; others are reserved for runway creates.
    pub fn prewarm_parents_for_heights(
        &self,
        items: &[(u32, [u8; 32])],
    ) -> Result<PrewarmStats, QueryError> {
        let mut st = PrewarmStats::default();
        if items.is_empty() {
            return Ok(st);
        }
        let tip = self.tip_height().map(|h| h.0).unwrap_or(0);
        self.confirm_parents.advance_tip(tip);

        for &(height, hash) in items {
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

        // Index this block's creates first so later spends in the same block
        // and later runway heights can resolve them without UTXO.
        let mut body_creates: Vec<(Fk, TxRecord, Vec<OutputRecord>)> = Vec::new();
        for &fk in &tx_fks {
            let tx = self.store.get_tx(fk)?;
            let outs = if tx.output_count > 0 {
                let run = tx.output_start_fk.get().ok_or(StoreError::InvalidFk)?;
                self.store.get_output_run(Fk(run), tx.output_count)?
            } else {
                Vec::new()
            };
            self.confirm_parents
                .register_runway_creates(fk, &tx, &outs, height);
            st.creates_registered = st.creates_registered.saturating_add(1);
            body_creates.push((fk, tx, outs));
        }

        // Same-block txid → fk for in-block spends.
        let mut local: HashMap<[u8; 32], Fk> = HashMap::new();
        for (fk, tx, _) in &body_creates {
            local.insert(tx.txid, *fk);
        }

        for (_fk, tx, _outs) in &body_creates {
            let inputs = if tx.input_count > 0 {
                let run = tx.input_start_fk.get().ok_or(StoreError::InvalidFk)?;
                self.store.get_input_run(Fk(run), tx.input_count)?
            } else {
                continue;
            };
            for inp in &inputs {
                if inp.is_coinbase() {
                    continue;
                }
                // Same-block create: pull from body we just registered.
                if let Some(&cfk) = local.get(&inp.prev_txid) {
                    if let Some((_, _, outs)) =
                        body_creates.iter().find(|(f, _, _)| *f == cfk)
                    {
                        if let Some(o) = outs.get(inp.prev_index as usize) {
                            let ptx = body_creates
                                .iter()
                                .find(|(f, _, _)| *f == cfk)
                                .map(|(_, t, _)| t.clone())
                                .unwrap();
                            self.confirm_parents.put_utxo_parent(
                                height,
                                cfk,
                                ptx,
                                inp.prev_index,
                                o.clone(),
                            );
                            st.utxo_parents = st.utxo_parents.saturating_add(1);
                        }
                    }
                    continue;
                }

                // Resolve create: light UTXO first (IBD catch-up), then durable
                // tx index / runway cache (Tip mode, or UTXO miss with known body).
                // Only reserve when the create is not findable yet (later runway).
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
                        // Prefer already-cached runway/UTXO outs (no re-read).
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
                            continue;
                        }
                        let ptx = self.store.get_tx(create_fk)?;
                        let run = ptx.output_start_fk.get().ok_or(StoreError::InvalidFk)?;
                        let all = self.store.get_output_run(Fk(run), ptx.output_count)?;
                        let o = all
                            .get(inp.prev_index as usize)
                            .cloned()
                            .ok_or(StoreError::NotFound)?;
                        self.confirm_parents.put_utxo_parent(
                            height,
                            create_fk,
                            ptx,
                            inp.prev_index,
                            o,
                        );
                        // Also seed tip_prevout for connect short-circuit.
                        if let Some((tx, outs_map)) =
                            self.confirm_parents.get_parent_outs(create_fk)
                        {
                            let n = tx.output_count as usize;
                            let mut slots = vec![None; n];
                            for (v, o) in outs_map {
                                if (v as usize) < n {
                                    slots[v as usize] = Some(o);
                                }
                            }
                            self.tip_prevout_cache.note_live_slots(create_fk, tx, &slots);
                        }
                        st.utxo_parents = st.utxo_parents.saturating_add(1);
                    }
                    None => {
                        // Create body not yet known — reserve for runway register.
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
