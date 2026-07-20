//! Tip confirm / connect / disconnect.

use super::*;

/// One height ready for Class C (header + body already archived).
///
/// Callers that already resolved `header_fk` / `tx_fks` (e.g. multi-block confirm)
/// pass them here to avoid redoing hash lookups.
#[derive(Debug, Clone)]
pub struct ConfirmPrepared {
    pub height: Height,
    pub header_fk: Fk,
    pub tx_fks: Vec<Fk>,
}

impl Query {
    pub fn confirm_block(&self, height: Height, header_hash: &[u8; 32]) -> Result<Fk, QueryError> {
        // Idempotent if already confirmed at this height.
        if let Some(h) = self.height_of_hash(header_hash)? {
            if h == height {
                if let Some((fk, _)) = self.get_header_by_hash(header_hash)? {
                    return Ok(fk);
                }
            }
        }

        let (header_fk, _rec) = self
            .get_header_by_hash(header_hash)?
            .ok_or(StoreError::NotFound)?;
        let tx_fks = self
            .store
            .header_txs
            .get_list(header_fk)?
            .ok_or(StoreError::Corrupt("confirm without archived body"))?;

        let out = self.confirm_blocks_run(&[ConfirmPrepared {
            height,
            header_fk,
            tx_fks,
        }])?;
        Ok(out[0])
    }

    /// Confirm a contiguous tip-extension run of already-archived bodies.
    ///
    /// Skips per-block `height_of_hash` / `get_header_by_hash` (caller supplies fks).
    ///
    /// # Class C write order (crash atomicity)
    ///
    /// Per block we write `strong_tx` + `tx_height` (and optional scripthash
    /// marks), then **last** advance `confirmed[]` for the whole run. The confirmed
    /// tip is the commit point: [`rbitcoin_store::Store::spenders`] /
    /// [`rbitcoin_store::Store::is_confirmed_strong`] only treat a spend as
    /// best-chain once `tx_height ≤ tip`. A hard kill after strong bits but
    /// before tip advance leaves recoverable state — open repairs strong above
    /// tip, and re-confirm of tip+1 does not see false PrevoutSpent.
    ///
    /// # Point edges
    ///
    /// When `spend_index` is on, archive already wrote durable point edges.
    /// Confirm does **not** re-probe `spenders_raw` / re-append edges: that was
    /// O(inputs × point.head) per block and hung tip Class C on large signet
    /// archives (@182692 class: scripts finished, `class_c_ms=0` forever).
    /// Gaps from archive-with-spend-off are filled by `backfill_point_spends`.
    pub fn confirm_blocks_run(
        &self,
        items: &[ConfirmPrepared],
    ) -> Result<Vec<Fk>, QueryError> {
        if items.is_empty() {
            return Ok(Vec::new());
        }
        self.ensure_spent_oracle_ready()?;
        for w in items.windows(2) {
            if w[1].height.0 != w[0].height.0.saturating_add(1) {
                return Err(StoreError::Corrupt("confirm run not contiguous heights"));
            }
        }

        // Tip linkage: first height must be genesis or tip+1.
        match self.tip_height() {
            None => {
                if items[0].height != Height::GENESIS {
                    return Err(StoreError::Corrupt("first block must be genesis height"));
                }
            }
            Some(tip) => {
                let expect = tip.next().ok_or(StoreError::Corrupt("height overflow"))?;
                if items[0].height != expect {
                    // Idempotent single-height re-confirm at current tip height.
                    if items.len() == 1 {
                        if let Some(fk) = self.store.confirmed.get(items[0].height)? {
                            if fk == items[0].header_fk {
                                return Ok(vec![fk]);
                            }
                        }
                    }
                    return Err(StoreError::Corrupt("connect height not tip+1"));
                }
            }
        }

        // Validate header fks up front (both parallel arms need valid items).
        for item in items {
            if item.header_fk.is_null() {
                return Err(StoreError::InvalidFk);
            }
        }

        // Parallel Class C: strong/height || scripthash collect+append.
        // Independent tables / caches; join before tip (commit point).
        // SH may lead tip on kill — queries require is_confirmed_strong.
        use std::sync::atomic::Ordering;
        let mut confirmed_pairs = Vec::with_capacity(items.len());
        let mut out = Vec::with_capacity(items.len());
        let mut strong_err: Option<QueryError> = None;
        let mut sh_err: Option<QueryError> = None;

        std::thread::scope(|scope| {
            let strong_slot = scope.spawn(|| -> Result<(Vec<(Height, Fk)>, Vec<Fk>), QueryError> {
                let t_strong = std::time::Instant::now();
                let mut pairs = Vec::with_capacity(items.len());
                let mut fks = Vec::with_capacity(items.len());
                for item in items {
                    let contiguous = item
                        .tx_fks
                        .windows(2)
                        .all(|w| w[1].0 == w[0].0.saturating_add(1));
                    if contiguous {
                        if let Some(&first) = item.tx_fks.first() {
                            self.store.strong_tx.set_strong_range(
                                first,
                                item.tx_fks.len() as u32,
                                item.header_fk,
                            )?;
                            self.store.tx_height.set_range(
                                first,
                                item.tx_fks.len() as u32,
                                item.height,
                            )?;
                        }
                    } else {
                        for &tx_fk in &item.tx_fks {
                            self.store.strong_tx.set_strong(tx_fk, item.header_fk)?;
                            self.store.tx_height.set(tx_fk, item.height)?;
                        }
                    }
                    pairs.push((item.height, item.header_fk));
                    fks.push(item.header_fk);
                }
                crate::class_c_phase_stats::STRONG_NS
                    .fetch_add(t_strong.elapsed().as_nanos() as u64, Ordering::Relaxed);
                Ok((pairs, fks))
            });

            let sh_slot = scope.spawn(|| -> Result<(), QueryError> {
                use crate::class_c_phase_stats::{self as sh_stats, add_sh_part};

                // Height watermark: skip heights already SH-indexed after prior tip commit.
                // (Replaces unbounded sh_tx_indexed HashSet.)
                let t_filter = std::time::Instant::now();
                let through = self.sh_indexed_through_height();
                let mut sh_new_txs: Vec<u64> = Vec::new();
                let wave_tx_n: usize = items.iter().map(|i| i.tx_fks.len()).sum();
                sh_new_txs.reserve(wave_tx_n);
                for item in items {
                    if through.map(|t| item.height.0 <= t).unwrap_or(false) {
                        continue;
                    }
                    for &tx_fk in &item.tx_fks {
                        sh_new_txs.push(tx_fk.0);
                    }
                }
                add_sh_part(
                    &sh_stats::SH_FILTER_NS,
                    t_filter.elapsed().as_nanos() as u64,
                );

                let t_collect = std::time::Instant::now();
                let mut sh_creates: Vec<ScriptHashRecord> = Vec::new();
                // Rough upper bound: a few outputs per new tx (grows as needed).
                sh_creates.reserve(sh_new_txs.len().saturating_mul(2));
                for &tx_id in &sh_new_txs {
                    self.collect_scripthash_creates(Fk(tx_id), &mut sh_creates)?;
                }
                add_sh_part(
                    &sh_stats::SH_COLLECT_NS,
                    t_collect.elapsed().as_nanos() as u64,
                );

                if !sh_creates.is_empty() {
                    if self.sh_run.is_enabled() {
                        // Catch-up: enqueue only (sequential runs + low-prio worker).
                        // No durable scripthash.head seed/head RMW on confirm.
                        self.sh_run.enqueue(&sh_creates);
                    } else {
                        let mut heads = self.sh_heads.lock().unwrap();
                        let (_fks, timing) = self
                            .store
                            .scripthash
                            .put_create_batch_append(&sh_creates, &mut heads)?;
                        add_sh_part(&sh_stats::SH_SORT_NS, timing.sort_ns);
                        add_sh_part(&sh_stats::SH_SEED_NS, timing.seed_ns);
                        add_sh_part(&sh_stats::SH_BODY_NS, timing.body_ns);
                        add_sh_part(&sh_stats::SH_HEAD_NS, timing.head_ns);
                    }
                }
                // Watermark advances only after tip commit (below).
                Ok(())
            });

            match strong_slot.join() {
                Ok(Ok((pairs, fks))) => {
                    confirmed_pairs = pairs;
                    out = fks;
                }
                Ok(Err(e)) => strong_err = Some(e),
                Err(_) => {
                    strong_err = Some(StoreError::Corrupt("strong/height thread panicked"));
                }
            }
            match sh_slot.join() {
                Ok(Ok(())) => {}
                Ok(Err(e)) => sh_err = Some(e),
                Err(_) => {
                    sh_err = Some(StoreError::Corrupt("scripthash thread panicked"));
                }
            }
        });

        if let Some(e) = strong_err {
            return Err(e);
        }
        if let Some(e) = sh_err {
            return Err(e);
        }

        // Tip is the commit point (after strong + SH both finished).
        let t_tip = std::time::Instant::now();
        self.store.confirmed.set_many(&confirmed_pairs)?;
        // SH height watermark only after tip — failed tip must re-enqueue SH.
        if let Some(last) = items.last() {
            self.set_sh_indexed_through_height(Some(last.height.0));
        }
        crate::class_c_phase_stats::TIP_NS.fetch_add(
            t_tip.elapsed().as_nanos() as u64,
            Ordering::Relaxed,
        );

        // Tip-window prevout cache only (C): creates we just confirmed.
        // Load via Class A (not tip_prevout probe) so we don't MISS-spam stats
        // while seeding the cache from the wave we just confirmed.
        for item in items {
            for &tx_fk in &item.tx_fks {
                let tx = match self.get_tx_class_a(tx_fk) {
                    Ok(t) => t,
                    Err(_) => continue,
                };
                let outs = if tx.output_count == 0 {
                    Vec::new()
                } else {
                    match self.tx_output_run_class_a(&tx) {
                        Ok(o) => o,
                        Err(_) => continue,
                    }
                };
                self.tip_prevout_note(tx_fk, tx, outs);
            }
        }

        Ok(out)
    }

    /// Append point multimap edges for all non-coinbase inputs of `tx_fk`.
    pub(crate) fn mark_spends_for_tx(
        &self,
        tx_fk: Fk,
        probe_existing: bool,
    ) -> Result<(), QueryError> {
        let edges = self.collect_spend_edges(tx_fk, probe_existing)?;
        if edges.is_empty() {
            return Ok(());
        }
        if edges.len() == 1 {
            let (txid, vout, sfk, idx) = edges[0];
            self.store.put_spend(&txid, vout, sfk, idx)?;
        } else {
            self.store.put_spend_batch(&edges)?;
        }
        Ok(())
    }

    /// Collect durable point edges for one tx (optionally skipping existing).
    pub(crate) fn collect_spend_edges(
        &self,
        tx_fk: Fk,
        probe_existing: bool,
    ) -> Result<Vec<([u8; 32], u32, Fk, u32)>, QueryError> {
        let tx = self.store.get_tx(tx_fk)?;
        if tx.input_count == 0 {
            return Ok(Vec::new());
        }
        let inputs = self.tx_input_run(&tx)?;
        let mut edges = Vec::with_capacity(inputs.len());
        for (i, inp) in inputs.iter().enumerate() {
            if inp.is_coinbase() {
                continue;
            }
            let prev_txid = self.resolve_prev_txid(inp)?;
            if prev_txid == [0u8; 32] {
                continue;
            }
            let in_idx = i as u32;
            if probe_existing {
                let already = self
                    .store
                    .spenders_raw(&prev_txid, inp.prev_index)?
                    .iter()
                    .any(|p| p.spending_tx_fk == tx_fk && p.spending_input_index == in_idx);
                if already {
                    continue;
                }
            }
            edges.push((prev_txid, inp.prev_index, tx_fk, in_idx));
        }
        Ok(edges)
    }

    /// Collect thin scripthash create pointers for one tx's outputs (no spend marks).
    pub(crate) fn collect_scripthash_creates(
        &self,
        tx_fk: Fk,
        out: &mut Vec<ScriptHashRecord>,
    ) -> Result<(), QueryError> {
        // Class A only: creates in the confirm wave are not tip-prevout probes.
        let tx = self.get_tx_class_a(tx_fk)?;
        if tx.output_count == 0 {
            return Ok(());
        }
        let outputs = self.tx_output_run_class_a(&tx)?;
        for (i, o) in outputs.iter().enumerate() {
            out.push(ScriptHashRecord {
                scripthash: script_hash(&o.script),
                create_tx_fk: tx_fk,
                vout: i as u32,
                next: Fk::NULL,
                txid: [0u8; 32],
                value: 0,
                create_height: 0,
            });
        }
        Ok(())
    }

    /// Load all inputs for a Class A tx (one run read).
    pub fn tx_input_run(&self, tx: &TxRecord) -> Result<Vec<InputRecord>, QueryError> {
        if tx.input_count == 0 {
            return Ok(Vec::new());
        }
        if let Some(fk) = self.lookup_tx_fk(&tx.txid)? {
            if let Some(inputs) = self.class_a_cache.get_inputs(fk) {
                return Ok(inputs);
            }
            let run = tx.input_start_fk.get().ok_or(StoreError::InvalidFk)?;
            let inputs = self.get_input_run(Fk(run), tx.input_count)?;
            self.class_a_cache.fill_inputs(fk, inputs.clone());
            return Ok(inputs);
        }
        let run = tx.input_start_fk.get().ok_or(StoreError::InvalidFk)?;
        self.get_input_run(Fk(run), tx.input_count)
    }

    /// Output run: tip_prevout → Class A → store (connect prevout path).
    ///
    /// Miss-fill notes into **tip_prevout** (promote resolved parents). Reconstruct
    /// must use [`Self::tx_output_run_class_a`] instead.
    pub(crate) fn tx_output_run(&self, tx: &TxRecord) -> Result<Vec<OutputRecord>, QueryError> {
        if tx.output_count == 0 {
            return Ok(Vec::new());
        }
        if let Some(fk) = self.lookup_tx_fk(&tx.txid)? {
            if let Some(outputs) = self.tip_prevout_cache.get_outputs(fk) {
                return Ok(outputs);
            }
            if let Some(outputs) = self.class_a_cache.get_outputs(fk) {
                return Ok(outputs);
            }
            let run = tx.output_start_fk.get().ok_or(StoreError::InvalidFk)?;
            let outputs = self.get_output_run(Fk(run), tx.output_count)?;
            self.tip_prevout_cache
                .note(fk, tx.clone(), outputs.clone());
            self.class_a_cache.fill_outputs(fk, outputs.clone());
            return Ok(outputs);
        }
        let run = tx.output_start_fk.get().ok_or(StoreError::InvalidFk)?;
        self.get_output_run(Fk(run), tx.output_count)
    }

    /// Output run via Class A → store only (no tip_prevout probe or miss-fill).
    pub(crate) fn tx_output_run_class_a(
        &self,
        tx: &TxRecord,
    ) -> Result<Vec<OutputRecord>, QueryError> {
        if tx.output_count == 0 {
            return Ok(Vec::new());
        }
        if let Some(fk) = self.lookup_tx_fk(&tx.txid)? {
            if let Some(outputs) = self.class_a_cache.get_outputs(fk) {
                return Ok(outputs);
            }
            let run = tx.output_start_fk.get().ok_or(StoreError::InvalidFk)?;
            let outputs = self.get_output_run(Fk(run), tx.output_count)?;
            self.class_a_cache.fill_outputs(fk, outputs.clone());
            return Ok(outputs);
        }
        let run = tx.output_start_fk.get().ok_or(StoreError::InvalidFk)?;
        self.get_output_run(Fk(run), tx.output_count)
    }

    /// Connect a block at `height` (genesis or tip+1): archive Class A then confirm Class C.
    ///
    /// Back-compat wrapper around [`archive_block`] + [`confirm_block`].
    pub fn connect_block(
        &self,
        height: Height,
        header: &HeaderRecord,
        txs: &[TxApply],
    ) -> Result<Fk, QueryError> {
        self.archive_block(header, txs)?;
        self.confirm_block(height, &header.hash)
    }

    /// Disconnect the current tip (Class C + scripthash create unlink; archive remains).
    ///
    /// Also removes this tip’s spends from [`Self::spent_local`] so reorg cannot
    /// leave false double-spend blocks under local-only spentness.
    pub fn disconnect_tip(&self) -> Result<(), QueryError> {
        let height = self
            .tip_height()
            .ok_or(StoreError::Corrupt("no tip to disconnect"))?;
        let tx_fks = self.block_tx_fks(height)?;

        // Undo catch-up spentness oracle for this tip (Core reorg).
        let mut unspends: Vec<([u8; 32], u32, Fk)> = Vec::new();
        let mut uncreates: Vec<([u8; 32], u32)> = Vec::new();
        for &tx_fk in &tx_fks {
            let tx = self.store.get_tx(tx_fk)?;
            for v in 0..tx.output_count {
                uncreates.push((tx.txid, v));
            }
            if tx.input_count == 0 {
                continue;
            }
            let inputs = self.tx_input_run(&tx)?;
            for inp in &inputs {
                if inp.is_coinbase() {
                    continue;
                }
                let prev_txid = self.resolve_prev_txid(inp)?;
                // Prefer stamped local prev; else head/runs; rare linear Class A scan.
                let parent_fk = inp
                    .prev_tx_fk
                    .get()
                    .map(Fk)
                    .or_else(|| self.tx_fk_by_txid(&prev_txid).ok().flatten())
                    .or_else(|| self.find_tx_fk_by_txid_scan(&prev_txid).ok().flatten())
                    .unwrap_or(Fk::NULL);
                unspends.push((prev_txid, inp.prev_index, parent_fk));
            }
        }
        if self.ibd_utxo_enabled() {
            // Reverse of apply: remove creates, re-insert spent prevouts with create fk.
            let mut g = self.ibd_utxo.lock().unwrap();
            if let Some(ref mut u) = *g {
                for &(txid, vout) in &uncreates {
                    let _ = u.take_spend(&txid, vout)?;
                }
                for &(txid, vout, parent_fk) in &unspends {
                    if !parent_fk.is_null() {
                        u.insert_create(&txid, vout, parent_fk)?;
                    }
                }
                let new_tip = height.0.checked_sub(1);
                u.commit_tip(new_tip)?;
            }
        } else {
            let spends_only: Vec<_> = unspends.iter().map(|&(t, v, _)| (t, v)).collect();
            self.unnote_outpoints_spent_local(&spends_only);
        }

        let mut touched_sh: Vec<[u8; 32]> = Vec::new();
        for &tx_fk in &tx_fks {
            let tx = self.store.get_tx(tx_fk)?;
            // Unlink thin scripthash creates for this block's outputs.
            if tx.output_count > 0 {
                let outputs = self.tx_output_run(&tx)?;
                for (i, o) in outputs.iter().enumerate() {
                    let sh = script_hash(&o.script);
                    let _ = self.store.scripthash.unlink_create(&sh, tx_fk, i as u32)?;
                    touched_sh.push(sh);
                }
            }
            self.store.strong_tx.set_unstrong(tx_fk)?;
            self.store.tx_height.clear(tx_fk)?;
        }
        // Refresh process heads for unlinked scripts (may now be NULL / older fk).
        if !touched_sh.is_empty() {
            let mut heads = self.sh_heads.lock().unwrap();
            for sh in touched_sh {
                let live = self.store.scripthash.live_head(&sh).unwrap_or(Fk::NULL);
                if live.is_null() {
                    heads.remove(&sh);
                } else {
                    heads.insert(sh, live);
                }
            }
        }
        // Class A header_txs list remains with the header; only tip Class C moves.
        self.store.confirmed.disconnect_tip(height)?;
        // SH watermark tracks confirmed tip (re-confirm will re-enqueue this height).
        self.set_sh_indexed_through_height(self.tip_height().map(|h| h.0));
        // Tip-window cache may hold creates from this tip; drop rather than partial unlink.
        self.tip_prevout_cache.clear();
        Ok(())
    }
}
