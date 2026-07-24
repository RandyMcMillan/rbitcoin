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
    /// # Spend annotations
    ///
    /// When `spend_index` is on, durable spend annotations land on create outputs
    /// (schema v5+). Under Direct IBD, **confirm** batch-writes those annotations
    /// after Class C (not archive). Tip mode assumes they are already complete.
    pub fn confirm_blocks_run(
        &self,
        items: &[ConfirmPrepared],
    ) -> Result<Vec<Fk>, QueryError> {
        if items.is_empty() {
            return Ok(Vec::new());
        }
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
                // Use Class A by **create fk** (wave already filled outs). Never
                // resolve via txid — catch-up has no durable head and tx_run.lookup
                // walks sorted runs per tx (signet multi-second SH collect).
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
                        let (_n, timing) = self
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
        // Always key packed body by `tx_fk` (catch-up has no durable `tx.head`).
        let inputs = self.tx_input_run_class_a(tx_fk, &tx)?;
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
    ///
    /// Prefer parent cache `by_body` outs (no Class A re-decode). Store
    /// full decode is last resort — write was spending multi-second SH collect
    /// re-reading bodies that scripts already held in RAM.
    pub(crate) fn collect_scripthash_creates(
        &self,
        tx_fk: Fk,
        out: &mut Vec<ScriptHashRecord>,
    ) -> Result<(), QueryError> {
        // Outs-only FIFO when still warm (no full body / no range cache).
        if let Some((_h, _tx, outputs, _ins)) = self.confirm_parents.get_body_for_pin(tx_fk) {
            for o in outputs.iter() {
                out.push(ScriptHashRecord::from_fk(script_hash(&o.script), tx_fk));
            }
            return Ok(());
        }
        // Cold store: create_fk → tx.idx → body.
        let tx = self.get_tx_class_a(tx_fk)?;
        if tx.output_count == 0 {
            return Ok(());
        }
        let outputs = self.tx_output_run_class_a(tx_fk, &tx)?;
        for o in outputs.iter() {
            out.push(ScriptHashRecord::from_fk(script_hash(&o.script), tx_fk));
        }
        Ok(())
    }

    /// Load all inputs for a Class A tx.
    ///
    /// Packed rows (`input_start_fk` null): one `get_tx_full` by txid→fk (needs
    /// head/runs). Prefer [`Self::tx_input_run_class_a`] when the create fk is known
    /// (catch-up with `tx.head` off).
    pub fn tx_input_run(&self, tx: &TxRecord) -> Result<Vec<InputRecord>, QueryError> {
        if tx.input_count == 0 {
            return Ok(Vec::new());
        }
        let fk = self
            .lookup_tx_fk(&tx.txid)?
            .ok_or(StoreError::NotFound)?;
        self.tx_input_run_class_a(fk, tx)
    }

    /// Input run keyed by known create fk (packed body works without `tx.head`).
    pub fn tx_input_run_class_a(
        &self,
        create_fk: Fk,
        tx: &TxRecord,
    ) -> Result<Vec<InputRecord>, QueryError> {
        if tx.input_count == 0 {
            return Ok(Vec::new());
        }
        // Packed Class A — one body IO.
        let (_, inputs, _) = self.store.get_tx_full(create_fk)?;
        if inputs.len() as u32 != tx.input_count {
            return Err(StoreError::Corrupt("packed input count mismatch"));
        }
        Ok(inputs)
    }

    /// Output run from store (keyed by known create fk — no txid lookup).
    ///
    /// Preferred for packed Class A (works with `tx.head` off). Callers that only
    /// have a [`TxRecord`] must resolve create fk first (UTXO / head / scan).
    pub(crate) fn tx_output_run_class_a(
        &self,
        create_fk: Fk,
        tx: &TxRecord,
    ) -> Result<Vec<OutputRecord>, QueryError> {
        if tx.output_count == 0 {
            return Ok(Vec::new());
        }
        // Packed Class A.
        let (_, _, outs) = self.store.get_tx_full(create_fk)?;
        if outs.len() as u32 != tx.output_count {
            return Err(StoreError::Corrupt("packed output count mismatch"));
        }
        Ok(outs)
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
    /// Durable point edges remain for archive history; strong bits cleared below.
    pub fn disconnect_tip(&self) -> Result<(), QueryError> {
        let height = self
            .tip_height()
            .ok_or(StoreError::Corrupt("no tip to disconnect"))?;
        let tx_fks = self.block_tx_fks(height)?;

        let mut touched_sh: Vec<[u8; 32]> = Vec::new();
        for &tx_fk in &tx_fks {
            let tx = self.store.get_tx(tx_fk)?;
            // Unlink thin scripthash creates for this block's outputs.
            if tx.output_count > 0 {
                let outputs = self.tx_output_run_class_a(tx_fk, &tx)?;
                for (i, o) in outputs.iter().enumerate() {
                    let sh = script_hash(&o.script);
                    let _ = self.store.scripthash.unlink_create(&sh, tx_fk, i as u32)?;
                    touched_sh.push(sh);
                }
            }
            self.store.strong_tx.set_unstrong(tx_fk)?;
            self.store.tx_height.clear(tx_fk)?;
        }
        // Refresh process heads for unlinked scripts.
        if !touched_sh.is_empty() {
            let mut heads = self.sh_heads.lock().unwrap();
            for sh in touched_sh {
                match self.store.scripthash.head_value(&sh) {
                    Ok(Some(v)) if !v.is_empty() => {
                        heads.insert(sh, v);
                    }
                    _ => {
                        heads.remove(&sh);
                    }
                }
            }
        }
        // Class A header_txs list remains with the header; only tip Class C moves.
        self.store.confirmed.disconnect_tip(height)?;
        // SH watermark tracks confirmed tip (re-confirm will re-enqueue this height).
        self.set_sh_indexed_through_height(self.tip_height().map(|h| h.0));
        Ok(())
    }
}
