//! Tip confirm / connect / disconnect.

use super::*;
use std::sync::atomic::Ordering;

/// One height of SH creates waiting for the Class B appender.
pub(crate) struct ShPendingJob {
    height: Height,
    txs: Vec<(Fk, Option<CreatePin>)>,
}

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
    /// Per block we write `strong_tx`, then **last** advance `confirmed[]` for
    /// the whole run. Scripthash creates are enqueued after that commit and
    /// applied by [`Self::apply_sh_pending`]. The confirmed tip is the commit
    /// point: [`rbitcoin_store::Store::spenders`] /
    /// [`rbitcoin_store::Store::is_confirmed_strong`] only treat a spend as
    /// best-chain once the height fence (confirmed + header_txs) contains the
    /// spender. A hard kill after strong bits but before tip advance leaves
    /// recoverable state — open repairs strong not on the fence, and re-confirm
    /// of tip+1 does not see false PrevoutSpent.
    ///
    /// # Spend annotations
    ///
    /// When `spend_index` is on, durable spend annotations land on create outputs
    /// (schema v5+). Under Direct IBD, **confirm** batch-writes those annotations
    /// after Class C (not archive). Tip mode assumes they are already complete.
    pub fn confirm_blocks_run(&self, items: &[ConfirmPrepared]) -> Result<Vec<Fk>, QueryError> {
        self.confirm_blocks_run_with_create_pins(items, None)
    }

    /// Like [`Self::confirm_blocks_run`], with optional write-batch create pins.
    ///
    /// `create_pins` is `create_fk → CreatePin` for creates committed on this write
    /// path. Pin Arcs are cloned onto the SH write-behind job so collect can
    /// skip Class A body re-read after tip returns.
    pub fn confirm_blocks_run_with_create_pins(
        &self,
        items: &[ConfirmPrepared],
        create_pins: Option<&crate::FkMap<CreatePin>>,
    ) -> Result<Vec<Fk>, QueryError> {
        if items.is_empty() {
            return Ok(Vec::new());
        }
        for w in items.windows(2) {
            if w[1].height.0 != w[0].height.0.saturating_add(1) {
                return Err(StoreError::Corrupt("confirm run not contiguous heights"));
            }
        }

        match self.tip_height() {
            None => {
                if items[0].height != Height::GENESIS {
                    return Err(StoreError::Corrupt("first block must be genesis height"));
                }
            }
            Some(tip) => {
                let expect = tip.next().ok_or(StoreError::Corrupt("height overflow"))?;
                if items[0].height != expect {
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

        for item in items {
            if item.header_fk.is_null() {
                return Err(StoreError::InvalidFk);
            }
        }

        let t_strong = std::time::Instant::now();
        let mut confirmed_pairs = Vec::with_capacity(items.len());
        let mut out = Vec::with_capacity(items.len());
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
                }
            } else {
                for &tx_fk in &item.tx_fks {
                    self.store.strong_tx.set_strong(tx_fk, item.header_fk)?;
                }
            }
            confirmed_pairs.push((item.height, item.header_fk));
            out.push(item.header_fk);
        }
        crate::class_c_phase_stats::STRONG_NS
            .fetch_add(t_strong.elapsed().as_nanos() as u64, Ordering::Relaxed);

        // Fence first: missing header_txs is Corrupt. Publishing confirmed[]
        // before extend would leave tip ahead of height_of (leftover TipOnly hole).
        let t_tip = std::time::Instant::now();
        for item in items {
            self.store
                .height_fence_extend(item.height, item.header_fk)?;
        }
        self.store.confirmed.set_many(&confirmed_pairs)?;
        // Do not forget pending here: drain may still be inserting tx.head
        // (67438). Write forgets after drain.join() *and* this extend.
        // L2 write-behind barrier: complete-or-fail Class C image to disk **before**
        // callers dequeue the body queue. Kill after this returns → tip durable;
        // kill before → BQ still holds blocks for re-drive.
        self.store.flush_class_c_tip()?;
        crate::class_c_phase_stats::TIP_NS
            .fetch_add(t_tip.elapsed().as_nanos() as u64, Ordering::Relaxed);

        self.enqueue_sh_pending(items, create_pins);

        if let Some(tip) = self.tip_height() {
            let _ = self.ensure_height_by_hash_index(tip);
        }

        Ok(out)
    }

    fn enqueue_sh_pending(
        &self,
        items: &[ConfirmPrepared],
        create_pins: Option<&crate::FkMap<CreatePin>>,
    ) {
        if !self.sh_index_enabled() || self.index_mode().is_direct() {
            return;
        }
        let through = self.sh_indexed_through_height();
        let mut jobs = Vec::new();
        for item in items {
            if through.map(|t| item.height.0 <= t).unwrap_or(false) {
                continue;
            }
            let txs = item
                .tx_fks
                .iter()
                .map(|&fk| {
                    let pin = create_pins.and_then(|m| m.get(&fk).cloned());
                    (fk, pin)
                })
                .collect();
            jobs.push(ShPendingJob {
                height: item.height,
                txs,
            });
        }
        if jobs.is_empty() {
            return;
        }
        self.sh_pending.lock().unwrap().extend(jobs);
    }

    /// Apply queued SH write-behind jobs in height order (one Class B appender).
    ///
    /// Production: the tip-follow worker. Tests / [`Self::connect_block`]: drain
    /// so SH history is visible immediately after the fixture connect.
    pub fn apply_sh_pending(&self) -> Result<(), QueryError> {
        loop {
            let job = self.sh_pending.lock().unwrap().pop_front();
            let Some(job) = job else {
                return Ok(());
            };
            self.apply_sh_job(job)?;
        }
    }

    fn apply_sh_job(&self, job: ShPendingJob) -> Result<(), QueryError> {
        use crate::class_c_phase_stats::{self as sh_stats, add_sh_part};

        if !self.sh_index_enabled() || self.index_mode().is_direct() {
            return Ok(());
        }
        if self
            .sh_indexed_through_height()
            .is_some_and(|t| job.height.0 <= t)
        {
            return Ok(());
        }
        if self.store.confirmed.get(job.height)?.is_none() {
            return Ok(());
        }

        let t_collect = std::time::Instant::now();
        let mut sh_creates: Vec<ScriptHashRecord> = Vec::new();
        sh_creates.reserve(job.txs.len().saturating_mul(2));
        for (tx_fk, pin) in &job.txs {
            self.collect_scripthash_creates(*tx_fk, &mut sh_creates, pin.as_ref())?;
        }
        add_sh_part(
            &sh_stats::SH_COLLECT_NS,
            t_collect.elapsed().as_nanos() as u64,
        );

        if !sh_creates.is_empty() {
            sh_stats::SH_CREATE_N.fetch_add(sh_creates.len() as u64, Ordering::Relaxed);
            let mut uniq = std::collections::HashSet::with_capacity(sh_creates.len());
            for r in &sh_creates {
                uniq.insert(r.scripthash);
            }
            sh_stats::SH_UNIQUE_N.fetch_add(uniq.len() as u64, Ordering::Relaxed);
        }

        let mut tip_sh_max_fk = 0u64;
        if !sh_creates.is_empty() {
            for r in &sh_creates {
                tip_sh_max_fk = tip_sh_max_fk.max(r.create_tx_fk.0);
            }
            let mut heads = self.sh_heads.lock().unwrap();
            let (n, timing) = self
                .store
                .scripthash
                .put_create_batch_append(&sh_creates, &mut heads)?;
            sh_stats::SH_WRITTEN_N.fetch_add(n as u64, Ordering::Relaxed);
            add_sh_part(&sh_stats::SH_SORT_NS, timing.sort_ns);
            add_sh_part(&sh_stats::SH_SEED_NS, timing.seed_ns);
            add_sh_part(&sh_stats::SH_BODY_NS, timing.body_ns);
            add_sh_part(&sh_stats::SH_HEAD_NS, timing.head_ns);
        }

        self.set_sh_indexed_through_height(Some(job.height.0));
        if tip_sh_max_fk > 0 {
            let _ = self.store.scripthash.note_include_hwm(tip_sh_max_fk);
            let _ = self.sh_run.publish_seal_watermark(tip_sh_max_fk);
        }
        Ok(())
    }

    fn drop_sh_pending_from(&self, height: Height) {
        self.sh_pending
            .lock()
            .unwrap()
            .retain(|job| job.height.0 < height.0);
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
    /// Source order (first hit wins):
    /// 1. **Write-batch CreatePin** — outs already on the confirm write path
    /// 2. **Cold store** — Class A body pread/decode
    ///
    /// Same-batch creates must hit (1) or SH collect re-reads every body.
    /// `write_pin` is the pin Arc for this `tx_fk` when the write path has it.
    pub(crate) fn collect_scripthash_creates(
        &self,
        tx_fk: Fk,
        out: &mut Vec<ScriptHashRecord>,
        write_pin: Option<&CreatePin>,
    ) -> Result<(), QueryError> {
        use std::sync::atomic::Ordering;
        if let Some(pin) = write_pin {
            let (_tx, outputs) = pin.as_ref();
            for o in outputs.iter() {
                out.push(ScriptHashRecord::from_fk(script_hash(&o.script), tx_fk));
            }
            crate::class_c_phase_stats::SH_COLLECT_PIN.fetch_add(1, Ordering::Relaxed);
            return Ok(());
        }
        let tx = self.get_tx_class_a(tx_fk)?;
        if tx.output_count == 0 {
            crate::class_c_phase_stats::SH_COLLECT_COLD.fetch_add(1, Ordering::Relaxed);
            return Ok(());
        }
        let outputs = self.tx_output_run_class_a(tx_fk, &tx)?;
        for o in outputs.iter() {
            out.push(ScriptHashRecord::from_fk(script_hash(&o.script), tx_fk));
        }
        crate::class_c_phase_stats::SH_COLLECT_COLD.fetch_add(1, Ordering::Relaxed);
        Ok(())
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
        let (_, _, outs) = self.store.get_tx_full(create_fk)?;
        if outs.len() as u32 != tx.output_count {
            return Err(StoreError::Corrupt("packed output count mismatch"));
        }
        Ok(outs)
    }

    /// Connect a block at `height` (genesis or tip+1): Class A then confirm Class C.
    ///
    /// Cheap store fixture (HeaderRecord + TxApply). Not `confirm_wire_run`.
    /// Drains SH write-behind so fixture reads see creates immediately.
    pub fn connect_block(
        &self,
        height: Height,
        header: &HeaderRecord,
        txs: &[TxApply],
    ) -> Result<Fk, QueryError> {
        self.commit_class_a_only(header, txs)?;
        let fk = self.confirm_block(height, &header.hash)?;
        self.apply_sh_pending()?;
        Ok(fk)
    }

    /// Disconnect the current tip (Class C + scripthash create unlink; archive remains).
    ///
    /// Durable point edges remain for archive history; strong bits cleared below.
    ///
    /// # Crash order (**tip first** — opposite of connect)
    ///
    /// 1. SH unlink (index only; filtered by strong+tip at query time).
    /// 2. `confirmed` truncate in RAM → **`flush_confirmed_only`** (durable tip shrink).
    /// 3. Then `set_unstrong` for disconnected txs → flush strong.
    ///
    /// Unstrong-before-tip would make tip txs fail `is_confirmed_strong` while
    /// tip is still high (permanent if kill). Mid-kill after tip shrink leaves
    /// strong **not on the new fence** → `repair_class_c_above_tip` heals.
    ///
    /// Every successful tip shrink logs [`format_disconnect_tip_line`] at **warn**.
    pub fn disconnect_tip(&self) -> Result<(), QueryError> {
        let height = self
            .tip_height()
            .ok_or(StoreError::Corrupt("no tip to disconnect"))?;
        self.drop_sh_pending_from(height);
        let hash = self
            .header_at_height(height)?
            .map(|(_, rec)| rec.hash)
            .unwrap_or([0u8; 32]);
        let tx_fks = self.block_tx_fks(height)?;

        let mut touched_sh: Vec<[u8; 32]> = Vec::new();
        for &tx_fk in &tx_fks {
            let tx = self.store.get_tx(tx_fk)?;
            if tx.output_count > 0 {
                let outputs = self.tx_output_run_class_a(tx_fk, &tx)?;
                for (i, o) in outputs.iter().enumerate() {
                    let sh = script_hash(&o.script);
                    let _ = self.store.scripthash.unlink_create(&sh, tx_fk, i as u32)?;
                    touched_sh.push(sh);
                }
            }
        }
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

        self.store.confirmed.disconnect_tip(height)?;
        self.store.height_fence_pop_tip(height);
        self.note_disconnect_height(height.0);
        let _ = self.on_load_pack();
        self.store.flush_confirmed_only()?;
        log_disconnect_tip(height.0, &hash, tx_fks.len());
        if let Some(new_tip) = self.tip_height() {
            let _ = self.ensure_height_by_hash_index(new_tip);
        } else {
            self.invalidate_height_by_hash_index();
        }

        for &tx_fk in &tx_fks {
            self.store.strong_tx.set_unstrong(tx_fk)?;
        }
        self.store.flush_class_c_after_disconnect_tip()?;

        if self.sh_indexed_through_height() == Some(height.0) {
            self.set_sh_indexed_through_height(self.tip_height().map(|h| h.0));
        }
        self.truncate_sp_tweaks_through_tip(self.tip_height())?;
        Ok(())
    }
}

/// Operator line for one confirmed-block disconnect (reorg / restore).
///
/// Hash is Core display-order hex (`BlockHash` `Display`).
pub fn format_disconnect_tip_line(height: u32, hash: &[u8; 32], n_tx: usize) -> String {
    let hash = BlockHash::from_byte_array(*hash);
    format!("DisconnectTip: hash={hash} height={height} tx={n_tx}")
}

fn log_disconnect_tip(height: u32, hash: &[u8; 32], n_tx: usize) {
    rbitcoin_log::warn!("{}", format_disconnect_tip_line(height, hash, n_tx));
}
