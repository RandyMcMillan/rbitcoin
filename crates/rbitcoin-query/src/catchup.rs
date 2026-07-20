//! Catch-up index mode: light UTXO, sorted runs, IndexMode helpers.
//!
//! Spentness truth in [`IndexMode::Catchup`] is the mmap unspent set
//! (`ibd_utxo.map`). Tip mode uses durable points (see [`super::Query::is_outpoint_spent`]).

use super::*;
use rbitcoin_store::IbdUtxo;

/// First-class index / spentness mode for catch-up vs tip-follow.
///
/// | Mode | Durable `tx.head` / points | Spentness truth |
/// |------|----------------------------|-----------------|
/// | [`Catchup`](IndexMode::Catchup) | off (sorted runs) | light mmap UTXO |
/// | [`Tip`](IndexMode::Tip) | on (open-hash) | confirmed-strong points |
///
/// Open defaults to [`Tip`] until the node calls [`Query::enter_catchup_mode`].
/// After catch-up, materialize runs then [`Query::enter_tip_index_mode`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IndexMode {
    /// IBD / catch-up: runs + UTXO; durable open-hash heads off.
    Catchup,
    /// Steady-state / Electrum: durable points + `tx.head`.
    Tip,
}

impl IndexMode {
    pub fn is_catchup(self) -> bool {
        matches!(self, Self::Catchup)
    }
    pub fn is_tip(self) -> bool {
        matches!(self, Self::Tip)
    }
}

impl Query {
    /// Open/create mmap IBD UTXO under the store dir. Aligns with store tip via
    /// replay/rebuild so resume skips a full chain walk when meta matches.
    pub fn enable_ibd_utxo(&self) -> Result<(), QueryError> {
        let mut g = self.ibd_utxo.lock().unwrap();
        if g.is_some() {
            return Ok(());
        }
        let mut u = IbdUtxo::open_or_create(self.store.path())?;
        let store_tip = self.tip_height().map(|h| h.0);
        // Empty live with a non-genesis tip is impossible (every height creates
        // ≥1 coinbase outpoint). Catches the pre-fix bug where multi-block
        // confirm advanced tip meta without inserting creates.
        let empty_poisoned = u.live_count() == 0 && u.tip().is_some();
        match (u.tip(), store_tip, empty_poisoned) {
            (_, _, true) => {
                u.clear()?;
                *g = Some(u);
                drop(g);
                self.rebuild_ibd_utxo_to_tip()?;
                return Ok(());
            }
            (t, s, _) if t == s => {
                // Consistent — ready without rebuild.
            }
            (Some(ut), Some(st), _) if ut < st => {
                *g = Some(u);
                drop(g);
                self.replay_ibd_utxo(ut + 1, st)?;
                return Ok(());
            }
            (None, Some(_), _) => {
                *g = Some(u);
                drop(g);
                self.rebuild_ibd_utxo_to_tip()?;
                return Ok(());
            }
            _ => {
                // meta ahead or mismatch — full rebuild
                u.clear()?;
                *g = Some(u);
                drop(g);
                self.rebuild_ibd_utxo_to_tip()?;
                return Ok(());
            }
        }
        *g = Some(u);
        Ok(())
    }

    pub fn ibd_utxo_enabled(&self) -> bool {
        self.ibd_utxo.lock().unwrap().is_some()
    }

    /// Snapshot for IBD perf: `(enabled, live_count, utxo_tip, rebuilds_this_window)`.
    ///
    /// `rebuilds_this_window` samples-and-resets [`ibd_utxo_stats::REBUILD_COUNT`].
    pub fn ibd_utxo_perf_snapshot(&self) -> (bool, u64, Option<u32>, u64) {
        let rebuilds = ibd_utxo_stats::sample_rebuilds_and_reset();
        let g = self.ibd_utxo.lock().unwrap();
        match *g {
            Some(ref u) => (true, u.live_count(), u.tip(), rebuilds),
            None => (false, 0, None, rebuilds),
        }
    }

    /// Count a confirm-path UTXO heal (apply failed → rebuild).
    pub fn note_ibd_utxo_rebuild(&self) {
        ibd_utxo_stats::note_rebuild();
    }

    /// Current index / spentness mode ([`IndexMode`]).
    ///
    /// Derived from durable spend-index flag: off ⇒ Catchup, on ⇒ Tip.
    #[inline]
    pub fn index_mode(&self) -> IndexMode {
        if self.spend_index_enabled() {
            IndexMode::Tip
        } else {
            IndexMode::Catchup
        }
    }

    /// Enter catch-up: durable heads off, sorted runs on, light UTXO required.
    ///
    /// Node IBD entry point. Prefer this over hand-toggling flags.
    pub fn enter_catchup_mode(&self) -> Result<(), QueryError> {
        self.set_spend_index(false);
        self.set_tx_index(false);
        self.enable_index_run_mode()
    }

    /// Flip durable index flags on for tip-follow (after run materialize / backfill).
    ///
    /// Does not materialize runs — the node `enter_tip_mode` path does that first.
    pub fn enter_tip_index_mode(&self) {
        self.set_spend_index(true);
        self.set_tx_index(true);
    }

    /// True if outpoint is spent on the **catch-up** oracle ([`IndexMode::Catchup`]).
    ///
    /// Light UTXO unspent set: not present ⇒ spent or never created.
    /// Tip mode returns false so callers use durable points.
    pub fn catchup_is_spent(&self, txid: &[u8; 32], vout: u32) -> Result<bool, QueryError> {
        if self.index_mode().is_tip() {
            return Ok(false); // durable path handles spentness
        }
        let g = self.ibd_utxo.lock().unwrap();
        let Some(ref u) = *g else {
            return Err(StoreError::Corrupt(
                "catch-up spentness requires light UTXO; call enter_catchup_mode / enable_ibd_utxo",
            ));
        };
        Ok(!u.contains(txid, vout)?)
    }

    /// After successful Class C: take spends, insert creates (with create fk), commit tip.
    ///
    /// Height-monotonic and idempotent: if `tip` is already reflected, no-op.
    /// Flushes mmap once (single-height / tests). Prefer [`Self::apply_ibd_utxo_run`]
    /// for multi-block confirm batches.
    pub fn apply_ibd_utxo_block(
        &self,
        spends: &[([u8; 32], u32)],
        creates: &[([u8; 32], u32, Fk)],
        tip: u32,
    ) -> Result<(), QueryError> {
        self.apply_ibd_utxo_run(&[(spends, creates, tip)])
    }

    /// Apply several heights in order (spends then creates per height), **one flush**.
    ///
    /// Each item is `(spends, creates, tip_height)`. Skips heights already ≤ UTXO tip.
    /// Critical for multi-block confirm: H creates must land before H+1 spends.
    pub fn apply_ibd_utxo_run(
        &self,
        steps: &[(
            & [([u8; 32], u32)],
            & [([u8; 32], u32, Fk)],
            u32,
        )],
    ) -> Result<(), QueryError> {
        if steps.is_empty() {
            return Ok(());
        }
        let mut g = self.ibd_utxo.lock().unwrap();
        let Some(ref mut u) = *g else {
            return Ok(());
        };
        let mut last_tip = u.tip();
        for &(spends, creates, tip) in steps {
            if let Some(have) = last_tip {
                if tip <= have {
                    continue;
                }
            }
            for &(txid, vout) in spends {
                let _ = u.take_spend(&txid, vout)?;
            }
            for &(txid, vout, create_fk) in creates {
                u.insert_create(&txid, vout, create_fk)?;
            }
            u.set_tip(Some(tip));
            last_tip = Some(tip);
        }
        u.flush()?;
        Ok(())
    }

    /// Unspent outpoint → create Class A fk (light UTXO). `None` if spent/missing
    /// or UTXO disabled.
    pub fn ibd_utxo_create_fk(
        &self,
        txid: &[u8; 32],
        vout: u32,
    ) -> Result<Option<Fk>, QueryError> {
        let g = self.ibd_utxo.lock().unwrap();
        let Some(ref u) = *g else {
            return Ok(None);
        };
        Ok(u.get_create_fk(txid, vout)?)
    }

    /// Rebuild mmap UTXO by replaying confirmed chain (creates then spends per height).
    pub fn rebuild_ibd_utxo_to_tip(&self) -> Result<u64, QueryError> {
        let mut g = self.ibd_utxo.lock().unwrap();
        let u = g.get_or_insert_with(|| {
            IbdUtxo::open_or_create(self.store.path()).expect("ibd utxo open")
        });
        u.clear()?;
        let Some(tip) = self.tip_height() else {
            u.commit_tip(None)?;
            return Ok(0);
        };
        drop(g);
        self.replay_ibd_utxo(0, tip.0)?;
        Ok(self
            .ibd_utxo
            .lock()
            .unwrap()
            .as_ref()
            .map(|u| u.live_count())
            .unwrap_or(0))
    }

    pub(crate) fn replay_ibd_utxo(&self, from_h: u32, to_h: u32) -> Result<(), QueryError> {
        for h in from_h..=to_h {
            let fks = match self.block_tx_fks(Height(h)) {
                Ok(f) => f,
                Err(StoreError::NotFound) => continue,
                Err(e) => return Err(e),
            };
            let mut g = self.ibd_utxo.lock().unwrap();
            let u = g.as_mut().ok_or(StoreError::Corrupt("ibd utxo missing"))?;
            // Per-tx order: spends then creates (same-block chain of custody).
            for fk in fks {
                let tx = self.store.get_tx(fk)?;
                if tx.input_count > 0 {
                    // Packed Class A: key body by create fk (no `tx.head` in catch-up).
                    let inputs = self.tx_input_run_class_a(fk, &tx)?;
                    for inp in &inputs {
                        if inp.is_coinbase() {
                            continue;
                        }
                        let prev = self.resolve_prev_txid(inp)?;
                        if !u.take_spend(&prev, inp.prev_index)? {
                            return Err(StoreError::Corrupt("ibd utxo rebuild take failed"));
                        }
                    }
                }
                for v in 0..tx.output_count {
                    u.insert_create(&tx.txid, v, fk)?;
                }
            }
            u.commit_tip(Some(h))?;
        }
        Ok(())
    }

    /// Hard-block catch-up confirm unless light UTXO is enabled and tip-aligned.
    ///
    /// [`IndexMode::Tip`] uses durable points only — always ready.
    pub fn ensure_spent_oracle_ready(&self) -> Result<(), QueryError> {
        if self.index_mode().is_tip() {
            return Ok(());
        }
        let chain_tip = self.tip_height().map(|h| h.0);
        let g = self.ibd_utxo.lock().unwrap();
        let Some(ref u) = *g else {
            return Err(StoreError::Corrupt(
                "catch-up requires light UTXO; call enter_catchup_mode / enable_ibd_utxo",
            ));
        };
        if u.tip() != chain_tip {
            return Err(StoreError::Corrupt(
                "light UTXO tip not aligned with chain tip; call rebuild_ibd_utxo_to_tip",
            ));
        }
        Ok(())
    }

    /// Rebuild catch-up spentness oracle to tip (light UTXO).
    pub fn rebuild_spent_oracle_to_tip(&self) -> Result<u64, QueryError> {
        self.rebuild_ibd_utxo_to_tip()
    }

    /// Enable SH / tx / point sorted runs + mmap UTXO (flags already off).
    ///
    /// Prefer [`Self::enter_catchup_mode`] which also clears durable index flags.
    pub fn enable_index_run_mode(&self) -> Result<(), QueryError> {
        self.sh_run.enable();
        self.tx_run.enable();
        self.point_run.enable();
        self.enable_ibd_utxo()?;
        Ok(())
    }

    /// Flush/compact SH runs and bulk-load durable scripthash tables (tip mode).
    pub fn finalize_sh_runs(&self) -> Result<u64, QueryError> {
        self.sh_run.finalize_and_materialize(&self.store)
    }

    pub fn finalize_tx_runs(&self) -> Result<u64, QueryError> {
        self.tx_run.finalize_and_materialize(&self.store)
    }

    pub fn finalize_point_runs(&self) -> Result<u64, QueryError> {
        self.point_run.finalize_and_materialize(&self.store)
    }

    /// Drive idle lead-compact of catch-up sorted runs (`tx` / `point` / `SH`).
    ///
    /// Call from the IBD loop with `arch_lead = arch_hwm - tip` and the archive
    /// prep queue depth. When lead is high and the queue is not hot, workers
    /// merge toward one run per family (see `RBITCOIN_RUN_COMPACT_*`).
    pub fn publish_run_compact_pressure(&self, arch_lead: u32, arch_queue: u32) {
        crate::run_builder_core::run_compact_pressure::publish(arch_lead, arch_queue);
    }

    /// On-disk run counts: `(tx, point, scripthash)`.
    pub fn index_run_counts(&self) -> (usize, usize, usize) {
        (
            self.tx_run.on_disk_run_count(),
            self.point_run.on_disk_run_count(),
            self.sh_run.on_disk_run_count(),
        )
    }

    pub(crate) fn tx_run_enabled(&self) -> bool {
        self.tx_run.is_enabled()
    }

    pub(crate) fn point_run_enabled(&self) -> bool {
        self.point_run.is_enabled()
    }

    pub(crate) fn enqueue_tx_run(&self, txid: [u8; 32], fk: Fk) {
        self.tx_run.enqueue(txid, fk);
    }

    pub(crate) fn enqueue_point_run_edges(
        &self,
        edges: &[( [u8; 32], u32, Fk, u32, u32)],
    ) {
        self.point_run.enqueue_batch(edges);
    }
}
