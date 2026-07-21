//! Index modes: light UTXO + runs, direct durable tx/spends, tip-follow.
//!
//! Spentness truth:
//! - [`IndexMode::Catchup`]: mmap unspent set (`ibd_utxo.map`)
//! - [`IndexMode::Direct`] / [`IndexMode::Tip`]: durable confirmed-strong spends

use super::*;
use rbitcoin_store::IbdUtxo;
use std::sync::atomic::Ordering;

/// Result of one hysteresis materialize step (one sorted run into a durable index).
#[derive(Clone, Debug)]
pub struct MaterializeRunResult {
    /// Which index: `point`, `tx.head`, or `scripthash`.
    pub store: &'static str,
    /// Keys / edges applied from that run.
    pub keys: u64,
    /// Wall time for claim + apply + remove.
    pub elapsed: std::time::Duration,
}

/// First-class index / spentness mode.
///
/// | Mode | Durable `tx.head` | Durable spends | Light UTXO | Runs |
/// |------|-------------------|----------------|------------|------|
/// | [`Catchup`](IndexMode::Catchup) | off (tx runs) | off (point runs) | on | tx+point+SH |
/// | [`Direct`](IndexMode::Direct) | on (archive batch) | on (confirm batch) | off | SH merge-only → bulk at tip |
/// | [`Tip`](IndexMode::Tip) | on | on (archive path) | off | none (materialized) |
///
/// Open defaults to [`Tip`] until the node calls [`Query::enter_direct_index_mode`]
/// (IBD default) or [`Query::enter_catchup_mode`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum IndexMode {
    /// IBD: sorted runs + light UTXO; durable open-hash heads off.
    Catchup = 0,
    /// IBD: archive writes `tx.head`; confirm batch-writes spend annotations; no UTXO.
    Direct = 1,
    /// Steady-state / Electrum: durable points + `tx.head` (+ SH materialized).
    Tip = 2,
}

impl IndexMode {
    pub fn is_catchup(self) -> bool {
        matches!(self, Self::Catchup)
    }
    pub fn is_direct(self) -> bool {
        matches!(self, Self::Direct)
    }
    pub fn is_tip(self) -> bool {
        matches!(self, Self::Tip)
    }
    /// Spentness uses durable confirmed-strong annotations (not light UTXO).
    pub fn uses_durable_spends(self) -> bool {
        matches!(self, Self::Direct | Self::Tip)
    }
    fn from_u8(v: u8) -> Self {
        match v {
            0 => Self::Catchup,
            1 => Self::Direct,
            _ => Self::Tip,
        }
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
    #[inline]
    pub fn index_mode(&self) -> IndexMode {
        IndexMode::from_u8(self.index_mode_cell.load(Ordering::SeqCst))
    }

    fn set_index_mode(&self, mode: IndexMode) {
        self.index_mode_cell
            .store(mode as u8, Ordering::SeqCst);
    }

    /// Enter catch-up: durable heads off, sorted runs on, light UTXO required.
    ///
    /// Kept for tests / experimental IBD. Production IBD uses
    /// [`Self::enter_direct_index_mode`].
    pub fn enter_catchup_mode(&self) -> Result<(), QueryError> {
        self.set_index_mode(IndexMode::Catchup);
        self.set_spend_index(false);
        self.set_tx_index(false);
        self.enable_index_run_mode()
    }

    /// Enter **direct** IBD: durable `tx.head` on archive, spend annotations on
    /// confirm (batch), no light UTXO, no point/tx runs (SH runs still on).
    ///
    /// On entry: remove any `ibd_utxo.map` and materialize leftover point/tx runs.
    /// Intended for a **fresh** (or already Direct) store — no chain backfill.
    pub fn enter_direct_index_mode(&self) -> Result<(), QueryError> {
        self.set_index_mode(IndexMode::Direct);
        self.set_spend_index(true);
        self.set_tx_index(true);
        // SH still deferred via runs; do not enable point/tx run workers.
        self.sh_run.enable();
        self.drop_ibd_utxo_file()?;
        self.consume_point_and_tx_runs()?;
        Ok(())
    }

    /// Flip durable index flags on for tip-follow (after run materialize / backfill).
    ///
    /// Does not materialize runs — the node `enter_tip_mode` path does that first.
    pub fn enter_tip_index_mode(&self) {
        self.set_index_mode(IndexMode::Tip);
        self.set_spend_index(true);
        self.set_tx_index(true);
    }

    /// Drop process UTXO handle and delete `ibd_utxo.map` if present.
    pub fn drop_ibd_utxo_file(&self) -> Result<(), QueryError> {
        {
            let mut g = self.ibd_utxo.lock().unwrap();
            *g = None;
        }
        let path = self.store.path().join("ibd_utxo.map");
        if path.exists() {
            std::fs::remove_file(&path).map_err(|e| StoreError::io(&path, e))?;
            rbitcoin_log::info!(
                "store: removed light UTXO map {} (direct index mode)",
                path.display()
            );
        }
        Ok(())
    }

    /// Materialize any on-disk point/tx sorted runs into durable indexes.
    ///
    /// Used when switching to [`IndexMode::Direct`] after a prior Catchup run.
    pub fn consume_point_and_tx_runs(&self) -> Result<(), QueryError> {
        // Finalize may no-op worker join if never enabled; still drains disk runs.
        match self.point_run.finalize_and_materialize(&self.store) {
            Ok(n) if n > 0 => {
                rbitcoin_log::info!("store: consumed point runs edges≈{n} into durable spends")
            }
            Ok(_) => {}
            Err(e) => {
                rbitcoin_log::warn!("store: point run consume failed: {e}");
                return Err(e);
            }
        }
        match self.tx_run.finalize_and_materialize(&self.store) {
            Ok(n) if n > 0 => {
                rbitcoin_log::info!("store: consumed tx runs keys≈{n} into tx.head")
            }
            Ok(_) => {}
            Err(e) => {
                rbitcoin_log::warn!("store: tx run consume failed: {e}");
                return Err(e);
            }
        }
        Ok(())
    }

    /// True if outpoint is spent on the **catch-up** oracle ([`IndexMode::Catchup`]).
    ///
    /// Light UTXO unspent set: not present ⇒ spent or never created.
    /// Direct/Tip return false so callers use durable points.
    pub fn catchup_is_spent(&self, txid: &[u8; 32], vout: u32) -> Result<bool, QueryError> {
        if !self.index_mode().is_catchup() {
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
        spends: &[([u8; 32], u32, Fk)],
        creates: &[([u8; 32], u32, Fk)],
        tip: u32,
    ) -> Result<(), QueryError> {
        self.apply_ibd_utxo_run(&[(spends, creates, tip)])
    }

    /// Apply several heights in order (spends then creates per height), **one flush**.
    ///
    /// Each item is `(spends, creates, tip_height)`. Spends are
    /// `(prev_txid, vout, spending_tx_fk)`. Skips heights already ≤ UTXO tip.
    /// Critical for multi-block confirm: H creates must land before H+1 spends.
    ///
    /// When spend-runs are enabled, enqueues durable spend annotations using
    /// create_fk from light UTXO **before** take_spend (confirm-time, like SH).
    pub fn apply_ibd_utxo_run(
        &self,
        steps: &[(
            & [([u8; 32], u32, Fk)],
            & [([u8; 32], u32, Fk)],
            u32,
        )],
    ) -> Result<(), QueryError> {
        if steps.is_empty() {
            return Ok(());
        }
        let mut run_edges: Vec<(Fk, u32, Fk)> = Vec::new();
        let enqueue_runs = self.point_run_enabled();
        {
            let mut g = self.ibd_utxo.lock().unwrap();
            let Some(ref mut u) = *g else {
                return Ok(());
            };
            let t_probe = std::time::Instant::now();
            let mut last_tip = u.tip();
            for &(spends, creates, tip) in steps {
                if let Some(have) = last_tip {
                    if tip <= have {
                        continue;
                    }
                }
                for &(txid, vout, spend_tx_fk) in spends {
                    if enqueue_runs && !spend_tx_fk.is_null() {
                        if let Some(create_fk) = u.get_create_fk(&txid, vout)? {
                            run_edges.push((create_fk, vout, spend_tx_fk));
                        }
                    }
                    let _ = u.take_spend(&txid, vout)?;
                }
                for &(txid, vout, create_fk) in creates {
                    u.insert_create(&txid, vout, create_fk)?;
                }
                // In-map tip only — no msync (rebuildable catch-up cache).
                u.set_tip(Some(tip));
                last_tip = Some(tip);
            }
            ibd_utxo_stats::note_probe_ns(t_probe.elapsed().as_nanos() as u64);
            // Flush counter kept for log shape; always 0 (no msync).
            ibd_utxo_stats::note_flush_ns(0);
        }
        if !run_edges.is_empty() {
            self.enqueue_spend_run_edges(&run_edges);
        }
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
    /// [`IndexMode::Direct`] / [`IndexMode::Tip`] use durable spends — always ready.
    pub fn ensure_spent_oracle_ready(&self) -> Result<(), QueryError> {
        if !self.index_mode().is_catchup() {
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

    /// Flush/compact SH runs and load durable scripthash tables (tip mode).
    ///
    /// Direct: migration-style cold bulk load after full merge.
    /// Catchup: may still have progressive mat residual; uses bulk if empty else append.
    pub fn finalize_sh_runs(&self) -> Result<u64, QueryError> {
        self.sh_run.finalize_and_bulk_materialize(&self.store)
    }

    pub fn finalize_tx_runs(&self) -> Result<u64, QueryError> {
        self.tx_run.finalize_and_materialize(&self.store)
    }

    pub fn finalize_point_runs(&self) -> Result<u64, QueryError> {
        self.point_run.finalize_and_materialize(&self.store)
    }

    /// Drive fetch↔materialize hysteresis for catch-up runs.
    ///
    /// `arch_lead = arch_hwm − tip`. `peer_inflight` = unique getdata in flight.
    /// Materialize starts only when lead hysteresis is active **and** inflight is 0.
    ///
    /// While materializing: defer `msync`/`fdatasync` on table flushes and skip
    /// inter-shard insert pacing. On transition back to fetch mode, flush Class B
    /// index tables so dirty pages land before archive/getdata resume.
    pub fn publish_run_materialize_control(
        &self,
        arch_lead: u32,
        archive_at_tip: bool,
        peer_inflight: u32,
    ) {
        use crate::run_builder_core::run_materialize_control as ctl;
        // Direct: SH runs flush/merge only — never progressive head materialize.
        if self.index_mode().is_direct() {
            let was_mat = ctl::in_materialize_mode();
            ctl::force_fetch_mode(arch_lead, archive_at_tip, peer_inflight);
            rbitcoin_store::set_defer_durable_flush(false);
            if was_mat {
                if let Err(e) = self.store.flush_index_tables() {
                    rbitcoin_log::warn!("store: flush after leaving mat (direct): {e}");
                }
            }
            return;
        }
        let was_mat = ctl::in_materialize_mode();
        ctl::publish(arch_lead, archive_at_tip, peer_inflight);
        let now_mat = ctl::in_materialize_mode();
        rbitcoin_store::set_defer_durable_flush(now_mat);
        if was_mat && !now_mat {
            if let Err(e) = self.store.flush_index_tables() {
                rbitcoin_log::warn!("store: flush index tables after materialize: {e}");
            } else {
                rbitcoin_log::info!(
                    "store: flushed point/tx/scripthash after materialize → fetch (lead={arch_lead})"
                );
            }
        }
    }

    /// On-disk run counts: `(tx, point, scripthash)`.
    pub fn index_run_counts(&self) -> (usize, usize, usize) {
        (
            self.tx_run.on_disk_run_count(),
            self.point_run.on_disk_run_count(),
            self.sh_run.on_disk_run_count(),
        )
    }

    /// Materialize one oldest run (point → tx → SH). `Ok(None)` if nothing to do.
    ///
    /// Direct mode never progressive-mats SH (runs merge only; bulk load at tip).
    pub fn materialize_one_index_run(&self) -> Result<Option<MaterializeRunResult>, QueryError> {
        let direct = self.index_mode().is_direct();
        let t0 = std::time::Instant::now();
        if !direct {
            if let Some(n) = self.point_run.materialize_oldest_run(&self.store)? {
                return Ok(Some(Self::finish_mat_step("point", n, t0)));
            }
        }
        let t0 = std::time::Instant::now();
        if !direct {
            if let Some(n) = self.tx_run.materialize_oldest_run(&self.store)? {
                return Ok(Some(Self::finish_mat_step("tx.head", n, t0)));
            }
        }
        let t0 = std::time::Instant::now();
        // Catchup only: progressive SH mat. Direct waits for tip bulk-load.
        if !direct {
            if let Some(n) = self.sh_run.materialize_oldest_run(&self.store)? {
                return Ok(Some(Self::finish_mat_step("scripthash", n, t0)));
            }
        }
        Ok(None)
    }

    fn finish_mat_step(
        store: &'static str,
        keys: u64,
        t0: std::time::Instant,
    ) -> MaterializeRunResult {
        let elapsed = t0.elapsed();
        crate::run_builder_core::run_materialize_control::note_materialized(keys);
        // Per-store DEBUG is emitted inside each `materialize_oldest_run`.
        MaterializeRunResult {
            store,
            keys,
            elapsed,
        }
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

    /// Confirm-time spend run enqueue: `(create_tx_fk, vout, spending_tx_fk)`.
    pub(crate) fn enqueue_spend_run_edges(&self, edges: &[(Fk, u32, Fk)]) {
        self.point_run.enqueue_batch(edges);
    }
}
