//! Index modes: Direct (IBD live heads/spends) and Tip (steady-state).
//!
//! Spentness truth for both: durable confirmed-strong spend annotations.
//! SH uses append-only target-sized runs + SEAL during Direct; bulk-loads at tip.

use super::*;
use rbitcoin_primitives::{Fk, Height};
use std::sync::atomic::Ordering;

/// Index / spentness mode.
///
/// | Mode | Durable `tx.head` | Durable spends | SH |
/// |------|-------------------|----------------|-----|
/// | [`Direct`](IndexMode::Direct) | archive live | confirm batch after Class C | target-sized runs + SEAL → bulk at tip |
/// | [`Tip`](IndexMode::Tip) | live | archive + connect | durable write-through after bulk |
///
/// Open defaults to [`Tip`] until the node calls [`Query::enter_direct_index_mode`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum IndexMode {
    /// IBD: archive writes `tx.head`; confirm batch-writes spend annotations.
    Direct = 1,
    /// Steady-state / Electrum: durable points + `tx.head` (+ SH materialized).
    Tip = 2,
}

impl IndexMode {
    pub fn is_direct(self) -> bool {
        matches!(self, Self::Direct)
    }
    pub fn is_tip(self) -> bool {
        matches!(self, Self::Tip)
    }
    /// Spentness uses durable confirmed-strong annotations.
    pub fn uses_durable_spends(self) -> bool {
        matches!(self, Self::Direct | Self::Tip)
    }
    fn from_u8(v: u8) -> Self {
        match v {
            1 => Self::Direct,
            // 0 was historical Catchup — treat as Direct (safe for residual stores).
            0 => Self::Direct,
            _ => Self::Tip,
        }
    }
}

impl Query {
    /// Current index / spentness mode ([`IndexMode`]).
    #[inline]
    pub fn index_mode(&self) -> IndexMode {
        IndexMode::from_u8(self.index_mode_cell.load(Ordering::SeqCst))
    }

    fn set_index_mode(&self, mode: IndexMode) {
        self.index_mode_cell
            .store(mode as u8, Ordering::SeqCst);
    }

    /// Enter **direct** IBD: durable `tx.head` on archive, spend annotations on
    /// confirm (batch), SH target-sized runs + SEAL (bulk at tip).
    ///
    /// Best-effort removes leftover `ibd_utxo.map` / point+tx run dirs from old
    /// Catchup datadirs. Re-enqueues Class A creates with `create_fk > SEAL`
    /// through confirmed tip (kill after tip before spill).
    pub fn enter_direct_index_mode(&self) -> Result<(), QueryError> {
        self.set_index_mode(IndexMode::Direct);
        self.set_spend_index(true);
        self.set_tx_index(true);
        self.sh_run.enable();
        self.drop_legacy_catchup_artifacts()?;
        self.rebuild_sh_unsealed_from_class_a()?;
        Ok(())
    }

    /// Flip durable index flags on for tip-follow (after SH bulk materialize).
    pub fn enter_tip_index_mode(&self) {
        self.set_index_mode(IndexMode::Tip);
        self.set_spend_index(true);
        self.set_tx_index(true);
    }

    /// Remove leftover Catchup artifacts (light UTXO map, point/tx run dirs).
    fn drop_legacy_catchup_artifacts(&self) -> Result<(), QueryError> {
        let path = self.store.path().join("ibd_utxo.map");
        if path.exists() {
            std::fs::remove_file(&path).map_err(|e| StoreError::io(&path, e))?;
            rbitcoin_log::info!(
                "store: removed leftover light UTXO map {} (direct index mode)",
                path.display()
            );
        }
        for name in ["point.runs", "tx.runs"] {
            let dir = self.store.path().join(name);
            if dir.is_dir() {
                match std::fs::remove_dir_all(&dir) {
                    Ok(()) => rbitcoin_log::info!(
                        "store: removed leftover catch-up run dir {}",
                        dir.display()
                    ),
                    Err(e) => rbitcoin_log::warn!(
                        "store: could not remove leftover run dir {}: {e}",
                        dir.display()
                    ),
                }
            }
        }
        Ok(())
    }

    /// Drain SH spills and cold bulk-load durable scripthash tables (tip entry).
    ///
    /// Direct IBD: append-only target-sized runs + SEAL. Tip: fan-in reduce + bulk load.
    ///
    /// **`RBITCOIN_SH_FORCE_REBUILD=1`:** wipe SH head/runs/SEAL/HWM, recollect **all**
    /// Class A creates into runs, then full cold materialize (not a catch-up tail).
    pub fn finalize_sh_runs(&self) -> Result<u64, QueryError> {
        self.finalize_sh_runs_cancellable(None)
    }

    /// Like [`Self::finalize_sh_runs`] with cooperative cancel (SIGINT → leave CHECKPOINT).
    pub fn finalize_sh_runs_cancellable(
        &self,
        cancel: Option<&std::sync::atomic::AtomicBool>,
    ) -> Result<u64, QueryError> {
        use crate::sh_builder::{
            plan_sh_pre_materialize, sh_catalog_total_records, sh_force_rebuild,
            ShPreMaterializeAction,
        };

        self.sh_run.refresh_seal();
        let tip_max = self.store.txs.count();
        let seal = self.sh_run.sealed_max_create_fk();
        let run_recs = sh_catalog_total_records(&self.store.path().join("scripthash.runs"));
        let head_durable = self.store.scripthash.has_durable_index();
        let include_hwm = self.store.scripthash.include_hwm();
        let force = sh_force_rebuild();
        let action = plan_sh_pre_materialize(
            force,
            head_durable,
            seal,
            tip_max,
            run_recs,
            include_hwm,
        );

        match action {
            ShPreMaterializeAction::ForceFullRebuild => {
                rbitcoin_log::info!(
                    "node: scripthash FORCE_REBUILD seal={seal} tip_max_create_fk={tip_max} \
                     catalog_recs≈{run_recs} head_durable={head_durable} include_hwm={include_hwm}"
                );
                self.sh_run.prepare_force_full_rebuild(&self.store)?;
            }
            ShPreMaterializeAction::ResetCatalogFullRecollect => {
                // Empty/wiped head + stale high SEAL + tail-only runs (or consumed
                // catalog with no head) would cold-load a tiny incomplete index —
                // recollect Class A from 0 instead.
                rbitcoin_log::warn!(
                    "node: scripthash empty head needs full Class A recollect \
                     (seal={seal} tip_max={tip_max} catalog_recs≈{run_recs})"
                );
                self.sh_run.reset_catalog_for_full_recollect()?;
            }
            ShPreMaterializeAction::BootstrapIncludeHwm { seal: s } => {
                // Legacy durable head without include_hwm: SEAL is the inclusion
                // watermark. Never clamp SEAL→0 (that would re-scan all Class A).
                rbitcoin_log::info!(
                    "node: scripthash durable head missing include_hwm — \
                     bootstrapping from SEAL={s} (no SEAL clamp)"
                );
                self.store.scripthash.note_include_hwm(s)?;
            }
            ShPreMaterializeAction::ClampSealTo { floor } => {
                rbitcoin_log::info!(
                    "node: scripthash durable head include_hwm={floor} < seal={seal} — \
                     clamping SEAL to HWM for gap recollect (warm residual)"
                );
                self.sh_run.set_sealed_max_for_recollect(floor)?;
            }
            ShPreMaterializeAction::Noop => {}
        }

        self.sh_run.refresh_seal();
        self.rebuild_sh_unsealed_from_class_a()?;
        match cancel {
            None => self.sh_run.finalize_and_bulk_materialize(&self.store),
            Some(c) => self
                .sh_run
                .finalize_and_bulk_materialize_cancellable(&self.store, Some(c)),
        }
    }

    /// On-disk scripthash sorted-run count (Direct IBD cache).
    pub fn scripthash_run_count(&self) -> usize {
        self.sh_run.on_disk_run_count()
    }

    /// Re-collect thin SH creates for confirmed txs with `create_fk > SEAL`.
    ///
    /// Covers kill after tip advance while memtable was still unspilled. Work is
    /// O(crash window) when SEAL tracks near tip; full chain only if SEAL=0.
    fn rebuild_sh_unsealed_from_class_a(&self) -> Result<(), QueryError> {
        if !self.sh_run.is_enabled() {
            return Ok(());
        }
        let sealed = self.sh_run.sealed_max_create_fk();
        let Some(tip) = self.store.tip_height() else {
            return Ok(());
        };
        let mut creates = Vec::new();
        let mut n_txs = 0u64;
        for h in 0..=tip.0 {
            let Some(hfk) = self.store.confirmed.get(Height(h))? else {
                continue;
            };
            let Some((first, count)) = self.store.header_txs.get_range(hfk)? else {
                continue;
            };
            if count == 0 || first.is_null() {
                continue;
            }
            let start = first.0;
            let end = start.saturating_add(u64::from(count));
            for fk in start..end {
                if fk <= sealed {
                    continue;
                }
                n_txs = n_txs.saturating_add(1);
                self.collect_scripthash_creates(Fk(fk), &mut creates, None)?;
            }
        }
        if creates.is_empty() {
            return Ok(());
        }
        rbitcoin_log::info!(
            "node: scripthash resume rebuild txs≈{n_txs} creates≈{} seal={sealed} tip={}",
            creates.len(),
            tip.0
        );
        self.sh_run.enqueue(&creates);
        self.sh_run.drain_spills()?;
        self.sh_run.refresh_seal();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sh_builder::{
        load_seal, plan_sh_pre_materialize, store_seal, ShPreMaterializeAction, SH_RUN_KEY_LEN,
        SH_RUN_REC_LEN,
    };
    use rbitcoin_primitives::Fk;
    use rbitcoin_store::{next_run_path, write_sorted_run, ScriptHashRecord};
    use std::time::{SystemTime, UNIX_EPOCH};

    fn encode_rec(sh: &[u8; 32], fk: Fk) -> [u8; 40] {
        let mut buf = [0u8; 40];
        buf[..32].copy_from_slice(sh);
        buf[32..40].copy_from_slice(&fk.0.to_le_bytes());
        buf
    }

    /// Drive [`Query::finalize_sh_runs`] with durable head + high SEAL + no HWM.
    /// Must not zero SEAL or wipe the head (catchup clamp regression).
    #[test]
    fn finalize_sh_runs_durable_head_missing_hwm_keeps_seal() {
        let n = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("rbitcoin-q-sh-hwm-{n}"));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let q = Query::open_or_create(&dir).unwrap();
        q.enter_direct_index_mode().unwrap();

        let mut sh0 = [0u8; 32];
        sh0[0] = 0x44;
        q.sh_run
            .enqueue(&[ScriptHashRecord::from_fk(sh0, Fk(3))]);
        let n0 = q.sh_run.finalize_and_bulk_materialize(&q.store).unwrap();
        assert!(n0 >= 1);
        assert!(q.store.scripthash.has_durable_index());
        let count_before = q.store.scripthash.entry_count();

        // Legacy post-materialize: high SEAL, empty runs, delete include_hwm.
        let runs_dir = dir.join("scripthash.runs");
        std::fs::create_dir_all(&runs_dir).unwrap();
        let high_seal = 1_411_000_000u64;
        store_seal(&runs_dir, high_seal).unwrap();
        q.sh_run.refresh_seal();
        let _ = std::fs::remove_file(dir.join(rbitcoin_store::INCLUDE_HWM_NAME));
        assert_eq!(q.store.scripthash.include_hwm(), 0);
        assert_eq!(q.sh_run.sealed_max_create_fk(), high_seal);

        // No tip txs → rebuild_sh is a no-op; prep must still bootstrap HWM.
        let _ = q.finalize_sh_runs().unwrap();
        assert_eq!(
            q.sh_run.sealed_max_create_fk(),
            high_seal,
            "SEAL must not be clamped to 0 when HWM was missing"
        );
        assert_eq!(
            q.store.scripthash.include_hwm(),
            high_seal,
            "include_hwm must bootstrap from SEAL"
        );
        assert_eq!(
            q.store.scripthash.entries(&sh0).unwrap().len(),
            1,
            "durable head must remain"
        );
        assert!(q.store.scripthash.entry_count() >= count_before);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn finalize_sh_runs_empty_head_stale_tail_resets_seal() {
        let n = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("rbitcoin-q-sh-stale-{n}"));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let q = Query::open_or_create(&dir).unwrap();
        q.enter_direct_index_mode().unwrap();
        assert!(!q.store.scripthash.has_durable_index());

        let runs_dir = dir.join("scripthash.runs");
        std::fs::create_dir_all(&runs_dir).unwrap();
        let high_seal = 1_400_000_000u64;
        store_seal(&runs_dir, high_seal).unwrap();
        q.sh_run.refresh_seal();
        // Tiny catch-up tail run.
        let mut body = Vec::new();
        body.extend_from_slice(&encode_rec(&[0xab; 32], Fk(99)));
        let path = next_run_path(&runs_dir, 1);
        write_sorted_run(&path, SH_RUN_KEY_LEN, SH_RUN_REC_LEN, &body).unwrap();

        assert_eq!(
            plan_sh_pre_materialize(
                false,
                false,
                high_seal,
                high_seal + 1_000_000,
                1,
                0
            ),
            ShPreMaterializeAction::ResetCatalogFullRecollect
        );

        // finalize with empty Class A tip: prep resets SEAL; materialize may apply
        // leftover or clear — SEAL after reset must start at 0 before recollect.
        // Call prep path only via plan (already asserted) + reset helper.
        q.sh_run.reset_catalog_for_full_recollect().unwrap();
        assert_eq!(q.sh_run.sealed_max_create_fk(), 0);
        assert_eq!(load_seal(&runs_dir), 0);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
