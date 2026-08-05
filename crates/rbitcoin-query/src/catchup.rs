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
    pub fn finalize_sh_runs(&self) -> Result<u64, QueryError> {
        self.finalize_sh_runs_cancellable(None)
    }

    /// Like [`Self::finalize_sh_runs`] with cooperative cancel (SIGINT → leave CHECKPOINT).
    pub fn finalize_sh_runs_cancellable(
        &self,
        cancel: Option<&std::sync::atomic::AtomicBool>,
    ) -> Result<u64, QueryError> {
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
