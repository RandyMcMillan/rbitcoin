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
            ShPreMaterializeAction, SH_SEAL_LAG_OK,
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
        let need_full_recollect = matches!(
            action,
            ShPreMaterializeAction::ForceFullRebuild
                | ShPreMaterializeAction::ResetCatalogFullRecollect
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
                // Ensure run pipeline can accept Class A recollect enqueues.
                self.sh_run.ensure_enabled();
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
        self.rebuild_sh_unsealed_from_class_a_cancellable(cancel)?;

        // Fail closed: never FullCold / "no runs" success on a zeroed head when
        // Class A still has creates above SEAL (FORCE / empty-head full recollect).
        self.sh_run.refresh_seal();
        let seal_after = self.sh_run.sealed_max_create_fk();
        let run_after = sh_catalog_total_records(&self.store.path().join("scripthash.runs"));
        let tip_max = self.store.txs.count();
        let head_durable = self.store.scripthash.has_durable_index();
        if !head_durable
            && tip_max > 0
            && run_after == 0
            && seal_after.saturating_add(SH_SEAL_LAG_OK) < tip_max
        {
            rbitcoin_log::error!(
                "node: scripthash recollect left empty catalog seal={seal_after} \
                 tip_max_create_fk={tip_max} force_or_reset={need_full_recollect} — abort"
            );
            return Err(StoreError::Corrupt(
                "scripthash Class A recollect produced empty catalog while creates remain above SEAL",
            ));
        }

        let n = match cancel {
            None => self.sh_run.finalize_and_bulk_materialize(&self.store)?,
            Some(c) => self
                .sh_run
                .finalize_and_bulk_materialize_cancellable(&self.store, Some(c))?,
        };

        // Second guard: materialize Ok(0) with empty head + Class A present.
        if n == 0
            && !self.store.scripthash.has_durable_index()
            && tip_max > 0
            && seal_after.saturating_add(SH_SEAL_LAG_OK) < tip_max
        {
            return Err(StoreError::Corrupt(
                "scripthash materialize finished empty while Class A creates remain above SEAL",
            ));
        }
        Ok(n)
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
        self.rebuild_sh_unsealed_from_class_a_cancellable(None)
    }

    /// Stream Class A → SH runs for `create_fk > SEAL`, with ~10s status logs,
    /// cooperative cancel, and SEAL-advancing spills (restart continues from SEAL).
    fn rebuild_sh_unsealed_from_class_a_cancellable(
        &self,
        cancel: Option<&std::sync::atomic::AtomicBool>,
    ) -> Result<(), QueryError> {
        use std::time::{Duration, Instant};

        // FORCE prep / prior finalize_wait_join leave the builder disabled — enqueue
        // would silently drop. Always re-enable before recollect.
        self.sh_run.ensure_enabled();

        let sealed0 = self.sh_run.sealed_max_create_fk();
        let Some(tip) = self.store.tip_height() else {
            return Ok(());
        };
        let tip_max = self.store.txs.count();
        if tip_max == 0 {
            return Ok(());
        }

        // Soft enqueue chunks (~10 MiB of 40 B records). Do **not** force-promote
        // each chunk: that wrote ~2.4 MiB catalog runs then compact rewrote the
        // growing catalog every spill (write amp disaster). Promote only when
        // unspilled body reaches recollect spill target (default ~0.75× run target).
        const ENQUEUE_CHUNK: usize = 256_000;
        const STATUS_INTERVAL: Duration = Duration::from_secs(10);
        let spill_target = crate::sh_builder::ShRunBuilder::recollect_spill_target_bytes();

        rbitcoin_log::info!(
            "node: scripthash Class A recollect start seal={sealed0} tip_height={} \
             tip_max_create_fk={tip_max} spill_target_MiB≈{:.0}",
            tip.0,
            spill_target as f64 / (1024.0 * 1024.0)
        );
        let t0 = Instant::now();
        let mut last_status = Instant::now();
        let mut batch: Vec<rbitcoin_store::ScriptHashRecord> =
            Vec::with_capacity(ENQUEUE_CHUNK.min(65_536));
        let mut n_txs = 0u64;
        let mut n_creates = 0u64;
        let mut max_fk = sealed0;
        let mut heights_scanned = 0u64;
        let mut n_spills = 0u64;

        let enqueue_chunk = |batch: &mut Vec<rbitcoin_store::ScriptHashRecord>,
                             sh_run: &crate::sh_builder::ShRunBuilder|
         -> Result<(), QueryError> {
            if batch.is_empty() {
                return Ok(());
            }
            sh_run.enqueue(batch);
            batch.clear();
            Ok(())
        };

        // Promote only when enough unspilled bytes (or force at cancel/end).
        let maybe_spill = |sh_run: &crate::sh_builder::ShRunBuilder,
                           force: bool,
                           n_spills: &mut u64|
         -> Result<(), QueryError> {
            let drained = if force {
                sh_run.drain_spills()?;
                true
            } else {
                sh_run.drain_spills_if_at_least(spill_target)?
            };
            if drained {
                *n_spills = n_spills.saturating_add(1);
                sh_run.refresh_seal();
            }
            Ok(())
        };

        for h in 0..=tip.0 {
            if cancel
                .map(|c| c.load(std::sync::atomic::Ordering::Relaxed))
                .unwrap_or(false)
            {
                enqueue_chunk(&mut batch, &self.sh_run)?;
                maybe_spill(&self.sh_run, true, &mut n_spills)?;
                rbitcoin_log::warn!(
                    "node: scripthash Class A recollect cancelled height={h}/{} \
                     txs≈{n_txs} creates≈{n_creates} seal={} spills={n_spills} elapsed={:?}",
                    tip.0,
                    self.sh_run.sealed_max_create_fk(),
                    t0.elapsed()
                );
                return Err(StoreError::Cancelled("scripthash Class A recollect"));
            }
            heights_scanned = heights_scanned.saturating_add(1);
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
                if fk <= sealed0 {
                    continue;
                }
                n_txs = n_txs.saturating_add(1);
                let before = batch.len();
                self.collect_scripthash_creates(Fk(fk), &mut batch, None)?;
                n_creates = n_creates.saturating_add((batch.len() - before) as u64);
                max_fk = max_fk.max(fk);
                if batch.len() >= ENQUEUE_CHUNK {
                    enqueue_chunk(&mut batch, &self.sh_run)?;
                    maybe_spill(&self.sh_run, false, &mut n_spills)?;
                }
            }
            if last_status.elapsed() >= STATUS_INTERVAL {
                self.sh_run.refresh_seal();
                let seal_now = self.sh_run.sealed_max_create_fk();
                let unspilled = self.sh_run.unspilled_body_bytes();
                let elapsed = t0.elapsed();
                let rate = if elapsed.as_secs_f64() > 0.0 {
                    n_creates as f64 / elapsed.as_secs_f64()
                } else {
                    0.0
                };
                rbitcoin_log::info!(
                    "node: scripthash Class A recollect status height={h}/{} \
                     heights_scanned≈{heights_scanned} txs≈{n_txs} creates≈{n_creates} \
                     max_fk={max_fk} seal={seal_now} tip_max_fk={tip_max} \
                     unspilled_MiB≈{:.1} spills={n_spills} rate≈{:.0} creates/s elapsed={:?}",
                    tip.0,
                    unspilled as f64 / (1024.0 * 1024.0),
                    rate,
                    elapsed
                );
                last_status = Instant::now();
            }
        }
        enqueue_chunk(&mut batch, &self.sh_run)?;
        // Final force-promote remaining memtable/L0 → catalog + SEAL.
        maybe_spill(&self.sh_run, true, &mut n_spills)?;
        self.sh_run.refresh_seal();
        let seal_final = self.sh_run.sealed_max_create_fk();
        if n_txs == 0 && n_creates == 0 {
            rbitcoin_log::info!(
                "node: scripthash Class A recollect done (nothing above seal={sealed0}) \
                 tip_max_fk={tip_max} elapsed={:?}",
                t0.elapsed()
            );
            return Ok(());
        }
        rbitcoin_log::info!(
            "node: scripthash Class A recollect done txs≈{n_txs} creates≈{n_creates} \
             seal={sealed0}→{seal_final} max_fk={max_fk} tip_height={} tip_max_fk={tip_max} \
             spills={n_spills} spill_target_MiB≈{:.0} elapsed={:?}",
            tip.0,
            spill_target as f64 / (1024.0 * 1024.0),
            t0.elapsed()
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sh_builder::{
        load_seal, plan_sh_pre_materialize, sh_catalog_total_records, sh_force_rebuild, store_seal,
        ShPreMaterializeAction, SH_RUN_KEY_LEN, SH_RUN_REC_LEN,
    };
    use rbitcoin_primitives::{Fk, Height};
    use rbitcoin_store::{
        next_run_path, write_sorted_run, HeaderRecord, InputRecord, OutputRecord, ScriptHashRecord,
        TxRecord,
    };
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Mutex;
    use std::time::{SystemTime, UNIX_EPOCH};

    /// Serialize FORCE_REBUILD env mutations (parallel tests share process env).
    static FORCE_ENV_LOCK: Mutex<()> = Mutex::new(());

    fn encode_rec(sh: &[u8; 32], fk: Fk) -> [u8; 40] {
        let mut buf = [0u8; 40];
        buf[..32].copy_from_slice(sh);
        buf[32..40].copy_from_slice(&fk.0.to_le_bytes());
        buf
    }

    fn coinbase_block(h: u32, prev: Fk) -> (HeaderRecord, crate::TxApply) {
        let mut hash = [0u8; 32];
        hash[0..4].copy_from_slice(&h.to_le_bytes());
        hash[4] = 0xcd;
        let header = HeaderRecord {
            prev_fk: prev,
            version: 1,
            timestamp: h + 1,
            bits: 0x207fffff,
            nonce: h,
            merkle_root: hash,
            hash,
        };
        let mut txid = [0u8; 32];
        txid[0..4].copy_from_slice(&h.to_le_bytes());
        txid[31] = 0xcb;
        let ta = crate::TxApply {
            tx: TxRecord {
                txid,
                version: 1,
                locktime: 0,
                input_start_fk: Fk::NULL,
                input_count: 1,
                output_start_fk: Fk::NULL,
                output_count: 1,
            },
            inputs: vec![InputRecord {
                prev_txid: [0u8; 32],
                create_fk: Fk::NULL,
                prev_index: u32::MAX,
                sequence: u32::MAX,
                script_sig: vec![h as u8],
                witness: vec![],
            }],
            outputs: vec![OutputRecord::unspent(50_0000_0000, vec![0x51, h as u8])],
        };
        (header, ta)
    }

    /// Archive + confirm `n` coinbase blocks under Direct mode (Class A + tip height).
    fn seed_direct_chain(q: &Query, n: u32) {
        q.enter_direct_index_mode().unwrap();
        let mut prev = Fk::NULL;
        for h in 0..n {
            let (header, ta) = coinbase_block(h, prev);
            prev = q.connect_block(Height(h), &header, &[ta]).unwrap();
        }
        assert_eq!(q.tip_height(), Some(Height(n - 1)));
        assert!(q.store.txs.count() >= u64::from(n));
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

    /// FORCE prep used to leave the SH builder **disabled**, so Class A recollect
    /// was a silent no-op and tip materialize reported creates≈0 on a zeroed head.
    #[test]
    fn force_rebuild_recollects_class_a_not_empty_materialize() {
        let _g = FORCE_ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let n = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("rbitcoin-q-sh-force-recol-{n}"));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let q = Query::open_or_create(&dir).unwrap();
        seed_direct_chain(&q, 6);
        let tip_max = q.store.txs.count();
        assert!(tip_max >= 6);

        // Ship path: prepare_force disables worker then must re-enable for recollect.
        q.sh_run.prepare_force_full_rebuild(&q.store).unwrap();
        assert!(
            q.sh_run.is_enabled(),
            "FORCE prep must re-enable SH builder for Class A recollect"
        );
        assert!(!q.store.scripthash.has_durable_index());
        assert_eq!(q.sh_run.sealed_max_create_fk(), 0);

        // Recollect alone must produce catalog runs + advance SEAL.
        q.rebuild_sh_unsealed_from_class_a().unwrap();
        q.sh_run.refresh_seal();
        let seal = q.sh_run.sealed_max_create_fk();
        let run_recs = sh_catalog_total_records(&dir.join("scripthash.runs"));
        assert!(
            seal > 0 && run_recs > 0,
            "recollect must spill runs seal={seal} recs={run_recs} tip_max={tip_max}"
        );

        // Full finalize under FORCE_REBUILD env must not Ok empty head.
        std::env::set_var("RBITCOIN_SH_FORCE_REBUILD", "1");
        assert!(sh_force_rebuild());
        let result = q.finalize_sh_runs();
        std::env::remove_var("RBITCOIN_SH_FORCE_REBUILD");
        let n_mat = result.expect("finalize after FORCE must not fail empty");
        assert!(
            n_mat > 0,
            "materialize must load Class A creates, got {n_mat}"
        );
        assert!(
            q.store.scripthash.has_durable_index(),
            "head must not stay empty after FORCE recollect+materialize"
        );
        assert!(q.store.scripthash.entry_count() > 0);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Mid-recollect cancel preserves SEAL; resume continues from watermark.
    #[test]
    fn class_a_recollect_cancel_resume_from_seal() {
        let n = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("rbitcoin-q-sh-recol-resume-{n}"));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let q = Query::open_or_create(&dir).unwrap();
        seed_direct_chain(&q, 12);
        let tip_max = q.store.txs.count();
        let mid = (tip_max / 2).max(1);

        // Plant SEAL mid (as after a partial spill), then cancel on entry.
        q.sh_run.reset_catalog_for_full_recollect().unwrap();
        q.sh_run.ensure_enabled();
        q.sh_run.set_sealed_max_for_recollect(mid).unwrap();
        let cancel = AtomicBool::new(true);
        let err = q
            .rebuild_sh_unsealed_from_class_a_cancellable(Some(&cancel))
            .unwrap_err();
        assert!(
            matches!(err, StoreError::Cancelled(_)),
            "expected Cancelled, got {err}"
        );
        q.sh_run.refresh_seal();
        assert_eq!(
            q.sh_run.sealed_max_create_fk(),
            mid,
            "cancel must not wipe SEAL watermark"
        );

        // Resume without cancel: only create_fk > mid, then materialize.
        cancel.store(false, Ordering::Relaxed);
        q.rebuild_sh_unsealed_from_class_a().unwrap();
        q.sh_run.refresh_seal();
        let seal_done = q.sh_run.sealed_max_create_fk();
        assert!(
            seal_done >= mid,
            "resume must keep/advance SEAL (got {seal_done} mid={mid})"
        );
        let run_recs = sh_catalog_total_records(&dir.join("scripthash.runs"));
        // Creates above mid should have been spilled (unless mid already covered tip).
        if mid + 1 < tip_max {
            assert!(
                run_recs > 0 || seal_done > mid,
                "resume recollect should spill remaining creates"
            );
        }
        let n_mat = q
            .sh_run
            .finalize_and_bulk_materialize(&q.store)
            .unwrap();
        if run_recs > 0 {
            assert!(n_mat > 0, "materialize residual after resume");
            assert!(q.store.scripthash.has_durable_index());
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Empty head + empty catalog + tip creates after recollect must not Ok(0).
    ///
    /// Simulates the historical FORCE bug (builder disabled → recollect no-op) by
    /// forcing an empty catalog while SEAL stays 0 and head is empty, then ensuring
    /// the shipped recollect path (which re-enables) fills the catalog before
    /// materialize — and that finalize under FORCE succeeds with non-zero creates.
    #[test]
    fn force_empty_catalog_guard_and_reenable() {
        let _g = FORCE_ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let n = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("rbitcoin-q-sh-empty-guard-{n}"));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let q = Query::open_or_create(&dir).unwrap();
        seed_direct_chain(&q, 5);
        assert!(q.store.txs.count() >= 5);

        q.sh_run.prepare_force_full_rebuild(&q.store).unwrap();
        assert!(
            q.sh_run.is_enabled(),
            "prepare_force must leave builder enabled"
        );
        assert!(!q.store.scripthash.has_durable_index());

        // If recollect were skipped, catalog stays empty → guard/error or empty mat.
        // Shipped path recollects:
        q.rebuild_sh_unsealed_from_class_a().unwrap();
        let recs = sh_catalog_total_records(&dir.join("scripthash.runs"));
        assert!(recs > 0, "Class A recollect must produce runs (got {recs})");
        assert!(q.sh_run.sealed_max_create_fk() > 0);

        std::env::set_var("RBITCOIN_SH_FORCE_REBUILD", "1");
        let n_mat = q.finalize_sh_runs().expect("FORCE finalize");
        std::env::remove_var("RBITCOIN_SH_FORCE_REBUILD");
        assert!(n_mat > 0, "FORCE must not report creates≈0");
        assert!(q.store.scripthash.entry_count() > 0);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
