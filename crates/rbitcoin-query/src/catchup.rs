//! Index modes: Direct (IBD live heads/spends) and Tip (steady-state).
//!
//! Spentness truth for both: durable confirmed-strong spend annotations.
//! SH uses append-only target-sized runs + SEAL during Direct; bulk-loads at tip.

use super::*;
use rbitcoin_primitives::Fk;
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
    /// Catchup datadirs. Re-collects Class A creates only for a **small** SEAL gap
    /// (crash window). Multi-hour full recollect is tip finalize only — never at
    /// Direct enter (avoids recollect-then-FORCE-wipe loops).
    pub fn enter_direct_index_mode(&self) -> Result<(), QueryError> {
        use crate::sh_builder::{should_defer_direct_recollect, SH_DIRECT_RECOLLECT_MAX_GAP};

        self.set_index_mode(IndexMode::Direct);
        self.set_spend_index(true);
        self.set_tx_index(true);
        self.sh_run.enable();
        self.drop_legacy_catchup_artifacts()?;
        self.sh_run.refresh_seal();
        let seal = self.sh_run.sealed_max_create_fk();
        let tip_max = self.store.txs.count();
        let gap = tip_max.saturating_sub(seal);
        if gap == 0 {
            return Ok(());
        }
        if should_defer_direct_recollect(seal, tip_max) {
            rbitcoin_log::info!(
                "node: scripthash defer Class A recollect to tip finalize \
                 (gap≈{gap} creates > max_direct={SH_DIRECT_RECOLLECT_MAX_GAP}; seal={seal} tip_max={tip_max})"
            );
            return Ok(());
        }
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
            ShPreMaterializeAction::ForceColdFromExistingCatalog => {
                rbitcoin_log::warn!(
                    "node: scripthash FORCE_REBUILD env set but catalog complete \
                     (seal={seal} tip_max={tip_max} catalog_recs≈{run_recs}) — \
                     reinit head only; unset RBITCOIN_SH_FORCE_REBUILD after success"
                );
                self.sh_run.prepare_force_cold_from_catalog(&self.store)?;
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

    /// Parallel Class A → SH runs for `create_fk > SEAL`.
    ///
    /// Work units are fixed **create_fk chunks** (~64k idx entries). One OS thread
    /// per CPU collects independently and spills a sorted catalog run whenever its
    /// local buffer exceeds **128 MiB** (some later merge/compact is expected).
    ///
    /// SEAL advances only over a **contiguous prefix** of completed chunks so
    /// cancel/restart never skips unfinished lower ranges. Status ~every 10s.
    fn rebuild_sh_unsealed_from_class_a_cancellable(
        &self,
        cancel: Option<&std::sync::atomic::AtomicBool>,
    ) -> Result<(), QueryError> {
        use crate::sh_builder::{RECOLLECT_THREAD_SPILL_BYTES, SH_RUN_REC_LEN};
        use rbitcoin_store::{script_hash, ScriptHashRecord};
        use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering as AtomicOrdering};
        use std::sync::Mutex;
        use std::time::{Duration, Instant};

        // Single enable gate for recollect (FORCE/reset prep also re-enable).
        // Parallel catalog writers: never run IBD crumb compact concurrently.
        self.sh_run.set_ibd_catalog_compact(false);
        self.sh_run.ensure_enabled();

        let sealed0 = self.sh_run.sealed_max_create_fk();
        let Some(tip) = self.store.tip_height() else {
            return Ok(());
        };
        let tip_max = self.store.txs.count();
        if tip_max == 0 || sealed0 >= tip_max {
            return Ok(());
        }

        /// Create_fk span per parallel work unit (tx.idx density).
        const CHUNK_FKS: u64 = 64_000;
        const STATUS_INTERVAL: Duration = Duration::from_secs(10);

        let workers = recollect_workers();
        let thread_spill_recs =
            (RECOLLECT_THREAD_SPILL_BYTES / u64::from(SH_RUN_REC_LEN)).max(1) as usize;
        let work_lo = sealed0.saturating_add(1);
        let work_span = tip_max.saturating_sub(sealed0);
        let n_chunks = work_span.div_ceil(CHUNK_FKS).max(1) as usize;

        rbitcoin_log::info!(
            "node: scripthash Class A recollect start seal={sealed0} tip_height={} \
             tip_max_create_fk={tip_max} chunks={n_chunks} chunk_fks={CHUNK_FKS} \
             workers={workers} thread_spill_MiB≈{:.0}",
            tip.0,
            RECOLLECT_THREAD_SPILL_BYTES as f64 / (1024.0 * 1024.0)
        );

        let t0 = Instant::now();
        let next_chunk = AtomicUsize::new(0);
        let n_txs = AtomicU64::new(0);
        let n_creates = AtomicU64::new(0);
        let n_spills = AtomicU64::new(0);
        let max_fk_seen = AtomicU64::new(sealed0);
        let seal_prefix = AtomicUsize::new(0);
        let done_flags = Mutex::new(vec![false; n_chunks]);
        let first_err: Mutex<Option<StoreError>> = Mutex::new(None);
        let stop = AtomicBool::new(false);

        let store = &self.store;
        let sh_run = &self.sh_run;

        std::thread::scope(|scope| {
            // Status heartbeats (same scope as workers; 1s poll, log every 10s).
            scope.spawn(|| {
                let mut last_log = Instant::now();
                loop {
                    if stop.load(AtomicOrdering::Relaxed)
                        || seal_prefix.load(AtomicOrdering::Relaxed) >= n_chunks
                    {
                        break;
                    }
                    std::thread::sleep(Duration::from_millis(250));
                    if last_log.elapsed() < STATUS_INTERVAL {
                        continue;
                    }
                    last_log = Instant::now();
                    let elapsed = t0.elapsed();
                    let creates = n_creates.load(AtomicOrdering::Relaxed);
                    let rate = if elapsed.as_secs_f64() > 0.0 {
                        creates as f64 / elapsed.as_secs_f64()
                    } else {
                        0.0
                    };
                    let prefix = seal_prefix.load(AtomicOrdering::Relaxed);
                    rbitcoin_log::info!(
                        "node: scripthash Class A recollect status \
                         seal_prefix={prefix}/{n_chunks} assigned={} seal={} \
                         txs≈{} creates≈{creates} max_fk={} spills={} workers={workers} \
                         rate≈{:.0} creates/s elapsed={:?}",
                        next_chunk.load(AtomicOrdering::Relaxed).min(n_chunks),
                        sh_run.sealed_max_create_fk(),
                        n_txs.load(AtomicOrdering::Relaxed),
                        max_fk_seen.load(AtomicOrdering::Relaxed),
                        n_spills.load(AtomicOrdering::Relaxed),
                        rate,
                        elapsed
                    );
                }
            });

            for _w in 0..workers {
                scope.spawn(|| {
                    // Hold records across many 64k-fk chunks until ~128 MiB.
                    // Spilling at every chunk end only yields ~5–10 MiB (64k×~2–3 outs).
                    let mut local: Vec<ScriptHashRecord> =
                        Vec::with_capacity(thread_spill_recs.min(1 << 22));
                    // Chunks fully scanned into `local` but not yet durable on disk.
                    let mut pending_done: Vec<usize> = Vec::with_capacity(32);

                    let flush_pending_done = |pending: &mut Vec<usize>| -> Result<(), StoreError> {
                        for chunk_id in pending.drain(..) {
                            mark_recollect_chunk_done(
                                chunk_id,
                                n_chunks,
                                sealed0,
                                tip_max,
                                CHUNK_FKS,
                                &done_flags,
                                &seal_prefix,
                                sh_run,
                            )?;
                        }
                        Ok(())
                    };

                    let spill_and_commit =
                        |local: &mut Vec<ScriptHashRecord>,
                         pending: &mut Vec<usize>|
                         -> Result<(), StoreError> {
                            if !local.is_empty() {
                                spill_local(
                                    sh_run,
                                    local,
                                    &n_spills,
                                    &n_creates,
                                    &max_fk_seen,
                                )?;
                            }
                            flush_pending_done(pending)
                        };

                    loop {
                        if stop.load(AtomicOrdering::Relaxed)
                            || cancel
                                .map(|c| c.load(AtomicOrdering::Relaxed))
                                .unwrap_or(false)
                        {
                            stop.store(true, AtomicOrdering::Relaxed);
                            // Durable progress: spill + commit only fully scanned chunks.
                            let _ = spill_and_commit(&mut local, &mut pending_done);
                            break;
                        }
                        if first_err.lock().unwrap().is_some() {
                            break;
                        }
                        let i = next_chunk.fetch_add(1, AtomicOrdering::Relaxed);
                        if i >= n_chunks {
                            // Worker idle: flush remaining buffer (may be <128 MiB tail).
                            if let Err(e) = spill_and_commit(&mut local, &mut pending_done) {
                                *first_err.lock().unwrap() = Some(e);
                                stop.store(true, AtomicOrdering::Relaxed);
                            }
                            break;
                        }
                        let lo =
                            work_lo.saturating_add((i as u64).saturating_mul(CHUNK_FKS));
                        let hi = lo
                            .saturating_add(CHUNK_FKS)
                            .saturating_sub(1)
                            .min(tip_max);
                        if lo > tip_max {
                            pending_done.push(i);
                            if local.len() >= thread_spill_recs {
                                if let Err(e) = spill_and_commit(&mut local, &mut pending_done)
                                {
                                    *first_err.lock().unwrap() = Some(e);
                                    stop.store(true, AtomicOrdering::Relaxed);
                                    break;
                                }
                            }
                            continue;
                        }

                        let mut chunk_txs = 0u64;
                        let mut chunk_max = sealed0;
                        let mut fk = lo;
                        let mut chunk_ok = true;
                        while fk <= hi {
                            if cancel
                                .map(|c| c.load(AtomicOrdering::Relaxed))
                                .unwrap_or(false)
                                || stop.load(AtomicOrdering::Relaxed)
                            {
                                stop.store(true, AtomicOrdering::Relaxed);
                                chunk_ok = false;
                                break;
                            }
                            match store.get_tx_meta_and_outputs(Fk(fk)) {
                                Ok((_tx, outputs)) => {
                                    chunk_txs = chunk_txs.saturating_add(1);
                                    chunk_max = chunk_max.max(fk);
                                    for o in &outputs {
                                        local.push(ScriptHashRecord::from_fk(
                                            script_hash(&o.script),
                                            Fk(fk),
                                        ));
                                    }
                                    // Spill only at ~128 MiB — not per chunk.
                                    if local.len() >= thread_spill_recs {
                                        // Current chunk not finished → do not put `i` in
                                        // pending yet; only commit earlier full chunks.
                                        if let Err(e) = spill_local(
                                            sh_run,
                                            &mut local,
                                            &n_spills,
                                            &n_creates,
                                            &max_fk_seen,
                                        ) {
                                            *first_err.lock().unwrap() = Some(e);
                                            stop.store(true, AtomicOrdering::Relaxed);
                                            chunk_ok = false;
                                            break;
                                        }
                                        if let Err(e) = flush_pending_done(&mut pending_done) {
                                            *first_err.lock().unwrap() = Some(e);
                                            stop.store(true, AtomicOrdering::Relaxed);
                                            chunk_ok = false;
                                            break;
                                        }
                                    }
                                }
                                Err(StoreError::NotFound) | Err(StoreError::InvalidFk) => {}
                                Err(e) => {
                                    *first_err.lock().unwrap() = Some(e);
                                    stop.store(true, AtomicOrdering::Relaxed);
                                    chunk_ok = false;
                                    break;
                                }
                            }
                            fk = fk.saturating_add(1);
                        }

                        if !chunk_ok {
                            // Partial chunk: spill durable prior chunks only.
                            let _ = spill_and_commit(&mut local, &mut pending_done);
                            break;
                        }

                        n_txs.fetch_add(chunk_txs, AtomicOrdering::Relaxed);
                        max_fk_seen.fetch_max(chunk_max, AtomicOrdering::Relaxed);
                        // Chunk fully scanned into `local` — commit after next spill
                        // (or worker exit spill) so SEAL never covers RAM-only data.
                        pending_done.push(i);
                        if local.len() >= thread_spill_recs {
                            if let Err(e) = spill_and_commit(&mut local, &mut pending_done) {
                                *first_err.lock().unwrap() = Some(e);
                                stop.store(true, AtomicOrdering::Relaxed);
                                break;
                            }
                        }
                    }
                });
            }
        });

        stop.store(true, AtomicOrdering::Relaxed);

        if let Some(e) = first_err.lock().unwrap().take() {
            return Err(e);
        }

        let cancelled = cancel
            .map(|c| c.load(AtomicOrdering::Relaxed))
            .unwrap_or(false);
        let prefix = seal_prefix.load(AtomicOrdering::Relaxed);
        self.sh_run.refresh_seal();
        let seal_final = if prefix >= n_chunks {
            tip_max
        } else {
            sealed0
                .saturating_add((prefix as u64).saturating_mul(CHUNK_FKS))
                .min(tip_max)
        };
        self.sh_run.publish_seal_watermark(seal_final)?;
        self.sh_run.refresh_seal();

        let txs = n_txs.load(AtomicOrdering::Relaxed);
        let creates = n_creates.load(AtomicOrdering::Relaxed);
        let spills = n_spills.load(AtomicOrdering::Relaxed);
        let max_fk = max_fk_seen.load(AtomicOrdering::Relaxed);

        if cancelled && prefix < n_chunks {
            rbitcoin_log::warn!(
                "node: scripthash Class A recollect cancelled \
                 seal_prefix={prefix}/{n_chunks} txs≈{txs} creates≈{creates} \
                 seal={sealed0}→{} spills={spills} elapsed={:?}",
                self.sh_run.sealed_max_create_fk(),
                t0.elapsed()
            );
            return Err(StoreError::Cancelled("scripthash Class A recollect"));
        }

        if txs == 0 && creates == 0 {
            rbitcoin_log::info!(
                "node: scripthash Class A recollect done (nothing above seal={sealed0}) \
                 tip_max_fk={tip_max} elapsed={:?}",
                t0.elapsed()
            );
            return Ok(());
        }
        rbitcoin_log::info!(
            "node: scripthash Class A recollect done txs≈{txs} creates≈{creates} \
             seal={sealed0}→{} max_fk={max_fk} tip_height={} tip_max_fk={tip_max} \
             chunks={n_chunks} workers={workers} spills={spills} elapsed={:?}",
            self.sh_run.sealed_max_create_fk(),
            tip.0,
            t0.elapsed()
        );
        Ok(())
    }
}

fn spill_local(
    sh_run: &crate::sh_builder::ShRunBuilder,
    local: &mut Vec<rbitcoin_store::ScriptHashRecord>,
    n_spills: &std::sync::atomic::AtomicU64,
    n_creates: &std::sync::atomic::AtomicU64,
    max_fk_seen: &std::sync::atomic::AtomicU64,
) -> Result<(), StoreError> {
    use std::sync::atomic::Ordering as AtomicOrdering;
    if local.is_empty() {
        return Ok(());
    }
    let (mfk, n) = sh_run.spill_creates_catalog(local)?;
    local.clear();
    n_spills.fetch_add(1, AtomicOrdering::Relaxed);
    n_creates.fetch_add(n, AtomicOrdering::Relaxed);
    max_fk_seen.fetch_max(mfk, AtomicOrdering::Relaxed);
    Ok(())
}

fn mark_recollect_chunk_done(
    chunk_id: usize,
    n_chunks: usize,
    sealed0: u64,
    tip_max: u64,
    chunk_fks: u64,
    done_flags: &std::sync::Mutex<Vec<bool>>,
    seal_prefix: &std::sync::atomic::AtomicUsize,
    sh_run: &crate::sh_builder::ShRunBuilder,
) -> Result<(), StoreError> {
    use std::sync::atomic::Ordering as AtomicOrdering;
    let mut d = done_flags.lock().unwrap();
    if chunk_id < d.len() {
        d[chunk_id] = true;
    }
    let mut p = seal_prefix.load(AtomicOrdering::Relaxed);
    while p < d.len() && d[p] {
        p += 1;
    }
    seal_prefix.store(p, AtomicOrdering::Relaxed);
    let new_seal = if p >= n_chunks {
        tip_max
    } else {
        sealed0
            .saturating_add((p as u64).saturating_mul(chunk_fks))
            .min(tip_max)
    };
    sh_run.publish_seal_watermark(new_seal)
}

/// Parallel recollect worker count (`RBITCOIN_SH_RECOLLECT_WORKERS`, else CPUs).
fn recollect_workers() -> usize {
    if let Ok(s) = std::env::var("RBITCOIN_SH_RECOLLECT_WORKERS") {
        if let Ok(n) = s.parse::<usize>() {
            return n.clamp(1, 256);
        }
    }
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4)
        .clamp(1, 256)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sh_builder::{
        load_seal, plan_sh_pre_materialize, sh_catalog_looks_complete, sh_catalog_total_records,
        sh_force_rebuild, should_defer_direct_recollect, store_seal, ShPreMaterializeAction,
        SH_DIRECT_RECOLLECT_MAX_GAP, SH_RUN_KEY_LEN, SH_RUN_REC_LEN,
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

    /// Mainnet regression: complete recollect then sticky FORCE must not wipe catalog.
    #[test]
    fn plan_force_with_complete_catalog_does_not_full_wipe() {
        let seal = 1_411_839_527u64;
        let tip = 1_411_887_545u64;
        let recs = 3_741_750_509u64;
        assert!(sh_catalog_looks_complete(seal, tip, recs));
        assert_eq!(
            plan_sh_pre_materialize(true, false, seal, tip, recs, 0),
            ShPreMaterializeAction::ForceColdFromExistingCatalog,
            "sticky FORCE + empty head + complete catalog must not ForceFullRebuild"
        );
        // Tip advanced multi-million past seal during recollect — still cold-load.
        assert_eq!(
            plan_sh_pre_materialize(true, false, seal, seal + 10_000_000, recs, 0),
            ShPreMaterializeAction::ForceColdFromExistingCatalog,
            "usable catalog must survive sticky FORCE even when tip >> seal"
        );
        // Durable tip + sticky FORCE → Noop even if floor lags tip (never wipe head).
        assert_eq!(
            plan_sh_pre_materialize(true, true, seal, seal + 10_000_000, 0, seal),
            ShPreMaterializeAction::Noop
        );
        // Incomplete tail + FORCE still nuclear.
        assert_eq!(
            plan_sh_pre_materialize(true, false, seal, tip, 222_511, 0),
            ShPreMaterializeAction::ForceFullRebuild
        );
    }

    /// End-to-end: recollect fills catalog → FORCE finalize cold-loads without SEAL=0 wipe.
    #[test]
    fn scenario_recollect_then_sticky_force_does_not_redo_class_a() {
        let _g = FORCE_ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let n = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("rbitcoin-q-sh-scenario-force-{n}"));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let q = Query::open_or_create(&dir).unwrap();
        seed_direct_chain(&q, 8);
        let tip_max = q.store.txs.count();

        // Simulate completed recollect: seal at tip, large enough run mass, empty head.
        q.sh_run.reset_catalog_for_full_recollect().unwrap();
        q.rebuild_sh_unsealed_from_class_a().unwrap();
        q.sh_run.refresh_seal();
        let seal = q.sh_run.sealed_max_create_fk();
        let recs = sh_catalog_total_records(&dir.join("scripthash.runs"));
        assert!(seal > 0 && recs > 0);
        assert!(!q.store.scripthash.has_durable_index());

        // Sticky FORCE at tip: must keep catalog, reinit head, materialize.
        std::env::set_var("RBITCOIN_SH_FORCE_REBUILD", "1");
        let n_mat = q.finalize_sh_runs().expect("sticky FORCE finalize");
        std::env::remove_var("RBITCOIN_SH_FORCE_REBUILD");
        assert!(n_mat > 0);
        assert!(q.store.scripthash.has_durable_index());
        // SEAL must not have been zeroed for a full redo.
        q.sh_run.refresh_seal();
        assert!(
            q.sh_run.sealed_max_create_fk() >= seal.min(tip_max),
            "SEAL must survive sticky FORCE when catalog was complete"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Direct enter defers multi-million gaps; small crash-window still recollects.
    #[test]
    fn scenario_direct_enter_defers_large_recollect_gap() {
        // Pure policy (mainnet-scale gaps without building millions of txs).
        assert!(!should_defer_direct_recollect(0, 0));
        assert!(!should_defer_direct_recollect(0, SH_DIRECT_RECOLLECT_MAX_GAP));
        assert!(!should_defer_direct_recollect(100, 100 + SH_DIRECT_RECOLLECT_MAX_GAP));
        assert!(should_defer_direct_recollect(
            0,
            SH_DIRECT_RECOLLECT_MAX_GAP + 1
        ));
        assert!(should_defer_direct_recollect(42_752_000, 1_411_839_527));

        let n = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("rbitcoin-q-sh-scenario-direct-{n}"));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let q = Query::open_or_create(&dir).unwrap();
        seed_direct_chain(&q, 3);
        let tip_max = q.store.txs.count();
        // Small chain: enter_direct recollects (gap ≤ max).
        assert!(!should_defer_direct_recollect(0, tip_max));
        q.sh_run.set_sealed_max_for_recollect(0).unwrap();
        q.enter_direct_index_mode().unwrap();
        q.sh_run.refresh_seal();
        assert!(
            q.sh_run.sealed_max_create_fk() >= tip_max.saturating_sub(1)
                || tip_max == 0,
            "small-gap direct enter must recollect up to tip"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Fresh IBD: direct seed → tip finalize materializes; second pass is stable.
    #[test]
    fn scenario_fresh_ibd_tip_materialize() {
        let n = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("rbitcoin-q-sh-scenario-fresh-{n}"));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let q = Query::open_or_create(&dir).unwrap();
        seed_direct_chain(&q, 5);
        let n_mat = q.finalize_sh_runs().unwrap();
        assert!(
            n_mat > 0 || q.store.scripthash.has_durable_index() || q.scripthash_run_count() == 0,
            "fresh tip finalize should settle SH"
        );
        let seal1 = q.sh_run.sealed_max_create_fk();
        let count1 = q.store.scripthash.entry_count();
        // Second finalize is cheap (durable head, no wipe).
        let _n2 = q.finalize_sh_runs().unwrap();
        assert_eq!(q.sh_run.sealed_max_create_fk(), seal1);
        assert!(
            q.store.scripthash.entry_count() >= count1,
            "repeat finalize must not thrash durable head"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Resumed IBD: partial SEAL + leftover runs, then tip finalize completes.
    #[test]
    fn scenario_resumed_ibd_partial_seal_then_tip() {
        let n = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("rbitcoin-q-sh-scenario-resume-ibd-{n}"));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let q = Query::open_or_create(&dir).unwrap();
        seed_direct_chain(&q, 12);
        let tip_max = q.store.txs.count();
        let mid = (tip_max / 2).max(1);

        // Simulate crash after partial recollect: SEAL at mid, some catalog from first half.
        q.sh_run.reset_catalog_for_full_recollect().unwrap();
        q.sh_run.set_sealed_max_for_recollect(0).unwrap();
        // Recollect only lower half by planting mid SEAL after a full rebuild then
        // resetting catalog is hard; instead: recollect all, then set seal mid + clear
        // would lose data. Better: plant seal=mid with empty runs (crash before spill).
        q.sh_run.set_sealed_max_for_recollect(mid).unwrap();
        assert_eq!(q.sh_run.sealed_max_create_fk(), mid);

        // Restart path: enter_direct (small gap) + tip finalize.
        q.enter_direct_index_mode().unwrap();
        q.sh_run.refresh_seal();
        assert!(
            q.sh_run.sealed_max_create_fk() >= mid,
            "resume must not lower SEAL"
        );
        let n_mat = q.finalize_sh_runs().unwrap();
        assert!(
            q.store.scripthash.has_durable_index() || n_mat > 0 || mid >= tip_max,
            "resumed IBD tip finalize should settle SH"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Crash mid-recollect: SEAL preserved; resume fills remainder; tip materializes.
    #[test]
    fn scenario_crash_resume_recollect_then_tip() {
        let n = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("rbitcoin-q-sh-scenario-crash-{n}"));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let q = Query::open_or_create(&dir).unwrap();
        seed_direct_chain(&q, 10);
        let tip_max = q.store.txs.count();
        q.sh_run.reset_catalog_for_full_recollect().unwrap();
        let mid = (tip_max / 2).max(1);
        q.sh_run.set_sealed_max_for_recollect(mid).unwrap();
        let cancel = AtomicBool::new(true);
        let _ = q.rebuild_sh_unsealed_from_class_a_cancellable(Some(&cancel));
        assert_eq!(q.sh_run.sealed_max_create_fk(), mid);
        // Resume + tip (no FORCE).
        q.rebuild_sh_unsealed_from_class_a().unwrap();
        let seal_after = q.sh_run.sealed_max_create_fk();
        assert!(seal_after >= mid);
        let n_mat = q.finalize_sh_runs().unwrap();
        assert!(
            q.store.scripthash.has_durable_index() || n_mat > 0 || mid >= tip_max,
            "crash resume should finish SH"
        );
        // Third pass after crash-resume must not recollect from 0.
        let seal_final = q.sh_run.sealed_max_create_fk();
        let _ = q.finalize_sh_runs().unwrap();
        assert!(
            q.sh_run.sealed_max_create_fk() >= seal_final,
            "post-settle finalize must not reset SEAL"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Mainnet sequence: recollect complete → sticky FORCE tip → cold load (not wipe)
    /// → durable head → sticky FORCE again is Noop.
    #[test]
    fn scenario_mainnet_recollect_then_force_then_stable() {
        let _g = FORCE_ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let n = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("rbitcoin-q-sh-scenario-mainnet-seq-{n}"));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let q = Query::open_or_create(&dir).unwrap();
        seed_direct_chain(&q, 8);
        let tip_max = q.store.txs.count();

        // Phase 1: full Class A recollect (as enter_direct / pre-tip).
        q.sh_run.reset_catalog_for_full_recollect().unwrap();
        q.rebuild_sh_unsealed_from_class_a().unwrap();
        q.sh_run.refresh_seal();
        let seal1 = q.sh_run.sealed_max_create_fk();
        let recs1 = sh_catalog_total_records(&dir.join("scripthash.runs"));
        assert!(seal1 > 0 && recs1 > 0, "recollect must produce catalog");
        assert!(!q.store.scripthash.has_durable_index());

        // Phase 2: tip finalize with sticky FORCE (mainnet wipe bug).
        std::env::set_var("RBITCOIN_SH_FORCE_REBUILD", "1");
        assert_eq!(
            plan_sh_pre_materialize(true, false, seal1, tip_max, recs1, 0),
            ShPreMaterializeAction::ForceColdFromExistingCatalog
        );
        let n_mat = q.finalize_sh_runs().expect("FORCE finalize cold-load");
        assert!(n_mat > 0);
        assert!(q.store.scripthash.has_durable_index());
        q.sh_run.refresh_seal();
        let seal2 = q.sh_run.sealed_max_create_fk();
        assert!(
            seal2 >= seal1.min(tip_max),
            "SEAL must survive FORCE cold path (got {seal2}, had {seal1})"
        );
        let count_after = q.store.scripthash.entry_count();
        assert!(count_after > 0);

        // Phase 3: sticky FORCE still set on durable head → Noop plan, no wipe.
        assert_eq!(
            plan_sh_pre_materialize(
                true,
                true,
                seal2,
                tip_max,
                0,
                q.store.scripthash.include_hwm().max(seal2)
            ),
            ShPreMaterializeAction::Noop
        );
        let _ = q.finalize_sh_runs().unwrap();
        assert!(q.store.scripthash.has_durable_index());
        assert!(
            q.store.scripthash.entry_count() >= count_after,
            "second FORCE finalize must not wipe durable head"
        );
        assert!(q.sh_run.sealed_max_create_fk() >= seal2.min(tip_max));
        std::env::remove_var("RBITCOIN_SH_FORCE_REBUILD");

        // Phase 4: clean restart finalize (no FORCE) stays stable.
        let seal3 = q.sh_run.sealed_max_create_fk();
        let _ = q.finalize_sh_runs().unwrap();
        assert_eq!(q.sh_run.sealed_max_create_fk(), seal3);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Incomplete FORCE (stale high SEAL + tiny runs) still does nuclear full rebuild.
    #[test]
    fn scenario_force_incomplete_catalog_still_full_rebuild() {
        let _g = FORCE_ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let n = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("rbitcoin-q-sh-scenario-force-stale-{n}"));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let q = Query::open_or_create(&dir).unwrap();
        seed_direct_chain(&q, 6);
        let tip_max = q.store.txs.count();

        // Plant stale high SEAL + tiny run (catch-up tail, not full catalog).
        q.sh_run.reset_catalog_for_full_recollect().unwrap();
        let runs_dir = dir.join("scripthash.runs");
        std::fs::create_dir_all(&runs_dir).unwrap();
        store_seal(&runs_dir, 1_400_000_000).unwrap();
        q.sh_run.refresh_seal();
        let mut body = Vec::new();
        body.extend_from_slice(&encode_rec(&[0xab; 32], Fk(99)));
        write_sorted_run(
            &next_run_path(&runs_dir, 1),
            SH_RUN_KEY_LEN,
            SH_RUN_REC_LEN,
            &body,
        )
        .unwrap();
        let recs = sh_catalog_total_records(&runs_dir);
        assert_eq!(
            plan_sh_pre_materialize(true, false, 1_400_000_000, tip_max.max(1_410_000_000), recs, 0),
            ShPreMaterializeAction::ForceFullRebuild
        );

        std::env::set_var("RBITCOIN_SH_FORCE_REBUILD", "1");
        let n_mat = q.finalize_sh_runs().expect("FORCE incomplete must recollect+materialize");
        std::env::remove_var("RBITCOIN_SH_FORCE_REBUILD");
        assert!(n_mat > 0);
        assert!(q.store.scripthash.has_durable_index());
        // After nuclear path SEAL should track real tip, not the planted 1.4e9.
        q.sh_run.refresh_seal();
        assert!(
            q.sh_run.sealed_max_create_fk() <= tip_max + 1_000,
            "full rebuild SEAL should match Class A tip, not stale plant"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Residual run after cold materialize must warm-apply (never FullCold wipe).
    #[test]
    fn scenario_tip_residual_warm_after_materialize() {
        let n = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("rbitcoin-q-sh-scenario-warm-{n}"));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let q = Query::open_or_create(&dir).unwrap();
        seed_direct_chain(&q, 4);
        q.sh_run.reset_catalog_for_full_recollect().unwrap();
        q.rebuild_sh_unsealed_from_class_a().unwrap();
        let n0 = q.finalize_sh_runs().unwrap();
        assert!(n0 > 0 || q.store.scripthash.has_durable_index());
        let count_before = q.store.scripthash.entry_count();
        let seal_before = q.sh_run.sealed_max_create_fk();
        // Plant residual run.
        let runs_dir = dir.join("scripthash.runs");
        std::fs::create_dir_all(&runs_dir).unwrap();
        let mut body = Vec::new();
        body.extend_from_slice(&encode_rec(&[0xee; 32], Fk(99)));
        write_sorted_run(
            &next_run_path(&runs_dir, 50),
            SH_RUN_KEY_LEN,
            SH_RUN_REC_LEN,
            &body,
        )
        .unwrap();
        let n1 = q.finalize_sh_runs().unwrap();
        assert!(n1 >= 1 || q.store.scripthash.entry_count() >= count_before);
        assert!(
            q.store.scripthash.entry_count() >= count_before,
            "warm residual must not wipe durable head"
        );
        assert!(
            q.sh_run.sealed_max_create_fk() >= seal_before,
            "warm residual must not reset SEAL"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }
}
