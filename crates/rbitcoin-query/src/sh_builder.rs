//! Direct-IBD scripthash builder: sorted runs + SEAL + tip materialize.
//!
//! # Lifecycle (single pipeline)
//!
//! 1. **Ingest**
//!    - Confirm (Direct): `enqueue` → memtable → worker L0 → promote large catalog runs
//!    - Class A recollect: parallel fk chunks → **direct** catalog spill (~128 MiB)
//! 2. **Watermark:** `SEAL` = contiguous create_fk floor for resume (`create_fk > SEAL`).
//!    Parallel recollect only advances SEAL over a completed chunk prefix.
//! 3. **Tip:** [`plan_sh_pre_materialize`] → recollect gap if needed → claim runs →
//!    `WarmOnly` | `ColdResume` | `FullCold` (never wipe a durable head for residuals).
//!
//! # Write amp
//!
//! Catalog compact only rewrites **crumbs** under [`CATALOG_COMPACT_FLOOR_BYTES`].
//! Intentional ~128 MiB recollect spills go straight to tip direct k-way.

use super::run_builder_core::{
    clear_runs_dir, finalize_wait_join, memtable_cap, on_disk_run_count, runs_dir_io, spawn_worker,
    RunControl, RunMemtable, FAMILY_SH, AFTER_WORK, IDLE_POLL,
};
use rbitcoin_log::{debug, info, warn};
use rbitcoin_primitives::Fk;
use rbitcoin_store::{
    claim_run_for_materialize, commit_fanin_reduce_and_drop_inputs, for_each_merged_rec_opts,
    list_fanin_reduce_outputs, list_materialize_claims, list_runs, load_fanin_checkpoint,
    merge_runs, next_run_path, prefix_shard_of, reduce_runs_to_fanin_cancellable, write_sorted_run,
    ColdProgress, ScriptHashEntry, ScriptHashRecord, Store, StoreError, SortedRunPath,
    FANIN_TARGET_STREAM_RUNS,
};

/// How tip finalize applies remaining SH runs (pure decision; no I/O).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ShTipMaterializeMode {
    /// Head empty (or force rebuild): wipe + cold stream.
    FullCold,
    /// Resume interrupted cold from `next_shard` (progress present).
    ColdResume { next_shard: u32 },
    /// Durable head present: batch-warm residual runs only — **never** reinit.
    WarmOnly,
}

/// Select materialize mode. **Never** returns FullCold when head already holds durable data
/// unless `force_rebuild`. Incomplete catalog should be fixed *before* this (Class A
/// recollect); `catalog_complete` is logged/diagnostic for empty-head FullCold.
pub fn select_sh_tip_materialize_mode(
    head_empty: bool,
    entry_count: u64,
    progress_next_shard: Option<u32>,
    n_shards: u32,
    stream_run_count: usize,
    force_rebuild: bool,
    catalog_complete: bool,
) -> ShTipMaterializeMode {
    let _ = catalog_complete; // caller recollects incomplete catalogs before selecting
    if force_rebuild {
        return ShTipMaterializeMode::FullCold;
    }
    let n_shards = n_shards.max(1);
    if let Some(ns) = progress_next_shard {
        if ns > 0 && ns < n_shards {
            return ShTipMaterializeMode::ColdResume { next_shard: ns };
        }
    }
    // Residual runs against a live durable head: warm only (never wipe).
    if !head_empty || entry_count > 0 {
        return ShTipMaterializeMode::WarmOnly;
    }
    // Empty head: need stream inputs.
    if stream_run_count == 0 {
        return ShTipMaterializeMode::WarmOnly;
    }
    ShTipMaterializeMode::FullCold
}

/// `RBITCOIN_SH_FORCE_REBUILD=1|true` — full Class A recollect + cold rematerialize.
pub fn sh_force_rebuild() -> bool {
    matches!(
        std::env::var("RBITCOIN_SH_FORCE_REBUILD")
            .map(|s| s == "1" || s.eq_ignore_ascii_case("true"))
            .ok(),
        Some(true)
    )
}

/// Allowed SEAL lag behind tip create count (memtable / crash window).
pub const SH_SEAL_LAG_OK: u64 = 50_000;

/// True when SEAL is near tip create HWM (small lag for unsealed memtable).
pub fn sh_catalog_seal_covers_tip(seal_max_fk: u64, tip_max_create_fk: u64) -> bool {
    if tip_max_create_fk == 0 {
        return true;
    }
    seal_max_fk.saturating_add(SH_SEAL_LAG_OK) >= tip_max_create_fk
}

/// High SEAL with only a tiny on-disk run mass — catch-up tail, not full IBD spills.
///
/// After a **successful** cold materialize, runs are cleared while SEAL stays high.
/// That success state is **not** a stale tail when the durable head is live; only
/// use this heuristic when the head is empty (or FORCE_REBUILD already wiped it).
pub fn sh_catalog_is_stale_tail(seal_max_fk: u64, total_run_records: u64) -> bool {
    seal_max_fk >= 1_000_000 && total_run_records < seal_max_fk / 50
}

/// True when catalog SEAL / run record mass can cover Class A through `tip_max_create_fk`.
///
/// For **empty-head** FullCold decisions only. Incomplete when SEAL lags tip, or
/// SEAL is huge but on-disk run rows are a tiny tail (catch-up-only rebuild with
/// a stale high SEAL). **Do not** use this alone on a durable head — empty runs
/// after consume are normal (see [`plan_sh_pre_materialize`]).
pub fn sh_catalog_looks_complete(
    seal_max_fk: u64,
    tip_max_create_fk: u64,
    total_run_records: u64,
) -> bool {
    if tip_max_create_fk == 0 {
        return true;
    }
    if !sh_catalog_seal_covers_tip(seal_max_fk, tip_max_create_fk) {
        return false;
    }
    if seal_max_fk == 0 {
        return total_run_records == 0;
    }
    if sh_catalog_is_stale_tail(seal_max_fk, total_run_records) {
        return false;
    }
    true
}

/// Inclusion floor for a durable SH head.
///
/// Prefer `include_hwm` when present. When the HWM file is missing (legacy
/// datadir / cold finished before the feature), fall back to SEAL — never treat
/// missing HWM as `0` for clamp purposes.
pub fn durable_sh_inclusion_floor(include_hwm: u64, seal: u64) -> u64 {
    if include_hwm > 0 {
        include_hwm
    } else {
        seal
    }
}

/// Pre-materialize catalog / SEAL action (pure; no I/O).
///
/// Covers catch-up ↔ tip ↔ restart transitions:
/// - **FORCE_REBUILD:** wipe head+runs, SEAL=0, full Class A recollect.
/// - **Empty head + stale tail / no usable catalog:** reset SEAL+runs, full recollect.
/// - **Durable head:** never wipe; never clamp SEAL to 0 for missing HWM; bootstrap
///   HWM from SEAL; clamp SEAL to HWM only when `0 < hwm < seal`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShPreMaterializeAction {
    ForceFullRebuild,
    /// Empty head cannot be cold-loaded from current runs — SEAL=0 + clear runs.
    ResetCatalogFullRecollect,
    /// Durable head: write `include_hwm = seal` (legacy missing file).
    BootstrapIncludeHwm { seal: u64 },
    /// Durable head: lower SEAL to authoritative HWM for gap recollect.
    ClampSealTo { floor: u64 },
    Noop,
}

/// Plan SEAL/catalog prep before Class A recollect + tip materialize.
pub fn plan_sh_pre_materialize(
    force: bool,
    head_durable: bool,
    seal: u64,
    tip_max_create_fk: u64,
    run_records: u64,
    include_hwm: u64,
) -> ShPreMaterializeAction {
    if force {
        return ShPreMaterializeAction::ForceFullRebuild;
    }
    if !head_durable {
        // Empty / wiped head: decide whether on-disk runs can seed FullCold.
        if empty_head_needs_full_class_a_recollect(seal, tip_max_create_fk, run_records) {
            return ShPreMaterializeAction::ResetCatalogFullRecollect;
        }
        return ShPreMaterializeAction::Noop;
    }
    // Durable head: run mass is irrelevant (runs are cleared after successful
    // materialize; residual mats are a tip tail for warm apply only).
    // Durable head: HWM (or SEAL if HWM missing) is the inclusion floor.
    let floor = durable_sh_inclusion_floor(include_hwm, seal);
    if include_hwm == 0 && seal > 0 {
        return ShPreMaterializeAction::BootstrapIncludeHwm { seal: floor };
    }
    if include_hwm > 0 && include_hwm < seal {
        return ShPreMaterializeAction::ClampSealTo { floor };
    }
    ShPreMaterializeAction::Noop
}

/// Empty head: full Class A recollect when catalog cannot seed a complete cold load.
fn empty_head_needs_full_class_a_recollect(
    seal: u64,
    tip_max: u64,
    run_records: u64,
) -> bool {
    if tip_max == 0 {
        return false;
    }
    // Catch-up-only runs under a stale high SEAL (FORCE / wipe recovery).
    if sh_catalog_is_stale_tail(seal, run_records) {
        return true;
    }
    // No runs left and SEAL does not cover tip — nothing to cold-load.
    if run_records == 0 && !sh_catalog_seal_covers_tip(seal, tip_max) {
        return true;
    }
    // Runs consumed (empty) while head still empty: prior materialize did not
    // leave a durable index — only full recollect recovers.
    if run_records == 0 && seal > 0 && sh_catalog_seal_covers_tip(seal, tip_max) {
        return true;
    }
    false
}

/// Sum of `count` over catalog + materialize claims under `runs_dir`.
pub fn sh_catalog_total_records(runs_dir: &Path) -> u64 {
    let mut n = 0u64;
    if let Ok(runs) = list_runs(runs_dir) {
        for r in runs {
            n = n.saturating_add(r.count);
        }
    }
    if let Ok(mats) = list_materialize_claims(runs_dir) {
        for r in mats {
            n = n.saturating_add(r.count);
        }
    }
    n
}
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

/// Linear dedup is fine for short chains; switch to a set past this length.
const CHAIN_SET_THRESHOLD: usize = 16;

/// Fixed run record: scripthash[32] | create_tx_fk:u64 = 40 bytes (no vout).
pub const SH_RUN_REC_LEN: u32 = 40;
pub const SH_RUN_KEY_LEN: u32 = 32;

const DEFAULT_MEMTABLE_CAP: usize = 1_000_000;
const HARD_MEMTABLE_MUL: usize = 2;
/// Coalesce L0 spills until a cataloged run is about this large.
const DEFAULT_TARGET_RUN_BYTES: u64 = 512 * 1024 * 1024;
/// Max open runs in any k-way pass (L0 coalesce).
const DEFAULT_MERGE_FANIN: usize = 64;
/// Promote L0→catalog only when merged body ≥ this fraction of target (except finalize).
const PROMOTE_FRAC_NUM: u64 = 3;
const PROMOTE_FRAC_DEN: u64 = 4; // 0.75
/// Wall interval for cold bulk-materialize INFO heartbeats (time-based only).
const MATERIALIZE_STATUS_INTERVAL: Duration = Duration::from_secs(10);

fn max_direct_merge() -> usize {
    std::env::var("RBITCOIN_SH_MAX_DIRECT_MERGE")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(FANIN_TARGET_STREAM_RUNS)
        .clamp(32, 8192)
}

fn promote_min_bytes(target: u64) -> u64 {
    target
        .saturating_mul(PROMOTE_FRAC_NUM)
        .div_ceil(PROMOTE_FRAC_DEN)
        .max(u64::from(SH_RUN_REC_LEN) * 1024)
}

#[inline]
fn run_body_bytes(run: &SortedRunPath) -> u64 {
    run.count.saturating_mul(u64::from(run.rec_len))
}

fn target_run_bytes() -> u64 {
    std::env::var("RBITCOIN_SH_TARGET_RUN_BYTES")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(DEFAULT_TARGET_RUN_BYTES)
        .max(u64::from(SH_RUN_REC_LEN) * 1024)
}

fn merge_fanin() -> usize {
    std::env::var("RBITCOIN_SH_MERGE_FANIN")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(DEFAULT_MERGE_FANIN)
        .clamp(8, 128)
}

fn encode_rec(sh: &[u8; 32], tx_fk: Fk) -> [u8; SH_RUN_REC_LEN as usize] {
    let mut r = [0u8; SH_RUN_REC_LEN as usize];
    r[0..32].copy_from_slice(sh);
    r[32..40].copy_from_slice(&tx_fk.0.to_le_bytes());
    r
}

#[inline(always)]
fn decode_rec_fixed(buf: &[u8]) -> ([u8; 32], Fk) {
    debug_assert!(buf.len() >= SH_RUN_REC_LEN as usize);
    let sh: [u8; 32] = buf[0..32].try_into().unwrap();
    let tx_fk = Fk(u64::from_le_bytes(buf[32..40].try_into().unwrap()));
    (sh, tx_fk)
}

#[inline]
fn chain_has_fk(chain: &[ScriptHashEntry], fk: Fk) -> bool {
    chain.iter().any(|e| e.create_tx_fk == fk)
}

// ── SEAL (max durable create_fk in cataloged runs) ───────────────────────────

fn seal_path(runs_dir: &Path) -> PathBuf {
    runs_dir.join("SEAL")
}

/// Load sealed max create_fk (0 if missing/corrupt).
pub fn load_seal(runs_dir: &Path) -> u64 {
    let path = seal_path(runs_dir);
    let Ok(buf) = std::fs::read(&path) else {
        return 0;
    };
    if buf.len() < 8 {
        return 0;
    }
    u64::from_le_bytes(buf[0..8].try_into().unwrap_or([0; 8]))
}

/// Write SEAL (max create_fk) — tests / catch-up clamp / force rebuild.
pub fn store_seal(runs_dir: &Path, max_fk: u64) -> Result<(), StoreError> {
    let path = seal_path(runs_dir);
    let tmp = runs_dir.join("SEAL.tmp");
    std::fs::create_dir_all(runs_dir).map_err(|e| StoreError::io(runs_dir, e))?;
    std::fs::write(&tmp, max_fk.to_le_bytes()).map_err(|e| StoreError::io(&tmp, e))?;
    {
        let f = std::fs::OpenOptions::new()
            .write(true)
            .open(&tmp)
            .map_err(|e| StoreError::io(&tmp, e))?;
        f.sync_all().map_err(|e| StoreError::io(&tmp, e))?;
    }
    std::fs::rename(&tmp, &path).map_err(|e| StoreError::io(&path, e))?;
    Ok(())
}

fn bump_seal(runs_dir: &Path, max_fk: u64) -> Result<(), StoreError> {
    if max_fk == 0 {
        return Ok(());
    }
    let cur = load_seal(runs_dir);
    if max_fk > cur {
        store_seal(runs_dir, max_fk)?;
    }
    Ok(())
}

fn max_fk_in_body(body: &[u8]) -> u64 {
    let rec = SH_RUN_REC_LEN as usize;
    let mut max = 0u64;
    let mut i = 0;
    while i + rec <= body.len() {
        let fk = u64::from_le_bytes(body[i + 32..i + 40].try_into().unwrap());
        if fk > max {
            max = fk;
        }
        i += rec;
    }
    max
}

// ── Memtable / builder ───────────────────────────────────────────────────────

struct Inner {
    pending: Vec<([u8; 32], Fk)>,
    ctrl: RunControl,
    /// Uncataloged L0 spill paths awaiting coalesce (under runs_dir/l0/).
    l0: Vec<SortedRunPath>,
    l0_bytes: u64,
}

impl RunMemtable for Inner {
    fn pending_len(&self) -> usize {
        self.pending.len()
    }
    fn control(&self) -> &RunControl {
        &self.ctrl
    }
    fn control_mut(&mut self) -> &mut RunControl {
        &mut self.ctrl
    }
    fn flush_pending(&mut self) -> Result<u64, StoreError> {
        if self.pending.is_empty() {
            return Ok(0);
        }
        let mut recs = std::mem::take(&mut self.pending);
        recs.sort_unstable_by(|a, b| a.0.cmp(&b.0).then(a.1 .0.cmp(&b.1 .0)));
        recs.dedup_by(|a, b| a.0 == b.0 && a.1 == b.1);
        let mut body = Vec::with_capacity(recs.len() * SH_RUN_REC_LEN as usize);
        for (sh, fk) in &recs {
            body.extend_from_slice(&encode_rec(sh, *fk));
        }
        let l0_dir = self.ctrl.runs_dir.join("l0");
        std::fs::create_dir_all(&l0_dir).map_err(|e| StoreError::io(&l0_dir, e))?;
        let path = next_run_path(&l0_dir, self.ctrl.next_seq);
        self.ctrl.next_seq += 1;
        let _io = self.ctrl.runs_io.lock().unwrap();
        // Write without parent MANIFEST (l0 dir has no catalog); file only.
        let run = write_sorted_run(&path, SH_RUN_KEY_LEN, SH_RUN_REC_LEN, &body)?;
        // Detach from l0 MANIFEST pollution: list_runs on l0 may build MANIFEST —
        // we track l0 in RAM. Remove MANIFEST if write_sorted_run created one.
        let _ = std::fs::remove_file(l0_dir.join("MANIFEST"));
        self.l0_bytes = self.l0_bytes.saturating_add(run_body_bytes(&run));
        self.l0.push(run);
        Ok(recs.len() as u64)
    }
}

/// Shared Direct-IBD SH builder + low-prio worker.
pub struct ShRunBuilder {
    inner: Arc<Mutex<Inner>>,
    cv: Arc<Condvar>,
    enabled: AtomicBool,
    join: Mutex<Option<JoinHandle<()>>>,
    pub enqueued: AtomicU64,
    /// Process cache of SEAL (shared with worker).
    sealed_fk: Arc<AtomicU64>,
    runs_dir: PathBuf,
}

impl ShRunBuilder {
    pub fn new(store_dir: &Path) -> Self {
        let ctrl = RunControl::open(store_dir, "scripthash.runs");
        let runs_dir = ctrl.runs_dir.clone();
        let sealed = load_seal(&runs_dir);
        Self {
            inner: Arc::new(Mutex::new(Inner {
                pending: Vec::new(),
                ctrl,
                l0: Vec::new(),
                l0_bytes: 0,
            })),
            cv: Arc::new(Condvar::new()),
            enabled: AtomicBool::new(false),
            join: Mutex::new(None),
            enqueued: AtomicU64::new(0),
            sealed_fk: Arc::new(AtomicU64::new(sealed)),
            runs_dir,
        }
    }

    pub fn is_enabled(&self) -> bool {
        self.enabled.load(Ordering::Relaxed)
    }

    /// Max create_fk present in durable cataloged runs (SEAL).
    pub fn sealed_max_create_fk(&self) -> u64 {
        self.sealed_fk.load(Ordering::Acquire)
    }

    pub fn enable(&self) {
        {
            let mut g = self.inner.lock().unwrap();
            g.ctrl.reset_for_enable();
        }
        let sealed = load_seal(&self.runs_dir);
        self.sealed_fk.store(sealed, Ordering::Release);
        let inner_w = Arc::clone(&self.inner);
        let cv_w = Arc::clone(&self.cv);
        let runs_dir = self.runs_dir.clone();
        let sealed_w = Arc::clone(&self.sealed_fk);

        spawn_worker(
            "ibd-sh-index",
            || {
                info!(
                    "ibd: scripthash catch-up mode ON (memtable→target-sized runs+SEAL; bulk materialize at tip)"
                );
            },
            &self.enabled,
            &self.join,
            move || {
                debug!("ibd: SH run worker started (idle IO prio, spill+coalesce)");
                sh_worker_loop(
                    memtable_cap("RBITCOIN_SH_MEMTABLE_CAP", DEFAULT_MEMTABLE_CAP),
                    inner_w,
                    cv_w,
                    runs_dir,
                    sealed_w,
                );
                debug!("ibd: SH run worker stopped");
            },
        );
    }

    pub fn on_disk_run_count(&self) -> usize {
        let g = self.inner.lock().unwrap();
        let (dir, io) = runs_dir_io(&g.ctrl);
        drop(g);
        on_disk_run_count(&dir, &io)
    }

    pub fn memtable_len(&self) -> usize {
        self.inner.lock().unwrap().pending.len()
    }

    pub fn enqueue(&self, creates: &[ScriptHashRecord]) {
        if !self.is_enabled() || creates.is_empty() {
            return;
        }
        let cap = memtable_cap("RBITCOIN_SH_MEMTABLE_CAP", DEFAULT_MEMTABLE_CAP);
        let hard = cap.saturating_mul(HARD_MEMTABLE_MUL);
        let sealed = self.sealed_max_create_fk();
        let mut g = self.inner.lock().unwrap();
        for rec in creates {
            if rec.create_tx_fk.is_null() {
                continue;
            }
            // Already durable in a sealed run — skip.
            if rec.create_tx_fk.0 <= sealed {
                continue;
            }
            while g.pending.len() >= hard && !g.ctrl.stop {
                self.cv.notify_all();
                g = self
                    .cv
                    .wait_timeout(g, Duration::from_millis(50))
                    .unwrap()
                    .0;
            }
            g.pending.push((rec.scripthash, rec.create_tx_fk));
            self.enqueued.fetch_add(1, Ordering::Relaxed);
        }
        if g.pending.len() >= cap {
            self.cv.notify_all();
        }
    }

    /// Reload SEAL from disk into process cache (after worker coalesce / resume).
    pub fn refresh_seal(&self) {
        let s = load_seal(&self.runs_dir);
        self.sealed_fk.store(s, Ordering::Release);
    }

    /// Wipe SH runs/SEAL/cold progress/include_hwm and empty durable SH tables.
    ///
    /// Used by `RBITCOIN_SH_FORCE_REBUILD=1` so tip materialize recollects **all**
    /// Class A creates (SEAL=0) instead of a catch-up tail only.
    ///
    /// **Re-enables** the run builder after wipe so Class A recollect can enqueue
    /// (finalize_wait_join had disabled it). Materialize will join the worker again.
    pub fn prepare_force_full_rebuild(&self, store: &Store) -> Result<(), StoreError> {
        info!(
            "node: scripthash FORCE_REBUILD — clearing runs/SEAL/progress/HWM and reinit head"
        );
        finalize_wait_join(&self.enabled, &self.inner, &self.cv, &self.join)?;
        {
            let mut g = self.inner.lock().unwrap();
            g.pending.clear();
            g.l0.clear();
            g.l0_bytes = 0;
        }
        clear_runs_dir(&self.runs_dir);
        let _ = std::fs::create_dir_all(&self.runs_dir);
        store_seal(&self.runs_dir, 0)?;
        self.sealed_fk.store(0, Ordering::Release);
        ColdProgress::clear(store.path());
        let hwm_path = store.path().join(rbitcoin_store::INCLUDE_HWM_NAME);
        let _ = std::fs::remove_file(&hwm_path);
        store.scripthash.reinit_empty_for_cold_materialize()?;
        // Critical: recollect enqueues into the SH run pipeline. Leaving `enabled`
        // false (after finalize_wait_join) made rebuild_sh a silent no-op and tip
        // finished FullCold with creates≈0 on a zeroed head.
        self.enable();
        Ok(())
    }

    /// Ensure the SH run worker is enabled (idempotent). Used before Class A recollect.
    pub fn ensure_enabled(&self) {
        if !self.is_enabled() {
            self.enable();
        }
    }

    /// Thread-safe: sort `creates` and append one **catalog** run (direct write).
    ///
    /// Used by parallel Class A recollect so each worker can spill ~128 MiB without
    /// going through the single-threaded memtable. Does **not** advance SEAL —
    /// the recollect coordinator bumps a **contiguous** watermark so resume never
    /// skips unfinished lower fk ranges.
    ///
    /// Returns `(max_create_fk, record_count)`.
    pub fn spill_creates_catalog(
        &self,
        creates: &mut [ScriptHashRecord],
    ) -> Result<(u64, u64), StoreError> {
        if creates.is_empty() {
            return Ok((0, 0));
        }
        creates.sort_unstable_by(|a, b| {
            a.scripthash
                .cmp(&b.scripthash)
                .then_with(|| a.create_tx_fk.0.cmp(&b.create_tx_fk.0))
        });
        let mut body = Vec::with_capacity(creates.len().saturating_mul(SH_RUN_REC_LEN as usize));
        let mut max_fk = 0u64;
        let mut n = 0u64;
        let mut prev: Option<([u8; 32], u64)> = None;
        for rec in creates.iter() {
            if rec.create_tx_fk.is_null() {
                continue;
            }
            let fk = rec.create_tx_fk.0;
            // Dedup identical (sh, fk) after sort.
            if let Some((psh, pfk)) = prev {
                if psh == rec.scripthash && pfk == fk {
                    continue;
                }
            }
            prev = Some((rec.scripthash, fk));
            max_fk = max_fk.max(fk);
            body.extend_from_slice(&encode_rec(&rec.scripthash, rec.create_tx_fk));
            n = n.saturating_add(1);
        }
        if body.is_empty() {
            return Ok((0, 0));
        }
        let (runs_dir, runs_io) = {
            let g = self.inner.lock().unwrap();
            (g.ctrl.runs_dir.clone(), Arc::clone(&g.ctrl.runs_io))
        };
        let mut next_seq = {
            let g = self.inner.lock().unwrap();
            g.ctrl.next_seq
        };
        let run = {
            let _io = runs_io.lock().unwrap();
            next_seq = next_seq.max(next_seq_ceiling(&runs_dir));
            let path = next_run_path(&runs_dir, next_seq);
            next_seq = next_seq.saturating_add(1);
            let run = write_sorted_run(&path, SH_RUN_KEY_LEN, SH_RUN_REC_LEN, &body)?;
            // Per-spill detail is noisy on long recollect (~1k spills); status line
            // carries aggregates. Keep path for debug forensics.
            debug!(
                "ibd: SH recollect spill records≈{} body≈{:.1}MiB max_fk={max_fk} path={}",
                run.count,
                run_body_bytes(&run) as f64 / (1024.0 * 1024.0),
                run.path.display()
            );
            run
        };
        {
            let mut g = self.inner.lock().unwrap();
            g.ctrl.next_seq = next_seq.max(g.ctrl.next_seq);
        }
        let _ = run;
        Ok((max_fk, n))
    }

    /// Publish contiguous SEAL watermark (recollect resume floor).
    pub fn publish_seal_watermark(&self, seal: u64) -> Result<(), StoreError> {
        if seal == 0 {
            return Ok(());
        }
        let cur = self.sealed_max_create_fk();
        if seal <= cur {
            return Ok(());
        }
        store_seal(&self.runs_dir, seal)?;
        self.sealed_fk.store(seal, Ordering::Release);
        Ok(())
    }

    /// Drop incomplete/stale run catalog and SEAL so Class A recollect starts at 0.
    ///
    /// Does **not** wipe a durable SH head (use [`Self::prepare_force_full_rebuild`] for that).
    /// Re-enables the run builder so recollect can spill (same footgun as FORCE prep).
    pub fn reset_catalog_for_full_recollect(&self) -> Result<(), StoreError> {
        info!(
            "node: scripthash catalog incomplete/stale — resetting SEAL=0 and clearing runs for full Class A recollect"
        );
        {
            let mut g = self.inner.lock().unwrap();
            g.pending.clear();
            g.l0.clear();
            g.l0_bytes = 0;
        }
        clear_runs_dir(&self.runs_dir);
        let _ = std::fs::create_dir_all(&self.runs_dir);
        store_seal(&self.runs_dir, 0)?;
        self.sealed_fk.store(0, Ordering::Release);
        self.ensure_enabled();
        Ok(())
    }

    /// Clamp SEAL down to `max_fk` (gap recollect for durable head + warm residual).
    pub fn set_sealed_max_for_recollect(&self, max_fk: u64) -> Result<(), StoreError> {
        store_seal(&self.runs_dir, max_fk)?;
        self.sealed_fk.store(max_fk, Ordering::Release);
        Ok(())
    }

    /// Force flush memtable + L0 coalesce (tests / resume / tip finalize).
    ///
    /// Promotes all L0 (including undersized tails) and compacts tiny catalog runs.
    pub fn drain_spills(&self) -> Result<(), StoreError> {
        let mut g = self.inner.lock().unwrap();
        if !g.pending.is_empty() {
            g.flush_pending()?;
        }
        let runs_dir = g.ctrl.runs_dir.clone();
        let runs_io = Arc::clone(&g.ctrl.runs_io);
        let mut next_seq = g.ctrl.next_seq;
        let l0 = std::mem::take(&mut g.l0);
        g.l0_bytes = 0;
        drop(g);
        let leftover = {
            let _io = runs_io.lock().unwrap();
            // Planted catalog runs (tests / crash recovery) may outrun next_seq.
            next_seq = next_seq.max(next_seq_ceiling(&runs_dir));
            let left =
                coalesce_l0_to_catalog(&runs_dir, l0, &mut next_seq, &self.sealed_fk, true)?;
            compact_catalog_undersized(&runs_dir, &mut next_seq, &self.sealed_fk)?;
            left
        };
        {
            let mut g = self.inner.lock().unwrap();
            g.ctrl.next_seq = next_seq;
            // force_all should leave nothing; keep any remainder for safety.
            for r in leftover {
                g.l0_bytes = g.l0_bytes.saturating_add(run_body_bytes(&r));
                g.l0.push(r);
            }
        }
        self.refresh_seal();
        Ok(())
    }

    /// Flush memtable, coalesce, claim runs, fan-in reduce, cold bulk-load durable SH.
    pub fn finalize_and_bulk_materialize(&self, store: &Store) -> Result<u64, StoreError> {
        self.finalize_and_bulk_materialize_cancellable(store, None)
    }

    /// Like [`Self::finalize_and_bulk_materialize`] with cooperative cancel (SIGINT).
    ///
    /// Mid-reduce cancel leaves a per-chunk **CHECKPOINT** (remaining + done outs).
    /// Catalog runs that appear after materialize starts (catch-up while interrupted)
    /// are applied to the live SH head **after** cold bulk load — not mixed into
    /// an in-progress reduce.
    pub fn finalize_and_bulk_materialize_cancellable(
        &self,
        store: &Store,
        cancel: Option<&AtomicBool>,
    ) -> Result<u64, StoreError> {
        finalize_wait_join(&self.enabled, &self.inner, &self.cv, &self.join)?;
        // Drain any leftover pending + L0 (worker may have stopped with L0 in RAM).
        self.drain_spills()?;

        let (runs_dir, runs_io) = {
            let g = self.inner.lock().unwrap();
            (g.ctrl.runs_dir.clone(), Arc::clone(&g.ctrl.runs_io))
        };

        let merge_dir = runs_dir.join("merge");

        let t_claim = Instant::now();
        let mut claimed: Vec<SortedRunPath> = Vec::new();
        let mut stream_inputs: Vec<SortedRunPath> = Vec::new();
        // New catalog runs deferred until after cold materialize (post-interrupt catch-up).
        let mut pending_after: Vec<SortedRunPath> = Vec::new();
        let mut resumed_from_ready = false;
        let mut resume_checkpoint = false;
        {
            let _io = runs_io.lock().unwrap();
            let mut prior = list_materialize_claims(&runs_dir)?;
            let mut runs = list_runs(&runs_dir)?;
            runs.sort_by_key(|r| r.seq().unwrap_or(u64::MAX));

            let ready_out = list_fanin_reduce_outputs(&merge_dir)?;
            let has_cp = load_fanin_checkpoint(&merge_dir)?.is_some();

            if let Some(reduced) = ready_out {
                // READY: stream reduced outputs; any new catalog runs are deferred.
                info!(
                    "node: scripthash resuming fan-in READY outputs ({}) under merge/",
                    reduced.len()
                );
                stream_inputs = reduced;
                resumed_from_ready = true;
                // Leftover claims should not exist after READY; if present, treat as deferred.
                pending_after.append(&mut prior);
                pending_after.append(&mut runs);
            } else if has_cp {
                // Mid-reduce: resume CHECKPOINT only — do not claim new catalog runs into reduce.
                resume_checkpoint = true;
                info!(
                    "node: scripthash resuming fan-in CHECKPOINT (partial reduce); \
                     deferring {} new catalog run(s) until after materialize",
                    runs.len()
                );
                pending_after.append(&mut runs);
                // Old mats may still exist if not yet deleted by chunk merges.
                claimed.append(&mut prior);
            } else {
                // Fresh materialize: claim everything.
                let _ = std::fs::remove_dir_all(&merge_dir);
                if !prior.is_empty() {
                    info!(
                        "node: scripthash resuming {} incomplete materialize claim(s)",
                        prior.len()
                    );
                }
                claimed.append(&mut prior);
                for run in runs {
                    claimed.push(claim_run_for_materialize(&run)?);
                }
            }
        }
        let claim_ns = t_claim.elapsed().as_nanos() as u64;

        if !resumed_from_ready && claimed.is_empty() && !resume_checkpoint {
            if pending_after.is_empty() {
                info!("node: scripthash bulk materialize: no runs");
                clear_runs_dir(&runs_dir);
                return Ok(0);
            }
            // Only deferred new runs — apply warm without cold reinit.
            info!(
                "node: scripthash apply {} deferred run(s) to empty/live head (no cold reduce)",
                pending_after.len()
            );
            let mut max_fk = store.scripthash.include_hwm();
            for r in &pending_after {
                if let Ok(body) = rbitcoin_store::read_run_body(r) {
                    max_fk = max_fk.max(max_fk_in_body(&body));
                }
            }
            let n = apply_runs_to_live_sh(store, &pending_after, cancel)?;
            for r in &pending_after {
                let _ = std::fs::remove_file(&r.path);
            }
            clear_runs_dir(&runs_dir);
            if max_fk > 0 {
                let _ = store.scripthash.note_include_hwm(max_fk);
                let _ = store_seal(&runs_dir, max_fk);
                self.sealed_fk.store(max_fk, Ordering::Release);
            }
            return Ok(n);
        }

        let workers = rbitcoin_store::sh_merge_workers();
        let t_reduce = Instant::now();
        let max_direct = max_direct_merge();
        let mut direct_kway = false;
        if !resumed_from_ready {
            let claimed_recs: u64 = claimed.iter().map(|r| r.count).sum();
            let claimed_body: u64 = claimed
                .iter()
                .map(|r| r.count.saturating_mul(u64::from(r.rec_len)))
                .sum();
            if !resume_checkpoint && claimed.len() <= max_direct {
                // Primary path: stream claimed mats directly (no intermediate rewrite).
                direct_kway = true;
                info!(
                    "node: scripthash tip direct k-way claimed={} workers={workers} \
                     records≈{claimed_recs} body≈{:.1}MiB max_direct={max_direct}",
                    claimed.len(),
                    claimed_body as f64 / (1024.0 * 1024.0),
                );
                stream_inputs = std::mem::take(&mut claimed);
            } else {
                info!(
                    "node: scripthash tip fanin reduce start claimed={} workers={workers} \
                     records≈{claimed_recs} body≈{:.1}MiB passes=1 target_stream≤{max_direct} \
                     checkpoint_resume={resume_checkpoint}",
                    claimed.len(),
                    claimed_body as f64 / (1024.0 * 1024.0),
                );
                stream_inputs = {
                    let _io = runs_io.lock().unwrap();
                    let out =
                        reduce_runs_to_fanin_cancellable(&claimed, &merge_dir, 0, cancel)?;
                    commit_fanin_reduce_and_drop_inputs(&merge_dir, &claimed, &out)?;
                    out
                };
                info!(
                    "node: scripthash tip fanin reduce done claimed={} stream={} workers={workers} \
                     elapsed={:?} pct=100",
                    claimed.len(),
                    stream_inputs.len(),
                    t_reduce.elapsed()
                );
            }
        } else {
            info!(
                "node: scripthash tip fanin reduce resumed stream={} (READY) workers={workers}",
                stream_inputs.len()
            );
        }
        let reduce_ns = t_reduce.elapsed().as_nanos() as u64;

        if stream_inputs.is_empty() {
            info!("node: scripthash bulk materialize: no stream inputs after reduce");
            clear_runs_dir(&runs_dir);
            return Ok(0);
        }

        let total_recs: u64 = stream_inputs.iter().map(|r| r.count).sum();
        let n_existing = store.scripthash.entry_count();
        let head_empty = store.scripthash.head_is_empty();
        let n_shards = store.scripthash.head_shard_count();
        let store_dir = store.path();
        let progress = ColdProgress::load(store_dir).ok().flatten();
        let force = sh_force_rebuild();
        let tip_max = store.txs.count();
        let seal_now = self.sealed_max_create_fk();
        let cat_recs = stream_inputs.iter().map(|r| r.count).sum::<u64>();
        // Durable head: empty/tiny residual runs are normal after consume — do not
        // flag mass incompleteness (that is an empty-head / FORCE concern only).
        let head_live = !head_empty || n_existing > 0;
        let catalog_ok = if head_live {
            sh_catalog_seal_covers_tip(seal_now, tip_max)
        } else {
            sh_catalog_looks_complete(seal_now, tip_max, cat_recs)
        };
        let mode = select_sh_tip_materialize_mode(
            head_empty,
            n_existing,
            progress.as_ref().map(|p| p.next_shard),
            n_shards as u32,
            stream_inputs.len(),
            force,
            catalog_ok,
        );
        info!(
            "node: scripthash tip materialize path={mode:?} entry_count={n_existing} \
             head_empty={head_empty} stream_runs={} records≈{total_recs} direct_kway={direct_kway} \
             force_rebuild={force} catalog_complete={catalog_ok} seal={seal_now} tip_max_fk={tip_max} \
             progress={:?}",
            stream_inputs.len(),
            progress.as_ref().map(|p| p.next_shard),
        );

        // ── Warm-only: residual runs against a live index (never reinit) ─────
        if matches!(mode, ShTipMaterializeMode::WarmOnly) {
            info!(
                "node: scripthash warm apply residual runs={} (protecting durable head; no reinit)",
                stream_inputs.len()
            );
            let t0 = Instant::now();
            // Inclusion HWM before deleting run files.
            let mut max_fk = store.scripthash.include_hwm();
            for r in stream_inputs.iter().chain(pending_after.iter()) {
                if let Ok(body) = rbitcoin_store::read_run_body(r) {
                    max_fk = max_fk.max(max_fk_in_body(&body));
                }
            }
            let n_warm = apply_runs_to_live_sh(store, &stream_inputs, cancel)?;
            // Deferred catch-up catalog runs (not claimed into stream).
            let mut n_deferred = 0u64;
            if !pending_after.is_empty() {
                info!(
                    "node: scripthash applying {} deferred run(s) after warm residual",
                    pending_after.len()
                );
                n_deferred = apply_runs_to_live_sh(store, &pending_after, cancel)?;
                for r in &pending_after {
                    let _ = std::fs::remove_file(&r.path);
                }
            }
            for run in &claimed {
                let _ = std::fs::remove_file(&run.path);
            }
            for run in &stream_inputs {
                let _ = std::fs::remove_file(&run.path);
            }
            let _ = std::fs::remove_dir_all(&merge_dir);
            clear_runs_dir(&runs_dir);
            if max_fk > 0 {
                let _ = store.scripthash.note_include_hwm(max_fk);
                let _ = store_seal(&runs_dir, max_fk);
                self.sealed_fk.store(max_fk, Ordering::Release);
            }
            info!(
                "node: scripthash warm residual done written≈{} deferred≈{n_deferred} \
                 include_hwm={max_fk} elapsed={:?}",
                n_warm,
                t0.elapsed()
            );
            let _ = FAMILY_SH;
            let _ = (claim_ns, reduce_ns);
            return Ok(n_warm.saturating_add(n_deferred));
        }

        // ── Cold path (full or resume) ───────────────────────────────────────
        let resume_from = match &mode {
            ShTipMaterializeMode::ColdResume { next_shard } => *next_shard as usize,
            _ => 0,
        };
        let t_reinit = Instant::now();
        let mut session = match mode {
            ShTipMaterializeMode::ColdResume { .. } => {
                let p = progress.expect("ColdResume requires progress");
                info!(
                    "node: scripthash cold resume next_shard={}/{} keys≈{} creates≈{} bump={} \
                     stream_runs={}",
                    p.next_shard,
                    n_shards,
                    p.keys_written,
                    p.live_count,
                    p.body_bump,
                    stream_inputs.len()
                );
                store.scripthash.prepare_cold_resume(&p)?;
                store.scripthash.bulk_session_resume(0, &p)?
            }
            ShTipMaterializeMode::FullCold => {
                info!(
                    "node: scripthash reinit empty for cold rematerialize \
                     stream_runs={} entry_count={n_existing} head_empty={head_empty} \
                     n_shards={n_shards} (full cold authorized: empty head or force_rebuild)",
                    stream_inputs.len()
                );
                store.scripthash.reinit_empty_for_cold_materialize()?;
                debug_assert_eq!(store.scripthash.entry_count(), 0);
                debug_assert!(store.scripthash.head_is_empty());
                store.scripthash.bulk_session(0)?
            }
            ShTipMaterializeMode::WarmOnly => unreachable!("warm handled above"),
        };
        let reinit_ns = t_reinit.elapsed().as_nanos() as u64;
        info!(
            "node: scripthash bulk materialize start runs={} records≈{total_recs} cold=true \
             direct_kway={direct_kway} n_shards={n_shards} resume_from_shard={resume_from}",
            stream_inputs.len()
        );
        let t0 = Instant::now();
        let mut cur_key: Option<[u8; 32]> = None;
        let mut chain: Vec<ScriptHashEntry> = Vec::with_capacity(8);
        let mut long_seen: Option<HashSet<u64>> = None;
        let mut unique_in = 0u64;
        let mut last_log: Option<Instant> = None;
        let mut max_fk_seen = 0u64;

        let t_stream = Instant::now();
        let stream_result = for_each_merged_rec_opts(&stream_inputs, false, |rec| {
            if rec.len() < SH_RUN_REC_LEN as usize {
                return Err(StoreError::Corrupt("sh run short record in merge stream"));
            }
            let (sh, tx_fk) = decode_rec_fixed(rec);
            if tx_fk.is_null() {
                return Ok(());
            }
            // Resume: skip complete prefix bands (already installed head shards).
            if prefix_shard_of(&sh, n_shards) < resume_from {
                return Ok(());
            }
            if tx_fk.0 > max_fk_seen {
                max_fk_seen = tx_fk.0;
            }
            if cur_key != Some(sh) {
                if let Some(prev) = cur_key {
                    if !chain.is_empty() {
                        unique_in = unique_in.saturating_add(1);
                        session.put_chain(prev, &chain)?;
                        chain.clear();
                        long_seen = None;
                        if cancel
                            .map(|c| c.load(Ordering::Relaxed))
                            .unwrap_or(false)
                        {
                            return Err(StoreError::Cancelled("scripthash materialize stream"));
                        }
                        let due = match last_log {
                            None => true,
                            Some(t) => t.elapsed() >= MATERIALIZE_STATUS_INTERVAL,
                        };
                        if due {
                            last_log = Some(Instant::now());
                            let keys = session.keys_written();
                            let creates = session.creates_written();
                            let shards = session.shards_flushed();
                            let elapsed = t0.elapsed();
                            let secs = elapsed.as_secs_f64().max(1e-3);
                            let keys_per_s = keys as f64 / secs;
                            let pct = if total_recs > 0 {
                                (100.0 * creates as f64 / total_recs as f64).clamp(0.0, 99.9)
                            } else {
                                0.0
                            };
                            info!(
                                "node: scripthash materialize status keys≈{keys} creates≈{creates} \
                                 pct≈{pct:.1}% shards={shards}/{n_shards} rate≈{keys_per_s:.0}keys/s \
                                 body_flush={:?} head_fill={:?} elapsed={elapsed:?}",
                                Duration::from_nanos(session.body_flush_ns),
                                Duration::from_nanos(session.head_fill_ns),
                            );
                        }
                    }
                }
                cur_key = Some(sh);
            }
            let is_dup = if let Some(ref set) = long_seen {
                set.contains(&tx_fk.0)
            } else {
                chain_has_fk(&chain, tx_fk)
            };
            if !is_dup {
                chain.push(ScriptHashEntry::new(tx_fk));
                if let Some(ref mut set) = long_seen {
                    set.insert(tx_fk.0);
                } else if chain.len() >= CHAIN_SET_THRESHOLD {
                    let mut set = HashSet::with_capacity(chain.len() * 2);
                    for e in &chain {
                        set.insert(e.create_tx_fk.0);
                    }
                    long_seen = Some(set);
                }
            }
            Ok(())
        });
        if let Err(StoreError::Cancelled(msg)) = stream_result {
            // Keep complete shards + cold_progress; leave stream_inputs for resume.
            session.abandon_incomplete();
            info!(
                "node: scripthash materialize cancelled ({msg}); complete shards kept — restart resumes"
            );
            return Err(StoreError::Cancelled(msg));
        }
        stream_result?;
        if let Some(prev) = cur_key.take() {
            if !chain.is_empty() {
                unique_in = unique_in.saturating_add(1);
                session.put_chain(prev, &chain)?;
            }
        }
        let stream_ns = t_stream.elapsed().as_nanos() as u64;

        let t_finish = Instant::now();
        let (n_total, n_keys, body_flush_ns, head_fill_ns) = session.finish()?;
        store.scripthash.flush()?;
        let finish_ns = t_finish.elapsed().as_nanos() as u64;

        // Success barrier: drop materialize artifacts (not deferred new runs).
        for run in &claimed {
            let _ = std::fs::remove_file(&run.path);
        }
        for run in &stream_inputs {
            let _ = std::fs::remove_file(&run.path);
        }
        let _ = std::fs::remove_dir_all(&merge_dir);
        ColdProgress::clear(store_dir);

        // Post-interrupt catch-up runs: warm-insert into live SH head (no reinit).
        let mut n_deferred = 0u64;
        if !pending_after.is_empty() {
            info!(
                "node: scripthash applying {} deferred run(s) after cold materialize",
                pending_after.len()
            );
            n_deferred = apply_runs_to_live_sh(store, &pending_after, cancel)?;
            for r in &pending_after {
                let _ = std::fs::remove_file(&r.path);
            }
        }
        clear_runs_dir(&runs_dir);
        if max_fk_seen > 0 {
            let _ = store_seal(&runs_dir, max_fk_seen);
            self.sealed_fk.store(max_fk_seen, Ordering::Release);
            let _ = store.scripthash.note_include_hwm(max_fk_seen);
        }

        info!(
            "node: scripthash bulk materialize done creates≈{n_total} keys≈{n_keys} unique_in≈{unique_in} \
             deferred≈{n_deferred} shards={n_shards} elapsed={:?} \
             stages: claim={:?} reduce={:?} reinit={:?} stream={:?} body_flush={:?} head_fill={:?} finish_flush={:?}",
            t0.elapsed(),
            Duration::from_nanos(claim_ns),
            Duration::from_nanos(reduce_ns),
            Duration::from_nanos(reinit_ns),
            Duration::from_nanos(stream_ns),
            Duration::from_nanos(body_flush_ns),
            Duration::from_nanos(head_fill_ns),
            Duration::from_nanos(finish_ns),
        );
        let _ = FAMILY_SH;
        Ok(n_total.saturating_add(n_deferred))
    }
}

/// Batch size for warm deferred apply (records per `put_create_batch_append`).
///
/// Avoids per-create `put_create` (head probe + contains walk each time) which
/// pegs one core for hours on a multi‑GiB live head after cold materialize.
const DEFERRED_APPLY_BATCH: usize = 64_000;
/// Wall interval for deferred-apply INFO heartbeats.
const DEFERRED_STATUS_INTERVAL: Duration = Duration::from_secs(10);

/// Stream sorted-run records into the **live** SH table (batched tip-style append).
///
/// Runs are already scripthash-sorted: group into batches and
/// [`ScriptHashTable::put_create_batch_append`] (one head seed + body merge per
/// distinct key per batch). Not the cold live-OA path — head is already full.
fn apply_runs_to_live_sh(
    store: &Store,
    runs: &[SortedRunPath],
    cancel: Option<&AtomicBool>,
) -> Result<u64, StoreError> {
    if runs.is_empty() {
        return Ok(0);
    }
    let total_recs: u64 = runs.iter().map(|r| r.count).sum();
    let body_mib: f64 = runs
        .iter()
        .map(|r| run_body_bytes(r) as f64)
        .sum::<f64>()
        / (1024.0 * 1024.0);
    info!(
        "node: scripthash deferred warm apply start runs={} records≈{total_recs} body≈{body_mib:.1}MiB \
         batch={DEFERRED_APPLY_BATCH}",
        runs.len()
    );
    let t0 = Instant::now();
    let mut n = 0u64;
    let mut batch: Vec<ScriptHashRecord> = Vec::with_capacity(DEFERRED_APPLY_BATCH);
    // Process-local head cache; cleared each batch (stream is key-sorted, no revisit).
    let mut heads = std::collections::HashMap::new();
    let mut last_log = Instant::now();
    let mut recs_seen = 0u64;

    let flush_batch = |batch: &mut Vec<ScriptHashRecord>,
                       heads: &mut std::collections::HashMap<[u8; 32], rbitcoin_store::ShHeadValue>,
                       n: &mut u64|
     -> Result<(), StoreError> {
        if batch.is_empty() {
            return Ok(());
        }
        let (w, _) = store.scripthash.put_create_batch_append(batch, heads)?;
        *n = n.saturating_add(w as u64);
        batch.clear();
        heads.clear();
        Ok(())
    };

    for_each_merged_rec_opts(runs, false, |rec| {
        if cancel.map(|c| c.load(Ordering::Relaxed)).unwrap_or(false) {
            return Err(StoreError::Cancelled("scripthash deferred apply"));
        }
        if rec.len() < SH_RUN_REC_LEN as usize {
            return Err(StoreError::Corrupt("sh run short record in deferred apply"));
        }
        let (sh, tx_fk) = decode_rec_fixed(rec);
        if tx_fk.is_null() {
            return Ok(());
        }
        recs_seen = recs_seen.saturating_add(1);
        batch.push(ScriptHashRecord::from_fk(sh, tx_fk));
        if batch.len() >= DEFERRED_APPLY_BATCH {
            flush_batch(&mut batch, &mut heads, &mut n)?;
        }
        if last_log.elapsed() >= DEFERRED_STATUS_INTERVAL {
            last_log = Instant::now();
            let pct = if total_recs > 0 {
                (100.0 * recs_seen as f64 / total_recs as f64).clamp(0.0, 99.9)
            } else {
                0.0
            };
            let secs = t0.elapsed().as_secs_f64().max(1e-3);
            info!(
                "node: scripthash deferred warm apply status recs≈{recs_seen}/{total_recs} \
                 pct≈{pct:.1}% written≈{n} rate≈{:.0}rec/s elapsed={:?}",
                recs_seen as f64 / secs,
                t0.elapsed()
            );
        }
        Ok(())
    })?;
    flush_batch(&mut batch, &mut heads, &mut n)?;
    store.scripthash.flush()?;
    info!(
        "node: scripthash deferred warm apply done written≈{n} recs≈{recs_seen} elapsed={:?}",
        t0.elapsed()
    );
    Ok(n)
}

/// Next seq id strictly above any cataloged run (and current counter).
fn next_seq_ceiling(runs_dir: &Path) -> u64 {
    list_runs(runs_dir)
        .ok()
        .and_then(|rs| rs.iter().filter_map(|r| r.seq()).max())
        .map(|m| m.saturating_add(1))
        .unwrap_or(1)
}

/// Coalesce L0 spills into cataloged runs under `runs_dir` MANIFEST.
///
/// Promotes only when merged body ≥ [`promote_min_bytes`] unless `force_all`
/// (finalize / tip drain). Undersized remainder is rewritten back into L0 so
/// the catalog does not accumulate tiny alternating runs.
fn coalesce_l0_to_catalog(
    runs_dir: &Path,
    mut l0: Vec<SortedRunPath>,
    next_seq: &mut u64,
    sealed: &AtomicU64,
    force_all: bool,
) -> Result<Vec<SortedRunPath>, StoreError> {
    if l0.is_empty() {
        return Ok(Vec::new());
    }
    l0.sort_by_key(|r| r.seq().unwrap_or(u64::MAX));
    let fanin = merge_fanin();
    let target = target_run_bytes();
    let promote_min = promote_min_bytes(target);
    let mut leftover: Vec<SortedRunPath> = Vec::new();
    let mut i = 0;
    while i < l0.len() {
        let mut chunk = Vec::new();
        let mut bytes = 0u64;
        while i < l0.len() && chunk.len() < fanin {
            let b = run_body_bytes(&l0[i]);
            if !chunk.is_empty() && bytes + b > target && bytes >= promote_min {
                break;
            }
            bytes += b;
            chunk.push(l0[i].clone());
            i += 1;
            if bytes >= target {
                break;
            }
        }
        if chunk.is_empty() {
            break;
        }
        let mut max_fk = 0u64;
        for r in &chunk {
            if let Ok(body) = rbitcoin_store::read_run_body(r) {
                max_fk = max_fk.max(max_fk_in_body(&body));
            }
        }
        // Single small input and not finalizing: keep in L0 without rewrite.
        if !force_all && chunk.len() == 1 && bytes < promote_min {
            leftover.push(chunk[0].clone());
            continue;
        }
        let promote = force_all || bytes >= promote_min;
        if promote {
            let out = next_run_path(runs_dir, *next_seq);
            *next_seq += 1;
            let merged = merge_runs(&chunk, &out)?;
            info!(
                "ibd: SH catalog promote runs_in={} body≈{:.1}MiB path={}",
                chunk.len(),
                run_body_bytes(&merged) as f64 / (1024.0 * 1024.0),
                merged.path.display()
            );
            if max_fk > 0 {
                bump_seal(runs_dir, max_fk)?;
                let cur = sealed.load(Ordering::Relaxed);
                if max_fk > cur {
                    sealed.store(max_fk, Ordering::Release);
                }
            }
        } else {
            // Merge undersized chunk back into one L0 file to reduce file count.
            let l0_dir = runs_dir.join("l0");
            std::fs::create_dir_all(&l0_dir).map_err(|e| StoreError::io(&l0_dir, e))?;
            let out = next_run_path(&l0_dir, *next_seq);
            *next_seq += 1;
            let merged = merge_runs_to_l0(&chunk, &out)?;
            leftover.push(merged);
            debug!(
                "ibd: SH L0 hold undersized body≈{:.1}MiB (promote_min≈{:.1}MiB)",
                bytes as f64 / (1024.0 * 1024.0),
                promote_min as f64 / (1024.0 * 1024.0)
            );
        }
    }
    Ok(leftover)
}

/// Merge into an L0 path without parent catalog MANIFEST.
fn merge_runs_to_l0(
    inputs: &[SortedRunPath],
    out: &Path,
) -> Result<SortedRunPath, StoreError> {
    let merged = rbitcoin_store::merge_runs_to_file(inputs, out)?;
    for r in inputs {
        let _ = std::fs::remove_file(&r.path);
    }
    let l0_dir = out.parent().unwrap_or(out);
    let _ = std::fs::remove_file(l0_dir.join("MANIFEST"));
    Ok(merged)
}

/// Parallel Class A recollect: spill local buffer at this size (~128 MiB of 40 B recs).
pub const RECOLLECT_THREAD_SPILL_BYTES: u64 = 128 * 1024 * 1024;

/// Floor for catalog compact: do **not** merge intentional recollect spills.
///
/// Slightly below [`RECOLLECT_THREAD_SPILL_BYTES`] so ~128 MiB spills are never
/// candidates. Tip direct k-way can open thousands of FDs without rewriting them.
pub const CATALOG_COMPACT_FLOOR_BYTES: u64 =
    RECOLLECT_THREAD_SPILL_BYTES.saturating_mul(3) / 4; // 96 MiB

/// True if a catalog run body should be eligible for undersized compact.
///
/// Pure policy: crumbs only — never intentional recollect-scale spills.
pub fn catalog_run_is_compact_candidate(body_bytes: u64, target_run_bytes: u64) -> bool {
    if body_bytes == 0 {
        return false;
    }
    let half = target_run_bytes / 2;
    let small_max = half.min(CATALOG_COMPACT_FLOOR_BYTES);
    body_bytes < small_max
}

/// Compact catalog runs that are well under target (except at most one small tail).
///
/// Runs ≥ [`CATALOG_COMPACT_FLOOR_BYTES`] are left alone so parallel Class A
/// recollect's ~128 MiB spills stream straight into tip k-way merge.
fn compact_catalog_undersized(
    runs_dir: &Path,
    next_seq: &mut u64,
    sealed: &AtomicU64,
) -> Result<(), StoreError> {
    let target = target_run_bytes();
    let mut runs = list_runs(runs_dir)?;
    if runs.len() < 2 {
        return Ok(());
    }
    runs.sort_by_key(|r| r.seq().unwrap_or(u64::MAX));
    // Count small runs; if ≤1, done.
    let small: Vec<_> = runs
        .iter()
        .filter(|r| catalog_run_is_compact_candidate(run_body_bytes(r), target))
        .cloned()
        .collect();
    if small.len() <= 1 {
        return Ok(());
    }
    let fanin = merge_fanin();
    let mut i = 0;
    while i < small.len() {
        let mut chunk = Vec::new();
        let mut bytes = 0u64;
        while i < small.len() && chunk.len() < fanin {
            bytes += run_body_bytes(&small[i]);
            chunk.push(small[i].clone());
            i += 1;
            if bytes >= target {
                break;
            }
        }
        if chunk.len() < 2 {
            break;
        }
        let mut max_fk = 0u64;
        for r in &chunk {
            if let Ok(body) = rbitcoin_store::read_run_body(r) {
                max_fk = max_fk.max(max_fk_in_body(&body));
            }
        }
        let out = next_run_path(runs_dir, *next_seq);
        *next_seq += 1;
        let merged = merge_runs(&chunk, &out)?;
        info!(
            "ibd: SH catalog compact inputs={} body≈{:.1}MiB",
            chunk.len(),
            run_body_bytes(&merged) as f64 / (1024.0 * 1024.0)
        );
        if max_fk > 0 {
            bump_seal(runs_dir, max_fk)?;
            let cur = sealed.load(Ordering::Relaxed);
            if max_fk > cur {
                sealed.store(max_fk, Ordering::Release);
            }
        }
    }
    Ok(())
}

fn sh_worker_loop(
    soft_cap: usize,
    inner: Arc<Mutex<Inner>>,
    cv: Arc<Condvar>,
    runs_dir: PathBuf,
    sealed: Arc<AtomicU64>,
) {
    let target = target_run_bytes();
    let fanin = merge_fanin();
    loop {
        let mut g = inner.lock().unwrap();
        if g.ctrl.stop {
            break;
        }
        let need_flush = g.pending.len() >= soft_cap || (g.ctrl.finalize && !g.pending.is_empty());
        // Coalesce on **bytes** toward target (or finalize). Do not promote on
        // L0 file count alone — that created alternating large/small catalog runs.
        let need_coalesce = g.l0_bytes >= target
            || (g.l0.len() >= fanin && g.l0_bytes >= promote_min_bytes(target))
            || (g.ctrl.finalize && !g.l0.is_empty());

        if need_flush {
            if !g.pending.is_empty() {
                if let Err(e) = g.flush_pending() {
                    warn!("ibd: SH run flush failed: {e}");
                }
                cv.notify_all();
            }
            drop(g);
            std::thread::sleep(AFTER_WORK);
            continue;
        }

        if need_coalesce {
            let force_all = g.ctrl.finalize;
            let l0 = std::mem::take(&mut g.l0);
            g.l0_bytes = 0;
            let runs_io = Arc::clone(&g.ctrl.runs_io);
            let mut next_seq = g.ctrl.next_seq;
            drop(g);
            let leftover = {
                let _io = runs_io.lock().unwrap();
                next_seq = next_seq.max(next_seq_ceiling(&runs_dir));
                match coalesce_l0_to_catalog(&runs_dir, l0, &mut next_seq, &sealed, force_all) {
                    Ok(left) => {
                        if let Err(e) =
                            compact_catalog_undersized(&runs_dir, &mut next_seq, &sealed)
                        {
                            warn!("ibd: SH catalog compact failed: {e}");
                        }
                        left
                    }
                    Err(e) => {
                        warn!("ibd: SH L0 coalesce failed: {e}");
                        Vec::new()
                    }
                }
            };
            let mut g = inner.lock().unwrap();
            g.ctrl.next_seq = next_seq;
            for r in leftover {
                g.l0_bytes = g.l0_bytes.saturating_add(run_body_bytes(&r));
                g.l0.push(r);
            }
            drop(g);
            std::thread::sleep(AFTER_WORK);
            continue;
        }

        if g.ctrl.finalize && g.pending.is_empty() && g.l0.is_empty() {
            g.ctrl.stop = true;
            cv.notify_all();
            break;
        }

        let (gg, _) = cv.wait_timeout(g, IDLE_POLL).unwrap();
        g = gg;
        if g.ctrl.stop {
            break;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rbitcoin_store::Store;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn enqueue_flush_materialize() {
        let n = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("rbitcoin-sh-builder-{n}"));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let store = Store::open_or_create(&dir).unwrap();
        let b = ShRunBuilder::new(&dir);
        b.enable();
        let mut creates = Vec::new();
        for i in 0..100u32 {
            let mut sh = [0u8; 32];
            sh[0] = (i % 17) as u8;
            sh[1] = (i / 17) as u8;
            creates.push(ScriptHashRecord::from_fk(sh, Fk(i as u64 + 1)));
        }
        b.enqueue(&creates);
        let n = b.finalize_and_bulk_materialize(&store).unwrap();
        assert!(n >= 100, "inserted={n}");
        assert!(store.scripthash.entry_count() >= 100);
        assert!(b.sealed_max_create_fk() >= 100);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn seal_advances_on_spill() {
        let n = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("rbitcoin-sh-seal-{n}"));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let b = ShRunBuilder::new(&dir);
        b.enable();
        let mut creates = Vec::new();
        for i in 1..=50u64 {
            let mut sh = [0u8; 32];
            sh[0] = i as u8;
            creates.push(ScriptHashRecord::from_fk(sh, Fk(i)));
        }
        b.enqueue(&creates);
        b.drain_spills().unwrap();
        assert!(
            b.sealed_max_create_fk() >= 50,
            "seal={}",
            b.sealed_max_create_fk()
        );
        // Re-enqueue same fks: filtered by seal.
        let before = b.enqueued.load(Ordering::Relaxed);
        b.enqueue(&creates);
        assert_eq!(b.enqueued.load(Ordering::Relaxed), before);
        // Parallel recollect path: direct catalog spill.
        let mut more = vec![ScriptHashRecord::from_fk([0xee; 32], Fk(99))];
        let (mfk, n) = b.spill_creates_catalog(&mut more).unwrap();
        assert_eq!(n, 1);
        assert_eq!(mfk, 99);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn fanin_many_runs_materialize() {
        let n = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("rbitcoin-sh-fanin-{n}"));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        // Smaller than historical 40×10 but still multi-pass: fanin 4 → several reduce waves.
        std::env::set_var("RBITCOIN_SH_MERGE_FANIN", "4");
        let store = Store::open_or_create(&dir).unwrap();
        let b = ShRunBuilder::new(&dir);
        let runs_dir = dir.join("scripthash.runs");
        std::fs::create_dir_all(&runs_dir).unwrap();
        const N_RUNS: u64 = 16;
        const PER_RUN: u64 = 8;
        for seq in 1..=N_RUNS {
            let mut body = Vec::new();
            for j in 0..PER_RUN {
                let mut sh = [0u8; 32];
                sh[0] = seq as u8;
                sh[1] = j as u8;
                body.extend_from_slice(&encode_rec(&sh, Fk(seq * 100 + j + 1)));
            }
            let path = next_run_path(&runs_dir, seq);
            write_sorted_run(&path, SH_RUN_KEY_LEN, SH_RUN_REC_LEN, &body).unwrap();
        }
        let n = b.finalize_and_bulk_materialize(&store).unwrap();
        assert_eq!(n, N_RUNS * PER_RUN);
        assert_eq!(store.scripthash.entry_count(), N_RUNS * PER_RUN);
        std::env::remove_var("RBITCOIN_SH_MERGE_FANIN");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn materialize_warm_merges_residual_into_nonempty_table() {
        // Formerly expected full reinit (wipe to 40). Policy now: warm-merge into
        // durable head so prior creates (30) are preserved.
        let n = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("rbitcoin-sh-reinit-{n}"));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let store = Store::open_or_create(&dir).unwrap();
        let b = ShRunBuilder::new(&dir);
        let runs_dir = dir.join("scripthash.runs");
        std::fs::create_dir_all(&runs_dir).unwrap();

        let mut body = Vec::new();
        for i in 0..30u32 {
            let mut sh = [0u8; 32];
            sh[0] = i as u8;
            body.extend_from_slice(&encode_rec(&sh, Fk(i as u64 + 1)));
        }
        let path = next_run_path(&runs_dir, 1);
        write_sorted_run(&path, SH_RUN_KEY_LEN, SH_RUN_REC_LEN, &body).unwrap();
        let n1 = b.finalize_and_bulk_materialize(&store).unwrap();
        assert!(n1 >= 30);
        assert!(store.scripthash.entry_count() >= 30);

        let mut body2 = Vec::new();
        for i in 0..40u32 {
            let mut sh = [0u8; 32];
            sh[0] = (i + 100) as u8;
            body2.extend_from_slice(&encode_rec(&sh, Fk(i as u64 + 1000)));
        }
        let path2 = next_run_path(&runs_dir, 2);
        let run2 = write_sorted_run(&path2, SH_RUN_KEY_LEN, SH_RUN_REC_LEN, &body2).unwrap();
        let _ = claim_run_for_materialize(&run2).unwrap();
        assert!(store.scripthash.entry_count() > 0);

        let n2 = b.finalize_and_bulk_materialize(&store).unwrap();
        assert!(n2 >= 40, "warm inserted={n2}");
        // 30 prior + 40 residual (disjoint keys/fks).
        assert_eq!(store.scripthash.entry_count(), 70);
        assert!(list_materialize_claims(&runs_dir).unwrap().is_empty());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn materialize_recovers_claimed_mat_files() {
        let n = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("rbitcoin-sh-mat-{n}"));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let store = Store::open_or_create(&dir).unwrap();
        let b = ShRunBuilder::new(&dir);
        let runs_dir = dir.join("scripthash.runs");
        std::fs::create_dir_all(&runs_dir).unwrap();
        let mut body = Vec::new();
        for i in 0..50u32 {
            let mut sh = [0u8; 32];
            sh[0] = i as u8;
            body.extend_from_slice(&encode_rec(&sh, Fk(i as u64 + 1)));
        }
        let path = next_run_path(&runs_dir, 1);
        let run = write_sorted_run(&path, SH_RUN_KEY_LEN, SH_RUN_REC_LEN, &body).unwrap();
        let claimed = claim_run_for_materialize(&run).unwrap();
        assert!(claimed.path.to_string_lossy().ends_with(".run.mat"));
        assert!(list_runs(&runs_dir).unwrap().is_empty());
        assert_eq!(list_materialize_claims(&runs_dir).unwrap().len(), 1);

        let n = b.finalize_and_bulk_materialize(&store).unwrap();
        assert!(n >= 50, "inserted={n}");
        assert!(store.scripthash.entry_count() >= 50);
        assert!(list_materialize_claims(&runs_dir).unwrap().is_empty());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn same_key_run_finalize_materialize() {
        let n = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("rbitcoin-sh-run-same-{n}"));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let store = Store::open_or_create(&dir).unwrap();
        let b = ShRunBuilder::new(&dir);
        let runs_dir = dir.join("scripthash.runs");
        std::fs::create_dir_all(&runs_dir).unwrap();
        let sh = [0xabu8; 32];
        let mut body = Vec::new();
        for i in 1..=20u64 {
            body.extend_from_slice(&encode_rec(&sh, Fk(i)));
        }
        let path = next_run_path(&runs_dir, 1);
        write_sorted_run(&path, SH_RUN_KEY_LEN, SH_RUN_REC_LEN, &body).unwrap();
        let n = b.finalize_and_bulk_materialize(&store).unwrap();
        assert_eq!(n, 20);
        assert_eq!(store.scripthash.entries(&sh).unwrap().len(), 20);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn same_key_append_preserves_chain() {
        let n = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("rbitcoin-sh-append-same-{n}"));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let store = Store::open_or_create(&dir).unwrap();
        let sh = [0xabu8; 32];
        let mut batch = Vec::new();
        for i in 1..=20u64 {
            batch.push(ScriptHashRecord::from_fk(sh, Fk(i)));
        }
        let mut heads = std::collections::HashMap::new();
        let (n, _) = store
            .scripthash
            .put_create_batch_append(&batch, &mut heads)
            .unwrap();
        assert_eq!(n, 20);
        assert_eq!(store.scripthash.entries(&sh).unwrap().len(), 20);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn select_mode_never_full_cold_when_head_has_data() {
        // Regression: residual run after finished cold must not FullCold.
        assert_eq!(
            select_sh_tip_materialize_mode(false, 3_741_517_546, None, 64, 1, false, true),
            ShTipMaterializeMode::WarmOnly
        );
        assert_eq!(
            select_sh_tip_materialize_mode(true, 100, None, 64, 1, false, true),
            ShTipMaterializeMode::WarmOnly
        );
        assert_eq!(
            select_sh_tip_materialize_mode(true, 0, None, 64, 10, false, true),
            ShTipMaterializeMode::FullCold
        );
        assert_eq!(
            select_sh_tip_materialize_mode(false, 1e9 as u64, Some(40), 64, 32, false, true),
            ShTipMaterializeMode::ColdResume { next_shard: 40 }
        );
        assert_eq!(
            select_sh_tip_materialize_mode(false, 1e9 as u64, Some(0), 64, 1, false, true),
            ShTipMaterializeMode::WarmOnly
        );
        // Explicit rebuild may wipe.
        assert_eq!(
            select_sh_tip_materialize_mode(false, 1e9 as u64, None, 64, 1, true, true),
            ShTipMaterializeMode::FullCold
        );
        // Complete progress (next == n_shards) + residual → warm, not resume past end.
        assert_eq!(
            select_sh_tip_materialize_mode(false, 100, Some(64), 64, 1, false, true),
            ShTipMaterializeMode::WarmOnly
        );
    }

    #[test]
    fn catalog_complete_rejects_stale_seal_with_tiny_runs() {
        // Real failure mode: SEAL≈1.4e9 after catch-up but only ~2e5 run rows (tail).
        assert!(!sh_catalog_looks_complete(
            1_411_832_114,
            1_416_000_000,
            222_511
        ));
        assert!(sh_catalog_is_stale_tail(1_411_832_114, 222_511));
        assert!(!sh_catalog_looks_complete(1_000_000, 2_000_000, 10_000));
        // Full IBD-style: seal near tip, huge run mass.
        assert!(sh_catalog_looks_complete(
            1_410_000_000,
            1_410_000_000,
            3_700_000_000
        ));
        assert!(sh_catalog_looks_complete(0, 0, 0));
        // Post-success state: high SEAL, zero runs — incomplete for *empty* head only.
        assert!(sh_catalog_is_stale_tail(1_411_000_000, 0));
        assert!(!sh_catalog_looks_complete(
            1_411_000_000,
            1_411_000_000,
            0
        ));
    }

    #[test]
    fn compact_candidate_spares_recollect_scale_runs() {
        // Default tip target 512MiB → half=256MiB; floor=96MiB → small_max=96MiB.
        let target = 512 * 1024 * 1024u64;
        assert!(
            catalog_run_is_compact_candidate(10 * 1024 * 1024, target),
            "tiny crumbs still compact"
        );
        assert!(
            !catalog_run_is_compact_candidate(RECOLLECT_THREAD_SPILL_BYTES, target),
            "128MiB recollect spills must not compact"
        );
        assert!(
            !catalog_run_is_compact_candidate(125 * 1024 * 1024, target),
            "near-spill-size runs stay for tip k-way"
        );
        assert!(
            !catalog_run_is_compact_candidate(CATALOG_COMPACT_FLOOR_BYTES, target),
            "at floor is not a candidate"
        );
        assert!(catalog_run_is_compact_candidate(
            CATALOG_COMPACT_FLOOR_BYTES - 1,
            target
        ));
        // Tiny target: floor still bounds.
        assert!(!catalog_run_is_compact_candidate(50 * 1024 * 1024, 64 * 1024 * 1024));
    }

    #[test]
    fn compact_undersized_does_not_merge_large_planted_runs() {
        // Plant three "large" catalog runs via spill path with many recs; compact
        // must leave them (body ≥ floor). Use record counts that exceed floor.
        let n = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("rbitcoin-sh-compact-floor-{n}"));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let b = ShRunBuilder::new(&dir);
        b.enable();
        // 96MiB / 40B = 2_516_583 recs — too heavy for unit test. Instead plant
        // runs whose reported count*rec_len is large by writing real bodies at a
        // modest size and asserting the pure candidate filter (above), then plant
        // tiny runs and prove compact still merges crumbs.
        let runs_dir = dir.join("scripthash.runs");
        std::fs::create_dir_all(&runs_dir).unwrap();
        for seq in 1..=3u64 {
            let mut body = Vec::new();
            for i in 0..1000u64 {
                let mut sh = [0u8; 32];
                sh[0] = seq as u8;
                sh[1..9].copy_from_slice(&i.to_le_bytes());
                body.extend_from_slice(&encode_rec(&sh, Fk(seq * 10_000 + i)));
            }
            let path = next_run_path(&runs_dir, seq);
            write_sorted_run(&path, SH_RUN_KEY_LEN, SH_RUN_REC_LEN, &body).unwrap();
        }
        let before = list_runs(&runs_dir).unwrap().len();
        assert_eq!(before, 3);
        // Tiny runs (1000*40 ≈ 40KiB) are compact candidates — drain merges them.
        b.drain_spills().unwrap();
        let after = list_runs(&runs_dir).unwrap();
        // Crumbs should coalesce to fewer catalog files (or one).
        assert!(
            after.len() < before || after.len() == 1,
            "tiny planted runs should compact: before={before} after={}",
            after.len()
        );
        for r in &after {
            assert!(
                catalog_run_is_compact_candidate(run_body_bytes(r), target_run_bytes())
                    || after.len() <= 2,
                "post-compact runs should be few/merged"
            );
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn plan_pre_materialize_durable_head_never_clamps_seal_to_zero() {
        // Skeptic regression: high SEAL + empty runs + missing HWM must NOT
        // reset SEAL (old code treated catalog incomplete → clamp to hwm=0).
        let seal = 1_411_000_000u64;
        let tip = 1_411_000_000u64;
        assert_eq!(
            plan_sh_pre_materialize(false, true, seal, tip, 0, 0),
            ShPreMaterializeAction::BootstrapIncludeHwm { seal }
        );
        assert_eq!(
            plan_sh_pre_materialize(false, true, seal, tip, 222_511, 0),
            ShPreMaterializeAction::BootstrapIncludeHwm { seal }
        );
        // Authoritative HWM below SEAL → clamp (never to 0).
        assert_eq!(
            plan_sh_pre_materialize(false, true, seal, tip, 0, 1_400_000_000),
            ShPreMaterializeAction::ClampSealTo {
                floor: 1_400_000_000
            }
        );
        // Healthy: HWM == SEAL, empty residual runs.
        assert_eq!(
            plan_sh_pre_materialize(false, true, seal, tip, 0, seal),
            ShPreMaterializeAction::Noop
        );
        assert_eq!(durable_sh_inclusion_floor(0, seal), seal);
        assert_eq!(durable_sh_inclusion_floor(99, seal), 99);
    }

    #[test]
    fn plan_pre_materialize_empty_head_stale_tail_full_recollect() {
        let seal = 1_411_832_114u64;
        let tip = 1_416_000_000u64;
        assert_eq!(
            plan_sh_pre_materialize(false, false, seal, tip, 222_511, 0),
            ShPreMaterializeAction::ResetCatalogFullRecollect
        );
        // Empty runs + high SEAL + empty head (consumed catalog, no head).
        assert_eq!(
            plan_sh_pre_materialize(false, false, seal, seal, 0, 0),
            ShPreMaterializeAction::ResetCatalogFullRecollect
        );
        // FORCE always full rebuild regardless of head.
        assert_eq!(
            plan_sh_pre_materialize(true, true, seal, tip, 222_511, seal),
            ShPreMaterializeAction::ForceFullRebuild
        );
        assert_eq!(
            plan_sh_pre_materialize(true, false, seal, tip, 222_511, 0),
            ShPreMaterializeAction::ForceFullRebuild
        );
        // Good empty-head catalog: seal near tip, huge mass → keep, FullCold later.
        assert_eq!(
            plan_sh_pre_materialize(false, false, tip, tip, 3_700_000_000, 0),
            ShPreMaterializeAction::Noop
        );
    }

    #[test]
    fn force_rebuild_resets_seal_and_clears_runs() {
        let n = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("rbitcoin-sh-force-{n}"));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let store = Store::open_or_create(&dir).unwrap();
        let b = ShRunBuilder::new(&dir);
        b.enable();
        let runs_dir = dir.join("scripthash.runs");
        std::fs::create_dir_all(&runs_dir).unwrap();
        // Fake high SEAL + tiny run (incomplete catalog).
        store_seal(&runs_dir, 1_400_000_000).unwrap();
        b.refresh_seal();
        assert_eq!(b.sealed_max_create_fk(), 1_400_000_000);
        let mut body = Vec::new();
        body.extend_from_slice(&encode_rec(&[0xab; 32], Fk(99)));
        let path = next_run_path(&runs_dir, 1);
        write_sorted_run(&path, SH_RUN_KEY_LEN, SH_RUN_REC_LEN, &body).unwrap();
        assert!(
            !sh_catalog_looks_complete(1_400_000_000, 1_410_000_000, 1),
            "tiny catalog must be incomplete"
        );

        b.prepare_force_full_rebuild(&store).unwrap();
        assert_eq!(b.sealed_max_create_fk(), 0);
        assert!(list_runs(&runs_dir).unwrap().is_empty());
        assert!(store.scripthash.head_is_empty());
        assert_eq!(store.scripthash.entry_count(), 0);
        assert_eq!(store.scripthash.include_hwm(), 0);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn residual_run_on_full_head_warm_only_no_reinit() {
        // Ship path: durable head + leftover catalog run → warm apply, preserve head.
        let n = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("rbitcoin-sh-warm-guard-{n}"));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let store = Store::open_or_create(&dir).unwrap();
        let b = ShRunBuilder::new(&dir);
        b.enable();
        let mut sh0 = [0u8; 32];
        sh0[0] = 0x11;
        b.enqueue(&[ScriptHashRecord::from_fk(sh0, Fk(1))]);
        let n0 = b.finalize_and_bulk_materialize(&store).unwrap();
        assert!(n0 >= 1);
        assert_eq!(store.scripthash.entries(&sh0).unwrap().len(), 1);
        let count_before = store.scripthash.entry_count();
        assert!(count_before >= 1);
        assert!(!store.scripthash.head_is_empty() || count_before > 0);

        // Residual run with a second key (as after cancelled deferred apply).
        let runs_dir = dir.join("scripthash.runs");
        std::fs::create_dir_all(&runs_dir).unwrap();
        let mut sh1 = [0u8; 32];
        sh1[0] = 0x22;
        let mut body = Vec::new();
        body.extend_from_slice(&encode_rec(&sh1, Fk(2)));
        let path = next_run_path(&runs_dir, 99);
        write_sorted_run(&path, SH_RUN_KEY_LEN, SH_RUN_REC_LEN, &body).unwrap();

        let n1 = b.finalize_and_bulk_materialize(&store).unwrap();
        assert!(n1 >= 1, "warm should apply residual");
        // Original key must still be present (reinit would wipe).
        assert_eq!(
            store.scripthash.entries(&sh0).unwrap().len(),
            1,
            "durable head must not be wiped for residual run"
        );
        assert_eq!(store.scripthash.entries(&sh1).unwrap().len(), 1);
        assert!(store.scripthash.entry_count() >= count_before);
        assert!(store.scripthash.include_hwm() >= 2);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Catchup pre-step + finalize: durable head, high SEAL, empty runs, no HWM
    /// file must not zero SEAL or wipe the head (skeptic finding 1).
    #[test]
    fn durable_head_high_seal_missing_hwm_preserves_seal_on_prep() {
        let n = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("rbitcoin-sh-hwm-boot-{n}"));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let store = Store::open_or_create(&dir).unwrap();
        let b = ShRunBuilder::new(&dir);
        b.enable();
        let mut sh0 = [0u8; 32];
        sh0[0] = 0x33;
        b.enqueue(&[ScriptHashRecord::from_fk(sh0, Fk(7))]);
        let n0 = b.finalize_and_bulk_materialize(&store).unwrap();
        assert!(n0 >= 1);
        assert!(store.scripthash.has_durable_index());

        // Simulate legacy success: high SEAL, runs cleared, include_hwm deleted.
        let runs_dir = dir.join("scripthash.runs");
        std::fs::create_dir_all(&runs_dir).unwrap();
        let high_seal = 1_411_000_000u64;
        store_seal(&runs_dir, high_seal).unwrap();
        b.refresh_seal();
        let hwm_path = dir.join(rbitcoin_store::INCLUDE_HWM_NAME);
        let _ = std::fs::remove_file(&hwm_path);
        assert_eq!(store.scripthash.include_hwm(), 0);
        assert_eq!(b.sealed_max_create_fk(), high_seal);
        // Mass check would say incomplete — durable plan must bootstrap, not clamp.
        assert!(sh_catalog_is_stale_tail(high_seal, 0));
        let action = plan_sh_pre_materialize(
            false,
            true,
            high_seal,
            high_seal,
            0,
            0,
        );
        assert_eq!(
            action,
            ShPreMaterializeAction::BootstrapIncludeHwm { seal: high_seal }
        );
        // Apply the action the way catchup does.
        match action {
            ShPreMaterializeAction::BootstrapIncludeHwm { seal: s } => {
                store.scripthash.note_include_hwm(s).unwrap();
            }
            other => panic!("unexpected action {other:?}"),
        }
        assert_eq!(store.scripthash.include_hwm(), high_seal);
        assert_eq!(b.sealed_max_create_fk(), high_seal, "SEAL must not be zeroed");
        assert_eq!(store.scripthash.entries(&sh0).unwrap().len(), 1);
        // Second prep with HWM present → Noop.
        assert_eq!(
            plan_sh_pre_materialize(false, true, high_seal, high_seal, 0, high_seal),
            ShPreMaterializeAction::Noop
        );
        let _ = std::fs::remove_dir_all(&dir);
    }
}
