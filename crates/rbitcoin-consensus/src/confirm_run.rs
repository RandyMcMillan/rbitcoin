//! Multi-block confirm orchestrator (IBD Class C path).
//!
//! Pipeline (optimistic scripts — assumevalid-shaped):
//! ```text
//! MATERIALIZE STAGE (ibd-confirm-materialize OS thread):
//!   prewarm_wait → resolve → wave → wire → assemble
//! SCRIPT STAGE (ibd-confirm OS thread + rayon):
//!   scripts only
//! WRITEBACK STAGE (ibd-confirm-writeback OS thread, FIFO):
//!   structural (spentness/maturity/subsidy) → class_c → spend annotate → tip GC
//! ```
//!
//! [`confirm_archived_run`] runs all stages synchronously (tests / tip path).
//! IBD pipelines the three stages so materialize(N+1) ∥ scripts(N) ∥ writeback(N−1).
//!
//! **Prewarm ownership:** the IBD background worker loads Class A into
//! [`ConfirmParentCache`]. Materialize **only waits** (Condvar notify on mark_scanned)
//! — it never last-miles / duplicates prewarm work while the worker is live.

use crate::block::{
    assemble_block_prevouts, bip34_height_script, block_has_witness, structural_validate_spends,
    ScriptCheckJob, ValidationContext,
};
use crate::error::ConsensusError;
use crate::header::{median_time_past_times, validate_header};
use crate::milestone::Milestone;
use crate::params::{genesis_block, ChainParams};
use crate::confirm_phase_stats;
use bitcoin::hashes::Hash;
use bitcoin::{Block, Target};
use rbitcoin_primitives::Height;
use rbitcoin_query::Query;
use rbitcoin_store::StoreError;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::Ordering;
use std::time::Instant;

/// One height resolved for the confirm wave (header + Class A body fks).
struct BodyMeta {
    height: Height,
    hash: [u8; 32],
    header_fk: rbitcoin_primitives::Fk,
    header_rec: rbitcoin_store::HeaderRecord,
    tx_fks: Vec<rbitcoin_primitives::Fk>,
}

/// Assemble output for one height (held through scripts → writeback).
struct Prepared {
    height: Height,
    header_fk: rbitcoin_primitives::Fk,
    tx_fks: Vec<rbitcoin_primitives::Fk>,
    jobs: Vec<ScriptCheckJob>,
    /// `(prev_txid, vout, spending_tx_fk, create_tx_fk)` — create_fk for Direct
    /// spend annotate without `tx.head`.
    spends: Vec<(
        [u8; 32],
        u32,
        rbitcoin_primitives::Fk,
        rbitcoin_primitives::Fk,
    )>,
    /// Total fees from assemble (for structural coinbase subsidy check).
    fees: i64,
    check_scripts: bool,
    time: u32,
    bits: bitcoin::CompactTarget,
    /// Header hash of this block (prev-link for the next height in the run).
    hash: [u8; 32],
}

/// Wire + assemble complete; script jobs still attached (not yet verified).
///
/// `Send` so IBD can hand off materialize → scripts threads.
pub struct MaterializedBatch {
    prepared: Vec<Prepared>,
    wire_blocks: Vec<Block>,
    wave_prevouts: rbitcoin_query::WavePrevoutCache,
}

/// Script-verified batch ready for ordered writeback (structural + Class C + spends).
///
/// `Send` so IBD can hand off scripts → writeback.
pub struct ScriptOkBatch {
    prepared: Vec<Prepared>,
    wire_blocks: Vec<Block>,
    wave_prevouts: rbitcoin_query::WavePrevoutCache,
}

/// Confirm a contiguous tip-extension run of archived bodies (sync all stages).
///
/// Prefer the split phases in IBD for pipeline overlap.
pub fn confirm_archived_run(
    query: &Query,
    params: &ChainParams,
    milestone: Milestone,
    blocks: &[(Height, [u8; 32])],
) -> Result<Vec<rbitcoin_primitives::Fk>, ConsensusError> {
    let mat = confirm_materialize_phase(query, params, milestone, blocks)?;
    let ok = confirm_scripts_phase(query, mat.batch)?;
    confirm_writeback_phase(query, params, milestone, ok.batch)
}

/// Outcome of materialize: batch ready for scripts + pure work wall.
pub struct ConfirmMaterializeOutcome {
    pub batch: MaterializedBatch,
    /// Resolve → wave → wire → assemble only (not prewarm Condvar wait).
    pub work_ns: u64,
}

/// Outcome of the script stage: ready batch + pure script wall.
pub struct ConfirmScriptOutcome {
    pub batch: ScriptOkBatch,
    /// Script verify only (when produced by [`confirm_scripts_phase`]).
    /// When produced by [`confirm_script_phase`], includes materialize work too.
    pub work_ns: u64,
}

/// MATERIALIZE STAGE: prewarm wait → resolve → wave → wire → assemble.
///
/// Does **not** run scripts, advance tip, or probe durable spentness (except
/// provisional same-run doubles during assemble).
///
/// Prewarm wait is tracked in [`confirm_phase_stats::PREWARM_WAIT_NS`] and
/// excluded from [`ConfirmMaterializeOutcome::work_ns`].
pub fn confirm_materialize_phase(
    query: &Query,
    params: &ChainParams,
    milestone: Milestone,
    blocks: &[(Height, [u8; 32])],
) -> Result<ConfirmMaterializeOutcome, ConsensusError> {
    if blocks.is_empty() {
        return Err(ConsensusError::BadBlock("empty confirm batch"));
    }
    query
        .ensure_spent_oracle_ready()
        .map_err(ConsensusError::Store)?;
    for w in blocks.windows(2) {
        if w[1].0 .0 != w[0].0 .0.saturating_add(1) {
            return Err(ConsensusError::BadBlock("confirm run not contiguous"));
        }
    }

    // Seed + wait **before** store access so cold plan/header probes are not
    // charged as materialize work and (with worker live) do not majflt.
    let heights: Vec<u32> = blocks.iter().map(|(h, _)| h.0).collect();
    let items: Vec<(u32, [u8; 32])> = blocks.iter().map(|(h, hash)| (h.0, *hash)).collect();
    let batch_end = heights.last().copied().unwrap_or(0);
    let t_pw = Instant::now();
    wait_for_prewarm(query, &heights, &items, batch_end)?;
    confirm_phase_stats::PREWARM_WAIT_NS.fetch_add(
        t_pw.elapsed().as_nanos() as u64,
        Ordering::Relaxed,
    );

    let t_work = Instant::now();
    let _majflt = ConfirmMajfltGuard::start(
        blocks.first().map(|b| b.0 .0).unwrap_or(0),
        blocks.len(),
    );

    // Prefer runway cache; store-fallback on miss (same work as 2-stage).
    let t_resolve = Instant::now();
    let metas = resolve_body_metas(query, blocks, false)?;
    confirm_phase_stats::RESOLVE_NS.fetch_add(
        t_resolve.elapsed().as_nanos() as u64,
        Ordering::Relaxed,
    );

    let mut wave_prevouts = wave_fill(query, &metas)?;
    let wire_blocks = wire_rebuild(query, &metas, &mut wave_prevouts)?;
    let prepared = assemble_run(
        query,
        params,
        milestone,
        metas,
        &wire_blocks,
        &wave_prevouts,
        false,
    )?;

    let work_ns = t_work.elapsed().as_nanos() as u64;
    Ok(ConfirmMaterializeOutcome {
        batch: MaterializedBatch {
            prepared,
            wire_blocks,
            wave_prevouts,
        },
        work_ns,
    })
}

/// SCRIPT STAGE: verify script jobs on a materialized batch (rayon).
///
/// Clears jobs after success so writeback carries spends/fees only.
pub fn confirm_scripts_phase(
    _query: &Query,
    mut batch: MaterializedBatch,
) -> Result<ConfirmScriptOutcome, ConsensusError> {
    let t_work = Instant::now();
    script_wave(&batch.prepared)?;
    for p in &mut batch.prepared {
        p.jobs.clear();
        p.jobs.shrink_to_fit();
    }
    let work_ns = t_work.elapsed().as_nanos() as u64;
    Ok(ConfirmScriptOutcome {
        batch: ScriptOkBatch {
            prepared: batch.prepared,
            wire_blocks: batch.wire_blocks,
            wave_prevouts: batch.wave_prevouts,
        },
        work_ns,
    })
}

/// MATERIALIZE + SCRIPTS in one call (tests / tip path / ChainHub compat).
///
/// Prewarm wait excluded from [`ConfirmScriptOutcome::work_ns`]; work is
/// materialize + scripts.
pub fn confirm_script_phase(
    query: &Query,
    params: &ChainParams,
    milestone: Milestone,
    blocks: &[(Height, [u8; 32])],
) -> Result<ConfirmScriptOutcome, ConsensusError> {
    let mat = confirm_materialize_phase(query, params, milestone, blocks)?;
    let mat_ns = mat.work_ns;
    let mut ok = confirm_scripts_phase(query, mat.batch)?;
    ok.work_ns = ok.work_ns.saturating_add(mat_ns);
    Ok(ok)
}

/// Keep only heights strictly above the confirmed tip (dup writeback race).
#[inline]
fn writeback_height_needed(tip: u32, height: u32) -> bool {
    height > tip
}

/// WRITEBACK STAGE: structural → class_c → spend annotate → tip GC (FIFO caller).
pub fn confirm_writeback_phase(
    query: &Query,
    params: &ChainParams,
    milestone: Milestone,
    mut batch: ScriptOkBatch,
) -> Result<Vec<rbitcoin_primitives::Fk>, ConsensusError> {
    // Idempotent: skip heights already on the confirmed tip (dup pipeline race).
    let tip = query.tip_height().map(|h| h.0).unwrap_or(0);
    let mut prep = Vec::with_capacity(batch.prepared.len());
    let mut wires = Vec::with_capacity(batch.wire_blocks.len());
    for (p, w) in batch
        .prepared
        .into_iter()
        .zip(batch.wire_blocks.into_iter())
    {
        if !writeback_height_needed(tip, p.height.0) {
            continue;
        }
        prep.push(p);
        wires.push(w);
    }
    if prep.is_empty() {
        return Ok(Vec::new());
    }
    batch.prepared = prep;
    batch.wire_blocks = wires;

    structural_run(
        query,
        params,
        milestone,
        &batch.prepared,
        &batch.wire_blocks,
        &batch.wave_prevouts,
    )?;
    let n_blocks = batch.prepared.len();
    let out = class_c_commit(query, &mut batch.prepared)?;
    post_commit(query, &batch.prepared)?;
    confirm_phase_stats::BLOCKS.fetch_add(n_blocks as u64, Ordering::Relaxed);
    Ok(out)
}

impl MaterializedBatch {
    /// Heights and header hashes in this batch (for events / feed scrub).
    pub fn heights_hashes(&self) -> Vec<(u32, [u8; 32])> {
        self.prepared
            .iter()
            .map(|p| (p.height.0, p.hash))
            .collect()
    }

    pub fn len(&self) -> usize {
        self.prepared.len()
    }

    pub fn is_empty(&self) -> bool {
        self.prepared.is_empty()
    }
}

impl ScriptOkBatch {
    /// Heights and header hashes in this batch (for events / feed scrub).
    pub fn heights_hashes(&self) -> Vec<(u32, [u8; 32])> {
        self.prepared
            .iter()
            .map(|p| (p.height.0, p.hash))
            .collect()
    }

    pub fn len(&self) -> usize {
        self.prepared.len()
    }

    pub fn is_empty(&self) -> bool {
        self.prepared.is_empty()
    }
}

/// Samples major page faults for the **calling** thread only (Linux).
///
/// Other threads (archive, prewarm, rayon script workers) do not count. Store
/// cold touches on the confirm OS thread show up here; `mlock` gaps → warn.
fn thread_majflt() -> Option<u64> {
    #[cfg(target_os = "linux")]
    {
        // RUSAGE_THREAD is Linux-specific (not in all libc feature sets as a
        // portable constant — use the numeric value 1 when needed).
        const RUSAGE_THREAD: libc::c_int = 1;
        let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
        let rc = unsafe { libc::getrusage(RUSAGE_THREAD, usage.as_mut_ptr()) };
        if rc != 0 {
            return None;
        }
        let u = unsafe { usage.assume_init() };
        Some(u.ru_majflt as u64)
    }
    #[cfg(not(target_os = "linux"))]
    {
        None
    }
}

/// On drop: warn if this confirm thread took major page faults during the run.
struct ConfirmMajfltGuard {
    before: Option<u64>,
    first_h: u32,
    n_blocks: usize,
    started: Instant,
}

impl ConfirmMajfltGuard {
    fn start(first_h: u32, n_blocks: usize) -> Self {
        Self {
            before: thread_majflt(),
            first_h,
            n_blocks,
            started: Instant::now(),
        }
    }
}

impl Drop for ConfirmMajfltGuard {
    fn drop(&mut self) {
        let Some(before) = self.before else {
            return;
        };
        let Some(after) = thread_majflt() else {
            return;
        };
        if after <= before {
            return;
        }
        let delta = after - before;
        rbitcoin_log::warn!(
            "confirm: major page fault(s) on confirm thread delta={delta} first_h={} n={} elapsed_ms={} (cold store touch; prewarm/mlock gap)",
            self.first_h,
            self.n_blocks,
            self.started.elapsed().as_millis(),
        );
    }
}

#[cfg(test)]
mod majflt_tests {
    use super::*;

    #[test]
    fn thread_majflt_samples_on_linux() {
        #[cfg(target_os = "linux")]
        {
            assert!(thread_majflt().is_some());
        }
        #[cfg(not(target_os = "linux"))]
        {
            assert!(thread_majflt().is_none());
        }
    }

    #[test]
    fn majflt_guard_drop_does_not_panic() {
        let g = ConfirmMajfltGuard::start(0, 1);
        drop(g);
    }
}

#[cfg(test)]
mod writeback_idempotent_tests {
    use super::writeback_height_needed;

    /// Heights at or below tip must be stripped before structural writeback
    /// (dup pipeline race after scripts claim the same tip+1 twice).
    #[test]
    fn filter_keeps_only_heights_above_tip() {
        let tip = 100u32;
        let heights = [98u32, 99, 100, 101, 102];
        let kept: Vec<u32> = heights
            .into_iter()
            .filter(|&h| writeback_height_needed(tip, h))
            .collect();
        assert_eq!(kept, vec![101, 102]);
        assert!(!writeback_height_needed(tip, tip));
        assert!(!writeback_height_needed(0, 0));
        assert!(writeback_height_needed(0, 1));
    }

    #[test]
    fn three_stage_entry_points_exist() {
        // Materialize / scripts / writeback are separate public surfaces for IBD.
        let _m = super::confirm_materialize_phase;
        let _s = super::confirm_scripts_phase;
        let _w = super::confirm_writeback_phase;
        let _combined = super::confirm_script_phase;
        let _sync = super::confirm_archived_run;
    }
}



// ─── phases ───────────────────────────────────────────────────────────────────

/// Wait for the confirm batch to be prewarm-ready.
///
/// - **Worker live (IBD):** only wait on ready Condvar — never call
///   `prewarm_parents_for_heights` (would duplicate the worker).
/// - **No worker (unit tests):** sole owner — prewarm the batch once, then proceed.
///
/// Headroom beyond the batch is soft (best-effort).
fn wait_for_prewarm(
    query: &Query,
    heights: &[u32],
    items: &[(u32, [u8; 32])],
    batch_end: u32,
) -> Result<(), ConsensusError> {
    if heights.is_empty() {
        return Ok(());
    }
    // Seed plans so the worker knows which heights to mark ready.
    query.seed_parent_runway(items);

    if query.prewarm_worker_live() {
        // Wait only — worker does all Class A load / decode / mlock.
        query
            .wait_prewarm_ready_with_headroom(
                heights,
                batch_end,
                None,
                std::time::Duration::from_secs(600),
            )
            .map_err(ConsensusError::Store)?;
    } else {
        // No background worker (tests): we own prewarm exclusively.
        query
            .prewarm_parents_for_heights(items)
            .map_err(ConsensusError::Store)?;
    }
    Ok(())
}

fn resolve_body_metas(
    query: &Query,
    blocks: &[(Height, [u8; 32])],
    cache_only: bool,
) -> Result<Vec<BodyMeta>, ConsensusError> {
    let mut metas = Vec::with_capacity(blocks.len());
    for &(height, hash) in blocks {
        // Prefer prewarm runway cache (header + header_txs, no store page faults).
        if let Some(plan) = query.confirm_parent_cache().get_header_plan(height.0) {
            if plan.header_rec.hash == hash {
                metas.push(BodyMeta {
                    height,
                    hash,
                    header_fk: plan.header_fk,
                    header_rec: plan.header_rec,
                    tx_fks: plan.tx_fks,
                });
                continue;
            }
        }
        if cache_only {
            return Err(ConsensusError::Store(StoreError::Corrupt(
                "confirm: prewarm incomplete (header plan missing after wait)",
            )));
        }
        let (header_fk, header_rec) = query
            .get_header_by_hash(&hash)
            .map_err(ConsensusError::Store)?
            .ok_or(ConsensusError::Store(StoreError::NotFound))?;
        let tx_fks = query
            .header_tx_fks(header_fk, Some(&hash))
            .map_err(ConsensusError::Store)?
            .ok_or(ConsensusError::Store(StoreError::Corrupt(
                "confirm without archived body",
            )))?;
        metas.push(BodyMeta {
            height,
            hash,
            header_fk,
            header_rec,
            tx_fks,
        });
    }
    Ok(metas)
}

fn wave_fill(
    query: &Query,
    metas: &[BodyMeta],
) -> Result<rbitcoin_query::WavePrevoutCache, ConsensusError> {
    // Prefetch Class A is a no-op (bodies live in ConfirmParentCache). Leave
    // PREFETCH_CLASS_A_NS at 0 so perf still shows p=0 cleanly.
    let t0 = Instant::now();
    let lists: Vec<&[rbitcoin_primitives::Fk]> =
        metas.iter().map(|m| m.tx_fks.as_slice()).collect();
    let (_n, wave) = query
        .wave_fill_for_tx_fk_lists(&lists)
        .map_err(ConsensusError::Store)?;
    let ns = t0.elapsed().as_nanos() as u64;
    confirm_phase_stats::WAVE_FILL_NS.fetch_add(ns, Ordering::Relaxed);
    confirm_phase_stats::RECONSTRUCT_NS.fetch_add(ns, Ordering::Relaxed);
    Ok(wave)
}

fn wire_rebuild(
    query: &Query,
    metas: &[BodyMeta],
    wave: &mut rbitcoin_query::WavePrevoutCache,
) -> Result<Vec<Block>, ConsensusError> {
    // Sequential by design: `rayon_audit` benches show par_iter reconstruct is
    // *slower* than sequential for 1–128 blocks. Wave-fill already decoded Class A
    // bodies once — wire rebuild takes those and only builds `bitcoin::Transaction`.
    let t0 = Instant::now();
    let mut blks = Vec::with_capacity(metas.len());
    for m in metas {
        let prev_hash = query
            .confirm_parent_cache()
            .get_header_plan(m.height.0)
            .map(|p| p.prev_hash);
        blks.push(
            query
                .reconstruct_archived_block_from_parts_wave(
                    m.header_rec.clone(),
                    m.tx_fks.clone(),
                    Some(wave),
                    prev_hash,
                )
                .map_err(ConsensusError::Store)?,
        );
    }
    let ns = t0.elapsed().as_nanos() as u64;
    confirm_phase_stats::RECONSTRUCT_WIRE_NS.fetch_add(ns, Ordering::Relaxed);
    confirm_phase_stats::RECONSTRUCT_NS.fetch_add(ns, Ordering::Relaxed);
    Ok(blks)
}

fn assemble_run(
    query: &Query,
    params: &ChainParams,
    milestone: Milestone,
    metas: Vec<BodyMeta>,
    wire_blocks: &[Block],
    wave_prevouts: &rbitcoin_query::WavePrevoutCache,
    cache_only: bool,
) -> Result<Vec<Prepared>, ConsensusError> {
    // Provisional same-run double-spend only (not durable spentness).
    let mut pending_spent: HashSet<([u8; 32], u32)> = HashSet::new();
    let mut pending_creates: HashMap<([u8; 32], u32), rbitcoin_primitives::Fk> = HashMap::new();
    let mut time_window: Vec<u32> = Vec::with_capacity(11);
    let mut prepared: Vec<Prepared> = Vec::with_capacity(metas.len());

    for (i, meta) in metas.into_iter().enumerate() {
        let block = &wire_blocks[i];
        let height = meta.height;
        let block_hash = meta.hash;
        let ctx = ValidationContext::at(params, height, milestone);

        if i == 0 {
            // MTP + prev link: prefer runway header plans (no header.body fault).
            if height.0 >= 1 {
                let prev_h = Height(height.0 - 1);
                let start = prev_h.0.saturating_sub(10);
                let mut from_plans = true;
                let mut times = Vec::with_capacity(11);
                for h in start..=prev_h.0 {
                    if let Some(plan) = query.confirm_parent_cache().get_header_plan(h) {
                        times.push(plan.header_rec.timestamp);
                        if h == prev_h.0
                            && plan.header_rec.hash != block.header.prev_blockhash.to_byte_array()
                        {
                            return Err(ConsensusError::BadPrev);
                        }
                    } else {
                        from_plans = false;
                        break;
                    }
                }
                if from_plans {
                    let mtp = median_time_past_times(&times);
                    if block.header.time <= mtp {
                        return Err(ConsensusError::BadHeader("timestamp <= median-time-past"));
                    }
                    time_window = times;
                } else {
                    // Prior tip headers may not be on the prewarm runway (only
                    // tip+1..). Header rows are tiny fixed records — use store.
                    // Class A body/parent cold paths remain cache-only above.
                    let _ = cache_only;
                    validate_header(query, params, height, &block.header)?;
                    for h in start..=prev_h.0 {
                        let (_fk, rec) = query
                            .header_at_height(Height(h))
                            .map_err(ConsensusError::Store)?
                            .ok_or(ConsensusError::BadPrev)?;
                        time_window.push(rec.timestamp);
                    }
                }
            } else {
                validate_header(query, params, height, &block.header)?;
            }
        } else {
            let prev = &prepared[i - 1];
            if block.header.prev_blockhash.to_byte_array() != prev.hash {
                return Err(ConsensusError::BadPrev);
            }
            let mtp = median_time_past_times(&time_window);
            if block.header.time <= mtp {
                return Err(ConsensusError::BadHeader("timestamp <= median-time-past"));
            }
            if let Some(cp) = params.checkpoint_at(height) {
                if cp != block.header.block_hash() {
                    return Err(ConsensusError::BadHeader("checkpoint mismatch"));
                }
            }
            let expected = expected_bits_extending(query, params, height, prev.bits, prev.time)?;
            if block.header.bits != expected {
                return Err(ConsensusError::BadHeader("incorrect proof of work bits"));
            }
            let target = Target::from_compact(block.header.bits);
            if target > params.pow_limit {
                return Err(ConsensusError::BadHeader("target above pow limit"));
            }
            block
                .header
                .validate_pow(target)
                .map_err(|_| ConsensusError::InvalidPow)?;
        }

        // Height-gated structure soft forks (archive prep skipped these).
        if params.bip34_active_at(height.0) {
            check_bip34(block, height.0)?;
        }
        if block_has_witness(block) && !params.segwit_active_at(height.0) {
            return Err(ConsensusError::BadBlock("unexpected witness before segwit"));
        }

        // BIP325: full signet challenge on tip confirm only.
        if height.0 > 0 {
            if let Some(challenge) = params.signet_challenge.as_ref() {
                crate::signet::validate_signet_block_solution(block, challenge.as_script())?;
            }
        }

        let t_connect = Instant::now();
        let (script_jobs, spends, fees) = assemble_block_prevouts(
            query,
            block,
            &ctx,
            Some(&meta.tx_fks),
            Some(wave_prevouts),
            &mut pending_spent,
            &mut pending_creates,
        )?;
        confirm_phase_stats::CONNECT_NS
            .fetch_add(t_connect.elapsed().as_nanos() as u64, Ordering::Relaxed);

        time_window.push(block.header.time);
        if time_window.len() > 11 {
            let n = time_window.len() - 11;
            time_window.drain(0..n);
        }

        prepared.push(Prepared {
            height,
            header_fk: meta.header_fk,
            tx_fks: meta.tx_fks,
            jobs: script_jobs,
            spends,
            fees,
            check_scripts: !milestone.skips_scripts_at(height.0),
            time: block.header.time,
            bits: block.header.bits,
            hash: block_hash,
        });
    }
    Ok(prepared)
}

/// Durable spentness + maturity + subsidy after scripts (height order).
fn structural_run(
    query: &Query,
    params: &ChainParams,
    milestone: Milestone,
    prepared: &[Prepared],
    wire_blocks: &[Block],
    wave_prevouts: &rbitcoin_query::WavePrevoutCache,
) -> Result<(), ConsensusError> {
    let t0 = Instant::now();
    let mut pending_spent: HashSet<([u8; 32], u32)> = HashSet::new();
    for (i, p) in prepared.iter().enumerate() {
        let ctx = ValidationContext::at(params, p.height, milestone);
        structural_validate_spends(
            query,
            &wire_blocks[i],
            &ctx,
            Some(&p.tx_fks),
            Some(wave_prevouts),
            &p.spends,
            p.fees,
            &mut pending_spent,
        )?;
    }
    confirm_phase_stats::STRUCTURAL_NS
        .fetch_add(t0.elapsed().as_nanos() as u64, Ordering::Relaxed);
    Ok(())
}

fn script_wave(prepared: &[Prepared]) -> Result<(), ConsensusError> {
    let t_script = Instant::now();
    {
        let mut n_slices = 0usize;
        let mut total_jobs = 0usize;
        let mut only: Option<&[ScriptCheckJob]> = None;
        for p in prepared {
            if p.check_scripts && !p.jobs.is_empty() {
                n_slices += 1;
                total_jobs += p.jobs.len();
                only = Some(p.jobs.as_slice());
            }
        }
        match n_slices {
            0 => {}
            1 => crate::block::verify_scripts_pool(only.unwrap())?,
            _ => {
                let mut all_jobs: Vec<&ScriptCheckJob> = Vec::with_capacity(total_jobs);
                for p in prepared {
                    if p.check_scripts {
                        all_jobs.extend(p.jobs.iter());
                    }
                }
                crate::block::verify_scripts_pool_jobs(&all_jobs)?;
            }
        }
    }
    confirm_phase_stats::SCRIPT_NS
        .fetch_add(t_script.elapsed().as_nanos() as u64, Ordering::Relaxed);
    Ok(())
}

fn class_c_commit(
    query: &Query,
    prepared: &mut [Prepared],
) -> Result<Vec<rbitcoin_primitives::Fk>, ConsensusError> {
    let t_class_c = Instant::now();
    let items: Vec<rbitcoin_query::ConfirmPrepared> = prepared
        .iter_mut()
        .map(|p| rbitcoin_query::ConfirmPrepared {
            height: p.height,
            header_fk: p.header_fk,
            tx_fks: std::mem::take(&mut p.tx_fks),
        })
        .collect();
    let out = query
        .confirm_blocks_run(&items)
        .map_err(ConsensusError::Store)?;
    confirm_phase_stats::CLASS_C_NS
        .fetch_add(t_class_c.elapsed().as_nanos() as u64, Ordering::Relaxed);
    Ok(out)
}

fn post_commit(query: &Query, prepared: &[Prepared]) -> Result<(), ConsensusError> {
    // Direct IBD: batch durable spend annotations for the whole run **before**
    // the next confirm batch (spentness = confirmed-strong + annotation).
    // Tip mode usually writes spends on archive; still safe if spend_index on.
    let t_spent = Instant::now();
    if query.spend_index_enabled() && query.index_mode().uses_durable_spends() {
        // Prefer create_fk + body range — no tx.head; ranged path skips tx.idx.
        // Resolve ranges once per unique create (cache or one idx probe), then
        // annotate almost entirely via the ranged batch API.
        use std::collections::{HashMap, HashSet};
        let mut pending: Vec<(
            rbitcoin_primitives::Fk,
            u32,
            rbitcoin_primitives::Fk,
        )> = Vec::new();
        let mut n_skip = 0u64;
        let mut unique_creates: HashSet<u64> = HashSet::new();
        for p in prepared {
            for &(_txid, vout, sfk, cfk) in &p.spends {
                if sfk.is_null() || cfk.is_null() {
                    n_skip = n_skip.saturating_add(1);
                    continue;
                }
                pending.push((cfk, vout, sfk));
                if let Some(id) = cfk.get() {
                    unique_creates.insert(id);
                }
            }
        }
        let mut range_by_create: HashMap<u64, (u64, u64)> =
            HashMap::with_capacity(unique_creates.len());
        let mut filled: Vec<(rbitcoin_primitives::Fk, u64, u64)> = Vec::new();
        for id in unique_creates {
            let fk = rbitcoin_primitives::Fk(id);
            if let Some(r) = query.confirm_parent_cache().get_body_range(fk) {
                range_by_create.insert(id, r);
            } else if let Ok(r) = query.store().tx_body_range(fk) {
                range_by_create.insert(id, r);
                filled.push((fk, r.0, r.1));
            }
        }
        if !filled.is_empty() {
            query.confirm_parent_cache().put_body_ranges_batch(&filled);
        }
        let mut ranged: Vec<(
            rbitcoin_primitives::Fk,
            u32,
            rbitcoin_primitives::Fk,
            u64,
            u64,
        )> = Vec::with_capacity(pending.len());
        let mut by_create: Vec<(
            rbitcoin_primitives::Fk,
            u32,
            rbitcoin_primitives::Fk,
        )> = Vec::new();
        for (cfk, vout, sfk) in pending {
            if let Some(id) = cfk.get() {
                if let Some(&(off, len)) = range_by_create.get(&id) {
                    ranged.push((cfk, vout, sfk, off, len));
                    continue;
                }
            }
            // Still no range (corrupt / missing body) — fall back to idx path.
            by_create.push((cfk, vout, sfk));
        }
        confirm_phase_stats::SPEND_ANNOTATE_RANGED
            .fetch_add(ranged.len() as u64, Ordering::Relaxed);
        confirm_phase_stats::SPEND_ANNOTATE_IDX
            .fetch_add(by_create.len() as u64, Ordering::Relaxed);
        confirm_phase_stats::SPEND_ANNOTATE_SKIP.fetch_add(n_skip, Ordering::Relaxed);
        if !ranged.is_empty() {
            query
                .store()
                .put_spend_batch_by_create_ranged(&ranged)
                .map_err(ConsensusError::Store)?;
        }
        if !by_create.is_empty() {
            query
                .store()
                .put_spend_batch_by_create(&by_create)
                .map_err(ConsensusError::Store)?;
        }
    }
    confirm_phase_stats::UTXO_APPLY_NS
        .fetch_add(t_spent.elapsed().as_nanos() as u64, Ordering::Relaxed);

    // IBD (Direct): skip per-spend unpin — tip GC drops the same runway outs.
    // Tip mode: still retire spent sparse parents so long-lived runway stays lean.
    let t_unpin = Instant::now();
    if query.index_mode() != rbitcoin_query::IndexMode::Direct {
        let all_spends: Vec<(rbitcoin_primitives::Fk, u32)> = prepared
            .iter()
            .flat_map(|p| {
                p.spends.iter().filter_map(|(_txid, vout, _sfk, cfk)| {
                    if cfk.is_null() {
                        None
                    } else {
                        Some((*cfk, *vout))
                    }
                })
            })
            .collect();
        let _ = query.unpin_spent_parent_outs(&all_spends);
    }
    confirm_phase_stats::UNPIN_NS.fetch_add(
        t_unpin.elapsed().as_nanos() as u64,
        Ordering::Relaxed,
    );

    // Prune confirm-parent runway for heights at/below new tip.
    if let Some(tip) = prepared.last().map(|p| p.height.0) {
        let t_tip = Instant::now();
        query.advance_parent_runway_tip(tip);
        confirm_phase_stats::RUNWAY_TIP_NS.fetch_add(
            t_tip.elapsed().as_nanos() as u64,
            Ordering::Relaxed,
        );
    }
    Ok(())
}

fn check_bip34(block: &Block, height: u32) -> Result<(), ConsensusError> {
    let coinbase = &block.txdata[0];
    let bytes = coinbase.input[0].script_sig.as_bytes();
    let expected = bip34_height_script(height);
    if bytes.len() < expected.len() || &bytes[..expected.len()] != expected.as_slice() {
        return Err(ConsensusError::BadBlock("bip34 height encoding"));
    }
    Ok(())
}

fn expected_bits_extending(
    query: &Query,
    params: &ChainParams,
    height: Height,
    prev_bits: bitcoin::CompactTarget,
    prev_time: u32,
) -> Result<bitcoin::CompactTarget, ConsensusError> {
    use bitcoin::CompactTarget;
    if height.0 == 0 {
        return Ok(genesis_block(params).header.bits);
    }
    let interval = params.difficulty_adjustment_interval();
    if params.no_pow_retargeting() || height.0 % interval != 0 {
        return Ok(prev_bits);
    }
    let first_height = Height(height.0 - interval);
    let (_fk, first_rec) = query
        .header_at_height(first_height)
        .map_err(ConsensusError::Store)?
        .ok_or(ConsensusError::BadHeader("missing retarget first header"))?;
    let timespan = prev_time.saturating_sub(first_rec.timestamp) as u64;
    Ok(CompactTarget::from_next_work_required(
        prev_bits,
        timespan,
        &params.btc,
    ))
}
