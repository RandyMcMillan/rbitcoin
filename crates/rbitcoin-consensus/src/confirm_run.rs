//! Multi-block confirm orchestrator (IBD Class C path).
//!
//! Pipeline (order is consensus-critical — do not reorder):
//! ```text
//! resolve_bodies → prewarm_wait → wave_fill → wire_rebuild
//!   → connect (headers + prevouts) → scripts → class_c
//!   → utxo_apply (catch-up only)
//! ```
//!
//! **Prewarm ownership:** the IBD background worker loads Class A into
//! [`ConfirmParentCache`]. Confirm **only waits** (Condvar notify on mark_scanned)
//! — it never last-miles / duplicates prewarm work while the worker is live.
//! Unit tests without a worker still call `prewarm_parents_for_heights` once
//! (sole owner of the load).
//!
//! Timers: [`crate::confirm_phase_stats`]. Multi-block order: sequential connect,
//! one script wave, one Class C run, one UTXO flush.

use crate::block::{
    bip34_height_script, block_has_witness, connect_block_prevouts, ScriptCheckJob,
    ValidationContext,
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

/// Connect output for one height (held until Class C + spend annotate).
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
    check_scripts: bool,
    time: u32,
    bits: bitcoin::CompactTarget,
    /// Header hash of this block (prev-link for the next height in the run).
    hash: [u8; 32],
}

/// Confirm a contiguous tip-extension run of archived bodies.
///
/// When archive leads tip (the post-milestone IBD case), a multi-height run:
/// 1. Connects sequentially (run-local pending spends — no spend poison on failure)
/// 2. Runs **one** rayon script wave across **all** inputs in the run
/// 3. Class C in height order
///
/// Intermediate heights are not yet in `confirmed[]`, so header checks after the
/// first use the previous block in the run (not `header_at_height`).
pub fn confirm_archived_run(
    query: &Query,
    params: &ChainParams,
    milestone: Milestone,
    blocks: &[(Height, [u8; 32])],
) -> Result<Vec<rbitcoin_primitives::Fk>, ConsensusError> {
    if blocks.is_empty() {
        return Ok(Vec::new());
    }
    query
        .ensure_spent_oracle_ready()
        .map_err(ConsensusError::Store)?;
    for w in blocks.windows(2) {
        if w[1].0 .0 != w[0].0 .0.saturating_add(1) {
            return Err(ConsensusError::BadBlock("confirm run not contiguous"));
        }
    }

    // ── 1. resolve_bodies ───────────────────────────────────────────────────
    let t_resolve = Instant::now();
    let metas = resolve_body_metas(query, blocks)?;
    confirm_phase_stats::RESOLVE_NS.fetch_add(
        t_resolve.elapsed().as_nanos() as u64,
        Ordering::Relaxed,
    );

    // ── 1b. wait for parent prewarm (worker owns Class A load) ─────────────
    // Confirm never duplicates prewarm: with a live worker we only wait for
    // ready notify; without a worker (unit tests) we are the sole prewarm owner.
    let heights: Vec<u32> = metas.iter().map(|m| m.height.0).collect();
    let items: Vec<(u32, [u8; 32])> = metas.iter().map(|m| (m.height.0, m.hash)).collect();
    let batch_end = heights.last().copied().unwrap_or(0);
    let t_pw = Instant::now();
    wait_for_prewarm(query, &heights, &items, batch_end)?;
    confirm_phase_stats::PREWARM_WAIT_NS.fetch_add(
        t_pw.elapsed().as_nanos() as u64,
        Ordering::Relaxed,
    );

    // ── 2. wave_fill (reuse resolve tx_fks — no header re-probe) ────────────
    let mut wave_prevouts = wave_fill(query, &metas)?;

    // ── 3. wire_rebuild (reuses wave body decodes — no second Class A parse) ─
    let wire_blocks = wire_rebuild(query, &metas, &mut wave_prevouts)?;

    // ── 4. connect (headers + prevouts; run-local pending) ──────────────────
    let mut prepared = connect_run(
        query,
        params,
        milestone,
        metas,
        &wire_blocks,
        &wave_prevouts,
    )?;

    // ── 5. scripts (one wave for the whole run) ─────────────────────────────
    script_wave(&prepared)?;

    // ── 6. class_c ──────────────────────────────────────────────────────────
    let n_blocks = prepared.len();
    let out = class_c_commit(query, &mut prepared)?;

    // ── 7. utxo_apply (catch-up) + runway unpin/tip GC ───────────────────────
    post_commit(query, &prepared)?;

    confirm_phase_stats::BLOCKS.fetch_add(n_blocks as u64, Ordering::Relaxed);
    Ok(out)
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
        blks.push(
            query
                .reconstruct_archived_block_from_parts_wave(
                    m.header_rec.clone(),
                    m.tx_fks.clone(),
                    Some(wave),
                )
                .map_err(ConsensusError::Store)?,
        );
    }
    let ns = t0.elapsed().as_nanos() as u64;
    confirm_phase_stats::RECONSTRUCT_WIRE_NS.fetch_add(ns, Ordering::Relaxed);
    confirm_phase_stats::RECONSTRUCT_NS.fetch_add(ns, Ordering::Relaxed);
    Ok(blks)
}

fn connect_run(
    query: &Query,
    params: &ChainParams,
    milestone: Milestone,
    metas: Vec<BodyMeta>,
    wire_blocks: &[Block],
    wave_prevouts: &rbitcoin_query::WavePrevoutCache,
) -> Result<Vec<Prepared>, ConsensusError> {
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
            validate_header(query, params, height, &block.header)?;
            if height.0 >= 1 {
                let prev_h = Height(height.0 - 1);
                let start = prev_h.0.saturating_sub(10);
                for h in start..=prev_h.0 {
                    let (_fk, rec) = query
                        .header_at_height(Height(h))
                        .map_err(ConsensusError::Store)?
                        .ok_or(ConsensusError::BadPrev)?;
                    time_window.push(rec.timestamp);
                }
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
        // Core/Inquisition: mainnet BIP34@227931 segwit@481824; signet both @1.
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
        let (script_jobs, spends) = connect_block_prevouts(
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
            check_scripts: !milestone.skips_scripts_at(height.0),
            time: block.header.time,
            bits: block.header.bits,
            hash: block_hash,
        });
    }
    Ok(prepared)
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

    let t_unpin = Instant::now();
    let all_spends: Vec<([u8; 32], u32)> = prepared
        .iter()
        .flat_map(|p| p.spends.iter().map(|(t, v, _, _)| (*t, *v)))
        .collect();
    let _ = query.unpin_spent_parent_outs(&all_spends);
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
