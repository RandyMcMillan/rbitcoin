//! Write / Class C commit stage.

use super::*;

pub(super) fn write_height_needed(tip: Option<u32>, height: u32) -> bool {
    match tip {
        None => true,
        Some(t) => height > t,
    }
}

/// COMMIT STAGE: optional Class A plan commit → structural → class_c → spend annotate → tip GC.
///
/// When `batch.archive_plan` is set (wire lookup/load path), Class A is appended in this
/// same stage before structural/annotate — single ordered commit era.
/// **Class A never leads tip** (no dual-track archive-ahead / body DONTNEED lead).
///
/// Accrues window timers in [`confirm_phase_stats`] and snapshots the last batch
/// for slow-write logs via [`confirm_phase_stats::last_write_phases`].
pub fn confirm_write_phase(
    query: &Query,
    params: &ChainParams,
    milestone: Milestone,
    mut batch: ScriptOkBatch,
) -> Result<Vec<rbitcoin_primitives::Fk>, ConsensusError> {
    // Idempotent: skip heights already on the confirmed tip (dup pipeline race).
    let tip = query.tip_height().map(|h| h.0);
    let mut kept = Vec::with_capacity(batch.prepared.len());
    let mut wires = Vec::with_capacity(batch.wire_blocks.len());
    for (p, w) in batch
        .prepared
        .into_iter()
        .zip(batch.wire_blocks.into_iter())
    {
        if !write_height_needed(tip, p.height.0) {
            continue;
        }
        kept.push(p);
        wires.push(w);
    }
    if kept.is_empty() {
        return Ok(Vec::new());
    }
    batch.prepared = kept;
    batch.wire_blocks = wires;

    let t_wall = Instant::now();

    // Single commit era: durable Class A for this batch before spentness RMW.
    // Keep create pins for SH collect (Class C) — same Arcs as layout fill; avoid
    // re-preading Class A bodies under RES=0 when residency is empty.
    let mut write_create_pins: FkMap<rbitcoin_query::CreatePin> = FkMap::default();
    let mut class_a_ns = 0u64;
    let mut ensure_ns = 0u64;
    if let Some(plan) = batch.archive_plan.take() {
        if !plan.is_empty() {
            // Shared CreatePin Arcs only (refcount) for post-commit layout fill —
            // no whole-plan packed deep clone of outs.
            let planned_fks = plan.planned_fks.clone();
            let pins: Vec<rbitcoin_query::CreatePin> =
                if plan.batch_pin.len() == plan.planned_fks.len() {
                    plan.batch_pin.iter().map(std::sync::Arc::clone).collect()
                } else {
                    plan.packed
                        .iter()
                        .map(|(pin, _)| std::sync::Arc::clone(pin))
                        .collect()
                };
            let t_ca = Instant::now();
            let committed = query
                .archive_commit_plan(plan)
                .map_err(ConsensusError::from)?;
            class_a_ns = t_ca.elapsed().as_nanos() as u64;
            // Layout + SH pins only after a real append. Idempotent skip (Class A
            // already present) uses store denserels via ensure / class_c cold pins.
            if committed {
                write_create_pins.reserve(planned_fks.len());
                for (fk, pin) in planned_fks.iter().zip(pins.iter()) {
                    write_create_pins.insert(*fk, std::sync::Arc::clone(pin));
                }
                let t_ens = Instant::now();
                fill_planned_create_layout_after_commit(
                    query,
                    &mut batch.batch_parents,
                    &planned_fks,
                    &pins,
                )?;
                ensure_ns = ensure_ns.saturating_add(t_ens.elapsed().as_nanos() as u64);
            }
        }
    }
    // Ensure denserels/abs for every spend edge before structural + annotate:
    // - load-ahead in-flight parents (no denserels at pin time)
    // - already-archived Class A (plan=None) same-batch creates never inserted
    // - partial pin after prior write committed Class A then failed annotate
    {
        let t_ens = Instant::now();
        ensure_spend_abs_layouts(query, &mut batch.batch_parents, &batch.prepared)?;
        ensure_ns = ensure_ns.saturating_add(t_ens.elapsed().as_nanos() as u64);
    }
    if class_a_ns > 0 {
        confirm_phase_stats::CLASS_A_NS.fetch_add(class_a_ns, Ordering::Relaxed);
    }
    if ensure_ns > 0 {
        confirm_phase_stats::ENSURE_LAYOUT_NS.fetch_add(ensure_ns, Ordering::Relaxed);
    }

    // Local Instant totals (not atomic deltas) — sample_and_reset races mid-batch.
    // Structural fills meta_by_abs for pure-write annotate (no second body pread).
    let mut meta_by_abs: rbitcoin_query::U64Map<(rbitcoin_primitives::Fk, u8)> =
        rbitcoin_query::U64Map::default();
    let t_struct = Instant::now();
    let struct_ph = structural_run(
        query,
        params,
        milestone,
        &batch.prepared,
        &batch.wire_blocks,
        &batch.batch_parents,
        &mut meta_by_abs,
    )?;
    let structural_ns = t_struct.elapsed().as_nanos() as u64;

    let n_blocks = batch.prepared.len();
    let cc0 = confirm_phase_stats::CLASS_C_NS.load(Ordering::Relaxed);
    let out = class_c_commit(query, &mut batch.prepared, &write_create_pins)?;
    // Tables only (strong+tip), matching CLASS_C_NS — not join wall / SH.
    let class_c_ns = confirm_phase_stats::CLASS_C_NS
        .load(Ordering::Relaxed)
        .saturating_sub(cc0);

    let (spend_ann_ns, tip_gc_ns) =
        post_commit(query, &batch.prepared, &batch.batch_parents, &meta_by_abs)?;

    // batch_parents dropped here with ScriptOkBatch — no tip GC of sparse pins.
    confirm_phase_stats::BLOCKS.fetch_add(n_blocks as u64, Ordering::Relaxed);
    confirm_phase_stats::note_last_write(confirm_phase_stats::LastWritePhases {
        n_blocks: n_blocks as u32,
        wall_ns: t_wall.elapsed().as_nanos() as u64,
        class_a_ns,
        ensure_ns,
        structural_ns,
        spent_ns: struct_ph.spent_ns,
        create_h_ns: struct_ph.create_h_ns,
        bip68_ns: struct_ph.bip68_ns,
        class_c_ns,
        spend_ann_ns,
        tip_gc_ns,
    });
    Ok(out)
}

/// After Class A commit, set body_range (+ denserels if missing) for **pinned**
/// planned creates only.
///
/// Uses `tx_body_range_batch` — **no** Class A body pread. Skips creates not in
/// `batch_parents` (most of the batch). Prefer denserels already set at load pin;
/// missing denserels come from shared [`rbitcoin_query::CreatePin`] (no packed reclone).
pub(super) fn fill_planned_create_layout_after_commit(
    query: &Query,
    batch_parents: &mut rbitcoin_query::BatchParents,
    planned_fks: &[rbitcoin_primitives::Fk],
    pins: &[rbitcoin_query::CreatePin],
) -> Result<(), ConsensusError> {
    if planned_fks.is_empty() || pins.is_empty() {
        return Ok(());
    }
    // Only parents actually pinned for spends and still missing abs layout.
    let missing: U64Set = batch_parents
        .fks_missing_layout()
        .into_iter()
        .filter_map(|f| f.get())
        .collect();
    if missing.is_empty() {
        return Ok(());
    }
    let mut need_fks: Vec<rbitcoin_primitives::Fk> = Vec::new();
    let mut need_pin_i: Vec<usize> = Vec::new();
    for (i, fk) in planned_fks.iter().enumerate() {
        let Some(id) = fk.get() else { continue };
        if !missing.contains(&id) {
            continue;
        }
        need_fks.push(*fk);
        need_pin_i.push(i);
    }
    if need_fks.is_empty() {
        return Ok(());
    }
    let ranges = query
        .store()
        .tx_body_range_batch(&need_fks)
        .map_err(ConsensusError::from)?;
    for ((&fk, range), &pi) in need_fks
        .iter()
        .zip(ranges.into_iter())
        .zip(need_pin_i.iter())
    {
        let Some((off, len)) = range else {
            continue;
        };
        // Load already attached sparse denserels: only body_range was missing.
        batch_parents.set_body_range_only(fk, (off, len));
        if batch_parents.has_abs_layout(fk) {
            continue;
        }
        // No denserels on pin yet — use shared CreatePin denserels (no packed reclone).
        let Some(pin) = pins.get(pi) else {
            continue;
        };
        let (_tx, outs, dense_rels) = pin.as_ref();
        if dense_rels.is_empty() && outs.is_empty() {
            continue;
        }
        batch_parents.set_layout(fk, (off, len), dense_rels);
    }
    Ok(())
}

