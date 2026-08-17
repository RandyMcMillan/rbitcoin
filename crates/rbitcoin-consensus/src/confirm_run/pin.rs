//! Pin denserels / spend abs layouts for wire confirm.

use super::*;

/// Select `need` vouts from a sorted sparse `(vout, out)` list.
///
/// `None` if any need vout is missing. Empty `need` with a non-empty list
/// returns a clone of the full list (legacy layout-only pin).
pub(super) fn take_need_outs(
    live_all: &[(u32, rbitcoin_store::OutputRecord)],
    need: &[u32],
) -> Option<Vec<(u32, rbitcoin_store::OutputRecord)>> {
    debug_assert!(
        live_all.windows(2).all(|w| w[0].0 < w[1].0),
        "sparse outs must be strictly increasing by vout"
    );
    if need.is_empty() {
        return Some(live_all.to_vec());
    }
    let mut live = Vec::with_capacity(need.len());
    for &v in need {
        match live_all.binary_search_by_key(&v, |(ov, _)| *ov) {
            Ok(i) => live.push((v, live_all[i].1.clone())),
            Err(_) => return None,
        }
    }
    Some(live)
}

/// Pin parents for wire load: **only spent parents** (sparse outs).
///
/// Sources: plan/in-flight offline denserels → **txout body by range** from
/// [`ParentPinStamp`] (lookup-stamped). Load never reads head / `tx.idx` /
/// `txid.body`. Write [`ensure_spend_abs_layouts`] stamps `spent.idx` ranges
/// for archived parents — load does not idx-batch the full parent set.
pub(super) fn pin_for_wire_batch(
    query: &Query,
    plan: Option<&rbitcoin_query::ArchiveWritePlan>,
    parent_pin: &ParentPinStamp,
    metas: &[BodyMeta],
    wire_blocks: &[Arc<Block>],
    in_flight: Option<&rbitcoin_query::InFlightView>,
    pipeline_parent_store: Option<&std::sync::Arc<rbitcoin_query::PipelineParentStore>>,
) -> Result<
    (
        rbitcoin_query::BatchParents,
        rbitcoin_query::BatchThin,
        DenserelsWarmStats,
    ),
    ConsensusError,
> {
    use rbitcoin_query::confirm_load_stats;
    use rbitcoin_query::ThinInput;
    use std::sync::atomic::Ordering;

    let t_pin = Instant::now();
    let mut batch_thin: rbitcoin_query::BatchThin = rbitcoin_query::BatchThin::default();
    let mut parent_vouts: U64Map<Vec<u32>> = U64Map::default();
    let mut n_same_batch = 0u32;

    // id → Arc pin (tx, outs, dense denserels). Spent parents only (after thin pass).
    let mut plan_by_id: U64Map<
        std::sync::Arc<(rbitcoin_store::TxRecord, Vec<rbitcoin_store::OutputRecord>)>,
    > = U64Map::default();
    // batch_pin by create id (Arc — preferred same-batch pin source).
    // packed pin half shares the same Arc; no separate outs clone.
    let mut batch_pin_by_id: U64Map<
        &std::sync::Arc<(rbitcoin_store::TxRecord, Vec<rbitcoin_store::OutputRecord>)>,
    > = U64Map::default();
    if let Some(plan) = plan {
        if plan.batch_pin.len() == plan.planned_fks.len() {
            for (fk, pin) in plan.planned_fks.iter().zip(plan.batch_pin.iter()) {
                if let Some(id) = fk.get() {
                    batch_pin_by_id.insert(id, pin);
                }
            }
        } else {
            // Partial plans (tests): fall back to packed pin half.
            for ((pin, _ins), fk) in plan.packed.iter().zip(plan.planned_fks.iter()) {
                if let Some(id) = fk.get() {
                    batch_pin_by_id.insert(id, pin);
                }
            }
        }
        for ((_pin, ins), fk) in plan.packed.iter().zip(plan.planned_fks.iter()) {
            let Some(sid) = fk.get() else { continue };
            let mut edges = Vec::with_capacity(ins.len());
            for inp in ins {
                if inp.is_coinbase() || inp.prev_index == u32::MAX {
                    edges.push(ThinInput {
                        create_fk: None,
                        prev_index: u32::MAX,
                    });
                    continue;
                }
                if let Some(pid) = inp.create_fk.get() {
                    edges.push(ThinInput {
                        create_fk: Some(pid),
                        prev_index: inp.prev_index,
                    });
                    // Same-batch or external parent — only spent creates need pin.
                    parent_vouts.entry(pid).or_default().push(inp.prev_index);
                } else {
                    edges.push(ThinInput {
                        create_fk: None,
                        prev_index: inp.prev_index,
                    });
                }
            }
            batch_thin.insert(sid, edges);
        }
    } else {
        // plan=None: create_fk from ParentPinStamp (lookup head/idx), never load head.
        for (m, block) in metas.iter().zip(wire_blocks.iter()) {
            for (ti, tx) in block.txdata.iter().enumerate() {
                let Some(sfk) = m.tx_fks.get(ti).and_then(|f| f.get()) else {
                    continue;
                };
                let mut edges = Vec::with_capacity(tx.input.len());
                for inp in &tx.input {
                    if inp.previous_output.is_null() {
                        edges.push(ThinInput {
                            create_fk: None,
                            prev_index: u32::MAX,
                        });
                        continue;
                    }
                    let prev_txid = inp.previous_output.txid.to_byte_array();
                    let vout = inp.previous_output.vout;
                    if let Some(&pid) = parent_pin.create_by_txid.get(&prev_txid) {
                        edges.push(ThinInput {
                            create_fk: Some(pid),
                            prev_index: vout,
                        });
                        parent_vouts.entry(pid).or_default().push(vout);
                        continue;
                    }
                    edges.push(ThinInput {
                        create_fk: None,
                        prev_index: vout,
                    });
                }
                batch_thin.insert(sfk, edges);
            }
        }
    }

    for vouts in parent_vouts.values_mut() {
        vouts.sort_unstable();
        vouts.dedup();
    }

    // Build plan/in-flight pin sources only for spent parents (not every create).
    // 1) Prior uncommitted plans (Arc pin — no deep clone).
    if let Some(ifo) = in_flight {
        for (id, need) in &parent_vouts {
            if plan_by_id.contains_key(id) {
                continue;
            }
            if let Some(pin) = ifo.get_out(*id) {
                let _ = need;
                plan_by_id.insert(*id, std::sync::Arc::clone(pin));
            }
        }
    }
    // 2) External sparse pins are applied after adopt (not dense CreatePin).
    // 2b deferred: range denserels after free pins are in batch_parents (sparse API).
    // 3) Same-batch creates: shared batch_pin / packed CreatePin Arc.
    for (id, _need) in &parent_vouts {
        if plan_by_id.contains_key(id) {
            continue;
        }
        if let Some(pin) = batch_pin_by_id.get(id) {
            plan_by_id.insert(*id, std::sync::Arc::clone(pin));
            n_same_batch = n_same_batch.saturating_add(1);
        }
    }

    let mut batch_parents = match pipeline_parent_store {
        Some(store) => rbitcoin_query::BatchParents::with_store(
            std::sync::Arc::clone(store),
            parent_vouts.len(),
        ),
        None => rbitcoin_query::BatchParents::with_capacity(parent_vouts.len()),
    };
    // One store lock: adopt live shared pins (writeq / peer load overlap) so free
    // path can skip OutputRecord clones when need is already covered.
    let t_adopt = Instant::now();
    if pipeline_parent_store.is_some() {
        batch_parents.adopt_from_store(parent_vouts.keys().copied());
    }
    let adopt_ns = t_adopt.elapsed().as_nanos() as u64;
    let mut still_need: U64Map<Vec<u32>> = U64Map::default();
    let mut n_plan_pin = 0u64;

    // Plan / in-flight / same-batch free pins → BatchParents (local HashMap put;
    // store mutex only at adopt/publish — not per parent).
    let t_plan = Instant::now();
    for (id, need) in &parent_vouts {
        let fk = rbitcoin_primitives::Fk(*id);
        // Cross-batch share hit: pin already covers need after adopt.
        // Pure hit: only refresh meta when plan/layout material is present
        // (skip empty refresh_pin_meta and avoid redundant outs loads).
        if !need.is_empty() && batch_parents.pin_covered(fk, need) {
            if let Some(pin) = plan_by_id.get(id) {
                let (tx, _outs) = pin.as_ref();
                let cb = if tx.input_count != 1 {
                    Some(false)
                } else {
                    None
                };
                let plan_range = parent_pin
                    .ranges
                    .get(id)
                    .copied()
                    .or_else(|| plan.and_then(|p| p.external_parent_ranges.get(id).copied()));
                if cb.is_some() || plan_range.is_some() {
                    batch_parents.refresh_pin_meta(fk, cb, plan_range, Vec::new());
                }
            } else if let Some(plan) = plan {
                // Sparse external: layout/coinbase only from plan-local pin.
                if let Some(ext) = plan.external_parent_outs.get(id) {
                    let (tx, _live) = ext.as_ref();
                    let cb = if tx.input_count != 1 {
                        Some(false)
                    } else {
                        None
                    };
                    let plan_range = parent_pin
                        .ranges
                        .get(id)
                        .copied()
                        .or_else(|| plan.external_parent_ranges.get(id).copied());
                    if cb.is_some() || plan_range.is_some() {
                        batch_parents.refresh_pin_meta(fk, cb, plan_range, Vec::new());
                    }
                }
            }
            n_plan_pin = n_plan_pin.saturating_add(1);
            continue;
        }
        // Sparse external parent pin (need-vouts only — no dense CreatePin).
        if let Some(plan) = plan {
            if let Some(ext) = plan.external_parent_outs.get(id) {
                let (tx, live_all) = ext.as_ref();
                if let Some(live) = take_need_outs(live_all, need) {
                    let checked = if need.is_empty() {
                        live.iter().map(|(v, _)| *v).collect()
                    } else {
                        need.clone()
                    };
                    let cb = if tx.input_count != 1 {
                        Some(false)
                    } else {
                        None
                    };
                    let plan_range = parent_pin
                        .ranges
                        .get(id)
                        .copied()
                        .or_else(|| plan.external_parent_ranges.get(id).copied());
                    batch_parents.insert_owned(
                        fk,
                        tx.clone(),
                        live,
                        checked,
                        cb,
                        plan_range,
                        Vec::new(),
                    );
                    n_plan_pin = n_plan_pin.saturating_add(1);
                    continue;
                }
                // Incomplete sparse pin — fall through to range / cold.
                still_need.insert(*id, need.clone());
                continue;
            }
        }
        if let Some(pin) = plan_by_id.get(id) {
            let (tx, outs) = pin.as_ref();
            let live: Vec<(u32, rbitcoin_store::OutputRecord)> = need
                .iter()
                .filter_map(|&v| outs.get(v as usize).map(|o| (v, o.clone())))
                .collect();
            if live.len() != need.len() {
                still_need.insert(*id, need.clone());
                continue;
            }
            let cb = if tx.input_count != 1 {
                Some(false)
            } else {
                None
            };
            let plan_range = parent_pin
                .ranges
                .get(id)
                .copied()
                .or_else(|| plan.and_then(|p| p.external_parent_ranges.get(id).copied()));
            batch_parents.insert_owned(
                fk,
                tx.clone(),
                live,
                need.clone(),
                cb,
                plan_range,
                Vec::new(),
            );
            n_plan_pin = n_plan_pin.saturating_add(1);
        } else {
            still_need.insert(*id, need.clone());
        }
    }
    let plan_pin_ns = t_plan.elapsed().as_nanos() as u64;
    // Batch-local cold walls/counts for last-pin / slow-load logs.
    let mut cold_range_batch_ns = 0u64;
    let mut n_range_new = 0u64;

    // 2b) Body denserels by range for still_need (lookup-stamped ranges only).
    {
        let mut range_jobs: Vec<(rbitcoin_primitives::Fk, (u64, u64), [u8; 32], Vec<u32>)> =
            Vec::new();
        let pending = std::mem::take(&mut still_need);
        for (id, need) in pending {
            let range = parent_pin
                .ranges
                .get(&id)
                .copied()
                .or_else(|| plan.and_then(|p| p.external_parent_ranges.get(&id).copied()));
            let Some(range) = range else {
                still_need.insert(id, need);
                continue;
            };
            let tid = parent_pin.create_txid(id).or_else(|| {
                plan.and_then(|p| p.external_parent_txid(id))
                    .filter(|t| *t != [0u8; 32])
            });
            let Some(tid) = tid else {
                return Err(ConsensusError::Store(StoreError::Corrupt(
                    "invariant: lookup stage miss (load parent create identity not stamped)",
                )));
            };
            range_jobs.push((rbitcoin_primitives::Fk(id), range, tid, need));
        }
        if !range_jobs.is_empty() {
            let n_range = range_jobs.len() as u64;
            let (decoded, body_ns, dec_ns) = query
                .store()
                .get_outs_by_range_batch(&range_jobs)
                .map_err(ConsensusError::from)?;
            let rng_ns = body_ns.saturating_add(dec_ns);
            cold_range_batch_ns = cold_range_batch_ns.saturating_add(rng_ns);
            if rng_ns > 0 {
                confirm_load_stats::COLD_IO_NS.fetch_add(rng_ns, Ordering::Relaxed);
                confirm_load_stats::COLD_RANGE_NS.fetch_add(rng_ns, Ordering::Relaxed);
            }
            if body_ns > 0 {
                confirm_load_stats::COLD_RANGE_BODY_NS.fetch_add(body_ns, Ordering::Relaxed);
            }
            if dec_ns > 0 {
                confirm_load_stats::COLD_RANGE_DECODE_NS.fetch_add(dec_ns, Ordering::Relaxed);
            }
            confirm_load_stats::COLD_RANGE_N.fetch_add(n_range, Ordering::Relaxed);
            confirm_load_stats::BODY_TX_READS.fetch_add(n_range, Ordering::Relaxed);
            confirm_load_stats::PIN_NEW.fetch_add(n_range, Ordering::Relaxed);
            n_range_new = n_range_new.saturating_add(n_range);
            let t_range_fill = Instant::now();
            for ((fk, range, _tid, need), row) in range_jobs.into_iter().zip(decoded.into_iter()) {
                let Some(id) = fk.get() else {
                    continue;
                };
                let Some((mut tx, live, sparse)) = row else {
                    return Err(ConsensusError::Store(StoreError::Corrupt(
                        "invariant: load denserels by range returned none for stamped parent",
                    )));
                };
                if live.len() != need.len() {
                    return Err(ConsensusError::Store(StoreError::Corrupt(
                        "invariant: load denserels by range incomplete outs for need_vouts",
                    )));
                }
                // Schema-13 decode leaves zero identity — stamp from parent_pin only.
                if tx.txid == [0u8; 32] {
                    tx.txid = parent_pin
                        .create_txid(id)
                        .ok_or(ConsensusError::Store(StoreError::Corrupt(
                        "invariant: lookup stage miss (load parent create identity not stamped)",
                    )))?;
                }
                let cb = if tx.input_count != 1 {
                    Some(false)
                } else {
                    None
                };
                batch_parents.insert_owned(fk, tx, live, need, cb, Some(range), sparse);
                still_need.remove(&id);
                // Cold range-fill: PIN_NEW only. Do not bump n_plan_pin /
                // PIN_CACHE_BODY — that would inflate pin_hit%.
            }
            let range_fill_ns = t_range_fill.elapsed().as_nanos() as u64;
            if range_fill_ns > 0 {
                confirm_load_stats::PIN_RANGE_FILL_NS.fetch_add(range_fill_ns, Ordering::Relaxed);
            }
        }
    }

    // Load IO contract: txout outs only via body-by-range (above) or plan/in-flight
    // offline pins. **Never** `tx.idx` / head cold denserels on load (idx is lookup).
    // `spent.idx` range stamp is write `ensure_spend_abs_layouts` (not here).
    let n_cold = 0u64;
    let cold_io_ns = 0u64;
    let cold_decode_ns = 0u64;
    if !still_need.is_empty() {
        return Err(ConsensusError::Store(StoreError::Corrupt(
            "invariant: lookup stage miss (load parent without body_range denserels)",
        )));
    }

    // Pin contract: every spent parent is in BatchParents with need outs.
    // Same-batch / load-ahead creates may still lack spent_range until write
    // (idx miss here; `fill_planned_create_layout_after_commit` + ensure).
    let t_contract = Instant::now();
    for (id, need) in &parent_vouts {
        let fk = rbitcoin_primitives::Fk(*id);
        if !batch_parents.contains(fk) {
            return Err(ConsensusError::Store(StoreError::Corrupt(
                "invariant: wire pin missing spent parent",
            )));
        }
        if !need.is_empty() && !batch_parents.pin_covered(fk, need) {
            return Err(ConsensusError::Store(StoreError::Corrupt(
                "invariant: wire pin incomplete outs for spent parent",
            )));
        }
    }
    let contract_ns = t_contract.elapsed().as_nanos() as u64;

    // One store lock: publish Weaks so peer load/writeq can adopt the same Arc.
    let t_publish = Instant::now();
    batch_parents.publish_to_store();
    let publish_ns = t_publish.elapsed().as_nanos() as u64;

    let n_unique = parent_vouts.len() as u64;
    if n_unique > 0 {
        confirm_load_stats::PARENT_UNIQUE.fetch_add(n_unique, Ordering::Relaxed);
        confirm_load_stats::UTXO_PARENTS.fetch_add(n_unique, Ordering::Relaxed);
    }
    if n_plan_pin > 0 {
        confirm_load_stats::PIN_PLAN.fetch_add(n_plan_pin, Ordering::Relaxed);
        confirm_load_stats::PIN_CACHE_BODY.fetch_add(n_plan_pin, Ordering::Relaxed);
    }
    if n_cold > 0 {
        confirm_load_stats::PIN_NEW.fetch_add(n_cold, Ordering::Relaxed);
    }
    if plan_pin_ns > 0 {
        confirm_load_stats::PLAN_PIN_NS.fetch_add(plan_pin_ns, Ordering::Relaxed);
    }
    if adopt_ns > 0 {
        confirm_load_stats::PIN_ADOPT_NS.fetch_add(adopt_ns, Ordering::Relaxed);
    }
    if contract_ns > 0 {
        confirm_load_stats::PIN_CONTRACT_NS.fetch_add(contract_ns, Ordering::Relaxed);
    }
    if publish_ns > 0 {
        confirm_load_stats::PIN_PUBLISH_NS.fetch_add(publish_ns, Ordering::Relaxed);
    }
    // Last-batch pin residual for slow-load logs (overwrite; not window-summed).
    let cold_batch_ns = cold_range_batch_ns
        .saturating_add(cold_io_ns)
        .saturating_add(cold_decode_ns);
    confirm_load_stats::note_last_pin(
        adopt_ns,
        plan_pin_ns,
        cold_batch_ns,
        contract_ns,
        publish_ns,
        n_plan_pin,
        n_cold.saturating_add(n_range_new),
    );
    if cold_io_ns > 0 {
        confirm_load_stats::COLD_IO_NS.fetch_add(cold_io_ns, Ordering::Relaxed);
        confirm_load_stats::PIN_NEW_META_NS.fetch_add(cold_io_ns, Ordering::Relaxed);
    }
    if cold_decode_ns > 0 {
        confirm_load_stats::COLD_DECODE_NS.fetch_add(cold_decode_ns, Ordering::Relaxed);
    }
    let pin_ns = t_pin.elapsed().as_nanos() as u64;
    if pin_ns > 0 {
        confirm_load_stats::PARENT_PIN_NS.fetch_add(pin_ns, Ordering::Relaxed);
        confirm_load_stats::PIN_BODY_NS.fetch_add(pin_ns, Ordering::Relaxed);
        // Wire path: `NS` is pin wall (legacy load path uses full load_confirm wall).
        confirm_load_stats::NS.fetch_add(pin_ns, Ordering::Relaxed);
    }
    let n_blks = metas.len() as u64;
    if n_blks > 0 {
        confirm_load_stats::BLOCKS.fetch_add(n_blks, Ordering::Relaxed);
    }

    let warm = DenserelsWarmStats {
        // External parents only (unique spent creates not same-batch offline).
        parents: parent_vouts.len().saturating_sub(n_same_batch as usize) as u32,
        already: n_plan_pin.saturating_sub(n_same_batch as u64) as u32,
        cold: n_cold as u32,
        same_batch: n_same_batch,
        work_ns: pin_ns,
    };
    Ok((batch_parents, batch_thin, warm))
}

/// SCRIPTS STAGE: pure verification of jobs already assembled at load.
///
/// **No store / Query / side effects.** Input is a [`LoadedBatch`] (script jobs
/// hold prevouts + txs + softfork flags); output is a [`ScriptOkBatch`] for the
/// write queue. Clears jobs after success so write carries spends/fees only.
///
/// Uses rayon for CPU parallelism only — does not touch disk or process-global
/// tables (aside from the rayon pool and script phase timers).
/// Ensure spend abs for every spend edge on the write batch.
///
/// Sole owner of `spent.idx` range stamp for write. Covers:
/// 1. Archived parents (load pin no longer idx-batches the full set)
/// 2. Same-batch creates (range only after `archive_commit_plan` + fill)
/// 3. Load-ahead parents not yet in `spent.idx` when pinned
/// 4. Retry after partial write
///
/// Missing abs after those fills is `Corrupt`. Write still annotates every
/// eligible edge; this function never calls `put_spend*`.
pub(super) fn ensure_spend_abs_layouts(
    query: &Query,
    batch_parents: &mut rbitcoin_query::BatchParents,
    prepared: &[Prepared],
) -> Result<(), ConsensusError> {
    use rbitcoin_store::IdxBodyMode;

    let mut need: U64Map<Vec<u32>> = U64Map::default();
    for p in prepared {
        for &(_txid, vout, sfk, cfk) in &p.spends {
            if sfk.is_null() || cfk.is_null() {
                continue;
            }
            if batch_parents.get_spender_abs(cfk, vout).is_some() {
                continue;
            }
            if let Some(id) = cfk.get() {
                need.entry(id).or_default().push(vout);
            }
        }
    }
    // Also repair pins that have outs but no layout (structural cold path would
    // skip unpinned; pinned-without-abs fails structural).
    for fk in batch_parents.fks_missing_layout() {
        if let Some(id) = fk.get() {
            need.entry(id).or_default();
        }
    }
    if need.is_empty() {
        return Ok(());
    }
    for vouts in need.values_mut() {
        vouts.sort_unstable();
        vouts.dedup();
    }

    // Stamp spent.body ranges first so abs = spent_off + SLOT×vout (idx only).
    {
        let mut spent_fks: Vec<rbitcoin_primitives::Fk> =
            need.keys().map(|id| rbitcoin_primitives::Fk(*id)).collect();
        spent_fks.sort_unstable_by_key(|f| f.0);
        spent_fks.dedup();
        if !spent_fks.is_empty() {
            let spent = query
                .store()
                .tx_spent_range_batch(&spent_fks)
                .map_err(ConsensusError::from)?;
            for (fk, opt) in spent_fks.iter().zip(spent.into_iter()) {
                if let Some(sr) = opt {
                    batch_parents.set_spent_range_only(*fk, sr);
                }
            }
        }
    }

    // 1) Pin denserels + body_range already on BatchParents — no body IO.
    let mut ensure_res = 0u64;
    let mut still: U64Map<Vec<u32>> = U64Map::default();
    // Pin has denserels but still no body_range — idx only (not denserels IO).
    let mut range_only: Vec<rbitcoin_primitives::Fk> = Vec::new();
    for (id, need_v) in &need {
        let fk = rbitcoin_primitives::Fk(*id);
        // Pin already complete for this create — skip.
        if batch_parents.has_abs_layout(fk)
            && (need_v.is_empty()
                || need_v
                    .iter()
                    .all(|&v| batch_parents.get_spender_abs(fk, v).is_some()))
        {
            ensure_res = ensure_res.saturating_add(1);
            continue;
        }
        // Range-only: denserels already on pin — do not cold-load Class A denserels body.
        if batch_parents.has_spender_rels(fk) {
            if batch_parents.has_abs_layout(fk)
                && (need_v.is_empty()
                    || need_v
                        .iter()
                        .all(|&v| batch_parents.get_spender_abs(fk, v).is_some()))
            {
                ensure_res = ensure_res.saturating_add(1);
                continue;
            }
            range_only.push(fk);
            continue;
        }
        still.insert(*id, need_v.clone());
    }

    // 1b) Idx body ranges for pin denserels without range (cheap; no denserels body).
    if !range_only.is_empty() {
        range_only.sort_unstable_by_key(|f| f.0);
        range_only.dedup();
        let ranges = query
            .store()
            .tx_body_range_batch(&range_only)
            .map_err(ConsensusError::from)?;
        let spent = query
            .store()
            .tx_spent_range_batch(&range_only)
            .map_err(ConsensusError::from)?;
        for (fk, sr) in range_only.iter().zip(spent.into_iter()) {
            if let Some(range) = sr {
                batch_parents.set_spent_range_only(*fk, range);
            }
        }
        for (fk, opt) in range_only.iter().zip(ranges.into_iter()) {
            let Some(range) = opt else {
                // No idx range yet (e.g. parent not committed) — hard fail at post-condition
                // if spend still needs abs; leave for invariant.
                continue;
            };
            batch_parents.set_body_range_only(*fk, range);
            let id = fk.get().unwrap_or(0);
            let need_v = need.get(&id).cloned().unwrap_or_default();
            if batch_parents.has_abs_layout(*fk)
                && (need_v.is_empty()
                    || need_v
                        .iter()
                        .all(|&v| batch_parents.get_spender_abs(*fk, v).is_some()))
            {
                ensure_res = ensure_res.saturating_add(1);
            } else {
                // denserels present but need_v not covered — should not happen if pin sparse
                // was built for need; fall through to cold denserels as last resort.
                still.entry(id).or_insert(need_v);
            }
        }
    }
    confirm_phase_stats::ENSURE_RES_HIT.fetch_add(ensure_res, Ordering::Relaxed);

    // 2) Class A denserels body for remainder only (must not re-load pin denserels hits).
    if !still.is_empty() {
        let fks: Vec<rbitcoin_primitives::Fk> = still
            .keys()
            .map(|id| rbitcoin_primitives::Fk(*id))
            .collect();
        confirm_phase_stats::ENSURE_COLD_N.fetch_add(fks.len() as u64, Ordering::Relaxed);
        // Structural denserels fill for pin gaps (cold Class A).
        let loaded = rbitcoin_query::load_creates_once(query.store(), &fks, IdxBodyMode::Outs)
            .map_err(ConsensusError::from)?;
        let secret = query.store().txs.store_secret();
        for c in loaded {
            let Some(id) = c.fk.get() else {
                return Err(ConsensusError::Store(StoreError::Corrupt(
                    "invariant: ensure denserels null create_fk",
                )));
            };
            let need_v = still.get(&id).cloned().unwrap_or_default();
            let (mut tx, outs, dense_rels) = if let Some(dec) = c.decoded_outs {
                dec
            } else {
                // load_creates_once OutsDenserels should always fill decoded_outs.
                rbitcoin_store::decode_packed_tx_outs_with_spender_rels_secret(&c.raw, Some(secret))
                    .map_err(|_| {
                        ConsensusError::Store(StoreError::Corrupt(
                            "invariant: ensure denserels decode failed",
                        ))
                    })?
            };
            // Write ensure may read txid.body (not load stage).
            tx.txid = known_create_txid_lookup(query, id, None)?;
            if batch_parents.contains(c.fk) {
                // Layout-only publish with already_covers short-circuit (batched style).
                batch_parents.set_layout_for_need(c.fk, c.body_range, &dense_rels, &need_v);
                continue;
            }
            // Not pinned at load (e.g. already-archived same-batch create): insert
            // with layout so annotate/structural abs paths work.
            let mut checked = need_v;
            if checked.is_empty() {
                checked = (0..outs.len() as u32).collect();
            }
            let live: Vec<(u32, rbitcoin_store::OutputRecord)> = checked
                .iter()
                .filter_map(|&v| outs.get(v as usize).map(|o| (v, o.clone())))
                .collect();
            if live.len() != checked.len() {
                return Err(ConsensusError::Store(StoreError::Corrupt(
                    "invariant: ensure denserels incomplete outs for need_vouts",
                )));
            }
            let sparse = rbitcoin_query::sparse_spender_rels(&dense_rels, &checked);
            if !rbitcoin_query::layout_covers_need(Some(c.body_range), &sparse, &checked) {
                return Err(ConsensusError::Store(StoreError::Corrupt(
                    "invariant: ensure denserels incomplete for need_vouts",
                )));
            }
            let cb = if tx.input_count != 1 {
                Some(false)
            } else {
                None
            };
            batch_parents.insert_owned(c.fk, tx, live, checked, cb, Some(c.body_range), sparse);
        }
        // Cold inserts are new pins — stamp spent ranges onto them too.
        let mut spent_fks: Vec<rbitcoin_primitives::Fk> = still
            .keys()
            .map(|id| rbitcoin_primitives::Fk(*id))
            .collect();
        spent_fks.sort_unstable_by_key(|f| f.0);
        if !spent_fks.is_empty() {
            let spent = query
                .store()
                .tx_spent_range_batch(&spent_fks)
                .map_err(ConsensusError::from)?;
            for (fk, opt) in spent_fks.iter().zip(spent.into_iter()) {
                if let Some(sr) = opt {
                    batch_parents.set_spent_range_only(*fk, sr);
                }
            }
        }
    }

    // Post-condition: every non-null spend edge has abs — no structural cold paper.
    for p in prepared {
        for &(_txid, vout, sfk, cfk) in &p.spends {
            if sfk.is_null() || cfk.is_null() {
                continue;
            }
            if batch_parents.get_spender_abs(cfk, vout).is_none() {
                return Err(ConsensusError::Store(StoreError::Corrupt(
                    "invariant: ensure denserels/abs incomplete for spend edge",
                )));
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod take_need_outs_tests {
    use super::take_need_outs;
    use rbitcoin_store::OutputRecord;

    #[test]
    fn take_need_outs_binary_search_high_vout() {
        let live = vec![
            (0, OutputRecord::unspent(1, vec![0x00])),
            (1, OutputRecord::unspent(2, vec![0x01])),
            (2, OutputRecord::unspent(3, vec![0x02])),
            (3, OutputRecord::unspent(4, vec![0x03])),
        ];
        let got = take_need_outs(&live, &[0, 3]).expect("need 0 and 3");
        assert_eq!(got.len(), 2);
        assert_eq!(got[0].0, 0);
        assert_eq!(got[1].0, 3);
        assert!(take_need_outs(&live, &[7]).is_none());
    }
}
