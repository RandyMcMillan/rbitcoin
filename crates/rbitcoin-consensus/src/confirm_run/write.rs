//! Write / Class C commit stage.

use super::phases::{class_c_commit, post_commit, structural_run};
use super::*;

pub(super) fn write_height_needed(tip: Option<u32>, height: u32) -> bool {
    match tip {
        None => true,
        Some(t) => height > t,
    }
}

/// Per-height ranges over a write batch's `planned_fks` / pins.
///
/// One RecentCreates fifo row per prepared height. A leftover tail (count
/// mismatch) is tagged with the last height so no create is dropped.
pub(super) fn recent_create_height_slices(
    prepared: &[(u32, usize)],
    total: usize,
) -> Vec<(u32, std::ops::Range<usize>)> {
    let mut out = Vec::new();
    let mut off = 0usize;
    for &(h, n) in prepared {
        if off >= total {
            break;
        }
        if off.saturating_add(n) > total {
            break;
        }
        if n > 0 {
            out.push((h, off..off + n));
        }
        off = off.saturating_add(n);
    }
    if off < total {
        let height = prepared.last().map(|(h, _)| *h).unwrap_or(0);
        out.push((height, off..total));
    }
    out
}

/// Pair idx ranges back to per-height RecentCreates rows (skip missing idx).
pub(super) fn recent_create_rows_for_slices(
    slices: &[(u32, std::ops::Range<usize>)],
    txid_fks: &[([u8; 32], rbitcoin_primitives::Fk)],
    ranges: &[Option<(u64, u64)>],
    pins: &[Option<rbitcoin_query::CreatePin>],
) -> Vec<(
    u32,
    Vec<(
        [u8; 32],
        rbitcoin_primitives::Fk,
        (u64, u64),
        Option<rbitcoin_query::CreatePin>,
    )>,
)> {
    let mut out = Vec::new();
    for (height, range) in slices {
        let mut rows = Vec::new();
        for i in range.clone() {
            let Some((txid, fk)) = txid_fks.get(i) else {
                break;
            };
            let Some(body) = ranges.get(i).copied().flatten() else {
                continue;
            };
            let pin = pins.get(i).cloned().flatten();
            rows.push((*txid, *fk, body, pin));
        }
        if !rows.is_empty() {
            out.push((*height, rows));
        }
    }
    out
}

/// COMMIT STAGE: optional Class A plan commit → structural → class_c → spend annotate → tip GC
/// → optional SP tweak index (**Tip write-through only**; Direct defers to backfill).
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

    // Keep create pins for SH collect (Class C) — same Arcs as layout fill; avoid
    // re-preading Class A bodies under RES=0 when residency is empty.
    let mut write_create_pins: FkMap<rbitcoin_query::CreatePin> = FkMap::default();
    let mut class_a_ns = 0u64;
    let mut ensure_ns = 0u64;
    let mut plan_take_ns = 0u64;
    let mut create_map_ns = 0u64;
    if let Some(plan) = batch.archive_plan.take() {
        if !plan.is_empty() {
            let t_take = Instant::now();
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
            plan_take_ns = t_take.elapsed().as_nanos() as u64;
            let t_ca = Instant::now();
            let committed = query
                .archive_commit_plan_defer_head(plan)
                .map_err(ConsensusError::from)?;
            class_a_ns = t_ca.elapsed().as_nanos() as u64;
            // Layout + SH pins only after a real append. Idempotent skip (Class A
            // already present) uses store denserels via ensure / class_c cold pins.
            if committed {
                let t_map = Instant::now();
                write_create_pins.reserve(planned_fks.len());
                for (fk, pin) in planned_fks.iter().zip(pins.iter()) {
                    write_create_pins.insert(*fk, std::sync::Arc::clone(pin));
                }
                create_map_ns = t_map.elapsed().as_nanos() as u64;
                let t_ens = Instant::now();
                fill_planned_create_layout_after_commit(
                    query,
                    &mut batch.batch_parents,
                    &planned_fks,
                    &pins,
                )?;
                ensure_ns = ensure_ns.saturating_add(t_ens.elapsed().as_nanos() as u64);
                let slices = recent_create_height_slices(
                    &batch
                        .prepared
                        .iter()
                        .map(|p| (p.height.0, p.tx_fks.len()))
                        .collect::<Vec<_>>(),
                    planned_fks.len(),
                );
                let t_recent = Instant::now();
                let txid_fks: Vec<([u8; 32], rbitcoin_primitives::Fk)> = planned_fks
                    .iter()
                    .zip(pins.iter())
                    .map(|(fk, pin)| (pin.0.txid, *fk))
                    .collect();
                let fks: Vec<rbitcoin_primitives::Fk> =
                    txid_fks.iter().map(|(_, fk)| *fk).collect();
                let t_idx = Instant::now();
                let ranges = query
                    .store()
                    .tx_body_range_batch(&fks)
                    .map_err(ConsensusError::from)?;
                let idx_ns = t_idx.elapsed().as_nanos() as u64;
                let pin_opts: Vec<Option<rbitcoin_query::CreatePin>> = pins
                    .iter()
                    .map(|p| Some(std::sync::Arc::clone(p)))
                    .collect();
                for (height, rows) in
                    recent_create_rows_for_slices(&slices, &txid_fks, &ranges, &pin_opts)
                {
                    query.note_recent_creates_pins(height, rows);
                }
                if let Some(last) = batch.prepared.last() {
                    query.set_class_a_hi(Some(last.height.0));
                }
                let t_clone = Instant::now();
                query.flush_recent_creates();
                let clone_ns = t_clone.elapsed().as_nanos() as u64;
                let recent_ns = t_recent.elapsed().as_nanos() as u64;
                if idx_ns > 0 {
                    confirm_phase_stats::WRITE_RECENT_IDX_NS.fetch_add(idx_ns, Ordering::Relaxed);
                }
                if clone_ns > 0 {
                    confirm_phase_stats::WRITE_RECENT_CLONE_NS
                        .fetch_add(clone_ns, Ordering::Relaxed);
                }
                if recent_ns > 0 {
                    confirm_phase_stats::WRITE_RECENT_NS.fetch_add(recent_ns, Ordering::Relaxed);
                }
            }
        }
    }
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
    if plan_take_ns > 0 {
        confirm_phase_stats::WRITE_PLAN_TAKE_NS.fetch_add(plan_take_ns, Ordering::Relaxed);
    }
    if create_map_ns > 0 {
        confirm_phase_stats::WRITE_CREATE_MAP_NS.fetch_add(create_map_ns, Ordering::Relaxed);
    }

    // Drain write-behind tx.head overlapping structural + Class C (one inserter).
    let t_head = Instant::now();
    let queued = query.store().txs.take_pending_queued();
    let drain_max_fk = queued.iter().filter_map(|(_, fk)| fk.get()).max();
    let drain = super::head_drain::submit_head_insert(query.store(), queued);
    let head_sub_ns = t_head.elapsed().as_nanos() as u64;
    if head_sub_ns > 0 {
        confirm_phase_stats::WRITE_HEAD_SUB_NS.fetch_add(head_sub_ns, Ordering::Relaxed);
    }

    let overlap = (|| -> Result<_, ConsensusError> {
        // Local Instant totals (not atomic deltas) — sample_and_reset races mid-batch.
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
        let t_cc = Instant::now();
        let out = class_c_commit(query, &mut batch.prepared, &write_create_pins)?;
        let class_c_wall_ns = t_cc.elapsed().as_nanos() as u64;
        // Tables only (strong+tip), matching CLASS_C_NS — not join wall / SH.
        let class_c_ns = confirm_phase_stats::CLASS_C_NS
            .load(Ordering::Relaxed)
            .saturating_sub(cc0);
        let class_c_join_ns = class_c_wall_ns.saturating_sub(class_c_ns);
        if class_c_join_ns > 0 {
            confirm_phase_stats::WRITE_CLASS_C_JOIN_NS
                .fetch_add(class_c_join_ns, Ordering::Relaxed);
        }

        let (spend_ann_ns, tip_gc_ns) =
            post_commit(query, &batch.prepared, &batch.batch_parents, &meta_by_abs)?;
        Ok((
            out,
            n_blocks,
            structural_ns,
            struct_ph,
            class_c_ns,
            spend_ann_ns,
            tip_gc_ns,
        ))
    })();

    let t_join = Instant::now();
    let drain_res = drain.join();
    let drain_join_ns = t_join.elapsed().as_nanos() as u64;
    if drain_join_ns > 0 {
        confirm_phase_stats::WRITE_DRAIN_JOIN_NS.fetch_add(drain_join_ns, Ordering::Relaxed);
    }
    let (out, n_blocks, structural_ns, struct_ph, class_c_ns, spend_ann_ns, tip_gc_ns) = overlap?;
    drain_res.map_err(ConsensusError::from)?;
    if let Some(fk) = drain_max_fk {
        query.note_head_drain_fk(fk);
    }

    let t_tweak = Instant::now();
    if query.sptweaks_enabled() && query.index_mode().is_tip() {
        if let Err(e) = index_sp_tweaks_batch(
            query,
            params,
            &batch.prepared,
            &batch.wire_blocks,
            &batch.batch_parents,
        ) {
            rbitcoin_log::warn!("sp_tweaks: skip confirm batch: {e}");
        }
    }
    let tweak_ns = t_tweak.elapsed().as_nanos() as u64;
    if tweak_ns > 0 {
        confirm_phase_stats::TWEAK_NS.fetch_add(tweak_ns, Ordering::Relaxed);
    }

    // No tip GC of sparse pins (dropped with ScriptOkBatch).
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
        tweak_ns,
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
    let spent = query
        .store()
        .tx_spent_range_batch(&need_fks)
        .map_err(ConsensusError::from)?;
    for (((&fk, range), spent_r), &_pi) in need_fks
        .iter()
        .zip(ranges.into_iter())
        .zip(spent.into_iter())
        .zip(need_pin_i.iter())
    {
        if let Some((off, len)) = range {
            batch_parents.set_body_range_only(fk, (off, len));
        }
        if let Some(sr) = spent_r {
            batch_parents.set_spent_range_only(fk, sr);
        }
    }
    Ok(())
}

fn index_sp_tweaks_batch(
    query: &Query,
    params: &ChainParams,
    prepared: &[Prepared],
    wires: &[Arc<Block>],
    parents: &rbitcoin_query::BatchParents,
) -> Result<(), ConsensusError> {
    // Tip write-through only. Direct IBD leaves the sequential cursor at origin
    // (or last backfill slot); post-IBD `backfill_sp_tweaks` owns the hole.
    let origin = params.taproot_height();
    let mut next = query
        .sptweaks_next_height()
        .unwrap_or(rbitcoin_primitives::Height(origin));
    if next.0 < origin {
        next = rbitcoin_primitives::Height(origin);
    }
    for (p, block) in prepared.iter().zip(wires.iter()) {
        if p.height.0 < origin {
            continue;
        }
        if p.height < next {
            continue;
        }
        if p.height > next {
            rbitcoin_log::debug!(
                "sp_tweaks: h={} > next={} (backfill owns hole)",
                p.height.0,
                next.0
            );
            return Ok(());
        }
        let recs = match records_from_wire(p, block, parents) {
            Some(r) => r,
            None => {
                // Same-block and unpinned parents are not a hole. Store path
                // runs after Class A commit (packed bodies + parent outs).
                rbitcoin_log::debug!(
                    "sp_tweaks: h={} wire pins incomplete; store fallback",
                    p.height.0
                );
                records_aligned_from_store(query, params, p, block)?
            }
        };
        query.put_sp_tweaks_block(p.height, p.header_fk, &recs)?;
        next = rbitcoin_primitives::Height(next.0.saturating_add(1));
    }
    Ok(())
}

/// Per-tx tweak or `None` (ineligible). Same-block prevouts come from `block`.
///
/// Non-P2TR txs are `None` without a prevout walk. Returns `None` (whole
/// height) only when an **eligible** external spend has no pin — caller falls
/// back to store. A height with no eligible txs is `Some` of `None`s.
fn records_from_wire(
    p: &Prepared,
    block: &Block,
    parents: &rbitcoin_query::BatchParents,
) -> Option<Vec<Option<[u8; 33]>>> {
    use crate::silent_payments::{tweak_from_tx, tx_has_p2tr_output};
    use bitcoin::hashes::Hash;
    use bitcoin::{Amount, ScriptBuf, TxOut};
    use std::collections::HashMap;

    let mut recs: Vec<Option<[u8; 33]>> = vec![None; block.txdata.len()];
    let eligible: Vec<usize> = block
        .txdata
        .iter()
        .enumerate()
        .filter(|(_, tx)| tx_has_p2tr_output(tx))
        .map(|(i, _)| i)
        .collect();
    if eligible.is_empty() {
        return Some(recs);
    }

    let mut by_txid: HashMap<[u8; 32], usize> = HashMap::with_capacity(block.txdata.len());
    for (i, tx) in block.txdata.iter().enumerate() {
        by_txid.insert(tx.compute_txid().to_byte_array(), i);
    }
    let mut spend_fk: HashMap<([u8; 32], u32), rbitcoin_primitives::Fk> =
        HashMap::with_capacity(p.spends.len());
    for &(pt, pv, _, cfk) in &p.spends {
        spend_fk.entry((pt, pv)).or_insert(cfk);
    }

    for &i in &eligible {
        let tx = &block.txdata[i];
        let mut prevouts = Vec::with_capacity(tx.input.len());
        for inp in &tx.input {
            if inp.previous_output.is_null() {
                prevouts.push(TxOut {
                    value: Amount::ZERO,
                    script_pubkey: ScriptBuf::new(),
                });
                continue;
            }
            let tid = inp.previous_output.txid.to_byte_array();
            let vout = inp.previous_output.vout;
            if let Some(&ti) = by_txid.get(&tid) {
                let parent = &block.txdata[ti];
                let out = parent.output.get(vout as usize)?;
                prevouts.push(out.clone());
                continue;
            }
            let create_fk = *spend_fk.get(&(tid, vout))?;
            if create_fk.is_null() {
                return None;
            }
            let (val, script) =
                parents.get_parent_txout_parts(create_fk, vout, |val, script, _| {
                    (val, script.to_vec())
                })?;
            let value = if val < 0 {
                Amount::ZERO
            } else {
                Amount::from_sat(val as u64)
            };
            prevouts.push(TxOut {
                value,
                script_pubkey: ScriptBuf::from_bytes(script),
            });
        }
        recs[i] = tweak_from_tx(tx, &prevouts).map(|t| t.tweak);
    }
    Some(recs)
}

fn records_aligned_from_store(
    query: &Query,
    params: &ChainParams,
    p: &Prepared,
    block: &Block,
) -> Result<Vec<Option<[u8; 33]>>, ConsensusError> {
    use crate::silent_payments::tweaks_for_height;
    use bitcoin::hashes::Hash;

    let map = tweaks_for_height(query, params, p.height)?;
    Ok(block
        .txdata
        .iter()
        .map(|tx| {
            let id = tx.compute_txid().to_byte_array();
            map.get(&id).map(|t| t.tweak)
        })
        .collect())
}

#[cfg(test)]
mod records_from_wire_tests {
    use super::*;
    use bitcoin::absolute::LockTime;
    use bitcoin::block::{Header, Version};
    use bitcoin::hashes::Hash;
    use bitcoin::transaction::Version as TxVersion;
    use bitcoin::{
        Amount, Block, CompactTarget, OutPoint, ScriptBuf, Sequence, Transaction, TxIn,
        TxMerkleNode, TxOut, Witness,
    };
    use rbitcoin_primitives::Fk;

    fn dummy_header() -> Header {
        Header {
            version: Version::ONE,
            prev_blockhash: bitcoin::BlockHash::from_byte_array([0u8; 32]),
            merkle_root: TxMerkleNode::from_byte_array([0u8; 32]),
            time: 1,
            bits: CompactTarget::from_consensus(0x207f_ffff),
            nonce: 0,
        }
    }

    fn coinbase() -> Transaction {
        Transaction {
            version: TxVersion::ONE,
            lock_time: LockTime::ZERO,
            input: vec![TxIn {
                previous_output: OutPoint::null(),
                script_sig: ScriptBuf::from_bytes(vec![0x01, 0x01]),
                sequence: Sequence::MAX,
                witness: Witness::new(),
            }],
            output: vec![TxOut {
                value: Amount::from_sat(50_0000_0000),
                script_pubkey: ScriptBuf::from_bytes(vec![0x51]),
            }],
        }
    }

    fn prepared(height: u32, spends: Vec<([u8; 32], u32, Fk, Fk)>) -> Prepared {
        Prepared {
            height: Height(height),
            header_fk: Fk(1),
            tx_fks: vec![],
            jobs: vec![],
            spends,
            fees: 0,
            check_scripts: false,
            time: 0,
            bits: CompactTarget::from_consensus(0x207f_ffff),
            hash: [0u8; 32],
            prev_mtp: 0,
        }
    }

    #[test]
    fn same_block_spend_does_not_need_parent_pin() {
        let cb = coinbase();
        let cb_id = cb.compute_txid();
        let child = Transaction {
            version: TxVersion::TWO,
            lock_time: LockTime::ZERO,
            input: vec![TxIn {
                previous_output: OutPoint {
                    txid: cb_id,
                    vout: 0,
                },
                script_sig: ScriptBuf::new(),
                sequence: Sequence::MAX,
                witness: Witness::new(),
            }],
            output: vec![TxOut {
                value: Amount::from_sat(1),
                script_pubkey: ScriptBuf::from_bytes(vec![0x51]),
            }],
        };
        let block = Block {
            header: dummy_header(),
            txdata: vec![cb, child],
        };
        let p = prepared(10, vec![(cb_id.to_byte_array(), 0, Fk(2), Fk::NULL)]);
        let parents = rbitcoin_query::BatchParents::new();
        let recs = records_from_wire(&p, &block, &parents)
            .expect("same-block prevout is on the wire; pin must not be required");
        assert_eq!(recs.len(), 2);
        assert!(recs.iter().all(|t| t.is_none()), "no P2TR → no tweaks");
    }

    /// After IBD, tip write-through runs this on every block. A pin miss on a
    /// non-P2TR tx used to `?` the whole height into `tweaks_for_height` (Class A
    /// walk + secp) even though `tweak_from_tx` would have returned `None`.
    #[test]
    fn ineligible_external_spend_does_not_fail_the_height() {
        let cb = coinbase();
        let spend = Transaction {
            version: TxVersion::TWO,
            lock_time: LockTime::ZERO,
            input: vec![TxIn {
                previous_output: OutPoint {
                    txid: bitcoin::Txid::from_byte_array([0x22; 32]),
                    vout: 1,
                },
                script_sig: ScriptBuf::new(),
                sequence: Sequence::MAX,
                witness: Witness::new(),
            }],
            output: vec![TxOut {
                value: Amount::from_sat(1),
                script_pubkey: ScriptBuf::from_bytes(vec![0x51]),
            }],
        };
        let block = Block {
            header: dummy_header(),
            txdata: vec![cb, spend],
        };
        let p = prepared(850_000, vec![]);
        let parents = rbitcoin_query::BatchParents::new();
        let recs = records_from_wire(&p, &block, &parents).expect(
            "no P2TR output → no prevout walk; pin miss must not store-fallback the height",
        );
        assert_eq!(recs.len(), 2);
        assert!(recs.iter().all(|t| t.is_none()), "no P2TR → no tweaks");
    }

    #[test]
    fn p2tr_external_spend_without_pin_still_fails_the_height() {
        let cb = coinbase();
        let mut spk = vec![0x51, 0x20];
        spk.extend_from_slice(&[0x11u8; 32]);
        let spend = Transaction {
            version: TxVersion::TWO,
            lock_time: LockTime::ZERO,
            input: vec![TxIn {
                previous_output: OutPoint {
                    txid: bitcoin::Txid::from_byte_array([0x22; 32]),
                    vout: 1,
                },
                script_sig: ScriptBuf::new(),
                sequence: Sequence::MAX,
                witness: Witness::new(),
            }],
            output: vec![TxOut {
                value: Amount::from_sat(1),
                script_pubkey: ScriptBuf::from_bytes(spk),
            }],
        };
        let block = Block {
            header: dummy_header(),
            txdata: vec![cb, spend],
        };
        let p = prepared(850_000, vec![]);
        let parents = rbitcoin_query::BatchParents::new();
        assert!(
            records_from_wire(&p, &block, &parents).is_none(),
            "P2TR eligible tx with no pin must still store-fallback"
        );
    }
}
