//! Write / Class C commit stage.

use super::*;
// Parent imports these from phases; access by path for non-glob private imports.
use super::phases::{class_c_commit, post_commit, structural_run};

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

    if query.sptweaks_enabled() {
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
    let origin = params.taproot_height();
    for (p, block) in prepared.iter().zip(wires.iter()) {
        if p.height.0 < origin {
            continue;
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
    }
    Ok(())
}

/// Per-tx tweak or `None` (ineligible). Same-block prevouts come from `block`.
///
/// Returns `None` only when an **external** spend has no pin (caller falls back
/// to store). A height with no eligible txs is `Some` of `None`s — that is
/// not a skip.
fn records_from_wire(
    p: &Prepared,
    block: &Block,
    parents: &rbitcoin_query::BatchParents,
) -> Option<Vec<Option<[u8; 33]>>> {
    use crate::silent_payments::tweak_from_tx;
    use bitcoin::hashes::Hash;
    use bitcoin::{Amount, ScriptBuf, TxOut};
    use std::collections::HashMap;

    let mut by_txid: HashMap<[u8; 32], usize> = HashMap::with_capacity(block.txdata.len());
    for (i, tx) in block.txdata.iter().enumerate() {
        by_txid.insert(tx.compute_txid().to_byte_array(), i);
    }

    let mut recs = Vec::with_capacity(block.txdata.len());
    for tx in &block.txdata {
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
            let create_fk = p.spends.iter().find_map(|(pt, pv, _, cfk)| {
                if *pt == tid && *pv == vout {
                    Some(*cfk)
                } else {
                    None
                }
            })?;
            if create_fk.is_null() {
                return None;
            }
            let (val, script, _) = parents.get_parent_txout_parts(create_fk, vout)?;
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
        recs.push(tweak_from_tx(tx, &prevouts).map(|t| t.tweak));
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
}
