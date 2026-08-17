//! Lookup / stamp / denserels warm for wire confirm.

use super::*;

/// Stats from lookup-stage denserels ensure (external parents → plan-local).
#[derive(Debug, Default, Clone, Copy)]
pub struct DenserelsWarmStats {
    /// Unique external parent creates considered (stamped create_fk, not same-batch).
    pub parents: u32,
    /// Already had denserels in plan.external_parent_outs or in-flight.
    pub already: u32,
    /// Cold denserels body loads (into plan-local only).
    pub cold: u32,
    /// Same-batch plan creates (offline denserels at pin).
    pub same_batch: u32,
    pub work_ns: u64,
}

/// External parents only: one `OutsDenserels` cold load into
/// **`plan.external_parent_outs`** (pipeline-local).
///
/// Parent create_fks come from **plan-stamped** inputs (and in-flight). No head
/// resolve here — plan already stamped via batch + head. Same-batch creates are
/// skipped (pin uses offline denserels).
///
/// After this returns, load pin must see every external parent covered via
/// plan-local map, in-flight, or same-batch (no cold denserels dual path on load).
pub fn ensure_external_parent_denserels_from_plan(
    query: &Query,
    plan: Option<&mut rbitcoin_query::ArchiveWritePlan>,
    in_flight: Option<&rbitcoin_query::InFlightView>,
) -> Result<DenserelsWarmStats, ConsensusError> {
    use rbitcoin_query::confirm_load_stats;
    use rbitcoin_store::IdxBodyMode;
    use std::sync::atomic::Ordering;

    let t0 = Instant::now();
    let mut st = DenserelsWarmStats::default();
    let Some(plan) = plan else {
        st.work_ns = t0.elapsed().as_nanos() as u64;
        return Ok(st);
    };

    let mut batch_create_ids: U64Map<()> = U64Map::default();
    for fk in &plan.planned_fks {
        if let Some(id) = fk.get() {
            batch_create_ids.insert(id, ());
        }
    }

    // Reverse map from wire prev_txid (lookup stamp may omit it on synthetic plans).
    let mut parent_vouts: U64Map<Vec<u32>> = U64Map::default();
    let t_collect = Instant::now();
    for ((_pin, ins), _) in plan.packed.iter().zip(plan.planned_fks.iter()) {
        for inp in ins {
            if inp.is_coinbase() || inp.prev_index == u32::MAX {
                continue;
            }
            if let Some(pid) = inp.create_fk.get() {
                parent_vouts.entry(pid).or_default().push(inp.prev_index);
                if inp.prev_txid != [0u8; 32] {
                    plan.external_parent_txids
                        .entry(pid)
                        .or_insert(inp.prev_txid);
                }
            }
        }
    }
    for vouts in parent_vouts.values_mut() {
        vouts.sort_unstable();
        vouts.dedup();
    }
    let collect_ns = t_collect.elapsed().as_nanos() as u64;

    let mut cold_fks: Vec<rbitcoin_primitives::Fk> = Vec::new();
    for (id, _need) in &parent_vouts {
        if batch_create_ids.contains_key(id) {
            st.same_batch = st.same_batch.saturating_add(1);
            continue;
        }
        st.parents = st.parents.saturating_add(1);
        let fk = rbitcoin_primitives::Fk(*id);
        if plan
            .external_parent_outs
            .get(id)
            .is_some_and(|pin| !pin.1.is_empty())
        {
            st.already = st.already.saturating_add(1);
            continue;
        }
        if let Some(ifo) = in_flight {
            if ifo.get_out(*id).is_some_and(|pin| !pin.1.is_empty()) {
                st.already = st.already.saturating_add(1);
                continue;
            }
        }
        cold_fks.push(fk);
    }
    cold_fks.sort_unstable_by_key(|f| f.0);
    cold_fks.dedup();
    st.cold = cold_fks.len() as u32;

    let mut cold_io_ns = 0u64;
    if !cold_fks.is_empty() {
        let t_io = Instant::now();
        let mut by_range: Vec<(rbitcoin_primitives::Fk, (u64, u64), [u8; 32], Vec<u32>)> =
            Vec::new();
        let mut need_idx: Vec<rbitcoin_primitives::Fk> = Vec::new();
        for fk in &cold_fks {
            let id = fk.get().unwrap_or(0);
            if let Some(&range) = plan.external_parent_ranges.get(&id) {
                let tid = known_create_txid_load(id, Some(plan))?;
                let need = parent_vouts.get(&id).cloned().unwrap_or_default();
                by_range.push((*fk, range, tid, need));
            } else {
                need_idx.push(*fk);
            }
        }
        if !by_range.is_empty() {
            let n_range = by_range.len() as u64;
            let (decoded, body_ns, dec_ns) = query
                .store()
                .get_outs_by_range_batch(&by_range)
                .map_err(ConsensusError::from)?;
            let rng_ns = body_ns.saturating_add(dec_ns);
            if rng_ns > 0 {
                confirm_load_stats::COLD_RANGE_NS.fetch_add(rng_ns, Ordering::Relaxed);
            }
            if body_ns > 0 {
                confirm_load_stats::COLD_RANGE_BODY_NS.fetch_add(body_ns, Ordering::Relaxed);
            }
            if dec_ns > 0 {
                confirm_load_stats::COLD_RANGE_DECODE_NS.fetch_add(dec_ns, Ordering::Relaxed);
            }
            confirm_load_stats::COLD_RANGE_N.fetch_add(n_range, Ordering::Relaxed);
            for ((_fk, _range, _tid, need), row) in by_range.into_iter().zip(decoded.into_iter()) {
                let Some(id) = _fk.get() else {
                    continue;
                };
                let Some((tx, live, _sparse)) = row else {
                    continue;
                };
                let _ = need;
                plan.external_parent_outs
                    .insert(id, std::sync::Arc::new((tx, live)));
            }
            confirm_load_stats::BODY_TX_READS.fetch_add(n_range, Ordering::Relaxed);
            confirm_load_stats::PIN_NEW.fetch_add(n_range, Ordering::Relaxed);
        }
        if !need_idx.is_empty() {
            let t_idx = Instant::now();
            let loaded =
                rbitcoin_query::load_creates_once(query.store(), &need_idx, IdxBodyMode::Outs)
                    .map_err(ConsensusError::from)?;
            let idx_ns = t_idx.elapsed().as_nanos() as u64;
            let n_idx = loaded.len() as u64;
            if idx_ns > 0 {
                confirm_load_stats::COLD_IDX_NS.fetch_add(idx_ns, Ordering::Relaxed);
            }
            if n_idx > 0 {
                confirm_load_stats::COLD_IDX_N.fetch_add(n_idx, Ordering::Relaxed);
            }
            confirm_load_stats::BODY_TX_READS.fetch_add(n_idx, Ordering::Relaxed);
            confirm_load_stats::FULL_TX_READS.fetch_add(n_idx, Ordering::Relaxed);
            confirm_load_stats::PIN_NEW.fetch_add(n_idx, Ordering::Relaxed);
            for c in loaded {
                let Some(id) = c.fk.get() else {
                    continue;
                };
                let (mut tx, outs, dens) = if let Some(dec) = c.decoded_outs {
                    dec
                } else {
                    rbitcoin_store::decode_packed_tx_outs_with_spender_rels_secret(
                        &c.raw,
                        Some(query.store().txs.store_secret()),
                    )
                    .map_err(|_| {
                        ConsensusError::Store(StoreError::Corrupt(
                            "invariant: lookup stage external parent denserels decode failed",
                        ))
                    })?
                };
                fill_create_txid_load(&mut tx, id, Some(plan))?;
                let need = parent_vouts.get(&id).cloned().unwrap_or_default();
                let live: Vec<(u32, rbitcoin_store::OutputRecord)> = if need.is_empty() {
                    outs.into_iter()
                        .enumerate()
                        .map(|(i, o)| (i as u32, o))
                        .collect()
                } else {
                    need.iter()
                        .filter_map(|&v| outs.get(v as usize).map(|o| (v, o.clone())))
                        .collect()
                };
                let _sparse = if need.is_empty() {
                    dens.into_iter()
                        .enumerate()
                        .filter(|(_, r)| *r != rbitcoin_query::SPENDER_REL_UNKNOWN)
                        .map(|(i, r)| (i as u32, r))
                        .collect()
                } else {
                    rbitcoin_query::sparse_spender_rels(&dens, &need)
                };
                plan.external_parent_outs
                    .insert(id, std::sync::Arc::new((tx, live)));
            }
        }
        cold_io_ns = t_io.elapsed().as_nanos() as u64;
        if cold_io_ns > 0 {
            confirm_load_stats::COLD_IO_NS.fetch_add(cold_io_ns, Ordering::Relaxed);
            confirm_load_stats::PIN_NEW_META_NS.fetch_add(cold_io_ns, Ordering::Relaxed);
        }
        // Union miss is Corrupt: every cold parent must be a plan-local sparse pin.
        for fk in &cold_fks {
            let id = fk.get().unwrap_or(0);
            if plan
                .external_parent_outs
                .get(&id)
                .is_none_or(|pin| pin.1.is_empty())
            {
                return Err(ConsensusError::Store(StoreError::Corrupt(
                    "invariant: lookup stage failed to load external parent denserels",
                )));
            }
        }
    }

    st.work_ns = t0.elapsed().as_nanos() as u64;
    lookup_stage_stats::note(
        0, // blocks counted by caller
        st.parents as u64,
        st.already as u64,
        st.cold as u64,
        st.same_batch as u64,
        0,
        collect_ns,
        0,
        cold_io_ns,
    );
    if st.work_ns > 0 {
        confirm_load_stats::PARENT_PIN_NS.fetch_add(st.work_ns, Ordering::Relaxed);
        confirm_load_stats::NS.fetch_add(st.work_ns, Ordering::Relaxed);
    }
    if st.parents > 0 {
        confirm_load_stats::PARENT_UNIQUE.fetch_add(st.parents as u64, Ordering::Relaxed);
    }
    if st.already > 0 {
        confirm_load_stats::PIN_CACHE_BODY.fetch_add(st.already as u64, Ordering::Relaxed);
    }
    Ok(st)
}

/// Lookup-stamped external parent material for load body denserels.
///
/// **Lookup** fills this via `tx.head` / `tx.idx` / `txid.body` (never `tx.body`).
/// **Load** denserels by range using only these maps (+ plan offline pins).
/// Integer create_fk maps use [`U64Map`] (identity hasher) — pack-scale win over SipHash.
#[derive(Debug, Default, Clone)]
pub struct ParentPinStamp {
    /// create_fk_id → Class A body range.
    pub ranges: U64Map<(u64, u64)>,
    /// create_fk_id → create txid (wire / sidefile at lookup).
    pub txids: U64Map<[u8; 32]>,
    /// prev_txid → create_fk_id (plan=None thin edges without head on load).
    pub create_by_txid: HashMap<[u8; 32], u64>,
}

impl ParentPinStamp {
    /// Move plan stamp maps into the load stamp (no 100k-entry clone).
    pub(crate) fn take_from_plan(plan: &mut rbitcoin_query::ArchiveWritePlan) -> Self {
        Self::from_maps(
            std::mem::take(&mut plan.external_parent_ranges),
            std::mem::take(&mut plan.external_parent_txids),
        )
    }

    fn from_maps(
        ranges: rbitcoin_query::U64Map<(u64, u64)>,
        txids: rbitcoin_query::U64Map<[u8; 32]>,
    ) -> Self {
        let mut create_by_txid = HashMap::with_capacity(txids.len());
        for (id, tid) in &txids {
            create_by_txid.insert(*tid, *id);
        }
        Self {
            ranges,
            txids,
            create_by_txid,
        }
    }

    #[inline]
    pub(super) fn create_txid(&self, create_fk_id: u64) -> Option<[u8; 32]> {
        self.txids
            .get(&create_fk_id)
            .copied()
            .filter(|t| *t != [0u8; 32])
    }
}

/// Lookup-stage output: structure + plan batch (create_fk + parent body ranges).
///
/// **No `tx.body` denserels on lookup.** Load denserels by range from
/// [`ParentPinStamp`] / plan ranges. Handoff is owned plan + parent pin stamp.
pub struct PlanStampOutcome {
    pub plan: Option<rbitcoin_query::ArchiveWritePlan>,
    /// External parent fk/range/txid stamped at lookup (always; including plan=None).
    pub parent_pin: ParentPinStamp,
    /// Wall ns for structure + plan_batch (head stamp).
    pub work_ns: u64,
    metas: Vec<BodyMeta>,
    wire_blocks: Vec<Arc<Block>>,
}

/// IBD **lookup** stage: structure + stamp create_fk + parent body ranges.
///
/// May read `tx.head`, `tx.idx`, `txid.body`. **Never** denserels-decode `tx.body`.
/// Wire blocks are `Arc` so IBD resolve can decode once and hand off without
/// cloning full `Block` payloads into stamp.
pub fn confirm_wire_lookup_stamp(
    query: &Query,
    params: &ChainParams,
    milestone: Milestone,
    blocks: &[(Height, Arc<Block>)],
    pipeline: Option<&WireLoadPipeline>,
) -> Result<PlanStampOutcome, ConsensusError> {
    confirm_wire_lookup_stamp_with_hits(query, params, milestone, blocks, pipeline, None)
}

/// Like [`confirm_wire_lookup_stamp`] with BQ-ahead hits.
///
/// IBD load stamps from those hits plus in-flight / live pins, then TipOnly
/// `tx.head` for remaining parents (open head / ages ≤3 sealed). Not TipThenAny.
pub fn confirm_wire_lookup_stamp_with_hits(
    query: &Query,
    params: &ChainParams,
    milestone: Milestone,
    blocks: &[(Height, Arc<Block>)],
    pipeline: Option<&WireLoadPipeline>,
    pre_resolved: Option<&rbitcoin_store::BqParentHits>,
) -> Result<PlanStampOutcome, ConsensusError> {
    let t0 = Instant::now();
    query.on_load_pack().map_err(ConsensusError::from)?;
    let (mut plan, metas, wire_blocks, plan_ns) =
        wire_lookup_phase(query, params, milestone, blocks, pipeline, pre_resolved)?;
    let ifo = pipeline.map(|p| &p.in_flight);
    let parent_pin = match plan.as_mut() {
        Some(p) => ParentPinStamp::take_from_plan(p),
        None => stamp_parent_pin_archived(query, params, &metas, &wire_blocks, ifo)?,
    };
    lookup_stage_stats::BLOCKS.fetch_add(blocks.len() as u64, Ordering::Relaxed);
    lookup_stage_stats::HEAD_NS.fetch_add(plan_ns, Ordering::Relaxed);
    let work_ns = t0.elapsed().as_nanos() as u64;
    lookup_stage_stats::TOTAL_NS.fetch_add(work_ns, Ordering::Relaxed);
    Ok(PlanStampOutcome {
        plan,
        parent_pin,
        work_ns,
        metas,
        wire_blocks,
    })
}

/// plan=None rehydrate: stamp external parent create_fk + body_range + txid
/// via head/idx/txid.body so load never probes those tables.
pub(super) fn stamp_parent_pin_archived(
    query: &Query,
    params: &ChainParams,
    metas: &[BodyMeta],
    wire_blocks: &[Arc<Block>],
    in_flight: Option<&rbitcoin_query::InFlightView>,
) -> Result<ParentPinStamp, ConsensusError> {
    let mut same_batch: HashMap<[u8; 32], u64> = HashMap::new();
    for m in metas {
        for (tid, fk) in m.txids.iter().zip(m.tx_fks.iter()) {
            if let Some(id) = fk.get() {
                same_batch.insert(*tid, id);
            }
        }
    }
    let mut need_external: HashMap<[u8; 32], ()> = HashMap::new();
    for (m, block) in metas.iter().zip(wire_blocks.iter()) {
        let _ = m;
        for tx in &block.txdata {
            for inp in &tx.input {
                if inp.previous_output.is_null() {
                    continue;
                }
                let prev = inp.previous_output.txid.to_byte_array();
                if same_batch.contains_key(&prev) {
                    continue;
                }
                if prev != [0u8; 32] {
                    need_external.insert(prev, ());
                }
            }
        }
        // BIP30 (pre-BIP34): same head wave as parents. TipOnly returns a
        // connected sibling if this create would overwrite a live txid.
        if !params.bip34_active_at(m.height.0) {
            for tx in &block.txdata {
                need_external.insert(tx.compute_txid().to_byte_array(), ());
            }
        }
    }
    let mut stamp = ParentPinStamp::default();
    for (tid, id) in &same_batch {
        stamp.create_by_txid.insert(*tid, *id);
        stamp.txids.insert(*id, *tid);
        // Same-batch denserels are offline at pin — range optional.
    }
    let mut need_head: Vec<[u8; 32]> = Vec::new();
    for tid in need_external.keys() {
        if let Some(ifo) = in_flight {
            if let Some(fk) = ifo.get_create_fk(tid) {
                if let Some(id) = fk.get() {
                    stamp.create_by_txid.insert(*tid, id);
                    stamp.txids.insert(id, *tid);
                    if ifo.get_out(id).is_none() {
                        // In-flight create without outs: body is on disk — fill range below.
                        need_head.push(*tid);
                    }
                    continue;
                }
            }
        }
        need_head.push(*tid);
    }
    need_head.sort_unstable();
    need_head.dedup();
    need_head.retain(|t| {
        stamp
            .create_by_txid
            .get(t)
            .map(|id| !stamp.ranges.contains_key(id))
            .unwrap_or(true)
    });
    if !need_head.is_empty() {
        need_head.sort_unstable_by_key(|txid| query.store().txs.head_primary_slot(txid));
        let hits = query
            .store()
            .get_fk_by_txid_batch(&need_head)
            .map_err(ConsensusError::from)?;
        for (txid, row) in hits {
            if let Some((fk, range)) = row {
                if let Some(id) = fk.get() {
                    stamp.create_by_txid.insert(txid, id);
                    stamp.txids.insert(id, txid);
                    stamp.ranges.insert(id, range);
                }
            }
        }
    }
    // plan=None same-batch already-archived creates have no CreatePin offline — idx body_range.
    let mut need_range: Vec<rbitcoin_primitives::Fk> = Vec::new();
    let mut seen = U64Set::default();
    for (&id, _) in &stamp.txids {
        if stamp.ranges.contains_key(&id) {
            continue;
        }
        if in_flight.and_then(|i| i.get_out(id)).is_some() {
            continue;
        }
        if seen.insert(id) {
            need_range.push(rbitcoin_primitives::Fk(id));
        }
    }
    if !need_range.is_empty() {
        let ranges = query
            .store()
            .tx_body_range_batch(&need_range)
            .map_err(ConsensusError::from)?;
        for (fk, row) in need_range.into_iter().zip(ranges.into_iter()) {
            let Some(id) = fk.get() else { continue };
            let Some(range) = row else {
                return Err(ConsensusError::Store(StoreError::Corrupt(
                    "archive: plan=None parent body_range missing after create_fk stamp",
                )));
            };
            stamp.ranges.insert(id, range);
        }
    }
    // Identities are stamped from wire prev_txid at insert time — never soft-fill
    // from txid.body here (that would be a dual path after lookup promised identity).
    for (&id, tid) in &stamp.txids {
        if *tid == [0u8; 32] {
            return Err(ConsensusError::Store(StoreError::Corrupt(
                "invariant: plan=None parent stamp zero create identity",
            )));
        }
        let _ = id;
    }
    Ok(stamp)
}

/// IBD **load** after lookup denserels ensure: pin + assemble.
///
/// Uses the owned stamped plan — does **not** re-run plan_batch / head resolve.
/// Single path: denserels by body range from lookup stamp (plan-local or
/// plan=None `ParentPinStamp`). Never cold dual-path denserels / txid.body on load.
pub fn confirm_wire_load_from_plan(
    query: &Query,
    params: &ChainParams,
    milestone: Milestone,
    stamped: PlanStampOutcome,
    pipeline: Option<&WireLoadPipeline>,
    preverified: &ScriptPreverified,
) -> Result<ConfirmLoadOutcome, ConsensusError> {
    let t_work = Instant::now();
    let t_load = Instant::now();
    let PlanStampOutcome {
        mut plan,
        parent_pin,
        metas,
        wire_blocks,
        ..
    } = stamped;

    let ifo = pipeline.map(|p| &p.in_flight);
    let parent_store = pipeline.map(|p| &p.parent_store);
    let (batch_parents, batch_thin, _warm) = pin_for_wire_batch(
        query,
        plan.as_ref(),
        &parent_pin,
        &metas,
        &wire_blocks,
        ifo,
        parent_store,
    )?;
    if let Some(ref mut p) = plan {
        p.freeze_after_pin();
    }

    confirm_phase_stats::LOAD_NS.fetch_add(t_load.elapsed().as_nanos() as u64, Ordering::Relaxed);

    let prepared = assemble_run(
        query,
        params,
        milestone,
        metas,
        &wire_blocks,
        &batch_parents,
        &batch_thin,
    )?;
    drop(batch_thin);

    let work_ns = t_work.elapsed().as_nanos() as u64;
    Ok(ConfirmLoadOutcome {
        batch: LoadedBatch {
            prepared,
            wire_blocks,
            batch_parents,
            script_preverified: preverified.clone(),
            archive_plan: plan,
        },
        work_ns,
    })
}

/// Structure + prepare + plan_batch only (stamp create_fk). Shared by lookup stage.
pub(super) fn wire_lookup_phase(
    query: &Query,
    params: &ChainParams,
    milestone: Milestone,
    blocks: &[(Height, Arc<Block>)],
    pipeline: Option<&WireLoadPipeline>,
    pre_resolved: Option<&rbitcoin_store::BqParentHits>,
) -> Result<
    (
        Option<rbitcoin_query::ArchiveWritePlan>,
        Vec<BodyMeta>,
        Vec<Arc<Block>>,
        u64,
    ),
    ConsensusError,
> {
    if blocks.is_empty() {
        return Err(ConsensusError::BadBlock("empty confirm batch"));
    }
    for w in blocks.windows(2) {
        if w[1].0 .0 != w[0].0 .0.saturating_add(1) {
            return Err(ConsensusError::BadBlock("confirm run not contiguous"));
        }
    }

    let mut with_fk: Vec<(
        rbitcoin_primitives::Fk,
        rbitcoin_store::HeaderRecord,
        Vec<rbitcoin_query::TxApply>,
    )> = Vec::with_capacity(blocks.len());
    let mut wire_blocks: Vec<Arc<Block>> = Vec::with_capacity(blocks.len());
    let mut metas: Vec<BodyMeta> = Vec::with_capacity(blocks.len());

    let tip_h = query.tip_height().map(|h| h.0);
    let store_path_lo = match tip_h {
        None => 0u32,
        Some(t) => t.saturating_add(1),
    };
    let path_lo = pipeline.map(|p| p.path_lo).unwrap_or(store_path_lo);

    let mut struct_ns = 0u64;
    let mut prepare_ns = 0u64;

    for (i, (height, block)) in blocks.iter().enumerate() {
        let block = Arc::clone(block);
        let hash = block.block_hash().to_byte_array();
        let ctx = ValidationContext::at(params, *height, milestone);
        let t_struct = Instant::now();
        // Sole compute_txid pass for this block in the confirm pipeline.
        let txids = crate::block::validate_block_structure_hashed(block.as_ref(), &ctx)?;
        if i == 0 {
            if height.0 != path_lo {
                return Err(ConsensusError::BadPrev);
            }
            if path_lo == store_path_lo {
                validate_header(query, params, *height, &block.header)?;
            } else {
                let expect_prev = pipeline.and_then(|p| p.parent_hash).unwrap_or([0u8; 32]);
                if block.header.prev_blockhash.to_byte_array() != expect_prev {
                    return Err(ConsensusError::BadPrev);
                }
                let target = bitcoin::Target::from_compact(block.header.bits);
                if target > params.pow_limit {
                    return Err(ConsensusError::BadHeader("target above pow limit"));
                }
                block
                    .header
                    .validate_pow(target)
                    .map_err(|_| ConsensusError::InvalidPow)?;
            }
        } else {
            // Prev wire hash already on metas[i-1] — no rehash.
            let prev_hash = metas[i - 1].hash;
            if block.header.prev_blockhash.to_byte_array() != prev_hash {
                return Err(ConsensusError::BadPrev);
            }
            let target = bitcoin::Target::from_compact(block.header.bits);
            if target > params.pow_limit {
                return Err(ConsensusError::BadHeader("target above pow limit"));
            }
            block
                .header
                .validate_pow(target)
                .map_err(|_| ConsensusError::InvalidPow)?;
        }
        struct_ns = struct_ns.saturating_add(t_struct.elapsed().as_nanos() as u64);

        let t_prep = Instant::now();
        let (header_rec, txs) =
            crate::prepare_block_for_archive_with_txids(query, block.as_ref(), &txids)?;
        let header_fk = if let Some((fk, _)) = query
            .get_header_by_hash(&header_rec.hash)
            .map_err(ConsensusError::from)?
        {
            fk
        } else {
            query
                .store()
                .put_header(&header_rec)
                .map_err(ConsensusError::from)?
        };
        let prev_bytes = block.header.prev_blockhash.to_byte_array();
        query.confirm_parent_cache().put_header_plan(
            height.0,
            header_fk,
            header_rec.clone(),
            Vec::new(),
            prev_bytes,
        );
        prepare_ns = prepare_ns.saturating_add(t_prep.elapsed().as_nanos() as u64);
        with_fk.push((header_fk, header_rec.clone(), txs));
        wire_blocks.push(block);
        metas.push(BodyMeta {
            height: *height,
            hash,
            header_fk,
            header_rec,
            tx_fks: Vec::new(),
            txids,
        });
    }

    let t_filter = Instant::now();
    let (_header_fks, mut need) = query
        .archive_filter_need_bodies(&mut with_fk)
        .map_err(ConsensusError::from)?;
    let filter_ns = t_filter.elapsed().as_nanos() as u64;
    let t_batch = Instant::now();
    let plan = if need.is_empty() {
        for (i, m) in metas.iter_mut().enumerate() {
            if let Some(list) = query
                .store()
                .header_txs
                .get_list(m.header_fk)
                .map_err(ConsensusError::from)?
            {
                m.tx_fks = list;
            }
            // Never rehash wire for lookup — index by batch position.
            let prev = wire_blocks[i].header.prev_blockhash.to_byte_array();
            query.confirm_parent_cache().put_header_plan(
                m.height.0,
                m.header_fk,
                m.header_rec.clone(),
                m.tx_fks.clone(),
                prev,
            );
        }
        None
    } else {
        let plan = match pipeline {
            Some(p) => query
                .archive_plan_batch_from_store(
                    &mut need,
                    p.next_tx_start.max(1),
                    &p.in_flight,
                    Some(p.parent_store.as_ref()),
                    pre_resolved,
                )
                .map_err(ConsensusError::from)?,
            None => query
                .archive_plan_batch_from_store(
                    &mut need,
                    query.tx_body_count().saturating_add(1).max(1),
                    &rbitcoin_query::InFlightView::empty(),
                    None,
                    pre_resolved,
                )
                .map_err(ConsensusError::from)?,
        };
        let by_header = create_fks_from_header_ranges(&plan.per_header_ranges);
        for (i, m) in metas.iter_mut().enumerate() {
            if let Some(id) = m.header_fk.get() {
                if let Some(fks) = by_header.get(&id) {
                    m.tx_fks = fks.clone();
                }
            }
            if m.tx_fks.is_empty() {
                if let Some(list) = query
                    .store()
                    .header_txs
                    .get_list(m.header_fk)
                    .map_err(ConsensusError::from)?
                {
                    m.tx_fks = list;
                }
            }
            let prev = wire_blocks[i].header.prev_blockhash.to_byte_array();
            query.confirm_parent_cache().put_header_plan(
                m.height.0,
                m.header_fk,
                m.header_rec.clone(),
                m.tx_fks.clone(),
                prev,
            );
        }
        Some(plan)
    };
    let batch_ns = t_batch.elapsed().as_nanos() as u64;
    // plan_ns for HEAD_NS: filter + batch (legacy “lookup wall” without struct/prepare).
    let plan_ns = filter_ns.saturating_add(batch_ns);
    plan_stamp_sub_stats::note(struct_ns, prepare_ns, filter_ns, batch_ns);
    Ok((plan, metas, wire_blocks, plan_ns))
}

pub(super) fn create_fks_from_header_ranges(
    per_header_ranges: &[(rbitcoin_primitives::Fk, rbitcoin_primitives::Fk, u32)],
) -> U64Map<Vec<rbitcoin_primitives::Fk>> {
    let mut by_header: U64Map<Vec<rbitcoin_primitives::Fk>> = U64Map::default();
    for &(hfk, first, n) in per_header_ranges {
        let Some(hid) = hfk.get() else { continue };
        let mut slice = Vec::with_capacity(n as usize);
        for i in 0..n {
            slice.push(rbitcoin_primitives::Fk(
                first.0.saturating_add(u64::from(i)),
            ));
        }
        by_header.insert(hid, slice);
    }
    by_header
}

/// Stamp-phase sub-walls for lookup_thr diagnosis (structure / prepare / filter / batch).
///
/// Batch is the archive plan_batch wall (assign+collect+inflight+head_fk+stamp+finish
/// already timed in `archive_phase_stats`). `head_fk` = leftover TipOnly
/// `get_fk_by_txid_batch`. Window sum is [`sample_and_reset`].
pub mod plan_stamp_sub_stats {
    use std::sync::atomic::{AtomicU64, Ordering};

    static STRUCT_NS: AtomicU64 = AtomicU64::new(0);
    static PREPARE_NS: AtomicU64 = AtomicU64::new(0);
    static FILTER_NS: AtomicU64 = AtomicU64::new(0);
    static BATCH_NS: AtomicU64 = AtomicU64::new(0);

    pub fn note(struct_ns: u64, prepare_ns: u64, filter_ns: u64, batch_ns: u64) {
        if struct_ns > 0 {
            STRUCT_NS.fetch_add(struct_ns, Ordering::Relaxed);
        }
        if prepare_ns > 0 {
            PREPARE_NS.fetch_add(prepare_ns, Ordering::Relaxed);
        }
        if filter_ns > 0 {
            FILTER_NS.fetch_add(filter_ns, Ordering::Relaxed);
        }
        if batch_ns > 0 {
            BATCH_NS.fetch_add(batch_ns, Ordering::Relaxed);
        }
    }

    #[derive(Debug, Default, Clone, Copy)]
    pub struct Sample {
        pub struct_ns: u64,
        pub prepare_ns: u64,
        pub filter_ns: u64,
        pub batch_ns: u64,
    }

    pub fn sample_and_reset() -> Sample {
        Sample {
            struct_ns: STRUCT_NS.swap(0, Ordering::Relaxed),
            prepare_ns: PREPARE_NS.swap(0, Ordering::Relaxed),
            filter_ns: FILTER_NS.swap(0, Ordering::Relaxed),
            batch_ns: BATCH_NS.swap(0, Ordering::Relaxed),
        }
    }
}

/// Accumulators for the **lookup** pipeline stage (plan+stamp + denserels ensure).
pub mod lookup_stage_stats {
    use std::sync::atomic::{AtomicU64, Ordering};

    pub static BLOCKS: AtomicU64 = AtomicU64::new(0);
    pub static PARENTS: AtomicU64 = AtomicU64::new(0);
    pub static ALREADY: AtomicU64 = AtomicU64::new(0);
    pub static COLD: AtomicU64 = AtomicU64::new(0);
    pub static UNRESOLVED: AtomicU64 = AtomicU64::new(0);
    pub static TOTAL_NS: AtomicU64 = AtomicU64::new(0);
    pub static COLLECT_NS: AtomicU64 = AtomicU64::new(0);
    pub static HEAD_NS: AtomicU64 = AtomicU64::new(0);
    pub static COLD_IO_NS: AtomicU64 = AtomicU64::new(0);

    pub fn note(
        blocks: u64,
        parents: u64,
        already: u64,
        cold: u64,
        unresolved: u64,
        total_ns: u64,
        collect_ns: u64,
        head_ns: u64,
        cold_io_ns: u64,
    ) {
        if blocks > 0 {
            BLOCKS.fetch_add(blocks, Ordering::Relaxed);
        }
        if parents > 0 {
            PARENTS.fetch_add(parents, Ordering::Relaxed);
        }
        if already > 0 {
            ALREADY.fetch_add(already, Ordering::Relaxed);
        }
        if cold > 0 {
            COLD.fetch_add(cold, Ordering::Relaxed);
        }
        if unresolved > 0 {
            UNRESOLVED.fetch_add(unresolved, Ordering::Relaxed);
        }
        if total_ns > 0 {
            TOTAL_NS.fetch_add(total_ns, Ordering::Relaxed);
        }
        if collect_ns > 0 {
            COLLECT_NS.fetch_add(collect_ns, Ordering::Relaxed);
        }
        if head_ns > 0 {
            HEAD_NS.fetch_add(head_ns, Ordering::Relaxed);
        }
        if cold_io_ns > 0 {
            COLD_IO_NS.fetch_add(cold_io_ns, Ordering::Relaxed);
        }
    }

    #[derive(Debug, Default, Clone, Copy)]
    pub struct Sample {
        pub blocks: u64,
        pub parents: u64,
        pub already: u64,
        pub cold: u64,
        pub unresolved: u64,
        pub total_ns: u64,
        pub collect_ns: u64,
        pub head_ns: u64,
        pub cold_io_ns: u64,
    }

    pub fn sample_and_reset() -> Sample {
        Sample {
            blocks: BLOCKS.swap(0, Ordering::Relaxed),
            parents: PARENTS.swap(0, Ordering::Relaxed),
            already: ALREADY.swap(0, Ordering::Relaxed),
            cold: COLD.swap(0, Ordering::Relaxed),
            unresolved: UNRESOLVED.swap(0, Ordering::Relaxed),
            total_ns: TOTAL_NS.swap(0, Ordering::Relaxed),
            collect_ns: COLLECT_NS.swap(0, Ordering::Relaxed),
            head_ns: HEAD_NS.swap(0, Ordering::Relaxed),
            cold_io_ns: COLD_IO_NS.swap(0, Ordering::Relaxed),
        }
    }
}

/// Create identity for **load** pin denserels: plan stamp reverse map only.
///
/// **Load never reads `txid.body`.** Lookup stamps `external_parent_txids` from
/// wire `prev_txid` (or lookup-side `txid.body` for plan=None rehydrate). Missing
/// identity here is a lookup miss, not a sidefile fallback.
#[inline]
pub(super) fn known_create_txid_load(
    create_fk_id: u64,
    plan: Option<&rbitcoin_query::ArchiveWritePlan>,
) -> Result<[u8; 32], ConsensusError> {
    if let Some(p) = plan {
        if let Some(tid) = p.external_parent_txid(create_fk_id) {
            if tid != [0u8; 32] {
                return Ok(tid);
            }
        }
    }
    Err(ConsensusError::Store(StoreError::Corrupt(
        "invariant: lookup stage miss (load parent create identity not stamped)",
    )))
}

/// Lookup-side identity fill: plan RAM first, else `txid.body` (lookup may read
/// the sidefile; load must not call this).
#[inline]
pub(super) fn known_create_txid_lookup(
    query: &Query,
    create_fk_id: u64,
    plan: Option<&rbitcoin_query::ArchiveWritePlan>,
) -> Result<[u8; 32], ConsensusError> {
    if let Some(p) = plan {
        if let Some(tid) = p.external_parent_txid(create_fk_id) {
            if tid != [0u8; 32] {
                return Ok(tid);
            }
        }
    }
    let tid = query
        .store()
        .txs
        .body_txid(rbitcoin_primitives::Fk(create_fk_id))
        .map_err(ConsensusError::from)?;
    if tid == [0u8; 32] {
        return Err(ConsensusError::Store(StoreError::Corrupt(
            "invariant: pin parent create identity still zero after txid.body",
        )));
    }
    Ok(tid)
}

/// Schema-13 denserels decode leaves zero identity — stamp from plan RAM only (load).
#[inline]
pub(super) fn fill_create_txid_load(
    tx: &mut rbitcoin_store::TxRecord,
    create_fk_id: u64,
    plan: Option<&rbitcoin_query::ArchiveWritePlan>,
) -> Result<(), ConsensusError> {
    if tx.txid != [0u8; 32] {
        return Ok(());
    }
    tx.txid = known_create_txid_load(create_fk_id, plan)?;
    Ok(())
}
