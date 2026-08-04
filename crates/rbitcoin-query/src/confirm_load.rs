//! Confirm **load** stage: Class A decode + parent pin for one claimed batch.
//!
//! For each height in the batch (ascending):
//! 1. **Cache** header + `header_txs` (process-local, tip-GCed).
//! 2. **Full Class A decode once** into [`BatchFullBodies`] (wire).
//! 3. **Thin edges** + **sparse parent pin** as **batch-local** maps
//!    (same-batch bodies, then cold denserels). Body ranges from `tx.idx`.

use super::*;
use crate::batch_full_bodies::BatchFullBodies;
use crate::batch_parents::BatchParents;
use crate::wave_prevout::ThinInput;
use std::collections::{HashMap, HashSet};
use std::time::Instant;

/// Spend-fk → thin create_fk edges for one confirm batch (assemble only).
pub type BatchThin = HashMap<u64, Vec<ThinInput>>;

#[derive(Debug, Default, Clone, Copy)]
pub struct ConfirmLoadStats {
    pub blocks: u32,
    pub utxo_parents: u32,
    pub creates_registered: u32,
    /// Unique parent create fks pinned this call (after dedup).
    pub parent_unique: u32,
    /// Of `parent_unique`: filled without store denserels IO (same-batch / plan-local).
    pub pin_cache_body: u32,
    /// Of `parent_unique`: missed same-batch (cold denserels).
    pub pin_new: u32,
    /// Historical: spent-filter during pin (now always 0 — structural owns spentness).
    pub pin_spent_ns: u64,
    /// FIFO hit path resolve (excludes spent timer).
    pub pin_body_ns: u64,
    /// pin_new meta/outs resolve (excludes spent timer).
    pub pin_new_meta_ns: u64,
    /// Same-batch create edges (identity known in-batch).
    pub parent_cache_hits: u32,
    /// Stamped create_fk on input, parent **not** in this batch (external fk).
    pub edge_fk: u32,
    /// Body txs full-decoded (phase 1).
    pub body_tx_reads: u32,
    /// Parent outs loaded from store (sparse pin).
    pub full_tx_reads: u32,
    /// Unstamped non-coinbase edges (should not occur on healthy v10 Class A).
    pub missing_parents: u32,
    /// Phase wall times (ns).
    pub header_ns: u64,
    pub body_decode_ns: u64,
    pub thin_ns: u64,
    pub parent_pin_ns: u64,
    pub cache_put_ns: u64,
    pub edge_same_batch: u32,
    pub edge_coinbase: u32,
}

impl Query {
    pub fn parent_cache_ready_through(&self) -> u32 {
        self.confirm_parents.ready_through()
    }

    /// Snapshot: `(ready_through, ahead, sparse_parents, bodies, plans)`.
    ///
    /// `ahead` is ready_through − tip (in-flight load watermark, not a depth knobs).
    /// Third field is always 0 (sparse parents are per-batch, not shared).
    pub fn parent_cache_perf_snapshot(&self) -> (u32, u32, usize, usize, usize) {
        let tip = self.tip_height().map(|h| h.0).unwrap_or(0);
        let through = self.confirm_parents.ready_through();
        let ahead = through.saturating_sub(tip);
        (
            through,
            ahead,
            0,
            0, // legacy bodies slot (process pin FIFO removed)
            self.confirm_parents.plan_count(),
        )
    }

    pub fn advance_parent_cache_tip(&self, tip: u32) {
        self.confirm_parents.advance_tip(tip);
    }

    pub fn seed_parent_cache(&self, items: &[(u32, [u8; 32])]) {
        self.confirm_parents.ensure_plans(items);
    }

    /// True when every height has been load-scanned (watermark / tests).
    pub fn is_confirm_load_ready(&self, heights: &[u32]) -> bool {
        self.confirm_parents.all_ready(heights)
    }

    /// Load Class A for heights: **per-batch** parent pin + thin edges.
    ///
    /// Returns `(stats, batch_parents, batch_thin, batch_bodies)`. Thin edges and
    /// full bodies are assemble/wire-only (pipeline pins; no process pin FIFO).
    pub fn load_confirm_parents(
        &self,
        items: &[(u32, [u8; 32])],
    ) -> Result<(ConfirmLoadStats, BatchParents, BatchThin, BatchFullBodies), QueryError> {
        let t0 = Instant::now();
        let mut st = ConfirmLoadStats::default();
        let mut batch_parents = BatchParents::new();
        let mut batch_thin = BatchThin::new();
        let mut batch_bodies = BatchFullBodies::new();
        if items.is_empty() {
            return Ok((st, batch_parents, batch_thin, batch_bodies));
        }
        // None tip: include genesis (height 0). `unwrap_or(0)` would skip h=0.
        let tip = self.tip_height().map(|h| h.0);

        // Always re-decode / re-pin claimed heights (batch-local thin + parents).
        let mut work: Vec<(u32, [u8; 32])> = Vec::new();
        for &(height, hash) in items {
            if self.confirm_cancelled() {
                crate::confirm_load_stats::note(&st, t0.elapsed().as_nanos() as u64);
                return Err(StoreError::Cancelled("confirm cancelled"));
            }
            if let Some(t) = tip {
                if height <= t {
                    continue;
                }
            }
            work.push((height, hash));
        }
        if work.is_empty() {
            crate::confirm_load_stats::note(&st, t0.elapsed().as_nanos() as u64);
            return Ok((st, batch_parents, batch_thin, batch_bodies));
        }
        self.confirm_parents.ensure_plans(&work);

        let mut height_tx_fks: Vec<(u32, Vec<Fk>)> = Vec::with_capacity(work.len());
        // Full-decoded bodies once for wire + FIFO outs (decode once).
        // (txid, edges): each edge is (create_fk_opt, soft prev_txid, vout).
        // v10: create_fk is stamped at archive; soft prev_txid may be zero.
        let mut body_prevouts: HashMap<u64, ([u8; 32], Vec<(Option<u64>, [u8; 32], u32)>)> =
            HashMap::new();
        let mut parent_need: HashMap<u64, Vec<u32>> = HashMap::new(); // parent_fk → need heights
        // parent_fk → needed prev_index (vouts) for sparse outs stash.
        let mut parent_vouts: HashMap<u64, Vec<u32>> = HashMap::new();
        let mut thin_by_spend: BatchThin = BatchThin::new();
        let mut batch_create_ids: HashSet<u64> = HashSet::new();

        for &(height, hash) in &work {
            if self.confirm_cancelled() {
                crate::confirm_load_stats::note(&st, t0.elapsed().as_nanos() as u64);
                return Err(StoreError::Cancelled("confirm cancelled"));
            }

            // ── Header + header_txs ────────────────────────────────────────
            let t_hdr = Instant::now();
            let Some((header_fk, header_rec)) = self.store.get_header_by_hash(&hash)? else {
                st.header_ns = st.header_ns.saturating_add(t_hdr.elapsed().as_nanos() as u64);
                continue;
            };
            if !self.store.header_txs.has_body(header_fk)? {
                st.header_ns = st.header_ns.saturating_add(t_hdr.elapsed().as_nanos() as u64);
                continue;
            }
            let Some(tx_fks) = self.store.header_txs.get_list(header_fk)? else {
                st.header_ns = st.header_ns.saturating_add(t_hdr.elapsed().as_nanos() as u64);
                continue;
            };
            if tx_fks.is_empty() {
                st.header_ns = st.header_ns.saturating_add(t_hdr.elapsed().as_nanos() as u64);
                continue;
            }
            // Body readiness: first tx must have an idx range (no tx.head probe).
            if let Some(&first) = tx_fks.first() {
                if self.store.tx_body_range(first).is_err() {
                    st.header_ns = st.header_ns.saturating_add(t_hdr.elapsed().as_nanos() as u64);
                    continue;
                }
            }
            let prev_hash = if header_rec.prev_fk.is_null() {
                [0u8; 32]
            } else {
                match self.store.get_header(header_rec.prev_fk) {
                    Ok(prev) => prev.hash,
                    Err(_) => {
                        st.header_ns =
                            st.header_ns.saturating_add(t_hdr.elapsed().as_nanos() as u64);
                        continue;
                    }
                }
            };
            self.confirm_parents.put_header_plan(
                height,
                header_fk,
                header_rec,
                tx_fks.clone(),
                prev_hash,
            );
            st.header_ns = st.header_ns.saturating_add(t_hdr.elapsed().as_nanos() as u64);

            // ── body ranges + full Class A decode ─────────────────────────
            st.blocks = st.blocks.saturating_add(1);
            let t_dec = Instant::now();

            let fks_work: std::borrow::Cow<'_, [Fk]> = if tx_fks_is_sorted_ascending(&tx_fks) {
                std::borrow::Cow::Borrowed(tx_fks.as_slice())
            } else {
                let mut v = tx_fks.clone();
                v.sort_unstable_by_key(|f| f.0);
                std::borrow::Cow::Owned(v)
            };

            let range_fks: Vec<Fk> = fks_work
                .iter()
                .copied()
                .filter(|fk| fk.get().is_some())
                .collect();
            // Idx→body + one full decode (in load_creates_once); no re-decode of raw.
            let creates = crate::combined_stage::load_creates_once(
                &self.store,
                &range_fks,
                rbitcoin_store::IdxBodyMode::Full,
            )?;
            let mut by_fk: HashMap<u64, crate::combined_stage::CombinedCreate> =
                HashMap::with_capacity(creates.len());
            for c in creates {
                by_fk.insert(c.fk.get().unwrap_or(0), c);
            }
            for fk in &range_fks {
                if self.confirm_cancelled() {
                    crate::confirm_load_stats::note(&st, t0.elapsed().as_nanos() as u64);
                    return Err(StoreError::Cancelled("confirm cancelled"));
                }
                let Some(id) = fk.get() else {
                    continue;
                };
                let Some(c) = by_fk.remove(&id) else {
                    return Err(StoreError::Corrupt(
                        "invariant: confirm load create missing body range",
                    )
                    .into());
                };
                // Body was fetched (job.ok + range); Full mode must yield a decode.
                // Silent skip hid corrupt packed creates and left tip with holes.
                let Some((tx, inputs, outs, denserels)) = c.decoded_full else {
                    return Err(StoreError::Corrupt(
                        "invariant: confirm load create body decode failed",
                    )
                    .into());
                };
                let body_range = Some(c.body_range);
                st.body_tx_reads = st.body_tx_reads.saturating_add(1);
                let prevouts: Vec<(Option<u64>, [u8; 32], u32)> = inputs
                    .iter()
                    .map(|inp| {
                        let soft = if inp.create_fk.is_null() {
                            inp.prev_txid
                        } else {
                            [0u8; 32]
                        };
                        (inp.create_fk.get(), soft, inp.prev_index)
                    })
                    .collect();
                batch_create_ids.insert(id);
                body_prevouts.insert(id, (tx.txid, prevouts));
                batch_bodies.insert(*fk, height, tx, inputs, outs, body_range, denserels);
                st.creates_registered = st.creates_registered.saturating_add(1);
            }
            st.body_decode_ns = st
                .body_decode_ns
                .saturating_add(t_dec.elapsed().as_nanos() as u64);
            height_tx_fks.push((height, tx_fks));
        }

        st.cache_put_ns = 0;

        // ── Thin edges: stamped create_fk only (schema v10) ────────────────
        // Soft prev_txid / head resolve removed — archive stamps create_fk.
        let t_thin = Instant::now();
        for (height, tx_fks) in &height_tx_fks {
            for fk in tx_fks {
                let Some(id) = fk.get() else {
                    continue;
                };
                let Some((_txid, prevouts)) = body_prevouts.get(&id) else {
                    continue;
                };
                let mut edges: Vec<ThinInput> = Vec::with_capacity(prevouts.len());
                for &(create_fk_opt, _prev_txid, prev_index) in prevouts {
                    if create_fk_opt.is_none() && prev_index == u32::MAX {
                        edges.push(ThinInput {
                            create_fk: None,
                            prev_index,
                        });
                        st.edge_coinbase = st.edge_coinbase.saturating_add(1);
                        continue;
                    }
                    if let Some(pid) = create_fk_opt {
                        edges.push(ThinInput {
                            create_fk: Some(pid),
                            prev_index,
                        });
                        st.utxo_parents = st.utxo_parents.saturating_add(1);
                        parent_need.entry(pid).or_default().push(*height);
                        parent_vouts.entry(pid).or_default().push(prev_index);
                        if batch_create_ids.contains(&pid) {
                            st.parent_cache_hits = st.parent_cache_hits.saturating_add(1);
                            st.edge_same_batch = st.edge_same_batch.saturating_add(1);
                        } else {
                            st.edge_fk = st.edge_fk.saturating_add(1);
                        }
                        continue;
                    }
                    // Unstamped non-coinbase: corrupt / pre-v10 body.
                    edges.push(ThinInput {
                        create_fk: None,
                        prev_index,
                    });
                    st.missing_parents = st.missing_parents.saturating_add(1);
                }
                thin_by_spend.insert(id, edges);
            }
        }
        st.thin_ns = st.thin_ns.saturating_add(t_thin.elapsed().as_nanos() as u64);

        // ── Pin parents into per-batch BatchParents ───────────────────────
        // Sparse spent-filtered outs live on the batch object (not tip-GCed).
        // Same-batch bodies first; cold denserels for external parents.
        let t_par = Instant::now();
        let mut uniq_parents: Vec<u64> = parent_need.keys().copied().collect();
        uniq_parents.sort_unstable();
        st.parent_unique = st.parent_unique.saturating_add(uniq_parents.len() as u32);

        // (pid, need_vouts)
        let mut pin_jobs: Vec<(u64, Vec<u32>)> = Vec::with_capacity(uniq_parents.len());
        for pid in uniq_parents {
            let _ = parent_need.remove(&pid);
            let mut need_vouts = parent_vouts.remove(&pid).unwrap_or_default();
            need_vouts.sort_unstable();
            need_vouts.dedup();
            pin_jobs.push((pid, need_vouts));
        }
        batch_parents = BatchParents::with_capacity(pin_jobs.len());

        // ── Same-batch hits (pin_cache) ────────────────────────────────────
        // Coinbase height: free hints when known. Store resolve is deferred to
        // structural write (maturity). Unset (`None`) means write must look up.
        let t_body = Instant::now();
        let mut pin_new_pending: Vec<(u64, Vec<u32>)> = Vec::new();
        for (pid, need_vouts) in pin_jobs {
            if self.confirm_cancelled() {
                crate::confirm_load_stats::note(&st, t0.elapsed().as_nanos() as u64);
                return Err(StoreError::Cancelled("confirm cancelled"));
            }
            let fk = Fk(pid);
            if need_vouts.is_empty() {
                continue;
            }
            // Same-batch create: pin from batch_bodies (no idx / body re-read).
            if let Some(body) = batch_bodies.get(fk) {
                let sparse =
                    crate::batch_parents::sparse_spender_rels(&body.denserels, &need_vouts);
                if crate::batch_parents::layout_covers_need(
                    body.body_range,
                    &sparse,
                    &need_vouts,
                ) {
                    st.pin_cache_body = st.pin_cache_body.saturating_add(1);
                    st.parent_cache_hits = st.parent_cache_hits.saturating_add(1);
                    let live = slim_dense_outs_to_need(&body.outputs, &need_vouts);
                    let cb = Some(is_coinbase_inputs(&body.tx, &body.inputs));
                    batch_parents.insert_owned(
                        fk,
                        body.tx.clone(),
                        live,
                        need_vouts,
                        cb,
                        body.body_range,
                        sparse,
                    );
                    st.utxo_parents = st.utxo_parents.saturating_add(1);
                    continue;
                }
            }
            st.pin_new = st.pin_new.saturating_add(1);
            pin_new_pending.push((pid, need_vouts));
        }
        st.pin_body_ns = st
            .pin_body_ns
            .saturating_add(t_body.elapsed().as_nanos() as u64);

        // ── pin_new: idx→body pipeline in **chunks** ─────────────────────
        // Holding ~90k full packed bodies + dense outs at once blew RSS.
        // Chunk so peak is O(PIN_NEW_CHUNK) bodies. Cold denserels only.
        const PIN_NEW_CHUNK: usize = 4096;
        let t_new = Instant::now();
        if self.confirm_cancelled() {
            crate::confirm_load_stats::note(&st, t0.elapsed().as_nanos() as u64);
            return Err(StoreError::Cancelled("confirm cancelled"));
        }
        for chunk in pin_new_pending.chunks(PIN_NEW_CHUNK) {
            if self.confirm_cancelled() {
                crate::confirm_load_stats::note(&st, t0.elapsed().as_nanos() as u64);
                return Err(StoreError::Cancelled("confirm cancelled"));
            }
            let fks: Vec<Fk> = chunk.iter().map(|(pid, _)| Fk(*pid)).collect();
            let loaded = crate::combined_stage::load_creates_once(
                &self.store,
                &fks,
                rbitcoin_store::IdxBodyMode::OutsDenserels,
            )?;
            let mut by_id: HashMap<u64, crate::combined_stage::CombinedCreate> =
                HashMap::with_capacity(loaded.len());
            for c in loaded {
                by_id.insert(c.fk.get().unwrap_or(0), c);
            }
            for (pid, need_vouts) in chunk {
                if self.confirm_cancelled() {
                    crate::confirm_load_stats::note(&st, t0.elapsed().as_nanos() as u64);
                    return Err(StoreError::Cancelled("confirm cancelled"));
                }
                let fk = Fk(*pid);
                if need_vouts.is_empty() {
                    continue;
                }
                let Some(c) = by_id.remove(pid) else {
                    return Err(StoreError::Corrupt(
                        "invariant: pin_new parent missing body range",
                    )
                    .into());
                };
                let range = Some(c.body_range);
                let Ok((mut tx, outs, dense_rels)) =
                    rbitcoin_store::decode_packed_tx_outs_with_spender_rels_secret(
                        &c.raw,
                        Some(self.store.txs.store_secret()),
                    )
                else {
                    return Err(StoreError::Corrupt(
                        "invariant: pin_new failed to decode parent denserels",
                    )
                    .into());
                };
                if let Ok(tid) = self.store.txs.body_txid(fk) {
                    tx.txid = tid;
                }
                let live = slim_dense_outs_to_need(&outs, need_vouts);
                let sparse = crate::batch_parents::sparse_spender_rels(&dense_rels, need_vouts);
                if !crate::batch_parents::layout_covers_need(range, &sparse, need_vouts) {
                    return Err(StoreError::Corrupt(
                        "invariant: pin_new denserels incomplete for need_vouts",
                    )
                    .into());
                }
                let cb = if tx.input_count != 1 {
                    Some(false)
                } else {
                    None
                };
                batch_parents.insert_owned(
                    fk,
                    tx,
                    live,
                    need_vouts.clone(),
                    cb,
                    range,
                    sparse,
                );
                st.full_tx_reads = st.full_tx_reads.saturating_add(1);
                st.utxo_parents = st.utxo_parents.saturating_add(1);
            }
        }
        st.pin_new_meta_ns = st
            .pin_new_meta_ns
            .saturating_add(t_new.elapsed().as_nanos() as u64);

        // Parents already moved into BatchParents; thin stays batch-local.
        st.parent_pin_ns = st
            .parent_pin_ns
            .saturating_add(t_par.elapsed().as_nanos() as u64);

        batch_thin = thin_by_spend;

        let scanned: Vec<u32> = height_tx_fks.iter().map(|(h, _)| *h).collect();
        if !scanned.is_empty() {
            self.confirm_parents.mark_scanned_many(&scanned);
        }

        crate::confirm_load_stats::note(&st, t0.elapsed().as_nanos() as u64);
        Ok((st, batch_parents, batch_thin, batch_bodies))
    }

    /// No-op: sparse parents are batch-local and drop with the confirm batch.
    pub fn unpin_spent_parent_outs(
        &self,
        _spends: &[(Fk, u32)],
    ) -> Result<(), QueryError> {
        Ok(())
    }
}

/// True when `fks` is empty or non-decreasing by Class A id (archive order).
#[inline]
fn tx_fks_is_sorted_ascending(fks: &[Fk]) -> bool {
    fks.windows(2).all(|w| w[0].0 <= w[1].0)
}

/// Dense `outs[vout]` → sparse need list (clone of need only).
///
/// Residual batch-local copies of scripts are the need-vouts in [`BatchParents`].
fn slim_dense_outs_to_need(
    outs: &[rbitcoin_store::OutputRecord],
    need: &[u32],
) -> Vec<(u32, rbitcoin_store::OutputRecord)> {
    let mut live = Vec::with_capacity(need.len());
    for &v in need {
        if let Some(o) = outs.get(v as usize) {
            live.push((v, o.clone()));
        }
    }
    live
}

/// Derive coinbase flag from decoded inputs (called once at pin).
fn is_coinbase_inputs(tx: &rbitcoin_store::TxRecord, inputs: &[rbitcoin_store::InputRecord]) -> bool {
    if tx.input_count != 1 {
        return false;
    }
    inputs
        .first()
        .is_some_and(|i| i.is_coinbase() || i.prev_index == u32::MAX)
}

#[cfg(test)]
mod pin_new_slim_tests {
    use super::slim_dense_outs_to_need;
    use rbitcoin_store::OutputRecord;

    /// Production slim: index dense outs by need vouts; drop unneeded (incl. large).
    #[test]
    fn slim_dense_outs_to_need_maps_and_drops() {
        let dense = vec![
            OutputRecord::unspent(0, vec![0; 64]),
            OutputRecord::unspent(1, vec![1; 64]),
            OutputRecord::unspent(2, vec![2; 256]), // unneeded large
        ];
        let live = slim_dense_outs_to_need(&dense, &[0, 2]);
        assert_eq!(live.len(), 2);
        assert_eq!(live[0].0, 0);
        assert_eq!(live[0].1.value, 0);
        assert_eq!(live[0].1.script.len(), 64);
        assert_eq!(live[1].0, 2);
        assert_eq!(live[1].1.value, 2);
        assert_eq!(live[1].1.script.len(), 256);
        // Dense source unchanged (callers still own full decode for denserels).
        assert_eq!(dense.len(), 3);
        assert!(slim_dense_outs_to_need(&dense, &[]).is_empty());
    }
}

/// Drive shipped `load_confirm_parents` prep-miss invariants (body decode / pin_new).
#[cfg(test)]
mod load_confirm_invariant_tests {
    use crate::Query;
    use rbitcoin_primitives::Fk;
    use rbitcoin_store::{HeaderRecord, InputRecord, OutputRecord, TxRecord};
    use std::sync::Once;

    fn temp_q(label: &str) -> (std::path::PathBuf, Query) {
        static ONCE: Once = Once::new();
        ONCE.call_once(|| {
            if std::env::var_os("RBITCOIN_HEAD_SCALE").is_none() {
                std::env::set_var("RBITCOIN_HEAD_SCALE", "tiny");
            }
        });
        let dir = std::env::temp_dir().join(format!(
            "rbitcoin-load-inv-{label}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0)
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let q = Query::open_or_create(dir.join("store")).unwrap();
        q.enter_direct_index_mode().unwrap();
        (dir, q)
    }

    /// Body present but Full decode fails → hard invariant (no silent skip).
    #[test]
    fn confirm_load_body_decode_fail_is_invariant() {
        let (dir, q) = temp_q("decode-fail");
        // Minimal valid packed create so put succeeds.
        let tx = TxRecord {
            txid: [0x71; 32],
            version: 1,
            locktime: 0,
            input_start_fk: Fk::NULL,
            input_count: 1,
            output_start_fk: Fk::NULL,
            output_count: 1,
        };
        let ins = vec![InputRecord {
            prev_txid: [0u8; 32],
            create_fk: Fk::NULL,
            prev_index: u32::MAX,
            sequence: u32::MAX,
            script_sig: vec![0x01],
            witness: vec![],
        }];
        let outs = vec![OutputRecord::unspent(1, vec![0x51])];
        let fks = q
            .store()
            .put_tx_full_batch_indexed(&[(tx, ins, outs)], true)
            .unwrap();
        // Overwrite packed body with meta claiming input_count=1 but no input
        // bytes → Full decode fails short (safe, no OOM) while idx range stays.
        let (off, len) = q.store().tx_body_range(fks[0]).unwrap();
        // Schema 13 body meta is 32 B (no leading txid).
        assert!(
            len >= rbitcoin_store::TxRecord::BODY_META_LEN as u64,
            "need full body meta room"
        );
        let mut trash = vec![0u8; len as usize];
        // Body meta: version[4] locktime[4] in_start[8] in_count[4] …
        trash[16..20].copy_from_slice(&1u32.to_le_bytes()); // input_count = 1
        let body_path = q.store().path().join("tx.body");
        {
            use std::io::{Seek, SeekFrom, Write};
            let mut f = std::fs::OpenOptions::new()
                .write(true)
                .open(&body_path)
                .unwrap();
            f.seek(SeekFrom::Start(off)).unwrap();
            f.write_all(&trash).unwrap();
            f.sync_all().unwrap();
        }
        drop(q);
        let q = Query::open_or_create(dir.join("store")).unwrap();
        q.enter_direct_index_mode().unwrap();

        let mut hash = [0u8; 32];
        hash[0] = 0x71;
        let header = HeaderRecord {
            prev_fk: Fk::NULL,
            version: 1,
            timestamp: 1,
            bits: 0x207f_ffff,
            nonce: 1,
            merkle_root: hash,
            hash,
        };
        // Header may already exist from first open — put_header is fine / idempotent hash.
        let hfk = q.put_header(&header).unwrap();
        q.store()
            .header_txs
            .put_ranges_batch(&[(hfk, fks[0], 1)])
            .unwrap();

        let err = q
            .load_confirm_parents(&[(1, hash)])
            .expect_err("corrupt body must hard-fail load");
        let msg = format!("{err}");
        assert!(
            msg.contains("invariant") && msg.contains("decode"),
            "unexpected err: {msg}"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }
}

/// Drive shipped `load_confirm_parents` pin_new path with prep-miss facts.
#[cfg(test)]
mod pin_new_invariant_tests {
    use crate::Query;
    use rbitcoin_primitives::Fk;
    use rbitcoin_store::{HeaderRecord, InputRecord, OutputRecord, TxRecord};
    use std::sync::Once;

    fn temp_q(label: &str) -> (std::path::PathBuf, Query) {
        static ONCE: Once = Once::new();
        ONCE.call_once(|| {
            if std::env::var_os("RBITCOIN_HEAD_SCALE").is_none() {
                std::env::set_var("RBITCOIN_HEAD_SCALE", "tiny");
            }
        });
        let dir = std::env::temp_dir().join(format!(
            "rbitcoin-pin-new-{label}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0)
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let q = Query::open_or_create(dir.join("store")).unwrap();
        q.enter_direct_index_mode().unwrap();
        (dir, q)
    }

    /// Ghost create_fk on spend → pin_new cannot load parent body range.
    #[test]
    fn pin_new_missing_parent_body_is_invariant_error() {
        let (dir, q) = temp_q("missing-parent");
        let ghost_parent = Fk(999_999);
        let mut hash = [0u8; 32];
        hash[0] = 0x11;
        let header = HeaderRecord {
            prev_fk: Fk::NULL,
            version: 1,
            timestamp: 1,
            bits: 0x207f_ffff,
            nonce: 1,
            merkle_root: hash,
            hash,
        };
        let spend_tx = TxRecord {
            txid: [0x22; 32],
            version: 1,
            locktime: 0,
            input_start_fk: Fk::NULL,
            input_count: 1,
            output_start_fk: Fk::NULL,
            output_count: 1,
        };
        let spend_ins = vec![InputRecord {
            prev_txid: [0x33; 32],
            create_fk: ghost_parent,
            prev_index: 0,
            sequence: u32::MAX,
            script_sig: vec![],
            witness: vec![],
        }];
        let spend_outs = vec![OutputRecord::unspent(1, vec![0x51])];
        let fks = q
            .store()
            .put_tx_full_batch_indexed(&[(spend_tx, spend_ins, spend_outs)], true)
            .unwrap();
        let hfk = q.put_header(&header).unwrap();
        q.store()
            .header_txs
            .put_ranges_batch(&[(hfk, fks[0], 1)])
            .unwrap();

        let err = q
            .load_confirm_parents(&[(1, hash)])
            .expect_err("ghost parent must hard-fail pin_new");
        let msg = format!("{err}");
        assert!(
            msg.contains("invariant") && msg.contains("pin_new") && msg.contains("body range"),
            "unexpected err: {msg}"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Spend needs OOB vout → pin denserels/outs cannot cover need_vouts.
    #[test]
    fn pin_new_incomplete_need_vouts_is_invariant_error() {
        let (dir, q) = temp_q("oob-vout");
        // Parent create with a single out.
        let parent_tx = TxRecord {
            txid: [0x44; 32],
            version: 1,
            locktime: 0,
            input_start_fk: Fk::NULL,
            input_count: 1,
            output_start_fk: Fk::NULL,
            output_count: 1,
        };
        let parent_ins = vec![InputRecord {
            prev_txid: [0u8; 32],
            create_fk: Fk::NULL,
            prev_index: u32::MAX,
            sequence: u32::MAX,
            script_sig: vec![0x01],
            witness: vec![],
        }];
        let parent_outs = vec![OutputRecord::unspent(50, vec![0x51])];
        let parent_fks = q
            .store()
            .put_tx_full_batch_indexed(&[(parent_tx, parent_ins, parent_outs)], true)
            .unwrap();
        let parent_fk = parent_fks[0];

        // Spend block needs vout 7 (OOB for parent with 1 out).
        let mut hash = [0u8; 32];
        hash[0] = 0x55;
        let header = HeaderRecord {
            prev_fk: Fk::NULL,
            version: 1,
            timestamp: 2,
            bits: 0x207f_ffff,
            nonce: 2,
            merkle_root: hash,
            hash,
        };
        let spend_tx = TxRecord {
            txid: [0x66; 32],
            version: 1,
            locktime: 0,
            input_start_fk: Fk::NULL,
            input_count: 1,
            output_start_fk: Fk::NULL,
            output_count: 1,
        };
        let spend_ins = vec![InputRecord {
            prev_txid: [0x44; 32],
            create_fk: parent_fk,
            prev_index: 7, // OOB
            sequence: u32::MAX,
            script_sig: vec![],
            witness: vec![],
        }];
        let spend_outs = vec![OutputRecord::unspent(1, vec![0x51])];
        let spend_fks = q
            .store()
            .put_tx_full_batch_indexed(&[(spend_tx, spend_ins, spend_outs)], true)
            .unwrap();
        let hfk = q.put_header(&header).unwrap();
        q.store()
            .header_txs
            .put_ranges_batch(&[(hfk, spend_fks[0], 1)])
            .unwrap();

        let err = q
            .load_confirm_parents(&[(1, hash)])
            .expect_err("OOB need_vouts must hard-fail pin_new");
        let msg = format!("{err}");
        assert!(
            msg.contains("invariant")
                && msg.contains("pin_new")
                && (msg.contains("incomplete") || msg.contains("denserels") || msg.contains("body")),
            "unexpected err: {msg}"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }
}


