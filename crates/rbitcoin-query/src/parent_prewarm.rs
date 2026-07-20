//! Confirm-runway parent prewarm: load + pin external parents before Class C.
//!
//! IBD's confirm tip freezes when wave-fill cold-loads parents while the archive
//! writer saturates disk. A background worker walks **tip+1 … tip+PREWARM_DEPTH**
//! archived bodies, resolves create fks (light UTXO), loads parent Class A rows
//! into RAM, promotes live slots into tip_prevout, and **pins** each needed
//! outpoint until the spending input is Class-C confirmed.

use super::*;
use std::collections::{HashMap, HashSet};

/// How far ahead of the confirmed tip to prewarm (blocks).
pub const PREWARM_DEPTH: u32 = 1000;
/// Max blocks processed per prewarm tick (keeps the worker snappy).
pub const PREWARM_BATCH: u32 = 48;

/// Stats from one prewarm call (for logs / tests).
#[derive(Debug, Default, Clone, Copy)]
pub struct PrewarmStats {
    pub blocks: u32,
    pub bodies_loaded: u32,
    pub parents_loaded: u32,
    pub outs_pinned: u32,
    pub already_warm: u32,
}

impl Query {
    /// Prefetch Class A bodies + external parents for archived blocks on the
    /// confirm runway. Pins each needed parent outpoint.
    ///
    /// Safe to call repeatedly; skips work already in cache / pinned.
    pub fn prewarm_parents_for_block_hashes(
        &self,
        hashes: &[[u8; 32]],
    ) -> Result<PrewarmStats, QueryError> {
        let mut st = PrewarmStats::default();
        if hashes.is_empty() {
            return Ok(st);
        }

        // ── Pass 1: ensure wave bodies are in Class A ──────────────────────
        for hash in hashes {
            let Some((header_fk, _)) = self.get_header_by_hash(hash)? else {
                continue;
            };
            let Some(tx_fks) = self.store.header_txs.get_list(header_fk)? else {
                continue;
            };
            st.blocks = st.blocks.saturating_add(1);
            for fk in &tx_fks {
                if self.class_a_cache.has_reconstruct_ready(*fk) {
                    st.already_warm = st.already_warm.saturating_add(1);
                    continue;
                }
                let tx = self.store.get_tx(*fk)?;
                let inputs = if tx.input_count > 0 {
                    let run = tx.input_start_fk.get().ok_or(StoreError::InvalidFk)?;
                    Some(self.store.get_input_run(Fk(run), tx.input_count)?)
                } else {
                    None
                };
                let outputs = if tx.output_count > 0 {
                    let run = tx.output_start_fk.get().ok_or(StoreError::InvalidFk)?;
                    Some(self.store.get_output_run(Fk(run), tx.output_count)?)
                } else {
                    None
                };
                self.class_a_cache.note(*fk, tx, outputs, inputs);
                st.bodies_loaded = st.bodies_loaded.saturating_add(1);
            }
        }

        // ── Pass 2: collect external parent (fk → vouts) ───────────────────
        let mut parent_needed: HashMap<u64, HashSet<u32>> = HashMap::new();
        let mut wave_fks: HashSet<u64> = HashSet::new();
        let mut wave_tx_fks: Vec<Fk> = Vec::new();
        for hash in hashes {
            let Some((header_fk, _)) = self.get_header_by_hash(hash)? else {
                continue;
            };
            let Some(tx_fks) = self.store.header_txs.get_list(header_fk)? else {
                continue;
            };
            for fk in tx_fks {
                if let Some(id) = fk.get() {
                    wave_fks.insert(id);
                }
                wave_tx_fks.push(fk);
            }
        }

        // Same-wave creates by txid for in-run spends (avoid UTXO for those).
        let mut same_wave_txid: HashMap<[u8; 32], Fk> = HashMap::new();
        for &fk in &wave_tx_fks {
            if let Some((tx, _, _)) = self.class_a_cache.get_reconstruct_parts(fk) {
                same_wave_txid.insert(tx.txid, fk);
            } else if let Ok(tx) = self.store.get_tx(fk) {
                same_wave_txid.insert(tx.txid, fk);
            }
        }

        for &fk in &wave_tx_fks {
            let inputs = if let Some((_, _, ins)) = self.class_a_cache.get_reconstruct_parts(fk) {
                ins
            } else {
                let tx = self.store.get_tx(fk)?;
                if tx.input_count == 0 {
                    continue;
                }
                let run = tx.input_start_fk.get().ok_or(StoreError::InvalidFk)?;
                self.store.get_input_run(Fk(run), tx.input_count)?
            };
            for inp in &inputs {
                if inp.is_coinbase() {
                    continue;
                }
                if same_wave_txid.contains_key(&inp.prev_txid) {
                    continue; // same-run create — wave body already holds it
                }
                let create_fk = self
                    .ibd_utxo_create_fk(&inp.prev_txid, inp.prev_index)?
                    .or(self.tx_fk_by_txid(&inp.prev_txid)?);
                let Some(pfk) = create_fk else {
                    continue;
                };
                let Some(pid) = pfk.get() else {
                    continue;
                };
                if wave_fks.contains(&pid) {
                    continue;
                }
                // Skip already-spent (UTXO miss already handled; double-check).
                if self.catchup_is_spent(&inp.prev_txid, inp.prev_index)? {
                    continue;
                }
                parent_needed
                    .entry(pid)
                    .or_default()
                    .insert(inp.prev_index);
            }
        }

        // ── Pass 3: load parents (sorted by fk), pin outs, tip_prevout seed ─
        let mut parents: Vec<(u64, HashSet<u32>)> = parent_needed.into_iter().collect();
        parents.sort_unstable_by_key(|(pid, _)| *pid);

        for (pid, vouts) in parents {
            let fk = Fk(pid);
            let tx = if let Some(t) = self.class_a_cache.get_tx(fk) {
                t
            } else {
                let t = self.store.get_tx(fk)?;
                let _ = self
                    .class_a_cache
                    .note_no_evict(fk, t.clone(), None, None);
                st.parents_loaded = st.parents_loaded.saturating_add(1);
                t
            };

            let n = tx.output_count as usize;
            if n == 0 {
                continue;
            }
            let raw_outs = if let Some(o) = self.class_a_cache.get_outputs(fk) {
                o
            } else {
                let run = tx.output_start_fk.get().ok_or(StoreError::InvalidFk)?;
                let o = self.store.get_output_run(Fk(run), tx.output_count)?;
                self.class_a_cache.fill_outputs(fk, o.clone());
                // Ensure entry exists even if note_no_evict failed earlier.
                if self.class_a_cache.get_tx(fk).is_none() {
                    self.class_a_cache.note(fk, tx.clone(), Some(o.clone()), None);
                }
                st.parents_loaded = st.parents_loaded.saturating_add(1);
                o
            };

            let mut slots: Vec<Option<OutputRecord>> = vec![None; n];
            for &v in &vouts {
                let vi = v as usize;
                if vi >= n {
                    continue;
                }
                if self.catchup_is_spent(&tx.txid, v)? {
                    continue;
                }
                slots[vi] = Some(raw_outs[vi].clone());
                self.class_a_cache.pin_out(fk, v);
                st.outs_pinned = st.outs_pinned.saturating_add(1);
            }
            // Seed tip_prevout so the next wave short-circuits.
            if slots.iter().any(|s| s.is_some()) {
                self.tip_prevout_cache
                    .note_live_slots(fk, tx, &slots);
            }
        }

        Ok(st)
    }

    /// Distinct parent outpoints currently pinned for the confirm runway.
    pub fn parent_pin_count(&self) -> usize {
        self.class_a_cache.pinned_out_count()
    }

    /// Release Class A pins for spent outpoints (after successful Class C).
    ///
    /// Resolves create_fk via light UTXO (still present before spend apply) or
    /// tip_prevout / tx lookup.
    pub fn unpin_spent_parent_outs(
        &self,
        spends: &[([u8; 32], u32)],
    ) -> Result<(), QueryError> {
        for &(txid, vout) in spends {
            let create_fk = self
                .ibd_utxo_create_fk(&txid, vout)?
                .or_else(|| {
                    self.tip_prevout_cache
                        .get_tx_and_output_by_txid(&txid, vout)
                        .map(|(fk, _, _)| fk)
                })
                .or(self.tx_fk_by_txid(&txid).ok().flatten());
            let Some(fk) = create_fk else {
                continue;
            };
            self.class_a_cache.unpin_out(fk, vout);
        }
        Ok(())
    }
}
