//! Per-confirm-batch spent-filtered parent outs.
//!
//! Lifetime: built during load pin, carried on load/script/write batch objects,
//! dropped when the batch finishes write. Not tip-GCed and not shared across
//! concurrent in-flight batches.
//!
//! **Hot path:** parents are inserted once per batch with ownership moves
//! ([`BatchParents::insert_owned`]). Storage uses small `Vec`s (typical 1–3
//! vouts) — not `HashMap`/`HashSet` per parent — to avoid thrashing the
//! allocator when pin volume is tens of thousands of creates per load window.
//!
//! **Spentness / annotate layout:** optional packed `body_range` + per-need-vout
//! relative offsets of the 9-byte durable spender meta so write structural can
//! bulk-pread and spend annotate can skip per-create idx.
//!
//! Create **heights** are not stashed here — write re-reads Class C `tx_height`
//! (authority). Pin may only stash coinbase **flag** (multi-in ⇒ not coinbase).

use rbitcoin_primitives::Fk;
use rbitcoin_store::{OutputRecord, TxRecord};
use std::collections::HashMap;

/// Sparse parent create row for one confirm batch.
#[derive(Debug, Clone)]
pub struct ParentEntry {
    pub tx: TxRecord,
    /// Live (unspent) needed outs. Spent vouts omitted. Content-only outs.
    pub outs: Vec<(u32, OutputRecord)>,
    /// Vouts fully spent-filtered for this batch (wave skips durable re-check).
    /// Sorted unique (from pin need_vouts).
    pub checked: Vec<u32>,
    /// Coinbase flag: `None` unknown; `Some(false)` not cb; `Some(true)` is cb.
    /// Height always comes from durable `tx_height` on write, not from pin.
    pub coinbase: Option<bool>,
    /// Packed Class A `(body_off, body_len)` when known (pin_new / FIFO).
    pub body_range: Option<(u64, u64)>,
    /// Sorted unique `(vout, rel)` for need vouts when layout known.
    /// `abs = body_off + rel` is the 9-byte spender meta.
    pub spender_rels: Vec<(u32, u32)>,
}

/// Spent-filtered parents for **one** confirm batch (load → write).
#[derive(Debug, Default, Clone)]
pub struct BatchParents {
    by_fk: HashMap<u64, ParentEntry>,
}

impl BatchParents {
    pub fn new() -> Self {
        Self {
            by_fk: HashMap::new(),
        }
    }

    pub fn with_capacity(n: usize) -> Self {
        Self {
            by_fk: HashMap::with_capacity(n),
        }
    }

    pub fn len(&self) -> usize {
        self.by_fk.len()
    }

    pub fn is_empty(&self) -> bool {
        self.by_fk.is_empty()
    }

    /// Insert one parent with **ownership** (load pin hot path).
    #[inline]
    pub fn insert_owned(
        &mut self,
        fk: Fk,
        tx: TxRecord,
        live: Vec<(u32, OutputRecord)>,
        checked: Vec<u32>,
        coinbase: Option<bool>,
        body_range: Option<(u64, u64)>,
        spender_rels: Vec<(u32, u32)>,
    ) {
        let Some(id) = fk.get() else {
            return;
        };
        self.by_fk.insert(
            id,
            ParentEntry {
                tx,
                outs: live,
                checked,
                coinbase,
                body_range,
                spender_rels,
            },
        );
    }

    /// Test / convenience: clone from slices into the map.
    pub fn put_resolved(
        &mut self,
        fk: Fk,
        tx: TxRecord,
        live: &[(u32, OutputRecord)],
        checked: &[u32],
        coinbase: Option<bool>,
    ) {
        self.insert_owned(
            fk,
            tx,
            live.to_vec(),
            checked.to_vec(),
            coinbase,
            None,
            Vec::new(),
        );
    }

    pub fn get_parent_out(&self, fk: Fk, vout: u32) -> Option<(TxRecord, OutputRecord)> {
        let id = fk.get()?;
        let e = self.by_fk.get(&id)?;
        let o = e.outs.iter().find(|(v, _)| *v == vout)?;
        Some((e.tx.clone(), o.1.clone()))
    }

    /// Optimistic assemble hot path: value + script + parent txid **without**
    /// cloning the full [`TxRecord`] (only the spent out's script bytes).
    #[inline]
    pub fn get_parent_txout_parts(&self, fk: Fk, vout: u32) -> Option<(i64, &[u8], [u8; 32])> {
        let id = fk.get()?;
        let e = self.by_fk.get(&id)?;
        let (_, o) = e.outs.iter().find(|(v, _)| *v == vout)?;
        Some((o.value, o.script.as_slice(), e.tx.txid))
    }

    pub fn get_parent_tx(&self, fk: Fk) -> Option<TxRecord> {
        let id = fk.get()?;
        self.by_fk.get(&id).map(|e| e.tx.clone())
    }

    /// Coinbase flag from pin when known (`None` = write must resolve).
    pub fn get_parent_coinbase(&self, fk: Fk) -> Option<bool> {
        let id = fk.get()?;
        self.by_fk.get(&id)?.coinbase
    }

    /// Packed body `(off, len)` when known.
    pub fn get_body_range(&self, fk: Fk) -> Option<(u64, u64)> {
        let id = fk.get()?;
        self.by_fk.get(&id)?.body_range
    }

    /// Set body range + dense spender rels after Class A commit (same-batch creates).
    ///
    /// Does not re-fetch outs; only fills layout for annotate/structural abs paths.
    /// Merges `extra_need` into `checked` so spend-annotate abs covers those vouts.
    pub fn set_layout(&mut self, fk: Fk, body_range: (u64, u64), dense_rels: &[u32]) {
        self.set_layout_for_need(fk, body_range, dense_rels, &[]);
    }

    /// Like [`set_layout`] but also ensure abs for `extra_need` vouts.
    pub fn set_layout_for_need(
        &mut self,
        fk: Fk,
        body_range: (u64, u64),
        dense_rels: &[u32],
        extra_need: &[u32],
    ) {
        let Some(id) = fk.get() else {
            return;
        };
        let Some(e) = self.by_fk.get_mut(&id) else {
            return;
        };
        e.body_range = Some(body_range);
        let mut need: Vec<u32> = if e.checked.is_empty() && extra_need.is_empty() {
            (0..dense_rels.len() as u32).collect()
        } else {
            e.checked.clone()
        };
        need.extend_from_slice(extra_need);
        need.sort_unstable();
        need.dedup();
        e.checked = need.clone();
        e.spender_rels = sparse_spender_rels(dense_rels, &need);
    }

    /// Attach body_range only (pin already has sparse denserels from prep).
    ///
    /// Avoids re-encoding denserels on write for prep-ahead parents once Class A
    /// commit publishes ranges.
    pub fn set_body_range_only(&mut self, fk: Fk, body_range: (u64, u64)) {
        let Some(id) = fk.get() else {
            return;
        };
        if let Some(e) = self.by_fk.get_mut(&id) {
            e.body_range = Some(body_range);
        }
    }

    /// Set layout from precomputed sparse denserels (one residency probe, no dense re-walk).
    pub fn set_layout_sparse(
        &mut self,
        fk: Fk,
        body_range: (u64, u64),
        sparse_rels: Vec<(u32, u32)>,
        extra_need: &[u32],
    ) {
        let Some(id) = fk.get() else {
            return;
        };
        let Some(e) = self.by_fk.get_mut(&id) else {
            return;
        };
        e.body_range = Some(body_range);
        if !extra_need.is_empty() {
            let mut need = e.checked.clone();
            need.extend_from_slice(extra_need);
            need.sort_unstable();
            need.dedup();
            e.checked = need;
        }
        if e.spender_rels.is_empty() {
            e.spender_rels = sparse_rels;
        } else if !sparse_rels.is_empty() {
            // Merge by vout (prefer new rel when present).
            let mut m: HashMap<u32, u32> = e.spender_rels.iter().copied().collect();
            for (v, r) in sparse_rels {
                m.insert(v, r);
            }
            let mut merged: Vec<(u32, u32)> = m.into_iter().collect();
            merged.sort_unstable_by_key(|(v, _)| *v);
            e.spender_rels = merged;
        }
    }

    /// True when body_range and at least one spender_rel are present.
    #[inline]
    pub fn has_abs_layout(&self, fk: Fk) -> bool {
        let Some(id) = fk.get() else {
            return false;
        };
        self.by_fk
            .get(&id)
            .is_some_and(|e| e.body_range.is_some() && !e.spender_rels.is_empty())
    }

    /// True when sparse denserels (spender_rels) are present — body_range may still be missing.
    ///
    /// Write ensure uses this to fetch **idx range only** instead of reloading Class A denserels.
    #[inline]
    pub fn has_spender_rels(&self, fk: Fk) -> bool {
        let Some(id) = fk.get() else {
            return false;
        };
        self.by_fk
            .get(&id)
            .is_some_and(|e| !e.spender_rels.is_empty())
    }

    /// Create fks pinned without body_range / spender rels (prep-ahead in-flight parents).
    ///
    /// Write fills these after prior pipeline batches have committed Class A so
    /// structural spentness and spend annotate can bulk-pread denserels abs.
    pub fn fks_missing_layout(&self) -> Vec<Fk> {
        self.by_fk
            .iter()
            .filter(|(_, e)| e.body_range.is_none() || e.spender_rels.is_empty())
            .map(|(&id, _)| Fk(id))
            .collect()
    }

    /// True when this create is present in the pin map (any layout state).
    #[inline]
    pub fn contains(&self, fk: Fk) -> bool {
        fk.get().is_some_and(|id| self.by_fk.contains_key(&id))
    }

    /// Absolute 9-byte spender meta offset for `vout`, if layout known.
    ///
    /// Returns `None` when body range or denserels were not prepared (caller
    /// should treat that as a prep invariant break on Direct confirm write).
    pub fn get_spender_abs(&self, fk: Fk, vout: u32) -> Option<u64> {
        let id = fk.get()?;
        let e = self.by_fk.get(&id)?;
        let (off, _) = e.body_range?;
        let i = e.spender_rels.binary_search_by_key(&vout, |(v, _)| *v).ok()?;
        let rel = e.spender_rels[i].1;
        if rel == SPENDER_REL_UNKNOWN {
            return None;
        }
        Some(off.saturating_add(u64::from(rel)))
    }

    pub fn has_parent_out(&self, fk: Fk, vout: u32) -> bool {
        let Some(id) = fk.get() else {
            return false;
        };
        self.by_fk
            .get(&id)
            .is_some_and(|e| e.outs.iter().any(|(v, _)| *v == vout))
    }

    /// True when all `vouts` are spent-filtered (`checked`) for this batch.
    pub fn pin_covered(&self, fk: Fk, vouts: &[u32]) -> bool {
        if vouts.is_empty() {
            return true;
        }
        let Some(id) = fk.get() else {
            return false;
        };
        let Some(e) = self.by_fk.get(&id) else {
            return false;
        };
        if e.checked.is_empty() {
            return false;
        }
        vouts.iter().all(|v| checked_contains(&e.checked, *v))
    }

    /// Absorb another batch's pin map (write megabatch drain).
    ///
    /// Same create_fk: merge outs / checked / denserels; prefer non-`None`
    /// body_range and coinbase flag. Typical case is disjoint parents across
    /// consecutive script-ok batches; overlap is same parent spent in two
    /// heights in the mega-run.
    pub fn extend_from(&mut self, other: Self) {
        if other.by_fk.is_empty() {
            return;
        }
        if self.by_fk.is_empty() {
            *self = other;
            return;
        }
        self.by_fk.reserve(other.by_fk.len());
        for (id, src) in other.by_fk {
            match self.by_fk.entry(id) {
                std::collections::hash_map::Entry::Vacant(v) => {
                    v.insert(src);
                }
                std::collections::hash_map::Entry::Occupied(mut o) => {
                    merge_parent_entry(o.get_mut(), src);
                }
            }
        }
    }

    /// Sparse outs for wave: `(tx, live outs for vouts, spent_filtered)`.
    pub fn get_parent_outs_needed(
        &self,
        fk: Fk,
        vouts: &[u32],
    ) -> Option<(TxRecord, Vec<(u32, OutputRecord)>, bool)> {
        let id = fk.get()?;
        let e = self.by_fk.get(&id)?;
        let covered = !e.checked.is_empty() && vouts.iter().all(|v| checked_contains(&e.checked, *v));
        if covered {
            let mut live = Vec::with_capacity(vouts.len());
            for &v in vouts {
                if let Some((_, o)) = e.outs.iter().find(|(ov, _)| *ov == v) {
                    live.push((v, o.clone()));
                }
            }
            return Some((e.tx.clone(), live, true));
        }
        if !e.outs.is_empty() && vouts.iter().all(|v| e.outs.iter().any(|(ov, _)| ov == v)) {
            let mut live = Vec::with_capacity(vouts.len());
            for &v in vouts {
                if let Some((_, o)) = e.outs.iter().find(|(ov, _)| *ov == v) {
                    live.push((v, o.clone()));
                }
            }
            return Some((e.tx.clone(), live, false));
        }
        None
    }
}

/// `checked` is sorted unique from pin dedup — binary search.
#[inline]
fn checked_contains(checked: &[u32], v: u32) -> bool {
    checked.binary_search(&v).is_ok()
}

/// Merge `src` into `dst` for the same create_fk (megabatch pin union).
fn merge_parent_entry(dst: &mut ParentEntry, src: ParentEntry) {
    for (v, o) in src.outs {
        if !dst.outs.iter().any(|(dv, _)| *dv == v) {
            dst.outs.push((v, o));
        }
    }
    dst.outs.sort_unstable_by_key(|(v, _)| *v);
    dst.checked.extend(src.checked);
    dst.checked.sort_unstable();
    dst.checked.dedup();
    if dst.coinbase.is_none() {
        dst.coinbase = src.coinbase;
    }
    if dst.body_range.is_none() {
        dst.body_range = src.body_range;
    }
    if src.spender_rels.is_empty() {
        return;
    }
    if dst.spender_rels.is_empty() {
        dst.spender_rels = src.spender_rels;
        return;
    }
    let mut m: HashMap<u32, u32> = dst.spender_rels.iter().copied().collect();
    for (v, r) in src.spender_rels {
        m.insert(v, r);
    }
    let mut merged: Vec<(u32, u32)> = m.into_iter().collect();
    merged.sort_unstable_by_key(|(v, _)| *v);
    dst.spender_rels = merged;
}

/// Relative offset sentinel: layout unknown for this out (FIFO seed without denserels).
pub const SPENDER_REL_UNKNOWN: u32 = u32::MAX;

/// Build sorted `(vout, rel)` for requested vouts from dense pin rels.
///
/// Skips missing denserels slots and [`SPENDER_REL_UNKNOWN`] so callers can
/// treat `out.len() == need_vouts.len()` as “layout complete for need_vouts”.
pub fn sparse_spender_rels(dense: &[u32], need_vouts: &[u32]) -> Vec<(u32, u32)> {
    let mut out = Vec::with_capacity(need_vouts.len());
    for &v in need_vouts {
        if let Some(&rel) = dense.get(v as usize) {
            if rel != SPENDER_REL_UNKNOWN {
                out.push((v, rel));
            }
        }
    }
    out
}

/// True when pin layout can supply abs spender meta for every `need_vout`.
pub fn layout_covers_need(
    body_range: Option<(u64, u64)>,
    sparse_rels: &[(u32, u32)],
    need_vouts: &[u32],
) -> bool {
    if body_range.is_none() || need_vouts.is_empty() {
        return body_range.is_some() && need_vouts.is_empty();
    }
    // sparse is sorted by vout from denserels walk order (need_vouts order).
    if sparse_rels.len() != need_vouts.len() {
        return false;
    }
    for (i, &v) in need_vouts.iter().enumerate() {
        if sparse_rels[i].0 != v || sparse_rels[i].1 == SPENDER_REL_UNKNOWN {
            return false;
        }
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use rbitcoin_primitives::Fk;
    use rbitcoin_store::{OutputRecord, TxRecord};

    fn tx(id: u8) -> TxRecord {
        let mut txid = [0u8; 32];
        txid[0] = id;
        TxRecord {
            txid,
            version: 1,
            locktime: 0,
            input_start_fk: Fk::NULL,
            input_count: 1,
            output_start_fk: Fk::NULL,
            output_count: 1,
        }
    }

    fn out(v: i64) -> OutputRecord {
        OutputRecord::unspent(v, vec![0x51])
    }

    #[test]
    fn extend_from_merges_disjoint_and_same_fk() {
        let mut a = BatchParents::new();
        a.insert_owned(
            Fk(1),
            tx(1),
            vec![(0, out(1))],
            vec![0],
            Some(false),
            Some((100, 50)),
            vec![(0, 10)],
        );
        let mut b = BatchParents::new();
        b.insert_owned(
            Fk(2),
            tx(2),
            vec![(0, out(2))],
            vec![0],
            Some(true),
            None,
            Vec::new(),
        );
        // Same fk extra vout + denserels.
        b.insert_owned(
            Fk(1),
            tx(1),
            vec![(1, out(3))],
            vec![1],
            None,
            None,
            vec![(1, 20)],
        );
        a.extend_from(b);
        assert_eq!(a.len(), 2);
        assert!(a.has_parent_out(Fk(1), 0));
        assert!(a.has_parent_out(Fk(1), 1));
        assert!(a.has_parent_out(Fk(2), 0));
        assert_eq!(a.get_spender_abs(Fk(1), 0), Some(110));
        assert_eq!(a.get_spender_abs(Fk(1), 1), Some(120));
        assert_eq!(a.get_parent_coinbase(Fk(1)), Some(false));
        assert_eq!(a.get_parent_coinbase(Fk(2)), Some(true));
    }

    /// Layout + coinbase flag + pin_covered — one test for BatchParents public surface.
    /// External pin/confirm behavior: rbitcoin-test three_stage_confirm_and_parent_pin_surface.
    #[test]
    fn insert_layout_coinbase_and_covered() {
        let mut bp = BatchParents::with_capacity(1);
        let live = vec![(0, out(42)), (2, out(99))];
        bp.insert_owned(
            Fk(9),
            tx(9),
            live,
            vec![0, 1, 2],
            Some(true),
            Some((1000, 200)),
            vec![(0, 50), (1, 70), (2, 90)],
        );
        assert_eq!(bp.len(), 1);
        assert!(bp.pin_covered(Fk(9), &[0, 1, 2]));
        assert!(!bp.pin_covered(Fk(9), &[0, 3]));
        assert!(!bp.has_parent_out(Fk(9), 1));
        assert_eq!(bp.get_spender_abs(Fk(9), 2), Some(1090));
        assert_eq!(bp.get_body_range(Fk(9)), Some((1000, 200)));
        assert_eq!(bp.get_parent_coinbase(Fk(9)), Some(true));
        assert!(bp.has_abs_layout(Fk(9)));
        let (_, o) = bp.get_parent_out(Fk(9), 0).unwrap();
        assert_eq!(o.value, 42);
        // A3: txout parts without TxRecord clone.
        let (v, script, parent_txid) = bp.get_parent_txout_parts(Fk(9), 0).unwrap();
        assert_eq!(v, 42);
        assert_eq!(script, &[0x51]);
        assert_eq!(parent_txid[0], 9);
        assert!(bp.get_parent_txout_parts(Fk(9), 1).is_none());
    }

    #[test]
    fn set_body_range_only_completes_layout_when_rels_present() {
        let mut bp = BatchParents::with_capacity(1);
        bp.insert_owned(
            Fk(3),
            tx(3),
            vec![(0, out(1))],
            vec![0],
            None,
            None,
            vec![(0, 40)],
        );
        assert!(!bp.has_abs_layout(Fk(3)));
        assert!(bp.has_spender_rels(Fk(3)));
        bp.set_body_range_only(Fk(3), (500, 80));
        assert!(bp.has_abs_layout(Fk(3)));
        assert_eq!(bp.get_spender_abs(Fk(3), 0), Some(540));
    }

    #[test]
    fn sparse_spender_rels_skips_unknown() {
        let dense = vec![10, SPENDER_REL_UNKNOWN, 30];
        let sparse = sparse_spender_rels(&dense, &[0, 1, 2]);
        assert_eq!(sparse, vec![(0, 10), (2, 30)]);
        assert!(!layout_covers_need(Some((0, 100)), &sparse, &[0, 1, 2]));
        assert!(layout_covers_need(
            Some((0, 100)),
            &[(0, 10), (2, 30)],
            &[0, 2]
        ));
        assert!(!layout_covers_need(None, &[(0, 10)], &[0]));
    }

    #[test]
    fn get_spender_abs_rejects_unknown_rel() {
        let mut bp = BatchParents::with_capacity(1);
        bp.insert_owned(
            Fk(1),
            tx(1),
            vec![(0, out(1))],
            vec![0],
            None,
            Some((100, 50)),
            vec![(0, SPENDER_REL_UNKNOWN)],
        );
        assert!(bp.get_spender_abs(Fk(1), 0).is_none());
    }
}
