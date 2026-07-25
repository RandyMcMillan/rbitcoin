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
//! **Spentness layout:** optional `body_off` + per-need-vout relative offsets of
//! the 9-byte durable spender meta so write structural can bulk-pread without
//! idx or full packed walks.

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
    /// Coinbase maturity: `None` unset; `Some(None)` not cb; `Some(Some(h))` height.
    pub coinbase_height: Option<Option<u32>>,
    /// Absolute packed body start in `tx.body` when known.
    pub body_off: Option<u64>,
    /// Sorted unique `(vout, rel)` for need vouts when layout known.
    /// `abs = body_off + rel` is the 9-byte spender meta.
    pub spender_rels: Vec<(u32, u32)>,
    /// Create block height when known from pin (FIFO `CreateOuts.height`).
    pub create_height: Option<u32>,
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
        coinbase_height: Option<Option<u32>>,
        body_off: Option<u64>,
        spender_rels: Vec<(u32, u32)>,
        create_height: Option<u32>,
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
                coinbase_height,
                body_off,
                spender_rels,
                create_height,
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
        coinbase_height: Option<Option<u32>>,
    ) {
        self.insert_owned(
            fk,
            tx,
            live.to_vec(),
            checked.to_vec(),
            coinbase_height,
            None,
            Vec::new(),
            None,
        );
    }

    pub fn get_parent_out(&self, fk: Fk, vout: u32) -> Option<(TxRecord, OutputRecord)> {
        let id = fk.get()?;
        let e = self.by_fk.get(&id)?;
        let o = e.outs.iter().find(|(v, _)| *v == vout)?;
        Some((e.tx.clone(), o.1.clone()))
    }

    pub fn get_parent_tx(&self, fk: Fk) -> Option<TxRecord> {
        let id = fk.get()?;
        self.by_fk.get(&id).map(|e| e.tx.clone())
    }

    pub fn get_parent_coinbase_height(&self, fk: Fk) -> Option<Option<u32>> {
        let id = fk.get()?;
        self.by_fk.get(&id)?.coinbase_height
    }

    /// Absolute body start when known.
    pub fn get_body_off(&self, fk: Fk) -> Option<u64> {
        let id = fk.get()?;
        self.by_fk.get(&id)?.body_off
    }

    /// Absolute 9-byte spender meta offset for `vout`, if layout known.
    pub fn get_spender_abs(&self, fk: Fk, vout: u32) -> Option<u64> {
        let id = fk.get()?;
        let e = self.by_fk.get(&id)?;
        let off = e.body_off?;
        let i = e.spender_rels.binary_search_by_key(&vout, |(v, _)| *v).ok()?;
        Some(off.saturating_add(u64::from(e.spender_rels[i].1)))
    }

    /// Create height stashed at pin (FIFO height), if known.
    pub fn get_create_height(&self, fk: Fk) -> Option<u32> {
        let id = fk.get()?;
        self.by_fk.get(&id)?.create_height
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

/// Build sorted `(vout, rel)` for requested vouts from dense pin rels.
pub fn sparse_spender_rels(dense: &[u32], need_vouts: &[u32]) -> Vec<(u32, u32)> {
    let mut out = Vec::with_capacity(need_vouts.len());
    for &v in need_vouts {
        if let Some(&rel) = dense.get(v as usize) {
            out.push((v, rel));
        }
    }
    out
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
    fn put_and_get_roundtrip() {
        let mut bp = BatchParents::new();
        bp.put_resolved(
            Fk(7),
            tx(7),
            &[(0, out(10)), (1, out(20))],
            &[0, 1],
            Some(None),
        );
        assert!(bp.pin_covered(Fk(7), &[0, 1]));
        let (t, o) = bp.get_parent_out(Fk(7), 0).unwrap();
        assert_eq!(t.txid[0], 7);
        assert_eq!(o.value, 10);
        assert_eq!(bp.get_parent_coinbase_height(Fk(7)), Some(None));
        assert_eq!(bp.len(), 1);
        assert!(bp.get_body_off(Fk(7)).is_none());
        assert!(bp.get_create_height(Fk(7)).is_none());
    }

    #[test]
    fn insert_owned_spender_abs() {
        let mut bp = BatchParents::with_capacity(1);
        let live = vec![(0, out(42)), (2, out(99))];
        bp.insert_owned(
            Fk(9),
            tx(9),
            live,
            vec![0, 1, 2],
            Some(Some(3)),
            Some(1000),
            vec![(0, 50), (1, 70), (2, 90)],
            Some(3),
        );
        assert!(bp.pin_covered(Fk(9), &[0, 1, 2]));
        assert!(!bp.has_parent_out(Fk(9), 1)); // spent-filtered, not live
        assert_eq!(bp.get_spender_abs(Fk(9), 2), Some(1090));
        assert_eq!(bp.get_create_height(Fk(9)), Some(3));
    }

    #[test]
    fn pin_covered_requires_checked_membership() {
        let mut bp = BatchParents::new();
        bp.insert_owned(
            Fk(1),
            tx(1),
            vec![(0, out(1))],
            vec![0, 2],
            None,
            None,
            Vec::new(),
            None,
        );
        assert!(bp.pin_covered(Fk(1), &[0, 2]));
        assert!(!bp.pin_covered(Fk(1), &[0, 1]));
    }
}
