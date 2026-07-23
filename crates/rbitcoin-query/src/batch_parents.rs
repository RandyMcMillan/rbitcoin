//! Per-confirm-batch spent-filtered parent outs.
//!
//! Lifetime: built during load pin, carried on [`crate`] load/script/write batch
//! objects, dropped when the batch finishes write. Not tip-GCed and not shared
//! across concurrent in-flight batches.

use rbitcoin_primitives::Fk;
use rbitcoin_store::{OutputRecord, TxRecord};
use std::collections::{HashMap, HashSet};

/// One needed prevout under a parent create.
#[derive(Debug, Clone)]
pub struct ParentOut {
    pub output: OutputRecord,
}

/// Sparse parent create row for one confirm batch.
#[derive(Debug, Clone)]
pub struct ParentEntry {
    pub tx: TxRecord,
    /// Live (unspent) needed vouts → output. Spent vouts are omitted.
    pub outs: HashMap<u32, ParentOut>,
    /// Vouts fully spent-filtered for this batch (wave skips durable re-check).
    pub checked: HashSet<u32>,
    /// Coinbase maturity: `None` unset; `Some(None)` not cb; `Some(Some(h))` height.
    pub coinbase_height: Option<Option<u32>>,
    /// Create height when known (body height).
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

    pub fn len(&self) -> usize {
        self.by_fk.len()
    }

    pub fn is_empty(&self) -> bool {
        self.by_fk.is_empty()
    }

    /// Insert/merge spent-filtered outs for a parent create.
    pub fn put_resolved(
        &mut self,
        fk: Fk,
        tx: TxRecord,
        live: &[(u32, OutputRecord)],
        checked: &[u32],
        coinbase_height: Option<Option<u32>>,
        create_height: Option<u32>,
    ) {
        let Some(id) = fk.get() else {
            return;
        };
        let e = self.by_fk.entry(id).or_insert_with(|| ParentEntry {
            tx: tx.clone(),
            outs: HashMap::new(),
            checked: HashSet::new(),
            coinbase_height: None,
            create_height: None,
        });
        e.tx = tx;
        if coinbase_height.is_some() {
            e.coinbase_height = coinbase_height;
        }
        if create_height.is_some() {
            e.create_height = create_height;
        }
        for &v in checked {
            e.checked.insert(v);
        }
        for (v, output) in live {
            e.outs.insert(
                *v,
                ParentOut {
                    output: output.clone(),
                },
            );
            e.checked.insert(*v);
        }
    }

    /// Batch insert (load pin finish).
    pub fn put_resolved_batch(
        &mut self,
        items: &[(
            u32, // max need height (unused; batch-scoped)
            Fk,
            TxRecord,
            Vec<(u32, OutputRecord)>,
            Vec<u32>,
            Option<Option<u32>>,
            Option<u32>,
        )],
    ) {
        for (_h, fk, tx, live, checked, cb, create_h) in items {
            self.put_resolved(*fk, tx.clone(), live, checked, *cb, *create_h);
        }
    }

    pub fn get_parent_out(&self, fk: Fk, vout: u32) -> Option<(TxRecord, OutputRecord)> {
        let id = fk.get()?;
        let e = self.by_fk.get(&id)?;
        let o = e.outs.get(&vout)?;
        Some((e.tx.clone(), o.output.clone()))
    }

    pub fn get_parent_tx(&self, fk: Fk) -> Option<TxRecord> {
        let id = fk.get()?;
        self.by_fk.get(&id).map(|e| e.tx.clone())
    }

    pub fn get_parent_coinbase_height(&self, fk: Fk) -> Option<Option<u32>> {
        let id = fk.get()?;
        self.by_fk.get(&id)?.coinbase_height
    }

    pub fn has_parent_out(&self, fk: Fk, vout: u32) -> bool {
        let Some(id) = fk.get() else {
            return false;
        };
        self.by_fk
            .get(&id)
            .is_some_and(|e| e.outs.contains_key(&vout))
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
        !e.checked.is_empty() && vouts.iter().all(|v| e.checked.contains(v))
    }

    /// Sparse outs for wave: `(tx, live outs for vouts, spent_filtered)`.
    pub fn get_parent_outs_needed(
        &self,
        fk: Fk,
        vouts: &[u32],
    ) -> Option<(TxRecord, Vec<(u32, OutputRecord)>, bool)> {
        let id = fk.get()?;
        let e = self.by_fk.get(&id)?;
        if !e.checked.is_empty() && vouts.iter().all(|v| e.checked.contains(v)) {
            let mut live = Vec::with_capacity(vouts.len());
            for &v in vouts {
                if let Some(o) = e.outs.get(&v) {
                    live.push((v, o.output.clone()));
                }
            }
            return Some((e.tx.clone(), live, true));
        }
        if !e.outs.is_empty() && vouts.iter().all(|v| e.outs.contains_key(v)) {
            let mut live = Vec::with_capacity(vouts.len());
            for &v in vouts {
                if let Some(o) = e.outs.get(&v) {
                    live.push((v, o.output.clone()));
                }
            }
            return Some((e.tx.clone(), live, false));
        }
        None
    }
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
            Some(5),
        );
        assert!(bp.pin_covered(Fk(7), &[0, 1]));
        let (t, o) = bp.get_parent_out(Fk(7), 0).unwrap();
        assert_eq!(t.txid[0], 7);
        assert_eq!(o.value, 10);
        assert_eq!(bp.get_parent_coinbase_height(Fk(7)), Some(None));
        assert_eq!(bp.len(), 1);
    }
}
