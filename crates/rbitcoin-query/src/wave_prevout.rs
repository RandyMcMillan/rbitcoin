//! Wave-scoped tables for one confirm run (lock-free on connect).
//!
//! Holds:
//! - **Parent creates** with live/spent output slots (write-through unspent)
//! - **Spending txs** in the wave (`TxRecord` + thin create-fk hints per input)
//! - **Full Class A decode** for wave-body txs (shared with wire rebuild — one decode)
//! - **Coinbase create height** for parents (maturity without `tx_height` re-read)
//!
//! Wave-body outputs are [`Arc`]-shared between parent lookup and wire rebuild so
//! fill does not clone every script twice. External parents keep only the
//! needed live vouts in a sparse map (no `n_out`-sized `Option` vec).

use rbitcoin_primitives::Fk;
use rbitcoin_store::{InputRecord, OutputRecord, TxRecord};
use std::collections::HashMap;
use std::sync::Arc;

/// Live outputs for one parent create.
enum ParentOuts {
    /// All slots live (wave-body creates). Shared with [`BodyWire`] via Arc.
    AllLive(Arc<Vec<OutputRecord>>),
    /// External parents: only needed unspent vouts.
    Sparse(HashMap<u32, OutputRecord>),
}

struct Parent {
    fk: Fk,
    tx: TxRecord,
    outputs: ParentOuts,
    /// `None` = not computed; `Some(None)` = not coinbase; `Some(Some(h))` = cb height.
    coinbase_height: Option<Option<u32>>,
}

/// Thin input edge: create-tx Class A fk when known at wave fill (UTXO / same-wave).
/// Coinbase / unknown → `create_fk = None`. Not stored on Class A disk.
///
/// Same layout as runway `StashedThinInput` (type alias there).
#[derive(Clone, Copy, Debug)]
pub struct ThinInput {
    pub create_fk: Option<u64>,
    pub prev_index: u32,
}

/// Full packed decode of a wave-body tx (outs + inputs) for wire rebuild without
/// a second store read. Outs are Arc-shared with the parent live map.
struct BodyWire {
    tx: TxRecord,
    outputs: Arc<Vec<OutputRecord>>,
    inputs: Vec<InputRecord>,
}

/// Process-local, single-threaded during connect of one confirm wave.
pub struct WavePrevoutCache {
    parents: HashMap<u64, Parent>,
    by_txid: HashMap<[u8; 32], u64>,
    /// Wave-body spending txs (and creates): fk → row.
    txs: HashMap<u64, TxRecord>,
    /// Wave-body non-coinbase inputs: spending_fk → edges aligned to input index.
    thin_inputs: HashMap<u64, Vec<ThinInput>>,
    /// Full Class A parts for wave-body fks (wire reconstruct).
    body_wire: HashMap<u64, BodyWire>,
}

impl WavePrevoutCache {
    pub fn with_capacity(n_parents: usize, n_txs: usize) -> Self {
        Self {
            parents: HashMap::with_capacity(n_parents),
            by_txid: HashMap::with_capacity(n_parents + n_txs),
            txs: HashMap::with_capacity(n_txs),
            thin_inputs: HashMap::with_capacity(n_txs),
            body_wire: HashMap::with_capacity(n_txs),
        }
    }

    pub fn len(&self) -> usize {
        self.parents.len()
    }

    pub fn is_empty(&self) -> bool {
        self.parents.is_empty() && self.txs.is_empty()
    }

    pub fn insert_tx(&mut self, fk: Fk, tx: TxRecord) {
        let id = match fk.get() {
            Some(i) => i,
            None => return,
        };
        self.by_txid.insert(tx.txid, id);
        self.txs.insert(id, tx);
    }

    pub fn insert_thin_inputs(&mut self, spending_fk: Fk, edges: Vec<ThinInput>) {
        let id = match spending_fk.get() {
            Some(i) => i,
            None => return,
        };
        self.thin_inputs.insert(id, edges);
    }

    /// Stash Class A decode for wire rebuild; Arc-share outs with parent live map.
    ///
    /// Call after (or with) [`Self::insert_parent_all_live`] using the same `outputs` Arc.
    pub fn insert_body_wire(
        &mut self,
        fk: Fk,
        tx: TxRecord,
        outputs: Arc<Vec<OutputRecord>>,
        inputs: Vec<InputRecord>,
    ) {
        let Some(id) = fk.get() else {
            return;
        };
        self.body_wire.insert(
            id,
            BodyWire {
                tx,
                outputs,
                inputs,
            },
        );
    }

    /// One-shot wave-body insert: parent all-live + body_wire, outs Arc-shared.
    ///
    /// Avoids cloning every script into both maps. Wire rebuild clones scripts
    /// into `bitcoin::Transaction` later (unavoidable); fill itself does not.
    pub fn insert_wave_body(
        &mut self,
        fk: Fk,
        tx: TxRecord,
        outputs: Vec<OutputRecord>,
        inputs: Vec<InputRecord>,
        coinbase_height: Option<Option<u32>>,
    ) {
        let outs = Arc::new(outputs);
        self.insert_parent_all_live(fk, tx.clone(), Arc::clone(&outs), coinbase_height);
        self.insert_body_wire(fk, tx, outs, inputs);
    }

    /// Move body outs/inputs out for wire rebuild (one consumer).
    ///
    /// When parent still holds the outs Arc, scripts are cloned once into an owned
    /// `Vec` for `transaction_from_class_a`. When unique, `Arc::try_unwrap` moves.
    pub fn take_body_wire(
        &mut self,
        fk: Fk,
    ) -> Option<(TxRecord, Vec<OutputRecord>, Vec<InputRecord>)> {
        let id = fk.get()?;
        let bw = self.body_wire.remove(&id)?;
        let outputs = match Arc::try_unwrap(bw.outputs) {
            Ok(v) => v,
            Err(arc) => (*arc).clone(),
        };
        Some((bw.tx, outputs, bw.inputs))
    }

    pub fn get_tx(&self, fk: Fk) -> Option<&TxRecord> {
        let id = fk.get()?;
        self.txs
            .get(&id)
            .or_else(|| self.parents.get(&id).map(|p| &p.tx))
    }

    pub fn thin_inputs(&self, spending_fk: Fk) -> Option<&[ThinInput]> {
        let id = spending_fk.get()?;
        self.thin_inputs.get(&id).map(|v| v.as_slice())
    }

    /// Insert parent with all outputs live (wave bodies / same-run creates).
    pub fn insert_parent_live(
        &mut self,
        fk: Fk,
        tx: TxRecord,
        outputs: Vec<OutputRecord>,
        coinbase_height: Option<Option<u32>>,
    ) {
        self.insert_parent_all_live(fk, tx, Arc::new(outputs), coinbase_height);
    }

    /// Parent all-live with pre-built Arc (share with body_wire).
    pub fn insert_parent_all_live(
        &mut self,
        fk: Fk,
        tx: TxRecord,
        outputs: Arc<Vec<OutputRecord>>,
        coinbase_height: Option<Option<u32>>,
    ) {
        let id = match fk.get() {
            Some(i) => i,
            None => return,
        };
        self.by_txid.insert(tx.txid, id);
        self.parents.insert(
            id,
            Parent {
                fk,
                tx,
                outputs: ParentOuts::AllLive(outputs),
                coinbase_height,
            },
        );
    }

    /// Insert parent with only selected live outs (external parents).
    ///
    /// Sparse map — no `n_out`-sized `Vec<Option<_>>` (multi-out creates stay small).
    pub fn insert_parent_sparse(
        &mut self,
        fk: Fk,
        tx: TxRecord,
        _n_out: u32,
        live: impl IntoIterator<Item = (u32, OutputRecord)>,
        coinbase_height: Option<Option<u32>>,
    ) {
        let mut map = HashMap::new();
        for (v, o) in live {
            map.insert(v, o);
        }
        let id = match fk.get() {
            Some(i) => i,
            None => return,
        };
        self.by_txid.insert(tx.txid, id);
        self.parents.insert(
            id,
            Parent {
                fk,
                tx,
                outputs: ParentOuts::Sparse(map),
                coinbase_height,
            },
        );
    }

    /// Insert from dense `Option` slots (tests / legacy).
    pub fn insert_parent_slots(
        &mut self,
        fk: Fk,
        tx: TxRecord,
        outputs: Vec<Option<OutputRecord>>,
        coinbase_height: Option<Option<u32>>,
    ) {
        let all_live = outputs.iter().all(|o| o.is_some());
        if all_live {
            let dense: Vec<OutputRecord> = outputs.into_iter().map(|o| o.unwrap()).collect();
            self.insert_parent_live(fk, tx, dense, coinbase_height);
            return;
        }
        let live: Vec<(u32, OutputRecord)> = outputs
            .into_iter()
            .enumerate()
            .filter_map(|(i, o)| o.map(|rec| (i as u32, rec)))
            .collect();
        self.insert_parent_sparse(fk, tx, 0, live, coinbase_height);
    }

    pub fn has_live_output_txid(&self, txid: &[u8; 32], vout: u32) -> bool {
        let Some(&id) = self.by_txid.get(txid) else {
            return false;
        };
        self.parents
            .get(&id)
            .is_some_and(|p| parent_out_ref(p, vout).is_some())
    }

    /// Cached coinbase height: `Some(None)` not coinbase; `Some(Some(h))` height.
    pub fn coinbase_height_fk(&self, fk: Fk) -> Option<Option<u32>> {
        let id = fk.get()?;
        self.parents.get(&id).and_then(|p| p.coinbase_height)
    }

    pub fn get_by_fk(&self, fk: Fk, vout: u32) -> Option<(Fk, &TxRecord, &OutputRecord)> {
        let id = fk.get()?;
        let p = self.parents.get(&id)?;
        let o = parent_out_ref(p, vout)?;
        Some((p.fk, &p.tx, o))
    }

    pub fn get_by_txid(
        &self,
        txid: &[u8; 32],
        vout: u32,
    ) -> Option<(Fk, &TxRecord, &OutputRecord)> {
        let id = *self.by_txid.get(txid)?;
        let p = self.parents.get(&id)?;
        let o = parent_out_ref(p, vout)?;
        Some((p.fk, &p.tx, o))
    }
}

#[inline]
fn parent_out_ref(p: &Parent, vout: u32) -> Option<&OutputRecord> {
    match &p.outputs {
        ParentOuts::AllLive(v) => v.get(vout as usize),
        ParentOuts::Sparse(m) => m.get(&vout),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rbitcoin_store::{OutputRecord, TxRecord};

    fn tx(id: u8, n_out: u32) -> TxRecord {
        let mut txid = [0u8; 32];
        txid[0] = id;
        TxRecord {
            txid,
            version: 1,
            locktime: 0,
            input_start_fk: Fk::NULL,
            input_count: 0,
            output_start_fk: Fk(1),
            output_count: n_out,
        }
    }

    fn out(v: i64) -> OutputRecord {
        OutputRecord::unspent(v, vec![0x51, v as u8])
    }

    #[test]
    fn get_by_txid_not_confused_by_wrong_fk_live_slot() {
        let mut w = WavePrevoutCache::with_capacity(2, 0);
        w.insert_parent_live(Fk(10), tx(10, 2), vec![out(1), out(2)], Some(None));
        w.insert_parent_slots(
            Fk(20),
            tx(20, 2),
            vec![None, Some(out(99))],
            Some(None),
        );

        let want_txid = tx(20, 2).txid;
        let wrong = w.get_by_fk(Fk(10), 1).unwrap();
        assert_ne!(wrong.1.txid, want_txid);
        assert_eq!(wrong.2.value, 2);

        let (fk, rec, o) = w.get_by_txid(&want_txid, 1).unwrap();
        assert_eq!(fk, Fk(20));
        assert_eq!(rec.txid, want_txid);
        assert_eq!(o.value, 99);

        let filtered = w
            .get_by_fk(Fk(10), 1)
            .filter(|(_, rec, _)| rec.txid == want_txid);
        assert!(filtered.is_none());
        let ok = w
            .get_by_fk(Fk(20), 1)
            .filter(|(_, rec, _)| rec.txid == want_txid);
        assert!(ok.is_some());
    }

    #[test]
    fn body_wire_arc_share_no_double_clone_at_fill() {
        let mut w = WavePrevoutCache::with_capacity(1, 1);
        let t = tx(1, 1);
        let outs = vec![out(10)];
        let ins: Vec<InputRecord> = vec![];
        w.insert_wave_body(Fk(1), t.clone(), outs, ins, Some(None));
        // Parent and body_wire both see the out without a second fill-time clone.
        assert_eq!(w.get_by_fk(Fk(1), 0).unwrap().2.value, 10);
        let (tx2, o2, i2) = w.take_body_wire(Fk(1)).unwrap();
        assert_eq!(tx2.txid, t.txid);
        assert_eq!(o2.len(), 1);
        assert!(i2.is_empty());
        // Parent still live after take (Arc shared → clone on take).
        assert_eq!(w.get_by_fk(Fk(1), 0).unwrap().2.value, 10);
        assert!(w.take_body_wire(Fk(1)).is_none());
    }

    #[test]
    fn sparse_parent_no_dense_slots() {
        let mut w = WavePrevoutCache::with_capacity(1, 0);
        // Parent claims 2000 outs on meta but only one live vout needed.
        w.insert_parent_sparse(
            Fk(5),
            tx(5, 2000),
            2000,
            vec![(1999, out(7))],
            Some(None),
        );
        assert!(w.get_by_fk(Fk(5), 0).is_none());
        assert_eq!(w.get_by_fk(Fk(5), 1999).unwrap().2.value, 7);
    }
}
