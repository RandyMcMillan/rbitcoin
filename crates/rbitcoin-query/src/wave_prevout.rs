//! Wave-scoped tables for one confirm run (lock-free on connect).
//!
//! Holds:
//! - **Parent creates** with live/spent output slots (write-through unspent)
//! - **Spending txs** in the wave (`TxRecord` + thin prev_fk per input)
//! - **Coinbase create height** for parents (maturity without `tx_height` re-read)
//!
//! Built during prefetch after Class A body warm.

use rbitcoin_primitives::Fk;
use rbitcoin_store::{OutputRecord, TxRecord};
use std::collections::HashMap;

struct Parent {
    fk: Fk,
    tx: TxRecord,
    /// Per-vout: `Some` = live unspent at fill; `None` = spent/absent.
    outputs: Vec<Option<OutputRecord>>,
    /// `None` = not computed; `Some(None)` = not coinbase; `Some(Some(h))` = cb height.
    coinbase_height: Option<Option<u32>>,
}

/// Thin input edge: prev create fk when known (coinbase / external hash → None).
#[derive(Clone, Copy, Debug)]
pub struct ThinInput {
    pub prev_tx_fk: Option<u64>,
    pub prev_index: u32,
}

/// Process-local, single-threaded during connect of one confirm wave.
pub struct WavePrevoutCache {
    parents: HashMap<u64, Parent>,
    by_txid: HashMap<[u8; 32], u64>,
    /// Wave-body spending txs (and creates): fk → row.
    txs: HashMap<u64, TxRecord>,
    /// Wave-body non-coinbase inputs: spending_fk → edges aligned to input index.
    thin_inputs: HashMap<u64, Vec<ThinInput>>,
}

impl WavePrevoutCache {
    pub fn with_capacity(n_parents: usize, n_txs: usize) -> Self {
        Self {
            parents: HashMap::with_capacity(n_parents),
            by_txid: HashMap::with_capacity(n_parents + n_txs),
            txs: HashMap::with_capacity(n_txs),
            thin_inputs: HashMap::with_capacity(n_txs),
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

    pub fn get_tx(&self, fk: Fk) -> Option<&TxRecord> {
        let id = fk.get()?;
        self.txs.get(&id).or_else(|| self.parents.get(&id).map(|p| &p.tx))
    }

    pub fn thin_inputs(&self, spending_fk: Fk) -> Option<&[ThinInput]> {
        let id = spending_fk.get()?;
        self.thin_inputs.get(&id).map(|v| v.as_slice())
    }

    /// Insert parent with all outputs live (same-run creates).
    pub fn insert_parent_live(
        &mut self,
        fk: Fk,
        tx: TxRecord,
        outputs: Vec<OutputRecord>,
        coinbase_height: Option<Option<u32>>,
    ) {
        let slots: Vec<Option<OutputRecord>> = outputs.into_iter().map(Some).collect();
        self.insert_parent_slots(fk, tx, slots, coinbase_height);
    }

    pub fn insert_parent_slots(
        &mut self,
        fk: Fk,
        tx: TxRecord,
        outputs: Vec<Option<OutputRecord>>,
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
                outputs,
                coinbase_height,
            },
        );
    }

    pub fn has_live_output_fk(&self, fk: Fk, vout: u32) -> bool {
        let Some(id) = fk.get() else {
            return false;
        };
        matches!(
            self.parents
                .get(&id)
                .and_then(|p| p.outputs.get(vout as usize)),
            Some(Some(_))
        )
    }

    pub fn has_live_output_txid(&self, txid: &[u8; 32], vout: u32) -> bool {
        let Some(&id) = self.by_txid.get(txid) else {
            return false;
        };
        matches!(
            self.parents
                .get(&id)
                .and_then(|p| p.outputs.get(vout as usize)),
            Some(Some(_))
        )
    }

    /// Cached coinbase height: `Some(None)` not coinbase; `Some(Some(h))` height.
    pub fn coinbase_height_fk(&self, fk: Fk) -> Option<Option<u32>> {
        let id = fk.get()?;
        self.parents.get(&id).and_then(|p| p.coinbase_height)
    }

    pub fn get_by_fk(&self, fk: Fk, vout: u32) -> Option<(Fk, &TxRecord, &OutputRecord)> {
        let id = fk.get()?;
        let p = self.parents.get(&id)?;
        let o = p.outputs.get(vout as usize)?.as_ref()?;
        Some((p.fk, &p.tx, o))
    }

    pub fn get_by_txid(
        &self,
        txid: &[u8; 32],
        vout: u32,
    ) -> Option<(Fk, &TxRecord, &OutputRecord)> {
        let id = *self.by_txid.get(txid)?;
        let p = self.parents.get(&id)?;
        let o = p.outputs.get(vout as usize)?.as_ref()?;
        Some((p.fk, &p.tx, o))
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
        OutputRecord {
            value: v,
            script: vec![0x51, v as u8],
        }
    }

    /// Regression: wrong prev_tx_fk must not be preferred over wire txid.
    ///
    /// Connect resolves by **txid first**; if only get_by_fk were used, a stale
    /// fk could return another wave parent's scriptPubKey (script false).
    #[test]
    fn get_by_txid_not_confused_by_wrong_fk_live_slot() {
        let mut w = WavePrevoutCache::with_capacity(2, 0);
        // Wave-body create at fk=10 (all outs live).
        w.insert_parent_live(Fk(10), tx(10, 2), vec![out(1), out(2)], Some(None));
        // External parent at fk=20 that the spend actually wants.
        w.insert_parent_slots(
            Fk(20),
            tx(20, 2),
            vec![None, Some(out(99))],
            Some(None),
        );

        let want_txid = tx(20, 2).txid;
        // Wrong fk (wave body) has a live vout=1, but different txid.
        let wrong = w.get_by_fk(Fk(10), 1).unwrap();
        assert_ne!(wrong.1.txid, want_txid);
        assert_eq!(wrong.2.value, 2);

        // Authoritative path: by wire txid → correct parent + output.
        let (fk, rec, o) = w.get_by_txid(&want_txid, 1).unwrap();
        assert_eq!(fk, Fk(20));
        assert_eq!(rec.txid, want_txid);
        assert_eq!(o.value, 99);

        // Production filter: only accept fk hit when txid matches.
        let filtered = w
            .get_by_fk(Fk(10), 1)
            .filter(|(_, rec, _)| rec.txid == want_txid);
        assert!(filtered.is_none());
        let ok = w
            .get_by_fk(Fk(20), 1)
            .filter(|(_, rec, _)| rec.txid == want_txid);
        assert!(ok.is_some());
    }
}
