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
