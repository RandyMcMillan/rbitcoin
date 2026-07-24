//! Batch-local full Class A bodies for confirm load → wire.
//!
//! Load decodes each create once into this map. Wire rebuild reads from here
//! instead of re-`get_tx_full` from the store. Long-lived OutFifo still holds
//! **outs only** (inputs are not retained process-wide).

use rbitcoin_primitives::Fk;
use rbitcoin_store::{InputRecord, OutputRecord, TxRecord};
use std::collections::HashMap;

/// Full packed Class A for creates decoded in one confirm load batch.
#[derive(Debug, Default, Clone)]
pub struct BatchFullBodies {
    /// create_fk id → (height, meta, inputs, outputs)
    map: HashMap<u64, (u32, TxRecord, Vec<InputRecord>, Vec<OutputRecord>)>,
}

impl BatchFullBodies {
    pub fn new() -> Self {
        Self {
            map: HashMap::new(),
        }
    }

    pub fn with_capacity(n: usize) -> Self {
        Self {
            map: HashMap::with_capacity(n),
        }
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    pub fn insert(
        &mut self,
        fk: Fk,
        height: u32,
        tx: TxRecord,
        inputs: Vec<InputRecord>,
        outputs: Vec<OutputRecord>,
    ) {
        let Some(id) = fk.get() else {
            return;
        };
        self.map.insert(id, (height, tx, inputs, outputs));
    }

    /// Borrow full Class A (meta, inputs, outputs) for wire rebuild.
    pub fn get(
        &self,
        fk: Fk,
    ) -> Option<&(u32, TxRecord, Vec<InputRecord>, Vec<OutputRecord>)> {
        fk.get().and_then(|id| self.map.get(&id))
    }

    /// Owned clone for callers that need to move into `bitcoin::Transaction`.
    pub fn get_owned(
        &self,
        fk: Fk,
    ) -> Option<(TxRecord, Vec<InputRecord>, Vec<OutputRecord>)> {
        let (_, tx, inputs, outs) = self.get(fk)?;
        Some((tx.clone(), inputs.clone(), outs.clone()))
    }

    /// Create txid if this batch decoded the create body.
    pub fn txid(&self, fk: Fk) -> Option<[u8; 32]> {
        self.get(fk).map(|(_, tx, _, _)| tx.txid)
    }

    pub fn iter(
        &self,
    ) -> impl Iterator<Item = (Fk, u32, &TxRecord, &[InputRecord], &[OutputRecord])> {
        self.map.iter().map(|(&id, (h, tx, ins, outs))| {
            (Fk(id), *h, tx, ins.as_slice(), outs.as_slice())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rbitcoin_primitives::Fk;

    fn sample_tx(i: u8) -> TxRecord {
        let mut txid = [0u8; 32];
        txid[0] = i;
        TxRecord {
            txid,
            version: 1,
            locktime: 0,
            input_start_fk: Fk::NULL,
            input_count: 0,
            output_start_fk: Fk::NULL,
            output_count: 1,
        }
    }

    #[test]
    fn insert_get_roundtrip() {
        let mut b = BatchFullBodies::new();
        let tx = sample_tx(7);
        b.insert(Fk(3), 10, tx.clone(), vec![], vec![]);
        assert_eq!(b.len(), 1);
        assert_eq!(b.txid(Fk(3)), Some(tx.txid));
        let got = b.get_owned(Fk(3)).unwrap();
        assert_eq!(got.0.txid, tx.txid);
        assert!(b.get(Fk(99)).is_none());
    }

    #[test]
    fn iter_covers_inserts() {
        let mut b = BatchFullBodies::new();
        b.insert(Fk(1), 1, sample_tx(1), vec![], vec![]);
        b.insert(Fk(2), 2, sample_tx(2), vec![], vec![]);
        assert_eq!(b.iter().count(), 2);
    }
}
