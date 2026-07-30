//! Batch-local full Class A bodies for confirm load → wire.
//!
//! Load decodes each create once into this map. Wire rebuild reads from here
//! instead of re-`get_tx_full` from the store. CreateResidency holds
//! **outs only** (inputs are not retained process-wide) plus body layout for pin.

use rbitcoin_primitives::Fk;
use rbitcoin_store::{InputRecord, OutputRecord, TxRecord};
use std::collections::HashMap;

/// One create decoded in the load batch (wire + pin layout).
#[derive(Debug, Clone)]
pub struct BatchBody {
    pub height: u32,
    pub tx: TxRecord,
    pub inputs: Vec<InputRecord>,
    pub outputs: Vec<OutputRecord>,
    /// Packed `(body_off, body_len)` when known (from idx/sticky at decode).
    pub body_range: Option<(u64, u64)>,
    /// Dense spender_rels (rel to body_off); empty ⇒ layout unknown.
    pub denserels: Vec<u32>,
}

/// Full packed Class A for creates decoded in one confirm load batch.
#[derive(Debug, Default, Clone)]
pub struct BatchFullBodies {
    /// create_fk id → body
    map: HashMap<u64, BatchBody>,
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
        body_range: Option<(u64, u64)>,
        denserels: Vec<u32>,
    ) {
        let Some(id) = fk.get() else {
            return;
        };
        self.map.insert(
            id,
            BatchBody {
                height,
                tx,
                inputs,
                outputs,
                body_range,
                denserels,
            },
        );
    }

    /// Borrow full Class A body for wire rebuild / same-batch pin.
    pub fn get(&self, fk: Fk) -> Option<&BatchBody> {
        fk.get().and_then(|id| self.map.get(&id))
    }

    /// Owned clone for callers that need to move into `bitcoin::Transaction`.
    pub fn get_owned(
        &self,
        fk: Fk,
    ) -> Option<(TxRecord, Vec<InputRecord>, Vec<OutputRecord>)> {
        let b = self.get(fk)?;
        Some((b.tx.clone(), b.inputs.clone(), b.outputs.clone()))
    }

    /// Create txid if this batch decoded the create body.
    pub fn txid(&self, fk: Fk) -> Option<[u8; 32]> {
        self.get(fk).map(|b| b.tx.txid)
    }

    pub fn iter(&self) -> impl Iterator<Item = (Fk, &BatchBody)> {
        self.map.iter().map(|(&id, b)| (Fk(id), b))
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
        b.insert(
            Fk(3),
            10,
            tx.clone(),
            vec![],
            vec![],
            Some((100, 50)),
            vec![10],
        );
        assert_eq!(b.len(), 1);
        assert_eq!(b.txid(Fk(3)), Some(tx.txid));
        let got = b.get_owned(Fk(3)).unwrap();
        assert_eq!(got.0.txid, tx.txid);
        assert_eq!(b.get(Fk(3)).unwrap().body_range, Some((100, 50)));
        assert_eq!(b.get(Fk(3)).unwrap().denserels, vec![10]);
        assert!(b.get(Fk(99)).is_none());
    }

    #[test]
    fn iter_covers_inserts() {
        let mut b = BatchFullBodies::new();
        b.insert(Fk(1), 1, sample_tx(1), vec![], vec![], None, vec![]);
        b.insert(Fk(2), 2, sample_tx(2), vec![], vec![], None, vec![]);
        assert_eq!(b.iter().count(), 2);
    }

    #[test]
    fn with_capacity_and_null_fk() {
        let mut b = BatchFullBodies::with_capacity(4);
        assert_eq!(b.len(), 0);
        assert!(b.is_empty());
        b.insert(Fk::NULL, 0, sample_tx(0), vec![], vec![], None, vec![]);
        assert!(b.is_empty());
        b.insert(Fk(5), 3, sample_tx(5), vec![], vec![], None, vec![]);
        assert!(!b.is_empty());
        assert_eq!(b.len(), 1);
    }
}
