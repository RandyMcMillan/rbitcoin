//! Unflushed `tx.head` insert map (write-behind).
//!
//! Sole Class A appender notes txid→fk after body/idx publish. Resolve/stamp
//! read a published snapshot (brief lock to clone the Arc). Drain uses the same
//! page-grouped [`crate::tx_table::TxTable::head_insert_many`]. Not a process
//! pin FIFO — no outs / spent_range.

use rbitcoin_primitives::Fk;
use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};

/// Soft cap on queued inserts. Writer must drain before enqueueing more.
pub const PENDING_HEAD_CAP: usize = 262_144;

pub struct PendingHeadInserts {
    snap: RwLock<Arc<HashMap<[u8; 32], Fk>>>,
    queued: Mutex<Vec<([u8; 32], Fk)>>,
}

impl PendingHeadInserts {
    pub fn new() -> Self {
        Self {
            snap: RwLock::new(Arc::new(HashMap::new())),
            queued: Mutex::new(Vec::new()),
        }
    }

    pub fn len(&self) -> usize {
        self.queued.lock().unwrap_or_else(|e| e.into_inner()).len()
    }

    pub fn get(&self, txid: &[u8; 32]) -> Option<Fk> {
        let snap = {
            let g = self.snap.read().unwrap_or_else(|e| e.into_inner());
            Arc::clone(&*g)
        };
        snap.get(txid).copied()
    }

    pub fn note(&self, entries: &[([u8; 32], Fk)]) {
        if entries.is_empty() {
            return;
        }
        {
            let mut q = self.queued.lock().unwrap_or_else(|e| e.into_inner());
            q.extend_from_slice(entries);
        }
        self.rebuild_snap();
    }

    /// Take queued inserts for drain. Snapshot stays until [`Self::forget`].
    pub fn take_queued(&self) -> Vec<([u8; 32], Fk)> {
        let mut q = self.queued.lock().unwrap_or_else(|e| e.into_inner());
        std::mem::take(&mut *q)
    }

    pub fn forget(&self, entries: &[([u8; 32], Fk)]) {
        if entries.is_empty() && self.is_empty_snap() {
            return;
        }
        let mut g = self.snap.write().unwrap_or_else(|e| e.into_inner());
        if entries.is_empty() {
            *g = Arc::new(HashMap::new());
            return;
        }
        let mut m = (**g).clone();
        for (txid, fk) in entries {
            if m.get(txid) == Some(fk) {
                m.remove(txid);
            }
        }
        *g = Arc::new(m);
    }

    fn is_empty_snap(&self) -> bool {
        self.snap
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .is_empty()
    }

    fn rebuild_snap(&self) {
        let q = self.queued.lock().unwrap_or_else(|e| e.into_inner());
        let mut m = HashMap::with_capacity(q.len());
        for &(txid, fk) in q.iter() {
            m.insert(txid, fk);
        }
        drop(q);
        // Keep already-draining keys (taken from queued, not yet forgotten).
        {
            let g = self.snap.read().unwrap_or_else(|e| e.into_inner());
            for (k, v) in g.iter() {
                m.entry(*k).or_insert(*v);
            }
        }
        *self.snap.write().unwrap_or_else(|e| e.into_inner()) = Arc::new(m);
    }
}

impl Default for PendingHeadInserts {
    fn default() -> Self {
        Self::new()
    }
}
