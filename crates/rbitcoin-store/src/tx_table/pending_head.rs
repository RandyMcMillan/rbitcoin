//! Unflushed `tx.head` insert list (write-local drain).
//!
//! Sole Class A appender notes txid→fk after body/idx publish. Drain uses
//! [`crate::tx_table::TxTable::head_insert_many`]. Leftover identity is
//! **not** here — load owns that map (`LoadLeftoverPending`).

use rbitcoin_primitives::Fk;
use std::sync::Mutex;

/// Soft cap on queued inserts. Writer must drain before enqueueing more.
pub const PENDING_HEAD_CAP: usize = 262_144;

pub struct PendingHeadInserts {
    queued: Mutex<Vec<([u8; 32], Fk)>>,
}

impl PendingHeadInserts {
    pub fn new() -> Self {
        Self {
            queued: Mutex::new(Vec::new()),
        }
    }

    pub fn len(&self) -> usize {
        self.queued.lock().unwrap_or_else(|e| e.into_inner()).len()
    }

    pub fn note(&self, entries: &[([u8; 32], Fk)]) {
        if entries.is_empty() {
            return;
        }
        let mut q = self.queued.lock().unwrap_or_else(|e| e.into_inner());
        q.extend_from_slice(entries);
    }

    /// Write-local: same-batch `put_spend` before drain (not leftover).
    pub fn queued_fk(&self, txid: &[u8; 32]) -> Option<Fk> {
        let q = self.queued.lock().unwrap_or_else(|e| e.into_inner());
        q.iter().rev().find(|(t, _)| t == txid).map(|(_, fk)| *fk)
    }

    /// Take queued inserts for drain (write thread, before spawn).
    pub fn take_queued(&self) -> Vec<([u8; 32], Fk)> {
        let mut q = self.queued.lock().unwrap_or_else(|e| e.into_inner());
        std::mem::take(&mut *q)
    }
}

impl Default for PendingHeadInserts {
    fn default() -> Self {
        Self::new()
    }
}
