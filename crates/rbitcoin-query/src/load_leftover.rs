//! Load-thread leftover write-behind identity (`txid → create_fk`).
//!
//! One map. Write sends **notes only** (Class A `txid → fk` before `tx.head`
//! insert). Load is the sole mutator. Drain complete is the max **inserted
//! fk** load polls (not tip, not fence — those advance during drain).
//! Header-cache GC polls `tip_height()`.
//!
//! Forget is per-fk, after leftover bind: keep if unfenced, or height ≥
//! pack_lo (`tip+1`), or `fk > drain_fk` (insert not done). Last fk wins
//! ([`docs/errata.md`](../../../docs/errata.md)).
//!
//! In-flight is a different prune (`covers_fk_span` of a layer) and is not
//! this map — see [`crate::in_flight`] and `docs/invariants.md`.

use rbitcoin_primitives::Fk;
use rbitcoin_store::HeightFence;
use std::collections::HashMap;
use std::sync::mpsc::{self, Receiver, Sender, TryRecvError};
use std::sync::Mutex;

/// Same class as store `PENDING_HEAD_CAP`. Inbox send never blocks write.
const LOAD_LEFTOVER_CAP: usize = 262_144;

/// Load-owned leftover identity. Mutated only on the load thread (or under
/// the inbox lock that write never takes).
#[derive(Debug, Default)]
pub struct LoadLeftoverPending {
    by_txid: HashMap<[u8; 32], Fk>,
}

impl LoadLeftoverPending {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn len(&self) -> usize {
        self.by_txid.len()
    }

    pub fn is_empty(&self) -> bool {
        self.by_txid.is_empty()
    }

    pub fn fk(&self, txid: &[u8; 32]) -> Option<Fk> {
        self.by_txid.get(txid).copied()
    }

    /// Last fk wins (errata one-fk-per-txid).
    pub fn apply_notes(&mut self, entries: impl IntoIterator<Item = ([u8; 32], Fk)>) {
        for (txid, fk) in entries {
            if fk.is_null() {
                continue;
            }
            self.by_txid.insert(txid, fk);
        }
    }

    /// Disconnect: evict identities we already have (block txids).
    pub fn drop_txids(&mut self, txids: impl IntoIterator<Item = [u8; 32]>) {
        for t in txids {
            self.by_txid.remove(&t);
        }
    }

    /// Per-fk forget after leftover bind.
    ///
    /// `drain_fk == 0` means insert has never completed (keep all).
    pub fn forget_ready(&mut self, fence: &HeightFence, pack_lo: u32, drain_fk: u64) {
        self.by_txid.retain(|_, fk| {
            let Some(id) = fk.get() else {
                return false;
            };
            let Some(h) = fence.height_of(*fk) else {
                return true;
            };
            if h >= pack_lo {
                return true;
            }
            id > drain_fk
        });
    }

    pub fn over_cap(&self) -> bool {
        self.by_txid.len() > LOAD_LEFTOVER_CAP
    }
}

struct LoadInboxInner {
    rx: Receiver<Vec<([u8; 32], Fk)>>,
    pending: LoadLeftoverPending,
}

/// Query-owned pair: write is sole sender; load is sole receiver.
pub struct LoadInbox {
    tx: Sender<Vec<([u8; 32], Fk)>>,
    inner: Mutex<LoadInboxInner>,
}

impl LoadInbox {
    pub fn new() -> Self {
        let (tx, rx) = mpsc::channel();
        Self {
            tx,
            inner: Mutex::new(LoadInboxInner {
                rx,
                pending: LoadLeftoverPending::new(),
            }),
        }
    }

    /// Never blocks. Disconnected rx (Query drop) is ignored.
    pub fn send_notes(&self, entries: Vec<([u8; 32], Fk)>) {
        if entries.is_empty() {
            return;
        }
        let _ = self.tx.send(entries);
    }

    pub fn ingest(&self) -> Result<(), rbitcoin_store::StoreError> {
        let mut g = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        loop {
            match g.rx.try_recv() {
                Ok(entries) => g.pending.apply_notes(entries),
                Err(TryRecvError::Empty) | Err(TryRecvError::Disconnected) => break,
            }
        }
        if g.pending.over_cap() {
            return Err(rbitcoin_store::StoreError::Corrupt(
                "load leftover pending exceeded PENDING_HEAD_CAP",
            ));
        }
        Ok(())
    }

    pub fn with_pending_mut<R>(&self, f: impl FnOnce(&mut LoadLeftoverPending) -> R) -> R {
        let mut g = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        f(&mut g.pending)
    }
}

impl Default for LoadInbox {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rbitcoin_primitives::Height;
    use rbitcoin_store::Store;

    fn temp_store(label: &str) -> (std::path::PathBuf, Store) {
        let n = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("rbitcoin-leftover-{label}-{n}"));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let s = Store::create(&dir).unwrap();
        (dir, s)
    }

    #[test]
    fn leftover_binds_load_owned_pending() {
        let mut p = LoadLeftoverPending::new();
        let txid = [0x11u8; 32];
        p.apply_notes([(txid, Fk(7))]);
        assert_eq!(p.fk(&txid), Some(Fk(7)));
        assert!(p.fk(&[0x22u8; 32]).is_none());
    }

    #[test]
    fn leftover_keeps_prev_pack_after_drain() {
        let (dir, store) = temp_store("keep-prev");
        store.confirmed.set(Height(0), Fk(1)).unwrap();
        store.header_txs.put_range(Fk(1), Fk(1), 1).unwrap();
        store.rebuild_height_fence().unwrap();
        let fence = store.height_fence_snapshot();

        let mut p = LoadLeftoverPending::new();
        let parent = [0xAAu8; 32];
        p.apply_notes([(parent, Fk(1))]);
        assert_eq!(p.fk(&parent), Some(Fk(1)), "bind height-1 child");
        // pack_lo=1, drain includes fk 1: may drop after bind.
        p.forget_ready(&fence, 1, 1);
        p.forget_ready(&fence, 2, 1);
        assert!(p.fk(&parent).is_none(), "below pack_lo + drained + fenced");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn leftover_keeps_until_insert_even_if_fenced() {
        let (dir, store) = temp_store("no-drain");
        store.confirmed.set(Height(0), Fk(1)).unwrap();
        store.header_txs.put_range(Fk(1), Fk(1), 1).unwrap();
        store.rebuild_height_fence().unwrap();
        let fence = store.height_fence_snapshot();

        let mut p = LoadLeftoverPending::new();
        let parent = [0xBBu8; 32];
        p.apply_notes([(parent, Fk(1))]);
        p.forget_ready(&fence, 99, 0);
        assert_eq!(
            p.fk(&parent),
            Some(Fk(1)),
            "drain_fk 0 ⇒ keep even if fence covers (67438)"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn leftover_drop_txids_on_disconnect() {
        let mut p = LoadLeftoverPending::new();
        let txid = [0xEEu8; 32];
        p.apply_notes([(txid, Fk(3))]);
        p.drop_txids([txid]);
        assert!(p.fk(&txid).is_none());
    }

    #[test]
    fn leftover_clobber_last_fk_wins() {
        let mut p = LoadLeftoverPending::new();
        let txid = [0xCCu8; 32];
        p.apply_notes([(txid, Fk(1)), (txid, Fk(2))]);
        assert_eq!(p.fk(&txid), Some(Fk(2)));
    }

    #[test]
    fn inbox_ingest_note_then_bind() {
        let inbox = LoadInbox::new();
        let txid = [0xDDu8; 32];
        inbox.send_notes(vec![(txid, Fk(9))]);
        inbox.ingest().unwrap();
        inbox.with_pending_mut(|p| {
            assert_eq!(p.fk(&txid), Some(Fk(9)));
        });
    }
}
