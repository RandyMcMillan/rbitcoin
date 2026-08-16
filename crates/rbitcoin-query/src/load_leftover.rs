//! Load-thread leftover write-behind identity (`txid → create_fk`).
//!
//! Write is the sole sender on [`LoadInbox`] (**`Note` only**). Load is the
//! sole receiver and the sole mutator of [`LoadLeftoverPending`]. Drain
//! complete is a post-insert HWM load polls (not a queue). Header-cache GC
//! polls `tip_height()`. Write still publishes pin layout on batch Arcs.
//!
//! Forget is load-only: fence covers **and** `height < drain_through_excl`
//! **and** height strictly below the pack being stamped. Keep the previous
//! pack (n−1 parent of tip+1). Last fk wins for a duplicate txid
//! ([`docs/errata.md`](../../../docs/errata.md)).

use rbitcoin_primitives::Fk;
use rbitcoin_store::HeightFence;
use std::collections::{BTreeMap, HashMap};
use std::sync::mpsc::{self, Receiver, Sender, TryRecvError};
use std::sync::Mutex;

/// Same class as store `PENDING_HEAD_CAP`. Inbox send never blocks write.
const LOAD_LEFTOVER_CAP: usize = 262_144;

/// Write → load handoff. Write must not block on a full channel.
///
/// Only payload load cannot poll: Class A `txid → fk` before `tx.head` drain.
#[derive(Debug, Clone)]
pub enum LoadInboxMsg {
    /// Class A published `txid.body` / idx; `tx.head` may still be queued.
    Note {
        height: u32,
        entries: Vec<([u8; 32], Fk)>,
    },
}

/// Load-owned leftover identity. Mutated only on the load thread (or under
/// the inbox lock that write never takes).
#[derive(Debug, Default)]
pub struct LoadLeftoverPending {
    by_txid: HashMap<[u8; 32], Fk>,
    by_height: BTreeMap<u32, Vec<([u8; 32], Fk)>>,
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

    /// Last fk wins (errata one-fk-per-txid). Stale `(txid, old_fk)` stays on
    /// the old height slot; forget only removes if `by_txid` still holds that fk.
    pub fn apply_note(&mut self, height: u32, entries: Vec<([u8; 32], Fk)>) {
        if entries.is_empty() {
            return;
        }
        let slot = self.by_height.entry(height).or_default();
        slot.reserve(entries.len());
        self.by_txid.reserve(entries.len());
        for (txid, fk) in entries {
            self.by_txid.insert(txid, fk);
            slot.push((txid, fk));
        }
    }

    /// Disconnect / abandon: drop leftover identity for this height now.
    pub fn drop_height(&mut self, height: u32) {
        if let Some(entries) = self.by_height.remove(&height) {
            for (t, noted_fk) in entries {
                if self.by_txid.get(&t) == Some(&noted_fk) {
                    self.by_txid.remove(&t);
                }
            }
        }
    }

    /// Drop heights `< pack_lo` whose inserts are visible (`height < drain_through_excl`)
    /// and every remaining noted fk is fence-connected. After leftover bind.
    ///
    /// `drain_through_excl == 0` means insert has never completed (keep all).
    pub fn forget_ready(&mut self, fence: &HeightFence, pack_lo: u32, drain_through_excl: u64) {
        let drop_h: Vec<u32> = self
            .by_height
            .range(..pack_lo)
            .filter(|(h, entries)| {
                (**h as u64) < drain_through_excl
                    && entries.iter().all(|(t, noted_fk)| {
                        self.by_txid
                            .get(t)
                            .is_none_or(|cur| *cur != *noted_fk || fence.height_of(*cur).is_some())
                    })
            })
            .map(|(h, _)| *h)
            .collect();
        for h in drop_h {
            if let Some(entries) = self.by_height.remove(&h) {
                for (t, noted_fk) in entries {
                    if self.by_txid.get(&t) == Some(&noted_fk) {
                        self.by_txid.remove(&t);
                    }
                }
            }
        }
    }

    pub fn over_cap(&self) -> bool {
        self.by_txid.len() > LOAD_LEFTOVER_CAP
    }
}

struct LoadInboxInner {
    rx: Receiver<LoadInboxMsg>,
    pending: LoadLeftoverPending,
}

/// Query-owned pair: write clones nothing; send is lock-free on the channel.
pub struct LoadInbox {
    tx: Sender<LoadInboxMsg>,
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
    pub fn send(&self, msg: LoadInboxMsg) {
        let _ = self.tx.send(msg);
    }

    /// Ingest Notes. Drain complete and tip are polled, not queued.
    pub fn ingest(&self) -> Result<(), rbitcoin_store::StoreError> {
        let mut g = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        loop {
            match g.rx.try_recv() {
                Ok(LoadInboxMsg::Note { height, entries }) => {
                    g.pending.apply_note(height, entries);
                }
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => break,
            }
        }
        if g.pending.over_cap() {
            return Err(rbitcoin_store::StoreError::Corrupt(
                "load leftover pending exceeded PENDING_HEAD_CAP",
            ));
        }
        Ok(())
    }

    /// Load leftover walk + forget. Holds the inbox lock for the pack (write
    /// never takes it).
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
        p.apply_note(0, vec![(txid, Fk(7))]);
        assert_eq!(p.fk(&txid), Some(Fk(7)));
        assert!(p.fk(&[0x22u8; 32]).is_none());
    }

    #[test]
    fn leftover_keeps_prev_pack_after_drain_done() {
        let (dir, store) = temp_store("keep-prev");
        store.confirmed.set(Height(0), Fk(1)).unwrap();
        store.header_txs.put_range(Fk(1), Fk(1), 1).unwrap();
        store.rebuild_height_fence().unwrap();
        let fence = store.height_fence_snapshot();
        assert!(fence.height_of(Fk(1)).is_some());

        let mut p = LoadLeftoverPending::new();
        let parent = [0xAAu8; 32];
        p.apply_note(0, vec![(parent, Fk(1))]);
        // Stamping height 1: pack_lo=1. Forget after bind may drop 0, so bind first.
        assert_eq!(p.fk(&parent), Some(Fk(1)), "bind height-1 child");
        p.forget_ready(&fence, 1, 1);
        // After leftover of height 1, height 0 may drop.
        // Stamping height 2: pack_lo=2 drops 0.
        p.forget_ready(&fence, 2, 1);
        assert!(
            p.fk(&parent).is_none(),
            "height 0 may drop once pack_lo is 2"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn leftover_keeps_without_drain_done() {
        let (dir, store) = temp_store("no-drain");
        store.confirmed.set(Height(0), Fk(1)).unwrap();
        store.header_txs.put_range(Fk(1), Fk(1), 1).unwrap();
        store.rebuild_height_fence().unwrap();
        let fence = store.height_fence_snapshot();

        let mut p = LoadLeftoverPending::new();
        let parent = [0xBBu8; 32];
        p.apply_note(0, vec![(parent, Fk(1))]);
        p.forget_ready(&fence, 99, 0);
        assert_eq!(
            p.fk(&parent),
            Some(Fk(1)),
            "drain_through 0 ⇒ keep even if fence covers"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn leftover_drop_height_on_disconnect() {
        let mut p = LoadLeftoverPending::new();
        let txid = [0xEEu8; 32];
        p.apply_note(5, vec![(txid, Fk(3))]);
        p.drop_height(5);
        assert!(p.fk(&txid).is_none());
    }

    #[test]
    fn leftover_clobber_last_fk_wins() {
        let mut p = LoadLeftoverPending::new();
        let txid = [0xCCu8; 32];
        p.apply_note(0, vec![(txid, Fk(1))]);
        p.apply_note(1, vec![(txid, Fk(2))]);
        assert_eq!(p.fk(&txid), Some(Fk(2)));
    }

    #[test]
    fn leftover_forget_does_not_drop_clobbered_fk() {
        let (dir, store) = temp_store("clobber-forget");
        store.confirmed.set(Height(0), Fk(1)).unwrap();
        store.header_txs.put_range(Fk(1), Fk(1), 1).unwrap();
        store.rebuild_height_fence().unwrap();
        let fence = store.height_fence_snapshot();
        let mut p = LoadLeftoverPending::new();
        let txid = [0xCDu8; 32];
        p.apply_note(0, vec![(txid, Fk(1))]);
        p.apply_note(1, vec![(txid, Fk(2))]);
        p.forget_ready(&fence, 1, 1);
        assert_eq!(
            p.fk(&txid),
            Some(Fk(2)),
            "forget of old height must not remove the newer fk"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn inbox_ingest_note_then_bind() {
        let inbox = LoadInbox::new();
        let txid = [0xDDu8; 32];
        inbox.send(LoadInboxMsg::Note {
            height: 3,
            entries: vec![(txid, Fk(9))],
        });
        inbox.ingest().unwrap();
        inbox.with_pending_mut(|p| {
            assert_eq!(p.fk(&txid), Some(Fk(9)));
        });
    }
}
