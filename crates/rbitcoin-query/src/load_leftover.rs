//! Load-thread leftover write-behind identity (`txid → create_fk`).
//!
//! Write is the sole sender on [`LoadInbox`]. Load is the sole receiver and
//! the sole mutator of [`LoadLeftoverPending`]. No `RwLock<Arc<HashMap>>`
//! snap — leftover bind is a plain map get after inbox ingest.
//!
//! Forget is load-only: fence covers **and** [`LoadInboxMsg::DrainDone`]
//! **and** height strictly below the pack being stamped. Keep the previous
//! pack (n−1 parent of tip+1). Last fk wins for a duplicate txid
//! ([`docs/errata.md`](../../../docs/errata.md)).

use rbitcoin_primitives::Fk;
use rbitcoin_store::HeightFence;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::mpsc::{self, Receiver, Sender, TryRecvError};
use std::sync::Mutex;

/// Same class as store `PENDING_HEAD_CAP`. Inbox send never blocks write.
const LOAD_LEFTOVER_CAP: usize = 262_144;

/// Write → load handoff. Write must not block on a full channel.
#[derive(Debug, Clone)]
pub enum LoadInboxMsg {
    /// Class A published `txid.body` / idx; `tx.head` may still be queued.
    Note {
        height: u32,
        entries: Vec<([u8; 32], Fk)>,
    },
    /// `head_insert_many` returned for this height's queued inserts.
    DrainDone { height: u32 },
    /// Class C tip advanced; load GCs header plans `<= tip`.
    TipAdvanced { tip: u32 },
    /// Write filled body/spent ranges on a **batch-local** pin. Load is the
    /// only publisher into [`crate::PipelineParentStore`].
    LayoutDone {
        fk: Fk,
        body_range: Option<(u64, u64)>,
        spent_range: Option<(u64, u64)>,
    },
}

/// Side effects from inbox ingest (applied by Query leftover, not the map).
#[derive(Debug, Clone)]
pub enum LoadInboxEffect {
    TipAdvanced {
        tip: u32,
    },
    LayoutDone {
        fk: Fk,
        body_range: Option<(u64, u64)>,
        spent_range: Option<(u64, u64)>,
    },
}

/// Load-owned leftover identity. Mutated only on the load thread (or under
/// the inbox lock that write never takes).
#[derive(Debug, Default)]
pub struct LoadLeftoverPending {
    by_txid: HashMap<[u8; 32], Fk>,
    by_height: BTreeMap<u32, Vec<[u8; 32]>>,
    drain_done: BTreeSet<u32>,
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
    pub fn apply_note(&mut self, height: u32, entries: Vec<([u8; 32], Fk)>) {
        if entries.is_empty() {
            return;
        }
        let slot = self.by_height.entry(height).or_default();
        slot.reserve(entries.len());
        self.by_txid.reserve(entries.len());
        for (txid, fk) in entries {
            self.by_txid.insert(txid, fk);
            slot.push(txid);
        }
    }

    pub fn apply_drain_done(&mut self, height: u32) {
        self.drain_done.insert(height);
    }

    /// Disconnect / abandon: drop leftover identity for this height now.
    pub fn drop_height(&mut self, height: u32) {
        if let Some(txids) = self.by_height.remove(&height) {
            for t in txids {
                self.by_txid.remove(&t);
            }
        }
        self.drain_done.remove(&height);
    }

    /// Drop heights `< pack_lo` that have DrainDone and every remaining fk
    /// is fence-connected. Call **after** leftover bind of this pack.
    pub fn forget_ready(&mut self, fence: &HeightFence, pack_lo: u32) {
        let drop_h: Vec<u32> = self
            .by_height
            .range(..pack_lo)
            .filter(|(h, txids)| {
                self.drain_done.contains(h)
                    && txids.iter().all(|t| {
                        self.by_txid
                            .get(t)
                            .is_none_or(|fk| fence.height_of(*fk).is_some())
                    })
            })
            .map(|(h, _)| *h)
            .collect();
        for h in drop_h {
            if let Some(txids) = self.by_height.remove(&h) {
                for t in txids {
                    self.by_txid.remove(&t);
                }
            }
            self.drain_done.remove(&h);
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

    /// Ingest the channel. Notes/DrainDone update the map. Tip/layout are
    /// returned for Query to apply (header GC, parent-store layout).
    pub fn ingest(&self) -> Result<Vec<LoadInboxEffect>, rbitcoin_store::StoreError> {
        let mut g = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let mut effects = Vec::new();
        loop {
            match g.rx.try_recv() {
                Ok(LoadInboxMsg::Note { height, entries }) => {
                    g.pending.apply_note(height, entries);
                }
                Ok(LoadInboxMsg::DrainDone { height }) => {
                    g.pending.apply_drain_done(height);
                }
                Ok(LoadInboxMsg::TipAdvanced { tip }) => {
                    effects.push(LoadInboxEffect::TipAdvanced { tip });
                }
                Ok(LoadInboxMsg::LayoutDone {
                    fk,
                    body_range,
                    spent_range,
                }) => {
                    effects.push(LoadInboxEffect::LayoutDone {
                        fk,
                        body_range,
                        spent_range,
                    });
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
        Ok(effects)
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
        p.apply_drain_done(0);
        // Stamping height 1: pack_lo=1. Forget after bind may drop 0, so bind first.
        assert_eq!(p.fk(&parent), Some(Fk(1)), "bind height-1 child");
        p.forget_ready(&fence, 1);
        // After leftover of height 1, height 0 may drop.
        // Stamping height 2: pack_lo=2 drops 0.
        p.forget_ready(&fence, 2);
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
        p.forget_ready(&fence, 99);
        assert_eq!(
            p.fk(&parent),
            Some(Fk(1)),
            "no DrainDone ⇒ keep even if fence covers"
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
    fn inbox_ingest_note_then_bind() {
        let inbox = LoadInbox::new();
        let txid = [0xDDu8; 32];
        inbox.send(LoadInboxMsg::Note {
            height: 3,
            entries: vec![(txid, Fk(9))],
        });
        let effects = inbox.ingest().unwrap();
        assert!(effects.is_empty());
        inbox.with_pending_mut(|p| {
            assert_eq!(p.fk(&txid), Some(Fk(9)));
        });
    }
}
