//! Height-FIFO identity ring: just-confirmed `txid → (create_fk, body_range)`.
//!
//! Write publishes after Class A + idx. Load stamp probes this **after**
//! published live-union and **before** leftover TipOnly. Outs are not stored
//! (not a process pin FIFO).
//!
//! Expire is `pop_front` of whole heights. Horizon is
//! [`recent_creates_horizon`] (`2 * soft_win`, floor 256) so the ring outlives
//! BQ / lookup lead (1× the soft 1-min window is not enough).
//!
//! Writers mutate the locked map and mark dirty. [`RecentCreates::publish_if_dirty`]
//! is the only full-map snapshot rebuild (confirm write: once per batch). Load
//! [`RecentCreates::get`] / [`RecentSnap::get`] see a small dirty overlay first
//! so stamp hits unflushed notes without cloning the live map.

use crate::published_ids::TxidHasher;
use arc_swap::ArcSwap;
use rbitcoin_primitives::Fk;
use std::collections::{HashMap, HashSet, VecDeque};
use std::hash::BuildHasherDefault;
use std::sync::{Arc, Mutex};

/// Floor so a cold / tiny `soft_win` still covers a short lookup lead.
pub const RECENT_CREATES_HORIZON_FLOOR: u32 = 256;

/// Heights to retain: twice the 1-min confirm window, at least
/// [`RECENT_CREATES_HORIZON_FLOOR`].
#[inline]
pub fn recent_creates_horizon(soft_win: u32) -> u32 {
    soft_win.saturating_mul(2).max(RECENT_CREATES_HORIZON_FLOOR)
}

type LiveMap = HashMap<[u8; 32], LiveEnt, BuildHasherDefault<TxidHasher>>;
type DeadSet = HashSet<[u8; 32], BuildHasherDefault<TxidHasher>>;

#[derive(Clone, Copy)]
struct LiveEnt {
    fk: Fk,
    range: (u64, u64),
    height: u32,
}

struct Inner {
    live: LiveMap,
    overlay: LiveMap,
    dead: DeadSet,
    fifo: VecDeque<(u32, Vec<[u8; 32]>)>,
    dirty: bool,
}

/// Published live map plus the unflushed overlay for one stamp pack.
#[derive(Clone)]
pub struct RecentSnap {
    published: std::sync::Arc<LiveMap>,
    overlay: std::sync::Arc<LiveMap>,
    dead: std::sync::Arc<DeadSet>,
}

impl RecentSnap {
    pub fn get(&self, txid: &[u8; 32]) -> Option<(Fk, (u64, u64))> {
        if *txid == [0u8; 32] {
            return None;
        }
        if self.dead.contains(txid) {
            return None;
        }
        if let Some(e) = self.overlay.get(txid) {
            return Some((e.fk, e.range));
        }
        self.published.get(txid).map(|e| (e.fk, e.range))
    }
}

/// Write-published, load-read identity ring.
pub struct RecentCreates {
    live: ArcSwap<LiveMap>,
    inner: Mutex<Inner>,
}

impl Default for RecentCreates {
    fn default() -> Self {
        Self {
            live: ArcSwap::from_pointee(LiveMap::default()),
            inner: Mutex::new(Inner {
                live: LiveMap::default(),
                overlay: LiveMap::default(),
                dead: DeadSet::default(),
                fifo: VecDeque::new(),
                dirty: false,
            }),
        }
    }
}

impl RecentCreates {
    pub fn new() -> Self {
        Self::default()
    }

    fn publish(live: &ArcSwap<LiveMap>, g: &Inner) {
        live.store(Arc::new(g.live.clone()));
    }

    /// Rebuild the load snapshot if [`Self::note`] / expire / drop dirtied the map.
    ///
    /// No-op when clean. Confirm write flushes once after all height notes + expire.
    pub fn publish_if_dirty(&self) {
        let mut g = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        if !g.dirty {
            return;
        }
        Self::publish(&self.live, &g);
        g.overlay.clear();
        g.dead.clear();
        g.dirty = false;
    }

    /// Insert creates at `height`. Last write wins if the txid is already live.
    ///
    /// Does not rebuild the snapshot — call [`Self::publish_if_dirty`].
    pub fn note(&self, height: u32, rows: impl IntoIterator<Item = ([u8; 32], Fk, (u64, u64))>) {
        let mut keys: Vec<[u8; 32]> = Vec::new();
        let mut g = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        for (txid, fk, range) in rows {
            if txid == [0u8; 32] {
                continue;
            }
            let ent = LiveEnt { fk, range, height };
            g.live.insert(txid, ent);
            g.overlay.insert(txid, ent);
            g.dead.remove(&txid);
            keys.push(txid);
        }
        if keys.is_empty() {
            return;
        }
        g.fifo.push_back((height, keys));
        g.dirty = true;
    }

    /// Drop heights `≤ through` (inclusive). A key stays if a newer height
    /// re-noted it (last-write `LiveEnt.height`).
    pub fn expire_through(&self, through: u32) {
        let mut g = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        let mut changed = false;
        while let Some(&(h, _)) = g.fifo.front() {
            if h > through {
                break;
            }
            let (_, keys) = g.fifo.pop_front().expect("front");
            for t in keys {
                if let Some(ent) = g.live.get(&t) {
                    if ent.height <= through {
                        g.live.remove(&t);
                        g.overlay.remove(&t);
                        g.dead.insert(t);
                        changed = true;
                    }
                }
            }
        }
        if changed {
            g.dirty = true;
        }
    }

    /// Forget heights `≤ tip − horizon`. No-op while `tip < horizon` so a
    /// genesis-height note is not dropped on the first packs.
    pub fn expire_to_horizon(&self, tip: u32, horizon: u32) {
        if tip < horizon {
            return;
        }
        self.expire_through(tip - horizon);
    }

    /// Disconnect: drop heights `≥ height`.
    pub fn drop_from(&self, height: u32) {
        let mut g = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        let before = g.live.len();
        g.fifo.retain(|(h, _)| *h < height);
        let mut dropped: Vec<[u8; 32]> = Vec::new();
        g.live.retain(|t, ent| {
            if ent.height < height {
                true
            } else {
                dropped.push(*t);
                false
            }
        });
        for t in dropped {
            g.overlay.remove(&t);
            g.dead.insert(t);
        }
        if g.live.len() != before {
            g.dirty = true;
        }
    }

    /// Point get. Zero txid is never a hit. Lock-free snapshot load.
    pub fn get(&self, txid: &[u8; 32]) -> Option<(Fk, (u64, u64))> {
        self.snapshot().get(txid)
    }

    /// Published Arc plus a small overlay (do not `load` per parent).
    pub fn snapshot(&self) -> RecentSnap {
        let g = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        RecentSnap {
            published: self.live.load_full(),
            overlay: Arc::new(g.overlay.clone()),
            dead: Arc::new(g.dead.clone()),
        }
    }

    /// Occupancy for `ibd: sizes`.
    pub fn size_snapshot(&self) -> (usize, usize) {
        let g = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        (g.fifo.len(), g.live.len())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tid(b: u8) -> [u8; 32] {
        let mut t = [0u8; 32];
        t[0] = b;
        t
    }

    #[test]
    fn recent_creates_horizon_is_2x_soft_win_with_floor() {
        assert_eq!(recent_creates_horizon(0), RECENT_CREATES_HORIZON_FLOOR);
        assert_eq!(recent_creates_horizon(100), RECENT_CREATES_HORIZON_FLOOR);
        assert_eq!(recent_creates_horizon(200), 400);
        assert_eq!(recent_creates_horizon(400), 800);
    }

    #[test]
    fn two_height_notes_are_two_fifo_rows() {
        let r = RecentCreates::new();
        r.note(10, [(tid(1), Fk(1), (1, 2))]);
        r.note(11, [(tid(2), Fk(2), (3, 4))]);
        assert_eq!(
            r.size_snapshot(),
            (2, 2),
            "one fifo row per prepared height, not one per write batch"
        );
    }

    #[test]
    fn two_notes_without_flush_do_not_rebuild_snapshot() {
        let r = RecentCreates::new();
        let before = r.snapshot();
        r.note(10, [(tid(1), Fk(1), (1, 2))]);
        r.note(11, [(tid(2), Fk(2), (3, 4))]);
        let mid = r.snapshot();
        assert!(
            std::sync::Arc::ptr_eq(&before.published, &mid.published),
            "note must not clone the live map; publish_if_dirty is the snapshot rebuild"
        );
        assert_eq!(
            r.get(&tid(1)),
            Some((Fk(1), (1, 2))),
            "dirty overlay must serve get before publish_if_dirty"
        );
        assert_eq!(mid.get(&tid(2)), Some((Fk(2), (3, 4))));
        assert_eq!(r.size_snapshot(), (2, 2));
        r.publish_if_dirty();
        assert_eq!(r.get(&tid(1)), Some((Fk(1), (1, 2))));
        assert_eq!(r.get(&tid(2)), Some((Fk(2), (3, 4))));
        let after = r.snapshot();
        assert!(!std::sync::Arc::ptr_eq(&before.published, &after.published));
        r.publish_if_dirty();
        assert!(
            std::sync::Arc::ptr_eq(&after.published, &r.snapshot().published),
            "second publish_if_dirty is a no-op when clean"
        );
    }

    #[test]
    fn note_makes_get_visible() {
        let r = RecentCreates::new();
        assert!(r.get(&tid(1)).is_none());
        r.note(10, [(tid(1), Fk(7), (100, 8))]);
        assert_eq!(r.get(&tid(1)), Some((Fk(7), (100, 8))));
        assert!(r.get(&tid(2)).is_none());
    }

    #[test]
    fn zero_txid_is_never_a_hit() {
        let r = RecentCreates::new();
        r.note(1, [([0u8; 32], Fk(1), (0, 1))]);
        assert!(r.get(&[0u8; 32]).is_none());
        assert_eq!(r.size_snapshot(), (0, 0));
    }

    #[test]
    fn expire_through_drops_old_keeps_newer() {
        let r = RecentCreates::new();
        r.note(10, [(tid(1), Fk(1), (1, 2)), (tid(2), Fk(2), (3, 4))]);
        r.note(11, [(tid(2), Fk(2), (3, 4)), (tid(3), Fk(3), (5, 6))]);
        r.expire_through(10);
        assert!(r.get(&tid(1)).is_none(), "height-10-only key must drop");
        assert_eq!(
            r.get(&tid(2)),
            Some((Fk(2), (3, 4))),
            "re-noted at 11 must survive expire of 10"
        );
        assert_eq!(r.get(&tid(3)), Some((Fk(3), (5, 6))));
        let (heights, keys) = r.size_snapshot();
        assert_eq!(heights, 1);
        assert_eq!(keys, 2);
    }

    #[test]
    fn expire_to_horizon_keeps_until_tip_covers_window() {
        let r = RecentCreates::new();
        r.note(0, [(tid(1), Fk(1), (1, 2))]);
        r.publish_if_dirty();
        r.expire_to_horizon(100, RECENT_CREATES_HORIZON_FLOOR);
        assert_eq!(
            r.get(&tid(1)),
            Some((Fk(1), (1, 2))),
            "tip below horizon must not drop genesis-height notes"
        );
        r.expire_to_horizon(RECENT_CREATES_HORIZON_FLOOR, RECENT_CREATES_HORIZON_FLOOR);
        r.publish_if_dirty();
        assert!(r.get(&tid(1)).is_none());
    }

    #[test]
    fn drop_from_removes_disconnect_height_and_above() {
        let r = RecentCreates::new();
        r.note(10, [(tid(1), Fk(1), (1, 2))]);
        r.note(12, [(tid(2), Fk(2), (3, 4))]);
        r.drop_from(12);
        assert_eq!(r.get(&tid(1)), Some((Fk(1), (1, 2))));
        assert!(r.get(&tid(2)).is_none());
    }
}
