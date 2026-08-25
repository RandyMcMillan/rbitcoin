//! Height-FIFO identity ring: just-confirmed `txid → (create_fk, body_range)`
//! plus optional [`CreatePin`] outs (Arc-shared with in-flight / `batch_pin`).
//!
//! Write publishes after Class A + idx. Load stamp probes this **after**
//! published live-union and **before** leftover TipOnly. [`RecentSnap::get`]
//! stays `(fk, range)` and does **not** clone the pin Arc. Load pin uses
//! [`RecentSnap::create_pin`].
//!
//! Expire is `pop_front` of whole heights. Horizon is
//! [`recent_creates_horizon`] on an EWMA of `lookup_taken_hi − tip`
//! (floor 32, cap `32*144`).
//!
//! Overlay + tombstones hold unflushed notes. [`RecentCreates::publish_if_dirty`]
//! builds **one** published [`Arc`] (confirm write: once per batch). There is no
//! second live HashMap. Load [`get`](RecentCreates::get) / [`RecentSnap::get`]
//! see overlay first so stamp hits unflushed notes without cloning.

use crate::published_ids::TxidHasher;
use crate::CreatePin;
use arc_swap::ArcSwap;
use rbitcoin_primitives::Fk;
use std::collections::{HashMap, HashSet, VecDeque};
use std::hash::BuildHasherDefault;
use std::sync::{Arc, Mutex};

/// Floor so a cold / empty lead still covers one pipeline of 1-high batches.
pub const RECENT_CREATES_HORIZON_FLOOR: u32 = 32;
/// Cap: 32 load-sized batches × 144-block hard pack.
pub const RECENT_CREATES_HORIZON_CAP: u32 = 32 * 144;

/// One EWMA step: `(3·ewma + span) / 4`. Cold `ewma == 0` starts at `span`.
#[inline]
pub fn recent_creates_ewma_step(ewma: u32, span: u32) -> u32 {
    if ewma == 0 {
        span
    } else {
        ewma.saturating_mul(3).saturating_add(span) / 4
    }
}

/// Heights to retain: EWMA lead, clamped to floor/cap.
#[inline]
pub fn recent_creates_horizon(ewma_lead: u32) -> u32 {
    ewma_lead.clamp(RECENT_CREATES_HORIZON_FLOOR, RECENT_CREATES_HORIZON_CAP)
}

type LiveMap = HashMap<[u8; 32], LiveEnt, BuildHasherDefault<TxidHasher>>;
type DeadSet = HashSet<[u8; 32], BuildHasherDefault<TxidHasher>>;

#[derive(Clone)]
struct LiveEnt {
    fk: Fk,
    range: (u64, u64),
    height: u32,
    outs: Option<CreatePin>,
}

struct Inner {
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

    /// Arc-clone the create pin when the note carried one. Identity-only notes
    /// return `None` (load pin then cold-fills). Does not run on [`Self::get`].
    pub fn create_pin(&self, txid: &[u8; 32]) -> Option<CreatePin> {
        if *txid == [0u8; 32] {
            return None;
        }
        if self.dead.contains(txid) {
            return None;
        }
        if let Some(e) = self.overlay.get(txid) {
            return e.outs.clone();
        }
        self.published.get(txid).and_then(|e| e.outs.clone())
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

    /// Merge overlay/dead into one new published Arc. No second live HashMap.
    ///
    /// No-op when clean. Confirm write flushes once after all height notes + expire.
    pub fn publish_if_dirty(&self) {
        let mut g = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        if !g.dirty {
            return;
        }
        let published = self.live.load_full();
        let mut next = (*published).clone();
        for (k, e) in g.overlay.iter() {
            next.insert(*k, e.clone());
        }
        for k in g.dead.iter() {
            next.remove(k);
        }
        self.live.store(Arc::new(next));
        g.overlay.clear();
        g.dead.clear();
        g.dirty = false;
    }

    /// Insert creates at `height`. Last write wins if the txid is already live.
    ///
    /// Does not rebuild the snapshot — call [`Self::publish_if_dirty`].
    /// Identity-only (no outs). [`Self::note_pins`] carries a [`CreatePin`].
    pub fn note(&self, height: u32, rows: impl IntoIterator<Item = ([u8; 32], Fk, (u64, u64))>) {
        self.note_pins(height, rows.into_iter().map(|(t, f, r)| (t, f, r, None)));
    }

    /// Same as [`Self::note`] with optional [`CreatePin`] (Arc clone, not script bytes).
    pub fn note_pins(
        &self,
        height: u32,
        rows: impl IntoIterator<Item = ([u8; 32], Fk, (u64, u64), Option<CreatePin>)>,
    ) {
        let mut keys: Vec<[u8; 32]> = Vec::new();
        let mut g = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        for (txid, fk, range, outs) in rows {
            if txid == [0u8; 32] {
                continue;
            }
            let ent = LiveEnt {
                fk,
                range,
                height,
                outs,
            };
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
        let published = self.live.load_full();
        let mut g = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        let mut changed = false;
        while let Some(&(h, _)) = g.fifo.front() {
            if h > through {
                break;
            }
            let (_, keys) = g.fifo.pop_front().expect("front");
            for t in keys {
                if let Some(ent) = g.overlay.get(&t) {
                    if ent.height <= through {
                        g.overlay.remove(&t);
                        g.dead.insert(t);
                        changed = true;
                    }
                    continue;
                }
                if let Some(ent) = published.get(&t) {
                    if ent.height <= through {
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
        let published = self.live.load_full();
        let mut g = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        g.fifo.retain(|(h, _)| *h < height);
        let mut changed = false;
        let mut drop_ov: Vec<[u8; 32]> = Vec::new();
        g.overlay.retain(|t, ent| {
            if ent.height < height {
                true
            } else {
                drop_ov.push(*t);
                false
            }
        });
        for t in drop_ov {
            g.dead.insert(t);
            changed = true;
        }
        for (t, ent) in published.iter() {
            if ent.height >= height {
                g.dead.insert(*t);
                changed = true;
            }
        }
        if changed {
            g.dirty = true;
        }
    }

    /// Point get. Zero txid is never a hit. Lock-free snapshot load.
    pub fn get(&self, txid: &[u8; 32]) -> Option<(Fk, (u64, u64))> {
        self.snapshot().get(txid)
    }

    /// Snapshot then [`RecentSnap::create_pin`].
    pub fn create_pin(&self, txid: &[u8; 32]) -> Option<CreatePin> {
        self.snapshot().create_pin(txid)
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
        let d = self.size_detail();
        (d.0, d.1)
    }

    /// `(heights, live, pub, overlay, fifo_keys)`.
    pub fn size_detail(&self) -> (usize, usize, usize, usize, usize) {
        let g = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        let fifo_keys = g.fifo.iter().map(|(_, k)| k.len()).sum();
        let pub_k = self.live.load().len();
        (
            g.fifo.len(),
            pub_k.saturating_add(g.overlay.len()),
            pub_k,
            g.overlay.len(),
            fifo_keys,
        )
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
    fn horizon_is_ewma_clamped_not_plus_quarter() {
        assert_eq!(recent_creates_ewma_step(0, 40), 40);
        assert_eq!(recent_creates_ewma_step(40, 40), 40);
        assert_eq!(recent_creates_horizon(0), RECENT_CREATES_HORIZON_FLOOR);
        assert_eq!(recent_creates_horizon(40), 40);
        assert_eq!(recent_creates_horizon(10_000), RECENT_CREATES_HORIZON_CAP);
        let mut e = 0u32;
        for _ in 0..8 {
            e = recent_creates_ewma_step(e, 40);
        }
        let h = recent_creates_horizon(e);
        assert_eq!(h, 40, "settled horizon={h}");
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
    fn size_detail_counts_live_and_pub_separately() {
        let r = RecentCreates::new();
        r.note(10, [(tid(1), Fk(1), (1, 2))]);
        let (h, live, pub_k, ov, fifo) = r.size_detail();
        assert_eq!(h, 1);
        assert_eq!(live, 1);
        assert_eq!(pub_k, 0, "unpublished notes are not on the ArcSwap");
        assert_eq!(ov, 1);
        assert_eq!(fifo, 1);
        r.publish_if_dirty();
        let (_, live, pub_k, ov, fifo) = r.size_detail();
        assert_eq!(live, 1);
        assert_eq!(pub_k, 1);
        assert_eq!(ov, 0);
        assert_eq!(fifo, 1);
        let a = r.snapshot();
        let b = r.snapshot();
        assert!(
            std::sync::Arc::ptr_eq(&a.published, &b.published),
            "flush must not keep a second live HashMap; snapshots share the Arc"
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
        r.expire_to_horizon(16, RECENT_CREATES_HORIZON_FLOOR);
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

    fn dummy_pin(script: Vec<u8>) -> CreatePin {
        Arc::new((
            rbitcoin_store::TxRecord {
                txid: tid(1),
                version: 1,
                locktime: 0,
                input_start_fk: Fk::NULL,
                input_count: 0,
                output_start_fk: Fk::NULL,
                output_count: 1,
            },
            vec![rbitcoin_store::OutputRecord::unspent(1, script)],
        ))
    }

    #[test]
    fn recent_snap_get_does_not_need_outs() {
        let r = RecentCreates::new();
        r.note(10, [(tid(1), Fk(7), (100, 8))]);
        let snap = r.snapshot();
        assert_eq!(snap.get(&tid(1)), Some((Fk(7), (100, 8))));
        assert!(
            snap.create_pin(&tid(1)).is_none(),
            "identity note must not clone a pin Arc on get; create_pin is None"
        );
        assert!(r.create_pin(&tid(1)).is_none());
    }

    #[test]
    fn recent_create_pin_survives_flush_and_expires() {
        let r = RecentCreates::new();
        let pin = dummy_pin(vec![0x51, 0xaa, 0xbb]);
        r.note_pins(10, [(tid(1), Fk(7), (100, 8), Some(Arc::clone(&pin)))]);
        assert!(
            Arc::ptr_eq(&r.create_pin(&tid(1)).expect("overlay pin"), &pin),
            "two notes without flush: overlay create_pin hits the same Arc"
        );
        r.publish_if_dirty();
        let flushed = r.create_pin(&tid(1)).expect("published pin");
        assert!(
            Arc::ptr_eq(&flushed, &pin),
            "flush clones the HashMap of Arcs, not script bytes"
        );
        assert_eq!(r.get(&tid(1)), Some((Fk(7), (100, 8))));
        r.expire_through(10);
        r.publish_if_dirty();
        assert!(r.get(&tid(1)).is_none());
        assert!(r.create_pin(&tid(1)).is_none());
    }
}
