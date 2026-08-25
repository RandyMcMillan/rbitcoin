//! Write-published identity + optional [`CreatePin`] as an Arc layer chain.
//!
//! Same splice/prepend as live_union ([`crate::layer_chain`]). One layer per
//! [`RecentCreates::publish_layer`]. Load snapshot walks pending then the head.
//! [`RecentSnap::get`] is fk+range; [`RecentSnap::create_pin`] clones the pin Arc.

use crate::layer_chain::{self, ChainLayer};
use crate::published_ids::TxidHasher;
use crate::CreatePin;
use arc_swap::ArcSwapOption;
use rbitcoin_primitives::Fk;
use std::collections::HashMap;
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
type RecentLayer = ChainLayer<u32, LiveMap>;

#[derive(Clone)]
struct LiveEnt {
    fk: Fk,
    range: (u64, u64),
    height: u32,
    outs: Option<CreatePin>,
}

struct Inner {
    pending: LiveMap,
}

/// Pending notes plus the published layer head.
#[derive(Clone)]
pub struct RecentSnap {
    head: Option<Arc<RecentLayer>>,
    pending: std::sync::Arc<LiveMap>,
}

impl RecentSnap {
    fn ent(&self, txid: &[u8; 32]) -> Option<(Fk, (u64, u64), Option<CreatePin>)> {
        if *txid == [0u8; 32] {
            return None;
        }
        if let Some(e) = self.pending.get(txid) {
            return Some((e.fk, e.range, e.outs.clone()));
        }
        self.head.as_ref()?.walk(|layer| {
            layer
                .hits
                .get(txid)
                .map(|e| (e.fk, e.range, e.outs.clone()))
        })
    }

    pub fn get(&self, txid: &[u8; 32]) -> Option<(Fk, (u64, u64))> {
        self.ent(txid).map(|(fk, range, _)| (fk, range))
    }

    /// Arc-clone the create pin when the note carried one. Identity-only notes
    /// return `None` (load pin then cold-fills).
    pub fn create_pin(&self, txid: &[u8; 32]) -> Option<CreatePin> {
        self.ent(txid).and_then(|(_, _, outs)| outs)
    }
}

/// Write-published, load-read identity ring.
pub struct RecentCreates {
    head: ArcSwapOption<RecentLayer>,
    inner: Mutex<Inner>,
}

impl Default for RecentCreates {
    fn default() -> Self {
        Self {
            head: ArcSwapOption::empty(),
            inner: Mutex::new(Inner {
                pending: LiveMap::default(),
            }),
        }
    }
}

impl RecentCreates {
    pub fn new() -> Self {
        Self::default()
    }

    /// Freeze pending into one layer and prepend. No-op when pending is empty.
    /// Does not clone older `hits` Arcs.
    pub fn publish_layer(&self, until: u32) {
        let mut g = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        if g.pending.is_empty() {
            return;
        }
        let hits = std::mem::take(&mut g.pending);
        let mut lo = u32::MAX;
        let mut hi = 0u32;
        for e in hits.values() {
            lo = lo.min(e.height);
            hi = hi.max(e.height);
        }
        drop(g);
        let older = self.head.load_full();
        self.head.store(Some(ChainLayer::prepend(
            older,
            lo,
            hi,
            until,
            Arc::new(hits),
        )));
    }

    /// Merge pending into a layer that is never drop_ready until Class A HWMs exist.
    pub fn publish_if_dirty(&self) {
        self.publish_layer(u32::MAX);
    }

    /// Drop layers with `until <= class_a_hi`. Kept nodes reuse `hits` Arc.
    pub fn drop_ready(&self, class_a_hi: u32) {
        let head = self.head.load_full();
        let next = layer_chain::splice_kept(head, |l| l.meta > class_a_hi);
        self.head.store(next);
    }

    pub fn note(&self, height: u32, rows: impl IntoIterator<Item = ([u8; 32], Fk, (u64, u64))>) {
        self.note_pins(height, rows.into_iter().map(|(t, f, r)| (t, f, r, None)));
    }

    pub fn note_pins(
        &self,
        height: u32,
        rows: impl IntoIterator<Item = ([u8; 32], Fk, (u64, u64), Option<CreatePin>)>,
    ) {
        let mut g = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        for (txid, fk, range, outs) in rows {
            if txid == [0u8; 32] {
                continue;
            }
            g.pending.insert(
                txid,
                LiveEnt {
                    fk,
                    range,
                    height,
                    outs,
                },
            );
        }
    }

    /// Drop published keys / pending with `LiveEnt.height ≤ through`.
    pub fn expire_through(&self, through: u32) {
        self.filter_pending(|e| e.height > through);
        self.filter_layers(|l| l.hi > through, |e| e.height > through);
    }

    pub fn expire_to_horizon(&self, tip: u32, horizon: u32) {
        if tip < horizon {
            return;
        }
        self.expire_through(tip - horizon);
    }

    /// Disconnect: drop heights `≥ height`.
    pub fn drop_from(&self, height: u32) {
        self.filter_pending(|e| e.height < height);
        self.filter_layers(|l| l.lo < height, |e| e.height < height);
    }

    fn filter_pending(&self, keep: impl Fn(&LiveEnt) -> bool) {
        let mut g = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        g.pending.retain(|_, e| keep(e));
    }

    fn filter_layers(
        &self,
        keep_layer: impl Fn(&RecentLayer) -> bool,
        keep_ent: impl Fn(&LiveEnt) -> bool,
    ) {
        let head = self.head.load_full();
        self.head.store(filter_chain(head, &keep_layer, &keep_ent));
    }

    pub fn get(&self, txid: &[u8; 32]) -> Option<(Fk, (u64, u64))> {
        self.snapshot().get(txid)
    }

    pub fn create_pin(&self, txid: &[u8; 32]) -> Option<CreatePin> {
        self.snapshot().create_pin(txid)
    }

    pub fn snapshot(&self) -> RecentSnap {
        let g = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        RecentSnap {
            head: self.head.load_full(),
            pending: Arc::new(g.pending.clone()),
        }
    }

    fn head_hits_arc(&self) -> Option<Arc<LiveMap>> {
        self.head.load_full().map(|h| Arc::clone(&h.hits))
    }

    /// Occupancy for `ibd: sizes`.
    pub fn size_snapshot(&self) -> (usize, usize) {
        let d = self.size_detail();
        (d.0, d.1)
    }

    /// `(layers, live_keys, pub_keys, pending_keys, live_keys)`.
    pub fn size_detail(&self) -> (usize, usize, usize, usize, usize) {
        let g = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        let pending = g.pending.len();
        drop(g);
        let mut layers = 0usize;
        let mut pub_k = 0usize;
        let mut cur = self.head.load_full();
        while let Some(layer) = cur {
            layers = layers.saturating_add(1);
            pub_k = pub_k.saturating_add(layer.hits.len());
            cur = layer.older.clone();
        }
        let live = pub_k.saturating_add(pending);
        (layers, live, pub_k, pending, live)
    }

    pub fn approx_pin_bytes(&self) -> u64 {
        let g = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        let mut n = 0u64;
        for e in g.pending.values() {
            if let Some(p) = &e.outs {
                n = n.saturating_add(crate::archive::create_pin_approx_bytes(p) as u64);
            }
        }
        drop(g);
        let mut cur = self.head.load_full();
        while let Some(layer) = cur {
            for e in layer.hits.values() {
                if let Some(p) = &e.outs {
                    n = n.saturating_add(crate::archive::create_pin_approx_bytes(p) as u64);
                }
            }
            cur = layer.older.clone();
        }
        n
    }
}

fn layer_ents_all(l: &RecentLayer, keep: impl Fn(&LiveEnt) -> bool) -> bool {
    l.hits.values().all(keep)
}

fn layer_ents_any(l: &RecentLayer, keep: impl Fn(&LiveEnt) -> bool) -> bool {
    l.hits.values().any(keep)
}

fn filter_chain(
    head: Option<Arc<RecentLayer>>,
    keep_layer: &impl Fn(&RecentLayer) -> bool,
    keep_ent: &impl Fn(&LiveEnt) -> bool,
) -> Option<Arc<RecentLayer>> {
    let mut nodes = Vec::new();
    let mut cur = head;
    while let Some(n) = cur {
        let older = n.older.clone();
        nodes.push(n);
        cur = older;
    }
    let mut new_head: Option<Arc<RecentLayer>> = None;
    for n in nodes.into_iter().rev() {
        if keep_layer(&n) && layer_ents_all(&n, keep_ent) {
            new_head = Some(ChainLayer::prepend(
                new_head,
                n.lo,
                n.hi,
                n.meta,
                Arc::clone(&n.hits),
            ));
            continue;
        }
        if !layer_ents_any(&n, keep_ent) {
            continue;
        }
        let mut hits = LiveMap::default();
        let mut lo = u32::MAX;
        let mut hi = 0u32;
        for (k, e) in n.hits.iter() {
            if keep_ent(e) {
                lo = lo.min(e.height);
                hi = hi.max(e.height);
                hits.insert(*k, e.clone());
            }
        }
        if hits.is_empty() {
            continue;
        }
        new_head = Some(ChainLayer::prepend(
            new_head,
            lo,
            hi,
            n.meta,
            Arc::new(hits),
        ));
    }
    new_head
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
    fn recent_layer_publish_prepends_without_cloning_older_hits() {
        let r = RecentCreates::new();
        r.note(10, [(tid(1), Fk(1), (1, 2))]);
        r.publish_layer(100);
        let first = r.head_hits_arc().expect("head");
        r.note(11, [(tid(2), Fk(2), (3, 4))]);
        r.publish_layer(101);
        let older = r
            .head
            .load_full()
            .expect("head")
            .older
            .clone()
            .expect("older");
        assert!(
            Arc::ptr_eq(&older.hits, &first),
            "second publish must prepend, not clone the older hits Arc"
        );
        assert_eq!(r.get(&tid(1)), Some((Fk(1), (1, 2))));
        assert_eq!(r.get(&tid(2)), Some((Fk(2), (3, 4))));
    }

    #[test]
    fn recent_layer_drop_ready_splices_older_keeps_newer_hits_arc() {
        let r = RecentCreates::new();
        r.note(10, [(tid(1), Fk(1), (1, 2))]);
        r.publish_layer(10);
        r.note(11, [(tid(2), Fk(2), (3, 4))]);
        r.publish_layer(40);
        let newer = r.head_hits_arc().expect("newer");
        r.drop_ready(10);
        assert!(r.get(&tid(1)).is_none(), "until=10 must drop at class_a 10");
        assert_eq!(r.get(&tid(2)), Some((Fk(2), (3, 4))));
        assert!(
            Arc::ptr_eq(&r.head_hits_arc().expect("kept"), &newer),
            "kept layer hits Arc must not be cloned on splice"
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
            match (&before.head, &mid.head) {
                (None, None) => true,
                (Some(a), Some(b)) => Arc::ptr_eq(a, b),
                _ => false,
            },
            "note must not publish a layer; publish_layer is the prepend"
        );
        assert_eq!(
            r.get(&tid(1)),
            Some((Fk(1), (1, 2))),
            "pending must serve get before publish_layer"
        );
        assert_eq!(mid.get(&tid(2)), Some((Fk(2), (3, 4))));
        r.publish_if_dirty();
        assert_eq!(r.get(&tid(1)), Some((Fk(1), (1, 2))));
        assert_eq!(r.get(&tid(2)), Some((Fk(2), (3, 4))));
        let after = r.snapshot();
        assert!(after.head.is_some());
        r.publish_if_dirty();
        assert!(
            Arc::ptr_eq(
                after.head.as_ref().unwrap(),
                r.snapshot().head.as_ref().unwrap()
            ),
            "second publish_if_dirty is a no-op when pending is empty"
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
        let (_layers, keys) = r.size_snapshot();
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
            Arc::ptr_eq(&r.create_pin(&tid(1)).expect("pending pin"), &pin),
            "pending create_pin hits the same Arc"
        );
        r.publish_if_dirty();
        let flushed = r.create_pin(&tid(1)).expect("published pin");
        assert!(
            Arc::ptr_eq(&flushed, &pin),
            "publish prepends an Arc of the map, not script bytes"
        );
        assert_eq!(r.get(&tid(1)), Some((Fk(7), (100, 8))));
        r.expire_through(10);
        r.publish_if_dirty();
        assert!(r.get(&tid(1)).is_none());
        assert!(r.create_pin(&tid(1)).is_none());
    }

    #[test]
    fn recent_size_counts_pin_bytes() {
        let r = RecentCreates::new();
        let script = vec![0x51; 400];
        let pin = dummy_pin(script.clone());
        r.note_pins(10, [(tid(1), Fk(7), (100, 8), Some(pin))]);
        let n = r.approx_pin_bytes();
        assert!(
            n >= script.len() as u64,
            "size must count pin script bytes, not 96 B/key; got {n}"
        );
        assert!(n > 96, "96 B/key identity estimate must not be the payload");
    }
}
