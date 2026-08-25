//! Published parent identity (`txid → fk + range`) as a wave-layered chain.
//!
//! Lookup prepends one [`IdLayer`] per resolve wave (a span of BQ heights)
//! and [`publish`](LiveUnion::publish) stores the chain head (`Arc` bump).
//! [`LiveUnion::get`] / [`partition_into_layer`](LiveUnion::partition_into_layer) and load
//! [`get`](PublishedIds::get) walk newest → older. A layer stays until
//! **no** height in its span is still on the body queue; drop is splice
//! only (no union rebuild). [`unpublish`](PublishedIds::unpublish) (store
//! `None`) drops visibility for new readers; a reader holding the old
//! `Arc` still sees hits.

use crate::layer_chain::{self, ChainLayer};
use arc_swap::ArcSwapOption;
use rbitcoin_primitives::Fk;
use std::collections::HashMap;
use std::hash::{BuildHasherDefault, Hasher};
use std::sync::Arc;

/// Identity hasher for `[u8; 32]` txids (already uniform). `finish()` is the
/// first 8 bytes; equality still compares the full key.
#[derive(Default, Clone, Copy)]
pub struct TxidHasher(u64);

impl Hasher for TxidHasher {
    #[inline]
    fn write(&mut self, bytes: &[u8]) {
        if bytes.len() >= 8 {
            let mut raw = [0u8; 8];
            raw.copy_from_slice(&bytes[..8]);
            self.0 = u64::from_le_bytes(raw);
        } else {
            self.0 = 0;
            for (i, &b) in bytes.iter().enumerate() {
                self.0 |= u64::from(b) << (8 * i);
            }
        }
    }

    #[inline]
    fn finish(&self) -> u64 {
        self.0
    }
}

/// Mixes every `write` so `(txid, vout)` keys stay distinct.
///
/// [`TxidHasher`] overwrites `self.0` per write — using it on an outpoint
/// would drop the txid hash when the vout bytes arrive.
#[derive(Default, Clone, Copy)]
pub struct OutPointHasher(u64);

impl Hasher for OutPointHasher {
    #[inline]
    fn write(&mut self, bytes: &[u8]) {
        for &b in bytes {
            self.0 ^= u64::from(b);
            self.0 = self.0.wrapping_mul(0x0100_0000_01b3);
        }
    }

    #[inline]
    fn finish(&self) -> u64 {
        self.0
    }
}

/// `(txid, vout)` set that folds every hasher write (see [`OutPointHasher`]).
pub type OutPointSet =
    std::collections::HashSet<([u8; 32], u32), BuildHasherDefault<OutPointHasher>>;

/// Immutable `txid → (create_fk, body_range)` for one resolve wave.
pub type IdMap = HashMap<[u8; 32], (Fk, (u64, u64)), BuildHasherDefault<TxidHasher>>;

/// One lookup wave's hits (`lo..=hi` BQ heights) plus the older chain.
pub type IdLayer = ChainLayer<(), IdMap>;

impl IdLayer {
    /// Newest-first walk. First layer that has `txid` wins.
    pub fn get(&self, txid: &[u8; 32]) -> Option<(Fk, (u64, u64))> {
        self.walk(|layer| layer.hits.get(txid).copied())
    }
}

/// Atomic published identity chain. Readers `load` an `Arc`; writers `store`.
#[derive(Debug)]
pub struct PublishedIds {
    inner: ArcSwapOption<IdLayer>,
}

impl Default for PublishedIds {
    fn default() -> Self {
        Self::new()
    }
}

impl PublishedIds {
    pub fn new() -> Self {
        Self {
            inner: ArcSwapOption::empty(),
        }
    }

    /// Replace the chain with a single layer (tests / one-shot stamp).
    pub fn publish(&self, map: Arc<IdMap>) {
        self.inner
            .store(Some(ChainLayer::prepend(None, 0, 0, (), map)));
    }

    pub(crate) fn publish_head(&self, head: Option<Arc<IdLayer>>) {
        self.inner.store(head);
    }

    /// New [`load`](Self::load) / [`get`](Self::get) miss. Held Arcs still work.
    pub fn unpublish(&self) {
        self.inner.store(None);
    }

    /// Chain head. `None` after [`unpublish`](Self::unpublish).
    pub fn load(&self) -> Option<Arc<IdLayer>> {
        self.inner.load_full()
    }

    /// Layer count and sum of hit-map lens (shared Arcs; O(layers)).
    pub fn size_snapshot(&self) -> (usize, usize) {
        let mut layers = 0usize;
        let mut keys = 0usize;
        let mut cur = self.load();
        while let Some(layer) = cur {
            layers = layers.saturating_add(1);
            keys = keys.saturating_add(layer.hits.len());
            cur = layer.older.clone();
        }
        (layers, keys)
    }

    /// Point get. Zero txid is never a hit.
    pub fn get(&self, txid: &[u8; 32]) -> Option<(Fk, (u64, u64))> {
        if *txid == [0u8; 32] {
            return None;
        }
        self.load()?.get(txid)
    }
}

/// Lookup-thread chain of still-queued height layers. Not shared with load.
#[derive(Debug, Default)]
pub struct LiveUnion {
    head: Option<Arc<IdLayer>>,
    next_wave: u32,
}

impl LiveUnion {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn get(&self, txid: &[u8; 32]) -> Option<(Fk, (u64, u64))> {
        if *txid == [0u8; 32] {
            return None;
        }
        self.head.as_deref()?.get(txid)
    }

    /// Re-home union hits into `layer` (this wave's map) and return TipOnly `need`.
    ///
    /// `skipped` is TipOnly avoided, not omitted from `layer`. Zero txid is
    /// never a hit and never appears in `need`.
    pub fn partition_into_layer<'a>(
        &self,
        keys: impl IntoIterator<Item = &'a [u8; 32]>,
        layer: &mut IdMap,
    ) -> (u32, Vec<[u8; 32]>) {
        let mut skipped = 0u32;
        let mut need = Vec::new();
        for t in keys {
            if *t == [0u8; 32] {
                continue;
            }
            match self.get(t) {
                Some(hit) => {
                    layer.insert(*t, hit);
                    skipped = skipped.saturating_add(1);
                }
                None => need.push(*t),
            }
        }
        (skipped, need)
    }

    /// Drop layers whose span has no remaining queued height. Does not swap.
    pub fn keep_heights(&mut self, keep: impl Fn(u32) -> bool) {
        self.head = layer_chain::splice_kept(self.head.take(), |l| span_kept(l.lo, l.hi, &keep));
    }

    /// Same as [`Self::keep_heights`] using a queued-height set (`range`, not `lo..=hi`).
    pub fn keep_queued_heights(&mut self, queued: &std::collections::BTreeSet<u32>) {
        self.head = splice_queued(self.head.take(), queued);
    }

    /// Keep layers that still overlap the BQ, overlap `(tip, taken_hi]`
    /// (taken onto loadq, already off BQ), **or** whose `hi` is inside
    /// `tip − horizon` (RecentCreates window). Identity only.
    pub fn keep_queued_or_horizon(
        &mut self,
        queued: &std::collections::BTreeSet<u32>,
        tip: u32,
        horizon: u32,
        taken_hi: Option<u32>,
    ) {
        self.head = splice_queued_or_horizon(self.head.take(), queued, tip, horizon, taken_hi);
    }

    /// Prepend one layer covering `lo..=hi` (inclusive).
    ///
    /// `hits` is this wave's parent identities (re-homed union hits + TipOnly
    /// misses). Moved into an Arc — no per-entry copy. Zero txid is stripped.
    pub fn note_span(&mut self, lo: u32, hi: u32, mut hits: IdMap) {
        let (lo, hi) = if lo <= hi { (lo, hi) } else { (hi, lo) };
        self.head = layer_chain::splice_kept(self.head.take(), |l| {
            span_kept(l.lo, l.hi, &|h| h < lo || h > hi)
        });
        hits.remove(&[0u8; 32]);
        if hits.is_empty() {
            return;
        }
        self.head = Some(ChainLayer::prepend(
            self.head.take(),
            lo,
            hi,
            (),
            Arc::new(hits),
        ));
    }

    /// Prepend a single-height layer (tests / one-shot).
    pub fn note_height(&mut self, height: u32, hits: IdMap) {
        self.note_span(height, height, hits);
    }

    /// Arc-bump the chain head. Call once after a wave's [`note_span`].
    pub fn publish(&self, published: &PublishedIds) {
        published.publish_head(self.head.clone());
    }

    /// Insert hits under a synthetic single-height wave, publish the chain head.
    pub fn finish_wave(&mut self, hits: IdMap, published: &PublishedIds) -> u32 {
        let height = self.next_wave;
        self.next_wave = self.next_wave.saturating_add(1);
        self.note_height(height, hits);
        self.publish(published);
        height
    }
}

fn span_kept(lo: u32, hi: u32, keep: &impl Fn(u32) -> bool) -> bool {
    (lo..=hi).any(keep)
}

/// True when any height in `queued` falls in `lo..=hi` (no 1080-wide walk).
pub fn span_overlaps_queued(lo: u32, hi: u32, queued: &std::collections::BTreeSet<u32>) -> bool {
    queued.range(lo..=hi).next().is_some()
}

fn layer_in_horizon(hi: u32, tip: u32, horizon: u32) -> bool {
    horizon > 0 && tip.saturating_sub(hi) < horizon
}

fn splice_queued_or_horizon(
    head: Option<Arc<IdLayer>>,
    queued: &std::collections::BTreeSet<u32>,
    tip: u32,
    horizon: u32,
    taken_hi: Option<u32>,
) -> Option<Arc<IdLayer>> {
    splice_queued_pred(head, |lo, hi| {
        span_overlaps_queued(lo, hi, queued)
            || layer_in_horizon(hi, tip, horizon)
            || span_overlaps_taken(lo, hi, tip, taken_hi)
    })
}

/// Overlap of `lo..=hi` with `(tip, taken_hi]` (in-pipeline, already off BQ).
fn span_overlaps_taken(lo: u32, hi: u32, tip: u32, taken_hi: Option<u32>) -> bool {
    let Some(taken) = taken_hi else {
        return false;
    };
    if taken <= tip {
        return false;
    }
    lo <= taken && hi > tip
}

fn splice_queued(
    head: Option<Arc<IdLayer>>,
    queued: &std::collections::BTreeSet<u32>,
) -> Option<Arc<IdLayer>> {
    splice_queued_pred(head, |lo, hi| span_overlaps_queued(lo, hi, queued))
}

fn splice_queued_pred(
    head: Option<Arc<IdLayer>>,
    keep_span: impl Fn(u32, u32) -> bool,
) -> Option<Arc<IdLayer>> {
    layer_chain::splice_kept(head, |l| keep_span(l.lo, l.hi))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn txid_hasher_uses_first_8_bytes() {
        let mut h = TxidHasher::default();
        let mut t = [0u8; 32];
        t[..8].copy_from_slice(&0x0102_0304_0506_0708u64.to_le_bytes());
        t[8] = 0xff;
        h.write(&t);
        assert_eq!(h.finish(), 0x0102_0304_0506_0708);
        let mut other = [0u8; 32];
        other[..8].copy_from_slice(&t[..8]);
        other[31] = 1;
        let mut h2 = TxidHasher::default();
        h2.write(&other);
        assert_eq!(
            h2.finish(),
            h.finish(),
            "same prefix must hash equal; full-key eq still separates them"
        );
        let mut m = IdMap::default();
        m.insert(t, (Fk(1), (0, 1)));
        m.insert(other, (Fk(2), (0, 1)));
        assert_eq!(m.get(&t).map(|v| v.0), Some(Fk(1)));
        assert_eq!(m.get(&other).map(|v| v.0), Some(Fk(2)));
    }

    #[test]
    fn outpoint_hasher_mixes_vout() {
        use std::collections::HashSet;
        use std::hash::{BuildHasher, BuildHasherDefault};
        let mut prefix = [0u8; 32];
        prefix[..8].copy_from_slice(&0x1111_2222_3333_4444u64.to_le_bytes());
        let mut other = prefix;
        other[8] = 0xaa;
        let build = BuildHasherDefault::<OutPointHasher>::default();
        let hash_of = |txid: [u8; 32], vout: u32| build.hash_one(&(txid, vout));
        assert_ne!(
            hash_of(prefix, 0),
            hash_of(prefix, 1),
            "same txid prefix, vout 0 vs 1 must hash distinct"
        );
        assert_ne!(
            hash_of(prefix, 0),
            hash_of(other, 0),
            "different txid, same vout must hash distinct (TxidHasher overwrites with vout)"
        );
        type Set = HashSet<([u8; 32], u32), BuildHasherDefault<OutPointHasher>>;
        let mut s: Set = HashSet::with_hasher(BuildHasherDefault::default());
        assert!(s.insert((prefix, 0)));
        assert!(
            s.insert((prefix, 1)),
            "same txid prefix, vout 0 vs 1 must be distinct"
        );
        assert_eq!(s.len(), 2);
        assert!(
            s.insert((other, 0)),
            "different txid, same vout must stay distinct"
        );
        assert_eq!(s.len(), 3);
    }

    fn tid(b: u8) -> [u8; 32] {
        let mut t = [0u8; 32];
        t[0] = b;
        t
    }

    fn map_one() -> Arc<IdMap> {
        let mut m = IdMap::default();
        m.insert(tid(1), (Fk(9), (100, 8)));
        Arc::new(m)
    }

    #[test]
    fn publish_makes_get_visible() {
        let p = PublishedIds::new();
        assert!(p.get(&tid(1)).is_none());
        p.publish(map_one());
        assert_eq!(p.get(&tid(1)), Some((Fk(9), (100, 8))));
        assert!(p.get(&tid(2)).is_none());
    }

    #[test]
    fn unpublish_hides_from_new_readers() {
        let p = PublishedIds::new();
        p.publish(map_one());
        p.unpublish();
        assert!(p.get(&tid(1)).is_none());
        assert!(p.load().is_none());
    }

    #[test]
    fn unpublish_keeps_old_arc() {
        let p = PublishedIds::new();
        p.publish(map_one());
        let held = p.load().expect("published");
        p.unpublish();
        assert_eq!(held.get(&tid(1)), Some((Fk(9), (100, 8))));
        assert!(p.get(&tid(1)).is_none());
    }

    #[test]
    fn zero_txid_is_never_a_hit() {
        let p = PublishedIds::new();
        let mut m = IdMap::default();
        m.insert([0u8; 32], (Fk(1), (0, 1)));
        p.publish(Arc::new(m));
        assert!(p.get(&[0u8; 32]).is_none());
    }

    fn hits(pairs: &[([u8; 32], Fk, (u64, u64))]) -> IdMap {
        pairs.iter().map(|(t, f, r)| (*t, (*f, *r))).collect()
    }

    #[test]
    fn finish_wave_publishes_and_second_wave_skips() {
        let published = PublishedIds::new();
        let mut live = LiveUnion::new();
        let t1 = tid(1);
        let t2 = tid(2);
        live.finish_wave(hits(&[(t1, Fk(10), (1, 2))]), &published);
        assert_eq!(published.get(&t1), Some((Fk(10), (1, 2))));
        let mut layer = IdMap::default();
        let (skipped, need) = live.partition_into_layer([&t1, &t2], &mut layer);
        assert_eq!(skipped, 1);
        assert_eq!(layer.get(&t1).copied(), Some((Fk(10), (1, 2))));
        assert_eq!(need, vec![t2]);
    }

    #[test]
    fn partition_rehomes_into_layer_without_known_map() {
        let mut live = LiveUnion::new();
        let p = tid(1);
        let fresh = tid(2);
        live.note_span(1, 1, hits(&[(p, Fk(10), (1, 2))]));
        let mut layer = IdMap::default();
        let (skipped, need) = live.partition_into_layer([&p, &fresh], &mut layer);
        assert_eq!(skipped, 1, "union hit skips TipOnly");
        assert_eq!(
            layer.get(&p).copied(),
            Some((Fk(10), (1, 2))),
            "this-wave layer must re-home the union parent"
        );
        assert!(!layer.contains_key(&fresh));
        assert_eq!(need, vec![fresh]);
        let (skipped0, need0) = live.partition_into_layer([&[0u8; 32], &p], &mut IdMap::default());
        assert_eq!(skipped0, 1);
        assert!(need0.is_empty(), "zero txid is never TipOnly need");
        assert!(
            live.get(&p).is_some(),
            "older layer still answers get until splice"
        );
    }

    #[test]
    fn rehome_survives_oldest_layer_drop() {
        let published = PublishedIds::new();
        let mut live = LiveUnion::new();
        let p = tid(1);
        live.note_span(1, 1, hits(&[(p, Fk(10), (1, 2))]));
        let mut layer = IdMap::default();
        let (skipped, need) = live.partition_into_layer([&p, &tid(2)], &mut layer);
        assert_eq!(skipped, 1);
        assert_eq!(need, vec![tid(2)]);
        live.note_span(2, 2, layer);
        live.keep_heights(|h| h != 1);
        live.publish(&published);
        assert_eq!(
            published.get(&p),
            Some((Fk(10), (1, 2))),
            "re-homed parent must survive drop of oldest span"
        );
        assert!(
            published.get(&tid(2)).is_none(),
            "unresolved this-wave key is not in the layer"
        );
    }

    #[test]
    fn publish_reuses_layer_arc_when_unchanged() {
        let published = PublishedIds::new();
        let mut live = LiveUnion::new();
        live.finish_wave(hits(&[(tid(1), Fk(10), (1, 2))]), &published);
        let a = published.load().expect("published");
        live.publish(&published);
        let b = published.load().expect("published");
        assert!(
            Arc::ptr_eq(&a, &b),
            "unchanged union must Arc-bump, not rebuild a HashMap"
        );
    }

    #[test]
    fn forget_only_wave1_drops_unique_keeps_shared() {
        let published = PublishedIds::new();
        let mut live = LiveUnion::new();
        let shared = tid(1);
        let only1 = tid(2);
        let w1 = live.finish_wave(
            hits(&[(shared, Fk(10), (1, 2)), (only1, Fk(11), (3, 4))]),
            &published,
        );
        let w2 = live.finish_wave(
            hits(&[(shared, Fk(10), (1, 2)), (tid(3), Fk(12), (5, 6))]),
            &published,
        );
        let kept_hits = Arc::clone(&published.load().expect("head").hits);
        live.keep_heights(|h| h != w1);
        live.publish(&published);
        assert!(published.get(&only1).is_none(), "wave-1-only key must drop");
        assert_eq!(
            published.get(&shared),
            Some((Fk(10), (1, 2))),
            "shared key survives forget of wave 1"
        );
        assert_eq!(published.get(&tid(3)), Some((Fk(12), (5, 6))));
        let head = published.load().expect("w2 remains");
        assert_eq!((head.lo, head.hi), (w2, w2));
        assert!(head.older.is_none(), "dropped layer must leave the chain");
        assert!(
            Arc::ptr_eq(&head.hits, &kept_hits),
            "kept layer hit map must not be cloned"
        );
    }

    #[test]
    fn span_overlaps_queued_does_not_walk_width() {
        let mut q = std::collections::BTreeSet::new();
        q.insert(500);
        assert!(span_overlaps_queued(0, 1079, &q));
        assert!(!span_overlaps_queued(0, 499, &q));
        assert!(!span_overlaps_queued(501, 1079, &q));
    }

    #[test]
    fn keep_queued_or_horizon_holds_after_bq_drop() {
        let published = PublishedIds::new();
        let mut live = LiveUnion::new();
        live.note_span(10, 12, hits(&[(tid(1), Fk(10), (1, 2))]));
        live.publish(&published);
        let empty = std::collections::BTreeSet::new();
        live.keep_queued_or_horizon(&empty, 20, 16, None);
        live.publish(&published);
        assert_eq!(
            published.get(&tid(1)),
            Some((Fk(10), (1, 2))),
            "layer hi=12 must stay while tip-hi < horizon"
        );
        live.keep_queued_or_horizon(&empty, 40, 16, None);
        live.publish(&published);
        assert!(
            published.get(&tid(1)).is_none(),
            "layer must drop once tip-hi >= horizon and BQ empty"
        );
    }

    #[test]
    fn keep_queued_or_horizon_holds_taken_off_bq_span() {
        let published = PublishedIds::new();
        let mut live = LiveUnion::new();
        live.note_span(10, 12, hits(&[(tid(1), Fk(10), (1, 2))]));
        live.publish(&published);
        let empty = std::collections::BTreeSet::new();
        live.keep_queued_or_horizon(&empty, 5, 0, Some(12));
        live.publish(&published);
        assert_eq!(
            published.get(&tid(1)),
            Some((Fk(10), (1, 2))),
            "taken (off-BQ) span overlapping (tip, taken_hi] must stay even at horizon=0"
        );
        live.keep_queued_or_horizon(&empty, 12, 0, Some(12));
        live.publish(&published);
        assert!(
            published.get(&tid(1)).is_none(),
            "layer must drop once tip has caught taken_hi and horizon is 0"
        );
    }

    #[test]
    fn keep_span_while_any_height_in_range_queued() {
        let published = PublishedIds::new();
        let mut live = LiveUnion::new();
        live.note_span(3, 5, hits(&[(tid(1), Fk(10), (1, 2))]));
        live.publish(&published);
        live.keep_heights(|h| h != 3);
        live.publish(&published);
        assert_eq!(
            published.get(&tid(1)),
            Some((Fk(10), (1, 2))),
            "layer 3..=5 must stay while 4 or 5 is still queued"
        );
        live.keep_heights(|h| h > 5);
        live.publish(&published);
        assert!(
            published.get(&tid(1)).is_none(),
            "layer must drop when no height in the span remains"
        );
    }

    #[test]
    fn note_span_and_keep_walk_layers_without_union_rebuild() {
        let mut live = LiveUnion::new();
        live.note_span(1, 1, hits(&[(tid(1), Fk(10), (1, 2))]));
        live.note_span(2, 2, hits(&[(tid(2), Fk(11), (3, 4))]));
        live.keep_heights(|h| h == 2);
        assert_eq!(live.get(&tid(2)), Some((Fk(11), (3, 4))));
        assert!(
            live.get(&tid(1)).is_none(),
            "dropped layer must not be rebuilt into a live union map"
        );
        let mut layer = IdMap::default();
        let (skipped, need) = live.partition_into_layer([&tid(2), &tid(3)], &mut layer);
        assert_eq!(skipped, 1);
        assert_eq!(layer.get(&tid(2)).copied(), Some((Fk(11), (3, 4))));
        assert_eq!(need, vec![tid(3)]);
    }

    #[test]
    fn store_none_hides_until_next_publish() {
        let published = PublishedIds::new();
        let mut live = LiveUnion::new();
        live.finish_wave(hits(&[(tid(1), Fk(10), (1, 2))]), &published);
        published.unpublish();
        assert!(published.get(&tid(1)).is_none());
        live.finish_wave(hits(&[(tid(1), Fk(10), (1, 2))]), &published);
        assert_eq!(published.get(&tid(1)), Some((Fk(10), (1, 2))));
    }
}
