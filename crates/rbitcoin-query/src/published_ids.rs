//! Published parent identity (`txid → fk + range`) as a height-layered chain.
//!
//! Lookup prepends one [`IdLayer`] per BQ height and [`publish`](LiveUnion::publish)
//! stores the chain head (`Arc` bump). Load [`get`](PublishedIds::get) walks
//! newest → older with no mutex. Layers whose heights have left the body queue
//! are spliced out at the next wave end. [`unpublish`](PublishedIds::unpublish)
//! (store `None`) drops visibility for new readers; a reader holding the old
//! `Arc` still sees hits.

use arc_swap::ArcSwapOption;
use rbitcoin_primitives::Fk;
use std::collections::HashMap;
use std::sync::Arc;

/// Immutable `txid → (create_fk, body_range)` for one height.
pub type IdMap = HashMap<[u8; 32], (Fk, (u64, u64))>;

/// One BQ height's hits plus the older chain.
#[derive(Debug)]
pub struct IdLayer {
    pub height: u32,
    pub hits: Arc<IdMap>,
    pub older: Option<Arc<IdLayer>>,
}

impl IdLayer {
    /// Newest-first walk. First layer that has `txid` wins.
    pub fn get(&self, txid: &[u8; 32]) -> Option<(Fk, (u64, u64))> {
        let mut layer = self;
        loop {
            if let Some(&v) = layer.hits.get(txid) {
                return Some(v);
            }
            match layer.older.as_deref() {
                Some(older) => layer = older,
                None => return None,
            }
        }
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
        self.inner.store(Some(Arc::new(IdLayer {
            height: 0,
            hits: map,
            older: None,
        })));
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

    /// Point get. Zero txid is never a hit.
    pub fn get(&self, txid: &[u8; 32]) -> Option<(Fk, (u64, u64))> {
        if *txid == [0u8; 32] {
            return None;
        }
        self.load()?.get(txid)
    }
}

/// Lookup-thread union of still-queued height layers. Not shared with load.
#[derive(Debug, Default)]
pub struct LiveUnion {
    live: HashMap<[u8; 32], (Fk, (u64, u64))>,
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
        self.live.get(txid).copied()
    }

    /// Split `keys` into already-known hits vs TipOnly need.
    pub fn partition<'a>(
        &self,
        keys: impl IntoIterator<Item = &'a [u8; 32]>,
    ) -> (IdMap, Vec<[u8; 32]>) {
        let mut known = IdMap::new();
        let mut need = Vec::new();
        for t in keys {
            if *t == [0u8; 32] {
                continue;
            }
            match self.get(t) {
                Some(hit) => {
                    known.insert(*t, hit);
                }
                None => need.push(*t),
            }
        }
        (known, need)
    }

    fn reindex(&mut self) {
        self.live.clear();
        let mut cur = self.head.as_deref();
        while let Some(layer) = cur {
            for (t, v) in layer.hits.iter() {
                self.live.entry(*t).or_insert(*v);
            }
            cur = layer.older.as_deref();
        }
    }

    /// Drop layers whose heights fail `keep`. Does not swap published.
    pub fn keep_heights(&mut self, keep: impl Fn(u32) -> bool) {
        self.head = splice_kept(self.head.take(), keep);
        self.reindex();
    }

    /// Prepend this height (replacing a prior layer at the same height).
    pub fn note_height(&mut self, height: u32, hits: &IdMap) {
        self.head = splice_kept(self.head.take(), |h| h != height);
        let mut layer_hits = IdMap::new();
        for (t, &v) in hits {
            if *t == [0u8; 32] {
                continue;
            }
            layer_hits.insert(*t, v);
        }
        if !layer_hits.is_empty() {
            self.head = Some(Arc::new(IdLayer {
                height,
                hits: Arc::new(layer_hits),
                older: self.head.take(),
            }));
        }
        self.reindex();
    }

    /// Arc-bump the chain head. Call once after a wave's [`note_height`]s.
    pub fn publish(&self, published: &PublishedIds) {
        published.publish_head(self.head.clone());
    }

    /// Insert hits under a synthetic height, publish the chain head.
    pub fn finish_wave(&mut self, hits: &IdMap, published: &PublishedIds) -> u32 {
        let height = self.next_wave;
        self.next_wave = self.next_wave.saturating_add(1);
        self.note_height(height, hits);
        self.publish(published);
        height
    }
}

/// Rebuild the chain keeping nodes that pass `keep`. Kept hit maps are
/// `Arc`-cloned; suffix nodes whose `older` pointer is unchanged are reused.
fn splice_kept(head: Option<Arc<IdLayer>>, keep: impl Fn(u32) -> bool) -> Option<Arc<IdLayer>> {
    let mut nodes = Vec::new();
    let mut cur = head;
    while let Some(n) = cur {
        let older = n.older.clone();
        nodes.push(n);
        cur = older;
    }
    let mut new_head: Option<Arc<IdLayer>> = None;
    for n in nodes.into_iter().rev() {
        if !keep(n.height) {
            continue;
        }
        let older_ok = match (n.older.as_ref(), new_head.as_ref()) {
            (None, None) => true,
            (Some(a), Some(b)) => Arc::ptr_eq(a, b),
            _ => false,
        };
        if older_ok {
            new_head = Some(n);
        } else {
            new_head = Some(Arc::new(IdLayer {
                height: n.height,
                hits: Arc::clone(&n.hits),
                older: new_head,
            }));
        }
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

    fn map_one() -> Arc<IdMap> {
        let mut m = HashMap::new();
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
        let mut m = HashMap::new();
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
        live.finish_wave(&hits(&[(t1, Fk(10), (1, 2))]), &published);
        assert_eq!(published.get(&t1), Some((Fk(10), (1, 2))));
        let (known, need) = live.partition([&t1, &t2]);
        assert_eq!(known.get(&t1).copied(), Some((Fk(10), (1, 2))));
        assert_eq!(need, vec![t2]);
    }

    #[test]
    fn publish_reuses_layer_arc_when_unchanged() {
        let published = PublishedIds::new();
        let mut live = LiveUnion::new();
        live.finish_wave(&hits(&[(tid(1), Fk(10), (1, 2))]), &published);
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
            &hits(&[(shared, Fk(10), (1, 2)), (only1, Fk(11), (3, 4))]),
            &published,
        );
        let w2 = live.finish_wave(
            &hits(&[(shared, Fk(10), (1, 2)), (tid(3), Fk(12), (5, 6))]),
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
        assert_eq!(head.height, w2);
        assert!(head.older.is_none(), "dropped layer must leave the chain");
        assert!(
            Arc::ptr_eq(&head.hits, &kept_hits),
            "kept layer hit map must not be cloned"
        );
    }

    #[test]
    fn store_none_hides_until_next_publish() {
        let published = PublishedIds::new();
        let mut live = LiveUnion::new();
        live.finish_wave(&hits(&[(tid(1), Fk(10), (1, 2))]), &published);
        published.unpublish();
        assert!(published.get(&tid(1)).is_none());
        live.finish_wave(&hits(&[(tid(1), Fk(10), (1, 2))]), &published);
        assert_eq!(published.get(&tid(1)), Some((Fk(10), (1, 2))));
    }
}
