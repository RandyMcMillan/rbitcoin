//! Write-once published parent identity (`txid → fk + range`).
//!
//! Lookup freezes a [`HashMap`] and [`PublishedIds::publish`] stores an `Arc`
//! so load stamp can [`get`](PublishedIds::get) with no mutex. [`unpublish`]
//! (store `None`) drops visibility for new readers; a reader holding the old
//! `Arc` still sees hits.

use arc_swap::ArcSwapOption;
use rbitcoin_primitives::Fk;
use std::collections::HashMap;
use std::sync::Arc;

/// Immutable `txid → (create_fk, body_range)` snapshot.
pub type IdMap = HashMap<[u8; 32], (Fk, (u64, u64))>;

/// Atomic published identity union. Readers `load` an `Arc`; writers `store`.
#[derive(Debug)]
pub struct PublishedIds {
    inner: ArcSwapOption<IdMap>,
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

    /// Make `map` visible to new [`load`](Self::load) / [`get`](Self::get).
    pub fn publish(&self, map: Arc<IdMap>) {
        self.inner.store(Some(map));
    }

    /// New [`load`](Self::load) / [`get`](Self::get) miss. Held Arcs still work.
    pub fn unpublish(&self) {
        self.inner.store(None);
    }

    /// Snapshot pointer. `None` after [`unpublish`](Self::unpublish).
    pub fn load(&self) -> Option<Arc<IdMap>> {
        self.inner.load_full()
    }

    /// Point get. Zero txid is never a hit.
    pub fn get(&self, txid: &[u8; 32]) -> Option<(Fk, (u64, u64))> {
        if *txid == [0u8; 32] {
            return None;
        }
        self.load()?.get(txid).copied()
    }
}

/// Wave ids waiting to be forgotten at the next lookup wave-end snapshot.
#[derive(Debug, Default)]
pub struct ForgetQueue {
    ids: std::sync::Mutex<Vec<u32>>,
}

impl ForgetQueue {
    pub fn new() -> Self {
        Self::default()
    }

    /// Dequeue / drop only enqueues. Lookup applies at wave end.
    pub fn enqueue(&self, wave_id: u32) {
        self.ids
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .push(wave_id);
    }

    pub fn drain(&self) -> Vec<u32> {
        std::mem::take(&mut *self.ids.lock().unwrap_or_else(|p| p.into_inner()))
    }
}

#[derive(Clone, Copy, Debug)]
struct LiveEnt {
    fk: Fk,
    range: (u64, u64),
    refs: u32,
}

/// Lookup-thread union of still-live wave hits. Not shared with load.
#[derive(Debug, Default)]
pub struct LiveUnion {
    live: HashMap<[u8; 32], LiveEnt>,
    wave_keys: HashMap<u32, Vec<[u8; 32]>>,
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
        self.live.get(txid).map(|e| (e.fk, e.range))
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

    fn apply_forgets(&mut self, ids: &[u32]) {
        for &id in ids {
            let Some(keys) = self.wave_keys.remove(&id) else {
                continue;
            };
            for t in keys {
                let Some(e) = self.live.get_mut(&t) else {
                    continue;
                };
                e.refs = e.refs.saturating_sub(1);
                if e.refs == 0 {
                    self.live.remove(&t);
                }
            }
        }
    }

    /// Apply dequeued heights, then insert this height's hits. Does not swap.
    pub fn note_height(&mut self, forget: &ForgetQueue, height: u32, hits: &IdMap) {
        self.apply_forgets(&forget.drain());
        let mut keys = Vec::with_capacity(hits.len());
        for (t, &(fk, range)) in hits {
            if *t == [0u8; 32] {
                continue;
            }
            keys.push(*t);
            match self.live.get_mut(t) {
                Some(e) => {
                    e.fk = fk;
                    e.range = range;
                    e.refs = e.refs.saturating_add(1);
                }
                None => {
                    self.live.insert(*t, LiveEnt { fk, range, refs: 1 });
                }
            }
        }
        self.wave_keys.insert(height, keys);
    }

    /// One snapshot swap. Call once after a wave's [`note_height`]s.
    pub fn publish(&self, published: &PublishedIds) {
        published.publish(Arc::new(self.snapshot()));
    }

    /// Drain forgets, insert hits under a synthetic height, publish one snapshot.
    pub fn finish_wave(
        &mut self,
        forget: &ForgetQueue,
        hits: &IdMap,
        published: &PublishedIds,
    ) -> u32 {
        let height = self.next_wave;
        self.next_wave = self.next_wave.saturating_add(1);
        self.note_height(forget, height, hits);
        self.publish(published);
        height
    }

    pub fn snapshot(&self) -> IdMap {
        self.live
            .iter()
            .map(|(t, e)| (*t, (e.fk, e.range)))
            .collect()
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
        assert_eq!(held.get(&tid(1)).copied(), Some((Fk(9), (100, 8))));
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
        let forget = ForgetQueue::new();
        let mut live = LiveUnion::new();
        let t1 = tid(1);
        let t2 = tid(2);
        live.finish_wave(&forget, &hits(&[(t1, Fk(10), (1, 2))]), &published);
        assert_eq!(published.get(&t1), Some((Fk(10), (1, 2))));
        let (known, need) = live.partition([&t1, &t2]);
        assert_eq!(known.get(&t1).copied(), Some((Fk(10), (1, 2))));
        assert_eq!(need, vec![t2]);
    }

    #[test]
    fn forget_only_wave1_drops_unique_keeps_shared() {
        let published = PublishedIds::new();
        let forget = ForgetQueue::new();
        let mut live = LiveUnion::new();
        let shared = tid(1);
        let only1 = tid(2);
        let w1 = live.finish_wave(
            &forget,
            &hits(&[(shared, Fk(10), (1, 2)), (only1, Fk(11), (3, 4))]),
            &published,
        );
        let _w2 = live.finish_wave(
            &forget,
            &hits(&[(shared, Fk(10), (1, 2)), (tid(3), Fk(12), (5, 6))]),
            &published,
        );
        forget.enqueue(w1);
        live.finish_wave(&forget, &IdMap::new(), &published);
        assert!(published.get(&only1).is_none(), "wave-1-only key must drop");
        assert_eq!(
            published.get(&shared),
            Some((Fk(10), (1, 2))),
            "shared key survives forget of wave 1"
        );
        assert_eq!(published.get(&tid(3)), Some((Fk(12), (5, 6))));
    }

    #[test]
    fn store_none_hides_until_next_publish() {
        let published = PublishedIds::new();
        let forget = ForgetQueue::new();
        let mut live = LiveUnion::new();
        live.finish_wave(&forget, &hits(&[(tid(1), Fk(10), (1, 2))]), &published);
        published.unpublish();
        assert!(published.get(&tid(1)).is_none());
        live.finish_wave(&forget, &hits(&[(tid(1), Fk(10), (1, 2))]), &published);
        assert_eq!(published.get(&tid(1)), Some((Fk(10), (1, 2))));
    }
}
