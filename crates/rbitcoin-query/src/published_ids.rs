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
}
