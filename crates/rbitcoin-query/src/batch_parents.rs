//! Pipeline-shared sparse parent pins for confirm.
//!
//! **Sharing:** one [`SharedParentPin`] per create_fk (refcounted via `Arc`) while
//! any in-flight batch needs it. Batches hold a cheap handle map
//! ([`BatchParents`]) of `Arc`s — no deep-copy of outs between stages/batches.
//!
//! **Registry:** [`PipelineParentStore`] keeps `Weak` entries. Prep **does not**
//! take the store mutex on every parent insert (that made free-plan pin ~9×
//! slower). Instead:
//! 1. [`BatchParents::adopt_from_store`] — one lock, upgrade live pins for the
//!    batch's parent set (cross-batch RAM share)
//! 2. Local [`BatchParents::insert_owned`] — lock-free HashMap insert (same cost
//!    class as the pre-share `ParentEntry` path)
//! 3. [`BatchParents::publish_to_store`] — one lock, publish Weaks / merge races
//!
//! Assemble/write read pin data through the batch's `Arc`s — **no** global map
//! lock on the hot path.
//!
//! **Immutable publish:** outs and layout are **separate** immutable Arc
//! snapshots published via [`arc_swap::ArcSwap`] (lock-free load; RCU store).
//! Widening need-vouts composes only the outs half; layout fill composes only
//! denserels/range — never clones script bytes for a layout-only publish.
//! No-op compose keeps Arc identity (no full-body clone on share hits). Never
//! `push`/mutate shared vectors in place.
//!
//! **Assemble sticky:** [`BatchParents`] caches the last outs Arc by create_fk
//! so multi-input same-parent prevout lookup does not re-load on every input.
//!
//! **Sparse:** only spent need-vouts + layout fields write/assemble need (not
//! full parent output sets). Vout merge when a later batch spends more outs.
//!
//! Create heights are not stashed — write re-reads the height fence.

use arc_swap::ArcSwap;
use rbitcoin_primitives::Fk;
use rbitcoin_store::{OutputRecord, TxRecord};
use std::cell::RefCell;
use std::collections::HashMap;
use std::hash::BuildHasherDefault;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Arc, Mutex, Weak};

pub use rbitcoin_store::{FkMap, FkSet, U32Map, U64IdentityHasher, U64Map, U64Set};

/// Relative offset sentinel: layout unknown for this out.
pub const SPENDER_REL_UNKNOWN: u32 = u32::MAX;

const CB_UNKNOWN: u8 = 0;
const CB_FALSE: u8 = 1;
const CB_TRUE: u8 = 2;

/// Immutable sparse need outs (compose → publish, never mutate).
#[derive(Debug, Clone)]
struct PinOuts {
    outs: Vec<(u32, OutputRecord)>,
    checked: Vec<u32>,
}

impl PinOuts {
    fn new(live: Vec<(u32, OutputRecord)>, checked: Vec<u32>) -> Self {
        Self {
            outs: ensure_outs_sorted(live),
            checked: ensure_checked_sorted(checked),
        }
    }

    fn covers_need(&self, need: &[u32]) -> bool {
        if need.is_empty() {
            return true;
        }
        if self.checked.is_empty() {
            return false;
        }
        need.iter().all(|v| checked_contains(&self.checked, *v))
    }

    fn has_all_live(&self, live: &[(u32, OutputRecord)]) -> bool {
        live.iter()
            .all(|(v, _)| self.outs.binary_search_by_key(v, |(dv, _)| *dv).is_ok())
    }

    /// Compose wider need coverage (new half; does not mutate `self`).
    fn compose(&self, live: Vec<(u32, OutputRecord)>, checked: &[u32]) -> Self {
        let mut outs = self.outs.clone();
        for (v, o) in live {
            if outs.binary_search_by_key(&v, |(dv, _)| *dv).is_err() {
                outs.push((v, o));
            }
        }
        outs.sort_unstable_by_key(|(v, _)| *v);
        let mut ch = self.checked.clone();
        ch.extend_from_slice(checked);
        ch.sort_unstable();
        ch.dedup();
        Self { outs, checked: ch }
    }
}

/// `txout` range + `spent` range. Abs = spent_off + 9×vout (no denserels).
#[derive(Debug, Clone, Default)]
struct ParentLayout {
    body_range: Option<(u64, u64)>,
    spent_range: Option<(u64, u64)>,
    spender_rels: Vec<(u32, u32)>,
}

impl ParentLayout {
    fn new(body_range: Option<(u64, u64)>, spender_rels: Vec<(u32, u32)>) -> Self {
        Self {
            body_range,
            spent_range: None,
            spender_rels,
        }
    }

    fn already_covers(&self, body_range: Option<(u64, u64)>, spender_rels: &[(u32, u32)]) -> bool {
        let range_done = body_range.is_none() || self.body_range.is_some();
        let rels_done = spender_rels.is_empty()
            || spender_rels.iter().all(|(v, r)| {
                self.spender_rels
                    .binary_search_by_key(v, |(dv, _)| *dv)
                    .ok()
                    .is_some_and(|i| self.spender_rels[i].1 == *r)
            });
        range_done && rels_done
    }

    /// Compose layout overlay (new half; does not mutate `self`).
    /// First writer wins for body_range; denserels merge by vout.
    fn compose(&self, body_range: Option<(u64, u64)>, spender_rels: &[(u32, u32)]) -> Self {
        let mut layout = self.clone();
        if layout.body_range.is_none() {
            layout.body_range = body_range;
        }
        merge_spender_rels_into(&mut layout.spender_rels, spender_rels);
        layout
    }

    /// Write-path: force body_range and merge denserels.
    fn compose_write(&self, body_range: (u64, u64), sparse_rels: &[(u32, u32)]) -> Self {
        let mut layout = self.clone();
        layout.body_range = Some(body_range);
        merge_spender_rels_into(&mut layout.spender_rels, sparse_rels);
        layout
    }
}

/// One create's sparse pin payload, shared across concurrent pipeline batches.
///
/// Outs and layout are independent immutable Arc halves (compose only the half
/// that changes), published via ArcSwap (lock-free load).
#[derive(Debug)]
pub struct SharedParentPin {
    fk: Fk,
    tx: TxRecord,
    /// 0 unknown, 1 not coinbase, 2 coinbase.
    coinbase: AtomicU8,
    /// Sparse need outs + checked (prep widen).
    outs: ArcSwap<PinOuts>,
    /// Abs layout for spentness/annotate (write fill).
    layout: ArcSwap<ParentLayout>,
}

impl SharedParentPin {
    fn new(
        fk: Fk,
        tx: TxRecord,
        live: Vec<(u32, OutputRecord)>,
        checked: Vec<u32>,
        coinbase: Option<bool>,
        body_range: Option<(u64, u64)>,
        spender_rels: Vec<(u32, u32)>,
    ) -> Self {
        let cb = match coinbase {
            Some(true) => CB_TRUE,
            Some(false) => CB_FALSE,
            None => CB_UNKNOWN,
        };
        Self {
            fk,
            tx,
            coinbase: AtomicU8::new(cb),
            outs: ArcSwap::from_pointee(PinOuts::new(live, checked)),
            layout: ArcSwap::from_pointee(ParentLayout::new(body_range, spender_rels)),
        }
    }

    #[inline]
    fn load_outs(&self) -> Arc<PinOuts> {
        self.outs.load_full()
    }

    #[inline]
    fn load_layout(&self) -> Arc<ParentLayout> {
        self.layout.load_full()
    }

    /// Compose outs half: `None` = no-op (keep existing Arc identity).
    ///
    /// Uses RCU so concurrent widens from peer batches merge correctly.
    fn publish_outs(&self, f: impl Fn(&PinOuts) -> Option<PinOuts>) {
        let cur = self.outs.load_full();
        if f(cur.as_ref()).is_none() {
            return;
        }
        self.outs.rcu(|cur| match f(cur.as_ref()) {
            None => Arc::clone(cur),
            Some(next) => Arc::new(next),
        });
    }

    /// Compose layout half: `None` = no-op (keep existing Arc identity).
    fn publish_layout(&self, f: impl Fn(&ParentLayout) -> Option<ParentLayout>) {
        let cur = self.layout.load_full();
        if f(cur.as_ref()).is_none() {
            return;
        }
        self.layout.rcu(|cur| match f(cur.as_ref()) {
            None => Arc::clone(cur),
            Some(next) => Arc::new(next),
        });
    }

    /// True when all `need` vouts are already in checked.
    #[inline]
    fn covers_need(&self, need: &[u32]) -> bool {
        self.load_outs().covers_need(need)
    }

    fn merge_outs(&self, live: Vec<(u32, OutputRecord)>, checked: &[u32]) {
        let snap = self.load_outs();
        if snap.covers_need(checked) && (live.is_empty() || snap.has_all_live(&live)) {
            return;
        }
        self.outs.rcu(|cur| {
            if !checked.is_empty()
                && cur.covers_need(checked)
                && (live.is_empty() || cur.has_all_live(&live))
            {
                Arc::clone(cur)
            } else {
                Arc::new(cur.compose(live.clone(), checked))
            }
        });
    }

    fn set_coinbase_if_known(&self, coinbase: Option<bool>) {
        if let Some(b) = coinbase {
            let v = if b { CB_TRUE } else { CB_FALSE };
            let _ =
                self.coinbase
                    .compare_exchange(CB_UNKNOWN, v, Ordering::Relaxed, Ordering::Relaxed);
        }
    }

    fn coinbase_opt(&self) -> Option<bool> {
        match self.coinbase.load(Ordering::Relaxed) {
            CB_TRUE => Some(true),
            CB_FALSE => Some(false),
            _ => None,
        }
    }

    /// Publish layout only when something new is present.
    fn maybe_merge_layout(&self, body_range: Option<(u64, u64)>, spender_rels: &[(u32, u32)]) {
        if body_range.is_none() && spender_rels.is_empty() {
            return;
        }
        let snap = self.load_layout();
        if snap.already_covers(body_range, spender_rels) {
            return;
        }
        self.layout.rcu(|cur| {
            if cur.already_covers(body_range, spender_rels) {
                Arc::clone(cur)
            } else {
                Arc::new(cur.compose(body_range, spender_rels))
            }
        });
    }

    /// Single-snap apply for free-pin Occupied path: outs widen and/or layout
    /// merge from one outs load + one layout load (no double compose when no-op).
    fn apply_pin_delta(
        &self,
        live: Option<(Vec<(u32, OutputRecord)>, &[u32])>,
        coinbase: Option<bool>,
        body_range: Option<(u64, u64)>,
        spender_rels: &[(u32, u32)],
    ) {
        self.set_coinbase_if_known(coinbase);
        if let Some((live, checked)) = live {
            self.merge_outs(live, checked);
        }
        self.maybe_merge_layout(body_range, spender_rels);
    }

    /// Pure share-hit: coinbase + layout only when material present (no outs touch).
    fn apply_meta_only(
        &self,
        coinbase: Option<bool>,
        body_range: Option<(u64, u64)>,
        spender_rels: &[(u32, u32)],
    ) {
        self.set_coinbase_if_known(coinbase);
        self.maybe_merge_layout(body_range, spender_rels);
    }
}

/// Prep-time registry: Weak map so dead pins free when last batch Arc drops.
///
/// Mutex is only for bulk adopt / publish of Arc handles — never held while
/// assemble walks inputs or write fills layout data, and **not** on the
/// per-parent insert hot path.
#[derive(Debug, Default)]
struct PinIndex {
    by_fk: U64Map<Weak<SharedParentPin>>,
    by_txid: HashMap<[u8; 32], Weak<SharedParentPin>>,
}

/// Prep-time registry: Weak map so dead pins free when last batch Arc drops.
///
/// Mutex is only for bulk adopt / publish / [`Self::bulk_lookup_txid`] — never
/// held while assemble walks inputs or write fills layout, and **not** on the
/// per-parent insert hot path.
#[derive(Debug, Default)]
pub struct PipelineParentStore {
    maps: Mutex<PinIndex>,
}

impl PipelineParentStore {
    pub fn new() -> Self {
        Self {
            maps: Mutex::new(PinIndex::default()),
        }
    }

    /// Live pin with non-zero txid and a stamped `txout` body range.
    ///
    /// Same Weak lifetime as outs share: last batch `Arc` drop → `None`.
    /// Zero txid is never indexed. Live pin without `body_range` is a miss
    /// (do not half-skip head).
    pub fn lookup_txid(&self, txid: &[u8; 32]) -> Option<(Fk, (u64, u64))> {
        self.bulk_lookup_txid(std::iter::once(txid))
            .into_iter()
            .next()
            .map(|(_, hit)| hit)
    }

    /// One lock: [`Self::lookup_txid`] rules for every key.
    ///
    /// Dead Weaks are dropped from the txid index. Keys with no live pin,
    /// zero txid, or no `body_range` are omitted from the map.
    pub fn bulk_lookup_txid<'a>(
        &self,
        txids: impl IntoIterator<Item = &'a [u8; 32]>,
    ) -> HashMap<[u8; 32], (Fk, (u64, u64))> {
        let mut g = self.maps.lock().unwrap_or_else(|e| e.into_inner());
        let mut out = HashMap::new();
        for txid in txids {
            if *txid == [0u8; 32] {
                continue;
            }
            let Some(w) = g.by_txid.get(txid) else {
                continue;
            };
            match w.upgrade() {
                Some(p) => {
                    if let Some(r) = p.load_layout().body_range {
                        out.insert(*txid, (p.fk, r));
                    }
                }
                None => {
                    g.by_txid.remove(txid);
                }
            }
        }
        out
    }

    /// Live strong pins still reachable via Weak (diagnostics / tests).
    pub fn live_count(&self) -> usize {
        let g = self.maps.lock().unwrap_or_else(|e| e.into_inner());
        g.by_fk.values().filter(|w| w.strong_count() > 0).count()
    }

    /// Occupancy: weak map slots, live strong pins, approx bytes of live pin outs.
    pub fn size_snapshot(&self) -> (usize, usize, u64) {
        let g = self.maps.lock().unwrap_or_else(|e| e.into_inner());
        let weak_slots = g.by_fk.len();
        let mut live = 0usize;
        let mut bytes = 0u64;
        for w in g.by_fk.values() {
            if let Some(p) = w.upgrade() {
                live = live.saturating_add(1);
                let outs = p.load_outs();
                // SharedParentPin: outs half scripts + layout rels + shell.
                bytes = bytes.saturating_add(96);
                for (_v, o) in &outs.outs {
                    bytes = bytes
                        .saturating_add(24)
                        .saturating_add(o.script.len() as u64);
                }
                bytes = bytes.saturating_add(outs.checked.len().saturating_mul(4) as u64);
                let lay = p.load_layout();
                bytes = bytes.saturating_add(lay.spender_rels.len().saturating_mul(8) as u64);
            }
        }
        // Weak map overhead (~24 B / slot including dead Weaks).
        bytes = bytes.saturating_add((weak_slots as u64).saturating_mul(24));
        (weak_slots, live, bytes)
    }

    /// Drop dead Weaks now (keeps map from retaining empty slots after pin drop).
    pub fn gc_dead_weaks(&self) {
        let mut g = self.maps.lock().unwrap_or_else(|e| e.into_inner());
        g.by_fk.retain(|_, w| w.strong_count() > 0);
        g.by_txid.retain(|_, w| w.strong_count() > 0);
        g.by_fk.shrink_to_fit();
        g.by_txid.shrink_to_fit();
    }

    /// One lock: upgrade live pins for `ids` into a map (prep batch start).
    pub(crate) fn bulk_upgrade(
        &self,
        ids: impl IntoIterator<Item = u64>,
    ) -> U64Map<Arc<SharedParentPin>> {
        let g = self.maps.lock().unwrap_or_else(|e| e.into_inner());
        let mut out = U64Map::default();
        for id in ids {
            if let Some(p) = g.by_fk.get(&id).and_then(|w| w.upgrade()) {
                out.insert(id, p);
            }
        }
        out
    }

    /// One lock: publish **selected** batch pins as Weaks (new Arc registrations).
    ///
    /// `publish_ids` should list create_fks whose local Arc is new to the store
    /// (typically vacant `insert_owned` results). Pure adopt hits already have a
    /// Weak and need not be re-walked — full-map publish was O(all parents).
    ///
    /// On Arc identity conflict (peer batch won the slot), merge local sparse
    /// fields into the existing Arc and replace the batch handle so both batches
    /// share one payload.
    pub(crate) fn bulk_publish_ids(
        &self,
        pins: &mut U64Map<Arc<SharedParentPin>>,
        publish_ids: &[u64],
    ) {
        if publish_ids.is_empty() {
            return;
        }
        let mut conflicts: Vec<(u64, Arc<SharedParentPin>, Arc<SharedParentPin>)> = Vec::new();
        {
            let mut g = self.maps.lock().unwrap_or_else(|e| e.into_inner());
            for &id in publish_ids {
                let Some(pin) = pins.get(&id) else {
                    continue;
                };
                match g.by_fk.get(&id).and_then(|w| w.upgrade()) {
                    Some(existing) if !Arc::ptr_eq(&existing, pin) => {
                        conflicts.push((id, existing, Arc::clone(pin)));
                    }
                    Some(_) => {}
                    None => {
                        let w = Arc::downgrade(pin);
                        g.by_fk.insert(id, w.clone());
                        if pin.tx.txid != [0u8; 32] {
                            g.by_txid.insert(pin.tx.txid, w);
                        }
                    }
                }
            }
            // Soft GC: drop dead Weaks periodically so the Weak map cannot retain
            // empty slots for the whole IBD. Threshold keeps some share hits
            // without unbounded weak growth (was 4k→65k; 16k is a middle ground).
            if g.by_fk.len() > 16_384 {
                g.by_fk.retain(|_, w| w.strong_count() > 0);
                g.by_txid.retain(|_, w| w.strong_count() > 0);
            }
        }
        for (id, existing, local) in conflicts {
            let src_outs = local.load_outs();
            let src_lay = local.load_layout();
            existing.merge_outs(src_outs.outs.clone(), &src_outs.checked);
            existing.set_coinbase_if_known(local.coinbase_opt());
            existing.maybe_merge_layout(src_lay.body_range, &src_lay.spender_rels);
            pins.insert(id, existing);
        }
    }
}

/// Per-batch handle map: `create_fk → Arc` shared pin (refcount only on clone).
///
/// Assemble sticky (`sticky_outs`) is batch-local and not shared across clones.
#[derive(Debug, Default)]
pub struct BatchParents {
    /// Optional pipeline store for sharing across concurrent batches.
    store: Option<Arc<PipelineParentStore>>,
    pins: U64Map<Arc<SharedParentPin>>,
    /// create_fks that need Weak registration (new Arc from this batch).
    /// Pure adopt hits are omitted — already published by a prior batch.
    publish_ids: Vec<u64>,
    /// Last outs Arc loaded for assemble (`get_parent_txout_parts`).
    sticky_outs: RefCell<Option<(u64, Arc<PinOuts>)>>,
}

impl Clone for BatchParents {
    fn clone(&self) -> Self {
        Self {
            store: self.store.clone(),
            pins: self.pins.clone(),
            // Cloned batch is a new handle map; re-publish if store-attached.
            publish_ids: self.pins.keys().copied().collect(),
            sticky_outs: RefCell::new(None),
        }
    }
}

impl BatchParents {
    pub fn new() -> Self {
        Self {
            store: None,
            pins: U64Map::default(),
            publish_ids: Vec::new(),
            sticky_outs: RefCell::new(None),
        }
    }

    pub fn with_capacity(n: usize) -> Self {
        Self {
            store: None,
            pins: U64Map::with_capacity_and_hasher(n, BuildHasherDefault::default()),
            publish_ids: Vec::with_capacity(n),
            sticky_outs: RefCell::new(None),
        }
    }

    /// Prep/IBD: share pins with other batches via `store`.
    ///
    /// Inserts stay local; call [`adopt_from_store`] before pin fill and
    /// [`publish_to_store`] after so the Weak registry stays current without a
    /// per-parent mutex on the free-plan path.
    pub fn with_store(store: Arc<PipelineParentStore>, capacity: usize) -> Self {
        Self {
            store: Some(store),
            pins: U64Map::with_capacity_and_hasher(capacity, BuildHasherDefault::default()),
            publish_ids: Vec::with_capacity(capacity),
            sticky_outs: RefCell::new(None),
        }
    }

    #[inline]
    fn invalidate_sticky(&self, id: u64) {
        let mut st = self.sticky_outs.borrow_mut();
        if st.as_ref().is_some_and(|(sid, _)| *sid == id) {
            *st = None;
        }
    }

    pub fn len(&self) -> usize {
        self.pins.len()
    }

    pub fn is_empty(&self) -> bool {
        self.pins.is_empty()
    }

    /// Bulk-adopt live shared pins for `ids` (one store lock). Call before pin fill.
    pub fn adopt_from_store(&mut self, ids: impl IntoIterator<Item = u64>) {
        let Some(store) = &self.store else {
            return;
        };
        let upgraded = store.bulk_upgrade(ids);
        if upgraded.is_empty() {
            return;
        }
        self.pins.reserve(upgraded.len());
        for (id, pin) in upgraded {
            self.pins.entry(id).or_insert(pin);
        }
    }

    /// Bulk-publish **new** local pins into the pipeline store (one store lock).
    /// Call after pin fill. Adopted Arcs are already registered and skipped.
    pub fn publish_to_store(&mut self) {
        let Some(store) = &self.store else {
            self.publish_ids.clear();
            return;
        };
        if self.publish_ids.is_empty() {
            return;
        }
        self.publish_ids.sort_unstable();
        self.publish_ids.dedup();
        store.bulk_publish_ids(&mut self.pins, &self.publish_ids);
        self.publish_ids.clear();
    }

    /// Layout / coinbase only when outs already cover need (cross-batch share hit).
    ///
    /// Skips all work when there is no meta material (pure share hit).
    #[inline]
    pub fn refresh_pin_meta(
        &mut self,
        fk: Fk,
        coinbase: Option<bool>,
        body_range: Option<(u64, u64)>,
        spender_rels: Vec<(u32, u32)>,
    ) {
        if coinbase.is_none() && body_range.is_none() && spender_rels.is_empty() {
            return;
        }
        let Some(id) = fk.get() else {
            return;
        };
        let Some(p) = self.pins.get(&id) else {
            return;
        };
        p.apply_meta_only(coinbase, body_range, &spender_rels);
    }

    /// Insert / merge one parent (prep pin hot path).
    ///
    /// **No store mutex** — pure batch HashMap. First insert for an id is the
    /// pre-share cost class (`ParentEntry` put). Merge only if the same batch
    /// already holds a partial pin (or after adopt left an incomplete cover).
    /// Occupied path uses one snap decision for outs+layout (single-snap).
    #[inline]
    pub fn insert_owned(
        &mut self,
        fk: Fk,
        tx: TxRecord,
        live: Vec<(u32, OutputRecord)>,
        checked: Vec<u32>,
        coinbase: Option<bool>,
        body_range: Option<(u64, u64)>,
        spender_rels: Vec<(u32, u32)>,
    ) {
        let Some(id) = fk.get() else {
            return;
        };
        match self.pins.entry(id) {
            std::collections::hash_map::Entry::Occupied(o) => {
                let p = o.get();
                let outs = p.load_outs();
                let need_outs = !checked.is_empty()
                    && !(outs.covers_need(&checked)
                        && (live.is_empty() || outs.has_all_live(&live)));
                if need_outs {
                    p.apply_pin_delta(
                        Some((live, checked.as_slice())),
                        coinbase,
                        body_range,
                        &spender_rels,
                    );
                    // Drop sticky so assemble does not see a stale narrower snap.
                    drop(outs);
                    self.invalidate_sticky(id);
                } else {
                    let _ = live;
                    p.apply_meta_only(coinbase, body_range, &spender_rels);
                }
            }
            std::collections::hash_map::Entry::Vacant(v) => {
                v.insert(Arc::new(SharedParentPin::new(
                    fk,
                    tx,
                    live,
                    checked,
                    coinbase,
                    body_range,
                    spender_rels,
                )));
                // New Arc — must register Weak on publish (adopt hits skip this).
                self.publish_ids.push(id);
            }
        }
    }

    /// Test / convenience: clone from slices into the map.
    pub fn put_resolved(
        &mut self,
        fk: Fk,
        tx: TxRecord,
        live: &[(u32, OutputRecord)],
        checked: &[u32],
        coinbase: Option<bool>,
    ) {
        self.insert_owned(
            fk,
            tx,
            live.to_vec(),
            checked.to_vec(),
            coinbase,
            None,
            Vec::new(),
        );
    }

    pub fn get_parent_out(&self, fk: Fk, vout: u32) -> Option<(TxRecord, OutputRecord)> {
        let id = fk.get()?;
        let e = self.pins.get(&id)?;
        let outs = e.load_outs();
        let i = outs.outs.binary_search_by_key(&vout, |(v, _)| *v).ok()?;
        Some((e.tx.clone(), outs.outs[i].1.clone()))
    }

    /// Assemble hot path: value + script bytes + parent txid (script owned from
    /// immutable outs snapshot).
    ///
    /// Sticky: multi-input spends of the same create reuse one outs Arc without
    /// re-entering the pin slot.
    #[inline]
    pub fn get_parent_txout_parts(&self, fk: Fk, vout: u32) -> Option<(i64, Vec<u8>, [u8; 32])> {
        self.parent_txout_parts_inner(fk, vout, true)
    }

    /// Same as [`get_parent_txout_parts`] but **always** `load_outs` (no sticky).
    /// Used as the fair cold control for sticky benches / tests.
    #[inline]
    pub fn get_parent_txout_parts_no_sticky(
        &self,
        fk: Fk,
        vout: u32,
    ) -> Option<(i64, Vec<u8>, [u8; 32])> {
        self.parent_txout_parts_inner(fk, vout, false)
    }

    #[inline]
    fn parent_txout_parts_inner(
        &self,
        fk: Fk,
        vout: u32,
        use_sticky: bool,
    ) -> Option<(i64, Vec<u8>, [u8; 32])> {
        let id = fk.get()?;
        let e = self.pins.get(&id)?;
        let outs = if use_sticky {
            let mut st = self.sticky_outs.borrow_mut();
            match st.as_ref() {
                Some((sid, snap)) if *sid == id => Arc::clone(snap),
                _ => {
                    let snap = e.load_outs();
                    *st = Some((id, Arc::clone(&snap)));
                    snap
                }
            }
        } else {
            e.load_outs()
        };
        let i = outs.outs.binary_search_by_key(&vout, |(v, _)| *v).ok()?;
        let o = &outs.outs[i].1;
        Some((o.value, o.script.clone(), e.tx.txid))
    }

    pub fn get_parent_tx(&self, fk: Fk) -> Option<TxRecord> {
        let id = fk.get()?;
        self.pins.get(&id).map(|e| e.tx.clone())
    }

    pub fn get_parent_coinbase(&self, fk: Fk) -> Option<bool> {
        let id = fk.get()?;
        self.pins.get(&id)?.coinbase_opt()
    }

    pub fn get_body_range(&self, fk: Fk) -> Option<(u64, u64)> {
        let id = fk.get()?;
        let e = self.pins.get(&id)?;
        e.load_layout().body_range
    }

    pub fn set_layout(&mut self, fk: Fk, body_range: (u64, u64), dense_rels: &[u32]) {
        self.set_layout_for_need(fk, body_range, dense_rels, &[]);
    }

    pub fn set_layout_for_need(
        &mut self,
        fk: Fk,
        body_range: (u64, u64),
        dense_rels: &[u32],
        extra_need: &[u32],
    ) {
        let Some(id) = fk.get() else {
            return;
        };
        let Some(e) = self.pins.get(&id) else {
            return;
        };
        // RCU must recompute checked from `cur` (not a stale pre-load snap):
        // concurrent prep merge_outs can add peer need-vouts between load and
        // publish; replacing with a snap-built list clobbered those vouts.
        let mut need_for_sparse: Vec<u32> = e.load_outs().checked.clone();
        let may_grow_checked =
            need_for_sparse.is_empty() && extra_need.is_empty() && !dense_rels.is_empty()
                || !extra_need.is_empty();
        if may_grow_checked {
            e.publish_outs(|cur| {
                let mut checked = cur.checked.clone();
                if checked.is_empty() && extra_need.is_empty() && !dense_rels.is_empty() {
                    checked = (0..dense_rels.len() as u32).collect();
                }
                if !extra_need.is_empty() {
                    checked.extend_from_slice(extra_need);
                    checked.sort_unstable();
                    checked.dedup();
                }
                if checked == cur.checked {
                    return None;
                }
                Some(PinOuts {
                    outs: cur.outs.clone(),
                    checked,
                })
            });
            self.invalidate_sticky(id);
            need_for_sparse = e.load_outs().checked.clone();
        }
        let sparse = sparse_spender_rels(dense_rels, &need_for_sparse);
        let lay = e.load_layout();
        if lay.body_range == Some(body_range) && lay.already_covers(Some(body_range), &sparse) {
            return;
        }
        e.publish_layout(|cur| {
            if cur.body_range == Some(body_range) && cur.already_covers(Some(body_range), &sparse) {
                return None;
            }
            Some(cur.compose_write(body_range, &sparse))
        });
    }

    pub fn set_body_range_only(&mut self, fk: Fk, body_range: (u64, u64)) {
        let Some(id) = fk.get() else {
            return;
        };
        if let Some(e) = self.pins.get(&id) {
            e.publish_layout(|cur| {
                if cur.body_range == Some(body_range) {
                    return None;
                }
                let mut next = cur.clone();
                next.body_range = Some(body_range);
                Some(next)
            });
        }
    }

    pub fn set_layout_sparse(
        &mut self,
        fk: Fk,
        body_range: (u64, u64),
        sparse_rels: Vec<(u32, u32)>,
        extra_need: &[u32],
    ) {
        let Some(id) = fk.get() else {
            return;
        };
        let Some(e) = self.pins.get(&id) else {
            return;
        };
        if !extra_need.is_empty() {
            e.publish_outs(|cur| {
                let mut checked = cur.checked.clone();
                checked.extend_from_slice(extra_need);
                checked.sort_unstable();
                checked.dedup();
                if checked == cur.checked {
                    return None;
                }
                Some(PinOuts {
                    outs: cur.outs.clone(),
                    checked,
                })
            });
            self.invalidate_sticky(id);
        }
        let lay = e.load_layout();
        if lay.body_range == Some(body_range) && lay.already_covers(Some(body_range), &sparse_rels)
        {
            return;
        }
        e.publish_layout(|cur| {
            if cur.body_range == Some(body_range)
                && cur.already_covers(Some(body_range), &sparse_rels)
            {
                return None;
            }
            Some(cur.compose_write(body_range, &sparse_rels))
        });
    }

    #[inline]
    pub fn has_abs_layout(&self, fk: Fk) -> bool {
        let Some(id) = fk.get() else {
            return false;
        };
        let Some(e) = self.pins.get(&id) else {
            return false;
        };
        let lay = e.load_layout();
        lay.spent_range.is_some()
    }

    #[inline]
    pub fn has_spender_rels(&self, fk: Fk) -> bool {
        let Some(id) = fk.get() else {
            return false;
        };
        let Some(e) = self.pins.get(&id) else {
            return false;
        };
        let lay = e.load_layout();
        // Schema 15: abs is spent_range only. Leftover txout-offset "rels" are not layout.
        lay.spent_range.is_some()
    }

    pub fn fks_missing_layout(&self) -> Vec<Fk> {
        self.pins
            .iter()
            .filter(|(_, e)| {
                let lay = e.load_layout();
                lay.spent_range.is_none()
                    && (lay.body_range.is_none() || lay.spender_rels.is_empty())
            })
            .map(|(&id, _)| Fk(id))
            .collect()
    }

    #[inline]
    pub fn contains(&self, fk: Fk) -> bool {
        fk.get().is_some_and(|id| self.pins.contains_key(&id))
    }

    pub fn get_spender_abs(&self, fk: Fk, vout: u32) -> Option<u64> {
        let id = fk.get()?;
        let e = self.pins.get(&id)?;
        let lay = e.load_layout();
        let (off, len) = lay.spent_range?;
        let abs = rbitcoin_store::spent_abs(off, vout);
        if abs.saturating_add(rbitcoin_store::OutputRecord::SPENT_SLOT_LEN as u64)
            > off.saturating_add(len)
        {
            return None;
        }
        Some(abs)
    }

    pub fn set_spent_range_only(&mut self, fk: Fk, spent_range: (u64, u64)) {
        let Some(id) = fk.get() else {
            return;
        };
        if let Some(e) = self.pins.get(&id) {
            e.publish_layout(|cur| {
                if cur.spent_range == Some(spent_range) {
                    return None;
                }
                let mut next = cur.clone();
                next.spent_range = Some(spent_range);
                Some(next)
            });
        }
    }

    pub fn has_parent_out(&self, fk: Fk, vout: u32) -> bool {
        let Some(id) = fk.get() else {
            return false;
        };
        let Some(e) = self.pins.get(&id) else {
            return false;
        };
        e.load_outs()
            .outs
            .binary_search_by_key(&vout, |(v, _)| *v)
            .is_ok()
    }

    pub fn pin_covered(&self, fk: Fk, vouts: &[u32]) -> bool {
        if vouts.is_empty() {
            return true;
        }
        let Some(id) = fk.get() else {
            return false;
        };
        let Some(e) = self.pins.get(&id) else {
            return false;
        };
        e.covers_need(vouts)
    }

    /// Absorb another batch's handles (write batch). Same create → keep one Arc
    /// (prefer already-present; merge sparse fields from `other` if needed).
    pub fn extend_from(&mut self, other: Self) {
        if other.pins.is_empty() {
            return;
        }
        if self.pins.is_empty() {
            *self = other;
            return;
        }
        self.pins.reserve(other.pins.len());
        // Carry forward other's unpublished Weak-registration set so a later
        // publish_to_store still registers Arcs that only lived on `other`.
        self.publish_ids.reserve(other.publish_ids.len());
        self.publish_ids.extend(other.publish_ids.iter().copied());
        for (id, src) in other.pins {
            match self.pins.entry(id) {
                std::collections::hash_map::Entry::Vacant(v) => {
                    v.insert(src);
                }
                std::collections::hash_map::Entry::Occupied(o) => {
                    if !Arc::ptr_eq(o.get(), &src) {
                        let src_outs = src.load_outs();
                        let src_lay = src.load_layout();
                        o.get().merge_outs(src_outs.outs.clone(), &src_outs.checked);
                        o.get().set_coinbase_if_known(src.coinbase_opt());
                        o.get()
                            .maybe_merge_layout(src_lay.body_range, &src_lay.spender_rels);
                    }
                }
            }
        }
    }

    pub fn get_parent_outs_needed(
        &self,
        fk: Fk,
        vouts: &[u32],
    ) -> Option<(TxRecord, Vec<(u32, OutputRecord)>, bool)> {
        let id = fk.get()?;
        let e = self.pins.get(&id)?;
        let body = e.load_outs();
        let covered =
            !body.checked.is_empty() && vouts.iter().all(|v| checked_contains(&body.checked, *v));
        if covered {
            let mut live = Vec::with_capacity(vouts.len());
            for &v in vouts {
                if let Ok(i) = body.outs.binary_search_by_key(&v, |(ov, _)| *ov) {
                    live.push((v, body.outs[i].1.clone()));
                }
            }
            return Some((e.tx.clone(), live, true));
        }
        if !body.outs.is_empty()
            && vouts
                .iter()
                .all(|v| body.outs.binary_search_by_key(v, |(ov, _)| *ov).is_ok())
        {
            let mut live = Vec::with_capacity(vouts.len());
            for &v in vouts {
                if let Ok(i) = body.outs.binary_search_by_key(&v, |(ov, _)| *ov) {
                    live.push((v, body.outs[i].1.clone()));
                }
            }
            return Some((e.tx.clone(), live, false));
        }
        None
    }
}

#[inline]
fn checked_contains(checked: &[u32], v: u32) -> bool {
    checked.binary_search(&v).is_ok()
}

#[inline]
fn ensure_outs_sorted(mut outs: Vec<(u32, OutputRecord)>) -> Vec<(u32, OutputRecord)> {
    if outs.windows(2).any(|w| w[0].0 > w[1].0) {
        outs.sort_unstable_by_key(|(v, _)| *v);
    }
    outs
}

#[inline]
fn ensure_checked_sorted(mut checked: Vec<u32>) -> Vec<u32> {
    if checked.windows(2).any(|w| w[0] > w[1]) {
        checked.sort_unstable();
    }
    // Dedup only when needed (sorted unique from pin path is the common case).
    if checked.windows(2).any(|w| w[0] == w[1]) {
        checked.dedup();
    }
    checked
}

/// Merge sparse denserels by vout (prefer `src` rel when both present).
fn merge_spender_rels_into(dst: &mut Vec<(u32, u32)>, src: &[(u32, u32)]) {
    if src.is_empty() {
        return;
    }
    if dst.is_empty() {
        *dst = src.to_vec();
        return;
    }
    let mut m: U32Map<u32> = dst.iter().copied().collect();
    for &(v, r) in src {
        m.insert(v, r);
    }
    let mut merged: Vec<(u32, u32)> = m.into_iter().collect();
    merged.sort_unstable_by_key(|(v, _)| *v);
    *dst = merged;
}

/// Build sorted `(vout, rel)` for requested vouts from dense pin rels.
pub fn sparse_spender_rels(dense: &[u32], need_vouts: &[u32]) -> Vec<(u32, u32)> {
    let mut out = Vec::with_capacity(need_vouts.len());
    for &v in need_vouts {
        if let Some(&rel) = dense.get(v as usize) {
            if rel != SPENDER_REL_UNKNOWN {
                out.push((v, rel));
            }
        }
    }
    out
}

/// True when pin layout can supply abs spender meta for every `need_vout`.
pub fn layout_covers_need(
    body_range: Option<(u64, u64)>,
    sparse_rels: &[(u32, u32)],
    need_vouts: &[u32],
) -> bool {
    if body_range.is_none() || need_vouts.is_empty() {
        return body_range.is_some() && need_vouts.is_empty();
    }
    if sparse_rels.len() != need_vouts.len() {
        return false;
    }
    for (i, &v) in need_vouts.iter().enumerate() {
        if sparse_rels[i].0 != v || sparse_rels[i].1 == SPENDER_REL_UNKNOWN {
            return false;
        }
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use rbitcoin_primitives::Fk;
    use rbitcoin_store::{OutputRecord, TxRecord};

    fn tx(id: u8) -> TxRecord {
        let mut txid = [0u8; 32];
        txid[0] = id;
        TxRecord {
            txid,
            version: 1,
            locktime: 0,
            input_start_fk: Fk::NULL,
            input_count: 1,
            output_start_fk: Fk::NULL,
            output_count: 1,
        }
    }

    fn out(v: i64) -> OutputRecord {
        OutputRecord::unspent(v, vec![0x51])
    }

    #[test]
    fn extend_from_merges_disjoint_and_same_fk() {
        let mut a = BatchParents::new();
        a.insert_owned(
            Fk(1),
            tx(1),
            vec![(0, out(1))],
            vec![0],
            Some(false),
            Some((100, 50)),
            vec![(0, 10)],
        );
        let mut b = BatchParents::new();
        b.insert_owned(
            Fk(2),
            tx(2),
            vec![(0, out(2))],
            vec![0],
            Some(true),
            None,
            Vec::new(),
        );
        b.insert_owned(
            Fk(1),
            tx(1),
            vec![(1, out(3))],
            vec![1],
            None,
            None,
            vec![(1, 20)],
        );
        a.extend_from(b);
        assert_eq!(a.len(), 2);
        assert!(a.has_parent_out(Fk(1), 0));
        assert!(a.has_parent_out(Fk(1), 1));
        assert!(a.has_parent_out(Fk(2), 0));
        a.set_spent_range_only(Fk(1), (1000, 24));
        assert_eq!(a.get_spender_abs(Fk(1), 0), Some(1000));
        assert_eq!(a.get_spender_abs(Fk(1), 1), Some(1008));
        assert_eq!(a.get_parent_coinbase(Fk(1)), Some(false));
        assert_eq!(a.get_parent_coinbase(Fk(2)), Some(true));
    }

    #[test]
    fn insert_layout_coinbase_and_covered() {
        let mut bp = BatchParents::with_capacity(1);
        let live = vec![(0, out(42)), (2, out(99))];
        bp.insert_owned(
            Fk(9),
            tx(9),
            live,
            vec![0, 1, 2],
            Some(true),
            Some((1000, 200)),
            vec![(0, 50), (1, 70), (2, 90)],
        );
        assert_eq!(bp.len(), 1);
        assert!(bp.pin_covered(Fk(9), &[0, 1, 2]));
        assert!(!bp.pin_covered(Fk(9), &[0, 3]));
        assert!(!bp.has_parent_out(Fk(9), 1));
        bp.set_spent_range_only(Fk(9), (2000, 24));
        assert_eq!(bp.get_spender_abs(Fk(9), 2), Some(2016));
        assert_eq!(bp.get_body_range(Fk(9)), Some((1000, 200)));
        assert_eq!(bp.get_parent_coinbase(Fk(9)), Some(true));
        assert!(bp.has_abs_layout(Fk(9)));
        let (_, o) = bp.get_parent_out(Fk(9), 0).unwrap();
        assert_eq!(o.value, 42);
        let (v, script, parent_txid) = bp.get_parent_txout_parts(Fk(9), 0).unwrap();
        assert_eq!(v, 42);
        assert_eq!(script, &[0x51]);
        assert_eq!(parent_txid[0], 9);
        assert!(bp.get_parent_txout_parts(Fk(9), 1).is_none());
    }

    #[test]
    fn set_body_range_only_completes_layout_when_rels_present() {
        let mut bp = BatchParents::with_capacity(1);
        bp.insert_owned(
            Fk(3),
            tx(3),
            vec![(0, out(1))],
            vec![0],
            None,
            None,
            vec![(0, 40)],
        );
        assert!(!bp.has_abs_layout(Fk(3)));
        assert!(!bp.has_spender_rels(Fk(3)));
        bp.set_body_range_only(Fk(3), (500, 80));
        assert!(!bp.has_abs_layout(Fk(3)));
        bp.set_spent_range_only(Fk(3), (500, 16));
        assert!(bp.has_abs_layout(Fk(3)));
        assert_eq!(bp.get_spender_abs(Fk(3), 0), Some(500));
    }

    #[test]
    fn sparse_spender_rels_skips_unknown() {
        let dense = vec![10, SPENDER_REL_UNKNOWN, 30];
        let sparse = sparse_spender_rels(&dense, &[0, 1, 2]);
        assert_eq!(sparse, vec![(0, 10), (2, 30)]);
        assert!(!layout_covers_need(Some((0, 100)), &sparse, &[0, 1, 2]));
        assert!(layout_covers_need(
            Some((0, 100)),
            &[(0, 10), (2, 30)],
            &[0, 2]
        ));
        assert!(!layout_covers_need(None, &[(0, 10)], &[0]));
    }

    #[test]
    fn get_spender_abs_rejects_unknown_rel() {
        let mut bp = BatchParents::with_capacity(1);
        bp.insert_owned(
            Fk(1),
            tx(1),
            vec![(0, out(1))],
            vec![0],
            None,
            Some((100, 50)),
            vec![(0, SPENDER_REL_UNKNOWN)],
        );
        assert!(bp.get_spender_abs(Fk(1), 0).is_none());
        bp.set_spent_range_only(Fk(1), (100, 8));
        assert_eq!(bp.get_spender_abs(Fk(1), 0), Some(100));
        assert!(bp.get_spender_abs(Fk(1), 1).is_none());
    }

    /// Two batches with the same store share one SharedParentPin after publish/adopt.
    #[test]
    fn pipeline_store_shares_one_arc_across_batches() {
        let store = Arc::new(PipelineParentStore::new());
        let mut a = BatchParents::with_store(Arc::clone(&store), 4);
        let mut b = BatchParents::with_store(Arc::clone(&store), 4);
        a.insert_owned(
            Fk(7),
            tx(7),
            vec![(0, out(10))],
            vec![0],
            Some(false),
            None,
            vec![(0, 5)],
        );
        a.publish_to_store();
        b.adopt_from_store([7]);
        b.insert_owned(
            Fk(7),
            tx(7),
            vec![(1, out(20))],
            vec![1],
            None,
            None,
            Vec::new(),
        );
        b.publish_to_store();
        let pa = a.pins.get(&7).expect("a has pin");
        let pb = b.pins.get(&7).expect("b has pin");
        assert!(
            Arc::ptr_eq(pa, pb),
            "batches must share one SharedParentPin Arc after adopt"
        );
        assert!(a.has_parent_out(Fk(7), 0));
        assert!(a.has_parent_out(Fk(7), 1), "merged vout 1 visible via a");
        assert!(b.has_parent_out(Fk(7), 0), "merged vout 0 visible via b");
        assert!(b.has_parent_out(Fk(7), 1));
        assert_eq!(store.live_count(), 1);
        drop(a);
        assert_eq!(store.live_count(), 1, "b still holds pin");
        drop(b);
        assert_eq!(store.live_count(), 0, "last batch drop releases pin");
    }

    /// Concurrent local inserts then publish: loser merges into winner Arc.
    #[test]
    fn bulk_publish_merges_race_to_one_arc() {
        let store = Arc::new(PipelineParentStore::new());
        let mut a = BatchParents::with_store(Arc::clone(&store), 2);
        let mut b = BatchParents::with_store(Arc::clone(&store), 2);
        a.insert_owned(
            Fk(1),
            tx(1),
            vec![(0, out(1))],
            vec![0],
            None,
            None,
            vec![(0, 1)],
        );
        b.insert_owned(
            Fk(1),
            tx(1),
            vec![(1, out(2))],
            vec![1],
            None,
            None,
            vec![(1, 2)],
        );
        // a publishes first (wins Weak slot).
        a.publish_to_store();
        // b publishes: must merge into a's Arc and swap handle.
        b.publish_to_store();
        let pa = a.pins.get(&1).unwrap();
        let pb = b.pins.get(&1).unwrap();
        assert!(Arc::ptr_eq(pa, pb));
        assert!(a.has_parent_out(Fk(1), 0));
        assert!(a.has_parent_out(Fk(1), 1));
        assert!(b.has_parent_out(Fk(1), 0));
        assert!(b.has_parent_out(Fk(1), 1));
        assert_eq!(store.live_count(), 1);
    }

    /// extend_from must carry publish_ids so vacant pins from `other` still register.
    #[test]
    fn extend_from_merges_publish_ids_for_store_registration() {
        let store = Arc::new(PipelineParentStore::new());
        let mut a = BatchParents::with_store(Arc::clone(&store), 8);
        let mut b = BatchParents::with_store(Arc::clone(&store), 8);
        a.insert_owned(
            Fk(1),
            tx(1),
            vec![(0, out(1))],
            vec![0],
            Some(false),
            Some((8, 16)),
            vec![(0, 4)],
        );
        b.insert_owned(
            Fk(2),
            tx(2),
            vec![(0, out(2))],
            vec![0],
            Some(false),
            Some((24, 16)),
            vec![(0, 4)],
        );
        a.extend_from(b);
        assert_eq!(a.len(), 2);
        a.publish_to_store();
        assert_eq!(
            store.live_count(),
            2,
            "both fks must register: extend_from must merge publish_ids"
        );
        assert!(a.contains(Fk(1)));
        assert!(a.contains(Fk(2)));
    }

    /// Adopted pins are already Weak-registered; publish only registers vacant inserts.
    #[test]
    fn publish_registers_only_new_not_pure_adopt() {
        let store = Arc::new(PipelineParentStore::new());
        let mut seed = BatchParents::with_store(Arc::clone(&store), 16);
        for i in 1..=10u64 {
            seed.insert_owned(
                Fk(i),
                tx(i as u8),
                vec![(0, out(i as i64))],
                vec![0],
                Some(false),
                Some((i * 8, 16)),
                vec![(0, 4)],
            );
        }
        seed.publish_to_store();
        assert_eq!(store.live_count(), 10);

        let mut bp = BatchParents::with_store(Arc::clone(&store), 16);
        bp.adopt_from_store(1..=10);
        assert_eq!(bp.len(), 10);
        // Pure adopt: publish is a no-op (no new Arcs).
        bp.publish_to_store();
        assert_eq!(store.live_count(), 10);

        // Vacant insert of a new fk registers on publish.
        bp.insert_owned(
            Fk(99),
            tx(99),
            vec![(0, out(99))],
            vec![0],
            Some(false),
            Some((99 * 8, 16)),
            vec![(0, 4)],
        );
        bp.publish_to_store();
        assert_eq!(store.live_count(), 11);
        assert!(bp.contains(Fk(99)));
        // Second publish with no new inserts is free.
        bp.publish_to_store();
        assert_eq!(store.live_count(), 11);
    }

    /// Identity hasher for pack-scale u64 keys is the raw key (no SipHash mix).
    /// Write/lookup structural maps depend on this for the measured CPU win.
    #[test]
    fn u64_identity_hasher_is_raw_key_and_map_roundtrips_pack_scale() {
        // Shipped path: store identity maps re-exported here; drive U64Map API.
        use std::hash::Hasher;
        let mut h = U64IdentityHasher::default();
        h.write_u64(0xdead_beef_cafe_u64);
        assert_eq!(h.finish(), 0xdead_beef_cafe_u64);

        // Pack-scale create_fk map: sequential ids must insert + get without loss.
        let n = 8_000u64;
        let mut m: U64Map<u32> = U64Map::with_capacity_and_hasher(n as usize, Default::default());
        for i in 1..=n {
            m.insert(i, (i % 1_000_000) as u32);
        }
        assert_eq!(m.len(), n as usize);
        for i in 1..=n {
            assert_eq!(m.get(&i).copied(), Some((i % 1_000_000) as u32));
        }
        // Collisions / wrong finish would drop keys under open addressing.
        assert_eq!(m.get(&0), None);
        assert_eq!(m.get(&(n + 1)), None);
    }

    /// Free-plan insert must not require a store hit — vacant path is local only.
    #[test]
    fn insert_owned_local_without_publish_leaves_store_empty() {
        let store = Arc::new(PipelineParentStore::new());
        let mut bp = BatchParents::with_store(Arc::clone(&store), 8);
        for i in 1..=100u64 {
            bp.insert_owned(
                Fk(i),
                tx((i % 200) as u8),
                vec![(0, out(i as i64))],
                vec![0],
                Some(false),
                None,
                Vec::new(),
            );
        }
        assert_eq!(bp.len(), 100);
        assert_eq!(store.live_count(), 0, "insert must not touch store");
        bp.publish_to_store();
        assert_eq!(store.live_count(), 100);
    }

    /// Prep∥write on one SharedParentPin: write set_layout_for_need must not
    /// clobber checked need-vouts or denserels composed by concurrent prep.
    ///
    /// Bodies are immutable snapshots; concurrent compose publishes new Arcs
    /// under the pin lock so peer need-vouts and denserels survive.
    #[test]
    fn set_layout_merges_not_clobbers_under_concurrent_prep() {
        use std::sync::Barrier;
        use std::thread;

        let store = Arc::new(PipelineParentStore::new());
        let mut writer = BatchParents::with_store(Arc::clone(&store), 1);
        writer.insert_owned(
            Fk(1),
            tx(1),
            vec![(0, out(10))],
            vec![0],
            Some(false),
            None,
            vec![(0, 10)],
        );
        writer.publish_to_store();
        let pin = Arc::clone(writer.pins.get(&1).expect("shared pin"));

        let barrier = Arc::new(Barrier::new(2));
        let pin_prep = Arc::clone(&pin);
        let barrier_prep = Arc::clone(&barrier);
        let prep = thread::spawn(move || {
            barrier_prep.wait();
            // Concurrent prep batch spends vout 1 of the same create.
            for _ in 0..200 {
                pin_prep.merge_outs(vec![(1, out(20))], &[1]);
                pin_prep.maybe_merge_layout(None, &[(1, 20)]);
            }
        });

        barrier.wait();
        // Write-side layout fill for this batch's need (vout 0); dense has both outs.
        for _ in 0..200 {
            writer.set_layout_for_need(Fk(1), (100, 80), &[10, 20], &[0]);
        }
        prep.join().expect("prep thread");

        // Peer need-vout must still be covered (not wiped by stale checked replace).
        assert!(
            writer.pin_covered(Fk(1), &[0, 1]),
            "checked must keep prep-merged vout 1 after write set_layout"
        );
        assert!(writer.has_parent_out(Fk(1), 1));
        writer.set_spent_range_only(Fk(1), (1000, 24));
        assert_eq!(writer.get_spender_abs(Fk(1), 0), Some(1000));
        assert_eq!(
            writer.get_spender_abs(Fk(1), 1),
            Some(1008),
            "spent abs covers peer vout after merge"
        );
    }

    /// Timed synthetic: multi-pack insert + layout compose at few-block scale.
    /// Prints ns/op so IBD regressions are visible without a criterion harness.
    ///
    /// **Probe shape matches pre-recovery baseline** (single need-vout insert) so
    /// covered/layout can be compared to `bench-baseline-*.txt`. Extra phases:
    /// - layout2: second ensure (same `set_layout_for_need` API, no-op path)
    /// - assemble: sticky vs `get_parent_txout_parts_no_sticky` (same return path)
    #[test]
    fn pin_compose_multi_pack_timed() {
        let n_parents = 8_000usize; // ~input budget scale
        let store = Arc::new(PipelineParentStore::new());
        let t0 = std::time::Instant::now();
        let mut a = BatchParents::with_store(Arc::clone(&store), n_parents);
        for i in 1..=n_parents as u64 {
            a.insert_owned(
                Fk(i),
                tx((i % 200) as u8),
                vec![(0, out(i as i64))],
                vec![0],
                Some(false),
                None,
                vec![(0, 10)],
            );
        }
        a.publish_to_store();
        let insert_ns = t0.elapsed().as_nanos();

        // Covered re-insert (production free-pin share hit after adopt).
        let t_cov = std::time::Instant::now();
        let mut cov = BatchParents::with_store(Arc::clone(&store), n_parents);
        cov.adopt_from_store(1..=n_parents as u64);
        for i in 1..=n_parents as u64 {
            cov.insert_owned(
                Fk(i),
                tx((i % 200) as u8),
                vec![(0, out(i as i64))],
                vec![0],
                None,
                None,
                vec![(0, 10)],
            );
        }
        let covered_ns = t_cov.elapsed().as_nanos();

        // Layout-only fill (write ensure path) — same denserels shape as baseline.
        let t_lay = std::time::Instant::now();
        for i in 1..=n_parents as u64 {
            cov.set_layout_for_need(Fk(i), (i * 100, 50), &[10], &[]);
        }
        let layout_ns = t_lay.elapsed().as_nanos();

        // Second ensure pass — same API; already_covers short-circuit.
        let t_lay2 = std::time::Instant::now();
        for i in 1..=n_parents as u64 {
            cov.set_layout_for_need(Fk(i), (i * 100, 50), &[10], &[]);
        }
        let layout2_ns = t_lay2.elapsed().as_nanos();

        let t1 = std::time::Instant::now();
        let mut b = BatchParents::with_store(Arc::clone(&store), n_parents);
        b.adopt_from_store(1..=n_parents as u64);
        for i in 1..=n_parents as u64 {
            // Widen need + layout (compose publish on shared pins).
            b.insert_owned(
                Fk(i),
                tx((i % 200) as u8),
                vec![(1, out(i as i64 + 1))],
                vec![1],
                None,
                Some((i * 100, 50)),
                vec![(1, 20)],
            );
        }
        b.publish_to_store();
        let widen_ns = t1.elapsed().as_nanos();

        // Multi-input same-parent: vouts 0 and 1 after widen (shared Arc on `a`).
        // Fair cold = same `parent_txout_parts` path with sticky disabled.
        let reps = 10usize;
        let n_inputs = n_parents * reps * 2;
        let t_cold = std::time::Instant::now();
        let mut sum_c = 0i64;
        for p in 1..=n_parents as u64 {
            for _ in 0..reps {
                for vout in 0u32..2 {
                    if let Some((v, _, _)) = a.get_parent_txout_parts_no_sticky(Fk(p), vout) {
                        sum_c = sum_c.wrapping_add(v);
                    }
                }
            }
        }
        let assemble_cold_ns = t_cold.elapsed().as_nanos();
        let t_asm = std::time::Instant::now();
        let mut sum = 0i64;
        for p in 1..=n_parents as u64 {
            for _ in 0..reps {
                for vout in 0u32..2 {
                    if let Some((v, _, _)) = a.get_parent_txout_parts(Fk(p), vout) {
                        sum = sum.wrapping_add(v);
                    }
                }
            }
        }
        let assemble_ns = t_asm.elapsed().as_nanos();
        assert!(sum != 0 || n_inputs == 0);
        assert_eq!(sum, sum_c);

        assert_eq!(a.len(), n_parents);
        assert_eq!(b.len(), n_parents);
        assert!(a.pin_covered(Fk(1), &[0, 1]));
        a.set_spent_range_only(Fk(1), (1000, 24));
        assert_eq!(a.get_spender_abs(Fk(1), 1), Some(1008));
        let n = n_parents as f64;
        eprintln!(
            "pin_compose_multi_pack n={n_parents} \
             insert={:.1}ns/op covered={:.1}ns/op layout={:.1}ns/op layout2={:.1}ns/op \
             widen={:.1}ns/op assemble_sticky={:.1}ns/op assemble_nosticky={:.1}ns/op \
             (insert_ns={insert_ns} covered_ns={covered_ns} layout_ns={layout_ns} \
             layout2_ns={layout2_ns} widen_ns={widen_ns} assemble_ns={assemble_ns} \
             assemble_nosticky_ns={assemble_cold_ns} n_in={n_inputs})",
            insert_ns as f64 / n,
            covered_ns as f64 / n,
            layout_ns as f64 / n,
            layout2_ns as f64 / n,
            widen_ns as f64 / n,
            assemble_ns as f64 / n_inputs as f64,
            assemble_cold_ns as f64 / n_inputs as f64,
        );
        // Timing gates only for structural short-circuits (layout no-op, covered
        // vs widen). Sticky vs no-sticky assemble is printed for hosts/benches but
        // not asserted: alternating multi-vout walks often make sticky snap
        // overhead match or exceed cold under debug + parallel load (see
        // sticky_and_nosticky_txout_parts_match for functional equality).
        // Floor avoids inverting layout/covered when both are sub-ms noise.
        const TIMING_FLOOR_NS: u128 = 2_000_000; // 2ms
        if layout_ns > TIMING_FLOOR_NS {
            assert!(
                layout2_ns < layout_ns,
                "layout no-op must beat first ensure: layout={layout_ns} layout2={layout2_ns}"
            );
        }
        // Sanity bound: free-plan insert should stay well under 50µs/op even in debug.
        assert!(
            insert_ns / (n_parents as u128) < 50_000,
            "insert ns/op too high: {}",
            insert_ns / n_parents as u128
        );
        // Covered re-insert should be cheaper than real widen when both are hot.
        if widen_ns > TIMING_FLOOR_NS && covered_ns > TIMING_FLOOR_NS / 4 {
            assert!(
                covered_ns < widen_ns,
                "covered re-insert should beat widen: covered={covered_ns} widen={widen_ns}"
            );
        }
    }

    /// Sticky and no-sticky assemble APIs return identical prevout parts.
    #[test]
    fn sticky_and_nosticky_txout_parts_match() {
        let mut bp = BatchParents::new();
        bp.insert_owned(
            Fk(3),
            tx(3),
            vec![(0, out(11)), (1, out(22))],
            vec![0, 1],
            Some(false),
            None,
            Vec::new(),
        );
        for vout in [0u32, 1] {
            let s = bp.get_parent_txout_parts(Fk(3), vout).unwrap();
            let c = bp.get_parent_txout_parts_no_sticky(Fk(3), vout).unwrap();
            assert_eq!(s.0, c.0);
            assert_eq!(s.1, c.1);
            assert_eq!(s.2, c.2);
        }
    }

    /// Sticky outs: consecutive same-parent lookups share one Arc (no re-load).
    #[test]
    fn sticky_assemble_reuses_outs_arc() {
        let mut bp = BatchParents::new();
        bp.insert_owned(
            Fk(7),
            tx(7),
            vec![(0, out(10)), (1, out(20)), (2, out(30))],
            vec![0, 1, 2],
            Some(false),
            None,
            Vec::new(),
        );
        let (v0, s0, t0) = bp.get_parent_txout_parts(Fk(7), 0).unwrap();
        let (v1, s1, t1) = bp.get_parent_txout_parts(Fk(7), 1).unwrap();
        let (v2, s2, t2) = bp.get_parent_txout_parts(Fk(7), 2).unwrap();
        assert_eq!(v0, 10);
        assert_eq!(v1, 20);
        assert_eq!(v2, 30);
        assert_eq!(s0, vec![0x51]);
        assert_eq!(s1, vec![0x51]);
        assert_eq!(s2, vec![0x51]);
        assert_eq!(t0[0], 7);
        assert_eq!(t1[0], 7);
        assert_eq!(t2[0], 7);
        // Sticky holds parent 7.
        assert_eq!(bp.sticky_outs.borrow().as_ref().map(|(id, _)| *id), Some(7));
        // Switch parent clears sticky to new id.
        bp.insert_owned(
            Fk(8),
            tx(8),
            vec![(0, out(99))],
            vec![0],
            None,
            None,
            Vec::new(),
        );
        let _ = bp.get_parent_txout_parts(Fk(8), 0).unwrap();
        assert_eq!(bp.sticky_outs.borrow().as_ref().map(|(id, _)| *id), Some(8));
    }

    /// Pure share-hit refresh with empty meta is a no-op (no layout store).
    #[test]
    fn refresh_pin_meta_empty_is_noop() {
        let mut bp = BatchParents::new();
        bp.insert_owned(
            Fk(1),
            tx(1),
            vec![(0, out(1))],
            vec![0],
            Some(false),
            Some((100, 50)),
            vec![(0, 10)],
        );
        let pin = Arc::clone(bp.pins.get(&1).unwrap());
        let lay_before = pin.load_layout();
        bp.refresh_pin_meta(Fk(1), None, None, Vec::new());
        let lay_after = pin.load_layout();
        assert!(Arc::ptr_eq(&lay_before, &lay_after));
    }

    /// Pure compose helpers: widening need and layout builds new halves without
    /// mutating the source snapshots (AGENTS prefer-immutable).
    #[test]
    fn pin_body_compose_does_not_mutate_source() {
        let pin = SharedParentPin::new(
            Fk(1),
            tx(1),
            vec![(0, out(10))],
            vec![0],
            Some(false),
            Some((100, 50)),
            vec![(0, 10)],
        );
        let before_outs = pin.load_outs();
        let before_lay = pin.load_layout();
        pin.merge_outs(vec![(1, out(20))], &[1]);
        pin.maybe_merge_layout(None, &[(1, 20)]);
        let after_outs = pin.load_outs();
        let after_lay = pin.load_layout();
        // Source snapshots unchanged.
        assert_eq!(before_outs.outs.len(), 1);
        assert_eq!(before_outs.checked, vec![0]);
        assert_eq!(before_lay.spender_rels, vec![(0, 10)]);
        // Published halves have the union.
        assert_eq!(after_outs.outs.len(), 2);
        assert!(after_outs.covers_need(&[0, 1]));
        assert_eq!(after_lay.spender_rels, vec![(0, 10), (1, 20)]);
        assert!(
            !Arc::ptr_eq(&before_outs, &after_outs),
            "outs compose must publish new Arc"
        );
        assert!(
            !Arc::ptr_eq(&before_lay, &after_lay),
            "layout compose must publish new Arc"
        );
    }

    /// Covered share hit must not replace outs Arc (no full clone on no-op).
    #[test]
    fn covered_insert_keeps_outs_arc_identity() {
        let mut bp = BatchParents::new();
        bp.insert_owned(
            Fk(1),
            tx(1),
            vec![(0, out(10))],
            vec![0],
            Some(false),
            Some((100, 50)),
            vec![(0, 10)],
        );
        let pin = Arc::clone(bp.pins.get(&1).unwrap());
        let outs_before = pin.load_outs();
        let lay_before = pin.load_layout();
        // Same need already covered — free-pin share hit.
        bp.insert_owned(
            Fk(1),
            tx(1),
            vec![(0, out(10))],
            vec![0],
            None,
            Some((100, 50)),
            vec![(0, 10)],
        );
        let outs_after = pin.load_outs();
        let lay_after = pin.load_layout();
        assert!(
            Arc::ptr_eq(&outs_before, &outs_after),
            "no-op outs must keep Arc identity (no clone)"
        );
        assert!(
            Arc::ptr_eq(&lay_before, &lay_after),
            "no-op layout must keep Arc identity"
        );
    }

    /// Layout-only write must not replace outs Arc (scripts stay shared).
    #[test]
    fn layout_only_write_keeps_outs_arc() {
        let mut bp = BatchParents::new();
        bp.insert_owned(
            Fk(1),
            tx(1),
            vec![(0, out(10)), (1, out(20))],
            vec![0, 1],
            Some(false),
            None,
            vec![(0, 10)],
        );
        let pin = Arc::clone(bp.pins.get(&1).unwrap());
        let outs_before = pin.load_outs();
        bp.set_layout_for_need(Fk(1), (500, 80), &[10, 20], &[]);
        let outs_after = pin.load_outs();
        let lay = pin.load_layout();
        assert!(
            Arc::ptr_eq(&outs_before, &outs_after),
            "layout fill must not clone outs half"
        );
        assert_eq!(lay.body_range, Some((500, 80)));
        bp.set_spent_range_only(Fk(1), (800, 24));
        assert_eq!(bp.get_spender_abs(Fk(1), 1), Some(808));
    }

    /// PipelineParentStore size_snapshot / gc + set_layout_sparse / body_range_only edges.
    #[test]
    fn pipeline_store_snapshot_gc_and_layout_sparse_surface() {
        let store = Arc::new(PipelineParentStore::new());
        assert_eq!(store.size_snapshot(), (0, 0, 0));
        assert_eq!(store.live_count(), 0);
        store.gc_dead_weaks(); // empty map no-op

        let mut bp = BatchParents::with_store(Arc::clone(&store), 8);
        // Null / missing pin early-outs (set_layout_sparse + set_body_range_only).
        bp.set_layout_sparse(Fk::NULL, (0, 10), vec![(0, 1)], &[]);
        bp.set_layout_sparse(Fk(99), (0, 10), vec![(0, 1)], &[0]);
        bp.set_body_range_only(Fk::NULL, (1, 2));
        bp.set_body_range_only(Fk(99), (1, 2));
        bp.set_layout(Fk::NULL, (0, 1), &[1]);
        bp.set_layout_for_need(Fk(99), (0, 1), &[1], &[]);

        bp.insert_owned(
            Fk(1),
            tx(1),
            vec![(0, out(10)), (1, out(20))],
            vec![0],
            Some(false),
            None,
            Vec::new(),
        );
        bp.insert_owned(
            Fk(2),
            tx(2),
            vec![(0, out(30))],
            vec![0],
            Some(false),
            Some((100, 40)),
            vec![(0, 5)],
        );
        bp.publish_to_store();
        let (weak, live, bytes) = store.size_snapshot();
        assert_eq!(weak, 2);
        assert_eq!(live, 2);
        assert!(bytes > 0);

        // Grow checked via extra_need, then fill sparse layout.
        bp.set_layout_sparse(Fk(1), (200, 50), vec![(0, 7), (1, 8)], &[1]);
        assert!(bp.has_parent_out(Fk(1), 0));
        assert_eq!(bp.get_body_range(Fk(1)), Some((200, 50)));
        assert!(!bp.has_abs_layout(Fk(1)));
        assert!(!bp.has_spender_rels(Fk(1)));
        bp.set_spent_range_only(Fk(1), (200, 24));
        assert!(bp.has_abs_layout(Fk(1)));
        // No-op when layout already covers same range+rels.
        bp.set_layout_sparse(Fk(1), (200, 50), vec![(0, 7), (1, 8)], &[]);
        assert_eq!(bp.get_spender_abs(Fk(1), 1), Some(208));

        // set_body_range_only updates range; same range is no-op.
        bp.set_body_range_only(Fk(2), (300, 60));
        assert_eq!(bp.get_body_range(Fk(2)), Some((300, 60)));
        bp.set_body_range_only(Fk(2), (300, 60));
        assert_eq!(bp.get_body_range(Fk(2)), Some((300, 60)));

        // Drop all strong refs → Weaks die; gc shrinks map.
        drop(bp);
        assert_eq!(store.live_count(), 0);
        let (weak_dead, live_dead, _) = store.size_snapshot();
        assert_eq!(live_dead, 0);
        assert!(weak_dead >= 2, "dead Weaks still occupy slots until gc");
        store.gc_dead_weaks();
        let (weak_after, live_after, bytes_after) = store.size_snapshot();
        assert_eq!(weak_after, 0);
        assert_eq!(live_after, 0);
        assert_eq!(bytes_after, 0);
    }

    /// has_abs_layout / has_spender_rels null and missing pins.
    #[test]
    fn has_layout_helpers_null_and_missing() {
        let bp = BatchParents::new();
        assert!(!bp.has_abs_layout(Fk::NULL));
        assert!(!bp.has_abs_layout(Fk(1)));
        assert!(!bp.has_spender_rels(Fk::NULL));
        assert!(!bp.has_spender_rels(Fk(1)));
        assert!(bp.get_parent_tx(Fk::NULL).is_none());
        assert!(bp.get_parent_coinbase(Fk::NULL).is_none());
        assert!(bp.get_body_range(Fk::NULL).is_none());
    }

    #[test]
    fn pipeline_parent_store_lookup_txid() {
        let store = Arc::new(PipelineParentStore::new());
        let mut bp = BatchParents::with_store(Arc::clone(&store), 1);
        let tid = tx(7).txid;
        bp.insert_owned(
            Fk(42),
            tx(7),
            vec![(0, out(1))],
            vec![0],
            Some(false),
            Some((1000, 80)),
            Vec::new(),
        );
        bp.publish_to_store();
        assert_eq!(
            store.lookup_txid(&tid),
            Some((Fk(42), (1000, 80))),
            "live pin with range must resolve by txid"
        );
        let mut zero = tx(8);
        zero.txid = [0u8; 32];
        let mut bp0 = BatchParents::with_store(Arc::clone(&store), 1);
        bp0.insert_owned(
            Fk(43),
            zero,
            vec![(0, out(1))],
            vec![0],
            Some(false),
            Some((2000, 10)),
            Vec::new(),
        );
        bp0.publish_to_store();
        assert!(
            store.lookup_txid(&[0u8; 32]).is_none(),
            "zero txid is never indexed"
        );
        drop(bp);
        drop(bp0);
        store.gc_dead_weaks();
        assert!(
            store.lookup_txid(&tid).is_none(),
            "txid index must die with the last pin Arc"
        );
    }

    /// One lock: live + range hits; dead Weak, missing range, zero txid, and
    /// unknown keys miss — same as N× [`PipelineParentStore::lookup_txid`].
    #[test]
    fn pipeline_parent_store_bulk_lookup_txid() {
        let store = Arc::new(PipelineParentStore::new());
        let live_tid = tx(1).txid;
        let dead_tid = tx(2).txid;
        let no_range_tid = tx(3).txid;
        let missing_tid = tx(4).txid;
        let zero = [0u8; 32];

        let mut live = BatchParents::with_store(Arc::clone(&store), 2);
        live.insert_owned(
            Fk(10),
            tx(1),
            vec![(0, out(1))],
            vec![0],
            Some(false),
            Some((1000, 80)),
            Vec::new(),
        );
        let mut no_range = BatchParents::with_store(Arc::clone(&store), 1);
        no_range.insert_owned(
            Fk(11),
            tx(3),
            vec![(0, out(1))],
            vec![0],
            Some(false),
            None,
            Vec::new(),
        );
        let mut dead = BatchParents::with_store(Arc::clone(&store), 1);
        dead.insert_owned(
            Fk(12),
            tx(2),
            vec![(0, out(1))],
            vec![0],
            Some(false),
            Some((2000, 40)),
            Vec::new(),
        );
        live.publish_to_store();
        no_range.publish_to_store();
        dead.publish_to_store();
        drop(dead);

        let keys = [live_tid, dead_tid, no_range_tid, missing_tid, zero];
        let expected: Vec<Option<(Fk, (u64, u64))>> =
            keys.iter().map(|t| store.lookup_txid(t)).collect();
        let bulk = store.bulk_lookup_txid(keys.iter());
        for (t, exp) in keys.iter().zip(expected.iter()) {
            assert_eq!(
                bulk.get(t).copied(),
                *exp,
                "bulk must match per-key lookup for {t:?}"
            );
        }
        assert_eq!(bulk.get(&live_tid).copied(), Some((Fk(10), (1000, 80))));
        assert!(bulk.get(&dead_tid).is_none());
        assert!(bulk.get(&no_range_tid).is_none());
        assert!(bulk.get(&missing_tid).is_none());
        assert!(bulk.get(&zero).is_none());
        let _keep = (live, no_range);
    }
}
