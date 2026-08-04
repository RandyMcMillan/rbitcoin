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
//! **Immutable publish:** outs + layout live in an immutable [`PinBody`] snapshot
//! (`Arc` under a short RwLock). Widening need-vouts or layout **composes a new
//! body and swaps the Arc** — never `push`/mutate shared vectors in place
//! (AGENTS: prefer immutable data + composition).
//!
//! **Sparse:** only spent need-vouts + layout fields write/assemble need (not
//! full parent output sets). Vout merge when a later batch spends more outs.
//!
//! Create heights are not stashed — write re-reads Class C `tx_height`.

use rbitcoin_primitives::Fk;
use rbitcoin_store::{OutputRecord, TxRecord};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Arc, Mutex, RwLock, Weak};

/// Relative offset sentinel: layout unknown for this out.
pub const SPENDER_REL_UNKNOWN: u32 = u32::MAX;

const CB_UNKNOWN: u8 = 0;
const CB_FALSE: u8 = 1;
const CB_TRUE: u8 = 2;

/// Body range + sparse denserels for abs spender meta (write-filled).
#[derive(Debug, Clone, Default)]
struct ParentLayout {
    body_range: Option<(u64, u64)>,
    spender_rels: Vec<(u32, u32)>,
}

/// Immutable sparse outs + layout snapshot (compose → publish, never mutate).
#[derive(Debug, Clone)]
struct PinBody {
    outs: Vec<(u32, OutputRecord)>,
    checked: Vec<u32>,
    layout: ParentLayout,
}

impl PinBody {
    fn with_outs_layout(
        live: Vec<(u32, OutputRecord)>,
        checked: Vec<u32>,
        body_range: Option<(u64, u64)>,
        spender_rels: Vec<(u32, u32)>,
    ) -> Self {
        Self {
            outs: ensure_outs_sorted(live),
            checked: ensure_checked_sorted(checked),
            layout: ParentLayout {
                body_range,
                spender_rels,
            },
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
            .all(|(v, _)| self.outs.iter().any(|(dv, _)| dv == v))
    }

    /// Compose wider need coverage (new body; does not mutate `self`).
    fn compose_outs(&self, live: Vec<(u32, OutputRecord)>, checked: &[u32]) -> Self {
        let mut outs = self.outs.clone();
        for (v, o) in live {
            if !outs.iter().any(|(dv, _)| *dv == v) {
                outs.push((v, o));
            }
        }
        outs.sort_unstable_by_key(|(v, _)| *v);
        let mut ch = self.checked.clone();
        ch.extend_from_slice(checked);
        ch.sort_unstable();
        ch.dedup();
        Self {
            outs,
            checked: ch,
            layout: self.layout.clone(),
        }
    }

    /// Compose layout overlay (new body; does not mutate `self`).
    fn compose_layout(
        &self,
        body_range: Option<(u64, u64)>,
        spender_rels: &[(u32, u32)],
    ) -> Self {
        let mut layout = self.layout.clone();
        if layout.body_range.is_none() {
            layout.body_range = body_range;
        }
        merge_spender_rels_into(&mut layout.spender_rels, spender_rels);
        Self {
            outs: self.outs.clone(),
            checked: self.checked.clone(),
            layout,
        }
    }

    fn layout_already_covers(
        &self,
        body_range: Option<(u64, u64)>,
        spender_rels: &[(u32, u32)],
    ) -> bool {
        let range_done = body_range.is_none() || self.layout.body_range.is_some();
        let rels_done = spender_rels.is_empty()
            || spender_rels.iter().all(|(v, r)| {
                self.layout
                    .spender_rels
                    .binary_search_by_key(v, |(dv, _)| *dv)
                    .ok()
                    .is_some_and(|i| self.layout.spender_rels[i].1 == *r)
            });
        range_done && rels_done
    }
}

/// One create's sparse pin payload, shared across concurrent pipeline batches.
///
/// Identity (`Arc` of this type) stays stable for writeq unique-parent metering.
/// Payload fields are published as immutable [`PinBody`] snapshots.
#[derive(Debug)]
pub struct SharedParentPin {
    tx: TxRecord,
    /// 0 unknown, 1 not coinbase, 2 coinbase.
    coinbase: AtomicU8,
    /// Published immutable snapshot. Compose + Arc-swap under short write lock.
    body: RwLock<Arc<PinBody>>,
}

impl SharedParentPin {
    fn new(
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
            tx,
            coinbase: AtomicU8::new(cb),
            body: RwLock::new(Arc::new(PinBody::with_outs_layout(
                live,
                checked,
                body_range,
                spender_rels,
            ))),
        }
    }

    /// Snapshot clone (cheap Arc bump under read lock).
    #[inline]
    fn load_body(&self) -> Arc<PinBody> {
        Arc::clone(
            &self
                .body
                .read()
                .unwrap_or_else(|e| e.into_inner()),
        )
    }

    /// Compose from current body under write lock; publish new Arc (never mutate body in place).
    fn publish_compose(&self, f: impl FnOnce(&PinBody) -> PinBody) {
        let mut g = self.body.write().unwrap_or_else(|e| e.into_inner());
        let next = f(g.as_ref());
        *g = Arc::new(next);
    }

    /// True when all `need` vouts are already in checked.
    #[inline]
    fn covers_need(&self, need: &[u32]) -> bool {
        self.load_body().covers_need(need)
    }

    fn merge_outs(&self, live: Vec<(u32, OutputRecord)>, checked: &[u32]) {
        // Shared-hit fast path: peer batch already pinned these vouts — no write lock.
        let snap = self.load_body();
        if snap.covers_need(checked) && (live.is_empty() || snap.has_all_live(&live)) {
            return;
        }
        self.publish_compose(|cur| {
            // Re-check under write lock (concurrent compose may have filled need).
            if !checked.is_empty()
                && cur.covers_need(checked)
                && (live.is_empty() || cur.has_all_live(&live))
            {
                return cur.clone();
            }
            cur.compose_outs(live, checked)
        });
    }

    fn set_coinbase_if_known(&self, coinbase: Option<bool>) {
        if let Some(b) = coinbase {
            let v = if b { CB_TRUE } else { CB_FALSE };
            let _ = self.coinbase.compare_exchange(
                CB_UNKNOWN,
                v,
                Ordering::Relaxed,
                Ordering::Relaxed,
            );
        }
    }

    fn coinbase_opt(&self) -> Option<bool> {
        match self.coinbase.load(Ordering::Relaxed) {
            CB_TRUE => Some(true),
            CB_FALSE => Some(false),
            _ => None,
        }
    }

    /// Publish layout only when something new is present (skip write lock on hits).
    fn maybe_merge_layout(&self, body_range: Option<(u64, u64)>, spender_rels: &[(u32, u32)]) {
        if body_range.is_none() && spender_rels.is_empty() {
            return;
        }
        let snap = self.load_body();
        if snap.layout_already_covers(body_range, spender_rels) {
            return;
        }
        self.publish_compose(|cur| {
            if cur.layout_already_covers(body_range, spender_rels) {
                return cur.clone();
            }
            cur.compose_layout(body_range, spender_rels)
        });
    }
}

/// Prep-time registry: Weak map so dead pins free when last batch Arc drops.
///
/// Mutex is only for bulk adopt / publish of Arc handles — never held while
/// assemble walks inputs or write fills layout data, and **not** on the
/// per-parent insert hot path.
#[derive(Debug, Default)]
pub struct PipelineParentStore {
    by_fk: Mutex<HashMap<u64, Weak<SharedParentPin>>>,
}

impl PipelineParentStore {
    pub fn new() -> Self {
        Self {
            by_fk: Mutex::new(HashMap::new()),
        }
    }

    /// Live strong pins still reachable via Weak (diagnostics / tests).
    pub fn live_count(&self) -> usize {
        let g = self.by_fk.lock().unwrap_or_else(|e| e.into_inner());
        g.values().filter(|w| w.strong_count() > 0).count()
    }

    /// One lock: upgrade live pins for `ids` into a map (prep batch start).
    pub fn bulk_upgrade(&self, ids: impl IntoIterator<Item = u64>) -> HashMap<u64, Arc<SharedParentPin>> {
        let g = self.by_fk.lock().unwrap_or_else(|e| e.into_inner());
        let mut out = HashMap::new();
        for id in ids {
            if let Some(p) = g.get(&id).and_then(|w| w.upgrade()) {
                out.insert(id, p);
            }
        }
        out
    }

    /// One lock: publish batch pins as Weaks. On Arc identity conflict (peer
    /// batch won the slot), merge local sparse fields into the existing Arc and
    /// replace the batch handle so both batches share one payload.
    pub fn bulk_publish(&self, pins: &mut HashMap<u64, Arc<SharedParentPin>>) {
        // Phase 1 under lock: insert vacant Weaks; collect conflicts to merge outside.
        let mut conflicts: Vec<(u64, Arc<SharedParentPin>, Arc<SharedParentPin>)> = Vec::new();
        {
            let mut g = self.by_fk.lock().unwrap_or_else(|e| e.into_inner());
            for (&id, pin) in pins.iter() {
                match g.get(&id).and_then(|w| w.upgrade()) {
                    Some(existing) if !Arc::ptr_eq(&existing, pin) => {
                        conflicts.push((id, existing, Arc::clone(pin)));
                    }
                    Some(_) => {}
                    None => {
                        g.insert(id, Arc::downgrade(pin));
                    }
                }
            }
            if g.len() > 4096 {
                g.retain(|_, w| w.strong_count() > 0);
            }
        }
        // Phase 2 outside lock: compose local → existing body, swap batch handle.
        for (id, existing, local) in conflicts {
            let src = local.load_body();
            existing.merge_outs(src.outs.clone(), &src.checked);
            existing.set_coinbase_if_known(local.coinbase_opt());
            existing.maybe_merge_layout(src.layout.body_range, &src.layout.spender_rels);
            pins.insert(id, existing);
        }
    }
}

/// Per-batch handle map: `create_fk → Arc` shared pin (refcount only on clone).
#[derive(Debug, Default, Clone)]
pub struct BatchParents {
    /// Optional pipeline store for sharing across concurrent batches.
    store: Option<Arc<PipelineParentStore>>,
    pins: HashMap<u64, Arc<SharedParentPin>>,
}

impl BatchParents {
    pub fn new() -> Self {
        Self {
            store: None,
            pins: HashMap::new(),
        }
    }

    pub fn with_capacity(n: usize) -> Self {
        Self {
            store: None,
            pins: HashMap::with_capacity(n),
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
            pins: HashMap::with_capacity(capacity),
        }
    }

    pub fn len(&self) -> usize {
        self.pins.len()
    }

    pub fn is_empty(&self) -> bool {
        self.pins.is_empty()
    }

    /// Stable payload identity for unique writeq occupancy metering.
    #[inline]
    pub fn parent_payload_ptrs(&self) -> impl Iterator<Item = usize> + '_ {
        self.pins
            .values()
            .map(|a| Arc::as_ptr(a) as usize)
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

    /// Bulk-publish local pins into the pipeline store (one store lock). Call after pin fill.
    pub fn publish_to_store(&mut self) {
        let Some(store) = &self.store else {
            return;
        };
        store.bulk_publish(&mut self.pins);
    }

    /// Layout / coinbase only when outs already cover need (cross-batch share hit).
    #[inline]
    pub fn refresh_pin_meta(
        &mut self,
        fk: Fk,
        coinbase: Option<bool>,
        body_range: Option<(u64, u64)>,
        spender_rels: Vec<(u32, u32)>,
    ) {
        let Some(id) = fk.get() else {
            return;
        };
        let Some(p) = self.pins.get(&id) else {
            return;
        };
        p.set_coinbase_if_known(coinbase);
        p.maybe_merge_layout(body_range, &spender_rels);
    }

    /// Insert / merge one parent (prep pin hot path).
    ///
    /// **No store mutex** — pure batch HashMap. First insert for an id is the
    /// pre-share cost class (`ParentEntry` put). Merge only if the same batch
    /// already holds a partial pin (or after adopt left an incomplete cover).
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
                if !p.covers_need(&checked) {
                    p.merge_outs(live, &checked);
                }
                p.set_coinbase_if_known(coinbase);
                p.maybe_merge_layout(body_range, &spender_rels);
            }
            std::collections::hash_map::Entry::Vacant(v) => {
                v.insert(Arc::new(SharedParentPin::new(
                    tx,
                    live,
                    checked,
                    coinbase,
                    body_range,
                    spender_rels,
                )));
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
        let body = e.load_body();
        let o = body.outs.iter().find(|(v, _)| *v == vout)?;
        Some((e.tx.clone(), o.1.clone()))
    }

    /// Assemble hot path: value + script bytes + parent txid (script owned from
    /// immutable pin body snapshot).
    #[inline]
    pub fn get_parent_txout_parts(&self, fk: Fk, vout: u32) -> Option<(i64, Vec<u8>, [u8; 32])> {
        let id = fk.get()?;
        let e = self.pins.get(&id)?;
        let body = e.load_body();
        let (_, o) = body.outs.iter().find(|(v, _)| *v == vout)?;
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
        e.load_body().layout.body_range
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
        // Compose new body under publish lock: extend checked with extra_need
        // and merge denserels (never replace with a stale full-vector snapshot —
        // concurrent prep compose_outs can add peer need-vouts).
        e.publish_compose(|cur| {
            let mut next = cur.clone();
            if next.checked.is_empty() && extra_need.is_empty() && !dense_rels.is_empty() {
                next.checked = (0..dense_rels.len() as u32).collect();
            }
            next.checked.extend_from_slice(extra_need);
            next.checked.sort_unstable();
            next.checked.dedup();
            let sparse = sparse_spender_rels(dense_rels, &next.checked);
            next.layout.body_range = Some(body_range);
            merge_spender_rels_into(&mut next.layout.spender_rels, &sparse);
            next
        });
    }

    pub fn set_body_range_only(&mut self, fk: Fk, body_range: (u64, u64)) {
        let Some(id) = fk.get() else {
            return;
        };
        if let Some(e) = self.pins.get(&id) {
            e.publish_compose(|cur| {
                let mut next = cur.clone();
                next.layout.body_range = Some(body_range);
                next
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
        e.publish_compose(|cur| {
            let mut next = cur.clone();
            if !extra_need.is_empty() {
                next.checked.extend_from_slice(extra_need);
                next.checked.sort_unstable();
                next.checked.dedup();
            }
            next.layout.body_range = Some(body_range);
            merge_spender_rels_into(&mut next.layout.spender_rels, &sparse_rels);
            next
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
        let lay = &e.load_body().layout;
        lay.body_range.is_some() && !lay.spender_rels.is_empty()
    }

    #[inline]
    pub fn has_spender_rels(&self, fk: Fk) -> bool {
        let Some(id) = fk.get() else {
            return false;
        };
        let Some(e) = self.pins.get(&id) else {
            return false;
        };
        !e.load_body().layout.spender_rels.is_empty()
    }

    pub fn fks_missing_layout(&self) -> Vec<Fk> {
        self.pins
            .iter()
            .filter(|(_, e)| {
                let lay = &e.load_body().layout;
                lay.body_range.is_none() || lay.spender_rels.is_empty()
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
        let lay = &e.load_body().layout;
        let (off, _) = lay.body_range?;
        let i = lay
            .spender_rels
            .binary_search_by_key(&vout, |(v, _)| *v)
            .ok()?;
        let rel = lay.spender_rels[i].1;
        if rel == SPENDER_REL_UNKNOWN {
            return None;
        }
        Some(off.saturating_add(u64::from(rel)))
    }

    pub fn has_parent_out(&self, fk: Fk, vout: u32) -> bool {
        let Some(id) = fk.get() else {
            return false;
        };
        let Some(e) = self.pins.get(&id) else {
            return false;
        };
        e.load_body().outs.iter().any(|(v, _)| *v == vout)
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

    /// Absorb another batch's handles (write megabatch). Same create → keep one Arc
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
        for (id, src) in other.pins {
            match self.pins.entry(id) {
                std::collections::hash_map::Entry::Vacant(v) => {
                    v.insert(src);
                }
                std::collections::hash_map::Entry::Occupied(o) => {
                    // Same Arc or two Arcs for same fk (no store) — compose into one.
                    if !Arc::ptr_eq(o.get(), &src) {
                        let body = src.load_body();
                        o.get().merge_outs(body.outs.clone(), &body.checked);
                        o.get().set_coinbase_if_known(src.coinbase_opt());
                        o.get().maybe_merge_layout(
                            body.layout.body_range,
                            &body.layout.spender_rels,
                        );
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
        let body = e.load_body();
        let covered =
            !body.checked.is_empty() && vouts.iter().all(|v| checked_contains(&body.checked, *v));
        if covered {
            let mut live = Vec::with_capacity(vouts.len());
            for &v in vouts {
                if let Some((_, o)) = body.outs.iter().find(|(ov, _)| *ov == v) {
                    live.push((v, o.clone()));
                }
            }
            return Some((e.tx.clone(), live, true));
        }
        if !body.outs.is_empty() && vouts.iter().all(|v| body.outs.iter().any(|(ov, _)| ov == v)) {
            let mut live = Vec::with_capacity(vouts.len());
            for &v in vouts {
                if let Some((_, o)) = body.outs.iter().find(|(ov, _)| *ov == v) {
                    live.push((v, o.clone()));
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
    let mut m: HashMap<u32, u32> = dst.iter().copied().collect();
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
        assert_eq!(a.get_spender_abs(Fk(1), 0), Some(110));
        assert_eq!(a.get_spender_abs(Fk(1), 1), Some(120));
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
        assert_eq!(bp.get_spender_abs(Fk(9), 2), Some(1090));
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
        assert!(bp.has_spender_rels(Fk(3)));
        bp.set_body_range_only(Fk(3), (500, 80));
        assert!(bp.has_abs_layout(Fk(3)));
        assert_eq!(bp.get_spender_abs(Fk(3), 0), Some(540));
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
        a.insert_owned(Fk(1), tx(1), vec![(0, out(1))], vec![0], None, None, vec![(0, 1)]);
        b.insert_owned(Fk(1), tx(1), vec![(1, out(2))], vec![1], None, None, vec![(1, 2)]);
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

    #[test]
    fn parent_payload_ptrs_stable_for_unique_metering() {
        let store = Arc::new(PipelineParentStore::new());
        let mut a = BatchParents::with_store(Arc::clone(&store), 2);
        let mut b = BatchParents::with_store(Arc::clone(&store), 2);
        a.insert_owned(Fk(1), tx(1), vec![(0, out(1))], vec![0], None, None, vec![]);
        a.publish_to_store();
        b.adopt_from_store([1]);
        b.insert_owned(Fk(1), tx(1), vec![(0, out(1))], vec![0], None, None, vec![]);
        b.publish_to_store();
        let pa: Vec<_> = a.parent_payload_ptrs().collect();
        let pb: Vec<_> = b.parent_payload_ptrs().collect();
        assert_eq!(pa, pb);
        assert_eq!(pa.len(), 1);
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
        assert_eq!(writer.get_spender_abs(Fk(1), 0), Some(110));
        // Peer denserels for vout 1 must survive (not full-replaced away).
        assert_eq!(
            writer.get_spender_abs(Fk(1), 1),
            Some(120),
            "spender_rels must merge, not replace, peer denserels"
        );
    }

    /// Timed synthetic: multi-pack insert + layout compose at few-block scale.
    /// Prints ns/op so IBD regressions are visible without a criterion harness.
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

        assert_eq!(a.len(), n_parents);
        assert_eq!(b.len(), n_parents);
        assert!(a.pin_covered(Fk(1), &[0, 1]));
        assert_eq!(a.get_spender_abs(Fk(1), 1), Some(120));
        eprintln!(
            "pin_compose_multi_pack n={n_parents} insert_ns={insert_ns} ({:.1} ns/op) widen_ns={widen_ns} ({:.1} ns/op)",
            insert_ns as f64 / n_parents as f64,
            widen_ns as f64 / n_parents as f64,
        );
        // Sanity bound: free-plan insert should stay well under 50µs/op even in debug.
        assert!(
            insert_ns / (n_parents as u128) < 50_000,
            "insert ns/op too high: {}",
            insert_ns / n_parents as u128
        );
    }

    /// Pure compose helpers: widening need and layout builds a new body without
    /// mutating the source snapshot (AGENTS prefer-immutable).
    #[test]
    fn pin_body_compose_does_not_mutate_source() {
        let pin = SharedParentPin::new(
            tx(1),
            vec![(0, out(10))],
            vec![0],
            Some(false),
            Some((100, 50)),
            vec![(0, 10)],
        );
        let before = pin.load_body();
        pin.merge_outs(vec![(1, out(20))], &[1]);
        pin.maybe_merge_layout(None, &[(1, 20)]);
        let after = pin.load_body();
        // Source snapshot unchanged.
        assert_eq!(before.outs.len(), 1);
        assert_eq!(before.checked, vec![0]);
        assert_eq!(before.layout.spender_rels, vec![(0, 10)]);
        // Published body has the union.
        assert_eq!(after.outs.len(), 2);
        assert!(after.covers_need(&[0, 1]));
        assert_eq!(after.layout.spender_rels, vec![(0, 10), (1, 20)]);
        assert!(!Arc::ptr_eq(&before, &after), "compose must publish new Arc");
    }
}
