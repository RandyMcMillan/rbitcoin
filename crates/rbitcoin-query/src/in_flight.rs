//! Immutable prep-ahead in-flight create material (confirm pipeline Phase A).
//!
//! **Publish model:** each planned pack becomes an [`InFlightLayer`] (frozen
//! maps of create pins). The plan thread appends layers to an [`InFlightLog`]
//! and hands prep an [`InFlightView`] (Arc slice of layers). Prior layers are
//! never mutated — no `Arc::make_mut` of a shared whole-map while prep holds a
//! snapshot.
//!
//! **Prune:** drop whole layers with `max_fk ≤ head_occupied`; rebuild only a
//! straddling layer as a **new** Arc (body-ahead-of-head seal window).
//!
//! Lookup is newest→oldest scan over layers (O(L)); pack counts are small and
//! L is bounded by pipeline queue depth.

use crate::archive::CreatePin;
use rbitcoin_primitives::Fk;
use std::collections::HashMap;
use std::sync::Arc;

/// One planned pack's published creates (immutable after construction).
#[derive(Debug, Clone)]
pub struct InFlightLayer {
    creates: HashMap<[u8; 32], Fk>,
    outs: HashMap<u64, CreatePin>,
    /// Highest create fk id in this layer (whole-layer prune fast path).
    max_fk: u64,
}

impl InFlightLayer {
    /// Build from planned fks + batch_pin (or packed pin half) Arc clones.
    pub fn from_plan_pins<'a>(pins: impl IntoIterator<Item = (Fk, &'a CreatePin)>) -> Self {
        let mut creates = HashMap::new();
        let mut outs = HashMap::new();
        let mut max_fk = 0u64;
        for (fk, pin) in pins {
            creates.insert(pin.0.txid, fk);
            if let Some(id) = fk.get() {
                max_fk = max_fk.max(id);
                outs.insert(id, Arc::clone(pin));
            }
        }
        Self {
            creates,
            outs,
            max_fk,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.outs.is_empty()
    }

    pub fn max_fk(&self) -> u64 {
        self.max_fk
    }

    pub fn outs_len(&self) -> usize {
        self.outs.len()
    }
}

/// Plan-owned append-only log of immutable layers.
#[derive(Debug, Default, Clone)]
pub struct InFlightLog {
    layers: Vec<Arc<InFlightLayer>>,
}

impl InFlightLog {
    pub fn new() -> Self {
        Self { layers: Vec::new() }
    }

    /// Publish one pack. Does not mutate existing layers.
    pub fn note_layer(&mut self, layer: InFlightLayer) {
        if layer.is_empty() {
            return;
        }
        self.layers.push(Arc::new(layer));
    }

    /// Drop create material with fk ≤ `head_occupied`.
    ///
    /// Whole layers with `max_fk ≤ head_occupied` are dropped. Layers that
    /// straddle the cutoff are replaced with a **new** Arc containing only
    /// body-ahead entries (never `make_mut` on a shared layer).
    pub fn prune(&mut self, head_occupied: u64) {
        if self.layers.is_empty() {
            return;
        }
        let mut next = Vec::with_capacity(self.layers.len());
        for layer in self.layers.drain(..) {
            if layer.max_fk <= head_occupied {
                continue;
            }
            let any_old = layer.outs.keys().any(|&id| id <= head_occupied);
            if !any_old {
                next.push(layer);
                continue;
            }
            let mut creates = HashMap::new();
            let mut outs = HashMap::new();
            let mut max_fk = 0u64;
            for (txid, fk) in &layer.creates {
                if fk.get().is_some_and(|id| id > head_occupied) {
                    creates.insert(*txid, *fk);
                }
            }
            for (&id, pin) in &layer.outs {
                if id > head_occupied {
                    max_fk = max_fk.max(id);
                    outs.insert(id, Arc::clone(pin));
                }
            }
            if outs.is_empty() {
                continue;
            }
            next.push(Arc::new(InFlightLayer {
                creates,
                outs,
                max_fk,
            }));
        }
        self.layers = next;
        self.layers.shrink_to_fit();
    }

    pub fn clear(&mut self) {
        self.layers.clear();
        self.layers.shrink_to_fit();
    }

    /// Prep-facing snapshot: Arc bumps only (no map clone).
    pub fn snapshot(&self) -> InFlightView {
        let layers: Arc<[Arc<InFlightLayer>]> = self.layers.iter().cloned().collect();
        InFlightView { layers }
    }

    pub fn layer_count(&self) -> usize {
        self.layers.len()
    }

    pub fn entry_count(&self) -> usize {
        self.layers.iter().map(|l| l.outs.len()).sum()
    }

    /// Occupancy for IBD `sizes`: layers, create-pin entries, approx payload bytes.
    pub fn size_snapshot(&self) -> (usize, usize, u64) {
        let mut entries = 0usize;
        let mut bytes = 0u64;
        for layer in &self.layers {
            entries = entries.saturating_add(layer.outs.len());
            for pin in layer.outs.values() {
                bytes = bytes.saturating_add(crate::archive::create_pin_approx_bytes(pin) as u64);
            }
            // HashMap overhead (rough): ~32 B / entry for creates + outs keys.
            bytes = bytes.saturating_add(
                (layer.outs.len().saturating_add(layer.creates.len()) as u64).saturating_mul(40),
            );
        }
        (self.layers.len(), entries, bytes)
    }
}

/// Immutable prep/plan view over published layers (newest→oldest lookup).
#[derive(Debug, Clone)]
pub struct InFlightView {
    layers: Arc<[Arc<InFlightLayer>]>,
}

impl Default for InFlightView {
    fn default() -> Self {
        Self::empty()
    }
}

impl InFlightView {
    pub fn empty() -> Self {
        Self {
            layers: Arc::from([]),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.layers.is_empty() || self.layers.iter().all(|l| l.is_empty())
    }

    pub fn layer_count(&self) -> usize {
        self.layers.len()
    }

    /// Pin material for create fk id (scan newest layer first).
    #[inline]
    pub fn get_out(&self, id: u64) -> Option<&CreatePin> {
        for layer in self.layers.iter().rev() {
            if let Some(p) = layer.outs.get(&id) {
                return Some(p);
            }
        }
        None
    }

    /// Create fk for txid from prior uncommitted packs.
    #[inline]
    pub fn get_create_fk(&self, txid: &[u8; 32]) -> Option<Fk> {
        for layer in self.layers.iter().rev() {
            if let Some(fk) = layer.creates.get(txid) {
                return Some(*fk);
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rbitcoin_store::{OutputRecord, TxRecord};

    fn pin(id: u64) -> CreatePin {
        let mut txid = [0u8; 32];
        txid[..8].copy_from_slice(&id.to_le_bytes());
        Arc::new((
            TxRecord {
                txid,
                version: 1,
                locktime: 0,
                input_start_fk: Fk::NULL,
                input_count: 1,
                output_start_fk: Fk::NULL,
                output_count: 1,
            },
            vec![OutputRecord::unspent(1, vec![0x51])],
            vec![0u32],
        ))
    }

    #[test]
    fn size_snapshot_counts_layers_and_bytes() {
        let mut log = InFlightLog::new();
        let p1 = pin(1);
        let p2 = pin(2);
        log.note_layer(InFlightLayer::from_plan_pins([(Fk(1), &p1)]));
        log.note_layer(InFlightLayer::from_plan_pins([(Fk(2), &p2)]));
        let (layers, entries, bytes) = log.size_snapshot();
        assert_eq!(layers, 2);
        assert_eq!(entries, 2);
        assert!(bytes > 0, "expected non-zero approx bytes");
        // Two pins with tiny scripts should stay well under 4 KiB.
        assert!(bytes < 4096, "bytes={bytes}");
        crate::process_mem_stats::note(layers, entries, bytes, 10, 2, 100);
        let s = crate::process_mem_stats::load();
        assert_eq!(s.inflight_layers, 2);
        assert_eq!(s.inflight_pins, 2);
        assert_eq!(s.pstore_weak, 10);
        assert_eq!(s.pstore_live, 2);
    }

    #[test]
    fn note_does_not_mutate_prior_layer_arcs() {
        let mut log = InFlightLog::new();
        let p1 = pin(1);
        log.note_layer(InFlightLayer::from_plan_pins([(Fk(1), &p1)]));
        let snap = log.snapshot();
        let layer0 = Arc::clone(&log.layers[0]);

        let p2 = pin(2);
        log.note_layer(InFlightLayer::from_plan_pins([(Fk(2), &p2)]));

        // Prior layer Arc identity unchanged (no make_mut rebuild).
        assert!(
            Arc::ptr_eq(&layer0, &log.layers[0]),
            "prior layer must not be rebuilt when noting a new pack"
        );
        assert_eq!(snap.layer_count(), 1);
        assert!(snap.get_out(1).is_some());
        assert!(snap.get_out(2).is_none(), "snapshot is frozen");
        assert!(log.snapshot().get_out(2).is_some());
    }

    #[test]
    fn prune_keeps_body_ahead_of_head() {
        let mut log = InFlightLog::new();
        // One layer with fks 85..=100 (simulates seal window batch).
        let pins: Vec<_> = (85u64..=100)
            .map(|id| {
                let p = pin(id);
                (Fk(id), p)
            })
            .collect();
        let layer = InFlightLayer::from_plan_pins(pins.iter().map(|(f, p)| (*f, p)));
        log.note_layer(layer);
        log.prune(90);
        let v = log.snapshot();
        for id in 91u64..=100 {
            assert!(v.get_out(id).is_some(), "keep {id}");
        }
        for id in 85u64..=90 {
            assert!(v.get_out(id).is_none(), "drop {id}");
        }
        assert_eq!(log.entry_count(), 10);
    }

    #[test]
    fn prune_drops_whole_old_layers() {
        let mut log = InFlightLog::new();
        let a = pin(10);
        let b = pin(50);
        log.note_layer(InFlightLayer::from_plan_pins([(Fk(10), &a)]));
        log.note_layer(InFlightLayer::from_plan_pins([(Fk(50), &b)]));
        assert_eq!(log.layer_count(), 2);
        log.prune(10);
        assert_eq!(log.layer_count(), 1);
        assert!(log.snapshot().get_out(50).is_some());
        assert!(log.snapshot().get_out(10).is_none());
    }

    #[test]
    fn clear_drops_all_layers_and_entries() {
        let mut log = InFlightLog::new();
        for i in 1u64..=5 {
            let p = pin(i);
            log.note_layer(InFlightLayer::from_plan_pins([(Fk(i), &p)]));
        }
        assert_eq!(log.layer_count(), 5);
        assert_eq!(log.entry_count(), 5);
        let held = log.snapshot();
        assert_eq!(held.layer_count(), 5);
        log.clear();
        assert_eq!(log.layer_count(), 0);
        assert_eq!(log.entry_count(), 0);
        // Prior snapshot stays valid (immutable); log is empty for new notes.
        assert_eq!(held.layer_count(), 5);
        assert!(held.get_out(3).is_some());
        assert!(log.snapshot().is_empty());
        let p = pin(99);
        log.note_layer(InFlightLayer::from_plan_pins([(Fk(99), &p)]));
        assert_eq!(log.layer_count(), 1);
        assert!(log.snapshot().get_out(99).is_some());
        assert!(log.snapshot().get_out(1).is_none());
    }

    #[test]
    fn snapshot_stable_while_log_notes_more() {
        let mut log = InFlightLog::new();
        let p1 = pin(1);
        log.note_layer(InFlightLayer::from_plan_pins([(Fk(1), &p1)]));
        let held = log.snapshot();
        for i in 2u64..=40 {
            let p = pin(i);
            log.note_layer(InFlightLayer::from_plan_pins([(Fk(i), &p)]));
        }
        // Prep-held view still only sees pack 1.
        assert_eq!(held.layer_count(), 1);
        assert!(held.get_out(1).is_some());
        assert!(held.get_out(40).is_none());
        assert_eq!(log.layer_count(), 40);
    }

    /// Timed multi-pack note while a prep snapshot is held — must stay O(C)
    /// per pack (no O(N) whole-map clone). Guards Phase A performance.
    #[test]
    fn multi_pack_note_under_held_snapshot_is_cheap() {
        use std::time::Instant;

        const PACKS: u64 = 80;
        const PER_PACK: u64 = 2_000;

        let mut log = InFlightLog::new();
        // Seed depth so N is large before the timed region.
        for pack in 0..40u64 {
            let pins: Vec<_> = (0..PER_PACK)
                .map(|i| {
                    let id = pack * PER_PACK + i + 1;
                    let p = pin(id);
                    (Fk(id), p)
                })
                .collect();
            log.note_layer(InFlightLayer::from_plan_pins(
                pins.iter().map(|(f, p)| (*f, p)),
            ));
        }
        let held = log.snapshot();
        assert!(held.layer_count() >= 40);

        let t0 = Instant::now();
        for pack in 40..PACKS {
            let pins: Vec<_> = (0..PER_PACK)
                .map(|i| {
                    let id = pack * PER_PACK + i + 1;
                    let p = pin(id);
                    (Fk(id), p)
                })
                .collect();
            log.note_layer(InFlightLayer::from_plan_pins(
                pins.iter().map(|(f, p)| (*f, p)),
            ));
        }
        let elapsed = t0.elapsed();
        // Held snapshot must remain frozen.
        assert_eq!(held.layer_count(), 40);
        assert!(held.get_out(1).is_some());
        assert!(held.get_out(40 * PER_PACK + 1).is_none());

        // 40 packs × 2000 pins under a held snapshot of 40 prior packs
        // (≈80k live entries when finishing). Old make_mut path cloned O(N)
        // HashMap shells each tick → multi-second; immutable note is O(C).
        eprintln!(
            "inflight_bench: packs={} per_pack={} note_under_hold={:?} final_layers={} final_entries={}",
            PACKS - 40,
            PER_PACK,
            elapsed,
            log.layer_count(),
            log.entry_count()
        );
        assert!(
            elapsed.as_millis() < 2_000,
            "note under held snapshot too slow: {elapsed:?} (possible map-clone regression)"
        );
        assert_eq!(log.layer_count(), PACKS as usize);
        let _ = held;
    }
}
