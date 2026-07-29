//! Process-local Class A body presence cache for IBD assign/status.

use crate::chain::ChainHub;
use bitcoin::BlockHash;
use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

/// O(1) occupancy of each [`BodyPresence`] set (for `ibd: sizes` logs).
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct BodyPresenceSizes {
    pub known: usize,
    pub pending: usize,
    pub missing: usize,
    pub archive_charged: usize,
    pub rejected: usize,
}

/// Process-local Class A body presence cache.
///
/// Assign/status used to call `is_archived` (store) for every ordered hash every
/// loop — that contended with the archive writer and delayed getdata top-up
/// (spiky receive). Probe each hash at most once until archive marks it ready.
///
/// `pending` covers the wire→archive gap: we clear getdata inflight on `block`
/// receipt, but must not re-request until Class A publish finishes.
///
/// `rejected` covers Class C permanent failures (BadPrev / missing prevout, …):
/// the body may still be in Class A, but we must not re-offer it to confirm or
/// re-queue getdata for the same hash (store still has the body).
pub(crate) struct BodyPresence {
    known: HashSet<BlockHash>,
    /// Received on the wire / in archive prep, not yet Class A.
    /// Membership **is** the pending set; value is when marked (stale expire).
    pending_since: HashMap<BlockHash, Instant>,
    /// Budget-charged into the archive job pipeline (not yet Ok/Err/Dropped).
    ///
    /// Distinct from pending: `BlockFramed` marks pending before decode, but
    /// charge happens only on `Block`. Redeliveries must not re-charge while this
    /// set still holds the hash (mainnet stacked tens of GiB of duplicate jobs).
    /// hash → charged wire_bytes (for release).
    archive_charged: HashMap<BlockHash, usize>,
    /// Store-probed and not archived yet (safe to request getdata).
    missing: HashSet<BlockHash>,
    /// Confirm rejected this hash; do not re-offer or re-download.
    rejected: HashSet<BlockHash>,
}

impl BodyPresence {
    pub(crate) fn new() -> Self {
        Self {
            known: HashSet::new(),
            pending_since: HashMap::new(),
            archive_charged: HashMap::new(),
            missing: HashSet::new(),
            rejected: HashSet::new(),
        }
    }

    #[inline]
    fn is_pending_hash(&self, h: &BlockHash) -> bool {
        self.pending_since.contains_key(h)
    }

    pub(crate) fn mark_pending(&mut self, h: BlockHash) {
        if self.rejected.contains(&h) {
            return;
        }
        self.missing.remove(&h);
        self.pending_since.entry(h).or_insert_with(Instant::now);
    }

    /// True if this hash already holds an archive-queue budget charge.
    pub(crate) fn is_archive_charged(&self, h: &BlockHash) -> bool {
        self.archive_charged.contains_key(h)
    }

    /// Record charge with wire byte size for later release.
    pub(crate) fn mark_archive_charged_bytes(&mut self, h: BlockHash, wire_bytes: usize) {
        if self.rejected.contains(&h) {
            return;
        }
        self.archive_charged.entry(h).or_insert(wire_bytes);
    }

    /// Drop the charged marker after budget [`release`] (Ok / Err / Dropped).
    pub(crate) fn clear_archive_charged(&mut self, h: &BlockHash) {
        self.archive_charged.remove(h);
    }

    /// Remove charge and return stored wire_bytes (if any).
    pub(crate) fn take_archive_charge_bytes(&mut self, h: &BlockHash) -> Option<usize> {
        self.archive_charged.remove(h)
    }

    pub(crate) fn mark_archived(&mut self, h: BlockHash) {
        if self.rejected.contains(&h) {
            return;
        }
        self.missing.remove(&h);
        self.pending_since.remove(&h);
        self.archive_charged.remove(&h);
        self.known.insert(h);
    }

    pub(crate) fn mark_missing(&mut self, h: BlockHash) {
        if self.rejected.contains(&h) {
            return;
        }
        self.known.remove(&h);
        self.pending_since.remove(&h);
        // Keep `archive_charged` if still set: a body may still be in the job
        // queue after pending-stale expire. Charge is cleared only on pipeline
        // result so redelivery cannot double-charge.
        self.missing.insert(h);
    }

    /// Drop optimistic Class A cache only — next [`Self::ready`] re-probes the store.
    ///
    /// Used when confirm saw "without archive". Do **not** poison `missing`
    /// (that would block re-offer until a fresh archive Ok).
    pub(crate) fn demote_known(&mut self, h: BlockHash) {
        self.known.remove(&h);
    }

    /// Permanent confirm failure: never re-offer this hash to the confirm engine
    /// and never re-getdata it (Class A may still hold the body).
    pub(crate) fn mark_rejected(&mut self, h: BlockHash) {
        self.known.remove(&h);
        self.pending_since.remove(&h);
        self.archive_charged.remove(&h);
        self.missing.remove(&h);
        self.rejected.insert(h);
    }

    // archive_charged is HashMap; len for sizes snap uses .len() still.

    /// Pending hashes older than `max_age` → mark missing so getdata can retry.
    ///
    /// Covers stuck decode/archive after [`mark_pending`] (tip hole otherwise
    /// never re-requests because `skip_download` treats pending as done).
    #[cfg(test)]
    pub(crate) fn expire_stale_pending(&mut self, max_age: Duration) -> Vec<BlockHash> {
        self.expire_stale_pending_if(max_age, |_| true)
    }

    /// Like [`expire_stale_pending`] but only for hashes matching `pred`.
    ///
    /// Used for ContigPark gap band (short timeout) without disturbing far pending.
    pub(crate) fn expire_stale_pending_if(
        &mut self,
        max_age: Duration,
        mut pred: impl FnMut(&BlockHash) -> bool,
    ) -> Vec<BlockHash> {
        let now = Instant::now();
        let stale: Vec<BlockHash> = self
            .pending_since
            .iter()
            .filter(|(h, t)| pred(h) && now.duration_since(**t) >= max_age)
            .map(|(h, _)| *h)
            .collect();
        for h in &stale {
            self.mark_missing(*h);
        }
        stale
    }

    pub(crate) fn is_rejected(&self, h: &BlockHash) -> bool {
        self.rejected.contains(h)
    }

    /// True if we already know Class A is present (no store probe).
    pub(crate) fn is_known_archived(&self, h: &BlockHash) -> bool {
        self.known.contains(h)
    }

    /// Process-local Class A hits (for density / header soft-cap decisions).
    pub(crate) fn known_len(&self) -> usize {
        self.known.len()
    }

    /// Framed / archive-pipeline hashes not yet Class A.
    pub(crate) fn pending_len(&self) -> usize {
        self.pending_since.len()
    }

    /// Cheap occupancy for the 5s `ibd: sizes` line (all O(1) lens).
    pub(crate) fn size_snapshot(&self) -> BodyPresenceSizes {
        BodyPresenceSizes {
            known: self.known.len(),
            pending: self.pending_since.len(),
            missing: self.missing.len(),
            archive_charged: self.archive_charged.len(),
            rejected: self.rejected.len(),
        }
    }

    /// Wire framed or body-queue / fallback job owns this hash (not tip-confirmed yet).
    pub(crate) fn is_pending(&self, h: &BlockHash) -> bool {
        self.is_pending_hash(h)
    }

    /// True if Class A body is present (confirmable). Does not treat pending as ready.
    ///
    /// Trusts the local `known` set (set only after durable archive Ok). If confirm
    /// races a stale known entry, the engine asks the main loop to [`Self::mark_missing`].
    pub(crate) fn ready(&mut self, hub: &ChainHub, h: &BlockHash) -> bool {
        if self.rejected.contains(h) {
            return false;
        }
        if self.known.contains(h) || hub.has_block(h) {
            return true;
        }
        if self.is_pending_hash(h) || self.missing.contains(h) {
            return false;
        }
        if hub.is_archived(h) {
            self.known.insert(*h);
            true
        } else {
            self.missing.insert(*h);
            false
        }
    }

    /// Skip getdata: already confirmed, Class A ready, rejected, or pending in body queue.
    ///
    /// Hot path: local sets first. Do **not** call `has_block` before checking
    /// `missing` — assign walks tens of thousands of known-missing far hashes and
    /// used to re-take the confirmed-set lock on every one (multi-100ms assign).
    pub(crate) fn skip_download(&mut self, hub: &ChainHub, h: &BlockHash) -> bool {
        if self.rejected.contains(h) || self.known.contains(h) || self.is_pending_hash(h) {
            return true;
        }
        if self.missing.contains(h) {
            return false;
        }
        // One-shot store / confirmed probe; caches into known or missing.
        self.ready(hub, h)
    }

    /// Drop cache entries that are no longer on the live work path.
    /// Rejected entries are never pruned.
    pub(crate) fn hygiene_retain(&mut self, mut keep: impl FnMut(&BlockHash) -> bool) {
        self.known.retain(|h| keep(h));
        self.missing.retain(|h| keep(h));
        self.pending_since.retain(|h, _| keep(h));
        // Do **not** hygiene-prune `archive_charged`. Markers clear only on
        // pipeline Ok/Err/Dropped via [`clear_archive_charged`]. Dropping them
        // while a job is still charged allows redelivery double-charge and loses
        // the only process-local "in pipeline" bit (budget meter desync).
        // rejected: permanent blacklist
    }

    /// Local decision without store probe (for tests / hot short-circuit).
    ///
    /// Returns `Some(true)` if we must skip getdata, `Some(false)` if we know
    /// the body is missing, `None` if a store probe would be required.
    #[cfg(test)]
    pub(crate) fn skip_download_cached(&self, h: &BlockHash) -> Option<bool> {
        if self.rejected.contains(h) || self.known.contains(h) || self.is_pending_hash(h) {
            return Some(true);
        }
        if self.missing.contains(h) {
            return Some(false);
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::BodyPresence;
    use bitcoin::hashes::Hash;
    use bitcoin::BlockHash;

    fn h(n: u8) -> BlockHash {
        let mut b = [0u8; 32];
        b[0] = n;
        BlockHash::from_byte_array(b)
    }

    /// skip_download / mark_pending / archived / missing / demote / counts / hygiene.
    #[test]
    fn presence_lifecycle_surface() {
        let mut body = BodyPresence::new();
        assert_eq!(body.skip_download_cached(&h(9)), None); // unknown → probe

        body.mark_pending(h(1));
        assert_eq!(body.skip_download_cached(&h(1)), Some(true));
        assert!(!body.known.contains(&h(1))); // pending ≠ archived

        body.mark_archived(h(2));
        assert_eq!(body.skip_download_cached(&h(2)), Some(true));
        assert!(body.known.contains(&h(2)));
        assert!(!body.is_pending(&h(2)));
        assert!(!body.missing.contains(&h(2)));

        body.mark_missing(h(3));
        assert_eq!(body.skip_download_cached(&h(3)), Some(false));

        assert_eq!(body.pending_len(), 1);
        assert_eq!(body.known_len(), 1);

        // archived clears pending; missing clears known; re-archive clears missing
        let hash = h(4);
        body.mark_pending(hash);
        body.mark_archived(hash);
        assert!(body.known.contains(&hash));
        assert!(!body.is_pending(&hash));
        body.mark_missing(hash);
        assert!(body.missing.contains(&hash));
        assert!(!body.known.contains(&hash));
        body.mark_archived(hash);
        assert!(body.known.contains(&hash));
        assert!(!body.missing.contains(&hash));

        body.demote_known(h(2));
        assert!(!body.is_known_archived(&h(2)));
        assert_eq!(body.skip_download_cached(&h(2)), None);

        body.mark_archived(h(10));
        body.mark_missing(h(11));
        body.mark_rejected(h(12));
        body.mark_archive_charged_bytes(h(11), 0);
        body.hygiene_retain(|x| *x == h(10));
        assert!(body.is_known_archived(&h(10)));
        assert_eq!(body.skip_download_cached(&h(11)), None); // missing dropped
        assert!(body.is_rejected(&h(12)));
        // Charged marker survives hygiene until pipeline result (no double-charge).
        assert!(
            body.is_archive_charged(&h(11)),
            "archive_charged must not be hygiene-pruned while job may still be in flight"
        );
        body.clear_archive_charged(&h(11));
        assert!(!body.is_archive_charged(&h(11)));
    }

    /// rejected sticky + archive_charged through pending-stale pipeline.
    #[test]
    fn rejected_and_archive_charged_surface() {
        let mut body = BodyPresence::new();
        let rej = h(5);
        body.mark_archived(rej);
        body.mark_rejected(rej);
        assert!(body.is_rejected(&rej));
        assert_eq!(body.skip_download_cached(&rej), Some(true));
        body.mark_archived(rej); // must not resurrect rejected
        assert!(body.is_rejected(&rej));
        assert!(!body.known.contains(&rej));

        let charge = h(7);
        body.mark_pending(charge);
        assert!(!body.is_archive_charged(&charge));
        body.mark_archive_charged_bytes(charge, 0);
        assert!(body.is_archive_charged(&charge));
        body.mark_missing(charge);
        assert!(
            body.is_archive_charged(&charge),
            "must not re-charge while in pipeline"
        );
        assert_eq!(body.skip_download_cached(&charge), Some(false));
        body.clear_archive_charged(&charge);
        assert!(!body.is_archive_charged(&charge));

        let done = h(8);
        body.mark_archive_charged_bytes(done, 0);
        body.mark_pending(done);
        body.mark_archived(done);
        assert!(!body.is_archive_charged(&done));
    }

    #[test]
    fn expire_stale_pending_and_size_snapshot() {
        use std::time::Duration;
        let mut body = BodyPresence::new();
        body.mark_pending(h(1));
        body.mark_pending(h(2));
        body.mark_archive_charged_bytes(h(2), 0);
        // Zero age → everything older than 0 is stale immediately.
        let expired = body.expire_stale_pending(Duration::ZERO);
        assert_eq!(expired.len(), 2);
        assert!(body.missing.contains(&h(1)));
        assert!(body.missing.contains(&h(2)));
        // Charge survives pending→missing transition.
        assert!(body.is_archive_charged(&h(2)));

        body.mark_pending(h(3));
        body.mark_pending(h(4));
        let only3 = body.expire_stale_pending_if(Duration::ZERO, |x| *x == h(3));
        assert_eq!(only3, vec![h(3)]);
        assert!(body.is_pending(&h(4)));

        body.mark_archived(h(10));
        body.mark_rejected(h(11));
        let snap = body.size_snapshot();
        assert_eq!(snap.known, 1);
        assert_eq!(snap.pending, 1); // h(4)
        assert!(snap.missing >= 2);
        assert_eq!(snap.rejected, 1);
        assert_eq!(snap.archive_charged, 1); // h(2)
    }

    #[test]
    fn rejected_blocks_mark_ops() {
        let mut body = BodyPresence::new();
        let r = h(20);
        body.mark_rejected(r);
        body.mark_pending(r);
        body.mark_archive_charged_bytes(r, 0);
        body.mark_missing(r);
        body.mark_archived(r);
        assert!(body.is_rejected(&r));
        assert!(!body.is_pending(&r));
        assert!(!body.is_archive_charged(&r));
        assert!(!body.known.contains(&r));
    }
}
