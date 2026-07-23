//! Process-local Class A body presence cache for IBD assign/status.

use crate::chain::ChainHub;
use bitcoin::BlockHash;
use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

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
    pending: HashSet<BlockHash>,
    /// When each pending hash was marked (for stuck-pipeline re-getdata).
    pending_since: HashMap<BlockHash, Instant>,
    /// Budget-charged into the archive job pipeline (not yet Ok/Err/Dropped).
    ///
    /// Distinct from [`pending`]: `BlockFramed` marks pending before decode, but
    /// charge happens only on `Block`. Redeliveries must not re-charge while this
    /// set still holds the hash (mainnet stacked tens of GiB of duplicate jobs).
    archive_charged: HashSet<BlockHash>,
    /// Store-probed and not archived yet (safe to request getdata).
    missing: HashSet<BlockHash>,
    /// Confirm rejected this hash; do not re-offer or re-download.
    rejected: HashSet<BlockHash>,
}

impl BodyPresence {
    pub(crate) fn new() -> Self {
        Self {
            known: HashSet::new(),
            pending: HashSet::new(),
            pending_since: HashMap::new(),
            archive_charged: HashSet::new(),
            missing: HashSet::new(),
            rejected: HashSet::new(),
        }
    }

    pub(crate) fn mark_pending(&mut self, h: BlockHash) {
        if self.rejected.contains(&h) {
            return;
        }
        self.missing.remove(&h);
        if self.pending.insert(h) {
            self.pending_since.insert(h, Instant::now());
        }
    }

    /// True if this hash already holds an archive-queue budget charge.
    pub(crate) fn is_archive_charged(&self, h: &BlockHash) -> bool {
        self.archive_charged.contains(h)
    }

    /// Record that `h` was successfully [`ArchiveQueueBudget::try_charge`]d.
    pub(crate) fn mark_archive_charged(&mut self, h: BlockHash) {
        if self.rejected.contains(&h) {
            return;
        }
        self.archive_charged.insert(h);
    }

    /// Drop the charged marker after budget [`release`] (Ok / Err / Dropped).
    pub(crate) fn clear_archive_charged(&mut self, h: &BlockHash) {
        self.archive_charged.remove(h);
    }

    pub(crate) fn mark_archived(&mut self, h: BlockHash) {
        if self.rejected.contains(&h) {
            return;
        }
        self.missing.remove(&h);
        self.pending.remove(&h);
        self.pending_since.remove(&h);
        self.archive_charged.remove(&h);
        self.known.insert(h);
    }

    pub(crate) fn mark_missing(&mut self, h: BlockHash) {
        if self.rejected.contains(&h) {
            return;
        }
        self.known.remove(&h);
        self.pending.remove(&h);
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
        self.pending.remove(&h);
        self.pending_since.remove(&h);
        self.archive_charged.remove(&h);
        self.missing.remove(&h);
        self.rejected.insert(h);
    }

    /// Pending hashes older than `max_age` → mark missing so getdata can retry.
    ///
    /// Covers stuck decode/archive after [`mark_pending`] (tip hole otherwise
    /// never re-requests because `skip_download` treats pending as done).
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
        self.pending.len()
    }

    /// Wire frame received / archive pipeline owns this hash (not confirmable yet).
    pub(crate) fn is_pending(&self, h: &BlockHash) -> bool {
        self.pending.contains(h)
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
        if self.pending.contains(h) || self.missing.contains(h) {
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

    /// Skip getdata: already confirmed, archived, rejected, or in the archive pipeline.
    ///
    /// Hot path: local sets first. Do **not** call `has_block` before checking
    /// `missing` — assign walks tens of thousands of known-missing far hashes and
    /// used to re-take the confirmed-set lock on every one (multi-100ms assign).
    pub(crate) fn skip_download(&mut self, hub: &ChainHub, h: &BlockHash) -> bool {
        if self.rejected.contains(h) || self.known.contains(h) || self.pending.contains(h) {
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
        let drop_pending: Vec<BlockHash> = self
            .pending
            .iter()
            .filter(|h| !keep(h))
            .copied()
            .collect();
        for h in drop_pending {
            self.pending.remove(&h);
            self.pending_since.remove(&h);
        }
        // Drop charged markers only when the hash left the work path; if a body
        // is still in the archive pipeline its charge is released via result.
        self.archive_charged.retain(|h| keep(h));
        // rejected: permanent blacklist
    }

    /// Local decision without store probe (for tests / hot short-circuit).
    ///
    /// Returns `Some(true)` if we must skip getdata, `Some(false)` if we know
    /// the body is missing, `None` if a store probe would be required.
    #[cfg(test)]
    pub(crate) fn skip_download_cached(&self, h: &BlockHash) -> Option<bool> {
        if self.rejected.contains(h) || self.known.contains(h) || self.pending.contains(h) {
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

    #[test]
    fn pending_skips_without_store() {
        let mut body = BodyPresence::new();
        let hash = h(1);
        body.mark_pending(hash);
        assert_eq!(body.skip_download_cached(&hash), Some(true));
        // Still not ready for confirm (pending ≠ archived).
        assert!(!body.known.contains(&hash));
    }

    #[test]
    fn archived_skips_and_is_known() {
        let mut body = BodyPresence::new();
        let hash = h(2);
        body.mark_archived(hash);
        assert_eq!(body.skip_download_cached(&hash), Some(true));
        assert!(body.known.contains(&hash));
        assert!(!body.pending.contains(&hash));
        assert!(!body.missing.contains(&hash));
    }

    #[test]
    fn missing_does_not_skip() {
        let mut body = BodyPresence::new();
        let hash = h(3);
        body.mark_missing(hash);
        assert_eq!(body.skip_download_cached(&hash), Some(false));
    }

    #[test]
    fn known_and_pending_counts() {
        let mut body = BodyPresence::new();
        assert_eq!(body.known_len(), 0);
        assert_eq!(body.pending_len(), 0);
        body.mark_pending(h(1));
        body.mark_archived(h(2));
        assert_eq!(body.pending_len(), 1);
        assert_eq!(body.known_len(), 1);
    }

    #[test]
    fn hygiene_retain_drops_stale_known_keeps_rejected() {
        let mut body = BodyPresence::new();
        body.mark_archived(h(1));
        body.mark_missing(h(2));
        body.mark_rejected(h(3));
        body.hygiene_retain(|x| *x == h(1));
        assert!(body.is_known_archived(&h(1)));
        assert_eq!(body.skip_download_cached(&h(2)), None); // missing dropped
        assert!(body.is_rejected(&h(3)));
    }

    #[test]
    fn unknown_needs_probe() {
        let body = BodyPresence::new();
        assert_eq!(body.skip_download_cached(&h(9)), None);
    }

    #[test]
    fn mark_archived_clears_pending_and_missing() {
        let mut body = BodyPresence::new();
        let hash = h(4);
        body.mark_pending(hash);
        body.mark_archived(hash);
        assert!(body.known.contains(&hash));
        assert!(!body.pending.contains(&hash));
        body.mark_missing(hash);
        assert!(body.missing.contains(&hash));
        assert!(!body.known.contains(&hash));
        body.mark_archived(hash);
        assert!(body.known.contains(&hash));
        assert!(!body.missing.contains(&hash));
    }

    #[test]
    fn demote_known_allows_reprobe_without_missing() {
        let mut body = BodyPresence::new();
        let hash = h(6);
        body.mark_archived(hash);
        assert!(body.is_known_archived(&hash));
        body.demote_known(hash);
        assert!(!body.is_known_archived(&hash));
        // Not in missing — ready() may re-probe store.
        assert_eq!(body.skip_download_cached(&hash), None);
    }

    #[test]
    fn rejected_skips_download_and_is_not_ready() {
        let mut body = BodyPresence::new();
        let hash = h(5);
        body.mark_archived(hash);
        body.mark_rejected(hash);
        assert!(body.is_rejected(&hash));
        assert_eq!(body.skip_download_cached(&hash), Some(true));
        // mark_archived must not resurrect a rejected hash.
        body.mark_archived(hash);
        assert!(body.is_rejected(&hash));
        assert!(!body.known.contains(&hash));
    }

    #[test]
    fn archive_charged_survives_pending_stale_missing() {
        let mut body = BodyPresence::new();
        let hash = h(7);
        // BlockFramed: pending before charge.
        body.mark_pending(hash);
        assert!(!body.is_archive_charged(&hash));
        // Block: charge once.
        body.mark_archive_charged(hash);
        assert!(body.is_archive_charged(&hash));
        // Pending-stale expire → missing, but charge still held (body in job q).
        body.mark_missing(hash);
        assert!(body.is_archive_charged(&hash), "must not re-charge while in pipeline");
        assert_eq!(body.skip_download_cached(&hash), Some(false)); // missing
        // Pipeline result releases charge.
        body.clear_archive_charged(&hash);
        assert!(!body.is_archive_charged(&hash));
    }

    #[test]
    fn mark_archived_clears_archive_charged() {
        let mut body = BodyPresence::new();
        let hash = h(8);
        body.mark_archive_charged(hash);
        body.mark_pending(hash);
        body.mark_archived(hash);
        assert!(!body.is_archive_charged(&hash));
    }
}
