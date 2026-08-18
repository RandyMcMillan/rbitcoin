//! In-RAM block payload queue for the combined archive/confirm path.
//!
//! **Why RAM (not disk):** peer wire would otherwise be written once to a durable
//! queue and again into Class A on confirm — **double disk write per block**.
//! Keeping the same FIFO / height-index structure in process memory trades
//! **redownload on restart** and peak RAM for a single durable write (Class A).
//!
//! **Lifecycle:** enqueue after peer framing (raw payload only — no full block
//! parse); **dequeue only after combined confirm-write** (or permanent reject).
//! Restart does **not** rehydrate payloads (queue starts empty).
//!
//! **Primary capacity** is soft densify assign in the net layer (no hysteresis):
//! under ~100 MiB free densify ahead; over ~100 MiB only the next ~1 min of
//! confirm work at tip rate. This type accepts payloads until an optional
//! absolute byte ceiling (env) is hit.
//!
//! Absolute safety ceiling (optional): `RBITCOIN_BLOCK_QUEUE_GB` (integer GiB)
//! or `RBITCOIN_BLOCK_QUEUE_BYTES`. When unset, enqueue is unlimited
//! (`u64::MAX`) aside from OOM.

use crate::error::StoreError;
use std::collections::{BTreeMap, HashSet};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

/// Default absolute byte ceiling: unlimited (soft time-depth gates densify).
pub const DEFAULT_BLOCK_QUEUE_BUDGET_BYTES: u64 = u64::MAX;

/// One queued block (full wire payload — held in RAM until confirm-write).
#[derive(Debug, Clone)]
pub struct QueuedBlock {
    pub id: u64,
    pub height: u32,
    pub hash: [u8; 32],
    pub header_fk: u64,
    pub payload: Vec<u8>,
}

/// Index-only view of a queue entry (no payload clone).
#[derive(Debug, Clone, Copy)]
pub struct QueuedBlockMeta {
    pub id: u64,
    pub height: u32,
    pub hash: [u8; 32],
    pub header_fk: u64,
    pub payload_len: u64,
    /// Lookup finished TipOnly for this height (load may still leftover-stamp).
    pub resolve_complete: bool,
}

/// Process-local FIFO of block payloads (append + delete-by-id).
///
/// Same shape as the former on-disk queue (`id`, height, hash, header_fk,
/// payload) but **never** writes rec files. `store_dir` is accepted for API
/// compatibility and to optionally ignore/remove a legacy `block_queue/` dir.
pub struct BlockQueue {
    budget: u64,
    next_id: AtomicU64,
    /// id → entry (payload owned)
    index: BTreeMap<u64, IndexEntry>,
    bytes: u64,
}

#[derive(Debug, Clone)]
struct IndexEntry {
    height: u32,
    hash: [u8; 32],
    header_fk: u64,
    payload: Vec<u8>,
    resolve_complete: bool,
}

impl BlockQueue {
    /// Absolute byte ceiling from env, or [`DEFAULT_BLOCK_QUEUE_BUDGET_BYTES`]
    /// (unlimited) when unset. Env values clamp to at least 64 MiB.
    pub fn budget_from_env() -> u64 {
        if let Ok(s) = std::env::var("RBITCOIN_BLOCK_QUEUE_BYTES") {
            if let Ok(n) = s.parse::<u64>() {
                return n.max(64 * 1024 * 1024);
            }
        }
        if let Ok(s) = std::env::var("RBITCOIN_BLOCK_QUEUE_GB") {
            if let Ok(n) = s.parse::<u64>() {
                return n.saturating_mul(1024 * 1024 * 1024).max(64 * 1024 * 1024);
            }
        }
        DEFAULT_BLOCK_QUEUE_BUDGET_BYTES
    }

    /// Create an empty in-RAM queue. Legacy on-disk `store_dir/block_queue/` is
    /// not loaded (redownload after restart); best-effort remove stale files.
    pub fn open_or_create(store_dir: &Path) -> Result<Self, StoreError> {
        Self::open_or_create_with_budget(store_dir, Self::budget_from_env())
    }

    pub fn open_or_create_with_budget(store_dir: &Path, budget: u64) -> Result<Self, StoreError> {
        // Drop legacy durable layout if present (no longer used).
        let legacy = store_dir.join("block_queue");
        if legacy.exists() {
            let _ = std::fs::remove_dir_all(&legacy);
        }
        let budget = if budget == u64::MAX {
            u64::MAX
        } else {
            budget.max(64 * 1024 * 1024)
        };
        Ok(Self {
            budget,
            next_id: AtomicU64::new(1),
            index: BTreeMap::new(),
            bytes: 0,
        })
    }

    pub fn budget(&self) -> u64 {
        self.budget
    }

    pub fn bytes(&self) -> u64 {
        self.bytes
    }

    pub fn count(&self) -> usize {
        self.index.len()
    }

    /// True when an optional absolute byte ceiling still has room.
    ///
    /// Default budget is unlimited; densify is gated by time-depth soft stop
    /// in the IBD assign path, not this check.
    pub fn can_enqueue(&self, payload_len: usize) -> bool {
        if self.budget == u64::MAX {
            return true;
        }
        self.bytes.saturating_add(payload_len as u64) <= self.budget
    }

    /// `bytes / budget` for finite caps; `0.0` when unlimited.
    pub fn fill_ratio(&self) -> f64 {
        if self.budget == 0 || self.budget == u64::MAX {
            return 0.0;
        }
        self.bytes as f64 / self.budget as f64
    }

    /// Append a block payload in RAM. Returns queue id.
    ///
    /// Refuses only when an optional absolute [`Self::budget`] would be
    /// exceeded ([`StoreError::BudgetFull`]). Default budget is unlimited —
    /// IBD soft time-depth stops **new densify getdata**, not in-flight offers.
    pub fn enqueue(
        &mut self,
        height: u32,
        hash: [u8; 32],
        header_fk: u64,
        payload: &[u8],
    ) -> Result<u64, StoreError> {
        if !self.can_enqueue(payload.len()) {
            return Err(StoreError::BudgetFull(
                "block_queue absolute ceiling (RBITCOIN_BLOCK_QUEUE_GB / _BYTES)",
            ));
        }
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        self.bytes = self.bytes.saturating_add(payload.len() as u64);
        self.index.insert(
            id,
            IndexEntry {
                height,
                hash,
                header_fk,
                payload: payload.to_vec(),
                resolve_complete: false,
            },
        );
        Ok(id)
    }

    /// Load payload by id.
    pub fn get(&self, id: u64) -> Result<Option<QueuedBlock>, StoreError> {
        let Some(e) = self.index.get(&id) else {
            return Ok(None);
        };
        Ok(Some(QueuedBlock {
            id,
            height: e.height,
            hash: e.hash,
            header_fk: e.header_fk,
            payload: e.payload.clone(),
        }))
    }

    /// Peek oldest id (FIFO by id).
    pub fn peek_oldest_id(&self) -> Option<u64> {
        self.index.keys().next().copied()
    }

    /// List all ids ascending.
    pub fn ids(&self) -> Vec<u64> {
        self.index.keys().copied().collect()
    }

    /// Remove after confirm-write / permanent reject.
    pub fn dequeue(&mut self, id: u64) -> Result<bool, StoreError> {
        let Some(e) = self.index.remove(&id) else {
            return Ok(false);
        };
        self.bytes = self.bytes.saturating_sub(e.payload.len() as u64);
        Ok(true)
    }

    /// Dequeue all records for a confirmed height (may be 0 or 1 in normal path).
    pub fn dequeue_height(&mut self, height: u32) -> Result<usize, StoreError> {
        let ids: Vec<u64> = self
            .index
            .iter()
            .filter(|(_, e)| e.height == height)
            .map(|(id, _)| *id)
            .collect();
        let mut n = 0usize;
        for id in ids {
            if self.dequeue(id)? {
                n += 1;
            }
        }
        Ok(n)
    }

    /// Lowest distinct queued heights `≥ path_lo` that are not resolve-complete
    /// and not in `skip`, capped at `cap`. One pass over the index (no
    /// per-height `is_resolve_complete` scan). Sorted ascending.
    pub fn unresolved_heights(&self, path_lo: u32, skip: &HashSet<u32>, cap: usize) -> Vec<u32> {
        if cap == 0 {
            return Vec::new();
        }
        let mut seen = HashSet::new();
        let mut out: Vec<u32> = Vec::new();
        for e in self.index.values() {
            if e.height < path_lo || e.resolve_complete || skip.contains(&e.height) {
                continue;
            }
            if seen.insert(e.height) {
                out.push(e.height);
            }
        }
        out.sort_unstable();
        out.truncate(cap);
        out
    }

    /// Index-only listing (ascending id).
    pub fn list_meta(&self) -> Vec<QueuedBlockMeta> {
        self.index
            .iter()
            .map(|(&id, e)| QueuedBlockMeta {
                id,
                height: e.height,
                hash: e.hash,
                header_fk: e.header_fk,
                payload_len: e.payload.len() as u64,
                resolve_complete: e.resolve_complete,
            })
            .collect()
    }

    /// Load every queued block with full payload (tests / tools only).
    pub fn load_all(&self) -> Result<Vec<QueuedBlock>, StoreError> {
        let mut out = Vec::with_capacity(self.index.len());
        for &id in self.index.keys() {
            if let Some(b) = self.get(id)? {
                out.push(b);
            }
        }
        Ok(out)
    }

    /// Load payload for a height (confirm load intake). First match by height.
    ///
    /// Does **not** dequeue — confirm-write / permanent reject removes the rec.
    pub fn get_by_height(&self, height: u32) -> Result<Option<QueuedBlock>, StoreError> {
        let Some((&id, _)) = self.index.iter().find(|(_, e)| e.height == height) else {
            return Ok(None);
        };
        self.get(id)
    }

    /// True if any rec exists for `height` (O(n) index walk).
    pub fn contains_height(&self, height: u32) -> bool {
        self.index.values().any(|e| e.height == height)
    }

    /// Heights currently on the queue.
    pub fn heights(&self) -> Vec<u32> {
        self.index.values().map(|e| e.height).collect()
    }

    /// Highest height among recs (`None` if empty).
    pub fn max_height(&self) -> Option<u32> {
        self.index.values().map(|e| e.height).max()
    }

    /// Lowest height among recs (`None` if empty).
    pub fn min_height(&self) -> Option<u32> {
        self.index.values().map(|e| e.height).min()
    }

    /// First queue id for `height`, if any.
    pub fn id_for_height(&self, height: u32) -> Option<u64> {
        self.index
            .iter()
            .find(|(_, e)| e.height == height)
            .map(|(&id, _)| id)
    }

    fn entry_mut_for_height(&mut self, height: u32) -> Result<&mut IndexEntry, StoreError> {
        self.index
            .values_mut()
            .find(|e| e.height == height)
            .ok_or(StoreError::NotFound)
    }

    /// Lookup finished this height: every external parent has a hit (or none exist).
    pub fn mark_resolve_complete(&mut self, height: u32) -> Result<(), StoreError> {
        self.entry_mut_for_height(height)?.resolve_complete = true;
        Ok(())
    }

    pub fn is_resolve_complete(&self, height: u32) -> bool {
        self.index
            .values()
            .any(|e| e.height == height && e.resolve_complete)
    }

    /// Hash of the first queue entry at `height`, if any (no payload clone).
    pub fn hash_at_height(&self, height: u32) -> Option<[u8; 32]> {
        self.index
            .values()
            .find(|e| e.height == height)
            .map(|e| e.hash)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    fn temp() -> std::path::PathBuf {
        static N: AtomicU64 = AtomicU64::new(0);
        let id = N.fetch_add(1, Ordering::Relaxed);
        let d = std::env::temp_dir().join(format!("rbitcoin-bq-ram-{id}"));
        let _ = std::fs::remove_dir_all(&d);
        std::fs::create_dir_all(&d).unwrap();
        d
    }

    #[test]
    fn get_by_height_peek_without_dequeue() {
        let dir = temp();
        let mut q = BlockQueue::open_or_create_with_budget(&dir, 64 * 1024 * 1024).unwrap();
        let payload = b"wire-bytes-height-42".to_vec();
        q.enqueue(42, [0x11u8; 32], 3, &payload).unwrap();
        let got = q.get_by_height(42).unwrap().expect("by height");
        assert_eq!(got.payload, payload);
        assert_eq!(got.height, 42);
        assert!(q.contains_height(42));
        assert!(q.get_by_height(99).unwrap().is_none());
        assert_eq!(q.count(), 1, "peek must not dequeue");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn enqueue_dequeue_no_disk_rehydrate() {
        let dir = temp();
        let mut q = BlockQueue::open_or_create_with_budget(&dir, 64 * 1024 * 1024).unwrap();
        let payload = b"fake-block-bytes-for-queue".to_vec();
        let id = q.enqueue(100, [0xABu8; 32], 7, &payload).unwrap();
        assert_eq!(q.count(), 1);
        let got = q.get(id).unwrap().expect("present");
        assert_eq!(got.height, 100);
        assert_eq!(got.hash, [0xABu8; 32]);
        assert_eq!(got.payload, payload);
        assert!(q.dequeue(id).unwrap());
        assert_eq!(q.count(), 0);
        // Restart: RAM queue is empty (by design — no durable rehydrate).
        drop(q);
        let q2 = BlockQueue::open_or_create_with_budget(&dir, 64 * 1024 * 1024).unwrap();
        assert_eq!(q2.count(), 0);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Serialize env mutators — parallel suite races `budget_from_env` readers.
    static BQ_ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    #[test]
    fn default_budget_unlimited_unless_env() {
        let _g = BQ_ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let b = BlockQueue::budget_from_env();
        if std::env::var("RBITCOIN_BLOCK_QUEUE_BYTES").is_ok()
            || std::env::var("RBITCOIN_BLOCK_QUEUE_GB").is_ok()
        {
            assert!(b >= 64 * 1024 * 1024);
        } else {
            assert_eq!(b, u64::MAX, "default absolute ceiling is unlimited");
        }
    }

    #[test]
    fn budget_from_env_bytes_and_gb_clamps() {
        let _g = BQ_ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let prev_b = std::env::var_os("RBITCOIN_BLOCK_QUEUE_BYTES");
        let prev_g = std::env::var_os("RBITCOIN_BLOCK_QUEUE_GB");
        std::env::remove_var("RBITCOIN_BLOCK_QUEUE_GB");
        std::env::set_var("RBITCOIN_BLOCK_QUEUE_BYTES", "1"); // below 64MiB floor
        assert_eq!(BlockQueue::budget_from_env(), 64 * 1024 * 1024);
        std::env::remove_var("RBITCOIN_BLOCK_QUEUE_BYTES");
        std::env::set_var("RBITCOIN_BLOCK_QUEUE_GB", "2");
        assert_eq!(BlockQueue::budget_from_env(), 2u64 * 1024 * 1024 * 1024);
        match prev_b {
            Some(v) => std::env::set_var("RBITCOIN_BLOCK_QUEUE_BYTES", v),
            None => std::env::remove_var("RBITCOIN_BLOCK_QUEUE_BYTES"),
        }
        match prev_g {
            Some(v) => std::env::set_var("RBITCOIN_BLOCK_QUEUE_GB", v),
            None => std::env::remove_var("RBITCOIN_BLOCK_QUEUE_GB"),
        }
    }

    #[test]
    fn can_enqueue_and_fill_ratio_budgeted() {
        let dir = temp();
        let mut q = BlockQueue::open_or_create_with_budget(&dir, 64 * 1024 * 1024).unwrap();
        assert!(q.can_enqueue(1024));
        assert!(q.fill_ratio() >= 0.0);
        let big = vec![0u8; 65 * 1024 * 1024];
        assert!(!q.can_enqueue(big.len()));
        assert!(q.enqueue(1, [9u8; 32], 1, &big).is_err());
        // Unlimited budget: fill_ratio 0.
        let q2 = BlockQueue::open_or_create_with_budget(&dir, u64::MAX).unwrap();
        assert_eq!(q2.fill_ratio(), 0.0);
        assert!(q2.can_enqueue(big.len()));
        // Legacy dir cleanup path.
        let legacy = dir.join("block_queue");
        std::fs::create_dir_all(&legacy).unwrap();
        let _ = BlockQueue::open_or_create(&dir).unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn max_height_span() {
        let dir = temp();
        let mut q = BlockQueue::open_or_create_with_budget(&dir, 64 * 1024 * 1024).unwrap();
        assert_eq!(q.max_height(), None);
        q.enqueue(10, [1u8; 32], 1, b"a").unwrap();
        q.enqueue(15, [2u8; 32], 2, b"b").unwrap();
        q.enqueue(12, [3u8; 32], 3, b"c").unwrap();
        assert_eq!(q.max_height(), Some(15));
        assert_eq!(q.min_height(), Some(10));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn fifo_oldest_and_heights() {
        let dir = temp();
        let mut q = BlockQueue::open_or_create_with_budget(&dir, 64 * 1024 * 1024).unwrap();
        let id1 = q.enqueue(1, [1u8; 32], 1, b"a").unwrap();
        let id2 = q.enqueue(2, [2u8; 32], 2, b"bb").unwrap();
        assert_eq!(q.peek_oldest_id(), Some(id1));
        let mut hs = q.heights();
        hs.sort_unstable();
        assert_eq!(hs, vec![1, 2]);
        assert_eq!(q.id_for_height(2), Some(id2));
        assert_eq!(q.dequeue_height(1).unwrap(), 1);
        assert_eq!(q.peek_oldest_id(), Some(id2));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn list_meta_matches_payload_len() {
        let dir = temp();
        let mut q = BlockQueue::open_or_create_with_budget(&dir, 64 * 1024 * 1024).unwrap();
        q.enqueue(5, [9u8; 32], 1, b"hello").unwrap();
        let m = q.list_meta();
        assert_eq!(m.len(), 1);
        assert_eq!(m[0].payload_len, 5);
        assert_eq!(m[0].height, 5);
        assert!(!m[0].resolve_complete, "enqueue starts unresolved");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn list_meta_reports_resolve_complete() {
        let dir = temp();
        let mut q = BlockQueue::open_or_create_with_budget(&dir, 64 * 1024 * 1024).unwrap();
        q.enqueue(3, [3u8; 32], 1, b"a").unwrap();
        q.enqueue(4, [4u8; 32], 2, b"b").unwrap();
        q.mark_resolve_complete(4).unwrap();
        let m = q.list_meta();
        assert_eq!(m.len(), 2);
        let h3 = m.iter().find(|e| e.height == 3).unwrap();
        let h4 = m.iter().find(|e| e.height == 4).unwrap();
        assert!(!h3.resolve_complete);
        assert!(h4.resolve_complete);
        assert!(q.is_resolve_complete(4));
        assert!(!q.is_resolve_complete(3));
        let id4 = q.id_for_height(4).unwrap();
        assert!(q.dequeue(id4).unwrap());
        let m = q.list_meta();
        assert_eq!(m.len(), 1);
        assert_eq!(m[0].height, 3);
        assert!(!m[0].resolve_complete);
        assert!(!q.is_resolve_complete(4));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn unresolved_heights_skips_complete_and_respects_cap() {
        use std::collections::HashSet;
        let dir = temp();
        let mut q = BlockQueue::open_or_create_with_budget(&dir, 64 * 1024 * 1024).unwrap();
        for h in 0..24u32 {
            q.enqueue(h, [h as u8; 32], 1, b"x").unwrap();
        }
        for h in 0..16u32 {
            q.mark_resolve_complete(h).unwrap();
        }
        let skip: HashSet<u32> = [16, 17].into_iter().collect();
        let got = q.unresolved_heights(10, &skip, 8);
        assert_eq!(got, vec![18, 19, 20, 21, 22, 23]);
        assert_eq!(q.unresolved_heights(10, &skip, 3), vec![18, 19, 20]);
        assert!(q.unresolved_heights(10, &skip, 0).is_empty());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn absolute_budget_refuses_enqueue() {
        let dir = temp();
        // 64 MiB floor still applies — use small payload vs tiny budget override
        // after open: construct with large floor then fill.
        let mut q = BlockQueue::open_or_create_with_budget(&dir, 64 * 1024 * 1024).unwrap();
        let big = vec![0u8; 32 * 1024 * 1024];
        q.enqueue(1, [1u8; 32], 1, &big).unwrap();
        q.enqueue(2, [2u8; 32], 2, &big).unwrap();
        // Third 32 MiB would exceed 64 MiB budget.
        assert!(q.enqueue(3, [3u8; 32], 3, &big).is_err());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn ids_and_get_by_height_and_fill_ratio() {
        let dir = temp();
        let mut q = BlockQueue::open_or_create_with_budget(&dir, 64 * 1024 * 1024).unwrap();
        assert!(q.ids().is_empty());
        assert!(!q.contains_height(1));
        assert!((q.fill_ratio() - 0.0).abs() < 1e-9);
        assert!(q.can_enqueue(1));
        let id = q.enqueue(7, [7u8; 32], 1, b"xyz").unwrap();
        let mut ids = q.ids();
        ids.sort_unstable();
        assert_eq!(ids, vec![id]);
        assert!(q.contains_height(7));
        assert!(!q.contains_height(8));
        assert!(q.get_by_height(99).unwrap().is_none());
        let got = q.get_by_height(7).unwrap().unwrap();
        assert_eq!(got.payload, b"xyz");
        assert_eq!(q.get(id).unwrap().unwrap().payload, b"xyz");
        assert!(q.get(999).unwrap().is_none());
        assert_eq!(q.min_height(), Some(7));
        assert_eq!(q.max_height(), Some(7));
        let _ = q.load_all().unwrap();
        assert!(q.dequeue(id).unwrap());
        assert!(!q.contains_height(7));
        let _ = BlockQueue::budget_from_env();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn resolve_complete_clears_with_dequeue() {
        let dir = temp();
        let mut q = BlockQueue::open_or_create_with_budget(&dir, 64 * 1024 * 1024).unwrap();
        let id = q.enqueue(5, [5u8; 32], 1, b"blk").unwrap();
        q.mark_resolve_complete(5).unwrap();
        assert!(q.is_resolve_complete(5));
        assert!(q.dequeue(id).unwrap());
        assert!(!q.is_resolve_complete(5));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn resolve_complete_clears_with_dequeue_height() {
        let dir = temp();
        let mut q = BlockQueue::open_or_create_with_budget(&dir, 64 * 1024 * 1024).unwrap();
        q.enqueue(9, [9u8; 32], 1, b"a").unwrap();
        q.enqueue(10, [10u8; 32], 2, b"b").unwrap();
        q.mark_resolve_complete(10).unwrap();
        assert_eq!(q.dequeue_height(10).unwrap(), 1);
        assert!(!q.is_resolve_complete(10));
        assert!(q.contains_height(9));
        assert!(!q.is_resolve_complete(9));
        assert!(q.mark_resolve_complete(10).is_err());
        let _ = std::fs::remove_dir_all(&dir);
    }
}
