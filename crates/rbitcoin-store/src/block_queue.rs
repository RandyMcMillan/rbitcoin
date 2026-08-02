//! Durable on-disk block payload queue for the combined archive/confirm path.
//!
//! Layout under `store_dir/block_queue/`:
//! ```text
//! block_queue/
//!   meta.bin          # magic, version, budget_bytes, next_id, count
//!   rec.NNNNNNNN      # one length-prefixed bitcoin block payload (+ header)
//! ```
//!
//! **Lifecycle:** enqueue after peer decode; **dequeue only after combined
//! confirm-write** (or permanent reject). Restart reopens without re-download.
//!
//! **Primary capacity is not bytes** — IBD densify uses a **time-depth soft
//! gate** (~5 min of tip-rate blocks) in the net layer. This type accepts
//! payloads until an optional absolute byte ceiling (env) is hit.
//!
//! Absolute safety ceiling (optional): `RBITCOIN_BLOCK_QUEUE_GB` (integer GiB)
//! or `RBITCOIN_BLOCK_QUEUE_BYTES`. When unset, enqueue is unlimited
//! (`u64::MAX`) aside from disk/IO errors.

use crate::error::StoreError;
use std::collections::BTreeMap;
use std::fs::OpenOptions;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

const META_MAGIC: &[u8; 4] = b"BQ01";
const META_VERSION: u32 = 1;
const REC_MAGIC: &[u8; 4] = b"BQR1";

/// Default absolute byte ceiling: unlimited (soft time-depth gates densify).
pub const DEFAULT_BLOCK_QUEUE_BUDGET_BYTES: u64 = u64::MAX;

/// One durable queued block (full payload — expensive for multi‑GiB queues).
#[derive(Debug, Clone)]
pub struct QueuedBlock {
    pub id: u64,
    pub height: u32,
    pub hash: [u8; 32],
    pub header_fk: u64,
    pub payload: Vec<u8>,
}

/// Index-only view of a durable queue entry (no payload IO).
///
/// Prefer this for restart rehydrate / status: height/hash/header_fk/payload_len
/// live in the in-memory index after open (headers only on disk scan).
#[derive(Debug, Clone, Copy)]
pub struct QueuedBlockMeta {
    pub id: u64,
    pub height: u32,
    pub hash: [u8; 32],
    pub header_fk: u64,
    pub payload_len: u64,
}

/// On-disk FIFO of block payloads (append + delete-by-id).
pub struct BlockQueue {
    dir: PathBuf,
    budget: u64,
    next_id: AtomicU64,
    /// id → (height, hash, header_fk, path, payload_len)
    index: BTreeMap<u64, IndexEntry>,
    bytes: u64,
}

#[derive(Debug, Clone)]
struct IndexEntry {
    height: u32,
    hash: [u8; 32],
    header_fk: u64,
    path: PathBuf,
    payload_len: u64,
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

    pub fn open_or_create(store_dir: &Path) -> Result<Self, StoreError> {
        Self::open_or_create_with_budget(store_dir, Self::budget_from_env())
    }

    pub fn open_or_create_with_budget(store_dir: &Path, budget: u64) -> Result<Self, StoreError> {
        let dir = store_dir.join("block_queue");
        std::fs::create_dir_all(&dir).map_err(|e| StoreError::io(&dir, e))?;
        // Unlimited (u64::MAX) stays unlimited; finite caps keep a 64 MiB floor.
        let budget = if budget == u64::MAX {
            u64::MAX
        } else {
            budget.max(64 * 1024 * 1024)
        };
        let meta_path = dir.join("meta.bin");
        let mut q = if meta_path.exists() {
            Self::load_existing(dir, budget)?
        } else {
            let q = Self {
                dir,
                budget,
                next_id: AtomicU64::new(1),
                index: BTreeMap::new(),
                bytes: 0,
            };
            q.write_meta()?;
            q
        };
        // Refresh budget from call (env may change between restarts).
        q.budget = budget;
        q.write_meta()?;
        Ok(q)
    }

    fn load_existing(dir: PathBuf, budget: u64) -> Result<Self, StoreError> {
        let meta_path = dir.join("meta.bin");
        let raw = std::fs::read(&meta_path).map_err(|e| StoreError::io(&meta_path, e))?;
        if raw.len() < 32 || &raw[0..4] != META_MAGIC {
            return Err(StoreError::Corrupt("block_queue meta magic"));
        }
        let ver = u32::from_le_bytes(raw[4..8].try_into().unwrap());
        if ver != META_VERSION {
            return Err(StoreError::Corrupt("block_queue meta version"));
        }
        let next_id = u64::from_le_bytes(raw[16..24].try_into().unwrap());
        let mut index = BTreeMap::new();
        let mut bytes = 0u64;
        for ent in std::fs::read_dir(&dir).map_err(|e| StoreError::io(&dir, e))? {
            let ent = ent.map_err(|e| StoreError::io(&dir, e))?;
            let name = ent.file_name();
            let name = name.to_string_lossy();
            if !name.starts_with("rec.") {
                continue;
            }
            let id: u64 = match name[4..].parse() {
                Ok(i) => i,
                Err(_) => continue,
            };
            let path = ent.path();
            let (height, hash, header_fk, payload_len) = read_rec_header(&path)?;
            bytes = bytes.saturating_add(payload_len);
            index.insert(
                id,
                IndexEntry {
                    height,
                    hash,
                    header_fk,
                    path,
                    payload_len,
                },
            );
        }
        Ok(Self {
            dir,
            budget,
            next_id: AtomicU64::new(next_id.max(1)),
            index,
            bytes,
        })
    }

    fn write_meta(&self) -> Result<(), StoreError> {
        let path = self.dir.join("meta.bin");
        let mut raw = Vec::with_capacity(32);
        raw.extend_from_slice(META_MAGIC);
        raw.extend_from_slice(&META_VERSION.to_le_bytes());
        raw.extend_from_slice(&self.budget.to_le_bytes());
        raw.extend_from_slice(&self.next_id.load(Ordering::Relaxed).to_le_bytes());
        raw.extend_from_slice(&(self.index.len() as u64).to_le_bytes());
        std::fs::write(&path, &raw).map_err(|e| StoreError::io(&path, e))?;
        Ok(())
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

    /// Append a block payload. Returns queue id.
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
        let path = self.dir.join(format!("rec.{id:08}"));
        let mut f = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
            .map_err(|e| StoreError::io(&path, e))?;
        // header: magic4 | height4 | hash32 | header_fk8 | payload_len8 | payload
        f.write_all(REC_MAGIC).map_err(|e| StoreError::io(&path, e))?;
        f.write_all(&height.to_le_bytes())
            .map_err(|e| StoreError::io(&path, e))?;
        f.write_all(&hash).map_err(|e| StoreError::io(&path, e))?;
        f.write_all(&header_fk.to_le_bytes())
            .map_err(|e| StoreError::io(&path, e))?;
        f.write_all(&(payload.len() as u64).to_le_bytes())
            .map_err(|e| StoreError::io(&path, e))?;
        f.write_all(payload).map_err(|e| StoreError::io(&path, e))?;
        f.sync_all().map_err(|e| StoreError::io(&path, e))?;
        // Idle on disk: drop page cache for this rec so multi‑GiB BQ lead does not
        // pin OS cache (competes with Class A mmap). Plan `get` re-faults as needed.
        advise_rec_dont_need(&f);
        drop(f);
        self.bytes = self.bytes.saturating_add(payload.len() as u64);
        self.index.insert(
            id,
            IndexEntry {
                height,
                hash,
                header_fk,
                path,
                payload_len: payload.len() as u64,
            },
        );
        self.write_meta()?;
        Ok(id)
    }

    /// Load payload by id (restart path).
    pub fn get(&self, id: u64) -> Result<Option<QueuedBlock>, StoreError> {
        let Some(e) = self.index.get(&id) else {
            return Ok(None);
        };
        let payload = read_rec_payload(&e.path)?;
        Ok(Some(QueuedBlock {
            id,
            height: e.height,
            hash: e.hash,
            header_fk: e.header_fk,
            payload,
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
        self.bytes = self.bytes.saturating_sub(e.payload_len);
        if e.path.exists() {
            std::fs::remove_file(&e.path).map_err(|e2| StoreError::io(&e.path, e2))?;
        }
        self.write_meta()?;
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

    /// Index-only listing (ascending id). **No payload read** — O(n) over the
    /// in-memory index after open.
    ///
    /// Production IBD restart must use this (or equivalent meta walk), **not**
    /// [`Self::load_all`], which materializes multi‑GiB of wire into heap.
    pub fn list_meta(&self) -> Vec<QueuedBlockMeta> {
        self.index
            .iter()
            .map(|(&id, e)| QueuedBlockMeta {
                id,
                height: e.height,
                hash: e.hash,
                header_fk: e.header_fk,
                payload_len: e.payload_len,
            })
            .collect()
    }

    /// Load every queued block with full payload (tests / tools only).
    ///
    /// **Do not call on production multi‑GiB queues** — peak RAM ≈ disk fill.
    /// Prefer [`Self::list_meta`] + [`Self::get`] / [`Self::get_by_height`] for
    /// single heights (confirm prep).
    pub fn load_all(&self) -> Result<Vec<QueuedBlock>, StoreError> {
        let mut out = Vec::with_capacity(self.index.len());
        for &id in self.index.keys() {
            if let Some(b) = self.get(id)? {
                out.push(b);
            }
        }
        Ok(out)
    }

    /// Load payload for a height (confirm prep intake). First match by height.
    ///
    /// Does **not** dequeue — confirm-write / permanent reject removes the rec.
    pub fn get_by_height(&self, height: u32) -> Result<Option<QueuedBlock>, StoreError> {
        let Some((&id, _)) = self.index.iter().find(|(_, e)| e.height == height) else {
            return Ok(None);
        };
        self.get(id)
    }

    /// True if any durable rec exists for `height` (O(n) index walk).
    pub fn contains_height(&self, height: u32) -> bool {
        self.index.values().any(|e| e.height == height)
    }

    /// Heights currently on the durable queue.
    pub fn heights(&self) -> Vec<u32> {
        self.index.values().map(|e| e.height).collect()
    }

    /// Highest height among durable recs (`None` if empty).
    ///
    /// Used by densify gap-fill: holes in `tip+1..=max_height` are always
    /// assigned even when soft time-depth pressure is latched.
    pub fn max_height(&self) -> Option<u32> {
        self.index.values().map(|e| e.height).max()
    }

    /// Lowest height among durable recs (`None` if empty).
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
}

/// Best-effort `POSIX_FADV_DONTNEED` so durable BQ recs are idle files, not cache.
///
/// Linux only; no-op elsewhere. Failures are ignored (same spirit as store body advise).
fn advise_rec_dont_need(f: &std::fs::File) {
    #[cfg(target_os = "linux")]
    {
        use std::os::unix::io::AsRawFd;
        let fd = f.as_raw_fd();
        // offset=0, len=0 → whole file
        let rc = unsafe {
            libc::posix_fadvise(fd, 0, 0, libc::POSIX_FADV_DONTNEED)
        };
        if rc != 0 {
            rbitcoin_log::trace!(
                "block_queue: posix_fadvise(DONTNEED) failed: {}",
                std::io::Error::from_raw_os_error(rc)
            );
        }
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = f;
    }
}

fn read_rec_header(path: &Path) -> Result<(u32, [u8; 32], u64, u64), StoreError> {
    let mut f = std::fs::File::open(path).map_err(|e| StoreError::io(path, e))?;
    let mut hdr = [0u8; 4 + 4 + 32 + 8 + 8];
    f.read_exact(&mut hdr).map_err(|e| StoreError::io(path, e))?;
    if &hdr[0..4] != REC_MAGIC {
        return Err(StoreError::Corrupt("block_queue rec magic"));
    }
    let height = u32::from_le_bytes(hdr[4..8].try_into().unwrap());
    let mut hash = [0u8; 32];
    hash.copy_from_slice(&hdr[8..40]);
    let header_fk = u64::from_le_bytes(hdr[40..48].try_into().unwrap());
    let payload_len = u64::from_le_bytes(hdr[48..56].try_into().unwrap());
    Ok((height, hash, header_fk, payload_len))
}

fn read_rec_payload(path: &Path) -> Result<Vec<u8>, StoreError> {
    let mut f = std::fs::File::open(path).map_err(|e| StoreError::io(path, e))?;
    let mut hdr = [0u8; 56];
    f.read_exact(&mut hdr).map_err(|e| StoreError::io(path, e))?;
    if &hdr[0..4] != REC_MAGIC {
        return Err(StoreError::Corrupt("block_queue rec magic"));
    }
    let payload_len = u64::from_le_bytes(hdr[48..56].try_into().unwrap()) as usize;
    let mut payload = vec![0u8; payload_len];
    f.read_exact(&mut payload)
        .map_err(|e| StoreError::io(path, e))?;
    Ok(payload)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    fn temp() -> PathBuf {
        static N: AtomicU64 = AtomicU64::new(0);
        let id = N.fetch_add(1, Ordering::Relaxed);
        let d = std::env::temp_dir().join(format!("rbitcoin-bq-{id}"));
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
    fn enqueue_reopen_dequeue() {
        let dir = temp();
        let mut q = BlockQueue::open_or_create_with_budget(&dir, 64 * 1024 * 1024).unwrap();
        assert!(q.budget() >= 64 * 1024 * 1024);
        let payload = b"fake-block-bytes-for-queue".to_vec();
        let id = q
            .enqueue(100, [0xABu8; 32], 7, &payload)
            .unwrap();
        assert_eq!(q.count(), 1);
        assert!(!q.can_enqueue(q.budget() as usize)); // nearly full only if payload huge — skip

        // Restart: reopen
        drop(q);
        let mut q2 = BlockQueue::open_or_create_with_budget(&dir, 64 * 1024 * 1024).unwrap();
        assert_eq!(q2.count(), 1);
        let got = q2.get(id).unwrap().expect("present after reopen");
        assert_eq!(got.height, 100);
        assert_eq!(got.hash, [0xABu8; 32]);
        assert_eq!(got.payload, payload);
        assert!(q2.dequeue(id).unwrap());
        assert_eq!(q2.count(), 0);
        assert!(q2.get(id).unwrap().is_none());

        // Still gone after third open
        drop(q2);
        let q3 = BlockQueue::open_or_create_with_budget(&dir, 64 * 1024 * 1024).unwrap();
        assert_eq!(q3.count(), 0);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn default_budget_unlimited_unless_env() {
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
        q.dequeue(id1).unwrap();
        assert_eq!(q.peek_oldest_id(), Some(id2));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn list_meta_no_payload_io() {
        let dir = temp();
        let mut q = BlockQueue::open_or_create_with_budget(&dir, 64 * 1024 * 1024).unwrap();
        let big = vec![0xABu8; 2 * 1024 * 1024];
        let id = q.enqueue(9, [0x11u8; 32], 3, &big).unwrap();
        let meta = q.list_meta();
        assert_eq!(meta.len(), 1);
        assert_eq!(meta[0].id, id);
        assert_eq!(meta[0].height, 9);
        assert_eq!(meta[0].hash, [0x11u8; 32]);
        assert_eq!(meta[0].header_fk, 3);
        assert_eq!(meta[0].payload_len, big.len() as u64);
        // Still on disk; get still works for prep path.
        assert_eq!(q.get(id).unwrap().unwrap().payload, big);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn enqueue_dontneed_still_readable() {
        // DONTNEED is best-effort; payload must remain durable and readable.
        let dir = temp();
        let mut q = BlockQueue::open_or_create_with_budget(&dir, 64 * 1024 * 1024).unwrap();
        let payload = b"dontneed-still-on-disk-payload".to_vec();
        let id = q.enqueue(7, [0xDEu8; 32], 1, &payload).unwrap();
        let got = q.get(id).unwrap().expect("readable after DONTNEED");
        assert_eq!(got.payload, payload);
        assert_eq!(got.height, 7);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn enqueue_rejects_when_over_budget() {
        let dir = temp();
        // Tiny budget: first small payload ok; second large fails.
        let mut q = BlockQueue::open_or_create_with_budget(&dir, 64 * 1024 * 1024).unwrap();
        // Force tiny budget after open (constructor clamps min 64MiB).
        q.budget = 100;
        q.bytes = 0;
        q.enqueue(1, [1u8; 32], 1, b"small").unwrap();
        assert!(!q.can_enqueue(200));
        let err = q.enqueue(2, [2u8; 32], 2, &vec![0u8; 200]).unwrap_err();
        assert!(
            matches!(err, StoreError::BudgetFull(_)),
            "budget full is soft, not corrupt: {err}"
        );
        assert_eq!(q.count(), 1, "failed enqueue must not leave a rec");
        assert!(q.fill_ratio() > 0.0);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
