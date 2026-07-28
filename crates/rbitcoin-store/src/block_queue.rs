//! Durable multi‑GiB on-disk block payload queue for the combined archive/confirm path.
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
//! Capacity is a soft budget on **sum of payload bytes** (default 8 GiB).
//! Override with `RBITCOIN_BLOCK_QUEUE_GB` (integer GiB) or
//! `RBITCOIN_BLOCK_QUEUE_BYTES`.

use crate::error::StoreError;
use std::collections::BTreeMap;
use std::fs::OpenOptions;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

const META_MAGIC: &[u8; 4] = b"BQ01";
const META_VERSION: u32 = 1;
const REC_MAGIC: &[u8; 4] = b"BQR1";

/// Default durable queue budget: 8 GiB of payload.
pub const DEFAULT_BLOCK_QUEUE_BUDGET_BYTES: u64 = 8 * 1024 * 1024 * 1024;

/// One durable queued block.
#[derive(Debug, Clone)]
pub struct QueuedBlock {
    pub id: u64,
    pub height: u32,
    pub hash: [u8; 32],
    pub header_fk: u64,
    pub payload: Vec<u8>,
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
        let budget = budget.max(64 * 1024 * 1024);
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

    pub fn can_enqueue(&self, payload_len: usize) -> bool {
        self.bytes.saturating_add(payload_len as u64) <= self.budget
    }

    /// Override budget (constructor clamps min 64 MiB). For tests / diagnostics.
    pub fn force_budget_for_test(&mut self, budget: u64) {
        self.budget = budget.max(1);
    }

    /// `bytes / budget` (may be 0..1 under normal load; rarely >1 if forced).
    pub fn fill_ratio(&self) -> f64 {
        let b = self.budget.max(1) as f64;
        self.bytes as f64 / b
    }

    /// Append a block payload. Returns queue id.
    ///
    /// Refuses when `bytes + payload.len()` would exceed [`Self::budget`]
    /// ([`Self::can_enqueue`]) with [`StoreError::BudgetFull`] — expected when
    /// the multi‑GiB cap is hit; callers buffer in RAM and gate new getdata.
    pub fn enqueue(
        &mut self,
        height: u32,
        hash: [u8; 32],
        header_fk: u64,
        payload: &[u8],
    ) -> Result<u64, StoreError> {
        if !self.can_enqueue(payload.len()) {
            return Err(StoreError::BudgetFull(
                "block_queue (raise RBITCOIN_BLOCK_QUEUE_GB / _BYTES only if too small)",
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

    /// Load every queued block (restart replay, ascending id).
    pub fn load_all(&self) -> Result<Vec<QueuedBlock>, StoreError> {
        let mut out = Vec::with_capacity(self.index.len());
        for &id in self.index.keys() {
            if let Some(b) = self.get(id)? {
                out.push(b);
            }
        }
        Ok(out)
    }

    /// Heights currently on the durable queue.
    pub fn heights(&self) -> Vec<u32> {
        self.index.values().map(|e| e.height).collect()
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
    fn multi_gib_budget_parse() {
        let b = BlockQueue::budget_from_env();
        assert!(b >= 64 * 1024 * 1024);
        // Default is multi-GiB scale when env unset.
        assert!(
            b >= 1024 * 1024 * 1024 || std::env::var("RBITCOIN_BLOCK_QUEUE_BYTES").is_ok()
                || std::env::var("RBITCOIN_BLOCK_QUEUE_GB").is_ok(),
            "default budget should be multi-GiB unless env overrides"
        );
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
