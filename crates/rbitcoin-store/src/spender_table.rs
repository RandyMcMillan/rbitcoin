//! Multi-spender overflow (`spent.ovf`, schema 22).
//!
//! Common case stores a sole `spending_tx_fk` + vin on the create output. Only
//! when an outpoint has multiple annotated spenders do we allocate nodes here.
//!
//! Fixed 16 B records (1-based fk): packed `(fk:u40, vin:u16)` | `next:u64`.
//!
//! An append writes past the durable end and moves only the RAM length.
//! [`SpenderTable::flush`] syncs the nodes, then writes and syncs the length
//! that names them, so the durable length is always that of some flush. A
//! crash keeps the nodes below it and ignores the bytes past it, with no
//! mark (the sector rule, crash-recovery.md).

use crate::error::StoreError;
use crate::file::{TableFile, FILE_HEADER_LEN};
use crate::tx_table::{pack_spent_field, unpack_spent_field};
use rbitcoin_primitives::{le_u64, Fk, TableKind};
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

pub const SPENDER_RECORD_LEN: usize = 16;
/// Multi-spender overflow next to `spent.body` (schema 17).
pub const SPENT_OVF_NAME: &str = "spent.ovf";
#[doc(hidden)]
pub const LEGACY_SPENDERS_BODY: &str = "spenders.body";

pub struct SpenderTable {
    pub body: TableFile,
    pub count: AtomicU64,
    /// A body sync failed. A retry can return 0 after the kernel dropped
    /// the dirty pages, so every later flush is refused until restart and
    /// no length is published over them.
    sync_failed: AtomicBool,
}

#[doc(hidden)]
pub fn spent_ovf_path(dir: &Path) -> Result<std::path::PathBuf, StoreError> {
    let ovf = dir.join(SPENT_OVF_NAME);
    let legacy = dir.join(LEGACY_SPENDERS_BODY);
    match (ovf.exists(), legacy.exists()) {
        (true, true) => {
            eprintln!(
                "store: dropping leftover {LEGACY_SPENDERS_BODY} (schema 17 uses {SPENT_OVF_NAME})"
            );
            let _ = std::fs::remove_file(&legacy);
        }
        (false, true) => {
            eprintln!("store: renaming {LEGACY_SPENDERS_BODY} → {SPENT_OVF_NAME}");
            std::fs::rename(&legacy, &ovf).map_err(|e| StoreError::io(&legacy, e))?;
        }
        _ => {}
    }
    Ok(ovf)
}

impl SpenderTable {
    pub fn create(dir: &Path) -> Result<Self, StoreError> {
        let body = TableFile::create(dir.join(SPENT_OVF_NAME), TableKind::Spender)?;
        Ok(Self {
            body,
            count: AtomicU64::new(0),
            sync_failed: AtomicBool::new(false),
        })
    }

    pub fn open(dir: &Path) -> Result<Self, StoreError> {
        let path = spent_ovf_path(dir)?;
        if !path.exists() {
            return Self::create(dir);
        }
        let body = TableFile::open(path, TableKind::Spender)?;
        let body_len = body.logical_len().saturating_sub(FILE_HEADER_LEN as u64);
        if body_len % SPENDER_RECORD_LEN as u64 != 0 {
            return Err(StoreError::Corrupt("spenders body size"));
        }
        let count = body_len / SPENDER_RECORD_LEN as u64;
        Ok(Self {
            body,
            count: AtomicU64::new(count),
            sync_failed: AtomicBool::new(false),
        })
    }

    pub fn count(&self) -> u64 {
        self.count.load(Ordering::Acquire)
    }

    #[doc(hidden)]
    pub fn offset(id: u64) -> u64 {
        FILE_HEADER_LEN as u64 + (id - 1) * SPENDER_RECORD_LEN as u64
    }

    /// Append one list node. Returns its fk.
    pub fn append(&self, spending_tx_fk: Fk, vin: u32, next: Fk) -> Result<Fk, StoreError> {
        if spending_tx_fk.is_null() {
            return Err(StoreError::InvalidFk);
        }
        let packed = pack_spent_field(spending_tx_fk, vin)?;
        // Single annotator role: load → write → publish count.
        let id = self.count.load(Ordering::Acquire) + 1;
        let mut buf = [0u8; SPENDER_RECORD_LEN];
        buf[0..8].copy_from_slice(&packed.to_le_bytes());
        buf[8..16].copy_from_slice(&next.0.to_le_bytes());
        let at = Self::offset(id);
        self.body.write_uncommitted(at, &buf)?;
        // RAM only: the header keeps the last flushed length.
        let _ = self.body.publish_end(at + SPENDER_RECORD_LEN as u64, false);
        self.count.store(id, Ordering::Release);
        Ok(Fk(id))
    }

    pub fn get(&self, fk: Fk) -> Result<(Fk, u32, Fk), StoreError> {
        let id = fk.get().ok_or(StoreError::InvalidFk)?;
        let count = self.count.load(Ordering::Acquire);
        if id == 0 || id > count {
            return Err(StoreError::NotFound);
        }
        let mut buf = [0u8; SPENDER_RECORD_LEN];
        self.body.read_at(Self::offset(id), &mut buf)?;
        let packed = le_u64!(buf, 0);
        let (spend_tx, vin) = unpack_spent_field(packed)?;
        Ok((spend_tx, vin, Fk(le_u64!(buf, 8))))
    }

    /// Sync the nodes, then write and sync the length that names them.
    pub fn flush(&self) -> Result<(), StoreError> {
        self.refuse_if_sync_failed()?;
        self.finish_sync(self.publish_synced())
    }

    /// A flush runs without the UTXO view, so an annotate append can land
    /// during it. The end is sampled before the sync: every append that
    /// published it had already written its node. One that lands after the
    /// sample stays past the header until the next flush.
    fn publish_synced(&self) -> Result<(), StoreError> {
        let end = self.body.logical_len();
        self.body.sync_data_only()?;
        #[cfg(debug_assertions)]
        run_after_node_sync(self);
        self.body.publish_synced_len(end)
    }

    fn refuse_if_sync_failed(&self) -> Result<(), StoreError> {
        if self.sync_failed.load(Ordering::Acquire) {
            return Err(StoreError::io(
                self.body.path(),
                std::io::Error::other("an earlier spent.ovf sync failed; restart the node"),
            ));
        }
        Ok(())
    }

    /// Latch the first sync error. A retried fsync can return 0 after the
    /// kernel dropped the dirty pages, so no later flush publishes a length.
    fn finish_sync(&self, synced: Result<(), StoreError>) -> Result<(), StoreError> {
        if synced.is_err() {
            self.sync_failed.store(true, Ordering::Release);
        }
        synced
    }

    pub fn flush_async(&self) -> Result<(), StoreError> {
        self.body.flush_async()
    }
}

#[cfg(debug_assertions)]
type AfterNodeSync = Box<dyn FnOnce(&SpenderTable)>;

#[cfg(debug_assertions)]
thread_local! {
    static AFTER_NODE_SYNC: std::cell::RefCell<Option<AfterNodeSync>> =
        const { std::cell::RefCell::new(None) };
}

/// Run `hook` once on this thread, after a flush's node sync and before its
/// length write.
#[cfg(debug_assertions)]
#[doc(hidden)]
pub fn set_after_node_sync(hook: Option<AfterNodeSync>) {
    AFTER_NODE_SYNC.with(|slot| *slot.borrow_mut() = hook);
}

#[cfg(debug_assertions)]
fn run_after_node_sync(table: &SpenderTable) {
    if let Some(hook) = AFTER_NODE_SYNC.with(|slot| slot.borrow_mut().take()) {
        hook(table);
    }
}
