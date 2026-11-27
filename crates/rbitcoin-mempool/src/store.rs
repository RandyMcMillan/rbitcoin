//! Private mempool durability under `{datadir}/mempool/` (not Class A).
//!
//! # Namespace (important)
//!
//! | Path | Role |
//! |------|------|
//! | `{datadir}/store/tx.body` | **Class A** confirmed archive — confirm commit sole writer |
//! | `{datadir}/mempool/tx.body` | **This file** — unconfirmed live set only |
//!
//! Schema **4** packed live records (`fee‖weight‖sigops‖accept_secs‖…`); schema 1–3
//! recode on open. Body is append-only (`body_persisted_len`);
//! `persist_due` appends the new records, syncs them, then writes and syncs
//! the body length, and only then publishes the new LIVE slots and meta as
//! one `guard/flush` mark. Compact still renames packed images. An admit
//! compacts first when dead payloads reach the live payload bytes, so a full
//! pool that reuses the slot each eviction frees does not grow `tx.body` until
//! a block. DEAD of a durable slot is folded into that same flush. A batch
//! writes the status bytes and `meta` once.
//! Open keeps LIVE rows inside the logical body and returns [`MempoolError::Corrupt`]
//! for an in-range payload that does not decode. The hub moves that image aside.
//!
//! # Transport (phase 5b M2)
//!
//! Process-owned buffers (`meta` fields + `slots` / `body` `Vec`s) are the
//! source of truth. Sidecar files are updated with normal `read`/`write` /
//! `pwrite`-style IO — **no `memmap2`**. Flush bumps generation and commits
//! through the same `guard/flush` mark.

use crate::error::MempoolError;
use crate::packed::{
    decode_packed_live, decode_packed_live_v2, decode_packed_live_v3, encode_packed_live,
    PackedLive, VinAux, ACCEPT_SECS_OFF, SIGOP_COST_OFF,
};
use bitcoin::consensus::encode::deserialize;
use bitcoin::hashes::Hash;
use bitcoin::{Transaction, Txid, Wtxid};
use rbitcoin_primitives::{bytes, le_u16, le_u32, le_u64};
use rbitcoin_store::durability::Durable as _;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::time::Instant;

/// File magic: `rBMP` (rbitcoin mempool).
pub const MEM_MAGIC: [u8; 4] = *b"rBMP";
/// Schema version for the meta header.
pub const MEM_SCHEMA: u16 = 4;

#[doc(hidden)]
pub const META_LEN: usize = 64;
/// Initial slot table capacity (records).
///
/// Sized for mainnet tip mempool under a ~300 MvB weight budget: many small
/// txs fit weight-wise long before 4k slots fill (overnight tip stall). Fixed
/// constant — no env. Existing datadirs with smaller caps grow on demand.
#[doc(hidden)]
pub const DEFAULT_SLOT_CAP: u32 = 131_072;
/// Hard ceiling when doubling the slot table (DoS / RAM bound).
#[doc(hidden)]
pub const MAX_SLOT_CAP: u32 = 1_048_576;
/// Slot record: status(1) + trust(1) + pad(2) + body_off(8) + body_len(4) + txid(32) = 48.
///
/// `trust` is 1 when this live tx was warn-and-applied from the trusted
/// session. Schema 4 already had these bytes. Older files leave them 0.
#[doc(hidden)]
pub const SLOT_REC: usize = 48;
/// The device writes a 512-byte sector whole (docs/crash-recovery.md).
const SECTOR: usize = 512;

/// One slot record. `off` names a record inside a table of
/// `SLOTS_HEADER + cap * SLOT_REC` bytes.
pub(crate) fn slot_rec(slots: &[u8], off: usize) -> [u8; SLOT_REC] {
    let mut rec = [0u8; SLOT_REC];
    rec.copy_from_slice(&slots[off..off + SLOT_REC]);
    rec
}
#[doc(hidden)]
pub const SLOTS_HEADER: usize = 16;
#[doc(hidden)]
pub const BODY_HEADER: usize = 16;
/// Prefix before each packed live record in `mempool/tx.body` (min size).
#[doc(hidden)]
pub const BODY_TX_PREFIX: usize = 88;
/// Schema 1 payload: `fee(8)‖weight(8)‖bitcoin-serialize`.
#[doc(hidden)]
pub const V1_BODY_PREFIX: usize = 16;

#[doc(hidden)]
pub const SLOT_FREE: u8 = 0;
#[doc(hidden)]
pub const SLOT_LIVE: u8 = 1;
#[doc(hidden)]
pub const SLOT_DEAD: u8 = 2;

/// Snapshot of durable meta fields.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MempoolMeta {
    /// Last committed generation (bumped on flush).
    pub generation: u64,
    /// Slot table capacity (record count).
    pub slot_cap: u32,
    /// Number of LIVE slots.
    pub live_count: u32,
}

/// One LIVE payload from [`Mempool::load_live_txs`].
#[derive(Debug, Clone)]
pub struct LiveTx {
    pub slot: u32,
    pub packed: PackedLive,
    /// Slot trust byte. Set for a warn-and-applied tx from the trusted session.
    pub trust_kept: bool,
}

/// Time-based sidecar persist interval (admits). Crash may lose ≤ this window.
///
/// DEAD of an already-durable slot is an immediate `pwrite` (not this timer).
/// Structural ops (slot grow, compact) and [`Mempool::flush`] persist immediately.
pub const PERSIST_INTERVAL_MS: u64 = 5_000;

/// Durable mempool under `dir` (`{datadir}/mempool`) — InRam buffers + file IO.
pub struct Mempool {
    pub dir: PathBuf,
    pub meta_file: File,
    pub slots_file: File,
    pub body_file: File,
    /// Full slots image (header + records).
    pub slots: Vec<u8>,
    /// Full body image including header; logical length in `body[8..16]`.
    pub body: Vec<u8>,
    pub generation: u64,
    pub slot_cap: u32,
    pub live_count: u32,
    /// Sum of the LIVE payload lengths named by the slot table.
    pub live_body_bytes: u64,
    /// True when `tx.body` grew since last persist (admits).
    pub body_dirty: bool,
    /// On-disk body length that slots may legally index (header + durable payloads).
    pub body_persisted_len: u64,
    /// Durable body length, synced and published. The next append starts
    /// here. It runs ahead of `body_persisted_len` while the slots naming
    /// those records are unpublished, and it bounds the deaths a removal
    /// queues: any slot naming a durable record may be LIVE on disk.
    body_synced_len: u64,
    pub opened: Instant,
    pub mock_now_ms: Option<u64>,
    pub last_persist_ms: u64,
    /// Tests pin the table at the live count so the next admit evicts.
    pub grow_pinned: bool,
    /// Next [`Self::append_live_tx`] returns [`MempoolError::Full`] once.
    #[cfg(debug_assertions)]
    pub fail_next_append: bool,
    /// Durable DEAD rows waiting for the next flush mark.
    pub deaths_unsynced: bool,
    /// Slots whose status byte the next flush mark must publish.
    death_slots: Vec<u32>,
    /// The live slot table is `slots.tmp`: a compact renamed the body but not
    /// the slots. Writes stay there until a persist or the next open links it.
    slots_pending: bool,
    /// `slots` file length known durable. A `set_len` whose sync failed is
    /// visible in the file length but not counted here, so a retry syncs it.
    slots_len_synced: u64,
    /// `guard/flush` may be published and not cleared. It may hold a LIVE
    /// slot image whose body never became durable, so a removal of that slot
    /// must still publish its death. The next commit applies it first.
    flush_mark_armed: bool,
}

impl Mempool {
    /// Create `dir` if needed and open (or initialize) meta/slots/body into RAM.
    pub fn open_or_create(dir: impl Into<PathBuf>) -> Result<Self, MempoolError> {
        let dir = dir.into();
        fs::create_dir_all(&dir).map_err(|e| MempoolError::io(&dir, e))?;
        // A mark armed after a compact's slots rename failed names
        // `slots.tmp`. Apply it before that file is linked into place.
        restore_mempool_mark(&dir)?;
        finish_pending_compact(&dir)?;

        let meta_path = dir.join("meta");
        let slots_path = dir.join("slots");
        let body_path = dir.join("tx.body");

        let (meta_file, generation, slot_cap, live_count, meta_schema) =
            open_or_init_meta(&meta_path)?;
        let (slots_file, slots) = open_or_init_slots(&slots_path, slot_cap)?;
        let slots_len_synced = slots_file
            .metadata()
            .map_err(|e| MempoolError::io(&slots_path, e))?
            .len();
        let (body_file, body) = open_or_init_body(&body_path)?;
        let body_persisted_len = body_logical_len(&body)? as u64;

        let mut mp = Self {
            dir,
            meta_file,
            slots_file,
            body_file,
            slots,
            body,
            generation,
            slot_cap,
            live_count,
            live_body_bytes: 0,
            body_dirty: false,
            body_persisted_len,
            body_synced_len: body_persisted_len,
            opened: Instant::now(),
            mock_now_ms: None,
            last_persist_ms: 0,
            grow_pinned: false,
            #[cfg(debug_assertions)]
            fail_next_append: false,
            deaths_unsynced: false,
            death_slots: Vec::new(),
            slots_pending: false,
            slots_len_synced,
            flush_mark_armed: false,
        };
        let mut body_hdr = [0u8; BODY_HEADER];
        body_hdr.copy_from_slice(&mp.body[..BODY_HEADER]);
        let body_schema = le_u16!(body_hdr, 4);
        let mut slots_hdr = [0u8; SLOTS_HEADER];
        slots_hdr.copy_from_slice(&mp.slots[..SLOTS_HEADER]);
        let slots_schema = le_u16!(slots_hdr, 4);
        if body_schema != MEM_SCHEMA {
            mp.migrate_to_current(body_schema)?;
        } else if meta_schema != MEM_SCHEMA || slots_schema != MEM_SCHEMA {
            // Crash after the migrate renamed body+slots, before meta.
            mp.slots[4..6].copy_from_slice(&MEM_SCHEMA.to_le_bytes());
            mp.body[4..6].copy_from_slice(&MEM_SCHEMA.to_le_bytes());
            mp.persist_slots_and_meta()?;
        }
        // Validate before deleting abandoned compact tmps so a corrupt image
        // can still be moved aside with those tmps.
        mp.load_live_txs()?;
        mp.drop_slots_past_body()?;
        mp.live_body_bytes = mp.scan_live_body_bytes();
        discard_abandoned_tmps(&mp.dir)?;
        Ok(mp)
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// The slot-table file this session writes: `slots.tmp` until a failed
    /// slots rename is linked, then `slots`.
    fn slots_path(&self) -> PathBuf {
        if self.slots_pending {
            self.dir.join("slots.tmp")
        } else {
            self.dir.join("slots")
        }
    }

    pub fn meta(&self) -> MempoolMeta {
        MempoolMeta {
            generation: self.generation,
            slot_cap: self.slot_cap,
            live_count: self.live_count,
        }
    }

    pub fn generation(&self) -> u64 {
        self.generation
    }

    pub fn live_count(&self) -> u32 {
        self.live_count
    }

    /// Test / rebuild helper: set live_count without scanning.
    pub fn set_live_count(&mut self, n: u32) {
        self.live_count = n;
    }

    /// Persist buffers, bump generation, and fsync sidecar files.
    ///
    /// Admits persist on [`Self::persist_due`] (5 s, one `guard/flush` mark).
    /// A crash may lose admits since the last persist.
    /// [`Self::flush`] is the durable checkpoint (generation + the same mark).
    pub fn flush(&mut self) -> Result<(), MempoolError> {
        self.generation = self.generation.saturating_add(1);
        self.persist_body_then_slots()
    }

    /// Time-based body persist: dirty admits wait [`PERSIST_INTERVAL_MS`].
    ///
    /// The body tail and its length are synced first. One mark then covers
    /// the new LIVE slot records, any durable DEAD bytes, and meta. Deaths do not wait out the admit interval.
    /// Flush / grow / compact still rewrite the full slot table. The body
    /// file is not renamed.
    pub fn persist_due(&mut self) -> Result<(), MempoolError> {
        let body_due = self.body_dirty
            && self.now_ms().saturating_sub(self.last_persist_ms) >= PERSIST_INTERVAL_MS;
        if !body_due {
            if self.deaths_unsynced {
                self.sync_deaths()?;
            }
            return Ok(());
        }
        let old_persisted = self.body_persisted_len;
        let mut ranges = self.slot_ranges_since(old_persisted);
        ranges.extend(self.death_ranges());
        self.commit_guarded(true, &ranges, None, true, &[], true)?;
        self.clear_dirty();
        self.last_persist_ms = self.now_ms();
        Ok(())
    }

    /// Best-effort alias of [`Self::persist_due`] (no generation bump).
    pub fn persist_if_dirty(&mut self) -> Result<(), MempoolError> {
        self.persist_due()
    }

    #[doc(hidden)]
    pub fn now_ms(&self) -> u64 {
        self.mock_now_ms
            .unwrap_or_else(|| self.opened.elapsed().as_millis() as u64)
    }

    /// Test clock. Production uses [`Instant`] elapsed from open.
    pub fn set_now_ms(&mut self, ms: u64) {
        self.mock_now_ms = Some(ms);
    }

    pub fn body_persisted_len(&self) -> u64 {
        self.body_persisted_len
    }

    #[doc(hidden)]
    pub fn clear_dirty(&mut self) {
        self.body_dirty = false;
    }

    #[doc(hidden)]
    pub fn persist_body_then_slots(&mut self) -> Result<(), MempoolError> {
        let logical = body_logical_len(&self.body)? as u64;
        let saved = self.body_persisted_len;
        self.body_persisted_len = logical;
        let demoted = self.demoted_slots_image();
        self.body_persisted_len = saved;
        let mut stashed = None;
        if demoted.is_none() {
            stashed = Some(std::mem::take(&mut self.slots));
        }
        let ram: &[u8] = match demoted.as_deref() {
            Some(bytes) => bytes,
            None => stashed.as_deref().unwrap_or(&[]),
        };
        let committed = self.commit_guarded(true, &[], Some(ram), true, &[], true);
        if let Some(slots) = stashed {
            self.slots = slots;
        }
        committed?;
        self.promote_pending_slots();
        self.clear_dirty();
        self.last_persist_ms = self.now_ms();
        Ok(())
    }

    /// Two slot records share one `pwrite` when the next record starts where the last ended.
    #[doc(hidden)]
    pub fn adjacent_slot_records(prev_end: usize, off: usize) -> bool {
        prev_end == off
    }

    /// LIVE rows whose payload starts at or past `old_persisted`.
    ///
    /// Adjacent new slots are one range. The bytes outside those ranges stay
    /// as they are on disk.
    fn slot_ranges_since(&self, old_persisted: u64) -> Vec<(usize, usize)> {
        let mut ranges: Vec<(usize, usize)> = Vec::new();
        for slot in 0..self.slot_cap {
            let off = SLOTS_HEADER + (slot as usize) * SLOT_REC;
            if self.slots[off] != SLOT_LIVE {
                continue;
            }
            let rec = slot_rec(&self.slots, off);
            let body_off = le_u64!(rec, 4);
            if body_off < old_persisted {
                continue;
            }
            let rec_end = off + SLOT_REC;
            if let Some((_, end)) = ranges.last_mut() {
                if Self::adjacent_slot_records(*end, off) {
                    *end = rec_end;
                    continue;
                }
            }
            ranges.push((off, rec_end));
        }
        ranges
    }

    fn death_ranges(&self) -> Vec<(usize, usize)> {
        self.death_slots
            .iter()
            .map(|slot| {
                let off = SLOTS_HEADER + (*slot as usize) * SLOT_REC;
                (off, off + 1)
            })
            .collect()
    }

    /// Append a packed live record; mark a FREE slot LIVE.
    ///
    /// Returns the slot index. RAM is updated immediately; sidecar write waits
    /// for [`Self::persist_due`].
    #[allow(clippy::too_many_arguments)] // one arg per packed-record field
    pub fn append_live_tx(
        &mut self,
        tx: &Transaction,
        txid: &Txid,
        wtxid: &Wtxid,
        fee_sat: u64,
        weight: u64,
        sigop_cost: u64,
        accept_secs: u64,
        vins: &[VinAux],
    ) -> Result<u32, MempoolError> {
        let payload = encode_packed_live(
            tx,
            txid,
            wtxid,
            fee_sat,
            weight,
            Some(sigop_cost),
            accept_secs,
            vins,
        )?;
        #[cfg(debug_assertions)]
        if self.fail_next_append {
            self.fail_next_append = false;
            return Err(MempoolError::Full);
        }
        if u32::try_from(payload.len()).is_err() {
            return Err(MempoolError::Corrupt("tx body too large"));
        }
        let payload_len = payload.len();
        let body_off = self.reserve_body(payload_len)?;
        let off = body_off as usize;
        self.body[off..off + payload_len].copy_from_slice(&payload);
        let slot = self.alloc_slot()?;
        self.write_slot(slot, SLOT_LIVE, body_off, payload_len as u32, txid)?;
        self.live_count = self.live_count.saturating_add(1);
        self.live_body_bytes = self.live_body_bytes.saturating_add(payload_len as u64);
        self.body_dirty = true;
        Ok(slot)
    }

    /// Drop every LIVE slot (Core `-persistmempool=0` start: do not reload).
    pub fn abandon_live(&mut self) -> Result<u32, MempoolError> {
        let mut n = 0u32;
        for slot in 0..self.slot_cap {
            let off = SLOTS_HEADER + (slot as usize) * SLOT_REC;
            if self.slots[off] == SLOT_LIVE {
                self.slots[off] = SLOT_DEAD;
                n = n.saturating_add(1);
            }
        }
        if n > 0 {
            self.live_count = 0;
            self.live_body_bytes = 0;
            self.flush()?;
        }
        Ok(n)
    }

    /// Mark slot DEAD and decrement live_count (confirm / RBF / eviction).
    ///
    /// If the slot is already on disk, the next flush mark publishes that
    /// status byte. An admit that was never durable stays RAM-only. This path
    /// must not dump the full slot table (LIVE rows whose body is still in
    /// the unpersisted tail).
    pub fn mark_slot_dead(&mut self, slot: u32) -> Result<(), MempoolError> {
        if slot >= self.slot_cap {
            return Err(MempoolError::Corrupt("slot OOB"));
        }
        let off = SLOTS_HEADER + (slot as usize) * SLOT_REC;
        if self.slots[off] != SLOT_LIVE {
            return Ok(());
        }
        let rec = slot_rec(&self.slots, off);
        let body_off = le_u64!(rec, 4);
        let body_len = le_u32!(rec, 12) as u64;
        self.slots[off] = SLOT_DEAD;
        self.live_count = self.live_count.saturating_sub(1);
        self.live_body_bytes = self.live_body_bytes.saturating_sub(body_len);
        // A slot whose record is durable may be LIVE on disk: a persist, or
        // a mark open or a retry restored, can have published it. An armed
        // flush mark may name it LIVE too: open would replay it.
        if body_off.saturating_add(body_len) <= self.body_synced_len || self.flush_mark_armed {
            self.death_slots.push(slot);
            self.deaths_unsynced = true;
        }
        Ok(())
    }

    /// Sum the payload lengths of every LIVE row.
    fn scan_live_body_bytes(&self) -> u64 {
        let mut n = 0u64;
        for slot in 0..self.slot_cap {
            let off = SLOTS_HEADER + (slot as usize) * SLOT_REC;
            if self.slots[off] != SLOT_LIVE {
                continue;
            }
            let rec = slot_rec(&self.slots, off);
            n = n.saturating_add(u64::from(le_u32!(rec, 12)));
        }
        n
    }

    /// Count FREE / LIVE / DEAD slots (for compaction triggers).
    pub fn slot_stats(&self) -> (u32, u32, u32) {
        let mut free = 0u32;
        let mut live = 0u32;
        let mut dead = 0u32;
        for slot in 0..self.slot_cap {
            let off = SLOTS_HEADER + (slot as usize) * SLOT_REC;
            match self.slots[off] {
                SLOT_LIVE => live += 1,
                SLOT_DEAD => dead += 1,
                _ => free += 1,
            }
        }
        (free, live, dead)
    }

    /// Logical body length in bytes (includes header).
    pub fn body_logical_len(&self) -> Result<usize, MempoolError> {
        body_logical_len(&self.body)
    }

    /// Payload bytes in the logical body (header excluded).
    pub fn body_payload_len(&self) -> Result<usize, MempoolError> {
        Ok(self.body_logical_len()? - BODY_HEADER)
    }

    /// Dead payload bytes still held by the logical body.
    pub fn orphaned_body_bytes(&self) -> Result<u64, MempoolError> {
        Ok((self.body_payload_len()? as u64).saturating_sub(self.live_body_bytes))
    }

    /// True when dead payloads are at least the live payload bytes.
    ///
    /// A full pool reuses the slot its eviction freed but appends the new
    /// payload, so DEAD slot counts stay near zero while the body grows. This
    /// is the trigger that bounds those bytes between block compactions.
    pub fn body_compaction_due(&self) -> Result<bool, MempoolError> {
        let dead = self.orphaned_body_bytes()?;
        Ok(dead > 0 && dead >= self.live_body_bytes)
    }

    /// Rewrite body/slots to contain only LIVE payloads packed from the header.
    ///
    /// Copies existing packed payload ranges (does not re-serialize). Returns
    /// `(live_after, body_bytes_after)`. Callers must rebuild RAM indexes with
    /// the new slot numbers from `load_live_txs`.
    pub fn compact(&mut self) -> Result<(u32, usize), MempoolError> {
        let logical = body_logical_len(&self.body)?;
        let mut new_body = vec![0u8; BODY_HEADER];
        new_body[0..4].copy_from_slice(&MEM_MAGIC);
        new_body[4..6].copy_from_slice(&MEM_SCHEMA.to_le_bytes());
        let mut new_slots = vec![0u8; SLOTS_HEADER + (self.slot_cap as usize) * SLOT_REC];
        new_slots[0..4].copy_from_slice(&MEM_MAGIC);
        new_slots[4..6].copy_from_slice(&MEM_SCHEMA.to_le_bytes());
        new_slots[8..12].copy_from_slice(&self.slot_cap.to_le_bytes());

        let mut next_slot = 0u32;
        for slot in 0..self.slot_cap {
            let off = SLOTS_HEADER + (slot as usize) * SLOT_REC;
            if self.slots[off] != SLOT_LIVE {
                continue;
            }
            let rec = slot_rec(&self.slots, off);
            let body_off = le_u64!(rec, 4);
            let body_len = le_u32!(rec, 12) as usize;
            if body_off as usize + body_len > logical || body_len < BODY_TX_PREFIX {
                return Err(MempoolError::Corrupt("live slot body range"));
            }
            let start = body_off as usize;
            let new_off = new_body.len() as u64;
            new_body.extend_from_slice(&self.body[start..start + body_len]);
            let dst = SLOTS_HEADER + (next_slot as usize) * SLOT_REC;
            new_slots[dst] = SLOT_LIVE;
            new_slots[dst + 1..dst + 4].copy_from_slice(&self.slots[off + 1..off + 4]);
            new_slots[dst + 4..dst + 12].copy_from_slice(&new_off.to_le_bytes());
            new_slots[dst + 12..dst + 16].copy_from_slice(&(body_len as u32).to_le_bytes());
            new_slots[dst + 16..dst + 48].copy_from_slice(&self.slots[off + 16..off + 48]);
            next_slot += 1;
        }
        let packed_len = new_body.len();
        new_body[8..16].copy_from_slice(&(packed_len as u64).to_le_bytes());

        self.install_images(
            new_body,
            new_slots,
            next_slot,
            packed_len.saturating_sub(BODY_HEADER) as u64,
        )?;
        self.clear_dirty();
        Ok((self.live_count, packed_len))
    }

    /// Recode leftover schema-1 (`fee‖weight‖raw_tx`), schema-2 or schema-3
    /// packed LIVE slots into schema 4.
    ///
    /// Same install as compact (tmp+rename). Older records carry no accept
    /// time, so each gets the migrate's wall-clock second: expiry counts from
    /// this open. Schema ≤ 2 sigop cost is written unknown;
    /// [`crate::ActiveMempool::recompute_missing_sigops`] fills it after open.
    /// Schema-1 vin aux is empty (SH reindex batch-fills). Other schemas refuse
    /// at header read ([`accepted_schema`]).
    #[doc(hidden)]
    pub fn migrate_to_current(&mut self, from: u16) -> Result<(), MempoolError> {
        let accept_secs = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        let logical = body_logical_len(&self.body)?;
        let mut new_body = vec![0u8; BODY_HEADER];
        new_body[0..4].copy_from_slice(&MEM_MAGIC);
        new_body[4..6].copy_from_slice(&MEM_SCHEMA.to_le_bytes());
        let mut new_slots = vec![0u8; SLOTS_HEADER + (self.slot_cap as usize) * SLOT_REC];
        new_slots[0..4].copy_from_slice(&MEM_MAGIC);
        new_slots[4..6].copy_from_slice(&MEM_SCHEMA.to_le_bytes());
        new_slots[8..12].copy_from_slice(&self.slot_cap.to_le_bytes());

        let mut next_slot = 0u32;
        for slot in 0..self.slot_cap {
            let off = SLOTS_HEADER + (slot as usize) * SLOT_REC;
            if self.slots[off] != SLOT_LIVE {
                continue;
            }
            let rec = slot_rec(&self.slots, off);
            let body_off = le_u64!(rec, 4);
            let body_len = le_u32!(rec, 12) as usize;
            if body_off as usize + body_len > logical || body_len < V1_BODY_PREFIX {
                return Err(MempoolError::Corrupt("legacy live slot body range"));
            }
            let rec = &self.body[body_off as usize..body_off as usize + body_len];
            let old = if from == 1 {
                let tx: Transaction = deserialize(&rec[V1_BODY_PREFIX..])
                    .map_err(|_| MempoolError::Corrupt("v1 tx deserialize"))?;
                let mut head = [0u8; V1_BODY_PREFIX];
                head.copy_from_slice(&rec[..V1_BODY_PREFIX]);
                PackedLive {
                    fee_sat: le_u64!(head, 0),
                    weight: le_u64!(head, 8),
                    sigop_cost: None,
                    accept_secs: 0,
                    txid: tx.compute_txid(),
                    wtxid: tx.compute_wtxid(),
                    tx,
                    vins: Vec::new(),
                }
            } else if from == 2 {
                decode_packed_live_v2(rec)?
            } else {
                decode_packed_live_v3(rec)?
            };
            let payload = encode_packed_live(
                &old.tx,
                &old.txid,
                &old.wtxid,
                old.fee_sat,
                old.weight,
                old.sigop_cost,
                accept_secs,
                &old.vins,
            )?;
            let new_off = new_body.len() as u64;
            let plen = payload.len() as u32;
            new_body.extend_from_slice(&payload);
            let dst = SLOTS_HEADER + (next_slot as usize) * SLOT_REC;
            new_slots[dst] = SLOT_LIVE;
            new_slots[dst + 4..dst + 12].copy_from_slice(&new_off.to_le_bytes());
            new_slots[dst + 12..dst + 16].copy_from_slice(&plen.to_le_bytes());
            new_slots[dst + 16..dst + 48].copy_from_slice(old.txid.as_byte_array());
            next_slot += 1;
        }
        let packed_len = new_body.len();
        new_body[8..16].copy_from_slice(&(packed_len as u64).to_le_bytes());
        self.install_images(
            new_body,
            new_slots,
            next_slot,
            packed_len.saturating_sub(BODY_HEADER) as u64,
        )?;
        self.clear_dirty();
        rbitcoin_log::info!(
            "mempool: converted schema {from} sidecar to schema {MEM_SCHEMA} (live={next_slot})"
        );
        Ok(())
    }

    /// Fill a LIVE record's sigop cost (post-migrate recompute).
    ///
    /// RAM image plus an 8-byte `pwrite` (no fsync) so the next open does not
    /// recompute it. A 512-byte sector lands whole, so a field inside one
    /// sector reads old (unknown) or new after a crash. A field that crosses
    /// a sector could tear into a cost under the block limit, so its lower
    /// sector is written and synced first: new low bytes under the old 0xFF
    /// high bytes decode as unknown. A record still in the unpersisted tail is
    /// rewritten from RAM by the next tail persist.
    pub fn set_sigop_cost(&mut self, slot: u32, cost: u64) -> Result<(), MempoolError> {
        if slot >= self.slot_cap {
            return Err(MempoolError::Corrupt("slot OOB"));
        }
        let off = SLOTS_HEADER + (slot as usize) * SLOT_REC;
        if self.slots[off] != SLOT_LIVE {
            return Err(MempoolError::Corrupt("sigops on non-live slot"));
        }
        let rec = slot_rec(&self.slots, off);
        let body_off = le_u64!(rec, 4);
        let at = body_off as usize + SIGOP_COST_OFF;
        let bytes = cost.to_le_bytes();
        self.body[at..at + 8].copy_from_slice(&bytes);
        let path = self.dir.join("tx.body");
        // Little-endian: the low-order bytes sit in the lower sector. Synced
        // under the old 0xFF high bytes, they decode above the block limit.
        let split = (SECTOR - at % SECTOR).min(8);
        let file = &mut self.body_file;
        let write = |file: &mut File, from: usize| -> std::io::Result<()> {
            let end = if from == 0 { split } else { 8 };
            file.seek(SeekFrom::Start((at + from) as u64))?;
            file.write_all(&bytes[from..end])
        };
        write(file, 0).map_err(|e| MempoolError::io(&path, e))?;
        if split < 8 {
            #[cfg(debug_assertions)]
            if sigop_sync_fails(&self.dir) {
                return Err(MempoolError::io(
                    &path,
                    std::io::Error::other("injected sigop sync failure"),
                ));
            }
            file.durable_sync_data()
                .map_err(|e| MempoolError::io(&path, e))?;
            write(file, split).map_err(|e| MempoolError::io(&path, e))?;
        }
        Ok(())
    }

    /// `accept_secs` of a LIVE slot's record, read from RAM.
    pub fn live_accept_secs(&self, slot: u32) -> Option<u64> {
        if slot >= self.slot_cap {
            return None;
        }
        let off = SLOTS_HEADER + (slot as usize) * SLOT_REC;
        if self.slots[off] != SLOT_LIVE {
            return None;
        }
        let rec = slot_rec(&self.slots, off);
        let body_off = le_u64!(rec, 4);
        let at = body_off as usize + ACCEPT_SECS_OFF;
        let bytes = self.body.get(at..at + 8)?;
        let mut word = [0u8; 8];
        word.copy_from_slice(bytes);
        Some(le_u64!(word, 0))
    }

    /// Load LIVE txs whose packed bytes sit inside the logical body.
    ///
    /// A slot past that length is the issue 859 tear (slots reached disk, the
    /// body tail did not). Those rows are skipped. An in-range record that does
    /// not decode, or whose txid does not match the slot, fails the whole open.
    pub fn load_live_txs(&self) -> Result<Vec<LiveTx>, MempoolError> {
        let mut out = Vec::new();
        let logical = body_logical_len(&self.body)? as u64;
        for slot in 0..self.slot_cap {
            let off = SLOTS_HEADER + (slot as usize) * SLOT_REC;
            if self.slots[off] != SLOT_LIVE {
                continue;
            }
            let rec = slot_rec(&self.slots, off);
            let body_off = le_u64!(rec, 4);
            let body_len = le_u32!(rec, 12) as u64;
            if live_slot_past_body(body_off, body_len, logical) {
                continue;
            }
            if (body_len as usize) < BODY_TX_PREFIX {
                return Err(MempoolError::Corrupt("live slot body range"));
            }
            let start = body_off as usize;
            let packed = decode_packed_live(&self.body[start..start + body_len as usize])?;
            let slot_txid = Txid::from_byte_array(bytes!(rec, 16, 32));
            if packed.txid != slot_txid {
                return Err(MempoolError::Corrupt("live slot txid"));
            }
            out.push(LiveTx {
                slot,
                packed,
                trust_kept: self.slots[off + 1] != 0,
            });
        }
        Ok(out)
    }

    /// Mark LIVE rows past the durable body FREE and publish that slot table.
    ///
    /// [`Self::load_live_txs`] already skips them. Clearing the bytes keeps a
    /// later compact from refusing the same tear.
    #[doc(hidden)]
    pub fn drop_slots_past_body(&mut self) -> Result<(), MempoolError> {
        let logical = self.body_persisted_len;
        let mut changed = false;
        for slot in 0..self.slot_cap {
            let off = SLOTS_HEADER + (slot as usize) * SLOT_REC;
            if self.slots[off] != SLOT_LIVE {
                continue;
            }
            let rec = slot_rec(&self.slots, off);
            let body_off = le_u64!(rec, 4);
            let body_len = le_u32!(rec, 12) as u64;
            if !live_slot_past_body(body_off, body_len, logical) {
                continue;
            }
            self.slots[off] = SLOT_FREE;
            self.live_count = self.live_count.saturating_sub(1);
            self.live_body_bytes = self.live_body_bytes.saturating_sub(body_len);
            changed = true;
        }
        if changed {
            self.persist_slots_and_meta()?;
        }
        Ok(())
    }

    /// True if at least one FREE or DEAD slot can be reused.
    pub fn has_free_slot(&self) -> bool {
        self.find_free_slot().is_some()
    }

    #[doc(hidden)]
    pub fn find_free_slot(&self) -> Option<u32> {
        for slot in 0..self.slot_cap {
            let off = SLOTS_HEADER + (slot as usize) * SLOT_REC;
            if self.slots[off] == SLOT_FREE || self.slots[off] == SLOT_DEAD {
                return Some(slot);
            }
        }
        None
    }

    #[doc(hidden)]
    pub fn alloc_slot(&mut self) -> Result<u32, MempoolError> {
        if let Some(s) = self.find_free_slot() {
            return Ok(s);
        }
        // Grow once so a full live set under a small legacy cap is not a hard fail.
        self.grow_slots()?;
        self.find_free_slot().ok_or(MempoolError::Full)
    }

    /// Pretend the slot table is at its cap and already full of LIVE rows.
    /// The next admit takes the slot-eviction path instead of growing.
    #[cfg(debug_assertions)]
    #[doc(hidden)]
    pub fn testing_pin_full_slot_table(&mut self) {
        self.slot_cap = self.live_count.max(1);
        self.grow_pinned = true;
    }

    /// Tests set the logical slot cap. The slots image must already cover it.
    #[cfg(debug_assertions)]
    pub fn testing_set_slot_cap(&mut self, cap: u32) {
        let need = SLOTS_HEADER + (cap as usize) * SLOT_REC;
        assert!(
            cap >= 1 && self.slots.len() >= need,
            "slot image is shorter than cap {cap}"
        );
        self.slots[8..12].copy_from_slice(&cap.to_le_bytes());
        self.slot_cap = cap;
    }

    /// Double slot capacity (up to [`MAX_SLOT_CAP`]) and extend the slots image with FREE records.
    pub fn grow_slots(&mut self) -> Result<(), MempoolError> {
        if self.grow_pinned || self.slot_cap >= MAX_SLOT_CAP {
            return Err(MempoolError::Full);
        }
        let new_cap = self
            .slot_cap
            .saturating_mul(2)
            .max(self.slot_cap.saturating_add(DEFAULT_SLOT_CAP.min(16_384)))
            .min(MAX_SLOT_CAP);
        if new_cap <= self.slot_cap {
            return Err(MempoolError::Full);
        }
        let old_cap = self.slot_cap;
        let need = SLOTS_HEADER + (new_cap as usize) * SLOT_REC;
        self.slots.resize(need, 0);
        self.slots[8..12].copy_from_slice(&new_cap.to_le_bytes());
        self.slot_cap = new_cap;
        self.persist_body_then_slots()?;
        rbitcoin_log::info!(
            "mempool: grew slot table {old_cap} → {new_cap} (live={})",
            self.live_count
        );
        Ok(())
    }

    #[doc(hidden)]
    pub fn write_slot(
        &mut self,
        slot: u32,
        status: u8,
        body_off: u64,
        body_len: u32,
        txid: &Txid,
    ) -> Result<(), MempoolError> {
        let off = SLOTS_HEADER + (slot as usize) * SLOT_REC;
        self.slots[off] = status;
        self.slots[off + 1..off + 4].fill(0);
        self.slots[off + 4..off + 12].copy_from_slice(&body_off.to_le_bytes());
        self.slots[off + 12..off + 16].copy_from_slice(&body_len.to_le_bytes());
        self.slots[off + 16..off + 48].copy_from_slice(txid.as_byte_array());
        Ok(())
    }

    /// Mark a LIVE slot as warn-and-applied from the trusted session.
    ///
    /// The byte sits in the schema-4 pad. Compact copies it. A dead slot is
    /// not reloaded, so clearing it is unnecessary.
    pub fn set_slot_trust_kept(&mut self, slot: u32, on: bool) {
        let off = SLOTS_HEADER + (slot as usize) * SLOT_REC + 1;
        self.slots[off] = u8::from(on);
    }

    /// Ensure body has `need` free bytes; return offset of free region. Updates logical len.
    #[doc(hidden)]
    pub fn reserve_body(&mut self, need: usize) -> Result<u64, MempoolError> {
        let logical = body_logical_len(&self.body)?;
        let end = logical.saturating_add(need);
        // Growing by nothing is the same as not growing. `checked_sub`
        // drops the `>` that `>=` would not change.
        if end.checked_sub(self.body.len()).is_some_and(|d| d != 0) {
            self.body.resize(end, 0);
        }
        self.body[8..16].copy_from_slice(&(end as u64).to_le_bytes());
        Ok(logical as u64)
    }

    /// Slots + live_count. LIVE rows whose body is not yet durable are written
    /// as FREE so a slots-ahead-of-body crash cannot load offsets past EOF.
    #[doc(hidden)]
    pub fn persist_slots_and_meta(&mut self) -> Result<(), MempoolError> {
        // Apply a leftover mark first, so its pages are on disk before this
        // image replaces them. Its admits are written FREE here until a
        // persist publishes their slots; the callers run at open, with no
        // mark armed.
        let body_path = self.dir.join("tx.body");
        self.settle_flush_mark(&body_path)?;
        let demoted = self.demoted_slots_image();
        let mut stashed = None;
        if demoted.is_none() {
            stashed = Some(std::mem::take(&mut self.slots));
        }
        let ram: &[u8] = match demoted.as_deref() {
            Some(bytes) => bytes,
            None => stashed.as_deref().unwrap_or(&[]),
        };
        let committed = self.commit_guarded(false, &[], Some(ram), true, &[], true);
        if let Some(slots) = stashed {
            self.slots = slots;
        }
        committed?;
        self.promote_pending_slots();
        Ok(())
    }

    /// Link a pending `slots.tmp` over `slots` after its bytes are durable.
    ///
    /// A failed rename leaves the file in place; the next persist or open
    /// retries. The open fd keeps the same inode, so no handle change is
    /// needed.
    fn promote_pending_slots(&mut self) {
        if !self.slots_pending {
            return;
        }
        let slots_path = self.dir.join("slots");
        let slots_tmp = self.dir.join("slots.tmp");
        if fs::rename(&slots_tmp, &slots_path).is_ok() {
            self.slots_pending = false;
        }
    }

    #[doc(hidden)]
    pub fn demoted_slots_image(&self) -> Option<Vec<u8>> {
        let mut disk = None;
        for slot in 0..self.slot_cap {
            let off = SLOTS_HEADER + (slot as usize) * SLOT_REC;
            if self.slots[off] != SLOT_LIVE {
                continue;
            }
            let rec = slot_rec(&self.slots, off);
            let body_off = le_u64!(rec, 4);
            let body_len = le_u32!(rec, 12) as u64;
            if body_off.saturating_add(body_len) > self.body_persisted_len {
                let img = disk.get_or_insert_with(|| self.slots.clone());
                img[off] = SLOT_FREE;
            }
        }
        disk
    }

    /// Test pin: write slots without a prior body tail. Must not introduce
    /// LIVE offsets ≥ [`Self::body_persisted_len`].
    pub fn persist_slots_without_body(&mut self) -> Result<(), MempoolError> {
        self.persist_slots_and_meta()
    }

    #[doc(hidden)]
    pub fn persist_meta(&mut self) -> Result<(), MempoolError> {
        self.commit_guarded(false, &[], None, true, &[], false)
    }

    /// Publish durable DEAD bytes and `live_count` in one mark.
    ///
    /// The status byte written is [`SLOT_DEAD`], not whatever RAM holds now.
    /// An admit can reuse the slot before this flush; copying RAM would mark
    /// the old on-disk record LIVE again. The replacement waits for the body
    /// persist. `deaths_unsynced` stays set until the mark is cleared, so a
    /// failed call retries. Does not rewrite the slot table and does not
    /// rename `tx.body`.
    pub fn sync_deaths(&mut self) -> Result<(), MempoolError> {
        if !self.deaths_unsynced {
            return Ok(());
        }
        let ranges = self.death_ranges();
        let dead_at: Vec<usize> = ranges.iter().map(|(off, _)| *off).collect();
        self.commit_guarded(false, &ranges, None, true, &dead_at, true)
    }

    /// One forward mark for the pages this flush dirties.
    ///
    /// The body tail and its length are synced before the mark. Slot ranges
    /// and meta are the image: a crash copies them onto the live files. Slot images start from the disk page and overlay only `ranges`,
    /// so a neighbor byte stays. `full_slots` is the demoted slot image and
    /// replaces whole pages that differ.
    ///
    /// `retire_deaths` drops the queued deaths once this commit is durable:
    /// a full slot image or a timed persist's death ranges published each
    /// row's current status, and a death sync published those deaths. A
    /// failure after that point, such as the `tx.body` trim, does not bring
    /// them back, or a later death sync could stamp a slot an admit reused.
    fn commit_guarded(
        &mut self,
        body: bool,
        slot_ranges: &[(usize, usize)],
        full_slots: Option<&[u8]>,
        meta: bool,
        stamp_dead: &[usize],
        retire_deaths: bool,
    ) -> Result<(), MempoolError> {
        let body_path = self.dir.join("tx.body");
        let slots_path = self.slots_path();
        let meta_path = self.dir.join("meta");
        let slots_name = slots_path
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("slots")
            .to_string();
        self.settle_flush_mark(&body_path)?;
        let logical = if body {
            Some(self.append_body(&body_path)?)
        } else {
            None
        };
        let mut images: Vec<(String, u64, [u8; 4096])> = Vec::new();
        self.collect_slot_images(
            slot_ranges,
            full_slots,
            &slots_path,
            &slots_name,
            &mut images,
        )?;
        stamp_slot_dead(&slots_name, stamp_dead, &mut images);
        if meta {
            self.collect_meta_image(&meta_path, &mut images)?;
        }
        self.publish_flush_mark(images, retire_deaths)?;
        // The slots that name the appended records are durable now.
        match logical {
            Some(logical) if logical != self.body_persisted_len => {
                self.adopt_body_len(&body_path, logical)
            }
            _ => Ok(()),
        }
    }

    /// Apply a `guard/flush` an earlier flush left armed, before this flush
    /// reads any live page. A failed flush can have written some of its
    /// pages, and this flush's clear drops every record of a file it syncs.
    /// Once applied, that mark's admits are on disk, so its body length is
    /// the durable one.
    fn settle_flush_mark(&mut self, body_path: &Path) -> Result<(), MempoolError> {
        if !self.flush_mark_armed {
            return Ok(());
        }
        {
            let _hold = rbitcoin_store::page_guard::hold_marks();
            restore_mempool_mark(&self.dir)?;
        }
        // The disk body length is durable, so the next append starts there.
        // It is not the slot watermark: the mark just restored may not hold
        // the slots that name those records, and the next persist must write
        // them. The watermark moves only when a mark publishes those slots.
        let page0 = read_page(&mut self.body_file, body_path, 0)?;
        let disk = le_u64!(page0, 8);
        if disk <= body_logical_len(&self.body)? as u64 {
            self.body_synced_len = self.body_synced_len.max(disk);
        }
        self.flush_mark_armed = false;
        Ok(())
    }

    /// Append the new body records past the durable end, sync, then write
    /// and sync the length in page 0. Under the sector rule this needs no
    /// image: the records share no committed byte, and the 8-byte length
    /// sits in one sector. A crash before the length keeps the old one and
    /// ignores the bytes past it. The LIVE slots that name these records
    /// arm after this, so they never name a record that is not durable.
    ///
    /// Returns the body length. The caller adopts it as the slot watermark,
    /// and trims the file to it, once the slots commit.
    fn append_body(&mut self, body_path: &Path) -> Result<u64, MempoolError> {
        let logical = body_logical_len(&self.body)? as u64;
        let old = self.body_synced_len.min(logical);
        if logical <= old {
            return Ok(logical);
        }
        pwrite_from(
            &mut self.body_file,
            body_path,
            old,
            old,
            &self.body[old as usize..logical as usize],
        )?;
        self.body_file
            .durable_sync_data()
            .map_err(|e| MempoolError::io(body_path, e))?;
        pwrite_from(&mut self.body_file, body_path, old, 8, &self.body[8..16])?;
        self.body_file
            .durable_sync_data()
            .map_err(|e| MempoolError::io(body_path, e))?;
        self.body_synced_len = logical;
        Ok(logical)
    }

    fn collect_slot_images(
        &mut self,
        slot_ranges: &[(usize, usize)],
        full_slots: Option<&[u8]>,
        slots_path: &Path,
        slots_name: &str,
        images: &mut Vec<(String, u64, [u8; 4096])>,
    ) -> Result<(), MempoolError> {
        let page = rbitcoin_store::page_guard::PAGE;
        if let Some(ram) = full_slots {
            let need = ram.len() as u64;
            let have = self
                .slots_file
                .metadata()
                .map_err(|e| MempoolError::io(slots_path, e))?
                .len();
            if need > self.slots_len_synced {
                if need > have {
                    self.slots_file
                        .set_len(need)
                        .map_err(|e| MempoolError::io(slots_path, e))?;
                }
                // The growth mark publishes `slot_cap` in page 0 and does not
                // image the zero tail. The longer file has to be durable first.
                #[cfg(debug_assertions)]
                if grow_sync_fails(&self.dir) {
                    return Err(MempoolError::io(
                        slots_path,
                        std::io::Error::other("injected slot growth sync failure"),
                    ));
                }
                self.slots_file
                    .durable_sync_all()
                    .map_err(|e| MempoolError::io(slots_path, e))?;
                self.slots_len_synced = need;
            }
            let mut page_off = 0u64;
            while (page_off as usize) < ram.len() {
                let mut image = read_page(&mut self.slots_file, slots_path, page_off)?;
                let before = image;
                let lo = page_off as usize;
                let hi = (lo + page as usize).min(ram.len());
                image[..hi - lo].copy_from_slice(&ram[lo..hi]);
                if image != before {
                    images.push((slots_name.to_string(), page_off, image));
                }
                page_off = page_off.saturating_add(page);
            }
        } else if !slot_ranges.is_empty() {
            let mut pages: std::collections::BTreeMap<u64, [u8; 4096]> =
                std::collections::BTreeMap::new();
            for &(start, end) in slot_ranges {
                if start >= end || end > self.slots.len() {
                    continue;
                }
                let mut at = start;
                while at < end {
                    let page_off = (at as u64) & !(page - 1);
                    let image = match pages.entry(page_off) {
                        std::collections::btree_map::Entry::Occupied(entry) => entry.into_mut(),
                        std::collections::btree_map::Entry::Vacant(entry) => {
                            let image = read_page(&mut self.slots_file, slots_path, page_off)?;
                            entry.insert(image)
                        }
                    };
                    let hi = end.min((page_off as usize).saturating_add(page as usize));
                    let dst = at - page_off as usize;
                    image[dst..dst + (hi - at)].copy_from_slice(&self.slots[at..hi]);
                    at = hi;
                }
            }
            for (off, image) in pages {
                images.push((slots_name.to_string(), off, image));
            }
        }
        Ok(())
    }

    fn collect_meta_image(
        &mut self,
        meta_path: &Path,
        images: &mut Vec<(String, u64, [u8; 4096])>,
    ) -> Result<(), MempoolError> {
        let mut image = read_page(&mut self.meta_file, meta_path, 0)?;
        let mut rec = [0u8; META_LEN];
        write_meta_bytes(&mut rec, self.generation, self.slot_cap, self.live_count);
        image[..META_LEN].copy_from_slice(&rec);
        images.push(("meta".to_string(), 0, image));
        Ok(())
    }

    fn publish_flush_mark(
        &mut self,
        images: Vec<(String, u64, [u8; 4096])>,
        retire_deaths: bool,
    ) -> Result<(), MempoolError> {
        let slots_path = self.slots_path();
        let slots_path = slots_path.as_path();
        let meta_path = self.dir.join("meta");
        let meta_path = meta_path.as_path();
        if images.is_empty() {
            if retire_deaths {
                self.retire_deaths();
            }
            return Ok(());
        }
        let _hold = rbitcoin_store::page_guard::hold_marks();
        let refs: Vec<(&str, u64, &[u8])> = images
            .iter()
            .map(|(name, off, image)| (name.as_str(), *off, image.as_slice()))
            .collect();
        let mark = self.dir.join("guard/flush");
        let armed =
            rbitcoin_store::page_guard::arm_page_guard_group(&mark, &refs).map_err(map_store);
        #[cfg(debug_assertions)]
        let armed = armed.and_then(|()| match take_fail_mempool_arm(&self.dir) {
            true => Err(MempoolError::io(
                &mark,
                std::io::Error::other("injected mempool arm failure after rename"),
            )),
            false => Ok(()),
        });
        if let Err(e) = armed {
            // The rename can land before the directory sync fails. A mark
            // that may be visible stays armed until a commit clears it.
            if mark.try_exists().unwrap_or(true) {
                self.flush_mark_armed = true;
            }
            return Err(e);
        }
        self.flush_mark_armed = true;
        #[cfg(debug_assertions)]
        if take_fail_mempool_guard(&self.dir) {
            return Err(MempoolError::io(
                &mark,
                std::io::Error::other("injected mempool guard failure"),
            ));
        }
        for (name, off, image) in &images {
            match name.as_str() {
                "meta" => pwrite_at(&mut self.meta_file, meta_path, *off, image)?,
                _ => pwrite_at(&mut self.slots_file, slots_path, *off, image)?,
            }
        }
        let mut live: Vec<(&File, &Path)> = Vec::new();
        let mut saw_slots = false;
        let mut saw_meta = false;
        for (name, _, _) in &images {
            match name.as_str() {
                "meta" => saw_meta = true,
                _ => saw_slots = true,
            }
        }
        if saw_slots {
            live.push((&self.slots_file, slots_path));
        }
        if saw_meta {
            live.push((&self.meta_file, meta_path));
        }
        rbitcoin_store::page_guard::clear_page_guard_files(&mark, &live).map_err(map_store)?;
        self.flush_mark_armed = mark.exists();
        if retire_deaths {
            self.retire_deaths();
        }
        Ok(())
    }

    fn retire_deaths(&mut self) {
        self.death_slots.clear();
        self.deaths_unsynced = false;
    }

    fn adopt_body_len(&mut self, path: &Path, logical: u64) -> Result<(), MempoolError> {
        // The LIVE bytes are already synced and the mark is gone. A later
        // `set_len` failure must not keep the old durable length, or a death
        // of this tail is skipped.
        self.body_persisted_len = logical;
        #[cfg(debug_assertions)]
        if take_fail_body_adopt(&self.dir) {
            return Err(MempoolError::io(
                path,
                std::io::Error::other("injected tx.body trim failure"),
            ));
        }
        self.body_file
            .set_len(logical)
            .map_err(|e| MempoolError::io(path, e))?;
        Ok(())
    }

    /// Write packed images, then adopt them in RAM.
    ///
    /// Both tmps are `sync_all`'d and renamed body-then-slots. A failed write,
    /// or a body rename that fails, leaves `self.body`, `self.slots`, and the
    /// live counts as they were. If the body rename lands and the slots rename
    /// fails, the body is already packed, so both RAM images are adopted and
    /// slot writes stay on `slots.tmp` until a persist or the next open links
    /// it (see [`finish_pending_compact`]).
    ///
    /// After rename, persist meta only. Rewriting packed body/slots in place
    /// (`set_len` / `write_all`) reopens the crash window tmp+rename closed.
    /// Crash after both renames, before meta: packed images + stale `live_count`
    /// still load (`load_live_txs` scans slots).
    fn install_images(
        &mut self,
        body: Vec<u8>,
        slots: Vec<u8>,
        live_count: u32,
        live_body_bytes: u64,
    ) -> Result<(), MempoolError> {
        // An interrupted flush mark still names the pre-compact pages. Apply
        // it and drop it before the packed names replace those files.
        restore_mempool_mark(&self.dir)?;
        let body_path = self.dir.join("tx.body");
        let slots_path = self.dir.join("slots");
        let body_tmp = self.dir.join("tx.body.tmp");
        let slots_tmp = self.dir.join("slots.tmp");
        let logical = body_logical_len(&body)?;
        if self.slots_pending {
            // The live table is still here from an earlier install. Link it
            // before reusing the temp name, or the write would truncate it.
            fs::rename(&slots_tmp, &slots_path).map_err(|e| MempoolError::io(&slots_path, e))?;
            self.slots_pending = false;
        }
        let body_file = write_file_synced(&body_tmp, &body[..logical])?;
        let slots_file = write_file_synced(&slots_tmp, &slots)?;
        fs::rename(&body_tmp, &body_path).map_err(|e| MempoolError::io(&body_path, e))?;
        self.body = body;
        self.body_file = body_file;
        self.body_persisted_len = logical as u64;
        self.body_synced_len = logical as u64;
        self.last_persist_ms = self.now_ms();
        self.body_dirty = false;
        if let Err(e) = fs::rename(&slots_tmp, &slots_path) {
            self.adopt_slot_table(slots, slots_file, live_count, live_body_bytes);
            self.slots_pending = true;
            // The body is committed. A failed meta leaves a stale live_count,
            // which open reconciles from the slot table.
            let _ = self.persist_meta();
            return Err(MempoolError::io(&slots_path, e));
        }
        self.adopt_slot_table(slots, slots_file, live_count, live_body_bytes);
        self.persist_meta()
    }

    /// The packed table is the table. Dead rows are gone and the rest moved,
    /// so indexes recorded against the previous table name different records.
    fn adopt_slot_table(
        &mut self,
        slots: Vec<u8>,
        slots_file: File,
        live_count: u32,
        live_body_bytes: u64,
    ) {
        // `write_file_synced` synced the whole packed table.
        self.slots_len_synced = slots.len() as u64;
        self.slots = slots;
        self.slots_file = slots_file;
        self.live_count = live_count;
        self.live_body_bytes = live_body_bytes;
        self.death_slots.clear();
        self.deaths_unsynced = false;
    }
}

/// Force status bytes to [`SLOT_DEAD`] on slot images this flush already built.
///
/// A one-byte death flush must not copy RAM: the slot may have been reused.
fn stamp_slot_dead(slots_name: &str, offsets: &[usize], images: &mut [(String, u64, [u8; 4096])]) {
    if offsets.is_empty() {
        return;
    }
    let page = rbitcoin_store::page_guard::PAGE;
    let by_page: std::collections::HashMap<u64, usize> = images
        .iter()
        .enumerate()
        .filter(|(_, (name, _, _))| name == slots_name)
        .map(|(i, (_, at, _))| (*at, i))
        .collect();
    for &off in offsets {
        let page_off = (off as u64) & !(page - 1);
        let dst = (off as u64 - page_off) as usize;
        if let Some(&i) = by_page.get(&page_off) {
            if let Some(byte) = images[i].2.get_mut(dst) {
                *byte = SLOT_DEAD;
            }
        }
    }
}

#[cfg(debug_assertions)]
static FAIL_MEMPOOL_GUARD: std::sync::Mutex<Vec<PathBuf>> = std::sync::Mutex::new(Vec::new());

/// The next mempool flush in `dir` arms `guard/flush`, then returns before
/// the shared pages are written. The mark stays.
#[cfg(debug_assertions)]
#[doc(hidden)]
pub fn fail_next_mempool_guard_before_write(dir: &Path) {
    FAIL_MEMPOOL_GUARD
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .push(dir.to_path_buf());
}

#[cfg(debug_assertions)]
fn take_fail_mempool_guard(dir: &Path) -> bool {
    let mut armed = FAIL_MEMPOOL_GUARD.lock().unwrap_or_else(|e| e.into_inner());
    let Some(i) = armed.iter().position(|p| p == dir) else {
        return false;
    };
    armed.remove(i);
    true
}

#[cfg(debug_assertions)]
static FAIL_MEMPOOL_ARM: std::sync::Mutex<Vec<PathBuf>> = std::sync::Mutex::new(Vec::new());

/// The next mempool flush in `dir` publishes `guard/flush`, then its arm
/// returns an error, as a failed directory sync after the rename would.
#[cfg(debug_assertions)]
#[doc(hidden)]
pub fn fail_next_mempool_arm_after_rename(dir: &Path) {
    FAIL_MEMPOOL_ARM
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .push(dir.to_path_buf());
}

#[cfg(debug_assertions)]
fn take_fail_mempool_arm(dir: &Path) -> bool {
    let mut armed = FAIL_MEMPOOL_ARM.lock().unwrap_or_else(|e| e.into_inner());
    let Some(i) = armed.iter().position(|p| p == dir) else {
        return false;
    };
    armed.remove(i);
    true
}

#[cfg(debug_assertions)]
static FAIL_GROW_SYNC: std::sync::Mutex<Vec<PathBuf>> = std::sync::Mutex::new(Vec::new());

/// While set for `dir`, the sync that makes a longer `slots` file durable
/// fails. It stays set until cleared, so a retry fails the same way.
#[cfg(debug_assertions)]
#[doc(hidden)]
pub fn set_mempool_grow_sync_fails(dir: &Path, fails: bool) {
    let mut armed = FAIL_GROW_SYNC.lock().unwrap_or_else(|e| e.into_inner());
    armed.retain(|p| p != dir);
    if fails {
        armed.push(dir.to_path_buf());
    }
}

#[cfg(debug_assertions)]
fn grow_sync_fails(dir: &Path) -> bool {
    FAIL_GROW_SYNC
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .iter()
        .any(|p| p == dir)
}

#[cfg(debug_assertions)]
static FAIL_SIGOP_SYNC: std::sync::Mutex<Vec<PathBuf>> = std::sync::Mutex::new(Vec::new());

/// While set for `dir`, the sync between the two sector writes of a sigop
/// cost that crosses a sector fails.
#[cfg(debug_assertions)]
#[doc(hidden)]
pub fn set_mempool_sigop_sync_fails(dir: &Path, fails: bool) {
    let mut armed = FAIL_SIGOP_SYNC.lock().unwrap_or_else(|e| e.into_inner());
    armed.retain(|p| p != dir);
    if fails {
        armed.push(dir.to_path_buf());
    }
}

#[cfg(debug_assertions)]
fn sigop_sync_fails(dir: &Path) -> bool {
    FAIL_SIGOP_SYNC
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .iter()
        .any(|p| p == dir)
}

#[cfg(debug_assertions)]
static FAIL_BODY_ADOPT: std::sync::Mutex<Vec<PathBuf>> = std::sync::Mutex::new(Vec::new());

/// The next mempool flush in `dir` commits and clears its mark, then fails
/// the `tx.body` trim that follows.
#[cfg(debug_assertions)]
#[doc(hidden)]
pub fn fail_next_mempool_body_adopt(dir: &Path) {
    FAIL_BODY_ADOPT
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .push(dir.to_path_buf());
}

#[cfg(debug_assertions)]
fn take_fail_body_adopt(dir: &Path) -> bool {
    let mut armed = FAIL_BODY_ADOPT.lock().unwrap_or_else(|e| e.into_inner());
    let Some(i) = armed.iter().position(|p| p == dir) else {
        return false;
    };
    armed.remove(i);
    true
}

fn map_store(e: rbitcoin_store::StoreError) -> MempoolError {
    match e {
        rbitcoin_store::StoreError::Io { path, source } => MempoolError::Io { path, source },
        rbitcoin_store::StoreError::BadMagic => MempoolError::BadMagic,
        other => map_store_status(other),
    }
}

fn map_store_status(e: rbitcoin_store::StoreError) -> MempoolError {
    match e {
        rbitcoin_store::StoreError::BadSchema(v) => MempoolError::BadSchema(v),
        rbitcoin_store::StoreError::Corrupt(s) => MempoolError::Corrupt(s),
        other => MempoolError::io(
            PathBuf::from("mempool"),
            std::io::Error::other(other.to_string()),
        ),
    }
}

/// Copy `mempool/guard/flush` onto the live files before they are read.
fn restore_mempool_mark(dir: &Path) -> Result<(), MempoolError> {
    rbitcoin_store::page_guard::restore_page_guard_group(&dir.join("guard/flush"), dir)
        .map(|_| ())
        .map_err(map_store)
}

fn read_page(file: &mut File, path: &Path, off: u64) -> Result<[u8; 4096], MempoolError> {
    let mut image = [0u8; 4096];
    file.seek(SeekFrom::Start(off))
        .map_err(|e| MempoolError::io(path, e))?;
    let mut got = 0usize;
    while got < image.len() {
        match file.read(&mut image[got..]) {
            Ok(0) => break,
            Ok(n) => got += n,
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {}
            Err(e) => return Err(MempoolError::io(path, e)),
        }
    }
    Ok(image)
}

/// Write a page the current mark names. The whole file length counts as
/// committed.
fn pwrite_at(file: &mut File, path: &Path, off: u64, bytes: &[u8]) -> Result<(), MempoolError> {
    let committed = file
        .metadata()
        .map_err(|e| MempoolError::io(path, e))?
        .len();
    pwrite_from(file, path, committed, off, bytes)
}

/// Write `bytes` at `off`. A range that shares a block below `committed`
/// has to be a page the current mark names.
fn pwrite_from(
    file: &mut File,
    path: &Path,
    committed: u64,
    off: u64,
    bytes: &[u8],
) -> Result<(), MempoolError> {
    rbitcoin_store::page_guard::check_armed_write(path, committed, off, bytes.len() as u64)
        .map_err(map_store)?;
    file.seek(SeekFrom::Start(off))
        .map_err(|e| MempoolError::io(path, e))?;
    file.write_all(bytes)
        .map_err(|e| MempoolError::io(path, e))?;
    Ok(())
}

#[doc(hidden)]
pub fn body_logical_len(body: &[u8]) -> Result<usize, MempoolError> {
    if body.len() < BODY_HEADER {
        return Err(MempoolError::Corrupt("body too short"));
    }
    let mut hdr = [0u8; BODY_HEADER];
    hdr.copy_from_slice(&body[..BODY_HEADER]);
    let n = le_u64!(hdr, 8) as usize;
    if n < BODY_HEADER || n > body.len() {
        return Err(MempoolError::Corrupt("body logical len"));
    }
    Ok(n)
}

#[doc(hidden)]
pub fn write_file_synced(path: &Path, bytes: &[u8]) -> Result<File, MempoolError> {
    {
        // `File::create` is write-only. The flush mark reads the live page
        // back, and a write-only fd returns EBADF on that read.
        let mut f = File::create(path).map_err(|e| MempoolError::io(path, e))?;
        f.write_all(bytes).map_err(|e| MempoolError::io(path, e))?;
        f.durable_sync_all()
            .map_err(|e| MempoolError::io(path, e))?;
    }
    OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .map_err(|e| MempoolError::io(path, e))
}

/// Finish a compact install interrupted after the body rename.
///
/// `slots.tmp` only: body rename landed — finish slots before the image is
/// read. Both tmps, or a body tmp alone: leave them. A successful open deletes
/// them ([`discard_abandoned_tmps`]). An unreadable image is moved aside with
/// them still present.
#[doc(hidden)]
pub fn finish_pending_compact(dir: &Path) -> Result<(), MempoolError> {
    let body_tmp = dir.join("tx.body.tmp");
    let slots_tmp = dir.join("slots.tmp");
    if !body_tmp.exists() && slots_tmp.exists() {
        fs::rename(&slots_tmp, dir.join("slots")).map_err(|e| MempoolError::io(dir, e))?;
    }
    Ok(())
}

#[doc(hidden)]
pub fn discard_abandoned_tmps(dir: &Path) -> Result<(), MempoolError> {
    for name in ["tx.body.tmp", "slots.tmp"] {
        let path = dir.join(name);
        if path.exists() {
            fs::remove_file(&path).map_err(|e| MempoolError::io(&path, e))?;
        }
    }
    Ok(())
}

#[doc(hidden)]
pub fn live_slot_past_body(body_off: u64, body_len: u64, logical: u64) -> bool {
    body_off.saturating_add(body_len) > logical
}

/// Move an unreadable mempool sidecar aside. Fee history stays in `dir`.
pub fn quarantine_sidecar(dir: &Path) -> Result<PathBuf, MempoolError> {
    let secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let mut dest = dir.join(format!("torn-{secs}"));
    let mut n = 0u32;
    while dest.exists() {
        n = n.saturating_add(1);
        dest = dir.join(format!("torn-{secs}-{n}"));
    }
    fs::create_dir(&dest).map_err(|e| MempoolError::io(&dest, e))?;
    for name in [
        "meta",
        "slots",
        "tx.body",
        "slots.tmp",
        "tx.body.tmp",
        "guard/flush",
    ] {
        let src = dir.join(name);
        if src.exists() {
            let to = dest.join(name);
            if let Some(parent) = to.parent() {
                fs::create_dir_all(parent).map_err(|e| MempoolError::io(parent, e))?;
            }
            fs::rename(&src, &to).map_err(|e| MempoolError::io(&src, e))?;
        }
    }
    Ok(dest)
}

#[doc(hidden)]
pub fn accepted_schema(schema: u16) -> Result<u16, MempoolError> {
    if matches!(schema, 1..=3) || schema == MEM_SCHEMA {
        Ok(schema)
    } else {
        Err(MempoolError::BadSchema(schema))
    }
}

#[doc(hidden)]
pub fn open_or_init_meta(path: &Path) -> Result<(File, u64, u32, u32, u16), MempoolError> {
    if path.exists() {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .map_err(|e| MempoolError::io(path, e))?;
        let mut buf = [0u8; META_LEN];
        file.read_exact(&mut buf)
            .map_err(|e| MempoolError::io(path, e))?;
        if buf[0..4] != MEM_MAGIC {
            return Err(MempoolError::BadMagic);
        }
        let schema = accepted_schema(u16::from_le_bytes([buf[4], buf[5]]))?;
        let generation = le_u64!(buf, 8);
        let slot_cap = le_u32!(buf, 16);
        let live_count = le_u32!(buf, 20);
        if slot_cap == 0 {
            return Err(MempoolError::Corrupt("slot_cap zero"));
        }
        Ok((file, generation, slot_cap, live_count, schema))
    } else {
        // Published whole: a crash leaves no `meta` or this one, never a short file.
        let mut buf = [0u8; META_LEN];
        write_meta_bytes(&mut buf, 0, DEFAULT_SLOT_CAP, 0);
        rbitcoin_store::file::write_synced_tmp_rename(path, &buf).map_err(|e| match e {
            rbitcoin_store::StoreError::Io { path, source } => MempoolError::io(path, source),
            other => MempoolError::io(path, std::io::Error::other(other.to_string())),
        })?;
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .map_err(|e| MempoolError::io(path, e))?;
        Ok((file, 0, DEFAULT_SLOT_CAP, 0, MEM_SCHEMA))
    }
}

#[doc(hidden)]
pub fn write_meta_bytes(buf: &mut [u8; META_LEN], generation: u64, slot_cap: u32, live_count: u32) {
    buf[0..4].copy_from_slice(&MEM_MAGIC);
    buf[4..6].copy_from_slice(&MEM_SCHEMA.to_le_bytes());
    buf[6..8].copy_from_slice(&0u16.to_le_bytes());
    buf[8..16].copy_from_slice(&generation.to_le_bytes());
    buf[16..20].copy_from_slice(&slot_cap.to_le_bytes());
    buf[20..24].copy_from_slice(&live_count.to_le_bytes());
}

#[doc(hidden)]
pub fn open_or_init_slots(path: &Path, slot_cap: u32) -> Result<(File, Vec<u8>), MempoolError> {
    let need = SLOTS_HEADER + (slot_cap as usize) * SLOT_REC;
    if path.exists() {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .map_err(|e| MempoolError::io(path, e))?;
        let len = file
            .metadata()
            .map_err(|e| MempoolError::io(path, e))?
            .len() as usize;
        if len < need {
            return Err(MempoolError::Corrupt("slots file short"));
        }
        let mut buf = vec![0u8; need];
        file.read_exact(&mut buf)
            .map_err(|e| MempoolError::io(path, e))?;
        if buf[0..4] != MEM_MAGIC {
            return Err(MempoolError::BadMagic);
        }
        accepted_schema(u16::from_le_bytes([buf[4], buf[5]]))?;
        Ok((file, buf))
    } else {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(path)
            .map_err(|e| MempoolError::io(path, e))?;
        let mut buf = vec![0u8; need];
        buf[0..4].copy_from_slice(&MEM_MAGIC);
        buf[4..6].copy_from_slice(&MEM_SCHEMA.to_le_bytes());
        buf[8..12].copy_from_slice(&slot_cap.to_le_bytes());
        file.write_all(&buf)
            .map_err(|e| MempoolError::io(path, e))?;
        sync_new_file(&file, path)?;
        Ok((file, buf))
    }
}

/// A new `slots` or `tx.body` is the base later marks patch in place, and
/// its length is taken as durable. Sync its bytes and its directory entry.
fn sync_new_file(file: &File, path: &Path) -> Result<(), MempoolError> {
    file.durable_sync_all()
        .map_err(|e| MempoolError::io(path, e))?;
    let dir = path.parent().filter(|d| !d.as_os_str().is_empty());
    if let Some(dir) = dir {
        rbitcoin_store::file::fsync_parent_dir(dir).map_err(|e| MempoolError::io(dir, e))?;
    }
    Ok(())
}

#[doc(hidden)]
pub fn open_or_init_body(path: &Path) -> Result<(File, Vec<u8>), MempoolError> {
    if path.exists() {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .map_err(|e| MempoolError::io(path, e))?;
        let len = file
            .metadata()
            .map_err(|e| MempoolError::io(path, e))?
            .len() as usize;
        if len < BODY_HEADER {
            return Err(MempoolError::Corrupt("body too short"));
        }
        let mut buf = vec![0u8; len];
        file.seek(SeekFrom::Start(0))
            .map_err(|e| MempoolError::io(path, e))?;
        file.read_exact(&mut buf)
            .map_err(|e| MempoolError::io(path, e))?;
        if buf[0..4] != MEM_MAGIC {
            return Err(MempoolError::BadMagic);
        }
        accepted_schema(u16::from_le_bytes([buf[4], buf[5]]))?;
        let logical = body_logical_len(&buf)?;
        if logical > len {
            return Err(MempoolError::Corrupt("body logical past file"));
        }
        Ok((file, buf))
    } else {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(path)
            .map_err(|e| MempoolError::io(path, e))?;
        let mut buf = vec![0u8; BODY_HEADER];
        buf[0..4].copy_from_slice(&MEM_MAGIC);
        buf[4..6].copy_from_slice(&MEM_SCHEMA.to_le_bytes());
        buf[8..16].copy_from_slice(&(BODY_HEADER as u64).to_le_bytes());
        file.write_all(&buf)
            .map_err(|e| MempoolError::io(path, e))?;
        sync_new_file(&file, path)?;
        Ok((file, buf))
    }
}
