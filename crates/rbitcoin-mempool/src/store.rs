//! Private mempool durability under `{datadir}/mempool/` (not Class A).
//!
//! # Namespace (important)
//!
//! | Path | Role |
//! |------|------|
//! | `{datadir}/store/tx.body` | **Class A** confirmed archive — confirm commit sole writer |
//! | `{datadir}/mempool/tx.body` | **This file** — unconfirmed live set only |
//!
//! Schema **2** packed live records. Body is append-only (`body_persisted_len`);
//! `persist_due` writes the dirty tail then slots+meta. Compact copies packed
//! payload ranges. DEAD of a durable slot is one-record `pwrite`.
//!
//! # Transport (phase 5b M2)
//!
//! Process-owned buffers (`meta` fields + `slots` / `body` `Vec`s) are the
//! source of truth. Sidecar files are updated with normal `read`/`write` /
//! `pwrite`-style IO — **no `memmap2`**. Flush bumps generation and `sync_data`.

use crate::error::MempoolError;
use crate::packed::{decode_packed_live, encode_packed_live, PackedLive, VinAux};
use bitcoin::consensus::encode::deserialize;
use bitcoin::hashes::Hash;
use bitcoin::{Transaction, Txid, Wtxid};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::time::Instant;

/// File magic: `rBMP` (rbitcoin mempool).
pub const MEM_MAGIC: [u8; 4] = *b"rBMP";
/// Schema version for the meta header.
pub const MEM_SCHEMA: u16 = 2;

const META_LEN: usize = 64;
/// Initial slot table capacity (records).
///
/// Sized for mainnet tip mempool under a ~300 MvB weight budget: many small
/// txs fit weight-wise long before 4k slots fill (overnight tip stall). Fixed
/// constant — no env. Existing datadirs with smaller caps grow on demand.
const DEFAULT_SLOT_CAP: u32 = 131_072;
/// Hard ceiling when doubling the slot table (DoS / RAM bound).
const MAX_SLOT_CAP: u32 = 1_048_576;
/// Slot record: status(1) + pad(3) + body_off(8) + body_len(4) + txid(32) = 48.
const SLOT_REC: usize = 48;
const SLOTS_HEADER: usize = 16;
const BODY_HEADER: usize = 16;
/// Prefix before each packed live record in `mempool/tx.body` (min size).
const BODY_TX_PREFIX: usize = 80;
/// Schema 1 payload: `fee(8)‖weight(8)‖bitcoin-serialize`.
const V1_BODY_PREFIX: usize = 16;

const SLOT_FREE: u8 = 0;
const SLOT_LIVE: u8 = 1;
const SLOT_DEAD: u8 = 2;

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
}

/// Time-based sidecar persist interval (admits). Crash may lose ≤ this window.
///
/// DEAD of an already-durable slot is an immediate `pwrite` (not this timer).
/// Structural ops (slot grow, compact) and [`Mempool::flush`] persist immediately.
pub const PERSIST_INTERVAL_MS: u64 = 5_000;

/// Durable mempool under `dir` (`{datadir}/mempool`) — InRam buffers + file IO.
pub struct Mempool {
    dir: PathBuf,
    meta_file: File,
    slots_file: File,
    body_file: File,
    /// Full slots image (header + records).
    slots: Vec<u8>,
    /// Full body image including header; logical length in `body[8..16]`.
    body: Vec<u8>,
    generation: u64,
    slot_cap: u32,
    live_count: u32,
    /// True when `tx.body` grew since last persist (admits).
    body_dirty: bool,
    /// On-disk body length that slots may legally index (header + durable payloads).
    body_persisted_len: u64,
    opened: Instant,
    mock_now_ms: Option<u64>,
    last_persist_ms: u64,
    /// Last `tx.body` write start offset (tests: incremental tail).
    last_body_write_off: u64,
    /// Bytes written to `slots` on the last persist (tests: incremental pwrite).
    last_slot_write_bytes: u64,
}

impl Mempool {
    /// Create `dir` if needed and open (or initialize) meta/slots/body into RAM.
    pub fn open_or_create(dir: impl Into<PathBuf>) -> Result<Self, MempoolError> {
        let dir = dir.into();
        fs::create_dir_all(&dir).map_err(|e| MempoolError::io(&dir, e))?;
        finish_pending_compact(&dir)?;

        let meta_path = dir.join("meta");
        let slots_path = dir.join("slots");
        let body_path = dir.join("tx.body");

        let (meta_file, generation, slot_cap, live_count, meta_schema) =
            open_or_init_meta(&meta_path)?;
        let (slots_file, slots) = open_or_init_slots(&slots_path, slot_cap)?;
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
            body_dirty: false,
            body_persisted_len,
            opened: Instant::now(),
            mock_now_ms: None,
            last_persist_ms: 0,
            last_body_write_off: 0,
            last_slot_write_bytes: 0,
        };
        let body_schema = u16::from_le_bytes(mp.body[4..6].try_into().unwrap());
        if body_schema == 1 {
            mp.migrate_v1_to_packed()?;
        } else if meta_schema == 1 || u16::from_le_bytes(mp.slots[4..6].try_into().unwrap()) == 1 {
            mp.slots[4..6].copy_from_slice(&MEM_SCHEMA.to_le_bytes());
            mp.body[4..6].copy_from_slice(&MEM_SCHEMA.to_le_bytes());
            mp.persist_slots_and_meta()?;
        }
        Ok(mp)
    }

    pub fn dir(&self) -> &Path {
        &self.dir
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
    pub(crate) fn set_live_count(&mut self, n: u32) {
        self.live_count = n;
    }

    /// Persist buffers, bump generation, and fsync sidecar files.
    ///
    /// Admits persist on [`Self::persist_due`] (5 s, no fsync). A crash may lose
    /// admits since the last persist. [`Self::flush`] is the durable checkpoint
    /// (generation + fsync). Body is written before LIVE slots.
    pub fn flush(&mut self) -> Result<(), MempoolError> {
        self.generation = self.generation.saturating_add(1);
        self.persist_body_then_slots()?;
        self.meta_file.sync_data().map_err(|e| MempoolError::io(self.dir.join("meta"), e))?;
        self.slots_file.sync_data().map_err(|e| MempoolError::io(self.dir.join("slots"), e))?;
        self.body_file.sync_data().map_err(|e| MempoolError::io(self.dir.join("tx.body"), e))?;
        Ok(())
    }

    /// Time-based body persist: dirty admits wait [`PERSIST_INTERVAL_MS`].
    ///
    /// No fsync. Body tail first, then `pwrite` of new LIVE slot records, then
    /// meta. DEAD of durable slots is [`Self::mark_slot_dead`]. Flush / grow /
    /// compact still rewrite the full slot table.
    pub fn persist_due(&mut self) -> Result<(), MempoolError> {
        if !self.body_dirty {
            return Ok(());
        }
        if self.now_ms().saturating_sub(self.last_persist_ms) < PERSIST_INTERVAL_MS {
            return Ok(());
        }
        let old_persisted = self.body_persisted_len;
        self.persist_body_tail()?;
        self.pwrite_live_slots_since(old_persisted)?;
        self.persist_meta()?;
        self.clear_dirty();
        self.last_persist_ms = self.now_ms();
        Ok(())
    }

    /// Best-effort alias of [`Self::persist_due`] (no generation bump / no fsync).
    pub fn persist_if_dirty(&mut self) -> Result<(), MempoolError> {
        self.persist_due()
    }

    fn now_ms(&self) -> u64 {
        self.mock_now_ms.unwrap_or_else(|| self.opened.elapsed().as_millis() as u64)
    }

    /// Test clock. Production uses [`Instant`] elapsed from open.
    pub fn set_now_ms(&mut self, ms: u64) {
        self.mock_now_ms = Some(ms);
    }

    pub fn body_persisted_len(&self) -> u64 {
        self.body_persisted_len
    }

    pub fn last_body_write_off(&self) -> u64 {
        self.last_body_write_off
    }

    pub fn last_slot_write_bytes(&self) -> u64 {
        self.last_slot_write_bytes
    }

    fn clear_dirty(&mut self) {
        self.body_dirty = false;
    }

    fn persist_body_then_slots(&mut self) -> Result<(), MempoolError> {
        self.persist_body_tail()?;
        self.persist_slots_and_meta()?;
        self.clear_dirty();
        self.last_persist_ms = self.now_ms();
        Ok(())
    }

    /// `pwrite` LIVE rows whose payload starts at or past `old_persisted`.
    ///
    /// Adjacent new slots are one write. Does not rewrite the rest of the table.
    fn pwrite_live_slots_since(&mut self, old_persisted: u64) -> Result<(), MempoolError> {
        let path = self.dir.join("slots");
        let mut ranges: Vec<(usize, usize)> = Vec::new();
        for slot in 0..self.slot_cap {
            let off = SLOTS_HEADER + (slot as usize) * SLOT_REC;
            if self.slots[off] != SLOT_LIVE {
                continue;
            }
            let body_off = u64::from_le_bytes(self.slots[off + 4..off + 12].try_into().unwrap());
            if body_off < old_persisted {
                continue;
            }
            let rec_end = off + SLOT_REC;
            if let Some((_, end)) = ranges.last_mut() {
                if *end == off {
                    *end = rec_end;
                    continue;
                }
            }
            ranges.push((off, rec_end));
        }
        let mut n = 0usize;
        for (start, end) in ranges {
            self.slots_file
                .seek(SeekFrom::Start(start as u64))
                .map_err(|e| MempoolError::io(&path, e))?;
            self.slots_file
                .write_all(&self.slots[start..end])
                .map_err(|e| MempoolError::io(&path, e))?;
            n += end - start;
        }
        self.last_slot_write_bytes = n as u64;
        Ok(())
    }

    /// Append a packed live record; mark a FREE slot LIVE.
    ///
    /// Returns the slot index. RAM is updated immediately; sidecar write waits
    /// for [`Self::persist_due`].
    pub fn append_live_tx(
        &mut self,
        tx: &Transaction,
        txid: &Txid,
        wtxid: &Wtxid,
        fee_sat: u64,
        weight: u64,
        vins: &[VinAux],
    ) -> Result<u32, MempoolError> {
        let payload = encode_packed_live(tx, txid, wtxid, fee_sat, weight, vins)?;
        if payload.len() > u32::MAX as usize {
            return Err(MempoolError::Corrupt("tx body too large"));
        }
        let payload_len = payload.len();
        let body_off = self.reserve_body(payload_len)?;
        let off = body_off as usize;
        self.body[off..off + payload_len].copy_from_slice(&payload);
        let slot = self.alloc_slot()?;
        self.write_slot(slot, SLOT_LIVE, body_off, payload_len as u32, txid)?;
        self.live_count = self.live_count.saturating_add(1);
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
            self.flush()?;
        }
        Ok(n)
    }

    /// Mark slot DEAD and decrement live_count (confirm / RBF / eviction).
    ///
    /// If the slot is already on disk, `pwrite` that one record to DEAD (and
    /// meta live_count). An admit that was never durable stays RAM-only — crash
    /// loses it; this path must not dump the full slot table (LIVE rows whose
    /// body is still in the unpersisted tail).
    pub fn mark_slot_dead(&mut self, slot: u32) -> Result<(), MempoolError> {
        if slot >= self.slot_cap {
            return Err(MempoolError::Corrupt("slot OOB"));
        }
        let off = SLOTS_HEADER + (slot as usize) * SLOT_REC;
        if self.slots[off] != SLOT_LIVE {
            return Ok(());
        }
        let body_off = u64::from_le_bytes(self.slots[off + 4..off + 12].try_into().unwrap());
        let body_len =
            u32::from_le_bytes(self.slots[off + 12..off + 16].try_into().unwrap()) as u64;
        self.slots[off] = SLOT_DEAD;
        self.live_count = self.live_count.saturating_sub(1);
        if body_off.saturating_add(body_len) <= self.body_persisted_len {
            self.pwrite_slot_status(slot, SLOT_DEAD)?;
            self.persist_meta()?;
        }
        Ok(())
    }

    fn pwrite_slot_status(&mut self, slot: u32, status: u8) -> Result<(), MempoolError> {
        let path = self.dir.join("slots");
        let off = (SLOTS_HEADER + (slot as usize) * SLOT_REC) as u64;
        self.slots_file.seek(SeekFrom::Start(off)).map_err(|e| MempoolError::io(&path, e))?;
        self.slots_file.write_all(&[status]).map_err(|e| MempoolError::io(&path, e))?;
        Ok(())
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

    /// Rewrite body/slots to contain only LIVE payloads packed from the header.
    ///
    /// Copies existing packed payload ranges (does not re-serialize). Returns
    /// `(live_after, body_bytes_after)`. Callers must rebuild RAM indexes with
    /// the new slot numbers from [`load_live_txs`].
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
            let body_off = u64::from_le_bytes(self.slots[off + 4..off + 12].try_into().unwrap());
            let body_len =
                u32::from_le_bytes(self.slots[off + 12..off + 16].try_into().unwrap()) as usize;
            if body_off as usize + body_len > logical || body_len < BODY_TX_PREFIX {
                return Err(MempoolError::Corrupt("live slot body range"));
            }
            let start = body_off as usize;
            let new_off = new_body.len() as u64;
            new_body.extend_from_slice(&self.body[start..start + body_len]);
            let dst = SLOTS_HEADER + (next_slot as usize) * SLOT_REC;
            new_slots[dst] = SLOT_LIVE;
            new_slots[dst + 4..dst + 12].copy_from_slice(&new_off.to_le_bytes());
            new_slots[dst + 12..dst + 16].copy_from_slice(&(body_len as u32).to_le_bytes());
            new_slots[dst + 16..dst + 48].copy_from_slice(&self.slots[off + 16..off + 48]);
            next_slot += 1;
        }
        let packed_len = new_body.len();
        new_body[8..16].copy_from_slice(&(packed_len as u64).to_le_bytes());

        self.body = new_body;
        self.slots = new_slots;
        self.live_count = next_slot;
        self.install_packed_images()?;
        self.clear_dirty();
        Ok((self.live_count, packed_len))
    }

    /// Recode leftover schema-1 `fee‖weight‖raw_tx` LIVE slots into packed schema 2.
    ///
    /// Same install as compact (tmp+rename). Vin aux is empty; SH reindex
    /// batch-fills missing hashes. Schema other than 1/2 still refuses.
    fn migrate_v1_to_packed(&mut self) -> Result<(), MempoolError> {
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
            let body_off = u64::from_le_bytes(self.slots[off + 4..off + 12].try_into().unwrap());
            let body_len =
                u32::from_le_bytes(self.slots[off + 12..off + 16].try_into().unwrap()) as usize;
            if body_off as usize + body_len > logical || body_len < V1_BODY_PREFIX {
                return Err(MempoolError::Corrupt("v1 live slot body range"));
            }
            let start = body_off as usize;
            let fee_sat = u64::from_le_bytes(self.body[start..start + 8].try_into().unwrap());
            let weight = u64::from_le_bytes(self.body[start + 8..start + 16].try_into().unwrap());
            let raw = &self.body[start + V1_BODY_PREFIX..start + body_len];
            let tx: Transaction =
                deserialize(raw).map_err(|_| MempoolError::Corrupt("v1 tx deserialize"))?;
            let txid = tx.compute_txid();
            let wtxid = tx.compute_wtxid();
            let payload = encode_packed_live(&tx, &txid, &wtxid, fee_sat, weight, &[])?;
            let new_off = new_body.len() as u64;
            let plen = payload.len() as u32;
            new_body.extend_from_slice(&payload);
            let dst = SLOTS_HEADER + (next_slot as usize) * SLOT_REC;
            new_slots[dst] = SLOT_LIVE;
            new_slots[dst + 4..dst + 12].copy_from_slice(&new_off.to_le_bytes());
            new_slots[dst + 12..dst + 16].copy_from_slice(&plen.to_le_bytes());
            new_slots[dst + 16..dst + 48].copy_from_slice(txid.as_byte_array());
            next_slot += 1;
        }
        let packed_len = new_body.len();
        new_body[8..16].copy_from_slice(&(packed_len as u64).to_le_bytes());
        self.body = new_body;
        self.slots = new_slots;
        self.live_count = next_slot;
        self.install_packed_images()?;
        self.clear_dirty();
        rbitcoin_log::info!("mempool: converted schema 1 sidecar to packed (live={next_slot})");
        Ok(())
    }

    /// Load all LIVE txs from slots/body for graph rebuild.
    pub fn load_live_txs(&self) -> Result<Vec<LiveTx>, MempoolError> {
        let mut out = Vec::new();
        let logical = body_logical_len(&self.body)?;
        for slot in 0..self.slot_cap {
            let off = SLOTS_HEADER + (slot as usize) * SLOT_REC;
            if self.slots[off] != SLOT_LIVE {
                continue;
            }
            let body_off = u64::from_le_bytes(self.slots[off + 4..off + 12].try_into().unwrap());
            let body_len =
                u32::from_le_bytes(self.slots[off + 12..off + 16].try_into().unwrap()) as usize;
            if body_off as usize + body_len > logical || body_len < BODY_TX_PREFIX {
                return Err(MempoolError::Corrupt("live slot body range"));
            }
            let start = body_off as usize;
            let packed = decode_packed_live(&self.body[start..start + body_len])?;
            out.push(LiveTx { slot, packed });
        }
        Ok(out)
    }

    /// True if at least one FREE or DEAD slot can be reused.
    pub fn has_free_slot(&self) -> bool {
        self.find_free_slot().is_some()
    }

    fn find_free_slot(&self) -> Option<u32> {
        for slot in 0..self.slot_cap {
            let off = SLOTS_HEADER + (slot as usize) * SLOT_REC;
            if self.slots[off] == SLOT_FREE || self.slots[off] == SLOT_DEAD {
                return Some(slot);
            }
        }
        None
    }

    fn alloc_slot(&mut self) -> Result<u32, MempoolError> {
        if let Some(s) = self.find_free_slot() {
            return Ok(s);
        }
        // Grow once so a full live set under a small legacy cap is not a hard fail.
        self.grow_slots()?;
        self.find_free_slot().ok_or(MempoolError::Full)
    }

    /// Double slot capacity (up to [`MAX_SLOT_CAP`]) and extend the slots image with FREE records.
    pub fn grow_slots(&mut self) -> Result<(), MempoolError> {
        if self.slot_cap >= MAX_SLOT_CAP {
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

    fn write_slot(
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

    /// Ensure body has `need` free bytes; return offset of free region. Updates logical len.
    fn reserve_body(&mut self, need: usize) -> Result<u64, MempoolError> {
        let logical = body_logical_len(&self.body)?;
        let end = logical.saturating_add(need);
        if end > self.body.len() {
            let new_len = (self.body.len().max(4096) * 2).max(end + 4096);
            self.body.resize(new_len, 0);
        }
        self.body[8..16].copy_from_slice(&(end as u64).to_le_bytes());
        Ok(logical as u64)
    }

    /// Write the dirty body tail, then LIVE slots, then meta (no fsync).
    ///
    /// Append-safe: a crash after a grown body and before new slots loses admits;
    /// old LIVE ranges stay a prefix of the new body. Packed compact must not
    /// use this order (see [`Self::install_packed_images`]).
    fn persist_body_tail(&mut self) -> Result<(), MempoolError> {
        let body_path = self.dir.join("tx.body");
        let logical = body_logical_len(&self.body)? as u64;
        let start = self.body_persisted_len.min(logical);
        self.last_body_write_off = start;
        if start < BODY_HEADER as u64 {
            self.body_file.seek(SeekFrom::Start(0)).map_err(|e| MempoolError::io(&body_path, e))?;
            self.body_file
                .write_all(&self.body[..logical as usize])
                .map_err(|e| MempoolError::io(&body_path, e))?;
        } else {
            if start > BODY_HEADER as u64 || logical != start {
                self.body_file
                    .seek(SeekFrom::Start(8))
                    .map_err(|e| MempoolError::io(&body_path, e))?;
                self.body_file
                    .write_all(&self.body[8..16])
                    .map_err(|e| MempoolError::io(&body_path, e))?;
            }
            if logical > start {
                self.body_file
                    .seek(SeekFrom::Start(start))
                    .map_err(|e| MempoolError::io(&body_path, e))?;
                self.body_file
                    .write_all(&self.body[start as usize..logical as usize])
                    .map_err(|e| MempoolError::io(&body_path, e))?;
            }
        }
        self.body_file.set_len(logical).map_err(|e| MempoolError::io(&body_path, e))?;
        self.body_persisted_len = logical;
        Ok(())
    }

    /// Slots + live_count. LIVE rows whose body is not yet durable are written
    /// as FREE so a slots-ahead-of-body crash cannot load offsets past EOF.
    fn persist_slots_and_meta(&mut self) -> Result<(), MempoolError> {
        let slots_path = self.dir.join("slots");
        if let Some(disk) = self.demoted_slots_image() {
            let n = disk.len() as u64;
            self.write_slots_bytes(&slots_path, &disk)?;
            self.last_slot_write_bytes = n;
        } else {
            let buf = std::mem::take(&mut self.slots);
            let n = buf.len() as u64;
            let w = Self::write_slots_file(&mut self.slots_file, &slots_path, &buf);
            self.slots = buf;
            self.last_slot_write_bytes = n;
            w?;
        }
        self.persist_meta()
    }

    fn write_slots_bytes(&mut self, path: &Path, bytes: &[u8]) -> Result<(), MempoolError> {
        Self::write_slots_file(&mut self.slots_file, path, bytes)
    }

    fn write_slots_file(file: &mut File, path: &Path, bytes: &[u8]) -> Result<(), MempoolError> {
        file.set_len(bytes.len() as u64).map_err(|e| MempoolError::io(path, e))?;
        file.seek(SeekFrom::Start(0)).map_err(|e| MempoolError::io(path, e))?;
        file.write_all(bytes).map_err(|e| MempoolError::io(path, e))?;
        Ok(())
    }

    fn demoted_slots_image(&self) -> Option<Vec<u8>> {
        let mut disk = None;
        for slot in 0..self.slot_cap {
            let off = SLOTS_HEADER + (slot as usize) * SLOT_REC;
            if self.slots[off] != SLOT_LIVE {
                continue;
            }
            let body_off = u64::from_le_bytes(self.slots[off + 4..off + 12].try_into().unwrap());
            let body_len =
                u32::from_le_bytes(self.slots[off + 12..off + 16].try_into().unwrap()) as u64;
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

    fn persist_meta(&mut self) -> Result<(), MempoolError> {
        let meta_path = self.dir.join("meta");
        let mut meta = [0u8; META_LEN];
        write_meta_bytes(&mut meta, self.generation, self.slot_cap, self.live_count);
        self.meta_file.seek(SeekFrom::Start(0)).map_err(|e| MempoolError::io(&meta_path, e))?;
        self.meta_file.write_all(&meta).map_err(|e| MempoolError::io(&meta_path, e))?;
        Ok(())
    }

    /// Packed body+slots: both tmps `sync_all`'d, then rename body then slots.
    ///
    /// After rename, persist meta only. Rewriting packed body/slots in place
    /// (`set_len` / `write_all`) reopens the crash window tmp+rename closed.
    /// Crash after both renames, before meta: packed images + stale `live_count`
    /// still load (`load_live_txs` scans slots).
    fn install_packed_images(&mut self) -> Result<(), MempoolError> {
        let body_path = self.dir.join("tx.body");
        let slots_path = self.dir.join("slots");
        let body_tmp = self.dir.join("tx.body.tmp");
        let slots_tmp = self.dir.join("slots.tmp");
        let logical = body_logical_len(&self.body)?;
        write_file_synced(&body_tmp, &self.body[..logical])?;
        write_file_synced(&slots_tmp, &self.slots)?;
        fs::rename(&body_tmp, &body_path).map_err(|e| MempoolError::io(&body_path, e))?;
        fs::rename(&slots_tmp, &slots_path).map_err(|e| MempoolError::io(&slots_path, e))?;
        self.reopen_body_slots()?;
        self.body_persisted_len = logical as u64;
        self.last_persist_ms = self.now_ms();
        self.persist_meta()
    }

    fn reopen_body_slots(&mut self) -> Result<(), MempoolError> {
        let body_path = self.dir.join("tx.body");
        let slots_path = self.dir.join("slots");
        self.body_file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&body_path)
            .map_err(|e| MempoolError::io(&body_path, e))?;
        self.slots_file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&slots_path)
            .map_err(|e| MempoolError::io(&slots_path, e))?;
        Ok(())
    }
}

fn body_logical_len(body: &[u8]) -> Result<usize, MempoolError> {
    if body.len() < BODY_HEADER {
        return Err(MempoolError::Corrupt("body too short"));
    }
    let n = u64::from_le_bytes(body[8..16].try_into().unwrap()) as usize;
    if n < BODY_HEADER || n > body.len() {
        return Err(MempoolError::Corrupt("body logical len"));
    }
    Ok(n)
}

fn write_file_synced(path: &Path, bytes: &[u8]) -> Result<(), MempoolError> {
    let mut f = File::create(path).map_err(|e| MempoolError::io(path, e))?;
    f.write_all(bytes).map_err(|e| MempoolError::io(path, e))?;
    f.sync_all().map_err(|e| MempoolError::io(path, e))?;
    Ok(())
}

/// Finish a compact install interrupted between the two renames.
///
/// Both tmps: neither rename landed — discard. `slots.tmp` only: body rename
/// landed — finish slots. `tx.body.tmp` only: discard. No tmp (crash after both
/// renames, before meta): packed body+slots with stale `live_count` still load.
fn finish_pending_compact(dir: &Path) -> Result<(), MempoolError> {
    let body_tmp = dir.join("tx.body.tmp");
    let slots_tmp = dir.join("slots.tmp");
    match (body_tmp.exists(), slots_tmp.exists()) {
        (true, true) => {
            let _ = fs::remove_file(&body_tmp);
            let _ = fs::remove_file(&slots_tmp);
        }
        (false, true) => {
            fs::rename(&slots_tmp, dir.join("slots")).map_err(|e| MempoolError::io(dir, e))?;
        }
        (true, false) => {
            let _ = fs::remove_file(&body_tmp);
        }
        (false, false) => {}
    }
    Ok(())
}

fn accepted_schema(schema: u16) -> Result<u16, MempoolError> {
    if schema == 1 || schema == MEM_SCHEMA {
        Ok(schema)
    } else {
        Err(MempoolError::BadSchema(schema))
    }
}

fn open_or_init_meta(path: &Path) -> Result<(File, u64, u32, u32, u16), MempoolError> {
    if path.exists() {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .map_err(|e| MempoolError::io(path, e))?;
        let mut buf = [0u8; META_LEN];
        file.read_exact(&mut buf).map_err(|e| MempoolError::io(path, e))?;
        if buf[0..4] != MEM_MAGIC {
            return Err(MempoolError::BadMagic);
        }
        let schema = accepted_schema(u16::from_le_bytes([buf[4], buf[5]]))?;
        let generation = u64::from_le_bytes(buf[8..16].try_into().unwrap());
        let slot_cap = u32::from_le_bytes(buf[16..20].try_into().unwrap());
        let live_count = u32::from_le_bytes(buf[20..24].try_into().unwrap());
        if slot_cap == 0 {
            return Err(MempoolError::Corrupt("slot_cap zero"));
        }
        Ok((file, generation, slot_cap, live_count, schema))
    } else {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(path)
            .map_err(|e| MempoolError::io(path, e))?;
        let mut buf = [0u8; META_LEN];
        write_meta_bytes(&mut buf, 0, DEFAULT_SLOT_CAP, 0);
        file.write_all(&buf).map_err(|e| MempoolError::io(path, e))?;
        file.flush().map_err(|e| MempoolError::io(path, e))?;
        Ok((file, 0, DEFAULT_SLOT_CAP, 0, MEM_SCHEMA))
    }
}

fn write_meta_bytes(buf: &mut [u8; META_LEN], generation: u64, slot_cap: u32, live_count: u32) {
    buf[0..4].copy_from_slice(&MEM_MAGIC);
    buf[4..6].copy_from_slice(&MEM_SCHEMA.to_le_bytes());
    buf[6..8].copy_from_slice(&0u16.to_le_bytes());
    buf[8..16].copy_from_slice(&generation.to_le_bytes());
    buf[16..20].copy_from_slice(&slot_cap.to_le_bytes());
    buf[20..24].copy_from_slice(&live_count.to_le_bytes());
}

fn open_or_init_slots(path: &Path, slot_cap: u32) -> Result<(File, Vec<u8>), MempoolError> {
    let need = SLOTS_HEADER + (slot_cap as usize) * SLOT_REC;
    if path.exists() {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .map_err(|e| MempoolError::io(path, e))?;
        let len = file.metadata().map_err(|e| MempoolError::io(path, e))?.len() as usize;
        if len < need {
            return Err(MempoolError::Corrupt("slots file short"));
        }
        let mut buf = vec![0u8; need];
        file.read_exact(&mut buf).map_err(|e| MempoolError::io(path, e))?;
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
        file.write_all(&buf).map_err(|e| MempoolError::io(path, e))?;
        file.flush().map_err(|e| MempoolError::io(path, e))?;
        Ok((file, buf))
    }
}

fn open_or_init_body(path: &Path) -> Result<(File, Vec<u8>), MempoolError> {
    let initial = BODY_HEADER + 64;
    if path.exists() {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .map_err(|e| MempoolError::io(path, e))?;
        let len = file.metadata().map_err(|e| MempoolError::io(path, e))?.len() as usize;
        if len < BODY_HEADER {
            return Err(MempoolError::Corrupt("body too short"));
        }
        let mut buf = vec![0u8; len];
        file.seek(SeekFrom::Start(0)).map_err(|e| MempoolError::io(path, e))?;
        file.read_exact(&mut buf).map_err(|e| MempoolError::io(path, e))?;
        if buf[0..4] != MEM_MAGIC {
            return Err(MempoolError::BadMagic);
        }
        accepted_schema(u16::from_le_bytes([buf[4], buf[5]]))?;
        let logical = body_logical_len(&buf)?;
        if logical > len {
            return Err(MempoolError::Corrupt("body logical past file"));
        }
        // Spare capacity in process for appends (not on disk until persist).
        if buf.len() < initial {
            buf.resize(initial, 0);
        }
        Ok((file, buf))
    } else {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(path)
            .map_err(|e| MempoolError::io(path, e))?;
        let mut buf = vec![0u8; initial];
        buf[0..4].copy_from_slice(&MEM_MAGIC);
        buf[4..6].copy_from_slice(&MEM_SCHEMA.to_le_bytes());
        buf[8..16].copy_from_slice(&(BODY_HEADER as u64).to_le_bytes());
        file.write_all(&buf[..BODY_HEADER]).map_err(|e| MempoolError::io(path, e))?;
        file.flush().map_err(|e| MempoolError::io(path, e))?;
        Ok((file, buf))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp_dir() -> rbitcoin_store::testutil::TempDir {
        rbitcoin_store::testutil::TempDir::labeled("mempool").unwrap()
    }

    fn tiny_tx() -> bitcoin::Transaction {
        bitcoin::Transaction {
            version: bitcoin::transaction::Version::TWO,
            lock_time: bitcoin::absolute::LockTime::ZERO,
            input: vec![bitcoin::TxIn {
                previous_output: bitcoin::OutPoint::null(),
                script_sig: bitcoin::ScriptBuf::new(),
                sequence: bitcoin::Sequence::MAX,
                witness: bitcoin::Witness::new(),
            }],
            output: vec![bitcoin::TxOut {
                value: bitcoin::Amount::from_sat(1),
                script_pubkey: bitcoin::ScriptBuf::new(),
            }],
        }
    }

    fn put_live(mp: &mut Mempool, tid: &Txid, fee: u64, weight: u64) -> u32 {
        let tx = tiny_tx();
        mp.append_live_tx(&tx, tid, &tx.compute_wtxid(), fee, weight, &[]).unwrap()
    }

    #[test]
    fn empty_create_reopen_flush() {
        let dir = tmp_dir();
        {
            let mut mp = Mempool::open_or_create(&dir).expect("create");
            let m = mp.meta();
            assert_eq!(m.generation, 0);
            assert_eq!(m.live_count, 0);
            assert_eq!(m.slot_cap, DEFAULT_SLOT_CAP);
            mp.flush().expect("flush");
            assert_eq!(mp.generation(), 1);
        }
        {
            let mp = Mempool::open_or_create(&dir).expect("reopen");
            assert_eq!(mp.generation(), 1);
            assert_eq!(mp.live_count(), 0);
            assert!(mp.dir().join("meta").exists());
            assert!(mp.dir().join("slots").exists());
            assert!(mp.dir().join("tx.body").exists());
        }
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn persist_due_waits_five_seconds() {
        let dir = tmp_dir();
        {
            let mut mp = Mempool::open_or_create(&dir).unwrap();
            mp.set_now_ms(0);
            let tid = Txid::from_byte_array([0x22; 32]);
            put_live(&mut mp, &tid, 1, 400);
            mp.persist_due().unwrap();
            assert_eq!(mp.live_count(), 1);
        }
        {
            let mp = Mempool::open_or_create(&dir).unwrap();
            assert_eq!(mp.live_count(), 0, "admit at t=0 is not durable");
        }
        {
            let mut mp = Mempool::open_or_create(&dir).unwrap();
            mp.set_now_ms(0);
            let tid = Txid::from_byte_array([0x33; 32]);
            put_live(&mut mp, &tid, 1, 400);
            mp.set_now_ms(PERSIST_INTERVAL_MS);
            mp.persist_due().unwrap();
        }
        {
            let mp = Mempool::open_or_create(&dir).unwrap();
            assert_eq!(mp.live_count(), 1, "persist_due after 5s is durable");
        }
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn dead_pwrite_of_flushed_slot_is_immediate() {
        let dir = tmp_dir();
        let mut mp = Mempool::open_or_create(&dir).unwrap();
        mp.set_now_ms(0);
        let tid = Txid::from_byte_array([0x11; 32]);
        let slot = put_live(&mut mp, &tid, 1, 400);
        mp.flush().unwrap();
        let body_before = fs::read(dir.join("tx.body")).unwrap();
        mp.mark_slot_dead(slot).unwrap();
        assert_eq!(mp.live_count(), 0);
        let body_after = fs::read(dir.join("tx.body")).unwrap();
        assert_eq!(body_before, body_after, "DEAD pwrite must not rewrite tx.body");
        drop(mp);
        let mp = Mempool::open_or_create(&dir).unwrap();
        assert_eq!(mp.live_count(), 0, "DEAD of a flushed slot is durable without 5s");
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn dead_of_unpersisted_admit_does_not_resurrect() {
        let dir = tmp_dir();
        {
            let mut mp = Mempool::open_or_create(&dir).unwrap();
            mp.set_now_ms(0);
            let tid = Txid::from_byte_array([0x44; 32]);
            let slot = put_live(&mut mp, &tid, 1, 400);
            mp.mark_slot_dead(slot).unwrap();
            mp.persist_slots_without_body().unwrap();
        }
        {
            let mp = Mempool::open_or_create(&dir).unwrap();
            assert_eq!(mp.live_count(), 0);
            assert!(
                mp.load_live_txs().unwrap().is_empty(),
                "unpersisted admit DEAD must not resurrect"
            );
        }
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn persist_slots_without_tail_does_not_point_live_past_body() {
        let dir = tmp_dir();
        let mut mp = Mempool::open_or_create(&dir).unwrap();
        mp.set_now_ms(0);
        let raw = tiny_tx();
        let t1 = Txid::from_byte_array([0x01; 32]);
        mp.append_live_tx(&raw, &t1, &raw.compute_wtxid(), 1, 400, &[]).unwrap();
        let persisted = mp.body_persisted_len();
        mp.persist_slots_without_body().unwrap();
        drop(mp);
        let mp = Mempool::open_or_create(&dir).unwrap();
        let live = mp.load_live_txs().expect("FREE-not-LIVE past EOF must load");
        assert!(live.is_empty(), "LIVE must not be written past body_persisted_len={persisted}");
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn persist_deaths_writes_slots_not_body() {
        let dir = tmp_dir();
        let n = 8u32;
        let mut slots = Vec::with_capacity(n as usize);
        let mut mp = Mempool::open_or_create(&dir).unwrap();
        mp.set_now_ms(0);
        for i in 0..n {
            let mut id = [0u8; 32];
            id[0] = i as u8;
            id[1] = (i >> 8) as u8;
            let tid = Txid::from_byte_array(id);
            slots.push(put_live(&mut mp, &tid, 1, 400));
        }
        mp.flush().unwrap();
        let body_before = fs::read(dir.join("tx.body")).unwrap();
        for slot in &slots {
            mp.mark_slot_dead(*slot).unwrap();
        }
        mp.persist_if_dirty().unwrap();
        let body_after = fs::read(dir.join("tx.body")).unwrap();
        assert_eq!(body_before, body_after, "DEAD persist must not rewrite mempool/tx.body");
        drop(mp);
        let mp = Mempool::open_or_create(&dir).unwrap();
        assert_eq!(mp.live_count(), 0);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn incremental_body_persist_writes_only_dirty_tail() {
        let dir = tmp_dir();
        let mut mp = Mempool::open_or_create(&dir).unwrap();
        mp.set_now_ms(0);
        let raw = tiny_tx();
        let t1 = Txid::from_byte_array([0x01; 32]);
        mp.append_live_tx(&raw, &t1, &raw.compute_wtxid(), 1, 400, &[]).unwrap();
        mp.set_now_ms(PERSIST_INTERVAL_MS);
        mp.persist_due().unwrap();
        let first_end = mp.body_persisted_len();
        let body_after_first = fs::read(dir.join("tx.body")).unwrap();
        let t2 = Txid::from_byte_array([0x02; 32]);
        mp.append_live_tx(&raw, &t2, &raw.compute_wtxid(), 2, 400, &[]).unwrap();
        mp.set_now_ms(PERSIST_INTERVAL_MS * 2);
        mp.persist_due().unwrap();
        assert_eq!(
            mp.last_body_write_off(),
            first_end,
            "second persist must start at the first payload's end"
        );
        let body_after_second = fs::read(dir.join("tx.body")).unwrap();
        assert_eq!(
            &body_after_second[BODY_HEADER..first_end as usize],
            &body_after_first[BODY_HEADER..],
            "first payload bytes must be unchanged"
        );
        drop(mp);
        let mp = Mempool::open_or_create(&dir).unwrap();
        assert_eq!(mp.load_live_txs().unwrap().len(), 2);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn persist_due_pwrites_only_new_live_slots() {
        let dir = tmp_dir();
        let mut mp = Mempool::open_or_create(&dir).unwrap();
        mp.set_now_ms(0);
        let raw = tiny_tx();
        let t1 = Txid::from_byte_array([0x01; 32]);
        mp.append_live_tx(&raw, &t1, &raw.compute_wtxid(), 1, 400, &[]).unwrap();
        mp.set_now_ms(PERSIST_INTERVAL_MS);
        mp.persist_due().unwrap();
        assert_eq!(
            mp.last_slot_write_bytes(),
            SLOT_REC as u64,
            "first persist_due writes one LIVE record, not the full table"
        );
        let t2 = Txid::from_byte_array([0x02; 32]);
        mp.append_live_tx(&raw, &t2, &raw.compute_wtxid(), 2, 400, &[]).unwrap();
        mp.set_now_ms(PERSIST_INTERVAL_MS * 2);
        mp.persist_due().unwrap();
        assert_eq!(
            mp.last_slot_write_bytes(),
            SLOT_REC as u64,
            "second persist_due must not rewrite the first LIVE record"
        );
        drop(mp);
        let mp = Mempool::open_or_create(&dir).unwrap();
        assert_eq!(mp.load_live_txs().unwrap().len(), 2);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn flush_bumps_generation_monotone() {
        let dir = tmp_dir();
        let mut mp = Mempool::open_or_create(&dir).unwrap();
        for i in 1..=5 {
            mp.flush().unwrap();
            assert_eq!(mp.generation(), i);
        }
        drop(mp);
        let mp = Mempool::open_or_create(&dir).unwrap();
        assert_eq!(mp.generation(), 5);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn abandon_live_clears_reopen() {
        let dir = tmp_dir();
        let mut mp = Mempool::open_or_create(&dir).unwrap();
        let tid = Txid::from_byte_array([0x22; 32]);
        put_live(&mut mp, &tid, 1, 400);
        mp.flush().unwrap();
        assert_eq!(mp.live_count(), 1);
        assert_eq!(mp.abandon_live().unwrap(), 1);
        drop(mp);
        let mp = Mempool::open_or_create(&dir).unwrap();
        assert_eq!(mp.live_count(), 0);
        assert!(mp.load_live_txs().unwrap().is_empty());
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn append_mark_dead_stats_and_bad_magic() {
        let dir = tmp_dir();
        let mut mp = Mempool::open_or_create(&dir).unwrap();
        let txid = Txid::from_byte_array([0x11; 32]);
        let slot =
            mp.append_live_tx(&tiny_tx(), &txid, &tiny_tx().compute_wtxid(), 10, 400, &[]).unwrap();
        assert_eq!(mp.live_count(), 1);
        let (free, live, dead) = mp.slot_stats();
        assert_eq!(live, 1);
        assert!(free + live + dead >= 1);
        mp.flush().unwrap();
        drop(mp);
        let mut mp = Mempool::open_or_create(&dir).unwrap();
        assert_eq!(mp.live_count(), 1);
        let (free2, live2, _dead2) = mp.slot_stats();
        assert_eq!(live2, 1);
        assert!(free2 + live2 >= 1);
        mp.mark_slot_dead(slot).unwrap();
        assert_eq!(mp.live_count(), 0);
        assert!(mp.mark_slot_dead(u32::MAX).is_err());
        drop(mp);

        let dir2 = tmp_dir();
        {
            let _ = Mempool::open_or_create(&dir2).unwrap();
        }
        {
            let mut f = fs::OpenOptions::new().write(true).open(dir2.join("meta")).unwrap();
            f.write_all(b"BAD!").unwrap();
        }
        match Mempool::open_or_create(&dir2) {
            Err(MempoolError::BadMagic) => {}
            Ok(_) => panic!("expected BadMagic, got Ok"),
            Err(e) => panic!("expected BadMagic, got {e}"),
        }
        let _ = fs::remove_dir_all(&dir);
        let _ = fs::remove_dir_all(&dir2);
    }

    /// Legacy tiny slot table must grow instead of returning Corrupt("slot table full").
    #[test]
    fn full_live_table_grows_not_corrupt() {
        let dir = tmp_dir();
        // Seed a 4-slot sidecar (legacy mainnet shape).
        fs::create_dir_all(&dir).unwrap();
        let tiny = 4u32;
        {
            let mut meta = [0u8; META_LEN];
            write_meta_bytes(&mut meta, 0, tiny, 0);
            fs::write(dir.join("meta"), meta).unwrap();
            let mut slots = vec![0u8; SLOTS_HEADER + (tiny as usize) * SLOT_REC];
            slots[0..4].copy_from_slice(&MEM_MAGIC);
            slots[4..6].copy_from_slice(&MEM_SCHEMA.to_le_bytes());
            slots[8..12].copy_from_slice(&tiny.to_le_bytes());
            fs::write(dir.join("slots"), &slots).unwrap();
            let mut body = vec![0u8; BODY_HEADER];
            body[0..4].copy_from_slice(&MEM_MAGIC);
            body[4..6].copy_from_slice(&MEM_SCHEMA.to_le_bytes());
            body[8..16].copy_from_slice(&(BODY_HEADER as u64).to_le_bytes());
            fs::write(dir.join("tx.body"), &body).unwrap();
        }
        let mut mp = Mempool::open_or_create(&dir).unwrap();
        assert_eq!(mp.meta().slot_cap, tiny);
        for i in 0..tiny {
            let mut tid = [0u8; 32];
            tid[0] = i as u8 + 1;
            let txid = Txid::from_byte_array(tid);
            put_live(&mut mp, &txid, 1, 400);
        }
        assert!(!mp.has_free_slot());
        // 5th append must grow, not Corrupt.
        let tid5 = Txid::from_byte_array([0x55; 32]);
        let tx = tiny_tx();
        let r = mp.append_live_tx(&tx, &tid5, &tx.compute_wtxid(), 1, 400, &[]);
        assert!(r.is_ok(), "expected grow on full table, got {:?}", r.err().map(|e| e.to_string()));
        assert!(mp.meta().slot_cap > tiny);
        assert_eq!(mp.live_count(), tiny + 1);
        // Must not be the old Corrupt message.
        assert!(!format!("{:?}", MempoolError::Full).contains("corrupt"));
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn persist_new_body_old_slots_reopens() {
        let dir = tmp_dir();
        let mut mp = Mempool::open_or_create(&dir).unwrap();
        let raw = tiny_tx();
        let t1 = Txid::from_byte_array([0x01; 32]);
        mp.append_live_tx(&raw, &t1, &raw.compute_wtxid(), 1, 400, &[]).unwrap();
        mp.flush().unwrap();
        let slots_first = fs::read(dir.join("slots")).unwrap();
        let t2 = Txid::from_byte_array([0x02; 32]);
        mp.append_live_tx(&raw, &t2, &raw.compute_wtxid(), 1, 400, &[]).unwrap();
        mp.flush().unwrap();
        drop(mp);
        fs::write(dir.join("slots"), &slots_first).unwrap();
        let mp = Mempool::open_or_create(&dir).unwrap();
        let live = mp.load_live_txs().expect("new body + old slots must load");
        assert!(live.len() <= 1);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn new_slots_old_short_body_is_corrupt() {
        let dir = tmp_dir();
        let mut mp = Mempool::open_or_create(&dir).unwrap();
        let raw = tiny_tx();
        let t1 = Txid::from_byte_array([0x01; 32]);
        mp.append_live_tx(&raw, &t1, &raw.compute_wtxid(), 1, 400, &[]).unwrap();
        mp.flush().unwrap();
        let body_first = fs::read(dir.join("tx.body")).unwrap();
        let t2 = Txid::from_byte_array([0x02; 32]);
        mp.append_live_tx(&raw, &t2, &raw.compute_wtxid(), 1, 400, &[]).unwrap();
        mp.flush().unwrap();
        drop(mp);
        fs::write(dir.join("tx.body"), body_first).unwrap();
        let mp = Mempool::open_or_create(&dir).unwrap();
        match mp.load_live_txs() {
            Err(MempoolError::Corrupt(m)) => {
                assert!(m.contains("live slot body range"), "{m}");
            }
            other => panic!("expected live slot body range, got {other:?}"),
        }
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn compact_crash_after_body_rename_finishes_slots() {
        let dir = tmp_dir();
        let mut mp = Mempool::open_or_create(&dir).unwrap();
        let raw = tiny_tx();
        let t1 = Txid::from_byte_array([0x01; 32]);
        mp.append_live_tx(&raw, &t1, &raw.compute_wtxid(), 1, 400, &[]).unwrap();
        mp.flush().unwrap();
        let slots_unpacked = fs::read(dir.join("slots")).unwrap();
        mp.mark_slot_dead(0).unwrap();
        let t2 = Txid::from_byte_array([0x02; 32]);
        mp.append_live_tx(&raw, &t2, &raw.compute_wtxid(), 1, 400, &[]).unwrap();
        mp.flush().unwrap();
        mp.compact().unwrap();
        let slots_packed = fs::read(dir.join("slots")).unwrap();
        drop(mp);
        fs::write(dir.join("slots"), &slots_unpacked).unwrap();
        fs::write(dir.join("slots.tmp"), &slots_packed).unwrap();
        let mp = Mempool::open_or_create(&dir).unwrap();
        assert!(!dir.join("slots.tmp").exists());
        mp.load_live_txs().expect("open must finish slots.tmp");
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn compact_both_tmps_discarded_on_open() {
        let dir = tmp_dir();
        let mut mp = Mempool::open_or_create(&dir).unwrap();
        mp.flush().unwrap();
        drop(mp);
        fs::copy(dir.join("tx.body"), dir.join("tx.body.tmp")).unwrap();
        fs::copy(dir.join("slots"), dir.join("slots.tmp")).unwrap();
        let mp = Mempool::open_or_create(&dir).unwrap();
        assert!(!dir.join("tx.body.tmp").exists());
        assert!(!dir.join("slots.tmp").exists());
        mp.load_live_txs().unwrap();
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn compact_crash_after_slots_rename_before_meta_still_loads() {
        let dir = tmp_dir();
        let mut mp = Mempool::open_or_create(&dir).unwrap();
        let raw = tiny_tx();
        let t1 = Txid::from_byte_array([0x01; 32]);
        mp.append_live_tx(&raw, &t1, &raw.compute_wtxid(), 1, 400, &[]).unwrap();
        let t2 = Txid::from_byte_array([0x02; 32]);
        mp.append_live_tx(&raw, &t2, &raw.compute_wtxid(), 1, 400, &[]).unwrap();
        mp.flush().unwrap();
        assert_eq!(mp.live_count(), 2);
        let meta_before_compact = fs::read(dir.join("meta")).unwrap();
        mp.mark_slot_dead(0).unwrap();
        mp.compact().unwrap();
        drop(mp);
        fs::write(dir.join("meta"), &meta_before_compact).unwrap();
        let mp = Mempool::open_or_create(&dir).unwrap();
        let live = mp.load_live_txs().expect("packed images + stale live_count must load");
        assert_eq!(live.len(), 1, "packed live set");
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn leftover_schema_v1_converts_on_open() {
        use bitcoin::consensus::encode::serialize;
        let dir = tmp_dir();
        fs::create_dir_all(&dir).unwrap();
        let tx = tiny_tx();
        let raw = serialize(&tx);
        let tid = tx.compute_txid();
        let fee = 123u64;
        let weight = 400u64;
        {
            let mut meta = [0u8; META_LEN];
            write_meta_bytes(&mut meta, 0, 4, 1);
            meta[4..6].copy_from_slice(&1u16.to_le_bytes());
            fs::write(dir.join("meta"), meta).unwrap();
            let mut slots = vec![0u8; SLOTS_HEADER + 4 * SLOT_REC];
            slots[0..4].copy_from_slice(&MEM_MAGIC);
            slots[4..6].copy_from_slice(&1u16.to_le_bytes());
            slots[8..12].copy_from_slice(&4u32.to_le_bytes());
            let off = SLOTS_HEADER;
            slots[off] = SLOT_LIVE;
            let body_off = BODY_HEADER as u64;
            let body_len = (16 + raw.len()) as u32;
            slots[off + 4..off + 12].copy_from_slice(&body_off.to_le_bytes());
            slots[off + 12..off + 16].copy_from_slice(&body_len.to_le_bytes());
            slots[off + 16..off + 48].copy_from_slice(tid.as_byte_array());
            fs::write(dir.join("slots"), &slots).unwrap();
            let mut body = vec![0u8; BODY_HEADER + body_len as usize];
            body[0..4].copy_from_slice(&MEM_MAGIC);
            body[4..6].copy_from_slice(&1u16.to_le_bytes());
            body[8..16].copy_from_slice(&((BODY_HEADER + body_len as usize) as u64).to_le_bytes());
            body[BODY_HEADER..BODY_HEADER + 8].copy_from_slice(&fee.to_le_bytes());
            body[BODY_HEADER + 8..BODY_HEADER + 16].copy_from_slice(&weight.to_le_bytes());
            body[BODY_HEADER + 16..].copy_from_slice(&raw);
            fs::write(dir.join("tx.body"), &body).unwrap();
        }
        {
            let mp = Mempool::open_or_create(&dir).expect("v1 leftover converts");
            let live = mp.load_live_txs().unwrap();
            assert_eq!(live.len(), 1);
            assert_eq!(live[0].packed.fee_sat, fee);
            assert_eq!(live[0].packed.weight, weight);
            assert_eq!(live[0].packed.txid, tid);
            assert_eq!(live[0].packed.wtxid, tx.compute_wtxid());
            assert_eq!(serialize(&live[0].packed.tx), raw);
            assert!(live[0]
                .packed
                .vins
                .iter()
                .all(|v| v.script_hash.is_none() && v.create_fk.is_none()));
        }
        let meta = fs::read(dir.join("meta")).unwrap();
        assert_eq!(&meta[4..6], &MEM_SCHEMA.to_le_bytes(), "meta stamped schema 2");
        let body = fs::read(dir.join("tx.body")).unwrap();
        assert_eq!(&body[4..6], &MEM_SCHEMA.to_le_bytes(), "body stamped schema 2");
        {
            let mp = Mempool::open_or_create(&dir).unwrap();
            let live = mp.load_live_txs().unwrap();
            assert_eq!(live.len(), 1);
            assert_eq!(live[0].packed.txid, tid);
            assert_eq!(serialize(&live[0].packed.tx), raw);
        }
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn leftover_schema_unknown_is_refused() {
        let dir = tmp_dir();
        fs::create_dir_all(&dir).unwrap();
        {
            let mut meta = [0u8; META_LEN];
            write_meta_bytes(&mut meta, 0, 4, 0);
            meta[4..6].copy_from_slice(&3u16.to_le_bytes());
            fs::write(dir.join("meta"), meta).unwrap();
            let mut slots = vec![0u8; SLOTS_HEADER + 4 * SLOT_REC];
            slots[0..4].copy_from_slice(&MEM_MAGIC);
            slots[4..6].copy_from_slice(&3u16.to_le_bytes());
            fs::write(dir.join("slots"), &slots).unwrap();
            let mut body = vec![0u8; BODY_HEADER];
            body[0..4].copy_from_slice(&MEM_MAGIC);
            body[4..6].copy_from_slice(&3u16.to_le_bytes());
            body[8..16].copy_from_slice(&(BODY_HEADER as u64).to_le_bytes());
            fs::write(dir.join("tx.body"), &body).unwrap();
        }
        match Mempool::open_or_create(&dir) {
            Err(MempoolError::BadSchema(3)) => {}
            Ok(_) => panic!("expected BadSchema(3), got Ok"),
            Err(e) => panic!("expected BadSchema(3), got {e}"),
        }
        let _ = fs::remove_dir_all(&dir);
    }
}
