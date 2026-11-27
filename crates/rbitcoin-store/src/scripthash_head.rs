//! Open-addressed head for hybrid scripthash: `key[16]` → pack8 value (24 B slots).
//!
//! Key is the first 16 bytes of Electrum SHA256(spk). Public APIs take full 32 B
//! hashes and truncate. Values are [`ShHeadValue`] pack8 encodings.
//!
//! # Occupancy (startup)
//!
//! Slot occupancy is process state for load-factor / `is_empty`, **not** needed for
//! probe lookups. Mainnet shards are multi‑GiB — a full slot scan on every open is
//! unacceptable. Durable sidecar `{shard}.occ` holds the count (written on create,
//! reinit, cold install, and inserts). Large shards without a sidecar skip
//! the scan and mark occupancy **unknown** (never treated as empty, never bulk-fill).
//! A zero sidecar on a large shard is not proof the slots are empty: the table
//! write can land before the sidecar rename. Open scans that head. Rows keep
//! the count. An empty table is removed, so the next open has no file to scan.
//! A head this process just created is still empty. The ingest file itself is
//! created on the first insert, not when the store is created.

use crate::durability::Durable as _;
use crate::error::StoreError;
use crate::file::{self, TableFile, FILE_HEADER_LEN};
use crate::open_address::{self, MAX_LOAD_DEN, MAX_LOAD_NUM};
use crate::scripthash_layout::{
    head_key_from_full, pack8_bytes, unpack8_bytes, ShHeadKey, ShHeadValue, SH_HEAD_KEY_LEN,
    SH_HEAD_SLOT_SIZE, SH_HEAD_VALUE_LEN,
};
use bitcoin_hashes::{sha256, Hash, HashEngine};
use rbitcoin_primitives::{bytes, TableKind};
use std::collections::BTreeMap;
#[cfg(debug_assertions)]
use std::collections::BTreeSet;
use std::io::{Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

/// Full occupancy scan only for tiny head files (tests / early IBD).
#[doc(hidden)]
pub const OCC_SCAN_BYTE_CAP: u64 = 16 * 1024 * 1024;
#[doc(hidden)]
pub const OCC_MAGIC: &[u8; 8] = b"SHOCC001";

fn sh_slot_at(buf: &[u8], off: usize) -> [u8; SH_HEAD_SLOT_SIZE] {
    let mut rec = [0u8; SH_HEAD_SLOT_SIZE];
    rec.copy_from_slice(&buf[off..off + SH_HEAD_SLOT_SIZE]);
    rec
}

#[doc(hidden)]
pub const SLOTS_PER_CHUNK: u64 = 128;
#[doc(hidden)]
pub const CHUNK_CACHE_MAX: usize = 256;

pub const SH_HEAD_FULL: &str = "invariant: scripthash head full";

/// Shard index from the **high bits** of `scripthash[0]` (power-of-two `n_shards`).
///
/// Unlike `key[0] % n`, this makes lexicographic order of full scripthashes
/// contiguous per shard: for 64 shards, bytes `0x00–0x03` → shard 0,
/// `0x04–0x07` → 1, … So sorted runs already stream one complete shard at a
/// time — cold materialize builds one live OA image per band.
///
/// `n_shards` must be 1 or a power of two ≤ 256 (mainnet SH uses **64**).
#[inline]
pub fn prefix_shard_of(full: &[u8; 32], n_shards: usize) -> usize {
    let n = n_shards.max(1);
    if n == 1 {
        return 0;
    }
    debug_assert!(
        n.is_power_of_two() && n <= 256,
        "scripthash prefix shards must be power-of-two ≤ 256, got {n}"
    );
    let bits = n.trailing_zeros() as usize;
    (full[0] as usize) >> (8 - bits)
}

pub struct ScriptHashHead {
    pub file: TableFile,
    pub state: Mutex<HashState>,
}

#[doc(hidden)]
pub struct HashState {
    pub slots: u64,
    pub occupied: u64,
    /// When false, `occupied` is not authoritative (large open without `.occ`).
    /// Never treat as empty / never bulk-fill from this state.
    pub occ_known: bool,
}

/// Drop the slot table and its sidecar. The caller must have closed the head
/// first so Windows can unlink the open file.
#[doc(hidden)]
pub fn remove_ingest_files(head_path: &Path) -> Result<(), StoreError> {
    if head_path.exists() {
        std::fs::remove_file(head_path).map_err(|e| StoreError::io(head_path, e))?;
    }
    let occ = occ_sidecar_path(head_path);
    if occ.exists() {
        std::fs::remove_file(&occ).map_err(|e| StoreError::io(&occ, e))?;
    }
    let ahead = ahead_path(head_path);
    if ahead.exists() {
        std::fs::remove_file(&ahead).map_err(|e| StoreError::io(&ahead, e))?;
    }
    Ok(())
}

#[doc(hidden)]
pub fn occ_sidecar_path(head_path: &Path) -> PathBuf {
    let mut p = head_path.as_os_str().to_os_string();
    p.push(".occ");
    PathBuf::from(p)
}

#[doc(hidden)]
pub fn load_occ_sidecar(head_path: &Path) -> Option<u64> {
    let p = occ_sidecar_path(head_path);
    let Ok(buf) = std::fs::read(&p) else {
        return None;
    };
    if buf.len() < 16 || &buf[0..8] != OCC_MAGIC {
        return None;
    }
    let mut word = [0u8; 8];
    word.copy_from_slice(&buf[8..16]);
    Some(u64::from_le_bytes(word))
}

#[doc(hidden)]
pub fn store_occ_sidecar(head_path: &Path, occupied: u64) -> Result<(), StoreError> {
    let p = occ_sidecar_path(head_path);
    let mut buf = [0u8; 16];
    buf[0..8].copy_from_slice(OCC_MAGIC);
    buf[8..16].copy_from_slice(&occupied.to_le_bytes());
    crate::file::write_synced_tmp_rename(&p, &buf)
}

#[doc(hidden)]
pub fn scan_occupied(file: &TableFile, slots: u64) -> Result<u64, StoreError> {
    let mut occupied = 0u64;
    let mut buf = vec![0u8; SH_HEAD_SLOT_SIZE * 1024];
    let mut slot = 0u64;
    while slot < slots {
        let n = ((slots - slot) as usize).min(1024);
        let off = FILE_HEADER_LEN as u64 + slot * SH_HEAD_SLOT_SIZE as u64;
        let bytes = n * SH_HEAD_SLOT_SIZE;
        file.read_at(off, &mut buf[..bytes])?;
        for rec in buf[..bytes].as_chunks::<SH_HEAD_SLOT_SIZE>().0 {
            let k = bytes!(rec, 0, 16);
            let v = bytes!(rec, 16, 8);
            if !is_empty_slot(&k, &v) {
                unpack8_bytes(&v)?;
                occupied += 1;
            }
        }
        slot += n as u64;
    }
    Ok(occupied)
}

const AHEAD_MAGIC: &[u8; 4] = b"SHA1";
const AHEAD_VERSION: u16 = 1;
const AHEAD_HEAD: usize = 12;
const AHEAD_REC: usize = 8 + 32 + 4096;

#[cfg(debug_assertions)]
static FAIL_AHEAD: Mutex<BTreeSet<PathBuf>> = Mutex::new(BTreeSet::new());
#[cfg(debug_assertions)]
static FAIL_SECOND_WRITE: Mutex<BTreeSet<PathBuf>> = Mutex::new(BTreeSet::new());

/// Leave the next scripthash-head page image behind and fail before the pwrite.
#[cfg(debug_assertions)]
#[doc(hidden)]
pub fn fail_next_scripthash_head_flush_after_ahead(path: &Path) {
    FAIL_AHEAD
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .insert(path.to_path_buf());
}

/// Fail after the second dirty-chunk pwrite of the next flush of `path`.
#[cfg(debug_assertions)]
#[doc(hidden)]
pub fn fail_next_scripthash_head_flush_on_second_write(path: &Path) {
    FAIL_SECOND_WRITE
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .insert(path.to_path_buf());
}

#[cfg(debug_assertions)]
fn take_fail_ahead(path: &Path) -> bool {
    FAIL_AHEAD
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .remove(path)
}

#[cfg(debug_assertions)]
fn take_fail_second_write(path: &Path) -> bool {
    FAIL_SECOND_WRITE
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .remove(path)
}

fn ahead_path(path: &Path) -> PathBuf {
    let mut name = path
        .file_name()
        .unwrap_or_else(|| std::ffi::OsStr::new("shhead"))
        .to_os_string();
    name.push(".ahead");
    match path.parent() {
        Some(parent) if !parent.as_os_str().is_empty() => parent.join(name),
        _ => PathBuf::from(name),
    }
}

fn read_page(path: &Path, file: &TableFile, block: u64) -> Result<[u8; 4096], StoreError> {
    let mut image = [0u8; 4096];
    let len = std::fs::metadata(path)
        .map_err(|e| StoreError::io(path, e))?
        .len();
    if block < len {
        let n = ((len - block) as usize).min(4096);
        file.pread_all(block, &mut image[..n])?;
    }
    Ok(image)
}

fn parse_ahead_pages(raw: &[u8]) -> Result<BTreeMap<u64, [u8; 4096]>, StoreError> {
    if raw.len() < AHEAD_HEAD || raw[0..4] != *AHEAD_MAGIC {
        return Err(StoreError::Corrupt("scripthash head ahead header"));
    }
    if u16::from_le_bytes([raw[4], raw[5]]) != AHEAD_VERSION {
        return Err(StoreError::Corrupt("scripthash head ahead version"));
    }
    let count = u32::from_le_bytes([raw[8], raw[9], raw[10], raw[11]]) as usize;
    let payload = raw.len().saturating_sub(AHEAD_HEAD);
    if !payload.is_multiple_of(AHEAD_REC) || payload / AHEAD_REC != count {
        return Err(StoreError::Corrupt("scripthash head ahead length"));
    }
    let mut at = AHEAD_HEAD;
    let mut images = BTreeMap::new();
    for _ in 0..count {
        if raw.len() < at + AHEAD_REC {
            return Err(StoreError::Corrupt("scripthash head ahead short"));
        }
        let mut off_buf = [0u8; 8];
        off_buf.copy_from_slice(&raw[at..at + 8]);
        let off = u64::from_le_bytes(off_buf);
        let sum = &raw[at + 8..at + 40];
        let image = &raw[at + 40..at + AHEAD_REC];
        if !off.is_multiple_of(4096) {
            return Err(StoreError::Corrupt("scripthash head ahead offset"));
        }
        let mut eng = sha256::HashEngine::default();
        eng.input(&off.to_le_bytes());
        eng.input(image);
        if sha256::Hash::from_engine(eng).to_byte_array() != *sum {
            return Err(StoreError::Corrupt("scripthash head ahead checksum"));
        }
        let mut page = [0u8; 4096];
        page.copy_from_slice(image);
        images.insert(off, page);
        at += AHEAD_REC;
    }
    if at != raw.len() {
        return Err(StoreError::Corrupt("scripthash head ahead length"));
    }
    Ok(images)
}

fn load_ahead_pages(path: &Path) -> Result<BTreeMap<u64, [u8; 4096]>, StoreError> {
    let side = ahead_path(path);
    match std::fs::read(&side) {
        Ok(raw) => parse_ahead_pages(&raw),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(BTreeMap::new()),
        Err(e) => Err(StoreError::io(&side, e)),
    }
}

fn encode_ahead_pages(blocks: &BTreeMap<u64, [u8; 4096]>) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.extend_from_slice(AHEAD_MAGIC);
    buf.extend_from_slice(&AHEAD_VERSION.to_le_bytes());
    buf.extend_from_slice(&0u16.to_le_bytes());
    buf.extend_from_slice(&(blocks.len() as u32).to_le_bytes());
    for (off, image) in blocks {
        buf.extend_from_slice(&off.to_le_bytes());
        let mut eng = sha256::HashEngine::default();
        eng.input(&off.to_le_bytes());
        eng.input(image);
        buf.extend_from_slice(&sha256::Hash::from_engine(eng).to_byte_array());
        buf.extend_from_slice(image);
    }
    buf
}

/// Apply full 4 KiB page images left by an interrupted `SlotPageCache::flush`.
fn restore_ahead(path: &Path) -> Result<(), StoreError> {
    let side = ahead_path(path);
    let raw = match std::fs::read(&side) {
        Ok(raw) => raw,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(StoreError::io(&side, e)),
    };
    let images = parse_ahead_pages(&raw)?;
    let mut file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .map_err(|e| StoreError::io(path, e))?;
    for (off, image) in images {
        file.seek(SeekFrom::Start(off))
            .map_err(|e| StoreError::io(path, e))?;
        file.write_all(&image)
            .map_err(|e| StoreError::io(path, e))?;
    }
    file.durable_sync_all()
        .map_err(|e| StoreError::io(path, e))?;
    drop(file);
    std::fs::remove_file(&side).map_err(|e| StoreError::io(&side, e))?;
    if let Some(parent) = side.parent() {
        file::fsync_parent_dir(parent).map_err(|e| StoreError::io(parent, e))?;
    }
    Ok(())
}

impl ScriptHashHead {
    /// Occupied/slots before ingest seals to L0 (`SHSR`).
    pub const SH_SEAL_LOAD: f64 = 0.80;

    pub fn create_with_slots(path: impl Into<PathBuf>, slots: u64) -> Result<Self, StoreError> {
        let slots = slots.max(2).next_power_of_two();
        let file = TableFile::create(path, TableKind::HashHead)?;
        let body_bytes = SH_HEAD_SLOT_SIZE as u64 * slots;
        let need = FILE_HEADER_LEN as u64 + body_bytes;
        file.ensure_capacity(need)?;
        file.set_logical_len(need)?;
        file.zero_range(FILE_HEADER_LEN as u64, body_bytes)?;
        let _ = store_occ_sidecar(file.path(), 0);
        Ok(Self {
            file,
            state: Mutex::new(HashState {
                slots,
                occupied: 0,
                occ_known: true,
            }),
        })
    }

    pub fn open(path: impl Into<PathBuf>) -> Result<Option<Self>, StoreError> {
        let path = path.into();
        if !path.exists() {
            return Ok(None);
        }
        restore_ahead(&path)?;
        let file = TableFile::open(&path, TableKind::HashHead)?;
        let head = Self::from_file(file)?;
        if !head.large_and_empty() {
            return Ok(Some(head));
        }
        let path = head.file.path().to_path_buf();
        drop(head);
        remove_ingest_files(&path)?;
        Ok(None)
    }

    fn large_and_empty(&self) -> bool {
        let st = self.state.lock().unwrap();
        let body = st.slots.saturating_mul(SH_HEAD_SLOT_SIZE as u64);
        st.occ_known && st.occupied == 0 && body > OCC_SCAN_BYTE_CAP
    }

    #[doc(hidden)]
    pub fn from_file(file: TableFile) -> Result<Self, StoreError> {
        let body = file.logical_len().saturating_sub(FILE_HEADER_LEN as u64);
        if !body.is_multiple_of(SH_HEAD_SLOT_SIZE as u64) || body == 0 {
            return Err(StoreError::Corrupt("scripthash head size"));
        }
        let slots = body / SH_HEAD_SLOT_SIZE as u64;
        if !slots.is_power_of_two() {
            return Err(StoreError::Corrupt(
                "scripthash head slots not power of two",
            ));
        }
        let (occupied, occ_known) = if body <= OCC_SCAN_BYTE_CAP {
            // Tiny heads: always walk slots so leftover pack8 Paged refuses on open.
            let occ = scan_occupied(&file, slots)?;
            let _ = store_occ_sidecar(file.path(), occ);
            (occ, true)
        } else if let Some(occ) = load_occ_sidecar(file.path()) {
            // Torn sidecar: the slot table can be durable while `.occ` is still 0.
            if occ == 0 {
                let live = scan_occupied(&file, slots)?;
                if live == 0 {
                    // `open` deletes this file. Rewriting 0 would make the next
                    // start scan the same empty table.
                    (0, true)
                } else {
                    store_occ_sidecar(file.path(), live)?;
                    (live, true)
                }
            } else {
                (occ, true)
            }
        } else {
            // Multi‑GiB mainnet shard without sidecar: do **not** read every slot.
            // Occupancy unknown — never empty, never bulk-fill (see insert path).
            rbitcoin_log::info!(
                "store: scripthash head open skip occupancy scan path={} body_MiB≈{:.0} \
                 (no .occ sidecar; lookups ok, is_empty=false until reinit/install)",
                file.path().display(),
                body as f64 / (1024.0 * 1024.0)
            );
            (0, false)
        };
        Ok(Self {
            file,
            state: Mutex::new(HashState {
                slots,
                occupied,
                occ_known,
            }),
        })
    }

    #[doc(hidden)]
    pub fn persist_occ(&self, occupied: u64) -> Result<(), StoreError> {
        store_occ_sidecar(self.file.path(), occupied)
    }

    #[doc(hidden)]
    pub fn set_occupied_known(&self, occupied: u64) -> Result<(), StoreError> {
        {
            let mut state = self.state.lock().unwrap();
            state.occupied = occupied;
            state.occ_known = true;
        }
        self.persist_occ(occupied)
    }

    #[doc(hidden)]
    pub fn hash_slot(key: &ShHeadKey, slots: u64) -> u64 {
        open_address::primary_slot(key, slots)
    }

    #[doc(hidden)]
    pub fn slot_file_off(slot: u64) -> u64 {
        FILE_HEADER_LEN as u64 + slot * SH_HEAD_SLOT_SIZE as u64
    }

    #[doc(hidden)]
    pub fn to_key(full: &[u8; 32]) -> ShHeadKey {
        head_key_from_full(full)
    }

    pub fn get(&self, full: &[u8; 32]) -> Result<Option<ShHeadValue>, StoreError> {
        Ok(self.get_with_chunk_loads(full)?.0)
    }

    /// Like [`Self::get`], also returns 4 KiB chunk `read_at` count (shared cache size = 1).
    #[doc(hidden)]
    pub fn get_with_chunk_loads(
        &self,
        full: &[u8; 32],
    ) -> Result<(Option<ShHeadValue>, u64), StoreError> {
        let key = Self::to_key(full);
        let slots = self.state.lock().unwrap().slots;
        let mut cache = SlotPageCache::new(self, slots);
        let mut slot = Self::hash_slot(&key, slots);
        for _ in 0..slots {
            let (k, v) = cache.read_slot(slot)?;
            if is_empty_slot(&k, &v) {
                return Ok((None, cache.chunk_loads));
            }
            if k == key {
                let val = unpack8_bytes(&v)?;
                if val.is_empty() {
                    return Ok((None, cache.chunk_loads));
                }
                return Ok((Some(val), cache.chunk_loads));
            }
            slot = (slot + 1) & (slots - 1);
        }
        Ok((None, cache.chunk_loads))
    }

    pub fn insert(&self, full: &[u8; 32], value: &ShHeadValue) -> Result<(), StoreError> {
        self.insert_many(&[(*full, value.clone())])
    }

    /// Byte offset of the pack8 word for an occupied key.
    pub fn value_word_offset(&self, full: &[u8; 32]) -> Result<Option<u64>, StoreError> {
        let key = Self::to_key(full);
        let slots = self.state.lock().unwrap().slots;
        let mut cache = SlotPageCache::new(self, slots);
        let mut slot = Self::hash_slot(&key, slots);
        for _ in 0..slots {
            let (k, v) = cache.read_slot(slot)?;
            if is_empty_slot(&k, &v) {
                return Ok(None);
            }
            if k == key {
                return Ok(Some(
                    FILE_HEADER_LEN as u64
                        + slot * SH_HEAD_SLOT_SIZE as u64
                        + SH_HEAD_KEY_LEN as u64,
                ));
            }
            slot = (slot + 1) & (slots - 1);
        }
        Ok(None)
    }

    pub fn read_value_word(&self, off: u64) -> Result<[u8; 8], StoreError> {
        let mut buf = [0u8; 8];
        self.file.read_at(off, &mut buf)?;
        Ok(buf)
    }

    pub fn write_value_word(&self, off: u64, bytes: &[u8; 8]) -> Result<(), StoreError> {
        self.file.write_at(off, bytes)
    }

    /// Soft-clear value; keeps probe chain.
    pub fn clear_key(&self, full: &[u8; 32]) -> Result<bool, StoreError> {
        let key = Self::to_key(full);
        let slots = self.state.lock().unwrap().slots;
        let mut cache = SlotPageCache::new(self, slots);
        let mut slot = Self::hash_slot(&key, slots);
        for _ in 0..slots {
            let (k, v) = cache.read_slot(slot)?;
            if is_empty_slot(&k, &v) {
                return Ok(false);
            }
            if k == key {
                cache.write_slot(slot, &key, &[0u8; SH_HEAD_VALUE_LEN])?;
                cache.flush_durable()?;
                return Ok(true);
            }
            slot = (slot + 1) & (slots - 1);
        }
        Ok(false)
    }

    pub fn insert_many(&self, entries: &[([u8; 32], ShHeadValue)]) -> Result<(), StoreError> {
        if entries.is_empty() {
            return Ok(());
        }
        let mut upserts = Vec::with_capacity(entries.len());
        for (full, v) in entries {
            if v.is_empty() {
                self.clear_key(full)?;
            } else {
                upserts.push((*full, v.clone()));
            }
        }
        if upserts.is_empty() {
            return Ok(());
        }
        // Bulk-fill only when occupancy is known empty — never when open skipped
        // the scan (would overwrite a live multi‑GiB table). Does not grow.
        {
            let state = self.state.lock().unwrap();
            if state.occ_known && state.occupied == 0 {
                drop(state);
                let keyed: Vec<(ShHeadKey, ShHeadValue)> = upserts
                    .iter()
                    .map(|(full, v)| (Self::to_key(full), v.clone()))
                    .collect();
                return self.bulk_fill_empty(&keyed);
            }
        }
        self.stage_rewrites(&upserts)?;
        self.sync_and_drop_ahead()
    }

    /// Write every row through one page cache. The `*.ahead` image stays until
    /// [`Self::sync_and_drop_ahead`] or [`Self::drop_ahead`] after the file sync.
    #[doc(hidden)]
    pub fn stage_rewrites(&self, rows: &[([u8; 32], ShHeadValue)]) -> Result<(), StoreError> {
        if rows.is_empty() {
            return Ok(());
        }
        let slots_now = self.state.lock().unwrap().slots;
        let cap = slots_now.saturating_mul(MAX_LOAD_NUM) / MAX_LOAD_DEN;
        let mut rows = rows.to_vec();
        rows.sort_unstable_by_key(|(full, _)| Self::hash_slot(&Self::to_key(full), slots_now));
        let mut cache = SlotPageCache::new(self, slots_now);
        for (full, val) in &rows {
            let key = Self::to_key(full);
            if val.is_empty() {
                write_clear(&mut cache, &key)?;
                continue;
            }
            let at_cap = {
                let st = self.state.lock().unwrap();
                st.occ_known && st.occupied >= cap
            };
            if at_cap && self.get(full)?.is_none() {
                cache.flush()?;
                return Err(StoreError::Corrupt(SH_HEAD_FULL));
            }
            let enc = pack8_bytes(val)?;
            match cache.try_insert(&key, &enc, true)? {
                InsertResult::Done(was_empty) => {
                    if was_empty {
                        let mut state = self.state.lock().unwrap();
                        if state.occ_known {
                            if state.occupied >= cap {
                                cache.flush()?;
                                return Err(StoreError::Corrupt(SH_HEAD_FULL));
                            }
                            state.occupied = state.occupied.saturating_add(1);
                        }
                    }
                }
                InsertResult::NeedSlot => {
                    cache.flush()?;
                    return Err(StoreError::Corrupt(SH_HEAD_FULL));
                }
            }
        }
        cache.flush()?;
        let state = self.state.lock().unwrap();
        if state.occ_known {
            let occ = state.occupied;
            drop(state);
            self.persist_occ(occ)?;
        }
        Ok(())
    }

    /// Byte spans [`Self::stage_rewrites`] will write for `keys`, in the same
    /// hash-slot order. The cache is not flushed. A later key displaced by
    /// an earlier one in this batch is included.
    pub(crate) fn upsert_write_spans(
        &self,
        keys: &[[u8; 32]],
    ) -> Result<Vec<(u64, u64)>, StoreError> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        let slots_now = self.state.lock().unwrap().slots;
        let val = ShHeadValue::inline_one(rbitcoin_primitives::Fk(1));
        let enc = pack8_bytes(&val)?;
        let mut order: Vec<&[u8; 32]> = keys.iter().collect();
        order.sort_unstable_by_key(|full| Self::hash_slot(&Self::to_key(full), slots_now));
        let mut cache = SlotPageCache::new(self, slots_now).preview();
        for full in order {
            let key = Self::to_key(full);
            let _ = cache.try_insert(&key, &enc, true)?;
        }
        Ok(cache.dirty_spans())
    }

    /// Sync the head, then drop its `*.ahead` image. No image means nothing to sync.
    #[doc(hidden)]
    pub fn sync_and_drop_ahead(&self) -> Result<(), StoreError> {
        let side = ahead_path(self.file.path());
        match std::fs::metadata(&side) {
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(e) => return Err(StoreError::io(&side, e)),
        }
        self.file.sync_data_only()?;
        self.drop_ahead()
    }

    /// Remove a sidecar the file sync already covered.
    #[doc(hidden)]
    pub fn drop_ahead(&self) -> Result<(), StoreError> {
        let side = ahead_path(self.file.path());
        match std::fs::remove_file(&side) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(e) => return Err(StoreError::io(&side, e)),
        }
        if let Some(parent) = side.parent() {
            file::fsync_parent_dir(parent).map_err(|e| StoreError::io(parent, e))?;
        }
        Ok(())
    }

    /// occupied/slots when occupancy is known (`None` if unknown).
    pub fn load_ratio(&self) -> Option<f64> {
        let state = self.state.lock().unwrap();
        if !state.occ_known || state.slots == 0 {
            return None;
        }
        Some(state.occupied as f64 / state.slots as f64)
    }

    /// How many new keys can land before ingest should seal (0 → seal first).
    pub fn room_before_seal(&self) -> u64 {
        let st = self.state.lock().unwrap();
        if !st.occ_known || st.slots == 0 {
            return 0;
        }
        Self::seal_room(st.slots, st.occupied)
    }

    /// Room in a table that is not on disk yet. That table is known empty.
    #[doc(hidden)]
    pub fn seal_room(slots: u64, occupied: u64) -> u64 {
        if slots == 0 {
            return 0;
        }
        let cap = ((slots as f64) * Self::SH_SEAL_LOAD).ceil() as u64;
        cap.saturating_sub(occupied)
    }

    /// Slot each key takes on an empty table, including a later key displaced
    /// by an earlier one in the same batch.
    #[doc(hidden)]
    pub fn assign_empty_slots(slots: u64, keys: &[ShHeadKey]) -> Result<Vec<u64>, StoreError> {
        if slots == 0 || !slots.is_power_of_two() {
            return Err(StoreError::Corrupt("scripthash head bulk_fill full"));
        }
        let mut owner: BTreeMap<u64, usize> = BTreeMap::new();
        let mut out = Vec::with_capacity(keys.len());
        for (i, key) in keys.iter().enumerate() {
            let mut slot = Self::hash_slot(key, slots);
            let mut placed = false;
            for _ in 0..slots {
                if let Some(prev) = owner.get(&slot).copied() {
                    if &keys[prev] == key {
                        out.push(slot);
                        placed = true;
                        break;
                    }
                } else {
                    owner.insert(slot, i);
                    out.push(slot);
                    placed = true;
                    break;
                }
                slot = (slot + 1) & (slots - 1);
            }
            if !placed {
                return Err(StoreError::Corrupt("scripthash head bulk_fill full"));
            }
        }
        Ok(out)
    }

    #[doc(hidden)]
    pub fn bulk_fill_empty(&self, entries: &[(ShHeadKey, ShHeadValue)]) -> Result<(), StoreError> {
        debug_assert!(self.is_known_empty());
        let slots = self.state.lock().unwrap().slots;
        let keys: Vec<ShHeadKey> = entries.iter().map(|(key, _)| *key).collect();
        let assigned = Self::assign_empty_slots(slots, &keys)?;
        let mut final_slot: BTreeMap<u64, [u8; SH_HEAD_SLOT_SIZE]> = BTreeMap::new();
        for (i, (key, val)) in entries.iter().enumerate() {
            let enc = pack8_bytes(val)?;
            let mut rec = [0u8; SH_HEAD_SLOT_SIZE];
            rec[..SH_HEAD_KEY_LEN].copy_from_slice(key);
            rec[SH_HEAD_KEY_LEN..].copy_from_slice(&enc);
            final_slot.insert(assigned[i], rec);
        }
        for (slot, rec) in &final_slot {
            self.file.write_at(Self::slot_file_off(*slot), rec)?;
        }
        self.set_occupied_known(final_slot.len() as u64)?;
        Ok(())
    }

    pub fn occupied(&self) -> u64 {
        self.state.lock().unwrap().occupied
    }

    /// True when occupancy is known and zero (safe for cold bulk-fill / install).
    pub fn is_known_empty(&self) -> bool {
        let state = self.state.lock().unwrap();
        state.occ_known && state.occupied == 0
    }

    /// Visit every occupied non-empty head value (key is zero-padded to 32 B for API).
    pub fn for_each_occupied(
        &self,
        mut f: impl FnMut([u8; 32], ShHeadValue) -> Result<(), StoreError>,
    ) -> Result<(), StoreError> {
        let slots = self.state.lock().unwrap().slots;
        let mut buf = vec![0u8; SH_HEAD_SLOT_SIZE * 1024];
        let mut slot = 0u64;
        while slot < slots {
            let n = ((slots - slot) as usize).min(1024);
            let off = FILE_HEADER_LEN as u64 + slot * SH_HEAD_SLOT_SIZE as u64;
            let bytes = n * SH_HEAD_SLOT_SIZE;
            self.file.read_at(off, &mut buf[..bytes])?;
            for rec in buf[..bytes].as_chunks::<SH_HEAD_SLOT_SIZE>().0 {
                let k = bytes!(rec, 0, 16);
                let v = bytes!(rec, 16, 8);
                if is_empty_slot(&k, &v) {
                    continue;
                }
                let val = unpack8_bytes(&v)?;
                if !val.is_empty() {
                    let mut full = [0u8; 32];
                    full[0..SH_HEAD_KEY_LEN].copy_from_slice(&k);
                    f(full, val)?;
                }
            }
            slot += n as u64;
        }
        Ok(())
    }

    pub fn flush(&self) -> Result<(), StoreError> {
        self.file.flush()
    }

    /// Sync rewritten slots without the header. A slot table never changes
    /// length, and the header page may sit outside a live mark's pages.
    pub fn sync_slots(&self) -> Result<(), StoreError> {
        self.file.sync_data_only()
    }

    pub fn flush_async(&self) -> Result<(), StoreError> {
        self.file.flush_async()
    }
}

fn write_clear(cache: &mut SlotPageCache<'_>, key: &ShHeadKey) -> Result<bool, StoreError> {
    let mut slot = ScriptHashHead::hash_slot(key, cache.slots);
    for _ in 0..cache.slots {
        let (k, v) = cache.read_slot(slot)?;
        if is_empty_slot(&k, &v) {
            return Ok(false);
        }
        if k == *key {
            cache.write_slot(slot, key, &[0u8; SH_HEAD_VALUE_LEN])?;
            return Ok(true);
        }
        slot = (slot + 1) & (cache.slots - 1);
    }
    Ok(false)
}

fn is_empty_slot(k: &ShHeadKey, v: &[u8; SH_HEAD_VALUE_LEN]) -> bool {
    *k == [0u8; SH_HEAD_KEY_LEN] && *v == [0u8; SH_HEAD_VALUE_LEN]
}

#[doc(hidden)]
pub enum InsertResult {
    /// Wrote the slot. `true` = filled a previously empty slot (new key).
    Done(bool),
    /// Key is not present and a new slot is required (or `allow_new` is false at
    /// the first empty / end of probe). Caller may rehash or spill to overflow.
    NeedSlot,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum ChunkSpill {
    /// A full cache writes the wave, then drops it.
    Durable,
    /// A full cache keeps dirty chunks in RAM and never writes the head.
    Preview,
}

#[doc(hidden)]
pub struct SlotPageCache<'a> {
    pub head: &'a ScriptHashHead,
    pub slots: u64,
    pub chunks: BTreeMap<u64, CachedChunk>,
    /// Number of `read_at` chunk faults (for microbenches / diagnostics).
    pub chunk_loads: u64,
    spill: ChunkSpill,
    spilled: BTreeMap<u64, CachedChunk>,
}

#[doc(hidden)]
pub struct CachedChunk {
    pub base_slot: u64,
    pub data: Vec<u8>,
    pub dirty: bool,
}

impl<'a> SlotPageCache<'a> {
    #[doc(hidden)]
    pub fn new(head: &'a ScriptHashHead, slots: u64) -> Self {
        Self {
            head,
            slots,
            chunks: BTreeMap::new(),
            chunk_loads: 0,
            spill: ChunkSpill::Durable,
            spilled: BTreeMap::new(),
        }
    }

    /// Plan a guard image. Dirty chunks stay in RAM, including past one wave.
    pub fn preview(mut self) -> Self {
        self.spill = ChunkSpill::Preview;
        self
    }

    fn dirty_spans(&self) -> Vec<(u64, u64)> {
        let mut spans = Vec::new();
        for chunk in self
            .chunks
            .values()
            .chain(self.spilled.values())
            .filter(|chunk| chunk.dirty)
        {
            spans.push((
                ScriptHashHead::slot_file_off(chunk.base_slot),
                chunk.data.len() as u64,
            ));
        }
        spans
    }

    fn make_preview_room(&mut self) {
        if self.chunks.len() < CHUNK_CACHE_MAX {
            return;
        }
        let taken = std::mem::take(&mut self.chunks);
        for (idx, chunk) in taken {
            if chunk.dirty {
                self.spilled.insert(idx, chunk);
            }
        }
    }

    fn load_chunk(&mut self, chunk_idx: u64) -> Result<(), StoreError> {
        if self.spill == ChunkSpill::Preview {
            if let Some(chunk) = self.spilled.remove(&chunk_idx) {
                self.make_preview_room();
                self.chunks.insert(chunk_idx, chunk);
                return Ok(());
            }
            self.make_preview_room();
        } else if self.chunks.len() >= CHUNK_CACHE_MAX {
            // This wave is durable before the next one starts, so a later
            // flush does not rewrite pages the sidecar already retired.
            self.flush_durable()?;
        }
        let base_slot = chunk_idx * SLOTS_PER_CHUNK;
        let n = ((self.slots - base_slot) as usize).min(SLOTS_PER_CHUNK as usize);
        let off = ScriptHashHead::slot_file_off(base_slot);
        let len = n * SH_HEAD_SLOT_SIZE;
        let mut data = vec![0u8; len];
        self.head.file.read_at(off, &mut data)?;
        self.chunk_loads = self.chunk_loads.saturating_add(1);
        self.chunks.insert(
            chunk_idx,
            CachedChunk {
                base_slot,
                data,
                dirty: false,
            },
        );
        Ok(())
    }

    /// Probe-insert. When `allow_new` is false, only in-place updates of an
    /// existing key succeed; empty slot (key absent under open addressing) or a
    /// full probe without a match returns [`InsertResult::NeedSlot`].
    #[doc(hidden)]
    pub fn try_insert(
        &mut self,
        key: &ShHeadKey,
        value: &[u8; SH_HEAD_VALUE_LEN],
        allow_new: bool,
    ) -> Result<InsertResult, StoreError> {
        let mut slot = ScriptHashHead::hash_slot(key, self.slots);
        for _ in 0..self.slots {
            let (k, old_v) = self.read_slot(slot)?;
            if is_empty_slot(&k, &old_v) {
                if !allow_new {
                    // Empty ends the open-address probe: key is not present.
                    return Ok(InsertResult::NeedSlot);
                }
                self.write_slot(slot, key, value)?;
                return Ok(InsertResult::Done(true));
            }
            if &k == key {
                self.write_slot(slot, key, value)?;
                return Ok(InsertResult::Done(false));
            }
            slot = (slot + 1) & (self.slots - 1);
        }
        Ok(InsertResult::NeedSlot)
    }

    #[doc(hidden)]
    pub fn read_slot(
        &mut self,
        slot: u64,
    ) -> Result<(ShHeadKey, [u8; SH_HEAD_VALUE_LEN]), StoreError> {
        let chunk = self.ensure_chunk(slot)?;
        let rel = ((slot - chunk.base_slot) as usize) * SH_HEAD_SLOT_SIZE;
        let rec = sh_slot_at(&chunk.data, rel);
        Ok((bytes!(rec, 0, 16), bytes!(rec, 16, 8)))
    }

    #[doc(hidden)]
    pub fn write_slot(
        &mut self,
        slot: u64,
        key: &ShHeadKey,
        value: &[u8; SH_HEAD_VALUE_LEN],
    ) -> Result<(), StoreError> {
        let chunk = self.ensure_chunk(slot)?;
        let rel = ((slot - chunk.base_slot) as usize) * SH_HEAD_SLOT_SIZE;
        chunk.data[rel..rel + SH_HEAD_KEY_LEN].copy_from_slice(key);
        chunk.data[rel + SH_HEAD_KEY_LEN..rel + SH_HEAD_SLOT_SIZE].copy_from_slice(value);
        chunk.dirty = true;
        Ok(())
    }

    #[doc(hidden)]
    pub fn ensure_chunk(&mut self, slot: u64) -> Result<&mut CachedChunk, StoreError> {
        let chunk_idx = slot / SLOTS_PER_CHUNK;
        if !self.chunks.contains_key(&chunk_idx) {
            self.load_chunk(chunk_idx)?;
        }
        Ok(self.chunks.get_mut(&chunk_idx).unwrap())
    }

    #[doc(hidden)]
    pub fn flush(&mut self) -> Result<(), StoreError> {
        let mut blocks: BTreeMap<u64, [u8; 4096]> = BTreeMap::new();
        for chunk in self.chunks.values().filter(|chunk| chunk.dirty) {
            let off = ScriptHashHead::slot_file_off(chunk.base_slot);
            let len = chunk.data.len() as u64;
            let first = off & !4095;
            let last = (off + len - 1) & !4095;
            let mut block = first;
            while block <= last {
                if let std::collections::btree_map::Entry::Vacant(entry) = blocks.entry(block) {
                    entry.insert(read_page(self.head.file.path(), &self.head.file, block)?);
                }
                let Some(image) = blocks.get_mut(&block) else {
                    block += 4096;
                    continue;
                };
                let lo = off.max(block);
                let hi = (off + len).min(block + 4096);
                let src = (lo - off) as usize;
                let dst = (lo - block) as usize;
                image[dst..dst + (hi - lo) as usize]
                    .copy_from_slice(&chunk.data[src..src + (hi - lo) as usize]);
                block += 4096;
            }
        }
        if !blocks.is_empty() {
            // A leftover sidecar from a failed write keeps pages this write
            // does not replace; new images win on pages they cover.
            let mut merged = load_ahead_pages(self.head.file.path())?;
            merged.extend(blocks);
            let buf = encode_ahead_pages(&merged);
            let side = ahead_path(self.head.file.path());
            file::write_synced_tmp_rename(&side, &buf)?;
            #[cfg(debug_assertions)]
            if take_fail_ahead(self.head.file.path()) {
                return Err(StoreError::io(
                    self.head.file.path(),
                    std::io::Error::other("injected scripthash head ahead failure"),
                ));
            }
        }
        let mut writes = 0u32;
        for (_, chunk) in self.chunks.iter_mut() {
            if chunk.dirty {
                let off = ScriptHashHead::slot_file_off(chunk.base_slot);
                self.head.file.write_at(off, &chunk.data)?;
                writes = writes.saturating_add(1);
                #[cfg(debug_assertions)]
                if writes >= 2 && take_fail_second_write(self.head.file.path()) {
                    return Err(StoreError::io(
                        self.head.file.path(),
                        std::io::Error::other("injected scripthash head second-write failure"),
                    ));
                }
            }
        }
        self.chunks.clear();
        Ok(())
    }

    /// [`Self::flush`] plus the head sync that drops the sidecar.
    #[doc(hidden)]
    pub fn flush_durable(&mut self) -> Result<(), StoreError> {
        self.flush()?;
        self.head.sync_and_drop_ahead()
    }
}
