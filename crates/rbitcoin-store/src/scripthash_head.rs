//! Open-addressed head for hybrid scripthash: key[16] → value[16] (32 B slots).
//!
//! Key is the first 16 bytes of Electrum SHA256(spk). Public APIs take full 32 B
//! hashes and truncate. Values are [`ShHeadValue`] encodings (two u64s).
//!
//! # Occupancy (startup)
//!
//! Slot occupancy is process state for load-factor / `is_empty`, **not** needed for
//! probe lookups. Mainnet shards are multi‑GiB — a full slot scan on every open is
//! unacceptable. Durable sidecar `{shard}.occ` holds the count (written on create,
//! reinit, cold install, rehash, and inserts). Large shards without a sidecar skip
//! the scan and mark occupancy **unknown** (never treated as empty, never bulk-fill).

use crate::error::StoreError;
use crate::file::{TableFile, FILE_HEADER_LEN};
use crate::hashhead::HeadScale;
#[cfg(test)]
use crate::hashhead::{initial_slots_for, HeadRole};
use crate::scripthash_layout::{
    head_key_from_full, pack8_bytes, unpack8_bytes, ShHeadKey, ShHeadValue, SH_HEAD_KEY_LEN,
    SH_HEAD_SLOT_SIZE, SH_HEAD_VALUE_LEN,
};
#[cfg(test)]
use crate::sharded_hashhead::initial_slots_per_shard;
#[cfg(test)]
use crate::sharded_hashhead::shard_count_for_role;
use rbitcoin_primitives::TableKind;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::Instant;

use crate::open_address::{self, MAX_LOAD_DEN, MAX_LOAD_NUM};

/// Full occupancy scan only for tiny head files (tests / early IBD).
const OCC_SCAN_BYTE_CAP: u64 = 16 * 1024 * 1024;
const OCC_MAGIC: &[u8; 8] = b"SHOCC001";

const DEFAULT_SLOTS: u64 = 64;
const SLOTS_PER_CHUNK: u64 = 128;
const CHUNK_CACHE_MAX: usize = 256;

/// Corrupt message when on-disk SH head shard count ≠ current layout (e.g. 16-way vs 64-way).
#[cfg(test)]
pub const SH_HEAD_SHARD_COUNT_MISMATCH: &str =
    "scripthash head shard count mismatch (reindex; expected 64-way mainnet layout)";

/// Open-address slot count for `keys` unique entries at 7/8 max load (pow2).
#[inline]
pub fn sh_slots_for_keys(keys: u64) -> u64 {
    if keys == 0 {
        return DEFAULT_SLOTS;
    }
    let min = keys
        .saturating_mul(MAX_LOAD_DEN)
        .div_ceil(MAX_LOAD_NUM)
        .max(1);
    min.next_power_of_two().max(DEFAULT_SLOTS)
}

/// Default unique-key hint for cold live OA pre-size (mainnet ~2e9).
///
/// Override with `RBITCOIN_SH_UNIQUE_HINT`. Tiny/test scale uses a small default
/// so unit tests do not allocate multi-GiB tables.
pub fn sh_unique_hint_default() -> u64 {
    if let Ok(s) = std::env::var("RBITCOIN_SH_UNIQUE_HINT") {
        if let Ok(n) = s.parse::<u64>() {
            return n.max(1);
        }
    }
    match HeadScale::from_env() {
        HeadScale::Tiny => 4_096,
        HeadScale::Mainnet => 2_000_000_000,
    }
}

/// Per-shard key capacity from a global unique hint (25% skew margin).
#[inline]
pub fn sh_per_shard_key_budget(unique_hint: u64, n_shards: usize) -> u64 {
    let n = n_shards.max(1) as u64;
    unique_hint
        .max(1)
        .div_ceil(n)
        .saturating_mul(5)
        .div_ceil(4)
        .max(1)
}

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
    file: TableFile,
    state: Mutex<HashState>,
}

struct HashState {
    slots: u64,
    occupied: u64,
    /// When false, `occupied` is not authoritative (large open without `.occ`).
    /// Never treat as empty / never bulk-fill from this state.
    occ_known: bool,
}

fn occ_sidecar_path(head_path: &Path) -> PathBuf {
    let mut p = head_path.as_os_str().to_os_string();
    p.push(".occ");
    PathBuf::from(p)
}

fn load_occ_sidecar(head_path: &Path) -> Option<u64> {
    let p = occ_sidecar_path(head_path);
    let Ok(buf) = std::fs::read(&p) else {
        return None;
    };
    if buf.len() < 16 || &buf[0..8] != OCC_MAGIC {
        return None;
    }
    Some(u64::from_le_bytes(buf[8..16].try_into().ok()?))
}

fn store_occ_sidecar(head_path: &Path, occupied: u64) -> Result<(), StoreError> {
    let p = occ_sidecar_path(head_path);
    let tmp = {
        let mut t = p.as_os_str().to_os_string();
        t.push(".tmp");
        PathBuf::from(t)
    };
    let mut buf = [0u8; 16];
    buf[0..8].copy_from_slice(OCC_MAGIC);
    buf[8..16].copy_from_slice(&occupied.to_le_bytes());
    if let Some(parent) = p.parent() {
        let _ = std::fs::create_dir_all(parent);
    }
    std::fs::write(&tmp, buf).map_err(|e| StoreError::io(&tmp, e))?;
    {
        let f = std::fs::OpenOptions::new()
            .write(true)
            .open(&tmp)
            .map_err(|e| StoreError::io(&tmp, e))?;
        f.sync_all().map_err(|e| StoreError::io(&tmp, e))?;
    }
    std::fs::rename(&tmp, &p).map_err(|e| StoreError::io(&p, e))?;
    Ok(())
}

fn scan_occupied(file: &TableFile, slots: u64) -> Result<u64, StoreError> {
    let mut occupied = 0u64;
    let mut buf = vec![0u8; SH_HEAD_SLOT_SIZE * 1024];
    let mut slot = 0u64;
    while slot < slots {
        let n = ((slots - slot) as usize).min(1024);
        let off = FILE_HEADER_LEN as u64 + slot * SH_HEAD_SLOT_SIZE as u64;
        let bytes = n * SH_HEAD_SLOT_SIZE;
        file.read_at(off, &mut buf[..bytes])?;
        for i in 0..n {
            let base = i * SH_HEAD_SLOT_SIZE;
            let k: ShHeadKey = buf[base..base + SH_HEAD_KEY_LEN].try_into().unwrap();
            let v: [u8; SH_HEAD_VALUE_LEN] = buf[base + SH_HEAD_KEY_LEN..base + SH_HEAD_SLOT_SIZE]
                .try_into()
                .unwrap();
            if !is_empty_slot(&k, &v) {
                occupied += 1;
            }
        }
        slot += n as u64;
    }
    Ok(occupied)
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

    pub fn open(path: impl Into<PathBuf>) -> Result<Self, StoreError> {
        let file = TableFile::open(path, TableKind::HashHead)?;
        Self::from_file(file)
    }

    fn from_file(file: TableFile) -> Result<Self, StoreError> {
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
        let (occupied, occ_known) = if let Some(occ) = load_occ_sidecar(file.path()) {
            (occ, true)
        } else if body <= OCC_SCAN_BYTE_CAP {
            // Tiny heads (tests / early scale): scan once and seal sidecar.
            let occ = scan_occupied(&file, slots)?;
            let _ = store_occ_sidecar(file.path(), occ);
            (occ, true)
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

    fn persist_occ(&self, occupied: u64) {
        let _ = store_occ_sidecar(self.file.path(), occupied);
    }

    fn set_occupied_known(&self, occupied: u64) {
        {
            let mut state = self.state.lock().unwrap();
            state.occupied = occupied;
            state.occ_known = true;
        }
        self.persist_occ(occupied);
    }

    fn hash_slot(key: &ShHeadKey, slots: u64) -> u64 {
        open_address::primary_slot(key, slots)
    }

    fn slot_file_off(slot: u64) -> u64 {
        FILE_HEADER_LEN as u64 + slot * SH_HEAD_SLOT_SIZE as u64
    }

    fn to_key(full: &[u8; 32]) -> ShHeadKey {
        head_key_from_full(full)
    }

    /// Zero all slots and reset occupied (cold rematerialize after partial load).
    ///
    /// Chunked `write_at` so FdOnly payload views match
    /// `occupied = 0` (not punch-hole alone).
    pub fn reinit_empty(&self) -> Result<(), StoreError> {
        let slots = {
            let state = self.state.lock().unwrap();
            state.slots
        };
        let body_bytes = SH_HEAD_SLOT_SIZE as u64 * slots;
        let zero = vec![
            0u8;
            (1024 * 1024)
                .min(body_bytes as usize)
                .max(SH_HEAD_SLOT_SIZE)
        ];
        let mut off = 0u64;
        while off < body_bytes {
            let n = ((body_bytes - off) as usize).min(zero.len());
            self.file
                .write_at(FILE_HEADER_LEN as u64 + off, &zero[..n])?;
            off += n as u64;
        }
        self.set_occupied_known(0);
        Ok(())
    }

    pub fn get(&self, full: &[u8; 32]) -> Result<Option<ShHeadValue>, StoreError> {
        Ok(self.get_with_chunk_loads(full)?.0)
    }

    /// Like [`Self::get`], also returns 4 KiB chunk `read_at` count (shared cache size = 1).
    fn get_with_chunk_loads(
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

    /// Batch head probe for tip SH seed (one `SlotPageCache` pass, keys sorted by slot).
    ///
    /// Faster than N independent [`Self::get`] calls when seeding thousands of
    /// unique scripts for `put_create_batch_append` (shared 4 KiB page cache).
    ///
    /// Slot + truncated key are precomputed once (sort keys are not re-hashed on
    /// every comparison); probes walk in primary-slot order so adjacent keys share
    /// the same 4 KiB chunk under [`SlotPageCache`].
    pub fn get_many(&self, fulls: &[[u8; 32]]) -> Result<Vec<Option<ShHeadValue>>, StoreError> {
        Ok(self.get_many_with_chunk_loads(fulls)?.0)
    }

    /// Like [`Self::get_many`], also returns total 4 KiB chunk `read_at` faults.
    fn get_many_with_chunk_loads(
        &self,
        fulls: &[[u8; 32]],
    ) -> Result<(Vec<Option<ShHeadValue>>, u64), StoreError> {
        if fulls.is_empty() {
            return Ok((Vec::new(), 0));
        }
        let slots = self.state.lock().unwrap().slots;
        let mut order: Vec<(u64, usize, ShHeadKey)> = fulls
            .iter()
            .enumerate()
            .map(|(i, full)| {
                let key = Self::to_key(full);
                let slot = Self::hash_slot(&key, slots);
                (slot, i, key)
            })
            .collect();
        order.sort_unstable_by_key(|&(slot, _, _)| slot);
        let mut out = vec![None; fulls.len()];
        let mut cache = SlotPageCache::new(self, slots);
        for &(primary, i, key) in &order {
            let mut slot = primary;
            for _ in 0..slots {
                let (k, v) = cache.read_slot(slot)?;
                if is_empty_slot(&k, &v) {
                    break;
                }
                if k == key {
                    let val = unpack8_bytes(&v)?;
                    if !val.is_empty() {
                        out[i] = Some(val);
                    }
                    break;
                }
                slot = (slot + 1) & (slots - 1);
            }
        }
        Ok((out, cache.chunk_loads))
    }

    pub fn insert(&self, full: &[u8; 32], value: &ShHeadValue) -> Result<(), StoreError> {
        self.insert_many(&[(*full, value.clone())])
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
                cache.flush()?;
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
        let mut upserts: Vec<(ShHeadKey, ShHeadValue)> = Vec::with_capacity(entries.len());
        for (full, v) in entries {
            let key = Self::to_key(full);
            if v.is_empty() {
                self.clear_key(full)?;
            } else {
                upserts.push((key, v.clone()));
            }
        }
        if upserts.is_empty() {
            return Ok(());
        }
        self.reserve_additional(upserts.len() as u64)?;

        // Bulk-fill only when occupancy is known empty — never when open skipped
        // the scan (would overwrite a live multi‑GiB table).
        {
            let state = self.state.lock().unwrap();
            if state.occ_known && state.occupied == 0 {
                drop(state);
                return self.bulk_fill_empty(&upserts);
            }
        }

        let mut work = upserts;
        let slots_now = self.state.lock().unwrap().slots;
        work.sort_unstable_by_key(|(k, _)| Self::hash_slot(k, slots_now));

        let mut i = 0usize;
        while i < work.len() {
            let slots = self.state.lock().unwrap().slots;
            if i > 0 {
                work[i..].sort_unstable_by_key(|(k, _)| Self::hash_slot(k, slots));
            }
            let mut cache = SlotPageCache::new(self, slots);
            let mut need_rehash = false;
            while i < work.len() {
                let (key, ref val) = work[i];
                let enc = pack8_bytes(val)?;
                match cache.try_insert(&key, &enc, true)? {
                    InsertResult::Done(was_empty) => {
                        if was_empty {
                            let mut state = self.state.lock().unwrap();
                            // Only bump when known; unknown open stays unknown (is_empty
                            // false, no bulk-fill) rather than inventing a wrong count.
                            if state.occ_known {
                                state.occupied = state.occupied.saturating_add(1);
                            }
                        }
                        i += 1;
                    }
                    InsertResult::NeedSlot => {
                        need_rehash = true;
                        break;
                    }
                }
            }
            cache.flush()?;
            if need_rehash {
                let (slots, occupied, occ_known) = {
                    let state = self.state.lock().unwrap();
                    (state.slots, state.occupied, state.occ_known)
                };
                // NeedSlot with unknown occupancy: double slots via rehash scan.
                let remain = (work.len() - i) as u64;
                let need = if occ_known {
                    Self::slots_for_keys(occupied.saturating_add(remain))
                        .max(slots.saturating_mul(2))
                } else {
                    slots.saturating_mul(2).max(Self::slots_for_keys(remain))
                };
                self.rehash_to(need)?;
            }
        }
        // Seal sidecar after batch when count is authoritative.
        {
            let state = self.state.lock().unwrap();
            if state.occ_known {
                let occ = state.occupied;
                drop(state);
                self.persist_occ(occ);
            }
        }
        Ok(())
    }

    /// Full-key upsert without rehash (maps to 16 B head keys; remaps remainder).
    pub fn insert_many_full_no_rehash(
        &self,
        entries: &[([u8; 32], ShHeadValue)],
        allow_new: bool,
    ) -> Result<Vec<([u8; 32], ShHeadValue)>, StoreError> {
        if entries.is_empty() {
            return Ok(Vec::new());
        }
        let mapped: Vec<_> = entries
            .iter()
            .map(|(k, v)| (head_key_from_full(k), v.clone()))
            .collect();
        let rem = self.insert_many_no_rehash(&mapped, allow_new)?;
        if rem.is_empty() {
            return Ok(Vec::new());
        }
        let mut out = Vec::with_capacity(rem.len());
        for (hk, hv) in rem {
            if let Some((full, _)) = entries.iter().find(|(f, _)| head_key_from_full(f) == hk) {
                out.push((*full, hv));
            } else {
                let mut full = [0u8; 32];
                full[..SH_HEAD_KEY_LEN].copy_from_slice(&hk);
                out.push((full, hv));
            }
        }
        Ok(out)
    }

    /// Like [`insert_many`] but **never rehashes**.
    ///
    /// `allow_new`: when true, new keys may occupy empty slots until the first
    /// miss, then the batch continues **update-only**. When false (sealed main
    /// try-upsert), **only** in-place updates apply — not-present keys always
    /// go to the returned remainder (even if free slots exist).
    ///
    /// Caller routes remainder to overflow. Empty remainder = full batch on this head.
    pub fn insert_many_no_rehash(
        &self,
        upserts: &[(ShHeadKey, ShHeadValue)],
        allow_new: bool,
    ) -> Result<Vec<(ShHeadKey, ShHeadValue)>, StoreError> {
        if upserts.is_empty() {
            return Ok(Vec::new());
        }
        if allow_new && self.is_known_empty() {
            let slots = self.state.lock().unwrap().slots;
            let max_fit = slots
                .saturating_mul(MAX_LOAD_NUM)
                .checked_div(MAX_LOAD_DEN)
                .unwrap_or(slots);
            if (upserts.len() as u64) <= max_fit && (upserts.len() as u64) <= slots {
                self.bulk_fill_empty(upserts)?;
                return Ok(Vec::new());
            }
        }
        if !allow_new && self.is_known_empty() {
            return Ok(upserts.to_vec());
        }

        let mut work: Vec<(ShHeadKey, ShHeadValue, usize)> = upserts
            .iter()
            .cloned()
            .enumerate()
            .map(|(i, (k, v))| (k, v, i))
            .collect();
        let slots_now = self.state.lock().unwrap().slots;
        work.sort_unstable_by_key(|(k, _, _)| Self::hash_slot(k, slots_now));

        let mut remainder: Vec<(ShHeadKey, ShHeadValue)> = Vec::new();
        let mut allow_new = allow_new;
        let mut i = 0usize;
        while i < work.len() {
            let slots = self.state.lock().unwrap().slots;
            if i > 0 {
                work[i..].sort_unstable_by_key(|(k, _, _)| Self::hash_slot(k, slots));
            }
            let mut cache = SlotPageCache::new(self, slots);
            while i < work.len() {
                let (key, ref val, _) = work[i];
                let enc = pack8_bytes(val)?;
                match cache.try_insert(&key, &enc, allow_new)? {
                    InsertResult::Done(was_empty) => {
                        if was_empty {
                            let mut state = self.state.lock().unwrap();
                            if state.occ_known {
                                state.occupied = state.occupied.saturating_add(1);
                            }
                        }
                        i += 1;
                    }
                    InsertResult::NeedSlot => {
                        allow_new = false;
                        remainder.push((key, val.clone()));
                        i += 1;
                    }
                }
            }
            cache.flush()?;
        }
        {
            let state = self.state.lock().unwrap();
            if state.occ_known {
                let occ = state.occupied;
                drop(state);
                self.persist_occ(occ);
            }
        }
        Ok(remainder)
    }

    /// occupied/slots when occupancy is known (`None` if unknown).
    pub fn load_ratio(&self) -> Option<f64> {
        let state = self.state.lock().unwrap();
        if !state.occ_known || state.slots == 0 {
            return None;
        }
        Some(state.occupied as f64 / state.slots as f64)
    }

    fn bulk_fill_empty(&self, entries: &[(ShHeadKey, ShHeadValue)]) -> Result<(), StoreError> {
        debug_assert!(self.is_known_empty());
        let slots = self.state.lock().unwrap().slots;
        let nbytes = (slots as usize).saturating_mul(SH_HEAD_SLOT_SIZE);
        let mut table = vec![0u8; nbytes];
        let mut occupied = 0u64;
        for (key, val) in entries {
            let enc = pack8_bytes(val)?;
            let mut slot = Self::hash_slot(key, slots);
            let mut placed = false;
            for _ in 0..slots {
                let off = (slot as usize) * SH_HEAD_SLOT_SIZE;
                let slot_key: ShHeadKey = table[off..off + SH_HEAD_KEY_LEN].try_into().unwrap();
                let slot_v: [u8; SH_HEAD_VALUE_LEN] = table
                    [off + SH_HEAD_KEY_LEN..off + SH_HEAD_SLOT_SIZE]
                    .try_into()
                    .unwrap();
                if is_empty_slot(&slot_key, &slot_v) {
                    table[off..off + SH_HEAD_KEY_LEN].copy_from_slice(key);
                    table[off + SH_HEAD_KEY_LEN..off + SH_HEAD_SLOT_SIZE].copy_from_slice(&enc);
                    occupied = occupied.saturating_add(1);
                    placed = true;
                    break;
                }
                if &slot_key == key {
                    table[off + SH_HEAD_KEY_LEN..off + SH_HEAD_SLOT_SIZE].copy_from_slice(&enc);
                    placed = true;
                    break;
                }
                slot = (slot + 1) & (slots - 1);
            }
            if !placed {
                return Err(StoreError::Corrupt("scripthash head bulk_fill full"));
            }
        }
        self.file.write_at(FILE_HEADER_LEN as u64, &table)?;
        self.set_occupied_known(occupied);
        Ok(())
    }

    fn slots_for_keys(keys: u64) -> u64 {
        sh_slots_for_keys(keys)
    }

    pub fn occupied(&self) -> u64 {
        self.state.lock().unwrap().occupied
    }

    /// Slot capacity of this head file (power of two).
    pub fn slots(&self) -> u64 {
        self.state.lock().unwrap().slots
    }

    /// True when occupancy is known and zero (safe for cold bulk-fill / install).
    pub fn is_known_empty(&self) -> bool {
        let state = self.state.lock().unwrap();
        state.occ_known && state.occupied == 0
    }

    pub fn reserve_additional(&self, additional: u64) -> Result<(), StoreError> {
        if additional == 0 {
            return Ok(());
        }
        let (occupied, slots, occ_known) = {
            let state = self.state.lock().unwrap();
            (state.occupied, state.slots, state.occ_known)
        };
        // Unknown occupancy: do not grow/rehash from a fake zero count (mainnet
        // tables are pre-sized; warm residual fits). Probe insert handles load.
        if !occ_known {
            return Ok(());
        }
        let need = Self::slots_for_keys(occupied.saturating_add(additional));
        if need > slots {
            if occupied == 0 {
                // Cold bulk: grow empty table without scanning zeros.
                self.grow_empty_to(need)?;
            } else {
                self.rehash_to(need)?;
            }
        }
        Ok(())
    }

    /// Install a pre-built cold slot image (live in-RAM fill). Requires empty occupied.
    pub fn install_cold_image(
        &self,
        table: &[u8],
        slots: u64,
        occupied: u64,
    ) -> Result<(), StoreError> {
        let slots = slots.max(2).next_power_of_two();
        let need_bytes = (slots as usize).saturating_mul(SH_HEAD_SLOT_SIZE);
        if table.len() != need_bytes {
            return Err(StoreError::Corrupt(
                "scripthash install_cold_image: table len mismatch",
            ));
        }
        if !self.is_known_empty() {
            return Err(StoreError::Corrupt(
                "scripthash install_cold_image: not empty",
            ));
        }
        let new_bytes = need_bytes as u64;
        let need = FILE_HEADER_LEN as u64 + new_bytes;
        self.file.ensure_capacity(need)?;
        self.file.set_logical_len(need)?;
        self.file.write_at(FILE_HEADER_LEN as u64, table)?;
        {
            let mut state = self.state.lock().unwrap();
            state.slots = slots;
        }
        self.set_occupied_known(occupied);
        Ok(())
    }

    /// Best-effort drop of head pages from page cache after cold install.
    pub fn advise_dont_need_all(&self) {
        let len = self.file.logical_len();
        if len > FILE_HEADER_LEN as u64 {
            self.file
                .advise_dont_need(FILE_HEADER_LEN as u64, len - FILE_HEADER_LEN as u64);
        }
    }

    /// Expand an **empty** open-address table to `new_slots` (power of two).
    ///
    /// Used by cold materialize so pre-size is fallocate/zero only — no slot scan.
    fn grow_empty_to(&self, new_slots: u64) -> Result<(), StoreError> {
        let new_slots = new_slots.max(2).next_power_of_two();
        let (old_slots, occupied) = {
            let state = self.state.lock().unwrap();
            (state.slots, state.occupied)
        };
        if occupied != 0 {
            return self.rehash_to(new_slots);
        }
        if new_slots <= old_slots {
            return Ok(());
        }
        let new_bytes = SH_HEAD_SLOT_SIZE as u64 * new_slots;
        let need = FILE_HEADER_LEN as u64 + new_bytes;
        self.file.ensure_capacity(need)?;
        self.file.set_logical_len(need)?;
        // Zero full body (new region may reuse stale bytes past old logical len).
        self.file.zero_range(FILE_HEADER_LEN as u64, new_bytes)?;
        self.state.lock().unwrap().slots = new_slots;
        Ok(())
    }

    fn rehash_to(&self, new_slots: u64) -> Result<(), StoreError> {
        let new_slots = new_slots.max(2).next_power_of_two();
        // Share process-wide gate with HashHead (no stacked multi-table freezes).
        let _rehash_serial = open_address::rehash_gate()
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let (old_slots, occupied) = {
            let state = self.state.lock().unwrap();
            (state.slots, state.occupied)
        };
        if new_slots <= old_slots {
            return Ok(());
        }
        let new_bytes = SH_HEAD_SLOT_SIZE as u64 * new_slots;
        let t0 = Instant::now();

        let mut entries: Vec<(ShHeadKey, [u8; SH_HEAD_VALUE_LEN])> = Vec::new();
        entries
            .try_reserve_exact(occupied as usize)
            .map_err(|_| StoreError::Corrupt("scripthash head rehash OOM"))?;
        let mut buf = vec![0u8; SH_HEAD_SLOT_SIZE * 1024];
        let mut slot = 0u64;
        while slot < old_slots {
            let n = ((old_slots - slot) as usize).min(1024);
            let off = FILE_HEADER_LEN as u64 + slot * SH_HEAD_SLOT_SIZE as u64;
            let bytes = n * SH_HEAD_SLOT_SIZE;
            self.file.read_at(off, &mut buf[..bytes])?;
            for i in 0..n {
                let base = i * SH_HEAD_SLOT_SIZE;
                let k: ShHeadKey = buf[base..base + SH_HEAD_KEY_LEN].try_into().unwrap();
                let v: [u8; SH_HEAD_VALUE_LEN] = buf
                    [base + SH_HEAD_KEY_LEN..base + SH_HEAD_SLOT_SIZE]
                    .try_into()
                    .unwrap();
                if !is_empty_slot(&k, &v) {
                    entries.push((k, v));
                }
            }
            slot += n as u64;
        }

        let need = FILE_HEADER_LEN as u64 + new_bytes;
        self.file.ensure_capacity(need)?;
        self.file.set_logical_len(need)?;
        self.file.zero_range(FILE_HEADER_LEN as u64, new_bytes)?;
        {
            let mut state = self.state.lock().unwrap();
            state.slots = new_slots;
            state.occupied = 0;
            // Mid-rehash: authoritative empty of the *new* image until refill.
            state.occ_known = true;
        }

        entries.sort_unstable_by_key(|(k, _)| Self::hash_slot(k, new_slots));
        let n_entries = entries.len() as u64;
        let mut cache = SlotPageCache::new(self, new_slots);
        for (k, v) in &entries {
            match cache.try_insert(k, v, true)? {
                InsertResult::Done(_) => {}
                InsertResult::NeedSlot => {
                    cache.flush()?;
                    return Err(StoreError::Corrupt("scripthash head rehash failed"));
                }
            }
        }
        cache.flush()?;
        // Rehash walks every old slot — count is authoritative even if open was unknown.
        self.set_occupied_known(n_entries);
        rbitcoin_log::trace!(
            "store: scripthash head rehash path={} {}→{} slots occupied={} elapsed={:?}",
            self.file.path().display(),
            old_slots,
            new_slots,
            n_entries,
            t0.elapsed()
        );
        Ok(())
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
            for i in 0..n {
                let base = i * SH_HEAD_SLOT_SIZE;
                let k: ShHeadKey = buf[base..base + SH_HEAD_KEY_LEN].try_into().unwrap();
                let v: [u8; SH_HEAD_VALUE_LEN] = buf
                    [base + SH_HEAD_KEY_LEN..base + SH_HEAD_SLOT_SIZE]
                    .try_into()
                    .unwrap();
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

    pub fn flush_async(&self) -> Result<(), StoreError> {
        self.file.flush_async()
    }
}

fn is_empty_slot(k: &ShHeadKey, v: &[u8; SH_HEAD_VALUE_LEN]) -> bool {
    *k == [0u8; SH_HEAD_KEY_LEN] && *v == [0u8; SH_HEAD_VALUE_LEN]
}

enum InsertResult {
    /// Wrote the slot. `true` = filled a previously empty slot (new key).
    Done(bool),
    /// Key is not present and a new slot is required (or `allow_new` is false at
    /// the first empty / end of probe). Caller may rehash or spill to overflow.
    NeedSlot,
}

struct SlotPageCache<'a> {
    head: &'a ScriptHashHead,
    slots: u64,
    chunks: BTreeMap<u64, CachedChunk>,
    /// Number of `read_at` chunk faults (for microbenches / diagnostics).
    chunk_loads: u64,
}

struct CachedChunk {
    base_slot: u64,
    data: Vec<u8>,
    dirty: bool,
}

impl<'a> SlotPageCache<'a> {
    fn new(head: &'a ScriptHashHead, slots: u64) -> Self {
        Self {
            head,
            slots,
            chunks: BTreeMap::new(),
            chunk_loads: 0,
        }
    }

    /// Probe-insert. When `allow_new` is false, only in-place updates of an
    /// existing key succeed; empty slot (key absent under open addressing) or a
    /// full probe without a match returns [`InsertResult::NeedSlot`].
    fn try_insert(
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

    fn read_slot(&mut self, slot: u64) -> Result<(ShHeadKey, [u8; SH_HEAD_VALUE_LEN]), StoreError> {
        let chunk = self.ensure_chunk(slot)?;
        let rel = ((slot - chunk.base_slot) as usize) * SH_HEAD_SLOT_SIZE;
        let k: ShHeadKey = chunk.data[rel..rel + SH_HEAD_KEY_LEN].try_into().unwrap();
        let v: [u8; SH_HEAD_VALUE_LEN] = chunk.data[rel + SH_HEAD_KEY_LEN..rel + SH_HEAD_SLOT_SIZE]
            .try_into()
            .unwrap();
        Ok((k, v))
    }

    fn write_slot(
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

    fn ensure_chunk(&mut self, slot: u64) -> Result<&mut CachedChunk, StoreError> {
        let chunk_idx = slot / SLOTS_PER_CHUNK;
        if !self.chunks.contains_key(&chunk_idx) {
            if self.chunks.len() >= CHUNK_CACHE_MAX {
                self.flush()?;
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
        }
        Ok(self.chunks.get_mut(&chunk_idx).unwrap())
    }

    fn flush(&mut self) -> Result<(), StoreError> {
        for (_, chunk) in self.chunks.iter_mut() {
            if chunk.dirty {
                let off = ScriptHashHead::slot_file_off(chunk.base_slot);
                self.head.file.write_at(off, &chunk.data)?;
                chunk.dirty = false;
            }
        }
        self.chunks.clear();
        Ok(())
    }
}

/// In-RAM open-address image for **one** cold-materialize shard.
///
/// Pre-sized to final `slots` at construction (no grow-from-tiny happy path).
/// Stream inserts by probe; on shard exit [`Self::install_into`] writes once.
pub struct LiveShardTable {
    slots: u64,
    occupied: u64,
    table: Vec<u8>,
    /// Unique keys inserted (same as occupied for cold empty-start).
    keys: u64,
}

impl LiveShardTable {
    /// Allocate a zeroed slot image for `key_budget` unique keys (final size).
    pub fn with_key_budget(key_budget: u64) -> Self {
        let slots = sh_slots_for_keys(key_budget);
        let nbytes = (slots as usize).saturating_mul(SH_HEAD_SLOT_SIZE);
        Self {
            slots,
            occupied: 0,
            table: vec![0u8; nbytes],
            keys: 0,
        }
    }

    pub fn slots(&self) -> u64 {
        self.slots
    }

    pub fn occupied(&self) -> u64 {
        self.occupied
    }

    pub fn keys(&self) -> u64 {
        self.keys
    }

    pub fn table_bytes(&self) -> usize {
        self.table.len()
    }

    /// Occupied `(key16, value16)` records in key order (cold seal into SortedHead).
    pub fn collect_sorted_recs(&self) -> Vec<(ShHeadKey, [u8; SH_HEAD_VALUE_LEN])> {
        let mut recs = Vec::with_capacity(self.occupied as usize);
        for s in 0..self.slots {
            let off = (s as usize) * SH_HEAD_SLOT_SIZE;
            let k: ShHeadKey = self.table[off..off + SH_HEAD_KEY_LEN].try_into().unwrap();
            let v: [u8; SH_HEAD_VALUE_LEN] = self.table
                [off + SH_HEAD_KEY_LEN..off + SH_HEAD_SLOT_SIZE]
                .try_into()
                .unwrap();
            if !is_empty_slot(&k, &v) {
                recs.push((k, v));
            }
        }
        recs.sort_unstable_by(|a, b| a.0.cmp(&b.0));
        recs
    }

    /// Insert one head value (full Electrum scripthash). Overwrites same key16.
    pub fn insert(&mut self, full: &[u8; 32], val: &ShHeadValue) -> Result<(), StoreError> {
        if val.is_empty() {
            return Ok(());
        }
        // Exception path only: pathological skew past pre-size.
        if self.occupied.saturating_add(1).saturating_mul(MAX_LOAD_DEN)
            > self.slots.saturating_mul(MAX_LOAD_NUM)
        {
            self.rehash_double()?;
        }
        let key = head_key_from_full(full);
        let enc = pack8_bytes(val)?;
        self.place(key, &enc)?;
        self.keys = self.keys.saturating_add(1);
        Ok(())
    }

    fn place(&mut self, key: ShHeadKey, enc: &[u8; SH_HEAD_VALUE_LEN]) -> Result<(), StoreError> {
        let mut slot = open_address::primary_slot(&key, self.slots);
        for _ in 0..self.slots {
            let off = (slot as usize) * SH_HEAD_SLOT_SIZE;
            let slot_key: ShHeadKey = self.table[off..off + SH_HEAD_KEY_LEN].try_into().unwrap();
            let slot_v: [u8; SH_HEAD_VALUE_LEN] = self.table
                [off + SH_HEAD_KEY_LEN..off + SH_HEAD_SLOT_SIZE]
                .try_into()
                .unwrap();
            if is_empty_slot(&slot_key, &slot_v) {
                self.table[off..off + SH_HEAD_KEY_LEN].copy_from_slice(&key);
                self.table[off + SH_HEAD_KEY_LEN..off + SH_HEAD_SLOT_SIZE].copy_from_slice(enc);
                self.occupied = self.occupied.saturating_add(1);
                return Ok(());
            }
            if slot_key == key {
                self.table[off + SH_HEAD_KEY_LEN..off + SH_HEAD_SLOT_SIZE].copy_from_slice(enc);
                return Ok(());
            }
            slot = (slot + 1) & (self.slots - 1);
        }
        Err(StoreError::Corrupt("scripthash live shard table full"))
    }

    fn rehash_double(&mut self) -> Result<(), StoreError> {
        let new_slots = self.slots.saturating_mul(2).max(2).next_power_of_two();
        let mut neu = LiveShardTable {
            slots: new_slots,
            occupied: 0,
            table: vec![0u8; (new_slots as usize).saturating_mul(SH_HEAD_SLOT_SIZE)],
            keys: 0,
        };
        let old_slots = self.slots;
        let old = std::mem::take(&mut self.table);
        for s in 0..old_slots {
            let off = (s as usize) * SH_HEAD_SLOT_SIZE;
            let k: ShHeadKey = old[off..off + SH_HEAD_KEY_LEN].try_into().unwrap();
            let v: [u8; SH_HEAD_VALUE_LEN] = old[off + SH_HEAD_KEY_LEN..off + SH_HEAD_SLOT_SIZE]
                .try_into()
                .unwrap();
            if !is_empty_slot(&k, &v) {
                neu.place(k, &v)?;
            }
        }
        neu.keys = self.keys;
        *self = neu;
        Ok(())
    }

    /// Sequential write into an **empty** on-disk shard; frees this image.
    pub fn install_into(self, head: &ScriptHashHead) -> Result<(), StoreError> {
        if !head.is_known_empty() {
            return Err(StoreError::Corrupt(
                "scripthash live install: shard not empty",
            ));
        }
        head.install_cold_image(&self.table, self.slots, self.occupied)
    }
}

/// Sharded facade (64-way mainnet) over [`ScriptHashHead`].
#[cfg(test)]
pub struct ShardedScriptHashHead {
    shards: Vec<ScriptHashHead>,
}

#[cfg(test)]
impl ShardedScriptHashHead {
    #[cfg(test)]
    pub fn create_for_role(path: impl Into<PathBuf>, role: HeadRole) -> Result<Self, StoreError> {
        debug_assert_eq!(role, HeadRole::ScriptHash);
        Self::create_sharded(
            path,
            shard_count_for_role(role),
            initial_slots_per_shard(role),
        )
    }

    pub fn create_sharded(
        path: impl Into<PathBuf>,
        shard_count: usize,
        slots_each: u64,
    ) -> Result<Self, StoreError> {
        let path = path.into();
        let n = shard_count.max(1);
        let per = slots_each.max(2).next_power_of_two();
        if n == 1 {
            let h = ScriptHashHead::create_with_slots(&path, per)?;
            return Ok(Self { shards: vec![h] });
        }
        if path.exists() {
            return Err(StoreError::io(
                &path,
                std::io::Error::new(
                    std::io::ErrorKind::AlreadyExists,
                    "sharded scripthash head path exists",
                ),
            ));
        }
        std::fs::create_dir_all(&path).map_err(|e| StoreError::io(&path, e))?;
        let mut shards = Vec::with_capacity(n);
        for i in 0..n {
            let shard_path = path.join(format!("{i:02x}"));
            shards.push(ScriptHashHead::create_with_slots(shard_path, per)?);
        }
        let _ = HeadScale::from_env();
        let _ = initial_slots_for(HeadRole::ScriptHash);
        Ok(Self { shards })
    }

    /// Zero every shard (cold rematerialize).
    pub fn reinit_empty(&self) -> Result<(), StoreError> {
        for s in &self.shards {
            s.reinit_empty()?;
        }
        Ok(())
    }

    pub fn open_for_role(path: impl Into<PathBuf>, role: HeadRole) -> Result<Self, StoreError> {
        let path = path.into();
        if path.is_dir() {
            // Shard files only (`00`..`3f`); ignore `00.occ` sidecars and temps.
            let mut names: Vec<String> = std::fs::read_dir(&path)
                .map_err(|e| StoreError::io(&path, e))?
                .filter_map(|e| e.ok())
                .filter(|e| e.file_type().map(|t| t.is_file()).unwrap_or(false))
                .map(|e| e.file_name().to_string_lossy().into_owned())
                .filter(|n| n.len() == 2 && n.chars().all(|c| c.is_ascii_hexdigit()))
                .collect();
            names.sort();
            if names.is_empty() {
                return Err(StoreError::Corrupt("sharded scripthash head empty"));
            }
            // Mainnet expects 64-way. Leftover live OA at `scripthash.head` is
            // refused by [`crate::scripthash::ScriptHashTable::open`].
            let expected = shard_count_for_role(role);
            if expected > 1 && names.len() != expected {
                return Err(StoreError::Corrupt(SH_HEAD_SHARD_COUNT_MISMATCH));
            }
            let mut shards = Vec::with_capacity(names.len());
            for (i, name) in names.iter().enumerate() {
                let expect = format!("{i:02x}");
                if name != &expect {
                    return Err(StoreError::Corrupt(
                        "sharded scripthash head unexpected shard name",
                    ));
                }
                shards.push(ScriptHashHead::open(path.join(name))?);
            }
            return Ok(Self { shards });
        }
        if path.is_file() {
            return Ok(Self {
                shards: vec![ScriptHashHead::open(path)?],
            });
        }
        Err(StoreError::io(
            &path,
            std::io::Error::new(std::io::ErrorKind::NotFound, "scripthash head missing"),
        ))
    }

    #[inline]
    fn shard_of(&self, full: &[u8; 32]) -> usize {
        prefix_shard_of(full, self.shards.len())
    }

    #[cfg(test)]
    pub fn get(&self, key: &[u8; 32]) -> Result<Option<ShHeadValue>, StoreError> {
        self.shards[self.shard_of(key)].get(key)
    }

    #[cfg(test)]
    pub fn insert(&self, key: &[u8; 32], value: &ShHeadValue) -> Result<(), StoreError> {
        self.shards[self.shard_of(key)].insert(key, value)
    }

    #[cfg(test)]
    pub fn clear_key(&self, key: &[u8; 32]) -> Result<bool, StoreError> {
        self.shards[self.shard_of(key)].clear_key(key)
    }

    pub fn shard_count(&self) -> usize {
        self.shards.len()
    }

    /// Slot count of one main shard (overflow segment geometry = this size).
    #[cfg(test)]
    pub fn slots_per_shard(&self) -> u64 {
        self.shards.first().map(|s| s.slots()).unwrap_or(64)
    }

    /// Total OA slots across all main shards.
    #[cfg(test)]
    pub fn total_slots(&self) -> u64 {
        self.shards.iter().map(|s| s.slots()).sum()
    }

    /// Shard index for a full Electrum scripthash (same as insert routing).
    #[inline]
    pub fn shard_index(&self, full: &[u8; 32]) -> usize {
        self.shard_of(full)
    }

    /// True when every shard is **known** empty (cold bulk fill precondition).
    ///
    /// Large shards opened without a `.occ` sidecar return false (occupancy
    /// unknown) so tip materialize never treats a live multi‑GiB head as empty.
    pub fn is_empty(&self) -> bool {
        self.shards.iter().all(|s| s.is_known_empty())
    }

    /// Install a finished [`LiveShardTable`] into `shard` (empty cold path).
    #[cfg(test)]
    pub fn install_live_shard(&self, shard: usize, live: LiveShardTable) -> Result<(), StoreError> {
        if shard >= self.shards.len() {
            return Err(StoreError::Corrupt(
                "scripthash install_live_shard: shard out of range",
            ));
        }
        if live.keys() == 0 && live.occupied() == 0 {
            return Ok(());
        }
        live.install_into(&self.shards[shard])
    }

    /// Insert head values, applying **one shard at a time** (sorted within shard).
    ///
    /// When `flush_each_shard` is true (large materialize runs), flush the shard
    /// file after its bucket so the working set does not keep every shard dirty
    /// at once. Small runs skip the per-shard flush and rely on later table flush.
    pub const SH_SEAL_LOAD: f64 = ScriptHashHead::SH_SEAL_LOAD;

    pub fn flush(&self) -> Result<(), StoreError> {
        for s in &self.shards {
            s.flush()?;
        }
        Ok(())
    }

    pub fn flush_async(&self) -> Result<(), StoreError> {
        for s in &self.shards {
            s.flush_async()?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rbitcoin_primitives::Fk;

    #[test]
    fn prefix_shard_of_high_bits_contiguous() {
        // 16 shards: first nibble of byte 0.
        assert_eq!(prefix_shard_of(&[0x00; 32], 16), 0);
        assert_eq!(prefix_shard_of(&[0x0f; 32], 16), 0);
        let mut k = [0u8; 32];
        k[0] = 0x10;
        assert_eq!(prefix_shard_of(&k, 16), 1);
        k[0] = 0x1f;
        assert_eq!(prefix_shard_of(&k, 16), 1);
        k[0] = 0xf0;
        assert_eq!(prefix_shard_of(&k, 16), 15);
        k[0] = 0xff;
        assert_eq!(prefix_shard_of(&k, 16), 15);
        // Lex order of first byte maps to non-decreasing shard ids.
        let mut prev = 0usize;
        for b in 0u16..=255 {
            k[0] = b as u8;
            let s = prefix_shard_of(&k, 16);
            assert!(s >= prev, "b={b:#x} shard={s} prev={prev}");
            prev = s;
        }
        assert_eq!(prefix_shard_of(&[0xab; 32], 1), 0);
    }

    #[test]
    fn head_insert_get_clear() {
        let path = std::env::temp_dir().join(format!(
            "rbitcoin-shhead-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(occ_sidecar_path(&path));
        let h = ScriptHashHead::create_with_slots(&path, 64).unwrap();
        let mut key = [0u8; 32];
        key[0] = 7;
        let val = ShHeadValue::inline_one(Fk(42));
        h.insert(&key, &val).unwrap();
        assert_eq!(h.get(&key).unwrap().unwrap(), val);
        assert!(h.clear_key(&key).unwrap());
        assert!(h.get(&key).unwrap().is_none());
        // Sidecar tracks occupied; reopen without full scan path for tiny files.
        drop(h);
        let h2 = ScriptHashHead::open(&path).unwrap();
        assert!(h2.is_known_empty() || h2.occupied() >= 1); // soft-clear keeps slot
        assert!(h2.get(&key).unwrap().is_none());
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(occ_sidecar_path(&path));
    }

    #[test]
    fn get_many_matches_serial_get() {
        let path = std::env::temp_dir().join(format!(
            "rbitcoin-shhead-many-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(occ_sidecar_path(&path));
        let h = ScriptHashHead::create_with_slots(&path, 4096).unwrap();
        let mut keys = Vec::new();
        for i in 0u32..200 {
            let mut k = [0u8; 32];
            k[0..4].copy_from_slice(&i.to_le_bytes());
            k[4] = 0x5a;
            h.insert(&k, &ShHeadValue::inline_one(Fk(u64::from(i) + 1)))
                .unwrap();
            keys.push(k);
        }
        // Mix in missing keys.
        let mut probe = keys.clone();
        for i in 1000u32..1100 {
            let mut k = [0u8; 32];
            k[0..4].copy_from_slice(&i.to_le_bytes());
            probe.push(k);
        }
        let batch = h.get_many(&probe).unwrap();
        assert_eq!(batch.len(), probe.len());
        for (k, got) in probe.iter().zip(batch.iter()) {
            let serial = h.get(k).unwrap();
            assert_eq!(got, &serial, "key mismatch");
        }
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(occ_sidecar_path(&path));
    }

    /// Batch head seed (`get_many`) vs N serial `get` — tip SH seed shape.
    ///
    /// Serial path creates a new `SlotPageCache` per key (old seed loop) → one
    /// 4 KiB pread per key. Batch shares one cache and visits keys in slot order
    /// → one pread per unique page. Asserts **chunk-load** speedup (deterministic).
    /// Wall-time multi-round probe lives under `#[ignore]` (diagnostic only).
    #[test]
    fn get_many_cuts_chunk_loads_vs_serial_seed() {
        let path = std::env::temp_dir().join(format!(
            "rbitcoin-shhead-batch-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(occ_sidecar_path(&path));
        // 4 Ki slots → 32 × 4 KiB pages. 512 keys ⇒ unique pages ≪ N.
        let h = ScriptHashHead::create_with_slots(&path, 1 << 12).unwrap();
        const N: u32 = 512;
        let mut keys = Vec::with_capacity(N as usize);
        for i in 0..N {
            let mut k = [0u8; 32];
            k[0..4].copy_from_slice(&i.to_le_bytes());
            k[8] = (i % 251) as u8;
            h.insert(&k, &ShHeadValue::inline_one(Fk(u64::from(i) + 1)))
                .unwrap();
            keys.push(k);
        }

        let mut serial_loads = 0u64;
        for k in &keys {
            let (_, n) = h.get_with_chunk_loads(k).unwrap();
            serial_loads = serial_loads.saturating_add(n);
        }
        let (batch_vals, batch_loads) = h.get_many_with_chunk_loads(&keys).unwrap();
        assert_eq!(batch_vals.len(), keys.len());
        for (k, got) in keys.iter().zip(batch_vals.iter()) {
            assert_eq!(got, &h.get(k).unwrap());
        }
        let load_speedup = serial_loads as f64 / batch_loads.max(1) as f64;
        eprintln!(
            "sh head seed chunk loads: serial={serial_loads} batch={batch_loads} \
             speedup={load_speedup:.2}× (keys={N})"
        );
        assert!(
            batch_loads * 3 < serial_loads && load_speedup >= 2.0,
            "get_many should cut 4 KiB preads by ≥2× vs serial get: \
             batch_loads={batch_loads} serial_loads={serial_loads} speedup={load_speedup:.2}×"
        );
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(occ_sidecar_path(&path));
    }

    #[test]
    fn open_large_without_occ_skips_scan_not_empty() {
        // Body just over OCC_SCAN_BYTE_CAP → skip full scan when .occ missing.
        let path = std::env::temp_dir().join(format!(
            "rbitcoin-shhead-large-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(occ_sidecar_path(&path));
        // body = slots * 32 B; need body > 16 MiB ⇒ slots > 16MiB/32 = 524_288.
        let min_slots = (OCC_SCAN_BYTE_CAP / SH_HEAD_SLOT_SIZE as u64) + 1;
        let slots = min_slots.next_power_of_two();
        assert!(slots * SH_HEAD_SLOT_SIZE as u64 > OCC_SCAN_BYTE_CAP);
        let h = ScriptHashHead::create_with_slots(&path, slots).unwrap();
        assert!(h.is_known_empty());
        let mut key = [0u8; 32];
        key[0] = 0xab;
        h.insert(&key, &ShHeadValue::inline_one(Fk(1))).unwrap();
        assert_eq!(h.occupied(), 1);
        drop(h);
        // Drop sidecar only — reopen must not scan body and must not claim empty.
        let _ = std::fs::remove_file(occ_sidecar_path(&path));
        let t0 = Instant::now();
        let h2 = ScriptHashHead::open(&path).unwrap();
        let open_ms = t0.elapsed().as_millis();
        assert!(
            !h2.is_known_empty(),
            "missing .occ on large head must not report empty"
        );
        assert!(
            open_ms < 2_000,
            "open without .occ must skip full scan (took {open_ms}ms)"
        );
        // Lookups still work (probe, not occupancy).
        assert_eq!(h2.get(&key).unwrap().unwrap().inline_fks(), vec![Fk(1)]);
        // Reinit seals known empty + sidecar for cold path.
        h2.reinit_empty().unwrap();
        assert!(h2.is_known_empty());
        assert!(load_occ_sidecar(&path) == Some(0));
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(occ_sidecar_path(&path));
    }

    /// No-rehash batch returns remainder for new keys after full; still applies
    /// later in-place updates (update-only mode) without a get-scan.
    #[test]
    fn insert_many_no_rehash_remainder_keeps_updates() {
        use crate::scripthash_layout::head_key_from_full;
        let path = std::env::temp_dir().join(format!(
            "rbitcoin-shhead-norem-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let _ = std::fs::remove_file(&path);
        // Tiny table: 8 slots. Seed via no_rehash so `insert` does not rehash to 64.
        let h = ScriptHashHead::create_with_slots(&path, 8).unwrap();
        let mut existing = Vec::new();
        let mut seed = Vec::new();
        for i in 0..6u8 {
            let mut full = [0u8; 32];
            full[0] = i;
            full[1] = 0x11;
            seed.push((
                head_key_from_full(&full),
                ShHeadValue::inline_one(Fk(u64::from(i) + 1)),
            ));
            existing.push(full);
        }
        assert!(h.insert_many_no_rehash(&seed, true).unwrap().is_empty());
        assert_eq!(h.slots(), 8);
        assert_eq!(h.occupied(), 6);
        // Batch: update first existing key + 6 brand-new keys (more than free slots).
        let mut batch: Vec<(crate::scripthash_layout::ShHeadKey, ShHeadValue)> = Vec::new();
        batch.push((
            head_key_from_full(&existing[0]),
            ShHeadValue::slab(0, 2, 4096),
        ));
        for i in 0..6u8 {
            let mut full = [0u8; 32];
            full[0] = 0xa0 + i;
            full[1] = 0x22;
            batch.push((
                head_key_from_full(&full),
                ShHeadValue::inline_one(Fk(100 + u64::from(i))),
            ));
        }
        let rem = h.insert_many_no_rehash(&batch, true).unwrap();
        assert!(
            !rem.is_empty(),
            "some new keys must remainder; occupied={} slots={} rem={} batch={}",
            h.occupied(),
            h.slots(),
            rem.len(),
            batch.len(),
        );
        assert_eq!(h.slots(), 8, "no-rehash must not grow");
        // Update applied on the existing key.
        let v = h.get(&existing[0]).unwrap().unwrap();
        match v {
            ShHeadValue::Slab { used: 2, .. } => {}
            other => panic!("expected 2-fk slab update, got {other:?}"),
        }
        // Remainder keys are not on this head.
        for (hk, _) in &rem {
            let mut full = [0u8; 32];
            full[..16].copy_from_slice(hk);
            assert!(
                h.get(&full).unwrap().is_none(),
                "remainder key must not be on head"
            );
        }
        // Update key must not appear in remainder.
        let upd_hk = head_key_from_full(&existing[0]);
        assert!(!rem.iter().any(|(k, _)| k == &upd_hk));

        // Update-only (allow_new=false): free slots must not take brand-new keys.
        let mut brand = [0u8; 32];
        brand[0] = 0xfe;
        brand[1] = 0x99;
        let only_new = vec![(head_key_from_full(&brand), ShHeadValue::inline_one(Fk(555)))];
        let rem2 = h.insert_many_no_rehash(&only_new, false).unwrap();
        assert_eq!(rem2.len(), 1, "update-only must remainder all new keys");
        assert!(h.get(&brand).unwrap().is_none());
        assert_eq!(h.slots(), 8);

        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(occ_sidecar_path(&path));
    }

    /// Rehash, live install, clear miss, for_each, open.
    #[test]
    fn scripthash_head_reserve_and_bulk_errors() {
        // Single-file head: reserve_additional cold + rehash.
        let path = std::env::temp_dir().join(format!(
            "rbitcoin-shhead-reserve-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let _ = std::fs::remove_file(&path);
        let h = ScriptHashHead::create_with_slots(&path, 8).unwrap();
        h.reserve_additional(0).unwrap();
        h.reserve_additional(200).unwrap(); // cold grow empty
        let mut k = [0u8; 32];
        k[0] = 9;
        h.insert(&k, &ShHeadValue::inline_one(Fk(1))).unwrap();
        h.reserve_additional(50).unwrap(); // rehash when occupied
        let _ = std::fs::remove_file(&path);

        // Sharded facade: live install + OOB.
        let sh_path = std::env::temp_dir().join(format!(
            "rbitcoin-shhead-sharded-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let _ = std::fs::remove_dir_all(&sh_path);
        let sh = ShardedScriptHashHead::create_sharded(&sh_path, 4, 16).unwrap();
        assert!(sh.is_empty());
        let mut live = LiveShardTable::with_key_budget(8);
        let mut k0 = [0u8; 32];
        k0[0] = 0x00;
        live.insert(&k0, &ShHeadValue::inline_one(Fk(3))).unwrap();
        sh.install_live_shard(0, live).unwrap();
        assert!(!sh.is_empty());
        assert!(matches!(
            sh.install_live_shard(9999, LiveShardTable::with_key_budget(1)),
            Err(StoreError::Corrupt(_))
        ));
        let _ = std::fs::remove_dir_all(&sh_path);
    }

    #[test]
    fn scripthash_head_rehash_bulk_and_for_each() {
        let path = std::env::temp_dir().join(format!(
            "rbitcoin-shhead-rehash-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let _ = std::fs::remove_file(&path);
        let h = ScriptHashHead::create_with_slots(&path, 8).unwrap(); // tiny → force rehash
        h.insert_many(&[]).unwrap();
        // Empty values clear path
        let mut k0 = [0u8; 32];
        k0[0] = 1;
        h.insert(&k0, &ShHeadValue::Empty).unwrap();
        assert!(!h.clear_key(&k0).unwrap()); // miss

        let mut batch = Vec::new();
        for i in 0u64..40 {
            let mut key = [0u8; 32];
            key[0..8].copy_from_slice(&i.to_le_bytes());
            let val = if i % 3 == 0 {
                ShHeadValue::slab(0, 2, 4096 + i)
            } else if i % 3 == 1 {
                ShHeadValue::inline_one(Fk(i + 1))
            } else {
                ShHeadValue::paged(4112 + i * 4096, 4112 + i * 4096)
            };
            batch.push((key, val));
        }
        h.insert_many(&batch).unwrap();
        // bulk path already used on first fill; re-insert more for rehash
        let mut more = Vec::new();
        for i in 40u64..80 {
            let mut key = [0u8; 32];
            key[0..8].copy_from_slice(&i.to_le_bytes());
            more.push((key, ShHeadValue::inline_one(Fk(i + 1))));
        }
        h.insert_many(&more).unwrap();
        let mut seen = 0u64;
        h.for_each_occupied(|full, val| {
            assert!(!val.is_empty());
            assert!(full.iter().any(|&b| b != 0) || full == [0u8; 32]);
            seen += 1;
            Ok(())
        })
        .unwrap();
        assert!(seen >= 70);
        h.flush().unwrap();
        h.flush_async().unwrap();
        drop(h);
        let h2 = ScriptHashHead::open(&path).unwrap();
        let mut key = [0u8; 32];
        key[0..8].copy_from_slice(&5u64.to_le_bytes());
        assert!(h2.get(&key).unwrap().is_some());
        // corrupt size open
        {
            std::fs::OpenOptions::new()
                .write(true)
                .open(&path)
                .unwrap()
                .set_len((FILE_HEADER_LEN + 3) as u64)
                .unwrap();
        }
        assert!(matches!(
            ScriptHashHead::open(&path),
            Err(StoreError::Corrupt(_))
        ));
        let _ = std::fs::remove_file(&path);
    }

    /// Multi-shard create/open/insert/reinit/flush + error arms.
    #[test]
    fn sharded_scripthash_head_create_open_and_errors() {
        let base = std::env::temp_dir().join(format!(
            "rbitcoin-sh-sharded-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let _ = std::fs::remove_dir_all(&base);
        std::fs::create_dir_all(&base).unwrap();
        // n=1 uses single-file path
        let single = base.join("single");
        let h1 = ShardedScriptHashHead::create_sharded(&single, 1, 32).unwrap();
        assert_eq!(h1.shard_count(), 1);
        let mut k = [0u8; 32];
        k[0] = 0xab;
        h1.insert(&k, &ShHeadValue::inline_one(Fk(9))).unwrap();
        assert!(h1.get(&k).unwrap().is_some());
        h1.clear_key(&k).unwrap();
        h1.reinit_empty().unwrap();
        h1.flush().unwrap();
        h1.flush_async().unwrap();
        drop(h1);

        // Multi-shard directory layout
        let multi = base.join("multi");
        let h = ShardedScriptHashHead::create_sharded(&multi, 4, 16).unwrap();
        assert_eq!(h.shard_count(), 4);
        // route inserts across shards
        for i in 0u8..16 {
            let mut key = [0u8; 32];
            key[0] = i.wrapping_mul(0x40); // spread high bits
            h.insert(&key, &ShHeadValue::inline_one(Fk(i as u64 + 1)))
                .unwrap();
            assert_eq!(h.shard_index(&key), prefix_shard_of(&key, 4));
        }
        h.flush().unwrap();
        drop(h);
        // open_for_role directory
        let h2 = ShardedScriptHashHead::open_for_role(&multi, HeadRole::ScriptHash).unwrap();
        assert_eq!(h2.shard_count(), 4);
        let key0 = [0u8; 32];
        assert!(h2.get(&key0).unwrap().is_some());
        h2.reinit_empty().unwrap();
        assert!(h2.get(&key0).unwrap().is_none());
        drop(h2);

        // create on existing path fails
        assert!(ShardedScriptHashHead::create_sharded(&multi, 4, 16).is_err());

        // open empty dir
        let empty = base.join("empty");
        std::fs::create_dir_all(&empty).unwrap();
        assert!(matches!(
            ShardedScriptHashHead::open_for_role(&empty, HeadRole::ScriptHash),
            Err(StoreError::Corrupt(_))
        ));
        // open missing
        assert!(
            ShardedScriptHashHead::open_for_role(base.join("nope"), HeadRole::ScriptHash).is_err()
        );
        // open single file via open_for_role
        let h3 = ShardedScriptHashHead::open_for_role(&single, HeadRole::ScriptHash).unwrap();
        assert_eq!(h3.shard_count(), 1);

        // unexpected shard name
        let bad = base.join("badnames");
        std::fs::create_dir_all(&bad).unwrap();
        std::fs::write(bad.join("zz"), b"x").unwrap();
        assert!(matches!(
            ShardedScriptHashHead::open_for_role(&bad, HeadRole::ScriptHash),
            Err(StoreError::Corrupt(_))
        ));

        let _ = std::fs::remove_dir_all(&base);
    }
}
