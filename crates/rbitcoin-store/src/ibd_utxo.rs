//! IBD-only unspent-outpoint table (mmap, 16-byte slots).
//!
//! Slot: `txid_prefix[12] || pack(state:u8, vout:u24)`.
//! Used for Core-equivalent spentness during catch-up without a multi‑tens-GiB
//! exact spend HashSet. Not a full coins cache (no value/script).

use crate::error::StoreError;
use memmap2::MmapMut;
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

const MAGIC: &[u8; 8] = b"RBUXTO02";
const HEADER_LEN: usize = 4096;
const SLOT_LEN: usize = 16;
const PREFIX_LEN: usize = 12;

const STATE_EMPTY: u8 = 0;
const STATE_LIVE: u8 = 1;
const STATE_TOMB: u8 = 2;

/// Product constraint: vout fits in 24 bits.
pub const VOUT_MAX: u32 = (1 << 24) - 1;

const OFF_MAGIC: usize = 0;
const OFF_VERSION: usize = 8;
const OFF_TIP: usize = 12;
const OFF_NUM_SLOTS: usize = 16;
const OFF_LIVE: usize = 24;
const OFF_PREFIX_BITS: usize = 32;
const OFF_VOUT_BITS: usize = 34;
const OFF_FLAGS: usize = 36;
const VERSION: u32 = 1;

/// Default initial slots (2^22 → 64 MiB of slots). Grows as needed.
/// Production: `RBITCOIN_IBD_UTXO_SLOTS=268435456` (~4 GiB) for mainnet-scale.
pub const DEFAULT_NUM_SLOTS: u64 = 1 << 22;
const LOAD_GROW: f64 = 0.80;

#[inline]
fn pack(state: u8, vout: u32) -> u32 {
    debug_assert!(vout <= VOUT_MAX);
    ((state as u32) << 24) | (vout & 0x00FF_FFFF)
}

#[inline]
fn unpack(p: u32) -> (u8, u32) {
    ((p >> 24) as u8, p & 0x00FF_FFFF)
}

#[inline]
fn prefix_of(txid: &[u8; 32]) -> [u8; PREFIX_LEN] {
    let mut p = [0u8; PREFIX_LEN];
    p.copy_from_slice(&txid[0..PREFIX_LEN]);
    p
}

/// Mix prefix+vout → slot index (triangular probe uses this as start).
fn hash0(prefix: &[u8; PREFIX_LEN], vout: u32) -> u64 {
    // FNV-1a 64
    let mut h = 0xcbf29ce484222325u64;
    for &b in prefix {
        h ^= u64::from(b);
        h = h.wrapping_mul(0x100000001b3);
    }
    h ^= u64::from(vout);
    h = h.wrapping_mul(0x100000001b3);
    h
}

fn io(path: &Path, e: std::io::Error) -> StoreError {
    StoreError::io(path, e)
}

/// Persistent IBD UTXO membership table.
pub struct IbdUtxo {
    path: PathBuf,
    file: File,
    map: MmapMut,
    num_slots: u64,
    live: u64,
    /// Confirmed tip height reflected in the table; `None` = empty chain.
    tip: Option<u32>,
    /// Rare (prefix,vout) collisions: full txids still unspent under that key.
    overflow: HashMap<([u8; PREFIX_LEN], u32), Vec<[u8; 32]>>,
}

impl IbdUtxo {
    fn slots_offset() -> usize {
        HEADER_LEN
    }

    fn file_len(num_slots: u64) -> u64 {
        Self::slots_offset() as u64 + num_slots * SLOT_LEN as u64
    }

    fn slot_ptr(map: &MmapMut, num_slots: u64, i: u64) -> (*const u8, *mut u8) {
        debug_assert!(i < num_slots);
        let off = Self::slots_offset() + (i as usize) * SLOT_LEN;
        let p = map.as_ptr().wrapping_add(off);
        (p, p as *mut u8)
    }

    fn read_slot(map: &MmapMut, num_slots: u64, i: u64) -> ([u8; PREFIX_LEN], u8, u32) {
        let (p, _) = Self::slot_ptr(map, num_slots, i);
        // SAFETY: i < num_slots, map covers slots region
        unsafe {
            let mut prefix = [0u8; PREFIX_LEN];
            std::ptr::copy_nonoverlapping(p, prefix.as_mut_ptr(), PREFIX_LEN);
            let mut pb = [0u8; 4];
            std::ptr::copy_nonoverlapping(p.add(PREFIX_LEN), pb.as_mut_ptr(), 4);
            let packed = u32::from_le_bytes(pb);
            let (state, vout) = unpack(packed);
            (prefix, state, vout)
        }
    }

    fn write_slot(
        map: &mut MmapMut,
        num_slots: u64,
        i: u64,
        prefix: &[u8; PREFIX_LEN],
        state: u8,
        vout: u32,
    ) {
        let (_, p) = Self::slot_ptr(map, num_slots, i);
        let packed = pack(state, vout).to_le_bytes();
        unsafe {
            std::ptr::copy_nonoverlapping(prefix.as_ptr(), p, PREFIX_LEN);
            std::ptr::copy_nonoverlapping(packed.as_ptr(), p.add(PREFIX_LEN), 4);
        }
    }

    fn write_header(map: &mut MmapMut, tip: Option<u32>, num_slots: u64, live: u64) {
        let buf = &mut map[..HEADER_LEN];
        buf[OFF_MAGIC..OFF_MAGIC + 8].copy_from_slice(MAGIC);
        buf[OFF_VERSION..OFF_VERSION + 4].copy_from_slice(&VERSION.to_le_bytes());
        let tip_raw = tip.map(|t| t).unwrap_or(u32::MAX);
        buf[OFF_TIP..OFF_TIP + 4].copy_from_slice(&tip_raw.to_le_bytes());
        buf[OFF_NUM_SLOTS..OFF_NUM_SLOTS + 8].copy_from_slice(&num_slots.to_le_bytes());
        buf[OFF_LIVE..OFF_LIVE + 8].copy_from_slice(&live.to_le_bytes());
        buf[OFF_PREFIX_BITS..OFF_PREFIX_BITS + 2].copy_from_slice(&96u16.to_le_bytes());
        buf[OFF_VOUT_BITS..OFF_VOUT_BITS + 2].copy_from_slice(&24u16.to_le_bytes());
        buf[OFF_FLAGS..OFF_FLAGS + 4].copy_from_slice(&0u32.to_le_bytes());
    }

    fn read_header(map: &MmapMut) -> Result<(Option<u32>, u64, u64), StoreError> {
        if map.len() < HEADER_LEN {
            return Err(StoreError::Corrupt("ibd utxo header short"));
        }
        if &map[OFF_MAGIC..OFF_MAGIC + 8] != MAGIC {
            return Err(StoreError::Corrupt("ibd utxo bad magic"));
        }
        let ver = u32::from_le_bytes(map[OFF_VERSION..OFF_VERSION + 4].try_into().unwrap());
        if ver != VERSION {
            return Err(StoreError::Corrupt("ibd utxo unsupported version"));
        }
        let tip_raw = u32::from_le_bytes(map[OFF_TIP..OFF_TIP + 4].try_into().unwrap());
        let tip = if tip_raw == u32::MAX {
            None
        } else {
            Some(tip_raw)
        };
        let num_slots = u64::from_le_bytes(map[OFF_NUM_SLOTS..OFF_NUM_SLOTS + 8].try_into().unwrap());
        let live = u64::from_le_bytes(map[OFF_LIVE..OFF_LIVE + 8].try_into().unwrap());
        if !num_slots.is_power_of_two() || num_slots < 16 {
            return Err(StoreError::Corrupt("ibd utxo bad num_slots"));
        }
        Ok((tip, num_slots, live))
    }

    /// Create a new empty table at `path` with `num_slots` (rounded up to pow2).
    pub fn create(path: impl Into<PathBuf>, num_slots: u64) -> Result<Self, StoreError> {
        let path = path.into();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(|e| io(parent, e))?;
        }
        let num_slots = num_slots.max(16).next_power_of_two();
        let flen = Self::file_len(num_slots);
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)
            .map_err(|e| io(&path, e))?;
        file.set_len(flen).map_err(|e| io(&path, e))?;
        let mut map = unsafe { MmapMut::map_mut(&file) }.map_err(|e| io(&path, e))?;
        // Zero slots (EMPTY=0).
        map[HEADER_LEN..].fill(0);
        Self::write_header(&mut map, None, num_slots, 0);
        map.flush().map_err(|e| io(&path, e))?;
        Ok(Self {
            path,
            file,
            map,
            num_slots,
            live: 0,
            tip: None,
            overflow: HashMap::new(),
        })
    }

    /// Open existing table (does not check store tip — caller recovers).
    pub fn open(path: impl Into<PathBuf>) -> Result<Self, StoreError> {
        let path = path.into();
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .map_err(|e| io(&path, e))?;
        let map = unsafe { MmapMut::map_mut(&file) }.map_err(|e| io(&path, e))?;
        let (tip, num_slots, live) = Self::read_header(&map)?;
        let expect = Self::file_len(num_slots);
        if (map.len() as u64) < expect {
            return Err(StoreError::Corrupt("ibd utxo file truncated"));
        }
        Ok(Self {
            path,
            file,
            map,
            num_slots,
            live,
            tip,
            overflow: HashMap::new(),
        })
    }

    /// Open if present, else create with `DEFAULT_NUM_SLOTS` (or env).
    pub fn open_or_create(dir: &Path) -> Result<Self, StoreError> {
        let path = dir.join("ibd_utxo.map");
        if path.exists() {
            Self::open(path)
        } else {
            let n = std::env::var("RBITCOIN_IBD_UTXO_SLOTS")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(DEFAULT_NUM_SLOTS);
            Self::create(path, n)
        }
    }

    pub fn tip(&self) -> Option<u32> {
        self.tip
    }

    pub fn live_count(&self) -> u64 {
        self.live
    }

    pub fn num_slots(&self) -> u64 {
        self.num_slots
    }

    pub fn load_factor(&self) -> f64 {
        if self.num_slots == 0 {
            return 0.0;
        }
        self.live as f64 / self.num_slots as f64
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    fn ensure_vout(vout: u32) -> Result<(), StoreError> {
        if vout > VOUT_MAX {
            return Err(StoreError::Corrupt("ibd utxo vout >= 2^24"));
        }
        Ok(())
    }

    /// True if outpoint is in the unspent set.
    pub fn contains(&self, txid: &[u8; 32], vout: u32) -> Result<bool, StoreError> {
        Self::ensure_vout(vout)?;
        let prefix = prefix_of(txid);
        if let Some(list) = self.overflow.get(&(prefix, vout)) {
            return Ok(list.iter().any(|t| t == txid));
        }
        let mask = self.num_slots - 1;
        let mut i = hash0(&prefix, vout) & mask;
        // Triangular probe: i + 0,1,3,6,...
        for step in 0..self.num_slots {
            let (sp, state, sv) = Self::read_slot(&self.map, self.num_slots, i);
            match state {
                STATE_EMPTY => return Ok(false),
                STATE_LIVE if sp == prefix && sv == vout => return Ok(true),
                STATE_LIVE | STATE_TOMB => {
                    i = (i + step + 1) & mask;
                }
                _ => return Err(StoreError::Corrupt("ibd utxo bad state")),
            }
        }
        Ok(false)
    }

    /// Insert a created outpoint (confirm success).
    ///
    /// Idempotent: re-insert of an already-live outpoint is `Ok` (partial apply
    /// retry after Class C, or height re-apply). Prefix collisions promote the
    /// new full txid into `overflow` (compressed slot already holds another).
    pub fn insert_create(&mut self, txid: &[u8; 32], vout: u32) -> Result<(), StoreError> {
        Self::ensure_vout(vout)?;
        if self.load_factor() >= LOAD_GROW {
            self.grow()?;
        }
        let prefix = prefix_of(txid);
        // Already tracked as unspent (exact or sole compressed occupant).
        if self.contains(txid, vout)? {
            return Ok(());
        }
        // Compressed key live / overflowed with a *different* full txid → overflow.
        if self.contains_compressed(&prefix, vout)? {
            let list = self
                .overflow
                .entry((prefix, vout))
                .or_insert_with(Vec::new);
            // Promote: first occupant was slot-only; keep slot + record new full ids.
            if !list.iter().any(|t| t == txid) {
                list.push(*txid);
            }
            return Ok(());
        }
        let mask = self.num_slots - 1;
        let mut i = hash0(&prefix, vout) & mask;
        for step in 0..self.num_slots {
            let (_sp, state, _sv) = Self::read_slot(&self.map, self.num_slots, i);
            match state {
                STATE_EMPTY | STATE_TOMB => {
                    Self::write_slot(&mut self.map, self.num_slots, i, &prefix, STATE_LIVE, vout);
                    self.live = self.live.saturating_add(1);
                    return Ok(());
                }
                STATE_LIVE => {
                    i = (i + step + 1) & mask;
                }
                _ => return Err(StoreError::Corrupt("ibd utxo bad state")),
            }
        }
        Err(StoreError::Corrupt("ibd utxo table full"))
    }

    fn contains_compressed(
        &self,
        prefix: &[u8; PREFIX_LEN],
        vout: u32,
    ) -> Result<bool, StoreError> {
        if self.overflow.contains_key(&(*prefix, vout)) {
            return Ok(true);
        }
        let mask = self.num_slots - 1;
        let mut i = hash0(prefix, vout) & mask;
        for step in 0..self.num_slots {
            let (sp, state, sv) = Self::read_slot(&self.map, self.num_slots, i);
            match state {
                STATE_EMPTY => return Ok(false),
                STATE_LIVE if sp == *prefix && sv == vout => return Ok(true),
                STATE_LIVE | STATE_TOMB => i = (i + step + 1) & mask,
                _ => return Err(StoreError::Corrupt("ibd utxo bad state")),
            }
        }
        Ok(false)
    }

    /// Remove a spent outpoint. Returns false if it was not unspent.
    pub fn take_spend(&mut self, txid: &[u8; 32], vout: u32) -> Result<bool, StoreError> {
        Self::ensure_vout(vout)?;
        let prefix = prefix_of(txid);
        if let Some(list) = self.overflow.get_mut(&(prefix, vout)) {
            if let Some(pos) = list.iter().position(|t| t == txid) {
                list.swap_remove(pos);
                if list.is_empty() {
                    self.overflow.remove(&(prefix, vout));
                    // Also clear LIVE slot if present.
                    let _ = self.tombstone_compressed(&prefix, vout)?;
                }
                return Ok(true);
            }
            return Ok(false);
        }
        let mask = self.num_slots - 1;
        let mut i = hash0(&prefix, vout) & mask;
        for step in 0..self.num_slots {
            let (sp, state, sv) = Self::read_slot(&self.map, self.num_slots, i);
            match state {
                STATE_EMPTY => return Ok(false),
                STATE_LIVE if sp == prefix && sv == vout => {
                    Self::write_slot(&mut self.map, self.num_slots, i, &prefix, STATE_TOMB, vout);
                    self.live = self.live.saturating_sub(1);
                    return Ok(true);
                }
                STATE_LIVE | STATE_TOMB => i = (i + step + 1) & mask,
                _ => return Err(StoreError::Corrupt("ibd utxo bad state")),
            }
        }
        Ok(false)
    }

    fn tombstone_compressed(
        &mut self,
        prefix: &[u8; PREFIX_LEN],
        vout: u32,
    ) -> Result<bool, StoreError> {
        let mask = self.num_slots - 1;
        let mut i = hash0(prefix, vout) & mask;
        for step in 0..self.num_slots {
            let (sp, state, sv) = Self::read_slot(&self.map, self.num_slots, i);
            match state {
                STATE_EMPTY => return Ok(false),
                STATE_LIVE if sp == *prefix && sv == vout => {
                    Self::write_slot(&mut self.map, self.num_slots, i, prefix, STATE_TOMB, vout);
                    self.live = self.live.saturating_sub(1);
                    return Ok(true);
                }
                STATE_LIVE | STATE_TOMB => i = (i + step + 1) & mask,
                _ => return Err(StoreError::Corrupt("ibd utxo bad state")),
            }
        }
        Ok(false)
    }

    /// Set reflected tip and flush header + dirty pages.
    pub fn commit_tip(&mut self, tip: Option<u32>) -> Result<(), StoreError> {
        self.tip = tip;
        Self::write_header(&mut self.map, self.tip, self.num_slots, self.live);
        self.map.flush().map_err(|e| io(&self.path, e))?;
        self.file.flush().map_err(|e| io(&self.path, e))?;
        Ok(())
    }

    /// Double capacity and reinsert all LIVE slots.
    pub fn grow(&mut self) -> Result<(), StoreError> {
        let new_slots = self.num_slots.saturating_mul(2).max(16);
        let tmp = self.path.with_extension("map.tmp");
        let mut neu = Self::create(&tmp, new_slots)?;
        // Reinsert from old map
        for i in 0..self.num_slots {
            let (prefix, state, vout) = Self::read_slot(&self.map, self.num_slots, i);
            if state != STATE_LIVE {
                continue;
            }
            // We only have prefix — reconstruct fake txid with prefix + zeros for reinsert
            // WRONG for overflow. Collect live as (prefix, vout) only works if no
            // full txid needed. insert_create needs full txid for overflow.
            // Grow path: copy slot stream by re-probing into new table with prefix-only API.
            neu.insert_live_prefix(&prefix, vout)?;
        }
        for ((prefix, vout), txids) in &self.overflow {
            for txid in txids {
                let mut t = *txid;
                // ensure prefix matches
                t[0..PREFIX_LEN].copy_from_slice(prefix);
                neu.insert_create(&t, *vout)?;
            }
        }
        neu.tip = self.tip;
        neu.live = self.live; // may recount
        // Recount live
        let mut live = 0u64;
        for i in 0..neu.num_slots {
            let (_, state, _) = Self::read_slot(&neu.map, neu.num_slots, i);
            if state == STATE_LIVE {
                live += 1;
            }
        }
        // Plus overflow extras beyond one slot each
        for list in neu.overflow.values() {
            if list.len() > 1 {
                live += (list.len() - 1) as u64;
            }
        }
        neu.live = live;
        neu.commit_tip(self.tip)?;
        // Swap files
        drop(neu);
        std::fs::rename(&tmp, &self.path).map_err(|e| io(&self.path, e))?;
        // Re-open
        let re = Self::open(&self.path)?;
        *self = re;
        Ok(())
    }

    /// Insert LIVE by prefix only (grow rehash).
    fn insert_live_prefix(
        &mut self,
        prefix: &[u8; PREFIX_LEN],
        vout: u32,
    ) -> Result<(), StoreError> {
        Self::ensure_vout(vout)?;
        let mask = self.num_slots - 1;
        let mut i = hash0(prefix, vout) & mask;
        for step in 0..self.num_slots {
            let (_, state, _) = Self::read_slot(&self.map, self.num_slots, i);
            if state == STATE_EMPTY || state == STATE_TOMB {
                Self::write_slot(&mut self.map, self.num_slots, i, prefix, STATE_LIVE, vout);
                self.live = self.live.saturating_add(1);
                return Ok(());
            }
            i = (i + step + 1) & mask;
        }
        Err(StoreError::Corrupt("ibd utxo grow full"))
    }

    /// Clear all slots (for rebuild).
    pub fn clear(&mut self) -> Result<(), StoreError> {
        self.map[HEADER_LEN..].fill(0);
        self.live = 0;
        self.tip = None;
        self.overflow.clear();
        self.commit_tip(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn tmp() -> PathBuf {
        let n = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let d = std::env::temp_dir().join(format!("rbitcoin-ibd-utxo-{n}"));
        std::fs::create_dir_all(&d).unwrap();
        d.join("ibd_utxo.map")
    }

    fn txid(seed: u8) -> [u8; 32] {
        let mut t = [0u8; 32];
        t[0] = seed;
        t[1] = seed.wrapping_add(1);
        t[11] = seed.wrapping_add(3);
        t[31] = seed.wrapping_add(9);
        t
    }

    #[test]
    fn pack_unpack_state_vout() {
        let p = pack(STATE_LIVE, 42);
        let (s, v) = unpack(p);
        assert_eq!(s, STATE_LIVE);
        assert_eq!(v, 42);
        assert!(VOUT_MAX < (1 << 24));
    }

    #[test]
    fn insert_take_double_spend() {
        let path = tmp();
        let mut u = IbdUtxo::create(&path, 1024).unwrap();
        let t = txid(7);
        u.insert_create(&t, 0).unwrap();
        assert!(u.contains(&t, 0).unwrap());
        // Idempotent re-create (partial apply retry).
        u.insert_create(&t, 0).unwrap();
        assert!(u.contains(&t, 0).unwrap());
        assert_eq!(u.live_count(), 1);
        assert!(u.take_spend(&t, 0).unwrap());
        assert!(!u.contains(&t, 0).unwrap());
        assert!(!u.take_spend(&t, 0).unwrap());
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn apply_height_monotonic_via_commit() {
        let path = tmp();
        let mut u = IbdUtxo::create(&path, 1024).unwrap();
        let t = txid(3);
        u.insert_create(&t, 0).unwrap();
        u.commit_tip(Some(5)).unwrap();
        assert_eq!(u.tip(), Some(5));
        // Re-insert after tip advance is still ok (idempotent).
        u.insert_create(&t, 0).unwrap();
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn many_inserts_and_roundtrip_file() {
        let path = tmp();
        let mut u = IbdUtxo::create(&path, 4096).unwrap();
        for i in 0..500u32 {
            let mut t = txid((i % 250) as u8);
            t[2] = (i >> 8) as u8;
            t[3] = i as u8;
            u.insert_create(&t, i % 10).unwrap();
        }
        u.commit_tip(Some(100)).unwrap();
        drop(u);
        let u2 = IbdUtxo::open(&path).unwrap();
        assert_eq!(u2.tip(), Some(100));
        assert!(u2.live_count() >= 500);
        let mut t = txid(0);
        t[2] = 0;
        t[3] = 0;
        // i=0
        assert!(u2.contains(&t, 0).unwrap());
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn rejects_huge_vout() {
        let path = tmp();
        let mut u = IbdUtxo::create(&path, 64).unwrap();
        let t = txid(1);
        assert!(u.insert_create(&t, VOUT_MAX + 1).is_err());
        let _ = std::fs::remove_file(&path);
    }
}
