//! Keyless addressable `tx.head`: `2^BITS` slots × 4 B or 8 B create_fk entries.
//!
//! **Layout:** each entry is LE create_fk (`0` = empty). No key material and
//! **no HAS_NEXT** — probe continues until an empty slot (no Class A deletes).
//! Callers verify identity via Class A body txid on **lookup**.
//!
//! **Insert (fast path):** probe until the **same fk** is already present (idempotent)
//! or an **empty** slot — **CAS** `0 → fk` there (no whole-map insert lock).
//! **No body_txid** on insert (no BIP30 displacement on write). Foreigners and
//! older same-txid creates are skipped blindly; a second Class A row for the same
//! txid lands at the next empty slot (deeper on the probe chain). Concurrent
//! inserts of different keys only serialize at colliding empty slots via CAS.
//! A process-wide `write_lock` remains only for online resize final catch-up/swap.
//!
//! **Lookup:** walk candidates from the **last occupied** probe slot toward the
//! first, body-verify — so the deepest same-txid create wins (newest under
//! append-deeper insert).
//!
//! **Probe:** double hashing from the txid (`h1` / odd `h2`), capped at
//! [`MAX_PROBE`]. Foreign occupants are normal on lookup: body mismatch ⇒ continue.
//!
//! **Mainnet default:** BITS=28 → **1 GiB** sparse @ 4 B entries. Online resize
//! widens BITS (sequential rebuild from `tx.idx`); entry width becomes 8 B at
//! BITS ≥ 33. Load trigger: [`HEAD_LOAD_START`] (0.80).

use crate::error::StoreError;
use crate::file::{TableFile, FILE_HEADER_LEN};
use crate::hashhead::HeadScale;
use rbitcoin_primitives::{Fk, TableKind};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

/// Hard cap — never scan the whole table.
pub const MAX_PROBE: u32 = 128;

/// Mainnet address width (2^28 slots × 4 B = 1 GiB sparse). Online resize grows.
pub const MAINNET_BITS: u32 = 28;
/// Tiny / unit-test width.
pub const TINY_BITS: u32 = 16;
/// Maximum supported address width (probe + create).
pub const MAX_BITS: u32 = 34;
/// Minimum supported address width.
pub const MIN_BITS: u32 = 8;

/// Start sequential rebuild when `txs.count() / slots >=` this.
pub const HEAD_LOAD_START: f64 = 0.80;
/// Warn while resizing if load reaches this.
pub const HEAD_LOAD_WARN: f64 = 0.85;
/// Soft ceiling (align open-address 7/8); avoid dwelling here.
pub const HEAD_LOAD_CEILING: f64 = 0.875;

const META_MAGIC: &[u8; 4] = b"THM1";
const META_VERSION: u16 = 1;

/// On-disk / in-memory address-head geometry.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct HeadLayout {
    pub bits: u32,
    /// 4 (create_fk as u32) or 8 (create_fk as u64).
    pub entry_bytes: u8,
}

impl HeadLayout {
    pub fn new(bits: u32) -> Result<Self, StoreError> {
        if !(MIN_BITS..=MAX_BITS).contains(&bits) {
            return Err(StoreError::Corrupt("address head bits out of range"));
        }
        Ok(Self {
            bits,
            entry_bytes: entry_bytes_for_bits(bits),
        })
    }

    pub fn with_entry_bytes(bits: u32, entry_bytes: u8) -> Result<Self, StoreError> {
        if !(MIN_BITS..=MAX_BITS).contains(&bits) {
            return Err(StoreError::Corrupt("address head bits out of range"));
        }
        if entry_bytes != 4 && entry_bytes != 8 {
            return Err(StoreError::Corrupt("address head entry_bytes must be 4 or 8"));
        }
        // BITS ≥ 33 requires 8 B (u32 fk space insufficient at 0.80 load).
        if bits >= 33 && entry_bytes != 8 {
            return Err(StoreError::Corrupt(
                "address head bits>=33 requires 8-byte entries",
            ));
        }
        Ok(Self { bits, entry_bytes })
    }

    pub fn slots(&self) -> u64 {
        1u64 << self.bits
    }

    pub fn entry_size(&self) -> u64 {
        u64::from(self.entry_bytes)
    }

    pub fn body_bytes(&self) -> u64 {
        self.slots() * self.entry_size()
    }
}

/// Entry width policy: 8 B starting at BITS 33 (capacity exceeds u32 create_fk).
#[inline]
pub fn entry_bytes_for_bits(bits: u32) -> u8 {
    if bits >= 33 {
        8
    } else {
        4
    }
}

/// Leading `bits` of the txid as a big-endian bit stream (supports bits up to 34).
#[inline]
pub fn h1(txid: &[u8; 32], bits: u32) -> u64 {
    debug_assert!((MIN_BITS..=MAX_BITS).contains(&bits));
    // First 8 bytes cover up to 64 bits; we only need ≤34.
    let v = u64::from_be_bytes([
        txid[0], txid[1], txid[2], txid[3], txid[4], txid[5], txid[6], txid[7],
    ]);
    v >> (64 - bits)
}

/// Odd step in `0..2^bits` from bytes after the h1 region.
#[inline]
pub fn h2(txid: &[u8; 32], bits: u32) -> u64 {
    debug_assert!((MIN_BITS..=MAX_BITS).contains(&bits));
    let mask = (1u64 << bits) - 1;
    // Use bytes 4..12 so low-bit tables still get entropy when h1 took top of 0..8.
    let v = u64::from_be_bytes([
        txid[4], txid[5], txid[6], txid[7], txid[8], txid[9], txid[10], txid[11],
    ]);
    (v | 1) & mask
}

/// Probe index at depth `d` (double hashing).
#[inline]
pub fn probe_index(txid: &[u8; 32], d: u32, bits: u32) -> u64 {
    let mask = (1u64 << bits) - 1;
    let h1 = h1(txid, bits);
    let h2 = h2(txid, bits);
    h1.wrapping_add(u64::from(d).wrapping_mul(h2)) & mask
}

/// Resolve address width for new creates.
pub fn bits_for_scale() -> u32 {
    if let Ok(s) = std::env::var("RBITCOIN_TX_HEAD_BITS") {
        if let Ok(n) = s.parse::<u32>() {
            if (MIN_BITS..=MAX_BITS).contains(&n) {
                return n;
            }
            rbitcoin_log::warn!(
                "store: RBITCOIN_TX_HEAD_BITS={s:?} out of {MIN_BITS}..={MAX_BITS}, using scale default"
            );
        }
    }
    match HeadScale::from_env() {
        HeadScale::Tiny => TINY_BITS,
        HeadScale::Mainnet => MAINNET_BITS,
    }
}

pub fn default_layout() -> HeadLayout {
    HeadLayout::new(bits_for_scale()).expect("default bits in range")
}

/// True when dense Class A count warrants a BITS widen.
#[inline]
pub fn load_needs_resize(tx_count: u64, slots: u64) -> bool {
    if slots == 0 {
        return false;
    }
    // n >= ceil(slots * HEAD_LOAD_START)
    let threshold = ((slots as f64) * HEAD_LOAD_START).ceil() as u64;
    tx_count >= threshold
}

#[inline]
pub fn load_ratio(tx_count: u64, slots: u64) -> f64 {
    if slots == 0 {
        return 0.0;
    }
    tx_count as f64 / slots as f64
}

fn meta_path(head_path: &Path) -> PathBuf {
    let mut p = head_path.as_os_str().to_os_string();
    p.push(".meta");
    PathBuf::from(p)
}

pub fn write_head_meta(head_path: &Path, layout: HeadLayout, generation: u64) -> Result<(), StoreError> {
    let path = meta_path(head_path);
    let mut buf = [0u8; 16];
    buf[0..4].copy_from_slice(META_MAGIC);
    buf[4..6].copy_from_slice(&META_VERSION.to_le_bytes());
    buf[6] = layout.bits as u8;
    buf[7] = layout.entry_bytes;
    buf[8..16].copy_from_slice(&generation.to_le_bytes());
    std::fs::write(&path, buf).map_err(|e| StoreError::io(&path, e))
}

pub fn read_head_meta(head_path: &Path) -> Result<Option<(HeadLayout, u64)>, StoreError> {
    let path = meta_path(head_path);
    if !path.exists() {
        return Ok(None);
    }
    let raw = std::fs::read(&path).map_err(|e| StoreError::io(&path, e))?;
    if raw.len() < 16 || &raw[0..4] != META_MAGIC {
        return Err(StoreError::Corrupt("tx.head.meta magic"));
    }
    let ver = u16::from_le_bytes([raw[4], raw[5]]);
    if ver != META_VERSION {
        return Err(StoreError::Corrupt("tx.head.meta version"));
    }
    let bits = u32::from(raw[6]);
    let entry_bytes = raw[7];
    let generation = u64::from_le_bytes(raw[8..16].try_into().unwrap());
    let layout = HeadLayout::with_entry_bytes(bits, entry_bytes)?;
    Ok(Some((layout, generation)))
}

/// Fixed-width keyless txid → dense create_fk table.
pub struct AddressHead {
    file: TableFile,
    layout: HeadLayout,
    slots: u64,
    occupied: AtomicU64,
    write_lock: Mutex<()>,
    generation: u64,
}

impl AddressHead {
    pub fn create(path: impl Into<PathBuf>) -> Result<Self, StoreError> {
        Self::create_with_layout(path, default_layout())
    }

    pub fn create_with_bits(path: impl Into<PathBuf>, bits: u32) -> Result<Self, StoreError> {
        Self::create_with_layout(path, HeadLayout::new(bits)?)
    }

    pub fn create_with_layout(
        path: impl Into<PathBuf>,
        layout: HeadLayout,
    ) -> Result<Self, StoreError> {
        let path = path.into();
        if path.exists() && path.is_dir() {
            return Err(StoreError::Corrupt(
                "tx.head is a directory (legacy shards); wipe datadir for address head",
            ));
        }
        let slots = layout.slots();
        let file = TableFile::create(&path, TableKind::HashHead)?;
        let body_bytes = layout.body_bytes();
        let need = FILE_HEADER_LEN as u64 + body_bytes;
        file.ensure_capacity(need)?;
        file.set_logical_len(need)?;
        file.zero_range(FILE_HEADER_LEN as u64, body_bytes)?;
        write_head_meta(&path, layout, 0)?;
        if layout.bits >= 24 {
            rbitcoin_log::info!(
                "store: address-head create path={} bits={} slots={} entry={}B (~{:.2} GiB sparse)",
                file.path().display(),
                layout.bits,
                slots,
                layout.entry_bytes,
                body_bytes as f64 / (1024.0 * 1024.0 * 1024.0)
            );
        }
        Ok(Self {
            file,
            layout,
            slots,
            occupied: AtomicU64::new(0),
            write_lock: Mutex::new(()),
            generation: 0,
        })
    }

    pub fn open(path: impl Into<PathBuf>) -> Result<Self, StoreError> {
        let path = path.into();
        if path.is_dir() {
            return Err(StoreError::Corrupt(
                "tx.head is a directory (legacy shards); wipe datadir for address head",
            ));
        }
        let file = TableFile::open(&path, TableKind::HashHead)?;
        let body = file.logical_len().saturating_sub(FILE_HEADER_LEN as u64);
        if body == 0 {
            return Err(StoreError::Corrupt("address head size"));
        }

        let (layout, generation) = if let Some((layout, gen)) = read_head_meta(&path)? {
            let expect = layout.body_bytes();
            if body != expect {
                return Err(StoreError::Corrupt(
                    "address head size mismatch vs tx.head.meta",
                ));
            }
            (layout, gen)
        } else {
            // Legacy: 4 B entries, infer bits from body.
            if body % 4 != 0 {
                return Err(StoreError::Corrupt("address head size (legacy 4B)"));
            }
            let slots = body / 4;
            if !slots.is_power_of_two() || slots < 256 {
                return Err(StoreError::Corrupt("address head slots not power of two"));
            }
            let bits = slots.trailing_zeros();
            if !(MIN_BITS..=MAX_BITS).contains(&bits) {
                return Err(StoreError::Corrupt("address head bits out of range"));
            }
            // Write meta so future opens are unambiguous.
            let layout = HeadLayout::with_entry_bytes(bits, 4)?;
            write_head_meta(&path, layout, 0)?;
            (layout, 0)
        };

        let slots = layout.slots();
        let occupied = count_occupied(&file, slots, layout.entry_bytes)?;
        Ok(Self {
            file,
            layout,
            slots,
            occupied: AtomicU64::new(occupied),
            write_lock: Mutex::new(()),
            generation,
        })
    }

    pub fn layout(&self) -> HeadLayout {
        self.layout
    }

    pub fn bits(&self) -> u32 {
        self.layout.bits
    }

    pub fn entry_bytes(&self) -> u8 {
        self.layout.entry_bytes
    }

    pub fn slots(&self) -> u64 {
        self.slots
    }

    pub fn occupied(&self) -> u64 {
        self.occupied.load(Ordering::Relaxed)
    }

    pub fn generation(&self) -> u64 {
        self.generation
    }

    #[inline]
    fn entry_off(&self, slot: u64) -> u64 {
        FILE_HEADER_LEN as u64 + slot * self.layout.entry_size()
    }

    fn read_entry(&self, slot: u64) -> Result<u64, StoreError> {
        match self.layout.entry_bytes {
            4 => {
                let mut buf = [0u8; 4];
                self.file.read_at(self.entry_off(slot), &mut buf)?;
                Ok(u64::from(u32::from_le_bytes(buf)))
            }
            8 => {
                let mut buf = [0u8; 8];
                self.file.read_at(self.entry_off(slot), &mut buf)?;
                Ok(u64::from_le_bytes(buf))
            }
            _ => Err(StoreError::Corrupt("address head entry_bytes")),
        }
    }

    fn encode_fk(&self, fk: Fk) -> Result<u64, StoreError> {
        if fk.is_null() {
            return Err(StoreError::InvalidFk);
        }
        if self.layout.entry_bytes == 4 && fk.0 > u64::from(u32::MAX) {
            return Err(StoreError::InvalidFk);
        }
        Ok(fk.0)
    }

    pub fn reserve_additional(&self, _additional: u64) -> Result<(), StoreError> {
        Ok(())
    }

    /// Insert one mapping (no body IO). Lock-free vs other inserts: CAS empty→fk.
    pub fn insert(&self, txid: &[u8; 32], new_fk: Fk) -> Result<(), StoreError> {
        self.insert_cas(txid, new_fk)
    }

    /// CAS empty slot → fk; idempotent if `new_fk` already on the probe chain.
    fn insert_cas(&self, txid: &[u8; 32], new_fk: Fk) -> Result<(), StoreError> {
        let new_u = self.encode_fk(new_fk)?;
        for d in 0..MAX_PROBE {
            let slot = probe_index(txid, d, self.layout.bits);
            let e = self.read_entry(slot)?;
            if e == new_u {
                return Ok(());
            }
            if e != 0 {
                continue;
            }
            // Empty: claim with CAS (another inserter may win the slot).
            if self.cas_entry(slot, 0, new_u)? {
                self.occupied.fetch_add(1, Ordering::Relaxed);
                return Ok(());
            }
            // Lost race — re-check; same fk is success, else keep probing.
            let e2 = self.read_entry(slot)?;
            if e2 == new_u {
                return Ok(());
            }
        }
        Err(StoreError::Corrupt("address head probe exhausted on insert"))
    }

    /// Compare-and-swap one head entry (`expected` → `new`).
    fn cas_entry(&self, slot: u64, expected: u64, new: u64) -> Result<bool, StoreError> {
        let off = self.entry_off(slot);
        match self.layout.entry_bytes {
            4 => {
                if expected > u64::from(u32::MAX) || new > u64::from(u32::MAX) {
                    return Err(StoreError::InvalidFk);
                }
                self.file
                    .cas_u32_le(off, expected as u32, new as u32)
            }
            8 => self.file.cas_u64_le(off, expected, new),
            _ => Err(StoreError::Corrupt("address head entry_bytes")),
        }
    }

    /// Bulk insert; primary-slot sort for locality. No body IO.
    ///
    /// Concurrent-safe with other inserts (per-slot CAS). Does **not** take
    /// [`lock_writes`] — that is only for resize swap.
    pub fn insert_many(&self, entries: &[([u8; 32], Fk)]) -> Result<(), StoreError> {
        if entries.is_empty() {
            return Ok(());
        }
        let mut work = entries.to_vec();
        let bits = self.layout.bits;
        work.sort_unstable_by_key(|(txid, _)| probe_index(txid, 0, bits));
        for (txid, fk) in &work {
            self.insert_cas(txid, *fk)?;
        }
        Ok(())
    }

    pub fn insert_many_paced(&self, entries: &[([u8; 32], Fk)]) -> Result<(), StoreError> {
        self.insert_many(entries)
    }

    /// Walk probe until empty; return every fk (may include foreigners).
    pub fn probe_fks(&self, txid: &[u8; 32]) -> Result<Vec<Fk>, StoreError> {
        let mut out = Vec::new();
        for d in 0..MAX_PROBE {
            let slot = probe_index(txid, d, self.layout.bits);
            let e = self.read_entry(slot)?;
            if e == 0 {
                break;
            }
            out.push(Fk(e));
        }
        Ok(out)
    }

    pub fn get_all_candidates(&self, txid: &[u8; 32]) -> Result<Vec<Fk>, StoreError> {
        self.probe_fks(txid)
    }

    pub fn mlock_probe(&self, txid: &[u8; 32]) -> Result<(u64, u64), StoreError> {
        let mut min_off = u64::MAX;
        let mut max_end = 0u64;
        let mut any = false;
        let es = self.layout.entry_size();
        for d in 0..MAX_PROBE {
            let slot = probe_index(txid, d, self.layout.bits);
            let off = self.entry_off(slot);
            min_off = min_off.min(off);
            max_end = max_end.max(off + es);
            any = true;
            let e = self.read_entry(slot)?;
            if e == 0 {
                break;
            }
        }
        if !any || min_off == u64::MAX {
            return Ok((0, 0));
        }
        self.file.mlock_range(min_off, max_end - min_off)
    }

    pub fn munlock_pages(&self, page_start: u64, page_len: u64) {
        self.file.munlock_range(page_start, page_len);
    }

    pub fn flush(&self) -> Result<(), StoreError> {
        self.file.flush()
    }

    pub fn flush_async(&self) -> Result<(), StoreError> {
        self.file.flush_async()
    }

    pub fn path(&self) -> &Path {
        self.file.path()
    }

    /// Exclusive barrier for online resize final catch-up + swap only.
    ///
    /// Steady-state inserts use per-slot CAS and do **not** take this lock.
    pub fn lock_writes(&self) -> std::sync::MutexGuard<'_, ()> {
        self.write_lock.lock().unwrap()
    }
}

fn count_occupied(file: &TableFile, slots: u64, entry_bytes: u8) -> Result<u64, StoreError> {
    let es = u64::from(entry_bytes);
    const SCAN_BYTE_CAP: u64 = 16 * 1024 * 1024; // 16 MiB
    if slots * es > SCAN_BYTE_CAP {
        rbitcoin_log::debug!(
            "store: address-head open slots={slots} entry={entry_bytes}B — skip full occupied scan"
        );
        return Ok(0);
    }
    let mut occupied = 0u64;
    const CHUNK: usize = 4096;
    let mut buf = vec![0u8; CHUNK * entry_bytes as usize];
    let mut slot = 0u64;
    while slot < slots {
        let n = ((slots - slot) as usize).min(CHUNK);
        let off = FILE_HEADER_LEN as u64 + slot * es;
        let bytes = n * entry_bytes as usize;
        file.read_at(off, &mut buf[..bytes])?;
        for i in 0..n {
            let empty = match entry_bytes {
                4 => {
                    let e = u32::from_le_bytes(buf[i * 4..i * 4 + 4].try_into().unwrap());
                    e == 0
                }
                8 => {
                    let e = u64::from_le_bytes(buf[i * 8..i * 8 + 8].try_into().unwrap());
                    e == 0
                }
                _ => return Err(StoreError::Corrupt("address head entry_bytes")),
            };
            if !empty {
                occupied += 1;
            }
        }
        slot += n as u64;
    }
    Ok(occupied)
}

// ── Resize control file ─────────────────────────────────────────────────────

/// In-progress sequential rebuild control (`tx.head.resize`).
#[derive(Clone, Debug)]
pub struct ResizeControl {
    pub target: HeadLayout,
    pub cursor: u64,
    pub generation: u64,
}

fn resize_control_path(head_path: &Path) -> PathBuf {
    let mut p = head_path.as_os_str().to_os_string();
    p.push(".resize");
    PathBuf::from(p)
}

pub fn write_resize_control(head_path: &Path, c: &ResizeControl) -> Result<(), StoreError> {
    let path = resize_control_path(head_path);
    // THR1 | ver:u16 | bits:u8 | entry:u8 | cursor:u64 | generation:u64
    let mut buf = [0u8; 24];
    buf[0..4].copy_from_slice(b"THR1");
    buf[4..6].copy_from_slice(&1u16.to_le_bytes());
    buf[6] = c.target.bits as u8;
    buf[7] = c.target.entry_bytes;
    buf[8..16].copy_from_slice(&c.cursor.to_le_bytes());
    buf[16..24].copy_from_slice(&c.generation.to_le_bytes());
    std::fs::write(&path, buf).map_err(|e| StoreError::io(&path, e))
}

pub fn read_resize_control(head_path: &Path) -> Result<Option<ResizeControl>, StoreError> {
    let path = resize_control_path(head_path);
    if !path.exists() {
        return Ok(None);
    }
    let raw = std::fs::read(&path).map_err(|e| StoreError::io(&path, e))?;
    if raw.len() < 24 || &raw[0..4] != b"THR1" {
        return Err(StoreError::Corrupt("tx.head.resize magic"));
    }
    let target = HeadLayout::with_entry_bytes(u32::from(raw[6]), raw[7])?;
    let cursor = u64::from_le_bytes(raw[8..16].try_into().unwrap());
    let generation = u64::from_le_bytes(raw[16..24].try_into().unwrap());
    Ok(Some(ResizeControl {
        target,
        cursor,
        generation,
    }))
}

pub fn clear_resize_control(head_path: &Path) {
    let path = resize_control_path(head_path);
    let _ = std::fs::remove_file(path);
}

pub fn shadow_head_path(head_path: &Path) -> PathBuf {
    let mut p = head_path.as_os_str().to_os_string();
    p.push(".new");
    PathBuf::from(p)
}

pub fn bak_head_path(head_path: &Path) -> PathBuf {
    let mut p = head_path.as_os_str().to_os_string();
    p.push(".bak");
    PathBuf::from(p)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};

    fn tmp(name: &str) -> PathBuf {
        static N: AtomicU64 = AtomicU64::new(0);
        let id = N.fetch_add(1, AtomicOrdering::Relaxed);
        let p = std::env::temp_dir().join(format!("rbitcoin-addr-head-{name}-{id}"));
        let _ = std::fs::remove_dir_all(&p);
        let _ = std::fs::remove_file(&p);
        let meta = meta_path(&p);
        let _ = std::fs::remove_file(&meta);
        p
    }

    #[test]
    fn probe_stable() {
        let k = [0xabu8; 32];
        assert_eq!(probe_index(&k, 0, 16), probe_index(&k, 0, 16));
        assert_ne!(probe_index(&k, 0, 16), probe_index(&k, 1, 16));
        assert!(probe_index(&k, 0, 16) < (1 << 16));
    }

    #[test]
    fn probe_bits_28_to_34_in_range() {
        let k = [0x11u8; 32];
        for bits in [28u32, 31, 32, 33, 34] {
            let idx = probe_index(&k, 0, bits);
            assert!(idx < (1u64 << bits), "bits={bits} idx={idx}");
            let idx2 = probe_index(&k, 7, bits);
            assert!(idx2 < (1u64 << bits));
        }
    }

    #[test]
    fn entry_bytes_policy() {
        assert_eq!(entry_bytes_for_bits(28), 4);
        assert_eq!(entry_bytes_for_bits(32), 4);
        assert_eq!(entry_bytes_for_bits(33), 8);
        assert_eq!(entry_bytes_for_bits(34), 8);
    }

    #[test]
    fn load_trigger_at_80_percent() {
        let slots = 1024u64;
        let thr = ((slots as f64) * HEAD_LOAD_START).ceil() as u64;
        assert!(!load_needs_resize(thr - 1, slots));
        assert!(load_needs_resize(thr, slots));
        assert!(load_needs_resize(slots, slots));
    }

    #[test]
    fn insert_get_roundtrip() {
        let path = tmp("roundtrip");
        let h = AddressHead::create_with_bits(&path, 12).unwrap();
        let mut txid = [0u8; 32];
        txid[0] = 1;
        h.insert(&txid, Fk(1)).unwrap();
        assert_eq!(h.probe_fks(&txid).unwrap(), vec![Fk(1)]);
        assert_eq!(h.occupied(), 1);
        h.insert(&txid, Fk(1)).unwrap();
        assert_eq!(h.occupied(), 1);
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(meta_path(&path));
    }

    #[test]
    fn eight_byte_entries_accept_fk_above_u32() {
        let path = tmp("u64fk");
        let layout = HeadLayout::with_entry_bytes(12, 8).unwrap();
        let h = AddressHead::create_with_layout(&path, layout).unwrap();
        assert_eq!(h.entry_bytes(), 8);
        let txid = [2u8; 32];
        let big = Fk(u64::from(u32::MAX) + 99);
        h.insert(&txid, big).unwrap();
        assert_eq!(h.probe_fks(&txid).unwrap(), vec![big]);
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(meta_path(&path));
    }

    #[test]
    fn foreigner_collision_both_found() {
        let path = tmp("foreigner");
        let h = AddressHead::create_with_bits(&path, 8).unwrap();
        let mut a = [0u8; 32];
        let mut b = [0u8; 32];
        a[0] = 0x10;
        b[0] = 0x10;
        b[4] = 0x02;
        h.insert(&a, Fk(1)).unwrap();
        h.insert(&b, Fk(2)).unwrap();
        assert!(h.probe_fks(&a).unwrap().contains(&Fk(1)));
        assert!(h.probe_fks(&b).unwrap().contains(&Fk(2)));
        assert_eq!(h.probe_fks(&a).unwrap()[0], Fk(1));
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(meta_path(&path));
    }

    #[test]
    fn bip30_second_create_appends_deeper() {
        let path = tmp("bip30");
        let h = AddressHead::create_with_bits(&path, 12).unwrap();
        let mut txid = [0u8; 32];
        txid[0] = 0x55;
        h.insert(&txid, Fk(1)).unwrap();
        h.insert(&txid, Fk(2)).unwrap();
        let cands = h.probe_fks(&txid).unwrap();
        assert_eq!(cands[0], Fk(1), "first insert stays at earliest slot");
        assert!(cands.contains(&Fk(2)));
        assert_eq!(h.occupied(), 2);
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(meta_path(&path));
    }

    #[test]
    fn rejects_fk_above_u32_on_4b() {
        let path = tmp("bigu32");
        let h = AddressHead::create_with_bits(&path, 12).unwrap();
        let txid = [1u8; 32];
        let err = h
            .insert(&txid, Fk(u64::from(u32::MAX) + 1))
            .unwrap_err();
        assert!(matches!(err, StoreError::InvalidFk));
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(meta_path(&path));
    }

    #[test]
    fn miss_empty() {
        let path = tmp("miss");
        let h = AddressHead::create_with_bits(&path, 12).unwrap();
        assert!(h.probe_fks(&[9u8; 32]).unwrap().is_empty());
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(meta_path(&path));
    }

    #[test]
    fn reopen_with_meta() {
        let path = tmp("reopen");
        {
            let h = AddressHead::create_with_bits(&path, 12).unwrap();
            let txid = [7u8; 32];
            h.insert(&txid, Fk(3)).unwrap();
            h.flush().unwrap();
        }
        let h = AddressHead::open(&path).unwrap();
        assert_eq!(h.bits(), 12);
        assert_eq!(h.entry_bytes(), 4);
        assert_eq!(h.occupied(), 1);
        assert_eq!(h.probe_fks(&[7u8; 32]).unwrap(), vec![Fk(3)]);
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(meta_path(&path));
    }

    #[test]
    fn reject_v7_directory() {
        let path = tmp("v7dir");
        std::fs::create_dir(&path).unwrap();
        match AddressHead::open(&path) {
            Err(StoreError::Corrupt(_)) => {}
            Err(e) => panic!("expected Corrupt, got {e}"),
            Ok(_) => panic!("expected error opening v7 directory"),
        }
        let _ = std::fs::remove_dir_all(&path);
    }

    #[test]
    fn insert_many_batch() {
        let path = tmp("batch");
        let h = AddressHead::create_with_bits(&path, 14).unwrap();
        let mut entries = Vec::new();
        for i in 1..=50u64 {
            let mut txid = [0u8; 32];
            txid[0] = (i & 0xff) as u8;
            txid[1] = ((i >> 8) & 0xff) as u8;
            txid[4] = (i * 3) as u8;
            entries.push((txid, Fk(i)));
        }
        h.insert_many(&entries).unwrap();
        assert_eq!(h.occupied(), 50);
        for (txid, fk) in &entries {
            assert!(h.probe_fks(txid).unwrap().contains(fk));
        }
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(meta_path(&path));
    }

    #[test]
    fn concurrent_inserts_and_probes_all_found() {
        use std::sync::{Arc, Barrier};
        use std::thread;

        let path = tmp("cas_conc");
        let h = Arc::new(AddressHead::create_with_bits(&path, 16).unwrap());
        let n = 200u64;
        let barrier = Arc::new(Barrier::new(5));
        let mut handles = Vec::new();

        // Four inserter threads, disjoint fk ranges.
        for t in 0..4u64 {
            let h = Arc::clone(&h);
            let barrier = Arc::clone(&barrier);
            handles.push(thread::spawn(move || {
                barrier.wait();
                let start = t * (n / 4) + 1;
                let end = if t == 3 { n } else { (t + 1) * (n / 4) };
                let mut batch = Vec::new();
                for i in start..=end {
                    let mut txid = [0u8; 32];
                    txid[0] = (i & 0xff) as u8;
                    txid[1] = ((i >> 8) & 0xff) as u8;
                    txid[2] = 0xca;
                    batch.push((txid, Fk(i)));
                }
                h.insert_many(&batch).unwrap();
            }));
        }

        // Prober while inserts run.
        {
            let h = Arc::clone(&h);
            let barrier = Arc::clone(&barrier);
            handles.push(thread::spawn(move || {
                barrier.wait();
                for _ in 0..500 {
                    let mut txid = [0u8; 32];
                    txid[0] = 1;
                    txid[2] = 0xca;
                    let _ = h.probe_fks(&txid);
                }
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }

        assert_eq!(h.occupied(), n);
        for i in 1..=n {
            let mut txid = [0u8; 32];
            txid[0] = (i & 0xff) as u8;
            txid[1] = ((i >> 8) & 0xff) as u8;
            txid[2] = 0xca;
            assert!(
                h.probe_fks(&txid).unwrap().contains(&Fk(i)),
                "missing fk {i}"
            );
        }
        // Idempotent re-insert of a subset.
        let mut again = Vec::new();
        for i in 1..=20u64 {
            let mut txid = [0u8; 32];
            txid[0] = (i & 0xff) as u8;
            txid[1] = ((i >> 8) & 0xff) as u8;
            txid[2] = 0xca;
            again.push((txid, Fk(i)));
        }
        h.insert_many(&again).unwrap();
        assert_eq!(h.occupied(), n);

        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(meta_path(&path));
    }

    #[test]
    fn mainnet_default_bits_is_28() {
        assert_eq!(MAINNET_BITS, 28);
        assert_eq!(entry_bytes_for_bits(MAINNET_BITS), 4);
    }
}
