//! Keyless addressable `tx.head`: fixed `2^BITS` × **4 B** entries, open-address probe.
//!
//! **Layout:** each entry is a LE `u32` create_fk (`0` = empty). No key material and
//! **no HAS_NEXT** — probe continues until an empty slot (no Class A deletes).
//! Callers verify identity via Class A body txid.
//!
//! **Probe:** double hashing from the txid (`h1` / odd `h2`), capped at
//! [`MAX_PROBE`]. Foreign occupants are normal: body mismatch ⇒ continue.
//!
//! Mainnet: BITS=31 → **8 GiB** sparse file. Tests / `HeadScale::Tiny`: BITS=16.
//!
//! **Limits:** create_fk must fit in `u32` (~4 B txs max before 8 B entries).
//! At ~3 B txs, address **BITS** needs a painful widen (e.g. 33-bit).

use crate::error::StoreError;
use crate::file::{TableFile, FILE_HEADER_LEN};
use crate::hashhead::HeadScale;
use rbitcoin_primitives::{Fk, TableKind};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

const ENTRY_SIZE: u64 = 4;
/// Hard cap — never scan the whole table.
pub const MAX_PROBE: u32 = 128;

/// Mainnet address width (2^31 slots × 4 B = 8 GiB).
pub const MAINNET_BITS: u32 = 31;
/// Tiny / unit-test width.
pub const TINY_BITS: u32 = 16;

/// Leading `bits` of the first four txid bytes (big-endian bit order).
#[inline]
pub fn h1(txid: &[u8; 32], bits: u32) -> u64 {
    debug_assert!((1..=31).contains(&bits));
    let v = u32::from_be_bytes([txid[0], txid[1], txid[2], txid[3]]);
    u64::from(v >> (32 - bits))
}

/// Odd step in `0..2^bits` from the next four txid bytes.
#[inline]
pub fn h2(txid: &[u8; 32], bits: u32) -> u64 {
    debug_assert!((1..=31).contains(&bits));
    let mask = (1u64 << bits) - 1;
    (u64::from(u32::from_be_bytes([txid[4], txid[5], txid[6], txid[7]])) | 1) & mask
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
            if (8..=31).contains(&n) {
                return n;
            }
            rbitcoin_log::warn!(
                "store: RBITCOIN_TX_HEAD_BITS={s:?} out of 8..=31, using scale default"
            );
        }
    }
    match HeadScale::from_env() {
        HeadScale::Tiny => TINY_BITS,
        HeadScale::Mainnet => MAINNET_BITS,
    }
}

#[inline]
fn fk_to_u32(fk: Fk) -> Result<u32, StoreError> {
    if fk.is_null() || fk.0 > u64::from(u32::MAX) {
        return Err(StoreError::InvalidFk);
    }
    Ok(fk.0 as u32)
}

/// Fixed-width keyless txid → dense create_fk table (single file, 4 B slots).
pub struct AddressHead {
    file: TableFile,
    bits: u32,
    slots: u64,
    occupied: AtomicU64,
    write_lock: Mutex<()>,
}

impl AddressHead {
    pub fn create(path: impl Into<PathBuf>) -> Result<Self, StoreError> {
        Self::create_with_bits(path, bits_for_scale())
    }

    pub fn create_with_bits(path: impl Into<PathBuf>, bits: u32) -> Result<Self, StoreError> {
        let path = path.into();
        if !(8..=31).contains(&bits) {
            return Err(StoreError::Corrupt("address head bits out of range"));
        }
        if path.exists() && path.is_dir() {
            return Err(StoreError::Corrupt(
                "tx.head is a directory (legacy shards); wipe datadir for address head",
            ));
        }
        let slots = 1u64 << bits;
        let file = TableFile::create(&path, TableKind::HashHead)?;
        let body_bytes = slots * ENTRY_SIZE;
        let need = FILE_HEADER_LEN as u64 + body_bytes;
        file.ensure_capacity(need)?;
        file.set_logical_len(need)?;
        file.zero_range(FILE_HEADER_LEN as u64, body_bytes)?;
        if bits >= 24 {
            rbitcoin_log::info!(
                "store: address-head create path={} bits={} slots={} entry=4B (~{:.2} GiB sparse)",
                file.path().display(),
                bits,
                slots,
                body_bytes as f64 / (1024.0 * 1024.0 * 1024.0)
            );
        }
        Ok(Self {
            file,
            bits,
            slots,
            occupied: AtomicU64::new(0),
            write_lock: Mutex::new(()),
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
        if body % ENTRY_SIZE != 0 || body == 0 {
            return Err(StoreError::Corrupt("address head size"));
        }
        let slots = body / ENTRY_SIZE;
        if !slots.is_power_of_two() || slots < 256 {
            return Err(StoreError::Corrupt("address head slots not power of two"));
        }
        let bits = slots.trailing_zeros();
        if bits > 31 {
            return Err(StoreError::Corrupt("address head bits > 31"));
        }
        // Reject legacy 8 B-entry tables (same slot count would be 2× body size).
        // Opened files with 8 B entries have body = slots*8; we only accept *4.
        let occupied = count_occupied(&file, slots)?;
        Ok(Self {
            file,
            bits,
            slots,
            occupied: AtomicU64::new(occupied),
            write_lock: Mutex::new(()),
        })
    }

    pub fn bits(&self) -> u32 {
        self.bits
    }

    pub fn slots(&self) -> u64 {
        self.slots
    }

    pub fn occupied(&self) -> u64 {
        self.occupied.load(Ordering::Relaxed)
    }

    #[inline]
    fn entry_off(slot: u64) -> u64 {
        FILE_HEADER_LEN as u64 + slot * ENTRY_SIZE
    }

    fn read_entry(&self, slot: u64) -> Result<u32, StoreError> {
        let mut buf = [0u8; 4];
        self.file.read_at(Self::entry_off(slot), &mut buf)?;
        Ok(u32::from_le_bytes(buf))
    }

    fn write_entry(&self, slot: u64, e: u32) -> Result<(), StoreError> {
        self.file
            .write_at(Self::entry_off(slot), &e.to_le_bytes())
    }

    pub fn reserve_additional(&self, _additional: u64) -> Result<(), StoreError> {
        Ok(())
    }

    /// Insert one mapping. `body_txid(fk)` must return the Class A body txid.
    ///
    /// BIP30: newest fk at earliest matching probe slot; older pushed deeper.
    pub fn insert(
        &self,
        txid: &[u8; 32],
        new_fk: Fk,
        mut body_txid: impl FnMut(Fk) -> Result<[u8; 32], StoreError>,
    ) -> Result<(), StoreError> {
        let _ = fk_to_u32(new_fk)?;
        let _guard = self.write_lock.lock().unwrap();
        self.insert_locked(txid, new_fk, &mut body_txid)
    }

    fn insert_locked(
        &self,
        txid: &[u8; 32],
        new_fk: Fk,
        body_txid: &mut dyn FnMut(Fk) -> Result<[u8; 32], StoreError>,
    ) -> Result<(), StoreError> {
        let mut to_place = new_fk;

        for d in 0..MAX_PROBE {
            let slot = probe_index(txid, d, self.bits);
            let e = self.read_entry(slot)?;
            if e == 0 {
                self.write_entry(slot, fk_to_u32(to_place)?)?;
                self.occupied.fetch_add(1, Ordering::Relaxed);
                return Ok(());
            }
            let cur_fk = Fk(u64::from(e));
            if cur_fk.0 == to_place.0 || cur_fk.0 == new_fk.0 {
                return Ok(());
            }
            let bt = body_txid(cur_fk)?;
            if &bt == txid {
                // BIP30: newest takes this slot; displace older.
                self.write_entry(slot, fk_to_u32(to_place)?)?;
                to_place = cur_fk;
                continue;
            }
            // Foreigner — continue until empty.
        }
        Err(StoreError::Corrupt("address head probe exhausted on insert"))
    }

    pub fn insert_many(
        &self,
        entries: &[([u8; 32], Fk)],
        mut body_txid: impl FnMut(Fk) -> Result<[u8; 32], StoreError>,
    ) -> Result<(), StoreError> {
        if entries.is_empty() {
            return Ok(());
        }
        let mut work = entries.to_vec();
        let bits = self.bits;
        work.sort_unstable_by_key(|(txid, _)| probe_index(txid, 0, bits));
        let _guard = self.write_lock.lock().unwrap();
        for (txid, fk) in &work {
            self.insert_locked(txid, *fk, &mut body_txid)?;
        }
        Ok(())
    }

    pub fn insert_many_paced(
        &self,
        entries: &[([u8; 32], Fk)],
        body_txid: impl FnMut(Fk) -> Result<[u8; 32], StoreError>,
    ) -> Result<(), StoreError> {
        self.insert_many(entries, body_txid)
    }

    /// Walk probe until empty; return every fk (may include foreigners).
    pub fn probe_fks(&self, txid: &[u8; 32]) -> Result<Vec<Fk>, StoreError> {
        let mut out = Vec::new();
        for d in 0..MAX_PROBE {
            let slot = probe_index(txid, d, self.bits);
            let e = self.read_entry(slot)?;
            if e == 0 {
                break;
            }
            out.push(Fk(u64::from(e)));
        }
        Ok(out)
    }

    pub fn get_all_candidates(&self, txid: &[u8; 32]) -> Result<Vec<Fk>, StoreError> {
        self.probe_fks(txid)
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
}

fn count_occupied(file: &TableFile, slots: u64) -> Result<u64, StoreError> {
    const SCAN_SLOT_CAP: u64 = 1 << 22; // 4 M slots ≈ 16 MiB at 4 B
    if slots > SCAN_SLOT_CAP {
        rbitcoin_log::debug!(
            "store: address-head open slots={slots} — skip full occupied scan (cap {SCAN_SLOT_CAP})"
        );
        return Ok(0);
    }
    let mut occupied = 0u64;
    const CHUNK: usize = 4096;
    let mut buf = vec![0u8; CHUNK * ENTRY_SIZE as usize];
    let mut slot = 0u64;
    while slot < slots {
        let n = ((slots - slot) as usize).min(CHUNK);
        let off = FILE_HEADER_LEN as u64 + slot * ENTRY_SIZE;
        let bytes = n * ENTRY_SIZE as usize;
        file.read_at(off, &mut buf[..bytes])?;
        for i in 0..n {
            let e = u32::from_le_bytes(buf[i * 4..i * 4 + 4].try_into().unwrap());
            if e != 0 {
                occupied += 1;
            }
        }
        slot += n as u64;
    }
    Ok(occupied)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};

    fn tmp(name: &str) -> PathBuf {
        static N: AtomicU64 = AtomicU64::new(0);
        let id = N.fetch_add(1, AtomicOrdering::Relaxed);
        let p = std::env::temp_dir().join(format!("rbitcoin-addr-head-{name}-{id}"));
        let _ = std::fs::remove_dir_all(&p);
        let _ = std::fs::remove_file(&p);
        p
    }

    fn body_map(m: &HashMap<u64, [u8; 32]>) -> impl FnMut(Fk) -> Result<[u8; 32], StoreError> + '_ {
        move |fk| {
            m.get(&fk.0)
                .copied()
                .ok_or(StoreError::Corrupt("missing body in test map"))
        }
    }

    #[test]
    fn probe_stable() {
        let k = [0xabu8; 32];
        assert_eq!(probe_index(&k, 0, 16), probe_index(&k, 0, 16));
        assert_ne!(probe_index(&k, 0, 16), probe_index(&k, 1, 16));
        assert!(probe_index(&k, 0, 16) < (1 << 16));
    }

    #[test]
    fn insert_get_roundtrip() {
        let path = tmp("roundtrip");
        let h = AddressHead::create_with_bits(&path, 12).unwrap();
        let mut bodies = HashMap::new();
        let mut txid = [0u8; 32];
        txid[0] = 1;
        bodies.insert(1, txid);
        h.insert(&txid, Fk(1), body_map(&bodies)).unwrap();
        assert_eq!(h.probe_fks(&txid).unwrap(), vec![Fk(1)]);
        assert_eq!(h.occupied(), 1);
        let _ = std::fs::remove_file(&path);
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
        let mut bodies = HashMap::new();
        bodies.insert(1, a);
        bodies.insert(2, b);
        h.insert(&a, Fk(1), body_map(&bodies)).unwrap();
        h.insert(&b, Fk(2), body_map(&bodies)).unwrap();
        assert!(h.probe_fks(&a).unwrap().contains(&Fk(1)));
        assert!(h.probe_fks(&b).unwrap().contains(&Fk(2)));
        assert_eq!(h.probe_fks(&a).unwrap()[0], Fk(1));
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn bip30_newest_first() {
        let path = tmp("bip30");
        let h = AddressHead::create_with_bits(&path, 12).unwrap();
        let mut txid = [0u8; 32];
        txid[0] = 0x55;
        let mut bodies = HashMap::new();
        bodies.insert(1, txid);
        bodies.insert(2, txid);
        h.insert(&txid, Fk(1), body_map(&bodies)).unwrap();
        h.insert(&txid, Fk(2), body_map(&bodies)).unwrap();
        let cands = h.probe_fks(&txid).unwrap();
        assert_eq!(cands[0], Fk(2));
        assert!(cands.contains(&Fk(1)));
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn rejects_fk_above_u32() {
        let path = tmp("bigu32");
        let h = AddressHead::create_with_bits(&path, 12).unwrap();
        let txid = [1u8; 32];
        let err = h
            .insert(&txid, Fk(u64::from(u32::MAX) + 1), |_| Ok(txid))
            .unwrap_err();
        assert!(matches!(err, StoreError::InvalidFk));
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn miss_empty() {
        let path = tmp("miss");
        let h = AddressHead::create_with_bits(&path, 12).unwrap();
        assert!(h.probe_fks(&[9u8; 32]).unwrap().is_empty());
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn reopen() {
        let path = tmp("reopen");
        {
            let h = AddressHead::create_with_bits(&path, 12).unwrap();
            let mut bodies = HashMap::new();
            let txid = [7u8; 32];
            bodies.insert(3, txid);
            h.insert(&txid, Fk(3), body_map(&bodies)).unwrap();
            h.flush().unwrap();
        }
        let h = AddressHead::open(&path).unwrap();
        assert_eq!(h.bits(), 12);
        assert_eq!(h.occupied(), 1);
        assert_eq!(h.probe_fks(&[7u8; 32]).unwrap(), vec![Fk(3)]);
        let _ = std::fs::remove_file(&path);
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
        let mut bodies = HashMap::new();
        let mut entries = Vec::new();
        for i in 1..=50u64 {
            let mut txid = [0u8; 32];
            txid[0] = (i & 0xff) as u8;
            txid[1] = ((i >> 8) & 0xff) as u8;
            txid[4] = (i * 3) as u8;
            bodies.insert(i, txid);
            entries.push((txid, Fk(i)));
        }
        h.insert_many(&entries, body_map(&bodies)).unwrap();
        assert_eq!(h.occupied(), 50);
        for (txid, fk) in &entries {
            assert!(h.probe_fks(txid).unwrap().contains(fk));
        }
        let _ = std::fs::remove_file(&path);
    }
}
