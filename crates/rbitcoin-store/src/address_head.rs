//! Keyless addressable `tx.head`: `2^BITS` slots × 4 B or 8 B create_fk entries.
//!
//! **Layout:** each entry is LE create_fk (`0` = empty). No key material and
//! **no HAS_NEXT** — probe continues until an empty slot (no Class A deletes).
//! Callers verify identity via Class A body txid on **lookup**.
//!
//! **Insert (sole writer):** probe until the **same fk** is already present
//! (idempotent) or an **empty** slot — plain **Release** store `0 → fk` (no CAS).
//! **No body_txid** on insert (no BIP30 displacement on write). Foreigners and
//! older same-txid creates are skipped blindly; a second Class A row for the same
//! txid lands at the next empty slot (deeper on the probe chain).
//!
//! **Concurrency:** at most **one** thread may insert into a given `tx.head`
//! (archive writer in IBD; single tip accept path after). Multi-writer races are
//! not supported. After each insert batch, a **SeqCst fence** publishes stores for
//! concurrent readers (Acquire loads on probe). Online resize still uses
//! `write_lock` only for final catch-up + file swap.
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
    pub(crate) fn entry_off(&self, slot: u64) -> u64 {
        FILE_HEADER_LEN as u64 + slot * self.layout.entry_size()
    }

    /// Read one open-address entry (0 = empty). Used by sequential and bulk probe.
    pub(crate) fn read_entry(&self, slot: u64) -> Result<u64, StoreError> {
        let off = self.entry_off(slot);
        match self.layout.entry_bytes {
            4 => Ok(u64::from(self.file.load_u32_le(off)?)),
            8 => self.file.load_u64_le(off),
            _ => Err(StoreError::Corrupt("address head entry_bytes")),
        }
    }

    /// FD for bulk io_uring / pread of head entries.
    #[inline]
    pub(crate) fn read_fd(&self) -> std::os::fd::RawFd {
        self.file.read_fd()
    }

    /// Published head file length (bounds bulk reads).
    #[inline]
    pub(crate) fn published_len(&self) -> u64 {
        self.file.logical_len()
    }

    #[inline]
    pub(crate) fn path_str(&self) -> &std::path::Path {
        self.file.path()
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

    /// Insert one mapping (no body IO). Sole writer: Release store into empty slot.
    pub fn insert(&self, txid: &[u8; 32], new_fk: Fk) -> Result<(), StoreError> {
        self.insert_many(&[(*txid, new_fk)])
    }

    /// Unconditional empty-slot store (Release). Sole-writer only.
    fn store_entry(&self, slot: u64, new: u64) -> Result<(), StoreError> {
        let off = self.entry_off(slot);
        match self.layout.entry_bytes {
            4 => {
                if new > u64::from(u32::MAX) {
                    return Err(StoreError::InvalidFk);
                }
                self.file.store_u32_le(off, new as u32)
            }
            8 => self.file.store_u64_le(off, new),
            _ => Err(StoreError::Corrupt("address head entry_bytes")),
        }
    }

    /// Sole-writer insert into first empty probe slot (no CAS).
    ///
    /// Idempotent if `new_fk` is already on the chain. Requires at most one
    /// concurrent inserter process-wide for this head.
    fn insert_one(&self, txid: &[u8; 32], new_fk: Fk) -> Result<(), StoreError> {
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
            self.store_entry(slot, new_u)?;
            self.occupied.fetch_add(1, Ordering::Relaxed);
            return Ok(());
        }
        Err(StoreError::Corrupt("address head probe exhausted on insert"))
    }

    /// Bulk insert in **call order** (no sort). **SeqCst fence** at end so
    /// concurrent Acquire probes observe the batch.
    ///
    /// When io_uring is enabled, uses wave-pipelined **pread probe + pwrite store**
    /// so the kernel can schedule many head slots at once (archive write path).
    /// Falls back to sequential mmap Release stores when uring is off.
    ///
    /// Does **not** take [`lock_writes`] — that is only for resize swap.
    pub fn insert_many(&self, entries: &[([u8; 32], Fk)]) -> Result<(), StoreError> {
        if entries.is_empty() {
            return Ok(());
        }
        if crate::bulk_io::io_uring_enabled() {
            return self.insert_many_uring(entries);
        }
        for (txid, fk) in entries {
            self.insert_one(txid, *fk)?;
        }
        // Publish the batch for readers (pairs with Acquire loads in read_entry).
        std::sync::atomic::fence(Ordering::SeqCst);
        Ok(())
    }

    /// io_uring bulk insert: many probe preads per wave, then empty-slot pwrites.
    ///
    /// Sole writer only (same contract as [`insert_one`]). Completions may finish
    /// out of order within a wave; each key still walks probe depths in order.
    fn insert_many_uring(&self, entries: &[([u8; 32], Fk)]) -> Result<(), StoreError> {
        use crate::bulk_io::{self, ReadOp, WriteOp};

        let fd = self.read_fd();
        let bits = self.layout.bits;
        let entry_bytes = self.layout.entry_bytes;
        let es = u64::from(entry_bytes);
        let published = self.published_len();
        let path = self.path_str();

        struct Key {
            txid: [u8; 32],
            new_u: u64,
            /// Next probe depth to issue (if not done).
            depth: u8,
            done: bool,
            /// True if we issued a successful empty-slot write (count for occupied).
            wrote: bool,
        }

        let mut keys: Vec<Key> = Vec::with_capacity(entries.len());
        for (txid, fk) in entries {
            let new_u = self.encode_fk(*fk)?;
            keys.push(Key {
                txid: *txid,
                new_u,
                depth: 0,
                done: false,
                wrote: false,
            });
        }

        // Pending probe work: (key_i, depth). Seed depth 0 for every key.
        let mut need_probe: Vec<(u32, u8)> = (0..keys.len() as u32).map(|i| (i, 0)).collect();

        while !need_probe.is_empty() {
            // --- probe wave ---
            let n = need_probe.len();
            let mut arena = vec![0u8; n * entry_bytes as usize];
            let mut offs = vec![0u64; n];
            {
                for (i, &(ki, depth)) in need_probe.iter().enumerate() {
                    let slot = probe_index(&keys[ki as usize].txid, u32::from(depth), bits);
                    let off = FILE_HEADER_LEN as u64 + slot * es;
                    if off.saturating_add(es) > published {
                        return Err(StoreError::Corrupt("head insert probe past published"));
                    }
                    offs[i] = off;
                }
            }
            let mut read_ops: Vec<ReadOp<'_>> = Vec::with_capacity(n);
            {
                let mut rest = arena.as_mut_slice();
                for i in 0..n {
                    let (left, right) = rest.split_at_mut(entry_bytes as usize);
                    rest = right;
                    read_ops.push(ReadOp {
                        fd,
                        offset: offs[i],
                        buf: left,
                        result: i32::MIN,
                    });
                }
                bulk_io::pread_batch(&mut read_ops);
            }

            // Collect empty slots to write this round.
            struct PendWrite {
                key_i: u32,
                off: u64,
                bytes: [u8; 8],
            }
            let mut pending_writes: Vec<PendWrite> = Vec::new();
            let mut next_probe: Vec<(u32, u8)> = Vec::new();

            for (i, ro) in read_ops.iter().enumerate() {
                let (ki, depth) = need_probe[i];
                let k = &mut keys[ki as usize];
                if k.done {
                    continue;
                }
                if ro.result < 0 {
                    return Err(StoreError::io(
                        path,
                        std::io::Error::from_raw_os_error(-ro.result),
                    ));
                }
                if ro.result as usize != entry_bytes as usize {
                    return Err(StoreError::Corrupt("head insert probe pread short"));
                }
                let e = match entry_bytes {
                    4 => u64::from(u32::from_le_bytes(ro.buf[..4].try_into().unwrap())),
                    8 => u64::from_le_bytes(ro.buf[..8].try_into().unwrap()),
                    _ => return Err(StoreError::Corrupt("address head entry_bytes")),
                };
                if e == k.new_u {
                    // Idempotent: already present.
                    k.done = true;
                    continue;
                }
                if e != 0 {
                    // Foreigner — deeper slot.
                    let nd = depth.saturating_add(1);
                    if u32::from(nd) >= MAX_PROBE {
                        return Err(StoreError::Corrupt(
                            "address head probe exhausted on insert",
                        ));
                    }
                    next_probe.push((ki, nd));
                    k.depth = nd;
                    continue;
                }
                // Empty: store here.
                let mut bytes = [0u8; 8];
                match entry_bytes {
                    4 => bytes[..4].copy_from_slice(&(k.new_u as u32).to_le_bytes()),
                    8 => bytes[..8].copy_from_slice(&k.new_u.to_le_bytes()),
                    _ => unreachable!(),
                }
                pending_writes.push(PendWrite {
                    key_i: ki,
                    off: offs[i],
                    bytes,
                });
            }

            // --- write wave (empty slots) ---
            if !pending_writes.is_empty() {
                let wlen = entry_bytes as usize;
                let mut warena = vec![0u8; pending_writes.len() * wlen];
                for (i, pw) in pending_writes.iter().enumerate() {
                    warena[i * wlen..i * wlen + wlen]
                        .copy_from_slice(&pw.bytes[..wlen]);
                }
                let mut write_ops: Vec<WriteOp<'_>> = Vec::with_capacity(pending_writes.len());
                let mut rest = warena.as_slice();
                let mut pieces: Vec<&[u8]> = Vec::with_capacity(pending_writes.len());
                for _ in 0..pending_writes.len() {
                    let (left, right) = rest.split_at(wlen);
                    pieces.push(left);
                    rest = right;
                }
                for (piece, pw) in pieces.into_iter().zip(pending_writes.iter()) {
                    write_ops.push(WriteOp {
                        fd,
                        offset: pw.off,
                        buf: piece,
                        result: i32::MIN,
                    });
                }
                bulk_io::pwrite_batch(&mut write_ops);
                for (wo, pw) in write_ops.iter().zip(pending_writes.iter()) {
                    if wo.result < 0 {
                        return Err(StoreError::io(
                            path,
                            std::io::Error::from_raw_os_error(-wo.result),
                        ));
                    }
                    if wo.result as usize != wlen {
                        return Err(StoreError::Corrupt("head insert pwrite short"));
                    }
                    let k = &mut keys[pw.key_i as usize];
                    k.done = true;
                    k.wrote = true;
                }
            }

            need_probe = next_probe;
        }

        if keys.iter().any(|k| !k.done) {
            return Err(StoreError::Corrupt("address head bulk insert incomplete"));
        }
        let n_new = keys.iter().filter(|k| k.wrote).count() as u64;
        if n_new > 0 {
            self.occupied.fetch_add(n_new, Ordering::Relaxed);
        }
        // Publish: pairs with Acquire loads on probe (mmap or pread readers).
        std::sync::atomic::fence(Ordering::SeqCst);
        Ok(())
    }

    pub fn insert_many_paced(&self, entries: &[([u8; 32], Fk)]) -> Result<(), StoreError> {
        self.insert_many(entries)
    }

    /// Alias of [`insert_many`] (historical archive name).
    #[inline]
    pub fn insert_many_sole(&self, entries: &[([u8; 32], Fk)]) -> Result<(), StoreError> {
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
    /// Steady-state sole-writer inserts do **not** take this lock.
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
    fn insert_many_sole_no_sort_roundtrip() {
        let path = tmp("sole");
        let h = AddressHead::create_with_bits(&path, 14).unwrap();
        let mut entries = Vec::new();
        // Unsorted / reverse-ish order (no primary-slot sort on sole path).
        for i in (1..=80u64).rev() {
            let mut txid = [0u8; 32];
            txid[0] = (i & 0xff) as u8;
            txid[1] = ((i >> 8) & 0xff) as u8;
            txid[3] = 0x5e;
            entries.push((txid, Fk(i)));
        }
        h.insert_many_sole(&entries).unwrap();
        assert_eq!(h.occupied(), 80);
        // Idempotent re-insert.
        h.insert_many_sole(&entries[..10]).unwrap();
        assert_eq!(h.occupied(), 80);
        for (txid, fk) in &entries {
            assert!(
                h.probe_fks(txid).unwrap().contains(fk),
                "missing after sole insert"
            );
        }
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(meta_path(&path));
    }

    /// Sole writer + concurrent probes (no multi-inserter).
    #[test]
    fn sole_writer_with_concurrent_probes_all_found() {
        use std::sync::{Arc, Barrier};
        use std::thread;

        let path = tmp("sole_probe");
        let h = Arc::new(AddressHead::create_with_bits(&path, 16).unwrap());
        let n = 200u64;
        let barrier = Arc::new(Barrier::new(2));

        let prober = {
            let h = Arc::clone(&h);
            let barrier = Arc::clone(&barrier);
            thread::spawn(move || {
                barrier.wait();
                for _ in 0..2000 {
                    let mut txid = [0u8; 32];
                    txid[0] = 1;
                    txid[2] = 0xca;
                    let _ = h.probe_fks(&txid);
                }
            })
        };

        barrier.wait();
        // Single inserter, batched (fences between batches).
        let mut batch = Vec::new();
        for i in 1..=n {
            let mut txid = [0u8; 32];
            txid[0] = (i & 0xff) as u8;
            txid[1] = ((i >> 8) & 0xff) as u8;
            txid[2] = 0xca;
            batch.push((txid, Fk(i)));
            if batch.len() >= 32 {
                h.insert_many(&batch).unwrap();
                batch.clear();
            }
        }
        if !batch.is_empty() {
            h.insert_many(&batch).unwrap();
        }
        prober.join().unwrap();

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
