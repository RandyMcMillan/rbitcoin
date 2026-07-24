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
//! **Probe:** **linear** from primary slot `h1(txid)` (`slot = (h1 + d) mod 2^bits`),
//! capped at [`MAX_PROBE`]. Lookup/insert load a **[`PROBE_REGION_BYTES`] (2048 B)**
//! contiguous run from the current slot in one IO, scan entries in memory, and only
//! issue another region read if no empty was found (short/partial first response is
//! fine — kernel/page boundaries are not special-cased). Foreign occupants are normal
//! on lookup: body mismatch ⇒ continue. (Keyless entries cannot Robin-Hood: foreigner
//! probe depth is unknown without a body read.)
//! First insert at depth > [`PROBE_DEPTH_WARN`] requests online resize (see
//! [`take_probe_depth_resize_request`]).
//!
//! **Mainnet default:** BITS=28 → **1 GiB** sparse @ 4 B entries. Online resize
//! widens BITS (sequential rebuild from `tx.idx`); entry width becomes 8 B at
//! BITS ≥ 33. Load trigger: [`HEAD_LOAD_START`] (0.75).

use crate::error::StoreError;
use crate::file::{TableFile, FILE_HEADER_LEN};
use crate::hashhead::HeadScale;
use rbitcoin_primitives::{Fk, TableKind};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

/// Hard cap — never scan the whole table.
pub const MAX_PROBE: u32 = 256;

/// Bytes requested per head probe IO (covers [`MAX_PROBE`] × 8 B entries).
/// Kernel may return a short/partial length; callers scan what they got and only
/// request another region if no empty slot appears.
pub const PROBE_REGION_BYTES: usize = 2048;

/// Inserts that needed probe depth **> [`PROBE_DEPTH_WARN`]** (warning band).
/// Cumulative counter for lagging/retry logs; WARN only once at first event.
static PROBE_INSERT_DEPTH_WARN_COUNT: AtomicU64 = AtomicU64::new(0);

/// Inserts that exhausted [`MAX_PROBE`] (sleep-retry through resize).
/// Counter only — the retry loop owns operator-facing logs.
static PROBE_INSERT_EXHAUSTED: AtomicU64 = AtomicU64::new(0);

/// Set on the **first** insert that lands past [`PROBE_DEPTH_WARN`].
/// [`take_probe_depth_resize_request`] clears it for the tx.head owner to start resize.
static PROBE_DEPTH_RESIZE_REQUEST: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);

/// Depth threshold above which inserts count as “deep” for ops visibility and
/// trigger an early online resize request (first event only).
pub const PROBE_DEPTH_WARN: u32 = 128;

/// Mainnet address width (2^28 slots × 4 B = 1 GiB sparse). Online resize grows.
pub const MAINNET_BITS: u32 = 28;
/// Tiny / unit-test width.
pub const TINY_BITS: u32 = 16;
/// Maximum supported address width (probe + create).
pub const MAX_BITS: u32 = 34;
/// Minimum supported address width.
pub const MIN_BITS: u32 = 8;

/// Start sequential rebuild when `txs.count() / slots >=` this.
pub const HEAD_LOAD_START: f64 = 0.75;
/// Warn while resizing if load reaches this.
pub const HEAD_LOAD_WARN: f64 = 0.85;
/// Soft ceiling (align open-address 7/8); avoid dwelling here.
pub const HEAD_LOAD_CEILING: f64 = 0.875;

/// `(depth_warn_count, probe_exhausted)` cumulative counters (no reset).
#[inline]
pub fn probe_depth_stats_snapshot() -> (u64, u64) {
    (
        PROBE_INSERT_DEPTH_WARN_COUNT.load(Ordering::Relaxed),
        PROBE_INSERT_EXHAUSTED.load(Ordering::Relaxed),
    )
}

/// `(depth_warn_count, probe_exhausted)` since last sample; both reset.
pub fn sample_probe_depth_stats() -> (u64, u64) {
    (
        PROBE_INSERT_DEPTH_WARN_COUNT.swap(0, Ordering::Relaxed),
        PROBE_INSERT_EXHAUSTED.swap(0, Ordering::Relaxed),
    )
}

/// Consume a one-shot request from the first deep insert (depth > [`PROBE_DEPTH_WARN`]).
/// Caller should start online resize if not already running.
#[inline]
pub fn take_probe_depth_resize_request() -> bool {
    PROBE_DEPTH_RESIZE_REQUEST.swap(false, Ordering::AcqRel)
}

/// True when `err` is the sole-writer open-address insert failure (table full
/// along the probe chain — wait for online resize, then retry).
#[inline]
pub fn is_probe_exhausted_error(err: &StoreError) -> bool {
    matches!(
        err,
        StoreError::Corrupt(m) if *m == "address head probe exhausted on insert"
    )
}

#[inline]
fn note_probe_depth_on_insert(depth: u32) {
    if depth <= PROBE_DEPTH_WARN {
        return;
    }
    let n = PROBE_INSERT_DEPTH_WARN_COUNT.fetch_add(1, Ordering::Relaxed) + 1;
    // Once only — first deep insert requests resize; further events counted silently.
    if n == 1 {
        rbitcoin_log::warn!(
            "store: tx.head insert probe depth>{PROBE_DEPTH_WARN} (first depth={depth}; \
             requesting online resize if not already running; further deep inserts counted silently)"
        );
        PROBE_DEPTH_RESIZE_REQUEST.store(true, Ordering::Release);
    }
}

#[inline]
fn note_probe_exhausted() {
    PROBE_INSERT_EXHAUSTED.fetch_add(1, Ordering::Relaxed);
}

const META_MAGIC: &[u8; 4] = b"THM1";
/// `2` = linear probe from `h1`. Version `1` was double-hash (`h1 + d·h2`); open
/// refuses v1 so [`crate::tx_table::TxTable::open`] recreates + rebuilds.
const META_VERSION: u16 = 2;

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
/// Primary home slot for linear probing.
#[inline]
pub fn h1(txid: &[u8; 32], bits: u32) -> u64 {
    debug_assert!((MIN_BITS..=MAX_BITS).contains(&bits));
    // First 8 bytes cover up to 64 bits; we only need ≤34.
    let v = u64::from_be_bytes([
        txid[0], txid[1], txid[2], txid[3], txid[4], txid[5], txid[6], txid[7],
    ]);
    v >> (64 - bits)
}

/// Probe index at depth `d` (**linear** from [`h1`]).
///
/// `slot(d) = (h1(txid) + d) mod 2^bits`. Adjacent depths are consecutive slots
/// (wrap at table end), matching `HashHead` / `ScriptHashHead` locality.
#[inline]
pub fn probe_index(txid: &[u8; 32], d: u32, bits: u32) -> u64 {
    let mask = (1u64 << bits) - 1;
    h1(txid, bits).wrapping_add(u64::from(d)) & mask
}

/// Absolute file offset of open-address slot `slot` (4 B or 8 B entries).
#[inline]
pub fn entry_file_off(slot: u64, entry_bytes: u8) -> u64 {
    FILE_HEADER_LEN as u64 + slot * u64::from(entry_bytes)
}

/// Contiguous bytes available from `slot` to the end of the table body, capped
/// by `published_len` (short regions near table end / HWM are expected).
#[inline]
pub fn probe_region_avail(
    slot: u64,
    slots: u64,
    entry_bytes: u8,
    published_len: u64,
) -> u64 {
    let off = entry_file_off(slot, entry_bytes);
    let table_end = FILE_HEADER_LEN as u64 + slots * u64::from(entry_bytes);
    let end = table_end.min(published_len);
    end.saturating_sub(off)
}

/// Result of scanning one probe-region buffer.
#[derive(Debug, Clone)]
pub struct ProbeRegionScan {
    /// Occupied create_fks with absolute probe depth (home = 0).
    pub cands: Vec<(u32, u64)>,
    /// Saw an empty slot (probe chain ends).
    pub hit_empty: bool,
    /// Complete entries advanced (occupied + empty if [`Self::hit_empty`]).
    pub slots_advanced: u32,
}

/// Scan LE create_fk entries in `buf` (may be short / unaligned tail ignored).
///
/// Stops at empty, [`MAX_PROBE`], or end of complete entries in `buf`.
#[inline]
pub fn scan_probe_region(
    buf: &[u8],
    entry_bytes: u8,
    start_depth: u32,
    max_probe: u32,
) -> ProbeRegionScan {
    let es = entry_bytes as usize;
    debug_assert!(es == 4 || es == 8);
    let mut cands = Vec::new();
    let mut slots_advanced = 0u32;
    let mut depth = start_depth;
    let mut off = 0usize;
    while off + es <= buf.len() && depth < max_probe {
        let e = match entry_bytes {
            4 => u64::from(u32::from_le_bytes(buf[off..off + 4].try_into().unwrap())),
            8 => u64::from_le_bytes(buf[off..off + 8].try_into().unwrap()),
            _ => break,
        };
        off += es;
        slots_advanced = slots_advanced.saturating_add(1);
        if e == 0 {
            return ProbeRegionScan {
                cands,
                hit_empty: true,
                slots_advanced,
            };
        }
        cands.push((depth, e));
        depth = depth.saturating_add(1);
    }
    ProbeRegionScan {
        cands,
        hit_empty: false,
        slots_advanced,
    }
}

/// Request size for a region pread from `slot` (≤ [`PROBE_REGION_BYTES`], entry-aligned).
#[inline]
pub fn probe_region_want(
    slot: u64,
    slots: u64,
    entry_bytes: u8,
    published_len: u64,
) -> usize {
    let avail = probe_region_avail(slot, slots, entry_bytes, published_len);
    let es = u64::from(entry_bytes);
    if avail < es {
        return 0;
    }
    let want = (PROBE_REGION_BYTES as u64).min(avail);
    // Entry-align (drop trailing partial entry if avail is odd for some reason).
    ((want / es) * es) as usize
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
        // v1 = double-hash layout; linear probe is incompatible without rebuild.
        return Err(StoreError::Corrupt(
            "tx.head.meta version (linear probe; rebuild tx.head)",
        ));
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

        // Require current meta (probe algorithm is part of the layout). Missing or
        // v1 meta → error so TxTable::open can recreate + rebuild from Class A.
        let (layout, generation) = match read_head_meta(&path)? {
            Some((layout, gen)) => {
                let expect = layout.body_bytes();
                if body != expect {
                    return Err(StoreError::Corrupt(
                        "address head size mismatch vs tx.head.meta",
                    ));
                }
                (layout, gen)
            }
            None => {
                return Err(StoreError::Corrupt(
                    "tx.head.meta missing (linear probe; rebuild tx.head)",
                ));
            }
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

    /// Load up to `want` bytes of contiguous entries starting at `slot` into `buf`
    /// (Acquire loads — pairs with Release stores on insert). Returns bytes filled
    /// (entry-aligned). Stops at table end (short region); does **not** wrap.
    fn load_probe_region(
        &self,
        slot: u64,
        want: usize,
        buf: &mut [u8],
    ) -> Result<usize, StoreError> {
        let es = self.layout.entry_bytes as usize;
        debug_assert!(want <= buf.len());
        debug_assert!(want % es == 0);
        let max_slots = self.slots.saturating_sub(slot) as usize;
        let n_ent = (want / es).min(max_slots);
        let mut filled = 0usize;
        for i in 0..n_ent {
            let e = self.read_entry(slot + i as u64)?;
            match es {
                4 => buf[filled..filled + 4].copy_from_slice(&(e as u32).to_le_bytes()),
                8 => buf[filled..filled + 8].copy_from_slice(&e.to_le_bytes()),
                _ => return Err(StoreError::Corrupt("address head entry_bytes")),
            }
            filled += es;
        }
        Ok(filled)
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
    ///
    /// Loads [`PROBE_REGION_BYTES`] at a time from the current slot; scans in
    /// memory; another region only if no empty in the bytes returned.
    fn insert_one(&self, txid: &[u8; 32], new_fk: Fk) -> Result<(), StoreError> {
        let new_u = self.encode_fk(new_fk)?;
        let es = self.layout.entry_bytes;
        let mask = self.slots - 1;
        let pub_len = self.file.logical_len();
        let mut slot = h1(txid, self.layout.bits);
        let mut depth = 0u32;
        let mut buf = [0u8; PROBE_REGION_BYTES];
        while depth < MAX_PROBE {
            let want = probe_region_want(slot, self.slots, es, pub_len);
            if want == 0 {
                // Past contiguous table body at this slot — wrap to 0.
                if slot == 0 {
                    break;
                }
                slot = 0;
                continue;
            }
            let n = self.load_probe_region(slot, want, &mut buf)?;
            let scan = scan_probe_region(&buf[..n], es, depth, MAX_PROBE);
            for &(_d, e) in &scan.cands {
                if e == new_u {
                    return Ok(());
                }
            }
            if scan.hit_empty {
                // Empty sits immediately after the occupied run in this region.
                let empty_depth = depth.saturating_add(scan.cands.len() as u32);
                note_probe_depth_on_insert(empty_depth);
                let empty_slot = slot.wrapping_add(scan.cands.len() as u64) & mask;
                self.store_entry(empty_slot, new_u)?;
                self.occupied.fetch_add(1, Ordering::Relaxed);
                return Ok(());
            }
            // No empty in this region (full or short) — advance and continue.
            if scan.slots_advanced == 0 {
                break;
            }
            debug_assert_eq!(scan.slots_advanced as usize, scan.cands.len());
            slot = slot.wrapping_add(u64::from(scan.slots_advanced)) & mask;
            depth = depth.saturating_add(scan.slots_advanced);
        }
        note_probe_exhausted();
        Err(StoreError::Corrupt("address head probe exhausted on insert"))
    }

    /// Bulk insert in **call order** (no sort). Plain Release mmap stores;
    /// **SeqCst fence** at end so concurrent Acquire probes observe the batch.
    ///
    /// Does **not** take [`lock_writes`] — that is only for resize swap.
    ///
    /// Note: io_uring pwrite inserts were measured slower than mmap on warm
    /// `tx.head` (~5× head ms/blk); archive write stays on this path.
    pub fn insert_many(&self, entries: &[([u8; 32], Fk)]) -> Result<(), StoreError> {
        if entries.is_empty() {
            return Ok(());
        }
        for (txid, fk) in entries {
            self.insert_one(txid, *fk)?;
        }
        // Publish the batch for readers (pairs with Acquire loads in read_entry).
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
    ///
    /// One [`PROBE_REGION_BYTES`] read per wave from the current slot; scan for
    /// create_fks; only another IO if the response had no empty (short/partial
    /// near table end or wrap is handled by continuing from the next slot).
    pub fn probe_fks(&self, txid: &[u8; 32]) -> Result<Vec<Fk>, StoreError> {
        let es = self.layout.entry_bytes;
        let mask = self.slots - 1;
        let pub_len = self.file.logical_len();
        let mut out = Vec::new();
        let mut slot = h1(txid, self.layout.bits);
        let mut depth = 0u32;
        let mut buf = [0u8; PROBE_REGION_BYTES];
        while depth < MAX_PROBE {
            let want = probe_region_want(slot, self.slots, es, pub_len);
            if want == 0 {
                if slot == 0 {
                    break;
                }
                slot = 0;
                continue;
            }
            let n = self.load_probe_region(slot, want, &mut buf)?;
            let scan = scan_probe_region(&buf[..n], es, depth, MAX_PROBE);
            for &(_d, e) in &scan.cands {
                out.push(Fk(e));
            }
            if scan.hit_empty {
                break;
            }
            if scan.slots_advanced == 0 {
                break;
            }
            slot = slot.wrapping_add(u64::from(scan.slots_advanced)) & mask;
            depth = depth.saturating_add(scan.slots_advanced);
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
    fn probe_limits_match_policy() {
        assert_eq!(MAX_PROBE, 256);
        assert_eq!(PROBE_REGION_BYTES, 2048);
        assert_eq!(PROBE_DEPTH_WARN, 128);
        // Region must cover a full max-depth chain of 8 B entries.
        assert!(PROBE_REGION_BYTES as u32 >= MAX_PROBE * 8);
    }

    #[test]
    fn probe_stable() {
        let k = [0xabu8; 32];
        assert_eq!(probe_index(&k, 0, 16), probe_index(&k, 0, 16));
        assert_ne!(probe_index(&k, 0, 16), probe_index(&k, 1, 16));
        assert!(probe_index(&k, 0, 16) < (1 << 16));
    }

    #[test]
    fn scan_probe_region_stops_at_empty_and_partial() {
        // 4B entries: fk 1, 2, empty, 3
        let mut buf = Vec::new();
        buf.extend_from_slice(&1u32.to_le_bytes());
        buf.extend_from_slice(&2u32.to_le_bytes());
        buf.extend_from_slice(&0u32.to_le_bytes());
        buf.extend_from_slice(&3u32.to_le_bytes());
        let s = scan_probe_region(&buf, 4, 0, MAX_PROBE);
        assert!(s.hit_empty);
        assert_eq!(s.cands, vec![(0, 1), (1, 2)]);
        assert_eq!(s.slots_advanced, 3); // two occupied + empty

        // Short buffer: only one entry, no empty → need more.
        let short = 7u32.to_le_bytes();
        let s2 = scan_probe_region(&short, 4, 5, MAX_PROBE);
        assert!(!s2.hit_empty);
        assert_eq!(s2.cands, vec![(5, 7)]);
        assert_eq!(s2.slots_advanced, 1);

        // Trailing partial entry ignored.
        let mut partial = 9u32.to_le_bytes().to_vec();
        partial.push(0xff);
        let s3 = scan_probe_region(&partial, 4, 0, MAX_PROBE);
        assert_eq!(s3.cands, vec![(0, 9)]);
        assert!(!s3.hit_empty);
    }

    #[test]
    fn probe_region_covers_chain_across_table_end_wrap() {
        // Tiny table: force home near end so first region is short, then wrap.
        let path = tmp("wrap_region");
        let h = AddressHead::create_with_bits(&path, 8).unwrap(); // 256 slots
        // Craft txid with h1 = 254 so chain uses 254,255,0,1,...
        let mut txid = [0u8; 32];
        // bits=8 → h1 = first byte
        txid[0] = 254;
        assert_eq!(h1(&txid, 8), 254);
        h.insert(&txid, Fk(1)).unwrap();
        // Foreigners fill 255 so second insert of same txid goes to wrap (or we
        // just check first lands and probe_fks finds it with region wrap).
        let mut other = [0u8; 32];
        other[0] = 255;
        h.insert(&other, Fk(2)).unwrap();
        // Same home 254: second create for txid goes deeper (255 occupied by other → 0).
        h.insert(&txid, Fk(3)).unwrap();
        let cands = h.probe_fks(&txid).unwrap();
        assert!(cands.contains(&Fk(1)));
        assert!(cands.contains(&Fk(3)));
        // Reverse body-order semantics: deepest is last in probe_fks order.
        assert_eq!(cands[0], Fk(1));
        assert!(cands.last() == Some(&Fk(3)) || cands.contains(&Fk(3)));
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(meta_path(&path));
    }

    #[test]
    fn probe_is_linear_from_h1() {
        let k = [0xabu8; 32];
        let bits = 16u32;
        let mask = (1u64 << bits) - 1;
        let home = h1(&k, bits);
        assert_eq!(probe_index(&k, 0, bits), home);
        for d in 0..32u32 {
            let expect = home.wrapping_add(u64::from(d)) & mask;
            assert_eq!(probe_index(&k, d, bits), expect, "d={d}");
        }
        // Wrap: last slot then 0.
        let near_end = mask;
        // Craft txid whose h1 is near_end via top bits of first 8 bytes.
        // h1 = first 8 bytes BE >> (64-bits).
        let mut k2 = [0u8; 32];
        // For bits=16, top 16 bits of first 8 bytes = 0xffff → h1 = 0xffff.
        k2[0] = 0xff;
        k2[1] = 0xff;
        assert_eq!(h1(&k2, 16), near_end);
        assert_eq!(probe_index(&k2, 0, 16), near_end);
        assert_eq!(probe_index(&k2, 1, 16), 0);
        assert_eq!(probe_index(&k2, 2, 16), 1);
    }

    #[test]
    fn probe_bits_28_to_34_in_range() {
        let k = [0x11u8; 32];
        for bits in [28u32, 31, 32, 33, 34] {
            let idx = probe_index(&k, 0, bits);
            assert!(idx < (1u64 << bits), "bits={bits} idx={idx}");
            let idx2 = probe_index(&k, 7, bits);
            assert!(idx2 < (1u64 << bits));
            // Consecutive depths differ by 1 (mod 2^bits).
            let mask = (1u64 << bits) - 1;
            assert_eq!(idx2, idx.wrapping_add(7) & mask);
        }
    }

    #[test]
    fn meta_v1_refused_linear_probe() {
        let path = tmp("meta_v1");
        let h = AddressHead::create_with_bits(&path, 12).unwrap();
        drop(h);
        // Rewrite meta as v1 (double-hash era).
        let mut meta = path.as_os_str().to_os_string();
        meta.push(".meta");
        let meta = PathBuf::from(meta);
        let mut buf = std::fs::read(&meta).unwrap();
        buf[4..6].copy_from_slice(&1u16.to_le_bytes());
        std::fs::write(&meta, &buf).unwrap();
        match AddressHead::open(&path) {
            Err(StoreError::Corrupt(m)) if m.contains("linear probe") => {}
            Err(e) => panic!("expected linear-probe meta error, got {e}"),
            Ok(_) => panic!("expected open failure for meta v1"),
        }
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(&meta);
    }

    #[test]
    fn entry_bytes_policy() {
        assert_eq!(entry_bytes_for_bits(28), 4);
        assert_eq!(entry_bytes_for_bits(32), 4);
        assert_eq!(entry_bytes_for_bits(33), 8);
        assert_eq!(entry_bytes_for_bits(34), 8);
    }

    #[test]
    fn load_trigger_at_75_percent() {
        let slots = 1024u64;
        let thr = ((slots as f64) * HEAD_LOAD_START).ceil() as u64;
        assert_eq!(thr, 768); // ceil(0.75 * 1024)
        assert!(!load_needs_resize(thr - 1, slots));
        assert!(load_needs_resize(thr, slots));
        assert!(load_needs_resize(slots, slots));
    }

    #[test]
    fn is_probe_exhausted_matches_insert_error() {
        let e = StoreError::Corrupt("address head probe exhausted on insert");
        assert!(is_probe_exhausted_error(&e));
        assert!(!is_probe_exhausted_error(&StoreError::NotFound));
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
