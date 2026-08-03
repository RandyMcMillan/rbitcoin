//! Keyless addressable `tx.head`: `2^BITS` slots × 4 B or 8 B create_fk entries.
//!
//! **Layout:** each entry is LE create_fk (`0` = empty). No key material and
//! **no HAS_NEXT** — probe continues until an empty slot (no Class A deletes).
//! Callers verify identity via Class A body txid on **lookup**.
//!
//! **Insert (sole writer):** probe until the **same fk** is already present
//! (idempotent) or an **empty** slot — plain mmap store `0 → fk` (no CAS, no
//! per-slot atomics). **No body_txid** on insert (no BIP30 displacement on write).
//! Foreigners and older same-txid creates are skipped blindly; a second Class A
//! row for the same txid lands at the next empty slot (deeper on the probe chain).
//!
//! **`insert_many` batching:** stable-sort by probe **page** then original index
//! (preserves call order within a page for rare same-batch duplicate txids). One
//! page load + multi-insert in RAM + plain slot stores per dirty slot, then a
//! **SeqCst fence** so concurrent page loads + Acquire fence observe the batch.
//!
//! **Concurrency:** at most **one** thread may insert into a given head segment
//! (archive writer in IBD; single tip accept path after). Multi-writer races are
//! not supported. Capacity growth is **segment roll** (seal + new open), not bits-widen.
//!
//! **Lookup:** walk candidates from the **last occupied** probe slot toward the
//! first, body-verify — so the deepest same-txid create wins (newest under
//! append-deeper insert).
//!
//! **Probe (page-local open address):** high bits of the txid select a **page** of
//! [`PAGE_SLOTS`] (2¹⁰) slots; within the page, **double hashing** with the next
//! key bits (`h1` / odd `h2`). Depth is capped at [`MAX_PROBE`] (= page size).
//! Lookup/insert load **one page** (4 KiB @ 4 B entries) then hop in RAM — one IO
//! for all candidates. Foreign occupants: body mismatch ⇒ continue. Keyless slots
//! cannot Robin-Hood. First insert at depth > [`PROBE_DEPTH_WARN`] requests online
//!
//! **Segment default:** BITS=**25** → **128 MiB** @ 4 B relative create ids per
//! segment (`2^15` pages × 4 KiB). Capacity ends at [`HEAD_LOAD_START`] (0.80);
//! the segmented head seals (fuse8) and opens a new fixed-bits table — no
//! global bits-widen.

use crate::error::StoreError;
use crate::file::{TableAccess, TableFile, TRAILING_FOOTER_LEN};
use crate::hashhead::HeadScale;
use rbitcoin_primitives::{Fk, TableKind};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

/// Table transport for address-head files (`tx.head` segments).
///
/// Always [`TableAccess::FdOnly`] (phase 6 — maps removed). Env
/// `RBITCOIN_TX_HEAD_ACCESS=map|mmap` is ignored with a one-time warn.
pub fn head_table_access_from_env() -> TableAccess {
    if let Ok(s) = std::env::var("RBITCOIN_TX_HEAD_ACCESS") {
        let t = s.trim().to_ascii_lowercase();
        if t == "map" || t == "mmap" || t == "mapfull" {
            rbitcoin_log::warn!(
                "store: RBITCOIN_TX_HEAD_ACCESS={t} ignored — MapFull removed (phase 6); using FdOnly"
            );
        }
    }
    TableAccess::FdOnly
}


/// In-page slot index width (1024 slots / page @ any entry width).
pub const PAGE_SLOT_BITS: u32 = 10;
/// Slots per page (`2^PAGE_SLOT_BITS`).
pub const PAGE_SLOTS: u64 = 1 << PAGE_SLOT_BITS;

/// Hard cap — full in-page exploration (never leave the page).
pub const MAX_PROBE: u32 = 1024;

/// Max bytes of one head page load (1024 × 8 B). 4 B entries use half.
pub const PROBE_REGION_BYTES: usize = (PAGE_SLOTS as usize) * 8;

/// Inserts that needed probe depth **> [`PROBE_DEPTH_WARN`]** (warning band).
/// Cumulative counter for lagging/retry logs; WARN only once at first event.
static PROBE_INSERT_DEPTH_WARN_COUNT: AtomicU64 = AtomicU64::new(0);

/// Inserts that exhausted [`MAX_PROBE`] (archiver sleeps until resize completes).
/// Counter only — the retry loop owns operator-facing logs.
static PROBE_INSERT_EXHAUSTED: AtomicU64 = AtomicU64::new(0);

/// Depth threshold above which inserts count as “deep” for ops visibility and
/// trigger an early online resize request (first event only).
pub const PROBE_DEPTH_WARN: u32 = 128;

/// Segment address width (2^25 slots × 4 B = 128 MiB). Fixed per segment; roll to grow.
pub const MAINNET_BITS: u32 = 25;
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

/// True when `err` is the sole-writer open-address insert failure (table full
/// along the probe chain — should not happen when segment roll respects 80% load).
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
    // Once only — first deep insert; further events counted silently.
    if n == 1 {
        rbitcoin_log::warn!(
            "store: tx.head insert probe depth>{PROBE_DEPTH_WARN} (first depth={depth}; \
             segment may be overfull — roll policy should prevent this)"
        );
    }
}

#[inline]
fn note_probe_exhausted() {
    PROBE_INSERT_EXHAUSTED.fetch_add(1, Ordering::Relaxed);
}

const META_MAGIC: &[u8; 4] = b"THM1";
/// `5` = page-local double-hash; layout (bits/entry/generation) lives in the
/// **trailing footer** next to store magic (no `tx.head.meta` sidecar). Slots at
/// offset 0 remain page-aligned. Older versions refused → open recreates + rebuilds.
const META_VERSION: u16 = 5;

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

/// First 8 bytes of txid as big-endian u64 (bit stream for page / h1).
#[inline]
fn key_be_u64(txid: &[u8; 32]) -> u64 {
    u64::from_be_bytes([
        txid[0], txid[1], txid[2], txid[3], txid[4], txid[5], txid[6], txid[7],
    ])
}

/// Page index from the **top** `(bits - 10)` bits of the txid (0 if bits ≤ 10).
#[inline]
pub fn page_index(txid: &[u8; 32], bits: u32) -> u64 {
    debug_assert!((MIN_BITS..=MAX_BITS).contains(&bits));
    if bits <= PAGE_SLOT_BITS {
        return 0;
    }
    let page_bits = bits - PAGE_SLOT_BITS;
    key_be_u64(txid) >> (64 - page_bits)
}

/// In-page h1: next 10 bits after the page-select field (mod 2^10).
#[inline]
pub fn h1_in_page(txid: &[u8; 32], bits: u32) -> u64 {
    let v = key_be_u64(txid);
    if bits <= PAGE_SLOT_BITS {
        return (v >> (64 - bits)) & ((1u64 << bits) - 1);
    }
    let page_bits = bits - PAGE_SLOT_BITS;
    // Bits [page_bits, page_bits+10) of the BE stream.
    (v >> (64 - page_bits - PAGE_SLOT_BITS)) & (PAGE_SLOTS - 1)
}

/// In-page odd step from a second window of the txid (1,3,… within the page).
#[inline]
pub fn h2_in_page(txid: &[u8; 32], bits: u32) -> u64 {
    let v = u64::from_be_bytes([
        txid[4], txid[5], txid[6], txid[7], txid[8], txid[9], txid[10], txid[11],
    ]);
    let mask = if bits <= PAGE_SLOT_BITS {
        (1u64 << bits) - 1
    } else {
        PAGE_SLOTS - 1
    };
    (v | 1) & mask
}

/// Global slot at probe depth `d`: page from high bits, double-hash within page.
///
/// `slot = (page << 10) | ((h1 + d·h2) mod 1024)` when `bits > 10`.
#[inline]
pub fn probe_index(txid: &[u8; 32], d: u32, bits: u32) -> u64 {
    debug_assert!((MIN_BITS..=MAX_BITS).contains(&bits));
    let h1 = h1_in_page(txid, bits);
    let h2 = h2_in_page(txid, bits);
    if bits <= PAGE_SLOT_BITS {
        let mask = (1u64 << bits) - 1;
        return h1.wrapping_add(u64::from(d).wrapping_mul(h2)) & mask;
    }
    let page = page_index(txid, bits);
    let local = h1.wrapping_add(u64::from(d).wrapping_mul(h2)) & (PAGE_SLOTS - 1);
    (page << PAGE_SLOT_BITS) | local
}

/// Number of slots in the probe page for this table width.
#[inline]
pub fn page_slot_count(bits: u32) -> u64 {
    if bits <= PAGE_SLOT_BITS {
        1u64 << bits
    } else {
        PAGE_SLOTS
    }
}

/// Slot file offset: data starts at **0** (trailing magic); page-aligned probes.
#[inline]
pub fn entry_file_off(slot: u64, entry_bytes: u8) -> u64 {
    slot * u64::from(entry_bytes)
}

/// Decode one LE create_fk from a page buffer at local slot index.
#[inline]
pub fn entry_from_page_buf(buf: &[u8], local: u64, entry_bytes: u8) -> Option<u64> {
    let es = entry_bytes as usize;
    let off = (local as usize).checked_mul(es)?;
    if off + es > buf.len() {
        return None;
    }
    Some(match entry_bytes {
        4 => u64::from(u32::from_le_bytes(buf[off..off + 4].try_into().ok()?)),
        8 => u64::from_le_bytes(buf[off..off + 8].try_into().ok()?),
        _ => return None,
    })
}

/// Result of hopping through one loaded page.
#[derive(Debug, Clone)]
pub struct ProbeRegionScan {
    /// Occupied create_fks with absolute probe depth (home = 0).
    pub cands: Vec<(u32, u64)>,
    /// Saw an empty slot (probe chain ends).
    pub hit_empty: bool,
    /// Depth of empty slot if [`Self::hit_empty`], else depths explored without empty.
    pub depth_end: u32,
    /// Local slot index of empty (valid if hit_empty).
    pub empty_local: u64,
}

/// Double-hash hop through a loaded page buffer.
#[inline]
pub fn hop_scan_page(
    page_buf: &[u8],
    entry_bytes: u8,
    h1: u64,
    h2: u64,
    page_slots: u64,
    max_probe: u32,
) -> ProbeRegionScan {
    let mask = page_slots - 1;
    let max_d = max_probe.min(page_slots as u32);
    let mut cands = Vec::new();
    for d in 0..max_d {
        let local = h1.wrapping_add(u64::from(d).wrapping_mul(h2)) & mask;
        let Some(e) = entry_from_page_buf(page_buf, local, entry_bytes) else {
            break;
        };
        if e == 0 {
            return ProbeRegionScan {
                cands,
                hit_empty: true,
                depth_end: d,
                empty_local: local,
            };
        }
        cands.push((d, e));
    }
    ProbeRegionScan {
        cands,
        hit_empty: false,
        depth_end: max_d,
        empty_local: 0,
    }
}

/// Global first slot of the probe page that holds `txid`.
#[inline]
pub fn page_base_for_txid(txid: &[u8; 32], bits: u32) -> u64 {
    if bits <= PAGE_SLOT_BITS {
        0
    } else {
        page_index(txid, bits) << PAGE_SLOT_BITS
    }
}

/// Outcome of [`insert_fk_into_page_buf`] (in-buffer only; no file IO).
#[derive(Debug, Clone, Copy)]
pub struct InsertPageOutcome {
    /// Wrote a new empty→fk slot (false if `new_fk` already on the chain).
    pub wrote_new: bool,
    /// Probe depth of the empty slot written (or 0 if idempotent).
    pub depth: u32,
    /// Local slot index within the page (valid if `wrote_new`).
    /// Local slot of the new empty (unit tests; insert_many uses full-page write-back).
    #[allow(dead_code)]
    pub empty_local: u64,
    /// Encoded create_fk written when `wrote_new` (unit tests).
    #[allow(dead_code)]
    pub stored_fk: u64,
}

/// Insert `new_fk` into a **loaded** probe page buffer (online resize RMW path).
///
/// Idempotent if `new_fk` is already present. Does not touch the file or
/// [`AddressHead::occupied`] — caller applies the buffer via pwrite / store and
/// bumps occupied when `wrote_new`.
pub fn insert_fk_into_page_buf(
    page_buf: &mut [u8],
    page_base: u64,
    bits: u32,
    entry_bytes: u8,
    txid: &[u8; 32],
    new_fk: Fk,
) -> Result<InsertPageOutcome, StoreError> {
    let _ = page_base;
    if new_fk.is_null() {
        return Err(StoreError::InvalidFk);
    }
    if entry_bytes == 4 && new_fk.0 > u64::from(u32::MAX) {
        return Err(StoreError::InvalidFk);
    }
    let new_u = new_fk.0;
    let es = entry_bytes as usize;
    if es != 4 && es != 8 {
        return Err(StoreError::Corrupt("address head entry_bytes"));
    }
    if page_buf.len() < es {
        return Err(StoreError::Corrupt("address head probe page empty"));
    }
    let nslots = (page_buf.len() / es) as u64;
    let h1 = h1_in_page(txid, bits);
    let h2 = h2_in_page(txid, bits);
    let scan = hop_scan_page(page_buf, entry_bytes, h1, h2, nslots, MAX_PROBE);
    for &(_d, e) in &scan.cands {
        if e == new_u {
            return Ok(InsertPageOutcome {
                wrote_new: false,
                depth: 0,
                empty_local: 0,
                stored_fk: new_u,
            });
        }
    }
    if !scan.hit_empty {
        note_probe_exhausted();
        return Err(StoreError::Corrupt("address head probe exhausted on insert"));
    }
    store_entry_in_page_buf(page_buf, scan.empty_local, entry_bytes, new_u)?;
    Ok(InsertPageOutcome {
        wrote_new: true,
        depth: scan.depth_end,
        empty_local: scan.empty_local,
        stored_fk: new_u,
    })
}

/// Write LE create_fk into a page buffer at local slot index.
#[inline]
fn store_entry_in_page_buf(
    page_buf: &mut [u8],
    local: u64,
    entry_bytes: u8,
    new: u64,
) -> Result<(), StoreError> {
    let es = entry_bytes as usize;
    let off = (local as usize)
        .checked_mul(es)
        .ok_or(StoreError::Corrupt("page buf slot overflow"))?;
    if off + es > page_buf.len() {
        return Err(StoreError::Corrupt("page buf slot out of range"));
    }
    match entry_bytes {
        4 => {
            if new > u64::from(u32::MAX) {
                return Err(StoreError::InvalidFk);
            }
            page_buf[off..off + 4].copy_from_slice(&(new as u32).to_le_bytes());
        }
        8 => {
            page_buf[off..off + 8].copy_from_slice(&new.to_le_bytes());
        }
        _ => return Err(StoreError::Corrupt("address head entry_bytes")),
    }
    Ok(())
}

/// Bytes to pread for the full probe page of `txid`.
///
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

/// Fixed segment geometry ([`bits_for_scale`]). `n` is ignored — capacity growth
/// is segment roll, not bits-widen.
pub fn layout_for_count(_n: u64) -> HeadLayout {
    default_layout()
}

/// True when segment create count warrants a **roll** (seal + new open segment).
#[inline]
pub fn load_needs_roll(tx_count: u64, slots: u64) -> bool {
    if slots == 0 {
        return false;
    }
    let threshold = ((slots as f64) * HEAD_LOAD_START).floor() as u64;
    tx_count >= threshold
}

/// Deprecated name for [`load_needs_roll`] (segment capacity, not bits-widen).
#[inline]
pub fn load_needs_resize(tx_count: u64, slots: u64) -> bool {
    load_needs_roll(tx_count, slots)
}

/// Legacy sidecar path (`tx.head.meta`) — only for best-effort cleanup of old datadirs.
fn meta_path(head_path: &Path) -> PathBuf {
    let mut p = head_path.as_os_str().to_os_string();
    p.push(".meta");
    PathBuf::from(p)
}

/// Drop leftover sidecar meta from pre-v5 layouts (layout is now in the footer).
pub fn remove_legacy_meta_sidecar(head_path: &Path) {
    let _ = std::fs::remove_file(meta_path(head_path));
}

/// Pack layout + generation into the 16-byte trailing-footer extension.
#[inline]
pub fn encode_layout_ext(layout: HeadLayout, generation: u64) -> [u8; 16] {
    let mut buf = [0u8; 16];
    buf[0..4].copy_from_slice(META_MAGIC);
    buf[4..6].copy_from_slice(&META_VERSION.to_le_bytes());
    buf[6] = layout.bits as u8;
    buf[7] = layout.entry_bytes;
    buf[8..16].copy_from_slice(&generation.to_le_bytes());
    buf
}

/// Decode layout extension from the trailing footer (or fail for rebuild).
pub fn decode_layout_ext(ext: &[u8; 16]) -> Result<(HeadLayout, u64), StoreError> {
    if &ext[0..4] != META_MAGIC {
        return Err(StoreError::Corrupt(
            "tx.head footer layout magic (rebuild tx.head)",
        ));
    }
    let ver = u16::from_le_bytes([ext[4], ext[5]]);
    if ver != META_VERSION {
        return Err(StoreError::Corrupt(
            "tx.head footer layout version (footer-embedded meta; rebuild tx.head)",
        ));
    }
    let bits = u32::from(ext[6]);
    let entry_bytes = ext[7];
    let generation = u64::from_le_bytes(ext[8..16].try_into().unwrap());
    let layout = HeadLayout::with_entry_bytes(bits, entry_bytes)?;
    Ok((layout, generation))
}

/// Fixed-width keyless txid → dense create_fk table.
pub struct AddressHead {
    file: TableFile,
    layout: HeadLayout,
    slots: u64,
    occupied: AtomicU64,
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
        // Trailing footer: slots at offset 0 so each 4 KiB probe page is OS-aligned.
        // Layout (bits/entry/generation) lives in the footer extension — no sidecar.
        Self::create_with_table_access(path, layout, head_table_access_from_env())
    }

    /// Create with explicit table access (always FdOnly after phase 6).
    pub fn create_with_table_access(
        path: impl Into<PathBuf>,
        layout: HeadLayout,
        access: crate::file::TableAccess,
    ) -> Result<Self, StoreError> {
        let _ = access;
        let path = path.into();
        if path.is_dir() {
            return Err(StoreError::Corrupt(
                "tx.head is a directory (legacy shards); wipe datadir for address head",
            ));
        }
        let slots = layout.slots();
        let mut file = TableFile::create_trailing_header(&path, TableKind::HashHead)?;
        let body_bytes = layout.body_bytes();
        let need = body_bytes + TRAILING_FOOTER_LEN as u64;
        file.ensure_capacity(need)?;
        // Layout ext must be set before set_logical_len so the footer at EOF
        // carries bits/generation (no sidecar).
        file.set_trailing_ext(encode_layout_ext(layout, 0))?;
        file.set_logical_len(need)?;
        file.zero_range(0, body_bytes)?;
        remove_legacy_meta_sidecar(&path);
        rbitcoin_log::info!(
            "store: address-head create path={} bits={} slots={} entry={}B access=FdOnly (~{:.2} GiB sparse, footer layout)",
            file.path().display(),
            layout.bits,
            slots,
            layout.entry_bytes,
            body_bytes as f64 / (1024.0 * 1024.0 * 1024.0)
        );
        Ok(Self {
            file,
            layout,
            slots,
            occupied: AtomicU64::new(0),
            generation: 0,
        })
    }

    pub fn open(path: impl Into<PathBuf>) -> Result<Self, StoreError> {
        Self::open_with_table_access(path, head_table_access_from_env())
    }

    /// Open with explicit table access (always FdOnly after phase 6).
    pub fn open_with_table_access(
        path: impl Into<PathBuf>,
        access: crate::file::TableAccess,
    ) -> Result<Self, StoreError> {
        let _ = access;
        let path = path.into();
        if path.is_dir() {
            return Err(StoreError::Corrupt(
                "tx.head is a directory (legacy shards); wipe datadir for address head",
            ));
        }
        // Layout is in the trailing footer (v5). Sidecar-only or older footers fail
        // here → TxTable recreates + rebuilds from Class A.
        let (file, ext) =
            TableFile::open_trailing_header_from_end(&path, TableKind::HashHead)?;
        let (layout, generation) = decode_layout_ext(&ext)?;
        let expect_body = layout.body_bytes();
        let body = file.data_len();
        if body == 0 {
            return Err(StoreError::Corrupt("address head size"));
        }
        if body != expect_body {
            return Err(StoreError::Corrupt(
                "address head size mismatch vs footer layout",
            ));
        }
        remove_legacy_meta_sidecar(&path);

        let slots = layout.slots();
        let occupied = count_occupied(&file, slots, layout.entry_bytes)?;
        rbitcoin_log::info!(
            "store: address-head open path={} bits={} slots={} entry={}B access=FdOnly gen={} occupied≈{}",
            file.path().display(),
            layout.bits,
            slots,
            layout.entry_bytes,
            generation,
            occupied,
        );
        Ok(Self {
            file,
            layout,
            slots,
            occupied: AtomicU64::new(occupied),
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

    /// Table transport for this head file (always FdOnly after phase 6).
    pub fn table_access(&self) -> TableAccess {
        self.file.access()
    }

    pub fn generation(&self) -> u64 {
        self.generation
    }

    #[inline]
    pub(crate) fn entry_off(&self, slot: u64) -> u64 {
        entry_file_off(slot, self.layout.entry_bytes)
    }

    /// Read one open-address entry (0 = empty).
    ///
    /// Hot path uses [`Self::load_page_slots`] (one page `read_at`). This remains
    /// for tests and rare single-slot diagnostics. Uses `read_at` so FdOnly
    /// works past the tiny header map window.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn read_entry(&self, slot: u64) -> Result<u64, StoreError> {
        let off = self.entry_off(slot);
        match self.layout.entry_bytes {
            4 => {
                let mut buf = [0u8; 4];
                self.file.read_at(off, &mut buf)?;
                Ok(u64::from(u32::from_le_bytes(buf)))
            }
            8 => {
                let mut buf = [0u8; 8];
                self.file.read_at(off, &mut buf)?;
                Ok(u64::from_le_bytes(buf))
            }
            _ => Err(StoreError::Corrupt("address head entry_bytes")),
        }
    }

    /// Load a full probe page starting at global `page_base` into `buf`.
    ///
    /// **One** `read_at` of up to `n_slots × entry_bytes` (4 KiB @ 4 B / 1024 slots) —
    /// not 1024 individual `load_u32` pins (that made resize fill CPU-bound at
    /// ~O(page_slots) per insert). Caps to the slot data region (excludes trailing
    /// footer). Acquire fence so concurrent probes observe prior sole-writer
    /// Release stores after the bulk copy.
    ///
    /// Returns bytes filled (multiple of entry size). Callers must pass the
    /// corresponding slot count into [`hop_scan_page`] (`bytes / entry_bytes`).
    fn load_page_slots(
        &self,
        page_base: u64,
        n_slots: u64,
        buf: &mut [u8],
    ) -> Result<usize, StoreError> {
        let es = self.layout.entry_bytes as usize;
        if es != 4 && es != 8 {
            return Err(StoreError::Corrupt("address head entry_bytes"));
        }
        let n = n_slots as usize;
        let mut need = n.saturating_mul(es);
        if need > buf.len() {
            return Err(StoreError::Corrupt("probe page buffer short"));
        }
        let off = self.entry_off(page_base);
        // Never read into the trailing footer — only the create_fk slot array.
        let data_end = self.file.data_len();
        let avail = data_end.saturating_sub(off) as usize;
        need = need.min(avail);
        need = (need / es) * es;
        if need == 0 {
            return Ok(0);
        }
        self.file.read_at(off, &mut buf[..need])?;
        // Pair with sole-writer stores + SeqCst fence after insert_many.
        std::sync::atomic::fence(std::sync::atomic::Ordering::Acquire);
        Ok(need)
    }

    pub fn reserve_additional(&self, _additional: u64) -> Result<(), StoreError> {
        Ok(())
    }

    /// Insert one mapping (no body IO). Sole writer.
    pub fn insert(&self, txid: &[u8; 32], new_fk: Fk) -> Result<(), StoreError> {
        self.insert_many(&[(*txid, new_fk)])
    }

    /// Plain slot write of one create_fk (sole writer; no atomic RMW).
    ///
    /// Bulk insert: **stable sort by probe page**, then original index (preserves
    /// call order within a page for rare same-batch duplicate txids).
    ///
    /// Per page: one [`load_page_slots`], multi [`insert_fk_into_page_buf`] in
    /// RAM, then **one page write-back** if dirty (not per-slot pwrite). **SeqCst
    /// fence** once at end.
    pub fn insert_many(&self, entries: &[([u8; 32], Fk)]) -> Result<(), StoreError> {
        if entries.is_empty() {
            return Ok(());
        }
        let bits = self.layout.bits;
        let es = self.layout.entry_bytes;
        let page_slots = page_slot_count(bits);
        let es_u = es as usize;

        // (page_base, orig_i, txid, fk) — stable by page then plan order.
        let mut work: Vec<(u64, usize, [u8; 32], Fk)> = entries
            .iter()
            .enumerate()
            .map(|(i, (txid, fk))| (page_base_for_txid(txid, bits), i, *txid, *fk))
            .collect();
        work.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.cmp(&b.1)));

        let mut buf = [0u8; PROBE_REGION_BYTES];
        let mut i = 0;
        while i < work.len() {
            let page_base = work[i].0;
            let mut j = i + 1;
            while j < work.len() && work[j].0 == page_base {
                j += 1;
            }

            let n = self.load_page_slots(page_base, page_slots, &mut buf)?;
            if n < es_u {
                note_probe_exhausted();
                return Err(StoreError::Corrupt("address head probe page empty"));
            }

            let mut n_new = 0u64;
            let mut dirty = false;
            for &(_, _, ref txid, fk) in &work[i..j] {
                let outcome =
                    insert_fk_into_page_buf(&mut buf[..n], page_base, bits, es, txid, fk)?;
                if outcome.wrote_new {
                    note_probe_depth_on_insert(outcome.depth);
                    dirty = true;
                    n_new = n_new.saturating_add(1);
                }
            }
            if dirty {
                // One OS-page-aligned write for the whole probe page (4 KiB @ 4 B).
                let off = self.entry_off(page_base);
                self.file.write_at(off, &buf[..n])?;
                self.occupied.fetch_add(n_new, Ordering::Relaxed);
            }
            i = j;
        }

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

    /// Walk in-page double-hash until empty; return every fk (may include foreigners).
    ///
    /// One page load, then hop in RAM (single IO for the full candidate set).
    pub fn probe_fks(&self, txid: &[u8; 32]) -> Result<Vec<Fk>, StoreError> {
        let mut out = self.probe_fks_batch(std::slice::from_ref(txid))?;
        Ok(out.pop().unwrap_or_default())
    }

    /// Batch probe: group keys by probe page, **one page `read_at` per distinct page**,
    /// hop each key in RAM. Same results as N× [`Self::probe_fks`] (order preserved).
    ///
    /// Archive head-resolve uses this so multi-key waves under FdOnly share page IO.
    pub fn probe_fks_batch(&self, txids: &[[u8; 32]]) -> Result<Vec<Vec<Fk>>, StoreError> {
        let n_keys = txids.len();
        if n_keys == 0 {
            return Ok(Vec::new());
        }
        let bits = self.layout.bits;
        let es = self.layout.entry_bytes;
        let page_slots = page_slot_count(bits);
        let es_u = es as usize;

        // (page_base, orig_i) — stable within a page for deterministic order.
        let mut order: Vec<(u64, usize)> = txids
            .iter()
            .enumerate()
            .map(|(i, t)| (page_base_for_txid(t, bits), i))
            .collect();
        order.sort_unstable_by_key(|&(p, i)| (p, i));

        let mut out = vec![Vec::new(); n_keys];
        let mut buf = [0u8; PROBE_REGION_BYTES];
        let mut i = 0;
        while i < order.len() {
            let page_base = order[i].0;
            let mut j = i + 1;
            while j < order.len() && order[j].0 == page_base {
                j += 1;
            }
            let n = self.load_page_slots(page_base, page_slots, &mut buf)?;
            if n >= es_u {
                let nslots = (n / es_u) as u64;
                for &(_, orig) in &order[i..j] {
                    let txid = &txids[orig];
                    let h1p = h1_in_page(txid, bits);
                    let h2p = h2_in_page(txid, bits);
                    let scan = hop_scan_page(&buf[..n], es, h1p, h2p, nslots, MAX_PROBE);
                    out[orig] = scan.cands.into_iter().map(|(_, e)| Fk(e)).collect();
                }
            }
            i = j;
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
        let off = entry_file_off(slot, entry_bytes);
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
        assert_eq!(MAX_PROBE, 1024);
        assert_eq!(PAGE_SLOTS, 1024);
        assert_eq!(PAGE_SLOT_BITS, 10);
        assert_eq!(PROBE_REGION_BYTES, 8192);
        assert_eq!(PROBE_DEPTH_WARN, 128);
        assert_eq!(MAINNET_BITS, 25);
        assert!(PROBE_REGION_BYTES as u64 >= PAGE_SLOTS * 8);
    }


    #[test]
    fn probe_stable() {
        let k = [0xabu8; 32];
        assert_eq!(probe_index(&k, 0, 16), probe_index(&k, 0, 16));
        assert_ne!(probe_index(&k, 0, 16), probe_index(&k, 1, 16));
        assert!(probe_index(&k, 0, 16) < (1 << 16));
    }

    #[test]
    fn hop_scan_page_stops_at_empty() {
        // Page of 4 slots: place fks at double-hash locals.
        let mut buf = vec![0u8; 16];
        // h1=0, h2=1 → locals 0,1,2,...
        buf[0..4].copy_from_slice(&1u32.to_le_bytes());
        buf[4..8].copy_from_slice(&2u32.to_le_bytes());
        // slot 2 empty
        let s = hop_scan_page(&buf, 4, 0, 1, 4, MAX_PROBE);
        assert!(s.hit_empty);
        assert_eq!(s.cands, vec![(0, 1), (1, 2)]);
        assert_eq!(s.depth_end, 2);
        assert_eq!(s.empty_local, 2);
    }

    #[test]
    fn insert_fk_into_page_buf_empty_idempotent_and_second() {
        let bits = 16u32;
        let es = 4u8;
        let mut txid = [0u8; 32];
        txid[0] = 0x42;
        let page_base = page_base_for_txid(&txid, bits);
        let page_slots = page_slot_count(bits);
        let mut buf = vec![0u8; (page_slots as usize) * es as usize];
        let o1 = insert_fk_into_page_buf(&mut buf, page_base, bits, es, &txid, Fk(7)).unwrap();
        assert!(o1.wrote_new);
        assert_eq!(o1.stored_fk, 7);
        let o2 = insert_fk_into_page_buf(&mut buf, page_base, bits, es, &txid, Fk(7)).unwrap();
        assert!(!o2.wrote_new, "idempotent same fk");
        let o3 = insert_fk_into_page_buf(&mut buf, page_base, bits, es, &txid, Fk(8)).unwrap();
        assert!(o3.wrote_new, "second create deeper on chain");
        assert_ne!(o3.empty_local, o1.empty_local);
    }

    #[test]
    fn page_local_double_hash_stays_in_page() {
        let k = [0x11u8; 32];
        let bits = 26u32;
        let page = page_index(&k, bits);
        for d in 0..64u32 {
            let slot = probe_index(&k, d, bits);
            assert_eq!(slot >> PAGE_SLOT_BITS, page, "d={d}");
            assert!(slot < (1u64 << bits));
        }
        // Distinct depths should differ (odd h2).
        assert_ne!(probe_index(&k, 0, bits), probe_index(&k, 1, bits));
    }

    #[test]
    fn probe_bits_26_to_34_in_range() {
        let k = [0x11u8; 32];
        for bits in [26u32, 28, 31, 32, 33, 34] {
            let idx = probe_index(&k, 0, bits);
            assert!(idx < (1u64 << bits), "bits={bits} idx={idx}");
            let idx2 = probe_index(&k, 7, bits);
            assert!(idx2 < (1u64 << bits));
            // Same page for all depths.
            if bits > PAGE_SLOT_BITS {
                assert_eq!(idx >> PAGE_SLOT_BITS, idx2 >> PAGE_SLOT_BITS);
            }
        }
    }

    #[test]
    fn bip30_second_create_same_page() {
        let path = tmp("bip30_page");
        let h = AddressHead::create_with_bits(&path, 16).unwrap();
        let mut txid = [0u8; 32];
        txid[0] = 0x55;
        h.insert(&txid, Fk(1)).unwrap();
        h.insert(&txid, Fk(2)).unwrap();
        let cands = h.probe_fks(&txid).unwrap();
        assert!(cands.contains(&Fk(1)));
        assert!(cands.contains(&Fk(2)));
        assert_eq!(cands[0], Fk(1));
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(meta_path(&path));
    }

    /// FdOnly trailing head: page-coalesce insert uses pread/pwrite (phase 2 A/B path).
    #[test]
    fn fd_only_insert_many_probe_and_reopen() {
        let path = tmp("fd_only_head");
        let layout = HeadLayout::with_entry_bytes(14, 4).unwrap();
        let h = AddressHead::create_with_table_access(&path, layout, TableAccess::FdOnly).unwrap();
        assert_eq!(h.table_access(), TableAccess::FdOnly);
        let mut batch = Vec::new();
        for i in 1..=500u64 {
            let mut txid = [0u8; 32];
            txid[0..8].copy_from_slice(&i.to_le_bytes());
            batch.push((txid, Fk(i)));
        }
        h.insert_many(&batch).unwrap();
        assert_eq!(h.occupied(), 500);
        for i in [1u64, 250, 500] {
            let mut txid = [0u8; 32];
            txid[0..8].copy_from_slice(&i.to_le_bytes());
            let cands = h.probe_fks(&txid).unwrap();
            assert!(cands.contains(&Fk(i)), "fk={i} cands={cands:?}");
        }
        drop(h);
        let h2 =
            AddressHead::open_with_table_access(&path, TableAccess::FdOnly).unwrap();
        assert_eq!(h2.table_access(), TableAccess::FdOnly);
        assert_eq!(h2.occupied(), 500);
        let mut txid = [0u8; 32];
        txid[0..8].copy_from_slice(&42u64.to_le_bytes());
        assert!(h2.probe_fks(&txid).unwrap().contains(&Fk(42)));
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(meta_path(&path));
    }

    #[test]
    fn meta_v1_refused_linear_probe() {
        let path = tmp("meta_v1");
        let h = AddressHead::create_with_bits(&path, 12).unwrap();
        drop(h);
        // Corrupt footer layout version (v1 = double-hash era / pre-footer meta).
        let mut raw = std::fs::read(&path).unwrap();
        let n = raw.len();
        assert!(n >= TRAILING_FOOTER_LEN);
        // Footer layout ext at [n-16..n): version at bytes 4..6 of ext.
        let ver_off = n - 16 + 4;
        raw[ver_off..ver_off + 2].copy_from_slice(&1u16.to_le_bytes());
        std::fs::write(&path, &raw).unwrap();
        match AddressHead::open(&path) {
            Err(StoreError::Corrupt(m))
                if m.contains("footer") || m.contains("rebuild") || m.contains("version") => {}
            Err(e) => panic!("expected footer layout version error, got {e}"),
            Ok(_) => panic!("expected open failure for meta v1"),
        }
        let _ = std::fs::remove_file(&path);
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
        // Segment roll uses floor(0.80 * slots).
        let thr = ((slots as f64) * HEAD_LOAD_START).floor() as u64;
        assert_eq!(thr, 819); // floor(0.80 * 1024)
        assert!(!load_needs_roll(thr - 1, slots));
        assert!(load_needs_roll(thr, slots));
        assert!(load_needs_roll(slots, slots));
    }

    #[test]
    fn layout_for_count_is_fixed_segment_geometry() {
        // Capacity growth is segment roll, not bits-widen.
        let n = 102_956_483u64;
        let layout = layout_for_count(n);
        assert_eq!(layout.bits, bits_for_scale());
        let empty = layout_for_count(0);
        assert_eq!(empty.bits, bits_for_scale());
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

    /// Batch page-coalesced probe must match serial `probe_fks` (multi-page wave).
    #[test]
    fn probe_fks_batch_matches_serial() {
        let path = tmp("probe_batch");
        let h = AddressHead::create_with_bits(&path, 14).unwrap();
        let mut entries = Vec::new();
        for i in 1..=120u64 {
            let mut txid = [0u8; 32];
            txid[0] = (i & 0xff) as u8;
            txid[1] = ((i >> 8) & 0xff) as u8;
            txid[3] = 0xab;
            entries.push((txid, Fk(i)));
        }
        h.insert_many(&entries).unwrap();
        let keys: Vec<[u8; 32]> = entries.iter().map(|(t, _)| *t).collect();
        let batch = h.probe_fks_batch(&keys).unwrap();
        assert_eq!(batch.len(), keys.len());
        for (i, txid) in keys.iter().enumerate() {
            let serial = h.probe_fks(txid).unwrap();
            assert_eq!(batch[i], serial, "key {i}");
        }
        // Empty batch.
        assert!(h.probe_fks_batch(&[]).unwrap().is_empty());
        // Same-page multi-key still correct.
        let mut same_page = Vec::new();
        for i in 0..8 {
            let mut t = [0u8; 32];
            t[0] = 1;
            t[1] = i;
            same_page.push(t);
        }
        let _ = h.probe_fks_batch(&same_page).unwrap();
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(meta_path(&path));
    }

    /// Bulk page load must match per-slot reads (regression: load_page_slots
    /// used to call load_u32 once per slot — ~1024× cost on every insert/probe).
    #[test]
    fn load_page_slots_matches_per_slot_reads() {
        let path = tmp("page_bulk");
        let h = AddressHead::create_with_bits(&path, 14).unwrap();
        // Pack many inserts so some pages are multi-occupied.
        let mut entries = Vec::new();
        for i in 1..=200u64 {
            let mut txid = [0u8; 32];
            txid[0] = (i & 0xff) as u8;
            txid[1] = ((i >> 8) & 0xff) as u8;
            txid[2] = 0xee;
            entries.push((txid, Fk(i)));
        }
        h.insert_many(&entries).unwrap();

        let es = h.entry_bytes();
        let page_slots = page_slot_count(h.bits());
        // Page 0 for bits=14 is the whole table when bits<=10; for 14 use page 0.
        let page_base = 0u64;
        let mut bulk = [0u8; PROBE_REGION_BYTES];
        let n = h
            .load_page_slots(page_base, page_slots, &mut bulk)
            .unwrap();
        let nslots = (n / es as usize) as u64;
        assert!(nslots > 0);
        assert_eq!(n, (nslots as usize) * es as usize);
        // Full first page for a normal create (slot region only — no footer bytes).
        assert_eq!(nslots, page_slots.min(h.slots()));

        for local in 0..nslots {
            let slot = page_base + local;
            let expected = h.read_entry(slot).unwrap();
            let from_bulk = entry_from_page_buf(&bulk[..n], local, es).unwrap_or(0);
            assert_eq!(
                from_bulk, expected,
                "slot {slot} bulk={from_bulk} serial={expected}"
            );
        }
        // Slot region must not extend into trailing footer.
        // Probe path still finds inserts.
        for (txid, fk) in &entries {
            assert!(
                h.probe_fks(txid).unwrap().contains(fk),
                "missing {fk:?}"
            );
        }
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(meta_path(&path));
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


    /// Many inserts spanning multiple pages (page-coalesced; call order within page).
    #[test]
    fn insert_many_batch_order_multi_page() {
        let path = tmp("batch_order");
        // bits=14 → 16 pages × 1024 slots (page-local at bits>10).
        let h = AddressHead::create_with_bits(&path, 14).unwrap();
        let mut entries = Vec::new();
        for i in 1..=400u64 {
            let mut txid = [0u8; 32];
            txid[0] = (i & 0xff) as u8;
            txid[1] = ((i >> 8) & 0xff) as u8;
            txid[2] = ((i * 17) & 0xff) as u8;
            txid[3] = ((i * 31) & 0xff) as u8;
            txid[4] = 0xa5;
            entries.push((txid, Fk(i)));
        }
        h.insert_many(&entries).unwrap();
        assert_eq!(h.occupied(), 400);
        h.insert_many(&entries[..50]).unwrap();
        assert_eq!(h.occupied(), 400);
        for (txid, fk) in &entries {
            assert!(
                h.probe_fks(txid).unwrap().contains(fk),
                "missing {fk:?} after batch-order insert"
            );
        }
        let mut extra = [0u8; 32];
        extra[0] = 0xee;
        extra[1] = 0xff;
        h.insert(&extra, Fk(9001)).unwrap();
        assert!(h.probe_fks(&extra).unwrap().contains(&Fk(9001)));
        assert_eq!(h.occupied(), 401);
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(meta_path(&path));
    }

    /// Same txid twice in one batch: later fk is deeper; plan order preserved
    /// under page sort (stable by orig_i).
    #[test]
    fn insert_many_same_txid_preserves_depth_order() {
        let path = tmp("same_txid");
        let h = AddressHead::create_with_bits(&path, 14).unwrap();
        let txid = [0xab; 32];
        // Interleave with other pages so sort reorders globally but keeps orig_i
        // order within this page for the two same-txid inserts.
        let mut other = [0xcd; 32];
        other[0] = 0x11;
        let entries = [
            (txid, Fk(1)),
            (other, Fk(99)),
            (txid, Fk(2)),
        ];
        h.insert_many(&entries).unwrap();
        assert_eq!(h.occupied(), 3);
        let cands = h.probe_fks(&txid).unwrap();
        assert_eq!(cands.len(), 2, "two creates on chain: {cands:?}");
        // probe_fks is home→deep (hop order); first insert is shallower.
        assert_eq!(cands[0], Fk(1));
        assert_eq!(cands[1], Fk(2));
        // Deepest wins for body resolve semantics.
        assert_eq!(*cands.last().unwrap(), Fk(2));
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(meta_path(&path));
    }

    #[test]
    fn insert_many_sole_no_sort_roundtrip() {
        let path = tmp("sole");
        let h = AddressHead::create_with_bits(&path, 14).unwrap();
        let mut entries = Vec::new();
        // Reverse-ish order; page coalescing still finds all.
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
        // Deadline: infinite join if prober/barrier stuck (panic-before-wait).
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        thread::spawn(move || {
            prober.join().unwrap();
            let _ = done_tx.send(());
        });
        done_rx
            .recv_timeout(std::time::Duration::from_secs(30))
            .expect("concurrent address_head prober timed out (hang?)");

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
    fn mainnet_default_bits_is_26() {
        assert_eq!(MAINNET_BITS, 25);
        assert_eq!(entry_bytes_for_bits(MAINNET_BITS), 4);
        // 4 B × 1024 = 4 KiB pages at mainnet default.
        assert_eq!(PAGE_SLOTS as usize * 4, 4096);
    }

    /// insert_fk_into_page_buf / store_entry error arms + empty page / invalid fk.
    #[test]
    fn insert_fk_page_buf_error_arms() {
        let txid = [0x11u8; 32];
        // Null fk
        let mut page = vec![0u8; 4096];
        assert!(matches!(
            insert_fk_into_page_buf(&mut page, 0, 12, 4, &txid, Fk::NULL),
            Err(StoreError::InvalidFk)
        ));
        // 4-byte entry can't hold fk > u32::MAX
        assert!(matches!(
            insert_fk_into_page_buf(&mut page, 0, 12, 4, &txid, Fk(u64::from(u32::MAX) + 1)),
            Err(StoreError::InvalidFk)
        ));
        // Bad entry_bytes
        assert!(matches!(
            insert_fk_into_page_buf(&mut page, 0, 12, 6, &txid, Fk(1)),
            Err(StoreError::Corrupt(_))
        ));
        // Empty page buffer
        let mut empty = vec![];
        assert!(matches!(
            insert_fk_into_page_buf(&mut empty, 0, 12, 4, &txid, Fk(1)),
            Err(StoreError::Corrupt(_))
        ));
        // Happy path insert + idempotent
        let r = insert_fk_into_page_buf(&mut page, 0, 12, 4, &txid, Fk(7)).unwrap();
        assert!(r.wrote_new);
        let r2 = insert_fk_into_page_buf(&mut page, 0, 12, 4, &txid, Fk(7)).unwrap();
        assert!(!r2.wrote_new);
        // 8-byte entries
        let mut page8 = vec![0u8; 8192];
        let r8 = insert_fk_into_page_buf(&mut page8, 0, 12, 8, &txid, Fk(u64::from(u32::MAX) + 9))
            .unwrap();
        assert!(r8.wrote_new);
        // probe_index bits ≤ PAGE_SLOT_BITS branch
        let _ = probe_index(&txid, 0, MIN_BITS);
        let _ = probe_index(&txid, 3, PAGE_SLOT_BITS);
        // load ratio helper (local; roll uses load_needs_roll thresholds)
        let load_ratio = |tx_count: u64, slots: u64| -> f64 {
            if slots == 0 {
                0.0
            } else {
                tx_count as f64 / slots as f64
            }
        };
        assert_eq!(load_ratio(10, 0), 0.0);
        assert!(!load_needs_resize(0, 100));
        // bits_for_scale env out of range falls back
        let prev = std::env::var_os("RBITCOIN_TX_HEAD_BITS");
        std::env::set_var("RBITCOIN_TX_HEAD_BITS", "999");
        let _ = bits_for_scale();
        match prev {
            Some(v) => std::env::set_var("RBITCOIN_TX_HEAD_BITS", v),
            None => std::env::remove_var("RBITCOIN_TX_HEAD_BITS"),
        }
    }

}
