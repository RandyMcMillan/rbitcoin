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
//! **Concurrency:** at most **one** thread may insert into a given `tx.head`
//! (archive writer in IBD; single tip accept path after). Multi-writer races are
//! not supported. Online resize still uses `write_lock` only for final catch-up
//! + file swap.
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
//! resize ([`take_probe_depth_resize_request`]).
//!
//! **Mainnet default:** BITS=**26** → **256 MiB** sparse @ 4 B (`2^16` pages × 4 KiB).
//! Online resize widens BITS; entry width becomes 8 B at BITS ≥ 33 (page then 8 KiB —
//! two OS pages; future tuning). Load trigger: [`HEAD_LOAD_START`] (0.80).

use crate::error::StoreError;
use crate::file::{TableFile, TRAILING_FOOTER_LEN};
use crate::hashhead::HeadScale;
use rbitcoin_primitives::{Fk, TableKind};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

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

/// Set on the **first** insert that lands past [`PROBE_DEPTH_WARN`].
/// [`take_probe_depth_resize_request`] clears it for the tx.head owner to start resize.
static PROBE_DEPTH_RESIZE_REQUEST: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);

/// Depth threshold above which inserts count as “deep” for ops visibility and
/// trigger an early online resize request (first event only).
pub const PROBE_DEPTH_WARN: u32 = 128;

/// Mainnet address width (2^26 slots × 4 B = 256 MiB sparse). Online resize grows.
pub const MAINNET_BITS: u32 = 26;
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

/// File offset of the page that holds `txid`'s probe chain.
#[inline]
pub fn page_file_off(txid: &[u8; 32], bits: u32, entry_bytes: u8) -> u64 {
    let base = if bits <= PAGE_SLOT_BITS {
        0
    } else {
        page_index(txid, bits) << PAGE_SLOT_BITS
    };
    entry_file_off(base, entry_bytes)
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
    pub empty_local: u64,
    /// Encoded create_fk value written (valid if `wrote_new`).
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
/// Caps to the **slot data region** (`slot_region_len` = `slots × entry_bytes`),
/// never into the trailing footer.
#[inline]
pub fn page_pread_len(
    txid: &[u8; 32],
    bits: u32,
    entry_bytes: u8,
    table_slots: u64,
    slot_region_len: u64,
) -> usize {
    let base = if bits <= PAGE_SLOT_BITS {
        0
    } else {
        page_index(txid, bits) << PAGE_SLOT_BITS
    };
    let nslots = page_slot_count(bits).min(table_slots.saturating_sub(base));
    let want = nslots.saturating_mul(u64::from(entry_bytes));
    let off = entry_file_off(base, entry_bytes);
    let avail = slot_region_len.saturating_sub(off);
    // Entry-align so hop_scan never sees a torn trailing slot.
    let avail = (avail / u64::from(entry_bytes)) * u64::from(entry_bytes);
    want.min(avail) as usize
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

/// Head geometry large enough that `n` Class A rows sit **below**
/// [`HEAD_LOAD_START`] (so a recovery rebuild does not immediately start online
/// resize). Starts from [`bits_for_scale`] and widens until load is OK or
/// [`MAX_BITS`].
pub fn layout_for_count(n: u64) -> HeadLayout {
    let mut bits = bits_for_scale();
    while bits < MAX_BITS {
        let layout = HeadLayout::new(bits).expect("bits in range");
        if n == 0 || !load_needs_resize(n, layout.slots()) {
            return layout;
        }
        bits += 1;
    }
    HeadLayout::new(MAX_BITS).expect("MAX_BITS in range")
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

/// Rewrite layout+generation into an existing head file's trailing footer.
///
/// Used after resize swap to bump generation without reopening for write.
pub fn write_head_meta(
    head_path: &Path,
    layout: HeadLayout,
    generation: u64,
) -> Result<(), StoreError> {
    let (mut file, _) =
        TableFile::open_trailing_header_from_end(head_path, TableKind::HashHead)?;
    let body = file.data_len();
    if body != layout.body_bytes() {
        return Err(StoreError::Corrupt(
            "tx.head size mismatch writing footer layout",
        ));
    }
    file.set_trailing_ext(encode_layout_ext(layout, generation))?;
    // Best-effort: drop any pre-v5 sidecar so operators are not confused.
    remove_legacy_meta_sidecar(head_path);
    Ok(())
}

/// Full `tx.head` slot array built entirely in process RAM (no mmap / no online
/// resize shadow). Used for offline timing vs background `tx.head` resize.
///
/// Insert uses the same page-local probe as live [`AddressHead`]. After fill,
/// [`Self::write_to`] writes a complete on-disk table (slots + trailing footer)
/// that [`AddressHead::open`] can load.
pub struct RamAddressHead {
    layout: HeadLayout,
    /// `slots × entry_bytes` create_fk array (offset 0 = slot 0).
    slots: Vec<u8>,
    occupied: u64,
}

impl RamAddressHead {
    /// Allocate a zeroed slot array for `layout` (O(body size) RAM).
    pub fn new(layout: HeadLayout) -> Result<Self, StoreError> {
        let body = layout.body_bytes();
        let size = usize::try_from(body).map_err(|_| {
            StoreError::Corrupt("tx.head body larger than usize (cannot build in RAM)")
        })?;
        // Avoid trying multi‑TiB allocs from a bad bits env.
        if size > 0 && size.saturating_mul(1) > (48usize << 30) {
            return Err(StoreError::Corrupt(
                "tx.head RAM build refuses >48 GiB allocation",
            ));
        }
        Ok(Self {
            layout,
            slots: vec![0u8; size],
            occupied: 0,
        })
    }

    pub fn layout(&self) -> HeadLayout {
        self.layout
    }

    pub fn occupied(&self) -> u64 {
        self.occupied
    }

    pub fn body_bytes(&self) -> u64 {
        self.slots.len() as u64
    }

    /// Insert mappings into the RAM table (same probe as live head).
    ///
    /// Page-sorted for cache locality; idempotent if `fk` already on the chain.
    pub fn insert_many(&mut self, entries: &[([u8; 32], Fk)]) -> Result<(), StoreError> {
        if entries.is_empty() {
            return Ok(());
        }
        let bits = self.layout.bits;
        let es = self.layout.entry_bytes;
        let es_u = es as usize;
        let page_slots = page_slot_count(bits);
        let page_bytes = (page_slots as usize).saturating_mul(es_u);

        let mut work: Vec<(u64, usize, [u8; 32], Fk)> = entries
            .iter()
            .enumerate()
            .map(|(i, (txid, fk))| (page_base_for_txid(txid, bits), i, *txid, *fk))
            .collect();
        work.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.cmp(&b.1)));

        let mut i = 0;
        while i < work.len() {
            let page_base = work[i].0;
            let mut j = i + 1;
            while j < work.len() && work[j].0 == page_base {
                j += 1;
            }
            let page_off = entry_file_off(page_base, es) as usize;
            if page_off.saturating_add(page_bytes) > self.slots.len() {
                return Err(StoreError::Corrupt("ram head page out of range"));
            }
            let page = &mut self.slots[page_off..page_off + page_bytes];
            for &(_, _, ref txid, fk) in &work[i..j] {
                let outcome = insert_fk_into_page_buf(page, page_base, bits, es, txid, fk)?;
                if outcome.wrote_new {
                    self.occupied = self.occupied.saturating_add(1);
                }
            }
            i = j;
        }
        Ok(())
    }

    /// Write a complete trailing-footer `tx.head` file (not mmap-based create).
    ///
    /// File layout matches live tables: slot body at offset 0, then 32-byte
    /// trailing footer (magic + schema + kind + logical_len + layout ext).
    pub fn write_to(&self, path: impl AsRef<Path>) -> Result<(), StoreError> {
        use rbitcoin_primitives::{SCHEMA_VERSION, STORE_MAGIC, TableKind};
        use std::io::Write;

        let path = path.as_ref();
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent).map_err(|e| StoreError::io(path, e))?;
            }
        }
        let body = self.slots.len() as u64;
        let logical = body.saturating_add(crate::file::TRAILING_FOOTER_LEN as u64);
        let ext = encode_layout_ext(self.layout, 0);
        let mut footer = [0u8; crate::file::TRAILING_FOOTER_LEN];
        footer[0..4].copy_from_slice(&STORE_MAGIC);
        footer[4..6].copy_from_slice(&SCHEMA_VERSION.to_le_bytes());
        footer[6..8].copy_from_slice(&TableKind::HashHead.as_u16().to_le_bytes());
        footer[8..16].copy_from_slice(&logical.to_le_bytes());
        footer[16..32].copy_from_slice(&ext);

        let mut f = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(path)
            .map_err(|e| StoreError::io(path, e))?;
        // Single sequential write of the slot array (the comparison point vs
        // online uring page RMW). Chunk to keep syscall sizes reasonable.
        const CHUNK: usize = 16 * 1024 * 1024;
        let mut off = 0usize;
        while off < self.slots.len() {
            let end = (off + CHUNK).min(self.slots.len());
            f.write_all(&self.slots[off..end])
                .map_err(|e| StoreError::io(path, e))?;
            off = end;
        }
        f.write_all(&footer).map_err(|e| StoreError::io(path, e))?;
        f.sync_all().map_err(|e| StoreError::io(path, e))?;
        remove_legacy_meta_sidecar(path);
        Ok(())
    }
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
        // Trailing footer: slots at offset 0 so each 4 KiB probe page is OS-aligned.
        // Layout (bits/entry/generation) lives in the footer extension — no sidecar.
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
        if layout.bits >= 24 {
            rbitcoin_log::info!(
                "store: address-head create path={} bits={} slots={} entry={}B (~{:.2} GiB sparse, footer layout)",
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
        entry_file_off(slot, self.layout.entry_bytes)
    }

    /// Read one open-address entry (0 = empty).
    ///
    /// Hot path uses [`Self::load_page_slots`] (one page `read_at`). This remains
    /// for tests and rare single-slot diagnostics.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn read_entry(&self, slot: u64) -> Result<u64, StoreError> {
        let off = self.entry_off(slot);
        match self.layout.entry_bytes {
            4 => Ok(u64::from(self.file.load_u32_le(off)?)),
            8 => self.file.load_u64_le(off),
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

    /// FD for bulk io_uring / pread of head entries.
    #[inline]
    pub(crate) fn read_fd(&self) -> std::os::fd::RawFd {
        self.file.read_fd()
    }

    /// Full published file length (slot body + trailing footer).
    #[inline]
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn published_len(&self) -> u64 {
        self.file.logical_len()
    }

    /// Slot-array byte length only (excludes trailing footer) — batch pread cap.
    #[inline]
    pub(crate) fn slot_region_len(&self) -> u64 {
        self.file.data_len()
    }

    #[inline]
    pub(crate) fn path_str(&self) -> &std::path::Path {
        self.file.path()
    }

    pub fn reserve_additional(&self, _additional: u64) -> Result<(), StoreError> {
        Ok(())
    }

    /// Insert one mapping (no body IO). Sole writer.
    pub fn insert(&self, txid: &[u8; 32], new_fk: Fk) -> Result<(), StoreError> {
        self.insert_many(&[(*txid, new_fk)])
    }

    /// Plain mmap write of one create_fk slot (sole writer; no atomic RMW).
    ///
    /// Visibility for concurrent probes is via the **SeqCst fence** at the end of
    /// [`Self::insert_many`] (paired with Acquire fence in [`Self::load_page_slots`]).
    fn store_entry_plain(&self, slot: u64, new: u64) -> Result<(), StoreError> {
        let off = self.entry_off(slot);
        match self.layout.entry_bytes {
            4 => {
                if new > u64::from(u32::MAX) {
                    return Err(StoreError::InvalidFk);
                }
                self.file.write_at(off, &(new as u32).to_le_bytes())
            }
            8 => self.file.write_at(off, &new.to_le_bytes()),
            _ => Err(StoreError::Corrupt("address head entry_bytes")),
        }
    }

    /// Bump occupied after offline / RMW fills that wrote `n` new empty→fk slots.
    #[inline]
    pub(crate) fn note_inserts(&self, n: u64) {
        if n > 0 {
            self.occupied.fetch_add(n, Ordering::Relaxed);
        }
    }

    /// Bulk insert: **stable sort by probe page**, then original index (preserves
    /// call order within a page for rare same-batch duplicate txids).
    ///
    /// Per page: one [`load_page_slots`], multi [`insert_fk_into_page_buf`], plain
    /// slot stores for new empties. **SeqCst fence** once at end.
    ///
    /// Does **not** take [`lock_writes`] — that is only for resize swap.
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
            for &(_, _, ref txid, fk) in &work[i..j] {
                let outcome =
                    insert_fk_into_page_buf(&mut buf[..n], page_base, bits, es, txid, fk)?;
                if outcome.wrote_new {
                    note_probe_depth_on_insert(outcome.depth);
                    let global = page_base + outcome.empty_local;
                    self.store_entry_plain(global, outcome.stored_fk)?;
                    n_new = n_new.saturating_add(1);
                }
            }
            if n_new > 0 {
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
        let bits = self.layout.bits;
        let es = self.layout.entry_bytes;
        let page_slots = page_slot_count(bits);
        let page_base = if bits <= PAGE_SLOT_BITS {
            0
        } else {
            page_index(txid, bits) << PAGE_SLOT_BITS
        };
        let h1p = h1_in_page(txid, bits);
        let h2p = h2_in_page(txid, bits);
        let mut buf = [0u8; PROBE_REGION_BYTES];
        let n = self.load_page_slots(page_base, page_slots, &mut buf)?;
        let es_u = es as usize;
        if n < es_u {
            return Ok(Vec::new());
        }
        let nslots = (n / es_u) as u64;
        let scan = hop_scan_page(&buf[..n], es, h1p, h2p, nslots, MAX_PROBE);
        Ok(scan.cands.into_iter().map(|(_, e)| Fk(e)).collect())
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
        assert_eq!(MAX_PROBE, 1024);
        assert_eq!(PAGE_SLOTS, 1024);
        assert_eq!(PAGE_SLOT_BITS, 10);
        assert_eq!(PROBE_REGION_BYTES, 8192);
        assert_eq!(PROBE_DEPTH_WARN, 128);
        assert_eq!(MAINNET_BITS, 26);
        assert!(PROBE_REGION_BYTES as u64 >= PAGE_SLOTS * 8);
    }

    #[test]
    fn layout_helpers_stats_and_entry_bytes() {
        // Drain stats
        let _ = sample_probe_depth_stats();
        let _ = take_probe_depth_resize_request();
        assert!(!take_probe_depth_resize_request());
        let _ = probe_depth_stats_snapshot();

        assert!(matches!(
            HeadLayout::new(1),
            Err(StoreError::Corrupt(_))
        ));
        assert!(matches!(
            HeadLayout::with_entry_bytes(1, 4),
            Err(StoreError::Corrupt(_))
        ));
        assert!(matches!(
            HeadLayout::with_entry_bytes(16, 3),
            Err(StoreError::Corrupt(_))
        ));
        assert!(matches!(
            HeadLayout::with_entry_bytes(33, 4),
            Err(StoreError::Corrupt(_))
        ));
        let l = HeadLayout::with_entry_bytes(16, 8).unwrap();
        assert_eq!(l.slots(), 1 << 16);
        assert_eq!(l.entry_size(), 8);
        assert_eq!(l.body_bytes(), (1u64 << 16) * 8);
        assert_eq!(entry_bytes_for_bits(16), 4);
        assert_eq!(entry_bytes_for_bits(33), 8);

        let k = [0xCDu8; 32];
        // bits ≤ PAGE_SLOT_BITS → page_index returns 0
        assert_eq!(page_index(&k, PAGE_SLOT_BITS), 0);
        assert_eq!(page_index(&k, PAGE_SLOT_BITS.saturating_sub(1).max(MIN_BITS)), 0);
        let bits = 12u32;
        let pi = page_index(&k, bits);
        let h1 = h1_in_page(&k, bits);
        let h2 = h2_in_page(&k, bits);
        assert!(h1 < page_slot_count(bits));
        assert!(h2 < page_slot_count(bits) || page_slot_count(bits) > 0);
        // h1_in_page / h2 when bits ≤ PAGE_SLOT_BITS
        let _ = h1_in_page(&k, PAGE_SLOT_BITS);
        let _ = h2_in_page(&k, PAGE_SLOT_BITS);
        let _ = (pi, h1, h2);
        assert_eq!(
            page_base_for_txid(&k, bits),
            page_index(&k, bits) * page_slot_count(bits)
        );
        let off = page_file_off(&k, bits, 4);
        assert_eq!(off, entry_file_off(page_base_for_txid(&k, bits), 4));
        assert!(page_pread_len(&k, bits, 4, 1 << bits, (1 << bits) * 4) > 0);
        // Empty / OOB local → None; zero fk may still decode as 0 depending on layout.
        assert!(entry_from_page_buf(&[0u8; 4], 9, 4).is_none());
        let mut buf = [0u8; 8];
        buf[0..4].copy_from_slice(&7u32.to_le_bytes());
        assert_eq!(entry_from_page_buf(&buf, 0, 4), Some(7));

        assert!(load_needs_resize(100, 64));
        assert!(!load_needs_resize(1, 1024));
        assert!(load_ratio(50, 100) > 0.0);
        let _ = bits_for_scale();
        let _ = default_layout();
        let _ = layout_for_count(0);
        let _ = layout_for_count(1_000_000);

        let ext = encode_layout_ext(HeadLayout::new(10).unwrap(), 3);
        let (dec, gen) = decode_layout_ext(&ext).unwrap();
        assert_eq!(dec.bits, 10);
        assert_eq!(gen, 3);
        assert!(decode_layout_ext(&[0xff; 16]).is_err());

        assert!(is_probe_exhausted_error(&StoreError::Corrupt(
            "address head probe exhausted on insert"
        )));
        assert!(!is_probe_exhausted_error(&StoreError::NotFound));

        let p = tmp("legacy-meta");
        remove_legacy_meta_sidecar(&p); // no-op missing
        let _ = std::fs::remove_file(&p);
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
        let thr = ((slots as f64) * HEAD_LOAD_START).ceil() as u64;
        assert_eq!(thr, 820); // ceil(0.80 * 1024)
        assert!(!load_needs_resize(thr - 1, slots));
        assert!(load_needs_resize(thr, slots));
        assert!(load_needs_resize(slots, slots));
    }

    #[test]
    fn layout_for_count_avoids_immediate_resize() {
        // ~103M Class A: default MAINNET 26 is too small; need at least 27.
        let n = 102_956_483u64;
        let layout = layout_for_count(n);
        assert!(layout.bits >= 27, "bits={}", layout.bits);
        assert!(!load_needs_resize(n, layout.slots()));
        // Empty / tiny stays at scale default.
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
        assert_eq!(h.slot_region_len(), h.slots() * u64::from(es));
        assert!(h.published_len() > h.slot_region_len());
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

    /// RAM build + write must open as a live AddressHead with the same mappings.
    #[test]
    fn ram_head_build_write_open_roundtrip() {
        let path = tmp("ram_build");
        let layout = HeadLayout::new(14).unwrap();
        let mut ram = RamAddressHead::new(layout).unwrap();
        let mut entries = Vec::new();
        for i in 1..=200u64 {
            let mut txid = [0u8; 32];
            txid[0] = (i & 0xff) as u8;
            txid[1] = ((i >> 8) & 0xff) as u8;
            txid[2] = ((i * 13) & 0xff) as u8;
            txid[4] = 0x5a;
            entries.push((txid, Fk(i)));
        }
        ram.insert_many(&entries).unwrap();
        assert_eq!(ram.occupied(), 200);
        // Idempotent re-insert.
        ram.insert_many(&entries[..20]).unwrap();
        assert_eq!(ram.occupied(), 200);
        ram.write_to(&path).unwrap();

        let h = AddressHead::open(&path).unwrap();
        assert_eq!(h.bits(), 14);
        assert_eq!(h.occupied(), 200);
        for (txid, fk) in &entries {
            assert!(
                h.probe_fks(txid).unwrap().contains(fk),
                "missing {fk:?} after RAM write"
            );
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
    fn resize_control_roundtrip_corrupt_and_paths() {
        let path = tmp("resize_ctrl");
        // Create empty head so path exists as a parent identity.
        let h = AddressHead::create_with_bits(&path, 12).unwrap();
        drop(h);
        assert!(read_resize_control(&path).unwrap().is_none());
        let ctrl = ResizeControl {
            target: HeadLayout::new(13).unwrap(),
            cursor: 42,
            generation: 7,
        };
        write_resize_control(&path, &ctrl).unwrap();
        let got = read_resize_control(&path).unwrap().unwrap();
        assert_eq!(got.cursor, 42);
        assert_eq!(got.generation, 7);
        assert_eq!(got.target.bits, 13);
        // Corrupt magic
        let rpath = resize_control_path(&path);
        std::fs::write(&rpath, b"XXXX________________").unwrap();
        assert!(matches!(
            read_resize_control(&path),
            Err(StoreError::Corrupt(_))
        ));
        clear_resize_control(&path);
        assert!(read_resize_control(&path).unwrap().is_none());
        // Path helpers
        assert!(shadow_head_path(&path)
            .to_string_lossy()
            .ends_with(".new"));
        assert!(bak_head_path(&path).to_string_lossy().ends_with(".bak"));
        // note_probe helpers
        let _ = take_probe_depth_resize_request(); // clear
        note_probe_depth_on_insert(PROBE_DEPTH_WARN); // no-op at threshold
        note_probe_depth_on_insert(PROBE_DEPTH_WARN + 1); // first deep
        assert!(take_probe_depth_resize_request());
        note_probe_exhausted();
        let (deep, exh) = probe_depth_stats_snapshot();
        assert!(deep >= 1 || exh >= 1);
        assert!(is_probe_exhausted_error(&StoreError::Corrupt(
            "address head probe exhausted on insert"
        )));
        assert!(!is_probe_exhausted_error(&StoreError::Corrupt("other")));
        let _ = std::fs::remove_file(&path);
        clear_resize_control(&path);
    }

    #[test]
    fn mainnet_default_bits_is_26() {
        assert_eq!(MAINNET_BITS, 26);
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
        // load_ratio zero slots
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

    #[test]
    fn trailing_footer_page_aligned_slots_and_layout() {
        let path = tmp("trail_align");
        let h = AddressHead::create_with_bits(&path, 16).unwrap();
        // Slot 0 at file offset 0; page boundaries on 4 KiB.
        assert_eq!(entry_file_off(0, 4), 0);
        assert_eq!(entry_file_off(1024, 4), 4096);
        let body = HeadLayout::new(16).unwrap().body_bytes();
        assert_eq!(body % 4096, 0);
        let st = std::fs::metadata(&path).unwrap();
        assert_eq!(st.len(), body + TRAILING_FOOTER_LEN as u64);
        // Footer not overlapping first page.
        assert!(st.len() >= 4096 + TRAILING_FOOTER_LEN as u64 || body < 4096);
        // No sidecar meta file.
        assert!(!meta_path(&path).exists());
        h.insert(&[1u8; 32], Fk(1)).unwrap();
        drop(h);
        let h2 = AddressHead::open(&path).unwrap();
        assert!(h2.probe_fks(&[1u8; 32]).unwrap().contains(&Fk(1)));
        assert_eq!(h2.bits(), 16);
        assert_eq!(h2.generation(), 0);
        // Footer layout round-trips after generation bump.
        write_head_meta(&path, HeadLayout::new(16).unwrap(), 7).unwrap();
        let h3 = AddressHead::open(&path).unwrap();
        assert_eq!(h3.generation(), 7);
        assert!(h3.probe_fks(&[1u8; 32]).unwrap().contains(&Fk(1)));
        let _ = std::fs::remove_file(&path);
    }
}
