//! Schema-30 spend delta: where every historical spend slot is recorded.
//!
//! `spent.body` holds one 8-byte slot per output, but Class A writes none of
//! its bytes: an append only moves its published end, and the file stays as
//! short as the furthest page a fold wrote. Every spend is a delta note, and
//! a fold is the only writer of `spent.body`. A page no fold wrote is never
//! read there: it reads zero wherever the delta holds nothing.
//!
//! Everything lives in `store/spent.delta/`:
//!
//! - `seg.<id>`: append-only segments of page records, each holding
//!   [`Policy::segment_cap`] record bytes past a first block that holds only
//!   the header. A record holds every override its page has over its base,
//!   explicit zeroes included. After a fold, the next record for that page
//!   holds only the notes since.
//! - `idx.<id>` and `log.<id>`: the page directory. The base holds one
//!   12-byte entry per body page; the log holds one checksummed batch per
//!   generation since, each setting the directory length and the entries
//!   that generation wrote. An entry is the generation that wrote it and a
//!   packed `Placed`: zero, a page `spent.body` holds with its nonzero slot
//!   count in units of eight, or a record in a segment over either base.
//!   Once the log outgrows its base, a checkpoint writes a new base and an
//!   empty log, and removes the old pair after its commit.
//! - `commit`: generation, directory length, the active segment and its end,
//!   lifetime write counters, the directory base and the log's end, and a
//!   ring of recent generations with the confirmed tip each sealed spends up
//!   to.
//! - `fold`: whole composed pages a fold is copying over `spent.body` pages
//!   readers use. It exists only between that copy and its commit.
//!
//! No write touches a 4 KiB block holding bytes the commit names, so a block
//! a crash tears to zeroes loses nothing committed: each generation's
//! segment records and log batch start on a fresh block past the segment's
//! header block, a base is never written again once named, and no delta
//! file header is rewritten. A fold onto a page `spent.body` holds goes
//! through `fold`; one onto a page no fold wrote goes straight to
//! `spent.body`, since nothing reads it there until the commit.
//!
//! Confirm notes spend slots in RAM, in a hash map by slot index sized from
//! the window before it: after a window at [`OPEN_EDGE_CAP`], the map takes
//! the cap and a full write phase past it without regrowing; after a smaller
//! window (a timer checkpoint, a failed one, or the first after open) it may
//! grow once. A checkpoint publishes one generation:
//!
//! 1. seal the open notes: under the exclusion confirm and disconnect take,
//!    swap in a map allocated before it; after a disconnect, a note whose
//!    spender left the best chain becomes an explicit zero. Then, outside
//!    the exclusion, sort the frozen notes into slot order, 16 bytes a note;
//! 2. merge each sealed page's notes over its committed record, folding
//!    instead a committed record that already spends half its page's unset
//!    slots, so the page's new record holds only the notes;
//! 3. take the sparsest eligible segments whose live share is under a
//!    threshold that rises with the delta's share of `spent.body`, reading
//!    only their live records: fold each one that spends at least half its
//!    page's unset slots, and keep the others to carry. Folds run in rounds,
//!    each composed, written and synced into `spent.body`;
//! 4. append the sealed pages' records and the carried ones to the active
//!    segment, rolling at the cap, and sync the segments;
//! 5. write each entry, records and the folded pages, as one log batch (or a
//!    new base), and sync it;
//! 6. rename `commit` and sync `spent.delta/`;
//! 7. install the new entries in RAM and drop the sealed layer, under one lock;
//! 8. remove `fold`, and unlink segments no entry names once no reader holds
//!    them.
//!
//! The commit is the only authority. Load reads it first, copies a leftover
//! `fold` onto `spent.body` again, ignores the table headers, reads the
//! directory base and the log up to the committed end, ignores what lies past
//! the committed lengths, and deletes segments and directory files nothing
//! names. After a crash the next generation number, and the next segment id,
//! are the ones the crashed attempt used; a crashed attempt's records and
//! batch lie past the committed ends, so nothing it wrote is selected. In one
//! process, generations are not reused, records are never written over, and
//! a failed attempt keeps its sealed layer, so the retry rewrites every
//! sealed page the failed attempt touched. A rename that landed before an
//! error is installed, so the next generation builds on the commit on disk.
//!
//! A fold only takes a record whose segment's live records all come from
//! generations sealed at or below the durable spend marker. A slot in
//! `spent.body` is out of reach of [`SpendPageLog::drop_spends_above`], and
//! below the marker every spender is durable in Class A, so no crash can hand
//! its fk to another create. A fold writes whole pages, so it takes only
//! pages wholly below the published end, and never page 0, whose first
//! block holds the `spent.body` header.

use crate::error::StoreError;
use crate::file::{
    fsync_parent_dir, write_synced_tmp_rename, GrowPolicy, TableFile, FILE_HEADER_LEN,
};
use crate::tx_table::BODY_PAGE_SIZE;
use rbitcoin_primitives::{schema_file_openable, TableKind, FILE_SCHEMA_VERSION, STORE_MAGIC};
use std::cell::Cell;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::hash::BuildHasherDefault;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::time::Instant;

/// Directory under the store that holds every delta file.
pub const SPEND_DELTA_DIR: &str = "spent.delta";
pub const SPEND_DELTA_COMMIT_NAME: &str = "commit";
/// Composed pages a fold is copying into `spent.body`.
pub const SPEND_DELTA_FOLD_NAME: &str = "fold";

/// File name of segment `id` inside [`SPEND_DELTA_DIR`].
pub fn segment_name(id: u32) -> String {
    format!("seg.{id}")
}

/// File name of the directory base the commit names as `id`.
pub fn idx_name(id: u32) -> String {
    format!("idx.{id}")
}

/// File name of the directory log appended after base `id`.
pub fn log_name(id: u32) -> String {
    format!("log.{id}")
}

/// Open slot deltas that ask for a checkpoint before the timer and make the
/// next confirm insert wait for it. The only early trigger: a record holds
/// only its page's entries, so open pages cost what their edges cost.
pub const OPEN_EDGE_CAP: u64 = 2_000_000;

/// The most notes an open map is sized for: 2^22 buckets at hashbrown's 7/8
/// load, 17 bytes a bucket (about 68 MiB). Confirm waits for room once per
/// write phase, which can merge a full `writeq` of runs, each 8,000 inputs
/// plus one block (about 450,000 notes past the cap at most), so a window
/// that fills to the cap never regrows a map of this size.
const OPEN_NOTES_ROOM: usize = (1 << 22) / 8 * 7;

/// Checkpoints that fail in a row before a confirm waiting at
/// [`OPEN_EDGE_CAP`] gives up with an error instead of parking: a disk that
/// keeps failing stops the node rather than stalling it with only warnings.
/// The sync thread retries about once a second while the cap is armed.
pub const CHECKPOINT_FAILURES_STOP: u32 = 60;

/// Segment size and reclaim thresholds.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Policy {
    /// Record bytes a segment holds past its first block; a record that
    /// would end past them rolls to a new segment. At most `2 << OFF_BITS`,
    /// the largest offset a record can name.
    pub segment_cap: u64,
    /// Below this many segments the threshold is [`Policy::sparse_permille`]
    /// alone.
    pub reclaim_from_segments: usize,
    /// Reclaim never leaves fewer segments than this.
    pub keep_segments: usize,
    /// Floor of the reclaim threshold, in thousandths of a segment's bytes
    /// live: a segment this sparse is reclaimed at any delta size.
    pub sparse_permille: u64,
    /// The reclaim threshold, in thousandths live, is this many times the
    /// delta's size in thousandths of `spent.body`. At 1000 or more, the
    /// ceiling, reclaim takes any segment and folds every record it can.
    pub reclaim_slope: u64,
    /// One checkpoint reclaims at most this many times the larger of its own
    /// record bytes and one segment.
    pub reclaim_rate: u64,
    /// Pages one fold round composes into the `fold` file before it copies
    /// them into `spent.body` and syncs; a checkpoint runs as many rounds as
    /// it has pages to fold.
    pub fold_round_pages: usize,
    /// The directory log folds into a new base once it is longer than this
    /// and than the base.
    pub log_fold_min: u64,
    /// Open notes, plus a failed checkpoint's sealed ones, at which confirm
    /// waits for a checkpoint. [`OPEN_EDGE_CAP`] outside tests.
    pub open_edge_cap: u64,
}

impl Policy {
    /// 16 MiB segments. Reclaim segments less live than twice the delta's
    /// share of `spent.body`, and any under 5% live: a delta at 10% of
    /// `spent.body` takes segments under 20% live, one at 25% those under
    /// half, and past 50% every segment. Reclaim at most twice a checkpoint's
    /// inflow, never below two segments, and fold 4,096 pages a round.
    pub const DEFAULT: Self = Self {
        segment_cap: 16 << 20,
        reclaim_from_segments: 3,
        keep_segments: 2,
        sparse_permille: 50,
        reclaim_slope: 2,
        reclaim_rate: 2,
        fold_round_pages: 4096,
        log_fold_min: 4 << 20,
        open_edge_cap: OPEN_EDGE_CAP,
    };

    /// The live share, in thousandths, under which a segment is reclaimed
    /// while the delta holds `total` bytes in `count` segments. At 1000 or
    /// more every segment qualifies.
    fn reclaim_below(&self, total: u64, count: usize, published: u64) -> u64 {
        if count < self.reclaim_from_segments {
            return self.sparse_permille;
        }
        let share = total.saturating_mul(1000) / published.max(1);
        self.sparse_permille
            .max(self.reclaim_slope.saturating_mul(share))
    }
}

/// `spent.body` as the fold sees it.
pub trait SpentBody {
    /// Published length.
    fn published_len(&self) -> u64;
    /// Read published bytes at `abs`.
    fn read(&self, abs: u64, buf: &mut [u8]) -> Result<(), StoreError>;
    /// Write a composed page image at `abs`, wholly below the published length.
    fn write(&mut self, abs: u64, bytes: &[u8]) -> Result<(), StoreError>;
    fn sync(&mut self) -> Result<(), StoreError>;
}

/// Lifetime write counters and current sizes, for the operator log.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SpendDeltaStats {
    /// Record bytes appended to segments.
    pub record_bytes: u64,
    /// Directory bytes written: log batches and new bases.
    pub idx_bytes: u64,
    /// Composed page bytes the fold wrote, to the `fold` file and to
    /// `spent.body` each.
    pub fold_bytes: u64,
    /// Records folded; each dirties one `spent.body` page.
    pub fold_records: u64,
    /// Segments on disk.
    pub segments: u64,
    /// Bytes in those segments.
    pub segment_bytes: u64,
    /// Bytes of records an entry names.
    pub live_bytes: u64,
    /// RAM the open, frozen and sealed notes allocate.
    pub notes_bytes: u64,
    /// RAM the page directory allocates: one entry per `spent.body` page up
    /// to the highest page a generation names.
    pub directory_bytes: u64,
}

impl SpendDeltaStats {
    /// Every byte written for historical spend tracking.
    pub fn written(&self) -> u64 {
        self.record_bytes
            .saturating_add(self.idx_bytes)
            .saturating_add(self.fold_bytes)
    }
}

/// What one checkpoint read and wrote, and how long each step took, for the
/// operator log. Times are wall milliseconds.
#[derive(Clone, Debug, Default)]
pub struct CheckpointReport {
    /// Pages and slot notes the checkpoint sealed.
    pub sealed_pages: u64,
    pub sealed_edges: u64,
    /// Since the previous checkpoint in this process started.
    pub since_ms: u64,
    /// Preads of the committed records the sealed notes merge into, records
    /// touching in a segment sharing one, and the bytes they read.
    pub merge_reads: u64,
    pub merge_read_bytes: u64,
    pub merge_ms: u64,
    /// Segments reclaimed, and the bytes read from them.
    pub reclaim_segments: u64,
    pub reclaim_read_bytes: u64,
    pub reclaim_read_ms: u64,
    /// Touched pages whose committed record spends half their unset slots:
    /// folded, or kept in the delta because their record's segment holds a
    /// generation above the durable marker, or because the page is not
    /// wholly below the published end.
    pub hot_folds: u64,
    /// Record bytes of the touch-folded records: what rewriting them would
    /// have cost, against about 8 KiB of `fold` and `spent.body` writes per
    /// fold.
    pub hot_fold_record_bytes: u64,
    pub dense_kept_gen: u64,
    pub dense_kept_tail: u64,
    /// Live records in those segments: folded, carried, or dropped because
    /// this checkpoint rewrites their page.
    pub folded: u64,
    pub carried: u64,
    pub dropped: u64,
    /// Fold rounds, and reading the `spent.body` pages they compose over.
    pub fold_rounds: u64,
    pub fold_compose_ms: u64,
    /// Writing and syncing the `fold` file.
    pub fold_file_ms: u64,
    pub body_write_ms: u64,
    pub body_sync_ms: u64,
    /// Record bytes appended to segments.
    pub record_bytes: u64,
    pub seg_write_ms: u64,
    pub seg_sync_ms: u64,
    /// `idx` entries written.
    pub idx_entries: u64,
    pub idx_write_ms: u64,
    pub idx_sync_ms: u64,
    /// Renaming `commit` and syncing the directory.
    pub commit_ms: u64,
    /// Installing the generation in RAM.
    pub install_ms: u64,
    pub unlink_ms: u64,
    pub total_ms: u64,
}

/// A checkpoint's detail line is INFO when it is this slow in total.
pub const CHECKPOINT_SLOW_TOTAL_MS: u64 = 1_000;
/// A checkpoint's detail line is INFO when its body and segment syncs
/// together take this long.
pub const CHECKPOINT_SLOW_SYNC_MS: u64 = 500;

/// Whether the detailed checkpoint line belongs at INFO rather than DEBUG.
#[doc(hidden)]
pub fn checkpoint_detail_is_info(r: &CheckpointReport) -> bool {
    r.total_ms >= CHECKPOINT_SLOW_TOTAL_MS
        || r.body_sync_ms.saturating_add(r.seg_sync_ms) >= CHECKPOINT_SLOW_SYNC_MS
}

fn ms_since(t: Instant) -> u64 {
    t.elapsed().as_millis() as u64
}

const SLOT: usize = 8;
const PAGE: u64 = BODY_PAGE_SIZE;
const SLOTS_PER_PAGE: usize = PAGE as usize / SLOT;
const HEAD: u64 = FILE_HEADER_LEN as u64;
/// Record: `page_off:u64 | count:u16 | count × (rel:u16, meta:[u8; 8])`.
const REC_HEAD: usize = 10;
const REC_ENTRY: usize = 2 + SLOT;
/// Directory entry: `generation:u32 | placed:u64`, the packed [`Placed`].
/// Generation zero is a page no generation wrote: it reads zero.
const DIR_ENTRY: u64 = 12;
type Entry = [u8; DIR_ENTRY as usize];
/// `log.<id>` batch header: `generation:u32 | count:u32 | dir_pages:u32 |
/// crc32:u32`, then `count` entries. The checksum covers the header with a
/// zero checksum field, then the entries.
const BATCH_HEAD: u64 = 16;
/// `log.<id>` entry: `page:u32 | entry`.
const LOG_ENTRY: u64 = 4 + DIR_ENTRY;
/// Each generation's writes to a segment or the log start on this
/// boundary, so no write touches a block holding bytes a commit names. A
/// segment's first block holds only its header.
const BLOCK: u64 = 4096;
/// Directory pages read per `pread` at load.
const LOAD_CHUNK_PAGES: u64 = 32 * 1024;
/// Commit layout tag. Earlier schema-30 builds wrote layouts 0 and 2 to 5.
const COMMIT_LAYOUT: u16 = 6;
/// `(generation, sealed tip)` pairs the commit keeps.
const RING: usize = 8;
const RING_AT: usize = 80;
const COMMIT_LEN: usize = RING_AT + RING * 8;
const ZERO: Slot = [0u8; SLOT];

// Bulk submissions and serial record preads of confirm-style delta lookups
// on this thread. A checkpoint merge counts one bulk submission per chunk.
// Reclaim does not count. Thread-local so a parallel suite does not mix
// another test's calls into the count.
// Always on, so a release test sees the same split a debug test does.
thread_local! {
    static DELTA_RECORD_BULK: Cell<u64> = const { Cell::new(0) };
    static DELTA_RECORD_SERIAL: Cell<u64> = const { Cell::new(0) };
    /// 0 unset, 1 every span still shared its file, 2 a span was the sole owner.
    static DELTA_RECORD_OWNER: Cell<u8> = const { Cell::new(0) };
    /// Off on a node. A test arms it for one lookup. The walk is `strong_count`.
    static DELTA_RECORD_OWNER_ARM: Cell<bool> = const { Cell::new(false) };
}

fn note_delta_record_serial() {
    DELTA_RECORD_SERIAL.with(|n| n.set(n.get() + 1));
}

fn note_delta_record_bulk() {
    DELTA_RECORD_BULK.with(|n| n.set(n.get() + 1));
}

fn owner_probe_armed() -> bool {
    DELTA_RECORD_OWNER_ARM.with(Cell::get)
}

fn note_delta_record_owner(held: bool) {
    DELTA_RECORD_OWNER.with(|c| {
        let next = if held { 1 } else { 2 };
        if c.get() != 2 {
            c.set(next);
        }
    });
}

/// Clear [`delta_record_io`] on this thread.
#[doc(hidden)]
pub fn reset_delta_record_io() {
    DELTA_RECORD_BULK.with(|n| n.set(0));
    DELTA_RECORD_SERIAL.with(|n| n.set(0));
    DELTA_RECORD_OWNER.with(|n| n.set(0));
    DELTA_RECORD_OWNER_ARM.with(|n| n.set(false));
}

/// Count shared owners on the next bulk read on this thread.
/// [`reset_delta_record_io`] clears it. A node leaves it off.
#[doc(hidden)]
pub fn arm_delta_record_owner_probe() {
    DELTA_RECORD_OWNER_ARM.with(|c| c.set(true));
}

/// `(bulk submissions, serial record preads)` since [`reset_delta_record_io`]
/// on this thread.
#[doc(hidden)]
pub fn delta_record_io() -> (u64, u64) {
    (
        DELTA_RECORD_BULK.with(Cell::get),
        DELTA_RECORD_SERIAL.with(Cell::get),
    )
}

/// Whether every bulk span since [`reset_delta_record_io`] still shared its
/// segment file with another owner at the read. A sole owner means this
/// lookup already dropped the `Arc` `unlink_doomed` is waiting on.
#[doc(hidden)]
pub fn delta_record_owner_held() -> bool {
    DELTA_RECORD_OWNER.with(|c| c.get() == 1)
}
/// [`Placed`] fields: `segment:22 | offset:23 | len:12 | base:7`.
const SEG_BITS: u32 = 22;
const OFF_BITS: u32 = 23;
const LEN_BITS: u32 = 12;
const BASE_BITS: u32 = 7;
/// A filled count is kept in units of this many slots, rounded up.
const BASE_UNIT: u16 = 8;

type Slot = [u8; SLOT];

/// A note's key: the index of the slot holding byte `abs`, as
/// [`page_of`] and [`rel_of`] place it.
fn slot_key(abs: u64) -> u64 {
    abs / SLOT as u64
}

fn key_page(key: u64) -> u64 {
    key / SLOTS_PER_PAGE as u64 * PAGE
}

fn key_rel(key: u64) -> u16 {
    (key % SLOTS_PER_PAGE as u64) as u16
}

/// Slot index to note, for the notes confirm adds between freezes.
type OpenNotes = crate::int_map::SlotMap<Slot>;

/// A map for the next window, sized from the `recent` notes the window
/// ending now holds plus a quarter: a window at the cap gets
/// [`OPEN_NOTES_ROOM`] and never regrows, one at the tip stays small.
fn open_notes_after(recent: usize) -> OpenNotes {
    let notes = recent.saturating_add(recent / 4).min(OPEN_NOTES_ROOM);
    OpenNotes::with_capacity_and_hasher(notes, BuildHasherDefault::default())
}

/// What an [`OpenNotes`] allocates: its buckets and their control bytes.
fn open_notes_bytes(m: &OpenNotes) -> u64 {
    rbitcoin_primitives::heap_bytes::hash_table_bytes::<u64, Slot>(m.capacity())
}

/// Frozen notes sorted by slot index, 16 bytes each, so a page's notes are
/// one run in slot order. `dir[b]` is the first note at or past key-range
/// bucket `b`: a lookup searches only its bucket. One bucket per 16 to 64
/// notes keeps the directory under a quarter byte a note (256 KiB at the
/// cap) while a bucket search stays a few compares.
struct SealedNotes {
    notes: Box<[(u64, Slot)]>,
    dir: Box<[u32]>,
    base: u64,
    shift: u32,
}

impl SealedNotes {
    /// `frozen` over `prior`, the notes a failed checkpoint left sealed.
    fn seal(frozen: &OpenNotes, prior: Option<&SealedNotes>) -> Self {
        let mut fresh: Vec<(u64, Slot)> = Vec::with_capacity(frozen.len());
        fresh.extend(frozen.iter().map(|(&k, &m)| (k, m)));
        fresh.sort_unstable_by_key(|&(k, _)| k);
        let notes = match prior {
            None => fresh,
            Some(p) => newer_over(&p.notes, &fresh),
        };
        Self::index(notes.into_boxed_slice())
    }

    fn index(notes: Box<[(u64, Slot)]>) -> Self {
        let (Some(&(base, _)), Some(&(last, _))) = (notes.first(), notes.last()) else {
            return Self {
                notes,
                dir: Box::new([0]),
                base: 0,
                shift: 0,
            };
        };
        let buckets = (notes.len().next_power_of_two() / 32).max(1) as u64;
        let mut shift = 0;
        while ((last - base) >> shift) + 1 > buckets {
            shift += 1;
        }
        let n = ((last - base) >> shift) as usize + 1;
        let mut dir = Vec::with_capacity(n + 1);
        let mut i = 0usize;
        for b in 0..=n {
            while i < notes.len() && (((notes[i].0 - base) >> shift) as usize) < b {
                i += 1;
            }
            dir.push(i as u32);
        }
        Self {
            notes,
            dir: dir.into_boxed_slice(),
            base,
            shift,
        }
    }

    /// Index of the first note at or past `key`.
    fn lower_bound(&self, key: u64) -> usize {
        if key <= self.base {
            return 0;
        }
        let b = ((key - self.base) >> self.shift) as usize;
        if b + 1 >= self.dir.len() {
            return self.notes.len();
        }
        let (lo, hi) = (self.dir[b] as usize, self.dir[b + 1] as usize);
        lo + self.notes[lo..hi].partition_point(|&(k, _)| k < key)
    }

    fn get(&self, key: u64) -> Option<Slot> {
        self.notes
            .get(self.lower_bound(key))
            .filter(|&&(k, _)| k == key)
            .map(|&(_, m)| m)
    }

    fn has_page(&self, page_off: u64) -> bool {
        let first = slot_key(page_off);
        self.notes
            .get(self.lower_bound(first))
            .is_some_and(|&(k, _)| k < first + SLOTS_PER_PAGE as u64)
    }

    /// Each page's notes, pages ascending.
    fn pages(&self) -> impl Iterator<Item = (u64, &[(u64, Slot)])> {
        self.notes
            .chunk_by(|a, b| key_page(a.0) == key_page(b.0))
            .map(|run| (key_page(run[0].0), run))
    }

    fn len(&self) -> usize {
        self.notes.len()
    }

    fn bytes(&self) -> u64 {
        (std::mem::size_of_val(&*self.notes) + std::mem::size_of_val(&*self.dir)) as u64
    }
}

/// Two sorted note runs as one; `newer` wins a key both hold.
fn newer_over(older: &[(u64, Slot)], newer: &[(u64, Slot)]) -> Vec<(u64, Slot)> {
    let mut out = Vec::with_capacity(older.len() + newer.len());
    let (mut i, mut j) = (0, 0);
    while i < older.len() && j < newer.len() {
        let (o, n) = (older[i], newer[j]);
        if o.0 < n.0 {
            out.push(o);
            i += 1;
            continue;
        }
        out.push(n);
        j += 1;
        if o.0 == n.0 {
            i += 1;
        }
    }
    out.extend_from_slice(&older[i..]);
    out.extend_from_slice(&newer[j..]);
    out
}

/// `spend_durable` sync thread wakes on this.
pub type SpendSyncWake = Arc<(Mutex<bool>, Condvar)>;

/// Make the next commit rename under `store` land and then report an
/// error, as a failed directory sync after the rename would.
#[cfg(debug_assertions)]
#[doc(hidden)]
pub fn fail_after_next_commit_rename(store: &Path) {
    FAIL_AFTER_RENAME
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .insert(store.join(SPEND_DELTA_DIR));
}

#[cfg(debug_assertions)]
static FAIL_AFTER_RENAME: Mutex<BTreeSet<PathBuf>> = Mutex::new(BTreeSet::new());

/// Make the next `fold` removal under `store` fail.
#[cfg(debug_assertions)]
#[doc(hidden)]
pub fn fail_next_fold_removal(store: &Path) {
    FAIL_FOLD_REMOVAL
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .insert(store.join(SPEND_DELTA_DIR));
}

#[cfg(debug_assertions)]
static FAIL_FOLD_REMOVAL: Mutex<BTreeSet<PathBuf>> = Mutex::new(BTreeSet::new());

#[cfg(debug_assertions)]
fn take_fail_after_rename(dir: &Path) -> bool {
    FAIL_AFTER_RENAME
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .remove(dir)
}

/// True when the 8-byte slot lies inside the published body.
pub fn is_historical(published: u64, abs: u64) -> bool {
    abs.saturating_add(SLOT as u64) <= published
}

fn page_of(abs: u64) -> u64 {
    abs & !(PAGE - 1)
}

fn rel_of(abs: u64) -> u16 {
    ((abs - page_of(abs)) / SLOT as u64) as u16
}

fn mask(bits: u32) -> u64 {
    (1 << bits) - 1
}

/// What lies under a page's delta: zeroes, or a `spent.body` page a fold
/// wrote, with its nonzero slot count in [`BASE_UNIT`]s rounded up. A page
/// no fold wrote is never read: Class A appends no `spent.body` bytes, so
/// whatever the file holds there (nothing, or a fold a crash left
/// uncommitted) is not this page.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct Base(u8);

impl Base {
    const ZERO: Self = Self(0);

    /// The base a page image with `filled` nonzero slots leaves.
    fn of(filled: usize) -> Self {
        Self((filled as u64).div_ceil(u64::from(BASE_UNIT)) as u8)
    }

    fn is_body(self) -> bool {
        self.0 != 0
    }

    /// Unset slots, at least: the count is rounded up.
    fn unset(self, page_off: u64) -> u64 {
        let slots = (SLOTS_PER_PAGE - first_rel(page_off)) as u64;
        slots.saturating_sub(u64::from(self.0) * u64::from(BASE_UNIT))
    }
}

/// One committed page entry in RAM: eight bytes per `spent.body` page,
/// about 50 MB for a 25 GB body. Zero is a page no generation wrote. A
/// segment of zero with a nonzero base is a page that reads `spent.body`; a
/// record is in a segment numbered from one and lies over its base.
#[derive(Clone, Copy, Default, Debug, PartialEq, Eq)]
struct Placed(u64);

enum At {
    Zero,
    Body,
    Rec {
        seg: u32,
        offset: u64,
        len: usize,
        base: Base,
    },
}

impl Placed {
    fn rec(seg: u32, offset: u64, len: usize, base: Base) -> Result<Self, StoreError> {
        let off = offset.wrapping_sub(BLOCK);
        if seg == 0
            || u64::from(seg) > mask(SEG_BITS)
            || offset < BLOCK
            || !off.is_multiple_of(2)
            || off / 2 > mask(OFF_BITS)
            || !len.is_multiple_of(2)
            || len as u64 / 2 > mask(LEN_BITS)
        {
            return Err(StoreError::Corrupt("spent.delta record out of range"));
        }
        Ok(Self(
            u64::from(seg) << (OFF_BITS + LEN_BITS + BASE_BITS)
                | (off / 2) << (LEN_BITS + BASE_BITS)
                | (len as u64 / 2) << BASE_BITS
                | u64::from(base.0),
        ))
    }

    /// A page whose slots are all in `spent.body` over `base`: zero when the
    /// base is.
    fn body(base: Base) -> Self {
        Self(u64::from(base.0))
    }

    fn base(self) -> Base {
        Base((self.0 & mask(BASE_BITS)) as u8)
    }

    fn at(self) -> At {
        let seg = (self.0 >> (OFF_BITS + LEN_BITS + BASE_BITS)) as u32;
        let base = self.base();
        if seg == 0 {
            return if base.is_body() { At::Body } else { At::Zero };
        }
        At::Rec {
            seg,
            offset: BLOCK + 2 * ((self.0 >> (LEN_BITS + BASE_BITS)) & mask(OFF_BITS)),
            len: 2 * ((self.0 >> BASE_BITS) & mask(LEN_BITS)) as usize,
            base,
        }
    }

    fn is_some(self) -> bool {
        self.0 != 0
    }

    /// Check bits a packed entry read from disk must not set.
    fn check(self) -> Result<Self, StoreError> {
        if u64::from(self.base().0) > SLOTS_PER_PAGE.div_ceil(BASE_UNIT as usize) as u64 {
            return Err(StoreError::Corrupt("spent.delta idx base out of range"));
        }
        Ok(self)
    }
}

/// First slot of a page: page 0 opens with the `spent.body` header.
fn first_rel(page_off: u64) -> usize {
    if page_off == 0 {
        HEAD as usize / SLOT
    } else {
        0
    }
}

fn align_block(at: u64) -> u64 {
    at.div_ceil(BLOCK) * BLOCK
}

fn entry_bytes(generation: u32, at: Placed) -> Entry {
    let mut b = [0u8; DIR_ENTRY as usize];
    b[0..4].copy_from_slice(&generation.to_le_bytes());
    b[4..12].copy_from_slice(&at.0.to_le_bytes());
    b
}

fn entry_generation(e: &[u8]) -> u32 {
    rd_u32(e, 0)
}

fn entry_placed(e: &[u8]) -> Result<Placed, StoreError> {
    if entry_generation(e) == 0 {
        return Ok(Placed::default());
    }
    Placed(rd_u64(e, 4)).check()
}

/// Nonzero slots in a whole page image.
fn filled_in(page_off: u64, image: &[u8]) -> usize {
    image
        .chunks_exact(SLOT)
        .skip(first_rel(page_off))
        .filter(|slot| *slot != ZERO.as_slice())
        .count()
}

/// The reflected CRC-32 (IEEE) polynomial.
const CRC32_POLY: u32 = 3_988_292_384;

/// CRC-32 (IEEE), for the log batches.
fn crc32(parts: &[&[u8]]) -> u32 {
    const TABLE: [u32; 256] = {
        let mut t = [0u32; 256];
        let mut i = 0;
        while i < 256 {
            let mut c = i as u32;
            let mut k = 0;
            while k < 8 {
                c = if c & 1 != 0 {
                    CRC32_POLY ^ (c >> 1)
                } else {
                    c >> 1
                };
                k += 1;
            }
            t[i] = c;
            i += 1;
        }
        t
    };
    let mut c = !0u32;
    for part in parts {
        for &b in *part {
            c = TABLE[usize::from(c.to_le_bytes()[0] ^ b)] ^ (c >> 8);
        }
    }
    !c
}

fn rd_u16(b: &[u8], o: usize) -> u16 {
    u16::from_le_bytes([b[o], b[o + 1]])
}

fn rd_u32(b: &[u8], o: usize) -> u32 {
    u32::from_le_bytes([b[o], b[o + 1], b[o + 2], b[o + 3]])
}

fn rd_u64(b: &[u8], o: usize) -> u64 {
    let mut a = [0u8; 8];
    a.copy_from_slice(&b[o..o + 8]);
    u64::from_le_bytes(a)
}

/// A generation's new records, encoded back to back in one buffer, each
/// page naming its bytes. Every producer writes entries straight into it,
/// so a checkpoint that merges many pages allocates this buffer and its
/// page list, not a map and a record per page.
#[derive(Default)]
struct PlanPages {
    bytes: Vec<u8>,
    pages: Vec<PagePlan>,
}

/// One page's new record and what it lies over.
struct PagePlan {
    page_off: u64,
    base: Base,
    at: std::ops::Range<usize>,
}

impl PlanPages {
    fn with_capacity(pages: usize, bytes: usize) -> Self {
        Self {
            bytes: Vec::with_capacity(bytes),
            pages: Vec::with_capacity(pages),
        }
    }

    fn len(&self) -> usize {
        self.pages.len()
    }

    fn is_empty(&self) -> bool {
        self.pages.is_empty()
    }

    /// Open `page_off`'s record. [`Self::entry`] appends in `rel` order;
    /// [`Self::end`] or [`Self::abandon`] closes it.
    fn begin(&mut self, page_off: u64) -> usize {
        let start = self.bytes.len();
        self.bytes.extend_from_slice(&page_off.to_le_bytes());
        self.bytes.extend_from_slice(&0u16.to_le_bytes());
        start
    }

    fn entry(&mut self, rel: u16, meta: Slot) {
        self.bytes.extend_from_slice(&rel.to_le_bytes());
        self.bytes.extend_from_slice(&meta);
    }

    /// Close the record [`Self::begin`] opened at `start`. Returns its length.
    fn end(&mut self, start: usize, page_off: u64, base: Base) -> usize {
        let len = self.bytes.len() - start;
        let count = ((len - REC_HEAD) / REC_ENTRY) as u16;
        self.bytes[start + 8..start + REC_HEAD].copy_from_slice(&count.to_le_bytes());
        self.pages.push(PagePlan {
            page_off,
            base,
            at: start..start + len,
        });
        len
    }

    /// Drop the record [`Self::begin`] opened at `start`.
    fn abandon(&mut self, start: usize) {
        self.bytes.truncate(start);
    }

    /// One whole record from `slots`, in `rel` order. Returns its length.
    fn push_slots(
        &mut self,
        page_off: u64,
        base: Base,
        slots: impl IntoIterator<Item = (u16, Slot)>,
    ) -> usize {
        let start = self.begin(page_off);
        for (rel, meta) in slots {
            self.entry(rel, meta);
        }
        self.end(start, page_off, base)
    }

    fn iter(&self) -> impl Iterator<Item = (&PagePlan, &[u8])> + '_ {
        self.pages.iter().map(|p| (p, &self.bytes[p.at.clone()]))
    }
}

/// Append one page's entries to the record open in `pages`: its committed
/// `kept` entries and its sealed `notes`, both in slot order, as one run in
/// `rel` order. A note wins over the committed entry for its slot. Returns
/// how many entries are spends (nonzero).
fn merge_page(
    pages: &mut PlanPages,
    kept: impl Iterator<Item = (u16, Slot)>,
    notes: &[(u64, Slot)],
) -> u64 {
    let mut kept = kept.peekable();
    let mut spends = 0u64;
    let mut put = |pages: &mut PlanPages, rel: u16, meta: Slot| {
        spends += u64::from(meta != ZERO);
        pages.entry(rel, meta);
    };
    for &(key, meta) in notes {
        let rel = key_rel(key);
        while let Some((older, old)) = kept.next_if(|&(r, _)| r < rel) {
            put(pages, older, old);
        }
        kept.next_if(|&(r, _)| r == rel);
        put(pages, rel, meta);
    }
    for (rel, meta) in kept {
        put(pages, rel, meta);
    }
    spends
}

/// Length of the record starting at `buf[0]`, from its header.
fn record_len(buf: &[u8]) -> Option<usize> {
    let n = rd_u16(buf.get(..REC_HEAD)?, 8) as usize;
    (n <= SLOTS_PER_PAGE).then_some(REC_HEAD + n * REC_ENTRY)
}

/// A record read back from a segment, checked against its page.
#[derive(Clone, Copy)]
struct Record<'a>(&'a [u8]);

impl<'a> Record<'a> {
    fn check(buf: &'a [u8], page_off: u64) -> Result<Self, StoreError> {
        if buf.len() < REC_HEAD || rd_u64(buf, 0) != page_off {
            return Err(StoreError::Corrupt("spent.delta record names another page"));
        }
        if record_len(buf) != Some(buf.len()) {
            return Err(StoreError::Corrupt("spent.delta bad record length"));
        }
        Ok(Self(buf))
    }

    fn entries(self) -> impl Iterator<Item = (u16, Slot)> + 'a {
        self.0[REC_HEAD..].chunks_exact(REC_ENTRY).map(|e| {
            let mut meta = [0u8; SLOT];
            meta.copy_from_slice(&e[2..]);
            (rd_u16(e, 0), meta)
        })
    }

    /// [`Self::entries`], once every `rel` is checked ascending and inside
    /// the page.
    fn ordered(self) -> Result<impl Iterator<Item = (u16, Slot)> + 'a, StoreError> {
        let mut prev = None;
        for (rel, _) in self.entries() {
            if prev.is_some_and(|p| rel <= p) || rel as usize >= SLOTS_PER_PAGE {
                return Err(StoreError::Corrupt("spent.delta rel order"));
            }
            prev = Some(rel);
        }
        Ok(self.entries())
    }

    /// Entries are written in `rel` order, so one lookup is a binary search.
    fn get(&self, rel: u16) -> Option<Slot> {
        let body = &self.0[REC_HEAD..];
        let n = body.len() / REC_ENTRY;
        let (mut lo, mut hi) = (0, n);
        while lo < hi {
            let mid = (lo + hi) / 2;
            let at = mid * REC_ENTRY;
            match rd_u16(body, at).cmp(&rel) {
                std::cmp::Ordering::Less => lo = mid + 1,
                std::cmp::Ordering::Greater => hi = mid,
                std::cmp::Ordering::Equal => {
                    let mut meta = [0u8; SLOT];
                    meta.copy_from_slice(&body[at + 2..at + REC_ENTRY]);
                    return Some(meta);
                }
            }
        }
        None
    }
}

/// The durable snapshot `commit` names.
///
/// Layout (little endian, 144 bytes): `magic:[u8; 4] | schema:u16 |
/// layout:u16 | generation:u32 | dir_pages:u32 | active:u32 | ring_len:u32 |
/// active_end:u64 | record_bytes:u64 | fold_bytes:u64 | fold_records:u64 |
/// idx_bytes:u64 | base:u32 | base_pages:u32 | log_end:u64 | ring[8] ×
/// (generation:u32, sealed_tip:u32)`. The directory is `idx.<base>` (its
/// first `base_pages` entries) then `log.<base>` up to `log_end`.
#[derive(Clone, Debug, PartialEq, Eq)]
struct Commit {
    generation: u32,
    dir_pages: u32,
    active: u32,
    active_end: u64,
    record_bytes: u64,
    fold_bytes: u64,
    fold_records: u64,
    idx_bytes: u64,
    /// The directory base, the pages it holds, and the committed log end.
    base: u32,
    base_pages: u32,
    log_end: u64,
    ring: Vec<(u32, u32)>,
}

/// How a publish moves the ring tip.
#[derive(Clone, Copy)]
enum Seal {
    /// Spends up to this confirmed tip were sealed.
    Raise(u32),
    /// Every committed slot above this tip was dropped.
    Filtered(u32),
}

/// What one publish adds to the lifetime counters.
#[derive(Default)]
struct Wrote {
    record_bytes: u64,
    fold_bytes: u64,
    fold_records: u64,
}

impl Commit {
    fn empty() -> Self {
        Self {
            generation: 0,
            dir_pages: 0,
            active: 1,
            active_end: HEAD,
            record_bytes: 0,
            fold_bytes: 0,
            fold_records: 0,
            idx_bytes: 0,
            base: 0,
            base_pages: 0,
            log_end: HEAD,
            ring: Vec::new(),
        }
    }

    fn sealed_tip(&self) -> Option<u32> {
        self.ring.last().map(|&(_, tip)| tip)
    }

    /// Newest generation whose sealed tip is at or below `durable`. Ring tips
    /// never fall except all together, so every older generation qualifies
    /// too. Zero when the ring cannot vouch for any generation.
    fn folds_through(&self, durable: u32) -> u32 {
        match self.ring.iter().rev().find(|&&(_, tip)| tip <= durable) {
            Some(&(generation, _)) => generation,
            None => 0,
        }
    }

    /// The next commit. A checkpoint never lowers the ring tip: a record
    /// rewritten at this generation still carries every older slot on its
    /// page. Only the open filter, which has dropped every slot above its tip,
    /// lowers it.
    #[allow(clippy::too_many_arguments)]
    fn next(
        &self,
        generation: u32,
        dir_pages: u32,
        active: u32,
        active_end: u64,
        idx_bytes: u64,
        wrote: &Wrote,
        seal: Seal,
    ) -> Self {
        let (mut ring, tip) = match seal {
            Seal::Raise(tip) => (
                self.ring.clone(),
                self.sealed_tip().map_or(tip, |prev| prev.max(tip)),
            ),
            Seal::Filtered(tip) => (
                self.ring.iter().map(|&(g, t)| (g, t.min(tip))).collect(),
                tip,
            ),
        };
        ring.push((generation, tip));
        if ring.len() > RING {
            ring.remove(0);
        }
        Self {
            generation,
            dir_pages,
            active,
            active_end,
            record_bytes: self.record_bytes.saturating_add(wrote.record_bytes),
            fold_bytes: self.fold_bytes.saturating_add(wrote.fold_bytes),
            fold_records: self.fold_records.saturating_add(wrote.fold_records),
            idx_bytes: self.idx_bytes.saturating_add(idx_bytes),
            base: self.base,
            base_pages: self.base_pages,
            log_end: self.log_end,
            ring,
        }
    }

    fn encode(&self) -> [u8; COMMIT_LEN] {
        let mut b = [0u8; COMMIT_LEN];
        b[0..4].copy_from_slice(&STORE_MAGIC);
        b[4..6].copy_from_slice(&FILE_SCHEMA_VERSION.to_le_bytes());
        b[6..8].copy_from_slice(&COMMIT_LAYOUT.to_le_bytes());
        b[8..12].copy_from_slice(&self.generation.to_le_bytes());
        b[12..16].copy_from_slice(&self.dir_pages.to_le_bytes());
        b[16..20].copy_from_slice(&self.active.to_le_bytes());
        b[20..24].copy_from_slice(&(self.ring.len() as u32).to_le_bytes());
        b[24..32].copy_from_slice(&self.active_end.to_le_bytes());
        b[32..40].copy_from_slice(&self.record_bytes.to_le_bytes());
        b[40..48].copy_from_slice(&self.fold_bytes.to_le_bytes());
        b[48..56].copy_from_slice(&self.fold_records.to_le_bytes());
        b[56..64].copy_from_slice(&self.idx_bytes.to_le_bytes());
        b[64..68].copy_from_slice(&self.base.to_le_bytes());
        b[68..72].copy_from_slice(&self.base_pages.to_le_bytes());
        b[72..80].copy_from_slice(&self.log_end.to_le_bytes());
        for (i, (generation, tip)) in self.ring.iter().enumerate() {
            let at = RING_AT + i * 8;
            b[at..at + 4].copy_from_slice(&generation.to_le_bytes());
            b[at + 4..at + 8].copy_from_slice(&tip.to_le_bytes());
        }
        b
    }

    fn parse(b: &[u8]) -> Result<Self, StoreError> {
        if b.len() < 8 || b[0..4] != STORE_MAGIC {
            return Err(StoreError::BadMagic);
        }
        let ver = rd_u16(b, 4);
        if !schema_file_openable(ver) {
            return Err(StoreError::BadSchema(ver));
        }
        if b.len() != COMMIT_LEN || rd_u16(b, 6) != COMMIT_LAYOUT {
            return Err(StoreError::Corrupt(EARLIER_BUILD));
        }
        let ring_len = rd_u32(b, 20) as usize;
        if ring_len > RING {
            return Err(StoreError::Corrupt("spent.delta commit ring too long"));
        }
        let ring = (0..ring_len)
            .map(|i| (rd_u32(b, RING_AT + i * 8), rd_u32(b, RING_AT + 4 + i * 8)))
            .collect();
        Ok(Self {
            generation: rd_u32(b, 8),
            dir_pages: rd_u32(b, 12),
            active: rd_u32(b, 16),
            active_end: rd_u64(b, 24),
            record_bytes: rd_u64(b, 32),
            fold_bytes: rd_u64(b, 40),
            fold_records: rd_u64(b, 48),
            idx_bytes: rd_u64(b, 56),
            base: rd_u32(b, 64),
            base_pages: rd_u32(b, 68),
            log_end: rd_u64(b, 72),
            ring,
        })
    }
}

const EARLIER_BUILD: &str =
    "spent.delta is from an earlier schema-30 build; wipe datadir and redo IBD";

fn read_commit(path: &Path) -> Result<Option<Commit>, StoreError> {
    match std::fs::read(path) {
        Ok(raw) => Commit::parse(&raw).map(Some),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(StoreError::io(path, e)),
    }
}

fn physical_len(file: &TableFile) -> Result<u64, StoreError> {
    file.read_file
        .metadata()
        .map(|m| m.len())
        .map_err(|e| StoreError::io(&file.path, e))
}

fn sync_dir(dir: &Path) -> Result<(), StoreError> {
    fsync_parent_dir(dir).map_err(|e| StoreError::io(dir, e))
}

/// Segment ids present in `spent.delta/`.
fn segment_ids(dir: &Path) -> Result<BTreeSet<u32>, StoreError> {
    ids_named(dir, "seg.")
}

/// Ids of the files in `spent.delta/` named `<prefix><id>`.
fn ids_named(dir: &Path, prefix: &str) -> Result<BTreeSet<u32>, StoreError> {
    let mut ids = BTreeSet::new();
    for entry in std::fs::read_dir(dir).map_err(|e| StoreError::io(dir, e))? {
        let entry = entry.map_err(|e| StoreError::io(dir, e))?;
        if let Some(id) = entry
            .file_name()
            .to_str()
            .and_then(|n| n.strip_prefix(prefix))
            .and_then(|n| n.parse::<u32>().ok())
        {
            ids.insert(id);
        }
    }
    Ok(ids)
}

/// Remove `dir/name`; a file already gone is fine.
fn remove_if_present(dir: &Path, name: &str) -> Result<(), StoreError> {
    let path = dir.join(name);
    match std::fs::remove_file(&path) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(StoreError::io(&path, e)),
    }
}

/// Create a segment or log file one block long, growing in 64 KiB steps,
/// so its length is on a block boundary before any commit names it: a later
/// extend never rewrites a block holding committed bytes.
fn create_aligned(path: PathBuf, kind: TableKind) -> Result<TableFile, StoreError> {
    let file = TableFile::create(path, kind)?;
    file.set_grow_policy(GrowPolicy::Align64k);
    file.grow_to(BLOCK)?;
    Ok(file)
}

fn open_aligned(path: PathBuf, kind: TableKind) -> Result<TableFile, StoreError> {
    let file = TableFile::open(path, kind)?;
    file.set_grow_policy(GrowPolicy::Align64k);
    Ok(file)
}

/// What [`SpendPageLog::append_records`] wrote.
struct Appended {
    placed: Vec<(u64, Placed)>,
    record_bytes: u64,
    wrote_segs: BTreeSet<u32>,
    /// One past the highest page written.
    pages: u64,
}

/// Where a commit finds the directory: base, its pages, and the log's end;
/// and the directory bytes this generation wrote.
struct DirAt {
    base: u32,
    base_pages: u32,
    log_end: u64,
    bytes: u64,
}

/// The directory files: a base no write touches once a commit names it, and
/// the log of one batch per generation since.
struct Idx {
    id: u32,
    base: TableFile,
    log: TableFile,
}

impl Idx {
    fn create(dir: &Path, id: u32, entries: &[Entry]) -> Result<Self, StoreError> {
        remove_if_present(dir, &idx_name(id))?;
        remove_if_present(dir, &log_name(id))?;
        let base = create_aligned(dir.join(idx_name(id)), TableKind::SpendDeltaIdx)?;
        base.write_uncommitted(HEAD, entries.as_flattened())?;
        base.sync_data_only()?;
        let log = create_aligned(dir.join(log_name(id)), TableKind::SpendDeltaLog)?;
        log.sync_data_only()?;
        Ok(Self { id, base, log })
    }

    fn open(dir: &Path, id: u32) -> Result<Self, StoreError> {
        Ok(Self {
            id,
            base: open_aligned(dir.join(idx_name(id)), TableKind::SpendDeltaIdx)?,
            log: open_aligned(dir.join(log_name(id)), TableKind::SpendDeltaLog)?,
        })
    }

    /// Every page's committed entry: the base, then each log batch in turn.
    /// A batch sets the directory length and then its entries. Anything that
    /// does not chain exactly to the commit's log end refuses; it is never
    /// read as a page without a record.
    fn entries(&self, commit: &Commit) -> Result<Vec<Entry>, StoreError> {
        let base_pages = u64::from(commit.base_pages);
        if commit.base != self.id || physical_len(&self.base)? < HEAD + base_pages * DIR_ENTRY {
            return Err(StoreError::Corrupt(
                "spent.delta idx base is shorter than its commit",
            ));
        }
        let mut entries: Vec<Entry> = Vec::with_capacity(base_pages as usize);
        let mut buf = Vec::new();
        let mut page = 0u64;
        while page < base_pages {
            let n = (base_pages - page).min(LOAD_CHUNK_PAGES);
            buf.resize((n * DIR_ENTRY) as usize, 0);
            self.base.pread_all(HEAD + page * DIR_ENTRY, &mut buf)?;
            entries.extend(buf.chunks_exact(DIR_ENTRY as usize).map(|e| {
                let mut entry = [0u8; DIR_ENTRY as usize];
                entry.copy_from_slice(e);
                entry
            }));
            page += n;
        }
        if physical_len(&self.log)? < commit.log_end {
            return Err(StoreError::Corrupt(
                "spent.delta log is shorter than its commit",
            ));
        }
        let mut at = HEAD;
        let mut prev = commit.base;
        while at < commit.log_end {
            at = align_block(at);
            let mut head = [0u8; BATCH_HEAD as usize];
            self.log.pread_all(at, &mut head)?;
            let (generation, count, dir_pages) = (
                rd_u32(&head, 0),
                u64::from(rd_u32(&head, 4)),
                rd_u32(&head, 8),
            );
            let end = at + BATCH_HEAD + count * LOG_ENTRY;
            if generation <= prev || generation > commit.generation || end > commit.log_end {
                return Err(StoreError::Corrupt("spent.delta log batch out of order"));
            }
            entries.resize(dir_pages as usize, [0u8; DIR_ENTRY as usize]);
            buf.resize((count * LOG_ENTRY) as usize, 0);
            self.log.pread_all(at + BATCH_HEAD, &mut buf)?;
            let sum = rd_u32(&head, 12);
            head[12..16].fill(0);
            if crc32(&[&head, &buf]) != sum {
                return Err(StoreError::Corrupt("spent.delta log batch checksum"));
            }
            for e in buf.chunks_exact(LOG_ENTRY as usize) {
                let page = rd_u32(e, 0) as usize;
                if page >= entries.len() || rd_u32(e, 4) != generation {
                    return Err(StoreError::Corrupt("spent.delta log entry out of range"));
                }
                entries[page].copy_from_slice(&e[4..]);
            }
            prev = generation;
            at = end;
        }
        if at != commit.log_end || entries.len() != commit.dir_pages as usize {
            return Err(StoreError::Corrupt(
                "spent.delta log does not end at its commit",
            ));
        }
        Ok(entries)
    }
}

struct Pace {
    early: AtomicBool,
    /// Checkpoints failed since the last one that succeeded.
    failures: AtomicU32,
    sync_wake: Mutex<Option<SpendSyncWake>>,
    room: Condvar,
    room_mu: Mutex<()>,
}

impl Pace {
    fn new() -> Self {
        Self {
            early: AtomicBool::new(false),
            failures: AtomicU32::new(0),
            sync_wake: Mutex::new(None),
            room: Condvar::new(),
            room_mu: Mutex::new(()),
        }
    }

    fn arm(&self) {
        self.early.store(true, Ordering::Release);
        let wake = self
            .sync_wake
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        let Some(wake) = wake else {
            return;
        };
        let (lock, cv) = &*wake;
        let _g = lock.lock().unwrap_or_else(|e| e.into_inner());
        cv.notify_one();
    }

    fn notify_room(&self) {
        let _g = self.room_mu.lock().unwrap_or_else(|e| e.into_inner());
        self.room.notify_all();
    }
}

/// One committed segment.
struct Seg {
    file: Arc<TableFile>,
    /// Bytes on disk, header included.
    size: u64,
    live_bytes: u64,
    live_records: u64,
    /// Newest generation that wrote into it; all its live records are at or
    /// below this.
    last_gen: u32,
}

impl Seg {
    /// Bytes past the header block: records and the padding between
    /// generations.
    fn data(&self) -> u64 {
        self.size.saturating_sub(BLOCK)
    }
}

/// RAM state readers and confirm share. Never held across IO.
struct Inner {
    open: OpenNotes,
    /// The map the next freeze installs as `open`, allocated by the
    /// checkpoint before it takes the exclusion the freeze runs under.
    next_open: Option<OpenNotes>,
    /// Open notes a freeze moved out, until its checkpoint sorts them into
    /// `sealed`, outside this lock.
    frozen: Option<Arc<OpenNotes>>,
    sealed: Option<Arc<SealedNotes>>,
    /// Highest confirmed tip a freeze moved into `sealed`.
    sealed_tip: Option<u32>,
    directory: Vec<Placed>,
    segs: BTreeMap<u32, Seg>,
    commit: Commit,
}

impl Inner {
    /// Notes frozen or sealed. After a failed checkpoint they count toward
    /// [`OPEN_EDGE_CAP`], so a retry that fails again releases no waiter.
    fn held_notes(&self) -> u64 {
        (self.frozen.as_ref().map_or(0, |f| f.len()) + self.sealed.as_ref().map_or(0, |s| s.len()))
            as u64
    }

    fn notes_bytes(&self) -> u64 {
        open_notes_bytes(&self.open)
            + self.next_open.as_ref().map_or(0, open_notes_bytes)
            + self.frozen.as_deref().map_or(0, open_notes_bytes)
            + self.sealed.as_ref().map_or(0, |s| s.bytes())
    }

    /// Newest note for the slot at `abs`: open, then frozen, then sealed.
    fn note_at(&self, abs: u64) -> Option<Slot> {
        let key = slot_key(abs);
        self.open
            .get(&key)
            .or_else(|| self.frozen.as_ref()?.get(&key))
            .copied()
            .or_else(|| self.sealed.as_ref()?.get(key))
    }

    fn stats(&self) -> SpendDeltaStats {
        SpendDeltaStats {
            record_bytes: self.commit.record_bytes,
            idx_bytes: self.commit.idx_bytes,
            fold_bytes: self.commit.fold_bytes,
            fold_records: self.commit.fold_records,
            segments: self.segs.len() as u64,
            segment_bytes: self.segs.values().map(|s| s.size).sum(),
            live_bytes: self.segs.values().map(|s| s.live_bytes).sum(),
            notes_bytes: self.notes_bytes(),
            directory_bytes: rbitcoin_primitives::heap_bytes::vec_bytes(&self.directory),
        }
    }

    /// Move one page's entry, keeping the live counts.
    fn place(&mut self, page: usize, at: Placed) {
        if self.directory.len() <= page {
            self.directory.resize(page + 1, Placed::default());
        }
        if let At::Rec { seg, len, .. } = self.directory[page].at() {
            if let Some(s) = self.segs.get_mut(&seg) {
                s.live_bytes = s.live_bytes.saturating_sub(len as u64);
                s.live_records = s.live_records.saturating_sub(1);
            }
        }
        if let At::Rec { seg, len, .. } = at.at() {
            if let Some(s) = self.segs.get_mut(&seg) {
                s.live_bytes += len as u64;
                s.live_records += 1;
            }
        }
        self.directory[page] = at;
    }
}

/// The active segment and segments rolled since the last commit.
struct Active {
    id: u32,
    file: Arc<TableFile>,
    end: u64,
}

/// The one durable writer. Held for a whole publish.
struct Writer {
    /// Never reused, so an attempt that failed after writing a new base
    /// cannot share its file names with a later commit.
    next_generation: u32,
    active: Active,
    /// Segments this writer filled since the last commit, oldest first.
    rolled: Vec<Active>,
    /// Segments no entry names, waiting for their last reader.
    doomed: Vec<(u32, Arc<TableFile>)>,
    /// The directory files the commit names.
    idx: Idx,
    /// When the previous checkpoint started.
    last_checkpoint: Option<Instant>,
    /// The last commit's rename landed but its directory sync failed.
    dir_unsynced: bool,
}

/// One directory miss: the segment record, and the slots this lookup wants.
struct RecordMiss {
    file: Arc<TableFile>,
    offset: u64,
    len: usize,
    base: Base,
    wanted: Vec<(u16, usize)>,
}

pub struct SpendPageLog {
    dir: PathBuf,
    policy: Policy,
    inner: Mutex<Inner>,
    writer: Mutex<Writer>,
    pace: Pace,
}

impl SpendPageLog {
    /// Fresh overlay for a new store, with the default policy.
    pub fn create(store: &Path) -> Result<Self, StoreError> {
        Self::create_with(store, Policy::DEFAULT)
    }

    pub fn create_with(store: &Path, policy: Policy) -> Result<Self, StoreError> {
        let dir = store.join(SPEND_DELTA_DIR);
        std::fs::create_dir(&dir).map_err(|e| StoreError::io(&dir, e))?;
        sync_dir(store)?;
        Self::init(store, policy)
    }

    /// Empty commit first, then the files, so files without a commit are
    /// always a lost commit.
    fn init(store: &Path, policy: Policy) -> Result<Self, StoreError> {
        let dir = store.join(SPEND_DELTA_DIR);
        let commit = Commit::empty();
        write_synced_tmp_rename(&dir.join(SPEND_DELTA_COMMIT_NAME), &commit.encode())?;
        let seg = create_aligned(dir.join(segment_name(1)), TableKind::SpendDelta)?;
        let idx = Idx::create(&dir, 0, &[])?;
        sync_dir(&dir)?;
        let mut segs = BTreeMap::new();
        segs.insert(
            1,
            Seg {
                file: Arc::new(seg),
                size: HEAD,
                live_bytes: 0,
                live_records: 0,
                last_gen: 0,
            },
        );
        Ok(Self::with(dir, policy, idx, Vec::new(), segs, commit))
    }

    /// Open the committed snapshot. A `fold` file is copied onto `spent.body`
    /// again first. Then drop what a crash left past the commit, and cut
    /// every slot at or past the `spent.body` end.
    pub fn load_with(
        store: &Path,
        body: &mut dyn SpentBody,
        policy: Policy,
    ) -> Result<Self, StoreError> {
        let body_end = body.published_len();
        let log = Self::open_committed(store, body, policy)?;
        log.clip(body, body_end)?;
        let s = log.stats();
        rbitcoin_log::info!(
            "store: spend delta open segments={} bytes={} live={} spent_body={} written={} records={} idx={} folded={} fold_records={} notes_ram={}",
            s.segments,
            s.segment_bytes,
            s.live_bytes,
            body_end,
            s.written(),
            s.record_bytes,
            s.idx_bytes,
            s.fold_bytes,
            s.fold_records,
            s.notes_bytes
        );
        Ok(log)
    }

    fn open_committed(
        store: &Path,
        body: &mut dyn SpentBody,
        policy: Policy,
    ) -> Result<Self, StoreError> {
        let dir = store.join(SPEND_DELTA_DIR);
        if dir.is_file() {
            return Err(StoreError::Corrupt(EARLIER_BUILD));
        }
        if !dir.exists() {
            return Self::create_with(store, policy);
        }
        let commit =
            match read_commit(&dir.join(SPEND_DELTA_COMMIT_NAME))? {
                Some(commit) => commit,
                None if segment_ids(&dir)?.is_empty()
                    && ids_named(&dir, "idx.")?.is_empty()
                    && ids_named(&dir, "log.")?.is_empty() =>
                {
                    return Self::init(store, policy);
                }
                None => return Err(StoreError::Corrupt(
                    "spent.delta/commit is missing beside its segments; wipe datadir and redo IBD",
                )),
            };
        if commit.generation == 0 {
            // Nothing was ever published. A first open that stopped between
            // the commit and the files left this; build it again.
            for id in segment_ids(&dir)? {
                remove_if_present(&dir, &segment_name(id))?;
            }
            remove_unnamed_idx(&dir, None)?;
            return Self::init(store, policy);
        }
        // Only a commit this build reads names the pages a `fold` holds.
        replay_fold_file(&dir, body)?;
        remove_unnamed_idx(&dir, Some(commit.base))?;
        let idx = Idx::open(&dir, commit.base)?;
        let scan = scan_directory(&idx.entries(&commit)?, &commit)?;
        let segs = open_segments(&dir, &commit, &scan.segs)?;
        Ok(Self::with(dir, policy, idx, scan.directory, segs, commit))
    }

    fn with(
        dir: PathBuf,
        policy: Policy,
        idx: Idx,
        directory: Vec<Placed>,
        segs: BTreeMap<u32, Seg>,
        commit: Commit,
    ) -> Self {
        let active = Active {
            id: commit.active,
            file: Arc::clone(&segs[&commit.active].file),
            end: commit.active_end,
        };
        Self {
            dir,
            policy,
            writer: Mutex::new(Writer {
                next_generation: commit.generation.saturating_add(1),
                active,
                rolled: Vec::new(),
                doomed: Vec::new(),
                idx,
                last_checkpoint: None,
                dir_unsynced: false,
            }),
            inner: Mutex::new(Inner {
                open: open_notes_after(0),
                next_open: None,
                frozen: None,
                sealed: None,
                sealed_tip: None,
                directory,
                segs,
                commit,
            }),
            pace: Pace::new(),
        }
    }

    fn lock(&self) -> MutexGuard<'_, Inner> {
        self.inner.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn writer(&self) -> MutexGuard<'_, Writer> {
        self.writer.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Lifetime counters and current sizes.
    pub fn stats(&self) -> SpendDeltaStats {
        self.lock().stats()
    }

    pub fn bind_sync_wake(&self, wake: SpendSyncWake) {
        *self
            .pace
            .sync_wake
            .lock()
            .unwrap_or_else(|e| e.into_inner()) = Some(wake);
    }

    pub fn checkpoint_early(&self) -> bool {
        self.pace.early.load(Ordering::Acquire)
    }

    pub fn relax_pace(&self) {
        let early = self.at_edge_cap(&self.lock());
        self.pace.early.store(early, Ordering::Release);
        if early {
            self.pace.arm();
        }
    }

    fn at_edge_cap(&self, g: &Inner) -> bool {
        let failed = self.pace.failures.load(Ordering::Acquire) > 0;
        let held = if failed { g.held_notes() } else { 0 };
        (g.open.len() as u64).saturating_add(held) >= self.policy.open_edge_cap
    }

    /// Wait while the open notes, and the sealed ones once a checkpoint
    /// failed, are at [`Policy::open_edge_cap`] and a sync thread
    /// is bound to drain them. Past [`CHECKPOINT_FAILURES_STOP`] failed
    /// checkpoints in a row this returns an error instead of waiting.
    pub fn wait_open_edge_room(&self) -> Result<(), StoreError> {
        loop {
            let room = self.pace.room_mu.lock().unwrap_or_else(|e| e.into_inner());
            if !self.at_edge_cap(&self.lock()) {
                return Ok(());
            }
            let bound = self
                .pace
                .sync_wake
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .is_some();
            if !bound {
                return Ok(());
            }
            if self.pace.failures.load(Ordering::Acquire) >= CHECKPOINT_FAILURES_STOP {
                return Err(StoreError::Corrupt(
                    "spend delta checkpoints keep failing; confirm stops at the open note cap",
                ));
            }
            self.pace.arm();
            drop(self.pace.room.wait(room).unwrap_or_else(|e| e.into_inner()));
        }
    }

    /// One record's bytes, checked against `page_off`.
    fn read_record(
        file: &TableFile,
        page_off: u64,
        offset: u64,
        len: usize,
    ) -> Result<Vec<u8>, StoreError> {
        note_delta_record_serial();
        let mut buf = vec![0u8; len];
        file.pread_all(offset, &mut buf)?;
        Record::check(&buf, page_off)?;
        Ok(buf)
    }

    /// The committed record for a page with its base and the newest
    /// generation its segment holds, a bound on the record's own.
    fn committed_record(&self, page_off: u64) -> Result<Option<OwnedCommitted>, StoreError> {
        let (file, offset, len, base, seg_gen) = {
            let g = self.lock();
            match placed_at(&g, page_off).at() {
                At::Rec {
                    seg,
                    offset,
                    len,
                    base,
                } => {
                    let s = &g.segs[&seg];
                    (Arc::clone(&s.file), offset, len, base, s.last_gen)
                }
                At::Zero | At::Body => return Ok(None),
            }
        };
        Ok(Some(OwnedCommitted {
            bytes: Self::read_record(&file, page_off, offset, len)?,
            base,
            seg_gen,
        }))
    }

    /// The committed records of `page_offs` that have one, read into one
    /// buffer sized from the directory. Records touching in a segment share
    /// one span. Spans go out together on the completion session, at ring
    /// depth 128, the same batch a confirm lookup uses. The directory lock
    /// is not held across the reads.
    fn committed_records(
        &self,
        page_offs: &[u64],
        report: &mut CheckpointReport,
    ) -> Result<CommittedRecords, StoreError> {
        struct Run {
            file: Arc<TableFile>,
            offset: u64,
            pos: usize,
            len: usize,
            seg_gen: u32,
            recs: Vec<LiveRecord>,
        }

        let mut by_seg: HashMap<u32, (Arc<TableFile>, u32, Vec<LiveRecord>)> = HashMap::new();
        {
            let g = self.lock();
            for &page_off in page_offs {
                if let At::Rec {
                    seg,
                    offset,
                    len,
                    base,
                } = placed_at(&g, page_off).at()
                {
                    let s = &g.segs[&seg];
                    by_seg
                        .entry(seg)
                        .or_insert_with(|| (Arc::clone(&s.file), s.last_gen, Vec::new()))
                        .2
                        .push(LiveRecord {
                            page_off,
                            offset,
                            len,
                            base,
                        });
                }
            }
        }
        let (count, total) = by_seg
            .values()
            .flat_map(|(_, _, records)| records)
            .fold((0, 0), |(n, bytes), r| (n + 1, bytes + r.len));
        let mut out = CommittedRecords {
            buf: vec![0u8; total],
            rows: Vec::with_capacity(count),
            next: 0,
        };
        let mut runs = Vec::new();
        let mut pos = 0;
        for (file, seg_gen, mut records) in by_seg.into_values() {
            records.sort_unstable_by_key(|r| r.offset);
            for run in coalesce(&records) {
                let first = run[0];
                let last = run[run.len() - 1];
                let len = (last.offset + last.len as u64 - first.offset) as usize;
                runs.push(Run {
                    file: Arc::clone(&file),
                    offset: first.offset,
                    pos,
                    len,
                    seg_gen,
                    recs: run.to_vec(),
                });
                pos += len;
            }
        }
        let backend = crate::io_backend::read_io_backend();
        let depth = crate::uring_session::DEFAULT_ENTRIES as usize;
        for chunk in runs.chunks(depth.max(1)) {
            note_delta_record_bulk();
            let paths: Vec<PathBuf> = chunk.iter().map(|run| run.file.path.clone()).collect();
            let ranges: Vec<(usize, usize)> = chunk.iter().map(|run| (run.pos, run.len)).collect();
            let slices = disjoint_slices(&mut out.buf, &ranges);
            let mut ops: Vec<crate::bulk_io::ReadOp<'_>> = slices
                .into_iter()
                .zip(chunk.iter())
                .map(|(buf, run)| {
                    let fd = run.file.io_handle();
                    crate::bulk_io::ReadOp {
                        fd,
                        offset: run.offset,
                        buf,
                        result: i32::MIN,
                    }
                })
                .collect();
            crate::bulk_io::pread_batch_backend(&mut ops, backend);
            for (i, op) in ops.iter().enumerate() {
                if op.result < 0 {
                    return Err(StoreError::io(
                        &paths[i],
                        std::io::Error::from_raw_os_error(-op.result),
                    ));
                }
                if op.result as usize != chunk[i].len {
                    return Err(StoreError::io(
                        &paths[i],
                        std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "pread short"),
                    ));
                }
            }
            drop(ops);
            for run in chunk {
                report.merge_reads += 1;
                report.merge_read_bytes += run.len as u64;
                for rec in &run.recs {
                    let at = run.pos + (rec.offset - run.offset) as usize;
                    Record::check(&out.buf[at..at + rec.len], rec.page_off)?;
                    out.rows.push(CommittedRow {
                        page_off: rec.page_off,
                        at: at..at + rec.len,
                        base: rec.base,
                        seg_gen: run.seg_gen,
                    });
                }
            }
        }
        out.rows.sort_unstable_by_key(|r| r.page_off);
        Ok(out)
    }

    /// The slot at `abs`. `None`: `spent.body` holds it, on a page a fold
    /// wrote. A page no fold wrote reads zero where the delta holds nothing.
    pub fn slot(&self, abs: u64) -> Result<Option<Slot>, StoreError> {
        Ok(self.slots_at(&[abs])?[0])
    }

    /// [`Self::slot`] for many slots.
    ///
    /// A RAM note is not read. Pages the directory still names as records are
    /// one bulk read on the completion session (ring depth 128, or the libc
    /// pread bulk fallback). A folded page with no record is not part of that
    /// read.
    pub fn slots_at(&self, abs: &[u64]) -> Result<Vec<Option<Slot>>, StoreError> {
        self.slots_at_backend(abs, crate::io_backend::read_io_backend())
    }

    /// [`Self::slots_at`] on `backend`, the one the caller already chose for
    /// the `spent.body` peeks.
    pub fn slots_at_backend(
        &self,
        abs: &[u64],
        backend: crate::io_backend::ReadIoBackend,
    ) -> Result<Vec<Option<Slot>>, StoreError> {
        let mut out = vec![Some(ZERO); abs.len()];
        let mut misses: BTreeMap<u64, RecordMiss> = BTreeMap::new();
        {
            let g = self.lock();
            for (i, &off) in abs.iter().enumerate() {
                let (page_off, rel) = (page_of(off), rel_of(off));
                if let Some(meta) = g.note_at(off) {
                    out[i] = Some(meta);
                    continue;
                }
                match placed_at(&g, page_off).at() {
                    At::Zero => {}
                    At::Body => out[i] = None,
                    At::Rec {
                        seg,
                        offset,
                        len,
                        base,
                    } => {
                        misses
                            .entry(page_off)
                            .or_insert_with(|| RecordMiss {
                                file: Arc::clone(&g.segs[&seg].file),
                                offset,
                                len,
                                base,
                                wanted: Vec::new(),
                            })
                            .wanted
                            .push((rel, i));
                    }
                }
            }
        }
        if misses.is_empty() {
            return Ok(out);
        }
        Self::read_record_misses(misses, backend, &mut out)?;
        Ok(out)
    }

    /// One completion-session read of the record misses, chunked at ring depth.
    /// Records that touch in a segment share a span, so the dead gap between
    /// two records is not read. The directory lock is not held.
    fn read_record_misses(
        misses: BTreeMap<u64, RecordMiss>,
        backend: crate::io_backend::ReadIoBackend,
        out: &mut [Option<Slot>],
    ) -> Result<(), StoreError> {
        struct Page {
            page_off: u64,
            offset: u64,
            len: usize,
            base: Base,
            wanted: Vec<(u16, usize)>,
        }
        struct SpanRec {
            at: usize,
            len: usize,
            page_off: u64,
            base: Base,
            wanted: Vec<(u16, usize)>,
        }
        struct Span {
            /// Kept until the batch returns. `unlink_doomed` closes a segment
            /// once this is the last `Arc`.
            file: Arc<TableFile>,
            offset: u64,
            buf: Vec<u8>,
            recs: Vec<SpanRec>,
        }

        let mut grouped: BTreeMap<usize, (Arc<TableFile>, Vec<Page>)> = BTreeMap::new();
        for (
            page_off,
            RecordMiss {
                file,
                offset,
                len,
                base,
                wanted,
            },
        ) in misses
        {
            let key = Arc::as_ptr(&file) as usize;
            grouped
                .entry(key)
                .or_insert_with(|| (file, Vec::new()))
                .1
                .push(Page {
                    page_off,
                    offset,
                    len,
                    base,
                    wanted,
                });
        }
        let mut spans = Vec::new();
        for (_, (file, mut pages)) in grouped {
            pages.sort_unstable_by_key(|p| p.offset);
            while !pages.is_empty() {
                let mut n = 1;
                while n < pages.len()
                    && pages[n].offset == pages[n - 1].offset + pages[n - 1].len as u64
                {
                    n += 1;
                }
                let run: Vec<Page> = pages.drain(..n).collect();
                let start = run[0].offset;
                let end = run[n - 1].offset + run[n - 1].len as u64;
                let recs = run
                    .into_iter()
                    .map(|p| SpanRec {
                        at: (p.offset - start) as usize,
                        len: p.len,
                        page_off: p.page_off,
                        base: p.base,
                        wanted: p.wanted,
                    })
                    .collect();
                spans.push(Span {
                    file: Arc::clone(&file),
                    offset: start,
                    buf: vec![0u8; (end - start) as usize],
                    recs,
                });
            }
        }
        let depth = crate::uring_session::DEFAULT_ENTRIES as usize;
        for chunk in spans.chunks_mut(depth.max(1)) {
            note_delta_record_bulk();
            if owner_probe_armed() {
                note_delta_record_owner(
                    chunk.iter().all(|span| Arc::strong_count(&span.file) >= 2),
                );
            }
            let paths: Vec<PathBuf> = chunk.iter().map(|span| span.file.path.clone()).collect();
            let lens: Vec<usize> = chunk.iter().map(|span| span.buf.len()).collect();
            let mut ops: Vec<crate::bulk_io::ReadOp<'_>> = chunk
                .iter_mut()
                .map(|span| {
                    let fd = span.file.io_handle();
                    crate::bulk_io::ReadOp {
                        fd,
                        offset: span.offset,
                        buf: span.buf.as_mut_slice(),
                        result: i32::MIN,
                    }
                })
                .collect();
            crate::bulk_io::pread_batch_backend(&mut ops, backend);
            for (i, op) in ops.iter().enumerate() {
                if op.result < 0 {
                    return Err(StoreError::io(
                        &paths[i],
                        std::io::Error::from_raw_os_error(-op.result),
                    ));
                }
                if op.result as usize != lens[i] {
                    return Err(StoreError::io(
                        &paths[i],
                        std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "pread short"),
                    ));
                }
            }
            drop(ops);
            for span in chunk.iter() {
                for rec in &span.recs {
                    let record = Record::check(&span.buf[rec.at..rec.at + rec.len], rec.page_off)?;
                    for &(rel, i) in &rec.wanted {
                        out[i] = match record.get(rel) {
                            Some(meta) => Some(meta),
                            None if rec.base.is_body() => None,
                            None => Some(ZERO),
                        };
                    }
                }
            }
        }
        Ok(())
    }

    pub fn note_slots(&self, slots: &[(u64, Slot)]) -> Result<(), StoreError> {
        if slots.is_empty() {
            return Ok(());
        }
        let arm = {
            let mut g = self.lock();
            let mut added = false;
            for &(abs, meta) in slots {
                added |= g.open.insert(slot_key(abs), meta).is_none();
            }
            added && self.at_edge_cap(&g)
        };
        if arm {
            self.pace.arm();
        }
        Ok(())
    }

    /// Move the open notes into the sealed layer. `tip` is the confirmed tip
    /// those notes may name spends from.
    ///
    /// With `keep`, every sealed note whose spender it rejects becomes an
    /// explicit zero first, so the generation's ring tip bounds every
    /// spender it holds. The caller holds confirm and disconnect off and
    /// passes `keep` when a disconnect ran since the last freeze; without
    /// one, every noted spender is still best chain. A reconnect after the
    /// freeze reads the zero and notes its spend again. An error leaves both
    /// layers as they were.
    ///
    /// The open edge count drops to zero here, so a confirm thread parked at
    /// the edge cap is released now, not after the publish. After a failed
    /// checkpoint the sealed notes still count, so a retry releases none.
    ///
    /// Only inside [`Self::checkpoint`]'s `before_freeze`: the freeze swaps
    /// in the map the checkpoint allocated before it, and the checkpoint
    /// sorts the frozen notes after it. Without `keep`, nothing under the
    /// exclusion allocates. With `keep` (only after a disconnect), the
    /// caller still holds the exclusion while every sealed and open note is
    /// copied for `keep` (about 32 MiB at the cap) and each rejected note is
    /// written into the frozen map as a zero. The copy cannot move before
    /// the exclusion: a disconnect between the copy and the freeze would
    /// leave spends it made stale unjudged.
    pub fn freeze_open<K>(&self, tip: u32, keep: Option<K>) -> Result<(), StoreError>
    where
        K: FnMut(&Slot) -> Result<bool, StoreError>,
    {
        // `keep` runs on a copy, outside the lock: it may read the delta.
        let mut rejected = Vec::new();
        if let Some(mut keep) = keep {
            let notes: Vec<(u64, Slot)> = {
                let g = self.lock();
                g.sealed
                    .iter()
                    .flat_map(|s| s.notes.iter().copied())
                    .chain(g.open.iter().map(|(&k, &m)| (k, m)))
                    .filter(|&(_, m)| m != ZERO)
                    .collect()
            };
            for (key, meta) in notes {
                if !keep(&meta)? {
                    rejected.push((key, meta));
                }
            }
        }
        let unused = {
            let mut g = self.lock();
            if g.frozen.is_some() {
                return Err(StoreError::Corrupt(
                    "invariant: spend notes frozen twice before one checkpoint sorted them",
                ));
            }
            let Some(fresh) = g.next_open.take() else {
                return Err(StoreError::Corrupt(
                    "invariant: spend notes frozen outside a checkpoint",
                ));
            };
            let mut frozen = std::mem::replace(&mut g.open, fresh);
            // A note rewritten since the copy was not judged; keep it.
            for (key, meta) in rejected {
                let newest = frozen
                    .get(&key)
                    .copied()
                    .or_else(|| g.sealed.as_ref()?.get(key));
                if newest == Some(meta) {
                    frozen.insert(key, ZERO);
                }
            }
            g.sealed_tip = Some(g.sealed_tip.map_or(tip, |prev| prev.max(tip)));
            if frozen.is_empty() {
                // Nothing to seal: the empty window keeps its map.
                Some(std::mem::replace(&mut g.open, frozen))
            } else {
                g.frozen = Some(Arc::new(frozen));
                None
            }
        };
        drop(unused);
        self.pace.notify_room();
        Ok(())
    }

    /// Sort the frozen notes over any sealed ones, outside the lock; readers
    /// use the frozen map until the sorted notes replace it.
    fn seal_frozen(&self) {
        let (frozen, prior) = {
            let g = self.lock();
            (g.frozen.clone(), g.sealed.clone())
        };
        let Some(frozen) = frozen else {
            return;
        };
        let sealed = Arc::new(SealedNotes::seal(&frozen, prior.as_deref()));
        let mut g = self.lock();
        g.sealed = Some(sealed);
        g.frozen = None;
    }

    /// Zero every committed slot `keep` rejects on pages a generation wrote
    /// while sealing spends above `tip`.
    ///
    /// Open runs this after a crash may have cut blocks the delta names
    /// spends from, and before anything is appended, so a spender fk past
    /// the recovered tip has not been handed to another create yet. The ring
    /// names those generations. When all of it is above `tip`, older
    /// generations may be too, and every page is checked.
    pub fn drop_spends_above<K>(&self, tip: u32, mut keep: K) -> Result<(), StoreError>
    where
        K: FnMut(&Slot) -> Result<bool, StoreError>,
    {
        let mut w = self.writer();
        let (ring, dir_pages) = {
            let g = self.lock();
            (g.commit.ring.clone(), g.commit.dir_pages)
        };
        let suspect: Vec<u32> = ring
            .iter()
            .filter(|&&(_, t)| t > tip)
            .map(|&(g, _)| g)
            .collect();
        if suspect.is_empty() {
            return Ok(());
        }
        let every_page = ring.len() == RING && ring[0].1 > tip;
        if every_page {
            rbitcoin_log::warn!(
                "store: every spent.delta generation in the ring sealed spends above tip {tip}; checking every page"
            );
        }
        let mut pages = PlanPages::default();
        for (page, generation) in self.selected_generations(&w.idx)?.into_iter().enumerate() {
            if generation == 0 || !(every_page || suspect.contains(&generation)) {
                continue;
            }
            let page_off = page as u64 * PAGE;
            let Some(owned) = self.committed_record(page_off)? else {
                continue;
            };
            let c = owned.view();
            let start = pages.begin(page_off);
            let mut changed = false;
            for (rel, meta) in c.record.ordered()? {
                let dropped = meta != ZERO && !keep(&meta)?;
                changed |= dropped;
                pages.entry(rel, if dropped { ZERO } else { meta });
            }
            if changed {
                pages.end(start, page_off, c.base);
            } else {
                pages.abandon(start);
            }
        }
        let plan = Plan {
            pages,
            dir_pages,
            seal: Seal::Filtered(tip),
            ..Plan::default()
        };
        self.publish(&mut w, plan, &mut CheckpointReport::default())
    }

    /// Install a schema-29 `spent.body` and page log as the first generation.
    ///
    /// A 29 body was written by Class A and folds alike, so every page with
    /// a nonzero slot becomes a page that reads `spent.body`, with its
    /// count; one pass reads the body once. Each 29 image keeps only the
    /// slots that differ from the body page, as a record over it. The page
    /// straddling the published end keeps its slots as a record over zeroes,
    /// so bytes a crashed 29 append left past the end are never read.
    ///
    /// An overlay that already published a generation adopted it on an
    /// earlier open that stopped before the 29 files were removed.
    #[doc(hidden)]
    pub fn adopt_v29(
        &self,
        images: Vec<(u64, Vec<u8>)>,
        tip: u32,
        body: &dyn SpentBody,
    ) -> Result<(), StoreError> {
        let mut w = self.writer();
        if self.lock().commit.generation != 0 {
            return Ok(());
        }
        let end = body.published_len();
        let mut images: HashMap<u64, Vec<u8>> = images.into_iter().collect();
        let mut plan = Plan {
            seal: Seal::Raise(tip),
            ..Plan::default()
        };
        let mut chunk = vec![0u8; (LOAD_CHUNK_PAGES * PAGE) as usize];
        let mut page_off = 0u64;
        while page_off < end {
            let n = (end - page_off).min(chunk.len() as u64) as usize;
            body.read(page_off, &mut chunk[..n])?;
            for (i, page) in chunk[..n].chunks(PAGE as usize).enumerate() {
                let off = page_off + i as u64 * PAGE;
                let mut slots = BTreeMap::new();
                if let Some(image) = images.remove(&off) {
                    for (rel, meta) in image.chunks_exact(SLOT).enumerate().skip(first_rel(off)) {
                        let at = rel * SLOT;
                        if page.get(at..at + SLOT) != Some(meta) {
                            slots.insert(rel as u16, meta.try_into().expect("one slot"));
                        }
                    }
                }
                let whole = off + PAGE <= end;
                if !whole {
                    for (rel, meta) in page.chunks_exact(SLOT).enumerate().skip(first_rel(off)) {
                        if meta != ZERO.as_slice() {
                            slots
                                .entry(rel as u16)
                                .or_insert_with(|| meta.try_into().expect("one slot"));
                        }
                    }
                }
                let base = if whole {
                    Base::of(filled_in(off, page))
                } else {
                    Base::ZERO
                };
                if !slots.is_empty() {
                    plan.pages.push_slots(off, base, slots);
                } else if base.is_body() {
                    plan.folded.push((off, Placed::body(base)));
                }
            }
            page_off += n as u64;
        }
        if plan.pages.is_empty() && plan.folded.is_empty() {
            return Ok(());
        }
        plan.dir_pages = u32::try_from(end.div_ceil(PAGE))
            .map_err(|_| StoreError::Corrupt("spent.delta idx past u32 pages"))?;
        rbitcoin_log::info!(
            "store: spend delta adopts a schema-29 spent.body: {} pages read it, {} records",
            plan.folded.len(),
            plan.pages.len()
        );
        self.publish(&mut w, plan, &mut CheckpointReport::default())
    }

    /// Generation of each page's committed record entry, zero where none.
    fn selected_generations(&self, idx: &Idx) -> Result<Vec<u32>, StoreError> {
        let commit = self.lock().commit.clone();
        idx.entries(&commit)?
            .iter()
            .map(|e| match entry_placed(e)?.at() {
                At::Rec { .. } => Ok(entry_generation(e)),
                At::Zero | At::Body => Ok(0),
            })
            .collect()
    }

    /// Publish the sealed layer as one generation, reclaiming old segments
    /// into `spent.body` and the active segment.
    ///
    /// `before_freeze` seals the open notes ([`Self::freeze_open`]). The
    /// merge reads no spender: a disconnect and reconnect while it runs
    /// would leave a zero the reconnect never saw. Older slots on a page are
    /// carried as they are: readers already require a best-chain spender,
    /// and [`Self::drop_spends_above`] covers a spender a crash cut.
    /// `durable` is the spend marker's durable height; without one nothing
    /// is folded. Any error leaves the sealed layer and the RAM directory as
    /// they were.
    pub fn checkpoint<B>(
        &self,
        before_freeze: B,
        body: &mut dyn SpentBody,
        durable: Option<u32>,
    ) -> Result<CheckpointReport, StoreError>
    where
        B: FnOnce(&Self) -> Result<(), StoreError>,
    {
        let out = self.checkpoint_once(before_freeze, body, durable);
        if out.is_ok() {
            self.pace.failures.store(0, Ordering::Release);
        } else {
            self.pace.failures.fetch_add(1, Ordering::AcqRel);
        }

        // A waiter re-checks the failure count as well as the room.
        self.pace.notify_room();
        out
    }

    fn checkpoint_once<B>(
        &self,
        before_freeze: B,
        body: &mut dyn SpentBody,
        durable: Option<u32>,
    ) -> Result<CheckpointReport, StoreError>
    where
        B: FnOnce(&Self) -> Result<(), StoreError>,
    {
        let started = Instant::now();
        let mut w = self.writer();
        self.settle(&mut w)?;
        let mut report = CheckpointReport {
            since_ms: w.last_checkpoint.map_or(0, ms_since),
            ..CheckpointReport::default()
        };
        w.last_checkpoint = Some(started);
        let recent = self.lock().open.len();
        let fresh = open_notes_after(recent);
        self.lock().next_open = Some(fresh);
        let froze = before_freeze(self);
        let unused = self.lock().next_open.take();
        drop(unused);
        self.seal_frozen();
        froze?;
        let (sealed, tip) = {
            let g = self.lock();
            let Some(sealed) = g.sealed.clone() else {
                return Ok(report);
            };
            (sealed, g.sealed_tip.unwrap_or(0))
        };
        replay_fold_file(&self.dir, body)?;
        let touch = TouchCtx {
            through: durable.map(|d| self.lock().commit.folds_through(d)),
            published: body.published_len(),
        };
        let mut rounds = Rounds::new(self.policy.fold_round_pages);
        let merge = Instant::now();
        let merged = self.merge_sealed(&sealed, touch, body, &mut rounds, &mut report)?;
        report.merge_ms = ms_since(merge);
        let fold = self.fold(
            &w,
            &sealed,
            merged.new_bytes,
            body,
            durable,
            &mut rounds,
            &mut report,
        )?;
        rounds.finish(&self.dir, body, &mut report)?;
        let carried_bytes = fold.carried.bytes.len() as u64;
        let plan = Plan {
            pages: merged.pages,
            carried: fold.carried,
            folded: fold.folded,
            dir_pages: self.lock().commit.dir_pages,
            seal: Seal::Raise(tip),
            retire: Some(sealed),
            wrote: Wrote {
                record_bytes: 0,
                fold_bytes: rounds.bytes,
                fold_records: rounds.records,
            },
        };
        self.publish(&mut w, plan, &mut report)?;
        if rounds.records > 0 {
            // After the commit: an error leaves `spend_durable` behind, and
            // the next checkpoint copies `fold` again before it folds.
            remove_fold_file(&self.dir)?;
        }
        report.total_ms = ms_since(started);
        self.log_checkpoint(
            &report,
            merged.new_bytes,
            merged.dense_bytes,
            carried_bytes,
            body,
            durable,
        );
        Ok(report)
    }

    /// Merge each sealed page's notes over its committed record.
    ///
    /// A committed record that already spends half its page's unset slots is
    /// folded into `spent.body` instead, when its segment's generations are
    /// all at or below the durable marker and its page is wholly below the
    /// published end; the page's new record holds only the sealed notes. A
    /// hot page so costs one page write per fold, not a rewrite of its whole
    /// record every checkpoint it is touched.
    fn merge_sealed(
        &self,
        sealed: &SealedNotes,
        touch: TouchCtx,
        body: &mut dyn SpentBody,
        rounds: &mut Rounds,
        report: &mut CheckpointReport,
    ) -> Result<Merged, StoreError> {
        let page_offs: Vec<u64> = sealed.pages().map(|(page_off, _)| page_off).collect();
        report.sealed_pages = page_offs.len() as u64;
        let mut committed = self.committed_records(&page_offs, report)?;
        // Room for every committed record kept whole plus a header and an
        // entry for each note: the merge never regrows the buffer.
        let room = committed.buf.len() + page_offs.len() * REC_HEAD + sealed.len() * REC_ENTRY;
        let mut out = Merged {
            pages: PlanPages::with_capacity(page_offs.len(), room),
            new_bytes: 0,
            dense_bytes: 0,
        };
        for (page_off, notes) in sealed.pages() {
            let mut base = placed_at(&self.lock(), page_off).base();
            let mut kept = None;
            if let Some(c) = committed.take(page_off) {
                if self.touch_folds(page_off, &c, touch, report) {
                    base = rounds.push(&self.dir, body, page_off, &c, report)?.base();
                    report.hot_folds += 1;
                    report.hot_fold_record_bytes += c.record.0.len() as u64;
                } else {
                    kept = Some(c.record.ordered()?);
                }
            }
            report.sealed_edges += notes.len() as u64;
            let start = out.pages.begin(page_off);
            let spends = merge_page(&mut out.pages, kept.into_iter().flatten(), notes);
            let bytes = out.pages.end(start, page_off, base) as u64;
            out.new_bytes += bytes;
            if 2 * spends >= base.unset(page_off) {
                out.dense_bytes += bytes;
            }
        }
        Ok(out)
    }

    /// True when a touched page's committed record folds now. Counts a
    /// dense record kept in the delta by the reason it stays.
    fn touch_folds(
        &self,
        page_off: u64,
        c: &Committed<'_>,
        touch: TouchCtx,
        report: &mut CheckpointReport,
    ) -> bool {
        if !foldable(page_off) || !spends_half_the_unset(page_off, c.base, &c.record) {
            return false;
        }
        if touch.through.is_none_or(|through| c.seg_gen > through) {
            report.dense_kept_gen += 1;
            return false;
        }
        if page_off + PAGE > touch.published {
            report.dense_kept_tail += 1;
            return false;
        }
        true
    }

    fn log_checkpoint(
        &self,
        r: &CheckpointReport,
        new_bytes: u64,
        dense_bytes: u64,
        carried_bytes: u64,
        body: &dyn SpentBody,
        durable: Option<u32>,
    ) {
        let s = self.stats();
        let body_len = body.published_len();
        let (generation, folds_through) = {
            let g = self.lock();
            (
                g.commit.generation,
                durable.map_or(0, |d| g.commit.folds_through(d)),
            )
        };
        rbitcoin_log::info!(
            "store: spend delta gen={} since_ms={} new_record_bytes={} dense_record_bytes={} carried={} segments={} written={} notes_ram={} total_ms={}",
            generation,
            r.since_ms,
            new_bytes,
            dense_bytes,
            r.carried,
            s.segments,
            s.written(),
            s.notes_bytes,
            r.total_ms
        );
        let detail = format!(
            "spend delta gen={} timing total_ms={} since_ms={} sealed_pages={} sealed_edges={} merge_reads={} merge_read_bytes={} merge_ms={} hot_folds={} hot_fold_record_bytes={} dense_kept_gen={} dense_kept_tail={} reclaim_read_bytes={} reclaim_read_ms={} folded_now={} dropped={} fold_rounds={} fold_compose_ms={} fold_file_ms={} body_write_ms={} body_sync_ms={} record_bytes={} seg_write_ms={} seg_sync_ms={} idx_entries={} idx_write_ms={} idx_sync_ms={} commit_ms={} install_ms={} unlink_ms={} folded_segments={} carried_bytes={} durable={} folds_through={} reclaim_below_permille={} bytes={} live={} spent_body={} records={} idx={} folded={} fold_records={} written_per_body_permille={}",
            generation,
            r.total_ms,
            r.since_ms,
            r.sealed_pages,
            r.sealed_edges,
            r.merge_reads,
            r.merge_read_bytes,
            r.merge_ms,
            r.hot_folds,
            r.hot_fold_record_bytes,
            r.dense_kept_gen,
            r.dense_kept_tail,
            r.reclaim_read_bytes,
            r.reclaim_read_ms,
            r.folded,
            r.dropped,
            r.fold_rounds,
            r.fold_compose_ms,
            r.fold_file_ms,
            r.body_write_ms,
            r.body_sync_ms,
            r.record_bytes,
            r.seg_write_ms,
            r.seg_sync_ms,
            r.idx_entries,
            r.idx_write_ms,
            r.idx_sync_ms,
            r.commit_ms,
            r.install_ms,
            r.unlink_ms,
            r.reclaim_segments,
            carried_bytes,
            durable.map_or_else(|| "none".to_string(), |d| d.to_string()),
            folds_through,
            self.policy
                .reclaim_below(s.segment_bytes, s.segments as usize, body_len),
            s.segment_bytes,
            s.live_bytes,
            body_len,
            s.record_bytes,
            s.idx_bytes,
            s.fold_bytes,
            s.fold_records,
            s.written().saturating_mul(1000) / body_len.max(1)
        );
        if checkpoint_detail_is_info(r) {
            rbitcoin_log::info!("store: {detail}");
        } else {
            rbitcoin_log::debug!(Store, "{detail}");
        }
    }

    /// Reclaim eligible segments, sparsest first, while their live share is
    /// under [`Policy::reclaim_below`]: [`Policy::reclaim_slope`] times the
    /// delta's share of `spent.body`, and never under
    /// [`Policy::sparse_permille`]. Returns the folded pages' new entries and
    /// the records to copy to the active segment.
    ///
    /// Eligible: not this writer's active or rolled segments, and every live
    /// record from a generation sealed at or below `durable`. Only the live
    /// records are read, found from the RAM directory; touching ones share a
    /// `pread`. A record whose page is in `sealed` is dropped unread: this
    /// generation rewrites that page. Each other goes one of two ways, and the
    /// victim is always freed:
    /// - its page is not page 0, is wholly below the published end, and its
    ///   spends cover at least half the slots its base leaves unset (any
    ///   record once the threshold reaches 1000): it is folded, in rounds of
    ///   [`Policy::fold_round_pages`];
    /// - otherwise it is carried: copied unchanged to the active segment, which
    ///   costs its record bytes instead of a page write.
    ///
    /// A spend is a nonzero entry over an unset slot. Entries over spent slots,
    /// explicit zeroes and rewrites, do not count, so a page folds again each
    /// time its delta spends half of what the last fold left unset.
    ///
    /// Victims are taken while the reclaimed bytes stay within
    /// [`Policy::reclaim_rate`] times the larger of `new_bytes` and one
    /// segment, and never below [`Policy::keep_segments`].
    ///
    /// Each folded page is composed whole, record over its base; see
    /// [`Rounds`] for how a round reaches `spent.body`.
    #[allow(clippy::too_many_arguments)]
    fn fold(
        &self,
        w: &Writer,
        sealed: &SealedNotes,
        new_bytes: u64,
        body: &mut dyn SpentBody,
        durable: Option<u32>,
        rounds: &mut Rounds,
        report: &mut CheckpointReport,
    ) -> Result<Folded, StoreError> {
        let mut out = Folded::default();
        let Some(durable) = durable else {
            return Ok(out);
        };
        let published = body.published_len();
        let victims = self.pick_victims(w, sealed, new_bytes, published, durable);
        report.reclaim_segments = victims.len() as u64;
        let (mut live, dropped) = self.live_records(&victims, sealed);
        report.dropped = dropped;
        for victim in &victims {
            let records = live.remove(&victim.id).unwrap_or_default();
            for run in coalesce(&records) {
                let first = run[0];
                let read = Instant::now();
                let buf = read_run(&victim.file, run)?;
                report.reclaim_read_bytes += buf.len() as u64;
                report.reclaim_read_ms += ms_since(read);
                for rec in run {
                    let at = (rec.offset - first.offset) as usize;
                    let c = Committed {
                        record: Record::check(&buf[at..at + rec.len], rec.page_off)?,
                        base: rec.base,
                        seg_gen: 0,
                    };
                    if foldable(rec.page_off)
                        && rec.page_off + PAGE <= published
                        && (victim.past_ceiling
                            || spends_half_the_unset(rec.page_off, rec.base, &c.record))
                    {
                        let at = rounds.push(&self.dir, body, rec.page_off, &c, report)?;
                        report.folded += 1;
                        out.folded.push((rec.page_off, at));
                    } else {
                        out.carried
                            .push_slots(rec.page_off, rec.base, c.record.ordered()?);
                        report.carried += 1;
                    }
                }
            }
        }
        Ok(out)
    }

    /// The segments to reclaim, sparsest first, each with whether the
    /// threshold was past its ceiling when it was taken. A record of a page
    /// this generation rewrites is not live: it dies with the publish.
    fn pick_victims(
        &self,
        w: &Writer,
        sealed: &SealedNotes,
        new_bytes: u64,
        published: u64,
        durable: u32,
    ) -> Vec<Victim> {
        let rolled: HashSet<u32> = w.rolled.iter().map(|a| a.id).collect();
        let (candidates, mut total, mut count) = {
            let g = self.lock();
            let through = g.commit.folds_through(durable);
            let mut dying: HashMap<u32, u64> = HashMap::new();
            for (page_off, _) in sealed.pages() {
                if let At::Rec { seg, len, .. } = placed_at(&g, page_off).at() {
                    *dying.entry(seg).or_default() += len as u64;
                }
            }
            let total: u64 = g
                .segs
                .iter()
                .filter(|(id, _)| !rolled.contains(id))
                .map(|(_, s)| s.data())
                .sum::<u64>()
                + w.rolled
                    .iter()
                    .map(|a| a.end.saturating_sub(BLOCK))
                    .sum::<u64>();
            let count = g.segs.keys().filter(|id| !rolled.contains(id)).count() + rolled.len();
            // (live permille, id, file, size), sparsest first.
            let mut candidates: Vec<(u64, u32, Arc<TableFile>, u64)> = g
                .segs
                .iter()
                .filter(|&(&id, s)| {
                    id != w.active.id && !rolled.contains(&id) && s.last_gen <= through
                })
                .map(|(&id, s)| {
                    let live_bytes = s
                        .live_bytes
                        .saturating_sub(dying.get(&id).copied().unwrap_or(0));
                    let live = live_bytes.saturating_mul(1000) / s.data().max(1);
                    (live, id, Arc::clone(&s.file), s.data())
                })
                .collect();
            candidates.sort_by_key(|&(live, id, _, _)| (live, id));
            (candidates, total, count)
        };
        let cap = new_bytes
            .max(self.policy.segment_cap)
            .saturating_mul(self.policy.reclaim_rate);
        let mut reclaimed = 0u64;
        let mut victims = Vec::new();
        for (live, id, file, size) in candidates {
            if count <= self.policy.keep_segments {
                break;
            }
            let limit = self.policy.reclaim_below(total, count, published);
            let past_ceiling = limit >= 1000;
            if !past_ceiling && live >= limit {
                break;
            }
            if reclaimed > 0 && reclaimed + size > cap {
                break;
            }
            victims.push(Victim {
                id,
                file,
                past_ceiling,
            });
            reclaimed += size;
            total = total.saturating_sub(size);
            count -= 1;
        }
        victims
    }

    /// The live records in `victims`, by segment and in file order, from the
    /// RAM directory, and how many records of `sealed` pages it left out: this
    /// generation rewrites those pages, so their records die unread. The
    /// writer lock is held, so the directory only changes under this writer's
    /// own publish; `Inner` is released between chunks.
    fn live_records(
        &self,
        victims: &[Victim],
        sealed: &SealedNotes,
    ) -> (HashMap<u32, Vec<LiveRecord>>, u64) {
        let mut out: HashMap<u32, Vec<LiveRecord>> = HashMap::new();
        let mut dropped = 0;
        if victims.is_empty() {
            return (out, dropped);
        }
        let ids: HashSet<u32> = victims.iter().map(|v| v.id).collect();
        let mut page = 0usize;
        loop {
            let g = self.lock();
            let end = (page + LOAD_CHUNK_PAGES as usize).min(g.directory.len());
            for (i, at) in g.directory[page.min(end)..end].iter().enumerate() {
                if let At::Rec {
                    seg,
                    offset,
                    len,
                    base,
                } = at.at()
                {
                    let page_off = (page + i) as u64 * PAGE;
                    if !ids.contains(&seg) {
                        continue;
                    }
                    if sealed.has_page(page_off) {
                        dropped += 1;
                    } else {
                        out.entry(seg).or_default().push(LiveRecord {
                            page_off,
                            offset,
                            len,
                            base,
                        });
                    }
                }
            }
            if end >= g.directory.len() {
                break;
            }
            page = end;
        }
        for records in out.values_mut() {
            records.sort_unstable_by_key(|r| r.offset);
        }
        (out, dropped)
    }

    /// Write one generation and install it.
    fn publish(
        &self,
        w: &mut Writer,
        plan: Plan,
        report: &mut CheckpointReport,
    ) -> Result<(), StoreError> {
        self.settle(w)?;
        let generation = w.next_generation;
        w.next_generation = generation
            .checked_add(1)
            .ok_or(StoreError::Corrupt("spent.delta generation overflow"))?;
        let t = Instant::now();
        let Appended {
            mut placed,
            record_bytes,
            wrote_segs,
            pages: written_pages,
        } = self.append_records(w, [&plan.pages, &plan.carried], plan.folded.len())?;
        let mut dir_pages = u64::from(plan.dir_pages).max(written_pages);
        report.record_bytes = record_bytes;
        report.seg_write_ms = ms_since(t);
        let t = Instant::now();
        for &(page_off, at) in &plan.folded {
            placed.push((page_off / PAGE, at));
            dir_pages = dir_pages.max(page_off / PAGE + 1);
        }
        let idx_write = ms_since(t);
        let t = Instant::now();
        for seg in w.rolled.iter().chain(std::iter::once(&w.active)) {
            seg.file.sync_data_only()?;
        }
        report.seg_sync_ms = ms_since(t);
        let dir_pages = u32::try_from(dir_pages)
            .map_err(|_| StoreError::Corrupt("spent.delta idx past u32 pages"))?;
        report.idx_entries = placed.len() as u64;
        let t = Instant::now();
        let (dir_at, new_idx) = self.write_directory(w, generation, &placed, dir_pages)?;
        report.idx_write_ms = idx_write + ms_since(t);
        let wrote = Wrote {
            record_bytes,
            ..plan.wrote
        };
        let mut commit = self.lock().commit.next(
            generation,
            dir_pages,
            w.active.id,
            w.active.end,
            dir_at.bytes,
            &wrote,
            plan.seal,
        );
        commit.base = dir_at.base;
        commit.base_pages = dir_at.base_pages;
        commit.log_end = dir_at.log_end;
        let t = Instant::now();
        let landed_err = self.write_commit(&commit)?;
        report.commit_ms = ms_since(t);
        if let Some(idx) = new_idx {
            let old = std::mem::replace(&mut w.idx, idx);
            // A rename that landed before an error may not be durable, and
            // the old commit still names the old pair: open removes it.
            if landed_err.is_none() {
                self.unlink_idx(old);
            }
        }
        let t = Instant::now();
        {
            let mut g = self.lock();
            for a in w.rolled.drain(..).chain(std::iter::once(Active {
                id: w.active.id,
                file: Arc::clone(&w.active.file),
                end: w.active.end,
            })) {
                let s = g.segs.entry(a.id).or_insert_with(|| Seg {
                    file: a.file,
                    size: HEAD,
                    live_bytes: 0,
                    live_records: 0,
                    last_gen: 0,
                });
                s.size = a.end;
                if wrote_segs.contains(&a.id) {
                    s.last_gen = generation;
                }
            }
            let new_len = dir_pages as usize;
            for page in new_len..g.directory.len() {
                g.place(page, Placed::default());
            }
            g.directory.resize(new_len, Placed::default());
            for (page, at) in placed {
                g.place(page as usize, at);
            }
            g.commit = commit;
            if let Some(r) = &plan.retire {
                if g.sealed.as_ref().is_some_and(|s| Arc::ptr_eq(s, r)) {
                    g.sealed = None;
                    g.sealed_tip = None;
                }
            }
            let empty: Vec<u32> = g
                .segs
                .iter()
                .filter(|&(&id, s)| id != w.active.id && s.live_records == 0)
                .map(|(&id, _)| id)
                .collect();
            for id in empty {
                if let Some(s) = g.segs.remove(&id) {
                    w.doomed.push((id, s.file));
                }
            }
        }
        report.install_ms = ms_since(t);
        if let Some(e) = landed_err {
            w.dir_unsynced = true;
            return Err(e);
        }
        let t = Instant::now();
        self.unlink_doomed(w);
        report.unlink_ms = ms_since(t);
        self.pace.notify_room();
        Ok(())
    }

    /// Rename `commit` into place and sync the directory. An error after the
    /// rename landed, a failed directory sync, returns that error to the
    /// caller to report after the install: RAM must match the commit on
    /// disk, or the next generation would write over the log batch it
    /// names. Any other error leaves the old commit.
    fn write_commit(&self, commit: &Commit) -> Result<Option<StoreError>, StoreError> {
        let path = self.dir.join(SPEND_DELTA_COMMIT_NAME);
        let err = match write_synced_tmp_rename(&path, &commit.encode()) {
            #[cfg(debug_assertions)]
            Ok(()) if take_fail_after_rename(&self.dir) => StoreError::io(
                &self.dir,
                std::io::Error::other("injected directory sync failure"),
            ),
            Ok(()) => return Ok(None),
            Err(e) => e,
        };
        match read_commit(&path) {
            Ok(Some(on_disk)) if on_disk == *commit => Ok(Some(err)),
            _ => Err(err),
        }
    }

    /// Sync `spent.delta/` again when the last commit's rename landed but
    /// its directory sync failed, before anything builds on that commit.
    fn settle(&self, w: &mut Writer) -> Result<(), StoreError> {
        if w.dir_unsynced {
            sync_dir(&self.dir)?;
            w.dir_unsynced = false;
        }
        Ok(())
    }

    /// Append each page's record to the active segment, rolling at the cap.
    ///
    /// The committed records may end mid-block, and the first block holds the
    /// segment's header. This generation starts on the next block past both,
    /// or in a new segment when that leaves no room, so no write touches a
    /// block holding committed bytes. A segment holds
    /// [`Policy::segment_cap`] record bytes past its first block. `plans`
    /// are written in order: the merged pages, then the carried ones.
    fn append_records(
        &self,
        w: &mut Writer,
        plans: [&PlanPages; 2],
        extra: usize,
    ) -> Result<Appended, StoreError> {
        let mut out = Appended {
            placed: Vec::with_capacity(plans.iter().map(|p| p.len()).sum::<usize>() + extra),
            record_bytes: 0,
            wrote_segs: BTreeSet::new(),
            pages: 0,
        };
        let cap = BLOCK + self.policy.segment_cap;
        for (i, (p, bytes)) in plans.iter().flat_map(|p| p.iter()).enumerate() {
            let len = bytes.len() as u64;
            if i == 0 {
                let aligned = align_block(w.active.end.max(BLOCK));
                if aligned + len <= cap {
                    w.active.end = aligned;
                } else {
                    self.roll(w)?;
                }
            } else if w.active.end + len > cap {
                self.roll(w)?;
            }
            let page = p.page_off / PAGE;
            let at = Placed::rec(w.active.id, w.active.end, bytes.len(), p.base)?;
            w.active.file.write_uncommitted(w.active.end, bytes)?;
            w.active.end += len;
            out.record_bytes += len;
            out.wrote_segs.insert(w.active.id);
            out.pages = out.pages.max(page + 1);
            out.placed.push((page, at));
        }
        Ok(out)
    }

    /// Write this generation's entries and sync them. They go to the log as
    /// one batch on a fresh block, or, once the log outgrows its base, into a
    /// new base and empty log no commit names yet. Returns where the commit
    /// finds the directory and the new files to install.
    fn write_directory(
        &self,
        w: &mut Writer,
        generation: u32,
        placed: &[(u64, Placed)],
        dir_pages: u32,
    ) -> Result<(DirAt, Option<Idx>), StoreError> {
        let entries: Vec<(u32, Entry)> = placed
            .iter()
            .map(|&(page, at)| (page as u32, entry_bytes(generation, at)))
            .collect();
        let commit = self.lock().commit.clone();
        let start = align_block(commit.log_end);
        let end = start + BATCH_HEAD + entries.len() as u64 * LOG_ENTRY;
        let base_bytes = u64::from(commit.base_pages) * DIR_ENTRY;
        if end - HEAD > base_bytes.max(self.policy.log_fold_min) {
            let mut all = w.idx.entries(&commit)?;
            all.resize(dir_pages as usize, [0u8; DIR_ENTRY as usize]);
            for (page, entry) in entries {
                all[page as usize] = entry;
            }
            let idx = Idx::create(&self.dir, generation, &all)?;
            sync_dir(&self.dir)?;
            let at = DirAt {
                base: generation,
                base_pages: dir_pages,
                log_end: HEAD,
                bytes: u64::from(dir_pages) * DIR_ENTRY,
            };
            return Ok((at, Some(idx)));
        }
        let mut batch = Vec::with_capacity((end - start) as usize);
        batch.extend_from_slice(&generation.to_le_bytes());
        batch.extend_from_slice(&(entries.len() as u32).to_le_bytes());
        batch.extend_from_slice(&dir_pages.to_le_bytes());
        batch.extend_from_slice(&0u32.to_le_bytes());
        for (page, entry) in &entries {
            batch.extend_from_slice(&page.to_le_bytes());
            batch.extend_from_slice(entry);
        }
        let sum = crc32(&[&batch]);
        batch[12..16].copy_from_slice(&sum.to_le_bytes());
        w.idx.log.write_uncommitted(start, &batch)?;
        w.idx.log.sync_data_only()?;
        let at = DirAt {
            base: commit.base,
            base_pages: commit.base_pages,
            log_end: end,
            bytes: batch.len() as u64,
        };
        Ok((at, None))
    }

    /// Remove the directory files a new base replaced. A failure leaves files
    /// the next open removes.
    fn unlink_idx(&self, old: Idx) {
        let id = old.id;
        // Close before unlinking: Windows refuses to delete an open file.
        drop(old);
        for name in [idx_name(id), log_name(id)] {
            if let Err(e) = remove_if_present(&self.dir, &name) {
                rbitcoin_log::warn!("store: spend delta {name}: {e}");
            }
        }
        if let Err(e) = sync_dir(&self.dir) {
            rbitcoin_log::warn!("store: spend delta idx unlink sync: {e}");
        }
    }

    /// Seal the active segment at its end and start the next one.
    fn roll(&self, w: &mut Writer) -> Result<(), StoreError> {
        let id = w
            .active
            .id
            .checked_add(1)
            .filter(|&id| u64::from(id) <= mask(SEG_BITS))
            .ok_or(StoreError::Corrupt("spent.delta segment ids exhausted"))?;
        // A cut on a block boundary rewrites no block, and the header is
        // left alone: the commit names the length.
        w.active.file.shrink_to(align_block(w.active.end))?;
        // No commit names an id past the writer's active segment, so a file
        // there is what a failed roll left.
        remove_if_present(&self.dir, &segment_name(id))?;
        let file = Arc::new(create_aligned(
            self.dir.join(segment_name(id)),
            TableKind::SpendDelta,
        )?);
        let done = std::mem::replace(
            &mut w.active,
            Active {
                id,
                file,
                end: BLOCK,
            },
        );
        w.rolled.push(done);
        Ok(())
    }

    /// Unlink segments no entry names once this writer holds the last
    /// handle. A reader that took one before the install keeps reading it;
    /// the next publish retries.
    fn unlink_doomed(&self, w: &mut Writer) {
        let (free, held): (Vec<_>, Vec<_>) = std::mem::take(&mut w.doomed)
            .into_iter()
            .partition(|(_, file)| Arc::strong_count(file) == 1);
        w.doomed = held;
        if free.is_empty() {
            return;
        }
        for (id, file) in free {
            // Close before unlinking: Windows refuses to delete an open file.
            drop(file);
            let path = self.dir.join(segment_name(id));
            if let Err(e) = std::fs::remove_file(&path) {
                rbitcoin_log::warn!("store: spend delta segment {}: {e}", path.display());
            }
        }
        if let Err(e) = sync_dir(&self.dir) {
            rbitcoin_log::warn!("store: spend delta segment unlink sync: {e}");
        }
    }

    /// Cut every committed slot at or past `body_end`.
    ///
    /// Pages wholly past it leave the directory: the batch names the shorter
    /// directory, and a page past it reads zero. A page straddling it is
    /// rewritten without the cut slots, as a record over zeroes: a fold may
    /// have written that page while it lay below a longer end, and its
    /// `spent.body` bytes past the cut are not slots of the creates that will
    /// take those offsets.
    fn clip(&self, body: &dyn SpentBody, body_end: u64) -> Result<(), StoreError> {
        let mut w = self.writer();
        let (dir_pages, tip) = {
            let g = self.lock();
            (g.commit.dir_pages, g.commit.sealed_tip().unwrap_or(0))
        };
        let kept_pages = body_end.div_ceil(PAGE).min(u64::from(dir_pages));
        let mut pages = PlanPages::default();
        let page_off = page_of(body_end);
        if !body_end.is_multiple_of(PAGE) && body_end / PAGE < u64::from(dir_pages) {
            let at = placed_at(&self.lock(), page_off);
            let mut slots = BTreeMap::new();
            if at.base().is_body() {
                let mut image = vec![0u8; (body_end - page_off) as usize];
                body.read(page_off, &mut image)?;
                for (rel, meta) in image
                    .chunks_exact(SLOT)
                    .enumerate()
                    .skip(first_rel(page_off))
                {
                    if meta != ZERO.as_slice() {
                        slots.insert(rel as u16, meta.try_into().expect("one slot"));
                    }
                }
            }
            if let Some(owned) = self.committed_record(page_off)? {
                slots.extend(owned.view().record.ordered()?);
            }
            let before = slots.len();
            slots.retain(|&rel, _| page_off + (u64::from(rel) + 1) * SLOT as u64 <= body_end);
            if slots.len() != before || at.base().is_body() {
                pages.push_slots(page_off, Base::ZERO, slots);
            }
        }
        let cut = self.lock().directory[kept_pages as usize..]
            .iter()
            .any(|at| at.is_some());
        if pages.is_empty() && !cut {
            return Ok(());
        }
        // `kept_pages` is at most `dir_pages`, a `u32`.
        let plan = Plan {
            pages,
            dir_pages: kept_pages as u32,
            seal: Seal::Raise(tip),
            ..Plan::default()
        };
        self.publish(&mut w, plan, &mut CheckpointReport::default())
    }
}

/// A committed record read back, with its base and the newest generation
/// its segment holds.
struct Committed<'a> {
    record: Record<'a>,
    base: Base,
    seg_gen: u32,
}

/// One committed record read on its own, for the paths that rewrite a
/// page at a time.
struct OwnedCommitted {
    bytes: Vec<u8>,
    base: Base,
    seg_gen: u32,
}

impl OwnedCommitted {
    /// The record was checked against its page when it was read.
    fn view(&self) -> Committed<'_> {
        Committed {
            record: Record(&self.bytes),
            base: self.base,
            seg_gen: self.seg_gen,
        }
    }
}

/// The committed records of a checkpoint's sealed pages: one buffer, and a
/// row per record ascending by page. The rows are a subset of the pages
/// the merge walks, in the same order.
struct CommittedRecords {
    buf: Vec<u8>,
    rows: Vec<CommittedRow>,
    /// The row the next page with a record takes.
    next: usize,
}

struct CommittedRow {
    page_off: u64,
    at: std::ops::Range<usize>,
    base: Base,
    seg_gen: u32,
}

impl CommittedRecords {
    /// The record of `page_off`, if it has one. Every page the rows were
    /// read for is asked for once, ascending.
    fn take(&mut self, page_off: u64) -> Option<Committed<'_>> {
        let row = self.rows.get(self.next)?;
        debug_assert!(row.page_off >= page_off, "a committed row was skipped");
        if row.page_off != page_off {
            return None;
        }
        self.next += 1;
        Some(Committed {
            record: Record(&self.buf[row.at.clone()]),
            base: row.base,
            seg_gen: row.seg_gen,
        })
    }
}

/// One generation's content.
struct Plan {
    pages: PlanPages,
    /// Records reclaim copies unchanged, appended after `pages`. A buffer of
    /// their own, so they never regrow the one the merge sized.
    carried: PlanPages,
    /// Pages a fold wrote whole, and the entry each now takes.
    folded: Vec<(u64, Placed)>,
    /// Directory length before these pages; a page past it extends it.
    dir_pages: u32,
    seal: Seal,
    /// The sealed layer this publishes; dropped from RAM on install.
    retire: Option<Arc<SealedNotes>>,
    wrote: Wrote,
}

impl Default for Plan {
    fn default() -> Self {
        Self {
            pages: PlanPages::default(),
            carried: PlanPages::default(),
            folded: Vec::new(),
            dir_pages: 0,
            seal: Seal::Raise(0),
            retire: None,
            wrote: Wrote::default(),
        }
    }
}

/// A segment reclaim takes.
struct Victim {
    id: u32,
    file: Arc<TableFile>,
    /// The reclaim threshold was at its ceiling: every record folds.
    past_ceiling: bool,
}

/// One live record in a victim segment.
#[derive(Clone, Copy)]
struct LiveRecord {
    page_off: u64,
    offset: u64,
    len: usize,
    base: Base,
}

/// One pread covering `run`, from its first record's offset.
fn read_run(file: &TableFile, run: &[LiveRecord]) -> Result<Vec<u8>, StoreError> {
    let (first, last) = (run[0], run[run.len() - 1]);
    let mut buf = vec![0u8; (last.offset + last.len as u64 - first.offset) as usize];
    file.pread_all(first.offset, &mut buf)?;
    Ok(buf)
}

/// Split offset-ordered records into runs of touching records, one span
/// each. A gap between two records holds dead bytes, which are not read.
fn coalesce(records: &[LiveRecord]) -> impl Iterator<Item = &[LiveRecord]> + '_ {
    records.chunk_by(|a, b| b.offset == a.offset + a.len as u64)
}

/// `ranges` are `(start, len)` into `buf`, in order, and do not overlap.
fn disjoint_slices<'a>(buf: &'a mut [u8], ranges: &[(usize, usize)]) -> Vec<&'a mut [u8]> {
    let mut out = Vec::with_capacity(ranges.len());
    let mut rest = buf;
    let mut consumed = 0usize;
    for &(start, len) in ranges {
        let skip = start - consumed;
        let (_, mid) = rest.split_at_mut(skip);
        let (mine, tail) = mid.split_at_mut(len);
        out.push(mine);
        rest = tail;
        consumed = start + len;
    }
    out
}

#[derive(Default)]
struct Folded {
    folded: Vec<(u64, Placed)>,
    /// Sparse records to copy to the active segment.
    carried: PlanPages,
}

/// The sealed pages merged over their committed records.
struct Merged {
    pages: PlanPages,
    new_bytes: u64,
    /// Record bytes of pages whose merged record spends half their unset
    /// slots.
    dense_bytes: u64,
}

/// What decides whether a touched page's record folds.
#[derive(Clone, Copy)]
struct TouchCtx {
    /// Newest generation the durable marker covers; none folds without one.
    through: Option<u32>,
    published: u64,
}

/// Page 0 never folds: its first block holds the `spent.body` header, which
/// every Class A publish rewrites. Its record stays in the delta.
fn foldable(page_off: u64) -> bool {
    page_off != 0
}

/// Folds, a round at a time. A page whose base is `spent.body` is composed
/// over that page and goes to a `fold` file, synced and renamed, before
/// `spent.body` is touched: a crash that tears the page loses no slot only
/// `spent.body` held, since open copies the file again. A page over zeroes
/// is written straight to `spent.body`: until the commit names it, nothing
/// reads that page there. The round then syncs `spent.body` before the next
/// one starts, so the file and the RAM it takes stay bounded. Every round
/// composes into the same buffers, emptied but not freed after its copy, so
/// a checkpoint that folds many rounds allocates them once.
struct Rounds {
    file: FoldFile,
    /// Pages over zeroes, composed whole: `(page_off:u64, image)` back to
    /// back, as in the `fold` file.
    direct: Vec<u8>,
    /// The page being composed.
    image: Vec<u8>,
    size: usize,
    records: u64,
    bytes: u64,
}

impl Rounds {
    fn new(size: usize) -> Self {
        Self {
            file: FoldFile::new(),
            direct: Vec::new(),
            image: vec![0u8; PAGE as usize],
            size: size.max(1),
            records: 0,
            bytes: 0,
        }
    }

    fn pages(&self) -> usize {
        self.file.count as usize + self.direct.len() / FOLD_PAGE
    }

    /// Compose a committed record over its base. Returns the entry the page
    /// takes once this generation commits: the body with its count, or zero
    /// when no slot is set, which writes nothing.
    fn push(
        &mut self,
        dir: &Path,
        body: &mut dyn SpentBody,
        page_off: u64,
        c: &Committed<'_>,
        report: &mut CheckpointReport,
    ) -> Result<Placed, StoreError> {
        let t = Instant::now();
        if c.base.is_body() {
            body.read(page_off, &mut self.image)?;
        } else {
            self.image.fill(0);
        }
        for (rel, meta) in c.record.entries() {
            let at = usize::from(rel) * SLOT;
            self.image[at..at + SLOT].copy_from_slice(&meta);
        }
        let base = Base::of(filled_in(page_off, &self.image));
        if base.is_body() {
            if c.base.is_body() {
                self.file.push(page_off, &self.image);
            } else {
                self.direct.extend_from_slice(&page_off.to_le_bytes());
                self.direct.extend_from_slice(&self.image);
            }
        }
        report.fold_compose_ms += ms_since(t);
        self.records += 1;
        if self.pages() >= self.size {
            self.flush(dir, body, report)?;
        }
        Ok(Placed::body(base))
    }

    /// Copy the last, partial round.
    fn finish(
        &mut self,
        dir: &Path,
        body: &mut dyn SpentBody,
        report: &mut CheckpointReport,
    ) -> Result<(), StoreError> {
        if self.pages() == 0 {
            return Ok(());
        }
        self.flush(dir, body, report)
    }

    fn flush(
        &mut self,
        dir: &Path,
        body: &mut dyn SpentBody,
        report: &mut CheckpointReport,
    ) -> Result<(), StoreError> {
        let t = Instant::now();
        if self.file.count > 0 {
            self.bytes += self.file.write(dir)?;
        }
        report.fold_file_ms += ms_since(t);
        let t = Instant::now();
        for (page_off, image) in self.file.images().chain(page_images(&self.direct)) {
            body.write(page_off, image)?;
            self.bytes += image.len() as u64;
        }
        report.body_write_ms += ms_since(t);
        self.file.clear();
        self.direct.clear();
        let t = Instant::now();
        body.sync()?;
        report.body_sync_ms += ms_since(t);
        report.fold_rounds += 1;
        Ok(())
    }
}

/// One `fold` file page: its offset, then its image.
const FOLD_PAGE: usize = 8 + PAGE as usize;

/// `(page_off, image)` for each page laid out back to back in `buf`.
fn page_images(buf: &[u8]) -> impl Iterator<Item = (u64, &[u8])> + '_ {
    buf.chunks_exact(FOLD_PAGE).map(|c| (rd_u64(c, 0), &c[8..]))
}

/// Whole composed pages a fold is copying over `spent.body` pages that
/// readers use. Layout: `magic:[u8; 4] | schema:u16 | reserved:u16 |
/// count:u32`, then `count` × `(page_off:u64, image:[u8; 4096])`. Written to
/// a synced temp file and renamed, so a `fold` that exists is whole.
struct FoldFile {
    buf: Vec<u8>,
    count: u32,
}

const FOLD_HEAD: usize = 12;

impl FoldFile {
    fn new() -> Self {
        let mut buf = Vec::new();
        buf.extend_from_slice(&STORE_MAGIC);
        buf.extend_from_slice(&FILE_SCHEMA_VERSION.to_le_bytes());
        buf.extend_from_slice(&0u16.to_le_bytes());
        buf.extend_from_slice(&0u32.to_le_bytes());
        Self { buf, count: 0 }
    }

    fn push(&mut self, page_off: u64, image: &[u8]) {
        self.buf.extend_from_slice(&page_off.to_le_bytes());
        self.buf.extend_from_slice(image);
        self.count += 1;
    }

    /// Back to the header alone; the buffer keeps its capacity.
    fn clear(&mut self) {
        self.buf.truncate(FOLD_HEAD);
        self.count = 0;
    }

    /// Write and rename the file. Returns its length.
    fn write(&mut self, dir: &Path) -> Result<u64, StoreError> {
        self.buf[8..12].copy_from_slice(&self.count.to_le_bytes());
        write_synced_tmp_rename(&dir.join(SPEND_DELTA_FOLD_NAME), &self.buf)?;
        Ok(self.buf.len() as u64)
    }

    fn images(&self) -> impl Iterator<Item = (u64, &[u8])> + '_ {
        page_images(&self.buf[FOLD_HEAD..])
    }
}

/// True when `record`'s spends cover at least half the slots its page has
/// unset under `base`: no page is read. A spend is a nonzero entry; an
/// entry rewriting a slot already spent, which only a reorg makes, counts
/// too and folds the page a little early.
fn spends_half_the_unset(page_off: u64, base: Base, record: &Record<'_>) -> bool {
    let spends = record.entries().filter(|&(_, meta)| meta != ZERO).count() as u64;
    2 * spends >= base.unset(page_off)
}

/// Copy a `fold` file onto `spent.body` again, sync, and remove it. Copying
/// twice is safe: the commit before the fold still reads those pages from
/// `spent.body` and its records, and the images are those pages with the
/// folded records laid over them. Open may have cut `spent.body` back since
/// the fold, so each image is copied only below the published end; `clip`
/// then rewrites the page that end straddles.
fn replay_fold_file(dir: &Path, body: &mut dyn SpentBody) -> Result<(), StoreError> {
    let path = dir.join(SPEND_DELTA_FOLD_NAME);
    let raw = match std::fs::read(&path) {
        Ok(raw) => raw,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(StoreError::io(&path, e)),
    };
    if raw.len() < FOLD_HEAD || raw[0..4] != STORE_MAGIC {
        return Err(StoreError::Corrupt("spent.delta/fold is not a fold file"));
    }
    let count = rd_u32(&raw, 8) as usize;
    if raw.len() != FOLD_HEAD + count * FOLD_PAGE {
        return Err(StoreError::Corrupt("spent.delta/fold short"));
    }
    let published = body.published_len();
    for (page_off, image) in page_images(&raw[FOLD_HEAD..]) {
        if !page_off.is_multiple_of(PAGE) || !foldable(page_off) {
            return Err(StoreError::Corrupt("spent.delta/fold names a torn page"));
        }
        let keep = published.saturating_sub(page_off).min(PAGE) as usize;
        if keep > 0 {
            body.write(page_off, &image[..keep])?;
        }
    }
    body.sync()?;
    rbitcoin_log::info!("store: spend delta copied {count} folded pages onto spent.body again");
    remove_fold_file(dir)
}

/// Remove `fold` and sync the directory; one already gone is fine. A
/// failure is an error: a `fold` left behind would be copied again at the
/// next checkpoint, over pages a later direct fold may have rewritten.
fn remove_fold_file(dir: &Path) -> Result<(), StoreError> {
    #[cfg(debug_assertions)]
    {
        if FAIL_FOLD_REMOVAL
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(dir)
        {
            return Err(StoreError::io(
                dir,
                std::io::Error::other("injected fold removal failure"),
            ));
        }
    }
    remove_if_present(dir, SPEND_DELTA_FOLD_NAME)?;
    sync_dir(dir)
}

fn placed_at(g: &Inner, page_off: u64) -> Placed {
    g.directory
        .get((page_off / PAGE) as usize)
        .copied()
        .unwrap_or_default()
}

/// What the directory scan learned about each segment it names.
#[derive(Default)]
struct SegScan {
    live_bytes: u64,
    live_records: u64,
    last_gen: u32,
    /// End of the furthest record an entry names.
    end: u64,
}

struct DirScan {
    directory: Vec<Placed>,
    segs: BTreeMap<u32, SegScan>,
}

/// Build the RAM directory and the per-segment live counts from the
/// committed entries. Nothing is written: a crash left nothing the commit
/// names past its lengths.
fn scan_directory(entries: &[Entry], commit: &Commit) -> Result<DirScan, StoreError> {
    let mut directory = Vec::with_capacity(entries.len());
    let mut segs: BTreeMap<u32, SegScan> = BTreeMap::new();
    for e in entries {
        let at = entry_placed(e)?;
        if let At::Rec {
            seg, offset, len, ..
        } = at.at()
        {
            if seg > commit.active
                || (seg == commit.active && offset + len as u64 > commit.active_end)
            {
                return Err(StoreError::Corrupt(
                    "spent.delta idx names bytes past the committed segment",
                ));
            }
            let s = segs.entry(seg).or_default();
            s.live_bytes += len as u64;
            s.live_records += 1;
            s.last_gen = s.last_gen.max(entry_generation(e));
            s.end = s.end.max(offset + len as u64);
        }
        directory.push(at);
    }
    Ok(DirScan { directory, segs })
}

/// Remove every `idx.<id>` and `log.<id>` but the pair `keep` names: what a
/// fold of the log into a new base left when its commit was not renamed, or
/// its old pair when the unlink after it did not finish.
fn remove_unnamed_idx(dir: &Path, keep: Option<u32>) -> Result<(), StoreError> {
    let mut removed = false;
    for (prefix, name) in [("idx.", idx_name as fn(u32) -> String), ("log.", log_name)] {
        for id in ids_named(dir, prefix)? {
            if Some(id) != keep {
                remove_if_present(dir, &name(id))?;
                removed = true;
            }
        }
    }
    if removed {
        sync_dir(dir)?;
    }
    Ok(())
}

/// Open every segment an entry names plus the active one, cut the active one
/// at its committed end, and delete every other segment file.
fn open_segments(
    dir: &Path,
    commit: &Commit,
    named: &BTreeMap<u32, SegScan>,
) -> Result<BTreeMap<u32, Seg>, StoreError> {
    let mut segs = BTreeMap::new();
    let mut ids: BTreeSet<u32> = named.keys().copied().collect();
    ids.insert(commit.active);
    for &id in &ids {
        let path = dir.join(segment_name(id));
        if !path.exists() {
            return Err(StoreError::Corrupt(
                "spent.delta segment named by idx is missing",
            ));
        }
        let file = open_aligned(path, TableKind::SpendDelta)?;
        let scan = named.get(&id);
        let need = scan.map_or(HEAD, |s| s.end.max(HEAD));
        let size = if id == commit.active {
            if physical_len(&file)? < commit.active_end {
                return Err(StoreError::Corrupt(
                    "spent.delta active segment is shorter than its commit",
                ));
            }
            // Bytes a crash left past the end stay: the next generation
            // writes from the next block on, and nothing reads past the
            // commit.
            commit.active_end
        } else {
            physical_len(&file)?
        };
        if size < need {
            return Err(StoreError::Corrupt(
                "spent.delta segment is shorter than its records",
            ));
        }
        segs.insert(
            id,
            Seg {
                file: Arc::new(file),
                size,
                live_bytes: scan.map_or(0, |s| s.live_bytes),
                live_records: scan.map_or(0, |s| s.live_records),
                last_gen: scan.map_or(0, |s| s.last_gen),
            },
        );
    }
    let mut removed = false;
    let present: HashSet<u32> = segment_ids(dir)?.into_iter().collect();
    for id in present.difference(&ids.iter().copied().collect()) {
        let path = dir.join(segment_name(*id));
        std::fs::remove_file(&path).map_err(|e| StoreError::io(&path, e))?;
        removed = true;
    }
    if removed {
        sync_dir(dir)?;
    }
    Ok(segs)
}
