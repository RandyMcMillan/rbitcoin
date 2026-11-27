//! Class B scripthash multimap (Electrum: SHA256(scriptPubKey)).
//!
//! Hybrid layout: head key = 16 B hash prefix; value = pack8 (inline / slab /
//! paged / extent). Body slabs pack ULEB128 fk deltas; vouts expanded from
//! Class A at query.

use crate::error::StoreError;
use crate::file::{GrowPolicy, TableFile, FILE_HEADER_LEN};
use crate::fuse8_filter::SealedFuse8;
use crate::hashhead::{sh_main_shard_count, HeadScale};
use crate::io_backend::ReadIoBackend;
use crate::page_guard::{self, PAGE};
use crate::scripthash_head::{prefix_shard_of, ScriptHashHead};
use crate::scripthash_layout::{
    head_key_from_full, pack8, pack8_bytes, payload_start, slab_bytes, ShHeadValue,
    SH_ALLOC_HEADER_LEN, SH_ALLOC_MAGIC, SH_ALLOC_VERSION, SH_INLINE_CAP, SH_MAX_CLASS,
    SH_MAX_SLAB_CLASS, SH_PAGE_SLAB_CLASS, SH_PREFIX_PAGE,
};
use crate::scripthash_mphf::{mix_key16, MphfHead};
use crate::scripthash_overflow::wipe_legacy_fullsize_overflow;
use crate::scripthash_pages::{
    sh_page_as_array, sh_page_chunk_ranges, sh_page_decode_slice_into, sh_page_extent,
    sh_page_first_off, sh_page_is_last, sh_page_last_fk, sh_page_next,
    sh_page_pack_extent_last_fks, sh_page_pack_fks, sh_page_set_next, sh_page_try_append,
    SH_PAGE_SIZE, SH_PAGE_STREAM_MAX,
};
use crate::scripthash_slabs::{
    decode_slab_payload_into, encode_slab_payload_into, slab_class_for_n_fks_with_slack,
    slab_class_for_packed_len, SH_MEGAKEY_MIN_FKS,
};
use crate::scripthash_sorted_head::SortedHead;
use bitcoin_hashes::{sha256, Hash};
use rbitcoin_primitives::{le_u64, uleb128_len, Fk, TableKind};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Mutex, RwLock};

#[cfg(debug_assertions)]
use std::cell::Cell;

#[cfg(debug_assertions)]
thread_local! {
    static FAIL_CLEAR_SH_GUARD: Cell<bool> = const { Cell::new(false) };
    static FAIL_INGEST_REL_DROP: Cell<bool> = const { Cell::new(false) };
}

#[path = "scripthash_unlink.rs"]
#[doc(hidden)]
pub mod unlink;

/// Durable cold-materialize resume marker (next to `scripthash.head`).
pub const COLD_PROGRESS_NAME: &str = "scripthash.cold_progress";
#[doc(hidden)]
pub const COLD_PROGRESS_MAGIC: &[u8; 8] = b"SHCOLDP1";
/// Max create_fk fully present in durable SH (inclusion HWM; crash catch-up).
pub const INCLUDE_HWM_NAME: &str = "scripthash.include_hwm";

/// Progress after each fully installed prefix shard (SIGINT resume).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ColdProgress {
    /// Next shard index to fill (`0..n_shards`). `n_shards` means all done.
    pub next_shard: u32,
    /// Body bump / logical HWM after last complete shard (orphan incomplete slabs discarded).
    pub body_bump: u64,
    pub live_count: u64,
    pub keys_written: u64,
}

impl ColdProgress {
    pub fn path(store_dir: &Path) -> PathBuf {
        store_dir.join(COLD_PROGRESS_NAME)
    }

    pub fn store(&self, store_dir: &Path) -> Result<(), StoreError> {
        let p = Self::path(store_dir);
        static TMP_SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        // Pack workers seal shards concurrently, so each write stages under
        // its own name; a shared `.tmp` would truncate the other writer.
        let tmp = store_dir.join(format!(
            "{COLD_PROGRESS_NAME}.{}.tmp",
            TMP_SEQ.fetch_add(1, Ordering::Relaxed)
        ));
        let mut buf = Vec::with_capacity(36);
        buf.extend_from_slice(COLD_PROGRESS_MAGIC);
        buf.extend_from_slice(&self.next_shard.to_le_bytes());
        buf.extend_from_slice(&self.body_bump.to_le_bytes());
        buf.extend_from_slice(&self.live_count.to_le_bytes());
        buf.extend_from_slice(&self.keys_written.to_le_bytes());
        crate::file::write_synced_tmp_rename_at(&tmp, &p, &buf)
    }

    pub fn clear(store_dir: &Path) {
        let _ = std::fs::remove_file(Self::path(store_dir));
    }
}

/// One byte in the hot store: the next start wipes and rebuilds the index.
pub const SH_REBUILD_NAME: &str = "scripthash.rebuild";

/// The write-behind apply read a torn slab; rebuild on the next start.
pub fn mark_sh_rebuild(store_dir: &Path) -> Result<(), StoreError> {
    crate::file::write_synced_tmp_rename(&store_dir.join(SH_REBUILD_NAME), &[1])
}

pub fn sh_rebuild_marked(store_dir: &Path) -> bool {
    store_dir.join(SH_REBUILD_NAME).is_file()
}

/// A requested rebuild drops the index files before open decodes them, so a
/// torn file cannot stop the open that would rebuild it. Open then creates
/// an empty index.
pub(crate) fn discard_index_for_rebuild(store_dir: &Path) -> Result<(), StoreError> {
    if !sh_rebuild_marked(store_dir) {
        return Ok(());
    }
    page_guard::clear_sh_marks(store_dir)?;
    for name in ["scripthash.body", "scripthash.ovf", "scripthash.head"] {
        remove_store_path(&store_dir.join(name))?;
    }
    crate::file::fsync_parent_dir(store_dir).map_err(|e| StoreError::io(store_dir, e))
}

/// A finished rebuild drops the mark. A crash before this rebuilds again.
pub fn clear_sh_rebuild(store_dir: &Path) -> Result<(), StoreError> {
    let path = store_dir.join(SH_REBUILD_NAME);
    match std::fs::remove_file(&path) {
        Ok(()) => {
            crate::file::fsync_parent_dir(store_dir).map_err(|e| StoreError::io(store_dir, e))
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(StoreError::io(&path, e)),
    }
}

/// Present while the index may hold a write that is not yet synced. Some of
/// those writes land in place over bytes a durable head still points at (a
/// same-class slab rewritten in place, a freed slab's free-list link, the
/// alloc header), so after an unclean stop the index can read as valid and
/// be wrong. The node refuses to start while it is present (#2074).
pub const SH_DIRTY_NAME: &str = "scripthash.dirty";

pub fn sh_dirty_marked(store_dir: &Path) -> bool {
    store_dir.join(SH_DIRTY_NAME).is_file()
}

/// Sync the directories whose files index writes and wipes create or
/// remove (ingest seal, sealed and packed heads, compaction), so those
/// entries are durable before [`SH_DIRTY_NAME`] goes.
fn fsync_sh_dirs(store_dir: &Path) -> Result<(), StoreError> {
    for d in [
        store_dir.join("scripthash.body"),
        store_dir.join("scripthash.head"),
        store_dir.join(crate::scripthash_overflow::OVERFLOW_DIR),
    ] {
        if d.is_dir() {
            crate::file::fsync_parent_dir(&d).map_err(|e| StoreError::io(&d, e))?;
        }
    }
    Ok(())
}

/// [`SH_DIRTY_NAME`] for one open table. A writer marks before it writes
/// and holds `scope` for the write. [`ScriptHashTable::flush`] clears the
/// mark only if no write started since its sync began (`gen` unchanged), and
/// never while `held`: a mark found at open, or one a failed write left, is
/// for bytes a sync does not repair, so only a wipe removes it.
struct ShDirty {
    scope: RwLock<()>,
    state: Mutex<ShDirtyState>,
}

struct ShDirtyState {
    marked: bool,
    held: bool,
    gen: u64,
}

impl ShDirty {
    fn open(store_dir: &Path) -> Self {
        let marked = sh_dirty_marked(store_dir);
        Self {
            scope: RwLock::new(()),
            state: Mutex::new(ShDirtyState {
                marked,
                held: marked,
                gen: 0,
            }),
        }
    }

    /// A write returned an error partway: keep the mark until a wipe.
    fn hold(&self) {
        self.state.lock().unwrap_or_else(|e| e.into_inner()).held = true;
    }

    fn begin(&self, store_dir: &Path) -> Result<std::sync::RwLockReadGuard<'_, ()>, StoreError> {
        let scope = self.scope.read().unwrap_or_else(|e| e.into_inner());
        let mut s = self.state.lock().unwrap_or_else(|e| e.into_inner());
        s.gen = s.gen.wrapping_add(1);
        if !s.marked {
            crate::file::write_synced_tmp_rename(&store_dir.join(SH_DIRTY_NAME), &[1])?;
            s.marked = true;
        }
        Ok(scope)
    }

    /// The write count with no write in progress.
    fn quiet_gen(&self) -> u64 {
        let _quiet = self.scope.write().unwrap_or_else(|e| e.into_inner());
        self.state.lock().unwrap_or_else(|e| e.into_inner()).gen
    }

    fn is_marked(&self) -> bool {
        self.state.lock().unwrap_or_else(|e| e.into_inner()).marked
    }

    /// The index files were wiped and the wipe synced: nothing is left to
    /// distrust.
    fn clear_after_wipe(&self, store_dir: &Path) -> Result<(), StoreError> {
        let _quiet = self.scope.write().unwrap_or_else(|e| e.into_inner());
        fsync_sh_dirs(store_dir)?;
        let mut s = self.state.lock().unwrap_or_else(|e| e.into_inner());
        Self::remove_marker(&mut s, store_dir)?;
        s.held = false;
        Ok(())
    }

    /// Remove the file and sync its directory. `marked` follows the file:
    /// once the unlink lands it is false even if the sync then fails, so the
    /// next write creates the mark again instead of trusting a missing one.
    fn remove_marker(s: &mut ShDirtyState, store_dir: &Path) -> Result<(), StoreError> {
        let path = store_dir.join(SH_DIRTY_NAME);
        match std::fs::remove_file(&path) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(StoreError::io(&path, e)),
        }
        s.marked = false;
        crate::file::fsync_parent_dir(store_dir).map_err(|e| StoreError::io(store_dir, e))
    }

    fn clear_if_unchanged(&self, store_dir: &Path, gen: u64) -> Result<(), StoreError> {
        let _quiet = self.scope.write().unwrap_or_else(|e| e.into_inner());
        let mut s = self.state.lock().unwrap_or_else(|e| e.into_inner());
        if s.marked && !s.held && s.gen == gen {
            fsync_sh_dirs(store_dir)?;
            Self::remove_marker(&mut s, store_dir)?;
        }
        Ok(())
    }
}

/// Load durable inclusion HWM (`0` if missing/corrupt).
pub fn load_include_hwm(store_dir: &Path) -> u64 {
    let p = store_dir.join(INCLUDE_HWM_NAME);
    let Ok(buf) = std::fs::read(&p) else {
        return 0;
    };
    if buf.len() < 8 {
        return 0;
    }
    let mut word = [0u8; 8];
    word.copy_from_slice(&buf[..8]);
    u64::from_le_bytes(word)
}

/// Store durable inclusion HWM (monotonic: never decreases).
pub fn store_include_hwm(store_dir: &Path, max_create_fk: u64) -> Result<(), StoreError> {
    if max_create_fk == 0 {
        return Ok(());
    }
    let cur = load_include_hwm(store_dir);
    if max_create_fk <= cur {
        return Ok(());
    }
    let p = store_dir.join(INCLUDE_HWM_NAME);
    crate::file::write_synced_tmp_rename(&p, &max_create_fk.to_le_bytes())
}

/// P2TR scriptPubKey: `OP_1` then a 32-byte x-only key.
#[doc(hidden)]
pub fn is_p2tr(spk: &[u8]) -> bool {
    spk.len() == 34 && spk.starts_with(&rbitcoin_primitives::P2TR_PREFIX)
}

/// Electrum scripthash = SHA256(scriptPubKey) (binary; API often reverses for hex).
pub fn script_hash(script: &[u8]) -> [u8; 32] {
    sha256::Hash::hash(script).to_byte_array()
}

/// Store / index create pointer for a scripthash.
///
/// On-disk SH tables store **only** `create_tx_fk` per key (schema v6). Electrum
/// expansion (vout / value / height / full txid) is a query-layer join, not part
/// of this store type.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ScriptHashRecord {
    pub scripthash: [u8; 32],
    pub create_tx_fk: Fk,
}

impl ScriptHashRecord {
    pub fn from_fk(scripthash: [u8; 32], create_tx_fk: Fk) -> Self {
        Self {
            scripthash,
            create_tx_fk,
        }
    }

    pub fn is_tombstone(&self) -> bool {
        self.create_tx_fk.is_null()
    }
}

/// Timing breakdown for one [`ScriptHashTable::put_create_batch_append`] (nanoseconds).
#[derive(Clone, Copy, Debug, Default)]
pub struct AppendTiming {
    pub sort_ns: u64,
    pub seed_ns: u64,
    pub body_ns: u64,
    pub head_ns: u64,
}

/// Slab and page writes, and slab frees, an ingest batch holds back from one body.
///
/// An ingest batch packs every key first. The payloads land only after the
/// allocator header that stops naming a popped offset is durable, and a
/// replaced slab joins the free list only after the head that named it is
/// durable. The trade is RAM for that order: the batch keeps every new slab
/// and page image until its body barrier, one contiguous buffer per body.
#[derive(Default)]
#[doc(hidden)]
pub struct StagedSlabs {
    bytes: Vec<u8>,
    writes: Vec<(u64, usize)>,
    frees: Vec<(u8, u64)>,
    /// Alignment-gap chunks the allocator carved. Their free-list links
    /// share a block with the slab below the old bump, so they are imaged
    /// and pushed with the frees, inside the armed commit.
    pub(crate) gaps: Vec<(u8, u64)>,
}

impl StagedSlabs {
    #[doc(hidden)]
    pub fn write(&mut self, off: u64, bytes: &[u8]) {
        self.writes.push((off, bytes.len()));
        self.bytes.extend_from_slice(bytes);
    }

    #[doc(hidden)]
    pub fn free(&mut self, class: u8, off: u64) {
        self.frees.push((class, off));
    }

    #[doc(hidden)]
    pub fn write_to(&self, body: &TableFile) -> Result<(), StoreError> {
        let mut at = 0usize;
        for &(off, len) in &self.writes {
            body.write_at(off, &self.bytes[at..at + len])?;
            at += len;
        }
        Ok(())
    }
}

/// One page image a mark takes: the store-relative file, its offset, and
/// the bytes, borrowed from the caller's imaged pages.
type PageImageRef<'a> = (&'a str, u64, &'a [u8]);

#[derive(Default)]
pub(super) struct ArmedSh {
    files: BTreeMap<String, BTreeSet<String>>,
}

fn body_sh_mark(dir: &Path, body: &TableFile) -> Result<String, StoreError> {
    let rel = page_guard::guard_rel_name(dir, body.path())?;
    if let Some(rest) = rel.strip_prefix("scripthash.body/") {
        Ok(page_guard::sh_mark(rest))
    } else {
        Ok(page_guard::sh_mark("ovf"))
    }
}

fn image_range(
    pages: &mut BTreeMap<(String, String, u64), [u8; PAGE as usize]>,
    mark: &str,
    rel: &str,
    path: &Path,
    file: &File,
    off: u64,
    len: u64,
) -> Result<(), StoreError> {
    if len == 0 {
        return Ok(());
    }
    let first = (off / PAGE) * PAGE;
    let last = ((off + len - 1) / PAGE) * PAGE;
    let mut block = first;
    while block <= last {
        let key = (mark.to_string(), rel.to_string(), block);
        if let std::collections::btree_map::Entry::Vacant(e) = pages.entry(key) {
            e.insert(page_guard::read_live_page(path, file, block)?);
        }
        block = block.saturating_add(PAGE);
    }
    Ok(())
}

#[cfg(debug_assertions)]
fn take_fail_sh_guard_clear() -> bool {
    FAIL_CLEAR_SH_GUARD.with(|c| c.replace(false))
}

/// One body's share of an ingest batch.
struct BodyStage<'a> {
    body: &'a TableFile,
    alloc: &'a Mutex<AllocState>,
    slabs: StagedSlabs,
    /// Packing took an offset off this body's free list.
    popped: bool,
}

/// One ingest batch between packing and its barriers.
#[derive(Default)]
struct IngestBatch<'a> {
    head_final: Vec<([u8; 32], ShHeadValue, KeyHome)>,
    stages: BTreeMap<usize, BodyStage<'a>>,
    moved_heads: Vec<MovedHead>,
    written: usize,
}

/// A head file whose bare in-place update must be synced before a slab its
/// key replaced is freed. An ingest head needs none: `insert_many` syncs it,
/// and a seal writes a synced sorted file.
#[derive(PartialEq)]
enum MovedHead {
    MainShard(usize),
    SealedOvf,
}

/// Body slab allocator state (persisted in alloc header).
#[derive(Clone, Copy)]
#[doc(hidden)]
pub struct AllocState {
    pub live_count: u64,
    pub bump: u64,
    pub free_head: [u64; SH_MAX_CLASS as usize + 1],
}

#[doc(hidden)]
pub fn largest_reloc_class_le(bytes: u64) -> Option<u8> {
    (0..=SH_MAX_SLAB_CLASS)
        .rev()
        .find(|&c| slab_bytes(c) <= bytes)
}

#[doc(hidden)]
pub fn push_free_head(
    body: &TableFile,
    free_head: &mut [u64; SH_MAX_CLASS as usize + 1],
    class: u8,
    off: u64,
) -> Result<(), StoreError> {
    let idx = class as usize;
    if idx >= free_head.len() {
        return Err(StoreError::Corrupt("scripthash free class overflow"));
    }
    let next = free_head[idx].to_le_bytes();
    body.write_at(off, &next)?;
    free_head[idx] = off;
    Ok(())
}

#[doc(hidden)]
pub fn pop_free_head(
    body: &TableFile,
    free_head: &mut [u64; SH_MAX_CLASS as usize + 1],
    class: u8,
    bump: u64,
) -> Result<Option<u64>, StoreError> {
    let idx = class as usize;
    if idx >= free_head.len() || free_head[idx] == 0 {
        return Ok(None);
    }
    let off = free_head[idx];
    let size = slab_bytes(class);
    if off < SH_PREFIX_PAGE as u64 || off.saturating_add(size) > bump {
        return Err(StoreError::Corrupt("scripthash free head out of range"));
    }
    let mut next = [0u8; 8];
    body.read_at(off, &mut next)?;
    let next = u64::from_le_bytes(next);
    if next != 0 && (next < SH_PREFIX_PAGE as u64 || next.saturating_add(size) > bump) {
        return Err(StoreError::Corrupt("scripthash free link out of range"));
    }
    free_head[idx] = next;
    Ok(Some(off))
}

#[doc(hidden)]
pub fn carve_gap_into_freelist(
    body: &TableFile,
    free_head: &mut [u64; SH_MAX_CLASS as usize + 1],
    from: u64,
    to: u64,
) -> Result<(), StoreError> {
    for (c, off) in gap_chunks(from, to) {
        push_free_head(body, free_head, c, off)?;
    }
    Ok(())
}

/// Free slabs that cover `[from, to)`, largest class first.
#[doc(hidden)]
pub fn gap_chunks(from: u64, to: u64) -> Vec<(u8, u64)> {
    let mut out = Vec::new();
    let mut off = from;
    while off < to {
        let Some(c) = largest_reloc_class_le(to - off) else {
            break;
        };
        out.push((c, off));
        off = off.saturating_add(slab_bytes(c));
    }
    out
}

/// Publish the allocator bump as the body length. An armed commit calls
/// this before its writes, so page 0 is imaged first.
pub(crate) fn publish_bump(body: &TableFile, alloc: &AllocState) -> Result<(), StoreError> {
    if alloc.bump > body.logical_len() {
        body.set_logical_len(alloc.bump)?;
    }
    Ok(())
}

pub struct ScriptHashTable {
    pub store_dir: PathBuf,
    pub layout: ShBodyLayout,
    pub bodies: Vec<TableFile>,
    pub ovf_body: Option<TableFile>,
    pub n_shards: usize,
    pub scale: HeadScale,
    /// Sealed MPHF main shards (set when a cold bulk shard is installed).
    /// Per-shard slot so Electrum `get` does not take a process-wide mutex.
    pub sorted_main: Box<[RwLock<Option<MphfHead>>]>,
    /// Global ingest OA (`scripthash.ovf/ingest`). Absent until the first insert
    /// and after a seal drains it. Missing means known empty: no scan, no walk.
    pub ingest: Mutex<Option<ScriptHashHead>>,
    /// L0 sealed global ovf (`SHSR`+fuse), newest last.
    pub sealed_ovf: Mutex<Vec<SortedHead>>,
    /// L1 promoted ovf (at most one MPHF+fuse per rematerialize lifetime).
    pub ovf_l1: Mutex<Option<OvfL1>>,
    pub l1_frozen_warned: AtomicBool,
    /// At least one sealed sorted main shard is installed.
    pub sorted_main_on: std::sync::atomic::AtomicBool,
    /// Pack commit per shard. True only after `scripthash.head/NN.packed` is durable.
    pub pack_marked: Box<[AtomicBool]>,
    /// One alloc per `bodies` entry (Shared: len 1).
    pub allocs: Vec<Mutex<AllocState>>,
    /// Dir-variant ovf alloc. Shared: `None` (ovf uses `allocs[0]`).
    ovf_alloc: Option<Mutex<AllocState>>,
    /// Full-list unlinks whose empty head waits for the tip shrink.
    pending_empty: Mutex<Vec<unlink::PendingEmpty>>,
    /// [`SH_DIRTY_NAME`] for this table.
    dirty: ShDirty,
}

#[doc(hidden)]
pub struct OvfL1 {
    pub head: MphfHead,
    pub fuse: SealedFuse8,
}

#[doc(hidden)]
pub const SH_L1_FROZEN_WARN: &str =
    "scripthash ovf L1 MPHF is frozen; wipe store/scripthash* and rematerialize (--sh-index)";

/// How `scripthash.body` is oriented on disk (schema 17 variant).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ShBodyLayout {
    /// `scripthash.body/NN` + `scripthash.ovf/body`.
    Sharded,
}

#[doc(hidden)]
pub fn sh_body_path(dir: &Path) -> PathBuf {
    dir.join("scripthash.body")
}

#[doc(hidden)]
pub fn sh_ovf_body_path(dir: &Path) -> PathBuf {
    dir.join("scripthash.ovf").join("body")
}

#[doc(hidden)]
pub fn sh_shard_body_path(dir: &Path, shard: usize) -> PathBuf {
    sh_body_path(dir).join(format!("{shard:02x}"))
}

#[doc(hidden)]
pub fn sh_set_body_grow(body: &TableFile) {
    body.set_grow_policy(GrowPolicy::Align64k);
}

#[doc(hidden)]
pub fn sh_body_layout_wipe_msg() -> String {
    "scripthash body layout mixed or incomplete; wipe store/scripthash* (head, body, ovf, \
     runs, include_hwm, cold_progress) and rematerialize"
        .into()
}

/// Leftover single-file `scripthash.body` (schema 17 Shared).
pub const INDEX_REFUSE_SHARED_SH_BODY: &str = "index refuses Shared (file) scripthash.body; wipe store/scripthash* then restart (Class A kept; SH rematerializes with --sh-index)";

/// Detect directory SH body. File (Shared) leftovers refuse.
pub fn detect_sh_body_layout(dir: &Path) -> Result<ShBodyLayout, StoreError> {
    let body = sh_body_path(dir);
    let ovf = sh_ovf_body_path(dir);
    match (body.is_file(), body.is_dir()) {
        (true, false) => Err(StoreError::Corrupt(INDEX_REFUSE_SHARED_SH_BODY)),
        (false, true) if ovf.is_file() => Ok(ShBodyLayout::Sharded),
        (false, true) => Err(StoreError::Layout(sh_body_layout_wipe_msg())),
        (false, false) => Err(StoreError::Layout(sh_body_layout_wipe_msg())),
        (true, true) => Err(StoreError::Layout(sh_body_layout_wipe_msg())),
    }
}

#[doc(hidden)]
pub fn init_empty_body(body: &TableFile) -> Result<AllocState, StoreError> {
    let payload0 = payload_start(FILE_HEADER_LEN);
    body.ensure_capacity(payload0)?;
    body.set_logical_len(payload0)?;
    let state = AllocState {
        live_count: 0,
        bump: payload0,
        free_head: [0; SH_MAX_CLASS as usize + 1],
    };
    write_alloc_header(body, &state)?;
    Ok(state)
}

#[doc(hidden)]
pub fn ingest_path(dir: &Path) -> PathBuf {
    dir.join("scripthash.ovf").join("ingest")
}

#[doc(hidden)]
pub fn ingest_oa_slots(scale: HeadScale) -> u64 {
    scale.ingest_oa_slots()
}

#[doc(hidden)]
pub fn sealed_ovf_path(dir: &Path, id: u32) -> PathBuf {
    dir.join("scripthash.ovf").join(format!("{id:06}"))
}

/// `Ok(true)` is `SHSR`. A short file is not. Any other read error fails the caller.
fn read_shsr_magic(path: &Path) -> Result<bool, StoreError> {
    let mut f = std::fs::File::open(path).map_err(|e| StoreError::io(path, e))?;
    let mut magic = [0u8; 4];
    use std::io::Read;
    match f.read_exact(&mut magic) {
        Ok(()) => Ok(magic == *crate::scripthash_sorted_head::DATA_MAGIC),
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => Ok(false),
        Err(e) => Err(StoreError::io(path, e)),
    }
}

fn six_digit_name(name: &str) -> Option<u32> {
    if name.len() == 6 && name.chars().all(|c| c.is_ascii_digit()) {
        name.parse().ok()
    } else {
        None
    }
}

/// Next L0 id is one past the highest six-digit name, including a hole.
fn next_sealed_ovf_id(dir: &Path) -> Result<u32, StoreError> {
    let ovf = dir.join("scripthash.ovf");
    let rd = match std::fs::read_dir(&ovf) {
        Ok(rd) => rd,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(0),
        Err(e) => return Err(StoreError::io(&ovf, e)),
    };
    let mut max_id: Option<u32> = None;
    for ent in rd {
        let ent = ent.map_err(|e| StoreError::io(&ovf, e))?;
        let name = ent.file_name();
        let Some(id) = six_digit_name(&name.to_string_lossy()) else {
            continue;
        };
        max_id = Some(max_id.map_or(id, |cur| cur.max(id)));
    }
    Ok(max_id.map_or(0, |id| id.saturating_add(1)))
}

pub fn sorted_main_shard_path(dir: &Path, shard: usize, n_shards: usize) -> PathBuf {
    let p = dir.join("scripthash.head");
    if n_shards <= 1 && p.is_file() {
        p
    } else {
        p.join(format!("{shard:02x}"))
    }
}

/// Pack commit sidecar. Pass-1 BDZ writes `.mphf`+`.val` and is not this file.
pub fn shard_pack_mark_path(base: &Path) -> PathBuf {
    let mut s = base.as_os_str().to_os_string();
    s.push(".packed");
    PathBuf::from(s)
}

#[doc(hidden)]
pub fn write_shard_pack_mark(base: &Path) -> Result<(), StoreError> {
    crate::file::write_synced_tmp_rename(&shard_pack_mark_path(base), &[1u8])
}

#[doc(hidden)]
pub fn unlink_shard_pack_mark(base: &Path) {
    let p = shard_pack_mark_path(base);
    let _ = std::fs::remove_file(&p);
    let mut tmp_s = p.as_os_str().to_os_string();
    tmp_s.push(".tmp");
    let _ = std::fs::remove_file(PathBuf::from(tmp_s));
}

/// `scripthash.unsorted` still has a pass-1 or pass-2 phase. Matches
/// `UNSORTED_SHARD_DIR` in the materialize module (that module depends on this one).
#[doc(hidden)]
pub fn sh_unsorted_extract_in_progress(dir: &Path) -> bool {
    let u = dir.join("scripthash.unsorted");
    if !u.is_dir() {
        return false;
    }
    if u.join("DONE.keys").is_file() || u.join("DONE.post").is_file() {
        return true;
    }
    u.join("keys").exists() || u.join("post").exists()
}

/// Complete pre-marker heads (unsorted gone, every shard has MPHF+val) get
/// `.packed`. An extract still on disk does not, even when `include_hwm` is at the tip.
#[doc(hidden)]
pub fn migrate_legacy_pack_marks(
    dir: &Path,
    n_shards: usize,
    create_count: u64,
) -> Result<(), StoreError> {
    if sh_unsorted_extract_in_progress(dir) {
        return Ok(());
    }
    let n = n_shards.max(1);
    let bases: Vec<PathBuf> = (0..n).map(|i| sorted_main_shard_path(dir, i, n)).collect();
    if !bases.iter().all(|b| MphfHead::exists(b)) {
        return Ok(());
    }
    let mut wrote = false;
    for b in &bases {
        if shard_pack_mark_path(b).is_file() {
            continue;
        }
        write_shard_pack_mark(b)?;
        wrote = true;
    }
    if wrote && load_include_hwm(dir) == 0 && create_count > 0 {
        store_include_hwm(dir, create_count)?;
    }
    Ok(())
}

#[doc(hidden)]
pub fn open_sorted_main_shards(
    dir: &Path,
    n_shards: usize,
) -> Result<Vec<Option<MphfHead>>, StoreError> {
    let n = n_shards.max(1);
    let mut out = Vec::with_capacity(n);
    for i in 0..n {
        let p = sorted_main_shard_path(dir, i, n);
        // Pass-1 BDZ exists without `.packed`. Leave the slot unsealed so pass 2
        // can `MphfHead::open` by path.
        if MphfHead::exists(&p) && shard_pack_mark_path(&p).is_file() {
            out.push(Some(MphfHead::open(&p)?));
        } else {
            out.push(None);
        }
    }
    Ok(out)
}

#[doc(hidden)]
pub fn wrap_sorted_slots(v: Vec<Option<MphfHead>>) -> Box<[RwLock<Option<MphfHead>>]> {
    v.into_iter().map(RwLock::new).collect()
}

#[doc(hidden)]
pub fn open_ovf_l1(dir: &Path) -> Result<Option<OvfL1>, StoreError> {
    let ovf = dir.join("scripthash.ovf");
    if !ovf.is_dir() {
        return Ok(None);
    }
    let mut ids: Vec<u32> = std::fs::read_dir(&ovf)
        .map_err(|e| StoreError::io(&ovf, e))?
        .filter_map(|e| e.ok())
        .filter_map(|e| {
            let name = e.file_name().into_string().ok()?;
            name.strip_suffix(".mphf")?
                .parse::<u32>()
                .ok()
                .filter(|_| name.len() == 6 + ".mphf".len())
        })
        .collect();
    ids.sort_unstable();
    let Some(&id) = ids.last() else {
        return Ok(None);
    };
    let base = sealed_ovf_path(dir, id);
    if !MphfHead::exists(&base) {
        return Ok(None);
    }
    let head = MphfHead::open(&base)?;
    let fuse = SealedFuse8::read_from(&{
        let mut p = base.as_os_str().to_os_string();
        p.push(".fuse8");
        PathBuf::from(p)
    })?;
    Ok(Some(OvfL1 { head, fuse }))
}

#[doc(hidden)]
pub fn open_sealed_sorted_ovf(dir: &Path) -> Result<Vec<SortedHead>, StoreError> {
    let ovf = dir.join("scripthash.ovf");
    if !ovf.is_dir() {
        return Ok(Vec::new());
    }
    let mut ids: Vec<u32> = std::fs::read_dir(&ovf)
        .map_err(|e| StoreError::io(&ovf, e))?
        .filter_map(|e| e.ok())
        .filter(|e| e.file_type().map(|t| t.is_file()).unwrap_or(false))
        .filter_map(|e| {
            let name = e.file_name().to_string_lossy().into_owned();
            if name.len() == 6 && name.chars().all(|c| c.is_ascii_digit()) {
                name.parse::<u32>().ok()
            } else {
                None
            }
        })
        .collect();
    ids.sort_unstable();
    ids.dedup();
    let mut out = Vec::new();
    for id in ids {
        let p = sealed_ovf_path(dir, id);
        if read_shsr_magic(&p)? {
            out.push(SortedHead::open(p)?);
        }
    }
    Ok(out)
}

#[doc(hidden)]
/// Open an ingest left by an earlier insert. A missing file, and a large file
/// whose sidecar is 0 and whose slots are empty, are both `None`.
pub fn open_ingest(dir: &Path) -> Result<Option<ScriptHashHead>, StoreError> {
    let p = ingest_path(dir);
    if let Some(parent) = p.parent() {
        std::fs::create_dir_all(parent).map_err(|e| StoreError::io(parent, e))?;
    }
    ScriptHashHead::open(p)
}

#[doc(hidden)]
pub fn sorted_main_present(dir: &Path, n_shards: usize) -> bool {
    let n = n_shards.max(1);
    (0..n).any(|i| MphfHead::exists(&sorted_main_shard_path(dir, i, n)))
}

#[doc(hidden)]
pub fn discard_incomplete_scripthash_head(dir: &Path, n_shards: usize) -> Result<(), StoreError> {
    if sorted_main_present(dir, n_shards) {
        return Ok(());
    }
    page_guard::clear_sh_marks(dir)?;
    remove_store_path(&dir.join("scripthash.head"))
}

#[doc(hidden)]
pub fn discard_leftover_oa_overflow(dir: &Path) -> Result<(), StoreError> {
    let ovf = dir.join("scripthash.ovf");
    let Ok(rd) = std::fs::read_dir(&ovf) else {
        return Ok(());
    };
    for ent in rd {
        let ent = ent.map_err(|e| StoreError::io(&ovf, e))?;
        let name = ent.file_name();
        let name = name.to_string_lossy();
        let path = ent.path();
        if six_digit_name(&name).is_some() && path.is_file() && !read_shsr_magic(&path)? {
            std::fs::remove_file(&path).map_err(|e| StoreError::io(&path, e))?;
        }
    }
    Ok(())
}

fn remove_store_path(path: &Path) -> Result<(), StoreError> {
    if path.is_dir() {
        std::fs::remove_dir_all(path).map_err(|e| StoreError::io(path, e))?;
    } else if path.exists() {
        std::fs::remove_file(path).map_err(|e| StoreError::io(path, e))?;
    }
    Ok(())
}

#[doc(hidden)]
pub fn unlink_leftover_oa_stub(dir: &Path) {
    let stub = dir.join("scripthash.head.oa_stub");
    if stub.is_dir() {
        let _ = std::fs::remove_dir_all(&stub);
    } else if stub.is_file() {
        let _ = std::fs::remove_file(&stub);
    }
}

#[doc(hidden)]
pub fn sharded_body_n_shards(dir: &Path) -> Result<usize, StoreError> {
    let body = sh_body_path(dir);
    let mut names: Vec<String> = std::fs::read_dir(&body)
        .map_err(|e| StoreError::io(&body, e))?
        .filter_map(|e| e.ok())
        .filter(|e| e.file_type().map(|t| t.is_file()).unwrap_or(false))
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .filter(|n| n.len() == 2 && n.chars().all(|c| c.is_ascii_hexdigit()))
        .collect();
    names.sort();
    if names.is_empty() {
        return Err(StoreError::Layout(sh_body_layout_wipe_msg()));
    }
    for (i, n) in names.iter().enumerate() {
        if *n != format!("{i:02x}") {
            return Err(StoreError::Layout(sh_body_layout_wipe_msg()));
        }
    }
    Ok(names.len())
}

#[doc(hidden)]
pub fn paged_first_from_last(body: &TableFile, last_page: u64) -> Result<u64, StoreError> {
    if last_page == 0 {
        return Ok(0);
    }
    let mut page = [0u8; SH_PAGE_SIZE];
    body.read_at(last_page, &mut page)?;
    let page = sh_page_as_array(&page)?;
    if sh_page_extent(page)?.is_none() {
        return Err(StoreError::Corrupt("scripthash extent last page not ver=2"));
    }
    sh_page_first_off(page)
}

/// Cap on a contiguous megakey span pread (64 MiB ≈ 16k pages).
#[doc(hidden)]
pub const SH_PAGE_SPAN_MAX: u64 = 64 * 1024 * 1024;

#[doc(hidden)]
pub fn read_sh_page_bytes(body: &TableFile, off: u64, buf: &mut [u8]) -> Result<(), StoreError> {
    if buf.is_empty() {
        return Ok(());
    }
    match crate::io_backend::read_io_backend() {
        ReadIoBackend::Pread => body.pread_at(off, buf),
        ReadIoBackend::Uring => {
            let end = off.saturating_add(buf.len() as u64);
            if end > body.logical_len() {
                return Err(StoreError::Corrupt("pread past logical end"));
            }
            use crate::bulk_io::{self, ReadOp};
            let fd = body.read_fd();
            let mut ops = [ReadOp {
                fd,
                offset: off,
                buf,
                result: i32::MIN,
            }];
            bulk_io::pread_batch(&mut ops);
            if ops[0].result < 0 {
                return Err(StoreError::io(
                    body.path(),
                    std::io::Error::from_raw_os_error(-ops[0].result),
                ));
            }
            if ops[0].result as usize != ops[0].buf.len() {
                return Err(StoreError::Corrupt("scripthash page read short"));
            }
            Ok(())
        }
    }
}

#[doc(hidden)]
pub fn extend_page_entries(
    out: &[Fk],
    prev_last: &mut Option<u64>,
    start: usize,
) -> Result<(), StoreError> {
    if start < out.len() {
        if let Some(pl) = *prev_last {
            if out[start].0 <= pl {
                return Err(StoreError::Corrupt(
                    "invariant: scripthash page chain create_fks not strictly increasing",
                ));
            }
        }
        *prev_last = Some(out[out.len() - 1].0);
    }
    Ok(())
}

#[doc(hidden)]
pub fn collect_page_chain_span(
    body: &TableFile,
    first_page: u64,
    n_pages: usize,
) -> Result<Option<(Vec<Fk>, u64)>, StoreError> {
    let mut buf = vec![0u8; n_pages.saturating_mul(SH_PAGE_SIZE)];
    read_sh_page_bytes(body, first_page, &mut buf)?;
    let mut out = Vec::new();
    let mut prev_last = None;
    let mut last_next = 0u64;
    for i in 0..n_pages {
        let start = i.saturating_mul(SH_PAGE_SIZE);
        let page = &buf[start..start.saturating_add(SH_PAGE_SIZE)];
        let at = out.len();
        let next = sh_page_decode_slice_into(page, &mut out)?;
        if i + 1 == n_pages {
            last_next = next;
        } else {
            let expect = first_page.saturating_add(((i + 1) * SH_PAGE_SIZE) as u64);
            if next != expect {
                return Ok(None);
            }
        }
        extend_page_entries(&out, &mut prev_last, at)?;
    }
    Ok(Some((out, last_next)))
}

/// Same stop as `hashhead` multi-list walks. A two-page cycle never returns otherwise.
#[doc(hidden)]
pub const SH_PAGE_CHAIN_MAX: u32 = 1_000_000;

/// Follow `next` and visit each page. `page_cap` stops a cycle.
#[doc(hidden)]
pub fn follow_page_chain(
    body: &TableFile,
    mut off: u64,
    page_cap: u32,
    mut visit: impl FnMut(&TableFile, u64) -> Result<(), StoreError>,
) -> Result<(), StoreError> {
    let mut seen = HashSet::new();
    let mut guard = 0u32;
    while off != 0 {
        if !seen.insert(off) {
            return Err(StoreError::Corrupt("scripthash page chain cycle"));
        }
        guard = guard.saturating_add(1);
        if guard > page_cap {
            return Err(StoreError::Corrupt("scripthash page chain cycle"));
        }
        let mut page = [0u8; SH_PAGE_SIZE];
        body.read_at(off, &mut page)?;
        let next = sh_page_next(&page)?;
        visit(body, off)?;
        off = next;
    }
    Ok(())
}

#[doc(hidden)]
pub fn collect_linked_pages(
    body: &TableFile,
    first_page: u64,
    out: &mut Vec<Fk>,
    prev_last: &mut Option<u64>,
    page_cap: u32,
) -> Result<(), StoreError> {
    if first_page == 0 {
        return Ok(());
    }
    let mut cur = [0u8; SH_PAGE_SIZE];
    let mut nxt = [0u8; SH_PAGE_SIZE];
    read_sh_page_bytes(body, first_page, &mut cur)?;
    let mut guard = 0u32;
    loop {
        guard = guard.saturating_add(1);
        if guard > page_cap {
            return Err(StoreError::Corrupt("scripthash page chain cycle"));
        }
        let at = out.len();
        let next = sh_page_decode_slice_into(&cur, out)?;
        extend_page_entries(out, prev_last, at)?;
        if next == 0 {
            break;
        }
        read_sh_page_bytes(body, next, &mut nxt)?;
        cur = nxt;
    }
    Ok(())
}

#[doc(hidden)]
pub fn collect_page_chain_linked(body: &TableFile, first_page: u64) -> Result<Vec<Fk>, StoreError> {
    let mut out = Vec::new();
    let mut prev_last = None;
    collect_linked_pages(
        body,
        first_page,
        &mut out,
        &mut prev_last,
        SH_PAGE_CHAIN_MAX,
    )?;
    Ok(out)
}

#[doc(hidden)]
pub fn collect_extent_then_tail(body: &TableFile, last_page: u64) -> Result<Vec<Fk>, StoreError> {
    if last_page == 0 {
        return Err(StoreError::Corrupt("scripthash extent: null last_page"));
    }
    let mut last_buf = [0u8; SH_PAGE_SIZE];
    read_sh_page_bytes(body, last_page, &mut last_buf)?;
    let last_arr = sh_page_as_array(&last_buf)?;
    let Some((base, n)) = sh_page_extent(last_arr)? else {
        return Err(StoreError::Corrupt("scripthash extent last page not ver=2"));
    };
    let n = n as usize;
    let bytes = (n as u64).saturating_mul(SH_PAGE_SIZE as u64);
    if n == 0 || bytes > SH_PAGE_SPAN_MAX || base.saturating_add(bytes) > body.logical_len() {
        // `extent_n` is a bulk-pread hint. IBD megakeys can exceed 64 MiB;
        // DisconnectTip unlink still walks `next` from first_off.
        let first = sh_page_first_off(last_arr)?;
        if first == 0 {
            return Err(StoreError::Corrupt("scripthash extent span overflow"));
        }
        return collect_page_chain_linked(body, first);
    }
    let last_in_ext = base.saturating_add(((n - 1) as u64).saturating_mul(SH_PAGE_SIZE as u64));
    let (mut out, tail_off) = collect_page_chain_span(body, base, n)?.ok_or(
        StoreError::Corrupt("scripthash extent prefix next links broken"),
    )?;
    if last_page == last_in_ext {
        return Ok(out);
    }
    if tail_off == 0 {
        return Err(StoreError::Corrupt(
            "scripthash extent last_page beyond extent with no tail",
        ));
    }
    let mut prev_last = out.last().map(|fk| fk.0);
    collect_linked_pages(body, tail_off, &mut out, &mut prev_last, SH_PAGE_CHAIN_MAX)?;
    Ok(out)
}

/// Where a scripthash key lives for head upsert routing.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[doc(hidden)]
pub enum KeyHome {
    /// Present on sealed sorted main.
    Main,
    /// Present on a sealed sorted global ovf file.
    SealedOvf,
    /// Present on the global ingest OA.
    Ingest,
    /// Not yet in either head.
    Absent,
}

/// A key [`ScriptHashTable::locate_head`] actually found. Absent is not a home
/// on that path: a miss is `Ok(None)`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[doc(hidden)]
pub enum LocatedHome {
    Main,
    SealedOvf,
    Ingest,
}

impl From<LocatedHome> for KeyHome {
    fn from(home: LocatedHome) -> Self {
        match home {
            LocatedHome::Main => Self::Main,
            LocatedHome::SealedOvf => Self::SealedOvf,
            LocatedHome::Ingest => Self::Ingest,
        }
    }
}

/// `seal_ingest` moved every ingest row into a sealed file. Keys still
/// tagged `Ingest` in the batch snapshot would also land in the new table.
fn retarget_ingest_after_seal(heads: &mut HashMap<[u8; 32], (ShHeadValue, KeyHome)>) {
    for (_, home) in heads.values_mut() {
        if *home == KeyHome::Ingest {
            *home = KeyHome::SealedOvf;
        }
    }
}

/// Remember the head file `apply_head_upserts` updates in place for `key`,
/// once per main shard or for the sealed overflow files.
fn note_moved_head(
    table: &ScriptHashTable,
    heads: &mut Vec<MovedHead>,
    key: &[u8; 32],
    kh: KeyHome,
) {
    let head = match kh {
        KeyHome::Main => MovedHead::MainShard(table.shard_index(key)),
        KeyHome::SealedOvf => MovedHead::SealedOvf,
        KeyHome::Ingest | KeyHome::Absent => return,
    };
    if !heads.contains(&head) {
        heads.push(head);
    }
}

impl ScriptHashTable {
    pub fn create(dir: &std::path::Path) -> Result<Self, StoreError> {
        Self::create_with_scale(dir, HeadScale::Mainnet)
    }

    pub fn create_tiny(dir: &std::path::Path) -> Result<Self, StoreError> {
        Self::create_with_scale(dir, HeadScale::Tiny)
    }

    pub fn create_with_scale(dir: &std::path::Path, scale: HeadScale) -> Result<Self, StoreError> {
        let n_shards = sh_main_shard_count(scale).max(1);
        let body_dir = sh_body_path(dir);
        std::fs::create_dir_all(&body_dir).map_err(|e| StoreError::io(&body_dir, e))?;
        let mut bodies = Vec::with_capacity(n_shards);
        let mut allocs = Vec::with_capacity(n_shards);
        for i in 0..n_shards {
            let f = TableFile::create(sh_shard_body_path(dir, i), TableKind::ScriptHash)?;
            sh_set_body_grow(&f);
            let st = init_empty_body(&f)?;
            bodies.push(f);
            allocs.push(Mutex::new(st));
        }
        let ovf_dir = dir.join("scripthash.ovf");
        std::fs::create_dir_all(&ovf_dir).map_err(|e| StoreError::io(&ovf_dir, e))?;
        let ovf = TableFile::create(sh_ovf_body_path(dir), TableKind::ScriptHash)?;
        sh_set_body_grow(&ovf);
        let ovf_st = init_empty_body(&ovf)?;
        Ok(Self {
            store_dir: dir.to_path_buf(),
            layout: ShBodyLayout::Sharded,
            bodies,
            ovf_body: Some(ovf),
            n_shards,
            scale,
            sorted_main: wrap_sorted_slots((0..n_shards).map(|_| None).collect()),
            ingest: Mutex::new(open_ingest(dir)?),
            sealed_ovf: Mutex::new(Vec::new()),
            ovf_l1: Mutex::new(None),
            l1_frozen_warned: AtomicBool::new(false),
            sorted_main_on: std::sync::atomic::AtomicBool::new(false),
            pack_marked: std::iter::repeat_with(|| AtomicBool::new(false))
                .take(n_shards)
                .collect::<Vec<_>>()
                .into_boxed_slice(),
            allocs,
            ovf_alloc: Some(Mutex::new(ovf_st)),
            pending_empty: Mutex::new(Vec::new()),
            dirty: ShDirty::open(dir),
        })
    }

    pub fn open(dir: &std::path::Path) -> Result<Self, StoreError> {
        Self::open_with_scale(dir, HeadScale::Mainnet)
    }

    pub fn open_tiny(dir: &std::path::Path) -> Result<Self, StoreError> {
        Self::open_with_scale(dir, HeadScale::Tiny)
    }

    pub fn open_with_scale(dir: &std::path::Path, scale: HeadScale) -> Result<Self, StoreError> {
        page_guard::restore_sh_marks(dir)?;
        let expected = sh_main_shard_count(scale);
        discard_incomplete_scripthash_head(dir, expected)?;
        discard_leftover_oa_overflow(dir)?;
        let layout = detect_sh_body_layout(dir)?;
        unlink_leftover_oa_stub(dir);
        let n_shards = sharded_body_n_shards(dir)?;
        Self::from_layout_and_n_shards(dir, layout, n_shards, scale)
    }

    #[doc(hidden)]
    pub fn from_layout_and_n_shards(
        dir: &Path,
        layout: ShBodyLayout,
        n_shards: usize,
        scale: HeadScale,
    ) -> Result<Self, StoreError> {
        page_guard::restore_sh_marks(dir)?;
        let n_shards = n_shards.max(1);
        let mut bodies = Vec::with_capacity(n_shards);
        let mut allocs = Vec::with_capacity(n_shards);
        let mut alloc_ver = SH_ALLOC_VERSION;
        for i in 0..n_shards {
            let f = TableFile::open(sh_shard_body_path(dir, i), TableKind::ScriptHash)?;
            sh_set_body_grow(&f);
            let (state, v) = read_alloc_header(&f)?;
            if i == 0 {
                alloc_ver = v;
            }
            bodies.push(f);
            allocs.push(Mutex::new(state));
        }
        let ovf = TableFile::open(sh_ovf_body_path(dir), TableKind::ScriptHash)?;
        sh_set_body_grow(&ovf);
        let (ost, _) = read_alloc_header(&ovf)?;
        let ovf_body = Some(ovf);
        let ovf_alloc = Some(Mutex::new(ost));
        wipe_legacy_fullsize_overflow(dir)?;
        let mut create_count = 0u64;
        for a in &allocs {
            create_count = create_count.saturating_add(a.lock().unwrap().live_count);
        }
        create_count =
            create_count.saturating_add(ovf_alloc.as_ref().unwrap().lock().unwrap().live_count);
        migrate_legacy_pack_marks(dir, n_shards, create_count)?;
        let sorted_main = open_sorted_main_shards(dir, n_shards)?;
        let sealed_ovf = open_sealed_sorted_ovf(dir)?;
        let ovf_l1 = open_ovf_l1(dir)?;
        let sorted_on = sorted_main.iter().any(|s| s.is_some());
        let pack_marked: Box<[AtomicBool]> = sorted_main
            .iter()
            .map(|s| AtomicBool::new(s.is_some()))
            .collect::<Vec<_>>()
            .into_boxed_slice();
        let table = Self {
            store_dir: dir.to_path_buf(),
            layout,
            bodies,
            ovf_body,
            n_shards,
            scale,
            sorted_main: wrap_sorted_slots(sorted_main),
            ingest: Mutex::new(open_ingest(dir)?),
            sealed_ovf: Mutex::new(sealed_ovf),
            ovf_l1: Mutex::new(ovf_l1),
            l1_frozen_warned: AtomicBool::new(false),
            sorted_main_on: std::sync::atomic::AtomicBool::new(sorted_on),
            pack_marked,
            allocs,
            ovf_alloc,
            pending_empty: Mutex::new(Vec::new()),
            dirty: ShDirty::open(dir),
        };
        // v1 = schema-13 slabs; v2 = schema-14 pages; v3 = schema-15 slabs.
        // Field layout is the same; only an empty older header upgrades silently.
        if alloc_ver != SH_ALLOC_VERSION {
            if table.has_index_occupancy() {
                return Err(StoreError::Corrupt(
                    "scripthash alloc is a pre-schema-15 body; wipe store/scripthash* (head, body, ovf, runs, include_hwm, cold_progress) and rematerialize",
                ));
            }
            table.reset_all_bodies()?;
        }
        table.recover_unlink_undos()?;
        Ok(table)
    }

    #[doc(hidden)]
    pub fn body(&self) -> &TableFile {
        &self.bodies[0]
    }

    #[doc(hidden)]
    pub fn ovf_file(&self) -> &TableFile {
        self.ovf_body.as_ref().unwrap_or(&self.bodies[0])
    }

    #[inline]
    #[doc(hidden)]
    pub fn shard_index(&self, full: &[u8; 32]) -> usize {
        prefix_shard_of(full, self.n_shards)
    }

    #[doc(hidden)]
    pub fn shard_body(&self, si: usize) -> &TableFile {
        &self.bodies[si]
    }

    #[doc(hidden)]
    pub fn shard_alloc(&self, si: usize) -> &Mutex<AllocState> {
        &self.allocs[si]
    }

    #[doc(hidden)]
    pub fn ovf_alloc_mutex(&self) -> &Mutex<AllocState> {
        self.ovf_alloc.as_ref().unwrap_or(&self.allocs[0])
    }

    #[doc(hidden)]
    pub fn body_for(&self, key: &[u8; 32], home: KeyHome) -> &TableFile {
        match home {
            KeyHome::Main => self.shard_body(self.shard_index(key)),
            KeyHome::Ingest | KeyHome::SealedOvf | KeyHome::Absent => self.ovf_file(),
        }
    }

    #[doc(hidden)]
    pub fn alloc_for(&self, key: &[u8; 32], home: KeyHome) -> &Mutex<AllocState> {
        match home {
            KeyHome::Main => self.shard_alloc(self.shard_index(key)),
            KeyHome::Ingest | KeyHome::SealedOvf | KeyHome::Absent => self.ovf_alloc_mutex(),
        }
    }

    #[doc(hidden)]
    pub fn reset_all_bodies(&self) -> Result<(), StoreError> {
        let payload0 = payload_start(FILE_HEADER_LEN);
        let empty = AllocState {
            live_count: 0,
            bump: payload0,
            free_head: [0; SH_MAX_CLASS as usize + 1],
        };
        page_guard::clear_sh_marks(&self.store_dir)?;
        // The unlink undo record shares the prefix block with the header. An
        // armed one names pages of the old body, so it goes with them.
        for (i, body) in self.bodies.iter().enumerate() {
            *self.allocs[i].lock().unwrap() = empty;
            unlink::persist_undo(body, &empty, None)?;
            body.set_logical_len(payload0)?;
        }
        if let (Some(ovf), Some(oa)) = (&self.ovf_body, &self.ovf_alloc) {
            *oa.lock().unwrap() = empty;
            unlink::persist_undo(ovf, &empty, None)?;
            ovf.set_logical_len(payload0)?;
        }
        Ok(())
    }

    #[doc(hidden)]
    pub fn persist_all_allocs(&self) -> Result<(), StoreError> {
        for (i, body) in self.bodies.iter().enumerate() {
            let g = self.allocs[i].lock().unwrap();
            write_alloc_header(body, &g)?;
        }
        if let (Some(ovf), Some(oa)) = (&self.ovf_body, &self.ovf_alloc) {
            let g = oa.lock().unwrap();
            write_alloc_header(ovf, &g)?;
        }
        Ok(())
    }

    fn sync_bodies_for(&self, keys: &[([u8; 32], ShHeadValue, KeyHome)]) -> Result<(), StoreError> {
        let mut seen = BTreeSet::new();
        for (key, _, kh) in keys {
            let body = self.body_for(key, *kh);
            if seen.insert(body.path().to_path_buf()) {
                body.sync_data_only()?;
            }
        }
        Ok(())
    }

    fn write_alloc_headers_for(
        &self,
        keys: &[([u8; 32], ShHeadValue, KeyHome)],
    ) -> Result<(), StoreError> {
        let mut seen = BTreeSet::new();
        for (key, _, kh) in keys {
            let body = self.body_for(key, *kh);
            if seen.insert(body.path().to_path_buf()) {
                let alloc_mu = self.alloc_for(key, *kh);
                let alloc = alloc_mu.lock().unwrap();
                write_alloc_header(body, &alloc)?;
            }
        }
        Ok(())
    }

    #[doc(hidden)]
    pub fn flush_all_bodies(&self) -> Result<(), StoreError> {
        for b in &self.bodies {
            b.flush()?;
        }
        if let Some(ovf) = &self.ovf_body {
            ovf.flush()?;
        }
        Ok(())
    }

    #[doc(hidden)]
    pub fn flush_all_bodies_async(&self) -> Result<(), StoreError> {
        for b in &self.bodies {
            b.flush_async()?;
        }
        if let Some(ovf) = &self.ovf_body {
            ovf.flush_async()?;
        }
        Ok(())
    }
}

/// Drop leftover `scripthash.runs` names except `SEAL` / `SEAL.tmp`.
///
/// Production no longer writes runs. A missing directory is fine. A sealed
/// SH head is not inspected.
pub fn unlink_scripthash_run_leftovers(store_dir: &Path) -> Result<(), StoreError> {
    let runs = store_dir.join("scripthash.runs");
    if !runs.exists() {
        return Ok(());
    }
    let rd = std::fs::read_dir(&runs).map_err(|e| StoreError::io(&runs, e))?;
    for e in rd {
        let e = e.map_err(|err| StoreError::io(&runs, err))?;
        let name = e.file_name();
        let name = name.to_string_lossy();
        if name == "SEAL" || name == "SEAL.tmp" {
            continue;
        }
        let p = e.path();
        if p.is_dir() {
            std::fs::remove_dir_all(&p).map_err(|err| StoreError::io(&p, err))?;
        } else {
            std::fs::remove_file(&p).map_err(|err| StoreError::io(&p, err))?;
        }
    }
    Ok(())
}

impl ScriptHashTable {
    pub fn entry_count(&self) -> u64 {
        let mut n = 0u64;
        for a in &self.allocs {
            n = n.saturating_add(a.lock().unwrap().live_count);
        }
        if let Some(oa) = &self.ovf_alloc {
            n = n.saturating_add(oa.lock().unwrap().live_count);
        }
        n
    }

    pub fn body_layout(&self) -> ShBodyLayout {
        self.layout
    }

    /// True when sorted main, ingest, and sealed ovf report no occupied keys.
    pub fn head_is_empty(&self) -> bool {
        if self.sorted_main.iter().any(|s| {
            s.read()
                .unwrap()
                .as_ref()
                .map(|h| !h.is_empty())
                .unwrap_or(false)
        }) {
            return false;
        }
        if !self.sealed_ovf.lock().unwrap().is_empty() {
            return false;
        }
        if self.ovf_l1.lock().unwrap().is_some() {
            return false;
        }
        self.ingest
            .lock()
            .unwrap()
            .as_ref()
            .is_none_or(|h| h.is_known_empty())
    }

    /// Wipe body alloc + all head slots for a full cold rematerialize.
    ///
    /// Used when runs/`*.run.mat` still hold the complete create set after a
    /// partial/crashed bulk load. Resets body alloc in place and unlinks
    /// `.packed` so the emptied head is not pack-complete. MPHF files stay
    /// until the next seal overwrites them. Exclusive: no concurrent SH readers/writers.
    ///
    /// Must run whenever claims are about to cold-load, not only when
    /// `entry_count > 0`: crash mid-finish can leave head shards occupied while
    /// the alloc header still says `live_count == 0`.
    pub fn reinit_empty_for_cold_materialize(&self) -> Result<(), StoreError> {
        self.reset_all_bodies()?;
        self.pending_empty.lock().unwrap().clear();
        self.wipe_overflow_heads()?;
        for slot in self.sorted_main.iter() {
            *slot.write().unwrap() = None;
        }
        self.sorted_main_on
            .store(false, std::sync::atomic::Ordering::Release);
        for b in self.pack_marked.iter() {
            b.store(false, Ordering::Release);
        }
        let n = self.n_shards;
        for i in 0..n {
            unlink_shard_pack_mark(&sorted_main_shard_path(&self.store_dir, i, n));
        }
        Ok(())
    }

    /// Drop the ingest OA, every sealed overflow head and the L1 head. Keys
    /// first seen after a pack live there, and they are looked up before main,
    /// so a rebuild that left them would shadow the packed chains. Exclusive:
    /// no concurrent SH readers/writers.
    fn wipe_overflow_heads(&self) -> Result<(), StoreError> {
        self.sealed_ovf.lock().unwrap().clear();
        *self.ovf_l1.lock().unwrap() = None;
        // Everything in `scripthash.ovf/` except the overflow body, which
        // `reset_all_bodies` already emptied: sealed heads, L1, ingest.
        let ovf_dir = self.store_dir.join("scripthash.ovf");
        if let Ok(rd) = std::fs::read_dir(&ovf_dir) {
            for ent in rd.flatten() {
                if ent.file_name() == "body" {
                    continue;
                }
                let p = ent.path();
                let r = if p.is_dir() {
                    std::fs::remove_dir_all(&p)
                } else {
                    std::fs::remove_file(&p)
                };
                r.map_err(|e| StoreError::io(&p, e))?;
            }
        }
        let mut ing = self.ingest.lock().unwrap();
        *ing = None;
        Ok(())
    }

    /// Main shards with no sealed `scripthash.head/NN` yet.
    pub fn unsealed_main_shards(&self) -> Vec<usize> {
        self.sorted_main
            .iter()
            .enumerate()
            .filter_map(|(i, s)| s.read().unwrap().is_none().then_some(i))
            .collect()
    }

    pub fn store_sharded_cold_progress(
        &self,
        keys_written: u64,
        live_count: u64,
    ) -> Result<(), StoreError> {
        let n = self.n_shards;
        let next = self
            .sorted_main
            .iter()
            .position(|s| s.read().unwrap().is_none())
            .unwrap_or(n) as u32;
        ColdProgress {
            next_shard: next,
            body_bump: 0,
            live_count,
            keys_written,
        }
        .store(&self.store_dir)
    }

    /// Store directory containing `scripthash.body` / head (parent of body path).
    pub fn store_dir(&self) -> &Path {
        &self.store_dir
    }

    /// Max create_fk known present in durable SH (see [`load_include_hwm`]).
    pub fn include_hwm(&self) -> u64 {
        load_include_hwm(self.store_dir())
    }

    /// Advance inclusion HWM after successful cold/warm materialize.
    pub fn note_include_hwm(&self, max_create_fk: u64) -> Result<(), StoreError> {
        store_include_hwm(self.store_dir(), max_create_fk)
    }

    /// True when every main shard's pack mark is durable.
    ///
    /// Pass-1 BDZ, ingest creates, and overflow are not a pack-complete head.
    /// Schema refuse uses [`Self::has_index_occupancy`].
    pub fn has_durable_index(&self) -> bool {
        !self.pack_marked.is_empty() && self.pack_marked.iter().all(|b| b.load(Ordering::Acquire))
    }

    /// Live creates, a loaded head, ingest, or sealed overflow. Not pack-complete.
    pub fn has_index_occupancy(&self) -> bool {
        if self.entry_count() > 0 || !self.head_is_empty() {
            return true;
        }
        if self.sorted_main.iter().any(|s| {
            s.read()
                .unwrap()
                .as_ref()
                .map(|h| !h.is_empty())
                .unwrap_or(false)
        }) {
            return true;
        }
        if self
            .ingest
            .lock()
            .unwrap()
            .as_ref()
            .is_some_and(|h| h.occupied() > 0)
        {
            return true;
        }
        if !self.sealed_ovf.lock().unwrap().is_empty() {
            return true;
        }
        if self.ovf_l1.lock().unwrap().is_some() {
            return true;
        }
        false
    }

    pub fn mphf_g_resident_bytes(&self) -> u64 {
        let mut n = 0u64;
        for slot in self.sorted_main.iter() {
            if let Some(h) = slot.read().unwrap_or_else(|e| e.into_inner()).as_ref() {
                n = n.saturating_add(h.g_bytes_resident() as u64);
            }
        }
        if let Some(l1) = self
            .ovf_l1
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .as_ref()
        {
            n = n.saturating_add(l1.head.g_bytes_resident() as u64);
        }
        n
    }

    pub fn mphf_occ_resident_bytes(&self) -> u64 {
        let mut n = 0u64;
        for slot in self.sorted_main.iter() {
            if let Some(h) = slot.read().unwrap_or_else(|e| e.into_inner()).as_ref() {
                n = n.saturating_add(h.occ_bytes_resident() as u64);
            }
        }
        if let Some(l1) = self
            .ovf_l1
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .as_ref()
        {
            n = n.saturating_add(l1.head.occ_bytes_resident() as u64);
        }
        n
    }

    /// Durable head value for a key.
    pub fn head_value(&self, scripthash: &[u8; 32]) -> Result<Option<ShHeadValue>, StoreError> {
        Ok(self.locate_head(scripthash)?.map(|(v, _)| v))
    }

    /// Which head segment holds `scripthash` (if any).
    #[doc(hidden)]
    pub fn key_home(&self, scripthash: &[u8; 32]) -> Result<KeyHome, StoreError> {
        Ok(self
            .locate_head(scripthash)?
            .map(|(_, h)| KeyHome::from(h))
            .unwrap_or(KeyHome::Absent))
    }

    /// Tip-mode probe: overflow first (ingest OA, then sealed ovf fuse), then main.
    ///
    /// Post-seal new keys live only on ingest / sealed ovf. Checking those
    /// (fuse-gated ovf, small OA) avoids a 4 KiB main-page pread on every miss.
    /// Historical keys pay a few RAM fuse checks then one main idx+page.
    /// One walk returns both value and home so seed + route share the pread.
    #[doc(hidden)]
    pub fn locate_head(
        &self,
        scripthash: &[u8; 32],
    ) -> Result<Option<(ShHeadValue, LocatedHome)>, StoreError> {
        if let Some(h) = self.ingest.lock().unwrap().as_ref() {
            if let Some(v) = h.get(scripthash)? {
                return Ok(Some((v, LocatedHome::Ingest)));
            }
        }
        let hk = head_key_from_full(scripthash);
        for h in self.sealed_ovf.lock().unwrap().iter().rev() {
            if let Some(v) = h.get(&hk)? {
                return Ok(Some((v, LocatedHome::SealedOvf)));
            }
        }
        if let Some(l1) = self.ovf_l1.lock().unwrap().as_ref() {
            if l1.fuse.contains(mix_key16(&hk)) {
                if let Some(v) = l1.head.get(&hk)? {
                    return Ok(Some((v, LocatedHome::SealedOvf)));
                }
            }
        }
        if self.has_sorted_main() {
            let si = self.shard_index(scripthash);
            if let Some(slot) = self.sorted_main.get(si) {
                let g = slot.read().unwrap();
                if let Some(h) = g.as_ref() {
                    if let Some(v) = h.get(&hk)? {
                        return Ok(Some((v, LocatedHome::Main)));
                    }
                }
            }
        }
        Ok(None)
    }

    #[doc(hidden)]
    pub fn has_sorted_main(&self) -> bool {
        self.sorted_main_on
            .load(std::sync::atomic::Ordering::Acquire)
    }

    #[doc(hidden)]
    pub fn install_sorted_main(&self, shard: usize, sealed: MphfHead) {
        if let Some(slot) = self.sorted_main.get(shard) {
            *slot.write().unwrap() = Some(sealed);
            self.sorted_main_on
                .store(true, std::sync::atomic::Ordering::Release);
        }
    }

    /// Test-only: set alloc `live_count = 0` without clearing head slots.
    ///
    /// Models crash mid-finish after deferred heads landed but before the
    /// alloc header was updated (entry_count==0, head non-empty). Process-local
    /// fault inject — no production caller; kept for reinit recovery regression.
    #[cfg(debug_assertions)]
    pub fn test_zero_live_count_keep_head(&self) -> Result<(), StoreError> {
        for (i, body) in self.bodies.iter().enumerate() {
            let mut alloc = self.allocs[i].lock().unwrap();
            alloc.live_count = 0;
            write_alloc_header(body, &alloc)?;
        }
        if let (Some(ovf), Some(oa)) = (&self.ovf_body, &self.ovf_alloc) {
            let mut alloc = oa.lock().unwrap();
            alloc.live_count = 0;
            write_alloc_header(ovf, &alloc)?;
        }
        Ok(())
    }

    /// Visit every live create_tx_fk across all keys (main + overflow occupancy walk).
    pub fn for_each_live_create(&self, mut f: impl FnMut(Fk)) -> Result<(), StoreError> {
        {
            for (si, slot) in self.sorted_main.iter().enumerate() {
                let g = slot.read().unwrap();
                let Some(h) = g.as_ref() else {
                    continue;
                };
                let body = &self.bodies[si];
                h.for_each_occupied(|_k, val| {
                    let entries = self.collect_entries_from(body, &val)?;
                    for fk in entries {
                        f(fk);
                    }
                    Ok(())
                })?;
            }
        }
        {
            let g = self.sealed_ovf.lock().unwrap();
            let body = self.ovf_file();
            for h in g.iter() {
                h.for_each_occupied(|_k, val| {
                    let entries = self.collect_entries_from(body, &val)?;
                    for fk in entries {
                        f(fk);
                    }
                    Ok(())
                })?;
            }
        }
        {
            let g = self.ovf_l1.lock().unwrap();
            if let Some(l1) = g.as_ref() {
                let body = self.ovf_file();
                l1.head.for_each_occupied(|_k, val| {
                    let entries = self.collect_entries_from(body, &val)?;
                    for fk in entries {
                        f(fk);
                    }
                    Ok(())
                })?;
            }
        }
        if let Some(h) = self.ingest.lock().unwrap().as_ref() {
            h.for_each_occupied(|_key, val| {
                let entries = self.collect_entries_from(self.ovf_file(), &val)?;
                for fk in entries {
                    f(fk);
                }
                Ok(())
            })?;
        }
        Ok(())
    }

    /// Create count without expanding Class A. Inline/slab use pack8 `used`.
    /// Extent reads last-page reserved (`0` = walk pages; no write — appender
    /// stamps reserved on pack).
    pub fn create_count(&self, scripthash: &[u8; 32]) -> Result<u32, StoreError> {
        let Some((val, home)) = self.locate_head(scripthash)? else {
            return Ok(0);
        };
        let home = KeyHome::from(home);
        let used = val.used();
        if used != u32::MAX {
            return Ok(used);
        }
        let body = self.body_for(scripthash, home);
        let ShHeadValue::Extent { last_page } = val else {
            return Ok(0);
        };
        let mut page = [0u8; SH_PAGE_SIZE];
        body.read_at(last_page, &mut page)?;
        let n = crate::scripthash_pages::sh_page_extent_creates(&page);
        if n > 0 {
            return Ok(n);
        }
        let fks = self.collect_entries_from(body, &val)?;
        Ok(u32::try_from(fks.len()).unwrap_or(u32::MAX))
    }

    /// Live create_tx_fks for a scripthash (oldest → newest).
    pub fn create_fks(&self, scripthash: &[u8; 32]) -> Result<Vec<Fk>, StoreError> {
        let Some((val, home)) = self.locate_head(scripthash)? else {
            return Ok(Vec::new());
        };
        self.collect_entries_from(self.body_for(scripthash, KeyHome::from(home)), &val)
    }

    /// Live creates for a scripthash (oldest → newest).
    ///
    /// Second element is a thin index row (no Class A joins). Expand at query.
    pub fn entries(
        &self,
        scripthash: &[u8; 32],
    ) -> Result<Vec<(Fk, ScriptHashRecord)>, StoreError> {
        Ok(self
            .create_fks(scripthash)?
            .into_iter()
            .map(|fk| (fk, ScriptHashRecord::from_fk(*scripthash, fk)))
            .collect())
    }

    /// Max durable create_tx_fk for a head value (**last page only** when paged).
    ///
    /// Sorted-chain invariant: max is the last entry of the last page (or max
    /// inline FK). Never walks earlier pages.
    #[doc(hidden)]
    pub fn last_create_fk_on(
        &self,
        body: &TableFile,
        val: &ShHeadValue,
    ) -> Result<Option<Fk>, StoreError> {
        match val {
            ShHeadValue::Empty => Ok(None),
            ShHeadValue::Inline { .. } => {
                let ents = val.inline_entries();
                Ok(ents.iter().copied().max_by_key(|f| f.0))
            }
            ShHeadValue::Slab { class, off, used } => {
                let ents = self.read_slab(body, *class, *off)?;
                if ents.len() != *used as usize {
                    return Err(StoreError::Corrupt(
                        "invariant: scripthash slab used != decoded fk count",
                    ));
                }
                Ok(ents.last().copied())
            }
            ShHeadValue::Extent { last_page } => {
                let mut page = [0u8; SH_PAGE_SIZE];
                body.read_at(*last_page, &mut page)?;
                if sh_page_extent(&page)?.is_none() {
                    return Err(StoreError::Corrupt("scripthash extent last page not ver=2"));
                }
                sh_page_last_fk(&page)
            }
        }
    }

    /// Forward-append creates. Each call locates heads from disk.
    ///
    /// Per scripthash key: sort create_tx_fks ascending, skip every `fk ≤` durable
    /// max (last page only), append the rest. **No full page-chain walk** on
    /// insert. Callers must apply SH batches in non-decreasing block/batch time
    /// order so skipped re-queues do not leave permanent holes.
    ///
    /// Head upserts: existing sorted-main / sealed-ovf keys update in place;
    /// new keys go to ingest OA (seal → `SHSR` ovf at ~0.80).
    pub fn put_create_batch_append(
        &self,
        recs: &[ScriptHashRecord],
    ) -> Result<(usize, AppendTiming), StoreError> {
        if recs.is_empty() {
            return Ok((0, AppendTiming::default()));
        }
        self.dirty_write(|| self.put_create_batch_append_marked(recs))
    }

    fn put_create_batch_append_marked(
        &self,
        recs: &[ScriptHashRecord],
    ) -> Result<(usize, AppendTiming), StoreError> {
        let mut timing = AppendTiming::default();

        let t_sort = std::time::Instant::now();
        let mut order: Vec<usize> = (0..recs.len()).collect();
        order.sort_by(|&a, &b| recs[a].scripthash.cmp(&recs[b].scripthash));
        timing.sort_ns = t_sort.elapsed().as_nanos() as u64;

        let t_seed = std::time::Instant::now();
        let mut heads = self.seed_batch_heads(recs, &order)?;
        timing.seed_ns = t_seed.elapsed().as_nanos() as u64;

        let mut written = 0usize;
        let mut i = 0usize;
        while i < order.len() {
            if self.ingest_room() == 0 {
                let before = self.ovf_seal_stamp();
                self.seal_ingest()?;
                if self.ovf_seal_stamp() != before {
                    retarget_ingest_after_seal(&mut heads);
                }
                continue;
            }
            let room = self.ingest_room();
            let t_body = std::time::Instant::now();
            let mut batch = IngestBatch::default();
            let mut staged = 0u64;
            while i < order.len() && staged < room {
                let key = recs[order[i]].scripthash;
                let mut fk_vals: Vec<u64> = Vec::new();
                while i < order.len() && recs[order[i]].scripthash == key {
                    let fk = recs[order[i]].create_tx_fk;
                    if !fk.is_null() {
                        fk_vals.push(fk.0);
                    }
                    i += 1;
                }
                if fk_vals.is_empty() {
                    continue;
                }
                let (cur, kh) = heads
                    .get(&key)
                    .cloned()
                    .unwrap_or((ShHeadValue::Empty, KeyHome::Absent));
                self.stage_key_creates(&mut batch, key, fk_vals, &cur, kh)?;
                staged = staged.saturating_add(1);
            }
            timing.body_ns = timing
                .body_ns
                .saturating_add(t_body.elapsed().as_nanos() as u64);
            if batch.head_final.is_empty() {
                continue;
            }
            let t_head = std::time::Instant::now();
            let flush_each = recs.len() as u64 >= Self::LARGE_BATCH_ROWS;
            let before = self.ovf_seal_stamp();
            self.commit_staged_batch(&batch, flush_each)?;
            if self.ovf_seal_stamp() != before {
                retarget_ingest_after_seal(&mut heads);
            }
            timing.head_ns = timing
                .head_ns
                .saturating_add(t_head.elapsed().as_nanos() as u64);
            written = written.saturating_add(batch.written);
        }
        Ok((written, timing))
    }

    /// Locate the durable head and home of every key the batch names. A key
    /// not in the map is absent; a cold body (no prior creates) skips the gets.
    fn seed_batch_heads(
        &self,
        recs: &[ScriptHashRecord],
        order: &[usize],
    ) -> Result<HashMap<[u8; 32], (ShHeadValue, KeyHome)>, StoreError> {
        let mut heads = HashMap::new();
        if self.entry_count() == 0 && self.head_is_empty() {
            return Ok(heads);
        }
        let mut keys: Vec<[u8; 32]> = order
            .iter()
            .map(|&i| &recs[i])
            .filter(|rec| !rec.create_tx_fk.is_null())
            .map(|rec| rec.scripthash)
            .collect();
        keys.sort_unstable();
        keys.dedup();
        for key in keys {
            if let Some((v, kh)) = self.locate_head(&key)? {
                heads.insert(key, (v, KeyHome::from(kh)));
            }
        }
        Ok(heads)
    }

    /// Pack one key's creates past its durable max into its body's stage.
    /// Nothing is written to the body here.
    fn stage_key_creates<'a>(
        &'a self,
        batch: &mut IngestBatch<'a>,
        key: [u8; 32],
        mut fk_vals: Vec<u64>,
        cur: &ShHeadValue,
        kh: KeyHome,
    ) -> Result<(), StoreError> {
        fk_vals.sort_unstable();
        fk_vals.dedup();
        let body = self.body_for(&key, kh);
        let max = self.last_create_fk_on(body, cur)?;
        let max_u = max.map(|f| f.0).unwrap_or(0);
        let add: Vec<Fk> = fk_vals
            .into_iter()
            .filter(|&fk| max.is_none() || fk > max_u)
            .map(Fk)
            .collect();
        if add.is_empty() {
            return Ok(());
        }
        batch.written += add.len();
        let alloc_mu = self.alloc_for(&key, kh);
        let stage = batch
            .stages
            .entry(std::ptr::from_ref(body) as usize)
            .or_insert_with(|| BodyStage {
                body,
                alloc: alloc_mu,
                slabs: StagedSlabs::default(),
                popped: false,
            });
        let frees_before = stage.slabs.frees.len();
        let mut alloc = alloc_mu.lock().unwrap();
        let free_before = alloc.free_head;
        let new_val = self.append_sorted_creates(body, &mut alloc, cur, &add, &mut stage.slabs)?;
        stage.popped |= alloc.free_head != free_before;
        drop(alloc);
        if stage.slabs.frees.len() > frees_before {
            note_moved_head(self, &mut batch.moved_heads, &key, kh);
        }
        batch.head_final.push((key, new_val, kh));
        Ok(())
    }

    /// Land a packed batch: payloads, alloc headers, heads, then the frees.
    ///
    /// A payload on a popped offset waits for a durable header that no longer
    /// names it. A body that popped nothing writes its payloads before the
    /// first barrier.
    fn commit_staged_batch(
        &self,
        batch: &IngestBatch<'_>,
        flush_each: bool,
    ) -> Result<(), StoreError> {
        let _hold = page_guard::hold_marks();
        let armed = self.arm_staged_batch(batch)?;
        self.land_staged_batch(batch, flush_each)?;
        self.clear_armed_sh(&armed)?;
        // Seal only after the mark is gone. A sealed file built while the
        // mark is armed would name body bytes the restore rolls back.
        self.maybe_seal_ingest()
    }

    fn land_staged_batch(
        &self,
        batch: &IngestBatch<'_>,
        flush_each: bool,
    ) -> Result<(), StoreError> {
        for stage in batch.stages.values() {
            publish_bump(stage.body, &stage.alloc.lock().unwrap())?;
        }
        for stage in batch.stages.values().filter(|s| !s.popped) {
            stage.slabs.write_to(stage.body)?;
        }
        self.sync_bodies_for(&batch.head_final)?;
        self.write_alloc_headers_for(&batch.head_final)?;
        self.sync_bodies_for(&batch.head_final)?;
        for stage in batch.stages.values().filter(|s| s.popped) {
            stage.slabs.write_to(stage.body)?;
            stage.body.sync_data_only()?;
        }
        self.apply_head_upserts(&batch.head_final, flush_each)?;
        self.free_replaced_slabs(&batch.stages, &batch.moved_heads)
    }

    fn arm_staged_batch(&self, batch: &IngestBatch<'_>) -> Result<ArmedSh, StoreError> {
        let mut pages: BTreeMap<(String, String, u64), [u8; PAGE as usize]> = BTreeMap::new();
        if batch
            .head_final
            .iter()
            .any(|(_, _, kh)| matches!(kh, KeyHome::Ingest | KeyHome::Absent))
        {
            self.ensure_ingest()?;
        }
        for stage in batch.stages.values() {
            self.image_body_stage(&mut pages, stage)?;
        }
        let heads: Vec<(&[u8; 32], KeyHome)> = batch
            .head_final
            .iter()
            .map(|(key, _, kh)| (key, *kh))
            .collect();
        self.image_head_keys(&mut pages, &heads)?;
        self.publish_sh_pages(&pages)
    }

    fn image_body_stage(
        &self,
        pages: &mut BTreeMap<(String, String, u64), [u8; PAGE as usize]>,
        stage: &BodyStage<'_>,
    ) -> Result<(), StoreError> {
        let mark = body_sh_mark(&self.store_dir, stage.body)?;
        let rel = page_guard::guard_rel_name(&self.store_dir, stage.body.path())?;
        image_range(
            pages,
            &mark,
            &rel,
            stage.body.path(),
            &stage.body.read_file,
            0,
            1,
        )?;
        let mut at = 0usize;
        for &(off, len) in &stage.slabs.writes {
            image_range(
                pages,
                &mark,
                &rel,
                stage.body.path(),
                &stage.body.read_file,
                off,
                len as u64,
            )?;
            at = at.saturating_add(len);
        }
        let _ = at;
        for &(_, off) in stage.slabs.frees.iter().chain(&stage.slabs.gaps) {
            image_range(
                pages,
                &mark,
                &rel,
                stage.body.path(),
                &stage.body.read_file,
                off,
                8,
            )?;
        }
        Ok(())
    }

    /// Image every head this batch will write. Ingest keys, and main or sealed
    /// keys that fall through to ingest, are one probe pass: a second key that
    /// loses the first empty slot lands on a later page.
    fn image_head_keys(
        &self,
        pages: &mut BTreeMap<(String, String, u64), [u8; PAGE as usize]>,
        keys: &[(&[u8; 32], KeyHome)],
    ) -> Result<(), StoreError> {
        let mut ingest = Vec::with_capacity(keys.len());
        for &(key, kh) in keys {
            let resident = match kh {
                KeyHome::Main => self.image_main_resident(pages, key)?,
                KeyHome::SealedOvf => self.image_sealed_resident(pages, key)?,
                KeyHome::Ingest | KeyHome::Absent => false,
            };
            if !resident {
                ingest.push(*key);
            }
        }
        self.image_ingest_keys(pages, &ingest)
    }

    fn image_main_resident(
        &self,
        pages: &mut BTreeMap<(String, String, u64), [u8; PAGE as usize]>,
        key: &[u8; 32],
    ) -> Result<bool, StoreError> {
        let hk = head_key_from_full(key);
        let si = self.shard_index(key);
        let Some(slot) = self.sorted_main.get(si) else {
            return Ok(false);
        };
        let g = slot.read().unwrap();
        let Some(h) = g.as_ref() else {
            return Ok(false);
        };
        let Some(off) = h.value_word_offset(&hk)? else {
            return Ok(false);
        };
        let vp = crate::scripthash_mphf::val_path(&h.base);
        let rel = page_guard::guard_rel_name(&self.store_dir, &vp)?;
        let mark = page_guard::sh_mark(&format!("{si:02x}"));
        image_range(pages, &mark, &rel, &vp, &h.val_file, off, 8)?;
        Ok(true)
    }

    fn image_sealed_resident(
        &self,
        pages: &mut BTreeMap<(String, String, u64), [u8; PAGE as usize]>,
        key: &[u8; 32],
    ) -> Result<bool, StoreError> {
        let hk = head_key_from_full(key);
        let mark = page_guard::sh_mark("ovf");
        {
            let g = self.sealed_ovf.lock().unwrap();
            for h in g.iter().rev() {
                if let Some(off) = h.value_word_offset(&hk)? {
                    let rel = page_guard::guard_rel_name(&self.store_dir, h.path())?;
                    image_range(pages, &mark, &rel, h.path(), &h.file, off, 8)?;
                    return Ok(true);
                }
            }
        }
        if let Some(l1) = self.ovf_l1.lock().unwrap().as_ref() {
            if let Some(off) = l1.head.value_word_offset(&hk)? {
                let vp = crate::scripthash_mphf::val_path(&l1.head.base);
                let rel = page_guard::guard_rel_name(&self.store_dir, &vp)?;
                image_range(pages, &mark, &rel, &vp, &l1.head.val_file, off, 8)?;
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn rearm_ingest_pages(&self, keys: &[[u8; 32]]) -> Result<(), StoreError> {
        let mark = self.store_dir.join(page_guard::sh_mark("ovf"));
        if !mark.try_exists().map_err(|e| StoreError::io(&mark, e))? {
            return Ok(());
        }
        let mut pages: BTreeMap<(String, String, u64), [u8; PAGE as usize]> = BTreeMap::new();
        self.image_ingest_keys(&mut pages, keys)?;
        self.publish_sh_pages(&pages).map(|_| ())
    }

    /// One image for the whole ingest batch. Empty tables bulk-fill slots.
    /// A live table flushes each dirty chunk, including a key displaced past
    /// its first empty slot.
    fn image_ingest_keys(
        &self,
        pages: &mut BTreeMap<(String, String, u64), [u8; PAGE as usize]>,
        keys: &[[u8; 32]],
    ) -> Result<(), StoreError> {
        if keys.is_empty() {
            return Ok(());
        }
        self.ensure_ingest()?;
        let empty = self
            .ingest
            .lock()
            .unwrap()
            .as_ref()
            .is_some_and(|h| h.is_known_empty());
        if empty {
            self.image_empty_ingest(pages, keys)
        } else {
            self.image_live_ingest(pages, keys)
        }
    }

    /// An empty ingest table places later keys past the first probe. Image
    /// every slot this batch will write, not only each key's first empty slot.
    fn image_empty_ingest(
        &self,
        pages: &mut BTreeMap<(String, String, u64), [u8; PAGE as usize]>,
        keys: &[[u8; 32]],
    ) -> Result<(), StoreError> {
        let g = self.ingest.lock().unwrap();
        let Some(h) = g.as_ref() else {
            return Ok(());
        };
        let slots = h.state.lock().unwrap().slots;
        let rel = page_guard::guard_rel_name(&self.store_dir, h.file.path())?;
        let path = h.file.path().to_path_buf();
        let keyed: Vec<_> = keys.iter().map(ScriptHashHead::to_key).collect();
        let assigned = ScriptHashHead::assign_empty_slots(slots, &keyed)?;
        let mark = page_guard::sh_mark("ovf");
        let mut seen = BTreeSet::new();
        for slot in assigned {
            if !seen.insert(slot) {
                continue;
            }
            image_range(
                pages,
                &mark,
                &rel,
                &path,
                &h.file.read_file,
                ScriptHashHead::slot_file_off(slot),
                crate::scripthash_layout::SH_HEAD_SLOT_SIZE as u64,
            )?;
        }
        self.image_occ_sidecar(pages, &mark, &path)
    }

    /// A non-empty ingest probe-inserts the batch in hash-slot order. Image
    /// every chunk that insert will write, including a key displaced past
    /// its first empty slot.
    fn image_live_ingest(
        &self,
        pages: &mut BTreeMap<(String, String, u64), [u8; PAGE as usize]>,
        keys: &[[u8; 32]],
    ) -> Result<(), StoreError> {
        let (rel, path, spans, occ_known) = {
            let g = self.ingest.lock().unwrap();
            let Some(h) = g.as_ref() else {
                return Ok(());
            };
            let rel = page_guard::guard_rel_name(&self.store_dir, h.file.path())?;
            let path = h.file.path().to_path_buf();
            let spans = h.upsert_write_spans(keys)?;
            let occ_known = h.state.lock().unwrap().occ_known;
            (rel, path, spans, occ_known)
        };
        if spans.is_empty() {
            return Ok(());
        }
        let file = std::fs::OpenOptions::new()
            .read(true)
            .open(&path)
            .map_err(|e| StoreError::io(&path, e))?;
        let mark = page_guard::sh_mark("ovf");
        for (off, len) in spans {
            image_range(pages, &mark, &rel, &path, &file, off, len)?;
        }
        if !occ_known {
            // Nothing writes `.occ` while the count is unknown. A record for
            // a sidecar that may not exist would keep the mark past its clear.
            return Ok(());
        }
        self.image_occ_sidecar(pages, &mark, &path)
    }

    /// Page 0 of `{head}.occ`. Restore writes only the bytes the sidecar has.
    fn image_occ_sidecar(
        &self,
        pages: &mut BTreeMap<(String, String, u64), [u8; PAGE as usize]>,
        mark: &str,
        head_path: &Path,
    ) -> Result<(), StoreError> {
        let occ = crate::scripthash_head::occ_sidecar_path(head_path);
        let rel = page_guard::guard_rel_name(&self.store_dir, &occ)?;
        let key = (mark.to_string(), rel, 0);
        if pages.contains_key(&key) {
            return Ok(());
        }
        let image = match std::fs::File::open(&occ) {
            Ok(file) => page_guard::read_live_page(&occ, &file, 0)?,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => [0u8; PAGE as usize],
            Err(e) => return Err(StoreError::io(&occ, e)),
        };
        pages.insert(key, image);
        Ok(())
    }

    /// `prev` is an earlier arm this write still lands under. A page its mark
    /// still holds keeps that pre-image. A page a later clear dropped is
    /// imaged again, and the result names both arms' files.
    pub(super) fn arm_suffix_pages(
        &self,
        body: &TableFile,
        scripthash: &[u8; 32],
        home: LocatedHome,
        offs: &[u64],
        image_head: bool,
        prev: Option<&ArmedSh>,
    ) -> Result<ArmedSh, StoreError> {
        let mut pages: BTreeMap<(String, String, u64), [u8; PAGE as usize]> = BTreeMap::new();
        let mark = body_sh_mark(&self.store_dir, body)?;
        let rel = page_guard::guard_rel_name(&self.store_dir, body.path())?;
        for &off in offs {
            image_range(
                &mut pages,
                &mark,
                &rel,
                body.path(),
                &body.read_file,
                off,
                1,
            )?;
        }
        if image_head {
            self.image_head_keys(&mut pages, &[(scripthash, KeyHome::from(home))])?;
        }
        let Some(prev) = prev else {
            return self.publish_sh_pages(&pages);
        };
        let mut held: BTreeMap<String, HashSet<(String, u64)>> = BTreeMap::new();
        for (mark, _, _) in pages.keys() {
            if !held.contains_key(mark) {
                let pages_held = page_guard::armed_group_pages(&self.store_dir.join(mark))?;
                held.insert(mark.clone(), pages_held);
            }
        }
        pages.retain(|(mark, rel, off), _| {
            !held
                .get(mark)
                .is_some_and(|h| h.contains(&(rel.clone(), *off)))
        });
        let mut armed = self.publish_sh_pages(&pages)?;
        for (mark, rels) in &prev.files {
            armed
                .files
                .entry(mark.clone())
                .or_default()
                .extend(rels.iter().cloned());
        }
        Ok(armed)
    }

    pub(super) fn arm_unlink_rewrites(
        &self,
        batch: &unlink::UnlinkBatch,
    ) -> Result<ArmedSh, StoreError> {
        let mut pages: BTreeMap<(String, String, u64), [u8; PAGE as usize]> = BTreeMap::new();
        if batch
            .rewrites
            .iter()
            .any(|j| matches!(j.home, LocatedHome::Ingest | LocatedHome::Main))
        {
            self.ensure_ingest()?;
        }
        let mut heads = Vec::with_capacity(batch.rewrites.len());
        for job in &batch.rewrites {
            let kh = KeyHome::from(job.home);
            let body = self.body_for(&job.scripthash, kh);
            let mark = body_sh_mark(&self.store_dir, body)?;
            let rel = page_guard::guard_rel_name(&self.store_dir, body.path())?;
            image_range(&mut pages, &mark, &rel, body.path(), &body.read_file, 0, 1)?;
            let mut at = 0usize;
            for &(off, len) in &job.staged.writes {
                image_range(
                    &mut pages,
                    &mark,
                    &rel,
                    body.path(),
                    &body.read_file,
                    off,
                    len as u64,
                )?;
                at = at.saturating_add(len);
            }
            let _ = at;
            for &(_, off) in &job.staged.gaps {
                image_range(
                    &mut pages,
                    &mark,
                    &rel,
                    body.path(),
                    &body.read_file,
                    off,
                    8,
                )?;
            }
            if unlink::frees_old_pages(&job.old) {
                match &job.old {
                    ShHeadValue::Slab { off, .. } => {
                        image_range(
                            &mut pages,
                            &mark,
                            &rel,
                            body.path(),
                            &body.read_file,
                            *off,
                            8,
                        )?;
                    }
                    ShHeadValue::Extent { last_page } => {
                        let first = paged_first_from_last(body, *last_page)?;
                        follow_page_chain(body, first, SH_PAGE_CHAIN_MAX, |body, off| {
                            image_range(
                                &mut pages,
                                &mark,
                                &rel,
                                body.path(),
                                &body.read_file,
                                off,
                                SH_PAGE_SIZE as u64,
                            )
                        })?;
                    }
                    _ => {}
                }
            }
            heads.push((&job.scripthash, kh));
        }
        self.image_head_keys(&mut pages, &heads)?;
        self.publish_sh_pages(&pages)
    }

    fn publish_sh_pages(
        &self,
        pages: &BTreeMap<(String, String, u64), [u8; PAGE as usize]>,
    ) -> Result<ArmedSh, StoreError> {
        let mut by_mark: BTreeMap<&str, Vec<PageImageRef<'_>>> = BTreeMap::new();
        let mut armed = ArmedSh::default();
        for ((mark, rel, off), image) in pages {
            by_mark
                .entry(mark.as_str())
                .or_default()
                .push((rel.as_str(), *off, image.as_slice()));
            armed
                .files
                .entry(mark.clone())
                .or_default()
                .insert(rel.clone());
        }
        for (mark, mut recs) in by_mark {
            // A backward mark keeps the oldest pre-image. A page a commit that
            // failed before its clear still holds keeps that image, not this
            // commit's view of it, which already has the failed bytes.
            let held = page_guard::armed_group_pages(&self.store_dir.join(mark))?;
            recs.retain(|(rel, off, _)| !held.contains(&(rel.to_string(), *off)));
            page_guard::arm_store_group(&self.store_dir, mark, &recs)?;
        }
        Ok(armed)
    }

    pub(super) fn clear_armed_sh(&self, armed: &ArmedSh) -> Result<(), StoreError> {
        #[cfg(debug_assertions)]
        if take_fail_sh_guard_clear() {
            return Err(StoreError::Corrupt("scripthash page guard clear failed"));
        }
        use std::fs::OpenOptions;
        for (mark, rels) in &armed.files {
            let mark_path = self.store_dir.join(mark);
            let mut opened: Vec<(File, PathBuf)> = Vec::new();
            for rel in rels {
                let path = self.store_dir.join(rel);
                match OpenOptions::new().read(true).write(true).open(&path) {
                    Ok(f) => opened.push((f, path)),
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
                    Err(e) => return Err(StoreError::io(&path, e)),
                }
            }
            let refs: Vec<(&File, &Path)> = opened.iter().map(|(f, p)| (f, p.as_path())).collect();
            page_guard::clear_page_guard_rel(&mark_path, &self.store_dir, &refs)?;
        }
        Ok(())
    }

    /// Leave the next scripthash page-guard clear behind.
    #[cfg(debug_assertions)]
    pub fn fail_next_sh_guard_clear(&self) {
        FAIL_CLEAR_SH_GUARD.with(|c| c.set(true));
    }

    /// Link the slabs a class change replaced once the heads that named them
    /// are durable: the links, a body barrier, then the header that names them.
    fn free_replaced_slabs(
        &self,
        stages: &BTreeMap<usize, BodyStage<'_>>,
        moved_heads: &[MovedHead],
    ) -> Result<(), StoreError> {
        for head in moved_heads {
            match head {
                MovedHead::MainShard(si) => {
                    if let Some(h) = self.sorted_main[*si].read().unwrap().as_ref() {
                        h.flush()?;
                    }
                }
                MovedHead::SealedOvf => {
                    for h in self.sealed_ovf.lock().unwrap().iter() {
                        h.flush()?;
                    }
                    if let Some(l1) = self.ovf_l1.lock().unwrap().as_ref() {
                        l1.head.flush()?;
                    }
                }
            }
        }
        for stage in stages
            .values()
            .filter(|s| !s.slabs.frees.is_empty() || !s.slabs.gaps.is_empty())
        {
            let mut alloc = stage.alloc.lock().unwrap();
            for &(class, off) in stage.slabs.frees.iter().chain(&stage.slabs.gaps) {
                self.free_slab(stage.body, &mut alloc, class, off)?;
            }
            stage.body.sync_data_only()?;
            write_alloc_header(stage.body, &alloc)?;
        }
        Ok(())
    }

    /// Route head upserts without get-then-insert:
    /// - **Overflow(seg)** → that overflow segment only (update-on-home)
    /// - **Main home** → try main (update; sealed uses update-only so no new slots)
    /// - **Absent** + sealed + fuse says not on main → open overflow
    /// - **Absent** + sealed + no fuse / fuse maybe → try main **update-only**;
    ///   not-present → remainder → open overflow (never allocate free slots on sealed main)
    /// - **Absent** + unsealed + main accepts → try main with new slots; remainder after full
    ///
    /// Overflow is mono segment stack: no_rehash only; NeedSlot → seal+roll.
    #[doc(hidden)]
    pub fn apply_head_upserts(
        &self,
        upserts: &[([u8; 32], ShHeadValue, KeyHome)],
        _flush_each: bool,
    ) -> Result<(), StoreError> {
        let mut ingest_ups: Vec<([u8; 32], ShHeadValue)> = Vec::new();
        let mut sealed_ovf_ups: Vec<([u8; 32], ShHeadValue)> = Vec::new();

        for (key, val, home) in upserts {
            match home {
                KeyHome::Ingest | KeyHome::Absent => {
                    ingest_ups.push((*key, val.clone()));
                }
                KeyHome::SealedOvf => {
                    sealed_ovf_ups.push((*key, val.clone()));
                }
                KeyHome::Main => self.upsert_main_or_ingest(*key, val, &mut ingest_ups)?,
            }
        }
        self.flush_sealed_ovf_upserts(sealed_ovf_ups, &mut ingest_ups)?;
        if !ingest_ups.is_empty() {
            self.ingest_insert_many(&ingest_ups)?;
        }
        Ok(())
    }

    #[doc(hidden)]
    pub fn upsert_main_or_ingest(
        &self,
        key: [u8; 32],
        val: &ShHeadValue,
        ingest_ups: &mut Vec<([u8; 32], ShHeadValue)>,
    ) -> Result<(), StoreError> {
        let hk = head_key_from_full(&key);
        let si = self.shard_index(&key);
        let updated = if let Some(slot) = self.sorted_main.get(si) {
            let g = slot.read().unwrap();
            match g.as_ref() {
                Some(h) => h.update_value(&hk, val)?,
                None => false,
            }
        } else {
            false
        };
        if !updated {
            ingest_ups.push((key, val.clone()));
        }
        Ok(())
    }

    #[doc(hidden)]
    pub fn flush_sealed_ovf_upserts(
        &self,
        sealed_ovf_ups: Vec<([u8; 32], ShHeadValue)>,
        ingest_ups: &mut Vec<([u8; 32], ShHeadValue)>,
    ) -> Result<(), StoreError> {
        if sealed_ovf_ups.is_empty() {
            return Ok(());
        }
        let missed = self.sealed_ovf_l0_misses(&sealed_ovf_ups)?;
        self.sealed_miss_try_l1(missed, ingest_ups)
    }

    #[doc(hidden)]
    pub fn sealed_ovf_l0_misses(
        &self,
        sealed_ovf_ups: &[([u8; 32], ShHeadValue)],
    ) -> Result<Vec<([u8; 32], ShHeadValue)>, StoreError> {
        let mut missed: Vec<([u8; 32], ShHeadValue)> = Vec::new();
        let g = self.sealed_ovf.lock().unwrap();
        for (key, val) in sealed_ovf_ups {
            if !self.sealed_ovf_update_rev(&g, key, val)? {
                missed.push((*key, val.clone()));
            }
        }
        Ok(missed)
    }

    #[doc(hidden)]
    pub fn sealed_ovf_update_rev(
        &self,
        sealed: &[SortedHead],
        key: &[u8; 32],
        val: &ShHeadValue,
    ) -> Result<bool, StoreError> {
        let hk = head_key_from_full(key);
        for h in sealed.iter().rev() {
            if h.update_value(&hk, val)? {
                return Ok(true);
            }
        }
        Ok(false)
    }

    #[doc(hidden)]
    pub fn sealed_miss_try_l1(
        &self,
        missed: Vec<([u8; 32], ShHeadValue)>,
        ingest_ups: &mut Vec<([u8; 32], ShHeadValue)>,
    ) -> Result<(), StoreError> {
        for (key, val) in missed {
            if !self.ovf_l1_update(&key, &val)? {
                ingest_ups.push((key, val));
            }
        }
        Ok(())
    }

    #[doc(hidden)]
    pub fn ovf_l1_update(&self, key: &[u8; 32], val: &ShHeadValue) -> Result<bool, StoreError> {
        let hk = head_key_from_full(key);
        if let Some(l1) = self.ovf_l1.lock().unwrap().as_ref() {
            return l1.head.update_value(&hk, val);
        }
        Ok(false)
    }

    fn ovf_seal_stamp(&self) -> (usize, bool) {
        (
            self.sealed_ovf.lock().unwrap().len(),
            self.ovf_l1.lock().unwrap().is_some(),
        )
    }

    fn ingest_room(&self) -> u64 {
        let g = self.ingest.lock().unwrap();
        match g.as_ref() {
            Some(h) => h.room_before_seal(),
            None => ScriptHashHead::seal_room(ingest_oa_slots(self.scale), 0),
        }
    }

    /// The first insert creates the slot table. Store open does not.
    fn ensure_ingest(&self) -> Result<(), StoreError> {
        let mut g = self.ingest.lock().unwrap();
        if g.is_some() {
            return Ok(());
        }
        *g = Some(ScriptHashHead::create_with_slots(
            ingest_path(&self.store_dir),
            ingest_oa_slots(self.scale),
        )?);
        Ok(())
    }

    #[doc(hidden)]
    pub fn ingest_insert_many(&self, ups: &[([u8; 32], ShHeadValue)]) -> Result<(), StoreError> {
        if ups.is_empty() {
            return Ok(());
        }
        if self.sh_mark_present()? {
            self.ensure_ingest()?;
            let keys: Vec<[u8; 32]> = ups.iter().map(|(k, _)| *k).collect();
            self.rearm_ingest_pages(&keys)?;
            let g = self.ingest.lock().unwrap();
            let Some(h) = g.as_ref() else {
                return Err(StoreError::Corrupt("scripthash: ingest missing"));
            };
            return h.insert_many(ups);
        }
        let mut i = 0usize;
        while i < ups.len() {
            let room = self.ingest_room();
            if room == 0 {
                self.seal_ingest()?;
                continue;
            }
            self.ensure_ingest()?;
            let rest: Vec<[u8; 32]> = ups[i..].iter().map(|(k, _)| *k).collect();
            self.rearm_ingest_pages(&rest)?;
            let n = (room as usize).min(ups.len() - i);
            {
                let g = self.ingest.lock().unwrap();
                let Some(h) = g.as_ref() else {
                    return Err(StoreError::Corrupt("scripthash: ingest missing"));
                };
                h.insert_many(&ups[i..i + n])?;
            }
            i += n;
            self.maybe_seal_ingest()?;
        }
        Ok(())
    }

    fn sh_mark_present(&self) -> Result<bool, StoreError> {
        let guard = self.store_dir.join("guard");
        let rd = match std::fs::read_dir(&guard) {
            Ok(rd) => rd,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(false),
            Err(e) => return Err(StoreError::io(&guard, e)),
        };
        for ent in rd {
            let ent = ent.map_err(|e| StoreError::io(&guard, e))?;
            let name = ent.file_name();
            let Some(name) = name.to_str() else {
                continue;
            };
            if page_guard::published_sh_mark(name) {
                return Ok(true);
            }
        }
        Ok(false)
    }

    #[doc(hidden)]
    pub fn maybe_seal_ingest(&self) -> Result<(), StoreError> {
        let load = {
            let g = self.ingest.lock().unwrap();
            g.as_ref().and_then(|h| h.load_ratio()).unwrap_or(0.0)
        };
        if load < ScriptHashHead::SH_SEAL_LOAD {
            return Ok(());
        }
        self.seal_ingest()
    }

    #[doc(hidden)]
    pub fn seal_ingest(&self) -> Result<(), StoreError> {
        // A commit that failed before its clear left body and ingest images
        // armed. A seal would drop only the ingest image, and open could then
        // roll back a slab the sealed head names. Open restores first.
        if self.sh_mark_present()? {
            return Err(StoreError::Rejected(
                "scripthash: a commit guard is still armed; restart to restore it before a seal",
            ));
        }
        let mut recs = {
            let g = self.ingest.lock().unwrap();
            let Some(g) = g.as_ref() else {
                return Ok(());
            };
            let mut recs = Vec::new();
            g.for_each_occupied(|full, val| {
                recs.push((head_key_from_full(&full), pack8_bytes(&val)?));
                Ok(())
            })?;
            recs
        };
        if recs.is_empty() {
            // Nothing to move. An empty table can still report no room when
            // its occupancy is unknown, so retire it instead of returning
            // with that table in place. The next insert creates a new one.
            let p = ingest_path(&self.store_dir);
            *self.ingest.lock().unwrap() = None;
            crate::scripthash_head::remove_ingest_files(&p)?;
            return Ok(());
        }
        recs.sort_unstable_by_key(|a| a.0);
        let id = next_sealed_ovf_id(&self.store_dir)?;
        let path = sealed_ovf_path(&self.store_dir, id);
        if path.exists() {
            return Err(StoreError::Corrupt(
                "scripthash.ovf: seal path already exists",
            ));
        }
        let sealed = SortedHead::write(&path, &recs)?;
        // Until the drop returns, ingest stays the only copy of these keys.
        if let Err(e) = self.drop_ingest_rel() {
            drop(sealed);
            if let Err(gone) = crate::scripthash_sorted_head::remove_sorted_head_files(&path) {
                rbitcoin_log::warn!("store: rejected scripthash seal not removed: {gone}");
            }
            return Err(e);
        }
        self.sealed_ovf.lock().unwrap().push(sealed);
        let p = ingest_path(&self.store_dir);
        // Drop the fd before unlink so the name can go away on Windows.
        *self.ingest.lock().unwrap() = None;
        crate::scripthash_head::remove_ingest_files(&p)?;
        self.maybe_compact_sealed_ovf()?;
        Ok(())
    }

    fn drop_ingest_rel(&self) -> Result<(), StoreError> {
        #[cfg(debug_assertions)]
        if FAIL_INGEST_REL_DROP.with(|c| c.replace(false)) {
            return Err(StoreError::Corrupt("scripthash ingest name drop failed"));
        }
        page_guard::drop_sh_rel(&self.store_dir, "scripthash.ovf/ingest")
    }

    /// Fail the next seal's drop of the ingest name from the sh marks.
    #[cfg(debug_assertions)]
    pub fn fail_next_ingest_rel_drop(&self) {
        FAIL_INGEST_REL_DROP.with(|c| c.set(true));
    }

    #[doc(hidden)]
    pub const SEALED_OVF_COMPACT_FILES: usize = 8;

    #[doc(hidden)]
    pub fn maybe_compact_sealed_ovf(&self) -> Result<(), StoreError> {
        if self.ovf_l1.lock().unwrap().is_some() {
            let n = self.sealed_ovf.lock().unwrap().len();
            if n >= Self::SEALED_OVF_COMPACT_FILES {
                self.warn_l1_frozen();
            }
            return Ok(());
        }
        let n = self.sealed_ovf.lock().unwrap().len();
        if n >= Self::SEALED_OVF_COMPACT_FILES {
            self.compact_sealed_ovf()?;
        }
        Ok(())
    }

    #[doc(hidden)]
    pub fn warn_l1_frozen(&self) {
        if self
            .l1_frozen_warned
            .compare_exchange(false, true, Ordering::Relaxed, Ordering::Relaxed)
            .is_ok()
        {
            rbitcoin_log::warn!("store: {SH_L1_FROZEN_WARN}");
        }
    }

    /// K-way merge of sealed global ovf heads. Body offs unchanged. Install L1
    /// before dropping L0 so locate_head never sees both empty.
    pub fn compact_sealed_ovf(&self) -> Result<(), StoreError> {
        if self.ovf_l1.lock().unwrap().is_some() {
            self.warn_l1_frozen();
            return Ok(());
        }
        let mut recs: Vec<(crate::scripthash_layout::ShHeadKey, u64)> = Vec::new();
        let old_paths: Vec<PathBuf> = {
            let g = self.sealed_ovf.lock().unwrap();
            if g.len() < 2 {
                return Ok(());
            }
            for h in g.iter() {
                h.for_each_occupied(|k, v| {
                    recs.push((k, pack8(&v)?));
                    Ok(())
                })?;
            }
            g.iter().map(|h| h.path().to_path_buf()).collect()
        };
        recs.sort_unstable_by_key(|a| a.0);
        for w in recs.windows(2) {
            if w[1].0 == w[0].0 {
                return Err(StoreError::Corrupt(
                    "invariant: sealed ovf compact saw a dual-home key",
                ));
            }
        }
        let id = {
            let g = self.sealed_ovf.lock().unwrap();
            g.iter()
                .filter_map(|h| {
                    h.path()
                        .file_name()
                        .and_then(|n| n.to_str())
                        .and_then(|s| s.parse::<u32>().ok())
                })
                .max()
                .unwrap_or(0)
                .saturating_add(1)
        };
        let path = sealed_ovf_path(&self.store_dir, id);
        let head = MphfHead::write_pack8(&path, &recs)?;
        let mut fuse_keys: Vec<u64> = recs.iter().map(|(k, _)| mix_key16(k)).collect();
        fuse_keys.sort_unstable();
        fuse_keys.dedup();
        let fuse = SealedFuse8::build(&fuse_keys)?;
        {
            let mut fp = path.as_os_str().to_os_string();
            fp.push(".fuse8");
            let fuse = fuse.write_then_map(&PathBuf::from(fp))?;
            *self.ovf_l1.lock().unwrap() = Some(OvfL1 { head, fuse });
        }
        let old = {
            let mut g = self.sealed_ovf.lock().unwrap();
            std::mem::take(&mut *g)
        };
        drop(old);
        for p in old_paths {
            let _ = std::fs::remove_file(&p);
            let mut idx = p.as_os_str().to_os_string();
            idx.push(".idx");
            let _ = std::fs::remove_file(idx);
            let mut fuse = p.as_os_str().to_os_string();
            fuse.push(".fuse8");
            let _ = std::fs::remove_file(fuse);
        }
        Ok(())
    }

    /// ≈1M create rows: materialize flushes each head shard after its bucket.
    pub const LARGE_BATCH_ROWS: u64 = 1_000_000;

    #[doc(hidden)]
    pub fn collect_entries_from(
        &self,
        body: &TableFile,
        val: &ShHeadValue,
    ) -> Result<Vec<Fk>, StoreError> {
        match val {
            ShHeadValue::Empty => Ok(Vec::new()),
            ShHeadValue::Inline { .. } => Ok(val.inline_fks()),
            ShHeadValue::Slab { class, off, used } => {
                let got = self.read_slab(body, *class, *off)?;
                if got.len() != *used as usize {
                    return Err(StoreError::Corrupt(
                        "invariant: scripthash slab used != decoded fk count",
                    ));
                }
                Ok(got)
            }
            ShHeadValue::Extent { last_page } => collect_extent_then_tail(body, *last_page),
        }
    }

    /// Pack `live` into a new inline, slab, or extent. Does not free `old`
    /// and does not publish the head. The caller writes the head, then frees.
    #[doc(hidden)]
    pub fn rewrite_entries_for_key(
        &self,
        body: &TableFile,
        alloc: &mut AllocState,
        live: &[Fk],
    ) -> Result<(ShHeadValue, StagedSlabs, bool), StoreError> {
        if live.is_empty() {
            return Ok((ShHeadValue::Empty, StagedSlabs::default(), false));
        }
        for w in live.windows(2) {
            if w[1].0 <= w[0].0 {
                return Err(StoreError::Corrupt(
                    "invariant: scripthash rewrite create_fks not strictly increasing",
                ));
            }
        }
        let mut staged = StagedSlabs::default();
        let free_before = alloc.free_head;
        let val = self.pack_entries(body, alloc, live, false, &mut staged)?;
        let popped = alloc.free_head != free_before;
        Ok((val, staged, popped))
    }

    /// Append strictly increasing `new_ents` (all already `> durable max`) without
    /// walking the full page chain. Body I/O: fill last page + optional new pages.
    #[doc(hidden)]
    pub fn append_sorted_creates(
        &self,
        body: &TableFile,
        alloc: &mut AllocState,
        old: &ShHeadValue,
        new_ents: &[Fk],
        staged: &mut StagedSlabs,
    ) -> Result<ShHeadValue, StoreError> {
        if new_ents.is_empty() {
            return Ok(old.clone());
        }
        for w in new_ents.windows(2) {
            if w[1].0 <= w[0].0 {
                return Err(StoreError::Corrupt(
                    "invariant: scripthash append batch create_fks not strictly increasing",
                ));
            }
        }
        alloc.live_count = alloc.live_count.saturating_add(new_ents.len() as u64);

        match old {
            ShHeadValue::Empty => self.pack_entries(body, alloc, new_ents, true, staged),
            ShHeadValue::Inline { .. } => {
                let mut live = old.inline_entries().to_vec();
                live.extend_from_slice(new_ents);
                self.pack_entries(body, alloc, &live, true, staged)
            }
            ShHeadValue::Slab { class, off, .. } => {
                let mut live = self.read_slab(body, *class, *off)?;
                if let (Some(last), Some(first_new)) = (live.last(), new_ents.first()) {
                    if first_new.0 <= last.0 {
                        return Err(StoreError::Corrupt(
                            "invariant: scripthash slab append create_fk not strictly increasing",
                        ));
                    }
                }
                live.extend_from_slice(new_ents);
                let new_val = self.pack_entries_reuse(
                    body,
                    alloc,
                    &live,
                    true,
                    Some((*class, *off)),
                    staged,
                )?;
                if !matches!(
                    &new_val,
                    ShHeadValue::Slab {
                        class: nc,
                        off: no,
                        ..
                    } if *nc == *class && *no == *off
                ) {
                    staged.free(*class, *off);
                }
                Ok(new_val)
            }
            ShHeadValue::Extent { last_page } => {
                let last = self.append_fks_to_pages(body, alloc, *last_page, new_ents, staged)?;
                Ok(ShHeadValue::extent(last))
            }
        }
    }

    /// Pack `live` into inline / slab / megakey pages. `slack` picks a class
    /// with a spare slot on tip grow; cold pack uses exact class.
    #[doc(hidden)]
    pub fn pack_entries(
        &self,
        body: &TableFile,
        alloc: &mut AllocState,
        live: &[Fk],
        slack: bool,
        staged: &mut StagedSlabs,
    ) -> Result<ShHeadValue, StoreError> {
        self.pack_entries_reuse(body, alloc, live, slack, None, staged)
    }

    #[doc(hidden)]
    pub fn pack_entries_reuse(
        &self,
        body: &TableFile,
        alloc: &mut AllocState,
        live: &[Fk],
        slack: bool,
        reuse: Option<(u8, u64)>,
        staged: &mut StagedSlabs,
    ) -> Result<ShHeadValue, StoreError> {
        let n = live.len() as u32;
        if n == 0 {
            return Ok(ShHeadValue::Empty);
        }
        if n <= SH_INLINE_CAP as u32 {
            return Ok(ShHeadValue::inline_one(live[0]));
        }
        if n >= SH_MEGAKEY_MIN_FKS {
            let last = self.write_new_page_chain(body, alloc, live, staged)?;
            return Ok(ShHeadValue::extent(last));
        }
        let mut raw = [0u64; SH_MEGAKEY_MIN_FKS as usize - 1];
        for (i, fk) in live.iter().enumerate() {
            raw[i] = fk.0;
        }
        let mut payload = [0u8; 2048];
        let packed_len = encode_slab_payload_into(&mut payload, &raw[..n as usize])?;
        let class = match Self::slab_class_fitting(n, packed_len, slack) {
            Some(c) => c,
            None => {
                let last = self.write_new_page_chain(body, alloc, live, staged)?;
                return Ok(ShHeadValue::extent(last));
            }
        };
        let cap = slab_bytes(class) as usize;
        if packed_len > cap {
            return Err(StoreError::Corrupt(
                "invariant: scripthash slab payload exceeds class bytes",
            ));
        }
        let off = if let Some((rc, ro)) = reuse {
            if rc == class {
                ro
            } else {
                self.alloc_slab(body, alloc, class, &mut staged.gaps)?
            }
        } else {
            self.alloc_slab(body, alloc, class, &mut staged.gaps)?
        };
        let mut buf = [0u8; 2048];
        buf[..packed_len].copy_from_slice(&payload[..packed_len]);
        staged.write(off, &buf[..cap]);
        Ok(ShHeadValue::slab(class, n as u16, off))
    }

    #[doc(hidden)]
    pub fn slab_class_fitting(n: u32, packed_len: usize, slack: bool) -> Option<u8> {
        if slack {
            let start = slab_class_for_n_fks_with_slack(n)?;
            return (start..=SH_MAX_SLAB_CLASS).find(|&c| slab_bytes(c) as usize >= packed_len);
        }
        slab_class_for_packed_len(packed_len)
    }

    #[doc(hidden)]
    pub fn read_slab(&self, body: &TableFile, class: u8, off: u64) -> Result<Vec<Fk>, StoreError> {
        if class > SH_MAX_SLAB_CLASS {
            return Err(StoreError::Corrupt("scripthash slab class overflow"));
        }
        let need = slab_bytes(class) as usize;
        let mut stack = [0u8; 2048];
        if need > stack.len() {
            return Err(StoreError::Corrupt("scripthash slab class overflow"));
        }
        let buf = &mut stack[..need];
        body.read_at(off, buf)?;
        let mut out = Vec::new();
        decode_slab_payload_into(buf, &mut out)?;
        Ok(out)
    }

    #[doc(hidden)]
    pub fn write_new_page_chain(
        &self,
        body: &TableFile,
        alloc: &mut AllocState,
        live: &[Fk],
        staged: &mut StagedSlabs,
    ) -> Result<u64, StoreError> {
        if live.is_empty() {
            return Err(StoreError::Corrupt("scripthash empty page chain"));
        }
        let chunks = sh_page_chunk_ranges(live)?;
        let n_pages = chunks.len();
        let base = self.alloc_extent(body, alloc, n_pages, &mut staged.gaps)?;
        let mut page = [0u8; SH_PAGE_SIZE];
        for (pi, &(start, end)) in chunks.iter().enumerate() {
            let off = base.saturating_add((pi as u64).saturating_mul(SH_PAGE_SIZE as u64));
            let raw: Vec<u64> = live[start..end].iter().map(|fk| fk.0).collect();
            if pi + 1 == n_pages {
                sh_page_pack_extent_last_fks(&mut page, &raw, base, n_pages as u32, 0)?;
                crate::scripthash_pages::sh_page_set_extent_creates(&mut page, live.len() as u32);
            } else {
                let next = off.saturating_add(SH_PAGE_SIZE as u64);
                sh_page_pack_fks(&mut page, &raw, next)?;
            }
            staged.write(off, &page);
        }
        Ok(base.saturating_add(((n_pages - 1) as u64).saturating_mul(SH_PAGE_SIZE as u64)))
    }

    /// Append `tail` FKs onto an existing chain ending at `last_page`.
    ///
    /// The last page is ver=2 with no successor. A zero creates counter is
    /// recounted from the pages. A ver=1 tail is not produced here: the
    /// extent check already rejected it.
    #[doc(hidden)]
    pub fn append_fks_to_pages(
        &self,
        body: &TableFile,
        alloc: &mut AllocState,
        last_page: u64,
        tail: &[Fk],
        staged: &mut StagedSlabs,
    ) -> Result<u64, StoreError> {
        if tail.is_empty() {
            return Ok(last_page);
        }
        let mut last = last_page;
        let mut page = [0u8; SH_PAGE_SIZE];
        body.read_at(last, &mut page)?;
        let Some((base, mut n)) = sh_page_extent(&page)? else {
            return Err(StoreError::Corrupt("scripthash extent last page not ver=2"));
        };
        // A low freelist tail and an inflated extent_n both sit below the
        // stamped geometric end. The tail is the page marked last.
        if !sh_page_is_last(&page)? {
            return Err(StoreError::Corrupt(
                "scripthash extent last page is not the extent tail",
            ));
        }
        let mut creates = crate::scripthash_pages::sh_page_extent_creates(&page);
        if creates == 0 {
            let existing = self.collect_entries_from(body, &ShHeadValue::extent(last_page))?;
            creates = u32::try_from(existing.len()).unwrap_or(u32::MAX);
        }
        for fk in tail {
            if !sh_page_try_append(&mut page, *fk)? {
                let new_off = self.alloc_page(body, alloc, &mut staged.gaps)?;
                sh_page_set_next(&mut page, new_off)?;
                staged.write(last, &page);
                let glued = new_off == base.saturating_add(u64::from(n) * SH_PAGE_SIZE as u64);
                n = if glued { n.saturating_add(1) } else { n };
                sh_page_pack_extent_last_fks(&mut page, &[fk.0], base, n, 0)?;
                if creates > 0 {
                    creates = creates.saturating_add(1);
                    crate::scripthash_pages::sh_page_set_extent_creates(&mut page, creates);
                }
                last = new_off;
            } else if creates > 0 {
                creates = creates.saturating_add(1);
            }
        }
        if creates > 0 {
            crate::scripthash_pages::sh_page_set_extent_creates(&mut page, creates);
        }
        staged.write(last, &page);
        Ok(last)
    }

    #[doc(hidden)]
    pub fn alloc_page(
        &self,
        body: &TableFile,
        alloc: &mut AllocState,
        gaps: &mut Vec<(u8, u64)>,
    ) -> Result<u64, StoreError> {
        self.alloc_slab(body, alloc, SH_PAGE_SLAB_CLASS, gaps)
    }

    #[doc(hidden)]
    ///
    /// An alignment gap goes to `gaps`, not the file. The caller pushes those
    /// links and publishes the bump inside its armed commit.
    pub fn alloc_extent(
        &self,
        body: &TableFile,
        alloc: &mut AllocState,
        n_pages: usize,
        gaps: &mut Vec<(u8, u64)>,
    ) -> Result<u64, StoreError> {
        if n_pages == 0 {
            return Err(StoreError::Corrupt("scripthash empty extent"));
        }
        let mut off = alloc.bump;
        let aligned = (off + 4095) & !4095;
        if aligned != off {
            gaps.extend(gap_chunks(off, aligned));
            alloc.bump = aligned;
            off = aligned;
        }
        let need = (n_pages as u64).saturating_mul(SH_PAGE_SIZE as u64);
        alloc.bump = alloc.bump.saturating_add(need);
        body.ensure_capacity(alloc.bump)?;
        Ok(off)
    }

    #[doc(hidden)]
    ///
    /// An alignment gap goes to `gaps`, not the file, as in
    /// [`Self::alloc_extent`].
    pub fn alloc_slab(
        &self,
        body: &TableFile,
        alloc: &mut AllocState,
        class: u8,
        gaps: &mut Vec<(u8, u64)>,
    ) -> Result<u64, StoreError> {
        if class > SH_MAX_CLASS {
            return Err(StoreError::Corrupt("scripthash page class overflow"));
        }
        if let Some(off) = pop_free_head(body, &mut alloc.free_head, class, alloc.bump)? {
            return Ok(off);
        }
        let need = slab_bytes(class);
        let mut off = alloc.bump;
        if class >= SH_PAGE_SLAB_CLASS {
            let aligned = (off + 4095) & !4095;
            if aligned != off {
                gaps.extend(gap_chunks(off, aligned));
                alloc.bump = aligned;
                off = aligned;
            }
        }
        alloc.bump = alloc.bump.saturating_add(need);
        body.ensure_capacity(alloc.bump)?;
        Ok(off)
    }

    #[doc(hidden)]
    pub fn free_if_paged(
        &self,
        body: &TableFile,
        alloc: &mut AllocState,
        old: &ShHeadValue,
    ) -> Result<(), StoreError> {
        match old {
            ShHeadValue::Extent { last_page } => {
                let first = paged_first_from_last(body, *last_page)?;
                follow_page_chain(body, first, SH_PAGE_CHAIN_MAX, |body, off| {
                    self.free_slab(body, alloc, SH_PAGE_SLAB_CLASS, off)
                })?;
            }
            ShHeadValue::Slab { class, off, .. } => {
                self.free_slab(body, alloc, *class, *off)?;
            }
            _ => {}
        }
        Ok(())
    }

    #[doc(hidden)]
    pub fn free_slab(
        &self,
        body: &TableFile,
        alloc: &mut AllocState,
        class: u8,
        off: u64,
    ) -> Result<(), StoreError> {
        push_free_head(body, &mut alloc.free_head, class, off)
    }

    #[doc(hidden)]
    pub fn unlink_write_home(
        &self,
        scripthash: &[u8; 32],
        home: LocatedHome,
        new_val: &ShHeadValue,
    ) -> Result<(), StoreError> {
        match home {
            LocatedHome::Main => self.unlink_write_main(scripthash, new_val),
            LocatedHome::Ingest => self.unlink_write_ingest(scripthash, new_val),
            LocatedHome::SealedOvf => self.unlink_write_sealed_ovf(scripthash, new_val),
        }
    }

    #[doc(hidden)]
    pub fn unlink_write_ingest(
        &self,
        scripthash: &[u8; 32],
        new_val: &ShHeadValue,
    ) -> Result<(), StoreError> {
        let g = self.ingest.lock().unwrap();
        let Some(g) = g.as_ref() else {
            return Err(StoreError::Corrupt("scripthash: ingest missing"));
        };
        g.stage_rewrites(&[(*scripthash, new_val.clone())])?;
        g.sync_and_drop_ahead()
    }

    #[doc(hidden)]
    pub fn unlink_write_main(
        &self,
        scripthash: &[u8; 32],
        new_val: &ShHeadValue,
    ) -> Result<(), StoreError> {
        if !self.sorted_main_updated(scripthash, new_val)? {
            self.unlink_write_ingest(scripthash, new_val)?;
        }
        Ok(())
    }

    fn sorted_main_updated(
        &self,
        scripthash: &[u8; 32],
        new_val: &ShHeadValue,
    ) -> Result<bool, StoreError> {
        let hk = head_key_from_full(scripthash);
        let si = self.shard_index(scripthash);
        let Some(slot) = self.sorted_main.get(si) else {
            return Ok(false);
        };
        let g = slot.read().unwrap();
        match g.as_ref() {
            Some(h) => h.update_value(&hk, new_val),
            None => Ok(false),
        }
    }

    #[doc(hidden)]
    pub fn unlink_write_sealed_ovf(
        &self,
        scripthash: &[u8; 32],
        new_val: &ShHeadValue,
    ) -> Result<(), StoreError> {
        let hk = head_key_from_full(scripthash);
        let g = self.sealed_ovf.lock().unwrap();
        let mut hit = false;
        for h in g.iter().rev() {
            if h.update_value(&hk, new_val)? {
                hit = true;
                break;
            }
        }
        drop(g);
        if !hit {
            if let Some(l1) = self.ovf_l1.lock().unwrap().as_ref() {
                if l1.head.update_value(&hk, new_val)? {
                    hit = true;
                }
            }
        }
        if !hit {
            return Err(StoreError::Corrupt(
                "scripthash: sealed ovf unlink missed home",
            ));
        }
        Ok(())
    }

    /// Write and sync every body and head. A sync with no write started
    /// since it began clears [`SH_DIRTY_NAME`].
    pub fn flush(&self) -> Result<(), StoreError> {
        let gen = self.dirty.quiet_gen();
        self.flush_synced()?;
        self.dirty.clear_if_unchanged(&self.store_dir, gen)
    }

    /// Process exit. A dirty index gets the full synced [`Self::flush`], which
    /// clears [`SH_DIRTY_NAME`] so the next start trusts it. A clean one has
    /// nothing unsynced to lose and keeps the fast unsynced writeback.
    pub fn flush_for_close(&self) -> Result<(), StoreError> {
        if self.dirty.is_marked() {
            self.flush()
        } else {
            self.flush_async()
        }
    }

    /// After a wipe for rebuild, sync the reset bodies and heads, then drop
    /// [`SH_DIRTY_NAME`] once the wipe is durable.
    pub fn clear_dirty_after_wipe(&self) -> Result<(), StoreError> {
        self.flush_synced()?;
        self.dirty.clear_after_wipe(&self.store_dir)
    }

    /// Run a write that can land in place with [`SH_DIRTY_NAME`] set for its
    /// whole span. An error keeps the mark until a wipe.
    pub(crate) fn dirty_write<T>(
        &self,
        write: impl FnOnce() -> Result<T, StoreError>,
    ) -> Result<T, StoreError> {
        let _scope = self.begin_dirty_write()?;
        let out = write();
        if out.is_err() {
            self.dirty.hold();
        }
        out
    }

    /// Mark [`SH_DIRTY_NAME`] before a write that can land in place. Hold the
    /// guard until the write returns.
    #[doc(hidden)]
    pub fn begin_dirty_write(&self) -> Result<std::sync::RwLockReadGuard<'_, ()>, StoreError> {
        self.dirty.begin(&self.store_dir)
    }

    fn flush_synced(&self) -> Result<(), StoreError> {
        self.persist_all_allocs()?;
        self.flush_all_bodies()?;
        if let Some(h) = self.ingest.lock().unwrap().as_ref() {
            h.flush()?;
        }
        for slot in self.sorted_main.iter() {
            if let Some(h) = slot.read().unwrap().as_ref() {
                h.flush()?;
            }
        }
        for h in self.sealed_ovf.lock().unwrap().iter() {
            h.flush()?;
        }
        if let Some(l1) = self.ovf_l1.lock().unwrap().as_ref() {
            l1.head.flush()?;
        }
        Ok(())
    }

    pub fn flush_async(&self) -> Result<(), StoreError> {
        self.persist_all_allocs()?;
        self.flush_all_bodies_async()?;
        if let Some(h) = self.ingest.lock().unwrap().as_ref() {
            h.flush_async()?;
        }
        for h in self.sealed_ovf.lock().unwrap().iter() {
            h.flush()?;
        }
        if let Some(l1) = self.ovf_l1.lock().unwrap().as_ref() {
            l1.head.flush()?;
        }
        Ok(())
    }

    /// Pack one prefix shard onto that shard's live body (no temp file).
    pub fn pack_shard_session(
        &self,
        shard: usize,
    ) -> Result<ScriptHashBulkSession<'_>, StoreError> {
        let payload0 = payload_start(FILE_HEADER_LEN);
        let (bump, prior_live, free_head) = {
            let a = self.shard_alloc(shard).lock().unwrap();
            (a.bump.max(payload0), a.live_count, a.free_head)
        };
        let body = self.shard_body(shard);
        body.ensure_capacity(bump)?;
        if bump > body.logical_len() {
            body.set_logical_len(bump)?;
        }
        Ok(ScriptHashBulkSession {
            table: self,
            bump,
            live_count: 0,
            committed_live_count: prior_live,
            active_shard: Some(shard),
            recs: Vec::new(),
            slot_words: Vec::new(),
            body_buf: Vec::with_capacity(BULK_BODY_FLUSH),
            body_write_off: bump,
            keys_written: 0,
            shards_flushed: 0,
            body_flush_ns: 0,
            pack_ns: 0,
            head_fill_ns: 0,
            peak_table_bytes: 0,
            open_key: None,
            fk_scratch: Vec::with_capacity(512),
            max_fk: 0,
            free_head,
        })
    }

    /// Number of SH head shards (1 on Tiny, 64 on mainnet).
    pub fn head_shard_count(&self) -> usize {
        self.n_shards
    }

    /// Current body bump (complete-shard HWM). Shared file: the one bump.
    /// Dir variant: shard 0's bump (each shard file has its own SHAL).
    pub fn alloc_bump(&self) -> u64 {
        self.allocs[0].lock().unwrap().bump
    }

    /// Pass-1 BDZ singles already inline in `head/NN` (not yet packed body).
    pub fn set_extract_inline_creates(&self, shard: usize, n: u64) -> Result<(), StoreError> {
        let body = self.shard_body(shard);
        let mut g = self.shard_alloc(shard).lock().unwrap();
        g.live_count = n;
        write_alloc_header(body, &g)
    }

    /// Seal the already-written shard body and install `scripthash.head/NN`.
    pub fn publish_packed_shard(&self, shard: usize, pack: ShShardPack) -> Result<u64, StoreError> {
        let (live, free_head) = {
            let a = self.shard_alloc(shard).lock().unwrap();
            (a.live_count, a.free_head)
        };
        let bump = pack.bump;
        let n_shards = self.n_shards;
        let path = sorted_main_shard_path(&self.store_dir, shard, n_shards);
        if let Some(parent) = path.parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        let sealed = if MphfHead::exists(&path) {
            let h = MphfHead::open(&path)?;
            if !pack.slot_words.is_empty() {
                h.rewrite_val_slots(&pack.slot_words)?;
            }
            h
        } else {
            MphfHead::write_pack8(&path, &pack.recs)?
        };
        self.install_sorted_main(shard, sealed);
        let body = self.shard_body(shard);
        if bump > body.logical_len() {
            body.set_logical_len(bump)?;
        }
        let state = AllocState {
            live_count: live,
            bump,
            free_head,
        };
        write_alloc_header(body, &state)?;
        *self.shard_alloc(shard).lock().unwrap() = state;
        self.note_shard_packed(shard)?;
        self.drop_ingest_covered_by_packed_main(shard)?;
        Ok(bump)
    }

    /// A tip append while this shard is unsealed lands on ingest (main MPHF is
    /// not loaded). Lookup reads ingest first, so that row would hide the
    /// packed chain. Clear ingest keys this shard's main now owns. Ingest body
    /// bytes stay; only the head slot is soft-cleared.
    #[doc(hidden)]
    pub fn drop_ingest_covered_by_packed_main(&self, shard: usize) -> Result<(), StoreError> {
        // Mainnet ingest is 2^25 slots. Known-empty has no row that can hide
        // the packed chain. Walking it once per shard holds tip entry past
        // the RPC cookie window.
        {
            let g = self.ingest.lock().unwrap();
            match g.as_ref() {
                None => return Ok(()),
                Some(h) if h.is_known_empty() => return Ok(()),
                Some(_) => {}
            }
        }
        let Some(slot) = self.sorted_main.get(shard) else {
            return Ok(());
        };
        let main = slot.read().unwrap();
        let Some(head) = main.as_ref() else {
            return Ok(());
        };
        let mut covered = Vec::new();
        self.ingest
            .lock()
            .unwrap()
            .as_ref()
            .ok_or(StoreError::Corrupt("scripthash: ingest missing"))?
            .for_each_occupied(|key, _val| {
                if self.shard_index(&key) != shard {
                    return Ok(());
                }
                let hk = head_key_from_full(&key);
                if head.get(&hk)?.is_some() {
                    covered.push(key);
                }
                Ok(())
            })?;
        drop(main);
        if covered.is_empty() {
            return Ok(());
        }
        let ingest = self.ingest.lock().unwrap();
        let Some(ingest) = ingest.as_ref() else {
            return Err(StoreError::Corrupt("scripthash: ingest missing"));
        };
        let rows: Vec<([u8; 32], ShHeadValue)> = covered
            .iter()
            .map(|key| (*key, ShHeadValue::Empty))
            .collect();
        ingest.stage_rewrites(&rows)?;
        ingest.sync_and_drop_ahead()
    }

    #[doc(hidden)]
    pub fn note_shard_packed(&self, shard: usize) -> Result<(), StoreError> {
        let path = sorted_main_shard_path(&self.store_dir, shard, self.n_shards);
        write_shard_pack_mark(&path)?;
        if let Some(b) = self.pack_marked.get(shard) {
            b.store(true, Ordering::Release);
        }
        Ok(())
    }
}

/// Pack writer for one scripthash head shard.
///
/// Sequential slabs land in a 16 MiB `body_buf` and reach disk in one
/// `write_at` per flush. [`ScriptHashTable::publish_packed_shard`] installs
/// the MPHF.
pub struct ScriptHashBulkSession<'a> {
    pub table: &'a ScriptHashTable,
    pub bump: u64,
    pub live_count: u64,
    pub committed_live_count: u64,
    pub active_shard: Option<usize>,
    /// Packed recs sealed by [`ScriptHashTable::publish_packed_shard`].
    pub recs: Vec<(crate::scripthash_layout::ShHeadKey, u64)>,
    /// MPHF slot + pack8 for pass-2 body writes (grouped by slot).
    pub slot_words: Vec<(u32, u64)>,
    /// Sequential slab bytes; flushed at [`BULK_BODY_FLUSH`] or before a
    /// direct `write_at` (freelist reuse, page, gap carve).
    pub body_buf: Vec<u8>,
    pub body_write_off: u64,
    pub keys_written: u64,
    pub shards_flushed: u32,
    /// Wall time spent in body `write_at` flushes.
    pub body_flush_ns: u64,
    /// Wall time spent in [`Self::finish_key`] (one sample per unique key).
    pub pack_ns: u64,
    /// Wall time spent installing head shards (write of live image).
    pub head_fill_ns: u64,
    /// Peak packed-rec buffer (bytes) — test/bench meter.
    pub peak_table_bytes: usize,
    /// In-flight key: at most one page of FKs (streaming megakey).
    pub open_key: Option<BulkOpenKey>,
    /// Reused FK page buffer (one alloc per session, not per key).
    pub fk_scratch: Vec<u64>,
    pub max_fk: u64,
    pub free_head: [u64; SH_MAX_CLASS as usize + 1],
}

/// One shard packed onto its live body, ready for ordered head publish.
pub struct ShShardPack {
    pub recs: Vec<(crate::scripthash_layout::ShHeadKey, u64)>,
    pub slot_words: Vec<(u32, u64)>,
    pub creates: u64,
    pub max_fk: u64,
    pub keys: u64,
    pub bump: u64,
    pub body_flush_ns: u64,
    pub pack_ns: u64,
    /// Fuse-hit postings with one unique create_fk (left inline; no body).
    pub fp_singles: u64,
}

/// One unfinished key in [`ScriptHashBulkSession`] (≤ one delta page of FKs).
#[derive(Clone, Copy)]
#[doc(hidden)]
pub enum BulkGroup {
    Script([u8; 32]),
    Slot(u32),
}

#[doc(hidden)]
pub struct BulkOpenKey {
    pub group: BulkGroup,
    pub buf: Vec<u64>,
    pub stream_used: usize,
    pub n_total: u32,
    pub first_page: Option<u64>,
    pub last_fk: Option<u64>,
}

#[doc(hidden)]
pub const BULK_BODY_FLUSH: usize = 16 * 1024 * 1024;

impl<'a> ScriptHashBulkSession<'a> {
    /// Unique keys packed so far.
    pub fn keys_written(&self) -> u64 {
        self.keys_written
    }

    pub fn reserve_pack_recs(&mut self, n: usize) {
        self.recs.reserve(n);
    }

    #[doc(hidden)]
    pub fn take_fk_scratch(&mut self) -> Vec<u64> {
        let mut buf = std::mem::take(&mut self.fk_scratch);
        if buf.capacity() < 512 {
            buf.reserve(512);
        }
        buf
    }

    #[doc(hidden)]
    pub fn return_fk_scratch(&mut self, mut buf: Vec<u64>) {
        buf.clear();
        self.fk_scratch = buf;
    }

    /// Head shards fully installed so far.
    pub fn shards_flushed(&self) -> u32 {
        self.shards_flushed
    }

    /// FKs buffered for the open key (never more than one page).
    pub fn buffered_fks(&self) -> usize {
        self.open_key.as_ref().map(|k| k.buf.len()).unwrap_or(0)
    }

    #[doc(hidden)]
    pub fn body(&self) -> &TableFile {
        match self.active_shard {
            Some(si) => self.table.shard_body(si),
            None => self.table.body(),
        }
    }

    #[doc(hidden)]
    pub fn body_and_free(&mut self) -> (&TableFile, &mut [u64; SH_MAX_CLASS as usize + 1]) {
        let body = match self.active_shard {
            Some(si) => self.table.shard_body(si),
            None => self.table.body(),
        };
        (body, &mut self.free_head)
    }

    #[doc(hidden)]
    pub fn persist_session_alloc(&self, live_count: u64, bump: u64) -> Result<(), StoreError> {
        let body = self.body();
        if bump > body.logical_len() {
            body.set_logical_len(bump)?;
        }
        let state = AllocState {
            live_count,
            bump,
            free_head: self.free_head,
        };
        write_alloc_header(body, &state)?;
        match self.active_shard {
            Some(si) => *self.table.shard_alloc(si).lock().unwrap() = state,
            None => *self.table.allocs[0].lock().unwrap() = state,
        }
        Ok(())
    }

    /// Seal the session into a remappable blob (no live head write).
    pub fn finish_pack(mut self) -> Result<ShShardPack, StoreError> {
        self.finish_key()?;
        self.flush_body()?;
        let persist_live = self.committed_live_count.saturating_add(self.live_count);
        self.persist_session_alloc(persist_live, self.bump)?;
        let pack = ShShardPack {
            recs: std::mem::take(&mut self.recs),
            slot_words: std::mem::take(&mut self.slot_words),
            creates: self.live_count,
            max_fk: self.max_fk,
            keys: self.keys_written,
            bump: self.bump,
            body_flush_ns: self.body_flush_ns,
            pack_ns: self.pack_ns,
            fp_singles: 0,
        };
        Ok(pack)
    }

    /// Stream one **strictly increasing** create_fk for `key`.
    ///
    /// Callers must present keys in non-decreasing scripthash order. A full
    /// page is written only when the next FK proves it is not last (so `next`
    /// is known). Adjacent duplicate FKs are skipped.
    pub fn push_sorted_fk(&mut self, key: [u8; 32], fk: Fk) -> Result<(), StoreError> {
        if fk.is_null() {
            return Ok(());
        }
        if self
            .open_key
            .as_ref()
            .is_some_and(|o| !matches!(o.group, BulkGroup::Script(k) if k == key))
        {
            self.finish_key()?;
        }
        if self.open_key.is_none() {
            if !self.prepare_stream_key(key)? {
                return Ok(());
            }
            let buf = self.take_fk_scratch();
            self.open_key = Some(BulkOpenKey {
                group: BulkGroup::Script(key),
                buf,
                stream_used: 0,
                n_total: 0,
                first_page: None,
                last_fk: None,
            });
        }
        self.push_open_fk(fk)
    }

    #[doc(hidden)]
    pub fn push_open_fk(&mut self, fk: Fk) -> Result<(), StoreError> {
        let add = {
            let open = self
                .open_key
                .as_ref()
                .expect("open_key after prepare_stream_key");
            if let Some(prev) = open.last_fk {
                if fk.0 == prev {
                    return Ok(());
                }
                if fk.0 < prev {
                    return Err(StoreError::Corrupt(
                        "scripthash bulk stream: create_fk not strictly increasing",
                    ));
                }
                if open.buf.is_empty() {
                    uleb128_len(fk.0)
                } else {
                    let add = uleb128_len(fk.0 - prev);
                    if open.stream_used.saturating_add(add) > SH_PAGE_STREAM_MAX {
                        self.write_open_full_page_with_next()?;
                        uleb128_len(fk.0)
                    } else {
                        add
                    }
                }
            } else {
                uleb128_len(fk.0)
            }
        };
        let open = self
            .open_key
            .as_mut()
            .expect("open_key after prepare_stream_key");
        open.stream_used = open.stream_used.saturating_add(add);
        open.buf.push(fk.0);
        open.last_fk = Some(fk.0);
        open.n_total = open.n_total.saturating_add(1);
        if fk.0 > self.max_fk {
            self.max_fk = fk.0;
        }
        Ok(())
    }

    /// Stream one create_fk for MPHF `slot` (pass-2 pack; grouped by slot).
    pub fn push_sorted_slot_fk(&mut self, slot: u32, fk: Fk) -> Result<(), StoreError> {
        if fk.is_null() {
            return Ok(());
        }
        if self
            .open_key
            .as_ref()
            .is_some_and(|o| !matches!(o.group, BulkGroup::Slot(s) if s == slot))
        {
            self.finish_key()?;
        }
        if self.open_key.is_none() {
            let buf = self.take_fk_scratch();
            self.open_key = Some(BulkOpenKey {
                group: BulkGroup::Slot(slot),
                buf,
                stream_used: 0,
                n_total: 0,
                first_page: None,
                last_fk: None,
            });
        }
        self.push_open_fk(fk)
    }

    /// Seal the open key (inline / slab / last page).
    pub fn finish_key(&mut self) -> Result<(), StoreError> {
        if self.open_key.is_none() {
            return Ok(());
        }
        let t0 = std::time::Instant::now();
        let flush_before = self.body_flush_ns;
        let r = self.finish_open_key();
        let elapsed = t0.elapsed().as_nanos() as u64;
        let flush_delta = self.body_flush_ns.saturating_sub(flush_before);
        self.pack_ns = self
            .pack_ns
            .saturating_add(elapsed.saturating_sub(flush_delta));
        r
    }

    #[doc(hidden)]
    pub fn finish_open_key(&mut self) -> Result<(), StoreError> {
        let Some(open) = self.open_key.take() else {
            return Ok(());
        };
        if open.n_total == 0 {
            self.return_fk_scratch(open.buf);
            return Ok(());
        }
        let n = open.n_total;
        let val = if open.first_page.is_none() {
            if n <= SH_INLINE_CAP as u32 {
                ShHeadValue::inline_one(Fk(open.buf[0]))
            } else {
                self.bulk_write_slab(&open.buf)?
            }
        } else {
            let last = if open.buf.is_empty() {
                return Err(StoreError::Corrupt(
                    "scripthash bulk stream: paged key missing last page",
                ));
            } else {
                let fks: Vec<Fk> = open.buf.iter().copied().map(Fk).collect();
                let chunks = sh_page_chunk_ranges(&fks)?;
                let mut last = 0u64;
                for (i, &(start, end)) in chunks.iter().enumerate() {
                    let part: Vec<u64> = fks[start..end].iter().map(|fk| fk.0).collect();
                    let is_last = i + 1 == chunks.len();
                    last = self.write_page(
                        &part,
                        !is_last,
                        if is_last { open.first_page } else { None },
                    )?;
                }
                last
            };
            ShHeadValue::extent(last)
        };
        self.live_count = self.live_count.saturating_add(u64::from(n));
        self.keys_written = self.keys_written.saturating_add(1);
        let word = pack8(&val)?;
        match open.group {
            BulkGroup::Script(key) => {
                self.recs.push((head_key_from_full(&key), word));
                self.peak_table_bytes = self
                    .peak_table_bytes
                    .max(self.recs.len().saturating_mul(24));
            }
            BulkGroup::Slot(slot) => {
                self.slot_words.push((slot, word));
            }
        }
        self.return_fk_scratch(open.buf);
        Ok(())
    }

    #[doc(hidden)]
    pub fn prepare_stream_key(&mut self, key: [u8; 32]) -> Result<bool, StoreError> {
        let si = self.table.shard_index(&key);
        if self.active_shard != Some(si) {
            return Err(StoreError::Corrupt(
                "scripthash pack session saw a key for a different shard",
            ));
        }
        Ok(true)
    }

    #[doc(hidden)]
    pub fn write_open_full_page_with_next(&mut self) -> Result<(), StoreError> {
        let mut buf = self
            .open_key
            .as_mut()
            .map(|o| std::mem::take(&mut o.buf))
            .unwrap_or_default();
        let off = self.write_page(&buf, true, None)?;
        buf.clear();
        if let Some(open) = self.open_key.as_mut() {
            if open.first_page.is_none() {
                open.first_page = Some(off);
            }
            open.stream_used = 0;
            open.buf = buf;
        } else {
            self.return_fk_scratch(buf);
        }
        Ok(())
    }

    /// Write one page at the aligned bump. `has_next` sets `next` to the following page.
    #[doc(hidden)]
    pub fn write_page(
        &mut self,
        fks: &[u64],
        has_next: bool,
        chain_first: Option<u64>,
    ) -> Result<u64, StoreError> {
        self.flush_body()?;
        debug_assert!(self.body_buf.is_empty());
        let base = self.align_bump_for_page()?;
        let next = if has_next {
            base.saturating_add(SH_PAGE_SIZE as u64)
        } else {
            0
        };
        let end = base.saturating_add(SH_PAGE_SIZE as u64);
        self.ensure_body_capacity(end)?;
        let mut page = [0u8; SH_PAGE_SIZE];
        if has_next {
            sh_page_pack_fks(&mut page, fks, next)?;
        } else {
            let first = chain_first.unwrap_or(base);
            let n = ((base.saturating_sub(first) / SH_PAGE_SIZE as u64).saturating_add(1)) as u32;
            sh_page_pack_extent_last_fks(&mut page, fks, first, n, 0)?;
        }
        debug_assert!(self.body_buf.is_empty());
        self.body().write_at(base, &page)?;
        self.bump = end;
        self.body_write_off = end;
        Ok(base)
    }

    #[doc(hidden)]
    pub fn align_bump_for_page(&mut self) -> Result<u64, StoreError> {
        self.flush_body()?;
        debug_assert!(self.body_buf.is_empty());
        let aligned = (self.bump + 4095) & !4095;
        if aligned != self.bump {
            let bump = self.bump;
            let (body, free) = self.body_and_free();
            carve_gap_into_freelist(body, free, bump, aligned)?;
            self.bump = aligned;
            self.body_write_off = aligned;
        }
        Ok(aligned)
    }

    #[doc(hidden)]
    pub fn alloc_reloc_class(&mut self, class: u8) -> Result<u64, StoreError> {
        {
            let bump = self.bump;
            let (body, free) = self.body_and_free();
            if let Some(off) = pop_free_head(body, free, class, bump)? {
                return Ok(off);
            }
        }
        let need = slab_bytes(class);
        let off = self.bump;
        self.bump = self.bump.saturating_add(need);
        self.ensure_body_capacity(self.bump)?;
        Ok(off)
    }

    /// Write one exact-class slab (byte-fit ULEB payload). Pages if stream > class 6.
    #[doc(hidden)]
    pub fn bulk_write_slab(&mut self, fks: &[u64]) -> Result<ShHeadValue, StoreError> {
        let n = fks.len() as u32;
        let mut payload = [0u8; 2048];
        let packed_len = match encode_slab_payload_into(&mut payload, fks) {
            Ok(len) => len,
            Err(StoreError::Corrupt(msg))
                if msg == "uleb128 dest short" || msg == "scripthash slab dest short" =>
            {
                let last = self.bulk_write_page_chain(fks)?;
                return Ok(ShHeadValue::extent(last));
            }
            Err(e) => return Err(e),
        };
        let Some(class) = slab_class_for_packed_len(packed_len) else {
            let last = self.bulk_write_page_chain(fks)?;
            return Ok(ShHeadValue::extent(last));
        };
        let off = self.alloc_reloc_class(class)?;
        let need = slab_bytes(class) as usize;
        let mut buf = [0u8; 2048];
        buf[..packed_len].copy_from_slice(&payload[..packed_len]);
        self.write_body_bytes(off, &buf[..need])?;
        Ok(ShHeadValue::slab(class, n as u16, off))
    }

    /// Write a full page chain at the aligned bump. Returns (first, last).
    #[doc(hidden)]
    pub fn bulk_write_page_chain(&mut self, fks: &[u64]) -> Result<u64, StoreError> {
        if fks.is_empty() {
            return Err(StoreError::Corrupt("scripthash bulk page chain empty"));
        }
        self.flush_body()?;
        debug_assert!(self.body_buf.is_empty());
        let base = self.align_bump_for_page()?;
        let wrapped: Vec<Fk> = fks.iter().copied().map(Fk).collect();
        let chunks = sh_page_chunk_ranges(&wrapped)?;
        let n_pages = chunks.len();
        let end = base.saturating_add((n_pages as u64).saturating_mul(SH_PAGE_SIZE as u64));
        self.ensure_body_capacity(end)?;
        let mut page = [0u8; SH_PAGE_SIZE];
        for (pi, &(start, end_i)) in chunks.iter().enumerate() {
            let off = base + (pi as u64) * (SH_PAGE_SIZE as u64);
            if pi + 1 < n_pages {
                let next = off + SH_PAGE_SIZE as u64;
                sh_page_pack_fks(&mut page, &fks[start..end_i], next)?;
            } else {
                sh_page_pack_extent_last_fks(
                    &mut page,
                    &fks[start..end_i],
                    base,
                    n_pages as u32,
                    0,
                )?;
                crate::scripthash_pages::sh_page_set_extent_creates(&mut page, fks.len() as u32);
            }
            debug_assert!(self.body_buf.is_empty());
            self.body().write_at(off, &page)?;
        }
        self.bump = end;
        self.body_write_off = end;
        Ok(base + ((n_pages - 1) as u64) * (SH_PAGE_SIZE as u64))
    }

    #[doc(hidden)]
    pub fn ensure_body_capacity(&self, need: u64) -> Result<(), StoreError> {
        self.body().ensure_capacity(need)
    }

    #[doc(hidden)]
    pub fn write_body_bytes(&mut self, off: u64, bytes: &[u8]) -> Result<(), StoreError> {
        let sequential = off
            == self
                .body_write_off
                .saturating_add(self.body_buf.len() as u64);
        if sequential {
            self.body_buf.extend_from_slice(bytes);
            if self.body_buf.len() >= BULK_BODY_FLUSH {
                self.flush_body()?;
            }
            return Ok(());
        }
        self.flush_body()?;
        debug_assert!(self.body_buf.is_empty());
        self.body().write_at(off, bytes)
    }

    /// One `write_at` of pending slab bytes; advances `body_write_off`.
    /// Alloc HWM persist stays at shard seal.
    #[doc(hidden)]
    pub fn flush_body(&mut self) -> Result<(), StoreError> {
        if self.body_buf.is_empty() {
            return Ok(());
        }
        let t0 = std::time::Instant::now();
        let end = self.body_write_off + self.body_buf.len() as u64;
        self.ensure_body_capacity(end)?;
        self.body().write_at(self.body_write_off, &self.body_buf)?;
        self.body_write_off = end;
        self.body_buf.clear();
        self.body_flush_ns = self
            .body_flush_ns
            .saturating_add(t0.elapsed().as_nanos() as u64);
        Ok(())
    }
}

#[doc(hidden)]
pub fn write_alloc_header(body: &TableFile, state: &AllocState) -> Result<(), StoreError> {
    // Stop at the freelist. Bytes after that hold an in-progress unlink undo.
    let mut buf = vec![0u8; 24 + 8 * (SH_MAX_CLASS as usize + 1)];
    buf[0..4].copy_from_slice(&SH_ALLOC_MAGIC);
    buf[4..6].copy_from_slice(&SH_ALLOC_VERSION.to_le_bytes());
    buf[8..16].copy_from_slice(&state.live_count.to_le_bytes());
    buf[16..24].copy_from_slice(&state.bump.to_le_bytes());
    let mut off = 24usize;
    for h in &state.free_head {
        if off + 8 > buf.len() {
            break;
        }
        buf[off..off + 8].copy_from_slice(&h.to_le_bytes());
        off += 8;
    }
    body.write_at(FILE_HEADER_LEN as u64, &buf)
}

/// Read SHAL alloc page. Returns `(state, on_disk_version)`.
///
/// **v1** (schema-13 slabs) and **v2** (schema-14 page chains) share the same
/// header field layout. An empty older header is reset; a durable pre-v3 body refuses.
#[doc(hidden)]
pub fn read_alloc_header(body: &TableFile) -> Result<(AllocState, u16), StoreError> {
    let mut buf = [0u8; SH_ALLOC_HEADER_LEN];
    let avail = body
        .logical_len()
        .saturating_sub(FILE_HEADER_LEN as u64)
        .min(SH_ALLOC_HEADER_LEN as u64) as usize;
    if avail < 24 {
        return Err(StoreError::Corrupt(
            "scripthash body missing alloc header (expected SHAL)",
        ));
    }
    body.read_at(FILE_HEADER_LEN as u64, &mut buf[..avail])?;
    if buf[0..4] != SH_ALLOC_MAGIC {
        return Err(StoreError::Corrupt(
            "scripthash body not hybrid (no SHAL magic)",
        ));
    }
    let ver = u16::from_le_bytes([buf[4], buf[5]]);
    // v1 = schema-13 slabs; v2 = schema-14 pages; v3 = schema-15. Same fields.
    if ver != 1 && ver != 2 && ver != SH_ALLOC_VERSION {
        return Err(StoreError::Corrupt("unsupported scripthash alloc version"));
    }
    let live_count = le_u64!(buf, 8);
    let bump = le_u64!(buf, 16);
    let logical = body.logical_len();
    if bump < SH_PREFIX_PAGE as u64 || bump > logical {
        return Err(StoreError::Corrupt("scripthash alloc bump out of range"));
    }
    let mut free_head = [0u64; SH_MAX_CLASS as usize + 1];
    let mut off = 24usize;
    for (class, h) in free_head.iter_mut().enumerate() {
        if off + 8 > avail {
            break;
        }
        let mut word = [0u8; 8];
        word.copy_from_slice(&buf[off..off + 8]);
        *h = u64::from_le_bytes(word);
        if *h != 0
            && (*h < SH_PREFIX_PAGE as u64 || h.saturating_add(slab_bytes(class as u8)) > bump)
        {
            return Err(StoreError::Corrupt("scripthash free head out of range"));
        }
        off += 8;
    }
    Ok((
        AllocState {
            live_count,
            bump,
            free_head,
        },
        ver,
    ))
}

/// On-disk SHAL version field (after RBT1 file header).
#[cfg(debug_assertions)]
#[doc(hidden)]
pub fn read_alloc_version_on_disk(body: &TableFile) -> Result<u16, StoreError> {
    let mut buf = [0u8; 6];
    body.read_at(FILE_HEADER_LEN as u64, &mut buf)?;
    if buf[0..4] != SH_ALLOC_MAGIC {
        return Err(StoreError::Corrupt(
            "scripthash body not hybrid (no SHAL magic)",
        ));
    }
    Ok(u16::from_le_bytes([buf[4], buf[5]]))
}
