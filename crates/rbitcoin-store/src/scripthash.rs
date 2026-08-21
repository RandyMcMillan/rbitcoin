//! Class B scripthash multimap (Electrum: SHA256(scriptPubKey)).
//!
//! Hybrid layout (schema 15): head key = 16 B hash prefix; value = two u64s
//! (≤2 inline, geometric **slab**, or megakey first/last **4 KiB page** offs).
//! Body slabs pack ULEB128 fk deltas; vouts expanded from Class A at query.

use crate::compact::uleb128_len;
use crate::error::StoreError;
use crate::file::{GrowPolicy, TableFile, FILE_HEADER_LEN};
use crate::fuse8_filter::SealedFuse8;
use crate::hashhead::HeadRole;
use crate::hashhead::HeadScale;
use crate::io_backend::ReadIoBackend;
#[cfg(test)]
use crate::scripthash_head::ShardedScriptHashHead;
use crate::scripthash_head::{
    prefix_shard_of, sh_per_shard_key_budget, sh_unique_hint_default, ScriptHashHead,
};
use crate::scripthash_layout::{
    head_key_from_full, pack8_bytes, payload_start, slab_bytes, ShEntry, ShHeadValue,
    SH_ALLOC_HEADER_LEN, SH_ALLOC_MAGIC, SH_ALLOC_VERSION, SH_HEAD_VALUE_LEN, SH_INLINE_CAP,
    SH_MAX_CLASS, SH_MAX_SLAB_CLASS, SH_PAGE_SLAB_CLASS,
};
use crate::scripthash_mphf::{self, mix_key16, MphfHead};
use crate::scripthash_overflow::wipe_legacy_fullsize_overflow;
use crate::scripthash_pages::{
    sh_page_as_array, sh_page_as_array_mut, sh_page_chunk_ranges, sh_page_decode_slice,
    sh_page_extent, sh_page_first_off, sh_page_init_empty, sh_page_is_last, sh_page_last_fk,
    sh_page_next, sh_page_pack, sh_page_pack_extent_last, sh_page_set_extent, sh_page_set_last,
    sh_page_set_next, sh_page_try_append, SH_PAGE_SIZE, SH_PAGE_STREAM_MAX,
};
use crate::scripthash_slabs::{
    decode_slab_payload, encode_slab_payload, slab_class_for_n_fks_with_slack,
    slab_class_for_packed_len, SH_MEGAKEY_MIN_FKS,
};
use crate::scripthash_sorted_head::{SortedHead, SortedHeadFilter};
use crate::sharded_hashhead::shard_count_for_role;
use crate::sorted_run::{
    list_fanin_reduce_outputs, list_materialize_claims, list_runs, load_fanin_checkpoint,
};
use bitcoin_hashes::{sha256, Hash};
use rbitcoin_primitives::{Fk, TableKind};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Mutex, RwLock};

/// Durable cold-materialize resume marker (next to `scripthash.head`).
pub const COLD_PROGRESS_NAME: &str = "scripthash.cold_progress";
const COLD_PROGRESS_MAGIC: &[u8; 8] = b"SHCOLDP1";
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

    pub fn load(store_dir: &Path) -> Result<Option<Self>, StoreError> {
        let p = Self::path(store_dir);
        let Ok(buf) = std::fs::read(&p) else {
            return Ok(None);
        };
        if buf.len() < 8 + 4 + 8 + 8 + 8 || &buf[0..8] != COLD_PROGRESS_MAGIC {
            return Ok(None);
        }
        let next_shard = u32::from_le_bytes(buf[8..12].try_into().unwrap());
        let body_bump = u64::from_le_bytes(buf[12..20].try_into().unwrap());
        let live_count = u64::from_le_bytes(buf[20..28].try_into().unwrap());
        let keys_written = u64::from_le_bytes(buf[28..36].try_into().unwrap());
        Ok(Some(Self {
            next_shard,
            body_bump,
            live_count,
            keys_written,
        }))
    }

    pub fn store(&self, store_dir: &Path) -> Result<(), StoreError> {
        let p = Self::path(store_dir);
        static TMP_SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let tmp = store_dir.join(format!(
            "{COLD_PROGRESS_NAME}.{}.tmp",
            TMP_SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        ));
        let mut buf = Vec::with_capacity(36);
        buf.extend_from_slice(COLD_PROGRESS_MAGIC);
        buf.extend_from_slice(&self.next_shard.to_le_bytes());
        buf.extend_from_slice(&self.body_bump.to_le_bytes());
        buf.extend_from_slice(&self.live_count.to_le_bytes());
        buf.extend_from_slice(&self.keys_written.to_le_bytes());
        std::fs::write(&tmp, &buf).map_err(|e| StoreError::io(&tmp, e))?;
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

    pub fn clear(store_dir: &Path) {
        let _ = std::fs::remove_file(Self::path(store_dir));
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
    u64::from_le_bytes(buf[0..8].try_into().unwrap_or([0; 8]))
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
    let tmp = store_dir.join(format!("{INCLUDE_HWM_NAME}.tmp"));
    std::fs::write(&tmp, max_create_fk.to_le_bytes()).map_err(|e| StoreError::io(&tmp, e))?;
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

/// Electrum scripthash = SHA256(scriptPubKey) (binary; API often reverses for hex).
pub fn script_hash(script: &[u8]) -> [u8; 32] {
    sha256::Hash::hash(script).to_byte_array()
}

pub use crate::scripthash_layout::ShEntry as ScriptHashEntry;

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
    pub fn entry(&self) -> ShEntry {
        ShEntry::new(self.create_tx_fk)
    }

    pub fn from_entry(scripthash: [u8; 32], e: ShEntry) -> Self {
        Self {
            scripthash,
            create_tx_fk: e.create_tx_fk,
        }
    }

    pub fn from_fk(scripthash: [u8; 32], create_tx_fk: Fk) -> Self {
        Self::from_entry(scripthash, ShEntry::new(create_tx_fk))
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

/// Body slab allocator state (persisted in alloc header).
#[derive(Clone, Copy)]
struct AllocState {
    live_count: u64,
    bump: u64,
    free_head: [u64; SH_MAX_CLASS as usize + 1],
}

fn largest_reloc_class_le(bytes: u64) -> Option<u8> {
    (0..=SH_MAX_SLAB_CLASS)
        .rev()
        .find(|&c| slab_bytes(c) <= bytes)
}

fn push_free_head(
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

fn pop_free_head(
    body: &TableFile,
    free_head: &mut [u64; SH_MAX_CLASS as usize + 1],
    class: u8,
) -> Result<Option<u64>, StoreError> {
    let idx = class as usize;
    if idx >= free_head.len() || free_head[idx] == 0 {
        return Ok(None);
    }
    let off = free_head[idx];
    let mut next = [0u8; 8];
    body.read_at(off, &mut next)?;
    free_head[idx] = u64::from_le_bytes(next);
    Ok(Some(off))
}

fn carve_gap_into_freelist(
    body: &TableFile,
    free_head: &mut [u64; SH_MAX_CLASS as usize + 1],
    from: u64,
    to: u64,
) -> Result<(), StoreError> {
    let mut off = from;
    while off < to {
        let rem = to - off;
        let Some(c) = largest_reloc_class_le(rem) else {
            break;
        };
        let sz = slab_bytes(c);
        push_free_head(body, free_head, c, off)?;
        off = off.saturating_add(sz);
    }
    Ok(())
}

pub struct ScriptHashTable {
    store_dir: PathBuf,
    layout: ShBodyLayout,
    bodies: Vec<TableFile>,
    ovf_body: Option<TableFile>,
    n_shards: usize,
    /// Sealed MPHF main shards (set when a cold bulk shard is installed).
    /// Per-shard slot so Electrum `get` does not take a process-wide mutex.
    sorted_main: Box<[RwLock<Option<MphfHead>>]>,
    /// Global ingest OA for keys first seen after main seal (`scripthash.ovf/ingest`).
    ingest: Mutex<ScriptHashHead>,
    /// L0 sealed global ovf (`SHSR`+fuse), newest last.
    sealed_ovf: Mutex<Vec<SortedHead>>,
    /// L1 promoted ovf (at most one MPHF+fuse per rematerialize lifetime).
    ovf_l1: Mutex<Option<OvfL1>>,
    l1_frozen_warned: AtomicBool,
    /// At least one sealed sorted main shard is installed.
    sorted_main_on: std::sync::atomic::AtomicBool,
    /// One alloc per `bodies` entry (Shared: len 1).
    allocs: Vec<Mutex<AllocState>>,
    /// Dir-variant ovf alloc. Shared: `None` (ovf uses `allocs[0]`).
    ovf_alloc: Option<Mutex<AllocState>>,
}

struct OvfL1 {
    head: MphfHead,
    fuse: SealedFuse8,
}

const SH_L1_FROZEN_WARN: &str =
    "scripthash ovf L1 MPHF is frozen; wipe store/scripthash* and rematerialize (--shindex)";

/// How `scripthash.body` is oriented on disk (schema 17 variant).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ShBodyLayout {
    /// Single `scripthash.body` file (legacy 17).
    Shared,
    /// `scripthash.body/NN` + `scripthash.ovf/body`.
    Sharded,
}

fn sh_body_path(dir: &Path) -> PathBuf {
    dir.join("scripthash.body")
}

fn sh_ovf_body_path(dir: &Path) -> PathBuf {
    dir.join("scripthash.ovf").join("body")
}

fn sh_shard_body_path(dir: &Path, shard: usize) -> PathBuf {
    sh_body_path(dir).join(format!("{shard:02x}"))
}

fn sh_set_body_grow(body: &TableFile) {
    body.set_grow_policy(GrowPolicy::Align64k);
}

fn sh_body_layout_wipe_msg() -> String {
    "scripthash body layout mixed or incomplete; wipe store/scripthash* (head, body, ovf, \
     runs, include_hwm, cold_progress) and rematerialize"
        .into()
}

/// Detect file vs directory SH body. Does not rewrite either orientation.
pub fn detect_sh_body_layout(dir: &Path) -> Result<ShBodyLayout, StoreError> {
    let body = sh_body_path(dir);
    let ovf = sh_ovf_body_path(dir);
    match (body.is_file(), body.is_dir()) {
        (true, false) if ovf.exists() => Err(StoreError::Layout(sh_body_layout_wipe_msg())),
        (true, false) => Ok(ShBodyLayout::Shared),
        (false, true) if ovf.is_file() => Ok(ShBodyLayout::Sharded),
        (false, true) => Err(StoreError::Layout(sh_body_layout_wipe_msg())),
        (false, false) => Err(StoreError::Layout(sh_body_layout_wipe_msg())),
        (true, true) => Err(StoreError::Layout(sh_body_layout_wipe_msg())),
    }
}

fn init_empty_body(body: &TableFile) -> Result<AllocState, StoreError> {
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

fn ingest_path(dir: &Path) -> PathBuf {
    dir.join("scripthash.ovf").join("ingest")
}

fn ingest_oa_slots() -> u64 {
    match HeadScale::from_env() {
        HeadScale::Tiny => 256,
        HeadScale::Mainnet => 1 << 25,
    }
}

fn sealed_ovf_path(dir: &Path, id: u32) -> PathBuf {
    dir.join("scripthash.ovf").join(format!("{id:06}"))
}

fn file_starts_with_shsr(path: &Path) -> bool {
    let Ok(mut f) = std::fs::File::open(path) else {
        return false;
    };
    let mut magic = [0u8; 4];
    use std::io::Read;
    matches!(f.read_exact(&mut magic), Ok(())) && magic == *b"SHSR"
}

fn sorted_main_shard_path(dir: &Path, shard: usize, n_shards: usize) -> PathBuf {
    let p = dir.join("scripthash.head");
    if n_shards <= 1 && p.is_file() {
        p
    } else {
        p.join(format!("{shard:02x}"))
    }
}

fn open_sorted_main_shards(
    dir: &Path,
    n_shards: usize,
) -> Result<Vec<Option<MphfHead>>, StoreError> {
    let n = n_shards.max(1);
    let mut out = Vec::with_capacity(n);
    for i in 0..n {
        let p = sorted_main_shard_path(dir, i, n);
        if MphfHead::exists(&p) {
            out.push(Some(MphfHead::open(&p)?));
        } else {
            out.push(None);
        }
    }
    Ok(out)
}

fn wrap_sorted_slots(v: Vec<Option<MphfHead>>) -> Box<[RwLock<Option<MphfHead>>]> {
    v.into_iter().map(RwLock::new).collect()
}

fn open_ovf_l1(dir: &Path) -> Result<Option<OvfL1>, StoreError> {
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

fn open_sealed_sorted_ovf(dir: &Path) -> Result<Vec<SortedHead>, StoreError> {
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
        if file_starts_with_shsr(&p) {
            out.push(SortedHead::open(p, SortedHeadFilter::Fuse8)?);
        }
    }
    Ok(out)
}

fn open_or_create_ingest(dir: &Path) -> Result<ScriptHashHead, StoreError> {
    let p = ingest_path(dir);
    if let Some(parent) = p.parent() {
        std::fs::create_dir_all(parent).map_err(|e| StoreError::io(parent, e))?;
    }
    if p.exists() {
        ScriptHashHead::open(p)
    } else {
        ScriptHashHead::create_with_slots(p, ingest_oa_slots())
    }
}

fn sorted_main_present(dir: &Path, n_shards: usize) -> bool {
    let n = n_shards.max(1);
    (0..n).any(|i| MphfHead::exists(&sorted_main_shard_path(dir, i, n)))
}

fn leftover_oa_wipe_msg() -> String {
    "scripthash leftover live OA index; wipe store/scripthash* (head, body, ovf, \
     runs, include_hwm, cold_progress, main_sealed, oa_stub) and rematerialize \
     (--shindex rebuilds on start)"
        .into()
}

fn unlink_leftover_oa_stub(dir: &Path) {
    let stub = dir.join("scripthash.head.oa_stub");
    if stub.is_dir() {
        let _ = std::fs::remove_dir_all(&stub);
    } else if stub.is_file() {
        let _ = std::fs::remove_file(&stub);
    }
}

fn sharded_body_n_shards(dir: &Path) -> Result<usize, StoreError> {
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

/// Live OA or schema-17 `SHSR` at `scripthash.head` (file or shard dir).
fn leftover_live_oa_main(head_path: &Path) -> bool {
    head_path.exists()
}

/// Non-`SHSR` six-digit files under `scripthash.ovf/` (old OA overflow segs).
fn leftover_oa_overflow(dir: &Path) -> bool {
    let ovf = dir.join("scripthash.ovf");
    let Ok(rd) = std::fs::read_dir(&ovf) else {
        return false;
    };
    rd.flatten().any(|e| {
        let name = e.file_name();
        let name = name.to_string_lossy();
        name.len() == 6
            && name.chars().all(|c| c.is_ascii_digit())
            && e.path().is_file()
            && !file_starts_with_shsr(&e.path())
    })
}

fn paged_first_from_last(body: &TableFile, last_page: u64) -> Result<u64, StoreError> {
    if last_page == 0 {
        return Ok(0);
    }
    note_sh_page_chain_io();
    let mut page = [0u8; SH_PAGE_SIZE];
    body.read_at(last_page, &mut page)?;
    sh_page_first_off(sh_page_as_array(&page)?)
}

/// Cap on a contiguous megakey span pread (64 MiB ≈ 16k pages).
const SH_PAGE_SPAN_MAX: u64 = 64 * 1024 * 1024;

#[cfg(test)]
thread_local! {
    static SH_PAGE_CHAIN_IOS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

#[cfg(test)]
fn note_sh_page_chain_io() {
    SH_PAGE_CHAIN_IOS.with(|c| c.set(c.get().saturating_add(1)));
}

#[cfg(not(test))]
fn note_sh_page_chain_io() {}

#[cfg(test)]
fn reset_sh_page_chain_ios() {
    SH_PAGE_CHAIN_IOS.with(|c| c.set(0));
}

#[cfg(test)]
fn sh_page_chain_ios() -> u64 {
    SH_PAGE_CHAIN_IOS.with(|c| c.get())
}

fn read_sh_page_bytes(body: &TableFile, off: u64, buf: &mut [u8]) -> Result<(), StoreError> {
    note_sh_page_chain_io();
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

fn extend_page_entries(
    out: &mut Vec<ShEntry>,
    prev_last: &mut Option<u64>,
    ents: Vec<ShEntry>,
) -> Result<(), StoreError> {
    if let (Some(pl), Some(first)) = (*prev_last, ents.first()) {
        if first.create_tx_fk.0 <= pl {
            return Err(StoreError::Corrupt(
                "invariant: scripthash page chain create_fks not strictly increasing",
            ));
        }
    }
    if let Some(last) = ents.last() {
        *prev_last = Some(last.create_tx_fk.0);
    }
    out.extend(ents);
    Ok(())
}

fn collect_page_chain_span(
    body: &TableFile,
    first_page: u64,
    n_pages: usize,
) -> Result<Option<(Vec<ShEntry>, u64)>, StoreError> {
    let mut buf = vec![0u8; n_pages.saturating_mul(SH_PAGE_SIZE)];
    read_sh_page_bytes(body, first_page, &mut buf)?;
    let mut out = Vec::new();
    let mut prev_last = None;
    let mut last_next = 0u64;
    for i in 0..n_pages {
        let start = i.saturating_mul(SH_PAGE_SIZE);
        let page = &buf[start..start.saturating_add(SH_PAGE_SIZE)];
        let (next, ents) = sh_page_decode_slice(page)?;
        if i + 1 == n_pages {
            last_next = next;
        } else {
            let expect = first_page.saturating_add(((i + 1) * SH_PAGE_SIZE) as u64);
            if next != expect {
                return Ok(None);
            }
        }
        extend_page_entries(&mut out, &mut prev_last, ents)?;
    }
    Ok(Some((out, last_next)))
}

fn collect_page_chain_linked(
    body: &TableFile,
    first_page: u64,
) -> Result<Vec<ShEntry>, StoreError> {
    let mut out = Vec::new();
    let mut prev_last: Option<u64> = None;
    let mut cur = [0u8; SH_PAGE_SIZE];
    let mut nxt = [0u8; SH_PAGE_SIZE];
    if first_page == 0 {
        return Ok(out);
    }
    read_sh_page_bytes(body, first_page, &mut cur)?;
    loop {
        let (next, ents) = sh_page_decode_slice(&cur)?;
        extend_page_entries(&mut out, &mut prev_last, ents)?;
        if next == 0 {
            break;
        }
        read_sh_page_bytes(body, next, &mut nxt)?;
        cur = nxt;
    }
    Ok(out)
}

fn collect_extent_then_tail(body: &TableFile, last_page: u64) -> Result<Vec<ShEntry>, StoreError> {
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
        return Err(StoreError::Corrupt("scripthash extent span overflow"));
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
    let mut prev_last = out.last().map(|e| e.create_tx_fk.0);
    let mut cur = [0u8; SH_PAGE_SIZE];
    let mut nxt = [0u8; SH_PAGE_SIZE];
    read_sh_page_bytes(body, tail_off, &mut cur)?;
    loop {
        let (next, ents) = sh_page_decode_slice(&cur)?;
        extend_page_entries(&mut out, &mut prev_last, ents)?;
        if next == 0 {
            break;
        }
        read_sh_page_bytes(body, next, &mut nxt)?;
        cur = nxt;
    }
    Ok(out)
}

/// Where a scripthash key lives for head upsert routing.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum KeyHome {
    /// Present on sealed sorted main.
    Main,
    /// Present on a sealed sorted global ovf file.
    SealedOvf,
    /// Present on the global ingest OA.
    Ingest,
    /// Not yet in either head.
    Absent,
}

impl ScriptHashTable {
    pub fn create(dir: &std::path::Path) -> Result<Self, StoreError> {
        let n_shards = shard_count_for_role(HeadRole::ScriptHash).max(1);
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
            sorted_main: wrap_sorted_slots((0..n_shards).map(|_| None).collect()),
            ingest: Mutex::new(open_or_create_ingest(dir)?),
            sealed_ovf: Mutex::new(Vec::new()),
            ovf_l1: Mutex::new(None),
            l1_frozen_warned: AtomicBool::new(false),
            sorted_main_on: std::sync::atomic::AtomicBool::new(false),
            allocs,
            ovf_alloc: Some(Mutex::new(ovf_st)),
        })
    }

    pub fn open(dir: &std::path::Path) -> Result<Self, StoreError> {
        let layout = detect_sh_body_layout(dir)?;
        if leftover_oa_overflow(dir) {
            return Err(StoreError::Layout(leftover_oa_wipe_msg()));
        }
        let head_path = dir.join("scripthash.head");
        let expected = shard_count_for_role(HeadRole::ScriptHash);
        if leftover_live_oa_main(&head_path) && !sorted_main_present(dir, expected) {
            return Err(StoreError::Layout(leftover_oa_wipe_msg()));
        }
        unlink_leftover_oa_stub(dir);
        let n_shards = match layout {
            ShBodyLayout::Shared => expected.max(1),
            ShBodyLayout::Sharded => sharded_body_n_shards(dir)?,
        };
        Self::from_layout_and_n_shards(dir, layout, n_shards)
    }

    fn from_layout_and_n_shards(
        dir: &Path,
        layout: ShBodyLayout,
        n_shards: usize,
    ) -> Result<Self, StoreError> {
        let n_shards = n_shards.max(1);
        let (bodies, ovf_body, allocs, ovf_alloc, alloc_ver) = match layout {
            ShBodyLayout::Shared => {
                let f = TableFile::open(sh_body_path(dir), TableKind::ScriptHash)?;
                sh_set_body_grow(&f);
                let (state, ver) = read_alloc_header(&f)?;
                (vec![f], None, vec![Mutex::new(state)], None, ver)
            }
            ShBodyLayout::Sharded => {
                let mut bodies = Vec::with_capacity(n_shards);
                let mut allocs = Vec::with_capacity(n_shards);
                let mut ver = SH_ALLOC_VERSION;
                for i in 0..n_shards {
                    let f = TableFile::open(sh_shard_body_path(dir, i), TableKind::ScriptHash)?;
                    sh_set_body_grow(&f);
                    let (state, v) = read_alloc_header(&f)?;
                    if i == 0 {
                        ver = v;
                    }
                    bodies.push(f);
                    allocs.push(Mutex::new(state));
                }
                let ovf = TableFile::open(sh_ovf_body_path(dir), TableKind::ScriptHash)?;
                sh_set_body_grow(&ovf);
                let (ost, _) = read_alloc_header(&ovf)?;
                (bodies, Some(ovf), allocs, Some(Mutex::new(ost)), ver)
            }
        };
        wipe_legacy_fullsize_overflow(dir)?;
        let sorted_main = open_sorted_main_shards(dir, n_shards)?;
        let sealed_ovf = open_sealed_sorted_ovf(dir)?;
        let ovf_l1 = open_ovf_l1(dir)?;
        let sorted_on = sorted_main.iter().any(|s| s.is_some());
        let table = Self {
            store_dir: dir.to_path_buf(),
            layout,
            bodies,
            ovf_body,
            n_shards,
            sorted_main: wrap_sorted_slots(sorted_main),
            ingest: Mutex::new(open_or_create_ingest(dir)?),
            sealed_ovf: Mutex::new(sealed_ovf),
            ovf_l1: Mutex::new(ovf_l1),
            l1_frozen_warned: AtomicBool::new(false),
            sorted_main_on: std::sync::atomic::AtomicBool::new(sorted_on),
            allocs,
            ovf_alloc,
        };
        // v1 = schema-13 slabs; v2 = schema-14 pages; v3 = schema-15 slabs.
        // Field layout is the same; only an empty older header upgrades silently.
        if alloc_ver != SH_ALLOC_VERSION {
            if table.has_durable_index() {
                return Err(StoreError::Corrupt(
                    "scripthash alloc is a pre-schema-15 body; wipe store/scripthash* (head, body, ovf, runs, include_hwm, cold_progress) and rematerialize",
                ));
            }
            table.reset_all_bodies()?;
        }
        Ok(table)
    }

    fn body(&self) -> &TableFile {
        &self.bodies[0]
    }

    fn ovf_file(&self) -> &TableFile {
        self.ovf_body.as_ref().unwrap_or(&self.bodies[0])
    }

    #[inline]
    fn shard_index(&self, full: &[u8; 32]) -> usize {
        prefix_shard_of(full, self.n_shards)
    }

    fn shard_body(&self, si: usize) -> &TableFile {
        match self.layout {
            ShBodyLayout::Shared => &self.bodies[0],
            ShBodyLayout::Sharded => &self.bodies[si],
        }
    }

    fn shard_alloc(&self, si: usize) -> &Mutex<AllocState> {
        match self.layout {
            ShBodyLayout::Shared => &self.allocs[0],
            ShBodyLayout::Sharded => &self.allocs[si],
        }
    }

    fn ovf_alloc_mutex(&self) -> &Mutex<AllocState> {
        self.ovf_alloc.as_ref().unwrap_or(&self.allocs[0])
    }

    fn body_for(&self, key: &[u8; 32], home: KeyHome) -> &TableFile {
        match home {
            KeyHome::Main => self.shard_body(self.shard_index(key)),
            KeyHome::Ingest | KeyHome::SealedOvf | KeyHome::Absent => self.ovf_file(),
        }
    }

    fn alloc_for(&self, key: &[u8; 32], home: KeyHome) -> &Mutex<AllocState> {
        match home {
            KeyHome::Main => self.shard_alloc(self.shard_index(key)),
            KeyHome::Ingest | KeyHome::SealedOvf | KeyHome::Absent => self.ovf_alloc_mutex(),
        }
    }

    fn reset_all_bodies(&self) -> Result<(), StoreError> {
        let payload0 = payload_start(FILE_HEADER_LEN);
        let empty = AllocState {
            live_count: 0,
            bump: payload0,
            free_head: [0; SH_MAX_CLASS as usize + 1],
        };
        for (i, body) in self.bodies.iter().enumerate() {
            *self.allocs[i].lock().unwrap() = empty;
            write_alloc_header(body, &empty)?;
            body.set_logical_len(payload0)?;
        }
        if let (Some(ovf), Some(oa)) = (&self.ovf_body, &self.ovf_alloc) {
            *oa.lock().unwrap() = empty;
            write_alloc_header(ovf, &empty)?;
            ovf.set_logical_len(payload0)?;
        }
        Ok(())
    }

    fn persist_all_allocs(&self) -> Result<(), StoreError> {
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

    fn flush_all_bodies(&self) -> Result<(), StoreError> {
        for b in &self.bodies {
            b.flush()?;
        }
        if let Some(ovf) = &self.ovf_body {
            ovf.flush()?;
        }
        Ok(())
    }

    fn flush_all_bodies_async(&self) -> Result<(), StoreError> {
        for b in &self.bodies {
            b.flush_async()?;
        }
        if let Some(ovf) = &self.ovf_body {
            ovf.flush_async()?;
        }
        Ok(())
    }
}

/// Schema 17 SH run compare key is the full 40-byte `{scripthash\|create_fk}` record.
pub const SH_RUN_SORT_KEY_LEN: u32 = 40;

/// Refuse leftover schema-16 SH run catalogs (`key_len != 40`).
///
/// Empty / missing `scripthash.runs` is ok. A sealed SH head is not inspected.
pub fn sh_run_catalog_key_len_ok(store_dir: &Path) -> Result<(), StoreError> {
    let runs = store_dir.join("scripthash.runs");
    if !runs.exists() {
        return Ok(());
    }
    let mut found = Vec::new();
    found.extend(list_runs(&runs)?);
    found.extend(list_materialize_claims(&runs)?);
    for r in found {
        if r.key_len != SH_RUN_SORT_KEY_LEN {
            return Err(StoreError::Corrupt(
                "schema 17 refuses key_len=32 scripthash.runs; wipe store/scripthash.runs and rematerialize",
            ));
        }
    }
    Ok(())
}

/// True when `scripthash.runs` (catalog, claims, merge CHECKPOINT/READY) can rebuild the head.
pub fn has_sh_run_rebuild_source(store_dir: &Path) -> bool {
    let runs = store_dir.join("scripthash.runs");
    if list_runs(&runs).map(|r| !r.is_empty()).unwrap_or(false) {
        return true;
    }
    if list_materialize_claims(&runs)
        .map(|r| !r.is_empty())
        .unwrap_or(false)
    {
        return true;
    }
    let merge = runs.join("merge");
    if list_fanin_reduce_outputs(&merge)
        .map(|o| o.map(|v| !v.is_empty()).unwrap_or(false))
        .unwrap_or(false)
    {
        return true;
    }
    if load_fanin_checkpoint(&merge).ok().flatten().is_some() {
        return true;
    }
    // MANIFEST-less leftovers (crash mid-write).
    dir_has_run_files(&runs) || dir_has_run_files(&merge)
}

fn dir_has_run_files(dir: &Path) -> bool {
    let Ok(rd) = std::fs::read_dir(dir) else {
        return false;
    };
    rd.flatten().any(|e| {
        let name = e.file_name();
        let s = name.to_string_lossy();
        s.ends_with(".run") || s.ends_with(".run.mat") || s.ends_with(".run.tmp")
    })
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
        self.ingest.lock().unwrap().is_known_empty()
    }

    /// Wipe body alloc + all head slots for a full cold rematerialize.
    ///
    /// Used when runs/`*.run.mat` still hold the complete create set after a
    /// partial/crashed bulk load. Does not delete files — resets in place so
    /// open table handles stay valid. Exclusive: no concurrent SH readers/writers.
    ///
    /// Must run whenever claims are about to cold-load, not only when
    /// `entry_count > 0`: crash mid-finish can leave head shards occupied while
    /// the alloc header still says `live_count == 0`.
    pub fn reinit_empty_for_cold_materialize(&self) -> Result<(), StoreError> {
        self.reset_all_bodies()?;
        for slot in self.sorted_main.iter() {
            *slot.write().unwrap() = None;
        }
        self.sorted_main_on
            .store(false, std::sync::atomic::Ordering::Release);
        Ok(())
    }

    /// Prepare resume after SIGINT: keep sealed main shards, reset the rest.
    ///
    /// Shared body: prefix HWM (`progress.next_shard..` + `body_bump`).
    /// Sharded bodies: each sealed `scripthash.head/NN` is a commit; holes stay.
    pub fn prepare_cold_resume(&self, progress: &ColdProgress) -> Result<(), StoreError> {
        let n = self.n_shards;
        let start = progress.next_shard as usize;
        if start > n {
            return Err(StoreError::Corrupt(
                "scripthash cold progress next_shard out of range",
            ));
        }
        let payload0 = payload_start(FILE_HEADER_LEN);
        match self.layout {
            ShBodyLayout::Shared => {
                let bump = progress.body_bump.max(payload0);
                let state = AllocState {
                    live_count: progress.live_count,
                    bump,
                    free_head: [0; SH_MAX_CLASS as usize + 1],
                };
                *self.allocs[0].lock().unwrap() = state;
                write_alloc_header(self.body(), &state)?;
                self.body().set_logical_len(bump)?;
            }
            ShBodyLayout::Sharded => {
                let sealed: Vec<bool> = self
                    .sorted_main
                    .iter()
                    .map(|s| s.read().unwrap().is_some())
                    .collect();
                let empty = AllocState {
                    live_count: 0,
                    bump: payload0,
                    free_head: [0; SH_MAX_CLASS as usize + 1],
                };
                for i in 0..n {
                    if sealed.get(i).copied().unwrap_or(false) {
                        continue;
                    }

                    *self.allocs[i].lock().unwrap() = empty;
                    write_alloc_header(&self.bodies[i], &empty)?;
                    self.bodies[i].set_logical_len(payload0)?;
                    let p = sorted_main_shard_path(&self.store_dir, i, n);
                    let _ = std::fs::remove_file(&p);
                    let _ = std::fs::remove_file(scripthash_mphf::mphf_path(&p));
                    let _ = std::fs::remove_file(scripthash_mphf::val_path(&p));
                    let mut idx = p.clone().into_os_string();
                    idx.push(".idx");
                    let _ = std::fs::remove_file(idx);
                    let mut part = p.into_os_string();
                    part.push(".part");
                    let part = PathBuf::from(part);
                    let _ = std::fs::remove_file(&part);
                    let mut part_idx = part.into_os_string();
                    part_idx.push(".idx");
                    let _ = std::fs::remove_file(part_idx);
                }
            }
        }
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

    pub(crate) fn store_sharded_cold_progress(
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

    /// True if durable head has any occupancy or live creates (protect from wipe).
    pub fn has_durable_index(&self) -> bool {
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
        if self.ingest.lock().unwrap().occupied() > 0 {
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

    /// Head value for a key (process-cache seed / disconnect refresh).
    pub fn head_value(&self, scripthash: &[u8; 32]) -> Result<Option<ShHeadValue>, StoreError> {
        Ok(self.locate_head(scripthash)?.map(|(v, _)| v))
    }

    /// Which head segment holds `scripthash` (if any).
    fn key_home(&self, scripthash: &[u8; 32]) -> Result<KeyHome, StoreError> {
        Ok(self
            .locate_head(scripthash)?
            .map(|(_, h)| h)
            .unwrap_or(KeyHome::Absent))
    }

    /// Tip-mode probe: overflow first (ingest OA, then sealed ovf fuse), then main.
    ///
    /// Post-seal new keys live only on ingest / sealed ovf. Checking those
    /// (fuse-gated ovf, small OA) avoids a 4 KiB main-page pread on every miss.
    /// Historical keys pay a few RAM fuse checks then one main idx+page.
    /// One walk returns both value and home so seed + route share the pread.
    fn locate_head(
        &self,
        scripthash: &[u8; 32],
    ) -> Result<Option<(ShHeadValue, KeyHome)>, StoreError> {
        if let Some(v) = self.ingest.lock().unwrap().get(scripthash)? {
            let v = self.fill_paged_first(scripthash, v, KeyHome::Ingest)?;
            return Ok(Some((v, KeyHome::Ingest)));
        }
        let hk = head_key_from_full(scripthash);
        for h in self.sealed_ovf.lock().unwrap().iter().rev() {
            if let Some(v) = h.get(&hk)? {
                let v = self.fill_paged_first(scripthash, v, KeyHome::SealedOvf)?;
                return Ok(Some((v, KeyHome::SealedOvf)));
            }
        }
        if let Some(l1) = self.ovf_l1.lock().unwrap().as_ref() {
            if l1.fuse.contains(mix_key16(&hk)) {
                if let Some(v) = l1.head.get(&hk)? {
                    let v = self.fill_paged_first(scripthash, v, KeyHome::SealedOvf)?;
                    return Ok(Some((v, KeyHome::SealedOvf)));
                }
            }
        }
        if self.has_sorted_main() {
            let si = self.shard_index(scripthash);
            if let Some(slot) = self.sorted_main.get(si) {
                let g = slot.read().unwrap();
                if let Some(h) = g.as_ref() {
                    if let Some(v) = h.get(&hk)? {
                        let v = self.fill_paged_first(scripthash, v, KeyHome::Main)?;
                        return Ok(Some((v, KeyHome::Main)));
                    }
                }
            }
        }
        Ok(None)
    }

    fn fill_paged_first(
        &self,
        key: &[u8; 32],
        val: ShHeadValue,
        home: KeyHome,
    ) -> Result<ShHeadValue, StoreError> {
        match val {
            ShHeadValue::Paged {
                first_page: 0,
                last_page,
            } if last_page != 0 => {
                let first = paged_first_from_last(self.body_for(key, home), last_page)?;
                Ok(ShHeadValue::paged(first, last_page))
            }
            other => Ok(other),
        }
    }

    fn has_sorted_main(&self) -> bool {
        self.sorted_main_on
            .load(std::sync::atomic::Ordering::Acquire)
    }

    fn install_sorted_main(&self, shard: usize, sealed: MphfHead) {
        if let Some(slot) = self.sorted_main.get(shard) {
            *slot.write().unwrap() = Some(sealed);
            self.sorted_main_on
                .store(true, std::sync::atomic::Ordering::Release);
        }
    }

    #[cfg(test)]
    fn sorted_main_pread_count(&self, shard: usize) -> u64 {
        self.sorted_main
            .get(shard)
            .and_then(|s| s.read().unwrap().as_ref().map(|h| h.pread_count()))
            .unwrap_or(0)
    }

    #[cfg(test)]
    fn reset_sorted_main_preads(&self) {
        for slot in self.sorted_main.iter() {
            if let Some(h) = slot.read().unwrap().as_ref() {
                h.reset_pread_count();
            }
        }
    }

    /// Test-only: set alloc `live_count = 0` without clearing head slots.
    ///
    /// Models crash mid-finish after deferred heads landed but before the
    /// alloc header was updated (entry_count==0, head non-empty). Process-local
    /// fault inject — no production caller; kept for reinit recovery regression.
    #[cfg(test)]
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
                let body = match self.layout {
                    ShBodyLayout::Shared => &self.bodies[0],
                    ShBodyLayout::Sharded => &self.bodies[si],
                };
                h.for_each_occupied(|_k, val| {
                    let entries = self.collect_entries_from(body, &val)?;
                    for e in entries {
                        f(e.create_tx_fk);
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
                    for e in entries {
                        f(e.create_tx_fk);
                    }
                    Ok(())
                })?;
            }
        }
        self.ingest.lock().unwrap().for_each_occupied(|_key, val| {
            let entries = self.collect_entries_from(self.ovf_file(), &val)?;
            for e in entries {
                f(e.create_tx_fk);
            }
            Ok(())
        })?;
        Ok(())
    }

    /// Live creates for a scripthash (oldest → newest).
    ///
    /// Second element is a thin index row (no Class A joins). Expand at query.
    pub fn entries(
        &self,
        scripthash: &[u8; 32],
    ) -> Result<Vec<(Fk, ScriptHashRecord)>, StoreError> {
        let Some((val, home)) = self.locate_head(scripthash)? else {
            return Ok(Vec::new());
        };
        let list = self.collect_entries_from(self.body_for(scripthash, home), &val)?;
        Ok(list
            .into_iter()
            .map(|e| (e.create_tx_fk, ScriptHashRecord::from_entry(*scripthash, e)))
            .collect())
    }

    fn collect_page_chain(
        &self,
        body: &TableFile,
        first_page: u64,
    ) -> Result<Vec<ShEntry>, StoreError> {
        if first_page == 0 {
            return Ok(Vec::new());
        }
        collect_page_chain_linked(body, first_page)
    }

    /// Max durable create_tx_fk for a head value (**last page only** when paged).
    ///
    /// Sorted-chain invariant: max is the last entry of the last page (or max
    /// inline FK). Never walks earlier pages.
    fn last_create_fk_for_key(
        &self,
        scripthash: &[u8; 32],
        val: &ShHeadValue,
    ) -> Result<Option<Fk>, StoreError> {
        let home = self.key_home(scripthash)?;
        self.last_create_fk_on(self.body_for(scripthash, home), val)
    }

    fn last_create_fk_on(
        &self,
        body: &TableFile,
        val: &ShHeadValue,
    ) -> Result<Option<Fk>, StoreError> {
        match val {
            ShHeadValue::Empty => Ok(None),
            ShHeadValue::Inline { .. } => {
                let ents = val.inline_entries();
                Ok(ents.iter().map(|e| e.create_tx_fk).max_by_key(|f| f.0))
            }
            ShHeadValue::Slab { class, off, .. } => {
                let ents = self.read_slab(body, *class, *off)?;
                Ok(ents.last().map(|e| e.create_tx_fk))
            }
            ShHeadValue::Paged { last_page, .. } | ShHeadValue::Extent { last_page } => {
                let mut page = [0u8; SH_PAGE_SIZE];
                body.read_at(*last_page, &mut page)?;
                sh_page_last_fk(&page)
            }
        }
    }

    pub fn contains_create(
        &self,
        scripthash: &[u8; 32],
        create_tx_fk: Fk,
    ) -> Result<bool, StoreError> {
        if create_tx_fk.is_null() {
            return Ok(false);
        }
        let Some(val) = self.head_value(scripthash)? else {
            return Ok(false);
        };
        // Sorted chains: present iff create_tx_fk ≤ max and (equal max or in chain).
        // Equality to max is enough for common re-queue of last create; lower may
        // still need a walk for exact contains — keep full walk for API accuracy.
        match self.last_create_fk_for_key(scripthash, &val)? {
            None => Ok(false),
            Some(max) if create_tx_fk.0 > max.0 => Ok(false),
            Some(max) if create_tx_fk.0 == max.0 => Ok(true),
            Some(_) => {
                for (_fk, rec) in self.entries(scripthash)? {
                    if rec.create_tx_fk == create_tx_fk {
                        return Ok(true);
                    }
                }
                Ok(false)
            }
        }
    }

    /// Append a create (idempotent: `fk ≤ max` existing is a no-op).
    pub fn put_create(&self, rec: &ScriptHashRecord) -> Result<(), StoreError> {
        if rec.create_tx_fk.is_null() {
            return Err(StoreError::InvalidFk);
        }
        let mut heads = HashMap::new();
        if let Some(v) = self.head_value(&rec.scripthash)? {
            heads.insert(rec.scripthash, v);
        }
        let _ = self.put_create_batch_append(std::slice::from_ref(rec), &mut heads)?;
        Ok(())
    }

    /// Bulk append. Re-queued FKs `≤` durable max are skipped; only higher FKs
    /// are written. Returns how many were written.
    pub fn put_create_batch(&self, recs: &[ScriptHashRecord]) -> Result<usize, StoreError> {
        if recs.is_empty() {
            return Ok(0);
        }
        let mut heads = HashMap::new();
        let (n, _) = self.put_create_batch_append(recs, &mut heads)?;
        Ok(n)
    }

    /// Forward-append creates. Process-local `heads` map.
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
        heads: &mut HashMap<[u8; 32], ShHeadValue>,
    ) -> Result<(usize, AppendTiming), StoreError> {
        let mut timing = AppendTiming::default();
        if recs.is_empty() {
            return Ok((0, timing));
        }

        let t_sort = std::time::Instant::now();
        let mut order: Vec<usize> = (0..recs.len()).collect();
        order.sort_by(|&a, &b| recs[a].scripthash.cmp(&recs[b].scripthash));
        timing.sort_ns = t_sort.elapsed().as_nanos() as u64;

        let mut home: HashMap<[u8; 32], KeyHome> = HashMap::new();

        let t_seed = std::time::Instant::now();
        // Cold body (no prior creates): skip N head gets — empty table probes.
        if self.entry_count() > 0 {
            let mut missing: Vec<[u8; 32]> = Vec::new();
            {
                let mut seen_miss = std::collections::HashSet::new();
                for &i in &order {
                    let rec = &recs[i];
                    if rec.create_tx_fk.is_null() {
                        continue;
                    }
                    if heads.contains_key(&rec.scripthash) {
                        continue;
                    }
                    if seen_miss.insert(rec.scripthash) {
                        missing.push(rec.scripthash);
                    }
                }
            }
            missing.sort_unstable();
            for key in missing {
                if let Some((v, kh)) = self.locate_head(&key)? {
                    heads.insert(key, v);
                    home.insert(key, kh);
                } else {
                    home.insert(key, KeyHome::Absent);
                }
            }
        }
        for &i in &order {
            let key = recs[i].scripthash;
            if home.contains_key(&key) {
                continue;
            }
            if heads.contains_key(&key) {
                home.insert(key, self.key_home(&key)?);
            } else {
                home.insert(key, KeyHome::Absent);
            }
        }
        timing.seed_ns = t_seed.elapsed().as_nanos() as u64;

        let t_body = std::time::Instant::now();
        let mut head_final: Vec<([u8; 32], ShHeadValue, KeyHome)> = Vec::new();
        let mut written = 0usize;

        let mut i = 0usize;
        while i < order.len() {
            let rec0 = &recs[order[i]];
            if rec0.create_tx_fk.is_null() {
                i += 1;
                continue;
            }
            let key = rec0.scripthash;
            let mut fk_vals: Vec<u64> = Vec::new();
            while i < order.len() {
                let rec = &recs[order[i]];
                if rec.scripthash != key {
                    break;
                }
                if !rec.create_tx_fk.is_null() {
                    fk_vals.push(rec.create_tx_fk.0);
                }
                i += 1;
            }
            if fk_vals.is_empty() {
                continue;
            }
            fk_vals.sort_unstable();
            fk_vals.dedup();

            let cur = heads.get(&key).cloned().unwrap_or(ShHeadValue::Empty);
            let kh_early = home.get(&key).copied().unwrap_or(KeyHome::Absent);
            let max = self.last_create_fk_on(self.body_for(&key, kh_early), &cur)?;
            let max_u = max.map(|f| f.0).unwrap_or(0);
            let add: Vec<ShEntry> = fk_vals
                .into_iter()
                .filter(|&fk| max.is_none() || fk > max_u)
                .map(|fk| ShEntry::new(Fk(fk)))
                .collect();
            if add.is_empty() {
                continue;
            }
            written += add.len();
            let kh = home.get(&key).copied().unwrap_or(KeyHome::Absent);
            let body = self.body_for(&key, kh);
            let alloc_mu = self.alloc_for(&key, kh);
            let mut alloc = alloc_mu.lock().unwrap();
            let new_val = self.append_sorted_creates(body, &mut alloc, &cur, &add)?;
            write_alloc_header(body, &alloc)?;
            drop(alloc);
            heads.insert(key, new_val.clone());
            head_final.push((key, new_val, kh));
        }

        timing.body_ns = t_body.elapsed().as_nanos() as u64;

        if !head_final.is_empty() {
            let t_head = std::time::Instant::now();
            let flush_each = recs.len() as u64 >= Self::LARGE_BATCH_ROWS;
            self.apply_head_upserts(&head_final, flush_each)?;
            timing.head_ns = t_head.elapsed().as_nanos() as u64;
        }
        Ok((written, timing))
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
    fn apply_head_upserts(
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
                KeyHome::Main => {
                    let hk = head_key_from_full(key);
                    let si = self.shard_index(key);
                    let updated = if let Some(slot) = self.sorted_main.get(si) {
                        let g = slot.read().unwrap();
                        match g.as_ref() {
                            Some(h) => h.update_value(&hk, val)?,
                            None => false,
                        }
                    } else {
                        false
                    };
                    if updated {
                        continue;
                    }
                    ingest_ups.push((*key, val.clone()));
                }
            }
        }

        if !sealed_ovf_ups.is_empty() {
            let mut missed: Vec<([u8; 32], ShHeadValue)> = Vec::new();
            {
                let g = self.sealed_ovf.lock().unwrap();
                for (key, val) in &sealed_ovf_ups {
                    let hk = head_key_from_full(key);
                    let mut hit = false;
                    for h in g.iter().rev() {
                        if h.update_value(&hk, val)? {
                            hit = true;
                            break;
                        }
                    }
                    if !hit {
                        missed.push((*key, val.clone()));
                    }
                }
            }
            for (key, val) in missed {
                let hk = head_key_from_full(&key);
                let mut hit = false;
                if let Some(l1) = self.ovf_l1.lock().unwrap().as_ref() {
                    hit = l1.head.update_value(&hk, &val)?;
                }
                if !hit {
                    ingest_ups.push((key, val));
                }
            }
        }
        if !ingest_ups.is_empty() {
            {
                let g = self.ingest.lock().unwrap();
                g.insert_many(&ingest_ups)?;
            }
            self.maybe_seal_ingest()?;
        }
        Ok(())
    }

    fn maybe_seal_ingest(&self) -> Result<(), StoreError> {
        let load = {
            let g = self.ingest.lock().unwrap();
            g.load_ratio().unwrap_or(0.0)
        };
        if load < ScriptHashHead::SH_SEAL_LOAD {
            return Ok(());
        }
        self.seal_ingest()
    }

    fn seal_ingest(&self) -> Result<(), StoreError> {
        let mut recs = {
            let g = self.ingest.lock().unwrap();
            let mut recs = Vec::new();
            g.for_each_occupied(|full, val| {
                recs.push((head_key_from_full(&full), pack8_bytes(&val)?));
                Ok(())
            })?;
            recs
        };
        if recs.is_empty() {
            return Ok(());
        }
        recs.sort_unstable_by(|a, b| a.0.cmp(&b.0));
        let id = {
            let g = self.sealed_ovf.lock().unwrap();
            g.len() as u32
        };
        let path = sealed_ovf_path(&self.store_dir, id);
        if path.exists() && !file_starts_with_shsr(&path) {
            return Err(StoreError::Corrupt(
                "scripthash.ovf: seal path occupied by non-sorted segment",
            ));
        }
        let sealed = SortedHead::write(&path, &recs, SortedHeadFilter::Fuse8)?;
        self.sealed_ovf.lock().unwrap().push(sealed);
        let p = ingest_path(&self.store_dir);
        let _ = std::fs::remove_file(&p);
        let _ = std::fs::remove_file({
            let mut s = p.as_os_str().to_os_string();
            s.push(".occ");
            PathBuf::from(s)
        });
        *self.ingest.lock().unwrap() = ScriptHashHead::create_with_slots(p, ingest_oa_slots())?;
        self.maybe_compact_sealed_ovf()?;
        Ok(())
    }

    const SEALED_OVF_COMPACT_FILES: usize = 8;

    fn maybe_compact_sealed_ovf(&self) -> Result<(), StoreError> {
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

    fn warn_l1_frozen(&self) {
        if self
            .l1_frozen_warned
            .compare_exchange(false, true, Ordering::Relaxed, Ordering::Relaxed)
            .is_ok()
        {
            rbitcoin_log::warn!("store: {SH_L1_FROZEN_WARN}");
        }
    }

    /// K-way merge of sealed global ovf heads. Body offs unchanged. Readers
    /// keep the old `Vec` until this lock is released after rename.
    pub fn compact_sealed_ovf(&self) -> Result<(), StoreError> {
        if self.ovf_l1.lock().unwrap().is_some() {
            self.warn_l1_frozen();
            return Ok(());
        }
        let mut recs: Vec<(crate::scripthash_layout::ShHeadKey, [u8; SH_HEAD_VALUE_LEN])> =
            Vec::new();
        let old_paths: Vec<PathBuf> = {
            let g = self.sealed_ovf.lock().unwrap();
            if g.len() < 2 {
                return Ok(());
            }
            for h in g.iter() {
                h.for_each_occupied(|k, v| {
                    recs.push((k, pack8_bytes(&v)?));
                    Ok(())
                })?;
            }
            g.iter().map(|h| h.path().to_path_buf()).collect()
        };
        recs.sort_unstable_by(|a, b| a.0.cmp(&b.0));
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
        let packed: Vec<(crate::scripthash_layout::ShHeadKey, u64)> = recs
            .iter()
            .map(|(k, raw)| (*k, u64::from_le_bytes(*raw)))
            .collect();
        let head = MphfHead::write_pack8(&path, &packed)?;
        let mut fuse_keys: Vec<u64> = packed.iter().map(|(k, _)| mix_key16(k)).collect();
        fuse_keys.sort_unstable();
        fuse_keys.dedup();
        let fuse = SealedFuse8::build(&fuse_keys)?;
        {
            let mut fp = path.as_os_str().to_os_string();
            fp.push(".fuse8");
            fuse.write_to(&PathBuf::from(fp))?;
        }
        let old = {
            let mut g = self.sealed_ovf.lock().unwrap();
            std::mem::take(&mut *g)
        };
        drop(old);
        *self.ovf_l1.lock().unwrap() = Some(OvfL1 { head, fuse });
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

    fn collect_entries_from(
        &self,
        body: &TableFile,
        val: &ShHeadValue,
    ) -> Result<Vec<ShEntry>, StoreError> {
        match val {
            ShHeadValue::Empty => Ok(Vec::new()),
            ShHeadValue::Inline { .. } => Ok(val.inline_entries().to_vec()),
            ShHeadValue::Slab { class, off, used } => {
                let got = self.read_slab(body, *class, *off)?;
                if got.len() != *used as usize {
                    return Err(StoreError::Corrupt(
                        "invariant: scripthash slab used != decoded fk count",
                    ));
                }
                Ok(got)
            }
            ShHeadValue::Paged {
                first_page,
                last_page,
            } => {
                let first = if *first_page != 0 {
                    *first_page
                } else {
                    paged_first_from_last(body, *last_page)?
                };
                self.collect_page_chain(body, first)
            }
            ShHeadValue::Extent { last_page } => collect_extent_then_tail(body, *last_page),
        }
    }

    fn rewrite_entries_for_key(
        &self,
        body: &TableFile,
        alloc: &mut AllocState,
        old: &ShHeadValue,
        live: &[ShEntry],
    ) -> Result<ShHeadValue, StoreError> {
        let n = live.len() as u32;
        let old_list = self.collect_entries_from(body, old)?;
        let old_n = old_list.len() as u32;
        if n > old_n {
            alloc.live_count = alloc.live_count.saturating_add(u64::from(n - old_n));
        } else if n < old_n {
            alloc.live_count = alloc.live_count.saturating_sub(u64::from(old_n - n));
        }

        if n == 0 {
            self.free_if_paged(body, alloc, old)?;
            return Ok(ShHeadValue::Empty);
        }
        for w in live.windows(2) {
            if w[1].create_tx_fk.0 <= w[0].create_tx_fk.0 {
                return Err(StoreError::Corrupt(
                    "invariant: scripthash rewrite create_fks not strictly increasing",
                ));
            }
        }
        self.free_if_paged(body, alloc, old)?;
        self.pack_entries(body, alloc, live, false)
    }

    /// Append strictly increasing `new_ents` (all already `> durable max`) without
    /// walking the full page chain. Body I/O: fill last page + optional new pages.
    fn append_sorted_creates(
        &self,
        body: &TableFile,
        alloc: &mut AllocState,
        old: &ShHeadValue,
        new_ents: &[ShEntry],
    ) -> Result<ShHeadValue, StoreError> {
        if new_ents.is_empty() {
            return Ok(old.clone());
        }
        // Defensive: batch must be strictly increasing.
        for w in new_ents.windows(2) {
            if w[1].create_tx_fk.0 <= w[0].create_tx_fk.0 {
                return Err(StoreError::Corrupt(
                    "invariant: scripthash append batch create_fks not strictly increasing",
                ));
            }
        }
        alloc.live_count = alloc.live_count.saturating_add(new_ents.len() as u64);

        match old {
            ShHeadValue::Empty => self.pack_entries(body, alloc, new_ents, true),
            ShHeadValue::Inline { .. } => {
                let mut live = old.inline_entries().to_vec();
                live.extend_from_slice(new_ents);
                self.pack_entries(body, alloc, &live, true)
            }
            ShHeadValue::Slab { class, off, .. } => {
                let mut live = self.read_slab(body, *class, *off)?;
                if let (Some(last), Some(first_new)) = (live.last(), new_ents.first()) {
                    if first_new.create_tx_fk.0 <= last.create_tx_fk.0 {
                        return Err(StoreError::Corrupt(
                            "invariant: scripthash slab append create_fk not strictly increasing",
                        ));
                    }
                }
                live.extend_from_slice(new_ents);
                let new_val =
                    self.pack_entries_reuse(body, alloc, &live, true, Some((*class, *off)))?;
                if !matches!(
                    &new_val,
                    ShHeadValue::Slab {
                        class: nc,
                        off: no,
                        ..
                    } if *nc == *class && *no == *off
                ) {
                    self.free_slab(body, alloc, *class, *off)?;
                }
                Ok(new_val)
            }
            ShHeadValue::Paged {
                first_page,
                last_page,
            } => {
                let last =
                    self.append_fks_to_pages(body, alloc, *first_page, *last_page, new_ents)?;
                Ok(ShHeadValue::paged(*first_page, last))
            }
            ShHeadValue::Extent { last_page } => {
                let first = paged_first_from_last(body, *last_page)?;
                let last = self.append_fks_to_pages(body, alloc, first, *last_page, new_ents)?;
                Ok(ShHeadValue::extent(last))
            }
        }
    }

    /// Pack `live` into inline / slab / megakey pages. `slack` picks a class
    /// with a spare slot on tip grow; cold pack uses exact class.
    fn pack_entries(
        &self,
        body: &TableFile,
        alloc: &mut AllocState,
        live: &[ShEntry],
        slack: bool,
    ) -> Result<ShHeadValue, StoreError> {
        self.pack_entries_reuse(body, alloc, live, slack, None)
    }

    fn pack_entries_reuse(
        &self,
        body: &TableFile,
        alloc: &mut AllocState,
        live: &[ShEntry],
        slack: bool,
        reuse: Option<(u8, u64)>,
    ) -> Result<ShHeadValue, StoreError> {
        let n = live.len() as u32;
        if n == 0 {
            return Ok(ShHeadValue::Empty);
        }
        if n <= SH_INLINE_CAP as u32 {
            return Ok(ShHeadValue::inline_one(live[0]));
        }
        if n >= SH_MEGAKEY_MIN_FKS {
            let last = self.write_new_page_chain(body, alloc, live)?;
            return Ok(ShHeadValue::extent(last));
        }
        let fks: Vec<Fk> = live.iter().map(|e| e.create_tx_fk).collect();
        let payload = encode_slab_payload(&fks)?;
        let class = match Self::slab_class_fitting(n, payload.len(), slack) {
            Some(c) => c,
            None => {
                let last = self.write_new_page_chain(body, alloc, live)?;
                return Ok(ShHeadValue::extent(last));
            }
        };
        let cap = slab_bytes(class) as usize;
        if payload.len() > cap {
            return Err(StoreError::Corrupt(
                "invariant: scripthash slab payload exceeds class bytes",
            ));
        }
        let off = if let Some((rc, ro)) = reuse {
            if rc == class {
                ro
            } else {
                self.alloc_slab(body, alloc, class)?
            }
        } else {
            self.alloc_slab(body, alloc, class)?
        };
        let mut buf = vec![0u8; cap];
        buf[..payload.len()].copy_from_slice(&payload);
        body.write_at(off, &buf)?;
        Ok(ShHeadValue::slab(class, n as u16, off))
    }

    fn slab_class_fitting(n: u32, packed_len: usize, slack: bool) -> Option<u8> {
        if slack {
            let start = slab_class_for_n_fks_with_slack(n)?;
            return (start..=SH_MAX_SLAB_CLASS).find(|&c| slab_bytes(c) as usize >= packed_len);
        }
        slab_class_for_packed_len(packed_len)
    }

    fn read_slab(&self, body: &TableFile, class: u8, off: u64) -> Result<Vec<ShEntry>, StoreError> {
        if class > SH_MAX_SLAB_CLASS {
            return Err(StoreError::Corrupt("scripthash slab class overflow"));
        }
        let mut buf = vec![0u8; slab_bytes(class) as usize];
        body.read_at(off, &mut buf)?;
        let fks = decode_slab_payload(&buf)?;
        Ok(fks.into_iter().map(ShEntry::new).collect())
    }

    fn write_new_page_chain(
        &self,
        body: &TableFile,
        alloc: &mut AllocState,
        live: &[ShEntry],
    ) -> Result<u64, StoreError> {
        if live.is_empty() {
            return Err(StoreError::Corrupt("scripthash empty page chain"));
        }
        let fks: Vec<Fk> = live.iter().map(|e| e.create_tx_fk).collect();
        let chunks = sh_page_chunk_ranges(&fks)?;
        let n_pages = chunks.len();
        let base = self.alloc_extent(body, alloc, n_pages)?;
        let mut page = [0u8; SH_PAGE_SIZE];
        for (pi, &(start, end)) in chunks.iter().enumerate() {
            let off = base.saturating_add((pi as u64).saturating_mul(SH_PAGE_SIZE as u64));
            if pi + 1 == n_pages {
                sh_page_pack_extent_last(&mut page, &live[start..end], base, n_pages as u32, 0)?;
            } else {
                let next = off.saturating_add(SH_PAGE_SIZE as u64);
                sh_page_pack(&mut page, &live[start..end], next)?;
            }
            body.write_at(off, &page)?;
        }
        Ok(base.saturating_add(((n_pages - 1) as u64).saturating_mul(SH_PAGE_SIZE as u64)))
    }

    /// Append `tail` FKs onto an existing chain ending at `last_page`.
    fn append_fks_to_pages(
        &self,
        body: &TableFile,
        alloc: &mut AllocState,
        first_page: u64,
        last_page: u64,
        tail: &[ShEntry],
    ) -> Result<u64, StoreError> {
        if tail.is_empty() {
            return Ok(last_page);
        }
        let mut last = last_page;
        let mut page = [0u8; SH_PAGE_SIZE];
        body.read_at(last, &mut page)?;
        let mut extent = sh_page_extent(&page)?;
        let chain_first = {
            let f = if sh_page_is_last(&page)? {
                sh_page_first_off(&page)?
            } else {
                0
            };
            if f == 0 {
                first_page
            } else {
                f
            }
        };
        for e in tail {
            if !sh_page_try_append(&mut page, e.create_tx_fk)? {
                let new_off = self.alloc_page(body, alloc)?;
                sh_page_set_next(&mut page, new_off)?;
                body.write_at(last, &page)?;
                if let Some((base, n)) = extent {
                    let glued = new_off == base.saturating_add(u64::from(n) * SH_PAGE_SIZE as u64);
                    let new_n = if glued { n.saturating_add(1) } else { n };
                    sh_page_pack_extent_last(&mut page, &[*e], base, new_n, 0)?;
                    extent = Some((base, new_n));
                } else {
                    sh_page_init_empty(&mut page);
                    sh_page_set_last(&mut page, chain_first)?;
                    assert!(sh_page_try_append(&mut page, e.create_tx_fk)?);
                }
                last = new_off;
            }
        }
        body.write_at(last, &page)?;
        Ok(last)
    }

    fn alloc_page(&self, body: &TableFile, alloc: &mut AllocState) -> Result<u64, StoreError> {
        self.alloc_slab(body, alloc, SH_PAGE_SLAB_CLASS)
    }

    fn alloc_extent(
        &self,
        body: &TableFile,
        alloc: &mut AllocState,
        n_pages: usize,
    ) -> Result<u64, StoreError> {
        if n_pages == 0 {
            return Err(StoreError::Corrupt("scripthash empty extent"));
        }
        let mut off = alloc.bump;
        let aligned = (off + 4095) & !4095;
        if aligned != off {
            carve_gap_into_freelist(body, &mut alloc.free_head, off, aligned)?;
            alloc.bump = aligned;
            off = aligned;
        }
        let need = (n_pages as u64).saturating_mul(SH_PAGE_SIZE as u64);
        alloc.bump = alloc.bump.saturating_add(need);
        body.ensure_capacity(alloc.bump)?;
        if alloc.bump > body.logical_len() {
            body.set_logical_len(alloc.bump)?;
        }
        Ok(off)
    }

    fn alloc_slab(
        &self,
        body: &TableFile,
        alloc: &mut AllocState,
        class: u8,
    ) -> Result<u64, StoreError> {
        if class > SH_MAX_CLASS {
            return Err(StoreError::Corrupt("scripthash page class overflow"));
        }
        if let Some(off) = pop_free_head(body, &mut alloc.free_head, class)? {
            return Ok(off);
        }
        let need = slab_bytes(class);
        let mut off = alloc.bump;
        if class >= SH_PAGE_SLAB_CLASS {
            let aligned = (off + 4095) & !4095;
            if aligned != off {
                carve_gap_into_freelist(body, &mut alloc.free_head, off, aligned)?;
                alloc.bump = aligned;
                off = aligned;
            }
        }
        alloc.bump = alloc.bump.saturating_add(need);
        body.ensure_capacity(alloc.bump)?;
        if alloc.bump > body.logical_len() {
            body.set_logical_len(alloc.bump)?;
        }
        Ok(off)
    }

    fn free_if_paged(
        &self,
        body: &TableFile,
        alloc: &mut AllocState,
        old: &ShHeadValue,
    ) -> Result<(), StoreError> {
        match old {
            ShHeadValue::Paged { first_page, .. } => {
                let mut off = *first_page;
                while off != 0 {
                    let mut page = [0u8; SH_PAGE_SIZE];
                    body.read_at(off, &mut page)?;
                    let (next, _) = sh_page_decode_slice(&page)?;
                    self.free_slab(body, alloc, SH_PAGE_SLAB_CLASS, off)?;
                    off = next;
                }
            }
            ShHeadValue::Extent { last_page } => {
                let mut off = paged_first_from_last(body, *last_page)?;
                while off != 0 {
                    let mut page = [0u8; SH_PAGE_SIZE];
                    body.read_at(off, &mut page)?;
                    let (next, _) = sh_page_decode_slice(&page)?;
                    self.free_slab(body, alloc, SH_PAGE_SLAB_CLASS, off)?;
                    off = next;
                }
            }
            ShHeadValue::Slab { class, off, .. } => {
                self.free_slab(body, alloc, *class, *off)?;
            }
            _ => {}
        }
        Ok(())
    }

    fn free_slab(
        &self,
        body: &TableFile,
        alloc: &mut AllocState,
        class: u8,
        off: u64,
    ) -> Result<(), StoreError> {
        push_free_head(body, &mut alloc.free_head, class, off)
    }

    /// Unlink one create_tx_fk (disconnect tip). Caller should only remove the fk
    /// when no remaining outputs of that tx still match this scripthash.
    /// Swap-remove; demote paged→inline when ≤2 remain.
    pub fn unlink_create(
        &self,
        scripthash: &[u8; 32],
        create_tx_fk: Fk,
        _vout: u32,
    ) -> Result<bool, StoreError> {
        let Some((val, home)) = self.locate_head(scripthash)? else {
            return Ok(false);
        };
        let body = self.body_for(scripthash, home);
        let mut live = self.collect_entries_from(body, &val)?;
        let Some(pos) = live.iter().position(|e| e.create_tx_fk == create_tx_fk) else {
            return Ok(false);
        };
        live.remove(pos); // keep remaining order; sort if swap would break
        live.sort_by_key(|e| e.create_tx_fk.0);
        let alloc_mu = self.alloc_for(scripthash, home);
        let mut alloc = alloc_mu.lock().unwrap();
        let new_val = self.rewrite_entries_for_key(body, &mut alloc, &val, &live)?;
        write_alloc_header(body, &alloc)?;
        drop(alloc);
        match home {
            KeyHome::Main | KeyHome::Absent => {
                let hk = head_key_from_full(scripthash);
                let si = self.shard_index(scripthash);
                let updated_sorted = if let Some(slot) = self.sorted_main.get(si) {
                    let g = slot.read().unwrap();
                    match g.as_ref() {
                        Some(h) => h.update_value(&hk, &new_val)?,
                        None => false,
                    }
                } else {
                    false
                };
                if !updated_sorted {
                    let g = self.ingest.lock().unwrap();
                    if new_val.is_empty() {
                        g.clear_key(scripthash)?;
                    } else {
                        g.insert(scripthash, &new_val)?;
                    }
                }
            }
            KeyHome::Ingest => {
                let g = self.ingest.lock().unwrap();
                if new_val.is_empty() {
                    g.clear_key(scripthash)?;
                } else {
                    g.insert(scripthash, &new_val)?;
                }
            }
            KeyHome::SealedOvf => {
                let hk = head_key_from_full(scripthash);
                let g = self.sealed_ovf.lock().unwrap();
                let mut hit = false;
                for h in g.iter().rev() {
                    if h.update_value(&hk, &new_val)? {
                        hit = true;
                        break;
                    }
                }
                drop(g);
                if !hit {
                    if let Some(l1) = self.ovf_l1.lock().unwrap().as_ref() {
                        if l1.head.update_value(&hk, &new_val)? {
                            hit = true;
                        }
                    }
                }
                if !hit {
                    return Err(StoreError::Corrupt(
                        "scripthash: sealed ovf unlink missed home",
                    ));
                }
            }
        }
        Ok(true)
    }

    pub fn flush(&self) -> Result<(), StoreError> {
        self.persist_all_allocs()?;
        self.flush_all_bodies()?;
        self.ingest.lock().unwrap().flush()?;
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
        self.ingest.lock().unwrap().flush_async()?;
        for h in self.sealed_ovf.lock().unwrap().iter() {
            h.flush()?;
        }
        if let Some(l1) = self.ovf_l1.lock().unwrap().as_ref() {
            l1.head.flush()?;
        }
        Ok(())
    }

    /// Start a cold bulk session (live in-RAM OA image per prefix shard).
    ///
    /// `unique_hint`: global unique-key estimate for **final** per-shard table
    /// pre-size. Pass **0** to use [`sh_unique_hint_default`] (env
    /// `RBITCOIN_SH_UNIQUE_HINT` / mainnet ~2e9 / tiny tests ~4k).
    ///
    /// **Do not** pass create-record counts — that oversizes the OA image.
    ///
    /// Callers stream **scripthash-sorted** chains via [`ScriptHashBulkSession::put_chain`].
    /// Peak head RAM ≈ one final-sized shard table only.
    pub fn bulk_session(&self, unique_hint: u64) -> Result<ScriptHashBulkSession<'_>, StoreError> {
        if !self.head_is_empty() {
            return Err(StoreError::Corrupt(
                "scripthash bulk_session requires empty head (reinit first)",
            ));
        }
        let n_shards = self.n_shards.max(1);
        let hint = if unique_hint == 0 {
            sh_unique_hint_default()
        } else {
            unique_hint
        };
        let key_budget = sh_per_shard_key_budget(hint, n_shards);
        let (bump, live_count, free_head) = {
            let a = self.allocs[0].lock().unwrap();
            (a.bump, a.live_count, a.free_head)
        };
        rbitcoin_log::info!(
            "store: scripthash bulk_session n_shards={n_shards} unique_hint={hint} \
             per_shard_keys≈{key_budget} (stream sorted recs; no live OA image)"
        );
        Ok(ScriptHashBulkSession {
            table: self,
            progress_dir: self.store_dir().to_path_buf(),
            bump,
            live_count,
            committed_bump: bump,
            shard_start_bump: bump,
            committed_live_count: live_count,
            committed_keys: 0,
            resume_from_shard: 0,
            active_shard: None,
            recs: Vec::new(),
            key_budget,
            body_buf: Vec::with_capacity(BULK_BODY_FLUSH.min(4 << 20)),
            body_write_off: bump,
            finished: false,
            keys_written: 0,
            shards_flushed: 0,
            body_flush_ns: 0,
            head_fill_ns: 0,
            peak_table_bytes: 0,
            open_key: None,
            pack_only: false,
            max_fk: 0,
            free_head,
        })
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
        let n_shards = self.n_shards.max(1);
        let hint = sh_unique_hint_default();
        let key_budget = sh_per_shard_key_budget(hint, n_shards);
        Ok(ScriptHashBulkSession {
            table: self,
            progress_dir: self.store_dir().to_path_buf(),
            bump,
            live_count: 0,
            committed_bump: bump,
            shard_start_bump: bump,
            committed_live_count: prior_live,
            committed_keys: 0,
            resume_from_shard: 0,
            active_shard: Some(shard),
            recs: Vec::new(),
            key_budget,
            body_buf: Vec::with_capacity(BULK_BODY_FLUSH.min(4 << 20)),
            body_write_off: bump,
            finished: false,
            keys_written: 0,
            shards_flushed: 0,
            body_flush_ns: 0,
            head_fill_ns: 0,
            peak_table_bytes: 0,
            open_key: None,
            pack_only: true,
            max_fk: 0,
            free_head,
        })
    }

    /// Resume cold bulk after SIGINT: keep shards `[0, progress.next_shard)`, fill from there.
    ///
    /// Caller must [`Self::prepare_cold_resume`] first and skip stream keys with
    /// `shard_index < progress.next_shard`.
    pub fn bulk_session_resume(
        &self,
        unique_hint: u64,
        progress: &ColdProgress,
    ) -> Result<ScriptHashBulkSession<'_>, StoreError> {
        let n_shards = self.n_shards.max(1);
        let start = progress.next_shard as usize;
        if start >= n_shards {
            return Err(StoreError::Corrupt(
                "scripthash bulk_session_resume: already complete",
            ));
        }
        // Remaining shards must be empty for live install.

        let hint = if unique_hint == 0 {
            sh_unique_hint_default()
        } else {
            unique_hint
        };
        let key_budget = sh_per_shard_key_budget(hint, n_shards);
        let payload0 = payload_start(FILE_HEADER_LEN);
        let bump = progress.body_bump.max(payload0);
        rbitcoin_log::info!(
            "store: scripthash bulk_session resume next_shard={start}/{n_shards} \
             bump={bump} live_count={} keys≈{} (stream sorted recs; no live OA image)",
            progress.live_count,
            progress.keys_written
        );
        Ok(ScriptHashBulkSession {
            table: self,
            progress_dir: self.store_dir().to_path_buf(),
            bump,
            live_count: progress.live_count,
            committed_bump: bump,
            shard_start_bump: bump,
            committed_live_count: progress.live_count,
            committed_keys: progress.keys_written,
            resume_from_shard: progress.next_shard,
            active_shard: None,
            recs: Vec::new(),
            key_budget,
            body_buf: Vec::with_capacity(BULK_BODY_FLUSH.min(4 << 20)),
            body_write_off: bump,
            finished: false,
            keys_written: progress.keys_written,
            shards_flushed: progress.next_shard,
            body_flush_ns: 0,
            head_fill_ns: 0,
            peak_table_bytes: 0,
            open_key: None,
            pack_only: false,
            max_fk: 0,
            free_head: [0; SH_MAX_CLASS as usize + 1],
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

    /// Seal `recs` as sorted main shard `shard` and publish alloc HWM.
    pub fn publish_sorted_shard(
        &self,
        shard: usize,
        recs: &[(crate::scripthash_layout::ShHeadKey, [u8; SH_HEAD_VALUE_LEN])],
        live_count: u64,
        bump: u64,
    ) -> Result<(), StoreError> {
        let mut recs = recs.to_vec();
        recs.sort_unstable_by(|a, b| a.0.cmp(&b.0));
        recs.dedup_by(|a, b| a.0 == b.0);
        let n_shards = self.n_shards;
        let path = sorted_main_shard_path(&self.store_dir, shard, n_shards);
        if let Some(parent) = path.parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        let sealed = MphfHead::write(&path, &recs)?;
        self.install_sorted_main(shard, sealed);
        let body = self.shard_body(shard);
        if bump > body.logical_len() {
            body.set_logical_len(bump)?;
        }
        let free_head = self.shard_alloc(shard).lock().unwrap().free_head;
        let state = AllocState {
            live_count,
            bump,
            free_head,
        };
        write_alloc_header(body, &state)?;
        *self.shard_alloc(shard).lock().unwrap() = state;
        Ok(())
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
        let sealed = MphfHead::write(&path, &pack.recs)?;
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
        Ok(bump)
    }
}

/// Shift slab/page file offsets in a packed head value by `delta`. Inline unchanged.
pub fn remap_sh_head_value(val: &ShHeadValue, delta: u64) -> ShHeadValue {
    match val {
        ShHeadValue::Empty | ShHeadValue::Inline { .. } => val.clone(),
        ShHeadValue::Slab { class, used, off } => {
            ShHeadValue::slab(*class, *used, off.saturating_add(delta))
        }
        ShHeadValue::Paged {
            first_page,
            last_page,
        } => ShHeadValue::paged(
            first_page.saturating_add(delta),
            last_page.saturating_add(delta),
        ),
        ShHeadValue::Extent { last_page } => ShHeadValue::extent(last_page.saturating_add(delta)),
    }
}

/// Copy `[src_lo, src_hi)` from `src` to `dst` at `dst_lo`.
pub fn copy_sh_body_range(
    src: &TableFile,
    src_lo: u64,
    src_hi: u64,
    dst: &TableFile,
    dst_lo: u64,
) -> Result<(), StoreError> {
    if src_hi < src_lo {
        return Err(StoreError::Corrupt("scripthash copy range inverted"));
    }
    let len = src_hi - src_lo;
    let dst_end = dst_lo.saturating_add(len);
    dst.ensure_capacity(dst_end)?;
    if dst_end > dst.logical_len() {
        dst.set_logical_len(dst_end)?;
    }
    let mut off = 0u64;
    let mut buf = [0u8; 64 * 1024];
    while off < len {
        let n = ((len - off) as usize).min(buf.len());
        src.read_at(src_lo + off, &mut buf[..n])?;
        dst.write_at(dst_lo + off, &buf[..n])?;
        off += n as u64;
    }
    Ok(())
}

/// Rewrite `next` on a copied page chain. `first_dest` is already remapped;
/// bytes still hold local (pre-delta) `next`.
pub fn remap_copied_page_chain(
    body: &TableFile,
    first_dest: u64,
    delta: u64,
) -> Result<(), StoreError> {
    let mut off = first_dest;
    while off != 0 {
        let mut page = [0u8; SH_PAGE_SIZE];
        body.read_at(off, &mut page)?;
        let arr = sh_page_as_array(&page)?;
        if sh_page_is_last(arr)? {
            let first = sh_page_first_off(arr)?;
            let dest_first = if first == 0 {
                first_dest
            } else {
                first.saturating_add(delta)
            };
            let arr = sh_page_as_array_mut(&mut page)?;
            sh_page_set_last(arr, dest_first)?;
            if let Some((base, n)) = sh_page_extent(arr)? {
                sh_page_set_extent(arr, base.saturating_add(delta), n)?;
            }
            body.write_at(off, &page)?;
            break;
        }
        let local_next = sh_page_next(arr)?;
        if local_next == 0 {
            break;
        }
        let dest_next = local_next.saturating_add(delta);
        let arr = sh_page_as_array_mut(&mut page)?;
        sh_page_set_next(arr, dest_next)?;
        body.write_at(off, &page)?;
        off = dest_next;
    }
    Ok(())
}

/// Live-OA bulk writer for cold SH materialize.
///
/// Stream **scripthash-sorted** [`put_chain`] calls. Prefix sharding makes that
/// order contiguous per head shard: body slabs buffer (~16 MiB); packed
/// `(key16,value16)` recs accumulate for the active shard and seal to
/// MPHF+val on the boundary. Peak head RAM ≈ unique keys in one shard × 32 B.
pub struct ScriptHashBulkSession<'a> {
    table: &'a ScriptHashTable,
    /// Directory for [`ColdProgress`] file.
    progress_dir: PathBuf,
    bump: u64,
    live_count: u64,
    /// Last durable complete-shard body HWM (SIGINT rolls back incomplete slabs here).
    committed_bump: u64,
    /// Bump of the active shard file when that shard started (dir variant rollback).
    shard_start_bump: u64,
    committed_live_count: u64,
    committed_keys: u64,
    /// Skip installing keys for shards `< resume_from_shard` (stream may still deliver them).
    resume_from_shard: u32,
    active_shard: Option<usize>,
    /// Packed recs for [`Self::bulk_session`] (sorted at shard seal; 16 B key order).
    recs: Vec<(crate::scripthash_layout::ShHeadKey, [u8; SH_HEAD_VALUE_LEN])>,
    /// Unique-key budget (log / tests). Does not pre-size an OA table.
    key_budget: u64,
    body_buf: Vec<u8>,
    body_write_off: u64,
    finished: bool,
    keys_written: u64,
    shards_flushed: u32,
    /// Wall time spent in body `write_at` flushes.
    pub body_flush_ns: u64,
    /// Wall time spent installing head shards (write of live image).
    pub head_fill_ns: u64,
    /// Peak packed-rec buffer (bytes) — test/bench meter.
    pub peak_table_bytes: usize,
    /// In-flight key: at most one page of FKs (streaming megakey).
    open_key: Option<BulkOpenKey>,
    /// When true, head is streamed to `.part` and installed at `publish_packed_shard`.
    pack_only: bool,
    max_fk: u64,
    free_head: [u64; SH_MAX_CLASS as usize + 1],
}

/// One shard packed onto its live body, ready for ordered head publish.
pub struct ShShardPack {
    pub recs: Vec<(crate::scripthash_layout::ShHeadKey, [u8; SH_HEAD_VALUE_LEN])>,
    pub creates: u64,
    pub max_fk: u64,
    pub keys: u64,
    pub bump: u64,
    pub body_flush_ns: u64,
}

/// One unfinished key in [`ScriptHashBulkSession`] (≤ one delta page of FKs).
struct BulkOpenKey {
    key: [u8; 32],
    buf: Vec<u64>,
    stream_used: usize,
    n_total: u32,
    first_page: Option<u64>,
    last_fk: Option<u64>,
}

const BULK_BODY_FLUSH: usize = 16 * 1024 * 1024;

impl<'a> ScriptHashBulkSession<'a> {
    /// Creates written so far (sum of chain lengths, not unique keys).
    pub fn creates_written(&self) -> u64 {
        self.live_count
    }

    /// Creates including the open key's accepted FKs (status while a megakey streams).
    pub fn stream_creates_written(&self) -> u64 {
        self.live_count.saturating_add(
            self.open_key
                .as_ref()
                .map(|o| u64::from(o.n_total))
                .unwrap_or(0),
        )
    }

    /// Unique keys packed so far.
    pub fn keys_written(&self) -> u64 {
        self.keys_written
    }

    /// Head shards fully installed so far.
    pub fn shards_flushed(&self) -> u32 {
        self.shards_flushed
    }

    /// FKs buffered for the open key (never more than one page).
    pub fn buffered_fks(&self) -> usize {
        self.open_key.as_ref().map(|k| k.buf.len()).unwrap_or(0)
    }

    fn body(&self) -> &TableFile {
        match self.active_shard {
            Some(si) => self.table.shard_body(si),
            None => self.table.body(),
        }
    }

    fn body_and_free(&mut self) -> (&TableFile, &mut [u64; SH_MAX_CLASS as usize + 1]) {
        let body = match self.active_shard {
            Some(si) => self.table.shard_body(si),
            None => self.table.body(),
        };
        (body, &mut self.free_head)
    }

    fn persist_session_alloc(&self, live_count: u64, bump: u64) -> Result<(), StoreError> {
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

    /// Seal the pack-only session into a remappable blob (no live head write).
    pub fn finish_pack(mut self) -> Result<ShShardPack, StoreError> {
        if !self.pack_only {
            return Err(StoreError::Corrupt(
                "scripthash finish_pack requires pack_shard_session",
            ));
        }
        self.finish_key()?;
        self.flush_body()?;
        let persist_live = match self.table.layout {
            ShBodyLayout::Shared => self.committed_live_count.saturating_add(self.live_count),
            ShBodyLayout::Sharded => self.live_count,
        };
        self.persist_session_alloc(persist_live, self.bump)?;
        let pack = ShShardPack {
            recs: std::mem::take(&mut self.recs),
            creates: self.live_count,
            max_fk: self.max_fk,
            keys: self.keys_written,
            bump: self.bump,
            body_flush_ns: self.body_flush_ns,
        };
        self.finished = true;
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
        if self.open_key.as_ref().is_some_and(|o| o.key != key) {
            self.finish_key()?;
        }
        if self.open_key.is_none() {
            if !self.prepare_stream_key(key)? {
                return Ok(());
            }
            self.open_key = Some(BulkOpenKey {
                key,
                buf: Vec::with_capacity(512),
                stream_used: 0,
                n_total: 0,
                first_page: None,
                last_fk: None,
            });
        }
        if let Some(open) = self.open_key.as_ref() {
            if !open.buf.is_empty() {
                let prev = open.last_fk.expect("open page has last_fk");
                if fk.0 > prev
                    && open.stream_used.saturating_add(uleb128_len(fk.0 - prev))
                        > SH_PAGE_STREAM_MAX
                {
                    self.write_open_full_page_with_next()?;
                }
            }
        }
        let open = self
            .open_key
            .as_mut()
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
        }
        let add = if open.buf.is_empty() {
            uleb128_len(fk.0)
        } else {
            uleb128_len(fk.0 - open.last_fk.unwrap())
        };
        open.stream_used = open.stream_used.saturating_add(add);
        open.buf.push(fk.0);
        open.last_fk = Some(fk.0);
        open.n_total = open.n_total.saturating_add(1);
        if fk.0 > self.max_fk {
            self.max_fk = fk.0;
        }
        Ok(())
    }

    /// Seal the open key (inline / slab / last page).
    pub fn finish_key(&mut self) -> Result<(), StoreError> {
        let Some(open) = self.open_key.take() else {
            return Ok(());
        };
        if open.n_total == 0 {
            return Ok(());
        }
        let n = open.n_total;
        let val = if open.first_page.is_none() {
            let ents: Vec<ShEntry> = open.buf.iter().map(|&fk| ShEntry::new(Fk(fk))).collect();
            if n <= SH_INLINE_CAP as u32 {
                ShHeadValue::inline_one(ents[0])
            } else {
                self.flush_body()?;
                self.bulk_write_slab(&ents)?
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
        let rec = (head_key_from_full(&open.key), pack8_bytes(&val)?);
        self.recs.push(rec);
        self.peak_table_bytes = self
            .peak_table_bytes
            .max(self.recs.len().saturating_mul(24));
        Ok(())
    }

    /// Pack one key's live creates (**strictly increasing** create_tx_fk). Empty skipped.
    ///
    /// Keys must be presented in **non-decreasing scripthash order** (sorted-run
    /// merge). Crossing a prefix-shard boundary installs the previous live image.
    /// FKs are sorted+deduped here so merge-stream order glitches never break pages.
    pub fn put_chain(&mut self, key: [u8; 32], entries: &[ShEntry]) -> Result<(), StoreError> {
        if entries.is_empty() {
            return Ok(());
        }
        let mut fks: Vec<u64> = entries
            .iter()
            .filter(|e| !e.create_tx_fk.is_null())
            .map(|e| e.create_tx_fk.0)
            .collect();
        fks.sort_unstable();
        fks.dedup();
        for fk in fks {
            self.push_sorted_fk(key, Fk(fk))?;
        }
        self.finish_key()
    }

    /// `Ok(false)` = resume skip (shard already installed).
    fn prepare_stream_key(&mut self, key: [u8; 32]) -> Result<bool, StoreError> {
        let si = self.table.shard_index(&key);
        if (si as u32) < self.resume_from_shard {
            return Ok(false);
        }
        if self.pack_only {
            if self.active_shard != Some(si) {
                return Err(StoreError::Corrupt(
                    "scripthash pack session saw a key for a different shard",
                ));
            }
            return Ok(true);
        }
        if self.active_shard != Some(si) {
            if let Some(prev) = self.active_shard {
                if si < prev {
                    return Err(StoreError::Corrupt(
                        "scripthash bulk put_chain: keys not sorted by scripthash (shard went backwards)",
                    ));
                }
                self.flush_active_shard()?;
            }
            self.start_live_shard(si)?;
        }
        Ok(true)
    }

    fn write_open_full_page_with_next(&mut self) -> Result<(), StoreError> {
        let buf = self
            .open_key
            .as_mut()
            .map(|o| std::mem::take(&mut o.buf))
            .unwrap_or_default();
        let off = self.write_page(&buf, true, None)?;
        if let Some(open) = self.open_key.as_mut() {
            if open.first_page.is_none() {
                open.first_page = Some(off);
            }
            open.stream_used = 0;
        }
        Ok(())
    }

    /// Write one page at the aligned bump. `has_next` sets `next` to the following page.
    fn write_page(
        &mut self,
        fks: &[u64],
        has_next: bool,
        chain_first: Option<u64>,
    ) -> Result<u64, StoreError> {
        self.flush_body()?;
        let base = self.align_bump_for_page()?;
        let next = if has_next {
            base.saturating_add(SH_PAGE_SIZE as u64)
        } else {
            0
        };
        let end = base.saturating_add(SH_PAGE_SIZE as u64);
        self.ensure_body_capacity(end)?;
        let ents: Vec<ShEntry> = fks.iter().copied().map(|fk| ShEntry::new(Fk(fk))).collect();
        let mut page = [0u8; SH_PAGE_SIZE];
        if has_next {
            sh_page_pack(&mut page, &ents, next)?;
        } else {
            let first = chain_first.unwrap_or(base);
            let n = ((base.saturating_sub(first) / SH_PAGE_SIZE as u64).saturating_add(1)) as u32;
            sh_page_pack_extent_last(&mut page, &ents, first, n, 0)?;
        }
        self.body().write_at(base, &page)?;
        self.bump = end;
        self.body_write_off = end;
        Ok(base)
    }

    fn align_bump_for_page(&mut self) -> Result<u64, StoreError> {
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

    fn alloc_reloc_class(&mut self, class: u8) -> Result<u64, StoreError> {
        {
            let (body, free) = self.body_and_free();
            if let Some(off) = pop_free_head(body, free, class)? {
                return Ok(off);
            }
        }
        let need = slab_bytes(class);
        let off = self.bump;
        self.bump = self.bump.saturating_add(need);
        self.body_write_off = self.bump;
        self.ensure_body_capacity(self.bump)?;
        Ok(off)
    }

    fn start_live_shard(&mut self, si: usize) -> Result<(), StoreError> {
        self.recs.clear();
        rbitcoin_log::info!(
            "store: scripthash live shard start id={si} key_budget={} (stream recs)",
            self.key_budget
        );
        if !self.pack_only && self.table.layout == ShBodyLayout::Sharded {
            let payload0 = payload_start(FILE_HEADER_LEN);
            let a = self.table.shard_alloc(si).lock().unwrap();
            let bump = a.bump.max(payload0);
            self.free_head = a.free_head;
            drop(a);
            self.bump = bump;
            self.body_write_off = bump;
            self.shard_start_bump = bump;
        } else {
            self.shard_start_bump = self.bump;
        }
        self.active_shard = Some(si);
        Ok(())
    }

    /// Group a scripthash-sorted record slice into chains and [`put_chain`] each.
    ///
    /// Dedups create_tx_fk within a key (first occurrence wins). Returns create
    /// count written.
    pub fn put_sorted_creates(&mut self, recs: &[ScriptHashRecord]) -> Result<usize, StoreError> {
        let mut written = 0usize;
        let mut i = 0usize;
        while i < recs.len() {
            if recs[i].create_tx_fk.is_null() {
                i += 1;
                continue;
            }
            let key = recs[i].scripthash;
            let mut live: Vec<ShEntry> = Vec::new();
            let mut seen: Vec<Fk> = Vec::new();
            while i < recs.len() {
                let r = &recs[i];
                if r.scripthash != key {
                    break;
                }
                if !r.create_tx_fk.is_null() && !seen.contains(&r.create_tx_fk) {
                    seen.push(r.create_tx_fk);
                    live.push(ShEntry::new(r.create_tx_fk));
                }
                i += 1;
            }
            if live.is_empty() {
                continue;
            }
            written = written.saturating_add(live.len());
            self.put_chain(key, &live)?;
        }
        Ok(written)
    }

    /// Write one exact-class slab (byte-fit ULEB payload). Pages if stream > class 6.
    fn bulk_write_slab(&mut self, entries: &[ShEntry]) -> Result<ShHeadValue, StoreError> {
        let n = entries.len() as u32;
        let fks: Vec<Fk> = entries.iter().map(|e| e.create_tx_fk).collect();
        let payload = encode_slab_payload(&fks)?;
        let Some(class) = slab_class_for_packed_len(payload.len()) else {
            let last = self.bulk_write_page_chain(entries)?;
            return Ok(ShHeadValue::extent(last));
        };
        let off = self.alloc_reloc_class(class)?;
        let need = slab_bytes(class) as usize;
        let mut buf = vec![0u8; need];
        buf[..payload.len()].copy_from_slice(&payload);
        self.body().write_at(off, &buf)?;
        Ok(ShHeadValue::slab(class, n as u16, off))
    }

    /// Write a full page chain at the aligned bump. Returns (first, last).
    fn bulk_write_page_chain(&mut self, entries: &[ShEntry]) -> Result<u64, StoreError> {
        if entries.is_empty() {
            return Err(StoreError::Corrupt("scripthash bulk page chain empty"));
        }
        let base = self.align_bump_for_page()?;
        let fks: Vec<Fk> = entries.iter().map(|e| e.create_tx_fk).collect();
        let chunks = sh_page_chunk_ranges(&fks)?;
        let n_pages = chunks.len();
        let end = base.saturating_add((n_pages as u64).saturating_mul(SH_PAGE_SIZE as u64));
        self.ensure_body_capacity(end)?;
        let mut page = [0u8; SH_PAGE_SIZE];
        for (pi, &(start, end_i)) in chunks.iter().enumerate() {
            let off = base + (pi as u64) * (SH_PAGE_SIZE as u64);
            if pi + 1 < n_pages {
                let next = off + SH_PAGE_SIZE as u64;
                sh_page_pack(&mut page, &entries[start..end_i], next)?;
            } else {
                sh_page_pack_extent_last(
                    &mut page,
                    &entries[start..end_i],
                    base,
                    n_pages as u32,
                    0,
                )?;
            }
            self.body().write_at(off, &page)?;
        }
        self.bump = end;
        self.body_write_off = end;
        Ok(base + ((n_pages - 1) as u64) * (SH_PAGE_SIZE as u64))
    }

    fn ensure_body_capacity(&self, need: u64) -> Result<(), StoreError> {
        let body = self.body();
        body.ensure_capacity(need)?;
        if need > body.logical_len() {
            body.set_logical_len(need)?;
        }
        Ok(())
    }

    fn flush_body(&mut self) -> Result<(), StoreError> {
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

    /// Flush body buffer, install live OA image, free head RAM, write resume checkpoint.
    fn flush_active_shard(&mut self) -> Result<(), StoreError> {
        self.finish_key()?;
        let Some(si) = self.active_shard else {
            return Ok(());
        };
        self.flush_body()?;
        if self.pack_only {
            return Ok(());
        }
        if self.active_shard.is_some() {
            let t0 = std::time::Instant::now();
            let mut recs = std::mem::take(&mut self.recs);
            recs.sort_unstable_by(|a, b| a.0.cmp(&b.0));
            recs.dedup_by(|a, b| a.0 == b.0);
            let keys = recs.len() as u64;
            if !recs.is_empty() {
                let n_shards = self.table.n_shards;
                let path = sorted_main_shard_path(&self.table.store_dir, si, n_shards);
                if let Some(parent) = path.parent() {
                    let _ = std::fs::create_dir_all(parent);
                }
                let sealed = MphfHead::write(&path, &recs)?;
                self.table.install_sorted_main(si, sealed);
            }
            let shard_live = match self.table.layout {
                ShBodyLayout::Shared => self.live_count,
                ShBodyLayout::Sharded => self.live_count.saturating_sub(self.committed_live_count),
            };
            self.persist_session_alloc(shard_live, self.bump)?;
            self.committed_bump = self.bump;
            self.committed_live_count = self.live_count;
            self.committed_keys = self.keys_written;
            let next = (si as u32).saturating_add(1);
            ColdProgress {
                next_shard: next,
                body_bump: self.committed_bump,
                live_count: self.committed_live_count,
                keys_written: self.committed_keys,
            }
            .store(&self.progress_dir)?;
            let elapsed = t0.elapsed();
            self.head_fill_ns = self.head_fill_ns.saturating_add(elapsed.as_nanos() as u64);
            self.shards_flushed = self.shards_flushed.saturating_add(1);
            rbitcoin_log::info!(
                "store: scripthash live shard done id={si} keys={keys} \
                 recs_MiB≈{:.1} write={elapsed:?} next_shard={next}",
                (keys as f64 * 24.0) / (1024.0 * 1024.0)
            );
        }
        self.active_shard = None;
        Ok(())
    }

    /// Discard incomplete live shard (no install); roll body HWM to last checkpoint.
    ///
    /// Call on cooperative cancel so Drop does not install a partial shard.
    pub fn abandon_incomplete(mut self) {
        self.recs.clear();
        self.body_buf.clear();
        self.open_key = None;
        if self.pack_only {
            self.active_shard = None;
            self.finished = true;
            return;
        }
        let (live, bump) = match self.table.layout {
            ShBodyLayout::Shared => (self.committed_live_count, self.committed_bump),
            ShBodyLayout::Sharded => (0, self.shard_start_bump),
        };
        let _ = self.persist_session_alloc(live, bump);
        self.active_shard = None;
        self.bump = self.committed_bump;
        self.live_count = self.committed_live_count;
        self.keys_written = self.committed_keys;
        self.body_write_off = self.committed_bump;
        self.finished = true;
        rbitcoin_log::info!(
            "store: scripthash bulk session abandoned incomplete; \
             committed_keys≈{} bump={}",
            self.committed_keys,
            self.committed_bump
        );
    }

    /// Flush last shard head + alloc header; clear resume marker.
    ///
    /// Returns `(creates, keys, body_flush_ns, head_fill_ns)`.
    pub fn finish(mut self) -> Result<(u64, u64, u64, u64), StoreError> {
        self.flush_active_shard()?;
        if self.table.layout == ShBodyLayout::Shared {
            let live = self.live_count;
            let bump = self.bump;
            self.active_shard = None;
            self.persist_session_alloc(live, bump)?;
        }
        ColdProgress::clear(&self.progress_dir);
        self.finished = true;
        Ok((
            self.live_count,
            self.keys_written,
            self.body_flush_ns,
            self.head_fill_ns,
        ))
    }
}

impl Drop for ScriptHashBulkSession<'_> {
    fn drop(&mut self) {
        if self.finished || self.pack_only {
            return;
        }
        self.recs.clear();
        self.body_buf.clear();
        self.open_key = None;
        let (live, bump) = match self.table.layout {
            ShBodyLayout::Shared => (self.committed_live_count, self.committed_bump),
            ShBodyLayout::Sharded => (0, self.shard_start_bump),
        };
        let _ = self.persist_session_alloc(live, bump);
        self.active_shard = None;
    }
}

fn write_alloc_header(body: &TableFile, state: &AllocState) -> Result<(), StoreError> {
    let mut buf = vec![0u8; SH_ALLOC_HEADER_LEN];
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
/// header field layout. Callers upgrade empty v1 → v2 or refuse durable v1.
fn read_alloc_header(body: &TableFile) -> Result<(AllocState, u16), StoreError> {
    let mut buf = vec![0u8; SH_ALLOC_HEADER_LEN];
    let avail = body
        .logical_len()
        .saturating_sub(FILE_HEADER_LEN as u64)
        .min(SH_ALLOC_HEADER_LEN as u64) as usize;
    if avail < 24 {
        return Err(StoreError::Corrupt(
            "scripthash body missing alloc header (expected hybrid SHAL; migrate v3 stores)",
        ));
    }
    body.read_at(FILE_HEADER_LEN as u64, &mut buf[..avail])?;
    if buf[0..4] != SH_ALLOC_MAGIC {
        return Err(StoreError::Corrupt(
            "scripthash body not hybrid (no SHAL magic; run migrate)",
        ));
    }
    let ver = u16::from_le_bytes([buf[4], buf[5]]);
    // v1 = schema-13 slabs; v2 = schema-14 pages; v3 = schema-15. Same fields.
    if ver != 1 && ver != 2 && ver != SH_ALLOC_VERSION {
        return Err(StoreError::Corrupt("unsupported scripthash alloc version"));
    }
    let live_count = u64::from_le_bytes(buf[8..16].try_into().unwrap());
    let bump = u64::from_le_bytes(buf[16..24].try_into().unwrap());
    let mut free_head = [0u64; SH_MAX_CLASS as usize + 1];
    let mut off = 24usize;
    for h in &mut free_head {
        if off + 8 > avail {
            break;
        }
        *h = u64::from_le_bytes(buf[off..off + 8].try_into().unwrap());
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
#[cfg(test)]
fn read_alloc_version_on_disk(body: &TableFile) -> Result<u16, StoreError> {
    let mut buf = [0u8; 6];
    body.read_at(FILE_HEADER_LEN as u64, &mut buf)?;
    if buf[0..4] != SH_ALLOC_MAGIC {
        return Err(StoreError::Corrupt(
            "scripthash body not hybrid (no SHAL magic)",
        ));
    }
    Ok(u16::from_le_bytes([buf[4], buf[5]]))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scripthash_pages::{
        sh_page_as_array, sh_page_extent, SH_PAGE_EXTENT_STREAM_MAX, SH_PAGE_SIZE,
        SH_PAGE_STREAM_MAX,
    };
    use std::sync::atomic::AtomicBool;

    fn tmp() -> std::path::PathBuf {
        let p = std::env::temp_dir().join(format!(
            "rbitcoin-sh-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&p).unwrap();
        p
    }

    fn four_shard_dir_table(dir: &std::path::Path) -> ScriptHashTable {
        let body_dir = dir.join("scripthash.body");
        std::fs::create_dir_all(&body_dir).unwrap();
        let payload0 = payload_start(FILE_HEADER_LEN);
        for i in 0..4 {
            let f = TableFile::create(body_dir.join(format!("{i:02x}")), TableKind::ScriptHash)
                .unwrap();
            f.ensure_capacity(payload0).unwrap();
            f.set_logical_len(payload0).unwrap();
            write_alloc_header(
                &f,
                &AllocState {
                    live_count: 0,
                    bump: payload0,
                    free_head: [0; SH_MAX_CLASS as usize + 1],
                },
            )
            .unwrap();
        }
        std::fs::create_dir_all(dir.join("scripthash.ovf")).unwrap();
        let ovf = TableFile::create(
            dir.join("scripthash.ovf").join("body"),
            TableKind::ScriptHash,
        )
        .unwrap();
        ovf.ensure_capacity(payload0).unwrap();
        ovf.set_logical_len(payload0).unwrap();
        write_alloc_header(
            &ovf,
            &AllocState {
                live_count: 0,
                bump: payload0,
                free_head: [0; SH_MAX_CLASS as usize + 1],
            },
        )
        .unwrap();
        drop(ovf);
        ScriptHashTable::open(dir).unwrap()
    }

    fn sh_prefix_key(shard: u8, i: u8) -> [u8; 32] {
        let mut k = [0u8; 32];
        k[0] = shard << 6 | (i & 0x3f);
        k
    }

    #[test]
    fn sh_body_create_grows_64k_not_slab() {
        HeadScale::test_with(HeadScale::Tiny, || {
            let dir = tmp();
            let t = ScriptHashTable::create(&dir).unwrap();
            let sh = script_hash(&[0x01]);
            t.put_create(&rec(sh, 1, 0)).unwrap();
            t.put_create(&rec(sh, 2, 0)).unwrap();
            t.flush().unwrap();
            drop(t);
            for e in std::fs::read_dir(dir.join("scripthash.body")).unwrap() {
                let p = e.unwrap().path();
                if !p.is_file() {
                    continue;
                }
                let on_disk = std::fs::metadata(&p).unwrap().len();
                assert!(
                    on_disk < 128 * 1024,
                    "{} len {on_disk} must stay under 128 KiB after one slab",
                    p.display()
                );
            }
            let ovf_len = std::fs::metadata(dir.join("scripthash.ovf").join("body"))
                .unwrap()
                .len();
            assert!(
                ovf_len < 128 * 1024,
                "ovf body {ovf_len} must stay under 128 KiB"
            );
            let _ = std::fs::remove_dir_all(&dir);
        });
    }

    #[test]
    fn sh_bodies_are_split() {
        HeadScale::test_with(HeadScale::Tiny, || {
            let dir = tmp();
            let t = four_shard_dir_table(&dir);
            assert_eq!(t.head_shard_count(), 4);
            assert_eq!(t.body_layout(), ShBodyLayout::Sharded);
            let k0 = sh_prefix_key(0, 0);
            let k2 = sh_prefix_key(2, 0);
            let ents: Vec<ShEntry> = (1..=8).map(|i| ShEntry::new(Fk(i))).collect();
            {
                let mut s = t.bulk_session(16).unwrap();
                s.put_chain(k0, &ents).unwrap();
                s.put_chain(k2, &ents).unwrap();
                s.finish().unwrap();
            }
            let payload0 = payload_start(FILE_HEADER_LEN);
            assert!(
                t.bodies[0].logical_len() > payload0,
                "shard 0 body must grow"
            );
            assert_eq!(
                t.bodies[1].logical_len(),
                payload0,
                "shard 1 body stays empty"
            );
            assert!(
                t.bodies[2].logical_len() > payload0,
                "shard 2 body must grow"
            );
            assert_eq!(t.bodies[3].logical_len(), payload0);
            let k_new = sh_prefix_key(1, 0);
            for i in 1..=8u64 {
                t.put_create(&rec(k_new, i, 0)).unwrap();
            }
            let ovf_len = t.ovf_body.as_ref().unwrap().logical_len();
            assert!(ovf_len > payload0, "ovf ingest slab must land in ovf/body");
            assert_eq!(
                t.bodies[1].logical_len(),
                payload0,
                "ingest must not grow a main shard body"
            );
            assert_eq!(t.entries(&k0).unwrap().len(), 8);
            assert_eq!(t.entries(&k2).unwrap().len(), 8);
            assert_eq!(t.entries(&k_new).unwrap().len(), 8);

            let file_dir = tmp();
            let ft = shared_body_table(&file_dir);
            assert_eq!(ft.body_layout(), ShBodyLayout::Shared);
            {
                let mut s = ft.bulk_session(16).unwrap();
                s.put_chain(k0, &ents).unwrap();
                s.put_chain(k2, &ents).unwrap();
                s.finish().unwrap();
            }
            for i in 1..=8u64 {
                ft.put_create(&rec(k_new, i, 0)).unwrap();
            }
            assert!(ft.bodies[0].logical_len() > payload0);
            assert!(ft.ovf_body.is_none());
            assert_eq!(ft.entries(&k0).unwrap().len(), 8);
            assert_eq!(ft.entries(&k_new).unwrap().len(), 8);
            let _ = std::fs::remove_dir_all(&dir);
            let _ = std::fs::remove_dir_all(&file_dir);
        });
    }

    #[test]
    fn sh_body_orientation() {
        let file_dir = tmp();
        TableFile::create(file_dir.join("scripthash.body"), TableKind::ScriptHash).unwrap();
        assert_eq!(
            detect_sh_body_layout(&file_dir).unwrap(),
            ShBodyLayout::Shared
        );

        let dir_dir = tmp();
        std::fs::create_dir_all(dir_dir.join("scripthash.body")).unwrap();
        TableFile::create(
            dir_dir.join("scripthash.body").join("00"),
            TableKind::ScriptHash,
        )
        .unwrap();
        std::fs::create_dir_all(dir_dir.join("scripthash.ovf")).unwrap();
        TableFile::create(
            dir_dir.join("scripthash.ovf").join("body"),
            TableKind::ScriptHash,
        )
        .unwrap();
        assert_eq!(
            detect_sh_body_layout(&dir_dir).unwrap(),
            ShBodyLayout::Sharded
        );

        let mixed = tmp();
        TableFile::create(mixed.join("scripthash.body"), TableKind::ScriptHash).unwrap();
        std::fs::create_dir_all(mixed.join("scripthash.ovf")).unwrap();
        TableFile::create(
            mixed.join("scripthash.ovf").join("body"),
            TableKind::ScriptHash,
        )
        .unwrap();
        match detect_sh_body_layout(&mixed) {
            Err(StoreError::Layout(m)) => {
                assert!(m.contains("scripthash*"), "{m}");
                assert!(m.contains("wipe"), "{m}");
            }
            other => panic!("expected Layout, got {other:?}"),
        }

        let no_ovf = tmp();
        std::fs::create_dir_all(no_ovf.join("scripthash.body")).unwrap();
        match detect_sh_body_layout(&no_ovf) {
            Err(StoreError::Layout(m)) => {
                assert!(m.contains("scripthash*"), "{m}");
            }
            other => panic!("expected Layout, got {other:?}"),
        }
        let created = tmp();
        let _t = ScriptHashTable::create(&created).unwrap();
        assert_eq!(
            detect_sh_body_layout(&created).unwrap(),
            ShBodyLayout::Sharded
        );
        assert!(created.join("scripthash.body").is_dir());
        assert!(created.join("scripthash.body").join("00").is_file());
        assert!(created.join("scripthash.ovf").join("body").is_file());
        let _ = std::fs::remove_dir_all(&file_dir);
        let _ = std::fs::remove_dir_all(&dir_dir);
        let _ = std::fs::remove_dir_all(&mixed);
        let _ = std::fs::remove_dir_all(&no_ovf);
        let _ = std::fs::remove_dir_all(&created);
    }

    fn rec(sh: [u8; 32], tx: u64, _vout: u32) -> ScriptHashRecord {
        ScriptHashRecord::from_fk(sh, Fk(tx))
    }

    fn put_unique(t: &ScriptHashTable, tag: u8, n: u32) {
        for i in 0..n {
            let sh = script_hash(&[tag, (i & 0xff) as u8, (i >> 8) as u8, 0x7e]);
            t.put_create(&rec(sh, u64::from(i) + 1, 0)).unwrap();
        }
    }

    #[test]
    fn script_hash_record_helpers_and_table_flush_open() {
        let e = ShEntry::new(Fk(9));
        let r = ScriptHashRecord::from_entry([1u8; 32], e);
        assert_eq!(r.entry(), e);
        assert!(!r.is_tombstone());
        let tomb = ScriptHashRecord::from_fk([2u8; 32], Fk::NULL);
        assert!(tomb.is_tombstone());
        let _ = script_hash(&[0x00, 0x14]);

        let dir = tmp();
        let t = ScriptHashTable::create(&dir).unwrap();
        let sh = script_hash(&[0x99]);
        t.put_create(&rec(sh, 1, 0)).unwrap();
        let _ = t.put_create_batch(&[]);
        assert_eq!(t.entry_count(), 1);
        t.flush().unwrap();
        t.flush_async().unwrap();
        drop(t);
        let t = ScriptHashTable::open(&dir).unwrap();
        assert_eq!(t.entries(&sh).unwrap().len(), 1);
        // for_each_live across table
        let mut n = 0u32;
        t.for_each_live_create(|_fk| {
            n += 1;
        })
        .unwrap();
        assert_eq!(n, 1);
        // missing key
        assert!(t.entries(&[0u8; 32]).unwrap().is_empty());
        assert!(t.head_value(&[0u8; 32]).unwrap().is_none());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn scripthash_thin_roundtrip() {
        let dir = tmp();
        let t = ScriptHashTable::create(&dir).unwrap();
        let sh = script_hash(&[0x51]);
        t.put_create(&rec(sh, 3, 0)).unwrap();
        let entries = t.entries(&sh).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].1.create_tx_fk, Fk(3));
        t.put_create(&rec(sh, 3, 0)).unwrap();
        assert_eq!(t.entries(&sh).unwrap().len(), 1);
        t.put_create(&rec(sh, 4, 1)).unwrap();
        assert_eq!(t.entries(&sh).unwrap().len(), 2);
        assert!(t.unlink_create(&sh, Fk(4), 1).unwrap());
        assert_eq!(t.entries(&sh).unwrap().len(), 1);
        assert!(t.unlink_create(&sh, Fk(3), 0).unwrap());
        assert!(t.entries(&sh).unwrap().is_empty());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn incremental_absent_lands_on_ingest() {
        let dir = tmp();
        let t = ScriptHashTable::create(&dir).unwrap();
        let sh = script_hash(&[0x51]);
        t.put_create(&rec(sh, 3, 0)).unwrap();
        assert_eq!(t.entries(&sh).unwrap().len(), 1);
        assert!(
            t.ingest.lock().unwrap().get(&sh).unwrap().is_some(),
            "new key must live on ingest, not live OA main"
        );
        assert!(
            !dir.join("scripthash.head").exists(),
            "create must not plant a live OA at scripthash.head"
        );
        t.flush().unwrap();
        drop(t);
        let t = ScriptHashTable::open(&dir).unwrap();
        assert_eq!(t.entries(&sh).unwrap().len(), 1);
        assert!(!dir.join("scripthash.head").exists());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn put_create_uses_slabs_then_pages() {
        let dir = tmp();
        let t = ScriptHashTable::create(&dir).unwrap();
        let sh = script_hash(&[0x15]);
        for i in 1..=5u64 {
            t.put_create(&rec(sh, i, 0)).unwrap();
        }
        match t.head_value(&sh).unwrap().unwrap() {
            ShHeadValue::Slab { class, used, off } => {
                assert_eq!(class, 1, "5 fks with slack → class 1 (64 B, cap 8)");
                assert_eq!(used, 5);
                assert!(off >= 4096);
            }
            other => panic!("expected class-1 slab, got {other:?}"),
        }
        assert_eq!(t.entries(&sh).unwrap().len(), 5);
        for i in 6..=9u64 {
            t.put_create(&rec(sh, i, 0)).unwrap();
        }
        match t.head_value(&sh).unwrap().unwrap() {
            ShHeadValue::Slab { class, used, .. } => {
                assert_eq!(class, 2, "9th fk grows class 1 → 2");
                assert_eq!(used, 9);
            }
            other => panic!("expected class-2 slab, got {other:?}"),
        }
        assert_eq!(
            t.put_create_batch(&[rec(sh, 9, 0), rec(sh, 5, 0)]).unwrap(),
            0,
            "fk ≤ max is a skip"
        );
        let rest: Vec<_> = (10..=257u64).map(|i| rec(sh, i, 0)).collect();
        assert_eq!(t.put_create_batch(&rest).unwrap(), 248);
        match t.head_value(&sh).unwrap().unwrap() {
            ShHeadValue::Extent { last_page } => {
                assert!(last_page > 0);
            }
            other => panic!("expected page chain at 257, got {other:?}"),
        }
        assert_eq!(t.entries(&sh).unwrap().len(), 257);
        assert!(t.contains_create(&sh, Fk(257)).unwrap());
        assert!(!t.contains_create(&sh, Fk(258)).unwrap());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn promote_ladder_inline_to_paged() {
        let dir = tmp();
        let t = ScriptHashTable::create(&dir).unwrap();
        let sh = script_hash(&[0x52]);
        for i in 1..=5u64 {
            t.put_create(&rec(sh, i, i as u32)).unwrap();
        }
        assert_eq!(t.entries(&sh).unwrap().len(), 5);
        let v = t.head_value(&sh).unwrap().unwrap();
        match v {
            ShHeadValue::Slab { class, used, off } => {
                assert_eq!(class, 1);
                assert_eq!(used, 5);
                assert!(off > 0);
            }
            other => panic!("expected slab, got {other:?}"),
        }
        assert_eq!(t.entry_count(), 5);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn put_create_batch_many_uses_pages() {
        let dir = tmp();
        let t = ScriptHashTable::create(&dir).unwrap();
        let sh = script_hash(&[0x53]);
        let recs: Vec<_> = (0..100u32).map(|v| rec(sh, u64::from(v) + 1, v)).collect();
        let n = t.put_create_batch(&recs).unwrap();
        assert_eq!(n, 100);
        let v = t.head_value(&sh).unwrap().unwrap();
        match v {
            ShHeadValue::Slab { class, used, off } => {
                assert_eq!(class, 5, "100 fks → class 5 (cap 128)");
                assert_eq!(used, 100);
                assert!(off > 0);
            }
            other => panic!("expected slab, got {other:?}"),
        }
        assert_eq!(t.entries(&sh).unwrap().len(), 100);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn put_create_batch_chains() {
        let dir = tmp();
        let t = ScriptHashTable::create(&dir).unwrap();
        let sh = script_hash(&[0x51]);
        let recs: Vec<_> = (0..3u32).map(|v| rec(sh, u64::from(v) + 1, v)).collect();
        let n = t.put_create_batch(&recs).unwrap();
        assert_eq!(n, 3);
        assert_eq!(t.entries(&sh).unwrap().len(), 3);
        let n2 = t.put_create_batch(&recs).unwrap();
        assert_eq!(n2, 0);
        assert_eq!(t.entries(&sh).unwrap().len(), 3);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Re-queued lower/equal FKs are skipped; only higher append. Multi-page max
    /// from last page only (sorted chain).
    #[test]
    fn put_create_batch_skips_leq_max_appends_higher() {
        use crate::scripthash_pages::SH_PAGE_STREAM_MAX;
        let dir = tmp();
        let t = ScriptHashTable::create(&dir).unwrap();
        let sh = script_hash(&[0xab]);
        // Fill past one delta page so last page holds the max.
        let n = SH_PAGE_STREAM_MAX + 5;
        let first: Vec<_> = (1..=n as u64).map(|i| rec(sh, i, 0)).collect();
        assert_eq!(t.put_create_batch(&first).unwrap(), n);
        assert_eq!(t.entries(&sh).unwrap().len(), n);
        let max = t
            .last_create_fk_for_key(&sh, &t.head_value(&sh).unwrap().unwrap())
            .unwrap()
            .unwrap();
        assert_eq!(max, Fk(n as u64));

        // Mix re-queued older FKs with new higher ones.
        let batch = vec![
            rec(sh, 1, 0),
            rec(sh, n as u64 / 2, 0),
            rec(sh, n as u64, 0),
            rec(sh, n as u64 + 1, 0),
            rec(sh, n as u64 + 3, 0),
            rec(sh, n as u64 + 2, 0), // unsorted in batch
        ];
        let written = t.put_create_batch(&batch).unwrap();
        assert_eq!(written, 3, "only fks > max must be written");
        let got = t.entries(&sh).unwrap();
        assert_eq!(got.len(), n + 3);
        for (i, (_, e)) in got.iter().enumerate() {
            assert_eq!(e.create_tx_fk.0, (i as u64) + 1);
        }
        // Only-lower batch is no-op.
        assert_eq!(
            t.put_create_batch(&[rec(sh, 1, 0), rec(sh, 2, 0)]).unwrap(),
            0
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn put_create_batch_append_uses_heads() {
        let dir = tmp();
        let t = ScriptHashTable::create(&dir).unwrap();
        let sh = script_hash(&[0x51]);
        let mut heads = HashMap::new();
        let recs: Vec<_> = (0..3u32).map(|v| rec(sh, u64::from(v) + 1, v)).collect();
        let (n, _t) = t.put_create_batch_append(&recs, &mut heads).unwrap();
        assert_eq!(n, 3);
        assert_eq!(t.entries(&sh).unwrap().len(), 3);
        assert!(heads.get(&sh).is_some());
        let more = vec![rec(sh, 10, 9)];
        let (n2, _) = t.put_create_batch_append(&more, &mut heads).unwrap();
        assert_eq!(n2, 1);
        assert_eq!(t.entries(&sh).unwrap().len(), 4);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn page_append_preserves_prefix_and_order() {
        let dir = tmp();
        let t = ScriptHashTable::create(&dir).unwrap();
        let sh = script_hash(&[0x7a]);
        let mut heads = HashMap::new();
        let first: Vec<_> = (1..=5u32).map(|v| rec(sh, u64::from(v), v)).collect();
        let (n, _) = t.put_create_batch_append(&first, &mut heads).unwrap();
        assert_eq!(n, 5);
        let first_off = match t.head_value(&sh).unwrap().unwrap() {
            ShHeadValue::Slab { class, used, off } => {
                assert_eq!(class, 1);
                assert_eq!(used, 5);
                off
            }
            other => panic!("expected slab, got {other:?}"),
        };
        let more: Vec<_> = (6..=7u32).map(|v| rec(sh, u64::from(v), v)).collect();
        let (n2, _) = t.put_create_batch_append(&more, &mut heads).unwrap();
        assert_eq!(n2, 2);
        match t.head_value(&sh).unwrap().unwrap() {
            ShHeadValue::Slab { class, used, off } => {
                assert_eq!(off, first_off, "in-class append must reuse slab off");
                assert_eq!(class, 1);
                assert_eq!(used, 7);
            }
            other => panic!("expected slab, got {other:?}"),
        }
        let ents = t.entries(&sh).unwrap();
        assert_eq!(ents.len(), 7);
        for (i, (_, e)) in ents.iter().enumerate() {
            assert_eq!(e.create_tx_fk, Fk(i as u64 + 1));
        }
        // Grow to megakey pages (≥257 FKs).
        let mut heads2 = HashMap::new();
        let sh2 = script_hash(&[0x7b]);
        let many: Vec<_> = (1..=600u32).map(|v| rec(sh2, u64::from(v), v)).collect();
        let (nm, _) = t.put_create_batch_append(&many, &mut heads2).unwrap();
        assert_eq!(nm, 600);
        match t.head_value(&sh2).unwrap().unwrap() {
            ShHeadValue::Extent { .. } => {}
            other => panic!("expected extent megakey, got {other:?}"),
        }
        assert_eq!(t.entries(&sh2).unwrap().len(), 600);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn unlink_demotes_paged_to_inline() {
        let dir = tmp();
        let t = ScriptHashTable::create(&dir).unwrap();
        let sh = script_hash(&[0x54]);
        for i in 1..=3u64 {
            t.put_create(&rec(sh, i, i as u32)).unwrap();
        }
        assert!(matches!(
            t.head_value(&sh).unwrap().unwrap(),
            ShHeadValue::Slab { .. }
        ));
        t.unlink_create(&sh, Fk(2), 2).unwrap();
        match t.head_value(&sh).unwrap().unwrap() {
            ShHeadValue::Slab { used, .. } => assert_eq!(used, 2),
            other => panic!("expected 2-fk slab, got {other:?}"),
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn ingest_oa_slots_mainnet_is_2_25() {
        HeadScale::test_with(HeadScale::Mainnet, || {
            assert_eq!(ingest_oa_slots(), 1 << 25);
            assert_eq!(SH_HEAD_VALUE_LEN, 8);
            assert_eq!(crate::scripthash_layout::SH_HEAD_SLOT_SIZE, 24);
        });
    }

    #[test]
    fn create_does_not_write_oa_stub() {
        let dir = tmp();
        let t = ScriptHashTable::create(&dir).unwrap();
        assert_eq!(
            t.head_shard_count(),
            shard_count_for_role(HeadRole::ScriptHash)
        );
        drop(t);
        assert!(
            !dir.join("scripthash.head.oa_stub").exists(),
            "create must not write leftover sharded OA stub"
        );
        std::fs::create_dir_all(dir.join("scripthash.head.oa_stub")).unwrap();
        let t = ScriptHashTable::open(&dir).unwrap();
        assert!(
            !dir.join("scripthash.head.oa_stub").exists(),
            "open must unlink leftover oa_stub"
        );
        assert_eq!(
            t.head_shard_count(),
            shard_count_for_role(HeadRole::ScriptHash)
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn leftover_live_oa_main_open_refuses() {
        let dir = tmp();
        let t = ScriptHashTable::create(&dir).unwrap();
        t.put_create(&rec(script_hash(&[0x01]), 1, 0)).unwrap();
        t.flush().unwrap();
        drop(t);
        ShardedScriptHashHead::create_sharded(dir.join("scripthash.head"), 1, 64).unwrap();
        match ScriptHashTable::open(&dir) {
            Ok(_) => panic!("leftover OA main must refuse"),
            Err(StoreError::Layout(m)) => {
                assert!(m.contains("scripthash*"), "{m}");
                assert!(m.contains("wipe") || m.contains("rematerialize"), "{m}");
            }
            Err(e) => panic!("expected Layout, got {e}"),
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn leftover_oa_overflow_seg_open_refuses() {
        let dir = tmp();
        let t = ScriptHashTable::create(&dir).unwrap();
        t.put_create(&rec(script_hash(&[0x02]), 1, 0)).unwrap();
        t.flush().unwrap();
        drop(t);
        let ovf = dir.join("scripthash.ovf");
        std::fs::create_dir_all(&ovf).unwrap();
        std::fs::write(ovf.join("000000"), b"not-shsr").unwrap();
        match ScriptHashTable::open(&dir) {
            Ok(_) => panic!("leftover OA ovf must refuse"),
            Err(StoreError::Layout(m)) => {
                assert!(m.contains("scripthash*"), "{m}");
            }
            Err(e) => panic!("expected Layout, got {e}"),
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn ingest_batch_update_and_new_keys() {
        let dir = tmp();
        let t = ScriptHashTable::create(&dir).unwrap();
        let sh0 = script_hash(&[0xc0, 0, 0, 0x11]);
        t.put_create(&rec(sh0, 1, 0)).unwrap();
        let mut batch = vec![rec(sh0, 99_999, 1)];
        for i in 0..20u32 {
            let sh = script_hash(&[0xc1, (i & 0xff) as u8, 0x22, 0x33]);
            batch.push(rec(sh, 10_000 + u64::from(i), 0));
        }
        assert_eq!(t.put_create_batch(&batch).unwrap(), 21);
        assert_eq!(t.entries(&sh0).unwrap().len(), 2);
        assert!(t.ingest.lock().unwrap().get(&sh0).unwrap().is_some());
        let mut n = 0u64;
        t.for_each_live_create(|_| n += 1).unwrap();
        assert_eq!(n, 22);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn ingest_many_unique_keys_reopen() {
        let dir = tmp();
        let t = ScriptHashTable::create(&dir).unwrap();
        put_unique(&t, 0xa0, 80);
        let sh0 = script_hash(&[0xa0, 0, 0, 0x7e]);
        t.put_create(&rec(sh0, 10_000, 1)).unwrap();
        assert_eq!(t.entries(&sh0).unwrap().len(), 2);
        t.flush().unwrap();
        drop(t);
        let t = ScriptHashTable::open(&dir).unwrap();
        assert_eq!(t.entries(&sh0).unwrap().len(), 2);
        assert!(!dir.join("scripthash.head").exists());
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Empty SHAL v1 (schema-13 body) opens and is rewritten to alloc v2.
    #[test]
    fn open_empty_alloc_v1_upgrades_to_v2() {
        let dir = tmp();
        {
            let t = ScriptHashTable::create(&dir).unwrap();
            assert!(!t.has_durable_index());
            t.flush().unwrap();
        }
        // Downgrade only the version field (layout is identical).
        let body_path = dir.join("scripthash.body").join("00");
        let body = TableFile::open(&body_path, TableKind::ScriptHash).unwrap();
        let (state, ver) = read_alloc_header(&body).unwrap();
        assert_eq!(ver, SH_ALLOC_VERSION);
        // Write v1 stamp with same empty state.
        let mut buf = vec![0u8; SH_ALLOC_HEADER_LEN];
        buf[0..4].copy_from_slice(&SH_ALLOC_MAGIC);
        buf[4..6].copy_from_slice(&1u16.to_le_bytes());
        buf[8..16].copy_from_slice(&state.live_count.to_le_bytes());
        buf[16..24].copy_from_slice(&state.bump.to_le_bytes());
        body.write_at(FILE_HEADER_LEN as u64, &buf).unwrap();
        body.flush().unwrap();
        drop(body);
        assert_eq!(
            read_alloc_version_on_disk(
                &TableFile::open(&body_path, TableKind::ScriptHash).unwrap()
            )
            .unwrap(),
            1
        );

        let t = ScriptHashTable::open(&dir).unwrap();
        assert!(!t.has_durable_index());
        drop(t);
        assert_eq!(
            read_alloc_version_on_disk(
                &TableFile::open(&body_path, TableKind::ScriptHash).unwrap()
            )
            .unwrap(),
            SH_ALLOC_VERSION,
            "empty v1 must be rewritten to current alloc version"
        );
        // Reopen stays v2.
        let t = ScriptHashTable::open(&dir).unwrap();
        t.put_create(&rec(script_hash(&[0x42]), 1, 0)).unwrap();
        assert_eq!(t.entries(&script_hash(&[0x42])).unwrap().len(), 1);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Durable SH with alloc v1 is refused (slab body incompatible with page chains).
    #[test]
    fn open_durable_alloc_v1_refused() {
        let dir = tmp();
        {
            let t = ScriptHashTable::create(&dir).unwrap();
            t.put_create(&rec(script_hash(&[0x99]), 7, 0)).unwrap();
            assert!(t.has_durable_index());
            t.flush().unwrap();
        }
        let body_path = dir.join("scripthash.body").join("00");
        let body = TableFile::open(&body_path, TableKind::ScriptHash).unwrap();
        let (state, _) = read_alloc_header(&body).unwrap();
        let mut buf = vec![0u8; SH_ALLOC_HEADER_LEN];
        buf[0..4].copy_from_slice(&SH_ALLOC_MAGIC);
        buf[4..6].copy_from_slice(&1u16.to_le_bytes());
        buf[8..16].copy_from_slice(&state.live_count.to_le_bytes());
        buf[16..24].copy_from_slice(&state.bump.to_le_bytes());
        let mut off = 24usize;
        for h in &state.free_head {
            buf[off..off + 8].copy_from_slice(&h.to_le_bytes());
            off += 8;
        }
        body.write_at(FILE_HEADER_LEN as u64, &buf).unwrap();
        body.flush().unwrap();
        drop(body);

        match ScriptHashTable::open(&dir) {
            Ok(_) => panic!("expected refuse for durable alloc v1"),
            Err(StoreError::Corrupt(m)) => {
                assert!(
                    m.contains("alloc v1") || m.contains("slab") || m.contains("rematerialize"),
                    "{m}"
                );
            }
            Err(e) => panic!("expected Corrupt, got {e}"),
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Legacy full-size ovf.head is wiped on open; table remains usable.
    #[test]
    fn open_wipes_legacy_fullsize_ovf_head() {
        let dir = tmp();
        {
            let t = ScriptHashTable::create(&dir).unwrap();
            t.put_create(&rec(script_hash(&[0x01]), 1, 0)).unwrap();
            t.flush().unwrap();
        }
        std::fs::write(
            dir.join(crate::scripthash_overflow::LEGACY_OVERFLOW_HEAD),
            b"x",
        )
        .unwrap();
        std::fs::write(
            dir.join(crate::scripthash_overflow::LEGACY_OVERFLOW_FUSE),
            b"SHFUSE01",
        )
        .unwrap();
        let t = ScriptHashTable::open(&dir).unwrap();
        assert!(!dir
            .join(crate::scripthash_overflow::LEGACY_OVERFLOW_HEAD)
            .exists());
        assert_eq!(t.entries(&script_hash(&[0x01])).unwrap().len(), 1);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn freelist_reuses_page() {
        let dir = tmp();
        let t = ScriptHashTable::create(&dir).unwrap();
        let sh1 = script_hash(&[0x61]);
        let sh2 = script_hash(&[0x62]);
        for i in 1..=3u64 {
            t.put_create(&rec(sh1, i, i as u32)).unwrap();
        }
        let off1 = match t.head_value(&sh1).unwrap().unwrap() {
            ShHeadValue::Slab { off, class, .. } => {
                assert_eq!(class, 0);
                off
            }
            other => panic!("expected slab, got {other:?}"),
        };
        for i in 1..=3u64 {
            t.unlink_create(&sh1, Fk(i), i as u32).unwrap();
        }
        for i in 1..=3u64 {
            t.put_create(&rec(sh2, 10 + i, i as u32)).unwrap();
        }
        let off2 = match t.head_value(&sh2).unwrap().unwrap() {
            ShHeadValue::Slab { off, class, .. } => {
                assert_eq!(class, 0);
                off
            }
            other => panic!("expected slab, got {other:?}"),
        };
        assert_eq!(off1, off2, "slab freelist should reuse offset");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn cold_install_sorted_main_and_global_ingest() {
        let dir = tmp();
        let t = ScriptHashTable::create(&dir).unwrap();
        let sh_main = script_hash(&[0x10]);
        let sh_new = script_hash(&[0x99]);
        let mut session = t.bulk_session(16).unwrap();
        session
            .put_chain(sh_main, &[ShEntry::new(Fk(1)), ShEntry::new(Fk(2))])
            .unwrap();
        session.finish().unwrap();
        assert!(
            t.has_sorted_main(),
            "bulk must emit a sealed sorted main shard"
        );
        let head_p = dir.join("scripthash.head");
        let shard_p = if head_p.is_dir() {
            head_p.join("00")
        } else {
            head_p
        };
        assert!(
            MphfHead::exists(&shard_p),
            "bulk must emit mphf+val for shard 00"
        );
        let mut idx = shard_p.as_os_str().to_os_string();
        idx.push(".idx");
        let mut fuse = shard_p.as_os_str().to_os_string();
        fuse.push(".fuse8");
        assert!(!PathBuf::from(idx).is_file());
        assert!(
            !PathBuf::from(fuse).is_file(),
            "main shards must not write a fuse"
        );

        t.put_create(&rec(sh_main, 3, 0)).unwrap();
        assert_eq!(t.entries(&sh_main).unwrap().len(), 3);
        assert!(matches!(t.key_home(&sh_main).unwrap(), KeyHome::Main));

        t.put_create(&rec(sh_new, 10, 0)).unwrap();
        assert_eq!(t.entries(&sh_new).unwrap().len(), 1);
        assert!(matches!(t.key_home(&sh_new).unwrap(), KeyHome::Ingest));
        // First create of a never-seen key must still miss on main (prove Absent).
        // Later hits live on ingest and must not touch the main page.
        t.reset_sorted_main_preads();
        t.put_create(&rec(sh_new, 11, 0)).unwrap();
        assert_eq!(t.entries(&sh_new).unwrap().len(), 2);
        assert!(matches!(t.key_home(&sh_new).unwrap(), KeyHome::Ingest));
        assert_eq!(
            t.sorted_main_pread_count(t.shard_index(&sh_new)),
            0,
            "key already on ingest must not pread the main page"
        );
        t.flush().unwrap();
        drop(t);
        let t = ScriptHashTable::open(&dir).unwrap();
        assert_eq!(t.entries(&sh_main).unwrap().len(), 3);
        assert_eq!(t.entries(&sh_new).unwrap().len(), 2);
        assert!(matches!(t.key_home(&sh_main).unwrap(), KeyHome::Main));
        assert!(matches!(t.key_home(&sh_new).unwrap(), KeyHome::Ingest));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn reopen_after_ingest_seal_and_unlink_homes() {
        HeadScale::test_with(HeadScale::Tiny, || {
            let dir = tmp();
            let t = ScriptHashTable::create(&dir).unwrap();
            let sh_main = script_hash(&[0x10]);
            let mut session = t.bulk_session(8).unwrap();
            session.put_chain(sh_main, &[ShEntry::new(Fk(1))]).unwrap();
            session.finish().unwrap();

            let mut first_new = [0u8; 32];
            for i in 0..210u32 {
                let sh = script_hash(&[0xa1, (i & 0xff) as u8, (i >> 8) as u8, 0x01]);
                if i == 0 {
                    first_new = sh;
                }
                t.put_create(&rec(sh, 1000 + u64::from(i), 0)).unwrap();
            }
            assert_eq!(t.sealed_ovf.lock().unwrap().len(), 1);
            assert!(matches!(
                t.key_home(&first_new).unwrap(),
                KeyHome::SealedOvf
            ));

            t.unlink_create(&sh_main, Fk(1), 0).unwrap();
            assert!(t.entries(&sh_main).unwrap().is_empty());
            t.unlink_create(&first_new, Fk(1000), 0).unwrap();
            assert!(t.entries(&first_new).unwrap().is_empty());

            t.flush().unwrap();
            drop(t);
            let t = ScriptHashTable::open(&dir).unwrap();
            assert!(t.entries(&sh_main).unwrap().is_empty());
            assert!(t.entries(&first_new).unwrap().is_empty());
            assert!(matches!(t.key_home(&sh_main).unwrap(), KeyHome::Main));
            assert!(matches!(
                t.key_home(&first_new).unwrap(),
                KeyHome::SealedOvf
            ));
            let _ = std::fs::remove_dir_all(&dir);
        });
    }

    #[test]
    fn compact_merges_two_sealed_global_ovf_files() {
        HeadScale::test_with(HeadScale::Tiny, || {
            let dir = tmp();
            let t = ScriptHashTable::create(&dir).unwrap();
            let sh_main = script_hash(&[0x10]);
            let mut session = t.bulk_session(8).unwrap();
            session.put_chain(sh_main, &[ShEntry::new(Fk(1))]).unwrap();
            session.finish().unwrap();

            let mut first_new = [0u8; 32];
            let mut second_new = [0u8; 32];
            for i in 0..210u32 {
                let sh = script_hash(&[0xa1, (i & 0xff) as u8, (i >> 8) as u8, 0x01]);
                if i == 0 {
                    first_new = sh;
                }
                t.put_create(&rec(sh, 1000 + u64::from(i), 0)).unwrap();
            }
            assert_eq!(t.sealed_ovf.lock().unwrap().len(), 1, "first ingest seal");
            for i in 0..210u32 {
                let sh = script_hash(&[0xa2, (i & 0xff) as u8, (i >> 8) as u8, 0x02]);
                if i == 0 {
                    second_new = sh;
                }
                t.put_create(&rec(sh, 2000 + u64::from(i), 0)).unwrap();
            }
            assert_eq!(t.sealed_ovf.lock().unwrap().len(), 2, "second ingest seal");

            t.compact_sealed_ovf().unwrap();
            assert_eq!(
                t.sealed_ovf.lock().unwrap().len(),
                0,
                "L0 unlinked after promote"
            );
            assert!(
                t.ovf_l1.lock().unwrap().is_some(),
                "compact promotes L1 MPHF"
            );
            assert_eq!(t.entries(&first_new).unwrap().len(), 1);
            assert_eq!(t.entries(&second_new).unwrap().len(), 1);
            assert_eq!(t.entries(&sh_main).unwrap().len(), 1);
            assert!(matches!(t.key_home(&sh_main).unwrap(), KeyHome::Main));
            assert!(matches!(
                t.key_home(&first_new).unwrap(),
                KeyHome::SealedOvf
            ));

            t.compact_sealed_ovf().unwrap();
            assert!(t.ovf_l1.lock().unwrap().is_some());
            assert_eq!(t.sealed_ovf.lock().unwrap().len(), 0);
            assert_eq!(t.entries(&first_new).unwrap().len(), 1);

            for i in 0..210u32 {
                let sh = script_hash(&[0xa3, (i & 0xff) as u8, (i >> 8) as u8, 0x03]);
                t.put_create(&rec(sh, 3000 + u64::from(i), 0)).unwrap();
            }
            assert_eq!(t.sealed_ovf.lock().unwrap().len(), 1);
            t.compact_sealed_ovf().unwrap();
            assert_eq!(t.sealed_ovf.lock().unwrap().len(), 1, "L1 frozen: L0 stays");
            assert_eq!(t.entries(&first_new).unwrap().len(), 1);
            let _ = std::fs::remove_dir_all(&dir);
        });
    }

    #[test]
    fn bulk_session_packs_exact_class_from_count() {
        let dir = tmp();
        let t = ScriptHashTable::create(&dir).unwrap();
        let mut session = t.bulk_session(16).unwrap();
        let cases: &[(u8, u32)] = &[(0x01, 1), (0x02, 2), (0x06, 6), (0x14, 20), (0x60, 600)];
        for &(tag, n) in cases {
            let mut sh = [0u8; 32];
            sh[0] = tag;
            let ents: Vec<_> = (1..=u64::from(n)).map(|i| ShEntry::new(Fk(i))).collect();
            session.put_chain(sh, &ents).unwrap();
        }
        let (creates, keys, _, _) = session.finish().unwrap();
        assert_eq!(keys, 5);
        assert_eq!(creates, 1 + 2 + 6 + 20 + 600);

        let sh = |tag: u8| {
            let mut k = [0u8; 32];
            k[0] = tag;
            k
        };
        assert!(matches!(
            t.head_value(&sh(0x01)).unwrap().unwrap(),
            ShHeadValue::Inline { used: 1, .. }
        ));
        assert!(matches!(
            t.head_value(&sh(0x02)).unwrap().unwrap(),
            ShHeadValue::Slab { used: 2, .. }
        ));
        match t.head_value(&sh(0x06)).unwrap().unwrap() {
            ShHeadValue::Slab { class, used, .. } => {
                assert_eq!(class, 0, "6 tight deltas fit class 0 (16 B)");
                assert_eq!(used, 6);
            }
            other => panic!("expected class-0 slab, got {other:?}"),
        }
        match t.head_value(&sh(0x14)).unwrap().unwrap() {
            ShHeadValue::Slab { class, used, .. } => {
                assert_eq!(class, 1, "20 tight deltas fit class 1 (32 B)");
                assert_eq!(used, 20);
            }
            other => panic!("expected class-0 slab, got {other:?}"),
        }
        match t.head_value(&sh(0x60)).unwrap().unwrap() {
            ShHeadValue::Slab { class, used, .. } => {
                assert_eq!(used, 600);
                assert!(
                    class <= 6,
                    "600 1-byte deltas stay in a relocating slab, class={class}"
                );
            }
            other => panic!("expected slab for 600 tight deltas, got {other:?}"),
        }
        assert_eq!(t.entries(&sh(0x06)).unwrap().len(), 6);
        assert_eq!(t.entries(&sh(0x60)).unwrap().len(), 600);

        let payload = t.body().logical_len().saturating_sub(4096);
        let tight = 32 + 32 + 1024;
        assert!(
            payload <= 2 * tight,
            "cold body {payload} must stay within 2× packed {tight}"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn bulk_session_put_chain_roundtrip() {
        let dir = tmp();
        let t = ScriptHashTable::create(&dir).unwrap();
        let mut session = t.bulk_session(100).unwrap();
        // Many distinct keys, mix of inline and slab.
        for i in 0..50u32 {
            let mut sh = [0u8; 32];
            sh[0] = i as u8;
            sh[1] = 0xab;
            let n = if i % 5 == 0 { 8 } else { 1 + (i % 2) };
            let ents: Vec<_> = (0..n)
                .map(|j| ShEntry::new(Fk(u64::from(i) * 100 + u64::from(j) + 1)))
                .collect();
            session.put_chain(sh, &ents).unwrap();
        }
        let (creates, keys, _, _) = session.finish().unwrap();
        assert_eq!(keys, 50);
        assert_eq!(creates, t.entry_count());
        assert!(creates > 50);
        // Spot-check a slab key (i=0 → 8 creates).
        let mut sh0 = [0u8; 32];
        sh0[1] = 0xab;
        assert_eq!(t.entries(&sh0).unwrap().len(), 8);
        // Spot-check inline.
        let mut sh1 = [0u8; 32];
        sh1[0] = 1;
        sh1[1] = 0xab;
        assert_eq!(t.entries(&sh1).unwrap().len(), 2);
        t.flush().unwrap();
        let t2 = ScriptHashTable::open(&dir).unwrap();
        assert_eq!(t2.entry_count(), creates);
        assert_eq!(t2.entries(&sh0).unwrap().len(), 8);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn bulk_session_stream_megakey_caps_buf_at_page() {
        use crate::scripthash_pages::SH_PAGE_STREAM_MAX;
        let dir = tmp();
        let t = ScriptHashTable::create(&dir).unwrap();
        let n = SH_PAGE_STREAM_MAX + 10;
        let mut sh = [0u8; 32];
        sh[0] = 0x42;
        let mut session = t.bulk_session(1).unwrap();
        let mut peak = 0usize;
        for i in 1..=n as u64 {
            session.push_sorted_fk(sh, Fk(i)).unwrap();
            peak = peak.max(session.buffered_fks());
            assert!(
                session.buffered_fks() <= SH_PAGE_STREAM_MAX,
                "buf={} after fk={i}",
                session.buffered_fks()
            );
        }
        session.finish_key().unwrap();
        assert!(peak <= SH_PAGE_STREAM_MAX, "peak buf={peak}");
        let (creates, keys, _, _) = session.finish().unwrap();
        assert_eq!(keys, 1);
        assert_eq!(creates, n as u64);
        assert_eq!(t.entries(&sh).unwrap().len(), n);
        match t.head_value(&sh).unwrap().unwrap() {
            ShHeadValue::Extent { last_page } => {
                let w = u64::from_le_bytes(pack8_bytes(&ShHeadValue::extent(last_page)).unwrap());
                assert_eq!(w >> 62, 3);
                let mut page = [0u8; SH_PAGE_SIZE];
                t.body().read_at(last_page, &mut page).unwrap();
                let (base, n) = sh_page_extent(sh_page_as_array(&page).unwrap())
                    .unwrap()
                    .expect("ver=2 last page");
                assert_eq!(n, 2);
                assert_eq!(last_page, base + SH_PAGE_SIZE as u64);
            }
            other => panic!("expected extent, got {other:?}"),
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn remap_sh_body() {
        use crate::scripthash_pages::SH_PAGE_STREAM_MAX;
        let src_dir = tmp();
        let dst_dir = tmp();
        let src = ScriptHashTable::create(&src_dir).unwrap();
        let mut slab_key = [0u8; 32];
        slab_key[0] = 0x11;
        let mut mega_key = [0u8; 32];
        mega_key[0] = 0x22;
        let n_mega = SH_PAGE_STREAM_MAX + 10;
        let mut session = src.bulk_session(2).unwrap();
        for i in 1..=8u64 {
            session.push_sorted_fk(slab_key, Fk(i)).unwrap();
        }
        session.finish_key().unwrap();
        for i in 1..=n_mega as u64 {
            session.push_sorted_fk(mega_key, Fk(i)).unwrap();
        }
        session.finish_key().unwrap();
        let _ = session.finish().unwrap();
        let slab_val = src.head_value(&slab_key).unwrap().unwrap();
        let mega_val = src.head_value(&mega_key).unwrap().unwrap();
        assert!(matches!(slab_val, ShHeadValue::Slab { .. }));
        assert!(matches!(mega_val, ShHeadValue::Extent { .. }));
        let src_lo = payload_start(FILE_HEADER_LEN);
        let src_hi = src.alloc_bump();
        let delta = 3 * SH_PAGE_SIZE as u64;
        let dst = ScriptHashTable::create(&dst_dir).unwrap();
        copy_sh_body_range(src.body(), src_lo, src_hi, dst.body(), src_lo + delta).unwrap();
        let slab_r = remap_sh_head_value(&slab_val, delta);
        let mega_r = remap_sh_head_value(&mega_val, delta);
        if let ShHeadValue::Extent { last_page } = mega_r {
            let mut page = [0u8; SH_PAGE_SIZE];
            dst.body().read_at(last_page, &mut page).unwrap();
            let (base, _) = sh_page_extent(sh_page_as_array(&page).unwrap())
                .unwrap()
                .expect("ver=2 last page");
            remap_copied_page_chain(dst.body(), base.saturating_add(delta), delta).unwrap();
        }
        let recs = vec![
            (head_key_from_full(&slab_key), pack8_bytes(&slab_r).unwrap()),
            (head_key_from_full(&mega_key), pack8_bytes(&mega_r).unwrap()),
        ];
        dst.publish_sorted_shard(0, &recs, 8 + n_mega as u64, src_hi + delta)
            .unwrap();
        assert_eq!(dst.entries(&slab_key).unwrap().len(), 8);
        assert_eq!(dst.entries(&mega_key).unwrap().len(), n_mega);
        let _ = std::fs::remove_dir_all(&src_dir);
        let _ = std::fs::remove_dir_all(&dst_dir);
    }

    #[test]
    fn pack_one_shard() {
        HeadScale::test_with(HeadScale::Tiny, || {
            let dir = tmp();
            let t = four_shard_dir_table(&dir);
            assert_eq!(t.head_shard_count(), 4);
            let key = |shard: u8, i: u8| {
                let mut k = [0u8; 32];
                k[0] = shard << 6 | (i & 0x3f);
                k
            };
            let k0 = key(0, 0);
            let k1 = key(0, 1);
            let mut session = t.pack_shard_session(0).unwrap();
            session.push_sorted_fk(k0, Fk(1)).unwrap();
            session.push_sorted_fk(k1, Fk(2)).unwrap();
            let pack = session.finish_pack().unwrap();
            assert_eq!(pack.keys, 2);
            let bump0 = t.alloc_bump();
            let new_bump = t.publish_packed_shard(0, pack).unwrap();
            assert!(new_bump >= bump0);
            assert_eq!(t.entries(&k0).unwrap().len(), 1);
            assert_eq!(t.entries(&k1).unwrap().len(), 1);
            assert!(t.head_value(&key(1, 0)).unwrap().is_none());
            let _ = std::fs::remove_dir_all(&dir);
        });
    }

    fn shard0_key(i: u8) -> [u8; 32] {
        let mut k = [0u8; 32];
        k[0] = i & 0x3f;
        k
    }

    #[test]
    fn bulk_dense_five_fks_use_class0_slab() {
        HeadScale::test_with(HeadScale::Tiny, || {
            let dir = tmp();
            let t = four_shard_table(&dir);
            let k = shard0_key(1);
            let mut session = t.pack_shard_session(0).unwrap();
            for fk in 1..=5u64 {
                session.push_sorted_fk(k, Fk(fk)).unwrap();
            }
            let pack = session.finish_pack().unwrap();
            t.publish_packed_shard(0, pack).unwrap();
            match t.head_value(&k).unwrap().unwrap() {
                ShHeadValue::Slab { class, used, .. } => {
                    assert_eq!(used, 5);
                    assert_eq!(class, 0, "5 tight deltas must fit a 32 B class-0 slab");
                }
                other => panic!("expected class-0 slab, got {other:?}"),
            }
            assert_eq!(t.entries(&k).unwrap().len(), 5);
            let _ = std::fs::remove_dir_all(&dir);
        });
    }

    #[test]
    fn bulk_dense_over_cap_stays_slab_until_deltas_fill() {
        HeadScale::test_with(HeadScale::Tiny, || {
            let dir = tmp();
            let t = four_shard_table(&dir);
            let k = shard0_key(2);
            let n = 300u64;
            let mut session = t.pack_shard_session(0).unwrap();
            for fk in 1..=n {
                session.push_sorted_fk(k, Fk(fk)).unwrap();
            }
            let pack = session.finish_pack().unwrap();
            t.publish_packed_shard(0, pack).unwrap();
            match t.head_value(&k).unwrap().unwrap() {
                ShHeadValue::Slab { class, used, .. } => {
                    assert_eq!(used, n as u16);
                    assert!(
                        class <= 5,
                        "300 1-byte deltas (~302 B payload) must not jump to pages; class={class}"
                    );
                }
                other => panic!("expected slab, got {other:?}"),
            }
            assert_eq!(t.entries(&k).unwrap().len(), n as usize);
            let _ = std::fs::remove_dir_all(&dir);
        });
    }

    #[test]
    fn bulk_reuses_page_align_gap_for_later_slab() {
        HeadScale::test_with(HeadScale::Tiny, || {
            let dir = tmp();
            let t = four_shard_table(&dir);
            let payload0 = payload_start(FILE_HEADER_LEN);
            let small_a = shard0_key(3);
            let mega = shard0_key(4);
            let small_b = shard0_key(5);
            let mut session = t.pack_shard_session(0).unwrap();
            for fk in 1..=5u64 {
                session.push_sorted_fk(small_a, Fk(fk)).unwrap();
            }
            for fk in 1..=2100u64 {
                session.push_sorted_fk(mega, Fk(fk)).unwrap();
            }
            for fk in 1..=5u64 {
                session.push_sorted_fk(small_b, Fk(fk)).unwrap();
            }
            let pack = session.finish_pack().unwrap();
            let bump = pack.bump;
            t.publish_packed_shard(0, pack).unwrap();
            let page = SH_PAGE_SIZE as u64;
            let aligned_after_first_slab = (payload0 + 32 + page - 1) & !(page - 1);
            assert_eq!(
                bump,
                aligned_after_first_slab + page,
                "second class-0 slab must come from the align-gap freelist; bump={bump}"
            );
            match t.head_value(&small_b).unwrap().unwrap() {
                ShHeadValue::Slab { off, class, .. } => {
                    assert_eq!(class, 0);
                    assert!(
                        off >= payload0 && off < aligned_after_first_slab,
                        "reused slab off={off} must sit in the gap before the megakey page"
                    );
                }
                other => panic!("expected reused slab, got {other:?}"),
            }
            assert_eq!(t.entries(&small_a).unwrap().len(), 5);
            assert_eq!(t.entries(&mega).unwrap().len(), 2100);
            assert_eq!(t.entries(&small_b).unwrap().len(), 5);
            let _ = std::fs::remove_dir_all(&dir);
        });
    }

    fn four_shard_table(dir: &std::path::Path) -> ScriptHashTable {
        four_shard_dir_table(dir)
    }

    fn shared_body_table(dir: &std::path::Path) -> ScriptHashTable {
        let body = TableFile::create(dir.join("scripthash.body"), TableKind::ScriptHash).unwrap();
        let payload0 = payload_start(FILE_HEADER_LEN);
        body.ensure_capacity(payload0).unwrap();
        body.set_logical_len(payload0).unwrap();
        write_alloc_header(
            &body,
            &AllocState {
                live_count: 0,
                bump: payload0,
                free_head: [0; SH_MAX_CLASS as usize + 1],
            },
        )
        .unwrap();
        drop(body);
        ScriptHashTable::open(dir).unwrap()
    }

    #[test]
    fn materialize_parallel_matches_serial() {
        HeadScale::test_with(HeadScale::Tiny, || {
            let dir = tmp();
            let runs_dir = dir.join("runs");
            std::fs::create_dir_all(&runs_dir).unwrap();
            let key = |shard: u8, i: u8| {
                let mut k = [0u8; 32];
                k[0] = shard << 6 | (i & 0x3f);
                k
            };
            let rec = |shard: u8, i: u8, fk: u64| {
                let mut r = [0u8; 40];
                r[..32].copy_from_slice(&key(shard, i));
                r[32..40].copy_from_slice(&fk.to_le_bytes());
                r
            };
            let mut a = Vec::new();
            let mut b = Vec::new();
            for shard in 0..4u8 {
                for i in 0..3u8 {
                    let fk = u64::from(shard) * 10 + u64::from(i) + 1;
                    if i % 2 == 0 {
                        a.extend_from_slice(&rec(shard, i, fk));
                    } else {
                        b.extend_from_slice(&rec(shard, i, fk));
                    }
                }
            }
            crate::sorted_run::write_sorted_run(&runs_dir.join("000001.run"), 40, 40, &a).unwrap();
            crate::sorted_run::write_sorted_run(&runs_dir.join("000002.run"), 40, 40, &b).unwrap();
            let inputs = [
                crate::sorted_run::open_run(&runs_dir.join("000001.run")).unwrap(),
                crate::sorted_run::open_run(&runs_dir.join("000002.run")).unwrap(),
            ];

            let serial_dir = dir.join("serial");
            std::fs::create_dir_all(&serial_dir).unwrap();
            let serial = four_shard_dir_table(&serial_dir);
            let s = crate::materialize_sh_shards(&serial, &inputs, 0, 1, None).unwrap();

            let par_dir = dir.join("par");
            std::fs::create_dir_all(&par_dir).unwrap();
            let par = four_shard_dir_table(&par_dir);
            assert_eq!(par.body_layout(), ShBodyLayout::Sharded);
            let p = crate::materialize_sh_shards(&par, &inputs, 0, 2, None).unwrap();

            assert_eq!(s.creates, p.creates);
            assert_eq!(s.keys, p.keys);
            assert_eq!(serial.entry_count(), par.entry_count());
            for shard in 0..4u8 {
                for i in 0..3u8 {
                    let k = key(shard, i);
                    assert_eq!(
                        serial.entries(&k).unwrap().len(),
                        par.entries(&k).unwrap().len(),
                        "shard={shard} i={i}"
                    );
                }
            }
            let _ = std::fs::remove_dir_all(&dir);
        });
    }

    #[test]
    fn materialize_parallel_resume() {
        HeadScale::test_with(HeadScale::Tiny, || {
            let dir = tmp();
            let runs_dir = dir.join("runs");
            std::fs::create_dir_all(&runs_dir).unwrap();
            let key = |shard: u8, i: u8| {
                let mut k = [0u8; 32];
                k[0] = shard << 6 | (i & 0x3f);
                k
            };
            let rec = |shard: u8, i: u8, fk: u64| {
                let mut r = [0u8; 40];
                r[..32].copy_from_slice(&key(shard, i));
                r[32..40].copy_from_slice(&fk.to_le_bytes());
                r
            };
            let mut body = Vec::new();
            for shard in 0..4u8 {
                body.extend_from_slice(&rec(shard, 0, u64::from(shard) + 1));
            }
            crate::sorted_run::write_sorted_run(&runs_dir.join("000001.run"), 40, 40, &body)
                .unwrap();
            let inputs = [crate::sorted_run::open_run(&runs_dir.join("000001.run")).unwrap()];

            let t = four_shard_dir_table(&dir);
            let k0 = key(0, 0);
            let mut session = t.pack_shard_session(0).unwrap();
            session.push_sorted_fk(k0, Fk(1)).unwrap();
            let pack = session.finish_pack().unwrap();
            let bump1 = t.publish_packed_shard(0, pack).unwrap();
            ColdProgress {
                next_shard: 1,
                body_bump: bump1,
                live_count: 1,
                keys_written: 1,
            }
            .store(&dir)
            .unwrap();
            assert_eq!(t.entries(&k0).unwrap().len(), 1);

            let cancel = AtomicBool::new(true);
            let err = crate::materialize_sh_shards(&t, &inputs, 1, 2, Some(&cancel));
            assert!(matches!(err, Err(StoreError::Cancelled(_))));
            assert_eq!(
                t.entries(&k0).unwrap()[0].1.create_tx_fk,
                Fk(1),
                "published shard 0 must survive cancel"
            );

            let resume_dir = dir.join("resume");
            std::fs::create_dir_all(&resume_dir).unwrap();
            let t2 = four_shard_dir_table(&resume_dir);
            let mut session = t2.pack_shard_session(0).unwrap();
            session.push_sorted_fk(k0, Fk(1)).unwrap();
            let pack = session.finish_pack().unwrap();
            let b = t2.publish_packed_shard(0, pack).unwrap();
            ColdProgress {
                next_shard: 1,
                body_bump: b,
                live_count: 1,
                keys_written: 1,
            }
            .store(t2.store_dir())
            .unwrap();
            crate::materialize_sh_shards(&t2, &inputs, 1, 2, None).unwrap();
            assert_eq!(t2.entries(&k0).unwrap()[0].1.create_tx_fk, Fk(1));
            assert_eq!(t2.entries(&key(3, 0)).unwrap().len(), 1);
            let _ = std::fs::remove_dir_all(&dir);
        });
    }

    #[test]
    fn materialize_sharded_resume_keeps_sealed_holes() {
        HeadScale::test_with(HeadScale::Tiny, || {
            let dir = tmp();
            let runs_dir = dir.join("runs");
            std::fs::create_dir_all(&runs_dir).unwrap();
            let key = |shard: u8, i: u8| {
                let mut k = [0u8; 32];
                k[0] = shard << 6 | (i & 0x3f);
                k
            };
            let rec = |shard: u8, i: u8, fk: u64| {
                let mut r = [0u8; 40];
                r[..32].copy_from_slice(&key(shard, i));
                r[32..40].copy_from_slice(&fk.to_le_bytes());
                r
            };
            let mut body = Vec::new();
            for shard in 0..4u8 {
                body.extend_from_slice(&rec(shard, 0, u64::from(shard) + 1));
            }
            crate::sorted_run::write_sorted_run(&runs_dir.join("000001.run"), 40, 40, &body)
                .unwrap();
            let inputs = [crate::sorted_run::open_run(&runs_dir.join("000001.run")).unwrap()];
            let t = four_shard_dir_table(&dir);
            for shard in [0usize, 2] {
                let k = key(shard as u8, 0);
                let mut session = t.pack_shard_session(shard).unwrap();
                session
                    .push_sorted_fk(k, Fk(u64::from(shard as u8) + 1))
                    .unwrap();
                let pack = session.finish_pack().unwrap();
                t.publish_packed_shard(shard, pack).unwrap();
            }
            assert_eq!(t.entries(&key(0, 0)).unwrap().len(), 1);
            assert_eq!(t.entries(&key(2, 0)).unwrap().len(), 1);
            assert!(
                MphfHead::exists(&dir.join("scripthash.head").join("02")),
                "shard 2 head is the durable mphf+val commit"
            );
            ColdProgress {
                next_shard: 1,
                body_bump: 0,
                live_count: 2,
                keys_written: 2,
            }
            .store(&dir)
            .unwrap();
            t.prepare_cold_resume(&ColdProgress::load(&dir).unwrap().unwrap())
                .unwrap();
            assert_eq!(
                t.entries(&key(2, 0)).unwrap().len(),
                1,
                "sealed shard 2 must survive a hole at shard 1"
            );
            crate::materialize_sh_shards(&t, &inputs, 1, 2, None).unwrap();
            for shard in 0..4u8 {
                assert_eq!(t.entries(&key(shard, 0)).unwrap().len(), 1, "shard {shard}");
            }
            let _ = std::fs::remove_dir_all(&dir);
        });
    }

    #[test]
    fn materialize_no_temp_body() {
        use crate::scripthash_pages::SH_PAGE_STREAM_MAX;
        HeadScale::test_with(HeadScale::Tiny, || {
            let dir = tmp();
            let runs_dir = dir.join("runs");
            std::fs::create_dir_all(&runs_dir).unwrap();
            let rec = |shard: u8, i: u8, fk: u64| {
                let mut r = [0u8; 40];
                r[..32].copy_from_slice(&sh_prefix_key(shard, i));
                r[32..40].copy_from_slice(&fk.to_le_bytes());
                r
            };
            let mut a = Vec::new();
            let mut b = Vec::new();
            let mega_key = sh_prefix_key(0, 7);
            let n_mega = SH_PAGE_STREAM_MAX + 10;
            for shard in 0..4u8 {
                for i in 0..3u8 {
                    let fk = u64::from(shard) * 10 + u64::from(i) + 1;
                    if i % 2 == 0 {
                        a.extend_from_slice(&rec(shard, i, fk));
                    } else {
                        b.extend_from_slice(&rec(shard, i, fk));
                    }
                }
                if shard == 0 {
                    for i in 1..=n_mega as u64 {
                        a.extend_from_slice(&rec(0, 7, i));
                    }
                }
            }
            crate::sorted_run::write_sorted_run(&runs_dir.join("000001.run"), 40, 40, &a).unwrap();
            crate::sorted_run::write_sorted_run(&runs_dir.join("000002.run"), 40, 40, &b).unwrap();
            let inputs = [
                crate::sorted_run::open_run(&runs_dir.join("000001.run")).unwrap(),
                crate::sorted_run::open_run(&runs_dir.join("000002.run")).unwrap(),
            ];
            let t = four_shard_dir_table(&dir);
            crate::materialize_sh_shards(&t, &inputs, 0, 2, None).unwrap();
            for e in std::fs::read_dir(&dir).unwrap() {
                let n = e.unwrap().file_name();
                let s = n.to_string_lossy();
                assert!(
                    !s.contains("pack") || !s.contains(".body"),
                    "leftover pack body {s}"
                );
            }
            let payload0 = crate::scripthash_layout::payload_start(crate::file::FILE_HEADER_LEN);
            let b0 = t.bodies[0].logical_len();
            let b1 = t.bodies[1].logical_len();
            assert!(
                b0 > b1.saturating_mul(2).max(payload0 + 4096),
                "megakey shard body must dwarf a small sibling, got {b0} vs {b1}"
            );
            assert_eq!(t.entries(&sh_prefix_key(0, 0)).unwrap().len(), 1);
            assert_eq!(t.entries(&mega_key).unwrap().len(), n_mega);
            match t.head_value(&mega_key).unwrap().unwrap() {
                ShHeadValue::Extent { last_page } => {
                    let mut page = [0u8; SH_PAGE_SIZE];
                    t.body().read_at(last_page, &mut page).unwrap();
                    let (base, n) = sh_page_extent(sh_page_as_array(&page).unwrap())
                        .unwrap()
                        .expect("ver=2 last page");
                    assert_eq!(n, 2);
                    assert_eq!(last_page, base + SH_PAGE_SIZE as u64);
                }
                other => panic!("expected extent megakey, got {other:?}"),
            }
            let _ = std::fs::remove_dir_all(&dir);
        });
    }

    #[test]
    fn bulk_session_stream_small_key_still_slab() {
        let dir = tmp();
        let t = ScriptHashTable::create(&dir).unwrap();
        let mut sh = [0u8; 32];
        sh[0] = 0x07;
        let mut session = t.bulk_session(1).unwrap();
        for i in 1..=8u64 {
            session.push_sorted_fk(sh, Fk(i)).unwrap();
            assert_eq!(session.buffered_fks(), i as usize);
        }
        session.finish_key().unwrap();
        let _ = session.finish().unwrap();
        match t.head_value(&sh).unwrap().unwrap() {
            ShHeadValue::Slab { .. } => {}
            other => panic!("expected slab, got {other:?}"),
        }
        assert_eq!(t.entries(&sh).unwrap().len(), 8);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Delta stream in 4073..=4088 B fits `ver=1` but not `ver=2` last-page header.
    #[test]
    fn bulk_session_extent_last_page_splits_when_ver2_header_eats_stream() {
        use crate::scripthash_pages::SH_PAGE_EXTENT_STREAM_MAX;
        let dir = tmp();
        let t = ScriptHashTable::create(&dir).unwrap();
        let n = SH_PAGE_EXTENT_STREAM_MAX + 8;
        let mut sh = [0u8; 32];
        sh[0] = 0x11;
        let ents: Vec<_> = (1..=n as u64).map(|i| ShEntry::new(Fk(i))).collect();
        let mut session = t.bulk_session(1).unwrap();
        session.put_chain(sh, &ents).unwrap();
        let (creates, keys, _, _) = session.finish().unwrap();
        assert_eq!(keys, 1);
        assert_eq!(creates, n as u64);
        assert_eq!(t.entries(&sh).unwrap().len(), n);
        match t.head_value(&sh).unwrap().unwrap() {
            ShHeadValue::Extent { last_page } => {
                let mut page = [0u8; SH_PAGE_SIZE];
                t.body().read_at(last_page, &mut page).unwrap();
                let (base, n_ext) = sh_page_extent(sh_page_as_array(&page).unwrap())
                    .unwrap()
                    .expect("ver=2 last page");
                assert!(
                    n_ext >= 2,
                    "must not pack 4080-byte stream as one ver=2 page"
                );
                assert_ne!(base, last_page);
            }
            other => panic!("expected extent, got {other:?}"),
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Streamed megakey: last remainder can sit in 4073..=4088 B after `ver=1` flushes.
    #[test]
    fn bulk_session_streamed_last_remainder_fits_ver2() {
        use crate::scripthash_pages::{SH_PAGE_EXTENT_STREAM_MAX, SH_PAGE_STREAM_MAX};
        let dir = tmp();
        let t = ScriptHashTable::create(&dir).unwrap();
        let n = SH_PAGE_STREAM_MAX + SH_PAGE_EXTENT_STREAM_MAX + 8;
        let mut sh = [0u8; 32];
        sh[0] = 0x12;
        let ents: Vec<_> = (1..=n as u64).map(|i| ShEntry::new(Fk(i))).collect();
        let mut session = t.bulk_session(1).unwrap();
        session.put_chain(sh, &ents).unwrap();
        let (creates, keys, _, _) = session.finish().unwrap();
        assert_eq!(keys, 1);
        assert_eq!(creates, n as u64);
        assert_eq!(t.entries(&sh).unwrap().len(), n);
        match t.head_value(&sh).unwrap().unwrap() {
            ShHeadValue::Extent { last_page } => {
                let mut page = [0u8; SH_PAGE_SIZE];
                t.body().read_at(last_page, &mut page).unwrap();
                assert!(sh_page_extent(sh_page_as_array(&page).unwrap())
                    .unwrap()
                    .is_some());
            }
            other => panic!("expected extent, got {other:?}"),
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Cold bulk megakey: multi-page chain is contiguous at bump (single-pass pack
    /// writes next links on first write — no previous-page RMW).
    #[test]
    fn bulk_session_megakey_page_chain_contiguous_once() {
        use crate::scripthash_pages::SH_PAGE_STREAM_MAX;
        let dir = tmp();
        let t = ScriptHashTable::create(&dir).unwrap();
        // Sequential FKs fill ~4080/page; this n spans two pages.
        let n = SH_PAGE_STREAM_MAX + 10;
        let mut sh = [0u8; 32];
        sh[0] = 0x10;
        sh[1] = 0xee;
        let ents: Vec<_> = (1..=n as u64).map(|i| ShEntry::new(Fk(i))).collect();
        let mut sh_next = [0u8; 32];
        sh_next[0] = 0x10;
        sh_next[1] = 0xef;
        let next_ents = vec![ShEntry::new(Fk(1)), ShEntry::new(Fk(2))];
        let mut session = t.bulk_session(2).unwrap();
        session.put_chain(sh, &ents).unwrap();
        session.put_chain(sh_next, &next_ents).unwrap();
        let (creates, keys, _, _) = session.finish().unwrap();
        assert_eq!(keys, 2);
        assert_eq!(creates, n as u64 + 2);
        let got = t.entries(&sh).unwrap();
        assert_eq!(got.len(), n);
        for (i, (_, e)) in got.iter().enumerate() {
            assert_eq!(e.create_tx_fk, Fk(i as u64 + 1));
        }
        reset_sh_page_chain_ios();
        let got2 = t.entries(&sh).unwrap();
        assert_eq!(got2.len(), n);
        assert!(
            sh_page_chain_ios() <= 2,
            "contiguous two-page chain should span-read, ios={}",
            sh_page_chain_ios()
        );
        let (first, last, extent_n) = match t.head_value(&sh).unwrap().unwrap() {
            ShHeadValue::Extent { last_page } => {
                let w = u64::from_le_bytes(pack8_bytes(&ShHeadValue::extent(last_page)).unwrap());
                assert_eq!(w >> 62, 3, "pack8 mode 11");
                let mut page = [0u8; SH_PAGE_SIZE];
                t.body().read_at(last_page, &mut page).unwrap();
                let (base, n) = sh_page_extent(sh_page_as_array(&page).unwrap())
                    .unwrap()
                    .expect("ver=2 last page");
                (base, last_page, n)
            }
            other => panic!("expected extent, got {other:?}"),
        };
        assert_eq!(extent_n, 2);
        assert_eq!(
            last,
            first + SH_PAGE_SIZE as u64,
            "tight extent: last = base + (n-1)*4096"
        );
        assert!(first > 0 && first % (SH_PAGE_SIZE as u64) == 0);
        match t.head_value(&sh_next).unwrap().unwrap() {
            ShHeadValue::Slab { off, .. } => {
                assert_eq!(
                    off,
                    last + SH_PAGE_SIZE as u64,
                    "next key must pack at extent_end (no slack hole)"
                );
            }
            other => panic!("expected slab after megakey, got {other:?}"),
        }
        // Tip-path multi-page (write_new_page_chain) also round-trips same size.
        let sh2 = script_hash(&[0xef]);
        let recs: Vec<_> = (1..=n as u32).map(|v| rec(sh2, u64::from(v), v)).collect();
        assert_eq!(t.put_create_batch(&recs).unwrap(), n);
        assert_eq!(t.entries(&sh2).unwrap().len(), n);
        match t.head_value(&sh2).unwrap().unwrap() {
            ShHeadValue::Extent { last_page } => {
                let mut page = [0u8; SH_PAGE_SIZE];
                t.body().read_at(last_page, &mut page).unwrap();
                let (base, n_ext) = sh_page_extent(sh_page_as_array(&page).unwrap())
                    .unwrap()
                    .expect("ver=2 last page");
                assert_eq!(n_ext, 2);
                assert_ne!(base, last_page);
            }
            other => panic!("expected extent, got {other:?}"),
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    fn extent_meta(t: &ScriptHashTable, sh: &[u8; 32]) -> (u64, u64, u32) {
        match t.head_value(sh).unwrap().unwrap() {
            ShHeadValue::Extent { last_page } => {
                let mut page = [0u8; SH_PAGE_SIZE];
                t.body().read_at(last_page, &mut page).unwrap();
                let (base, n) = sh_page_extent(sh_page_as_array(&page).unwrap())
                    .unwrap()
                    .expect("ver=2 last page");
                (base, last_page, n)
            }
            other => panic!("expected extent, got {other:?}"),
        }
    }

    #[test]
    fn extent_append_links_tail_when_bump_moved() {
        let dir = tmp();
        let t = ScriptHashTable::create(&dir).unwrap();
        let n = SH_PAGE_STREAM_MAX + SH_PAGE_EXTENT_STREAM_MAX - 1;
        let mut sh = [0u8; 32];
        sh[0] = 0x21;
        sh[1] = 0xaa;
        let ents: Vec<_> = (1..=n as u64).map(|i| ShEntry::new(Fk(i))).collect();
        let mut sh_gap = [0u8; 32];
        sh_gap[0] = 0x21;
        sh_gap[1] = 0xab;
        let mut session = t.bulk_session(2).unwrap();
        session.put_chain(sh, &ents).unwrap();
        session
            .put_chain(sh_gap, &[ShEntry::new(Fk(1)), ShEntry::new(Fk(2))])
            .unwrap();
        let _ = session.finish().unwrap();
        let (base, last0, n0) = extent_meta(&t, &sh);
        assert_eq!(n0, 2);
        t.put_create(&rec(sh, n as u64 + 1, 0)).unwrap();
        let (base2, last1, n1) = extent_meta(&t, &sh);
        assert_eq!(base2, base);
        assert_eq!(n1, 2, "tail must not bump extent_n");
        assert_ne!(
            last1,
            base + (u64::from(n1) - 1) * SH_PAGE_SIZE as u64,
            "overflow last_page is a linked tail"
        );
        assert_ne!(last1, last0);
        assert_eq!(t.entries(&sh).unwrap().len(), n + 1);
        reset_sh_page_chain_ios();
        assert_eq!(t.entries(&sh).unwrap().len(), n + 1);
        assert!(
            sh_page_chain_ios() >= 3,
            "span + tail read, ios={}",
            sh_page_chain_ios()
        );
        let extra2: Vec<_> = ((n as u64 + 2)..=(n as u64 + 1 + SH_PAGE_EXTENT_STREAM_MAX as u64))
            .map(|i| rec(sh, i, 0))
            .collect();
        assert_eq!(t.put_create_batch(&extra2).unwrap(), extra2.len());
        let (_, last2, n2) = extent_meta(&t, &sh);
        assert_eq!(n2, 2);
        assert_ne!(last2, last1, "second overflow adds another linked page");
        assert_eq!(t.entries(&sh).unwrap().len(), n + 1 + extra2.len());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn extent_append_glued_bumps_extent_n() {
        let dir = tmp();
        let t = ScriptHashTable::create(&dir).unwrap();
        let n = SH_PAGE_STREAM_MAX + SH_PAGE_EXTENT_STREAM_MAX - 1;
        let mut sh = [0u8; 32];
        sh[0] = 0x22;
        let ents: Vec<_> = (1..=n as u64).map(|i| ShEntry::new(Fk(i))).collect();
        let mut session = t.bulk_session(1).unwrap();
        session.put_chain(sh, &ents).unwrap();
        let _ = session.finish().unwrap();
        let (base, _, n0) = extent_meta(&t, &sh);
        assert_eq!(n0, 2);
        t.put_create(&rec(sh, n as u64 + 1, 0)).unwrap();
        let (_, last, n1) = extent_meta(&t, &sh);
        assert_eq!(n1, 3, "glued HWM grows extent_n in place");
        assert_eq!(last, base + 2 * SH_PAGE_SIZE as u64);
        assert_eq!(t.entries(&sh).unwrap().len(), n + 1);
        reset_sh_page_chain_ios();
        assert_eq!(t.entries(&sh).unwrap().len(), n + 1);
        assert!(
            sh_page_chain_ios() <= 2,
            "glued grow stays one span, ios={}",
            sh_page_chain_ios()
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn bulk_session_put_sorted_creates_dedups() {
        let dir = tmp();
        let t = ScriptHashTable::create(&dir).unwrap();
        let sh = script_hash(&[0x99]);
        let recs = vec![
            rec(sh, 1, 0),
            rec(sh, 1, 0), // dup
            rec(sh, 2, 0),
            rec(sh, 3, 0),
        ];
        let mut session = t.bulk_session(1).unwrap();
        let n = session.put_sorted_creates(&recs).unwrap();
        let _ = session.finish().unwrap();
        assert_eq!(n, 3);
        assert_eq!(t.entries(&sh).unwrap().len(), 3);
        assert_eq!(t.entry_count(), 3);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn reinit_clears_head_when_live_count_already_zero() {
        // Crash mid-finish: heads durable, alloc live_count still 0.
        // bulk_session must not hard-error; reinit then cold load.
        let dir = tmp();
        let t = ScriptHashTable::create(&dir).unwrap();
        let mut sh = [0u8; 32];
        sh[0] = 0x7e;
        let mut session = t.bulk_session(1).unwrap();
        session.put_chain(sh, &[ShEntry::new(Fk(42))]).unwrap();
        let _ = session.finish().unwrap();
        assert!(!t.head_is_empty());
        t.test_zero_live_count_keep_head().unwrap();
        assert_eq!(t.entry_count(), 0);
        assert!(!t.head_is_empty());
        // Old bug: only reinit when entry_count>0 → bulk_session fails here.
        assert!(t.bulk_session(1).is_err());
        t.reinit_empty_for_cold_materialize().unwrap();
        assert!(t.head_is_empty());
        assert_eq!(t.entry_count(), 0);
        let mut session = t.bulk_session(2).unwrap();
        session.put_chain(sh, &[ShEntry::new(Fk(1))]).unwrap();
        let (n, _, _, _) = session.finish().unwrap();
        assert_eq!(n, 1);
        assert_eq!(t.entries(&sh).unwrap()[0].1.create_tx_fk, Fk(1));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn bulk_session_flushes_head_on_prefix_shard_boundary() {
        // Live OA image stays off-disk until shard boundary / finish.
        let dir = tmp();
        let t = ScriptHashTable::create(&dir).unwrap();
        const N: u32 = 80_000;
        // Unique 16 B head prefixes (head truncates full 32 B to 16 B).
        let key = |i: u32| {
            let mut sh = [0u8; 32];
            sh[0..4].copy_from_slice(&i.to_le_bytes());
            sh[4] = (i >> 8) as u8; // spread across shard byte for multi-shard
            sh
        };
        let mut session = t.bulk_session(u64::from(N)).unwrap();
        assert!(t.head_value(&key(0)).unwrap().is_none());
        for i in 0..N {
            let sh = key(i);
            session
                .put_chain(sh, &[ShEntry::new(Fk(u64::from(i) + 1))])
                .unwrap();
            // Active shard not yet installed: this key is only in the live image.
            if i == 70_000 {
                assert!(
                    t.head_value(&sh).unwrap().is_none(),
                    "active-shard heads must not land until shard boundary"
                );
            }
        }
        let peak = session.peak_table_bytes;
        let (creates, keys, _, _) = session.finish().unwrap();
        assert_eq!(creates, u64::from(N));
        assert_eq!(keys, u64::from(N));
        assert_eq!(t.entry_count(), u64::from(N));
        // Peak is packed recs (32 B/key), not a 2 GiB OA slot table.
        assert_eq!(peak, (N as usize) * 24);
        // Spot-check a few keys survive live install.
        for i in [0u32, 1, 65_535, 70_000, N - 1] {
            let ents = t.entries(&key(i)).unwrap();
            assert_eq!(ents.len(), 1, "i={i}");
            assert_eq!(ents[0].1.create_tx_fk, Fk(u64::from(i) + 1));
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn cold_progress_and_resume_skips_complete_shards() {
        // 4-way head: fill shard 0, abandon, resume from progress, fill rest.
        HeadScale::test_with(HeadScale::Tiny, || {
            let dir = tmp();
            let t = four_shard_dir_table(&dir);
            assert_eq!(t.head_shard_count(), 4);

            // Keys: shard = full[0] >> 6 for n=4 (top 2 bits).
            let key = |shard: u8, i: u8| {
                let mut k = [0u8; 32];
                k[0] = shard << 6 | (i & 0x3f);
                k
            };
            let mut session = t.bulk_session(64).unwrap();
            for i in 0..8u8 {
                session
                    .put_chain(key(0, i), &[ShEntry::new(Fk(u64::from(i) + 1))])
                    .unwrap();
            }
            // Cross into shard 1 so shard 0 is installed + checkpointed.
            session
                .put_chain(key(1, 0), &[ShEntry::new(Fk(100))])
                .unwrap();
            assert!(ColdProgress::load(&dir).unwrap().is_some());
            let p = ColdProgress::load(&dir).unwrap().unwrap();
            assert_eq!(p.next_shard, 1);
            session.abandon_incomplete();

            // Resume: skip shard 0 keys, fill 1..3.
            let p = ColdProgress::load(&dir).unwrap().unwrap();
            t.prepare_cold_resume(&p).unwrap();
            let mut session = t.bulk_session_resume(64, &p).unwrap();
            // Re-deliver shard 0 keys (must be ignored).
            for i in 0..8u8 {
                session
                    .put_chain(key(0, i), &[ShEntry::new(Fk(u64::from(i) + 1))])
                    .unwrap();
            }
            for shard in 1u8..4 {
                for i in 0..4u8 {
                    session
                        .put_chain(
                            key(shard, i),
                            &[ShEntry::new(Fk(u64::from(shard) * 100 + u64::from(i)))],
                        )
                        .unwrap();
                }
            }
            let (creates, keys, _, _) = session.finish().unwrap();
            assert!(ColdProgress::load(&dir).unwrap().is_none());
            // Shard0 kept (8). Resume fills shards 1..3 × 4 keys (the mid-shard1 key was abandoned).
            assert_eq!(keys, 8 + 12);
            assert_eq!(creates, 8 + 12);
            assert_eq!(t.entries(&key(0, 0)).unwrap().len(), 1);
            assert_eq!(t.entries(&key(3, 3)).unwrap().len(), 1);
            let _ = std::fs::remove_dir_all(&dir);
        });
    }

    #[test]
    fn live_session_does_not_size_from_create_count() {
        // Regression: bulk_session(total_recs) used to allocate create-count-sized
        // OA images. unique_hint=1000 must not allocate a multi-GiB table.
        let dir = tmp();
        let t = ScriptHashTable::create(&dir).unwrap();
        let mut session = t.bulk_session(1_000).unwrap();
        let mut sh = [0u8; 32];
        sh[0] = 1;
        session.put_chain(sh, &[ShEntry::new(Fk(1))]).unwrap();
        let peak = session.peak_table_bytes;
        let _ = session.finish().unwrap();
        assert_eq!(peak, 24, "one streamed rec is 24 B, not an OA image");
        assert!(
            peak < 16 * 1024 * 1024,
            "peak {peak} looks like create-count sizing"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn open_migrates_legacy_head_when_runs_present() {
        // Leftover live OA main is refused even when runs exist (wipe + rematerialize).
        HeadScale::test_with(HeadScale::Mainnet, || {
            let dir = tmp();
            let body =
                TableFile::create(dir.join("scripthash.body"), TableKind::ScriptHash).unwrap();
            let payload0 = payload_start(FILE_HEADER_LEN);
            body.ensure_capacity(payload0).unwrap();
            body.set_logical_len(payload0).unwrap();
            let state = AllocState {
                live_count: 0,
                bump: payload0,
                free_head: [0; SH_MAX_CLASS as usize + 1],
            };
            write_alloc_header(&body, &state).unwrap();
            drop(body);
            ShardedScriptHashHead::create_sharded(dir.join("scripthash.head"), 16, 64).unwrap();

            let runs_dir = dir.join("scripthash.runs");
            std::fs::create_dir_all(&runs_dir).unwrap();
            let mut rec = [0u8; 40];
            rec[0] = 0xab;
            rec[32..40].copy_from_slice(&1u64.to_le_bytes());
            let path = crate::sorted_run::next_run_path(&runs_dir, 1);
            crate::sorted_run::write_sorted_run(&path, 32, 40, &rec).unwrap();
            assert!(has_sh_run_rebuild_source(&dir));

            match ScriptHashTable::open(&dir) {
                Ok(_) => panic!("leftover OA must refuse"),
                Err(StoreError::Layout(m)) => {
                    assert!(m.contains("scripthash*"), "{m}");
                }
                Err(e) => panic!("expected Layout, got {e}"),
            }
            let _ = std::fs::remove_dir_all(&dir);
        });
    }

    #[test]
    fn open_refuses_legacy_head_without_runs() {
        HeadScale::test_with(HeadScale::Mainnet, || {
            let dir = tmp();
            let body =
                TableFile::create(dir.join("scripthash.body"), TableKind::ScriptHash).unwrap();
            let payload0 = payload_start(FILE_HEADER_LEN);
            body.ensure_capacity(payload0).unwrap();
            body.set_logical_len(payload0).unwrap();
            let state = AllocState {
                live_count: 0,
                bump: payload0,
                free_head: [0; SH_MAX_CLASS as usize + 1],
            };
            write_alloc_header(&body, &state).unwrap();
            drop(body);
            ShardedScriptHashHead::create_sharded(dir.join("scripthash.head"), 16, 64).unwrap();
            assert!(!has_sh_run_rebuild_source(&dir));
            match ScriptHashTable::open(&dir) {
                Err(StoreError::Layout(m)) => {
                    assert!(m.contains("scripthash*"), "{m}");
                }
                Ok(_) => panic!("expected leftover OA refuse"),
                Err(e) => panic!("unexpected error: {e}"),
            }
            let _ = std::fs::remove_dir_all(&dir);
        });
    }

    #[test]
    fn for_each_live_create_skips_unlinked() {
        let dir = tmp();
        let t = ScriptHashTable::create(&dir).unwrap();
        let sh = script_hash(&[0x51]);
        let mut heads = HashMap::new();
        t.put_create_batch_append(&[rec(sh, 1, 0), rec(sh, 2, 0), rec(sh, 3, 0)], &mut heads)
            .unwrap();
        t.unlink_create(&sh, Fk(2), 0).unwrap();
        let mut seen = Vec::new();
        t.for_each_live_create(|c| seen.push(c.0)).unwrap();
        seen.sort_unstable();
        assert_eq!(seen, vec![1, 3]);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
