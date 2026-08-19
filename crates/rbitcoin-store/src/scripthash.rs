//! Class B scripthash multimap (Electrum: SHA256(scriptPubKey)).
//!
//! Hybrid layout (schema 15): head key = 16 B hash prefix; value = two u64s
//! (≤2 inline, geometric **slab**, or megakey first/last **4 KiB page** offs).
//! Body slabs pack ULEB128 fk deltas; vouts expanded from Class A at query.

use crate::error::StoreError;
use crate::file::{TableFile, FILE_HEADER_LEN};
use crate::hashhead::HeadRole;
use crate::hashhead::HeadScale;
use crate::scripthash_head::{
    sh_per_shard_key_budget, sh_unique_hint_default, ScriptHashHead, ShardedScriptHashHead,
};
use crate::scripthash_layout::{
    head_key_from_full, payload_start, slab_bytes, ShEntry, ShHeadValue, SH_ALLOC_HEADER_LEN,
    SH_ALLOC_MAGIC, SH_ALLOC_VERSION, SH_INLINE_CAP, SH_MAX_CLASS, SH_MAX_SLAB_CLASS,
    SH_PAGE_SLAB_CLASS,
};
use crate::scripthash_overflow::wipe_legacy_fullsize_overflow;
use crate::scripthash_pages::{
    sh_page_as_array, sh_page_as_array_mut, sh_page_chunk_ranges, sh_page_decode_slice,
    sh_page_init_empty, sh_page_last_fk, sh_page_next, sh_page_pack, sh_page_set_next,
    sh_page_try_append, sh_page_would_append, SH_PAGE_SIZE,
};
use crate::scripthash_slabs::{
    decode_slab_payload, encode_slab_payload, slab_class_for_n_fks,
    slab_class_for_n_fks_with_slack, SH_MEGAKEY_MIN_FKS,
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
use std::sync::Mutex;

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
        let tmp = store_dir.join(format!("{COLD_PROGRESS_NAME}.tmp"));
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
struct AllocState {
    live_count: u64,
    bump: u64,
    free_head: [u64; SH_MAX_CLASS as usize + 1],
}

pub struct ScriptHashTable {
    store_dir: PathBuf,
    body: TableFile,
    head: ShardedScriptHashHead,
    /// Sealed sorted main shards (set when a cold bulk shard is installed).
    sorted_main: Mutex<Vec<Option<SortedHead>>>,
    /// Global ingest OA for keys first seen after main seal (`scripthash.ovf/ingest`).
    ingest: Mutex<ScriptHashHead>,
    /// Sealed global ovf (sorted+fuse+idx), newest last.
    sealed_ovf: Mutex<Vec<SortedHead>>,
    /// At least one sealed sorted main shard is installed.
    sorted_main_on: std::sync::atomic::AtomicBool,
    alloc: Mutex<AllocState>,
}

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
        HeadScale::Mainnet => 1 << 22,
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
) -> Result<Vec<Option<SortedHead>>, StoreError> {
    let n = n_shards.max(1);
    let mut out = Vec::with_capacity(n);
    for i in 0..n {
        let p = sorted_main_shard_path(dir, i, n);
        if file_starts_with_shsr(&p) {
            out.push(Some(SortedHead::open(&p, SortedHeadFilter::None)?));
        } else {
            out.push(None);
        }
    }
    Ok(out)
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
    (0..n).any(|i| file_starts_with_shsr(&sorted_main_shard_path(dir, i, n)))
}

fn leftover_oa_wipe_msg() -> String {
    "scripthash leftover live OA index; wipe store/scripthash* (head, body, ovf, \
     runs, include_hwm, cold_progress, main_sealed, oa_stub) and rematerialize \
     (--shindex rebuilds on start)"
        .into()
}

/// Live OA at `scripthash.head` (file or shard dir) that is not sealed `SHSR`.
fn leftover_live_oa_main(head_path: &Path) -> bool {
    if !head_path.exists() {
        return false;
    }
    if head_path.is_file() {
        return !file_starts_with_shsr(head_path);
    }
    head_path.is_dir()
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

fn open_or_create_oa_stub(
    stub: &Path,
    n_shards: usize,
) -> Result<ShardedScriptHashHead, StoreError> {
    let n = n_shards.max(1);
    if stub.exists() {
        return ShardedScriptHashHead::open_for_role(stub, HeadRole::ScriptHash);
    }
    ShardedScriptHashHead::create_sharded(stub, n, 64)
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
        let mut first: Option<TableFile> = None;
        let mut state = AllocState {
            live_count: 0,
            bump: payload_start(FILE_HEADER_LEN),
            free_head: [0; SH_MAX_CLASS as usize + 1],
        };
        for i in 0..n_shards {
            let f = TableFile::create(sh_shard_body_path(dir, i), TableKind::ScriptHash)?;
            let st = init_empty_body(&f)?;
            if i == 0 {
                state = st;
                first = Some(f);
            }
        }
        let ovf_dir = dir.join("scripthash.ovf");
        std::fs::create_dir_all(&ovf_dir).map_err(|e| StoreError::io(&ovf_dir, e))?;
        let ovf = TableFile::create(sh_ovf_body_path(dir), TableKind::ScriptHash)?;
        let _ = init_empty_body(&ovf)?;
        let body = first.ok_or(StoreError::Corrupt("scripthash create: no shard body"))?;
        let head = open_or_create_oa_stub(&dir.join("scripthash.head.oa_stub"), n_shards)?;
        Ok(Self {
            store_dir: dir.to_path_buf(),
            body,
            head,
            sorted_main: Mutex::new((0..n_shards).map(|_| None).collect()),
            ingest: Mutex::new(open_or_create_ingest(dir)?),
            sealed_ovf: Mutex::new(Vec::new()),
            sorted_main_on: std::sync::atomic::AtomicBool::new(false),
            alloc: Mutex::new(state),
        })
    }

    pub fn open(dir: &std::path::Path) -> Result<Self, StoreError> {
        let layout = detect_sh_body_layout(dir)?;
        let body = match layout {
            ShBodyLayout::Shared => TableFile::open(sh_body_path(dir), TableKind::ScriptHash)?,
            ShBodyLayout::Sharded => {
                TableFile::open(sh_shard_body_path(dir, 0), TableKind::ScriptHash)?
            }
        };
        if leftover_oa_overflow(dir) {
            return Err(StoreError::Layout(leftover_oa_wipe_msg()));
        }
        let head_path = dir.join("scripthash.head");
        let expected = shard_count_for_role(HeadRole::ScriptHash);
        if sorted_main_present(dir, expected) {
            let stub = dir.join("scripthash.head.oa_stub");
            let head = open_or_create_oa_stub(&stub, expected)?;
            return Self::from_body_and_head(dir, body, head);
        }
        if leftover_live_oa_main(&head_path) {
            return Err(StoreError::Layout(leftover_oa_wipe_msg()));
        }
        // Ingest-only (no sorted main, no leftover OA at scripthash.head).
        let stub = dir.join("scripthash.head.oa_stub");
        let head = open_or_create_oa_stub(&stub, expected)?;
        Self::from_body_and_head(dir, body, head)
    }

    fn from_body_and_head(
        dir: &Path,
        body: TableFile,
        head: ShardedScriptHashHead,
    ) -> Result<Self, StoreError> {
        let (state, alloc_ver) = read_alloc_header(&body)?;
        wipe_legacy_fullsize_overflow(dir)?;
        let n_shards = head.shard_count();
        let sorted_main = open_sorted_main_shards(dir, n_shards)?;
        let sealed_ovf = open_sealed_sorted_ovf(dir)?;
        let sorted_on = sorted_main.iter().any(|s| s.is_some());
        let table = Self {
            store_dir: dir.to_path_buf(),
            body,
            head,
            sorted_main: Mutex::new(sorted_main),
            ingest: Mutex::new(open_or_create_ingest(dir)?),
            sealed_ovf: Mutex::new(sealed_ovf),
            sorted_main_on: std::sync::atomic::AtomicBool::new(sorted_on),
            alloc: Mutex::new(state),
        };
        // v1 = schema-13 slabs; v2 = schema-14 pages; v3 = schema-15 slabs.
        // Field layout is the same; only an empty older header upgrades silently.
        if alloc_ver != SH_ALLOC_VERSION {
            if table.has_durable_index() {
                return Err(StoreError::Corrupt(
                    "scripthash alloc is a pre-schema-15 body; wipe store/scripthash* (head, body, ovf, runs, include_hwm, cold_progress) and rematerialize",
                ));
            }
            // Empty SH: stamp current alloc version and combined-prefix bump.
            let mut g = table.alloc.lock().unwrap();
            g.bump = g.bump.max(payload_start(FILE_HEADER_LEN));
            write_alloc_header(&table.body, &g)?;
        }
        Ok(table)
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
        self.alloc.lock().unwrap().live_count
    }

    /// True when sorted main, ingest, and sealed ovf report no occupied keys.
    pub fn head_is_empty(&self) -> bool {
        if self
            .sorted_main
            .lock()
            .unwrap()
            .iter()
            .any(|s| s.as_ref().map(|h| !h.is_empty()).unwrap_or(false))
        {
            return false;
        }
        if !self.sealed_ovf.lock().unwrap().is_empty() {
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
        let payload0 = payload_start(FILE_HEADER_LEN);
        {
            let mut alloc = self.alloc.lock().unwrap();
            *alloc = AllocState {
                live_count: 0,
                bump: payload0,
                free_head: [0; SH_MAX_CLASS as usize + 1],
            };
            write_alloc_header(&self.body, &alloc)?;
        }
        // Discard old slabs from the published HWM (new cold load bumps from payload0).
        self.body.set_logical_len(payload0)?;
        self.head.reinit_empty()?;
        *self.sorted_main.lock().unwrap() = (0..self.head.shard_count()).map(|_| None).collect();
        self.sorted_main_on
            .store(false, std::sync::atomic::Ordering::Release);
        debug_assert!(self.head.is_empty());
        Ok(())
    }

    /// Prepare resume after SIGINT: keep complete shards, zero `progress.next_shard..`,
    /// restore body HWM to the last complete-shard checkpoint.
    pub fn prepare_cold_resume(&self, progress: &ColdProgress) -> Result<(), StoreError> {
        let n = self.head.shard_count();
        let start = progress.next_shard as usize;
        if start > n {
            return Err(StoreError::Corrupt(
                "scripthash cold progress next_shard out of range",
            ));
        }
        self.head.reinit_shards_from(start)?;
        let payload0 = payload_start(FILE_HEADER_LEN);
        let bump = progress.body_bump.max(payload0);
        {
            let mut alloc = self.alloc.lock().unwrap();
            *alloc = AllocState {
                live_count: progress.live_count,
                bump,
                free_head: [0; SH_MAX_CLASS as usize + 1],
            };
            write_alloc_header(&self.body, &alloc)?;
        }
        self.body.set_logical_len(bump)?;
        Ok(())
    }

    /// Store directory containing `scripthash.body` / head (parent of body path).
    pub fn store_dir(&self) -> &Path {
        self.body.path().parent().unwrap_or_else(|| Path::new("."))
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
        if self
            .sorted_main
            .lock()
            .unwrap()
            .iter()
            .any(|s| s.as_ref().map(|h| !h.is_empty()).unwrap_or(false))
        {
            return true;
        }
        if self.ingest.lock().unwrap().occupied() > 0 {
            return true;
        }
        if !self.sealed_ovf.lock().unwrap().is_empty() {
            return true;
        }
        false
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
            return Ok(Some((v, KeyHome::Ingest)));
        }
        let hk = head_key_from_full(scripthash);
        for h in self.sealed_ovf.lock().unwrap().iter().rev() {
            if let Some(v) = h.get(&hk)? {
                return Ok(Some((v, KeyHome::SealedOvf)));
            }
        }
        if self.has_sorted_main() {
            let g = self.sorted_main.lock().unwrap();
            let si = self.head.shard_index(scripthash);
            if let Some(Some(h)) = g.get(si) {
                if let Some(v) = h.get(&hk)? {
                    return Ok(Some((v, KeyHome::Main)));
                }
            }
        }
        Ok(None)
    }

    fn has_sorted_main(&self) -> bool {
        self.sorted_main_on
            .load(std::sync::atomic::Ordering::Acquire)
    }

    #[cfg(test)]
    fn sorted_main_pread_count(&self, shard: usize) -> u64 {
        self.sorted_main
            .lock()
            .unwrap()
            .get(shard)
            .and_then(|s| s.as_ref())
            .map(|h| h.pread_count())
            .unwrap_or(0)
    }

    #[cfg(test)]
    fn reset_sorted_main_preads(&self) {
        for h in self.sorted_main.lock().unwrap().iter().flatten() {
            h.reset_pread_count();
        }
    }

    /// Test-only: set alloc `live_count = 0` without clearing head slots.
    ///
    /// Models crash mid-finish after deferred heads landed but before the
    /// alloc header was updated (entry_count==0, head non-empty). Process-local
    /// fault inject — no production caller; kept for reinit recovery regression.
    #[cfg(test)]
    pub fn test_zero_live_count_keep_head(&self) -> Result<(), StoreError> {
        let mut alloc = self.alloc.lock().unwrap();
        alloc.live_count = 0;
        write_alloc_header(&self.body, &alloc)?;
        Ok(())
    }

    /// Visit every live create_tx_fk across all keys (main + overflow occupancy walk).
    pub fn for_each_live_create(&self, mut f: impl FnMut(Fk)) -> Result<(), StoreError> {
        {
            let g = self.sorted_main.lock().unwrap();
            for h in g.iter().flatten() {
                h.for_each_occupied(|_k, val| {
                    let entries = self.collect_entries(&[0u8; 32], &val)?;
                    for e in entries {
                        f(e.create_tx_fk);
                    }
                    Ok(())
                })?;
            }
        }
        {
            let g = self.sealed_ovf.lock().unwrap();
            for h in g.iter() {
                h.for_each_occupied(|_k, val| {
                    let entries = self.collect_entries(&[0u8; 32], &val)?;
                    for e in entries {
                        f(e.create_tx_fk);
                    }
                    Ok(())
                })?;
            }
        }
        self.ingest.lock().unwrap().for_each_occupied(|_key, val| {
            let entries = self.collect_entries(&_key, &val)?;
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
        let Some(val) = self.head_value(scripthash)? else {
            return Ok(Vec::new());
        };
        let list = self.collect_entries(scripthash, &val)?;
        Ok(list
            .into_iter()
            .map(|e| {
                // Synthetic fk = create_tx_fk for API compat (no body row id).
                (e.create_tx_fk, ScriptHashRecord::from_entry(*scripthash, e))
            })
            .collect())
    }

    fn collect_entries(
        &self,
        _scripthash: &[u8; 32],
        val: &ShHeadValue,
    ) -> Result<Vec<ShEntry>, StoreError> {
        match val {
            ShHeadValue::Empty => Ok(Vec::new()),
            ShHeadValue::Inline { .. } => Ok(val.inline_entries().to_vec()),
            ShHeadValue::Slab { class, off, used } => {
                let got = self.read_slab(*class, *off)?;
                if got.len() != *used as usize {
                    return Err(StoreError::Corrupt(
                        "invariant: scripthash slab used != decoded fk count",
                    ));
                }
                Ok(got)
            }
            ShHeadValue::Paged { first_page, .. } => self.collect_page_chain(*first_page),
        }
    }

    fn collect_page_chain(&self, first_page: u64) -> Result<Vec<ShEntry>, StoreError> {
        let mut out = Vec::new();
        let mut off = first_page;
        let mut prev_last: Option<u64> = None;
        while off != 0 {
            let mut page = [0u8; SH_PAGE_SIZE];
            self.body.read_at(off, &mut page)?;
            let (next, ents) = sh_page_decode_slice(&page)?;
            if let (Some(pl), Some(first)) = (prev_last, ents.first()) {
                if first.create_tx_fk.0 <= pl {
                    return Err(StoreError::Corrupt(
                        "invariant: scripthash page chain create_fks not strictly increasing",
                    ));
                }
            }
            if let Some(last) = ents.last() {
                prev_last = Some(last.create_tx_fk.0);
            }
            out.extend(ents);
            off = next;
        }
        Ok(out)
    }

    /// Max durable create_tx_fk for a head value (**last page only** when paged).
    ///
    /// Sorted-chain invariant: max is the last entry of the last page (or max
    /// inline FK). Never walks earlier pages.
    pub fn last_create_fk_for_value(&self, val: &ShHeadValue) -> Result<Option<Fk>, StoreError> {
        match val {
            ShHeadValue::Empty => Ok(None),
            ShHeadValue::Inline { .. } => {
                let ents = val.inline_entries();
                Ok(ents.iter().map(|e| e.create_tx_fk).max_by_key(|f| f.0))
            }
            ShHeadValue::Slab { class, off, .. } => {
                let ents = self.read_slab(*class, *off)?;
                Ok(ents.last().map(|e| e.create_tx_fk))
            }
            ShHeadValue::Paged { last_page, .. } => {
                let mut page = [0u8; SH_PAGE_SIZE];
                self.body.read_at(*last_page, &mut page)?;
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
        match self.last_create_fk_for_value(&val)? {
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
        let mut alloc = self.alloc.lock().unwrap();

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
            let max = self.last_create_fk_for_value(&cur)?;
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
            let new_val = self.append_sorted_creates(&mut alloc, &cur, &add)?;
            heads.insert(key, new_val.clone());
            let kh = home.get(&key).copied().unwrap_or(KeyHome::Absent);
            head_final.push((key, new_val, kh));
        }

        write_alloc_header(&self.body, &alloc)?;
        drop(alloc);
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
                    let si = self.head.shard_index(key);
                    let g = self.sorted_main.lock().unwrap();
                    if let Some(Some(h)) = g.get(si) {
                        if h.update_value(&hk, val)? {
                            continue;
                        }
                    }
                    ingest_ups.push((*key, val.clone()));
                }
            }
        }

        if !sealed_ovf_ups.is_empty() {
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
                    ingest_ups.push((*key, val.clone()));
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
        if load < ShardedScriptHashHead::SH_SEAL_LOAD {
            return Ok(());
        }
        self.seal_ingest()
    }

    fn seal_ingest(&self) -> Result<(), StoreError> {
        let mut recs = {
            let g = self.ingest.lock().unwrap();
            let mut recs = Vec::new();
            g.for_each_occupied(|full, val| {
                recs.push((head_key_from_full(&full), val.encode()));
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
        let n = self.sealed_ovf.lock().unwrap().len();
        if n >= Self::SEALED_OVF_COMPACT_FILES {
            self.compact_sealed_ovf()?;
        }
        Ok(())
    }

    /// K-way merge of sealed global ovf heads. Body offs unchanged. Readers
    /// keep the old `Vec` until this lock is released after rename.
    pub fn compact_sealed_ovf(&self) -> Result<(), StoreError> {
        let mut recs: Vec<(crate::scripthash_layout::ShHeadKey, [u8; 16])> = Vec::new();
        let old_paths: Vec<PathBuf> = {
            let g = self.sealed_ovf.lock().unwrap();
            if g.len() < 2 {
                return Ok(());
            }
            for h in g.iter() {
                h.for_each_occupied(|k, v| {
                    recs.push((k, v.encode()));
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
        let merged = SortedHead::write(&path, &recs, SortedHeadFilter::Fuse8)?;
        let old = {
            let mut g = self.sealed_ovf.lock().unwrap();
            std::mem::replace(&mut *g, vec![merged])
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

    fn collect_entries_locked(&self, val: &ShHeadValue) -> Result<Vec<ShEntry>, StoreError> {
        self.collect_entries(&[0u8; 32], val)
    }

    /// Rewrite full sorted chain for a key (unlink / rare full rebuild).
    /// Prefer [`Self::append_sorted_creates`] for tip append.
    fn rewrite_entries_for_key(
        &self,
        alloc: &mut AllocState,
        old: &ShHeadValue,
        live: &[ShEntry],
    ) -> Result<ShHeadValue, StoreError> {
        let n = live.len() as u32;
        let old_list = self.collect_entries_locked(old)?;
        let old_n = old_list.len() as u32;
        if n > old_n {
            alloc.live_count = alloc.live_count.saturating_add(u64::from(n - old_n));
        } else if n < old_n {
            alloc.live_count = alloc.live_count.saturating_sub(u64::from(old_n - n));
        }

        if n == 0 {
            self.free_if_paged(alloc, old)?;
            return Ok(ShHeadValue::Empty);
        }
        for w in live.windows(2) {
            if w[1].create_tx_fk.0 <= w[0].create_tx_fk.0 {
                return Err(StoreError::Corrupt(
                    "invariant: scripthash rewrite create_fks not strictly increasing",
                ));
            }
        }
        self.free_if_paged(alloc, old)?;
        self.pack_entries(alloc, live, false)
    }

    /// Append strictly increasing `new_ents` (all already `> durable max`) without
    /// walking the full page chain. Body I/O: fill last page + optional new pages.
    fn append_sorted_creates(
        &self,
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
            ShHeadValue::Empty => self.pack_entries(alloc, new_ents, true),
            ShHeadValue::Inline { .. } => {
                let mut live = old.inline_entries().to_vec();
                live.extend_from_slice(new_ents);
                self.pack_entries(alloc, &live, true)
            }
            ShHeadValue::Slab { class, off, .. } => {
                let mut live = self.read_slab(*class, *off)?;
                if let (Some(last), Some(first_new)) = (live.last(), new_ents.first()) {
                    if first_new.create_tx_fk.0 <= last.create_tx_fk.0 {
                        return Err(StoreError::Corrupt(
                            "invariant: scripthash slab append create_fk not strictly increasing",
                        ));
                    }
                }
                live.extend_from_slice(new_ents);
                let new_val = self.pack_entries_reuse(alloc, &live, true, Some((*class, *off)))?;
                if !matches!(
                    &new_val,
                    ShHeadValue::Slab {
                        class: nc,
                        off: no,
                        ..
                    } if *nc == *class && *no == *off
                ) {
                    self.free_slab(alloc, *class, *off)?;
                }
                Ok(new_val)
            }
            ShHeadValue::Paged {
                first_page,
                last_page,
            } => {
                let last = self.append_fks_to_pages(alloc, *first_page, *last_page, new_ents)?;
                Ok(ShHeadValue::paged(*first_page, last))
            }
        }
    }

    /// Pack `live` into inline / slab / megakey pages. `slack` picks a class
    /// with a spare slot on tip grow; cold pack uses exact class.
    fn pack_entries(
        &self,
        alloc: &mut AllocState,
        live: &[ShEntry],
        slack: bool,
    ) -> Result<ShHeadValue, StoreError> {
        self.pack_entries_reuse(alloc, live, slack, None)
    }

    fn pack_entries_reuse(
        &self,
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
            return Ok(if n == 1 {
                ShHeadValue::inline_one(live[0])
            } else {
                ShHeadValue::inline_two(live[0], live[1])
            });
        }
        if n >= SH_MEGAKEY_MIN_FKS {
            let (first, last) = self.write_new_page_chain(alloc, live)?;
            return Ok(ShHeadValue::paged(first, last));
        }
        let fks: Vec<Fk> = live.iter().map(|e| e.create_tx_fk).collect();
        let payload = encode_slab_payload(&fks)?;
        let class = match Self::slab_class_fitting(n, payload.len(), slack) {
            Some(c) => c,
            None => {
                let (first, last) = self.write_new_page_chain(alloc, live)?;
                return Ok(ShHeadValue::paged(first, last));
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
                self.alloc_slab(alloc, class)?
            }
        } else {
            self.alloc_slab(alloc, class)?
        };
        let mut buf = vec![0u8; cap];
        buf[..payload.len()].copy_from_slice(&payload);
        self.body.write_at(off, &buf)?;
        Ok(ShHeadValue::slab(class, n as u16, off))
    }

    fn slab_class_fitting(n: u32, packed_len: usize, slack: bool) -> Option<u8> {
        let start = if slack {
            slab_class_for_n_fks_with_slack(n)
        } else {
            slab_class_for_n_fks(n)
        }?;
        (start..=SH_MAX_SLAB_CLASS).find(|&c| slab_bytes(c) as usize >= packed_len)
    }

    fn read_slab(&self, class: u8, off: u64) -> Result<Vec<ShEntry>, StoreError> {
        if class > SH_MAX_SLAB_CLASS {
            return Err(StoreError::Corrupt("scripthash slab class overflow"));
        }
        let mut buf = vec![0u8; slab_bytes(class) as usize];
        self.body.read_at(off, &mut buf)?;
        let fks = decode_slab_payload(&buf)?;
        Ok(fks.into_iter().map(ShEntry::new).collect())
    }

    fn write_new_page_chain(
        &self,
        alloc: &mut AllocState,
        live: &[ShEntry],
    ) -> Result<(u64, u64), StoreError> {
        if live.is_empty() {
            return Err(StoreError::Corrupt("scripthash empty page chain"));
        }
        // Pre-allocate all pages, then pack each with next known — one write per page
        // (no RMW of the previous page to fix up next). Offsets may be non-contiguous
        // when freelist reuses slabs.
        let fks: Vec<Fk> = live.iter().map(|e| e.create_tx_fk).collect();
        let chunks = sh_page_chunk_ranges(&fks)?;
        let n_pages = chunks.len();
        let mut offs = Vec::with_capacity(n_pages);
        for _ in 0..n_pages {
            offs.push(self.alloc_page(alloc)?);
        }
        let mut page = [0u8; SH_PAGE_SIZE];
        for (pi, &off) in offs.iter().enumerate() {
            let (start, end) = chunks[pi];
            let next = offs.get(pi + 1).copied().unwrap_or(0);
            sh_page_pack(&mut page, &live[start..end], next)?;
            self.body.write_at(off, &page)?;
        }
        Ok((offs[0], *offs.last().expect("n_pages >= 1")))
    }

    /// Append `tail` FKs onto an existing chain ending at `last_page`.
    fn append_fks_to_pages(
        &self,
        alloc: &mut AllocState,
        first_page: u64,
        last_page: u64,
        tail: &[ShEntry],
    ) -> Result<u64, StoreError> {
        let _ = first_page;
        if tail.is_empty() {
            return Ok(last_page);
        }
        let mut last = last_page;
        let mut page = [0u8; SH_PAGE_SIZE];
        self.body.read_at(last, &mut page)?;
        for e in tail {
            if !sh_page_try_append(&mut page, e.create_tx_fk)? {
                let new_off = self.alloc_page(alloc)?;
                sh_page_set_next(&mut page, new_off)?;
                self.body.write_at(last, &page)?;
                sh_page_init_empty(&mut page);
                assert!(sh_page_try_append(&mut page, e.create_tx_fk)?);
                last = new_off;
            }
        }
        self.body.write_at(last, &page)?;
        Ok(last)
    }

    fn alloc_page(&self, alloc: &mut AllocState) -> Result<u64, StoreError> {
        self.alloc_slab(alloc, SH_PAGE_SLAB_CLASS)
    }

    fn alloc_slab(&self, alloc: &mut AllocState, class: u8) -> Result<u64, StoreError> {
        if class > SH_MAX_CLASS {
            return Err(StoreError::Corrupt("scripthash page class overflow"));
        }
        let idx = class as usize;
        if alloc.free_head[idx] != 0 {
            let off = alloc.free_head[idx];
            let mut next = [0u8; 8];
            self.body.read_at(off, &mut next)?;
            alloc.free_head[idx] = u64::from_le_bytes(next);
            return Ok(off);
        }
        let need = slab_bytes(class);
        let mut off = alloc.bump;
        // Megakey pages 4 KiB-align. Small slabs pack from bump with no pad.
        if class >= SH_PAGE_SLAB_CLASS {
            let aligned = (off + 4095) & !4095;
            if aligned != off {
                alloc.bump = aligned;
                off = aligned;
            }
        }
        alloc.bump = alloc.bump.saturating_add(need);
        self.body.ensure_capacity(alloc.bump)?;
        if alloc.bump > self.body.logical_len() {
            self.body.set_logical_len(alloc.bump)?;
        }
        Ok(off)
    }

    fn free_if_paged(&self, alloc: &mut AllocState, old: &ShHeadValue) -> Result<(), StoreError> {
        match old {
            ShHeadValue::Paged { first_page, .. } => {
                let mut off = *first_page;
                while off != 0 {
                    let mut page = [0u8; SH_PAGE_SIZE];
                    self.body.read_at(off, &mut page)?;
                    let (next, _) = sh_page_decode_slice(&page)?;
                    self.free_slab(alloc, SH_PAGE_SLAB_CLASS, off)?;
                    off = next;
                }
            }
            ShHeadValue::Slab { class, off, .. } => {
                self.free_slab(alloc, *class, *off)?;
            }
            _ => {}
        }
        Ok(())
    }

    fn free_slab(&self, alloc: &mut AllocState, class: u8, off: u64) -> Result<(), StoreError> {
        let idx = class as usize;
        let next = alloc.free_head[idx].to_le_bytes();
        self.body.write_at(off, &next)?;
        alloc.free_head[idx] = off;
        Ok(())
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
        let mut live = self.collect_entries(scripthash, &val)?;
        let Some(pos) = live.iter().position(|e| e.create_tx_fk == create_tx_fk) else {
            return Ok(false);
        };
        live.remove(pos); // keep remaining order; sort if swap would break
        live.sort_by_key(|e| e.create_tx_fk.0);
        let mut alloc = self.alloc.lock().unwrap();
        let new_val = self.rewrite_entries_for_key(&mut alloc, &val, &live)?;
        write_alloc_header(&self.body, &alloc)?;
        drop(alloc);
        match home {
            KeyHome::Main | KeyHome::Absent => {
                let hk = head_key_from_full(scripthash);
                let si = self.head.shard_index(scripthash);
                let updated_sorted = {
                    let g = self.sorted_main.lock().unwrap();
                    match g.get(si).and_then(|s| s.as_ref()) {
                        Some(h) => h.update_value(&hk, &new_val)?,
                        None => false,
                    }
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
        {
            let alloc = self.alloc.lock().unwrap();
            write_alloc_header(&self.body, &alloc)?;
        }
        self.body.flush()?;
        self.head.flush()?;
        self.ingest.lock().unwrap().flush()?;
        for h in self.sealed_ovf.lock().unwrap().iter() {
            h.flush()?;
        }
        Ok(())
    }

    pub fn flush_async(&self) -> Result<(), StoreError> {
        {
            let alloc = self.alloc.lock().unwrap();
            write_alloc_header(&self.body, &alloc)?;
        }
        self.body.flush_async()?;
        self.head.flush_async()?;
        self.ingest.lock().unwrap().flush_async()?;
        for h in self.sealed_ovf.lock().unwrap().iter() {
            h.flush()?;
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
        let n_shards = self.head.shard_count().max(1);
        let hint = if unique_hint == 0 {
            sh_unique_hint_default()
        } else {
            unique_hint
        };
        let key_budget = sh_per_shard_key_budget(hint, n_shards);
        let (bump, live_count) = {
            let a = self.alloc.lock().unwrap();
            (a.bump, a.live_count)
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
            pack_body: None,
            pack_only: false,
            max_fk: 0,
        })
    }

    /// Pack one shard into `temp` (local bump from payload start). Publisher remaps.
    pub fn pack_shard_session(
        &self,
        temp: TableFile,
    ) -> Result<ScriptHashBulkSession<'_>, StoreError> {
        let payload0 = payload_start(FILE_HEADER_LEN);
        temp.ensure_capacity(payload0)?;
        if payload0 > temp.logical_len() {
            temp.set_logical_len(payload0)?;
        }
        let n_shards = self.head.shard_count().max(1);
        let hint = sh_unique_hint_default();
        let key_budget = sh_per_shard_key_budget(hint, n_shards);
        Ok(ScriptHashBulkSession {
            table: self,
            progress_dir: self.store_dir().to_path_buf(),
            bump: payload0,
            live_count: 0,
            committed_bump: payload0,
            committed_live_count: 0,
            committed_keys: 0,
            resume_from_shard: 0,
            active_shard: None,
            recs: Vec::new(),
            key_budget,
            body_buf: Vec::with_capacity(BULK_BODY_FLUSH.min(4 << 20)),
            body_write_off: payload0,
            finished: false,
            keys_written: 0,
            shards_flushed: 0,
            body_flush_ns: 0,
            head_fill_ns: 0,
            peak_table_bytes: 0,
            open_key: None,
            pack_body: Some(temp),
            pack_only: true,
            max_fk: 0,
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
        let n_shards = self.head.shard_count().max(1);
        let start = progress.next_shard as usize;
        if start >= n_shards {
            return Err(StoreError::Corrupt(
                "scripthash bulk_session_resume: already complete",
            ));
        }
        // Remaining shards must be empty for live install.
        for i in start..n_shards {
            if self.head.shard_occupied(i) != 0 {
                return Err(StoreError::Corrupt(
                    "scripthash bulk_session_resume: incomplete shard not empty",
                ));
            }
        }
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
            pack_body: None,
            pack_only: false,
            max_fk: 0,
        })
    }

    /// Number of SH head shards (1 on Tiny, 64 on mainnet).
    pub fn head_shard_count(&self) -> usize {
        self.head.shard_count()
    }

    /// Current body bump (complete-shard HWM).
    pub fn alloc_bump(&self) -> u64 {
        self.alloc.lock().unwrap().bump
    }

    /// Seal `recs` as sorted main shard `shard` and publish alloc HWM.
    pub fn publish_sorted_shard(
        &self,
        shard: usize,
        recs: &[(crate::scripthash_layout::ShHeadKey, [u8; 16])],
        live_count: u64,
        bump: u64,
    ) -> Result<(), StoreError> {
        let mut recs = recs.to_vec();
        recs.sort_unstable_by(|a, b| a.0.cmp(&b.0));
        recs.dedup_by(|a, b| a.0 == b.0);
        if !recs.is_empty() {
            let n_shards = self.head.shard_count();
            let path = sorted_main_shard_path(&self.store_dir, shard, n_shards);
            if let Some(parent) = path.parent() {
                let _ = std::fs::create_dir_all(parent);
            }
            let sealed = SortedHead::write(&path, &recs, SortedHeadFilter::None)?;
            let mut g = self.sorted_main.lock().unwrap();
            if g.len() < n_shards {
                g.resize_with(n_shards, || None);
            }
            g[shard] = Some(sealed);
            self.sorted_main_on
                .store(true, std::sync::atomic::Ordering::Release);
        }
        if bump > self.body.logical_len() {
            self.body.set_logical_len(bump)?;
        }
        let state = AllocState {
            live_count,
            bump,
            free_head: [0; SH_MAX_CLASS as usize + 1],
        };
        write_alloc_header(&self.body, &state)?;
        *self.alloc.lock().unwrap() = state;
        Ok(())
    }

    /// Append a locally packed shard at `global_bump` (4 KiB aligned) and seal its head.
    pub fn publish_packed_shard(
        &self,
        shard: usize,
        pack: ShShardPack,
        global_bump: u64,
    ) -> Result<u64, StoreError> {
        let local_start = pack.local_start;
        let local_end = pack.local_end;
        if local_end < local_start {
            return Err(StoreError::Corrupt("scripthash pack range inverted"));
        }
        let len = local_end - local_start;
        let dest = if len == 0 {
            global_bump
        } else {
            (global_bump + 4095) & !4095
        };
        if len > 0 {
            copy_sh_body_range(&pack.body, local_start, local_end, &self.body, dest)?;
        }
        let delta = dest.saturating_sub(local_start);
        let mut recs = Vec::with_capacity(pack.recs.len());
        for (k, raw) in pack.recs {
            let val = ShHeadValue::decode(&raw)?;
            let remapped = remap_sh_head_value(&val, delta);
            if let ShHeadValue::Paged { first_page, .. } = &remapped {
                remap_copied_page_chain(&self.body, *first_page, delta)?;
            }
            recs.push((k, remapped.encode()));
        }
        let new_bump = dest.saturating_add(len);
        let live = self
            .alloc
            .lock()
            .unwrap()
            .live_count
            .saturating_add(pack.creates);
        self.publish_sorted_shard(shard, &recs, live, new_bump)?;
        Ok(new_bump)
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
        let local_next = sh_page_next(sh_page_as_array(&page)?)?;
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
/// `SortedHead` on the boundary. Peak head RAM ≈ unique keys in one shard × 32 B.
pub struct ScriptHashBulkSession<'a> {
    table: &'a ScriptHashTable,
    /// Directory for [`ColdProgress`] file.
    progress_dir: PathBuf,
    bump: u64,
    live_count: u64,
    /// Last durable complete-shard body HWM (SIGINT rolls back incomplete slabs here).
    committed_bump: u64,
    committed_live_count: u64,
    committed_keys: u64,
    /// Skip installing keys for shards `< resume_from_shard` (stream may still deliver them).
    resume_from_shard: u32,
    active_shard: Option<usize>,
    /// Packed sorted recs for the active shard (not an OA image).
    recs: Vec<(crate::scripthash_layout::ShHeadKey, [u8; 16])>,
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
    /// Private temp body when packing a shard off the live file.
    pack_body: Option<TableFile>,
    /// When true, do not write SortedHead / ColdProgress (publisher does that).
    pack_only: bool,
    max_fk: u64,
}

/// One shard packed at a local bump, ready to remap onto the live body.
pub struct ShShardPack {
    pub recs: Vec<(crate::scripthash_layout::ShHeadKey, [u8; 16])>,
    pub body: TableFile,
    pub local_start: u64,
    pub local_end: u64,
    pub creates: u64,
    pub max_fk: u64,
    pub keys: u64,
}

/// One unfinished key in [`ScriptHashBulkSession`] (≤ one delta page of FKs).
struct BulkOpenKey {
    key: [u8; 32],
    buf: Vec<u64>,
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
        self.pack_body.as_ref().unwrap_or(&self.table.body)
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
        let body = self
            .pack_body
            .take()
            .ok_or(StoreError::Corrupt("scripthash pack missing temp body"))?;
        let local_start = payload_start(FILE_HEADER_LEN);
        let pack = ShShardPack {
            recs: std::mem::take(&mut self.recs),
            body,
            local_start,
            local_end: self.bump,
            creates: self.live_count,
            max_fk: self.max_fk,
            keys: self.keys_written,
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
                n_total: 0,
                first_page: None,
                last_fk: None,
            });
        }
        if let Some(open) = self.open_key.as_ref() {
            if !open.buf.is_empty() {
                let cur: Vec<Fk> = open.buf.iter().copied().map(Fk).collect();
                if !sh_page_would_append(&cur, fk)? {
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
        let val = if open.first_page.is_none() && n < SH_MEGAKEY_MIN_FKS {
            let ents: Vec<ShEntry> = open.buf.iter().map(|&fk| ShEntry::new(Fk(fk))).collect();
            if n <= SH_INLINE_CAP as u32 {
                if n == 1 {
                    ShHeadValue::inline_one(ents[0])
                } else {
                    ShHeadValue::inline_two(ents[0], ents[1])
                }
            } else {
                self.flush_body()?;
                let (val, new_bump) = Self::bulk_write_slab(self.body(), self.bump, &ents)?;
                self.bump = new_bump;
                self.body_write_off = new_bump;
                val
            }
        } else {
            let last = if open.buf.is_empty() {
                return Err(StoreError::Corrupt(
                    "scripthash bulk stream: paged key missing last page",
                ));
            } else {
                self.write_page(&open.buf, false)?
            };
            let first = open.first_page.unwrap_or(last);
            ShHeadValue::paged(first, last)
        };
        self.live_count = self.live_count.saturating_add(u64::from(n));
        self.keys_written = self.keys_written.saturating_add(1);
        self.recs
            .push((head_key_from_full(&open.key), val.encode()));
        self.peak_table_bytes = self
            .peak_table_bytes
            .max(self.recs.len().saturating_mul(32));
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
        let si = self.table.head.shard_index(&key);
        if (si as u32) < self.resume_from_shard {
            return Ok(false);
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
        let fks = self
            .open_key
            .as_ref()
            .map(|o| o.buf.clone())
            .unwrap_or_default();
        let off = self.write_page(&fks, true)?;
        if let Some(open) = self.open_key.as_mut() {
            if open.first_page.is_none() {
                open.first_page = Some(off);
            }
            open.buf.clear();
        }
        Ok(())
    }

    /// Write one page at the aligned bump. `has_next` sets `next` to the following page.
    fn write_page(&mut self, fks: &[u64], has_next: bool) -> Result<u64, StoreError> {
        self.flush_body()?;
        let base = (self.bump + 4095) & !4095;
        let next = if has_next {
            base.saturating_add(SH_PAGE_SIZE as u64)
        } else {
            0
        };
        let end = base.saturating_add(SH_PAGE_SIZE as u64);
        self.ensure_body_capacity(end)?;
        let ents: Vec<ShEntry> = fks.iter().copied().map(|fk| ShEntry::new(Fk(fk))).collect();
        let mut page = [0u8; SH_PAGE_SIZE];
        sh_page_pack(&mut page, &ents, next)?;
        self.body().write_at(base, &page)?;
        self.bump = end;
        self.body_write_off = end;
        Ok(base)
    }

    fn start_live_shard(&mut self, si: usize) -> Result<(), StoreError> {
        self.recs.clear();
        rbitcoin_log::info!(
            "store: scripthash live shard start id={si} key_budget={} (stream recs)",
            self.key_budget
        );
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

    /// Write one exact-class slab at `bump` (no 4 KiB pad). Returns (value, new_bump).
    fn bulk_write_slab(
        body: &TableFile,
        bump: u64,
        entries: &[ShEntry],
    ) -> Result<(ShHeadValue, u64), StoreError> {
        let n = entries.len() as u32;
        let fks: Vec<Fk> = entries.iter().map(|e| e.create_tx_fk).collect();
        let payload = encode_slab_payload(&fks)?;
        let Some(class) = ScriptHashTable::slab_class_fitting(n, payload.len(), false) else {
            return Self::bulk_write_page_chain(body, bump, entries)
                .map(|(first, last, end)| (ShHeadValue::paged(first, last), end));
        };
        let need = slab_bytes(class);
        let end = bump.saturating_add(need);
        body.ensure_capacity(end)?;
        if end > body.logical_len() {
            body.set_logical_len(end)?;
        }
        let mut buf = vec![0u8; need as usize];
        buf[..payload.len()].copy_from_slice(&payload);
        body.write_at(bump, &buf)?;
        Ok((ShHeadValue::slab(class, n as u16, bump), end))
    }

    /// Write a full page chain at `bump` (4 KiB aligned). Returns (first, last, new_bump).
    ///
    /// Contiguous layout: pack each page with `next` known, **one** `write_at` per page.
    /// (Previously: write full page with next=0, then RMW previous when allocating next.)
    fn bulk_write_page_chain(
        body: &TableFile,
        bump: u64,
        entries: &[ShEntry],
    ) -> Result<(u64, u64, u64), StoreError> {
        if entries.is_empty() {
            return Err(StoreError::Corrupt("scripthash bulk page chain empty"));
        }
        let base = (bump + 4095) & !4095;
        let fks: Vec<Fk> = entries.iter().map(|e| e.create_tx_fk).collect();
        let chunks = sh_page_chunk_ranges(&fks)?;
        let n_pages = chunks.len();
        let end = base.saturating_add((n_pages as u64).saturating_mul(SH_PAGE_SIZE as u64));
        body.ensure_capacity(end)?;
        if end > body.logical_len() {
            body.set_logical_len(end)?;
        }
        let mut page = [0u8; SH_PAGE_SIZE];
        for (pi, &(start, end_i)) in chunks.iter().enumerate() {
            let off = base + (pi as u64) * (SH_PAGE_SIZE as u64);
            let next = if pi + 1 < n_pages {
                off + SH_PAGE_SIZE as u64
            } else {
                0
            };
            sh_page_pack(&mut page, &entries[start..end_i], next)?;
            body.write_at(off, &page)?;
        }
        let first = base;
        let last = base + ((n_pages - 1) as u64) * (SH_PAGE_SIZE as u64);
        Ok((first, last, end))
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
                let n_shards = self.table.head.shard_count();
                let path = sorted_main_shard_path(&self.table.store_dir, si, n_shards);
                if let Some(parent) = path.parent() {
                    let _ = std::fs::create_dir_all(parent);
                }
                let sealed = SortedHead::write(&path, &recs, SortedHeadFilter::None)?;
                let mut g = self.table.sorted_main.lock().unwrap();
                if g.len() < n_shards {
                    g.resize_with(n_shards, || None);
                }
                g[si] = Some(sealed);
                self.table
                    .sorted_main_on
                    .store(true, std::sync::atomic::Ordering::Release);
            }
            // Publish alloc so complete shards survive kill before finish().
            let state = AllocState {
                live_count: self.live_count,
                bump: self.bump,
                free_head: [0; SH_MAX_CLASS as usize + 1],
            };
            if self.bump > self.table.body.logical_len() {
                self.table.body.set_logical_len(self.bump)?;
            }
            write_alloc_header(&self.table.body, &state)?;
            *self.table.alloc.lock().unwrap() = state;
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
                (keys as f64 * 32.0) / (1024.0 * 1024.0)
            );
            let _ = self.table.head.shard_advise_dont_need(si);
        }
        self.active_shard = None;
        Ok(())
    }

    /// Discard incomplete live shard (no install); roll body HWM to last checkpoint.
    ///
    /// Call on cooperative cancel so Drop does not install a partial shard.
    pub fn abandon_incomplete(mut self) {
        self.recs.clear();
        self.active_shard = None;
        self.body_buf.clear();
        self.open_key = None;
        if self.pack_only {
            self.finished = true;
            return;
        }
        self.bump = self.committed_bump;
        self.live_count = self.committed_live_count;
        self.keys_written = self.committed_keys;
        self.body_write_off = self.committed_bump;
        let state = AllocState {
            live_count: self.committed_live_count,
            bump: self.committed_bump,
            free_head: [0; SH_MAX_CLASS as usize + 1],
        };
        let _ = self.table.body.set_logical_len(self.committed_bump);
        let _ = write_alloc_header(&self.table.body, &state);
        if let Ok(mut g) = self.table.alloc.lock() {
            *g = state;
        }
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
        if self.bump > self.table.body.logical_len() {
            self.table.body.set_logical_len(self.bump)?;
        }
        let state = AllocState {
            live_count: self.live_count,
            bump: self.bump,
            free_head: [0; SH_MAX_CLASS as usize + 1],
        };
        write_alloc_header(&self.table.body, &state)?;
        *self.table.alloc.lock().unwrap() = state;
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
        self.active_shard = None;
        self.body_buf.clear();
        self.open_key = None;
        let state = AllocState {
            live_count: self.committed_live_count,
            bump: self.committed_bump,
            free_head: [0; SH_MAX_CLASS as usize + 1],
        };
        let _ = self.table.body.set_logical_len(self.committed_bump);
        let _ = write_alloc_header(&self.table.body, &state);
        if let Ok(mut g) = self.table.alloc.lock() {
            *g = state;
        }
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
            ShHeadValue::Paged {
                first_page,
                last_page,
            } => {
                assert!(first_page > 0);
                assert_eq!(first_page, last_page, "257 fks fit one megakey page");
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
            .last_create_fk_for_value(&t.head_value(&sh).unwrap().unwrap())
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
            ShHeadValue::Paged { .. } => {}
            other => panic!("expected paged megakey, got {other:?}"),
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
            ShHeadValue::Inline { used, .. } => assert_eq!(used, 2),
            other => panic!("expected inline demote, got {other:?}"),
        }
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
        let body_path = dir.join("scripthash.body");
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
        let body_path = dir.join("scripthash.body");
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
        assert!(shard_p.is_file());
        let mut idx = shard_p.as_os_str().to_os_string();
        idx.push(".idx");
        let mut fuse = shard_p.as_os_str().to_os_string();
        fuse.push(".fuse8");
        assert!(PathBuf::from(idx).is_file());
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
            t.sorted_main_pread_count(t.head.shard_index(&sh_new)),
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
            assert_eq!(t.sealed_ovf.lock().unwrap().len(), 1);
            assert_eq!(t.entries(&first_new).unwrap().len(), 1);
            assert_eq!(t.entries(&second_new).unwrap().len(), 1);
            assert_eq!(t.entries(&sh_main).unwrap().len(), 1);
            assert!(matches!(t.key_home(&sh_main).unwrap(), KeyHome::Main));
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
            ShHeadValue::Inline { used: 2, .. }
        ));
        match t.head_value(&sh(0x06)).unwrap().unwrap() {
            ShHeadValue::Slab { class, used, .. } => {
                assert_eq!(class, 1, "6 fks exact class 1");
                assert_eq!(used, 6);
            }
            other => panic!("expected class-1 slab, got {other:?}"),
        }
        match t.head_value(&sh(0x14)).unwrap().unwrap() {
            ShHeadValue::Slab { class, used, .. } => {
                assert_eq!(class, 3, "20 fks exact class 3");
                assert_eq!(used, 20);
            }
            other => panic!("expected class-3 slab, got {other:?}"),
        }
        assert!(matches!(
            t.head_value(&sh(0x60)).unwrap().unwrap(),
            ShHeadValue::Paged { .. }
        ));
        assert_eq!(t.entries(&sh(0x06)).unwrap().len(), 6);
        assert_eq!(t.entries(&sh(0x60)).unwrap().len(), 600);

        let payload = t.body.logical_len().saturating_sub(4096);
        // Tight: class1 64 + class3 256 + two 4 KiB pages.
        let tight = 64 + 256 + 2 * 4096;
        assert!(
            payload <= 2 * tight,
            "cold body {payload} must stay within 2× tight {tight} (not 4 KiB × paged keys)"
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
        use crate::scripthash_pages::{SH_PAGE_SIZE, SH_PAGE_STREAM_MAX};
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
            ShHeadValue::Paged {
                first_page,
                last_page,
            } => {
                assert_eq!(last_page, first_page + SH_PAGE_SIZE as u64);
            }
            other => panic!("expected paged, got {other:?}"),
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
        assert!(matches!(mega_val, ShHeadValue::Paged { .. }));
        let src_lo = payload_start(FILE_HEADER_LEN);
        let src_hi = src.alloc.lock().unwrap().bump;
        let delta = 3 * SH_PAGE_SIZE as u64;
        let dst = ScriptHashTable::create(&dst_dir).unwrap();
        copy_sh_body_range(&src.body, src_lo, src_hi, &dst.body, src_lo + delta).unwrap();
        let slab_r = remap_sh_head_value(&slab_val, delta);
        let mega_r = remap_sh_head_value(&mega_val, delta);
        if let ShHeadValue::Paged { first_page, .. } = mega_r {
            remap_copied_page_chain(&dst.body, first_page, delta).unwrap();
        }
        let recs = vec![
            (head_key_from_full(&slab_key), slab_r.encode()),
            (head_key_from_full(&mega_key), mega_r.encode()),
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
            ShardedScriptHashHead::create_sharded(dir.join("scripthash.head.oa_stub"), 4, 256)
                .unwrap();
            let t = ScriptHashTable::open(&dir).unwrap();
            assert_eq!(t.head_shard_count(), 4);
            let key = |shard: u8, i: u8| {
                let mut k = [0u8; 32];
                k[0] = shard << 6 | (i & 0x3f);
                k
            };
            let k0 = key(0, 0);
            let k1 = key(0, 1);
            let temp = TableFile::create(dir.join("pack0.body"), TableKind::ScriptHash).unwrap();
            let mut session = t.pack_shard_session(temp).unwrap();
            session.push_sorted_fk(k0, Fk(1)).unwrap();
            session.push_sorted_fk(k1, Fk(2)).unwrap();
            let pack = session.finish_pack().unwrap();
            assert_eq!(pack.recs.len(), 2);
            let bump0 = t.alloc.lock().unwrap().bump;
            let new_bump = t.publish_packed_shard(0, pack, bump0).unwrap();
            assert!(new_bump >= bump0);
            assert_eq!(t.entries(&k0).unwrap().len(), 1);
            assert_eq!(t.entries(&k1).unwrap().len(), 1);
            assert!(t.head_value(&key(1, 0)).unwrap().is_none());
            let _ = std::fs::remove_dir_all(&dir);
        });
    }

    fn four_shard_table(dir: &std::path::Path) -> ScriptHashTable {
        let body = TableFile::create(dir.join("scripthash.body"), TableKind::ScriptHash).unwrap();
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
        ShardedScriptHashHead::create_sharded(dir.join("scripthash.head.oa_stub"), 4, 256).unwrap();
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
            let serial = four_shard_table(&serial_dir);
            let s = crate::materialize_sh_shards(&serial, &inputs, 0, 1, None).unwrap();

            let par_dir = dir.join("par");
            std::fs::create_dir_all(&par_dir).unwrap();
            let par = four_shard_table(&par_dir);
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

            let t = four_shard_table(&dir);
            let k0 = key(0, 0);
            let temp = TableFile::create(dir.join("pack0.body"), TableKind::ScriptHash).unwrap();
            let mut session = t.pack_shard_session(temp).unwrap();
            session.push_sorted_fk(k0, Fk(1)).unwrap();
            let pack = session.finish_pack().unwrap();
            let bump0 = t.alloc_bump();
            let bump1 = t.publish_packed_shard(0, pack, bump0).unwrap();
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
            let t2 = four_shard_table(&resume_dir);
            let temp =
                TableFile::create(resume_dir.join("pack0.body"), TableKind::ScriptHash).unwrap();
            let mut session = t2.pack_shard_session(temp).unwrap();
            session.push_sorted_fk(k0, Fk(1)).unwrap();
            let pack = session.finish_pack().unwrap();
            let b = t2.publish_packed_shard(0, pack, t2.alloc_bump()).unwrap();
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

    /// Cold bulk megakey: multi-page chain is contiguous at bump (single-pass pack
    /// writes next links on first write — no previous-page RMW).
    #[test]
    fn bulk_session_megakey_page_chain_contiguous_once() {
        use crate::scripthash_pages::{SH_PAGE_SIZE, SH_PAGE_STREAM_MAX};
        let dir = tmp();
        let t = ScriptHashTable::create(&dir).unwrap();
        // Sequential FKs fill ~4080/page; this n spans two pages.
        let n = SH_PAGE_STREAM_MAX + 10;
        let mut sh = [0u8; 32];
        sh[0] = 0x10;
        sh[1] = 0xee;
        let ents: Vec<_> = (1..=n as u64).map(|i| ShEntry::new(Fk(i))).collect();
        let mut session = t.bulk_session(1).unwrap();
        session.put_chain(sh, &ents).unwrap();
        let (creates, keys, _, _) = session.finish().unwrap();
        assert_eq!(keys, 1);
        assert_eq!(creates, n as u64);
        let got = t.entries(&sh).unwrap();
        assert_eq!(got.len(), n);
        for (i, (_, e)) in got.iter().enumerate() {
            assert_eq!(e.create_tx_fk, Fk(i as u64 + 1));
        }
        let (first, last) = match t.head_value(&sh).unwrap().unwrap() {
            ShHeadValue::Paged {
                first_page,
                last_page,
            } => (first_page, last_page),
            other => panic!("expected paged, got {other:?}"),
        };
        // Contiguous bump layout: last = first + (n_pages-1)*4096.
        assert_eq!(
            last,
            first + SH_PAGE_SIZE as u64,
            "bulk chain pages must be contiguous at bump"
        );
        assert!(first > 0 && first % (SH_PAGE_SIZE as u64) == 0);
        // Tip-path multi-page (write_new_page_chain) also round-trips same size.
        let sh2 = script_hash(&[0xef]);
        let recs: Vec<_> = (1..=n as u32).map(|v| rec(sh2, u64::from(v), v)).collect();
        assert_eq!(t.put_create_batch(&recs).unwrap(), n);
        assert_eq!(t.entries(&sh2).unwrap().len(), n);
        match t.head_value(&sh2).unwrap().unwrap() {
            ShHeadValue::Paged {
                first_page,
                last_page,
            } => {
                assert_ne!(first_page, last_page);
            }
            other => panic!("expected paged, got {other:?}"),
        }
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
        assert_eq!(peak, (N as usize) * 32);
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
            ShardedScriptHashHead::create_sharded(dir.join("scripthash.head.oa_stub"), 4, 256)
                .unwrap();
            let t = ScriptHashTable::open(&dir).unwrap();
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
        assert_eq!(peak, 32, "one streamed rec is 32 B, not an OA image");
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
