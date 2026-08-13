//! Class B scripthash multimap (Electrum: SHA256(scriptPubKey)).
//!
//! Hybrid layout (schema 15): head key = 16 B hash prefix; value = two u64s
//! (≤2 inline, geometric **slab**, or megakey first/last **4 KiB page** offs).
//! Body slabs pack ULEB128 fk deltas; vouts expanded from Class A at query.

use crate::error::StoreError;
use crate::file::{TableFile, FILE_HEADER_LEN};
use crate::fuse8_filter::{fuse_key_from_mixed, SealedFuse8};
use crate::hashhead::HeadRole;
use crate::hashhead::HeadScale;
use crate::scripthash_head::{
    sh_per_shard_key_budget, sh_unique_hint_default, LiveShardTable, ScriptHashHead,
    ShardedScriptHashHead, SH_HEAD_SHARD_COUNT_MISMATCH,
};
use crate::scripthash_layout::{
    head_key_from_full, payload_start, slab_bytes, ShEntry, ShHeadValue, SH_ALLOC_HEADER_LEN,
    SH_ALLOC_MAGIC, SH_ALLOC_VERSION, SH_INLINE_CAP, SH_MAX_CLASS, SH_MAX_SLAB_CLASS,
    SH_PAGE_SLAB_CLASS,
};
#[cfg(test)]
use crate::scripthash_overflow::{ovf_dir, ovf_seg_path};
use crate::scripthash_overflow::{ovf_segment_slots, ShOverflowStack};
use crate::scripthash_pages::{
    sh_page_count_for_entries, sh_page_decode_slice, sh_page_init_empty, sh_page_last_fk,
    sh_page_pack, sh_page_set_next, sh_page_try_append, SH_PAGE_FK_CAP, SH_PAGE_SIZE,
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
use std::collections::{HashMap, HashSet};
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
    /// Mono overflow segment stack (`scripthash.ovf/NNNNNN`) after main seals.
    overflow: Mutex<ShOverflowStack>,
    /// Main head no longer accepts **new** keys (still updates existing).
    main_sealed: std::sync::atomic::AtomicBool,
    /// Optional main membership filter after seal (no FN for keys at build).
    main_fuse: Mutex<Option<SealedFuse8>>,
    alloc: Mutex<AllocState>,
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

const MAIN_SEALED_NAME: &str = "scripthash.main_sealed";
const MAIN_FUSE_NAME: &str = "scripthash.head.fuse8";

/// Fuse key from full Electrum scripthash (uses 16 B head prefix + zero pad).
#[inline]
fn sh_fuse_key(full: &[u8; 32]) -> u64 {
    let mut pad = [0u8; 32];
    pad[..16].copy_from_slice(&full[..16]);
    fuse_key_from_mixed(&pad)
}

/// Load main seal fuse if a valid BF8R file exists (placeholder bytes → None).
fn load_main_fuse(dir: &Path) -> Option<SealedFuse8> {
    let path = dir.join(MAIN_FUSE_NAME);
    if !path.is_file() {
        return None;
    }
    SealedFuse8::read_from(&path).ok()
}

/// Open main head from disk, fold **16 B OA head keys** into fuse u64s, write BF8R.
///
/// Not Class A / `tx.body` txids — SH head slots only store Electrum prefix keys.
fn build_main_fuse_from_disk(dir: &Path) -> Result<(), StoreError> {
    let head_path = dir.join("scripthash.head");
    let head = ShardedScriptHashHead::open_for_role(&head_path, HeadRole::ScriptHash)?;
    let mut set: HashSet<u64> = HashSet::new();
    head.for_each_occupied(|full, _val| {
        // `full` is head_key[16] zero-padded to 32 (see ScriptHashHead::for_each_occupied).
        set.insert(sh_fuse_key(&full));
        Ok(())
    })?;
    let mut keys: Vec<u64> = set.into_iter().collect();
    keys.sort_unstable();
    let fuse = SealedFuse8::build(&keys)?;
    fuse.write_to(&dir.join(MAIN_FUSE_NAME))?;
    Ok(())
}

/// Where a scripthash key lives for head upsert routing.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum KeyHome {
    /// Already present on main OA or sealed sorted main.
    Main,
    /// Present on overflow segment `id` (update stays on that segment).
    Overflow(u32),
    /// Present on the global ingest OA.
    Ingest,
    /// Not yet in either head.
    Absent,
}

impl ScriptHashTable {
    pub fn create(dir: &std::path::Path) -> Result<Self, StoreError> {
        let body = TableFile::create(dir.join("scripthash.body"), TableKind::ScriptHash)?;
        let payload0 = payload_start(FILE_HEADER_LEN);
        let need = payload0; // header + empty alloc page
        body.ensure_capacity(need)?;
        body.set_logical_len(need)?;
        let state = AllocState {
            live_count: 0,
            bump: payload0,
            free_head: [0; SH_MAX_CLASS as usize + 1],
        };
        write_alloc_header(&body, &state)?;
        let head = ShardedScriptHashHead::create_for_role(
            dir.join("scripthash.head"),
            HeadRole::ScriptHash,
        )?;
        let n_shards = head.shard_count();
        Ok(Self {
            store_dir: dir.to_path_buf(),
            body,
            head,
            sorted_main: Mutex::new((0..n_shards).map(|_| None).collect()),
            ingest: Mutex::new(open_or_create_ingest(dir)?),
            sealed_ovf: Mutex::new(Vec::new()),
            overflow: Mutex::new(ShOverflowStack::empty(dir)),
            main_sealed: std::sync::atomic::AtomicBool::new(false),
            main_fuse: Mutex::new(None),
            alloc: Mutex::new(state),
        })
    }

    pub fn open(dir: &std::path::Path) -> Result<Self, StoreError> {
        let body = TableFile::open(dir.join("scripthash.body"), TableKind::ScriptHash)?;
        let head_path = dir.join("scripthash.head");
        let expected = shard_count_for_role(HeadRole::ScriptHash);
        // Detect legacy multi-shard layouts before open (e.g. 16-way → 64-way).
        if expected > 1 {
            if let Some(on_disk) = count_sh_head_shards(&head_path)? {
                if on_disk != expected {
                    if has_sh_run_rebuild_source(dir) {
                        return migrate_legacy_sh_head_from_runs(dir, body);
                    }
                    return Err(StoreError::Corrupt(SH_HEAD_SHARD_COUNT_MISMATCH));
                }
            }
        }
        if file_starts_with_shsr(&head_path) {
            let stub = dir.join("scripthash.head.oa_stub");
            let head = if stub.exists() {
                ShardedScriptHashHead::open_for_role(&stub, HeadRole::ScriptHash)?
            } else {
                ShardedScriptHashHead::create_sharded(&stub, 1, 64)?
            };
            return Self::from_body_and_head(dir, body, head);
        }
        match ShardedScriptHashHead::open_for_role(&head_path, HeadRole::ScriptHash) {
            Ok(head) => Self::from_body_and_head(dir, body, head),
            Err(StoreError::Corrupt(msg)) if msg == SH_HEAD_SHARD_COUNT_MISMATCH => {
                if !has_sh_run_rebuild_source(dir) {
                    return Err(StoreError::Corrupt(SH_HEAD_SHARD_COUNT_MISMATCH));
                }
                migrate_legacy_sh_head_from_runs(dir, body)
            }
            Err(e) => Err(e),
        }
    }

    fn from_body_and_head(
        dir: &Path,
        body: TableFile,
        head: ShardedScriptHashHead,
    ) -> Result<Self, StoreError> {
        let (state, alloc_ver) = read_alloc_header(&body)?;
        let sealed = dir.join(MAIN_SEALED_NAME).is_file();
        // Opens segmented `scripthash.ovf/`; wipes legacy full-size ovf.head.
        let overflow = ShOverflowStack::open(dir)?;
        let fuse = load_main_fuse(dir);
        let n_shards = head.shard_count();
        let sorted_main = open_sorted_main_shards(dir, n_shards)?;
        let sealed_ovf = open_sealed_sorted_ovf(dir)?;
        let table = Self {
            store_dir: dir.to_path_buf(),
            body,
            head,
            sorted_main: Mutex::new(sorted_main),
            ingest: Mutex::new(open_or_create_ingest(dir)?),
            sealed_ovf: Mutex::new(sealed_ovf),
            overflow: Mutex::new(overflow),
            main_sealed: std::sync::atomic::AtomicBool::new(sealed),
            main_fuse: Mutex::new(fuse),
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

    fn main_accepts_new_key(&self) -> bool {
        if self.main_sealed.load(std::sync::atomic::Ordering::Acquire) {
            return false;
        }
        match self.head.load_ratio() {
            Some(r) if r >= ShardedScriptHashHead::SH_SEAL_LOAD => false,
            _ => true,
        }
    }

    fn ensure_overflow(&self) -> Result<(), StoreError> {
        let mut g = self.overflow.lock().unwrap();
        if g.segs().last().map(|s| s.is_open()).unwrap_or(false) {
            return Ok(());
        }
        let slots = ovf_segment_slots(&self.head);
        g.ensure_open(slots)
    }

    fn maybe_seal_main(&self) -> Result<(), StoreError> {
        if self.main_sealed.load(std::sync::atomic::Ordering::Acquire) {
            return Ok(());
        }
        let Some(r) = self.head.load_ratio() else {
            return Ok(());
        };
        if r < ShardedScriptHashHead::SH_SEAL_LOAD {
            return Ok(());
        }
        let marker = self.store_dir.join(MAIN_SEALED_NAME);
        let _ = std::fs::write(&marker, b"1");
        self.main_sealed
            .store(true, std::sync::atomic::Ordering::Release);
        // Fuse8 is **background** only: seal must not block tip/write while walking
        // multi‑GiB main OA + building a huge filter. Until the product lands,
        // sealed routing uses try-upsert (update-only) — slower, never blocks.
        self.spawn_main_fuse_build();
        Ok(())
    }

    /// Best-effort bg walk of main head (16 B keys) → BF8R file. Does not touch
    /// `self.main_fuse` from this thread; [`Self::main_fuse_opt`] reloads when ready.
    fn spawn_main_fuse_build(&self) {
        let dir = self.store_dir.clone();
        let _ = std::thread::Builder::new()
            .name("sh-main-fuse".into())
            .spawn(move || {
                if let Err(e) = build_main_fuse_from_disk(&dir) {
                    rbitcoin_log::warn!(
                        "store: scripthash main fuse build failed (try-upsert until retry): {e}"
                    );
                }
            });
    }

    /// In-memory fuse if ready; otherwise try load BF8R written by the bg builder.
    fn main_fuse_opt(&self) -> Option<SealedFuse8> {
        {
            let g = self.main_fuse.lock().unwrap();
            if g.is_some() {
                return g.clone();
            }
        }
        let loaded = load_main_fuse(&self.store_dir)?;
        *self.main_fuse.lock().unwrap() = Some(loaded.clone());
        Some(loaded)
    }

    /// Seal open overflow segment at load ≥ seal threshold (real BF8R + roll).
    fn maybe_seal_overflow(&self) -> Result<(), StoreError> {
        let mut g = self.overflow.lock().unwrap();
        g.maybe_seal_at_load(ShardedScriptHashHead::SH_SEAL_LOAD)
    }

    /// True when main head is sealed (no new keys; overflow only).
    pub fn main_is_sealed(&self) -> bool {
        self.main_sealed.load(std::sync::atomic::Ordering::Acquire) || !self.main_accepts_new_key()
    }
}

/// Number of hex-named shard files under `scripthash.head/` (`None` if missing/single-file).
///
/// Ignores occupancy sidecars (`00.occ`) and other non-shard files.
fn count_sh_head_shards(head_path: &Path) -> Result<Option<usize>, StoreError> {
    if !head_path.is_dir() {
        return Ok(None);
    }
    let mut names: Vec<String> = std::fs::read_dir(head_path)
        .map_err(|e| StoreError::io(head_path, e))?
        .filter_map(|e| e.ok())
        .filter(|e| e.file_type().map(|t| t.is_file()).unwrap_or(false))
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .filter(|n| n.len() == 2 && n.chars().all(|c| c.is_ascii_hexdigit()))
        .collect();
    names.sort();
    if names.is_empty() {
        return Ok(Some(0));
    }
    Ok(Some(names.len()))
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

/// Replace legacy SH head with empty current-layout head; clear body for cold reload from runs.
fn migrate_legacy_sh_head_from_runs(
    store_dir: &Path,
    body: TableFile,
) -> Result<ScriptHashTable, StoreError> {
    let head_path = store_dir.join("scripthash.head");
    let expected = shard_count_for_role(HeadRole::ScriptHash);
    let stamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let bak: PathBuf = store_dir.join(format!("scripthash.head.legacy-{stamp}"));
    if head_path.exists() {
        // Prefer rename (keep bytes for forensics); fall back to remove.
        if let Err(e) = std::fs::rename(&head_path, &bak) {
            rbitcoin_log::warn!(
                "store: could not rename legacy scripthash.head to {}: {e}; removing",
                bak.display()
            );
            if head_path.is_dir() {
                std::fs::remove_dir_all(&head_path).map_err(|e| StoreError::io(&head_path, e))?;
            } else {
                std::fs::remove_file(&head_path).map_err(|e| StoreError::io(&head_path, e))?;
            }
        } else {
            rbitcoin_log::info!(
                "store: moved legacy scripthash.head → {} (rebuild from scripthash.runs)",
                bak.display()
            );
        }
    }
    let head = ShardedScriptHashHead::create_for_role(&head_path, HeadRole::ScriptHash)?;
    let table = ScriptHashTable::from_body_and_head(store_dir, body, head)?;
    // Old body slabs are orphaned once head is gone; cold materialize rewrites both.
    table.reinit_empty_for_cold_materialize()?;
    rbitcoin_log::info!(
        "store: scripthash head migrated to {expected}-way empty layout; \
         tip entry will bulk-materialize from sorted runs (direct k-way / live OA)"
    );
    Ok(table)
}

impl ScriptHashTable {
    pub fn entry_count(&self) -> u64 {
        self.alloc.lock().unwrap().live_count
    }

    /// True when every head shard reports zero occupied slots.
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
        self.head.is_empty()
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
        // Overflow stack present means durable SH index material exists.
        !self.overflow.lock().unwrap().is_empty()
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
        let sorted_on = self.has_sorted_main();
        if sorted_on {
            if let Some(v) = self.ingest.lock().unwrap().get(scripthash)? {
                return Ok(Some((v, KeyHome::Ingest)));
            }
            let hk = head_key_from_full(scripthash);
            for h in self.sealed_ovf.lock().unwrap().iter().rev() {
                if let Some(v) = h.get(&hk)? {
                    return Ok(Some((v, KeyHome::Overflow(u32::MAX))));
                }
            }
            {
                let g = self.sorted_main.lock().unwrap();
                let si = self.head.shard_index(scripthash);
                if let Some(Some(h)) = g.get(si) {
                    if let Some(v) = h.get(&hk)? {
                        return Ok(Some((v, KeyHome::Main)));
                    }
                }
            }
            return Ok(None);
        }
        if let Some(v) = self.head.get(scripthash)? {
            return Ok(Some((v, KeyHome::Main)));
        }
        let g = self.overflow.lock().unwrap();
        if let Some((id, v)) = g.get_with_home(scripthash)? {
            return Ok(Some((v, KeyHome::Overflow(id))));
        }
        Ok(None)
    }

    fn has_sorted_main(&self) -> bool {
        self.sorted_main.lock().unwrap().iter().any(|s| s.is_some())
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
        if !self.has_sorted_main() {
            self.head.for_each_occupied(|_key, val| {
                let entries = self.collect_entries(&_key, &val)?;
                for e in entries {
                    f(e.create_tx_fk);
                }
                Ok(())
            })?;
        }
        self.ingest.lock().unwrap().for_each_occupied(|_key, val| {
            let entries = self.collect_entries(&_key, &val)?;
            for e in entries {
                f(e.create_tx_fk);
            }
            Ok(())
        })?;
        let g = self.overflow.lock().unwrap();
        g.for_each_occupied(|_key, val| {
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
    /// Head upserts use **no rehash**: existing main keys stay on main; new keys
    /// go to main until seal load (~0.8), then to overflow.
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

        // Track which segment each key already lives on (main append vs overflow).
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
            if self.has_sorted_main() {
                for key in missing {
                    if let Some((v, kh)) = self.locate_head(&key)? {
                        heads.insert(key, v);
                        home.insert(key, kh);
                    } else {
                        home.insert(key, KeyHome::Absent);
                    }
                }
            } else {
                let seeded = self.head.get_many(&missing)?;
                let mut still: Vec<[u8; 32]> = Vec::new();
                for (key, v) in missing.into_iter().zip(seeded.into_iter()) {
                    if let Some(v) = v {
                        heads.insert(key, v);
                        home.insert(key, KeyHome::Main);
                    } else {
                        still.push(key);
                    }
                }
                if !still.is_empty() {
                    let g = self.overflow.lock().unwrap();
                    for key in still {
                        if let Some((id, v)) = g.get_with_home(&key)? {
                            heads.insert(key, v);
                            home.insert(key, KeyHome::Overflow(id));
                        } else {
                            home.insert(key, KeyHome::Absent);
                        }
                    }
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

        // Walk scripthash-sorted order; per key: sort FKs, skip ≤ max, append.
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
            let _ = self.maybe_seal_main();
            let _ = self.maybe_seal_overflow();
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
        flush_each: bool,
    ) -> Result<(), StoreError> {
        let mut main_try: Vec<([u8; 32], ShHeadValue)> = Vec::new();
        let mut ovf_new: Vec<([u8; 32], ShHeadValue)> = Vec::new();
        // Home-segment updates (id → upserts).
        let mut ovf_home: HashMap<u32, Vec<([u8; 32], ShHeadValue)>> = HashMap::new();

        let fuse = self.main_fuse_opt();
        let sealed = self.main_is_sealed();
        let sorted_on = self.has_sorted_main();

        let mut ingest_ups: Vec<([u8; 32], ShHeadValue)> = Vec::new();
        let mut sealed_ovf_ups: Vec<([u8; 32], ShHeadValue)> = Vec::new();

        for (key, val, home) in upserts {
            match home {
                KeyHome::Ingest => {
                    ingest_ups.push((*key, val.clone()));
                }
                KeyHome::Overflow(id) if *id == u32::MAX => {
                    sealed_ovf_ups.push((*key, val.clone()));
                }
                KeyHome::Overflow(id) => {
                    ovf_home.entry(*id).or_default().push((*key, val.clone()));
                }
                KeyHome::Main if sorted_on => {
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
                KeyHome::Main => {
                    main_try.push((*key, val.clone()));
                }
                KeyHome::Absent if sorted_on => {
                    ingest_ups.push((*key, val.clone()));
                }
                KeyHome::Absent => {
                    if sealed {
                        if let Some(ref f) = fuse {
                            if !f.contains(sh_fuse_key(key)) {
                                ovf_new.push((*key, val.clone()));
                                continue;
                            }
                        }
                        main_try.push((*key, val.clone()));
                    } else if self.main_accepts_new_key() {
                        main_try.push((*key, val.clone()));
                    } else {
                        ovf_new.push((*key, val.clone()));
                    }
                }
            }
        }

        if !main_try.is_empty() {
            let allow_new = !sealed;
            let rem = self
                .head
                .insert_many_sharded_no_rehash(&main_try, flush_each, allow_new)?;
            ovf_new.extend(rem);
        }

        if !ovf_home.is_empty() || !ovf_new.is_empty() {
            self.ensure_overflow()?;
            let mut g = self.overflow.lock().unwrap();
            for (id, batch) in ovf_home {
                g.insert_on_segment(id, &batch)?;
            }
            if !ovf_new.is_empty() {
                g.insert_new_with_roll(&ovf_new, ShardedScriptHashHead::SH_SEAL_LOAD)?;
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
        // Replace ingest with a fresh empty OA.
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
        // Ensure sorted before pack.
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
        let n_pages = sh_page_count_for_entries(live.len());
        let mut offs = Vec::with_capacity(n_pages);
        for _ in 0..n_pages {
            offs.push(self.alloc_page(alloc)?);
        }
        let mut page = [0u8; SH_PAGE_SIZE];
        for (pi, &off) in offs.iter().enumerate() {
            let start = pi * SH_PAGE_FK_CAP;
            let end = (start + SH_PAGE_FK_CAP).min(live.len());
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
                // Roll: write full page once with next set, start empty successor.
                let new_off = self.alloc_page(alloc)?;
                sh_page_set_next(&mut page, new_off)?;
                self.body.write_at(last, &page)?;
                sh_page_init_empty(&mut page);
                assert!(sh_page_try_append(&mut page, e.create_tx_fk)?);
                last = new_off;
            }
        }
        // Single write of the open last page (was per-FK write_at before).
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
                        Some(h) if !new_val.is_empty() => h.update_value(&hk, &new_val)?,
                        _ => false,
                    }
                };
                if !updated_sorted {
                    if new_val.is_empty() {
                        self.head.clear_key(scripthash)?;
                    } else {
                        self.head.insert(scripthash, &new_val)?;
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
            KeyHome::Overflow(_) => {
                let g = self.overflow.lock().unwrap();
                if g.is_empty() {
                    return Err(StoreError::Corrupt(
                        "scripthash: overflow home without overflow stack",
                    ));
                }
                if new_val.is_empty() {
                    g.clear_key(scripthash)?;
                } else {
                    g.insert(scripthash, &new_val)?;
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
        let g = self.overflow.lock().unwrap();
        g.flush()?;
        Ok(())
    }

    pub fn flush_async(&self) -> Result<(), StoreError> {
        {
            let alloc = self.alloc.lock().unwrap();
            write_alloc_header(&self.body, &alloc)?;
        }
        self.body.flush_async()?;
        self.head.flush_async()?;
        let g = self.overflow.lock().unwrap();
        g.flush_async()?;
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
            "store: scripthash bulk_session live OA n_shards={n_shards} unique_hint={hint} \
             per_shard_keys≈{key_budget} table_MiB≈{:.1}",
            (crate::scripthash_head::sh_slots_for_keys(key_budget) as f64 * 32.0)
                / (1024.0 * 1024.0)
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
            live: None,
            key_budget,
            body_buf: Vec::with_capacity(BULK_BODY_FLUSH.min(4 << 20)),
            body_write_off: bump,
            finished: false,
            keys_written: 0,
            shards_flushed: 0,
            body_flush_ns: 0,
            head_fill_ns: 0,
            peak_table_bytes: 0,
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
             bump={bump} live_count={} keys≈{} table_MiB≈{:.1}",
            progress.live_count,
            progress.keys_written,
            (crate::scripthash_head::sh_slots_for_keys(key_budget) as f64 * 32.0)
                / (1024.0 * 1024.0)
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
            live: None,
            key_budget,
            body_buf: Vec::with_capacity(BULK_BODY_FLUSH.min(4 << 20)),
            body_write_off: bump,
            finished: false,
            keys_written: progress.keys_written,
            shards_flushed: progress.next_shard,
            body_flush_ns: 0,
            head_fill_ns: 0,
            peak_table_bytes: 0,
        })
    }

    /// Number of SH head shards (1 on Tiny, 64 on mainnet).
    pub fn head_shard_count(&self) -> usize {
        self.head.shard_count()
    }
}

/// Live-OA bulk writer for cold SH materialize.
///
/// Stream **scripthash-sorted** [`put_chain`] calls. Prefix sharding makes that
/// order contiguous per head shard: body slabs buffer (~16 MiB); each key is
/// probe-inserted into a **pre-sized** in-RAM OA image for the active shard.
/// On shard boundary the image is written once and freed. Peak head RAM ≈ one
/// final-sized shard table.
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
    live: Option<LiveShardTable>,
    /// Unique-key budget used to size each live table (final size).
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
    /// Peak live OA table allocation (bytes) — test/bench meter.
    pub peak_table_bytes: usize,
}

const BULK_BODY_FLUSH: usize = 16 * 1024 * 1024;

impl<'a> ScriptHashBulkSession<'a> {
    /// Creates written so far (sum of chain lengths, not unique keys).
    pub fn creates_written(&self) -> u64 {
        self.live_count
    }

    /// Unique keys packed so far.
    pub fn keys_written(&self) -> u64 {
        self.keys_written
    }

    /// Head shards fully installed so far.
    pub fn shards_flushed(&self) -> u32 {
        self.shards_flushed
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
        // Sort+dedup create_tx_fk ascending (cold stream may interleave within key).
        let mut fks: Vec<u64> = entries
            .iter()
            .filter(|e| !e.create_tx_fk.is_null())
            .map(|e| e.create_tx_fk.0)
            .collect();
        fks.sort_unstable();
        fks.dedup();
        let sorted: Vec<ShEntry> = fks.into_iter().map(|fk| ShEntry::new(Fk(fk))).collect();
        let n = sorted.len() as u32;
        if n == 0 {
            return Ok(());
        }
        let si = self.table.head.shard_index(&key);
        if (si as u32) < self.resume_from_shard {
            // Resume: stream still delivers earlier bands; skip without counting.
            return Ok(());
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

        self.live_count = self.live_count.saturating_add(u64::from(n));
        self.keys_written = self.keys_written.saturating_add(1);

        let val = if n <= SH_INLINE_CAP as u32 {
            if n == 1 {
                ShHeadValue::inline_one(sorted[0])
            } else {
                ShHeadValue::inline_two(sorted[0], sorted[1])
            }
        } else if n < SH_MEGAKEY_MIN_FKS {
            self.flush_body()?;
            let (val, new_bump) = Self::bulk_write_slab(&self.table.body, self.bump, &sorted)?;
            self.bump = new_bump;
            self.body_write_off = new_bump;
            val
        } else {
            // Megakey: flush then write page chain at bump (4 KiB aligned).
            self.flush_body()?;
            let (first, last, new_bump) =
                Self::bulk_write_page_chain(&self.table.body, self.bump, &sorted)?;
            self.bump = new_bump;
            self.body_write_off = new_bump;
            ShHeadValue::paged(first, last)
        };

        let live = self
            .live
            .as_mut()
            .ok_or(StoreError::Corrupt("scripthash bulk: no live shard"))?;
        live.insert(&key, &val)?;
        Ok(())
    }

    fn start_live_shard(&mut self, si: usize) -> Result<(), StoreError> {
        let live = LiveShardTable::with_key_budget(self.key_budget);
        self.peak_table_bytes = self.peak_table_bytes.max(live.table_bytes());
        rbitcoin_log::info!(
            "store: scripthash live shard start id={si} slots={} table_MiB≈{:.1} key_budget={}",
            live.slots(),
            live.table_bytes() as f64 / (1024.0 * 1024.0),
            self.key_budget
        );
        self.live = Some(live);
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
        let n_pages = sh_page_count_for_entries(entries.len());
        let end = base.saturating_add((n_pages as u64).saturating_mul(SH_PAGE_SIZE as u64));
        body.ensure_capacity(end)?;
        if end > body.logical_len() {
            body.set_logical_len(end)?;
        }
        let mut page = [0u8; SH_PAGE_SIZE];
        for pi in 0..n_pages {
            let off = base + (pi as u64) * (SH_PAGE_SIZE as u64);
            let start = pi * SH_PAGE_FK_CAP;
            let end_i = (start + SH_PAGE_FK_CAP).min(entries.len());
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
        self.table.body.ensure_capacity(need)?;
        if need > self.table.body.logical_len() {
            self.table.body.set_logical_len(need)?;
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
        self.table
            .body
            .write_at(self.body_write_off, &self.body_buf)?;
        self.body_write_off = end;
        self.body_buf.clear();
        self.body_flush_ns = self
            .body_flush_ns
            .saturating_add(t0.elapsed().as_nanos() as u64);
        Ok(())
    }

    /// Flush body buffer, install live OA image, free head RAM, write resume checkpoint.
    fn flush_active_shard(&mut self) -> Result<(), StoreError> {
        let Some(si) = self.active_shard else {
            return Ok(());
        };
        // Body slabs for this shard's keys must be durable before head points at them.
        self.flush_body()?;
        if let Some(live) = self.live.take() {
            let t0 = std::time::Instant::now();
            let keys = live.keys();
            let slots = live.slots();
            let table_mib = live.table_bytes() as f64 / (1024.0 * 1024.0);
            let occ = live.occupied();
            let recs = live.collect_sorted_recs();
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
            } else {
                self.table.head.install_live_shard(si, live)?;
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
                "store: scripthash live shard done id={si} keys={keys} occupied={occ} \
                 slots={slots} table_MiB≈{table_mib:.1} write={elapsed:?} next_shard={next}"
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
        self.live = None;
        self.active_shard = None;
        self.body_buf.clear();
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
        // Progress file already has next_shard from last complete install.
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
        if self.finished {
            return;
        }
        // Panic / cancel without abandon: do **not** install partial live shard.
        self.live = None;
        self.active_shard = None;
        self.body_buf.clear();
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

    /// Serialize `RBITCOIN_HEAD_SCALE` mutations (parallel tests share process env).
    static HEAD_SCALE_ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

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

    fn rec(sh: [u8; 32], tx: u64, _vout: u32) -> ScriptHashRecord {
        ScriptHashRecord::from_fk(sh, Fk(tx))
    }

    /// Create a SH table with **mono 64-slot** main head (suite-speed + env-race safe).
    ///
    /// Parallel tests may briefly set `RBITCOIN_HEAD_SCALE=mainnet`; hardcoding
    /// 52-key seal assumes tiny geometry. We rewrite the head to a fixed mono
    /// layout after create so ovf stack tests stay ≪2 s and deterministic.
    fn create_tiny_mono_sh(dir: &Path) -> ScriptHashTable {
        let t = ScriptHashTable::create(dir).unwrap();
        t.flush().unwrap();
        drop(t);
        let head_path = dir.join("scripthash.head");
        if head_path.is_dir() {
            let _ = std::fs::remove_dir_all(&head_path);
        } else if head_path.exists() {
            let _ = std::fs::remove_file(&head_path);
        }
        // 1 shard × 64 slots — same as HeadScale::Tiny SH create.
        ShardedScriptHashHead::create_sharded(&head_path, 1, 64).unwrap();
        let t = ScriptHashTable::open(dir).unwrap();
        assert_eq!(t.head.shard_count(), 1);
        assert_eq!(t.head.slots_per_shard(), 64);
        t
    }

    /// Put unique keys until main seals (tiny mono: ~52 keys).
    fn fill_until_main_sealed(t: &ScriptHashTable, tag: u8) -> u32 {
        let mut i = 0u32;
        while !t.main_is_sealed() {
            assert!(
                i < 200,
                "main did not seal after {i} keys (slots={})",
                t.head.total_slots()
            );
            let sh = script_hash(&[tag, (i & 0xff) as u8, (i >> 8) as u8, 0x7e]);
            t.put_create(&rec(sh, u64::from(i) + 1, 0)).unwrap();
            i += 1;
        }
        i
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
        use crate::scripthash_pages::SH_PAGE_FK_CAP;
        let dir = tmp();
        let t = ScriptHashTable::create(&dir).unwrap();
        let sh = script_hash(&[0xab]);
        // Fill past two pages so last page holds the max.
        let n = SH_PAGE_FK_CAP * 2 + 5;
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
        // Grow past one page (510 FKs/page): force multi-page with many puts.
        let mut heads2 = HashMap::new();
        let sh2 = script_hash(&[0x7b]);
        let many: Vec<_> = (1..=600u32).map(|v| rec(sh2, u64::from(v), v)).collect();
        let (nm, _) = t.put_create_batch_append(&many, &mut heads2).unwrap();
        assert_eq!(nm, 600);
        match t.head_value(&sh2).unwrap().unwrap() {
            ShHeadValue::Paged {
                first_page,
                last_page,
            } => {
                assert_ne!(first_page, last_page, "600 fks need >1 page");
            }
            other => panic!("expected multi-page, got {other:?}"),
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

    /// Batch that crosses main capacity: no dual-home, and a main-key **update**
    /// in the same batch still applies on main (update-only continues after remainder).
    #[test]
    fn put_create_batch_across_capacity_remainder_and_main_update() {
        let dir = tmp();
        let t = ScriptHashTable::create(&dir).unwrap();
        // 50 unique keys on main (load 50/64 ≈ 0.78 — still accepts new keys).
        let mut main_keys = Vec::new();
        for i in 0..50u32 {
            let sh = script_hash(&[0xc0, (i & 0xff) as u8, (i >> 8) as u8, 0x11]);
            t.put_create(&rec(sh, u64::from(i) + 1, 0)).unwrap();
            main_keys.push(sh);
        }
        let sh_update = main_keys[0];
        // One public batch: update an existing main key + 20 brand-new keys.
        // insert sorts by slot; after first new-key NeedSlot, update-only must still
        // land the main-key append on main (not skip, not dual-home on overflow).
        let mut batch = vec![rec(sh_update, 99_999, 1)];
        for i in 0..20u32 {
            let sh = script_hash(&[0xc1, (i & 0xff) as u8, 0x22, 0x33]);
            batch.push(rec(sh, 10_000 + u64::from(i), 0));
        }
        let n = t.put_create_batch(&batch).unwrap();
        assert_eq!(n, 21, "1 update append + 20 new");

        // Updated main key: both FKs, on main only.
        assert!(t.head.get(&sh_update).unwrap().is_some());
        assert!(t.contains_create(&sh_update, Fk(1)).unwrap());
        assert!(t.contains_create(&sh_update, Fk(99_999)).unwrap());
        assert_eq!(t.entries(&sh_update).unwrap().len(), 2);
        {
            let g = t.overflow.lock().unwrap();
            assert!(
                g.get(&sh_update).unwrap().is_none(),
                "main update must not dual-home on overflow"
            );
        }

        let mut for_each_n = 0u64;
        t.for_each_live_create(|_| for_each_n += 1).unwrap();
        // 50 original creates + 1 update FK + 20 new = 71 unique FKs
        assert_eq!(
            for_each_n, 71,
            "for_each must equal unique creates; dual-home would inflate"
        );

        for rec in batch.iter().skip(1) {
            assert!(t
                .contains_create(&rec.scripthash, rec.create_tx_fk)
                .unwrap());
            let on_main = t.head.get(&rec.scripthash).unwrap().is_some();
            let on_ovf = {
                let g = t.overflow.lock().unwrap();
                g.get(&rec.scripthash).unwrap().is_some()
            };
            assert!(
                on_main ^ on_ovf,
                "new key must live on exactly one head (main={on_main} ovf={on_ovf})"
            );
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// After seal, fuse excludes new keys → overflow without main get-then-insert.
    #[test]
    fn sealed_fuse_absent_goes_overflow_try_upsert_updates_main() {
        let dir = tmp();
        // Fixed mono 64-slot head — not `create()` alone (env race on HEAD_SCALE).
        let t = create_tiny_mono_sh(&dir);
        let _n_main = fill_until_main_sealed(&t, 0xd0);
        assert!(t.main_is_sealed());
        // fill_until_main_sealed: tag + i LE bytes; first key is i=0.
        let sh0 = script_hash(&[0xd0, 0, 0, 0x7e]);
        // Fuse is built asynchronously — wait for BF8R product (tiny heads are quick).
        for _ in 0..200 {
            if dir.join(MAIN_FUSE_NAME).is_file() && load_main_fuse(&dir).is_some() {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        assert!(
            load_main_fuse(&dir).is_some(),
            "bg fuse must write valid BF8R (not placeholder)"
        );
        assert!(
            t.main_fuse_opt().is_some(),
            "main_fuse_opt reloads when ready"
        );

        let sh_new = script_hash(&[0xd1, 0xaa, 0xbb, 0xcc]);
        t.put_create(&rec(sh_new, 7_777, 0)).unwrap();
        assert!(
            t.head.get(&sh_new).unwrap().is_none(),
            "fuse-absent key must not land on main"
        );
        assert_eq!(t.entries(&sh_new).unwrap().len(), 1);
        assert!(t.contains_create(&sh_new, Fk(7_777)).unwrap());

        // Existing main key: try-upsert path (fuse says present) updates main.
        t.put_create(&rec(sh0, 8_888, 1)).unwrap();
        assert!(t.head.get(&sh0).unwrap().is_some());
        assert_eq!(t.entries(&sh0).unwrap().len(), 2);
        assert!(t.contains_create(&sh0, Fk(8_888)).unwrap());

        drop(t);
        let t = ScriptHashTable::open(&dir).unwrap();
        assert!(
            t.main_fuse_opt().is_some() || load_main_fuse(&dir).is_some(),
            "fuse reloads on open"
        );
        assert_eq!(t.entries(&sh_new).unwrap().len(), 1);
        assert_eq!(t.entries(&sh0).unwrap().len(), 2);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Sealed + try-upsert path (no fuse / fuse maybe): not-present key must
    /// **not** take free main slots — remainder → overflow only.
    #[test]
    fn sealed_try_upsert_absent_key_overflows_not_main_slot() {
        let dir = tmp();
        let t = create_tiny_mono_sh(&dir);
        let n_main = fill_until_main_sealed(&t, 0xe0);
        assert!(t.main_is_sealed());
        // Free slots remain (~0.8 of 64); drop fuse so Absent is forced through
        // try-upsert (not fuse-absent short-circuit).
        *t.main_fuse.lock().unwrap() = None;
        let _ = std::fs::remove_file(dir.join(MAIN_FUSE_NAME));

        let sh_new = script_hash(&[0xe1, 0x11, 0x22, 0x33]);
        // Public put: KeyHome::Absent, sealed, no fuse → try main update-only.
        t.put_create(&rec(sh_new, 42_042, 0)).unwrap();

        assert!(
            t.head.get(&sh_new).unwrap().is_none(),
            "sealed try-upsert must not allocate free main slots for absent keys"
        );
        assert!(
            ovf_seg_path(&dir, 0).exists() || ovf_dir(&dir).is_dir(),
            "overflow mono segment required for sealed remainder"
        );
        {
            let g = t.overflow.lock().unwrap();
            assert!(
                g.get(&sh_new).unwrap().is_some(),
                "absent sealed key must live on overflow"
            );
            assert_eq!(
                g.open_segment_slots(),
                Some(t.head.slots_per_shard()),
                "ovf mono slots == one main shard"
            );
        }
        assert_eq!(t.entries(&sh_new).unwrap().len(), 1);
        assert!(t.contains_create(&sh_new, Fk(42_042)).unwrap());
        // for_each must not double-count (no dual-home).
        let mut n = 0u64;
        t.for_each_live_create(|_| n += 1).unwrap();
        assert_eq!(n, u64::from(n_main) + 1, "{n_main} main + 1 overflow only");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Tiny head: 64 slots, seal load 0.80 → after ≥52 unique keys new keys go overflow.
    #[test]
    fn seal_load_routes_new_keys_to_overflow_main_append_stays() {
        let dir = tmp();
        // Fixed mono 64-slot head — not bare create() (env race on HEAD_SCALE).
        let t = create_tiny_mono_sh(&dir);
        let mut main_keys = Vec::new();
        // 52 unique keys → load 52/64 = 0.8125 ≥ SH_SEAL_LOAD.
        for i in 0..52u32 {
            let sh = script_hash(&[0xa0, (i & 0xff) as u8, (i >> 8) as u8, 0x01]);
            t.put_create(&rec(sh, u64::from(i) + 1, 0)).unwrap();
            main_keys.push(sh);
        }
        let ratio = t.head.load_ratio().expect("occupancy known after inserts");
        assert!(
            ratio + f64::EPSILON >= ShardedScriptHashHead::SH_SEAL_LOAD,
            "expected load ≥ seal threshold, got {ratio}"
        );
        assert!(
            t.main_is_sealed(),
            "main should refuse new keys after seal load"
        );

        // New key must land in overflow, not main.
        let sh_new = script_hash(&[0xb0, 0xff, 0xee, 0x02]);
        t.put_create(&rec(sh_new, 9_001, 0)).unwrap();
        assert!(
            t.head.get(&sh_new).unwrap().is_none(),
            "new key must not occupy main after seal"
        );
        assert!(
            ovf_seg_path(&dir, 0).is_file(),
            "overflow mono segment file must exist"
        );
        {
            let g = t.overflow.lock().unwrap();
            assert_eq!(
                g.open_segment_slots()
                    .or_else(|| g.segs().first().map(|s| s.slots())),
                Some(t.head.slots_per_shard()),
                "ovf slots == one main shard (not full main size)"
            );
            assert!(g.get(&sh_new).unwrap().is_some());
            assert!(
                g.get(&sh_new).unwrap().is_some() && t.head.get(&sh_new).unwrap().is_none(),
                "key only on overflow"
            );
        }
        assert_eq!(t.entries(&sh_new).unwrap().len(), 1);
        assert!(t.contains_create(&sh_new, Fk(9_001)).unwrap());

        // Existing main key still appends on main (not overflow).
        let sh0 = main_keys[0];
        t.put_create(&rec(sh0, 10_000, 1)).unwrap();
        assert!(t.head.get(&sh0).unwrap().is_some());
        assert_eq!(t.entries(&sh0).unwrap().len(), 2);
        assert!(t.contains_create(&sh0, Fk(10_000)).unwrap());

        t.flush().unwrap();
        assert!(
            dir.join(MAIN_SEALED_NAME).is_file() || ratio >= ShardedScriptHashHead::SH_SEAL_LOAD,
            "seal marker or load gate"
        );
        // Bg fuse (may lag); sealed routing is correct via try-upsert until ready.
        for _ in 0..200 {
            if load_main_fuse(&dir).is_some() {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        assert!(
            load_main_fuse(&dir).is_some(),
            "bg main fuse BF8R after seal (tiny head)"
        );

        drop(t);
        let t = ScriptHashTable::open(&dir).unwrap();
        assert_eq!(t.entries(&sh_new).unwrap().len(), 1);
        assert_eq!(t.entries(&sh0).unwrap().len(), 2);
        assert!(t.main_is_sealed());
        // for_each sees both main and overflow creates
        let mut n = 0u64;
        t.for_each_live_create(|_| n += 1).unwrap();
        assert_eq!(n, 52 + 1 + 1, "52 main + 1 ovf + 1 main append");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// After main seal, overflow is mono OA with slots == one main shard (not 64-way).
    #[test]
    fn ovf_stack_mono_geometry_after_main_seal() {
        let dir = tmp();
        let t = create_tiny_mono_sh(&dir);
        let main_shard_slots = t.head.slots_per_shard();
        assert_eq!(main_shard_slots, 64);
        fill_until_main_sealed(&t, 0xf0);
        assert!(t.main_is_sealed());
        let sh_new = script_hash(&[0xf1, 0xaa, 0xbb, 0xcc]);
        t.put_create(&rec(sh_new, 50_001, 0)).unwrap();
        assert!(t.head.get(&sh_new).unwrap().is_none());
        assert!(ovf_seg_path(&dir, 0).is_file());
        assert!(!ovf_seg_path(&dir, 0).is_dir());
        // No 64-way shard files under ovf/.
        let names: Vec<_> = std::fs::read_dir(ovf_dir(&dir))
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .collect();
        assert!(names.iter().any(|n| n == "000000"));
        assert!(!names
            .iter()
            .any(|n| n.len() == 2 && n.chars().all(|c| c.is_ascii_hexdigit())));
        {
            let g = t.overflow.lock().unwrap();
            assert_eq!(g.open_segment_slots(), Some(main_shard_slots));
            assert!(g.get(&sh_new).unwrap().is_some());
        }
        assert_eq!(t.entries(&sh_new).unwrap().len(), 1);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Fill open overflow until seal+roll: ≥2 segments, real fuse, key on new open.
    #[test]
    fn ovf_seal_roll_via_public_put_entries() {
        let dir = tmp();
        let t = create_tiny_mono_sh(&dir);
        let n_main = fill_until_main_sealed(&t, 0xf2);
        assert!(t.main_is_sealed());
        let ovf_slots = t.head.slots_per_shard();
        assert_eq!(ovf_slots, 64);
        // Open ovf holds ~0.8 * slots unique keys before seal.
        let n_force = ((ovf_slots as f64 * ShardedScriptHashHead::SH_SEAL_LOAD).ceil() as u32) + 3;
        let mut ovf_keys = Vec::new();
        for i in 0..n_force {
            let sh = script_hash(&[0xf3, (i & 0xff) as u8, ((i >> 8) & 0xff) as u8, 0x55]);
            t.put_create(&rec(sh, 100_000 + u64::from(i), 0)).unwrap();
            ovf_keys.push(sh);
        }
        {
            let g = t.overflow.lock().unwrap();
            assert!(
                g.segment_count() >= 2,
                "expected ovf seal+roll, segs={}",
                g.segment_count()
            );
            assert_eq!(g.open_segment_slots(), Some(ovf_slots));
        }
        let fuse0 = crate::scripthash_overflow::ovf_fuse_path(&dir, 0);
        assert!(fuse0.is_file(), "sealed segment 0 must have real fuse file");
        let fuse = crate::fuse8_filter::SealedFuse8::read_from(&fuse0).expect("BF8R");
        let mut in_fuse = false;
        for k in ovf_keys.iter().take(4) {
            if fuse.contains(crate::scripthash_overflow::sh_ovf_fuse_key(k)) {
                in_fuse = true;
                break;
            }
        }
        assert!(in_fuse, "fuse should contain sealed ovf keys");
        for (i, sh) in ovf_keys.iter().enumerate() {
            assert_eq!(
                t.entries(sh).unwrap().len(),
                1,
                "missing entries for ovf key {i}"
            );
            assert!(t.head.get(sh).unwrap().is_none(), "must not dual-home main");
        }
        let mut n = 0u64;
        t.for_each_live_create(|_| n += 1).unwrap();
        assert_eq!(n, u64::from(n_main) + u64::from(n_force));
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Update a key that lives only on a sealed overflow segment (no dual-home).
    #[test]
    fn ovf_update_on_sealed_home_no_dual_home() {
        let dir = tmp();
        let t = create_tiny_mono_sh(&dir);
        let n_main = fill_until_main_sealed(&t, 0xf4);
        let ovf_slots = t.head.slots_per_shard();
        let n_force = ((ovf_slots as f64 * ShardedScriptHashHead::SH_SEAL_LOAD).ceil() as u32) + 3;
        let mut first_ovf = None;
        for i in 0..n_force {
            let sh = script_hash(&[0xf5, (i & 0xff) as u8, ((i >> 8) & 0xff) as u8, 0x66]);
            t.put_create(&rec(sh, 200_000 + u64::from(i), 0)).unwrap();
            if first_ovf.is_none() {
                first_ovf = Some(sh);
            }
        }
        let sh0 = first_ovf.expect("at least one ovf key");
        {
            let g = t.overflow.lock().unwrap();
            assert!(
                g.segment_count() >= 2,
                "need sealed segment for this test, segs={}",
                g.segment_count()
            );
            let (home_id, _) = g.get_with_home(&sh0).unwrap().expect("on ovf");
            assert!(
                !g.segs().iter().find(|s| s.id == home_id).unwrap().is_open(),
                "first keys should land on sealed segment 0"
            );
        }
        t.put_create(&rec(sh0, 300_001, 1)).unwrap();
        assert_eq!(t.entries(&sh0).unwrap().len(), 2);
        assert!(t.contains_create(&sh0, Fk(200_000)).unwrap());
        assert!(t.contains_create(&sh0, Fk(300_001)).unwrap());
        assert!(t.head.get(&sh0).unwrap().is_none(), "still not on main");
        {
            let g = t.overflow.lock().unwrap();
            let (id, _) = g.get_with_home(&sh0).unwrap().unwrap();
            let mut homes = 0u32;
            for seg in g.segs() {
                if seg.head.get(&sh0).unwrap().is_some() {
                    homes += 1;
                    assert_eq!(seg.id, id);
                }
            }
            assert_eq!(homes, 1, "must not dual-home across ovf segments");
            if let Some(open) = g.segs().last().filter(|s| s.is_open()) {
                assert!(
                    open.head.get(&sh0).unwrap().is_none() || open.id == id,
                    "sealed update must not place a second open slot"
                );
            }
        }
        let mut n = 0u64;
        t.for_each_live_create(|_| n += 1).unwrap();
        assert_eq!(n, u64::from(n_main) + u64::from(n_force) + 1);
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
            let t = create_tiny_mono_sh(&dir);
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
        // If head was rewritten to mono 64, seal + ovf still works.
        if t.head.total_slots() <= 256 {
            fill_until_main_sealed(&t, 0xf6);
            let sh_new = script_hash(&[0xf7, 0x11, 0x22, 0x33]);
            t.put_create(&rec(sh_new, 9_999, 0)).unwrap();
            assert!(ovf_seg_path(&dir, 0).is_file());
            assert_eq!(t.entries(&sh_new).unwrap().len(), 1);
        }
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
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn compact_merges_two_sealed_global_ovf_files() {
        let _g = HEAD_SCALE_ENV_LOCK
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let prev_scale = std::env::var("RBITCOIN_HEAD_SCALE").ok();
        std::env::remove_var("RBITCOIN_HEAD_SCALE");
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
        if let Some(v) = prev_scale {
            std::env::set_var("RBITCOIN_HEAD_SCALE", v);
        }
        let _ = std::fs::remove_dir_all(&dir);
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

    /// Cold bulk megakey: multi-page chain is contiguous at bump (single-pass pack
    /// writes next links on first write — no previous-page RMW).
    #[test]
    fn bulk_session_megakey_page_chain_contiguous_once() {
        use crate::scripthash_pages::{SH_PAGE_FK_CAP, SH_PAGE_SIZE};
        let dir = tmp();
        let t = ScriptHashTable::create(&dir).unwrap();
        // Two full pages + 3 FKs → 3 pages.
        let n = SH_PAGE_FK_CAP * 2 + 3;
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
            first + 2 * SH_PAGE_SIZE as u64,
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
        // Peak live table sized from unique_hint, not a multi-GiB create-count bug.
        let budget = crate::scripthash_head::sh_per_shard_key_budget(u64::from(N), 1);
        let expect_slots = crate::scripthash_head::sh_slots_for_keys(budget);
        assert_eq!(peak, (expect_slots as usize) * 32);
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
        // Hold HEAD_SCALE lock and force Tiny so open() does not require 64-way
        // mainnet layout (parallel tests may set RBITCOIN_HEAD_SCALE=mainnet).
        let _g = HEAD_SCALE_ENV_LOCK
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let prev_scale = std::env::var("RBITCOIN_HEAD_SCALE").ok();
        std::env::remove_var("RBITCOIN_HEAD_SCALE"); // Tiny default under cfg(test)
        let dir = tmp();
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
        ShardedScriptHashHead::create_sharded(dir.join("scripthash.head"), 4, 256).unwrap();
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
        match prev_scale {
            Some(v) => std::env::set_var("RBITCOIN_HEAD_SCALE", v),
            None => std::env::remove_var("RBITCOIN_HEAD_SCALE"),
        }
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
        let budget = crate::scripthash_head::sh_per_shard_key_budget(1_000, 1);
        let expect = (crate::scripthash_head::sh_slots_for_keys(budget) as usize) * 32;
        assert_eq!(peak, expect);
        assert!(
            peak < 16 * 1024 * 1024,
            "peak {peak} looks like create-count sizing"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn open_migrates_legacy_head_when_runs_present() {
        // 16-way head + catalog run → open rewrites to current shard count, keeps runs.
        let _g = HEAD_SCALE_ENV_LOCK
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        std::env::set_var("RBITCOIN_HEAD_SCALE", "mainnet");
        let dir = tmp();
        // Body only (empty alloc), then force 16-way head.
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
        ShardedScriptHashHead::create_sharded(dir.join("scripthash.head"), 16, 64).unwrap();

        let runs_dir = dir.join("scripthash.runs");
        std::fs::create_dir_all(&runs_dir).unwrap();
        let mut rec = [0u8; 40];
        rec[0] = 0xab;
        rec[32..40].copy_from_slice(&1u64.to_le_bytes());
        let path = crate::sorted_run::next_run_path(&runs_dir, 1);
        crate::sorted_run::write_sorted_run(&path, 32, 40, &rec).unwrap();
        assert!(has_sh_run_rebuild_source(&dir));

        let t = ScriptHashTable::open(&dir).unwrap();
        assert_eq!(t.head_shard_count(), 64);
        assert!(t.head_is_empty());
        assert_eq!(t.entry_count(), 0);
        let catalog = list_runs(&runs_dir).unwrap();
        assert_eq!(catalog.len(), 1, "runs must survive migration");
        // Cold materialize from the preserved run.
        let mut session = t.bulk_session(16).unwrap();
        session
            .put_chain(
                {
                    let mut k = [0u8; 32];
                    k[0] = 0xab;
                    k
                },
                &[ShEntry::new(Fk(1))],
            )
            .unwrap();
        let (n, _, _, _) = session.finish().unwrap();
        assert_eq!(n, 1);
        assert_eq!(t.entries(&[0xab; 32]).unwrap().len(), 0); // key is 0xab then zeros
        let mut full = [0u8; 32];
        full[0] = 0xab;
        assert_eq!(t.entries(&full).unwrap().len(), 1);
        std::env::remove_var("RBITCOIN_HEAD_SCALE");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn open_refuses_legacy_head_without_runs() {
        let _g = HEAD_SCALE_ENV_LOCK
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        std::env::set_var("RBITCOIN_HEAD_SCALE", "mainnet");
        let dir = tmp();
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
        ShardedScriptHashHead::create_sharded(dir.join("scripthash.head"), 16, 64).unwrap();
        assert!(!has_sh_run_rebuild_source(&dir));
        match ScriptHashTable::open(&dir) {
            Err(StoreError::Corrupt(m)) if m == SH_HEAD_SHARD_COUNT_MISMATCH => {}
            Ok(_) => panic!("expected shard mismatch error"),
            Err(e) => panic!("unexpected error: {e}"),
        }
        std::env::remove_var("RBITCOIN_HEAD_SCALE");
        let _ = std::fs::remove_dir_all(&dir);
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
