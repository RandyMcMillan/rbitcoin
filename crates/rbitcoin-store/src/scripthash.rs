//! Class B scripthash multimap (Electrum: SHA256(scriptPubKey)).
//!
//! Hybrid layout (schema v6): head key = 16 B hash prefix; value = two u64s
//! (≤2 inline create_tx_fks or slab meta). Body slabs pack 8 B create_tx_fks only;
//! vouts are expanded from Class A at query.

use crate::error::StoreError;
use crate::file::{TableFile, FILE_HEADER_LEN};
use crate::hashhead::HeadRole;
use crate::scripthash_head::{
    sh_per_shard_key_budget, sh_unique_hint_default, LiveShardTable, ShardedScriptHashHead,
    SH_HEAD_SHARD_COUNT_MISMATCH,
};
use crate::scripthash_layout::{
    class_for_count, payload_start, slab_bytes, slab_cap, ShEntry, ShHeadValue, SH_ALLOC_HEADER_LEN,
    SH_ALLOC_MAGIC, SH_ALLOC_VERSION, SH_ENTRY_LEN, SH_INLINE_CAP, SH_MAX_CLASS,
};
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
    body: TableFile,
    head: ShardedScriptHashHead,
    alloc: Mutex<AllocState>,
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
        Ok(Self {
            body,
            head: ShardedScriptHashHead::create_for_role(
                dir.join("scripthash.head"),
                HeadRole::ScriptHash,
            )?,
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
        match ShardedScriptHashHead::open_for_role(&head_path, HeadRole::ScriptHash) {
            Ok(head) => Self::from_body_and_head(body, head),
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
        body: TableFile,
        head: ShardedScriptHashHead,
    ) -> Result<Self, StoreError> {
        let state = read_alloc_header(&body)?;
        Ok(Self {
            body,
            head,
            alloc: Mutex::new(state),
        })
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
    if load_fanin_checkpoint(&merge)
        .ok()
        .flatten()
        .is_some()
    {
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
    let table = ScriptHashTable::from_body_and_head(body, head)?;
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
        self.body
            .path()
            .parent()
            .unwrap_or_else(|| Path::new("."))
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
        self.entry_count() > 0 || !self.head_is_empty()
    }

    /// Head value for a key (process-cache seed / disconnect refresh).
    pub fn head_value(&self, scripthash: &[u8; 32]) -> Result<Option<ShHeadValue>, StoreError> {
        self.head.get(scripthash)
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

    /// Visit every live create_tx_fk across all keys (head occupancy walk).
    pub fn for_each_live_create(&self, mut f: impl FnMut(Fk)) -> Result<(), StoreError> {
        self.head.for_each_occupied(|_key, val| {
            let entries = self.collect_entries(&_key, &val)?;
            for e in entries {
                f(e.create_tx_fk);
            }
            Ok(())
        })
    }

    /// Live creates for a scripthash (oldest → newest).
    ///
    /// Second element is a thin index row (no Class A joins). Expand at query.
    pub fn entries(
        &self,
        scripthash: &[u8; 32],
    ) -> Result<Vec<(Fk, ScriptHashRecord)>, StoreError> {
        let Some(val) = self.head.get(scripthash)? else {
            return Ok(Vec::new());
        };
        let list = self.collect_entries(scripthash, &val)?;
        Ok(list
            .into_iter()
            .map(|e| {
                // Synthetic fk = create_tx_fk for API compat (no body row id).
                (
                    e.create_tx_fk,
                    ScriptHashRecord::from_entry(*scripthash, e),
                )
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
            ShHeadValue::Slab {
                used, slab_off, ..
            } => {
                let nbytes = *used as usize * SH_ENTRY_LEN;
                let mut buf = vec![0u8; nbytes];
                self.body.read_at(*slab_off, &mut buf)?;
                let mut out = Vec::with_capacity(*used as usize);
                for i in 0..*used as usize {
                    out.push(ShEntry::decode(&buf[i * SH_ENTRY_LEN..(i + 1) * SH_ENTRY_LEN])?);
                }
                Ok(out)
            }
        }
    }

    pub fn contains_create(
        &self,
        scripthash: &[u8; 32],
        create_tx_fk: Fk,
    ) -> Result<bool, StoreError> {
        for (_fk, rec) in self.entries(scripthash)? {
            if rec.create_tx_fk == create_tx_fk {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Append a create (idempotent on create_tx_fk).
    pub fn put_create(&self, rec: &ScriptHashRecord) -> Result<(), StoreError> {
        if rec.create_tx_fk.is_null() {
            return Err(StoreError::InvalidFk);
        }
        if self.contains_create(&rec.scripthash, rec.create_tx_fk)? {
            return Ok(());
        }
        let mut heads = HashMap::new();
        if let Some(v) = self.head.get(&rec.scripthash)? {
            heads.insert(rec.scripthash, v);
        }
        let _ = self.put_create_batch_append(std::slice::from_ref(rec), &mut heads)?;
        Ok(())
    }

    /// Bulk append with durable dup walk. Returns how many were written.
    pub fn put_create_batch(&self, recs: &[ScriptHashRecord]) -> Result<usize, StoreError> {
        if recs.is_empty() {
            return Ok(0);
        }
        let mut known: HashMap<[u8; 32], Vec<Fk>> = HashMap::new();
        let mut heads: HashMap<[u8; 32], ShHeadValue> = HashMap::new();
        for rec in recs {
            if !known.contains_key(&rec.scripthash) {
                let mut fks = Vec::new();
                for (_fk, e) in self.entries(&rec.scripthash)? {
                    fks.push(e.create_tx_fk);
                }
                known.insert(rec.scripthash, fks);
                if let Some(v) = self.head.get(&rec.scripthash)? {
                    heads.insert(rec.scripthash, v);
                }
            }
        }
        let filtered: Vec<ScriptHashRecord> = recs
            .iter()
            .filter(|rec| {
                if rec.create_tx_fk.is_null() {
                    return false;
                }
                let durable = known.get(&rec.scripthash).map(|v| v.as_slice()).unwrap_or(&[]);
                !durable.iter().any(|&c| c == rec.create_tx_fk)
            })
            .cloned()
            .collect();
        let (n, _) = self.put_create_batch_append(&filtered, &mut heads)?;
        Ok(n)
    }

    /// Forward-append creates (no durable chain walk). Process-local `heads` map.
    ///
    /// Tip-mode / steady-state path (not cold bulk materialize). Returns
    /// `(written_count, timing)`.
    ///
    /// Creates for the **same scripthash** are applied in one body write (after
    /// sorting). Head upserts are applied **per shard**; when `recs.len()` is
    /// ≥ [`Self::LARGE_BATCH_ROWS`], each shard is flushed before the next to
    /// limit dirty head pages on large tip batches.
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
            // Seed in key order (same as body apply) for probe locality.
            missing.sort_unstable();
            for key in missing {
                if let Some(v) = self.head.get(&key)? {
                    heads.insert(key, v);
                }
            }
        }
        timing.seed_ns = t_seed.elapsed().as_nanos() as u64;

        // Walk sorted order; one body write per distinct scripthash with all
        // new create_tx_fks for that key.
        let t_body = std::time::Instant::now();
        let mut head_final: Vec<([u8; 32], ShHeadValue)> = Vec::new();
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
            let mut new_ents: Vec<ShEntry> = Vec::new();
            let mut seen_fk: Vec<Fk> = Vec::new();
            while i < order.len() {
                let rec = &recs[order[i]];
                if rec.scripthash != key {
                    break;
                }
                if !rec.create_tx_fk.is_null() && !seen_fk.iter().any(|&c| c == rec.create_tx_fk) {
                    seen_fk.push(rec.create_tx_fk);
                    new_ents.push(ShEntry::new(rec.create_tx_fk));
                }
                i += 1;
            }
            if new_ents.is_empty() {
                continue;
            }

            let cur = heads.get(&key).cloned().unwrap_or(ShHeadValue::Empty);
            let mut old_live = self.collect_entries_locked(&cur)?;
            let add: Vec<ShEntry> = new_ents
                .into_iter()
                .filter(|e| {
                    !old_live
                        .iter()
                        .any(|o| o.create_tx_fk == e.create_tx_fk)
                })
                .collect();
            if add.is_empty() {
                continue;
            }
            written += add.len();
            old_live.extend(add);
            let new_val = self.write_entries_for_key(&mut alloc, &cur, &old_live)?;
            heads.insert(key, new_val.clone());
            head_final.push((key, new_val));
        }

        write_alloc_header(&self.body, &alloc)?;
        drop(alloc);
        timing.body_ns = t_body.elapsed().as_nanos() as u64;

        if !head_final.is_empty() {
            let t_head = std::time::Instant::now();
            // Per-shard apply; flush each shard when this batch is "large".
            let flush_each = recs.len() as u64 >= Self::LARGE_BATCH_ROWS;
            self.head
                .insert_many_sharded(&head_final, flush_each)?;
            timing.head_ns = t_head.elapsed().as_nanos() as u64;
        }
        Ok((written, timing))
    }

    /// ≈1M create rows: materialize flushes each head shard after its bucket.
    pub const LARGE_BATCH_ROWS: u64 = 1_000_000;

    fn collect_entries_locked(&self, val: &ShHeadValue) -> Result<Vec<ShEntry>, StoreError> {
        match val {
            ShHeadValue::Empty => Ok(Vec::new()),
            ShHeadValue::Inline { .. } => Ok(val.inline_entries().to_vec()),
            ShHeadValue::Slab {
                used, slab_off, ..
            } => {
                let nbytes = *used as usize * SH_ENTRY_LEN;
                let mut buf = vec![0u8; nbytes];
                self.body.read_at(*slab_off, &mut buf)?;
                let mut out = Vec::with_capacity(*used as usize);
                for i in 0..*used as usize {
                    out.push(ShEntry::decode(&buf[i * SH_ENTRY_LEN..(i + 1) * SH_ENTRY_LEN])?);
                }
                Ok(out)
            }
        }
    }

    /// Pack `live` (full set, oldest→newest) into inline or slab; free old slab if needed.
    ///
    /// **Same-class growth is append-only:** when `old` is already a slab of the
    /// target class and `live` extends it (`n >= old.used`, prefix unchanged on
    /// disk), only the new tail is written. Shrinks / reorder (unlink) still
    /// rewrite the used prefix; class bumps allocate a new slab and copy all.
    fn write_entries_for_key(
        &self,
        alloc: &mut AllocState,
        old: &ShHeadValue,
        live: &[ShEntry],
    ) -> Result<ShHeadValue, StoreError> {
        let n = live.len() as u32;
        let old_live = old.used();
        if n > old_live {
            alloc.live_count = alloc.live_count.saturating_add(u64::from(n - old_live));
        } else if n < old_live {
            alloc.live_count = alloc.live_count.saturating_sub(u64::from(old_live - n));
        }

        if n == 0 {
            self.free_if_slab(alloc, old)?;
            return Ok(ShHeadValue::Empty);
        }
        if n <= SH_INLINE_CAP as u32 {
            self.free_if_slab(alloc, old)?;
            return Ok(if n == 1 {
                ShHeadValue::inline_one(live[0])
            } else {
                ShHeadValue::inline_two(live[0], live[1])
            });
        }

        let class = class_for_count(n).ok_or(StoreError::Corrupt(
            "scripthash entry count exceeds max slab class",
        ))?;

        // Reuse existing slab if same class and capacity sufficient.
        if let ShHeadValue::Slab {
            class: oc,
            used: old_used,
            slab_off,
        } = old
        {
            if *oc == class && slab_cap(*oc) >= n {
                if n >= *old_used && (live.len() as u32) >= *old_used {
                    // Append-only: disk already holds live[..old_used].
                    let skip = *old_used as usize;
                    if skip < live.len() {
                        let tail_off =
                            *slab_off + (*old_used as u64) * SH_ENTRY_LEN as u64;
                        self.write_slab_entries(tail_off, &live[skip..])?;
                    }
                } else {
                    // Shrink / reorder (unlink swap-remove): rewrite used prefix.
                    self.write_slab_entries(*slab_off, live)?;
                }
                return Ok(ShHeadValue::Slab {
                    class: *oc,
                    used: n,
                    slab_off: *slab_off,
                });
            }
        }

        let off = self.alloc_slab(alloc, class)?;
        self.write_slab_entries(off, live)?;
        self.free_if_slab(alloc, old)?;
        Ok(ShHeadValue::Slab {
            class,
            used: n,
            slab_off: off,
        })
    }

    fn write_slab_entries(&self, off: u64, live: &[ShEntry]) -> Result<(), StoreError> {
        if live.is_empty() {
            return Ok(());
        }
        let mut blob = Vec::with_capacity(live.len() * SH_ENTRY_LEN);
        for e in live {
            blob.extend_from_slice(&e.encode());
        }
        self.body.write_at(off, &blob)
    }

    fn alloc_slab(&self, alloc: &mut AllocState, class: u8) -> Result<u64, StoreError> {
        if class > SH_MAX_CLASS {
            return Err(StoreError::Corrupt("scripthash slab class overflow"));
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
        let off = alloc.bump;
        alloc.bump = alloc.bump.saturating_add(need);
        self.body.ensure_capacity(alloc.bump)?;
        if alloc.bump > self.body.logical_len() {
            self.body.set_logical_len(alloc.bump)?;
        }
        Ok(off)
    }

    fn free_if_slab(&self, alloc: &mut AllocState, old: &ShHeadValue) -> Result<(), StoreError> {
        if let ShHeadValue::Slab { class, slab_off, .. } = old {
            self.free_slab(alloc, *class, *slab_off)?;
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
    /// Swap-remove; demote slab→inline when used≤2.
    pub fn unlink_create(
        &self,
        scripthash: &[u8; 32],
        create_tx_fk: Fk,
        _vout: u32,
    ) -> Result<bool, StoreError> {
        let Some(val) = self.head.get(scripthash)? else {
            return Ok(false);
        };
        let mut live = self.collect_entries(scripthash, &val)?;
        let Some(pos) = live.iter().position(|e| e.create_tx_fk == create_tx_fk) else {
            return Ok(false);
        };
        live.swap_remove(pos);
        let mut alloc = self.alloc.lock().unwrap();
        let new_val = self.write_entries_for_key(&mut alloc, &val, &live)?;
        write_alloc_header(&self.body, &alloc)?;
        drop(alloc);
        if new_val.is_empty() {
            self.head.clear_key(scripthash)?;
        } else {
            self.head.insert(scripthash, &new_val)?;
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
        Ok(())
    }

    pub fn flush_async(&self) -> Result<(), StoreError> {
        {
            let alloc = self.alloc.lock().unwrap();
            write_alloc_header(&self.body, &alloc)?;
        }
        self.body.flush_async()?;
        self.head.flush_async()?;
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
        if !self.head.is_empty() {
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

    /// Pack one key's live creates (oldest→newest). Empty chains are skipped.
    ///
    /// Keys must be presented in **non-decreasing scripthash order** (sorted-run
    /// merge). Crossing a prefix-shard boundary installs the previous live image.
    pub fn put_chain(&mut self, key: [u8; 32], entries: &[ShEntry]) -> Result<(), StoreError> {
        let n = entries.len() as u32;
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
                ShHeadValue::inline_one(entries[0])
            } else {
                ShHeadValue::inline_two(entries[0], entries[1])
            }
        } else {
            let class = class_for_count(n).ok_or(StoreError::Corrupt(
                "scripthash bulk: entry count exceeds max slab class",
            ))?;
            let need = slab_bytes(class);
            let off = self.bump;
            self.bump = self.bump.saturating_add(need);
            let pending_end = self.body_write_off + self.body_buf.len() as u64;
            if pending_end < off {
                self.flush_body()?;
            }
            for e in entries {
                self.body_buf.extend_from_slice(&e.encode());
            }
            let live_bytes = entries.len() * SH_ENTRY_LEN;
            let pad = need as usize - live_bytes;
            if pad > 0 {
                self.body_buf.resize(self.body_buf.len() + pad, 0);
            }
            if self.body_buf.len() >= BULK_BODY_FLUSH {
                self.flush_body()?;
            }
            ShHeadValue::Slab {
                class,
                used: n,
                slab_off: off,
            }
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
                if !r.create_tx_fk.is_null() && !seen.iter().any(|&c| c == r.create_tx_fk) {
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
            self.table.head.install_live_shard(si, live)?;
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
            self.head_fill_ns = self
                .head_fill_ns
                .saturating_add(elapsed.as_nanos() as u64);
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

fn read_alloc_header(body: &TableFile) -> Result<AllocState, StoreError> {
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
    if ver != SH_ALLOC_VERSION {
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
    Ok(AllocState {
        live_count,
        bump,
        free_head,
    })
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
    fn promote_ladder_inline_to_slab() {
        let dir = tmp();
        let t = ScriptHashTable::create(&dir).unwrap();
        let sh = script_hash(&[0x52]);
        for i in 1..=5u64 {
            t.put_create(&rec(sh, i, i as u32)).unwrap();
        }
        assert_eq!(t.entries(&sh).unwrap().len(), 5);
        let v = t.head_value(&sh).unwrap().unwrap();
        match v {
            ShHeadValue::Slab { class, used, .. } => {
                assert_eq!(class, 1); // cap 8
                assert_eq!(used, 5);
            }
            other => panic!("expected slab, got {other:?}"),
        }
        assert_eq!(t.entry_count(), 5);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn bulk_class_for_count_single_slab() {
        let dir = tmp();
        let t = ScriptHashTable::create(&dir).unwrap();
        let sh = script_hash(&[0x53]);
        let recs: Vec<_> = (0..100u32)
            .map(|v| rec(sh, u64::from(v) + 1, v))
            .collect();
        let n = t.put_create_batch(&recs).unwrap();
        assert_eq!(n, 100);
        let v = t.head_value(&sh).unwrap().unwrap();
        match v {
            ShHeadValue::Slab { class, used, .. } => {
                assert_eq!(used, 100);
                assert_eq!(class, class_for_count(100).unwrap());
            }
            other => panic!("expected slab, got {other:?}"),
        }
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
    fn same_class_append_preserves_prefix_and_order() {
        // Grow within class 1 (cap 8): first 5 entries, then +2 without class bump.
        // Append-only path must leave the original prefix intact on disk.
        let dir = tmp();
        let t = ScriptHashTable::create(&dir).unwrap();
        let sh = script_hash(&[0x7a]);
        let mut heads = HashMap::new();
        let first: Vec<_> = (1..=5u32).map(|v| rec(sh, u64::from(v), v)).collect();
        let (n, _) = t.put_create_batch_append(&first, &mut heads).unwrap();
        assert_eq!(n, 5);
        let off = match t.head_value(&sh).unwrap().unwrap() {
            ShHeadValue::Slab {
                class,
                used,
                slab_off,
            } => {
                assert_eq!(class, 1);
                assert_eq!(used, 5);
                slab_off
            }
            other => panic!("expected slab, got {other:?}"),
        };
        let more: Vec<_> = (6..=7u32).map(|v| rec(sh, u64::from(v), v)).collect();
        let (n2, _) = t.put_create_batch_append(&more, &mut heads).unwrap();
        assert_eq!(n2, 2);
        match t.head_value(&sh).unwrap().unwrap() {
            ShHeadValue::Slab {
                class,
                used,
                slab_off,
            } => {
                assert_eq!(class, 1);
                assert_eq!(used, 7);
                assert_eq!(slab_off, off, "same-class growth must reuse slab");
            }
            other => panic!("expected slab, got {other:?}"),
        }
        let ents = t.entries(&sh).unwrap();
        assert_eq!(ents.len(), 7);
        for (i, (_, e)) in ents.iter().enumerate() {
            assert_eq!(e.create_tx_fk, Fk(i as u64 + 1));
        }
        // One more wave that still fits (used 7 → 8, class 1 cap 8).
        let last = vec![rec(sh, 8, 8)];
        let (n3, _) = t.put_create_batch_append(&last, &mut heads).unwrap();
        assert_eq!(n3, 1);
        assert_eq!(t.entries(&sh).unwrap().len(), 8);
        // Class bump (9 needs class 2, cap 16): new slab, full history preserved.
        let (n4, _) = t
            .put_create_batch_append(&[rec(sh, 9, 9)], &mut heads)
            .unwrap();
        assert_eq!(n4, 1);
        match t.head_value(&sh).unwrap().unwrap() {
            ShHeadValue::Slab { class, used, .. } => {
                assert_eq!(class, 2);
                assert_eq!(used, 9);
            }
            other => panic!("expected class-2 slab, got {other:?}"),
        }
        let ents = t.entries(&sh).unwrap();
        assert_eq!(ents.len(), 9);
        for (i, (_, e)) in ents.iter().enumerate() {
            assert_eq!(e.create_tx_fk, Fk(i as u64 + 1));
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn unlink_demotes_slab_to_inline() {
        let dir = tmp();
        let t = ScriptHashTable::create(&dir).unwrap();
        let sh = script_hash(&[0x54]);
        for i in 1..=3u64 {
            t.put_create(&rec(sh, i, i as u32)).unwrap();
        }
        assert!(matches!(
            t.head_value(&sh).unwrap().unwrap(),
            ShHeadValue::Slab { used: 3, .. }
        ));
        t.unlink_create(&sh, Fk(2), 2).unwrap();
        match t.head_value(&sh).unwrap().unwrap() {
            ShHeadValue::Inline { used, .. } => assert_eq!(used, 2),
            other => panic!("expected inline demote, got {other:?}"),
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn freelist_reuses_class() {
        let dir = tmp();
        let t = ScriptHashTable::create(&dir).unwrap();
        let sh1 = script_hash(&[0x61]);
        let sh2 = script_hash(&[0x62]);
        for i in 1..=3u64 {
            t.put_create(&rec(sh1, i, i as u32)).unwrap();
        }
        let off1 = match t.head_value(&sh1).unwrap().unwrap() {
            ShHeadValue::Slab { slab_off, .. } => slab_off,
            _ => panic!("slab"),
        };
        // Unlink all → free slab
        for i in 1..=3u64 {
            t.unlink_create(&sh1, Fk(i), i as u32).unwrap();
        }
        for i in 1..=3u64 {
            t.put_create(&rec(sh2, 10 + i, i as u32)).unwrap();
        }
        let off2 = match t.head_value(&sh2).unwrap().unwrap() {
            ShHeadValue::Slab { slab_off, .. } => slab_off,
            _ => panic!("slab"),
        };
        assert_eq!(off1, off2, "class-0 freelist should reuse offset");
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
        session
            .put_chain(sh, &[ShEntry::new(Fk(42))])
            .unwrap();
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
        session
            .put_chain(sh, &[ShEntry::new(Fk(1))])
            .unwrap();
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
        session
            .put_chain(sh, &[ShEntry::new(Fk(1))])
            .unwrap();
        let peak = session.peak_table_bytes;
        let _ = session.finish().unwrap();
        let budget = crate::scripthash_head::sh_per_shard_key_budget(1_000, 1);
        let expect = (crate::scripthash_head::sh_slots_for_keys(budget) as usize) * 32;
        assert_eq!(peak, expect);
        assert!(peak < 16 * 1024 * 1024, "peak {peak} looks like create-count sizing");
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
        t.put_create_batch_append(
            &[rec(sh, 1, 0), rec(sh, 2, 0), rec(sh, 3, 0)],
            &mut heads,
        )
        .unwrap();
        t.unlink_create(&sh, Fk(2), 0).unwrap();
        let mut seen = Vec::new();
        t.for_each_live_create(|c| seen.push(c.0)).unwrap();
        seen.sort_unstable();
        assert_eq!(seen, vec![1, 3]);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
