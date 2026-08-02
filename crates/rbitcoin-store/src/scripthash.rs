//! Class B scripthash multimap (Electrum: SHA256(scriptPubKey)).
//!
//! Hybrid layout (schema v6): head key = 16 B hash prefix; value = two u64s
//! (≤2 inline create_tx_fks or slab meta). Body slabs pack 8 B create_tx_fks only;
//! vouts are expanded from Class A at query.

use crate::error::StoreError;
use crate::file::{TableFile, FILE_HEADER_LEN};
use crate::hashhead::HeadRole;
use crate::scripthash_head::ShardedScriptHashHead;
use crate::scripthash_layout::{
    class_for_count, payload_start, slab_bytes, slab_cap, ShEntry, ShHeadValue, SH_ALLOC_HEADER_LEN,
    SH_ALLOC_MAGIC, SH_ALLOC_VERSION, SH_ENTRY_LEN, SH_INLINE_CAP, SH_MAX_CLASS,
};
use bitcoin_hashes::{sha256, Hash};
use rbitcoin_primitives::{Fk, TableKind};
use std::collections::HashMap;
use std::sync::Mutex;

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
        Self::from_body_and_head(
            body,
            ShardedScriptHashHead::open_for_role(dir.join("scripthash.head"), HeadRole::ScriptHash)?,
        )
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

    /// Start a buffered cold bulk session (historical migration builder path).
    ///
    /// Pre-sizes **empty** head shards for `expected_keys`. Callers stream
    /// **scripthash-sorted** chains via [`ScriptHashBulkSession::put_chain`]:
    /// with prefix sharding, lex order is contiguous per shard, so the session
    /// bulk-fills each head as the stream crosses a shard boundary (only one
    /// shard's head values in RAM). Exclusive until finish.
    pub fn bulk_session(&self, expected_keys: u64) -> Result<ScriptHashBulkSession<'_>, StoreError> {
        if !self.head.is_empty() {
            return Err(StoreError::Corrupt(
                "scripthash bulk_session requires empty head (reinit first)",
            ));
        }
        if expected_keys > 0 {
            self.head.reserve_for_cold_bulk(expected_keys)?;
        }
        let n_shards = self.head.shard_count().max(1);
        let per_cap = (expected_keys as usize)
            .div_ceil(n_shards)
            .saturating_add(1)
            .min(1 << 20);
        let (bump, live_count) = {
            let a = self.alloc.lock().unwrap();
            (a.bump, a.live_count)
        };
        Ok(ScriptHashBulkSession {
            table: self,
            bump,
            live_count,
            active_shard: None,
            head_buf: Vec::with_capacity(per_cap),
            head_cap_hint: per_cap,
            body_buf: Vec::with_capacity(BULK_BODY_FLUSH.min(4 << 20)),
            body_write_off: bump,
            finished: false,
            keys_written: 0,
            shards_flushed: 0,
            body_flush_ns: 0,
            head_fill_ns: 0,
        })
    }

    /// Number of SH head shards (1 on Tiny, 16 on mainnet).
    pub fn head_shard_count(&self) -> usize {
        self.head.shard_count()
    }
}

/// Buffered bulk writer for cold SH materialize.
///
/// Stream **scripthash-sorted** [`put_chain`] calls (as sorted runs produce).
/// Prefix sharding makes that order contiguous per head shard: body slabs
/// buffer (~16 MiB); when the stream enters a new shard, the previous shard's
/// head is written with one empty-table bulk fill and the head buffer is
/// freed. Peak head RAM ≈ one shard only.
pub struct ScriptHashBulkSession<'a> {
    table: &'a ScriptHashTable,
    bump: u64,
    live_count: u64,
    active_shard: Option<usize>,
    /// Deferred heads for `active_shard` only.
    head_buf: Vec<([u8; 32], ShHeadValue)>,
    head_cap_hint: usize,
    body_buf: Vec<u8>,
    body_write_off: u64,
    finished: bool,
    keys_written: u64,
    shards_flushed: u32,
    /// Wall time spent in body `write_at` flushes.
    pub body_flush_ns: u64,
    /// Wall time spent bulk-filling head shards.
    pub head_fill_ns: u64,
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

    /// Head shards fully bulk-filled so far.
    pub fn shards_flushed(&self) -> u32 {
        self.shards_flushed
    }

    /// Pack one key's live creates (oldest→newest). Empty chains are skipped.
    ///
    /// Keys must be presented in **non-decreasing scripthash order** (sorted-run
    /// merge). Crossing a prefix-shard boundary flushes the previous shard head.
    pub fn put_chain(&mut self, key: [u8; 32], entries: &[ShEntry]) -> Result<(), StoreError> {
        let n = entries.len() as u32;
        if n == 0 {
            return Ok(());
        }
        let si = self.table.head.shard_index(&key);
        if self.active_shard != Some(si) {
            if let Some(prev) = self.active_shard {
                if si < prev {
                    return Err(StoreError::Corrupt(
                        "scripthash bulk put_chain: keys not sorted by scripthash (shard went backwards)",
                    ));
                }
                self.flush_active_shard()?;
            }
            self.active_shard = Some(si);
            self.head_buf = Vec::with_capacity(self.head_cap_hint);
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

        self.head_buf.push((key, val));
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

    /// Flush body buffer, bulk-fill active shard head, free head RAM.
    fn flush_active_shard(&mut self) -> Result<(), StoreError> {
        let Some(si) = self.active_shard else {
            return Ok(());
        };
        // Body slabs for this shard's keys must be durable before head points at them.
        self.flush_body()?;
        if !self.head_buf.is_empty() {
            let t0 = std::time::Instant::now();
            self.table
                .head
                .bulk_fill_one_shard_cold(si, &mut self.head_buf)?;
            self.head_fill_ns = self
                .head_fill_ns
                .saturating_add(t0.elapsed().as_nanos() as u64);
            self.shards_flushed = self.shards_flushed.saturating_add(1);
        }
        self.head_buf.clear();
        self.head_buf.shrink_to_fit();
        self.active_shard = None;
        Ok(())
    }

    /// Flush last shard head + alloc header.
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
        let _ = self.flush_active_shard();
        if self.bump > self.table.body.logical_len() {
            let _ = self.table.body.set_logical_len(self.bump);
        }
        let state = AllocState {
            live_count: self.live_count,
            bump: self.bump,
            free_head: [0; SH_MAX_CLASS as usize + 1],
        };
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
        // Active-shard heads stay in RAM until the stream crosses a prefix-shard
        // boundary (or finish); each boundary does one bulk_fill_empty.
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
            // Active shard not yet bulk-filled: this key is only in the head buffer.
            if i == 70_000 {
                assert!(
                    t.head_value(&sh).unwrap().is_none(),
                    "active-shard heads must not land until shard boundary"
                );
            }
        }
        let (creates, keys, _, _) = session.finish().unwrap();
        assert_eq!(creates, u64::from(N));
        assert_eq!(keys, u64::from(N));
        assert_eq!(t.entry_count(), u64::from(N));
        // Spot-check a few keys survive bulk fill.
        for i in [0u32, 1, 65_535, 70_000, N - 1] {
            let ents = t.entries(&key(i)).unwrap();
            assert_eq!(ents.len(), 1, "i={i}");
            assert_eq!(ents[0].1.create_tx_fk, Fk(u64::from(i) + 1));
        }
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
