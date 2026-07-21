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

    /// True when `scripthash.body` looks like hybrid (SHAL magic).
    pub fn body_is_hybrid(dir: &std::path::Path) -> Result<bool, StoreError> {
        let path = dir.join("scripthash.body");
        if !path.exists() {
            return Ok(false);
        }
        let mut f = std::fs::File::open(&path).map_err(|e| StoreError::io(&path, e))?;
        use std::io::{Read, Seek, SeekFrom};
        f.seek(SeekFrom::Start(FILE_HEADER_LEN as u64))
            .map_err(|e| StoreError::io(&path, e))?;
        let mut magic = [0u8; 4];
        match f.read_exact(&mut magic) {
            Ok(()) => Ok(magic == SH_ALLOC_MAGIC),
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => Ok(false),
            Err(e) => Err(StoreError::io(&path, e)),
        }
    }

    pub fn entry_count(&self) -> u64 {
        self.alloc.lock().unwrap().live_count
    }

    /// Head value for a key (process-cache seed / disconnect refresh).
    pub fn head_value(&self, scripthash: &[u8; 32]) -> Result<Option<ShHeadValue>, StoreError> {
        self.head.get(scripthash)
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

    /// Deprecated alias: true if the key has any live creates.
    pub fn live_head(&self, scripthash: &[u8; 32]) -> Result<Fk, StoreError> {
        match self.head.get(scripthash)? {
            Some(v) if !v.is_empty() => Ok(Fk(1)), // non-null sentinel
            _ => Ok(Fk::NULL),
        }
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
    /// Returns `(written_count, timing)`.
    ///
    /// Creates for the **same scripthash** are applied in one body write (after
    /// sorting). Head upserts are applied **per shard**; when `recs.len()` is
    /// ≥ [`Self::LARGE_BATCH_ROWS`], each shard is flushed before the next so a
    /// multi-million-row materialize does not keep every head shard dirty.
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

    /// Cold bulk load of **scripthash-sorted** creates (migration-style).
    ///
    /// When the table is empty: no per-key head probes, bump-only slab alloc,
    /// batched `insert_many` for heads, **one** alloc-header write at the end.
    /// When non-empty: falls back to [`Self::put_create_batch_append`].
    ///
    /// `recs` should be sorted by `scripthash` (as sorted-run materialize produces).
    pub fn bulk_load_sorted_creates(
        &self,
        recs: &[ScriptHashRecord],
    ) -> Result<usize, StoreError> {
        if recs.is_empty() {
            return Ok(0);
        }
        if self.entry_count() > 0 {
            let mut heads = HashMap::new();
            let (n, _) = self.put_create_batch_append(recs, &mut heads)?;
            return Ok(n);
        }

        // Count unique keys for head reserve.
        let mut n_keys = 0u64;
        {
            let mut prev: Option<[u8; 32]> = None;
            for r in recs {
                if r.create_tx_fk.is_null() {
                    continue;
                }
                if prev != Some(r.scripthash) {
                    n_keys = n_keys.saturating_add(1);
                    prev = Some(r.scripthash);
                }
            }
        }
        if n_keys > 0 {
            self.head.reserve_additional(n_keys)?;
        }

        const HEAD_FLUSH: usize = 65_536;
        let mut head_buf: Vec<([u8; 32], ShHeadValue)> = Vec::with_capacity(HEAD_FLUSH);
        let mut written = 0usize;
        let mut alloc = self.alloc.lock().unwrap();

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
            let n = live.len() as u32;
            written = written.saturating_add(live.len());
            alloc.live_count = alloc.live_count.saturating_add(u64::from(n));

            let val = if n <= SH_INLINE_CAP as u32 {
                if n == 1 {
                    ShHeadValue::inline_one(live[0])
                } else {
                    ShHeadValue::inline_two(live[0], live[1])
                }
            } else {
                let class = class_for_count(n).ok_or(StoreError::Corrupt(
                    "scripthash bulk: entry count exceeds max slab class",
                ))?;
                let need = slab_bytes(class);
                let off = alloc.bump;
                alloc.bump = alloc.bump.saturating_add(need);
                self.body.ensure_capacity(alloc.bump)?;
                if alloc.bump > self.body.logical_len() {
                    self.body.set_logical_len(alloc.bump)?;
                }
                let mut blob = Vec::with_capacity(need as usize);
                for e in &live {
                    blob.extend_from_slice(&e.encode());
                }
                blob.resize(need as usize, 0);
                self.body.write_at(off, &blob)?;
                ShHeadValue::Slab {
                    class,
                    used: n,
                    slab_off: off,
                }
            };
            head_buf.push((key, val));
            if head_buf.len() >= HEAD_FLUSH {
                self.head.insert_many_sharded(&head_buf, true)?;
                head_buf.clear();
            }
        }
        if !head_buf.is_empty() {
            self.head.insert_many_sharded(&head_buf, true)?;
        }
        write_alloc_header(&self.body, &alloc)?;
        drop(alloc);
        Ok(written)
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
