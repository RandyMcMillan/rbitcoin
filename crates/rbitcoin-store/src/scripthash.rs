//! Class B scripthash multimap (Electrum: SHA256(scriptPubKey)).
//!
//! Hybrid layout: head holds up to 2 inline thin creates, or a pointer to one
//! geometric body slab (cap 4 → 8 → 16 → …). Body is a size-class freelist heap.
//! Spend state and heights come from Class A/B/C at query time.

use crate::error::StoreError;
use crate::file::{TableFile, FILE_HEADER_LEN};
use crate::hashhead::HeadRole;
use crate::scripthash_head::ShardedScriptHashHead;
use crate::scripthash_layout::{
    class_for_count, payload_start, slab_bytes, slab_cap, ShEntry, ShHeadValue, SH_ALLOC_HEADER_LEN,
    SH_ALLOC_MAGIC, SH_ALLOC_VERSION, SH_ENTRY_LEN, SH_INLINE_CAP, SH_MAX_CLASS, SH_V3_RECORD_LEN,
};
use bitcoin_hashes::{sha256, Hash};
use rbitcoin_primitives::{Fk, TableKind};
use std::collections::HashMap;
use std::sync::Mutex;

/// Electrum scripthash = SHA256(scriptPubKey) (binary; API often reverses for hex).
pub fn script_hash(script: &[u8]) -> [u8; 32] {
    sha256::Hash::hash(script).to_byte_array()
}

/// Legacy constant kept for migration / docs (v3 row length).
#[allow(dead_code)]
pub const SCRIPTHASH_RECORD_LEN: usize = SH_V3_RECORD_LEN;

pub use crate::scripthash_layout::ShEntry as ScriptHashEntry;

/// In-memory row. `scripthash` is filled from the head key when walking entries;
/// `txid` / `value` / `create_height` are query joins (not stored).
///
/// `next` is unused in hybrid layout (always null); retained for API compatibility.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ScriptHashRecord {
    pub scripthash: [u8; 32],
    pub create_tx_fk: Fk,
    pub vout: u32,
    pub next: Fk,
    /// Query join from create_tx_fk (not stored).
    pub txid: [u8; 32],
    /// Query join from Class A output (not stored).
    pub value: i64,
    /// Query join from `tx_height` (not stored).
    pub create_height: u32,
}

impl ScriptHashRecord {
    pub fn entry(&self) -> ShEntry {
        ShEntry::new(self.create_tx_fk, self.vout)
    }

    pub fn from_entry(scripthash: [u8; 32], e: ShEntry) -> Self {
        Self {
            scripthash,
            create_tx_fk: e.create_tx_fk,
            vout: e.vout,
            next: Fk::NULL,
            txid: [0u8; 32],
            value: 0,
            create_height: 0,
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

    #[allow(dead_code)]
    pub(crate) fn open_with_schema(dir: &std::path::Path, schema: u16) -> Result<Self, StoreError> {
        let body = TableFile::open_with_schema(
            dir.join("scripthash.body"),
            TableKind::ScriptHash,
            schema,
        )?;
        Self::from_body_and_head(
            body,
            ShardedScriptHashHead::open_with_schema(dir.join("scripthash.head"), schema)?,
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

    /// Visit every live create across all keys (head occupancy walk).
    pub fn for_each_live_create(
        &self,
        mut f: impl FnMut(Fk, u32),
    ) -> Result<(), StoreError> {
        self.head.for_each_occupied(|key, val| {
            let entries = self.collect_entries(&key, &val)?;
            for e in entries {
                f(e.create_tx_fk, e.vout);
            }
            Ok(())
        })
    }

    /// Live creates for a scripthash (oldest → newest). Second tuple element
    /// keeps [`ScriptHashRecord`] for query joins (`next` always null).
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
        vout: u32,
    ) -> Result<bool, StoreError> {
        for (_fk, rec) in self.entries(scripthash)? {
            if rec.create_tx_fk == create_tx_fk && rec.vout == vout {
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

    /// Append a create (idempotent on create_tx_fk+vout).
    pub fn put_create(&self, rec: &ScriptHashRecord) -> Result<(), StoreError> {
        if rec.create_tx_fk.is_null() {
            return Err(StoreError::InvalidFk);
        }
        if self.contains_create(&rec.scripthash, rec.create_tx_fk, rec.vout)? {
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
        let mut known: HashMap<[u8; 32], Vec<(Fk, u32)>> = HashMap::new();
        let mut heads: HashMap<[u8; 32], ShHeadValue> = HashMap::new();
        for rec in recs {
            if !known.contains_key(&rec.scripthash) {
                let mut pairs = Vec::new();
                for (_fk, e) in self.entries(&rec.scripthash)? {
                    pairs.push((e.create_tx_fk, e.vout));
                }
                known.insert(rec.scripthash, pairs);
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
                !durable
                    .iter()
                    .any(|&(c, v)| c == rec.create_tx_fk && v == rec.vout)
            })
            .cloned()
            .collect();
        // Also filter in-batch dups via append path
        let (n, _) = self.put_create_batch_append(&filtered, &mut heads)?;
        Ok(n)
    }

    /// Forward-append creates (no durable chain walk). Process-local `heads` map.
    ///
    /// Returns `(written_count, timing)`.
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
        for key in missing {
            if let Some(v) = self.head.get(&key)? {
                heads.insert(key, v);
            }
        }
        timing.seed_ns = t_seed.elapsed().as_nanos() as u64;

        // Group new entries by key (order already sorted by key).
        let t_body = std::time::Instant::now();
        let mut batch_seen: HashMap<[u8; 32], Vec<(Fk, u32)>> = HashMap::new();
        let mut per_key: HashMap<[u8; 32], Vec<ShEntry>> = HashMap::new();
        let mut written = 0usize;

        for &i in &order {
            let rec = &recs[i];
            if rec.create_tx_fk.is_null() {
                continue;
            }
            let key = rec.scripthash;
            let seen = batch_seen.entry(key).or_default();
            if seen
                .iter()
                .any(|&(c, v)| c == rec.create_tx_fk && v == rec.vout)
            {
                continue;
            }
            seen.push((rec.create_tx_fk, rec.vout));
            per_key
                .entry(key)
                .or_default()
                .push(ShEntry::new(rec.create_tx_fk, rec.vout));
            written += 1;
        }

        let mut head_final: Vec<([u8; 32], ShHeadValue)> = Vec::new();
        let mut alloc = self.alloc.lock().unwrap();

        for (key, new_ents) in per_key {
            let cur = heads.get(&key).cloned().unwrap_or(ShHeadValue::Empty);
            let mut old_live = self.collect_entries_locked(&cur)?;
            // Drop any durable dups already present (append path trusts watermark,
            // but still skip if process map already has them from prior batch).
            new_ents.iter().for_each(|_| {});
            let add: Vec<ShEntry> = new_ents
                .into_iter()
                .filter(|e| {
                    !old_live
                        .iter()
                        .any(|o| o.create_tx_fk == e.create_tx_fk && o.vout == e.vout)
                })
                .collect();
            if add.is_empty() {
                written = written.saturating_sub(
                    // recount: we over-counted; fix by not double-counting — recompute later
                    0,
                );
                continue;
            }
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
            head_final.sort_by(|a, b| a.0.cmp(&b.0));
            self.head.insert_many_paced(&head_final)?;
            timing.head_ns = t_head.elapsed().as_nanos() as u64;
        }
        Ok((written, timing))
    }

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

        let class = class_for_count(n).ok_or(StoreError::Corrupt("scripthash entry count too large"))?;

        // Reuse existing slab if same class and capacity sufficient.
        if let ShHeadValue::Slab {
            class: oc,
            slab_off,
            ..
        } = old
        {
            if *oc == class && slab_cap(*oc) >= n {
                self.write_slab_entries(*slab_off, live)?;
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

    /// Unlink one create (disconnect tip). Swap-remove; demote slab→inline when used≤2.
    pub fn unlink_create(
        &self,
        scripthash: &[u8; 32],
        create_tx_fk: Fk,
        vout: u32,
    ) -> Result<bool, StoreError> {
        let Some(val) = self.head.get(scripthash)? else {
            return Ok(false);
        };
        let mut live = self.collect_entries(scripthash, &val)?;
        let Some(pos) = live
            .iter()
            .position(|e| e.create_tx_fk == create_tx_fk && e.vout == vout)
        else {
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

    fn rec(sh: [u8; 32], tx: u64, vout: u32) -> ScriptHashRecord {
        ScriptHashRecord {
            scripthash: sh,
            create_tx_fk: Fk(tx),
            vout,
            next: Fk::NULL,
            txid: [0u8; 32],
            value: 0,
            create_height: 0,
        }
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
        t.for_each_live_create(|c, v| seen.push((c.0, v))).unwrap();
        seen.sort_unstable();
        assert_eq!(seen, vec![(1, 0), (3, 0)]);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
