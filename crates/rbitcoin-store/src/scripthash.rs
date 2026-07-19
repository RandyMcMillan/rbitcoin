//! Class B scripthash multimap (Electrum: SHA256(scriptPubKey)).
//!
//! Thin outpoint pointers only: head key = scripthash; body =
//! `create_tx_fk | vout | next`. Spend state and heights come from Class A/B/C
//! at query time (points + `is_confirmed_strong` / `tx_height`).

use crate::error::StoreError;
use crate::file::{TableFile, FILE_HEADER_LEN};
use crate::sharded_hashhead::ShardedHashHead;
use bitcoin_hashes::{sha256, Hash};
use rbitcoin_primitives::{Fk, TableKind};
use std::collections::HashMap;

/// Electrum scripthash = SHA256(scriptPubKey) (binary; API often reverses for hex).
pub fn script_hash(script: &[u8]) -> [u8; 32] {
    sha256::Hash::hash(script).to_byte_array()
}

/// Fixed scripthash body: create_tx_fk u64 | vout u32 | next u64 = 20 bytes.
///
/// Scripthash is the hash-head key only (not duplicated in the body).
/// `create_tx_fk == 0` is a tombstone (unlinked on disconnect).
pub const SCRIPTHASH_RECORD_LEN: usize = 20;

/// In-memory row. `scripthash` is filled from the head key when walking entries;
/// `txid` / `value` / `create_height` are query joins (not stored).
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
    fn encode_body(&self) -> [u8; SCRIPTHASH_RECORD_LEN] {
        let mut out = [0u8; SCRIPTHASH_RECORD_LEN];
        out[0..8].copy_from_slice(&self.create_tx_fk.0.to_le_bytes());
        out[8..12].copy_from_slice(&self.vout.to_le_bytes());
        out[12..20].copy_from_slice(&self.next.0.to_le_bytes());
        out
    }

    fn decode_body(buf: &[u8], scripthash: [u8; 32]) -> Result<Self, StoreError> {
        if buf.len() < SCRIPTHASH_RECORD_LEN {
            return Err(StoreError::Corrupt("short scripthash record"));
        }
        Ok(Self {
            scripthash,
            create_tx_fk: Fk(u64::from_le_bytes(buf[0..8].try_into().unwrap())),
            vout: u32::from_le_bytes(buf[8..12].try_into().unwrap()),
            next: Fk(u64::from_le_bytes(buf[12..20].try_into().unwrap())),
            txid: [0u8; 32],
            value: 0,
            create_height: 0,
        })
    }

    pub fn is_tombstone(&self) -> bool {
        self.create_tx_fk.is_null()
    }
}

/// Timing breakdown for one [`ScriptHashTable::put_create_batch_append`] (nanoseconds).
#[derive(Clone, Copy, Debug, Default)]
pub struct AppendTiming {
    /// Sort create records by scripthash.
    pub sort_ns: u64,
    /// Unique-key head probes (may be parallel).
    pub seed_ns: u64,
    /// Encode rows + body `write_at`.
    pub body_ns: u64,
    /// Durable `scripthash.head` insert_many.
    pub head_ns: u64,
}

pub struct ScriptHashTable {
    body: TableFile,
    head: ShardedHashHead,
    count: std::sync::Mutex<u64>,
}

impl ScriptHashTable {
    pub fn create(dir: &std::path::Path) -> Result<Self, StoreError> {
        Ok(Self {
            body: TableFile::create(dir.join("scripthash.body"), TableKind::ScriptHash)?,
            head: ShardedHashHead::create_for_role(
                dir.join("scripthash.head"),
                crate::hashhead::HeadRole::ScriptHash,
            )?,
            count: std::sync::Mutex::new(0),
        })
    }

    pub fn open(dir: &std::path::Path) -> Result<Self, StoreError> {
        let body = TableFile::open(dir.join("scripthash.body"), TableKind::ScriptHash)?;
        let head = ShardedHashHead::open_for_role(
            dir.join("scripthash.head"),
            crate::hashhead::HeadRole::ScriptHash,
        )?;
        let body_len = body.logical_len().saturating_sub(FILE_HEADER_LEN as u64);
        if body_len % SCRIPTHASH_RECORD_LEN as u64 != 0 {
            return Err(StoreError::Corrupt(
                "scripthash body size (expected 20-byte rows; reindex if upgrading)",
            ));
        }
        let count = body_len / SCRIPTHASH_RECORD_LEN as u64;
        Ok(Self {
            body,
            head,
            count: std::sync::Mutex::new(count),
        })
    }

    pub fn entry_count(&self) -> u64 {
        *self.count.lock().unwrap()
    }

    /// Sequential scan of the body for live (non-tombstone) create txs.
    ///
    /// Used once after open to warm process-local idempotency sets without
    /// walking hash chains (kill-safe re-confirm without O(chain) probes).
    /// Invokes `f(create_tx_fk, vout)` for each live row in body order.
    pub fn for_each_live_create(
        &self,
        mut f: impl FnMut(Fk, u32),
    ) -> Result<(), StoreError> {
        let n = self.entry_count();
        if n == 0 {
            return Ok(());
        }
        // One range read when small enough; else chunk to limit peak alloc.
        const ROW: usize = SCRIPTHASH_RECORD_LEN;
        const CHUNK_ROWS: usize = 64 * 1024; // 1.25 MiB
        let mut id = 1u64;
        while id <= n {
            let rows = ((n - id + 1) as usize).min(CHUNK_ROWS);
            let offset = FILE_HEADER_LEN as u64 + (id - 1) * ROW as u64;
            let mut buf = vec![0u8; rows * ROW];
            self.body.read_at(offset, &mut buf)?;
            for i in 0..rows {
                let rec = ScriptHashRecord::decode_body(
                    &buf[i * ROW..(i + 1) * ROW],
                    [0u8; 32],
                )?;
                if !rec.is_tombstone() {
                    f(rec.create_tx_fk, rec.vout);
                }
            }
            id += rows as u64;
        }
        Ok(())
    }

    fn read_fk(&self, fk: Fk, scripthash: [u8; 32]) -> Result<ScriptHashRecord, StoreError> {
        let id = fk.get().ok_or(StoreError::InvalidFk)?;
        let n = *self.count.lock().unwrap();
        if id == 0 || id > n {
            return Err(StoreError::NotFound);
        }
        let offset = FILE_HEADER_LEN as u64 + (id - 1) * SCRIPTHASH_RECORD_LEN as u64;
        let mut buf = [0u8; SCRIPTHASH_RECORD_LEN];
        self.body.read_at(offset, &mut buf)?;
        ScriptHashRecord::decode_body(&buf, scripthash)
    }

    fn write_fk(&self, fk: Fk, rec: &ScriptHashRecord) -> Result<(), StoreError> {
        let id = fk.get().ok_or(StoreError::InvalidFk)?;
        let offset = FILE_HEADER_LEN as u64 + (id - 1) * SCRIPTHASH_RECORD_LEN as u64;
        self.body.write_at(offset, &rec.encode_body())
    }

    /// Live (non-tombstone) create outpoints for a scripthash (newest-first).
    pub fn entries(&self, scripthash: &[u8; 32]) -> Result<Vec<(Fk, ScriptHashRecord)>, StoreError> {
        let mut out = Vec::new();
        let mut cur = self.head.get(scripthash)?;
        let sh = *scripthash;
        while let Some(fk) = cur {
            let rec = self.read_fk(fk, sh)?;
            let next = if rec.next.is_null() {
                None
            } else {
                Some(rec.next)
            };
            if !rec.is_tombstone() {
                out.push((fk, rec));
            }
            cur = next;
        }
        Ok(out)
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

    /// First live head fk for chaining (skips tombstones at head).
    pub fn live_head(&self, scripthash: &[u8; 32]) -> Result<Fk, StoreError> {
        let mut cur = self.head.get(scripthash)?;
        let sh = *scripthash;
        while let Some(fk) = cur {
            let rec = self.read_fk(fk, sh)?;
            if !rec.is_tombstone() {
                return Ok(fk);
            }
            cur = if rec.next.is_null() {
                None
            } else {
                Some(rec.next)
            };
        }
        Ok(Fk::NULL)
    }

    /// Append a create outpoint (idempotent on create_tx_fk+vout).
    pub fn put_create(&self, rec: &ScriptHashRecord) -> Result<Fk, StoreError> {
        let key = rec.scripthash;
        if rec.create_tx_fk.is_null() {
            return Err(StoreError::InvalidFk);
        }
        if self.contains_create(&key, rec.create_tx_fk, rec.vout)? {
            for (fk, e) in self.entries(&key)? {
                if e.create_tx_fk == rec.create_tx_fk && e.vout == rec.vout {
                    return Ok(fk);
                }
            }
        }
        let prev_head = self.live_head(&key)?;
        let mut count = self.count.lock().unwrap();
        let fk = Fk(*count + 1);
        let stored = ScriptHashRecord {
            scripthash: key,
            create_tx_fk: rec.create_tx_fk,
            vout: rec.vout,
            next: prev_head,
            txid: [0u8; 32],
            value: 0,
            create_height: 0,
        };
        let offset = FILE_HEADER_LEN as u64 + (*count) * SCRIPTHASH_RECORD_LEN as u64;
        self.body.write_at(offset, &stored.encode_body())?;
        *count += 1;
        drop(count);
        self.head.insert(&key, fk)?;
        Ok(fk)
    }

    /// Bulk append creates. Skips durable dups (walks each chain once). Returns fks (`NULL` if skipped).
    ///
    /// Prefer [`Self::put_create_batch_append`] on the sequential confirm hot path:
    /// full chain walks dominate Class C once popular scripts grow long.
    pub fn put_create_batch(&self, recs: &[ScriptHashRecord]) -> Result<Vec<Fk>, StoreError> {
        if recs.is_empty() {
            return Ok(Vec::new());
        }
        // Precompute durable heads and dups without holding count lock.
        let mut durable_head: HashMap<[u8; 32], Fk> = HashMap::new();
        let mut known_keys: HashMap<[u8; 32], Vec<(Fk, u32)>> = HashMap::new();
        for rec in recs {
            if !durable_head.contains_key(&rec.scripthash) {
                durable_head.insert(rec.scripthash, self.live_head(&rec.scripthash)?);
                let mut pairs = Vec::new();
                for (_fk, e) in self.entries(&rec.scripthash)? {
                    pairs.push((e.create_tx_fk, e.vout));
                }
                known_keys.insert(rec.scripthash, pairs);
            }
        }

        let mut out = Vec::with_capacity(recs.len());
        let mut local_heads = durable_head;
        let mut body_blob = Vec::new();
        let mut head_final: HashMap<[u8; 32], Fk> = HashMap::new();
        let mut batch_seen: HashMap<[u8; 32], Vec<(Fk, u32)>> = HashMap::new();

        let mut count = self.count.lock().unwrap();
        let start = *count;

        for rec in recs {
            if rec.create_tx_fk.is_null() {
                out.push(Fk::NULL);
                continue;
            }
            let key = rec.scripthash;
            let durable = known_keys.get(&key).map(|v| v.as_slice()).unwrap_or(&[]);
            let seen = batch_seen.entry(key).or_default();
            if durable
                .iter()
                .any(|&(c, v)| c == rec.create_tx_fk && v == rec.vout)
                || seen
                    .iter()
                    .any(|&(c, v)| c == rec.create_tx_fk && v == rec.vout)
            {
                out.push(Fk::NULL);
                continue;
            }
            let prev = local_heads.get(&key).copied().unwrap_or(Fk::NULL);
            let fk = Fk(*count + 1);
            let stored = ScriptHashRecord {
                scripthash: key,
                create_tx_fk: rec.create_tx_fk,
                vout: rec.vout,
                next: prev,
                txid: [0u8; 32],
                value: 0,
                create_height: 0,
            };
            body_blob.extend_from_slice(&stored.encode_body());
            local_heads.insert(key, fk);
            head_final.insert(key, fk);
            seen.push((rec.create_tx_fk, rec.vout));
            out.push(fk);
            *count += 1;
        }
        drop(count);

        if !body_blob.is_empty() {
            let offset = FILE_HEADER_LEN as u64 + start * SCRIPTHASH_RECORD_LEN as u64;
            self.body.write_at(offset, &body_blob)?;
            let pairs: Vec<([u8; 32], Fk)> = head_final.into_iter().collect();
            // Pre-size head for new keys so insert_many rarely mid-batch rehashes.
            self.head.reserve_additional(pairs.len() as u64)?;
            self.head.insert_many(&pairs)?;
        }
        Ok(out)
    }

    /// Forward-append creates for sequential confirm (no durable chain walk).
    ///
    /// `heads` is a process-local map of scripthash → current head body fk. Missing
    /// keys are seeded with one [`Self::live_head`] (hash probe only). In-batch and
    /// process-local dups are skipped via `batch_seen` only — callers must skip
    /// already-indexed create txs (see Query confirm cache).
    ///
    /// Updates `heads` to the new heads for written keys.
    /// Append thin creates using a process-local head map (no full chain walks).
    ///
    /// Direct head writes (no write-behind overlay): one sorted `insert_many` per
    /// batch after a single body append. Returns per-step timings for IBD logs.
    pub fn put_create_batch_append(
        &self,
        recs: &[ScriptHashRecord],
        heads: &mut HashMap<[u8; 32], Fk>,
    ) -> Result<(Vec<Fk>, AppendTiming), StoreError> {
        let mut timing = AppendTiming::default();
        if recs.is_empty() {
            return Ok((Vec::new(), timing));
        }

        // Sort by scripthash for head insert locality (shard/slot clustering).
        let t_sort = std::time::Instant::now();
        let mut order: Vec<usize> = (0..recs.len()).collect();
        order.sort_by(|&a, &b| recs[a].scripthash.cmp(&recs[b].scripthash));
        timing.sort_ns = t_sort.elapsed().as_nanos() as u64;

        // Seed missing heads: unique keys not already in process map.
        // One hash-head probe each (`head.get` only — no body tombstone walk).
        // Parallel when many misses (cold mmap probes dominate IBD Class C).
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
        if !missing.is_empty() {
            // Parallel head.get over unique missing keys (shards are independent).
            const PAR_THRESH: usize = 64;
            const N_WORKERS: usize = 4;
            let seeded = if missing.len() >= PAR_THRESH {
                let chunk = missing.len().div_ceil(N_WORKERS).max(1);
                let mut pairs: Vec<([u8; 32], Fk)> = Vec::with_capacity(missing.len());
                let mut par_err: Option<StoreError> = None;
                std::thread::scope(|scope| {
                    let mut handles = Vec::new();
                    for c in missing.chunks(chunk) {
                        let c = c.to_vec();
                        handles.push(scope.spawn(|| -> Result<Vec<([u8; 32], Fk)>, StoreError> {
                            let mut out = Vec::with_capacity(c.len());
                            for key in c {
                                let prev = self.head.get(&key)?.unwrap_or(Fk::NULL);
                                out.push((key, prev));
                            }
                            Ok(out)
                        }));
                    }
                    for h in handles {
                        match h.join() {
                            Ok(Ok(part)) => pairs.extend(part),
                            Ok(Err(e)) => {
                                par_err = Some(e);
                            }
                            Err(_) => {
                                par_err =
                                    Some(StoreError::Corrupt("scripthash seed worker panicked"));
                            }
                        }
                    }
                });
                if let Some(e) = par_err {
                    return Err(e);
                }
                pairs
            } else {
                let mut pairs = Vec::with_capacity(missing.len());
                for key in &missing {
                    let prev = self.head.get(key)?.unwrap_or(Fk::NULL);
                    pairs.push((*key, prev));
                }
                pairs
            };
            for (key, prev) in seeded {
                heads.insert(key, prev);
            }
        }
        timing.seed_ns = t_seed.elapsed().as_nanos() as u64;

        let mut out = vec![Fk::NULL; recs.len()];
        let mut body_blob = Vec::new();
        let mut head_final: HashMap<[u8; 32], Fk> = HashMap::new();
        let mut batch_seen: HashMap<[u8; 32], Vec<(Fk, u32)>> = HashMap::new();

        let t_body = std::time::Instant::now();
        let mut count = self.count.lock().unwrap();
        let start = *count;

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
            let prev = heads.get(&key).copied().unwrap_or(Fk::NULL);
            let fk = Fk(*count + 1);
            let stored = ScriptHashRecord {
                scripthash: key,
                create_tx_fk: rec.create_tx_fk,
                vout: rec.vout,
                next: prev,
                txid: [0u8; 32],
                value: 0,
                create_height: 0,
            };
            body_blob.extend_from_slice(&stored.encode_body());
            heads.insert(key, fk);
            head_final.insert(key, fk);
            seen.push((rec.create_tx_fk, rec.vout));
            out[i] = fk;
            *count += 1;
        }
        drop(count);

        if !body_blob.is_empty() {
            let offset = FILE_HEADER_LEN as u64 + start * SCRIPTHASH_RECORD_LEN as u64;
            self.body.write_at(offset, &body_blob)?;
        }
        timing.body_ns = t_body.elapsed().as_nanos() as u64;

        if !head_final.is_empty() {
            let t_head = std::time::Instant::now();
            let mut pairs: Vec<([u8; 32], Fk)> = head_final.into_iter().collect();
            pairs.sort_by(|a, b| a.0.cmp(&b.0));
            self.head.reserve_additional(pairs.len() as u64)?;
            self.head.insert_many(&pairs)?;
            timing.head_ns = t_head.elapsed().as_nanos() as u64;
        }
        Ok((out, timing))
    }

    /// Unlink one create (disconnect tip). Tombstones body; updates chain links.
    pub fn unlink_create(
        &self,
        scripthash: &[u8; 32],
        create_tx_fk: Fk,
        vout: u32,
    ) -> Result<bool, StoreError> {
        let sh = *scripthash;
        let mut cur = self.head.get(scripthash)?;
        let mut walker_prev: Option<Fk> = None;
        while let Some(fk) = cur {
            let rec = self.read_fk(fk, sh)?;
            let next = rec.next;
            if !rec.is_tombstone() && rec.create_tx_fk == create_tx_fk && rec.vout == vout {
                // Tombstone this row; rewire predecessor or head.
                let mut dead = rec;
                dead.create_tx_fk = Fk::NULL;
                // Keep next so walk continues past tombstone.
                self.write_fk(fk, &dead)?;
                if let Some(p) = walker_prev {
                    let mut prev = self.read_fk(p, sh)?;
                    // Only rewire if prev still points at fk (tombstones keep next).
                    if prev.next == fk {
                        prev.next = next;
                        self.write_fk(p, &prev)?;
                    }
                } else {
                    // Was head (possibly after skipping tombstones at head).
                    // Point head at next live-or-tombstone successor.
                    if !next.is_null() {
                        self.head.insert(scripthash, next)?;
                    }
                    // If next is null, leave head pointing at tombstone; entries skip it.
                }
                return Ok(true);
            }
            if !rec.is_tombstone() {
                walker_prev = Some(fk);
            }
            cur = if next.is_null() { None } else { Some(next) };
        }
        Ok(false)
    }

    pub fn flush(&self) -> Result<(), StoreError> {
        self.body.flush()?;
        self.head.flush()?;
        Ok(())
    }
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
    fn put_create_batch_chains() {
        let dir = tmp();
        let t = ScriptHashTable::create(&dir).unwrap();
        let sh = script_hash(&[0x51]);
        let recs: Vec<_> = (0..3u32).map(|v| rec(sh, u64::from(v) + 1, v)).collect();
        let fks = t.put_create_batch(&recs).unwrap();
        assert_eq!(fks.iter().filter(|f| !f.is_null()).count(), 3);
        assert_eq!(t.entries(&sh).unwrap().len(), 3);
        // Batch dups + durable dups skipped
        let fks2 = t.put_create_batch(&recs).unwrap();
        assert!(fks2.iter().all(|f| f.is_null()));
        assert_eq!(t.entries(&sh).unwrap().len(), 3);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn put_create_batch_append_uses_heads_no_dup_walk() {
        let dir = tmp();
        let t = ScriptHashTable::create(&dir).unwrap();
        let sh = script_hash(&[0x51]);
        let mut heads = HashMap::new();
        let recs: Vec<_> = (0..3u32).map(|v| rec(sh, u64::from(v) + 1, v)).collect();
        let (fks, _t) = t.put_create_batch_append(&recs, &mut heads).unwrap();
        assert_eq!(fks.iter().filter(|f| !f.is_null()).count(), 3);
        assert_eq!(t.entries(&sh).unwrap().len(), 3);
        assert_eq!(heads.get(&sh).copied().unwrap(), fks[2]);
        // In-batch dup skipped; cross-batch append without process skip would re-add
        // (caller must track indexed txs). Second batch of new vouts chains.
        let more = vec![rec(sh, 10, 9)];
        let (fks2, _t2) = t.put_create_batch_append(&more, &mut heads).unwrap();
        assert!(!fks2[0].is_null());
        assert_eq!(t.entries(&sh).unwrap().len(), 4);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn for_each_live_create_sequential_skips_tombstones() {
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
        // Kill-safe warm: load create_tx set, skip those on re-append.
        let mut indexed: std::collections::HashSet<u64> =
            seen.iter().map(|(c, _)| *c).collect();
        let mut heads2 = HashMap::new();
        let again = [rec(sh, 1, 0), rec(sh, 3, 0), rec(sh, 4, 0)];
        let to_put: Vec<_> = again
            .into_iter()
            .filter(|r| !indexed.contains(&r.create_tx_fk.0))
            .collect();
        assert_eq!(to_put.len(), 1);
        t.put_create_batch_append(&to_put, &mut heads2).unwrap();
        assert_eq!(t.entries(&sh).unwrap().len(), 3); // 1,3 live + 4 (2 tombstoned)
        indexed.insert(4);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
