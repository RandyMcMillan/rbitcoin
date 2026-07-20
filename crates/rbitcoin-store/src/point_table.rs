use crate::error::StoreError;
use crate::file::{TableFile, FILE_HEADER_LEN};
use crate::sharded_hashhead::ShardedHashHead;
use bitcoin_hashes::{sha256, Hash};
use rbitcoin_primitives::{Fk, TableKind};
use std::collections::HashMap;

/// Class B point multimap entry (schema v3: edge only — outpoint is head key).
///
/// Body = spending_tx_fk (8) + spending_input_index (4) + next (8) = 20 bytes.
/// Outpoint recovered from the query key when walking spenders.
pub const POINT_RECORD_LEN: usize = 20;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PointRecord {
    /// Filled from query args when walking `spenders` (not stored in body).
    pub out_txid: [u8; 32],
    pub out_index: u32,
    pub spending_tx_fk: Fk,
    pub spending_input_index: u32,
    pub next: Fk,
}

impl PointRecord {
    fn encode_body(&self) -> [u8; POINT_RECORD_LEN] {
        let mut out = [0u8; POINT_RECORD_LEN];
        out[0..8].copy_from_slice(&self.spending_tx_fk.0.to_le_bytes());
        out[8..12].copy_from_slice(&self.spending_input_index.to_le_bytes());
        out[12..20].copy_from_slice(&self.next.0.to_le_bytes());
        out
    }

    fn decode_body(buf: &[u8]) -> Result<(Fk, u32, Fk), StoreError> {
        if buf.len() < POINT_RECORD_LEN {
            return Err(StoreError::Corrupt("short point record"));
        }
        Ok((
            Fk(u64::from_le_bytes(buf[0..8].try_into().unwrap())),
            u32::from_le_bytes(buf[8..12].try_into().unwrap()),
            Fk(u64::from_le_bytes(buf[12..20].try_into().unwrap())),
        ))
    }

    /// Non-lossy outpoint key: SHA256(txid || vout_le).
    pub fn outpoint_key(out_txid: &[u8; 32], out_index: u32) -> [u8; 32] {
        let mut buf = [0u8; 36];
        buf[0..32].copy_from_slice(out_txid);
        buf[32..36].copy_from_slice(&out_index.to_le_bytes());
        sha256::Hash::hash(&buf).to_byte_array()
    }
}

pub struct PointTable {
    body: TableFile,
    head: ShardedHashHead,
    count: std::sync::Mutex<u64>,
}

impl PointTable {
    pub fn create(dir: &std::path::Path) -> Result<Self, StoreError> {
        Ok(Self {
            body: TableFile::create(dir.join("point.body"), TableKind::Point)?,
            head: ShardedHashHead::create_for_role(
                dir.join("point.head"),
                crate::hashhead::HeadRole::Point,
            )?,
            count: std::sync::Mutex::new(0),
        })
    }

    pub fn open(dir: &std::path::Path) -> Result<Self, StoreError> {
        let body = TableFile::open(dir.join("point.body"), TableKind::Point)?;
        let head = ShardedHashHead::open_for_role(
            dir.join("point.head"),
            crate::hashhead::HeadRole::Point,
        )?;
        let body_len = body.logical_len().saturating_sub(FILE_HEADER_LEN as u64);
        if body_len % POINT_RECORD_LEN as u64 != 0 {
            return Err(StoreError::Corrupt("point body size"));
        }
        let count = body_len / POINT_RECORD_LEN as u64;
        Ok(Self {
            body,
            head,
            count: std::sync::Mutex::new(count),
        })
    }

    /// Number of durable point (spend) edges on disk.
    pub fn edge_count(&self) -> u64 {
        *self.count.lock().unwrap()
    }

    /// Append a spend index entry. Chains onto existing head for the outpoint key.
    pub fn put_spend(
        &self,
        out_txid: &[u8; 32],
        out_index: u32,
        spending_tx_fk: Fk,
        spending_input_index: u32,
    ) -> Result<Fk, StoreError> {
        let fks = self.put_spend_batch(&[(
            *out_txid,
            out_index,
            spending_tx_fk,
            spending_input_index,
        )])?;
        Ok(fks[0])
    }

    /// Bulk append spend edges (one body write + batched head inserts).
    ///
    /// Each edge is `(out_txid, out_index, spending_tx_fk, spending_input_index)`.
    /// Multiple spends of the same outpoint in one batch chain correctly; only
    /// the final head key per outpoint is written to the hash head (via
    /// [`HashHead::insert_many`]).
    ///
    /// Returns the assigned point fks in input order.
    pub fn put_spend_batch(
        &self,
        edges: &[([u8; 32], u32, Fk, u32)],
    ) -> Result<Vec<Fk>, StoreError> {
        if edges.is_empty() {
            return Ok(Vec::new());
        }
        for (_, _, spending_tx_fk, _) in edges {
            if spending_tx_fk.is_null() {
                return Err(StoreError::InvalidFk);
            }
        }

        // Local head for keys we already appended in this batch (so chaining works
        // without a round-trip to the hash head for each edge of the same outpoint).
        let mut local_heads: HashMap<[u8; 32], Fk> = HashMap::new();
        let mut body = Vec::with_capacity(edges.len() * POINT_RECORD_LEN);
        let mut fks = Vec::with_capacity(edges.len());

        let mut count = self.count.lock().unwrap();
        let start = *count;
        let start_offset = FILE_HEADER_LEN as u64 + start * POINT_RECORD_LEN as u64;

        for &(out_txid, out_index, spending_tx_fk, spending_input_index) in edges {
            let key = PointRecord::outpoint_key(&out_txid, out_index);
            let prev_head = if let Some(&h) = local_heads.get(&key) {
                h
            } else {
                self.head.get(&key)?.unwrap_or(Fk::NULL)
            };
            let fk = Fk(*count + 1);
            let rec = PointRecord {
                out_txid,
                out_index,
                spending_tx_fk,
                spending_input_index,
                next: prev_head,
            };
            body.extend_from_slice(&rec.encode_body());
            local_heads.insert(key, fk);
            fks.push(fk);
            *count += 1;
        }
        let end_count = *count;
        drop(count);

        self.body.write_at(start_offset, &body)?;
        // Final head per outpoint only (body chain already links older edges).
        let head_batch: Vec<([u8; 32], Fk)> = local_heads.into_iter().collect();
        // Paced: materialize / large batches must not rehash many shards at once.
        self.head.insert_many_paced(&head_batch)?;
        debug_assert_eq!(end_count, start + edges.len() as u64);
        Ok(fks)
    }

    pub fn get(&self, fk: Fk) -> Result<(Fk, u32, Fk), StoreError> {
        let id = fk.get().ok_or(StoreError::InvalidFk)?;
        let count = *self.count.lock().unwrap();
        if id == 0 || id > count {
            return Err(StoreError::NotFound);
        }
        let offset = FILE_HEADER_LEN as u64 + (id - 1) * POINT_RECORD_LEN as u64;
        let mut buf = [0u8; POINT_RECORD_LEN];
        self.body.read_at(offset, &mut buf)?;
        PointRecord::decode_body(&buf)
    }

    /// Walk spend edges for an outpoint without allocating a `Vec`.
    ///
    /// `visit` receives `(spending_tx_fk, spending_input_index)`. Return `Ok(false)`
    /// to stop early, `Ok(true)` to continue.
    pub fn for_each_spender<F>(
        &self,
        out_txid: &[u8; 32],
        out_index: u32,
        visit: F,
    ) -> Result<(), StoreError>
    where
        F: FnMut(Fk, u32) -> Result<bool, StoreError>,
    {
        let key = PointRecord::outpoint_key(out_txid, out_index);
        self.for_each_spender_key(&key, visit)
    }

    /// Walk by precomputed outpoint key (wave_fill batch-sorted probes).
    pub fn for_each_spender_key<F>(
        &self,
        outpoint_key: &[u8; 32],
        mut visit: F,
    ) -> Result<(), StoreError>
    where
        F: FnMut(Fk, u32) -> Result<bool, StoreError>,
    {
        let mut cur = self.head.get(outpoint_key)?;
        while let Some(fk) = cur {
            let (spending_tx_fk, spending_input_index, next) = self.get(fk)?;
            if !visit(spending_tx_fk, spending_input_index)? {
                return Ok(());
            }
            cur = if next.is_null() { None } else { Some(next) };
        }
        Ok(())
    }

    /// Collect all spenders of an outpoint (may be empty). Outpoint fields filled from args.
    pub fn spenders(
        &self,
        out_txid: &[u8; 32],
        out_index: u32,
    ) -> Result<Vec<PointRecord>, StoreError> {
        let mut out = Vec::new();
        self.for_each_spender(out_txid, out_index, |spending_tx_fk, spending_input_index| {
            out.push(PointRecord {
                out_txid: *out_txid,
                out_index,
                spending_tx_fk,
                spending_input_index,
                next: Fk::NULL, // not needed by callers; chain already walked
            });
            Ok(true)
        })?;
        Ok(out)
    }

    /// Enable process-local write-behind on `point.head` (IBD full-validation path).
    ///
    /// Upserts buffer in RAM and spill sorted/page-buffered when the cap is hit
    /// or on [`Self::flush`] / [`Self::spill_head`]. `get`/`spenders` stay coherent.
    pub fn enable_head_write_behind(&self, max_entries: usize) -> Result<(), StoreError> {
        self.head.enable_write_behind(max_entries)
    }

    /// Disable write-behind after spilling pending head updates.
    pub fn disable_head_write_behind(&self) -> Result<(), StoreError> {
        self.head.disable_write_behind()
    }

    /// Spill pending `point.head` write-behind entries without fsync (chunked).
    pub fn spill_head(&self) -> Result<(), StoreError> {
        self.head.spill_write_behind()
    }

    /// Single-apply spill for process exit.
    pub fn spill_head_fast(&self) -> Result<(), StoreError> {
        self.head.spill_write_behind_fast()
    }

    /// Budgeted spill: at most `max_entries` keys (archive interleave / background).
    pub fn spill_head_budget(&self, max_entries: usize) -> Result<usize, StoreError> {
        self.head.spill_write_behind_budget(max_entries)
    }

    /// One short-slice step when the overlay needs draining.
    pub fn spill_head_step_if_needed(&self) -> Result<usize, StoreError> {
        self.head.spill_write_behind_step_if_needed()
    }

    pub fn head_write_behind_len(&self) -> usize {
        self.head.write_behind_len()
    }

    /// Defer soft-cap point.head spills during confirm (probe from RAM overlay).
    /// Clearing defer does not bulk-spill — background / archive steps drain.
    pub fn set_head_defer_spill(&self, defer: bool) -> Result<(), StoreError> {
        self.head.set_defer_spill(defer)
    }

    pub fn flush(&self) -> Result<(), StoreError> {
        self.body.flush()?;
        self.head.flush()?;
        Ok(())
    }

    pub fn flush_async(&self) -> Result<(), StoreError> {
        self.body.flush_async()?;
        self.head.flush_async()?;
        Ok(())
    }

    /// Body + head files MS_ASYNC after a prior fast spill (no second spill storm).
    pub fn flush_async_no_spill(&self) -> Result<(), StoreError> {
        self.body.flush_async()?;
        self.head.flush_async_no_spill()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp() -> std::path::PathBuf {
        let p = std::env::temp_dir().join(format!(
            "rbitcoin-point-{}-{}",
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
    fn point_edge_roundtrip_and_spenders() {
        let dir = tmp();
        let t = PointTable::create(&dir).unwrap();
        let txid = [9u8; 32];
        let fk = t.put_spend(&txid, 1, Fk(10), 0).unwrap();
        assert_eq!(fk, Fk(1));
        let spenders = t.spenders(&txid, 1).unwrap();
        assert_eq!(spenders.len(), 1);
        assert_eq!(spenders[0].out_txid, txid);
        assert_eq!(spenders[0].out_index, 1);
        assert_eq!(spenders[0].spending_tx_fk, Fk(10));
        // Second spend chains
        t.put_spend(&txid, 1, Fk(11), 2).unwrap();
        assert_eq!(t.spenders(&txid, 1).unwrap().len(), 2);
        assert!(t.spenders(&[0u8; 32], 0).unwrap().is_empty());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn put_spend_batch_chains_same_outpoint() {
        let dir = tmp();
        let t = PointTable::create(&dir).unwrap();
        let txid = [3u8; 32];
        let fks = t
            .put_spend_batch(&[
                (txid, 0, Fk(100), 0),
                (txid, 0, Fk(101), 1),
                ([4u8; 32], 2, Fk(102), 0),
            ])
            .unwrap();
        assert_eq!(fks.len(), 3);
        assert_eq!(t.edge_count(), 3);
        let s = t.spenders(&txid, 0).unwrap();
        assert_eq!(s.len(), 2);
        // Newest head first.
        assert_eq!(s[0].spending_tx_fk, Fk(101));
        assert_eq!(s[1].spending_tx_fk, Fk(100));
        assert_eq!(t.spenders(&[4u8; 32], 2).unwrap().len(), 1);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn for_each_spender_key_matches_txid_path() {
        let dir = std::env::temp_dir().join(format!(
            "rbitcoin-point-key-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let _ = std::fs::create_dir_all(&dir);
        let t = PointTable::create(&dir).unwrap();
        let txid = [7u8; 32];
        t.put_spend(&txid, 3, Fk(99), 1).unwrap();
        let key = PointRecord::outpoint_key(&txid, 3);
        let mut n = 0u32;
        t.for_each_spender_key(&key, |sp, idx| {
            assert_eq!(sp, Fk(99));
            assert_eq!(idx, 1);
            n += 1;
            Ok(true)
        })
        .unwrap();
        assert_eq!(n, 1);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn write_behind_head_keeps_spenders_coherent() {
        let dir = tmp();
        let t = PointTable::create(&dir).unwrap();
        t.enable_head_write_behind(10_000).unwrap();
        let txid = [5u8; 32];
        t.put_spend_batch(&[
            (txid, 0, Fk(1), 0),
            (txid, 0, Fk(2), 1),
            (txid, 1, Fk(3), 0),
        ])
        .unwrap();
        // Heads still in overlay; spenders must see them.
        assert_eq!(t.spenders(&txid, 0).unwrap().len(), 2);
        assert_eq!(t.spenders(&txid, 1).unwrap().len(), 1);
        t.spill_head().unwrap();
        assert_eq!(t.spenders(&txid, 0).unwrap()[0].spending_tx_fk, Fk(2));
        t.flush().unwrap();
        let t2 = PointTable::open(&dir).unwrap();
        assert_eq!(t2.spenders(&txid, 0).unwrap().len(), 2);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
