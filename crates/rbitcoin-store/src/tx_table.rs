use crate::compact::{
    input_flags, output_flags, read_compact_size, read_uleb128, write_compact_size, write_uleb128,
};
use crate::error::StoreError;
use crate::sharded_hashhead::ShardedHashHead;
use crate::var_table::VarTable;
use rbitcoin_primitives::{Fk, TableKind};
use std::path::Path;

/// Class A tx row (no wire blob — reconstruct from inputs/outputs + witness).
///
/// `input_start_fk` / `output_start_fk` address a **per-tx run** record (one idx
/// entry for all inputs/outputs of this tx), not a global per-I/O FK.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TxRecord {
    pub txid: [u8; 32],
    pub version: i32,
    pub locktime: u32,
    pub input_start_fk: Fk,
    pub input_count: u32,
    pub output_start_fk: Fk,
    pub output_count: u32,
}

impl TxRecord {
    /// Fixed payload size (unframed).
    pub const ENCODED_LEN: usize = 32 + 4 + 4 + 8 + 4 + 8 + 4;

    pub fn encode_into(&self, out: &mut Vec<u8>) {
        out.reserve(Self::ENCODED_LEN);
        out.extend_from_slice(&self.txid);
        out.extend_from_slice(&self.version.to_le_bytes());
        out.extend_from_slice(&self.locktime.to_le_bytes());
        out.extend_from_slice(&self.input_start_fk.0.to_le_bytes());
        out.extend_from_slice(&self.input_count.to_le_bytes());
        out.extend_from_slice(&self.output_start_fk.0.to_le_bytes());
        out.extend_from_slice(&self.output_count.to_le_bytes());
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(Self::ENCODED_LEN);
        self.encode_into(&mut out);
        out
    }

    pub fn decode(buf: &[u8]) -> Result<Self, StoreError> {
        if buf.len() < Self::ENCODED_LEN {
            return Err(StoreError::Corrupt("short tx record"));
        }
        Ok(Self {
            txid: buf[0..32].try_into().unwrap(),
            version: i32::from_le_bytes(buf[32..36].try_into().unwrap()),
            locktime: u32::from_le_bytes(buf[36..40].try_into().unwrap()),
            input_start_fk: Fk(u64::from_le_bytes(buf[40..48].try_into().unwrap())),
            input_count: u32::from_le_bytes(buf[48..52].try_into().unwrap()),
            output_start_fk: Fk(u64::from_le_bytes(buf[52..60].try_into().unwrap())),
            output_count: u32::from_le_bytes(buf[60..64].try_into().unwrap()),
        })
    }
}

/// Class A output (addressed via `tx.output_start_fk` run + local vout).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OutputRecord {
    pub value: i64,
    pub script: Vec<u8>,
}

impl OutputRecord {
    pub fn encode_into(&self, out: &mut Vec<u8>) {
        let mut flags = 0u8;
        if self.script.is_empty() {
            flags |= output_flags::EMPTY_SCRIPT;
        } else if self.script == [0x51] {
            flags |= output_flags::OP_TRUE;
        }
        out.push(flags);
        // Non-negative sats as uleb128 (Bitcoin values are ≥ 0).
        let v = if self.value < 0 { 0u64 } else { self.value as u64 };
        write_uleb128(out, v);
        if flags & (output_flags::EMPTY_SCRIPT | output_flags::OP_TRUE) == 0 {
            write_compact_size(out, self.script.len() as u64);
            out.extend_from_slice(&self.script);
        }
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(1 + 10 + self.script.len());
        self.encode_into(&mut out);
        out
    }

    /// Decode one output; returns (record, bytes_consumed).
    pub fn decode_at(buf: &[u8]) -> Result<(Self, usize), StoreError> {
        if buf.is_empty() {
            return Err(StoreError::Corrupt("short output record"));
        }
        let flags = buf[0];
        let mut off = 1usize;
        let (v, n) = read_uleb128(&buf[off..])?;
        off += n;
        if v > i64::MAX as u64 {
            return Err(StoreError::Corrupt("output value too large"));
        }
        let value = v as i64;
        let script = if flags & output_flags::EMPTY_SCRIPT != 0 {
            Vec::new()
        } else if flags & output_flags::OP_TRUE != 0 {
            vec![0x51]
        } else {
            let (slen, n) = read_compact_size(&buf[off..])?;
            off += n;
            let slen = slen as usize;
            if buf.len() < off + slen {
                return Err(StoreError::Corrupt("output script truncated"));
            }
            let s = buf[off..off + slen].to_vec();
            off += slen;
            s
        };
        Ok((Self { value, script }, off))
    }

    pub fn decode(buf: &[u8]) -> Result<Self, StoreError> {
        let (rec, used) = Self::decode_at(buf)?;
        if used != buf.len() {
            return Err(StoreError::Corrupt("output trailing bytes"));
        }
        Ok(rec)
    }

    pub fn encoded_len(&self) -> usize {
        1 + 10 + 9 + self.script.len()
    }
}

/// Encode a per-tx output run (concat of compact outputs; count lives on TxRecord).
pub fn encode_output_run(recs: &[OutputRecord], out: &mut Vec<u8>) {
    for r in recs {
        r.encode_into(out);
    }
}

pub fn decode_output_run(buf: &[u8], count: u32) -> Result<Vec<OutputRecord>, StoreError> {
    let mut out = Vec::with_capacity(count as usize);
    let mut off = 0;
    for _ in 0..count {
        let (rec, used) = OutputRecord::decode_at(&buf[off..])?;
        off += used;
        out.push(rec);
    }
    if off != buf.len() {
        return Err(StoreError::Corrupt("output run trailing bytes"));
    }
    Ok(out)
}

/// Class A input + BIP141 witness (addressed via `tx.input_start_fk` run + local i).
///
/// Prevout encoding (on disk):
/// - coinbase: `NULL_PREV`
/// - local: `LOCAL_PREV` + CompactSize `prev_tx_fk` + CompactSize `vout`
/// - external: full `prev_txid[32]` + CompactSize `vout`
///
/// In memory, `prev_txid` may be zeros when only `prev_tx_fk` is known (resolve
/// via `get_tx` before building wire OutPoints).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InputRecord {
    pub prev_txid: [u8; 32],
    /// When non-null, prev is a local Class A tx (preferred on-disk form).
    pub prev_tx_fk: Fk,
    pub prev_index: u32,
    pub sequence: u32,
    pub script_sig: Vec<u8>,
    /// Witness stack items (empty = no witness).
    pub witness: Vec<Vec<u8>>,
}

impl InputRecord {
    pub fn is_coinbase(&self) -> bool {
        self.prev_tx_fk.is_null()
            && self.prev_txid == [0u8; 32]
            && self.prev_index == u32::MAX
    }

    pub fn encode_into(&self, out: &mut Vec<u8>) {
        let null_prev = self.is_coinbase();
        let local = !null_prev && !self.prev_tx_fk.is_null();
        let mut flags = 0u8;
        if self.sequence == u32::MAX {
            flags |= input_flags::SEQ_FINAL;
        }
        if self.script_sig.is_empty() {
            flags |= input_flags::EMPTY_SCRIPT;
        }
        if self.witness.is_empty() {
            flags |= input_flags::EMPTY_WITNESS;
        }
        if null_prev {
            flags |= input_flags::NULL_PREV;
        } else if local {
            flags |= input_flags::LOCAL_PREV;
        }
        out.push(flags);
        if null_prev {
            // nothing
        } else if local {
            write_compact_size(out, self.prev_tx_fk.0);
            write_compact_size(out, u64::from(self.prev_index));
        } else {
            out.extend_from_slice(&self.prev_txid);
            write_compact_size(out, u64::from(self.prev_index));
        }
        if flags & input_flags::SEQ_FINAL == 0 {
            out.extend_from_slice(&self.sequence.to_le_bytes());
        }
        if flags & input_flags::EMPTY_SCRIPT == 0 {
            write_compact_size(out, self.script_sig.len() as u64);
            out.extend_from_slice(&self.script_sig);
        }
        if flags & input_flags::EMPTY_WITNESS == 0 {
            write_compact_size(out, self.witness.len() as u64);
            for item in &self.witness {
                write_compact_size(out, item.len() as u64);
                out.extend_from_slice(item);
            }
        }
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        self.encode_into(&mut out);
        out
    }

    /// Decode one input; returns (record, bytes_consumed).
    pub fn decode_at(buf: &[u8]) -> Result<(Self, usize), StoreError> {
        if buf.is_empty() {
            return Err(StoreError::Corrupt("short input record"));
        }
        let flags = buf[0];
        let mut off = 1usize;
        let (prev_txid, prev_tx_fk, prev_index) = if flags & input_flags::NULL_PREV != 0 {
            ([0u8; 32], Fk::NULL, u32::MAX)
        } else if flags & input_flags::LOCAL_PREV != 0 {
            let (fk_raw, n) = read_compact_size(&buf[off..])?;
            off += n;
            let Some(prev_tx_fk) = Fk::new(fk_raw) else {
                return Err(StoreError::Corrupt("local prev fk null"));
            };
            let (vout, n) = read_compact_size(&buf[off..])?;
            off += n;
            if vout > u64::from(u32::MAX) {
                return Err(StoreError::Corrupt("prev_index too large"));
            }
            ([0u8; 32], prev_tx_fk, vout as u32)
        } else {
            if buf.len() < off + 32 {
                return Err(StoreError::Corrupt("input prev_txid truncated"));
            }
            let prev_txid: [u8; 32] = buf[off..off + 32].try_into().unwrap();
            off += 32;
            let (vout, n) = read_compact_size(&buf[off..])?;
            off += n;
            if vout > u64::from(u32::MAX) {
                return Err(StoreError::Corrupt("prev_index too large"));
            }
            (prev_txid, Fk::NULL, vout as u32)
        };
        let sequence = if flags & input_flags::SEQ_FINAL != 0 {
            u32::MAX
        } else {
            if buf.len() < off + 4 {
                return Err(StoreError::Corrupt("input sequence truncated"));
            }
            let s = u32::from_le_bytes(buf[off..off + 4].try_into().unwrap());
            off += 4;
            s
        };
        let script_sig = if flags & input_flags::EMPTY_SCRIPT != 0 {
            Vec::new()
        } else {
            let (slen, n) = read_compact_size(&buf[off..])?;
            off += n;
            let slen = slen as usize;
            if buf.len() < off + slen {
                return Err(StoreError::Corrupt("input script truncated"));
            }
            let s = buf[off..off + slen].to_vec();
            off += slen;
            s
        };
        let witness = if flags & input_flags::EMPTY_WITNESS != 0 {
            Vec::new()
        } else {
            let (nw, n) = read_compact_size(&buf[off..])?;
            off += n;
            let mut witness = Vec::with_capacity(nw as usize);
            for _ in 0..nw {
                let (ilen, n) = read_compact_size(&buf[off..])?;
                off += n;
                let ilen = ilen as usize;
                if buf.len() < off + ilen {
                    return Err(StoreError::Corrupt("witness item truncated"));
                }
                witness.push(buf[off..off + ilen].to_vec());
                off += ilen;
            }
            witness
        };
        Ok((
            Self {
                prev_txid,
                prev_tx_fk,
                prev_index,
                sequence,
                script_sig,
                witness,
            },
            off,
        ))
    }

    pub fn decode(buf: &[u8]) -> Result<Self, StoreError> {
        let (rec, used) = Self::decode_at(buf)?;
        if used != buf.len() {
            return Err(StoreError::Corrupt("input trailing bytes"));
        }
        Ok(rec)
    }

    pub fn encoded_len(&self) -> usize {
        // upper bound for reserve estimates
        1 + 32 + 9 + 9 + 4 + 9 + self.script_sig.len() + 9
            + self.witness.iter().map(|i| 9 + i.len()).sum::<usize>()
    }
}

/// Encode a per-tx input run (concat of compact inputs; count lives on TxRecord).
pub fn encode_input_run(recs: &[InputRecord], out: &mut Vec<u8>) {
    for r in recs {
        r.encode_into(out);
    }
}

pub fn decode_input_run(buf: &[u8], count: u32) -> Result<Vec<InputRecord>, StoreError> {
    let mut out = Vec::with_capacity(count as usize);
    let mut off = 0;
    for _ in 0..count {
        let (rec, used) = InputRecord::decode_at(&buf[off..])?;
        off += used;
        out.push(rec);
    }
    if off != buf.len() {
        return Err(StoreError::Corrupt("input run trailing bytes"));
    }
    Ok(out)
}

pub struct TxTable {
    body: VarTable,
    head: ShardedHashHead,
}

impl TxTable {
    pub fn create(dir: &Path) -> Result<Self, StoreError> {
        Ok(Self {
            body: VarTable::create(dir, "tx", TableKind::Tx)?,
            head: ShardedHashHead::create_for_role(
                dir.join("tx.head"),
                crate::hashhead::HeadRole::Tx,
            )?,
        })
    }

    pub fn open(dir: &Path) -> Result<Self, StoreError> {
        Ok(Self {
            body: VarTable::open(dir, "tx", TableKind::Tx)?,
            head: ShardedHashHead::open_for_role(
                dir.join("tx.head"),
                crate::hashhead::HeadRole::Tx,
            )?,
        })
    }

    pub fn count(&self) -> u64 {
        self.body.count()
    }

    pub fn reserve_append(&self, body_bytes: u64, n_records: u64) -> Result<(), StoreError> {
        self.body.reserve_append(body_bytes, n_records)
    }

    pub fn put(&self, rec: &TxRecord) -> Result<Fk, StoreError> {
        let mut fks = self.put_batch(std::slice::from_ref(rec))?;
        Ok(fks.pop().expect("one tx"))
    }

    pub fn put_batch(&self, recs: &[TxRecord]) -> Result<Vec<Fk>, StoreError> {
        self.put_batch_indexed(recs, true)
    }

    pub fn put_batch_indexed(
        &self,
        recs: &[TxRecord],
        index: bool,
    ) -> Result<Vec<Fk>, StoreError> {
        if recs.is_empty() {
            return Ok(Vec::new());
        }
        let est: usize = recs.len() * TxRecord::ENCODED_LEN;
        let fks = self.body.put_batch_encode(recs.len(), est, |i, buf| {
            recs[i].encode_into(buf);
        })?;
        if index {
            let heads: Vec<([u8; 32], Fk)> = recs
                .iter()
                .zip(fks.iter())
                .map(|(r, fk)| (r.txid, *fk))
                .collect();
            self.head.insert_many(&heads)?;
        }
        Ok(fks)
    }

    pub fn get(&self, fk: Fk) -> Result<TxRecord, StoreError> {
        let raw = self.body.get_raw(fk)?;
        TxRecord::decode(&raw)
    }

    pub fn get_by_txid(&self, txid: &[u8; 32]) -> Result<Option<(Fk, TxRecord)>, StoreError> {
        match self.head.get(txid)? {
            None => Ok(None),
            Some(fk) => Ok(Some((fk, self.get(fk)?))),
        }
    }

    /// Ensure durable `tx.head` maps `txid → fk` for every Class A body.
    ///
    /// Used after milestone IBD (archive with `index=false`) so Electrum
    /// `transaction.get` / scripthash prevout joins can resolve by txid.
    /// Idempotent: skips keys already present. Returns number of inserts.
    ///
    /// `on_progress(done_bodies, total_bodies, inserted)` is invoked periodically.
    pub fn backfill_head(
        &self,
        mut on_progress: impl FnMut(u64, u64, u64),
    ) -> Result<u64, StoreError> {
        let n = self.count();
        if n == 0 {
            return Ok(0);
        }
        let mut inserted = 0u64;
        // Batch inserts for fewer rehash decisions.
        const CHUNK: usize = 4096;
        const PROGRESS_EVERY: u64 = 50_000;
        let mut batch: Vec<([u8; 32], Fk)> = Vec::with_capacity(CHUNK);
        let mut last_progress = 0u64;
        for id in 1..=n {
            let fk = Fk(id);
            let rec = self.get(fk)?;
            if self.head.get(&rec.txid)?.is_none() {
                batch.push((rec.txid, fk));
                if batch.len() >= CHUNK {
                    inserted += batch.len() as u64;
                    self.head.insert_many(&batch)?;
                    batch.clear();
                }
            }
            if id - last_progress >= PROGRESS_EVERY || id == n {
                on_progress(id, n, inserted + batch.len() as u64);
                last_progress = id;
            }
        }
        if !batch.is_empty() {
            inserted += batch.len() as u64;
            self.head.insert_many(&batch)?;
        }
        on_progress(n, n, inserted);
        Ok(inserted)
    }

    /// Approximate occupied slots in the hash head (for "needs backfill?" checks).
    pub fn head_occupied(&self) -> u64 {
        self.head.occupied()
    }

    /// Bulk-insert head entries (sorted-run materialize / backfill helper).
    pub fn head_insert_many(&self, entries: &[([u8; 32], Fk)]) -> Result<(), StoreError> {
        if entries.is_empty() {
            return Ok(());
        }
        self.head.reserve_additional(entries.len() as u64)?;
        self.head.insert_many(entries)
    }

    /// Enable process-local write-behind on `tx.head` (optional IBD path).
    pub fn enable_head_write_behind(&self, max_entries: usize) -> Result<(), StoreError> {
        self.head.enable_write_behind(max_entries)
    }

    /// Disable write-behind after spilling pending head updates.
    pub fn disable_head_write_behind(&self) -> Result<(), StoreError> {
        self.head.disable_write_behind()
    }

    /// Spill pending `tx.head` write-behind entries without fsync.
    pub fn spill_head(&self) -> Result<(), StoreError> {
        self.head.spill_write_behind()
    }

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

    /// Defer soft-cap `tx.head` spills during confirm (same as point.head).
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

    pub fn flush_async_no_spill(&self) -> Result<(), StoreError> {
        self.body.flush_async()?;
        self.head.flush_async_no_spill()?;
        Ok(())
    }
}

/// Per-tx output **runs**: one var record = all outputs of one tx.
pub struct OutputTable {
    body: VarTable,
}

impl OutputTable {
    pub fn create(dir: &Path) -> Result<Self, StoreError> {
        Ok(Self {
            body: VarTable::create(dir, "output", TableKind::Output)?,
        })
    }

    pub fn open(dir: &Path) -> Result<Self, StoreError> {
        Ok(Self {
            body: VarTable::open(dir, "output", TableKind::Output)?,
        })
    }

    pub fn count(&self) -> u64 {
        self.body.count()
    }

    pub fn reserve_append(&self, body_bytes: u64, n_records: u64) -> Result<(), StoreError> {
        self.body.reserve_append(body_bytes, n_records)
    }

    /// Append one run (all outputs of a tx). Returns run FK.
    pub fn put_run(&self, recs: &[OutputRecord]) -> Result<Fk, StoreError> {
        let mut fks = self.put_runs(std::slice::from_ref(&recs))?;
        Ok(fks.pop().expect("one run"))
    }

    /// Append many runs. `runs[i]` is the output list for one tx.
    pub fn put_runs(&self, runs: &[&[OutputRecord]]) -> Result<Vec<Fk>, StoreError> {
        if runs.is_empty() {
            return Ok(Vec::new());
        }
        let est: usize = runs
            .iter()
            .map(|r| r.iter().map(|o| o.encoded_len()).sum::<usize>())
            .sum();
        self.body.put_batch_encode(runs.len(), est, |i, buf| {
            encode_output_run(runs[i], buf);
        })
    }

    /// Decode full run; `count` must match TxRecord.output_count.
    pub fn get_run(&self, fk: Fk, count: u32) -> Result<Vec<OutputRecord>, StoreError> {
        let raw = self.body.get_raw(fk)?;
        decode_output_run(&raw, count)
    }

    pub fn get_at(&self, fk: Fk, count: u32, index: u32) -> Result<OutputRecord, StoreError> {
        if index >= count {
            return Err(StoreError::NotFound);
        }
        let mut run = self.get_run(fk, count)?;
        Ok(run.swap_remove(index as usize))
    }

    pub fn flush(&self) -> Result<(), StoreError> {
        self.body.flush()
    }

    pub fn flush_async(&self) -> Result<(), StoreError> {
        self.body.flush_async()
    }
}

/// Per-tx input **runs**: one var record = all inputs of one tx.
pub struct InputTable {
    body: VarTable,
}

impl InputTable {
    pub fn create(dir: &Path) -> Result<Self, StoreError> {
        Ok(Self {
            body: VarTable::create(dir, "input", TableKind::Input)?,
        })
    }

    pub fn open(dir: &Path) -> Result<Self, StoreError> {
        Ok(Self {
            body: VarTable::open(dir, "input", TableKind::Input)?,
        })
    }

    pub fn count(&self) -> u64 {
        self.body.count()
    }

    pub fn reserve_append(&self, body_bytes: u64, n_records: u64) -> Result<(), StoreError> {
        self.body.reserve_append(body_bytes, n_records)
    }

    pub fn put_run(&self, recs: &[InputRecord]) -> Result<Fk, StoreError> {
        let mut fks = self.put_runs(std::slice::from_ref(&recs))?;
        Ok(fks.pop().expect("one run"))
    }

    pub fn put_runs(&self, runs: &[&[InputRecord]]) -> Result<Vec<Fk>, StoreError> {
        if runs.is_empty() {
            return Ok(Vec::new());
        }
        let est: usize = runs
            .iter()
            .map(|r| r.iter().map(|o| o.encoded_len()).sum::<usize>())
            .sum();
        self.body.put_batch_encode(runs.len(), est, |i, buf| {
            encode_input_run(runs[i], buf);
        })
    }

    pub fn get_run(&self, fk: Fk, count: u32) -> Result<Vec<InputRecord>, StoreError> {
        let raw = self.body.get_raw(fk)?;
        decode_input_run(&raw, count)
    }

    pub fn get_at(&self, fk: Fk, count: u32, index: u32) -> Result<InputRecord, StoreError> {
        if index >= count {
            return Err(StoreError::NotFound);
        }
        let mut run = self.get_run(fk, count)?;
        Ok(run.swap_remove(index as usize))
    }

    pub fn flush(&self) -> Result<(), StoreError> {
        self.body.flush()
    }

    pub fn flush_async(&self) -> Result<(), StoreError> {
        self.body.flush_async()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn input_witness_roundtrip() {
        let rec = InputRecord {
            prev_txid: [1u8; 32],
            prev_tx_fk: Fk::NULL,
            prev_index: 2,
            sequence: 0xffff_fffe,
            script_sig: vec![0x00],
            witness: vec![vec![0x30, 0x01], vec![0x21, 0xaa]],
        };
        let enc = rec.encode();
        let dec = InputRecord::decode(&enc).unwrap();
        assert_eq!(rec, dec);
    }

    #[test]
    fn input_flags_roundtrip() {
        let rec = InputRecord {
            prev_txid: [0u8; 32],
            prev_tx_fk: Fk::NULL,
            prev_index: u32::MAX,
            sequence: u32::MAX,
            script_sig: vec![],
            witness: vec![],
        };
        let enc = rec.encode();
        // flags only: null prev + final seq + empty script + empty witness
        assert_eq!(enc.len(), 1);
        assert_eq!(InputRecord::decode(&enc).unwrap(), rec);
    }

    #[test]
    fn input_local_prev_roundtrip() {
        let rec = InputRecord {
            prev_txid: [0u8; 32], // not stored when LOCAL_PREV
            prev_tx_fk: Fk(42),
            prev_index: 1,
            sequence: u32::MAX,
            script_sig: vec![],
            witness: vec![],
        };
        let enc = rec.encode();
        // flags + compact 42 + compact 1
        assert!(enc.len() < 1 + 32);
        let dec = InputRecord::decode(&enc).unwrap();
        assert_eq!(dec.prev_tx_fk, Fk(42));
        assert_eq!(dec.prev_index, 1);
        assert_eq!(dec.prev_txid, [0u8; 32]);
        assert!(dec.encoded_len() >= enc.len());
    }

    #[test]
    fn input_run_roundtrip() {
        let run = vec![
            InputRecord {
                prev_txid: [0u8; 32],
                prev_tx_fk: Fk::NULL,
                prev_index: u32::MAX,
                sequence: u32::MAX,
                script_sig: vec![0x01],
                witness: vec![],
            },
            InputRecord {
                prev_txid: [2u8; 32],
                prev_tx_fk: Fk::NULL,
                prev_index: 0,
                sequence: 1,
                script_sig: vec![],
                witness: vec![vec![0xab]],
            },
            InputRecord {
                prev_txid: [0u8; 32],
                prev_tx_fk: Fk(7),
                prev_index: 3,
                sequence: u32::MAX,
                script_sig: vec![],
                witness: vec![],
            },
        ];
        let mut enc = Vec::new();
        encode_input_run(&run, &mut enc);
        assert_eq!(decode_input_run(&enc, 3).unwrap(), run);
    }

    #[test]
    fn output_run_roundtrip() {
        let run = vec![
            OutputRecord {
                value: 50_0000_0000,
                script: vec![0x51],
            },
            OutputRecord {
                value: 0,
                script: vec![],
            },
            OutputRecord {
                value: 12345,
                script: vec![0x00, 0x14, 0xaa],
            },
        ];
        let mut enc = Vec::new();
        encode_output_run(&run, &mut enc);
        assert_eq!(decode_output_run(&enc, 3).unwrap(), run);
        // OP_TRUE / empty should be tiny
        let mut tiny = Vec::new();
        run[0].encode_into(&mut tiny);
        assert!(tiny.len() < 12, "op_true+value should be compact: {}", tiny.len());
    }

    #[test]
    fn tx_fixed_roundtrip() {
        let rec = TxRecord {
            txid: [9u8; 32],
            version: 2,
            locktime: 100,
            input_start_fk: Fk(1),
            input_count: 1,
            output_start_fk: Fk(2),
            output_count: 2,
        };
        let enc = rec.encode();
        assert_eq!(enc.len(), TxRecord::ENCODED_LEN);
        assert_eq!(TxRecord::decode(&enc).unwrap(), rec);
    }
}
