use crate::address_head::AddressHead;
use crate::compact::{
    input_flags, output_flags, read_compact_size, read_uleb128, write_compact_size, write_uleb128,
};
use crate::error::StoreError;
use crate::var_table::VarTable;
use rbitcoin_primitives::{Fk, TableKind};
use std::path::Path;

/// Class A tx row (no wire blob — reconstruct from inputs/outputs + witness).
///
/// On-disk bodies are **packed-only** ([`PACKED_TX_V1`]): inputs and outputs are
/// embedded in the same `tx.body` payload. `input_start_fk` / `output_start_fk`
/// are always [`Fk::NULL`] on write and ignored on read (kept in the fixed meta
/// layout for encoding stability).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TxRecord {
    pub txid: [u8; 32],
    pub version: i32,
    pub locktime: u32,
    /// Always [`Fk::NULL`] for packed Class A (legacy split-run address unused).
    pub input_start_fk: Fk,
    pub input_count: u32,
    /// Always [`Fk::NULL`] for packed Class A (legacy split-run address unused).
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
    /// Schema v5: sole `spending_tx_fk` if !multi; else head fk into `spenders.body`.
    pub spender_field: Fk,
    /// When true, `spender_field` is a multi-list head (not a single spending_tx_fk).
    pub multi_spender: bool,
}

impl OutputRecord {
    pub fn unspent(value: i64, script: Vec<u8>) -> Self {
        Self {
            value,
            script,
            spender_field: Fk::NULL,
            multi_spender: false,
        }
    }

    pub fn encode_into(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(&self.spender_field.0.to_le_bytes());
        let mut flags = 0u8;
        if self.script.is_empty() {
            flags |= output_flags::EMPTY_SCRIPT;
        } else if self.script == [0x51] {
            flags |= output_flags::OP_TRUE;
        }
        if self.multi_spender {
            flags |= output_flags::MULTI_SPENDER;
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
        let mut out = Vec::with_capacity(8 + 1 + 10 + self.script.len());
        self.encode_into(&mut out);
        out
    }

    /// Decode one output; returns (record, bytes_consumed).
    pub fn decode_at(buf: &[u8]) -> Result<(Self, usize), StoreError> {
        if buf.len() < 9 {
            return Err(StoreError::Corrupt("short output record"));
        }
        let spender_field = Fk(u64::from_le_bytes(buf[0..8].try_into().unwrap()));
        let flags = buf[8];
        let multi_spender = flags & output_flags::MULTI_SPENDER != 0;
        let mut off = 9usize;
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
        Ok((
            Self {
                value,
                script,
                spender_field,
                multi_spender,
            },
            off,
        ))
    }

    pub fn decode(buf: &[u8]) -> Result<Self, StoreError> {
        let (rec, used) = Self::decode_at(buf)?;
        if used != buf.len() {
            return Err(StoreError::Corrupt("output trailing bytes"));
        }
        Ok(rec)
    }

    pub fn encoded_len(&self) -> usize {
        8 + 1 + 10 + 9 + self.script.len()
    }
}

/// Encode a per-tx output run (concat of compact outputs; count lives on TxRecord).
pub fn encode_output_run(recs: &[OutputRecord], out: &mut Vec<u8>) {
    for r in recs {
        r.encode_into(out);
    }
}

#[cfg(test)]
fn decode_output_run(buf: &[u8], count: u32) -> Result<Vec<OutputRecord>, StoreError> {
    let (out, used) = decode_output_run_prefix(buf, count)?;
    if used != buf.len() {
        return Err(StoreError::Corrupt("output run trailing bytes"));
    }
    Ok(out)
}

/// Decode `count` outputs; returns records + bytes consumed (allows trailing data).
pub fn decode_output_run_prefix(
    buf: &[u8],
    count: u32,
) -> Result<(Vec<OutputRecord>, usize), StoreError> {
    let mut out = Vec::with_capacity(count as usize);
    let mut off = 0;
    for _ in 0..count {
        let (rec, used) = OutputRecord::decode_at(&buf[off..])?;
        off += used;
        out.push(rec);
    }
    Ok((out, off))
}

/// Class A input + BIP141 witness (addressed via `tx.input_start_fk` run + local i).
///
/// Prevout encoding (on disk):
/// - coinbase: `NULL_PREV`
/// - non-coinbase: full `prev_txid[32]` + CompactSize `vout`
///
/// No local `prev_tx_fk` on Class A: catch-up resolves create fk via light UTXO;
/// tip mode uses durable points / `tx.head`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InputRecord {
    pub prev_txid: [u8; 32],
    pub prev_index: u32,
    pub sequence: u32,
    pub script_sig: Vec<u8>,
    /// Witness stack items (empty = no witness).
    pub witness: Vec<Vec<u8>>,
}

impl InputRecord {
    pub fn is_coinbase(&self) -> bool {
        self.prev_txid == [0u8; 32] && self.prev_index == u32::MAX
    }

    pub fn encode_into(&self, out: &mut Vec<u8>) {
        let null_prev = self.is_coinbase();
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
        }
        out.push(flags);
        if null_prev {
            // nothing
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

    /// Walk one input: return `(prev_txid, prev_index, bytes_consumed)` without
    /// allocating `script_sig` / `witness` (prewarm parent discovery).
    pub fn decode_prevout_at(buf: &[u8]) -> Result<([u8; 32], u32, usize), StoreError> {
        if buf.is_empty() {
            return Err(StoreError::Corrupt("short input record"));
        }
        let flags = buf[0];
        let mut off = 1usize;
        if flags & input_flags::LOCAL_PREV != 0 {
            return Err(StoreError::Corrupt(
                "input LOCAL_PREV removed; re-archive Class A (use external prev_txid)",
            ));
        }
        let (prev_txid, prev_index) = if flags & input_flags::NULL_PREV != 0 {
            ([0u8; 32], u32::MAX)
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
            (prev_txid, vout as u32)
        };
        if flags & input_flags::SEQ_FINAL == 0 {
            if buf.len() < off + 4 {
                return Err(StoreError::Corrupt("input sequence truncated"));
            }
            off += 4;
        }
        if flags & input_flags::EMPTY_SCRIPT == 0 {
            let (slen, n) = read_compact_size(&buf[off..])?;
            off += n;
            let slen = slen as usize;
            if buf.len() < off + slen {
                return Err(StoreError::Corrupt("input script truncated"));
            }
            off += slen;
        }
        if flags & input_flags::EMPTY_WITNESS == 0 {
            let (nw, n) = read_compact_size(&buf[off..])?;
            off += n;
            for _ in 0..nw {
                let (ilen, n) = read_compact_size(&buf[off..])?;
                off += n;
                let ilen = ilen as usize;
                if buf.len() < off + ilen {
                    return Err(StoreError::Corrupt("witness item truncated"));
                }
                off += ilen;
            }
        }
        Ok((prev_txid, prev_index, off))
    }

    /// Decode one input; returns (record, bytes_consumed).
    pub fn decode_at(buf: &[u8]) -> Result<(Self, usize), StoreError> {
        if buf.is_empty() {
            return Err(StoreError::Corrupt("short input record"));
        }
        let flags = buf[0];
        let mut off = 1usize;
        if flags & input_flags::LOCAL_PREV != 0 {
            return Err(StoreError::Corrupt(
                "input LOCAL_PREV removed; re-archive Class A (use external prev_txid)",
            ));
        }
        let (prev_txid, prev_index) = if flags & input_flags::NULL_PREV != 0 {
            ([0u8; 32], u32::MAX)
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
            (prev_txid, vout as u32)
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

#[cfg(test)]
fn decode_input_run(buf: &[u8], count: u32) -> Result<Vec<InputRecord>, StoreError> {
    let (out, used) = decode_input_run_prefix(buf, count)?;
    if used != buf.len() {
        return Err(StoreError::Corrupt("input run trailing bytes"));
    }
    Ok(out)
}

/// Decode `count` inputs; returns records + bytes consumed (allows trailing data).
pub fn decode_input_run_prefix(
    buf: &[u8],
    count: u32,
) -> Result<(Vec<InputRecord>, usize), StoreError> {
    let mut out = Vec::with_capacity(count as usize);
    let mut off = 0;
    for _ in 0..count {
        let (rec, used) = InputRecord::decode_at(&buf[off..])?;
        off += used;
        out.push(rec);
    }
    Ok((out, off))
}

/// Packed Class A payload tag (first byte of `tx.body` record).
///
/// Layout: `PACKED_V1 || TxRecord(64) || input_run || output_run`
/// so one `get_raw(fk)` returns the full transaction body (single body IO).
///
/// **All Class A bodies are packed** (schema current). Non-packed payloads are
/// rejected as corrupt on read.
pub const PACKED_TX_V1: u8 = 0x01;

/// Encode a full Class A tx as one var payload.
pub fn encode_packed_tx(
    tx: &TxRecord,
    inputs: &[InputRecord],
    outputs: &[OutputRecord],
    out: &mut Vec<u8>,
) {
    debug_assert_eq!(inputs.len() as u32, tx.input_count);
    debug_assert_eq!(outputs.len() as u32, tx.output_count);
    out.push(PACKED_TX_V1);
    // I/O fks are unused for packed rows (body is self-contained).
    let mut meta = tx.clone();
    meta.input_start_fk = Fk::NULL;
    meta.output_start_fk = Fk::NULL;
    meta.encode_into(out);
    encode_input_run(inputs, out);
    encode_output_run(outputs, out);
}

/// Decode packed Class A; `raw` is the full var payload including tag.
pub fn decode_packed_tx(
    raw: &[u8],
) -> Result<(TxRecord, Vec<InputRecord>, Vec<OutputRecord>), StoreError> {
    if raw.first().copied() != Some(PACKED_TX_V1) {
        return Err(StoreError::Corrupt("not a packed Class A tx"));
    }
    if raw.len() < 1 + TxRecord::ENCODED_LEN {
        return Err(StoreError::Corrupt("short packed Class A tx"));
    }
    let meta = TxRecord::decode(&raw[1..1 + TxRecord::ENCODED_LEN])?;
    let mut off = 1 + TxRecord::ENCODED_LEN;
    let (inputs, in_used) = decode_input_run_prefix(&raw[off..], meta.input_count)?;
    off += in_used;
    let (outputs, out_used) = decode_output_run_prefix(&raw[off..], meta.output_count)?;
    off += out_used;
    if off != raw.len() {
        return Err(StoreError::Corrupt("packed Class A trailing bytes"));
    }
    if inputs.len() as u32 != meta.input_count || outputs.len() as u32 != meta.output_count {
        return Err(StoreError::Corrupt("packed Class A count mismatch"));
    }
    Ok((meta, inputs, outputs))
}

/// Packed meta + input prevouts only (skip scripts, witnesses, and outputs).
pub fn scan_packed_meta_and_prevouts(
    raw: &[u8],
) -> Result<(TxRecord, Vec<([u8; 32], u32)>), StoreError> {
    if raw.first().copied() != Some(PACKED_TX_V1) {
        return Err(StoreError::Corrupt("not a packed Class A tx"));
    }
    if raw.len() < 1 + TxRecord::ENCODED_LEN {
        return Err(StoreError::Corrupt("short packed Class A tx"));
    }
    let meta = TxRecord::decode(&raw[1..1 + TxRecord::ENCODED_LEN])?;
    let mut off = 1 + TxRecord::ENCODED_LEN;
    let mut prevouts = Vec::with_capacity(meta.input_count as usize);
    for _ in 0..meta.input_count {
        let (prev_txid, prev_index, used) = InputRecord::decode_prevout_at(&raw[off..])?;
        off += used;
        prevouts.push((prev_txid, prev_index));
    }
    let _ = off;
    Ok((meta, prevouts))
}

/// Decode packed Class A **meta + outputs only** (skip allocating parent inputs).
///
/// Same body IO as [`decode_packed_tx`]; cheaper CPU for prewarm parent loads
/// that only need prevout script/value.
pub fn decode_packed_tx_outs_only(
    raw: &[u8],
) -> Result<(TxRecord, Vec<OutputRecord>), StoreError> {
    if raw.first().copied() != Some(PACKED_TX_V1) {
        return Err(StoreError::Corrupt("not a packed Class A tx"));
    }
    if raw.len() < 1 + TxRecord::ENCODED_LEN {
        return Err(StoreError::Corrupt("short packed Class A tx"));
    }
    let meta = TxRecord::decode(&raw[1..1 + TxRecord::ENCODED_LEN])?;
    let mut off = 1 + TxRecord::ENCODED_LEN;
    // Walk inputs without keeping records (witness can be large).
    for _ in 0..meta.input_count {
        let (_txid, _vout, used) = InputRecord::decode_prevout_at(&raw[off..])?;
        off += used;
    }
    let (outputs, out_used) = decode_output_run_prefix(&raw[off..], meta.output_count)?;
    off += out_used;
    if off != raw.len() {
        return Err(StoreError::Corrupt("packed Class A trailing bytes"));
    }
    if outputs.len() as u32 != meta.output_count {
        return Err(StoreError::Corrupt("packed Class A count mismatch"));
    }
    Ok((meta, outputs))
}

#[inline]
pub fn is_packed_tx_payload(raw: &[u8]) -> bool {
    raw.len() > TxRecord::ENCODED_LEN && raw.first().copied() == Some(PACKED_TX_V1)
}

pub struct TxTable {
    body: VarTable,
    head: AddressHead,
}

impl TxTable {
    pub fn create(dir: &Path) -> Result<Self, StoreError> {
        Ok(Self {
            body: VarTable::create(dir, "tx", TableKind::Tx)?,
            head: AddressHead::create(dir.join("tx.head"))?,
        })
    }

    pub fn open(dir: &Path) -> Result<Self, StoreError> {
        Ok(Self {
            body: VarTable::open(dir, "tx", TableKind::Tx)?,
            head: AddressHead::open(dir.join("tx.head"))?,
        })
    }

    pub fn count(&self) -> u64 {
        self.body.count()
    }

    /// Current `tx.body` logical length (including file header).
    pub fn body_logical_len(&self) -> u64 {
        self.body.body_logical_len()
    }

    /// Best-effort: drop `tx.body` page-cache for a written range (archive far lead).
    pub fn advise_body_dont_need(&self, offset: u64, len: u64) {
        self.body.advise_body_dont_need(offset, len);
    }

    /// Absolute `(offset, len)` of packed body for `fk`.
    pub fn body_range(&self, fk: Fk) -> Result<(u64, u64), StoreError> {
        self.body.record_range(fk)
    }

    /// `mlock` pages covering the packed body for `fk`.
    ///
    /// Returns page-aligned `(page_start, page_len)` for later [`Self::munlock_body_pages`].
    pub fn mlock_body(&self, fk: Fk) -> Result<(u64, u64), StoreError> {
        self.body.mlock_record(fk)
    }

    /// `mlock` the `tx.idx` slot for `fk`.
    pub fn mlock_idx(&self, fk: Fk) -> Result<(u64, u64), StoreError> {
        self.body.mlock_idx_entry(fk)
    }

    /// `mlock` address-head probe slots for `txid` (until empty / MAX_PROBE).
    pub fn mlock_head_probe(&self, txid: &[u8; 32]) -> Result<(u64, u64), StoreError> {
        self.head.mlock_probe(txid)
    }

    /// Best-effort `munlock` for a prior [`Self::mlock_body`] page range.
    pub fn munlock_body_pages(&self, page_start: u64, page_len: u64) {
        self.body.munlock_body_pages(page_start, page_len);
    }

    pub fn munlock_idx_pages(&self, page_start: u64, page_len: u64) {
        self.body.munlock_idx_pages(page_start, page_len);
    }

    pub fn munlock_head_pages(&self, page_start: u64, page_len: u64) {
        self.head.munlock_pages(page_start, page_len);
    }

    /// Meta + input prevouts only (no script/witness allocation, no outputs).
    ///
    /// Used by prewarm: discover parents after `mlock` without full parse into RAM.
    pub fn get_meta_and_prevouts(
        &self,
        fk: Fk,
    ) -> Result<(TxRecord, Vec<([u8; 32], u32)>), StoreError> {
        self.body.with_raw(fk, |raw| scan_packed_meta_and_prevouts(raw))
    }

    /// Full decode from a known body range (skip idx).
    pub fn get_full_at(
        &self,
        offset: u64,
        len: u64,
    ) -> Result<(TxRecord, Vec<InputRecord>, Vec<OutputRecord>), StoreError> {
        self.body
            .with_bytes_at(offset, len, |raw| decode_packed_tx(raw))
    }

    /// Meta + prevouts from a known body range (skip idx).
    pub fn get_meta_and_prevouts_at(
        &self,
        offset: u64,
        len: u64,
    ) -> Result<(TxRecord, Vec<([u8; 32], u32)>), StoreError> {
        self.body
            .with_bytes_at(offset, len, |raw| scan_packed_meta_and_prevouts(raw))
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
            self.head_insert_many(&heads)?;
        }
        Ok(fks)
    }

    pub fn get(&self, fk: Fk) -> Result<TxRecord, StoreError> {
        let raw = self.body.get_raw(fk)?;
        let (tx, _, _) = decode_packed_tx(&raw)?;
        Ok(tx)
    }

    /// Read Class A body txid only (packed prefix or bare TxRecord).
    fn body_txid(&self, fk: Fk) -> Result<[u8; 32], StoreError> {
        let raw = self.body.get_raw(fk)?;
        if raw.first().copied() == Some(PACKED_TX_V1) {
            if raw.len() < 1 + 32 {
                return Err(StoreError::Corrupt("short packed tx for txid"));
            }
            return Ok(raw[1..33].try_into().unwrap());
        }
        // Bare meta (legacy test paths): TxRecord starts with txid[32].
        if raw.len() < 32 {
            return Err(StoreError::Corrupt("short tx record for txid"));
        }
        Ok(raw[0..32].try_into().unwrap())
    }

    /// Relative byte offset of output `vout`'s `spender_field` inside a packed tx payload.
    fn packed_output_spender_rel(raw: &[u8], vout: u32) -> Result<u64, StoreError> {
        if raw.first().copied() != Some(PACKED_TX_V1) {
            return Err(StoreError::Corrupt("not a packed Class A tx"));
        }
        if raw.len() < 1 + TxRecord::ENCODED_LEN {
            return Err(StoreError::Corrupt("short packed tx"));
        }
        let meta = TxRecord::decode(&raw[1..1 + TxRecord::ENCODED_LEN])?;
        if vout >= meta.output_count {
            return Err(StoreError::NotFound);
        }
        let mut off = 1 + TxRecord::ENCODED_LEN;
        for _ in 0..meta.input_count {
            let (_rec, used) = InputRecord::decode_at(&raw[off..])?;
            off += used;
        }
        for i in 0..=vout {
            if off >= raw.len() {
                return Err(StoreError::Corrupt("packed outputs short"));
            }
            if i == vout {
                return Ok(off as u64);
            }
            let (_, used) = OutputRecord::decode_at(&raw[off..])?;
            off += used;
        }
        Err(StoreError::NotFound)
    }

    /// Read multi + spender_field for create tx output (packed Class A body).
    pub fn get_output_spender_meta(
        &self,
        create_tx_fk: Fk,
        vout: u32,
    ) -> Result<(bool, Fk), StoreError> {
        let raw = self.body.get_raw(create_tx_fk)?;
        let rel = Self::packed_output_spender_rel(&raw, vout)? as usize;
        if raw.len() < rel + 9 {
            return Err(StoreError::Corrupt("packed spender meta short"));
        }
        let field = Fk(u64::from_le_bytes(raw[rel..rel + 8].try_into().unwrap()));
        let multi = raw[rel + 8] & output_flags::MULTI_SPENDER != 0;
        Ok((multi, field))
    }

    /// Patch multi + spender_field on create tx output (packed Class A body).
    pub fn set_output_spender_meta(
        &self,
        create_tx_fk: Fk,
        vout: u32,
        multi: bool,
        field: Fk,
    ) -> Result<(), StoreError> {
        let raw = self.body.get_raw(create_tx_fk)?;
        let rel = Self::packed_output_spender_rel(&raw, vout)?;
        self.body
            .write_at_record(create_tx_fk, rel, &field.0.to_le_bytes())?;
        let flag_rel = rel + 8;
        let mut flags = [0u8; 1];
        // re-read flags byte after possible remap — use original raw
        let fo = flag_rel as usize;
        if fo >= raw.len() {
            return Err(StoreError::Corrupt("packed flags missing"));
        }
        flags[0] = raw[fo];
        if multi {
            flags[0] |= output_flags::MULTI_SPENDER;
        } else {
            flags[0] &= !output_flags::MULTI_SPENDER;
        }
        self.body.write_at_record(create_tx_fk, flag_rel, &flags)?;
        Ok(())
    }

    /// Full tx body in **one** `tx.body` read (packed Class A only).
    pub fn get_full(
        &self,
        fk: Fk,
    ) -> Result<(TxRecord, Vec<InputRecord>, Vec<OutputRecord>), StoreError> {
        let raw = self.body.get_raw(fk)?;
        decode_packed_tx(&raw)
    }

    /// Meta + outputs only (one body IO; skips input materialization).
    pub fn get_meta_and_outputs(
        &self,
        fk: Fk,
    ) -> Result<(TxRecord, Vec<OutputRecord>), StoreError> {
        let raw = self.body.get_raw(fk)?;
        decode_packed_tx_outs_only(&raw)
    }

    /// Append packed full-tx records (one var payload per tx = one body IO on read).
    pub fn put_full_batch_indexed(
        &self,
        items: &[(TxRecord, Vec<InputRecord>, Vec<OutputRecord>)],
        index: bool,
    ) -> Result<Vec<Fk>, StoreError> {
        if items.is_empty() {
            return Ok(Vec::new());
        }
        let est: usize = items
            .iter()
            .map(|(_tx, ins, outs)| {
                1 + TxRecord::ENCODED_LEN
                    + ins.iter().map(|i| i.encoded_len()).sum::<usize>()
                    + outs.iter().map(|o| o.encoded_len()).sum::<usize>()
            })
            .sum();
        let fks = self.body.put_batch_encode(items.len(), est, |i, buf| {
            let (tx, ins, outs) = &items[i];
            encode_packed_tx(tx, ins, outs, buf);
        })?;
        if index {
            let heads: Vec<([u8; 32], Fk)> = items
                .iter()
                .zip(fks.iter())
                .map(|((tx, _, _), fk)| (tx.txid, *fk))
                .collect();
            self.head_insert_many(&heads)?;
        }
        Ok(fks)
    }

    pub fn get_by_txid(&self, txid: &[u8; 32]) -> Result<Option<(Fk, TxRecord)>, StoreError> {
        // Keyless address head: probe candidates, verify body; prefer first match
        // (newest-first BIP30 insert places newest earliest on the probe chain).
        for fk in self.head.probe_fks(txid)? {
            let rec = self.get(fk)?;
            if rec.txid == *txid {
                return Ok(Some((fk, rec)));
            }
        }
        Ok(None)
    }

    /// All Class A fks whose body txid equals `txid` (BIP30: more than one).
    pub fn get_all_by_txid(&self, txid: &[u8; 32]) -> Result<Vec<(Fk, TxRecord)>, StoreError> {
        let mut out = Vec::new();
        for fk in self.head.probe_fks(txid)? {
            let rec = self.get(fk)?;
            if rec.txid == *txid {
                out.push((fk, rec));
            }
        }
        Ok(out)
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
            // Skip if this exact Class A fk is already on the probe chain.
            if !self.head.probe_fks(&rec.txid)?.contains(&fk) {
                batch.push((rec.txid, fk));
                if batch.len() >= CHUNK {
                    inserted += batch.len() as u64;
                    self.head_insert_many(&batch)?;
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
            self.head_insert_many(&batch)?;
        }
        on_progress(n, n, inserted);
        Ok(inserted)
    }

    /// Approximate occupied slots in the hash head (for "needs backfill?" checks).
    pub fn head_occupied(&self) -> u64 {
        self.head.occupied()
    }

    /// Fixed address table — no growth rehash (capacity is `2^BITS` slots).
    pub fn head_reserve_additional(&self, additional: u64) -> Result<(), StoreError> {
        self.head.reserve_additional(additional)
    }

    /// Bulk-insert head entries (archive / run materialize / backfill).
    ///
    /// Body txids are loaded for collision / BIP30 decisions (keyless head).
    pub fn head_insert_many(&self, entries: &[([u8; 32], Fk)]) -> Result<(), StoreError> {
        if entries.is_empty() {
            return Ok(());
        }
        self.head.insert_many_paced(entries, |fk| self.body_txid(fk))
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
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decode_prevout_at_skips_script_and_witness() {
        let rec = InputRecord {
            prev_txid: [9u8; 32],
            prev_index: 3,
            sequence: 0xffff_fffe,
            script_sig: vec![0xab; 40],
            witness: vec![vec![0x30; 70], vec![0x21; 33]],
        };
        let enc = rec.encode();
        let (txid, vout, used) = InputRecord::decode_prevout_at(&enc).unwrap();
        assert_eq!(txid, [9u8; 32]);
        assert_eq!(vout, 3);
        assert_eq!(used, enc.len());
        // Full decode still matches.
        let (full, used2) = InputRecord::decode_at(&enc).unwrap();
        assert_eq!(used2, used);
        assert_eq!(full.script_sig.len(), 40);
    }

    #[test]
    fn scan_packed_meta_and_prevouts_no_output_alloc() {
        let tx = TxRecord {
            txid: [7u8; 32],
            version: 2,
            locktime: 0,
            input_start_fk: Fk::NULL,
            input_count: 2,
            output_start_fk: Fk::NULL,
            output_count: 1,
        };
        let inputs = vec![
            InputRecord {
                prev_txid: [0u8; 32],
                prev_index: u32::MAX,
                sequence: u32::MAX,
                script_sig: vec![0x01],
                witness: vec![],
            },
            InputRecord {
                prev_txid: [3u8; 32],
                prev_index: 1,
                sequence: u32::MAX,
                script_sig: vec![],
                witness: vec![vec![0xaa]],
            },
        ];
        let outputs = vec![OutputRecord::unspent(50, vec![0x51])];
        let mut raw = Vec::new();
        encode_packed_tx(&tx, &inputs, &outputs, &mut raw);
        let (meta, prevouts) = scan_packed_meta_and_prevouts(&raw).unwrap();
        assert_eq!(meta.txid, [7u8; 32]);
        assert_eq!(prevouts.len(), 2);
        assert_eq!(prevouts[0], ([0u8; 32], u32::MAX));
        assert_eq!(prevouts[1], ([3u8; 32], 1));
    }

    #[test]
    fn input_witness_roundtrip() {
        let rec = InputRecord {
            prev_txid: [1u8; 32],
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
    fn input_rejects_legacy_local_prev() {
        use crate::compact::write_compact_size;
        // flags: LOCAL_PREV | SEQ_FINAL | EMPTY_SCRIPT | EMPTY_WITNESS
        let flags = input_flags::LOCAL_PREV
            | input_flags::SEQ_FINAL
            | input_flags::EMPTY_SCRIPT
            | input_flags::EMPTY_WITNESS;
        let mut enc = vec![flags];
        write_compact_size(&mut enc, 42);
        write_compact_size(&mut enc, 1);
        assert!(InputRecord::decode(&enc).is_err());
    }

    #[test]
    fn input_run_roundtrip() {
        let run = vec![
            InputRecord {
                prev_txid: [0u8; 32],
                prev_index: u32::MAX,
                sequence: u32::MAX,
                script_sig: vec![0x01],
                witness: vec![],
            },
            InputRecord {
                prev_txid: [2u8; 32],
                prev_index: 0,
                sequence: 1,
                script_sig: vec![],
                witness: vec![vec![0xab]],
            },
            InputRecord {
                prev_txid: [3u8; 32],
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
            OutputRecord::unspent(50_0000_0000, vec![0x51]),
            OutputRecord::unspent(0, vec![]),
            OutputRecord::unspent(12345, vec![0x00, 0x14, 0xaa]),
        ];
        let mut enc = Vec::new();
        encode_output_run(&run, &mut enc);
        assert_eq!(decode_output_run(&enc, 3).unwrap(), run);
        // OP_TRUE + spender_field(8) + flags + uleb value
        let mut tiny = Vec::new();
        run[0].encode_into(&mut tiny);
        assert!(tiny.len() < 24, "op_true+value should be compact: {}", tiny.len());
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

    #[test]
    fn packed_tx_roundtrip() {
        let tx = TxRecord {
            txid: [7u8; 32],
            version: 2,
            locktime: 0,
            input_start_fk: Fk(99), // ignored in packed
            input_count: 1,
            output_start_fk: Fk(88),
            output_count: 2,
        };
        let inputs = vec![InputRecord {
            prev_txid: [0u8; 32],
            prev_index: u32::MAX,
            sequence: u32::MAX,
            script_sig: vec![0x01, 0x00],
            witness: vec![],
        }];
        let outputs = vec![
            OutputRecord::unspent(50_0000_0000, vec![0x51]),
            OutputRecord::unspent(1, vec![0x00, 0x14]),
        ];
        let mut enc = Vec::new();
        encode_packed_tx(&tx, &inputs, &outputs, &mut enc);
        assert!(is_packed_tx_payload(&enc));
        let (dtx, dins, douts) = decode_packed_tx(&enc).unwrap();
        assert_eq!(dtx.txid, tx.txid);
        assert_eq!(dtx.input_count, 1);
        assert_eq!(dtx.output_count, 2);
        assert!(dtx.input_start_fk.get().is_none());
        assert_eq!(dins, inputs);
        assert_eq!(douts, outputs);
    }

    #[test]
    fn non_packed_tx_body_rejected() {
        // Bare TxRecord meta without PACKED_TX_V1 tag (legacy 3-table layout).
        let rec = TxRecord {
            txid: [1u8; 32],
            version: 1,
            locktime: 0,
            input_start_fk: Fk(1),
            input_count: 1,
            output_start_fk: Fk(2),
            output_count: 1,
        };
        let raw = rec.encode();
        assert!(!is_packed_tx_payload(&raw));
        assert!(matches!(
            decode_packed_tx(&raw),
            Err(StoreError::Corrupt(_))
        ));
        assert!(matches!(
            decode_packed_tx_outs_only(&raw),
            Err(StoreError::Corrupt(_))
        ));
    }

    #[test]
    fn address_head_get_by_txid() {
        let dir = std::env::temp_dir().join(format!(
            "rbitcoin-tx-addr-head-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        // Force tiny address width for the test process.
        std::env::set_var("RBITCOIN_HEAD_SCALE", "tiny");
        let t = TxTable::create(&dir).unwrap();
        let tx = TxRecord {
            txid: [0x42u8; 32],
            version: 1,
            locktime: 0,
            input_start_fk: Fk::NULL,
            input_count: 1,
            output_start_fk: Fk::NULL,
            output_count: 1,
        };
        let inputs = vec![InputRecord {
            prev_txid: [0u8; 32],
            prev_index: u32::MAX,
            sequence: u32::MAX,
            script_sig: vec![],
            witness: vec![],
        }];
        let outputs = vec![OutputRecord::unspent(50_0000_0000, vec![0x51])];
        let fks = t
            .put_full_batch_indexed(&[(tx.clone(), inputs, outputs)], true)
            .unwrap();
        assert_eq!(fks.len(), 1);
        let (fk, rec) = t.get_by_txid(&tx.txid).unwrap().expect("found");
        assert_eq!(fk, fks[0]);
        assert_eq!(rec.txid, tx.txid);
        assert!(t.get_by_txid(&[0x99u8; 32]).unwrap().is_none());
        let _ = std::fs::remove_dir_all(&dir);
    }
}
