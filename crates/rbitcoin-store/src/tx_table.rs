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

    /// `mlock` body pages for a known absolute `(offset, len)` (no idx read).
    pub fn mlock_body_at(&self, offset: u64, len: u64) -> Result<(u64, u64), StoreError> {
        self.body.mlock_body_range(offset, len)
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

    /// Meta + outputs only from a known body range (skip allocating parent inputs).
    pub fn get_meta_and_outputs_at(
        &self,
        offset: u64,
        len: u64,
    ) -> Result<(TxRecord, Vec<OutputRecord>), StoreError> {
        self.body
            .with_bytes_at(offset, len, |raw| decode_packed_tx_outs_only(raw))
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
    ///
    /// Input walk uses [`InputRecord::decode_prevout_at`] (no script/witness alloc).
    fn packed_output_spender_rel(raw: &[u8], vout: u32) -> Result<u64, StoreError> {
        let found = Self::packed_output_spender_rels(raw, &[vout])?;
        found
            .into_iter()
            .next()
            .map(|(_, rel)| rel)
            .ok_or(StoreError::NotFound)
    }

    /// One packed walk: for each requested `vout`, relative offset of its spender_field.
    ///
    /// `vouts` need not be sorted; results are returned in ascending vout order.
    /// Missing vouts are omitted (caller treats as NotFound).
    fn packed_output_spender_rels(raw: &[u8], vouts: &[u32]) -> Result<Vec<(u32, u64)>, StoreError> {
        if raw.first().copied() != Some(PACKED_TX_V1) {
            return Err(StoreError::Corrupt("not a packed Class A tx"));
        }
        if raw.len() < 1 + TxRecord::ENCODED_LEN {
            return Err(StoreError::Corrupt("short packed tx"));
        }
        if vouts.is_empty() {
            return Ok(Vec::new());
        }
        let meta = TxRecord::decode(&raw[1..1 + TxRecord::ENCODED_LEN])?;
        let mut want: Vec<u32> = vouts.to_vec();
        want.sort_unstable();
        want.dedup();
        let max_v = *want.last().unwrap();
        if max_v >= meta.output_count {
            return Err(StoreError::NotFound);
        }
        let mut off = 1 + TxRecord::ENCODED_LEN;
        // Skip inputs without materializing script/witness.
        for _ in 0..meta.input_count {
            let (_txid, _vout, used) = InputRecord::decode_prevout_at(&raw[off..])?;
            off += used;
        }
        let mut out = Vec::with_capacity(want.len());
        let mut wi = 0usize;
        for i in 0..=max_v {
            if off >= raw.len() {
                return Err(StoreError::Corrupt("packed outputs short"));
            }
            if wi < want.len() && want[wi] == i {
                out.push((i, off as u64));
                wi += 1;
            }
            let (_, used) = OutputRecord::decode_at(&raw[off..])?;
            off += used;
        }
        if out.len() != want.len() {
            return Err(StoreError::NotFound);
        }
        Ok(out)
    }

    /// Read Class A body txid from a known range (no idx).
    pub fn body_txid_at(&self, offset: u64, len: u64) -> Result<[u8; 32], StoreError> {
        self.body.with_bytes_at(offset, len, |raw| {
            if raw.first().copied() == Some(PACKED_TX_V1) {
                if raw.len() < 1 + 32 {
                    return Err(StoreError::Corrupt("short packed tx for txid"));
                }
                return Ok(raw[1..33].try_into().unwrap());
            }
            if raw.len() < 32 {
                return Err(StoreError::Corrupt("short tx record for txid"));
            }
            Ok(raw[0..32].try_into().unwrap())
        })
    }

    /// Primary head probe slot for `txid` (sort key for locality-friendly batches).
    #[inline]
    pub fn head_primary_slot(&self, txid: &[u8; 32]) -> u64 {
        crate::address_head::probe_index(txid, 0, self.head.bits())
    }

    /// Probe address head and verify body **txid only** (no full packed decode).
    pub fn get_fk_by_txid(&self, txid: &[u8; 32]) -> Result<Option<Fk>, StoreError> {
        for fk in self.head.probe_fks(txid)? {
            if self.body_txid(fk)? == *txid {
                return Ok(Some(fk));
            }
        }
        Ok(None)
    }

    /// Read multi + spender_field for create tx output (packed Class A body).
    pub fn get_output_spender_meta(
        &self,
        create_tx_fk: Fk,
        vout: u32,
    ) -> Result<(bool, Fk), StoreError> {
        let raw = self.body.get_raw(create_tx_fk)?;
        Self::spender_meta_from_raw(&raw, vout)
    }

    /// Like [`Self::get_output_spender_meta`] but uses a prewarmed body range (no idx).
    pub fn get_output_spender_meta_at(
        &self,
        body_off: u64,
        body_len: u64,
        vout: u32,
    ) -> Result<(bool, Fk), StoreError> {
        self.body
            .with_bytes_at(body_off, body_len, |raw| Self::spender_meta_from_raw(raw, vout))
    }

    /// One packed body walk: spender meta for many vouts (ascending).
    ///
    /// Returns `(vout, multi, field)` for each found vout. Missing vouts omitted.
    pub fn get_output_spender_metas_at(
        &self,
        body_off: u64,
        body_len: u64,
        vouts: &[u32],
    ) -> Result<Vec<(u32, bool, Fk)>, StoreError> {
        if vouts.is_empty() {
            return Ok(Vec::new());
        }
        self.body.with_bytes_at(body_off, body_len, |raw| {
            let rels = Self::packed_output_spender_rels(raw, vouts)?;
            let mut out = Vec::with_capacity(rels.len());
            for (v, rel) in rels {
                let fo = rel as usize;
                if raw.len() < fo + 9 {
                    return Err(StoreError::Corrupt("packed spender meta short"));
                }
                let field = Fk(u64::from_le_bytes(raw[fo..fo + 8].try_into().unwrap()));
                let multi = raw[fo + 8] & output_flags::MULTI_SPENDER != 0;
                out.push((v, multi, field));
            }
            Ok(out)
        })
    }

    fn spender_meta_from_raw(raw: &[u8], vout: u32) -> Result<(bool, Fk), StoreError> {
        let rel = Self::packed_output_spender_rel(raw, vout)? as usize;
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
        let (off, len) = self.body.record_range(create_tx_fk)?;
        self.set_output_spender_meta_at(off, len, vout, multi, field)
    }

    /// Patch spender meta using a prewarmed body range (no idx read on the hot path).
    pub fn set_output_spender_meta_at(
        &self,
        body_off: u64,
        body_len: u64,
        vout: u32,
        multi: bool,
        field: Fk,
    ) -> Result<(), StoreError> {
        let (rel, flag0) = self.body.with_bytes_at(body_off, body_len, |raw| {
            let rel = Self::packed_output_spender_rel(raw, vout)?;
            let fo = rel as usize + 8;
            if fo >= raw.len() {
                return Err(StoreError::Corrupt("packed flags missing"));
            }
            Ok((rel, raw[fo]))
        })?;
        self.body
            .write_body_abs(body_off + rel, &field.0.to_le_bytes())?;
        let mut flags = [flag0];
        if multi {
            flags[0] |= output_flags::MULTI_SPENDER;
        } else {
            flags[0] &= !output_flags::MULTI_SPENDER;
        }
        self.body.write_body_abs(body_off + rel + 8, &flags)?;
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
        // Probe + body_txid only; full decode only on match.
        let Some(fk) = self.get_fk_by_txid(txid)? else {
            return Ok(None);
        };
        Ok(Some((fk, self.get(fk)?)))
    }

    /// All Class A fks whose body txid equals `txid` (BIP30: more than one).
    pub fn get_all_by_txid(&self, txid: &[u8; 32]) -> Result<Vec<(Fk, TxRecord)>, StoreError> {
        let mut out = Vec::new();
        for fk in self.head.probe_fks(txid)? {
            if self.body_txid(fk)? != *txid {
                continue;
            }
            out.push((fk, self.get(fk)?));
        }
        Ok(out)
    }

    /// One body pin: apply many spend annotations `(vout, spending_tx_fk)` on one create.
    ///
    /// Walks inputs once (prevout-only) and outputs up to max needed vout once,
    /// then patches each output's spender field.
    pub fn put_spends_on_create_at(
        &self,
        spenders: &crate::spender_table::SpenderTable,
        body_off: u64,
        body_len: u64,
        edges: &[(u32, Fk)],
    ) -> Result<(), StoreError> {
        if edges.is_empty() {
            return Ok(());
        }
        for &(_, sfk) in edges {
            if sfk.is_null() {
                return Err(StoreError::InvalidFk);
            }
        }
        let vouts: Vec<u32> = {
            let mut v: Vec<u32> = edges.iter().map(|(v, _)| *v).collect();
            v.sort_unstable();
            v.dedup();
            v
        };
        let metas: Vec<(u32, u64, bool, Fk)> =
            self.body.with_bytes_at(body_off, body_len, |raw| {
                let rels = Self::packed_output_spender_rels(raw, &vouts)?;
                let mut out = Vec::with_capacity(rels.len());
                for (v, rel) in rels {
                    let fo = rel as usize;
                    if raw.len() < fo + 9 {
                        return Err(StoreError::Corrupt("packed spender meta short"));
                    }
                    let field = Fk(u64::from_le_bytes(raw[fo..fo + 8].try_into().unwrap()));
                    let multi = raw[fo + 8] & output_flags::MULTI_SPENDER != 0;
                    out.push((v, rel, multi, field));
                }
                Ok(out)
            })?;
        let mut by_vout: std::collections::HashMap<u32, (u64, bool, Fk)> =
            std::collections::HashMap::with_capacity(metas.len());
        for (v, rel, multi, field) in metas {
            by_vout.insert(v, (rel, multi, field));
        }
        for &(vout, spend_fk) in edges {
            let Some(&(rel, multi, field)) = by_vout.get(&vout) else {
                return Err(StoreError::NotFound);
            };
            let (new_multi, new_field) = if !multi && field.is_null() {
                (false, spend_fk)
            } else if !multi && field == spend_fk {
                continue;
            } else if !multi {
                let e1 = spenders.append(field, Fk::NULL)?;
                let e2 = spenders.append(spend_fk, e1)?;
                (true, e2)
            } else {
                let e = spenders.append(spend_fk, field)?;
                (true, e)
            };
            self.body
                .write_body_abs(body_off + rel, &new_field.0.to_le_bytes())?;
            let flag0 = self.body.with_bytes_at(body_off, body_len, |raw| {
                let fo = rel as usize + 8;
                if fo >= raw.len() {
                    return Err(StoreError::Corrupt("packed flags missing"));
                }
                Ok(raw[fo])
            })?;
            let mut flags = [flag0];
            if new_multi {
                flags[0] |= output_flags::MULTI_SPENDER;
            } else {
                flags[0] &= !output_flags::MULTI_SPENDER;
            }
            self.body.write_body_abs(body_off + rel + 8, &flags)?;
            by_vout.insert(vout, (rel, new_multi, new_field));
        }
        Ok(())
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
    fn packed_output_spender_rels_multi_vout_one_walk() {
        let tx = TxRecord {
            txid: [8u8; 32],
            version: 1,
            locktime: 0,
            input_start_fk: Fk::NULL,
            input_count: 1,
            output_start_fk: Fk::NULL,
            output_count: 4,
        };
        let inputs = vec![InputRecord {
            prev_txid: [0u8; 32],
            prev_index: u32::MAX,
            sequence: u32::MAX,
            script_sig: vec![0x01],
            witness: vec![],
        }];
        let outputs = vec![
            OutputRecord::unspent(1, vec![0x51]),
            OutputRecord::unspent(2, vec![0x51]),
            OutputRecord::unspent(3, vec![0x51]),
            OutputRecord::unspent(4, vec![0x51]),
        ];
        let mut raw = Vec::new();
        encode_packed_tx(&tx, &inputs, &outputs, &mut raw);
        let single0 = TxTable::packed_output_spender_rel(&raw, 0).unwrap();
        let single3 = TxTable::packed_output_spender_rel(&raw, 3).unwrap();
        let multi = TxTable::packed_output_spender_rels(&raw, &[3, 0, 3]).unwrap();
        assert_eq!(multi.len(), 2);
        assert_eq!(multi[0], (0, single0));
        assert_eq!(multi[1], (3, single3));
        // Each rel points at a 9-byte spender field (null fk + flags).
        for (_, rel) in multi {
            let fo = rel as usize;
            assert!(raw.len() >= fo + 9);
            assert_eq!(&raw[fo..fo + 8], &[0u8; 8]);
        }
    }

    #[test]
    fn head_primary_slot_stable_and_ordered() {
        let dir = std::env::temp_dir().join(format!(
            "rbitcoin-tx-slot-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        std::env::set_var("RBITCOIN_HEAD_SCALE", "tiny");
        let t = TxTable::create(&dir).unwrap();
        let a = [1u8; 32];
        let b = [2u8; 32];
        let sa = t.head_primary_slot(&a);
        let sb = t.head_primary_slot(&b);
        assert_eq!(sa, t.head_primary_slot(&a));
        // Distinct keys almost always land on distinct primary slots at tiny scale.
        assert_ne!(sa, sb);
        let mut keys = vec![b, a];
        keys.sort_unstable_by_key(|k| t.head_primary_slot(k));
        assert!(t.head_primary_slot(&keys[0]) <= t.head_primary_slot(&keys[1]));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn get_output_spender_metas_at_one_walk() {
        let dir = std::env::temp_dir().join(format!(
            "rbitcoin-tx-metas-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        std::env::set_var("RBITCOIN_HEAD_SCALE", "tiny");
        let t = TxTable::create(&dir).unwrap();
        let spenders = crate::spender_table::SpenderTable::create(&dir).unwrap();
        let tx = TxRecord {
            txid: [0xcd; 32],
            version: 1,
            locktime: 0,
            input_start_fk: Fk::NULL,
            input_count: 1,
            output_start_fk: Fk::NULL,
            output_count: 3,
        };
        let inputs = vec![InputRecord {
            prev_txid: [0u8; 32],
            prev_index: u32::MAX,
            sequence: u32::MAX,
            script_sig: vec![0x01],
            witness: vec![],
        }];
        let outputs = vec![
            OutputRecord::unspent(1, vec![0x51]),
            OutputRecord::unspent(2, vec![0x51]),
            OutputRecord::unspent(3, vec![0x51]),
        ];
        let fks = t
            .put_full_batch_indexed(&[(tx, inputs, outputs)], false)
            .unwrap();
        let (off, len) = t.body_range(fks[0]).unwrap();
        let s1 = Fk(10);
        t.put_spends_on_create_at(&spenders, off, len, &[(0, s1), (2, Fk(20))])
            .unwrap();
        let metas = t
            .get_output_spender_metas_at(off, len, &[0, 1, 2])
            .unwrap();
        assert_eq!(metas.len(), 3);
        assert!(!metas[0].1 && metas[0].2 == s1);
        assert!(!metas[1].1 && metas[1].2.is_null());
        assert!(!metas[2].1 && metas[2].2 == Fk(20));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn put_spends_on_create_at_batch_patches_all_vouts() {
        let dir = std::env::temp_dir().join(format!(
            "rbitcoin-tx-spend-batch-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        std::env::set_var("RBITCOIN_HEAD_SCALE", "tiny");
        let t = TxTable::create(&dir).unwrap();
        let spenders = crate::spender_table::SpenderTable::create(&dir).unwrap();
        let tx = TxRecord {
            txid: [0xab; 32],
            version: 1,
            locktime: 0,
            input_start_fk: Fk::NULL,
            input_count: 1,
            output_start_fk: Fk::NULL,
            output_count: 3,
        };
        let inputs = vec![InputRecord {
            prev_txid: [0u8; 32],
            prev_index: u32::MAX,
            sequence: u32::MAX,
            script_sig: vec![0x01],
            witness: vec![],
        }];
        let outputs = vec![
            OutputRecord::unspent(10, vec![0x51]),
            OutputRecord::unspent(20, vec![0x51]),
            OutputRecord::unspent(30, vec![0x51]),
        ];
        let fks = t
            .put_full_batch_indexed(&[(tx, inputs, outputs)], true)
            .unwrap();
        let fk = fks[0];
        let (off, len) = t.body_range(fk).unwrap();
        let s1 = Fk(100);
        let s2 = Fk(200);
        t.put_spends_on_create_at(&spenders, off, len, &[(0, s1), (2, s2)])
            .unwrap();
        let (m0, f0) = t.get_output_spender_meta_at(off, len, 0).unwrap();
        let (m2, f2) = t.get_output_spender_meta_at(off, len, 2).unwrap();
        assert!(!m0 && f0 == s1);
        assert!(!m2 && f2 == s2);
        let (m1, f1) = t.get_output_spender_meta_at(off, len, 1).unwrap();
        assert!(!m1 && f1.is_null());
        let _ = std::fs::remove_dir_all(&dir);
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
