use crate::address_head::{
    bak_head_path, clear_resize_control, load_needs_resize, load_ratio, read_resize_control,
    shadow_head_path, write_head_meta, write_resize_control, AddressHead, HeadLayout,
    ResizeControl, HEAD_LOAD_WARN, MAX_BITS,
};
use crate::compact::{
    input_flags, output_flags, read_compact_size, read_uleb128, write_compact_size, write_uleb128,
};
use crate::error::StoreError;
use crate::var_table::VarTable;
use rbitcoin_primitives::{Fk, TableKind};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, RwLock};

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

/// Class A input + BIP141 witness (schema v10).
///
/// On-disk prevout:
/// - coinbase: `NULL_PREV` (no payload)
/// - non-coinbase: **`create_fk:u64` LE** + CompactSize `vout` (not prev_txid)
///
/// [`Self::prev_txid`] is a soft cache for wire rebuild (zeros until filled from
/// the create body or from the wire convert path). Encoding never writes it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InputRecord {
    /// Soft: wire txid of parent create (`[0;32]` if unknown / coinbase).
    pub prev_txid: [u8; 32],
    /// Dense Class A fk of parent create; [`Fk::NULL`] for coinbase.
    pub create_fk: Fk,
    pub prev_index: u32,
    pub sequence: u32,
    pub script_sig: Vec<u8>,
    /// Witness stack items (empty = no witness).
    pub witness: Vec<Vec<u8>>,
}

impl InputRecord {
    pub fn is_coinbase(&self) -> bool {
        self.create_fk.is_null() && self.prev_index == u32::MAX
    }

    /// Coinbase null prevout.
    pub fn coinbase(sequence: u32, script_sig: Vec<u8>, witness: Vec<Vec<u8>>) -> Self {
        Self {
            prev_txid: [0u8; 32],
            create_fk: Fk::NULL,
            prev_index: u32::MAX,
            sequence,
            script_sig,
            witness,
        }
    }

    pub fn encode_into(&self, out: &mut Vec<u8>) {
        let null_prev = self.create_fk.is_null() && self.prev_index == u32::MAX;
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
            debug_assert!(
                !self.create_fk.is_null(),
                "non-coinbase input requires create_fk before encode"
            );
            out.extend_from_slice(&self.create_fk.0.to_le_bytes());
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

    /// Skip past one input after reading create_fk + vout (no script/witness alloc).
    ///
    /// Returns `(create_fk, prev_index, bytes_consumed)`. Coinbase → `(NULL, u32::MAX, …)`.
    pub fn decode_prevout_at(buf: &[u8]) -> Result<(Fk, u32, usize), StoreError> {
        if buf.is_empty() {
            return Err(StoreError::Corrupt("short input record"));
        }
        let flags = buf[0];
        let mut off = 1usize;
        if flags & input_flags::RESERVED4 != 0 {
            return Err(StoreError::Corrupt(
                "input reserved flag set (wipe datadir for schema v10)",
            ));
        }
        let (create_fk, prev_index) = if flags & input_flags::NULL_PREV != 0 {
            (Fk::NULL, u32::MAX)
        } else {
            if buf.len() < off + 8 {
                return Err(StoreError::Corrupt("input create_fk truncated"));
            }
            let id = u64::from_le_bytes(buf[off..off + 8].try_into().unwrap());
            off += 8;
            if id == 0 {
                return Err(StoreError::Corrupt("non-coinbase create_fk is null"));
            }
            let (vout, n) = read_compact_size(&buf[off..])?;
            off += n;
            if vout > u64::from(u32::MAX) {
                return Err(StoreError::Corrupt("prev_index too large"));
            }
            (Fk(id), vout as u32)
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
        Ok((create_fk, prev_index, off))
    }

    /// Decode one input; returns (record, bytes_consumed).
    ///
    /// `prev_txid` is left zero; fill from create body when wire needs it.
    pub fn decode_at(buf: &[u8]) -> Result<(Self, usize), StoreError> {
        if buf.is_empty() {
            return Err(StoreError::Corrupt("short input record"));
        }
        let flags = buf[0];
        let mut off = 1usize;
        if flags & input_flags::RESERVED4 != 0 {
            return Err(StoreError::Corrupt(
                "input reserved flag set (wipe datadir for schema v10)",
            ));
        }
        let (create_fk, prev_index) = if flags & input_flags::NULL_PREV != 0 {
            (Fk::NULL, u32::MAX)
        } else {
            if buf.len() < off + 8 {
                return Err(StoreError::Corrupt("input create_fk truncated"));
            }
            let id = u64::from_le_bytes(buf[off..off + 8].try_into().unwrap());
            off += 8;
            if id == 0 {
                return Err(StoreError::Corrupt("non-coinbase create_fk is null"));
            }
            let (vout, n) = read_compact_size(&buf[off..])?;
            off += n;
            if vout > u64::from(u32::MAX) {
                return Err(StoreError::Corrupt("prev_index too large"));
            }
            (Fk(id), vout as u32)
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
                prev_txid: [0u8; 32],
                create_fk,
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
        // flags + create_fk(8) + vout + sequence + script + witness (upper bound)
        1 + 8 + 9 + 4 + 9 + self.script_sig.len() + 9
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

/// Packed meta + input create edges only (skip scripts, witnesses, and outputs).
///
/// Each edge is `(create_fk, vout)`; coinbase → `(Fk::NULL, u32::MAX)`.
pub fn scan_packed_meta_and_prevouts(
    raw: &[u8],
) -> Result<(TxRecord, Vec<(Fk, u32)>), StoreError> {
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
        let (create_fk, prev_index, used) = InputRecord::decode_prevout_at(&raw[off..])?;
        off += used;
        prevouts.push((create_fk, prev_index));
    }
    let _ = off;
    Ok((meta, prevouts))
}

/// Decode packed Class A **meta + outputs only** (skip allocating parent inputs).
///
/// Same body IO as [`decode_packed_tx`]; cheaper CPU for runway parent loads
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

/// In-progress sequential `tx.head` rebuild (shadow filled from `tx.idx` order).
struct HeadResize {
    shadow: AddressHead,
    cursor: u64,
    target: HeadLayout,
}

pub struct TxTable {
    body: VarTable,
    head: RwLock<AddressHead>,
    /// Directory containing `tx.head` (for rename / control paths).
    head_path: PathBuf,
    resize: Mutex<Option<HeadResize>>,
}

impl TxTable {
    pub fn create(dir: &Path) -> Result<Self, StoreError> {
        let head_path = dir.join("tx.head");
        Ok(Self {
            body: VarTable::create(dir, "tx", TableKind::Tx)?,
            head: RwLock::new(AddressHead::create(&head_path)?),
            head_path,
            resize: Mutex::new(None),
        })
    }

    pub fn open(dir: &Path) -> Result<Self, StoreError> {
        let head_path = dir.join("tx.head");
        let body = VarTable::open(dir, "tx", TableKind::Tx)?;
        let n_bodies = body.count();
        // Operator recovery: delete `tx.head` (+ optional `.meta`) → empty create
        // + full rebuild from Class A bodies on next open. Incomplete heads that
        // still open successfully are *not* auto-rebuilt (delete to force).
        let mut need_rebuild = false;
        let head = if !head_path.exists() {
            need_rebuild = n_bodies > 0;
            Self::prepare_fresh_head(&head_path, n_bodies)?
        } else {
            match AddressHead::open(&head_path) {
                Ok(h) => h,
                Err(e) => {
                    // Unreadable head with live Class A: recreate rather than
                    // refuse the whole store (same recovery as a deliberate delete).
                    if n_bodies > 0 {
                        rbitcoin_log::warn!(
                            "store: tx.head open failed ({e}) with {n_bodies} Class A bodies — recreating and rebuilding"
                        );
                        let _ = std::fs::remove_file(&head_path);
                        let mut meta = head_path.as_os_str().to_os_string();
                        meta.push(".meta");
                        let _ = std::fs::remove_file(PathBuf::from(meta));
                        need_rebuild = true;
                        Self::prepare_fresh_head(&head_path, n_bodies)?
                    } else {
                        return Err(e);
                    }
                }
            }
        };
        let t = Self {
            body,
            head: RwLock::new(head),
            head_path,
            resize: Mutex::new(None),
        };
        if need_rebuild {
            let inserted = t.rebuild_head_from_bodies(|done, total, ins| {
                if done == total || done % 1_000_000 == 0 {
                    rbitcoin_log::info!(
                        "store: tx.head rebuild progress {done}/{total} inserted={ins}"
                    );
                }
            })?;
            t.head.read().unwrap().flush()?;
            rbitcoin_log::warn!(
                "store: tx.head rebuild complete inserted={inserted} bodies={}",
                t.count()
            );
        } else {
            t.resume_head_resize_if_needed()?;
        }
        Ok(t)
    }

    /// Create empty `tx.head` (default layout), drop resize leftovers.
    fn prepare_fresh_head(head_path: &Path, n_bodies: u64) -> Result<AddressHead, StoreError> {
        clear_resize_control(head_path);
        let shadow = shadow_head_path(head_path);
        let _ = std::fs::remove_file(&shadow);
        {
            let mut p = shadow.as_os_str().to_os_string();
            p.push(".meta");
            let _ = std::fs::remove_file(PathBuf::from(p));
        }
        let bak = bak_head_path(head_path);
        let _ = std::fs::remove_file(&bak);
        {
            let mut p = bak.as_os_str().to_os_string();
            p.push(".meta");
            let _ = std::fs::remove_file(PathBuf::from(p));
        }
        // Stale meta from a deleted head would confuse layout; drop it.
        {
            let mut meta = head_path.as_os_str().to_os_string();
            meta.push(".meta");
            let _ = std::fs::remove_file(PathBuf::from(meta));
        }
        if n_bodies > 0 {
            rbitcoin_log::warn!(
                "store: tx.head missing/recreated — rebuilding from {n_bodies} Class A bodies (this can take a while)"
            );
        } else {
            rbitcoin_log::info!("store: tx.head missing — creating empty address head");
        }
        AddressHead::create(head_path)
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
        self.head.read().unwrap().mlock_probe(txid)
    }

    /// Best-effort `munlock` for a prior [`Self::mlock_body`] page range.
    pub fn munlock_body_pages(&self, page_start: u64, page_len: u64) {
        self.body.munlock_body_pages(page_start, page_len);
    }

    pub fn munlock_idx_pages(&self, page_start: u64, page_len: u64) {
        self.body.munlock_idx_pages(page_start, page_len);
    }

    pub fn munlock_head_pages(&self, page_start: u64, page_len: u64) {
        self.head.read().unwrap().munlock_pages(page_start, page_len);
    }

    /// Meta + input prevouts only (no script/witness allocation, no outputs).
    ///
    /// Used by load: discover parents after `mlock` without full parse into RAM.
    pub fn get_meta_and_prevouts(
        &self,
        fk: Fk,
    ) -> Result<(TxRecord, Vec<(Fk, u32)>), StoreError> {
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
    ) -> Result<(TxRecord, Vec<(Fk, u32)>), StoreError> {
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
    ///
    /// Public for wire rebuild / archive sticky: schema v10 inputs store
    /// `create_fk` only; callers fill soft `prev_txid` from the create body.
    pub fn body_txid(&self, fk: Fk) -> Result<[u8; 32], StoreError> {
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
        let bits = self.head.read().unwrap().bits();
        crate::address_head::probe_index(txid, 0, bits)
    }

    /// Probe address head and verify body **txid only** (no full packed decode).
    ///
    /// Body-check order is **last occupied probe slot → first** so that, after
    /// fast inserts (second same-txid lands deeper, no newest-first displace),
    /// the newest Class A create is preferred (BIP30-shaped duplicates).
    pub fn get_fk_by_txid(&self, txid: &[u8; 32]) -> Result<Option<Fk>, StoreError> {
        let cands = self.head.read().unwrap().probe_fks(txid)?;
        for fk in cands.into_iter().rev() {
            if self.body_txid(fk)? == *txid {
                return Ok(Some(fk));
            }
        }
        Ok(None)
    }

    /// Batch head resolve for load thin (caller should **primary-slot sort**).
    ///
    /// 1. Sequential head probe in call order (slot-sorted → page locality).
    /// 2. Unique candidate fks sorted → sequential body-txid reads.
    /// 3. Match each txid to the **last** probe candidate whose body txid equals
    ///    it (same preference as [`Self::get_fk_by_txid`]).
    ///
    /// Beats N independent `get_fk_by_txid` when body verifies thrash randomly
    /// and when head walks share nearby slots.
    pub fn get_fk_by_txid_batch(
        &self,
        txids: &[[u8; 32]],
    ) -> Result<Vec<([u8; 32], Option<Fk>)>, StoreError> {
        if txids.is_empty() {
            return Ok(Vec::new());
        }
        let mut pairs: Vec<([u8; 32], Vec<Fk>)> = Vec::with_capacity(txids.len());
        let mut cand_ids: Vec<u64> = Vec::new();
        {
            let head = self.head.read().unwrap();
            for txid in txids {
                let cands = head.probe_fks(txid)?;
                for fk in &cands {
                    if let Some(id) = fk.get() {
                        cand_ids.push(id);
                    }
                }
                pairs.push((*txid, cands));
            }
        }
        cand_ids.sort_unstable();
        cand_ids.dedup();
        // Body txid for unique fks in ascending order (page locality).
        let mut body_txids: HashMap<u64, [u8; 32]> = HashMap::with_capacity(cand_ids.len());
        for id in cand_ids {
            match self.body_txid(Fk(id)) {
                Ok(t) => {
                    body_txids.insert(id, t);
                }
                Err(StoreError::NotFound) => {}
                Err(e) => return Err(e),
            }
        }
        let mut out = Vec::with_capacity(pairs.len());
        for (txid, cands) in pairs {
            let mut hit = None;
            // Reverse: prefer deepest body match (newest under append-deeper insert).
            for fk in cands.into_iter().rev() {
                let Some(id) = fk.get() else {
                    continue;
                };
                if body_txids.get(&id) == Some(&txid) {
                    hit = Some(fk);
                    break;
                }
            }
            out.push((txid, hit));
        }
        Ok(out)
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

    /// Like [`Self::get_output_spender_meta`] but uses a runway-cached body range (no idx).
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

    /// Patch spender meta using a runway-cached body range (no idx read on the hot path).
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
    ///
    /// Order is **newest-first** (deepest probe match first), matching
    /// [`Self::get_fk_by_txid`].
    pub fn get_all_by_txid(&self, txid: &[u8; 32]) -> Result<Vec<(Fk, TxRecord)>, StoreError> {
        let mut out = Vec::new();
        for fk in self.head.read().unwrap().probe_fks(txid)?.into_iter().rev() {
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
    /// Idempotent: skips fks already present in the probe chain. Prefer
    /// [`Self::rebuild_head_from_bodies`] after a deliberate empty recreate
    /// (skips presence probes — much faster for a full rebuild).
    ///
    /// `on_progress(done_bodies, total_bodies, inserted)` is invoked periodically.
    pub fn backfill_head(
        &self,
        on_progress: impl FnMut(u64, u64, u64),
    ) -> Result<u64, StoreError> {
        self.backfill_head_inner(/* force_all */ false, on_progress)
    }

    /// Insert **every** Class A body into `tx.head` without presence probes.
    ///
    /// Used when the head was just created empty (missing-file recovery on open).
    /// Assumes the head is empty or overwrite-safe (same fk re-insert is a no-op).
    pub fn rebuild_head_from_bodies(
        &self,
        on_progress: impl FnMut(u64, u64, u64),
    ) -> Result<u64, StoreError> {
        self.backfill_head_inner(/* force_all */ true, on_progress)
    }

    fn backfill_head_inner(
        &self,
        force_all: bool,
        mut on_progress: impl FnMut(u64, u64, u64),
    ) -> Result<u64, StoreError> {
        let n = self.count();
        if n == 0 {
            return Ok(0);
        }
        let mut inserted = 0u64;
        const CHUNK: usize = 4096;
        const PROGRESS_EVERY: u64 = 50_000;
        let mut batch: Vec<([u8; 32], Fk)> = Vec::with_capacity(CHUNK);
        let mut last_progress = 0u64;
        for id in 1..=n {
            let fk = Fk(id);
            // body_txid only — avoid full packed decode for tens of millions of rows.
            let txid = self.body_txid(fk)?;
            if !force_all {
                let present = self
                    .head
                    .read()
                    .unwrap()
                    .probe_fks(&txid)?
                    .contains(&fk);
                if present {
                    if id - last_progress >= PROGRESS_EVERY || id == n {
                        on_progress(id, n, inserted + batch.len() as u64);
                        last_progress = id;
                    }
                    continue;
                }
            }
            batch.push((txid, fk));
            if batch.len() >= CHUNK {
                inserted += batch.len() as u64;
                self.head_insert_many(&batch)?;
                batch.clear();
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
        if last_progress != n {
            on_progress(n, n, inserted);
        }
        Ok(inserted)
    }

    /// Approximate occupied slots in the hash head (for "needs backfill?" checks).
    pub fn head_occupied(&self) -> u64 {
        self.head.read().unwrap().occupied()
    }

    pub fn head_bits(&self) -> u32 {
        self.head.read().unwrap().bits()
    }

    pub fn head_slots(&self) -> u64 {
        self.head.read().unwrap().slots()
    }

    pub fn head_entry_bytes(&self) -> u8 {
        self.head.read().unwrap().entry_bytes()
    }

    /// No-op capacity reserve (growth is online sequential resize).
    pub fn head_reserve_additional(&self, additional: u64) -> Result<(), StoreError> {
        self.head.read().unwrap().reserve_additional(additional)
    }

    /// Bulk-insert head entries (archive / run materialize / backfill).
    ///
    /// Fast: probe until same fk (idempotent) or empty — **no body_txid** on insert.
    /// May start / advance a sequential online resize (shadow filled from `tx.idx`).
    pub fn head_insert_many(&self, entries: &[([u8; 32], Fk)]) -> Result<(), StoreError> {
        if !entries.is_empty() {
            self.head.read().unwrap().insert_many(entries)?;
        }
        // After live inserts into primary only — never dual-write to shadow.
        self.maybe_start_head_resize()?;
        // Amortize sequential fill with archive batches (cooperative worker).
        self.head_resize_poll(8_192)?;
        Ok(())
    }

    /// True if a sequential head rebuild is in progress.
    pub fn head_resize_in_progress(&self) -> bool {
        self.resize.lock().unwrap().is_some()
    }

    /// Resume incomplete resize from `tx.head.resize` control file (on open).
    fn resume_head_resize_if_needed(&self) -> Result<(), StoreError> {
        let Some(ctrl) = read_resize_control(&self.head_path)? else {
            // Orphan .new without control → drop it.
            let shadow = shadow_head_path(&self.head_path);
            if shadow.exists() {
                let _ = std::fs::remove_file(&shadow);
                let mut m = shadow.clone();
                m.set_extension("new.meta"); // not used; meta is path+".meta"
                let meta = {
                    let mut p = shadow.as_os_str().to_os_string();
                    p.push(".meta");
                    PathBuf::from(p)
                };
                let _ = std::fs::remove_file(meta);
            }
            return Ok(());
        };
        let shadow_path = shadow_head_path(&self.head_path);
        if !shadow_path.exists() {
            // Control without shadow: restart shadow create.
            let shadow = AddressHead::create_with_layout(&shadow_path, ctrl.target)?;
            *self.resize.lock().unwrap() = Some(HeadResize {
                shadow,
                cursor: ctrl.cursor.max(1),
                target: ctrl.target,
            });
            rbitcoin_log::info!(
                "store: resume tx.head resize bits={} entry={}B cursor={}",
                ctrl.target.bits,
                ctrl.target.entry_bytes,
                ctrl.cursor
            );
            return Ok(());
        }
        let shadow = AddressHead::open(&shadow_path)?;
        if shadow.layout() != ctrl.target {
            return Err(StoreError::Corrupt(
                "tx.head.resize layout mismatch vs shadow",
            ));
        }
        *self.resize.lock().unwrap() = Some(HeadResize {
            shadow,
            cursor: ctrl.cursor.max(1),
            target: ctrl.target,
        });
        rbitcoin_log::info!(
            "store: resume tx.head resize bits={} entry={}B cursor={}",
            ctrl.target.bits,
            ctrl.target.entry_bytes,
            ctrl.cursor
        );
        Ok(())
    }

    /// Start BITS+1 sequential rebuild when load ≥ [`crate::address_head::HEAD_LOAD_START`].
    pub fn maybe_start_head_resize(&self) -> Result<(), StoreError> {
        {
            let rg = self.resize.lock().unwrap();
            if rg.is_some() {
                // Warn if primary load is high while resizing.
                let (slots, n) = {
                    let h = self.head.read().unwrap();
                    (h.slots(), self.count())
                };
                let ratio = load_ratio(n, slots);
                if ratio >= HEAD_LOAD_WARN {
                    rbitcoin_log::warn!(
                        "store: tx.head resize lagging load={ratio:.3} n={n} slots={slots}"
                    );
                }
                return Ok(());
            }
        }
        let (bits, slots, n) = {
            let h = self.head.read().unwrap();
            (h.bits(), h.slots(), self.count())
        };
        if bits >= MAX_BITS {
            return Ok(());
        }
        if !load_needs_resize(n, slots) {
            return Ok(());
        }
        let new_bits = bits + 1;
        let target = HeadLayout::new(new_bits)?;
        self.start_head_resize(target)
    }

    /// Force start of sequential rebuild to `target` (tests / operator).
    pub fn start_head_resize(&self, target: HeadLayout) -> Result<(), StoreError> {
        let mut rg = self.resize.lock().unwrap();
        if rg.is_some() {
            return Ok(());
        }
        let cur_bits = self.head.read().unwrap().bits();
        if target.bits <= cur_bits && target.entry_bytes <= self.head.read().unwrap().entry_bytes()
        {
            // Only widen.
            if target.bits < cur_bits {
                return Err(StoreError::Corrupt("tx.head resize must widen bits"));
            }
        }
        let shadow_path = shadow_head_path(&self.head_path);
        let _ = std::fs::remove_file(&shadow_path);
        {
            let mut p = shadow_path.as_os_str().to_os_string();
            p.push(".meta");
            let _ = std::fs::remove_file(PathBuf::from(p));
        }
        let shadow = AddressHead::create_with_layout(&shadow_path, target)?;
        let gen = self.head.read().unwrap().generation();
        let ctrl = ResizeControl {
            target,
            cursor: 1,
            generation: gen,
        };
        write_resize_control(&self.head_path, &ctrl)?;
        rbitcoin_log::info!(
            "store: tx.head resize start {}→{} bits entry={}B n={} slots_old={}",
            cur_bits,
            target.bits,
            target.entry_bytes,
            self.count(),
            self.head.read().unwrap().slots()
        );
        *rg = Some(HeadResize {
            shadow,
            cursor: 1,
            target,
        });
        Ok(())
    }

    /// Advance sequential shadow fill by up to `budget` Class A fks; swap when caught up.
    ///
    /// Does **not** dual-write live inserts — only `tx.idx` order into shadow.
    pub fn head_resize_poll(&self, budget: u64) -> Result<(), StoreError> {
        if budget == 0 {
            return Ok(());
        }
        let n = self.count();
        let mut done_fill = false;
        {
            let mut rg = self.resize.lock().unwrap();
            let Some(r) = rg.as_mut() else {
                return Ok(());
            };
            if r.cursor == 0 {
                r.cursor = 1;
            }
            if n == 0 || r.cursor > n {
                done_fill = true;
            } else {
                let end = (r.cursor + budget - 1).min(n);
                let mut batch: Vec<([u8; 32], Fk)> = Vec::with_capacity(256);
                for id in r.cursor..=end {
                    let fk = Fk(id);
                    let txid = self.body_txid(fk)?;
                    batch.push((txid, fk));
                    if batch.len() >= 256 {
                        r.shadow.insert_many(&batch)?;
                        batch.clear();
                    }
                }
                if !batch.is_empty() {
                    r.shadow.insert_many(&batch)?;
                }
                r.cursor = end + 1;
                write_resize_control(
                    &self.head_path,
                    &ResizeControl {
                        target: r.target,
                        cursor: r.cursor,
                        generation: self.head.read().unwrap().generation(),
                    },
                )?;
                if r.cursor > n {
                    done_fill = true;
                }
            }
        }
        if done_fill {
            self.try_complete_head_resize()?;
        }
        Ok(())
    }

    /// Final catch-up under primary insert lock, then atomic rename swap.
    fn try_complete_head_resize(&self) -> Result<(), StoreError> {
        // Exclusive: pause primary head inserts, catch up shadow, swap files.
        let mut head_w = self.head.write().unwrap();
        let primary_writes = head_w.lock_writes();
        let n = self.count();
        let mut rg = self.resize.lock().unwrap();
        let Some(r) = rg.as_mut() else {
            return Ok(());
        };
        if r.cursor <= n {
            let mut batch: Vec<([u8; 32], Fk)> = Vec::with_capacity(256);
            for id in r.cursor..=n {
                let fk = Fk(id);
                let txid = self.body_txid(fk)?;
                batch.push((txid, fk));
                if batch.len() >= 256 {
                    r.shadow.insert_many(&batch)?;
                    batch.clear();
                }
            }
            if !batch.is_empty() {
                r.shadow.insert_many(&batch)?;
            }
            r.cursor = n + 1;
        }
        // Body may have grown if archive appends without head insert; re-check once.
        let n2 = self.count();
        if r.cursor <= n2 {
            let mut batch: Vec<([u8; 32], Fk)> = Vec::with_capacity(256);
            for id in r.cursor..=n2 {
                let fk = Fk(id);
                let txid = self.body_txid(fk)?;
                batch.push((txid, fk));
                if batch.len() >= 256 {
                    r.shadow.insert_many(&batch)?;
                    batch.clear();
                }
            }
            if !batch.is_empty() {
                r.shadow.insert_many(&batch)?;
            }
            r.cursor = n2 + 1;
        }
        r.shadow.flush()?;
        let target = r.target;
        let new_gen = head_w.generation().saturating_add(1);
        let shadow_path = shadow_head_path(&self.head_path);
        let bak = bak_head_path(&self.head_path);
        let _ = std::fs::remove_file(&bak);
        let _shadow = match rg.take() {
            Some(s) => s.shadow,
            None => return Ok(()),
        };
        drop(_shadow);
        drop(primary_writes);
        // head_w still held — primary mmap open on old path; Linux allows rename.
        std::fs::rename(&self.head_path, &bak).map_err(|e| StoreError::io(&self.head_path, e))?;
        std::fs::rename(&shadow_path, &self.head_path)
            .map_err(|e| StoreError::io(&shadow_path, e))?;
        {
            let mut old_meta = shadow_path.as_os_str().to_os_string();
            old_meta.push(".meta");
            let old_meta = PathBuf::from(old_meta);
            let mut new_meta = self.head_path.as_os_str().to_os_string();
            new_meta.push(".meta");
            let new_meta = PathBuf::from(new_meta);
            let _ = std::fs::rename(&old_meta, &new_meta);
        }
        write_head_meta(&self.head_path, target, new_gen)?;
        clear_resize_control(&self.head_path);
        *head_w = AddressHead::open(&self.head_path)?;
        let _ = std::fs::remove_file(&bak);
        {
            let mut bak_meta = bak.as_os_str().to_os_string();
            bak_meta.push(".meta");
            let _ = std::fs::remove_file(PathBuf::from(bak_meta));
        }
        rbitcoin_log::info!(
            "store: tx.head resize complete bits={} entry={}B slots={} gen={}",
            target.bits,
            target.entry_bytes,
            head_w.slots(),
            new_gen
        );
        Ok(())
    }

    pub fn flush(&self) -> Result<(), StoreError> {
        self.body.flush()?;
        self.head.read().unwrap().flush()?;
        if let Some(r) = self.resize.lock().unwrap().as_ref() {
            r.shadow.flush()?;
        }
        Ok(())
    }

    pub fn flush_async(&self) -> Result<(), StoreError> {
        self.body.flush_async()?;
        self.head.read().unwrap().flush_async()?;
        if let Some(r) = self.resize.lock().unwrap().as_ref() {
            r.shadow.flush_async()?;
        }
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
            create_fk: Fk(1),
            prev_index: 3,
            sequence: 0xffff_fffe,
            script_sig: vec![0xab; 40],
            witness: vec![vec![0x30; 70], vec![0x21; 33]],
        };
        let enc = rec.encode();
        let (cfk, vout, used) = InputRecord::decode_prevout_at(&enc).unwrap();
        assert_eq!(cfk, Fk(1));
        assert_eq!(vout, 3);
        assert_eq!(used, enc.len());
        // Full decode still matches.
        let (full, used2) = InputRecord::decode_at(&enc).unwrap();
        assert_eq!(used2, used);
        assert_eq!(full.script_sig.len(), 40);
    }

    /// v10: non-coinbase prev is create_fk(8) + vout, not prev_txid(32) (−24 B).
    #[test]
    fn input_encode_create_fk_not_prev_txid() {
        let rec = InputRecord {
            prev_txid: [0xaa; 32], // soft only — not on disk
            create_fk: Fk(42),
            prev_index: 7,
            sequence: u32::MAX,
            script_sig: vec![],
            witness: vec![],
        };
        let enc = rec.encode();
        // flags(1) + create_fk(8) + compact vout(1 for 7) = 10
        assert_eq!(enc.len(), 10, "enc={:?}", enc);
        // v9 would have been flags + 32-byte txid + vout = 34 for same case
        assert!(enc.len() + 24 <= 34);
        let dec = InputRecord::decode(&enc).unwrap();
        assert_eq!(dec.create_fk, Fk(42));
        assert_eq!(dec.prev_index, 7);
        assert_eq!(dec.prev_txid, [0u8; 32]);
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
            create_fk: Fk::NULL,
                prev_index: u32::MAX,
                sequence: u32::MAX,
                script_sig: vec![0x01],
                witness: vec![],
            },
            InputRecord {
                prev_txid: [3u8; 32],
            create_fk: Fk(1),
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
        assert_eq!(prevouts[0], (Fk::NULL, u32::MAX));
        assert_eq!(prevouts[1], (Fk(1), 1));
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
            create_fk: Fk::NULL,
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

    /// After fast head insert (no write-time BIP30 displace), sole lookup must
    /// prefer the **newer** create (deeper on the probe chain).
    #[test]
    fn get_fk_by_txid_prefers_newer_bip30_duplicate() {
        let dir = std::env::temp_dir().join(format!(
            "rbitcoin-tx-bip30-get-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        std::env::set_var("RBITCOIN_HEAD_SCALE", "tiny");
        let t = TxTable::create(&dir).unwrap();
        let txid = [0x55u8; 32];
        let mk = |fk_hint: u8| {
            let rec = TxRecord {
                txid,
                version: 1,
                locktime: 0,
                input_start_fk: Fk::NULL,
                input_count: 1,
                output_start_fk: Fk::NULL,
                output_count: 1,
            };
            let inputs = vec![InputRecord {
                prev_txid: [0u8; 32],
            create_fk: Fk::NULL,
                prev_index: u32::MAX,
                sequence: u32::MAX,
                script_sig: vec![fk_hint],
                witness: vec![],
            }];
            let outputs = vec![OutputRecord::unspent(1, vec![0x51])];
            (rec, inputs, outputs)
        };
        // Two packed bodies, same txid (BIP30-shaped). Distinct script_sig so
        // bodies differ but body_txid still reads the shared txid prefix.
        let a = mk(1);
        let b = mk(2);
        let fk1 = t.put_full_batch_indexed(&[a], true).unwrap()[0];
        let fk2 = t.put_full_batch_indexed(&[b], true).unwrap()[0];
        assert!(fk2.0 > fk1.0);
        // Probe order: older first, newer deeper.
        let cands = t.head.read().unwrap().probe_fks(&txid).unwrap();
        assert_eq!(cands[0], fk1);
        assert!(cands.contains(&fk2));
        // Sole resolve prefers newest.
        assert_eq!(t.get_fk_by_txid(&txid).unwrap(), Some(fk2));
        let batch = t.get_fk_by_txid_batch(&[txid]).unwrap();
        assert_eq!(batch[0].1, Some(fk2));
        let all = t.get_all_by_txid(&txid).unwrap();
        assert_eq!(all.len(), 2);
        assert_eq!(all[0].0, fk2);
        assert_eq!(all[1].0, fk1);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Sequential online resize: shadow filled from dense fk order; primary-only
    /// inserts during fill; post-swap all txids resolve.
    #[test]
    fn head_sequential_resize_widens_bits() {
        let dir = std::env::temp_dir().join(format!(
            "rbitcoin-tx-head-resize-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        // Tiny head: 2^10 = 1024 slots.
        std::env::set_var("RBITCOIN_TX_HEAD_BITS", "10");
        let t = TxTable::create(&dir).unwrap();
        assert_eq!(t.head_bits(), 10);
        assert_eq!(t.head_entry_bytes(), 4);

        let mk = |i: u64| {
            let mut txid = [0u8; 32];
            txid[0..8].copy_from_slice(&i.to_le_bytes());
            let rec = TxRecord {
                txid,
                version: 1,
                locktime: 0,
                input_start_fk: Fk::NULL,
                input_count: 1,
                output_start_fk: Fk::NULL,
                output_count: 1,
            };
            let inputs = vec![InputRecord {
                prev_txid: [0u8; 32],
                create_fk: Fk::NULL,
                prev_index: u32::MAX,
                sequence: u32::MAX,
                script_sig: vec![],
                witness: vec![],
            }];
            let outputs = vec![OutputRecord::unspent(1, vec![0x51])];
            (rec, inputs, outputs)
        };

        // Seed some bodies + head entries.
        for i in 1..=50u64 {
            let _ = t.put_full_batch_indexed(&[mk(i)], true).unwrap();
        }
        assert_eq!(t.count(), 50);
        assert!(!t.head_resize_in_progress());

        // Force widen 10 → 11.
        t.start_head_resize(crate::address_head::HeadLayout::new(11).unwrap())
            .unwrap();
        assert!(t.head_resize_in_progress());

        // Concurrent primary inserts while resizing (no dual-write).
        for i in 51..=80u64 {
            let _ = t.put_full_batch_indexed(&[mk(i)], true).unwrap();
        }
        // Drive fill to completion.
        for _ in 0..32 {
            if !t.head_resize_in_progress() {
                break;
            }
            t.head_resize_poll(10_000).unwrap();
        }
        assert!(
            !t.head_resize_in_progress(),
            "resize should complete"
        );
        assert_eq!(t.head_bits(), 11);
        assert_eq!(t.count(), 80);
        for i in 1..=80u64 {
            let mut txid = [0u8; 32];
            txid[0..8].copy_from_slice(&i.to_le_bytes());
            let fk = t.get_fk_by_txid(&txid).unwrap();
            assert_eq!(fk, Some(Fk(i)), "txid {i} missing after resize");
        }
        let _ = std::fs::remove_dir_all(&dir);
        std::env::remove_var("RBITCOIN_TX_HEAD_BITS");
    }

    #[test]
    fn head_load_trigger_starts_resize() {
        let dir = std::env::temp_dir().join(format!(
            "rbitcoin-tx-head-load-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        // 2^8 = 256 slots; trigger at ceil(0.80*256)=205.
        std::env::set_var("RBITCOIN_TX_HEAD_BITS", "8");
        let t = TxTable::create(&dir).unwrap();
        assert_eq!(t.head_slots(), 256);
        let mk = |i: u64| {
            let mut txid = [0u8; 32];
            txid[0..8].copy_from_slice(&i.to_le_bytes());
            let rec = TxRecord {
                txid,
                version: 1,
                locktime: 0,
                input_start_fk: Fk::NULL,
                input_count: 1,
                output_start_fk: Fk::NULL,
                output_count: 1,
            };
            let inputs = vec![InputRecord {
                prev_txid: [0u8; 32],
                create_fk: Fk::NULL,
                prev_index: u32::MAX,
                sequence: u32::MAX,
                script_sig: vec![],
                witness: vec![],
            }];
            let outputs = vec![OutputRecord::unspent(1, vec![0x51])];
            (rec, inputs, outputs)
        };
        for i in 1..=210u64 {
            let _ = t.put_full_batch_indexed(&[mk(i)], true).unwrap();
        }
        // head_insert_many should have started resize; poll until done.
        for _ in 0..64 {
            if !t.head_resize_in_progress() && t.head_bits() >= 9 {
                break;
            }
            t.head_resize_poll(10_000).unwrap();
            t.maybe_start_head_resize().unwrap();
        }
        assert!(t.head_bits() >= 9, "bits={}", t.head_bits());
        // Spot-check resolves.
        for i in [1u64, 100, 210] {
            let mut txid = [0u8; 32];
            txid[0..8].copy_from_slice(&i.to_le_bytes());
            assert_eq!(t.get_fk_by_txid(&txid).unwrap(), Some(Fk(i)));
        }
        let _ = std::fs::remove_dir_all(&dir);
        std::env::remove_var("RBITCOIN_TX_HEAD_BITS");
    }

    #[test]
    fn get_fk_by_txid_batch_matches_single() {
        let dir = std::env::temp_dir().join(format!(
            "rbitcoin-tx-batch-fk-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        std::env::set_var("RBITCOIN_HEAD_SCALE", "tiny");
        let t = TxTable::create(&dir).unwrap();
        let mut items = Vec::new();
        for i in 0u8..5 {
            let mut txid = [0u8; 32];
            txid[0] = i.wrapping_add(1);
            let tx = TxRecord {
                txid,
                version: 1,
                locktime: 0,
                input_start_fk: Fk::NULL,
                input_count: 1,
                output_start_fk: Fk::NULL,
                output_count: 1,
            };
            let inputs = vec![InputRecord {
                prev_txid: [0u8; 32],
            create_fk: Fk::NULL,
                prev_index: u32::MAX,
                sequence: u32::MAX,
                script_sig: vec![0x01],
                witness: vec![],
            }];
            let outputs = vec![OutputRecord::unspent(1, vec![0x51])];
            items.push((tx, inputs, outputs));
        }
        let fks = t.put_full_batch_indexed(&items, true).unwrap();
        let mut keys: Vec<[u8; 32]> = items.iter().map(|(tx, _, _)| tx.txid).collect();
        keys.sort_unstable_by_key(|k| t.head_primary_slot(k));
        let batch = t.get_fk_by_txid_batch(&keys).unwrap();
        assert_eq!(batch.len(), 5);
        for (txid, fk) in &batch {
            let single = t.get_fk_by_txid(txid).unwrap();
            assert_eq!(*fk, single);
            assert!(fk.is_some());
        }
        // Miss
        let miss = t.get_fk_by_txid_batch(&[[0xff; 32]]).unwrap();
        assert_eq!(miss[0].1, None);
        let _ = fks;
        let _ = std::fs::remove_dir_all(&dir);
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
        std::env::remove_var("RBITCOIN_HEAD_SCALE");
    }

    /// Operator recovery: delete `tx.head` → open rebuilds from Class A bodies.
    #[test]
    fn missing_tx_head_rebuilds_from_bodies_on_open() {
        let dir = std::env::temp_dir().join(format!(
            "rbitcoin-tx-head-rebuild-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        std::env::set_var("RBITCOIN_HEAD_SCALE", "tiny");

        let mk = |i: u64| {
            let mut txid = [0u8; 32];
            txid[0..8].copy_from_slice(&i.to_le_bytes());
            let rec = TxRecord {
                txid,
                version: 1,
                locktime: 0,
                input_start_fk: Fk::NULL,
                input_count: 1,
                output_start_fk: Fk::NULL,
                output_count: 1,
            };
            let inputs = vec![InputRecord {
                prev_txid: [0u8; 32],
                create_fk: Fk::NULL,
                prev_index: u32::MAX,
                sequence: u32::MAX,
                script_sig: vec![],
                witness: vec![],
            }];
            let outputs = vec![OutputRecord::unspent(1, vec![0x51])];
            (rec, inputs, outputs)
        };

        {
            let t = TxTable::create(&dir).unwrap();
            for i in 1..=20u64 {
                let _ = t.put_full_batch_indexed(&[mk(i)], true).unwrap();
            }
            assert_eq!(t.count(), 20);
            // Sanity: head resolves before delete.
            let mut txid = [0u8; 32];
            txid[0..8].copy_from_slice(&7u64.to_le_bytes());
            assert_eq!(t.get_fk_by_txid(&txid).unwrap(), Some(Fk(7)));
            t.flush().unwrap();
        }

        // Simulate operator: wipe head (+ meta).
        let head = dir.join("tx.head");
        assert!(head.exists());
        std::fs::remove_file(&head).unwrap();
        let mut meta = head.as_os_str().to_os_string();
        meta.push(".meta");
        let _ = std::fs::remove_file(std::path::PathBuf::from(meta));

        // Reopen must recreate head and rebuild from bodies.
        let t = TxTable::open(&dir).unwrap();
        assert_eq!(t.count(), 20);
        assert!(dir.join("tx.head").exists());
        for i in 1..=20u64 {
            let mut txid = [0u8; 32];
            txid[0..8].copy_from_slice(&i.to_le_bytes());
            assert_eq!(
                t.get_fk_by_txid(&txid).unwrap(),
                Some(Fk(i)),
                "txid {i} missing after head rebuild"
            );
        }
        let _ = std::fs::remove_dir_all(&dir);
        std::env::remove_var("RBITCOIN_HEAD_SCALE");
    }

    #[test]
    fn missing_tx_head_with_no_bodies_creates_empty() {
        let dir = std::env::temp_dir().join(format!(
            "rbitcoin-tx-head-empty-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        std::env::set_var("RBITCOIN_HEAD_SCALE", "tiny");
        {
            let t = TxTable::create(&dir).unwrap();
            t.flush().unwrap();
        }
        std::fs::remove_file(dir.join("tx.head")).unwrap();
        let t = TxTable::open(&dir).unwrap();
        assert_eq!(t.count(), 0);
        assert!(dir.join("tx.head").exists());
        let _ = std::fs::remove_dir_all(&dir);
        std::env::remove_var("RBITCOIN_HEAD_SCALE");
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
            create_fk: Fk::NULL,
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
            create_fk: Fk::NULL,
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
            create_fk: Fk(1),
            prev_index: 2,
            sequence: 0xffff_fffe,
            script_sig: vec![0x00],
            witness: vec![vec![0x30, 0x01], vec![0x21, 0xaa]],
        };
        let enc = rec.encode();
        let dec = InputRecord::decode(&enc).unwrap();
        assert_eq!(dec.create_fk, Fk(1));
        assert_eq!(dec.prev_index, 2);
        assert_eq!(dec.sequence, rec.sequence);
        assert_eq!(dec.script_sig, rec.script_sig);
        assert_eq!(dec.witness, rec.witness);
        assert_eq!(dec.prev_txid, [0u8; 32], "prev_txid not on disk");
    }

    #[test]
    fn input_flags_roundtrip() {
        let rec = InputRecord {
            prev_txid: [0u8; 32],
            create_fk: Fk::NULL,
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
        let flags = input_flags::RESERVED4
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
            create_fk: Fk::NULL,
                prev_index: u32::MAX,
                sequence: u32::MAX,
                script_sig: vec![0x01],
                witness: vec![],
            },
            InputRecord {
                prev_txid: [2u8; 32],
            create_fk: Fk(1),
                prev_index: 0,
                sequence: 1,
                script_sig: vec![],
                witness: vec![vec![0xab]],
            },
            InputRecord {
                prev_txid: [3u8; 32],
            create_fk: Fk(1),
                prev_index: 3,
                sequence: u32::MAX,
                script_sig: vec![],
                witness: vec![],
            },
        ];
        let mut enc = Vec::new();
        encode_input_run(&run, &mut enc);
        let dec = decode_input_run(&enc, 3).unwrap();
        assert_eq!(dec.len(), 3);
        assert!(dec[0].is_coinbase());
        assert_eq!(dec[1].create_fk, Fk(1));
        assert_eq!(dec[1].prev_index, 0);
        assert_eq!(dec[1].witness, vec![vec![0xab]]);
        assert_eq!(dec[2].create_fk, Fk(1));
        assert_eq!(dec[2].prev_index, 3);
        // Soft prev_txid not on disk.
        assert_eq!(dec[1].prev_txid, [0u8; 32]);
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
            create_fk: Fk::NULL,
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
            create_fk: Fk::NULL,
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
