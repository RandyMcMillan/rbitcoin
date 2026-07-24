use crate::address_head::{
    bak_head_path, clear_resize_control, is_probe_exhausted_error, load_needs_resize,
    load_ratio, read_resize_control, shadow_head_path, take_probe_depth_resize_request,
    write_head_meta, write_resize_control, AddressHead, HeadLayout, ResizeControl,
    HEAD_LOAD_WARN, MAX_BITS,
};
use crate::compact::{
    input_flags, output_flags, read_compact_size, read_uleb128, write_compact_size, write_uleb128,
};
use crate::error::StoreError;
use crate::var_table::VarTable;
use rbitcoin_primitives::{Fk, TableKind};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::sync::{Mutex, RwLock};
use std::thread::JoinHandle;
use std::time::Duration;

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
/// Same body IO as [`decode_packed_tx`]; cheaper CPU for cache parent loads
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

/// Class A fks the background resize thread advances per `head_resize_poll`.
const RESIZE_BG_POLL_BUDGET: u64 = 1_048_576;

pub struct TxTable {
    body: VarTable,
    head: RwLock<AddressHead>,
    /// Directory containing `tx.head` (for rename / control paths).
    head_path: PathBuf,
    resize: Mutex<Option<HeadResize>>,
    /// Background sequential fill for `tx.head.new` (independent of archive inserts).
    resize_bg: Mutex<Option<JoinHandle<()>>>,
    /// Generation of the live bg worker; bump to ask a previous worker to exit.
    resize_bg_gen: AtomicU64,
}

impl Drop for TxTable {
    fn drop(&mut self) {
        // Stop + join bg fill so we never outlive `self` (worker holds `*const TxTable`).
        self.resize_bg_gen
            .fetch_add(1, AtomicOrdering::AcqRel);
        if let Some(h) = self.resize_bg.lock().unwrap().take() {
            let _ = h.join();
        }
    }
}

impl TxTable {
    pub fn create(dir: &Path) -> Result<Self, StoreError> {
        let head_path = dir.join("tx.head");
        Ok(Self {
            body: VarTable::create(dir, "tx", TableKind::Tx)?,
            head: RwLock::new(AddressHead::create(&head_path)?),
            head_path,
            resize: Mutex::new(None),
            resize_bg: Mutex::new(None),
            resize_bg_gen: AtomicU64::new(0),
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
            resize_bg: Mutex::new(None),
            resize_bg_gen: AtomicU64::new(0),
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

    /// Ensure a dedicated OS thread is continuously filling `tx.head.new`.
    ///
    /// Fill must **not** depend on the archive writer sleeping between probe-exhaust
    /// retries — that made multi‑hour resizes crawl at a few k fks/s of wall time.
    fn ensure_resize_bg_running(&self) {
        if !self.head_resize_in_progress() {
            return;
        }
        let mut slot = self.resize_bg.lock().unwrap();
        if let Some(h) = slot.as_ref() {
            if !h.is_finished() {
                return;
            }
            if let Some(h) = slot.take() {
                let _ = h.join();
            }
        }
        if !self.head_resize_in_progress() {
            return;
        }
        // fetch_add returns previous; worker watches for this generation.
        let gen = self
            .resize_bg_gen
            .fetch_add(1, AtomicOrdering::AcqRel)
            + 1;

        // SAFETY: worker exits when gen changes or resize completes; Drop joins
        // before TxTable is deallocated (Store owns TxTable for process life).
        let this = self as *const TxTable as usize;
        let handle = std::thread::Builder::new()
            .name("rbitcoin-tx-head-resize".into())
            .spawn(move || {
                // SAFETY: `this` is a live TxTable for the lifetime of this join.
                let table = unsafe { &*(this as *const TxTable) };
                rbitcoin_log::info!(
                    "store: tx.head resize background fill started (budget={RESIZE_BG_POLL_BUDGET}/wave)"
                );
                loop {
                    if table.resize_bg_gen.load(AtomicOrdering::Acquire) != gen {
                        break;
                    }
                    if !table.head_resize_in_progress() {
                        break;
                    }
                    match table.head_resize_poll(RESIZE_BG_POLL_BUDGET) {
                        Ok(()) => {}
                        Err(e) => {
                            rbitcoin_log::error!(
                                "store: tx.head resize background fill error: {e} — retry in 1s"
                            );
                            std::thread::sleep(Duration::from_secs(1));
                            continue;
                        }
                    }
                    // Continuous: no sleep between successful waves.
                }
                rbitcoin_log::info!("store: tx.head resize background fill exited");
            })
            .unwrap_or_else(|e| {
                panic!("store: failed to spawn tx.head resize thread: {e}");
            });
        *slot = Some(handle);
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













    /// Meta + input prevouts only (no script/witness allocation, no outputs).
    ///
    /// Used by load: discover parents without full parse into RAM.
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

    /// Read Class A body **txid only** (packed prefix or bare TxRecord).
    ///
    /// Thin I/O: idx range + first **33** body bytes (magic+txid) — does **not**
    /// load scripts/inputs/witness. Used by head resolve (`get_fk_by_txid*`) and
    /// archive sticky prewarm.
    ///
    /// Public for wire rebuild / archive sticky: schema v10 inputs store
    /// `create_fk` only; callers fill soft `prev_txid` from the create body.
    pub fn body_txid(&self, fk: Fk) -> Result<[u8; 32], StoreError> {
        use std::time::Instant;
        // Packed: [PACKED_TX_V1][txid;32]…  Bare legacy: [txid;32]…
        let t_idx = Instant::now();
        let (off, len) = self.body.record_range(fk)?;
        crate::head_resolve_stats::add_idx(t_idx.elapsed().as_nanos() as u64);
        let t_body = Instant::now();
        let mut prefix = [0u8; 33];
        let n = self.body.read_prefix_at(off, len, &mut prefix)?;
        crate::head_resolve_stats::add_body(t_body.elapsed().as_nanos() as u64);
        crate::head_resolve_stats::add_body_lookups(1);
        Self::txid_from_body_prefix(&prefix[..n])
    }

    /// Bulk Class A body txids for consecutive ids `first..=last` (1-based).
    ///
    /// 1. One sequential `tx.idx` pread for the range ([`VarTable::record_ranges`]).
    /// 2. Parallel / io_uring preads of the leading **33** body bytes per record
    ///    ([`crate::bulk_io::pread_batch`]).
    /// 3. Parse each prefix to a txid (same rules as [`Self::body_txid`]).
    ///
    /// Used by online `tx.head` shadow fill so resize is not one pread per fk.
    pub fn body_txid_range(&self, first: u64, last: u64) -> Result<Vec<[u8; 32]>, StoreError> {
        use crate::bulk_io::{self, ReadOp};
        if last < first {
            return Ok(Vec::new());
        }
        let ranges = self.body.record_ranges(first, last)?;
        let n = ranges.len();
        if n == 0 {
            return Ok(Vec::new());
        }
        let body_fd = self.body.body_read_fd();
        let body_pub = self.body.body_published_len();
        let body_path = self.body.body_file_path();

        // Fixed 33-byte prefix buffers (packed magic+txid, or bare txid).
        let mut prefixes: Vec<[u8; 33]> = vec![[0u8; 33]; n];
        let mut prefix_lens: Vec<usize> = vec![0; n];
        for (i, &(off, len)) in ranges.iter().enumerate() {
            let want = (len as usize).min(33);
            if want == 0 {
                return Err(StoreError::Corrupt("empty body for txid"));
            }
            if off.saturating_add(want as u64) > body_pub {
                return Err(StoreError::Corrupt("body past published"));
            }
            prefix_lens[i] = want;
        }

        // SAFETY: each `prefixes[i]` is a distinct stack-slot allocation in the
        // vec; submitted indices are unique so mut slices do not alias.
        let mut read_ops: Vec<ReadOp<'_>> = Vec::with_capacity(n);
        for i in 0..n {
            let want = prefix_lens[i];
            let ptr = prefixes[i].as_mut_ptr();
            let slice = unsafe { std::slice::from_raw_parts_mut(ptr, want) };
            read_ops.push(ReadOp {
                fd: body_fd,
                offset: ranges[i].0,
                buf: slice,
                result: i32::MIN,
            });
        }
        bulk_io::pread_batch(&mut read_ops);

        let mut out = Vec::with_capacity(n);
        for (i, ro) in read_ops.iter().enumerate() {
            let want = prefix_lens[i];
            if ro.result < 0 {
                return Err(StoreError::io(
                    body_path,
                    std::io::Error::from_raw_os_error(-ro.result),
                ));
            }
            if ro.result as usize != want {
                return Err(StoreError::Corrupt("bulk body_txid pread short"));
            }
            out.push(Self::txid_from_body_prefix(&prefixes[i][..want])?);
        }
        crate::head_resolve_stats::add_body_lookups(n as u64);
        Ok(out)
    }

    /// Parse txid from the leading bytes of a Class A body payload.
    #[inline]
    fn txid_from_body_prefix(raw: &[u8]) -> Result<[u8; 32], StoreError> {
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

    /// Read Class A body txid from a known range (no idx). Thin: first 33 bytes only.
    pub fn body_txid_at(&self, offset: u64, len: u64) -> Result<[u8; 32], StoreError> {
        let mut prefix = [0u8; 33];
        let n = self.body.read_prefix_at(offset, len, &mut prefix)?;
        Self::txid_from_body_prefix(&prefix[..n])
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
        use std::time::Instant;
        let t_probe = Instant::now();
        let cands = self.head.read().unwrap().probe_fks(txid)?;
        crate::head_resolve_stats::add_probe(t_probe.elapsed().as_nanos() as u64);
        crate::head_resolve_stats::add_keys(1);
        crate::head_resolve_stats::add_cands(cands.len() as u64);
        for fk in cands.into_iter().rev() {
            if self.body_txid(fk)? == *txid {
                return Ok(Some(fk));
            }
        }
        Ok(None)
    }

    /// Batch head resolve (archive prep bulk path).
    ///
    /// **Probe:** one full **page** pread per key (io_uring / parallel), then
    /// in-page double-hash hop in RAM. **Then** idx + body-prefix waves for all
    /// candidates. BIP30: deepest matching body wins.
    ///
    /// Timers: [`crate::head_resolve_stats`] probe / idx / body (per-wave wall).
    pub fn get_fk_by_txid_batch(
        &self,
        txids: &[[u8; 32]],
    ) -> Result<Vec<([u8; 32], Option<Fk>)>, StoreError> {
        use crate::address_head::{
            h1_in_page, h2_in_page, hop_scan_page, page_file_off, page_pread_len,
            page_slot_count, MAX_PROBE,
        };
        use crate::bulk_io::{self, ReadOp};
        use std::time::Instant;
        if txids.is_empty() {
            return Ok(Vec::new());
        }
        crate::head_resolve_stats::add_keys(txids.len() as u64);

        // Snapshot head geometry + fd under a **brief** read lock, then drop.
        // Holding `head.read()` for the whole batch starves resize swap.
        let (bits, entry_bytes, head_fd, head_pub, head_path, head_slots) = {
            let head = self.head.read().unwrap();
            (
                head.bits(),
                head.entry_bytes(),
                head.read_fd(),
                head.published_len(),
                head.path_str().to_path_buf(),
                head.slots(),
            )
        };
        let idx_fd = self.body.idx_read_fd();
        let body_fd = self.body.body_read_fd();
        let idx_path = self.body.idx_file_path();
        let body_path = self.body.body_file_path();
        let body_pub = self.body.body_published_len();
        let idx_pub = self.body.idx_published_len();
        let count = self.body.count();
        let body_logical = self.body.body_logical_len().max(crate::file::FILE_HEADER_LEN as u64);
        let es = entry_bytes;
        let page_slots = page_slot_count(bits);

        // --- Phase 1: one page pread per key ---
        let t_probe = Instant::now();
        let wants: Vec<usize> = txids
            .iter()
            .map(|t| page_pread_len(t, bits, es, head_slots, head_pub))
            .collect();
        let offs: Vec<u64> = txids
            .iter()
            .map(|t| page_file_off(t, bits, es))
            .collect();

        let mut cands_by_key: Vec<Vec<(u32, u64)>> = vec![Vec::new(); txids.len()];
        let mut cands_total = 0u64;

        let io_keys: Vec<usize> = wants
            .iter()
            .enumerate()
            .filter(|(_, w)| **w > 0)
            .map(|(i, _)| i)
            .collect();
        if !io_keys.is_empty() {
            let total: usize = io_keys.iter().map(|&i| wants[i]).sum();
            let mut arena = vec![0u8; total.max(1)];
            let mut spans: Vec<(usize, usize, usize)> = Vec::with_capacity(io_keys.len());
            let mut aoff = 0usize;
            for &i in &io_keys {
                let w = wants[i];
                spans.push((i, w, aoff));
                aoff += w;
            }
            let mut read_ops: Vec<ReadOp<'_>> = Vec::with_capacity(spans.len());
            for &(i, w, a) in &spans {
                let ptr = arena[a..a + w].as_mut_ptr();
                let slice = unsafe { std::slice::from_raw_parts_mut(ptr, w) };
                read_ops.push(ReadOp {
                    fd: head_fd,
                    offset: offs[i],
                    buf: slice,
                    result: i32::MIN,
                });
            }
            bulk_io::pread_batch(&mut read_ops);
            for (ro, &(key_i, want, a)) in read_ops.iter().zip(spans.iter()) {
                if ro.result < 0 {
                    return Err(StoreError::io(
                        &head_path,
                        std::io::Error::from_raw_os_error(-ro.result),
                    ));
                }
                let got = (ro.result as usize).min(want);
                let esz = es as usize;
                let n = (got / esz) * esz;
                let buf = &arena[a..a + n];
                let txid = &txids[key_i];
                let h1p = h1_in_page(txid, bits);
                let h2p = h2_in_page(txid, bits);
                let scan = hop_scan_page(buf, es, h1p, h2p, page_slots, MAX_PROBE);
                for &(d, fk) in &scan.cands {
                    cands_by_key[key_i].push((d, fk));
                    cands_total = cands_total.saturating_add(1);
                }
            }
        }
        crate::head_resolve_stats::add_probe(t_probe.elapsed().as_nanos() as u64);

        // --- Phase 2: idx + body for every candidate ---
        #[derive(Clone, Copy)]
        enum Stage {
            Idx { depth: u8, fk: u64 },
            Body {
                depth: u8,
                fk: u64,
                off: u64,
                len: u64,
            },
        }

        struct Op {
            key_i: u32,
            stage: Stage,
            buf: [u8; 33],
            buf_len: u8,
            file_off: u64,
            fd: std::os::fd::RawFd,
            range: (u64, u64),
            prefix_n: usize,
            dead: bool,
            err: Option<StoreError>,
        }

        struct KeyState {
            best: Option<(u64, u8)>,
        }

        let mut keys: Vec<KeyState> = (0..txids.len())
            .map(|_| KeyState { best: None })
            .collect();

        let mut scheduled: Vec<Op> = Vec::new();
        for (i, cands) in cands_by_key.iter().enumerate() {
            for &(d, fk) in cands {
                scheduled.push(Op {
                    key_i: i as u32,
                    stage: Stage::Idx {
                        depth: d as u8,
                        fk,
                    },
                    buf: [0u8; 33],
                    buf_len: 0,
                    file_off: 0,
                    fd: idx_fd,
                    range: (0, 0),
                    prefix_n: 0,
                    dead: false,
                    err: None,
                });
            }
        }

        let mut body_lookups = 0u64;
        while !scheduled.is_empty() {
            let t_wave = Instant::now();
            let mut any_idx = false;
            let mut any_body = false;

            for op in scheduled.iter_mut() {
                match op.stage {
                    Stage::Idx { fk, .. } => {
                        any_idx = true;
                        let id = fk;
                        if id == 0 || id > count {
                            op.dead = true;
                            op.buf_len = 0;
                            continue;
                        }
                        let idx_off = crate::file::FILE_HEADER_LEN as u64 + (id - 1) * 8;
                        let nbytes: u8 = if id < count { 16 } else { 8 };
                        if idx_off.saturating_add(u64::from(nbytes)) > idx_pub {
                            op.dead = true;
                            op.buf_len = 0;
                            continue;
                        }
                        op.fd = idx_fd;
                        op.file_off = idx_off;
                        op.buf_len = nbytes;
                        op.buf[..nbytes as usize].fill(0);
                    }
                    Stage::Body { off, len, .. } => {
                        any_body = true;
                        let n = (len as usize).min(33);
                        if n == 0 {
                            op.err = Some(StoreError::Corrupt("empty body for txid"));
                            continue;
                        }
                        if off.saturating_add(n as u64) > body_pub {
                            op.err = Some(StoreError::Corrupt("body past published"));
                            continue;
                        }
                        op.fd = body_fd;
                        op.file_off = off;
                        op.buf_len = n as u8;
                        op.buf[..n].fill(0);
                    }
                }
            }

            let io_idx: Vec<usize> = scheduled
                .iter()
                .enumerate()
                .filter(|(_, op)| op.err.is_none() && op.buf_len > 0)
                .map(|(i, _)| i)
                .collect();
            if !io_idx.is_empty() {
                let total: usize = io_idx.iter().map(|&i| scheduled[i].buf_len as usize).sum();
                let mut arena = vec![0u8; total];
                let spans: Vec<(usize, usize)> = io_idx
                    .iter()
                    .map(|&i| (i, scheduled[i].buf_len as usize))
                    .collect();
                let mut rest = arena.as_mut_slice();
                let mut pieces: Vec<&mut [u8]> = Vec::with_capacity(spans.len());
                for &(_, len) in &spans {
                    let (left, right) = rest.split_at_mut(len);
                    pieces.push(left);
                    rest = right;
                }
                let mut read_ops: Vec<ReadOp<'_>> = Vec::with_capacity(spans.len());
                for (piece, &(op_i, _)) in pieces.into_iter().zip(spans.iter()) {
                    let s = &scheduled[op_i];
                    read_ops.push(ReadOp {
                        fd: s.fd,
                        offset: s.file_off,
                        buf: piece,
                        result: i32::MIN,
                    });
                }
                bulk_io::pread_batch(&mut read_ops);
                for (ro, &(op_i, len)) in read_ops.iter().zip(spans.iter()) {
                    let op = &mut scheduled[op_i];
                    if ro.result < 0 {
                        let path: &std::path::Path = match op.stage {
                            Stage::Idx { .. } => idx_path.as_ref(),
                            Stage::Body { .. } => body_path.as_ref(),
                        };
                        op.err = Some(StoreError::io(
                            path,
                            std::io::Error::from_raw_os_error(-ro.result),
                        ));
                        continue;
                    }
                    // Idx/body still require full length (known sizes).
                    if ro.result as usize != len {
                        op.err = Some(StoreError::Corrupt("bulk pread short"));
                        continue;
                    }
                    op.buf[..len].copy_from_slice(&ro.buf[..len]);
                    match op.stage {
                        Stage::Idx { fk, .. } => {
                            let start = u64::from_le_bytes(op.buf[..8].try_into().unwrap());
                            let end = if fk < count {
                                u64::from_le_bytes(op.buf[8..16].try_into().unwrap())
                            } else {
                                body_logical
                            };
                            if end < start {
                                op.err = Some(StoreError::Corrupt("var record end < start"));
                            } else {
                                op.range = (start, end - start);
                            }
                        }
                        Stage::Body { .. } => {
                            op.prefix_n = len;
                        }
                    }
                }
            }

            let wave_ns = t_wave.elapsed().as_nanos() as u64;
            if any_body && !any_idx {
                crate::head_resolve_stats::add_body(wave_ns);
            } else if any_idx && !any_body {
                crate::head_resolve_stats::add_idx(wave_ns);
            } else {
                let parts = u64::from(any_idx) + u64::from(any_body);
                let share = if parts > 0 { wave_ns / parts } else { 0 };
                if any_idx {
                    crate::head_resolve_stats::add_idx(share);
                }
                if any_body {
                    crate::head_resolve_stats::add_body(share);
                }
            }

            let mut next: Vec<Op> = Vec::with_capacity(scheduled.len());
            for op in scheduled {
                if let Some(e) = op.err {
                    return Err(e);
                }
                let ki = op.key_i as usize;
                match op.stage {
                    Stage::Idx { depth, fk } => {
                        if op.dead {
                            continue;
                        }
                        let (off, len) = op.range;
                        if len == 0 {
                            continue;
                        }
                        next.push(Op {
                            key_i: op.key_i,
                            stage: Stage::Body {
                                depth,
                                fk,
                                off,
                                len,
                            },
                            buf: [0u8; 33],
                            buf_len: 0,
                            file_off: 0,
                            fd: body_fd,
                            range: (0, 0),
                            prefix_n: 0,
                            dead: false,
                            err: None,
                        });
                    }
                    Stage::Body { depth, fk, .. } => {
                        body_lookups = body_lookups.saturating_add(1);
                        let want = &txids[ki];
                        match Self::txid_from_body_prefix(&op.buf[..op.prefix_n]) {
                            Ok(got) if got == *want => match keys[ki].best {
                                Some((_, best_d)) if depth <= best_d => {}
                                _ => keys[ki].best = Some((fk, depth)),
                            },
                            Ok(_) => {}
                            Err(StoreError::NotFound) | Err(StoreError::Corrupt(_)) => {}
                            Err(e) => return Err(e),
                        }
                    }
                }
            }
            scheduled = next;
        }

        crate::head_resolve_stats::add_cands(cands_total);
        crate::head_resolve_stats::add_body_lookups(body_lookups);

        let mut out = Vec::with_capacity(txids.len());
        for (i, txid) in txids.iter().enumerate() {
            let hit = keys[i].best.map(|(id, _)| Fk(id));
            out.push((*txid, hit));
        }
        Ok(out)
    }

    /// Bulk `body_range` for many fks (confirm load). io_uring idx preads.
    pub fn body_range_batch(
        &self,
        fks: &[Fk],
    ) -> Result<Vec<Option<(u64, u64)>>, StoreError> {
        use crate::bulk_io::{self, ReadOp};
        if fks.is_empty() {
            return Ok(Vec::new());
        }
        let count = self.body.count();
        let body_logical = self.body.body_logical_len().max(crate::file::FILE_HEADER_LEN as u64);
        let idx_fd = self.body.idx_read_fd();
        let idx_path = self.body.idx_file_path();
        let idx_pub = self.body.idx_published_len();

        // Prepare: for each valid fk, 8 or 16 byte idx read.
        struct Job {
            id: u64,
            off: u64,
            nbytes: u8,
            buf: [u8; 16],
            out: Option<(u64, u64)>,
            skip: bool,
            err: Option<StoreError>,
        }
        let mut jobs: Vec<Job> = fks
            .iter()
            .map(|fk| {
                let Some(id) = fk.get() else {
                    return Job {
                        id: 0,
                        off: 0,
                        nbytes: 0,
                        buf: [0u8; 16],
                        out: None,
                        skip: true,
                        err: None,
                    };
                };
                if id == 0 || id > count {
                    return Job {
                        id,
                        off: 0,
                        nbytes: 0,
                        buf: [0u8; 16],
                        out: None,
                        skip: true,
                        err: None,
                    };
                }
                let off = crate::file::FILE_HEADER_LEN as u64 + (id - 1) * 8;
                let nbytes: u8 = if id < count { 16 } else { 8 };
                if off.saturating_add(u64::from(nbytes)) > idx_pub {
                    return Job {
                        id,
                        off,
                        nbytes,
                        buf: [0u8; 16],
                        out: None,
                        skip: true,
                        err: Some(StoreError::Corrupt("idx past published")),
                    };
                }
                Job {
                    id,
                    off,
                    nbytes,
                    buf: [0u8; 16],
                    out: None,
                    skip: false,
                    err: None,
                }
            })
            .collect();

        // Arena submit for non-skip jobs.
        let active: Vec<usize> = jobs
            .iter()
            .enumerate()
            .filter(|(_, j)| !j.skip && j.err.is_none())
            .map(|(i, _)| i)
            .collect();
        if !active.is_empty() {
            let mut arena = vec![0u8; active.iter().map(|&i| jobs[i].nbytes as usize).sum()];
            let mut spans: Vec<(usize, usize)> = Vec::with_capacity(active.len()); // (job_i, len)
            let mut rest = arena.as_mut_slice();
            let mut pieces: Vec<&mut [u8]> = Vec::with_capacity(active.len());
            for &ji in &active {
                let len = jobs[ji].nbytes as usize;
                let (left, right) = rest.split_at_mut(len);
                pieces.push(left);
                rest = right;
                spans.push((ji, len));
            }
            let mut read_ops: Vec<ReadOp<'_>> = Vec::with_capacity(active.len());
            for (piece, &(ji, _)) in pieces.into_iter().zip(spans.iter()) {
                read_ops.push(ReadOp {
                    fd: idx_fd,
                    offset: jobs[ji].off,
                    buf: piece,
                    result: i32::MIN,
                });
            }
            bulk_io::pread_batch(&mut read_ops);
            for (ro, &(ji, len)) in read_ops.iter().zip(spans.iter()) {
                if ro.result < 0 {
                    jobs[ji].err = Some(StoreError::io(
                        idx_path,
                        std::io::Error::from_raw_os_error(-ro.result),
                    ));
                    continue;
                }
                if ro.result as usize != len {
                    jobs[ji].err = Some(StoreError::Corrupt("bulk idx pread short"));
                    continue;
                }
                jobs[ji].buf[..len].copy_from_slice(&ro.buf[..len]);
                let start = u64::from_le_bytes(jobs[ji].buf[..8].try_into().unwrap());
                let end = if jobs[ji].id < count {
                    u64::from_le_bytes(jobs[ji].buf[8..16].try_into().unwrap())
                } else {
                    body_logical
                };
                if end < start {
                    jobs[ji].err = Some(StoreError::Corrupt("var record end < start"));
                } else {
                    jobs[ji].out = Some((start, end - start));
                }
            }
        }

        let mut out = Vec::with_capacity(jobs.len());
        for job in jobs {
            if let Some(e) = job.err {
                return Err(e);
            }
            out.push(job.out);
        }
        Ok(out)
    }

    /// Bulk full packed decode from known ranges (confirm load create bodies).
    ///
    /// io_uring body preads, then CPU decode.
    pub fn get_full_batch_at(
        &self,
        ranges: &[(Fk, u64, u64)],
    ) -> Result<Vec<Option<(TxRecord, Vec<InputRecord>, Vec<OutputRecord>)>>, StoreError> {
        use crate::bulk_io::{self, ReadOp};
        if ranges.is_empty() {
            return Ok(Vec::new());
        }
        let body_fd = self.body.body_read_fd();
        let body_pub = self.body.body_published_len();

        let submitted: Vec<usize> = ranges
            .iter()
            .enumerate()
            .filter(|(_, (_, off, len))| *len > 0 && off.saturating_add(*len) <= body_pub)
            .map(|(i, _)| i)
            .collect();

        let mut bufs: Vec<Vec<u8>> = ranges
            .iter()
            .map(|(_, _, len)| vec![0u8; *len as usize])
            .collect();

        // SAFETY: each `bufs[i]` is a distinct allocation; submitted indices unique.
        let mut read_ops: Vec<ReadOp<'_>> = Vec::with_capacity(submitted.len());
        for &i in &submitted {
            let off = ranges[i].1;
            let len = ranges[i].2 as usize;
            let ptr = bufs[i].as_mut_ptr();
            let slice = unsafe { std::slice::from_raw_parts_mut(ptr, len) };
            read_ops.push(ReadOp {
                fd: body_fd,
                offset: off,
                buf: slice,
                result: i32::MIN,
            });
        }
        bulk_io::pread_batch(&mut read_ops);

        let mut out: Vec<Option<(TxRecord, Vec<InputRecord>, Vec<OutputRecord>)>> =
            vec![None; ranges.len()];
        for (ro, &i) in read_ops.iter().zip(submitted.iter()) {
            if ro.result < 0 || ro.result as u64 != ranges[i].2 {
                continue;
            }
            if let Ok(v) = decode_packed_tx(&bufs[i]) {
                out[i] = Some(v);
            }
        }
        Ok(out)
    }

    /// Bulk meta+outputs from known ranges (confirm pin_new). io_uring body preads.
    pub fn get_meta_and_outputs_batch_at(
        &self,
        ranges: &[(u64, u64)],
    ) -> Result<Vec<Option<(TxRecord, Vec<OutputRecord>)>>, StoreError> {
        use crate::bulk_io::{self, ReadOp};
        if ranges.is_empty() {
            return Ok(Vec::new());
        }
        let body_fd = self.body.body_read_fd();
        let body_pub = self.body.body_published_len();

        let submitted: Vec<usize> = ranges
            .iter()
            .enumerate()
            .filter(|(_, (off, len))| *len > 0 && off.saturating_add(*len) <= body_pub)
            .map(|(i, _)| i)
            .collect();

        let mut bufs: Vec<Vec<u8>> = ranges
            .iter()
            .map(|(_, len)| vec![0u8; *len as usize])
            .collect();

        let mut read_ops: Vec<ReadOp<'_>> = Vec::with_capacity(submitted.len());
        for &i in &submitted {
            let (off, len) = ranges[i];
            let ptr = bufs[i].as_mut_ptr();
            let slice = unsafe { std::slice::from_raw_parts_mut(ptr, len as usize) };
            read_ops.push(ReadOp {
                fd: body_fd,
                offset: off,
                buf: slice,
                result: i32::MIN,
            });
        }
        bulk_io::pread_batch(&mut read_ops);

        let mut out: Vec<Option<(TxRecord, Vec<OutputRecord>)>> = vec![None; ranges.len()];
        for (ro, &i) in read_ops.iter().zip(submitted.iter()) {
            if ro.result < 0 || ro.result as u64 != ranges[i].1 {
                continue;
            }
            if let Ok(v) = decode_packed_tx_outs_only(&bufs[i]) {
                out[i] = Some(v);
            }
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

    /// Like [`Self::get_output_spender_meta`] but uses a cache-held body range (no idx).
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

    /// Patch spender meta using a cache-held body range (no idx read on the hot path).
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
        // Bulk body_txid waves (io_uring / parallel pread); insert_many still
        // chunked for fence / probe cost.
        const READ_CHUNK: u64 = 8192;
        const INSERT_CHUNK: usize = 4096;
        const PROGRESS_EVERY: u64 = 50_000;
        let mut batch: Vec<([u8; 32], Fk)> = Vec::with_capacity(INSERT_CHUNK);
        let mut last_progress = 0u64;
        let mut cur = 1u64;
        while cur <= n {
            let end = (cur + READ_CHUNK - 1).min(n);
            // Contiguous idx + bulk 33B prefixes — same path as head resize fill.
            let txids = self.body_txid_range(cur, end)?;
            debug_assert_eq!(txids.len() as u64, end - cur + 1);
            for (i, txid) in txids.into_iter().enumerate() {
                let id = cur + i as u64;
                let fk = Fk(id);
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
                if batch.len() >= INSERT_CHUNK {
                    inserted += batch.len() as u64;
                    self.head_insert_many(&batch)?;
                    batch.clear();
                }
                if id - last_progress >= PROGRESS_EVERY || id == n {
                    on_progress(id, n, inserted + batch.len() as u64);
                    last_progress = id;
                }
            }
            cur = end + 1;
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

    /// Bulk-insert head entries (archive / tip / rebuild).
    ///
    /// Sole writer: plain store empty→fk, call order, SeqCst fence after batch.
    /// **No body_txid** on insert. May start sequential online resize (background
    /// thread fills the shadow continuously).
    ///
    /// On open-address **probe exhaust**, blocks the writer until the bg resize
    /// swaps a wider primary, then retries the batch.
    pub fn head_insert_many(&self, entries: &[([u8; 32], Fk)]) -> Result<(), StoreError> {
        if !entries.is_empty() {
            self.head_insert_many_with_resize_retry(entries)?;
        }
        // First insert past PROBE_DEPTH_WARN requests early widen (before load 0.75
        // or probe exhaust).
        if take_probe_depth_resize_request() && !self.head_resize_in_progress() {
            self.ensure_head_resize_for_probe_exhaust()?;
        }
        // After live inserts into primary only — never dual-write to shadow.
        self.maybe_start_head_resize()?;
        self.ensure_resize_bg_running();
        Ok(())
    }

    /// Insert batch; if the primary probe chain is full, wait for bg resize.
    fn head_insert_many_with_resize_retry(
        &self,
        entries: &[([u8; 32], Fk)],
    ) -> Result<(), StoreError> {
        use std::time::Instant;

        /// How often the blocked writer re-checks whether resize finished.
        const WAIT_SLICE: Duration = Duration::from_millis(250);
        /// Safety valve — operator can restart if resize is truly stuck.
        const MAX_WAIT: Duration = Duration::from_secs(30 * 60);
        /// How often to emit a wait-progress WARN.
        const LOG_EVERY: Duration = Duration::from_secs(30);

        let t0 = Instant::now();
        let mut attempts = 0u32;
        let mut last_log = Instant::now()
            .checked_sub(LOG_EVERY)
            .unwrap_or_else(Instant::now);
        loop {
            // Drop the read guard **before** resize/swap. Matching on
            // `head.read().insert_many(...)` would keep the guard through the
            // Err arm; `try_complete` then needs `head.write()` → self-deadlock
            // ("waiting for exclusive head lock" forever).
            let insert_result = {
                let head = self.head.read().unwrap();
                head.insert_many(entries)
            };
            match insert_result {
                Ok(()) => {
                    if attempts > 0 {
                        rbitcoin_log::info!(
                            "store: tx.head insert resumed after probe exhaust \
                             (attempts={attempts} waited={:?} batch={})",
                            t0.elapsed(),
                            entries.len()
                        );
                    }
                    return Ok(());
                }
                Err(e) if is_probe_exhausted_error(&e) => {
                    attempts = attempts.saturating_add(1);
                    // Ensure a widen is running (force start even if under load
                    // threshold — probe exhaust is a hard capacity signal).
                    self.ensure_head_resize_for_probe_exhaust()?;
                    // Background thread owns continuous fill — do **not** do
                    // small sleep-poll chunks on the archive writer.
                    self.ensure_resize_bg_running();
                    if t0.elapsed() > MAX_WAIT {
                        let (cursor, n, _, _) = self.head_resize_progress();
                        rbitcoin_log::error!(
                            "store: tx.head probe exhausted — resize did not free capacity \
                             within {MAX_WAIT:?} (attempts={attempts} cursor={cursor}/{n})"
                        );
                        return Err(e);
                    }
                    if self.head_resize_in_progress() {
                        let should_log =
                            attempts == 1 || last_log.elapsed() >= LOG_EVERY;
                        if should_log {
                            let (cursor, n, target_bits, slots) = self.head_resize_progress();
                            let (deep, exh) =
                                crate::address_head::probe_depth_stats_snapshot();
                            let pct = if n > 0 {
                                100.0 * (cursor.saturating_sub(1) as f64) / (n as f64)
                            } else {
                                100.0
                            };
                            rbitcoin_log::warn!(
                                "store: tx.head probe exhausted — waiting on bg resize \
                                 (attempt={attempts} \
                                 cursor={}/{n} ({pct:.1}%) target_bits={target_bits} \
                                 slots={slots} deep_warn={deep} exhaust={exh} \
                                 batch={} elapsed={:?})",
                                cursor.saturating_sub(1),
                                entries.len(),
                                t0.elapsed()
                            );
                            last_log = Instant::now();
                        }
                        std::thread::sleep(WAIT_SLICE);
                    }
                }
                Err(e) => return Err(e),
            }
        }
    }

    /// `(shadow_cursor, class_a_count, target_bits, primary_slots)` for logs.
    fn head_resize_progress(&self) -> (u64, u64, u32, u64) {
        let n = self.count();
        let slots = self.head.read().unwrap().slots();
        let g = self.resize.lock().unwrap();
        match g.as_ref() {
            Some(r) => (r.cursor, n, r.target.bits, slots),
            None => (0, n, self.head.read().unwrap().bits(), slots),
        }
    }

    /// Start a BITS+1 rebuild if none is running (probe-exhaust path).
    fn ensure_head_resize_for_probe_exhaust(&self) -> Result<(), StoreError> {
        if self.head_resize_in_progress() {
            return Ok(());
        }
        let bits = self.head.read().unwrap().bits();
        if bits >= MAX_BITS {
            return Ok(());
        }
        let target = HeadLayout::new(bits + 1)?;
        rbitcoin_log::info!(
            "store: tx.head probe exhausted — forcing resize start {}→{} bits (n={})",
            bits,
            target.bits,
            self.count()
        );
        self.start_head_resize(target)?;
        self.ensure_resize_bg_running();
        Ok(())
    }

    /// Same as [`Self::head_insert_many`] (sole-writer path is the only path).
    #[inline]
    pub fn head_insert_many_sole(&self, entries: &[([u8; 32], Fk)]) -> Result<(), StoreError> {
        self.head_insert_many(entries)
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
            self.ensure_resize_bg_running();
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
        self.ensure_resize_bg_running();
        Ok(())
    }

    /// Start BITS+1 sequential rebuild when load ≥ [`crate::address_head::HEAD_LOAD_START`].
    pub fn maybe_start_head_resize(&self) -> Result<(), StoreError> {
        {
            let rg = self.resize.lock().unwrap();
            if let Some(r) = rg.as_ref() {
                // Warn if primary load is high while resizing.
                let (slots, n) = {
                    let h = self.head.read().unwrap();
                    (h.slots(), self.count())
                };
                let ratio = load_ratio(n, slots);
                if ratio >= HEAD_LOAD_WARN {
                    // Rate-limit: maybe_start is called on every head batch.
                    static LAST_LAG_LOG_MS: std::sync::atomic::AtomicU64 =
                        std::sync::atomic::AtomicU64::new(0);
                    let now_ms = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .map(|d| d.as_millis() as u64)
                        .unwrap_or(0);
                    let prev = LAST_LAG_LOG_MS.load(std::sync::atomic::Ordering::Relaxed);
                    if now_ms.saturating_sub(prev) >= 60_000
                        && LAST_LAG_LOG_MS
                            .compare_exchange(
                                prev,
                                now_ms,
                                std::sync::atomic::Ordering::Relaxed,
                                std::sync::atomic::Ordering::Relaxed,
                            )
                            .is_ok()
                    {
                        let pct = if n > 0 {
                            100.0 * (r.cursor.saturating_sub(1) as f64) / (n as f64)
                        } else {
                            100.0
                        };
                        let (deep, exh) = crate::address_head::probe_depth_stats_snapshot();
                        rbitcoin_log::warn!(
                            "store: tx.head resize lagging load={ratio:.3} n={n} slots={slots} \
                             shadow_cursor={}/{} ({pct:.1}%) target_bits={} \
                             deep_warn={deep} exhaust={exh}",
                            r.cursor.saturating_sub(1),
                            n,
                            r.target.bits
                        );
                    }
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
        let slots_old = self.head.read().unwrap().slots();
        let n = self.count();
        let load = load_ratio(n, slots_old);
        rbitcoin_log::info!(
            "store: tx.head resize start {}→{} bits entry={}B n={n} slots_old={slots_old} \
             load={load:.3} slots_new={} (threshold={})",
            cur_bits,
            target.bits,
            target.entry_bytes,
            target.slots(),
            crate::address_head::HEAD_LOAD_START
        );
        *rg = Some(HeadResize {
            shadow,
            cursor: 1,
            target,
        });
        drop(rg);
        self.ensure_resize_bg_running();
        Ok(())
    }

    /// Class A fks per bulk body-txid read during head resize (`tx.idx` range +
    /// io_uring / parallel body prefixes). Override with
    /// `RBITCOIN_TX_HEAD_RESIZE_READ_BATCH` (default **8000**).
    fn head_resize_read_batch() -> u64 {
        std::env::var("RBITCOIN_TX_HEAD_RESIZE_READ_BATCH")
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .filter(|&n| n > 0)
            .unwrap_or(8_000)
            .clamp(1, 1_000_000)
    }

    /// Fill shadow head for consecutive Class A ids using bulk idx + body reads.
    ///
    /// Chunks body IO by [`Self::head_resize_read_batch`]; writes via existing
    /// `insert_many` in smaller write groups (probe locality / fence cost).
    fn shadow_fill_fk_range(
        &self,
        shadow: &AddressHead,
        first: u64,
        last: u64,
    ) -> Result<(), StoreError> {
        if last < first {
            return Ok(());
        }
        const WRITE_CHUNK: usize = 256;
        let read_batch = Self::head_resize_read_batch();
        let mut cur = first;
        while cur <= last {
            let end = (cur + read_batch - 1).min(last);
            let txids = self.body_txid_range(cur, end)?;
            debug_assert_eq!(txids.len() as u64, end - cur + 1);
            let mut batch: Vec<([u8; 32], Fk)> = Vec::with_capacity(WRITE_CHUNK);
            for (i, txid) in txids.into_iter().enumerate() {
                batch.push((txid, Fk(cur + i as u64)));
                if batch.len() >= WRITE_CHUNK {
                    shadow.insert_many(&batch)?;
                    batch.clear();
                }
            }
            if !batch.is_empty() {
                shadow.insert_many(&batch)?;
            }
            cur = end + 1;
        }
        Ok(())
    }

    /// Advance sequential shadow fill by up to `budget` Class A fks; swap when caught up.
    ///
    /// Does **not** dual-write live inserts — only `tx.idx` order into shadow.
    /// Body txids are read in bulk (io_uring / parallel pread); shadow inserts
    /// remain ordered `insert_many` on `tx.head.new`.
    pub fn head_resize_poll(&self, budget: u64) -> Result<(), StoreError> {
        if budget == 0 {
            return Ok(());
        }
        let n = self.count();
        let mut done_fill = false;
        let mut progress_log: Option<(u64, u64, u32)> = None;
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
                let start = r.cursor;
                let end = (r.cursor + budget - 1).min(n);
                self.shadow_fill_fk_range(&r.shadow, r.cursor, end)?;
                r.cursor = end + 1;
                write_resize_control(
                    &self.head_path,
                    &ResizeControl {
                        target: r.target,
                        cursor: r.cursor,
                        generation: self.head.read().unwrap().generation(),
                    },
                )?;
                // Progress log every ~1M Class A fks advanced (or when finishing).
                // Bg waves are 1M; this aligns with one INFO per wave.
                let advanced = r.cursor.saturating_sub(start);
                let milestone = r.cursor > 1
                    && (r.cursor / 1_000_000 > start.saturating_sub(1) / 1_000_000
                        || r.cursor > n
                        || advanced >= 1_000_000);
                if milestone {
                    progress_log = Some((r.cursor.saturating_sub(1), n, r.target.bits));
                }
                if r.cursor > n {
                    done_fill = true;
                }
            }
        }
        if let Some((cur, total, bits)) = progress_log {
            let pct = if total > 0 {
                100.0 * (cur as f64) / (total as f64)
            } else {
                100.0
            };
            rbitcoin_log::info!(
                "store: tx.head resize progress cursor={cur}/{total} ({pct:.1}%) target_bits={bits}"
            );
        }
        if done_fill {
            self.try_complete_head_resize()?;
        }
        Ok(())
    }

    /// Final catch-up under primary insert lock, then atomic rename swap.
    fn try_complete_head_resize(&self) -> Result<(), StoreError> {
        use std::time::{Duration, Instant};

        rbitcoin_log::info!(
            "store: tx.head resize fill done — final catch-up + swap (shadow → primary)"
        );

        // Phase 1: catch-up + flush **without** exclusive `head.write()` so we do
        // not block forever behind long head-resolve batches (sole inserter is us).
        {
            let n = self.count();
            let mut rg = self.resize.lock().unwrap();
            let Some(r) = rg.as_mut() else {
                return Ok(());
            };
            if r.cursor <= n {
                self.shadow_fill_fk_range(&r.shadow, r.cursor, n)?;
                r.cursor = n + 1;
            }
            let n2 = self.count();
            if r.cursor <= n2 {
                self.shadow_fill_fk_range(&r.shadow, r.cursor, n2)?;
                r.cursor = n2 + 1;
            }
            rbitcoin_log::info!(
                "store: tx.head resize flushing shadow (cursor={}, n={})",
                r.cursor.saturating_sub(1),
                n2.max(n)
            );
            r.shadow.flush()?;
        }

        // Phase 2: exclusive head ownership for rename + reopen. Use try_write so
        // we log while waiting (std RwLock write can starve under continuous reads).
        rbitcoin_log::info!("store: tx.head resize acquiring exclusive head lock for swap…");
        let t_lock = Instant::now();
        let mut waited = 0u32;
        let mut head_w = loop {
            match self.head.try_write() {
                Ok(g) => break g,
                Err(_) => {
                    waited = waited.saturating_add(1);
                    if waited == 1 || waited % 40 == 0 {
                        rbitcoin_log::warn!(
                            "store: tx.head resize waiting for exclusive head lock \
                             ({:?}; other threads still hold head.read)",
                            t_lock.elapsed()
                        );
                    }
                    std::thread::sleep(Duration::from_millis(50));
                }
            }
        };
        if waited > 0 {
            rbitcoin_log::info!(
                "store: tx.head resize exclusive lock acquired after {:?}",
                t_lock.elapsed()
            );
        }

        let primary_writes = head_w.lock_writes();
        // One more catch-up under exclusive insert barrier.
        let n3 = self.count();
        let mut rg = self.resize.lock().unwrap();
        let Some(r) = rg.as_mut() else {
            return Ok(());
        };
        if r.cursor <= n3 {
            self.shadow_fill_fk_range(&r.shadow, r.cursor, n3)?;
            r.cursor = n3 + 1;
            r.shadow.flush()?;
        }
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
        drop(rg);

        rbitcoin_log::info!(
            "store: tx.head resize renaming shadow → primary (bits={} slots={})",
            target.bits,
            target.slots()
        );
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
        // Background fill thread owns progress; optional poll still OK.
        for _ in 0..200 {
            if !t.head_resize_in_progress() {
                break;
            }
            std::thread::sleep(Duration::from_millis(5));
        }
        assert!(
            !t.head_resize_in_progress(),
            "resize should complete via bg thread"
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
        // 2^8 = 256 slots; trigger at ceil(0.75*256)=192.
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
        for i in 1..=200u64 {
            let _ = t.put_full_batch_indexed(&[mk(i)], true).unwrap();
        }
        // head_insert_many should have started resize; bg thread fills.
        for _ in 0..400 {
            if !t.head_resize_in_progress() && t.head_bits() >= 9 {
                break;
            }
            t.maybe_start_head_resize().unwrap();
            std::thread::sleep(Duration::from_millis(5));
        }
        assert!(t.head_bits() >= 9, "bits={}", t.head_bits());
        // Spot-check resolves.
        for i in [1u64, 100, 200] {
            let mut txid = [0u8; 32];
            txid[0..8].copy_from_slice(&i.to_le_bytes());
            assert_eq!(t.get_fk_by_txid(&txid).unwrap(), Some(Fk(i)));
        }
        let _ = std::fs::remove_dir_all(&dir);
        std::env::remove_var("RBITCOIN_TX_HEAD_BITS");
    }

    /// Bulk body_txid_range matches serial body_txid (idx batch + bulk pread).
    #[test]
    fn body_txid_range_matches_serial() {
        let dir = std::env::temp_dir().join(format!(
            "rbitcoin-tx-txid-range-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        std::env::set_var("RBITCOIN_HEAD_SCALE", "tiny");
        let t = TxTable::create(&dir).unwrap();
        let mk = |i: u64| {
            let mut txid = [0u8; 32];
            txid[0..8].copy_from_slice(&i.to_le_bytes());
            txid[8] = 0xce;
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
        for i in 1..=40u64 {
            let _ = t.put_full_batch_indexed(&[mk(i)], true).unwrap();
        }
        assert!(t.body_txid_range(5, 4).unwrap().is_empty());
        let bulk = t.body_txid_range(1, 40).unwrap();
        assert_eq!(bulk.len(), 40);
        for i in 1..=40u64 {
            assert_eq!(bulk[(i - 1) as usize], t.body_txid(Fk(i)).unwrap());
        }
        let mid = t.body_txid_range(10, 25).unwrap();
        for (j, id) in (10..=25).enumerate() {
            assert_eq!(mid[j], t.body_txid(Fk(id)).unwrap());
        }
        // Through last published id (body-end path for last length).
        let tail = t.body_txid_range(38, 40).unwrap();
        assert_eq!(tail.len(), 3);
        for (j, id) in (38..=40).enumerate() {
            assert_eq!(tail[j], t.body_txid(Fk(id)).unwrap());
        }
        let _ = std::fs::remove_dir_all(&dir);
        std::env::remove_var("RBITCOIN_HEAD_SCALE");
    }

    /// Resize with a tiny bulk-read batch still fills shadow correctly.
    #[test]
    fn head_resize_with_small_read_batch() {
        let dir = std::env::temp_dir().join(format!(
            "rbitcoin-tx-head-resize-batch-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        std::env::set_var("RBITCOIN_TX_HEAD_BITS", "10");
        // Force multi-chunk bulk reads inside each poll budget.
        std::env::set_var("RBITCOIN_TX_HEAD_RESIZE_READ_BATCH", "7");
        let t = TxTable::create(&dir).unwrap();
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
        for i in 1..=60u64 {
            let _ = t.put_full_batch_indexed(&[mk(i)], true).unwrap();
        }
        t.start_head_resize(crate::address_head::HeadLayout::new(11).unwrap())
            .unwrap();
        for _ in 0..400 {
            if !t.head_resize_in_progress() {
                break;
            }
            std::thread::sleep(Duration::from_millis(5));
        }
        assert!(!t.head_resize_in_progress());
        assert_eq!(t.head_bits(), 11);
        for i in 1..=60u64 {
            let mut txid = [0u8; 32];
            txid[0..8].copy_from_slice(&i.to_le_bytes());
            assert_eq!(t.get_fk_by_txid(&txid).unwrap(), Some(Fk(i)));
        }
        let _ = std::fs::remove_dir_all(&dir);
        std::env::remove_var("RBITCOIN_TX_HEAD_BITS");
        std::env::remove_var("RBITCOIN_TX_HEAD_RESIZE_READ_BATCH");
    }

    /// Fat packed body: body_txid must match full-record path without needing
    /// the full payload (large witness).
    #[test]
    fn body_txid_thin_prefix_matches_fat_packed_body() {
        let dir = std::env::temp_dir().join(format!(
            "rbitcoin-tx-thin-txid-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        std::env::set_var("RBITCOIN_HEAD_SCALE", "tiny");
        let t = TxTable::create(&dir).unwrap();
        let mut txid = [0xabu8; 32];
        txid[0] = 0x7e;
        let tx = TxRecord {
            txid,
            version: 2,
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
            script_sig: vec![0xde; 64],
            witness: vec![vec![0xad; 50_000]], // fat body
        }];
        let outputs = vec![OutputRecord::unspent(42, vec![0x51; 100])];
        let fk = t
            .put_full_batch_indexed(&[(tx, inputs, outputs)], true)
            .unwrap()[0];
        let from_thin = t.body_txid(fk).unwrap();
        assert_eq!(from_thin, txid);
        // Range path (cache-held off/len).
        let (off, len) = t.body.record_range(fk).unwrap();
        assert!(len > 50_000, "body should be large");
        assert_eq!(t.body_txid_at(off, len).unwrap(), txid);
        // Head resolve still works.
        assert_eq!(t.get_fk_by_txid(&txid).unwrap(), Some(fk));
        let _ = std::fs::remove_dir_all(&dir);
        std::env::remove_var("RBITCOIN_HEAD_SCALE");
    }

    /// Bulk body_range + get_full_batch_at agree with sequential paths.
    #[test]
    fn bulk_body_range_and_full_match_sequential() {
        let dir = std::env::temp_dir().join(format!(
            "rbitcoin-tx-bulk-body-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        std::env::set_var("RBITCOIN_HEAD_SCALE", "tiny");
        let t = TxTable::create(&dir).unwrap();
        let mut fks = Vec::new();
        for i in 0u8..12 {
            let mut txid = [0u8; 32];
            txid[0] = i.wrapping_add(10);
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
                script_sig: vec![i],
                witness: vec![],
            }];
            let outputs = vec![OutputRecord::unspent(1, vec![0x51])];
            fks.push(
                t.put_full_batch_indexed(&[(tx, inputs, outputs)], true)
                    .unwrap()[0],
            );
        }
        let batch_ranges = t.body_range_batch(&fks).unwrap();
        for (fk, br) in fks.iter().zip(batch_ranges.iter()) {
            let seq = t.body_range(*fk).unwrap();
            assert_eq!(*br, Some(seq));
        }
        let range_args: Vec<(Fk, u64, u64)> = fks
            .iter()
            .zip(batch_ranges.iter())
            .filter_map(|(fk, r)| r.map(|(o, l)| (*fk, o, l)))
            .collect();
        let bulk = t.get_full_batch_at(&range_args).unwrap();
        for ((fk, off, len), got) in range_args.iter().zip(bulk.iter()) {
            let seq = t.get_full_at(*off, *len).unwrap();
            let b = got.as_ref().expect("bulk decode");
            assert_eq!(b.0.txid, seq.0.txid);
            assert_eq!(b.0.txid, t.body_txid(*fk).unwrap());
        }
        let meta_ranges: Vec<(u64, u64)> = range_args.iter().map(|(_, o, l)| (*o, *l)).collect();
        let meta = t.get_meta_and_outputs_batch_at(&meta_ranges).unwrap();
        for ((_, off, len), got) in range_args.iter().zip(meta.iter()) {
            let seq = t.get_meta_and_outputs_at(*off, *len).unwrap();
            let b = got.as_ref().expect("meta bulk");
            assert_eq!(b.0, seq.0);
            assert_eq!(b.1.len(), seq.1.len());
        }
        let _ = std::fs::remove_dir_all(&dir);
        std::env::remove_var("RBITCOIN_HEAD_SCALE");
    }

    /// Depth-max match: foreigners + two same-txid creates; batch prefers deepest.
    #[test]
    fn get_fk_by_txid_batch_depth_wins_with_workers() {
        std::env::set_var("RBITCOIN_BULK_IO_WORKERS", "4");
        let dir = std::env::temp_dir().join(format!(
            "rbitcoin-tx-batch-depth-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        std::env::set_var("RBITCOIN_HEAD_SCALE", "tiny");
        let t = TxTable::create(&dir).unwrap();
        let txid = [0xab; 32];
        let mk = |hint: u8| {
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
                script_sig: vec![hint],
                witness: vec![],
            }];
            let outputs = vec![OutputRecord::unspent(1, vec![0x51])];
            (rec, inputs, outputs)
        };
        let fk1 = t.put_full_batch_indexed(&[mk(1)], true).unwrap()[0];
        let fk2 = t.put_full_batch_indexed(&[mk(2)], true).unwrap()[0];
        // Also resolve a few unrelated keys in the same bulk call.
        let mut extra = Vec::new();
        for i in 0u8..10 {
            let mut other = [0u8; 32];
            other[0] = i.wrapping_add(1);
            let rec = TxRecord {
                txid: other,
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
            let fk = t
                .put_full_batch_indexed(&[(rec, inputs, outputs)], true)
                .unwrap()[0];
            extra.push((other, fk));
        }
        let mut keys: Vec<[u8; 32]> = extra.iter().map(|(t, _)| *t).collect();
        keys.push(txid);
        keys.push([0xff; 32]); // miss
        let batch = t.get_fk_by_txid_batch(&keys).unwrap();
        let hit = batch.iter().find(|(t, _)| *t == txid).unwrap().1;
        assert_eq!(hit, Some(fk2));
        assert_ne!(hit, Some(fk1));
        for (other, fk) in &extra {
            let h = batch.iter().find(|(t, _)| t == other).unwrap().1;
            assert_eq!(h, Some(*fk));
        }
        assert!(batch.iter().any(|(t, f)| *t == [0xff; 32] && f.is_none()));
        let _ = std::fs::remove_dir_all(&dir);
        std::env::remove_var("RBITCOIN_HEAD_SCALE");
        std::env::remove_var("RBITCOIN_BULK_IO_WORKERS");
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
